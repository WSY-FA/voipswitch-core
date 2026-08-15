use crate::runtime::call::actor::{
    CalleeSessionActor, PendingAttemptCandidate, PendingAttemptRegistration, SessionActor,
};
use crate::runtime::call::model::{OutboundCandidate, OutboundTarget};
use crate::runtime::call::registry::LegEventDeduper;
use crate::runtime::call::session::{ControlMessage, SessionHandle};
use crate::runtime::call::timer::CallTimerKind;
use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, info, warn};
use voipswitch_core::media::SdpBody;
use voipswitch_core::types::call::{CallState, HangupCause};

pub(crate) const ATTEMPT_REGISTRATION_CAPACITY: usize = 1024;

pub(crate) struct AttemptRegistration {
    pub(crate) call_id: String,
    pub(crate) coordinator_session_id: String,
    pub(crate) coordinator_generation: u64,
    pub(crate) previous_session_id: String,
    pub(crate) attempt_seq: u16,
    pub(crate) session_id: String,
    pub(crate) handle: SessionHandle,
    pub(crate) actor: CalleeSessionActor,
}

#[derive(Debug)]
pub(crate) enum AttemptRegistrationResult {
    Registered {
        attempt_seq: u16,
        session_id: String,
    },
    Rejected {
        attempt_seq: u16,
        session_id: String,
        reason: String,
    },
}

#[derive(Clone)]
pub(crate) struct AttemptRegistrar {
    tx: mpsc::Sender<AttemptRegistration>,
}

impl AttemptRegistrar {
    pub(crate) fn new() -> (Self, mpsc::Receiver<AttemptRegistration>) {
        let (tx, rx) = mpsc::channel(ATTEMPT_REGISTRATION_CAPACITY);
        (Self { tx }, rx)
    }

    pub(crate) fn try_register(&self, request: AttemptRegistration) -> Result<()> {
        self.tx.try_send(request).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => anyhow!("attempt registration queue full"),
            mpsc::error::TrySendError::Closed(_) => {
                anyhow!("attempt registration service stopped")
            }
        })
    }
}

impl SessionActor {
    pub(super) fn route_budget_remaining(&self) -> Duration {
        self.route_deadline
            .saturating_duration_since(Instant::now())
    }

    pub(super) fn has_retry_candidate(&self, status_code: u16) -> bool {
        !self.call.is_answered()
            && !self.call.is_terminating()
            && self.pending_attempt_candidate.is_none()
            && self.pending_attempt_registration.is_none()
            && self.retry_after_cleanup.is_none()
            && !self.remaining_candidates.is_empty()
            && retryable_status(status_code)
            && self.route_budget_remaining()
                >= self.call.config_snapshot.timeouts.min_attempt_window
    }

    pub(super) fn wait_for_retry_cleanup(&mut self, status_code: u16) -> Result<bool> {
        if !self.has_retry_candidate(status_code) {
            return Ok(false);
        }
        self.cancel_timer(CallTimerKind::Dial);
        self.cancel_timer(CallTimerKind::Ring);
        self.retry_after_cleanup = Some(status_code);
        self.call.last_status = Some(status_code);
        let call_id = self.call.call_id().to_string();
        let attempt_seq = self.current_attempt_seq;
        self.submit_callee_action(
            "CancelOutbound",
            format!("retry-cancel-{call_id}-attempt-{attempt_seq}"),
            json!({ "reason": "sequential_hunt_retry" }),
        )?;
        self.start_timer(CallTimerKind::Cleanup);
        info!(
            call_id,
            attempt_seq, status_code, "callee attempt waiting for cleanup before retry"
        );
        self.publish_call_view();
        Ok(true)
    }

    pub(super) fn retry_after_originate_failure(&mut self) -> Result<bool> {
        if !self.has_retry_candidate(503) {
            return Ok(false);
        }
        self.call.callee_terminated = true;
        self.begin_next_attempt(503)
    }

    pub(super) fn retry_after_attempt_ended(&mut self, status_code: u16) -> Result<bool> {
        if let Some(retry_status) = self.retry_after_cleanup.take() {
            self.cancel_timer(CallTimerKind::Cleanup);
            self.call.callee_terminated = true;
            if !self.begin_next_attempt(retry_status)? {
                self.terminate_exhausted_retry(retry_status)?;
            }
            return Ok(true);
        } else if !self.has_retry_candidate(status_code) {
            return Ok(false);
        }
        self.call.callee_terminated = true;
        self.begin_next_attempt(status_code)
    }

    pub(super) fn retry_after_late_answer(&mut self) -> Result<bool> {
        let Some(status_code) = self.retry_after_cleanup.take() else {
            return Ok(false);
        };
        self.cancel_timer(CallTimerKind::Cleanup);
        let call_id = self.call.call_id().to_string();
        let attempt_seq = self.current_attempt_seq;
        self.submit_callee_action(
            "HangupDialog",
            format!("retry-late-answer-hangup-{call_id}-attempt-{attempt_seq}"),
            json!({}),
        )?;
        self.call.callee_terminated = true;
        warn!(
            call_id,
            attempt_seq,
            status_code,
            "late answer received while callee attempt awaited retry cleanup"
        );
        if !self.begin_next_attempt(status_code)? {
            self.terminate_exhausted_retry(status_code)?;
        }
        Ok(true)
    }

    pub(super) fn force_retry_after_cleanup(&mut self) -> Result<bool> {
        let Some(status_code) = self.retry_after_cleanup.take() else {
            return Ok(false);
        };
        let call_id = self.call.call_id().to_string();
        let attempt_seq = self.current_attempt_seq;
        self.submit_callee_action(
            "HangupDialog",
            format!("retry-cleanup-hangup-{call_id}-attempt-{attempt_seq}"),
            json!({}),
        )?;
        self.call.callee_terminated = true;
        warn!(
            call_id,
            attempt_seq,
            status_code,
            "callee attempt cleanup deadline reached; forcing sequential retry"
        );
        if !self.begin_next_attempt(status_code)? {
            self.terminate_exhausted_retry(status_code)?;
        }
        Ok(true)
    }

    fn begin_next_attempt(&mut self, previous_status: u16) -> Result<bool> {
        if !retryable_status(previous_status)
            || self.route_budget_remaining() < self.call.config_snapshot.timeouts.min_attempt_window
        {
            return Ok(false);
        }
        let Some(candidate) = self.remaining_candidates.pop_front() else {
            return Ok(false);
        };
        self.call
            .update_current_attempt(
                voipswitch_core::call::CalleeAttemptState::Failed,
                Some(hangup_cause_for_status(previous_status)),
            )
            .map_err(anyhow::Error::msg)?;
        self.cancel_timer(CallTimerKind::Dial);
        self.cancel_timer(CallTimerKind::Ring);
        self.cancel_timer(CallTimerKind::Cleanup);
        let attempt_seq = self.current_attempt_seq.saturating_add(1);
        self.pending_attempt_candidate = Some(PendingAttemptCandidate {
            attempt_seq,
            candidate: candidate.clone(),
        });
        self.call.last_status = Some(previous_status);
        self.media_generation = self.media_generation.saturating_add(1);
        self.media_executor.prepare_callee_offer(
            format!(
                "prepare-attempt-offer-{}-{attempt_seq}",
                self.call.call_id()
            ),
            self.media_generation,
            attempt_seq,
            self.caller_offer.clone(),
            candidate.callee_route_target,
        )?;
        info!(
            call_id = self.call.call_id(),
            previous_attempt_seq = self.current_attempt_seq,
            next_attempt_seq = attempt_seq,
            previous_status,
            target = candidate.outbound_target.stable_ref(),
            "starting sequential callee attempt preparation"
        );
        self.publish_call_view();
        Ok(true)
    }

    pub(super) fn handle_attempt_offer_prepared(
        &mut self,
        attempt_seq: u16,
        result: std::result::Result<SdpBody, String>,
    ) -> Result<()> {
        let Some(pending) = self.pending_attempt_candidate.take() else {
            debug!(
                call_id = self.call.call_id(),
                attempt_seq, "stale callee attempt SDP result ignored"
            );
            return Ok(());
        };
        if pending.attempt_seq != attempt_seq || self.call.is_terminating() {
            debug!(
                call_id = self.call.call_id(),
                attempt_seq,
                expected_attempt_seq = pending.attempt_seq,
                "stale callee attempt SDP result ignored"
            );
            return Ok(());
        }
        let callee_offer = match result {
            Ok(offer) => offer,
            Err(reason) => {
                warn!(
                    call_id = self.call.call_id(),
                    attempt_seq, reason, "prepare callee attempt SDP failed"
                );
                self.terminate_retry_setup("attempt_media_failed")?;
                return Ok(());
            }
        };
        let (handle, control_rx, event_rx) = SessionHandle::new_pair();
        let session_id = format!(
            "session-{}-callee-attempt-{attempt_seq}",
            self.call.call_id()
        );
        self.action_generation = self.action_generation.saturating_add(1);
        let callee_session = self
            .call
            .make_callee_session(
                session_id.clone(),
                attempt_seq,
                &pending.candidate.outbound_target,
                voipswitch_core::types::time::unix_timestamp_ms(),
            )
            .map_err(|error| anyhow!(error))?;
        let actor = CalleeSessionActor {
            session: callee_session,
            attempt_seq,
            leg_event_deduper: LegEventDeduper::default(),
            control_rx,
            event_rx,
            coordinator_handle: self.coordinator_handle.clone(),
            control_dispatcher: self.control_dispatcher.clone(),
            action_generation: self.action_generation,
            completed_actions: HashSet::new(),
            dtmf_source: crate::runtime::call::dtmf::DtmfSourceState::default(),
        };
        self.pending_attempt_registration = Some(PendingAttemptRegistration {
            attempt_seq,
            candidate: pending.candidate,
            session_id: session_id.clone(),
            callee_control_tx: handle.control_sender(),
            callee_offer,
        });
        let request = AttemptRegistration {
            call_id: self.call.call_id().to_string(),
            coordinator_session_id: self.call.aggregate.coordinator().as_str().to_string(),
            coordinator_generation: self.call.aggregate.coordinator_generation(),
            previous_session_id: self.call.callee_session_id().to_string(),
            attempt_seq,
            session_id,
            handle,
            actor,
        };
        if let Err(error) = self.attempt_registrar.try_register(request) {
            self.pending_attempt_registration = None;
            warn!(
                call_id = self.call.call_id(),
                attempt_seq,
                error = %error,
                "register callee attempt failed"
            );
            self.terminate_retry_setup("attempt_registration_overload")?;
        }
        Ok(())
    }

    pub(super) fn handle_attempt_registration_result(
        &mut self,
        result: AttemptRegistrationResult,
    ) -> Result<()> {
        match result {
            AttemptRegistrationResult::Registered {
                attempt_seq,
                session_id,
            } => {
                let Some(pending) = self.pending_attempt_registration.take() else {
                    return Ok(());
                };
                if pending.attempt_seq != attempt_seq
                    || pending.session_id != session_id
                    || self.call.is_terminating()
                {
                    let _ = pending.callee_control_tx.try_send(ControlMessage::Shutdown);
                    debug!(
                        call_id = self.call.call_id(),
                        attempt_seq, session_id, "stale callee attempt registration ignored"
                    );
                    return Ok(());
                }
                self.call
                    .register_attempt(
                        attempt_seq,
                        session_id.clone(),
                        &pending.candidate.outbound_target,
                        voipswitch_core::types::time::unix_timestamp_ms(),
                    )
                    .map_err(anyhow::Error::msg)?;
                self.current_attempt_seq = attempt_seq;
                self.call.callee_target = pending.candidate.outbound_target.stable_ref();
                self.call.outbound_trunk_ref = pending.candidate.outbound_trunk_ref.clone();
                self.call.outbound_trunk_name = pending.candidate.outbound_trunk_name.clone();
                self.call.recording_requested = pending.candidate.recording_requested;
                self.call.ai_policy = pending.candidate.ai_policy.clone();
                self.call.recording_start_error = None;
                self.call.last_status = None;
                self.call.hangup_cause = None;
                self.call.callee_terminated = false;
                self.call.late_answer_cleanup_sent = false;
                self.media_executor.update_dtmf_callee_target(
                    session_id.clone(),
                    pending.callee_control_tx.clone(),
                );
                self.callee_control_tx = pending.callee_control_tx;
                self.submit_originate(&pending.candidate, pending.callee_offer)?;
                self.start_timer(CallTimerKind::Dial);
                info!(
                    call_id = self.call.call_id(),
                    attempt_seq,
                    session_id,
                    target = self.call.callee_target,
                    remaining_candidates = self.remaining_candidates.len(),
                    "sequential callee attempt originated"
                );
                self.publish_call_view();
            }
            AttemptRegistrationResult::Rejected {
                attempt_seq,
                session_id,
                reason,
            } => {
                let matches_pending =
                    self.pending_attempt_registration
                        .as_ref()
                        .is_some_and(|pending| {
                            pending.attempt_seq == attempt_seq && pending.session_id == session_id
                        });
                if !matches_pending {
                    return Ok(());
                }
                self.pending_attempt_registration = None;
                warn!(
                    call_id = self.call.call_id(),
                    attempt_seq, session_id, reason, "callee attempt registration rejected"
                );
                self.terminate_retry_setup("attempt_registration_failed")?;
            }
        }
        Ok(())
    }

    fn submit_originate(&self, candidate: &OutboundCandidate, callee_offer: SdpBody) -> Result<()> {
        let call_id = self.call.call_id().to_string();
        let attempt_seq = self.current_attempt_seq;
        match &candidate.outbound_target {
            OutboundTarget::Endpoint {
                endpoint_id,
                endpoint_number,
            } => self.submit_callee_action(
                "OriginateEndpoint",
                format!("originate-{call_id}-attempt-{attempt_seq}"),
                json!({
                    "domain_id": self.call.domain_id(),
                    "caller_session_id": self.call.caller_session_id(),
                    "endpoint_id": endpoint_id,
                    "endpoint_number": endpoint_number,
                    "caller_number": self.call.config_snapshot.numbers.signaling_caller,
                    "caller_display_name": Value::Null,
                    "callee_number": self.call.config_snapshot.numbers.signaling_callee,
                    "sdp_offer": callee_offer,
                    "sip_metadata": {},
                    "extension_headers": {},
                }),
            ),
            OutboundTarget::Trunk { trunk_ref } => self.submit_callee_action(
                "OriginateTrunk",
                format!("originate-{call_id}-attempt-{attempt_seq}"),
                json!({
                    "domain_id": self.call.domain_id(),
                    "caller_session_id": self.call.caller_session_id(),
                    "trunk_ref": trunk_ref,
                    "caller_number": self.call.config_snapshot.numbers.signaling_caller,
                    "caller_display_name": Value::Null,
                    "callee_number": self.call.config_snapshot.numbers.signaling_callee,
                    "sdp_offer": callee_offer,
                    "sip_metadata": {},
                    "extension_headers": {},
                }),
            ),
        }
    }

    fn terminate_retry_setup(&mut self, cause: &str) -> Result<()> {
        self.call.begin_terminating(HangupCause::InternalError);
        self.call.state = CallState::Terminating;
        self.call.last_status = Some(503);
        self.call.hangup_cause = Some(HangupCause::InternalError);
        self.call.callee_terminated = true;
        self.submit_caller_action(
            "RejectInboundInvite",
            format!("reject-{cause}-{}", self.call.call_id()),
            json!({
                "adapter_call_leg_id": self.call.caller_adapter_leg_id.as_str(),
                "status_code": 503,
            }),
        )?;
        self.start_timer(CallTimerKind::Cleanup);
        self.publish_call_view();
        Ok(())
    }

    fn terminate_exhausted_retry(&mut self, status_code: u16) -> Result<()> {
        self.call.begin_terminating(HangupCause::InternalError);
        self.call.state = CallState::Terminating;
        self.call.last_status = Some(status_code);
        self.call.hangup_cause = Some(HangupCause::NoRoute);
        self.call.callee_terminated = true;
        self.submit_caller_action(
            "RejectInboundInvite",
            format!("reject-route-budget-{}", self.call.call_id()),
            json!({
                "adapter_call_leg_id": self.call.caller_adapter_leg_id.as_str(),
                "status_code": status_code,
            }),
        )?;
        self.start_timer(CallTimerKind::Cleanup);
        self.publish_call_view();
        Ok(())
    }
}

pub(crate) fn retryable_status(status_code: u16) -> bool {
    matches!(status_code, 408 | 480 | 500 | 502 | 503 | 504)
}

fn hangup_cause_for_status(status_code: u16) -> voipswitch_core::types::call::HangupCause {
    match status_code {
        408 | 504 => HangupCause::RecoveryOnTimerExpire,
        480 => HangupCause::NoUserResponse,
        486 | 600 => HangupCause::UserBusy,
        503 => HangupCause::TemporaryFailure,
        _ => HangupCause::NetworkFailure,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attempt_registration_queue_is_bounded() {
        let (registrar, _rx) = AttemptRegistrar::new();
        assert_eq!(registrar.tx.max_capacity(), ATTEMPT_REGISTRATION_CAPACITY);
    }

    #[test]
    fn retryable_failure_policy_is_explicit() {
        for status in [408, 480, 500, 502, 503, 504] {
            assert!(retryable_status(status), "{status} should retry");
        }
        for status in [401, 403, 404, 407, 410, 484, 486, 600, 603, 604, 606] {
            assert!(!retryable_status(status), "{status} should terminate");
        }
    }
}
