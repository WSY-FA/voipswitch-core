use crate::app::AppState;
use crate::data_store::CallTraceMessage;
use crate::runtime::call::action::AdapterActionExecutor;
use crate::runtime::call::attempt::AttemptRegistrar;
use crate::runtime::call::dtmf::{
    DigitCollectorState, DigitCollectorTimerSlot, DtmfDisposition, DtmfRouter, DtmfSourceDecision,
    DtmfSourceState, wait_for_digit_collector_timer,
};
use crate::runtime::call::event::{
    ActionDeliveryFailed, ActionIdentity, CallActionAck, CallLegEvent, DtmfInfoSendResult,
};
use crate::runtime::call::media_action::MediaActionExecutor;
use crate::runtime::call::model::{CallRuntime, OutboundCandidate};
use crate::runtime::call::record_ai_agent_trace;
use crate::runtime::call::registry::LegEventDeduper;
use crate::runtime::call::session::{
    CallCoordinatorHandle, ControlMessage, CoordinatorHandoffToken, CoordinatorIdentity,
    CriticalControlDispatcher, EventMessage,
};
use crate::runtime::call::timer::{CallTimerKind, CallTimerSlot, wait_for_timer};
use ai_protocol::control::{ActionRequested, ActionResult, AgentAction};
use anyhow::Result;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, info, warn};
use voipswitch_core::dtmf::{DigitEvent, DtmfDigit, DtmfEventId, DtmfTransport};
use voipswitch_core::media::SdpBody;
use voipswitch_core::session::Session;
use voipswitch_core::types::call::{CallState, HangupCause, LegState};
use voipswitch_core::types::time::unix_timestamp_ms;

pub(crate) struct InitialCallAction {
    pub(crate) callee: bool,
    pub(crate) action_kind: String,
    pub(crate) action_id: String,
    pub(crate) body: Value,
}

pub(crate) struct PendingAnswer {
    pub(crate) action_id: String,
    pub(crate) payload_types: Vec<u8>,
}

pub(crate) struct PendingAttemptCandidate {
    pub(crate) attempt_seq: u16,
    pub(crate) candidate: OutboundCandidate,
}

pub(crate) struct PendingAttemptRegistration {
    pub(crate) attempt_seq: u16,
    pub(crate) candidate: OutboundCandidate,
    pub(crate) session_id: String,
    pub(crate) callee_control_tx: mpsc::Sender<ControlMessage>,
    pub(crate) callee_offer: SdpBody,
}

pub(crate) struct PendingAiTransfer {
    pub(crate) conversation: ai_protocol::control::JobRef,
    pub(crate) operation_id: String,
    pub(crate) generation: u64,
    pub(crate) number: String,
    pub(crate) language: String,
}

pub(crate) struct CoordinatorRuntimeState {
    pub(crate) call: CallRuntime,
    pub(crate) actor_started: bool,
    pub(crate) leg_event_deduper: LegEventDeduper,
    pub(crate) finalized: bool,
    pub(crate) finalization_started: bool,
    pub(crate) action_generation: u64,
    pub(crate) completed_actions: HashSet<(String, u64)>,
    pub(crate) media_generation: u64,
    pub(crate) dtmf_source: DtmfSourceState,
    pub(crate) dtmf_router: DtmfRouter,
    pub(crate) dtmf_collectors:
        BTreeMap<voipswitch_core::types::ids::CollectorId, DigitCollectorState>,
    pub(crate) dtmf_collector_timer: Option<DigitCollectorTimerSlot>,
    pub(crate) dtmf_collector_timer_generation: u64,
    pub(crate) initial_actions: Vec<InitialCallAction>,
    pub(crate) pending_answer: Option<PendingAnswer>,
    pub(crate) answer_in_progress: bool,
    pub(crate) deferred_callee_disconnect: Option<CallLegEvent>,
    pub(crate) dial_timer: Option<CallTimerSlot>,
    pub(crate) ring_timer: Option<CallTimerSlot>,
    pub(crate) cleanup_timer: Option<CallTimerSlot>,
    pub(crate) remaining_candidates: VecDeque<OutboundCandidate>,
    pub(crate) current_attempt_seq: u16,
    pub(crate) pending_attempt_candidate: Option<PendingAttemptCandidate>,
    pub(crate) pending_attempt_registration: Option<PendingAttemptRegistration>,
    pub(crate) retry_after_cleanup: Option<u16>,
    pub(crate) caller_offer: SdpBody,
    pub(crate) route_deadline: Instant,
    pub(crate) pending_ai_transfer: Option<PendingAiTransfer>,
    pub(crate) ai_transfer_failed: bool,
    pub(crate) prompt_generation: Arc<AtomicU64>,
}

pub(crate) struct SessionActor {
    pub(crate) runtime: CoordinatorRuntimeState,
    pub(crate) state: AppState,
    pub(crate) action_executor: AdapterActionExecutor,
    pub(crate) media_executor: MediaActionExecutor,
    pub(crate) control_dispatcher: CriticalControlDispatcher,
    pub(crate) attempt_registrar: AttemptRegistrar,
    pub(crate) control_rx: mpsc::Receiver<ControlMessage>,
    pub(crate) event_rx: mpsc::Receiver<EventMessage>,
    pub(crate) coordinator_control_rx: mpsc::Receiver<ControlMessage>,
    pub(crate) coordinator_event_rx: mpsc::Receiver<EventMessage>,
    pub(crate) coordinator_handle: CallCoordinatorHandle,
    pub(crate) callee_control_tx: mpsc::Sender<ControlMessage>,
    pub(crate) finished_tx: mpsc::Sender<String>,
    pub(crate) owned_session_id: String,
    pub(crate) owned_action_generation: u64,
    pub(crate) owned_completed_actions: HashSet<(String, u64)>,
}

pub(crate) struct CoordinatorHandoffPackage {
    token: CoordinatorHandoffToken,
    actor: Box<SessionActor>,
}

impl CoordinatorHandoffPackage {
    pub(crate) fn target_ready(&self, target: &CalleeSessionActor) -> Result<(), String> {
        if target.call_id() != self.token.call_id
            || target.session_id() != self.token.target_session_id
            || target.session.state.is_terminal()
        {
            return Err("coordinator_handoff_target_unavailable".to_string());
        }
        if self.actor.call.call_id() != self.token.call_id
            || self.actor.owned_session_id != self.token.source_session_id
            || self.actor.call.aggregate.coordinator().as_str() != self.token.source_session_id
            || self.actor.call.aggregate.coordinator_generation() != self.token.source_generation
            || self.actor.coordinator_handle.call_id() != self.token.call_id
            || target.coordinator_handle.call_id() != self.token.call_id
        {
            return Err("coordinator_handoff_package_mismatch".to_string());
        }
        Ok(())
    }

    pub(crate) fn rollback(self) -> SessionActor {
        *self.actor
    }

    pub(crate) fn install_on(
        self,
        target: CalleeSessionActor,
        committed: &CoordinatorIdentity,
    ) -> Result<SessionActor, Box<CoordinatorHandoffInstallFailure>> {
        if committed.session_id != self.token.target_session_id
            || committed.generation != self.token.target_generation
        {
            return Err(Box::new(CoordinatorHandoffInstallFailure {
                package: self,
                target: Box::new(target),
                reason: "coordinator_handoff_not_committed".to_string(),
            }));
        }
        if let Err(error) = self.target_ready(&target) {
            return Err(Box::new(CoordinatorHandoffInstallFailure {
                package: self,
                target: Box::new(target),
                reason: error,
            }));
        }

        let CoordinatorHandoffPackage { token, actor } = self;
        let mut actor = *actor;
        let target_id =
            voipswitch_core::types::ids::SessionId::from(token.target_session_id.clone());
        match actor.call.aggregate.handoff_coordinator(
            &voipswitch_core::types::ids::SessionId::from(token.source_session_id.clone()),
            token.source_generation,
            &target_id,
        ) {
            Ok(generation) if generation == token.target_generation => {}
            Ok(_) => {
                return Err(Box::new(CoordinatorHandoffInstallFailure {
                    package: CoordinatorHandoffPackage {
                        token,
                        actor: Box::new(actor),
                    },
                    target: Box::new(target),
                    reason: "coordinator_handoff_generation_mismatch".to_string(),
                }));
            }
            Err(error) => {
                return Err(Box::new(CoordinatorHandoffInstallFailure {
                    package: CoordinatorHandoffPackage {
                        token,
                        actor: Box::new(actor),
                    },
                    target: Box::new(target),
                    reason: error.to_string(),
                }));
            }
        }

        actor.owned_session_id = target.session.id.as_str().to_string();
        actor.runtime.leg_event_deduper = target.leg_event_deduper;
        actor.runtime.dtmf_source = target.dtmf_source;
        actor.owned_action_generation = target.action_generation;
        actor.owned_completed_actions = target.completed_actions;
        actor.control_rx = target.control_rx;
        actor.event_rx = target.event_rx;
        Ok(actor)
    }
}

pub(crate) struct CoordinatorHandoffInstallFailure {
    pub(crate) package: CoordinatorHandoffPackage,
    pub(crate) target: Box<CalleeSessionActor>,
    pub(crate) reason: String,
}

pub(crate) struct CoordinatorHandoffPrepareFailure {
    pub(crate) actor: Box<SessionActor>,
    pub(crate) reason: String,
}

impl Deref for SessionActor {
    type Target = CoordinatorRuntimeState;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl DerefMut for SessionActor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.runtime
    }
}

#[derive(Clone, Copy)]
enum ControlSource {
    OwnedLeg,
    Coordinator,
}

impl SessionActor {
    pub(crate) fn prepare_handoff(
        self,
        token: CoordinatorHandoffToken,
    ) -> Result<CoordinatorHandoffPackage, CoordinatorHandoffPrepareFailure> {
        let valid = self.call.call_id() == token.call_id
            && self.owned_session_id == token.source_session_id
            && self.call.aggregate.coordinator().as_str() == token.source_session_id
            && self.call.aggregate.coordinator_generation() == token.source_generation
            && token.source_generation.checked_add(1) == Some(token.target_generation)
            && self
                .call
                .aggregate
                .handoff_candidates(None)
                .iter()
                .any(|candidate| candidate.as_str() == token.target_session_id);
        if !valid {
            return Err(CoordinatorHandoffPrepareFailure {
                actor: Box::new(self),
                reason: "coordinator_handoff_prepare_mismatch".to_string(),
            });
        }
        Ok(CoordinatorHandoffPackage {
            token,
            actor: Box::new(self),
        })
    }

    pub(crate) async fn run(mut self) {
        if self.state.is_shutting_down() {
            self.actor_started = true;
            let result = self.begin_shutdown();
            self.complete_processing(result);
        }
        if !self.actor_started {
            self.actor_started = true;
            let initial_actions = std::mem::take(&mut self.initial_actions);
            for action in initial_actions {
                let result = if action.callee {
                    self.submit_callee_action(&action.action_kind, action.action_id, action.body)
                } else {
                    self.submit_caller_action(&action.action_kind, action.action_id, action.body)
                };
                if let Err(error) = result {
                    warn!(call_id = self.call.call_id(), error = %error, "submit initial call action failed");
                    self.call.begin_terminating(HangupCause::InternalError);
                    self.call.hangup_cause = Some(HangupCause::InternalError);
                    self.begin_finish_call();
                    break;
                }
            }
            if !self.finalization_started && self.call.callee_target.starts_with("ai_agent:") {
                self.media_generation = self.media_generation.saturating_add(1);
                if let Err(error) = self.media_executor.prepare_sdp(
                    format!("prepare-ai-agent-answer-{}", self.call.call_id()),
                    self.media_generation,
                    crate::runtime::call::media_action::PrepareSdpPurpose::AiAgentAnswer,
                    self.caller_offer.clone(),
                ) {
                    warn!(call_id = self.call.call_id(), error = %error, "AI Agent media answer preparation failed");
                    self.call.begin_terminating(HangupCause::InternalError);
                    self.begin_finish_call();
                }
            }
            if !self.finalization_started {
                self.start_timer(crate::runtime::call::timer::CallTimerKind::Dial);
                self.publish_call_view();
                info!(
                    call_id = self.call.call_id(),
                    caller = self.call.caller_number,
                    callee = self.call.callee_number,
                    runtime_config_version = self.call.config_snapshot.runtime_config_version,
                    domain_config_version = self.call.config_snapshot.domain_config_version,
                    recording_policy_ids = ?self.call.config_snapshot.recording.initial_policy_ids,
                    "basic call originated"
                );
            }
        }
        let mut shutdown = self.state.subscribe_shutdown();
        let mut shutdown_seen = false;
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed(), if !shutdown_seen => {
                    shutdown_seen = true;
                    let result = self.begin_shutdown();
                    if !self.complete_processing(result) { break; }
                }
                message = self.control_rx.recv() => {
                    let Some(message) = message else { break; };
                    if !self.handle_control(message, ControlSource::OwnedLeg).await {
                        break;
                    }
                }
                message = self.coordinator_control_rx.recv() => {
                    let Some(message) = message else { break; };
                    if let ControlMessage::PrepareCoordinatorHandoff { token, reply } = message {
                        let call_id = token.call_id.clone();
                        match self.prepare_handoff(token) {
                            Ok(package) => {
                                info!(call_id, "coordinator actor quiesced for handoff");
                                match reply.send(Ok(package)) {
                                    Ok(()) => return,
                                    Err(Ok(package)) => {
                                        self = package.rollback();
                                        continue;
                                    }
                                    Err(Err(_)) => unreachable!("source package was ready"),
                                }
                            }
                            Err(failure) => {
                                let reason = failure.reason.clone();
                                self = *failure.actor;
                                let _ = reply.send(Err(reason));
                                continue;
                            }
                        }
                    }
                    if !self.handle_control(message, ControlSource::Coordinator).await {
                        break;
                    }
                }
                generation = wait_for_timer(&mut self.runtime.dial_timer) => {
                    let result = self.handle_timer_expiry(CallTimerKind::Dial, generation).await;
                    if !self.complete_processing(result) {
                        break;
                    }
                }
                generation = wait_for_timer(&mut self.runtime.ring_timer) => {
                    let result = self.handle_timer_expiry(CallTimerKind::Ring, generation).await;
                    if !self.complete_processing(result) {
                        break;
                    }
                }
                generation = wait_for_timer(&mut self.runtime.cleanup_timer) => {
                    let result = self.handle_timer_expiry(CallTimerKind::Cleanup, generation).await;
                    if !self.complete_processing(result) {
                        break;
                    }
                }
                timer = wait_for_digit_collector_timer(&mut self.runtime.dtmf_collector_timer) => {
                    let result = self.handle_digit_collector_timeout(timer.0, &timer.1, timer.2);
                    if !self.complete_processing(result) {
                        break;
                    }
                }
                message = self.event_rx.recv() => {
                    let Some(message) = message else { break; };
                    self.handle_event_message(message);
                }
                message = self.coordinator_event_rx.recv() => {
                    let Some(message) = message else { break; };
                    self.handle_event_message(message);
                }
            }
        }

        if !self.finalized {
            self.call
                .hangup_cause
                .get_or_insert(HangupCause::InternalError);
            self.begin_finish_call();
        }
        if self.owned_session_id != self.call.callee_session_id() {
            let _ = self.callee_control_tx.send(ControlMessage::Shutdown).await;
        }
        let _ = self.finished_tx.send(self.call.call_id().to_string()).await;
    }

    async fn handle_control(&mut self, message: ControlMessage, source: ControlSource) -> bool {
        if self.finalization_started {
            if matches!(message, ControlMessage::ForceCleanup) {
                return false;
            }
            if matches!(message, ControlMessage::CallFinalized) {
                self.finalized = true;
                return false;
            }
            debug!(
                call_id = self.call.call_id(),
                "control message ignored while call finalization is in progress"
            );
            return true;
        }
        let result = match message {
            ControlMessage::LegEvent(frame_type, event) => {
                if self.owned_session_id == self.call.caller_session_id() {
                    self.handle_caller_leg_event(&frame_type, event).await
                } else {
                    self.handle_peer_leg_event(&frame_type, event).await
                }
            }
            ControlMessage::PeerLegEvent(frame_type, event) => {
                self.handle_peer_leg_event(&frame_type, event).await
            }
            ControlMessage::ActionDeliveryFailed(failure) => {
                let peer_failure = matches!(source, ControlSource::OwnedLeg)
                    && self.owned_session_id != self.call.caller_session_id();
                self.handle_action_failure(failure, peer_failure)
            }
            ControlMessage::PeerActionFailed(failure) => self.handle_action_failure(failure, true),
            ControlMessage::CallActionAck(ack) => {
                if matches!(source, ControlSource::OwnedLeg)
                    && self.owned_session_id != self.call.caller_session_id()
                {
                    self.handle_owned_leg_action_ack(ack)
                } else {
                    self.handle_action_ack(ack)
                }
            }
            ControlMessage::MediaActionResult(result) => self.handle_media_action_result(result),
            ControlMessage::MediaDtmfObserved(event) => self.handle_source_digit(event),
            ControlMessage::SipDtmfReceived(event) => self.handle_source_digit(event),
            ControlMessage::PeerDigitEvent(event) => self.handle_peer_digit(event),
            ControlMessage::DtmfInfoSendResult(result) => self.handle_dtmf_info_send_result(result),
            ControlMessage::AttemptRegistrationResult(result) => {
                self.handle_attempt_registration_result(result)
            }
            ControlMessage::AiActionRequested(request) => self.handle_ai_action(request),
            ControlMessage::AiGatewayUnavailable => self.handle_ai_gateway_unavailable(),
            ControlMessage::PrepareCoordinatorHandoff { .. }
            | ControlMessage::PrepareCoordinatorTarget { .. } => {
                warn!(
                    call_id = self.call.call_id(),
                    "coordinator handoff control delivered through the wrong mailbox"
                );
                Ok(())
            }
            ControlMessage::CallFinalized => {
                self.finalized = true;
                return false;
            }
            ControlMessage::ForceCleanup => {
                let cause = if self.state.is_shutting_down() {
                    HangupCause::SystemShutdown
                } else {
                    HangupCause::InternalError
                };
                self.call.begin_terminating(cause);
                self.begin_finish_call();
                return false;
            }
            ControlMessage::Shutdown => self.begin_shutdown(),
        };
        self.complete_processing(result)
    }

    pub(super) fn begin_shutdown(&mut self) -> Result<()> {
        self.retry_after_cleanup = None;
        if self.call.is_terminating() {
            return Ok(());
        }
        let answered = self.call.is_answered();
        self.call.begin_terminating(HangupCause::SystemShutdown);
        self.cancel_timer(CallTimerKind::Dial);
        self.cancel_timer(CallTimerKind::Ring);
        let call_id = self.call.call_id().to_string();
        if !self.call.caller_terminated {
            self.submit_caller_action(
                if answered { "HangupDialog" } else { "RejectInboundInvite" },
                format!("shutdown-caller-{call_id}"),
                json!({"adapter_call_leg_id": self.call.caller_adapter_leg_id.as_str(), "status_code": 503}),
            )?;
        }
        if self.ai_agent_conversation_active() && self.pending_ai_transfer.is_none() {
            self.call.callee_terminated = true;
        }
        if !self.call.callee_terminated {
            self.submit_callee_action(
                if answered && self.pending_ai_transfer.is_none() {
                    "HangupDialog"
                } else {
                    "CancelOutbound"
                },
                format!("shutdown-callee-{call_id}"),
                json!({"reason": "system_shutdown"}),
            )?;
        }
        self.start_timer(CallTimerKind::Cleanup);
        self.publish_call_view();
        Ok(())
    }

    fn complete_processing(&mut self, result: Result<()>) -> bool {
        if let Err(error) = result {
            warn!(call_id = %self.call.call_id(), error = %error, "caller session actor event failed");
            self.call.begin_terminating(HangupCause::InternalError);
            self.call
                .hangup_cause
                .get_or_insert(HangupCause::InternalError);
            self.begin_finish_call();
            return true;
        }
        if self.finalized {
            return false;
        }
        if self.call.caller_terminated && self.call.callee_terminated {
            self.begin_finish_call();
            return true;
        }
        true
    }

    fn handle_ai_action(&mut self, request: ActionRequested) -> Result<()> {
        let conversation = request.conversation.clone();
        let operation_id = request.operation_id.clone();
        let generation = request.generation;
        let action_kind = match &request.action {
            AgentAction::PlayText { .. } => "play_text",
            AgentAction::CollectDigits { .. } => "collect_digits",
            AgentAction::TransferToExtension { .. } => "transfer_to_extension",
            AgentAction::TransferToBusinessTarget { .. } => "transfer_to_business_target",
            AgentAction::EndCall { .. } => "end_call",
        };
        let result = self.validate_ai_action(&request).and_then(|_| {
            let key = (format!("ai:{operation_id}"), generation);
            if !self.runtime.completed_actions.insert(key) {
                return Ok((true, "IDEMPOTENT_REPLAY".to_string(), None));
            }
            match request.action {
                AgentAction::EndCall { reason } => {
                    self.call.begin_terminating(HangupCause::NormalClearing);
                    self.call.hangup_cause = Some(HangupCause::NormalClearing);
                    self.call.state = CallState::Terminating;
                    self.call.callee_terminated = true;
                    self.cancel_timer(CallTimerKind::Dial);
                    self.cancel_timer(CallTimerKind::Ring);
                    self.submit_caller_action(
                        "HangupDialog",
                        format!("ai-end-call-{}", self.call.call_id()),
                        json!({"reason": reason}),
                    )?;
                    self.start_timer(CallTimerKind::Cleanup);
                    self.publish_call_view();
                    Ok((true, "ACCEPTED".to_string(), None))
                }
                AgentAction::CollectDigits {
                    max_digits,
                    timeout_ms,
                } => {
                    let timeout_ms = timeout_ms.min(
                        request
                            .deadline_at_ms
                            .saturating_sub(unix_timestamp_ms())
                            .max(1),
                    );
                    let Some(ai_jobs) = self.state.ai_jobs() else {
                        return Err(anyhow::anyhow!("AI connector is unavailable"));
                    };
                    let allowed = HashSet::from([
                        DtmfDigit::D0,
                        DtmfDigit::D1,
                        DtmfDigit::D2,
                        DtmfDigit::D3,
                        DtmfDigit::D4,
                        DtmfDigit::D5,
                        DtmfDigit::D6,
                        DtmfDigit::D7,
                        DtmfDigit::D8,
                        DtmfDigit::D9,
                        DtmfDigit::Star,
                    ]);
                    let spec = crate::runtime::call::dtmf::DigitCollectionSpec {
                        collector_id: voipswitch_core::types::ids::CollectorId::from(
                            operation_id.to_string(),
                        ),
                        owner: voipswitch_core::types::ids::BusinessOperationId::from(
                            operation_id.to_string(),
                        ),
                        source_session_id: voipswitch_core::types::ids::SessionId::from(
                            self.call.caller_session_id().to_string(),
                        ),
                        mode: crate::runtime::call::dtmf::DigitCollectionMode::Collect,
                        allowed,
                        min_digits: 1,
                        max_digits: usize::from(max_digits),
                        terminators: HashSet::from([DtmfDigit::Pound]),
                        first_digit_timeout: Duration::from_millis(timeout_ms),
                        inter_digit_timeout: Duration::from_millis(timeout_ms),
                        overall_timeout: Duration::from_millis(timeout_ms),
                    };
                    let receivers = self
                        .coordinator_handle
                        .start_digit_collection(spec)
                        .map_err(|error| anyhow::anyhow!(error))?;
                    let connector = ai_jobs.clone();
                    let result_conversation = conversation.clone();
                    let result_operation = operation_id.clone();
                    tokio::spawn(async move {
                        let ready = receivers.ready.await;
                        let outcome = receivers.result.await;
                        let (success, code, message) = match (ready, outcome) {
                            (Ok(Ok(_)), Ok(outcome)) => {
                                let digits = dtmf_digits_to_string(&outcome.digits);
                                let success = matches!(
                                    outcome.code,
                                    crate::runtime::call::dtmf::DigitCollectionResultCode::Completed
                                );
                                (
                                    success,
                                    format!("DTMF_{:?}", outcome.code).to_ascii_uppercase(),
                                    Some(digits),
                                )
                            }
                            (Ok(Err(error)), _) => {
                                (false, "DTMF_ACQUIRE_FAILED".to_string(), Some(error))
                            }
                            _ => (
                                false,
                                "DTMF_RESULT_UNAVAILABLE".to_string(),
                                Some("digit collector result channel closed".to_string()),
                            ),
                        };
                        let _ = connector.try_send_action_result(ActionResult {
                            conversation: result_conversation,
                            operation_id: result_operation,
                            generation,
                            success,
                            code,
                            message,
                        });
                    });
                    Ok((true, "DEFERRED".to_string(), None))
                }
                AgentAction::TransferToExtension { number } => {
                    if self.runtime.pending_ai_transfer.is_some() {
                        return Ok((true, "IDEMPOTENT_REPLAY".to_string(), None));
                    }
                    let config = self.state.config().snapshot();
                    let Some(domain) =
                        config
                            .domains
                            .get(&voipswitch_core::types::ids::DomainId::from(
                                self.call.domain_id().to_string(),
                            ))
                    else {
                        self.start_transfer_failure_prompt(
                            conversation.clone(),
                            operation_id.clone(),
                            generation,
                            &number,
                            "domain_not_found",
                        )?;
                        return Ok((true, "DEFERRED".to_string(), None));
                    };
                    let Some(extension) = domain
                        .extensions
                        .iter()
                        .find(|e| e.enabled && e.number == number)
                    else {
                        self.start_transfer_failure_prompt(
                            conversation.clone(),
                            operation_id.clone(),
                            generation,
                            &number,
                            "extension_not_found",
                        )?;
                        return Ok((true, "DEFERRED".to_string(), None));
                    };
                    let Some(route) = crate::runtime::call::endpoint_resolved_route(
                        &self.state,
                        self.call.domain_id(),
                        extension.id.to_string(),
                        extension.number.clone(),
                        self.call.caller_number.clone(),
                        extension.number.clone(),
                    ) else {
                        self.start_transfer_failure_prompt(
                            conversation.clone(),
                            operation_id.clone(),
                            generation,
                            &number,
                            "extension_unavailable",
                        )?;
                        return Ok((true, "DEFERRED".to_string(), None));
                    };
                    let Some(candidate) = route.candidates.into_iter().next() else {
                        self.start_transfer_failure_prompt(
                            conversation.clone(),
                            operation_id.clone(),
                            generation,
                            &number,
                            "extension_unavailable",
                        )?;
                        return Ok((true, "DEFERRED".to_string(), None));
                    };
                    self.runtime.pending_ai_transfer = Some(PendingAiTransfer {
                        conversation: conversation.clone(),
                        operation_id: operation_id.to_string(),
                        generation,
                        number: number.clone(),
                        language: self.current_ai_language(),
                    });
                    self.runtime
                        .prompt_generation
                        .fetch_add(1, Ordering::AcqRel);
                    self.media_executor
                        .media_handle()
                        .set_ai_caller_input_enabled(false);
                    spawn_builtin_prompt(
                        self.media_executor.media_handle(),
                        self.runtime.prompt_generation.clone(),
                        true,
                        self.current_ai_language(),
                    );
                    if let Err(error) = self.begin_ai_transfer_attempt(candidate) {
                        self.fail_ai_transfer(format!("attempt_start_failed:{error}"))?;
                    }
                    Ok((true, "DEFERRED".to_string(), None))
                }
                AgentAction::TransferToBusinessTarget { .. } | AgentAction::PlayText { .. } => {
                    Ok((
                        false,
                        "ACTION_NOT_AVAILABLE".to_string(),
                        Some("action requires a later core capability batch".to_string()),
                    ))
                }
            }
        });
        let (success, code, message) = match result {
            Ok(value) => value,
            Err(error) => (
                false,
                "ACTION_REJECTED".to_string(),
                Some(error.to_string()),
            ),
        };
        if code != "DEFERRED"
            && let Some(ai_jobs) = self.state.ai_jobs()
        {
            ai_jobs.try_send_action_result(ActionResult {
                conversation,
                operation_id: operation_id.clone(),
                generation,
                success,
                code: code.clone(),
                message,
            })?;
        }
        self.state.record_call_trace(CallTraceMessage {
            call_id: self.call.call_id().to_string(),
            domain_id: self.call.domain_id().to_string(),
            sequence: unix_timestamp_ms(),
            observed_at_ms: unix_timestamp_ms(),
            direction: "ai-action".to_string(),
            adapter_call_leg_id: self.call.caller_adapter_leg_id.as_str().to_string(),
            session_id: Some(self.call.caller_session_id().to_string()),
            source_addr: None,
            destination_addr: None,
            start_line: format!("AI ActionRequested {action_kind}"),
            packet: serde_json::json!({
                "operation_id": operation_id,
                "generation": generation,
                "agent_revision": self.call.ai_agent_revision,
                "action": action_kind,
                "success": success,
                "code": code,
            })
            .to_string(),
        });
        Ok(())
    }

    fn start_transfer_failure_prompt(
        &mut self,
        conversation: ai_protocol::control::JobRef,
        operation_id: ai_protocol::id::OperationId,
        generation: u64,
        number: &str,
        reason: &str,
    ) -> Result<()> {
        self.runtime
            .prompt_generation
            .fetch_add(1, Ordering::AcqRel);
        let media = self.media_executor.media_handle();
        media.set_ai_caller_input_enabled(false);
        spawn_builtin_prompt(
            media,
            self.runtime.prompt_generation.clone(),
            false,
            self.current_ai_language(),
        );
        record_ai_agent_trace(
            &self.state,
            self.call.call_id(),
            self.call.domain_id(),
            self.call.caller_adapter_leg_id.as_str(),
            self.call.caller_session_id(),
            "transfer_failed",
            generation,
            json!({
                "operation_id": operation_id,
                "number": number,
                "reason": reason,
                "agent_revision": self.call.ai_agent_revision,
            }),
        );
        if let Some(ai_jobs) = self.state.ai_jobs() {
            ai_jobs.try_send_action_result(ActionResult {
                conversation,
                operation_id,
                generation,
                success: false,
                code: "TRANSFER_FAILED".to_string(),
                message: Some(reason.to_string()),
            })?;
        }
        Ok(())
    }

    pub(super) fn current_ai_language(&self) -> String {
        let Some(agent_id) = self.call.original_ai_agent_id() else {
            return "zh-CN".to_string();
        };
        let config = self.state.config().snapshot();
        config
            .domains
            .get(&voipswitch_core::types::ids::DomainId::from(
                self.call.domain_id().to_string(),
            ))
            .and_then(|domain| {
                domain
                    .ai_agents
                    .iter()
                    .find(|agent| agent.agent_id == agent_id)
            })
            .map(|agent| agent.welcome_language.clone())
            .filter(|language| matches!(language.as_str(), "zh-CN" | "en-US"))
            .unwrap_or_else(|| "zh-CN".to_string())
    }

    fn validate_ai_action(&self, request: &ActionRequested) -> Result<()> {
        request.validate().map_err(anyhow::Error::msg)?;
        anyhow::ensure!(
            request.conversation.conversation_id.as_str() == self.call.call_id(),
            "conversation does not match active call"
        );
        anyhow::ensure!(
            request.conversation.tenant_id.as_str() == self.call.domain_id(),
            "conversation tenant does not match active domain"
        );
        anyhow::ensure!(
            request.generation == request.conversation.generation,
            "action generation mismatch"
        );
        anyhow::ensure!(
            request.deadline_at_ms >= unix_timestamp_ms(),
            "action deadline exceeded"
        );
        anyhow::ensure!(
            self.ai_agent_conversation_active() && self.call.is_answered(),
            "AI action is only valid on an answered AI Agent call"
        );
        anyhow::ensure!(!self.call.is_terminating(), "call is terminating");
        Ok(())
    }

    fn handle_ai_gateway_unavailable(&mut self) -> Result<()> {
        if !self.ai_agent_conversation_active() || self.call.is_terminating() {
            return Ok(());
        }
        let fallback_target = self
            .call
            .ai_fallback_target
            .clone()
            .unwrap_or_else(|| "end_call".to_string());
        let fallback_mode = if fallback_target == "end_call" {
            "end_call"
        } else {
            // Transfer targets remain disabled until the formal multi-leg transfer topology exists.
            warn!(
                call_id = self.call.call_id(),
                fallback_target = %fallback_target,
                "AI fallback target is not available; ending call"
            );
            "unsupported"
        };
        self.call.begin_terminating(HangupCause::TemporaryFailure);
        self.call.hangup_cause = Some(HangupCause::TemporaryFailure);
        self.call.state = CallState::Terminating;
        self.call.callee_terminated = true;
        self.cancel_timer(CallTimerKind::Dial);
        self.cancel_timer(CallTimerKind::Ring);
        self.submit_caller_action(
            "HangupDialog",
            format!("ai-gateway-unavailable-{}", self.call.call_id()),
            json!({"reason": "ai_gateway_unavailable", "fallback": fallback_mode}),
        )?;
        self.state.record_call_trace(CallTraceMessage {
            call_id: self.call.call_id().to_string(),
            domain_id: self.call.domain_id().to_string(),
            sequence: unix_timestamp_ms(),
            observed_at_ms: unix_timestamp_ms(),
            direction: "ai-agent".to_string(),
            adapter_call_leg_id: self.call.caller_adapter_leg_id.as_str().to_string(),
            session_id: Some(self.call.caller_session_id().to_string()),
            source_addr: None,
            destination_addr: None,
            start_line: "AI gateway unavailable fallback".to_string(),
            packet: json!({
                "event": "gateway_unavailable_fallback",
                "fallback_target": fallback_target,
                "fallback_mode": fallback_mode,
                "agent_revision": self.call.ai_agent_revision,
            })
            .to_string(),
        });
        self.start_timer(CallTimerKind::Cleanup);
        self.publish_call_view();
        warn!(
            call_id = self.call.call_id(),
            "AI gateway unavailable; AI call fallback ended call"
        );
        Ok(())
    }

    pub(super) fn ai_agent_conversation_active(&self) -> bool {
        ai_agent_conversation_active(
            &self.call.callee_target,
            self.runtime.pending_ai_transfer.is_some(),
            self.runtime.ai_transfer_failed,
        )
    }

    async fn handle_caller_leg_event(
        &mut self,
        frame_type: &str,
        event: CallLegEvent,
    ) -> Result<()> {
        if !self.accept_leg_event(frame_type, &event) {
            return Ok(());
        }
        match frame_type {
            "InboundInviteCancelled" => self.handle_cancelled(event).await,
            "DialogDisconnected" => self.handle_disconnected(event),
            _ => {
                warn!(
                    call_id = %self.call.call_id(),
                    frame_type,
                    "callee event was routed directly to caller actor"
                );
                Ok(())
            }
        }
    }

    async fn handle_peer_leg_event(&mut self, frame_type: &str, event: CallLegEvent) -> Result<()> {
        if event.session_id.as_deref() != Some(self.call.callee_session_id()) {
            debug!(
                call_id = self.call.call_id(),
                current_session_id = self.call.callee_session_id(),
                event_session_id = ?event.session_id,
                frame_type,
                "stale callee attempt event ignored"
            );
            return Ok(());
        }
        if frame_type == "OutboundProvisional" && self.retry_after_cleanup.is_some() {
            debug!(
                call_id = self.call.call_id(),
                attempt_seq = self.current_attempt_seq,
                "provisional ignored while callee attempt awaits retry cleanup"
            );
            return Ok(());
        }
        if frame_type == "DialogDisconnected"
            && self.answer_in_progress
            && !self.call.is_terminating()
        {
            self.deferred_callee_disconnect = Some(event);
            debug!(
                call_id = self.call.call_id(),
                "callee disconnect deferred until caller answer action is acknowledged"
            );
            return Ok(());
        }
        match frame_type {
            "OutboundProvisional" => self.handle_provisional(event).await,
            "OutboundAnswered" => self.handle_answered(event).await,
            "OutboundFailed" => self.handle_failed(event).await,
            "DialogDisconnected" => self.handle_disconnected(event),
            _ => Ok(()),
        }
    }

    fn accept_leg_event(&mut self, frame_type: &str, event: &CallLegEvent) -> bool {
        if event
            .call_id
            .as_deref()
            .is_some_and(|call_id| call_id != self.call.call_id())
        {
            warn!(
                call_id = %self.call.call_id(),
                event_call_id = ?event.call_id,
                frame_type,
                "caller leg event call id mismatch"
            );
            return false;
        }
        if self
            .leg_event_deduper
            .accept(&event.adapter_call_leg_id, event.leg_event_seq())
        {
            return true;
        }
        debug!(
            call_id = %self.call.call_id(),
            frame_type,
            adapter_call_leg_id = %event.adapter_call_leg_id,
            leg_event_seq = event.leg_event_seq(),
            "duplicate or stale caller leg event ignored"
        );
        false
    }

    fn handle_event_message(&mut self, message: EventMessage) {
        match message {
            EventMessage::QuerySessionSummary(reply) => {
                let source_stats = self.dtmf_source.stats();
                let router_stats = self.dtmf_router.stats();
                let _ = reply.send(Some(json!({
                    "session_id": self.owned_session_id,
                    "call_id": self.call.call_id(),
                    "domain_id": self.call.domain_id(),
                    "role": "call_coordinator",
                    "coordinator_session_id": self.call.aggregate.coordinator().as_str(),
                    "coordinator_generation": self.call.aggregate.coordinator_generation(),
                    "state": self.call.state_str(),
                    "runtime_config_version": self.call.config_snapshot.runtime_config_version,
                    "domain_config_version": self.call.config_snapshot.domain_config_version,
                    "dtmf_accepted_total": source_stats.accepted,
                    "dtmf_duplicate_total": source_stats.duplicate,
                    "dtmf_source_conflict_total": source_stats.conflict,
                    "dtmf_stale_generation_total": source_stats.stale_generation,
                    "dtmf_forwarded_total": router_stats.forwarded,
                    "dtmf_consumed_total": router_stats.consumed,
                    "dtmf_ignored_total": router_stats.ignored,
                    "dtmf_info_send_succeeded_total": router_stats.info_send_succeeded,
                    "dtmf_info_send_failed_total": router_stats.info_send_failed,
                    "dtmf_collector_count": self.dtmf_collectors.len(),
                    "dtmf_collectors": self.dtmf_collectors.values().map(|collector| json!({
                        "collector_id": collector.spec.collector_id.as_str(),
                        "source_session_id": collector.spec.source_session_id.as_str(),
                        "mode": format!("{:?}", collector.spec.mode).to_ascii_lowercase(),
                        "phase": format!("{:?}", collector.phase).to_ascii_lowercase(),
                    })).collect::<Vec<_>>(),
                })));
            }
            EventMessage::StartDigitCollection {
                spec,
                ready_reply,
                result_reply,
            } => self.start_digit_collection(*spec, ready_reply, result_reply),
            EventMessage::CancelDigitCollection {
                collector_id,
                reply,
            } => {
                let _ = reply.send(self.cancel_digit_collection(&collector_id));
            }
        }
    }

    fn handle_source_digit(&mut self, event: DigitEvent) -> Result<()> {
        if !valid_source_event(
            &event,
            self.call.domain_id(),
            self.call.call_id(),
            &self.owned_session_id,
        ) {
            warn!(
                call_id = self.call.call_id(),
                owned_session_id = self.owned_session_id,
                source_session_id = event.source_session_id.as_str(),
                "misrouted coordinator owner DTMF event ignored"
            );
            return Ok(());
        }
        match self.dtmf_source.accept(&event) {
            DtmfSourceDecision::Accepted => self.route_digit(event)?,
            decision => {
                debug!(
                    call_id = self.call.call_id(),
                    source_session_id = event.source_session_id.as_str(),
                    ?decision,
                    "caller DTMF event rejected by source arbitration"
                );
            }
        }
        Ok(())
    }

    fn handle_peer_digit(&mut self, event: DigitEvent) -> Result<()> {
        if event.domain_id.as_str() != self.call.domain_id()
            || event.call_id.as_str() != self.call.call_id()
        {
            warn!(
                call_id = self.call.call_id(),
                source_session_id = event.source_session_id.as_str(),
                "foreign peer DTMF event ignored"
            );
            return Ok(());
        }
        self.route_digit(event)
    }

    fn route_digit(&mut self, event: DigitEvent) -> Result<()> {
        if self.collect_digit(&event) {
            self.dtmf_router.record_consumed();
            self.publish_call_view();
            return Ok(());
        }
        let caller_session_id = self.call.caller_session_id().to_string();
        let callee_session_id = self.call.callee_session_id().to_string();
        let routable = !self.call.is_terminating();
        let disposition = self.dtmf_router.route(
            &event,
            &caller_session_id,
            Some(&callee_session_id),
            routable,
        );
        match disposition {
            DtmfDisposition::Forward { peer_session_id }
                if matches!(
                    event.transport,
                    DtmfTransport::SipInfoRelay | DtmfTransport::SipInfoDtmf
                ) =>
            {
                let action_id = match event.event_id {
                    DtmfEventId::SipInfo {
                        dialog_generation,
                        cseq,
                        ..
                    } => format!(
                        "dtmf-info-{}-{dialog_generation}-{cseq}",
                        self.call.call_id()
                    ),
                    _ => return Ok(()),
                };
                self.submit_caller_action(
                    "SendDtmfInfo",
                    action_id,
                    json!({
                        "target_session_id": peer_session_id,
                        "digit": event.digit,
                        "duration_ms": event.duration_ms,
                        "content_mode": event.transport,
                    }),
                )?;
                debug!(
                    call_id = self.call.call_id(),
                    source_session_id = event.source_session_id.as_str(),
                    peer_session_id = peer_session_id.as_str(),
                    transport = ?event.transport,
                    "DTMF INFO forwarding requested"
                );
            }
            DtmfDisposition::Forward { peer_session_id } => debug!(
                call_id = self.call.call_id(),
                source_session_id = event.source_session_id.as_str(),
                peer_session_id = peer_session_id.as_str(),
                transport = ?event.transport,
                "DTMF event follows existing transparent media bridge"
            ),
            DtmfDisposition::Ignore { reason } => debug!(
                call_id = self.call.call_id(),
                source_session_id = event.source_session_id.as_str(),
                ?reason,
                "DTMF event ignored by call router"
            ),
        }
        Ok(())
    }

    fn handle_dtmf_info_send_result(&mut self, result: DtmfInfoSendResult) -> Result<()> {
        if result.call_id != self.call.call_id()
            || result.session_id != self.call.caller_session_id()
            || result.generation != self.action_generation
        {
            debug!(
                call_id = self.call.call_id(),
                result_call_id = result.call_id,
                result_session_id = result.session_id,
                action_id = result.action_id,
                generation = result.generation,
                "stale DTMF INFO send result ignored"
            );
            return Ok(());
        }
        self.dtmf_router.record_info_send_result(result.succeeded);
        if result.succeeded {
            info!(
                call_id = self.call.call_id(),
                adapter_call_leg_id = result.adapter_call_leg_id,
                action_id = result.action_id,
                status_code = ?result.status_code,
                "DTMF INFO forwarding completed"
            );
        } else {
            warn!(
                call_id = self.call.call_id(),
                adapter_call_leg_id = result.adapter_call_leg_id,
                action_id = result.action_id,
                status_code = ?result.status_code,
                reason = result.reason,
                "DTMF INFO forwarding failed"
            );
        }
        self.publish_call_view();
        Ok(())
    }

    pub(super) fn submit_caller_action(
        &self,
        action_kind: &str,
        action_id: String,
        body: Value,
    ) -> Result<()> {
        self.submit_action(
            self.call.caller_session_id().to_string(),
            self.coordinator_handle.weak_control_sender(),
            action_kind,
            action_id,
            body,
        )
    }

    pub(super) fn submit_callee_action(
        &self,
        action_kind: &str,
        action_id: String,
        body: Value,
    ) -> Result<()> {
        self.submit_action(
            self.call.callee_session_id().to_string(),
            self.callee_control_tx.downgrade(),
            action_kind,
            action_id,
            body,
        )
    }

    fn submit_action(
        &self,
        session_id: String,
        target: mpsc::WeakSender<ControlMessage>,
        action_kind: &str,
        action_id: String,
        body: Value,
    ) -> Result<()> {
        self.action_executor.submit(
            ActionIdentity {
                call_id: self.call.call_id().to_string(),
                session_id,
                action_kind: action_kind.to_string(),
                action_id,
                generation: self.action_generation,
            },
            self.call.domain_id(),
            body,
            target,
        )
    }

    fn handle_action_ack(&mut self, ack: CallActionAck) -> Result<()> {
        let identity = ack.identity();
        if ack.accepted() {
            if !self.accept_action_result(&identity) {
                return Ok(());
            }
            debug!(
                call_id = identity.call_id,
                session_id = identity.session_id,
                action_kind = identity.action_kind,
                action_id = identity.action_id,
                generation = identity.generation,
                status = ?ack.status,
                "adapter call action acknowledged"
            );
            if identity.action_kind == "AnswerInboundInvite" {
                self.confirm_answer(&identity.action_id)?;
            }
            return Ok(());
        }
        let reason = ack
            .result
            .get("message")
            .and_then(Value::as_str)
            .or_else(|| ack.result.get("original_status").and_then(Value::as_str))
            .unwrap_or("adapter rejected call action")
            .to_string();
        self.handle_action_failure(ActionDeliveryFailed { identity, reason }, false)
    }

    fn handle_owned_leg_action_ack(&mut self, ack: CallActionAck) -> Result<()> {
        let identity = ack.identity();
        if identity.call_id != self.call.call_id()
            || identity.session_id != self.owned_session_id
            || identity.generation != self.owned_action_generation
            || !self
                .owned_completed_actions
                .insert((identity.action_id.clone(), identity.generation))
        {
            debug!(
                call_id = self.call.call_id(),
                session_id = self.owned_session_id,
                action_id = identity.action_id,
                generation = identity.generation,
                current_generation = self.owned_action_generation,
                "stale or duplicate coordinator owner leg action result ignored"
            );
            return Ok(());
        }
        if ack.accepted() {
            debug!(
                call_id = identity.call_id,
                session_id = identity.session_id,
                action_kind = identity.action_kind,
                action_id = identity.action_id,
                generation = identity.generation,
                status = ?ack.status,
                "coordinator owner leg action acknowledged"
            );
            return Ok(());
        }
        let reason = ack
            .result
            .get("message")
            .and_then(Value::as_str)
            .or_else(|| ack.result.get("original_status").and_then(Value::as_str))
            .unwrap_or("adapter rejected owner leg action")
            .to_string();
        self.handle_action_failure(ActionDeliveryFailed { identity, reason }, true)
    }

    fn handle_action_failure(
        &mut self,
        failure: ActionDeliveryFailed,
        peer_failure: bool,
    ) -> Result<()> {
        if !peer_failure && !self.accept_action_result(&failure.identity) {
            return Ok(());
        }
        warn!(
            call_id = failure.identity.call_id,
            session_id = failure.identity.session_id,
            action_kind = failure.identity.action_kind,
            action_id = failure.identity.action_id,
            generation = failure.identity.generation,
            reason = failure.reason,
            "call action failed"
        );
        if failure.identity.action_kind == "SendDtmfInfo" {
            self.dtmf_router.record_info_send_result(false);
            self.publish_call_view();
            return Ok(());
        }
        if peer_failure
            && failure.identity.action_kind.starts_with("Originate")
            && self.runtime.pending_ai_transfer.is_some()
        {
            self.fail_ai_transfer(format!("originate_failed:{}", failure.reason))?;
            return Ok(());
        }
        if peer_failure
            && failure.identity.action_kind.starts_with("Originate")
            && self.retry_after_originate_failure()?
        {
            return Ok(());
        }
        if self.call.is_terminating() {
            return Ok(());
        }
        self.call.begin_terminating(HangupCause::InternalError);
        self.call.state = CallState::Terminating;
        self.call.last_status.get_or_insert(503);
        self.call.hangup_cause = Some(HangupCause::InternalError);
        self.cancel_timer(CallTimerKind::Dial);
        self.cancel_timer(CallTimerKind::Ring);
        if peer_failure {
            self.call.callee_terminated = true;
        }
        if failure.identity.action_kind.starts_with("Originate") {
            self.submit_caller_action(
                "RejectInboundInvite",
                format!("reject-action-failure-{}", self.call.call_id()),
                json!({
                    "adapter_call_leg_id": self.call.caller_adapter_leg_id,
                    "status_code": 503,
                }),
            )?;
        }
        self.start_timer(crate::runtime::call::timer::CallTimerKind::Cleanup);
        self.publish_call_view();
        Ok(())
    }

    fn accept_action_result(&mut self, identity: &ActionIdentity) -> bool {
        if identity.call_id != self.call.call_id()
            || identity.session_id != self.call.caller_session_id()
            || identity.generation != self.action_generation
        {
            debug!(
                call_id = self.call.call_id(),
                result_call_id = identity.call_id,
                session_id = identity.session_id,
                action_id = identity.action_id,
                generation = identity.generation,
                current_generation = self.action_generation,
                "stale or misrouted caller action result ignored"
            );
            return false;
        }
        self.completed_actions
            .insert((identity.action_id.clone(), identity.generation))
    }
}

/// Owns one outbound leg. It never mutates the caller or call aggregate;
/// accepted leg facts are posted to the caller coordinator.
pub(crate) struct CalleeSessionActor {
    pub(crate) session: Session,
    pub(crate) attempt_seq: u16,
    pub(crate) leg_event_deduper: LegEventDeduper,
    pub(crate) control_rx: mpsc::Receiver<ControlMessage>,
    pub(crate) event_rx: mpsc::Receiver<EventMessage>,
    pub(crate) coordinator_handle: CallCoordinatorHandle,
    pub(crate) control_dispatcher: CriticalControlDispatcher,
    pub(crate) action_generation: u64,
    pub(crate) completed_actions: HashSet<(String, u64)>,
    pub(crate) dtmf_source: DtmfSourceState,
}

fn dtmf_digits_to_string(digits: &[DtmfDigit]) -> String {
    digits
        .iter()
        .map(|digit| match digit {
            DtmfDigit::D0 => '0',
            DtmfDigit::D1 => '1',
            DtmfDigit::D2 => '2',
            DtmfDigit::D3 => '3',
            DtmfDigit::D4 => '4',
            DtmfDigit::D5 => '5',
            DtmfDigit::D6 => '6',
            DtmfDigit::D7 => '7',
            DtmfDigit::D8 => '8',
            DtmfDigit::D9 => '9',
            DtmfDigit::Star => '*',
            DtmfDigit::Pound => '#',
            DtmfDigit::A => 'A',
            DtmfDigit::B => 'B',
            DtmfDigit::C => 'C',
            DtmfDigit::D => 'D',
            DtmfDigit::Flash => 'F',
        })
        .collect()
}

fn ai_agent_conversation_active(
    callee_target: &str,
    transfer_pending: bool,
    transfer_failed: bool,
) -> bool {
    callee_target.starts_with("ai_agent:") || transfer_pending || transfer_failed
}

impl CalleeSessionActor {
    fn session_id(&self) -> &str {
        self.session.id.as_str()
    }

    fn call_id(&self) -> &str {
        self.session.call_id.as_str()
    }

    pub(crate) async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                message = self.control_rx.recv() => {
                    let Some(message) = message else { break; };
                    if let ControlMessage::PrepareCoordinatorTarget { token, reply } = message {
                        if self.call_id() != token.call_id
                            || self.session_id() != token.target_session_id
                            || self.session.state.is_terminal()
                        {
                            let _ = reply.send(Err(
                                "coordinator_handoff_target_unavailable".to_string(),
                            ));
                            continue;
                        }
                        info!(
                            call_id = token.call_id,
                            session_id = token.target_session_id,
                            "target actor ready for coordinator handoff"
                        );
                        match reply.send(Ok(self)) {
                            Ok(()) => return,
                            Err(Ok(actor)) => {
                                self = actor;
                                continue;
                            }
                            Err(Err(_)) => unreachable!("target actor was ready"),
                        }
                    }
                    if !self.handle_control(message).await {
                        break;
                    }
                }
                message = self.event_rx.recv() => {
                    let Some(message) = message else { break; };
                    self.handle_event_message(message);
                }
            }
        }
        debug!(
            call_id = self.call_id(),
            session_id = self.session_id(),
            state = ?self.session.state,
            "callee session actor stopped"
        );
    }

    async fn handle_control(&mut self, message: ControlMessage) -> bool {
        match message {
            ControlMessage::LegEvent(frame_type, event) => {
                if !self.accept_leg_event(&frame_type, &event) {
                    return true;
                }
                self.apply_leg_state(&frame_type, &event);
                let terminal = self.session.state.is_terminal();
                if self
                    .control_dispatcher
                    .dispatch_to(
                        self.session_id(),
                        self.coordinator_handle.control_sender(),
                        ControlMessage::PeerLegEvent(frame_type, event),
                    )
                    .await
                    .is_err()
                {
                    return false;
                }
                !terminal
            }
            ControlMessage::CallActionAck(ack) => self.handle_action_ack(ack).await,
            ControlMessage::ActionDeliveryFailed(failure) => {
                self.forward_action_failure(failure).await
            }
            ControlMessage::MediaDtmfObserved(event) => self.handle_source_digit(event).await,
            ControlMessage::SipDtmfReceived(event) => self.handle_source_digit(event).await,
            ControlMessage::Shutdown | ControlMessage::ForceCleanup => false,
            ControlMessage::PrepareCoordinatorHandoff { .. }
            | ControlMessage::PrepareCoordinatorTarget { .. } => {
                warn!(
                    call_id = self.call_id(),
                    session_id = self.session_id(),
                    "coordinator handoff control delivered through the wrong mailbox"
                );
                true
            }
            ControlMessage::PeerLegEvent(_, _)
            | ControlMessage::PeerActionFailed(_)
            | ControlMessage::MediaActionResult(_)
            | ControlMessage::PeerDigitEvent(_)
            | ControlMessage::DtmfInfoSendResult(_)
            | ControlMessage::AttemptRegistrationResult(_)
            | ControlMessage::AiActionRequested(_)
            | ControlMessage::AiGatewayUnavailable
            | ControlMessage::CallFinalized => {
                warn!(
                    call_id = self.call_id(),
                    session_id = self.session_id(),
                    "unsupported control message delivered to callee actor"
                );
                true
            }
        }
    }

    fn accept_leg_event(&mut self, frame_type: &str, event: &CallLegEvent) -> bool {
        if event
            .call_id
            .as_deref()
            .is_some_and(|call_id| call_id != self.call_id())
        {
            warn!(
                call_id = self.call_id(),
                event_call_id = ?event.call_id,
                session_id = self.session_id(),
                frame_type,
                "callee leg event call id mismatch"
            );
            return false;
        }
        if self
            .leg_event_deduper
            .accept(&event.adapter_call_leg_id, event.leg_event_seq())
        {
            return true;
        }
        debug!(
            call_id = self.call_id(),
            session_id = self.session_id(),
            frame_type,
            adapter_call_leg_id = event.adapter_call_leg_id,
            leg_event_seq = event.leg_event_seq(),
            "duplicate or stale callee leg event ignored"
        );
        false
    }

    fn apply_leg_state(&mut self, frame_type: &str, event: &CallLegEvent) {
        self.session.state = match frame_type {
            "OutboundProvisional" => LegState::WaitForAnswer,
            "OutboundAnswered" => LegState::Bridged,
            "OutboundFailed" | "DialogDisconnected" => LegState::Destroyed,
            _ => self.session.state,
        };
        debug!(
            call_id = self.call_id(),
            session_id = self.session_id(),
            frame_type,
            status = ?event.status_code,
            state = ?self.session.state,
            "callee leg state advanced"
        );
    }

    fn handle_event_message(&mut self, message: EventMessage) {
        match message {
            EventMessage::QuerySessionSummary(reply) => {
                let source_stats = self.dtmf_source.stats();
                let _ = reply.send(Some(json!({
                    "session_id": self.session_id(),
                    "call_id": self.call_id(),
                    "role": "callee",
                    "attempt_seq": self.attempt_seq,
                    "state": self.session.state,
                    "dtmf_accepted_total": source_stats.accepted,
                    "dtmf_duplicate_total": source_stats.duplicate,
                    "dtmf_source_conflict_total": source_stats.conflict,
                    "dtmf_stale_generation_total": source_stats.stale_generation,
                })));
            }
            EventMessage::StartDigitCollection {
                spec,
                ready_reply,
                result_reply,
            } => {
                let _ = ready_reply.send(Err("collector_requires_call_coordinator".to_string()));
                let _ = result_reply.send(crate::runtime::call::dtmf::DigitCollectionOutcome {
                    collector_id: spec.collector_id,
                    code: crate::runtime::call::dtmf::DigitCollectionResultCode::Failed,
                    digits: Vec::new(),
                    reason: Some("collector_requires_call_coordinator".to_string()),
                });
            }
            EventMessage::CancelDigitCollection { reply, .. } => {
                let _ = reply.send(Err("collector_requires_call_coordinator".to_string()));
            }
        }
    }

    async fn handle_source_digit(&mut self, event: DigitEvent) -> bool {
        if !valid_source_event(
            &event,
            self.session.domain_id.as_str(),
            self.call_id(),
            self.session_id(),
        ) {
            warn!(
                call_id = self.call_id(),
                session_id = self.session_id(),
                source_session_id = event.source_session_id.as_str(),
                "misrouted callee DTMF event ignored"
            );
            return true;
        }
        match self.dtmf_source.accept(&event) {
            DtmfSourceDecision::Accepted => self
                .control_dispatcher
                .dispatch_to(
                    self.session_id(),
                    self.coordinator_handle.control_sender(),
                    ControlMessage::PeerDigitEvent(event),
                )
                .await
                .is_ok(),
            decision => {
                debug!(
                    call_id = self.call_id(),
                    session_id = self.session_id(),
                    ?decision,
                    "callee DTMF event rejected by source arbitration"
                );
                true
            }
        }
    }

    async fn handle_action_ack(&mut self, ack: CallActionAck) -> bool {
        let identity = ack.identity();
        if !self.accept_action_result(&identity) {
            return true;
        }
        if ack.accepted() {
            debug!(
                call_id = identity.call_id,
                session_id = identity.session_id,
                action_kind = identity.action_kind,
                action_id = identity.action_id,
                generation = identity.generation,
                status = ?ack.status,
                "adapter call action acknowledged"
            );
            return true;
        }
        let reason = ack
            .result
            .get("message")
            .and_then(Value::as_str)
            .or_else(|| ack.result.get("original_status").and_then(Value::as_str))
            .unwrap_or("adapter rejected call action")
            .to_string();
        self.forward_action_failure(ActionDeliveryFailed { identity, reason })
            .await
    }

    async fn forward_action_failure(&mut self, failure: ActionDeliveryFailed) -> bool {
        if failure.identity.call_id != self.call_id()
            || failure.identity.session_id != self.session_id()
            || failure.identity.generation != self.action_generation
        {
            debug!(
                call_id = self.call_id(),
                session_id = self.session_id(),
                action_id = failure.identity.action_id,
                generation = failure.identity.generation,
                current_generation = self.action_generation,
                "stale or misrouted callee action result ignored"
            );
            return true;
        }
        self.session.state = LegState::Destroyed;
        let _ = self
            .control_dispatcher
            .dispatch_to(
                self.session_id(),
                self.coordinator_handle.control_sender(),
                ControlMessage::PeerActionFailed(failure),
            )
            .await;
        false
    }

    fn accept_action_result(&mut self, identity: &ActionIdentity) -> bool {
        if identity.call_id != self.call_id()
            || identity.session_id != self.session_id()
            || identity.generation != self.action_generation
        {
            debug!(
                call_id = self.call_id(),
                session_id = self.session_id(),
                result_call_id = identity.call_id,
                result_session_id = identity.session_id,
                action_id = identity.action_id,
                generation = identity.generation,
                current_generation = self.action_generation,
                "stale or misrouted callee action result ignored"
            );
            return false;
        }
        self.completed_actions
            .insert((identity.action_id.clone(), identity.generation))
    }
}

pub(crate) fn spawn_builtin_prompt(
    media: crate::runtime::media::MediaBridgeHandle,
    generation: Arc<AtomicU64>,
    looping: bool,
    language: String,
) {
    tokio::spawn(async move {
        let wav: &[u8] = match (language.as_str(), looping) {
            ("en-US", true) => {
                include_bytes!("../../../assets/prompts/en-US/transfer_waiting.wav")
            }
            ("en-US", false) => {
                include_bytes!("../../../assets/prompts/en-US/transfer_failed.wav")
            }
            (_, true) => include_bytes!("../../../assets/prompts/zh-CN/transfer_waiting.wav"),
            (_, false) => include_bytes!("../../../assets/prompts/zh-CN/transfer_failed.wav"),
        };
        let Some(pcm) = wav_pcm_payload(wav) else {
            warn!("invalid built-in PBX prompt WAV");
            return;
        };
        let start_generation = generation.load(Ordering::Acquire);
        let mut sequence = 40_000_u16;
        let mut timestamp = 0_u32;
        let ssrc = 0x5653_5058_u32;
        loop {
            if generation.load(Ordering::Acquire) != start_generation || media.is_stopped() {
                return;
            }
            for chunk in pcm.chunks(160 * 2) {
                if generation.load(Ordering::Acquire) != start_generation || media.is_stopped() {
                    return;
                }
                let payload: Vec<u8> = chunk
                    .chunks_exact(2)
                    .map(|sample| pcm16_to_alaw(i16::from_le_bytes([sample[0], sample[1]])))
                    .collect();
                if media
                    .send_tts_rtp(&payload, 8, timestamp, sequence, ssrc)
                    .await
                    .is_err()
                {
                    return;
                }
                sequence = sequence.wrapping_add(1);
                timestamp = timestamp.wrapping_add(160);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            if !looping {
                media.set_ai_caller_input_enabled(true);
                return;
            }
        }
    });
}

fn wav_pcm_payload(wav: &[u8]) -> Option<&[u8]> {
    if wav.get(0..4)? != b"RIFF" || wav.get(8..12)? != b"WAVE" {
        return None;
    }
    let mut offset: usize = 12;
    while offset.checked_add(8)? <= wav.len() {
        let id = &wav[offset..offset + 4];
        let size = u32::from_le_bytes(wav[offset + 4..offset + 8].try_into().ok()?) as usize;
        let end = offset.checked_add(8)?.checked_add(size)?.min(wav.len());
        if id == b"data" {
            return Some(&wav[offset + 8..end]);
        }
        offset = end + (size & 1);
    }
    None
}

fn pcm16_to_alaw(sample: i16) -> u8 {
    let value = i32::from(sample).abs().min(32_635);
    let sign = if sample >= 0 { 0x80 } else { 0 };
    let exponent = if value < 256 {
        0
    } else {
        ((31 - (value as u32).leading_zeros()) as i32 - 7).min(7)
    };
    let mantissa = if exponent == 0 {
        (value >> 4) & 0x0f
    } else {
        (value >> (exponent + 3)) & 0x0f
    };
    (sign | ((exponent << 4) | mantissa) as u8) ^ 0x55
}

fn valid_source_event(
    event: &DigitEvent,
    domain_id: &str,
    call_id: &str,
    session_id: &str,
) -> bool {
    event.identity_is_consistent()
        && event.domain_id.as_str() == domain_id
        && event.call_id.as_str() == call_id
        && event.source_session_id.as_str() == session_id
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::call::event::CallActionAckStatus;
    use crate::runtime::call::session::{
        CallCoordinatorHandle, CoordinatorHandoffToken, CriticalControlDispatcher, SessionHandle,
    };
    use voipswitch_core::dtmf::{DtmfDigit, DtmfEventId, DtmfSourceGeneration, DtmfTransport};
    use voipswitch_core::session::{LegRole, Session, SessionEndpoint, SessionVars};
    use voipswitch_core::types::call::{LegDirection, LegState};
    use voipswitch_core::types::ids::{CallId, CalleeAttemptId, DomainId, SessionId};

    fn test_callee_session(session_id: &str, call_id: &str) -> Session {
        Session {
            id: SessionId::from(session_id),
            domain_id: DomainId::from("domain-a"),
            call_id: CallId::from(call_id),
            role: LegRole::Callee {
                attempt_id: CalleeAttemptId::from("attempt-1"),
            },
            direction: LegDirection::Outbound,
            endpoint: SessionEndpoint::External {
                number: "1001".to_string(),
            },
            state: LegState::Dialing,
            sip: None,
            peer: None,
            media: None,
            variables: SessionVars::new(),
            created_at_ms: 1,
            answered_at_ms: None,
            hangup_cause: None,
        }
    }
    use tokio::time::{Duration, timeout};

    fn leg_event(sequence: u64) -> CallLegEvent {
        CallLegEvent {
            call_id: Some("call-a".to_string()),
            session_id: Some("callee-a".to_string()),
            adapter_call_leg_id: "leg-a".to_string(),
            leg_event_seq: sequence,
            status_code: Some(180),
            sdp: None,
            reason: None,
        }
    }

    fn digit_event() -> DigitEvent {
        let source_session_id = SessionId::from("callee-a");
        DigitEvent {
            event_id: DtmfEventId::Rfc4733 {
                media_generation: 1,
                source_session_id: source_session_id.clone(),
                ssrc: 7,
                timestamp: 42,
                event_code: 5,
            },
            domain_id: DomainId::from("domain-a"),
            call_id: CallId::from("call-a"),
            source_session_id,
            source_media_leg_id: None,
            digit: DtmfDigit::D5,
            transport: DtmfTransport::Rfc4733,
            duration_ms: 100,
            observed_at_ms: 1,
            source_generation: DtmfSourceGeneration::Media(1),
            incomplete_end: false,
        }
    }

    #[test]
    fn failed_transfer_keeps_ai_conversation_active_until_a_transfer_connects() {
        assert!(ai_agent_conversation_active(
            "ai_agent:frontdesk",
            false,
            false
        ));
        assert!(ai_agent_conversation_active("ext:1002", true, false));
        assert!(ai_agent_conversation_active("ext:1002", false, true));
        assert!(!ai_agent_conversation_active("ext:1002", false, false));
    }

    #[tokio::test]
    async fn callee_actor_advances_its_leg_before_forwarding_to_caller() {
        let (callee_handle, control_rx, event_rx) = SessionHandle::new_pair();
        let (coordinator_handle, mut coordinator_mailbox) =
            CallCoordinatorHandle::new_pair("call-a");
        let (control_dispatcher, dispatcher_task) = CriticalControlDispatcher::spawn();
        let actor = CalleeSessionActor {
            session: test_callee_session("callee-a", "call-a"),
            attempt_seq: 1,
            leg_event_deduper: LegEventDeduper::default(),
            control_rx,
            event_rx,
            coordinator_handle,
            control_dispatcher: control_dispatcher.clone(),
            action_generation: 1,
            completed_actions: HashSet::new(),
            dtmf_source: DtmfSourceState::default(),
        };
        let actor_task = tokio::spawn(actor.run());

        callee_handle
            .control_sender()
            .send(ControlMessage::LegEvent(
                "OutboundProvisional".to_string(),
                leg_event(1),
            ))
            .await
            .expect("post provisional");

        let forwarded = timeout(
            Duration::from_secs(1),
            coordinator_mailbox.control_rx.recv(),
        )
        .await
        .expect("callee forwarded event")
        .expect("caller mailbox open");
        assert!(matches!(
            forwarded,
            ControlMessage::PeerLegEvent(frame_type, _) if frame_type == "OutboundProvisional"
        ));

        callee_handle
            .control_sender()
            .send(ControlMessage::Shutdown)
            .await
            .expect("stop callee actor");
        actor_task.await.expect("callee actor joined");
        drop(control_dispatcher);
        dispatcher_task.await.expect("dispatcher joined");
    }

    #[tokio::test]
    async fn callee_actor_drops_duplicate_leg_events() {
        let (callee_handle, control_rx, event_rx) = SessionHandle::new_pair();
        let (coordinator_handle, mut coordinator_mailbox) =
            CallCoordinatorHandle::new_pair("call-a");
        let (control_dispatcher, dispatcher_task) = CriticalControlDispatcher::spawn();
        let actor = CalleeSessionActor {
            session: test_callee_session("callee-a", "call-a"),
            attempt_seq: 1,
            leg_event_deduper: LegEventDeduper::default(),
            control_rx,
            event_rx,
            coordinator_handle,
            control_dispatcher: control_dispatcher.clone(),
            action_generation: 1,
            completed_actions: HashSet::new(),
            dtmf_source: DtmfSourceState::default(),
        };
        let actor_task = tokio::spawn(actor.run());
        let control_tx = callee_handle.control_sender();

        control_tx
            .send(ControlMessage::LegEvent(
                "OutboundProvisional".to_string(),
                leg_event(1),
            ))
            .await
            .expect("post first event");
        control_tx
            .send(ControlMessage::LegEvent(
                "OutboundProvisional".to_string(),
                leg_event(1),
            ))
            .await
            .expect("post duplicate event");

        timeout(
            Duration::from_secs(1),
            coordinator_mailbox.control_rx.recv(),
        )
        .await
        .expect("first event forwarded")
        .expect("caller mailbox open");
        assert!(
            timeout(
                Duration::from_millis(50),
                coordinator_mailbox.control_rx.recv(),
            )
            .await
            .is_err()
        );

        control_tx
            .send(ControlMessage::Shutdown)
            .await
            .expect("stop callee actor");
        actor_task.await.expect("callee actor joined");
        drop(control_dispatcher);
        dispatcher_task.await.expect("dispatcher joined");
    }

    #[tokio::test]
    async fn callee_actor_quiesce_moves_receiver_with_queued_events() {
        let (callee_handle, control_rx, event_rx) = SessionHandle::new_pair();
        let (coordinator_handle, _coordinator_mailbox) = CallCoordinatorHandle::new_pair("call-a");
        let (control_dispatcher, dispatcher_task) = CriticalControlDispatcher::spawn();
        let actor = CalleeSessionActor {
            session: test_callee_session("callee-a", "call-a"),
            attempt_seq: 1,
            leg_event_deduper: LegEventDeduper::default(),
            control_rx,
            event_rx,
            coordinator_handle,
            control_dispatcher: control_dispatcher.clone(),
            action_generation: 1,
            completed_actions: HashSet::new(),
            dtmf_source: DtmfSourceState::default(),
        };
        let actor_task = tokio::spawn(actor.run());
        let (reply, ready) = tokio::sync::oneshot::channel();
        callee_handle
            .control_sender()
            .send(ControlMessage::PrepareCoordinatorTarget {
                token: CoordinatorHandoffToken {
                    call_id: "call-a".to_string(),
                    source_session_id: "caller-a".to_string(),
                    target_session_id: "callee-a".to_string(),
                    source_generation: 1,
                    target_generation: 2,
                },
                reply,
            })
            .await
            .expect("request target readiness");
        callee_handle
            .control_sender()
            .send(ControlMessage::LegEvent(
                "OutboundProvisional".to_string(),
                leg_event(7),
            ))
            .await
            .expect("queue event behind handoff request");

        let mut actor = timeout(Duration::from_secs(1), ready)
            .await
            .expect("target readiness timeout")
            .expect("target readiness channel")
            .expect("target ready");
        actor_task.await.expect("quiesced actor joined");
        assert!(matches!(
            actor.control_rx.try_recv(),
            Ok(ControlMessage::LegEvent(frame_type, event))
                if frame_type == "OutboundProvisional" && event.leg_event_seq() == 7
        ));
        assert_eq!(actor.session_id(), "callee-a");

        drop(actor);
        drop(callee_handle);
        drop(control_dispatcher);
        dispatcher_task.await.expect("dispatcher joined");
    }

    #[tokio::test]
    async fn callee_actor_arbitrates_and_forwards_digit_once() {
        let (callee_handle, control_rx, event_rx) = SessionHandle::new_pair();
        let (coordinator_handle, mut coordinator_mailbox) =
            CallCoordinatorHandle::new_pair("call-a");
        let (control_dispatcher, dispatcher_task) = CriticalControlDispatcher::spawn();
        let actor = CalleeSessionActor {
            session: test_callee_session("callee-a", "call-a"),
            attempt_seq: 1,
            leg_event_deduper: LegEventDeduper::default(),
            control_rx,
            event_rx,
            coordinator_handle,
            control_dispatcher: control_dispatcher.clone(),
            action_generation: 1,
            completed_actions: HashSet::new(),
            dtmf_source: DtmfSourceState::default(),
        };
        let actor_task = tokio::spawn(actor.run());
        let event = digit_event();
        callee_handle
            .control_sender()
            .send(ControlMessage::MediaDtmfObserved(event.clone()))
            .await
            .unwrap();
        callee_handle
            .control_sender()
            .send(ControlMessage::MediaDtmfObserved(event))
            .await
            .unwrap();

        let forwarded = timeout(
            Duration::from_secs(1),
            coordinator_mailbox.control_rx.recv(),
        )
        .await
        .expect("digit forwarded")
        .expect("caller mailbox open");
        assert!(matches!(
            forwarded,
            ControlMessage::PeerDigitEvent(event)
                if event.source_session_id.as_str() == "callee-a"
        ));
        assert!(
            timeout(
                Duration::from_millis(50),
                coordinator_mailbox.control_rx.recv(),
            )
            .await
            .is_err()
        );

        callee_handle
            .control_sender()
            .send(ControlMessage::Shutdown)
            .await
            .unwrap();
        actor_task.await.unwrap();
        drop(control_dispatcher);
        dispatcher_task.await.unwrap();
    }

    #[tokio::test]
    async fn callee_actor_forwards_rejected_originate_ack_once() {
        let (callee_handle, control_rx, event_rx) = SessionHandle::new_pair();
        let (coordinator_handle, mut coordinator_mailbox) =
            CallCoordinatorHandle::new_pair("call-a");
        let (control_dispatcher, dispatcher_task) = CriticalControlDispatcher::spawn();
        let actor = CalleeSessionActor {
            session: test_callee_session("callee-a", "call-a"),
            attempt_seq: 1,
            leg_event_deduper: LegEventDeduper::default(),
            control_rx,
            event_rx,
            coordinator_handle,
            control_dispatcher: control_dispatcher.clone(),
            action_generation: 1,
            completed_actions: HashSet::new(),
            dtmf_source: DtmfSourceState::default(),
        };
        let actor_task = tokio::spawn(actor.run());
        callee_handle
            .control_sender()
            .send(ControlMessage::CallActionAck(CallActionAck {
                call_id: "call-a".to_string(),
                session_id: "callee-a".to_string(),
                action_kind: "OriginateEndpoint".to_string(),
                action_id: "originate-call-a".to_string(),
                generation: 1,
                status: CallActionAckStatus::Rejected,
                result: json!({ "message": "endpoint_unregistered" }),
            }))
            .await
            .expect("post rejected ack");

        let forwarded = timeout(
            Duration::from_secs(1),
            coordinator_mailbox.control_rx.recv(),
        )
        .await
        .expect("failure forwarded")
        .expect("caller mailbox open");
        assert!(matches!(
            forwarded,
            ControlMessage::PeerActionFailed(ActionDeliveryFailed { identity, .. })
                if identity.action_id == "originate-call-a"
        ));
        actor_task.await.expect("callee actor joined");
        drop(callee_handle);
        drop(control_dispatcher);
        dispatcher_task.await.expect("dispatcher joined");
    }
}
