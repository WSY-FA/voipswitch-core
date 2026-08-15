use crate::runtime::call::actor::SessionActor;
use anyhow::Result;
use serde_json::json;
use std::{future::pending, time::Duration};
use tokio::time::Instant;
use tracing::{info, warn};
use voipswitch_core::types::call::{CallState, HangupCause};

pub(crate) const DEFAULT_ROUTE_TIMEOUT_MS: u64 = 3_000;
pub(crate) const DEFAULT_DIAL_TIMEOUT_MS: u64 = 5_000;
pub(crate) const DEFAULT_RING_TIMEOUT_MS: u64 = 60_000;
pub(crate) const DEFAULT_CLEANUP_TIMEOUT_MS: u64 = 5_000;
pub(crate) const DEFAULT_CALL_ROUTE_BUDGET_MS: u64 = 180_000;
pub(crate) const DEFAULT_MIN_ATTEMPT_WINDOW_MS: u64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CallTimerKind {
    Dial,
    Ring,
    Cleanup,
}

#[derive(Debug, Clone)]
pub(crate) struct CallTimerEvent {
    pub(crate) call_id: String,
    pub(crate) kind: CallTimerKind,
    pub(crate) generation: u64,
}

pub(crate) struct CallTimerSlot {
    generation: u64,
    deadline: Instant,
}

impl CallTimerSlot {
    fn new(generation: u64, delay: Duration) -> Self {
        Self::from_deadline(generation, Instant::now() + delay)
    }

    pub(crate) fn from_deadline(generation: u64, deadline: Instant) -> Self {
        Self {
            generation,
            deadline,
        }
    }
}

pub(crate) async fn wait_for_timer(slot: &mut Option<CallTimerSlot>) -> u64 {
    match slot {
        Some(timer) => {
            tokio::time::sleep_until(timer.deadline).await;
            timer.generation
        }
        None => pending().await,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CallTimeouts {
    pub(crate) route: Duration,
    pub(crate) dial: Duration,
    pub(crate) ring: Duration,
    pub(crate) cleanup: Duration,
    pub(crate) route_budget: Duration,
    pub(crate) min_attempt_window: Duration,
}

impl Default for CallTimeouts {
    fn default() -> Self {
        Self {
            route: timeout_from_env("VOIPSWITCH_ROUTE_TIMEOUT_MS", DEFAULT_ROUTE_TIMEOUT_MS),
            dial: timeout_from_env("VOIPSWITCH_DIAL_TIMEOUT_MS", DEFAULT_DIAL_TIMEOUT_MS),
            ring: timeout_from_env("VOIPSWITCH_RING_TIMEOUT_MS", DEFAULT_RING_TIMEOUT_MS),
            cleanup: timeout_from_env("VOIPSWITCH_CLEANUP_TIMEOUT_MS", DEFAULT_CLEANUP_TIMEOUT_MS),
            route_budget: timeout_from_env(
                "VOIPSWITCH_CALL_ROUTE_BUDGET_MS",
                DEFAULT_CALL_ROUTE_BUDGET_MS,
            ),
            min_attempt_window: timeout_from_env(
                "VOIPSWITCH_MIN_ATTEMPT_WINDOW_MS",
                DEFAULT_MIN_ATTEMPT_WINDOW_MS,
            ),
        }
    }
}

pub(crate) fn timeout_from_env(name: &str, default_ms: u64) -> Duration {
    match std::env::var(name) {
        Ok(value) => match value.parse::<u64>() {
            Ok(milliseconds) if milliseconds > 0 => Duration::from_millis(milliseconds),
            _ => {
                warn!(
                    variable = name,
                    value, default_ms, "invalid call timeout override"
                );
                Duration::from_millis(default_ms)
            }
        },
        Err(_) => Duration::from_millis(default_ms),
    }
}

impl SessionActor {
    pub(crate) async fn handle_timer(&mut self, event: CallTimerEvent) -> Result<()> {
        match event.kind {
            CallTimerKind::Dial => {
                if self.call.call_id() != event.call_id
                    || self.call.dial_timer_generation != event.generation
                    || self.call.is_answered()
                    || self.call.is_terminating()
                {
                    return Ok(());
                }
                if self.wait_for_retry_cleanup(408)? {
                    return Ok(());
                }
                self.cancel_timer(CallTimerKind::Dial);
                let call = &mut self.call;

                call.state = CallState::Terminating;
                call.last_status = Some(408);
                call.hangup_cause = Some(HangupCause::RecoveryOnTimerExpire);
                let call_id = call.call_id().to_string();
                self.submit_callee_action(
                    "CancelOutbound",
                    format!("dial-timeout-cancel-{call_id}"),
                    json!({
                        "reason": "dial_timeout",
                    }),
                )?;
                self.submit_caller_action(
                    "RejectInboundInvite",
                    format!("dial-timeout-reject-{call_id}"),
                    json!({
                        "adapter_call_leg_id": self.call.caller_adapter_leg_id.as_str(),
                        "status_code": 408,
                    }),
                )?;
                info!(call_id = event.call_id, "basic call dial timeout");
                self.start_timer(CallTimerKind::Cleanup);
                self.publish_call_view();
            }
            CallTimerKind::Ring => {
                if self.call.call_id() != event.call_id
                    || self.call.ring_timer_generation != event.generation
                    || self.call.is_answered()
                    || self.call.is_terminating()
                {
                    return Ok(());
                }
                if self.wait_for_retry_cleanup(480)? {
                    return Ok(());
                }
                self.cancel_timer(CallTimerKind::Ring);
                let call = &mut self.call;

                call.state = CallState::Terminating;
                call.last_status = Some(480);
                call.hangup_cause = Some(HangupCause::NoUserResponse);
                let call_id = call.call_id().to_string();
                self.submit_callee_action(
                    "CancelOutbound",
                    format!("ring-timeout-cancel-{call_id}"),
                    json!({
                        "reason": "ring_timeout",
                    }),
                )?;
                self.submit_caller_action(
                    "RejectInboundInvite",
                    format!("ring-timeout-reject-{call_id}"),
                    json!({
                        "adapter_call_leg_id": self.call.caller_adapter_leg_id.as_str(),
                        "status_code": 480,
                    }),
                )?;
                info!(call_id = event.call_id, "basic call ring timeout");
                self.start_timer(CallTimerKind::Cleanup);
                self.publish_call_view();
            }
            CallTimerKind::Cleanup => {
                if self.call.call_id() != event.call_id
                    || self.call.cleanup_timer_generation != event.generation
                {
                    return Ok(());
                }
                if self.retry_after_cleanup.is_some() {
                    self.force_retry_after_cleanup()?;
                    return Ok(());
                }
                let call = &self.call;
                if !call.is_terminating() {
                    return Ok(());
                }
                let pending_sessions = [
                    (!call.caller_terminated)
                        .then(|| (false, format!("cleanup-hangup-caller-{}", call.call_id()))),
                    (!call.callee_terminated)
                        .then(|| (true, format!("cleanup-hangup-callee-{}", call.call_id()))),
                ];
                for (callee, action_id) in pending_sessions.into_iter().flatten() {
                    if callee {
                        self.submit_callee_action("HangupDialog", action_id, json!({}))?;
                    } else {
                        self.submit_caller_action("HangupDialog", action_id, json!({}))?;
                    }
                }
                warn!(
                    call_id = event.call_id,
                    "basic call cleanup timeout forced removal"
                );
                self.call
                    .hangup_cause
                    .get_or_insert(HangupCause::InternalError);
                self.begin_finish_call();
            }
        }
        Ok(())
    }

    pub(super) fn start_timer(&mut self, kind: CallTimerKind) {
        match kind {
            CallTimerKind::Dial => {
                self.call.dial_timer_generation = self.call.dial_timer_generation.saturating_add(1);
                self.dial_timer = Some(CallTimerSlot::new(
                    self.call.dial_timer_generation,
                    self.call
                        .config_snapshot
                        .timeouts
                        .dial
                        .min(self.route_budget_remaining()),
                ));
            }
            CallTimerKind::Ring => {
                self.call.ring_timer_generation = self.call.ring_timer_generation.saturating_add(1);
                self.ring_timer = Some(CallTimerSlot::new(
                    self.call.ring_timer_generation,
                    self.call
                        .config_snapshot
                        .timeouts
                        .ring
                        .min(self.route_budget_remaining()),
                ));
            }
            CallTimerKind::Cleanup => {
                self.call.cleanup_timer_generation =
                    self.call.cleanup_timer_generation.saturating_add(1);
                self.cleanup_timer = Some(CallTimerSlot::new(
                    self.call.cleanup_timer_generation,
                    self.call.config_snapshot.timeouts.cleanup,
                ));
            }
        }
    }

    pub(super) fn cancel_timer(&mut self, kind: CallTimerKind) {
        match kind {
            CallTimerKind::Dial => {
                self.call.dial_timer_generation = self.call.dial_timer_generation.saturating_add(1);
                self.dial_timer = None;
            }
            CallTimerKind::Ring => {
                self.call.ring_timer_generation = self.call.ring_timer_generation.saturating_add(1);
                self.ring_timer = None;
            }
            CallTimerKind::Cleanup => {
                self.call.cleanup_timer_generation =
                    self.call.cleanup_timer_generation.saturating_add(1);
                self.cleanup_timer = None;
            }
        }
    }

    pub(super) fn cancel_all_timers(&mut self) {
        self.cancel_timer(CallTimerKind::Dial);
        self.cancel_timer(CallTimerKind::Ring);
        self.cancel_timer(CallTimerKind::Cleanup);
    }

    pub(super) async fn handle_timer_expiry(
        &mut self,
        kind: CallTimerKind,
        generation: u64,
    ) -> Result<()> {
        match kind {
            CallTimerKind::Dial => self.dial_timer = None,
            CallTimerKind::Ring => self.ring_timer = None,
            CallTimerKind::Cleanup => self.cleanup_timer = None,
        }
        self.handle_timer(CallTimerEvent {
            call_id: self.call.call_id().to_string(),
            kind,
            generation,
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn timer_slot_returns_its_generation() {
        let mut slot = Some(CallTimerSlot::new(7, Duration::from_millis(1)));

        let generation =
            tokio::time::timeout(Duration::from_millis(100), wait_for_timer(&mut slot))
                .await
                .expect("timer should expire");

        assert_eq!(generation, 7);
    }

    #[tokio::test]
    async fn empty_timer_slot_remains_pending() {
        let mut slot = None;

        let result =
            tokio::time::timeout(Duration::from_millis(10), wait_for_timer(&mut slot)).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn replacing_timer_slot_discards_the_old_generation() {
        let mut slot = Some(CallTimerSlot::new(3, Duration::from_secs(60)));
        assert_eq!(slot.as_ref().map(|timer| timer.generation), Some(3));
        slot = Some(CallTimerSlot::new(4, Duration::from_millis(1)));

        let generation =
            tokio::time::timeout(Duration::from_millis(100), wait_for_timer(&mut slot))
                .await
                .expect("replacement timer should expire");

        assert_eq!(generation, 4);
    }

    #[tokio::test]
    async fn rebuilding_timer_uses_original_absolute_deadline() {
        let deadline = Instant::now() + Duration::from_millis(30);
        let original = CallTimerSlot::from_deadline(9, deadline);
        assert_eq!(original.generation, 9);
        assert_eq!(original.deadline, deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut rebuilt = Some(CallTimerSlot::from_deadline(
            original.generation,
            original.deadline,
        ));
        tokio::time::timeout(Duration::from_millis(25), wait_for_timer(&mut rebuilt))
            .await
            .expect("rebuilt timer must not restart its full duration");
    }
}
