use crate::runtime::call::config_snapshot::{AiPolicyDecisionSnapshot, CallConfigSnapshot};
use crate::runtime::media::MediaBridge;
use std::net::SocketAddr;
use std::sync::Arc;
use voipswitch_core::analysis::{CalleeTarget, EndpointRef};
use voipswitch_core::call::{CallAggregate, CallContext, CalleeAttemptState};
use voipswitch_core::session::{LegRole, Session, SessionEndpoint, SessionVars, SipBinding};
use voipswitch_core::types::call::{CallState, HangupCause, LegDirection, LegState};
use voipswitch_core::types::ids::{
    AdapterCallLegId, BridgeId, CallId, CalleeAttemptId, EndpointId, SessionId, TrunkId,
};
use voipswitch_core::types::number::ExtensionNumber;

pub(crate) struct CallRuntime {
    pub(crate) config_snapshot: Arc<CallConfigSnapshot>,
    pub(crate) aggregate: CallAggregate,
    pub(crate) caller: Session,
    pub(crate) current_attempt_id: CalleeAttemptId,
    pub(crate) current_callee_session_id: SessionId,
    pub(crate) current_bridge_id: Option<BridgeId>,
    pub(crate) caller_adapter_leg_id: AdapterCallLegId,
    pub(crate) caller_number: String,
    pub(crate) callee_number: String,
    pub(crate) callee_target: String,
    pub(crate) inbound_route_id: Option<String>,
    pub(crate) inbound_route_name: Option<String>,
    pub(crate) inbound_trunk_ref: Option<String>,
    pub(crate) inbound_trunk_name: Option<String>,
    pub(crate) outbound_route_id: Option<String>,
    pub(crate) outbound_route_name: Option<String>,
    pub(crate) outbound_trunk_ref: Option<String>,
    pub(crate) outbound_trunk_name: Option<String>,
    pub(crate) recording_requested: bool,
    pub(crate) ai_policy: Option<AiPolicyDecisionSnapshot>,
    pub(crate) recording_start_error: Option<String>,
    pub(crate) media: Option<MediaBridge>,
    pub(crate) state: CallState,
    pub(crate) last_status: Option<u16>,
    pub(crate) hangup_cause: Option<HangupCause>,

    pub(crate) dial_timer_generation: u64,
    pub(crate) ring_timer_generation: u64,
    pub(crate) cleanup_timer_generation: u64,
    pub(crate) late_answer_cleanup_sent: bool,
    pub(crate) caller_terminated: bool,
    pub(crate) callee_terminated: bool,
}

impl CallRuntime {
    pub(crate) fn new(
        call_id: String,
        caller_session_id: String,
        callee_session_id: String,
        caller_adapter_leg_id: String,
        candidate: &OutboundCandidate,
        config_snapshot: Arc<CallConfigSnapshot>,
        started_at_ms: u64,
    ) -> Result<Self, String> {
        if !config_snapshot.media_policy.audio_only
            || config_snapshot.media_policy.max_audio_m_lines != 1
        {
            return Err("unsupported call media policy snapshot".to_string());
        }
        let call_id = CallId::from(call_id);
        let domain_id = config_snapshot.domain_id.clone();
        let caller_number = config_snapshot.numbers.original_caller.clone();
        let callee_number = config_snapshot.numbers.original_callee.clone();
        let caller_session_id = SessionId::from(caller_session_id);
        let callee_session_id = SessionId::from(callee_session_id);
        let attempt_id = CalleeAttemptId::from("attempt-1");
        let caller_adapter_leg_id = AdapterCallLegId::from(caller_adapter_leg_id);
        let caller = Session {
            id: caller_session_id,
            domain_id,
            call_id,
            role: LegRole::Caller,
            direction: LegDirection::Inbound,
            endpoint: SessionEndpoint::External {
                number: caller_number.clone(),
            },
            state: LegState::Dialing,
            sip: Some(SipBinding {
                adapter_call_leg_id: caller_adapter_leg_id.clone(),
                generation: 1,
            }),
            peer: Some(callee_session_id.clone()),
            media: None,
            variables: SessionVars::new(),
            created_at_ms: started_at_ms,
            answered_at_ms: None,
            hangup_cause: None,
        };
        let callee = Self::callee_session(
            &caller,
            callee_session_id.clone(),
            attempt_id.clone(),
            &candidate.outbound_target,
            started_at_ms,
        )?;
        let mut aggregate = CallAggregate::new(
            &caller,
            config_snapshot.bridge_policy,
            started_at_ms,
            CallContext::new(),
        )
        .map_err(|error| error.to_string())?;
        aggregate
            .add_leg(&callee)
            .map_err(|error| error.to_string())?;
        aggregate
            .add_callee_attempt(
                attempt_id.clone(),
                callee_session_id.clone(),
                candidate.outbound_target.callee_target()?,
            )
            .map_err(|error| error.to_string())?;
        aggregate
            .update_attempt_state(&attempt_id, CalleeAttemptState::Dialing, None)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            inbound_route_id: config_snapshot.inbound.route_id.clone(),
            inbound_route_name: config_snapshot.inbound.route_name.clone(),
            inbound_trunk_ref: config_snapshot.inbound.trunk_ref.clone(),
            inbound_trunk_name: config_snapshot.inbound.trunk_name.clone(),
            outbound_route_id: config_snapshot.outbound.route_id.clone(),
            outbound_route_name: config_snapshot.outbound.route_name.clone(),
            config_snapshot: config_snapshot.clone(),
            aggregate,
            caller,
            current_attempt_id: attempt_id,
            current_callee_session_id: callee_session_id,
            current_bridge_id: None,
            caller_adapter_leg_id,
            caller_number,
            callee_number,
            callee_target: candidate.outbound_target.stable_ref(),
            outbound_trunk_ref: candidate.outbound_trunk_ref.clone(),
            outbound_trunk_name: candidate.outbound_trunk_name.clone(),
            recording_requested: config_snapshot.recording.initial_requested,
            ai_policy: config_snapshot.initial_ai_policy.clone(),
            recording_start_error: None,
            media: None,
            state: CallState::Establishing,
            last_status: None,
            hangup_cause: None,
            dial_timer_generation: 0,
            ring_timer_generation: 0,
            cleanup_timer_generation: 0,
            late_answer_cleanup_sent: false,
            caller_terminated: false,
            callee_terminated: false,
        })
    }

    fn callee_session(
        caller: &Session,
        session_id: SessionId,
        attempt_id: CalleeAttemptId,
        target: &OutboundTarget,
        created_at_ms: u64,
    ) -> Result<Session, String> {
        Ok(Session {
            id: session_id,
            domain_id: caller.domain_id.clone(),
            call_id: caller.call_id.clone(),
            role: LegRole::Callee { attempt_id },
            direction: LegDirection::Outbound,
            endpoint: target.session_endpoint()?,
            state: LegState::Dialing,
            sip: None,
            peer: Some(caller.id.clone()),
            media: None,
            variables: SessionVars::new(),
            created_at_ms,
            answered_at_ms: None,
            hangup_cause: None,
        })
    }

    pub(crate) fn make_callee_session(
        &self,
        session_id: String,
        attempt_seq: u16,
        target: &OutboundTarget,
        created_at_ms: u64,
    ) -> Result<Session, String> {
        Self::callee_session(
            &self.caller,
            SessionId::from(session_id),
            CalleeAttemptId::from(format!("attempt-{attempt_seq}")),
            target,
            created_at_ms,
        )
    }

    pub(crate) fn call_id(&self) -> &str {
        self.aggregate.id().as_str()
    }

    pub(crate) fn domain_id(&self) -> &str {
        self.aggregate.domain_id().as_str()
    }

    pub(crate) fn caller_session_id(&self) -> &str {
        self.caller.id.as_str()
    }

    pub(crate) fn callee_session_id(&self) -> &str {
        self.current_callee_session_id.as_str()
    }

    pub(crate) fn started_at_ms(&self) -> u64 {
        self.aggregate.started_at_ms()
    }
    pub(crate) fn is_terminating(&self) -> bool {
        self.state.is_terminating_or_completed()
    }
    pub(crate) fn state_str(&self) -> &'static str {
        self.state.as_str()
    }
    pub(crate) fn hangup_cause_str(&self) -> Option<&str> {
        self.hangup_cause.map(HangupCause::as_cdr_string)
    }
    pub(crate) fn begin_terminating(&mut self, cause: HangupCause) {
        if !self.state.is_terminating_or_completed() {
            self.state = CallState::Terminating;
        }
        self.hangup_cause.get_or_insert(cause);
    }

    pub(crate) fn answered_at_ms(&self) -> Option<u64> {
        self.caller.answered_at_ms
    }

    pub(crate) fn is_answered(&self) -> bool {
        self.caller.answered_at_ms.is_some()
    }

    pub(crate) fn mark_answered(
        &mut self,
        answered_at_ms: u64,
        media_generation: u64,
    ) -> Result<(), String> {
        self.update_current_attempt(CalleeAttemptState::Answered, None)?;
        self.select_current_attempt(answered_at_ms)?;
        self.caller.mark_answered(answered_at_ms);
        self.caller.state = LegState::Bridged;
        self.aggregate
            .update_leg_state(&self.caller.id, LegState::Bridged)
            .map_err(|error| error.to_string())?;
        self.aggregate
            .update_leg_state(&self.current_callee_session_id, LegState::Bridged)
            .map_err(|error| error.to_string())?;
        let bridge_id = BridgeId::from(format!("bridge-{}", self.current_attempt_id));
        self.aggregate
            .add_bridge(
                bridge_id.clone(),
                self.caller.id.clone(),
                self.current_callee_session_id.clone(),
                media_generation,
                answered_at_ms,
            )
            .map_err(|error| error.to_string())?;
        self.current_bridge_id = Some(bridge_id);
        Ok(())
    }

    pub(crate) fn activate_current_bridge(
        &mut self,
        media_generation: u64,
        active_at_ms: u64,
    ) -> Result<(), String> {
        let bridge_id = self
            .current_bridge_id
            .as_ref()
            .ok_or_else(|| "current bridge missing".to_string())?;
        self.aggregate
            .activate_bridge(bridge_id, media_generation, active_at_ms)
            .map_err(|error| error.to_string())
    }

    pub(crate) fn release_topology(&mut self, released_at_ms: u64) {
        self.aggregate.release_all_bridges(released_at_ms);
        let _ = self
            .aggregate
            .mark_leg_left(&self.caller.id, released_at_ms);
        for attempt in self.aggregate.snapshot().callee_attempts {
            let _ = self
                .aggregate
                .mark_leg_left(&attempt.session_id, released_at_ms);
        }
    }

    pub(crate) fn register_attempt(
        &mut self,
        attempt_seq: u16,
        session_id: String,
        target: &OutboundTarget,
        created_at_ms: u64,
    ) -> Result<(), String> {
        let attempt_id = CalleeAttemptId::from(format!("attempt-{attempt_seq}"));
        let session_id = SessionId::from(session_id);
        let callee = Self::callee_session(
            &self.caller,
            session_id.clone(),
            attempt_id.clone(),
            target,
            created_at_ms,
        )?;
        self.aggregate
            .add_leg(&callee)
            .map_err(|error| error.to_string())?;
        self.aggregate
            .add_callee_attempt(
                attempt_id.clone(),
                session_id.clone(),
                target.callee_target()?,
            )
            .map_err(|error| error.to_string())?;
        self.aggregate
            .update_attempt_state(&attempt_id, CalleeAttemptState::Dialing, None)
            .map_err(|error| error.to_string())?;
        self.current_attempt_id = attempt_id;
        self.current_callee_session_id = session_id.clone();
        self.caller.peer = Some(session_id);
        Ok(())
    }

    pub(crate) fn update_current_attempt(
        &mut self,
        state: CalleeAttemptState,
        cause: Option<HangupCause>,
    ) -> Result<(), String> {
        self.aggregate
            .update_attempt_state(&self.current_attempt_id, state, cause)
            .map_err(|error| error.to_string())
    }

    pub(crate) fn select_current_attempt(&mut self, answered_at_ms: u64) -> Result<(), String> {
        self.aggregate
            .select_winner(&self.current_attempt_id, answered_at_ms)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

#[derive(Debug, Clone)]
pub(crate) enum OutboundTarget {
    Endpoint {
        endpoint_id: String,
        endpoint_number: String,
    },
    Trunk {
        trunk_ref: String,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct OutboundCandidate {
    pub(crate) outbound_target: OutboundTarget,
    pub(crate) callee_route_target: Option<SocketAddr>,
    pub(crate) outbound_trunk_ref: Option<String>,
    pub(crate) outbound_trunk_name: Option<String>,
    pub(crate) recording_requested: bool,
    pub(crate) recording_policy_ids: Arc<[u64]>,
    pub(crate) ai_policy: Option<AiPolicyDecisionSnapshot>,
}

pub(crate) struct ResolvedRoute {
    pub(crate) signaling_caller_number: String,
    pub(crate) signaling_callee_number: String,
    pub(crate) candidates: Vec<OutboundCandidate>,
    pub(crate) outbound_route_id: Option<String>,
    pub(crate) outbound_route_name: Option<String>,
}

impl OutboundTarget {
    pub(crate) fn stable_ref(&self) -> String {
        match self {
            Self::Endpoint { endpoint_id, .. } => format!("endpoint:{endpoint_id}"),
            Self::Trunk { trunk_ref } => trunk_ref.clone(),
        }
    }

    fn callee_target(&self) -> Result<CalleeTarget, String> {
        match self {
            Self::Endpoint {
                endpoint_id,
                endpoint_number,
            } => Ok(CalleeTarget::Extension {
                endpoint_id: EndpointId::from(endpoint_id.clone()),
                number: ExtensionNumber::parse(endpoint_number)
                    .map_err(|error| error.to_string())?,
            }),
            Self::Trunk { trunk_ref } => Ok(CalleeTarget::Trunk {
                trunk_id: TrunkId::from(trunk_ref.clone()),
            }),
        }
    }

    fn session_endpoint(&self) -> Result<SessionEndpoint, String> {
        match self {
            Self::Endpoint {
                endpoint_id,
                endpoint_number,
            } => Ok(SessionEndpoint::Endpoint(EndpointRef {
                endpoint_id: EndpointId::from(endpoint_id.clone()),
                number: ExtensionNumber::parse(endpoint_number)
                    .map_err(|error| error.to_string())?,
            })),
            Self::Trunk { trunk_ref } => Ok(SessionEndpoint::Trunk {
                trunk_id: TrunkId::from(trunk_ref.clone()),
            }),
        }
    }
}
