use crate::analysis::EndpointRef;
use crate::media::SessionMediaState;
use crate::types::call::{HangupCause, LegDirection, LegState};
use crate::types::ids::{AdapterCallLegId, CallId, CalleeAttemptId, DomainId, SessionId, TrunkId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::{self, Display};

pub type SessionVars = BTreeMap<String, Value>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegRole {
    Caller,
    Callee { attempt_id: CalleeAttemptId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionEndpoint {
    Endpoint(EndpointRef),
    Trunk { trunk_id: TrunkId },
    External { number: String },
    AiAgent { agent_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SipBinding {
    pub adapter_call_leg_id: AdapterCallLegId,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub id: SessionId,
    pub domain_id: DomainId,
    pub call_id: CallId,
    pub role: LegRole,
    pub direction: LegDirection,
    pub endpoint: SessionEndpoint,
    pub state: LegState,
    pub sip: Option<SipBinding>,
    pub peer: Option<SessionId>,
    pub media: Option<SessionMediaState>,
    pub variables: SessionVars,
    pub created_at_ms: u64,
    pub answered_at_ms: Option<u64>,
    pub hangup_cause: Option<HangupCause>,
}

impl Session {
    pub fn transition(&mut self, next: LegState) -> Result<(), SessionError> {
        if self.state == next {
            return Ok(());
        }
        if !valid_transition(self.state, next) {
            return Err(SessionError::InvalidTransition {
                from: self.state,
                to: next,
            });
        }
        self.state = next;
        Ok(())
    }

    pub fn mark_answered(&mut self, answered_at_ms: u64) {
        self.answered_at_ms.get_or_insert(answered_at_ms);
    }

    pub fn begin_hangup(&mut self, cause: HangupCause) -> Result<(), SessionError> {
        self.hangup_cause.get_or_insert(cause);
        self.transition(LegState::HangingUp)
    }

    pub fn update_media_stats(&mut self, stats: crate::media::MediaStatsSnapshot) {
        if let Some(media) = &mut self.media {
            media.latest_stats.merge_from(&stats);
        }
    }
}

fn valid_transition(from: LegState, to: LegState) -> bool {
    use LegState::*;
    matches!(
        (from, to),
        (Idle, Routing | Dialing | HangingUp)
            | (Routing, Dialing | HangingUp)
            | (Dialing, WaitForAnswer | HangingUp)
            | (WaitForAnswer, Bridged | HangingUp)
            | (Bridged, HangingUp)
            | (HangingUp, Destroyed)
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionError {
    InvalidTransition { from: LegState, to: LegState },
}

impl Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTransition { from, to } => {
                write!(f, "invalid session transition from {from:?} to {to:?}")
            }
        }
    }
}

impl std::error::Error for SessionError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> Session {
        Session {
            id: SessionId::from("session-1"),
            domain_id: DomainId::from("domain-1"),
            call_id: CallId::from("call-1"),
            role: LegRole::Caller,
            direction: LegDirection::Inbound,
            endpoint: SessionEndpoint::External {
                number: "1000".to_string(),
            },
            state: LegState::Idle,
            sip: None,
            peer: None,
            media: None,
            variables: SessionVars::new(),
            created_at_ms: 1,
            answered_at_ms: None,
            hangup_cause: None,
        }
    }

    #[test]
    fn enforces_basic_leg_lifecycle() {
        let mut session = session();
        session.transition(LegState::Routing).unwrap();
        session.transition(LegState::Dialing).unwrap();
        session.transition(LegState::WaitForAnswer).unwrap();
        session.transition(LegState::Bridged).unwrap();
        session.begin_hangup(HangupCause::NormalClearing).unwrap();
        session.transition(LegState::Destroyed).unwrap();
        assert_eq!(session.state, LegState::Destroyed);
    }

    #[test]
    fn rejects_skipping_directly_to_bridged() {
        let mut session = session();
        assert!(matches!(
            session.transition(LegState::Bridged),
            Err(SessionError::InvalidTransition {
                from: LegState::Idle,
                to: LegState::Bridged
            })
        ));
    }
}
