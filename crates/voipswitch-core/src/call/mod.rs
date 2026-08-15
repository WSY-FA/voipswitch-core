use crate::analysis::CalleeTarget;
use crate::session::{LegRole, Session, SessionEndpoint};
use crate::types::call::{HangupCause, LegState};
use crate::types::ids::{BridgeId, CallId, CalleeAttemptId, DomainId, SessionId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::{self, Display};

pub type CallContext = BTreeMap<String, Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgePolicy {
    SingleCallee,
    ParallelFirstAnswer,
    SequentialHunt,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallLeg {
    pub session_id: SessionId,
    pub domain_id: DomainId,
    pub role: LegRole,
    pub endpoint: SessionEndpoint,
    pub state: LegState,
    pub role_history: Vec<LegRoleAssignment>,
    pub joined_at_ms: u64,
    pub left_at_ms: Option<u64>,
}

impl From<&Session> for CallLeg {
    fn from(value: &Session) -> Self {
        Self {
            session_id: value.id.clone(),
            domain_id: value.domain_id.clone(),
            role: value.role.clone(),
            endpoint: value.endpoint.clone(),
            state: value.state,
            role_history: vec![LegRoleAssignment {
                role: value.role.clone(),
                effective_at_ms: value.created_at_ms,
            }],
            joined_at_ms: value.created_at_ms,
            left_at_ms: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LegRoleAssignment {
    pub role: LegRole,
    pub effective_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeState {
    Planned,
    Active,
    Released,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeEdge {
    pub bridge_id: BridgeId,
    pub leg_a: SessionId,
    pub leg_b: SessionId,
    pub media_generation: u64,
    pub state: BridgeState,
    pub created_at_ms: u64,
    pub active_at_ms: Option<u64>,
    pub released_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CalleeAttemptState {
    Planned,
    Dialing,
    Ringing,
    Answered,
    Selected,
    Cancelled,
    Failed,
    Destroyed,
}

impl CalleeAttemptState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Cancelled | Self::Failed | Self::Destroyed)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalleeAttempt {
    pub attempt_id: CalleeAttemptId,
    pub session_id: SessionId,
    pub target: CalleeTarget,
    pub state: CalleeAttemptState,
    pub failure_cause: Option<HangupCause>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallSnapshot {
    pub id: CallId,
    pub domain_id: DomainId,
    pub caller_leg: SessionId,
    pub coordinator: SessionId,
    pub coordinator_generation: u64,
    pub legs: Vec<CallLeg>,
    pub callee_attempts: Vec<CalleeAttempt>,
    pub bridges: Vec<BridgeEdge>,
    pub bridge_policy: BridgePolicy,
    pub winner: Option<CalleeAttemptId>,
    pub started_at_ms: u64,
    pub answered_at_ms: Option<u64>,
    pub context: CallContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WinnerSelection {
    pub winner_session_id: SessionId,
    pub cancelled_sessions: Vec<SessionId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallAggregate {
    id: CallId,
    domain_id: DomainId,
    caller_leg: SessionId,
    coordinator: SessionId,
    coordinator_generation: u64,
    bridge_policy: BridgePolicy,
    started_at_ms: u64,
    context: CallContext,
    legs: BTreeMap<SessionId, CallLeg>,
    callee_attempts: BTreeMap<CalleeAttemptId, CalleeAttempt>,
    bridges: BTreeMap<BridgeId, BridgeEdge>,
    winner: Option<CalleeAttemptId>,
    answered_at_ms: Option<u64>,
}

impl CallAggregate {
    pub fn new(
        caller: &Session,
        bridge_policy: BridgePolicy,
        started_at_ms: u64,
        context: CallContext,
    ) -> Result<Self, CallError> {
        if caller.role != LegRole::Caller {
            return Err(CallError::CallerRoleRequired);
        }
        let caller_leg = CallLeg::from(caller);
        let mut legs = BTreeMap::new();
        legs.insert(caller.id.clone(), caller_leg);
        Ok(Self {
            id: caller.call_id.clone(),
            domain_id: caller.domain_id.clone(),
            caller_leg: caller.id.clone(),
            coordinator: caller.id.clone(),
            coordinator_generation: 1,
            bridge_policy,
            started_at_ms,
            context,
            legs,
            callee_attempts: BTreeMap::new(),
            bridges: BTreeMap::new(),
            winner: None,
            answered_at_ms: None,
        })
    }

    pub fn id(&self) -> &CallId {
        &self.id
    }

    pub fn domain_id(&self) -> &DomainId {
        &self.domain_id
    }

    pub fn caller_leg_id(&self) -> &SessionId {
        &self.caller_leg
    }

    pub fn coordinator(&self) -> &SessionId {
        &self.coordinator
    }

    pub fn coordinator_generation(&self) -> u64 {
        self.coordinator_generation
    }

    pub fn started_at_ms(&self) -> u64 {
        self.started_at_ms
    }

    pub fn answered_at_ms(&self) -> Option<u64> {
        self.answered_at_ms
    }

    pub fn winner(&self) -> Option<&CalleeAttemptId> {
        self.winner.as_ref()
    }

    pub fn attempt(&self, attempt_id: &CalleeAttemptId) -> Option<&CalleeAttempt> {
        self.callee_attempts.get(attempt_id)
    }

    pub fn leg(&self, session_id: &SessionId) -> Option<&CallLeg> {
        self.legs.get(session_id)
    }

    pub fn bridge(&self, bridge_id: &BridgeId) -> Option<&BridgeEdge> {
        self.bridges.get(bridge_id)
    }

    pub fn update_leg_role(
        &mut self,
        session_id: &SessionId,
        role: LegRole,
        effective_at_ms: u64,
    ) -> Result<(), CallError> {
        let leg = self
            .legs
            .get_mut(session_id)
            .ok_or_else(|| CallError::SessionNotFound(session_id.clone()))?;
        if leg.role == role {
            return Ok(());
        }
        leg.role = role.clone();
        leg.role_history.push(LegRoleAssignment {
            role,
            effective_at_ms,
        });
        Ok(())
    }

    pub fn mark_leg_left(
        &mut self,
        session_id: &SessionId,
        left_at_ms: u64,
    ) -> Result<(), CallError> {
        let leg = self
            .legs
            .get_mut(session_id)
            .ok_or_else(|| CallError::SessionNotFound(session_id.clone()))?;
        leg.left_at_ms.get_or_insert(left_at_ms);
        leg.state = LegState::Destroyed;
        Ok(())
    }

    pub fn add_bridge(
        &mut self,
        bridge_id: BridgeId,
        leg_a: SessionId,
        leg_b: SessionId,
        media_generation: u64,
        created_at_ms: u64,
    ) -> Result<(), CallError> {
        if leg_a == leg_b {
            return Err(CallError::BridgeRequiresDistinctLegs);
        }
        for session_id in [&leg_a, &leg_b] {
            if !self.legs.contains_key(session_id) {
                return Err(CallError::SessionNotFound(session_id.clone()));
            }
        }
        if self.bridges.contains_key(&bridge_id) {
            return Err(CallError::DuplicateBridge(bridge_id));
        }
        self.bridges.insert(
            bridge_id.clone(),
            BridgeEdge {
                bridge_id,
                leg_a,
                leg_b,
                media_generation,
                state: BridgeState::Planned,
                created_at_ms,
                active_at_ms: None,
                released_at_ms: None,
            },
        );
        Ok(())
    }

    pub fn activate_bridge(
        &mut self,
        bridge_id: &BridgeId,
        media_generation: u64,
        active_at_ms: u64,
    ) -> Result<(), CallError> {
        let bridge = self
            .bridges
            .get_mut(bridge_id)
            .ok_or_else(|| CallError::BridgeNotFound(bridge_id.clone()))?;
        if bridge.media_generation != media_generation {
            return Err(CallError::BridgeGenerationMismatch {
                bridge_id: bridge_id.clone(),
                expected: bridge.media_generation,
                actual: media_generation,
            });
        }
        match bridge.state {
            BridgeState::Planned => {
                bridge.state = BridgeState::Active;
                bridge.active_at_ms.get_or_insert(active_at_ms);
                Ok(())
            }
            BridgeState::Active => Ok(()),
            BridgeState::Released => Err(CallError::BridgeAlreadyReleased(bridge_id.clone())),
        }
    }

    pub fn release_bridge(
        &mut self,
        bridge_id: &BridgeId,
        released_at_ms: u64,
    ) -> Result<(), CallError> {
        let bridge = self
            .bridges
            .get_mut(bridge_id)
            .ok_or_else(|| CallError::BridgeNotFound(bridge_id.clone()))?;
        bridge.state = BridgeState::Released;
        bridge.released_at_ms.get_or_insert(released_at_ms);
        Ok(())
    }

    pub fn release_all_bridges(&mut self, released_at_ms: u64) {
        for bridge in self.bridges.values_mut() {
            bridge.state = BridgeState::Released;
            bridge.released_at_ms.get_or_insert(released_at_ms);
        }
    }

    pub fn handoff_coordinator(
        &mut self,
        expected_owner: &SessionId,
        expected_generation: u64,
        target: &SessionId,
    ) -> Result<u64, CallError> {
        if &self.coordinator != expected_owner || self.coordinator_generation != expected_generation
        {
            return Err(CallError::StaleCoordinator {
                expected_owner: self.coordinator.clone(),
                expected_generation: self.coordinator_generation,
            });
        }
        let target_leg = self
            .legs
            .get(target)
            .ok_or_else(|| CallError::SessionNotFound(target.clone()))?;
        if target_leg.state.is_terminal() || target_leg.left_at_ms.is_some() {
            return Err(CallError::CoordinatorTargetUnavailable(target.clone()));
        }
        if target == expected_owner {
            return Ok(self.coordinator_generation);
        }
        let next_generation = self
            .coordinator_generation
            .checked_add(1)
            .ok_or(CallError::CoordinatorGenerationExhausted)?;
        self.coordinator = target.clone();
        self.coordinator_generation = next_generation;
        Ok(self.coordinator_generation)
    }

    pub fn handoff_candidates(&self, continuation: Option<&SessionId>) -> Vec<SessionId> {
        let is_available = |leg: &&CallLeg| {
            leg.session_id != self.coordinator
                && !leg.state.is_terminal()
                && leg.left_at_ms.is_none()
        };
        let mut candidates: Vec<&CallLeg> = self.legs.values().filter(is_available).collect();
        candidates.sort_by_key(|leg| {
            let explicit = continuation == Some(&leg.session_id);
            let bridged = leg.state == LegState::Bridged
                && self.bridges.values().any(|bridge| {
                    bridge.state == BridgeState::Active
                        && (bridge.leg_a == leg.session_id || bridge.leg_b == leg.session_id)
                });
            (
                !explicit,
                !bridged,
                leg.joined_at_ms,
                leg.session_id.clone(),
            )
        });
        candidates
            .into_iter()
            .map(|leg| leg.session_id.clone())
            .collect()
    }

    pub fn add_leg(&mut self, session: &Session) -> Result<(), CallError> {
        if session.domain_id != self.domain_id || session.call_id != self.id {
            return Err(CallError::ForeignSession {
                session_id: session.id.clone(),
            });
        }
        if self.legs.contains_key(&session.id) {
            return Err(CallError::DuplicateSession(session.id.clone()));
        }
        self.legs.insert(session.id.clone(), CallLeg::from(session));
        Ok(())
    }

    pub fn add_callee_attempt(
        &mut self,
        attempt_id: CalleeAttemptId,
        session_id: SessionId,
        target: CalleeTarget,
    ) -> Result<(), CallError> {
        let Some(leg) = self.legs.get(&session_id) else {
            return Err(CallError::SessionNotFound(session_id));
        };
        if leg.role
            != (LegRole::Callee {
                attempt_id: attempt_id.clone(),
            })
        {
            return Err(CallError::AttemptRoleMismatch {
                attempt_id,
                session_id,
            });
        }
        if self.callee_attempts.contains_key(&attempt_id) {
            return Err(CallError::DuplicateAttempt(attempt_id));
        }
        if self.bridge_policy == BridgePolicy::SingleCallee && !self.callee_attempts.is_empty() {
            return Err(CallError::SingleCalleeLimitExceeded);
        }
        self.callee_attempts.insert(
            attempt_id.clone(),
            CalleeAttempt {
                attempt_id,
                session_id,
                target,
                state: CalleeAttemptState::Planned,
                failure_cause: None,
            },
        );
        Ok(())
    }

    pub fn update_leg_state(
        &mut self,
        session_id: &SessionId,
        state: LegState,
    ) -> Result<(), CallError> {
        let leg = self
            .legs
            .get_mut(session_id)
            .ok_or_else(|| CallError::SessionNotFound(session_id.clone()))?;
        leg.state = state;
        Ok(())
    }

    pub fn update_attempt_state(
        &mut self,
        attempt_id: &CalleeAttemptId,
        state: CalleeAttemptState,
        cause: Option<HangupCause>,
    ) -> Result<(), CallError> {
        let attempt = self
            .callee_attempts
            .get_mut(attempt_id)
            .ok_or_else(|| CallError::AttemptNotFound(attempt_id.clone()))?;
        if attempt.state == state {
            return Ok(());
        }
        if !valid_attempt_transition(attempt.state, state) {
            return Err(CallError::InvalidAttemptTransition {
                attempt_id: attempt_id.clone(),
                from: attempt.state,
                to: state,
            });
        }
        attempt.state = state;
        if cause.is_some() {
            attempt.failure_cause = cause;
        }
        Ok(())
    }

    pub fn select_winner(
        &mut self,
        attempt_id: &CalleeAttemptId,
        answered_at_ms: u64,
    ) -> Result<WinnerSelection, CallError> {
        if let Some(winner) = &self.winner {
            if winner == attempt_id {
                let session_id = self
                    .callee_attempts
                    .get(attempt_id)
                    .expect("winner attempt must exist")
                    .session_id
                    .clone();
                return Ok(WinnerSelection {
                    winner_session_id: session_id,
                    cancelled_sessions: Vec::new(),
                });
            }
            return Err(CallError::WinnerAlreadySelected(winner.clone()));
        }

        let winner_session_id = {
            let attempt = self
                .callee_attempts
                .get_mut(attempt_id)
                .ok_or_else(|| CallError::AttemptNotFound(attempt_id.clone()))?;
            if attempt.state != CalleeAttemptState::Answered {
                return Err(CallError::AttemptNotAnswered(attempt_id.clone()));
            }
            attempt.state = CalleeAttemptState::Selected;
            attempt.session_id.clone()
        };

        let mut cancelled_sessions = Vec::new();
        for (other_id, attempt) in &mut self.callee_attempts {
            if other_id != attempt_id && !attempt.state.is_terminal() {
                attempt.state = CalleeAttemptState::Cancelled;
                cancelled_sessions.push(attempt.session_id.clone());
            }
        }
        self.winner = Some(attempt_id.clone());
        self.answered_at_ms.get_or_insert(answered_at_ms);
        Ok(WinnerSelection {
            winner_session_id,
            cancelled_sessions,
        })
    }

    pub fn snapshot(&self) -> CallSnapshot {
        CallSnapshot {
            id: self.id.clone(),
            domain_id: self.domain_id.clone(),
            caller_leg: self.caller_leg.clone(),
            coordinator: self.coordinator.clone(),
            coordinator_generation: self.coordinator_generation,
            legs: self.legs.values().cloned().collect(),
            callee_attempts: self.callee_attempts.values().cloned().collect(),
            bridges: self.bridges.values().cloned().collect(),
            bridge_policy: self.bridge_policy,
            winner: self.winner.clone(),
            started_at_ms: self.started_at_ms,
            answered_at_ms: self.answered_at_ms,
            context: self.context.clone(),
        }
    }

    pub fn is_finished(&self) -> bool {
        self.legs.values().all(|leg| leg.state.is_terminal())
    }
}

fn valid_attempt_transition(from: CalleeAttemptState, to: CalleeAttemptState) -> bool {
    use CalleeAttemptState::*;
    matches!(
        (from, to),
        (Planned, Dialing | Cancelled | Failed)
            | (Dialing, Ringing | Answered | Cancelled | Failed)
            | (Ringing, Answered | Cancelled | Failed)
            | (Answered, Selected | Cancelled | Failed)
            | (Selected | Cancelled | Failed, Destroyed)
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError {
    CallerRoleRequired,
    ForeignSession {
        session_id: SessionId,
    },
    DuplicateSession(SessionId),
    SessionNotFound(SessionId),
    DuplicateAttempt(CalleeAttemptId),
    SingleCalleeLimitExceeded,
    AttemptNotFound(CalleeAttemptId),
    AttemptRoleMismatch {
        attempt_id: CalleeAttemptId,
        session_id: SessionId,
    },
    AttemptNotAnswered(CalleeAttemptId),
    WinnerAlreadySelected(CalleeAttemptId),
    InvalidAttemptTransition {
        attempt_id: CalleeAttemptId,
        from: CalleeAttemptState,
        to: CalleeAttemptState,
    },
    DuplicateBridge(BridgeId),
    BridgeNotFound(BridgeId),
    BridgeRequiresDistinctLegs,
    BridgeGenerationMismatch {
        bridge_id: BridgeId,
        expected: u64,
        actual: u64,
    },
    BridgeAlreadyReleased(BridgeId),
    StaleCoordinator {
        expected_owner: SessionId,
        expected_generation: u64,
    },
    CoordinatorTargetUnavailable(SessionId),
    CoordinatorGenerationExhausted,
}

impl Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for CallError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::EndpointRef;
    use crate::session::SessionVars;
    use crate::types::call::LegDirection;
    use crate::types::ids::EndpointId;
    use crate::types::number::ExtensionNumber;

    fn caller(domain: &str, call: &str) -> Session {
        Session {
            id: SessionId::from(format!("{call}-caller")),
            domain_id: DomainId::from(domain),
            call_id: CallId::from(call),
            role: LegRole::Caller,
            direction: LegDirection::Inbound,
            endpoint: SessionEndpoint::External {
                number: "1000".to_string(),
            },
            state: LegState::Routing,
            sip: None,
            peer: None,
            media: None,
            variables: SessionVars::new(),
            created_at_ms: 1,
            answered_at_ms: None,
            hangup_cause: None,
        }
    }

    fn callee(caller: &Session, suffix: &str, number: &str) -> (Session, CalleeTarget) {
        let attempt_id = CalleeAttemptId::from(format!("attempt-{suffix}"));
        let endpoint_id = EndpointId::from(format!("endpoint-{suffix}"));
        let number = ExtensionNumber::parse(number).unwrap();
        let endpoint = EndpointRef {
            endpoint_id: endpoint_id.clone(),
            number: number.clone(),
        };
        (
            Session {
                id: SessionId::from(format!("session-{suffix}")),
                domain_id: caller.domain_id.clone(),
                call_id: caller.call_id.clone(),
                role: LegRole::Callee {
                    attempt_id: attempt_id.clone(),
                },
                direction: LegDirection::Outbound,
                endpoint: SessionEndpoint::Endpoint(endpoint),
                state: LegState::Dialing,
                sip: None,
                peer: None,
                media: None,
                variables: SessionVars::new(),
                created_at_ms: 2,
                answered_at_ms: None,
                hangup_cause: None,
            },
            CalleeTarget::Extension {
                endpoint_id,
                number,
            },
        )
    }

    #[test]
    fn first_answer_selects_one_attempt_and_cancels_the_rest() {
        let caller = caller("domain-a", "call-1");
        let mut call = CallAggregate::new(
            &caller,
            BridgePolicy::ParallelFirstAnswer,
            1,
            CallContext::new(),
        )
        .unwrap();
        let (first, first_target) = callee(&caller, "1", "1001");
        let (second, second_target) = callee(&caller, "2", "1002");
        let first_attempt = match &first.role {
            LegRole::Callee { attempt_id } => attempt_id.clone(),
            LegRole::Caller => unreachable!(),
        };
        let second_attempt = match &second.role {
            LegRole::Callee { attempt_id } => attempt_id.clone(),
            LegRole::Caller => unreachable!(),
        };
        call.add_leg(&first).unwrap();
        call.add_leg(&second).unwrap();
        call.add_callee_attempt(first_attempt.clone(), first.id.clone(), first_target)
            .unwrap();
        call.add_callee_attempt(second_attempt.clone(), second.id.clone(), second_target)
            .unwrap();
        call.update_attempt_state(&first_attempt, CalleeAttemptState::Dialing, None)
            .unwrap();
        call.update_attempt_state(&first_attempt, CalleeAttemptState::Answered, None)
            .unwrap();

        let selection = call.select_winner(&first_attempt, 100).unwrap();

        assert_eq!(selection.winner_session_id, first.id);
        assert_eq!(selection.cancelled_sessions, vec![second.id]);
        assert_eq!(call.snapshot().winner, Some(first_attempt));
        assert!(matches!(
            call.select_winner(&second_attempt, 101),
            Err(CallError::WinnerAlreadySelected(_))
        ));
        assert!(matches!(
            call.update_attempt_state(&second_attempt, CalleeAttemptState::Answered, None),
            Err(CallError::InvalidAttemptTransition {
                from: CalleeAttemptState::Cancelled,
                to: CalleeAttemptState::Answered,
                ..
            })
        ));
    }

    #[test]
    fn single_callee_policy_rejects_a_second_attempt() {
        let caller = caller("domain-a", "call-1");
        let mut call =
            CallAggregate::new(&caller, BridgePolicy::SingleCallee, 1, CallContext::new()).unwrap();
        let (first, first_target) = callee(&caller, "1", "1001");
        let (second, second_target) = callee(&caller, "2", "1002");
        let first_attempt = match &first.role {
            LegRole::Callee { attempt_id } => attempt_id.clone(),
            LegRole::Caller => unreachable!(),
        };
        let second_attempt = match &second.role {
            LegRole::Callee { attempt_id } => attempt_id.clone(),
            LegRole::Caller => unreachable!(),
        };
        call.add_leg(&first).unwrap();
        call.add_leg(&second).unwrap();
        call.add_callee_attempt(first_attempt, first.id, first_target)
            .unwrap();

        assert_eq!(
            call.add_callee_attempt(second_attempt, second.id, second_target),
            Err(CallError::SingleCalleeLimitExceeded)
        );
    }

    #[test]
    fn bridge_topology_and_coordinator_handoff_are_generation_guarded() {
        let caller = caller("domain-a", "call-1");
        let mut call = CallAggregate::new(
            &caller,
            BridgePolicy::ParallelFirstAnswer,
            1,
            CallContext::new(),
        )
        .unwrap();
        let (first, first_target) = callee(&caller, "1", "1001");
        let (second, second_target) = callee(&caller, "2", "1002");
        let first_attempt = match &first.role {
            LegRole::Callee { attempt_id } => attempt_id.clone(),
            LegRole::Caller => unreachable!(),
        };
        let second_attempt = match &second.role {
            LegRole::Callee { attempt_id } => attempt_id.clone(),
            LegRole::Caller => unreachable!(),
        };
        call.add_leg(&first).unwrap();
        call.add_leg(&second).unwrap();
        call.add_callee_attempt(first_attempt, first.id.clone(), first_target)
            .unwrap();
        call.add_callee_attempt(second_attempt, second.id.clone(), second_target)
            .unwrap();
        call.update_leg_state(&first.id, LegState::Bridged).unwrap();
        call.add_bridge(
            BridgeId::from("bridge-1"),
            caller.id.clone(),
            first.id.clone(),
            3,
            10,
        )
        .unwrap();
        call.activate_bridge(&BridgeId::from("bridge-1"), 3, 11)
            .unwrap();

        assert_eq!(
            call.handoff_candidates(Some(&second.id)),
            vec![second.id.clone(), first.id.clone()]
        );
        assert_eq!(call.handoff_coordinator(&caller.id, 1, &first.id), Ok(2));
        assert_eq!(call.coordinator(), &first.id);
        assert!(matches!(
            call.handoff_coordinator(&caller.id, 1, &second.id),
            Err(CallError::StaleCoordinator {
                expected_generation: 2,
                ..
            })
        ));
        call.release_bridge(&BridgeId::from("bridge-1"), 20)
            .unwrap();
        let snapshot = call.snapshot();
        assert_eq!(snapshot.coordinator_generation, 2);
        assert_eq!(snapshot.bridges[0].state, BridgeState::Released);
        assert_eq!(snapshot.bridges[0].released_at_ms, Some(20));
    }

    #[test]
    fn coordinator_generation_overflow_does_not_change_owner() {
        let caller = caller("domain-a", "call-1");
        let mut call = CallAggregate::new(
            &caller,
            BridgePolicy::ParallelFirstAnswer,
            1,
            CallContext::new(),
        )
        .unwrap();
        let (callee, _) = callee(&caller, "1", "1001");
        call.add_leg(&callee).unwrap();
        call.coordinator_generation = u64::MAX;

        assert_eq!(
            call.handoff_coordinator(&caller.id, u64::MAX, &callee.id),
            Err(CallError::CoordinatorGenerationExhausted)
        );
        assert_eq!(call.coordinator(), &caller.id);
        assert_eq!(call.coordinator_generation(), u64::MAX);
    }
}
