use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallDirection {
    Inbound,
    Outbound,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegDirection {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallState {
    Establishing,
    Answering,
    Connected,
    Terminating,
    Completed,
}

impl CallState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed)
    }

    pub fn is_terminating_or_completed(self) -> bool {
        matches!(self, Self::Terminating | Self::Completed)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Establishing => "establishing",
            Self::Answering => "answering",
            Self::Connected => "connected",
            Self::Terminating => "terminating",
            Self::Completed => "completed",
        }
    }
}

pub fn valid_call_state_transition(from: CallState, to: CallState) -> bool {
    use CallState::*;
    matches!(
        (from, to),
        (Establishing, Answering | Terminating)
            | (Answering, Connected | Terminating)
            | (Connected, Terminating)
            | (Terminating, Completed)
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegState {
    Idle,
    Routing,
    Dialing,
    WaitForAnswer,
    Bridged,
    HangingUp,
    Destroyed,
}

impl LegState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Destroyed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HangupCause {
    NormalClearing,
    OriginatorCancel,
    NoRoute,
    UnallocatedNumber,
    NoAnswer,
    NoUserResponse,
    UserBusy,
    UserNotRegistered,
    CallRejected,
    InvalidNumberFormat,
    ResourceLimit,
    LineBusy,
    IncompatibleDestination,
    NetworkFailure,
    TemporaryFailure,
    RecoveryOnTimerExpire,
    DomainDisabled,
    SystemShutdown,
    InternalError,
}

impl HangupCause {
    pub fn as_cdr_string(self) -> &'static str {
        match self {
            Self::NormalClearing => "normal_clearing",
            Self::OriginatorCancel => "originator_cancel",
            Self::NoRoute => "no_route",
            Self::UnallocatedNumber => "unallocated_number",
            Self::NoAnswer => "no_answer",
            Self::NoUserResponse => "no_user_response",
            Self::UserBusy => "user_busy",
            Self::UserNotRegistered => "user_not_registered",
            Self::CallRejected => "call_rejected",
            Self::InvalidNumberFormat => "invalid_number_format",
            Self::ResourceLimit => "resource_limit",
            Self::LineBusy => "line_busy",
            Self::IncompatibleDestination => "incompatible_destination",
            Self::NetworkFailure => "network_failure",
            Self::TemporaryFailure => "temporary_failure",
            Self::RecoveryOnTimerExpire => "recovery_on_timer_expire",
            Self::DomainDisabled => "domain_disabled",
            Self::SystemShutdown => "system_shutdown",
            Self::InternalError => "internal_error",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_state_allows_forward_progression() {
        assert!(valid_call_state_transition(
            CallState::Establishing,
            CallState::Answering
        ));
        assert!(valid_call_state_transition(
            CallState::Answering,
            CallState::Connected
        ));
        assert!(valid_call_state_transition(
            CallState::Connected,
            CallState::Terminating
        ));
        assert!(valid_call_state_transition(
            CallState::Terminating,
            CallState::Completed
        ));
    }

    #[test]
    fn call_state_allows_early_termination() {
        assert!(valid_call_state_transition(
            CallState::Establishing,
            CallState::Terminating
        ));
        assert!(valid_call_state_transition(
            CallState::Answering,
            CallState::Terminating
        ));
    }

    #[test]
    fn call_state_rejects_backward_and_skip() {
        assert!(!valid_call_state_transition(
            CallState::Connected,
            CallState::Establishing
        ));
        assert!(!valid_call_state_transition(
            CallState::Establishing,
            CallState::Connected
        ));
        assert!(!valid_call_state_transition(
            CallState::Completed,
            CallState::Terminating
        ));
        assert!(!valid_call_state_transition(
            CallState::Terminating,
            CallState::Connected
        ));
    }

    #[test]
    fn hangup_cause_cdr_string_is_snake_case() {
        assert_eq!(
            HangupCause::NormalClearing.as_cdr_string(),
            "normal_clearing"
        );
        assert_eq!(
            HangupCause::RecoveryOnTimerExpire.as_cdr_string(),
            "recovery_on_timer_expire"
        );
        assert_eq!(HangupCause::InternalError.as_cdr_string(), "internal_error");
    }
}
