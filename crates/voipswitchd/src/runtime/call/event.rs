use serde::Deserialize;
use serde_json::Value;
use std::net::SocketAddr;
use voipswitch_core::dtmf::{
    DigitEvent, DtmfDigit, DtmfEventId, DtmfSourceGeneration, DtmfTransport,
};
use voipswitch_core::media::SdpBody;
use voipswitch_core::types::ids::{CallId, DomainId, SessionId};

#[derive(Debug, Deserialize)]
pub(crate) struct InboundInviteOffered {
    pub(crate) domain_id: String,
    pub(crate) adapter_call_leg_id: String,
    pub(crate) caller_number: String,
    pub(crate) callee_number: String,
    #[serde(default, alias = "source")]
    pub(crate) origin: Option<InboundInviteSource>,
    #[serde(default)]
    pub(crate) route_target: Option<SocketAddr>,
    pub(crate) sdp_offer: SdpBody,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum InboundInviteSource {
    Endpoint {
        #[serde(rename = "endpoint_id")]
        _endpoint_id: String,
    },
    Trunk {
        trunk_ref: String,
    },
}

#[derive(Debug, Deserialize)]
pub(crate) struct CallLegEvent {
    pub(crate) call_id: Option<String>,
    pub(crate) session_id: Option<String>,
    pub(crate) adapter_call_leg_id: String,
    #[serde(default)]
    pub(crate) leg_event_seq: u64,
    pub(crate) status_code: Option<u16>,
    pub(crate) sdp: Option<SdpBody>,
    pub(crate) reason: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SipDtmfReceived {
    pub(crate) domain_id: String,
    pub(crate) call_id: String,
    pub(crate) session_id: String,
    pub(crate) adapter_call_leg_id: String,
    pub(crate) leg_event_seq: u64,
    pub(crate) dialog_generation: u64,
    pub(crate) cseq: u32,
    pub(crate) digit: DtmfDigit,
    pub(crate) duration_ms: u32,
    pub(crate) content_mode: DtmfTransport,
    pub(crate) observed_at_ms: u64,
}

impl SipDtmfReceived {
    pub(crate) fn into_digit_event(self) -> Option<DigitEvent> {
        if !matches!(
            self.content_mode,
            DtmfTransport::SipInfoRelay | DtmfTransport::SipInfoDtmf
        ) {
            return None;
        }
        let source_session_id = SessionId::from(self.session_id);
        Some(DigitEvent {
            event_id: DtmfEventId::SipInfo {
                dialog_generation: self.dialog_generation,
                source_session_id: source_session_id.clone(),
                cseq: self.cseq,
            },
            domain_id: DomainId::from(self.domain_id),
            call_id: CallId::from(self.call_id),
            source_session_id,
            source_media_leg_id: None,
            digit: self.digit,
            transport: self.content_mode,
            duration_ms: self.duration_ms,
            observed_at_ms: self.observed_at_ms,
            source_generation: DtmfSourceGeneration::Dialog(self.dialog_generation),
            incomplete_end: false,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct DtmfInfoSendResult {
    pub(crate) call_id: String,
    pub(crate) session_id: String,
    pub(crate) adapter_call_leg_id: String,
    pub(crate) action_id: String,
    pub(crate) generation: u64,
    pub(crate) status_code: Option<u16>,
    pub(crate) reason: i64,
    pub(crate) succeeded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActionIdentity {
    pub(crate) call_id: String,
    pub(crate) session_id: String,
    pub(crate) action_kind: String,
    pub(crate) action_id: String,
    pub(crate) generation: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ActionDeliveryFailed {
    pub(crate) identity: ActionIdentity,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CallActionAckStatus {
    Accepted,
    Rejected,
    Duplicate,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CallActionAck {
    pub(crate) call_id: String,
    pub(crate) session_id: String,
    #[serde(rename = "action")]
    pub(crate) action_kind: String,
    pub(crate) action_id: String,
    pub(crate) generation: u64,
    pub(crate) status: CallActionAckStatus,
    #[serde(default)]
    pub(crate) result: Value,
}

impl CallActionAck {
    pub(crate) fn identity(&self) -> ActionIdentity {
        ActionIdentity {
            call_id: self.call_id.clone(),
            session_id: self.session_id.clone(),
            action_kind: self.action_kind.clone(),
            action_id: self.action_id.clone(),
            generation: self.generation,
        }
    }

    pub(crate) fn accepted(&self) -> bool {
        match self.status {
            CallActionAckStatus::Accepted => true,
            CallActionAckStatus::Rejected => false,
            CallActionAckStatus::Duplicate => self
                .result
                .get("original_status")
                .and_then(Value::as_str)
                .is_some_and(|status| status == "accepted"),
        }
    }

    pub(crate) fn adapter_call_leg_id(&self) -> Option<&str> {
        self.result
            .get("data")
            .and_then(|data| data.get("adapter_call_leg_id"))
            .and_then(Value::as_str)
    }
}

impl CallLegEvent {
    pub(crate) fn leg_event_seq(&self) -> u64 {
        self.leg_event_seq
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RouteAnalysisFailure {
    Timeout,
    Worker(String),
}

pub(crate) fn is_call_leg_event(frame_type: &str) -> bool {
    matches!(
        frame_type,
        "InboundInviteOffered"
            | "OutboundProvisional"
            | "OutboundAnswered"
            | "OutboundFailed"
            | "InboundInviteCancelled"
            | "DialogDisconnected"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sip_info_event(content_mode: DtmfTransport) -> SipDtmfReceived {
        SipDtmfReceived {
            domain_id: "domain-a".to_string(),
            call_id: "call-a".to_string(),
            session_id: "session-a".to_string(),
            adapter_call_leg_id: "17".to_string(),
            leg_event_seq: 9,
            dialog_generation: 17,
            cseq: 42,
            digit: DtmfDigit::D5,
            duration_ms: 180,
            content_mode,
            observed_at_ms: 123,
        }
    }

    #[test]
    fn sip_info_maps_to_consistent_digit_identity() {
        let event = sip_info_event(DtmfTransport::SipInfoRelay)
            .into_digit_event()
            .expect("supported SIP INFO mode");

        assert!(event.identity_is_consistent());
        assert_eq!(event.domain_id.as_str(), "domain-a");
        assert_eq!(event.call_id.as_str(), "call-a");
        assert_eq!(event.source_session_id.as_str(), "session-a");
        assert_eq!(event.source_generation, DtmfSourceGeneration::Dialog(17));
        assert_eq!(event.duration_ms, 180);
        assert!(matches!(
            event.event_id,
            DtmfEventId::SipInfo {
                dialog_generation: 17,
                cseq: 42,
                ..
            }
        ));
    }

    #[test]
    fn non_info_transport_is_not_accepted_as_sip_info() {
        assert!(
            sip_info_event(DtmfTransport::Rfc4733)
                .into_digit_event()
                .is_none()
        );
    }
}
