use crate::types::ids::{CallId, DomainId, MediaLegId, SessionId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DtmfDigit {
    D0,
    D1,
    D2,
    D3,
    D4,
    D5,
    D6,
    D7,
    D8,
    D9,
    Star,
    Pound,
    A,
    B,
    C,
    D,
    Flash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DtmfTransport {
    Rfc4733,
    SipInfoRelay,
    SipInfoDtmf,
    InBand,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "generation")]
pub enum DtmfSourceGeneration {
    Media(u64),
    Dialog(u64),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum DtmfEventId {
    Rfc4733 {
        media_generation: u64,
        source_session_id: SessionId,
        ssrc: u32,
        timestamp: u32,
        event_code: u8,
    },
    SipInfo {
        dialog_generation: u64,
        source_session_id: SessionId,
        cseq: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigitEvent {
    pub event_id: DtmfEventId,
    pub domain_id: DomainId,
    pub call_id: CallId,
    pub source_session_id: SessionId,
    pub source_media_leg_id: Option<MediaLegId>,
    pub digit: DtmfDigit,
    pub transport: DtmfTransport,
    pub duration_ms: u32,
    pub observed_at_ms: u64,
    pub source_generation: DtmfSourceGeneration,
    pub incomplete_end: bool,
}

impl DigitEvent {
    pub fn identity_is_consistent(&self) -> bool {
        match (&self.event_id, self.transport, self.source_generation) {
            (
                DtmfEventId::Rfc4733 {
                    media_generation,
                    source_session_id,
                    ..
                },
                DtmfTransport::Rfc4733,
                DtmfSourceGeneration::Media(source_generation),
            ) => {
                *media_generation == source_generation
                    && source_session_id == &self.source_session_id
            }
            (
                DtmfEventId::SipInfo {
                    dialog_generation,
                    source_session_id,
                    ..
                },
                DtmfTransport::SipInfoRelay | DtmfTransport::SipInfoDtmf,
                DtmfSourceGeneration::Dialog(source_generation),
            ) => {
                *dialog_generation == source_generation
                    && source_session_id == &self.source_session_id
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DtmfSourcePolicy {
    Auto,
    Rfc4733Only,
    SipInfoOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum DtmfSourceLock {
    Unset,
    Rfc4733 { media_generation: u64 },
    SipInfo { dialog_generation: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rfc_event() -> DigitEvent {
        let source_session_id = SessionId::from("session-a");
        DigitEvent {
            event_id: DtmfEventId::Rfc4733 {
                media_generation: 2,
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
            source_generation: DtmfSourceGeneration::Media(2),
            incomplete_end: false,
        }
    }

    #[test]
    fn structured_event_identity_must_match_source_and_generation() {
        let event = rfc_event();
        assert!(event.identity_is_consistent());

        let mut wrong_generation = event.clone();
        wrong_generation.source_generation = DtmfSourceGeneration::Media(3);
        assert!(!wrong_generation.identity_is_consistent());

        let mut wrong_source = event;
        wrong_source.source_session_id = SessionId::from("session-b");
        assert!(!wrong_source.identity_is_consistent());
    }
}
