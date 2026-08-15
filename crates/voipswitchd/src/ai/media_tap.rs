use super::AiConnector;
use ai_protocol::control::{AudioCodec, JobRef, MediaDirection};
use ai_protocol::id::{ParticipantId, StreamId};
use ai_protocol::media::{MediaFrame, MediaFrameMetadata};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AiTapSide {
    Caller,
    Callee,
}

#[derive(Debug, Clone)]
pub(crate) struct AiTapStreamSpec {
    pub(crate) participant_id: ParticipantId,
    pub(crate) stream_id: StreamId,
    pub(crate) payload_type: u8,
    pub(crate) codec: AudioCodec,
    pub(crate) sample_rate: u32,
    pub(crate) channels: u8,
}

#[derive(Debug, Clone)]
pub(crate) struct AiMediaTapSpec {
    pub(crate) job: JobRef,
    pub(crate) caller: AiTapStreamSpec,
    pub(crate) callee: AiTapStreamSpec,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AiTapStats {
    pub(crate) packets_tapped: u64,
    pub(crate) packets_dropped: u64,
    pub(crate) packets_ignored: u64,
}

pub(crate) struct AiCaptureFinalized {
    pub(crate) job: JobRef,
    pub(crate) final_sequences: BTreeMap<StreamId, u64>,
    pub(crate) stats: AiTapStats,
}

struct StreamRuntime {
    spec: AiTapStreamSpec,
    sequence: AtomicU64,
}

struct AiMediaTapInner {
    connector: AiConnector,
    job: JobRef,
    caller: StreamRuntime,
    callee: StreamRuntime,
    packets_tapped: AtomicU64,
    packets_dropped: AtomicU64,
    packets_ignored: AtomicU64,
}

#[derive(Clone)]
pub(crate) struct AiMediaTapSender {
    inner: Arc<AiMediaTapInner>,
}

impl AiMediaTapSender {
    pub(crate) fn new(connector: AiConnector, spec: AiMediaTapSpec) -> Self {
        Self {
            inner: Arc::new(AiMediaTapInner {
                connector,
                job: spec.job,
                caller: StreamRuntime {
                    spec: spec.caller,
                    sequence: AtomicU64::new(0),
                },
                callee: StreamRuntime {
                    spec: spec.callee,
                    sequence: AtomicU64::new(0),
                },
                packets_tapped: AtomicU64::new(0),
                packets_dropped: AtomicU64::new(0),
                packets_ignored: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn tap(&self, side: AiTapSide, packet: &[u8]) {
        let stream = match side {
            AiTapSide::Caller => &self.inner.caller,
            AiTapSide::Callee => &self.inner.callee,
        };
        let Some(parsed) = parse_rtp_audio_payload(packet) else {
            self.inner.packets_ignored.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if parsed.payload_type != stream.spec.payload_type {
            self.inner.packets_ignored.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let sequence = stream.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let samples = match stream.spec.codec {
            AudioCodec::Pcma | AudioCodec::Pcmu => parsed.payload.len(),
            AudioCodec::Pcm16Le => parsed.payload.len() / 2,
        };
        let duration_ms = ((samples as u64 * 1_000) / u64::from(stream.spec.sample_rate))
            .clamp(1, u64::from(u16::MAX)) as u16;
        let frame = MediaFrame {
            metadata: MediaFrameMetadata {
                job_id: self.inner.job.job_id.clone(),
                tenant_id: self.inner.job.tenant_id.clone(),
                conversation_id: self.inner.job.conversation_id.clone(),
                participant_id: stream.spec.participant_id.clone(),
                stream_id: stream.spec.stream_id.clone(),
                sequence,
                generation: self.inner.job.generation,
                direction: MediaDirection::FromParticipant,
                codec: stream.spec.codec,
                sample_rate: stream.spec.sample_rate,
                channels: stream.spec.channels,
                media_timestamp: u64::from(parsed.timestamp),
                duration_ms,
                end_of_stream: false,
            },
            payload: parsed.payload.to_vec(),
        };
        if self.inner.connector.try_send_media(frame) {
            self.inner.packets_tapped.fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner.packets_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn finish(&self) -> AiCaptureFinalized {
        AiCaptureFinalized {
            job: self.inner.job.clone(),
            final_sequences: BTreeMap::from([
                (
                    self.inner.caller.spec.stream_id.clone(),
                    self.inner.caller.sequence.load(Ordering::Relaxed),
                ),
                (
                    self.inner.callee.spec.stream_id.clone(),
                    self.inner.callee.sequence.load(Ordering::Relaxed),
                ),
            ]),
            stats: AiTapStats {
                packets_tapped: self.inner.packets_tapped.load(Ordering::Relaxed),
                packets_dropped: self.inner.packets_dropped.load(Ordering::Relaxed),
                packets_ignored: self.inner.packets_ignored.load(Ordering::Relaxed),
            },
        }
    }
}

struct ParsedRtpPayload<'a> {
    payload_type: u8,
    timestamp: u32,
    payload: &'a [u8],
}

fn parse_rtp_audio_payload(packet: &[u8]) -> Option<ParsedRtpPayload<'_>> {
    if packet.len() < 12 || packet[0] >> 6 != 2 {
        return None;
    }
    let csrc_count = usize::from(packet[0] & 0x0f);
    let mut offset = 12_usize.checked_add(csrc_count.checked_mul(4)?)?;
    if offset > packet.len() {
        return None;
    }
    if packet[0] & 0x10 != 0 {
        if offset + 4 > packet.len() {
            return None;
        }
        let extension_words = u16::from_be_bytes([packet[offset + 2], packet[offset + 3]]);
        offset = offset.checked_add(4 + usize::from(extension_words).checked_mul(4)?)?;
        if offset > packet.len() {
            return None;
        }
    }
    let padding = if packet[0] & 0x20 != 0 {
        usize::from(*packet.last()?)
    } else {
        0
    };
    if padding > packet.len().saturating_sub(offset) {
        return None;
    }
    let payload_end = packet.len() - padding;
    if offset == payload_end {
        return None;
    }
    Some(ParsedRtpPayload {
        payload_type: packet[1] & 0x7f,
        timestamp: u32::from_be_bytes(packet[4..8].try_into().ok()?),
        payload: &packet[offset..payload_end],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_payload_after_csrc_extension_and_padding() {
        let mut packet = vec![0xb1, 0x80, 0, 1, 0, 0, 0x03, 0x20, 0, 0, 0, 1];
        packet.extend_from_slice(&[0, 0, 0, 2]);
        packet.extend_from_slice(&[0xbe, 0xde, 0, 1, 1, 2, 3, 4]);
        packet.extend_from_slice(&[0x7f; 160]);
        packet.extend_from_slice(&[0, 2]);
        let parsed = parse_rtp_audio_payload(&packet).unwrap();
        assert_eq!(parsed.payload_type, 0);
        assert_eq!(parsed.timestamp, 800);
        assert_eq!(parsed.payload, &[0x7f; 160]);
    }

    #[test]
    fn rejects_non_rtp_and_empty_payload() {
        assert!(parse_rtp_audio_payload(&[0; 12]).is_none());
        let packet = [0x80, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1];
        assert!(parse_rtp_audio_payload(&packet).is_none());
    }
}
