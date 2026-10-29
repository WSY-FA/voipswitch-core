use crate::control::{AudioCodec, MediaDirection};
use crate::frame::{read_frame_len, write_frame};
use crate::id::{ConversationId, JobId, ParticipantId, StreamId, TenantId};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

pub const MAX_MEDIA_FRAME_LEN: usize = 256 * 1024;
pub const MAX_MEDIA_METADATA_LEN: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaFrameMetadata {
    pub job_id: JobId,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub participant_id: ParticipantId,
    pub stream_id: StreamId,
    pub sequence: u64,
    pub generation: u64,
    pub direction: MediaDirection,
    pub codec: AudioCodec,
    pub sample_rate: u32,
    pub channels: u8,
    pub media_timestamp: u64,
    pub duration_ms: u16,
    pub end_of_stream: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaFrame {
    pub metadata: MediaFrameMetadata,
    pub payload: Vec<u8>,
}

impl MediaFrame {
    pub fn validate(&self) -> Result<()> {
        if self.metadata.generation == 0 {
            bail!("media generation must be greater than zero");
        }
        if self.metadata.channels == 0 || self.metadata.channels > 2 {
            bail!("media channels must be 1 or 2");
        }
        if self.metadata.sample_rate == 0 || self.metadata.duration_ms == 0 {
            bail!("media sample rate and duration must be non-zero");
        }
        if self.payload.is_empty() && !self.metadata.end_of_stream {
            bail!("non-terminal media frame requires payload");
        }
        Ok(())
    }
}

pub async fn read_media_frame<R>(reader: &mut R) -> Result<MediaFrame>
where
    R: AsyncRead + Unpin,
{
    let total_len = read_frame_len(reader, MAX_MEDIA_FRAME_LEN).await?;
    if total_len < 4 {
        bail!("media frame too short");
    }
    let mut body = vec![0_u8; total_len];
    reader
        .read_exact(&mut body)
        .await
        .context("read media frame body")?;
    let metadata_len = u32::from_be_bytes(body[..4].try_into().unwrap()) as usize;
    if metadata_len == 0
        || metadata_len > MAX_MEDIA_METADATA_LEN
        || metadata_len > body.len().saturating_sub(4)
    {
        bail!("invalid media metadata length {metadata_len}");
    }
    let metadata =
        serde_json::from_slice(&body[4..4 + metadata_len]).context("decode media metadata")?;
    let frame = MediaFrame {
        metadata,
        payload: body[4 + metadata_len..].to_vec(),
    };
    frame.validate()?;
    Ok(frame)
}

pub async fn write_media_frame<W>(writer: &mut W, frame: &MediaFrame) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    frame.validate()?;
    let metadata = serde_json::to_vec(&frame.metadata).context("encode media metadata")?;
    if metadata.len() > MAX_MEDIA_METADATA_LEN {
        bail!("media metadata too large");
    }
    let metadata_len = u32::try_from(metadata.len()).context("metadata length exceeds u32")?;
    let mut body = Vec::with_capacity(4 + metadata.len() + frame.payload.len());
    body.extend_from_slice(&metadata_len.to_be_bytes());
    body.extend_from_slice(&metadata);
    body.extend_from_slice(&frame.payload);
    write_frame(writer, &body, MAX_MEDIA_FRAME_LEN).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id<T: TryFrom<String, Error = anyhow::Error>>(value: &str) -> T {
        value.to_string().try_into().unwrap()
    }

    #[tokio::test]
    async fn roundtrips_binary_media_frame() {
        let frame = MediaFrame {
            metadata: MediaFrameMetadata {
                job_id: id("job-1"),
                tenant_id: id("tenant-1"),
                conversation_id: id("conversation-1"),
                participant_id: id("participant-1"),
                stream_id: id("stream-1"),
                sequence: 4,
                generation: 1,
                direction: MediaDirection::FromParticipant,
                codec: AudioCodec::Pcmu,
                sample_rate: 8_000,
                channels: 1,
                media_timestamp: 640,
                duration_ms: 20,
                end_of_stream: false,
            },
            payload: vec![0x7f; 160],
        };
        let (mut client, mut server) = tokio::io::duplex(4096);
        let expected = frame.clone();
        let write = tokio::spawn(async move { write_media_frame(&mut client, &frame).await });
        let actual = read_media_frame(&mut server).await.unwrap();
        write.await.unwrap().unwrap();
        assert_eq!(actual, expected);
    }
}
