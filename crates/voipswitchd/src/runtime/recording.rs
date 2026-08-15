use crate::app::AppState;
use crate::data_store::RecordingRecord;
use anyhow::{Context, Result, anyhow, ensure};
use chrono::{Local, TimeZone};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::thread::JoinHandle;
use std::time::Instant;
use voipswitch_core::types::time::unix_timestamp_ms;

const SAMPLE_RATE: u32 = 8_000;
const CHANNELS: u16 = 2;
const BITS_PER_SAMPLE: u16 = 16;
const RECORDING_QUEUE_CAPACITY: usize = 2048;
const MAX_TIMESTAMP_GAP_SAMPLES: usize = SAMPLE_RATE as usize * 5;
const CLEANUP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

pub fn start_cleanup_task(state: AppState) {
    tokio::spawn(async move {
        loop {
            let config = state.config().snapshot();
            let backend = state.backend();
            let retention_days = config.recording_retention_days();
            let max_size_gb = config.recording_max_size_gb();
            match tokio::task::spawn_blocking(move || {
                backend.cleanup_recordings(retention_days, max_size_gb, unix_timestamp_ms())
            })
            .await
            {
                Ok(Ok(expired)) if expired > 0 => {
                    tracing::info!(expired, "expired old recording files");
                }
                Ok(Ok(_)) => {}
                Ok(Err(err)) => tracing::warn!(error = %err, "recording cleanup failed"),
                Err(err) => tracing::warn!(error = %err, "recording cleanup task failed"),
            }
            tokio::time::sleep(CLEANUP_INTERVAL).await;
        }
    });
}

#[derive(Debug, Clone)]
pub struct RecordingSpec {
    pub call_id: String,
    pub domain_id: String,
    pub caller_number: String,
    pub callee_number: String,
    pub started_at_ms: u64,
    pub recording_dir: PathBuf,
    pub payload_types: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub enum RecordingSide {
    Caller,
    Callee,
}

#[derive(Clone)]
pub struct RecordingTapSender {
    sender: SyncSender<RecorderMessage>,
    packets_tapped: Arc<AtomicU64>,
    packets_dropped: Arc<AtomicU64>,
}

impl RecordingTapSender {
    pub fn tap(&self, side: RecordingSide, packet: &[u8], observed_at: Instant) {
        let message = RecorderMessage::Packet {
            side,
            packet: packet.to_vec(),
            observed_at,
        };
        match self.sender.try_send(message) {
            Ok(()) => {
                self.packets_tapped.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.packets_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

pub struct RecordingSession {
    sender: SyncSender<RecorderMessage>,
    join: Option<JoinHandle<RecordingRecord>>,
    tap: RecordingTapSender,
    spec: RecordingSpec,
}

impl RecordingSession {
    pub fn start(spec: RecordingSpec) -> Result<Self> {
        ensure!(
            spec.payload_types
                .iter()
                .any(|payload| matches!(payload, 0 | 8)),
            "unsupported_codec: recording requires PCMU or PCMA"
        );
        let paths = RecordingPaths::create(&spec)?;
        let (sender, receiver) = mpsc::sync_channel(RECORDING_QUEUE_CAPACITY);
        let packets_tapped = Arc::new(AtomicU64::new(0));
        let packets_dropped = Arc::new(AtomicU64::new(0));
        let tap = RecordingTapSender {
            sender: sender.clone(),
            packets_tapped: packets_tapped.clone(),
            packets_dropped: packets_dropped.clone(),
        };
        let thread_spec = spec.clone();
        let join = std::thread::Builder::new()
            .name(format!("recorder-{}", safe_component(&spec.call_id)))
            .spawn(move || {
                let result = run_recorder(&thread_spec, &paths, receiver);
                match result {
                    Ok(mut record) => {
                        record.packets_tapped = packets_tapped.load(Ordering::Relaxed);
                        record.packets_dropped = packets_dropped.load(Ordering::Relaxed);
                        if record.packets_dropped > 0 && record.status == "complete" {
                            record.status = "incomplete".to_string();
                            record.error_code = Some("recording_queue_overflow".to_string());
                            record.error_message =
                                Some("recording packet queue overflowed".to_string());
                        }
                        record
                    }
                    Err(err) => failed_record(&thread_spec, &paths, err),
                }
            })
            .context("spawn recording writer thread")?;
        Ok(Self {
            sender,
            join: Some(join),
            tap,
            spec,
        })
    }

    pub fn tap_sender(&self) -> RecordingTapSender {
        self.tap.clone()
    }

    pub async fn finish(mut self) -> RecordingRecord {
        drop(self.tap);
        drop(self.sender);
        let join = self.join.take().expect("recording join handle exists");
        let spec = self.spec;
        match tokio::task::spawn_blocking(move || join.join()).await {
            Ok(Ok(record)) => record,
            Ok(Err(_)) => panic_record(&spec, "recording writer thread panicked"),
            Err(err) => panic_record(&spec, &format!("recording writer join failed: {err}")),
        }
    }
}

enum RecorderMessage {
    Packet {
        side: RecordingSide,
        packet: Vec<u8>,
        observed_at: Instant,
    },
}

struct RecordingPaths {
    root: PathBuf,
    final_path: PathBuf,
    wav_part: PathBuf,
    caller_raw: PathBuf,
    callee_raw: PathBuf,
    file_name: String,
}

impl RecordingPaths {
    fn create(spec: &RecordingSpec) -> Result<Self> {
        ensure!(
            spec.recording_dir.is_absolute(),
            "recording_dir must be absolute"
        );
        fs::create_dir_all(&spec.recording_dir)
            .with_context(|| format!("create {}", spec.recording_dir.display()))?;
        let recording_root = fs::canonicalize(&spec.recording_dir)
            .with_context(|| format!("resolve {}", spec.recording_dir.display()))?;
        let started = Local
            .timestamp_millis_opt(i64::try_from(spec.started_at_ms)?)
            .single()
            .ok_or_else(|| anyhow!("invalid recording start timestamp"))?;
        let directory = recording_root
            .join(safe_component(&spec.domain_id))
            .join(started.format("%Y").to_string())
            .join(started.format("%m").to_string())
            .join(started.format("%d").to_string());
        fs::create_dir_all(&directory)
            .with_context(|| format!("create {}", directory.display()))?;
        let file_name = format!(
            "{}_{}_{}_{}.wav",
            started.format("%Y%m%d-%H%M%S"),
            safe_number(&spec.caller_number),
            safe_number(&spec.callee_number),
            safe_component(&spec.call_id)
        );
        let final_path = directory.join(&file_name);
        Ok(Self {
            root: recording_root,
            wav_part: final_path.with_extension("wav.part"),
            caller_raw: final_path.with_extension("caller.raw.part"),
            callee_raw: final_path.with_extension("callee.raw.part"),
            final_path,
            file_name,
        })
    }
}

fn run_recorder(
    spec: &RecordingSpec,
    paths: &RecordingPaths,
    receiver: mpsc::Receiver<RecorderMessage>,
) -> Result<RecordingRecord> {
    let started = Instant::now();
    let mut caller = ChannelWriter::create(&paths.caller_raw, started)?;
    let mut callee = ChannelWriter::create(&paths.callee_raw, started)?;
    while let Ok(message) = receiver.recv() {
        match message {
            RecorderMessage::Packet {
                side,
                packet,
                observed_at,
            } => {
                let Some(parsed) = parse_rtp(&packet) else {
                    continue;
                };
                if !spec.payload_types.contains(&parsed.payload_type) {
                    continue;
                }
                let samples: Vec<i16> = match parsed.payload_type {
                    0 => parsed
                        .payload
                        .iter()
                        .map(|byte| decode_mulaw(*byte))
                        .collect(),
                    8 => parsed
                        .payload
                        .iter()
                        .map(|byte| decode_alaw(*byte))
                        .collect(),
                    _ => continue,
                };
                match side {
                    RecordingSide::Caller => {
                        caller.write_packet(parsed.timestamp, &samples, observed_at)?
                    }
                    RecordingSide::Callee => {
                        callee.write_packet(parsed.timestamp, &samples, observed_at)?
                    }
                }
            }
        }
    }
    let caller_samples = caller.finish()?;
    let callee_samples = callee.finish()?;
    let total_samples = caller_samples.max(callee_samples);
    let ended_at_ms = unix_timestamp_ms();
    if total_samples == 0 {
        let _ = fs::remove_file(&paths.caller_raw);
        let _ = fs::remove_file(&paths.callee_raw);
        return Ok(RecordingRecord {
            recording_id: format!("recording-{}", spec.call_id),
            call_id: spec.call_id.clone(),
            domain_id: spec.domain_id.clone(),
            status: "incomplete".to_string(),
            caller_number: spec.caller_number.clone(),
            callee_number: spec.callee_number.clone(),
            started_at_ms: spec.started_at_ms,
            ended_at_ms: Some(ended_at_ms),
            duration_ms: 0,
            format: "wav".to_string(),
            sample_rate: SAMPLE_RATE,
            channel_count: CHANNELS as u8,
            file_name: paths.file_name.clone(),
            storage_root: paths.root.to_string_lossy().into_owned(),
            storage_path: String::new(),
            file_size_bytes: 0,
            packets_tapped: 0,
            packets_dropped: 0,
            error_code: Some("no_media".to_string()),
            error_message: Some("no supported RTP payload was recorded".to_string()),
        });
    }
    interleave_wav(paths, caller_samples, callee_samples)?;
    let file_size_bytes = fs::metadata(&paths.final_path)?.len();
    Ok(RecordingRecord {
        recording_id: format!("recording-{}", spec.call_id),
        call_id: spec.call_id.clone(),
        domain_id: spec.domain_id.clone(),
        status: "complete".to_string(),
        caller_number: spec.caller_number.clone(),
        callee_number: spec.callee_number.clone(),
        started_at_ms: spec.started_at_ms,
        ended_at_ms: Some(ended_at_ms),
        duration_ms: total_samples.saturating_mul(1000) / u64::from(SAMPLE_RATE),
        format: "wav".to_string(),
        sample_rate: SAMPLE_RATE,
        channel_count: CHANNELS as u8,
        file_name: paths.file_name.clone(),
        storage_root: paths.root.to_string_lossy().into_owned(),
        storage_path: paths.final_path.to_string_lossy().into_owned(),
        file_size_bytes,
        packets_tapped: 0,
        packets_dropped: 0,
        error_code: None,
        error_message: None,
    })
}

struct ChannelWriter {
    writer: BufWriter<File>,
    started: Instant,
    samples_written: u64,
    last_timestamp: Option<u32>,
    last_sample_count: usize,
}

impl ChannelWriter {
    fn create(path: &Path, started: Instant) -> Result<Self> {
        Ok(Self {
            writer: BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(path)?,
            ),
            started,
            samples_written: 0,
            last_timestamp: None,
            last_sample_count: 0,
        })
    }

    fn write_packet(
        &mut self,
        timestamp: u32,
        samples: &[i16],
        observed_at: Instant,
    ) -> Result<()> {
        let mut offset = 0;
        if let Some(last_timestamp) = self.last_timestamp {
            let delta = timestamp.wrapping_sub(last_timestamp) as usize;
            if delta >= self.last_sample_count {
                let gap = delta - self.last_sample_count;
                if gap <= MAX_TIMESTAMP_GAP_SAMPLES {
                    self.write_silence(gap)?;
                }
            } else {
                offset = (self.last_sample_count - delta).min(samples.len());
            }
        } else {
            let leading = observed_at
                .saturating_duration_since(self.started)
                .as_millis()
                .saturating_mul(u128::from(SAMPLE_RATE))
                / 1000;
            self.write_silence(usize::try_from(leading.min(u128::from(u32::MAX)))?)?;
        }
        for sample in &samples[offset..] {
            self.writer.write_all(&sample.to_le_bytes())?;
        }
        self.samples_written = self
            .samples_written
            .saturating_add(u64::try_from(samples.len().saturating_sub(offset))?);
        self.last_timestamp = Some(timestamp);
        self.last_sample_count = samples.len();
        Ok(())
    }

    fn write_silence(&mut self, samples: usize) -> Result<()> {
        const SILENCE: [u8; 512] = [0; 512];
        let mut bytes = samples.saturating_mul(2);
        while bytes > 0 {
            let chunk = bytes.min(SILENCE.len());
            self.writer.write_all(&SILENCE[..chunk])?;
            bytes -= chunk;
        }
        self.samples_written = self.samples_written.saturating_add(samples as u64);
        Ok(())
    }

    fn finish(mut self) -> Result<u64> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(self.samples_written)
    }
}

fn interleave_wav(paths: &RecordingPaths, caller_samples: u64, callee_samples: u64) -> Result<()> {
    let total_samples = caller_samples.max(callee_samples);
    let data_bytes = total_samples
        .checked_mul(u64::from(CHANNELS))
        .and_then(|value| value.checked_mul(u64::from(BITS_PER_SAMPLE / 8)))
        .ok_or_else(|| anyhow!("recording WAV size overflow"))?;
    ensure!(
        data_bytes <= u64::from(u32::MAX) - 36,
        "recording WAV exceeds RIFF size limit"
    );
    let mut output = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&paths.wav_part)?,
    );
    write_wav_header(&mut output, u32::try_from(data_bytes)?)?;
    let mut caller = BufReader::new(File::open(&paths.caller_raw)?);
    let mut callee = BufReader::new(File::open(&paths.callee_raw)?);
    for _ in 0..total_samples {
        output.write_all(&read_sample_or_silence(&mut caller)?)?;
        output.write_all(&read_sample_or_silence(&mut callee)?)?;
    }
    output.flush()?;
    output.get_ref().sync_all()?;
    fs::rename(&paths.wav_part, &paths.final_path)?;
    let _ = fs::remove_file(&paths.caller_raw);
    let _ = fs::remove_file(&paths.callee_raw);
    Ok(())
}

fn write_wav_header(writer: &mut (impl Write + Seek), data_bytes: u32) -> Result<()> {
    writer.seek(SeekFrom::Start(0))?;
    writer.write_all(b"RIFF")?;
    writer.write_all(&(36_u32 + data_bytes).to_le_bytes())?;
    writer.write_all(b"WAVEfmt ")?;
    writer.write_all(&16_u32.to_le_bytes())?;
    writer.write_all(&1_u16.to_le_bytes())?;
    writer.write_all(&CHANNELS.to_le_bytes())?;
    writer.write_all(&SAMPLE_RATE.to_le_bytes())?;
    let byte_rate = SAMPLE_RATE * u32::from(CHANNELS) * u32::from(BITS_PER_SAMPLE / 8);
    writer.write_all(&byte_rate.to_le_bytes())?;
    let block_align = CHANNELS * (BITS_PER_SAMPLE / 8);
    writer.write_all(&block_align.to_le_bytes())?;
    writer.write_all(&BITS_PER_SAMPLE.to_le_bytes())?;
    writer.write_all(b"data")?;
    writer.write_all(&data_bytes.to_le_bytes())?;
    Ok(())
}

fn read_sample_or_silence(reader: &mut impl Read) -> Result<[u8; 2]> {
    let mut sample = [0_u8; 2];
    match reader.read_exact(&mut sample) {
        Ok(()) => Ok(sample),
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok([0, 0]),
        Err(err) => Err(err.into()),
    }
}

struct ParsedRtp<'a> {
    payload_type: u8,
    timestamp: u32,
    payload: &'a [u8],
}

fn parse_rtp(packet: &[u8]) -> Option<ParsedRtp<'_>> {
    if packet.len() < 12 || packet[0] >> 6 != 2 {
        return None;
    }
    let csrc_count = usize::from(packet[0] & 0x0f);
    let mut offset = 12 + csrc_count * 4;
    if packet.len() < offset {
        return None;
    }
    if packet[0] & 0x10 != 0 {
        if packet.len() < offset + 4 {
            return None;
        }
        let words = usize::from(u16::from_be_bytes([packet[offset + 2], packet[offset + 3]]));
        offset = offset.checked_add(4 + words * 4)?;
        if packet.len() < offset {
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
    Some(ParsedRtp {
        payload_type: packet[1] & 0x7f,
        timestamp: u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]),
        payload: &packet[offset..packet.len() - padding],
    })
}

fn decode_mulaw(value: u8) -> i16 {
    let value = !value;
    let sign = value & 0x80;
    let exponent = (value >> 4) & 0x07;
    let mantissa = value & 0x0f;
    let sample = (((i32::from(mantissa) << 3) + 0x84) << exponent) - 0x84;
    if sign != 0 {
        (-sample) as i16
    } else {
        sample as i16
    }
}

fn decode_alaw(value: u8) -> i16 {
    let value = value ^ 0x55;
    let sign = value & 0x80;
    let exponent = (value >> 4) & 0x07;
    let mantissa = value & 0x0f;
    let mut sample = (i32::from(mantissa) << 4) + 8;
    if exponent != 0 {
        sample += 0x100;
        sample <<= exponent - 1;
    }
    if sign != 0 {
        sample as i16
    } else {
        (-sample) as i16
    }
}

fn safe_number(value: &str) -> String {
    if value.trim().is_empty() {
        "unknown".to_string()
    } else {
        safe_component(value)
    }
}

fn safe_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        "unknown".to_string()
    } else {
        sanitized
    }
}

fn failed_record(
    spec: &RecordingSpec,
    paths: &RecordingPaths,
    error: anyhow::Error,
) -> RecordingRecord {
    let _ = fs::remove_file(&paths.wav_part);
    let _ = fs::remove_file(&paths.caller_raw);
    let _ = fs::remove_file(&paths.callee_raw);
    let ended_at_ms = unix_timestamp_ms();
    RecordingRecord {
        recording_id: format!("recording-{}", spec.call_id),
        call_id: spec.call_id.clone(),
        domain_id: spec.domain_id.clone(),
        status: "failed".to_string(),
        caller_number: spec.caller_number.clone(),
        callee_number: spec.callee_number.clone(),
        started_at_ms: spec.started_at_ms,
        ended_at_ms: Some(ended_at_ms),
        duration_ms: ended_at_ms.saturating_sub(spec.started_at_ms),
        format: "wav".to_string(),
        sample_rate: SAMPLE_RATE,
        channel_count: CHANNELS as u8,
        file_name: paths.file_name.clone(),
        storage_root: paths.root.to_string_lossy().into_owned(),
        storage_path: String::new(),
        file_size_bytes: 0,
        packets_tapped: 0,
        packets_dropped: 0,
        error_code: Some("recording_io_error".to_string()),
        error_message: Some(error.to_string()),
    }
}

fn panic_record(spec: &RecordingSpec, message: &str) -> RecordingRecord {
    RecordingRecord {
        recording_id: format!("recording-{}", spec.call_id),
        call_id: spec.call_id.clone(),
        domain_id: spec.domain_id.clone(),
        status: "failed".to_string(),
        caller_number: spec.caller_number.clone(),
        callee_number: spec.callee_number.clone(),
        started_at_ms: spec.started_at_ms,
        ended_at_ms: Some(unix_timestamp_ms()),
        duration_ms: 0,
        format: "wav".to_string(),
        sample_rate: SAMPLE_RATE,
        channel_count: CHANNELS as u8,
        file_name: String::new(),
        storage_root: String::new(),
        storage_path: String::new(),
        file_size_bytes: 0,
        packets_tapped: 0,
        packets_dropped: 0,
        error_code: Some("recording_thread_failure".to_string()),
        error_message: Some(message.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rtp_and_decodes_g711() {
        let mut packet = vec![0x80, 0x00, 0, 1, 0, 0, 0, 160, 0, 0, 0, 1];
        packet.extend([0xff, 0x7f]);
        let parsed = parse_rtp(&packet).unwrap();
        assert_eq!(parsed.payload_type, 0);
        assert_eq!(parsed.timestamp, 160);
        assert_eq!(parsed.payload.len(), 2);
        assert_eq!(decode_mulaw(0xff), 0);
        assert_ne!(decode_alaw(0xd5), i16::MIN);
    }

    #[test]
    fn sanitizes_recording_file_components() {
        assert_eq!(safe_component("../../1001"), ".._.._1001");
        assert_eq!(safe_number(""), "unknown");
        assert_eq!(safe_component("call:1/2"), "call_1_2");
    }

    #[test]
    fn writes_stereo_wav_header() {
        let mut bytes = std::io::Cursor::new(Vec::new());
        write_wav_header(&mut bytes, 320).unwrap();
        let bytes = bytes.into_inner();
        assert_eq!(&bytes[0..4], b"RIFF");
        assert_eq!(&bytes[8..12], b"WAVE");
        assert_eq!(u16::from_le_bytes([bytes[22], bytes[23]]), 2);
        assert_eq!(
            u32::from_le_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]),
            8000
        );
    }
}
