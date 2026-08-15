use crate::data_store::CdrWriteCommand;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const SPOOL_SCHEMA_VERSION: u16 = 1;
const SEGMENT_SIZE_LIMIT: u64 = 16 * 1024 * 1024;
const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;
const FRAME_HEADER_SIZE: usize = 8;

#[derive(Debug, Serialize, Deserialize)]
struct SpoolFrame {
    schema_version: u16,
    command: CdrWriteCommand,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct SpoolPosition {
    segment: u64,
    offset: u64,
}

#[derive(Debug)]
pub(super) enum ReplayEntry {
    Write {
        end: SpoolPosition,
        command: Box<CdrWriteCommand>,
    },
    Quarantined {
        end: SpoolPosition,
        reason: String,
    },
}

impl ReplayEntry {
    pub(super) fn end(&self) -> SpoolPosition {
        match self {
            Self::Write { end, .. } | Self::Quarantined { end, .. } => *end,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct CdrSpool {
    root: PathBuf,
}

impl CdrSpool {
    pub(super) fn open(root: PathBuf) -> Result<Self> {
        fs::create_dir_all(root.join("quarantine"))
            .with_context(|| format!("create CDR spool directory {}", root.display()))?;
        Ok(Self { root })
    }

    pub(super) fn append(&self, command: &CdrWriteCommand) -> Result<SpoolPosition> {
        let payload = serde_json::to_vec(&SpoolFrame {
            schema_version: SPOOL_SCHEMA_VERSION,
            command: command.clone(),
        })?;
        if payload.len() > MAX_FRAME_SIZE {
            bail!("CDR spool frame exceeds maximum size: {}", payload.len());
        }
        let payload_len = u32::try_from(payload.len()).context("CDR spool frame too large")?;
        let mut frame = Vec::with_capacity(FRAME_HEADER_SIZE + payload.len());
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        frame.extend_from_slice(&payload);

        let checkpoint = self.read_checkpoint()?;
        let segments = self.segment_numbers()?;
        let mut segment = segments
            .last()
            .copied()
            .unwrap_or_else(|| checkpoint.segment.saturating_add(1).max(1));
        let mut path = self.segment_path(segment);
        let mut offset = path.metadata().map(|meta| meta.len()).unwrap_or(0);
        if offset > 0 && offset.saturating_add(frame.len() as u64) > SEGMENT_SIZE_LIMIT {
            segment = segment.saturating_add(1);
            path = self.segment_path(segment);
            offset = 0;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("open CDR spool segment {}", path.display()))?;
        file.write_all(&frame)?;
        file.sync_data()?;
        Ok(SpoolPosition {
            segment,
            offset: offset.saturating_add(frame.len() as u64),
        })
    }

    pub(super) fn replay(&self) -> Result<Vec<ReplayEntry>> {
        let checkpoint = self.read_checkpoint()?;
        let segments = self.segment_numbers()?;
        let latest = segments.last().copied();
        let mut entries = Vec::new();

        for segment in segments {
            if segment < checkpoint.segment {
                continue;
            }
            let path = self.segment_path(segment);
            let mut bytes = Vec::new();
            File::open(&path)?.read_to_end(&mut bytes)?;
            let mut offset = if segment == checkpoint.segment {
                usize::try_from(checkpoint.offset).context("CDR spool checkpoint out of range")?
            } else {
                0
            };
            if offset > bytes.len() {
                bail!(
                    "CDR spool checkpoint {} exceeds segment {} length {}",
                    offset,
                    segment,
                    bytes.len()
                );
            }

            while offset < bytes.len() {
                let frame_start = offset;
                if bytes.len() - offset < FRAME_HEADER_SIZE {
                    self.recover_incomplete_tail(&path, segment, frame_start, latest)?;
                    break;
                }
                let payload_len = u32::from_le_bytes(
                    bytes[offset..offset + 4]
                        .try_into()
                        .expect("four-byte spool length"),
                ) as usize;
                let expected_crc = u32::from_le_bytes(
                    bytes[offset + 4..offset + 8]
                        .try_into()
                        .expect("four-byte spool CRC"),
                );
                offset += FRAME_HEADER_SIZE;
                if payload_len > MAX_FRAME_SIZE {
                    self.quarantine_suffix(
                        segment,
                        frame_start,
                        &bytes[frame_start..],
                        "frame length exceeds maximum",
                    )?;
                    truncate_file(&path, frame_start as u64)?;
                    break;
                }
                if bytes.len() - offset < payload_len {
                    self.recover_incomplete_tail(&path, segment, frame_start, latest)?;
                    break;
                }
                let payload = &bytes[offset..offset + payload_len];
                offset += payload_len;
                let end = SpoolPosition {
                    segment,
                    offset: offset as u64,
                };
                let raw_frame = &bytes[frame_start..offset];

                if crc32fast::hash(payload) != expected_crc {
                    let reason = "CRC mismatch".to_string();
                    self.quarantine_frame(segment, frame_start, raw_frame, &reason)?;
                    entries.push(ReplayEntry::Quarantined { end, reason });
                    continue;
                }

                match serde_json::from_slice::<SpoolFrame>(payload) {
                    Ok(frame) if frame.schema_version == SPOOL_SCHEMA_VERSION => {
                        entries.push(ReplayEntry::Write {
                            end,
                            command: Box::new(frame.command),
                        });
                    }
                    Ok(frame) => {
                        let reason = format!(
                            "unsupported schema version {} (expected {})",
                            frame.schema_version, SPOOL_SCHEMA_VERSION
                        );
                        self.quarantine_frame(segment, frame_start, raw_frame, &reason)?;
                        entries.push(ReplayEntry::Quarantined { end, reason });
                    }
                    Err(err) => {
                        let reason = format!("invalid JSON payload: {err}");
                        self.quarantine_frame(segment, frame_start, raw_frame, &reason)?;
                        entries.push(ReplayEntry::Quarantined { end, reason });
                    }
                }
            }
        }
        Ok(entries)
    }

    pub(super) fn acknowledge(&self, position: SpoolPosition) -> Result<()> {
        let current = self.read_checkpoint()?;
        if position < current {
            bail!("CDR spool checkpoint cannot move backwards");
        }
        let checkpoint_path = self.root.join("checkpoint.json");
        let temporary_path = self.root.join("checkpoint.json.tmp");
        let bytes = serde_json::to_vec(&position)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary_path, &checkpoint_path)?;
        sync_directory(&self.root)?;

        for segment in self.segment_numbers()? {
            if segment < position.segment {
                fs::remove_file(self.segment_path(segment))?;
            }
        }
        Ok(())
    }

    pub(super) fn backlog_bytes(&self) -> Result<u64> {
        let checkpoint = self.read_checkpoint()?;
        let mut bytes = 0_u64;
        for segment in self.segment_numbers()? {
            if segment < checkpoint.segment {
                continue;
            }
            let length = self.segment_path(segment).metadata()?.len();
            bytes = bytes.saturating_add(if segment == checkpoint.segment {
                length.saturating_sub(checkpoint.offset)
            } else {
                length
            });
        }
        Ok(bytes)
    }

    fn read_checkpoint(&self) -> Result<SpoolPosition> {
        let path = self.root.join("checkpoint.json");
        match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .with_context(|| format!("parse CDR spool checkpoint {}", path.display())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(SpoolPosition {
                segment: 0,
                offset: 0,
            }),
            Err(err) => Err(err.into()),
        }
    }

    fn segment_numbers(&self) -> Result<Vec<u64>> {
        let mut segments = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if let Some(sequence) = parse_segment_name(&name) {
                segments.push(sequence);
            }
        }
        segments.sort_unstable();
        Ok(segments)
    }

    fn segment_path(&self, segment: u64) -> PathBuf {
        self.root.join(format!("segment-{segment:020}.log"))
    }

    fn recover_incomplete_tail(
        &self,
        path: &Path,
        segment: u64,
        offset: usize,
        latest: Option<u64>,
    ) -> Result<()> {
        if latest != Some(segment) {
            let mut bytes = Vec::new();
            File::open(path)?.read_to_end(&mut bytes)?;
            self.quarantine_suffix(
                segment,
                offset,
                &bytes[offset..],
                "incomplete frame in closed segment",
            )?;
        }
        truncate_file(path, offset as u64)
    }

    fn quarantine_frame(
        &self,
        segment: u64,
        offset: usize,
        frame: &[u8],
        reason: &str,
    ) -> Result<()> {
        self.quarantine_suffix(segment, offset, frame, reason)
    }

    fn quarantine_suffix(
        &self,
        segment: u64,
        offset: usize,
        bytes: &[u8],
        reason: &str,
    ) -> Result<()> {
        let base = self
            .root
            .join("quarantine")
            .join(format!("segment-{segment:020}-offset-{offset:020}"));
        fs::write(base.with_extension("frame"), bytes)?;
        fs::write(base.with_extension("reason"), reason.as_bytes())?;
        Ok(())
    }
}

fn parse_segment_name(name: &str) -> Option<u64> {
    name.strip_prefix("segment-")?
        .strip_suffix(".log")?
        .parse()
        .ok()
}

fn truncate_file(path: &Path, length: u64) -> Result<()> {
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.set_len(length)?;
    file.seek(SeekFrom::Start(length))?;
    file.sync_all()?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_store::CdrRecord;

    fn command(call_id: &str) -> CdrWriteCommand {
        CdrWriteCommand {
            call_cdr: CdrRecord {
                call_id: call_id.to_string(),
                domain_id: "domain-a".to_string(),
                caller_number: "1001".to_string(),
                callee_number: "1002".to_string(),
                inbound_route_id: None,
                inbound_route_name: None,
                inbound_trunk_ref: None,
                inbound_trunk_name: None,
                outbound_route_id: None,
                outbound_route_name: None,
                outbound_trunk_ref: None,
                outbound_trunk_name: None,
                started_at_ms: 100,
                answered_at_ms: None,
                ended_at_ms: 200,
                duration_ms: 100,
                billable_ms: 0,
                answered: false,
                final_status: Some(480),
                hangup_cause: "unavailable".to_string(),
                media_forwarding_mode: None,
                caller_to_callee_packets: 0,
                caller_to_callee_bytes: 0,
                callee_to_caller_packets: 0,
                callee_to_caller_bytes: 0,
                caller_to_callee_rtcp_packets: 0,
                callee_to_caller_rtcp_packets: 0,
                trace_available: false,
                trace_incomplete: false,
                recording_status: None,
                recording_available: false,
                incomplete: false,
                incomplete_reason: None,
            },
            leg_cdrs: Vec::new(),
            recording: None,
            trace_call_id: call_id.to_string(),
            trace_domain_id: "domain-a".to_string(),
            trace_ended_at_ms: 200,
        }
    }

    #[test]
    fn appends_replays_and_checkpoints_frames() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let spool = CdrSpool::open(temp.path().to_path_buf())?;
        let first = spool.append(&command("call-1"))?;
        let second = spool.append(&command("call-2"))?;
        assert!(spool.backlog_bytes()? > 0);

        let entries = spool.replay()?;
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].end(), first);
        assert_eq!(entries[1].end(), second);
        spool.acknowledge(first)?;
        let entries = spool.replay()?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].end(), second);
        spool.acknowledge(second)?;
        assert_eq!(spool.backlog_bytes()?, 0);
        Ok(())
    }

    #[test]
    fn truncates_an_incomplete_tail_without_replaying_it() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let spool = CdrSpool::open(temp.path().to_path_buf())?;
        let valid = spool.append(&command("call-1"))?;
        let path = spool.segment_path(valid.segment);
        let mut file = OpenOptions::new().append(true).open(&path)?;
        file.write_all(&[1, 2, 3, 4])?;
        file.sync_all()?;

        let entries = spool.replay()?;
        assert_eq!(entries.len(), 1);
        assert_eq!(path.metadata()?.len(), valid.offset);
        Ok(())
    }
}
