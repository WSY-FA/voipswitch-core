use ai_protocol::control::{
    ControlMessage, EndAudioInput, JobCompleted, JobRef, ResultPersisted, SubmitPostCallJob,
};
use ai_protocol::id::StreamId;
use ai_protocol::time::unix_timestamp_ms;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

const OUTBOX_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OutboxState {
    PendingSubmission,
    Accepted,
    InputEnded,
    Completed,
    ResultStored,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct OutboxRecord {
    schema_version: u16,
    pub(crate) submission: SubmitPostCallJob,
    pub(crate) state: OutboxState,
    #[serde(default)]
    pub(crate) final_sequences: BTreeMap<StreamId, u64>,
    #[serde(default)]
    pub(crate) completed: Option<JobCompleted>,
    #[serde(default)]
    pub(crate) completed_received_at_ms: Option<u64>,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
}

impl OutboxRecord {
    pub(crate) fn job(&self) -> &JobRef {
        &self.submission.job
    }

    pub(crate) fn pending_message(&self) -> Option<ControlMessage> {
        match self.state {
            OutboxState::PendingSubmission => {
                Some(ControlMessage::SubmitPostCallJob(self.submission.clone()))
            }
            OutboxState::InputEnded => Some(ControlMessage::EndAudioInput(EndAudioInput {
                job: self.submission.job.clone(),
                final_sequences: self.final_sequences.clone(),
            })),
            OutboxState::ResultStored => self.completed.as_ref().map(|completed| {
                ControlMessage::ResultPersisted(ResultPersisted {
                    job: completed.job.clone(),
                    result_version: completed.result_version,
                })
            }),
            OutboxState::Accepted | OutboxState::Completed => None,
        }
    }

    pub(crate) fn completed_received_at_ms(&self) -> Option<u64> {
        self.completed
            .as_ref()
            .map(|_| self.completed_received_at_ms.unwrap_or(self.updated_at_ms))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AiSubmissionOutbox {
    root: PathBuf,
}

impl AiSubmissionOutbox {
    pub(crate) fn open(root: PathBuf) -> Result<Self> {
        fs::create_dir_all(&root)
            .with_context(|| format!("create AI outbox directory {}", root.display()))?;
        Ok(Self { root })
    }

    pub(crate) fn append(&self, submission: SubmitPostCallJob) -> Result<OutboxRecord> {
        submission.validate()?;
        let path = self.record_path(submission.job.job_id.as_str());
        if path.exists() {
            let existing = self.read_path(&path)?;
            if existing.submission != submission {
                bail!("AI outbox job_id collision: {}", submission.job.job_id);
            }
            return Ok(existing);
        }
        let now = unix_timestamp_ms();
        let record = OutboxRecord {
            schema_version: OUTBOX_SCHEMA_VERSION,
            submission,
            state: OutboxState::PendingSubmission,
            final_sequences: BTreeMap::new(),
            completed: None,
            completed_received_at_ms: None,
            created_at_ms: now,
            updated_at_ms: now,
        };
        self.write(&record)?;
        Ok(record)
    }

    pub(crate) fn mark_accepted(&self, job: &JobRef) -> Result<OutboxRecord> {
        self.update(job, |record| {
            if record.state == OutboxState::PendingSubmission {
                record.state = OutboxState::Accepted;
            }
            Ok(())
        })
    }

    pub(crate) fn mark_input_ended(
        &self,
        job: &JobRef,
        final_sequences: BTreeMap<StreamId, u64>,
    ) -> Result<OutboxRecord> {
        if final_sequences.is_empty() {
            bail!("AI input end requires final stream sequences");
        }
        self.update(job, |record| {
            if matches!(
                record.state,
                OutboxState::Completed | OutboxState::ResultStored
            ) {
                return Ok(());
            }
            if !record.final_sequences.is_empty() && record.final_sequences != final_sequences {
                bail!("AI final sequence collision for job {}", job.job_id);
            }
            record.final_sequences = final_sequences;
            record.state = OutboxState::InputEnded;
            Ok(())
        })
    }

    pub(crate) fn mark_completed(&self, completed: JobCompleted) -> Result<OutboxRecord> {
        let job = completed.job.clone();
        let received_at_ms = unix_timestamp_ms();
        self.update(&job, |record| {
            if let Some(existing) = &record.completed {
                if existing.result_version > completed.result_version {
                    return Ok(());
                }
                if existing.result_version == completed.result_version {
                    if existing != &completed {
                        bail!(
                            "AI completed result collision for job {} version {}",
                            job.job_id,
                            completed.result_version
                        );
                    }
                    return Ok(());
                }
            }
            record.completed = Some(completed);
            record.completed_received_at_ms = Some(received_at_ms);
            record.state = OutboxState::Completed;
            Ok(())
        })
    }

    pub(crate) fn mark_result_stored(
        &self,
        job: &JobRef,
        result_version: u64,
    ) -> Result<OutboxRecord> {
        self.update(job, |record| {
            let completed = record
                .completed
                .as_ref()
                .context("AI outbox has no completed result")?;
            if completed.result_version != result_version {
                bail!(
                    "AI stored result version mismatch for job {}: expected {}, got {}",
                    job.job_id,
                    completed.result_version,
                    result_version
                );
            }
            record.state = OutboxState::ResultStored;
            Ok(())
        })
    }

    pub(crate) fn acknowledge_result(&self, job: &JobRef, result_version: u64) -> Result<bool> {
        let path = self.record_path(job.job_id.as_str());
        if !path.exists() {
            return Ok(false);
        }
        let record = self.read_path(&path)?;
        if record.submission.job != *job {
            bail!("AI outbox job reference mismatch: {}", job.job_id);
        }
        let completed = record
            .completed
            .as_ref()
            .context("AI outbox has no completed result")?;
        if record.state != OutboxState::ResultStored || completed.result_version != result_version {
            bail!(
                "AI persisted ACK does not match stored result for job {} version {}",
                job.job_id,
                result_version
            );
        }
        fs::remove_file(&path)?;
        File::open(&self.root)?.sync_all()?;
        Ok(true)
    }

    pub(crate) fn list(&self) -> Result<Vec<OutboxRecord>> {
        let mut paths = fs::read_dir(&self.root)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                entry
                    .file_type()
                    .ok()
                    .filter(|kind| kind.is_file())
                    .and_then(|_| {
                        entry
                            .file_name()
                            .to_str()
                            .filter(|name| name.ends_with(".json"))
                            .map(|_| entry.path())
                    })
            })
            .collect::<Vec<_>>();
        paths.sort();
        paths
            .into_iter()
            .map(|path| self.read_path(&path))
            .collect()
    }

    fn update(
        &self,
        job: &JobRef,
        mutate: impl FnOnce(&mut OutboxRecord) -> Result<()>,
    ) -> Result<OutboxRecord> {
        let path = self.record_path(job.job_id.as_str());
        let mut record = self.read_path(&path)?;
        if record.submission.job != *job {
            bail!("AI outbox job reference mismatch: {}", job.job_id);
        }
        mutate(&mut record)?;
        record.updated_at_ms = unix_timestamp_ms();
        self.write(&record)?;
        Ok(record)
    }

    fn read_path(&self, path: &Path) -> Result<OutboxRecord> {
        let bytes =
            fs::read(path).with_context(|| format!("read AI outbox record {}", path.display()))?;
        let record: OutboxRecord = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse AI outbox record {}", path.display()))?;
        if record.schema_version != OUTBOX_SCHEMA_VERSION {
            bail!(
                "unsupported AI outbox schema {} in {}",
                record.schema_version,
                path.display()
            );
        }
        Ok(record)
    }

    fn write(&self, record: &OutboxRecord) -> Result<()> {
        let path = self.record_path(record.submission.job.job_id.as_str());
        let temporary = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec(record)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)
            .with_context(|| format!("create AI outbox record {}", temporary.display()))?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, &path)?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    fn record_path(&self, job_id: &str) -> PathBuf {
        self.root.join(format!("{job_id}.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ai_protocol::control::{
        AiPipelineType, AiProfileSnapshot, AudioCodec, CaptureQuality, MediaDirection, Participant,
        StreamBinding, StructuredCallResult,
    };
    use ai_protocol::id::{
        ConversationId, JobId, OperationId, ParticipantId, ProfileId, StreamId, TenantId,
    };
    use tempfile::tempdir;

    fn submission() -> SubmitPostCallJob {
        let participant_id = ParticipantId::new("caller").unwrap();
        SubmitPostCallJob {
            job: JobRef {
                job_id: JobId::new("job-1").unwrap(),
                tenant_id: TenantId::new("domain-1").unwrap(),
                conversation_id: ConversationId::new("call-1").unwrap(),
                operation_id: OperationId::new("operation-1").unwrap(),
                generation: 1,
            },
            profile: AiProfileSnapshot {
                profile_id: ProfileId::new("profile-1").unwrap(),
                profile_version: 1,
                pipeline_type: AiPipelineType::PostCallAnalysis,
                asr_provider_id: Some("mock-asr".to_string()),
                llm_provider_id: Some("mock-llm".to_string()),
                tts_provider_id: None,
                capture_complete_ratio: 0.995,
                capture_process_min_ratio: 0.95,
                capture_complete_max_gap_ms: 200,
                capture_process_max_gap_ms: 5_000,
            },
            participants: vec![Participant {
                participant_id: participant_id.clone(),
                role: "caller".to_string(),
                display_number: Some("1000".to_string()),
            }],
            streams: vec![StreamBinding {
                stream_id: StreamId::new("caller-audio").unwrap(),
                participant_id,
                direction: MediaDirection::FromParticipant,
                codec: AudioCodec::Pcmu,
                sample_rate: 8_000,
                channels: 1,
            }],
        }
    }

    fn completed(job: JobRef) -> JobCompleted {
        JobCompleted {
            job,
            result_version: 1,
            capture_quality: CaptureQuality::Complete,
            transcript: Vec::new(),
            result: StructuredCallResult {
                schema_version: 1,
                summary: "summary".to_string(),
                purpose: "purpose".to_string(),
                outcome: "outcome".to_string(),
                key_points: Vec::new(),
                action_items: Vec::new(),
                tags: Vec::new(),
            },
        }
    }

    #[test]
    fn persists_and_replays_state_transitions() {
        let root = tempdir().unwrap();
        let outbox = AiSubmissionOutbox::open(root.path().to_path_buf()).unwrap();
        let request = submission();
        let job = request.job.clone();
        assert_eq!(
            outbox.append(request.clone()).unwrap().state,
            OutboxState::PendingSubmission
        );
        assert_eq!(
            outbox.append(request).unwrap().state,
            OutboxState::PendingSubmission
        );
        assert_eq!(
            outbox.mark_accepted(&job).unwrap().state,
            OutboxState::Accepted
        );

        let sequences = BTreeMap::from([(StreamId::new("caller-audio").unwrap(), 12)]);
        assert_eq!(
            outbox
                .mark_input_ended(&job, sequences.clone())
                .unwrap()
                .pending_message(),
            Some(ControlMessage::EndAudioInput(EndAudioInput {
                job: job.clone(),
                final_sequences: sequences,
            }))
        );
        outbox.mark_completed(completed(job.clone())).unwrap();
        assert_eq!(
            AiSubmissionOutbox::open(root.path().to_path_buf())
                .unwrap()
                .list()
                .unwrap()[0]
                .state,
            OutboxState::Completed
        );
        let stored = outbox.mark_result_stored(&job, 1).unwrap();
        assert_eq!(stored.state, OutboxState::ResultStored);
        assert_eq!(
            stored.pending_message(),
            Some(ControlMessage::ResultPersisted(ResultPersisted {
                job: job.clone(),
                result_version: 1,
            }))
        );
        assert!(outbox.acknowledge_result(&job, 1).unwrap());
        assert!(outbox.list().unwrap().is_empty());
        assert!(!outbox.acknowledge_result(&job, 1).unwrap());
    }

    #[test]
    fn rejects_different_completion_with_same_result_version() {
        let root = tempdir().unwrap();
        let outbox = AiSubmissionOutbox::open(root.path().to_path_buf()).unwrap();
        let request = submission();
        let job = request.job.clone();
        outbox.append(request).unwrap();
        let result = completed(job.clone());
        outbox.mark_completed(result.clone()).unwrap();
        outbox.mark_completed(result).unwrap();

        let mut collision = completed(job);
        collision.result.summary = "different".to_string();
        let error = outbox.mark_completed(collision).unwrap_err();
        assert!(error.to_string().contains("completed result collision"));
    }
}
