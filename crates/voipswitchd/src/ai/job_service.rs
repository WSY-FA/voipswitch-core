use super::outbox::OutboxRecord;
use super::{
    AiCaptureFinalized, AiConnector, AiMediaTapSender, AiMediaTapSpec, AiSubmissionOutbox,
    OutboxState,
};
use crate::data_store::{AiCallResultRecord, ConfigBackend};
use crate::runtime::media::MediaBridgeHandle;
use ai_protocol::control::{
    ControlMessage, JobResultRequest, JobState, StartConversation, SubmitPostCallJob,
};
use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

const COMMAND_QUEUE_CAPACITY: usize = 256;
const REPLAY_INTERVAL: Duration = Duration::from_secs(1);

enum JobCommand {
    Submit {
        request: Box<SubmitPostCallJob>,
        media: MediaBridgeHandle,
        tap: Box<AiMediaTapSpec>,
    },
    EndCall {
        conversation_id: String,
        capture: Option<AiCaptureFinalized>,
    },
}

struct ActiveCapture {
    media: MediaBridgeHandle,
    tap: AiMediaTapSpec,
    enabled: bool,
}

#[derive(Clone)]
pub(crate) struct AiJobService {
    tx: mpsc::Sender<JobCommand>,
    active_conversations: Arc<Mutex<BTreeSet<String>>>,
    connector: AiConnector,
}

impl AiJobService {
    pub(crate) fn spawn(backend: Arc<dyn ConfigBackend>, connector: AiConnector) -> Self {
        let (tx, rx) = mpsc::channel(COMMAND_QUEUE_CAPACITY);
        tokio::spawn(run(backend, connector.clone(), rx));
        Self {
            tx,
            active_conversations: Arc::new(Mutex::new(BTreeSet::new())),
            connector,
        }
    }

    pub(crate) fn executable_profile(
        &self,
        profile_id: &str,
    ) -> Option<ai_protocol::control::AiProfileSnapshot> {
        self.connector.executable_profile(profile_id)
    }

    pub(crate) fn validate_profile_reference(&self, profile_id: &str) -> Result<()> {
        if let Some(enabled) = self.connector.known_post_call_profile_enabled(profile_id) {
            anyhow::ensure!(
                enabled,
                "INVALID_REFERENCE: AI profile {profile_id} is missing or disabled"
            );
        }
        Ok(())
    }

    pub(crate) fn profile_catalog(&self) -> Option<ai_protocol::control::ProfileCatalogSnapshot> {
        self.connector.profile_catalog()
    }

    pub(crate) fn profile_snapshot(
        &self,
        profile_id: &str,
    ) -> Option<ai_protocol::control::AiProfileSnapshot> {
        self.connector
            .profile_catalog()?
            .profiles
            .into_iter()
            .find(|projection| projection.profile.profile_id.as_str() == profile_id)
            .map(|projection| projection.profile)
    }

    pub(crate) fn try_start_conversation(&self, request: StartConversation) -> Result<()> {
        self.connector.try_start_conversation(request)
    }

    pub(crate) fn try_submit(
        &self,
        request: SubmitPostCallJob,
        media: MediaBridgeHandle,
        tap: AiMediaTapSpec,
    ) -> Result<()> {
        let conversation_id = request.job.conversation_id.to_string();
        anyhow::ensure!(
            self.active_conversations
                .lock()
                .expect("AI active conversation lock poisoned")
                .insert(conversation_id.clone()),
            "AI job already registered for conversation {conversation_id}"
        );
        if let Err(error) = self.tx.try_send(JobCommand::Submit {
            request: Box::new(request),
            media,
            tap: Box::new(tap),
        }) {
            self.active_conversations
                .lock()
                .expect("AI active conversation lock poisoned")
                .remove(&conversation_id);
            return Err(anyhow::anyhow!(
                "AI job submission queue unavailable: {error}"
            ));
        }
        Ok(())
    }

    pub(crate) fn try_end_call(
        &self,
        conversation_id: String,
        capture: Option<AiCaptureFinalized>,
    ) -> Result<()> {
        if !self
            .active_conversations
            .lock()
            .expect("AI active conversation lock poisoned")
            .remove(&conversation_id)
        {
            return Ok(());
        }
        if let Err(error) = self.tx.try_send(JobCommand::EndCall {
            conversation_id,
            capture,
        }) {
            let conversation_id = match &error {
                mpsc::error::TrySendError::Full(JobCommand::EndCall {
                    conversation_id, ..
                })
                | mpsc::error::TrySendError::Closed(JobCommand::EndCall {
                    conversation_id, ..
                }) => conversation_id.clone(),
                _ => unreachable!("try_end_call only submits EndCall"),
            };
            self.active_conversations
                .lock()
                .expect("AI active conversation lock poisoned")
                .insert(conversation_id);
            return Err(anyhow::anyhow!(
                "AI job completion queue unavailable: {error}"
            ));
        }
        Ok(())
    }
}

async fn run(
    backend: Arc<dyn ConfigBackend>,
    connector: AiConnector,
    mut commands: mpsc::Receiver<JobCommand>,
) {
    let mut events = connector.subscribe();
    let mut active = BTreeMap::<String, ActiveCapture>::new();
    let mut replay = tokio::time::interval(REPLAY_INTERVAL);
    replay.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { return; };
                if let Err(error) =
                    handle_command(backend.clone(), &connector, &mut active, command).await
                {
                    error!(error = %error, "AI job command failed");
                }
            }
            event = events.recv() => match event {
                Ok(event) => {
                    if let Err(error) = handle_event(
                        backend.clone(),
                        &connector,
                        &mut active,
                        event.message,
                    ).await {
                        error!(error = %error, "AI gateway event handling failed");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                    warn!(count, "AI gateway event receiver lagged; durable records retained");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            },
            _ = replay.tick() => {
                if let Err(error) = replay_pending(backend.clone(), &connector).await {
                    debug!(error = %error, "AI outbox replay deferred");
                }
            }
        }
    }
}

async fn handle_command(
    backend: Arc<dyn ConfigBackend>,
    connector: &AiConnector,
    active: &mut BTreeMap<String, ActiveCapture>,
    command: JobCommand,
) -> Result<()> {
    let record = match command {
        JobCommand::Submit {
            request,
            media,
            tap,
        } => {
            let request = *request;
            let tap = *tap;
            anyhow::ensure!(
                request.job == tap.job,
                "AI tap job does not match submission"
            );
            let job_id = request.job.job_id.to_string();
            let conversation_id = request.job.conversation_id.to_string();
            let record = tokio::task::spawn_blocking(move || {
                let outbox = AiSubmissionOutbox::open(
                    backend.ai_outbox_dir(request.job.tenant_id.as_str())?,
                )?;
                outbox.append(request)
            })
            .await
            .context("AI outbox worker failed")??;
            active.entry(job_id).or_insert(ActiveCapture {
                media,
                tap,
                enabled: false,
            });
            info!(
                conversation_id,
                job_id = %record.job().job_id,
                "AI capture registered"
            );
            Some(record)
        }
        JobCommand::EndCall {
            conversation_id,
            capture,
        } => {
            let (job, final_sequences) = match capture {
                Some(capture) => (capture.job, capture.final_sequences),
                None => {
                    let Some(job_id) = active.iter().find_map(|(job_id, pending)| {
                        (pending.tap.job.conversation_id.as_str() == conversation_id)
                            .then(|| job_id.clone())
                    }) else {
                        return Ok(());
                    };
                    let pending = active
                        .remove(&job_id)
                        .expect("active AI capture disappeared");
                    (
                        pending.tap.job,
                        BTreeMap::from([
                            (pending.tap.caller.stream_id, 0),
                            (pending.tap.callee.stream_id, 0),
                        ]),
                    )
                }
            };
            active.remove(job.job_id.as_str());
            let record = tokio::task::spawn_blocking(move || {
                let outbox =
                    AiSubmissionOutbox::open(backend.ai_outbox_dir(job.tenant_id.as_str())?)?;
                outbox.mark_input_ended(&job, final_sequences)
            })
            .await
            .context("AI outbox worker failed")??;
            Some(record)
        }
    };
    let Some(record) = record else {
        return Ok(());
    };
    if let Some(message) = record.pending_message()
        && let Err(error) = connector.try_send(message)
    {
        debug!(job_id = %record.job().job_id, error = %error, "AI control send deferred to outbox replay");
    }
    Ok(())
}

async fn handle_event(
    backend: Arc<dyn ConfigBackend>,
    connector: &AiConnector,
    active: &mut BTreeMap<String, ActiveCapture>,
    message: ControlMessage,
) -> Result<()> {
    match message {
        ControlMessage::DurableAccepted(accepted) => {
            let job = accepted.job;
            tokio::task::spawn_blocking(move || {
                let outbox =
                    AiSubmissionOutbox::open(backend.ai_outbox_dir(job.tenant_id.as_str())?)?;
                outbox.mark_accepted(&job).map(|_| ())
            })
            .await
            .context("AI outbox event worker failed")??;
        }
        ControlMessage::AudioInputReady(ready) => {
            enable_capture(connector, active, ready)?;
        }
        ControlMessage::JobCompleted(completed) => {
            active.remove(completed.job.job_id.as_str());
            let record = tokio::task::spawn_blocking(move || {
                let outbox = AiSubmissionOutbox::open(
                    backend.ai_outbox_dir(completed.job.tenant_id.as_str())?,
                )?;
                let record = outbox.mark_completed(completed)?;
                persist_completed_record(backend.as_ref(), &outbox, record)
            })
            .await
            .context("AI result persistence worker failed")??;
            send_pending(connector, &record);
        }
        ControlMessage::JobStatus(status) => match status.state {
            JobState::Completed => {
                connector.try_send(ControlMessage::JobResultRequest(JobResultRequest {
                    job: status.job,
                }))?;
            }
            JobState::Persisted => {
                let Some(result_version) = status.result_version else {
                    anyhow::bail!("persisted AI job has no result version");
                };
                let job_id = status.job.job_id.clone();
                let job = status.job;
                let removed = tokio::task::spawn_blocking(move || {
                    let outbox =
                        AiSubmissionOutbox::open(backend.ai_outbox_dir(job.tenant_id.as_str())?)?;
                    outbox.acknowledge_result(&job, result_version)
                })
                .await
                .context("AI result ACK worker failed")??;
                if removed {
                    info!(
                        job_id = %job_id,
                        result_version,
                        "AI final result persisted and acknowledged"
                    );
                }
            }
            _ => {}
        },
        ControlMessage::Error(error) => {
            warn!(
                code = error.code,
                retryable = error.retryable,
                message = error.message,
                "AI gateway rejected control request"
            );
        }
        _ => {}
    }
    Ok(())
}

fn persist_completed_record(
    backend: &dyn ConfigBackend,
    outbox: &AiSubmissionOutbox,
    record: OutboxRecord,
) -> Result<OutboxRecord> {
    let completed = record
        .completed
        .as_ref()
        .context("AI outbox completion missing")?;
    let received_at_ms = record
        .completed_received_at_ms()
        .context("AI outbox completion timestamp missing")?;
    let stored = AiCallResultRecord {
        job_id: completed.job.job_id.clone(),
        result_version: completed.result_version,
        domain_id: completed.job.tenant_id.to_string(),
        call_id: completed.job.conversation_id.to_string(),
        operation_id: completed.job.operation_id.clone(),
        generation: completed.job.generation,
        profile_id: record.submission.profile.profile_id.clone(),
        profile_version: record.submission.profile.profile_version,
        capture_quality: completed.capture_quality,
        transcript: completed.transcript.clone(),
        result: completed.result.clone(),
        received_at_ms,
    };
    backend.persist_ai_result(&stored)?;
    let updated = outbox.mark_result_stored(&completed.job, completed.result_version)?;
    info!(
        domain_id = stored.domain_id,
        call_id = stored.call_id,
        job_id = %stored.job_id,
        result_version = stored.result_version,
        "AI final result stored"
    );
    Ok(updated)
}

fn send_pending(connector: &AiConnector, record: &OutboxRecord) {
    if let Some(message) = record.pending_message()
        && let Err(error) = connector.try_send(message)
    {
        debug!(job_id = %record.job().job_id, error = %error, "AI result ACK deferred to outbox replay");
    }
}

fn enable_capture(
    connector: &AiConnector,
    active: &mut BTreeMap<String, ActiveCapture>,
    ready: ai_protocol::control::AudioInputReady,
) -> Result<()> {
    let Some(capture) = active.get_mut(ready.job.job_id.as_str()) else {
        warn!(job_id = %ready.job.job_id, "AI audio ready has no live media bridge");
        return Ok(());
    };
    if capture.enabled {
        debug!(job_id = %ready.job.job_id, "duplicate AI audio ready ignored");
        return Ok(());
    }
    anyhow::ensure!(
        capture.tap.job == ready.job,
        "AI audio ready job reference mismatch"
    );
    let accepted = ready.accepted_streams.into_iter().collect::<BTreeSet<_>>();
    let required = BTreeSet::from([
        capture.tap.caller.stream_id.clone(),
        capture.tap.callee.stream_id.clone(),
    ]);
    if !streams_ready(&required, &accepted) {
        warn!(
            job_id = %ready.job.job_id,
            accepted_streams = accepted.len(),
            "AI gateway did not accept all required audio streams"
        );
        return Ok(());
    }
    capture.media.enable_ai_tap(AiMediaTapSender::new(
        connector.clone(),
        capture.tap.clone(),
    ))?;
    capture.enabled = true;
    info!(
        job_id = %ready.job.job_id,
        streams = required.len(),
        "AI media tap enabled"
    );
    Ok(())
}

fn streams_ready(
    required: &BTreeSet<ai_protocol::id::StreamId>,
    accepted: &BTreeSet<ai_protocol::id::StreamId>,
) -> bool {
    required.is_subset(accepted)
}

async fn replay_pending(backend: Arc<dyn ConfigBackend>, connector: &AiConnector) -> Result<()> {
    let records = tokio::task::spawn_blocking(move || {
        let mut records = Vec::new();
        for domain_id in backend.list_ai_outbox_domains()? {
            let outbox = AiSubmissionOutbox::open(backend.ai_outbox_dir(&domain_id)?)?;
            for record in outbox.list()? {
                let record = if record.state == OutboxState::Completed {
                    persist_completed_record(backend.as_ref(), &outbox, record)?
                } else {
                    record
                };
                if matches!(
                    record.state,
                    OutboxState::PendingSubmission
                        | OutboxState::InputEnded
                        | OutboxState::ResultStored
                ) {
                    records.push(record);
                }
            }
        }
        anyhow::Ok(records)
    })
    .await
    .context("AI outbox replay worker failed")??;
    if !connector.is_control_connected() {
        return Ok(());
    }
    for record in records {
        let Some(message) = record.pending_message() else {
            continue;
        };
        if let Err(error) = connector.try_send(message) {
            debug!(job_id = %record.job().job_id, error = %error, "AI outbox replay queue full");
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::AiConnectorConfig;
    use crate::data_store::SeaOrmConfigBackend;
    use ai_protocol::control::{
        AiPipelineType, AiProfileSnapshot, AudioCodec, CaptureQuality, JobCompleted, JobRef,
        MediaDirection, Participant, StreamBinding, StructuredCallResult,
    };
    use ai_protocol::id::{
        ConversationId, JobId, OperationId, ParticipantId, ProfileId, StreamId, TenantId,
    };
    use tempfile::tempdir;

    #[test]
    fn job_command_queue_is_bounded() {
        let (tx, _rx) = mpsc::channel::<JobCommand>(COMMAND_QUEUE_CAPACITY);
        assert_eq!(tx.max_capacity(), COMMAND_QUEUE_CAPACITY);
    }

    #[test]
    fn media_tap_requires_every_declared_stream() {
        let caller = StreamId::new("caller-audio").unwrap();
        let callee = StreamId::new("callee-audio").unwrap();
        let required = BTreeSet::from([caller.clone(), callee.clone()]);
        assert!(!streams_ready(&required, &BTreeSet::from([caller])));
        assert!(streams_ready(
            &required,
            &BTreeSet::from([callee, StreamId::new("caller-audio").unwrap()])
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn replay_stores_completed_result_while_gateway_is_disconnected() {
        let root = tempdir().unwrap();
        let backend = Arc::new(SeaOrmConfigBackend::sqlite(root.path(), "test").unwrap());
        let participant_id = ParticipantId::new("caller").unwrap();
        let request = SubmitPostCallJob {
            job: JobRef {
                job_id: JobId::new("job-offline").unwrap(),
                tenant_id: TenantId::new("domain-offline").unwrap(),
                conversation_id: ConversationId::new("call-offline").unwrap(),
                operation_id: OperationId::new("post-call-v1").unwrap(),
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
                display_number: Some("1001".to_string()),
            }],
            streams: vec![StreamBinding {
                stream_id: StreamId::new("caller-audio").unwrap(),
                participant_id,
                direction: MediaDirection::FromParticipant,
                codec: AudioCodec::Pcmu,
                sample_rate: 8_000,
                channels: 1,
            }],
        };
        let outbox = AiSubmissionOutbox::open(
            backend
                .ai_outbox_dir(request.job.tenant_id.as_str())
                .unwrap(),
        )
        .unwrap();
        outbox.append(request.clone()).unwrap();
        outbox
            .mark_completed(JobCompleted {
                job: request.job.clone(),
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
            })
            .unwrap();
        let connector = AiConnector::spawn(AiConnectorConfig {
            instance_id: "offline-test".to_string(),
            control_socket: root.path().join("missing-control.sock"),
            media_socket: root.path().join("missing-media.sock"),
        })
        .unwrap();

        replay_pending(backend.clone(), &connector).await.unwrap();

        assert_eq!(outbox.list().unwrap()[0].state, OutboxState::ResultStored);
        let call_id = request.job.conversation_id.to_string();
        let domain_id = request.job.tenant_id.to_string();
        let stored =
            tokio::task::spawn_blocking(move || backend.get_ai_results(&call_id, &domain_id))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].job_id, request.job.job_id);
    }
}
