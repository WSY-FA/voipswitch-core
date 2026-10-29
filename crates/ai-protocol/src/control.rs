use crate::PROTOCOL_VERSION;
use crate::id::{
    ConnectorInstanceId, ConversationId, JobId, MessageId, OperationId, ParticipantId, ProfileId,
    StreamId, TenantId,
};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlEnvelope {
    pub protocol_version: u16,
    pub message_id: MessageId,
    pub timestamp_ms: u64,
    #[serde(flatten)]
    pub message: ControlMessage,
}

impl ControlEnvelope {
    pub fn validate(&self) -> Result<()> {
        if self.protocol_version != PROTOCOL_VERSION {
            bail!(
                "unsupported protocol version {}, expected {}",
                self.protocol_version,
                PROTOCOL_VERSION
            );
        }
        self.message.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum ControlMessage {
    ConnectorHello(ConnectorHello),
    GatewayHello(GatewayHello),
    ProfileCatalogRequest(ProfileCatalogRequest),
    ProfileCatalogSnapshot(ProfileCatalogSnapshot),
    SubmitPostCallJob(SubmitPostCallJob),
    SubmitLlmTask(SubmitLlmTask),
    LlmTaskCompleted(LlmTaskCompleted),
    CancelLlmTask(CancelLlmTask),
    DurableAccepted(DurableAccepted),
    AudioInputReady(AudioInputReady),
    EndAudioInput(EndAudioInput),
    JobCompleted(JobCompleted),
    ResultPersisted(ResultPersisted),
    CancelJob(CancelJob),
    JobStatusRequest(JobStatusRequest),
    JobStatus(JobStatus),
    JobResultRequest(JobResultRequest),
    StartConversation(StartConversation),
    StartAssistConversation(StartAssistConversation),
    AssistConversationReady(AssistConversationReady),
    StopAssistConversation(StopAssistConversation),
    AsrPartial(AsrPartial),
    AsrFinal(AsrFinal),
    AssistSuggestion(AssistSuggestion),
    ConversationReady(ConversationReady),
    StopConversation(StopConversation),
    ConversationStopped(ConversationStopped),
    ActionRequested(ActionRequested),
    ActionResult(ActionResult),
    TtsStateChanged(TtsStateChanged),
    SynthesizeTts(SynthesizeTts),
    TtsSynthesized(TtsSynthesized),
    Error(ProtocolError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SynthesizeTts {
    pub request_id: String,
    pub profile_id: ProfileId,
    pub text: String,
    #[serde(default)]
    pub voice: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtsSynthesized {
    pub request_id: String,
    pub success: bool,
    #[serde(default)]
    pub pcm16_le: Vec<u8>,
    #[serde(default)]
    pub sample_rate: u32,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartConversation {
    pub conversation: JobRef,
    pub profile: AiProfileSnapshot,
    pub participant: Participant,
    pub input_stream: StreamBinding,
    #[serde(default)]
    pub welcome: Option<WelcomePrompt>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartAssistConversation {
    pub conversation: JobRef,
    pub profile: AiProfileSnapshot,
    pub participants: Vec<Participant>,
    pub streams: Vec<StreamBinding>,
}

impl StartAssistConversation {
    pub fn validate(&self) -> Result<()> {
        self.profile.validate()?;
        if self.profile.pipeline_type != AiPipelineType::RealtimeAssist {
            bail!("assist conversation requires a realtime_assist profile");
        }
        if self.conversation.generation == 0
            || self.participants.is_empty()
            || self.streams.is_empty()
        {
            bail!("assist conversation requires generation, participants and streams");
        }
        for stream in &self.streams {
            if !self
                .participants
                .iter()
                .any(|p| p.participant_id == stream.participant_id)
            {
                bail!("assist stream references unknown participant");
            }
            if stream.direction != MediaDirection::FromParticipant {
                bail!("assist stream must be from participant");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistConversationReady {
    pub conversation: JobRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StopAssistConversation {
    pub conversation: JobRef,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AsrPartial {
    pub conversation: JobRef,
    pub participant_id: ParticipantId,
    pub segment_id: u64,
    pub text: String,
    pub start_ms: u64,
    pub end_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AsrFinal {
    pub conversation: JobRef,
    pub participant_id: ParticipantId,
    pub segment_id: u64,
    pub text: String,
    pub start_ms: u64,
    pub end_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssistSuggestion {
    pub conversation: JobRef,
    pub suggestion_id: u64,
    pub kind: String,
    pub text: String,
    pub confidence: Option<f32>,
    pub source_segment_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum WelcomePrompt {
    Text(String),
    PcmWav(Vec<u8>),
}

impl StartConversation {
    pub fn validate(&self) -> Result<()> {
        self.profile.validate()?;
        if self.profile.pipeline_type != AiPipelineType::VoiceAgent {
            bail!("conversation requires a voice_agent profile");
        }
        if self.conversation.generation == 0 {
            bail!("conversation generation must be greater than zero");
        }
        if self.input_stream.participant_id != self.participant.participant_id {
            bail!("conversation stream participant mismatch");
        }
        if self.input_stream.direction != MediaDirection::FromParticipant {
            bail!("conversation input stream must be from participant");
        }
        if let Some(welcome) = &self.welcome {
            match welcome {
                WelcomePrompt::Text(text) if text.trim().is_empty() => {
                    bail!("welcome text must not be empty")
                }
                WelcomePrompt::Text(text) if text.len() > 2_000 => {
                    bail!("welcome text is too long")
                }
                WelcomePrompt::PcmWav(bytes) if bytes.is_empty() || bytes.len() > 768 * 1024 => {
                    bail!("welcome WAV must contain 1..=786432 bytes")
                }
                WelcomePrompt::Text(_) | WelcomePrompt::PcmWav(_) => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationReady {
    pub conversation: JobRef,
    pub state: ConversationState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StopConversation {
    pub conversation: JobRef,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationStopped {
    pub conversation: JobRef,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationState {
    Starting,
    Listening,
    Thinking,
    Speaking,
    Stopping,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionRequested {
    pub conversation: JobRef,
    pub operation_id: OperationId,
    pub generation: u64,
    pub action: AgentAction,
    pub deadline_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentAction {
    PlayText { text: String, voice: String },
    CollectDigits { max_digits: u8, timeout_ms: u64 },
    TransferToExtension { number: String },
    TransferToBusinessTarget { target: String },
    EndCall { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionResult {
    pub conversation: JobRef,
    pub operation_id: OperationId,
    pub generation: u64,
    pub success: bool,
    pub code: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtsStateChanged {
    pub conversation: JobRef,
    pub generation: u64,
    pub state: TtsState,
    pub sample_rate: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TtsState {
    Started,
    Stopped,
    Interrupted,
    Failed,
}

impl ControlMessage {
    fn validate(&self) -> Result<()> {
        match self {
            Self::SubmitPostCallJob(request) => request.validate(),
            Self::SubmitLlmTask(request) => request.validate(),
            Self::LlmTaskCompleted(result) => result.validate(),
            Self::CancelLlmTask(request) => request.validate(),
            Self::ProfileCatalogSnapshot(snapshot) => snapshot.validate(),
            Self::StartConversation(request) => request.validate(),
            Self::StartAssistConversation(request) => request.validate(),
            Self::ActionRequested(request) => request.validate(),
            Self::SynthesizeTts(request) => {
                if request.request_id.trim().is_empty() || request.request_id.len() > 128 {
                    bail!("invalid TTS request id");
                }
                if request.text.trim().is_empty() || request.text.len() > 2_000 {
                    bail!("invalid TTS text");
                }
                if request.voice.len() > 256 {
                    bail!("TTS voice is too long");
                }
                Ok(())
            }
            Self::EndAudioInput(request) if request.final_sequences.is_empty() => {
                bail!("end_audio_input requires at least one final sequence")
            }
            _ => Ok(()),
        }
    }
}

impl ActionRequested {
    pub fn validate(&self) -> Result<()> {
        if self.conversation.generation == 0 || self.generation == 0 {
            bail!("action generation must be greater than zero");
        }
        if self.operation_id.as_str().trim().is_empty() {
            bail!("action operation id must not be empty");
        }
        if self.deadline_at_ms == 0 {
            bail!("action deadline must be set");
        }
        match &self.action {
            AgentAction::PlayText { text, voice } => {
                if text.trim().is_empty() || text.len() > 16 * 1024 || voice.len() > 256 {
                    bail!("invalid PlayText parameters");
                }
            }
            AgentAction::CollectDigits {
                max_digits,
                timeout_ms,
            } => {
                if *max_digits == 0 || *max_digits > 32 || *timeout_ms == 0 || *timeout_ms > 300_000
                {
                    bail!("invalid CollectDigits parameters");
                }
            }
            AgentAction::TransferToExtension { number } => {
                if number.is_empty()
                    || number.len() > 32
                    || !number.bytes().all(|b| b.is_ascii_digit())
                {
                    bail!("invalid extension number");
                }
            }
            AgentAction::TransferToBusinessTarget { target } => {
                if target.is_empty() || target.len() > 256 || target.contains(['\n', '\r']) {
                    bail!("invalid business target");
                }
            }
            AgentAction::EndCall { reason } => {
                if reason.len() > 1024 {
                    bail!("end-call reason is too long");
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorHello {
    pub connector_instance_id: ConnectorInstanceId,
    pub connector_kind: String,
    pub supported_versions: Vec<u16>,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayHello {
    pub selected_version: u16,
    pub gateway_instance_id: String,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileCatalogRequest {
    pub known_catalog_version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProfileCatalogSnapshot {
    pub catalog_version: u64,
    pub profiles: Vec<AiProfileProjection>,
}

impl ProfileCatalogSnapshot {
    pub fn validate(&self) -> Result<()> {
        if self.catalog_version == 0 {
            bail!("profile catalog version must be greater than zero");
        }
        let mut ids = BTreeSet::new();
        for projection in &self.profiles {
            projection.profile.validate()?;
            if !ids.insert(projection.profile.profile_id.clone()) {
                bail!("duplicate profile {}", projection.profile.profile_id);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AiProfileProjection {
    pub profile: AiProfileSnapshot,
    pub enabled: bool,
    pub executable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubmitPostCallJob {
    pub job: JobRef,
    pub profile: AiProfileSnapshot,
    pub participants: Vec<Participant>,
    pub streams: Vec<StreamBinding>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubmitLlmTask {
    pub task_id: String,
    pub request_id: String,
    pub task_kind: String,
    pub profile: AiProfileSnapshot,
    pub schema_version: u32,
    pub prompt_version: u32,
    pub evidence_digest: String,
    pub evidence_json: String,
    pub deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmTaskCompleted {
    pub task_id: String,
    pub request_id: String,
    pub schema_version: u32,
    pub status: String,
    pub result_json: String,
    pub evidence_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelLlmTask {
    pub task_id: String,
    pub request_id: String,
    pub reason: String,
}

impl CancelLlmTask {
    pub fn validate(&self) -> Result<()> {
        if self.task_id.is_empty() || self.request_id.is_empty() || self.reason.len() > 512 {
            bail!("invalid llm task cancellation");
        }
        Ok(())
    }
}

impl LlmTaskCompleted {
    pub fn validate(&self) -> Result<()> {
        if self.task_id.is_empty() || self.request_id.is_empty() {
            bail!("task and request ids are required");
        }
        if self.schema_version == 0 || self.result_json.len() > 262_144 {
            bail!("invalid llm task result schema or size");
        }
        if !matches!(
            self.status.as_str(),
            "completed" | "insufficient_evidence" | "failed"
        ) {
            bail!("invalid llm task result status");
        }
        Ok(())
    }
}

impl SubmitLlmTask {
    pub fn validate(&self) -> Result<()> {
        if self.task_id.is_empty() || self.request_id.is_empty() {
            bail!("task and request ids are required");
        }
        if self.task_kind != "ops_diagnosis" {
            bail!("unsupported llm task kind");
        }
        self.profile.validate()?;
        if self.profile.pipeline_type != AiPipelineType::LlmTask {
            bail!("llm task requires an llm_task profile");
        }
        if self.schema_version == 0
            || self.prompt_version == 0
            || self.deadline_ms == 0
            || self.deadline_ms > 300_000
        {
            bail!("task versions and deadline must be positive");
        }
        if self.evidence_json.len() > 262_144 {
            bail!("evidence budget exceeded");
        }
        Ok(())
    }
}

impl SubmitPostCallJob {
    pub fn validate(&self) -> Result<()> {
        if self.job.generation == 0 {
            bail!("generation must be greater than zero");
        }
        if self.participants.is_empty() || self.streams.is_empty() {
            bail!("job requires participants and streams");
        }
        self.profile.validate()?;
        if self.profile.pipeline_type != AiPipelineType::PostCallAnalysis {
            bail!("post-call job requires a post_call_analysis profile");
        }
        for stream in &self.streams {
            if !self
                .participants
                .iter()
                .any(|participant| participant.participant_id == stream.participant_id)
            {
                bail!("stream {} references unknown participant", stream.stream_id);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AiPipelineType {
    Transcription,
    RealtimeAssist,
    #[default]
    PostCallAnalysis,
    LlmTask,
    VoiceAgent,
}

impl AiPipelineType {
    pub fn requires_asr(self) -> bool {
        matches!(
            self,
            Self::Transcription | Self::RealtimeAssist | Self::PostCallAnalysis | Self::VoiceAgent
        )
    }

    pub fn requires_llm(self) -> bool {
        matches!(
            self,
            Self::RealtimeAssist | Self::PostCallAnalysis | Self::LlmTask | Self::VoiceAgent
        )
    }

    pub fn requires_tts(self) -> bool {
        self == Self::VoiceAgent
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRef {
    pub job_id: JobId,
    pub tenant_id: TenantId,
    pub conversation_id: ConversationId,
    pub operation_id: OperationId,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AiProfileSnapshot {
    pub profile_id: ProfileId,
    pub profile_version: u64,
    #[serde(default)]
    pub pipeline_type: AiPipelineType,
    pub asr_provider_id: Option<String>,
    pub llm_provider_id: Option<String>,
    #[serde(default)]
    pub tts_provider_id: Option<String>,
    pub capture_complete_ratio: f64,
    pub capture_process_min_ratio: f64,
    pub capture_complete_max_gap_ms: u64,
    pub capture_process_max_gap_ms: u64,
}

impl AiProfileSnapshot {
    pub fn validate(&self) -> Result<()> {
        if self.profile_version == 0 {
            bail!("profile version must be greater than zero");
        }
        validate_provider_combination(
            self.pipeline_type,
            self.asr_provider_id.as_deref(),
            self.llm_provider_id.as_deref(),
            self.tts_provider_id.as_deref(),
        )?;
        if !(0.0..=1.0).contains(&self.capture_complete_ratio)
            || !(0.0..=1.0).contains(&self.capture_process_min_ratio)
            || self.capture_complete_ratio < self.capture_process_min_ratio
        {
            bail!("invalid capture ratio thresholds");
        }
        if self.capture_complete_max_gap_ms > self.capture_process_max_gap_ms {
            bail!("complete gap threshold must not exceed process threshold");
        }
        Ok(())
    }
}

fn validate_provider_combination(
    pipeline_type: AiPipelineType,
    asr_provider_id: Option<&str>,
    llm_provider_id: Option<&str>,
    tts_provider_id: Option<&str>,
) -> Result<()> {
    for (capability, required, value) in [
        ("ASR", pipeline_type.requires_asr(), asr_provider_id),
        ("LLM", pipeline_type.requires_llm(), llm_provider_id),
        ("TTS", pipeline_type.requires_tts(), tts_provider_id),
    ] {
        let present = value.is_some_and(|id| !id.trim().is_empty());
        if required != present {
            if required {
                bail!("{capability} provider is required for {pipeline_type:?}");
            }
            bail!("{capability} provider is not used by {pipeline_type:?}");
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Participant {
    pub participant_id: ParticipantId,
    pub role: String,
    pub display_number: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamBinding {
    pub stream_id: StreamId,
    pub participant_id: ParticipantId,
    pub direction: MediaDirection,
    pub codec: AudioCodec,
    pub sample_rate: u32,
    pub channels: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaDirection {
    FromParticipant,
    ToParticipant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioCodec {
    Pcma,
    Pcmu,
    Pcm16Le,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableAccepted {
    pub job: JobRef,
    pub duplicate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioInputReady {
    pub job: JobRef,
    pub accepted_streams: Vec<StreamId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndAudioInput {
    pub job: JobRef,
    pub final_sequences: BTreeMap<StreamId, u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobCompleted {
    pub job: JobRef,
    pub result_version: u64,
    pub capture_quality: CaptureQuality,
    pub transcript: Vec<TranscriptSegment>,
    pub result: StructuredCallResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureQuality {
    Complete,
    IncompleteProcessable,
    Insufficient,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TranscriptSegment {
    pub participant_id: ParticipantId,
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
    pub final_segment: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuredCallResult {
    pub schema_version: u32,
    pub summary: String,
    pub purpose: String,
    pub outcome: String,
    pub key_points: Vec<String>,
    pub action_items: Vec<String>,
    pub tags: Vec<String>,
    #[serde(default)]
    pub action: Option<AgentAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultPersisted {
    pub job: JobRef,
    pub result_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelJob {
    pub job: JobRef,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobStatusRequest {
    pub job: JobRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobResultRequest {
    pub job: JobRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobStatus {
    pub job: JobRef,
    pub state: JobState,
    pub analysis_version: u32,
    pub result_version: Option<u64>,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Capturing,
    Queued,
    RunningAsr,
    RunningLlm,
    Completed,
    Persisted,
    Cancelled,
    Failed,
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub related_message_id: Option<MessageId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id<T: TryFrom<String, Error = anyhow::Error>>(value: &str) -> T {
        value.to_string().try_into().unwrap()
    }

    #[test]
    fn rejects_stream_for_unknown_participant() {
        let request = SubmitPostCallJob {
            job: JobRef {
                job_id: id("job-1"),
                tenant_id: id("tenant-1"),
                conversation_id: id("conversation-1"),
                operation_id: id("operation-1"),
                generation: 1,
            },
            profile: AiProfileSnapshot {
                profile_id: id("profile-1"),
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
                participant_id: id("participant-1"),
                role: "caller".to_string(),
                display_number: None,
            }],
            streams: vec![StreamBinding {
                stream_id: id("stream-1"),
                participant_id: id("participant-2"),
                direction: MediaDirection::FromParticipant,
                codec: AudioCodec::Pcmu,
                sample_rate: 8_000,
                channels: 1,
            }],
        };

        assert!(request.validate().is_err());
    }

    #[test]
    fn validates_provider_requirements_by_pipeline_type() {
        let base = AiProfileSnapshot {
            profile_id: id("profile-1"),
            profile_version: 1,
            pipeline_type: AiPipelineType::Transcription,
            asr_provider_id: Some("mock-asr".to_string()),
            llm_provider_id: None,
            tts_provider_id: None,
            capture_complete_ratio: 0.995,
            capture_process_min_ratio: 0.95,
            capture_complete_max_gap_ms: 200,
            capture_process_max_gap_ms: 5_000,
        };
        assert!(base.validate().is_ok());
        assert!(
            AiProfileSnapshot {
                pipeline_type: AiPipelineType::LlmTask,
                asr_provider_id: None,
                llm_provider_id: Some("mock-llm".to_string()),
                ..base.clone()
            }
            .validate()
            .is_ok()
        );
        assert!(
            AiProfileSnapshot {
                pipeline_type: AiPipelineType::VoiceAgent,
                asr_provider_id: Some("mock-asr".to_string()),
                llm_provider_id: Some("mock-llm".to_string()),
                tts_provider_id: Some("mock-tts".to_string()),
                ..base.clone()
            }
            .validate()
            .is_ok()
        );
        assert!(
            AiProfileSnapshot {
                pipeline_type: AiPipelineType::PostCallAnalysis,
                llm_provider_id: None,
                ..base
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn connector_hello_fixture_matches_wire_type() {
        let envelope: ControlEnvelope =
            serde_json::from_str(include_str!("../fixtures/connector_hello.v1.json")).unwrap();
        envelope.validate().unwrap();
        assert!(matches!(
            envelope.message,
            ControlMessage::ConnectorHello(_)
        ));
    }

    #[test]
    fn validates_voice_agent_action_allowlist_parameters() {
        let conversation = JobRef {
            job_id: id("job-1"),
            tenant_id: id("tenant-1"),
            conversation_id: id("conversation-1"),
            operation_id: id("voice-agent-v1"),
            generation: 1,
        };
        let request = ActionRequested {
            conversation: conversation.clone(),
            operation_id: id("voice-agent-v1:action:1"),
            generation: 1,
            action: AgentAction::EndCall {
                reason: "user requested".to_string(),
            },
            deadline_at_ms: 10,
        };
        assert!(request.validate().is_ok());
        assert!(
            ActionRequested {
                action: AgentAction::TransferToExtension {
                    number: "12x".to_string(),
                },
                ..request
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn validates_realtime_assist_profile_and_streams() {
        let profile = AiProfileSnapshot {
            profile_id: id("assist-profile"),
            profile_version: 1,
            pipeline_type: AiPipelineType::RealtimeAssist,
            asr_provider_id: Some("asr".to_string()),
            llm_provider_id: Some("llm".to_string()),
            tts_provider_id: None,
            capture_complete_ratio: 0.995,
            capture_process_min_ratio: 0.95,
            capture_complete_max_gap_ms: 200,
            capture_process_max_gap_ms: 5_000,
        };
        let conversation = JobRef {
            job_id: id("job"),
            tenant_id: id("tenant"),
            conversation_id: id("conversation"),
            operation_id: id("operation"),
            generation: 1,
        };
        let participant = Participant {
            participant_id: id("caller"),
            role: "caller".into(),
            display_number: None,
        };
        let stream = StreamBinding {
            stream_id: id("audio"),
            participant_id: participant.participant_id.clone(),
            direction: MediaDirection::FromParticipant,
            codec: AudioCodec::Pcma,
            sample_rate: 8_000,
            channels: 1,
        };
        assert!(
            StartAssistConversation {
                conversation,
                profile,
                participants: vec![participant],
                streams: vec![stream]
            }
            .validate()
            .is_ok()
        );
    }
}
