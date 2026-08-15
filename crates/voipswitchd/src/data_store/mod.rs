pub(crate) mod seaorm;

use crate::config_service::RuntimeConfig;
use ai_protocol::control::{CaptureQuality, StructuredCallResult, TranscriptSegment};
use ai_protocol::id::{JobId, OperationId, ProfileId};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use voipswitch_core::media::MediaForwardingMode;

pub use seaorm::SeaOrmConfigBackend;

pub trait ConfigBackend: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn load_runtime_config(&self) -> Result<RuntimeConfig>;
    fn health_check(&self) -> Result<()>;
    fn insert_cdr(&self, record: &CdrRecord) -> Result<()>;
    fn list_cdr(&self, domain_id: Option<&str>, page: PageRequest)
    -> Result<PageResult<CdrRecord>>;
    fn insert_call_trace_message(&self, message: &CallTraceMessage) -> Result<()>;
    fn mark_call_trace_incomplete(&self, call_id: &str, domain_id: &str) -> Result<()>;
    fn complete_call_trace(
        &self,
        call_id: &str,
        domain_id: &str,
        ended_at_ms: u64,
        incomplete: bool,
    ) -> Result<()>;
    fn get_call_trace(&self, call_id: &str, domain_id: Option<&str>) -> Result<Option<CallTrace>>;
    fn upsert_recording(&self, record: &RecordingRecord) -> Result<()>;
    fn get_recording(
        &self,
        call_id: &str,
        domain_id: Option<&str>,
    ) -> Result<Option<RecordingRecord>>;
    fn cleanup_recordings(&self, retention_days: u64, max_size_gb: u64, now_ms: u64)
    -> Result<u64>;
    fn insert_leg_cdr(&self, record: &LegCdrRecord) -> Result<()>;
    #[allow(dead_code)]
    fn list_leg_cdrs(&self, call_id: &str, domain_id: Option<&str>) -> Result<Vec<LegCdrRecord>>;
    fn cdr_spool_dir(&self, domain_id: &str) -> Result<PathBuf>;
    fn list_cdr_spool_domains(&self) -> Result<Vec<String>>;
    fn ai_outbox_dir(&self, domain_id: &str) -> Result<PathBuf>;
    fn list_ai_outbox_domains(&self) -> Result<Vec<String>>;
    fn persist_ai_result(&self, record: &AiCallResultRecord) -> Result<()>;
    fn get_ai_results(&self, call_id: &str, domain_id: &str) -> Result<Vec<AiCallResultRecord>>;

    fn persist_cdr_batch(&self, command: &CdrWriteCommand) -> Result<()> {
        let domain_id = command.call_cdr.domain_id.as_str();
        if command.trace_domain_id != domain_id
            || command
                .leg_cdrs
                .iter()
                .any(|leg| leg.domain_id != domain_id)
            || command
                .recording
                .as_ref()
                .is_some_and(|recording| recording.domain_id != domain_id)
        {
            bail!("CDR write batch contains records from multiple domains");
        }
        if let Some(recording) = &command.recording {
            self.upsert_recording(recording)?;
        }
        for leg in &command.leg_cdrs {
            self.insert_leg_cdr(leg)?;
        }
        self.insert_cdr(&command.call_cdr)?;
        if command.call_cdr.trace_available {
            self.complete_call_trace(
                &command.trace_call_id,
                &command.trace_domain_id,
                command.trace_ended_at_ms,
                false,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AiCallResultRecord {
    pub job_id: JobId,
    pub result_version: u64,
    pub domain_id: String,
    pub call_id: String,
    pub operation_id: OperationId,
    pub generation: u64,
    pub profile_id: ProfileId,
    pub profile_version: u64,
    pub capture_quality: CaptureQuality,
    pub transcript: Vec<TranscriptSegment>,
    pub result: StructuredCallResult,
    pub received_at_ms: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct PageRequest {
    pub page: u64,
    pub page_size: u64,
}

#[derive(Debug, Clone)]
pub struct PageResult<T> {
    pub rows: Vec<T>,
    pub total: u64,
    pub page: Option<PageRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CdrRecord {
    pub call_id: String,
    pub domain_id: String,
    pub caller_number: String,
    pub callee_number: String,
    #[serde(default)]
    pub inbound_route_id: Option<String>,
    #[serde(default)]
    pub inbound_route_name: Option<String>,
    #[serde(default)]
    pub inbound_trunk_ref: Option<String>,
    #[serde(default)]
    pub inbound_trunk_name: Option<String>,
    #[serde(default)]
    pub outbound_route_id: Option<String>,
    #[serde(default)]
    pub outbound_route_name: Option<String>,
    #[serde(default)]
    pub outbound_trunk_ref: Option<String>,
    #[serde(default)]
    pub outbound_trunk_name: Option<String>,
    pub started_at_ms: u64,
    pub answered_at_ms: Option<u64>,
    pub ended_at_ms: u64,
    pub duration_ms: u64,
    pub billable_ms: u64,
    pub answered: bool,
    pub final_status: Option<u16>,
    pub hangup_cause: String,
    #[serde(default)]
    pub media_forwarding_mode: Option<MediaForwardingMode>,
    pub caller_to_callee_packets: u64,
    pub caller_to_callee_bytes: u64,
    pub callee_to_caller_packets: u64,
    pub callee_to_caller_bytes: u64,
    pub caller_to_callee_rtcp_packets: u64,
    pub callee_to_caller_rtcp_packets: u64,
    #[serde(default)]
    pub trace_available: bool,
    #[serde(default)]
    pub trace_incomplete: bool,
    #[serde(default)]
    pub recording_status: Option<String>,
    #[serde(default)]
    pub recording_available: bool,
    #[serde(default)]
    pub incomplete: bool,
    #[serde(default)]
    pub incomplete_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LegCdrRecord {
    pub call_id: String,
    pub session_id: String,
    pub domain_id: String,
    pub leg_role: String,
    pub direction: String,
    pub endpoint_ref: Option<String>,
    pub endpoint_number: Option<String>,
    pub signaling_number: Option<String>,
    pub route_id: Option<String>,
    pub route_name: Option<String>,
    pub trunk_ref: Option<String>,
    pub trunk_name: Option<String>,
    pub joined_at_ms: u64,
    pub answered_at_ms: Option<u64>,
    pub left_at_ms: u64,
    pub final_status: Option<u16>,
    pub hangup_cause: Option<String>,
    pub media_packets: u64,
    pub media_bytes: u64,
    pub media_rtcp_packets: u64,
    pub bridge_ids: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdrWriteCommand {
    pub call_cdr: CdrRecord,
    pub leg_cdrs: Vec<LegCdrRecord>,
    pub recording: Option<RecordingRecord>,
    pub trace_call_id: String,
    pub trace_domain_id: String,
    pub trace_ended_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordingRecord {
    pub recording_id: String,
    pub call_id: String,
    pub domain_id: String,
    pub status: String,
    pub caller_number: String,
    pub callee_number: String,
    pub started_at_ms: u64,
    pub ended_at_ms: Option<u64>,
    pub duration_ms: u64,
    pub format: String,
    pub sample_rate: u32,
    pub channel_count: u8,
    pub file_name: String,
    #[serde(skip_serializing)]
    pub storage_root: String,
    #[serde(skip_serializing)]
    pub storage_path: String,
    pub file_size_bytes: u64,
    pub packets_tapped: u64,
    pub packets_dropped: u64,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CallTraceMessage {
    pub call_id: String,
    pub domain_id: String,
    pub sequence: u64,
    pub observed_at_ms: u64,
    pub direction: String,
    pub adapter_call_leg_id: String,
    pub session_id: Option<String>,
    pub source_addr: Option<String>,
    pub destination_addr: Option<String>,
    pub start_line: String,
    pub packet: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CallTrace {
    pub call_id: String,
    pub domain_id: String,
    pub ended_at_ms: Option<u64>,
    pub incomplete: bool,
    pub messages: Vec<CallTraceMessage>,
}

#[derive(Debug, Clone)]
pub enum ConfigBackendSettings {
    Sqlite {
        data_dir: PathBuf,
        instance_id: String,
    },
    Mysql {
        url: String,
        instance_id: String,
    },
}

pub fn open_config_backend(settings: ConfigBackendSettings) -> Result<Arc<dyn ConfigBackend>> {
    match settings {
        ConfigBackendSettings::Sqlite {
            data_dir,
            instance_id,
        } => Ok(Arc::new(SeaOrmConfigBackend::sqlite(
            data_dir,
            instance_id,
        )?)),
        ConfigBackendSettings::Mysql { url, instance_id } => {
            let _ = (url, instance_id);
            bail!("mysql config backend is not implemented yet")
        }
    }
}
