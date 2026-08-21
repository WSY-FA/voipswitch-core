use crate::pbx::ai_policy::model::AiPolicyConfig;
use crate::pbx::extension::model::ExtensionConfig;
use crate::pbx::recording::model::RecordingPolicyConfig;
use crate::pbx::route::model::{InboundRouteConfig, OutboundRouteConfig};
use crate::pbx::trunk::model::{PeerTrunkConfig, RegisterAccountConfig, RegisterTrunkConfig};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use voipswitch_core::types::ids::DomainId;

pub const CALL_TRACE_ENABLED_KEY: &str = "call_trace_enabled";
pub const SIP_PORT_KEY: &str = "sip_port";
pub const LOG_LEVEL_KEY: &str = "log_level";
pub const RECORDING_DIR_KEY: &str = "recording_dir";
pub const RECORDING_RETENTION_DAYS_KEY: &str = "recording_retention_days";
pub const RECORDING_MAX_SIZE_GB_KEY: &str = "recording_max_size_gb";
pub const CDR_SPOOL_WARNING_MB_KEY: &str = "cdr_spool_warning_mb";
pub const CDR_SPOOL_REJECT_MB_KEY: &str = "cdr_spool_reject_mb";
pub const CDR_SPOOL_RESUME_MB_KEY: &str = "cdr_spool_resume_mb";
pub const DEFAULT_SIP_PORT: u16 = 5060;
pub const DEFAULT_LOG_LEVEL: &str = "info";
pub const DEFAULT_RECORDING_RETENTION_DAYS: u64 = 30;
pub const DEFAULT_RECORDING_MAX_SIZE_GB: u64 = 20;
pub const DEFAULT_CDR_SPOOL_WARNING_MB: u64 = 512;
pub const DEFAULT_CDR_SPOOL_REJECT_MB: u64 = 1024;
pub const DEFAULT_CDR_SPOOL_RESUME_MB: u64 = 800;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CdrSpoolLimits {
    pub warning_mb: u64,
    pub reject_mb: u64,
    pub resume_mb: u64,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub system: SystemConfig,
    pub globals: BTreeMap<String, GlobalValue>,
    pub domains: BTreeMap<DomainId, Arc<DomainRuntimeConfig>>,
    pub version: u64,
}

impl RuntimeConfig {
    pub fn call_trace_enabled(&self) -> bool {
        match self.globals.get(CALL_TRACE_ENABLED_KEY) {
            Some(GlobalValue::Bool(enabled)) => *enabled,
            _ => true,
        }
    }

    pub fn sip_port(&self) -> u16 {
        match self.globals.get(SIP_PORT_KEY) {
            Some(GlobalValue::Integer(port)) => u16::try_from(*port).ok().filter(|port| *port > 0),
            _ => None,
        }
        .unwrap_or(DEFAULT_SIP_PORT)
    }

    pub fn log_level(&self) -> &str {
        match self.globals.get(LOG_LEVEL_KEY) {
            Some(GlobalValue::String(level)) if is_log_level(level) => level,
            _ => DEFAULT_LOG_LEVEL,
        }
    }

    pub fn recording_dir(&self) -> PathBuf {
        match self.globals.get(RECORDING_DIR_KEY) {
            Some(GlobalValue::String(path)) if !path.trim().is_empty() => PathBuf::from(path),
            _ => Path::new(&self.system.data_dir).join("recordings"),
        }
    }

    pub fn recording_retention_days(&self) -> u64 {
        positive_global_integer(
            &self.globals,
            RECORDING_RETENTION_DAYS_KEY,
            DEFAULT_RECORDING_RETENTION_DAYS,
        )
    }

    pub fn recording_max_size_gb(&self) -> u64 {
        positive_global_integer(
            &self.globals,
            RECORDING_MAX_SIZE_GB_KEY,
            DEFAULT_RECORDING_MAX_SIZE_GB,
        )
    }

    pub fn cdr_spool_limits(&self) -> CdrSpoolLimits {
        CdrSpoolLimits {
            warning_mb: positive_global_integer(
                &self.globals,
                CDR_SPOOL_WARNING_MB_KEY,
                DEFAULT_CDR_SPOOL_WARNING_MB,
            ),
            reject_mb: positive_global_integer(
                &self.globals,
                CDR_SPOOL_REJECT_MB_KEY,
                DEFAULT_CDR_SPOOL_REJECT_MB,
            ),
            resume_mb: positive_global_integer(
                &self.globals,
                CDR_SPOOL_RESUME_MB_KEY,
                DEFAULT_CDR_SPOOL_RESUME_MB,
            ),
        }
    }
}

fn positive_global_integer(
    globals: &BTreeMap<String, GlobalValue>,
    key: &str,
    default: u64,
) -> u64 {
    match globals.get(key) {
        Some(GlobalValue::Integer(value)) => u64::try_from(*value).ok().filter(|value| *value > 0),
        _ => None,
    }
    .unwrap_or(default)
}

pub fn is_log_level(value: &str) -> bool {
    matches!(value, "error" | "warn" | "info" | "debug" | "trace")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemConfig {
    pub instance_id: String,
    pub data_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum GlobalValue {
    String(String),
    Integer(i64),
    Bool(bool),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainRuntimeConfig {
    pub domain_id: DomainId,
    pub name: String,
    pub realm: String,
    pub password: String,
    pub remark: String,
    pub enabled: bool,
    pub extensions: Vec<ExtensionConfig>,
    pub peer_trunks: Vec<PeerTrunkConfig>,
    pub reg_trunks: Vec<RegisterTrunkConfig>,
    pub reg_accounts: Vec<RegisterAccountConfig>,
    pub inbound_routes: Vec<InboundRouteConfig>,
    pub outbound_routes: Vec<OutboundRouteConfig>,
    pub recording_policies: Vec<RecordingPolicyConfig>,
    pub ai_policies: Vec<AiPolicyConfig>,
    pub ai_agents: Vec<crate::pbx::ai_agent::model::AiAgentConfig>,
    pub version: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_settings_use_defaults_and_typed_values() {
        let mut config = RuntimeConfig {
            system: SystemConfig {
                instance_id: "test".to_string(),
                data_dir: "/tmp/test".to_string(),
            },
            globals: BTreeMap::new(),
            domains: BTreeMap::new(),
            version: 1,
        };
        assert!(config.call_trace_enabled());
        assert_eq!(
            config.cdr_spool_limits(),
            CdrSpoolLimits {
                warning_mb: 512,
                reject_mb: 1024,
                resume_mb: 800,
            }
        );

        config
            .globals
            .insert(CALL_TRACE_ENABLED_KEY.to_string(), GlobalValue::Bool(false));
        config
            .globals
            .insert(SIP_PORT_KEY.to_string(), GlobalValue::Integer(15060));
        config.globals.insert(
            LOG_LEVEL_KEY.to_string(),
            GlobalValue::String("debug".to_string()),
        );
        config.globals.insert(
            CDR_SPOOL_WARNING_MB_KEY.to_string(),
            GlobalValue::Integer(256),
        );
        assert!(!config.call_trace_enabled());
        assert_eq!(config.sip_port(), 15060);
        assert_eq!(config.log_level(), "debug");
        assert_eq!(config.cdr_spool_limits().warning_mb, 256);
        assert_eq!(
            config.recording_dir(),
            PathBuf::from("/tmp/test/recordings")
        );
    }
}
