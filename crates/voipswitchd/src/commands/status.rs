use crate::app::AppState;
use crate::commands::ConfigCommandRegistry;
use serde_json::json;
use std::collections::BTreeMap;
use voipswitch_core::command_service::CommandResult;

pub(super) fn status(state: &AppState, config_registry: &ConfigCommandRegistry) -> CommandResult {
    let config = state.config().snapshot();
    let adapter = state.adapter_runtime();
    let metrics = state.system_metrics();
    let mut items = BTreeMap::new();
    items.insert("service".to_string(), json!("voipswitchd"));
    items.insert("instance_id".to_string(), json!(config.system.instance_id));
    items.insert("data_dir".to_string(), json!(config.system.data_dir));
    items.insert("started_at_ms".to_string(), json!(state.started_at_ms()));
    items.insert("runtime_config_version".to_string(), json!(config.version));
    items.insert(
        "call_trace_enabled".to_string(),
        json!(config.call_trace_enabled()),
    );
    items.insert("sip_port".to_string(), json!(config.sip_port()));
    items.insert("log_level".to_string(), json!(config.log_level()));
    items.insert("recording_dir".to_string(), json!(config.recording_dir()));
    items.insert(
        "recording_retention_days".to_string(),
        json!(config.recording_retention_days()),
    );
    items.insert(
        "recording_max_size_gb".to_string(),
        json!(config.recording_max_size_gb()),
    );
    items.insert("adapter_sip_port".to_string(), json!(adapter.sip_port));
    items.insert(
        "adapter_bind_source".to_string(),
        json!(adapter.bind_source),
    );
    items.insert(
        "sip_restart_required".to_string(),
        json!(
            adapter.bind_source.as_deref() != Some("cli")
                && adapter
                    .sip_port
                    .is_some_and(|port| port != config.sip_port())
        ),
    );
    items.insert("sampled_at_ms".to_string(), json!(metrics.sampled_at_ms));
    items.insert("cpu_percent".to_string(), json!(metrics.cpu_percent));
    items.insert(
        "memory_used_bytes".to_string(),
        json!(metrics.memory_used_bytes),
    );
    items.insert(
        "memory_total_bytes".to_string(),
        json!(metrics.memory_total_bytes),
    );
    items.insert(
        "active_call_count".to_string(),
        json!(metrics.active_call_count),
    );
    items.insert("domain_count".to_string(), json!(config.domains.len()));
    items.insert(
        "adapter_clients".to_string(),
        json!(state.adapter_clients()),
    );
    items.insert(
        "config_resources".to_string(),
        json!(config_registry.names()),
    );
    CommandResult::kv("status", items)
}

pub(super) fn config_check(
    state: &AppState,
    config_registry: &ConfigCommandRegistry,
) -> CommandResult {
    let config = state.config().snapshot();
    let mut items = BTreeMap::new();
    items.insert("valid".to_string(), json!(true));
    items.insert("runtime_config_version".to_string(), json!(config.version));
    items.insert("domain_count".to_string(), json!(config.domains.len()));
    items.insert(
        "call_trace_enabled".to_string(),
        json!(config.call_trace_enabled()),
    );
    items.insert("sip_port".to_string(), json!(config.sip_port()));
    items.insert("log_level".to_string(), json!(config.log_level()));
    items.insert(
        "config_resources".to_string(),
        json!(config_registry.names()),
    );
    CommandResult::kv("config valid", items)
}
