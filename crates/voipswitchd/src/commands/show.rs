use crate::app::AppState;
use crate::commands::{ApiCommandHandler, object_result};
use crate::data_store::PageRequest;
use crate::pbx;
use anyhow::{Result, anyhow};
use serde_json::json;
use std::collections::BTreeMap;
use voipswitch_core::command_service::{ApiCommand, CommandRenderData, CommandResult};
use voipswitch_core::types::ids::DomainId;

pub(super) struct ShowApiCommand;

impl ApiCommandHandler for ShowApiCommand {
    fn name(&self) -> &str {
        "show"
    }

    fn handle(&self, state: &AppState, command: &ApiCommand) -> Result<CommandResult> {
        let Some(topic) = command.args.first() else {
            return Err(anyhow!("show requires a topic"));
        };

        match topic.as_str() {
            "status" => {
                let config = state.config().snapshot();
                let mut items = BTreeMap::new();
                items.insert("service".to_string(), json!("voipswitchd"));
                items.insert("instance_id".to_string(), json!(config.system.instance_id));
                items.insert("data_dir".to_string(), json!(config.system.data_dir));
                items.insert("started_at_ms".to_string(), json!(state.started_at_ms()));
                items.insert("runtime_config_version".to_string(), json!(config.version));
                items.insert("domain_count".to_string(), json!(config.domains.len()));
                items.insert(
                    "adapter_clients".to_string(),
                    json!(state.adapter_clients()),
                );
                Ok(CommandResult::kv("status", items))
            }
            "calls" => {
                let rows = state
                    .active_calls(command.domain_id.as_ref().map(DomainId::as_str))
                    .into_iter()
                    .map(|call| {
                        vec![
                            json!(call.call_id),
                            json!(call.domain_id),
                            json!(call.caller_number),
                            json!(call.callee_number),
                            json!(call.state),
                            json!(call.started_at_ms),
                            json!(call.answered_at_ms),
                            json!(call.last_status),
                            json!(call.topology.coordinator),
                            json!(call.topology.coordinator_generation),
                            json!(call.topology.legs.len()),
                            json!(call.topology.bridges.len()),
                        ]
                    })
                    .collect();
                Ok(CommandResult {
                    code: "OK".to_string(),
                    message: "active calls".to_string(),
                    data: CommandRenderData::Table {
                        columns: vec![
                            "call_id".to_string(),
                            "domain_id".to_string(),
                            "caller_number".to_string(),
                            "callee_number".to_string(),
                            "state".to_string(),
                            "started_at_ms".to_string(),
                            "answered_at_ms".to_string(),
                            "last_status".to_string(),
                            "coordinator_session_id".to_string(),
                            "coordinator_generation".to_string(),
                            "leg_count".to_string(),
                            "bridge_count".to_string(),
                        ],
                        rows,
                    },
                    warnings: Vec::new(),
                })
            }
            "call" => {
                let call_id = command
                    .args
                    .get(1)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| anyhow!("INVALID_ARGUMENT: show call requires call_id"))?;
                let call = state
                    .active_calls(command.domain_id.as_ref().map(DomainId::as_str))
                    .into_iter()
                    .find(|call| call.call_id == *call_id)
                    .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: active call {call_id}"))?;
                Ok(object_result(
                    "active call topology",
                    json!({
                        "resource": "active_call",
                        "data": call,
                    }),
                ))
            }
            "sessions" => {
                let rows = state
                    .active_sessions(command.domain_id.as_ref().map(DomainId::as_str))
                    .into_iter()
                    .map(|session| {
                        vec![
                            json!(session.session_id),
                            json!(session.call_id),
                            json!(session.domain_id),
                            json!(session.direction),
                            json!(session.number),
                            json!(session.peer_number),
                            json!(session.state),
                            json!(session.started_at_ms),
                            json!(session.answered_at_ms),
                        ]
                    })
                    .collect();
                Ok(CommandResult {
                    code: "OK".to_string(),
                    message: "active sessions".to_string(),
                    data: CommandRenderData::Table {
                        columns: vec![
                            "session_id".to_string(),
                            "call_id".to_string(),
                            "domain_id".to_string(),
                            "direction".to_string(),
                            "number".to_string(),
                            "peer_number".to_string(),
                            "state".to_string(),
                            "started_at_ms".to_string(),
                            "answered_at_ms".to_string(),
                        ],
                        rows,
                    },
                    warnings: Vec::new(),
                })
            }
            "cdr" => {
                let page = parse_page_request(&command.fields)?;
                let result = state
                    .backend()
                    .list_cdr(command.domain_id.as_ref().map(DomainId::as_str), page)?;
                Ok(object_result(
                    "call detail records",
                    json!({
                        "resource": "cdr",
                        "data": {
                            "records": result.rows,
                            "pagination": {
                                "page": page.page,
                                "page_size": page.page_size,
                                "total": result.total,
                            }
                        }
                    }),
                ))
            }
            "ai-profiles" => {
                let catalog = state.ai_jobs().and_then(|jobs| jobs.profile_catalog());
                Ok(object_result(
                    "AI profile catalog",
                    json!({
                        "resource": "ai_profiles",
                        "data": {
                            "available": catalog.is_some(),
                            "catalog_version": catalog.as_ref().map(|value| value.catalog_version),
                            "profiles": catalog.map(|value| value.profiles).unwrap_or_default(),
                        },
                    }),
                ))
            }
            "ai-results" => {
                let call_id = command
                    .args
                    .get(1)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| anyhow!("INVALID_ARGUMENT: show ai-results requires call_id"))?;
                let domain_id = command.domain_id.as_ref().ok_or_else(|| {
                    anyhow!("INVALID_ARGUMENT: show ai-results requires --domain")
                })?;
                let results = state
                    .backend()
                    .get_ai_results(call_id, domain_id.as_str())?;
                Ok(object_result(
                    "call AI results",
                    json!({
                        "resource": "ai_results",
                        "data": {
                            "call_id": call_id,
                            "domain_id": domain_id,
                            "results": results,
                        },
                    }),
                ))
            }
            "call-trace" => {
                let call_id = command
                    .args
                    .get(1)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| anyhow!("INVALID_ARGUMENT: show call-trace requires call_id"))?;
                let trace = state
                    .backend()
                    .get_call_trace(call_id, command.domain_id.as_ref().map(DomainId::as_str))?
                    .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: call trace {call_id}"))?;
                Ok(object_result(
                    "call trace",
                    json!({
                        "resource": "call_trace",
                        "data": trace,
                    }),
                ))
            }
            "dtmf-operation" => {
                let operation_id = command
                    .args
                    .get(1)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        anyhow!("INVALID_ARGUMENT: show dtmf-operation requires operation_id")
                    })?;
                let operation = state
                    .dtmf_operations()
                    .get(operation_id)
                    .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: DTMF operation {operation_id}"))?;
                Ok(object_result(
                    "DTMF operation",
                    json!({
                        "resource": "dtmf_operation",
                        "data": operation,
                    }),
                ))
            }
            "recording" => {
                let call_id = command
                    .args
                    .get(1)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| anyhow!("INVALID_ARGUMENT: show recording requires call_id"))?;
                let recording = state
                    .backend()
                    .get_recording(call_id, command.domain_id.as_ref().map(DomainId::as_str))?
                    .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: recording {call_id}"))?;
                Ok(object_result(
                    "call recording",
                    json!({
                        "resource": "recording",
                        "data": recording,
                    }),
                ))
            }
            "recording-file" => {
                let call_id = command
                    .args
                    .get(1)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        anyhow!("INVALID_ARGUMENT: show recording-file requires call_id")
                    })?;
                let recording = state
                    .backend()
                    .get_recording(call_id, command.domain_id.as_ref().map(DomainId::as_str))?
                    .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: recording {call_id}"))?;
                if !matches!(recording.status.as_str(), "complete" | "incomplete")
                    || recording.storage_path.is_empty()
                {
                    return Err(anyhow!("RESOURCE_NOT_FOUND: recording file {call_id}"));
                }
                let root = std::fs::canonicalize(&recording.storage_root)
                    .map_err(|_| anyhow!("RESOURCE_NOT_FOUND: recording root {call_id}"))?;
                let path = std::fs::canonicalize(&recording.storage_path)
                    .map_err(|_| anyhow!("RESOURCE_NOT_FOUND: recording file {call_id}"))?;
                if !path.starts_with(&root) || !path.is_file() {
                    return Err(anyhow!("INVALID_RECORDING_PATH: recording file {call_id}"));
                }
                Ok(object_result(
                    "recording file grant",
                    json!({
                        "resource": "recording_file",
                        "data": {
                            "call_id": call_id,
                            "domain_id": recording.domain_id,
                            "file_name": recording.file_name,
                            "content_type": "audio/wav",
                            "file_size_bytes": recording.file_size_bytes,
                            "path": path,
                        },
                    }),
                ))
            }
            _ => pbx::handle_show_command(state, command)?
                .ok_or_else(|| anyhow!("unsupported show topic: {topic}")),
        }
    }
}

fn parse_page_request(fields: &BTreeMap<String, String>) -> Result<PageRequest> {
    let page = fields
        .get("page")
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|_| anyhow!("INVALID_ARGUMENT: page must be a positive integer"))?
        .unwrap_or(1);
    let page_size = fields
        .get("page-size")
        .or_else(|| fields.get("page_size"))
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|_| anyhow!("INVALID_ARGUMENT: page-size must be a positive integer"))?
        .unwrap_or(50);
    if page == 0 || page_size == 0 || page_size > 500 {
        return Err(anyhow!(
            "INVALID_ARGUMENT: page must be >= 1 and page-size must be 1..=500"
        ));
    }
    Ok(PageRequest { page, page_size })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_store::{ConfigBackend, SeaOrmConfigBackend};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn state() -> AppState {
        let root = tempdir().unwrap().keep();
        let backend = Arc::new(SeaOrmConfigBackend::sqlite(&root, "test").unwrap());
        let config = backend.load_runtime_config().unwrap();
        AppState::new(config, backend, 1)
    }

    fn command(topic: &str, args: &[&str], domain_id: Option<&str>) -> ApiCommand {
        ApiCommand {
            name: "show".to_string(),
            args: std::iter::once(topic.to_string())
                .chain(args.iter().map(|value| (*value).to_string()))
                .collect(),
            domain_id: domain_id.map(DomainId::from),
            key: None,
            fields: BTreeMap::new(),
        }
    }

    #[test]
    fn ai_results_require_explicit_domain() {
        let error = ShowApiCommand
            .handle(&state(), &command("ai-results", &["call-1"], None))
            .unwrap_err();
        assert!(error.to_string().contains("requires --domain"));
    }

    #[test]
    fn ai_results_return_an_empty_domain_scoped_collection() {
        let result = ShowApiCommand
            .handle(
                &state(),
                &command("ai-results", &["call-1"], Some("domain-1")),
            )
            .unwrap();
        let CommandRenderData::Object { value } = result.data else {
            panic!("AI results must use object rendering");
        };
        assert_eq!(value["data"]["domain_id"], "domain-1");
        assert_eq!(value["data"]["results"], json!([]));
    }

    #[test]
    fn ai_profiles_report_catalog_unavailable_before_connector_start() {
        let result = ShowApiCommand
            .handle(&state(), &command("ai-profiles", &[], None))
            .unwrap();
        let CommandRenderData::Object { value } = result.data else {
            panic!("AI profiles must use object rendering");
        };
        assert_eq!(value["data"]["available"], false);
        assert_eq!(value["data"]["profiles"], json!([]));
    }
}
