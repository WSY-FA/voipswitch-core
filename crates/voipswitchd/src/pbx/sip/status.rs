use crate::app::{AppState, RegistrationState};
use crate::commands::{ConfigCommandHandler, ConfigCommandRegistry};
use crate::pbx::command_helpers::output;
use crate::pbx::vc_config::VcConfigTableHandler;
use anyhow::{Result, anyhow};
use serde_json::json;
use std::sync::Arc;
use voipswitch_core::command_service::{CommandResult, ConfigCommand};
use voipswitch_core::types::ids::DomainId;

pub fn register_config_commands(registry: &ConfigCommandRegistry) {
    registry.register(Arc::new(ExtStatusResource));
    registry.register(Arc::new(SipTrunkStatusResource));
}

pub fn register_vc_config_tables(registry: &mut crate::pbx::vc_config::VcConfigTableRegistry) {
    registry.register(Arc::new(ExtStatusResource));
    registry.register(Arc::new(SipTrunkStatusResource));
}

struct ExtStatusResource;
struct SipTrunkStatusResource;

impl ConfigCommandHandler for ExtStatusResource {
    fn name(&self) -> &str {
        "ext_status"
    }

    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<CommandResult> {
        if command.action != "select" {
            return Err(anyhow!("unsupported ext_status action: {}", command.action));
        }
        Ok(output(
            command,
            registration_data(state, command.domain_id.as_ref()),
        ))
    }
}

impl VcConfigTableHandler for ExtStatusResource {
    fn table(&self) -> &str {
        "ext_status"
    }

    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<serde_json::Value> {
        if command.action != "select" {
            return Err(anyhow!(
                "unsupported vc config action for ext_status: {}",
                command.action
            ));
        }
        Ok(registration_data(state, command.domain_id.as_ref()))
    }
}

impl ConfigCommandHandler for SipTrunkStatusResource {
    fn name(&self) -> &str {
        "siptrk_status"
    }

    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<CommandResult> {
        if command.action != "select" {
            return Err(anyhow!(
                "unsupported siptrk_status action: {}",
                command.action
            ));
        }
        Ok(output(
            command,
            trunk_runtime_data(state, command.domain_id.as_ref()),
        ))
    }
}

impl VcConfigTableHandler for SipTrunkStatusResource {
    fn table(&self) -> &str {
        "siptrk_status"
    }

    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<serde_json::Value> {
        if command.action != "select" {
            return Err(anyhow!(
                "unsupported vc config action for siptrk_status: {}",
                command.action
            ));
        }
        Ok(trunk_runtime_data(state, command.domain_id.as_ref()))
    }
}

fn registration_data(state: &AppState, domain_filter: Option<&DomainId>) -> serde_json::Value {
    let mirror = state.registrations();
    let config = state.config().snapshot();
    let registrations: Vec<_> = mirror
        .items
        .values()
        .filter(|registration| {
            matches!(registration.state, RegistrationState::Registered)
                && domain_filter
                    .map(|domain_id| domain_id.as_str() == registration.domain_id)
                    .unwrap_or(true)
        })
        .map(|registration| {
            let number = registration
                .endpoint_id
                .parse::<u64>()
                .ok()
                .and_then(|endpoint_id| {
                    config
                        .domains
                        .values()
                        .find(|domain| domain.domain_id.as_str() == registration.domain_id)
                        .and_then(|domain| {
                            domain
                                .extensions
                                .iter()
                                .find(|extension| extension.id == endpoint_id)
                        })
                        .map(|extension| extension.number.clone())
                });
            json!({
                "domain_id": registration.domain_id,
                "number": number,
                "endpoint_id": registration.endpoint_id,
                "status": registration.state,
                "state": registration.state,
                "aor": registration.contact,
                "contact": registration.contact,
                "route_target": registration.route_target,
                "expires_at_ms": registration.expires_at_ms,
                "agent": registration.user_agent,
                "user_agent": registration.user_agent,
                "version": registration.version,
                "source": "memory",
            })
        })
        .collect();

    json!({
        "mirror_ready": mirror.ready,
        "registrations": registrations,
    })
}

fn trunk_runtime_data(state: &AppState, domain_filter: Option<&DomainId>) -> serde_json::Value {
    let mirror = state.trunks();
    let registrations: Vec<_> = mirror
        .registrations
        .values()
        .filter(|registration| {
            domain_filter
                .map(|domain_id| domain_id.as_str() == registration.domain_id)
                .unwrap_or(true)
        })
        .map(|registration| {
            json!({
                "domain_id": registration.domain_id,
                "trunk_ref": format!(
                    "reg:{}/{}",
                    registration.reg_trunk_id, registration.reg_account_id
                ),
                "trunk_type": "reg_account",
                "reg_trunk_id": registration.reg_trunk_id,
                "reg_account_id": registration.reg_account_id,
                "state": registration.state,
                "expires_at_ms": registration.expires_at_ms,
                "response_code": registration.response_code,
                "reason": registration.reason,
                "version": registration.version,
                "source": "memory",
            })
        })
        .collect();
    let health: Vec<_> = mirror
        .health
        .values()
        .filter(|item| {
            domain_filter
                .map(|domain_id| domain_id.as_str() == item.domain_id)
                .unwrap_or(true)
        })
        .map(|item| {
            json!({
                "domain_id": item.domain_id,
                "trunk_ref": format!("{}:{}", item.trunk_type, item.trunk_id),
                "trunk_type": item.trunk_type,
                "trunk_id": item.trunk_id,
                "state": item.state,
                "checked_at_ms": item.checked_at_ms,
                "response_code": item.response_code,
                "reason": item.reason,
                "version": item.version,
                "source": "memory",
            })
        })
        .collect();

    json!({
        "mirror_ready": mirror.ready,
        "registrations": registrations,
        "health": health,
    })
}
