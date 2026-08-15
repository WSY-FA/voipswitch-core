use crate::app::AppState;
use crate::commands::object_result;
use crate::data_store::seaorm::SeaOrmConfigBackend;
use anyhow::{Result, anyhow};
use serde_json::json;
use std::collections::BTreeMap;
use voipswitch_core::command_service::{CommandResult, ConfigCommand};
use voipswitch_core::types::ids::DomainId;

use crate::data_store::PageRequest;

pub(crate) fn output(command: &ConfigCommand, data: serde_json::Value) -> CommandResult {
    object_result(
        format!("{} {}", command.resource, command.action),
        json!({
            "resource": command.resource,
            "action": command.action,
            "data": data,
        }),
    )
}

pub(crate) fn required_domain(command: &ConfigCommand) -> Result<DomainId> {
    command
        .domain_id
        .clone()
        .or_else(|| command.fields.get("domain").cloned().map(DomainId::from))
        .ok_or_else(|| anyhow!("domain is required"))
}

pub(crate) fn selected_keys(command: &ConfigCommand, default: &[&str]) -> Vec<String> {
    command
        .fields
        .get("keys")
        .map(|keys| {
            keys.split(',')
                .map(str::trim)
                .filter(|key| !key.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|keys| !keys.is_empty())
        .unwrap_or_else(|| default.iter().map(|key| key.to_string()).collect())
}

pub(crate) fn decoded_set(command: &ConfigCommand) -> Result<BTreeMap<String, String>> {
    if let Some(set) = command.fields.get("set") {
        return parse_assignments(set);
    }

    let set = command
        .fields
        .iter()
        .filter(|(key, _)| !matches!(key.as_str(), "table" | "cond" | "keys"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    if set.is_empty() {
        return Err(anyhow!("set is required"));
    }
    Ok(set)
}

pub(crate) fn page_request(command: &ConfigCommand) -> Option<PageRequest> {
    let page = command
        .fields
        .get("page")
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)?;
    let page_size = command
        .fields
        .get("page-size")
        .or_else(|| command.fields.get("page_size"))
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(50)
        .min(500);
    Some(PageRequest { page, page_size })
}

pub(crate) fn pagination_value(total: u64, page: Option<PageRequest>) -> Option<serde_json::Value> {
    page.map(|page| {
        serde_json::json!({
            "page": page.page,
            "page_size": page.page_size,
            "total": total,
            "total_pages": total.div_ceil(page.page_size),
        })
    })
}

pub(crate) fn apply_page<E>(
    query: sea_orm::Select<E>,
    page: Option<PageRequest>,
) -> sea_orm::Select<E>
where
    E: sea_orm::EntityTrait,
{
    if let Some(page) = page {
        let offset = page.page.saturating_sub(1).saturating_mul(page.page_size);
        use sea_orm::QuerySelect;
        query.limit(page.page_size).offset(offset)
    } else {
        query
    }
}

pub(crate) fn with_seaorm_backend<T>(
    state: &AppState,
    f: impl FnOnce(&SeaOrmConfigBackend) -> Result<T>,
) -> Result<T> {
    let backend = state.backend();
    let seaorm = backend
        .as_any()
        .downcast_ref::<SeaOrmConfigBackend>()
        .ok_or_else(|| anyhow!("current config backend does not expose SeaORM read operations"))?;
    f(seaorm)
}

pub(crate) fn required_set(set: &BTreeMap<String, String>, name: &str) -> Result<String> {
    set.get(name)
        .cloned()
        .ok_or_else(|| anyhow!("{name} is required"))
}

pub(crate) fn parse_bool(value: &str, default: bool) -> bool {
    match value {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => default,
    }
}

pub(crate) fn split_conditions(cond: &str) -> Vec<(String, String)> {
    cond.split('&')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .filter_map(|item| {
            item.split_once('=').map(|(key, value)| {
                (
                    key.trim().to_string(),
                    value
                        .trim()
                        .trim_matches('"')
                        .trim_matches('\'')
                        .to_string(),
                )
            })
        })
        .collect()
}

fn parse_assignments(value: &str) -> Result<BTreeMap<String, String>> {
    let mut set = BTreeMap::new();
    for item in value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let Some((key, val)) = item.split_once('=') else {
            return Err(anyhow!("invalid assignment: {item}"));
        };
        set.insert(key.trim().to_string(), val.trim().to_string());
    }
    if set.is_empty() {
        return Err(anyhow!("set is required"));
    }
    Ok(set)
}
