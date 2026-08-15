use crate::app::AppState;
use crate::config_service::{
    CALL_TRACE_ENABLED_KEY, CDR_SPOOL_REJECT_MB_KEY, CDR_SPOOL_RESUME_MB_KEY,
    CDR_SPOOL_WARNING_MB_KEY, LOG_LEVEL_KEY, RECORDING_DIR_KEY, RECORDING_MAX_SIZE_GB_KEY,
    RECORDING_RETENTION_DAYS_KEY, RuntimeConfig, SIP_PORT_KEY, is_log_level,
};
use crate::pbx::command_helpers::{
    decoded_set, selected_keys, split_conditions, with_seaorm_backend,
};
use crate::pbx::domain::config::open_pbx_system_db;
use crate::pbx::global_setting::db as global_setting;
use crate::pbx::vc_config::{VcConfigTableHandler, VcConfigTableRegistry};
use anyhow::{Result, anyhow, ensure};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, Set,
    TransactionTrait,
};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use voipswitch_core::command_service::ConfigCommand;

const SUPPORTED_KEYS: &[&str] = &[
    CALL_TRACE_ENABLED_KEY,
    SIP_PORT_KEY,
    LOG_LEVEL_KEY,
    RECORDING_DIR_KEY,
    RECORDING_RETENTION_DAYS_KEY,
    RECORDING_MAX_SIZE_GB_KEY,
    CDR_SPOOL_WARNING_MB_KEY,
    CDR_SPOOL_REJECT_MB_KEY,
    CDR_SPOOL_RESUME_MB_KEY,
];

#[derive(Debug, Clone, PartialEq, Eq)]
enum NormalizedSetting {
    Bool(bool),
    Integer(i64),
    String(String),
}

impl NormalizedSetting {
    fn stored_value(&self) -> String {
        match self {
            Self::Bool(value) => value.to_string(),
            Self::Integer(value) => value.to_string(),
            Self::String(value) => value.clone(),
        }
    }

    fn value_type(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::Integer(_) => "integer",
            Self::String(_) => "string",
        }
    }

    fn json_value(&self) -> Value {
        match self {
            Self::Bool(value) => json!(value),
            Self::Integer(value) => json!(value),
            Self::String(value) => json!(value),
        }
    }
}

pub(crate) fn register_vc_config_table(registry: &mut VcConfigTableRegistry) {
    registry.register(Arc::new(GlobalSettingVcConfigTable));
}

struct GlobalSettingVcConfigTable;

impl VcConfigTableHandler for GlobalSettingVcConfigTable {
    fn table(&self) -> &str {
        "global_setting"
    }

    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<Value> {
        match command.action.as_str() {
            "select" => select_settings(state, command),
            "update" => update_setting(state, command),
            "insert" | "delete" => Err(anyhow!(
                "global_setting only supports select and update for registered keys"
            )),
            action => Err(anyhow!("unsupported global_setting action: {action}")),
        }
    }
}

fn select_settings(state: &AppState, command: &ConfigCommand) -> Result<Value> {
    let selected = selected_keys(
        command,
        &["key", "value", "value_type", "version", "updated_at"],
    );
    let requested = selected_setting_keys(command)?;
    let rows = with_seaorm_backend(state, |backend| {
        backend.block_on(async {
            let conn = open_pbx_system_db(backend).await?;
            let rows = global_setting::Entity::find()
                .filter(global_setting::Column::Key.is_in(requested))
                .order_by_asc(global_setting::Column::Key)
                .all(&conn)
                .await?;
            Ok(rows)
        })
    })?;
    Ok(json!({
        "table": "global_setting",
        "rows": rows
            .into_iter()
            .map(|row| project_row(row, &selected))
            .collect::<Result<Vec<_>>>()?,
    }))
}

fn update_setting(state: &AppState, command: &ConfigCommand) -> Result<Value> {
    let keys = selected_setting_keys(command)?;
    let set = decoded_set(command)?;
    let updates = requested_updates(&keys, &set)?;
    let normalized = updates
        .iter()
        .map(|(key, value)| Ok((key.clone(), normalize_setting(key, value)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    validate_cdr_spool_limits(&state.config().snapshot(), &normalized)?;
    let was_enabled = state.config().snapshot().call_trace_enabled();

    with_seaorm_backend(state, |backend| {
        backend.block_on(async {
            let conn = open_pbx_system_db(backend).await?;
            let txn = conn.begin().await?;
            let updated_at = unix_timestamp_ms()?;
            for (key, value) in &normalized {
                let row = global_setting::Entity::find_by_id(key.clone())
                    .one(&txn)
                    .await?
                    .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: global setting {key}"))?;
                let mut active = row.into_active_model();
                active.value = Set(value.stored_value());
                active.value_type = Set(value.value_type().to_string());
                active.version = Set(active.version.as_ref().saturating_add(1));
                active.updated_at = Set(updated_at);
                active.update(&txn).await?;
            }
            txn.commit().await?;
            Ok(())
        })
    })?;

    let backend = state.backend();
    let next = backend.load_runtime_config()?;
    if was_enabled && !next.call_trace_enabled() {
        state.mark_active_call_traces_incomplete();
    }
    crate::logging::set_level(next.log_level())?;
    let cdr_spool_limits = next.cdr_spool_limits();
    state.config().replace(next);
    if let Some(writer) = state.cdr_writer() {
        writer.refresh_admission(cdr_spool_limits);
    }
    if was_enabled && !state.config().snapshot().call_trace_enabled() {
        state.mark_active_call_traces_incomplete();
    }

    Ok(json!({
        "table": "global_setting",
        "updated": normalized.len(),
        "settings": normalized
            .iter()
            .map(|(key, value)| (key.clone(), value.json_value()))
            .collect::<Map<_, _>>(),
    }))
}

fn requested_updates(
    selected_keys: &[String],
    set: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    if let Some(value) = set.get("value") {
        ensure!(
            set.len() == 1 && selected_keys.len() == 1,
            "value form requires exactly one selected global setting"
        );
        return Ok(BTreeMap::from([(selected_keys[0].clone(), value.clone())]));
    }

    let supported = SUPPORTED_KEYS.iter().copied().collect::<BTreeSet<_>>();
    let selected = selected_keys
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    ensure!(
        set.keys().all(|key| supported.contains(key.as_str())),
        "global_setting update contains an unsupported key"
    );
    ensure!(
        set.keys().all(|key| selected.contains(key.as_str())),
        "global_setting update key is outside the selected condition"
    );
    Ok(set.clone())
}

fn selected_setting_keys(command: &ConfigCommand) -> Result<Vec<String>> {
    let conditions = command
        .fields
        .get("cond")
        .map(|cond| split_conditions(cond))
        .unwrap_or_default();
    if conditions.is_empty() {
        return Ok(SUPPORTED_KEYS.iter().map(|key| key.to_string()).collect());
    }
    ensure!(
        conditions.iter().all(|(field, _)| field == "key"),
        "global_setting only supports key conditions"
    );
    let supported = SUPPORTED_KEYS.iter().copied().collect::<BTreeSet<_>>();
    let keys = conditions
        .into_iter()
        .map(|(_, value)| value)
        .collect::<BTreeSet<_>>();
    ensure!(
        keys.iter().all(|key| supported.contains(key.as_str())),
        "unsupported global setting key"
    );
    Ok(keys.into_iter().collect())
}

fn project_row(row: global_setting::Model, keys: &[String]) -> Result<Value> {
    let normalized = normalize_setting(&row.key, &row.value)?;
    let mut value = Map::new();
    for key in keys {
        let field = match key.as_str() {
            "key" => json!(&row.key),
            "value" => normalized.json_value(),
            "value_type" => json!(normalized.value_type()),
            "version" => json!(row.version),
            "updated_at" => json!(row.updated_at),
            _ => continue,
        };
        value.insert(key.clone(), field);
    }
    Ok(Value::Object(value))
}

fn normalize_setting(key: &str, value: &str) -> Result<NormalizedSetting> {
    match key {
        CALL_TRACE_ENABLED_KEY => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(NormalizedSetting::Bool(true)),
            "0" | "false" | "no" | "off" => Ok(NormalizedSetting::Bool(false)),
            _ => Err(anyhow!("{CALL_TRACE_ENABLED_KEY} must be a boolean")),
        },
        SIP_PORT_KEY => {
            let port = value
                .trim()
                .parse::<u16>()
                .map_err(|_| anyhow!("{SIP_PORT_KEY} must be an integer in 1..=65535"))?;
            ensure!(port > 0, "{SIP_PORT_KEY} must be an integer in 1..=65535");
            Ok(NormalizedSetting::Integer(i64::from(port)))
        }
        LOG_LEVEL_KEY => {
            let level = value.trim().to_ascii_lowercase();
            ensure!(
                is_log_level(&level),
                "{LOG_LEVEL_KEY} must be error, warn, info, debug or trace"
            );
            Ok(NormalizedSetting::String(level))
        }
        RECORDING_DIR_KEY => {
            let path = value.trim();
            if path.is_empty() {
                return Ok(NormalizedSetting::String(String::new()));
            }
            ensure!(
                Path::new(path).is_absolute(),
                "{RECORDING_DIR_KEY} must be an absolute path"
            );
            std::fs::create_dir_all(path)
                .map_err(|err| anyhow!("cannot create {RECORDING_DIR_KEY}: {err}"))?;
            let metadata = std::fs::metadata(path)
                .map_err(|err| anyhow!("cannot inspect {RECORDING_DIR_KEY}: {err}"))?;
            ensure!(
                metadata.is_dir() && !metadata.permissions().readonly(),
                "{RECORDING_DIR_KEY} must be a writable directory"
            );
            Ok(NormalizedSetting::String(path.to_string()))
        }
        RECORDING_RETENTION_DAYS_KEY
        | RECORDING_MAX_SIZE_GB_KEY
        | CDR_SPOOL_WARNING_MB_KEY
        | CDR_SPOOL_REJECT_MB_KEY
        | CDR_SPOOL_RESUME_MB_KEY => {
            let number = value
                .trim()
                .parse::<u64>()
                .map_err(|_| anyhow!("{key} must be a positive integer"))?;
            ensure!(number > 0, "{key} must be a positive integer");
            Ok(NormalizedSetting::Integer(i64::try_from(number)?))
        }
        _ => Err(anyhow!("unsupported global setting key: {key}")),
    }
}

fn validate_cdr_spool_limits(
    current: &RuntimeConfig,
    updates: &BTreeMap<String, NormalizedSetting>,
) -> Result<()> {
    let current = current.cdr_spool_limits();
    let warning = updated_integer(updates, CDR_SPOOL_WARNING_MB_KEY).unwrap_or(current.warning_mb);
    let reject = updated_integer(updates, CDR_SPOOL_REJECT_MB_KEY).unwrap_or(current.reject_mb);
    let resume = updated_integer(updates, CDR_SPOOL_RESUME_MB_KEY).unwrap_or(current.resume_mb);
    ensure!(
        warning < resume && resume < reject,
        "CDR spool limits must satisfy warning < resume < reject"
    );
    Ok(())
}

fn updated_integer(updates: &BTreeMap<String, NormalizedSetting>, key: &str) -> Option<u64> {
    match updates.get(key) {
        Some(NormalizedSetting::Integer(value)) => u64::try_from(*value).ok(),
        _ => None,
    }
}

fn unix_timestamp_ms() -> Result<i64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| anyhow!("system clock before unix epoch: {err}"))?
        .as_millis();
    i64::try_from(millis).map_err(|_| anyhow!("unix timestamp overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_store::{ConfigBackend, SeaOrmConfigBackend};
    use std::collections::BTreeMap;
    use voipswitch_core::command_service::ConfigCommand;

    #[test]
    fn global_values_are_strictly_normalized() {
        assert_eq!(
            normalize_setting(CALL_TRACE_ENABLED_KEY, "on").unwrap(),
            NormalizedSetting::Bool(true)
        );
        assert_eq!(
            normalize_setting(SIP_PORT_KEY, "15060").unwrap(),
            NormalizedSetting::Integer(15060)
        );
        assert_eq!(
            normalize_setting(LOG_LEVEL_KEY, "DEBUG").unwrap(),
            NormalizedSetting::String("debug".to_string())
        );
        assert_eq!(
            normalize_setting(RECORDING_RETENTION_DAYS_KEY, "30").unwrap(),
            NormalizedSetting::Integer(30)
        );
        assert_eq!(
            normalize_setting(CDR_SPOOL_WARNING_MB_KEY, "512").unwrap(),
            NormalizedSetting::Integer(512)
        );
        assert!(normalize_setting(SIP_PORT_KEY, "0").is_err());
        assert!(normalize_setting(LOG_LEVEL_KEY, "verbose").is_err());
        assert!(normalize_setting(RECORDING_MAX_SIZE_GB_KEY, "0").is_err());
    }

    #[test]
    fn persisted_setting_defaults_on_and_hot_updates_runtime_snapshot() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let backend = Arc::new(SeaOrmConfigBackend::sqlite(temp.path(), "test")?);
        let config = backend.load_runtime_config()?;
        assert!(config.call_trace_enabled());
        let state = AppState::new(config, backend, 1);
        let command = ConfigCommand {
            resource: "vc_config".to_string(),
            action: "update".to_string(),
            domain_id: None,
            key: Some("global_setting".to_string()),
            fields: BTreeMap::from([
                ("table".to_string(), "global_setting".to_string()),
                ("cond".to_string(), "any".to_string()),
                (
                    "set".to_string(),
                    "call_trace_enabled=false,sip_port=15060,log_level=debug,recording_retention_days=31,recording_max_size_gb=21,cdr_spool_warning_mb=256,cdr_spool_resume_mb=400,cdr_spool_reject_mb=512".to_string(),
                ),
            ]),
        };

        update_setting(&state, &command)?;

        assert!(!state.config().snapshot().call_trace_enabled());
        assert_eq!(state.config().snapshot().sip_port(), 15060);
        assert_eq!(state.config().snapshot().log_level(), "debug");
        assert_eq!(state.config().snapshot().recording_retention_days(), 31);
        assert_eq!(state.config().snapshot().recording_max_size_gb(), 21);
        assert_eq!(state.config().snapshot().cdr_spool_limits().warning_mb, 256);
        assert_eq!(state.config().snapshot().cdr_spool_limits().resume_mb, 400);
        assert_eq!(state.config().snapshot().cdr_spool_limits().reject_mb, 512);
        let persisted = state.backend().load_runtime_config()?;
        assert!(!persisted.call_trace_enabled());
        assert_eq!(persisted.sip_port(), 15060);
        assert_eq!(persisted.log_level(), "debug");
        Ok(())
    }

    #[test]
    fn invalid_batch_does_not_persist_any_setting() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let backend = Arc::new(SeaOrmConfigBackend::sqlite(temp.path(), "test")?);
        let state = AppState::new(backend.load_runtime_config()?, backend, 1);
        let command = ConfigCommand {
            resource: "vc_config".to_string(),
            action: "update".to_string(),
            domain_id: None,
            key: Some("global_setting".to_string()),
            fields: BTreeMap::from([
                ("table".to_string(), "global_setting".to_string()),
                ("cond".to_string(), "any".to_string()),
                (
                    "set".to_string(),
                    "sip_port=15060,log_level=verbose".to_string(),
                ),
            ]),
        };

        assert!(update_setting(&state, &command).is_err());
        let persisted = state.backend().load_runtime_config()?;
        assert_eq!(persisted.sip_port(), 5060);
        assert_eq!(persisted.log_level(), "info");
        Ok(())
    }

    #[test]
    fn cdr_spool_watermarks_are_validated_as_one_configuration() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let backend = Arc::new(SeaOrmConfigBackend::sqlite(temp.path(), "test")?);
        let state = AppState::new(backend.load_runtime_config()?, backend, 1);
        let command = ConfigCommand {
            resource: "vc_config".to_string(),
            action: "update".to_string(),
            domain_id: None,
            key: Some("global_setting".to_string()),
            fields: BTreeMap::from([
                ("table".to_string(), "global_setting".to_string()),
                ("cond".to_string(), "any".to_string()),
                (
                    "set".to_string(),
                    "cdr_spool_warning_mb=900,cdr_spool_resume_mb=800,cdr_spool_reject_mb=1024"
                        .to_string(),
                ),
            ]),
        };

        assert!(update_setting(&state, &command).is_err());
        assert_eq!(
            state.backend().load_runtime_config()?.cdr_spool_limits(),
            crate::config_service::CdrSpoolLimits {
                warning_mb: 512,
                reject_mb: 1024,
                resume_mb: 800,
            }
        );
        Ok(())
    }
}
