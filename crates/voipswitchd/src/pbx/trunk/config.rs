use crate::app::AppState;
use crate::pbx::command_helpers::{
    decoded_set, parse_bool, required_domain, selected_keys, with_seaorm_backend,
};
use crate::pbx::trunk::model::{PeerTrunkConfig, RegisterAccountConfig, RegisterTrunkConfig};
use crate::pbx::vc_config::{VcConfigTableHandler, VcConfigTableRegistry};
use anyhow::{Context, Result, anyhow};
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend, Statement,
    TransactionTrait,
};
use serde_json::{Value, json};
use std::sync::Arc;
use voipswitch_core::command_service::ConfigCommand;
use voipswitch_core::types::ids::DomainId;
use voipswitch_core::types::time::unix_timestamp_ms;

pub fn register_vc_config_table(registry: &mut VcConfigTableRegistry) {
    registry.register(Arc::new(PeerTrunkVcConfigTable));
    registry.register(Arc::new(RegTrunkVcConfigTable));
    registry.register(Arc::new(RegAccountVcConfigTable));
}

pub(crate) async fn load_peer_trunks(
    conn: &DatabaseConnection,
    domain_id: &DomainId,
) -> Result<Vec<PeerTrunkConfig>> {
    let rows = conn
        .query_all(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT id, name, server_host, server_port, outbound_proxy_host,
                    outbound_proxy_port, transport, keep_alive_seconds, enabled
             FROM peer_trunk WHERE domain_id=? ORDER BY id",
            vec![domain_id.as_str().into()],
        ))
        .await?;
    rows.into_iter().map(peer_from_row).collect()
}

pub(crate) async fn load_reg_trunks(
    conn: &DatabaseConnection,
    domain_id: &DomainId,
) -> Result<Vec<RegisterTrunkConfig>> {
    let rows = conn
        .query_all(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT id, name, server_host, server_port, outbound_proxy_host,
                    outbound_proxy_port, transport, keep_alive_seconds,
                    requested_expires_seconds, enabled
             FROM reg_trunk WHERE domain_id=? ORDER BY id",
            vec![domain_id.as_str().into()],
        ))
        .await?;
    rows.into_iter().map(reg_trunk_from_row).collect()
}

pub(crate) async fn load_reg_accounts(
    conn: &DatabaseConnection,
    domain_id: &DomainId,
) -> Result<Vec<RegisterAccountConfig>> {
    let rows = conn
        .query_all(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT id, reg_trunk_id, auth_name, auth_pwd, enabled
             FROM reg_account WHERE domain_id=? ORDER BY id",
            vec![domain_id.as_str().into()],
        ))
        .await?;
    rows.into_iter().map(reg_account_from_row).collect()
}

struct PeerTrunkVcConfigTable;

impl VcConfigTableHandler for PeerTrunkVcConfigTable {
    fn table(&self) -> &str {
        "peer_trunk"
    }

    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<serde_json::Value> {
        let domain_id = required_domain(command)?;
        let rows = peer_rows(state, &domain_id);
        match command.action.as_str() {
            "select" => {
                let id = id_condition(command, false)?;
                Ok(json!({
                    "table": self.table(),
                    "domain_id": domain_id,
                    "rows": rows.into_iter().filter(|row| id.is_none_or(|id| row.id == id)).map(|row| project_peer(&domain_id, &row, &selected_keys(command, &["id", "name", "server_host", "server_port", "outbound_proxy_host", "outbound_proxy_port", "transport", "keep_alive_seconds", "enabled"]))).collect::<Vec<_>>(),
                }))
            }
            "count" => {
                let id = id_condition(command, false)?;
                Ok(
                    json!({ "table": self.table(), "domain_id": domain_id, "total": rows.iter().filter(|row| id.is_none_or(|id| row.id == id)).count() }),
                )
            }
            "insert" => {
                let set = decoded_set(command)?;
                anyhow::ensure!(
                    !set.contains_key("id"),
                    "id is server-assigned for peer_trunk insert"
                );
                let peer = peer_from_set(&set, None)?;
                let id =
                    with_seaorm_backend(state, |backend| insert_peer(backend, &domain_id, &peer))?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id }))
            }
            "batch_insert" => {
                let peers = decode_batch_sets(command, self.table())?
                    .into_iter()
                    .map(|set| {
                        anyhow::ensure!(
                            !set.contains_key("id"),
                            "id is server-assigned for peer_trunk insert"
                        );
                        peer_from_set(&set, None)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let ids = with_seaorm_backend(state, |backend| {
                    insert_peer_batch(backend, &domain_id, &peers)
                })?;
                reload(state)?;
                Ok(batch_result(self.table(), &domain_id, ids))
            }
            "update" => {
                let id = id_condition(command, true)?.expect("required id");
                let existing = rows
                    .into_iter()
                    .find(|row| row.id == id)
                    .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: peer_trunk id {id}"))?;
                let peer = peer_from_set(&decoded_set(command)?, Some(existing))?;
                with_seaorm_backend(state, |backend| update_peer(backend, &domain_id, &peer))?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id, "updated": 1 }))
            }
            "delete" => {
                let id = id_condition(command, true)?.expect("required id");
                crate::pbx::recording::config::ensure_target_not_referenced(
                    state,
                    &domain_id,
                    crate::pbx::recording::model::RecordingTargetType::PeerTrunk,
                    id,
                )?;
                with_seaorm_backend(state, |backend| {
                    delete_row(backend, &domain_id, "peer_trunk", id)
                })?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id, "deleted": 1 }))
            }
            action => Err(anyhow!(
                "unsupported vc config action for peer_trunk: {action}"
            )),
        }
    }
}

struct RegTrunkVcConfigTable;

impl VcConfigTableHandler for RegTrunkVcConfigTable {
    fn table(&self) -> &str {
        "reg_trunk"
    }

    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<serde_json::Value> {
        let domain_id = required_domain(command)?;
        let rows = reg_trunk_rows(state, &domain_id);
        match command.action.as_str() {
            "select" => {
                let id = id_condition(command, false)?;
                Ok(json!({
                    "table": self.table(),
                    "domain_id": domain_id,
                    "rows": rows.into_iter().filter(|row| id.is_none_or(|id| row.id == id)).map(|row| project_reg_trunk(&domain_id, &row, &selected_keys(command, &["id", "name", "server_host", "server_port", "outbound_proxy_host", "outbound_proxy_port", "transport", "keep_alive_seconds", "requested_expires_seconds", "enabled"]))).collect::<Vec<_>>(),
                }))
            }
            "count" => {
                let id = id_condition(command, false)?;
                Ok(
                    json!({ "table": self.table(), "domain_id": domain_id, "total": rows.iter().filter(|row| id.is_none_or(|id| row.id == id)).count() }),
                )
            }
            "insert" => {
                let set = decoded_set(command)?;
                anyhow::ensure!(
                    !set.contains_key("id"),
                    "id is server-assigned for reg_trunk insert"
                );
                let trunk = reg_trunk_from_set(&set, None)?;
                let id = with_seaorm_backend(state, |backend| {
                    insert_reg_trunk(backend, &domain_id, &trunk)
                })?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id }))
            }
            "batch_insert" => {
                let trunks = decode_batch_sets(command, self.table())?
                    .into_iter()
                    .map(|set| {
                        anyhow::ensure!(
                            !set.contains_key("id"),
                            "id is server-assigned for reg_trunk insert"
                        );
                        reg_trunk_from_set(&set, None)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let ids = with_seaorm_backend(state, |backend| {
                    insert_reg_trunk_batch(backend, &domain_id, &trunks)
                })?;
                reload(state)?;
                Ok(batch_result(self.table(), &domain_id, ids))
            }
            "update" => {
                let id = id_condition(command, true)?.expect("required id");
                let existing = rows
                    .into_iter()
                    .find(|row| row.id == id)
                    .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: reg_trunk id {id}"))?;
                let trunk = reg_trunk_from_set(&decoded_set(command)?, Some(existing))?;
                with_seaorm_backend(state, |backend| {
                    update_reg_trunk(backend, &domain_id, &trunk)
                })?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id, "updated": 1 }))
            }
            "delete" => {
                let id = id_condition(command, true)?.expect("required id");
                crate::pbx::recording::config::ensure_target_not_referenced(
                    state,
                    &domain_id,
                    crate::pbx::recording::model::RecordingTargetType::RegTrunk,
                    id,
                )?;
                with_seaorm_backend(state, |backend| delete_reg_trunk(backend, &domain_id, id))?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id, "deleted": 1 }))
            }
            action => Err(anyhow!(
                "unsupported vc config action for reg_trunk: {action}"
            )),
        }
    }
}

struct RegAccountVcConfigTable;

impl VcConfigTableHandler for RegAccountVcConfigTable {
    fn table(&self) -> &str {
        "reg_account"
    }

    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<serde_json::Value> {
        let domain_id = required_domain(command)?;
        let rows = reg_account_rows(state, &domain_id);
        match command.action.as_str() {
            "select" => {
                let id = id_condition(command, false)?;
                Ok(json!({
                    "table": self.table(),
                    "domain_id": domain_id,
                    "rows": rows.into_iter().filter(|row| id.is_none_or(|id| row.id == id)).map(|row| project_reg_account(&domain_id, &row, &selected_keys(command, &["id", "reg_trunk_id", "auth_name", "auth_pwd", "enabled"]))).collect::<Vec<_>>(),
                }))
            }
            "count" => {
                let id = id_condition(command, false)?;
                Ok(
                    json!({ "table": self.table(), "domain_id": domain_id, "total": rows.iter().filter(|row| id.is_none_or(|id| row.id == id)).count() }),
                )
            }
            "insert" => {
                let set = decoded_set(command)?;
                anyhow::ensure!(
                    !set.contains_key("id"),
                    "id is server-assigned for reg_account insert"
                );
                let account = reg_account_from_set(&set, None)?;
                let id = with_seaorm_backend(state, |backend| {
                    insert_reg_account(backend, &domain_id, &account)
                })?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id }))
            }
            "batch_insert" => {
                let accounts = decode_batch_sets(command, self.table())?
                    .into_iter()
                    .map(|set| {
                        anyhow::ensure!(
                            !set.contains_key("id"),
                            "id is server-assigned for reg_account insert"
                        );
                        reg_account_from_set(&set, None)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let ids = with_seaorm_backend(state, |backend| {
                    insert_reg_account_batch(backend, &domain_id, &accounts)
                })?;
                reload(state)?;
                Ok(batch_result(self.table(), &domain_id, ids))
            }
            "update" => {
                let id = id_condition(command, true)?.expect("required id");
                let existing = rows
                    .into_iter()
                    .find(|row| row.id == id)
                    .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: reg_account id {id}"))?;
                let account = reg_account_from_set(&decoded_set(command)?, Some(existing))?;
                with_seaorm_backend(state, |backend| {
                    update_reg_account(backend, &domain_id, &account)
                })?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id, "updated": 1 }))
            }
            "delete" => {
                let id = id_condition(command, true)?.expect("required id");
                with_seaorm_backend(state, |backend| {
                    delete_row(backend, &domain_id, "reg_account", id)
                })?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id, "deleted": 1 }))
            }
            action => Err(anyhow!(
                "unsupported vc config action for reg_account: {action}"
            )),
        }
    }
}

fn peer_rows(state: &AppState, domain_id: &DomainId) -> Vec<PeerTrunkConfig> {
    state
        .config()
        .snapshot()
        .domains
        .get(domain_id)
        .map(|domain| domain.peer_trunks.clone())
        .unwrap_or_default()
}

fn reg_trunk_rows(state: &AppState, domain_id: &DomainId) -> Vec<RegisterTrunkConfig> {
    state
        .config()
        .snapshot()
        .domains
        .get(domain_id)
        .map(|domain| domain.reg_trunks.clone())
        .unwrap_or_default()
}

fn reg_account_rows(state: &AppState, domain_id: &DomainId) -> Vec<RegisterAccountConfig> {
    state
        .config()
        .snapshot()
        .domains
        .get(domain_id)
        .map(|domain| domain.reg_accounts.clone())
        .unwrap_or_default()
}

fn peer_from_set(
    set: &std::collections::BTreeMap<String, String>,
    existing: Option<PeerTrunkConfig>,
) -> Result<PeerTrunkConfig> {
    let existing = existing.as_ref();
    let config = PeerTrunkConfig {
        id: existing.map(|row| row.id).unwrap_or_default(),
        name: value_or(set, "name", existing.map(|row| row.name.as_str()))?,
        server_host: value_or(
            set,
            "server_host",
            existing.map(|row| row.server_host.as_str()),
        )?,
        server_port: port_or(
            set,
            "server_port",
            existing.map(|row| row.server_port),
            5060,
        )?,
        outbound_proxy_host: optional_value(
            set,
            "outbound_proxy_host",
            existing.and_then(|row| row.outbound_proxy_host.as_deref()),
        ),
        outbound_proxy_port: optional_port(
            set,
            "outbound_proxy_port",
            existing.and_then(|row| row.outbound_proxy_port),
        )?,
        transport: transport_or(set, existing.map(|row| row.transport.as_str()))?,
        keep_alive_seconds: keep_alive_or(set, existing.map(|row| row.keep_alive_seconds))?,
        enabled: bool_or(
            set,
            "enabled",
            existing.map(|row| row.enabled).unwrap_or(true),
        ),
    };
    validate_peer(&config)?;
    Ok(config)
}

fn reg_trunk_from_set(
    set: &std::collections::BTreeMap<String, String>,
    existing: Option<RegisterTrunkConfig>,
) -> Result<RegisterTrunkConfig> {
    let existing = existing.as_ref();
    let config = RegisterTrunkConfig {
        id: existing.map(|row| row.id).unwrap_or_default(),
        name: value_or(set, "name", existing.map(|row| row.name.as_str()))?,
        server_host: value_or(
            set,
            "server_host",
            existing.map(|row| row.server_host.as_str()),
        )?,
        server_port: port_or(
            set,
            "server_port",
            existing.map(|row| row.server_port),
            5060,
        )?,
        outbound_proxy_host: optional_value(
            set,
            "outbound_proxy_host",
            existing.and_then(|row| row.outbound_proxy_host.as_deref()),
        ),
        outbound_proxy_port: optional_port(
            set,
            "outbound_proxy_port",
            existing.and_then(|row| row.outbound_proxy_port),
        )?,
        transport: transport_or(set, existing.map(|row| row.transport.as_str()))?,
        keep_alive_seconds: keep_alive_or(set, existing.map(|row| row.keep_alive_seconds))?,
        requested_expires_seconds: set
            .get("requested_expires_seconds")
            .map(|value| value.parse().context("invalid requested_expires_seconds"))
            .transpose()?
            .or_else(|| existing.map(|row| row.requested_expires_seconds))
            .unwrap_or(300),
        enabled: bool_or(
            set,
            "enabled",
            existing.map(|row| row.enabled).unwrap_or(true),
        ),
    };
    validate_reg_trunk(&config)?;
    Ok(config)
}

fn reg_account_from_set(
    set: &std::collections::BTreeMap<String, String>,
    existing: Option<RegisterAccountConfig>,
) -> Result<RegisterAccountConfig> {
    let existing = existing.as_ref();
    let config = RegisterAccountConfig {
        id: existing.map(|row| row.id).unwrap_or_default(),
        reg_trunk_id: set
            .get("reg_trunk_id")
            .map(|value| value.parse().context("invalid reg_trunk_id"))
            .transpose()?
            .or_else(|| existing.map(|row| row.reg_trunk_id))
            .ok_or_else(|| anyhow!("reg_trunk_id is required"))?,
        auth_name: value_or(set, "auth_name", existing.map(|row| row.auth_name.as_str()))?,
        auth_pwd: value_or(set, "auth_pwd", existing.map(|row| row.auth_pwd.as_str()))?,
        enabled: bool_or(
            set,
            "enabled",
            existing.map(|row| row.enabled).unwrap_or(true),
        ),
    };
    validate_reg_account(&config)?;
    Ok(config)
}

fn value_or(
    set: &std::collections::BTreeMap<String, String>,
    key: &str,
    existing: Option<&str>,
) -> Result<String> {
    let value = set
        .get(key)
        .map(String::as_str)
        .or(existing)
        .ok_or_else(|| anyhow!("{key} is required"))?
        .trim();
    anyhow::ensure!(!value.is_empty(), "{key} is required");
    Ok(value.to_string())
}

fn optional_value(
    set: &std::collections::BTreeMap<String, String>,
    key: &str,
    existing: Option<&str>,
) -> Option<String> {
    set.get(key)
        .map(String::as_str)
        .or(existing)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn port_or(
    set: &std::collections::BTreeMap<String, String>,
    key: &str,
    existing: Option<u16>,
    default: u16,
) -> Result<u16> {
    Ok(set
        .get(key)
        .map(|value| value.parse().context(format!("invalid {key}")))
        .transpose()?
        .or(existing)
        .unwrap_or(default))
}

fn optional_port(
    set: &std::collections::BTreeMap<String, String>,
    key: &str,
    existing: Option<u16>,
) -> Result<Option<u16>> {
    let Some(value) = set.get(key) else {
        return Ok(existing);
    };
    if value.trim().is_empty() {
        return Ok(None);
    }
    value.parse().map(Some).context(format!("invalid {key}"))
}

fn transport_or(
    set: &std::collections::BTreeMap<String, String>,
    existing: Option<&str>,
) -> Result<String> {
    let value = set
        .get("transport")
        .map(String::as_str)
        .or(existing)
        .unwrap_or("udp")
        .to_ascii_lowercase();
    anyhow::ensure!(
        matches!(value.as_str(), "udp" | "tcp"),
        "transport must be udp or tcp"
    );
    Ok(value)
}

fn keep_alive_or(
    set: &std::collections::BTreeMap<String, String>,
    existing: Option<u32>,
) -> Result<u32> {
    let value = set
        .get("keep_alive_seconds")
        .map(|value| value.parse().context("invalid keep_alive_seconds"))
        .transpose()?
        .or(existing)
        .unwrap_or(60);
    anyhow::ensure!(
        value == 0 || (10..=3600).contains(&value),
        "keep_alive_seconds must be 0 or 10..3600"
    );
    Ok(value)
}

fn bool_or(set: &std::collections::BTreeMap<String, String>, key: &str, default: bool) -> bool {
    set.get(key)
        .map(|value| parse_bool(value, default))
        .unwrap_or(default)
}

fn validate_peer(value: &PeerTrunkConfig) -> Result<()> {
    validate_common(
        &value.name,
        &value.server_host,
        value.server_port,
        value.outbound_proxy_host.as_deref(),
        value.outbound_proxy_port,
    )
}

fn validate_reg_trunk(value: &RegisterTrunkConfig) -> Result<()> {
    validate_common(
        &value.name,
        &value.server_host,
        value.server_port,
        value.outbound_proxy_host.as_deref(),
        value.outbound_proxy_port,
    )?;
    anyhow::ensure!(
        (30..=3600).contains(&value.requested_expires_seconds),
        "requested_expires_seconds must be 30..3600"
    );
    Ok(())
}

fn validate_reg_account(value: &RegisterAccountConfig) -> Result<()> {
    anyhow::ensure!(value.reg_trunk_id > 0, "reg_trunk_id must be positive");
    anyhow::ensure!(
        !value.auth_name.is_empty() && value.auth_name.len() <= 128,
        "invalid auth_name"
    );
    anyhow::ensure!(
        !value.auth_pwd.is_empty() && value.auth_pwd.len() <= 256,
        "invalid auth_pwd"
    );
    Ok(())
}

fn validate_common(
    name: &str,
    server_host: &str,
    server_port: u16,
    proxy_host: Option<&str>,
    proxy_port: Option<u16>,
) -> Result<()> {
    anyhow::ensure!(!name.is_empty() && name.len() <= 64, "invalid name");
    anyhow::ensure!(
        !server_host.is_empty() && server_host.len() <= 255,
        "invalid server_host"
    );
    anyhow::ensure!(server_port > 0, "invalid server_port");
    anyhow::ensure!(
        proxy_host.is_some() || proxy_port.is_none(),
        "outbound_proxy_port requires outbound_proxy_host"
    );
    Ok(())
}

fn insert_peer(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    value: &PeerTrunkConfig,
) -> Result<u64> {
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let id = allocate_id(&txn, domain_id, "peer_trunk").await?;
        insert_peer_statement(&txn, domain_id, id, value).await?;
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        u64::try_from(id).context("allocated id is negative")
    })
}

fn insert_peer_batch(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    values: &[PeerTrunkConfig],
) -> Result<Vec<u64>> {
    anyhow::ensure!(!values.is_empty(), "batch records must not be empty");
    anyhow::ensure!(
        values.len() <= 1000,
        "batch records exceeds maximum of 1000"
    );
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let mut ids = Vec::with_capacity(values.len());
        for value in values {
            let id = allocate_id(&txn, domain_id, "peer_trunk").await?;
            insert_peer_statement(&txn, domain_id, id, value).await?;
            ids.push(u64::try_from(id).context("allocated id is negative")?);
        }
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(ids)
    })
}

async fn insert_peer_statement(
    txn: &DatabaseTransaction,
    domain_id: &DomainId,
    id: i64,
    value: &PeerTrunkConfig,
) -> Result<()> {
    let now = unix_timestamp_ms() as i64;
    txn.execute(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO peer_trunk (domain_id,id,name,server_host,server_port,outbound_proxy_host,outbound_proxy_port,transport,keep_alive_seconds,enabled,note,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,'',?,?)",
        vec![
            domain_id.as_str().into(),
            id.into(),
            value.name.clone().into(),
            value.server_host.clone().into(),
            (value.server_port as i64).into(),
            value.outbound_proxy_host.clone().into(),
            value.outbound_proxy_port.map(i64::from).into(),
            value.transport.clone().into(),
            (value.keep_alive_seconds as i64).into(),
            value.enabled.into(),
            now.into(),
            now.into(),
        ],
    ))
    .await?;
    Ok(())
}

fn update_peer(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    value: &PeerTrunkConfig,
) -> Result<()> {
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        if !value.enabled {
            ensure_trunk_not_referenced(&txn, domain_id, &format!("peer:{}", value.id)).await?;
        }
        let now = unix_timestamp_ms();
        let result = txn.execute(Statement::from_sql_and_values(DbBackend::Sqlite,
            "UPDATE peer_trunk SET name=?,server_host=?,server_port=?,outbound_proxy_host=?,outbound_proxy_port=?,transport=?,keep_alive_seconds=?,enabled=?,updated_at=? WHERE domain_id=? AND id=?",
            vec![value.name.clone().into(), value.server_host.clone().into(), (value.server_port as i64).into(), value.outbound_proxy_host.clone().into(), value.outbound_proxy_port.map(i64::from).into(), value.transport.clone().into(), (value.keep_alive_seconds as i64).into(), value.enabled.into(), (now as i64).into(), domain_id.as_str().into(), (value.id as i64).into()])).await?;
        anyhow::ensure!(result.rows_affected() == 1, "RESOURCE_NOT_FOUND: peer_trunk id {}", value.id);
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(())
    })
}

fn insert_reg_trunk(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    value: &RegisterTrunkConfig,
) -> Result<u64> {
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let id = allocate_id(&txn, domain_id, "reg_trunk").await?;
        insert_reg_trunk_statement(&txn, domain_id, id, value).await?;
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        u64::try_from(id).context("allocated id is negative")
    })
}

fn insert_reg_trunk_batch(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    values: &[RegisterTrunkConfig],
) -> Result<Vec<u64>> {
    anyhow::ensure!(!values.is_empty(), "batch records must not be empty");
    anyhow::ensure!(
        values.len() <= 1000,
        "batch records exceeds maximum of 1000"
    );
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let mut ids = Vec::with_capacity(values.len());
        for value in values {
            let id = allocate_id(&txn, domain_id, "reg_trunk").await?;
            insert_reg_trunk_statement(&txn, domain_id, id, value).await?;
            ids.push(u64::try_from(id).context("allocated id is negative")?);
        }
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(ids)
    })
}

async fn insert_reg_trunk_statement(
    txn: &DatabaseTransaction,
    domain_id: &DomainId,
    id: i64,
    value: &RegisterTrunkConfig,
) -> Result<()> {
    let now = unix_timestamp_ms() as i64;
    txn.execute(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO reg_trunk (domain_id,id,name,server_host,server_port,outbound_proxy_host,outbound_proxy_port,transport,keep_alive_seconds,requested_expires_seconds,enabled,note,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,'',?,?)",
        vec![
            domain_id.as_str().into(),
            id.into(),
            value.name.clone().into(),
            value.server_host.clone().into(),
            (value.server_port as i64).into(),
            value.outbound_proxy_host.clone().into(),
            value.outbound_proxy_port.map(i64::from).into(),
            value.transport.clone().into(),
            (value.keep_alive_seconds as i64).into(),
            (value.requested_expires_seconds as i64).into(),
            value.enabled.into(),
            now.into(),
            now.into(),
        ],
    ))
    .await?;
    Ok(())
}

fn update_reg_trunk(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    value: &RegisterTrunkConfig,
) -> Result<()> {
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        if !value.enabled {
            let accounts = txn
                .query_all(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "SELECT id FROM reg_account WHERE domain_id=? AND reg_trunk_id=?",
                    vec![domain_id.as_str().into(), (value.id as i64).into()],
                ))
                .await?;
            for account in accounts {
                let account_id: i64 = account.try_get("", "id")?;
                ensure_trunk_not_referenced(
                    &txn,
                    domain_id,
                    &format!("reg:{}/{}", value.id, account_id),
                )
                .await?;
            }
        }
        let now = unix_timestamp_ms();
        let result = txn.execute(Statement::from_sql_and_values(DbBackend::Sqlite,
            "UPDATE reg_trunk SET name=?,server_host=?,server_port=?,outbound_proxy_host=?,outbound_proxy_port=?,transport=?,keep_alive_seconds=?,requested_expires_seconds=?,enabled=?,updated_at=? WHERE domain_id=? AND id=?",
            vec![value.name.clone().into(), value.server_host.clone().into(), (value.server_port as i64).into(), value.outbound_proxy_host.clone().into(), value.outbound_proxy_port.map(i64::from).into(), value.transport.clone().into(), (value.keep_alive_seconds as i64).into(), (value.requested_expires_seconds as i64).into(), value.enabled.into(), (now as i64).into(), domain_id.as_str().into(), (value.id as i64).into()])).await?;
        anyhow::ensure!(result.rows_affected() == 1, "RESOURCE_NOT_FOUND: reg_trunk id {}", value.id);
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(())
    })
}

fn insert_reg_account(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    value: &RegisterAccountConfig,
) -> Result<u64> {
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        ensure_reg_trunk_enabled(&txn, domain_id, value.reg_trunk_id).await?;
        let id = allocate_id(&txn, domain_id, "reg_account").await?;
        insert_reg_account_statement(&txn, domain_id, id, value).await?;
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        u64::try_from(id).context("allocated id is negative")
    })
}

fn insert_reg_account_batch(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    values: &[RegisterAccountConfig],
) -> Result<Vec<u64>> {
    anyhow::ensure!(!values.is_empty(), "batch records must not be empty");
    anyhow::ensure!(
        values.len() <= 1000,
        "batch records exceeds maximum of 1000"
    );
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let mut ids = Vec::with_capacity(values.len());
        for value in values {
            ensure_reg_trunk_enabled(&txn, domain_id, value.reg_trunk_id).await?;
            let id = allocate_id(&txn, domain_id, "reg_account").await?;
            insert_reg_account_statement(&txn, domain_id, id, value).await?;
            ids.push(u64::try_from(id).context("allocated id is negative")?);
        }
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(ids)
    })
}

async fn ensure_reg_trunk_enabled(
    conn: &impl ConnectionTrait,
    domain_id: &DomainId,
    reg_trunk_id: u64,
) -> Result<()> {
    let parent = conn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT enabled FROM reg_trunk WHERE domain_id=? AND id=?",
            vec![domain_id.as_str().into(), (reg_trunk_id as i64).into()],
        ))
        .await?
        .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: reg_trunk id {reg_trunk_id}"))?;
    anyhow::ensure!(
        parent.try_get::<bool>("", "enabled")?,
        "reg_trunk must be enabled"
    );
    Ok(())
}

async fn insert_reg_account_statement(
    txn: &DatabaseTransaction,
    domain_id: &DomainId,
    id: i64,
    value: &RegisterAccountConfig,
) -> Result<()> {
    let now = unix_timestamp_ms() as i64;
    txn.execute(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO reg_account (domain_id,id,reg_trunk_id,auth_name,auth_pwd,enabled,note,created_at,updated_at) VALUES (?,?,?,?,?,?,'',?,?)",
        vec![
            domain_id.as_str().into(),
            id.into(),
            (value.reg_trunk_id as i64).into(),
            value.auth_name.clone().into(),
            value.auth_pwd.clone().into(),
            value.enabled.into(),
            now.into(),
            now.into(),
        ],
    ))
    .await?;
    Ok(())
}

fn update_reg_account(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    value: &RegisterAccountConfig,
) -> Result<()> {
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let parent = txn.query_one(Statement::from_sql_and_values(DbBackend::Sqlite, "SELECT enabled FROM reg_trunk WHERE domain_id=? AND id=?", vec![domain_id.as_str().into(), (value.reg_trunk_id as i64).into()])).await?
            .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: reg_trunk id {}", value.reg_trunk_id))?;
        anyhow::ensure!(!value.enabled || parent.try_get::<bool>("", "enabled")?, "reg_trunk must be enabled");
        if !value.enabled {
            ensure_trunk_not_referenced(
                &txn,
                domain_id,
                &format!("reg:{}/{}", value.reg_trunk_id, value.id),
            )
            .await?;
        }
        let now = unix_timestamp_ms();
        let result = txn.execute(Statement::from_sql_and_values(DbBackend::Sqlite,
            "UPDATE reg_account SET reg_trunk_id=?,auth_name=?,auth_pwd=?,enabled=?,updated_at=? WHERE domain_id=? AND id=?",
            vec![(value.reg_trunk_id as i64).into(), value.auth_name.clone().into(), value.auth_pwd.clone().into(), value.enabled.into(), (now as i64).into(), domain_id.as_str().into(), (value.id as i64).into()])).await?;
        anyhow::ensure!(result.rows_affected() == 1, "RESOURCE_NOT_FOUND: reg_account id {}", value.id);
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(())
    })
}

fn delete_reg_trunk(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    id: u64,
) -> Result<()> {
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let accounts = txn
            .query_one(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT COUNT(*) AS count FROM reg_account WHERE domain_id=? AND reg_trunk_id=?",
                vec![domain_id.as_str().into(), (id as i64).into()],
            ))
            .await?
            .expect("count row");
        anyhow::ensure!(
            accounts.try_get::<i64>("", "count")? == 0,
            "RESOURCE_IN_USE: reg_trunk id {id} has reg_account records"
        );
        delete_row_async(&txn, domain_id, "reg_trunk", id).await?;
        txn.commit().await?;
        Ok(())
    })
}

fn delete_row(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    table: &'static str,
    id: u64,
) -> Result<()> {
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        if table == "peer_trunk" {
            ensure_trunk_not_referenced(&txn, domain_id, &format!("peer:{id}")).await?;
        } else if table == "reg_account" {
            let row = txn
                .query_one(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "SELECT reg_trunk_id FROM reg_account WHERE domain_id=? AND id=?",
                    vec![domain_id.as_str().into(), (id as i64).into()],
                ))
                .await?
                .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: reg_account id {id}"))?;
            let reg_trunk_id: i64 = row.try_get("", "reg_trunk_id")?;
            ensure_trunk_not_referenced(&txn, domain_id, &format!("reg:{reg_trunk_id}/{id}"))
                .await?;
        }
        delete_row_async(&txn, domain_id, table, id).await?;
        txn.commit().await?;
        Ok(())
    })
}

async fn ensure_trunk_not_referenced<C>(
    conn: &C,
    domain_id: &DomainId,
    trunk_ref: &str,
) -> Result<()>
where
    C: ConnectionTrait,
{
    if let Some(row) = conn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT route_id FROM outbound_route_trunks
             WHERE domain_id=? AND trunk_ref=? LIMIT 1",
            vec![domain_id.as_str().into(), trunk_ref.into()],
        ))
        .await?
    {
        let route_id: i64 = row.try_get("", "route_id")?;
        return Err(anyhow!(
            "RESOURCE_IN_USE: trunk {trunk_ref} referenced by outbound_route id {route_id}"
        ));
    }
    if let Some(row) = conn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT id FROM inbound_route WHERE domain_id=? AND trunk_match=? LIMIT 1",
            vec![domain_id.as_str().into(), trunk_ref.into()],
        ))
        .await?
    {
        let route_id: i64 = row.try_get("", "id")?;
        return Err(anyhow!(
            "RESOURCE_IN_USE: trunk {trunk_ref} referenced by inbound_route id {route_id}"
        ));
    }
    Ok(())
}

async fn delete_row_async<C>(conn: &C, domain_id: &DomainId, table: &str, id: u64) -> Result<()>
where
    C: ConnectionTrait,
{
    let result = conn
        .execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            format!("DELETE FROM {table} WHERE domain_id=? AND id=?"),
            vec![domain_id.as_str().into(), (id as i64).into()],
        ))
        .await?;
    anyhow::ensure!(
        result.rows_affected() == 1,
        "RESOURCE_NOT_FOUND: {table} id {id}"
    );
    crate::pbx::domain::config::bump_domain_config_version(conn).await?;
    Ok(())
}

async fn allocate_id(
    txn: &DatabaseTransaction,
    domain_id: &DomainId,
    resource_type: &str,
) -> Result<i64> {
    let row = txn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO config_id_sequences (domain_id, resource_type, next_id) VALUES (?, ?, 2)
         ON CONFLICT(domain_id, resource_type) DO UPDATE SET next_id=config_id_sequences.next_id+1
         RETURNING next_id-1 AS allocated_id",
            vec![domain_id.as_str().into(), resource_type.into()],
        ))
        .await?
        .ok_or_else(|| anyhow!("allocate {resource_type} id returned no row"))?;
    row.try_get("", "allocated_id").context("read allocated id")
}

fn decode_batch_sets(
    command: &ConfigCommand,
    table: &str,
) -> Result<Vec<std::collections::BTreeMap<String, String>>> {
    let records = command
        .fields
        .get("records")
        .ok_or_else(|| anyhow!("records is required for {table} batch_insert"))?;
    let records: Vec<std::collections::BTreeMap<String, Value>> =
        serde_json::from_str(records).context("records must be a JSON array of objects")?;
    anyhow::ensure!(!records.is_empty(), "batch records must not be empty");
    anyhow::ensure!(
        records.len() <= 1000,
        "batch records exceeds maximum of 1000"
    );
    records
        .into_iter()
        .map(|record| {
            record
                .into_iter()
                .map(|(key, value)| {
                    let value = match value {
                        Value::String(value) => value,
                        Value::Bool(value) => value.to_string(),
                        Value::Number(value) => value.to_string(),
                        Value::Null => String::new(),
                        _ => return Err(anyhow!("batch field {key} must be a scalar value")),
                    };
                    Ok((key, value))
                })
                .collect()
        })
        .collect()
}

fn batch_result(table: &str, domain_id: &DomainId, ids: Vec<u64>) -> Value {
    json!({
        "table": table,
        "domain_id": domain_id,
        "items": ids
            .into_iter()
            .enumerate()
            .map(|(input_index, id)| json!({ "input_index": input_index, "id": id }))
            .collect::<Vec<_>>(),
    })
}

fn reload(state: &AppState) -> Result<()> {
    let backend = state.backend();
    state.config().replace(backend.load_runtime_config()?);
    Ok(())
}

fn id_condition(command: &ConfigCommand, required: bool) -> Result<Option<u64>> {
    let cond = command
        .fields
        .get("cond")
        .map(String::as_str)
        .unwrap_or("any");
    if matches!(cond, "" | "any") {
        return if required {
            Err(anyhow!("CONDITION_NOT_ALLOWED: requires cond=id=<id>"))
        } else {
            Ok(None)
        };
    }
    let Some(value) = cond.strip_prefix("id=") else {
        return Err(anyhow!("CONDITION_NOT_ALLOWED: requires cond=id=<id>"));
    };
    let id = value.parse::<u64>().context("invalid id")?;
    anyhow::ensure!(id > 0, "invalid id");
    Ok(Some(id))
}

fn project_peer(domain_id: &DomainId, row: &PeerTrunkConfig, keys: &[String]) -> serde_json::Value {
    let mut data = serde_json::Map::new();
    for key in keys {
        match key.as_str() {
            "domain_id" => {
                data.insert(key.clone(), json!(domain_id));
            }
            "id" => {
                data.insert(key.clone(), json!(row.id));
            }
            "name" => {
                data.insert(key.clone(), json!(row.name));
            }
            "server_host" => {
                data.insert(key.clone(), json!(row.server_host));
            }
            "server_port" => {
                data.insert(key.clone(), json!(row.server_port));
            }
            "outbound_proxy_host" => {
                data.insert(key.clone(), json!(row.outbound_proxy_host));
            }
            "outbound_proxy_port" => {
                data.insert(key.clone(), json!(row.outbound_proxy_port));
            }
            "transport" => {
                data.insert(key.clone(), json!(row.transport));
            }
            "keep_alive_seconds" => {
                data.insert(key.clone(), json!(row.keep_alive_seconds));
            }
            "enabled" => {
                data.insert(key.clone(), json!(row.enabled));
            }
            _ => {}
        }
    }
    serde_json::Value::Object(data)
}

fn project_reg_trunk(
    domain_id: &DomainId,
    row: &RegisterTrunkConfig,
    keys: &[String],
) -> serde_json::Value {
    let mut value = project_peer(
        domain_id,
        &PeerTrunkConfig {
            id: row.id,
            name: row.name.clone(),
            server_host: row.server_host.clone(),
            server_port: row.server_port,
            outbound_proxy_host: row.outbound_proxy_host.clone(),
            outbound_proxy_port: row.outbound_proxy_port,
            transport: row.transport.clone(),
            keep_alive_seconds: row.keep_alive_seconds,
            enabled: row.enabled,
        },
        keys,
    );
    if keys.iter().any(|key| key == "requested_expires_seconds") {
        value.as_object_mut().expect("object").insert(
            "requested_expires_seconds".to_string(),
            json!(row.requested_expires_seconds),
        );
    }
    value
}

fn project_reg_account(
    domain_id: &DomainId,
    row: &RegisterAccountConfig,
    keys: &[String],
) -> serde_json::Value {
    let mut data = serde_json::Map::new();
    for key in keys {
        match key.as_str() {
            "domain_id" => {
                data.insert(key.clone(), json!(domain_id));
            }
            "id" => {
                data.insert(key.clone(), json!(row.id));
            }
            "reg_trunk_id" => {
                data.insert(key.clone(), json!(row.reg_trunk_id));
            }
            "auth_name" => {
                data.insert(key.clone(), json!(row.auth_name));
            }
            "auth_pwd" => {
                data.insert(key.clone(), json!(row.auth_pwd));
            }
            "enabled" => {
                data.insert(key.clone(), json!(row.enabled));
            }
            _ => {}
        }
    }
    serde_json::Value::Object(data)
}

fn peer_from_row(row: sea_orm::QueryResult) -> Result<PeerTrunkConfig> {
    Ok(PeerTrunkConfig {
        id: positive_id(row.try_get("", "id")?)?,
        name: row.try_get("", "name")?,
        server_host: row.try_get("", "server_host")?,
        server_port: port_from_row(&row, "server_port")?,
        outbound_proxy_host: row.try_get("", "outbound_proxy_host")?,
        outbound_proxy_port: optional_port_from_row(&row, "outbound_proxy_port")?,
        transport: row.try_get("", "transport")?,
        keep_alive_seconds: u32::try_from(row.try_get::<i64>("", "keep_alive_seconds")?)
            .context("keep_alive_seconds is negative")?,
        enabled: row.try_get("", "enabled")?,
    })
}

fn reg_trunk_from_row(row: sea_orm::QueryResult) -> Result<RegisterTrunkConfig> {
    Ok(RegisterTrunkConfig {
        id: positive_id(row.try_get("", "id")?)?,
        name: row.try_get("", "name")?,
        server_host: row.try_get("", "server_host")?,
        server_port: port_from_row(&row, "server_port")?,
        outbound_proxy_host: row.try_get("", "outbound_proxy_host")?,
        outbound_proxy_port: optional_port_from_row(&row, "outbound_proxy_port")?,
        transport: row.try_get("", "transport")?,
        keep_alive_seconds: u32::try_from(row.try_get::<i64>("", "keep_alive_seconds")?)
            .context("keep_alive_seconds is negative")?,
        requested_expires_seconds: u32::try_from(
            row.try_get::<i64>("", "requested_expires_seconds")?,
        )
        .context("requested_expires_seconds is negative")?,
        enabled: row.try_get("", "enabled")?,
    })
}

fn reg_account_from_row(row: sea_orm::QueryResult) -> Result<RegisterAccountConfig> {
    Ok(RegisterAccountConfig {
        id: positive_id(row.try_get("", "id")?)?,
        reg_trunk_id: positive_id(row.try_get("", "reg_trunk_id")?)?,
        auth_name: row.try_get("", "auth_name")?,
        auth_pwd: row.try_get("", "auth_pwd")?,
        enabled: row.try_get("", "enabled")?,
    })
}

fn positive_id(value: i64) -> Result<u64> {
    let value = u64::try_from(value).context("id is negative")?;
    anyhow::ensure!(value > 0, "id must be positive");
    Ok(value)
}

fn port_from_row(row: &sea_orm::QueryResult, column: &str) -> Result<u16> {
    u16::try_from(row.try_get::<i64>("", column)?).context("port out of range")
}

fn optional_port_from_row(row: &sea_orm::QueryResult, column: &str) -> Result<Option<u16>> {
    row.try_get::<Option<i64>>("", column)?
        .map(|value| u16::try_from(value).context("port out of range"))
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_empty_optional_port_clears_existing_value() {
        let mut set = std::collections::BTreeMap::new();
        set.insert("outbound_proxy_port".to_string(), String::new());

        assert_eq!(
            optional_port(&set, "outbound_proxy_port", Some(5080)).unwrap(),
            None
        );
    }

    #[test]
    fn missing_optional_port_preserves_existing_value() {
        assert_eq!(
            optional_port(
                &std::collections::BTreeMap::new(),
                "outbound_proxy_port",
                Some(5080)
            )
            .unwrap(),
            Some(5080)
        );
    }

    #[test]
    fn reg_account_projection_returns_configured_password() {
        let row = RegisterAccountConfig {
            id: 4,
            reg_trunk_id: 3,
            auth_name: "account".to_string(),
            auth_pwd: "configured-password".to_string(),
            enabled: true,
        };

        let value =
            project_reg_account(&DomainId::from("domain-a"), &row, &["auth_pwd".to_string()]);

        assert_eq!(value["auth_pwd"], "configured-password");
    }
}
