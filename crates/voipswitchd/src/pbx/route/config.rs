use crate::app::AppState;
use crate::pbx::command_helpers::{
    decoded_set, parse_bool, required_domain, selected_keys, with_seaorm_backend,
};
use crate::pbx::route::model::{InboundRouteConfig, OutboundRouteConfig};
use crate::pbx::vc_config::{VcConfigTableHandler, VcConfigTableRegistry};
use anyhow::{Context, Result, anyhow};
use regex::Regex;
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend, Statement,
    TransactionTrait,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use voipswitch_core::command_service::ConfigCommand;
use voipswitch_core::types::ids::DomainId;
use voipswitch_core::types::time::unix_timestamp_ms;

pub fn register_vc_config_table(registry: &mut VcConfigTableRegistry) {
    registry.register(Arc::new(InboundRouteVcConfigTable));
    registry.register(Arc::new(OutboundRouteVcConfigTable));
}

pub(crate) async fn load_inbound_routes(
    conn: &DatabaseConnection,
    domain_id: &DomainId,
) -> Result<Vec<InboundRouteConfig>> {
    let rows = conn
        .query_all(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT id,name,enabled,trunk_match,dst_pattern,src_pattern,dst_strip,
                    dst_prefix,dst_suffix,src_strip,src_prefix,src_suffix,target,priority
             FROM inbound_route WHERE domain_id=? ORDER BY priority,id",
            vec![domain_id.as_str().into()],
        ))
        .await?;
    rows.into_iter().map(inbound_from_row).collect()
}

pub(crate) async fn load_outbound_routes(
    conn: &DatabaseConnection,
    domain_id: &DomainId,
) -> Result<Vec<OutboundRouteConfig>> {
    let rows = conn
        .query_all(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT id,name,enabled,dst_pattern,src_pattern,dst_strip,dst_prefix,
                    dst_suffix,src_strip,src_prefix,src_suffix,priority
             FROM outbound_route WHERE domain_id=? ORDER BY priority,id",
            vec![domain_id.as_str().into()],
        ))
        .await?;
    let trunk_rows = conn
        .query_all(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT route_id,trunk_ref FROM outbound_route_trunks
             WHERE domain_id=? ORDER BY route_id,position",
            vec![domain_id.as_str().into()],
        ))
        .await?;
    let mut refs = BTreeMap::<i64, Vec<String>>::new();
    for row in trunk_rows {
        refs.entry(row.try_get("", "route_id")?)
            .or_default()
            .push(row.try_get("", "trunk_ref")?);
    }
    rows.into_iter()
        .map(|row| {
            let id: i64 = row.try_get("", "id")?;
            outbound_from_row(row, refs.remove(&id).unwrap_or_default())
        })
        .collect()
}

struct InboundRouteVcConfigTable;

impl VcConfigTableHandler for InboundRouteVcConfigTable {
    fn table(&self) -> &str {
        "inbound_route"
    }

    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<serde_json::Value> {
        let domain_id = required_domain(command)?;
        let rows = state
            .config()
            .snapshot()
            .domains
            .get(&domain_id)
            .map(|domain| domain.inbound_routes.clone())
            .unwrap_or_default();
        match command.action.as_str() {
            "select" => {
                let id = id_condition(command, false)?;
                let keys = selected_keys(
                    command,
                    &[
                        "id",
                        "name",
                        "trunk_match",
                        "dst_pattern",
                        "src_pattern",
                        "dst_strip",
                        "dst_prefix",
                        "dst_suffix",
                        "src_strip",
                        "src_prefix",
                        "src_suffix",
                        "target",
                        "priority",
                        "enabled",
                    ],
                );
                Ok(json!({
                    "table": self.table(),
                    "domain_id": domain_id,
                    "rows": rows.into_iter().filter(|row| id.is_none_or(|id| row.id == id))
                        .map(|row| project_inbound(&domain_id, &row, &keys)).collect::<Vec<_>>(),
                }))
            }
            "count" => {
                let id = id_condition(command, false)?;
                Ok(json!({
                    "table": self.table(),
                    "domain_id": domain_id,
                    "total": rows.iter().filter(|row| id.is_none_or(|id| row.id == id)).count(),
                }))
            }
            "insert" => {
                let set = decoded_set(command)?;
                anyhow::ensure!(
                    !set.contains_key("id"),
                    "id is server-assigned for inbound_route insert"
                );
                let route = inbound_from_set(&set, None)?;
                validate_inbound_references(state, &domain_id, &route)?;
                let id = with_seaorm_backend(state, |backend| {
                    insert_inbound(backend, &domain_id, &route)
                })?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id }))
            }
            "batch_insert" => {
                let routes = decode_batch_sets(command, self.table())?
                    .into_iter()
                    .map(|set| {
                        anyhow::ensure!(
                            !set.contains_key("id"),
                            "id is server-assigned for inbound_route insert"
                        );
                        inbound_from_set(&set, None)
                    })
                    .collect::<Result<Vec<_>>>()?;
                for route in &routes {
                    validate_inbound_references(state, &domain_id, route)?;
                }
                let ids = with_seaorm_backend(state, |backend| {
                    insert_inbound_batch(backend, &domain_id, &routes)
                })?;
                reload(state)?;
                Ok(batch_result(self.table(), &domain_id, ids))
            }
            "update" => {
                let id = id_condition(command, true)?.expect("required id");
                let existing = rows
                    .into_iter()
                    .find(|row| row.id == id)
                    .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: inbound_route id {id}"))?;
                let route = inbound_from_set(&decoded_set(command)?, Some(existing))?;
                validate_inbound_references(state, &domain_id, &route)?;
                with_seaorm_backend(state, |backend| update_inbound(backend, &domain_id, &route))?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id, "updated": 1 }))
            }
            "delete" => {
                let id = id_condition(command, true)?.expect("required id");
                with_seaorm_backend(state, |backend| {
                    delete_route(backend, &domain_id, "inbound_route", id)
                })?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id, "deleted": 1 }))
            }
            action => Err(anyhow!(
                "unsupported vc config action for inbound_route: {action}"
            )),
        }
    }
}

struct OutboundRouteVcConfigTable;

impl VcConfigTableHandler for OutboundRouteVcConfigTable {
    fn table(&self) -> &str {
        "outbound_route"
    }

    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<serde_json::Value> {
        let domain_id = required_domain(command)?;
        let rows = state
            .config()
            .snapshot()
            .domains
            .get(&domain_id)
            .map(|domain| domain.outbound_routes.clone())
            .unwrap_or_default();
        match command.action.as_str() {
            "select" => {
                let id = id_condition(command, false)?;
                let keys = selected_keys(
                    command,
                    &[
                        "id",
                        "name",
                        "dst_pattern",
                        "src_pattern",
                        "dst_strip",
                        "dst_prefix",
                        "dst_suffix",
                        "src_strip",
                        "src_prefix",
                        "src_suffix",
                        "priority",
                        "trunk_refs",
                        "enabled",
                    ],
                );
                Ok(json!({
                    "table": self.table(),
                    "domain_id": domain_id,
                    "rows": rows.into_iter().filter(|row| id.is_none_or(|id| row.id == id))
                        .map(|row| project_outbound(&domain_id, &row, &keys)).collect::<Vec<_>>(),
                }))
            }
            "count" => {
                let id = id_condition(command, false)?;
                Ok(json!({
                    "table": self.table(),
                    "domain_id": domain_id,
                    "total": rows.iter().filter(|row| id.is_none_or(|id| row.id == id)).count(),
                }))
            }
            "insert" => {
                let set = decoded_set(command)?;
                anyhow::ensure!(
                    !set.contains_key("id"),
                    "id is server-assigned for outbound_route insert"
                );
                let route = outbound_from_set(&set, None)?;
                validate_trunk_refs(state, &domain_id, &route.trunk_refs)?;
                let id = with_seaorm_backend(state, |backend| {
                    insert_outbound(backend, &domain_id, &route)
                })?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id }))
            }
            "batch_insert" => {
                let routes = decode_batch_sets(command, self.table())?
                    .into_iter()
                    .map(|set| {
                        anyhow::ensure!(
                            !set.contains_key("id"),
                            "id is server-assigned for outbound_route insert"
                        );
                        outbound_from_set(&set, None)
                    })
                    .collect::<Result<Vec<_>>>()?;
                for route in &routes {
                    validate_trunk_refs(state, &domain_id, &route.trunk_refs)?;
                }
                let ids = with_seaorm_backend(state, |backend| {
                    insert_outbound_batch(backend, &domain_id, &routes)
                })?;
                reload(state)?;
                Ok(batch_result(self.table(), &domain_id, ids))
            }
            "update" => {
                let id = id_condition(command, true)?.expect("required id");
                let existing = rows
                    .into_iter()
                    .find(|row| row.id == id)
                    .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: outbound_route id {id}"))?;
                let route = outbound_from_set(&decoded_set(command)?, Some(existing))?;
                validate_trunk_refs(state, &domain_id, &route.trunk_refs)?;
                with_seaorm_backend(state, |backend| {
                    update_outbound(backend, &domain_id, &route)
                })?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id, "updated": 1 }))
            }
            "delete" => {
                let id = id_condition(command, true)?.expect("required id");
                with_seaorm_backend(state, |backend| {
                    delete_route(backend, &domain_id, "outbound_route", id)
                })?;
                reload(state)?;
                Ok(json!({ "table": self.table(), "domain_id": domain_id, "id": id, "deleted": 1 }))
            }
            action => Err(anyhow!(
                "unsupported vc config action for outbound_route: {action}"
            )),
        }
    }
}

fn inbound_from_set(
    set: &BTreeMap<String, String>,
    existing: Option<InboundRouteConfig>,
) -> Result<InboundRouteConfig> {
    let existing = existing.as_ref();
    let route = InboundRouteConfig {
        id: existing.map(|row| row.id).unwrap_or_default(),
        name: required_or(set, "name", existing.map(|row| row.name.as_str()))?,
        enabled: bool_or(
            set,
            "enabled",
            existing.map(|row| row.enabled).unwrap_or(true),
        ),
        trunk_match: optional_or(
            set,
            "trunk_match",
            existing.map(|row| row.trunk_match.as_str()),
        ),
        dst_pattern: required_or(
            set,
            "dst_pattern",
            existing.map(|row| row.dst_pattern.as_str()),
        )?,
        src_pattern: optional_pattern(
            set,
            "src_pattern",
            existing.and_then(|row| row.src_pattern.as_deref()),
        ),
        dst_strip: u8_or(set, "dst_strip", existing.map(|row| row.dst_strip), 0)?,
        dst_prefix: optional_or(
            set,
            "dst_prefix",
            existing.map(|row| row.dst_prefix.as_str()),
        ),
        dst_suffix: optional_or(
            set,
            "dst_suffix",
            existing.map(|row| row.dst_suffix.as_str()),
        ),
        src_strip: u8_or(set, "src_strip", existing.map(|row| row.src_strip), 0)?,
        src_prefix: optional_or(
            set,
            "src_prefix",
            existing.map(|row| row.src_prefix.as_str()),
        ),
        src_suffix: optional_or(
            set,
            "src_suffix",
            existing.map(|row| row.src_suffix.as_str()),
        ),
        target: required_or(set, "target", existing.map(|row| row.target.as_str()))?,
        priority: priority_or(set, existing.map(|row| row.priority))?,
    };
    validate_route_common(
        &route.name,
        &route.dst_pattern,
        route.src_pattern.as_deref(),
        route.dst_strip,
        &route.dst_prefix,
        &route.dst_suffix,
        route.src_strip,
        &route.src_prefix,
        &route.src_suffix,
        route.priority,
    )?;
    anyhow::ensure!(
        route.target == "rej"
            || route.target == "auto"
            || route
                .target
                .strip_prefix("ext-")
                .is_some_and(|number| !number.is_empty()),
        "target must be rej, auto, or ext-<number>"
    );
    Ok(route)
}

fn outbound_from_set(
    set: &BTreeMap<String, String>,
    existing: Option<OutboundRouteConfig>,
) -> Result<OutboundRouteConfig> {
    let existing = existing.as_ref();
    let route = OutboundRouteConfig {
        id: existing.map(|row| row.id).unwrap_or_default(),
        name: required_or(set, "name", existing.map(|row| row.name.as_str()))?,
        enabled: bool_or(
            set,
            "enabled",
            existing.map(|row| row.enabled).unwrap_or(true),
        ),
        dst_pattern: required_or(
            set,
            "dst_pattern",
            existing.map(|row| row.dst_pattern.as_str()),
        )?,
        src_pattern: optional_pattern(
            set,
            "src_pattern",
            existing.and_then(|row| row.src_pattern.as_deref()),
        ),
        dst_strip: u8_or(set, "dst_strip", existing.map(|row| row.dst_strip), 0)?,
        dst_prefix: optional_or(
            set,
            "dst_prefix",
            existing.map(|row| row.dst_prefix.as_str()),
        ),
        dst_suffix: optional_or(
            set,
            "dst_suffix",
            existing.map(|row| row.dst_suffix.as_str()),
        ),
        src_strip: u8_or(set, "src_strip", existing.map(|row| row.src_strip), 0)?,
        src_prefix: optional_or(
            set,
            "src_prefix",
            existing.map(|row| row.src_prefix.as_str()),
        ),
        src_suffix: optional_or(
            set,
            "src_suffix",
            existing.map(|row| row.src_suffix.as_str()),
        ),
        priority: priority_or(set, existing.map(|row| row.priority))?,
        trunk_refs: set
            .get("trunk_refs")
            .map(|value| split_refs(value))
            .or_else(|| existing.map(|row| row.trunk_refs.clone()))
            .unwrap_or_default(),
    };
    validate_route_common(
        &route.name,
        &route.dst_pattern,
        route.src_pattern.as_deref(),
        route.dst_strip,
        &route.dst_prefix,
        &route.dst_suffix,
        route.src_strip,
        &route.src_prefix,
        &route.src_suffix,
        route.priority,
    )?;
    anyhow::ensure!(
        !route.trunk_refs.is_empty(),
        "outbound route requires trunk_refs"
    );
    Ok(route)
}

#[allow(clippy::too_many_arguments)]
fn validate_route_common(
    name: &str,
    dst_pattern: &str,
    src_pattern: Option<&str>,
    dst_strip: u8,
    dst_prefix: &str,
    dst_suffix: &str,
    src_strip: u8,
    src_prefix: &str,
    src_suffix: &str,
    priority: u16,
) -> Result<()> {
    anyhow::ensure!(!name.is_empty() && name.len() <= 64, "invalid route name");
    compile_pattern(dst_pattern, "dst_pattern")?;
    if let Some(pattern) = src_pattern {
        compile_pattern(pattern, "src_pattern")?;
    }
    anyhow::ensure!(dst_strip <= 32 && src_strip <= 32, "strip must be 0..32");
    anyhow::ensure!(priority <= 10000, "priority must be 0..10000");
    for (field, value) in [
        ("dst_prefix", dst_prefix),
        ("dst_suffix", dst_suffix),
        ("src_prefix", src_prefix),
        ("src_suffix", src_suffix),
    ] {
        anyhow::ensure!(value.len() <= 32, "{field} is too long");
        anyhow::ensure!(
            value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'*' | b'#' | b'F' | b'f')),
            "{field} contains invalid dial characters"
        );
    }
    Ok(())
}

fn compile_pattern(pattern: &str, field: &str) -> Result<()> {
    anyhow::ensure!(!pattern.is_empty(), "{field} is required");
    anyhow::ensure!(pattern.len() <= 512, "{field} is too long");
    Regex::new(&format!(r"\A(?:{pattern})\z")).with_context(|| format!("invalid {field} regex"))?;
    Ok(())
}

fn validate_inbound_references(
    state: &AppState,
    domain_id: &DomainId,
    route: &InboundRouteConfig,
) -> Result<()> {
    if !route.trunk_match.is_empty() {
        validate_trunk_refs(state, domain_id, std::slice::from_ref(&route.trunk_match))?;
    }
    if let Some(number) = route.target.strip_prefix("ext-") {
        let snapshot = state.config().snapshot();
        let extension = snapshot
            .domains
            .get(domain_id)
            .and_then(|domain| {
                domain
                    .extensions
                    .iter()
                    .find(|extension| extension.number == number)
            })
            .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: extension number {number}"))?;
        anyhow::ensure!(extension.enabled, "extension target must be enabled");
    }
    Ok(())
}

fn validate_trunk_refs(state: &AppState, domain_id: &DomainId, refs: &[String]) -> Result<()> {
    let snapshot = state.config().snapshot();
    let domain = snapshot
        .domains
        .get(domain_id)
        .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: domain {domain_id}"))?;
    for trunk_ref in refs {
        if let Some(id) = trunk_ref.strip_prefix("peer:") {
            let id = id.parse::<u64>().context("invalid peer trunk reference")?;
            let trunk = domain
                .peer_trunks
                .iter()
                .find(|trunk| trunk.id == id)
                .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: peer_trunk id {id}"))?;
            anyhow::ensure!(trunk.enabled, "peer_trunk reference must be enabled");
            continue;
        }
        if let Some(value) = trunk_ref.strip_prefix("reg:") {
            let (trunk_id, account_id) = value
                .split_once('/')
                .ok_or_else(|| anyhow!("invalid reg trunk reference"))?;
            let trunk_id = trunk_id.parse::<u64>().context("invalid reg_trunk id")?;
            let account_id = account_id
                .parse::<u64>()
                .context("invalid reg_account id")?;
            let trunk = domain
                .reg_trunks
                .iter()
                .find(|trunk| trunk.id == trunk_id)
                .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: reg_trunk id {trunk_id}"))?;
            let account = domain
                .reg_accounts
                .iter()
                .find(|account| account.id == account_id && account.reg_trunk_id == trunk_id)
                .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: reg_account id {account_id}"))?;
            anyhow::ensure!(
                trunk.enabled && account.enabled,
                "registered trunk reference must be enabled"
            );
            continue;
        }
        return Err(anyhow!(
            "trunk reference must be peer:<id> or reg:<trunk_id>/<account_id>"
        ));
    }
    Ok(())
}

async fn validate_inbound_references_in_db<C>(
    conn: &C,
    domain_id: &DomainId,
    route: &InboundRouteConfig,
) -> Result<()>
where
    C: ConnectionTrait,
{
    if !route.trunk_match.is_empty() {
        validate_trunk_refs_in_db(conn, domain_id, std::slice::from_ref(&route.trunk_match))
            .await?;
    }
    if let Some(number) = route.target.strip_prefix("ext-") {
        let extension = conn
            .query_one(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT enabled FROM extensions WHERE domain_id=? AND number=?",
                vec![domain_id.as_str().into(), number.into()],
            ))
            .await?
            .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: extension number {number}"))?;
        anyhow::ensure!(
            extension.try_get::<bool>("", "enabled")?,
            "extension target must be enabled"
        );
    }
    Ok(())
}

async fn validate_trunk_refs_in_db<C>(conn: &C, domain_id: &DomainId, refs: &[String]) -> Result<()>
where
    C: ConnectionTrait,
{
    for trunk_ref in refs {
        if let Some(id) = trunk_ref.strip_prefix("peer:") {
            let id = id.parse::<i64>().context("invalid peer trunk reference")?;
            let trunk = conn
                .query_one(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "SELECT enabled FROM peer_trunk WHERE domain_id=? AND id=?",
                    vec![domain_id.as_str().into(), id.into()],
                ))
                .await?
                .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: peer_trunk id {id}"))?;
            anyhow::ensure!(
                trunk.try_get::<bool>("", "enabled")?,
                "peer_trunk reference must be enabled"
            );
            continue;
        }
        if let Some(value) = trunk_ref.strip_prefix("reg:") {
            let (trunk_id, account_id) = value
                .split_once('/')
                .ok_or_else(|| anyhow!("invalid reg trunk reference"))?;
            let trunk_id = trunk_id.parse::<i64>().context("invalid reg_trunk id")?;
            let account_id = account_id
                .parse::<i64>()
                .context("invalid reg_account id")?;
            let row = conn
                .query_one(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "SELECT t.enabled AS trunk_enabled, a.enabled AS account_enabled
                     FROM reg_trunk t
                     JOIN reg_account a
                       ON a.domain_id=t.domain_id AND a.reg_trunk_id=t.id
                     WHERE t.domain_id=? AND t.id=? AND a.id=?",
                    vec![
                        domain_id.as_str().into(),
                        trunk_id.into(),
                        account_id.into(),
                    ],
                ))
                .await?
                .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: reg_account id {account_id}"))?;
            anyhow::ensure!(
                row.try_get::<bool>("", "trunk_enabled")?
                    && row.try_get::<bool>("", "account_enabled")?,
                "registered trunk reference must be enabled"
            );
            continue;
        }
        return Err(anyhow!(
            "trunk reference must be peer:<id> or reg:<trunk_id>/<account_id>"
        ));
    }
    Ok(())
}

fn insert_inbound(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    route: &InboundRouteConfig,
) -> Result<u64> {
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        validate_inbound_references_in_db(&txn, domain_id, route).await?;
        let id = allocate_id(&txn, domain_id, "inbound_route").await?;
        write_inbound(&txn, domain_id, route, id, true).await?;
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        u64::try_from(id).context("allocated id is negative")
    })
}

fn insert_inbound_batch(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    routes: &[InboundRouteConfig],
) -> Result<Vec<u64>> {
    anyhow::ensure!(!routes.is_empty(), "batch records must not be empty");
    anyhow::ensure!(
        routes.len() <= 1000,
        "batch records exceeds maximum of 1000"
    );
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let mut ids = Vec::with_capacity(routes.len());
        for route in routes {
            validate_inbound_references_in_db(&txn, domain_id, route).await?;
            let id = allocate_id(&txn, domain_id, "inbound_route").await?;
            write_inbound(&txn, domain_id, route, id, true).await?;
            ids.push(u64::try_from(id).context("allocated id is negative")?);
        }
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(ids)
    })
}

fn update_inbound(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    route: &InboundRouteConfig,
) -> Result<()> {
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        validate_inbound_references_in_db(&txn, domain_id, route).await?;
        write_inbound(&txn, domain_id, route, route.id as i64, false).await?;
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(())
    })
}

async fn write_inbound(
    txn: &DatabaseTransaction,
    domain_id: &DomainId,
    route: &InboundRouteConfig,
    id: i64,
    insert: bool,
) -> Result<()> {
    let now = unix_timestamp_ms() as i64;
    let statement = if insert {
        Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO inbound_route (domain_id,id,name,enabled,trunk_match,dst_pattern,src_pattern,dst_strip,dst_prefix,dst_suffix,src_strip,src_prefix,src_suffix,target,priority,note,created_at,updated_at)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'',?,?)",
            inbound_values(domain_id, route, id, now, true),
        )
    } else {
        Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE inbound_route SET name=?,enabled=?,trunk_match=?,dst_pattern=?,src_pattern=?,dst_strip=?,dst_prefix=?,dst_suffix=?,src_strip=?,src_prefix=?,src_suffix=?,target=?,priority=?,updated_at=? WHERE domain_id=? AND id=?",
            inbound_values(domain_id, route, id, now, false),
        )
    };
    let result = txn.execute(statement).await?;
    anyhow::ensure!(
        insert || result.rows_affected() == 1,
        "RESOURCE_NOT_FOUND: inbound_route id {}",
        route.id
    );
    Ok(())
}

fn inbound_values(
    domain_id: &DomainId,
    route: &InboundRouteConfig,
    id: i64,
    now: i64,
    insert: bool,
) -> Vec<sea_orm::Value> {
    let mut values = Vec::new();
    if insert {
        values.push(domain_id.as_str().into());
        values.push(id.into());
    }
    values.extend([
        route.name.clone().into(),
        route.enabled.into(),
        route.trunk_match.clone().into(),
        route.dst_pattern.clone().into(),
        route.src_pattern.clone().into(),
        (route.dst_strip as i64).into(),
        route.dst_prefix.clone().into(),
        route.dst_suffix.clone().into(),
        (route.src_strip as i64).into(),
        route.src_prefix.clone().into(),
        route.src_suffix.clone().into(),
        route.target.clone().into(),
        (route.priority as i64).into(),
        now.into(),
    ]);
    if insert {
        values.push(now.into());
    } else {
        values.push(domain_id.as_str().into());
        values.push(id.into());
    }
    values
}

fn insert_outbound(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    route: &OutboundRouteConfig,
) -> Result<u64> {
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        validate_trunk_refs_in_db(&txn, domain_id, &route.trunk_refs).await?;
        let id = allocate_id(&txn, domain_id, "outbound_route").await?;
        write_outbound(&txn, domain_id, route, id, true).await?;
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        u64::try_from(id).context("allocated id is negative")
    })
}

fn insert_outbound_batch(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    routes: &[OutboundRouteConfig],
) -> Result<Vec<u64>> {
    anyhow::ensure!(!routes.is_empty(), "batch records must not be empty");
    anyhow::ensure!(
        routes.len() <= 1000,
        "batch records exceeds maximum of 1000"
    );
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let mut ids = Vec::with_capacity(routes.len());
        for route in routes {
            validate_trunk_refs_in_db(&txn, domain_id, &route.trunk_refs).await?;
            let id = allocate_id(&txn, domain_id, "outbound_route").await?;
            write_outbound(&txn, domain_id, route, id, true).await?;
            ids.push(u64::try_from(id).context("allocated id is negative")?);
        }
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(ids)
    })
}

fn update_outbound(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    route: &OutboundRouteConfig,
) -> Result<()> {
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        validate_trunk_refs_in_db(&txn, domain_id, &route.trunk_refs).await?;
        write_outbound(&txn, domain_id, route, route.id as i64, false).await?;
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(())
    })
}

async fn write_outbound(
    txn: &DatabaseTransaction,
    domain_id: &DomainId,
    route: &OutboundRouteConfig,
    id: i64,
    insert: bool,
) -> Result<()> {
    let now = unix_timestamp_ms() as i64;
    let statement = if insert {
        Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO outbound_route (domain_id,id,name,enabled,dst_pattern,src_pattern,dst_strip,dst_prefix,dst_suffix,src_strip,src_prefix,src_suffix,priority,note,created_at,updated_at)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'',?,?)",
            outbound_values(domain_id, route, id, now, true),
        )
    } else {
        Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE outbound_route SET name=?,enabled=?,dst_pattern=?,src_pattern=?,dst_strip=?,dst_prefix=?,dst_suffix=?,src_strip=?,src_prefix=?,src_suffix=?,priority=?,updated_at=? WHERE domain_id=? AND id=?",
            outbound_values(domain_id, route, id, now, false),
        )
    };
    let result = txn.execute(statement).await?;
    anyhow::ensure!(
        insert || result.rows_affected() == 1,
        "RESOURCE_NOT_FOUND: outbound_route id {}",
        route.id
    );
    txn.execute(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "DELETE FROM outbound_route_trunks WHERE domain_id=? AND route_id=?",
        vec![domain_id.as_str().into(), id.into()],
    ))
    .await?;
    for (position, trunk_ref) in route.trunk_refs.iter().enumerate() {
        txn.execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO outbound_route_trunks (domain_id,route_id,trunk_ref,position)
             VALUES (?,?,?,?)",
            vec![
                domain_id.as_str().into(),
                id.into(),
                trunk_ref.clone().into(),
                (position as i64).into(),
            ],
        ))
        .await?;
    }
    Ok(())
}

fn outbound_values(
    domain_id: &DomainId,
    route: &OutboundRouteConfig,
    id: i64,
    now: i64,
    insert: bool,
) -> Vec<sea_orm::Value> {
    let mut values = Vec::new();
    if insert {
        values.push(domain_id.as_str().into());
        values.push(id.into());
    }
    values.extend([
        route.name.clone().into(),
        route.enabled.into(),
        route.dst_pattern.clone().into(),
        route.src_pattern.clone().into(),
        (route.dst_strip as i64).into(),
        route.dst_prefix.clone().into(),
        route.dst_suffix.clone().into(),
        (route.src_strip as i64).into(),
        route.src_prefix.clone().into(),
        route.src_suffix.clone().into(),
        (route.priority as i64).into(),
        now.into(),
    ]);
    if insert {
        values.push(now.into());
    } else {
        values.push(domain_id.as_str().into());
        values.push(id.into());
    }
    values
}

fn delete_route(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    table: &'static str,
    id: u64,
) -> Result<()> {
    backend.block_on(async {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let result = txn
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
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(())
    })
}

async fn allocate_id(
    txn: &DatabaseTransaction,
    domain_id: &DomainId,
    resource_type: &str,
) -> Result<i64> {
    let row = txn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO config_id_sequences (domain_id,resource_type,next_id) VALUES (?,?,2)
             ON CONFLICT(domain_id,resource_type) DO UPDATE SET next_id=config_id_sequences.next_id+1
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
) -> Result<Vec<BTreeMap<String, String>>> {
    let records = command
        .fields
        .get("records")
        .ok_or_else(|| anyhow!("records is required for {table} batch_insert"))?;
    let records: Vec<BTreeMap<String, Value>> =
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
    let value = cond
        .strip_prefix("id=")
        .ok_or_else(|| anyhow!("CONDITION_NOT_ALLOWED: requires cond=id=<id>"))?;
    let id = value.parse::<u64>().context("invalid id")?;
    anyhow::ensure!(id > 0, "invalid id");
    Ok(Some(id))
}

fn required_or(
    set: &BTreeMap<String, String>,
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

fn optional_or(set: &BTreeMap<String, String>, key: &str, existing: Option<&str>) -> String {
    set.get(key)
        .map(String::as_str)
        .or(existing)
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn optional_pattern(
    set: &BTreeMap<String, String>,
    key: &str,
    existing: Option<&str>,
) -> Option<String> {
    let value = optional_or(set, key, existing);
    (!value.is_empty()).then_some(value)
}

fn bool_or(set: &BTreeMap<String, String>, key: &str, default: bool) -> bool {
    set.get(key)
        .map(|value| parse_bool(value, default))
        .unwrap_or(default)
}

fn u8_or(
    set: &BTreeMap<String, String>,
    key: &str,
    existing: Option<u8>,
    default: u8,
) -> Result<u8> {
    Ok(set
        .get(key)
        .map(|value| value.parse().with_context(|| format!("invalid {key}")))
        .transpose()?
        .or(existing)
        .unwrap_or(default))
}

fn priority_or(set: &BTreeMap<String, String>, existing: Option<u16>) -> Result<u16> {
    Ok(set
        .get("priority")
        .map(|value| value.parse().context("invalid priority"))
        .transpose()?
        .or(existing)
        .unwrap_or(100))
}

fn split_refs(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn inbound_from_row(row: sea_orm::QueryResult) -> Result<InboundRouteConfig> {
    Ok(InboundRouteConfig {
        id: positive_id(row.try_get("", "id")?)?,
        name: row.try_get("", "name")?,
        enabled: row.try_get("", "enabled")?,
        trunk_match: row.try_get("", "trunk_match")?,
        dst_pattern: row.try_get("", "dst_pattern")?,
        src_pattern: row.try_get("", "src_pattern")?,
        dst_strip: u8::try_from(row.try_get::<i64>("", "dst_strip")?)
            .context("dst_strip out of range")?,
        dst_prefix: row.try_get("", "dst_prefix")?,
        dst_suffix: row.try_get("", "dst_suffix")?,
        src_strip: u8::try_from(row.try_get::<i64>("", "src_strip")?)
            .context("src_strip out of range")?,
        src_prefix: row.try_get("", "src_prefix")?,
        src_suffix: row.try_get("", "src_suffix")?,
        target: row.try_get("", "target")?,
        priority: u16::try_from(row.try_get::<i64>("", "priority")?)
            .context("priority out of range")?,
    })
}

fn outbound_from_row(
    row: sea_orm::QueryResult,
    trunk_refs: Vec<String>,
) -> Result<OutboundRouteConfig> {
    Ok(OutboundRouteConfig {
        id: positive_id(row.try_get("", "id")?)?,
        name: row.try_get("", "name")?,
        enabled: row.try_get("", "enabled")?,
        dst_pattern: row.try_get("", "dst_pattern")?,
        src_pattern: row.try_get("", "src_pattern")?,
        dst_strip: u8::try_from(row.try_get::<i64>("", "dst_strip")?)
            .context("dst_strip out of range")?,
        dst_prefix: row.try_get("", "dst_prefix")?,
        dst_suffix: row.try_get("", "dst_suffix")?,
        src_strip: u8::try_from(row.try_get::<i64>("", "src_strip")?)
            .context("src_strip out of range")?,
        src_prefix: row.try_get("", "src_prefix")?,
        src_suffix: row.try_get("", "src_suffix")?,
        priority: u16::try_from(row.try_get::<i64>("", "priority")?)
            .context("priority out of range")?,
        trunk_refs,
    })
}

fn positive_id(value: i64) -> Result<u64> {
    let value = u64::try_from(value).context("id is negative")?;
    anyhow::ensure!(value > 0, "id must be positive");
    Ok(value)
}

fn project_inbound(
    domain_id: &DomainId,
    row: &InboundRouteConfig,
    keys: &[String],
) -> serde_json::Value {
    let mut value = serde_json::to_value(row).expect("serialize inbound route");
    project_keys(domain_id, &mut value, keys)
}

fn project_outbound(
    domain_id: &DomainId,
    row: &OutboundRouteConfig,
    keys: &[String],
) -> serde_json::Value {
    let mut value = serde_json::to_value(row).expect("serialize outbound route");
    project_keys(domain_id, &mut value, keys)
}

fn project_keys(
    domain_id: &DomainId,
    value: &mut serde_json::Value,
    keys: &[String],
) -> serde_json::Value {
    let source = value.as_object().expect("route object");
    let mut projected = serde_json::Map::new();
    for key in keys {
        if key == "domain_id" {
            projected.insert(key.clone(), json!(domain_id));
        } else if let Some(value) = source.get(key) {
            projected.insert(key.clone(), value.clone());
        }
    }
    serde_json::Value::Object(projected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_store::seaorm::connect_sqlite;
    use crate::pbx::migration::migrate_domain_db;
    use tempfile::tempdir;

    #[tokio::test]
    async fn trunk_references_must_resolve_enabled_rows_in_same_domain() -> Result<()> {
        let temp = tempdir()?;
        let conn = connect_sqlite(&temp.path().join("domain.db")).await?;
        let domain_a = DomainId::from("domain-a");
        let domain_b = DomainId::from("domain-b");
        migrate_domain_db(&conn, &domain_a).await?;
        migrate_domain_db(&conn, &domain_b).await?;

        insert_peer(&conn, &domain_b, 1, true).await?;
        assert!(
            validate_trunk_refs_in_db(&conn, &domain_a, &["peer:1".to_string()])
                .await
                .is_err()
        );

        insert_peer(&conn, &domain_a, 1, false).await?;
        assert!(
            validate_trunk_refs_in_db(&conn, &domain_a, &["peer:1".to_string()])
                .await
                .is_err()
        );

        conn.execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE peer_trunk SET enabled=1 WHERE domain_id=? AND id=1",
            vec![domain_a.as_str().into()],
        ))
        .await?;
        validate_trunk_refs_in_db(&conn, &domain_a, &["peer:1".to_string()]).await?;
        Ok(())
    }

    async fn insert_peer<C>(conn: &C, domain_id: &DomainId, id: i64, enabled: bool) -> Result<()>
    where
        C: ConnectionTrait,
    {
        conn.execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO peer_trunk
             (domain_id,id,name,server_host,server_port,outbound_proxy_host,
              outbound_proxy_port,transport,keep_alive_seconds,enabled,note,created_at,updated_at)
             VALUES (?,?,?,'127.0.0.1',5060,NULL,NULL,'udp',60,?,'',0,0)",
            vec![
                domain_id.as_str().into(),
                id.into(),
                format!("peer-{id}").into(),
                enabled.into(),
            ],
        ))
        .await?;
        Ok(())
    }
}
