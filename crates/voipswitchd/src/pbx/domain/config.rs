use crate::app::AppState;
use crate::config_service::{DomainRuntimeConfig, GlobalValue, RuntimeConfig, SystemConfig};
use crate::data_store::seaorm::{SeaOrmConfigBackend, connect_sqlite};
use crate::pbx::command_helpers::{
    apply_page, decoded_set, page_request, pagination_value, parse_bool, required_set,
    selected_keys, split_conditions, with_seaorm_backend,
};
use crate::pbx::domain::db::domain_record;
use crate::pbx::domain::model::DomainUpsert;
use crate::pbx::extension::db::extension_record;
use crate::pbx::global_setting::db as global_setting;
use crate::pbx::migration::{migrate_domain_db, migrate_system_db};
use crate::pbx::vc_config::{VcConfigTableHandler, VcConfigTableRegistry};
use anyhow::{Context, Result, anyhow};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait,
    IntoActiveModel, PaginatorTrait, QueryFilter, QueryOrder, Set, Statement,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use voipswitch_core::command_service::ConfigCommand;
use voipswitch_core::types::ids::DomainId;

#[derive(Debug, Clone)]
struct DomainListRow {
    domain_id: DomainId,
    name: String,
    realm: String,
    password: String,
    remark: String,
    enabled: bool,
    extension_count: u64,
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct DomainConfigService {
    state: AppState,
}

#[allow(dead_code)]
impl DomainConfigService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    fn rows(&self) -> Vec<DomainRuntimeConfig> {
        self.state
            .config()
            .snapshot()
            .domains
            .values()
            .map(|domain| domain.as_ref().clone())
            .collect()
    }

    fn row(&self, domain_id: &DomainId) -> Option<DomainRuntimeConfig> {
        self.state
            .config()
            .snapshot()
            .domains
            .get(domain_id)
            .map(|domain| domain.as_ref().clone())
    }

    fn insert_or_update(&self, mut domain: DomainUpsert) -> Result<DomainRuntimeConfig> {
        if domain.domain_id.is_none() {
            domain.domain_id = Some(self.allocate_domain_id());
        }
        let requested_id = domain.domain_id.clone();
        with_seaorm_backend(&self.state, |backend| upsert_domain(backend, &domain))?;
        self.reload()?;
        let Some(domain_id) = requested_id else {
            return Err(anyhow!("domain id was not allocated"));
        };
        self.row(&domain_id)
            .ok_or_else(|| anyhow!("domain not found after reload: {domain_id}"))
    }

    fn delete_row(&self, domain_id: &DomainId) -> Result<()> {
        with_seaorm_backend(&self.state, |backend| delete_domain(backend, domain_id))?;
        self.reload()
    }

    fn reload(&self) -> Result<()> {
        let backend = self.state.backend();
        self.state.config().replace(backend.load_runtime_config()?);
        Ok(())
    }

    fn allocate_domain_id(&self) -> DomainId {
        let existing = self
            .rows()
            .into_iter()
            .map(|domain| domain.domain_id)
            .collect::<std::collections::BTreeSet<_>>();
        let base = format!("domain-{}", unix_timestamp_ms());
        if !existing.contains(&DomainId::from(base.clone())) {
            return DomainId::from(base);
        }
        for index in 1.. {
            let candidate = format!("{base}-{index}");
            if !existing.contains(&DomainId::from(candidate.clone())) {
                return DomainId::from(candidate);
            }
        }
        unreachable!("infinite domain id allocation loop")
    }
}

pub(crate) fn load_runtime_config(backend: &SeaOrmConfigBackend) -> Result<RuntimeConfig> {
    backend.block_on(async {
        let conn = open_pbx_system_db(backend).await?;
        let globals = load_globals(&conn).await?;
        let domains = domain_record::Entity::find()
            .order_by_asc(domain_record::Column::Id)
            .all(&conn)
            .await?;
        let mut runtime_domains = BTreeMap::new();
        let mut max_version = 1;

        for row in domains {
            let domain_db = resolve_domain_db_path(backend.data_dir(), &row.db_path);
            let domain = match load_domain_config(backend, &domain_db, row).await {
                Ok(domain) => domain,
                Err(err) => {
                    tracing::error!(error = %err, path = %domain_db.display(), "failed to load domain config");
                    continue;
                }
            };
            max_version = max_version.max(domain.version);
            runtime_domains.insert(domain.domain_id.clone(), Arc::new(domain));
        }

        let data_dir = std::fs::canonicalize(backend.data_dir())
            .unwrap_or_else(|_| backend.data_dir().to_path_buf());
        Ok(RuntimeConfig {
            system: SystemConfig {
                instance_id: backend.instance_id().to_string(),
                data_dir: data_dir.to_string_lossy().into_owned(),
            },
            globals,
            domains: runtime_domains,
            version: max_version,
        })
    })
}

pub(crate) async fn open_domain_db(
    backend: &SeaOrmConfigBackend,
    domain_id: &DomainId,
) -> Result<(DatabaseConnection, PathBuf)> {
    let system = open_pbx_system_db(backend).await?;
    let row = domain_record::Entity::find_by_id(domain_id.as_str().to_string())
        .one(&system)
        .await?
        .with_context(|| format!("domain not found: {domain_id}"))?;
    let domain_db_path = resolve_domain_db_path(backend.data_dir(), &row.db_path);
    let conn = connect_sqlite(&domain_db_path).await?;
    migrate_domain_db(&conn, domain_id).await?;
    Ok((conn, domain_db_path))
}

pub(crate) async fn open_pbx_system_db(
    backend: &SeaOrmConfigBackend,
) -> Result<DatabaseConnection> {
    let conn = backend.open_system_db().await?;
    migrate_system_db(&conn).await?;
    Ok(conn)
}

pub(crate) async fn load_domain_config_version<C>(conn: &C) -> Result<u64>
where
    C: ConnectionTrait,
{
    let Some(row) = conn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT value FROM config_meta WHERE key='version'",
            Vec::new(),
        ))
        .await?
    else {
        return Ok(1);
    };
    let value: String = row.try_get("", "value")?;
    Ok(value.parse::<u64>().unwrap_or(1).max(1))
}

pub(crate) async fn bump_domain_config_version<C>(conn: &C) -> Result<u64>
where
    C: ConnectionTrait,
{
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        "INSERT OR IGNORE INTO config_meta (key, value) VALUES ('version', '1')".to_string(),
    ))
    .await?;
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        "UPDATE config_meta SET value = CAST(value AS INTEGER) + 1 WHERE key='version'".to_string(),
    ))
    .await?;
    load_domain_config_version(conn).await
}

fn upsert_domain(backend: &SeaOrmConfigBackend, domain: &DomainUpsert) -> Result<()> {
    backend.block_on(async {
        let conn = open_pbx_system_db(backend).await?;
        validate_domain_name(&domain.name)?;
        validate_domain_realm(&domain.realm)?;
        let domain_id = domain
            .domain_id
            .clone()
            .unwrap_or_else(|| next_domain_id(backend.instance_id()));
        validate_domain_id(domain_id.as_str())?;
        ensure_domain_identity_available(&conn, &domain_id, &domain.name, &domain.realm).await?;
        let db_path = domain_db_path(domain_id.as_str());
        let status = if domain.enabled {
            "enabled"
        } else {
            "disabled"
        }
        .to_string();
        let now = unix_timestamp_ms();

        if let Some(existing) = domain_record::Entity::find_by_id(domain_id.as_str().to_string())
            .one(&conn)
            .await?
        {
            let next_version = existing.version + 1;
            let mut active = existing.into_active_model();
            active.name = Set(domain.name.clone());
            active.realm = Set(domain.realm.clone());
            active.password = Set(domain.password.clone());
            active.remark = Set(domain.remark.clone());
            active.status = Set(status);
            active.version = Set(next_version);
            active.updated_at = Set(now);
            active.update(&conn).await?;
        } else {
            domain_record::Entity::insert(domain_record::ActiveModel {
                id: Set(domain_id.as_str().to_string()),
                name: Set(domain.name.clone()),
                realm: Set(domain.realm.clone()),
                password: Set(domain.password.clone()),
                remark: Set(domain.remark.clone()),
                status: Set(status),
                db_path: Set(db_path.clone()),
                version: Set(1),
                updated_at: Set(now),
            })
            .exec_without_returning(&conn)
            .await?;
        }

        let full_domain_db_path = resolve_domain_db_path(backend.data_dir(), &db_path);
        let domain_conn = connect_sqlite(&full_domain_db_path).await?;
        migrate_domain_db(&domain_conn, &domain_id).await?;
        Ok(())
    })
}

async fn ensure_domain_identity_available(
    conn: &DatabaseConnection,
    domain_id: &DomainId,
    name: &str,
    realm: &str,
) -> Result<()> {
    let name_owner = conn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT id FROM domains
             WHERE name=? COLLATE NOCASE AND id<>?
             LIMIT 1",
            vec![name.into(), domain_id.as_str().into()],
        ))
        .await?;
    anyhow::ensure!(
        name_owner.is_none(),
        "ALREADY_EXISTS: domain name already exists"
    );
    let realm_owner = conn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT id FROM domains
             WHERE realm=? COLLATE NOCASE AND id<>?
             LIMIT 1",
            vec![realm.into(), domain_id.as_str().into()],
        ))
        .await?;
    anyhow::ensure!(
        realm_owner.is_none(),
        "ALREADY_EXISTS: domain realm already exists"
    );
    Ok(())
}

fn delete_domain(backend: &SeaOrmConfigBackend, domain_id: &DomainId) -> Result<()> {
    backend.block_on(async {
        validate_domain_id(domain_id.as_str())?;
        let conn = open_pbx_system_db(backend).await?;
        let result = domain_record::Entity::delete_by_id(domain_id.as_str().to_string())
            .exec(&conn)
            .await?;
        anyhow::ensure!(
            result.rows_affected == 1,
            "RESOURCE_NOT_FOUND: domain {domain_id}"
        );
        Ok(())
    })
}

pub fn register_vc_config_table(registry: &mut VcConfigTableRegistry) {
    registry.register(Arc::new(DomainVcConfigTable));
}

struct DomainVcConfigTable;

impl VcConfigTableHandler for DomainVcConfigTable {
    fn table(&self) -> &str {
        "domain"
    }

    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<serde_json::Value> {
        let service = DomainConfigService::new(state.clone());
        let cond = command
            .fields
            .get("cond")
            .map(String::as_str)
            .unwrap_or("any");
        let key_filter = selected_keys(
            command,
            &[
                "domain_id",
                "name",
                "realm",
                "password",
                "remark",
                "enabled",
                "extension_count",
            ],
        );

        match command.action.as_str() {
            "select" => {
                ensure_orm_select_cond(cond)?;
                let result = select_domain_rows(state, page_request(command))?;
                let mut data = json!({
                    "table": "domain",
                    "rows": result.rows
                        .into_iter()
                        .map(|domain| project_domain_list_row(&domain, &key_filter))
                        .collect::<Vec<_>>(),
                });
                if let Some(pagination) = pagination_value(result.total, result.page) {
                    data["pagination"] = pagination;
                }
                Ok(data)
            }
            "count" => {
                ensure_orm_select_cond(cond)?;
                Ok(json!({
                    "table": "domain",
                    "total": count_domain_rows(state)?,
                }))
            }
            "insert" => {
                let set = decoded_set(command)?;
                let domain = service.insert_or_update(DomainUpsert {
                    domain_id: set.get("domain_id").cloned().map(DomainId::from),
                    name: required_set(&set, "name")?,
                    realm: required_set(&set, "realm")?,
                    password: required_set(&set, "password")?,
                    remark: set.get("remark").cloned().unwrap_or_default(),
                    enabled: set
                        .get("enabled")
                        .map(|value| parse_bool(value, true))
                        .unwrap_or(true),
                })?;
                Ok(json!({ "table": "domain", "domain": domain }))
            }
            "update" => {
                let set = decoded_set(command)?;
                let mut changed = 0usize;
                for domain in service
                    .rows()
                    .into_iter()
                    .filter(|domain| match_domain_cond(domain, cond))
                {
                    service.insert_or_update(DomainUpsert {
                        domain_id: Some(domain.domain_id.clone()),
                        name: set
                            .get("name")
                            .cloned()
                            .unwrap_or_else(|| domain.name.clone()),
                        realm: set
                            .get("realm")
                            .cloned()
                            .unwrap_or_else(|| domain.realm.clone()),
                        password: set
                            .get("password")
                            .cloned()
                            .unwrap_or_else(|| domain.password.clone()),
                        remark: set
                            .get("remark")
                            .cloned()
                            .unwrap_or_else(|| domain.remark.clone()),
                        enabled: set
                            .get("enabled")
                            .map(|value| parse_bool(value, domain.enabled))
                            .unwrap_or(domain.enabled),
                    })?;
                    changed += 1;
                }
                Ok(json!({ "table": "domain", "updated": changed }))
            }
            "delete" => {
                let domain_id = domain_delete_id(cond)?;
                service.delete_row(&domain_id)?;
                Ok(json!({
                    "table": "domain",
                    "deleted": 1,
                    "domain_id": domain_id,
                }))
            }
            action => Err(anyhow!("unsupported vc config action for domain: {action}")),
        }
    }
}

fn domain_delete_id(cond: &str) -> Result<DomainId> {
    let conditions = split_conditions(cond);
    let [(field, value)] = conditions.as_slice() else {
        return Err(anyhow!(
            "CONDITION_NOT_ALLOWED: domain delete requires cond=domain_id=<domain_id>"
        ));
    };
    if !matches!(field.as_str(), "domain_id" | "id") || value.is_empty() {
        return Err(anyhow!(
            "CONDITION_NOT_ALLOWED: domain delete requires cond=domain_id=<domain_id>"
        ));
    }
    Ok(DomainId::from(value.clone()))
}

fn match_domain_cond(domain: &DomainRuntimeConfig, cond: &str) -> bool {
    match cond {
        "" | "any" => true,
        _ => split_conditions(cond)
            .into_iter()
            .all(|(field, value)| match field.as_str() {
                "domain_id" | "id" => domain.domain_id.as_str() == value,
                "name" => domain.name == value,
                "realm" => domain.realm == value,
                "remark" => domain.remark == value,
                "enabled" => domain.enabled == parse_bool(&value, domain.enabled),
                _ => false,
            }),
    }
}

fn project_domain_list_row(domain: &DomainListRow, keys: &[String]) -> serde_json::Value {
    let mut row = serde_json::Map::new();
    for key in keys {
        match key.as_str() {
            "domain_id" | "id" => {
                row.insert(key.clone(), json!(domain.domain_id));
            }
            "name" => {
                row.insert(key.clone(), json!(domain.name));
            }
            "realm" => {
                row.insert(key.clone(), json!(domain.realm));
            }
            "password" => {
                row.insert(key.clone(), json!(domain.password));
            }
            "remark" => {
                row.insert(key.clone(), json!(domain.remark));
            }
            "enabled" => {
                row.insert(key.clone(), json!(domain.enabled));
            }
            "extension_count" => {
                row.insert(key.clone(), json!(domain.extension_count));
            }
            _ => {}
        }
    }
    serde_json::Value::Object(row)
}

fn select_domain_rows(
    state: &AppState,
    page: Option<crate::data_store::PageRequest>,
) -> Result<crate::data_store::PageResult<DomainListRow>> {
    with_seaorm_backend(state, |backend| {
        backend.block_on(async {
            let conn = open_pbx_system_db(backend).await?;
            let query = domain_record::Entity::find().order_by_asc(domain_record::Column::Id);
            let total = query.clone().count(&conn).await?;
            let rows = apply_page(query, page).all(&conn).await?;
            let mut result = Vec::with_capacity(rows.len());
            for row in rows {
                let domain_id = DomainId::new(row.id);
                let (domain_conn, _) = open_domain_db(backend, &domain_id).await?;
                let extension_count = extension_record::Entity::find()
                    .filter(extension_record::Column::DomainId.eq(domain_id.as_str()))
                    .count(&domain_conn)
                    .await?;
                result.push(DomainListRow {
                    domain_id,
                    name: row.name,
                    realm: row.realm,
                    password: row.password,
                    remark: row.remark,
                    enabled: row.status == "enabled",
                    extension_count,
                });
            }
            Ok(crate::data_store::PageResult {
                rows: result,
                total,
                page,
            })
        })
    })
}

fn count_domain_rows(state: &AppState) -> Result<u64> {
    with_seaorm_backend(state, |backend| {
        backend.block_on(async {
            let conn = open_pbx_system_db(backend).await?;
            Ok(domain_record::Entity::find().count(&conn).await?)
        })
    })
}

async fn load_globals(conn: &DatabaseConnection) -> Result<BTreeMap<String, GlobalValue>> {
    let rows = global_setting::Entity::find()
        .order_by_asc(global_setting::Column::Key)
        .all(conn)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.key, parse_global_value(&row.value, &row.value_type)))
        .collect())
}

fn parse_global_value(value: &str, value_type: &str) -> GlobalValue {
    match value_type {
        "integer" => GlobalValue::Integer(value.parse().unwrap_or_default()),
        "bool" => GlobalValue::Bool(matches!(value, "1" | "true" | "yes" | "on")),
        _ => GlobalValue::String(value.to_string()),
    }
}

async fn load_domain_config(
    backend: &SeaOrmConfigBackend,
    db_path: &Path,
    row: domain_record::Model,
) -> Result<DomainRuntimeConfig> {
    let conn = connect_sqlite(db_path).await?;
    let domain_id = DomainId::new(row.id.clone());
    migrate_domain_db(&conn, &domain_id).await?;

    let extensions = crate::pbx::extension::config::load_extensions(&conn, &domain_id).await?;
    let peer_trunks = crate::pbx::trunk::config::load_peer_trunks(&conn, &domain_id).await?;
    let reg_trunks = crate::pbx::trunk::config::load_reg_trunks(&conn, &domain_id).await?;
    let reg_accounts = crate::pbx::trunk::config::load_reg_accounts(&conn, &domain_id).await?;
    let inbound_routes = crate::pbx::route::config::load_inbound_routes(&conn, &domain_id).await?;
    let outbound_routes =
        crate::pbx::route::config::load_outbound_routes(&conn, &domain_id).await?;
    let recording_policies =
        crate::pbx::recording::config::load_recording_policies(&conn, &domain_id).await?;
    let ai_policies = crate::pbx::ai_policy::load_ai_policies(&conn, &domain_id).await?;
    let domain_config_version = load_domain_config_version(&conn).await?;
    let _ = backend;

    Ok(DomainRuntimeConfig {
        domain_id,
        name: row.name,
        realm: row.realm,
        password: row.password,
        remark: row.remark,
        enabled: row.status == "enabled",
        extensions,
        peer_trunks,
        reg_trunks,
        reg_accounts,
        inbound_routes,
        outbound_routes,
        recording_policies,
        ai_policies,
        version: domain_config_version.max(row.version.max(1) as u64),
    })
}

fn resolve_domain_db_path(data_dir: &Path, db_path: &str) -> PathBuf {
    let path = PathBuf::from(db_path);
    if path.is_absolute() {
        path
    } else {
        data_dir.join(path)
    }
}

fn domain_db_path(domain_id: &str) -> String {
    format!("domains/{domain_id}/config.db")
}

fn next_domain_id(instance_id: &str) -> DomainId {
    DomainId::new(format!("domain-{}-{}", instance_id, unix_timestamp_ms()))
}

fn validate_domain_id(value: &str) -> Result<()> {
    anyhow::ensure!(!value.is_empty(), "domain id is required");
    anyhow::ensure!(value.len() <= 64, "domain id is too long");
    anyhow::ensure!(
        value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_')),
        "domain id only allows ASCII letters, digits, '-' and '_'"
    );
    Ok(())
}

fn validate_domain_name(value: &str) -> Result<()> {
    anyhow::ensure!(!value.trim().is_empty(), "domain name is required");
    anyhow::ensure!(value.len() <= 128, "domain name is too long");
    Ok(())
}

fn validate_domain_realm(value: &str) -> Result<()> {
    anyhow::ensure!(!value.trim().is_empty(), "domain realm is required");
    anyhow::ensure!(value.len() <= 255, "domain realm is too long");
    anyhow::ensure!(
        value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-')),
        "domain realm only allows ASCII letters, digits, '.' and '-'"
    );
    Ok(())
}

fn ensure_orm_select_cond(cond: &str) -> Result<()> {
    if matches!(cond, "" | "any") {
        Ok(())
    } else {
        Err(anyhow!(
            "select/count cond is not implemented in ORM path yet: {cond}"
        ))
    }
}

fn unix_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn domain_delete_requires_one_unique_domain_id() {
        assert_eq!(
            domain_delete_id("domain_id=domain-a").unwrap(),
            DomainId::from("domain-a")
        );
        assert_eq!(
            domain_delete_id("id=domain-a").unwrap(),
            DomainId::from("domain-a")
        );
        assert!(domain_delete_id("any").is_err());
        assert!(domain_delete_id("domain_id=domain-a&enabled=true").is_err());
        assert!(domain_delete_id("name=Domain A").is_err());
    }

    #[tokio::test]
    async fn domain_name_and_realm_must_be_available() {
        let dir = tempdir().unwrap();
        let conn = connect_sqlite(&dir.path().join("system.db")).await.unwrap();
        migrate_system_db(&conn).await.unwrap();
        conn.execute(Statement::from_string(
            DbBackend::Sqlite,
            "INSERT INTO domains
             (id,name,realm,password,remark,status,db_path,version,updated_at)
             VALUES ('domain-a','tenant-a','tenant-a.example','secret','','enabled','a.db',1,0)"
                .to_string(),
        ))
        .await
        .unwrap();

        assert!(
            ensure_domain_identity_available(
                &conn,
                &DomainId::from("domain-b"),
                "TENANT-A",
                "tenant-b.example"
            )
            .await
            .is_err()
        );
        assert!(
            ensure_domain_identity_available(
                &conn,
                &DomainId::from("domain-b"),
                "tenant-b",
                "TENANT-A.EXAMPLE"
            )
            .await
            .is_err()
        );
        ensure_domain_identity_available(
            &conn,
            &DomainId::from("domain-a"),
            "tenant-a",
            "tenant-a.example",
        )
        .await
        .unwrap();
    }
}
