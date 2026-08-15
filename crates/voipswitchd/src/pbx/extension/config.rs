use crate::app::AppState;
use crate::pbx::command_helpers::{
    apply_page, decoded_set, page_request, pagination_value, parse_bool, required_domain,
    required_set, selected_keys, split_conditions, with_seaorm_backend,
};
use crate::pbx::extension::db::extension_record;
use crate::pbx::extension::model::ExtensionConfig;
use crate::pbx::vc_config::{VcConfigTableHandler, VcConfigTableRegistry};
use anyhow::{Context, Result, anyhow};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction,
    DbBackend, EntityTrait, IntoActiveModel, PaginatorTrait, QueryFilter, QueryOrder, Set,
    Statement, TransactionTrait,
};
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeSet;
use std::sync::Arc;
use voipswitch_core::command_service::ConfigCommand;
use voipswitch_core::types::ids::DomainId;

#[derive(Clone)]
#[allow(dead_code)]
pub struct ExtensionConfigService {
    state: AppState,
}

#[allow(dead_code)]
impl ExtensionConfigService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    fn rows(&self, domain_id: &DomainId) -> Vec<ExtensionConfig> {
        self.state
            .config()
            .snapshot()
            .domains
            .get(domain_id)
            .map(|domain| domain.extensions.clone())
            .unwrap_or_default()
    }

    fn row(&self, domain_id: &DomainId, id: u64) -> Option<ExtensionConfig> {
        self.rows(domain_id)
            .into_iter()
            .find(|extension| extension.id == id)
    }

    fn insert(&self, domain_id: &DomainId, extension: ExtensionConfig) -> Result<ExtensionConfig> {
        let ids = self.batch_insert(domain_id, vec![extension])?;
        let id = ids[0];
        self.row(domain_id, id)
            .ok_or_else(|| anyhow!("extension not found after reload: {domain_id}/{id}"))
    }

    fn batch_insert(
        &self,
        domain_id: &DomainId,
        extensions: Vec<ExtensionConfig>,
    ) -> Result<Vec<u64>> {
        let ids = with_seaorm_backend(&self.state, |backend| {
            insert_extensions(backend, domain_id, &extensions)
        })?;
        self.reload()?;
        Ok(ids)
    }

    fn update(&self, domain_id: &DomainId, extension: ExtensionConfig) -> Result<ExtensionConfig> {
        with_seaorm_backend(&self.state, |backend| {
            update_extension(backend, domain_id, &extension)
        })?;
        self.reload()?;
        self.row(domain_id, extension.id).ok_or_else(|| {
            anyhow!(
                "extension not found after reload: {domain_id}/{}",
                extension.id
            )
        })
    }

    fn delete_row(&self, domain_id: &DomainId, id: u64) -> Result<()> {
        crate::pbx::recording::config::ensure_target_not_referenced(
            &self.state,
            domain_id,
            crate::pbx::recording::model::RecordingTargetType::Extension,
            id,
        )?;
        with_seaorm_backend(&self.state, |backend| {
            delete_extension(backend, domain_id, id)
        })?;
        self.reload()
    }

    fn reload(&self) -> Result<()> {
        let backend = self.state.backend();
        self.state.config().replace(backend.load_runtime_config()?);
        Ok(())
    }
}

pub fn register_vc_config_table(registry: &mut VcConfigTableRegistry) {
    registry.register(Arc::new(ExtensionVcConfigTable));
}

pub(crate) async fn load_extensions(
    conn: &DatabaseConnection,
    domain_id: &DomainId,
) -> Result<Vec<ExtensionConfig>> {
    let rows = extension_record::Entity::find()
        .filter(extension_record::Column::DomainId.eq(domain_id.as_str()))
        .order_by_asc(extension_record::Column::Number)
        .all(conn)
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(ExtensionConfig {
                id: u64::try_from(row.id).context("extension id is negative")?,
                number: row.number,
                auth_user: row.auth_user,
                password: row.password,
                enabled: row.enabled,
            })
        })
        .collect::<Result<Vec<_>>>()
}

fn insert_extensions(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    extensions: &[ExtensionConfig],
) -> Result<Vec<u64>> {
    validate_batch_extensions(extensions)?;
    backend.block_on(async {
        let (conn, domain_db_path) =
            crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let mut ids = Vec::with_capacity(extensions.len());
        for extension in extensions {
            let id = allocate_extension_id(&txn, domain_id).await?;
            let now = voipswitch_core::types::time::unix_timestamp_ms() as i64;
            extension_record::Entity::insert(extension_record::ActiveModel {
                domain_id: Set(domain_id.as_str().to_string()),
                id: Set(id),
                number: Set(extension.number.clone()),
                auth_user: Set(extension.auth_user.clone()),
                password: Set(extension.password.clone()),
                enabled: Set(extension.enabled),
                note: Set(String::new()),
                created_at: Set(now),
                updated_at: Set(now),
            })
            .exec_without_returning(&txn)
            .await
            .with_context(|| format!("insert extension in {}", domain_db_path.display()))?;
            ids.push(u64::try_from(id).context("allocated extension id is negative")?);
        }
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(ids)
    })
}

fn update_extension(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    extension: &ExtensionConfig,
) -> Result<()> {
    backend.block_on(async {
        validate_extension(extension)?;
        let id = i64::try_from(extension.id).context("extension id is too large")?;
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let existing = find_extension(domain_id, id)
            .one(&txn)
            .await?
            .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: extension id {}", extension.id))?;
        if existing.number != extension.number {
            return Err(anyhow!("extension number is immutable"));
        }
        if !extension.enabled {
            ensure_extension_not_referenced(&txn, domain_id, &existing.number).await?;
        }
        let mut active = existing.into_active_model();
        active.auth_user = Set(extension.auth_user.clone());
        active.password = Set(extension.password.clone());
        active.enabled = Set(extension.enabled);
        active.updated_at = Set(voipswitch_core::types::time::unix_timestamp_ms() as i64);
        active.update(&txn).await?;
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(())
    })
}

fn delete_extension(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    id: u64,
) -> Result<()> {
    backend.block_on(async {
        let id = i64::try_from(id).context("extension id is too large")?;
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let existing = find_extension(domain_id, id)
            .one(&txn)
            .await?
            .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: extension id {id}"))?;
        ensure_extension_not_referenced(&txn, domain_id, &existing.number).await?;
        let deleted = extension_record::Entity::delete_many()
            .filter(extension_record::Column::DomainId.eq(domain_id.as_str()))
            .filter(extension_record::Column::Id.eq(id))
            .exec(&txn)
            .await?;
        if deleted.rows_affected != 1 {
            return Err(anyhow!("RESOURCE_NOT_FOUND: extension id {id}"));
        }
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(())
    })
}

fn find_extension(domain_id: &DomainId, id: i64) -> sea_orm::Select<extension_record::Entity> {
    extension_record::Entity::find()
        .filter(extension_record::Column::DomainId.eq(domain_id.as_str()))
        .filter(extension_record::Column::Id.eq(id))
}

async fn ensure_extension_not_referenced<C>(
    conn: &C,
    domain_id: &DomainId,
    number: &str,
) -> Result<()>
where
    C: ConnectionTrait,
{
    let target = format!("ext-{number}");
    if let Some(row) = conn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT id FROM inbound_route WHERE domain_id=? AND target=? LIMIT 1",
            vec![domain_id.as_str().into(), target.into()],
        ))
        .await?
    {
        let route_id: i64 = row.try_get("", "id")?;
        return Err(anyhow!(
            "RESOURCE_IN_USE: extension {number} referenced by inbound_route id {route_id}"
        ));
    }
    Ok(())
}

async fn allocate_extension_id(txn: &DatabaseTransaction, domain_id: &DomainId) -> Result<i64> {
    let row = txn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO config_id_sequences (domain_id, resource_type, next_id)
             VALUES (?, ?, 2)
             ON CONFLICT(domain_id, resource_type) DO UPDATE
             SET next_id = config_id_sequences.next_id + 1
             RETURNING next_id - 1 AS allocated_id",
            vec![domain_id.as_str().into(), "extension".into()],
        ))
        .await?
        .ok_or_else(|| anyhow!("allocate extension id returned no row"))?;
    row.try_get("", "allocated_id")
        .context("read allocated extension id")
}

struct ExtensionVcConfigTable;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExtensionBatchInsertRecord {
    number: String,
    #[serde(default)]
    auth_user: Option<String>,
    password: String,
    #[serde(default)]
    enabled: Option<bool>,
}

impl VcConfigTableHandler for ExtensionVcConfigTable {
    fn table(&self) -> &str {
        "ext"
    }

    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<serde_json::Value> {
        let service = ExtensionConfigService::new(state.clone());
        let domain_id = required_domain(command)?;
        let cond = command
            .fields
            .get("cond")
            .map(String::as_str)
            .unwrap_or("any");
        let key_filter = selected_keys(
            command,
            &["id", "number", "auth_user", "password", "enabled"],
        );

        match command.action.as_str() {
            "select" => {
                let id = extension_id_condition(cond, false)?;
                let result = select_ext_rows(state, &domain_id, id, page_request(command))?;
                let mut data = json!({
                    "table": "ext",
                    "domain_id": domain_id,
                    "rows": result.rows
                        .into_iter()
                        .map(|extension| project_ext_row(&domain_id, &extension, &key_filter))
                        .collect::<Vec<_>>(),
                });
                if let Some(pagination) = pagination_value(result.total, result.page) {
                    data["pagination"] = pagination;
                }
                Ok(data)
            }
            "count" => {
                let id = extension_id_condition(cond, false)?;
                Ok(json!({
                    "table": "ext",
                    "domain_id": domain_id,
                    "total": count_ext_rows(state, &domain_id, id)?,
                }))
            }
            "insert" => {
                let set = decoded_set(command)?;
                if set.contains_key("id") {
                    return Err(anyhow!("id is server-assigned for ext insert"));
                }
                let number = required_set(&set, "number")?;
                let auth_user = set
                    .get("auth_user")
                    .cloned()
                    .unwrap_or_else(|| number.clone());
                let password = required_set(&set, "password")?;
                let enabled = set
                    .get("enabled")
                    .map(|value| parse_bool(value, true))
                    .unwrap_or(true);
                let extension = service.insert(
                    &domain_id,
                    ExtensionConfig {
                        id: 0,
                        number,
                        auth_user,
                        password,
                        enabled,
                    },
                )?;
                Ok(json!({
                    "table": "ext",
                    "domain_id": domain_id,
                    "id": extension.id,
                    "status": "ok",
                }))
            }
            "batch_insert" => {
                let extensions = decode_batch_extensions(command)?;
                let ids = service.batch_insert(&domain_id, extensions)?;
                Ok(json!({
                    "table": "ext",
                    "domain_id": domain_id,
                    "items": ids
                        .into_iter()
                        .enumerate()
                        .map(|(input_index, id)| json!({
                            "input_index": input_index,
                            "id": id,
                        }))
                        .collect::<Vec<_>>(),
                }))
            }
            "update" => {
                let set = decoded_set(command)?;
                if set.contains_key("number") {
                    return Err(anyhow!("extension number is immutable"));
                }
                let id = extension_id_condition(cond, true)?.expect("required id");
                let extension = service
                    .row(&domain_id, id)
                    .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: extension id {id}"))?;
                let extension = service.update(
                    &domain_id,
                    ExtensionConfig {
                        id,
                        number: extension.number,
                        auth_user: set.get("auth_user").cloned().unwrap_or(extension.auth_user),
                        password: set.get("password").cloned().unwrap_or(extension.password),
                        enabled: set
                            .get("enabled")
                            .map(|value| parse_bool(value, extension.enabled))
                            .unwrap_or(extension.enabled),
                    },
                )?;
                Ok(json!({
                    "table": "ext",
                    "domain_id": domain_id,
                    "id": extension.id,
                    "updated": 1,
                }))
            }
            "delete" => {
                let id = extension_id_condition(cond, true)?.expect("required id");
                service.delete_row(&domain_id, id)?;
                Ok(json!({
                    "table": "ext",
                    "domain_id": domain_id,
                    "id": id,
                    "deleted": 1,
                }))
            }
            action => Err(anyhow!("unsupported vc config action for ext: {action}")),
        }
    }
}

fn project_ext_row(
    domain_id: &DomainId,
    extension: &ExtensionConfig,
    keys: &[String],
) -> serde_json::Value {
    let mut row = serde_json::Map::new();
    for key in keys {
        match key.as_str() {
            "domain_id" => {
                row.insert(key.clone(), json!(domain_id));
            }
            "id" => {
                row.insert(key.clone(), json!(extension.id));
            }
            "number" => {
                row.insert(key.clone(), json!(extension.number));
            }
            "auth_user" => {
                row.insert(key.clone(), json!(extension.auth_user));
            }
            "password" => {
                row.insert(key.clone(), json!(extension.password));
            }
            "enabled" => {
                row.insert(key.clone(), json!(extension.enabled));
            }
            _ => {}
        }
    }
    serde_json::Value::Object(row)
}

fn select_ext_rows(
    state: &AppState,
    domain_id: &DomainId,
    id: Option<u64>,
    page: Option<crate::data_store::PageRequest>,
) -> Result<crate::data_store::PageResult<ExtensionConfig>> {
    with_seaorm_backend(state, |backend| {
        backend.block_on(async {
            let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
            let mut query = extension_record::Entity::find()
                .filter(extension_record::Column::DomainId.eq(domain_id.as_str()))
                .order_by_asc(extension_record::Column::Number);
            if let Some(id) = id {
                query = query.filter(
                    extension_record::Column::Id
                        .eq(i64::try_from(id).context("extension id is too large")?),
                );
            }
            let total = query.clone().count(&conn).await?;
            let rows = apply_page(query, page).all(&conn).await?;
            Ok(crate::data_store::PageResult {
                rows: rows
                    .into_iter()
                    .map(|row| {
                        Ok(ExtensionConfig {
                            id: u64::try_from(row.id).context("extension id is negative")?,
                            number: row.number,
                            auth_user: row.auth_user,
                            password: row.password,
                            enabled: row.enabled,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                total,
                page,
            })
        })
    })
}

fn count_ext_rows(state: &AppState, domain_id: &DomainId, id: Option<u64>) -> Result<u64> {
    with_seaorm_backend(state, |backend| {
        backend.block_on(async {
            let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
            let mut query = extension_record::Entity::find()
                .filter(extension_record::Column::DomainId.eq(domain_id.as_str()));
            if let Some(id) = id {
                query = query.filter(
                    extension_record::Column::Id
                        .eq(i64::try_from(id).context("extension id is too large")?),
                );
            }
            Ok(query.count(&conn).await?)
        })
    })
}

fn validate_extension(extension: &ExtensionConfig) -> Result<()> {
    validate_extension_number(&extension.number)?;
    anyhow::ensure!(!extension.auth_user.is_empty(), "auth user is required");
    anyhow::ensure!(!extension.password.is_empty(), "password is required");
    Ok(())
}

fn validate_batch_extensions(extensions: &[ExtensionConfig]) -> Result<()> {
    anyhow::ensure!(!extensions.is_empty(), "batch records must not be empty");
    anyhow::ensure!(
        extensions.len() <= 1000,
        "batch records exceeds maximum of 1000"
    );

    let mut numbers = BTreeSet::new();
    let mut auth_users = BTreeSet::new();
    for extension in extensions {
        validate_extension(extension)?;
        anyhow::ensure!(
            numbers.insert(extension.number.as_str()),
            "duplicate extension number in batch: {}",
            extension.number
        );
        anyhow::ensure!(
            auth_users.insert(extension.auth_user.as_str()),
            "duplicate extension auth user in batch: {}",
            extension.auth_user
        );
    }
    Ok(())
}

fn decode_batch_extensions(command: &ConfigCommand) -> Result<Vec<ExtensionConfig>> {
    let records = command
        .fields
        .get("records")
        .ok_or_else(|| anyhow!("records is required for ext batch_insert"))?;
    let records: Vec<ExtensionBatchInsertRecord> =
        serde_json::from_str(records).context("records must be a JSON array of ext records")?;
    let extensions = records
        .into_iter()
        .map(|record| {
            let auth_user = record.auth_user.unwrap_or_else(|| record.number.clone());
            ExtensionConfig {
                id: 0,
                number: record.number,
                auth_user,
                password: record.password,
                enabled: record.enabled.unwrap_or(true),
            }
        })
        .collect::<Vec<_>>();
    validate_batch_extensions(&extensions)?;
    Ok(extensions)
}

fn validate_extension_number(value: &str) -> Result<()> {
    anyhow::ensure!(!value.is_empty(), "extension number is required");
    anyhow::ensure!(value.len() <= 32, "extension number is too long");
    anyhow::ensure!(
        value.bytes().all(|b| b.is_ascii_digit()),
        "extension number only allows ASCII digits"
    );
    Ok(())
}

fn extension_id_condition(cond: &str, required: bool) -> Result<Option<u64>> {
    if matches!(cond, "" | "any") {
        return if required {
            Err(anyhow!("CONDITION_NOT_ALLOWED: ext requires cond=id=<id>"))
        } else {
            Ok(None)
        };
    }
    let conditions = split_conditions(cond);
    if conditions.len() != 1 || conditions[0].0 != "id" {
        return Err(anyhow!("CONDITION_NOT_ALLOWED: ext requires cond=id=<id>"));
    }
    let id = conditions[0]
        .1
        .parse::<u64>()
        .context("invalid extension id")?;
    if id == 0 {
        return Err(anyhow!("invalid extension id"));
    }
    Ok(Some(id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_projection_returns_configured_password() {
        let row = ExtensionConfig {
            id: 1,
            number: "1001".to_string(),
            auth_user: "1001".to_string(),
            password: "configured-password".to_string(),
            enabled: true,
        };

        let value = project_ext_row(&DomainId::from("domain-a"), &row, &["password".to_string()]);

        assert_eq!(value["password"], "configured-password");
    }
}
