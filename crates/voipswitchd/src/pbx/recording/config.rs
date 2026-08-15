use crate::app::AppState;
use crate::pbx::command_helpers::{
    decoded_set, required_domain, selected_keys, split_conditions, with_seaorm_backend,
};
use crate::pbx::recording::model::{
    RecordingDirection, RecordingPolicyConfig, RecordingTargetRef, RecordingTargetType,
};
use crate::pbx::vc_config::{VcConfigTableHandler, VcConfigTableRegistry};
use anyhow::{Context, Result, anyhow, ensure};
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend, Statement,
    TransactionTrait,
};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use voipswitch_core::command_service::ConfigCommand;
use voipswitch_core::types::ids::DomainId;
use voipswitch_core::types::time::unix_timestamp_ms;

pub fn register_vc_config_table(registry: &mut VcConfigTableRegistry) {
    registry.register(Arc::new(RecordingPolicyVcConfigTable));
}

pub(crate) async fn load_recording_policies(
    conn: &DatabaseConnection,
    domain_id: &DomainId,
) -> Result<Vec<RecordingPolicyConfig>> {
    let target_rows = conn
        .query_all(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT policy_id,target_type,target_id
             FROM recording_policy_target
             WHERE domain_id=? ORDER BY policy_id,position,target_type,target_id",
            vec![domain_id.as_str().into()],
        ))
        .await?;
    let mut targets = BTreeMap::<u64, Vec<RecordingTargetRef>>::new();
    for row in target_rows {
        let policy_id = u64::try_from(row.try_get::<i64>("", "policy_id")?)
            .context("recording policy id is negative")?;
        targets
            .entry(policy_id)
            .or_default()
            .push(RecordingTargetRef {
                target_type: RecordingTargetType::try_from(
                    row.try_get::<String>("", "target_type")?.as_str(),
                )?,
                target_id: u64::try_from(row.try_get::<i64>("", "target_id")?)
                    .context("recording target id is negative")?,
            });
    }
    conn.query_all(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "SELECT id,name,enabled,direction,priority
         FROM recording_policy WHERE domain_id=? ORDER BY priority,id",
        vec![domain_id.as_str().into()],
    ))
    .await?
    .into_iter()
    .map(|row| {
        let id = u64::try_from(row.try_get::<i64>("", "id")?)
            .context("recording policy id is negative")?;
        Ok(RecordingPolicyConfig {
            id,
            name: row.try_get("", "name")?,
            enabled: row.try_get("", "enabled")?,
            targets: targets.remove(&id).unwrap_or_default(),
            direction: RecordingDirection::try_from(
                row.try_get::<String>("", "direction")?.as_str(),
            )?,
            priority: i32::try_from(row.try_get::<i64>("", "priority")?)
                .context("recording priority is out of range")?,
        })
    })
    .collect()
}

struct RecordingPolicyVcConfigTable;

impl VcConfigTableHandler for RecordingPolicyVcConfigTable {
    fn table(&self) -> &str {
        "recording_policy"
    }

    fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<Value> {
        let domain_id = required_domain(command)?;
        let rows = state
            .config()
            .snapshot()
            .domains
            .get(&domain_id)
            .map(|domain| domain.recording_policies.clone())
            .unwrap_or_default();
        match command.action.as_str() {
            "select" => {
                let id = id_condition(command, false)?;
                let keys = selected_keys(
                    command,
                    &[
                        "domain_id",
                        "id",
                        "name",
                        "enabled",
                        "target_refs",
                        "direction",
                        "priority",
                    ],
                );
                Ok(json!({
                    "table": self.table(),
                    "domain_id": domain_id,
                    "rows": rows
                        .into_iter()
                        .filter(|row| id.is_none_or(|id| row.id == id))
                        .map(|row| project(&domain_id, &row, &keys))
                        .collect::<Vec<_>>(),
                }))
            }
            "insert" => {
                let set = decoded_set(command)?;
                ensure!(
                    !set.contains_key("id"),
                    "id is server-assigned for recording_policy insert"
                );
                let policy = policy_from_set(&set, None)?;
                validate_targets(state, &domain_id, &policy)?;
                let id = with_seaorm_backend(state, |backend| {
                    backend.block_on(insert_policy(backend, &domain_id, &policy))
                })?;
                reload(state)?;
                Ok(json!({
                    "table": self.table(),
                    "domain_id": domain_id,
                    "id": id,
                }))
            }
            "update" => {
                let id = id_condition(command, true)?.expect("required id");
                let existing = rows
                    .into_iter()
                    .find(|row| row.id == id)
                    .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: recording_policy id {id}"))?;
                let policy = policy_from_set(&decoded_set(command)?, Some(existing))?;
                validate_targets(state, &domain_id, &policy)?;
                with_seaorm_backend(state, |backend| {
                    backend.block_on(update_policy(backend, &domain_id, &policy))
                })?;
                reload(state)?;
                Ok(json!({
                    "table": self.table(),
                    "domain_id": domain_id,
                    "id": id,
                    "updated": 1,
                }))
            }
            "delete" => {
                let id = id_condition(command, true)?.expect("required id");
                with_seaorm_backend(state, |backend| {
                    backend.block_on(delete_policy(backend, &domain_id, id))
                })?;
                reload(state)?;
                Ok(json!({
                    "table": self.table(),
                    "domain_id": domain_id,
                    "id": id,
                    "deleted": 1,
                }))
            }
            action => Err(anyhow!(
                "unsupported vc config action for recording_policy: {action}"
            )),
        }
    }
}

fn policy_from_set(
    set: &BTreeMap<String, String>,
    existing: Option<RecordingPolicyConfig>,
) -> Result<RecordingPolicyConfig> {
    let id = existing.as_ref().map(|row| row.id).unwrap_or_default();
    let name = set
        .get("name")
        .cloned()
        .or_else(|| existing.as_ref().map(|row| row.name.clone()))
        .ok_or_else(|| anyhow!("name is required"))?;
    ensure!(!name.trim().is_empty(), "name cannot be empty");
    let enabled = set
        .get("enabled")
        .map(|value| match value.as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(anyhow!("enabled must be a boolean")),
        })
        .transpose()?
        .or_else(|| existing.as_ref().map(|row| row.enabled))
        .unwrap_or(true);
    let targets = if let Some(value) = set.get("target_refs") {
        parse_target_refs(value)?
    } else if set.contains_key("target_type") || set.contains_key("target_id") {
        let target_type = set
            .get("target_type")
            .ok_or_else(|| anyhow!("target_type is required with target_id"))
            .and_then(|value| RecordingTargetType::try_from(value.as_str()))?;
        let target_id = set
            .get("target_id")
            .ok_or_else(|| anyhow!("target_id is required with target_type"))?
            .parse::<u64>()
            .map_err(|_| anyhow!("target_id must be a positive integer"))?;
        ensure!(target_id > 0, "target_id must be a positive integer");
        vec![RecordingTargetRef {
            target_type,
            target_id,
        }]
    } else {
        existing
            .as_ref()
            .map(|row| row.targets.clone())
            .ok_or_else(|| anyhow!("target_refs is required"))?
    };
    ensure!(
        !targets.is_empty(),
        "target_refs must contain at least one target"
    );
    let direction = set
        .get("direction")
        .map(|value| RecordingDirection::try_from(value.as_str()))
        .transpose()?
        .or_else(|| existing.as_ref().map(|row| row.direction))
        .unwrap_or(RecordingDirection::Both);
    let priority = set
        .get("priority")
        .map(|value| value.parse::<i32>())
        .transpose()
        .map_err(|_| anyhow!("priority must be an integer"))?
        .or_else(|| existing.as_ref().map(|row| row.priority))
        .unwrap_or(100);
    Ok(RecordingPolicyConfig {
        id,
        name: name.trim().to_string(),
        enabled,
        targets,
        direction,
        priority,
    })
}

fn parse_target_refs(value: &str) -> Result<Vec<RecordingTargetRef>> {
    let mut seen = BTreeSet::new();
    let mut targets = Vec::new();
    for raw in value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let target = RecordingTargetRef::parse(raw)?;
        if seen.insert(target.stable_ref()) {
            targets.push(target);
        }
    }
    Ok(targets)
}

fn validate_targets(
    state: &AppState,
    domain_id: &DomainId,
    policy: &RecordingPolicyConfig,
) -> Result<()> {
    let snapshot = state.config().snapshot();
    let domain = snapshot
        .domains
        .get(domain_id)
        .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: domain {domain_id}"))?;
    for target in &policy.targets {
        let exists = match target.target_type {
            RecordingTargetType::Extension => domain
                .extensions
                .iter()
                .any(|row| row.id == target.target_id && row.enabled),
            RecordingTargetType::PeerTrunk => domain
                .peer_trunks
                .iter()
                .any(|row| row.id == target.target_id && row.enabled),
            RecordingTargetType::RegTrunk => domain
                .reg_trunks
                .iter()
                .any(|row| row.id == target.target_id && row.enabled),
        };
        ensure!(
            exists,
            "INVALID_REFERENCE: enabled {} {} not found in domain {}",
            target.target_type.as_str(),
            target.target_id,
            domain_id
        );
    }
    Ok(())
}

pub(crate) fn ensure_target_not_referenced(
    state: &AppState,
    domain_id: &DomainId,
    target_type: RecordingTargetType,
    target_id: u64,
) -> Result<()> {
    let snapshot = state.config().snapshot();
    let referenced = snapshot.domains.get(domain_id).into_iter().any(|domain| {
        domain
            .recording_policies
            .iter()
            .flat_map(|policy| policy.targets.iter())
            .chain(
                domain
                    .ai_policies
                    .iter()
                    .flat_map(|policy| policy.targets.iter()),
            )
            .any(|target| target.target_type == target_type && target.target_id == target_id)
    });
    ensure!(
        !referenced,
        "RESOURCE_IN_USE: {} {} is referenced by recording_policy or ai_policy",
        target_type.as_str(),
        target_id
    );
    Ok(())
}

fn project(domain_id: &DomainId, policy: &RecordingPolicyConfig, keys: &[String]) -> Value {
    let mut object = Map::new();
    for key in keys {
        let value = match key.as_str() {
            "domain_id" => json!(domain_id),
            "id" => json!(policy.id),
            "name" => json!(policy.name),
            "enabled" => json!(policy.enabled),
            "target_refs" => json!(
                policy
                    .targets
                    .iter()
                    .copied()
                    .map(RecordingTargetRef::stable_ref)
                    .collect::<Vec<_>>()
            ),
            "direction" => json!(policy.direction.as_str()),
            "priority" => json!(policy.priority),
            _ => continue,
        };
        object.insert(key.clone(), value);
    }
    Value::Object(object)
}

fn id_condition(command: &ConfigCommand, required: bool) -> Result<Option<u64>> {
    let conditions = command
        .fields
        .get("cond")
        .map(|value| split_conditions(value))
        .unwrap_or_default();
    if conditions.is_empty() {
        ensure!(!required, "cond=id=<id> is required");
        return Ok(None);
    }
    ensure!(
        conditions.len() == 1 && conditions[0].0 == "id",
        "recording_policy only supports cond=id=<id>"
    );
    let id = conditions[0]
        .1
        .parse::<u64>()
        .map_err(|_| anyhow!("recording_policy id must be a positive integer"))?;
    ensure!(id > 0, "recording_policy id must be a positive integer");
    Ok(Some(id))
}

async fn insert_policy(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    policy: &RecordingPolicyConfig,
) -> Result<u64> {
    let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
    let txn = conn.begin().await?;
    let id = allocate_id(&txn, domain_id).await?;
    let now = i64::try_from(unix_timestamp_ms()).context("timestamp overflow")?;
    txn.execute(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO recording_policy
         (domain_id,id,name,enabled,direction,priority,version,created_at,updated_at)
         VALUES (?,?,?,?,?,?,1,?,?)",
        vec![
            domain_id.as_str().into(),
            i64::try_from(id)?.into(),
            policy.name.clone().into(),
            policy.enabled.into(),
            policy.direction.as_str().into(),
            i64::from(policy.priority).into(),
            now.into(),
            now.into(),
        ],
    ))
    .await?;
    replace_policy_targets(&txn, domain_id, id, &policy.targets).await?;
    crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
    txn.commit().await?;
    Ok(id)
}

async fn update_policy(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    policy: &RecordingPolicyConfig,
) -> Result<()> {
    let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
    let txn = conn.begin().await?;
    let result = txn
        .execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE recording_policy SET name=?,enabled=?,direction=?,priority=?,
             version=version+1,updated_at=?
             WHERE domain_id=? AND id=?",
            vec![
                policy.name.clone().into(),
                policy.enabled.into(),
                policy.direction.as_str().into(),
                i64::from(policy.priority).into(),
                i64::try_from(unix_timestamp_ms())?.into(),
                domain_id.as_str().into(),
                i64::try_from(policy.id)?.into(),
            ],
        ))
        .await?;
    ensure!(
        result.rows_affected() == 1,
        "RESOURCE_NOT_FOUND: recording_policy id {}",
        policy.id
    );
    replace_policy_targets(&txn, domain_id, policy.id, &policy.targets).await?;
    crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
    txn.commit().await?;
    Ok(())
}

async fn delete_policy(
    backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
    domain_id: &DomainId,
    id: u64,
) -> Result<()> {
    let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
    let txn = conn.begin().await?;
    txn.execute(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "DELETE FROM recording_policy_target WHERE domain_id=? AND policy_id=?",
        vec![domain_id.as_str().into(), i64::try_from(id)?.into()],
    ))
    .await?;
    let result = txn
        .execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "DELETE FROM recording_policy WHERE domain_id=? AND id=?",
            vec![domain_id.as_str().into(), i64::try_from(id)?.into()],
        ))
        .await?;
    ensure!(
        result.rows_affected() == 1,
        "RESOURCE_NOT_FOUND: recording_policy id {id}"
    );
    crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
    txn.commit().await?;
    Ok(())
}

async fn replace_policy_targets(
    txn: &DatabaseTransaction,
    domain_id: &DomainId,
    policy_id: u64,
    targets: &[RecordingTargetRef],
) -> Result<()> {
    txn.execute(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "DELETE FROM recording_policy_target WHERE domain_id=? AND policy_id=?",
        vec![domain_id.as_str().into(), i64::try_from(policy_id)?.into()],
    ))
    .await?;
    for (position, target) in targets.iter().enumerate() {
        txn.execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO recording_policy_target
             (domain_id,policy_id,target_type,target_id,position) VALUES (?,?,?,?,?)",
            vec![
                domain_id.as_str().into(),
                i64::try_from(policy_id)?.into(),
                target.target_type.as_str().into(),
                i64::try_from(target.target_id)?.into(),
                i64::try_from(position)?.into(),
            ],
        ))
        .await?;
    }
    Ok(())
}

async fn allocate_id(txn: &DatabaseTransaction, domain_id: &DomainId) -> Result<u64> {
    let row = txn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO config_id_sequences (domain_id,resource_type,next_id) VALUES (?,?,2)
             ON CONFLICT(domain_id,resource_type)
             DO UPDATE SET next_id=config_id_sequences.next_id+1
             RETURNING next_id-1 AS allocated_id",
            vec![domain_id.as_str().into(), "recording_policy".into()],
        ))
        .await?
        .context("allocate recording policy id")?;
    u64::try_from(row.try_get::<i64>("", "allocated_id")?)
        .context("allocated recording policy id is negative")
}

fn reload(state: &AppState) -> Result<()> {
    let next = state.backend().load_runtime_config()?;
    state.config().replace(next);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pbx::extension::model::ExtensionConfig;

    #[test]
    fn policy_update_preserves_unspecified_fields() {
        let existing = RecordingPolicyConfig {
            id: 7,
            name: "record 1001".to_string(),
            enabled: true,
            targets: vec![RecordingTargetRef {
                target_type: RecordingTargetType::Extension,
                target_id: 1,
            }],
            direction: RecordingDirection::Both,
            priority: 100,
        };
        let policy = policy_from_set(
            &BTreeMap::from([("priority".to_string(), "10".to_string())]),
            Some(existing),
        )
        .unwrap();
        assert_eq!(policy.id, 7);
        assert_eq!(policy.targets[0].target_id, 1);
        assert_eq!(policy.priority, 10);
    }

    #[test]
    fn target_refs_support_mixed_types_and_deduplicate() {
        let policy = policy_from_set(
            &BTreeMap::from([
                ("name".to_string(), "mixed".to_string()),
                (
                    "target_refs".to_string(),
                    "ext:1,peer:2,ext:1,reg:3".to_string(),
                ),
            ]),
            None,
        )
        .unwrap();
        assert_eq!(
            policy
                .targets
                .iter()
                .copied()
                .map(RecordingTargetRef::stable_ref)
                .collect::<Vec<_>>(),
            ["ext:1", "peer:2", "reg:3"]
        );
    }

    #[test]
    fn legacy_single_target_fields_remain_supported() {
        let policy = policy_from_set(
            &BTreeMap::from([
                ("name".to_string(), "legacy".to_string()),
                ("target_type".to_string(), "extension".to_string()),
                ("target_id".to_string(), "9".to_string()),
            ]),
            None,
        )
        .unwrap();
        assert_eq!(policy.targets[0].stable_ref(), "ext:9");
    }

    #[test]
    fn target_enums_are_strict() {
        assert!(RecordingTargetType::try_from("route").is_err());
        assert!(RecordingDirection::try_from("sideways").is_err());
        let _ = ExtensionConfig {
            id: 1,
            number: "1001".to_string(),
            auth_user: "1001".to_string(),
            password: "secret".to_string(),
            enabled: true,
        };
    }
}
