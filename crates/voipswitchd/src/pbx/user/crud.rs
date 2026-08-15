use crate::pbx::user::auth::hash_password;
use crate::pbx::user::schema::{BOOTSTRAP_ADMIN, DOMAIN_ADMIN, SYSTEM_ADMIN};
use anyhow::{Result, anyhow, ensure};
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DatabaseTransaction, DbBackend, Statement,
    TransactionTrait,
};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use voipswitch_core::command_service::{ApiCommand, CommandResult};
use voipswitch_core::types::time::unix_timestamp_ms;

pub(crate) struct WebUserApiCommand;

impl crate::commands::ApiCommandHandler for WebUserApiCommand {
    fn name(&self) -> &str {
        "web-user"
    }

    fn handle(&self, state: &crate::app::AppState, command: &ApiCommand) -> Result<CommandResult> {
        let action = command
            .args
            .first()
            .map(String::as_str)
            .ok_or_else(|| anyhow!("INVALID_ARGUMENT: web-user requires an action"))?;
        let data = crate::pbx::command_helpers::with_seaorm_backend(state, |backend| {
            backend.block_on(async {
                let conn = crate::pbx::domain::config::open_pbx_system_db(backend).await?;
                match action {
                    "list" => Ok(json!({ "users": list_users(&conn).await? })),
                    "create" => create_user(&conn, command).await,
                    "update" => update_user(&conn, command).await,
                    "delete" => delete_user(&conn, command).await,
                    _ => Err(anyhow!("INVALID_ARGUMENT: unsupported web-user action")),
                }
            })
        })?;
        Ok(CommandResult::object(
            format!("web user {action}"),
            json!({ "data": data }),
        ))
    }
}

pub(crate) async fn list_domain_ids<C>(conn: &C, user_id: i64) -> Result<Vec<String>>
where
    C: ConnectionTrait,
{
    let rows = conn
        .query_all(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT domain_id FROM web_user_domain
             WHERE user_id=? ORDER BY position,domain_id",
            vec![user_id.into()],
        ))
        .await?;
    rows.into_iter()
        .map(|row| row.try_get("", "domain_id").map_err(Into::into))
        .collect()
}

async fn list_users(conn: &DatabaseConnection) -> Result<Vec<Value>> {
    let rows = conn
        .query_all(Statement::from_string(
            DbBackend::Sqlite,
            "SELECT id,username,role,enabled,version,created_at,updated_at
             FROM web_user ORDER BY CASE role WHEN 'system_admin' THEN 0 ELSE 1 END,username"
                .to_string(),
        ))
        .await?;
    let mut users = Vec::with_capacity(rows.len());
    for row in rows {
        let user_id: i64 = row.try_get("", "id")?;
        users.push(json!({
            "user_id": user_id,
            "username": row.try_get::<String>("", "username")?,
            "role": row.try_get::<String>("", "role")?,
            "enabled": row.try_get::<i64>("", "enabled")? != 0,
            "version": row.try_get::<i64>("", "version")?,
            "created_at": row.try_get::<i64>("", "created_at")?,
            "updated_at": row.try_get::<i64>("", "updated_at")?,
            "domain_ids": list_domain_ids(conn, user_id).await?,
        }));
    }
    Ok(users)
}

async fn create_user(conn: &DatabaseConnection, command: &ApiCommand) -> Result<Value> {
    let username = normalize_username(required_field(command, "username")?)?;
    let password = required_field(command, "password")?;
    validate_password(password)?;
    let enabled = parse_bool(command.fields.get("enabled"), true)?;
    let domain_ids = parse_domain_ids(command.fields.get("domain_ids"));
    ensure!(
        !enabled || !domain_ids.is_empty(),
        "VALIDATION_ERROR: enabled domain user requires at least one domain"
    );
    if username == BOOTSTRAP_ADMIN {
        return Err(anyhow!("ALREADY_EXISTS: username already exists"));
    }

    let txn = conn.begin().await?;
    ensure_username_available(&txn, &username, None).await?;
    validate_domain_ids(&txn, &domain_ids).await?;
    let now = unix_timestamp_ms();
    let password_hash = hash_password(password)?;
    let result = txn
        .execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO web_user
             (username,password_hash,role,enabled,version,created_at,updated_at)
             VALUES (?,?,?,?,1,?,?)",
            vec![
                username.clone().into(),
                password_hash.into(),
                DOMAIN_ADMIN.into(),
                i64::from(enabled).into(),
                now.into(),
                now.into(),
            ],
        ))
        .await?;
    let user_id = i64::try_from(result.last_insert_id())?;
    replace_domain_grants(&txn, user_id, &domain_ids).await?;
    txn.commit().await?;
    Ok(json!({
        "user": {
            "user_id": user_id,
            "username": username,
            "role": DOMAIN_ADMIN,
            "enabled": enabled,
            "domain_ids": domain_ids,
        }
    }))
}

async fn update_user(conn: &DatabaseConnection, command: &ApiCommand) -> Result<Value> {
    let user_id = parse_user_id(command)?;
    let txn = conn.begin().await?;
    let current = find_user(&txn, user_id).await?;
    let is_bootstrap_admin = current.username == BOOTSTRAP_ADMIN && current.role == SYSTEM_ADMIN;
    let username = match command.fields.get("username") {
        Some(value) => normalize_username(value)?,
        None => current.username.clone(),
    };
    let enabled = parse_bool(command.fields.get("enabled"), current.enabled)?;
    if is_bootstrap_admin {
        ensure!(
            username == BOOTSTRAP_ADMIN && enabled,
            "PROTECTED_RESOURCE: bootstrap admin cannot be renamed or disabled"
        );
    }
    ensure_username_available(&txn, &username, Some(user_id)).await?;

    let domain_ids = if current.role == DOMAIN_ADMIN {
        let domains = command
            .fields
            .get("domain_ids")
            .map(|value| parse_domain_ids(Some(value)))
            .unwrap_or(list_domain_ids(&txn, user_id).await?);
        ensure!(
            !enabled || !domains.is_empty(),
            "VALIDATION_ERROR: enabled domain user requires at least one domain"
        );
        validate_domain_ids(&txn, &domains).await?;
        domains
    } else {
        Vec::new()
    };
    let password_hash = match command
        .fields
        .get("password")
        .filter(|value| !value.is_empty())
    {
        Some(password) => {
            validate_password(password)?;
            Some(hash_password(password)?)
        }
        None => None,
    };
    let now = unix_timestamp_ms();
    match password_hash {
        Some(password_hash) => {
            txn.execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE web_user
                 SET username=?,password_hash=?,enabled=?,version=version+1,updated_at=?
                 WHERE id=?",
                vec![
                    username.clone().into(),
                    password_hash.into(),
                    i64::from(enabled).into(),
                    now.into(),
                    user_id.into(),
                ],
            ))
            .await?;
        }
        None => {
            txn.execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE web_user
                 SET username=?,enabled=?,version=version+1,updated_at=?
                 WHERE id=?",
                vec![
                    username.clone().into(),
                    i64::from(enabled).into(),
                    now.into(),
                    user_id.into(),
                ],
            ))
            .await?;
        }
    }
    if current.role == DOMAIN_ADMIN {
        replace_domain_grants(&txn, user_id, &domain_ids).await?;
    }
    txn.commit().await?;
    Ok(json!({
        "user": {
            "user_id": user_id,
            "username": username,
            "role": current.role,
            "enabled": enabled,
            "domain_ids": domain_ids,
        }
    }))
}

async fn delete_user(conn: &DatabaseConnection, command: &ApiCommand) -> Result<Value> {
    let user_id = parse_user_id(command)?;
    let txn = conn.begin().await?;
    let current = find_user(&txn, user_id).await?;
    ensure!(
        !(current.username == BOOTSTRAP_ADMIN && current.role == SYSTEM_ADMIN),
        "PROTECTED_RESOURCE: bootstrap admin cannot be deleted"
    );
    txn.execute(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "DELETE FROM web_user WHERE id=?",
        vec![user_id.into()],
    ))
    .await?;
    txn.commit().await?;
    Ok(json!({ "deleted_user_id": user_id }))
}

async fn replace_domain_grants(
    txn: &DatabaseTransaction,
    user_id: i64,
    domain_ids: &[String],
) -> Result<()> {
    txn.execute(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "DELETE FROM web_user_domain WHERE user_id=?",
        vec![user_id.into()],
    ))
    .await?;
    for (position, domain_id) in domain_ids.iter().enumerate() {
        txn.execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO web_user_domain (user_id,domain_id,position) VALUES (?,?,?)",
            vec![
                user_id.into(),
                domain_id.clone().into(),
                i64::try_from(position)?.into(),
            ],
        ))
        .await?;
    }
    Ok(())
}

async fn validate_domain_ids(txn: &DatabaseTransaction, domain_ids: &[String]) -> Result<()> {
    for domain_id in domain_ids {
        let exists = txn
            .query_one(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT id FROM domains WHERE id=?",
                vec![domain_id.clone().into()],
            ))
            .await?
            .is_some();
        ensure!(exists, "RESOURCE_NOT_FOUND: domain {domain_id}");
    }
    Ok(())
}

async fn ensure_username_available(
    txn: &DatabaseTransaction,
    username: &str,
    current_user_id: Option<i64>,
) -> Result<()> {
    let row = txn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT id FROM web_user WHERE username=?",
            vec![username.into()],
        ))
        .await?;
    if let Some(row) = row {
        let found: i64 = row.try_get("", "id")?;
        ensure!(
            Some(found) == current_user_id,
            "ALREADY_EXISTS: username already exists"
        );
    }
    Ok(())
}

async fn find_user<C>(conn: &C, user_id: i64) -> Result<UserRow>
where
    C: ConnectionTrait,
{
    let row = conn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT username,role,enabled FROM web_user WHERE id=?",
            vec![user_id.into()],
        ))
        .await?
        .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: web user {user_id}"))?;
    Ok(UserRow {
        username: row.try_get("", "username")?,
        role: row.try_get("", "role")?,
        enabled: row.try_get::<i64>("", "enabled")? != 0,
    })
}

struct UserRow {
    username: String,
    role: String,
    enabled: bool,
}

pub(crate) fn required_field<'a>(command: &'a ApiCommand, name: &str) -> Result<&'a str> {
    command
        .fields
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("INVALID_ARGUMENT: missing {name}"))
}

fn parse_user_id(command: &ApiCommand) -> Result<i64> {
    let value = required_field(command, "user_id")?;
    let user_id = value
        .parse::<i64>()
        .map_err(|_| anyhow!("INVALID_ARGUMENT: invalid user_id"))?;
    ensure!(user_id > 0, "INVALID_ARGUMENT: invalid user_id");
    Ok(user_id)
}

fn normalize_username(value: &str) -> Result<String> {
    let value = value.trim();
    ensure!(
        (3..=64).contains(&value.len()),
        "VALIDATION_ERROR: username length must be 3..64"
    );
    ensure!(
        value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'@' | b'-')
        }),
        "VALIDATION_ERROR: username contains unsupported characters"
    );
    Ok(value.to_string())
}

fn validate_password(value: &str) -> Result<()> {
    ensure!(
        (4..=128).contains(&value.len()),
        "VALIDATION_ERROR: password length must be 4..128"
    );
    Ok(())
}

fn parse_bool(value: Option<&String>, default: bool) -> Result<bool> {
    match value.map(String::as_str) {
        None => Ok(default),
        Some("true" | "1" | "yes" | "on") => Ok(true),
        Some("false" | "0" | "no" | "off") => Ok(false),
        Some(_) => Err(anyhow!("VALIDATION_ERROR: invalid boolean value")),
    }
}

fn parse_domain_ids(value: Option<&String>) -> Vec<String> {
    value
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
