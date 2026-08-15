use crate::pbx::command_helpers::with_seaorm_backend;
use crate::pbx::domain::config::open_pbx_system_db;
use crate::pbx::user::schema::DOMAIN_ADMIN;
use anyhow::{Result, anyhow};
use argon2::password_hash::{SaltString, rand_core::OsRng};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};
use serde_json::{Value, json};
use voipswitch_core::command_service::{ApiCommand, CommandResult};

pub(crate) fn hash_password(password: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|err| anyhow!("hash password: {err}"))
}

pub(crate) fn verify_password(password_hash: &str, password: &str) -> bool {
    PasswordHash::new(password_hash).ok().is_some_and(|parsed| {
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    })
}

pub(crate) fn auth_failed() -> anyhow::Error {
    anyhow!("AUTH_FAILED: invalid username or password")
}

pub(crate) async fn authenticate(
    conn: &DatabaseConnection,
    username: &str,
    password: &str,
) -> Result<Value> {
    if let Some(row) = conn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT id,username,password_hash,role,enabled
             FROM web_user WHERE username=?",
            vec![username.into()],
        ))
        .await?
    {
        let enabled: i64 = row.try_get("", "enabled")?;
        let password_hash: String = row.try_get("", "password_hash")?;
        if enabled == 0 || !verify_password(&password_hash, password) {
            return Err(auth_failed());
        }
        let user_id: i64 = row.try_get("", "id")?;
        return Ok(json!({
            "user_id": user_id,
            "username": row.try_get::<String>("", "username")?,
            "role": row.try_get::<String>("", "role")?,
            "enabled": true,
            "domain_ids": crate::pbx::user::crud::list_domain_ids(conn, user_id).await?,
            "auth_source": "web_user",
        }));
    }

    let rows = conn
        .query_all(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT id,name,password,status
             FROM domains
             WHERE name=? COLLATE NOCASE
             LIMIT 2",
            vec![username.into()],
        ))
        .await?;
    if rows.len() != 1 {
        return Err(auth_failed());
    }
    let row = &rows[0];
    let enabled = row.try_get::<String>("", "status")? == "enabled";
    let configured_password: String = row.try_get("", "password")?;
    if !enabled || configured_password.is_empty() || configured_password != password {
        return Err(auth_failed());
    }
    let domain_id: String = row.try_get("", "id")?;
    Ok(json!({
        "user_id": 0,
        "username": row.try_get::<String>("", "name")?,
        "role": DOMAIN_ADMIN,
        "enabled": true,
        "domain_ids": [domain_id],
        "auth_source": "domain",
    }))
}

pub(crate) struct WebAuthApiCommand;

impl crate::commands::ApiCommandHandler for WebAuthApiCommand {
    fn name(&self) -> &str {
        "web-auth"
    }

    fn handle(&self, state: &crate::app::AppState, command: &ApiCommand) -> Result<CommandResult> {
        anyhow::ensure!(
            command.args.first().map(String::as_str) == Some("verify"),
            "INVALID_ARGUMENT: web-auth requires verify"
        );
        let username = crate::pbx::user::crud::required_field(command, "username")?;
        let password = crate::pbx::user::crud::required_field(command, "password")?;
        let user = with_seaorm_backend(state, |backend| {
            backend.block_on(async {
                let conn = open_pbx_system_db(backend).await?;
                authenticate(&conn, username, password).await
            })
        })?;
        Ok(CommandResult::object(
            "web user authenticated",
            json!({ "data": user }),
        ))
    }
}
