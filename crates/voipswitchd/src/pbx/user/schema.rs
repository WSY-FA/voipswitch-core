use anyhow::Result;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};
use voipswitch_core::types::time::unix_timestamp_ms;

pub(crate) const BOOTSTRAP_ADMIN: &str = "admin";
pub(crate) const SYSTEM_ADMIN: &str = "system_admin";
pub(crate) const DOMAIN_ADMIN: &str = "domain_admin";

pub(crate) async fn ensure_schema_and_bootstrap(conn: &DatabaseConnection) -> Result<()> {
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        "CREATE TABLE IF NOT EXISTS web_user (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            version INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            CHECK(role IN ('system_admin','domain_admin'))
        );
        CREATE TABLE IF NOT EXISTS web_user_domain (
            user_id INTEGER NOT NULL,
            domain_id TEXT NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(user_id,domain_id),
            FOREIGN KEY(user_id) REFERENCES web_user(id) ON DELETE CASCADE,
            FOREIGN KEY(domain_id) REFERENCES domains(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_web_user_domain_domain
            ON web_user_domain(domain_id,user_id);"
            .to_string(),
    ))
    .await?;

    let existing = conn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT id FROM web_user WHERE username=?",
            vec![BOOTSTRAP_ADMIN.into()],
        ))
        .await?;
    if existing.is_none() {
        let now = unix_timestamp_ms();
        let password_hash = crate::pbx::user::auth::hash_password("admin")?;
        conn.execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO web_user
             (username,password_hash,role,enabled,version,created_at,updated_at)
             VALUES (?,?,?,1,1,?,?)",
            vec![
                BOOTSTRAP_ADMIN.into(),
                password_hash.into(),
                SYSTEM_ADMIN.into(),
                now.into(),
                now.into(),
            ],
        ))
        .await?;
    }
    Ok(())
}
