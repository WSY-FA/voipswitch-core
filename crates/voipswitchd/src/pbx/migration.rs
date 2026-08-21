use crate::config_service::{
    CALL_TRACE_ENABLED_KEY, CDR_SPOOL_REJECT_MB_KEY, CDR_SPOOL_RESUME_MB_KEY,
    CDR_SPOOL_WARNING_MB_KEY, DEFAULT_CDR_SPOOL_REJECT_MB, DEFAULT_CDR_SPOOL_RESUME_MB,
    DEFAULT_CDR_SPOOL_WARNING_MB, DEFAULT_LOG_LEVEL, DEFAULT_RECORDING_MAX_SIZE_GB,
    DEFAULT_RECORDING_RETENTION_DAYS, DEFAULT_SIP_PORT, LOG_LEVEL_KEY, RECORDING_DIR_KEY,
    RECORDING_MAX_SIZE_GB_KEY, RECORDING_RETENTION_DAYS_KEY, SIP_PORT_KEY,
};
use crate::pbx::domain::db::domain_record;
use crate::pbx::global_setting::db as global_setting;
use anyhow::{Result, anyhow};
use sea_orm::sea_query::{ColumnDef, Iden, Table};
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait, Schema, Statement};
use voipswitch_core::types::ids::DomainId;

pub(crate) async fn migrate_system_db(conn: &DatabaseConnection) -> Result<()> {
    create_table(conn, domain_record::Entity).await?;
    create_table(conn, global_setting::Entity).await?;
    ensure_domain_columns(conn).await?;
    ensure_domain_unique_indexes(conn).await?;
    seed_global_settings(conn).await?;
    crate::pbx::user::ensure_schema_and_bootstrap(conn).await?;
    Ok(())
}

async fn seed_global_settings(conn: &DatabaseConnection) -> Result<()> {
    for (key, value, value_type) in [
        (CALL_TRACE_ENABLED_KEY, "true".to_string(), "bool"),
        (SIP_PORT_KEY, DEFAULT_SIP_PORT.to_string(), "integer"),
        (LOG_LEVEL_KEY, DEFAULT_LOG_LEVEL.to_string(), "string"),
        (RECORDING_DIR_KEY, String::new(), "string"),
        (
            RECORDING_RETENTION_DAYS_KEY,
            DEFAULT_RECORDING_RETENTION_DAYS.to_string(),
            "integer",
        ),
        (
            RECORDING_MAX_SIZE_GB_KEY,
            DEFAULT_RECORDING_MAX_SIZE_GB.to_string(),
            "integer",
        ),
        (
            CDR_SPOOL_WARNING_MB_KEY,
            DEFAULT_CDR_SPOOL_WARNING_MB.to_string(),
            "integer",
        ),
        (
            CDR_SPOOL_REJECT_MB_KEY,
            DEFAULT_CDR_SPOOL_REJECT_MB.to_string(),
            "integer",
        ),
        (
            CDR_SPOOL_RESUME_MB_KEY,
            DEFAULT_CDR_SPOOL_RESUME_MB.to_string(),
            "integer",
        ),
    ] {
        conn.execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT OR IGNORE INTO global_settings
             (key, value, value_type, version, updated_at)
             VALUES (?, ?, ?, 1, 0)",
            vec![key.into(), value.into(), value_type.into()],
        ))
        .await?;
    }
    Ok(())
}

pub(crate) async fn migrate_domain_db(
    conn: &DatabaseConnection,
    domain_id: &DomainId,
) -> Result<()> {
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        "PRAGMA foreign_keys = OFF".to_string(),
    ))
    .await?;
    let result = async {
        migrate_extensions(conn, domain_id).await?;
        create_config_id_sequences(conn).await?;
        create_trunk_and_route_tables(conn, domain_id).await?;
        migrate_recording_policy_tables(conn).await?;
        create_ai_policy_tables(conn).await?;
        create_ai_agent_tables(conn).await?;
        create_config_meta(conn).await?;
        seed_config_sequences(conn, domain_id).await?;
        seed_config_version(conn).await?;
        Result::<()>::Ok(())
    }
    .await;
    let restore_result = conn
        .execute(Statement::from_string(
            DbBackend::Sqlite,
            "PRAGMA foreign_keys = ON".to_string(),
        ))
        .await;
    result?;
    restore_result?;
    Ok(())
}

async fn migrate_recording_policy_tables(conn: &DatabaseConnection) -> Result<()> {
    if has_sqlite_table(conn, "recording_policy").await?
        && has_sqlite_column(conn, "recording_policy", "target_type").await?
    {
        conn.execute(Statement::from_string(
            DbBackend::Sqlite,
            "ALTER TABLE recording_policy RENAME TO recording_policy_legacy".to_string(),
        ))
        .await?;
        create_recording_policy_tables(conn).await?;
        conn.execute(Statement::from_string(
            DbBackend::Sqlite,
            "INSERT INTO recording_policy
             (domain_id,id,name,enabled,direction,priority,version,created_at,updated_at)
             SELECT domain_id,id,name,enabled,direction,priority,version,created_at,updated_at
             FROM recording_policy_legacy;
             INSERT INTO recording_policy_target
             (domain_id,policy_id,target_type,target_id,position)
             SELECT domain_id,id,target_type,target_id,0
             FROM recording_policy_legacy;
             DROP TABLE recording_policy_legacy;"
                .to_string(),
        ))
        .await?;
    } else {
        create_recording_policy_tables(conn).await?;
    }
    Ok(())
}

async fn create_recording_policy_tables(conn: &DatabaseConnection) -> Result<()> {
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        "CREATE TABLE IF NOT EXISTS recording_policy (
            domain_id TEXT NOT NULL,
            id INTEGER NOT NULL,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            direction TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 100,
            version INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(domain_id, id),
            CHECK(id > 0),
            CHECK(direction IN ('inbound','outbound','both'))
        );
        CREATE TABLE IF NOT EXISTS recording_policy_target (
            domain_id TEXT NOT NULL,
            policy_id INTEGER NOT NULL,
            target_type TEXT NOT NULL,
            target_id INTEGER NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(domain_id,policy_id,target_type,target_id),
            FOREIGN KEY(domain_id,policy_id)
                REFERENCES recording_policy(domain_id,id) ON DELETE CASCADE,
            CHECK(target_id > 0),
            CHECK(target_type IN ('extension','peer_trunk','reg_trunk'))
        );
        CREATE INDEX IF NOT EXISTS idx_recording_policy_match
            ON recording_policy(domain_id, enabled, direction);
        CREATE INDEX IF NOT EXISTS idx_recording_policy_target_match
            ON recording_policy_target(domain_id,target_type,target_id,policy_id);"
            .to_string(),
    ))
    .await?;
    Ok(())
}

async fn create_ai_policy_tables(conn: &DatabaseConnection) -> Result<()> {
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        "CREATE TABLE IF NOT EXISTS ai_policy (
            domain_id TEXT NOT NULL,
            id INTEGER NOT NULL,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            direction TEXT NOT NULL DEFAULT 'any',
            priority INTEGER NOT NULL DEFAULT 100,
            ai_profile_id TEXT NOT NULL,
            version INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(domain_id,id),
            CHECK(id > 0),
            CHECK(direction IN ('any','internal','inbound','outbound'))
        );
        CREATE TABLE IF NOT EXISTS ai_policy_target (
            domain_id TEXT NOT NULL,
            policy_id INTEGER NOT NULL,
            target_type TEXT NOT NULL,
            target_id INTEGER NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(domain_id,policy_id,target_type,target_id),
            FOREIGN KEY(domain_id,policy_id)
                REFERENCES ai_policy(domain_id,id) ON DELETE CASCADE,
            CHECK(target_id > 0),
            CHECK(target_type IN ('extension','peer_trunk','reg_trunk'))
        );
        CREATE INDEX IF NOT EXISTS idx_ai_policy_match
            ON ai_policy(domain_id,enabled,direction,priority,id);
        CREATE INDEX IF NOT EXISTS idx_ai_policy_target_match
            ON ai_policy_target(domain_id,target_type,target_id,policy_id);"
            .to_string(),
    ))
    .await?;
    Ok(())
}

async fn create_ai_agent_tables(conn: &DatabaseConnection) -> Result<()> {
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        "CREATE TABLE IF NOT EXISTS ai_agent (
            domain_id TEXT NOT NULL,
            id INTEGER NOT NULL,
            agent_id TEXT NOT NULL,
            name TEXT NOT NULL,
            service_number TEXT NOT NULL,
            profile_id TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            revision INTEGER NOT NULL DEFAULT 1,
            fallback_target TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(domain_id,id),
            UNIQUE(domain_id,agent_id),
            UNIQUE(domain_id,service_number),
            CHECK(id > 0),
            CHECK(length(agent_id) BETWEEN 1 AND 64),
            CHECK(length(service_number) BETWEEN 1 AND 32)
        );
        CREATE INDEX IF NOT EXISTS idx_ai_agent_number
            ON ai_agent(domain_id,enabled,service_number);"
            .to_string(),
    ))
    .await?;
    Ok(())
}

async fn create_table<E>(conn: &DatabaseConnection, entity: E) -> Result<()>
where
    E: EntityTrait,
{
    let backend = conn.get_database_backend();
    let schema = Schema::new(backend);
    let mut statement = schema.create_table_from_entity(entity);
    statement.if_not_exists();
    conn.execute(backend.build(&statement)).await?;
    Ok(())
}

async fn create_config_meta(conn: &DatabaseConnection) -> Result<()> {
    let backend = conn.get_database_backend();
    let mut statement = Table::create();
    statement
        .table(ConfigMeta::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(ConfigMeta::Key)
                .string()
                .not_null()
                .primary_key(),
        )
        .col(ColumnDef::new(ConfigMeta::Value).string().not_null());
    conn.execute(backend.build(&statement)).await?;
    Ok(())
}

async fn migrate_extensions(conn: &DatabaseConnection, domain_id: &DomainId) -> Result<()> {
    let source_id = if has_sqlite_column(conn, "extensions", "id").await? {
        "id"
    } else {
        "rowid"
    };
    let source_expressions = format!("{source_id},number,auth_user,password,enabled,'',0,0");
    ensure_domain_table(
        conn,
        domain_id,
        "extensions",
        "domain_id TEXT NOT NULL,
         id INTEGER NOT NULL,
         number TEXT NOT NULL,
         auth_user TEXT NOT NULL,
         password TEXT NOT NULL,
         enabled INTEGER NOT NULL DEFAULT 1,
         note TEXT NOT NULL DEFAULT '',
         created_at INTEGER NOT NULL,
         updated_at INTEGER NOT NULL,
         PRIMARY KEY(domain_id, id),
         UNIQUE(domain_id, number),
         UNIQUE(domain_id, auth_user),
         CHECK(id > 0)",
        "id,number,auth_user,password,enabled,note,created_at,updated_at",
        &source_expressions,
    )
    .await?;
    execute_statements(
        conn,
        &["CREATE INDEX IF NOT EXISTS idx_extensions_domain_enabled
           ON extensions(domain_id, enabled)"],
    )
    .await?;
    Ok(())
}

async fn create_config_id_sequences(conn: &DatabaseConnection) -> Result<()> {
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        "CREATE TABLE IF NOT EXISTS config_id_sequences (
            domain_id TEXT NOT NULL,
            resource_type TEXT NOT NULL,
            next_id INTEGER NOT NULL,
            PRIMARY KEY(domain_id, resource_type),
            CHECK(next_id > 0)
        )"
        .to_string(),
    ))
    .await?;
    Ok(())
}

async fn create_trunk_and_route_tables(
    conn: &DatabaseConnection,
    domain_id: &DomainId,
) -> Result<()> {
    ensure_domain_table(
        conn,
        domain_id,
        "peer_trunk",
        "domain_id TEXT NOT NULL,
            id INTEGER NOT NULL,
            name TEXT NOT NULL,
            server_host TEXT NOT NULL,
            server_port INTEGER NOT NULL DEFAULT 5060,
            outbound_proxy_host TEXT,
            outbound_proxy_port INTEGER,
            transport TEXT NOT NULL DEFAULT 'udp',
            keep_alive_seconds INTEGER NOT NULL DEFAULT 60,
            enabled INTEGER NOT NULL DEFAULT 1,
            note TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(domain_id, id),
            UNIQUE(domain_id, name),
            CHECK(id > 0),
            CHECK(server_port BETWEEN 1 AND 65535),
            CHECK(outbound_proxy_port IS NULL OR outbound_proxy_port BETWEEN 1 AND 65535),
            CHECK(transport IN ('udp', 'tcp')),
            CHECK(keep_alive_seconds = 0 OR keep_alive_seconds BETWEEN 10 AND 3600)",
        "id,name,server_host,server_port,outbound_proxy_host,outbound_proxy_port,transport,keep_alive_seconds,enabled,note,created_at,updated_at",
        "id,name,server_host,server_port,outbound_proxy_host,outbound_proxy_port,transport,keep_alive_seconds,enabled,note,created_at,updated_at",
    )
    .await?;
    ensure_domain_table(
        conn,
        domain_id,
        "reg_trunk",
        "domain_id TEXT NOT NULL,
            id INTEGER NOT NULL,
            name TEXT NOT NULL,
            server_host TEXT NOT NULL,
            server_port INTEGER NOT NULL DEFAULT 5060,
            outbound_proxy_host TEXT,
            outbound_proxy_port INTEGER,
            transport TEXT NOT NULL DEFAULT 'udp',
            keep_alive_seconds INTEGER NOT NULL DEFAULT 60,
            requested_expires_seconds INTEGER NOT NULL DEFAULT 300,
            enabled INTEGER NOT NULL DEFAULT 1,
            note TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(domain_id, id),
            UNIQUE(domain_id, name),
            CHECK(id > 0),
            CHECK(server_port BETWEEN 1 AND 65535),
            CHECK(outbound_proxy_port IS NULL OR outbound_proxy_port BETWEEN 1 AND 65535),
            CHECK(transport IN ('udp', 'tcp')),
            CHECK(keep_alive_seconds = 0 OR keep_alive_seconds BETWEEN 10 AND 3600),
            CHECK(requested_expires_seconds BETWEEN 30 AND 3600)",
        "id,name,server_host,server_port,outbound_proxy_host,outbound_proxy_port,transport,keep_alive_seconds,requested_expires_seconds,enabled,note,created_at,updated_at",
        "id,name,server_host,server_port,outbound_proxy_host,outbound_proxy_port,transport,keep_alive_seconds,requested_expires_seconds,enabled,note,created_at,updated_at",
    )
    .await?;
    ensure_domain_table(
        conn,
        domain_id,
        "reg_account",
        "domain_id TEXT NOT NULL,
            id INTEGER NOT NULL,
            reg_trunk_id INTEGER NOT NULL,
            auth_name TEXT NOT NULL,
            auth_pwd TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            note TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(domain_id, id),
            UNIQUE(domain_id, reg_trunk_id, auth_name),
            CHECK(id > 0),
            FOREIGN KEY(domain_id, reg_trunk_id)
                REFERENCES reg_trunk(domain_id, id)
                ON UPDATE CASCADE
                ON DELETE RESTRICT",
        "id,reg_trunk_id,auth_name,auth_pwd,enabled,note,created_at,updated_at",
        "id,reg_trunk_id,auth_name,auth_pwd,enabled,note,created_at,updated_at",
    )
    .await?;
    ensure_domain_table(
        conn,
        domain_id,
        "outbound_route",
        "domain_id TEXT NOT NULL,
            id INTEGER NOT NULL,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            dst_pattern TEXT NOT NULL,
            src_pattern TEXT,
            dst_strip INTEGER NOT NULL DEFAULT 0,
            dst_prefix TEXT NOT NULL DEFAULT '',
            dst_suffix TEXT NOT NULL DEFAULT '',
            src_strip INTEGER NOT NULL DEFAULT 0,
            src_prefix TEXT NOT NULL DEFAULT '',
            src_suffix TEXT NOT NULL DEFAULT '',
            priority INTEGER NOT NULL DEFAULT 100,
            note TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(domain_id, id),
            UNIQUE(domain_id, name),
            CHECK(id > 0),
            CHECK(dst_strip BETWEEN 0 AND 32),
            CHECK(src_strip BETWEEN 0 AND 32),
            CHECK(priority BETWEEN 0 AND 10000)",
        "id,name,enabled,dst_pattern,src_pattern,dst_strip,dst_prefix,dst_suffix,src_strip,src_prefix,src_suffix,priority,note,created_at,updated_at",
        "id,name,enabled,dst_pattern,src_pattern,dst_strip,dst_prefix,dst_suffix,src_strip,src_prefix,src_suffix,priority,note,created_at,updated_at",
    )
    .await?;
    ensure_domain_table(
        conn,
        domain_id,
        "outbound_route_trunks",
        "domain_id TEXT NOT NULL,
            route_id INTEGER NOT NULL,
            trunk_ref TEXT NOT NULL,
            position INTEGER NOT NULL,
            PRIMARY KEY(domain_id, route_id, trunk_ref),
            UNIQUE(domain_id, route_id, position),
            CHECK(position >= 0),
            FOREIGN KEY(domain_id, route_id)
                REFERENCES outbound_route(domain_id, id)
                ON UPDATE CASCADE
                ON DELETE CASCADE",
        "route_id,trunk_ref,position",
        "route_id,trunk_ref,position",
    )
    .await?;
    ensure_domain_table(
        conn,
        domain_id,
        "inbound_route",
        "domain_id TEXT NOT NULL,
            id INTEGER NOT NULL,
            name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            trunk_match TEXT NOT NULL DEFAULT '',
            dst_pattern TEXT NOT NULL,
            src_pattern TEXT,
            dst_strip INTEGER NOT NULL DEFAULT 0,
            dst_prefix TEXT NOT NULL DEFAULT '',
            dst_suffix TEXT NOT NULL DEFAULT '',
            src_strip INTEGER NOT NULL DEFAULT 0,
            src_prefix TEXT NOT NULL DEFAULT '',
            src_suffix TEXT NOT NULL DEFAULT '',
            target TEXT NOT NULL DEFAULT 'rej',
            priority INTEGER NOT NULL DEFAULT 100,
            note TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(domain_id, id),
            UNIQUE(domain_id, name),
            CHECK(id > 0),
            CHECK(dst_strip BETWEEN 0 AND 32),
            CHECK(src_strip BETWEEN 0 AND 32),
            CHECK(priority BETWEEN 0 AND 10000)",
        "id,name,enabled,trunk_match,dst_pattern,src_pattern,dst_strip,dst_prefix,dst_suffix,src_strip,src_prefix,src_suffix,target,priority,note,created_at,updated_at",
        "id,name,enabled,trunk_match,dst_pattern,src_pattern,dst_strip,dst_prefix,dst_suffix,src_strip,src_prefix,src_suffix,target,priority,note,created_at,updated_at",
    )
    .await?;

    execute_statements(
        conn,
        &[
            "CREATE INDEX IF NOT EXISTS idx_peer_trunk_domain_enabled
             ON peer_trunk(domain_id, enabled)",
            "CREATE INDEX IF NOT EXISTS idx_reg_trunk_domain_enabled
             ON reg_trunk(domain_id, enabled)",
            "CREATE INDEX IF NOT EXISTS idx_reg_account_trunk_enabled
             ON reg_account(domain_id, reg_trunk_id, enabled)",
            "CREATE INDEX IF NOT EXISTS idx_outbound_route_domain_enabled
             ON outbound_route(domain_id, enabled)",
            "CREATE INDEX IF NOT EXISTS idx_outbound_route_trunks_order
             ON outbound_route_trunks(domain_id, route_id, position)",
            "CREATE INDEX IF NOT EXISTS idx_inbound_route_match
             ON inbound_route(domain_id, trunk_match, enabled)",
        ],
    )
    .await?;
    Ok(())
}

async fn execute_statements(conn: &DatabaseConnection, statements: &[&str]) -> Result<()> {
    for statement in statements {
        conn.execute(Statement::from_string(
            DbBackend::Sqlite,
            (*statement).to_string(),
        ))
        .await?;
    }
    Ok(())
}

async fn ensure_domain_table(
    conn: &DatabaseConnection,
    domain_id: &DomainId,
    table: &str,
    schema: &str,
    destination_columns: &str,
    source_expressions: &str,
) -> Result<()> {
    if !has_sqlite_table(conn, table).await? {
        conn.execute(Statement::from_string(
            DbBackend::Sqlite,
            format!("CREATE TABLE {table} ({schema})"),
        ))
        .await?;
        return Ok(());
    }
    if has_sqlite_column(conn, table, "domain_id").await? {
        return Ok(());
    }

    let migrated = format!("{table}__domain_migration");
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        format!("DROP TABLE IF EXISTS {migrated}"),
    ))
    .await?;
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        format!("CREATE TABLE {migrated} ({schema})"),
    ))
    .await?;
    conn.execute(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        format!(
            "INSERT INTO {migrated} (domain_id,{destination_columns})
             SELECT ?,{source_expressions} FROM {table}"
        ),
        vec![domain_id.as_str().into()],
    ))
    .await?;
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        format!("DROP TABLE {table}"),
    ))
    .await?;
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        format!("ALTER TABLE {migrated} RENAME TO {table}"),
    ))
    .await?;
    Ok(())
}

async fn has_sqlite_table(conn: &DatabaseConnection, table: &str) -> Result<bool> {
    let row = conn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            vec![table.into()],
        ))
        .await?;
    Ok(row.is_some())
}

async fn seed_config_sequences(conn: &DatabaseConnection, domain_id: &DomainId) -> Result<()> {
    for (resource_type, table) in [
        ("extension", "extensions"),
        ("peer_trunk", "peer_trunk"),
        ("reg_trunk", "reg_trunk"),
        ("reg_account", "reg_account"),
        ("inbound_route", "inbound_route"),
        ("outbound_route", "outbound_route"),
        ("recording_policy", "recording_policy"),
        ("ai_policy", "ai_policy"),
    ] {
        let sql = format!(
            "INSERT INTO config_id_sequences (domain_id, resource_type, next_id)
             VALUES (?, ?, COALESCE((SELECT MAX(id) FROM {table} WHERE domain_id=?), 0) + 1)
             ON CONFLICT(domain_id, resource_type) DO UPDATE
             SET next_id = MAX(config_id_sequences.next_id, excluded.next_id)"
        );
        conn.execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            sql,
            vec![
                domain_id.as_str().into(),
                resource_type.into(),
                domain_id.as_str().into(),
            ],
        ))
        .await?;
    }
    Ok(())
}

async fn seed_config_version(conn: &DatabaseConnection) -> Result<()> {
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        "INSERT OR IGNORE INTO config_meta (key, value) VALUES ('version', '1')".to_string(),
    ))
    .await?;
    Ok(())
}

async fn ensure_domain_columns(conn: &DatabaseConnection) -> Result<()> {
    ensure_column_if_missing(
        conn,
        "domains",
        "password",
        "ALTER TABLE domains ADD COLUMN password TEXT NOT NULL DEFAULT ''",
    )
    .await?;
    ensure_column_if_missing(
        conn,
        "domains",
        "remark",
        "ALTER TABLE domains ADD COLUMN remark TEXT NOT NULL DEFAULT ''",
    )
    .await?;
    Ok(())
}

async fn ensure_domain_unique_indexes(conn: &DatabaseConnection) -> Result<()> {
    for (column, index) in [
        ("name", "idx_domains_name_unique"),
        ("realm", "idx_domains_realm_unique"),
    ] {
        if let Some(row) = conn
            .query_one(Statement::from_string(
                DbBackend::Sqlite,
                format!(
                    "SELECT {column} AS value, COUNT(*) AS count
                     FROM domains
                     GROUP BY {column} COLLATE NOCASE
                     HAVING COUNT(*) > 1
                     LIMIT 1"
                ),
            ))
            .await?
        {
            let value: String = row.try_get("", "value")?;
            return Err(anyhow!(
                "MIGRATION_CONFLICT: duplicate domain {column}: {value}"
            ));
        }
        conn.execute(Statement::from_string(
            DbBackend::Sqlite,
            format!(
                "CREATE UNIQUE INDEX IF NOT EXISTS {index}
                 ON domains({column} COLLATE NOCASE)"
            ),
        ))
        .await?;
    }
    Ok(())
}

async fn ensure_column_if_missing(
    conn: &DatabaseConnection,
    table: &str,
    column: &str,
    sql: &str,
) -> Result<()> {
    if has_sqlite_column(conn, table, column).await? {
        return Ok(());
    }
    conn.execute(Statement::from_string(DbBackend::Sqlite, sql.to_string()))
        .await?;
    Ok(())
}

async fn has_sqlite_column(conn: &DatabaseConnection, table: &str, column: &str) -> Result<bool> {
    let rows = conn
        .query_all(Statement::from_string(
            DbBackend::Sqlite,
            format!("PRAGMA table_info({table})"),
        ))
        .await?;
    Ok(rows.iter().any(|row| {
        row.try_get::<String>("", "name")
            .map(|name| name == column)
            .unwrap_or(false)
    }))
}

enum ConfigMeta {
    Table,
    Key,
    Value,
}

impl Iden for ConfigMeta {
    fn unquoted(&self, output: &mut dyn std::fmt::Write) {
        let name = match self {
            Self::Table => "config_meta",
            Self::Key => "key",
            Self::Value => "value",
        };
        write!(output, "{name}").expect("write identifier");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_store::seaorm::connect_sqlite;
    use sea_orm::TransactionTrait;
    use tempfile::tempdir;

    #[tokio::test]
    async fn creates_domain_ai_policy_tables_and_sequence() -> Result<()> {
        let temp = tempdir()?;
        let conn = connect_sqlite(&temp.path().join("domain.db")).await?;
        let domain_id = DomainId::from("domain-ai");
        migrate_domain_db(&conn, &domain_id).await?;
        assert!(has_sqlite_table(&conn, "ai_policy").await?);
        assert!(has_sqlite_table(&conn, "ai_policy_target").await?);
        let row = conn
            .query_one(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT next_id FROM config_id_sequences
                 WHERE domain_id=? AND resource_type='ai_policy'",
                vec![domain_id.as_str().into()],
            ))
            .await?
            .ok_or_else(|| anyhow::anyhow!("AI policy sequence missing"))?;
        assert_eq!(row.try_get::<i64>("", "next_id")?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn system_domain_name_and_realm_are_unique() -> Result<()> {
        let temp = tempdir()?;
        let conn = connect_sqlite(&temp.path().join("system.db")).await?;
        migrate_system_db(&conn).await?;
        conn.execute(Statement::from_string(
            DbBackend::Sqlite,
            "INSERT INTO domains
             (id,name,realm,password,remark,status,db_path,version,updated_at)
             VALUES ('domain-a','tenant-a','tenant-a.example','secret','','enabled','a.db',1,0)"
                .to_string(),
        ))
        .await?;

        let duplicate_name = conn
            .execute(Statement::from_string(
                DbBackend::Sqlite,
                "INSERT INTO domains
                 (id,name,realm,password,remark,status,db_path,version,updated_at)
                 VALUES ('domain-b','TENANT-A','tenant-b.example','secret','','enabled','b.db',1,0)"
                    .to_string(),
            ))
            .await;
        assert!(duplicate_name.is_err());

        let duplicate_realm = conn
            .execute(Statement::from_string(
                DbBackend::Sqlite,
                "INSERT INTO domains
                 (id,name,realm,password,remark,status,db_path,version,updated_at)
                 VALUES ('domain-c','tenant-c','TENANT-A.EXAMPLE','secret','','enabled','c.db',1,0)"
                    .to_string(),
            ))
            .await;
        assert!(duplicate_realm.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn migrates_legacy_extension_table_into_domain_scoped_schema() -> Result<()> {
        let temp = tempdir()?;
        let conn = connect_sqlite(&temp.path().join("domain.db")).await?;
        conn.execute(Statement::from_string(
            DbBackend::Sqlite,
            "CREATE TABLE extensions (
                number TEXT PRIMARY KEY,
                auth_user TEXT NOT NULL,
                password TEXT NOT NULL,
                enabled INTEGER NOT NULL
            )"
            .to_string(),
        ))
        .await?;
        conn.execute(Statement::from_string(
            DbBackend::Sqlite,
            "INSERT INTO extensions (number, auth_user, password, enabled)
             VALUES ('1001', '1001', 'secret', 1)"
                .to_string(),
        ))
        .await?;

        let domain_id = DomainId::from("domain-a");
        migrate_domain_db(&conn, &domain_id).await?;

        let row = conn
            .query_one(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT domain_id, id, number FROM extensions".to_string(),
            ))
            .await?
            .expect("migrated extension");
        assert_eq!(row.try_get::<String>("", "domain_id")?, "domain-a");
        assert_eq!(row.try_get::<i64>("", "id")?, 1);
        assert_eq!(row.try_get::<String>("", "number")?, "1001");
        assert_eq!(sequence_next_id(&conn, &domain_id, "extension").await?, 2);
        assert_eq!(pragma_foreign_keys(&conn).await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn migrates_single_target_recording_policy_to_target_rows() -> Result<()> {
        let temp = tempdir()?;
        let conn = connect_sqlite(&temp.path().join("domain.db")).await?;
        conn.execute(Statement::from_string(
            DbBackend::Sqlite,
            "CREATE TABLE recording_policy (
                domain_id TEXT NOT NULL,
                id INTEGER NOT NULL,
                name TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                target_type TEXT NOT NULL,
                target_id INTEGER NOT NULL,
                direction TEXT NOT NULL,
                priority INTEGER NOT NULL,
                version INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(domain_id,id)
            )"
            .to_string(),
        ))
        .await?;
        conn.execute(Statement::from_string(
            DbBackend::Sqlite,
            "INSERT INTO recording_policy
             (domain_id,id,name,enabled,target_type,target_id,direction,priority,version,created_at,updated_at)
             VALUES ('domain-a',7,'legacy',1,'extension',3,'both',100,1,10,10)"
                .to_string(),
        ))
        .await?;

        let domain_id = DomainId::from("domain-a");
        migrate_domain_db(&conn, &domain_id).await?;

        assert!(!has_sqlite_column(&conn, "recording_policy", "target_type").await?);
        let row = conn
            .query_one(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT target_type,target_id,position
                 FROM recording_policy_target
                 WHERE domain_id='domain-a' AND policy_id=7"
                    .to_string(),
            ))
            .await?
            .expect("migrated recording policy target");
        assert_eq!(row.try_get::<String>("", "target_type")?, "extension");
        assert_eq!(row.try_get::<i64>("", "target_id")?, 3);
        assert_eq!(row.try_get::<i64>("", "position")?, 0);
        assert!(!has_sqlite_table(&conn, "recording_policy_legacy").await?);
        Ok(())
    }

    #[tokio::test]
    async fn business_ids_and_unique_keys_are_scoped_by_domain() -> Result<()> {
        let temp = tempdir()?;
        let conn = connect_sqlite(&temp.path().join("domain.db")).await?;
        let domain_a = DomainId::from("domain-a");
        let domain_b = DomainId::from("domain-b");
        migrate_domain_db(&conn, &domain_a).await?;
        insert_extension(&conn, &domain_a, 1, "1001").await?;
        migrate_domain_db(&conn, &domain_a).await?;

        migrate_domain_db(&conn, &domain_b).await?;
        insert_extension(&conn, &domain_b, 1, "1001").await?;

        let row = conn
            .query_one(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT COUNT(*) AS count FROM extensions WHERE id=1 AND number='1001'".to_string(),
            ))
            .await?
            .expect("count row");
        assert_eq!(row.try_get::<i64>("", "count")?, 2);
        assert_eq!(sequence_next_id(&conn, &domain_a, "extension").await?, 2);
        assert_eq!(sequence_next_id(&conn, &domain_b, "extension").await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn reg_account_requires_parent_in_same_domain() -> Result<()> {
        let temp = tempdir()?;
        let conn = connect_sqlite(&temp.path().join("domain.db")).await?;
        let domain_id = DomainId::from("domain-a");
        migrate_domain_db(&conn, &domain_id).await?;

        let result = conn
            .execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO reg_account
                 (domain_id,id,reg_trunk_id,auth_name,auth_pwd,enabled,note,created_at,updated_at)
                 VALUES (?,?,?,?,?,1,'',0,0)",
                vec![
                    domain_id.as_str().into(),
                    1_i64.into(),
                    9_i64.into(),
                    "user".into(),
                    "secret".into(),
                ],
            ))
            .await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn rollback_preserves_sequence_and_config_version() -> Result<()> {
        let temp = tempdir()?;
        let conn = connect_sqlite(&temp.path().join("domain.db")).await?;
        let domain_id = DomainId::from("domain-a");
        migrate_domain_db(&conn, &domain_id).await?;

        let txn = conn.begin().await?;
        let allocated = txn
            .query_one(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT INTO config_id_sequences (domain_id,resource_type,next_id) VALUES (?,?,2)
                 ON CONFLICT(domain_id,resource_type)
                 DO UPDATE SET next_id=config_id_sequences.next_id+1
                 RETURNING next_id-1 AS allocated_id",
                vec![domain_id.as_str().into(), "extension".into()],
            ))
            .await?
            .expect("allocated id")
            .try_get::<i64>("", "allocated_id")?;
        insert_extension(&txn, &domain_id, allocated, "1001").await?;
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;

        let duplicate = insert_extension(&txn, &domain_id, allocated + 1, "1001").await;
        assert!(duplicate.is_err());
        txn.rollback().await?;

        assert_eq!(sequence_next_id(&conn, &domain_id, "extension").await?, 1);
        assert_eq!(
            crate::pbx::domain::config::load_domain_config_version(&conn).await?,
            1
        );
        let row = conn
            .query_one(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT COUNT(*) AS count FROM extensions".to_string(),
            ))
            .await?
            .expect("count row");
        assert_eq!(row.try_get::<i64>("", "count")?, 0);
        Ok(())
    }

    async fn insert_extension<C>(
        conn: &C,
        domain_id: &DomainId,
        id: i64,
        number: &str,
    ) -> Result<()>
    where
        C: ConnectionTrait,
    {
        conn.execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO extensions
             (domain_id,id,number,auth_user,password,enabled,note,created_at,updated_at)
             VALUES (?,?,?,?,?,1,'',0,0)",
            vec![
                domain_id.as_str().into(),
                id.into(),
                number.into(),
                number.into(),
                "secret".into(),
            ],
        ))
        .await?;
        Ok(())
    }

    async fn sequence_next_id<C>(conn: &C, domain_id: &DomainId, resource_type: &str) -> Result<i64>
    where
        C: ConnectionTrait,
    {
        let row = conn
            .query_one(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT next_id FROM config_id_sequences
                 WHERE domain_id=? AND resource_type=?",
                vec![domain_id.as_str().into(), resource_type.into()],
            ))
            .await?
            .expect("sequence row");
        Ok(row.try_get("", "next_id")?)
    }

    async fn pragma_foreign_keys<C>(conn: &C) -> Result<i64>
    where
        C: ConnectionTrait,
    {
        let row = conn
            .query_one(Statement::from_string(
                DbBackend::Sqlite,
                "PRAGMA foreign_keys".to_string(),
            ))
            .await?
            .expect("pragma row");
        Ok(row.try_get("", "foreign_keys")?)
    }
}
