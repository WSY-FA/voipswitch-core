mod auth;
mod crud;
mod schema;

use crate::commands::ApiCommandRegistry;
use anyhow::Result;
use sea_orm::DatabaseConnection;
use std::sync::Arc;

pub(crate) fn register_api_commands(registry: &ApiCommandRegistry) {
    registry.register(Arc::new(auth::WebAuthApiCommand));
    registry.register(Arc::new(crud::WebUserApiCommand));
}

pub(crate) async fn ensure_schema_and_bootstrap(conn: &DatabaseConnection) -> Result<()> {
    schema::ensure_schema_and_bootstrap(conn).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_store::seaorm::connect_sqlite;
    use crate::pbx::migration::migrate_system_db;
    use sea_orm::{ConnectionTrait, DbBackend, Statement};

    #[test]
    fn password_hash_round_trip() {
        let hash = auth::hash_password("admin").unwrap();
        assert_ne!(hash, "admin");
        assert!(auth::verify_password(&hash, "admin"));
        assert!(!auth::verify_password(&hash, "wrong"));
    }

    #[tokio::test]
    async fn migration_bootstraps_admin_once() {
        let dir = tempfile::tempdir().unwrap();
        let conn = connect_sqlite(&dir.path().join("system.db")).await.unwrap();
        migrate_system_db(&conn).await.unwrap();
        let first = conn
            .query_one(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT password_hash FROM web_user WHERE username='admin'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<String>("", "password_hash")
            .unwrap();
        assert!(auth::verify_password(&first, "admin"));

        migrate_system_db(&conn).await.unwrap();
        let second = conn
            .query_one(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT password_hash FROM web_user WHERE username='admin'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<String>("", "password_hash")
            .unwrap();
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn domain_name_and_password_create_domain_principal() {
        let dir = tempfile::tempdir().unwrap();
        let conn = connect_sqlite(&dir.path().join("system.db")).await.unwrap();
        migrate_system_db(&conn).await.unwrap();
        conn.execute(Statement::from_string(
            DbBackend::Sqlite,
            "INSERT INTO domains
             (id,name,realm,password,remark,status,db_path,version,updated_at)
             VALUES ('domain-a','tenant-a','tenant-a.example','secret','','enabled',
                     'domains/domain-a/config.db',1,0)"
                .to_string(),
        ))
        .await
        .unwrap();

        let principal = auth::authenticate(&conn, "TENANT-A", "secret")
            .await
            .unwrap();
        assert_eq!(principal["role"], schema::DOMAIN_ADMIN);
        assert_eq!(principal["auth_source"], "domain");
        assert_eq!(principal["domain_ids"], serde_json::json!(["domain-a"]));
        assert!(
            auth::authenticate(&conn, "tenant-a", "wrong")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn web_user_shadows_same_named_domain_login() {
        let dir = tempfile::tempdir().unwrap();
        let conn = connect_sqlite(&dir.path().join("system.db")).await.unwrap();
        migrate_system_db(&conn).await.unwrap();
        conn.execute(Statement::from_string(
            DbBackend::Sqlite,
            "INSERT INTO domains
             (id,name,realm,password,remark,status,db_path,version,updated_at)
             VALUES ('domain-a','admin','tenant-a.example','domain-secret','','enabled',
                     'domains/domain-a/config.db',1,0)"
                .to_string(),
        ))
        .await
        .unwrap();

        assert!(
            auth::authenticate(&conn, "admin", "domain-secret")
                .await
                .is_err()
        );
        let principal = auth::authenticate(&conn, "admin", "admin").await.unwrap();
        assert_eq!(principal["auth_source"], "web_user");
    }
}
