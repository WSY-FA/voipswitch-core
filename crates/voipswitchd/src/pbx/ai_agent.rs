pub(crate) mod model {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct AiAgentConfig {
        pub id: u64,
        pub agent_id: String,
        pub name: String,
        pub service_number: String,
        pub profile_id: String,
        pub enabled: bool,
        pub revision: u64,
        pub fallback_target: Option<String>,
    }
}

pub(crate) mod config {
    use super::model::AiAgentConfig;
    use anyhow::Result;
    use sea_orm::{ConnectionTrait, DbBackend, Statement};
    use voipswitch_core::types::ids::DomainId;

    pub(crate) async fn load_ai_agents<C>(
        conn: &C,
        domain_id: &DomainId,
    ) -> Result<Vec<AiAgentConfig>>
    where
        C: ConnectionTrait,
    {
        let rows = conn
            .query_all(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT id,agent_id,name,service_number,profile_id,enabled,revision,fallback_target
                 FROM ai_agent WHERE domain_id=? ORDER BY service_number,id",
                vec![domain_id.as_str().into()],
            ))
            .await?;
        rows.into_iter()
            .map(|row| {
                Ok(AiAgentConfig {
                    id: row.try_get("", "id")?,
                    agent_id: row.try_get("", "agent_id")?,
                    name: row.try_get("", "name")?,
                    service_number: row.try_get("", "service_number")?,
                    profile_id: row.try_get("", "profile_id")?,
                    enabled: row.try_get::<i64>("", "enabled")? != 0,
                    revision: row.try_get("", "revision")?,
                    fallback_target: row.try_get("", "fallback_target")?,
                })
            })
            .collect()
    }
}
