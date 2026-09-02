pub(crate) mod model {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct AiAgentConfig {
        pub id: u64,
        pub agent_id: String,
        pub name: String,
        pub service_number: String,
        pub profile_id: String,
        pub welcome_text: Option<String>,
        pub welcome_audio: Option<Vec<u8>>,
        pub welcome_language: String,
        pub enabled: bool,
        pub revision: u64,
        pub fallback_target: Option<String>,
    }
}

pub(crate) fn register_vc_config_table(
    registry: &mut crate::pbx::vc_config::VcConfigTableRegistry,
) {
    config::register_vc_config_table(registry);
}

pub(crate) fn register_analysis(
    registry: &voipswitch_core::analysis::AnalysisRegistry,
    state: crate::app::AppState,
) {
    use std::sync::Arc;
    use voipswitch_core::analysis::{
        AnalysisPhase, BusinessModuleRoute, NumberAnalysisRequest, NumberAnalyzer, RouteDecision,
        RouteResult,
    };

    struct AiAgentNumberAnalyzer {
        state: crate::app::AppState,
    }

    impl NumberAnalyzer for AiAgentNumberAnalyzer {
        fn name(&self) -> &str {
            "ai-agent-number"
        }

        fn phase(&self) -> AnalysisPhase {
            AnalysisPhase::CalleeBusinessAnalysis
        }

        fn priority(&self) -> i32 {
            100
        }

        fn analyze(&self, request: &NumberAnalysisRequest) -> RouteDecision {
            let snapshot = self.state.config().snapshot();
            let Some(domain) = snapshot.domains.get(&request.domain_id) else {
                return RouteDecision::Continue;
            };
            let Some(agent) = domain
                .ai_agents
                .iter()
                .find(|agent| agent.enabled && agent.service_number == request.callee)
            else {
                return RouteDecision::Continue;
            };
            RouteDecision::Matched(Box::new(RouteResult::BusinessModule(BusinessModuleRoute {
                domain_id: request.domain_id.clone(),
                module: "ai_agent".to_string(),
                target_id: agent.agent_id.clone(),
                original_caller: request.caller.clone(),
                original_callee: request.callee.clone(),
                transformed_caller: request.caller.clone(),
                transformed_callee: request.callee.clone(),
                callee_session_plan: None,
            })))
        }
    }

    registry.register(Arc::new(AiAgentNumberAnalyzer { state }));
}

pub(crate) mod config {
    use super::model::AiAgentConfig;
    use crate::app::AppState;
    use crate::pbx::command_helpers::{
        decoded_set, required_domain, selected_keys, split_conditions, with_seaorm_backend,
    };
    use crate::pbx::vc_config::{VcConfigTableHandler, VcConfigTableRegistry};
    use anyhow::{Context, Result, anyhow, ensure};
    use base64::Engine;
    use sea_orm::{ConnectionTrait, DatabaseTransaction, DbBackend, Statement, TransactionTrait};
    use serde_json::{Map, Value, json};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use voipswitch_core::command_service::ConfigCommand;
    use voipswitch_core::types::ids::DomainId;
    use voipswitch_core::types::time::unix_timestamp_ms;

    pub(crate) fn register_vc_config_table(registry: &mut VcConfigTableRegistry) {
        registry.register(Arc::new(AiAgentVcConfigTable));
    }

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
                "SELECT id,agent_id,name,service_number,profile_id,welcome_text,welcome_audio,welcome_language,enabled,revision,fallback_target
                 FROM ai_agent WHERE domain_id=? ORDER BY service_number,id",
                vec![domain_id.as_str().into()],
            ))
            .await?;
        rows.into_iter()
            .map(|row| {
                Ok(AiAgentConfig {
                    id: u64::try_from(row.try_get::<i64>("", "id")?)
                        .context("AI Agent id is negative")?,
                    agent_id: row.try_get("", "agent_id")?,
                    name: row.try_get("", "name")?,
                    service_number: row.try_get("", "service_number")?,
                    profile_id: row.try_get("", "profile_id")?,
                    welcome_text: row.try_get("", "welcome_text")?,
                    welcome_audio: row.try_get("", "welcome_audio")?,
                    welcome_language: row
                        .try_get::<Option<String>>("", "welcome_language")?
                        .unwrap_or_else(|| "zh-CN".to_string()),
                    enabled: row.try_get::<i64>("", "enabled")? != 0,
                    revision: u64::try_from(row.try_get::<i64>("", "revision")?)
                        .context("AI Agent revision is negative")?,
                    fallback_target: row.try_get("", "fallback_target")?,
                })
            })
            .collect()
    }

    struct AiAgentVcConfigTable;

    impl VcConfigTableHandler for AiAgentVcConfigTable {
        fn table(&self) -> &str {
            "ai_agent"
        }

        fn handle(&self, state: &AppState, command: &ConfigCommand) -> Result<Value> {
            let domain_id = required_domain(command)?;
            let rows = state
                .config()
                .snapshot()
                .domains
                .get(&domain_id)
                .map(|domain| domain.ai_agents.clone())
                .unwrap_or_default();
            match command.action.as_str() {
                "select" => {
                    let id = id_condition(command, false)?;
                    let keys = selected_keys(
                        command,
                        &[
                            "domain_id",
                            "id",
                            "agent_id",
                            "name",
                            "service_number",
                            "profile_id",
                            "welcome_text",
                            "welcome_audio",
                            "welcome_language",
                            "enabled",
                            "revision",
                            "fallback_target",
                        ],
                    );
                    Ok(json!({
                        "table": self.table(), "domain_id": domain_id,
                        "rows": rows.into_iter().filter(|row| id.is_none_or(|id| row.id == id))
                            .map(|row| project(&domain_id, &row, &keys)).collect::<Vec<_>>(),
                    }))
                }
                "insert" => {
                    let set = decoded_set(command)?;
                    ensure!(
                        !set.contains_key("id"),
                        "id is server-assigned for ai_agent insert"
                    );
                    let mut agent = agent_from_set(&set, None)?;
                    prepare_welcome(state, &mut agent)?;
                    persist_welcome_audio(state, &domain_id, &agent)?;
                    validate_agent(state, &domain_id, &agent)?;
                    let id = with_seaorm_backend(state, |backend| {
                        backend.block_on(insert_agent(backend, &domain_id, &agent))
                    })?;
                    reload(state)?;
                    Ok(json!({"table": self.table(), "domain_id": domain_id, "id": id}))
                }
                "update" => {
                    let id = id_condition(command, true)?.expect("required id");
                    let existing = rows
                        .into_iter()
                        .find(|row| row.id == id)
                        .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: ai_agent id {id}"))?;
                    let set = decoded_set(command)?;
                    let previous_profile = existing.profile_id.clone();
                    let mut agent = agent_from_set(&set, Some(existing))?;
                    if agent.profile_id != previous_profile {
                        agent.welcome_audio = None;
                    }
                    prepare_welcome(state, &mut agent)?;
                    persist_welcome_audio(state, &domain_id, &agent)?;
                    validate_agent(state, &domain_id, &agent)?;
                    with_seaorm_backend(state, |backend| {
                        backend.block_on(update_agent(backend, &domain_id, &agent))
                    })?;
                    reload(state)?;
                    Ok(
                        json!({"table": self.table(), "domain_id": domain_id, "id": id, "updated": 1}),
                    )
                }
                "delete" => {
                    let id = id_condition(command, true)?.expect("required id");
                    with_seaorm_backend(state, |backend| {
                        backend.block_on(delete_agent(backend, &domain_id, id))
                    })?;
                    reload(state)?;
                    Ok(
                        json!({"table": self.table(), "domain_id": domain_id, "id": id, "deleted": 1}),
                    )
                }
                action => Err(anyhow!(
                    "unsupported vc config action for ai_agent: {action}"
                )),
            }
        }
    }

    fn agent_from_set(
        set: &BTreeMap<String, String>,
        existing: Option<AiAgentConfig>,
    ) -> Result<AiAgentConfig> {
        let agent_id = required_string(
            set,
            existing.as_ref().map(|item| &item.agent_id),
            "agent_id",
        )?;
        let name = required_string(set, existing.as_ref().map(|item| &item.name), "name")?;
        let service_number = required_string(
            set,
            existing.as_ref().map(|item| &item.service_number),
            "service_number",
        )?;
        ensure!(
            service_number.len() <= 32 && service_number.bytes().all(|byte| byte.is_ascii_digit()),
            "service_number must contain 1..=32 ASCII digits"
        );
        let profile_id = required_string(
            set,
            existing.as_ref().map(|item| &item.profile_id),
            "profile_id",
        )?;
        ai_protocol::id::ProfileId::new(profile_id.clone())?;
        let welcome_language = set
            .get("welcome_language")
            .cloned()
            .or_else(|| existing.as_ref().map(|item| item.welcome_language.clone()))
            .unwrap_or_else(|| "zh-CN".to_string());
        ensure!(
            matches!(welcome_language.as_str(), "zh-CN" | "en-US"),
            "welcome_language must be zh-CN or en-US"
        );
        let welcome_text = set
            .get("welcome_text")
            .cloned()
            .or_else(|| existing.as_ref().and_then(|item| item.welcome_text.clone()))
            .filter(|value| !value.trim().is_empty());
        let welcome_audio = set
            .get("welcome_audio_base64")
            .map(|value| base64::engine::general_purpose::STANDARD.decode(value))
            .transpose()
            .map_err(|error| anyhow!("welcome_audio_base64 is invalid: {error}"))?
            .filter(|bytes| !bytes.is_empty())
            .or_else(|| {
                if set.contains_key("welcome_audio_base64") {
                    None
                } else {
                    existing
                        .as_ref()
                        .and_then(|item| item.welcome_audio.clone())
                }
            });
        // A generated WAV is kept alongside its source text as a cache. Explicit edits clear
        // the opposite input; an update with neither field preserves the existing cache.
        let welcome_text =
            if set.contains_key("welcome_audio_base64") && !set.contains_key("welcome_text") {
                None
            } else {
                welcome_text
            };
        let welcome_audio =
            if set.contains_key("welcome_text") && !set.contains_key("welcome_audio_base64") {
                None
            } else {
                welcome_audio
            };
        if let Some(bytes) = &welcome_audio {
            ensure!(bytes.len() <= 768 * 1024, "welcome audio is too large");
            ensure!(
                bytes.len() >= 44 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WAVE",
                "welcome audio must be a RIFF/WAVE file"
            );
        }
        let enabled = set
            .get("enabled")
            .map(|value| parse_bool(value))
            .transpose()?
            .or_else(|| existing.as_ref().map(|item| item.enabled))
            .unwrap_or(true);
        let fallback_target = set
            .get("fallback_target")
            .cloned()
            .or_else(|| {
                existing
                    .as_ref()
                    .and_then(|item| item.fallback_target.clone())
            })
            .filter(|value| !value.trim().is_empty());
        Ok(AiAgentConfig {
            id: existing.as_ref().map(|item| item.id).unwrap_or_default(),
            agent_id,
            name,
            service_number,
            profile_id,
            welcome_text,
            welcome_audio,
            welcome_language,
            enabled,
            revision: existing.as_ref().map(|item| item.revision).unwrap_or(1),
            fallback_target,
        })
    }

    fn required_string(
        set: &BTreeMap<String, String>,
        current: Option<&String>,
        key: &str,
    ) -> Result<String> {
        let value = set
            .get(key)
            .cloned()
            .or_else(|| current.cloned())
            .ok_or_else(|| anyhow!("{key} is required"))?;
        let value = value.trim().to_string();
        ensure!(
            !value.is_empty() && value.len() <= 128,
            "{key} must contain 1..=128 bytes"
        );
        ensure!(
            value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric()
                    || matches!(byte, b'-' | b'_' | b'.' | b':')),
            "{key} contains unsupported characters"
        );
        Ok(value)
    }

    fn parse_bool(value: &str) -> Result<bool> {
        match value {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(anyhow!("enabled must be a boolean")),
        }
    }

    fn validate_agent(state: &AppState, domain_id: &DomainId, agent: &AiAgentConfig) -> Result<()> {
        let snapshot = state.config().snapshot();
        let domain = snapshot
            .domains
            .get(domain_id)
            .ok_or_else(|| anyhow!("RESOURCE_NOT_FOUND: domain {domain_id}"))?;
        ensure!(
            !domain
                .extensions
                .iter()
                .any(|extension| extension.enabled
                    && extension.number.as_str() == agent.service_number),
            "CONFLICT: service_number {} is already an enabled extension",
            agent.service_number
        );
        ensure!(
            !domain
                .ai_agents
                .iter()
                .any(|other| other.id != agent.id && other.service_number == agent.service_number),
            "CONFLICT: service_number {} is already assigned to an AI Agent",
            agent.service_number
        );
        if agent.enabled
            && let Some(ai_jobs) = state.ai_jobs()
        {
            ensure!(
                ai_jobs
                    .executable_voice_agent_profile(&agent.profile_id)
                    .is_some(),
                "INVALID_REFERENCE: voice_agent profile {} is missing, disabled, or not executable",
                agent.profile_id
            );
        }
        Ok(())
    }

    fn prepare_welcome(state: &AppState, agent: &mut AiAgentConfig) -> Result<()> {
        let Some(text) = agent.welcome_text.clone() else {
            return Ok(());
        };
        if agent.welcome_audio.is_some() {
            return Ok(());
        }
        let jobs = state
            .ai_jobs()
            .ok_or_else(|| anyhow!("AI gateway is unavailable"))?;
        let profile_id = ai_protocol::id::ProfileId::new(agent.profile_id.clone())?;
        let synthesized = tokio::task::block_in_place(|| jobs.synthesize_tts(profile_id, text))?;
        ensure!(
            synthesized.success,
            synthesized
                .error
                .unwrap_or_else(|| "TTS synthesis failed".to_string())
        );
        ensure!(
            !synthesized.pcm16_le.is_empty() && synthesized.pcm16_le.len().is_multiple_of(2),
            "TTS returned empty audio"
        );
        let sample_rate = synthesized.sample_rate;
        ensure!(
            (8_000..=48_000).contains(&sample_rate),
            "TTS returned unsupported sample rate"
        );
        agent.welcome_audio = Some(pcm_wav(&synthesized.pcm16_le, sample_rate));
        Ok(())
    }

    fn persist_welcome_audio(
        state: &AppState,
        domain_id: &DomainId,
        agent: &AiAgentConfig,
    ) -> Result<()> {
        let directory = state.config().snapshot().ai_welcome_audio_dir();
        std::fs::create_dir_all(&directory)
            .map_err(|error| anyhow!("cannot create AI welcome audio directory: {error}"))?;
        let path = directory.join(format!(
            "{}-{}-welcome.wav",
            domain_id.as_str(),
            agent.agent_id
        ));
        if let Some(bytes) = &agent.welcome_audio {
            std::fs::write(&path, bytes)
                .map_err(|error| anyhow!("cannot write AI welcome audio: {error}"))?;
        } else if path.exists() {
            std::fs::remove_file(&path)
                .map_err(|error| anyhow!("cannot remove AI welcome audio: {error}"))?;
        }
        Ok(())
    }

    fn pcm_wav(pcm: &[u8], sample_rate: u32) -> Vec<u8> {
        let data_len = pcm.len() as u32;
        let mut wav = Vec::with_capacity(44 + pcm.len());
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&(36 + data_len).to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&sample_rate.to_le_bytes());
        wav.extend_from_slice(&(sample_rate * 2).to_le_bytes());
        wav.extend_from_slice(&2u16.to_le_bytes());
        wav.extend_from_slice(&16u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&data_len.to_le_bytes());
        wav.extend_from_slice(pcm);
        wav
    }

    fn project(domain_id: &DomainId, agent: &AiAgentConfig, keys: &[String]) -> Value {
        let mut object = Map::new();
        for key in keys {
            let value = match key.as_str() {
                "domain_id" => json!(domain_id),
                "id" => json!(agent.id),
                "agent_id" => json!(agent.agent_id),
                "name" => json!(agent.name),
                "service_number" => json!(agent.service_number),
                "profile_id" => json!(agent.profile_id),
                "welcome_text" => json!(agent.welcome_text),
                "welcome_language" => json!(agent.welcome_language),
                "welcome_audio" => json!(
                    agent
                        .welcome_audio
                        .as_ref()
                        .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes))
                ),
                "enabled" => json!(agent.enabled),
                "revision" => json!(agent.revision),
                "fallback_target" => json!(agent.fallback_target),
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
            "ai_agent only supports cond=id=<id>"
        );
        let id = conditions[0]
            .1
            .parse::<u64>()
            .map_err(|_| anyhow!("ai_agent id must be a positive integer"))?;
        ensure!(id > 0, "ai_agent id must be a positive integer");
        Ok(Some(id))
    }

    async fn insert_agent(
        backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
        domain_id: &DomainId,
        agent: &AiAgentConfig,
    ) -> Result<u64> {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let id = allocate_id(&txn, domain_id).await?;
        let now = i64::try_from(unix_timestamp_ms()).context("timestamp overflow")?;
        txn.execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO ai_agent (domain_id,id,agent_id,name,service_number,profile_id,welcome_text,welcome_audio,welcome_language,enabled,revision,fallback_target,created_at,updated_at)
             VALUES (?,?,?,?,?,?,?,?,?, ?,1,?,?,?)",
            vec![
                domain_id.as_str().into(), i64::try_from(id)?.into(), agent.agent_id.clone().into(),
                agent.name.clone().into(), agent.service_number.clone().into(), agent.profile_id.clone().into(),
                agent.welcome_text.clone().into(), agent.welcome_audio.clone().into(), agent.welcome_language.clone().into(), agent.enabled.into(), agent.fallback_target.clone().into(), now.into(), now.into(),
            ],
        )).await?;
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(id)
    }

    async fn update_agent(
        backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
        domain_id: &DomainId,
        agent: &AiAgentConfig,
    ) -> Result<()> {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let result = txn.execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "UPDATE ai_agent SET agent_id=?,name=?,service_number=?,profile_id=?,welcome_text=?,welcome_audio=?,welcome_language=?,enabled=?,revision=revision+1,fallback_target=?,updated_at=?
             WHERE domain_id=? AND id=?",
            vec![
                agent.agent_id.clone().into(), agent.name.clone().into(), agent.service_number.clone().into(),
                agent.profile_id.clone().into(), agent.welcome_text.clone().into(), agent.welcome_audio.clone().into(), agent.welcome_language.clone().into(), agent.enabled.into(), agent.fallback_target.clone().into(),
                i64::try_from(unix_timestamp_ms())?.into(), domain_id.as_str().into(), i64::try_from(agent.id)?.into(),
            ],
        )).await?;
        ensure!(
            result.rows_affected() == 1,
            "RESOURCE_NOT_FOUND: ai_agent id {}",
            agent.id
        );
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(())
    }

    async fn delete_agent(
        backend: &crate::data_store::seaorm::SeaOrmConfigBackend,
        domain_id: &DomainId,
        id: u64,
    ) -> Result<()> {
        let (conn, _) = crate::pbx::domain::config::open_domain_db(backend, domain_id).await?;
        let txn = conn.begin().await?;
        let result = txn
            .execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "DELETE FROM ai_agent WHERE domain_id=? AND id=?",
                vec![domain_id.as_str().into(), i64::try_from(id)?.into()],
            ))
            .await?;
        ensure!(
            result.rows_affected() == 1,
            "RESOURCE_NOT_FOUND: ai_agent id {id}"
        );
        crate::pbx::domain::config::bump_domain_config_version(&txn).await?;
        txn.commit().await?;
        Ok(())
    }

    async fn allocate_id(txn: &DatabaseTransaction, domain_id: &DomainId) -> Result<u64> {
        let row = txn.query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "INSERT INTO config_id_sequences (domain_id,resource_type,next_id) VALUES (?,?,2)
             ON CONFLICT(domain_id,resource_type) DO UPDATE SET next_id=config_id_sequences.next_id+1
             RETURNING next_id-1 AS allocated_id",
            vec![domain_id.as_str().into(), "ai_agent".into()],
        )).await?.context("allocate AI Agent id")?;
        u64::try_from(row.try_get::<i64>("", "allocated_id")?)
            .context("allocated AI Agent id is negative")
    }

    fn reload(state: &AppState) -> Result<()> {
        state
            .config()
            .replace(state.backend().load_runtime_config()?);
        Ok(())
    }
}
