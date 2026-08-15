use super::{
    AiCallResultRecord, CallTrace, CallTraceMessage, CdrRecord, ConfigBackend, LegCdrRecord,
    PageRequest, PageResult, RecordingRecord,
};
use crate::config_service::RuntimeConfig;
use anyhow::{Context, Result, bail};
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement,
    TransactionTrait,
};
use std::collections::BTreeSet;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tokio::runtime::{Handle, Runtime};
use voipswitch_core::media::MediaForwardingMode;

#[derive(Clone)]
pub struct SeaOrmConfigBackend {
    data_dir: PathBuf,
    instance_id: String,
    runtime: Arc<Runtime>,
}

impl SeaOrmConfigBackend {
    pub fn sqlite(data_dir: impl Into<PathBuf>, instance_id: impl Into<String>) -> Result<Self> {
        Ok(Self {
            data_dir: data_dir.into(),
            instance_id: instance_id.into(),
            runtime: Arc::new(Runtime::new().context("create SeaORM runtime")?),
        })
    }

    pub(crate) fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub(crate) fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub(crate) fn block_on<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T>,
    {
        if let Ok(handle) = Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }

    fn system_db_path(&self) -> PathBuf {
        self.data_dir.join("system.db")
    }

    pub(crate) async fn open_system_db(&self) -> Result<DatabaseConnection> {
        fs::create_dir_all(&self.data_dir)
            .with_context(|| format!("create data dir {}", self.data_dir.display()))?;
        connect_sqlite(&self.system_db_path()).await
    }

    fn domain_cdr_path(&self, domain_id: &str) -> PathBuf {
        self.data_dir.join("domains").join(domain_id).join("cdr.db")
    }

    pub(crate) async fn open_domain_cdr_db(&self, domain_id: &str) -> Result<DatabaseConnection> {
        let path = self.domain_cdr_path(domain_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create domain cdr dir {}", parent.display()))?;
        }
        let conn = connect_sqlite(&path).await?;
        ensure_cdr_schema(&conn).await?;
        ensure_call_trace_schema(&conn).await?;
        ensure_recording_schema(&conn).await?;
        ensure_leg_cdr_schema(&conn).await?;
        ensure_ai_result_schema(&conn).await?;
        Ok(conn)
    }

    async fn list_domain_ids(&self) -> Result<Vec<String>> {
        let domains_dir = self.data_dir.join("domains");
        if !domains_dir.is_dir() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        for entry in fs::read_dir(&domains_dir)
            .with_context(|| format!("read domains dir {}", domains_dir.display()))?
        {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && let Some(name) = entry.file_name().to_str()
                && self.domain_cdr_path(name).is_file()
            {
                ids.push(name.to_string());
            }
        }
        ids.sort();
        Ok(ids)
    }

    async fn candidate_domain_ids(&self, domain_id: Option<&str>) -> Result<Vec<String>> {
        match domain_id {
            Some(domain_id) => Ok(vec![domain_id.to_string()]),
            None => self.list_domain_ids().await,
        }
    }

    async fn list_spool_domain_ids(&self) -> Result<Vec<String>> {
        self.list_domain_subdirectories("spool/cdr").await
    }

    async fn list_ai_outbox_domain_ids(&self) -> Result<Vec<String>> {
        self.list_domain_subdirectories("spool/ai").await
    }

    async fn list_domain_subdirectories(&self, relative: &str) -> Result<Vec<String>> {
        let domains_dir = self.data_dir.join("domains");
        if !domains_dir.is_dir() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        for entry in fs::read_dir(&domains_dir)
            .with_context(|| format!("read domains dir {}", domains_dir.display()))?
        {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if entry.file_type()?.is_dir()
                && self
                    .data_dir
                    .join("domains")
                    .join(&name)
                    .join(relative)
                    .is_dir()
            {
                ids.push(name);
            }
        }
        ids.sort();
        Ok(ids)
    }

    async fn migrate_legacy_runtime_data(&self, system: &DatabaseConnection) -> Result<()> {
        let tables = LegacyRuntimeTables::inspect(system).await?;
        if !tables.any() {
            return Ok(());
        }

        tables.ensure_schemas(system).await?;
        let domain_ids = tables.domain_ids(system).await?;
        for domain_id in domain_ids {
            validate_domain_storage_id(&domain_id)?;
            let domain_conn = self.open_domain_cdr_db(&domain_id).await?;
            drop(domain_conn);

            let counts = migrate_legacy_domain(
                system,
                &self.domain_cdr_path(&domain_id),
                &domain_id,
                &tables,
            )
            .await
            .with_context(|| format!("migrate legacy runtime data for domain {domain_id}"))?;
            tracing::info!(
                domain_id,
                call_cdr = counts[0],
                call_trace_call = counts[1],
                call_trace_message = counts[2],
                call_recording = counts[3],
                leg_cdr = counts[4],
                "migrated legacy runtime data to domain cdr database"
            );
        }

        drop_legacy_runtime_tables(system).await?;
        Ok(())
    }
}

impl ConfigBackend for SeaOrmConfigBackend {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn load_runtime_config(&self) -> Result<RuntimeConfig> {
        crate::pbx::domain::config::load_runtime_config(self)
    }

    fn health_check(&self) -> Result<()> {
        self.block_on(async {
            let conn = self.open_system_db().await?;
            self.migrate_legacy_runtime_data(&conn).await?;
            Ok(())
        })
    }

    fn insert_cdr(&self, record: &CdrRecord) -> Result<()> {
        self.block_on(async {
            let conn = self.open_domain_cdr_db(&record.domain_id).await?;
            conn.execute(cdr_upsert_statement(record)?).await?;
            Ok(())
        })
    }

    fn list_cdr(
        &self,
        domain_id: Option<&str>,
        page: PageRequest,
    ) -> Result<PageResult<CdrRecord>> {
        self.block_on(async {
            let offset = page.page.saturating_sub(1).saturating_mul(page.page_size);
            let fetch_limit = offset.saturating_add(page.page_size);
            let domain_ids = match domain_id {
                Some(id) => vec![id.to_string()],
                None => self.list_domain_ids().await?,
            };
            if domain_ids.is_empty() {
                return Ok(PageResult {
                    rows: Vec::new(),
                    total: 0,
                    page: Some(page),
                });
            }
            let mut all_rows = Vec::new();
            let mut total: u64 = 0;
            for did in &domain_ids {
                let conn = self.open_domain_cdr_db(did).await?;
                let count = conn
                    .query_one(Statement::from_string(
                        DbBackend::Sqlite,
                        "SELECT COUNT(*) AS count FROM call_cdr",
                    ))
                    .await?
                    .context("read CDR count")?
                    .try_get::<i64>("", "count")?;
                total = total.saturating_add(as_u64(count)?);
                let rows = conn
                    .query_all(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        "SELECT call_cdr.*,
                            (
                                call_cdr.trace_available = 1
                                OR EXISTS(
                                SELECT 1 FROM call_trace_message
                                WHERE call_trace_message.call_id = call_cdr.call_id
                                )
                            ) AS trace_available,
                            COALESCE(
                                (
                                    SELECT incomplete FROM call_trace_call
                                    WHERE call_trace_call.call_id = call_cdr.call_id
                                ),
                                0
                            ) AS trace_incomplete,
                            (
                                SELECT status FROM call_recording
                                WHERE call_recording.call_id = call_cdr.call_id
                            ) AS recording_status,
                            EXISTS(
                                SELECT 1 FROM call_recording
                                WHERE call_recording.call_id = call_cdr.call_id
                                  AND call_recording.status IN ('complete','incomplete')
                                  AND call_recording.storage_path <> ''
                            ) AS recording_available
                         FROM call_cdr
                         ORDER BY ended_at_ms DESC, call_id DESC
                         LIMIT ?",
                        vec![as_i64(fetch_limit)?.into()],
                    ))
                    .await?
                    .into_iter()
                    .map(|row| cdr_from_row(&row))
                    .collect::<Result<Vec<_>>>()?;
                all_rows.extend(rows);
            }
            all_rows.sort_by(|a, b| {
                b.ended_at_ms
                    .cmp(&a.ended_at_ms)
                    .then_with(|| a.domain_id.cmp(&b.domain_id))
                    .then_with(|| b.call_id.cmp(&a.call_id))
            });
            let offset = usize::try_from(offset).context("CDR page offset out of range")?;
            let page_size =
                usize::try_from(page.page_size).context("CDR page size out of range")?;
            let rows = all_rows.into_iter().skip(offset).take(page_size).collect();
            Ok(PageResult {
                rows,
                total,
                page: Some(page),
            })
        })
    }

    fn insert_call_trace_message(&self, message: &CallTraceMessage) -> Result<()> {
        self.block_on(async {
            let conn = self.open_domain_cdr_db(&message.domain_id).await?;
            let txn = conn.begin().await?;
            txn.execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                r#"
                INSERT INTO call_trace_call (
                    call_id, domain_id, first_observed_at_ms, ended_at_ms, incomplete
                ) VALUES (?, ?, ?, NULL, 0)
                ON CONFLICT(call_id) DO UPDATE SET
                    domain_id=excluded.domain_id,
                    first_observed_at_ms=MIN(
                        call_trace_call.first_observed_at_ms,
                        excluded.first_observed_at_ms
                    )
                "#,
                vec![
                    message.call_id.clone().into(),
                    message.domain_id.clone().into(),
                    as_i64(message.observed_at_ms)?.into(),
                ],
            ))
            .await?;
            txn.execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                r#"
                INSERT OR IGNORE INTO call_trace_message (
                    call_id, domain_id, sequence, observed_at_ms, direction,
                    adapter_call_leg_id, session_id, source_addr, destination_addr,
                    start_line, packet
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
                vec![
                    message.call_id.clone().into(),
                    message.domain_id.clone().into(),
                    as_i64(message.sequence)?.into(),
                    as_i64(message.observed_at_ms)?.into(),
                    message.direction.clone().into(),
                    message.adapter_call_leg_id.clone().into(),
                    message.session_id.clone().into(),
                    message.source_addr.clone().into(),
                    message.destination_addr.clone().into(),
                    message.start_line.clone().into(),
                    message.packet.clone().into(),
                ],
            ))
            .await?;
            txn.commit().await?;
            Ok(())
        })
    }

    fn complete_call_trace(
        &self,
        call_id: &str,
        domain_id: &str,
        ended_at_ms: u64,
        incomplete: bool,
    ) -> Result<()> {
        self.block_on(async {
            let conn = self.open_domain_cdr_db(domain_id).await?;
            let txn = conn.begin().await?;
            txn.execute(complete_trace_statement(
                call_id,
                domain_id,
                ended_at_ms,
                incomplete,
            )?)
            .await?;
            for statement in trace_retention_statements() {
                txn.execute(statement).await?;
            }
            txn.commit().await?;
            Ok(())
        })
    }

    fn mark_call_trace_incomplete(&self, call_id: &str, domain_id: &str) -> Result<()> {
        self.block_on(async {
            let conn = self.open_domain_cdr_db(domain_id).await?;
            conn.execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "UPDATE call_trace_call SET incomplete = 1 WHERE call_id = ?",
                vec![call_id.to_string().into()],
            ))
            .await?;
            Ok(())
        })
    }

    fn get_call_trace(&self, call_id: &str, domain_id: Option<&str>) -> Result<Option<CallTrace>> {
        self.block_on(async {
            for domain_id in self.candidate_domain_ids(domain_id).await? {
                let conn = self.open_domain_cdr_db(&domain_id).await?;
                let Some(call) = conn
                    .query_one(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        r#"
                    SELECT call_id, domain_id, ended_at_ms, incomplete
                    FROM call_trace_call
                    WHERE call_id = ?
                    "#,
                        vec![call_id.to_string().into()],
                    ))
                    .await?
                else {
                    continue;
                };
                let messages = conn
                    .query_all(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        r#"
                    SELECT call_id, domain_id, sequence, observed_at_ms, direction,
                           adapter_call_leg_id, session_id, source_addr, destination_addr,
                           start_line, packet
                    FROM call_trace_message
                    WHERE call_id = ?
                    ORDER BY sequence ASC, id ASC
                    "#,
                        vec![call_id.to_string().into()],
                    ))
                    .await?
                    .into_iter()
                    .map(|row| {
                        Ok(CallTraceMessage {
                            call_id: row.try_get("", "call_id")?,
                            domain_id: row.try_get("", "domain_id")?,
                            sequence: as_u64(row.try_get("", "sequence")?)?,
                            observed_at_ms: as_u64(row.try_get("", "observed_at_ms")?)?,
                            direction: row.try_get("", "direction")?,
                            adapter_call_leg_id: row.try_get("", "adapter_call_leg_id")?,
                            session_id: row.try_get("", "session_id")?,
                            source_addr: row.try_get("", "source_addr")?,
                            destination_addr: row.try_get("", "destination_addr")?,
                            start_line: row.try_get("", "start_line")?,
                            packet: row.try_get("", "packet")?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                return Ok(Some(CallTrace {
                    call_id: call.try_get("", "call_id")?,
                    domain_id: call.try_get("", "domain_id")?,
                    ended_at_ms: call
                        .try_get::<Option<i64>>("", "ended_at_ms")?
                        .map(as_u64)
                        .transpose()?,
                    incomplete: call.try_get::<i64>("", "incomplete")? != 0,
                    messages,
                }));
            }
            Ok(None)
        })
    }

    fn upsert_recording(&self, record: &RecordingRecord) -> Result<()> {
        self.block_on(async {
            let conn = self.open_domain_cdr_db(&record.domain_id).await?;
            conn.execute(recording_upsert_statement(record)?).await?;
            Ok(())
        })
    }

    fn get_recording(
        &self,
        call_id: &str,
        domain_id: Option<&str>,
    ) -> Result<Option<RecordingRecord>> {
        self.block_on(async {
            for domain_id in self.candidate_domain_ids(domain_id).await? {
                let conn = self.open_domain_cdr_db(&domain_id).await?;
                if let Some(recording) = conn
                    .query_one(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        "SELECT * FROM call_recording WHERE call_id=?",
                        vec![call_id.into()],
                    ))
                    .await?
                    .map(recording_from_row)
                    .transpose()?
                {
                    return Ok(Some(recording));
                }
            }
            Ok(None)
        })
    }

    fn insert_leg_cdr(&self, record: &LegCdrRecord) -> Result<()> {
        self.block_on(async {
            let conn = self.open_domain_cdr_db(&record.domain_id).await?;
            conn.execute(leg_cdr_upsert_statement(record)?).await?;
            Ok(())
        })
    }

    fn list_leg_cdrs(&self, call_id: &str, domain_id: Option<&str>) -> Result<Vec<LegCdrRecord>> {
        self.block_on(async {
            for domain_id in self.candidate_domain_ids(domain_id).await? {
                let conn = self.open_domain_cdr_db(&domain_id).await?;
                let rows = conn
                    .query_all(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        "SELECT * FROM leg_cdr WHERE call_id=? ORDER BY joined_at_ms ASC",
                        vec![call_id.into()],
                    ))
                    .await?;
                if !rows.is_empty() {
                    return rows.iter().map(leg_cdr_from_row).collect();
                }
            }
            Ok(Vec::new())
        })
    }

    fn cleanup_recordings(
        &self,
        retention_days: u64,
        max_size_gb: u64,
        now_ms: u64,
    ) -> Result<u64> {
        self.block_on(async {
            let domain_ids = self.list_domain_ids().await?;
            let retention_ms = retention_days.saturating_mul(24 * 60 * 60 * 1000);
            let cutoff = now_ms.saturating_sub(retention_ms);
            let max_bytes = max_size_gb.saturating_mul(1024 * 1024 * 1024);
            let mut total_expired = 0_u64;
            let mut total_bytes = 0_u64;
            for did in &domain_ids {
                let conn = self.open_domain_cdr_db(did).await?;
                let rows = conn
                    .query_all(Statement::from_string(
                        DbBackend::Sqlite,
                        "SELECT call_id,status,ended_at_ms,storage_root,storage_path,file_size_bytes
                         FROM call_recording
                         WHERE status IN ('complete','incomplete','failed')
                         ORDER BY COALESCE(ended_at_ms,started_at_ms),call_id"
                            .to_string(),
                    ))
                    .await?;
                total_bytes = total_bytes.saturating_add(
                    rows.iter()
                        .filter_map(|row| row.try_get::<i64>("", "file_size_bytes").ok())
                        .filter_map(|value| u64::try_from(value).ok())
                        .sum::<u64>(),
                );
                for row in rows {
                    let ended_at = row
                        .try_get::<Option<i64>>("", "ended_at_ms")?
                        .and_then(|value| u64::try_from(value).ok())
                        .unwrap_or(now_ms);
                    let file_size = u64::try_from(row.try_get::<i64>("", "file_size_bytes")?)
                        .unwrap_or_default();
                    if ended_at >= cutoff && total_bytes <= max_bytes {
                        continue;
                    }
                    let root: String = row.try_get("", "storage_root")?;
                    let path: String = row.try_get("", "storage_path")?;
                    if !path.is_empty() {
                        let canonical_root = std::fs::canonicalize(&root);
                        let canonical_path = std::fs::canonicalize(&path);
                        match (canonical_root, canonical_path) {
                            (Ok(root), Ok(path)) if path.starts_with(&root) && path.is_file() => {
                                std::fs::remove_file(&path)?;
                            }
                            (_, Err(err)) if err.kind() == std::io::ErrorKind::NotFound => {}
                            _ => continue,
                        }
                    }
                    let call_id: String = row.try_get("", "call_id")?;
                    conn.execute(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        "UPDATE call_recording
                         SET status='expired',storage_path='',file_size_bytes=0,updated_at=?
                         WHERE call_id=?",
                        vec![as_i64(now_ms)?.into(), call_id.into()],
                    ))
                    .await?;
                    total_bytes = total_bytes.saturating_sub(file_size);
                    total_expired = total_expired.saturating_add(1);
                }
            }
            Ok(total_expired)
        })
    }

    fn cdr_spool_dir(&self, domain_id: &str) -> Result<PathBuf> {
        validate_domain_storage_id(domain_id)?;
        Ok(self
            .data_dir
            .join("domains")
            .join(domain_id)
            .join("spool/cdr"))
    }

    fn list_cdr_spool_domains(&self) -> Result<Vec<String>> {
        self.block_on(self.list_spool_domain_ids())
    }

    fn ai_outbox_dir(&self, domain_id: &str) -> Result<PathBuf> {
        validate_domain_storage_id(domain_id)?;
        Ok(self
            .data_dir
            .join("domains")
            .join(domain_id)
            .join("spool/ai"))
    }

    fn list_ai_outbox_domains(&self) -> Result<Vec<String>> {
        self.block_on(self.list_ai_outbox_domain_ids())
    }

    fn persist_ai_result(&self, record: &AiCallResultRecord) -> Result<()> {
        validate_domain_storage_id(&record.domain_id)?;
        if record.result_version == 0 || record.profile_version == 0 {
            bail!("AI result and profile versions must be greater than zero");
        }
        let payload_json = serde_json::to_string(record)?;
        let transcript_json = serde_json::to_string(&record.transcript)?;
        let key_points_json = serde_json::to_string(&record.result.key_points)?;
        let action_items_json = serde_json::to_string(&record.result.action_items)?;
        let tags_json = serde_json::to_string(&record.result.tags)?;
        self.block_on(async {
            let conn = self.open_domain_cdr_db(&record.domain_id).await?;
            let txn = conn.begin().await?;
            txn.execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                r#"
                INSERT OR IGNORE INTO ai_call_result (
                    job_id,result_version,domain_id,call_id,operation_id,generation,
                    profile_id,profile_version,capture_quality,transcript_json,
                    result_schema_version,summary,purpose,outcome,key_points_json,
                    action_items_json,tags_json,payload_json,received_at_ms
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                "#,
                vec![
                    record.job_id.as_str().into(),
                    as_i64(record.result_version)?.into(),
                    record.domain_id.clone().into(),
                    record.call_id.clone().into(),
                    record.operation_id.as_str().into(),
                    as_i64(record.generation)?.into(),
                    record.profile_id.as_str().into(),
                    as_i64(record.profile_version)?.into(),
                    capture_quality_name(record.capture_quality).into(),
                    transcript_json.into(),
                    i64::from(record.result.schema_version).into(),
                    record.result.summary.clone().into(),
                    record.result.purpose.clone().into(),
                    record.result.outcome.clone().into(),
                    key_points_json.into(),
                    action_items_json.into(),
                    tags_json.into(),
                    payload_json.clone().into(),
                    as_i64(record.received_at_ms)?.into(),
                ],
            ))
            .await?;
            let stored = txn
                .query_one(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "SELECT payload_json FROM ai_call_result WHERE job_id=? AND result_version=?",
                    vec![
                        record.job_id.as_str().into(),
                        as_i64(record.result_version)?.into(),
                    ],
                ))
                .await?
                .context("read persisted AI result")?
                .try_get::<String>("", "payload_json")?;
            if stored != payload_json {
                bail!(
                    "AI result collision for job {} version {}",
                    record.job_id,
                    record.result_version
                );
            }
            txn.commit().await?;
            Ok(())
        })
    }

    fn get_ai_results(&self, call_id: &str, domain_id: &str) -> Result<Vec<AiCallResultRecord>> {
        validate_domain_storage_id(domain_id)?;
        self.block_on(async {
            let conn = self.open_domain_cdr_db(domain_id).await?;
            conn.query_all(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "SELECT payload_json FROM ai_call_result
                 WHERE domain_id=? AND call_id=? ORDER BY result_version DESC,job_id",
                vec![domain_id.into(), call_id.into()],
            ))
            .await?
            .into_iter()
            .map(|row| {
                let payload: String = row.try_get("", "payload_json")?;
                serde_json::from_str(&payload).context("decode persisted AI result")
            })
            .collect()
        })
    }

    fn persist_cdr_batch(&self, command: &super::CdrWriteCommand) -> Result<()> {
        let domain_id = command.call_cdr.domain_id.as_str();
        if command.trace_domain_id != domain_id
            || command
                .leg_cdrs
                .iter()
                .any(|leg| leg.domain_id != domain_id)
            || command
                .recording
                .as_ref()
                .is_some_and(|recording| recording.domain_id != domain_id)
        {
            bail!("CDR write batch contains records from multiple domains");
        }

        self.block_on(async {
            let conn = self.open_domain_cdr_db(domain_id).await?;
            let txn = conn.begin().await?;
            if let Some(recording) = &command.recording {
                txn.execute(recording_upsert_statement(recording)?).await?;
            }
            for leg in &command.leg_cdrs {
                txn.execute(leg_cdr_upsert_statement(leg)?).await?;
            }
            txn.execute(cdr_upsert_statement(&command.call_cdr)?)
                .await?;
            if command.call_cdr.trace_available {
                txn.execute(complete_trace_statement(
                    &command.trace_call_id,
                    &command.trace_domain_id,
                    command.trace_ended_at_ms,
                    false,
                )?)
                .await?;
                for statement in trace_retention_statements() {
                    txn.execute(statement).await?;
                }
            }
            txn.commit().await?;
            Ok(())
        })
    }
}

fn cdr_from_row(row: &sea_orm::QueryResult) -> Result<CdrRecord> {
    Ok(CdrRecord {
        call_id: row.try_get("", "call_id")?,
        domain_id: row.try_get("", "domain_id")?,
        caller_number: row.try_get("", "caller_number")?,
        callee_number: row.try_get("", "callee_number")?,
        inbound_route_id: row.try_get("", "inbound_route_id")?,
        inbound_route_name: row.try_get("", "inbound_route_name")?,
        inbound_trunk_ref: row.try_get("", "inbound_trunk_ref")?,
        inbound_trunk_name: row.try_get("", "inbound_trunk_name")?,
        outbound_route_id: row.try_get("", "outbound_route_id")?,
        outbound_route_name: row.try_get("", "outbound_route_name")?,
        outbound_trunk_ref: row.try_get("", "outbound_trunk_ref")?,
        outbound_trunk_name: row.try_get("", "outbound_trunk_name")?,
        started_at_ms: as_u64(row.try_get("", "started_at_ms")?)?,
        answered_at_ms: row
            .try_get::<Option<i64>>("", "answered_at_ms")?
            .map(as_u64)
            .transpose()?,
        ended_at_ms: as_u64(row.try_get("", "ended_at_ms")?)?,
        duration_ms: as_u64(row.try_get("", "duration_ms")?)?,
        billable_ms: as_u64(row.try_get("", "billable_ms")?)?,
        answered: row.try_get("", "answered")?,
        final_status: row
            .try_get::<Option<i64>>("", "final_status")?
            .map(u16::try_from)
            .transpose()
            .context("final status out of range")?,
        hangup_cause: row.try_get("", "hangup_cause")?,
        media_forwarding_mode: row
            .try_get::<Option<String>>("", "media_forwarding_mode")?
            .map(|value| {
                MediaForwardingMode::from_str(&value)
                    .map_err(anyhow::Error::msg)
                    .with_context(|| format!("invalid media forwarding mode: {value}"))
            })
            .transpose()?,
        caller_to_callee_packets: as_u64(row.try_get("", "caller_to_callee_packets")?)?,
        caller_to_callee_bytes: as_u64(row.try_get("", "caller_to_callee_bytes")?)?,
        callee_to_caller_packets: as_u64(row.try_get("", "callee_to_caller_packets")?)?,
        callee_to_caller_bytes: as_u64(row.try_get("", "callee_to_caller_bytes")?)?,
        caller_to_callee_rtcp_packets: as_u64(row.try_get("", "caller_to_callee_rtcp_packets")?)?,
        callee_to_caller_rtcp_packets: as_u64(row.try_get("", "callee_to_caller_rtcp_packets")?)?,
        trace_available: row.try_get::<i64>("", "trace_available")? != 0,
        trace_incomplete: row.try_get::<i64>("", "trace_incomplete")? != 0,
        recording_status: row.try_get("", "recording_status")?,
        incomplete: row.try_get::<Option<i64>>("", "incomplete")?.unwrap_or(0) != 0,
        incomplete_reason: row.try_get("", "incomplete_reason")?,
        recording_available: row.try_get::<i64>("", "recording_available")? != 0,
    })
}

fn cdr_upsert_statement(record: &CdrRecord) -> Result<Statement> {
    Ok(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        r#"
        INSERT INTO call_cdr (
            call_id, domain_id, caller_number, callee_number,
            inbound_route_id, inbound_route_name, inbound_trunk_ref, inbound_trunk_name,
            outbound_route_id, outbound_route_name, outbound_trunk_ref, outbound_trunk_name,
            started_at_ms, answered_at_ms, ended_at_ms,
            duration_ms, billable_ms, answered, final_status, hangup_cause,
            media_forwarding_mode, trace_available,
            caller_to_callee_packets, caller_to_callee_bytes,
            callee_to_caller_packets, callee_to_caller_bytes,
            caller_to_callee_rtcp_packets, callee_to_caller_rtcp_packets,
            incomplete, incomplete_reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(call_id) DO UPDATE SET
            caller_number=excluded.caller_number,
            callee_number=excluded.callee_number,
            inbound_route_id=excluded.inbound_route_id,
            inbound_route_name=excluded.inbound_route_name,
            inbound_trunk_ref=excluded.inbound_trunk_ref,
            inbound_trunk_name=excluded.inbound_trunk_name,
            outbound_route_id=excluded.outbound_route_id,
            outbound_route_name=excluded.outbound_route_name,
            outbound_trunk_ref=excluded.outbound_trunk_ref,
            outbound_trunk_name=excluded.outbound_trunk_name,
            answered_at_ms=excluded.answered_at_ms,
            ended_at_ms=excluded.ended_at_ms,
            duration_ms=excluded.duration_ms,
            billable_ms=excluded.billable_ms,
            answered=excluded.answered,
            final_status=excluded.final_status,
            hangup_cause=excluded.hangup_cause,
            media_forwarding_mode=excluded.media_forwarding_mode,
            trace_available=MAX(call_cdr.trace_available, excluded.trace_available),
            caller_to_callee_packets=excluded.caller_to_callee_packets,
            caller_to_callee_bytes=excluded.caller_to_callee_bytes,
            callee_to_caller_packets=excluded.callee_to_caller_packets,
            callee_to_caller_bytes=excluded.callee_to_caller_bytes,
            caller_to_callee_rtcp_packets=excluded.caller_to_callee_rtcp_packets,
            callee_to_caller_rtcp_packets=excluded.callee_to_caller_rtcp_packets,
            incomplete=MAX(call_cdr.incomplete, excluded.incomplete),
            incomplete_reason=COALESCE(excluded.incomplete_reason, call_cdr.incomplete_reason)
        "#,
        vec![
            record.call_id.clone().into(),
            record.domain_id.clone().into(),
            record.caller_number.clone().into(),
            record.callee_number.clone().into(),
            record.inbound_route_id.clone().into(),
            record.inbound_route_name.clone().into(),
            record.inbound_trunk_ref.clone().into(),
            record.inbound_trunk_name.clone().into(),
            record.outbound_route_id.clone().into(),
            record.outbound_route_name.clone().into(),
            record.outbound_trunk_ref.clone().into(),
            record.outbound_trunk_name.clone().into(),
            as_i64(record.started_at_ms)?.into(),
            record.answered_at_ms.map(as_i64).transpose()?.into(),
            as_i64(record.ended_at_ms)?.into(),
            as_i64(record.duration_ms)?.into(),
            as_i64(record.billable_ms)?.into(),
            record.answered.into(),
            record.final_status.map(i64::from).into(),
            record.hangup_cause.clone().into(),
            record
                .media_forwarding_mode
                .map(|mode| mode.as_str().to_string())
                .into(),
            record.trace_available.into(),
            as_i64(record.caller_to_callee_packets)?.into(),
            as_i64(record.caller_to_callee_bytes)?.into(),
            as_i64(record.callee_to_caller_packets)?.into(),
            as_i64(record.callee_to_caller_bytes)?.into(),
            as_i64(record.caller_to_callee_rtcp_packets)?.into(),
            as_i64(record.callee_to_caller_rtcp_packets)?.into(),
            record.incomplete.into(),
            record.incomplete_reason.clone().into(),
        ],
    ))
}

fn recording_upsert_statement(record: &RecordingRecord) -> Result<Statement> {
    Ok(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        "INSERT INTO call_recording (
            recording_id,call_id,domain_id,status,caller_number,callee_number,
            started_at_ms,ended_at_ms,duration_ms,format,sample_rate,channel_count,
            file_name,storage_root,storage_path,file_size_bytes,packets_tapped,
            packets_dropped,error_code,error_message,created_at,updated_at
         ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
         ON CONFLICT(call_id) DO UPDATE SET
            status=excluded.status,ended_at_ms=excluded.ended_at_ms,
            duration_ms=excluded.duration_ms,file_name=excluded.file_name,
            storage_root=excluded.storage_root,storage_path=excluded.storage_path,
            file_size_bytes=excluded.file_size_bytes,
            packets_tapped=excluded.packets_tapped,
            packets_dropped=excluded.packets_dropped,
            error_code=excluded.error_code,error_message=excluded.error_message,
            updated_at=excluded.updated_at",
        vec![
            record.recording_id.clone().into(),
            record.call_id.clone().into(),
            record.domain_id.clone().into(),
            record.status.clone().into(),
            record.caller_number.clone().into(),
            record.callee_number.clone().into(),
            as_i64(record.started_at_ms)?.into(),
            record.ended_at_ms.map(as_i64).transpose()?.into(),
            as_i64(record.duration_ms)?.into(),
            record.format.clone().into(),
            i64::from(record.sample_rate).into(),
            i64::from(record.channel_count).into(),
            record.file_name.clone().into(),
            record.storage_root.clone().into(),
            record.storage_path.clone().into(),
            as_i64(record.file_size_bytes)?.into(),
            as_i64(record.packets_tapped)?.into(),
            as_i64(record.packets_dropped)?.into(),
            record.error_code.clone().into(),
            record.error_message.clone().into(),
            as_i64(record.started_at_ms)?.into(),
            as_i64(record.ended_at_ms.unwrap_or(record.started_at_ms))?.into(),
        ],
    ))
}

fn leg_cdr_upsert_statement(record: &LegCdrRecord) -> Result<Statement> {
    Ok(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        r#"
        INSERT INTO leg_cdr (
            call_id, session_id, domain_id, leg_role, direction,
            endpoint_ref, endpoint_number, signaling_number,
            route_id, route_name, trunk_ref, trunk_name,
            joined_at_ms, answered_at_ms, left_at_ms,
            final_status, hangup_cause,
            media_packets, media_bytes, media_rtcp_packets, bridge_ids
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(call_id, session_id) DO UPDATE SET
            leg_role=excluded.leg_role,
            direction=excluded.direction,
            endpoint_ref=excluded.endpoint_ref,
            endpoint_number=excluded.endpoint_number,
            signaling_number=excluded.signaling_number,
            route_id=excluded.route_id,
            route_name=excluded.route_name,
            trunk_ref=excluded.trunk_ref,
            trunk_name=excluded.trunk_name,
            answered_at_ms=excluded.answered_at_ms,
            left_at_ms=excluded.left_at_ms,
            final_status=excluded.final_status,
            hangup_cause=excluded.hangup_cause,
            media_packets=excluded.media_packets,
            media_bytes=excluded.media_bytes,
            media_rtcp_packets=excluded.media_rtcp_packets,
            bridge_ids=excluded.bridge_ids
        "#,
        vec![
            record.call_id.clone().into(),
            record.session_id.clone().into(),
            record.domain_id.clone().into(),
            record.leg_role.clone().into(),
            record.direction.clone().into(),
            record.endpoint_ref.clone().into(),
            record.endpoint_number.clone().into(),
            record.signaling_number.clone().into(),
            record.route_id.clone().into(),
            record.route_name.clone().into(),
            record.trunk_ref.clone().into(),
            record.trunk_name.clone().into(),
            as_i64(record.joined_at_ms)?.into(),
            record.answered_at_ms.map(as_i64).transpose()?.into(),
            as_i64(record.left_at_ms)?.into(),
            record.final_status.map(i64::from).into(),
            record.hangup_cause.clone().into(),
            as_i64(record.media_packets)?.into(),
            as_i64(record.media_bytes)?.into(),
            as_i64(record.media_rtcp_packets)?.into(),
            record.bridge_ids.clone().into(),
        ],
    ))
}

fn complete_trace_statement(
    call_id: &str,
    domain_id: &str,
    ended_at_ms: u64,
    incomplete: bool,
) -> Result<Statement> {
    Ok(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        r#"
        INSERT INTO call_trace_call (
            call_id, domain_id, first_observed_at_ms, ended_at_ms, incomplete
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(call_id) DO UPDATE SET
            domain_id=excluded.domain_id,
            ended_at_ms=excluded.ended_at_ms,
            incomplete=MAX(call_trace_call.incomplete, excluded.incomplete)
        "#,
        vec![
            call_id.to_string().into(),
            domain_id.to_string().into(),
            as_i64(ended_at_ms)?.into(),
            as_i64(ended_at_ms)?.into(),
            incomplete.into(),
        ],
    ))
}

fn trace_retention_statements() -> [Statement; 2] {
    [
        Statement::from_string(
            DbBackend::Sqlite,
            "DELETE FROM call_trace_message
             WHERE call_id IN (
                SELECT call_id FROM call_trace_call
                WHERE ended_at_ms IS NOT NULL
                ORDER BY ended_at_ms DESC, call_id DESC
                LIMIT -1 OFFSET 100
             )"
            .to_string(),
        ),
        Statement::from_string(
            DbBackend::Sqlite,
            "DELETE FROM call_trace_call
             WHERE call_id IN (
                SELECT call_id FROM call_trace_call
                WHERE ended_at_ms IS NOT NULL
                ORDER BY ended_at_ms DESC, call_id DESC
                LIMIT -1 OFFSET 100
             )"
            .to_string(),
        ),
    ]
}

#[derive(Clone, Copy)]
struct LegacyRuntimeTables {
    call_cdr: bool,
    call_trace_call: bool,
    call_trace_message: bool,
    call_recording: bool,
    leg_cdr: bool,
}

impl LegacyRuntimeTables {
    async fn inspect(conn: &DatabaseConnection) -> Result<Self> {
        Ok(Self {
            call_cdr: table_exists(conn, "call_cdr").await?,
            call_trace_call: table_exists(conn, "call_trace_call").await?,
            call_trace_message: table_exists(conn, "call_trace_message").await?,
            call_recording: table_exists(conn, "call_recording").await?,
            leg_cdr: table_exists(conn, "leg_cdr").await?,
        })
    }

    fn any(self) -> bool {
        self.call_cdr
            || self.call_trace_call
            || self.call_trace_message
            || self.call_recording
            || self.leg_cdr
    }

    async fn ensure_schemas(self, conn: &DatabaseConnection) -> Result<()> {
        if self.call_cdr {
            ensure_cdr_schema(conn).await?;
        }
        if self.call_trace_call || self.call_trace_message {
            ensure_call_trace_schema(conn).await?;
        }
        if self.call_recording {
            ensure_recording_schema(conn).await?;
        }
        if self.leg_cdr {
            ensure_leg_cdr_schema(conn).await?;
        }
        Ok(())
    }

    async fn domain_ids(self, conn: &DatabaseConnection) -> Result<BTreeSet<String>> {
        let mut domain_ids = BTreeSet::new();
        for (table, exists) in [
            ("call_cdr", self.call_cdr),
            ("call_trace_call", self.call_trace_call),
            ("call_trace_message", self.call_trace_message),
            ("call_recording", self.call_recording),
            ("leg_cdr", self.leg_cdr),
        ] {
            if !exists {
                continue;
            }
            let rows = conn
                .query_all(Statement::from_string(
                    DbBackend::Sqlite,
                    format!("SELECT DISTINCT domain_id FROM {table}"),
                ))
                .await?;
            for row in rows {
                domain_ids.insert(row.try_get("", "domain_id")?);
            }
        }
        Ok(domain_ids)
    }
}

async fn table_exists(conn: &DatabaseConnection, table: &str) -> Result<bool> {
    Ok(conn
        .query_one(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
            vec![table.into()],
        ))
        .await?
        .is_some())
}

fn validate_domain_storage_id(domain_id: &str) -> Result<()> {
    if domain_id.is_empty()
        || domain_id == "."
        || domain_id == ".."
        || domain_id.contains('/')
        || domain_id.contains('\\')
    {
        bail!("invalid domain id in legacy runtime data: {domain_id:?}");
    }
    Ok(())
}

async fn migrate_legacy_domain(
    system: &DatabaseConnection,
    domain_path: &Path,
    domain_id: &str,
    tables: &LegacyRuntimeTables,
) -> Result<[u64; 5]> {
    system
        .execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "ATTACH DATABASE ? AS domain_cdr",
            vec![domain_path.to_string_lossy().into_owned().into()],
        ))
        .await?;

    let migration = async {
        let txn = system.begin().await?;
        let mut counts = [0; 5];

        if tables.call_cdr {
            txn.execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                r#"
                INSERT OR REPLACE INTO domain_cdr.call_cdr (
                    call_id, domain_id, caller_number, callee_number,
                    inbound_route_id, inbound_route_name, inbound_trunk_ref, inbound_trunk_name,
                    outbound_route_id, outbound_route_name, outbound_trunk_ref, outbound_trunk_name,
                    started_at_ms, answered_at_ms, ended_at_ms, duration_ms, billable_ms,
                    answered, final_status, hangup_cause, media_forwarding_mode,
                    caller_to_callee_packets, caller_to_callee_bytes,
                    callee_to_caller_packets, callee_to_caller_bytes,
                    caller_to_callee_rtcp_packets, callee_to_caller_rtcp_packets,
                    trace_available, incomplete, incomplete_reason
                )
                SELECT
                    call_id, domain_id, caller_number, callee_number,
                    inbound_route_id, inbound_route_name, inbound_trunk_ref, inbound_trunk_name,
                    outbound_route_id, outbound_route_name, outbound_trunk_ref, outbound_trunk_name,
                    started_at_ms, answered_at_ms, ended_at_ms, duration_ms, billable_ms,
                    answered, final_status, hangup_cause, media_forwarding_mode,
                    caller_to_callee_packets, caller_to_callee_bytes,
                    callee_to_caller_packets, callee_to_caller_bytes,
                    caller_to_callee_rtcp_packets, callee_to_caller_rtcp_packets,
                    trace_available, incomplete, incomplete_reason
                FROM main.call_cdr WHERE domain_id = ?
                "#,
                vec![domain_id.into()],
            ))
            .await?;
            counts[0] = delete_legacy_domain_rows(&txn, "call_cdr", domain_id).await?;
        }

        if tables.call_trace_call {
            txn.execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT OR REPLACE INTO domain_cdr.call_trace_call
                    (call_id,domain_id,first_observed_at_ms,ended_at_ms,incomplete)
                 SELECT call_id,domain_id,first_observed_at_ms,ended_at_ms,incomplete
                 FROM main.call_trace_call WHERE domain_id = ?",
                vec![domain_id.into()],
            ))
            .await?;
            counts[1] = delete_legacy_domain_rows(&txn, "call_trace_call", domain_id).await?;
        }

        if tables.call_trace_message {
            txn.execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT OR REPLACE INTO domain_cdr.call_trace_message
                    (call_id,domain_id,sequence,observed_at_ms,direction,adapter_call_leg_id,
                     session_id,source_addr,destination_addr,start_line,packet)
                 SELECT call_id,domain_id,sequence,observed_at_ms,direction,adapter_call_leg_id,
                     session_id,source_addr,destination_addr,start_line,packet
                 FROM main.call_trace_message WHERE domain_id = ?",
                vec![domain_id.into()],
            ))
            .await?;
            counts[2] = delete_legacy_domain_rows(&txn, "call_trace_message", domain_id).await?;
        }

        if tables.call_recording {
            txn.execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT OR REPLACE INTO domain_cdr.call_recording
                    (recording_id,call_id,domain_id,status,caller_number,callee_number,
                     started_at_ms,ended_at_ms,duration_ms,format,sample_rate,channel_count,
                     file_name,storage_root,storage_path,file_size_bytes,packets_tapped,
                     packets_dropped,error_code,error_message,created_at,updated_at)
                 SELECT recording_id,call_id,domain_id,status,caller_number,callee_number,
                     started_at_ms,ended_at_ms,duration_ms,format,sample_rate,channel_count,
                     file_name,storage_root,storage_path,file_size_bytes,packets_tapped,
                     packets_dropped,error_code,error_message,created_at,updated_at
                 FROM main.call_recording WHERE domain_id = ?",
                vec![domain_id.into()],
            ))
            .await?;
            counts[3] = delete_legacy_domain_rows(&txn, "call_recording", domain_id).await?;
        }

        if tables.leg_cdr {
            txn.execute(Statement::from_sql_and_values(
                DbBackend::Sqlite,
                "INSERT OR REPLACE INTO domain_cdr.leg_cdr
                    (call_id,session_id,domain_id,leg_role,direction,endpoint_ref,
                     endpoint_number,signaling_number,route_id,route_name,trunk_ref,trunk_name,
                     joined_at_ms,answered_at_ms,left_at_ms,final_status,hangup_cause,
                     media_packets,media_bytes,media_rtcp_packets,bridge_ids)
                 SELECT call_id,session_id,domain_id,leg_role,direction,endpoint_ref,
                     endpoint_number,signaling_number,route_id,route_name,trunk_ref,trunk_name,
                     joined_at_ms,answered_at_ms,left_at_ms,final_status,hangup_cause,
                     media_packets,media_bytes,media_rtcp_packets,bridge_ids
                 FROM main.leg_cdr WHERE domain_id = ?",
                vec![domain_id.into()],
            ))
            .await?;
            counts[4] = delete_legacy_domain_rows(&txn, "leg_cdr", domain_id).await?;
        }

        txn.commit().await?;
        Ok::<_, anyhow::Error>(counts)
    }
    .await;

    let detach = system
        .execute(Statement::from_string(
            DbBackend::Sqlite,
            "DETACH DATABASE domain_cdr".to_string(),
        ))
        .await;
    let counts = migration?;
    detach?;
    Ok(counts)
}

async fn delete_legacy_domain_rows<C>(conn: &C, table: &str, domain_id: &str) -> Result<u64>
where
    C: ConnectionTrait,
{
    Ok(conn
        .execute(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            format!("DELETE FROM main.{table} WHERE domain_id = ?"),
            vec![domain_id.into()],
        ))
        .await?
        .rows_affected())
}

async fn drop_legacy_runtime_tables(conn: &DatabaseConnection) -> Result<()> {
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        "DROP TABLE IF EXISTS call_trace_message;
         DROP TABLE IF EXISTS call_trace_call;
         DROP TABLE IF EXISTS call_recording;
         DROP TABLE IF EXISTS leg_cdr;
         DROP TABLE IF EXISTS call_cdr;"
            .to_string(),
    ))
    .await?;
    Ok(())
}

async fn ensure_cdr_schema(conn: &DatabaseConnection) -> Result<()> {
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
        CREATE TABLE IF NOT EXISTS call_cdr (
            call_id TEXT PRIMARY KEY,
            domain_id TEXT NOT NULL,
            caller_number TEXT NOT NULL,
            callee_number TEXT NOT NULL,
            inbound_route_id TEXT,
            inbound_route_name TEXT,
            inbound_trunk_ref TEXT,
            inbound_trunk_name TEXT,
            outbound_route_id TEXT,
            outbound_route_name TEXT,
            outbound_trunk_ref TEXT,
            outbound_trunk_name TEXT,
            started_at_ms INTEGER NOT NULL,
            answered_at_ms INTEGER,
            ended_at_ms INTEGER NOT NULL,
            duration_ms INTEGER NOT NULL,
            billable_ms INTEGER NOT NULL,
            answered INTEGER NOT NULL,
            final_status INTEGER,
            hangup_cause TEXT NOT NULL,
            media_forwarding_mode TEXT,
            caller_to_callee_packets INTEGER NOT NULL,
            caller_to_callee_bytes INTEGER NOT NULL,
            callee_to_caller_packets INTEGER NOT NULL,
            callee_to_caller_bytes INTEGER NOT NULL,
            caller_to_callee_rtcp_packets INTEGER NOT NULL,
            callee_to_caller_rtcp_packets INTEGER NOT NULL,
            trace_available INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_call_cdr_domain_ended
            ON call_cdr(domain_id, ended_at_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_call_cdr_ended
            ON call_cdr(ended_at_ms DESC);
        "#,
    ))
    .await?;
    ensure_column(conn, "call_cdr", "inbound_route_id", "TEXT").await?;
    ensure_column(conn, "call_cdr", "inbound_route_name", "TEXT").await?;
    ensure_column(conn, "call_cdr", "inbound_trunk_ref", "TEXT").await?;
    ensure_column(conn, "call_cdr", "inbound_trunk_name", "TEXT").await?;
    ensure_column(conn, "call_cdr", "outbound_route_id", "TEXT").await?;
    ensure_column(conn, "call_cdr", "outbound_route_name", "TEXT").await?;
    ensure_column(conn, "call_cdr", "outbound_trunk_ref", "TEXT").await?;
    ensure_column(conn, "call_cdr", "outbound_trunk_name", "TEXT").await?;
    ensure_column(conn, "call_cdr", "media_forwarding_mode", "TEXT").await?;
    ensure_column(
        conn,
        "call_cdr",
        "trace_available",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    ensure_column(conn, "call_cdr", "incomplete", "INTEGER NOT NULL DEFAULT 0").await?;
    ensure_column(conn, "call_cdr", "incomplete_reason", "TEXT").await?;
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        "UPDATE call_cdr
         SET media_forwarding_mode = 'userspace'
         WHERE media_forwarding_mode IS NULL
           AND (
               answered = 1
               OR caller_to_callee_packets > 0
               OR callee_to_caller_packets > 0
           )"
        .to_string(),
    ))
    .await?;
    Ok(())
}

async fn ensure_column(
    conn: &DatabaseConnection,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<()> {
    let rows = conn
        .query_all(Statement::from_string(
            DbBackend::Sqlite,
            format!("PRAGMA table_info({table})"),
        ))
        .await?;
    if rows.iter().any(|row| {
        row.try_get::<String>("", "name")
            .is_ok_and(|name| name == column)
    }) {
        return Ok(());
    }
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
    ))
    .await?;
    Ok(())
}

async fn ensure_call_trace_schema(conn: &DatabaseConnection) -> Result<()> {
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
        CREATE TABLE IF NOT EXISTS call_trace_call (
            call_id TEXT PRIMARY KEY,
            domain_id TEXT NOT NULL,
            first_observed_at_ms INTEGER NOT NULL,
            ended_at_ms INTEGER,
            incomplete INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_call_trace_call_ended
            ON call_trace_call(ended_at_ms DESC, call_id DESC);

        CREATE TABLE IF NOT EXISTS call_trace_message (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            call_id TEXT NOT NULL,
            domain_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            observed_at_ms INTEGER NOT NULL,
            direction TEXT NOT NULL,
            adapter_call_leg_id TEXT NOT NULL,
            session_id TEXT,
            source_addr TEXT,
            destination_addr TEXT,
            start_line TEXT NOT NULL,
            packet TEXT NOT NULL,
            UNIQUE(call_id, sequence)
        );
        CREATE INDEX IF NOT EXISTS idx_call_trace_message_call_sequence
            ON call_trace_message(call_id, sequence, id);
        "#,
    ))
    .await?;
    Ok(())
}

async fn ensure_recording_schema(conn: &DatabaseConnection) -> Result<()> {
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        "CREATE TABLE IF NOT EXISTS call_recording (
            recording_id TEXT PRIMARY KEY,
            call_id TEXT NOT NULL UNIQUE,
            domain_id TEXT NOT NULL,
            status TEXT NOT NULL,
            caller_number TEXT NOT NULL,
            callee_number TEXT NOT NULL,
            started_at_ms INTEGER NOT NULL,
            ended_at_ms INTEGER,
            duration_ms INTEGER NOT NULL DEFAULT 0,
            format TEXT NOT NULL,
            sample_rate INTEGER NOT NULL,
            channel_count INTEGER NOT NULL,
            file_name TEXT NOT NULL,
            storage_root TEXT NOT NULL,
            storage_path TEXT NOT NULL,
            file_size_bytes INTEGER NOT NULL DEFAULT 0,
            packets_tapped INTEGER NOT NULL DEFAULT 0,
            packets_dropped INTEGER NOT NULL DEFAULT 0,
            error_code TEXT,
            error_message TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_call_recording_domain_started
            ON call_recording(domain_id, started_at_ms DESC);
        CREATE INDEX IF NOT EXISTS idx_call_recording_status_ended
            ON call_recording(status, ended_at_ms);"
            .to_string(),
    ))
    .await?;
    Ok(())
}

fn recording_from_row(row: sea_orm::QueryResult) -> Result<RecordingRecord> {
    Ok(RecordingRecord {
        recording_id: row.try_get("", "recording_id")?,
        call_id: row.try_get("", "call_id")?,
        domain_id: row.try_get("", "domain_id")?,
        status: row.try_get("", "status")?,
        caller_number: row.try_get("", "caller_number")?,
        callee_number: row.try_get("", "callee_number")?,
        started_at_ms: as_u64(row.try_get("", "started_at_ms")?)?,
        ended_at_ms: row
            .try_get::<Option<i64>>("", "ended_at_ms")?
            .map(as_u64)
            .transpose()?,
        duration_ms: as_u64(row.try_get("", "duration_ms")?)?,
        format: row.try_get("", "format")?,
        sample_rate: u32::try_from(row.try_get::<i64>("", "sample_rate")?)
            .context("recording sample rate out of range")?,
        channel_count: u8::try_from(row.try_get::<i64>("", "channel_count")?)
            .context("recording channel count out of range")?,
        file_name: row.try_get("", "file_name")?,
        storage_root: row.try_get("", "storage_root")?,
        storage_path: row.try_get("", "storage_path")?,
        file_size_bytes: as_u64(row.try_get("", "file_size_bytes")?)?,
        packets_tapped: as_u64(row.try_get("", "packets_tapped")?)?,
        packets_dropped: as_u64(row.try_get("", "packets_dropped")?)?,
        error_code: row.try_get("", "error_code")?,
        error_message: row.try_get("", "error_message")?,
    })
}

fn as_i64(value: u64) -> Result<i64> {
    i64::try_from(value).context("value out of SQLite INTEGER range")
}

fn as_u64(value: i64) -> Result<u64> {
    u64::try_from(value).context("negative value in unsigned CDR column")
}

pub(crate) async fn connect_sqlite(db_path: &Path) -> Result<DatabaseConnection> {
    if let Some(parent) = db_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create sqlite data dir {}", parent.display()))?;
    }
    let url = format!("sqlite://{}?mode=rwc", db_path.display());
    let mut options = ConnectOptions::new(url.clone());
    options.max_connections(1).min_connections(1);
    let conn = Database::connect(options)
        .await
        .with_context(|| format!("connect {url}"))?;
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        "PRAGMA foreign_keys = ON".to_string(),
    ))
    .await?;
    Ok(conn)
}

async fn ensure_leg_cdr_schema(conn: &DatabaseConnection) -> Result<()> {
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
        CREATE TABLE IF NOT EXISTS leg_cdr (
            call_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            domain_id TEXT NOT NULL,
            leg_role TEXT NOT NULL,
            direction TEXT NOT NULL,
            endpoint_ref TEXT,
            endpoint_number TEXT,
            signaling_number TEXT,
            route_id TEXT,
            route_name TEXT,
            trunk_ref TEXT,
            trunk_name TEXT,
            joined_at_ms INTEGER NOT NULL,
            answered_at_ms INTEGER,
            left_at_ms INTEGER NOT NULL,
            final_status INTEGER,
            hangup_cause TEXT,
            media_packets INTEGER NOT NULL DEFAULT 0,
            media_bytes INTEGER NOT NULL DEFAULT 0,
            media_rtcp_packets INTEGER NOT NULL DEFAULT 0,
            bridge_ids TEXT NOT NULL DEFAULT '',
            PRIMARY KEY(call_id, session_id)
        );
        CREATE INDEX IF NOT EXISTS idx_leg_cdr_call
            ON leg_cdr(call_id);
        "#,
    ))
    .await?;
    Ok(())
}

async fn ensure_ai_result_schema(conn: &DatabaseConnection) -> Result<()> {
    conn.execute(Statement::from_string(
        DbBackend::Sqlite,
        r#"
        CREATE TABLE IF NOT EXISTS ai_call_result (
            job_id TEXT NOT NULL,
            result_version INTEGER NOT NULL,
            domain_id TEXT NOT NULL,
            call_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            generation INTEGER NOT NULL,
            profile_id TEXT NOT NULL,
            profile_version INTEGER NOT NULL,
            capture_quality TEXT NOT NULL,
            transcript_json TEXT NOT NULL,
            result_schema_version INTEGER NOT NULL,
            summary TEXT NOT NULL,
            purpose TEXT NOT NULL,
            outcome TEXT NOT NULL,
            key_points_json TEXT NOT NULL,
            action_items_json TEXT NOT NULL,
            tags_json TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            received_at_ms INTEGER NOT NULL,
            PRIMARY KEY(job_id,result_version),
            CHECK(result_version > 0),
            CHECK(profile_version > 0),
            CHECK(capture_quality IN ('complete','incomplete_processable','insufficient'))
        );
        CREATE INDEX IF NOT EXISTS idx_ai_call_result_call
            ON ai_call_result(call_id,result_version DESC);
        CREATE INDEX IF NOT EXISTS idx_ai_call_result_received
            ON ai_call_result(received_at_ms DESC,job_id);
        "#,
    ))
    .await?;
    Ok(())
}

fn capture_quality_name(quality: ai_protocol::control::CaptureQuality) -> &'static str {
    match quality {
        ai_protocol::control::CaptureQuality::Complete => "complete",
        ai_protocol::control::CaptureQuality::IncompleteProcessable => "incomplete_processable",
        ai_protocol::control::CaptureQuality::Insufficient => "insufficient",
    }
}

fn leg_cdr_from_row(row: &sea_orm::QueryResult) -> Result<LegCdrRecord> {
    Ok(LegCdrRecord {
        call_id: row.try_get("", "call_id")?,
        session_id: row.try_get("", "session_id")?,
        domain_id: row.try_get("", "domain_id")?,
        leg_role: row.try_get("", "leg_role")?,
        direction: row.try_get("", "direction")?,
        endpoint_ref: row.try_get("", "endpoint_ref")?,
        endpoint_number: row.try_get("", "endpoint_number")?,
        signaling_number: row.try_get("", "signaling_number")?,
        route_id: row.try_get("", "route_id")?,
        route_name: row.try_get("", "route_name")?,
        trunk_ref: row.try_get("", "trunk_ref")?,
        trunk_name: row.try_get("", "trunk_name")?,
        joined_at_ms: as_u64(row.try_get::<i64>("", "joined_at_ms")?)?,
        answered_at_ms: row
            .try_get::<Option<i64>>("", "answered_at_ms")?
            .map(|v| as_u64(v).unwrap_or(0)),
        left_at_ms: as_u64(row.try_get::<i64>("", "left_at_ms")?)?,
        final_status: row
            .try_get::<Option<i64>>("", "final_status")?
            .map(|v| v as u16),
        hangup_cause: row.try_get("", "hangup_cause")?,
        media_packets: as_u64(row.try_get::<i64>("", "media_packets")?)?,
        media_bytes: as_u64(row.try_get::<i64>("", "media_bytes")?)?,
        media_rtcp_packets: as_u64(row.try_get::<i64>("", "media_rtcp_packets")?)?,
        bridge_ids: row.try_get("", "bridge_ids")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ai_protocol::control::{CaptureQuality, StructuredCallResult, TranscriptSegment};
    use ai_protocol::id::{JobId, OperationId, ParticipantId, ProfileId};

    fn ai_result() -> AiCallResultRecord {
        AiCallResultRecord {
            job_id: JobId::new("job-ai-1").unwrap(),
            result_version: 1,
            domain_id: "domain-a".to_string(),
            call_id: "call-ai-1".to_string(),
            operation_id: OperationId::new("post-call-v1").unwrap(),
            generation: 1,
            profile_id: ProfileId::new("profile-1").unwrap(),
            profile_version: 3,
            capture_quality: CaptureQuality::Complete,
            transcript: vec![TranscriptSegment {
                participant_id: ParticipantId::new("caller").unwrap(),
                start_ms: 0,
                end_ms: 1_000,
                text: "hello".to_string(),
                final_segment: true,
            }],
            result: StructuredCallResult {
                schema_version: 1,
                summary: "summary".to_string(),
                purpose: "support".to_string(),
                outcome: "resolved".to_string(),
                key_points: vec!["point".to_string()],
                action_items: vec!["follow up".to_string()],
                tags: vec!["support".to_string()],
            },
            received_at_ms: 1_000,
        }
    }

    #[test]
    fn persists_domain_ai_results_idempotently_and_rejects_collisions() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let backend = SeaOrmConfigBackend::sqlite(temp.path(), "test")?;
        let record = ai_result();
        backend.persist_ai_result(&record)?;
        backend.persist_ai_result(&record)?;
        assert_eq!(
            backend.get_ai_results(&record.call_id, &record.domain_id)?,
            vec![record.clone()]
        );

        let mut collision = record;
        collision.result.summary = "different".to_string();
        let error = backend.persist_ai_result(&collision).unwrap_err();
        assert!(error.to_string().contains("AI result collision"));
        Ok(())
    }

    #[test]
    fn persists_and_pages_cdr_records() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let backend = SeaOrmConfigBackend::sqlite(temp.path(), "test")?;
        backend.health_check()?;
        let record = CdrRecord {
            call_id: "call-1".to_string(),
            domain_id: "domain-a".to_string(),
            caller_number: "1001".to_string(),
            callee_number: "1002".to_string(),
            inbound_route_id: None,
            inbound_route_name: None,
            inbound_trunk_ref: None,
            inbound_trunk_name: None,
            outbound_route_id: Some("route-1".to_string()),
            outbound_route_name: Some("本地出局".to_string()),
            outbound_trunk_ref: Some("reg:1/1".to_string()),
            outbound_trunk_name: Some("reg-5.182/5217".to_string()),
            started_at_ms: 100,
            answered_at_ms: Some(200),
            ended_at_ms: 1_200,
            duration_ms: 1_100,
            billable_ms: 1_000,
            answered: true,
            final_status: Some(200),
            hangup_cause: "normal_clearing".to_string(),
            media_forwarding_mode: Some(MediaForwardingMode::Userspace),
            caller_to_callee_packets: 10,
            caller_to_callee_bytes: 1_600,
            callee_to_caller_packets: 12,
            callee_to_caller_bytes: 1_920,
            caller_to_callee_rtcp_packets: 1,
            callee_to_caller_rtcp_packets: 2,
            trace_available: false,
            trace_incomplete: false,
            recording_status: None,
            recording_available: false,
            incomplete: false,
            incomplete_reason: None,
        };
        backend.insert_cdr(&record)?;

        let page = backend.list_cdr(
            Some("domain-a"),
            PageRequest {
                page: 1,
                page_size: 20,
            },
        )?;
        assert_eq!(page.total, 1);
        assert_eq!(page.rows, vec![record]);
        Ok(())
    }

    #[test]
    fn disabled_trace_batch_does_not_create_or_expose_an_empty_trace() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let backend = SeaOrmConfigBackend::sqlite(temp.path(), "test")?;
        let record = CdrRecord {
            call_id: "call-trace-disabled".to_string(),
            domain_id: "domain-a".to_string(),
            caller_number: "1001".to_string(),
            callee_number: "1002".to_string(),
            inbound_route_id: None,
            inbound_route_name: None,
            inbound_trunk_ref: None,
            inbound_trunk_name: None,
            outbound_route_id: None,
            outbound_route_name: None,
            outbound_trunk_ref: None,
            outbound_trunk_name: None,
            started_at_ms: 100,
            answered_at_ms: Some(200),
            ended_at_ms: 1_200,
            duration_ms: 1_100,
            billable_ms: 1_000,
            answered: true,
            final_status: Some(200),
            hangup_cause: "normal_clearing".to_string(),
            media_forwarding_mode: Some(MediaForwardingMode::Userspace),
            caller_to_callee_packets: 10,
            caller_to_callee_bytes: 1_600,
            callee_to_caller_packets: 12,
            callee_to_caller_bytes: 1_920,
            caller_to_callee_rtcp_packets: 1,
            callee_to_caller_rtcp_packets: 2,
            trace_available: false,
            trace_incomplete: false,
            recording_status: None,
            recording_available: false,
            incomplete: false,
            incomplete_reason: None,
        };
        backend.persist_cdr_batch(&super::super::CdrWriteCommand {
            call_cdr: record.clone(),
            leg_cdrs: Vec::new(),
            recording: None,
            trace_call_id: record.call_id.clone(),
            trace_domain_id: record.domain_id.clone(),
            trace_ended_at_ms: record.ended_at_ms,
        })?;

        assert!(
            backend
                .get_call_trace(&record.call_id, Some(&record.domain_id))?
                .is_none()
        );

        // Existing databases can contain empty trace indexes created by older builds.
        backend.complete_call_trace(
            &record.call_id,
            &record.domain_id,
            record.ended_at_ms,
            false,
        )?;
        let listed = backend.list_cdr(
            Some(&record.domain_id),
            PageRequest {
                page: 1,
                page_size: 20,
            },
        )?;
        assert_eq!(listed.rows, vec![record]);
        Ok(())
    }

    #[test]
    fn migrates_existing_cdr_table_with_route_columns() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let backend = SeaOrmConfigBackend::sqlite(temp.path(), "test")?;
        backend.block_on(async {
            let conn = backend.open_system_db().await?;
            conn.execute(Statement::from_string(
                DbBackend::Sqlite,
                r#"
                CREATE TABLE call_cdr (
                    call_id TEXT PRIMARY KEY,
                    domain_id TEXT NOT NULL,
                    caller_number TEXT NOT NULL,
                    callee_number TEXT NOT NULL,
                    started_at_ms INTEGER NOT NULL,
                    answered_at_ms INTEGER,
                    ended_at_ms INTEGER NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    billable_ms INTEGER NOT NULL,
                    answered INTEGER NOT NULL,
                    final_status INTEGER,
                    hangup_cause TEXT NOT NULL,
                    caller_to_callee_packets INTEGER NOT NULL,
                    caller_to_callee_bytes INTEGER NOT NULL,
                    callee_to_caller_packets INTEGER NOT NULL,
                    callee_to_caller_bytes INTEGER NOT NULL,
                    caller_to_callee_rtcp_packets INTEGER NOT NULL,
                    callee_to_caller_rtcp_packets INTEGER NOT NULL
                );
                INSERT INTO call_cdr VALUES (
                    'legacy-call', 'domain-a', '1001', '5219',
                    100, NULL, 200, 100, 0, 0, 480, 'unavailable',
                    0, 0, 0, 0, 0, 0
                );
                "#,
            ))
            .await?;
            Ok::<_, anyhow::Error>(())
        })?;

        backend.health_check()?;
        let record = CdrRecord {
            call_id: "legacy-call".to_string(),
            domain_id: "domain-a".to_string(),
            caller_number: "1001".to_string(),
            callee_number: "5219".to_string(),
            inbound_route_id: None,
            inbound_route_name: None,
            inbound_trunk_ref: None,
            inbound_trunk_name: None,
            outbound_route_id: None,
            outbound_route_name: None,
            outbound_trunk_ref: None,
            outbound_trunk_name: None,
            started_at_ms: 100,
            answered_at_ms: None,
            ended_at_ms: 200,
            duration_ms: 100,
            billable_ms: 0,
            answered: false,
            final_status: Some(480),
            hangup_cause: "unavailable".to_string(),
            media_forwarding_mode: None,
            caller_to_callee_packets: 0,
            caller_to_callee_bytes: 0,
            callee_to_caller_packets: 0,
            callee_to_caller_bytes: 0,
            caller_to_callee_rtcp_packets: 0,
            callee_to_caller_rtcp_packets: 0,
            trace_available: false,
            trace_incomplete: false,
            recording_status: None,
            recording_available: false,
            incomplete: false,
            incomplete_reason: None,
        };
        assert_eq!(
            backend
                .list_cdr(
                    None,
                    PageRequest {
                        page: 1,
                        page_size: 1
                    }
                )?
                .rows,
            vec![record]
        );
        backend.block_on(async {
            let conn = backend.open_system_db().await?;
            assert!(!table_exists(&conn, "call_cdr").await?);
            Ok::<_, anyhow::Error>(())
        })?;
        backend.health_check()?;
        Ok(())
    }

    #[test]
    fn migrates_trace_recording_and_leg_runtime_tables() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let backend = SeaOrmConfigBackend::sqlite(temp.path(), "test")?;
        backend.block_on(async {
            let conn = backend.open_system_db().await?;
            ensure_call_trace_schema(&conn).await?;
            ensure_recording_schema(&conn).await?;
            ensure_leg_cdr_schema(&conn).await?;
            conn.execute(Statement::from_string(
                DbBackend::Sqlite,
                r#"
                INSERT INTO call_trace_call VALUES
                    ('legacy-runtime-call','domain-a',100,200,1);
                INSERT INTO call_trace_message
                    (call_id,domain_id,sequence,observed_at_ms,direction,adapter_call_leg_id,
                     session_id,source_addr,destination_addr,start_line,packet)
                VALUES
                    ('legacy-runtime-call','domain-a',1,100,'rx','41','session-a',
                     '192.0.2.10:5060','192.0.2.20:5060','INVITE sip:1002@example.test SIP/2.0',
                     'INVITE sip:1002@example.test SIP/2.0');
                INSERT INTO call_recording
                    (recording_id,call_id,domain_id,status,caller_number,callee_number,
                     started_at_ms,ended_at_ms,duration_ms,format,sample_rate,channel_count,
                     file_name,storage_root,storage_path,file_size_bytes,packets_tapped,
                     packets_dropped,error_code,error_message,created_at,updated_at)
                VALUES
                    ('recording-legacy','legacy-runtime-call','domain-a','complete','1001','1002',
                     100,200,100,'wav',8000,2,'legacy.wav','/tmp','/tmp/legacy.wav',44,10,0,
                     NULL,NULL,100,200);
                INSERT INTO leg_cdr
                    (call_id,session_id,domain_id,leg_role,direction,endpoint_ref,
                     endpoint_number,signaling_number,route_id,route_name,trunk_ref,trunk_name,
                     joined_at_ms,answered_at_ms,left_at_ms,final_status,hangup_cause,
                     media_packets,media_bytes,media_rtcp_packets,bridge_ids)
                VALUES
                    ('legacy-runtime-call','session-a','domain-a','caller','inbound','ext-1001',
                     '1001','1001',NULL,NULL,NULL,NULL,100,120,200,200,'normal_clearing',
                     10,1600,1,'bridge-1');
                "#,
            ))
            .await?;
            Ok::<_, anyhow::Error>(())
        })?;

        backend.health_check()?;

        let trace = backend
            .get_call_trace("legacy-runtime-call", Some("domain-a"))?
            .expect("migrated trace should exist");
        assert!(trace.incomplete);
        assert_eq!(trace.messages.len(), 1);
        assert_eq!(trace.messages[0].session_id.as_deref(), Some("session-a"));

        let recording = backend
            .get_recording("legacy-runtime-call", Some("domain-a"))?
            .expect("migrated recording should exist");
        assert_eq!(recording.recording_id, "recording-legacy");

        let legs = backend.list_leg_cdrs("legacy-runtime-call", Some("domain-a"))?;
        assert_eq!(legs.len(), 1);
        assert_eq!(legs[0].session_id, "session-a");

        backend.block_on(async {
            let conn = backend.open_system_db().await?;
            for table in [
                "call_trace_call",
                "call_trace_message",
                "call_recording",
                "leg_cdr",
            ] {
                assert!(!table_exists(&conn, table).await?);
            }
            Ok::<_, anyhow::Error>(())
        })?;
        backend.health_check()?;
        Ok(())
    }

    #[test]
    fn persists_raw_call_trace_and_marks_cdr_availability() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let backend = SeaOrmConfigBackend::sqlite(temp.path(), "test")?;
        let mut cdr = CdrRecord {
            call_id: "call-trace".to_string(),
            domain_id: "domain-a".to_string(),
            caller_number: "1001".to_string(),
            callee_number: "1002".to_string(),
            inbound_route_id: None,
            inbound_route_name: None,
            inbound_trunk_ref: None,
            inbound_trunk_name: None,
            outbound_route_id: None,
            outbound_route_name: None,
            outbound_trunk_ref: None,
            outbound_trunk_name: None,
            started_at_ms: 100,
            answered_at_ms: Some(200),
            ended_at_ms: 1_200,
            duration_ms: 1_100,
            billable_ms: 1_000,
            answered: true,
            final_status: Some(200),
            hangup_cause: "normal_clearing".to_string(),
            media_forwarding_mode: Some(MediaForwardingMode::Userspace),
            caller_to_callee_packets: 10,
            caller_to_callee_bytes: 1_600,
            callee_to_caller_packets: 12,
            callee_to_caller_bytes: 1_920,
            caller_to_callee_rtcp_packets: 1,
            callee_to_caller_rtcp_packets: 2,
            trace_available: false,
            trace_incomplete: false,
            recording_status: None,
            recording_available: false,
            incomplete: false,
            incomplete_reason: None,
        };
        backend.insert_cdr(&cdr)?;
        backend.insert_call_trace_message(&CallTraceMessage {
            call_id: cdr.call_id.clone(),
            domain_id: cdr.domain_id.clone(),
            sequence: 7,
            observed_at_ms: 101,
            direction: "rx".to_string(),
            adapter_call_leg_id: "41".to_string(),
            session_id: Some("session-caller".to_string()),
            source_addr: Some("192.0.2.10:5060".to_string()),
            destination_addr: None,
            start_line: "INVITE sip:1002@example.test SIP/2.0".to_string(),
            packet: "INVITE sip:1002@example.test SIP/2.0\r\nAuthorization: raw\r\n\r\n"
                .to_string(),
        })?;
        backend.complete_call_trace(&cdr.call_id, &cdr.domain_id, cdr.ended_at_ms, true)?;

        let trace = backend
            .get_call_trace(&cdr.call_id, Some(&cdr.domain_id))?
            .expect("trace should exist");
        assert!(trace.incomplete);
        assert_eq!(trace.messages.len(), 1);
        assert!(trace.messages[0].packet.contains("Authorization: raw"));

        cdr.trace_available = true;
        cdr.trace_incomplete = true;
        let page = backend.list_cdr(
            Some("domain-a"),
            PageRequest {
                page: 1,
                page_size: 20,
            },
        )?;
        assert_eq!(page.rows, vec![cdr]);
        Ok(())
    }

    #[test]
    fn completed_trace_index_exists_for_unanswered_calls() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let backend = SeaOrmConfigBackend::sqlite(temp.path(), "test")?;
        let cdr = CdrRecord {
            call_id: "unanswered-trace".to_string(),
            domain_id: "domain-a".to_string(),
            caller_number: "1001".to_string(),
            callee_number: "1002".to_string(),
            inbound_route_id: None,
            inbound_route_name: None,
            inbound_trunk_ref: None,
            inbound_trunk_name: None,
            outbound_route_id: None,
            outbound_route_name: None,
            outbound_trunk_ref: None,
            outbound_trunk_name: None,
            started_at_ms: 100,
            answered_at_ms: None,
            ended_at_ms: 500,
            duration_ms: 400,
            billable_ms: 0,
            answered: false,
            final_status: Some(480),
            hangup_cause: "unavailable".to_string(),
            media_forwarding_mode: None,
            caller_to_callee_packets: 0,
            caller_to_callee_bytes: 0,
            callee_to_caller_packets: 0,
            callee_to_caller_bytes: 0,
            caller_to_callee_rtcp_packets: 0,
            callee_to_caller_rtcp_packets: 0,
            trace_available: true,
            trace_incomplete: false,
            recording_status: None,
            recording_available: false,
            incomplete: false,
            incomplete_reason: None,
        };
        backend.insert_cdr(&cdr)?;
        backend.complete_call_trace(&cdr.call_id, &cdr.domain_id, cdr.ended_at_ms, false)?;

        let trace = backend
            .get_call_trace(&cdr.call_id, Some(&cdr.domain_id))?
            .expect("unanswered call should have a trace index");
        assert_eq!(trace.domain_id, cdr.domain_id);
        assert_eq!(trace.ended_at_ms, Some(cdr.ended_at_ms));
        assert!(trace.messages.is_empty());
        assert!(
            backend
                .list_cdr(
                    Some(&cdr.domain_id),
                    PageRequest {
                        page: 1,
                        page_size: 1,
                    },
                )?
                .rows[0]
                .trace_available
        );
        Ok(())
    }

    #[test]
    fn persists_and_expires_recording_metadata() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let backend = SeaOrmConfigBackend::sqlite(temp.path(), "test")?;
        let root = temp.path().join("recordings");
        std::fs::create_dir_all(&root)?;
        let path = root.join("call.wav");
        std::fs::write(&path, b"RIFFrecording")?;
        let record = RecordingRecord {
            recording_id: "recording-call-1".to_string(),
            call_id: "call-1".to_string(),
            domain_id: "domain-a".to_string(),
            status: "complete".to_string(),
            caller_number: "1001".to_string(),
            callee_number: "1002".to_string(),
            started_at_ms: 100,
            ended_at_ms: Some(200),
            duration_ms: 100,
            format: "wav".to_string(),
            sample_rate: 8_000,
            channel_count: 2,
            file_name: "call.wav".to_string(),
            storage_root: root.to_string_lossy().into_owned(),
            storage_path: path.to_string_lossy().into_owned(),
            file_size_bytes: 13,
            packets_tapped: 2,
            packets_dropped: 0,
            error_code: None,
            error_message: None,
        };
        backend.upsert_recording(&record)?;
        assert_eq!(
            backend.get_recording("call-1", Some("domain-a"))?,
            Some(record)
        );

        assert_eq!(backend.cleanup_recordings(1, 20, 86_400_201)?, 1);
        assert!(!path.exists());
        let expired = backend.get_recording("call-1", Some("domain-a"))?.unwrap();
        assert_eq!(expired.status, "expired");
        assert!(expired.storage_path.is_empty());
        Ok(())
    }

    #[test]
    fn retains_only_latest_one_hundred_completed_call_traces() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let backend = SeaOrmConfigBackend::sqlite(temp.path(), "test")?;
        for index in 1..=101_u64 {
            let call_id = format!("call-{index:03}");
            backend.insert_call_trace_message(&CallTraceMessage {
                call_id: call_id.clone(),
                domain_id: "domain-a".to_string(),
                sequence: index,
                observed_at_ms: index,
                direction: "tx".to_string(),
                adapter_call_leg_id: "42".to_string(),
                session_id: None,
                source_addr: None,
                destination_addr: Some("192.0.2.20:5060".to_string()),
                start_line: "SIP/2.0 100 Trying".to_string(),
                packet: "SIP/2.0 100 Trying\r\n\r\n".to_string(),
            })?;
            backend.complete_call_trace(&call_id, "domain-a", index, false)?;
        }
        assert!(
            backend
                .get_call_trace("call-001", Some("domain-a"))?
                .is_none()
        );
        assert!(
            backend
                .get_call_trace("call-002", Some("domain-a"))?
                .is_some()
        );
        assert!(
            backend
                .get_call_trace("call-101", Some("domain-a"))?
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn incomplete_marker_can_arrive_after_trace_completion() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let backend = SeaOrmConfigBackend::sqlite(temp.path(), "test")?;
        let call_id = "call-late-incomplete";
        backend.insert_call_trace_message(&CallTraceMessage {
            call_id: call_id.to_string(),
            domain_id: "domain-a".to_string(),
            sequence: 1,
            observed_at_ms: 10,
            direction: "rx".to_string(),
            adapter_call_leg_id: "9".to_string(),
            session_id: None,
            source_addr: None,
            destination_addr: None,
            start_line: "INVITE sip:1002@example.test SIP/2.0".to_string(),
            packet: "INVITE sip:1002@example.test SIP/2.0\r\n\r\n".to_string(),
        })?;
        backend.complete_call_trace(call_id, "domain-a", 20, false)?;
        backend.mark_call_trace_incomplete(call_id, "domain-a")?;

        assert!(
            backend
                .get_call_trace(call_id, Some("domain-a"))?
                .expect("trace should exist")
                .incomplete
        );
        Ok(())
    }
}
