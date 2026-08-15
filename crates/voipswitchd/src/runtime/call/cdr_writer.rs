use super::cdr_spool::{CdrSpool, ReplayEntry};
use crate::app::AppState;
use crate::config_service::CdrSpoolLimits;
use crate::data_store::CdrWriteCommand;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

const CDR_WRITER_QUEUE_CAPACITY: usize = 512;
const MAX_RETRY_ATTEMPTS: u32 = 5;
const BASE_RETRY_DELAY_MS: u64 = 200;
const MAX_RETRY_DELAY_MS: u64 = 10_000;
const MIB: u64 = 1024 * 1024;

pub(crate) enum CdrWriterMessage {
    #[allow(clippy::large_enum_variant)]
    Write {
        command: Box<CdrWriteCommand>,
        durable_tx: oneshot::Sender<Result<(), String>>,
    },
    #[allow(dead_code)]
    Shutdown,
}

#[derive(Clone)]
pub(crate) struct CdrWriter {
    tx: mpsc::Sender<CdrWriterMessage>,
    admission: Arc<SpoolAdmission>,
}

#[derive(Clone)]
struct DomainWorkerHandle {
    wake_tx: mpsc::Sender<()>,
    spool_lock: Arc<Mutex<()>>,
}

struct SpoolAdmission {
    domains: Mutex<HashMap<String, DomainSpoolStatus>>,
    limits: RwLock<CdrSpoolLimits>,
}

#[derive(Default)]
struct DomainSpoolStatus {
    backlog_bytes: u64,
    rejecting_new_calls: bool,
    warning_active: bool,
}

impl SpoolAdmission {
    fn new(limits: CdrSpoolLimits) -> Self {
        Self {
            domains: Mutex::new(HashMap::new()),
            limits: RwLock::new(limits),
        }
    }

    fn update(&self, domain_id: &str, backlog_bytes: u64) {
        let limits = *self.limits.read().expect("CDR spool limits lock");
        let warning_bytes = limits.warning_mb.saturating_mul(MIB);
        let reject_bytes = limits.reject_mb.saturating_mul(MIB);
        let resume_bytes = limits.resume_mb.saturating_mul(MIB);
        let mut domains = self.domains.lock().expect("CDR spool admission lock");
        let status = domains.entry(domain_id.to_string()).or_default();
        status.backlog_bytes = backlog_bytes;

        if !status.warning_active && backlog_bytes >= warning_bytes {
            status.warning_active = true;
            warn!(
                domain_id,
                backlog_bytes, "domain CDR spool reached warning watermark"
            );
        } else if status.warning_active && backlog_bytes < warning_bytes {
            status.warning_active = false;
            info!(
                domain_id,
                backlog_bytes, "domain CDR spool cleared warning watermark"
            );
        }

        if !status.rejecting_new_calls && backlog_bytes >= reject_bytes {
            status.rejecting_new_calls = true;
            error!(
                domain_id,
                backlog_bytes, "domain CDR spool is rejecting new calls"
            );
        } else if status.rejecting_new_calls && backlog_bytes < resume_bytes {
            status.rejecting_new_calls = false;
            info!(
                domain_id,
                backlog_bytes, "domain CDR spool resumed call admission"
            );
        }
    }

    fn admits_new_call(&self, domain_id: &str) -> bool {
        self.domains
            .lock()
            .expect("CDR spool admission lock")
            .get(domain_id)
            .is_none_or(|status| !status.rejecting_new_calls)
    }

    fn set_limits(&self, limits: CdrSpoolLimits) {
        *self.limits.write().expect("CDR spool limits lock") = limits;
        let backlogs = self
            .domains
            .lock()
            .expect("CDR spool admission lock")
            .iter()
            .map(|(domain_id, status)| (domain_id.clone(), status.backlog_bytes))
            .collect::<Vec<_>>();
        for (domain_id, backlog_bytes) in backlogs {
            self.update(&domain_id, backlog_bytes);
        }
    }
}

impl CdrWriter {
    pub(crate) fn spawn(state: AppState) -> Self {
        let (tx, rx) = mpsc::channel(CDR_WRITER_QUEUE_CAPACITY);
        let admission = Arc::new(SpoolAdmission::new(
            state.config().snapshot().cdr_spool_limits(),
        ));
        tokio::spawn(run_writer(rx, state, admission.clone()));
        Self { tx, admission }
    }

    pub(crate) async fn enqueue_durable(&self, command: CdrWriteCommand) -> Result<()> {
        let (durable_tx, durable_rx) = oneshot::channel();
        self.tx
            .send(CdrWriterMessage::Write {
                command: Box::new(command),
                durable_tx,
            })
            .await
            .context("CDR writer queue closed")?;
        durable_rx
            .await
            .context("CDR writer durable acknowledgement dropped")?
            .map_err(anyhow::Error::msg)
    }

    pub(crate) fn admits_new_call(&self, domain_id: &str) -> bool {
        self.admission.admits_new_call(domain_id)
    }

    pub(crate) fn refresh_admission(&self, limits: CdrSpoolLimits) {
        self.admission.set_limits(limits);
    }

    #[allow(dead_code)]
    pub(crate) async fn shutdown(&self) {
        let _ = self.tx.send(CdrWriterMessage::Shutdown).await;
    }
}

async fn run_writer(
    mut rx: mpsc::Receiver<CdrWriterMessage>,
    state: AppState,
    admission: Arc<SpoolAdmission>,
) {
    let mut workers: HashMap<String, DomainWorkerHandle> = HashMap::new();
    start_replay_workers(&state, &admission, &mut workers).await;
    info!("CDR writer started");

    while let Some(message) = rx.recv().await {
        match message {
            CdrWriterMessage::Write {
                command,
                durable_tx,
            } => {
                let result = append_and_wake(&state, &admission, &mut workers, *command).await;
                let _ = durable_tx.send(result.map_err(|err| err.to_string()));
            }
            CdrWriterMessage::Shutdown => {
                info!("CDR writer shutting down, durably spooling queued messages");
                while let Ok(CdrWriterMessage::Write {
                    command,
                    durable_tx,
                }) = rx.try_recv()
                {
                    let result = append_and_wake(&state, &admission, &mut workers, *command).await;
                    let _ = durable_tx.send(result.map_err(|err| err.to_string()));
                }
                break;
            }
        }
    }
    info!("CDR writer stopped");
}

async fn start_replay_workers(
    state: &AppState,
    admission: &Arc<SpoolAdmission>,
    workers: &mut HashMap<String, DomainWorkerHandle>,
) {
    let backend = state.backend();
    let domains = tokio::task::spawn_blocking(move || backend.list_cdr_spool_domains()).await;
    match domains {
        Ok(Ok(domains)) => {
            for domain_id in domains {
                let worker = ensure_domain_worker(state, admission, workers, &domain_id);
                refresh_backlog(state, admission, &domain_id, &worker.spool_lock).await;
                wake_domain_worker(state, admission, workers, &domain_id);
            }
        }
        Ok(Err(err)) => error!(error = %err, "list CDR spool domains failed"),
        Err(err) => error!(error = %err, "CDR spool discovery task failed"),
    }
}

async fn append_and_wake(
    state: &AppState,
    admission: &Arc<SpoolAdmission>,
    workers: &mut HashMap<String, DomainWorkerHandle>,
    command: CdrWriteCommand,
) -> Result<()> {
    let call_id = command.call_cdr.call_id.clone();
    let domain_id = command.call_cdr.domain_id.clone();
    let worker = ensure_domain_worker(state, admission, workers, &domain_id);
    let backend = state.backend();
    let append_domain_id = domain_id.clone();
    let spool_lock = worker.spool_lock.clone();
    let result = tokio::task::spawn_blocking(move || {
        let _guard = spool_lock.lock().expect("domain CDR spool lock");
        let root = backend.cdr_spool_dir(&append_domain_id)?;
        let spool = CdrSpool::open(root)?;
        let position = spool.append(&command)?;
        Ok::<_, anyhow::Error>((position, spool.backlog_bytes()?))
    })
    .await;

    match result {
        Ok(Ok((position, backlog_bytes))) => {
            admission.update(&domain_id, backlog_bytes);
            info!(
                call_id,
                domain_id,
                ?position,
                "CDR batch appended to durable spool"
            );
            let _ = worker.wake_tx.try_send(());
            Ok(())
        }
        Ok(Err(err)) => {
            error!(
                call_id,
                domain_id,
                error = %err,
                "CDR durable spool append failed"
            );
            Err(err)
        }
        Err(err) => {
            error!(
                call_id,
                domain_id,
                error = %err,
                "CDR spool append task failed"
            );
            Err(err).context("CDR spool append task failed")
        }
    }
}

fn wake_domain_worker(
    state: &AppState,
    admission: &Arc<SpoolAdmission>,
    workers: &mut HashMap<String, DomainWorkerHandle>,
    domain_id: &str,
) {
    let worker = ensure_domain_worker(state, admission, workers, domain_id);
    let _ = worker.wake_tx.try_send(());
}

fn ensure_domain_worker(
    state: &AppState,
    admission: &Arc<SpoolAdmission>,
    workers: &mut HashMap<String, DomainWorkerHandle>,
    domain_id: &str,
) -> DomainWorkerHandle {
    workers
        .entry(domain_id.to_string())
        .or_insert_with(|| {
            let (tx, rx) = mpsc::channel(1);
            let spool_lock = Arc::new(Mutex::new(()));
            tokio::spawn(run_domain_worker(
                domain_id.to_string(),
                state.clone(),
                admission.clone(),
                spool_lock.clone(),
                rx,
            ));
            DomainWorkerHandle {
                wake_tx: tx,
                spool_lock,
            }
        })
        .clone()
}

async fn run_domain_worker(
    domain_id: String,
    state: AppState,
    admission: Arc<SpoolAdmission>,
    spool_lock: Arc<Mutex<()>>,
    mut wake_rx: mpsc::Receiver<()>,
) {
    info!(domain_id, "domain CDR writer started");
    while wake_rx.recv().await.is_some() {
        while wake_rx.try_recv().is_ok() {}
        loop {
            let entries = match load_replay_entries(&state, &domain_id, &spool_lock).await {
                Ok(entries) => entries,
                Err(err) => {
                    error!(domain_id, error = %err, "load domain CDR spool failed");
                    tokio::time::sleep(Duration::from_millis(MAX_RETRY_DELAY_MS)).await;
                    continue;
                }
            };
            if entries.is_empty() {
                break;
            }

            let mut retry_later = false;
            for entry in entries {
                let end = entry.end();
                match entry {
                    ReplayEntry::Write { command, .. } => {
                        let call_id = command.call_cdr.call_id.clone();
                        if let Err(err) = persist_with_retry(&state, *command).await {
                            error!(
                                call_id,
                                domain_id,
                                error = %err,
                                "CDR batch remains in spool after retry exhaustion"
                            );
                            retry_later = true;
                            break;
                        }
                        if let Err(err) = acknowledge(&state, &domain_id, end, &spool_lock).await {
                            error!(call_id, domain_id, error = %err, "advance CDR spool checkpoint failed");
                            retry_later = true;
                            break;
                        }
                        refresh_backlog(&state, &admission, &domain_id, &spool_lock).await;
                        info!(call_id, domain_id, "CDR batch persisted and checkpointed");
                    }
                    ReplayEntry::Quarantined { reason, .. } => {
                        error!(domain_id, reason, "quarantined invalid CDR spool frame");
                        if let Err(err) = acknowledge(&state, &domain_id, end, &spool_lock).await {
                            error!(domain_id, error = %err, "checkpoint quarantined CDR frame failed");
                            retry_later = true;
                            break;
                        }
                        refresh_backlog(&state, &admission, &domain_id, &spool_lock).await;
                    }
                }
            }
            if retry_later {
                tokio::time::sleep(Duration::from_millis(MAX_RETRY_DELAY_MS)).await;
                continue;
            }
        }
    }
    info!(domain_id, "domain CDR writer stopped");
}

async fn load_replay_entries(
    state: &AppState,
    domain_id: &str,
    spool_lock: &Arc<Mutex<()>>,
) -> Result<Vec<ReplayEntry>> {
    let backend = state.backend();
    let domain_id = domain_id.to_string();
    let spool_lock = spool_lock.clone();
    tokio::task::spawn_blocking(move || {
        let _guard = spool_lock.lock().expect("domain CDR spool lock");
        let root = backend.cdr_spool_dir(&domain_id)?;
        CdrSpool::open(root)?.replay()
    })
    .await
    .context("CDR spool replay task failed")?
}

async fn acknowledge(
    state: &AppState,
    domain_id: &str,
    position: super::cdr_spool::SpoolPosition,
    spool_lock: &Arc<Mutex<()>>,
) -> Result<()> {
    let backend = state.backend();
    let domain_id = domain_id.to_string();
    let spool_lock = spool_lock.clone();
    tokio::task::spawn_blocking(move || {
        let _guard = spool_lock.lock().expect("domain CDR spool lock");
        let root = backend.cdr_spool_dir(&domain_id)?;
        CdrSpool::open(root)?.acknowledge(position)
    })
    .await
    .context("CDR spool checkpoint task failed")?
}

async fn refresh_backlog(
    state: &AppState,
    admission: &SpoolAdmission,
    domain_id: &str,
    spool_lock: &Arc<Mutex<()>>,
) {
    let backend = state.backend();
    let owned_domain_id = domain_id.to_string();
    let spool_lock = spool_lock.clone();
    let result = tokio::task::spawn_blocking(move || {
        let _guard = spool_lock.lock().expect("domain CDR spool lock");
        let root = backend.cdr_spool_dir(&owned_domain_id)?;
        CdrSpool::open(root)?.backlog_bytes()
    })
    .await;
    match result {
        Ok(Ok(backlog_bytes)) => admission.update(domain_id, backlog_bytes),
        Ok(Err(err)) => warn!(domain_id, error = %err, "read CDR spool backlog failed"),
        Err(err) => warn!(domain_id, error = %err, "CDR spool backlog task failed"),
    }
}

async fn persist_with_retry(state: &AppState, command: CdrWriteCommand) -> Result<()> {
    let call_id = command.call_cdr.call_id.clone();
    let backend = state.backend();
    let mut attempt = 0_u32;
    loop {
        let command = command.clone();
        let backend = backend.clone();
        let result = tokio::task::spawn_blocking(move || backend.persist_cdr_batch(&command)).await;
        match result {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(err)) => {
                attempt += 1;
                if attempt >= MAX_RETRY_ATTEMPTS {
                    return Err(err).context("CDR database retry limit reached");
                }
                let delay_ms = retry_delay_ms(attempt);
                warn!(
                    call_id,
                    error = %err,
                    attempt,
                    retry_in_ms = delay_ms,
                    "CDR batch write failed, retrying"
                );
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Err(err) => return Err(err).context("CDR database task failed"),
        }
    }
}

fn retry_delay_ms(attempt: u32) -> u64 {
    BASE_RETRY_DELAY_MS
        .saturating_mul(1_u64 << attempt.min(6))
        .min(MAX_RETRY_DELAY_MS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spool_admission_uses_configured_hysteresis() {
        let admission = SpoolAdmission::new(CdrSpoolLimits {
            warning_mb: 1,
            resume_mb: 2,
            reject_mb: 3,
        });
        admission.update("domain-a", 3 * MIB);
        assert!(!admission.admits_new_call("domain-a"));

        admission.update("domain-a", 2 * MIB);
        assert!(!admission.admits_new_call("domain-a"));
        admission.update("domain-a", 2 * MIB - 1);
        assert!(admission.admits_new_call("domain-a"));
        assert!(admission.admits_new_call("domain-b"));
    }

    #[test]
    fn updated_limits_recalculate_existing_domain_status() {
        let admission = SpoolAdmission::new(CdrSpoolLimits {
            warning_mb: 1,
            resume_mb: 2,
            reject_mb: 3,
        });
        admission.update("domain-a", 3 * MIB);
        assert!(!admission.admits_new_call("domain-a"));

        admission.set_limits(CdrSpoolLimits {
            warning_mb: 3,
            resume_mb: 4,
            reject_mb: 5,
        });
        assert!(admission.admits_new_call("domain-a"));
    }
}
