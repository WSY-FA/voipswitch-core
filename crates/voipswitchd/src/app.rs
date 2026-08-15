use crate::ai::AiJobService;
use crate::config_service::{RuntimeConfig, RuntimeConfigStore};
use crate::data_store::{CallTraceMessage, ConfigBackend};
use crate::runtime::call::cdr_writer::CdrWriter;
use crate::runtime::call::dtmf_operation::DtmfOperationService;
use crate::runtime::call::handoff::CoordinatorHandoffService;
use crate::runtime::system_metrics::{SystemMetricsSampler, SystemMetricsSnapshot};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicUsize, Ordering},
    mpsc::{SyncSender, TrySendError, sync_channel},
};
use tracing::{error, warn};
use voipswitch_core::call::CallSnapshot;

const CALL_TRACE_QUEUE_CAPACITY: usize = 4096;

#[derive(Clone)]
pub struct AppState {
    inner: Arc<AppStateInner>,
}

struct AppStateInner {
    config: RuntimeConfigStore,
    backend: Arc<dyn ConfigBackend>,
    registrations: RwLock<RegistrationMirror>,
    trunks: RwLock<TrunkRuntimeMirror>,
    calls: RwLock<BTreeMap<String, ActiveCallView>>,
    call_trace_writer: CallTraceWriter,
    cdr_writer: std::sync::Mutex<Option<CdrWriter>>,
    ai_jobs: std::sync::Mutex<Option<AiJobService>>,
    dtmf_operations: DtmfOperationService,
    coordinator_handoffs: CoordinatorHandoffService,
    system_metrics: Mutex<SystemMetricsSampler>,
    adapter_runtime: RwLock<AdapterRuntimeSummary>,
    started_at_ms: u64,
    adapter_clients: AtomicUsize,
}

impl AppState {
    pub fn new(config: RuntimeConfig, backend: Arc<dyn ConfigBackend>, started_at_ms: u64) -> Self {
        let call_trace_writer = CallTraceWriter::new(backend.clone());
        Self {
            inner: Arc::new(AppStateInner {
                config: RuntimeConfigStore::new(config),
                backend,
                registrations: RwLock::new(RegistrationMirror::default()),
                trunks: RwLock::new(TrunkRuntimeMirror::default()),
                calls: RwLock::new(BTreeMap::new()),
                call_trace_writer,
                cdr_writer: std::sync::Mutex::new(None),
                ai_jobs: std::sync::Mutex::new(None),
                dtmf_operations: DtmfOperationService::default(),
                coordinator_handoffs: CoordinatorHandoffService::default(),
                system_metrics: Mutex::new(SystemMetricsSampler::default()),
                adapter_runtime: RwLock::new(AdapterRuntimeSummary::default()),
                started_at_ms,
                adapter_clients: AtomicUsize::new(0),
            }),
        }
    }

    pub fn config(&self) -> &RuntimeConfigStore {
        &self.inner.config
    }

    pub fn backend(&self) -> Arc<dyn ConfigBackend> {
        self.inner.backend.clone()
    }
    pub fn set_cdr_writer(&self, writer: CdrWriter) {
        *self.inner.cdr_writer.lock().expect("cdr_writer lock") = Some(writer);
    }
    pub fn cdr_writer(&self) -> Option<CdrWriter> {
        self.inner
            .cdr_writer
            .lock()
            .expect("cdr_writer lock")
            .clone()
    }

    pub(crate) fn set_ai_jobs(&self, service: AiJobService) {
        *self.inner.ai_jobs.lock().expect("ai_jobs lock") = Some(service);
    }

    pub(crate) fn ai_jobs(&self) -> Option<AiJobService> {
        self.inner.ai_jobs.lock().expect("ai_jobs lock").clone()
    }

    pub(crate) fn dtmf_operations(&self) -> DtmfOperationService {
        self.inner.dtmf_operations.clone()
    }

    pub(crate) fn coordinator_handoffs(&self) -> CoordinatorHandoffService {
        self.inner.coordinator_handoffs.clone()
    }

    pub fn started_at_ms(&self) -> u64 {
        self.inner.started_at_ms
    }

    pub fn adapter_clients(&self) -> usize {
        self.inner.adapter_clients.load(Ordering::Relaxed)
    }

    pub fn adapter_runtime(&self) -> AdapterRuntimeSummary {
        self.inner
            .adapter_runtime
            .read()
            .expect("adapter runtime summary lock poisoned")
            .clone()
    }

    pub fn set_adapter_ready(&self, ready: AdapterReady) {
        *self
            .inner
            .adapter_runtime
            .write()
            .expect("adapter runtime summary lock poisoned") = AdapterRuntimeSummary {
            ready: true,
            sip_port: ready.sip_port,
            bind_source: ready.bind_source,
        };
    }

    pub fn clear_adapter_runtime(&self) {
        *self
            .inner
            .adapter_runtime
            .write()
            .expect("adapter runtime summary lock poisoned") = AdapterRuntimeSummary::default();
    }

    pub fn system_metrics(&self) -> SystemMetricsSnapshot {
        let active_call_count = self
            .inner
            .calls
            .read()
            .expect("active call view lock poisoned")
            .len();
        self.inner
            .system_metrics
            .lock()
            .expect("system metrics sampler lock poisoned")
            .sample(active_call_count)
    }

    pub fn registrations(&self) -> RegistrationMirror {
        self.inner
            .registrations
            .read()
            .expect("registration mirror lock poisoned")
            .clone()
    }

    pub fn mark_registration_mirror_ready(&self, ready: bool) {
        let mut mirror = self
            .inner
            .registrations
            .write()
            .expect("registration mirror lock poisoned");
        mirror.ready = ready;
        mirror.snapshot_in_progress = false;
    }

    pub fn begin_registration_snapshot(&self) {
        let mut mirror = self
            .inner
            .registrations
            .write()
            .expect("registration mirror lock poisoned");
        mirror.ready = false;
        mirror.snapshot_in_progress = true;
        mirror.items.clear();
    }

    pub fn trunks(&self) -> TrunkRuntimeMirror {
        self.inner
            .trunks
            .read()
            .expect("trunk runtime mirror lock poisoned")
            .clone()
    }

    pub fn mark_trunk_runtime_mirror_ready(&self, ready: bool) {
        self.inner
            .trunks
            .write()
            .expect("trunk runtime mirror lock poisoned")
            .ready = ready;
    }

    pub fn apply_registration_changed(&self, event: RegistrationChanged) {
        let mut mirror = self
            .inner
            .registrations
            .write()
            .expect("registration mirror lock poisoned");
        if !mirror.snapshot_in_progress {
            mirror.ready = true;
        }
        let key = (event.domain_id.clone(), event.endpoint_id.clone());
        mirror.items.insert(
            key,
            RegistrationView {
                domain_id: event.domain_id,
                endpoint_id: event.endpoint_id,
                state: event.state,
                contact: event.contact,
                route_target: event.route_target,
                expires_at_ms: event.expires_at_ms,
                user_agent: event.user_agent,
                version: Some(event.version),
            },
        );
    }

    pub fn apply_trunk_registration_changed(&self, event: TrunkRegistrationChanged) {
        let mut mirror = self
            .inner
            .trunks
            .write()
            .expect("trunk runtime mirror lock poisoned");
        mirror.ready = true;
        let key = (
            event.domain_id.clone(),
            event.reg_trunk_id,
            event.reg_account_id,
        );
        mirror.registrations.insert(
            key,
            TrunkRegistrationView {
                domain_id: event.domain_id,
                reg_trunk_id: event.reg_trunk_id,
                reg_account_id: event.reg_account_id,
                state: event.state,
                expires_at_ms: event.expires_at_ms,
                response_code: event.response_code,
                reason: event.reason,
                version: event.version,
            },
        );
    }

    pub fn apply_trunk_health_changed(&self, event: TrunkHealthChanged) {
        let mut mirror = self
            .inner
            .trunks
            .write()
            .expect("trunk runtime mirror lock poisoned");
        mirror.ready = true;
        let key = (
            event.domain_id.clone(),
            event.trunk_type.clone(),
            event.trunk_id,
        );
        mirror.health.insert(
            key,
            TrunkHealthView {
                domain_id: event.domain_id,
                trunk_type: event.trunk_type,
                trunk_id: event.trunk_id,
                state: event.state,
                response_code: event.response_code,
                reason: event.reason,
                checked_at_ms: event.checked_at_ms,
                version: event.version,
            },
        );
    }

    pub fn increment_adapter_clients(&self) {
        self.inner.adapter_clients.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_adapter_clients(&self) {
        self.inner.adapter_clients.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn upsert_active_call(&self, call: ActiveCallView) {
        self.inner
            .calls
            .write()
            .expect("active call view lock poisoned")
            .insert(call.call_id.clone(), call);
    }

    pub fn remove_active_call(&self, call_id: &str) -> Option<ActiveCallView> {
        self.inner
            .calls
            .write()
            .expect("active call view lock poisoned")
            .remove(call_id)
    }

    pub fn clear_active_calls(&self) {
        self.inner
            .calls
            .write()
            .expect("active call view lock poisoned")
            .clear();
    }

    pub fn active_calls(&self, domain_id: Option<&str>) -> Vec<ActiveCallView> {
        self.inner
            .calls
            .read()
            .expect("active call view lock poisoned")
            .values()
            .filter(|call| domain_id.is_none_or(|domain| call.domain_id == domain))
            .cloned()
            .collect()
    }

    pub fn active_sessions(&self, domain_id: Option<&str>) -> Vec<ActiveSessionView> {
        self.active_calls(domain_id)
            .into_iter()
            .flat_map(|call| {
                [
                    ActiveSessionView {
                        session_id: call.caller_session_id.clone(),
                        call_id: call.call_id.clone(),
                        domain_id: call.domain_id.clone(),
                        direction: "inbound".to_string(),
                        number: call.caller_number.clone(),
                        peer_number: call.callee_number.clone(),
                        state: if call.caller_terminated {
                            "terminated".to_string()
                        } else {
                            call.state.clone()
                        },
                        started_at_ms: call.started_at_ms,
                        answered_at_ms: call.answered_at_ms,
                    },
                    ActiveSessionView {
                        session_id: call.callee_session_id,
                        call_id: call.call_id,
                        domain_id: call.domain_id,
                        direction: "outbound".to_string(),
                        number: call.callee_number,
                        peer_number: call.caller_number,
                        state: if call.callee_terminated {
                            "terminated".to_string()
                        } else {
                            call.state
                        },
                        started_at_ms: call.started_at_ms,
                        answered_at_ms: call.answered_at_ms,
                    },
                ]
            })
            .collect()
    }

    pub fn record_call_trace(&self, message: CallTraceMessage) {
        if self.config().snapshot().call_trace_enabled() {
            self.inner.call_trace_writer.record(message);
        } else {
            self.inner
                .call_trace_writer
                .mark_incomplete(&message.call_id, &message.domain_id);
        }
    }

    #[allow(dead_code)]
    pub fn complete_call_trace(&self, call_id: &str, domain_id: &str, ended_at_ms: u64) {
        self.inner
            .call_trace_writer
            .complete(call_id, domain_id, ended_at_ms);
    }

    pub fn call_trace_available(&self, call_id: &str) -> bool {
        self.inner.call_trace_writer.has_tracked_call(call_id)
    }

    pub fn mark_active_call_traces_incomplete(&self) {
        for call in self
            .inner
            .calls
            .read()
            .expect("active call view lock poisoned")
            .values()
        {
            self.inner
                .call_trace_writer
                .mark_incomplete(&call.call_id, &call.domain_id);
        }
    }
}

#[derive(Clone)]
struct CallTraceWriter {
    tx: SyncSender<CallTraceWriteCommand>,
    incomplete_calls: Arc<Mutex<BTreeSet<String>>>,
    tracked_calls: Arc<Mutex<BTreeSet<String>>>,
}

enum CallTraceWriteCommand {
    Record(CallTraceMessage),
    MarkIncomplete {
        call_id: String,
        domain_id: String,
    },
    Complete {
        call_id: String,
        domain_id: String,
        ended_at_ms: u64,
        incomplete: bool,
    },
}

impl CallTraceWriter {
    fn new(backend: Arc<dyn ConfigBackend>) -> Self {
        let (tx, rx) = sync_channel(CALL_TRACE_QUEUE_CAPACITY);
        let incomplete_calls = Arc::new(Mutex::new(BTreeSet::new()));
        let tracked_calls = Arc::new(Mutex::new(BTreeSet::new()));
        std::thread::Builder::new()
            .name("call-trace-writer".to_string())
            .spawn(move || {
                while let Ok(command) = rx.recv() {
                    let result = match command {
                        CallTraceWriteCommand::Record(message) => {
                            backend.insert_call_trace_message(&message)
                        }
                        CallTraceWriteCommand::MarkIncomplete { call_id, domain_id } => {
                            backend.mark_call_trace_incomplete(&call_id, &domain_id)
                        }
                        CallTraceWriteCommand::Complete {
                            call_id,
                            domain_id,
                            ended_at_ms,
                            incomplete,
                        } => backend.complete_call_trace(
                            &call_id,
                            &domain_id,
                            ended_at_ms,
                            incomplete,
                        ),
                    };
                    if let Err(err) = result {
                        error!(error = %err, "call trace persistence failed");
                    }
                }
            })
            .expect("spawn call trace writer");
        Self {
            tx,
            incomplete_calls,
            tracked_calls,
        }
    }

    fn record(&self, message: CallTraceMessage) {
        let call_id = message.call_id.clone();
        let domain_id = message.domain_id.clone();
        self.tracked_calls
            .lock()
            .expect("call trace tracked set lock poisoned")
            .insert(call_id.clone());
        match self.tx.try_send(CallTraceWriteCommand::Record(message)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.mark_incomplete(&call_id, &domain_id);
                warn!(call_id, "call trace writer queue full; message dropped");
            }
            Err(TrySendError::Disconnected(_)) => {
                self.mark_incomplete(&call_id, &domain_id);
                error!(call_id, "call trace writer stopped; message dropped");
            }
        }
    }

    fn complete(&self, call_id: &str, domain_id: &str, ended_at_ms: u64) {
        let tracked = self
            .tracked_calls
            .lock()
            .expect("call trace tracked set lock poisoned")
            .remove(call_id);
        if !tracked {
            self.incomplete_calls
                .lock()
                .expect("call trace incomplete set lock poisoned")
                .remove(call_id);
            return;
        }
        let incomplete = self
            .incomplete_calls
            .lock()
            .expect("call trace incomplete set lock poisoned")
            .remove(call_id);
        let command = CallTraceWriteCommand::Complete {
            call_id: call_id.to_string(),
            domain_id: domain_id.to_string(),
            ended_at_ms,
            incomplete,
        };
        match self.tx.try_send(command) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                warn!(call_id, "call trace completion queue full");
            }
            Err(TrySendError::Disconnected(_)) => {
                error!(call_id, "call trace writer stopped before completion");
            }
        }
    }

    fn has_tracked_call(&self, call_id: &str) -> bool {
        self.tracked_calls
            .lock()
            .expect("call trace tracked set lock poisoned")
            .contains(call_id)
    }

    fn mark_incomplete(&self, call_id: &str, domain_id: &str) {
        self.incomplete_calls
            .lock()
            .expect("call trace incomplete set lock poisoned")
            .insert(call_id.to_string());
        match self.tx.try_send(CallTraceWriteCommand::MarkIncomplete {
            call_id: call_id.to_string(),
            domain_id: domain_id.to_string(),
        }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                warn!(call_id, "call trace incomplete marker queue full");
            }
            Err(TrySendError::Disconnected(_)) => {
                error!(
                    call_id,
                    "call trace writer stopped before incomplete marker"
                );
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveCallView {
    pub call_id: String,
    pub domain_id: String,
    pub caller_session_id: String,
    pub callee_session_id: String,
    pub caller_number: String,
    pub callee_number: String,
    pub state: String,
    pub started_at_ms: u64,
    pub answered_at_ms: Option<u64>,
    pub last_status: Option<u16>,
    pub caller_terminated: bool,
    pub callee_terminated: bool,
    pub runtime_config_version: u64,
    pub domain_config_version: u64,
    pub topology: CallSnapshot,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActiveSessionView {
    pub session_id: String,
    pub call_id: String,
    pub domain_id: String,
    pub direction: String,
    pub number: String,
    pub peer_number: String,
    pub state: String,
    pub started_at_ms: u64,
    pub answered_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct AdapterRuntimeSummary {
    pub ready: bool,
    pub sip_port: Option<u16>,
    pub bind_source: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AdapterReady {
    #[serde(default)]
    pub sip_port: Option<u16>,
    #[serde(default)]
    pub bind_source: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RegistrationMirror {
    pub ready: bool,
    pub snapshot_in_progress: bool,
    pub items: BTreeMap<(String, String), RegistrationView>,
}

#[derive(Debug, Clone)]
pub struct RegistrationView {
    pub domain_id: String,
    pub endpoint_id: String,
    pub state: RegistrationState,
    pub contact: Option<String>,
    pub route_target: Option<SocketAddr>,
    pub expires_at_ms: Option<u64>,
    pub user_agent: Option<String>,
    pub version: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegistrationState {
    Unknown,
    Unregistered,
    TentativeRegistered,
    Registered,
    Expired,
    Removed,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RegistrationChanged {
    pub domain_id: String,
    pub endpoint_id: String,
    pub state: RegistrationState,
    pub contact: Option<String>,
    #[serde(default)]
    pub route_target: Option<SocketAddr>,
    pub expires_at_ms: Option<u64>,
    pub user_agent: Option<String>,
    pub version: u64,
}

#[derive(Debug, Clone, Default)]
pub struct TrunkRuntimeMirror {
    pub ready: bool,
    pub registrations: BTreeMap<(String, u64, u64), TrunkRegistrationView>,
    pub health: BTreeMap<(String, String, u64), TrunkHealthView>,
}

#[derive(Debug, Clone)]
pub struct TrunkRegistrationView {
    pub domain_id: String,
    pub reg_trunk_id: u64,
    pub reg_account_id: u64,
    pub state: TrunkRegistrationState,
    pub expires_at_ms: Option<u64>,
    pub response_code: Option<u16>,
    pub reason: Option<String>,
    pub version: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TrunkRegistrationChanged {
    pub domain_id: String,
    pub reg_trunk_id: u64,
    pub reg_account_id: u64,
    pub state: TrunkRegistrationState,
    pub expires_at_ms: Option<u64>,
    pub response_code: Option<u16>,
    pub reason: Option<String>,
    pub version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrunkRegistrationState {
    Registering,
    Registered,
    Failed,
    Stopped,
}

#[derive(Debug, Clone)]
pub struct TrunkHealthView {
    pub domain_id: String,
    pub trunk_type: String,
    pub trunk_id: u64,
    pub state: TrunkHealthState,
    pub response_code: Option<u16>,
    pub reason: Option<String>,
    pub checked_at_ms: u64,
    pub version: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TrunkHealthChanged {
    pub domain_id: String,
    pub trunk_type: String,
    pub trunk_id: u64,
    pub state: TrunkHealthState,
    pub response_code: Option<u16>,
    pub reason: Option<String>,
    pub checked_at_ms: u64,
    pub version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrunkHealthState {
    Up,
    Down,
}
