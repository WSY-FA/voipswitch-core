use crate::app::{AppState, RegistrationState, TrunkHealthState};
use serde::Serialize;
use std::time::{Duration, Instant};

pub const HEALTH_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const MAX_SCANNED_ITEMS: usize = 10_000;
#[allow(dead_code)]
pub const MAX_EVIDENCE_ITEMS: usize = 256;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TaskState {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl TaskState {
    #[allow(dead_code)]
    pub fn can_transition(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Queued, Self::Running | Self::Cancelled)
                | (
                    Self::Running,
                    Self::Completed | Self::Failed | Self::Cancelled
                )
        )
    }
}

#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)]
pub struct EvidenceItem {
    pub id: String,
    pub kind: String,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)]
pub struct EvidenceBundle {
    pub items: Vec<EvidenceItem>,
    pub truncated: bool,
}

impl EvidenceBundle {
    #[allow(dead_code)]
    pub fn bounded(mut items: Vec<EvidenceItem>) -> Self {
        let truncated = items.len() > MAX_EVIDENCE_ITEMS;
        items.truncate(MAX_EVIDENCE_ITEMS);
        Self { items, truncated }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize)]
pub struct OpsTask {
    pub task_id: String,
    pub request_id: String,
    pub kind: String,
    pub state: TaskState,
    pub deadline_ms: u64,
}

impl OpsTask {
    #[allow(dead_code)]
    pub fn transition(&mut self, next: TaskState) -> bool {
        if self.state.can_transition(next) {
            self.state = next;
            true
        } else {
            false
        }
    }

    // AI-04 scheduler is not yet connected; keep its contract under test.
    #[cfg(test)]
    pub fn cancel(&mut self) -> bool {
        self.transition(TaskState::Cancelled)
    }
}

#[allow(dead_code)]
pub fn deadline_expired(start: Instant, timeout: Duration) -> bool {
    start.elapsed() >= timeout
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthSnapshot {
    pub schema_version: u32,
    pub generated_at_ms: u64,
    pub config_revision: u64,
    pub runtime_generation: Option<u64>,
    pub domain_id: Option<String>,
    pub truncated: bool,
    pub component_health: ComponentHealth,
    pub adapter_connection: AdapterConnection,
    pub active_calls: usize,
    pub call_concurrency: usize,
    pub registrations: RegistrationHealth,
    pub trunk_health: TrunkHealth,
    pub system_metrics: crate::runtime::system_metrics::SystemMetricsSnapshot,
    pub cdr_spool_backlog: CdrSpoolBacklog,
}

#[derive(Debug, Clone, Serialize)]
pub struct ComponentHealth {
    pub voipswitchd: &'static str,
    pub sip_adapter: &'static str,
    pub cdr_backend: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdapterConnection {
    pub ready: bool,
    pub clients: usize,
    pub sip_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegistrationHealth {
    pub mirror_ready: bool,
    pub snapshot_in_progress: bool,
    pub total: usize,
    pub registered: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct TrunkHealth {
    pub mirror_ready: bool,
    pub registrations: usize,
    pub health_entries: usize,
    pub down: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct CdrSpoolBacklog {
    pub status: &'static str,
    pub bytes: Option<u64>,
}

pub fn health_snapshot(state: &AppState, domain_id: Option<&str>) -> HealthSnapshot {
    let config = state.config().snapshot();
    let registrations = state.registrations();
    let trunks = state.trunks();
    let calls = state.active_calls(domain_id);
    let registration_items = registrations
        .items
        .values()
        .filter(|item| domain_id.is_none_or(|domain| item.domain_id == domain))
        .take(MAX_SCANNED_ITEMS)
        .collect::<Vec<_>>();
    let trunk_registrations = trunks
        .registrations
        .values()
        .filter(|item| domain_id.is_none_or(|domain| item.domain_id == domain))
        .take(MAX_SCANNED_ITEMS)
        .collect::<Vec<_>>();
    let trunk_health = trunks
        .health
        .values()
        .filter(|item| domain_id.is_none_or(|domain| item.domain_id == domain))
        .take(MAX_SCANNED_ITEMS)
        .collect::<Vec<_>>();
    let truncated = registration_items.len() == MAX_SCANNED_ITEMS
        || trunk_registrations.len() == MAX_SCANNED_ITEMS
        || trunk_health.len() == MAX_SCANNED_ITEMS
        || calls.len() > MAX_SCANNED_ITEMS;
    let metrics = state.system_metrics();
    let adapter = state.adapter_runtime();
    let cdr_backend = if state.backend().health_check().is_ok() {
        "up"
    } else {
        "degraded"
    };

    HealthSnapshot {
        schema_version: HEALTH_SNAPSHOT_SCHEMA_VERSION,
        generated_at_ms: metrics.sampled_at_ms,
        config_revision: config.version,
        runtime_generation: None,
        domain_id: domain_id.map(str::to_owned),
        truncated,
        component_health: ComponentHealth {
            voipswitchd: "up",
            sip_adapter: if adapter.ready { "up" } else { "degraded" },
            cdr_backend,
        },
        adapter_connection: AdapterConnection {
            ready: adapter.ready,
            clients: state.adapter_clients(),
            sip_port: adapter.sip_port,
        },
        active_calls: calls.len(),
        call_concurrency: calls.len(),
        registrations: RegistrationHealth {
            mirror_ready: registrations.ready,
            snapshot_in_progress: registrations.snapshot_in_progress,
            total: registration_items.len(),
            registered: registration_items
                .iter()
                .filter(|item| matches!(item.state, RegistrationState::Registered))
                .count(),
        },
        trunk_health: TrunkHealth {
            mirror_ready: trunks.ready,
            registrations: trunk_registrations.len(),
            health_entries: trunk_health.len(),
            down: trunk_health
                .iter()
                .filter(|item| matches!(item.state, TrunkHealthState::Down))
                .count(),
        },
        system_metrics: metrics,
        cdr_spool_backlog: CdrSpoolBacklog {
            status: "unknown",
            bytes: None,
        },
    }
}

pub fn health_snapshot_value(state: &AppState, domain_id: Option<&str>) -> serde_json::Value {
    serde_json::to_value(health_snapshot(state, domain_id)).expect("health snapshot serializable")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_store::{ConfigBackend, SeaOrmConfigBackend};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn state() -> AppState {
        let root = tempdir().unwrap().keep();
        let backend = Arc::new(SeaOrmConfigBackend::sqlite(&root, "test").unwrap());
        let config = backend.load_runtime_config().unwrap();
        AppState::new(config, backend, 1)
    }

    #[test]
    fn snapshot_is_bounded_and_marks_unknown_spool_backlog() {
        let snapshot = health_snapshot(&state(), Some("domain-1"));
        assert_eq!(snapshot.schema_version, HEALTH_SNAPSHOT_SCHEMA_VERSION);
        assert_eq!(snapshot.domain_id.as_deref(), Some("domain-1"));
        assert!(!snapshot.truncated);
        assert_eq!(snapshot.cdr_spool_backlog.status, "unknown");
        assert_eq!(snapshot.cdr_spool_backlog.bytes, None);
    }

    #[test]
    fn evidence_bundle_is_bounded() {
        let items = (0..(MAX_EVIDENCE_ITEMS + 1))
            .map(|i| EvidenceItem {
                id: i.to_string(),
                kind: "cdr".into(),
                summary: "redacted".into(),
            })
            .collect();
        let bundle = EvidenceBundle::bounded(items);
        assert_eq!(bundle.items.len(), MAX_EVIDENCE_ITEMS);
        assert!(bundle.truncated);
    }

    #[test]
    fn task_state_transitions_are_monotonic() {
        let mut task = OpsTask {
            task_id: "t1".into(),
            request_id: "r1".into(),
            kind: "health_review".into(),
            state: TaskState::Queued,
            deadline_ms: 100,
        };
        assert!(task.transition(TaskState::Running));
        assert!(!task.transition(TaskState::Queued));
        assert!(task.cancel());
        assert!(!task.transition(TaskState::Completed));
    }
}
