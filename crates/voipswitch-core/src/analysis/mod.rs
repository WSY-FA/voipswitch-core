use crate::types::call::HangupCause;
use crate::types::ids::{DomainId, EndpointId, TrunkId};
use crate::types::number::ExtensionNumber;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnalysisPhase {
    CallerAnalysis,
    CallerBusinessAnalysis,
    CalleePreAnalysis,
    InboundRoute,
    CalleeAnalysis,
    CalleeBusinessAnalysis,
    OutboundRoute,
    ResultHook,
}

impl AnalysisPhase {
    pub const ORDERED: [Self; 8] = [
        Self::CallerAnalysis,
        Self::CallerBusinessAnalysis,
        Self::CalleePreAnalysis,
        Self::InboundRoute,
        Self::CalleeAnalysis,
        Self::CalleeBusinessAnalysis,
        Self::OutboundRoute,
        Self::ResultHook,
    ];
}

#[derive(Debug, Clone)]
pub struct NumberAnalysisRequest {
    pub domain_id: DomainId,
    pub caller: String,
    pub callee: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RouteDecision {
    Continue,
    Matched(Box<RouteResult>),
    Reject(RouteReject),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteRejectReason {
    InvalidNumberFormat,
    NoSuchExtension,
    NoBusinessModuleMatch,
    UnsupportedBusinessTarget,
    BusinessTargetNotFound,
    BusinessTargetDisabled,
    BusinessTargetUnavailable,
    BusinessTargetBusy,
    NoAvailableTrunk,
    DomainDisabled,
    Forbidden,
    InternalError,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteReject {
    pub cause: HangupCause,
    pub sip_status: u16,
    pub reason: RouteRejectReason,
}

impl RouteReject {
    pub fn not_found(reason: RouteRejectReason) -> Self {
        Self {
            cause: HangupCause::NoRoute,
            sip_status: 404,
            reason,
        }
    }

    pub fn disabled(reason: RouteRejectReason) -> Self {
        Self {
            cause: HangupCause::CallRejected,
            sip_status: 480,
            reason,
        }
    }
}

pub type RouteVariables = BTreeMap<String, Value>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalleeHook {
    pub module: String,
    pub name: String,
    #[serde(default)]
    pub arguments: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct CalleeSessionPlan {
    pub business_module: Option<String>,
    #[serde(default)]
    pub initial_state: Value,
    #[serde(default)]
    pub hooks: Vec<CalleeHook>,
    #[serde(default)]
    pub variables: RouteVariables,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointRef {
    pub endpoint_id: EndpointId,
    pub number: ExtensionNumber,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtensionRoute {
    pub domain_id: DomainId,
    pub extension: ExtensionNumber,
    pub endpoint: EndpointRef,
    pub transformed_caller: String,
    pub transformed_callee: String,
    pub callee_session_plan: Option<CalleeSessionPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CalleeSetPolicy {
    Single,
    ParallelFirstAnswer,
    SequentialHunt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CalleeTarget {
    Extension {
        endpoint_id: EndpointId,
        number: ExtensionNumber,
    },
    Trunk {
        trunk_id: TrunkId,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalleeAttemptPlan {
    pub target: CalleeTarget,
    pub callee_session_plan: Option<CalleeSessionPlan>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalleeSetRoute {
    pub domain_id: DomainId,
    pub members: Vec<CalleeAttemptPlan>,
    pub policy: CalleeSetPolicy,
    pub original_caller: String,
    pub original_callee: String,
    pub transformed_caller: String,
    pub transformed_callee: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BusinessModuleRoute {
    pub domain_id: DomainId,
    pub module: String,
    pub target_id: String,
    pub original_caller: String,
    pub original_callee: String,
    pub transformed_caller: String,
    pub transformed_callee: String,
    pub callee_session_plan: Option<CalleeSessionPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrunkCandidate {
    pub trunk_id: TrunkId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrunkRoute {
    pub domain_id: DomainId,
    pub route_id: String,
    pub route_name: String,
    pub trunks: Vec<TrunkCandidate>,
    pub original_caller: String,
    pub original_callee: String,
    pub transformed_caller: String,
    pub transformed_callee: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteResult {
    Extension(ExtensionRoute),
    CalleeSet(CalleeSetRoute),
    BusinessModule(BusinessModuleRoute),
    Trunk(TrunkRoute),
}

pub trait NumberAnalyzer: Send + Sync {
    fn name(&self) -> &str;
    fn phase(&self) -> AnalysisPhase;
    fn priority(&self) -> i32;
    fn analyze(&self, request: &NumberAnalysisRequest) -> RouteDecision;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredAnalyzer {
    pub name: String,
    pub phase: AnalysisPhase,
    pub priority: i32,
    pub registered_order: u64,
}

struct AnalyzerEntry {
    analyzer: Arc<dyn NumberAnalyzer>,
    registered_order: u64,
}

#[derive(Default)]
struct AnalysisRegistryInner {
    analyzers: RwLock<Vec<AnalyzerEntry>>,
    next_registered_order: AtomicU64,
}

#[derive(Clone, Default)]
pub struct AnalysisRegistry {
    inner: Arc<AnalysisRegistryInner>,
}

impl AnalysisRegistry {
    pub fn register(&self, analyzer: Arc<dyn NumberAnalyzer>) -> u64 {
        let registered_order = self
            .inner
            .next_registered_order
            .fetch_add(1, Ordering::Relaxed);
        let mut analyzers = self
            .inner
            .analyzers
            .write()
            .expect("analysis registry lock poisoned");
        analyzers.push(AnalyzerEntry {
            analyzer,
            registered_order,
        });
        analyzers.sort_by(|left, right| {
            left.analyzer
                .phase()
                .cmp(&right.analyzer.phase())
                .then_with(|| left.analyzer.priority().cmp(&right.analyzer.priority()))
                .then_with(|| left.registered_order.cmp(&right.registered_order))
        });
        registered_order
    }

    pub fn analyze_phase(
        &self,
        phase: AnalysisPhase,
        request: &NumberAnalysisRequest,
    ) -> RouteDecision {
        for entry in self
            .inner
            .analyzers
            .read()
            .expect("analysis registry lock poisoned")
            .iter()
            .filter(|entry| entry.analyzer.phase() == phase)
        {
            match entry.analyzer.analyze(request) {
                RouteDecision::Continue => {}
                decision => return decision,
            }
        }
        RouteDecision::Continue
    }

    pub fn analyze(&self, request: &NumberAnalysisRequest) -> RouteDecision {
        for phase in AnalysisPhase::ORDERED {
            match self.analyze_phase(phase, request) {
                RouteDecision::Continue => {}
                decision => return decision,
            }
        }
        RouteDecision::Reject(RouteReject::not_found(
            RouteRejectReason::NoBusinessModuleMatch,
        ))
    }

    pub fn registered_analyzers(&self) -> Vec<RegisteredAnalyzer> {
        self.inner
            .analyzers
            .read()
            .expect("analysis registry lock poisoned")
            .iter()
            .map(|entry| RegisteredAnalyzer {
                name: entry.analyzer.name().to_string(),
                phase: entry.analyzer.phase(),
                priority: entry.analyzer.priority(),
                registered_order: entry.registered_order,
            })
            .collect()
    }

    pub fn names(&self) -> Vec<String> {
        self.registered_analyzers()
            .into_iter()
            .map(|entry| entry.name)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct ProbeAnalyzer {
        name: &'static str,
        phase: AnalysisPhase,
        priority: i32,
        calls: Arc<Mutex<Vec<&'static str>>>,
    }

    impl NumberAnalyzer for ProbeAnalyzer {
        fn name(&self) -> &str {
            self.name
        }

        fn phase(&self) -> AnalysisPhase {
            self.phase
        }

        fn priority(&self) -> i32 {
            self.priority
        }

        fn analyze(&self, _request: &NumberAnalysisRequest) -> RouteDecision {
            self.calls.lock().unwrap().push(self.name);
            RouteDecision::Continue
        }
    }

    fn request() -> NumberAnalysisRequest {
        NumberAnalysisRequest {
            domain_id: DomainId::from("default"),
            caller: "1000".to_string(),
            callee: "1001".to_string(),
        }
    }

    #[test]
    fn same_priority_preserves_registration_order() {
        let registry = AnalysisRegistry::default();
        let calls = Arc::new(Mutex::new(Vec::new()));
        registry.register(Arc::new(ProbeAnalyzer {
            name: "z-last-by-name",
            phase: AnalysisPhase::CalleeBusinessAnalysis,
            priority: 100,
            calls: calls.clone(),
        }));
        registry.register(Arc::new(ProbeAnalyzer {
            name: "a-first-by-name",
            phase: AnalysisPhase::CalleeBusinessAnalysis,
            priority: 100,
            calls: calls.clone(),
        }));

        registry.analyze_phase(AnalysisPhase::CalleeBusinessAnalysis, &request());

        assert_eq!(
            calls.lock().unwrap().as_slice(),
            ["z-last-by-name", "a-first-by-name"]
        );
    }

    #[test]
    fn fixed_phase_order_wins_over_registration_order() {
        let registry = AnalysisRegistry::default();
        let calls = Arc::new(Mutex::new(Vec::new()));
        registry.register(Arc::new(ProbeAnalyzer {
            name: "callee",
            phase: AnalysisPhase::CalleeAnalysis,
            priority: 100,
            calls: calls.clone(),
        }));
        registry.register(Arc::new(ProbeAnalyzer {
            name: "caller",
            phase: AnalysisPhase::CallerAnalysis,
            priority: 100,
            calls: calls.clone(),
        }));

        let _ = registry.analyze(&request());

        assert_eq!(calls.lock().unwrap().as_slice(), ["caller", "callee"]);
    }
}
