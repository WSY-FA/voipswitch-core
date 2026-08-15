use crate::app::AppState;
use std::sync::Arc;
use voipswitch_core::analysis::{
    AnalysisPhase, AnalysisRegistry, EndpointRef, ExtensionRoute, NumberAnalysisRequest,
    NumberAnalyzer, RouteDecision, RouteReject, RouteRejectReason, RouteResult,
};
use voipswitch_core::types::ids::EndpointId;
use voipswitch_core::types::number::ExtensionNumber;

pub fn register(registry: &AnalysisRegistry, state: AppState) {
    registry.register(Arc::new(ExtensionCallerAnalyzer {
        state: state.clone(),
    }));
    registry.register(Arc::new(ExtensionCalleeAnalyzer { state }));
}

struct ExtensionCallerAnalyzer {
    state: AppState,
}

impl NumberAnalyzer for ExtensionCallerAnalyzer {
    fn name(&self) -> &str {
        "extension-caller"
    }

    fn phase(&self) -> AnalysisPhase {
        AnalysisPhase::CallerAnalysis
    }

    fn priority(&self) -> i32 {
        100
    }

    fn analyze(&self, request: &NumberAnalysisRequest) -> RouteDecision {
        let config = self.state.config().snapshot();
        let Some(domain) = config.domains.get(&request.domain_id) else {
            return RouteDecision::Reject(RouteReject::not_found(
                RouteRejectReason::NoSuchExtension,
            ));
        };
        let Some(extension) = domain
            .extensions
            .iter()
            .find(|extension| extension.number == request.caller)
        else {
            return RouteDecision::Continue;
        };
        if extension.enabled {
            RouteDecision::Continue
        } else {
            RouteDecision::Reject(RouteReject::disabled(
                RouteRejectReason::BusinessTargetDisabled,
            ))
        }
    }
}

struct ExtensionCalleeAnalyzer {
    state: AppState,
}

impl NumberAnalyzer for ExtensionCalleeAnalyzer {
    fn name(&self) -> &str {
        "extension-callee"
    }

    fn phase(&self) -> AnalysisPhase {
        AnalysisPhase::CalleeAnalysis
    }

    fn priority(&self) -> i32 {
        100
    }

    fn analyze(&self, request: &NumberAnalysisRequest) -> RouteDecision {
        let config = self.state.config().snapshot();
        let Some(domain) = config.domains.get(&request.domain_id) else {
            return RouteDecision::Reject(RouteReject::not_found(
                RouteRejectReason::NoSuchExtension,
            ));
        };
        let Some(extension) = domain
            .extensions
            .iter()
            .find(|extension| extension.number == request.callee)
        else {
            return RouteDecision::Continue;
        };
        if !extension.enabled {
            return RouteDecision::Reject(RouteReject::disabled(
                RouteRejectReason::BusinessTargetDisabled,
            ));
        }
        let Ok(extension_number) = ExtensionNumber::parse(&extension.number) else {
            return RouteDecision::Reject(RouteReject {
                cause: voipswitch_core::types::call::HangupCause::InvalidNumberFormat,
                sip_status: 484,
                reason: RouteRejectReason::InvalidNumberFormat,
            });
        };
        let endpoint = EndpointRef {
            endpoint_id: EndpointId::from(extension.id.to_string()),
            number: extension_number.clone(),
        };
        RouteDecision::Matched(Box::new(RouteResult::Extension(ExtensionRoute {
            domain_id: request.domain_id.clone(),
            extension: extension_number,
            endpoint,
            transformed_caller: request.caller.clone(),
            transformed_callee: request.callee.clone(),
            callee_session_plan: None,
        })))
    }
}
