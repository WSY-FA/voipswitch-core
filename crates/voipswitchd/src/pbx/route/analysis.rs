use crate::app::{AppState, TrunkHealthState, TrunkRegistrationState};
use crate::pbx::route::model::{InboundRouteConfig, OutboundRouteConfig};
use regex::Regex;
use std::sync::Arc;
use voipswitch_core::analysis::{
    AnalysisPhase, AnalysisRegistry, NumberAnalysisRequest, NumberAnalyzer, RouteDecision,
    RouteReject, RouteRejectReason, RouteResult, TrunkCandidate, TrunkRoute,
};
use voipswitch_core::types::call::HangupCause;
use voipswitch_core::types::ids::TrunkId;

pub fn register(registry: &AnalysisRegistry, state: AppState) {
    registry.register(Arc::new(OutboundRouteAnalyzer { state }));
}

struct OutboundRouteAnalyzer {
    state: AppState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InboundRouteTarget {
    Reject,
    Auto,
    Extension(String),
    AiAgent(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InboundRouteMatch {
    pub route_id: String,
    pub route_name: String,
    pub transformed_caller: String,
    pub transformed_callee: String,
    pub target: InboundRouteTarget,
}

pub(crate) fn analyze_inbound_route(
    domain: &crate::config_service::DomainRuntimeConfig,
    trunk_ref: &str,
    caller: &str,
    callee: &str,
) -> Option<InboundRouteMatch> {
    for trunk_match in [trunk_ref, ""] {
        for route in domain
            .inbound_routes
            .iter()
            .filter(|route| route.enabled && route.trunk_match == trunk_match)
        {
            if !inbound_route_matches(route, caller, callee) {
                continue;
            }
            let Some(transformed_caller) = transform_number(
                caller,
                route.src_strip,
                &route.src_prefix,
                &route.src_suffix,
            ) else {
                continue;
            };
            let Some(transformed_callee) = transform_number(
                callee,
                route.dst_strip,
                &route.dst_prefix,
                &route.dst_suffix,
            ) else {
                continue;
            };
            let target = match route.target.as_str() {
                "rej" => InboundRouteTarget::Reject,
                "auto" => InboundRouteTarget::Auto,
                value if value.starts_with("ai_agent:") => {
                    InboundRouteTarget::AiAgent(value.strip_prefix("ai_agent:")?.to_string())
                }
                value => InboundRouteTarget::Extension(value.strip_prefix("ext-")?.to_string()),
            };
            return Some(InboundRouteMatch {
                route_id: route.id.to_string(),
                route_name: route.name.clone(),
                transformed_caller,
                transformed_callee,
                target,
            });
        }
    }
    None
}

impl NumberAnalyzer for OutboundRouteAnalyzer {
    fn name(&self) -> &str {
        "outbound-route"
    }

    fn phase(&self) -> AnalysisPhase {
        AnalysisPhase::OutboundRoute
    }

    fn priority(&self) -> i32 {
        100
    }

    fn analyze(&self, request: &NumberAnalysisRequest) -> RouteDecision {
        let config = self.state.config().snapshot();
        let Some(domain) = config.domains.get(&request.domain_id) else {
            return RouteDecision::Continue;
        };

        for route in domain.outbound_routes.iter().filter(|route| route.enabled) {
            if !route_matches(route, &request.caller, &request.callee) {
                continue;
            }
            let Some(transformed_caller) = transform_number(
                &request.caller,
                route.src_strip,
                &route.src_prefix,
                &route.src_suffix,
            ) else {
                continue;
            };
            let Some(transformed_callee) = transform_number(
                &request.callee,
                route.dst_strip,
                &route.dst_prefix,
                &route.dst_suffix,
            ) else {
                continue;
            };
            let trunks = route
                .trunk_refs
                .iter()
                .filter(|trunk_ref| trunk_available(&self.state, domain, trunk_ref.as_str()))
                .map(|trunk_ref| TrunkCandidate {
                    trunk_id: TrunkId::from(trunk_ref.clone()),
                })
                .collect::<Vec<_>>();
            if trunks.is_empty() {
                return RouteDecision::Reject(RouteReject {
                    cause: HangupCause::TemporaryFailure,
                    sip_status: 503,
                    reason: RouteRejectReason::NoAvailableTrunk,
                });
            }
            return RouteDecision::Matched(Box::new(RouteResult::Trunk(TrunkRoute {
                domain_id: request.domain_id.clone(),
                route_id: route.id.to_string(),
                route_name: route.name.clone(),
                trunks,
                original_caller: request.caller.clone(),
                original_callee: request.callee.clone(),
                transformed_caller,
                transformed_callee,
            })));
        }

        RouteDecision::Continue
    }
}

fn route_matches(route: &OutboundRouteConfig, caller: &str, callee: &str) -> bool {
    pattern_matches(&route.dst_pattern, callee)
        && route
            .src_pattern
            .as_deref()
            .is_none_or(|pattern| pattern_matches(pattern, caller))
}

fn inbound_route_matches(route: &InboundRouteConfig, caller: &str, callee: &str) -> bool {
    pattern_matches(&route.dst_pattern, callee)
        && route
            .src_pattern
            .as_deref()
            .is_none_or(|pattern| pattern_matches(pattern, caller))
}

fn pattern_matches(pattern: &str, number: &str) -> bool {
    Regex::new(&format!(r"\A(?:{pattern})\z")).is_ok_and(|regex| regex.is_match(number))
}

fn transform_number(number: &str, strip: u8, prefix: &str, suffix: &str) -> Option<String> {
    let suffix_start = number
        .char_indices()
        .nth(usize::from(strip))
        .map(|(index, _)| index)
        .or_else(|| (number.chars().count() == usize::from(strip)).then_some(number.len()))?;
    Some(format!("{prefix}{}{suffix}", &number[suffix_start..]))
}

fn trunk_available(
    state: &AppState,
    domain: &crate::config_service::DomainRuntimeConfig,
    trunk_ref: &str,
) -> bool {
    let runtime = state.trunks();
    if let Some(id) = trunk_ref.strip_prefix("peer:") {
        let Ok(id) = id.parse::<u64>() else {
            return false;
        };
        let Some(trunk) = domain
            .peer_trunks
            .iter()
            .find(|trunk| trunk.id == id && trunk.enabled)
        else {
            return false;
        };
        return trunk.keep_alive_seconds == 0
            || (runtime.ready
                && runtime
                    .health
                    .get(&(
                        domain.domain_id.as_str().to_string(),
                        "peer".to_string(),
                        id,
                    ))
                    .is_some_and(|health| health.state == TrunkHealthState::Up));
    }

    let Some(reference) = trunk_ref.strip_prefix("reg:") else {
        return false;
    };
    let Some((trunk_id, account_id)) = reference.split_once('/') else {
        return false;
    };
    let (Ok(trunk_id), Ok(account_id)) = (trunk_id.parse::<u64>(), account_id.parse::<u64>())
    else {
        return false;
    };
    let Some(trunk) = domain
        .reg_trunks
        .iter()
        .find(|trunk| trunk.id == trunk_id && trunk.enabled)
    else {
        return false;
    };
    if !domain.reg_accounts.iter().any(|account| {
        account.id == account_id && account.reg_trunk_id == trunk_id && account.enabled
    }) {
        return false;
    }
    let registered = runtime.ready
        && runtime
            .registrations
            .get(&(domain.domain_id.as_str().to_string(), trunk_id, account_id))
            .is_some_and(|registration| registration.state == TrunkRegistrationState::Registered);
    let healthy = trunk.keep_alive_seconds == 0
        || (runtime.ready
            && runtime
                .health
                .get(&(
                    domain.domain_id.as_str().to_string(),
                    "register".to_string(),
                    trunk_id,
                ))
                .is_some_and(|health| health.state == TrunkHealthState::Up));
    registered && healthy
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(pattern: &str) -> OutboundRouteConfig {
        OutboundRouteConfig {
            id: 1,
            name: "test".to_string(),
            enabled: true,
            dst_pattern: pattern.to_string(),
            src_pattern: None,
            dst_strip: 0,
            dst_prefix: String::new(),
            dst_suffix: String::new(),
            src_strip: 0,
            src_prefix: String::new(),
            src_suffix: String::new(),
            priority: 100,
            trunk_refs: vec!["peer:1".to_string()],
        }
    }

    #[test]
    fn route_patterns_are_full_rust_regexes() {
        assert!(!route_matches(&route("5*"), "1002", "5219"));
        assert!(route_matches(&route("5[0-9]*"), "1002", "5219"));
    }

    #[test]
    fn number_transform_applies_strip_prefix_and_suffix() {
        assert_eq!(
            transform_number("05219", 1, "9", "#").as_deref(),
            Some("95219#")
        );
        assert_eq!(transform_number("52", 3, "", ""), None);
    }

    #[test]
    fn inbound_route_prefers_exact_trunk_and_parses_extension_target() {
        let mut domain = crate::config_service::DomainRuntimeConfig {
            domain_id: "domain-a".into(),
            name: "A".to_string(),
            realm: "example.com".to_string(),
            password: "secret".to_string(),
            remark: String::new(),
            enabled: true,
            extensions: Vec::new(),
            peer_trunks: Vec::new(),
            reg_trunks: Vec::new(),
            reg_accounts: Vec::new(),
            inbound_routes: vec![
                InboundRouteConfig {
                    id: 1,
                    name: "wildcard".to_string(),
                    enabled: true,
                    trunk_match: String::new(),
                    dst_pattern: "5[0-9]*".to_string(),
                    src_pattern: None,
                    dst_strip: 0,
                    dst_prefix: String::new(),
                    dst_suffix: String::new(),
                    src_strip: 0,
                    src_prefix: String::new(),
                    src_suffix: String::new(),
                    target: "rej".to_string(),
                    priority: 100,
                },
                InboundRouteConfig {
                    id: 2,
                    name: "exact".to_string(),
                    enabled: true,
                    trunk_match: "reg:1/1".to_string(),
                    dst_pattern: "5[0-9]*".to_string(),
                    src_pattern: None,
                    dst_strip: 0,
                    dst_prefix: String::new(),
                    dst_suffix: String::new(),
                    src_strip: 0,
                    src_prefix: String::new(),
                    src_suffix: String::new(),
                    target: "ext-1002".to_string(),
                    priority: 100,
                },
            ],
            outbound_routes: Vec::new(),
            recording_policies: Vec::new(),
            ai_policies: Vec::new(),
            ai_agents: Vec::new(),
            version: 1,
        };
        domain
            .inbound_routes
            .sort_by_key(|route| (route.priority, route.id));

        let matched = analyze_inbound_route(&domain, "reg:1/1", "5219", "5217").unwrap();
        assert_eq!(matched.route_name, "exact");
        assert_eq!(
            matched.target,
            InboundRouteTarget::Extension("1002".to_string())
        );
    }

    #[test]
    fn inbound_route_parses_ai_agent_target() {
        let domain = crate::config_service::DomainRuntimeConfig {
            domain_id: "domain-a".into(),
            name: "A".to_string(),
            realm: "example.com".to_string(),
            password: "secret".to_string(),
            remark: String::new(),
            enabled: true,
            extensions: Vec::new(),
            peer_trunks: Vec::new(),
            reg_trunks: Vec::new(),
            reg_accounts: Vec::new(),
            inbound_routes: vec![InboundRouteConfig {
                id: 1,
                name: "agent".to_string(),
                enabled: true,
                trunk_match: String::new(),
                dst_pattern: "9000".to_string(),
                src_pattern: None,
                dst_strip: 0,
                dst_prefix: String::new(),
                dst_suffix: String::new(),
                src_strip: 0,
                src_prefix: String::new(),
                src_suffix: String::new(),
                target: "ai_agent:agent-sales".to_string(),
                priority: 1,
            }],
            outbound_routes: Vec::new(),
            recording_policies: Vec::new(),
            ai_policies: Vec::new(),
            ai_agents: Vec::new(),
            version: 1,
        };
        let matched = analyze_inbound_route(&domain, "", "1001", "9000").unwrap();
        assert_eq!(
            matched.target,
            InboundRouteTarget::AiAgent("agent-sales".to_string())
        );
    }
}
