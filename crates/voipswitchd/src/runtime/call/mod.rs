mod action;
mod actor;
pub(crate) mod attempt;
mod cdr;
mod cdr_spool;
pub(crate) mod cdr_writer;
mod config_snapshot;
mod dtmf;
pub(crate) mod dtmf_operation;
pub(crate) mod event;
pub(crate) mod handoff;
mod media_action;
mod model;
mod registry;
pub(crate) mod session;
pub(crate) mod timer;

pub(crate) use self::action::reject_inbound as reject_inbound_invite;
use self::action::{AdapterActionExecutor, call_action_id, reject_inbound};
use self::attempt::AttemptRegistrar;
use self::config_snapshot::{
    AiPolicyDecisionSnapshot, CallConfigSnapshot, CallNumberSnapshot, RouteSnapshot,
};
pub(crate) use self::dtmf::DigitCollectionSpec;
use self::event::{CallLegEvent, InboundInviteOffered, InboundInviteSource, RouteAnalysisFailure};
use self::media_action::{MediaActionExecutor, MediaActionResult, PrepareSdpPurpose};
use self::model::{CallRuntime, OutboundCandidate, OutboundTarget, ResolvedRoute};
use self::registry::LegEventDeduper;
use self::session::{CallCoordinatorHandle, CriticalControlDispatcher, SessionHandle};
use self::timer::{CallTimeouts, CallTimerKind};
use crate::ai::{AiMediaTapSpec, AiTapStreamSpec};
use crate::app::{AppState, RegistrationState};
use crate::pbx::recording::model::{RecordingDirection, RecordingTargetType};
use crate::pbx::route::analysis::{InboundRouteTarget, analyze_inbound_route};
use crate::runtime::adapter::AdapterRuntimeWriter;
pub(crate) use crate::runtime::call::actor::{CalleeSessionActor, SessionActor};
use crate::runtime::call::actor::{CoordinatorRuntimeState, InitialCallAction, PendingAnswer};
use crate::runtime::media::{DtmfSessionBindings, MediaPlaneManager};
use crate::runtime::recording::RecordingSpec;
use ai_protocol::control::{
    AiProfileSnapshot, AudioCodec, JobRef, MediaDirection, Participant, StreamBinding,
    SubmitPostCallJob,
};
use ai_protocol::id::{ConversationId, JobId, OperationId, ParticipantId, StreamId, TenantId};
use anyhow::{Result, anyhow};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};
use voipswitch_core::analysis::{
    AnalysisRegistry, NumberAnalysisRequest, RouteDecision, RouteResult,
};
use voipswitch_core::media::parse_audio_sdp;
use voipswitch_core::types::call::{CallState, HangupCause};
use voipswitch_core::types::ids::DomainId;
use voipswitch_core::types::time::unix_timestamp_ms;

static CALL_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub(crate) struct CreatedCall {
    pub caller_actor: SessionActor,
    pub caller_handle: SessionHandle,
    pub coordinator_handle: CallCoordinatorHandle,
    pub callee_actor: CalleeSessionActor,
    pub callee_handle: SessionHandle,
    pub call_id: String,
    pub caller_session_id: String,
    pub callee_session_id: String,
    pub caller_adapter_leg_id: String,
}

#[derive(Clone)]
pub(crate) struct CallActorServices {
    pub(crate) state: AppState,
    pub(crate) analysis: AnalysisRegistry,
    pub(crate) writer: AdapterRuntimeWriter,
    pub(crate) media_plane: MediaPlaneManager,
    pub(crate) timeouts: CallTimeouts,
    pub(crate) finished_tx: mpsc::Sender<String>,
    pub(crate) control_dispatcher: CriticalControlDispatcher,
    pub(crate) attempt_registrar: AttemptRegistrar,
}

impl SessionActor {
    pub(crate) async fn create_call(
        event: InboundInviteOffered,
        services: CallActorServices,
    ) -> Result<Option<CreatedCall>> {
        let CallActorServices {
            state,
            analysis,
            writer,
            media_plane,
            timeouts,
            finished_tx,
            control_dispatcher,
            attempt_registrar,
        } = services;
        let domain_id = DomainId::from(event.domain_id.clone());
        let config = state.config().snapshot();
        let Some(domain) = config.domains.get(&domain_id).filter(|d| d.enabled) else {
            reject_inbound(&writer, &event, 404).await?;
            return Ok(None);
        };
        if state
            .cdr_writer()
            .is_some_and(|cdr_writer| !cdr_writer.admits_new_call(domain_id.as_str()))
        {
            warn!(
                domain_id = domain_id.as_str(),
                "rejecting new call because domain CDR spool is over capacity"
            );
            reject_inbound(&writer, &event, 503).await?;
            return Ok(None);
        }
        let inbound_trunk_ref = event.origin.as_ref().and_then(|origin| match origin {
            InboundInviteSource::Trunk { trunk_ref } => Some(trunk_ref.clone()),
            InboundInviteSource::Endpoint { .. } => None,
        });
        let mut inbound_route_id = None;
        let mut inbound_route_name = None;
        let mut direct_extension = None;
        let request = if let Some(trunk_ref) = inbound_trunk_ref.as_deref() {
            let Some(matched) = analyze_inbound_route(
                domain,
                trunk_ref,
                &event.caller_number,
                &event.callee_number,
            ) else {
                reject_inbound(&writer, &event, 404).await?;
                return Ok(None);
            };
            inbound_route_id = Some(matched.route_id);
            inbound_route_name = Some(matched.route_name);
            match matched.target {
                InboundRouteTarget::Reject => {
                    reject_inbound(&writer, &event, 403).await?;
                    return Ok(None);
                }
                InboundRouteTarget::Extension(number) => {
                    direct_extension = Some((number, matched.transformed_caller.clone()));
                }
                InboundRouteTarget::Auto => {}
            }
            NumberAnalysisRequest {
                domain_id: domain_id.clone(),
                caller: matched.transformed_caller,
                callee: matched.transformed_callee,
            }
        } else {
            NumberAnalysisRequest {
                domain_id: domain_id.clone(),
                caller: event.caller_number.clone(),
                callee: event.callee_number.clone(),
            }
        };
        let resolved_route = if let Some((number, signaling_caller_number)) = direct_extension {
            let Some(extension) = domain
                .extensions
                .iter()
                .find(|e| e.enabled && e.number == number)
            else {
                reject_inbound(&writer, &event, 404).await?;
                return Ok(None);
            };
            let Some(route) = endpoint_resolved_route(
                &state,
                &event.domain_id,
                extension.id.to_string(),
                extension.number.clone(),
                signaling_caller_number,
                extension.number.clone(),
            ) else {
                reject_inbound(&writer, &event, 480).await?;
                return Ok(None);
            };
            route
        } else {
            let route =
                match analyze_route_with_timeout(analysis.clone(), request, timeouts.route).await {
                    Ok(decision) => decision,
                    Err(RouteAnalysisFailure::Timeout) => {
                        reject_inbound(&writer, &event, 408).await?;
                        return Ok(None);
                    }
                    Err(RouteAnalysisFailure::Worker(msg)) => {
                        warn!(
                            error = msg,
                            domain_id = event.domain_id,
                            "basic call route worker failed"
                        );
                        reject_inbound(&writer, &event, 500).await?;
                        return Ok(None);
                    }
                };
            let route = match route {
                RouteDecision::Matched(r) => r,
                RouteDecision::Reject(r) => {
                    reject_inbound(&writer, &event, r.sip_status).await?;
                    return Ok(None);
                }
                RouteDecision::Continue => {
                    reject_inbound(&writer, &event, 404).await?;
                    return Ok(None);
                }
            };
            match *route {
                RouteResult::Extension(route) => {
                    let Some(resolved) = endpoint_resolved_route(
                        &state,
                        &event.domain_id,
                        route.endpoint.endpoint_id.as_str().to_string(),
                        route.endpoint.number.as_str().to_string(),
                        route.transformed_caller,
                        route.transformed_callee,
                    ) else {
                        reject_inbound(&writer, &event, 480).await?;
                        return Ok(None);
                    };
                    resolved
                }
                RouteResult::Trunk(route) => {
                    if route.trunks.is_empty() {
                        reject_inbound(&writer, &event, 503).await?;
                        return Ok(None);
                    }
                    let candidates = route
                        .trunks
                        .into_iter()
                        .map(|candidate| {
                            let trunk_ref = candidate.trunk_id.as_str().to_string();
                            OutboundCandidate {
                                outbound_target: OutboundTarget::Trunk {
                                    trunk_ref: trunk_ref.clone(),
                                },
                                callee_route_target: trunk_route_target(domain, &trunk_ref),
                                outbound_trunk_name: trunk_display_name(domain, &trunk_ref),
                                outbound_trunk_ref: Some(trunk_ref),
                                recording_requested: false,
                                recording_policy_ids: Arc::from([]),
                                ai_policy: None,
                            }
                        })
                        .collect();
                    ResolvedRoute {
                        signaling_caller_number: route.transformed_caller,
                        signaling_callee_number: route.transformed_callee,
                        candidates,
                        outbound_route_id: Some(route.route_id),
                        outbound_route_name: Some(route.route_name),
                    }
                }
                RouteResult::CalleeSet(_) | RouteResult::BusinessModule(_) => {
                    reject_inbound(&writer, &event, 501).await?;
                    return Ok(None);
                }
            }
        };
        let ResolvedRoute {
            signaling_caller_number,
            signaling_callee_number,
            mut candidates,
            outbound_route_id,
            outbound_route_name,
        } = resolved_route;
        if candidates.is_empty() {
            reject_inbound(&writer, &event, 503).await?;
            return Ok(None);
        }
        let inbound_trunk_name = inbound_trunk_ref
            .as_deref()
            .and_then(|t| trunk_display_name(domain, t));
        let caller_extension_id = domain
            .extensions
            .iter()
            .find(|e| e.enabled && e.number == event.caller_number)
            .map(|e| e.id);
        for candidate in &mut candidates {
            candidate.recording_policy_ids = recording_policy_ids_for_candidate(
                domain,
                caller_extension_id,
                inbound_trunk_ref.as_deref(),
                &candidate.outbound_target,
            );
            candidate.recording_requested = !candidate.recording_policy_ids.is_empty();
            candidate.ai_policy = ai_policy_for_candidate(
                domain,
                caller_extension_id,
                inbound_trunk_ref.as_deref(),
                &candidate.outbound_target,
            );
        }
        let first_candidate = candidates
            .first()
            .cloned()
            .expect("candidate list checked above");
        let config_snapshot = Arc::new(
            CallConfigSnapshot::new(
                config.version,
                domain.version,
                domain_id.clone(),
                CallNumberSnapshot {
                    original_caller: event.caller_number.clone(),
                    original_callee: event.callee_number.clone(),
                    signaling_caller: signaling_caller_number.clone(),
                    signaling_callee: signaling_callee_number.clone(),
                },
                RouteSnapshot {
                    route_id: inbound_route_id.clone(),
                    route_name: inbound_route_name.clone(),
                    trunk_ref: inbound_trunk_ref.clone(),
                    trunk_name: inbound_trunk_name.clone(),
                },
                RouteSnapshot {
                    route_id: outbound_route_id.clone(),
                    route_name: outbound_route_name.clone(),
                    trunk_ref: first_candidate.outbound_trunk_ref.clone(),
                    trunk_name: first_candidate.outbound_trunk_name.clone(),
                },
                candidates,
                timeouts,
                config.recording_dir(),
            )
            .map_err(anyhow::Error::msg)?,
        );
        let (media, callee_offer) = match media_plane
            .allocate_bridge(
                &event.sdp_offer,
                event.route_target,
                first_candidate.callee_route_target,
            )
            .await
        {
            Ok(v) => v,
            Err(err) => {
                warn!(error = %err, "media allocation failed");
                reject_inbound(&writer, &event, 488).await?;
                return Ok(None);
            }
        };
        let media_handle = media.handle();
        let sequence = CALL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let call_id = format!("call-{}-{sequence}", unix_timestamp_ms());
        let caller_session_id = format!("session-{}-caller", call_id);
        let callee_session_id = format!("session-{}-callee-attempt-1", call_id);
        let started_at_ms = unix_timestamp_ms();
        let route_deadline = tokio::time::Instant::now() + config_snapshot.timeouts.route_budget;
        let mut call = CallRuntime::new(
            call_id.clone(),
            caller_session_id.clone(),
            callee_session_id.clone(),
            event.adapter_call_leg_id.clone(),
            &first_candidate,
            config_snapshot.clone(),
            started_at_ms,
        )
        .map_err(|error| anyhow!(error))?;
        call.media = Some(media);
        let (caller_handle, control_rx, event_rx) = SessionHandle::new_pair();
        let (coordinator_handle, coordinator_mailbox) =
            CallCoordinatorHandle::new_pair(call_id.clone());
        let (callee_handle, callee_control_rx, callee_event_rx) = SessionHandle::new_pair();
        let action_executor =
            AdapterActionExecutor::new(writer.clone(), control_dispatcher.clone());
        media_handle.bind_dtmf_sessions(DtmfSessionBindings {
            domain_id: event.domain_id.clone(),
            call_id: call_id.clone(),
            caller_session_id: caller_session_id.clone(),
            caller_control_tx: caller_handle.control_sender(),
            callee_session_id: callee_session_id.clone(),
            callee_control_tx: callee_handle.control_sender(),
            dispatcher: control_dispatcher.clone(),
        });
        let media_executor = MediaActionExecutor::spawn(
            media_handle,
            caller_session_id.clone(),
            coordinator_handle.weak_control_sender(),
            control_dispatcher.clone(),
        );
        let mut initial_actions = vec![InitialCallAction {
            callee: false,
            action_kind: "AdoptInboundInvite".to_string(),
            action_id: call_action_id("adopt", &call_id),
            body: json!({
                "domain_id": event.domain_id,
                "adapter_call_leg_id": event.adapter_call_leg_id,
            }),
        }];
        match &first_candidate.outbound_target {
            OutboundTarget::Endpoint {
                endpoint_id,
                endpoint_number,
            } => initial_actions.push(InitialCallAction {
                callee: true,
                action_kind: "OriginateEndpoint".to_string(),
                action_id: format!("originate-{call_id}-attempt-1"),
                body: json!({
                    "domain_id": domain.domain_id, "caller_session_id": caller_session_id,
                    "endpoint_id": endpoint_id, "endpoint_number": endpoint_number,
                    "caller_number": signaling_caller_number, "caller_display_name": Value::Null,
                    "callee_number": signaling_callee_number, "sdp_offer": callee_offer,
                    "sip_metadata": {}, "extension_headers": {},
                }),
            }),
            OutboundTarget::Trunk { trunk_ref } => initial_actions.push(InitialCallAction {
                callee: true,
                action_kind: "OriginateTrunk".to_string(),
                action_id: format!("originate-{call_id}-attempt-1"),
                body: json!({
                    "domain_id": domain.domain_id, "caller_session_id": caller_session_id,
                    "trunk_ref": trunk_ref, "caller_number": signaling_caller_number,
                    "caller_display_name": Value::Null, "callee_number": signaling_callee_number,
                    "sdp_offer": callee_offer, "sip_metadata": {}, "extension_headers": {},
                }),
            }),
        }
        let callee_session = call
            .make_callee_session(
                callee_session_id.clone(),
                1,
                &first_candidate.outbound_target,
                started_at_ms,
            )
            .map_err(|error| anyhow!(error))?;
        let actor = SessionActor {
            runtime: CoordinatorRuntimeState {
                call,
                actor_started: false,
                leg_event_deduper: LegEventDeduper::default(),
                finalized: false,
                finalization_started: false,
                action_generation: 1,
                completed_actions: HashSet::new(),
                media_generation: 0,
                dtmf_source: self::dtmf::DtmfSourceState::default(),
                dtmf_router: self::dtmf::DtmfRouter::default(),
                dtmf_collectors: std::collections::BTreeMap::new(),
                dtmf_collector_timer: None,
                dtmf_collector_timer_generation: 0,
                initial_actions,
                pending_answer: None,
                answer_in_progress: false,
                deferred_callee_disconnect: None,
                dial_timer: None,
                ring_timer: None,
                cleanup_timer: None,
                remaining_candidates: config_snapshot.candidates.iter().skip(1).cloned().collect(),
                current_attempt_seq: 1,
                pending_attempt_candidate: None,
                pending_attempt_registration: None,
                retry_after_cleanup: None,
                caller_offer: event.sdp_offer.clone(),
                route_deadline,
            },
            state: state.clone(),
            action_executor,
            media_executor,
            control_dispatcher: control_dispatcher.clone(),
            attempt_registrar,
            control_rx,
            event_rx,
            coordinator_control_rx: coordinator_mailbox.control_rx,
            coordinator_event_rx: coordinator_mailbox.event_rx,
            coordinator_handle: coordinator_handle.clone(),
            callee_control_tx: callee_handle.control_sender(),
            finished_tx,
            owned_session_id: caller_session_id.clone(),
            owned_action_generation: 1,
            owned_completed_actions: HashSet::new(),
        };
        let callee_actor = CalleeSessionActor {
            session: callee_session,
            attempt_seq: 1,
            leg_event_deduper: LegEventDeduper::default(),
            control_rx: callee_control_rx,
            event_rx: callee_event_rx,
            coordinator_handle: coordinator_handle.clone(),
            control_dispatcher,
            action_generation: 1,
            completed_actions: HashSet::new(),
            dtmf_source: self::dtmf::DtmfSourceState::default(),
        };
        let caller_adapter_leg_id = actor.call.caller_adapter_leg_id.as_str().to_string();
        Ok(Some(CreatedCall {
            caller_actor: actor,
            caller_handle,
            coordinator_handle,
            callee_actor,
            callee_handle,
            call_id,
            caller_session_id,
            callee_session_id,
            caller_adapter_leg_id,
        }))
    }

    async fn handle_provisional(&mut self, event: CallLegEvent) -> Result<()> {
        self.cancel_timer(CallTimerKind::Dial);
        {
            let call = &mut self.call;
            let status_code = event.status_code.unwrap_or(180);
            call.last_status = Some(status_code);
            call.state = CallState::Establishing;
        }
        if self.call.is_terminating() {
            return Ok(());
        }
        let status_code = event.status_code.unwrap_or(180);
        let start_ring_timer =
            matches!(status_code, 180 | 183) && self.call.ring_timer_generation == 0;
        if let Some(sdp) = event.sdp {
            if self.call.media.is_none() {
                self.fail_media_negotiation(
                    "CancelOutbound",
                    anyhow!("call media bridge missing"),
                )?;
                return Ok(());
            }
            self.media_generation = self.media_generation.saturating_add(1);
            self.media_executor.prepare_sdp(
                format!("prepare-provisional-{}-{status_code}", self.call.call_id()),
                self.media_generation,
                PrepareSdpPurpose::Provisional {
                    status_code,
                    start_ring_timer,
                },
                sdp,
            )?;
            self.publish_call_view();
            return Ok(());
        }
        self.submit_caller_action("ForwardProvisional", format!("provisional-{}-{status_code}", self.call.call_id()), json!({
            "session_id": self.call.caller_session_id(), "adapter_call_leg_id": self.call.caller_adapter_leg_id.as_str(),
            "status_code": status_code, "sdp": Value::Null,
        }))?;
        if start_ring_timer {
            self.start_timer(CallTimerKind::Ring);
        }
        self.publish_call_view();
        Ok(())
    }

    async fn handle_answered(&mut self, event: CallLegEvent) -> Result<()> {
        self.cancel_timer(CallTimerKind::Dial);
        if self.retry_after_late_answer()? {
            return Ok(());
        }
        if self.call.is_answered() || self.pending_answer.is_some() {
            return Ok(());
        }
        if self.call.is_terminating() {
            if self.call.late_answer_cleanup_sent {
                return Ok(());
            }
            self.call.late_answer_cleanup_sent = true;
            let call_id = self.call.call_id().to_string();
            self.submit_callee_action(
                "HangupDialog",
                format!("hangup-late-answer-{call_id}"),
                json!({}),
            )?;
            warn!(
                call_id,
                "late outbound answer received while call is terminating"
            );
            return Ok(());
        }
        let Some(answer) = event.sdp else {
            self.fail_media_negotiation(
                "HangupDialog",
                anyhow!("OutboundAnswered missing SDP answer"),
            )?;
            return Ok(());
        };
        if self.call.media.is_none() {
            self.fail_media_negotiation("HangupDialog", anyhow!("call media bridge missing"))?;
            return Ok(());
        }
        let payload_types = parse_audio_sdp(&answer)
            .map(|parsed| parsed.payload_types)
            .unwrap_or_default();
        self.media_generation = self.media_generation.saturating_add(1);
        self.answer_in_progress = true;
        self.media_executor.prepare_sdp(
            format!("prepare-answer-{}", self.call.call_id()),
            self.media_generation,
            PrepareSdpPurpose::Answer { payload_types },
            answer,
        )?;
        Ok(())
    }

    fn complete_answer(
        &mut self,
        caller_answer: voipswitch_core::media::SdpBody,
        payload_types: Vec<u8>,
    ) -> Result<()> {
        let call_id = self.call.call_id().to_string();
        let action_id = format!("answer-{call_id}");
        self.submit_caller_action("AnswerInboundInvite", action_id.clone(), json!({
            "domain_id": self.call.domain_id(), "call_id": call_id,
            "adapter_call_leg_id": self.call.caller_adapter_leg_id.as_str(), "status_code": 200, "sdp_answer": caller_answer,
            "sip_metadata": {}, "extension_headers": {},
        }))?;
        self.call.state = CallState::Answering;
        self.pending_answer = Some(PendingAnswer {
            action_id,
            payload_types,
        });
        self.publish_call_view();
        Ok(())
    }

    pub(crate) fn confirm_answer(&mut self, action_id: &str) -> Result<()> {
        let Some(pending) = self.pending_answer.take() else {
            return Ok(());
        };
        if pending.action_id != action_id || self.call.is_terminating() {
            self.answer_in_progress = false;
            return Ok(());
        }
        let call_id = self.call.call_id().to_string();
        self.call.state = CallState::Connected;
        let answered_at_ms = unix_timestamp_ms();
        let media_generation = self.runtime.media_generation;
        self.call
            .mark_answered(answered_at_ms, media_generation)
            .map_err(anyhow::Error::msg)?;
        self.call.last_status = Some(200);
        self.cancel_timer(CallTimerKind::Ring);
        let recording = if self.call.recording_requested {
            Some(recording_spec_from_snapshot(
                &self.call,
                answered_at_ms,
                pending.payload_types.clone(),
            ))
        } else {
            None
        };
        let ai_capture_requested = self.try_start_ai_capture(&pending.payload_types);
        self.media_executor.start_answer_media(
            format!("start-answer-media-{call_id}"),
            self.media_generation,
            recording,
            call_id,
            !self.call.recording_requested && !ai_capture_requested,
        )?;
        self.answer_in_progress = false;
        self.publish_call_view();
        if let Some(event) = self.deferred_callee_disconnect.take() {
            self.handle_disconnected(event)?;
        }
        Ok(())
    }

    fn try_start_ai_capture(&self, payload_types: &[u8]) -> bool {
        let Some(policy) = self.call.ai_policy.as_ref() else {
            return false;
        };
        let Some(ai_jobs) = self.state.ai_jobs() else {
            warn!(call_id = self.call.call_id(), "AI service unavailable");
            return false;
        };
        let Some(profile) = ai_jobs.executable_profile(&policy.profile_id) else {
            warn!(
                call_id = self.call.call_id(),
                ai_policy_id = policy.policy_id,
                ai_profile_id = policy.profile_id,
                "AI capture unavailable because profile is not executable"
            );
            return false;
        };
        let Some((payload_type, codec)) =
            payload_types
                .iter()
                .find_map(|payload_type| match payload_type {
                    0 => Some((0, AudioCodec::Pcmu)),
                    8 => Some((8, AudioCodec::Pcma)),
                    _ => None,
                })
        else {
            warn!(
                call_id = self.call.call_id(),
                ai_policy_id = policy.policy_id,
                "AI capture unavailable because negotiated codec is unsupported"
            );
            return false;
        };
        let result = build_ai_submission(&self.call, profile, payload_type, codec).and_then(
            |(request, tap)| ai_jobs.try_submit(request, self.media_executor.media_handle(), tap),
        );
        match result {
            Ok(()) => {
                info!(
                    call_id = self.call.call_id(),
                    ai_policy_id = policy.policy_id,
                    ai_profile_id = policy.profile_id,
                    "AI post-call capture submitted"
                );
                true
            }
            Err(error) => {
                warn!(
                    call_id = self.call.call_id(),
                    ai_policy_id = policy.policy_id,
                    error = %error,
                    "AI capture submission skipped"
                );
                false
            }
        }
    }

    fn fail_media_negotiation(&mut self, callee_command: &str, error: anyhow::Error) -> Result<()> {
        if self.call.is_terminating() {
            return Ok(());
        }
        self.call.begin_terminating(HangupCause::InternalError);
        self.answer_in_progress = false;
        self.call.state = CallState::Terminating;
        self.call.last_status = Some(488);
        self.call.hangup_cause = Some(HangupCause::IncompatibleDestination);
        self.cancel_timer(CallTimerKind::Dial);
        self.cancel_timer(CallTimerKind::Ring);
        self.media_generation = self.media_generation.saturating_add(1);
        let call_id = self.call.call_id().to_string();
        self.submit_callee_action(
            callee_command,
            format!("media-failure-callee-{call_id}"),
            json!({}),
        )?;
        self.submit_caller_action(
            "RejectInboundInvite",
            format!("media-failure-caller-{call_id}"),
            json!({
                "adapter_call_leg_id": self.call.caller_adapter_leg_id.as_str(),
                "status_code": 488,
            }),
        )?;
        warn!(call_id, callee_command, error = %error, "basic call media negotiation failed");
        self.start_timer(CallTimerKind::Cleanup);
        self.publish_call_view();
        Ok(())
    }

    pub(crate) fn handle_media_action_result(&mut self, result: MediaActionResult) -> Result<()> {
        match result {
            MediaActionResult::SdpPrepared {
                action_id,
                generation,
                purpose,
                result,
            } => {
                if generation != self.media_generation || self.call.is_terminating() {
                    tracing::debug!(
                        call_id = self.call.call_id(),
                        action_id,
                        generation,
                        current_generation = self.media_generation,
                        "stale media SDP result ignored"
                    );
                    return Ok(());
                }
                let prepared = match result {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        let command = match purpose {
                            PrepareSdpPurpose::Provisional { .. } => "CancelOutbound",
                            PrepareSdpPurpose::Answer { .. } => "HangupDialog",
                            PrepareSdpPurpose::CalleeAttemptOffer { attempt_seq } => {
                                self.handle_attempt_offer_prepared(attempt_seq, Err(error))?;
                                return Ok(());
                            }
                        };
                        self.fail_media_negotiation(command, anyhow!(error))?;
                        return Ok(());
                    }
                };
                match purpose {
                    PrepareSdpPurpose::Provisional {
                        status_code,
                        start_ring_timer,
                    } => {
                        self.submit_caller_action(
                            "ForwardProvisional",
                            format!("provisional-{}-{status_code}", self.call.call_id()),
                            json!({
                                "adapter_call_leg_id": self.call.caller_adapter_leg_id.as_str(),
                                "status_code": status_code,
                                "sdp": prepared,
                            }),
                        )?;
                        if start_ring_timer {
                            self.start_timer(CallTimerKind::Ring);
                        }
                    }
                    PrepareSdpPurpose::Answer { payload_types } => {
                        self.complete_answer(prepared, payload_types)?;
                    }
                    PrepareSdpPurpose::CalleeAttemptOffer { attempt_seq } => {
                        self.handle_attempt_offer_prepared(attempt_seq, Ok(prepared))?;
                    }
                }
            }
            MediaActionResult::AnswerMediaStarted {
                action_id,
                generation,
                recording_error,
                caller_remote,
                callee_remote,
            } => {
                if generation != self.media_generation {
                    tracing::debug!(
                        call_id = self.call.call_id(),
                        action_id,
                        generation,
                        current_generation = self.media_generation,
                        "stale answer media result ignored"
                    );
                    return Ok(());
                }
                if let Some(error) = recording_error {
                    warn!(
                        call_id = self.call.call_id(),
                        error, "start recording failed"
                    );
                    self.call.recording_start_error = Some(error);
                }
                self.call
                    .activate_current_bridge(generation, unix_timestamp_ms())
                    .map_err(anyhow::Error::msg)?;
                self.publish_call_view();
                info!(call_id = self.call.call_id(), caller_remote = ?caller_remote, callee_remote = ?callee_remote, "basic call answered");
            }
            MediaActionResult::DtmfCapabilityAcquired {
                action_id,
                generation,
                result,
            } => {
                if generation != self.media_generation || self.call.is_terminating() {
                    tracing::debug!(
                        call_id = self.call.call_id(),
                        action_id,
                        generation,
                        current_generation = self.media_generation,
                        "stale DTMF capability result ignored"
                    );
                    return Ok(());
                }
                self.handle_dtmf_capability_acquired(action_id, result);
            }
            MediaActionResult::DtmfCapabilityReleased {
                action_id,
                generation,
                result,
            } => {
                if generation != self.media_generation {
                    tracing::debug!(
                        call_id = self.call.call_id(),
                        action_id,
                        generation,
                        current_generation = self.media_generation,
                        "stale DTMF capability release ignored"
                    );
                    return Ok(());
                }
                self.handle_dtmf_capability_released(action_id, result);
            }
        }
        Ok(())
    }

    async fn handle_failed(&mut self, event: CallLegEvent) -> Result<()> {
        self.cancel_timer(CallTimerKind::Dial);
        let status_code = event.status_code.unwrap_or(480);
        if self.retry_after_attempt_ended(status_code)? {
            return Ok(());
        }
        let should_signal_peer = !self.call.is_terminating();
        self.call.last_status = event.status_code;
        self.call.state = CallState::Terminating;
        self.call.hangup_cause = Some(HangupCause::CallRejected);
        if event.session_id.as_deref() == Some(self.call.callee_session_id()) {
            self.call.callee_terminated = true;
        }
        let mut start_cleanup_timer = false;
        if should_signal_peer {
            self.call.begin_terminating(HangupCause::InternalError);
            self.cancel_timer(CallTimerKind::Ring);
            let call_id = self.call.call_id().to_string();
            self.submit_caller_action(
                "RejectInboundInvite",
                format!("reject-{call_id}"),
                json!({
                    "adapter_call_leg_id": self.call.caller_adapter_leg_id.as_str(),
                    "status_code": event.status_code.unwrap_or(480),
                }),
            )?;
            start_cleanup_timer = true;
        }
        if self.call.caller_terminated && self.call.callee_terminated {
            self.begin_finish_call();
        } else {
            if start_cleanup_timer {
                self.start_timer(CallTimerKind::Cleanup);
            }
            self.publish_call_view();
        }
        Ok(())
    }

    async fn handle_cancelled(&mut self, _event: CallLegEvent) -> Result<()> {
        self.cancel_timer(CallTimerKind::Dial);
        let should_signal_peer = !self.call.is_terminating();
        self.call.caller_terminated = true;
        self.call.state = CallState::Terminating;
        self.call.hangup_cause = Some(HangupCause::OriginatorCancel);
        let mut start_cleanup_timer = false;
        if should_signal_peer {
            self.call.begin_terminating(HangupCause::InternalError);
            self.cancel_timer(CallTimerKind::Ring);
            let (command, action_id) = if self.call.is_answered() {
                (
                    "HangupDialog",
                    format!("hangup-after-cancel-{}", self.call.call_id()),
                )
            } else {
                ("CancelOutbound", format!("cancel-{}", self.call.call_id()))
            };
            self.submit_callee_action(
                command,
                action_id,
                json!({
                    "reason": "caller_cancelled",
                }),
            )?;
            start_cleanup_timer = true;
        }
        if self.call.caller_terminated && self.call.callee_terminated {
            self.begin_finish_call();
        } else {
            if start_cleanup_timer {
                self.start_timer(CallTimerKind::Cleanup);
            }
            self.publish_call_view();
        }
        Ok(())
    }

    fn handle_disconnected(&mut self, event: CallLegEvent) -> Result<()> {
        let session_id = event.session_id.as_deref();
        if session_id == Some(self.call.callee_session_id()) && self.retry_after_cleanup.is_some() {
            self.retry_after_attempt_ended(event.status_code.unwrap_or(487))?;
            return Ok(());
        }
        self.cancel_timer(CallTimerKind::Dial);
        let should_signal_peer = !self.call.is_terminating();
        self.call.state = CallState::Terminating;
        self.call.last_status = event.status_code.or(self.call.last_status);
        self.call
            .hangup_cause
            .get_or_insert(HangupCause::NormalClearing);
        if session_id == Some(self.call.caller_session_id()) {
            self.call.caller_terminated = true;
        } else if session_id == Some(self.call.callee_session_id()) {
            self.call.callee_terminated = true;
        }
        let mut start_cleanup_timer = false;
        if should_signal_peer {
            self.call.begin_terminating(HangupCause::InternalError);
            self.cancel_timer(CallTimerKind::Ring);
            let peer_is_callee = session_id == Some(self.call.caller_session_id());
            let call_id = self.call.call_id().to_string();
            if peer_is_callee {
                self.submit_callee_action("HangupDialog", format!("hangup-{call_id}"), json!({}))?;
            } else {
                self.submit_caller_action("HangupDialog", format!("hangup-{call_id}"), json!({}))?;
            }
            start_cleanup_timer = true;
        }
        info!(call_id = self.call.call_id(), session_id, status = ?event.status_code, reason = ?event.reason, "dialog disconnected");
        if self.call.caller_terminated && self.call.callee_terminated {
            self.begin_finish_call();
        } else {
            if start_cleanup_timer {
                self.start_timer(CallTimerKind::Cleanup);
            }
            self.publish_call_view();
        }
        Ok(())
    }
}

fn endpoint_resolved_route(
    state: &AppState,
    domain_id: &str,
    endpoint_id: String,
    endpoint_number: String,
    signaling_caller_number: String,
    signaling_callee_number: String,
) -> Option<ResolvedRoute> {
    let registrations = state.registrations();
    let registration = registrations
        .items
        .get(&(domain_id.to_string(), endpoint_id.clone()))
        .filter(|item| matches!(item.state, RegistrationState::Registered))?;
    registrations.ready.then_some(ResolvedRoute {
        signaling_caller_number,
        signaling_callee_number,
        candidates: vec![OutboundCandidate {
            outbound_target: OutboundTarget::Endpoint {
                endpoint_id,
                endpoint_number,
            },
            callee_route_target: registration.route_target,
            outbound_trunk_ref: None,
            outbound_trunk_name: None,
            recording_requested: false,
            recording_policy_ids: Arc::from([]),
            ai_policy: None,
        }],
        outbound_route_id: None,
        outbound_route_name: None,
    })
}

fn recording_policy_ids_for_candidate(
    domain: &crate::config_service::DomainRuntimeConfig,
    caller_extension_id: Option<u64>,
    inbound_trunk_ref: Option<&str>,
    outbound_target: &OutboundTarget,
) -> Arc<[u64]> {
    domain
        .recording_policies
        .iter()
        .filter(|policy| {
            policy.enabled
            && policy
                .targets
                .iter()
                .any(|target| match target.target_type {
                    RecordingTargetType::Extension => {
                        (caller_extension_id == Some(target.target_id)
                            && policy.direction.matches(RecordingDirection::Outbound))
                            || (matches!(outbound_target, OutboundTarget::Endpoint { endpoint_id, .. } if endpoint_id.parse::<u64>().ok() == Some(target.target_id))
                                && policy.direction.matches(RecordingDirection::Inbound))
                    }
                    RecordingTargetType::PeerTrunk => {
                        let target_ref = format!("peer:{}", target.target_id);
                        (inbound_trunk_ref == Some(target_ref.as_str())
                            && policy.direction.matches(RecordingDirection::Inbound))
                            || (matches!(outbound_target, OutboundTarget::Trunk { trunk_ref } if trunk_ref == &target_ref)
                                && policy.direction.matches(RecordingDirection::Outbound))
                    }
                    RecordingTargetType::RegTrunk => {
                        let inbound_matches = inbound_trunk_ref
                            .is_some_and(|trunk| reg_trunk_id(trunk) == Some(target.target_id));
                        let outbound_matches = matches!(outbound_target, OutboundTarget::Trunk { trunk_ref } if reg_trunk_id(trunk_ref) == Some(target.target_id));
                        (inbound_matches
                            && policy.direction.matches(RecordingDirection::Inbound))
                            || (outbound_matches
                                && policy.direction.matches(RecordingDirection::Outbound))
                    }
                })
        })
        .map(|policy| policy.id)
        .collect::<Vec<_>>()
        .into()
}

fn ai_policy_for_candidate(
    domain: &crate::config_service::DomainRuntimeConfig,
    caller_extension_id: Option<u64>,
    inbound_trunk_ref: Option<&str>,
    outbound_target: &OutboundTarget,
) -> Option<AiPolicyDecisionSnapshot> {
    use crate::pbx::recording::model::RecordingTargetType;

    let outbound_trunk_ref = match outbound_target {
        OutboundTarget::Trunk { trunk_ref } => Some(trunk_ref.as_str()),
        OutboundTarget::Endpoint { .. } => None,
    };
    let callee_extension_id = match outbound_target {
        OutboundTarget::Endpoint { endpoint_id, .. } => endpoint_id.parse::<u64>().ok(),
        OutboundTarget::Trunk { .. } => None,
    };
    domain
        .ai_policies
        .iter()
        .filter(|policy| {
            policy.enabled
                && policy
                    .direction
                    .matches(inbound_trunk_ref.is_some(), outbound_trunk_ref.is_some())
                && policy
                    .targets
                    .iter()
                    .any(|target| match target.target_type {
                        RecordingTargetType::Extension => {
                            caller_extension_id == Some(target.target_id)
                                || callee_extension_id == Some(target.target_id)
                        }
                        RecordingTargetType::PeerTrunk => {
                            let target_ref = format!("peer:{}", target.target_id);
                            inbound_trunk_ref == Some(target_ref.as_str())
                                || outbound_trunk_ref == Some(target_ref.as_str())
                        }
                        RecordingTargetType::RegTrunk => {
                            inbound_trunk_ref
                                .is_some_and(|trunk| reg_trunk_id(trunk) == Some(target.target_id))
                                || outbound_trunk_ref.is_some_and(|trunk| {
                                    reg_trunk_id(trunk) == Some(target.target_id)
                                })
                        }
                    })
        })
        .map(|policy| AiPolicyDecisionSnapshot {
            policy_id: policy.id,
            profile_id: policy.ai_profile_id.clone(),
        })
        .next()
}

fn recording_spec_from_snapshot(
    call: &CallRuntime,
    answered_at_ms: u64,
    payload_types: Vec<u8>,
) -> RecordingSpec {
    RecordingSpec {
        call_id: call.call_id().to_string(),
        domain_id: call.domain_id().to_string(),
        caller_number: call.config_snapshot.numbers.original_caller.clone(),
        callee_number: call.config_snapshot.numbers.original_callee.clone(),
        started_at_ms: answered_at_ms,
        recording_dir: call.config_snapshot.recording.recording_dir.clone(),
        payload_types,
    }
}

fn build_ai_submission(
    call: &CallRuntime,
    profile: AiProfileSnapshot,
    payload_type: u8,
    codec: AudioCodec,
) -> Result<(SubmitPostCallJob, AiMediaTapSpec)> {
    let call_id = call.call_id().to_string();
    let job = JobRef {
        job_id: JobId::new(format!("ai-{call_id}"))?,
        tenant_id: TenantId::new(call.domain_id().to_string())?,
        conversation_id: ConversationId::new(call_id)?,
        operation_id: OperationId::new("post-call-v1")?,
        generation: 1,
    };
    let caller_participant = ParticipantId::new("caller")?;
    let callee_participant = ParticipantId::new("callee")?;
    let caller_stream = StreamId::new("caller-audio")?;
    let callee_stream = StreamId::new("callee-audio")?;
    let request = SubmitPostCallJob {
        job: job.clone(),
        profile,
        participants: vec![
            Participant {
                participant_id: caller_participant.clone(),
                role: "caller".to_string(),
                display_number: Some(call.config_snapshot.numbers.original_caller.clone()),
            },
            Participant {
                participant_id: callee_participant.clone(),
                role: "callee".to_string(),
                display_number: Some(call.config_snapshot.numbers.original_callee.clone()),
            },
        ],
        streams: vec![
            StreamBinding {
                stream_id: caller_stream.clone(),
                participant_id: caller_participant.clone(),
                direction: MediaDirection::FromParticipant,
                codec,
                sample_rate: 8_000,
                channels: 1,
            },
            StreamBinding {
                stream_id: callee_stream.clone(),
                participant_id: callee_participant.clone(),
                direction: MediaDirection::FromParticipant,
                codec,
                sample_rate: 8_000,
                channels: 1,
            },
        ],
    };
    request.validate()?;
    let tap = AiMediaTapSpec {
        job,
        caller: AiTapStreamSpec {
            participant_id: caller_participant,
            stream_id: caller_stream,
            payload_type,
            codec,
            sample_rate: 8_000,
            channels: 1,
        },
        callee: AiTapStreamSpec {
            participant_id: callee_participant,
            stream_id: callee_stream,
            payload_type,
            codec,
            sample_rate: 8_000,
            channels: 1,
        },
    };
    Ok((request, tap))
}

fn trunk_display_name(
    domain: &crate::config_service::DomainRuntimeConfig,
    trunk_ref: &str,
) -> Option<String> {
    if let Some(id) = trunk_ref.strip_prefix("peer:") {
        let id = id.parse::<u64>().ok()?;
        return domain
            .peer_trunks
            .iter()
            .find(|trunk| trunk.id == id)
            .map(|trunk| trunk.name.clone());
    }

    let (trunk_id, account_id) = trunk_ref.strip_prefix("reg:")?.split_once('/')?;
    let trunk_id = trunk_id.parse::<u64>().ok()?;
    let account_id = account_id.parse::<u64>().ok()?;
    let trunk = domain
        .reg_trunks
        .iter()
        .find(|trunk| trunk.id == trunk_id)?;
    let account = domain
        .reg_accounts
        .iter()
        .find(|account| account.id == account_id && account.reg_trunk_id == trunk_id)?;
    Some(format!("{}/{}", trunk.name, account.auth_name))
}

fn reg_trunk_id(trunk_ref: &str) -> Option<u64> {
    trunk_ref
        .strip_prefix("reg:")?
        .split_once('/')?
        .0
        .parse()
        .ok()
}

fn trunk_route_target(
    domain: &crate::config_service::DomainRuntimeConfig,
    trunk_ref: &str,
) -> Option<SocketAddr> {
    let (host, port) = if let Some(id) = trunk_ref.strip_prefix("peer:") {
        let id = id.parse::<u64>().ok()?;
        let trunk = domain
            .peer_trunks
            .iter()
            .find(|trunk| trunk.id == id && trunk.enabled)?;
        (
            trunk
                .outbound_proxy_host
                .as_deref()
                .unwrap_or(&trunk.server_host),
            trunk.outbound_proxy_port.unwrap_or(trunk.server_port),
        )
    } else {
        let (trunk_id, account_id) = trunk_ref.strip_prefix("reg:")?.split_once('/')?;
        let trunk_id = trunk_id.parse::<u64>().ok()?;
        let account_id = account_id.parse::<u64>().ok()?;
        domain.reg_accounts.iter().find(|account| {
            account.id == account_id && account.reg_trunk_id == trunk_id && account.enabled
        })?;
        let trunk = domain
            .reg_trunks
            .iter()
            .find(|trunk| trunk.id == trunk_id && trunk.enabled)?;
        (
            trunk
                .outbound_proxy_host
                .as_deref()
                .unwrap_or(&trunk.server_host),
            trunk.outbound_proxy_port.unwrap_or(trunk.server_port),
        )
    };
    host.parse::<IpAddr>()
        .ok()
        .map(|address| SocketAddr::new(address, port))
}

async fn analyze_route_with_timeout(
    analysis: AnalysisRegistry,
    request: NumberAnalysisRequest,
    timeout: Duration,
) -> std::result::Result<RouteDecision, RouteAnalysisFailure> {
    match tokio::time::timeout(
        timeout,
        tokio::task::spawn_blocking(move || analysis.analyze(&request)),
    )
    .await
    {
        Ok(Ok(decision)) => Ok(decision),
        Ok(Err(err)) => Err(RouteAnalysisFailure::Worker(err.to_string())),
        Err(_) => Err(RouteAnalysisFailure::Timeout),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_service::DomainRuntimeConfig;
    use crate::pbx::ai_policy::model::{AiPolicyConfig, AiPolicyDirection};
    use crate::pbx::recording::model::RecordingTargetRef;
    use crate::runtime::call::registry::LegEventDeduper;
    use std::path::PathBuf;
    use std::sync::Arc;
    use voipswitch_core::analysis::{AnalysisPhase, NumberAnalyzer};

    #[test]
    fn call_action_ids_remain_unique_across_process_sequence_restarts() {
        let first = call_action_id("originate", "call-1784708165809-1");
        let after_restart = call_action_id("originate", "call-1784709000000-1");

        assert_ne!(first, after_restart);
        assert_eq!(after_restart, "originate-call-1784709000000-1");
    }

    #[test]
    fn leg_event_deduper_rejects_duplicate_and_stale_sequences() {
        let mut deduper = LegEventDeduper::default();
        assert!(deduper.accept("leg-1", 1));
        assert!(!deduper.accept("leg-1", 1));
        assert!(!deduper.accept("leg-1", 0));
        assert!(deduper.accept("leg-1", 2));
        assert!(deduper.accept("leg-2", 1));
    }

    #[test]
    fn ai_policy_uses_direction_then_priority_and_stable_id() {
        let mut policies = vec![
            AiPolicyConfig {
                id: 2,
                name: "second".to_string(),
                enabled: true,
                targets: vec![RecordingTargetRef {
                    target_type: RecordingTargetType::Extension,
                    target_id: 1001,
                }],
                direction: AiPolicyDirection::Internal,
                priority: 10,
                ai_profile_id: "profile-2".to_string(),
            },
            AiPolicyConfig {
                id: 1,
                name: "first".to_string(),
                enabled: true,
                targets: vec![RecordingTargetRef {
                    target_type: RecordingTargetType::Extension,
                    target_id: 1001,
                }],
                direction: AiPolicyDirection::Internal,
                priority: 10,
                ai_profile_id: "profile-1".to_string(),
            },
        ];
        policies.sort_by_key(|policy| (policy.priority, policy.id));
        let domain = DomainRuntimeConfig {
            domain_id: DomainId::from("domain-a"),
            name: "a".to_string(),
            realm: "a.example".to_string(),
            password: String::new(),
            remark: String::new(),
            enabled: true,
            extensions: Vec::new(),
            peer_trunks: Vec::new(),
            reg_trunks: Vec::new(),
            reg_accounts: Vec::new(),
            inbound_routes: Vec::new(),
            outbound_routes: Vec::new(),
            recording_policies: Vec::new(),
            ai_policies: policies,
            version: 1,
        };
        let target = OutboundTarget::Endpoint {
            endpoint_id: "1002".to_string(),
            endpoint_number: "1002".to_string(),
        };
        let selected = ai_policy_for_candidate(&domain, Some(1001), None, &target).unwrap();
        assert_eq!(selected.policy_id, 1);
        assert_eq!(selected.profile_id, "profile-1");
        assert!(ai_policy_for_candidate(&domain, Some(1001), Some("peer:8"), &target).is_none());
    }

    struct SlowAnalyzer;

    impl NumberAnalyzer for SlowAnalyzer {
        fn name(&self) -> &str {
            "slow"
        }

        fn phase(&self) -> AnalysisPhase {
            AnalysisPhase::CallerAnalysis
        }

        fn priority(&self) -> i32 {
            0
        }

        fn analyze(&self, _request: &NumberAnalysisRequest) -> RouteDecision {
            std::thread::sleep(Duration::from_millis(100));
            RouteDecision::Continue
        }
    }

    #[tokio::test]
    async fn route_analysis_is_bounded_by_timeout() {
        let registry = AnalysisRegistry::default();
        registry.register(Arc::new(SlowAnalyzer));
        let request = NumberAnalysisRequest {
            domain_id: DomainId::from("domain-route-timeout"),
            caller: "1001".to_string(),
            callee: "1002".to_string(),
        };

        let result = analyze_route_with_timeout(registry, request, Duration::from_millis(10)).await;
        assert_eq!(result, Err(RouteAnalysisFailure::Timeout));
    }

    #[test]
    fn answered_recording_uses_call_scoped_config_snapshot() {
        let candidate = OutboundCandidate {
            outbound_target: OutboundTarget::Endpoint {
                endpoint_id: "2".to_string(),
                endpoint_number: "1002".to_string(),
            },
            callee_route_target: Some("127.0.0.1:5090".parse().unwrap()),
            outbound_trunk_ref: None,
            outbound_trunk_name: None,
            recording_requested: true,
            recording_policy_ids: Arc::from([41_u64]),
            ai_policy: None,
        };
        let frozen_recording_dir = PathBuf::from("/tmp/voipswitch-recordings-v7");
        let frozen_timeouts = CallTimeouts {
            route: Duration::from_secs(1),
            dial: Duration::from_secs(2),
            ring: Duration::from_secs(3),
            cleanup: Duration::from_secs(4),
            route_budget: Duration::from_secs(5),
            min_attempt_window: Duration::from_millis(600),
        };
        let snapshot = Arc::new(
            CallConfigSnapshot::new(
                17,
                7,
                DomainId::from("domain-a"),
                CallNumberSnapshot {
                    original_caller: "1001".to_string(),
                    original_callee: "1002".to_string(),
                    signaling_caller: "1001".to_string(),
                    signaling_callee: "1002".to_string(),
                },
                RouteSnapshot {
                    route_id: None,
                    route_name: None,
                    trunk_ref: None,
                    trunk_name: None,
                },
                RouteSnapshot {
                    route_id: Some("route-1".to_string()),
                    route_name: Some("local".to_string()),
                    trunk_ref: None,
                    trunk_name: None,
                },
                vec![candidate.clone()],
                frozen_timeouts,
                frozen_recording_dir.clone(),
            )
            .unwrap(),
        );
        let call = CallRuntime::new(
            "call-a".to_string(),
            "caller-session".to_string(),
            "callee-session".to_string(),
            "adapter-leg".to_string(),
            &candidate,
            snapshot,
            100,
        )
        .unwrap();

        // Simulate a global reload before the 200 response is acknowledged.
        let latest_recording_dir = PathBuf::from("/tmp/voipswitch-recordings-v8");
        let latest_dial_timeout = Duration::from_secs(20);
        let spec = recording_spec_from_snapshot(&call, 200, vec![0, 8]);

        assert_ne!(latest_recording_dir, frozen_recording_dir);
        assert_eq!(spec.recording_dir, frozen_recording_dir);
        assert_eq!(spec.caller_number, "1001");
        assert_eq!(spec.callee_number, "1002");
        assert_eq!(call.config_snapshot.runtime_config_version, 17);
        assert_eq!(call.config_snapshot.domain_config_version, 7);
        assert_eq!(call.config_snapshot.candidates.len(), 1);
        assert_ne!(latest_dial_timeout, frozen_timeouts.dial);
        assert_eq!(call.config_snapshot.timeouts, frozen_timeouts);
        assert_eq!(&*call.config_snapshot.recording.initial_policy_ids, &[41]);
    }
}
