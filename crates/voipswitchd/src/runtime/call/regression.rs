use super::*;
use crate::data_store::{ConfigBackend, seaorm::SeaOrmConfigBackend};
use crate::runtime::adapter::{RuntimeEnvelope, spawn_adapter_runtime_writer};
use crate::runtime::call::timer::CallTimerEvent;
use tokio::time::{Duration, timeout};

pub(super) fn candidate(target: OutboundTarget) -> OutboundCandidate {
    OutboundCandidate {
        outbound_target: target,
        callee_route_target: None,
        outbound_trunk_ref: None,
        outbound_trunk_name: None,
        recording_requested: false,
        recording_policy_ids: Arc::from([]),
        ai_policy: None,
        ai_agent_revision: None,
        ai_fallback_target: None,
    }
}

pub(super) fn test_call(target: OutboundTarget) -> CallRuntime {
    let candidate = candidate(target);
    let route = RouteSnapshot {
        route_id: None,
        route_name: None,
        trunk_ref: None,
        trunk_name: None,
    };
    let snapshot = Arc::new(
        CallConfigSnapshot::new(
            1,
            1,
            DomainId::from("domain-a"),
            CallNumberSnapshot {
                original_caller: "1001".into(),
                original_callee: "1002".into(),
                signaling_caller: "1001".into(),
                signaling_callee: "1002".into(),
            },
            route.clone(),
            route,
            vec![candidate.clone()],
            CallTimeouts::default(),
            std::path::PathBuf::from("recordings"),
        )
        .unwrap(),
    );
    CallRuntime::new(
        "call-a".into(),
        "caller-a".into(),
        "callee-a".into(),
        "adapter-caller".into(),
        &candidate,
        snapshot,
        100,
    )
    .unwrap()
}

pub(super) fn test_state(path: &std::path::Path) -> AppState {
    let backend = Arc::new(SeaOrmConfigBackend::sqlite(path, "test").unwrap());
    let config = backend.load_runtime_config().unwrap();
    AppState::new(config, backend, 100)
}

async fn actor(state: AppState) -> (SessionActor, tokio::io::DuplexStream) {
    let target = OutboundTarget::Endpoint {
        endpoint_id: "2".into(),
        endpoint_number: "1002".into(),
    };
    let mut call = test_call(target);
    let offer = voipswitch_core::media::SdpBody { content_type: "application/sdp".into(), text: "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=test\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\nm=audio 4000 RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\n".to_owned() };
    let (media, _) = MediaPlaneManager::default()
        .allocate_bridge(&offer, None, None)
        .await
        .unwrap();
    let media_handle = media.handle();
    call.media = Some(media);
    let (_, control_rx, event_rx) = SessionHandle::new_pair();
    let (coordinator_handle, mailbox) = CallCoordinatorHandle::new_pair("call-a");
    let (callee_handle, _, _) = SessionHandle::new_pair();
    let (dispatcher, _) = CriticalControlDispatcher::spawn();
    let (write, read) = tokio::io::duplex(64 * 1024);
    let (writer, _) = spawn_adapter_runtime_writer(write);
    let executor = AdapterActionExecutor::new(writer, dispatcher.clone());
    let media_executor = MediaActionExecutor::spawn(
        media_handle,
        "caller-a".into(),
        coordinator_handle.weak_control_sender(),
        dispatcher.clone(),
    );
    let (attempt_registrar, _) = AttemptRegistrar::new();
    let (finished_tx, _) = mpsc::channel(10);
    (
        SessionActor {
            runtime: CoordinatorRuntimeState {
                call,
                actor_started: true,
                leg_event_deduper: LegEventDeduper::default(),
                finalized: false,
                finalization_started: false,
                action_generation: 1,
                completed_actions: HashSet::new(),
                media_generation: 0,
                dtmf_source: Default::default(),
                dtmf_router: Default::default(),
                dtmf_collectors: Default::default(),
                dtmf_collector_timer: None,
                dtmf_collector_timer_generation: 0,
                initial_actions: Vec::new(),
                pending_answer: None,
                answer_in_progress: false,
                deferred_callee_disconnect: None,
                dial_timer: None,
                ring_timer: None,
                cleanup_timer: None,
                remaining_candidates: Default::default(),
                current_attempt_seq: 1,
                pending_attempt_candidate: None,
                pending_attempt_registration: None,
                retry_after_cleanup: None,
                caller_offer: offer,
                route_deadline: tokio::time::Instant::now() + Duration::from_secs(60),
                pending_ai_transfer: None,
                ai_transfer_failed: false,
                prompt_generation: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            },
            state,
            action_executor: executor,
            media_executor,
            control_dispatcher: dispatcher,
            attempt_registrar,
            control_rx,
            event_rx,
            coordinator_control_rx: mailbox.control_rx,
            coordinator_event_rx: mailbox.event_rx,
            coordinator_handle,
            callee_control_tx: callee_handle.control_sender(),
            finished_tx,
            owned_session_id: "caller-a".into(),
            owned_action_generation: 1,
            owned_completed_actions: HashSet::new(),
        },
        read,
    )
}

fn event(status_code: u16) -> CallLegEvent {
    CallLegEvent {
        call_id: Some("call-a".into()),
        session_id: Some("callee-a".into()),
        adapter_call_leg_id: "adapter-callee".into(),
        leg_event_seq: 1,
        status_code: Some(status_code),
        sdp: None,
        reason: None,
    }
}

fn with_state(test: impl AsyncFnOnce(AppState)) {
    let temp = tempfile::tempdir().unwrap();
    let state = test_state(temp.path());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(test(state.clone()));
    drop(runtime);
    drop(state);
}

#[test]
fn second_attempt_rings_once_and_times_out() {
    with_state(async |state| {
        let (mut actor, _reader) = actor(state).await;
        actor.handle_provisional(event(180)).await.unwrap();
        let first_generation = actor.call.ring_timer_generation;
        actor.cancel_timer(CallTimerKind::Ring);
        actor
            .call
            .register_attempt(
                2,
                "callee-b".into(),
                &OutboundTarget::Endpoint {
                    endpoint_id: "3".into(),
                    endpoint_number: "1003".into(),
                },
                150,
            )
            .unwrap();
        actor.start_timer(CallTimerKind::Dial);
        actor.handle_provisional(event(180)).await.unwrap();
        assert!(actor.ring_timer.is_some());
        assert!(actor.dial_timer.is_none());
        let generation = actor.call.ring_timer_generation;
        assert!(generation > first_generation);
        actor.handle_provisional(event(183)).await.unwrap();
        assert_eq!(actor.call.ring_timer_generation, generation);
        actor
            .handle_timer(CallTimerEvent {
                kind: CallTimerKind::Ring,
                call_id: "call-a".into(),
                generation,
            })
            .await
            .unwrap();
        assert!(actor.call.is_terminating());
        assert_eq!(actor.call.hangup_cause, Some(HangupCause::NoUserResponse));
        assert!(actor.cleanup_timer.is_some());
    });
}

#[test]
fn late_provisional_and_487_preserve_termination_and_cause() {
    with_state(async |state| {
        let (mut actor, _reader) = actor(state).await;
        actor.call.begin_terminating(HangupCause::OriginatorCancel);
        actor.call.last_status = Some(480);
        actor.retry_after_cleanup = Some(503);
        actor.start_timer(CallTimerKind::Cleanup);
        actor.handle_provisional(event(180)).await.unwrap();
        actor.handle_provisional(event(183)).await.unwrap();
        assert!(actor.call.is_terminating());
        assert!(actor.ring_timer.is_none());
        actor.handle_failed(event(487)).await.unwrap();
        assert_eq!(actor.call.hangup_cause, Some(HangupCause::OriginatorCancel));
        assert_eq!(actor.call.last_status, Some(480));
        actor.retry_after_cleanup = None;
        let generation = actor.call.cleanup_timer_generation;
        actor
            .handle_timer(CallTimerEvent {
                kind: CallTimerKind::Cleanup,
                call_id: "call-a".into(),
                generation,
            })
            .await
            .unwrap();
        assert!(actor.finalization_started);
    });
}

#[test]
fn provisional_during_answer_and_trying_keep_existing_deadlines() {
    with_state(async |state| {
        let (mut actor, _reader) = actor(state).await;
        actor.start_timer(CallTimerKind::Dial);
        actor.handle_provisional(event(100)).await.unwrap();
        assert!(actor.dial_timer.is_some());
        actor.call.state = CallState::Answering;
        actor.answer_in_progress = true;
        actor.handle_provisional(event(180)).await.unwrap();
        assert_eq!(actor.call.state, CallState::Answering);
        assert!(actor.ring_timer.is_none());
    });
}

#[test]
fn shutdown_emits_reject_cancel_or_bye_then_cleanup() {
    with_state(async |state| {
        for answered in [false, true] {
            let (mut actor, mut reader) = actor(state.clone()).await;
            if answered {
                actor.call.mark_answered(200, 1).unwrap();
            }
            actor.begin_shutdown().unwrap();
            assert_eq!(actor.call.hangup_cause, Some(HangupCause::SystemShutdown));
            assert!(!actor.call.caller_terminated);
            assert!(!actor.finalization_started);
            assert!(actor.cleanup_timer.is_some());
            let first: RuntimeEnvelope = timeout(
                Duration::from_secs(1),
                voipswitch_core::ipc::frame::read_json_frame(&mut reader),
            )
            .await
            .unwrap()
            .unwrap();
            let second: RuntimeEnvelope = timeout(
                Duration::from_secs(1),
                voipswitch_core::ipc::frame::read_json_frame(&mut reader),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                first.r#type,
                if answered {
                    "HangupDialog"
                } else {
                    "RejectInboundInvite"
                }
            );
            assert_eq!(
                second.r#type,
                if answered {
                    "HangupDialog"
                } else {
                    "CancelOutbound"
                }
            );
        }
    });
}
