mod writer;

use crate::app::{
    AdapterReady, AppState, RegistrationChanged, TrunkHealthChanged, TrunkRegistrationChanged,
};
use crate::config_service::RuntimeConfig;
use crate::data_store::CallTraceMessage;
use crate::runtime::call::attempt::{
    AttemptRegistrar, AttemptRegistration, AttemptRegistrationResult,
};
use crate::runtime::call::dtmf_operation::{
    DtmfOperationService, DtmfOperationSource, DtmfRuntimeOperation,
};
use crate::runtime::call::event::{
    CallActionAck, CallLegEvent, DtmfInfoSendResult, InboundInviteOffered, SipDtmfReceived,
    is_call_leg_event,
};
use crate::runtime::call::handoff::{
    CoordinatorHandoffReady, CoordinatorHandoffRequest, HANDOFF_READY_CAPACITY,
    await_actor_readiness, recover_actors,
};
use crate::runtime::call::session::{ControlMessage, CriticalControlDispatcher, SessionManager};
use crate::runtime::call::timer::CallTimeouts;
use crate::runtime::call::{
    CallActorServices, CreatedCall, DigitCollectionSpec, SessionActor, reject_inbound_invite,
};
use crate::runtime::media::MediaPlaneManager;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, info, warn};
use voipswitch_core::analysis::AnalysisRegistry;
use voipswitch_core::ipc::frame::{read_json_frame, write_json_frame};
use voipswitch_core::types::ids::{
    BusinessOperationId, CollectorId, DomainId, EndpointId, SessionId,
};
use voipswitch_core::types::time::unix_timestamp_ms;

pub(crate) use writer::AdapterRuntimeWriter;
use writer::spawn_adapter_runtime_writer;

const CALL_ADMISSION_CAPACITY: usize = 256;
const RUNTIME_INBOUND_QUEUE_CAPACITY: usize = 50_000;

struct CallCreationResult {
    adapter_call_leg_id: String,
    result: Result<Option<CreatedCall>>,
}

fn spawn_adapter_runtime_reader<R>(
    mut reader: R,
    tx: mpsc::Sender<RuntimeEnvelope>,
) -> tokio::task::JoinHandle<Result<()>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            let frame = read_json_frame(&mut reader).await?;
            if tx.send(frame).await.is_err() {
                return Ok(());
            }
        }
    })
}

pub async fn run_adapter_runtime_socket(
    state: AppState,
    analysis: AnalysisRegistry,
    media_plane: MediaPlaneManager,
    path: impl AsRef<std::path::Path>,
) -> Result<()> {
    let path = path.as_ref();
    let listener = bind_unix_listener(path)
        .await
        .with_context(|| format!("bind adapter runtime socket {}", path.display()))?;
    info!(socket = %path.display(), "adapter runtime socket listening");

    loop {
        let (stream, _) = listener.accept().await.context("accept adapter client")?;
        let state = state.clone();
        let analysis = analysis.clone();
        let media_plane = media_plane.clone();
        tokio::spawn(async move {
            state.increment_adapter_clients();
            let result = handle_adapter_client(stream, state.clone(), analysis, media_plane).await;
            state.decrement_adapter_clients();
            if state.adapter_clients() == 0 {
                state.mark_registration_mirror_ready(false);
                state.mark_trunk_runtime_mirror_ready(false);
                state.clear_active_calls();
                state.clear_adapter_runtime();
            }
            if let Err(err) = result {
                debug!(error = %err, "adapter client disconnected");
            }
        });
    }
}

async fn handle_adapter_client(
    mut stream: UnixStream,
    state: AppState,
    analysis: AnalysisRegistry,
    media_plane: MediaPlaneManager,
) -> Result<()> {
    let mut config_updates = state.config().subscribe();
    let hello: RuntimeEnvelope = read_json_frame(&mut stream)
        .await
        .context("read adapter hello")?;
    if hello.r#type != "AdapterHello" {
        let response = RuntimeEnvelope::new(
            "core",
            "Error",
            hello.request_id.clone(),
            json!({
                "code": "expected_adapter_hello",
                "message": "first frame must be AdapterHello"
            }),
        );
        write_json_frame(&mut stream, &response).await?;
        return Ok(());
    }

    info!(body = %hello.body, "adapter connected");
    let ack = RuntimeEnvelope::new(
        "core",
        "CoreHelloAck",
        hello.request_id.clone(),
        json!({
            "core_instance_id": state.config().snapshot().system.instance_id,
            "core_session_id": format!(
                "{}-{}",
                state.config().snapshot().system.instance_id,
                state.started_at_ms()
            ),
            "accepted": true,
            "protocol_version": 1,
            "heartbeat_interval_ms": 5000,
            "write_timeout_ms": 100
        }),
    );
    write_json_frame(&mut stream, &ack).await?;

    let (reader, writer_half) = stream.into_split();
    let (runtime_writer, mut writer_task) = spawn_adapter_runtime_writer(writer_half);
    let (inbound_tx, mut inbound_rx) =
        mpsc::channel::<RuntimeEnvelope>(RUNTIME_INBOUND_QUEUE_CAPACITY);
    let mut reader_task = spawn_adapter_runtime_reader(reader, inbound_tx);
    let initial = config_updates.borrow_and_update().clone();
    let mut last_sent_revision = send_runtime_view(&runtime_writer, &initial).await?;
    let mut session_manager = SessionManager::default();
    let dtmf_operations = state.dtmf_operations();
    let (dtmf_runtime_generation, mut dtmf_operation_rx) = dtmf_operations.attach_runtime();
    let coordinator_handoffs = state.coordinator_handoffs();
    let (handoff_runtime_generation, mut handoff_request_rx) =
        coordinator_handoffs.attach_runtime();
    let (handoff_ready_tx, mut handoff_ready_rx) =
        tokio::sync::mpsc::channel::<CoordinatorHandoffReady>(HANDOFF_READY_CAPACITY);
    let handoff_permits = Arc::new(Semaphore::new(HANDOFF_READY_CAPACITY));
    let (finished_tx, mut finished_rx) = tokio::sync::mpsc::channel::<String>(4096);
    let (control_dispatcher, control_dispatcher_task) = CriticalControlDispatcher::spawn();
    let (attempt_registrar, mut attempt_rx) = AttemptRegistrar::new();
    let call_timeouts = CallTimeouts::default();
    let (creation_tx, mut creation_rx) =
        tokio::sync::mpsc::channel::<CallCreationResult>(CALL_ADMISSION_CAPACITY);
    let admission_permits = Arc::new(Semaphore::new(CALL_ADMISSION_CAPACITY));
    let mut inflight_invites = HashSet::new();

    let loop_result = async {
        loop {
            tokio::select! {
                frame = inbound_rx.recv() => {
                    let frame = frame.context("adapter runtime reader stopped")?;
                    if is_call_leg_event(&frame.r#type) {
                        if frame.r#type == "InboundInviteOffered" {
                            let event: InboundInviteOffered = serde_json::from_value(frame.body.clone())
                                .context("decode InboundInviteOffered")?;
                            let adapter_call_leg_id = event.adapter_call_leg_id.clone();
                            if !inflight_invites.insert(adapter_call_leg_id.clone()) {
                                debug!(adapter_call_leg_id, "duplicate in-flight inbound invite ignored");
                                continue;
                            }
                            let permit = match admission_permits.clone().try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    inflight_invites.remove(&adapter_call_leg_id);
                                    let writer = runtime_writer.clone();
                                    tokio::spawn(async move {
                                        let _ = reject_inbound_invite(&writer, &event, 503).await;
                                    });
                                    continue;
                                }
                            };
                            let services = CallActorServices {
                                state: state.clone(),
                                analysis: analysis.clone(),
                                writer: runtime_writer.clone(),
                                media_plane: media_plane.clone(),
                                timeouts: call_timeouts,
                                finished_tx: finished_tx.clone(),
                                control_dispatcher: control_dispatcher.clone(),
                                attempt_registrar: attempt_registrar.clone(),
                            };
                            let creation_tx = creation_tx.clone();
                            tokio::spawn(async move {
                                let result = SessionActor::create_call(event, services).await;
                                drop(permit);
                                let _ = creation_tx
                                    .send(CallCreationResult {
                                        adapter_call_leg_id,
                                        result,
                                    })
                                    .await;
                            });
                        } else {
                            let event: CallLegEvent = serde_json::from_value(frame.body.clone())
                                .with_context(|| format!("decode {}", frame.r#type))?;
                            let target_session = event.session_id.clone().or_else(|| {
                                session_manager
                                    .lookup_by_adapter_leg(&event.adapter_call_leg_id)
                                    .map(str::to_string)
                            });
                            if let Some(session_id) = target_session
                                && let Some(handle) = session_manager.lookup_by_session(&session_id)
                                && let Err(error) = control_dispatcher
                                    .dispatch(
                                        &session_id,
                                        handle,
                                        ControlMessage::LegEvent(frame.r#type.clone(), event),
                                    )
                                    .await
                            {
                                debug!(session_id, ?error, "call leg event target unavailable");
                            }
                        }
                        continue;
                    }
                    match frame.r#type.as_str() {
                        "SipDtmfReceived" => {
                            let received: SipDtmfReceived = match serde_json::from_value(frame.body) {
                                Ok(received) => received,
                                Err(error) => {
                                    warn!(error = %error, "invalid SIP INFO DTMF frame ignored");
                                    continue;
                                }
                            };
                            let session_id = received.session_id.clone();
                            let adapter_call_leg_id = received.adapter_call_leg_id.clone();
                            let leg_event_seq = received.leg_event_seq;
                            let Some(event) = received.into_digit_event() else {
                                warn!(session_id, adapter_call_leg_id, "unsupported SIP INFO DTMF transport ignored");
                                continue;
                            };
                            if let Some(handle) = session_manager.lookup_by_session(&session_id)
                                && let Err(error) = control_dispatcher
                                    .dispatch(
                                        &session_id,
                                        handle,
                                        ControlMessage::SipDtmfReceived(event),
                                    )
                                    .await
                            {
                                debug!(session_id, adapter_call_leg_id, leg_event_seq, ?error, "SIP INFO DTMF target unavailable");
                            } else if session_manager.lookup_by_session(&session_id).is_none() {
                                debug!(session_id, adapter_call_leg_id, leg_event_seq, "SIP INFO DTMF for destroyed session ignored");
                            }
                        }
                        "DtmfInfoSendResult" => {
                            let result: DtmfInfoSendResult = match serde_json::from_value(frame.body) {
                                Ok(result) => result,
                                Err(error) => {
                                    warn!(error = %error, "invalid DTMF INFO send result ignored");
                                    continue;
                                }
                            };
                            let call_id = result.call_id.clone();
                            if let Some(handle) = session_manager.coordinator_handle(&call_id)
                                && let Err(error) = control_dispatcher
                                    .dispatch_to(
                                        &call_id,
                                        handle.control_sender(),
                                        ControlMessage::DtmfInfoSendResult(result),
                                    )
                                    .await
                            {
                                debug!(call_id, ?error, "DTMF INFO result coordinator unavailable");
                            } else if session_manager.coordinator_handle(&call_id).is_none() {
                                debug!(call_id, "DTMF INFO result for destroyed call ignored");
                            }
                        }
                        "CallActionAck" => {
                            let ack = match serde_json::from_value::<CallActionAck>(frame.body) {
                                Ok(ack) => ack,
                                Err(error) => {
                                    debug!(error = %error, "unroutable pre-session call action ack ignored");
                                    continue;
                                }
                            };
                            let session_id = ack.session_id.clone();
                            if ack.accepted()
                                && ack.action_kind.starts_with("Originate")
                                && let Some(adapter_call_leg_id) = ack.adapter_call_leg_id()
                                && !session_manager.bind_adapter_leg(&session_id, adapter_call_leg_id)
                            {
                                debug!(session_id, adapter_call_leg_id, "originated adapter leg owner no longer exists");
                            }
                            let call_id = ack.call_id.clone();
                            let caller_result = session_manager.caller_session(&call_id)
                                == Some(session_id.as_str());
                            if caller_result {
                                if let Some(handle) = session_manager.coordinator_handle(&call_id)
                                    && let Err(error) = control_dispatcher
                                        .dispatch_to(
                                            &call_id,
                                            handle.control_sender(),
                                            ControlMessage::CallActionAck(ack),
                                        )
                                        .await
                                {
                                    debug!(call_id, ?error, "caller action ack coordinator unavailable");
                                } else if session_manager.coordinator_handle(&call_id).is_none() {
                                    debug!(call_id, "caller action ack for destroyed call ignored");
                                }
                            } else if let Some(handle) = session_manager.lookup_by_session(&session_id)
                                && let Err(error) = control_dispatcher
                                    .dispatch(
                                        &session_id,
                                        handle,
                                        ControlMessage::CallActionAck(ack),
                                    )
                                    .await
                            {
                                debug!(session_id, ?error, "leg action ack target unavailable");
                            } else if session_manager.lookup_by_session(&session_id).is_none() {
                                debug!(session_id, "leg action ack for destroyed session ignored");
                            }
                        }
                        "ApplyRuntimeViewAck" => {
                            let ack: ApplyRuntimeViewAck = serde_json::from_value(frame.body)
                                .context("decode ApplyRuntimeViewAck")?;
                            anyhow::ensure!(
                                ack.view_revision <= last_sent_revision,
                                "adapter acked unsent runtime revision {}",
                                ack.view_revision
                            );
                            match ack.status {
                                ApplyRuntimeViewStatus::Accepted | ApplyRuntimeViewStatus::Stale => {
                                    info!(
                                        view_revision = ack.view_revision,
                                        status = ?ack.status,
                                        applied_at_ms = ack.applied_at_ms,
                                        "adapter runtime view acknowledged"
                                    );
                                }
                                ApplyRuntimeViewStatus::Partial => {
                                    for error in &ack.errors {
                                        warn!(
                                            view_revision = ack.view_revision,
                                            object_type = %error.object_type,
                                            object_key = %error.object_key,
                                            code = %error.code,
                                            message = %error.message,
                                            "adapter runtime object rejected"
                                        );
                                    }
                                    warn!(
                                        view_revision = ack.view_revision,
                                        error_count = ack.errors.len(),
                                        "adapter partially applied runtime view"
                                    );
                                }
                                ApplyRuntimeViewStatus::Rejected => {
                                    anyhow::bail!(
                                        "adapter rejected runtime revision {}: {:?}",
                                        ack.view_revision,
                                        ack.errors
                                    );
                                }
                            }
                        }
                        "AdapterReady" => {
                            let ready: AdapterReady = serde_json::from_value(frame.body.clone())
                                .context("decode AdapterReady")?;
                            state.set_adapter_ready(ready);
                            state.mark_registration_mirror_ready(false);
                            state.mark_trunk_runtime_mirror_ready(true);
                            info!(body = %frame.body, "adapter ready");
                        }
                        "RegistrationSnapshotBegin" => {
                            state.begin_registration_snapshot();
                        }
                        "RegistrationChanged" => {
                            let event: RegistrationChanged =
                                serde_json::from_value(frame.body).context("decode RegistrationChanged")?;
                            state.apply_registration_changed(event);
                        }
                        "RegistrationSnapshotEnd" => {
                            state.mark_registration_mirror_ready(true);
                        }
                        "TrunkRegistrationChanged" => {
                            let event: TrunkRegistrationChanged = serde_json::from_value(frame.body)
                                .context("decode TrunkRegistrationChanged")?;
                            state.apply_trunk_registration_changed(event);
                        }
                        "TrunkHealthChanged" => {
                            let event: TrunkHealthChanged = serde_json::from_value(frame.body)
                                .context("decode TrunkHealthChanged")?;
                            state.apply_trunk_health_changed(event);
                        }
                        "SipTraceObserved" => {
                            let message: CallTraceMessage = serde_json::from_value(frame.body)
                                .context("decode SipTraceObserved")?;
                            state.record_call_trace(message);
                        }
                        _ => {
                            debug!(frame_type = %frame.r#type, kind = %frame.kind, "runtime frame received");
                        }
                    }
                }
                changed = config_updates.changed() => {
                    changed.context("runtime config update channel closed")?;
                    let config = config_updates.borrow_and_update().clone();
                    last_sent_revision = send_runtime_view(&runtime_writer, &config).await?;
                }
                finished_call = finished_rx.recv() => {
                    if let Some(call_id) = finished_call {
                        session_manager.destroy_call(&call_id);
                    }
                }
                created = creation_rx.recv() => {
                    let Some(created) = created else {
                        anyhow::bail!("call creation result channel closed");
                    };
                    inflight_invites.remove(&created.adapter_call_leg_id);
                    match created.result {
                        Ok(Some(created)) => {
                            session_manager.register_caller(
                                &created.caller_session_id,
                                &created.call_id,
                                &created.caller_adapter_leg_id,
                                created.caller_handle.clone(),
                                created.coordinator_handle.clone(),
                            );
                            session_manager.register_callee(
                                &created.callee_session_id,
                                &created.call_id,
                                created.callee_handle.clone(),
                            );
                            tokio::spawn(created.callee_actor.run());
                            tokio::spawn(created.caller_actor.run());
                        }
                        Ok(None) => {}
                        Err(error) => {
                            warn!(
                                adapter_call_leg_id = created.adapter_call_leg_id,
                                error = %error,
                                "call admission failed"
                            );
                        }
                    }
                }
                registration = attempt_rx.recv() => {
                    let Some(registration) = registration else {
                        anyhow::bail!("attempt registration channel closed");
                    };
                    handle_attempt_registration(
                        registration,
                        &mut session_manager,
                        &control_dispatcher,
                    ).await;
                }
                operation = dtmf_operation_rx.recv() => {
                    let Some(operation) = operation else {
                        anyhow::bail!("DTMF operation channel closed");
                    };
                    handle_dtmf_runtime_operation(
                        operation,
                        &session_manager,
                        dtmf_operations.clone(),
                    );
                }
                request = handoff_request_rx.recv() => {
                    let Some(request) = request else {
                        anyhow::bail!("coordinator handoff request channel closed");
                    };
                    begin_coordinator_handoff(
                        request,
                        &mut session_manager,
                        &control_dispatcher,
                        handoff_ready_tx.clone(),
                        handoff_permits.clone(),
                    ).await;
                }
                ready = handoff_ready_rx.recv() => {
                    let Some(ready) = ready else {
                        anyhow::bail!("coordinator handoff ready channel closed");
                    };
                    complete_coordinator_handoff(ready, &mut session_manager);
                }
                writer_result = &mut writer_task => {
                    return writer_result
                        .context("join adapter runtime writer")?
                        .context("adapter runtime writer stopped");
                }
                reader_result = &mut reader_task => {
                    return reader_result
                        .context("join adapter runtime reader")?
                        .context("adapter runtime reader stopped");
                }
            }
        }
    }
    .await;

    for (session_id, handle) in session_manager.handles() {
        let _ = control_dispatcher
            .dispatch(session_id, handle, ControlMessage::Shutdown)
            .await;
    }
    drop(control_dispatcher);
    let _ = control_dispatcher_task.await;
    drop(runtime_writer);
    if !writer_task.is_finished() {
        writer_task.abort();
        let _ = writer_task.await;
    }
    if !reader_task.is_finished() {
        reader_task.abort();
        let _ = reader_task.await;
    }
    dtmf_operations.detach_runtime(dtmf_runtime_generation);
    coordinator_handoffs.detach_runtime(handoff_runtime_generation);
    loop_result
}

async fn begin_coordinator_handoff(
    request: CoordinatorHandoffRequest,
    session_manager: &mut SessionManager,
    dispatcher: &CriticalControlDispatcher,
    ready_tx: tokio::sync::mpsc::Sender<CoordinatorHandoffReady>,
    permits: Arc<Semaphore>,
) {
    let CoordinatorHandoffRequest {
        call_id,
        target_session_id,
        reply,
    } = request;
    let permit = match permits.try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            let _ = reply.send(Err("coordinator_handoff_capacity_exhausted".to_string()));
            return;
        }
    };
    let Some(source) = session_manager.coordinator_identity(&call_id) else {
        let _ = reply.send(Err("coordinator_handoff_call_not_found".to_string()));
        return;
    };
    let Some(coordinator) = session_manager.coordinator_handle(&call_id).cloned() else {
        let _ = reply.send(Err("coordinator_handoff_source_unavailable".to_string()));
        return;
    };
    let Some(target) = session_manager
        .lookup_by_session(&target_session_id)
        .cloned()
    else {
        let _ = reply.send(Err("coordinator_handoff_target_unavailable".to_string()));
        return;
    };
    let token = match session_manager.prepare_coordinator_handoff(
        &call_id,
        &source.session_id,
        source.generation,
        &target_session_id,
    ) {
        Ok(token) => token,
        Err(error) => {
            let _ = reply.send(Err(format!("coordinator_handoff_prepare_failed:{error:?}")));
            return;
        }
    };
    let (source_reply, source_ready) = tokio::sync::oneshot::channel();
    let (target_reply, target_ready) = tokio::sync::oneshot::channel();
    let source_result = dispatcher
        .dispatch_to(
            &call_id,
            coordinator.control_sender(),
            ControlMessage::PrepareCoordinatorHandoff {
                token: token.clone(),
                reply: source_reply,
            },
        )
        .await;
    let target_result = dispatcher
        .dispatch(
            &target_session_id,
            &target,
            ControlMessage::PrepareCoordinatorTarget {
                token: token.clone(),
                reply: target_reply,
            },
        )
        .await;
    info!(
        call_id,
        source_session_id = source.session_id,
        target_session_id,
        source_generation = source.generation,
        target_generation = token.target_generation,
        source_dispatched = source_result.is_ok(),
        target_dispatched = target_result.is_ok(),
        "coordinator handoff prepared"
    );
    await_actor_readiness(token, source_ready, target_ready, reply, ready_tx, permit);
}

fn complete_coordinator_handoff(
    ready: CoordinatorHandoffReady,
    session_manager: &mut SessionManager,
) {
    let CoordinatorHandoffReady {
        token,
        source,
        target,
        reply,
        _permit: _,
    } = ready;
    if source.is_err() || target.is_err() {
        let reason = source
            .as_ref()
            .err()
            .or_else(|| target.as_ref().err())
            .cloned()
            .unwrap_or_else(|| "coordinator_handoff_not_ready".to_string());
        let _ = session_manager.abort_coordinator_handoff(&token);
        recover_actors(source, target);
        warn!(
            call_id = token.call_id,
            source_session_id = token.source_session_id,
            target_session_id = token.target_session_id,
            reason,
            "coordinator handoff aborted before commit"
        );
        let _ = reply.send(Err(reason));
        return;
    }
    let package = source.expect("source readiness checked");
    let target = target.expect("target readiness checked");
    if let Err(reason) = package.target_ready(&target) {
        let _ = session_manager.abort_coordinator_handoff(&token);
        recover_actors(Ok(package), Ok(target));
        let _ = reply.send(Err(reason));
        return;
    }
    let committed = match session_manager.commit_coordinator_handoff(&token) {
        Ok(committed) => committed,
        Err(error) => {
            let reason = format!("coordinator_handoff_commit_failed:{error:?}");
            let _ = session_manager.abort_coordinator_handoff(&token);
            recover_actors(Ok(package), Ok(target));
            let _ = reply.send(Err(reason));
            return;
        }
    };
    match package.install_on(target, &committed) {
        Ok(actor) => {
            session_manager.unregister_session(&token.source_session_id);
            tokio::spawn(actor.run());
            info!(
                call_id = token.call_id,
                source_session_id = token.source_session_id,
                target_session_id = committed.session_id,
                coordinator_generation = committed.generation,
                "coordinator handoff committed"
            );
            let _ = reply.send(Ok(committed));
        }
        Err(failure) => {
            let reason = failure.reason.clone();
            let rollback = session_manager.rollback_committed_coordinator_handoff(&token);
            recover_actors(Ok(failure.package), Ok(*failure.target));
            warn!(
                call_id = token.call_id,
                source_session_id = token.source_session_id,
                target_session_id = token.target_session_id,
                reason,
                rollback_ok = rollback.is_ok(),
                "coordinator handoff install failed; previous owner restored"
            );
            let _ = reply.send(Err(reason));
        }
    }
}

fn handle_dtmf_runtime_operation(
    operation: DtmfRuntimeOperation,
    session_manager: &SessionManager,
    service: DtmfOperationService,
) {
    match operation {
        DtmfRuntimeOperation::Start { operation_id, spec } => {
            let Some(_coordinator_identity) = session_manager.coordinator_identity(&spec.call_id)
            else {
                service.fail(&operation_id, "call_not_found");
                return;
            };
            let source_session_id = match spec.source {
                DtmfOperationSource::Caller => {
                    let Some(caller_session_id) = session_manager.caller_session(&spec.call_id)
                    else {
                        service.fail(&operation_id, "caller_session_not_found");
                        return;
                    };
                    caller_session_id
                }
                DtmfOperationSource::Callee => {
                    let Some(peer_session_id) = session_manager.peer_session(&spec.call_id) else {
                        service.fail(&operation_id, "current_callee_not_found");
                        return;
                    };
                    peer_session_id
                }
            };
            let Some(coordinator) = session_manager.coordinator_handle(&spec.call_id).cloned()
            else {
                service.fail(&operation_id, "call_coordinator_not_found");
                return;
            };
            service.mark_acquiring(&operation_id);
            let collection = DigitCollectionSpec {
                collector_id: CollectorId::from(operation_id.clone()),
                owner: BusinessOperationId::from(operation_id.clone()),
                source_session_id: SessionId::from(source_session_id),
                mode: spec.mode.into(),
                allowed: spec.allowed,
                min_digits: spec.min_digits,
                max_digits: spec.max_digits,
                terminators: spec.terminators,
                first_digit_timeout: spec.first_digit_timeout,
                inter_digit_timeout: spec.inter_digit_timeout,
                overall_timeout: spec.overall_timeout,
            };
            let receivers = match coordinator.start_digit_collection(collection) {
                Ok(receivers) => receivers,
                Err(error) => {
                    service.fail(&operation_id, error);
                    return;
                }
            };
            tokio::spawn(async move {
                let ready_error = match receivers.ready.await {
                    Ok(Ok(ready)) => {
                        service.mark_ready(&operation_id, ready.media_generation);
                        None
                    }
                    Ok(Err(error)) => Some(error),
                    Err(_) => Some("digit_collection_ready_channel_closed".to_string()),
                };
                match receivers.result.await {
                    Ok(outcome) => service.complete(&operation_id, outcome),
                    Err(_) => service.fail(
                        &operation_id,
                        ready_error.unwrap_or_else(|| {
                            "digit_collection_result_channel_closed".to_string()
                        }),
                    ),
                }
            });
        }
        DtmfRuntimeOperation::Cancel {
            operation_id,
            call_id,
        } => {
            let Some(_coordinator_identity) = session_manager.coordinator_identity(&call_id) else {
                service.fail(&operation_id, "call_not_found");
                return;
            };
            let Some(coordinator) = session_manager.coordinator_handle(&call_id) else {
                service.fail(&operation_id, "call_coordinator_not_found");
                return;
            };
            let reply = match coordinator
                .cancel_digit_collection(CollectorId::from(operation_id.clone()))
            {
                Ok(reply) => reply,
                Err(error) => {
                    service.fail(&operation_id, error);
                    return;
                }
            };
            tokio::spawn(async move {
                match reply.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => service.fail(&operation_id, error),
                    Err(_) => service.fail(&operation_id, "digit_collection_cancel_channel_closed"),
                }
            });
        }
    }
}

async fn handle_attempt_registration(
    registration: AttemptRegistration,
    session_manager: &mut SessionManager,
    dispatcher: &CriticalControlDispatcher,
) {
    let AttemptRegistration {
        call_id,
        coordinator_session_id,
        coordinator_generation,
        previous_session_id,
        attempt_seq,
        session_id,
        handle,
        actor,
    } = registration;
    let coordinator_handle = session_manager.coordinator_handle(&call_id).cloned();
    let valid = session_manager
        .coordinator_identity(&call_id)
        .is_some_and(|identity| {
            identity.session_id == coordinator_session_id
                && identity.generation == coordinator_generation
        })
        && coordinator_handle.is_some();
    let result = if valid {
        if let Some(previous) = session_manager.unregister_session(&previous_session_id) {
            let _ = dispatcher
                .dispatch(&previous_session_id, &previous, ControlMessage::Shutdown)
                .await;
        }
        session_manager.register_callee(&session_id, &call_id, handle.clone());
        tokio::spawn(actor.run());
        info!(
            call_id,
            attempt_seq, previous_session_id, session_id, "callee attempt registered"
        );
        AttemptRegistrationResult::Registered {
            attempt_seq,
            session_id: session_id.clone(),
        }
    } else {
        AttemptRegistrationResult::Rejected {
            attempt_seq,
            session_id: session_id.clone(),
            reason: "call coordinator no longer active".to_string(),
        }
    };

    let Some(coordinator_handle) = coordinator_handle else {
        session_manager.unregister_session(&session_id);
        return;
    };
    if dispatcher
        .dispatch_to(
            &call_id,
            coordinator_handle.control_sender(),
            ControlMessage::AttemptRegistrationResult(result),
        )
        .await
        .is_err()
        && let Some(new_attempt) = session_manager.unregister_session(&session_id)
    {
        let _ = dispatcher
            .dispatch(&session_id, &new_attempt, ControlMessage::Shutdown)
            .await;
    }
}

async fn send_runtime_view(writer: &AdapterRuntimeWriter, config: &RuntimeConfig) -> Result<u64> {
    let view = build_sip_runtime_view(config);
    let revision = view.view_revision;
    let request_id = Some(format!("runtime-view-{revision}-{}", unix_timestamp_ms()));
    let apply_view = RuntimeEnvelope::new(
        "command",
        "ApplyRuntimeView",
        request_id,
        serde_json::to_value(view).context("encode SipRuntimeView")?,
    );
    writer.send(apply_view).await?;
    Ok(revision)
}

fn build_sip_runtime_view(config: &RuntimeConfig) -> SipRuntimeView {
    let mut domains = Vec::new();
    let mut endpoints = Vec::new();
    let mut peer_trunks = Vec::new();
    let mut reg_trunks = Vec::new();
    let mut reg_accounts = Vec::new();

    for domain in config.domains.values().filter(|domain| domain.enabled) {
        let host_aliases = Vec::new();
        domains.push(SipDomainView {
            domain_id: domain.domain_id.clone(),
            realm: domain.realm.clone(),
            host_aliases,
            config_hash: config_hash(&(&domain.realm, Vec::<String>::new())),
        });
        endpoints.extend(
            domain
                .extensions
                .iter()
                .filter(|ext| ext.enabled)
                .map(|ext| SipEndpointAuthView {
                    domain_id: domain.domain_id.clone(),
                    endpoint_id: EndpointId::new(ext.id.to_string()),
                    number: ext.number.clone(),
                    auth_user: ext.auth_user.clone(),
                    password: ext.password.clone(),
                    config_hash: config_hash(&(&ext.number, &ext.auth_user, &ext.password)),
                }),
        );
        peer_trunks.extend(domain.peer_trunks.iter().map(|trunk| SipPeerTrunkView {
            domain_id: domain.domain_id.clone(),
            peer_trunk_id: trunk.id,
            server_host: trunk.server_host.clone(),
            server_port: trunk.server_port,
            outbound_proxy_host: trunk.outbound_proxy_host.clone(),
            outbound_proxy_port: trunk.outbound_proxy_port,
            transport: trunk.transport.clone(),
            keep_alive_seconds: trunk.keep_alive_seconds,
            enabled: trunk.enabled,
            config_hash: config_hash(&(
                &trunk.server_host,
                trunk.server_port,
                &trunk.outbound_proxy_host,
                trunk.outbound_proxy_port,
                &trunk.transport,
                trunk.keep_alive_seconds,
                trunk.enabled,
            )),
        }));
        reg_trunks.extend(domain.reg_trunks.iter().map(|trunk| SipRegisterTrunkView {
            domain_id: domain.domain_id.clone(),
            reg_trunk_id: trunk.id,
            server_host: trunk.server_host.clone(),
            server_port: trunk.server_port,
            outbound_proxy_host: trunk.outbound_proxy_host.clone(),
            outbound_proxy_port: trunk.outbound_proxy_port,
            transport: trunk.transport.clone(),
            keep_alive_seconds: trunk.keep_alive_seconds,
            requested_expires_seconds: trunk.requested_expires_seconds,
            enabled: trunk.enabled,
            config_hash: config_hash(&(
                &trunk.server_host,
                trunk.server_port,
                &trunk.outbound_proxy_host,
                trunk.outbound_proxy_port,
                &trunk.transport,
                trunk.keep_alive_seconds,
                trunk.requested_expires_seconds,
                trunk.enabled,
            )),
        }));
        reg_accounts.extend(
            domain
                .reg_accounts
                .iter()
                .map(|account| SipRegisterAccountView {
                    domain_id: domain.domain_id.clone(),
                    reg_trunk_id: account.reg_trunk_id,
                    reg_account_id: account.id,
                    auth_name: account.auth_name.clone(),
                    auth_pwd: account.auth_pwd.clone(),
                    enabled: account.enabled,
                    config_hash: config_hash(&(
                        account.reg_trunk_id,
                        &account.auth_name,
                        &account.auth_pwd,
                        account.enabled,
                    )),
                }),
        );
    }

    SipRuntimeView {
        view_revision: config.version,
        mode: RuntimeViewMode::Full,
        generated_at_ms: unix_timestamp_ms(),
        sip_port: config.sip_port(),
        log_level: config.log_level().to_string(),
        call_trace_enabled: config.call_trace_enabled(),
        transports: Vec::new(),
        profiles: Vec::new(),
        domains,
        endpoints,
        peer_trunks,
        reg_trunks,
        reg_accounts,
        policy: SipOverloadPolicy {
            reject_new_invite_when_overloaded: true,
        },
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SipRuntimeView {
    pub view_revision: u64,
    pub mode: RuntimeViewMode,
    pub generated_at_ms: u64,
    pub sip_port: u16,
    pub log_level: String,
    pub call_trace_enabled: bool,
    pub transports: Vec<Value>,
    pub profiles: Vec<Value>,
    pub domains: Vec<SipDomainView>,
    pub endpoints: Vec<SipEndpointAuthView>,
    pub peer_trunks: Vec<SipPeerTrunkView>,
    pub reg_trunks: Vec<SipRegisterTrunkView>,
    pub reg_accounts: Vec<SipRegisterAccountView>,
    pub policy: SipOverloadPolicy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeViewMode {
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SipDomainView {
    pub domain_id: DomainId,
    pub realm: String,
    pub host_aliases: Vec<String>,
    pub config_hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SipEndpointAuthView {
    pub domain_id: DomainId,
    pub endpoint_id: EndpointId,
    pub number: String,
    pub auth_user: String,
    pub password: String,
    pub config_hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SipPeerTrunkView {
    pub domain_id: DomainId,
    pub peer_trunk_id: u64,
    pub server_host: String,
    pub server_port: u16,
    pub outbound_proxy_host: Option<String>,
    pub outbound_proxy_port: Option<u16>,
    pub transport: String,
    pub keep_alive_seconds: u32,
    pub enabled: bool,
    pub config_hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SipRegisterTrunkView {
    pub domain_id: DomainId,
    pub reg_trunk_id: u64,
    pub server_host: String,
    pub server_port: u16,
    pub outbound_proxy_host: Option<String>,
    pub outbound_proxy_port: Option<u16>,
    pub transport: String,
    pub keep_alive_seconds: u32,
    pub requested_expires_seconds: u32,
    pub enabled: bool,
    pub config_hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SipRegisterAccountView {
    pub domain_id: DomainId,
    pub reg_trunk_id: u64,
    pub reg_account_id: u64,
    pub auth_name: String,
    pub auth_pwd: String,
    pub enabled: bool,
    pub config_hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SipOverloadPolicy {
    pub reject_new_invite_when_overloaded: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApplyRuntimeViewAck {
    pub view_revision: u64,
    pub status: ApplyRuntimeViewStatus,
    pub applied_at_ms: u64,
    #[serde(default)]
    pub errors: Vec<RuntimeViewObjectError>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplyRuntimeViewStatus {
    Accepted,
    Partial,
    Rejected,
    Stale,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeViewObjectError {
    pub object_type: String,
    pub object_key: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeEnvelope {
    pub version: u16,
    pub kind: String,
    #[serde(rename = "type")]
    pub r#type: String,
    pub request_id: Option<String>,
    pub domain_id: Option<String>,
    pub timestamp_ms: u64,
    pub body: Value,
}

impl RuntimeEnvelope {
    pub fn new(
        kind: impl Into<String>,
        frame_type: impl Into<String>,
        request_id: Option<String>,
        body: Value,
    ) -> Self {
        Self {
            version: 1,
            kind: kind.into(),
            r#type: frame_type.into(),
            request_id,
            domain_id: None,
            timestamp_ms: unix_timestamp_ms(),
            body,
        }
    }
}

fn config_hash(value: &impl Serialize) -> u64 {
    let bytes = serde_json::to_vec(value).expect("runtime config is serializable");
    bytes.into_iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    })
}

async fn bind_unix_listener(path: &std::path::Path) -> Result<UnixListener> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create socket dir {}", parent.display()))?;
    }

    match tokio::fs::remove_file(path).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            warn!(socket = %path.display(), error = %err, "failed to remove stale socket");
        }
    }

    UnixListener::bind(path).with_context(|| format!("bind {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_service::{DomainRuntimeConfig, SystemConfig};
    use crate::pbx::extension::model::ExtensionConfig;
    use crate::pbx::trunk::model::{PeerTrunkConfig, RegisterAccountConfig, RegisterTrunkConfig};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[tokio::test]
    async fn runtime_reader_buffers_framed_events_in_fifo_order() {
        assert_eq!(RUNTIME_INBOUND_QUEUE_CAPACITY, 50_000);

        let (mut client, server) = tokio::io::duplex(4096);
        let (tx, mut rx) = tokio::sync::mpsc::channel(RUNTIME_INBOUND_QUEUE_CAPACITY);
        let task = spawn_adapter_runtime_reader(server, tx);
        let first = RuntimeEnvelope::new("event", "First", Some("one".to_string()), json!({}));
        let second = RuntimeEnvelope::new("event", "Second", Some("two".to_string()), json!({}));

        write_json_frame(&mut client, &first).await.unwrap();
        write_json_frame(&mut client, &second).await.unwrap();

        assert_eq!(rx.recv().await.unwrap().r#type, "First");
        assert_eq!(rx.recv().await.unwrap().r#type, "Second");

        drop(client);
        assert!(task.await.unwrap().is_err());
    }

    #[test]
    fn runtime_view_uses_flat_full_wire_shape() {
        let view = build_sip_runtime_view(&runtime_config("endpoint-secret", "account-secret"));
        let value = serde_json::to_value(&view).unwrap();

        assert_eq!(value["view_revision"], 42);
        assert_eq!(value["mode"], "full");
        assert_eq!(value["domains"].as_array().unwrap().len(), 1);
        assert_eq!(value["endpoints"].as_array().unwrap().len(), 1);
        assert_eq!(value["peer_trunks"].as_array().unwrap().len(), 1);
        assert_eq!(value["reg_trunks"].as_array().unwrap().len(), 1);
        assert_eq!(value["reg_accounts"].as_array().unwrap().len(), 1);
        assert!(value["domains"][0].get("endpoints").is_none());
        assert_eq!(value["policy"]["reject_new_invite_when_overloaded"], true);
    }

    #[test]
    fn secret_changes_are_covered_by_object_hashes() {
        let original = build_sip_runtime_view(&runtime_config("endpoint-secret", "account-secret"));
        let changed =
            build_sip_runtime_view(&runtime_config("new-endpoint-secret", "new-account-secret"));

        assert_ne!(
            original.endpoints[0].config_hash,
            changed.endpoints[0].config_hash
        );
        assert_ne!(
            original.reg_accounts[0].config_hash,
            changed.reg_accounts[0].config_hash
        );
        assert_eq!(
            original.peer_trunks[0].config_hash,
            changed.peer_trunks[0].config_hash
        );
    }

    fn runtime_config(endpoint_password: &str, account_password: &str) -> RuntimeConfig {
        let domain_id = DomainId::from("domain-a");
        RuntimeConfig {
            system: SystemConfig {
                instance_id: "test".to_string(),
                data_dir: "/tmp/test".to_string(),
            },
            globals: BTreeMap::new(),
            domains: BTreeMap::from([(
                domain_id.clone(),
                Arc::new(DomainRuntimeConfig {
                    domain_id,
                    name: "Domain A".to_string(),
                    realm: "example.com".to_string(),
                    password: "domain-secret".to_string(),
                    remark: String::new(),
                    enabled: true,
                    extensions: vec![ExtensionConfig {
                        id: 1,
                        number: "1001".to_string(),
                        auth_user: "1001".to_string(),
                        password: endpoint_password.to_string(),
                        enabled: true,
                    }],
                    peer_trunks: vec![PeerTrunkConfig {
                        id: 1,
                        name: "peer".to_string(),
                        server_host: "peer.example.com".to_string(),
                        server_port: 5060,
                        outbound_proxy_host: None,
                        outbound_proxy_port: None,
                        transport: "udp".to_string(),
                        keep_alive_seconds: 60,
                        enabled: true,
                    }],
                    reg_trunks: vec![RegisterTrunkConfig {
                        id: 2,
                        name: "reg".to_string(),
                        server_host: "reg.example.com".to_string(),
                        server_port: 5060,
                        outbound_proxy_host: None,
                        outbound_proxy_port: None,
                        transport: "udp".to_string(),
                        keep_alive_seconds: 60,
                        requested_expires_seconds: 300,
                        enabled: true,
                    }],
                    reg_accounts: vec![RegisterAccountConfig {
                        id: 3,
                        reg_trunk_id: 2,
                        auth_name: "account".to_string(),
                        auth_pwd: account_password.to_string(),
                        enabled: true,
                    }],
                    inbound_routes: Vec::new(),
                    outbound_routes: Vec::new(),
                    recording_policies: Vec::new(),
                    ai_policies: Vec::new(),
                    version: 1,
                }),
            )]),
            version: 42,
        }
    }
}
