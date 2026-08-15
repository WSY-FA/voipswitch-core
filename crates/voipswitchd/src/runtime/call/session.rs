use crate::runtime::call::actor::{CalleeSessionActor, CoordinatorHandoffPackage};
use crate::runtime::call::attempt::AttemptRegistrationResult;
use crate::runtime::call::dtmf::{
    DigitCollectionOutcome, DigitCollectionReady, DigitCollectionSpec,
};
use crate::runtime::call::event::{
    ActionDeliveryFailed, CallActionAck, CallLegEvent, DtmfInfoSendResult,
};
use crate::runtime::call::media_action::MediaActionResult;
use serde_json::Value;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};
use voipswitch_core::dtmf::DigitEvent;

pub(crate) const CONTROL_MAILBOX_CAPACITY: usize = 64;
pub(crate) const EVENT_MAILBOX_CAPACITY: usize = 128;
pub(crate) const COORDINATOR_CONTROL_MAILBOX_CAPACITY: usize = 128;
pub(crate) const COORDINATOR_EVENT_MAILBOX_CAPACITY: usize = 128;
pub(crate) const CRITICAL_DISPATCHER_CAPACITY: usize = 50_000;

/// Lifecycle messages that must either reach the owning actor or encounter
/// explicit bounded backpressure.
pub(crate) enum ControlMessage {
    LegEvent(String, CallLegEvent),
    PeerLegEvent(String, CallLegEvent),
    PeerActionFailed(ActionDeliveryFailed),
    ActionDeliveryFailed(ActionDeliveryFailed),
    CallActionAck(CallActionAck),
    MediaActionResult(MediaActionResult),
    MediaDtmfObserved(DigitEvent),
    SipDtmfReceived(DigitEvent),
    PeerDigitEvent(DigitEvent),
    DtmfInfoSendResult(DtmfInfoSendResult),
    AttemptRegistrationResult(AttemptRegistrationResult),
    PrepareCoordinatorHandoff {
        token: CoordinatorHandoffToken,
        reply: oneshot::Sender<Result<CoordinatorHandoffPackage, String>>,
    },
    PrepareCoordinatorTarget {
        token: CoordinatorHandoffToken,
        reply: oneshot::Sender<Result<CalleeSessionActor, String>>,
    },
    CallFinalized,
    Shutdown,
}

/// Soft real-time notifications and queries. These never consume control
/// mailbox capacity and callers may reject them when the mailbox is full.
#[allow(dead_code)]
pub(crate) enum EventMessage {
    QuerySessionSummary(oneshot::Sender<Option<Value>>),
    StartDigitCollection {
        spec: Box<DigitCollectionSpec>,
        ready_reply: oneshot::Sender<Result<DigitCollectionReady, String>>,
        result_reply: oneshot::Sender<DigitCollectionOutcome>,
    },
    CancelDigitCollection {
        collector_id: voipswitch_core::types::ids::CollectorId,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

#[derive(Clone)]
pub(crate) struct SessionHandle {
    control_tx: mpsc::Sender<ControlMessage>,
    event_tx: mpsc::Sender<EventMessage>,
}

impl SessionHandle {
    pub(crate) fn new_pair() -> (
        Self,
        mpsc::Receiver<ControlMessage>,
        mpsc::Receiver<EventMessage>,
    ) {
        let (control_tx, control_rx) = mpsc::channel(CONTROL_MAILBOX_CAPACITY);
        let (event_tx, event_rx) = mpsc::channel(EVENT_MAILBOX_CAPACITY);
        (
            Self {
                control_tx,
                event_tx,
            },
            control_rx,
            event_rx,
        )
    }

    pub(crate) fn control_sender(&self) -> mpsc::Sender<ControlMessage> {
        self.control_tx.clone()
    }

    #[allow(dead_code)]
    pub(crate) fn start_digit_collection(
        &self,
        spec: DigitCollectionSpec,
    ) -> Result<DigitCollectionReceivers, &'static str> {
        let (ready_reply, ready) = oneshot::channel();
        let (result_reply, result) = oneshot::channel();
        self.event_tx
            .try_send(EventMessage::StartDigitCollection {
                spec: Box::new(spec),
                ready_reply,
                result_reply,
            })
            .map_err(map_event_submit_error)?;
        Ok(DigitCollectionReceivers { ready, result })
    }

    #[allow(dead_code)]
    pub(crate) fn cancel_digit_collection(
        &self,
        collector_id: voipswitch_core::types::ids::CollectorId,
    ) -> Result<oneshot::Receiver<Result<(), String>>, &'static str> {
        let (reply, receiver) = oneshot::channel();
        self.event_tx
            .try_send(EventMessage::CancelDigitCollection {
                collector_id,
                reply,
            })
            .map_err(map_event_submit_error)?;
        Ok(receiver)
    }

    #[allow(dead_code)]
    pub(crate) fn try_post_event(
        &self,
        message: EventMessage,
    ) -> Result<(), mpsc::error::TrySendError<EventMessage>> {
        self.event_tx.try_send(message)
    }
}

#[derive(Clone)]
pub(crate) struct CallCoordinatorHandle {
    call_id: String,
    control_tx: mpsc::Sender<ControlMessage>,
    event_tx: mpsc::Sender<EventMessage>,
}

pub(crate) struct CallCoordinatorMailbox {
    pub(crate) control_rx: mpsc::Receiver<ControlMessage>,
    pub(crate) event_rx: mpsc::Receiver<EventMessage>,
}

impl CallCoordinatorHandle {
    pub(crate) fn new_pair(call_id: impl Into<String>) -> (Self, CallCoordinatorMailbox) {
        let (control_tx, control_rx) = mpsc::channel(COORDINATOR_CONTROL_MAILBOX_CAPACITY);
        let (event_tx, event_rx) = mpsc::channel(COORDINATOR_EVENT_MAILBOX_CAPACITY);
        (
            Self {
                call_id: call_id.into(),
                control_tx,
                event_tx,
            },
            CallCoordinatorMailbox {
                control_rx,
                event_rx,
            },
        )
    }

    pub(crate) fn call_id(&self) -> &str {
        &self.call_id
    }

    pub(crate) fn control_sender(&self) -> mpsc::Sender<ControlMessage> {
        self.control_tx.clone()
    }

    pub(crate) fn weak_control_sender(&self) -> mpsc::WeakSender<ControlMessage> {
        self.control_tx.downgrade()
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.control_tx.is_closed() || self.event_tx.is_closed()
    }

    pub(crate) fn start_digit_collection(
        &self,
        spec: DigitCollectionSpec,
    ) -> Result<DigitCollectionReceivers, &'static str> {
        let (ready_reply, ready) = oneshot::channel();
        let (result_reply, result) = oneshot::channel();
        self.event_tx
            .try_send(EventMessage::StartDigitCollection {
                spec: Box::new(spec),
                ready_reply,
                result_reply,
            })
            .map_err(map_coordinator_event_submit_error)?;
        Ok(DigitCollectionReceivers { ready, result })
    }

    pub(crate) fn cancel_digit_collection(
        &self,
        collector_id: voipswitch_core::types::ids::CollectorId,
    ) -> Result<oneshot::Receiver<Result<(), String>>, &'static str> {
        let (reply, receiver) = oneshot::channel();
        self.event_tx
            .try_send(EventMessage::CancelDigitCollection {
                collector_id,
                reply,
            })
            .map_err(map_coordinator_event_submit_error)?;
        Ok(receiver)
    }
}

#[allow(dead_code)]
pub(crate) struct DigitCollectionReceivers {
    pub(crate) ready: oneshot::Receiver<Result<DigitCollectionReady, String>>,
    pub(crate) result: oneshot::Receiver<DigitCollectionOutcome>,
}

fn map_event_submit_error(error: mpsc::error::TrySendError<EventMessage>) -> &'static str {
    match error {
        mpsc::error::TrySendError::Full(_) => "session_event_mailbox_full",
        mpsc::error::TrySendError::Closed(_) => "session_gone",
    }
}

fn map_coordinator_event_submit_error(
    error: mpsc::error::TrySendError<EventMessage>,
) -> &'static str {
    match error {
        mpsc::error::TrySendError::Full(_) => "call_coordinator_event_mailbox_full",
        mpsc::error::TrySendError::Closed(_) => "call_coordinator_gone",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PostError {
    Gone,
    DispatcherClosed,
}

struct CriticalDispatch {
    session_id: String,
    target: mpsc::Sender<ControlMessage>,
    message: ControlMessage,
}

/// Bounded retry path for lifecycle messages. A full actor mailbox moves the
/// message into this queue; a full dispatcher queue backpressures the runtime
/// reader instead of allocating an unbounded pending list.
#[derive(Clone)]
pub(crate) struct CriticalControlDispatcher {
    tx: mpsc::Sender<CriticalDispatch>,
}

impl CriticalControlDispatcher {
    pub(crate) fn spawn() -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<CriticalDispatch>(CRITICAL_DISPATCHER_CAPACITY);
        let task = tokio::spawn(async move {
            while let Some(dispatch) = rx.recv().await {
                match dispatch.target.try_send(dispatch.message) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        debug!(
                            session_id = dispatch.session_id,
                            "critical control target no longer exists"
                        );
                    }
                    Err(mpsc::error::TrySendError::Full(message)) => {
                        warn!(
                            session_id = dispatch.session_id,
                            "session control mailbox full; dispatcher waiting for capacity"
                        );
                        if dispatch.target.send(message).await.is_err() {
                            debug!(
                                session_id = dispatch.session_id,
                                "critical control target closed while dispatcher waited"
                            );
                        }
                    }
                }
            }
        });
        (Self { tx }, task)
    }

    pub(crate) async fn dispatch(
        &self,
        session_id: &str,
        handle: &SessionHandle,
        message: ControlMessage,
    ) -> Result<(), PostError> {
        self.dispatch_to(session_id, handle.control_sender(), message)
            .await
    }

    pub(crate) async fn dispatch_to(
        &self,
        session_id: &str,
        target: mpsc::Sender<ControlMessage>,
        message: ControlMessage,
    ) -> Result<(), PostError> {
        if target.is_closed() {
            return Err(PostError::Gone);
        }
        self.tx
            .send(CriticalDispatch {
                session_id: session_id.to_string(),
                target,
                message,
            })
            .await
            .map_err(|_| PostError::DispatcherClosed)
    }
}

/// Owns actor indexes only. FSM state remains inside each session actor.
#[derive(Default)]
pub(crate) struct SessionManager {
    sessions: HashMap<String, SessionHandle>,
    session_calls: HashMap<String, String>,
    call_originators: HashMap<String, String>,
    call_coordinators: HashMap<String, CoordinatorRegistration>,
    pending_coordinator_handoffs: HashMap<String, CoordinatorHandoffToken>,
    leg_sessions: HashMap<String, String>,
}

#[derive(Clone)]
struct CoordinatorRegistration {
    session_id: String,
    generation: u64,
    handle: CallCoordinatorHandle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoordinatorIdentity {
    pub(crate) session_id: String,
    pub(crate) generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CoordinatorHandoffToken {
    pub(crate) call_id: String,
    pub(crate) source_session_id: String,
    pub(crate) target_session_id: String,
    pub(crate) source_generation: u64,
    pub(crate) target_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CoordinatorHandoffError {
    CallNotFound,
    HandoffInProgress,
    StaleOwner,
    InvalidTarget,
    GenerationExhausted,
    TokenMismatch,
}

impl SessionManager {
    pub(crate) fn register_caller(
        &mut self,
        session_id: &str,
        call_id: &str,
        adapter_call_leg_id: &str,
        handle: SessionHandle,
        coordinator: CallCoordinatorHandle,
    ) {
        debug_assert_eq!(coordinator.call_id(), call_id);
        self.sessions.insert(session_id.to_string(), handle);
        self.session_calls
            .insert(session_id.to_string(), call_id.to_string());
        self.call_originators
            .insert(call_id.to_string(), session_id.to_string());
        self.call_coordinators.insert(
            call_id.to_string(),
            CoordinatorRegistration {
                session_id: session_id.to_string(),
                generation: 1,
                handle: coordinator,
            },
        );
        self.leg_sessions
            .insert(adapter_call_leg_id.to_string(), session_id.to_string());
    }

    pub(crate) fn register_callee(
        &mut self,
        session_id: &str,
        call_id: &str,
        handle: SessionHandle,
    ) {
        self.sessions.insert(session_id.to_string(), handle);
        self.session_calls
            .insert(session_id.to_string(), call_id.to_string());
    }

    pub(crate) fn bind_adapter_leg(&mut self, session_id: &str, adapter_call_leg_id: &str) -> bool {
        if !self.sessions.contains_key(session_id) {
            return false;
        }
        self.leg_sessions
            .retain(|_, candidate| candidate != session_id);
        self.leg_sessions
            .insert(adapter_call_leg_id.to_string(), session_id.to_string());
        true
    }

    pub(crate) fn unregister_session(&mut self, session_id: &str) -> Option<SessionHandle> {
        self.pending_coordinator_handoffs.retain(|_, handoff| {
            handoff.source_session_id != session_id && handoff.target_session_id != session_id
        });
        self.session_calls.remove(session_id);
        self.leg_sessions
            .retain(|_, candidate| candidate != session_id);
        self.sessions.remove(session_id)
    }

    pub(crate) fn coordinator_session(&self, call_id: &str) -> Option<&str> {
        self.call_coordinators
            .get(call_id)
            .map(|registration| registration.session_id.as_str())
    }

    pub(crate) fn caller_session(&self, call_id: &str) -> Option<&str> {
        self.call_originators.get(call_id).map(String::as_str)
    }

    pub(crate) fn coordinator_identity(&self, call_id: &str) -> Option<CoordinatorIdentity> {
        self.call_coordinators
            .get(call_id)
            .map(|registration| CoordinatorIdentity {
                session_id: registration.session_id.clone(),
                generation: registration.generation,
            })
    }

    pub(crate) fn coordinator_handle(&self, call_id: &str) -> Option<&CallCoordinatorHandle> {
        self.call_coordinators
            .get(call_id)
            .filter(|registration| !registration.handle.is_closed())
            .map(|registration| &registration.handle)
    }

    pub(crate) fn prepare_coordinator_handoff(
        &mut self,
        call_id: &str,
        source_session_id: &str,
        source_generation: u64,
        target_session_id: &str,
    ) -> Result<CoordinatorHandoffToken, CoordinatorHandoffError> {
        if self.pending_coordinator_handoffs.contains_key(call_id) {
            return Err(CoordinatorHandoffError::HandoffInProgress);
        }
        let registration = self
            .call_coordinators
            .get(call_id)
            .ok_or(CoordinatorHandoffError::CallNotFound)?;
        if registration.session_id != source_session_id
            || registration.generation != source_generation
        {
            return Err(CoordinatorHandoffError::StaleOwner);
        }
        if target_session_id == source_session_id
            || !self.sessions.contains_key(target_session_id)
            || self
                .session_calls
                .get(target_session_id)
                .map(String::as_str)
                != Some(call_id)
        {
            return Err(CoordinatorHandoffError::InvalidTarget);
        }
        let target_generation = source_generation
            .checked_add(1)
            .ok_or(CoordinatorHandoffError::GenerationExhausted)?;
        let token = CoordinatorHandoffToken {
            call_id: call_id.to_string(),
            source_session_id: source_session_id.to_string(),
            target_session_id: target_session_id.to_string(),
            source_generation,
            target_generation,
        };
        self.pending_coordinator_handoffs
            .insert(call_id.to_string(), token.clone());
        Ok(token)
    }

    pub(crate) fn commit_coordinator_handoff(
        &mut self,
        token: &CoordinatorHandoffToken,
    ) -> Result<CoordinatorIdentity, CoordinatorHandoffError> {
        if self.pending_coordinator_handoffs.get(&token.call_id) != Some(token) {
            return Err(CoordinatorHandoffError::TokenMismatch);
        }
        let registration = self
            .call_coordinators
            .get_mut(&token.call_id)
            .ok_or(CoordinatorHandoffError::CallNotFound)?;
        if registration.session_id != token.source_session_id
            || registration.generation != token.source_generation
        {
            return Err(CoordinatorHandoffError::StaleOwner);
        }
        if !self.sessions.contains_key(&token.target_session_id)
            || self
                .session_calls
                .get(&token.target_session_id)
                .map(String::as_str)
                != Some(token.call_id.as_str())
        {
            return Err(CoordinatorHandoffError::InvalidTarget);
        }
        registration.session_id = token.target_session_id.clone();
        registration.generation = token.target_generation;
        self.pending_coordinator_handoffs.remove(&token.call_id);
        Ok(CoordinatorIdentity {
            session_id: registration.session_id.clone(),
            generation: registration.generation,
        })
    }

    pub(crate) fn abort_coordinator_handoff(
        &mut self,
        token: &CoordinatorHandoffToken,
    ) -> Result<(), CoordinatorHandoffError> {
        if self.pending_coordinator_handoffs.get(&token.call_id) != Some(token) {
            return Err(CoordinatorHandoffError::TokenMismatch);
        }
        self.pending_coordinator_handoffs.remove(&token.call_id);
        Ok(())
    }

    pub(crate) fn rollback_committed_coordinator_handoff(
        &mut self,
        token: &CoordinatorHandoffToken,
    ) -> Result<CoordinatorIdentity, CoordinatorHandoffError> {
        if self
            .pending_coordinator_handoffs
            .contains_key(&token.call_id)
        {
            return Err(CoordinatorHandoffError::HandoffInProgress);
        }
        let registration = self
            .call_coordinators
            .get_mut(&token.call_id)
            .ok_or(CoordinatorHandoffError::CallNotFound)?;
        if registration.session_id != token.target_session_id
            || registration.generation != token.target_generation
        {
            return Err(CoordinatorHandoffError::StaleOwner);
        }
        if !self.sessions.contains_key(&token.source_session_id)
            || self
                .session_calls
                .get(&token.source_session_id)
                .map(String::as_str)
                != Some(token.call_id.as_str())
        {
            return Err(CoordinatorHandoffError::InvalidTarget);
        }
        registration.session_id = token.source_session_id.clone();
        registration.generation = token.source_generation;
        Ok(CoordinatorIdentity {
            session_id: registration.session_id.clone(),
            generation: registration.generation,
        })
    }

    pub(crate) fn peer_session(&self, call_id: &str) -> Option<&str> {
        let coordinator = self.coordinator_session(call_id);
        self.session_calls
            .iter()
            .find_map(|(session_id, candidate_call)| {
                (candidate_call == call_id && coordinator != Some(session_id.as_str()))
                    .then_some(session_id.as_str())
            })
    }

    pub(crate) fn destroy_call(&mut self, call_id: &str) {
        let session_ids: Vec<String> = self
            .session_calls
            .iter()
            .filter(|(_, candidate)| *candidate == call_id)
            .map(|(session_id, _)| session_id.clone())
            .collect();
        for session_id in &session_ids {
            self.sessions.remove(session_id);
            self.session_calls.remove(session_id);
        }
        self.call_coordinators.remove(call_id);
        self.pending_coordinator_handoffs.remove(call_id);
        self.call_originators.remove(call_id);
        self.leg_sessions
            .retain(|_, session_id| !session_ids.contains(session_id));
    }

    pub(crate) fn lookup_by_session(&self, session_id: &str) -> Option<&SessionHandle> {
        self.sessions.get(session_id)
    }

    pub(crate) fn lookup_by_adapter_leg(&self, adapter_call_leg_id: &str) -> Option<&str> {
        self.leg_sessions
            .get(adapter_call_leg_id)
            .map(String::as_str)
    }

    pub(crate) fn handles(&self) -> impl Iterator<Item = (&str, &SessionHandle)> {
        self.sessions
            .iter()
            .map(|(session_id, handle)| (session_id.as_str(), handle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::call::dtmf::{DigitCollectionMode, DigitCollectionResultCode};
    use std::collections::HashSet;
    use std::time::Duration;
    use voipswitch_core::dtmf::DtmfDigit;
    use voipswitch_core::types::ids::{BusinessOperationId, CollectorId, SessionId};

    fn collection_spec() -> DigitCollectionSpec {
        DigitCollectionSpec {
            collector_id: CollectorId::from("collector-a"),
            owner: BusinessOperationId::from("operation-a"),
            source_session_id: SessionId::from("caller-a"),
            mode: DigitCollectionMode::Collect,
            allowed: HashSet::from([DtmfDigit::D5]),
            min_digits: 1,
            max_digits: 4,
            terminators: HashSet::from([DtmfDigit::Pound]),
            first_digit_timeout: Duration::from_secs(5),
            inter_digit_timeout: Duration::from_secs(3),
            overall_timeout: Duration::from_secs(20),
        }
    }

    #[test]
    fn session_mailboxes_have_documented_bounds() {
        let (handle, _control_rx, _event_rx) = SessionHandle::new_pair();
        for _ in 0..CONTROL_MAILBOX_CAPACITY {
            handle
                .control_tx
                .try_send(ControlMessage::Shutdown)
                .expect("control mailbox capacity");
        }
        assert!(matches!(
            handle.control_tx.try_send(ControlMessage::Shutdown),
            Err(mpsc::error::TrySendError::Full(_))
        ));

        for _ in 0..EVENT_MAILBOX_CAPACITY {
            let (reply, _reply_rx) = oneshot::channel();
            handle
                .try_post_event(EventMessage::QuerySessionSummary(reply))
                .expect("event mailbox capacity");
        }
        let (reply, _reply_rx) = oneshot::channel();
        assert!(matches!(
            handle.try_post_event(EventMessage::QuerySessionSummary(reply)),
            Err(mpsc::error::TrySendError::Full(_))
        ));
    }

    #[test]
    fn coordinator_mailbox_is_stable_and_bounded() {
        let (handle, mut mailbox) = CallCoordinatorHandle::new_pair("call-a");
        assert_eq!(handle.call_id(), "call-a");
        for _ in 0..COORDINATOR_CONTROL_MAILBOX_CAPACITY {
            handle
                .control_tx
                .try_send(ControlMessage::Shutdown)
                .expect("coordinator control mailbox capacity");
        }
        assert!(matches!(
            handle.control_tx.try_send(ControlMessage::CallFinalized),
            Err(mpsc::error::TrySendError::Full(_))
        ));
        for _ in 0..COORDINATOR_CONTROL_MAILBOX_CAPACITY {
            assert!(matches!(
                mailbox.control_rx.try_recv(),
                Ok(ControlMessage::Shutdown)
            ));
        }
    }

    #[tokio::test]
    async fn session_handle_wraps_digit_collection_messages_and_replies() {
        let (handle, _control_rx, mut event_rx) = SessionHandle::new_pair();
        let receivers = handle
            .start_digit_collection(collection_spec())
            .expect("submit digit collection");
        let message = event_rx.recv().await.expect("collection event");
        match message {
            EventMessage::StartDigitCollection {
                spec,
                ready_reply,
                result_reply,
            } => {
                assert_eq!(spec.collector_id.as_str(), "collector-a");
                ready_reply
                    .send(Ok(DigitCollectionReady {
                        collector_id: spec.collector_id.clone(),
                        media_generation: 7,
                    }))
                    .expect("ready receiver");
                result_reply
                    .send(DigitCollectionOutcome {
                        collector_id: spec.collector_id,
                        code: DigitCollectionResultCode::Completed,
                        digits: vec![DtmfDigit::D5],
                        reason: None,
                    })
                    .expect("result receiver");
            }
            _ => panic!("unexpected session event"),
        }
        assert_eq!(receivers.ready.await.unwrap().unwrap().media_generation, 7);
        assert_eq!(
            receivers.result.await.unwrap().code,
            DigitCollectionResultCode::Completed
        );

        let cancel = handle
            .cancel_digit_collection(CollectorId::from("collector-a"))
            .expect("submit cancellation");
        match event_rx.recv().await.expect("cancel event") {
            EventMessage::CancelDigitCollection {
                collector_id,
                reply,
            } => {
                assert_eq!(collector_id.as_str(), "collector-a");
                reply.send(Ok(())).expect("cancel receiver");
            }
            _ => panic!("unexpected session event"),
        }
        assert_eq!(cancel.await.unwrap(), Ok(()));
    }

    #[test]
    fn destroy_call_removes_all_session_and_leg_indexes() {
        let mut manager = SessionManager::default();
        let (caller, _, _) = SessionHandle::new_pair();
        let (callee, _, _) = SessionHandle::new_pair();
        let (coordinator, _coordinator_mailbox) = CallCoordinatorHandle::new_pair("call-a");
        manager.register_caller("caller", "call-a", "leg-a", caller, coordinator);
        manager.register_callee("callee", "call-a", callee);

        manager.destroy_call("call-a");

        assert!(manager.lookup_by_session("caller").is_none());
        assert!(manager.lookup_by_session("callee").is_none());
        assert!(manager.lookup_by_adapter_leg("leg-a").is_none());
    }

    #[test]
    fn replacing_callee_attempt_preserves_caller_coordinator() {
        let mut manager = SessionManager::default();
        let (caller, _, _) = SessionHandle::new_pair();
        let (first_callee, _, _) = SessionHandle::new_pair();
        let (second_callee, _, _) = SessionHandle::new_pair();
        let (coordinator, _coordinator_mailbox) = CallCoordinatorHandle::new_pair("call-a");
        manager.register_caller("caller", "call-a", "leg-a", caller, coordinator);
        manager.register_callee("callee-attempt-1", "call-a", first_callee);

        let removed = manager.unregister_session("callee-attempt-1");
        manager.register_callee("callee-attempt-2", "call-a", second_callee);

        assert!(removed.is_some());
        assert!(manager.lookup_by_session("callee-attempt-1").is_none());
        assert!(manager.lookup_by_session("callee-attempt-2").is_some());
        assert!(manager.lookup_by_session("caller").is_some());
        assert_eq!(manager.coordinator_session("call-a"), Some("caller"));
        assert_eq!(manager.caller_session("call-a"), Some("caller"));
        assert_eq!(
            manager.coordinator_identity("call-a"),
            Some(CoordinatorIdentity {
                session_id: "caller".to_string(),
                generation: 1,
            })
        );
        assert!(manager.coordinator_handle("call-a").is_some());
        assert_eq!(manager.lookup_by_adapter_leg("leg-a"), Some("caller"));
    }

    #[test]
    fn adapter_leg_binding_follows_current_callee_attempt() {
        let mut manager = SessionManager::default();
        let (callee, _, _) = SessionHandle::new_pair();
        manager.register_callee("callee-attempt-1", "call-a", callee);

        assert!(manager.bind_adapter_leg("callee-attempt-1", "leg-a"));
        assert_eq!(
            manager.lookup_by_adapter_leg("leg-a"),
            Some("callee-attempt-1")
        );

        assert!(manager.bind_adapter_leg("callee-attempt-1", "leg-b"));
        assert!(manager.lookup_by_adapter_leg("leg-a").is_none());
        assert_eq!(
            manager.lookup_by_adapter_leg("leg-b"),
            Some("callee-attempt-1")
        );

        manager.unregister_session("callee-attempt-1");
        assert!(manager.lookup_by_adapter_leg("leg-b").is_none());
        assert!(!manager.bind_adapter_leg("callee-attempt-1", "leg-c"));
    }

    #[test]
    fn coordinator_handoff_commits_only_after_prepare() {
        let mut manager = SessionManager::default();
        let (caller, _, _) = SessionHandle::new_pair();
        let (callee, _, _) = SessionHandle::new_pair();
        let (coordinator, mut mailbox) = CallCoordinatorHandle::new_pair("call-a");
        manager.register_caller("caller", "call-a", "leg-a", caller, coordinator);
        manager.register_callee("callee", "call-a", callee);

        manager
            .coordinator_handle("call-a")
            .expect("coordinator handle")
            .control_tx
            .try_send(ControlMessage::Shutdown)
            .expect("enqueue before prepare");
        let token = manager
            .prepare_coordinator_handoff("call-a", "caller", 1, "callee")
            .expect("prepare handoff");
        assert_eq!(manager.coordinator_session("call-a"), Some("caller"));
        assert_eq!(token.target_generation, 2);

        assert_eq!(
            manager.commit_coordinator_handoff(&token),
            Ok(CoordinatorIdentity {
                session_id: "callee".to_string(),
                generation: 2,
            })
        );
        assert_eq!(manager.coordinator_session("call-a"), Some("callee"));
        manager
            .coordinator_handle("call-a")
            .expect("coordinator handle after commit")
            .control_tx
            .try_send(ControlMessage::CallFinalized)
            .expect("enqueue after commit");
        assert!(matches!(
            mailbox.control_rx.try_recv(),
            Ok(ControlMessage::Shutdown)
        ));
        assert!(matches!(
            mailbox.control_rx.try_recv(),
            Ok(ControlMessage::CallFinalized)
        ));
    }

    #[test]
    fn coordinator_handoff_rejects_stale_owner_and_concurrent_prepare() {
        let mut manager = SessionManager::default();
        let (caller, _, _) = SessionHandle::new_pair();
        let (callee, _, _) = SessionHandle::new_pair();
        let (coordinator, _mailbox) = CallCoordinatorHandle::new_pair("call-a");
        manager.register_caller("caller", "call-a", "leg-a", caller, coordinator);
        manager.register_callee("callee", "call-a", callee);

        assert_eq!(
            manager.prepare_coordinator_handoff("call-a", "caller", 2, "callee"),
            Err(CoordinatorHandoffError::StaleOwner)
        );
        let token = manager
            .prepare_coordinator_handoff("call-a", "caller", 1, "callee")
            .expect("prepare handoff");
        assert_eq!(
            manager.prepare_coordinator_handoff("call-a", "caller", 1, "callee"),
            Err(CoordinatorHandoffError::HandoffInProgress)
        );
        manager
            .abort_coordinator_handoff(&token)
            .expect("abort handoff");
        assert_eq!(manager.coordinator_session("call-a"), Some("caller"));
        manager
            .prepare_coordinator_handoff("call-a", "caller", 1, "callee")
            .expect("prepare after rollback");
    }

    #[test]
    fn committed_handoff_rolls_back_only_from_matching_target_generation() {
        let mut manager = SessionManager::default();
        let (caller, _, _) = SessionHandle::new_pair();
        let (callee, _, _) = SessionHandle::new_pair();
        let (coordinator, _mailbox) = CallCoordinatorHandle::new_pair("call-a");
        manager.register_caller("caller", "call-a", "leg-a", caller, coordinator);
        manager.register_callee("callee", "call-a", callee);
        let token = manager
            .prepare_coordinator_handoff("call-a", "caller", 1, "callee")
            .expect("prepare handoff");
        manager
            .commit_coordinator_handoff(&token)
            .expect("commit handoff");

        let stale = CoordinatorHandoffToken {
            target_generation: token.target_generation.saturating_add(1),
            ..token.clone()
        };
        assert_eq!(
            manager.rollback_committed_coordinator_handoff(&stale),
            Err(CoordinatorHandoffError::StaleOwner)
        );
        assert_eq!(manager.coordinator_session("call-a"), Some("callee"));
        assert_eq!(
            manager.rollback_committed_coordinator_handoff(&token),
            Ok(CoordinatorIdentity {
                session_id: "caller".to_string(),
                generation: 1,
            })
        );
        assert_eq!(manager.coordinator_session("call-a"), Some("caller"));
    }

    #[test]
    fn committed_handoff_cannot_restore_a_destroyed_source() {
        let mut manager = SessionManager::default();
        let (caller, _, _) = SessionHandle::new_pair();
        let (callee, _, _) = SessionHandle::new_pair();
        let (coordinator, _mailbox) = CallCoordinatorHandle::new_pair("call-a");
        manager.register_caller("caller", "call-a", "leg-a", caller, coordinator);
        manager.register_callee("callee", "call-a", callee);
        let token = manager
            .prepare_coordinator_handoff("call-a", "caller", 1, "callee")
            .expect("prepare handoff");
        manager
            .commit_coordinator_handoff(&token)
            .expect("commit handoff");
        manager.unregister_session("caller");

        assert_eq!(
            manager.rollback_committed_coordinator_handoff(&token),
            Err(CoordinatorHandoffError::InvalidTarget)
        );
        assert_eq!(manager.coordinator_session("call-a"), Some("callee"));
    }

    #[test]
    fn coordinator_handoff_cannot_commit_after_target_disappears() {
        let mut manager = SessionManager::default();
        let (caller, _, _) = SessionHandle::new_pair();
        let (callee, _, _) = SessionHandle::new_pair();
        let (coordinator, _mailbox) = CallCoordinatorHandle::new_pair("call-a");
        manager.register_caller("caller", "call-a", "leg-a", caller, coordinator);
        manager.register_callee("callee", "call-a", callee);
        let token = manager
            .prepare_coordinator_handoff("call-a", "caller", 1, "callee")
            .expect("prepare handoff");

        manager.unregister_session("callee");
        assert_eq!(
            manager.commit_coordinator_handoff(&token),
            Err(CoordinatorHandoffError::TokenMismatch)
        );
        assert_eq!(manager.coordinator_session("call-a"), Some("caller"));
    }

    #[test]
    fn coordinator_handoff_rejects_generation_overflow() {
        let mut manager = SessionManager::default();
        let (caller, _, _) = SessionHandle::new_pair();
        let (callee, _, _) = SessionHandle::new_pair();
        let (coordinator, _mailbox) = CallCoordinatorHandle::new_pair("call-a");
        manager.register_caller("caller", "call-a", "leg-a", caller, coordinator);
        manager.register_callee("callee", "call-a", callee);
        manager
            .call_coordinators
            .get_mut("call-a")
            .expect("coordinator registration")
            .generation = u64::MAX;

        assert_eq!(
            manager.prepare_coordinator_handoff("call-a", "caller", u64::MAX, "callee"),
            Err(CoordinatorHandoffError::GenerationExhausted)
        );
        assert_eq!(manager.coordinator_session("call-a"), Some("caller"));
    }

    #[test]
    fn dispatcher_capacity_is_sized_for_runtime_bursts() {
        assert_eq!(CRITICAL_DISPATCHER_CAPACITY, 50_000);
    }

    #[tokio::test]
    async fn dispatcher_preserves_order_when_actor_mailbox_is_full() {
        let (handle, mut control_rx, _event_rx) = SessionHandle::new_pair();
        for _ in 0..CONTROL_MAILBOX_CAPACITY {
            handle
                .control_tx
                .try_send(ControlMessage::Shutdown)
                .expect("fill actor control mailbox");
        }
        let (dispatcher, dispatcher_task) = CriticalControlDispatcher::spawn();
        dispatcher
            .dispatch("session-a", &handle, ControlMessage::CallFinalized)
            .await
            .expect("queue first critical control");
        dispatcher
            .dispatch("session-a", &handle, ControlMessage::Shutdown)
            .await
            .expect("queue second critical control");

        for _ in 0..CONTROL_MAILBOX_CAPACITY {
            control_rx.recv().await.expect("filled control message");
        }
        assert!(matches!(
            control_rx.recv().await.expect("first dispatched control"),
            ControlMessage::CallFinalized
        ));
        assert!(matches!(
            control_rx.recv().await.expect("second dispatched control"),
            ControlMessage::Shutdown
        ));

        drop(dispatcher);
        dispatcher_task.await.expect("dispatcher joined");
    }

    #[tokio::test]
    async fn dispatcher_reports_destroyed_actor_as_gone() {
        let (handle, control_rx, _event_rx) = SessionHandle::new_pair();
        drop(control_rx);
        let (dispatcher, dispatcher_task) = CriticalControlDispatcher::spawn();

        let result = dispatcher
            .dispatch("session-a", &handle, ControlMessage::Shutdown)
            .await;

        assert_eq!(result, Err(PostError::Gone));
        drop(dispatcher);
        dispatcher_task.await.expect("dispatcher joined");
    }
}
