use crate::runtime::call::actor::{CalleeSessionActor, CoordinatorHandoffPackage};
use crate::runtime::call::session::{CoordinatorHandoffToken, CoordinatorIdentity};
use std::sync::{Arc, Mutex};
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::{mpsc, oneshot};

const HANDOFF_REQUEST_CAPACITY: usize = 32;
pub(crate) const HANDOFF_READY_CAPACITY: usize = 32;

struct RuntimeBinding {
    generation: u64,
    tx: mpsc::Sender<CoordinatorHandoffRequest>,
}

#[derive(Default)]
struct CoordinatorHandoffServiceState {
    runtime_generation: u64,
    runtime: Option<RuntimeBinding>,
}

/// Bounded entry point for a business module whose current coordinator leg is
/// leaving while another leg must continue owning the call.
#[derive(Clone, Default)]
pub(crate) struct CoordinatorHandoffService {
    inner: Arc<Mutex<CoordinatorHandoffServiceState>>,
}

impl CoordinatorHandoffService {
    pub(crate) fn attach_runtime(&self) -> (u64, mpsc::Receiver<CoordinatorHandoffRequest>) {
        let (tx, rx) = mpsc::channel(HANDOFF_REQUEST_CAPACITY);
        let mut state = self.inner.lock().expect("handoff service lock poisoned");
        state.runtime_generation = state.runtime_generation.saturating_add(1);
        let generation = state.runtime_generation;
        state.runtime = Some(RuntimeBinding { generation, tx });
        (generation, rx)
    }

    pub(crate) fn detach_runtime(&self, generation: u64) {
        let mut state = self.inner.lock().expect("handoff service lock poisoned");
        if state.runtime.as_ref().map(|runtime| runtime.generation) == Some(generation) {
            state.runtime = None;
        }
    }

    #[allow(
        dead_code,
        reason = "called by transfer/pickup modules in a later milestone"
    )]
    pub(crate) fn request_owner_exit_handoff(
        &self,
        call_id: impl Into<String>,
        target_session_id: impl Into<String>,
    ) -> Result<oneshot::Receiver<Result<CoordinatorIdentity, String>>, &'static str> {
        let state = self.inner.lock().expect("handoff service lock poisoned");
        let Some(runtime) = state.runtime.as_ref() else {
            return Err("ADAPTER_UNAVAILABLE: call runtime is not connected");
        };
        let (reply, response) = oneshot::channel();
        runtime
            .tx
            .try_send(CoordinatorHandoffRequest {
                call_id: call_id.into(),
                target_session_id: target_session_id.into(),
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    "RESOURCE_EXHAUSTED: coordinator handoff queue is full"
                }
                mpsc::error::TrySendError::Closed(_) => {
                    "ADAPTER_UNAVAILABLE: coordinator handoff queue is closed"
                }
            })?;
        Ok(response)
    }
}

pub(crate) struct CoordinatorHandoffRequest {
    pub(crate) call_id: String,
    pub(crate) target_session_id: String,
    pub(crate) reply: oneshot::Sender<Result<CoordinatorIdentity, String>>,
}

pub(crate) struct CoordinatorHandoffReady {
    pub(crate) token: CoordinatorHandoffToken,
    pub(crate) source: Result<CoordinatorHandoffPackage, String>,
    pub(crate) target: Result<CalleeSessionActor, String>,
    pub(crate) reply: oneshot::Sender<Result<CoordinatorIdentity, String>>,
    pub(crate) _permit: OwnedSemaphorePermit,
}

pub(crate) fn await_actor_readiness(
    token: CoordinatorHandoffToken,
    source: oneshot::Receiver<Result<CoordinatorHandoffPackage, String>>,
    target: oneshot::Receiver<Result<CalleeSessionActor, String>>,
    reply: oneshot::Sender<Result<CoordinatorIdentity, String>>,
    ready_tx: mpsc::Sender<CoordinatorHandoffReady>,
    permit: OwnedSemaphorePermit,
) {
    tokio::spawn(async move {
        let source = source
            .await
            .unwrap_or_else(|_| Err("coordinator_handoff_source_channel_closed".to_string()));
        let target = target
            .await
            .unwrap_or_else(|_| Err("coordinator_handoff_target_channel_closed".to_string()));
        let ready = CoordinatorHandoffReady {
            token,
            source,
            target,
            reply,
            _permit: permit,
        };
        if let Err(error) = ready_tx.send(ready).await {
            let ready = error.0;
            recover_actors(ready.source, ready.target);
            let _ = ready
                .reply
                .send(Err("coordinator_handoff_runtime_stopped".to_string()));
        }
    });
}

pub(crate) fn recover_actors(
    source: Result<CoordinatorHandoffPackage, String>,
    target: Result<CalleeSessionActor, String>,
) {
    if let Ok(package) = source {
        tokio::spawn(package.rollback().run());
    }
    if let Ok(actor) = target {
        tokio::spawn(actor.run());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn service_is_bounded_and_detaches_by_generation() {
        let service = CoordinatorHandoffService::default();
        assert!(
            service
                .request_owner_exit_handoff("call-a", "callee-a")
                .is_err()
        );
        let (generation, _rx) = service.attach_runtime();
        for sequence in 0..HANDOFF_REQUEST_CAPACITY {
            service
                .request_owner_exit_handoff("call-a", format!("callee-{sequence}"))
                .expect("queue request");
        }
        assert_eq!(
            service
                .request_owner_exit_handoff("call-a", "overflow")
                .expect_err("bounded queue must reject overflow"),
            "RESOURCE_EXHAUSTED: coordinator handoff queue is full"
        );
        service.detach_runtime(generation.saturating_add(1));
        assert_eq!(
            service
                .request_owner_exit_handoff("call-b", "callee-b")
                .expect_err("old generation must remain attached"),
            "RESOURCE_EXHAUSTED: coordinator handoff queue is full"
        );
        service.detach_runtime(generation);
        assert!(
            service
                .request_owner_exit_handoff("call-c", "callee-c")
                .is_err()
        );
    }
}
