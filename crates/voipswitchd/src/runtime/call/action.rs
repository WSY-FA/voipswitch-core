use crate::runtime::adapter::{AdapterRuntimeWriter, RuntimeEnvelope};
use crate::runtime::call::event::{ActionDeliveryFailed, ActionIdentity, InboundInviteOffered};
use crate::runtime::call::session::{ControlMessage, CriticalControlDispatcher};
use anyhow::{Result, anyhow};
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;
use tracing::debug;
use voipswitch_core::types::time::unix_timestamp_ms;

pub(crate) fn call_action_id(action: &str, call_id: &str) -> String {
    format!("{action}-{call_id}")
}

#[derive(Clone)]
pub(crate) struct AdapterActionExecutor {
    writer: AdapterRuntimeWriter,
    control_dispatcher: CriticalControlDispatcher,
}

impl AdapterActionExecutor {
    pub(crate) fn new(
        writer: AdapterRuntimeWriter,
        control_dispatcher: CriticalControlDispatcher,
    ) -> Self {
        Self {
            writer,
            control_dispatcher,
        }
    }

    pub(crate) fn submit(
        &self,
        identity: ActionIdentity,
        domain_id: &str,
        mut body: Value,
        target: mpsc::WeakSender<ControlMessage>,
    ) -> Result<()> {
        let object = body
            .as_object_mut()
            .ok_or_else(|| anyhow!("call action body must be an object"))?;
        insert_identity(object, &identity);
        let request_id = format!(
            "call-action-{}-{}-{}",
            identity.action_kind,
            identity.action_id,
            unix_timestamp_ms()
        );
        let mut frame =
            RuntimeEnvelope::new("command", &identity.action_kind, Some(request_id), body);
        frame.domain_id = Some(domain_id.to_string());
        let completion = self.writer.try_send(frame)?;
        let dispatcher = self.control_dispatcher.clone();
        tokio::spawn(async move {
            let failure = match completion.await {
                Ok(Ok(())) => return,
                Ok(Err(reason)) => reason,
                Err(_) => "adapter runtime writer dropped action completion".to_string(),
            };
            let Some(target) = target.upgrade() else {
                debug!(
                    call_id = identity.call_id,
                    session_id = identity.session_id,
                    action_id = identity.action_id,
                    "action delivery failure target no longer exists"
                );
                return;
            };
            let session_id = identity.session_id.clone();
            let _ = dispatcher
                .dispatch_to(
                    &session_id,
                    target,
                    ControlMessage::ActionDeliveryFailed(ActionDeliveryFailed {
                        identity,
                        reason: failure,
                    }),
                )
                .await;
        });
        Ok(())
    }
}

fn insert_identity(body: &mut Map<String, Value>, identity: &ActionIdentity) {
    body.insert("call_id".to_string(), json!(identity.call_id));
    body.insert("session_id".to_string(), json!(identity.session_id));
    body.insert("action_id".to_string(), json!(identity.action_id));
    body.insert("generation".to_string(), json!(identity.generation));
}

pub(crate) async fn reject_inbound(
    writer: &AdapterRuntimeWriter,
    event: &InboundInviteOffered,
    status_code: u16,
) -> Result<()> {
    send_call_command_wait(
        writer,
        "RejectInboundInvite",
        &event.domain_id,
        json!({
            "adapter_call_leg_id": event.adapter_call_leg_id,
            "status_code": status_code,
            "action_id": format!("reject-{}", event.adapter_call_leg_id),
            "generation": 1,
        }),
    )
    .await
}

async fn send_call_command_wait(
    writer: &AdapterRuntimeWriter,
    frame_type: &str,
    domain_id: &str,
    body: Value,
) -> Result<()> {
    let request_id = format!("call-action-{}-{}", frame_type, unix_timestamp_ms());
    let mut frame = RuntimeEnvelope::new("command", frame_type, Some(request_id), body);
    frame.domain_id = Some(domain_id.to_string());
    writer.send(frame).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_identity_overrides_untrusted_body_fields() {
        let identity = ActionIdentity {
            call_id: "call-a".to_string(),
            session_id: "session-a".to_string(),
            action_kind: "HangupDialog".to_string(),
            action_id: "hangup-a".to_string(),
            generation: 7,
        };
        let mut body = json!({
            "call_id": "wrong",
            "session_id": "wrong",
            "action_id": "wrong",
            "generation": 1,
        });
        insert_identity(body.as_object_mut().unwrap(), &identity);

        assert_eq!(body["call_id"], "call-a");
        assert_eq!(body["session_id"], "session-a");
        assert_eq!(body["action_id"], "hangup-a");
        assert_eq!(body["generation"], 7);
    }
}
