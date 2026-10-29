//! Wire DTOs for the runtime socket.
//!
//! The envelope itself is shared with core (`voipswitch_core::ipc::runtime`).
//! Bodies are parsed as JSON values so the mock stays tolerant of fields it
//! does not model, matching the adapter boundary rule that unknown
//! non-critical fields are ignored.

use serde::Deserialize;
use serde_json::Value;

pub use voipswitch_core::ipc::runtime::RuntimeEnvelope;

pub const PROTOCOL_VERSION: u16 = 1;

pub fn hello(instance_id: &str) -> RuntimeEnvelope {
    RuntimeEnvelope::new(
        "event",
        "AdapterHello",
        Some("hello-1".to_string()),
        serde_json::json!({
            "instance_id": instance_id,
            "role": "mock-loopback",
            "protocol_version": PROTOCOL_VERSION,
        }),
    )
}

pub fn event(frame_type: &str, body: Value) -> RuntimeEnvelope {
    RuntimeEnvelope::new("event", frame_type, None, body)
}

#[derive(Debug, Clone, Deserialize)]
pub struct HelloAck {
    #[serde(default)]
    pub accepted: bool,
    #[serde(default)]
    pub protocol_version: Option<u16>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeView {
    pub view_revision: u64,
    #[serde(default)]
    pub endpoints: Vec<EndpointView>,
    #[serde(default)]
    pub domains: Vec<DomainView>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EndpointView {
    pub domain_id: String,
    pub endpoint_id: String,
    pub number: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DomainView {
    pub domain_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ActionRequest {
    #[serde(default)]
    pub call_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub action_id: Option<String>,
    #[serde(default)]
    pub generation: Option<u64>,
    #[serde(default)]
    pub adapter_call_leg_id: Option<String>,
}

pub fn action_ack(
    frame_type: &str,
    request: &ActionRequest,
    status: &str,
    result: Value,
) -> RuntimeEnvelope {
    event(
        "CallActionAck",
        serde_json::json!({
            "call_id": request.call_id.clone().unwrap_or_default(),
            "session_id": request.session_id.clone().unwrap_or_default(),
            "action": frame_type,
            "action_id": request.action_id.clone().unwrap_or_default(),
            "generation": request.generation.unwrap_or(1),
            "status": status,
            "result": result,
        }),
    )
}

pub struct LegEvent<'a> {
    pub frame_type: &'a str,
    pub call_id: &'a str,
    pub session_id: &'a str,
    pub adapter_call_leg_id: &'a str,
    pub seq: u64,
    pub status_code: Option<u16>,
    pub sdp: Option<Value>,
    pub reason: Option<i64>,
}

impl LegEvent<'_> {
    pub fn into_envelope(self) -> RuntimeEnvelope {
        event(
            self.frame_type,
            serde_json::json!({
                "call_id": self.call_id,
                "session_id": self.session_id,
                "adapter_call_leg_id": self.adapter_call_leg_id,
                "leg_event_seq": self.seq,
                "status_code": self.status_code,
                "sdp": self.sdp,
                "reason": self.reason,
            }),
        )
    }
}
