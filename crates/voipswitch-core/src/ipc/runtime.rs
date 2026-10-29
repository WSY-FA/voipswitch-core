use crate::types::time::unix_timestamp_ms;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Versioned frame exchanged by `voipswitchd` and a SIP adapter over the
/// runtime Unix socket. Both directions use the same envelope.
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
