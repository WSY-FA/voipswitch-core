use serde::{Deserialize, Serialize};
use voipswitch_core::types::ids::DomainId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainUpsert {
    pub domain_id: Option<DomainId>,
    pub name: String,
    pub realm: String,
    pub password: String,
    pub remark: String,
    pub enabled: bool,
}
