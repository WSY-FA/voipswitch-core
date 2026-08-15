use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionConfig {
    pub id: u64,
    pub number: String,
    pub auth_user: String,
    pub password: String,
    pub enabled: bool,
}
