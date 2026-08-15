use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerTrunkConfig {
    pub id: u64,
    pub name: String,
    pub server_host: String,
    pub server_port: u16,
    pub outbound_proxy_host: Option<String>,
    pub outbound_proxy_port: Option<u16>,
    pub transport: String,
    pub keep_alive_seconds: u32,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterTrunkConfig {
    pub id: u64,
    pub name: String,
    pub server_host: String,
    pub server_port: u16,
    pub outbound_proxy_host: Option<String>,
    pub outbound_proxy_port: Option<u16>,
    pub transport: String,
    pub keep_alive_seconds: u32,
    pub requested_expires_seconds: u32,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterAccountConfig {
    pub id: u64,
    pub reg_trunk_id: u64,
    pub auth_name: String,
    pub auth_pwd: String,
    pub enabled: bool,
}
