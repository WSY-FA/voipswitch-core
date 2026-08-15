use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundRouteConfig {
    pub id: u64,
    pub name: String,
    pub enabled: bool,
    pub trunk_match: String,
    pub dst_pattern: String,
    pub src_pattern: Option<String>,
    pub dst_strip: u8,
    pub dst_prefix: String,
    pub dst_suffix: String,
    pub src_strip: u8,
    pub src_prefix: String,
    pub src_suffix: String,
    pub target: String,
    pub priority: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboundRouteConfig {
    pub id: u64,
    pub name: String,
    pub enabled: bool,
    pub dst_pattern: String,
    pub src_pattern: Option<String>,
    pub dst_strip: u8,
    pub dst_prefix: String,
    pub dst_suffix: String,
    pub src_strip: u8,
    pub src_prefix: String,
    pub src_suffix: String,
    pub priority: u16,
    pub trunk_refs: Vec<String>,
}
