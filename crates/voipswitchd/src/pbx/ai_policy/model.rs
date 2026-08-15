use crate::pbx::recording::model::RecordingTargetRef;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiPolicyConfig {
    pub id: u64,
    pub name: String,
    pub enabled: bool,
    pub targets: Vec<RecordingTargetRef>,
    pub direction: AiPolicyDirection,
    pub priority: i32,
    pub ai_profile_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiPolicyDirection {
    Any,
    Internal,
    Inbound,
    Outbound,
}

impl AiPolicyDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::Internal => "internal",
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        }
    }

    pub fn matches(self, inbound: bool, outbound: bool) -> bool {
        match self {
            Self::Any => true,
            Self::Internal => !inbound && !outbound,
            Self::Inbound => inbound,
            Self::Outbound => outbound,
        }
    }
}

impl TryFrom<&str> for AiPolicyDirection {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "any" => Ok(Self::Any),
            "internal" => Ok(Self::Internal),
            "inbound" => Ok(Self::Inbound),
            "outbound" => Ok(Self::Outbound),
            _ => anyhow::bail!("direction must be any, internal, inbound or outbound"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_handles_trunk_to_trunk_calls() {
        assert!(AiPolicyDirection::Inbound.matches(true, true));
        assert!(AiPolicyDirection::Outbound.matches(true, true));
        assert!(!AiPolicyDirection::Internal.matches(true, true));
        assert!(AiPolicyDirection::Internal.matches(false, false));
    }
}
