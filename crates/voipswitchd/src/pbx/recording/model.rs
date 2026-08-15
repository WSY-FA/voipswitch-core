use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingPolicyConfig {
    pub id: u64,
    pub name: String,
    pub enabled: bool,
    pub targets: Vec<RecordingTargetRef>,
    pub direction: RecordingDirection,
    pub priority: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingTargetRef {
    pub target_type: RecordingTargetType,
    pub target_id: u64,
}

impl RecordingTargetRef {
    pub fn stable_ref(self) -> String {
        let prefix = match self.target_type {
            RecordingTargetType::Extension => "ext",
            RecordingTargetType::PeerTrunk => "peer",
            RecordingTargetType::RegTrunk => "reg",
        };
        format!("{prefix}:{}", self.target_id)
    }

    pub fn parse(value: &str) -> anyhow::Result<Self> {
        let (prefix, id) = value
            .trim()
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("invalid recording target reference: {value}"))?;
        let target_type = match prefix {
            "ext" => RecordingTargetType::Extension,
            "peer" => RecordingTargetType::PeerTrunk,
            "reg" => RecordingTargetType::RegTrunk,
            _ => anyhow::bail!("invalid recording target reference: {value}"),
        };
        let target_id = id
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("invalid recording target reference: {value}"))?;
        anyhow::ensure!(target_id > 0, "invalid recording target reference: {value}");
        Ok(Self {
            target_type,
            target_id,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordingTargetType {
    Extension,
    PeerTrunk,
    RegTrunk,
}

impl RecordingTargetType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Extension => "extension",
            Self::PeerTrunk => "peer_trunk",
            Self::RegTrunk => "reg_trunk",
        }
    }
}

impl TryFrom<&str> for RecordingTargetType {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "extension" => Ok(Self::Extension),
            "peer_trunk" => Ok(Self::PeerTrunk),
            "reg_trunk" => Ok(Self::RegTrunk),
            _ => anyhow::bail!("target_type must be extension, peer_trunk or reg_trunk"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordingDirection {
    Inbound,
    Outbound,
    Both,
}

impl RecordingDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
            Self::Both => "both",
        }
    }

    pub fn matches(self, direction: Self) -> bool {
        self == Self::Both || self == direction
    }
}

impl TryFrom<&str> for RecordingDirection {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "inbound" => Ok(Self::Inbound),
            "outbound" => Ok(Self::Outbound),
            "both" => Ok(Self::Both),
            _ => anyhow::bail!("direction must be inbound, outbound or both"),
        }
    }
}
