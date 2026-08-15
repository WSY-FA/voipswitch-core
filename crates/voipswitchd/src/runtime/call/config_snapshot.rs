use crate::runtime::call::model::OutboundCandidate;
use crate::runtime::call::timer::CallTimeouts;
use std::path::PathBuf;
use std::sync::Arc;
use voipswitch_core::call::BridgePolicy;
use voipswitch_core::types::ids::DomainId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CallNumberSnapshot {
    pub(crate) original_caller: String,
    pub(crate) original_callee: String,
    pub(crate) signaling_caller: String,
    pub(crate) signaling_callee: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RouteSnapshot {
    pub(crate) route_id: Option<String>,
    pub(crate) route_name: Option<String>,
    pub(crate) trunk_ref: Option<String>,
    pub(crate) trunk_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordingDecisionSnapshot {
    pub(crate) initial_requested: bool,
    pub(crate) initial_policy_ids: Arc<[u64]>,
    pub(crate) recording_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AiPolicyDecisionSnapshot {
    pub(crate) policy_id: u64,
    pub(crate) profile_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MediaPolicySnapshot {
    pub(crate) audio_only: bool,
    pub(crate) max_audio_m_lines: u8,
}

pub(crate) type CandidateSnapshot = OutboundCandidate;

#[derive(Debug, Clone)]
pub(crate) struct CallConfigSnapshot {
    pub(crate) runtime_config_version: u64,
    pub(crate) domain_config_version: u64,
    pub(crate) domain_id: DomainId,
    pub(crate) numbers: CallNumberSnapshot,
    pub(crate) inbound: RouteSnapshot,
    pub(crate) outbound: RouteSnapshot,
    pub(crate) candidates: Arc<[CandidateSnapshot]>,
    pub(crate) timeouts: CallTimeouts,
    pub(crate) bridge_policy: BridgePolicy,
    pub(crate) recording: RecordingDecisionSnapshot,
    pub(crate) initial_ai_policy: Option<AiPolicyDecisionSnapshot>,
    pub(crate) media_policy: MediaPolicySnapshot,
}

impl CallConfigSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        runtime_config_version: u64,
        domain_config_version: u64,
        domain_id: DomainId,
        numbers: CallNumberSnapshot,
        inbound: RouteSnapshot,
        outbound: RouteSnapshot,
        candidates: Vec<CandidateSnapshot>,
        timeouts: CallTimeouts,
        recording_dir: PathBuf,
    ) -> Result<Self, String> {
        let Some(first) = candidates.first() else {
            return Err("call config snapshot requires at least one candidate".to_string());
        };
        let initial_recording_requested = first.recording_requested;
        let initial_recording_policy_ids = first.recording_policy_ids.clone();
        let initial_ai_policy = first.ai_policy.clone();
        Ok(Self {
            runtime_config_version,
            domain_config_version,
            domain_id,
            numbers,
            inbound,
            outbound,
            candidates: candidates.into(),
            timeouts,
            bridge_policy: BridgePolicy::SequentialHunt,
            recording: RecordingDecisionSnapshot {
                initial_requested: initial_recording_requested,
                initial_policy_ids: initial_recording_policy_ids,
                recording_dir,
            },
            initial_ai_policy,
            media_policy: MediaPolicySnapshot {
                audio_only: true,
                max_audio_m_lines: 1,
            },
        })
    }
}
