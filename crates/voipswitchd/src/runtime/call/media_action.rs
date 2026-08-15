use crate::runtime::call::session::{ControlMessage, CriticalControlDispatcher};
use crate::runtime::media::{DtmfCapabilityLeaseRequest, DtmfCapabilityReady, MediaBridgeHandle};
use crate::runtime::recording::RecordingSpec;
use anyhow::{Result, anyhow};
use std::net::SocketAddr;
use tokio::sync::mpsc;
use voipswitch_core::media::SdpBody;

const MEDIA_ACTION_QUEUE_CAPACITY: usize = 16;

#[derive(Debug, Clone)]
pub(crate) enum PrepareSdpPurpose {
    Provisional {
        status_code: u16,
        start_ring_timer: bool,
    },
    Answer {
        payload_types: Vec<u8>,
    },
    CalleeAttemptOffer {
        attempt_seq: u16,
    },
}

pub(crate) enum MediaActionResult {
    SdpPrepared {
        action_id: String,
        generation: u64,
        purpose: PrepareSdpPurpose,
        result: std::result::Result<SdpBody, String>,
    },
    AnswerMediaStarted {
        action_id: String,
        generation: u64,
        recording_error: Option<String>,
        caller_remote: Option<SocketAddr>,
        callee_remote: Option<SocketAddr>,
    },
    DtmfCapabilityAcquired {
        action_id: String,
        generation: u64,
        result: std::result::Result<DtmfCapabilityReady, String>,
    },
    DtmfCapabilityReleased {
        action_id: String,
        generation: u64,
        result: std::result::Result<u64, String>,
    },
}

enum MediaAction {
    PrepareSdp {
        action_id: String,
        generation: u64,
        purpose: PrepareSdpPurpose,
        sdp: SdpBody,
        route_target: Option<SocketAddr>,
    },
    StartAnswerMedia {
        action_id: String,
        generation: u64,
        recording: Option<RecordingSpec>,
        bridge_id: String,
        allow_fast_path: bool,
    },
    AcquireDtmfCapability {
        action_id: String,
        generation: u64,
        request: DtmfCapabilityLeaseRequest,
    },
    ReleaseDtmfCapability {
        action_id: String,
        generation: u64,
        lease_id: voipswitch_core::types::ids::MediaCapabilityLeaseId,
    },
}

#[derive(Clone)]
pub(crate) struct MediaActionExecutor {
    tx: mpsc::Sender<MediaAction>,
    media: MediaBridgeHandle,
}

impl MediaActionExecutor {
    pub(crate) fn media_handle(&self) -> MediaBridgeHandle {
        self.media.clone()
    }

    pub(crate) fn spawn(
        media: MediaBridgeHandle,
        session_id: String,
        target: mpsc::WeakSender<ControlMessage>,
        dispatcher: CriticalControlDispatcher,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<MediaAction>(MEDIA_ACTION_QUEUE_CAPACITY);
        let worker_media = media.clone();
        tokio::spawn(async move {
            while let Some(action) = rx.recv().await {
                let result = match action {
                    MediaAction::PrepareSdp {
                        action_id,
                        generation,
                        purpose,
                        sdp,
                        route_target,
                    } => {
                        let result = match purpose {
                            PrepareSdpPurpose::CalleeAttemptOffer { .. } => {
                                worker_media
                                    .prepare_callee_sdp(&sdp, route_target, generation)
                                    .await
                            }
                            PrepareSdpPurpose::Provisional { .. }
                            | PrepareSdpPurpose::Answer { .. } => {
                                worker_media.prepare_caller_sdp(&sdp, generation).await
                            }
                        };
                        MediaActionResult::SdpPrepared {
                            action_id,
                            generation,
                            purpose,
                            result: result.map_err(|error| error.to_string()),
                        }
                    }
                    MediaAction::StartAnswerMedia {
                        action_id,
                        generation,
                        recording,
                        bridge_id,
                        allow_fast_path,
                    } => {
                        let recording_error = match recording {
                            Some(spec) => worker_media
                                .start_recording(spec)
                                .await
                                .err()
                                .map(|error| error.to_string()),
                            None => None,
                        };
                        worker_media
                            .try_promote_fast_path(&bridge_id, allow_fast_path)
                            .await;
                        MediaActionResult::AnswerMediaStarted {
                            action_id,
                            generation,
                            recording_error,
                            caller_remote: worker_media.caller_remote().await,
                            callee_remote: worker_media.callee_remote().await,
                        }
                    }
                    MediaAction::AcquireDtmfCapability {
                        action_id,
                        generation,
                        request,
                    } => MediaActionResult::DtmfCapabilityAcquired {
                        action_id,
                        generation,
                        result: worker_media
                            .acquire_dtmf_capability(request)
                            .await
                            .map_err(|error| error.to_string()),
                    },
                    MediaAction::ReleaseDtmfCapability {
                        action_id,
                        generation,
                        lease_id,
                    } => MediaActionResult::DtmfCapabilityReleased {
                        action_id,
                        generation,
                        result: worker_media
                            .release_dtmf_capability(&lease_id)
                            .await
                            .map_err(|error| error.to_string()),
                    },
                };
                let Some(target) = target.upgrade() else {
                    break;
                };
                if dispatcher
                    .dispatch_to(
                        &session_id,
                        target,
                        ControlMessage::MediaActionResult(result),
                    )
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Self { tx, media }
    }

    pub(crate) fn prepare_sdp(
        &self,
        action_id: String,
        generation: u64,
        purpose: PrepareSdpPurpose,
        sdp: SdpBody,
    ) -> Result<()> {
        self.tx
            .try_send(MediaAction::PrepareSdp {
                action_id,
                generation,
                purpose,
                sdp,
                route_target: None,
            })
            .map_err(map_submit_error)
    }

    pub(crate) fn prepare_callee_offer(
        &self,
        action_id: String,
        generation: u64,
        attempt_seq: u16,
        sdp: SdpBody,
        route_target: Option<SocketAddr>,
    ) -> Result<()> {
        self.tx
            .try_send(MediaAction::PrepareSdp {
                action_id,
                generation,
                purpose: PrepareSdpPurpose::CalleeAttemptOffer { attempt_seq },
                sdp,
                route_target,
            })
            .map_err(map_submit_error)
    }

    pub(crate) fn start_answer_media(
        &self,
        action_id: String,
        generation: u64,
        recording: Option<RecordingSpec>,
        bridge_id: String,
        allow_fast_path: bool,
    ) -> Result<()> {
        self.tx
            .try_send(MediaAction::StartAnswerMedia {
                action_id,
                generation,
                recording,
                bridge_id,
                allow_fast_path,
            })
            .map_err(map_submit_error)
    }

    pub(crate) fn update_dtmf_callee_target(
        &self,
        session_id: String,
        control_tx: mpsc::Sender<ControlMessage>,
    ) {
        self.media.update_dtmf_callee_target(session_id, control_tx);
    }

    pub(crate) fn acquire_dtmf_capability(
        &self,
        action_id: String,
        generation: u64,
        request: DtmfCapabilityLeaseRequest,
    ) -> Result<()> {
        self.tx
            .try_send(MediaAction::AcquireDtmfCapability {
                action_id,
                generation,
                request,
            })
            .map_err(map_submit_error)
    }

    pub(crate) fn release_dtmf_capability(
        &self,
        action_id: String,
        generation: u64,
        lease_id: voipswitch_core::types::ids::MediaCapabilityLeaseId,
    ) -> Result<()> {
        self.tx
            .try_send(MediaAction::ReleaseDtmfCapability {
                action_id,
                generation,
                lease_id,
            })
            .map_err(map_submit_error)
    }
}

fn map_submit_error(error: mpsc::error::TrySendError<MediaAction>) -> anyhow::Error {
    match error {
        mpsc::error::TrySendError::Full(_) => anyhow!("media action queue full"),
        mpsc::error::TrySendError::Closed(_) => anyhow!("media action executor stopped"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_action_queue_is_bounded() {
        let (tx, _rx) = mpsc::channel::<MediaAction>(MEDIA_ACTION_QUEUE_CAPACITY);
        assert_eq!(tx.max_capacity(), MEDIA_ACTION_QUEUE_CAPACITY);
    }
}
