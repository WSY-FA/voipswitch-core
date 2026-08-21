use crate::ai::media_tap::{AiCaptureFinalized, AiMediaTapSender, AiTapSide};
use crate::data_store::RecordingRecord;
use crate::runtime::call::session::{ControlMessage, CriticalControlDispatcher};
use crate::runtime::dtmf::{DtmfMediaMode, ParserConfig, ParserObservation, Rfc4733Parser};
use crate::runtime::fastpath::EbpfFastPathController;
use crate::runtime::recording::{
    RecordingSession, RecordingSide, RecordingSpec, RecordingTapSender,
};
use anyhow::{Context, Result, bail};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::RwLock as StdRwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, Notify, RwLock, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use voipswitch_core::dtmf::{DigitEvent, DtmfEventId, DtmfSourceGeneration, DtmfTransport};
use voipswitch_core::media::{
    DtmfCapability, FastPathAvailability, FastPathBridgeSpec, FastPathController, FastPathError,
    FastPathFallbackReason, FastPathFlowSpec, FastPathMediaKind, FastPathStats, MediaFlowDirection,
    MediaForwardingHistory, MediaForwardingMode, RelayEndpoint, SdpBody, parse_audio_sdp,
    rewrite_audio_sdp,
};
use voipswitch_core::types::ids::{
    BusinessOperationId, CallId, DomainId, MediaCapabilityLeaseId, SessionId,
};
use voipswitch_core::types::time::unix_timestamp_ms;

const FAST_PATH_HEALTH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);
const FAST_PATH_REDIRECT_ERROR_THRESHOLD: u64 = 3;
const DTMF_OBSERVATION_QUEUE_CAPACITY: usize = 32;
const DTMF_COLLECT_RELEASE_DRAIN: std::time::Duration = std::time::Duration::from_millis(250);

#[derive(Debug, Default)]
struct UnavailableFastPathController;

impl FastPathController for UnavailableFastPathController {
    fn availability(&self) -> FastPathAvailability {
        FastPathAvailability::Unavailable
    }

    fn promote(&self, _spec: &FastPathBridgeSpec) -> Result<(), FastPathError> {
        Err(fast_path_unavailable())
    }

    fn snapshot(&self, _bridge_id: &str, _generation: u64) -> Result<FastPathStats, FastPathError> {
        Err(fast_path_unavailable())
    }

    fn demote(
        &self,
        _bridge_id: &str,
        _generation: u64,
        _reason: FastPathFallbackReason,
    ) -> Result<FastPathStats, FastPathError> {
        Err(fast_path_unavailable())
    }

    fn remove(&self, _bridge_id: &str, _generation: u64) -> Result<FastPathStats, FastPathError> {
        Err(fast_path_unavailable())
    }
}

fn fast_path_unavailable() -> FastPathError {
    FastPathError {
        code: "fast_path_unavailable".to_string(),
        message: "tc/eBPF fast path backend is not available".to_string(),
    }
}

#[derive(Clone)]
pub struct MediaPlaneManager {
    fast_path: Arc<dyn FastPathController>,
}

impl Default for MediaPlaneManager {
    fn default() -> Self {
        let mode = std::env::var("VOIPSWITCH_FASTPATH_MODE")
            .unwrap_or_else(|_| "auto".to_string())
            .to_ascii_lowercase();
        if mode == "userspace" {
            return Self {
                fast_path: Arc::new(UnavailableFastPathController),
            };
        }
        match EbpfFastPathController::load() {
            Ok(controller) => Self {
                fast_path: Arc::new(controller),
            },
            Err(err) => {
                warn!(mode, error = ?err, "RTP fast path unavailable; using userspace relay");
                Self {
                    fast_path: Arc::new(UnavailableFastPathController),
                }
            }
        }
    }
}

impl MediaPlaneManager {
    pub fn fast_path_availability(&self) -> FastPathAvailability {
        self.fast_path.availability()
    }

    pub async fn allocate_bridge(
        &self,
        offer: &SdpBody,
        caller_route_target: Option<SocketAddr>,
        callee_route_target: Option<SocketAddr>,
    ) -> Result<(MediaBridge, SdpBody)> {
        debug!(
            fast_path = ?self.fast_path_availability(),
            "allocating userspace media bridge"
        );
        MediaBridge::allocate_userspace(
            offer,
            caller_route_target,
            callee_route_target,
            self.fast_path.clone(),
        )
        .await
    }
}

#[derive(Debug, Default)]
struct RelayCounters {
    caller_to_callee_packets: AtomicU64,
    caller_to_callee_bytes: AtomicU64,
    callee_to_caller_packets: AtomicU64,
    callee_to_caller_bytes: AtomicU64,
    caller_to_callee_rtcp_packets: AtomicU64,
    callee_to_caller_rtcp_packets: AtomicU64,
}

#[derive(Debug, Default)]
struct DtmfObservationCounters {
    completed: AtomicU64,
    incomplete: AtomicU64,
    invalid: AtomicU64,
    delivery_fault: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DtmfObservationStats {
    pub completed: u64,
    pub incomplete: u64,
    pub invalid: u64,
    pub delivery_fault: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MediaStatsSnapshot {
    pub caller_to_callee_packets: u64,
    pub caller_to_callee_bytes: u64,
    pub callee_to_caller_packets: u64,
    pub callee_to_caller_bytes: u64,
    pub caller_to_callee_rtcp_packets: u64,
    pub callee_to_caller_rtcp_packets: u64,
    pub dtmf: DtmfObservationStats,
}

pub struct MediaFinalized {
    pub stats: MediaStatsSnapshot,
    pub recording: Option<RecordingRecord>,
    pub forwarding_mode: MediaForwardingMode,
    pub(crate) ai_capture: Option<AiCaptureFinalized>,
}

#[derive(Debug, Clone, Copy)]
enum RelayDirection {
    CallerToCalleeRtp,
    CalleeToCallerRtp,
    CallerToCalleeRtcp,
    CalleeToCallerRtcp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RemoteEndpointSlot {
    CallerRtp,
    CallerRtcp,
    CalleeRtp,
    CalleeRtcp,
}

impl RemoteEndpointSlot {
    fn direction(self) -> MediaFlowDirection {
        match self {
            Self::CallerRtp | Self::CallerRtcp => MediaFlowDirection::CallerToCallee,
            Self::CalleeRtp | Self::CalleeRtcp => MediaFlowDirection::CalleeToCaller,
        }
    }

    fn confirmation_packets(self) -> u8 {
        match self {
            Self::CallerRtp | Self::CalleeRtp => 3,
            Self::CallerRtcp | Self::CalleeRtcp => 1,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RemoteEndpointObservation {
    slot: RemoteEndpointSlot,
    source: SocketAddr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DtmfRelayDirection {
    CallerToCallee,
    CalleeToCaller,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DtmfCapabilityLeaseRequest {
    pub(crate) lease_id: MediaCapabilityLeaseId,
    pub(crate) owner: BusinessOperationId,
    pub(crate) source_session_id: SessionId,
    pub(crate) mode: DtmfMediaMode,
    pub(crate) requested_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DtmfCapabilityReady {
    pub(crate) lease_id: MediaCapabilityLeaseId,
    pub(crate) source_session_id: SessionId,
    pub(crate) mode: DtmfMediaMode,
    pub(crate) media_generation: u64,
}

#[derive(Debug, Clone)]
struct DtmfCapabilityLease {
    request: DtmfCapabilityLeaseRequest,
    direction: DtmfRelayDirection,
    ready_generation: u64,
    active: bool,
    released_generation: Option<u64>,
}

#[derive(Default)]
struct DtmfCapabilityRuntimeState {
    leases: HashMap<MediaCapabilityLeaseId, DtmfCapabilityLease>,
}

#[derive(Debug, Clone, Copy)]
struct DtmfRelayObservation {
    direction: DtmfRelayDirection,
    observation: ParserObservation,
}

#[derive(Clone)]
struct DtmfDispatchTarget {
    session_id: SessionId,
    control_tx: mpsc::Sender<ControlMessage>,
}

#[derive(Clone)]
struct DtmfDispatchBindings {
    domain_id: DomainId,
    call_id: CallId,
    caller: DtmfDispatchTarget,
    callee: DtmfDispatchTarget,
    dispatcher: CriticalControlDispatcher,
}

pub(crate) struct DtmfSessionBindings {
    pub(crate) domain_id: String,
    pub(crate) call_id: String,
    pub(crate) caller_session_id: String,
    pub(crate) caller_control_tx: mpsc::Sender<ControlMessage>,
    pub(crate) callee_session_id: String,
    pub(crate) callee_control_tx: mpsc::Sender<ControlMessage>,
    pub(crate) dispatcher: CriticalControlDispatcher,
}

struct RelayTaskSpec {
    recv_socket: Arc<UdpSocket>,
    send_socket: Arc<UdpSocket>,
    source_endpoint: Arc<RwLock<Option<SocketAddr>>>,
    target: Arc<RwLock<Option<SocketAddr>>>,
    counters: Arc<RelayCounters>,
    direction: RelayDirection,
    endpoint_slot: RemoteEndpointSlot,
    observation_tx: mpsc::Sender<RemoteEndpointObservation>,
    recording: Option<(Arc<StdRwLock<Option<RecordingTapSender>>>, RecordingSide)>,
    ai_tap: Option<(Arc<StdRwLock<Option<AiMediaTapSender>>>, AiTapSide)>,
    dtmf_config: Option<watch::Receiver<ParserConfig>>,
    dtmf_policy_applied: Option<(Arc<AtomicU64>, Arc<Notify>)>,
    dtmf_observation_tx: Option<mpsc::Sender<DtmfRelayObservation>>,
    dtmf_counters: Arc<DtmfObservationCounters>,
}

#[derive(Debug, Clone, Copy)]
struct RebindCandidate {
    source: SocketAddr,
    packets: u8,
}

#[derive(Default)]
struct FastPathRuntimeState {
    bridge_id: Option<String>,
    generation: u64,
    active_generation: Option<u64>,
    accumulated_stats: FastPathStats,
    forwarding_history: MediaForwardingHistory,
    promotion_blocked: bool,
    stopped: bool,
}

struct FastPathRuntime {
    controller: Arc<dyn FastPathController>,
    state: StdMutex<FastPathRuntimeState>,
    caller_rtp: Arc<UdpSocket>,
    caller_rtcp: Arc<UdpSocket>,
    callee_rtp: Arc<UdpSocket>,
    callee_rtcp: Arc<UdpSocket>,
    caller_remote_rtp: Arc<RwLock<Option<SocketAddr>>>,
    caller_remote_rtcp: Arc<RwLock<Option<SocketAddr>>>,
    callee_remote_rtp: Arc<RwLock<Option<SocketAddr>>>,
    callee_remote_rtcp: Arc<RwLock<Option<SocketAddr>>>,
    caller_advertise_ip: IpAddr,
    callee_advertise_ip: Arc<RwLock<IpAddr>>,
}

pub struct MediaBridge {
    caller_rtp: Arc<UdpSocket>,
    caller_rtcp: Arc<UdpSocket>,
    callee_rtp: Arc<UdpSocket>,
    caller_remote_rtp: Arc<RwLock<Option<SocketAddr>>>,
    callee_remote_rtp: Arc<RwLock<Option<SocketAddr>>>,
    callee_remote_rtcp: Arc<RwLock<Option<SocketAddr>>>,
    caller_advertise_ip: IpAddr,
    counters: Arc<RelayCounters>,
    dtmf_counters: Arc<DtmfObservationCounters>,
    caller_dtmf_config: watch::Sender<ParserConfig>,
    callee_dtmf_config: watch::Sender<ParserConfig>,
    dtmf_dispatch: watch::Sender<Option<DtmfDispatchBindings>>,
    dtmf_capabilities: Arc<Mutex<DtmfCapabilityRuntimeState>>,
    dtmf_capability_ops: Arc<Mutex<()>>,
    caller_dtmf_applied: Arc<AtomicU64>,
    callee_dtmf_applied: Arc<AtomicU64>,
    caller_dtmf_notify: Arc<Notify>,
    callee_dtmf_notify: Arc<Notify>,
    recording_tap: Arc<StdRwLock<Option<RecordingTapSender>>>,
    ai_tap: Arc<StdRwLock<Option<AiMediaTapSender>>>,
    recording: Arc<Mutex<Option<RecordingSession>>>,
    stopped: Arc<AtomicBool>,
    fast_path_runtime: Arc<FastPathRuntime>,
    stop_tx: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct MediaBridgeHandle {
    caller_rtp: Arc<UdpSocket>,
    caller_rtcp: Arc<UdpSocket>,
    caller_remote_rtp: Arc<RwLock<Option<SocketAddr>>>,
    callee_remote_rtp: Arc<RwLock<Option<SocketAddr>>>,
    callee_remote_rtcp: Arc<RwLock<Option<SocketAddr>>>,
    caller_advertise_ip: IpAddr,
    recording_tap: Arc<StdRwLock<Option<RecordingTapSender>>>,
    ai_tap: Arc<StdRwLock<Option<AiMediaTapSender>>>,
    recording: Arc<Mutex<Option<RecordingSession>>>,
    stopped: Arc<AtomicBool>,
    fast_path_runtime: Arc<FastPathRuntime>,
    caller_dtmf_config: watch::Sender<ParserConfig>,
    callee_dtmf_config: watch::Sender<ParserConfig>,
    dtmf_dispatch: watch::Sender<Option<DtmfDispatchBindings>>,
    dtmf_capabilities: Arc<Mutex<DtmfCapabilityRuntimeState>>,
    dtmf_capability_ops: Arc<Mutex<()>>,
    caller_dtmf_applied: Arc<AtomicU64>,
    callee_dtmf_applied: Arc<AtomicU64>,
    caller_dtmf_notify: Arc<Notify>,
    callee_dtmf_notify: Arc<Notify>,
}

impl MediaBridge {
    async fn allocate_userspace(
        offer: &SdpBody,
        caller_route_target: Option<SocketAddr>,
        callee_route_target: Option<SocketAddr>,
        fast_path: Arc<dyn FastPathController>,
    ) -> Result<(Self, SdpBody)> {
        let parsed_offer = parse_audio_sdp(offer).context("parse caller offer")?;
        let caller_route_target = caller_route_target.unwrap_or(parsed_offer.remote_rtp);
        let callee_route_target = callee_route_target.unwrap_or(parsed_offer.remote_rtp);
        let caller_advertise_ip = advertise_ip_for(caller_route_target)?;
        let callee_advertise_ip = advertise_ip_for(callee_route_target)?;
        info!(
            %caller_route_target,
            %callee_route_target,
            %caller_advertise_ip,
            %callee_advertise_ip,
            "media relay route selected"
        );
        let caller_rtp = Arc::new(bind_socket().await?);
        let caller_rtcp = Arc::new(bind_socket().await?);
        let callee_rtp = Arc::new(bind_socket().await?);
        let callee_rtcp = Arc::new(bind_socket().await?);
        let callee_relay = RelayEndpoint {
            rtp: SocketAddr::new(callee_advertise_ip, callee_rtp.local_addr()?.port()),
            rtcp: Some(SocketAddr::new(
                callee_advertise_ip,
                callee_rtcp.local_addr()?.port(),
            )),
        };
        let prepared =
            rewrite_audio_sdp(offer, callee_relay).context("rewrite caller offer for callee")?;
        let caller_remote_rtp = Arc::new(RwLock::new(Some(prepared.parsed.remote_rtp)));
        let caller_remote_rtcp = Arc::new(RwLock::new(prepared.parsed.remote_rtcp));
        let callee_remote_rtp = Arc::new(RwLock::new(None));
        let callee_remote_rtcp = Arc::new(RwLock::new(None));
        let counters = Arc::new(RelayCounters::default());
        let dtmf_counters = Arc::new(DtmfObservationCounters::default());
        let recording_tap = Arc::new(StdRwLock::new(None));
        let ai_tap = Arc::new(StdRwLock::new(None));
        let (stop_tx, stop_rx) = watch::channel(false);
        let (observation_tx, observation_rx) = mpsc::channel(64);
        let (dtmf_observation_tx, dtmf_observation_rx) =
            mpsc::channel(DTMF_OBSERVATION_QUEUE_CAPACITY);
        let (caller_dtmf_config, caller_dtmf_rx) = watch::channel(ParserConfig::transparent(
            0,
            parsed_offer.telephone_event.clone(),
        ));
        let (callee_dtmf_config, callee_dtmf_rx) = watch::channel(ParserConfig::disabled(0));
        let (dtmf_dispatch, dtmf_dispatch_rx) = watch::channel(None);
        let dtmf_capabilities = Arc::new(Mutex::new(DtmfCapabilityRuntimeState::default()));
        let dtmf_capability_ops = Arc::new(Mutex::new(()));
        let caller_dtmf_applied = Arc::new(AtomicU64::new(0));
        let callee_dtmf_applied = Arc::new(AtomicU64::new(0));
        let caller_dtmf_notify = Arc::new(Notify::new());
        let callee_dtmf_notify = Arc::new(Notify::new());
        let callee_advertise_ip = Arc::new(RwLock::new(callee_advertise_ip));
        let fast_path_runtime = Arc::new(FastPathRuntime {
            controller: fast_path,
            state: StdMutex::new(FastPathRuntimeState::default()),
            caller_rtp: caller_rtp.clone(),
            caller_rtcp: caller_rtcp.clone(),
            callee_rtp: callee_rtp.clone(),
            callee_rtcp: callee_rtcp.clone(),
            caller_remote_rtp: caller_remote_rtp.clone(),
            caller_remote_rtcp: caller_remote_rtcp.clone(),
            callee_remote_rtp: callee_remote_rtp.clone(),
            callee_remote_rtcp: callee_remote_rtcp.clone(),
            caller_advertise_ip,
            callee_advertise_ip,
        });

        // Each relay receives on one socket and sends via the *other* side's
        // socket so the peer sees RTP from the port advertised in its SDP.
        let tasks = vec![
            spawn_relay(
                RelayTaskSpec {
                    recv_socket: caller_rtp.clone(),
                    send_socket: callee_rtp.clone(),
                    source_endpoint: caller_remote_rtp.clone(),
                    target: callee_remote_rtp.clone(),
                    counters: counters.clone(),
                    direction: RelayDirection::CallerToCalleeRtp,
                    endpoint_slot: RemoteEndpointSlot::CallerRtp,
                    observation_tx: observation_tx.clone(),
                    recording: Some((recording_tap.clone(), RecordingSide::Caller)),
                    ai_tap: Some((ai_tap.clone(), AiTapSide::Caller)),
                    dtmf_config: Some(caller_dtmf_rx),
                    dtmf_policy_applied: Some((
                        caller_dtmf_applied.clone(),
                        caller_dtmf_notify.clone(),
                    )),
                    dtmf_observation_tx: Some(dtmf_observation_tx.clone()),
                    dtmf_counters: dtmf_counters.clone(),
                },
                stop_rx.clone(),
            ),
            spawn_relay(
                RelayTaskSpec {
                    recv_socket: callee_rtp.clone(),
                    send_socket: caller_rtp.clone(),
                    source_endpoint: callee_remote_rtp.clone(),
                    target: caller_remote_rtp.clone(),
                    counters: counters.clone(),
                    direction: RelayDirection::CalleeToCallerRtp,
                    endpoint_slot: RemoteEndpointSlot::CalleeRtp,
                    observation_tx: observation_tx.clone(),
                    recording: Some((recording_tap.clone(), RecordingSide::Callee)),
                    ai_tap: Some((ai_tap.clone(), AiTapSide::Callee)),
                    dtmf_config: Some(callee_dtmf_rx),
                    dtmf_policy_applied: Some((
                        callee_dtmf_applied.clone(),
                        callee_dtmf_notify.clone(),
                    )),
                    dtmf_observation_tx: Some(dtmf_observation_tx),
                    dtmf_counters: dtmf_counters.clone(),
                },
                stop_rx.clone(),
            ),
            spawn_relay(
                RelayTaskSpec {
                    recv_socket: caller_rtcp.clone(),
                    send_socket: callee_rtcp.clone(),
                    source_endpoint: caller_remote_rtcp.clone(),
                    target: callee_remote_rtcp.clone(),
                    counters: counters.clone(),
                    direction: RelayDirection::CallerToCalleeRtcp,
                    endpoint_slot: RemoteEndpointSlot::CallerRtcp,
                    observation_tx: observation_tx.clone(),
                    recording: None,
                    ai_tap: None,
                    dtmf_config: None,
                    dtmf_policy_applied: None,
                    dtmf_observation_tx: None,
                    dtmf_counters: dtmf_counters.clone(),
                },
                stop_rx.clone(),
            ),
            spawn_relay(
                RelayTaskSpec {
                    recv_socket: callee_rtcp.clone(),
                    send_socket: caller_rtcp.clone(),
                    source_endpoint: callee_remote_rtcp.clone(),
                    target: caller_remote_rtcp.clone(),
                    counters: counters.clone(),
                    direction: RelayDirection::CalleeToCallerRtcp,
                    endpoint_slot: RemoteEndpointSlot::CalleeRtcp,
                    observation_tx,
                    recording: None,
                    ai_tap: None,
                    dtmf_config: None,
                    dtmf_policy_applied: None,
                    dtmf_observation_tx: None,
                    dtmf_counters: dtmf_counters.clone(),
                },
                stop_rx.clone(),
            ),
            spawn_endpoint_monitor(fast_path_runtime.clone(), observation_rx, stop_rx),
            spawn_dtmf_monitor(
                dtmf_observation_rx,
                dtmf_counters.clone(),
                dtmf_dispatch_rx,
                stop_tx.subscribe(),
            ),
        ];

        let stopped = Arc::new(AtomicBool::new(false));
        Ok((
            Self {
                caller_rtp,
                caller_rtcp,
                callee_rtp,
                caller_remote_rtp,
                callee_remote_rtp,
                callee_remote_rtcp,
                caller_advertise_ip,
                counters,
                dtmf_counters,
                caller_dtmf_config,
                callee_dtmf_config,
                dtmf_dispatch,
                dtmf_capabilities,
                dtmf_capability_ops,
                caller_dtmf_applied,
                callee_dtmf_applied,
                caller_dtmf_notify,
                callee_dtmf_notify,
                recording_tap,
                ai_tap,
                recording: Arc::new(Mutex::new(None)),
                stopped,
                fast_path_runtime,
                stop_tx,
                tasks,
            },
            prepared.body,
        ))
    }

    pub fn handle(&self) -> MediaBridgeHandle {
        MediaBridgeHandle {
            caller_rtp: self.caller_rtp.clone(),
            caller_rtcp: self.caller_rtcp.clone(),
            caller_remote_rtp: self.caller_remote_rtp.clone(),
            callee_remote_rtp: self.callee_remote_rtp.clone(),
            callee_remote_rtcp: self.callee_remote_rtcp.clone(),
            caller_advertise_ip: self.caller_advertise_ip,
            recording_tap: self.recording_tap.clone(),
            ai_tap: self.ai_tap.clone(),
            recording: self.recording.clone(),
            stopped: self.stopped.clone(),
            fast_path_runtime: self.fast_path_runtime.clone(),
            caller_dtmf_config: self.caller_dtmf_config.clone(),
            callee_dtmf_config: self.callee_dtmf_config.clone(),
            dtmf_dispatch: self.dtmf_dispatch.clone(),
            dtmf_capabilities: self.dtmf_capabilities.clone(),
            dtmf_capability_ops: self.dtmf_capability_ops.clone(),
            caller_dtmf_applied: self.caller_dtmf_applied.clone(),
            callee_dtmf_applied: self.callee_dtmf_applied.clone(),
            caller_dtmf_notify: self.caller_dtmf_notify.clone(),
            callee_dtmf_notify: self.callee_dtmf_notify.clone(),
        }
    }

    #[cfg(test)]
    async fn prepare_caller_sdp(&self, answer: &SdpBody, generation: u64) -> Result<SdpBody> {
        self.handle().prepare_caller_sdp(answer, generation).await
    }

    #[cfg(test)]
    async fn try_promote_fast_path(&self, bridge_id: &str, allowed: bool) {
        self.handle()
            .try_promote_fast_path(bridge_id, allowed)
            .await;
    }

    pub async fn stop(mut self, call_id: &str) -> MediaFinalized {
        self.stopped.store(true, Ordering::Release);
        let (fast_path_stats, forwarding_history) = self.fast_path_runtime.stop(call_id);
        *self
            .recording_tap
            .write()
            .expect("recording tap lock poisoned") = None;
        let ai_capture = self
            .ai_tap
            .write()
            .expect("AI tap lock poisoned")
            .take()
            .map(|tap| tap.finish());
        let _ = self.stop_tx.send(true);
        for task in self.tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }
        let mut stats = self.stats();
        stats.caller_to_callee_packets = stats
            .caller_to_callee_packets
            .saturating_add(fast_path_stats.caller_to_callee_packets);
        stats.caller_to_callee_bytes = stats
            .caller_to_callee_bytes
            .saturating_add(fast_path_stats.caller_to_callee_bytes);
        stats.callee_to_caller_packets = stats
            .callee_to_caller_packets
            .saturating_add(fast_path_stats.callee_to_caller_packets);
        stats.callee_to_caller_bytes = stats
            .callee_to_caller_bytes
            .saturating_add(fast_path_stats.callee_to_caller_bytes);
        stats.caller_to_callee_rtcp_packets = stats
            .caller_to_callee_rtcp_packets
            .saturating_add(fast_path_stats.caller_to_callee_rtcp_packets);
        stats.callee_to_caller_rtcp_packets = stats
            .callee_to_caller_rtcp_packets
            .saturating_add(fast_path_stats.callee_to_caller_rtcp_packets);
        info!(
            call_id,
            caller_to_callee_packets = stats.caller_to_callee_packets,
            caller_to_callee_bytes = stats.caller_to_callee_bytes,
            callee_to_caller_packets = stats.callee_to_caller_packets,
            callee_to_caller_bytes = stats.callee_to_caller_bytes,
            caller_to_callee_rtcp_packets = stats.caller_to_callee_rtcp_packets,
            callee_to_caller_rtcp_packets = stats.callee_to_caller_rtcp_packets,
            dtmf_completed = stats.dtmf.completed,
            dtmf_incomplete = stats.dtmf.incomplete,
            dtmf_invalid = stats.dtmf.invalid,
            dtmf_delivery_fault = stats.dtmf.delivery_fault,
            caller_rtp = %self.caller_rtp.local_addr().unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0))),
            callee_rtp = %self.callee_rtp.local_addr().unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0))),
            "media bridge stopped"
        );
        let recording = match self.recording.lock().await.take() {
            Some(recording) => Some(recording.finish().await),
            None => None,
        };
        let forwarding_mode = forwarding_history.effective_mode(fast_path_stats);
        MediaFinalized {
            stats,
            recording,
            forwarding_mode,
            ai_capture,
        }
    }

    pub fn stats(&self) -> MediaStatsSnapshot {
        MediaStatsSnapshot {
            caller_to_callee_packets: self
                .counters
                .caller_to_callee_packets
                .load(Ordering::Relaxed),
            caller_to_callee_bytes: self.counters.caller_to_callee_bytes.load(Ordering::Relaxed),
            callee_to_caller_packets: self
                .counters
                .callee_to_caller_packets
                .load(Ordering::Relaxed),
            callee_to_caller_bytes: self.counters.callee_to_caller_bytes.load(Ordering::Relaxed),
            caller_to_callee_rtcp_packets: self
                .counters
                .caller_to_callee_rtcp_packets
                .load(Ordering::Relaxed),
            callee_to_caller_rtcp_packets: self
                .counters
                .callee_to_caller_rtcp_packets
                .load(Ordering::Relaxed),
            dtmf: dtmf_stats(&self.dtmf_counters),
        }
    }
}

impl MediaBridgeHandle {
    pub(crate) async fn send_tts_rtp(
        &self,
        payload: &[u8],
        payload_type: u8,
        timestamp: u32,
        sequence: u16,
        ssrc: u32,
    ) -> Result<usize> {
        if self.stopped.load(Ordering::Acquire) {
            bail!("call_or_leg_terminating");
        }
        let remote = self
            .caller_remote_rtp
            .read()
            .await
            .as_ref()
            .copied()
            .context("caller RTP remote is not ready")?;
        let mut packet = Vec::with_capacity(12 + payload.len());
        packet.extend_from_slice(&[0x80, payload_type & 0x7f]);
        packet.extend_from_slice(&sequence.to_be_bytes());
        packet.extend_from_slice(&timestamp.to_be_bytes());
        packet.extend_from_slice(&ssrc.to_be_bytes());
        packet.extend_from_slice(payload);
        self.caller_rtp
            .send_to(&packet, remote)
            .await
            .context("send TTS RTP packet")
    }

    pub(crate) fn bind_dtmf_sessions(&self, bindings: DtmfSessionBindings) {
        self.dtmf_dispatch.send_replace(Some(DtmfDispatchBindings {
            domain_id: DomainId::from(bindings.domain_id),
            call_id: CallId::from(bindings.call_id),
            caller: DtmfDispatchTarget {
                session_id: SessionId::from(bindings.caller_session_id),
                control_tx: bindings.caller_control_tx,
            },
            callee: DtmfDispatchTarget {
                session_id: SessionId::from(bindings.callee_session_id),
                control_tx: bindings.callee_control_tx,
            },
            dispatcher: bindings.dispatcher,
        }));
    }

    pub(crate) fn update_dtmf_callee_target(
        &self,
        session_id: String,
        control_tx: mpsc::Sender<ControlMessage>,
    ) {
        self.dtmf_dispatch.send_modify(|bindings| {
            if let Some(bindings) = bindings {
                bindings.callee = DtmfDispatchTarget {
                    session_id: SessionId::from(session_id),
                    control_tx,
                };
            }
        });
    }

    pub(crate) async fn acquire_dtmf_capability(
        &self,
        request: DtmfCapabilityLeaseRequest,
    ) -> Result<DtmfCapabilityReady> {
        let _operation = self.dtmf_capability_ops.lock().await;
        if self.stopped.load(Ordering::Acquire) {
            bail!("call_or_leg_terminating");
        }
        if request.mode == DtmfMediaMode::Transparent {
            bail!("invalid_dtmf_capability_mode");
        }
        let direction = self.dtmf_direction(&request.source_session_id)?;
        let sender = self.dtmf_config(direction);
        {
            let state = self.dtmf_capabilities.lock().await;
            if let Some(existing) = state.leases.get(&request.lease_id) {
                if existing.request != request {
                    bail!("media_capability_lease_id_collision");
                }
                if !existing.active {
                    bail!("media_capability_lease_released");
                }
                return Ok(DtmfCapabilityReady {
                    lease_id: request.lease_id,
                    source_session_id: request.source_session_id,
                    mode: request.mode,
                    media_generation: existing.ready_generation,
                });
            }
        }
        let current = sender.borrow().clone();
        // Capability operations are serialized and may legitimately advance the
        // per-direction parser generation between two requests issued by the
        // same call-actor generation. A request from a future actor generation
        // is invalid; an older parser generation is rebased onto current state.
        if request.requested_generation > current.generation {
            bail!("stale_media_generation");
        }
        if current.capability.is_none() {
            bail!("dtmf_not_negotiated");
        }

        let mut state = self.dtmf_capabilities.lock().await;
        if request.mode == DtmfMediaMode::Collect
            && state.leases.values().any(|lease| {
                lease.active
                    && lease.direction == direction
                    && lease.request.mode == DtmfMediaMode::Collect
            })
        {
            bail!("dtmf_collector_conflict");
        }

        self.fast_path_runtime
            .block_for_dtmf_capability()
            .map_err(|error| {
                anyhow::anyhow!(
                    "capability_demotion_failed:{}:{}",
                    error.code,
                    error.message
                )
            })?;

        let desired_mode = state
            .leases
            .values()
            .filter(|lease| lease.active && lease.direction == direction)
            .map(|lease| lease.request.mode)
            .chain(std::iter::once(request.mode))
            .max_by_key(|mode| dtmf_mode_priority(*mode))
            .unwrap_or(DtmfMediaMode::Transparent);
        let ready_generation = set_dtmf_mode(sender, desired_mode);
        state.leases.insert(
            request.lease_id.clone(),
            DtmfCapabilityLease {
                request: request.clone(),
                direction,
                ready_generation,
                active: true,
                released_generation: None,
            },
        );
        drop(state);
        if let Err(error) = self
            .wait_dtmf_policy_applied(direction, ready_generation)
            .await
        {
            let mut state = self.dtmf_capabilities.lock().await;
            state.leases.remove(&request.lease_id);
            let fallback_mode = state
                .leases
                .values()
                .filter(|lease| lease.active && lease.direction == direction)
                .map(|lease| lease.request.mode)
                .max_by_key(|mode| dtmf_mode_priority(*mode))
                .unwrap_or(DtmfMediaMode::Transparent);
            set_dtmf_mode(sender, fallback_mode);
            return Err(error);
        }
        info!(
            lease_id = %request.lease_id,
            owner = %request.owner,
            source_session_id = %request.source_session_id,
            mode = ?request.mode,
            requested_generation = request.requested_generation,
            ready_generation,
            "DTMF media capability ready"
        );
        Ok(DtmfCapabilityReady {
            lease_id: request.lease_id,
            source_session_id: request.source_session_id,
            mode: request.mode,
            media_generation: ready_generation,
        })
    }

    pub(crate) async fn release_dtmf_capability(
        &self,
        lease_id: &MediaCapabilityLeaseId,
    ) -> Result<u64> {
        let _operation = self.dtmf_capability_ops.lock().await;
        let mut state = self.dtmf_capabilities.lock().await;
        let Some(existing) = state.leases.get(lease_id) else {
            bail!("media_capability_lease_not_found");
        };
        if !existing.active {
            return Ok(existing
                .released_generation
                .unwrap_or(existing.ready_generation));
        }
        let direction = existing.direction;
        let owner = existing.request.owner.clone();
        let source_session_id = existing.request.source_session_id.clone();
        if let Some(existing) = state.leases.get_mut(lease_id) {
            existing.active = false;
        }
        let mut desired_mode = state
            .leases
            .values()
            .filter(|lease| lease.active && lease.direction == direction)
            .map(|lease| lease.request.mode)
            .max_by_key(|mode| dtmf_mode_priority(*mode))
            .unwrap_or(DtmfMediaMode::Transparent);
        let drain_collect_retransmissions = self.dtmf_config(direction).borrow().mode
            == DtmfMediaMode::Collect
            && desired_mode != DtmfMediaMode::Collect;
        if drain_collect_retransmissions {
            drop(state);
            tokio::time::sleep(DTMF_COLLECT_RELEASE_DRAIN).await;
            state = self.dtmf_capabilities.lock().await;
            desired_mode = state
                .leases
                .values()
                .filter(|lease| lease.active && lease.direction == direction)
                .map(|lease| lease.request.mode)
                .max_by_key(|mode| dtmf_mode_priority(*mode))
                .unwrap_or(DtmfMediaMode::Transparent);
        }
        let released_generation = set_dtmf_mode(self.dtmf_config(direction), desired_mode);
        if let Some(existing) = state.leases.get_mut(lease_id) {
            existing.released_generation = Some(released_generation);
        }
        drop(state);
        self.wait_dtmf_policy_applied(direction, released_generation)
            .await?;
        info!(
            lease_id = %lease_id,
            owner = %owner,
            source_session_id = %source_session_id,
            released_generation,
            remaining_mode = ?desired_mode,
            "DTMF media capability released"
        );
        Ok(released_generation)
    }

    fn dtmf_direction(&self, source_session_id: &SessionId) -> Result<DtmfRelayDirection> {
        let bindings = self.dtmf_dispatch.borrow();
        let Some(bindings) = bindings.as_ref() else {
            bail!("dtmf_session_bindings_unavailable");
        };
        if bindings.caller.session_id == *source_session_id {
            Ok(DtmfRelayDirection::CallerToCallee)
        } else if bindings.callee.session_id == *source_session_id {
            Ok(DtmfRelayDirection::CalleeToCaller)
        } else {
            bail!("stale_source_session");
        }
    }

    fn dtmf_config(&self, direction: DtmfRelayDirection) -> &watch::Sender<ParserConfig> {
        match direction {
            DtmfRelayDirection::CallerToCallee => &self.caller_dtmf_config,
            DtmfRelayDirection::CalleeToCaller => &self.callee_dtmf_config,
        }
    }

    async fn wait_dtmf_policy_applied(
        &self,
        direction: DtmfRelayDirection,
        generation: u64,
    ) -> Result<()> {
        let (applied, notify) = match direction {
            DtmfRelayDirection::CallerToCallee => {
                (&self.caller_dtmf_applied, &self.caller_dtmf_notify)
            }
            DtmfRelayDirection::CalleeToCaller => {
                (&self.callee_dtmf_applied, &self.callee_dtmf_notify)
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if applied.load(Ordering::Acquire) >= generation {
                    return;
                }
                notify.notified().await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("dtmf_policy_apply_timeout"))?;
        Ok(())
    }

    pub async fn prepare_callee_sdp(
        &self,
        offer: &SdpBody,
        route_target: Option<SocketAddr>,
        generation: u64,
    ) -> Result<SdpBody> {
        let parsed = parse_audio_sdp(offer).context("parse caller offer for callee attempt")?;
        let route_target = route_target.unwrap_or(parsed.remote_rtp);
        let advertise_ip = advertise_ip_for(route_target)?;
        *self.fast_path_runtime.callee_advertise_ip.write().await = advertise_ip;
        *self.callee_remote_rtp.write().await = None;
        *self.callee_remote_rtcp.write().await = None;
        update_dtmf_negotiation(&self.caller_dtmf_config, parsed.telephone_event, generation);
        update_dtmf_negotiation(&self.callee_dtmf_config, None, generation);
        let relay = RelayEndpoint {
            rtp: SocketAddr::new(
                advertise_ip,
                self.fast_path_runtime.callee_rtp.local_addr()?.port(),
            ),
            rtcp: Some(SocketAddr::new(
                advertise_ip,
                self.fast_path_runtime.callee_rtcp.local_addr()?.port(),
            )),
        };
        let prepared =
            rewrite_audio_sdp(offer, relay).context("rewrite caller offer for callee attempt")?;
        info!(
            %route_target,
            %advertise_ip,
            "media relay route updated for callee attempt"
        );
        Ok(prepared.body)
    }

    pub async fn prepare_caller_sdp(&self, answer: &SdpBody, generation: u64) -> Result<SdpBody> {
        let relay = RelayEndpoint {
            rtp: SocketAddr::new(
                self.caller_advertise_ip,
                self.caller_rtp.local_addr()?.port(),
            ),
            rtcp: Some(SocketAddr::new(
                self.caller_advertise_ip,
                self.caller_rtcp.local_addr()?.port(),
            )),
        };
        let prepared =
            rewrite_audio_sdp(answer, relay).context("rewrite callee answer for caller")?;
        *self.callee_remote_rtp.write().await = Some(prepared.parsed.remote_rtp);
        *self.callee_remote_rtcp.write().await = prepared.parsed.remote_rtcp;
        let telephone_event = prepared.parsed.telephone_event;
        update_dtmf_negotiation(
            &self.caller_dtmf_config,
            telephone_event.clone(),
            generation,
        );
        update_dtmf_negotiation(&self.callee_dtmf_config, telephone_event, generation);
        Ok(prepared.body)
    }

    pub async fn start_recording(&self, spec: RecordingSpec) -> Result<()> {
        let mut slot = self.recording.lock().await;
        if self.stopped.load(Ordering::Acquire) {
            bail!("media bridge already stopped");
        }
        if slot.is_some() {
            return Ok(());
        }
        let recording = tokio::task::spawn_blocking(move || RecordingSession::start(spec))
            .await
            .context("recording start worker failed")??;
        *self
            .recording_tap
            .write()
            .expect("recording tap lock poisoned") = Some(recording.tap_sender());
        *slot = Some(recording);
        Ok(())
    }

    pub(crate) fn enable_ai_tap(&self, tap: AiMediaTapSender) -> Result<()> {
        if self.stopped.load(Ordering::Acquire) {
            bail!("media bridge already stopped");
        }
        self.fast_path_runtime
            .block_for_ai_tap()
            .map_err(|error| anyhow::anyhow!("{}: {}", error.code, error.message))?;
        let mut slot = self.ai_tap.write().expect("AI tap lock poisoned");
        if slot.is_some() {
            bail!("AI media tap already enabled");
        }
        *slot = Some(tap);
        Ok(())
    }

    pub async fn try_promote_fast_path(&self, bridge_id: &str, allowed: bool) {
        if !allowed {
            return;
        }
        self.fast_path_runtime.promote(bridge_id).await;
    }

    pub async fn caller_remote(&self) -> Option<SocketAddr> {
        *self.caller_remote_rtp.read().await
    }

    pub async fn callee_remote(&self) -> Option<SocketAddr> {
        *self.callee_remote_rtp.read().await
    }
}

impl FastPathRuntime {
    fn block_for_ai_tap(&self) -> std::result::Result<(), FastPathError> {
        self.block_for_userspace_capability(FastPathFallbackReason::AiMediaTap, "AI media tap")
    }

    fn block_for_dtmf_capability(&self) -> std::result::Result<(), FastPathError> {
        self.block_for_userspace_capability(
            FastPathFallbackReason::DtmfCapability,
            "DTMF media capability",
        )
    }

    fn block_for_userspace_capability(
        &self,
        reason: FastPathFallbackReason,
        capability: &str,
    ) -> std::result::Result<(), FastPathError> {
        let mut state = self.state.lock().expect("fast path runtime lock poisoned");
        if state.stopped {
            return Err(FastPathError {
                code: "media_bridge_stopped".to_string(),
                message: "media bridge is already stopped".to_string(),
            });
        }
        state.promotion_blocked = true;
        let (Some(bridge_id), Some(generation)) =
            (state.bridge_id.clone(), state.active_generation)
        else {
            return Ok(());
        };
        let stats = self.controller.demote(&bridge_id, generation, reason)?;
        state.active_generation = None;
        state.accumulated_stats.merge_from(stats);
        state
            .forwarding_history
            .mark_demoted(MediaFlowDirection::CallerToCallee);
        state
            .forwarding_history
            .mark_demoted(MediaFlowDirection::CalleeToCaller);
        info!(
            bridge_id,
            generation, capability, "fast path demoted for userspace media capability"
        );
        Ok(())
    }

    async fn promote(&self, bridge_id: &str) {
        if self.controller.availability() != FastPathAvailability::Available {
            return;
        }
        let Some(flows) = self.current_flows().await else {
            debug!(
                bridge_id,
                "RTP/RTCP fast path skipped for incomplete or non-IPv4 media"
            );
            return;
        };
        let mut state = self.state.lock().expect("fast path runtime lock poisoned");
        if state.stopped || state.promotion_blocked || state.active_generation.is_some() {
            return;
        }
        match &state.bridge_id {
            Some(current) if current != bridge_id => return,
            None => state.bridge_id = Some(bridge_id.to_string()),
            Some(_) => {}
        }
        self.promote_locked(&mut state, flows);
    }

    async fn confirm_rebind(&self, slot: RemoteEndpointSlot, source: SocketAddr) {
        let endpoint = self.remote_endpoint(slot);
        let old = {
            let mut current = endpoint.write().await;
            if *current == Some(source) {
                return;
            }
            let old = *current;
            *current = Some(source);
            old
        };
        let flows = self.current_flows().await;
        let mut state = self.state.lock().expect("fast path runtime lock poisoned");
        if state.stopped {
            return;
        }
        let Some(bridge_id) = state.bridge_id.clone() else {
            return;
        };
        if let Some(generation) = state.active_generation {
            match self.controller.demote(
                &bridge_id,
                generation,
                FastPathFallbackReason::RemoteEndpointChanged,
            ) {
                Ok(stats) => {
                    state.active_generation = None;
                    state.accumulated_stats.merge_from(stats);
                }
                Err(err) => {
                    state.forwarding_history.mark_demoted(slot.direction());
                    warn!(
                        bridge_id,
                        generation,
                        code = err.code,
                        error = err.message,
                        "demote fast path after media endpoint rebind failed"
                    );
                    return;
                }
            }
            state.forwarding_history.mark_demoted(slot.direction());
        }
        info!(
            bridge_id,
            endpoint = ?slot,
            previous = ?old,
            current = %source,
            generation = state.generation,
            "media endpoint NAT rebind confirmed"
        );
        if let Some(flows) = flows {
            self.promote_locked(&mut state, flows);
        }
    }

    fn promote_locked(&self, state: &mut FastPathRuntimeState, flows: Vec<FastPathFlowSpec>) {
        if state.promotion_blocked {
            return;
        }
        let Some(bridge_id) = state.bridge_id.clone() else {
            return;
        };
        let generation = state.generation.saturating_add(1);
        let spec = FastPathBridgeSpec {
            bridge_id: bridge_id.clone(),
            generation,
            flows,
        };
        match self.controller.promote(&spec) {
            Ok(()) => {
                state.generation = generation;
                state.active_generation = Some(generation);
                state
                    .forwarding_history
                    .mark_promoted(MediaFlowDirection::CallerToCallee);
                state
                    .forwarding_history
                    .mark_promoted(MediaFlowDirection::CalleeToCaller);
            }
            Err(err) => warn!(
                bridge_id,
                generation,
                code = err.code,
                error = err.message,
                "RTP/RTCP fast path promotion failed; using userspace relay"
            ),
        }
    }

    fn check_redirect_health(&self) {
        let mut state = self.state.lock().expect("fast path runtime lock poisoned");
        if state.stopped || (state.promotion_blocked && state.active_generation.is_none()) {
            return;
        }
        let (Some(bridge_id), Some(generation)) =
            (state.bridge_id.clone(), state.active_generation)
        else {
            return;
        };
        match self.controller.snapshot(&bridge_id, generation) {
            Ok(stats) if max_redirect_errors(stats) >= FAST_PATH_REDIRECT_ERROR_THRESHOLD => {
                self.demote_and_block_locked(
                    &mut state,
                    &bridge_id,
                    generation,
                    FastPathFallbackReason::RedirectErrors,
                    "redirect error threshold reached",
                );
            }
            Ok(_) => {}
            Err(err) => {
                warn!(
                    bridge_id,
                    generation,
                    code = err.code,
                    error = err.message,
                    "fast path stats snapshot failed; demoting to userspace"
                );
                self.demote_and_block_locked(
                    &mut state,
                    &bridge_id,
                    generation,
                    FastPathFallbackReason::ControllerFailure("stats_snapshot_failed".to_string()),
                    "stats snapshot failed",
                );
            }
        }
    }

    fn demote_and_block_locked(
        &self,
        state: &mut FastPathRuntimeState,
        bridge_id: &str,
        generation: u64,
        reason: FastPathFallbackReason,
        message: &str,
    ) {
        if state.active_generation != Some(generation) {
            return;
        }
        match self.controller.demote(bridge_id, generation, reason) {
            Ok(stats) => {
                state.active_generation = None;
                state.accumulated_stats.merge_from(stats);
            }
            Err(err) => {
                warn!(
                    bridge_id,
                    generation,
                    code = err.code,
                    error = err.message,
                    "automatic fast path demotion failed; will retry"
                );
            }
        }
        state
            .forwarding_history
            .mark_demoted(MediaFlowDirection::CallerToCallee);
        state
            .forwarding_history
            .mark_demoted(MediaFlowDirection::CalleeToCaller);
        state.promotion_blocked = true;
        warn!(
            bridge_id,
            generation,
            reason = message,
            "fast path automatically demoted; userspace locked for call"
        );
    }

    fn stop(&self, call_id: &str) -> (FastPathStats, MediaForwardingHistory) {
        let mut state = self.state.lock().expect("fast path runtime lock poisoned");
        state.stopped = true;
        if let (Some(bridge_id), Some(generation)) =
            (state.bridge_id.clone(), state.active_generation.take())
        {
            match self.controller.remove(&bridge_id, generation) {
                Ok(stats) => state.accumulated_stats.merge_from(stats),
                Err(err) => warn!(
                    call_id,
                    generation,
                    code = err.code,
                    error = err.message,
                    "remove RTP/RTCP fast path rules failed"
                ),
            }
        }
        (state.accumulated_stats, state.forwarding_history)
    }

    fn stop_without_log(&self) {
        let mut state = self.state.lock().expect("fast path runtime lock poisoned");
        if state.stopped {
            return;
        }
        state.stopped = true;
        if let (Some(bridge_id), Some(generation)) =
            (state.bridge_id.clone(), state.active_generation.take())
        {
            let _ = self.controller.remove(&bridge_id, generation);
        }
    }

    async fn current_flows(&self) -> Option<Vec<FastPathFlowSpec>> {
        let (
            IpAddr::V4(caller_local_ip),
            IpAddr::V4(callee_local_ip),
            Some(SocketAddr::V4(caller_remote_rtp)),
            Some(SocketAddr::V4(caller_remote_rtcp)),
            Some(SocketAddr::V4(callee_remote_rtp)),
            Some(SocketAddr::V4(callee_remote_rtcp)),
        ) = (
            self.caller_advertise_ip,
            *self.callee_advertise_ip.read().await,
            *self.caller_remote_rtp.read().await,
            *self.caller_remote_rtcp.read().await,
            *self.callee_remote_rtp.read().await,
            *self.callee_remote_rtcp.read().await,
        )
        else {
            return None;
        };
        let caller = FastPathLegEndpoints {
            local_rtp: std::net::SocketAddrV4::new(
                caller_local_ip,
                self.caller_rtp.local_addr().ok()?.port(),
            ),
            remote_rtp: caller_remote_rtp,
            local_rtcp: std::net::SocketAddrV4::new(
                caller_local_ip,
                self.caller_rtcp.local_addr().ok()?.port(),
            ),
            remote_rtcp: caller_remote_rtcp,
        };
        let callee = FastPathLegEndpoints {
            local_rtp: std::net::SocketAddrV4::new(
                callee_local_ip,
                self.callee_rtp.local_addr().ok()?.port(),
            ),
            remote_rtp: callee_remote_rtp,
            local_rtcp: std::net::SocketAddrV4::new(
                callee_local_ip,
                self.callee_rtcp.local_addr().ok()?.port(),
            ),
            remote_rtcp: callee_remote_rtcp,
        };
        Some(fast_path_flows(caller, callee))
    }

    fn remote_endpoint(&self, slot: RemoteEndpointSlot) -> &Arc<RwLock<Option<SocketAddr>>> {
        match slot {
            RemoteEndpointSlot::CallerRtp => &self.caller_remote_rtp,
            RemoteEndpointSlot::CallerRtcp => &self.caller_remote_rtcp,
            RemoteEndpointSlot::CalleeRtp => &self.callee_remote_rtp,
            RemoteEndpointSlot::CalleeRtcp => &self.callee_remote_rtcp,
        }
    }
}

impl Drop for MediaBridge {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        self.fast_path_runtime.stop_without_log();
        let _ = self.stop_tx.send(true);
        for task in &self.tasks {
            task.abort();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FastPathLegEndpoints {
    local_rtp: std::net::SocketAddrV4,
    remote_rtp: std::net::SocketAddrV4,
    local_rtcp: std::net::SocketAddrV4,
    remote_rtcp: std::net::SocketAddrV4,
}

fn fast_path_flows(
    caller: FastPathLegEndpoints,
    callee: FastPathLegEndpoints,
) -> Vec<FastPathFlowSpec> {
    vec![
        FastPathFlowSpec {
            media_kind: FastPathMediaKind::Rtp,
            direction: MediaFlowDirection::CallerToCallee,
            local: caller.local_rtp,
            remote: caller.remote_rtp,
            rewritten_source: callee.local_rtp,
            rewritten_destination: callee.remote_rtp,
        },
        FastPathFlowSpec {
            media_kind: FastPathMediaKind::Rtp,
            direction: MediaFlowDirection::CalleeToCaller,
            local: callee.local_rtp,
            remote: callee.remote_rtp,
            rewritten_source: caller.local_rtp,
            rewritten_destination: caller.remote_rtp,
        },
        FastPathFlowSpec {
            media_kind: FastPathMediaKind::Rtcp,
            direction: MediaFlowDirection::CallerToCallee,
            local: caller.local_rtcp,
            remote: caller.remote_rtcp,
            rewritten_source: callee.local_rtcp,
            rewritten_destination: callee.remote_rtcp,
        },
        FastPathFlowSpec {
            media_kind: FastPathMediaKind::Rtcp,
            direction: MediaFlowDirection::CalleeToCaller,
            local: callee.local_rtcp,
            remote: callee.remote_rtcp,
            rewritten_source: caller.local_rtcp,
            rewritten_destination: caller.remote_rtcp,
        },
    ]
}

fn spawn_relay(spec: RelayTaskSpec, mut stop_rx: watch::Receiver<bool>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut packet = vec![0_u8; 2048];
        let mut dtmf_config = spec.dtmf_config.clone();
        let mut dtmf_parser = dtmf_config
            .as_ref()
            .map(|receiver| Rfc4733Parser::new(receiver.borrow().clone()));
        loop {
            let dtmf_deadline = dtmf_parser.as_ref().and_then(Rfc4733Parser::next_deadline);
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        return;
                    }
                }
                changed = wait_for_dtmf_config(&mut dtmf_config) => {
                    match changed {
                        Some(config) => {
                            let generation = config.generation;
                            if let Some(parser) = dtmf_parser.as_mut() {
                                parser.update_config(config);
                            }
                            if let Some((applied, notify)) = &spec.dtmf_policy_applied {
                                applied.store(generation, Ordering::Release);
                                notify.notify_waiters();
                            }
                        }
                        None => dtmf_config = None,
                    }
                }
                _ = wait_for_dtmf_deadline(dtmf_deadline) => {
                    if let Some(parser) = dtmf_parser.as_mut() {
                        emit_dtmf_observations(&spec, parser.expire(std::time::Instant::now()));
                    }
                }
                received = spec.recv_socket.recv_from(&mut packet) => {
                    let Ok((size, source)) = received else {
                        return;
                    };
                    let source_confirmed = *spec.source_endpoint.read().await == Some(source);
                    if !source_confirmed
                        && valid_media_packet(spec.direction, &packet[..size])
                    {
                        let _ = spec.observation_tx.try_send(RemoteEndpointObservation {
                            slot: spec.endpoint_slot,
                            source,
                        });
                    }
                    if source_confirmed && let Some(parser) = dtmf_parser.as_mut() {
                        emit_dtmf_observations(
                            &spec,
                            parser.observe_packet(&packet[..size], std::time::Instant::now()),
                        );
                    }
                    if dtmf_parser
                        .as_ref()
                        .is_some_and(|parser| parser.suppresses_packet(&packet[..size]))
                    {
                        continue;
                    }
                    let remote = *spec.target.read().await;
                    let Some(remote) = remote else {
                        continue;
                    };
                    if let Some((tap, side)) = &spec.recording
                        && let Some(sender) = tap
                            .read()
                            .expect("recording tap lock poisoned")
                            .as_ref()
                            .cloned()
                    {
                        sender.tap(*side, &packet[..size], std::time::Instant::now());
                    }
                    if let Some((tap, side)) = &spec.ai_tap
                        && let Some(sender) = tap
                            .read()
                            .expect("AI tap lock poisoned")
                            .as_ref()
                            .cloned()
                    {
                        sender.tap(*side, &packet[..size]);
                    }
                    if spec.send_socket.send_to(&packet[..size], remote).await.is_ok() {
                        count_packet(&spec.counters, spec.direction, size as u64);
                    }
                }
            }
        }
    })
}

async fn wait_for_dtmf_config(
    receiver: &mut Option<watch::Receiver<ParserConfig>>,
) -> Option<ParserConfig> {
    let Some(receiver) = receiver.as_mut() else {
        return std::future::pending().await;
    };
    receiver.changed().await.ok()?;
    Some(receiver.borrow_and_update().clone())
}

async fn wait_for_dtmf_deadline(deadline: Option<std::time::Instant>) {
    let Some(deadline) = deadline else {
        return std::future::pending().await;
    };
    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
}

fn emit_dtmf_observations(spec: &RelayTaskSpec, observations: Vec<ParserObservation>) {
    let Some(sender) = &spec.dtmf_observation_tx else {
        return;
    };
    let direction = match spec.direction {
        RelayDirection::CallerToCalleeRtp => DtmfRelayDirection::CallerToCallee,
        RelayDirection::CalleeToCallerRtp => DtmfRelayDirection::CalleeToCaller,
        RelayDirection::CallerToCalleeRtcp | RelayDirection::CalleeToCallerRtcp => return,
    };
    for observation in observations {
        if sender
            .try_send(DtmfRelayObservation {
                direction,
                observation,
            })
            .is_err()
        {
            spec.dtmf_counters
                .delivery_fault
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn spawn_dtmf_monitor(
    mut observations: mpsc::Receiver<DtmfRelayObservation>,
    counters: Arc<DtmfObservationCounters>,
    dispatch: watch::Receiver<Option<DtmfDispatchBindings>>,
    mut stop_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        return;
                    }
                }
                observed = observations.recv() => {
                    let Some(observed) = observed else {
                        return;
                    };
                    match observed.observation {
                        ParserObservation::Completed(completed) => {
                            counters.completed.fetch_add(1, Ordering::Relaxed);
                            if completed.incomplete_end {
                                counters.incomplete.fetch_add(1, Ordering::Relaxed);
                            }
                            debug!(
                                direction = ?observed.direction,
                                generation = completed.generation,
                                duration_ms = completed.duration_ms,
                                incomplete_end = completed.incomplete_end,
                                "RFC4733 event observed"
                            );
                            let bindings = dispatch.borrow().clone();
                            if let Some(bindings) = bindings {
                                let source = match observed.direction {
                                    DtmfRelayDirection::CallerToCallee => bindings.caller.clone(),
                                    DtmfRelayDirection::CalleeToCaller => bindings.callee.clone(),
                                };
                                let event = DigitEvent {
                                    event_id: DtmfEventId::Rfc4733 {
                                        media_generation: completed.generation,
                                        source_session_id: source.session_id.clone(),
                                        ssrc: completed.ssrc,
                                        timestamp: completed.timestamp,
                                        event_code: completed.event_code,
                                    },
                                    domain_id: bindings.domain_id.clone(),
                                    call_id: bindings.call_id.clone(),
                                    source_session_id: source.session_id.clone(),
                                    source_media_leg_id: None,
                                    digit: completed.digit,
                                    transport: DtmfTransport::Rfc4733,
                                    duration_ms: completed.duration_ms,
                                    observed_at_ms: unix_timestamp_ms(),
                                    source_generation: DtmfSourceGeneration::Media(
                                        completed.generation,
                                    ),
                                    incomplete_end: completed.incomplete_end,
                                };
                                if bindings
                                    .dispatcher
                                    .dispatch_to(
                                        source.session_id.as_str(),
                                        source.control_tx,
                                        ControlMessage::MediaDtmfObserved(event),
                                    )
                                    .await
                                    .is_err()
                                {
                                    counters.delivery_fault.fetch_add(1, Ordering::Relaxed);
                                    debug!(
                                        direction = ?observed.direction,
                                        generation = completed.generation,
                                        "RFC4733 source actor unavailable"
                                    );
                                }
                            }
                        }
                        ParserObservation::Invalid => {
                            counters.invalid.fetch_add(1, Ordering::Relaxed);
                            debug!(
                                direction = ?observed.direction,
                                "invalid RFC4733 packet observed"
                            );
                        }
                    }
                }
            }
        }
    })
}

fn spawn_endpoint_monitor(
    runtime: Arc<FastPathRuntime>,
    mut observations: mpsc::Receiver<RemoteEndpointObservation>,
    mut stop_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut candidates: HashMap<RemoteEndpointSlot, RebindCandidate> = HashMap::new();
        let mut health_interval = tokio::time::interval(FAST_PATH_HEALTH_INTERVAL);
        health_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        return;
                    }
                }
                observed = observations.recv() => {
                    let Some(observed) = observed else {
                        return;
                    };
                    if *runtime.remote_endpoint(observed.slot).read().await
                        == Some(observed.source)
                    {
                        candidates.remove(&observed.slot);
                        continue;
                    }
                    let candidate = candidates
                        .entry(observed.slot)
                        .or_insert(RebindCandidate {
                            source: observed.source,
                            packets: 0,
                        });
                    if candidate.source != observed.source {
                        candidate.source = observed.source;
                        candidate.packets = 0;
                    }
                    candidate.packets = candidate.packets.saturating_add(1);
                    if candidate.packets >= observed.slot.confirmation_packets() {
                        candidates.remove(&observed.slot);
                        runtime.confirm_rebind(observed.slot, observed.source).await;
                    }
                }
                _ = health_interval.tick() => {
                    runtime.check_redirect_health();
                }
            }
        }
    })
}

fn max_redirect_errors(stats: FastPathStats) -> u64 {
    [
        stats.caller_to_callee_redirect_errors,
        stats.callee_to_caller_redirect_errors,
        stats.caller_to_callee_rtcp_redirect_errors,
        stats.callee_to_caller_rtcp_redirect_errors,
    ]
    .into_iter()
    .max()
    .unwrap_or_default()
}

fn valid_media_packet(direction: RelayDirection, packet: &[u8]) -> bool {
    match direction {
        RelayDirection::CallerToCalleeRtp | RelayDirection::CalleeToCallerRtp => {
            packet.len() >= 12 && packet[0] >> 6 == 2
        }
        RelayDirection::CallerToCalleeRtcp | RelayDirection::CalleeToCallerRtcp => {
            packet.len() >= 4 && packet[0] >> 6 == 2 && (192..=223).contains(&packet[1])
        }
    }
}

fn count_packet(counters: &RelayCounters, direction: RelayDirection, bytes: u64) {
    match direction {
        RelayDirection::CallerToCalleeRtp => {
            counters
                .caller_to_callee_packets
                .fetch_add(1, Ordering::Relaxed);
            counters
                .caller_to_callee_bytes
                .fetch_add(bytes, Ordering::Relaxed);
        }
        RelayDirection::CalleeToCallerRtp => {
            counters
                .callee_to_caller_packets
                .fetch_add(1, Ordering::Relaxed);
            counters
                .callee_to_caller_bytes
                .fetch_add(bytes, Ordering::Relaxed);
        }
        RelayDirection::CallerToCalleeRtcp => {
            counters
                .caller_to_callee_rtcp_packets
                .fetch_add(1, Ordering::Relaxed);
        }
        RelayDirection::CalleeToCallerRtcp => {
            counters
                .callee_to_caller_rtcp_packets
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn update_dtmf_negotiation(
    sender: &watch::Sender<ParserConfig>,
    capability: Option<DtmfCapability>,
    generation: u64,
) {
    sender.send_modify(|current| {
        *current = ParserConfig::with_mode(generation, capability, current.mode);
    });
}

fn set_dtmf_mode(sender: &watch::Sender<ParserConfig>, mode: DtmfMediaMode) -> u64 {
    let current = sender.borrow().clone();
    if current.mode == mode {
        return current.generation;
    }
    let generation = current.generation.saturating_add(1);
    sender.send_replace(ParserConfig::with_mode(
        generation,
        current.capability,
        mode,
    ));
    generation
}

fn dtmf_mode_priority(mode: DtmfMediaMode) -> u8 {
    match mode {
        DtmfMediaMode::Transparent => 0,
        DtmfMediaMode::Observe => 1,
        DtmfMediaMode::Collect => 2,
    }
}

fn dtmf_stats(counters: &DtmfObservationCounters) -> DtmfObservationStats {
    DtmfObservationStats {
        completed: counters.completed.load(Ordering::Relaxed),
        incomplete: counters.incomplete.load(Ordering::Relaxed),
        invalid: counters.invalid.load(Ordering::Relaxed),
        delivery_fault: counters.delivery_fault.load(Ordering::Relaxed),
    }
}

async fn bind_socket() -> Result<UdpSocket> {
    UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 0)))
        .await
        .context("bind media relay UDP socket")
}

fn advertise_ip_for(route_target: SocketAddr) -> Result<IpAddr> {
    if let Some(value) = std::env::var_os("VOIPSWITCH_MEDIA_IP") {
        let ip = value
            .to_string_lossy()
            .parse::<IpAddr>()
            .context("parse VOIPSWITCH_MEDIA_IP")?;
        if ip.is_unspecified() {
            bail!("VOIPSWITCH_MEDIA_IP cannot be an unspecified address");
        }
        return Ok(ip);
    }
    route_local_ip(route_target)
}

fn route_local_ip(route_target: SocketAddr) -> Result<IpAddr> {
    let bind_addr = match route_target {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    };
    let socket = StdUdpSocket::bind(bind_addr).context("bind media route probe")?;
    socket
        .connect(route_target)
        .with_context(|| format!("connect media route probe to {route_target}"))?;
    let local_ip = socket.local_addr()?.ip();
    if local_ip.is_unspecified() {
        bail!("media route probe selected an unspecified address");
    }
    Ok(local_ip)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::call::session::SessionHandle;

    #[derive(Default)]
    struct TestFastPathController {
        stats: StdMutex<FastPathStats>,
        active: StdMutex<Option<(String, u64)>>,
        promotions: AtomicU64,
        demotions: AtomicU64,
        fail_demotion: AtomicBool,
    }

    impl FastPathController for TestFastPathController {
        fn availability(&self) -> FastPathAvailability {
            FastPathAvailability::Available
        }

        fn promote(&self, spec: &FastPathBridgeSpec) -> Result<(), FastPathError> {
            *self.active.lock().unwrap() = Some((spec.bridge_id.clone(), spec.generation));
            self.promotions.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn snapshot(
            &self,
            _bridge_id: &str,
            _generation: u64,
        ) -> Result<FastPathStats, FastPathError> {
            Ok(*self.stats.lock().unwrap())
        }

        fn demote(
            &self,
            _bridge_id: &str,
            _generation: u64,
            _reason: FastPathFallbackReason,
        ) -> Result<FastPathStats, FastPathError> {
            if self.fail_demotion.load(Ordering::Relaxed) {
                return Err(FastPathError {
                    code: "test_demotion_failed".to_string(),
                    message: "injected demotion failure".to_string(),
                });
            }
            *self.active.lock().unwrap() = None;
            self.demotions.fetch_add(1, Ordering::Relaxed);
            Ok(*self.stats.lock().unwrap())
        }

        fn remove(
            &self,
            _bridge_id: &str,
            _generation: u64,
        ) -> Result<FastPathStats, FastPathError> {
            *self.active.lock().unwrap() = None;
            Ok(*self.stats.lock().unwrap())
        }
    }

    fn audio_sdp(rtp_port: u16, rtcp_port: u16) -> SdpBody {
        SdpBody {
            content_type: "application/sdp".to_string(),
            text: format!(
                "v=0\r\n\
                 o=- 1 1 IN IP4 127.0.0.1\r\n\
                 s=test\r\n\
                 c=IN IP4 127.0.0.1\r\n\
                 t=0 0\r\n\
                 m=audio {rtp_port} RTP/AVP 8\r\n\
                 a=rtpmap:8 PCMA/8000\r\n\
                 a=rtcp:{rtcp_port} IN IP4 127.0.0.1\r\n\
                 a=sendrecv\r\n"
            ),
        }
    }

    fn dtmf_audio_sdp(rtp_port: u16, rtcp_port: u16) -> SdpBody {
        let mut body = audio_sdp(rtp_port, rtcp_port);
        body.text = body.text.replace("RTP/AVP 8", "RTP/AVP 8 101").replace(
            "a=rtpmap:8 PCMA/8000\r\n",
            concat!(
                "a=rtpmap:8 PCMA/8000\r\n",
                "a=rtpmap:101 telephone-event/8000\r\n",
                "a=fmtp:101 0-15\r\n"
            ),
        );
        body
    }

    fn rfc4733_packet(sequence: u16, timestamp: u32, duration: u16, ended: bool) -> Vec<u8> {
        let mut packet = vec![0_u8; 16];
        packet[0] = 0x80;
        packet[1] = 101;
        packet[2..4].copy_from_slice(&sequence.to_be_bytes());
        packet[4..8].copy_from_slice(&timestamp.to_be_bytes());
        packet[8..12].copy_from_slice(&7_u32.to_be_bytes());
        packet[12] = 5;
        packet[13] = if ended { 0x80 } else { 0 };
        packet[14..16].copy_from_slice(&duration.to_be_bytes());
        packet
    }

    fn dtmf_lease(
        lease_id: &str,
        owner: &str,
        source_session_id: &str,
        mode: DtmfMediaMode,
        requested_generation: u64,
    ) -> DtmfCapabilityLeaseRequest {
        DtmfCapabilityLeaseRequest {
            lease_id: MediaCapabilityLeaseId::from(lease_id),
            owner: BusinessOperationId::from(owner),
            source_session_id: SessionId::from(source_session_id),
            mode,
            requested_generation,
        }
    }

    #[test]
    fn route_probe_selects_loopback_for_loopback_target() {
        let local_ip = route_local_ip(SocketAddr::from(([127, 0, 0, 1], 5060))).unwrap();
        assert_eq!(local_ip, IpAddr::V4(Ipv4Addr::LOCALHOST));
    }

    #[tokio::test]
    async fn relay_sends_from_the_socket_advertised_to_the_target_leg() {
        let recv_socket = Arc::new(bind_socket().await.unwrap());
        let send_socket = Arc::new(bind_socket().await.unwrap());
        let source_peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target_peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let source_endpoint = Arc::new(RwLock::new(Some(source_peer.local_addr().unwrap())));
        let target = Arc::new(RwLock::new(Some(target_peer.local_addr().unwrap())));
        let counters = Arc::new(RelayCounters::default());
        let (stop_tx, stop_rx) = watch::channel(false);
        let (observation_tx, _observation_rx) = mpsc::channel(1);

        let task = spawn_relay(
            RelayTaskSpec {
                recv_socket: recv_socket.clone(),
                send_socket: send_socket.clone(),
                source_endpoint,
                target,
                counters,
                direction: RelayDirection::CallerToCalleeRtp,
                endpoint_slot: RemoteEndpointSlot::CallerRtp,
                observation_tx,
                recording: None,
                ai_tap: None,
                dtmf_config: None,
                dtmf_policy_applied: None,
                dtmf_observation_tx: None,
                dtmf_counters: Arc::new(DtmfObservationCounters::default()),
            },
            stop_rx,
        );
        source_peer
            .send_to(b"rtp", recv_socket.local_addr().unwrap())
            .await
            .unwrap();

        let mut packet = [0_u8; 16];
        let (size, source) = target_peer.recv_from(&mut packet).await.unwrap();
        assert_eq!(&packet[..size], b"rtp");
        assert_eq!(source.port(), send_socket.local_addr().unwrap().port());
        assert_eq!(source.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));

        let _ = stop_tx.send(true);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn relay_observes_rfc4733_without_changing_forwarded_packets() {
        let source_peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let unconfirmed_peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target_peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let source_port = source_peer.local_addr().unwrap().port();
        let target_port = target_peer.local_addr().unwrap().port();
        let (bridge, _) = MediaBridge::allocate_userspace(
            &dtmf_audio_sdp(source_port, source_port.saturating_add(1)),
            Some("127.0.0.1:5060".parse().unwrap()),
            Some("127.0.0.1:5060".parse().unwrap()),
            Arc::new(UnavailableFastPathController),
        )
        .await
        .unwrap();
        let (caller_handle, mut caller_control_rx, _caller_event_rx) = SessionHandle::new_pair();
        let (callee_handle, mut callee_control_rx, _callee_event_rx) = SessionHandle::new_pair();
        let (dispatcher, dispatcher_task) = CriticalControlDispatcher::spawn();
        bridge.handle().bind_dtmf_sessions(DtmfSessionBindings {
            domain_id: "domain-test".to_string(),
            call_id: "call-test".to_string(),
            caller_session_id: "caller-test".to_string(),
            caller_control_tx: caller_handle.control_sender(),
            callee_session_id: "callee-test".to_string(),
            callee_control_tx: callee_handle.control_sender(),
            dispatcher: dispatcher.clone(),
        });
        bridge
            .prepare_caller_sdp(
                &dtmf_audio_sdp(target_port, target_port.saturating_add(1)),
                2,
            )
            .await
            .unwrap();
        let ready = bridge
            .handle()
            .acquire_dtmf_capability(dtmf_lease(
                "observe-caller",
                "test-observer",
                "caller-test",
                DtmfMediaMode::Observe,
                2,
            ))
            .await
            .unwrap();
        assert_eq!(ready.media_generation, 3);

        let unconfirmed = rfc4733_packet(0, 41, 80, true);
        unconfirmed_peer
            .send_to(&unconfirmed, bridge.caller_rtp.local_addr().unwrap())
            .await
            .unwrap();
        let mut forwarded = [0_u8; 64];
        let (size, _) = target_peer.recv_from(&mut forwarded).await.unwrap();
        assert_eq!(&forwarded[..size], unconfirmed);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(bridge.stats().dtmf.completed, 0);

        for packet in [
            rfc4733_packet(1, 42, 80, false),
            rfc4733_packet(2, 42, 160, false),
            rfc4733_packet(3, 42, 240, true),
        ] {
            source_peer
                .send_to(&packet, bridge.caller_rtp.local_addr().unwrap())
                .await
                .unwrap();
            let mut forwarded = [0_u8; 64];
            let (size, _) = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                target_peer.recv_from(&mut forwarded),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(&forwarded[..size], packet);
        }

        let observed =
            tokio::time::timeout(std::time::Duration::from_secs(1), caller_control_rx.recv())
                .await
                .expect("DTMF event dispatched to caller source actor")
                .expect("caller source mailbox open");
        assert!(matches!(
            observed,
            ControlMessage::MediaDtmfObserved(event)
                if event.call_id.as_str() == "call-test"
                    && event.source_session_id.as_str() == "caller-test"
                    && event.source_generation == DtmfSourceGeneration::Media(3)
        ));

        let (replacement_handle, mut replacement_control_rx, _replacement_event_rx) =
            SessionHandle::new_pair();
        bridge.handle().update_dtmf_callee_target(
            "callee-attempt-2".to_string(),
            replacement_handle.control_sender(),
        );
        bridge
            .handle()
            .acquire_dtmf_capability(dtmf_lease(
                "observe-callee",
                "test-observer",
                "callee-attempt-2",
                DtmfMediaMode::Observe,
                2,
            ))
            .await
            .unwrap();
        let callee_packet = rfc4733_packet(4, 84, 320, true);
        target_peer
            .send_to(&callee_packet, bridge.callee_rtp.local_addr().unwrap())
            .await
            .unwrap();
        let mut returned = [0_u8; 64];
        let (size, _) = source_peer.recv_from(&mut returned).await.unwrap();
        assert_eq!(&returned[..size], callee_packet);
        let observed = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            replacement_control_rx.recv(),
        )
        .await
        .expect("DTMF event dispatched to replacement callee source actor")
        .expect("replacement callee source mailbox open");
        assert!(matches!(
            observed,
            ControlMessage::MediaDtmfObserved(event)
                if event.source_session_id.as_str() == "callee-attempt-2"
                    && event.source_generation == DtmfSourceGeneration::Media(3)
        ));
        assert!(callee_control_rx.try_recv().is_err());

        let stats = bridge.stats().dtmf;
        assert_eq!(stats.completed, 2);
        assert_eq!(stats.incomplete, 0);
        assert_eq!(stats.invalid, 0);
        assert_eq!(stats.delivery_fault, 0);

        bridge.stop("dtmf-relay-test").await;
        drop(caller_handle);
        drop(callee_handle);
        drop(replacement_handle);
        drop(dispatcher);
        dispatcher_task.await.unwrap();
    }

    #[tokio::test]
    async fn collect_lease_suppresses_only_the_selected_source_telephone_event() {
        let source_peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target_peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let source_port = source_peer.local_addr().unwrap().port();
        let target_port = target_peer.local_addr().unwrap().port();
        let (bridge, _) = MediaBridge::allocate_userspace(
            &dtmf_audio_sdp(source_port, source_port.saturating_add(1)),
            Some("127.0.0.1:5060".parse().unwrap()),
            Some("127.0.0.1:5060".parse().unwrap()),
            Arc::new(UnavailableFastPathController),
        )
        .await
        .unwrap();
        let (caller_handle, mut caller_control_rx, _caller_event_rx) = SessionHandle::new_pair();
        let (callee_handle, _callee_control_rx, _callee_event_rx) = SessionHandle::new_pair();
        let (dispatcher, dispatcher_task) = CriticalControlDispatcher::spawn();
        let handle = bridge.handle();
        handle.bind_dtmf_sessions(DtmfSessionBindings {
            domain_id: "domain-test".to_string(),
            call_id: "call-collect".to_string(),
            caller_session_id: "caller-test".to_string(),
            caller_control_tx: caller_handle.control_sender(),
            callee_session_id: "callee-test".to_string(),
            callee_control_tx: callee_handle.control_sender(),
            dispatcher: dispatcher.clone(),
        });
        handle
            .prepare_caller_sdp(
                &dtmf_audio_sdp(target_port, target_port.saturating_add(1)),
                1,
            )
            .await
            .unwrap();

        let request = dtmf_lease(
            "collect-caller",
            "test-collector",
            "caller-test",
            DtmfMediaMode::Collect,
            1,
        );
        let ready = handle
            .acquire_dtmf_capability(request.clone())
            .await
            .unwrap();
        assert_eq!(ready.media_generation, 2);
        assert_eq!(
            handle
                .acquire_dtmf_capability(request)
                .await
                .unwrap()
                .media_generation,
            2
        );
        let conflict = handle
            .acquire_dtmf_capability(dtmf_lease(
                "collect-caller-2",
                "other-collector",
                "caller-test",
                DtmfMediaMode::Collect,
                2,
            ))
            .await
            .unwrap_err();
        assert!(conflict.to_string().contains("dtmf_collector_conflict"));

        for packet in [
            rfc4733_packet(1, 42, 80, false),
            rfc4733_packet(2, 42, 160, false),
            rfc4733_packet(3, 42, 240, true),
        ] {
            source_peer
                .send_to(&packet, bridge.caller_rtp.local_addr().unwrap())
                .await
                .unwrap();
        }
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                target_peer.recv_from(&mut [0_u8; 64]),
            )
            .await
            .is_err()
        );
        let observed =
            tokio::time::timeout(std::time::Duration::from_secs(1), caller_control_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(
            observed,
            ControlMessage::MediaDtmfObserved(event)
                if event.source_generation == DtmfSourceGeneration::Media(2)
        ));

        let lease_id = MediaCapabilityLeaseId::from("collect-caller");
        let release_handle = handle.clone();
        let release_lease_id = lease_id.clone();
        let release = tokio::spawn(async move {
            release_handle
                .release_dtmf_capability(&release_lease_id)
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        for packet in [
            rfc4733_packet(4, 42, 240, true),
            rfc4733_packet(5, 42, 240, true),
        ] {
            source_peer
                .send_to(&packet, bridge.caller_rtp.local_addr().unwrap())
                .await
                .unwrap();
        }
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                target_peer.recv_from(&mut [0_u8; 64]),
            )
            .await
            .is_err()
        );
        assert_eq!(release.await.unwrap().unwrap(), 3);
        assert_eq!(handle.release_dtmf_capability(&lease_id).await.unwrap(), 3);
        let transparent = rfc4733_packet(6, 84, 240, true);
        source_peer
            .send_to(&transparent, bridge.caller_rtp.local_addr().unwrap())
            .await
            .unwrap();
        let mut forwarded = [0_u8; 64];
        let (size, _) = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            target_peer.recv_from(&mut forwarded),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&forwarded[..size], transparent);
        assert!(caller_control_rx.try_recv().is_err());

        bridge.stop("dtmf-collect-test").await;
        drop(handle);
        drop(caller_handle);
        drop(callee_handle);
        drop(dispatcher);
        dispatcher_task.await.unwrap();
    }

    #[tokio::test]
    async fn capability_ready_waits_for_fast_path_demotion_and_blocks_repromotion() {
        let controller = Arc::new(TestFastPathController::default());
        let (bridge, _) = MediaBridge::allocate_userspace(
            &dtmf_audio_sdp(30000, 30001),
            Some("127.0.0.1:5060".parse().unwrap()),
            Some("127.0.0.1:5060".parse().unwrap()),
            controller.clone(),
        )
        .await
        .unwrap();
        let (caller_handle, _caller_control_rx, _caller_event_rx) = SessionHandle::new_pair();
        let (callee_handle, _callee_control_rx, _callee_event_rx) = SessionHandle::new_pair();
        let (dispatcher, dispatcher_task) = CriticalControlDispatcher::spawn();
        let handle = bridge.handle();
        handle.bind_dtmf_sessions(DtmfSessionBindings {
            domain_id: "domain-test".to_string(),
            call_id: "call-capability".to_string(),
            caller_session_id: "caller-test".to_string(),
            caller_control_tx: caller_handle.control_sender(),
            callee_session_id: "callee-test".to_string(),
            callee_control_tx: callee_handle.control_sender(),
            dispatcher: dispatcher.clone(),
        });
        handle
            .prepare_caller_sdp(&dtmf_audio_sdp(31000, 31001), 1)
            .await
            .unwrap();
        handle.try_promote_fast_path("call-capability", true).await;
        assert_eq!(controller.promotions.load(Ordering::Relaxed), 1);

        let ready = handle
            .acquire_dtmf_capability(dtmf_lease(
                "observe-caller",
                "test-observer",
                "caller-test",
                DtmfMediaMode::Observe,
                1,
            ))
            .await
            .unwrap();
        assert_eq!(ready.media_generation, 2);
        assert_eq!(controller.demotions.load(Ordering::Relaxed), 1);
        assert!(controller.active.lock().unwrap().is_none());

        let second_ready = handle
            .acquire_dtmf_capability(dtmf_lease(
                "observe-caller-2",
                "test-observer-2",
                "caller-test",
                DtmfMediaMode::Observe,
                1,
            ))
            .await
            .unwrap();
        assert_eq!(second_ready.media_generation, 2);
        assert_eq!(controller.demotions.load(Ordering::Relaxed), 1);

        handle
            .release_dtmf_capability(&MediaCapabilityLeaseId::from("observe-caller"))
            .await
            .unwrap();
        assert_eq!(
            handle.caller_dtmf_config.borrow().mode,
            DtmfMediaMode::Observe
        );
        handle
            .release_dtmf_capability(&MediaCapabilityLeaseId::from("observe-caller-2"))
            .await
            .unwrap();
        handle.try_promote_fast_path("call-capability", true).await;
        assert_eq!(controller.promotions.load(Ordering::Relaxed), 1);

        bridge.stop("call-capability").await;
        drop(handle);
        drop(caller_handle);
        drop(callee_handle);
        drop(dispatcher);
        dispatcher_task.await.unwrap();
    }

    #[tokio::test]
    async fn demotion_failure_does_not_install_dtmf_policy_or_report_ready() {
        let controller = Arc::new(TestFastPathController::default());
        let (bridge, _) = MediaBridge::allocate_userspace(
            &dtmf_audio_sdp(30000, 30001),
            Some("127.0.0.1:5060".parse().unwrap()),
            Some("127.0.0.1:5060".parse().unwrap()),
            controller.clone(),
        )
        .await
        .unwrap();
        let (caller_handle, _caller_control_rx, _caller_event_rx) = SessionHandle::new_pair();
        let (callee_handle, _callee_control_rx, _callee_event_rx) = SessionHandle::new_pair();
        let (dispatcher, dispatcher_task) = CriticalControlDispatcher::spawn();
        let handle = bridge.handle();
        handle.bind_dtmf_sessions(DtmfSessionBindings {
            domain_id: "domain-test".to_string(),
            call_id: "call-capability-fail".to_string(),
            caller_session_id: "caller-test".to_string(),
            caller_control_tx: caller_handle.control_sender(),
            callee_session_id: "callee-test".to_string(),
            callee_control_tx: callee_handle.control_sender(),
            dispatcher: dispatcher.clone(),
        });
        handle
            .prepare_caller_sdp(&dtmf_audio_sdp(31000, 31001), 1)
            .await
            .unwrap();
        handle
            .try_promote_fast_path("call-capability-fail", true)
            .await;
        controller.fail_demotion.store(true, Ordering::Relaxed);

        let error = handle
            .acquire_dtmf_capability(dtmf_lease(
                "observe-caller",
                "test-observer",
                "caller-test",
                DtmfMediaMode::Observe,
                1,
            ))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("capability_demotion_failed"));
        assert_eq!(
            handle.caller_dtmf_config.borrow().mode,
            DtmfMediaMode::Transparent
        );
        assert_eq!(handle.caller_dtmf_config.borrow().generation, 1);
        assert!(controller.active.lock().unwrap().is_some());

        bridge.stop("call-capability-fail").await;
        drop(handle);
        drop(caller_handle);
        drop(callee_handle);
        drop(dispatcher);
        dispatcher_task.await.unwrap();
    }

    #[test]
    fn dtmf_observation_queue_is_bounded_and_non_blocking() {
        assert_eq!(DTMF_OBSERVATION_QUEUE_CAPACITY, 32);
        let (sender, _receiver) = mpsc::channel(DTMF_OBSERVATION_QUEUE_CAPACITY);
        let observation = DtmfRelayObservation {
            direction: DtmfRelayDirection::CallerToCallee,
            observation: ParserObservation::Invalid,
        };
        for _ in 0..DTMF_OBSERVATION_QUEUE_CAPACITY {
            sender.try_send(observation).unwrap();
        }
        assert!(matches!(
            sender.try_send(observation),
            Err(mpsc::error::TrySendError::Full(_))
        ));
    }

    #[tokio::test]
    async fn callee_attempt_reuses_relay_ports_and_clears_previous_remote() {
        let (bridge, first_offer) = MediaBridge::allocate_userspace(
            &audio_sdp(30000, 30001),
            Some("127.0.0.1:5060".parse().unwrap()),
            Some("127.0.0.1:15062".parse().unwrap()),
            Arc::new(UnavailableFastPathController),
        )
        .await
        .unwrap();
        let handle = bridge.handle();
        let first = parse_audio_sdp(&first_offer).unwrap();

        handle
            .prepare_caller_sdp(&audio_sdp(31000, 31001), 1)
            .await
            .unwrap();
        assert_eq!(
            handle.callee_remote().await,
            Some("127.0.0.1:31000".parse().unwrap())
        );

        let second_offer = handle
            .prepare_callee_sdp(
                &audio_sdp(30000, 30001),
                Some("127.0.0.1:15063".parse().unwrap()),
                2,
            )
            .await
            .unwrap();
        let second = parse_audio_sdp(&second_offer).unwrap();

        assert_eq!(second.remote_rtp.port(), first.remote_rtp.port());
        assert_eq!(
            second.remote_rtcp.unwrap().port(),
            first.remote_rtcp.unwrap().port()
        );
        assert_eq!(handle.callee_remote().await, None);
        assert_eq!(*handle.callee_remote_rtcp.read().await, None);

        bridge.stop("call-attempt-switch").await;
    }

    #[test]
    fn fast_path_builds_independent_bidirectional_rtp_and_rtcp_flows() {
        let caller = FastPathLegEndpoints {
            local_rtp: "192.0.2.10:20000".parse().unwrap(),
            remote_rtp: "198.51.100.10:30000".parse().unwrap(),
            local_rtcp: "192.0.2.10:20001".parse().unwrap(),
            remote_rtcp: "198.51.100.10:30001".parse().unwrap(),
        };
        let callee = FastPathLegEndpoints {
            local_rtp: "192.0.2.20:21000".parse().unwrap(),
            remote_rtp: "198.51.100.20:31000".parse().unwrap(),
            local_rtcp: "192.0.2.20:21001".parse().unwrap(),
            remote_rtcp: "198.51.100.20:31001".parse().unwrap(),
        };

        let flows = fast_path_flows(caller, callee);

        assert_eq!(flows.len(), 4);
        assert_eq!(
            flows[0],
            FastPathFlowSpec {
                media_kind: FastPathMediaKind::Rtp,
                direction: MediaFlowDirection::CallerToCallee,
                local: caller.local_rtp,
                remote: caller.remote_rtp,
                rewritten_source: callee.local_rtp,
                rewritten_destination: callee.remote_rtp,
            }
        );
        assert_eq!(
            flows[3],
            FastPathFlowSpec {
                media_kind: FastPathMediaKind::Rtcp,
                direction: MediaFlowDirection::CalleeToCaller,
                local: callee.local_rtcp,
                remote: callee.remote_rtcp,
                rewritten_source: caller.local_rtcp,
                rewritten_destination: caller.remote_rtcp,
            }
        );
    }

    #[test]
    fn rebind_confirmation_and_packet_validation_are_media_specific() {
        assert_eq!(RemoteEndpointSlot::CallerRtp.confirmation_packets(), 3);
        assert_eq!(RemoteEndpointSlot::CalleeRtcp.confirmation_packets(), 1);

        let mut rtp = [0_u8; 12];
        rtp[0] = 0x80;
        assert!(valid_media_packet(RelayDirection::CallerToCalleeRtp, &rtp));
        assert!(!valid_media_packet(
            RelayDirection::CallerToCalleeRtp,
            &[0x80; 4]
        ));

        let rtcp = [0x80, 201, 0, 1, 0, 0, 0, 1];
        assert!(valid_media_packet(
            RelayDirection::CalleeToCallerRtcp,
            &rtcp
        ));
        assert!(!valid_media_packet(
            RelayDirection::CalleeToCallerRtcp,
            &[0x80, 100, 0, 1]
        ));
    }

    #[tokio::test]
    async fn redirect_error_threshold_demotes_and_blocks_repromotion() {
        let controller = Arc::new(TestFastPathController::default());
        let (bridge, _) = MediaBridge::allocate_userspace(
            &audio_sdp(30000, 30001),
            Some("127.0.0.1:5060".parse().unwrap()),
            Some("127.0.0.1:5060".parse().unwrap()),
            controller.clone(),
        )
        .await
        .unwrap();
        bridge
            .prepare_caller_sdp(&audio_sdp(31000, 31001), 1)
            .await
            .unwrap();
        bridge.try_promote_fast_path("call-health", true).await;
        assert_eq!(controller.promotions.load(Ordering::Relaxed), 1);

        controller
            .stats
            .lock()
            .unwrap()
            .caller_to_callee_redirect_errors = FAST_PATH_REDIRECT_ERROR_THRESHOLD;
        bridge.fast_path_runtime.check_redirect_health();

        assert_eq!(controller.demotions.load(Ordering::Relaxed), 1);
        bridge.try_promote_fast_path("call-health", true).await;
        assert_eq!(controller.promotions.load(Ordering::Relaxed), 1);

        let finalized = bridge.stop("call-health").await;
        assert_eq!(finalized.forwarding_mode, MediaForwardingMode::Mixed);
    }
}
