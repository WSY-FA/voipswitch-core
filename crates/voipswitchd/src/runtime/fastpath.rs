use anyhow::{Context, Result, anyhow};
use aya::maps::{HashMap as BpfHashMap, MapData};
use aya::programs::{SchedClassifier, TcAttachType, tc};
use aya::{Ebpf, Pod};
use std::collections::HashMap;
use std::ffi::CString;
use std::fs;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Mutex;
use tracing::{info, warn};
use voipswitch_core::media::{
    FastPathAvailability, FastPathBridgeSpec, FastPathController, FastPathError,
    FastPathFallbackReason, FastPathMediaKind, FastPathStats, MediaFlowDirection,
};

const BPF_OBJECT: &[u8] =
    aya::include_bytes_aligned!(concat!(env!("OUT_DIR"), "/rtp_fastpath.bpf.o"));

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct FlowKey {
    ingress_ifindex: u32,
    local_ip: u32,
    remote_ip: u32,
    local_port: u16,
    remote_port: u16,
    protocol: u8,
    padding: [u8; 3],
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct FlowAction {
    egress_ifindex: u32,
    rewritten_src_ip: u32,
    rewritten_dst_ip: u32,
    rewritten_src_port: u16,
    rewritten_dst_port: u16,
    direction: u32,
    generation: u64,
    packets: u64,
    bytes: u64,
    redirect_errors: u64,
}

unsafe impl Pod for FlowKey {}
unsafe impl Pod for FlowAction {}

#[derive(Debug, Clone, Copy)]
struct InstalledFlow {
    key: FlowKey,
    media_kind: FastPathMediaKind,
    direction: MediaFlowDirection,
}

struct ControllerState {
    _ebpf: Ebpf,
    flows: BpfHashMap<MapData, FlowKey, FlowAction>,
    bridges: HashMap<String, (u64, Vec<InstalledFlow>)>,
}

pub struct EbpfFastPathController {
    interface: String,
    ifindex: u32,
    state: Mutex<ControllerState>,
}

impl EbpfFastPathController {
    pub fn load() -> Result<Self> {
        let interface = fast_path_interface()?;
        let ifindex = interface_index(&interface)?;
        let mut ebpf = Ebpf::load(BPF_OBJECT)
            .map_err(|err| anyhow!("load RTP fast path BPF object: {err:?}"))?;
        if let Err(err) = tc::qdisc_add_clsact(&interface)
            && err.kind() != std::io::ErrorKind::AlreadyExists
        {
            return Err(err).with_context(|| format!("add clsact qdisc to {interface}"));
        }
        let program: &mut SchedClassifier = ebpf
            .program_mut("rtp_fastpath")
            .context("RTP fast path classifier missing")?
            .try_into()
            .context("convert RTP fast path classifier")?;
        program.load().context("load RTP fast path classifier")?;
        program
            .attach(&interface, TcAttachType::Ingress)
            .with_context(|| format!("attach RTP fast path to {interface} ingress"))?;
        let flows = BpfHashMap::try_from(
            ebpf.take_map("RTP_FLOWS")
                .context("RTP_FLOWS map missing")?,
        )
        .context("open RTP_FLOWS map")?;
        info!(interface, ifindex, "RTP tc/eBPF fast path available");
        Ok(Self {
            interface,
            ifindex,
            state: Mutex::new(ControllerState {
                _ebpf: ebpf,
                flows,
                bridges: HashMap::new(),
            }),
        })
    }

    fn remove_locked(
        state: &mut ControllerState,
        bridge_id: &str,
        generation: u64,
    ) -> Result<FastPathStats, FastPathError> {
        let Some((installed_generation, installed)) = state.bridges.remove(bridge_id) else {
            return Ok(FastPathStats::default());
        };
        if installed_generation != generation {
            state
                .bridges
                .insert(bridge_id.to_string(), (installed_generation, installed));
            return Err(controller_error(
                "stale_generation",
                format!(
                    "bridge {bridge_id} generation {generation} does not match {installed_generation}"
                ),
            ));
        }
        let mut stats = FastPathStats::default();
        let mut remove_error = None;
        for flow in installed {
            if let Ok(action) = state.flows.get(&flow.key, 0) {
                add_flow_stats(&mut stats, flow, action);
            }
            if let Err(err) = state.flows.remove(&flow.key) {
                remove_error.get_or_insert_with(|| err.to_string());
            }
        }
        if stats.caller_to_callee_redirect_errors != 0
            || stats.callee_to_caller_redirect_errors != 0
            || stats.caller_to_callee_rtcp_redirect_errors != 0
            || stats.callee_to_caller_rtcp_redirect_errors != 0
        {
            warn!(
                bridge_id,
                generation,
                caller_to_callee_redirect_errors = stats.caller_to_callee_redirect_errors,
                callee_to_caller_redirect_errors = stats.callee_to_caller_redirect_errors,
                caller_to_callee_rtcp_redirect_errors = stats.caller_to_callee_rtcp_redirect_errors,
                callee_to_caller_rtcp_redirect_errors = stats.callee_to_caller_rtcp_redirect_errors,
                "RTP/RTCP fast path redirect errors observed"
            );
        }
        info!(
            bridge_id,
            generation,
            caller_to_callee_packets = stats.caller_to_callee_packets,
            caller_to_callee_bytes = stats.caller_to_callee_bytes,
            caller_to_callee_redirect_errors = stats.caller_to_callee_redirect_errors,
            callee_to_caller_packets = stats.callee_to_caller_packets,
            callee_to_caller_bytes = stats.callee_to_caller_bytes,
            callee_to_caller_redirect_errors = stats.callee_to_caller_redirect_errors,
            caller_to_callee_rtcp_packets = stats.caller_to_callee_rtcp_packets,
            caller_to_callee_rtcp_bytes = stats.caller_to_callee_rtcp_bytes,
            caller_to_callee_rtcp_redirect_errors = stats.caller_to_callee_rtcp_redirect_errors,
            callee_to_caller_rtcp_packets = stats.callee_to_caller_rtcp_packets,
            callee_to_caller_rtcp_bytes = stats.callee_to_caller_rtcp_bytes,
            callee_to_caller_rtcp_redirect_errors = stats.callee_to_caller_rtcp_redirect_errors,
            "RTP/RTCP fast path rules removed"
        );
        if let Some(error) = remove_error {
            return Err(controller_error("map_remove_failed", error));
        }
        Ok(stats)
    }

    fn snapshot_locked(
        state: &ControllerState,
        bridge_id: &str,
        generation: u64,
    ) -> Result<FastPathStats, FastPathError> {
        let Some((installed_generation, installed)) = state.bridges.get(bridge_id) else {
            return Err(controller_error(
                "bridge_missing",
                format!("bridge {bridge_id} has no active fast path rules"),
            ));
        };
        if *installed_generation != generation {
            return Err(controller_error(
                "stale_generation",
                format!(
                    "bridge {bridge_id} generation {generation} does not match {installed_generation}"
                ),
            ));
        }
        let mut stats = FastPathStats::default();
        for flow in installed {
            let action = state
                .flows
                .get(&flow.key, 0)
                .map_err(|err| controller_error("map_read_failed", err.to_string()))?;
            add_flow_stats(&mut stats, *flow, action);
        }
        Ok(stats)
    }
}

impl FastPathController for EbpfFastPathController {
    fn availability(&self) -> FastPathAvailability {
        FastPathAvailability::Available
    }

    fn promote(&self, spec: &FastPathBridgeSpec) -> Result<(), FastPathError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| controller_error("lock_poisoned", "fast path controller lock poisoned"))?;
        if state.bridges.contains_key(&spec.bridge_id) {
            return Err(controller_error(
                "bridge_exists",
                format!("bridge {} already has fast path rules", spec.bridge_id),
            ));
        }
        let mut installed: Vec<InstalledFlow> = Vec::with_capacity(spec.flows.len());
        for flow in &spec.flows {
            let key = flow_key(self.ifindex, flow.local, flow.remote);
            let action = flow_action(self.ifindex, spec.generation, flow);
            if let Err(err) = state.flows.insert(key, action, 0) {
                for installed_flow in &installed {
                    let _ = state.flows.remove(&installed_flow.key);
                }
                return Err(controller_error("map_insert_failed", err.to_string()));
            }
            installed.push(InstalledFlow {
                key,
                media_kind: flow.media_kind,
                direction: flow.direction,
            });
        }
        state
            .bridges
            .insert(spec.bridge_id.clone(), (spec.generation, installed));
        info!(
            bridge_id = spec.bridge_id,
            generation = spec.generation,
            interface = self.interface,
            flows = spec.flows.len(),
            "RTP/RTCP fast path promoted"
        );
        Ok(())
    }

    fn snapshot(&self, bridge_id: &str, generation: u64) -> Result<FastPathStats, FastPathError> {
        let state = self
            .state
            .lock()
            .map_err(|_| controller_error("lock_poisoned", "fast path controller lock poisoned"))?;
        Self::snapshot_locked(&state, bridge_id, generation)
    }

    fn demote(
        &self,
        bridge_id: &str,
        generation: u64,
        _reason: FastPathFallbackReason,
    ) -> Result<FastPathStats, FastPathError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| controller_error("lock_poisoned", "fast path controller lock poisoned"))?;
        Self::remove_locked(&mut state, bridge_id, generation)
    }

    fn remove(&self, bridge_id: &str, generation: u64) -> Result<FastPathStats, FastPathError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| controller_error("lock_poisoned", "fast path controller lock poisoned"))?;
        Self::remove_locked(&mut state, bridge_id, generation)
    }
}

fn add_flow_stats(stats: &mut FastPathStats, flow: InstalledFlow, action: FlowAction) {
    let (packets, bytes, redirect_errors) = match (flow.media_kind, flow.direction) {
        (FastPathMediaKind::Rtp, MediaFlowDirection::CallerToCallee) => (
            &mut stats.caller_to_callee_packets,
            &mut stats.caller_to_callee_bytes,
            &mut stats.caller_to_callee_redirect_errors,
        ),
        (FastPathMediaKind::Rtp, MediaFlowDirection::CalleeToCaller) => (
            &mut stats.callee_to_caller_packets,
            &mut stats.callee_to_caller_bytes,
            &mut stats.callee_to_caller_redirect_errors,
        ),
        (FastPathMediaKind::Rtcp, MediaFlowDirection::CallerToCallee) => (
            &mut stats.caller_to_callee_rtcp_packets,
            &mut stats.caller_to_callee_rtcp_bytes,
            &mut stats.caller_to_callee_rtcp_redirect_errors,
        ),
        (FastPathMediaKind::Rtcp, MediaFlowDirection::CalleeToCaller) => (
            &mut stats.callee_to_caller_rtcp_packets,
            &mut stats.callee_to_caller_rtcp_bytes,
            &mut stats.callee_to_caller_rtcp_redirect_errors,
        ),
    };
    *packets = packets.saturating_add(action.packets);
    *bytes = bytes.saturating_add(action.bytes);
    *redirect_errors = redirect_errors.saturating_add(action.redirect_errors);
}

fn flow_key(ifindex: u32, local: SocketAddrV4, remote: SocketAddrV4) -> FlowKey {
    FlowKey {
        ingress_ifindex: ifindex,
        local_ip: ipv4_native(*local.ip()),
        remote_ip: ipv4_native(*remote.ip()),
        local_port: port_native(local.port()),
        remote_port: port_native(remote.port()),
        protocol: libc::IPPROTO_UDP as u8,
        padding: [0; 3],
    }
}

fn flow_action(
    ifindex: u32,
    generation: u64,
    flow: &voipswitch_core::media::FastPathFlowSpec,
) -> FlowAction {
    FlowAction {
        egress_ifindex: ifindex,
        rewritten_src_ip: ipv4_native(*flow.rewritten_source.ip()),
        rewritten_dst_ip: ipv4_native(*flow.rewritten_destination.ip()),
        rewritten_src_port: port_native(flow.rewritten_source.port()),
        rewritten_dst_port: port_native(flow.rewritten_destination.port()),
        direction: match flow.direction {
            MediaFlowDirection::CallerToCallee => 1,
            MediaFlowDirection::CalleeToCaller => 2,
        },
        generation,
        packets: 0,
        bytes: 0,
        redirect_errors: 0,
    }
}

fn ipv4_native(ip: Ipv4Addr) -> u32 {
    u32::from_ne_bytes(ip.octets())
}

fn port_native(port: u16) -> u16 {
    u16::from_ne_bytes(port.to_be_bytes())
}

fn fast_path_interface() -> Result<String> {
    if let Ok(value) = std::env::var("VOIPSWITCH_FASTPATH_INTERFACE") {
        let interface = value.trim();
        if !interface.is_empty() {
            return Ok(interface.to_string());
        }
    }
    let routes = fs::read_to_string("/proc/net/route").context("read /proc/net/route")?;
    routes
        .lines()
        .skip(1)
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            (fields.len() > 3 && fields[1] == "00000000" && fields[3] != "0000")
                .then(|| fields[0].to_string())
        })
        .next()
        .ok_or_else(|| anyhow!("default IPv4 route interface not found"))
}

fn interface_index(interface: &str) -> Result<u32> {
    let name = CString::new(interface).context("interface name contains NUL")?;
    let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
    if index == 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("resolve interface index for {interface}"));
    }
    Ok(index)
}

fn controller_error(code: impl Into<String>, message: impl Into<String>) -> FastPathError {
    FastPathError {
        code: code.into(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_key_preserves_network_order_bytes() {
        let key = flow_key(
            2,
            "192.0.2.10:4000".parse().unwrap(),
            "198.51.100.20:5000".parse().unwrap(),
        );
        assert_eq!(key.local_ip.to_ne_bytes(), [192, 0, 2, 10]);
        assert_eq!(key.remote_ip.to_ne_bytes(), [198, 51, 100, 20]);
        assert_eq!(key.local_port.to_ne_bytes(), 4000_u16.to_be_bytes());
        assert_eq!(key.remote_port.to_ne_bytes(), 5000_u16.to_be_bytes());
    }

    #[test]
    fn flow_stats_keep_rtp_and_rtcp_separate() {
        let mut stats = FastPathStats::default();
        let key = flow_key(
            2,
            "192.0.2.10:4000".parse().unwrap(),
            "198.51.100.20:5000".parse().unwrap(),
        );
        let action = FlowAction {
            egress_ifindex: 2,
            rewritten_src_ip: 0,
            rewritten_dst_ip: 0,
            rewritten_src_port: 0,
            rewritten_dst_port: 0,
            direction: 1,
            generation: 1,
            packets: 3,
            bytes: 240,
            redirect_errors: 1,
        };

        add_flow_stats(
            &mut stats,
            InstalledFlow {
                key,
                media_kind: FastPathMediaKind::Rtp,
                direction: MediaFlowDirection::CallerToCallee,
            },
            action,
        );
        add_flow_stats(
            &mut stats,
            InstalledFlow {
                key,
                media_kind: FastPathMediaKind::Rtcp,
                direction: MediaFlowDirection::CallerToCallee,
            },
            action,
        );

        assert_eq!(stats.caller_to_callee_packets, 3);
        assert_eq!(stats.caller_to_callee_rtcp_packets, 3);
        assert_eq!(stats.caller_to_callee_bytes, 240);
        assert_eq!(stats.caller_to_callee_rtcp_bytes, 240);
        assert_eq!(stats.caller_to_callee_redirect_errors, 1);
        assert_eq!(stats.caller_to_callee_rtcp_redirect_errors, 1);
    }
}
