use crate::types::ids::{CallId, DomainId, MediaBridgeId, MediaLegId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display};
use std::net::{IpAddr, SocketAddr, SocketAddrV4};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SdpBody {
    pub content_type: String,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaDirection {
    SendRecv,
    SendOnly,
    RecvOnly,
    Inactive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioPayload {
    pub payload_type: u8,
    pub encoding: Option<String>,
    pub clock_rate: Option<u32>,
    pub channels: Option<u8>,
    pub fmtp: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DtmfEventSet {
    events: BTreeSet<u8>,
}

impl DtmfEventSet {
    pub fn contains(&self, event: u8) -> bool {
        self.events.contains(&event)
    }

    pub fn iter(&self) -> impl Iterator<Item = u8> + '_ {
        self.events.iter().copied()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DtmfCapability {
    pub payload_type: u8,
    pub clock_rate: u32,
    pub events: DtmfEventSet,
    pub detectable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedAudioSdp {
    pub remote_rtp: SocketAddr,
    pub remote_rtcp: Option<SocketAddr>,
    pub payload_types: Vec<u8>,
    pub payloads: Vec<AudioPayload>,
    pub telephone_event: Option<DtmfCapability>,
    pub direction: MediaDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelayEndpoint {
    pub rtp: SocketAddr,
    pub rtcp: Option<SocketAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSdp {
    pub body: SdpBody,
    pub parsed: ParsedAudioSdp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SdpError {
    UnsupportedContentType(String),
    MissingMedia,
    MultipleMediaLines,
    UnsupportedMedia(String),
    UnsupportedTransport(String),
    MissingConnection,
    InvalidConnection(String),
    InvalidMediaLine(String),
    InvalidRtcpAttribute(String),
    InvalidPayloadType(String),
    InvalidRtpmapAttribute(String),
    DuplicateRtpmapAttribute(u8),
    InvalidFmtpAttribute(String),
    DuplicateFmtpAttribute(u8),
    UnsupportedAttribute(String),
}

impl Display for SdpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedContentType(value) => {
                write!(f, "unsupported SDP content type: {value}")
            }
            Self::MissingMedia => f.write_str("SDP is missing m=audio"),
            Self::MultipleMediaLines => f.write_str("only one SDP media line is supported"),
            Self::UnsupportedMedia(value) => write!(f, "unsupported SDP media: {value}"),
            Self::UnsupportedTransport(value) => {
                write!(f, "unsupported SDP transport: {value}")
            }
            Self::MissingConnection => f.write_str("SDP is missing an effective c= line"),
            Self::InvalidConnection(value) => write!(f, "invalid SDP connection: {value}"),
            Self::InvalidMediaLine(value) => write!(f, "invalid SDP media line: {value}"),
            Self::InvalidRtcpAttribute(value) => {
                write!(f, "invalid SDP a=rtcp attribute: {value}")
            }
            Self::InvalidPayloadType(value) => {
                write!(f, "invalid SDP payload type: {value}")
            }
            Self::InvalidRtpmapAttribute(value) => {
                write!(f, "invalid SDP a=rtpmap attribute: {value}")
            }
            Self::DuplicateRtpmapAttribute(payload_type) => {
                write!(f, "duplicate SDP a=rtpmap for payload type {payload_type}")
            }
            Self::InvalidFmtpAttribute(value) => {
                write!(f, "invalid SDP a=fmtp attribute: {value}")
            }
            Self::DuplicateFmtpAttribute(payload_type) => {
                write!(f, "duplicate SDP a=fmtp for payload type {payload_type}")
            }
            Self::UnsupportedAttribute(value) => {
                write!(f, "unsupported SDP attribute: {value}")
            }
        }
    }
}

impl std::error::Error for SdpError {}

pub fn parse_audio_sdp(body: &SdpBody) -> Result<ParsedAudioSdp, SdpError> {
    let lines = sdp_lines(body)?;
    let media_index = media_index(&lines)?;
    parse_audio_lines(&lines, media_index)
}

pub fn rewrite_audio_sdp(body: &SdpBody, relay: RelayEndpoint) -> Result<PreparedSdp, SdpError> {
    let mut lines = sdp_lines(body)?;
    let media_index = media_index(&lines)?;
    let parsed = parse_audio_lines(&lines, media_index)?;

    let media_tokens: Vec<&str> = lines[media_index][2..].split_whitespace().collect();
    let payloads = media_tokens[3..].join(" ");
    lines[media_index] = format!("m=audio {} RTP/AVP {payloads}", relay.rtp.port());
    rewrite_origin_address(&mut lines, relay.rtp.ip());

    let media_connection_index = lines
        .iter()
        .enumerate()
        .skip(media_index + 1)
        .find_map(|(index, line)| line.starts_with("c=").then_some(index));
    let session_connection_index = lines[..media_index]
        .iter()
        .enumerate()
        .find_map(|(index, line)| line.starts_with("c=").then_some(index));
    let connection_index = media_connection_index
        .or(session_connection_index)
        .ok_or(SdpError::MissingConnection)?;
    lines[connection_index] = connection_line(relay.rtp.ip());

    let rtcp_index = lines
        .iter()
        .enumerate()
        .skip(media_index + 1)
        .find_map(|(index, line)| line.starts_with("a=rtcp:").then_some(index));
    if let Some(rtcp) = relay.rtcp {
        let line = rtcp_line(rtcp);
        if let Some(index) = rtcp_index {
            lines[index] = line;
        } else {
            lines.insert(media_index + 1, line);
        }
    } else if let Some(index) = rtcp_index {
        lines.remove(index);
    }

    Ok(PreparedSdp {
        body: SdpBody {
            content_type: "application/sdp".to_string(),
            text: format!("{}\r\n", lines.join("\r\n")),
        },
        parsed,
    })
}

fn sdp_lines(body: &SdpBody) -> Result<Vec<String>, SdpError> {
    let media_type = body
        .content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim();
    if !media_type.eq_ignore_ascii_case("application/sdp") {
        return Err(SdpError::UnsupportedContentType(body.content_type.clone()));
    }
    Ok(body
        .text
        .lines()
        .map(|line| line.trim_end_matches('\r').to_string())
        .filter(|line| !line.is_empty())
        .collect())
}

fn rewrite_origin_address(lines: &mut [String], ip: IpAddr) {
    let Some(origin) = lines.iter_mut().find(|line| line.starts_with("o=")) else {
        return;
    };
    let mut tokens = origin[2..]
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    if tokens.len() < 6 {
        return;
    }
    tokens[3] = "IN".to_string();
    tokens[4] = match ip {
        IpAddr::V4(_) => "IP4",
        IpAddr::V6(_) => "IP6",
    }
    .to_string();
    tokens[5] = ip.to_string();
    *origin = format!("o={}", tokens.join(" "));
}

fn media_index(lines: &[String]) -> Result<usize, SdpError> {
    let media_lines: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| line.starts_with("m=").then_some(index))
        .collect();
    match media_lines.as_slice() {
        [] => Err(SdpError::MissingMedia),
        [index] => Ok(*index),
        _ => Err(SdpError::MultipleMediaLines),
    }
}

fn parse_audio_lines(lines: &[String], media_index: usize) -> Result<ParsedAudioSdp, SdpError> {
    if let Some(attribute) = lines.iter().find(|line| {
        line.starts_with("a=ice-")
            || line.starts_with("a=candidate:")
            || line.starts_with("a=fingerprint:")
            || line.starts_with("a=setup:")
            || line.starts_with("a=group:BUNDLE")
            || line.starts_with("a=mid:")
    }) {
        return Err(SdpError::UnsupportedAttribute(attribute.clone()));
    }

    let media_line = &lines[media_index];
    let tokens: Vec<&str> = media_line[2..].split_whitespace().collect();
    if tokens.len() < 4 {
        return Err(SdpError::InvalidMediaLine(media_line.clone()));
    }
    if tokens[0] != "audio" {
        return Err(SdpError::UnsupportedMedia(tokens[0].to_string()));
    }
    if tokens[1].contains('/') {
        return Err(SdpError::InvalidMediaLine(media_line.clone()));
    }
    let rtp_port = tokens[1]
        .parse::<u16>()
        .map_err(|_| SdpError::InvalidMediaLine(media_line.clone()))?;
    if rtp_port == 0 {
        return Err(SdpError::InvalidMediaLine(media_line.clone()));
    }
    if tokens[2] != "RTP/AVP" {
        return Err(SdpError::UnsupportedTransport(tokens[2].to_string()));
    }
    let payload_types = tokens[3..]
        .iter()
        .map(|value| {
            let payload_type = value
                .parse::<u8>()
                .map_err(|_| SdpError::InvalidPayloadType((*value).to_string()))?;
            if payload_type > 127 {
                return Err(SdpError::InvalidPayloadType((*value).to_string()));
            }
            Ok(payload_type)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (payloads, telephone_event) = parse_audio_payloads(lines, media_index, &payload_types)?;

    let connection = lines
        .iter()
        .skip(media_index + 1)
        .find(|line| line.starts_with("c="))
        .or_else(|| {
            lines[..media_index]
                .iter()
                .find(|line| line.starts_with("c="))
        })
        .ok_or(SdpError::MissingConnection)?;
    let remote_ip = parse_connection(connection)?;
    let remote_rtp = SocketAddr::new(remote_ip, rtp_port);
    let remote_rtcp = match lines
        .iter()
        .skip(media_index + 1)
        .find(|line| line.starts_with("a=rtcp:"))
    {
        Some(line) => Some(parse_rtcp(line, remote_ip)?),
        None => rtp_port
            .checked_add(1)
            .map(|port| SocketAddr::new(remote_ip, port)),
    };

    let direction = lines
        .iter()
        .skip(media_index + 1)
        .find_map(|line| parse_direction(line))
        .or_else(|| {
            lines[..media_index]
                .iter()
                .find_map(|line| parse_direction(line))
        })
        .unwrap_or(MediaDirection::SendRecv);

    Ok(ParsedAudioSdp {
        remote_rtp,
        remote_rtcp,
        payload_types,
        payloads,
        telephone_event,
        direction,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Rtpmap {
    encoding: String,
    clock_rate: u32,
    channels: Option<u8>,
}

fn parse_audio_payloads(
    lines: &[String],
    media_index: usize,
    payload_types: &[u8],
) -> Result<(Vec<AudioPayload>, Option<DtmfCapability>), SdpError> {
    let offered = payload_types.iter().copied().collect::<BTreeSet<_>>();
    let mut rtpmaps = BTreeMap::new();
    let mut fmtps = BTreeMap::new();

    for line in lines.iter().skip(media_index + 1) {
        if let Some(value) = line.strip_prefix("a=rtpmap:") {
            let (payload_type, rtpmap) = parse_rtpmap(value, line)?;
            if !offered.contains(&payload_type) {
                continue;
            }
            if rtpmaps.insert(payload_type, rtpmap).is_some() {
                return Err(SdpError::DuplicateRtpmapAttribute(payload_type));
            }
        } else if let Some(value) = line.strip_prefix("a=fmtp:") {
            let (payload_type, fmtp) = parse_fmtp(value, line)?;
            if !offered.contains(&payload_type) {
                continue;
            }
            if fmtps.insert(payload_type, fmtp).is_some() {
                return Err(SdpError::DuplicateFmtpAttribute(payload_type));
            }
        }
    }

    let payloads = payload_types
        .iter()
        .map(|payload_type| {
            let rtpmap = rtpmaps.get(payload_type);
            AudioPayload {
                payload_type: *payload_type,
                encoding: rtpmap.map(|value| value.encoding.clone()),
                clock_rate: rtpmap.map(|value| value.clock_rate),
                channels: rtpmap.and_then(|value| value.channels),
                fmtp: fmtps.get(payload_type).cloned(),
            }
        })
        .collect::<Vec<_>>();

    let mut telephone_event = None;
    for payload in &payloads {
        if !payload
            .encoding
            .as_deref()
            .is_some_and(|encoding| encoding.eq_ignore_ascii_case("telephone-event"))
        {
            continue;
        }
        if telephone_event.is_some() {
            return Err(SdpError::InvalidRtpmapAttribute(
                "multiple telephone-event payloads are not supported".to_string(),
            ));
        }
        let clock_rate = payload.clock_rate.ok_or_else(|| {
            SdpError::InvalidRtpmapAttribute(format!(
                "telephone-event payload {} is missing a clock rate",
                payload.payload_type
            ))
        })?;
        let events = match payload.fmtp.as_deref() {
            Some(value) => parse_dtmf_event_set(value)?,
            None => DtmfEventSet {
                events: (0..=15).collect(),
            },
        };
        telephone_event = Some(DtmfCapability {
            payload_type: payload.payload_type,
            clock_rate,
            detectable: clock_rate == 8_000 && events.events.iter().any(|event| *event <= 15),
            events,
        });
    }

    Ok((payloads, telephone_event))
}

fn parse_rtpmap(value: &str, line: &str) -> Result<(u8, Rtpmap), SdpError> {
    let (payload_type, mapping) = value
        .split_once(char::is_whitespace)
        .ok_or_else(|| SdpError::InvalidRtpmapAttribute(line.to_string()))?;
    let payload_type = parse_attribute_payload_type(payload_type, line, true)?;
    let fields = mapping.trim().split('/').collect::<Vec<_>>();
    if !(2..=3).contains(&fields.len()) || fields[0].is_empty() {
        return Err(SdpError::InvalidRtpmapAttribute(line.to_string()));
    }
    let clock_rate = fields[1]
        .parse::<u32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| SdpError::InvalidRtpmapAttribute(line.to_string()))?;
    let channels = fields
        .get(2)
        .map(|value| {
            value
                .parse::<u8>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| SdpError::InvalidRtpmapAttribute(line.to_string()))
        })
        .transpose()?;
    Ok((
        payload_type,
        Rtpmap {
            encoding: fields[0].to_string(),
            clock_rate,
            channels,
        },
    ))
}

fn parse_fmtp(value: &str, line: &str) -> Result<(u8, String), SdpError> {
    let (payload_type, parameters) = value
        .split_once(char::is_whitespace)
        .ok_or_else(|| SdpError::InvalidFmtpAttribute(line.to_string()))?;
    let payload_type = parse_attribute_payload_type(payload_type, line, false)?;
    let parameters = parameters.trim();
    if parameters.is_empty() {
        return Err(SdpError::InvalidFmtpAttribute(line.to_string()));
    }
    Ok((payload_type, parameters.to_string()))
}

fn parse_attribute_payload_type(value: &str, line: &str, rtpmap: bool) -> Result<u8, SdpError> {
    value
        .parse::<u8>()
        .ok()
        .filter(|value| *value <= 127)
        .ok_or_else(|| {
            if rtpmap {
                SdpError::InvalidRtpmapAttribute(line.to_string())
            } else {
                SdpError::InvalidFmtpAttribute(line.to_string())
            }
        })
}

fn parse_dtmf_event_set(value: &str) -> Result<DtmfEventSet, SdpError> {
    let mut events = BTreeSet::new();
    for item in value.split(',') {
        let item = item.trim();
        if item.is_empty() {
            return Err(SdpError::InvalidFmtpAttribute(value.to_string()));
        }
        if let Some((start, end)) = item.split_once('-') {
            let start = parse_dtmf_event(start, value)?;
            let end = parse_dtmf_event(end, value)?;
            if start > end {
                return Err(SdpError::InvalidFmtpAttribute(value.to_string()));
            }
            events.extend(start..=end);
        } else {
            events.insert(parse_dtmf_event(item, value)?);
        }
    }
    if events.is_empty() {
        return Err(SdpError::InvalidFmtpAttribute(value.to_string()));
    }
    Ok(DtmfEventSet { events })
}

fn parse_dtmf_event(value: &str, attribute: &str) -> Result<u8, SdpError> {
    value
        .trim()
        .parse::<u8>()
        .map_err(|_| SdpError::InvalidFmtpAttribute(attribute.to_string()))
}

fn parse_direction(line: &str) -> Option<MediaDirection> {
    match line {
        "a=sendonly" => Some(MediaDirection::SendOnly),
        "a=recvonly" => Some(MediaDirection::RecvOnly),
        "a=inactive" => Some(MediaDirection::Inactive),
        "a=sendrecv" => Some(MediaDirection::SendRecv),
        _ => None,
    }
}

fn parse_connection(line: &str) -> Result<IpAddr, SdpError> {
    let tokens: Vec<&str> = line[2..].split_whitespace().collect();
    if tokens.len() != 3 || tokens[0] != "IN" || !matches!(tokens[1], "IP4" | "IP6") {
        return Err(SdpError::InvalidConnection(line.to_string()));
    }
    tokens[2]
        .parse()
        .map_err(|_| SdpError::InvalidConnection(line.to_string()))
}

fn parse_rtcp(line: &str, default_ip: IpAddr) -> Result<SocketAddr, SdpError> {
    let tokens: Vec<&str> = line["a=rtcp:".len()..].split_whitespace().collect();
    let port = tokens
        .first()
        .ok_or_else(|| SdpError::InvalidRtcpAttribute(line.to_string()))?
        .parse::<u16>()
        .map_err(|_| SdpError::InvalidRtcpAttribute(line.to_string()))?;
    match tokens.as_slice() {
        [_] => Ok(SocketAddr::new(default_ip, port)),
        [_, "IN", "IP4" | "IP6", address] => {
            let ip = address
                .parse()
                .map_err(|_| SdpError::InvalidRtcpAttribute(line.to_string()))?;
            Ok(SocketAddr::new(ip, port))
        }
        _ => Err(SdpError::InvalidRtcpAttribute(line.to_string())),
    }
}

fn connection_line(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(_) => format!("c=IN IP4 {ip}"),
        IpAddr::V6(_) => format!("c=IN IP6 {ip}"),
    }
}

fn rtcp_line(address: SocketAddr) -> String {
    match address.ip() {
        IpAddr::V4(_) => format!("a=rtcp:{} IN IP4 {}", address.port(), address.ip()),
        IpAddr::V6(_) => format!("a=rtcp:{} IN IP6 {}", address.port(), address.ip()),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct RtpDirectionStats {
    pub packets: u64,
    pub bytes: u64,
    pub lost: u64,
    pub out_of_order: u64,
    pub duplicates: u64,
    pub jitter_rtp_units: Option<f64>,
    pub jitter_ms: Option<f64>,
    pub ssrc_count: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct RtcpStats {
    pub reports: u64,
    pub fraction_lost: Option<f64>,
    pub cumulative_lost: Option<i64>,
    pub interarrival_jitter: Option<u32>,
    pub round_trip_time_ms: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RecordingStats {
    pub enabled: bool,
    pub packets_tapped: u64,
    pub packets_dropped: u64,
    pub bytes_tapped: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct MediaStatsSnapshot {
    pub observed_at_ms: u64,
    pub first_rtp_at_ms: Option<u64>,
    pub last_rtp_at_ms: Option<u64>,
    pub rx: RtpDirectionStats,
    pub tx: RtpDirectionStats,
    pub rtcp: Option<RtcpStats>,
    pub recording: RecordingStats,
}

impl MediaStatsSnapshot {
    pub fn merge_from(&mut self, later: &Self) {
        self.observed_at_ms = self.observed_at_ms.max(later.observed_at_ms);
        self.first_rtp_at_ms = match (self.first_rtp_at_ms, later.first_rtp_at_ms) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        self.last_rtp_at_ms = match (self.last_rtp_at_ms, later.last_rtp_at_ms) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
        merge_rtp_stats(&mut self.rx, &later.rx);
        merge_rtp_stats(&mut self.tx, &later.tx);
        self.recording.packets_tapped = self
            .recording
            .packets_tapped
            .max(later.recording.packets_tapped);
        self.recording.packets_dropped = self
            .recording
            .packets_dropped
            .max(later.recording.packets_dropped);
        self.recording.bytes_tapped = self
            .recording
            .bytes_tapped
            .max(later.recording.bytes_tapped);
        self.recording.enabled |= later.recording.enabled;
        if later.recording.error.is_some() {
            self.recording.error.clone_from(&later.recording.error);
        }
        if later.rtcp.is_some() {
            self.rtcp.clone_from(&later.rtcp);
        }
    }
}

fn merge_rtp_stats(current: &mut RtpDirectionStats, later: &RtpDirectionStats) {
    current.packets = current.packets.max(later.packets);
    current.bytes = current.bytes.max(later.bytes);
    current.lost = current.lost.max(later.lost);
    current.out_of_order = current.out_of_order.max(later.out_of_order);
    current.duplicates = current.duplicates.max(later.duplicates);
    current.ssrc_count = current.ssrc_count.max(later.ssrc_count);
    if later.jitter_rtp_units.is_some() {
        current.jitter_rtp_units = later.jitter_rtp_units;
    }
    if later.jitter_ms.is_some() {
        current.jitter_ms = later.jitter_ms;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodecSummary {
    pub name: String,
    pub payload_type: u8,
    pub clock_rate: u32,
    pub channels: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionMediaState {
    pub media_bridge_id: MediaBridgeId,
    pub media_leg_id: MediaLegId,
    pub codec: Option<CodecSummary>,
    pub latest_stats: MediaStatsSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaPlaneKind {
    UserspaceRtpRelay,
    EbpfTcFastPath,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaForwardingMode {
    Userspace,
    FastPath,
    Mixed,
}

impl MediaForwardingMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Userspace => "userspace",
            Self::FastPath => "fast_path",
            Self::Mixed => "mixed",
        }
    }
}

impl Display for MediaForwardingMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for MediaForwardingMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "userspace" => Ok(Self::Userspace),
            "fast_path" => Ok(Self::FastPath),
            "mixed" => Ok(Self::Mixed),
            _ => Err(format!("unknown media forwarding mode: {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MediaFlowDirection {
    CallerToCallee,
    CalleeToCaller,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct DirectionForwardingHistory {
    promoted: bool,
    demoted: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MediaForwardingHistory {
    caller_to_callee: DirectionForwardingHistory,
    callee_to_caller: DirectionForwardingHistory,
}

impl MediaForwardingHistory {
    pub fn mark_promoted(&mut self, direction: MediaFlowDirection) {
        self.direction_mut(direction).promoted = true;
    }

    pub fn mark_demoted(&mut self, direction: MediaFlowDirection) {
        let history = self.direction_mut(direction);
        if history.promoted {
            history.demoted = true;
        }
    }

    pub fn mode(self) -> MediaForwardingMode {
        let caller_to_callee = self.caller_to_callee;
        let callee_to_caller = self.callee_to_caller;
        if !caller_to_callee.promoted && !callee_to_caller.promoted {
            MediaForwardingMode::Userspace
        } else if caller_to_callee.promoted
            && callee_to_caller.promoted
            && !caller_to_callee.demoted
            && !callee_to_caller.demoted
        {
            MediaForwardingMode::FastPath
        } else {
            MediaForwardingMode::Mixed
        }
    }

    pub fn effective_mode(self, stats: FastPathStats) -> MediaForwardingMode {
        let mode = self.mode();
        if mode == MediaForwardingMode::Mixed {
            return mode;
        }
        let fast_path_packets = stats
            .caller_to_callee_packets
            .saturating_add(stats.callee_to_caller_packets);
        let redirect_errors = stats
            .caller_to_callee_redirect_errors
            .saturating_add(stats.callee_to_caller_redirect_errors);
        if redirect_errors == 0 {
            mode
        } else if fast_path_packets == 0 {
            MediaForwardingMode::Userspace
        } else {
            MediaForwardingMode::Mixed
        }
    }

    fn direction_mut(&mut self, direction: MediaFlowDirection) -> &mut DirectionForwardingHistory {
        match direction {
            MediaFlowDirection::CallerToCallee => &mut self.caller_to_callee,
            MediaFlowDirection::CalleeToCaller => &mut self.callee_to_caller,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FastPathAvailability {
    Available,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FastPathBridgeSpec {
    pub bridge_id: String,
    pub generation: u64,
    pub flows: Vec<FastPathFlowSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FastPathFlowSpec {
    pub media_kind: FastPathMediaKind,
    pub direction: MediaFlowDirection,
    pub local: SocketAddrV4,
    pub remote: SocketAddrV4,
    pub rewritten_source: SocketAddrV4,
    pub rewritten_destination: SocketAddrV4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FastPathMediaKind {
    Rtp,
    Rtcp,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FastPathStats {
    pub caller_to_callee_packets: u64,
    pub caller_to_callee_bytes: u64,
    pub caller_to_callee_redirect_errors: u64,
    pub callee_to_caller_packets: u64,
    pub callee_to_caller_bytes: u64,
    pub callee_to_caller_redirect_errors: u64,
    pub caller_to_callee_rtcp_packets: u64,
    pub caller_to_callee_rtcp_bytes: u64,
    pub caller_to_callee_rtcp_redirect_errors: u64,
    pub callee_to_caller_rtcp_packets: u64,
    pub callee_to_caller_rtcp_bytes: u64,
    pub callee_to_caller_rtcp_redirect_errors: u64,
}

impl FastPathStats {
    pub fn merge_from(&mut self, later: Self) {
        self.caller_to_callee_packets = self
            .caller_to_callee_packets
            .saturating_add(later.caller_to_callee_packets);
        self.caller_to_callee_bytes = self
            .caller_to_callee_bytes
            .saturating_add(later.caller_to_callee_bytes);
        self.caller_to_callee_redirect_errors = self
            .caller_to_callee_redirect_errors
            .saturating_add(later.caller_to_callee_redirect_errors);
        self.callee_to_caller_packets = self
            .callee_to_caller_packets
            .saturating_add(later.callee_to_caller_packets);
        self.callee_to_caller_bytes = self
            .callee_to_caller_bytes
            .saturating_add(later.callee_to_caller_bytes);
        self.callee_to_caller_redirect_errors = self
            .callee_to_caller_redirect_errors
            .saturating_add(later.callee_to_caller_redirect_errors);
        self.caller_to_callee_rtcp_packets = self
            .caller_to_callee_rtcp_packets
            .saturating_add(later.caller_to_callee_rtcp_packets);
        self.caller_to_callee_rtcp_bytes = self
            .caller_to_callee_rtcp_bytes
            .saturating_add(later.caller_to_callee_rtcp_bytes);
        self.caller_to_callee_rtcp_redirect_errors = self
            .caller_to_callee_rtcp_redirect_errors
            .saturating_add(later.caller_to_callee_rtcp_redirect_errors);
        self.callee_to_caller_rtcp_packets = self
            .callee_to_caller_rtcp_packets
            .saturating_add(later.callee_to_caller_rtcp_packets);
        self.callee_to_caller_rtcp_bytes = self
            .callee_to_caller_rtcp_bytes
            .saturating_add(later.callee_to_caller_rtcp_bytes);
        self.callee_to_caller_rtcp_redirect_errors = self
            .callee_to_caller_rtcp_redirect_errors
            .saturating_add(later.callee_to_caller_rtcp_redirect_errors);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FastPathFallbackReason {
    RecordingEnabled,
    AiMediaTap,
    DtmfCapability,
    UnsupportedMedia,
    RemoteEndpointChanged,
    RedirectErrors,
    RouteUnavailable,
    ControllerFailure(String),
    Teardown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FastPathError {
    pub code: String,
    pub message: String,
}

pub trait FastPathController: Send + Sync {
    fn availability(&self) -> FastPathAvailability;
    fn promote(&self, spec: &FastPathBridgeSpec) -> Result<(), FastPathError>;
    fn snapshot(&self, bridge_id: &str, generation: u64) -> Result<FastPathStats, FastPathError>;
    fn demote(
        &self,
        bridge_id: &str,
        generation: u64,
        reason: FastPathFallbackReason,
    ) -> Result<FastPathStats, FastPathError>;
    fn remove(&self, bridge_id: &str, generation: u64) -> Result<FastPathStats, FastPathError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordingMode {
    Disabled,
    DualLegRtp,
    DualChannelPcm,
    MixedPcm,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaBridgeSpec {
    pub bridge_id: MediaBridgeId,
    pub domain_id: DomainId,
    pub call_id: CallId,
    pub caller_leg_id: MediaLegId,
    pub callee_leg_id: MediaLegId,
    pub recording: RecordingMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaLeg {
    pub id: MediaLegId,
    pub local_rtp: SocketAddr,
    pub local_rtcp: Option<SocketAddr>,
    pub remote_rtp: Option<SocketAddr>,
    pub remote_rtcp: Option<SocketAddr>,
    pub direction: MediaDirection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaBridge {
    pub id: MediaBridgeId,
    pub domain_id: DomainId,
    pub call_id: CallId,
    pub caller_leg: MediaLeg,
    pub callee_leg: MediaLeg,
    pub mode: MediaPlaneKind,
    pub recording: RecordingMode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MediaBridgeFinalStats {
    pub bridge_id: MediaBridgeId,
    pub legs: Vec<(MediaLegId, MediaStatsSnapshot)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaError {
    Sdp(SdpError),
    BridgeNotFound(MediaBridgeId),
    ResourceExhausted,
    InvalidState(String),
    Io(String),
}

impl From<SdpError> for MediaError {
    fn from(value: SdpError) -> Self {
        Self::Sdp(value)
    }
}

pub trait MediaTap: Send + Sync {
    fn on_rtp(&self, bridge_id: &MediaBridgeId, leg_id: &MediaLegId, packet: &[u8]);
    fn on_rtcp(&self, bridge_id: &MediaBridgeId, leg_id: &MediaLegId, packet: &[u8]);
}

pub type MediaTapRef = Arc<dyn MediaTap>;

pub trait MediaPlane: Send + Sync {
    fn kind(&self) -> MediaPlaneKind;
    fn allocate_bridge(&self, spec: MediaBridgeSpec) -> Result<MediaBridge, MediaError>;
    fn prepare_offer(
        &self,
        bridge_id: &MediaBridgeId,
        caller_offer: &SdpBody,
    ) -> Result<PreparedSdp, MediaError>;
    fn complete_answer(
        &self,
        bridge_id: &MediaBridgeId,
        callee_answer: &SdpBody,
    ) -> Result<PreparedSdp, MediaError>;
    fn start_bridge(&self, bridge_id: &MediaBridgeId) -> Result<(), MediaError>;
    fn stop_bridge(&self, bridge_id: &MediaBridgeId) -> Result<MediaBridgeFinalStats, MediaError>;
    fn attach_tap(&self, bridge_id: &MediaBridgeId, tap: MediaTapRef) -> Result<(), MediaError>;
}

#[derive(Debug, Default)]
pub struct SsrcSet {
    values: BTreeSet<u32>,
}

impl SsrcSet {
    pub fn observe(&mut self, ssrc: u32) {
        self.values.insert(ssrc);
    }

    pub fn count(&self) -> u32 {
        self.values.len().try_into().unwrap_or(u32::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn audio_offer() -> SdpBody {
        SdpBody {
            content_type: "application/sdp".to_string(),
            text: concat!(
                "v=0\r\n",
                "o=- 1 1 IN IP4 192.0.2.10\r\n",
                "s=-\r\n",
                "c=IN IP4 192.0.2.10\r\n",
                "t=0 0\r\n",
                "m=audio 40000 RTP/AVP 0 101\r\n",
                "a=rtcp:40001 IN IP4 192.0.2.10\r\n",
                "a=rtpmap:0 PCMU/8000\r\n",
                "a=rtpmap:101 telephone-event/8000\r\n",
                "a=sendrecv\r\n"
            )
            .to_string(),
        }
    }

    #[test]
    fn parses_and_rewrites_single_audio_offer() {
        let relay = RelayEndpoint {
            rtp: "198.51.100.20:30000".parse().unwrap(),
            rtcp: Some("198.51.100.20:30001".parse().unwrap()),
        };

        let prepared = rewrite_audio_sdp(&audio_offer(), relay).unwrap();

        assert_eq!(
            prepared.parsed.remote_rtp,
            "192.0.2.10:40000".parse::<SocketAddr>().unwrap()
        );
        assert!(
            prepared
                .body
                .text
                .contains("o=- 1 1 IN IP4 198.51.100.20\r\n")
        );
        assert!(prepared.body.text.contains("c=IN IP4 198.51.100.20\r\n"));
        assert!(
            prepared
                .body
                .text
                .contains("m=audio 30000 RTP/AVP 0 101\r\n")
        );
        assert!(
            prepared
                .body
                .text
                .contains("a=rtcp:30001 IN IP4 198.51.100.20\r\n")
        );
        assert!(prepared.body.text.contains("a=rtpmap:0 PCMU/8000\r\n"));
        let telephone_event = prepared.parsed.telephone_event.unwrap();
        assert_eq!(telephone_event.payload_type, 101);
        assert_eq!(telephone_event.clock_rate, 8_000);
        assert!(telephone_event.detectable);
        assert_eq!(
            telephone_event.events.iter().collect::<Vec<_>>(),
            (0..=15).collect::<Vec<_>>()
        );
    }

    #[test]
    fn parses_explicit_telephone_event_set_case_insensitively() {
        let mut body = audio_offer();
        body.text = body
            .text
            .replace("telephone-event/8000", "TeLePhOnE-EvEnT/8000")
            .replace("a=sendrecv", "a=fmtp:101 0-11,14-16\r\na=sendrecv");

        let parsed = parse_audio_sdp(&body).unwrap();
        let capability = parsed.telephone_event.unwrap();

        assert!(capability.detectable);
        assert!(capability.events.contains(0));
        assert!(capability.events.contains(16));
        assert!(!capability.events.contains(12));
        assert_eq!(parsed.payloads[1].fmtp.as_deref(), Some("0-11,14-16"));
    }

    #[test]
    fn ignores_telephone_event_mapping_not_listed_in_media_line() {
        let mut body = audio_offer();
        body.text = body
            .text
            .replace("m=audio 40000 RTP/AVP 0 101", "m=audio 40000 RTP/AVP 0")
            .replace("a=rtpmap:101", "a=rtpmap:102");

        let parsed = parse_audio_sdp(&body).unwrap();

        assert!(parsed.telephone_event.is_none());
        assert_eq!(parsed.payloads.len(), 1);
    }

    #[test]
    fn rejects_duplicate_or_conflicting_rtpmap() {
        let mut duplicate = audio_offer();
        duplicate
            .text
            .push_str("a=rtpmap:101 telephone-event/8000\r\n");
        assert_eq!(
            parse_audio_sdp(&duplicate),
            Err(SdpError::DuplicateRtpmapAttribute(101))
        );

        let mut conflicting = audio_offer();
        conflicting.text.push_str("a=rtpmap:101 PCMA/8000\r\n");
        assert_eq!(
            parse_audio_sdp(&conflicting),
            Err(SdpError::DuplicateRtpmapAttribute(101))
        );
    }

    #[test]
    fn rejects_malformed_telephone_event_fmtp() {
        let mut body = audio_offer();
        body.text.push_str("a=fmtp:101 0-15,broken\r\n");

        assert!(matches!(
            parse_audio_sdp(&body),
            Err(SdpError::InvalidFmtpAttribute(_))
        ));
    }

    #[test]
    fn retains_but_does_not_detect_non_8khz_telephone_event() {
        let mut body = audio_offer();
        body.text = body
            .text
            .replace("telephone-event/8000", "telephone-event/16000");

        let capability = parse_audio_sdp(&body).unwrap().telephone_event.unwrap();

        assert_eq!(capability.clock_rate, 16_000);
        assert!(!capability.detectable);
    }

    #[test]
    fn rejects_multiple_media_lines() {
        let mut body = audio_offer();
        body.text.push_str("m=video 50000 RTP/AVP 96\r\n");

        assert_eq!(parse_audio_sdp(&body), Err(SdpError::MultipleMediaLines));
    }

    #[test]
    fn rejects_secure_transport_in_first_phase() {
        let mut body = audio_offer();
        body.text = body.text.replace("RTP/AVP", "RTP/SAVP");

        assert_eq!(
            parse_audio_sdp(&body),
            Err(SdpError::UnsupportedTransport("RTP/SAVP".to_string()))
        );
    }

    #[test]
    fn rejects_ice_in_first_phase() {
        let mut body = audio_offer();
        body.text.push_str("a=ice-ufrag:test\r\n");

        assert_eq!(
            parse_audio_sdp(&body),
            Err(SdpError::UnsupportedAttribute(
                "a=ice-ufrag:test".to_string()
            ))
        );
    }

    #[test]
    fn rejects_rejected_audio_stream() {
        let mut body = audio_offer();
        body.text = body.text.replace("m=audio 40000", "m=audio 0");

        assert!(matches!(
            parse_audio_sdp(&body),
            Err(SdpError::InvalidMediaLine(_))
        ));
    }

    #[test]
    fn rejects_unsupported_content_type() {
        let mut body = audio_offer();
        body.content_type = "text/plain".to_string();

        assert_eq!(
            parse_audio_sdp(&body),
            Err(SdpError::UnsupportedContentType("text/plain".to_string()))
        );
    }

    #[test]
    fn rejects_missing_or_non_audio_media() {
        let mut missing = audio_offer();
        missing.text = missing
            .text
            .lines()
            .filter(|line| !line.starts_with("m="))
            .collect::<Vec<_>>()
            .join("\r\n");
        assert_eq!(parse_audio_sdp(&missing), Err(SdpError::MissingMedia));

        let mut video = audio_offer();
        video.text = video.text.replace("m=audio", "m=video");
        assert_eq!(
            parse_audio_sdp(&video),
            Err(SdpError::UnsupportedMedia("video".to_string()))
        );
    }

    #[test]
    fn rejects_invalid_payload_connection_and_rtcp() {
        let mut payload = audio_offer();
        payload.text = payload.text.replace("0 101", "0 invalid");
        assert_eq!(
            parse_audio_sdp(&payload),
            Err(SdpError::InvalidPayloadType("invalid".to_string()))
        );

        let mut connection = audio_offer();
        connection.text = connection.text.replace("c=IN IP4 192.0.2.10\r\n", "");
        assert_eq!(
            parse_audio_sdp(&connection),
            Err(SdpError::MissingConnection)
        );

        let mut rtcp = audio_offer();
        rtcp.text = rtcp
            .text
            .replace("a=rtcp:40001 IN IP4 192.0.2.10", "a=rtcp:invalid");
        assert!(matches!(
            parse_audio_sdp(&rtcp),
            Err(SdpError::InvalidRtcpAttribute(_))
        ));
    }

    #[test]
    fn media_connection_and_direction_override_session_values() {
        let mut body = audio_offer();
        body.text = body.text.replace(
            "m=audio 40000 RTP/AVP 0 101\r\n",
            concat!(
                "m=audio 40000 RTP/AVP 0 101\r\n",
                "c=IN IP4 198.51.100.30\r\n",
                "a=rtcp:41000\r\n",
                "a=recvonly\r\n"
            ),
        );

        let parsed = parse_audio_sdp(&body).unwrap();

        assert_eq!(parsed.remote_rtp, "198.51.100.30:40000".parse().unwrap());
        assert_eq!(
            parsed.remote_rtcp,
            Some("198.51.100.30:41000".parse().unwrap())
        );
        assert_eq!(parsed.direction, MediaDirection::RecvOnly);
    }

    #[test]
    fn defaults_rtcp_to_the_port_after_rtp() {
        let mut body = audio_offer();
        body.text = body
            .text
            .lines()
            .filter(|line| !line.starts_with("a=rtcp:"))
            .collect::<Vec<_>>()
            .join("\r\n");

        let parsed = parse_audio_sdp(&body).unwrap();

        assert_eq!(
            parsed.remote_rtcp,
            Some("192.0.2.10:40001".parse().unwrap())
        );
    }

    #[test]
    fn rewrites_ipv6_connection_and_rtcp() {
        let mut body = audio_offer();
        body.text = body.text.replace("IP4 192.0.2.10", "IP6 2001:db8::10");
        let relay = RelayEndpoint {
            rtp: "[2001:db8::20]:30000".parse().unwrap(),
            rtcp: Some("[2001:db8::20]:30001".parse().unwrap()),
        };

        let prepared = rewrite_audio_sdp(&body, relay).unwrap();

        assert!(
            prepared
                .body
                .text
                .contains("o=- 1 1 IN IP6 2001:db8::20\r\n")
        );
        assert!(prepared.body.text.contains("c=IN IP6 2001:db8::20\r\n"));
        assert!(
            prepared
                .body
                .text
                .contains("a=rtcp:30001 IN IP6 2001:db8::20\r\n")
        );
    }

    #[test]
    fn forwarding_history_ignores_initial_userspace_negotiation() {
        assert_eq!(
            MediaForwardingHistory::default().mode(),
            MediaForwardingMode::Userspace
        );
    }

    #[test]
    fn forwarding_history_marks_stable_bidirectional_promotion_as_fast_path() {
        let mut history = MediaForwardingHistory::default();
        history.mark_promoted(MediaFlowDirection::CallerToCallee);
        history.mark_promoted(MediaFlowDirection::CalleeToCaller);

        assert_eq!(history.mode(), MediaForwardingMode::FastPath);
    }

    #[test]
    fn forwarding_history_marks_partial_or_demoted_promotion_as_mixed() {
        let mut partial = MediaForwardingHistory::default();
        partial.mark_promoted(MediaFlowDirection::CallerToCallee);
        assert_eq!(partial.mode(), MediaForwardingMode::Mixed);

        let mut demoted = MediaForwardingHistory::default();
        demoted.mark_promoted(MediaFlowDirection::CallerToCallee);
        demoted.mark_promoted(MediaFlowDirection::CalleeToCaller);
        demoted.mark_demoted(MediaFlowDirection::CallerToCallee);
        assert_eq!(demoted.mode(), MediaForwardingMode::Mixed);
    }

    #[test]
    fn forwarding_history_uses_actual_redirect_results() {
        let mut history = MediaForwardingHistory::default();
        history.mark_promoted(MediaFlowDirection::CallerToCallee);
        history.mark_promoted(MediaFlowDirection::CalleeToCaller);

        assert_eq!(
            history.effective_mode(FastPathStats {
                caller_to_callee_redirect_errors: 10,
                callee_to_caller_redirect_errors: 10,
                ..FastPathStats::default()
            }),
            MediaForwardingMode::Userspace
        );
        assert_eq!(
            history.effective_mode(FastPathStats {
                caller_to_callee_packets: 10,
                callee_to_caller_redirect_errors: 10,
                ..FastPathStats::default()
            }),
            MediaForwardingMode::Mixed
        );
    }

    #[test]
    fn statistics_merge_is_monotonic() {
        let mut current = MediaStatsSnapshot {
            observed_at_ms: 10,
            rx: RtpDirectionStats {
                packets: 100,
                ..Default::default()
            },
            ..Default::default()
        };
        current.merge_from(&MediaStatsSnapshot {
            observed_at_ms: 20,
            rx: RtpDirectionStats {
                packets: 90,
                bytes: 1000,
                jitter_ms: Some(2.5),
                ..Default::default()
            },
            ..Default::default()
        });

        assert_eq!(current.observed_at_ms, 20);
        assert_eq!(current.rx.packets, 100);
        assert_eq!(current.rx.bytes, 1000);
        assert_eq!(current.rx.jitter_ms, Some(2.5));
    }
}
