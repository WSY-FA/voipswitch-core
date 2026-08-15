use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use voipswitch_core::dtmf::DtmfDigit;
use voipswitch_core::media::DtmfCapability;

const STATE_CAPACITY: usize = 32;
const RECENT_TTL: Duration = Duration::from_secs(2);
const IDLE_TIMEOUT: Duration = Duration::from_millis(250);
const MAX_PRESS_DURATION: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParserConfig {
    pub(crate) generation: u64,
    pub(crate) capability: Option<DtmfCapability>,
    pub(crate) mode: DtmfMediaMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DtmfMediaMode {
    Transparent,
    Observe,
    Collect,
}

impl ParserConfig {
    pub(crate) fn disabled(generation: u64) -> Self {
        Self {
            generation,
            capability: None,
            mode: DtmfMediaMode::Transparent,
        }
    }

    pub(crate) fn transparent(generation: u64, capability: Option<DtmfCapability>) -> Self {
        Self::with_mode(generation, capability, DtmfMediaMode::Transparent)
    }

    pub(crate) fn with_mode(
        generation: u64,
        capability: Option<DtmfCapability>,
        mode: DtmfMediaMode,
    ) -> Self {
        Self {
            generation,
            capability: capability.filter(|value| value.detectable),
            mode,
        }
    }

    fn active_capability(&self) -> Option<&DtmfCapability> {
        (self.mode != DtmfMediaMode::Transparent)
            .then_some(self.capability.as_ref())
            .flatten()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct EventKey {
    ssrc: u32,
    timestamp: u32,
    event: u8,
}

#[derive(Debug, Clone, Copy)]
struct ActivePress {
    digit: DtmfDigit,
    max_duration: u16,
    first_seen: Instant,
    last_seen: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Rfc4733DigitCompleted {
    pub(crate) generation: u64,
    pub(crate) ssrc: u32,
    pub(crate) timestamp: u32,
    pub(crate) event_code: u8,
    pub(crate) digit: DtmfDigit,
    pub(crate) duration_ms: u32,
    pub(crate) incomplete_end: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParserObservation {
    Completed(Rfc4733DigitCompleted),
    Invalid,
}

pub(crate) struct Rfc4733Parser {
    config: ParserConfig,
    active: HashMap<EventKey, ActivePress>,
    recent: VecDeque<(EventKey, Instant)>,
}

impl Rfc4733Parser {
    pub(crate) fn new(config: ParserConfig) -> Self {
        Self {
            config,
            active: HashMap::new(),
            recent: VecDeque::new(),
        }
    }

    pub(crate) fn update_config(&mut self, config: ParserConfig) {
        if self.config != config {
            self.config = config;
            self.active.clear();
            self.recent.clear();
        }
    }

    pub(crate) fn observe_packet(&mut self, packet: &[u8], now: Instant) -> Vec<ParserObservation> {
        let mut observations = self.expire(now);
        let Some(capability) = self.config.active_capability() else {
            return observations;
        };
        let rtp = match parse_rtp(packet) {
            Ok(rtp) => rtp,
            Err(()) => {
                observations.push(ParserObservation::Invalid);
                return observations;
            }
        };
        if rtp.payload_type != capability.payload_type {
            return observations;
        }
        if rtp.payload.len() < 4 {
            observations.push(ParserObservation::Invalid);
            return observations;
        }

        let event = rtp.payload[0];
        let Some(digit) = map_event(event, capability) else {
            return observations;
        };
        let key = EventKey {
            ssrc: rtp.ssrc,
            timestamp: rtp.timestamp,
            event,
        };
        if self.recent.iter().any(|(recent, _)| *recent == key) {
            return observations;
        }

        let duration = u16::from_be_bytes([rtp.payload[2], rtp.payload[3]]);
        let ended = rtp.payload[1] & 0x80 != 0;
        if let Some(active) = self.active.get(&key)
            && duration < active.max_duration
        {
            observations.push(ParserObservation::Invalid);
            return observations;
        }
        if !self.active.contains_key(&key) {
            while self.active.len() + self.recent.len() >= STATE_CAPACITY && !self.recent.is_empty()
            {
                self.recent.pop_front();
            }
            if self.active.len() + self.recent.len() >= STATE_CAPACITY {
                observations.push(ParserObservation::Invalid);
                return observations;
            }
        }
        let active = self.active.entry(key).or_insert(ActivePress {
            digit,
            max_duration: duration,
            first_seen: now,
            last_seen: now,
        });
        active.max_duration = active.max_duration.max(duration);
        active.last_seen = now;

        if ended {
            observations.push(ParserObservation::Completed(self.complete(key, false, now)));
        }
        observations
    }

    pub(crate) fn expire(&mut self, now: Instant) -> Vec<ParserObservation> {
        self.prune_recent(now);
        let expired = self
            .active
            .iter()
            .filter_map(|(key, active)| {
                let idle_deadline = active.last_seen.checked_add(IDLE_TIMEOUT)?;
                let maximum_deadline = active.first_seen.checked_add(MAX_PRESS_DURATION)?;
                (now >= idle_deadline || now >= maximum_deadline).then_some(*key)
            })
            .collect::<Vec<_>>();
        expired
            .into_iter()
            .map(|key| ParserObservation::Completed(self.complete(key, true, now)))
            .collect()
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.active
            .values()
            .filter_map(|active| {
                let idle = active.last_seen.checked_add(IDLE_TIMEOUT)?;
                let maximum = active.first_seen.checked_add(MAX_PRESS_DURATION)?;
                Some(idle.min(maximum))
            })
            .min()
    }

    pub(crate) fn suppresses_packet(&self, packet: &[u8]) -> bool {
        self.config.mode == DtmfMediaMode::Collect
            && self.config.capability.as_ref().is_some_and(|capability| {
                packet.len() >= 2
                    && packet[0] >> 6 == 2
                    && packet[1] & 0x7f == capability.payload_type
            })
    }

    fn complete(
        &mut self,
        key: EventKey,
        incomplete_end: bool,
        now: Instant,
    ) -> Rfc4733DigitCompleted {
        let active = self.active.remove(&key).expect("active DTMF event exists");
        self.recent.push_back((key, now));
        let clock_rate = self
            .config
            .active_capability()
            .as_ref()
            .map(|value| value.clock_rate)
            .unwrap_or(8_000);
        let duration_ms = u32::from(active.max_duration)
            .saturating_mul(1_000)
            .checked_div(clock_rate)
            .unwrap_or_default()
            .min(MAX_PRESS_DURATION.as_millis() as u32);
        Rfc4733DigitCompleted {
            generation: self.config.generation,
            ssrc: key.ssrc,
            timestamp: key.timestamp,
            event_code: key.event,
            digit: active.digit,
            duration_ms,
            incomplete_end,
        }
    }

    fn prune_recent(&mut self, now: Instant) {
        while self
            .recent
            .front()
            .is_some_and(|(_, completed)| now.saturating_duration_since(*completed) >= RECENT_TTL)
        {
            self.recent.pop_front();
        }
    }
}

struct ParsedRtp<'a> {
    payload_type: u8,
    timestamp: u32,
    ssrc: u32,
    payload: &'a [u8],
}

fn parse_rtp(packet: &[u8]) -> Result<ParsedRtp<'_>, ()> {
    if packet.len() < 12 || packet[0] >> 6 != 2 {
        return Err(());
    }
    let csrc_count = usize::from(packet[0] & 0x0f);
    let mut header_len = 12_usize
        .checked_add(csrc_count.checked_mul(4).ok_or(())?)
        .ok_or(())?;
    if header_len > packet.len() {
        return Err(());
    }
    if packet[0] & 0x10 != 0 {
        let extension_header_end = header_len.checked_add(4).ok_or(())?;
        if extension_header_end > packet.len() {
            return Err(());
        }
        let extension_words = usize::from(u16::from_be_bytes([
            packet[header_len + 2],
            packet[header_len + 3],
        ]));
        header_len = extension_header_end
            .checked_add(extension_words.checked_mul(4).ok_or(())?)
            .ok_or(())?;
        if header_len > packet.len() {
            return Err(());
        }
    }
    let mut payload_end = packet.len();
    if packet[0] & 0x20 != 0 {
        let padding = usize::from(*packet.last().ok_or(())?);
        if padding == 0 || padding > payload_end.saturating_sub(header_len) {
            return Err(());
        }
        payload_end -= padding;
    }
    Ok(ParsedRtp {
        payload_type: packet[1] & 0x7f,
        timestamp: u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]),
        ssrc: u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]),
        payload: &packet[header_len..payload_end],
    })
}

fn map_event(event: u8, capability: &DtmfCapability) -> Option<DtmfDigit> {
    if !capability.events.contains(event) {
        return None;
    }
    match event {
        0 => Some(DtmfDigit::D0),
        1 => Some(DtmfDigit::D1),
        2 => Some(DtmfDigit::D2),
        3 => Some(DtmfDigit::D3),
        4 => Some(DtmfDigit::D4),
        5 => Some(DtmfDigit::D5),
        6 => Some(DtmfDigit::D6),
        7 => Some(DtmfDigit::D7),
        8 => Some(DtmfDigit::D8),
        9 => Some(DtmfDigit::D9),
        10 => Some(DtmfDigit::Star),
        11 => Some(DtmfDigit::Pound),
        12 => Some(DtmfDigit::A),
        13 => Some(DtmfDigit::B),
        14 => Some(DtmfDigit::C),
        15 => Some(DtmfDigit::D),
        16 => Some(DtmfDigit::Flash),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use voipswitch_core::media::{SdpBody, parse_audio_sdp};

    fn config(generation: u64) -> ParserConfig {
        let parsed = parse_audio_sdp(&SdpBody {
            content_type: "application/sdp".to_string(),
            text: concat!(
                "v=0\r\n",
                "o=- 1 1 IN IP4 127.0.0.1\r\n",
                "s=-\r\n",
                "c=IN IP4 127.0.0.1\r\n",
                "t=0 0\r\n",
                "m=audio 30000 RTP/AVP 8 101\r\n",
                "a=rtpmap:8 PCMA/8000\r\n",
                "a=rtpmap:101 telephone-event/8000\r\n",
                "a=fmtp:101 0-16\r\n"
            )
            .to_string(),
        })
        .unwrap();
        ParserConfig::with_mode(generation, parsed.telephone_event, DtmfMediaMode::Observe)
    }

    fn packet(sequence: u16, duration: u16, ended: bool) -> Vec<u8> {
        let mut packet = vec![0_u8; 16];
        packet[0] = 0x80;
        packet[1] = 101;
        packet[2..4].copy_from_slice(&sequence.to_be_bytes());
        packet[4..8].copy_from_slice(&42_u32.to_be_bytes());
        packet[8..12].copy_from_slice(&7_u32.to_be_bytes());
        packet[12] = 5;
        packet[13] = if ended { 0x80 } else { 0 };
        packet[14..16].copy_from_slice(&duration.to_be_bytes());
        packet
    }

    fn completions(observations: &[ParserObservation]) -> Vec<Rfc4733DigitCompleted> {
        observations
            .iter()
            .filter_map(|observation| match observation {
                ParserObservation::Completed(value) => Some(*value),
                ParserObservation::Invalid => None,
            })
            .collect()
    }

    #[test]
    fn start_progress_and_repeated_end_complete_once() {
        let now = Instant::now();
        let mut parser = Rfc4733Parser::new(config(3));
        assert!(parser.observe_packet(&packet(1, 80, false), now).is_empty());
        assert!(
            parser
                .observe_packet(&packet(2, 160, false), now)
                .is_empty()
        );
        let completed = parser.observe_packet(&packet(3, 240, true), now);
        assert_eq!(completions(&completed).len(), 1);
        assert_eq!(completions(&completed)[0].duration_ms, 30);
        assert_eq!(completions(&completed)[0].generation, 3);
        assert!(parser.observe_packet(&packet(4, 240, true), now).is_empty());
        assert!(parser.observe_packet(&packet(5, 240, true), now).is_empty());
    }

    #[test]
    fn duration_regression_and_reordering_do_not_duplicate() {
        let now = Instant::now();
        let mut parser = Rfc4733Parser::new(config(1));
        parser.observe_packet(&packet(3, 240, false), now);
        assert_eq!(
            parser.observe_packet(&packet(2, 160, false), now),
            vec![ParserObservation::Invalid]
        );
        let observations = parser.observe_packet(&packet(4, 240, true), now);

        let completed = completions(&observations);
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].duration_ms, 30);
    }

    #[test]
    fn missing_end_completes_after_idle_deadline() {
        let now = Instant::now();
        let mut parser = Rfc4733Parser::new(config(1));
        parser.observe_packet(&packet(1, 800, false), now);

        assert!(parser.expire(now + Duration::from_millis(249)).is_empty());
        let completed = completions(&parser.expire(now + Duration::from_millis(250)));
        assert_eq!(completed.len(), 1);
        assert!(completed[0].incomplete_end);
        assert_eq!(completed[0].duration_ms, 100);
    }

    #[test]
    fn ignores_other_payload_type_and_rejects_invalid_rtp_layouts() {
        let now = Instant::now();
        let mut parser = Rfc4733Parser::new(config(1));
        let mut other = packet(1, 80, true);
        other[1] = 8;
        assert!(parser.observe_packet(&other, now).is_empty());
        assert_eq!(
            parser.observe_packet(&[0x80, 101], now),
            vec![ParserObservation::Invalid]
        );

        let mut bad_extension = packet(1, 80, true);
        bad_extension[0] |= 0x10;
        bad_extension[14..16].copy_from_slice(&10_u16.to_be_bytes());
        assert_eq!(
            parser.observe_packet(&bad_extension, now),
            vec![ParserObservation::Invalid]
        );

        let mut bad_padding = packet(1, 80, true);
        bad_padding[0] |= 0x20;
        *bad_padding.last_mut().unwrap() = 100;
        assert_eq!(
            parser.observe_packet(&bad_padding, now),
            vec![ParserObservation::Invalid]
        );
    }

    #[test]
    fn generation_change_clears_active_and_recent_state() {
        let now = Instant::now();
        let mut parser = Rfc4733Parser::new(config(1));
        parser.observe_packet(&packet(1, 80, false), now);
        parser.update_config(config(2));
        assert!(parser.expire(now + IDLE_TIMEOUT).is_empty());

        let completed = completions(&parser.observe_packet(&packet(2, 160, true), now));
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].generation, 2);

        parser.update_config(config(3));
        let completed = completions(&parser.observe_packet(&packet(3, 240, true), now));
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].generation, 3);
    }
}
