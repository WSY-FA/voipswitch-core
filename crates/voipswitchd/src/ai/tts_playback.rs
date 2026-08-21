use anyhow::{Result, bail};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlaybackCodec {
    Pcma,
    Pcmu,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlaybackFrame {
    pub(crate) generation: u64,
    pub(crate) timestamp: u32,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Default)]
pub(crate) struct TtsPlayback {
    generation: u64,
    timestamp: u32,
}

impl TtsPlayback {
    pub(crate) fn new(generation: u64) -> Result<Self> {
        if generation == 0 {
            bail!("playback generation must be greater than zero");
        }
        Ok(Self {
            generation,
            timestamp: 0,
        })
    }

    pub(crate) fn interrupt(&mut self, generation: u64) -> Result<()> {
        if generation <= self.generation {
            bail!("playback generation must increase");
        }
        self.generation = generation;
        self.timestamp = 0;
        Ok(())
    }

    pub(crate) fn packetize(
        &mut self,
        pcm16_le: &[u8],
        codec: PlaybackCodec,
    ) -> Result<Vec<PlaybackFrame>> {
        if pcm16_le.len() % 2 != 0 {
            bail!("PCM16LE payload must contain complete samples");
        }
        let samples_per_packet = 160;
        let mut frames = Vec::new();
        for chunk in pcm16_le.chunks(samples_per_packet * 2) {
            let payload = chunk
                .chunks_exact(2)
                .map(|bytes| {
                    let sample = i16::from_le_bytes([bytes[0], bytes[1]]);
                    match codec {
                        PlaybackCodec::Pcma => alaw_encode(sample),
                        PlaybackCodec::Pcmu => ulaw_encode(sample),
                    }
                })
                .collect();
            frames.push(PlaybackFrame {
                generation: self.generation,
                timestamp: self.timestamp,
                payload,
            });
            self.timestamp = self.timestamp.wrapping_add(samples_per_packet as u32);
        }
        Ok(frames)
    }
}

fn ulaw_encode(sample: i16) -> u8 {
    let mut value = i32::from(sample);
    let sign = if value < 0 {
        value = -value;
        0x80
    } else {
        0
    };
    let value = (value + 0x84).min(0x7fff);
    let exponent = (0..8)
        .rev()
        .find(|shift| (value & (0x7f80 >> shift)) != 0)
        .unwrap_or(0);
    let mantissa = (value >> (exponent + 3)) & 0x0f;
    !(sign | (exponent << 4) | mantissa) as u8
}

fn alaw_encode(sample: i16) -> u8 {
    let sign = if sample < 0 { 0x80 } else { 0 };
    let value = i32::from(sample).unsigned_abs().min(0x7fff) as i32;
    let exponent = if value < 256 {
        0
    } else {
        (value.ilog2() as i32 - 7).clamp(0, 7)
    };
    let mantissa = if exponent == 0 {
        (value >> 4) & 0x0f
    } else {
        (value >> (exponent + 3)) & 0x0f
    };
    (sign | (exponent << 4) | mantissa) as u8 ^ 0x55
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn packetizes_twenty_ms_and_interrupts_generation() {
        let mut playback = TtsPlayback::new(1).unwrap();
        let frames = playback
            .packetize(&vec![0; 320 * 2], PlaybackCodec::Pcma)
            .unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].payload.len(), 160);
        assert_eq!(frames[1].timestamp, 160);
        playback.interrupt(2).unwrap();
        assert_eq!(
            playback.packetize(&[0, 0], PlaybackCodec::Pcmu).unwrap()[0].generation,
            2
        );
    }
}
