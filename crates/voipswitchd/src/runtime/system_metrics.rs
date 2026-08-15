use serde::Serialize;
use std::fs;
use voipswitch_core::types::time::unix_timestamp_ms;

#[derive(Debug, Clone, Serialize)]
pub struct SystemMetricsSnapshot {
    pub sampled_at_ms: u64,
    pub cpu_percent: Option<f64>,
    pub memory_used_bytes: Option<u64>,
    pub memory_total_bytes: Option<u64>,
    pub active_call_count: usize,
}

#[derive(Debug, Default)]
pub struct SystemMetricsSampler {
    previous_cpu: Option<CpuTimes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CpuTimes {
    total: u64,
    idle: u64,
}

impl SystemMetricsSampler {
    pub fn sample(&mut self, active_call_count: usize) -> SystemMetricsSnapshot {
        let current_cpu = read_cpu_times().ok();
        let cpu_percent = current_cpu.and_then(|current| {
            self.previous_cpu
                .map(|previous| calculate_cpu_percent(previous, current))
        });
        if current_cpu.is_some() {
            self.previous_cpu = current_cpu;
        }
        let (memory_used_bytes, memory_total_bytes) = read_memory()
            .map(|(used, total)| (Some(used), Some(total)))
            .unwrap_or((None, None));
        SystemMetricsSnapshot {
            sampled_at_ms: unix_timestamp_ms(),
            cpu_percent,
            memory_used_bytes,
            memory_total_bytes,
            active_call_count,
        }
    }
}

fn read_cpu_times() -> std::io::Result<CpuTimes> {
    parse_cpu_times(&fs::read_to_string("/proc/stat")?)
        .ok_or_else(|| std::io::Error::other("missing aggregate cpu line"))
}

fn parse_cpu_times(input: &str) -> Option<CpuTimes> {
    let line = input.lines().find(|line| line.starts_with("cpu "))?;
    let values = line
        .split_whitespace()
        .skip(1)
        .map(str::parse::<u64>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    if values.len() < 4 {
        return None;
    }
    let idle = values[3].saturating_add(values.get(4).copied().unwrap_or_default());
    Some(CpuTimes {
        total: values.iter().copied().sum(),
        idle,
    })
}

fn calculate_cpu_percent(previous: CpuTimes, current: CpuTimes) -> f64 {
    let total = current.total.saturating_sub(previous.total);
    if total == 0 {
        return 0.0;
    }
    let idle = current.idle.saturating_sub(previous.idle).min(total);
    ((total - idle) as f64 * 100.0 / total as f64).clamp(0.0, 100.0)
}

fn read_memory() -> std::io::Result<(u64, u64)> {
    parse_memory(&fs::read_to_string("/proc/meminfo")?)
        .ok_or_else(|| std::io::Error::other("missing memory totals"))
}

fn parse_memory(input: &str) -> Option<(u64, u64)> {
    let mut total_kib = None;
    let mut available_kib = None;
    for line in input.lines() {
        let (name, value) = line.split_once(':')?;
        let value = value.split_whitespace().next()?.parse::<u64>().ok()?;
        match name {
            "MemTotal" => total_kib = Some(value),
            "MemAvailable" => available_kib = Some(value),
            _ => {}
        }
    }
    let total = total_kib?.saturating_mul(1024);
    let available = available_kib?.saturating_mul(1024).min(total);
    Some((total.saturating_sub(available), total))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cpu_and_calculates_delta_percentage() {
        let previous = parse_cpu_times("cpu  10 0 10 80 0 0 0 0\n").unwrap();
        let current = parse_cpu_times("cpu  20 0 20 160 0 0 0 0\n").unwrap();
        assert!((calculate_cpu_percent(previous, current) - 20.0).abs() < 0.001);
    }

    #[test]
    fn parses_available_memory_as_used_memory() {
        let (used, total) = parse_memory("MemTotal: 1000 kB\nMemAvailable: 250 kB\n").unwrap();
        assert_eq!(total, 1_024_000);
        assert_eq!(used, 768_000);
    }
}
