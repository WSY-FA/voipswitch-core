use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::reload;
use tracing_subscriber::util::SubscriberInitExt;

type ReloadFn = dyn Fn(&str) -> Result<(), String> + Send + Sync;

struct LogReloader {
    apply: Box<ReloadFn>,
}

static LOG_RELOADER: OnceLock<LogReloader> = OnceLock::new();

struct LocalTimer;

impl FormatTime for LocalTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(
            w,
            "{}",
            chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z")
        )
    }
}

pub fn resolve_log_dir(cli: Option<PathBuf>) -> PathBuf {
    cli.or_else(|| std::env::var_os("VOIPSWITCH_LOG_DIR").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("logs"))
}

pub fn init(log_dir: &Path) -> Result<WorkerGuard> {
    std::fs::create_dir_all(log_dir)
        .with_context(|| format!("create log directory {}", log_dir.display()))?;
    let appender = tracing_appender::rolling::daily(log_dir, "voipswitchd.log");
    let (writer, guard) = tracing_appender::non_blocking(appender);
    let initial_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| application_filter("info"));
    let (filter, reload_handle) = reload::Layer::new(initial_filter);
    let console = tracing_subscriber::fmt::layer().with_timer(LocalTimer);
    let file = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_timer(LocalTimer)
        .with_writer(writer);
    tracing_subscriber::registry()
        .with(filter)
        .with(console)
        .with(file)
        .init();
    LOG_RELOADER
        .set(LogReloader {
            apply: Box::new(move |level| {
                reload_handle
                    .reload(application_filter(level))
                    .map_err(|err| err.to_string())
            }),
        })
        .map_err(|_| anyhow::anyhow!("logging reload handle initialized more than once"))?;
    Ok(guard)
}

pub fn set_level(level: &str) -> Result<()> {
    anyhow::ensure!(
        crate::config_service::is_log_level(level),
        "invalid log level: {level}"
    );
    let Some(reloader) = LOG_RELOADER.get() else {
        return Ok(());
    };
    (reloader.apply)(level).map_err(anyhow::Error::msg)
}

fn application_filter(level: &str) -> EnvFilter {
    EnvFilter::new(format!("warn,voipswitchd={level},voipswitch_core={level}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_filter_accepts_supported_levels() {
        for level in ["error", "warn", "info", "debug", "trace"] {
            assert!(application_filter(level).to_string().contains(level));
        }
    }
}
