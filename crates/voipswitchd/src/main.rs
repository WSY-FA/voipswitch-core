mod ai;
mod app;
mod commands;
mod config_service;
mod data_store;
mod logging;
mod pbx;
mod runtime;

use ai::{AiConnector, AiConnectorConfig, AiJobService};
use anyhow::{Context, Result};
use app::AppState;
use clap::Parser;
use data_store::{ConfigBackendSettings, open_config_backend};
use runtime::adapter::run_adapter_runtime_socket;
use runtime::media::MediaPlaneManager;
use std::path::PathBuf;
use tokio::signal;
use tracing::info;
use voipswitch_core::ipc::server::run_control_socket;
use voipswitch_core::types::time::unix_timestamp_ms;

#[derive(Debug, Parser)]
#[command(name = "voipswitchd")]
#[command(about = "VoIPSwitch core daemon")]
struct Args {
    #[arg(long, default_value = "data")]
    data_dir: PathBuf,

    #[arg(long, default_value = "sqlite")]
    db_backend: String,

    #[arg(long)]
    mysql_url: Option<String>,

    #[arg(long)]
    control_socket: Option<PathBuf>,

    #[arg(long)]
    runtime_socket: Option<PathBuf>,

    #[arg(long, default_value = "local")]
    instance_id: String,

    #[arg(long)]
    log_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let log_dir = logging::resolve_log_dir(args.log_dir.clone());
    let _log_guard = logging::init(&log_dir)?;
    let control_socket = args
        .control_socket
        .clone()
        .unwrap_or_else(|| default_socket_path("control.sock"));
    let runtime_socket = args
        .runtime_socket
        .clone()
        .unwrap_or_else(|| default_socket_path("runtime.sock"));
    let ai_control_socket = default_socket_path("ai-control.sock");
    let ai_media_socket = default_socket_path("ai-media.sock");

    let backend = open_config_backend(backend_settings(&args)?).context("open config backend")?;
    backend
        .health_check()
        .context("configuration backend health check")?;
    let runtime_config = backend
        .load_runtime_config()
        .context("load runtime configuration")?;
    logging::set_level(runtime_config.log_level())?;
    let state = AppState::new(runtime_config, backend, unix_timestamp_ms());
    let cdr_writer = crate::runtime::call::cdr_writer::CdrWriter::spawn(state.clone());
    state.set_cdr_writer(cdr_writer);
    let ai_connector = AiConnector::spawn(AiConnectorConfig {
        instance_id: args.instance_id.clone(),
        control_socket: ai_control_socket.clone(),
        media_socket: ai_media_socket.clone(),
    })?;
    let ai_jobs = AiJobService::spawn(state.backend(), ai_connector.clone());
    state.set_ai_jobs(ai_jobs);
    runtime::recording::start_cleanup_task(state.clone());
    let pbx_services = pbx::PbxServices::new();
    let command_service = commands::CommandService::new(state.clone(), &pbx_services);
    let media_plane = MediaPlaneManager::default();

    info!(
        data_dir = %args.data_dir.display(),
        log_dir = %log_dir.display(),
        control_socket = %control_socket.display(),
        runtime_socket = %runtime_socket.display(),
        ai_control_socket = %ai_control_socket.display(),
        ai_media_socket = %ai_media_socket.display(),
        ai_control_connected = ai_connector.metrics().control_connected,
        analyzers = ?pbx_services.analyzer_names(),
        fast_path = ?media_plane.fast_path_availability(),
        "voipswitchd starting"
    );

    let control = tokio::spawn(run_control_socket(command_service, control_socket));
    let runtime = tokio::spawn(run_adapter_runtime_socket(
        state.clone(),
        pbx_services.analysis.clone(),
        media_plane,
        runtime_socket,
    ));

    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("shutdown signal received");
        }
        result = control => {
            result.context("control socket task join")??;
        }
        result = runtime => {
            result.context("runtime socket task join")??;
        }
    }

    Ok(())
}

fn default_socket_path(file_name: &str) -> PathBuf {
    if let Some(runtime_dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(runtime_dir)
            .join("voipswitch")
            .join(file_name);
    }
    PathBuf::from("/tmp/voipswitch").join(file_name)
}

fn backend_settings(args: &Args) -> Result<ConfigBackendSettings> {
    match args.db_backend.as_str() {
        "sqlite" => Ok(ConfigBackendSettings::Sqlite {
            data_dir: args.data_dir.clone(),
            instance_id: args.instance_id.clone(),
        }),
        "mysql" => Ok(ConfigBackendSettings::Mysql {
            url: args
                .mysql_url
                .clone()
                .context("--mysql-url is required when --db-backend mysql")?,
            instance_id: args.instance_id.clone(),
        }),
        value => anyhow::bail!("unsupported --db-backend: {value}"),
    }
}
