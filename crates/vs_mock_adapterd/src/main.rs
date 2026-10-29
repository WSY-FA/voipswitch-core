//! `vs_mock_adapterd` — loopback SIP adapter for local demos and CI.
//!
//! It speaks the same runtime socket protocol as a real SIP adapter but sends
//! no SIP and no RTP. It registers the endpoints from the runtime view,
//! originates one deterministic inbound call on request, answers it and closes
//! the call after a hold period.
//!
//! It is a test tool: it must not be used as a production SIP adapter and it
//! does not replace SIPp or private-adapter regression tests.

mod protocol;
mod scenario;
mod sdp;

use anyhow::{Context, Result, bail};
use clap::Parser;
use std::path::PathBuf;
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tracing::{info, warn};
use voipswitch_core::ipc::frame::{read_json_frame, write_json_frame};

#[derive(Debug, Parser)]
#[command(name = "vs_mock_adapterd")]
#[command(about = "Loopback SIP adapter for VoIPSwitch core: no SIP, no RTP")]
struct Args {
    /// Core runtime socket. Defaults to $XDG_RUNTIME_DIR/voipswitch/runtime.sock.
    #[arg(long)]
    runtime_socket: Option<PathBuf>,

    /// Start one inbound call, e.g. --auto-call 1001=1002.
    #[arg(long)]
    auto_call: Option<String>,

    /// Keep running after the auto call completes.
    #[arg(long)]
    keep_running: bool,

    /// Time between the callee INVITE and 180 Ringing.
    #[arg(long, default_value_t = 120)]
    provisional_delay_ms: u64,

    /// Time between 180 Ringing and 200 OK.
    #[arg(long, default_value_t = 250)]
    answer_delay_ms: u64,

    /// Time between the answer and the automatic hangup. 0 disables it.
    #[arg(long, default_value_t = 3000)]
    call_hold_ms: u64,

    /// Give up after this long. 0 disables the deadline.
    #[arg(long, default_value_t = 30_000)]
    timeout_ms: u64,

    /// How long to wait for core to accept the connection.
    #[arg(long, default_value_t = 10_000)]
    connect_timeout_ms: u64,

    #[arg(long, default_value = "vs-mock-adapterd")]
    instance_id: String,
}

impl Args {
    fn auto_call_numbers(&self) -> Result<Option<(String, String)>> {
        let Some(raw) = self.auto_call.as_deref() else {
            return Ok(None);
        };
        let (caller, callee) = raw
            .split_once('=')
            .or_else(|| raw.split_once(':'))
            .with_context(|| format!("--auto-call must be CALLER=CALLEE, got {raw:?}"))?;
        if caller.is_empty() || callee.is_empty() {
            bail!("--auto-call needs both caller and callee numbers");
        }
        Ok(Some((caller.to_string(), callee.to_string())))
    }

    fn socket_path(&self) -> PathBuf {
        if let Some(path) = &self.runtime_socket {
            return path.clone();
        }
        match std::env::var_os("XDG_RUNTIME_DIR") {
            Some(dir) => PathBuf::from(dir).join("voipswitch").join("runtime.sock"),
            None => PathBuf::from("/tmp/voipswitch").join("runtime.sock"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let auto_call = args.auto_call_numbers()?;
    let socket_path = args.socket_path();

    let stream =
        connect_with_retry(&socket_path, Duration::from_millis(args.connect_timeout_ms)).await?;
    info!(socket = %socket_path.display(), "connected to core runtime socket");

    let (mut reader, mut writer) = stream.into_split();
    write_json_frame(&mut writer, &protocol::hello(&args.instance_id))
        .await
        .context("send AdapterHello")?;

    let hello_ack: protocol::RuntimeEnvelope =
        tokio::time::timeout(Duration::from_secs(3), read_json_frame(&mut reader))
            .await
            .context("core hello deadline")?
            .context("read CoreHelloAck")?;
    if hello_ack.r#type != "CoreHelloAck" {
        bail!("expected CoreHelloAck, got {}", hello_ack.r#type);
    }
    let accepted: protocol::HelloAck =
        serde_json::from_value(hello_ack.body).context("decode CoreHelloAck")?;
    if !accepted.accepted {
        bail!("core rejected AdapterHello");
    }
    info!(
        protocol_version = ?accepted.protocol_version,
        "handshake complete"
    );

    let (out_tx, out_rx) = mpsc::channel::<protocol::RuntimeEnvelope>(1024);
    let (in_tx, in_rx) = mpsc::channel::<protocol::RuntimeEnvelope>(1024);

    let writer_task = tokio::spawn(async move {
        let mut out_rx = out_rx;
        while let Some(frame) = out_rx.recv().await {
            if let Err(error) = write_json_frame(&mut writer, &frame).await {
                warn!(%error, "runtime socket write failed");
                break;
            }
        }
    });

    tokio::spawn(async move {
        loop {
            match read_json_frame(&mut reader).await {
                Ok(frame) => {
                    if in_tx.send(frame).await.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    info!(%error, "runtime socket reader stopped");
                    break;
                }
            }
        }
    });

    let config = scenario::Config {
        auto_call: auto_call.clone(),
        provisional_delay: Duration::from_millis(args.provisional_delay_ms),
        answer_delay: Duration::from_millis(args.answer_delay_ms),
        call_hold: Duration::from_millis(args.call_hold_ms),
        exit_after_call: auto_call.is_some() && !args.keep_running,
    };

    let run = scenario::Scenario::new(config).run(in_rx, out_tx.clone());
    let deadline = Duration::from_millis(args.timeout_ms);
    let outcome = if deadline.is_zero() {
        run.await
    } else {
        match tokio::time::timeout(deadline, run).await {
            Ok(result) => result,
            Err(_) => {
                drop(out_tx);
                eprintln!("mock adapter: timed out after {} ms", args.timeout_ms);
                std::process::exit(2);
            }
        }
    };

    drop(out_tx);
    let _ = tokio::time::timeout(Duration::from_secs(1), writer_task).await;

    match outcome {
        Ok(()) => {
            info!("loopback scenario finished");
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn connect_with_retry(path: &std::path::Path, budget: Duration) -> Result<UnixStream> {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        match UnixStream::connect(path).await {
            Ok(stream) => return Ok(stream),
            Err(error) if tokio::time::Instant::now() < deadline => {
                tracing::debug!(%error, path = %path.display(), "core not ready, retrying");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("connect runtime socket {}", path.display()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(auto_call: Option<&str>) -> Args {
        Args {
            runtime_socket: None,
            auto_call: auto_call.map(str::to_string),
            keep_running: false,
            provisional_delay_ms: 120,
            answer_delay_ms: 250,
            call_hold_ms: 3000,
            timeout_ms: 30_000,
            connect_timeout_ms: 10_000,
            instance_id: "test".to_string(),
        }
    }

    #[test]
    fn parses_auto_call_pairs() {
        assert_eq!(
            args(Some("1001=1002")).auto_call_numbers().unwrap(),
            Some(("1001".to_string(), "1002".to_string()))
        );
        assert_eq!(
            args(Some("1001:1002")).auto_call_numbers().unwrap(),
            Some(("1001".to_string(), "1002".to_string()))
        );
        assert_eq!(args(None).auto_call_numbers().unwrap(), None);
    }

    #[test]
    fn rejects_incomplete_auto_call() {
        assert!(args(Some("1001")).auto_call_numbers().is_err());
        assert!(args(Some("=1002")).auto_call_numbers().is_err());
    }
}
