use crate::command_service::{CliSessionContext, CommandRequest, CommandResponse};
use crate::ipc::frame::{read_json_frame, write_json_frame};
use anyhow::{Context, Result};
use std::path::Path;
use std::sync::Arc;
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, info, warn};

pub trait ControlCommandHandler: Send + Sync {
    fn execute(&self, request: CommandRequest) -> CommandResponse;

    fn execute_session(
        &self,
        context: &mut CliSessionContext,
        request: CommandRequest,
    ) -> CommandResponse {
        let _ = context;
        self.execute(request)
    }
}

pub async fn run_control_socket(
    handler: Arc<dyn ControlCommandHandler>,
    path: impl AsRef<Path>,
) -> Result<()> {
    let path = path.as_ref();
    let listener = bind_unix_listener(path)
        .await
        .with_context(|| format!("bind control socket {}", path.display()))?;
    info!(socket = %path.display(), "control socket listening");

    loop {
        let (stream, _) = listener.accept().await.context("accept control client")?;
        let handler = handler.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_control_client(stream, handler).await {
                debug!(error = %err, "control client disconnected");
            }
        });
    }
}

async fn handle_control_client(
    mut stream: UnixStream,
    handler: Arc<dyn ControlCommandHandler>,
) -> Result<()> {
    let mut context = CliSessionContext::default();
    loop {
        let request: CommandRequest = read_json_frame(&mut stream).await?;
        let response = handler.execute_session(&mut context, request);
        let exit = response.exit;
        write_json_frame(&mut stream, &response).await?;
        if exit {
            break;
        }
    }
    Ok(())
}

async fn bind_unix_listener(path: &Path) -> Result<UnixListener> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create socket dir {}", parent.display()))?;
    }

    match tokio::fs::remove_file(path).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            warn!(socket = %path.display(), error = %err, "failed to remove stale socket");
        }
    }

    UnixListener::bind(path).with_context(|| format!("bind {}", path.display()))
}

pub async fn send_command(
    mut stream: UnixStream,
    request: CommandRequest,
) -> Result<CommandResponse> {
    write_json_frame(&mut stream, &request).await?;
    read_json_frame(&mut stream).await
}
