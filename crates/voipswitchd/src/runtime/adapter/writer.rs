use super::RuntimeEnvelope;
use anyhow::{Context, Result, anyhow};
use std::time::Duration;
use tokio::io::AsyncWrite;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use voipswitch_core::ipc::frame::write_json_frame;

const ADAPTER_ACTION_QUEUE_CAPACITY: usize = 50_000;
const ADAPTER_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

struct WriteRequest {
    frame: RuntimeEnvelope,
    completion: oneshot::Sender<std::result::Result<(), String>>,
}

#[derive(Clone)]
pub(crate) struct AdapterRuntimeWriter {
    tx: mpsc::Sender<WriteRequest>,
}

impl AdapterRuntimeWriter {
    pub(crate) async fn send(&self, frame: RuntimeEnvelope) -> Result<()> {
        let (completion, result) = oneshot::channel();
        self.tx
            .send(WriteRequest { frame, completion })
            .await
            .map_err(|_| anyhow!("adapter runtime writer stopped"))?;
        result
            .await
            .context("adapter runtime writer dropped completion")?
            .map_err(anyhow::Error::msg)
    }

    pub(crate) fn try_send(
        &self,
        frame: RuntimeEnvelope,
    ) -> Result<oneshot::Receiver<std::result::Result<(), String>>> {
        let (completion, result) = oneshot::channel();
        self.tx
            .try_send(WriteRequest { frame, completion })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => anyhow!("adapter action queue full"),
                mpsc::error::TrySendError::Closed(_) => anyhow!("adapter runtime writer stopped"),
            })?;
        Ok(result)
    }
}

pub(crate) fn spawn_adapter_runtime_writer<W>(
    writer: W,
) -> (AdapterRuntimeWriter, JoinHandle<Result<()>>)
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    spawn_with_settings(writer, ADAPTER_ACTION_QUEUE_CAPACITY, ADAPTER_WRITE_TIMEOUT)
}

fn spawn_with_settings<W>(
    writer: W,
    capacity: usize,
    write_timeout: Duration,
) -> (AdapterRuntimeWriter, JoinHandle<Result<()>>)
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel(capacity);
    let task = tokio::spawn(run_writer(writer, rx, write_timeout));
    (AdapterRuntimeWriter { tx }, task)
}

async fn run_writer<W>(
    mut writer: W,
    mut rx: mpsc::Receiver<WriteRequest>,
    write_timeout: Duration,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    while let Some(request) = rx.recv().await {
        let write_result =
            tokio::time::timeout(write_timeout, write_json_frame(&mut writer, &request.frame))
                .await
                .map_err(|_| anyhow!("adapter runtime write timed out after {write_timeout:?}"))
                .and_then(|result| result.context("write adapter runtime frame"));
        match write_result {
            Ok(()) => {
                let _ = request.completion.send(Ok(()));
            }
            Err(err) => {
                let message = err.to_string();
                let _ = request.completion.send(Err(message.clone()));
                while let Ok(pending) = rx.try_recv() {
                    let _ = pending.completion.send(Err(message.clone()));
                }
                return Err(err);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::duplex;
    use voipswitch_core::ipc::frame::read_json_frame;

    struct PendingWriter;

    struct PartialWriter {
        wrote_prefix: bool,
    }

    impl AsyncWrite for PendingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for PartialWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            if !self.wrote_prefix {
                self.wrote_prefix = true;
                return Poll::Ready(Ok(buffer.len().min(4)));
            }
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "partial frame write failed",
            )))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn writes_runtime_frames_in_enqueue_order() {
        let (client, mut server) = duplex(4096);
        let (writer, task) = spawn_with_settings(client, 2, Duration::from_secs(1));
        let first = RuntimeEnvelope::new("command", "First", Some("one".to_string()), json!({}));
        let second = RuntimeEnvelope::new("command", "Second", Some("two".to_string()), json!({}));

        writer.send(first).await.unwrap();
        writer.send(second).await.unwrap();

        let first: RuntimeEnvelope = read_json_frame(&mut server).await.unwrap();
        let second: RuntimeEnvelope = read_json_frame(&mut server).await.unwrap();
        assert_eq!(first.r#type, "First");
        assert_eq!(second.r#type, "Second");

        drop(writer);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn reports_closed_runtime_connection() {
        let (client, server) = duplex(64);
        drop(server);
        let (writer, task) = spawn_with_settings(client, 1, Duration::from_secs(1));

        let error = writer
            .send(RuntimeEnvelope::new("command", "Closed", None, json!({})))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("write adapter runtime frame"));
        assert!(task.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn enforces_runtime_write_deadline() {
        let (writer, task) = spawn_with_settings(PendingWriter, 1, Duration::from_millis(10));

        let error = writer
            .send(RuntimeEnvelope::new("command", "Blocked", None, json!({})))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("timed out"));
        assert!(task.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn reports_partial_frame_write_to_async_completion() {
        let (writer, task) = spawn_with_settings(
            PartialWriter {
                wrote_prefix: false,
            },
            1,
            Duration::from_secs(1),
        );
        let completion = writer
            .try_send(RuntimeEnvelope::new(
                "command",
                "Partial",
                None,
                json!({ "payload": "frame" }),
            ))
            .expect("action enqueued");

        let error = completion
            .await
            .expect("writer completion returned")
            .expect_err("partial write must fail");
        assert!(error.contains("write adapter runtime frame"));
        assert!(task.await.unwrap().is_err());
    }

    #[test]
    fn action_queue_capacity_is_sized_for_runtime_bursts() {
        assert_eq!(ADAPTER_ACTION_QUEUE_CAPACITY, 50_000);
    }

    #[test]
    fn action_queue_is_bounded() {
        let (tx, _rx) = mpsc::channel::<WriteRequest>(3);
        assert_eq!(tx.max_capacity(), 3);
    }

    #[tokio::test]
    async fn try_send_reports_full_without_waiting() {
        let (tx, _rx) = mpsc::channel::<WriteRequest>(1);
        let writer = AdapterRuntimeWriter { tx };
        writer
            .try_send(RuntimeEnvelope::new("command", "First", None, json!({})))
            .expect("first request fits");

        let error = writer
            .try_send(RuntimeEnvelope::new("command", "Second", None, json!({})))
            .expect_err("second request must observe capacity");
        assert!(error.to_string().contains("queue full"));
    }
}
