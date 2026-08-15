use ai_protocol::PROTOCOL_VERSION;
use ai_protocol::control::{
    AiPipelineType, AiProfileSnapshot, ConnectorHello, ControlEnvelope, ControlMessage,
    GatewayHello, ProfileCatalogRequest, ProfileCatalogSnapshot,
};
use ai_protocol::frame::{read_json_frame, write_json_frame};
use ai_protocol::id::{ConnectorInstanceId, MessageId};
use ai_protocol::media::{MediaFrame, write_media_frame};
use ai_protocol::time::unix_timestamp_ms;
use anyhow::{Context, Result, bail};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::sync::{broadcast, mpsc, watch};
use tracing::{debug, info, warn};

const CONTROL_QUEUE_CAPACITY: usize = 256;
const MEDIA_QUEUE_CAPACITY: usize = 2_048;
const EVENT_QUEUE_CAPACITY: usize = 256;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(3);
const RECONNECT_MIN_DELAY: Duration = Duration::from_millis(100);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub(crate) struct AiConnectorConfig {
    pub(crate) instance_id: String,
    pub(crate) control_socket: PathBuf,
    pub(crate) media_socket: PathBuf,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ConnectorMetricsSnapshot {
    pub(crate) control_connected: bool,
    pub(crate) media_connected: bool,
    pub(crate) control_queue_rejected: u64,
    pub(crate) media_queue_rejected: u64,
    pub(crate) media_write_failed: u64,
}

#[derive(Default)]
struct ConnectorMetrics {
    control_connected: AtomicBool,
    media_connected: AtomicBool,
    control_queue_rejected: AtomicU64,
    media_queue_rejected: AtomicU64,
    media_write_failed: AtomicU64,
}

#[derive(Clone)]
pub(crate) struct AiConnector {
    control_tx: mpsc::Sender<ControlEnvelope>,
    media_tx: mpsc::Sender<MediaFrame>,
    events: broadcast::Sender<ControlEnvelope>,
    connected: watch::Receiver<bool>,
    profile_catalog: watch::Receiver<Option<ProfileCatalogSnapshot>>,
    sequence: Arc<AtomicU64>,
    metrics: Arc<ConnectorMetrics>,
}

impl AiConnector {
    pub(crate) fn spawn(config: AiConnectorConfig) -> Result<Self> {
        let connector_instance_id = ConnectorInstanceId::new(config.instance_id.clone())?;
        let (control_tx, control_rx) = mpsc::channel(CONTROL_QUEUE_CAPACITY);
        let (media_tx, media_rx) = mpsc::channel(MEDIA_QUEUE_CAPACITY);
        let (events, _) = broadcast::channel(EVENT_QUEUE_CAPACITY);
        let (connected_tx, connected) = watch::channel(false);
        let (profile_catalog_tx, profile_catalog) = watch::channel(None);
        let sequence = Arc::new(AtomicU64::new(1));
        let metrics = Arc::new(ConnectorMetrics::default());

        tokio::spawn(run_control_connection(
            config.control_socket,
            connector_instance_id,
            control_rx,
            events.clone(),
            connected_tx,
            sequence.clone(),
            metrics.clone(),
            profile_catalog_tx,
        ));
        tokio::spawn(run_media_connection(
            config.media_socket,
            media_rx,
            metrics.clone(),
        ));

        Ok(Self {
            control_tx,
            media_tx,
            events,
            connected,
            profile_catalog,
            sequence,
            metrics,
        })
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<ControlEnvelope> {
        self.events.subscribe()
    }

    pub(crate) fn is_control_connected(&self) -> bool {
        *self.connected.borrow()
    }

    pub(crate) fn executable_profile(&self, profile_id: &str) -> Option<AiProfileSnapshot> {
        if !self.is_control_connected() {
            return None;
        }
        self.profile_catalog
            .borrow()
            .as_ref()?
            .profiles
            .iter()
            .find(|projection| {
                projection.profile.profile_id.as_str() == profile_id
                    && projection.profile.pipeline_type == AiPipelineType::PostCallAnalysis
                    && projection.enabled
                    && projection.executable
            })
            .map(|projection| projection.profile.clone())
    }

    pub(crate) fn known_post_call_profile_enabled(&self, profile_id: &str) -> Option<bool> {
        let catalog = self.profile_catalog.borrow();
        let catalog = catalog.as_ref()?;
        Some(catalog.profiles.iter().any(|projection| {
            projection.profile.profile_id.as_str() == profile_id
                && projection.profile.pipeline_type == AiPipelineType::PostCallAnalysis
                && projection.enabled
        }))
    }

    pub(crate) fn profile_catalog(&self) -> Option<ProfileCatalogSnapshot> {
        self.profile_catalog.borrow().clone()
    }

    pub(crate) fn try_send(&self, message: ControlMessage) -> Result<()> {
        let envelope = self.envelope(message)?;
        self.control_tx.try_send(envelope).map_err(|error| {
            self.metrics
                .control_queue_rejected
                .fetch_add(1, Ordering::Relaxed);
            anyhow::anyhow!(match error {
                mpsc::error::TrySendError::Full(_) => "AI control queue full",
                mpsc::error::TrySendError::Closed(_) => "AI control connector stopped",
            })
        })
    }

    pub(crate) fn try_send_media(&self, frame: MediaFrame) -> bool {
        match self.media_tx.try_send(frame) {
            Ok(()) => true,
            Err(_) => {
                self.metrics
                    .media_queue_rejected
                    .fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    pub(crate) fn metrics(&self) -> ConnectorMetricsSnapshot {
        ConnectorMetricsSnapshot {
            control_connected: self.metrics.control_connected.load(Ordering::Relaxed),
            media_connected: self.metrics.media_connected.load(Ordering::Relaxed),
            control_queue_rejected: self.metrics.control_queue_rejected.load(Ordering::Relaxed),
            media_queue_rejected: self.metrics.media_queue_rejected.load(Ordering::Relaxed),
            media_write_failed: self.metrics.media_write_failed.load(Ordering::Relaxed),
        }
    }

    fn envelope(&self, message: ControlMessage) -> Result<ControlEnvelope> {
        Ok(ControlEnvelope {
            protocol_version: PROTOCOL_VERSION,
            message_id: MessageId::new(format!(
                "core-{}",
                self.sequence.fetch_add(1, Ordering::Relaxed)
            ))?,
            timestamp_ms: unix_timestamp_ms(),
            message,
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_control_connection(
    socket: PathBuf,
    connector_instance_id: ConnectorInstanceId,
    mut outbound: mpsc::Receiver<ControlEnvelope>,
    events: broadcast::Sender<ControlEnvelope>,
    connected_tx: watch::Sender<bool>,
    sequence: Arc<AtomicU64>,
    metrics: Arc<ConnectorMetrics>,
    profile_catalog_tx: watch::Sender<Option<ProfileCatalogSnapshot>>,
) {
    let mut delay = RECONNECT_MIN_DELAY;
    loop {
        match connect_control(&socket, &connector_instance_id, &sequence).await {
            Ok(stream) => {
                delay = RECONNECT_MIN_DELAY;
                connected_tx.send_replace(true);
                metrics.control_connected.store(true, Ordering::Release);
                info!(socket = %socket.display(), "AI control connector ready");
                let (mut reader, mut writer) = stream.into_split();
                loop {
                    tokio::select! {
                        outbound_message = outbound.recv() => {
                            let Some(outbound_message) = outbound_message else { return; };
                            if let Err(error) = write_json_frame(&mut writer, &outbound_message).await {
                                warn!(error = %error, "AI control write failed; durable outbox will retry");
                                break;
                            }
                        }
                        inbound = read_json_frame::<_, ControlEnvelope>(&mut reader) => {
                            match inbound {
                                Ok(envelope) => {
                                    if let Err(error) = envelope.validate() {
                                        warn!(error = %error, "invalid AI gateway control envelope");
                                        break;
                                    }
                                    if let ControlMessage::ProfileCatalogSnapshot(snapshot) = &envelope.message {
                                        let snapshot = snapshot.clone();
                                        profile_catalog_tx.send_if_modified(|current| {
                                            let replace = current.as_ref().is_none_or(|current| {
                                                snapshot.catalog_version >= current.catalog_version
                                            });
                                            if replace {
                                                *current = Some(snapshot.clone());
                                            }
                                            replace
                                        });
                                        info!(
                                            catalog_version = snapshot.catalog_version,
                                            profiles = snapshot.profiles.len(),
                                            "AI profile catalog updated"
                                        );
                                    }
                                    let _ = events.send(envelope);
                                }
                                Err(error) => {
                                    debug!(error = %error, "AI control connection closed");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Err(error) => {
                debug!(socket = %socket.display(), error = %error, "AI control gateway unavailable")
            }
        }
        connected_tx.send_replace(false);
        metrics.control_connected.store(false, Ordering::Release);
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(RECONNECT_MAX_DELAY);
    }
}

async fn connect_control(
    socket: &PathBuf,
    connector_instance_id: &ConnectorInstanceId,
    sequence: &AtomicU64,
) -> Result<UnixStream> {
    let mut stream = UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect {}", socket.display()))?;
    let hello = ControlEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_id: MessageId::new(format!(
            "core-hello-{}",
            sequence.fetch_add(1, Ordering::Relaxed)
        ))?,
        timestamp_ms: unix_timestamp_ms(),
        message: ControlMessage::ConnectorHello(ConnectorHello {
            connector_instance_id: connector_instance_id.clone(),
            connector_kind: "voipswitch".to_string(),
            supported_versions: vec![PROTOCOL_VERSION],
            capabilities: vec!["audio_input".to_string(), "post_call_job".to_string()],
        }),
    };
    tokio::time::timeout(HANDSHAKE_TIMEOUT, write_json_frame(&mut stream, &hello))
        .await
        .context("AI control hello write timeout")??;
    let response = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        read_json_frame::<_, ControlEnvelope>(&mut stream),
    )
    .await
    .context("AI control hello response timeout")??;
    response.validate()?;
    let ControlMessage::GatewayHello(GatewayHello {
        selected_version, ..
    }) = response.message
    else {
        bail!("gateway_hello expected during AI handshake");
    };
    if selected_version != PROTOCOL_VERSION {
        bail!("AI gateway selected unsupported protocol version {selected_version}");
    }
    let catalog_request = ControlEnvelope {
        protocol_version: PROTOCOL_VERSION,
        message_id: MessageId::new(format!(
            "core-profile-catalog-{}",
            sequence.fetch_add(1, Ordering::Relaxed)
        ))?,
        timestamp_ms: unix_timestamp_ms(),
        message: ControlMessage::ProfileCatalogRequest(ProfileCatalogRequest {
            known_catalog_version: None,
        }),
    };
    tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        write_json_frame(&mut stream, &catalog_request),
    )
    .await
    .context("AI profile catalog request write timeout")??;
    Ok(stream)
}

async fn run_media_connection(
    socket: PathBuf,
    mut outbound: mpsc::Receiver<MediaFrame>,
    metrics: Arc<ConnectorMetrics>,
) {
    let mut delay = RECONNECT_MIN_DELAY;
    loop {
        match UnixStream::connect(&socket).await {
            Ok(mut stream) => {
                delay = RECONNECT_MIN_DELAY;
                metrics.media_connected.store(true, Ordering::Release);
                info!(socket = %socket.display(), "AI media connector ready");
                while let Some(frame) = outbound.recv().await {
                    if let Err(error) = write_media_frame(&mut stream, &frame).await {
                        metrics.media_write_failed.fetch_add(1, Ordering::Relaxed);
                        debug!(error = %error, "AI media connection closed");
                        break;
                    }
                }
            }
            Err(error) => {
                debug!(socket = %socket.display(), error = %error, "AI media gateway unavailable")
            }
        }
        metrics.media_connected.store(false, Ordering::Release);
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(RECONNECT_MAX_DELAY);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ai_protocol::control::{
        AiProfileProjection, JobRef, JobStatusRequest, ProfileCatalogSnapshot,
    };
    use ai_protocol::frame::{read_json_frame, write_json_frame};
    use ai_protocol::id::{ConversationId, JobId, OperationId, ProfileId, TenantId};
    use ai_protocol::time::unix_timestamp_ms;
    use tempfile::tempdir;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn connects_handshakes_and_delivers_control_events() {
        let root = tempdir().unwrap();
        let control_socket = root.path().join("control.sock");
        let media_socket = root.path().join("media.sock");
        let listener = UnixListener::bind(&control_socket).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let hello: ControlEnvelope = read_json_frame(&mut stream).await.unwrap();
            assert!(matches!(hello.message, ControlMessage::ConnectorHello(_)));
            write_json_frame(
                &mut stream,
                &ControlEnvelope {
                    protocol_version: PROTOCOL_VERSION,
                    message_id: MessageId::new("gateway-1").unwrap(),
                    timestamp_ms: unix_timestamp_ms(),
                    message: ControlMessage::GatewayHello(GatewayHello {
                        selected_version: PROTOCOL_VERSION,
                        gateway_instance_id: "test".to_string(),
                        capabilities: vec!["post_call_job".to_string()],
                    }),
                },
            )
            .await
            .unwrap();
            let catalog_request: ControlEnvelope = read_json_frame(&mut stream).await.unwrap();
            assert!(matches!(
                catalog_request.message,
                ControlMessage::ProfileCatalogRequest(_)
            ));
            write_json_frame(
                &mut stream,
                &ControlEnvelope {
                    protocol_version: PROTOCOL_VERSION,
                    message_id: MessageId::new("gateway-2").unwrap(),
                    timestamp_ms: unix_timestamp_ms(),
                    message: ControlMessage::ProfileCatalogSnapshot(ProfileCatalogSnapshot {
                        catalog_version: 1,
                        profiles: vec![AiProfileProjection {
                            profile: AiProfileSnapshot {
                                profile_id: ProfileId::new("profile-1").unwrap(),
                                profile_version: 1,
                                pipeline_type: AiPipelineType::PostCallAnalysis,
                                asr_provider_id: Some("mock-asr".to_string()),
                                llm_provider_id: Some("mock-llm".to_string()),
                                tts_provider_id: None,
                                capture_complete_ratio: 0.995,
                                capture_process_min_ratio: 0.95,
                                capture_complete_max_gap_ms: 200,
                                capture_process_max_gap_ms: 5_000,
                            },
                            enabled: true,
                            executable: true,
                        }],
                    }),
                },
            )
            .await
            .unwrap();
            let request: ControlEnvelope = read_json_frame(&mut stream).await.unwrap();
            write_json_frame(&mut stream, &request).await.unwrap();
        });
        let connector = AiConnector::spawn(AiConnectorConfig {
            instance_id: "test-core".to_string(),
            control_socket,
            media_socket,
        })
        .unwrap();
        let mut events = connector.subscribe();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !connector.is_control_connected() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while connector.executable_profile("profile-1").is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        connector
            .try_send(ControlMessage::JobStatusRequest(JobStatusRequest {
                job: JobRef {
                    job_id: JobId::new("job-1").unwrap(),
                    tenant_id: TenantId::new("domain-1").unwrap(),
                    conversation_id: ConversationId::new("call-1").unwrap(),
                    operation_id: OperationId::new("operation-1").unwrap(),
                    generation: 1,
                },
            }))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let event = events.recv().await.unwrap();
                if matches!(event.message, ControlMessage::JobStatusRequest(_)) {
                    break;
                }
            }
        })
        .await
        .unwrap();
        server.await.unwrap();
    }

    #[test]
    fn control_queue_is_bounded() {
        let (tx, _rx) = mpsc::channel::<ControlEnvelope>(CONTROL_QUEUE_CAPACITY);
        assert_eq!(tx.max_capacity(), CONTROL_QUEUE_CAPACITY);
    }

    #[test]
    fn media_queue_is_bounded() {
        let (tx, _rx) = mpsc::channel::<MediaFrame>(MEDIA_QUEUE_CAPACITY);
        assert_eq!(tx.max_capacity(), MEDIA_QUEUE_CAPACITY);
    }
}
