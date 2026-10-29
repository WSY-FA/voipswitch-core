//! Loopback scenario: registers the endpoints from the runtime view, starts one
//! inbound call on request and answers it deterministically.

use crate::protocol::{self, ActionRequest, EndpointView, RuntimeEnvelope, RuntimeView};
use crate::sdp;
use anyhow::{Context, Result, bail};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{info, warn};
use voipswitch_core::types::time::unix_timestamp_ms;

pub struct Config {
    pub auto_call: Option<(String, String)>,
    pub provisional_delay: Duration,
    pub answer_delay: Duration,
    pub call_hold: Duration,
    pub exit_after_call: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Caller,
    Callee,
}

struct Leg {
    call_id: String,
    session_id: String,
    role: Role,
    seq: u64,
}

struct Scheduled {
    at: Instant,
    frame: RuntimeEnvelope,
}

pub struct Scenario {
    cfg: Config,
    endpoints: BTreeMap<String, EndpointView>,
    domains: BTreeSet<String>,
    ready_sent: bool,
    call_triggered: bool,
    next_leg: u64,
    legs: BTreeMap<String, Leg>,
    caller_leg: Option<String>,
    callee_leg: Option<String>,
    disconnected: BTreeSet<String>,
    finished: bool,
    queue: Vec<Scheduled>,
    registration_version: u64,
}

impl Scenario {
    pub fn new(cfg: Config) -> Self {
        Self {
            cfg,
            endpoints: BTreeMap::new(),
            domains: BTreeSet::new(),
            ready_sent: false,
            call_triggered: false,
            next_leg: 1,
            legs: BTreeMap::new(),
            caller_leg: None,
            callee_leg: None,
            disconnected: BTreeSet::new(),
            finished: false,
            queue: Vec::new(),
            registration_version: 0,
        }
    }

    fn new_leg_id(&mut self) -> String {
        let id = format!("mock-leg-{}", self.next_leg);
        self.next_leg += 1;
        id
    }

    fn schedule(&mut self, after: Duration, frame: RuntimeEnvelope) {
        self.queue.push(Scheduled {
            at: Instant::now() + after,
            frame,
        });
    }

    async fn flush_due(&mut self, tx: &mpsc::Sender<RuntimeEnvelope>) -> Result<()> {
        let now = Instant::now();
        let mut due: Vec<Scheduled> = Vec::new();
        let mut pending = Vec::new();
        for entry in self.queue.drain(..) {
            if entry.at <= now {
                due.push(entry);
            } else {
                pending.push(entry);
            }
        }
        self.queue = pending;
        due.sort_by_key(|entry| entry.at);
        for entry in due {
            tracing::debug!(frame_type = %entry.frame.r#type, "sending runtime frame");
            tx.send(entry.frame)
                .await
                .context("scenario writer stopped")?;
        }
        Ok(())
    }

    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<RuntimeEnvelope>,
        tx: mpsc::Sender<RuntimeEnvelope>,
    ) -> Result<()> {
        loop {
            let next_at = self.queue.iter().map(|entry| entry.at).min();
            let timer = async {
                match next_at {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                frame = rx.recv() => {
                    let Some(frame) = frame else {
                        bail!("core closed the runtime socket");
                    };
                    self.handle(frame, &tx).await?;
                }
                _ = timer, if next_at.is_some() => {
                    self.flush_due(&tx).await?;
                }
            }
            if self.finished {
                return Ok(());
            }
        }
    }

    async fn handle(
        &mut self,
        frame: RuntimeEnvelope,
        tx: &mpsc::Sender<RuntimeEnvelope>,
    ) -> Result<()> {
        match frame.r#type.as_str() {
            "ApplyRuntimeView" => self.apply_view(frame, tx).await,
            "CoreHelloAck" => Ok(()),
            _ if frame.kind == "command" => self.handle_command(frame, tx).await,
            other => {
                info!(frame_type = other, "ignoring runtime frame");
                Ok(())
            }
        }
    }

    async fn apply_view(
        &mut self,
        frame: RuntimeEnvelope,
        tx: &mpsc::Sender<RuntimeEnvelope>,
    ) -> Result<()> {
        let view: RuntimeView =
            serde_json::from_value(frame.body.clone()).context("decode ApplyRuntimeView")?;
        self.endpoints = view
            .endpoints
            .into_iter()
            .map(|endpoint| (endpoint.number.clone(), endpoint))
            .collect();
        self.domains = view
            .domains
            .into_iter()
            .map(|domain| domain.domain_id)
            .collect();
        info!(
            view_revision = view.view_revision,
            endpoints = self.endpoints.len(),
            domains = self.domains.len(),
            "runtime view applied"
        );

        let ack = protocol::event(
            "ApplyRuntimeViewAck",
            json!({
                "view_revision": view.view_revision,
                "status": "accepted",
                "applied_at_ms": unix_timestamp_ms(),
                "errors": [],
            }),
        );
        tx.send(ack).await.context("send runtime view ack")?;

        if !self.ready_sent {
            self.ready_sent = true;
            self.register_endpoints().await?;
            self.schedule_auto_call();
        }
        Ok(())
    }

    async fn register_endpoints(&mut self) -> Result<()> {
        self.registration_version += 1;
        let version = self.registration_version;
        let expires_at_ms = unix_timestamp_ms().saturating_add(3_600_000);
        let mut frames = vec![
            // AdapterReady resets core's registration mirror, so it must come
            // before the snapshot that marks the mirror ready.
            protocol::event(
                "AdapterReady",
                json!({ "sip_port": 15060, "bind_source": "mock" }),
            ),
            protocol::event("RegistrationSnapshotBegin", json!({})),
        ];
        for endpoint in self.endpoints.values() {
            frames.push(protocol::event(
                "RegistrationChanged",
                json!({
                    "domain_id": endpoint.domain_id,
                    "endpoint_id": endpoint.endpoint_id,
                    "state": "registered",
                    "contact": format!("sip:{}@mock.invalid", endpoint.number),
                    "route_target": null,
                    "expires_at_ms": expires_at_ms,
                    "user_agent": "vs_mock_adapterd",
                    "version": version,
                }),
            ));
        }
        frames.push(protocol::event("RegistrationSnapshotEnd", json!({})));
        for frame in frames {
            self.schedule(Duration::from_millis(0), frame);
        }
        Ok(())
    }

    fn schedule_auto_call(&mut self) {
        let Some((caller, callee)) = self.cfg.auto_call.clone() else {
            return;
        };
        if self.call_triggered {
            return;
        }
        self.call_triggered = true;
        let Some(caller_endpoint) = self.endpoints.get(&caller).cloned() else {
            warn!(caller, "auto-call caller is not a configured endpoint");
            self.finished = true;
            return;
        };
        if !self.endpoints.contains_key(&callee) {
            warn!(callee, "auto-call callee is not a configured endpoint");
            self.finished = true;
            return;
        }
        let leg_id = self.new_leg_id();
        let frame = protocol::event(
            "InboundInviteOffered",
            json!({
                "domain_id": caller_endpoint.domain_id,
                "adapter_call_leg_id": leg_id,
                "caller_number": caller,
                "callee_number": callee,
                "origin": {
                    "type": "endpoint",
                    "endpoint_id": caller_endpoint.endpoint_id,
                },
                "route_target": null,
                "sdp_offer": sdp::body(sdp::OFFER),
            }),
        );
        self.schedule(Duration::from_millis(200), frame);
        info!("scheduled loopback inbound call");
    }

    async fn handle_command(
        &mut self,
        frame: RuntimeEnvelope,
        tx: &mpsc::Sender<RuntimeEnvelope>,
    ) -> Result<()> {
        let action = frame.r#type.clone();
        let request: ActionRequest =
            serde_json::from_value(frame.body).context("decode call action")?;
        let call_id = request.call_id.clone().unwrap_or_default();
        let session_id = request.session_id.clone().unwrap_or_default();

        match action.as_str() {
            "AdoptInboundInvite" => {
                let leg_id = request
                    .adapter_call_leg_id
                    .clone()
                    .unwrap_or_else(|| self.new_leg_id());
                self.legs.insert(
                    leg_id.clone(),
                    Leg {
                        call_id,
                        session_id,
                        role: Role::Caller,
                        seq: 1,
                    },
                );
                self.caller_leg = Some(leg_id);
                self.ack(&action, &request, tx, json!({})).await?;
            }
            "OriginateEndpoint" | "OriginateTrunk" => {
                let leg_id = self.new_leg_id();
                self.legs.insert(
                    leg_id.clone(),
                    Leg {
                        call_id: call_id.clone(),
                        session_id: session_id.clone(),
                        role: Role::Callee,
                        seq: 1,
                    },
                );
                self.callee_leg = Some(leg_id.clone());
                self.ack(
                    &action,
                    &request,
                    tx,
                    json!({ "data": { "adapter_call_leg_id": leg_id } }),
                )
                .await?;
                self.schedule_callee_progress(leg_id);
            }
            "AnswerInboundInvite" => {
                self.ack(&action, &request, tx, json!({})).await?;
                self.schedule_hold();
            }
            "HangupDialog" => {
                self.ack(&action, &request, tx, json!({})).await?;
                let leg_id = self
                    .legs
                    .iter()
                    .find(|(_, leg)| {
                        leg.call_id == call_id
                            && leg.session_id == session_id
                            && leg.role == Role::Callee
                    })
                    .map(|(id, _)| id.clone())
                    .or_else(|| self.callee_leg.clone());
                if let Some(leg_id) = leg_id {
                    self.disconnect_leg(&leg_id, tx).await?;
                    if self.cfg.exit_after_call {
                        self.finished = true;
                    }
                }
            }
            "RejectInboundInvite" | "CancelOutbound" => {
                self.ack(&action, &request, tx, json!({})).await?;
                if action == "CancelOutbound"
                    && let Some(leg_id) = self.callee_leg.clone()
                {
                    self.send_failed(&leg_id, 487, tx).await?;
                }
            }
            _ => {
                info!(action, "acknowledging unsupported action");
                self.ack(&action, &request, tx, json!({})).await?;
            }
        }
        Ok(())
    }

    async fn ack(
        &self,
        action: &str,
        request: &ActionRequest,
        tx: &mpsc::Sender<RuntimeEnvelope>,
        result: serde_json::Value,
    ) -> Result<()> {
        tx.send(protocol::action_ack(action, request, "accepted", result))
            .await
            .context("send call action ack")
    }

    fn schedule_callee_progress(&mut self, leg_id: String) {
        let Some(leg) = self.legs.get(&leg_id) else {
            return;
        };
        let (call_id, session_id, seq) = (leg.call_id.clone(), leg.session_id.clone(), leg.seq);
        let provisional = protocol::LegEvent {
            frame_type: "OutboundProvisional",
            call_id: &call_id,
            session_id: &session_id,
            adapter_call_leg_id: &leg_id,
            seq,
            status_code: Some(180),
            sdp: None,
            reason: None,
        }
        .into_envelope();
        self.schedule(self.cfg.provisional_delay, provisional);

        let answered = protocol::LegEvent {
            frame_type: "OutboundAnswered",
            call_id: &call_id,
            session_id: &session_id,
            adapter_call_leg_id: &leg_id,
            seq: seq + 1,
            status_code: Some(200),
            sdp: Some(sdp::body(sdp::ANSWER)),
            reason: None,
        }
        .into_envelope();
        self.schedule(self.cfg.answer_delay, answered);
        if let Some(leg) = self.legs.get_mut(&leg_id) {
            leg.seq += 1;
        }
    }

    fn schedule_hold(&mut self) {
        if self.cfg.call_hold.is_zero() {
            return;
        }
        let Some(caller_leg) = self.caller_leg.clone() else {
            return;
        };
        let Some(leg) = self.legs.get(&caller_leg) else {
            return;
        };
        let frame = protocol::LegEvent {
            frame_type: "DialogDisconnected",
            call_id: &leg.call_id,
            session_id: &leg.session_id,
            adapter_call_leg_id: &caller_leg,
            seq: leg.seq,
            status_code: None,
            sdp: None,
            reason: Some(16),
        }
        .into_envelope();
        self.schedule(self.cfg.call_hold, frame);
    }

    async fn send_failed(
        &mut self,
        leg_id: &str,
        status_code: u16,
        tx: &mpsc::Sender<RuntimeEnvelope>,
    ) -> Result<()> {
        let Some(leg) = self.legs.get(leg_id) else {
            return Ok(());
        };
        tx.send(
            protocol::LegEvent {
                frame_type: "OutboundFailed",
                call_id: &leg.call_id,
                session_id: &leg.session_id,
                adapter_call_leg_id: leg_id,
                seq: leg.seq,
                status_code: Some(status_code),
                sdp: None,
                reason: Some(16),
            }
            .into_envelope(),
        )
        .await
        .context("send outbound failure")
    }

    async fn disconnect_leg(
        &mut self,
        leg_id: &str,
        tx: &mpsc::Sender<RuntimeEnvelope>,
    ) -> Result<()> {
        if !self.disconnected.insert(leg_id.to_string()) {
            return Ok(());
        }
        let Some(leg) = self.legs.get(leg_id) else {
            return Ok(());
        };
        tx.send(
            protocol::LegEvent {
                frame_type: "DialogDisconnected",
                call_id: &leg.call_id,
                session_id: &leg.session_id,
                adapter_call_leg_id: leg_id,
                seq: leg.seq,
                status_code: None,
                sdp: None,
                reason: Some(16),
            }
            .into_envelope(),
        )
        .await
        .context("send dialog disconnect")
    }
}
