use crate::app::AppState;
use crate::data_store::CdrWriteCommand;
use crate::data_store::{CdrRecord, LegCdrRecord, RecordingRecord};
use crate::runtime::call::actor::SessionActor;
use crate::runtime::call::model::CallRuntime;
use crate::runtime::call::session::ControlMessage;
use crate::runtime::media::{MediaBridge, MediaStatsSnapshot};
use tracing::{info, warn};
use voipswitch_core::call::CallSnapshot;
use voipswitch_core::session::SessionEndpoint;
use voipswitch_core::types::time::unix_timestamp_ms;

struct FinalCallSnapshot {
    call_id: String,
    domain_id: String,
    caller_number: String,
    callee_number: String,
    topology: CallSnapshot,
    callee_target: String,
    ai_agent_call: bool,
    inbound_route_id: Option<String>,
    inbound_route_name: Option<String>,
    inbound_trunk_ref: Option<String>,
    inbound_trunk_name: Option<String>,
    outbound_route_id: Option<String>,
    outbound_route_name: Option<String>,
    outbound_trunk_ref: Option<String>,
    outbound_trunk_name: Option<String>,
    started_at_ms: u64,
    answered_at_ms: Option<u64>,
    last_status: Option<u16>,
    hangup_cause: Option<String>,
    answered: bool,
    recording_requested: bool,
    recording_start_error: Option<String>,
    caller_session_id: String,
    callee_session_id: String,
}

impl From<&CallRuntime> for FinalCallSnapshot {
    fn from(call: &CallRuntime) -> Self {
        Self {
            call_id: call.call_id().to_string(),
            domain_id: call.domain_id().to_string(),
            caller_number: call.caller_number.clone(),
            callee_number: call.callee_number.clone(),
            topology: call.aggregate.snapshot(),
            callee_target: call.callee_target.clone(),
            ai_agent_call: call.ai_agent_call,
            inbound_route_id: call.inbound_route_id.clone(),
            inbound_route_name: call.inbound_route_name.clone(),
            inbound_trunk_ref: call.inbound_trunk_ref.clone(),
            inbound_trunk_name: call.inbound_trunk_name.clone(),
            outbound_route_id: call.outbound_route_id.clone(),
            outbound_route_name: call.outbound_route_name.clone(),
            outbound_trunk_ref: call.outbound_trunk_ref.clone(),
            outbound_trunk_name: call.outbound_trunk_name.clone(),
            started_at_ms: call.started_at_ms(),
            answered_at_ms: call.answered_at_ms(),
            last_status: call.last_status,
            hangup_cause: call.hangup_cause_str().map(|s| s.to_string()),
            answered: call.is_answered(),
            recording_requested: call.recording_requested,
            recording_start_error: call.recording_start_error.clone(),
            caller_session_id: call.caller_session_id().to_string(),
            callee_session_id: call.callee_session_id().to_string(),
        }
    }
}

fn failed_recording_for_call(
    call: &FinalCallSnapshot,
    ended_at_ms: u64,
    message: String,
) -> RecordingRecord {
    RecordingRecord {
        recording_id: format!("recording-{}", call.call_id),
        call_id: call.call_id.clone(),
        domain_id: call.domain_id.clone(),
        status: "failed".to_string(),
        caller_number: call.caller_number.clone(),
        callee_number: call.callee_number.clone(),
        started_at_ms: call.answered_at_ms.unwrap_or(call.started_at_ms),
        ended_at_ms: Some(ended_at_ms),
        duration_ms: 0,
        format: "wav".to_string(),
        sample_rate: 8_000,
        channel_count: 2,
        file_name: String::new(),
        storage_root: String::new(),
        storage_path: String::new(),
        file_size_bytes: 0,
        packets_tapped: 0,
        packets_dropped: 0,
        error_code: Some("recording_start_failed".to_string()),
        error_message: Some(message),
    }
}

impl SessionActor {
    pub(super) fn begin_finish_call(&mut self) {
        if self.finalization_started {
            return;
        }
        self.finalization_started = true;
        self.abort_digit_collection("call_terminated");
        self.cancel_all_timers();
        self.media_generation = self.media_generation.saturating_add(1);
        let call_id = self.call.call_id().to_string();
        self.call.release_topology(unix_timestamp_ms());
        self.state.remove_active_call(&call_id);
        let state = self.state.clone();
        let snapshot = FinalCallSnapshot::from(&self.call);
        let media = self.call.media.take();
        let control_tx = self.coordinator_handle.weak_control_sender();
        let dispatcher = self.control_dispatcher.clone();
        let session_id = self.call.caller_session_id().to_string();
        let guard = state.track_finalizer();
        tokio::spawn(async move {
            let _guard = guard;
            finalize_call(state, snapshot, media).await;
            if let Some(control_tx) = control_tx.upgrade() {
                let _ = dispatcher
                    .dispatch_to(&session_id, control_tx, ControlMessage::CallFinalized)
                    .await;
            }
        });
    }
}

fn build_leg_cdrs(
    call: &FinalCallSnapshot,
    media_stats: &MediaStatsSnapshot,
    ended_at_ms: u64,
) -> Vec<LegCdrRecord> {
    let total_packets = media_stats.caller_to_callee_packets + media_stats.callee_to_caller_packets;
    let total_bytes = media_stats.caller_to_callee_bytes + media_stats.callee_to_caller_bytes;
    let total_rtcp =
        media_stats.caller_to_callee_rtcp_packets + media_stats.callee_to_caller_rtcp_packets;
    let hangup = call.hangup_cause.clone().unwrap_or_else(|| {
        if call.answered {
            "normal_clearing".to_string()
        } else {
            "not_answered".to_string()
        }
    });

    call.topology
        .legs
        .iter()
        .filter_map(|leg| {
            let caller = leg.session_id.as_str() == call.caller_session_id;
            let bridges: Vec<_> = call
                .topology
                .bridges
                .iter()
                .filter(|bridge| {
                    bridge.active_at_ms.is_some()
                        && (bridge.leg_a == leg.session_id || bridge.leg_b == leg.session_id)
                })
                .collect();
            if !caller && bridges.is_empty() {
                return None;
            }
            let current = leg.session_id.as_str() == call.callee_session_id;
            let (endpoint_ref, endpoint_number, signaling_number, trunk_ref) = match &leg.endpoint {
                SessionEndpoint::Endpoint(endpoint) => (
                    Some(endpoint.endpoint_id.to_string()),
                    Some(endpoint.number.to_string()),
                    Some(endpoint.number.to_string()),
                    None,
                ),
                SessionEndpoint::Trunk { trunk_id } => (
                    Some(trunk_id.to_string()),
                    None,
                    Some(call.callee_number.clone()),
                    Some(trunk_id.to_string()),
                ),
                SessionEndpoint::AiAgent { agent_id } => (
                    Some(format!("ai_agent:{agent_id}")),
                    None,
                    Some(call.callee_number.clone()),
                    None,
                ),
                SessionEndpoint::External { number } => (
                    call.inbound_trunk_ref.clone(),
                    Some(number.clone()),
                    Some(number.clone()),
                    call.inbound_trunk_ref.clone(),
                ),
            };
            Some(LegCdrRecord {
                call_id: call.call_id.clone(),
                session_id: leg.session_id.to_string(),
                domain_id: leg.domain_id.to_string(),
                leg_role: if caller { "caller" } else { "callee" }.to_owned(),
                direction: if caller { "inbound" } else { "outbound" }.to_owned(),
                endpoint_ref,
                endpoint_number,
                signaling_number,
                route_id: if caller {
                    call.inbound_route_id.clone()
                } else {
                    call.outbound_route_id.clone()
                },
                route_name: if caller {
                    call.inbound_route_name.clone()
                } else {
                    call.outbound_route_name.clone()
                },
                trunk_name: if caller {
                    call.inbound_trunk_name.clone()
                } else if trunk_ref == call.outbound_trunk_ref {
                    call.outbound_trunk_name.clone()
                } else {
                    None
                },
                trunk_ref,
                joined_at_ms: leg.joined_at_ms,
                answered_at_ms: bridges
                    .iter()
                    .map(|bridge| bridge.created_at_ms)
                    .min()
                    .or(if caller { call.answered_at_ms } else { None }),
                left_at_ms: leg.left_at_ms.unwrap_or(ended_at_ms),
                final_status: if caller || current {
                    call.last_status
                } else {
                    Some(200)
                },
                hangup_cause: Some(if caller || current {
                    hangup.clone()
                } else {
                    "normal_clearing".to_owned()
                }),
                media_packets: if caller || current { total_packets } else { 0 },
                media_bytes: if caller || current { total_bytes } else { 0 },
                media_rtcp_packets: if caller || current { total_rtcp } else { 0 },
                bridge_ids: bridges
                    .iter()
                    .map(|bridge| bridge.bridge_id.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            })
        })
        .collect()
}

async fn finalize_call(state: AppState, call: FinalCallSnapshot, media: Option<MediaBridge>) {
    let call_id = call.call_id.clone();
    if let Some(ai_jobs) = state.ai_jobs()
        && call.ai_agent_call
    {
        ai_jobs.unregister_voice_coordinator(&call_id);
    }
    let (media_stats, mut recording, media_forwarding_mode) = match media {
        Some(media) => {
            let finalized = media.stop(&call_id).await;
            let ai_capture = finalized.ai_capture;
            if let Some(capture) = ai_capture.as_ref() {
                info!(
                    call_id,
                    job_id = %capture.job.job_id,
                    packets_tapped = capture.stats.packets_tapped,
                    packets_dropped = capture.stats.packets_dropped,
                    packets_ignored = capture.stats.packets_ignored,
                    "AI media capture ended"
                );
            }
            if let Some(ai_jobs) = state.ai_jobs()
                && let Err(error) = ai_jobs.try_end_call(call_id.clone(), ai_capture)
            {
                warn!(call_id, error = %error, "AI input end submission dropped");
            }
            (
                finalized.stats,
                finalized.recording,
                Some(finalized.forwarding_mode),
            )
        }
        None => (MediaStatsSnapshot::default(), None, None),
    };

    let ended_at_ms = unix_timestamp_ms();
    if recording.is_none() && call.answered && call.recording_requested {
        recording = Some(failed_recording_for_call(
            &call,
            ended_at_ms,
            call.recording_start_error
                .clone()
                .unwrap_or_else(|| "recording did not start".to_string()),
        ));
    }

    let record = CdrRecord {
        call_id: call.call_id.clone(),
        domain_id: call.domain_id.clone(),
        caller_number: call.caller_number.clone(),
        callee_number: call.callee_number.clone(),
        inbound_route_id: call.inbound_route_id.clone(),
        inbound_route_name: call.inbound_route_name.clone(),
        inbound_trunk_ref: call.inbound_trunk_ref.clone(),
        inbound_trunk_name: call.inbound_trunk_name.clone(),
        outbound_route_id: call.outbound_route_id.clone(),
        outbound_route_name: call.outbound_route_name.clone(),
        outbound_trunk_ref: call.outbound_trunk_ref.clone(),
        outbound_trunk_name: call.outbound_trunk_name.clone(),
        started_at_ms: call.started_at_ms,
        answered_at_ms: call.answered_at_ms,
        ended_at_ms,
        duration_ms: ended_at_ms.saturating_sub(call.started_at_ms),
        billable_ms: call
            .answered_at_ms
            .map(|answered_at| ended_at_ms.saturating_sub(answered_at))
            .unwrap_or(0),
        answered: call.answered,
        final_status: call.last_status,
        hangup_cause: call.hangup_cause.clone().unwrap_or_else(|| {
            if call.answered {
                "normal_clearing".to_string()
            } else {
                "not_answered".to_string()
            }
        }),
        media_forwarding_mode,
        caller_to_callee_packets: media_stats.caller_to_callee_packets,
        caller_to_callee_bytes: media_stats.caller_to_callee_bytes,
        callee_to_caller_packets: media_stats.callee_to_caller_packets,
        callee_to_caller_bytes: media_stats.callee_to_caller_bytes,
        caller_to_callee_rtcp_packets: media_stats.caller_to_callee_rtcp_packets,
        callee_to_caller_rtcp_packets: media_stats.callee_to_caller_rtcp_packets,
        trace_available: state.call_trace_available(&call_id),
        trace_incomplete: false,
        incomplete: false,
        incomplete_reason: None,
        recording_status: recording.as_ref().map(|record| record.status.clone()),
        recording_available: recording.as_ref().is_some_and(|record| {
            matches!(record.status.as_str(), "complete" | "incomplete")
                && !record.storage_path.is_empty()
        }),
    };

    let leg_cdrs = build_leg_cdrs(&call, &media_stats, ended_at_ms);

    let write_cmd = CdrWriteCommand {
        call_cdr: record,
        leg_cdrs,
        recording: recording.clone(),
        trace_call_id: call_id.clone(),
        trace_domain_id: call.domain_id.clone(),
        trace_ended_at_ms: ended_at_ms,
    };

    if let Some(cdr_writer) = state.cdr_writer() {
        if let Err(err) = cdr_writer.enqueue_durable(write_cmd).await {
            warn!(call_id, error = %err, "durably spool CDR batch failed");
        }
    } else {
        let backend = state.backend();
        let persisted =
            tokio::task::spawn_blocking(move || backend.insert_cdr(&write_cmd.call_cdr)).await;
        match persisted {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!(call_id, error = %err, "persist CDR failed (no writer)"),
            Err(err) => warn!(call_id, error = %err, "CDR writer task failed"),
        }
    }
    info!(
        call_id,
        caller = call.caller_number,
        callee = call.callee_number,
        callee_target = call.callee_target,
        answered = call.answered,
        "basic call completed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::call::model::OutboundTarget;
    use crate::runtime::call::regression::test_call;

    fn endpoint(number: &str) -> OutboundTarget {
        OutboundTarget::Endpoint {
            endpoint_id: number.into(),
            endpoint_number: number.into(),
        }
    }

    #[test]
    fn unconnected_attempts_do_not_fabricate_callee_cdrs() {
        let call = test_call(endpoint("1002"));
        let legs = build_leg_cdrs(
            &FinalCallSnapshot::from(&call),
            &MediaStatsSnapshot::default(),
            300,
        );
        assert_eq!(legs.len(), 1);
        assert_eq!(legs[0].session_id, "caller-a");
        assert!(legs[0].bridge_ids.is_empty());
        assert_eq!(legs[0].answered_at_ms, None);
    }

    #[test]
    fn sequential_hunt_uses_winner_join_time_and_real_bridge_id() {
        let mut call = test_call(endpoint("1002"));
        call.register_attempt(2, "callee-b".into(), &endpoint("1003"), 150)
            .unwrap();
        call.mark_answered(200, 1).unwrap();
        call.activate_current_bridge(1, 210).unwrap();
        call.release_topology(300);
        let legs = build_leg_cdrs(
            &FinalCallSnapshot::from(&call),
            &MediaStatsSnapshot::default(),
            310,
        );
        assert_eq!(legs.len(), 2);
        let winner = legs
            .iter()
            .find(|leg| leg.session_id == "callee-b")
            .unwrap();
        assert_eq!(winner.joined_at_ms, 150);
        assert_eq!(winner.answered_at_ms, Some(200));
        assert_eq!(winner.left_at_ms, 300);
        assert_eq!(winner.bridge_ids, "bridge-attempt-2");
        assert_eq!(winner.endpoint_number.as_deref(), Some("1003"));
        assert_eq!(winner.signaling_number.as_deref(), Some("1003"));
    }

    #[test]
    fn ai_transfer_keeps_both_bridged_legs_and_ends_original_ai_leg() {
        let mut call = test_call(OutboundTarget::AiAgent {
            agent_id: "frontdesk".into(),
        });
        call.mark_answered(120, 1).unwrap();
        call.activate_current_bridge(1, 125).unwrap();
        call.register_attempt(2, "callee-human".into(), &endpoint("1003"), 200)
            .unwrap();
        call.mark_transfer_answered(250, 2).unwrap();
        call.activate_current_bridge(2, 255).unwrap();
        call.release_topology(400);
        let stats = MediaStatsSnapshot {
            caller_to_callee_packets: 10,
            ..Default::default()
        };
        let legs = build_leg_cdrs(&FinalCallSnapshot::from(&call), &stats, 420);
        assert_eq!(legs.len(), 3);
        let ai = legs
            .iter()
            .find(|leg| leg.session_id == "callee-a")
            .unwrap();
        assert_eq!(ai.answered_at_ms, Some(120));
        assert_eq!(ai.left_at_ms, 250);
        assert_eq!(ai.bridge_ids, "bridge-attempt-1");
        assert_eq!(ai.media_packets, 0);
        let human = legs
            .iter()
            .find(|leg| leg.session_id == "callee-human")
            .unwrap();
        assert_eq!(human.joined_at_ms, 200);
        assert_eq!(human.answered_at_ms, Some(250));
        assert_eq!(human.left_at_ms, 400);
        assert_eq!(human.bridge_ids, "bridge-attempt-2");
        assert_eq!(human.media_packets, 10);
        let caller = legs
            .iter()
            .find(|leg| leg.session_id == "caller-a")
            .unwrap();
        assert_eq!(caller.bridge_ids, "bridge-attempt-1,bridge-attempt-2");
    }
}
