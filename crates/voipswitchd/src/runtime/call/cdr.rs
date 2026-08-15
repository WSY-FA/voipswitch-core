use crate::app::AppState;
use crate::data_store::CdrWriteCommand;
use crate::data_store::{CdrRecord, LegCdrRecord, RecordingRecord};
use crate::runtime::call::actor::SessionActor;
use crate::runtime::call::model::CallRuntime;
use crate::runtime::call::session::ControlMessage;
use crate::runtime::media::{MediaBridge, MediaStatsSnapshot};
use tracing::{info, warn};
use voipswitch_core::types::time::unix_timestamp_ms;

struct FinalCallSnapshot {
    call_id: String,
    domain_id: String,
    caller_number: String,
    callee_number: String,
    callee_target: String,
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
    callee_endpoint_ref: Option<String>,
    callee_endpoint_number: Option<String>,
}

impl From<&CallRuntime> for FinalCallSnapshot {
    fn from(call: &CallRuntime) -> Self {
        Self {
            call_id: call.call_id().to_string(),
            domain_id: call.domain_id().to_string(),
            caller_number: call.caller_number.clone(),
            callee_number: call.callee_number.clone(),
            callee_target: call.callee_target.clone(),
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
            callee_endpoint_ref: if call.outbound_trunk_ref.is_some() {
                call.outbound_trunk_ref.clone()
            } else {
                call.callee_target
                    .strip_prefix("endpoint:")
                    .map(|s| s.to_string())
            },
            callee_endpoint_number: if call.outbound_trunk_ref.is_some() {
                None
            } else {
                Some(call.callee_number.clone())
            },
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
        tokio::spawn(async move {
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

    let caller_leg = LegCdrRecord {
        call_id: call.call_id.clone(),
        session_id: call.caller_session_id.clone(),
        domain_id: call.domain_id.clone(),
        leg_role: "caller".to_string(),
        direction: "inbound".to_string(),
        endpoint_ref: call.inbound_trunk_ref.clone(),
        endpoint_number: Some(call.caller_number.clone()),
        signaling_number: Some(call.caller_number.clone()),
        route_id: call.inbound_route_id.clone(),
        route_name: call.inbound_route_name.clone(),
        trunk_ref: call.inbound_trunk_ref.clone(),
        trunk_name: call.inbound_trunk_name.clone(),
        joined_at_ms: call.started_at_ms,
        answered_at_ms: call.answered_at_ms,
        left_at_ms: ended_at_ms,
        final_status: call.last_status,
        hangup_cause: Some(hangup.clone()),
        media_packets: total_packets,
        media_bytes: total_bytes,
        media_rtcp_packets: total_rtcp,
        bridge_ids: call.callee_session_id.clone(),
    };

    let callee_leg = LegCdrRecord {
        call_id: call.call_id.clone(),
        session_id: call.callee_session_id.clone(),
        domain_id: call.domain_id.clone(),
        leg_role: "callee".to_string(),
        direction: "outbound".to_string(),
        endpoint_ref: call.callee_endpoint_ref.clone(),
        endpoint_number: call.callee_endpoint_number.clone(),
        signaling_number: Some(call.callee_number.clone()),
        route_id: call.outbound_route_id.clone(),
        route_name: call.outbound_route_name.clone(),
        trunk_ref: call.outbound_trunk_ref.clone(),
        trunk_name: call.outbound_trunk_name.clone(),
        joined_at_ms: call.started_at_ms,
        answered_at_ms: call.answered_at_ms,
        left_at_ms: ended_at_ms,
        final_status: call.last_status,
        hangup_cause: Some(hangup),
        media_packets: total_packets,
        media_bytes: total_bytes,
        media_rtcp_packets: total_rtcp,
        bridge_ids: call.caller_session_id.clone(),
    };

    vec![caller_leg, callee_leg]
}

async fn finalize_call(state: AppState, call: FinalCallSnapshot, media: Option<MediaBridge>) {
    let call_id = call.call_id.clone();
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
