use crate::app::ActiveCallView;
use crate::runtime::call::actor::SessionActor;
use std::collections::{HashMap, VecDeque};

pub(crate) struct LegEventDeduper {
    highest_sequences: HashMap<String, u64>,
    order: VecDeque<String>,
    capacity: usize,
}

impl Default for LegEventDeduper {
    fn default() -> Self {
        Self {
            highest_sequences: HashMap::new(),
            order: VecDeque::new(),
            capacity: 16_384,
        }
    }
}

impl LegEventDeduper {
    pub(crate) fn accept(&mut self, adapter_call_leg_id: &str, sequence: u64) -> bool {
        if self
            .highest_sequences
            .get(adapter_call_leg_id)
            .is_some_and(|highest| sequence <= *highest)
        {
            return false;
        }
        if !self.highest_sequences.contains_key(adapter_call_leg_id) {
            self.order.push_back(adapter_call_leg_id.to_string());
        }
        self.highest_sequences
            .insert(adapter_call_leg_id.to_string(), sequence);
        while self.highest_sequences.len() > self.capacity {
            if let Some(expired) = self.order.pop_front() {
                self.highest_sequences.remove(&expired);
            }
        }
        true
    }
}

impl SessionActor {
    pub(crate) fn publish_call_view(&self) {
        let call = &self.call;
        self.state.upsert_active_call(ActiveCallView {
            call_id: call.call_id().to_string(),
            domain_id: call.domain_id().to_string(),
            caller_session_id: call.caller_session_id().to_string(),
            callee_session_id: call.callee_session_id().to_string(),
            caller_number: call.caller_number.clone(),
            callee_number: call.callee_number.clone(),
            state: call.state_str().to_string(),
            started_at_ms: call.started_at_ms(),
            answered_at_ms: call.answered_at_ms(),
            last_status: call.last_status,
            caller_terminated: call.caller_terminated,
            callee_terminated: call.callee_terminated,
            runtime_config_version: call.config_snapshot.runtime_config_version,
            domain_config_version: call.config_snapshot.domain_config_version,
            topology: call.aggregate.snapshot(),
        });
    }
}
