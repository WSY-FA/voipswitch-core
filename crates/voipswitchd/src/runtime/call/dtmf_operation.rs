use crate::runtime::call::dtmf::{
    DigitCollectionMode, DigitCollectionOutcome, DigitCollectionResultCode,
};
use serde::Serialize;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use voipswitch_core::dtmf::DtmfDigit;
use voipswitch_core::types::time::unix_timestamp_ms;

const DTMF_OPERATION_QUEUE_CAPACITY: usize = 32;
const DTMF_OPERATION_HISTORY_CAPACITY: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DtmfOperationSource {
    Caller,
    Callee,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DtmfOperationMode {
    Observe,
    Collect,
}

impl From<DtmfOperationMode> for DigitCollectionMode {
    fn from(value: DtmfOperationMode) -> Self {
        match value {
            DtmfOperationMode::Observe => Self::Observe,
            DtmfOperationMode::Collect => Self::Collect,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DtmfOperationSpec {
    pub(crate) call_id: String,
    pub(crate) source: DtmfOperationSource,
    pub(crate) mode: DtmfOperationMode,
    pub(crate) allowed: HashSet<DtmfDigit>,
    pub(crate) min_digits: usize,
    pub(crate) max_digits: usize,
    pub(crate) terminators: HashSet<DtmfDigit>,
    pub(crate) first_digit_timeout: Duration,
    pub(crate) inter_digit_timeout: Duration,
    pub(crate) overall_timeout: Duration,
}

#[derive(Debug, Clone)]
pub(crate) enum DtmfRuntimeOperation {
    Start {
        operation_id: String,
        spec: DtmfOperationSpec,
    },
    Cancel {
        operation_id: String,
        call_id: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DtmfOperationStatus {
    Pending,
    Acquiring,
    Ready,
    Completed,
    Cancelling,
    Cancelled,
    Failed,
}

impl DtmfOperationStatus {
    fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct DtmfOperationView {
    pub(crate) operation_id: String,
    pub(crate) call_id: String,
    pub(crate) source: DtmfOperationSource,
    pub(crate) mode: DtmfOperationMode,
    pub(crate) status: DtmfOperationStatus,
    pub(crate) media_generation: Option<u64>,
    pub(crate) result_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) digits: Option<String>,
    pub(crate) digit_count: usize,
    pub(crate) reason: Option<String>,
    pub(crate) created_at_ms: u64,
    pub(crate) updated_at_ms: u64,
}

struct RuntimeBinding {
    generation: u64,
    tx: mpsc::Sender<DtmfRuntimeOperation>,
}

#[derive(Default)]
struct DtmfOperationState {
    runtime_generation: u64,
    runtime: Option<RuntimeBinding>,
    operation_sequence: u64,
    operations: BTreeMap<String, DtmfOperationView>,
    order: VecDeque<String>,
}

#[derive(Clone, Default)]
pub(crate) struct DtmfOperationService {
    inner: Arc<Mutex<DtmfOperationState>>,
}

impl DtmfOperationService {
    pub(crate) fn attach_runtime(&self) -> (u64, mpsc::Receiver<DtmfRuntimeOperation>) {
        let (tx, rx) = mpsc::channel(DTMF_OPERATION_QUEUE_CAPACITY);
        let mut state = self.inner.lock().expect("DTMF operation lock poisoned");
        state.runtime_generation = state.runtime_generation.saturating_add(1);
        let generation = state.runtime_generation;
        state.runtime = Some(RuntimeBinding { generation, tx });
        (generation, rx)
    }

    pub(crate) fn detach_runtime(&self, generation: u64) {
        let mut state = self.inner.lock().expect("DTMF operation lock poisoned");
        if state.runtime.as_ref().map(|runtime| runtime.generation) == Some(generation) {
            state.runtime = None;
        }
        let now = unix_timestamp_ms();
        for operation in state.operations.values_mut() {
            if !operation.status.is_terminal() {
                operation.status = DtmfOperationStatus::Failed;
                operation.reason = Some("adapter_runtime_disconnected".to_string());
                operation.updated_at_ms = now;
            }
        }
    }

    pub(crate) fn start(&self, spec: DtmfOperationSpec) -> Result<DtmfOperationView, &'static str> {
        let mut state = self.inner.lock().expect("DTMF operation lock poisoned");
        let Some(runtime_tx) = state.runtime.as_ref().map(|runtime| runtime.tx.clone()) else {
            return Err("ADAPTER_UNAVAILABLE: adapter runtime is not connected");
        };
        state.operation_sequence = state.operation_sequence.saturating_add(1);
        let operation_id = format!(
            "dtmf-op-{}-{}",
            unix_timestamp_ms(),
            state.operation_sequence
        );
        let now = unix_timestamp_ms();
        let view = DtmfOperationView {
            operation_id: operation_id.clone(),
            call_id: spec.call_id.clone(),
            source: spec.source,
            mode: spec.mode,
            status: DtmfOperationStatus::Pending,
            media_generation: None,
            result_code: None,
            digits: None,
            digit_count: 0,
            reason: None,
            created_at_ms: now,
            updated_at_ms: now,
        };
        match runtime_tx.try_send(DtmfRuntimeOperation::Start {
            operation_id: operation_id.clone(),
            spec,
        }) {
            Ok(()) => {
                state.operations.insert(operation_id.clone(), view.clone());
                state.order.push_back(operation_id);
                trim_history(&mut state);
                Ok(view)
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                Err("RESOURCE_EXHAUSTED: DTMF operation queue is full")
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                state.runtime = None;
                Err("ADAPTER_UNAVAILABLE: adapter runtime operation queue is closed")
            }
        }
    }

    pub(crate) fn cancel(&self, operation_id: &str) -> Result<DtmfOperationView, &'static str> {
        let mut state = self.inner.lock().expect("DTMF operation lock poisoned");
        let Some(runtime_tx) = state.runtime.as_ref().map(|runtime| runtime.tx.clone()) else {
            return Err("ADAPTER_UNAVAILABLE: adapter runtime is not connected");
        };
        let Some(operation) = state.operations.get(operation_id) else {
            return Err("RESOURCE_NOT_FOUND: DTMF operation not found");
        };
        if operation.status.is_terminal() {
            return Err("FAILED_PRECONDITION: DTMF operation is already complete");
        }
        let call_id = operation.call_id.clone();
        match runtime_tx.try_send(DtmfRuntimeOperation::Cancel {
            operation_id: operation_id.to_string(),
            call_id,
        }) {
            Ok(()) => {
                let operation = state
                    .operations
                    .get_mut(operation_id)
                    .expect("operation exists");
                operation.status = DtmfOperationStatus::Cancelling;
                operation.updated_at_ms = unix_timestamp_ms();
                Ok(operation.clone())
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                Err("RESOURCE_EXHAUSTED: DTMF operation queue is full")
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                state.runtime = None;
                Err("ADAPTER_UNAVAILABLE: adapter runtime operation queue is closed")
            }
        }
    }

    pub(crate) fn get(&self, operation_id: &str) -> Option<DtmfOperationView> {
        self.inner
            .lock()
            .expect("DTMF operation lock poisoned")
            .operations
            .get(operation_id)
            .cloned()
    }

    pub(crate) fn mark_acquiring(&self, operation_id: &str) {
        self.update(operation_id, |operation| {
            operation.status = DtmfOperationStatus::Acquiring;
        });
    }

    pub(crate) fn mark_ready(&self, operation_id: &str, media_generation: u64) {
        self.update(operation_id, |operation| {
            operation.status = DtmfOperationStatus::Ready;
            operation.media_generation = Some(media_generation);
            operation.reason = None;
        });
    }

    pub(crate) fn complete(&self, operation_id: &str, outcome: DigitCollectionOutcome) {
        self.update(operation_id, |operation| {
            operation.status = match outcome.code {
                DigitCollectionResultCode::Cancelled => DtmfOperationStatus::Cancelled,
                DigitCollectionResultCode::Failed => DtmfOperationStatus::Failed,
                DigitCollectionResultCode::Completed
                | DigitCollectionResultCode::FirstDigitTimeout
                | DigitCollectionResultCode::InterDigitTimeout
                | DigitCollectionResultCode::OverallTimeout => DtmfOperationStatus::Completed,
            };
            operation.result_code = Some(result_code(outcome.code).to_string());
            operation.digit_count = outcome.digits.len();
            operation.digits = Some(format_digits(&outcome.digits));
            operation.reason = outcome.reason;
        });
    }

    pub(crate) fn fail(&self, operation_id: &str, reason: impl Into<String>) {
        let reason = reason.into();
        self.update(operation_id, move |operation| {
            operation.status = DtmfOperationStatus::Failed;
            operation.reason = Some(reason);
        });
    }

    fn update(&self, operation_id: &str, update: impl FnOnce(&mut DtmfOperationView)) {
        let mut state = self.inner.lock().expect("DTMF operation lock poisoned");
        if let Some(operation) = state.operations.get_mut(operation_id) {
            if operation.status.is_terminal() {
                return;
            }
            update(operation);
            operation.updated_at_ms = unix_timestamp_ms();
        }
    }
}

pub(crate) fn all_digits() -> HashSet<DtmfDigit> {
    HashSet::from([
        DtmfDigit::D0,
        DtmfDigit::D1,
        DtmfDigit::D2,
        DtmfDigit::D3,
        DtmfDigit::D4,
        DtmfDigit::D5,
        DtmfDigit::D6,
        DtmfDigit::D7,
        DtmfDigit::D8,
        DtmfDigit::D9,
        DtmfDigit::Star,
        DtmfDigit::Pound,
        DtmfDigit::A,
        DtmfDigit::B,
        DtmfDigit::C,
        DtmfDigit::D,
        DtmfDigit::Flash,
    ])
}

pub(crate) fn parse_digit_set(value: &str) -> Result<HashSet<DtmfDigit>, &'static str> {
    let mut digits = HashSet::new();
    for value in value.chars() {
        let digit = match value {
            '0' => DtmfDigit::D0,
            '1' => DtmfDigit::D1,
            '2' => DtmfDigit::D2,
            '3' => DtmfDigit::D3,
            '4' => DtmfDigit::D4,
            '5' => DtmfDigit::D5,
            '6' => DtmfDigit::D6,
            '7' => DtmfDigit::D7,
            '8' => DtmfDigit::D8,
            '9' => DtmfDigit::D9,
            '*' => DtmfDigit::Star,
            '#' => DtmfDigit::Pound,
            'A' | 'a' => DtmfDigit::A,
            'B' | 'b' => DtmfDigit::B,
            'C' | 'c' => DtmfDigit::C,
            'D' | 'd' => DtmfDigit::D,
            'F' | 'f' => DtmfDigit::Flash,
            _ => return Err("INVALID_ARGUMENT: unsupported DTMF digit"),
        };
        digits.insert(digit);
    }
    Ok(digits)
}

fn format_digits(digits: &[DtmfDigit]) -> String {
    digits.iter().map(|digit| digit_char(*digit)).collect()
}

fn digit_char(digit: DtmfDigit) -> char {
    match digit {
        DtmfDigit::D0 => '0',
        DtmfDigit::D1 => '1',
        DtmfDigit::D2 => '2',
        DtmfDigit::D3 => '3',
        DtmfDigit::D4 => '4',
        DtmfDigit::D5 => '5',
        DtmfDigit::D6 => '6',
        DtmfDigit::D7 => '7',
        DtmfDigit::D8 => '8',
        DtmfDigit::D9 => '9',
        DtmfDigit::Star => '*',
        DtmfDigit::Pound => '#',
        DtmfDigit::A => 'A',
        DtmfDigit::B => 'B',
        DtmfDigit::C => 'C',
        DtmfDigit::D => 'D',
        DtmfDigit::Flash => 'F',
    }
}

fn result_code(code: DigitCollectionResultCode) -> &'static str {
    match code {
        DigitCollectionResultCode::Completed => "completed",
        DigitCollectionResultCode::FirstDigitTimeout => "first_digit_timeout",
        DigitCollectionResultCode::InterDigitTimeout => "inter_digit_timeout",
        DigitCollectionResultCode::OverallTimeout => "overall_timeout",
        DigitCollectionResultCode::Cancelled => "cancelled",
        DigitCollectionResultCode::Failed => "failed",
    }
}

fn trim_history(state: &mut DtmfOperationState) {
    while state.order.len() > DTMF_OPERATION_HISTORY_CAPACITY {
        if let Some(operation_id) = state.order.pop_front() {
            state.operations.remove(&operation_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use voipswitch_core::types::ids::CollectorId;

    fn spec() -> DtmfOperationSpec {
        DtmfOperationSpec {
            call_id: "call-a".to_string(),
            source: DtmfOperationSource::Caller,
            mode: DtmfOperationMode::Collect,
            allowed: all_digits(),
            min_digits: 1,
            max_digits: 1,
            terminators: HashSet::new(),
            first_digit_timeout: Duration::from_secs(1),
            inter_digit_timeout: Duration::from_secs(1),
            overall_timeout: Duration::from_secs(2),
        }
    }

    #[test]
    fn service_requires_runtime_and_retains_only_bounded_history() {
        let service = DtmfOperationService::default();
        assert!(
            service
                .start(spec())
                .unwrap_err()
                .contains("ADAPTER_UNAVAILABLE")
        );
        let (_generation, mut rx) = service.attach_runtime();
        let mut first = None;
        for index in 0..=DTMF_OPERATION_HISTORY_CAPACITY {
            let mut spec = spec();
            spec.call_id = format!("call-{index}");
            let operation = service.start(spec).unwrap();
            first.get_or_insert(operation.operation_id);
            rx.try_recv().unwrap();
        }
        assert!(service.get(first.as_deref().unwrap()).is_none());
    }

    #[test]
    fn result_is_available_only_from_exact_operation_view() {
        let service = DtmfOperationService::default();
        let (_generation, mut rx) = service.attach_runtime();
        let operation = service.start(spec()).unwrap();
        rx.try_recv().unwrap();
        service.mark_acquiring(&operation.operation_id);
        service.mark_ready(&operation.operation_id, 4);
        service.complete(
            &operation.operation_id,
            DigitCollectionOutcome {
                collector_id: CollectorId::from(operation.operation_id.clone()),
                code: DigitCollectionResultCode::Completed,
                digits: vec![DtmfDigit::D5],
                reason: None,
            },
        );
        let view = service.get(&operation.operation_id).unwrap();
        assert_eq!(view.status, DtmfOperationStatus::Completed);
        assert_eq!(view.media_generation, Some(4));
        assert_eq!(view.digits.as_deref(), Some("5"));
        assert_eq!(view.digit_count, 1);
    }

    #[test]
    fn digit_set_parser_is_strict() {
        assert_eq!(parse_digit_set("5#A").unwrap().len(), 3);
        assert!(parse_digit_set("x").is_err());
    }
}
