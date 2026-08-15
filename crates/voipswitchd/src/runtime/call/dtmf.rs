use crate::runtime::call::actor::SessionActor;
use crate::runtime::dtmf::DtmfMediaMode;
use crate::runtime::media::{DtmfCapabilityLeaseRequest, DtmfCapabilityReady};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::future::pending;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::{Instant, Sleep};
use tracing::warn;
use voipswitch_core::dtmf::{
    DigitEvent, DtmfDigit, DtmfEventId, DtmfSourceGeneration, DtmfSourceLock, DtmfSourcePolicy,
    DtmfTransport,
};
use voipswitch_core::types::ids::{BusinessOperationId, CollectorId, SessionId};

const SOURCE_EVENT_DEDUP_CAPACITY: usize = 128;
pub(crate) const DTMF_COLLECTOR_CAPACITY: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DigitCollectionMode {
    Observe,
    Collect,
}

impl DigitCollectionMode {
    pub(crate) fn default_observe() -> Self {
        Self::Observe
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DigitCollectionSpec {
    pub(crate) collector_id: CollectorId,
    pub(crate) owner: BusinessOperationId,
    pub(crate) source_session_id: SessionId,
    pub(crate) mode: DigitCollectionMode,
    pub(crate) allowed: HashSet<DtmfDigit>,
    pub(crate) min_digits: usize,
    pub(crate) max_digits: usize,
    pub(crate) terminators: HashSet<DtmfDigit>,
    pub(crate) first_digit_timeout: Duration,
    pub(crate) inter_digit_timeout: Duration,
    pub(crate) overall_timeout: Duration,
}

impl DigitCollectionSpec {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.min_digits > self.max_digits || self.max_digits == 0 {
            return Err("invalid_digit_count_range");
        }
        if self.allowed.is_empty() {
            return Err("empty_allowed_digit_set");
        }
        if self.first_digit_timeout.is_zero()
            || self.inter_digit_timeout.is_zero()
            || self.overall_timeout.is_zero()
        {
            return Err("invalid_digit_collection_timeout");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DigitCollectionResultCode {
    Completed,
    FirstDigitTimeout,
    InterDigitTimeout,
    OverallTimeout,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DigitCollectionReady {
    pub(crate) collector_id: CollectorId,
    pub(crate) media_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DigitCollectionOutcome {
    pub(crate) collector_id: CollectorId,
    pub(crate) code: DigitCollectionResultCode,
    pub(crate) digits: Vec<DtmfDigit>,
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DigitCollectorDeadline {
    FirstDigit,
    InterDigit,
    Overall,
}

impl DigitCollectorDeadline {
    fn result_code(self) -> DigitCollectionResultCode {
        match self {
            Self::FirstDigit => DigitCollectionResultCode::FirstDigitTimeout,
            Self::InterDigit => DigitCollectionResultCode::InterDigitTimeout,
            Self::Overall => DigitCollectionResultCode::OverallTimeout,
        }
    }
}

pub(crate) struct DigitCollectorTimerSlot {
    pub(crate) generation: u64,
    pub(crate) collector_id: CollectorId,
    pub(crate) deadline: DigitCollectorDeadline,
    sleep: Pin<Box<Sleep>>,
}

impl DigitCollectorTimerSlot {
    pub(crate) fn new(
        generation: u64,
        collector_id: CollectorId,
        deadline: DigitCollectorDeadline,
        at: Instant,
    ) -> Self {
        Self {
            generation,
            collector_id,
            deadline,
            sleep: Box::pin(tokio::time::sleep_until(at)),
        }
    }
}

pub(crate) async fn wait_for_digit_collector_timer(
    slot: &mut Option<DigitCollectorTimerSlot>,
) -> (u64, CollectorId, DigitCollectorDeadline) {
    match slot {
        Some(timer) => {
            timer.sleep.as_mut().await;
            (timer.generation, timer.collector_id.clone(), timer.deadline)
        }
        None => pending().await,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DigitCollectorPhase {
    Acquiring,
    Active,
    Releasing,
}

pub(crate) struct DigitCollectorState {
    pub(crate) spec: DigitCollectionSpec,
    pub(crate) lease_id: voipswitch_core::types::ids::MediaCapabilityLeaseId,
    pub(crate) acquire_action_id: String,
    pub(crate) release_action_id: String,
    pub(crate) phase: DigitCollectorPhase,
    pub(crate) ready_reply: Option<oneshot::Sender<Result<DigitCollectionReady, String>>>,
    pub(crate) result_reply: Option<oneshot::Sender<DigitCollectionOutcome>>,
    pub(crate) digits: Vec<DtmfDigit>,
    pub(crate) cancel_requested: bool,
    pub(crate) first_deadline: Option<Instant>,
    pub(crate) inter_deadline: Option<Instant>,
    pub(crate) overall_deadline: Option<Instant>,
    pub(crate) pending_outcome: Option<DigitCollectionOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DigitCollectorInput {
    Ignored,
    Continue {
        consume: bool,
    },
    Finish {
        consume: bool,
        code: DigitCollectionResultCode,
        reason: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DigitCollectorCancel {
    AwaitAcquire,
    Finish,
    AlreadyReleasing,
}

struct DigitCollectorBatch {
    consume: bool,
    completed: Vec<(CollectorId, DigitCollectionResultCode, Option<String>)>,
}

fn validate_digit_collector_registration(
    collectors: &BTreeMap<CollectorId, DigitCollectorState>,
    spec: &DigitCollectionSpec,
) -> Result<(), &'static str> {
    if collectors.contains_key(&spec.collector_id) {
        return Err("dtmf_collector_id_conflict");
    }
    if collectors.len() >= DTMF_COLLECTOR_CAPACITY {
        return Err("dtmf_collector_overloaded");
    }
    if spec.mode == DigitCollectionMode::Collect
        && collectors.values().any(|collector| {
            collector.spec.mode == DigitCollectionMode::Collect
                && collector.spec.source_session_id == spec.source_session_id
        })
    {
        return Err("dtmf_collector_conflict");
    }
    Ok(())
}

fn apply_digit_to_collectors(
    collectors: &mut BTreeMap<CollectorId, DigitCollectorState>,
    event: &DigitEvent,
    now: Instant,
) -> DigitCollectorBatch {
    let collector_ids = collectors.keys().cloned().collect::<Vec<_>>();
    let mut batch = DigitCollectorBatch {
        consume: false,
        completed: Vec::new(),
    };
    for collector_id in collector_ids {
        let Some(collector) = collectors.get_mut(&collector_id) else {
            continue;
        };
        match collector.apply_digit(event, now) {
            DigitCollectorInput::Ignored => {}
            DigitCollectorInput::Continue { consume } => batch.consume |= consume,
            DigitCollectorInput::Finish {
                consume,
                code,
                reason,
            } => {
                batch.consume |= consume;
                batch.completed.push((collector_id, code, reason));
            }
        }
    }
    batch
}

fn next_digit_collector_deadline(
    collectors: &BTreeMap<CollectorId, DigitCollectorState>,
) -> Option<(Instant, CollectorId, DigitCollectorDeadline)> {
    collectors
        .iter()
        .filter_map(|(collector_id, collector)| {
            collector
                .next_deadline()
                .map(|(at, deadline)| (at, collector_id.clone(), deadline))
        })
        .min_by_key(|(at, _, _)| *at)
}

impl DigitCollectorState {
    #[allow(clippy::too_many_arguments)]
    fn new(
        spec: DigitCollectionSpec,
        lease_id: voipswitch_core::types::ids::MediaCapabilityLeaseId,
        acquire_action_id: String,
        release_action_id: String,
        ready_reply: oneshot::Sender<Result<DigitCollectionReady, String>>,
        result_reply: oneshot::Sender<DigitCollectionOutcome>,
    ) -> Self {
        Self {
            spec,
            lease_id,
            acquire_action_id,
            release_action_id,
            phase: DigitCollectorPhase::Acquiring,
            ready_reply: Some(ready_reply),
            result_reply: Some(result_reply),
            digits: Vec::new(),
            cancel_requested: false,
            first_deadline: None,
            inter_deadline: None,
            overall_deadline: None,
            pending_outcome: None,
        }
    }

    fn activate(&mut self, now: Instant) {
        self.phase = DigitCollectorPhase::Active;
        self.first_deadline = Some(now + self.spec.first_digit_timeout);
        self.overall_deadline = Some(now + self.spec.overall_timeout);
    }

    fn apply_digit(&mut self, event: &DigitEvent, now: Instant) -> DigitCollectorInput {
        if self.phase != DigitCollectorPhase::Active
            || self.spec.source_session_id != event.source_session_id
        {
            return DigitCollectorInput::Ignored;
        }
        let consume = self.spec.mode == DigitCollectionMode::Collect;
        if self.spec.terminators.contains(&event.digit) {
            let code = if self.digits.len() >= self.spec.min_digits {
                DigitCollectionResultCode::Completed
            } else {
                DigitCollectionResultCode::Failed
            };
            return DigitCollectorInput::Finish {
                consume,
                code,
                reason: (code == DigitCollectionResultCode::Failed)
                    .then(|| "digit_collection_too_short".to_string()),
            };
        }
        if !self.spec.allowed.contains(&event.digit) {
            return DigitCollectorInput::Continue { consume };
        }
        self.digits.push(event.digit);
        self.first_deadline = None;
        self.inter_deadline = Some(now + self.spec.inter_digit_timeout);
        if self.digits.len() >= self.spec.max_digits {
            DigitCollectorInput::Finish {
                consume,
                code: DigitCollectionResultCode::Completed,
                reason: None,
            }
        } else {
            DigitCollectorInput::Continue { consume }
        }
    }

    fn next_deadline(&self) -> Option<(Instant, DigitCollectorDeadline)> {
        [
            self.first_deadline
                .map(|at| (at, DigitCollectorDeadline::FirstDigit)),
            self.inter_deadline
                .map(|at| (at, DigitCollectorDeadline::InterDigit)),
            self.overall_deadline
                .map(|at| (at, DigitCollectorDeadline::Overall)),
        ]
        .into_iter()
        .flatten()
        .min_by_key(|(at, _)| *at)
    }

    fn request_cancel(&mut self) -> DigitCollectorCancel {
        match self.phase {
            DigitCollectorPhase::Acquiring => {
                self.cancel_requested = true;
                DigitCollectorCancel::AwaitAcquire
            }
            DigitCollectorPhase::Active => DigitCollectorCancel::Finish,
            DigitCollectorPhase::Releasing => DigitCollectorCancel::AlreadyReleasing,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DtmfSourceDecision {
    Accepted,
    Duplicate,
    Conflict,
    StaleGeneration,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DtmfSourceStats {
    pub(crate) accepted: u64,
    pub(crate) duplicate: u64,
    pub(crate) conflict: u64,
    pub(crate) stale_generation: u64,
}

pub(crate) struct DtmfSourceState {
    policy: DtmfSourcePolicy,
    source_lock: DtmfSourceLock,
    recent: HashSet<DtmfEventId>,
    order: VecDeque<DtmfEventId>,
    stats: DtmfSourceStats,
}

impl Default for DtmfSourceState {
    fn default() -> Self {
        Self {
            policy: DtmfSourcePolicy::Auto,
            source_lock: DtmfSourceLock::Unset,
            recent: HashSet::new(),
            order: VecDeque::new(),
            stats: DtmfSourceStats::default(),
        }
    }
}

impl DtmfSourceState {
    #[cfg(test)]
    pub(crate) fn with_policy(policy: DtmfSourcePolicy) -> Self {
        Self {
            policy,
            ..Self::default()
        }
    }

    pub(crate) fn accept(&mut self, event: &DigitEvent) -> DtmfSourceDecision {
        if !event.identity_is_consistent() {
            return self.record(DtmfSourceDecision::Conflict);
        }
        let Some(candidate_lock) = event_source_lock(event) else {
            return self.record(DtmfSourceDecision::Conflict);
        };
        if !self.policy_allows(event.transport) {
            return self.record(DtmfSourceDecision::Conflict);
        }
        match generation_order(self.source_lock, candidate_lock) {
            GenerationOrder::Initial | GenerationOrder::Newer => {
                self.source_lock = candidate_lock;
                self.recent.clear();
                self.order.clear();
            }
            GenerationOrder::Current => {}
            GenerationOrder::OtherTransport => {
                return self.record(DtmfSourceDecision::Conflict);
            }
            GenerationOrder::Older => {
                return self.record(DtmfSourceDecision::StaleGeneration);
            }
        }
        if !self.recent.insert(event.event_id.clone()) {
            return self.record(DtmfSourceDecision::Duplicate);
        }
        self.order.push_back(event.event_id.clone());
        while self.order.len() > SOURCE_EVENT_DEDUP_CAPACITY {
            if let Some(expired) = self.order.pop_front() {
                self.recent.remove(&expired);
            }
        }
        self.record(DtmfSourceDecision::Accepted)
    }

    pub(crate) fn stats(&self) -> DtmfSourceStats {
        self.stats
    }

    fn policy_allows(&self, transport: DtmfTransport) -> bool {
        match self.policy {
            DtmfSourcePolicy::Auto => true,
            DtmfSourcePolicy::Rfc4733Only => transport == DtmfTransport::Rfc4733,
            DtmfSourcePolicy::SipInfoOnly => matches!(
                transport,
                DtmfTransport::SipInfoRelay | DtmfTransport::SipInfoDtmf
            ),
        }
    }

    fn record(&mut self, decision: DtmfSourceDecision) -> DtmfSourceDecision {
        match decision {
            DtmfSourceDecision::Accepted => {
                self.stats.accepted = self.stats.accepted.saturating_add(1)
            }
            DtmfSourceDecision::Duplicate => {
                self.stats.duplicate = self.stats.duplicate.saturating_add(1)
            }
            DtmfSourceDecision::Conflict => {
                self.stats.conflict = self.stats.conflict.saturating_add(1)
            }
            DtmfSourceDecision::StaleGeneration => {
                self.stats.stale_generation = self.stats.stale_generation.saturating_add(1)
            }
        }
        decision
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GenerationOrder {
    Initial,
    Current,
    Newer,
    Older,
    OtherTransport,
}

fn event_source_lock(event: &DigitEvent) -> Option<DtmfSourceLock> {
    match (event.transport, event.source_generation) {
        (DtmfTransport::Rfc4733, DtmfSourceGeneration::Media(media_generation)) => {
            Some(DtmfSourceLock::Rfc4733 { media_generation })
        }
        (
            DtmfTransport::SipInfoRelay | DtmfTransport::SipInfoDtmf,
            DtmfSourceGeneration::Dialog(dialog_generation),
        ) => Some(DtmfSourceLock::SipInfo { dialog_generation }),
        _ => None,
    }
}

fn generation_order(current: DtmfSourceLock, candidate: DtmfSourceLock) -> GenerationOrder {
    match (current, candidate) {
        (DtmfSourceLock::Unset, _) => GenerationOrder::Initial,
        (
            DtmfSourceLock::Rfc4733 {
                media_generation: current,
            },
            DtmfSourceLock::Rfc4733 {
                media_generation: candidate,
            },
        ) => compare_generation(current, candidate),
        (
            DtmfSourceLock::SipInfo {
                dialog_generation: current,
            },
            DtmfSourceLock::SipInfo {
                dialog_generation: candidate,
            },
        ) => compare_generation(current, candidate),
        _ => GenerationOrder::OtherTransport,
    }
}

fn compare_generation(current: u64, candidate: u64) -> GenerationOrder {
    match candidate.cmp(&current) {
        std::cmp::Ordering::Less => GenerationOrder::Older,
        std::cmp::Ordering::Equal => GenerationOrder::Current,
        std::cmp::Ordering::Greater => GenerationOrder::Newer,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DtmfIgnoreReason {
    StaleSourceSession,
    MissingPeer,
    CallTerminating,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DtmfDisposition {
    Forward { peer_session_id: SessionId },
    Ignore { reason: DtmfIgnoreReason },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DtmfRouterStats {
    pub(crate) forwarded: u64,
    pub(crate) consumed: u64,
    pub(crate) ignored: u64,
    pub(crate) info_send_succeeded: u64,
    pub(crate) info_send_failed: u64,
}

#[derive(Default)]
pub(crate) struct DtmfRouter {
    stats: DtmfRouterStats,
}

impl DtmfRouter {
    pub(crate) fn route(
        &mut self,
        event: &DigitEvent,
        caller_session_id: &str,
        callee_session_id: Option<&str>,
        routable: bool,
    ) -> DtmfDisposition {
        let disposition = if !routable {
            DtmfDisposition::Ignore {
                reason: DtmfIgnoreReason::CallTerminating,
            }
        } else if event.source_session_id.as_str() == caller_session_id {
            match callee_session_id {
                Some(peer) => DtmfDisposition::Forward {
                    peer_session_id: SessionId::from(peer),
                },
                None => DtmfDisposition::Ignore {
                    reason: DtmfIgnoreReason::MissingPeer,
                },
            }
        } else if callee_session_id == Some(event.source_session_id.as_str()) {
            DtmfDisposition::Forward {
                peer_session_id: SessionId::from(caller_session_id),
            }
        } else {
            DtmfDisposition::Ignore {
                reason: DtmfIgnoreReason::StaleSourceSession,
            }
        };
        match disposition {
            DtmfDisposition::Forward { .. } => {
                self.stats.forwarded = self.stats.forwarded.saturating_add(1)
            }
            DtmfDisposition::Ignore { .. } => {
                self.stats.ignored = self.stats.ignored.saturating_add(1)
            }
        }
        disposition
    }

    pub(crate) fn stats(&self) -> DtmfRouterStats {
        self.stats
    }

    pub(crate) fn record_info_send_result(&mut self, succeeded: bool) {
        if succeeded {
            self.stats.info_send_succeeded = self.stats.info_send_succeeded.saturating_add(1);
        } else {
            self.stats.info_send_failed = self.stats.info_send_failed.saturating_add(1);
        }
    }

    pub(crate) fn record_consumed(&mut self) {
        self.stats.consumed = self.stats.consumed.saturating_add(1);
    }
}

impl SessionActor {
    pub(crate) fn start_digit_collection(
        &mut self,
        spec: DigitCollectionSpec,
        ready_reply: oneshot::Sender<Result<DigitCollectionReady, String>>,
        result_reply: oneshot::Sender<DigitCollectionOutcome>,
    ) {
        let validation = spec.validate().map_err(str::to_string).and_then(|_| {
            if !self.call.is_answered() || self.call.is_terminating() {
                return Err("call_or_leg_terminating".to_string());
            }
            validate_digit_collector_registration(&self.dtmf_collectors, &spec)
                .map_err(str::to_string)?;
            if spec.source_session_id.as_str() != self.call.caller_session_id()
                && spec.source_session_id.as_str() != self.call.callee_session_id()
            {
                return Err("stale_source_session".to_string());
            }
            Ok(())
        });
        if let Err(error) = validation {
            let _ = ready_reply.send(Err(error.clone()));
            let _ = result_reply.send(DigitCollectionOutcome {
                collector_id: spec.collector_id,
                code: DigitCollectionResultCode::Failed,
                digits: Vec::new(),
                reason: Some(error),
            });
            return;
        }

        let lease_id = voipswitch_core::types::ids::MediaCapabilityLeaseId::from(format!(
            "dtmf-collector-{}-{}",
            self.call.call_id(),
            spec.collector_id
        ));
        let acquire_action_id = format!("acquire-{lease_id}");
        let release_action_id = format!("release-{lease_id}");
        let request = DtmfCapabilityLeaseRequest {
            lease_id: lease_id.clone(),
            owner: spec.owner.clone(),
            source_session_id: spec.source_session_id.clone(),
            mode: if spec.mode == DigitCollectionMode::default_observe() {
                DtmfMediaMode::Observe
            } else {
                DtmfMediaMode::Collect
            },
            requested_generation: self.media_generation,
        };
        let collector_id = spec.collector_id.clone();
        self.dtmf_collectors.insert(
            collector_id.clone(),
            DigitCollectorState::new(
                spec,
                lease_id,
                acquire_action_id.clone(),
                release_action_id,
                ready_reply,
                result_reply,
            ),
        );
        if let Err(error) = self.media_executor.acquire_dtmf_capability(
            acquire_action_id,
            self.media_generation,
            request,
        ) {
            self.fail_digit_collection_start(&collector_id, error.to_string());
        }
        self.publish_call_view();
    }

    pub(crate) fn cancel_digit_collection(
        &mut self,
        collector_id: &CollectorId,
    ) -> Result<(), String> {
        let Some(collector) = self.dtmf_collectors.get_mut(collector_id) else {
            return Err("dtmf_collector_not_found".to_string());
        };
        let cancel = collector.request_cancel();
        match cancel {
            DigitCollectorCancel::AwaitAcquire => {
                if let Some(reply) = collector.ready_reply.take() {
                    let _ = reply.send(Err("dtmf_collection_cancelled".to_string()));
                }
            }
            DigitCollectorCancel::Finish => {
                self.finish_digit_collection(
                    collector_id,
                    DigitCollectionResultCode::Cancelled,
                    Some("dtmf_collection_cancelled".to_string()),
                );
            }
            DigitCollectorCancel::AlreadyReleasing => {}
        }
        Ok(())
    }

    pub(crate) fn handle_dtmf_capability_acquired(
        &mut self,
        action_id: String,
        result: Result<DtmfCapabilityReady, String>,
    ) {
        let Some(collector_id) =
            self.dtmf_collectors
                .iter()
                .find_map(|(collector_id, collector)| {
                    (collector.phase == DigitCollectorPhase::Acquiring
                        && collector.acquire_action_id == action_id)
                        .then(|| collector_id.clone())
                })
        else {
            return;
        };
        match result {
            Err(error) => self.fail_digit_collection_start(&collector_id, error),
            Ok(ready) => {
                let mismatch = self
                    .dtmf_collectors
                    .get(&collector_id)
                    .is_none_or(|collector| {
                        ready.lease_id != collector.lease_id
                            || ready.source_session_id != collector.spec.source_session_id
                    });
                if mismatch {
                    self.fail_digit_collection_start(
                        &collector_id,
                        "dtmf_capability_result_mismatch".to_string(),
                    );
                    return;
                }
                let collector = self
                    .dtmf_collectors
                    .get_mut(&collector_id)
                    .expect("collector action was resolved");
                let cancelled = collector.cancel_requested;
                if let Some(reply) = collector.ready_reply.take() {
                    let response = if cancelled {
                        Err("dtmf_collection_cancelled".to_string())
                    } else {
                        Ok(DigitCollectionReady {
                            collector_id: collector.spec.collector_id.clone(),
                            media_generation: ready.media_generation,
                        })
                    };
                    let _ = reply.send(response);
                }
                if cancelled {
                    collector.phase = DigitCollectorPhase::Active;
                    self.finish_digit_collection(
                        &collector_id,
                        DigitCollectionResultCode::Cancelled,
                        Some("dtmf_collection_cancelled".to_string()),
                    );
                    return;
                }
                let now = Instant::now();
                collector.activate(now);
                self.schedule_digit_collector_timer();
                self.publish_call_view();
            }
        }
    }

    pub(crate) fn handle_dtmf_capability_released(
        &mut self,
        action_id: String,
        result: Result<u64, String>,
    ) {
        let Some(collector_id) =
            self.dtmf_collectors
                .iter()
                .find_map(|(collector_id, collector)| {
                    (collector.phase == DigitCollectorPhase::Releasing
                        && collector.release_action_id == action_id)
                        .then(|| collector_id.clone())
                })
        else {
            return;
        };
        let mut collector = self
            .dtmf_collectors
            .remove(&collector_id)
            .expect("collector action was resolved");
        let mut outcome = collector
            .pending_outcome
            .take()
            .unwrap_or(DigitCollectionOutcome {
                collector_id: collector.spec.collector_id.clone(),
                code: DigitCollectionResultCode::Failed,
                digits: collector.digits.clone(),
                reason: Some("missing_collection_outcome".to_string()),
            });
        match result {
            Ok(_media_generation) => {}
            Err(error) => {
                outcome.code = DigitCollectionResultCode::Failed;
                outcome.reason = Some(format!("capability_release_failed:{error}"));
            }
        }
        if let Some(reply) = collector.result_reply.take() {
            let _ = reply.send(outcome);
        }
        self.schedule_digit_collector_timer();
        self.publish_call_view();
    }

    pub(crate) fn collect_digit(&mut self, event: &DigitEvent) -> bool {
        let batch = apply_digit_to_collectors(&mut self.dtmf_collectors, event, Instant::now());
        for (collector_id, code, reason) in batch.completed {
            self.finish_digit_collection(&collector_id, code, reason);
        }
        self.schedule_digit_collector_timer();
        batch.consume
    }

    pub(crate) fn handle_digit_collector_timeout(
        &mut self,
        generation: u64,
        collector_id: &CollectorId,
        deadline: DigitCollectorDeadline,
    ) -> anyhow::Result<()> {
        if generation != self.dtmf_collector_timer_generation {
            return Ok(());
        }
        self.dtmf_collector_timer = None;
        self.finish_digit_collection(collector_id, deadline.result_code(), None);
        self.schedule_digit_collector_timer();
        Ok(())
    }

    pub(super) fn abort_digit_collection(&mut self, reason: &str) {
        self.dtmf_collector_timer_generation =
            self.dtmf_collector_timer_generation.saturating_add(1);
        self.dtmf_collector_timer = None;
        if self.dtmf_collectors.is_empty() {
            return;
        }
        for (_, mut collector) in std::mem::take(&mut self.dtmf_collectors) {
            if let Some(reply) = collector.ready_reply.take() {
                let _ = reply.send(Err(reason.to_string()));
            }
            // Acquire and release share one ordered media worker. Enqueuing release
            // while acquire is pending guarantees that a late Ready cannot leak a
            // capability lease after the actor starts call teardown.
            if let Err(error) = self.media_executor.release_dtmf_capability(
                collector.release_action_id.clone(),
                self.media_generation,
                collector.lease_id.clone(),
            ) {
                warn!(
                    call_id = self.call.call_id(),
                    collector_id = collector.spec.collector_id.as_str(),
                    lease_id = collector.lease_id.as_str(),
                    error = %error,
                    "failed to enqueue DTMF capability release during collector abort"
                );
            }
            if let Some(reply) = collector.result_reply.take() {
                let _ = reply.send(DigitCollectionOutcome {
                    collector_id: collector.spec.collector_id,
                    code: DigitCollectionResultCode::Failed,
                    digits: collector.digits,
                    reason: Some(reason.to_string()),
                });
            }
        }
    }

    fn fail_digit_collection_start(&mut self, collector_id: &CollectorId, error: String) {
        let Some(mut collector) = self.dtmf_collectors.remove(collector_id) else {
            return;
        };
        if let Some(reply) = collector.ready_reply.take() {
            let _ = reply.send(Err(error.clone()));
        }
        if let Some(reply) = collector.result_reply.take() {
            let code = if collector.cancel_requested {
                DigitCollectionResultCode::Cancelled
            } else {
                DigitCollectionResultCode::Failed
            };
            let _ = reply.send(DigitCollectionOutcome {
                collector_id: collector.spec.collector_id,
                code,
                digits: collector.digits,
                reason: Some(error),
            });
        }
        self.schedule_digit_collector_timer();
        self.publish_call_view();
    }

    fn finish_digit_collection(
        &mut self,
        collector_id: &CollectorId,
        code: DigitCollectionResultCode,
        reason: Option<String>,
    ) {
        let Some(collector) = self.dtmf_collectors.get_mut(collector_id) else {
            return;
        };
        if collector.phase != DigitCollectorPhase::Active {
            return;
        }
        collector.phase = DigitCollectorPhase::Releasing;
        collector.first_deadline = None;
        collector.inter_deadline = None;
        collector.overall_deadline = None;
        collector.pending_outcome = Some(DigitCollectionOutcome {
            collector_id: collector.spec.collector_id.clone(),
            code,
            digits: collector.digits.clone(),
            reason,
        });
        let release_action_id = collector.release_action_id.clone();
        let lease_id = collector.lease_id.clone();
        let media_generation = self.runtime.media_generation;
        if let Err(error) = self.media_executor.release_dtmf_capability(
            release_action_id.clone(),
            media_generation,
            lease_id,
        ) {
            self.handle_dtmf_capability_released(release_action_id, Err(error.to_string()));
        }
        self.schedule_digit_collector_timer();
    }

    fn schedule_digit_collector_timer(&mut self) {
        let deadline = next_digit_collector_deadline(&self.dtmf_collectors);
        self.dtmf_collector_timer_generation =
            self.dtmf_collector_timer_generation.saturating_add(1);
        self.dtmf_collector_timer = deadline.map(|(at, collector_id, deadline)| {
            DigitCollectorTimerSlot::new(
                self.dtmf_collector_timer_generation,
                collector_id,
                deadline,
                at,
            )
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use voipswitch_core::dtmf::{DtmfDigit, DtmfEventId};
    use voipswitch_core::types::ids::{
        BusinessOperationId, CallId, CollectorId, DomainId, MediaCapabilityLeaseId,
    };

    fn event(session_id: &str, generation: u64, timestamp: u32) -> DigitEvent {
        let source_session_id = SessionId::from(session_id);
        DigitEvent {
            event_id: DtmfEventId::Rfc4733 {
                media_generation: generation,
                source_session_id: source_session_id.clone(),
                ssrc: 7,
                timestamp,
                event_code: 5,
            },
            domain_id: DomainId::from("domain-a"),
            call_id: CallId::from("call-a"),
            source_session_id,
            source_media_leg_id: None,
            digit: DtmfDigit::D5,
            transport: DtmfTransport::Rfc4733,
            duration_ms: 100,
            observed_at_ms: 1,
            source_generation: DtmfSourceGeneration::Media(generation),
            incomplete_end: false,
        }
    }

    fn collection_spec(mode: DigitCollectionMode) -> DigitCollectionSpec {
        DigitCollectionSpec {
            collector_id: CollectorId::from("collector-a"),
            owner: BusinessOperationId::from("operation-a"),
            source_session_id: SessionId::from("caller"),
            mode,
            allowed: HashSet::from([DtmfDigit::D5]),
            min_digits: 1,
            max_digits: 2,
            terminators: HashSet::from([DtmfDigit::Pound]),
            first_digit_timeout: Duration::from_secs(5),
            inter_digit_timeout: Duration::from_secs(3),
            overall_timeout: Duration::from_secs(20),
        }
    }

    fn collector_for(
        mode: DigitCollectionMode,
        collector_id: &str,
        source_session_id: &str,
    ) -> DigitCollectorState {
        let (ready_reply, _ready) = oneshot::channel();
        let (result_reply, _result) = oneshot::channel();
        let mut spec = collection_spec(mode);
        spec.collector_id = CollectorId::from(collector_id);
        spec.owner = BusinessOperationId::from(format!("operation-{collector_id}"));
        spec.source_session_id = SessionId::from(source_session_id);
        DigitCollectorState::new(
            spec,
            MediaCapabilityLeaseId::from(format!("lease-{collector_id}")),
            format!("acquire-{collector_id}"),
            format!("release-{collector_id}"),
            ready_reply,
            result_reply,
        )
    }

    fn collector(mode: DigitCollectionMode) -> DigitCollectorState {
        collector_for(mode, "collector-a", "caller")
    }

    #[test]
    fn source_state_deduplicates_and_advances_generation() {
        let mut state = DtmfSourceState::default();
        let first = event("caller", 1, 10);
        assert_eq!(state.accept(&first), DtmfSourceDecision::Accepted);
        assert_eq!(state.accept(&first), DtmfSourceDecision::Duplicate);
        assert_eq!(
            state.accept(&event("caller", 0, 20)),
            DtmfSourceDecision::StaleGeneration
        );
        assert_eq!(
            state.accept(&event("caller", 2, 10)),
            DtmfSourceDecision::Accepted
        );
        assert_eq!(state.stats().accepted, 2);
        assert_eq!(state.stats().duplicate, 1);
        assert_eq!(state.stats().stale_generation, 1);
    }

    #[test]
    fn explicit_source_policy_rejects_other_transport() {
        let mut state = DtmfSourceState::with_policy(DtmfSourcePolicy::SipInfoOnly);
        assert_eq!(
            state.accept(&event("caller", 1, 10)),
            DtmfSourceDecision::Conflict
        );
        assert_eq!(state.stats().conflict, 1);
    }

    #[test]
    fn router_uses_current_bridge_edge_and_rejects_old_attempt() {
        let mut router = DtmfRouter::default();
        assert_eq!(
            router.route(&event("caller", 1, 10), "caller", Some("callee-2"), true,),
            DtmfDisposition::Forward {
                peer_session_id: SessionId::from("callee-2")
            }
        );
        assert_eq!(
            router.route(&event("callee-2", 1, 11), "caller", Some("callee-2"), true,),
            DtmfDisposition::Forward {
                peer_session_id: SessionId::from("caller")
            }
        );
        assert_eq!(
            router.route(&event("callee-1", 1, 12), "caller", Some("callee-2"), true,),
            DtmfDisposition::Ignore {
                reason: DtmfIgnoreReason::StaleSourceSession
            }
        );
        assert_eq!(router.stats().forwarded, 2);
        assert_eq!(router.stats().ignored, 1);

        assert_eq!(
            router.route(&event("caller", 1, 13), "caller", Some("callee-2"), false,),
            DtmfDisposition::Ignore {
                reason: DtmfIgnoreReason::CallTerminating
            }
        );
    }

    #[test]
    fn collection_spec_rejects_invalid_ranges_and_timeouts() {
        let mut spec = collection_spec(DigitCollectionMode::Collect);
        assert_eq!(spec.validate(), Ok(()));

        spec.max_digits = 0;
        assert_eq!(spec.validate(), Err("invalid_digit_count_range"));
        spec.max_digits = 2;
        spec.allowed.clear();
        assert_eq!(spec.validate(), Err("empty_allowed_digit_set"));
        spec.allowed.insert(DtmfDigit::D5);
        spec.overall_timeout = Duration::ZERO;
        assert_eq!(spec.validate(), Err("invalid_digit_collection_timeout"));
    }

    #[test]
    fn collection_timers_start_only_after_ready_and_choose_nearest_deadline() {
        let mut collector = collector(DigitCollectionMode::Collect);
        assert_eq!(collector.phase, DigitCollectorPhase::Acquiring);
        assert!(collector.next_deadline().is_none());

        let ready_at = Instant::now();
        collector.activate(ready_at);
        assert_eq!(collector.phase, DigitCollectorPhase::Active);
        assert_eq!(
            collector.next_deadline(),
            Some((
                ready_at + Duration::from_secs(5),
                DigitCollectorDeadline::FirstDigit
            ))
        );

        let digit_at = ready_at + Duration::from_secs(1);
        assert_eq!(
            collector.apply_digit(&event("caller", 1, 10), digit_at),
            DigitCollectorInput::Continue { consume: true }
        );
        assert_eq!(
            collector.next_deadline(),
            Some((
                digit_at + Duration::from_secs(3),
                DigitCollectorDeadline::InterDigit
            ))
        );
    }

    #[test]
    fn observe_forwards_while_collect_consumes_matching_source() {
        let now = Instant::now();
        let input = event("caller", 1, 10);
        let mut observe = collector(DigitCollectionMode::Observe);
        observe.activate(now);
        assert_eq!(
            observe.apply_digit(&input, now),
            DigitCollectorInput::Continue { consume: false }
        );

        let mut collect = collector(DigitCollectionMode::Collect);
        collect.activate(now);
        assert_eq!(
            collect.apply_digit(&input, now),
            DigitCollectorInput::Continue { consume: true }
        );
        assert_eq!(
            collect.apply_digit(&event("stale", 1, 11), now),
            DigitCollectorInput::Ignored
        );
    }

    #[test]
    fn multiple_observers_receive_the_same_digit_without_consuming_it() {
        let now = Instant::now();
        let mut collectors = BTreeMap::new();
        for collector_id in ["observe-a", "observe-b"] {
            let mut collector = collector_for(DigitCollectionMode::Observe, collector_id, "caller");
            collector.spec.max_digits = 1;
            collector.activate(now);
            collectors.insert(collector.spec.collector_id.clone(), collector);
        }

        let batch = apply_digit_to_collectors(&mut collectors, &event("caller", 1, 10), now);
        assert!(!batch.consume);
        assert_eq!(batch.completed.len(), 2);
        assert!(
            batch
                .completed
                .iter()
                .all(|(_, code, _)| *code == DigitCollectionResultCode::Completed)
        );
    }

    #[test]
    fn collect_and_observe_share_events_but_collect_suppresses_forwarding() {
        let now = Instant::now();
        let mut collectors = BTreeMap::new();
        for (collector_id, mode) in [
            ("observe", DigitCollectionMode::Observe),
            ("collect", DigitCollectionMode::Collect),
        ] {
            let mut collector = collector_for(mode, collector_id, "caller");
            collector.spec.max_digits = 1;
            collector.activate(now);
            collectors.insert(collector.spec.collector_id.clone(), collector);
        }

        let batch = apply_digit_to_collectors(&mut collectors, &event("caller", 1, 10), now);
        assert!(batch.consume);
        assert_eq!(batch.completed.len(), 2);
    }

    #[test]
    fn collector_registry_allows_shared_observe_and_one_collect_per_source() {
        let mut collectors = BTreeMap::new();
        let caller_collect = collector_for(DigitCollectionMode::Collect, "collect-a", "caller");
        collectors.insert(caller_collect.spec.collector_id.clone(), caller_collect);

        let same_source_collect =
            collector_for(DigitCollectionMode::Collect, "collect-b", "caller");
        assert_eq!(
            validate_digit_collector_registration(&collectors, &same_source_collect.spec),
            Err("dtmf_collector_conflict")
        );
        let same_source_observe =
            collector_for(DigitCollectionMode::Observe, "observe-a", "caller");
        assert_eq!(
            validate_digit_collector_registration(&collectors, &same_source_observe.spec),
            Ok(())
        );
        let other_source_collect =
            collector_for(DigitCollectionMode::Collect, "collect-c", "callee");
        assert_eq!(
            validate_digit_collector_registration(&collectors, &other_source_collect.spec),
            Ok(())
        );
    }

    #[test]
    fn collector_registry_is_bounded_and_timer_selects_nearest_deadline() {
        let now = Instant::now();
        let mut collectors = BTreeMap::new();
        for index in 0..DTMF_COLLECTOR_CAPACITY {
            let collector_id = format!("observe-{index:02}");
            let mut collector =
                collector_for(DigitCollectionMode::Observe, &collector_id, "caller");
            collector.spec.first_digit_timeout = Duration::from_secs(5 + index as u64);
            collector.activate(now);
            collectors.insert(collector.spec.collector_id.clone(), collector);
        }
        let overflow = collector_for(DigitCollectionMode::Observe, "overflow", "callee");
        assert_eq!(
            validate_digit_collector_registration(&collectors, &overflow.spec),
            Err("dtmf_collector_overloaded")
        );
        let (deadline, collector_id, kind) =
            next_digit_collector_deadline(&collectors).expect("nearest deadline");
        assert_eq!(collector_id.as_str(), "observe-00");
        assert_eq!(kind, DigitCollectorDeadline::FirstDigit);
        assert_eq!(deadline, now + Duration::from_secs(5));
    }

    #[test]
    fn collection_finishes_on_max_digits_or_terminator() {
        let now = Instant::now();
        let mut max_digits = collector(DigitCollectionMode::Collect);
        max_digits.activate(now);
        assert!(matches!(
            max_digits.apply_digit(&event("caller", 1, 10), now),
            DigitCollectorInput::Continue { consume: true }
        ));
        assert_eq!(
            max_digits.apply_digit(&event("caller", 1, 11), now),
            DigitCollectorInput::Finish {
                consume: true,
                code: DigitCollectionResultCode::Completed,
                reason: None,
            }
        );

        let mut terminated = collector(DigitCollectionMode::Observe);
        terminated.activate(now);
        terminated.digits.push(DtmfDigit::D5);
        let mut terminator = event("caller", 1, 12);
        terminator.digit = DtmfDigit::Pound;
        assert_eq!(
            terminated.apply_digit(&terminator, now),
            DigitCollectorInput::Finish {
                consume: false,
                code: DigitCollectionResultCode::Completed,
                reason: None,
            }
        );

        let mut too_short = collector(DigitCollectionMode::Collect);
        too_short.activate(now);
        assert_eq!(
            too_short.apply_digit(&terminator, now),
            DigitCollectorInput::Finish {
                consume: true,
                code: DigitCollectionResultCode::Failed,
                reason: Some("digit_collection_too_short".to_string()),
            }
        );
    }

    #[test]
    fn collector_timeout_and_cancel_states_are_explicit() {
        assert_eq!(
            DigitCollectorDeadline::FirstDigit.result_code(),
            DigitCollectionResultCode::FirstDigitTimeout
        );
        assert_eq!(
            DigitCollectorDeadline::InterDigit.result_code(),
            DigitCollectionResultCode::InterDigitTimeout
        );
        assert_eq!(
            DigitCollectorDeadline::Overall.result_code(),
            DigitCollectionResultCode::OverallTimeout
        );

        let mut collector = collector(DigitCollectionMode::Collect);
        assert_eq!(
            collector.request_cancel(),
            DigitCollectorCancel::AwaitAcquire
        );
        assert!(collector.cancel_requested);
        collector.phase = DigitCollectorPhase::Active;
        assert_eq!(collector.request_cancel(), DigitCollectorCancel::Finish);
        collector.phase = DigitCollectorPhase::Releasing;
        assert_eq!(
            collector.request_cancel(),
            DigitCollectorCancel::AlreadyReleasing
        );
    }
}
