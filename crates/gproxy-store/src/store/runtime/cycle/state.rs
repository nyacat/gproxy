use crate::StoreError;
use crate::records::{
    CredentialQuotaCycleRecord, CredentialQuotaObservation, CycleTracking, QuotaBoundaryConfidence,
    QuotaBoundarySource,
};
use rust_decimal::Decimal;

/// Absolute cap on stamp jitter. Sliding unused windows walk farther than
/// this; those are pinned by [`hold_boundary`] via slide detection, not by
/// widening this cap — a week-long window would swallow a real reset.
const BOUNDARY_SLACK_SECONDS: i64 = 300;

/// Persist a heartbeat at most this often when counters and boundaries are
/// unchanged. Header observations still skip the store entirely.
pub(super) const HEARTBEAT_SECONDS: i64 = 30;

/// Normalise, then check the store's input contract. Returns the observation
/// that should actually be recorded.
pub(super) fn settle(
    input: &CredentialQuotaObservation,
) -> Result<CredentialQuotaObservation, StoreError> {
    let mut input = input.clone();
    unstarted(&mut input);
    validate(&input)?;
    Ok(input)
}

/// A rolling window that has not been used yet reports its whole length as
/// remaining, so `reset_at - window` lands on the observation instant and walks
/// forward with every probe (Codex Spark does this on `/wham/usage` and in its
/// `x-…-reset-at` headers). Only an absolute zero counter proves the window is
/// unused; a rounded percentage cannot establish that fact.
fn unstarted(input: &mut CredentialQuotaObservation) {
    // A rounded percentage of zero does not prove that the window is unused.
    let unused = input.unit.is_some() && input.upstream_used == Some(Decimal::ZERO);
    let starts_now = input
        .period_start
        .is_some_and(|start| start >= input.observed_at - 5);
    if unused && starts_now {
        input.period_start = None;
        input.period_end = None;
        input.boundary_source = QuotaBoundarySource::Unknown;
        input.boundary_confidence = QuotaBoundaryConfidence::Unknown;
    }
}

/// Compare drift with the canonical stamp, so repeated small corrections cannot
/// walk the boundary indefinitely without ever crossing the rollover threshold.
///
/// Unused rolling windows (Codex Spark `/wham/usage` and `x-…-reset-at`) report
/// `reset_at = now + W`, so both ends advance together. That is a slide of the
/// same period, not a rollover: pin the first canonical stamp instead of minting
/// a cycle every five minutes.
pub(super) fn hold_boundary(
    previous: &CredentialQuotaCycleRecord,
    next: &mut CredentialQuotaObservation,
) {
    if rollover(previous, next) {
        return;
    }
    if sliding(
        previous.period_start,
        previous.period_end,
        next.period_start,
        next.period_end,
    ) {
        next.period_start = previous.period_start;
        next.period_end = previous.period_end;
        return;
    }
    if slack(previous.period_start, next.period_start) {
        next.period_start = previous.period_start;
    }
    if slack(previous.period_end, next.period_end) {
        next.period_end = previous.period_end;
    }
}

fn rollover(previous: &CredentialQuotaCycleRecord, next: &CredentialQuotaObservation) -> bool {
    previous
        .period_start
        .zip(previous.period_end)
        .zip(next.period_start)
        .is_some_and(|((start, end), new_start)| {
            next.observed_at >= end
                && new_start > start
                && end.abs_diff(new_start) <= window_slack(end.saturating_sub(start))
        })
}

fn slack(previous: Option<i64>, next: Option<i64>) -> bool {
    previous.zip(next).is_some_and(|(previous, next)| {
        previous.abs_diff(next) <= BOUNDARY_SLACK_SECONDS.unsigned_abs()
    })
}

/// Both ends advanced by about the same Δ, window length preserved, and the
/// shift is smaller than the window — the unused rolling-window pattern.
fn sliding(
    old_start: Option<i64>,
    old_end: Option<i64>,
    new_start: Option<i64>,
    new_end: Option<i64>,
) -> bool {
    let Some(((old_start, old_end), (new_start, new_end))) =
        old_start.zip(old_end).zip(new_start.zip(new_end))
    else {
        return false;
    };
    let old_len = old_end.saturating_sub(old_start);
    let new_len = new_end.saturating_sub(new_start);
    if old_len <= 0 || new_len <= 0 {
        return false;
    }
    let length_slack = window_slack(old_len).max(1);
    if old_len.abs_diff(new_len) > length_slack {
        return false;
    }
    let delta_start = new_start - old_start;
    let delta_end = new_end - old_end;
    if delta_start.unsigned_abs() != 0
        && delta_start.unsigned_abs() > window_slack(old_len)
        && delta_start.signum() != delta_end.signum()
    {
        return false;
    }
    if delta_start.abs_diff(delta_end) > window_slack(old_len) {
        return false;
    }
    let shift = delta_start.unsigned_abs().max(delta_end.unsigned_abs());
    if shift == 0 {
        return true;
    }
    // Near-boundary jitter is handled by hold_boundary once the old period
    // has elapsed. Before then, a rolling unused window can approach its end.
    if shift >= old_len as u64 {
        return false;
    }
    new_start < old_end
}

fn window_slack(length: i64) -> u64 {
    let relative = u64::try_from(length.max(0) / 100).unwrap_or(0).max(5);
    relative.min(BOUNDARY_SLACK_SECONDS as u64)
}

fn validate(input: &CredentialQuotaObservation) -> Result<(), StoreError> {
    let invalid = input.window_key.trim().is_empty()
        || input
            .period_start
            .zip(input.period_end)
            .is_some_and(|(start, end)| end <= start)
        || input
            .period_start
            .is_some_and(|start| start > input.observed_at)
        || input.upstream_used.is_some_and(|used| used < Decimal::ZERO)
        || input
            .upstream_limit
            .is_some_and(|limit| limit <= Decimal::ZERO)
        || input
            .used_percent
            .is_some_and(|percent| percent < Decimal::ZERO)
        || input.sample.started_at_ms > input.sample.received_at_ms;
    if invalid {
        return Err(StoreError::InvalidData {
            field: "quota observation",
            message: "invalid window, sample or counter".into(),
        });
    }
    Ok(())
}

pub(super) fn sample(input: &CredentialQuotaObservation) -> gproxy_core::QuotaSample {
    input.sample
}

pub(super) fn percent(
    percent: Option<Decimal>,
    used: Option<Decimal>,
    limit: Option<Decimal>,
) -> Option<Decimal> {
    percent.or_else(|| {
        used.zip(limit)
            .filter(|(_, limit)| *limit > Decimal::ZERO)
            .map(|(used, limit)| used / limit * Decimal::ONE_HUNDRED)
    })
}

pub(super) fn changed(
    open: &CredentialQuotaCycleRecord,
    next: &CredentialQuotaObservation,
) -> bool {
    rollover(open, next)
        || open
            .period_start
            .zip(next.period_start)
            .is_some_and(|(old, new)| old.abs_diff(new) > BOUNDARY_SLACK_SECONDS as u64)
        || open
            .period_end
            .zip(next.period_end)
            .is_some_and(|(old, new)| old.abs_diff(new) > BOUNDARY_SLACK_SECONDS as u64)
}

pub(super) fn counters_unchanged(
    open: &CredentialQuotaCycleRecord,
    next: &CredentialQuotaObservation,
) -> bool {
    open.upstream_used == next.upstream_used
        && open.upstream_limit == next.upstream_limit
        && percent(open.used_percent, open.upstream_used, open.upstream_limit)
            == percent(next.used_percent, next.upstream_used, next.upstream_limit)
        && open.label == next.label
}

pub(super) fn heartbeat_due(
    open: &CredentialQuotaCycleRecord,
    next: &CredentialQuotaObservation,
) -> bool {
    next.observed_at.saturating_sub(open.last_observed_at) >= HEARTBEAT_SECONDS
}

pub(super) fn adjusted(
    open: &CredentialQuotaCycleRecord,
    next: &CredentialQuotaObservation,
) -> bool {
    open.upstream_limit
        .zip(next.upstream_limit)
        .is_some_and(|(old, new)| old != new)
        || (open.tracking.scope != next.scope
            || open.tracking.unit != next.unit
            || open.tracking.reset_behavior != next.reset_behavior)
}

pub(super) fn decreased(
    open: &CredentialQuotaCycleRecord,
    next: &CredentialQuotaObservation,
) -> bool {
    if next.reset_behavior != gproxy_core::QuotaResetBehavior::Periodic || adjusted(open, next) {
        return false;
    }
    if next.unit.is_some()
        && let Some((old, new)) = open.upstream_used.zip(next.upstream_used)
    {
        return new < old;
    }
    percent(open.used_percent, open.upstream_used, open.upstream_limit)
        .zip(percent(
            next.used_percent,
            next.upstream_used,
            next.upstream_limit,
        ))
        .is_some_and(|(old, new)| new < old)
}

pub(super) fn tracking(input: &CredentialQuotaObservation, local_boundary: bool) -> CycleTracking {
    let sample = sample(input);
    CycleTracking {
        pending_observation: None,
        unit: input.unit.clone(),
        reset_behavior: input.reset_behavior,
        models: Default::default(),
        needs_rebuild: !matches!(input.scope, gproxy_core::QuotaScope::Unknown),
        rebuild_after: None,
        scope: input.scope.clone(),
        sample,
        baseline_at_ms: sample.received_at_ms,
        baseline_percent: percent(
            input.used_percent,
            input.upstream_used,
            input.upstream_limit,
        ),
        baseline_limit: input.upstream_limit,
        uncertain: false,
        local_boundary,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn spark_walk_is_a_slide_not_a_rollover() {
        let start = 1_788_951_321;
        let window = 18_000;
        assert!(super::sliding(
            Some(start),
            Some(start + window),
            Some(start + 329),
            Some(start + window + 329),
        ));
        assert!(!super::sliding(
            Some(start),
            Some(start + window),
            Some(start + window),
            Some(start + window + window),
        ));
    }

    #[test]
    fn expanding_end_is_not_a_slide() {
        assert!(!super::sliding(Some(0), Some(100), Some(0), Some(500),));
    }

    #[test]
    fn five_minute_window_slides_until_rollover() {
        let start = 1_000;
        let window = 300;
        assert!(super::sliding(
            Some(start),
            Some(start + window),
            Some(start + 30),
            Some(start + window + 30),
        ));
        assert!(!super::sliding(
            Some(start),
            Some(start + window),
            Some(start + window),
            Some(start + window + window),
        ));
    }
}
