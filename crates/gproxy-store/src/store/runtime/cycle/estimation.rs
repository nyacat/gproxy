use crate::StoreError;
use crate::records::{CredentialQuotaCycleRecord, CycleObservationRecord, UsageTotals};
use rust_decimal::Decimal;
use std::collections::BTreeMap;

pub(super) struct UsageSample {
    pub at: i64,
    pub model: String,
    pub input: u64,
    pub output: u64,
    pub cached: u64,
    pub cost: Decimal,
    pub metrics: BTreeMap<String, Decimal>,
    pub session: bool,
}

impl UsageSample {
    pub(super) fn parse(row: &crate::backend::Row) -> Result<Self, StoreError> {
        let json = |field| {
            serde_json::from_str::<serde_json::Value>(row.text(field)?)
                .map_err(|error| StoreError::Database(error.to_string()))
        };
        let dimensions = json("dimensions_json")?;
        Ok(Self {
            at: row.i64("upstream_started_at_ms")?,
            model: row.text("upstream_model")?.into(),
            input: crate::store::usage::unsigned(row.i64("input_tokens")?, "input_tokens")?,
            output: crate::store::usage::unsigned(row.i64("output_tokens")?, "output_tokens")?,
            cached: crate::store::usage::unsigned(
                row.i64("cached_input_tokens")?,
                "cached_input_tokens",
            )?,
            cost: row
                .text("cost")?
                .parse()
                .map_err(|error: rust_decimal::Error| StoreError::Database(error.to_string()))?,
            metrics: serde_json::from_value(json("metrics_json")?)
                .map_err(|error| StoreError::Database(error.to_string()))?,
            session: dimensions
                .get("usage_incomplete")
                .and_then(serde_json::Value::as_str)
                == Some("true")
                || dimensions
                    .get("quota_attribution")
                    .and_then(serde_json::Value::as_str)
                    == Some("session"),
        })
    }
}

#[derive(Clone, Copy, Default)]
struct Prefix {
    at: i64,
    requests: u64,
    tokens: Decimal,
    cost: Decimal,
    sessions: u64,
}

/// One prefix index per credential and read operation. Counts are independent
/// of quota windows, so overlapping windows no longer traverse usage again.
pub(super) struct UsageIndex(BTreeMap<String, Vec<Prefix>>);

/// Index uncertain usage by model as well as time; a long-lived gap must not
/// turn every observation into another linear walk over all missing attempts.
#[derive(Default)]
pub(super) struct PendingIndex(BTreeMap<String, Vec<i64>>);

impl PendingIndex {
    pub(super) fn new(pending: &[(i64, String)]) -> Self {
        let mut models = BTreeMap::<String, Vec<i64>>::new();
        for (at, model) in pending {
            models.entry(model.clone()).or_default().push(*at);
        }
        for times in models.values_mut() {
            times.sort_unstable();
            times.dedup();
        }
        Self(models)
    }

    fn overlaps(&self, from: i64, to: i64, scope: &gproxy_core::QuotaScope) -> bool {
        from < to
            && self.0.iter().any(|(model, times)| {
                scope.includes(model)
                    && times
                        .get(times.partition_point(|at| *at < from))
                        .is_some_and(|at| *at < to)
            })
    }
}

impl UsageIndex {
    pub(super) fn new(usages: &[UsageSample]) -> Result<Self, StoreError> {
        let mut models = BTreeMap::<String, (UsageTotals, Vec<Prefix>)>::new();
        let mut global = UsageTotals::default();
        for usage in usages {
            // Extreme sums or decimal rescaling must retain the legacy
            // per-window behavior, not lose small values during subtraction.
            let before = global.cost;
            global.add_values(
                usage.input,
                usage.output,
                usage.cached,
                usage.cost,
                usage.metrics.clone(),
            )?;
            if global.cost.checked_sub(before) != Some(usage.cost) {
                return Err(StoreError::Database(
                    "quota estimate prefix loses decimal precision".into(),
                ));
            }
            let (totals, values) = models.entry(usage.model.clone()).or_default();
            totals.add_values(
                usage.input,
                usage.output,
                usage.cached,
                usage.cost,
                usage.metrics.clone(),
            )?;
            let sessions = values.last().map_or(0, |p| p.sessions) + u64::from(usage.session);
            let prefix = Prefix {
                at: usage.at,
                requests: totals.requests,
                tokens: totals.total_tokens(),
                cost: totals.cost,
                sessions,
            };
            if values.last().is_some_and(|last| last.at == usage.at) {
                *values.last_mut().expect("last bucket") = prefix;
            } else {
                values.push(prefix);
            }
        }
        Ok(Self(
            models
                .into_iter()
                .map(|(model, (_, values))| (model, values))
                .collect(),
        ))
    }

    fn totals(
        &self,
        from: i64,
        to: i64,
        scope: &gproxy_core::QuotaScope,
    ) -> Result<Prefix, StoreError> {
        let mut total = Prefix::default();
        if from >= to {
            return Ok(total);
        }
        let overflow = || StoreError::Database("quota estimate prefix overflow".into());
        for (model, values) in &self.0 {
            if !scope.includes(model) {
                continue;
            }
            let before = |at| {
                values
                    .partition_point(|value| value.at < at)
                    .checked_sub(1)
                    .map(|i| values[i])
                    .unwrap_or_default()
            };
            let left = before(from);
            let right = before(to);
            total.requests = total
                .requests
                .checked_add(right.requests - left.requests)
                .ok_or_else(overflow)?;
            total.sessions = total
                .sessions
                .checked_add(right.sessions - left.sessions)
                .ok_or_else(overflow)?;
            total.tokens = total
                .tokens
                .checked_add(right.tokens.checked_sub(left.tokens).ok_or_else(overflow)?)
                .ok_or_else(overflow)?;
            total.cost = total
                .cost
                .checked_add(right.cost.checked_sub(left.cost).ok_or_else(overflow)?)
                .ok_or_else(overflow)?;
        }
        Ok(total)
    }

    pub(super) fn calculate(
        &self,
        cycle: &CredentialQuotaCycleRecord,
        samples: &mut [CycleObservationRecord],
        pending: &PendingIndex,
    ) -> Result<(), StoreError> {
        let mut previous: Option<CycleObservationRecord> = None;
        let mut last_valid = None;
        for sample in samples {
            if previous.as_ref().is_none_or(|left| {
                left.baseline_at_ms != sample.baseline_at_ms
                    || left.scope != sample.scope
                    || left.upstream_limit != sample.upstream_limit
                    || left.unit != sample.unit
            }) {
                last_valid = None;
            }
            let from = cycle.accounting_start_ms.max(sample.baseline_at_ms);
            let total = self.totals(
                from,
                sample
                    .observed_at_ms
                    .min(cycle.accounting_end_ms.unwrap_or(i64::MAX)),
                &sample.scope,
            )?;
            let incomplete = cycle.tracking.needs_rebuild
                || total.sessions > 0
                || pending.overlaps(from, sample.observed_at_ms, &sample.scope);
            let mut estimate = super::metrics::calculate_totals(
                sample,
                total.requests,
                total.tokens,
                total.cost,
                incomplete,
            );
            if estimate.reason.is_none() {
                last_valid = Some(estimate.clone());
            } else if let Some(valid) = &last_valid {
                let reason = estimate.reason;
                estimate = valid.clone();
                estimate.reason = reason;
            }
            sample.estimate = Some(estimate);
            previous = Some(sample.clone());
        }
        Ok(())
    }
}

pub(super) fn calculate(
    cycle: &CredentialQuotaCycleRecord,
    samples: &mut [CycleObservationRecord],
    usages: &[UsageSample],
    pending: &[(i64, String)],
) -> Result<(), StoreError> {
    let ordered = usages
        .iter()
        .filter(|usage| {
            usage.at >= cycle.accounting_start_ms
                && cycle.accounting_end_ms.is_none_or(|end| usage.at < end)
        })
        .collect::<Vec<_>>();
    let mut total = UsageTotals::default();
    let mut session_attribution = false;
    let mut cursor = 0;
    let mut previous: Option<CycleObservationRecord> = None;
    let mut last_valid = None;
    for sample in samples {
        if previous.as_ref().is_none_or(|left| {
            left.baseline_at_ms != sample.baseline_at_ms
                || left.scope != sample.scope
                || left.upstream_limit != sample.upstream_limit
                || left.unit != sample.unit
        }) {
            total = UsageTotals::default();
            session_attribution = false;
            last_valid = None;
            cursor = ordered.partition_point(|usage| usage.at < sample.baseline_at_ms);
        }
        while let Some(usage) = ordered.get(cursor) {
            if usage.at >= sample.observed_at_ms {
                break;
            }
            cursor += 1;
            if !sample.scope.includes(&usage.model) {
                continue;
            }
            total.add_values(
                usage.input,
                usage.output,
                usage.cached,
                usage.cost,
                usage.metrics.clone(),
            )?;
            session_attribution |= usage.session;
        }
        let incomplete = cycle.tracking.needs_rebuild
            || session_attribution
            || pending.iter().any(|(at, model)| {
                *at >= sample.baseline_at_ms
                    && *at >= cycle.accounting_start_ms
                    && *at < sample.observed_at_ms
                    && sample.scope.includes(model)
            });
        let mut estimate = super::metrics::calculate(sample, &total, incomplete);
        if estimate.reason.is_none() {
            last_valid = Some(estimate.clone());
        } else if let Some(valid) = &last_valid {
            // Keep the original sample time and provenance when newer usage is
            // missing. A missing row is never treated as zero consumption.
            let reason = estimate.reason;
            estimate = valid.clone();
            estimate.reason = reason;
        }
        sample.estimate = Some(estimate);
        previous = Some(sample.clone());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample(at: i64, cost: Decimal, cache: Decimal) -> UsageSample {
        UsageSample {
            at,
            model: "model".into(),
            input: 3,
            output: 2,
            cached: 1,
            cost,
            metrics: BTreeMap::from([("cache_creation_5m_tokens".into(), cache)]),
            session: false,
        }
    }
    #[test]
    fn index_retains_duplicate_timestamps_cache_tokens_and_exact_cost() {
        let usages = [
            sample(1, Decimal::new(1, 8), Decimal::new(25, 2)),
            sample(1, Decimal::new(2, 8), Decimal::new(75, 2)),
            sample(2, Decimal::new(3, 8), Decimal::ONE),
        ];
        let index = UsageIndex::new(&usages).unwrap();
        let total = index.totals(1, 2, &gproxy_core::QuotaScope::All).unwrap();
        assert_eq!(total.requests, 2);
        assert_eq!(total.tokens, Decimal::from(11));
        assert_eq!(total.cost, Decimal::new(3, 8));
        let last = index.totals(2, 3, &gproxy_core::QuotaScope::All).unwrap();
        assert_eq!(last.tokens, Decimal::from(6));
        assert_eq!(last.cost, Decimal::new(3, 8));
    }
    #[test]
    fn index_declines_extreme_sums_instead_of_rounding_small_windows_to_zero() {
        let usages = [
            sample(1, Decimal::MAX, Decimal::ZERO),
            sample(2, Decimal::new(1, 8), Decimal::ZERO),
        ];
        assert!(UsageIndex::new(&usages).is_err());
        let mut usages = [
            sample(1, Decimal::ZERO, Decimal::ZERO),
            sample(2, Decimal::ZERO, Decimal::ZERO),
        ];
        usages[0].input = u64::MAX;
        usages[1].model = "other-model".into();
        assert!(UsageIndex::new(&usages).is_err());
    }
}
