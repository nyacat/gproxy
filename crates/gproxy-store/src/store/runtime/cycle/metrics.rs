use crate::records::{
    CredentialQuotaCycleModelRecord, CredentialQuotaCycleRecord, CycleEstimate,
    CycleObservationRecord, UsageTotals,
};
use rust_decimal::Decimal;
use serde_json::Value;

pub(super) fn hydrate(
    cycle: &mut CredentialQuotaCycleRecord,
    calculate: bool,
    samples: &[CycleObservationRecord],
) {
    let tracking = &cycle.tracking;
    if tracking.scope == gproxy_core::QuotaScope::Unknown {
        cycle.metrics = serde_json::json!({});
        cycle.models.clear();
        cycle.estimate =
            calculate.then(|| unavailable("unknown_scope", &CycleObservationRecord::from(&*cycle)));
        return;
    }
    cycle.models = tracking
        .models
        .iter()
        .map(|(model, metrics)| CredentialQuotaCycleModelRecord {
            model: model.clone(),
            metrics: metrics.clone(),
        })
        .collect();
    cycle.estimate = samples.last().and_then(|sample| sample.estimate.clone());
}

pub(super) fn calculate(
    sample: &CycleObservationRecord,
    delta: &UsageTotals,
    incomplete: bool,
) -> CycleEstimate {
    calculate_totals(
        sample,
        delta.requests,
        delta.total_tokens(),
        delta.cost,
        incomplete,
    )
}

pub(super) fn calculate_totals(
    sample: &CycleObservationRecord,
    requests: u64,
    tokens: Decimal,
    cost: Decimal,
    incomplete: bool,
) -> CycleEstimate {
    let current = super::state::percent(
        sample.used_percent,
        sample.upstream_used,
        sample.upstream_limit,
    );
    let growth = current
        .zip(sample.baseline_percent)
        .map(|(current, baseline)| current - baseline);
    if sample.scope == gproxy_core::QuotaScope::Unknown {
        unavailable("unknown_scope", sample)
    } else if sample.uncertain {
        unavailable("unordered_observations", sample)
    } else if incomplete {
        unavailable("incomplete_usage", sample)
    } else if requests == 0 || growth.is_none_or(|growth| growth < Decimal::ONE) {
        unavailable("insufficient_samples", sample)
    } else {
        let factor = Decimal::ONE_HUNDRED / growth.expect("positive growth");
        match (tokens.checked_mul(factor), cost.checked_mul(factor)) {
            (Some(tokens), Some(cost)) => CycleEstimate {
                tokens: Some(tokens),
                cost: Some(cost),
                reason: None,
                from_ms: Some(sample.baseline_at_ms),
                to_ms: Some(sample.observed_at_ms),
            },
            _ => unavailable("estimate_overflow", sample),
        }
    }
}

fn unavailable(reason: &str, sample: &CycleObservationRecord) -> CycleEstimate {
    CycleEstimate {
        tokens: None,
        cost: None,
        reason: Some(reason.into()),
        from_ms: Some(sample.baseline_at_ms),
        to_ms: Some(sample.observed_at_ms),
    }
}

pub(super) fn metrics(totals: &UsageTotals) -> Value {
    let mut metrics = totals.metrics.clone();
    metrics.extend([
        ("requests".into(), Decimal::from(totals.requests)),
        ("input_tokens".into(), Decimal::from(totals.input_tokens)),
        ("output_tokens".into(), Decimal::from(totals.output_tokens)),
        (
            "cached_input_tokens".into(),
            Decimal::from(totals.cached_input_tokens),
        ),
        ("total_tokens".into(), totals.total_tokens()),
        ("cost".into(), totals.cost),
    ]);
    serde_json::to_value(metrics).expect("decimal metrics serialize")
}
