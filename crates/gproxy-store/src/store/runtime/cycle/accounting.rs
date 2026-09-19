use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::BTreeMap;

use crate::query::runtime;
use crate::records::{CredentialQuotaCycleRecord, UsageInput, UsageTotals};
use crate::{Store, StoreError};

pub(super) fn increment(metrics: &mut Value, usage: &UsageInput) -> Result<(), StoreError> {
    let mut totals = UsageTotals::default();
    totals.add(usage)?;
    let delta = super::metrics::metrics(&totals);
    let mut values: BTreeMap<String, Decimal> = serde_json::from_value(metrics.clone())
        .map_err(|error| StoreError::Database(error.to_string()))?;
    let delta: BTreeMap<String, Decimal> =
        serde_json::from_value(delta).map_err(|error| StoreError::Database(error.to_string()))?;
    for (name, amount) in delta {
        *values.entry(name).or_default() += amount;
    }
    *metrics = serde_json::to_value(values).expect("decimal metrics serialize");
    Ok(())
}

impl Store {
    pub(super) async fn rebuild_cycle(&self, id: i64) -> Result<(), StoreError> {
        let mut conflicts = 0;
        while conflicts < 8 {
            let Some(mut cycle) = self.credential_quota_cycle(id).await? else {
                return Ok(());
            };
            if !cycle.tracking.needs_rebuild {
                return Ok(());
            }
            let expected = cycle.version;
            let mut batch = vec![runtime::lock_cycle(&cycle)?];
            if cycle.tracking.scope == gproxy_core::QuotaScope::Unknown {
                cycle.tracking.needs_rebuild = false;
                cycle.tracking.rebuild_after = None;
                cycle.metrics = serde_json::json!({});
                cycle.tracking.models.clear();
            } else {
                let after = cycle.tracking.rebuild_after.unwrap_or(0);
                if cycle.tracking.rebuild_after.is_none() {
                    cycle.tracking.models.clear();
                    cycle.metrics = super::metrics::metrics(&UsageTotals::default());
                }
                let rows = self
                    .backend()
                    .execute(runtime::cycle_usage_rows(&cycle, after, None)?)
                    .await?
                    .rows;
                if rows.is_empty() {
                    cycle.tracking.needs_rebuild = false;
                    cycle.tracking.rebuild_after = None;
                } else {
                    for row in rows {
                        let record = crate::store::usage::parse_usage(row)?;
                        cycle.tracking.rebuild_after = Some(record.id);
                        if cycle.tracking.scope.includes(&record.usage.upstream_model) {
                            accumulate(&mut cycle, &record.usage)?;
                            batch.push(runtime::link_cycle_usage(&cycle, record.id)?);
                        }
                    }
                }
            }
            cycle.version += 1;
            batch.push(runtime::update_tracked_cycle(&cycle, expected)?);
            // Lock the cycle before linking, then commit the links, totals and
            // cursor together. A losing CAS must never leave uncounted links.
            let results = self.backend().batch(batch).await?;
            if results.last().expect("cycle update").affected_rows == 1 {
                conflicts = 0;
                if !cycle.tracking.needs_rebuild {
                    return Ok(());
                }
            } else {
                conflicts += 1;
            }
        }
        Err(StoreError::Database(
            "cycle rebuild remained contended".into(),
        ))
    }
}

pub(super) fn accumulate(
    cycle: &mut CredentialQuotaCycleRecord,
    usage: &UsageInput,
) -> Result<(), StoreError> {
    increment(&mut cycle.metrics, usage)?;
    let model = cycle
        .tracking
        .models
        .entry(usage.upstream_model.clone())
        .or_insert_with(|| serde_json::json!({}));
    increment(model, usage)
}
