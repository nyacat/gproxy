use std::collections::{BTreeMap, BTreeSet};

use super::estimation::UsageSample;
use crate::query::runtime;
use crate::records::{CredentialQuotaCycleRecord, CycleObservationRecord};
use crate::{Store, StoreError};

type Samples = BTreeMap<i64, Vec<CycleObservationRecord>>;

#[derive(Default)]
struct ReadMetrics {
    query_count: usize,
    fallback_rounds: usize,
    calculated_rows: usize,
    observation_rows: usize,
    usage_rows: usize,
    pending_rows: usize,
    observation_ms: u64,
    usage_ms: u64,
    calculation_ms: u64,
}

#[derive(Default)]
struct CredentialUsage {
    ranges: Vec<(i64, i64)>,
    usages: Vec<UsageSample>,
    pending: Vec<(i64, String)>,
    index: Option<super::estimation::UsageIndex>,
    pending_index: super::estimation::PendingIndex,
}

impl Store {
    pub async fn credential_quota_observations(
        &self,
        cycle: &CredentialQuotaCycleRecord,
        calculate: bool,
    ) -> Result<Vec<CycleObservationRecord>, StoreError> {
        Ok(self
            .load_cycle_observations(std::slice::from_ref(cycle), calculate)
            .await?
            .remove(&cycle.id)
            .unwrap_or_default())
    }

    pub(super) async fn load_cycle_observations(
        &self,
        cycles: &[CredentialQuotaCycleRecord],
        calculate: bool,
    ) -> Result<Samples, StoreError> {
        self.load_selected_cycle_observations(cycles, calculate, true, None)
            .await
    }

    pub(super) async fn load_selected_cycle_observations(
        &self,
        cycles: &[CredentialQuotaCycleRecord],
        calculate: bool,
        history: bool,
        range: Option<(i64, i64)>,
    ) -> Result<Samples, StoreError> {
        let started = web_time::Instant::now();
        let mut metrics = ReadMetrics::default();
        let mut samples = Samples::new();
        let mut usage = BTreeMap::<i64, CredentialUsage>::new();
        let reads = cycles
            .iter()
            .map(|cycle| runtime::ObservationSlice {
                cycle: cycle.id,
                from: if history {
                    range.map_or(i64::MIN, |r| r.0)
                } else {
                    i64::MIN
                },
                to: cycle
                    .tracking
                    .sample
                    .received_at_ms
                    .saturating_add(1)
                    .min(range.map_or(i64::MAX, |r| r.1)),
                before: None,
            })
            .collect::<Vec<_>>();
        let mut cursors = self
            .read_observation_slices(&reads, (!history).then_some(1), &mut samples, &mut metrics)
            .await?;
        if calculate && history && range.is_some() {
            // The visible range does not change an estimate's original baseline
            // or the most recent valid fallback before its first visible point.
            let context = cycles
                .iter()
                .filter_map(|cycle| {
                    let first = samples.get(&cycle.id)?.first()?;
                    (first.baseline_at_ms < range.expect("bounded history").0).then_some(
                        runtime::ObservationSlice {
                            cycle: cycle.id,
                            from: first.baseline_at_ms,
                            to: first.observed_at_ms.saturating_add(1),
                            before: Some((first.observed_at_ms, first.started_at_ms)),
                        },
                    )
                })
                .collect::<Vec<_>>();
            self.read_observation_slices(&context, None, &mut samples, &mut metrics)
                .await?;
        }
        if calculate {
            self.calculate_observations(cycles, &mut samples, &mut usage, &mut metrics)
                .await?;
        }
        if calculate && !history {
            // Keep only the latest result. Each older page is evaluated once,
            // sharing the usage snapshot but never sorting or recalculating the
            // prefix of observations already visited by another round.
            let mut exhausted = BTreeSet::new();
            let mut page_size = 32;
            loop {
                let reads = cycles
                    .iter()
                    .filter_map(|cycle| {
                        if exhausted.contains(&cycle.id) || cycle.tracking.needs_rebuild {
                            return None;
                        }
                        let cursor = *cursors.get(&cycle.id)?;
                        let latest = samples.get(&cycle.id).and_then(|v| v.last());
                        if latest.and_then(|s| s.estimate.as_ref()).is_some_and(|e| {
                            e.tokens.is_some() || e.reason.as_deref() == Some("unknown_scope")
                        }) {
                            return None;
                        }
                        Some(runtime::ObservationSlice {
                            cycle: cycle.id,
                            from: latest.map_or(i64::MIN, |s| s.baseline_at_ms),
                            to: cycle.tracking.sample.received_at_ms.saturating_add(1),
                            before: Some(cursor),
                        })
                    })
                    .collect::<Vec<_>>();
                if reads.is_empty() {
                    break;
                }
                let mut page = Samples::new();
                let next = self
                    .read_observation_slices(&reads, Some(page_size), &mut page, &mut metrics)
                    .await?;
                metrics.fallback_rounds += 1;
                for read in &reads {
                    if let Some(cursor) = next.get(&read.cycle) {
                        cursors.insert(read.cycle, *cursor);
                    } else {
                        exhausted.insert(read.cycle);
                    }
                    if let Some(values) = page.get_mut(&read.cycle)
                        && let Some(latest) = samples.get(&read.cycle).and_then(|v| v.last())
                        && let Some(boundary) =
                            values.iter().rposition(|v| !same_baseline(v, latest))
                    {
                        // A fallback cannot cross a scope/limit/baseline change.
                        values.drain(..=boundary);
                        exhausted.insert(read.cycle);
                    }
                }
                self.calculate_observations(cycles, &mut page, &mut usage, &mut metrics)
                    .await?;
                for (id, values) in page {
                    if let Some(latest) = samples.get_mut(&id).and_then(|v| v.last_mut()) {
                        if let Some(valid) = values
                            .iter()
                            .rev()
                            .filter_map(|s| s.estimate.as_ref())
                            .find(|e| e.reason.is_none())
                        {
                            let reason = latest.estimate.as_ref().and_then(|e| e.reason.clone());
                            latest.estimate = Some(valid.clone());
                            latest.estimate.as_mut().expect("fallback").reason = reason;
                        }
                    } else if let Some(latest) = values.last() {
                        samples.insert(id, vec![latest.clone()]);
                    }
                }
                // Grow I/O batches for long gaps; this bounds memory, never the
                // searched history. Every observation remains eligible.
                page_size = (page_size * 2).min(1024);
            }
        }
        let elapsed_ms = started.elapsed().as_millis() as u64;
        macro_rules! log_read {
            ($level:ident) => {
                tracing::$level!(
                    cycles = cycles.len(),
                    calculate,
                    history,
                    query_count = metrics.query_count,
                    fallback_rounds = metrics.fallback_rounds,
                    calculated_rows = metrics.calculated_rows,
                    observation_rows = metrics.observation_rows,
                    usage_rows = metrics.usage_rows,
                    pending_rows = metrics.pending_rows,
                    observation_ms = metrics.observation_ms,
                    usage_ms = metrics.usage_ms,
                    calculation_ms = metrics.calculation_ms,
                    elapsed_ms,
                    "quota.statistics.loaded"
                )
            };
        }
        if elapsed_ms >= 200 {
            log_read!(warn);
        } else {
            log_read!(debug);
        }

        Ok(samples)
    }

    async fn read_observation_slices(
        &self,
        reads: &[runtime::ObservationSlice],
        limit: Option<u64>,
        samples: &mut Samples,
        metrics: &mut ReadMetrics,
    ) -> Result<BTreeMap<i64, (i64, i64)>, StoreError> {
        let started = web_time::Instant::now();
        let mut cursors = BTreeMap::new();
        for reads in reads.chunks(runtime::OBSERVATION_BATCH_SIZE) {
            let rows = self
                .backend()
                .execute(runtime::observation_slice(reads, limit)?)
                .await?
                .rows;
            metrics.query_count += 1;
            metrics.observation_rows += rows.len();
            for row in rows {
                let id = row.i64("cycle_id")?;
                let cursor = (row.i64("observed_at_ms")?, row.i64("started_at_ms")?);
                cursors
                    .entry(id)
                    .and_modify(|current: &mut (i64, i64)| *current = (*current).min(cursor))
                    .or_insert(cursor);
                let sample: CycleObservationRecord =
                    serde_json::from_str(row.text("snapshot_json")?)
                        .map_err(|e| StoreError::Database(e.to_string()))?;
                if !sample.rejected {
                    samples.entry(id).or_default().push(sample);
                }
            }
        }
        for samples in samples.values_mut() {
            samples.sort_by_key(|s| (s.observed_at_ms, s.started_at_ms));
            samples.dedup_by_key(|s| (s.observed_at_ms, s.started_at_ms));
        }
        metrics.observation_ms += started.elapsed().as_millis() as u64;
        Ok(cursors)
    }

    async fn calculate_observations(
        &self,
        cycles: &[CredentialQuotaCycleRecord],
        samples: &mut Samples,
        cached: &mut BTreeMap<i64, CredentialUsage>,
        metrics: &mut ReadMetrics,
    ) -> Result<(), StoreError> {
        let mut credentials = BTreeMap::<i64, Vec<&CredentialQuotaCycleRecord>>::new();
        for cycle in cycles {
            if let Some(values) = samples.get_mut(&cycle.id)
                && (cycle.tracking.needs_rebuild
                    || values
                        .iter()
                        .all(|s| s.scope == gproxy_core::QuotaScope::Unknown))
            {
                // These states cannot yield a valid estimate, independent of
                // any usage row. Preserve the reason without reading usage.
                for sample in values {
                    sample.estimate = Some(super::metrics::calculate_totals(
                        sample,
                        0,
                        Default::default(),
                        Default::default(),
                        cycle.tracking.needs_rebuild,
                    ));
                    metrics.calculated_rows += 1;
                }
                continue;
            }
            if samples.get(&cycle.id).is_some_and(|v| !v.is_empty()) {
                credentials
                    .entry(cycle.credential_id)
                    .or_default()
                    .push(cycle);
            }
        }
        for (credential, cycles) in credentials {
            let started = web_time::Instant::now();
            let ranges = merge_ranges(
                cycles
                    .iter()
                    .flat_map(|cycle| {
                        samples[&cycle.id].iter().map(|sample| {
                            (
                                cycle.accounting_start_ms.max(sample.baseline_at_ms),
                                sample.observed_at_ms,
                            )
                        })
                    })
                    .filter(|(from, to)| from < to)
                    .collect(),
            );
            let cache = cached.entry(credential).or_default();
            let missing = subtract_ranges(&ranges, &cache.ranges);
            for ranges in missing.chunks(runtime::OBSERVATION_BATCH_SIZE) {
                // Pending first prevents a concurrent settlement from looking
                // like zero consumption. Reuse this conservative snapshot.
                let rows = self
                    .backend()
                    .execute(runtime::estimate_pending(credential, ranges)?)
                    .await?
                    .rows;
                metrics.query_count += 1;
                metrics.pending_rows += rows.len();
                for row in rows {
                    cache
                        .pending
                        .push((row.i64("started_at_ms")?, row.text("model")?.to_owned()));
                }
                let mut after = None;
                loop {
                    let rows = self
                        .backend()
                        .execute(runtime::estimate_usage(credential, ranges, after)?)
                        .await?
                        .rows;
                    metrics.query_count += 1;
                    let more = rows.len() > runtime::ESTIMATE_PAGE_SIZE;
                    for row in rows.into_iter().take(runtime::ESTIMATE_PAGE_SIZE) {
                        after = Some((row.i64("upstream_started_at_ms")?, row.i64("id")?));
                        cache.usages.push(UsageSample::parse(&row)?);
                        metrics.usage_rows += 1;
                    }
                    if !more {
                        break;
                    }
                }
            }
            if !missing.is_empty() {
                cache.ranges = merge_ranges(cache.ranges.iter().copied().chain(ranges).collect());
                cache.usages.sort_by_key(|usage| usage.at);
                cache.index = super::estimation::UsageIndex::new(&cache.usages).ok();
                cache.pending_index = super::estimation::PendingIndex::new(&cache.pending);
            }
            metrics.usage_ms += started.elapsed().as_millis() as u64;
            let started = web_time::Instant::now();
            for cycle in cycles {
                let samples = samples.get_mut(&cycle.id).expect("observations");
                metrics.calculated_rows += samples.len();
                if cache.index.as_ref().is_none_or(|index| {
                    index
                        .calculate(cycle, samples, &cache.pending_index)
                        .is_err()
                }) {
                    // Preserve the original per-window overflow semantics if a
                    // prefix over the entire query would exceed its number range.
                    super::estimation::calculate(cycle, samples, &cache.usages, &cache.pending)?;
                }
            }
            metrics.calculation_ms += started.elapsed().as_millis() as u64;
        }
        Ok(())
    }
}

fn same_baseline(left: &CycleObservationRecord, right: &CycleObservationRecord) -> bool {
    left.baseline_at_ms == right.baseline_at_ms
        && left.scope == right.scope
        && left.upstream_limit == right.upstream_limit
        && left.unit == right.unit
}

fn merge_ranges(mut ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    ranges.sort_unstable();
    let mut merged: Vec<(i64, i64)> = Vec::new();
    for (from, to) in ranges {
        if let Some(last) = merged.last_mut()
            && from <= last.1
        {
            last.1 = last.1.max(to);
        } else {
            merged.push((from, to));
        }
    }
    merged
}

fn subtract_ranges(ranges: &[(i64, i64)], covered: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut output = Vec::new();
    for &(from, to) in ranges {
        let mut cursor = from;
        for &(left, right) in covered {
            if right <= cursor || left >= to {
                continue;
            }
            if cursor < left {
                output.push((cursor, left));
            }
            cursor = cursor.max(right);
            if cursor >= to {
                break;
            }
        }
        if cursor < to {
            output.push((cursor, to));
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn range_difference_only_loads_new_intervals() {
        assert_eq!(
            subtract_ranges(&[(0, 10), (15, 25)], &[(2, 5), (8, 20)]),
            vec![(0, 2), (5, 8), (20, 25)]
        );
        assert!(subtract_ranges(&[(2, 5)], &[(0, 10)]).is_empty());
    }
}
