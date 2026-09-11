use crate::records::CredentialQuotaCycleRecord;
use crate::{Store, StoreError};

impl Store {
    pub(super) async fn with_models(
        &self,
        cycles: Vec<CredentialQuotaCycleRecord>,
    ) -> Result<Vec<CredentialQuotaCycleRecord>, StoreError> {
        Ok(self
            .cycle_statistics(cycles, true, false)
            .await?
            .into_iter()
            .map(|value| value.cycle)
            .collect())
    }

    pub(super) async fn cycle_statistics(
        &self,
        cycles: Vec<CredentialQuotaCycleRecord>,
        calculate: bool,
        history: bool,
    ) -> Result<Vec<crate::records::CredentialQuotaCycleStatistics>, StoreError> {
        self.cycle_statistics_selected(cycles, calculate, history, None)
            .await
    }

    pub(super) async fn cycle_statistics_selected(
        &self,
        cycles: Vec<CredentialQuotaCycleRecord>,
        calculate: bool,
        history: bool,
        range: Option<(i64, i64)>,
    ) -> Result<Vec<crate::records::CredentialQuotaCycleStatistics>, StoreError> {
        let mut samples = if calculate || history {
            self.load_selected_cycle_observations(&cycles, calculate, history, range)
                .await?
        } else {
            tracing::debug!(
                cycles = cycles.len(),
                calculate,
                history,
                query_count = 0,
                observation_rows = 0,
                usage_rows = 0,
                pending_rows = 0,
                observation_ms = 0,
                usage_ms = 0,
                calculation_ms = 0,
                elapsed_ms = 0,
                "quota.statistics.loaded"
            );
            Default::default()
        };
        Ok(cycles
            .into_iter()
            .map(|mut cycle| {
                let mut observations = samples.remove(&cycle.id).unwrap_or_default();
                super::metrics::hydrate(&mut cycle, calculate, &observations);
                if let Some((from, to)) = range {
                    observations.retain(|sample| {
                        sample.observed_at_ms >= from && sample.observed_at_ms < to
                    });
                }
                crate::records::CredentialQuotaCycleStatistics {
                    cycle,
                    observations: if history { observations } else { Vec::new() },
                }
            })
            .collect())
    }
}

#[cfg(test)]
impl Store {
    /// Reference the previous per-cycle reads for parity and performance tests.
    pub(crate) async fn legacy_cycle_statistics(
        &self,
    ) -> Result<Vec<crate::records::CredentialQuotaCycleStatistics>, StoreError> {
        use crate::query::runtime;
        let mut values = Vec::new();
        for mut cycle in self.unclosed_credential_quota_cycles(None).await? {
            let mut samples = self
                .backend()
                .execute(runtime::cycle_observations(&cycle)?)
                .await?
                .rows
                .into_iter()
                .map(|row| {
                    serde_json::from_str::<crate::records::CycleObservationRecord>(
                        row.text("snapshot_json")?,
                    )
                    .map_err(|error| StoreError::Database(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            samples.retain(|sample| !sample.rejected);
            let ranges = [(
                cycle.accounting_start_ms,
                cycle.tracking.sample.received_at_ms,
            )];
            let pending = self
                .backend()
                .execute(runtime::estimate_pending(cycle.credential_id, &ranges)?)
                .await?
                .rows
                .into_iter()
                .map(|row| Ok((row.i64("started_at_ms")?, row.text("model")?.to_owned())))
                .collect::<Result<Vec<_>, StoreError>>()?;
            let mut after = 0;
            let mut usages = Vec::new();
            loop {
                let rows = self
                    .backend()
                    .execute(runtime::cycle_usage_rows(&cycle, after, None)?)
                    .await?
                    .rows;
                if rows.is_empty() {
                    break;
                }
                for row in rows {
                    after = row.i64("id")?;
                    usages.push(super::estimation::UsageSample::parse(&row)?);
                }
            }
            usages.sort_by_key(|usage| usage.at);
            super::estimation::calculate(&cycle, &mut samples, &usages, &pending)?;
            super::metrics::hydrate(&mut cycle, true, &samples);
            values.push(crate::records::CredentialQuotaCycleStatistics {
                cycle,
                observations: samples,
            });
        }
        Ok(values)
    }
}
