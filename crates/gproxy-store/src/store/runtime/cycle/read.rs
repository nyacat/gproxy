use rust_decimal::Decimal;

use super::{boundary, row};
use crate::query::runtime;
use crate::records::{CredentialQuotaCycleRecord, CredentialQuotaPressure};
use crate::{Store, StoreError};

impl Store {
    pub async fn credential_quota_window_states(
        &self,
        credential: Option<i64>,
        now: i64,
    ) -> Result<Vec<crate::records::CredentialQuotaWindowState>, StoreError> {
        self.backend()
            .execute(runtime::quota_window_states(credential)?)
            .await?
            .rows
            .into_iter()
            .map(row::window)
            .filter_map(|result| match result {
                Ok(window) if window.reset_at().is_some_and(|end| end <= now) => None,
                result => Some(result),
            })
            .collect()
    }
    pub async fn credential_quota_statistics(
        &self,
        query: &crate::records::CredentialQuotaCycleQuery,
    ) -> Result<Vec<crate::records::CredentialQuotaCycleStatistics>, StoreError> {
        self.credential_quota_statistics_with_options(query, &Default::default())
            .await
    }

    pub async fn credential_quota_statistics_with_options(
        &self,
        query: &crate::records::CredentialQuotaCycleQuery,
        options: &crate::records::CredentialQuotaStatisticsOptions,
    ) -> Result<Vec<crate::records::CredentialQuotaCycleStatistics>, StoreError> {
        if options.cycle_ids.as_ref().is_some_and(Vec::is_empty) {
            return Ok(Vec::new());
        }
        let cycles = self
            .backend()
            .execute(runtime::select_quota_statistics_cycles(
                query.credential_id,
                query.provider_id,
                query.from,
                query.to,
                options,
            )?)
            .await?
            .rows
            .into_iter()
            .map(row::parse)
            .collect::<Result<Vec<_>, _>>()?;
        self.cycle_statistics_selected(
            cycles,
            query.calculate,
            query.history,
            options.observation_range_ms,
        )
        .await
    }

    pub async fn has_recent_quota_observation(
        &self,
        credential: i64,
        after: i64,
        before: i64,
    ) -> Result<bool, StoreError> {
        Ok(!self
            .backend()
            .execute(runtime::recent_quota_observation(
                credential, after, before,
            )?)
            .await?
            .rows
            .is_empty())
    }

    /// Read window state without loading observations or estimating historical usage.
    pub async fn credential_quota_windows(
        &self,
        credential: Option<i64>,
        now: i64,
    ) -> Result<Vec<CredentialQuotaCycleRecord>, StoreError> {
        self.query_open_credential_quota_cycles(credential, now)
            .await
    }
    pub async fn credential_quota_cycles(
        &self,
        credential_id: Option<i64>,
        from: i64,
        to: i64,
    ) -> Result<Vec<CredentialQuotaCycleRecord>, StoreError> {
        let cycles = self
            .backend()
            .execute(runtime::select_credential_quota_cycles(
                credential_id,
                from,
                to,
            )?)
            .await?
            .rows
            .into_iter()
            .map(row::parse)
            .collect::<Result<Vec<_>, _>>()?;
        self.with_models(cycles).await
    }

    pub async fn open_credential_quota_cycles(
        &self,
        credential_id: i64,
        now: i64,
    ) -> Result<Vec<CredentialQuotaCycleRecord>, StoreError> {
        let cycles = self
            .query_open_credential_quota_cycles(Some(credential_id), now)
            .await?;
        self.with_models(cycles).await
    }

    /// Maintenance must see expired rows that have not been closed yet.
    pub async fn unclosed_credential_quota_cycles(
        &self,
        credential_id: Option<i64>,
    ) -> Result<Vec<CredentialQuotaCycleRecord>, StoreError> {
        self.backend()
            .execute(runtime::select_open_credential_quota_cycles(credential_id)?)
            .await?
            .rows
            .into_iter()
            .map(row::parse)
            .collect()
    }

    pub async fn credential_quota_cycle_history(
        &self,
        credential_id: i64,
        window_key: &str,
    ) -> Result<Vec<CredentialQuotaCycleRecord>, StoreError> {
        let cycles = self
            .backend()
            .execute(runtime::select_credential_quota_cycle_history(
                credential_id,
                window_key,
            )?)
            .await?
            .rows
            .into_iter()
            .map(row::parse)
            .collect::<Result<Vec<_>, _>>()?;
        self.with_models(cycles).await
    }

    pub async fn credential_quota_pressures(
        &self,
        now: i64,
    ) -> Result<Vec<CredentialQuotaPressure>, StoreError> {
        Ok(self
            .credential_quota_window_states(None, now)
            .await?
            .into_iter()
            .filter_map(|cycle| {
                cycle
                    .pressure()
                    .map(|used_percent| CredentialQuotaPressure {
                        cycle_id: cycle.id,
                        credential_id: cycle.credential_id,
                        window_key: cycle.window_key.clone(),
                        version: cycle.version,
                        last_observed_at: cycle.last_observed_at,
                        used_percent,
                        period_end: cycle.reset_at(),
                    })
            })
            .collect())
    }

    pub async fn credential_quota_pressure(
        &self,
        credential_id: i64,
        now: i64,
    ) -> Result<Option<Decimal>, StoreError> {
        Ok(self
            .credential_quota_window_states(Some(credential_id), now)
            .await?
            .iter()
            .filter_map(|window| window.pressure())
            .max())
    }

    async fn query_open_credential_quota_cycles(
        &self,
        credential_id: Option<i64>,
        now: i64,
    ) -> Result<Vec<CredentialQuotaCycleRecord>, StoreError> {
        let cycles = self
            .backend()
            .execute(runtime::select_open_credential_quota_cycles(credential_id)?)
            .await?
            .rows
            .into_iter()
            .map(row::parse)
            .filter_map(|result| match result {
                Ok(cycle) if boundary::trusted_reset(&cycle).is_some_and(|reset| reset <= now) => {
                    None
                }
                result => Some(result),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(cycles)
    }

    pub(super) async fn open_credential_quota_cycle(
        &self,
        credential_id: i64,
        window_key: &str,
    ) -> Result<Option<CredentialQuotaCycleRecord>, StoreError> {
        let result = self
            .backend()
            .execute(runtime::read_open_credential_quota_cycle(
                credential_id,
                window_key,
            )?)
            .await?;
        let cycle = result.rows.into_iter().next().map(row::parse).transpose()?;
        Ok(cycle)
    }

    pub(super) async fn credential_quota_cycle(
        &self,
        id: i64,
    ) -> Result<Option<CredentialQuotaCycleRecord>, StoreError> {
        let result = self
            .backend()
            .execute(runtime::read_credential_quota_cycle(id)?)
            .await?;
        let cycle = result.rows.into_iter().next().map(row::parse).transpose()?;
        Ok(cycle)
    }

    pub(super) async fn latest_credential_quota_cycle(
        &self,
        credential_id: i64,
        window_key: &str,
    ) -> Result<Option<CredentialQuotaCycleRecord>, StoreError> {
        let result = self
            .backend()
            .execute(runtime::read_latest_credential_quota_cycle(
                credential_id,
                window_key,
            )?)
            .await?;
        let cycle = result.rows.into_iter().next().map(row::parse).transpose()?;
        Ok(cycle)
    }
}
