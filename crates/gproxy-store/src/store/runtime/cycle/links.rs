use crate::query::runtime;
use crate::records::UsageRecord;
use crate::{Store, StoreError};

impl Store {
    /// Discover rebuilds independently of open windows and recent traffic.
    /// The caller advances `after` even if a particular rebuild fails.
    pub async fn pending_credential_quota_rebuilds(
        &self,
        credential: Option<i64>,
        after: i64,
    ) -> Result<Vec<i64>, StoreError> {
        self.backend()
            .execute(runtime::pending_credential_quota_rebuilds(
                credential, after,
            )?)
            .await?
            .rows
            .iter()
            .map(|row| row.i64("id"))
            .collect()
    }

    pub async fn repair_credential_quota_cycle(&self, cycle_id: i64) -> Result<(), StoreError> {
        self.rebuild_cycle(cycle_id).await
    }

    pub async fn begin_credential_usage(
        &self,
        request: &str,
        credential: i64,
        model: &str,
        at_ms: i64,
    ) -> Result<(), StoreError> {
        self.begin_credential_attempt(request, request, credential, model, at_ms)
            .await
    }

    pub async fn begin_credential_attempt(
        &self,
        request: &str,
        parent: &str,
        credential: i64,
        model: &str,
        at_ms: i64,
    ) -> Result<(), StoreError> {
        self.backend()
            .execute(runtime::begin_usage(
                request, parent, credential, model, at_ms,
            )?)
            .await?;
        Ok(())
    }
    pub async fn finish_credential_attempt(
        &self,
        request: &str,
        credential: i64,
        at_ms: i64,
    ) -> Result<(), StoreError> {
        self.backend()
            .execute(runtime::finish_usage(request, credential, at_ms)?)
            .await?;
        Ok(())
    }

    pub async fn finish_credential_usage(&self, parent: &str) -> Result<(), StoreError> {
        self.backend()
            .execute(runtime::finish_usage_request(parent)?)
            .await?;
        Ok(())
    }

    pub(super) async fn repair_cycle_links(
        &self,
        credential: i64,
        window: &str,
    ) -> Result<(), StoreError> {
        let cycles = self
            .backend()
            .execute(runtime::select_credential_quota_cycle_history_limited(
                credential,
                window,
                Some(runtime::REPAIR_HISTORY),
            )?)
            .await?
            .rows;
        for row in cycles {
            let cycle = super::row::parse(row)?;
            if cycle.tracking.needs_rebuild {
                self.rebuild_cycle(cycle.id).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn attribute_usage(&self, record: &UsageRecord) -> Result<(), StoreError> {
        let at = record
            .usage
            .upstream_started_at_ms
            .ok_or_else(|| StoreError::InvalidData {
                field: "upstream_started_at_ms",
                message: "usage attribution requires the actual upstream send time".into(),
            })?;
        for _ in 0..8 {
            let cycles = self
                .backend()
                .execute(runtime::cycles_for_usage(record.usage.credential_id, at)?)
                .await?
                .rows;
            let mut windows = std::collections::BTreeSet::new();
            let mut complete = true;
            for row in cycles {
                let mut cycle = super::row::parse(row)?;
                let tracking = &cycle.tracking;
                if !tracking.scope.includes(&record.usage.upstream_model)
                    || !windows.insert(cycle.window_key.clone())
                {
                    continue;
                }
                if tracking.needs_rebuild {
                    self.rebuild_cycle(cycle.id).await?;
                    complete = false;
                    continue;
                }
                if !self
                    .backend()
                    .execute(runtime::read_cycle_usage_link(cycle.id, record.id)?)
                    .await?
                    .rows
                    .is_empty()
                {
                    continue;
                }
                let lock = runtime::lock_cycle(&cycle)?;
                let link = runtime::link_cycle_usage(&cycle, record.id)?;
                let expected = cycle.version;
                super::accounting::accumulate(&mut cycle, &record.usage)?;
                cycle.version += 1;
                let results = self
                    .backend()
                    .batch(vec![
                        lock,
                        link,
                        runtime::update_cycle_after_link(&cycle, expected)?,
                    ])
                    .await?;
                complete &= results[2].affected_rows == 1;
            }
            if complete {
                return Ok(());
            }
        }
        Err(StoreError::Database(
            "usage attribution remained contended".into(),
        ))
    }

    pub async fn repair_credential_quota(
        &self,
        credential: i64,
        now: i64,
    ) -> Result<(), StoreError> {
        for id in self
            .pending_credential_quota_rebuilds(Some(credential), 0)
            .await?
        {
            self.rebuild_cycle(id).await?;
        }
        let opens = self
            .unclosed_credential_quota_cycles(Some(credential))
            .await?;
        let mut windows = std::collections::BTreeSet::new();
        for cycle in &opens {
            windows.insert(cycle.window_key.clone());
            if cycle.accounting_end_ms.is_some_and(|end| end <= now * 1000) {
                let end = cycle.accounting_end_ms.expect("checked end") / 1000;
                self.close_credential_quota_cycle(
                    cycle.id,
                    crate::records::QuotaCycleCloseReason::BoundaryCrossed,
                    end,
                )
                .await?;
            }
        }
        for window in windows {
            self.repair_cycle_links(credential, &window).await?;
            let cycles = self
                .backend()
                .execute(runtime::select_credential_quota_cycle_history_limited(
                    credential,
                    &window,
                    Some(runtime::REPAIR_HISTORY),
                )?)
                .await?
                .rows;
            for row in cycles {
                let cycle = super::row::parse(row)?;
                if cycle.tracking.scope == gproxy_core::QuotaScope::Unknown
                    || !cycle.tracking.needs_rebuild
                        && cycle.status != crate::records::QuotaCycleStatus::Open
                {
                    continue;
                }
                let mut after = 0;
                loop {
                    let rows = self
                        .backend()
                        .execute(runtime::cycle_usage_rows(&cycle, after, Some(true))?)
                        .await?
                        .rows;
                    if rows.is_empty() {
                        break;
                    }
                    for row in rows {
                        let record = crate::store::usage::parse_usage(row)?;
                        after = record.id;
                        self.attribute_usage(&record).await?;
                    }
                }
            }
        }
        Ok(())
    }
}
