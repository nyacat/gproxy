use crate::query::usage;
use crate::records::{UsageFilter, UsageRecord, UsageTotals};
use crate::{Store, StoreError};

impl Store {
    pub async fn active_usage_credentials(
        &self,
        since: i64,
    ) -> Result<std::collections::BTreeSet<i64>, StoreError> {
        self.backend()
            .execute(usage::active_credentials(since)?)
            .await?
            .rows
            .into_iter()
            .map(|row| row.i64("credential_id"))
            .collect()
    }
    pub async fn usage_records(
        &self,
        filter: &UsageFilter,
        page: u64,
        page_size: u64,
    ) -> Result<(Vec<UsageRecord>, u64), StoreError> {
        let (records, total, _) = self
            .usage_records_page(filter, page, page_size, true)
            .await?;
        Ok((records, total.expect("requested total")))
    }

    pub async fn usage_records_page(
        &self,
        filter: &UsageFilter,
        page: u64,
        page_size: u64,
        include_total: bool,
    ) -> Result<(Vec<UsageRecord>, Option<u64>, bool), StoreError> {
        let offset = page
            .checked_sub(1)
            .and_then(|page| page.checked_mul(page_size))
            .filter(|_| matches!(page_size, 10 | 20 | 50 | 100))
            .ok_or_else(|| StoreError::InvalidData {
                field: "pagination",
                message: "invalid page or page size".into(),
            })?;
        let statement = usage::records(filter, offset, page_size + 1)?;
        let results = if include_total {
            self.backend()
                .batch(vec![statement, usage::count_filtered(filter)?])
                .await?
        } else {
            vec![self.backend().execute(statement).await?]
        };
        let mut results = results.into_iter();
        let rows = results.next().expect("records result").rows;
        let has_more = rows.len() > page_size as usize;
        let rows = rows
            .into_iter()
            .take(page_size as usize)
            .map(super::usage::parse_usage)
            .collect::<Result<Vec<_>, _>>()?;
        let count = if include_total {
            Some(
                results
                    .next()
                    .expect("count result")
                    .rows
                    .pop()
                    .expect("count row")
                    .i64("count")? as u64,
            )
        } else {
            None
        };
        Ok((rows, count, has_more))
    }

    pub async fn usage_summary(&self, filter: &UsageFilter) -> Result<UsageTotals, StoreError> {
        const PAGE_SIZE: u64 = 5_000;
        let mut totals = UsageTotals::default();
        let mut after = None;
        loop {
            let rows = self
                .backend()
                .execute(usage::summary_rows(filter, after, PAGE_SIZE + 1)?)
                .await?
                .rows;
            // A lookahead row avoids a final empty query for exact full pages.
            // It is included in the next page and accumulated only once.
            let has_more = rows.len() > PAGE_SIZE as usize;
            for row in rows.into_iter().take(PAGE_SIZE as usize) {
                after = Some((row.i64("at")?, row.i64("id")?));
                let tokens = |field| {
                    u64::try_from(row.i64(field)?).map_err(|_| StoreError::InvalidData {
                        field,
                        message: "expected nonnegative integer".into(),
                    })
                };
                let cost = row
                    .text("cost")?
                    .parse()
                    .map_err(|error| StoreError::InvalidData {
                        field: "cost",
                        message: format!("{error}"),
                    })?;
                let (metrics, _) = super::usage::read_metrics(&row)?;
                let metrics =
                    serde_json::from_value(metrics).map_err(|error| StoreError::InvalidData {
                        field: "metrics_json",
                        message: error.to_string(),
                    })?;
                totals.add_values(
                    tokens("input_tokens")?,
                    tokens("output_tokens")?,
                    tokens("cached_input_tokens")?,
                    cost,
                    metrics,
                )?;
            }
            if !has_more {
                break;
            }
        }
        Ok(totals)
    }
}
