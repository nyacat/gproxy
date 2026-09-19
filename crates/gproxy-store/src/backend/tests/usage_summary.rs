use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rust_decimal::Decimal;

use super::super::{DbFuture, Executor, QueryResult, SharedExecutor, Statement};
use crate::Store;
use crate::records::UsageFilter;

struct CountingExecutor {
    inner: SharedExecutor,
    queries: AtomicUsize,
}

impl Executor for CountingExecutor {
    fn execute<'a>(&'a self, statement: Statement) -> DbFuture<'a, QueryResult> {
        self.queries.fetch_add(1, Ordering::Relaxed);
        self.inner.execute(statement)
    }

    fn batch<'a>(&'a self, statements: Vec<Statement>) -> DbFuture<'a, Vec<QueryResult>> {
        self.inner.batch(statements)
    }
}

#[tokio::test]
async fn usage_summary_page_boundaries_preserve_decimal_metrics_without_empty_queries() {
    let directory = tempfile::tempdir().unwrap();
    let (store, database) = super::native_store(directory.path().join("summary.db"))
        .await
        .unwrap();
    database
        .execute(Statement::plain(
            r#"WITH RECURSIVE samples(n) AS (
                SELECT 1 UNION ALL SELECT n + 1 FROM samples WHERE n < 5001
            )
            INSERT INTO usage_rows (
                request_id, at, provider_id, credential_id, upstream_model,
                input_tokens, output_tokens, cached_input_tokens,
                metrics_json, dimensions_json, cost, usage_source, ended, latency_ms
            ) SELECT
                'summary-' || n, n, 1, 1, 'summary-model', 3, 2, 1,
                '{"cache_creation_5m_tokens":"0.25","audio_seconds":0.125}',
                '{}', '0.00000001', 'upstream', 'complete', 1
            FROM samples"#,
        ))
        .await
        .unwrap();
    let executor = Arc::new(CountingExecutor {
        inner: store.executor,
        queries: AtomicUsize::new(0),
    });
    let store = Store {
        executor: executor.clone(),
        dialect: store.dialect,
        quota_window_locks: store.quota_window_locks,
    };
    for (count, queries) in [(0, 1), (4_999, 1), (5_000, 1), (5_001, 2)] {
        executor.queries.store(0, Ordering::Relaxed);
        let summary = store
            .usage_summary(&UsageFilter {
                from: 1,
                to: count + 1,
                model: Some("summary-model".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(executor.queries.load(Ordering::Relaxed), queries);
        assert_eq!(summary.requests, count as u64);
        assert_eq!(summary.input_tokens, count as u64 * 3);
        assert_eq!(summary.output_tokens, count as u64 * 2);
        assert_eq!(summary.cached_input_tokens, count as u64);
        assert_eq!(summary.cost, Decimal::from(count) * Decimal::new(1, 8));
        assert_eq!(
            summary.total_tokens(),
            Decimal::from(count) * Decimal::new(525, 2)
        );
        if count != 0 {
            assert_eq!(
                summary.metrics["audio_seconds"],
                Decimal::from(count) * Decimal::new(125, 3)
            );
            assert_eq!(
                summary.metrics["cache_creation_5m_tokens"],
                Decimal::from(count) * Decimal::new(25, 2)
            );
        }
    }
}
