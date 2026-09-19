use std::sync::{Arc, Mutex};

use rust_decimal::Decimal;

use crate::Store;
use crate::backend::{DbFuture, Executor, QueryResult, SharedExecutor, Statement};
use crate::records::UsageFilter;

struct Counting {
    inner: SharedExecutor,
    statements: Mutex<Vec<Statement>>,
}

impl Executor for Counting {
    fn execute<'a>(&'a self, statement: Statement) -> DbFuture<'a, QueryResult> {
        self.statements.lock().unwrap().push(statement.clone());
        self.inner.execute(statement)
    }
    fn batch<'a>(&'a self, statements: Vec<Statement>) -> DbFuture<'a, Vec<QueryResult>> {
        self.statements.lock().unwrap().extend(statements.clone());
        self.inner.batch(statements)
    }
}

async fn seed(store: &Store, old: i64) {
    let last = old + 5001;
    store
        .backend()
        .execute(Statement::plain(format!(
            r#"
        WITH RECURSIVE samples(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM samples WHERE n < {last})
        INSERT INTO usage_rows (request_id, at, provider_id, credential_id, upstream_model,
            input_tokens, output_tokens, cached_input_tokens, metrics_json, dimensions_json,
            cost, usage_source, ended, latency_ms)
        SELECT 'record-' || n, CASE WHEN n <= {old} THEN n / 10 ELSE 200000 + ({last} - n) / 2 END,
            1, 1, 'model', 3, 2, 1, '{{"cache_creation_5m_tokens":"0.25","audio_seconds":0.125}}',
            '{{"detail":"{}"}}', '0.00000001', 'upstream', 'complete', 1 FROM samples
    "#,
            "x".repeat(512)
        )))
        .await
        .unwrap();
}

async fn exercise(store: Store) {
    let executor = Arc::new(Counting {
        inner: store.executor,
        statements: Default::default(),
    });
    let store = Store {
        executor: executor.clone(),
        dialect: store.dialect,
        quota_window_locks: store.quota_window_locks,
    };
    let filter = UsageFilter {
        from: 200000,
        to: 203000,
        ..Default::default()
    };
    let (first, total, more) = store
        .usage_records_page(&filter, 1, 10, false)
        .await
        .unwrap();
    assert_eq!(
        executor.statements.lock().unwrap().len(),
        1,
        "a records page must not count the entire range"
    );
    assert_eq!(first.len(), 10);
    assert_eq!(total, None);
    assert!(more);
    assert!(
        first
            .windows(2)
            .all(|pair| (pair[0].usage.at, pair[0].id) > (pair[1].usage.at, pair[1].id))
    );
    let (next, _, _) = store
        .usage_records_page(&filter, 2, 10, false)
        .await
        .unwrap();
    assert!(
        first
            .iter()
            .all(|row| next.iter().all(|other| row.id != other.id))
    );
    let (last, _, more) = store
        .usage_records_page(&filter, 501, 10, false)
        .await
        .unwrap();
    assert_eq!(last.len(), 1);
    assert!(!more);
    let (empty, _, more) = store
        .usage_records_page(&filter, 502, 10, false)
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert!(!more);
    let (legacy, total) = store.usage_records(&filter, 1, 10).await.unwrap();
    assert_eq!(legacy, first);
    assert_eq!(total, 5001);
    executor.statements.lock().unwrap().clear();
    let summary = store.usage_summary(&filter).await.unwrap();
    assert_eq!(executor.statements.lock().unwrap().len(), 2);
    assert_eq!(
        summary.requests, 5001,
        "equal timestamps and reverse id/time ordering must not skip or double count rows"
    );
    assert_eq!(summary.cost, Decimal::new(5001, 8));
    assert_eq!(
        summary.total_tokens(),
        Decimal::from(5001) * Decimal::new(525, 2)
    );
    assert_eq!(
        summary.metrics["audio_seconds"],
        Decimal::from(5001) * Decimal::new(125, 3)
    );
    let exact = UsageFilter {
        request_id: Some(first[0].usage.request_id.clone()),
        ..filter
    };
    let (rows, total, more) = store.usage_records_page(&exact, 1, 10, true).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(total, Some(1));
    assert!(!more);
}

#[tokio::test]
async fn usage_pages_and_summaries_preserve_filters_precision_and_timestamp_ties() {
    let directory = tempfile::tempdir().unwrap();
    let (store, _) = super::native_store(directory.path().join("records.db"))
        .await
        .unwrap();
    seed(&store, 1000).await;
    exercise(store).await;
}

#[tokio::test]
#[ignore = "requires an isolated PostgreSQL database via GPROXY_TEST_POSTGRES_DSN"]
async fn postgres_usage_pages_and_summary_index_work() {
    use crate::backend::DbValue;
    use crate::schema::Dialect;
    let dsn = std::env::var("GPROXY_TEST_POSTGRES_DSN").unwrap();
    let store = Store::open(crate::BackendConfig::Postgres {
        dsn: dsn.clone(),
        pool_size: 2,
        checkout_timeout: std::time::Duration::from_secs(5),
    })
    .await
    .unwrap();
    seed(&store, 100_000).await;
    store
        .backend()
        .execute(Statement::plain("VACUUM ANALYZE usage_rows"))
        .await
        .unwrap();
    let filter = UsageFilter {
        from: 200000,
        to: 203000,
        ..Default::default()
    };
    let summary = crate::query::usage::summary_rows(&filter, None, 5001).unwrap();
    let continuation =
        crate::query::usage::summary_rows(&filter, Some((202499, 100003)), 5001).unwrap();
    let page = crate::query::usage::records(&filter, 5000, 11).unwrap();
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    for (kind, statement) in [
        ("summary", summary),
        ("summary_continuation", continuation),
        ("page", page),
    ] {
        let mut sql = statement.sql_for(Dialect::Postgres).to_owned();
        for (i, value) in statement.args.iter().enumerate().rev() {
            let DbValue::Integer(value) = value else {
                panic!("integer fixture arguments only")
            };
            sql = sql.replace(&format!("${}", i + 1), &value.to_string());
        }
        let messages = client
            .simple_query(&format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"))
            .await
            .unwrap();
        let row = messages
            .iter()
            .find_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .unwrap();
        let plan: serde_json::Value = serde_json::from_str(row.get(0).unwrap()).unwrap();
        fn check(node: &serde_json::Value, wide: &mut f64) {
            assert_ne!(node["Node Type"], "Seq Scan", "{node}");
            assert_eq!(node["Temp Written Blocks"].as_u64().unwrap_or(0), 0);
            if node["Relation Name"] == "usage_rows" && node["Node Type"] == "Index Scan" {
                *wide += (node["Actual Rows"].as_f64().unwrap_or(0.0)
                    + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0))
                    * node["Actual Loops"].as_f64().unwrap_or(1.0);
            }
            if let Some(children) = node["Plans"].as_array() {
                for child in children {
                    check(child, wide)
                }
            }
        }
        let mut wide = 0.0;
        check(&plan[0]["Plan"], &mut wide);
        assert!(
            wide <= match kind {
                "page" => 11.0,
                "summary_continuation" => 1.0,
                _ => 5001.0,
            },
            "{kind}: wide tuple reads {wide}: {plan}"
        );
        println!(
            "usage_index_work {}",
            serde_json::json!({"kind":kind,"wide_rows":wide,"plan":plan})
        );
    }
    drop(client);
    connection.await.unwrap().unwrap();
    exercise(store).await;
}
