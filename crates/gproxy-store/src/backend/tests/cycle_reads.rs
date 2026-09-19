use std::sync::{Arc, Mutex};

use crate::backend::{DbFuture, Executor, QueryResult, SharedExecutor, Statement};
use crate::records::{
    CredentialQuotaCycleQuery, CredentialQuotaObservation, QuotaBoundaryConfidence,
    QuotaBoundarySource,
};
use crate::{Store, StoreError};

struct Counting {
    inner: SharedExecutor,
    queries: Mutex<Vec<String>>,
    rows: Mutex<Vec<(String, usize)>>,
}

impl Executor for Counting {
    fn execute<'a>(&'a self, statement: Statement) -> DbFuture<'a, QueryResult> {
        self.queries.lock().unwrap().push(statement.sql.clone());
        Box::pin(async move {
            let sql = statement.sql.clone();
            let result = self.inner.execute(statement).await?;
            self.rows.lock().unwrap().push((sql, result.rows.len()));
            Ok(result)
        })
    }
    fn batch<'a>(&'a self, statements: Vec<Statement>) -> DbFuture<'a, Vec<QueryResult>> {
        self.queries
            .lock()
            .unwrap()
            .extend(statements.iter().map(|s| s.sql.clone()));
        self.inner.batch(statements)
    }
}

fn counted(store: Store) -> (Store, Arc<Counting>) {
    let executor = Arc::new(Counting {
        inner: store.executor,
        queries: Mutex::new(Vec::new()),
        rows: Mutex::new(Vec::new()),
    });
    (
        Store {
            executor: executor.clone(),
            dialect: store.dialect,
            quota_window_locks: store.quota_window_locks,
        },
        executor,
    )
}

fn observation(window: &str, at: i64, percent: i64) -> CredentialQuotaObservation {
    CredentialQuotaObservation {
        credential_id: 1,
        window_key: window.into(),
        label: None,
        period_start: Some(0),
        period_end: Some(1_000),
        observed_at: at,
        boundary_source: QuotaBoundarySource::Upstream,
        boundary_confidence: QuotaBoundaryConfidence::Exact,
        sample: gproxy_core::QuotaSample {
            source: gproxy_core::QuotaSampleSource::Unknown,
            started_at_ms: at * 1_000,
            received_at_ms: at * 1_000,
        },
        scope: gproxy_core::QuotaScope::All,
        reset_behavior: gproxy_core::QuotaResetBehavior::Periodic,
        unit: None,
        upstream_used: None,
        upstream_limit: None,
        used_percent: Some(percent.into()),
    }
}

async fn seed(store: &Store, cycles: usize, usages: usize) {
    for n in 0..cycles {
        for (at, percent) in [(1, 1), (100, 2), (200, 3), (300, 4), (500, 5)] {
            store
                .observe_credential_quota_cycle(&observation(&format!("window-{n}"), at, percent))
                .await
                .unwrap();
        }
    }
    if usages > 0 {
        store.backend().execute(Statement::plain(format!(r#"
            WITH RECURSIVE samples(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM samples WHERE n < {usages})
            INSERT INTO usage_rows (request_id, at, upstream_started_at_ms, provider_id, credential_id,
                upstream_model, input_tokens, output_tokens, cached_input_tokens, metrics_json, dimensions_json,
                cost, usage_source, ended, latency_ms)
            SELECT 'quota-read-' || n, 3, 2000, 1, 1, 'model', 3, 2, 1, '{{"audio_seconds":"0.125"}}',
                '{{}}', '0.00000001', 'upstream', 'complete', 1 FROM samples
        "#))).await.unwrap();
    }
}

async fn exercise(store: Store, count: usize) {
    seed(&store, 100, count).await;
    let (store, counter) = counted(store);
    for history in [false, true] {
        counter.queries.lock().unwrap().clear();
        let values = store
            .credential_quota_statistics(&CredentialQuotaCycleQuery {
                credential_id: Some(1),
                provider_id: None,
                from: 0,
                to: 1_000,
                calculate: true,
                history,
            })
            .await
            .unwrap();
        assert_eq!(values.len(), 100);
        let expected_pages = count.div_ceil(1_024).max(1);
        let queries = counter.queries.lock().unwrap();
        assert!(
            queries.len() <= 5 + expected_pages,
            "bounded fallback reads must not turn into per-cycle queries: {}",
            queries.len()
        );
        assert_eq!(
            queries
                .iter()
                .filter(|sql| sql.contains("\"usage_rows\"")
                    && !sql.contains("credential_quota_activity"))
                .count(),
            expected_pages,
            "fallback estimation must reuse the request's usage snapshot"
        );
        assert!(
            queries.len() * 5 < 100 * (3 + count / 256),
            "at least 80% fewer queries than per-cycle hydration"
        );
        drop(queries);
        for value in &values {
            let estimate = value.cycle.estimate.as_ref().unwrap();
            if count > 0 {
                assert_eq!(
                    estimate.tokens,
                    Some(rust_decimal::Decimal::from(count as u64 * 125))
                );
                assert_eq!(
                    estimate.cost,
                    Some(rust_decimal::Decimal::new(count as i64 * 25, 8))
                );
            }
            if history {
                assert_eq!(value.observations.len(), 5);
                assert_eq!(
                    value.observations.last().unwrap().estimate.as_ref(),
                    Some(estimate)
                );
            } else {
                assert!(value.observations.is_empty());
            }
        }
    }
    counter.queries.lock().unwrap().clear();
    assert_eq!(
        store.credential_quota_pressures(501).await.unwrap().len(),
        100
    );
    assert_eq!(
        store
            .credential_quota_window_states(Some(1), 501)
            .await
            .unwrap()
            .len(),
        100
    );
    assert!(
        store
            .has_recent_quota_observation(1, 499, 501)
            .await
            .unwrap()
    );
    assert!(
        !store
            .has_recent_quota_observation(1, 500, 501)
            .await
            .unwrap()
    );
    let queries = counter.queries.lock().unwrap();
    assert_eq!(queries.len(), 4);
    assert!(queries.iter().all(|sql| !sql.contains("usage_rows")
        && !sql.contains("credential_quota_observations")
        && !sql.contains("tracking_json")
        && !sql.contains("metrics_json")));
}

#[tokio::test]
async fn cycle_statistics_share_reads_and_handle_equal_timestamp_page_boundaries() {
    for (remote, rows) in [(false, 0), (false, 1_024), (true, 1_025), (false, 10_000)] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cycle-reads.db");
        let (store, _) = if remote {
            super::libsql_store(path).await.unwrap()
        } else {
            super::native_store(path).await.unwrap()
        };
        exercise(store, rows).await;
    }
}

#[tokio::test]
#[ignore = "requires an empty PostgreSQL database via GPROXY_TEST_POSTGRES_DSN"]
async fn postgres_cycle_statistics_share_reads() -> Result<(), StoreError> {
    let store = Store::open(crate::BackendConfig::Postgres {
        dsn: std::env::var("GPROXY_TEST_POSTGRES_DSN").unwrap(),
        pool_size: 8,
        checkout_timeout: std::time::Duration::from_secs(5),
    })
    .await?;
    exercise(store, 10_000).await;
    Ok(())
}

#[tokio::test]
#[ignore = "performance comparison requires an empty PostgreSQL database"]
async fn postgres_cycle_statistics_benchmark() {
    let store = Store::open(crate::BackendConfig::Postgres {
        dsn: std::env::var("GPROXY_TEST_POSTGRES_DSN").unwrap(),
        pool_size: 8,
        checkout_timeout: std::time::Duration::from_secs(5),
    })
    .await
    .unwrap();
    seed(&store, 100, 10_000).await;
    let (store, counter) = counted(store);
    let mut before = Vec::new();
    let mut after = Vec::new();
    for _ in 0..5 {
        counter.queries.lock().unwrap().clear();
        let started = web_time::Instant::now();
        let mut reference = store.legacy_cycle_statistics().await.unwrap();
        before.push(started.elapsed().as_millis());
        let old_queries = counter.queries.lock().unwrap().len();
        counter.queries.lock().unwrap().clear();
        let started = web_time::Instant::now();
        let mut current = store
            .credential_quota_statistics(&CredentialQuotaCycleQuery {
                credential_id: None,
                provider_id: None,
                from: 0,
                to: 1_000,
                calculate: true,
                history: true,
            })
            .await
            .unwrap();
        after.push(started.elapsed().as_millis());
        let new_queries = counter.queries.lock().unwrap().len();
        reference.sort_by_key(|value| value.cycle.id);
        current.sort_by_key(|value| value.cycle.id);
        for (old, new) in reference.iter().zip(&current) {
            assert_eq!(old.cycle, new.cycle);
            assert_eq!(old.observations, new.observations);
        }
        assert_eq!(reference.len(), current.len());
        assert!(new_queries * 5 <= old_queries);
        println!(
            "quota benchmark: before_queries={old_queries}, after_queries={new_queries}, before_ms={}, after_ms={}",
            before.last().unwrap(),
            after.last().unwrap()
        );
    }
    before.sort_unstable();
    after.sort_unstable();
    // With five runs the nearest-rank p95 is the maximum. This is an opt-in
    // benchmark on the same isolated database, not a timing-sensitive CI test.
    println!(
        "quota benchmark p95: before_ms={}, after_ms={}",
        before[4], after[4]
    );
    assert!(after[4] <= before[4]);
}

#[tokio::test]
async fn bounded_statistics_preserve_fallbacks_and_skip_unrequested_reads() {
    use crate::records::CredentialQuotaStatisticsOptions;
    let directory = tempfile::tempdir().unwrap();
    let (store, _) = super::native_store(directory.path().join("bounded.db"))
        .await
        .unwrap();
    seed(&store, 3, 1025).await;
    // A later session cannot be attributed exactly: keep the valid estimate
    // from before it, including its original sample time.
    store.backend().execute(Statement::plain(r#"INSERT INTO usage_rows
        (request_id, at, upstream_started_at_ms, provider_id, credential_id, upstream_model,
         input_tokens, output_tokens, cached_input_tokens, metrics_json, dimensions_json, cost, usage_source, ended, latency_ms)
        VALUES ('session', 351, 350000, 1, 1, 'model', 3, 2, 1, '{}', '{"quota_attribution":"session"}', '0.00001', 'upstream', 'complete', 1)"#)).await.unwrap();
    let mut reference = store.legacy_cycle_statistics().await.unwrap();
    reference.sort_by_key(|value| value.cycle.id);
    let id = reference[0].cycle.id;
    let (store, counter) = counted(store);
    let base = CredentialQuotaCycleQuery {
        credential_id: Some(1),
        provider_id: None,
        from: 0,
        to: 1000,
        calculate: false,
        history: false,
    };
    let read = CredentialQuotaStatisticsOptions {
        cycle_ids: Some(vec![id]),
        current_only: false,
        observation_range_ms: Some((400000, 600000)),
    };
    let lightweight = store
        .credential_quota_statistics_with_options(&base, &read)
        .await
        .unwrap();
    assert_eq!(lightweight.len(), 1);
    assert!(lightweight[0].cycle.estimate.is_none());
    assert!(counter.queries.lock().unwrap().iter().all(|sql| {
        !sql.contains("credential_quota_observations")
            && !sql.contains("usage_rows")
            && !sql.contains("credential_quota_activity")
    }));
    for (history, from, to) in [(false, 0, 1000), (true, 400, 600), (true, 100, 301)] {
        counter.queries.lock().unwrap().clear();
        let current = store
            .credential_quota_statistics_with_options(
                &CredentialQuotaCycleQuery {
                    from,
                    to,
                    calculate: true,
                    history,
                    ..base
                },
                &CredentialQuotaStatisticsOptions {
                    observation_range_ms: Some((from * 1000, to * 1000)),
                    ..read.clone()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            current.len(),
            1,
            "cycle must remain visible when it has newer observations beyond the range"
        );
        if !history || to == 600 {
            assert_eq!(current[0].cycle.estimate, reference[0].cycle.estimate);
            assert_eq!(
                current[0].cycle.estimate.as_ref().unwrap().to_ms,
                Some(300000)
            );
        }
        if history {
            let expected: Vec<_> = reference[0]
                .observations
                .iter()
                .filter(|s| s.observed_at_ms >= from * 1000 && s.observed_at_ms < to * 1000)
                .cloned()
                .collect();
            assert_eq!(current[0].observations, expected);
        } else {
            assert!(current[0].observations.is_empty());
        }
        let queries = counter.queries.lock().unwrap();
        assert_eq!(
            queries
                .iter()
                .filter(|sql| sql.contains("\"usage_rows\"")
                    && !sql.contains("credential_quota_activity"))
                .count(),
            2,
            "backward fallback must not read the usage pages twice"
        );
    }
    counter.queries.lock().unwrap().clear();
    let percent = store
        .credential_quota_statistics_with_options(
            &CredentialQuotaCycleQuery {
                history: true,
                from: 100,
                to: 301,
                ..base
            },
            &CredentialQuotaStatisticsOptions {
                observation_range_ms: Some((100000, 301000)),
                ..read.clone()
            },
        )
        .await
        .unwrap();
    assert_eq!(percent[0].observations.len(), 3);
    assert!(percent[0].observations.iter().all(|s| s.estimate.is_none()));
    assert!(
        counter
            .queries
            .lock()
            .unwrap()
            .iter()
            .all(|sql| !sql.contains("usage_rows") && !sql.contains("credential_quota_activity"))
    );
    counter.queries.lock().unwrap().clear();
    let empty = store
        .credential_quota_statistics_with_options(
            &base,
            &CredentialQuotaStatisticsOptions {
                cycle_ids: Some(vec![]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(empty.is_empty());
    assert!(counter.queries.lock().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "production-sized benchmark requires an empty local PostgreSQL database"]
async fn postgres_production_scale_statistics_benchmark() {
    use crate::records::CredentialQuotaStatisticsOptions;
    let store = Store::open(crate::BackendConfig::Postgres {
        dsn: std::env::var("GPROXY_TEST_POSTGRES_DSN").unwrap(),
        pool_size: 8,
        checkout_timeout: std::time::Duration::from_secs(5),
    })
    .await
    .unwrap();
    seed(&store, 1007, 28700).await;
    store.backend().execute(Statement::plain(r#"
        INSERT INTO credential_quota_observations (cycle_id, observed_at_ms, started_at_ms, snapshot_json)
        SELECT cycle_id, 400000 + n*1000, 400000 + n*1000,
            jsonb_set(jsonb_set(snapshot_json::jsonb, '{observed_at_ms}', to_jsonb(400000 + n*1000)), '{started_at_ms}', to_jsonb(400000 + n*1000))::text
        FROM credential_quota_observations CROSS JOIN generate_series(1,37) AS n WHERE observed_at_ms = 300000
    "#)).await.unwrap();
    let (store, counter) = counted(store);
    let mut measurements = Vec::new();
    for iteration in 0..5 {
        let mut reference = Vec::new();
        for (name, calculate, history, from, ids, current_only) in [
            ("full_history_estimated", true, true, 0, None, false),
            ("full_history_percent", false, true, 0, None, false),
            ("bounded_history_percent", false, true, 450, None, false),
            ("overview", false, false, 0, None, true),
            ("latest_estimated", true, false, 0, None, false),
            ("one_cycle_details", true, false, 0, Some(vec![1]), false),
        ] {
            counter.queries.lock().unwrap().clear();
            counter.rows.lock().unwrap().clear();
            let started = web_time::Instant::now();
            let values = store
                .credential_quota_statistics_with_options(
                    &CredentialQuotaCycleQuery {
                        credential_id: Some(1),
                        provider_id: None,
                        from,
                        to: 600,
                        calculate,
                        history,
                    },
                    &CredentialQuotaStatisticsOptions {
                        cycle_ids: ids,
                        current_only,
                        observation_range_ms: Some((from * 1000, 600000)),
                    },
                )
                .await
                .unwrap();
            let elapsed = started.elapsed().as_secs_f64() * 1000.0;
            let expected = if name == "one_cycle_details" { 1 } else { 1007 };
            assert_eq!(values.len(), expected);
            if name == "full_history_estimated" {
                assert_eq!(
                    values.iter().map(|v| v.observations.len()).sum::<usize>(),
                    42294
                );
                reference = values
                    .iter()
                    .map(|v| (v.cycle.id, v.cycle.estimate.clone()))
                    .collect();
            } else if calculate {
                for value in &values {
                    assert_eq!(
                        value.cycle.estimate,
                        reference
                            .iter()
                            .find(|(id, _)| *id == value.cycle.id)
                            .unwrap()
                            .1
                    );
                }
            }
            let rows = counter.rows.lock().unwrap();
            let observation_rows: usize = rows
                .iter()
                .filter(|(sql, _)| sql.contains("credential_quota_observations"))
                .map(|(_, n)| n)
                .sum();
            let pending_rows: usize = rows
                .iter()
                .filter(|(sql, _)| sql.contains("credential_quota_activity"))
                .map(|(_, n)| n)
                .sum();
            let usage_rows: usize = rows
                .iter()
                .filter(|(sql, _)| {
                    sql.contains("usage_rows") && !sql.contains("credential_quota_activity")
                })
                .map(|(_, n)| n)
                .sum();
            if !calculate {
                assert_eq!(usage_rows + pending_rows, 0);
            }
            if name == "overview" {
                assert_eq!(observation_rows, 0);
            }
            if name == "latest_estimated" {
                assert_eq!(observation_rows, 1007);
            }
            if name == "one_cycle_details" {
                assert_eq!(observation_rows, 1);
            }
            measurements.push(serde_json::json!({"iteration":iteration,"scenario":name,"elapsed_ms":elapsed,"queries":counter.queries.lock().unwrap().len(),"cycle_rows":values.len(),"observation_rows":observation_rows,"usage_rows":usage_rows,"pending_rows":pending_rows}));
        }
    }
    println!(
        "PRODUCTION_SCALE_BENCHMARK={}",
        serde_json::to_string(&measurements).unwrap()
    );
}

async fn long_gap_fixture(store: &Store) {
    use sea_query::{Alias, Query};
    seed(store, 2, 10).await;
    let rows = store.backend().execute(Statement::plain("SELECT cycle_id, snapshot_json FROM credential_quota_observations WHERE observed_at_ms = 300000")).await.unwrap().rows;
    for row in rows {
        let id = row.i64("cycle_id").unwrap();
        let template: crate::records::CycleObservationRecord =
            serde_json::from_str(row.text("snapshot_json").unwrap()).unwrap();
        for chunk in (0..10_000).collect::<Vec<_>>().chunks(100) {
            let mut insert = Query::insert();
            insert
                .into_table(Alias::new("credential_quota_observations"))
                .columns(
                    [
                        "cycle_id",
                        "observed_at_ms",
                        "started_at_ms",
                        "snapshot_json",
                    ]
                    .map(Alias::new),
                );
            for n in chunk {
                let mut sample = template.clone();
                // Duplicate receipt timestamps straddle pages; send time is
                // the tie breaker and must participate in the index cursor.
                sample.observed_at_ms = 300_001 + n / 2;
                sample.started_at_ms = sample.observed_at_ms - n % 2;
                insert.values_panic([
                    id.into(),
                    sample.observed_at_ms.into(),
                    sample.started_at_ms.into(),
                    serde_json::to_string(&sample).unwrap().into(),
                ]);
            }
            store
                .backend()
                .execute(Statement::query(&insert).unwrap())
                .await
                .unwrap();
        }
    }
    store
        .begin_credential_usage("long-gap", 1, "model", 300_000)
        .await
        .unwrap();
    store.finish_credential_usage("long-gap").await.unwrap();
}

async fn long_gap_reads(store: Store) -> Store {
    let (store, counter) = counted(store);
    for no_valid in [false, true] {
        if no_valid {
            store
                .begin_credential_usage("no-valid", 1, "model", 1000)
                .await
                .unwrap();
        }
        let reference = store.legacy_cycle_statistics().await.unwrap();
        counter.queries.lock().unwrap().clear();
        counter.rows.lock().unwrap().clear();
        let started = web_time::Instant::now();
        let values = store
            .credential_quota_statistics(&CredentialQuotaCycleQuery {
                credential_id: Some(1),
                provider_id: None,
                from: 0,
                to: 1000,
                calculate: true,
                history: false,
            })
            .await
            .unwrap();
        for value in &values {
            let expected = reference
                .iter()
                .find(|r| r.cycle.id == value.cycle.id)
                .unwrap();
            assert_eq!(value.cycle.estimate, expected.cycle.estimate);
            let estimate = value.cycle.estimate.as_ref().unwrap();
            assert_eq!(estimate.tokens.is_none(), no_valid);
            if !no_valid {
                assert_eq!(estimate.to_ms, Some(300_000));
            }
            assert_eq!(estimate.reason.as_deref(), Some("incomplete_usage"));
        }
        let rows = counter.rows.lock().unwrap();
        let observations: usize = rows
            .iter()
            .filter(|(q, _)| q.contains("credential_quota_observations"))
            .map(|(_, n)| n)
            .sum();
        let usage: usize = rows
            .iter()
            .filter(|(q, _)| q.contains("usage_rows") && !q.contains("credential_quota_activity"))
            .map(|(_, n)| n)
            .sum();
        assert!(
            observations <= 20_010,
            "every observation is read at most once"
        );
        assert_eq!(usage, 10, "all fallback pages reuse the usage snapshot");
        assert!(
            counter.queries.lock().unwrap().len() <= 20,
            "long gaps must not require hundreds of tiny round trips"
        );
        println!(
            "LONG_GAP_BENCHMARK={}",
            serde_json::json!({"no_valid":no_valid,"elapsed_ms":started.elapsed().as_secs_f64()*1000.0,"observations":observations,"usage_rows":usage,"queries":counter.queries.lock().unwrap().len()})
        );
    }
    store
}

#[tokio::test]
async fn long_missing_usage_keeps_fallbacks_without_repeated_history_reads() {
    let dir = tempfile::tempdir().unwrap();
    for remote in [false, true] {
        let (store, _) = if remote {
            super::libsql_store(dir.path().join("long-libsql.db"))
                .await
                .unwrap()
        } else {
            super::native_store(dir.path().join("long-native.db"))
                .await
                .unwrap()
        };
        long_gap_fixture(&store).await;
        long_gap_reads(store).await;
    }
}

#[tokio::test]
async fn settled_incomplete_usage_keeps_estimates_conservative() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _) = super::native_store(dir.path().join("incomplete.db"))
        .await
        .unwrap();
    seed(&store, 1, 1).await;
    store
        .backend()
        .execute(Statement::plain(
            "UPDATE usage_rows SET dimensions_json = '{\"usage_incomplete\":\"true\"}'",
        ))
        .await
        .unwrap();
    let values = store
        .credential_quota_statistics(&CredentialQuotaCycleQuery {
            credential_id: Some(1),
            provider_id: None,
            from: 0,
            to: 1000,
            calculate: true,
            history: false,
        })
        .await
        .unwrap();
    let estimate = values[0].cycle.estimate.as_ref().unwrap();
    assert!(estimate.tokens.is_none());
    assert_eq!(estimate.reason.as_deref(), Some("incomplete_usage"));
}

#[tokio::test]
#[ignore = "requires an empty local PostgreSQL database"]
async fn postgres_long_gap_estimates_and_index_work() {
    use crate::{
        query::runtime::{ObservationSlice, observation_slice},
        schema::Dialect,
    };
    let store = Store::open(crate::BackendConfig::Postgres {
        dsn: std::env::var("GPROXY_TEST_POSTGRES_DSN").unwrap(),
        pool_size: 4,
        checkout_timeout: std::time::Duration::from_secs(5),
    })
    .await
    .unwrap();
    long_gap_fixture(&store).await;
    let store = long_gap_reads(store).await;
    store
        .backend()
        .execute(Statement::plain("ANALYZE credential_quota_observations"))
        .await
        .unwrap();
    let query = observation_slice(
        &[1, 2].map(|cycle| ObservationSlice {
            cycle,
            from: 1000,
            to: 500001,
            before: Some((304000, 303999)),
        }),
        Some(32),
    )
    .unwrap();
    // EXPLAIN returns PostgreSQL JSON, outside the store's portable row types.
    // Use the text protocol for this diagnostic on the isolated test database.
    let mut sql = query.sql_for(Dialect::Postgres).to_owned();
    for (i, value) in query.args.iter().enumerate().rev() {
        let crate::backend::DbValue::Integer(value) = value else {
            panic!("integer fixture")
        };
        sql = sql.replace(&format!("${}", i + 1), &value.to_string());
    }
    let (client, connection) = tokio_postgres::connect(
        &std::env::var("GPROXY_TEST_POSTGRES_DSN").unwrap(),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    let connection = tokio::spawn(connection);
    let rows = client
        .simple_query(&format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}"))
        .await
        .unwrap();
    let row = rows
        .iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("EXPLAIN row");
    let plan: serde_json::Value = serde_json::from_str(row.get(0).unwrap()).unwrap();
    drop(client);
    connection.await.unwrap().unwrap();
    fn check(node: &serde_json::Value, visited: &mut usize) {
        let kind = node["Node Type"].as_str().unwrap_or_default();
        assert_ne!(kind, "WindowAgg");
        assert_eq!(node["Temp Written Blocks"].as_u64().unwrap_or(0), 0);
        if kind.contains("Scan") && node["Relation Name"] == "credential_quota_observations" {
            assert!(kind.contains("Index"), "{node}");
            let rows = node["Actual Rows"].as_f64().unwrap_or(0.0)
                + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0);
            assert!(
                rows <= 32.0,
                "cursor and limit must bound actual database work: {node}"
            );
            *visited += 1;
        }
        for child in node["Plans"].as_array().into_iter().flatten() {
            check(child, visited);
        }
    }
    let mut scans = 0;
    check(&plan[0]["Plan"], &mut scans);
    assert_eq!(scans, 2);
    println!("LONG_GAP_INDEX_PLAN={plan}");
}
