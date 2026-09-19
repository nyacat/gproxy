use crate::Store;
use crate::backend::tests::{libsql_store, native_store};
use crate::records::{CredentialQuotaObservation, QuotaCycleCloseReason, UsageFilter, UsageInput};
use gproxy_core::{QuotaResetBehavior, QuotaSample, QuotaScope};
use rust_decimal::Decimal;
use serde_json::json;

struct RebuildSchedule {
    inner: crate::backend::SharedExecutor,
    phase: std::sync::atomic::AtomicU8,
    first_scanned: tokio::sync::Notify,
    second_linked: tokio::sync::Notify,
    resume_first: tokio::sync::Notify,
    resume_second: tokio::sync::Notify,
}

impl crate::backend::Executor for RebuildSchedule {
    fn execute<'a>(
        &'a self,
        statement: crate::backend::Statement,
    ) -> crate::backend::DbFuture<'a, crate::backend::QueryResult> {
        Box::pin(async move {
            let usage_page =
                statement.sql.contains("FROM \"usage_rows\"") && statement.sql.contains("ORDER BY");
            let result = self.inner.execute(statement).await?;
            if usage_page
                && result.rows.is_empty()
                && self
                    .phase
                    .compare_exchange(
                        0,
                        1,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                    )
                    .is_ok()
            {
                self.first_scanned.notify_one();
                self.resume_first.notified().await;
            }
            Ok(result)
        })
    }

    fn batch<'a>(
        &'a self,
        statements: Vec<crate::backend::Statement>,
    ) -> crate::backend::DbFuture<'a, Vec<crate::backend::QueryResult>> {
        Box::pin(async move {
            let contains_links = statements.iter().any(|s| {
                s.sql
                    .starts_with("INSERT INTO \"credential_quota_cycle_usage\"")
            });
            let result = self.inner.batch(statements).await?;
            if contains_links
                && self
                    .phase
                    .compare_exchange(
                        1,
                        2,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                    )
                    .is_ok()
            {
                self.second_linked.notify_one();
                self.resume_second.notified().await;
            }
            Ok(result)
        })
    }
}

struct InterruptedRebuild {
    inner: crate::backend::SharedExecutor,
    fail: std::sync::atomic::AtomicBool,
}

impl crate::backend::Executor for InterruptedRebuild {
    fn execute<'a>(
        &'a self,
        statement: crate::backend::Statement,
    ) -> crate::backend::DbFuture<'a, crate::backend::QueryResult> {
        self.inner.execute(statement)
    }

    fn batch<'a>(
        &'a self,
        statements: Vec<crate::backend::Statement>,
    ) -> crate::backend::DbFuture<'a, Vec<crate::backend::QueryResult>> {
        Box::pin(async move {
            assert!(
                statements.len() <= 258,
                "cycle work must stay bounded per transaction"
            );
            let links = statements.iter().any(|statement| {
                statement
                    .sql
                    .starts_with("INSERT INTO \"credential_quota_cycle_usage\"")
            });
            let result = self.inner.batch(statements).await?;
            if links && self.fail.swap(false, std::sync::atomic::Ordering::SeqCst) {
                return Err(crate::StoreError::Database("lost batch reply".into()));
            }
            Ok(result)
        })
    }
}

#[tokio::test]
async fn interrupted_rebuild_resumes_from_an_atomic_page() {
    let directory = tempfile::tempdir().unwrap();
    let (mut store, _) = native_store(directory.path().join("rebuild-pages.db"))
        .await
        .unwrap();
    for index in 0..601 {
        store
            .record_usage(&usage(&format!("page-{index}"), "model-a", 12_000, 13))
            .await
            .unwrap();
    }
    store.executor = std::sync::Arc::new(InterruptedRebuild {
        inner: store.executor.clone(),
        fail: std::sync::atomic::AtomicBool::new(true),
    });
    assert!(
        store
            .observe_credential_quota_cycle(&reading(20_000, 20))
            .await
            .is_err()
    );
    let interrupted = store
        .credential_quota_cycle_history(7, "primary")
        .await
        .unwrap();
    assert!(interrupted[0].tracking.needs_rebuild);
    assert_eq!(interrupted[0].tracking.rebuild_after, Some(256));
    assert_eq!(interrupted[0].metrics["requests"], json!("256"));
    store.repair_credential_quota(7, 50).await.unwrap();
    let complete = store
        .credential_quota_cycle_history(7, "primary")
        .await
        .unwrap();
    assert!(!complete[0].tracking.needs_rebuild);
    assert_eq!(complete[0].tracking.rebuild_after, None);
    assert_eq!(complete[0].metrics["requests"], json!("601"));
    assert_eq!(complete[0].metrics["cost"], json!("1202"));
}

#[tokio::test]
async fn closing_a_partial_rebuild_restarts_after_a_boundary_change() {
    let directory = tempfile::tempdir().unwrap();
    let (mut store, _) = native_store(directory.path().join("close-rebuild.db"))
        .await
        .unwrap();
    for index in 0..301 {
        let at = if index % 2 == 0 { 12_000 } else { 70_000 };
        store
            .record_usage(&usage(&format!("close-{index}"), "model-a", at, 80))
            .await
            .unwrap();
    }
    store.executor = std::sync::Arc::new(InterruptedRebuild {
        inner: store.executor.clone(),
        fail: std::sync::atomic::AtomicBool::new(true),
    });
    assert!(
        store
            .observe_credential_quota_cycle(&reading(20_000, 20))
            .await
            .is_err()
    );
    let id = store
        .credential_quota_cycle_history(7, "primary")
        .await
        .unwrap()[0]
        .id;
    let closed = store
        .close_credential_quota_cycle(id, QuotaCycleCloseReason::ManualReset, 50)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closed.metrics["requests"], json!("151"));
    assert!(!closed.tracking.needs_rebuild);
    assert_eq!(closed.tracking.rebuild_after, None);
}

#[tokio::test]
async fn concurrent_rebuild_preserves_every_linked_usage() {
    let directory = tempfile::tempdir().unwrap();
    let (store, _) = native_store(directory.path().join("review-rebuild.db"))
        .await
        .unwrap();
    assert_concurrent_rebuild(store).await;
}

#[tokio::test]
#[ignore = "requires an empty PostgreSQL database via GPROXY_TEST_POSTGRES_CYCLE_DSN"]
async fn postgres_concurrent_rebuild_preserves_every_linked_usage() {
    let store = Store::open(crate::BackendConfig::Postgres {
        dsn: std::env::var("GPROXY_TEST_POSTGRES_CYCLE_DSN")
            .expect("GPROXY_TEST_POSTGRES_CYCLE_DSN"),
        pool_size: 8,
        checkout_timeout: std::time::Duration::from_secs(5),
    })
    .await
    .unwrap();
    assert_concurrent_rebuild(store).await;
}

async fn assert_concurrent_rebuild(mut store: Store) {
    store
        .observe_credential_quota_cycle(&reading(10_000, 10))
        .await
        .unwrap();
    store
        .record_usage(&usage("review-first", "model-a", 12_000, 13))
        .await
        .unwrap();
    let schedule = std::sync::Arc::new(RebuildSchedule {
        inner: store.executor.clone(),
        phase: std::sync::atomic::AtomicU8::new(0),
        first_scanned: Default::default(),
        second_linked: Default::default(),
        resume_first: Default::default(),
        resume_second: Default::default(),
    });
    store.executor = schedule.clone();
    let first_store = store.clone();
    let first = tokio::spawn(async move {
        let mut observation = reading(20_000, 20);
        observation.scope = QuotaScope::Models(vec!["model-a".into()]);
        first_store
            .observe_credential_quota_cycle(&observation)
            .await
            .unwrap();
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        schedule.first_scanned.notified(),
    )
    .await
    .unwrap();
    let second_store = store.clone();
    let second = tokio::spawn(async move {
        second_store
            .record_usage(&usage("review-second", "model-a", 19_000, 21))
            .await
            .unwrap();
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        schedule.second_linked.notified(),
    )
    .await
    .unwrap();
    schedule.resume_first.notify_one();
    first.await.unwrap();
    schedule.resume_second.notify_one();
    second.await.unwrap();
    let history = store
        .credential_quota_cycle_history(7, "primary")
        .await
        .unwrap();
    assert_eq!(
        history[0].metrics["requests"],
        json!("2"),
        "every linked usage row must be represented in the aggregate after concurrent rebuilds"
    );
}

#[tokio::test]
async fn repair_closes_an_expired_upstream_cycle() {
    let directory = tempfile::tempdir().unwrap();
    let (store, _) = native_store(directory.path().join("review-expired.db"))
        .await
        .unwrap();
    store
        .observe_credential_quota_cycle(&reading(10_000, 10))
        .await
        .unwrap();
    store.repair_credential_quota(7, 101).await.unwrap();
    let history = store
        .credential_quota_cycle_history(7, "primary")
        .await
        .unwrap();
    assert_eq!(
        history[0].status,
        crate::records::QuotaCycleStatus::Closed,
        "maintenance must close the elapsed upstream period"
    );
}

#[tokio::test]
async fn unused_window_does_not_roll_before_its_end() {
    let directory = tempfile::tempdir().unwrap();
    let (store, _) = native_store(directory.path().join("review-slide.db"))
        .await
        .unwrap();
    let start = 1_788_951_321;
    let duration = 18_000;
    for step in 0..600 {
        store
            .observe_credential_quota_cycle(&super::spark_observation(
                7,
                "additional_primary:codex_bengalfox",
                start + step * 30,
                duration,
                0,
            ))
            .await
            .unwrap();
    }
    let history = store
        .credential_quota_cycle_history(7, "additional_primary:codex_bengalfox")
        .await
        .unwrap();
    assert_eq!(
        history.len(),
        1,
        "an unused five-hour window must not create a new cycle after only two and a half hours"
    );
}

#[tokio::test]
async fn quota_rounds_and_usage_records_have_backend_parity() {
    let directory = tempfile::tempdir().unwrap();
    let native_path = directory.path().join("native.db");
    let remote_path = directory.path().join("remote.db");
    let (native, _) = native_store(native_path.clone()).await.unwrap();
    let (remote, _) = libsql_store(remote_path.clone()).await.unwrap();
    for store in [&native, &remote] {
        exercise(store).await;
    }
    let (reopened, _) = native_store(native_path).await.unwrap();
    let native_history = reopened
        .credential_quota_cycle_history(7, "primary")
        .await
        .unwrap();
    let remote_history = remote
        .credential_quota_cycle_history(7, "primary")
        .await
        .unwrap();
    assert_eq!(native_history, remote_history);
    for (native_cycle, remote_cycle) in native_history.iter().zip(&remote_history) {
        assert_eq!(
            reopened
                .credential_quota_observations(native_cycle, true)
                .await
                .unwrap(),
            remote
                .credential_quota_observations(remote_cycle, true)
                .await
                .unwrap(),
        );
    }
    reopened.repair_credential_quota(7, 150).await.unwrap();
    assert_eq!(
        reopened
            .credential_quota_cycle_history(7, "primary")
            .await
            .unwrap(),
        native_history
    );
}

async fn exercise(store: &Store) {
    let first = store
        .observe_credential_quota_cycle(&reading(10_000, 10))
        .await
        .unwrap();
    store
        .begin_credential_usage("sample", 7, "model-a", 10_500)
        .await
        .unwrap();
    store
        .observe_credential_quota_cycle(&reading(20_000, 20))
        .await
        .unwrap();
    let pending = store
        .credential_quota_cycle_history(7, "primary")
        .await
        .unwrap();
    assert_eq!(
        pending[0].estimate.as_ref().unwrap().reason.as_deref(),
        Some("incomplete_usage")
    );
    let samples = store
        .credential_quota_observations(&pending[0], true)
        .await
        .unwrap();
    assert_eq!(samples.len(), 2);
    assert_eq!(
        samples[1].estimate.as_ref().unwrap().reason.as_deref(),
        Some("incomplete_usage")
    );
    let sample = usage("sample", "model-a", 10_500, 21);
    assert!(store.record_usage(&sample).await.unwrap());
    assert!(!store.record_usage(&sample).await.unwrap());
    let estimated = store
        .credential_quota_cycle_history(7, "primary")
        .await
        .unwrap();
    assert_eq!(estimated[0].id, first.id);
    assert_eq!(estimated[0].metrics["requests"], json!("1"));
    assert_eq!(estimated[0].metrics["total_tokens"], json!("130"));
    assert_eq!(
        estimated[0].estimate.as_ref().unwrap().tokens,
        Some(Decimal::from(1300))
    );
    assert_eq!(
        estimated[0].estimate.as_ref().unwrap().cost,
        Some(Decimal::from(20))
    );

    let reset = store
        .observe_credential_quota_cycle(&reading(30_100, 5))
        .await
        .unwrap();
    let samples = store
        .credential_quota_observations(&estimated[0], true)
        .await
        .unwrap();
    assert_eq!(
        samples
            .iter()
            .map(|sample| sample.observed_at_ms)
            .collect::<Vec<_>>(),
        vec![10_000, 20_000]
    );
    assert_eq!(samples[0].estimate.as_ref().unwrap().tokens, None);
    assert_eq!(
        samples[1].estimate.as_ref().unwrap().tokens,
        Some(Decimal::from(1300))
    );
    assert!(
        store
            .credential_quota_observations(&estimated[0], false)
            .await
            .unwrap()
            .iter()
            .all(|sample| sample.estimate.is_none())
    );
    assert_ne!(reset.id, first.id);
    assert_eq!(reset.accounting_start_ms, 30_100);
    store
        .record_usage(&usage("long", "model-a", 29_000, 35))
        .await
        .unwrap();
    let history = store
        .credential_quota_cycle_history(7, "primary")
        .await
        .unwrap();
    assert_eq!(history[1].period_end, Some(100));
    assert_eq!(history[1].accounting_end_ms, Some(30_100));
    assert_eq!(
        history[1].close_reason,
        Some(QuotaCycleCloseReason::UsageDecreased)
    );
    assert_eq!(history[1].metrics["requests"], json!("2"));
    assert_eq!(history[0].metrics["requests"], json!("0"));

    let again = store
        .observe_credential_quota_cycle(&reading(30_200, 3))
        .await
        .unwrap();
    assert_ne!(again.id, reset.id);
    let stale = store
        .observe_credential_quota_cycle(&reading(29_900, 1))
        .await
        .unwrap();
    assert_eq!(stale.id, again.id);
    let mut overlap = reading(30_250, 1);
    overlap.sample.started_at_ms = 30_150;
    let held = store
        .observe_credential_quota_cycle(&overlap)
        .await
        .unwrap();
    assert_eq!(held.id, again.id);
    assert!(held.tracking.uncertain);
    let same = store
        .observe_credential_quota_cycle(&reading(30_200, 3))
        .await
        .unwrap();
    assert_eq!(same.id, again.id);
    assert_eq!(
        store
            .credential_quota_observations(&same, false)
            .await
            .unwrap()
            .len(),
        1
    );
    let verified = store
        .observe_credential_quota_cycle(&reading(30_300, 1))
        .await
        .unwrap();
    assert_ne!(verified.id, again.id);
    assert_eq!(verified.accounting_start_ms, 30_250);

    let mut expanded = reading(40_000, 1);
    expanded.upstream_limit = Some(Decimal::from(200));
    let expanded = store
        .observe_credential_quota_cycle(&expanded)
        .await
        .unwrap();
    assert_eq!(expanded.id, verified.id);
    assert_eq!(expanded.tracking.baseline_at_ms, 40_000);
    let mut scoped = reading(50_000, 4);
    scoped.scope = QuotaScope::Models(vec!["model-a".into()]);
    let scoped = store.observe_credential_quota_cycle(&scoped).await.unwrap();
    assert_eq!(scoped.id, verified.id);
    let samples = store
        .credential_quota_observations(&scoped, false)
        .await
        .unwrap();
    assert_eq!(samples.len(), 3);
    assert_eq!(samples[0].upstream_limit, Some(Decimal::from(100)));
    assert_eq!(samples[1].upstream_limit, Some(Decimal::from(200)));
    assert_eq!(samples[0].scope, QuotaScope::All);
    assert_eq!(samples[2].scope, QuotaScope::Models(vec!["model-a".into()]));
    store
        .record_usage(&usage("outside", "model-b", 51_000, 52))
        .await
        .unwrap();
    store
        .record_usage(&usage("inside", "model-a", 51_001, 52))
        .await
        .unwrap();
    let scope = store
        .credential_quota_cycle_history(7, "primary")
        .await
        .unwrap();
    assert_eq!(scope[0].metrics["requests"], json!("1"));
    assert_eq!(scope[0].models.len(), 1);

    let mut next = reading(510_000, 0);
    next.period_start = Some(500);
    next.period_end = Some(600);
    let (left, right) = tokio::join!(
        store.observe_credential_quota_cycle(&next),
        store.observe_credential_quota_cycle(&next)
    );
    assert_eq!(left.unwrap().id, right.unwrap().id);
    let history = store
        .credential_quota_cycle_history(7, "primary")
        .await
        .unwrap();
    assert_eq!(history.len(), 5);
    assert_eq!(history[0].accounting_start_ms, 500_000);
    assert_eq!(history[1].period_end, Some(100));
    let (first_write, duplicate_write) =
        tokio::join!(store.record_usage(&sample), store.record_usage(&sample));
    assert!(!first_write.unwrap() && !duplicate_write.unwrap());

    let mut recovering = reading(10_000, 70);
    recovering.window_key = "recovering".into();
    recovering.reset_behavior = QuotaResetBehavior::Recovering;
    let old = store
        .observe_credential_quota_cycle(&recovering)
        .await
        .unwrap();
    recovering.upstream_used = Some(Decimal::from(30));
    recovering.sample = QuotaSample {
        source: gproxy_core::QuotaSampleSource::Unknown,
        started_at_ms: 20_000,
        received_at_ms: 20_000,
    };
    recovering.observed_at = 20;
    let recovered = store
        .observe_credential_quota_cycle(&recovering)
        .await
        .unwrap();
    assert_eq!(recovered.id, old.id);
    records(store).await;
}

/// Reset stamps wobble by a second between observations, and an unused rolling
/// window reports `reset_at = now + window` so its start walks forward on every
/// probe. Both read as boundary crossings and minted a fresh cycle each time,
/// restarting accounting and crowding quieter windows out of the console page;
/// a stamp landing a second ahead of our clock was rejected outright.
#[tokio::test]
async fn wobbling_boundaries_keep_one_cycle() {
    let directory = tempfile::tempdir().unwrap();
    let (store, _) = native_store(directory.path().join("wobble.db"))
        .await
        .unwrap();

    for (index, end) in [18_000, 17_999, 18_000, 18_001].into_iter().enumerate() {
        let at = 1_000 + index as i64;
        store
            .observe_credential_quota_cycle(&super::observation(
                7,
                "wobble",
                end - 18_000,
                end,
                at,
                40,
            ))
            .await
            .unwrap();
    }
    let wobble = store
        .credential_quota_cycle_history(7, "wobble")
        .await
        .unwrap();
    assert_eq!(wobble.len(), 1);
    assert_eq!(wobble[0].period_end, Some(18_000));

    for at in [2_000, 2_010, 2_020] {
        store
            .observe_credential_quota_cycle(&super::observation(
                7,
                "unused",
                at,
                at + 18_000,
                at,
                0,
            ))
            .await
            .unwrap();
    }
    let unused = store
        .credential_quota_cycle_history(7, "unused")
        .await
        .unwrap();
    assert_eq!(unused.len(), 1);
    assert_eq!(unused[0].period_start, None);

    // Usage proves the period began, so the same cycle adopts its boundary.
    let started = store
        .observe_credential_quota_cycle(&super::observation(
            7,
            "unused",
            2_030,
            2_030 + 18_000,
            2_030,
            12,
        ))
        .await
        .unwrap();
    assert_eq!(started.id, unused[0].id);
    assert_eq!(started.period_end, Some(2_030 + 18_000));

    store
        .observe_credential_quota_cycle(&super::observation(
            7,
            "ahead",
            3_001,
            3_001 + 18_000,
            3_000,
            0,
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn spark_rolling_window_does_not_mint_cycles() {
    let directory = tempfile::tempdir().unwrap();
    let (store, _) = native_store(directory.path().join("spark.db"))
        .await
        .unwrap();
    let window = 18_000;
    let start = 1_788_951_321;
    for step in 0..600 {
        let at = start + step * 30;
        store
            .observe_credential_quota_cycle(&super::spark_observation(
                7,
                "additional_primary:codex_bengalfox",
                at,
                window,
                0,
            ))
            .await
            .unwrap();
    }
    let history = store
        .credential_quota_cycle_history(7, "additional_primary:codex_bengalfox")
        .await
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, crate::records::QuotaCycleStatus::Open);
    assert_eq!(history[0].close_reason, None);
    assert_eq!(history[0].period_start, Some(start));
    assert_eq!(history[0].period_end, Some(start + window));

    let rolled = store
        .observe_credential_quota_cycle(&super::spark_observation(
            7,
            "additional_primary:codex_bengalfox",
            start + window,
            window,
            0,
        ))
        .await
        .unwrap();
    assert_ne!(rolled.id, history[0].id);
    let history = store
        .credential_quota_cycle_history(7, "additional_primary:codex_bengalfox")
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(
        history[1].close_reason,
        Some(QuotaCycleCloseReason::BoundaryCrossed)
    );
}

#[tokio::test]
async fn rounded_zero_percent_does_not_clear_a_used_window() {
    let directory = tempfile::tempdir().unwrap();
    let (store, _) = native_store(directory.path().join("rounded.db"))
        .await
        .unwrap();
    let mut used = super::observation(7, "primary", 1_000, 19_000, 1_000, 1);
    used.used_percent = Some(Decimal::ZERO);
    let cycle = store.observe_credential_quota_cycle(&used).await.unwrap();
    assert_eq!(cycle.period_start, Some(1_000));
    assert_eq!(cycle.period_end, Some(19_000));
    assert_eq!(cycle.upstream_used, Some(Decimal::from(1)));
}

async fn records(store: &Store) {
    for index in 0..21 {
        store
            .record_usage(&usage(
                &format!("page-{index}"),
                "paged",
                120_000 + index,
                121,
            ))
            .await
            .unwrap();
    }
    let filter = UsageFilter {
        from: 120,
        to: 122,
        model: Some("paged".into()),
        ..Default::default()
    };
    let (first, total) = store.usage_records(&filter, 1, 10).await.unwrap();
    let (second, _) = store.usage_records(&filter, 2, 10).await.unwrap();
    let (last, _) = store.usage_records(&filter, 3, 10).await.unwrap();
    assert_eq!(
        (first.len(), second.len(), last.len(), total),
        (10, 10, 1, 21)
    );
    assert!(first[9].id > second[0].id && second[9].id > last[0].id);
    let summary = store.usage_summary(&filter).await.unwrap();
    assert_eq!(summary.requests, 21);
    assert_eq!(summary.cost, Decimal::from(42));
    assert_eq!(summary.total_tokens(), Decimal::from(2730));
    let exact = UsageFilter {
        request_id: Some("page-4".into()),
        user_key_id: Some(9),
        credential_id: Some(7),
        provider_id: Some(3),
        user_id: Some(8),
        operation: Some("generate".into()),
        ended: Some("complete".into()),
        usage_source: Some("upstream".into()),
        ..filter
    };
    assert_eq!(store.usage_records(&exact, 1, 10).await.unwrap().1, 1);
    assert_eq!(
        store.usage_summary(&exact).await.unwrap().cost,
        Decimal::from(2)
    );
}

fn reading(at_ms: i64, used: i64) -> CredentialQuotaObservation {
    let mut value = super::observation(7, "primary", 0, 100, at_ms / 1000, used);
    value.sample = QuotaSample {
        source: gproxy_core::QuotaSampleSource::Unknown,
        started_at_ms: at_ms,
        received_at_ms: at_ms,
    };
    value
}

fn usage(request: &str, model: &str, started: i64, at: i64) -> UsageInput {
    UsageInput {
        request_id: request.into(),
        at,
        upstream_started_at_ms: Some(started),
        provider_id: 3,
        credential_id: 7,
        organization_id: None,
        team_id: None,
        user_id: Some(8),
        user_key_id: Some(9),
        operation: Some("generate".into()),
        upstream_model: model.into(),
        input_tokens: 100,
        output_tokens: 20,
        cached_input_tokens: 80,
        metrics: json!({"cache_creation_5m_tokens": "10"}),
        dimensions: json!({}),
        cost: Decimal::from(2),
        usage_source: "upstream".into(),
        ended: "complete".into(),
        latency_ms: 1,
    }
}

#[tokio::test]
async fn real_rollover_with_small_boundary_jitter_is_not_a_slide() {
    let directory = tempfile::tempdir().unwrap();
    let (store, _) = native_store(directory.path().join("boundary-jitter.db"))
        .await
        .unwrap();
    for duration in [300, 18_000] {
        for jitter in [-1, 0, 1] {
            let key = format!("rollover-{duration}-{jitter}");
            let first = store
                .observe_credential_quota_cycle(&super::observation(7, &key, 0, duration, 100, 10))
                .await
                .unwrap();
            let start = duration + jitter;
            let next = store
                .observe_credential_quota_cycle(&super::observation(
                    7,
                    &key,
                    start,
                    start + duration,
                    duration + 2,
                    10,
                ))
                .await
                .unwrap();
            assert_ne!(
                first.id, next.id,
                "a new period must not retain the expired cycle"
            );
            assert_eq!(next.period_end, Some(start + duration));
        }
    }
}

#[tokio::test]
async fn maintenance_recovers_a_closed_partial_rebuild() {
    let directory = tempfile::tempdir().unwrap();
    let (mut store, _) = native_store(directory.path().join("closed-partial.db"))
        .await
        .unwrap();
    for index in 0..301 {
        store
            .record_usage(&usage(
                &format!("closed-partial-{index}"),
                "model-a",
                12_000,
                13,
            ))
            .await
            .unwrap();
    }
    let cycle = store
        .observe_credential_quota_cycle(&reading(20_000, 20))
        .await
        .unwrap();
    store.executor = std::sync::Arc::new(InterruptedRebuild {
        inner: store.executor.clone(),
        fail: std::sync::atomic::AtomicBool::new(true),
    });
    assert!(
        store
            .close_credential_quota_cycle(cycle.id, QuotaCycleCloseReason::ManualReset, 50)
            .await
            .is_err()
    );
    assert!(
        store
            .credential_quota_cycle_history(7, "primary")
            .await
            .unwrap()[0]
            .tracking
            .needs_rebuild
    );
    store.repair_credential_quota(7, 60).await.unwrap();
    let repaired = store
        .credential_quota_cycle_history(7, "primary")
        .await
        .unwrap()
        .remove(0);
    assert!(
        !repaired.tracking.needs_rebuild,
        "maintenance must recover durable closed rebuild work"
    );
    assert_eq!(repaired.metrics["requests"], json!("301"));
}
