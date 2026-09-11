use futures_util::future::join_all;
use rust_decimal::Decimal;

use crate::records::{QuotaInput, QuotaWindowKind};
use crate::{BackendConfig, Store};

#[tokio::test]
async fn native_and_libsql_same_window_settlements_survive_concurrency() {
    for remote in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("quota-contention.db");
        let (store, _) = if remote {
            super::libsql_store(path).await.unwrap()
        } else {
            super::native_store(path).await.unwrap()
        };
        exercise_concurrent_settlement(store).await;
    }
}

#[tokio::test]
#[ignore = "requires an empty PostgreSQL database via GPROXY_TEST_POSTGRES_DSN"]
async fn postgres_same_window_settlements_survive_concurrency() {
    let store = Store::open(BackendConfig::Postgres {
        dsn: std::env::var("GPROXY_TEST_POSTGRES_DSN").expect("GPROXY_TEST_POSTGRES_DSN"),
        pool_size: 8,
        checkout_timeout: std::time::Duration::from_secs(5),
    })
    .await
    .unwrap();
    exercise_concurrent_settlement(store).await;
}

async fn exercise_concurrent_settlement(store: Store) {
    let quota = store
        .insert_quota(&QuotaInput {
            subject_kind: "credential".into(),
            subject_id: 1,
            quota_total: Some(1_000.into()),
            quota_daily: None,
            quota_weekly: None,
            quota_monthly: None,
            quota_5h: None,
            quota_7d: None,
            enabled: true,
        })
        .await
        .unwrap();
    let window = store
        .ensure_quota_window(quota, QuotaWindowKind::Total, 0)
        .await
        .unwrap();
    let results = join_all((0..100).map(|id| {
        let store = store.clone();
        async move {
            store
                .add_quota_cost(&format!("concurrent-{id}"), window.id, Decimal::ONE)
                .await
        }
    }))
    .await;
    for result in results {
        result.expect("healthy storage must settle contending requests");
    }
    assert_eq!(
        store
            .quota_window(window.id)
            .await
            .unwrap()
            .unwrap()
            .cost_used,
        Decimal::from(100)
    );
    let replay = store
        .add_quota_cost("concurrent-0", window.id, Decimal::ONE)
        .await
        .unwrap();
    assert_eq!(replay.cost_used, Decimal::from(100));
    for id in 0..100 {
        assert!(
            store
                .quota_settlement_exists(&format!("concurrent-{id}"), window.id)
                .await
                .unwrap()
        );
    }
}
