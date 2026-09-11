use crate::{
    Store, StoreError,
    backend::{Executor, Statement},
    records::UsageInput,
    schema::{Dialect, SchemaVersion},
};

fn usage(request: &str, credential: i64, started: i64) -> UsageInput {
    serde_json::from_value(serde_json::json!({
        "request_id": request, "credential_id": credential, "provider_id": 1,
        "upstream_started_at_ms": started, "at": 10, "upstream_model": "model",
        "input_tokens": 3, "output_tokens": 2, "cached_input_tokens": 0,
        "metrics": {}, "dimensions": {}, "cost": "0.01", "usage_source": "upstream",
        "ended": "complete", "latency_ms": 1
    }))
    .unwrap()
}

async fn states(executor: &dyn Executor) -> Vec<(i64, String)> {
    executor
        .execute(Statement::plain(
            "SELECT credential_id, state FROM credential_quota_activity ORDER BY credential_id",
        ))
        .await
        .unwrap()
        .rows
        .into_iter()
        .map(|r| {
            (
                r.i64("credential_id").unwrap(),
                r.text("state").unwrap().into(),
            )
        })
        .collect()
}

async fn legacy(executor: &dyn Executor, dialect: Dialect) {
    crate::migration::migrate_to(executor, dialect, SchemaVersion::SettlementRecovery)
        .await
        .unwrap();
    executor.execute(Statement::plain("INSERT INTO credential_quota_activity (request_id, credential_id, model, started_at_ms) VALUES ('retry', 1, 'model', 1), ('retry', 2, 'model', 2), ('lost', 3, 'model', 3)" )).await.unwrap();
    executor
        .execute(crate::query::usage::insert_usage(&usage("retry", 2, 2)).unwrap())
        .await
        .unwrap();
    crate::migration::migrate(executor, dialect).await.unwrap();
    assert_eq!(
        states(executor).await,
        [
            (1, "unresolved".into()),
            (2, "settled".into()),
            (3, "unresolved".into())
        ]
    );
    crate::migration::migrate(executor, dialect).await.unwrap();
}

async fn lifecycle(store: Store) {
    let input = usage("request", 5, 10);
    store
        .begin_credential_attempt("request", "request", 4, "model", 9)
        .await
        .unwrap();
    store
        .finish_credential_attempt("request", 4, 9)
        .await
        .unwrap();
    store
        .begin_credential_attempt("request", "request", 5, "model", 10)
        .await
        .unwrap();
    store
        .begin_credential_attempt("request:attempt:1", "request", 6, "model", 11)
        .await
        .unwrap();
    let result = store
        .backend()
        .batch(vec![
            crate::query::usage::insert_usage(&input).unwrap(),
            crate::query::runtime::settle_usage(Some("request")).unwrap(),
            Statement::plain("INSERT INTO nonexistent_quota_activity_test_table VALUES (1)"),
        ])
        .await;
    assert!(result.is_err());
    assert!(store.usage_by_request("request").await.unwrap().is_none());
    assert!(
        states(store.backend())
            .await
            .contains(&(5, "in_flight".into()))
    );
    assert!(store.record_usage(&input).await.unwrap());
    assert!(!store.record_usage(&input).await.unwrap());
    store.finish_credential_usage("request").await.unwrap();
    // Neither a parent-wide finish nor a late destructor can erase settlement.
    store
        .finish_credential_attempt("request", 5, 10)
        .await
        .unwrap();
    store
        .begin_credential_usage("request", 5, "model", 10)
        .await
        .unwrap();
    // ON CONFLICT on a different credential is not evidence of metering it.
    assert!(!store.record_usage(&usage("request", 4, 9)).await.unwrap());
    let pending = store
        .backend()
        .execute(crate::query::runtime::estimate_pending(4, &[(0, 100)]).unwrap())
        .await
        .unwrap();
    assert_eq!(pending.rows.len(), 1);
    let current = states(store.backend()).await;
    assert!(current.contains(&(4, "unresolved".into())));
    assert!(current.contains(&(5, "settled".into())));
    assert!(current.contains(&(6, "unresolved".into())));
    store
        .record_usage(&usage("request:attempt:1", 6, 11))
        .await
        .unwrap();
    assert!(
        states(store.backend())
            .await
            .contains(&(6, "settled".into()))
    );
}

#[tokio::test]
async fn activity_migration_retry_identity_and_atomic_settlement() {
    let dir = tempfile::tempdir().unwrap();
    let executor = std::sync::Arc::new(
        super::super::native::NativeSql::open(dir.path().join("legacy.db"))
            .await
            .unwrap(),
    );
    legacy(executor.as_ref(), Dialect::NativeSqlite).await;
    lifecycle(super::store(executor)).await;
    let (store, _) = super::libsql_store(dir.path().join("libsql.db"))
        .await
        .unwrap();
    lifecycle(store).await;
}

#[tokio::test]
#[ignore = "requires an empty PostgreSQL database"]
async fn postgres_activity_migration_retry_identity_and_atomic_settlement() -> Result<(), StoreError>
{
    let config = crate::BackendConfig::Postgres {
        dsn: std::env::var("GPROXY_TEST_POSTGRES_DSN").unwrap(),
        pool_size: 4,
        checkout_timeout: std::time::Duration::from_secs(5),
    };
    let executor = crate::backend::open(config.clone()).await?;
    legacy(executor.as_ref(), Dialect::Postgres).await;
    lifecycle(Store::open(config).await?).await;
    Ok(())
}
