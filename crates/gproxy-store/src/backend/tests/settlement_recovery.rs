use serde_json::json;

use super::super::native::NativeSql;
use crate::schema::{Dialect, SchemaVersion};

#[tokio::test]
#[ignore = "requires an empty PostgreSQL database via GPROXY_TEST_POSTGRES_DSN"]
async fn postgres_settlement_replay_migration_progress_completion_and_pagination() {
    use crate::backend::Statement;

    let config = crate::BackendConfig::Postgres {
        dsn: std::env::var("GPROXY_TEST_POSTGRES_DSN").expect("GPROXY_TEST_POSTGRES_DSN"),
        pool_size: 8,
        checkout_timeout: std::time::Duration::from_secs(5),
    };
    {
        let executor = crate::backend::open(config.clone()).await.unwrap();
        crate::migration::migrate_to(
            executor.as_ref(),
            Dialect::Postgres,
            SchemaVersion::QuotaRebuildIndex,
        )
        .await
        .unwrap();
        assert_eq!(
            executor
                .execute(Statement::plain(
                    "SELECT COUNT(*) AS count FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'settlement_replays'",
                ))
                .await
                .unwrap()
                .rows[0]
                .i64("count")
                .unwrap(),
            0,
        );
    }

    {
        // Opening an existing v11 database must install the v12 table, default
        // pending state, and pending index before any recovery write is issued.
        let store = crate::Store::open(config.clone()).await.unwrap();
        assert_eq!(
            store
                .backend()
                .execute(Statement::plain(format!(
                    "SELECT COUNT(*) AS count FROM schema_migrations WHERE version = {}",
                    SchemaVersion::SettlementRecovery.number(),
                )))
                .await
                .unwrap()
                .rows[0]
                .i64("count")
                .unwrap(),
            1,
        );
        assert_eq!(
            store
                .backend()
                .execute(Statement::plain(
                    "SELECT COUNT(*) AS count FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'settlement_replays' AND indexname = 'ix_settlement_replays_pending'",
                ))
                .await
                .unwrap()
                .rows[0]
                .i64("count")
                .unwrap(),
            1,
        );
        assert_enqueue_preserves_advanced_stage(&store).await;
        store
            .delete_settlement_replay("request-stage")
            .await
            .unwrap();
        assert_completion_prevents_stale_copy_revival(&store).await;
        // Pagination must remain correct in the presence of completed receipts.
        assert_upsert_pagination_and_delete(&store).await;
    }

    let reopened = crate::Store::open(config).await.unwrap();
    reopened
        .enqueue_settlement_replay("request-done", &json!({"stage": "usage_pending"}))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .get_settlement_replay("request-done")
            .await
            .unwrap(),
        Some(json!({"version": 1, "completed": true})),
    );
    assert!(
        reopened
            .list_settlement_replays(None, 10)
            .await
            .unwrap()
            .is_empty(),
    );
}

#[tokio::test]
async fn settlement_replay_completion_prevents_stale_copy_revival() {
    let directory = tempfile::tempdir().unwrap();
    let (native, _) = super::native_store(directory.path().join("native.db"))
        .await
        .unwrap();
    let (libsql, _) = super::libsql_store(directory.path().join("libsql.db"))
        .await
        .unwrap();

    for store in [native, libsql] {
        assert_completion_prevents_stale_copy_revival(&store).await;
    }
}

async fn assert_completion_prevents_stale_copy_revival(store: &crate::Store) {
    let initial = json!({"stage": "usage_pending"});
    let advanced = json!({"stage": "admission_pending"});
    let completed = json!({"version": 1, "completed": true});
    store
        .enqueue_settlement_replay("request-done", &initial)
        .await
        .unwrap();
    assert!(
        store
            .replace_settlement_replay("request-done", &initial, &advanced)
            .await
            .unwrap()
    );
    assert!(
        !store
            .replace_settlement_replay("request-done", &initial, &initial)
            .await
            .unwrap()
    );
    assert!(
        !store
            .replace_settlement_replay("request-missing", &initial, &advanced)
            .await
            .unwrap()
    );
    // Completion based on an earlier read must not erase the task after
    // another writer has advanced or upgraded its payload.
    assert!(
        !store
            .complete_settlement_replay_if("request-done", &initial)
            .await
            .unwrap()
    );
    assert!(
        !store
            .complete_settlement_replay_if("request-missing", &initial)
            .await
            .unwrap()
    );
    assert_eq!(
        store.get_settlement_replay("request-done").await.unwrap(),
        Some(advanced.clone())
    );

    assert!(
        store
            .complete_settlement_replay_if("request-done", &advanced)
            .await
            .unwrap()
    );
    assert!(
        !store
            .complete_settlement_replay_if("request-done", &completed)
            .await
            .unwrap()
    );
    store
        .complete_settlement_replay("request-done")
        .await
        .unwrap();
    store
        .enqueue_settlement_replay("request-done", &initial)
        .await
        .unwrap();
    store
        .put_settlement_replay("request-done", &initial)
        .await
        .unwrap();
    assert!(
        !store
            .replace_settlement_replay("request-done", &completed, &initial)
            .await
            .unwrap()
    );
    assert_eq!(
        store.get_settlement_replay("request-done").await.unwrap(),
        Some(completed)
    );
    assert!(
        store
            .list_settlement_replays(None, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn settlement_replay_completion_survives_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("completed.db");
    let initial = json!({"stage": "usage_pending"});
    {
        let (store, _) = super::native_store(path.clone()).await.unwrap();
        store
            .enqueue_settlement_replay("request-durable", &initial)
            .await
            .unwrap();
        store
            .complete_settlement_replay("request-durable")
            .await
            .unwrap();
    }
    let (store, _) = super::native_store(path).await.unwrap();
    store
        .enqueue_settlement_replay("request-durable", &initial)
        .await
        .unwrap();
    store
        .put_settlement_replay("request-durable", &initial)
        .await
        .unwrap();
    assert_eq!(
        store
            .get_settlement_replay("request-durable")
            .await
            .unwrap(),
        Some(json!({"version": 1, "completed": true}))
    );
    assert!(
        store
            .list_settlement_replays(None, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn settlement_replay_enqueue_preserves_advanced_stage() {
    let directory = tempfile::tempdir().unwrap();
    let (native, _) = super::native_store(directory.path().join("native.db"))
        .await
        .unwrap();
    let (libsql, _) = super::libsql_store(directory.path().join("libsql.db"))
        .await
        .unwrap();

    for store in [native, libsql] {
        assert_enqueue_preserves_advanced_stage(&store).await;
    }
}

async fn assert_enqueue_preserves_advanced_stage(store: &crate::Store) {
    let initial = json!({"stage": "usage_pending", "cost": "0.01"});
    let advanced = json!({"stage": "admission_pending", "cost": "0.01"});
    store
        .enqueue_settlement_replay("request-stage", &initial)
        .await
        .unwrap();
    assert_eq!(
        store.get_settlement_replay("request-stage").await.unwrap(),
        Some(initial.clone())
    );

    let (progress, repeated_enqueue) = tokio::join!(
        store.put_settlement_replay("request-stage", &advanced),
        store.enqueue_settlement_replay("request-stage", &initial),
    );
    progress.unwrap();
    repeated_enqueue.unwrap();
    store
        .enqueue_settlement_replay("request-stage", &initial)
        .await
        .unwrap();
    assert_eq!(
        store.list_settlement_replays(None, 10).await.unwrap(),
        vec![("request-stage".into(), advanced)]
    );
}

#[tokio::test]
async fn settlement_replay_upsert_pagination_and_delete_match_libsql() {
    let directory = tempfile::tempdir().unwrap();
    let (native, _) = super::native_store(directory.path().join("native.db"))
        .await
        .unwrap();
    let (libsql, _) = super::libsql_store(directory.path().join("libsql.db"))
        .await
        .unwrap();

    for store in [native, libsql] {
        assert_upsert_pagination_and_delete(&store).await;
    }
}

async fn assert_upsert_pagination_and_delete(store: &crate::Store) {
    let original = json!({"cost": "0.01", "complete": false});
    let updated = json!({"cost": "0.02", "complete": true});
    assert!(
        store
            .get_settlement_replay("request-b")
            .await
            .unwrap()
            .is_none()
    );
    for request_id in ["request-c", "request-a", "request-b", "request-b"] {
        store
            .put_settlement_replay(request_id, &original)
            .await
            .unwrap();
    }
    store
        .put_settlement_replay("request-b", &updated)
        .await
        .unwrap();

    assert_eq!(
        store.get_settlement_replay("request-a").await.unwrap(),
        Some(original.clone())
    );
    assert_eq!(
        store.get_settlement_replay("request-b").await.unwrap(),
        Some(updated.clone())
    );
    assert_eq!(
        store.list_settlement_replays(None, 2).await.unwrap(),
        vec![
            ("request-a".into(), original.clone()),
            ("request-b".into(), updated.clone()),
        ]
    );
    assert_eq!(
        store
            .list_settlement_replays(Some("request-b"), 2)
            .await
            .unwrap(),
        vec![("request-c".into(), original.clone())]
    );
    assert!(
        store
            .list_settlement_replays(Some("request-c"), 2)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .list_settlement_replays(None, 0)
            .await
            .unwrap()
            .is_empty()
    );

    // A processed cursor can be deleted before fetching the next page.
    store.delete_settlement_replay("request-a").await.unwrap();
    store.delete_settlement_replay("request-a").await.unwrap();
    assert!(
        store
            .get_settlement_replay("request-a")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .list_settlement_replays(Some("request-a"), 2)
            .await
            .unwrap(),
        vec![
            ("request-b".into(), updated),
            ("request-c".into(), original),
        ]
    );
    for request_id in ["request-b", "request-c"] {
        store.delete_settlement_replay(request_id).await.unwrap();
    }
    assert!(
        store
            .list_settlement_replays(None, 2)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn settlement_replay_migrates_existing_database_and_survives_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("replay.db");
    {
        let old = NativeSql::open(path.clone()).await.unwrap();
        crate::migration::migrate_to(
            &old,
            Dialect::NativeSqlite,
            SchemaVersion::QuotaRebuildIndex,
        )
        .await
        .unwrap();
    }

    let payload = json!({
        "version": 1,
        "usage": {"input_tokens": 13, "cost": "0.000013"},
        "quota_pending": true
    });
    {
        let (store, _) = super::native_store(path.clone()).await.unwrap();
        store
            .put_settlement_replay("request-durable", &payload)
            .await
            .unwrap();
    }
    {
        let (store, _) = super::native_store(path.clone()).await.unwrap();
        assert_eq!(
            store
                .get_settlement_replay("request-durable")
                .await
                .unwrap(),
            Some(payload.clone())
        );
        assert_eq!(
            store.list_settlement_replays(None, 1).await.unwrap(),
            vec![("request-durable".into(), payload)]
        );
        store
            .delete_settlement_replay("request-durable")
            .await
            .unwrap();
    }
    let (store, _) = super::native_store(path).await.unwrap();
    assert!(
        store
            .list_settlement_replays(None, 1)
            .await
            .unwrap()
            .is_empty()
    );
}
