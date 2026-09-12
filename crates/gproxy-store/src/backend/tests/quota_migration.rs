mod branch_history;

use std::sync::Arc;

use super::super::{Executor, Statement, native::NativeSql};
use crate::schema::{Dialect, SchemaVersion};

#[tokio::test]
async fn upgrade_indexes_closed_rebuilds_and_pages_past_recent_history() {
    for (remote, version) in [false, true].into_iter().flat_map(|remote| {
        [
            SchemaVersion::OwnedRows,
            SchemaVersion::ModelPermissions,
            SchemaVersion::RouteStrategies,
            SchemaVersion::QuotaSnapshots,
        ]
        .map(|version| (remote, version))
    }) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("quota-rebuild-migration.db");
        let executor = Arc::new(NativeSql::open(path.clone()).await.unwrap());
        crate::migration::migrate_to(executor.as_ref(), Dialect::NativeSqlite, version)
            .await
            .unwrap();
        for id in 1..=40 {
            let tracking = serde_json::json!({"needs_rebuild": id <= 20});
            executor.execute(Statement::plain(format!(
                "INSERT INTO credential_quota_cycles (id, accounting_start_ms, tracking_json, version, credential_id, window_key, boundary_source, boundary_confidence, status, last_observed_at, coverage, metrics_json) VALUES ({id}, 0, '{tracking}', 1, 7, 'primary', 'upstream', 'exact', 'closed', 1, 'partial_lower_bound', '{{}}')"
            ))).await.unwrap();
        }
        drop(executor);
        let (store, executor) = if remote {
            super::libsql_store(path).await.unwrap()
        } else {
            super::native_store(path).await.unwrap()
        };
        assert!(
            store
                .unclosed_credential_quota_cycles(None)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(store.active_usage_credentials(0).await.unwrap().is_empty());
        assert_eq!(
            store
                .pending_credential_quota_rebuilds(None, 0)
                .await
                .unwrap(),
            (1..=16).collect::<Vec<_>>()
        );
        assert_eq!(
            store
                .pending_credential_quota_rebuilds(Some(7), 16)
                .await
                .unwrap(),
            (17..=20).collect::<Vec<_>>()
        );
        assert!(
            store
                .pending_credential_quota_rebuilds(None, 20)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .pending_credential_quota_rebuilds(Some(8), 0)
                .await
                .unwrap()
                .is_empty()
        );
        let plan = executor.execute(Statement::plain("EXPLAIN QUERY PLAN SELECT id FROM credential_quota_cycles WHERE needs_rebuild = 1 AND id > 0 ORDER BY id LIMIT 16")).await.unwrap();
        assert!(plan.rows.iter().any(|row| {
            row.text("detail")
                .unwrap()
                .contains("ix_credential_quota_cycles_rebuild")
        }));
    }
}

#[tokio::test]
async fn legacy_local_version_eight_preserves_data_and_completes_upstream_migrations() {
    for (remote, progress) in [false, true]
        .into_iter()
        .flat_map(|remote| (0..=2).map(move |progress| (remote, progress)))
    {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("legacy-quota-migration.db");
        let executor = NativeSql::open(path.clone()).await.unwrap();
        seed_legacy_schema(&executor, Dialect::NativeSqlite, progress).await;
        drop(executor);
        let (store, executor) = if remote {
            super::libsql_store(path).await.unwrap()
        } else {
            super::native_store(path).await.unwrap()
        };
        verify_legacy_upgrade(&store, executor.as_ref()).await;
        // Reopening or retrying after a completed upgrade must not replay DDL.
        crate::migration::migrate(
            executor.as_ref(),
            if remote {
                Dialect::Libsql
            } else {
                Dialect::NativeSqlite
            },
        )
        .await
        .unwrap();
        verify_legacy_upgrade(&store, executor.as_ref()).await;
    }
}

#[tokio::test]
#[ignore = "requires an empty PostgreSQL database via GPROXY_TEST_POSTGRES_DSN"]
async fn postgres_legacy_local_schema_completes_upstream_migrations() {
    let executor = crate::backend::open(crate::BackendConfig::Postgres {
        dsn: std::env::var("GPROXY_TEST_POSTGRES_DSN").expect("GPROXY_TEST_POSTGRES_DSN"),
        pool_size: 8,
        checkout_timeout: std::time::Duration::from_secs(5),
    })
    .await
    .unwrap();
    seed_legacy_schema(executor.as_ref(), Dialect::Postgres, 0).await;
    let store = crate::Store {
        executor: executor.clone(),
        dialect: Dialect::Postgres,
        quota_window_locks: Default::default(),
    };
    for _ in 0..2 {
        crate::migration::migrate(executor.as_ref(), Dialect::Postgres)
            .await
            .unwrap();
        verify_legacy_upgrade(&store, executor.as_ref()).await;
    }
}

async fn seed_legacy_schema(executor: &dyn Executor, dialect: Dialect, progress: u8) {
    crate::migration::migrate_to(executor, dialect, SchemaVersion::OwnedRows)
        .await
        .unwrap();
    // Version 8 belonged to a released self schema. Keep that DDL frozen,
    // rather than recreating it from today's canonical rebuild migration.
    let mut statements = branch_history::SELF_INDEX
        .iter()
        .map(|sql| {
            Statement::plain(if dialect == Dialect::Postgres {
                sql.replace(" INTEGER", " BIGINT")
            } else {
                (*sql).to_owned()
            })
        })
        .collect::<Vec<_>>();
    statements.extend([
        Statement::plain("INSERT INTO schema_migrations(version,applied_at) VALUES(8,0)"),
        Statement::plain("INSERT INTO permissions(subject_kind,subject_id,allowed) VALUES('user',1,1)"),
        Statement::plain("INSERT INTO routes(name,max_attempts,enabled) VALUES('legacy-route',3,1)"),
        Statement::plain("INSERT INTO credential_quota_cycles (id, accounting_start_ms, tracking_json, version, credential_id, window_key, boundary_source, boundary_confidence, status, last_observed_at, coverage, metrics_json, needs_rebuild) VALUES (1, 0, '{\"needs_rebuild\":true}', 1, 7, 'primary', 'upstream', 'exact', 'closed', 1, 'partial_lower_bound', '{}', 1)"),
    ]);
    executor.batch(statements).await.unwrap();
    // Exercise recovery when a previous attempt stopped after repairing
    // permissions or applying the upstream route migration.
    if progress > 0 {
        executor
            .batch(
                crate::schema::migration_statements(SchemaVersion::ModelPermissions, dialect)
                    .into_iter()
                    .map(Statement::plain)
                    .collect(),
            )
            .await
            .unwrap();
    }
    if progress > 1 {
        let mut statements =
            crate::schema::migration_statements(SchemaVersion::RouteStrategies, dialect)
                .into_iter()
                .map(Statement::plain)
                .collect::<Vec<_>>();
        statements.push(Statement::plain(
            "INSERT INTO schema_migrations(version,applied_at) VALUES(9,0)",
        ));
        executor.batch(statements).await.unwrap();
    }
}

async fn verify_legacy_upgrade(store: &crate::Store, executor: &dyn Executor) {
    let versions = executor
        .execute(Statement::plain(
            "SELECT version FROM schema_migrations ORDER BY version",
        ))
        .await
        .unwrap();
    assert_eq!(
        versions
            .rows
            .iter()
            .map(|row| row.i64("version").unwrap())
            .collect::<Vec<_>>(),
        (1..=SchemaVersion::LATEST.number()).collect::<Vec<_>>()
    );
    let permissions = executor
        .execute(Statement::plain(
            "SELECT COUNT(*) AS count FROM permissions WHERE model_pattern IS NULL AND allowed = 1",
        ))
        .await
        .unwrap();
    assert_eq!(permissions.rows[0].i64("count").unwrap(), 1);
    let routes = executor
        .execute(Statement::plain(
            "SELECT strategy FROM routes WHERE name = 'legacy-route'",
        ))
        .await
        .unwrap();
    assert_eq!(routes.rows[0].text("strategy").unwrap(), "weighted");
    assert_eq!(
        store
            .pending_credential_quota_rebuilds(None, 0)
            .await
            .unwrap(),
        vec![1]
    );
}
