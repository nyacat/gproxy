use crate::backend::{Executor, Statement, native::NativeSql};
use crate::schema::{Dialect, SchemaVersion};

// Historical self DDL is deliberately frozen here. The same version number
// represented different migrations in upstream and self installations.
pub(super) const SELF_INDEX: &[&str] = &[
    "ALTER TABLE credential_quota_cycles ADD COLUMN needs_rebuild INTEGER NOT NULL DEFAULT 0",
    "CREATE INDEX ix_credential_quota_cycles_rebuild ON credential_quota_cycles(needs_rebuild,id)",
    "CREATE INDEX ix_credential_quota_cycles_credential_rebuild ON credential_quota_cycles(credential_id,needs_rebuild,id)",
];
const SELF_REPLAY: &[&str] = &[
    "CREATE TABLE settlement_replays(request_id TEXT NOT NULL PRIMARY KEY,payload_json TEXT NOT NULL,completed INTEGER NOT NULL DEFAULT 0)",
    "CREATE INDEX ix_settlement_replays_pending ON settlement_replays(completed,request_id)",
];
const SELF_ACTIVITY: &[&str] = &[
    "ALTER TABLE credential_quota_activity ADD COLUMN parent_request_id TEXT",
    "ALTER TABLE credential_quota_activity ADD COLUMN state TEXT NOT NULL DEFAULT 'unresolved'",
    "CREATE INDEX ix_quota_activity_parent ON credential_quota_activity(parent_request_id,state)",
    "CREATE INDEX ix_quota_activity_unresolved ON credential_quota_activity(credential_id,state,started_at_ms)",
];

async fn execute(executor: &dyn Executor, statements: &[&str]) {
    executor
        .batch(
            statements
                .iter()
                .map(|sql| Statement::plain(*sql))
                .collect(),
        )
        .await
        .unwrap();
}

async fn seed_self(executor: &dyn Executor, dialect: Dialect, version: i64) {
    crate::migration::migrate_to(executor, dialect, SchemaVersion::RouteStrategies)
        .await
        .unwrap();
    for (step, ddl) in [(10, SELF_INDEX), (11, SELF_REPLAY), (12, SELF_ACTIVITY)] {
        if step > version {
            break;
        }
        // SQLite calls this type INTEGER; deployed PostgreSQL schemas use
        // BIGINT for the same catalogue integer columns.
        let ddl = ddl
            .iter()
            .map(|sql| {
                Statement::plain(if dialect == Dialect::Postgres {
                    sql.replace(" INTEGER", " BIGINT")
                } else {
                    (*sql).to_owned()
                })
            })
            .collect();
        executor.batch(ddl).await.unwrap();
        executor
            .execute(Statement::plain(format!(
                "INSERT INTO schema_migrations(version,applied_at) VALUES({step},123)"
            )))
            .await
            .unwrap();
    }
    if version >= 11 {
        execute(executor, &[
            "INSERT INTO settlement_replays(request_id,payload_json,completed) VALUES('pending','{\"keep\":1}',0),('done','{\"keep\":2}',1)",
        ]).await;
    }
    seed_activity(executor).await;
    if version >= 12 {
        // Real attempt ids differ from their parent request and must survive.
        execute(executor, &[
            "UPDATE credential_quota_activity SET parent_request_id='root-request',state='in_flight' WHERE request_id='metered:attempt:1'",
            "UPDATE credential_quota_activity SET parent_request_id='other-root',state='settled' WHERE request_id='already-settled'",
        ]).await;
    }
}

async fn verify_self(executor: &dyn Executor, version: i64) {
    verify_activity(executor, version == 12).await;
    let replays = executor
        .execute(Statement::plain(
            "SELECT request_id,payload_json,completed FROM settlement_replays ORDER BY request_id",
        ))
        .await
        .unwrap();
    let rows = replays
        .rows
        .iter()
        .map(|row| {
            (
                row.text("request_id").unwrap(),
                row.text("payload_json").unwrap(),
                row.i64("completed").unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        if version >= 11 {
            vec![("done", "{\"keep\":2}", 1), ("pending", "{\"keep\":1}", 0)]
        } else {
            Vec::new()
        }
    );
    let timestamp = executor
        .execute(Statement::plain(
            "SELECT applied_at FROM schema_migrations WHERE version=10",
        ))
        .await
        .unwrap();
    assert_eq!(timestamp.rows[0].i64("applied_at").unwrap(), 123);
}

async fn seed_activity(executor: &dyn Executor) {
    execute(executor, &[
        "INSERT INTO credential_quota_activity(request_id,credential_id,model,started_at_ms) VALUES('metered:attempt:1',7,'model',100),('metered:attempt:1',8,'model',100),('already-settled',9,'model',101),('unknown',10,'model',102)",
    ]).await;
    for (request, credential, started) in
        [("metered:attempt:1", 7, 100), ("already-settled", 9, 101)]
    {
        let usage = serde_json::from_value(serde_json::json!({
            "request_id": request, "credential_id": credential, "provider_id": 1,
            "upstream_started_at_ms": started, "at": 10, "upstream_model": "model",
            "input_tokens": 3, "output_tokens": 2, "cached_input_tokens": 0,
            "metrics": {}, "dimensions": {}, "cost": "0.01", "usage_source": "upstream",
            "ended": "complete", "latency_ms": 1
        }))
        .unwrap();
        executor
            .execute(crate::query::usage::insert_usage(&usage).unwrap())
            .await
            .unwrap();
    }
}

async fn verify_activity(executor: &dyn Executor, old_lifecycle: bool) {
    let result = executor.execute(Statement::plain(
        "SELECT credential_id,parent_request_id,state FROM credential_quota_activity ORDER BY credential_id",
    )).await.unwrap();
    let actual = result
        .rows
        .iter()
        .map(|row| {
            (
                row.i64("credential_id").unwrap(),
                row.text("parent_request_id").unwrap(),
                row.text("state").unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            (
                7,
                if old_lifecycle {
                    "root-request"
                } else {
                    "metered:attempt:1"
                },
                "settled"
            ),
            (
                8,
                if old_lifecycle {
                    "root-request"
                } else {
                    "metered:attempt:1"
                },
                if old_lifecycle {
                    "in_flight"
                } else {
                    "unresolved"
                }
            ),
            (
                9,
                if old_lifecycle {
                    "other-root"
                } else {
                    "already-settled"
                },
                "settled"
            ),
            (10, "unknown", "unresolved"),
        ]
    );
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
    // Both upstream snapshot tables are required regardless of the old lineage.
    execute(
        executor,
        &[
            "SELECT * FROM credential_quota_sources",
            "SELECT * FROM credential_quota_response_entries",
            "SELECT needs_rebuild FROM credential_quota_cycles",
            "SELECT * FROM settlement_replays",
        ],
    )
    .await;
}

#[tokio::test]
async fn self_versions_ten_through_twelve_upgrade_without_losing_replays_or_parent_ids() {
    for remote in [false, true] {
        for version in 10..=12 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("self-history.db");
            let executor = NativeSql::open(path.clone()).await.unwrap();
            seed_self(&executor, Dialect::NativeSqlite, version).await;
            drop(executor);
            let (store, executor) = if remote {
                super::super::libsql_store(path).await.unwrap()
            } else {
                super::super::native_store(path).await.unwrap()
            };
            for _ in 0..2 {
                crate::migration::migrate(
                    store.backend(),
                    if remote {
                        Dialect::Libsql
                    } else {
                        Dialect::NativeSqlite
                    },
                )
                .await
                .unwrap();
                verify_self(executor.as_ref(), version).await;
            }
        }
    }
}

#[tokio::test]
async fn upstream_version_ten_preserves_snapshots_and_adds_self_accounting() {
    for remote in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("main-history.db");
        let executor = NativeSql::open(path.clone()).await.unwrap();
        crate::migration::migrate_to(
            &executor,
            Dialect::NativeSqlite,
            SchemaVersion::QuotaSnapshots,
        )
        .await
        .unwrap();
        execute(&executor, &[
            "INSERT INTO credential_quota_sources(credential_id,source_id,capability_json,entries_json) VALUES(7,'quota','{\"keep\":1}','[]')",
            "INSERT INTO credential_quota_response_entries(credential_id,source_id,entry_id,observed_at_ms,entry_json) VALUES(7,'headers','tokens',100,'{\"keep\":2}')",
        ]).await;
        seed_activity(&executor).await;
        drop(executor);
        let (store, executor) = if remote {
            super::super::libsql_store(path).await.unwrap()
        } else {
            super::super::native_store(path).await.unwrap()
        };
        for _ in 0..2 {
            crate::migration::migrate(
                store.backend(),
                if remote {
                    Dialect::Libsql
                } else {
                    Dialect::NativeSqlite
                },
            )
            .await
            .unwrap();
            verify_activity(executor.as_ref(), false).await;
            let source = executor
                .execute(Statement::plain(
                    "SELECT capability_json FROM credential_quota_sources",
                ))
                .await
                .unwrap();
            assert_eq!(source.rows.len(), 1);
            assert_eq!(
                source.rows[0].text("capability_json").unwrap(),
                "{\"keep\":1}"
            );
            let entries = executor
                .execute(Statement::plain(
                    "SELECT entry_json FROM credential_quota_response_entries",
                ))
                .await
                .unwrap();
            assert_eq!(entries.rows.len(), 1);
            assert_eq!(entries.rows[0].text("entry_json").unwrap(), "{\"keep\":2}");
        }
    }
}

#[tokio::test]
async fn partially_committed_accounting_ddl_still_backfills_and_finishes_indexes() {
    for (previous, ddl, applied_ddl) in [
        (SchemaVersion::QuotaSnapshots, SELF_INDEX),
        (SchemaVersion::QuotaRebuildIndex, SELF_REPLAY),
        (SchemaVersion::SettlementRecovery, SELF_ACTIVITY),
    ]
    .into_iter()
    .flat_map(|(previous, ddl)| (1..=ddl.len()).map(move |count| (previous, ddl, count)))
    {
        let dir = tempfile::tempdir().unwrap();
        let executor = NativeSql::open(dir.path().join("interrupted.db"))
            .await
            .unwrap();
        crate::migration::migrate_to(&executor, Dialect::NativeSqlite, previous)
            .await
            .unwrap();
        seed_activity(&executor).await;
        // Simulate MySQL committing DDL before a process failure, while the
        // data backfill and version marker have not committed yet.
        execute(&executor, &ddl[..applied_ddl]).await;
        for _ in 0..2 {
            crate::migration::migrate(&executor, Dialect::NativeSqlite)
                .await
                .unwrap();
            verify_activity(&executor, false).await;
            let indexes = executor.execute(Statement::plain("SELECT name FROM pragma_index_list('credential_quota_activity') WHERE name IN ('ix_quota_activity_parent','ix_quota_activity_unresolved')")).await.unwrap();
            assert_eq!(indexes.rows.len(), 2);
        }
    }
}

#[tokio::test]
async fn partially_completed_upstream_reconciliation_preserves_existing_snapshot_rows() {
    let ddl =
        crate::schema::migration_statements(SchemaVersion::QuotaSnapshots, Dialect::NativeSqlite);
    for applied_ddl in 1..=ddl.len() {
        let dir = tempfile::tempdir().unwrap();
        let executor = NativeSql::open(dir.path().join("reconcile-retry.db"))
            .await
            .unwrap();
        seed_self(&executor, Dialect::NativeSqlite, 12).await;
        executor
            .batch(ddl[..applied_ddl].iter().map(Statement::plain).collect())
            .await
            .unwrap();
        execute(&executor, &[
            "INSERT INTO credential_quota_sources(credential_id,source_id,capability_json,entries_json) VALUES(7,'quota','{\"keep\":1}','[]')",
        ]).await;
        for _ in 0..2 {
            crate::migration::migrate(&executor, Dialect::NativeSqlite)
                .await
                .unwrap();
            verify_activity(&executor, true).await;
            let sources = executor
                .execute(Statement::plain(
                    "SELECT capability_json FROM credential_quota_sources",
                ))
                .await
                .unwrap();
            assert_eq!(sources.rows.len(), 1);
            assert_eq!(
                sources.rows[0].text("capability_json").unwrap(),
                "{\"keep\":1}"
            );
        }
    }
}
mod history_index;
mod interruption;
mod postgres;
