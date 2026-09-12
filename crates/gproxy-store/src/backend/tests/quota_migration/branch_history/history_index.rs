use std::sync::Arc;

use crate::{
    backend::{Executor, SharedExecutor, Statement, native::NativeSql},
    schema::{Dialect, SchemaVersion},
};

// Freeze the new index DDL too: a retry must recognize a deployment which
// stopped after any of these independently committed statements.
const INDEXES: &[(&str, &str)] = &[
    ("ix_credential_quota_cycles_observed", "last_observed_at,id"),
    (
        "ix_credential_quota_cycles_credential_observed",
        "credential_id,last_observed_at,id",
    ),
    (
        "ix_credential_quota_cycles_credential_window_observed",
        "credential_id,window_key,last_observed_at,id",
    ),
    (
        "ix_credential_quota_cycles_window_observed",
        "window_key,last_observed_at,id",
    ),
];

pub(super) async fn upgrade(executor: &dyn Executor, dialect: Dialect, partial: usize) {
    super::seed_self(executor, dialect, 12).await;
    crate::migration::migrate_to(executor, dialect, SchemaVersion::QuotaActivityLifecycle)
        .await
        .unwrap();
    let checkpoint = executor
        .execute(Statement::plain(
            "SELECT applied_at FROM schema_migrations WHERE version=13",
        ))
        .await
        .unwrap()
        .rows[0]
        .i64("applied_at")
        .unwrap();
    for (name, columns) in INDEXES.iter().take(partial) {
        executor
            .execute(Statement::plain(format!(
                "CREATE INDEX {name} ON credential_quota_cycles({columns})",
            )))
            .await
            .unwrap();
    }
    for _ in 0..2 {
        crate::migration::migrate(executor, dialect).await.unwrap();
        super::verify_self(executor, 12).await;
        assert_eq!(
            executor
                .execute(Statement::plain(
                    "SELECT applied_at FROM schema_migrations WHERE version=13",
                ))
                .await
                .unwrap()
                .rows[0]
                .i64("applied_at")
                .unwrap(),
            checkpoint
        );
        let sql = match dialect {
            Dialect::Postgres => {
                "SELECT indexname::text AS name FROM pg_indexes WHERE schemaname=current_schema() AND tablename='credential_quota_cycles'"
            }
            Dialect::Mysql => {
                "SELECT DISTINCT index_name AS name FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='credential_quota_cycles'"
            }
            Dialect::NativeSqlite | Dialect::Libsql => {
                "SELECT name FROM pragma_index_list('credential_quota_cycles')"
            }
        };
        let indexes = executor.execute(Statement::plain(sql)).await.unwrap();
        for (name, _) in INDEXES {
            assert!(
                indexes
                    .rows
                    .iter()
                    .any(|row| row.text("name").unwrap() == *name)
            );
        }
    }
}

#[tokio::test]
async fn version_thirteen_history_indexes_upgrade_after_partial_ddl() {
    for remote in [false, true] {
        for partial in 0..=INDEXES.len() {
            let directory = tempfile::tempdir().unwrap();
            let database = Arc::new(
                NativeSql::open(directory.path().join("history-index.db"))
                    .await
                    .unwrap(),
            );
            let (executor, dialect): (SharedExecutor, _) = if remote {
                (
                    Arc::new(crate::backend::libsql::LibsqlHttp::with_sender(
                        "https://store.invalid".into(),
                        "test-token".into(),
                        super::super::super::sender::SqliteHrana::new(database),
                    )),
                    Dialect::Libsql,
                )
            } else {
                (database, Dialect::NativeSqlite)
            };
            upgrade(executor.as_ref(), dialect, partial).await;
        }
    }
}

#[tokio::test]
async fn history_page_queries_use_ordered_indexes_before_reading_payloads() {
    use crate::records::{CredentialQuotaCycleCursor, CredentialQuotaCyclePageQuery};

    let directory = tempfile::tempdir().unwrap();
    let database = NativeSql::open(directory.path().join("history-plans.db"))
        .await
        .unwrap();
    crate::migration::migrate(&database, Dialect::NativeSqlite)
        .await
        .unwrap();
    // Populate several credentials and windows so the optimizer chooses from
    // real selectivity instead of treating every empty-table index as equal.
    database.execute(Statement::plain(
        "WITH RECURSIVE sequence(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM sequence WHERE id<4096)
         INSERT INTO credential_quota_cycles(id,accounting_start_ms,tracking_json,version,credential_id,window_key,boundary_source,boundary_confidence,status,last_observed_at,coverage,metrics_json)
         SELECT id,0,'{}',1,(id%16)+1,'window-'||(id%17),'upstream','exact','closed',id/4,'partial_lower_bound','{}' FROM sequence",
    )).await.unwrap();
    database.execute(Statement::plain(
        "WITH RECURSIVE sequence(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM sequence WHERE id<16)
         INSERT INTO credentials(id,provider_id,ciphertext,wrapped_key,payload_nonce,key_nonce,version,enabled)
         SELECT id,(id%2)+1,x'00',x'00',x'00',x'00',1,1 FROM sequence",
    )).await.unwrap();
    database.execute(Statement::plain("ANALYZE")).await.unwrap();
    for (credential_id, provider_id, window_key, index) in [
        (None, None, None, INDEXES[0].0),
        (Some(7), None, None, INDEXES[1].0),
        (Some(7), None, Some("window-3".to_owned()), INDEXES[2].0),
        (None, Some(1), None, INDEXES[0].0),
        (None, None, Some("window-3".to_owned()), INDEXES[3].0),
        (None, Some(1), Some("window-3".to_owned()), INDEXES[3].0),
    ] {
        let filter = CredentialQuotaCyclePageQuery {
            credential_id,
            provider_id,
            window_key,
            from: 1,
            to: 1_000,
            cursor: Some(CredentialQuotaCycleCursor {
                last_observed_at: 500,
                id: 80,
            }),
            limit: 10,
        };
        let query =
            crate::query::runtime::select_credential_quota_cycle_page(&filter, filter.limit)
                .unwrap();
        let plan = database
            .execute(Statement::with_args(
                format!("EXPLAIN QUERY PLAN {}", query.sql),
                query.args,
            ))
            .await
            .unwrap();
        let details = plan
            .rows
            .iter()
            .map(|row| row.text("detail").unwrap())
            .collect::<Vec<_>>();
        assert!(
            details.iter().any(|detail| detail.contains(index)),
            "{details:?}"
        );
        // The outer hydration may sort its bounded page. The co-routine which
        // selects that page must reach its limit directly through the index.
        let payload_lookup = details
            .iter()
            .position(|detail| detail.contains("INTEGER PRIMARY KEY"))
            .unwrap();
        assert!(
            !details[..payload_lookup]
                .iter()
                .any(|detail| detail.contains("USE TEMP B-TREE")),
            "{details:?}"
        );
    }
}
