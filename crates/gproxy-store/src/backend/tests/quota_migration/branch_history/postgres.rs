use crate::backend::{SharedExecutor, Statement};
use crate::schema::{Dialect, SchemaVersion};

async fn executor() -> SharedExecutor {
    crate::backend::open(crate::BackendConfig::Postgres {
        dsn: std::env::var("GPROXY_TEST_POSTGRES_DSN").expect("GPROXY_TEST_POSTGRES_DSN"),
        pool_size: 4,
        checkout_timeout: std::time::Duration::from_secs(5),
    })
    .await
    .unwrap()
}

async fn upgrade_self(version: i64, partial_snapshots: bool) {
    let executor = executor().await;
    super::seed_self(executor.as_ref(), Dialect::Postgres, version).await;
    if partial_snapshots {
        let ddl =
            crate::schema::migration_statements(SchemaVersion::QuotaSnapshots, Dialect::Postgres);
        executor
            .execute(Statement::plain(ddl[0].clone()))
            .await
            .unwrap();
        super::execute(executor.as_ref(), &[
            "INSERT INTO credential_quota_sources(credential_id,source_id,capability_json,entries_json) VALUES(7,'quota','{\"keep\":1}','[]')",
        ]).await;
    }
    for _ in 0..2 {
        crate::migration::migrate(executor.as_ref(), Dialect::Postgres)
            .await
            .unwrap();
        super::verify_self(executor.as_ref(), version).await;
        if partial_snapshots {
            let rows = executor
                .execute(Statement::plain(
                    "SELECT capability_json FROM credential_quota_sources",
                ))
                .await
                .unwrap()
                .rows;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].text("capability_json").unwrap(), "{\"keep\":1}");
        }
    }
}

#[tokio::test]
#[ignore = "requires an empty PostgreSQL database via GPROXY_TEST_POSTGRES_DSN"]
async fn postgres_self_v10_upgrade() {
    upgrade_self(10, false).await;
}

#[tokio::test]
#[ignore = "requires an empty PostgreSQL database via GPROXY_TEST_POSTGRES_DSN"]
async fn postgres_self_v11_upgrade() {
    upgrade_self(11, false).await;
}

#[tokio::test]
#[ignore = "requires an empty PostgreSQL database via GPROXY_TEST_POSTGRES_DSN"]
async fn postgres_self_v12_upgrade() {
    upgrade_self(12, false).await;
}

#[tokio::test]
#[ignore = "requires an empty PostgreSQL database via GPROXY_TEST_POSTGRES_DSN"]
async fn postgres_self_v12_retries_partial_snapshot_reconciliation() {
    upgrade_self(12, true).await;
}

#[tokio::test]
#[ignore = "requires an empty PostgreSQL database via GPROXY_TEST_POSTGRES_DSN"]
async fn postgres_main_v10_upgrade_preserves_snapshots() {
    let executor = executor().await;
    crate::migration::migrate_to(
        executor.as_ref(),
        Dialect::Postgres,
        SchemaVersion::QuotaSnapshots,
    )
    .await
    .unwrap();
    super::seed_activity(executor.as_ref()).await;
    super::execute(executor.as_ref(), &[
        "INSERT INTO credential_quota_sources(credential_id,source_id,capability_json,entries_json) VALUES(7,'quota','{\"keep\":1}','[]')",
        "INSERT INTO credential_quota_response_entries(credential_id,source_id,entry_id,observed_at_ms,entry_json) VALUES(7,'headers','tokens',100,'{\"keep\":2}')",
    ]).await;
    for _ in 0..2 {
        crate::migration::migrate(executor.as_ref(), Dialect::Postgres)
            .await
            .unwrap();
        super::verify_activity(executor.as_ref(), false).await;
        let sources = executor
            .execute(Statement::plain(
                "SELECT capability_json FROM credential_quota_sources",
            ))
            .await
            .unwrap()
            .rows;
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].text("capability_json").unwrap(), "{\"keep\":1}");
        let entries = executor
            .execute(Statement::plain(
                "SELECT entry_json FROM credential_quota_response_entries",
            ))
            .await
            .unwrap()
            .rows;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].text("entry_json").unwrap(), "{\"keep\":2}");
    }
}
