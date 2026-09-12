use super::super::{Executor, Statement, native::NativeSql};
use super::{libsql_store, native_store};
use crate::records::{CredentialHealthInput, CredentialHealthRecord, CredentialHealthState};
use crate::schema::{Dialect, SchemaVersion};
use crate::{Store, StoreError};

fn observation(version: i64, state: CredentialHealthState) -> CredentialHealthInput {
    CredentialHealthInput {
        credential_id: 901,
        model: "model-a".into(),
        credential_version: 3,
        version,
        state,
        observed_at: 1_000 + version,
        response_status: Some(if state == CredentialHealthState::Healthy {
            200
        } else {
            503
        }),
        detail: (state != CredentialHealthState::Healthy)
            .then(|| format!("server_is_overloaded:{version}")),
    }
}

async fn write(store: &Store, input: &CredentialHealthInput) -> CredentialHealthRecord {
    store.record_credential_health(input).await.unwrap();
    store
        .credential_model_health(input.credential_id, &input.model)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn native_and_libsql_credential_health_backoff_and_ordering() {
    let directory = tempfile::tempdir().unwrap();
    let (native, _) = native_store(directory.path().join("native.db"))
        .await
        .unwrap();
    let (libsql, _) = libsql_store(directory.path().join("libsql.db"))
        .await
        .unwrap();
    for store in [native, libsql] {
        assert_backoff_and_ordering(&store).await;
    }
}

async fn assert_backoff_and_ordering(store: &Store) {
    use CredentialHealthState::{Dead, Degraded, Healthy};

    store.clear_credential_health(901).await.unwrap();
    store.clear_credential_health(902).await.unwrap();
    assert!(
        store
            .credential_model_health(901, "model-a")
            .await
            .unwrap()
            .is_none()
    );
    let first = observation(10, Degraded);
    let row = write(store, &first).await;
    assert_eq!(row.consecutive_failures, 1);
    assert_eq!(write(store, &first).await, row, "replay must be idempotent");
    let second = observation(11, Degraded);
    let row = write(store, &second).await;
    assert_eq!(row.consecutive_failures, 2);
    assert_eq!(row.observed_at, second.observed_at);
    assert_eq!(row.detail, second.detail);
    assert_eq!(write(store, &second).await, row);
    assert_eq!(write(store, &observation(10, Healthy)).await, row);
    assert_eq!(write(store, &observation(11, Healthy)).await, row);
    let stale_credential = CredentialHealthInput {
        credential_version: 2,
        ..observation(20_000, Healthy)
    };
    assert_eq!(write(store, &stale_credential).await, row);

    // A new credential generation starts a new sequence, even when its first
    // request version is less than the version from the previous generation.
    let new_credential = CredentialHealthInput {
        credential_version: 4,
        ..observation(1, Degraded)
    };
    let row = write(store, &new_credential).await;
    assert_eq!(row.credential_version, 4);
    assert_eq!(row.version, 1);
    assert_eq!(row.consecutive_failures, 1);
    assert_eq!(row.observed_at, new_credential.observed_at);
    assert_eq!(row.detail, new_credential.detail);
    assert_eq!(write(store, &second).await, row);

    let next = |version, state| CredentialHealthInput {
        credential_version: 4,
        ..observation(version, state)
    };
    assert_eq!(
        write(store, &next(2, Degraded)).await.consecutive_failures,
        2
    );
    let healthy = write(store, &next(3, Healthy)).await;
    assert_eq!(healthy.consecutive_failures, 0);
    assert_eq!(healthy.state, Healthy);
    assert_eq!(healthy.response_status, Some(200));
    assert_eq!(healthy.detail, None);
    assert_eq!(
        write(store, &next(4, Degraded)).await.consecutive_failures,
        1
    );
    assert_eq!(write(store, &next(5, Dead)).await.consecutive_failures, 0);
    assert_eq!(
        write(store, &next(6, Degraded)).await.consecutive_failures,
        1
    );
    for version in 7..=40 {
        let row = write(store, &next(version, Degraded)).await;
        assert_eq!(row.consecutive_failures, ((version - 5) as u32).min(16));
    }
    let original = store
        .credential_model_health(901, "model-a")
        .await
        .unwrap()
        .unwrap();
    for input in [
        CredentialHealthInput {
            model: "model-b".into(),
            ..observation(1, Healthy)
        },
        CredentialHealthInput {
            credential_id: 902,
            ..observation(1, Healthy)
        },
        CredentialHealthInput {
            model: "*".into(),
            ..observation(1, Dead)
        },
    ] {
        let other = write(store, &input).await;
        assert_eq!(other.consecutive_failures, 0);
    }
    assert_eq!(
        store
            .credential_model_health(901, "model-a")
            .await
            .unwrap()
            .unwrap(),
        original,
        "other account, model and account-wide records must remain independent"
    );
    assert!(store.credential_health().await.unwrap().contains(&original));
}

#[tokio::test]
async fn native_and_libsql_conditional_health_recovery_preserves_newer_evidence_and_resets() {
    let directory = tempfile::tempdir().unwrap();
    let (native, _) = native_store(directory.path().join("native.db"))
        .await
        .unwrap();
    let (libsql, _) = libsql_store(directory.path().join("libsql.db"))
        .await
        .unwrap();
    for store in [native, libsql] {
        assert_conditional_recovery(&store).await;
    }
}

async fn assert_conditional_recovery(store: &Store) {
    use CredentialHealthState::{Dead, Degraded, Healthy};

    let account = |version, state| CredentialHealthInput {
        model: "*".into(),
        ..observation(version, state)
    };
    store.clear_credential_health(901).await.unwrap();
    store.clear_credential_health(902).await.unwrap();
    write(store, &account(10, Degraded)).await;
    let row = write(store, &account(11, Degraded)).await;
    assert_eq!(row.consecutive_failures, 2);
    for input in [account(10, Healthy), account(11, Healthy)] {
        store
            .recover_degraded_credential_health(&input)
            .await
            .unwrap();
        assert_eq!(
            store.credential_model_health(901, "*").await.unwrap(),
            Some(row.clone()),
            "late or replayed success cannot clear more recent degradation"
        );
    }
    let other_model = write(store, &observation(11, Degraded)).await;
    let other_account = write(
        store,
        &CredentialHealthInput {
            credential_id: 902,
            ..account(11, Degraded)
        },
    )
    .await;
    let success = account(12, Healthy);
    store
        .recover_degraded_credential_health(&success)
        .await
        .unwrap();
    let recovered = store
        .credential_model_health(901, "*")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.state, Healthy);
    assert_eq!(recovered.consecutive_failures, 0);
    assert_eq!(recovered.version, success.version);
    assert_eq!(recovered.observed_at, success.observed_at);
    assert_eq!(recovered.response_status, success.response_status);
    assert_eq!(recovered.detail, success.detail);
    assert_eq!(
        store.credential_model_health(901, "model-a").await.unwrap(),
        Some(other_model)
    );
    assert_eq!(
        store.credential_model_health(902, "*").await.unwrap(),
        Some(other_account)
    );
    store
        .recover_degraded_credential_health(&account(13, Healthy))
        .await
        .unwrap();
    assert_eq!(
        store.credential_model_health(901, "*").await.unwrap(),
        Some(recovered),
        "repeated success does not refresh a row that is already healthy"
    );

    let dead = write(store, &account(14, Dead)).await;
    store
        .recover_degraded_credential_health(&account(100, Healthy))
        .await
        .unwrap();
    assert_eq!(
        store.credential_model_health(901, "*").await.unwrap(),
        Some(dead)
    );

    let rotated = CredentialHealthInput {
        credential_version: 4,
        ..account(1, Degraded)
    };
    let row = write(store, &rotated).await;
    store
        .recover_degraded_credential_health(&account(100, Healthy))
        .await
        .unwrap();
    assert_eq!(
        store.credential_model_health(901, "*").await.unwrap(),
        Some(row.clone()),
        "success from an old credential generation cannot recover the new generation"
    );
    store
        .recover_degraded_credential_health(&CredentialHealthInput {
            credential_version: 5,
            ..account(100, Healthy)
        })
        .await
        .unwrap();
    assert_eq!(
        store.credential_model_health(901, "*").await.unwrap(),
        Some(row.clone()),
        "recovery applies only within the same credential generation"
    );
    for state in [Degraded, Dead] {
        let invalid = CredentialHealthInput {
            credential_version: 4,
            ..account(100, state)
        };
        assert!(matches!(
            store.recover_degraded_credential_health(&invalid).await,
            Err(StoreError::InvalidData { .. })
        ));
        assert_eq!(
            store.credential_model_health(901, "*").await.unwrap(),
            Some(row.clone())
        );
    }
    store.clear_credential_model_health(901, "*").await.unwrap();
    store
        .recover_degraded_credential_health(&CredentialHealthInput {
            credential_version: 4,
            ..account(100, Healthy)
        })
        .await
        .unwrap();
    assert!(
        store
            .credential_model_health(901, "*")
            .await
            .unwrap()
            .is_none(),
        "a reset must not be undone by a late recovery update"
    );
}

#[tokio::test]
async fn concurrent_credential_health_replay_counts_once_and_survives_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("concurrent.db");
    let (store, database) = native_store(path.clone()).await.unwrap();
    let input = observation(10, CredentialHealthState::Degraded);
    let outcomes =
        futures_util::future::join_all((0..16).map(|_| store.record_credential_health(&input)))
            .await;
    for result in outcomes {
        result.unwrap();
    }
    drop(store);
    drop(database);
    let (store, _) = native_store(path).await.unwrap();
    let row = store
        .credential_model_health(input.credential_id, &input.model)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.consecutive_failures, 1);
    assert_eq!(row.observed_at, input.observed_at);
    assert_eq!(row.version, input.version);
}

#[tokio::test]
async fn credential_health_backoff_upgrade_preserves_existing_rows_and_retries_ddl() {
    for dialect in [Dialect::NativeSqlite, Dialect::Libsql] {
        for ddl_already_applied in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let database = NativeSql::open(directory.path().join("upgrade.db"))
                .await
                .unwrap();
            crate::migration::migrate_to(&database, dialect, SchemaVersion::QuotaHistoryIndex)
                .await
                .unwrap();
            database
                .execute(Statement::plain(
                    "INSERT INTO credential_health(credential_id,model,credential_version,version,state,observed_at,response_status,detail) VALUES(901,'model-a',3,10,'degraded',1000,503,'server_is_overloaded')",
                ))
                .await
                .unwrap();
            if ddl_already_applied {
                for statement in crate::schema::migration_statements(
                    SchemaVersion::CredentialHealthBackoff,
                    dialect,
                ) {
                    database.execute(Statement::plain(statement)).await.unwrap();
                }
                database
                    .execute(Statement::plain(
                        "UPDATE credential_health SET consecutive_failures=4",
                    ))
                    .await
                    .unwrap();
            }
            crate::migration::migrate(&database, dialect).await.unwrap();
            crate::migration::migrate(&database, dialect).await.unwrap();
            let row = database
                .execute(Statement::plain(
                    "SELECT * FROM credential_health WHERE credential_id=901 AND model='model-a'",
                ))
                .await
                .unwrap()
                .rows
                .pop()
                .unwrap();
            assert_eq!(
                row.i64("consecutive_failures").unwrap(),
                if ddl_already_applied { 4 } else { 0 }
            );
            assert_eq!(row.i64("credential_version").unwrap(), 3);
            assert_eq!(row.i64("version").unwrap(), 10);
            assert_eq!(row.text("state").unwrap(), "degraded");
            assert_eq!(row.i64("observed_at").unwrap(), 1000);
            assert_eq!(row.i64("response_status").unwrap(), 503);
            assert_eq!(row.text("detail").unwrap(), "server_is_overloaded");
            let versions = database
                .execute(Statement::plain(
                    "SELECT version FROM schema_migrations ORDER BY version",
                ))
                .await
                .unwrap()
                .rows
                .into_iter()
                .map(|row| row.i64("version").unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                versions,
                (1..=SchemaVersion::LATEST.number()).collect::<Vec<_>>()
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires a PostgreSQL database via GPROXY_TEST_POSTGRES_DSN"]
async fn postgres_credential_health_backoff_and_ordering() -> Result<(), StoreError> {
    let store = Store::open(crate::BackendConfig::Postgres {
        dsn: std::env::var("GPROXY_TEST_POSTGRES_DSN").expect("GPROXY_TEST_POSTGRES_DSN"),
        pool_size: 8,
        checkout_timeout: std::time::Duration::from_secs(5),
    })
    .await?;
    assert_backoff_and_ordering(&store).await;
    assert_conditional_recovery(&store).await;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a MySQL database via GPROXY_TEST_MYSQL_DSN"]
async fn mysql_credential_health_backoff_and_ordering() -> Result<(), StoreError> {
    let store = Store::open(crate::BackendConfig::Mysql {
        dsn: std::env::var("GPROXY_TEST_MYSQL_DSN").expect("GPROXY_TEST_MYSQL_DSN"),
    })
    .await?;
    assert_backoff_and_ordering(&store).await;
    assert_conditional_recovery(&store).await;
    Ok(())
}
