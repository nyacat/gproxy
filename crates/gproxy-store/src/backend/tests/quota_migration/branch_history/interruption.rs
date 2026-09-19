use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::{
    StoreError,
    backend::{DbFuture, Executor, QueryResult, SharedExecutor, Statement, native::NativeSql},
    schema::{Dialect, SchemaVersion},
};

#[derive(Clone, Copy)]
enum Failure {
    Rollback,
    LostAcknowledgement,
}

struct Interrupted {
    inner: SharedExecutor,
    batches: AtomicUsize,
    fail_at: usize,
    failure: Failure,
}

impl Executor for Interrupted {
    fn execute<'a>(&'a self, statement: Statement) -> DbFuture<'a, QueryResult> {
        self.inner.execute(statement)
    }

    fn batch<'a>(&'a self, mut statements: Vec<Statement>) -> DbFuture<'a, Vec<QueryResult>> {
        Box::pin(async move {
            if self.batches.fetch_add(1, Ordering::SeqCst) + 1 != self.fail_at {
                return self.inner.batch(statements).await;
            }
            match self.failure {
                Failure::Rollback => {
                    // Fail after the DDL and backfills, immediately before the
                    // final statement (the version marker on canonical steps).
                    let at = statements.len().saturating_sub(1);
                    statements.insert(at, Statement::plain("SELECT * FROM absent_migration_fault"));
                    self.inner.batch(statements).await
                }
                Failure::LostAcknowledgement => {
                    self.inner.batch(statements).await?;
                    Err(StoreError::Database(
                        "migration response was lost after commit".into(),
                    ))
                }
            }
        })
    }
}

#[tokio::test]
async fn interrupted_branch_upgrades_preserve_data_across_retry() {
    for remote in [false, true] {
        for version in 10..=12 {
            // Missing upstream snapshots are repaired first, then only the
            // canonical steps after the old numeric marker are applied.
            let batches = 1 + (SchemaVersion::LATEST.number() - version) as usize;
            for fail_at in 1..=batches {
                for failure in [Failure::Rollback, Failure::LostAcknowledgement] {
                    let directory = tempfile::tempdir().unwrap();
                    let database = Arc::new(
                        NativeSql::open(directory.path().join("interrupted.db"))
                            .await
                            .unwrap(),
                    );
                    super::seed_self(database.as_ref(), Dialect::NativeSqlite, version).await;
                    let (inner, dialect): (SharedExecutor, _) = if remote {
                        (
                            Arc::new(crate::backend::libsql::LibsqlHttp::with_sender(
                                "https://store.invalid".into(),
                                "test-token".into(),
                                super::super::super::sender::SqliteHrana::new(database.clone()),
                            )),
                            Dialect::Libsql,
                        )
                    } else {
                        (database.clone(), Dialect::NativeSqlite)
                    };
                    let interrupted = Interrupted {
                        inner: inner.clone(),
                        batches: AtomicUsize::new(0),
                        fail_at,
                        failure,
                    };
                    assert!(
                        crate::migration::migrate(&interrupted, dialect)
                            .await
                            .is_err()
                    );
                    assert_eq!(interrupted.batches.load(Ordering::SeqCst), fail_at);
                    crate::migration::migrate(inner.as_ref(), dialect)
                        .await
                        .unwrap();
                    super::verify_self(database.as_ref(), version).await;

                    // A completed version is an authoritative checkpoint.
                    // Reopening must not replay any data migration batch.
                    let completed = Interrupted {
                        inner,
                        batches: AtomicUsize::new(0),
                        fail_at: 1,
                        failure: Failure::Rollback,
                    };
                    crate::migration::migrate(&completed, dialect)
                        .await
                        .unwrap();
                    assert_eq!(completed.batches.load(Ordering::SeqCst), 0);
                    super::verify_self(database.as_ref(), version).await;
                }
            }
        }
    }
}
