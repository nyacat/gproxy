mod cache;
mod credential_budget;
mod cycle_reads;
mod oauth_migration;
mod parity;
mod quota_activity;
mod quota_contention;
mod quota_migration;
mod quota_observation_version;
mod scenario;
mod sender;
mod settlement_recovery;
mod usage_records;
mod usage_summary;

use std::sync::Arc;

use super::libsql::LibsqlHttp;
use super::native::NativeSql;
use super::{Executor, SharedExecutor};
use crate::migration;
use crate::schema::Dialect;
use crate::{Store, StoreError};

async fn native_store(path: std::path::PathBuf) -> Result<(Store, Arc<NativeSql>), StoreError> {
    let executor = Arc::new(NativeSql::open(path).await?);
    migration::migrate(executor.as_ref(), Dialect::NativeSqlite).await?;
    Ok((store(executor.clone()), executor))
}

async fn libsql_store(path: std::path::PathBuf) -> Result<(Store, Arc<NativeSql>), StoreError> {
    let database = Arc::new(NativeSql::open(path).await?);
    let executor = Arc::new(LibsqlHttp::with_sender(
        "https://store.invalid".into(),
        "test-token".into(),
        sender::SqliteHrana::new(database.clone()),
    ));
    migration::migrate(executor.as_ref(), Dialect::Libsql).await?;
    Ok((store(executor), database))
}

fn store(executor: Arc<impl Executor + 'static>) -> Store {
    let executor: SharedExecutor = executor;
    Store {
        executor,
        dialect: Dialect::NativeSqlite,
        quota_window_locks: Default::default(),
    }
}
