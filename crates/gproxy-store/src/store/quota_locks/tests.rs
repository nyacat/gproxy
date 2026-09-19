use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::{future::join_all, poll};
use rust_decimal::Decimal;

use super::*;
use crate::Store;
use crate::backend::{DbFuture, DbValue, Executor, QueryResult, Row, Statement};
use crate::schema::Dialect;

#[derive(Default)]
struct Window {
    cost: Mutex<Decimal>,
    readers: AtomicUsize,
    max_readers: AtomicUsize,
}

#[derive(Default)]
struct ContendedExecutor {
    windows: [Window; 2],
    readers: AtomicUsize,
    max_readers: AtomicUsize,
    conflicts: AtomicUsize,
}

struct Reader<'a>(&'a AtomicUsize, &'a AtomicUsize);

impl Drop for Reader<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
        self.1.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Executor for ContendedExecutor {
    fn execute<'a>(&'a self, statement: Statement) -> DbFuture<'a, QueryResult> {
        Box::pin(async move {
            if statement.sql.contains("quota_settlements") {
                return Ok(QueryResult::default());
            }
            let DbValue::Integer(id) = statement.args[0] else {
                panic!("window id")
            };
            let window = &self.windows[(id - 1) as usize];
            let active = window.readers.fetch_add(1, Ordering::SeqCst) + 1;
            window.max_readers.fetch_max(active, Ordering::SeqCst);
            let active = self.readers.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_readers.fetch_max(active, Ordering::SeqCst);
            let _reader = Reader(&window.readers, &self.readers);
            let cost = *window.cost.lock().unwrap();
            // Every contender yields after its snapshot. Without the Store
            // coordinator all calls read the same cost before CAS can commit.
            tokio::task::yield_now().await;
            Ok(QueryResult {
                rows: vec![Row::new([
                    ("id".into(), DbValue::Integer(id)),
                    ("quota_id".into(), DbValue::Integer(1)),
                    ("window_kind".into(), DbValue::Text("total".into())),
                    ("window_start".into(), DbValue::Integer(0)),
                    ("reset_at".into(), DbValue::Null),
                    ("cost_used".into(), DbValue::Text(cost.to_string())),
                ])],
                ..Default::default()
            })
        })
    }

    fn batch<'a>(&'a self, statements: Vec<Statement>) -> DbFuture<'a, Vec<QueryResult>> {
        Box::pin(async move {
            let [
                DbValue::Text(next),
                DbValue::Integer(id),
                DbValue::Text(expected),
            ] = statements[0].args.as_slice()
            else {
                panic!("window CAS")
            };
            let mut cost = self.windows[(*id - 1) as usize].cost.lock().unwrap();
            let affected_rows = if *cost == expected.parse::<Decimal>().unwrap() {
                *cost = next.parse().unwrap();
                1
            } else {
                self.conflicts.fetch_add(1, Ordering::SeqCst);
                0
            };
            Ok(vec![
                QueryResult {
                    affected_rows,
                    ..Default::default()
                };
                2
            ])
        })
    }
}

fn store() -> (Store, Arc<ContendedExecutor>) {
    let executor = Arc::new(ContendedExecutor::default());
    (
        Store {
            executor: executor.clone(),
            dialect: Dialect::NativeSqlite,
            quota_window_locks: Default::default(),
        },
        executor,
    )
}

#[tokio::test]
async fn same_window_snapshots_do_not_race_across_store_clones() {
    let (store, executor) = store();
    let results = join_all((0..100).map(|id| {
        let store = store.clone();
        async move {
            store
                .add_quota_cost(&format!("request-{id}"), 1, Decimal::ONE)
                .await
        }
    }))
    .await;
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(
        *executor.windows[0].cost.lock().unwrap(),
        Decimal::from(100)
    );
    assert_eq!(executor.windows[0].max_readers.load(Ordering::SeqCst), 1);
    assert_eq!(executor.conflicts.load(Ordering::SeqCst), 0);
    assert!(store.quota_window_locks.windows.lock().unwrap().is_empty());
}

#[tokio::test]
async fn different_windows_can_read_concurrently() {
    let (store, executor) = store();
    let other = store.clone();
    let (left, right) = futures_util::join!(
        store.add_quota_cost("left", 1, Decimal::ONE),
        other.add_quota_cost("right", 2, Decimal::ONE),
    );
    left.unwrap();
    right.unwrap();
    assert_eq!(executor.max_readers.load(Ordering::SeqCst), 2);
    assert!(store.quota_window_locks.windows.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelling_waiting_and_active_updates_releases_locks_and_index() {
    let (store, executor) = store();
    let mut owner = Box::pin(store.add_quota_cost("owner", 1, Decimal::ONE));
    assert!(poll!(&mut owner).is_pending());
    let mut waiter = Box::pin(store.add_quota_cost("waiter", 1, Decimal::ONE));
    assert!(poll!(&mut waiter).is_pending());
    assert_eq!(executor.windows[0].readers.load(Ordering::SeqCst), 1);
    drop(waiter);
    assert_eq!(store.quota_window_locks.windows.lock().unwrap().len(), 1);
    drop(owner);
    assert_eq!(executor.windows[0].readers.load(Ordering::SeqCst), 0);
    assert!(store.quota_window_locks.windows.lock().unwrap().is_empty());
    store
        .add_quota_cost("replacement", 1, Decimal::ONE)
        .await
        .unwrap();
    assert!(store.quota_window_locks.windows.lock().unwrap().is_empty());
    assert_eq!(*executor.windows[0].cost.lock().unwrap(), Decimal::ONE);
}

#[tokio::test]
async fn visiting_many_windows_does_not_retain_dead_index_entries() {
    let locks = QuotaWindowLocks::default();
    for id in 0..1_000 {
        let window = locks.window(id);
        let _guard = window.lock().await;
    }
    assert!(locks.windows.lock().unwrap().is_empty());
}
