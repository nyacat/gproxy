use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tokio_postgres::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use tokio_postgres::{GenericClient, NoTls};

use super::{DbValue, Executor, QueryResult, Row, Statement};
use crate::StoreError;
use crate::schema::Dialect;

pub(super) const DEFAULT_POOL_SIZE: usize = 32;
pub(super) const MIN_POOL_SIZE: usize = 8;
pub(super) const DEFAULT_CHECKOUT_TIMEOUT: Duration = Duration::from_secs(5);
const STATEMENT_CACHE_CAPACITY: usize = 256;

#[derive(Default)]
struct StatementCache {
    entries: HashMap<String, tokio_postgres::Statement>,
    order: VecDeque<String>,
}

impl StatementCache {
    fn get(&self, sql: &str) -> Option<&tokio_postgres::Statement> {
        self.entries.get(sql)
    }

    fn insert(&mut self, sql: String, prepared: tokio_postgres::Statement) {
        // FIFO eviction keeps hits allocation-free and bounds both local
        // statement handles and prepared statements on each server connection.
        if self.entries.len() == STATEMENT_CACHE_CAPACITY {
            let oldest = self.order.pop_front().expect("full statement cache");
            self.entries.remove(&oldest);
        }
        self.order.push_back(sql.clone());
        self.entries.insert(sql, prepared);
    }
}

const _: () = assert!(DEFAULT_POOL_SIZE >= MIN_POOL_SIZE);
const _: () = assert!(DEFAULT_CHECKOUT_TIMEOUT.as_millis() > 0);

pub(super) fn clamp_pool_size(size: usize) -> usize {
    size.max(MIN_POOL_SIZE)
}

struct PooledConn {
    client: tokio_postgres::Client,
    statements: StatementCache,
    connection_task: tokio::task::JoinHandle<()>,
}

impl PooledConn {
    fn new(client: tokio_postgres::Client, connection_task: tokio::task::JoinHandle<()>) -> Self {
        Self {
            client,
            statements: StatementCache::default(),
            connection_task,
        }
    }
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

struct Inner {
    idle: std::sync::Mutex<Vec<PooledConn>>,
    capacity: Arc<tokio::sync::Semaphore>,
    dsn: String,
    checkout_timeout: Duration,
}

// A lease owns the capacity permit as well as the connection. Dropping a query
// future (including during a transaction) always returns both to the pool.
struct Lease {
    conn: Option<PooledConn>,
    reusable: bool,
    inner: Arc<Inner>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl std::ops::Deref for Lease {
    type Target = PooledConn;

    fn deref(&self) -> &PooledConn {
        self.conn.as_ref().expect("live lease")
    }
}

impl std::ops::DerefMut for Lease {
    fn deref_mut(&mut self) -> &mut PooledConn {
        self.conn.as_mut().expect("live lease")
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            if self.reusable && !conn.client.is_closed() {
                self.inner.idle.lock().expect("postgres pool").push(conn);
            } else {
                // A cancelled query can still be running on the server. Never
                // lend that connection to another caller; cancel it and close
                // its driver, while making the pool slot immediately reusable.
                let cancel = conn.client.cancel_token();
                drop(conn);
                if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                    runtime.spawn(async move {
                        let _ = tokio::time::timeout(
                            DEFAULT_CHECKOUT_TIMEOUT,
                            cancel.cancel_query(NoTls),
                        )
                        .await;
                    });
                }
            }
        }
        // The permit is released after the connection is returned or closed.
    }
}

pub(super) struct Postgres {
    inner: Arc<Inner>,
}

impl Postgres {
    pub(super) async fn connect(
        dsn: &str,
        pool_size: usize,
        checkout_timeout: Duration,
    ) -> Result<Self, StoreError> {
        let pool_size = clamp_pool_size(pool_size);
        let first = tokio::time::timeout(checkout_timeout, connect_one(dsn))
            .await
            .map_err(|_| self::checkout_timeout())??;
        Ok(Self {
            inner: Arc::new(Inner {
                idle: std::sync::Mutex::new(vec![first]),
                capacity: Arc::new(tokio::sync::Semaphore::new(pool_size)),
                dsn: dsn.to_owned(),
                checkout_timeout,
            }),
        })
    }

    async fn checkout(&self) -> Result<Lease, StoreError> {
        let started = web_time::Instant::now();
        let result = tokio::time::timeout(self.inner.checkout_timeout, async {
            let permit = Arc::clone(&self.inner.capacity)
                .acquire_owned()
                .await
                .map_err(|_| StoreError::Database("PostgreSQL pool closed".into()))?;
            let idle = {
                let mut idle = self.inner.idle.lock().expect("postgres pool");
                std::iter::from_fn(|| idle.pop()).find(|conn| !conn.client.is_closed())
            };
            let conn = match idle {
                Some(conn) => conn,
                None => connect_one(&self.inner.dsn).await?,
            };
            Ok(Lease {
                conn: Some(conn),
                reusable: false,
                inner: Arc::clone(&self.inner),
                _permit: permit,
            })
        })
        .await
        .unwrap_or_else(|_| Err(checkout_timeout()));
        let checkout_ms = started.elapsed().as_millis() as u64;
        if checkout_ms >= 100 || result.is_err() {
            tracing::warn!(checkout_ms, success = result.is_ok(), "postgres.checkout");
        } else {
            tracing::debug!(checkout_ms, "postgres.checkout");
        }
        result
    }
}

fn checkout_timeout() -> StoreError {
    StoreError::Database("PostgreSQL connection pool checkout timed out".into())
}

async fn connect_one(dsn: &str) -> Result<PooledConn, StoreError> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .map_err(|_| StoreError::Database("PostgreSQL connection failed".into()))?;
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(PooledConn::new(client, connection_task))
}

impl Executor for Postgres {
    fn execute<'a>(&'a self, statement: Statement) -> super::DbFuture<'a, QueryResult> {
        Box::pin(async move {
            let mut conn = self.checkout().await?;
            let result = {
                let PooledConn {
                    client,
                    statements: cache,
                    ..
                } = &mut *conn;
                run(cache, client, statement, None).await
            };
            conn.settle(result)
        })
    }

    fn batch<'a>(&'a self, statements: Vec<Statement>) -> super::DbFuture<'a, Vec<QueryResult>> {
        Box::pin(async move {
            let mut conn = self.checkout().await?;
            let result = async {
                let PooledConn {
                    client,
                    statements: cache,
                    ..
                } = &mut *conn;
                let transaction = client.transaction().await.map_err(failure)?;
                let mut results = Vec::with_capacity(statements.len());
                for statement in statements {
                    let changes = results
                        .last()
                        .map(|result: &QueryResult| result.affected_rows);
                    results.push(run(cache, &transaction, statement, changes).await?);
                }
                // Dropping an aborted transaction queues its ROLLBACK ahead of
                // whatever the next caller sends, so the connection survives a
                // rejected statement here too.
                transaction.commit().await.map_err(failure)?;
                Ok(results)
            }
            .await;
            conn.settle(result)
        })
    }
}

/// A statement the server rejected and a connection the server lost are not the
/// same failure. The first leaves the session in a known state and the second
/// does not, so only the second may discard the connection: treating every
/// error alike turns one burst of rejected statements into a full pool
/// reconnect, and throws away every prepared statement those connections held.
struct Failure {
    error: StoreError,
    reusable: bool,
}

impl From<StoreError> for Failure {
    // Failures raised after the server answered leave nothing in flight.
    fn from(error: StoreError) -> Self {
        Self {
            error,
            reusable: true,
        }
    }
}

impl Lease {
    fn settle<T>(&mut self, result: Result<T, Failure>) -> Result<T, StoreError> {
        match result {
            Ok(value) => {
                self.reusable = true;
                Ok(value)
            }
            Err(failure) => {
                self.reusable = failure.reusable;
                Err(failure.error)
            }
        }
    }
}

async fn run(
    cache: &mut StatementCache,
    client: &impl GenericClient,
    statement: Statement,
    changes: Option<u64>,
) -> Result<QueryResult, Failure> {
    let sql = replace_changes(statement.sql_for(Dialect::Postgres), changes);
    let cache_hit = cache.get(&sql).is_some();
    let started = web_time::Instant::now();
    let result = run_query(cache, client, statement, &sql).await;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let rows = result.as_ref().map_or(0, |result| result.rows.len());
    // Identifying the statement costs a hash of the whole text, so leave it to
    // the logging macros: they skip their arguments when nothing is listening.
    if elapsed_ms >= 200 {
        tracing::warn!(
            query_id = query_id(&sql),
            elapsed_ms,
            rows,
            cache_hit,
            success = result.is_ok(),
            "postgres.query"
        );
    } else {
        tracing::debug!(
            query_id = query_id(&sql),
            elapsed_ms,
            rows,
            cache_hit,
            success = result.is_ok(),
            "postgres.query"
        );
    }
    result
}

fn query_id(sql: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sql.hash(&mut hasher);
    hasher.finish()
}

async fn run_query(
    cache: &mut StatementCache,
    client: &impl GenericClient,
    statement: Statement,
    sql: &str,
) -> Result<QueryResult, Failure> {
    let values = statement
        .args
        .into_iter()
        .map(PgValue::from)
        .collect::<Vec<_>>();
    let parameters = values
        .iter()
        .map(|value| value as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();
    let prepared = if let Some(prepared) = cache.get(sql) {
        prepared.clone()
    } else {
        let prepared = client.prepare(sql).await.map_err(failure)?;
        cache.insert(sql.to_owned(), prepared.clone());
        prepared
    };
    if prepared.columns().is_empty() {
        let affected_rows = client
            .execute(&prepared, &parameters)
            .await
            .map_err(|error| query_error(error, &prepared, &values, sql))?;
        return Ok(QueryResult {
            rows: Vec::new(),
            affected_rows,
            last_insert_id: None,
        });
    }
    let selected = client
        .query(&prepared, &parameters)
        .await
        .map_err(|error| query_error(error, &prepared, &values, sql))?;
    let writes = !sql
        .trim_start()
        .get(..6)
        .is_some_and(|keyword| keyword.eq_ignore_ascii_case("SELECT"));
    let last_insert_id = selected.first().and_then(|row| {
        row.columns()
            .iter()
            .position(|column| column.name() == "id")
            .and_then(|index| row.try_get::<_, i64>(index).ok())
    });
    let rows: Vec<Row> = selected
        .into_iter()
        .map(decode_row)
        .collect::<Result<_, _>>()?;
    Ok(QueryResult {
        affected_rows: if writes { rows.len() as u64 } else { 0 },
        rows,
        last_insert_id,
    })
}

fn replace_changes(sql: &str, changes: Option<u64>) -> String {
    let value = changes.unwrap_or_default().to_string();
    sql.replace("\"changes\"()", &value)
        .replace("changes()", &value)
}

#[derive(Debug)]
enum PgValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<DbValue> for PgValue {
    fn from(value: DbValue) -> Self {
        match value {
            DbValue::Null => Self::Null,
            DbValue::Integer(value) => Self::Integer(value),
            DbValue::Real(value) => Self::Real(value),
            DbValue::Text(value) => Self::Text(value),
            DbValue::Blob(value) => Self::Blob(value),
        }
    }
}

impl ToSql for PgValue {
    fn to_sql(
        &self,
        ty: &Type,
        output: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            Self::Null => Ok(IsNull::Yes),
            Self::Integer(value) if *ty == Type::INT4 => i32::try_from(*value)?.to_sql(ty, output),
            Self::Integer(value) if matches!(*ty, Type::TEXT | Type::VARCHAR) => {
                value.to_string().to_sql(ty, output)
            }
            Self::Integer(value) => value.to_sql(ty, output),
            Self::Real(value) if *ty == Type::FLOAT4 => (*value as f32).to_sql(ty, output),
            Self::Real(value) if matches!(*ty, Type::TEXT | Type::VARCHAR) => {
                value.to_string().to_sql(ty, output)
            }
            Self::Real(value) => value.to_sql(ty, output),
            Self::Text(value) => value.to_sql(ty, output),
            Self::Blob(value) => value.to_sql(ty, output),
        }
    }
    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::INT4
                | Type::INT8
                | Type::FLOAT4
                | Type::FLOAT8
                | Type::TEXT
                | Type::VARCHAR
                | Type::BYTEA
        )
    }
    to_sql_checked!();
}

fn decode_row(row: tokio_postgres::Row) -> Result<Row, StoreError> {
    let values = row
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let value = match *column.type_() {
                Type::INT8 => optional::<i64>(&row, index, DbValue::Integer)?,
                Type::INT4 => {
                    optional::<i32>(&row, index, |value| DbValue::Integer(i64::from(value)))?
                }
                Type::FLOAT8 => optional::<f64>(&row, index, DbValue::Real)?,
                Type::FLOAT4 => {
                    optional::<f32>(&row, index, |value| DbValue::Real(f64::from(value)))?
                }
                Type::BYTEA => optional::<Vec<u8>>(&row, index, DbValue::Blob)?,
                Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
                    optional::<String>(&row, index, DbValue::Text)?
                }
                _ => {
                    return Err(StoreError::Database(format!(
                        "unsupported PostgreSQL result type {}",
                        column.type_()
                    )));
                }
            };
            Ok((column.name().to_owned(), value))
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    Ok(Row::new(values))
}

fn optional<'a, T: FromSql<'a>>(
    row: &'a tokio_postgres::Row,
    index: usize,
    map: impl FnOnce(T) -> DbValue,
) -> Result<DbValue, StoreError> {
    row.try_get::<_, Option<T>>(index)
        .map(|value| value.map_or(DbValue::Null, map))
        .map_err(database_error)
}

fn database_error(error: tokio_postgres::Error) -> StoreError {
    match error.as_db_error() {
        Some(database) => StoreError::Database(format!("PostgreSQL: {}", database.message())),
        None => StoreError::Database(format!("PostgreSQL: {error}")),
    }
}

// An ErrorResponse means the server rejected the statement and is ready for the
// next one. Anything else — a closed socket, a driver that gave up mid-exchange
// — leaves a connection nobody may lend out again.
fn failure(error: tokio_postgres::Error) -> Failure {
    Failure {
        reusable: error.as_db_error().is_some(),
        error: database_error(error),
    }
}

fn query_error(
    error: tokio_postgres::Error,
    statement: &tokio_postgres::Statement,
    values: &[PgValue],
    sql: &str,
) -> Failure {
    for (index, (value, ty)) in values.iter().zip(statement.params()).enumerate() {
        if value.to_sql_checked(ty, &mut BytesMut::new()).is_err() {
            // The driver rejected the arguments before writing anything.
            return StoreError::Database(format!(
                "PostgreSQL cannot encode parameter {index} as {ty} for {}",
                sql.split_whitespace().take(4).collect::<Vec<_>>().join(" ")
            ))
            .into();
        }
    }
    failure(error)
}

#[cfg(test)]
mod tests {
    #[test]
    fn pool_size_never_drops_below_eight() {
        assert_eq!(super::clamp_pool_size(0), 8);
        assert_eq!(super::clamp_pool_size(7), 8);
        assert_eq!(super::clamp_pool_size(8), 8);
        assert_eq!(super::clamp_pool_size(super::DEFAULT_POOL_SIZE), 32);
        assert!(super::DEFAULT_CHECKOUT_TIMEOUT.as_millis() >= 1_000);
    }

    struct Server {
        dsn: String,
        online: Arc<std::sync::atomic::AtomicBool>,
        connections: Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
        task: tokio::task::JoinHandle<()>,
    }

    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    impl Server {
        async fn start() -> Self {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let dsn = format!(
                "host=127.0.0.1 port={} user=pool_test sslmode=disable",
                listener.local_addr().unwrap().port()
            );
            let online = Arc::new(std::sync::atomic::AtomicBool::new(true));
            let connections = Arc::new(std::sync::Mutex::new(Vec::new()));
            let status = online.clone();
            let tasks = connections.clone();
            let task = tokio::spawn(async move {
                while let Ok((mut stream, _)) = listener.accept().await {
                    if !status.load(Ordering::SeqCst) {
                        continue;
                    }
                    let task = tokio::spawn(async move {
                        let Ok(size) = stream.read_u32().await else {
                            return;
                        };
                        let mut startup = vec![0; size as usize - 4];
                        if stream.read_exact(&mut startup).await.is_err() {
                            return;
                        }
                        // PostgreSQL AuthenticationOk and ReadyForQuery. Tests
                        // exercise pool ownership; SQL replies are deliberately held.
                        if stream
                            .write_all(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I")
                            .await
                            .is_err()
                        {
                            return;
                        }
                        let mut bytes = [0; 1024];
                        while matches!(stream.read(&mut bytes).await, Ok(n) if n > 0) {}
                    });
                    tasks.lock().unwrap().push(task);
                }
            });
            Self {
                dsn,
                online,
                connections,
                task,
            }
        }

        fn disconnect(&self) {
            self.online.store(false, Ordering::SeqCst);
            for task in self.connections.lock().unwrap().drain(..) {
                task.abort();
            }
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
            self.disconnect();
        }
    }

    #[tokio::test]
    async fn cancelling_queries_returns_every_pool_slot() {
        use crate::backend::Executor;
        let server = Server::start().await;
        let pool = Arc::new(
            super::Postgres::connect(&server.dsn, 8, Duration::from_secs(1))
                .await
                .unwrap(),
        );
        // More cancellations than the entire pool used to exhaust it forever.
        for _ in 0..16 {
            let executing = pool.clone();
            let task = tokio::spawn(async move {
                executing
                    .execute(crate::backend::Statement::plain("SELECT 1"))
                    .await
            });
            tokio::time::timeout(Duration::from_secs(1), async {
                while pool.inner.capacity.available_permits() == 8 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert_eq!(pool.inner.capacity.available_permits(), 8);
        }
        let mut held = Vec::new();
        for _ in 0..8 {
            held.push(pool.checkout().await.unwrap());
        }
        assert_eq!(pool.inner.capacity.available_permits(), 0);
        drop(held);
        assert_eq!(pool.inner.capacity.available_permits(), 8);
    }

    #[tokio::test]
    async fn failed_reconnections_do_not_permanently_shrink_the_pool() {
        let server = Server::start().await;
        let pool = super::Postgres::connect(&server.dsn, 8, Duration::from_secs(1))
            .await
            .unwrap();
        server.disconnect();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if pool
                    .inner
                    .idle
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|conn| conn.client.is_closed())
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for _ in 0..3 {
            assert!(pool.checkout().await.is_err());
            assert_eq!(pool.inner.capacity.available_permits(), 8);
        }
        server.online.store(true, Ordering::SeqCst);
        let mut held = Vec::new();
        for _ in 0..8 {
            held.push(pool.checkout().await.unwrap());
        }
        assert_eq!(held.len(), 8);
    }

    // A rejected statement is the server working, not the connection failing.
    // Recycling the connection for it would answer an error burst by
    // reconnecting the pool and re-preparing everything it had cached.
    #[tokio::test]
    #[ignore = "requires PostgreSQL via GPROXY_TEST_POSTGRES_DSN"]
    async fn postgres_rejected_statements_keep_their_connection_and_prepared_statements() {
        use crate::backend::{DbValue, Executor, Statement};
        let dsn = std::env::var("GPROXY_TEST_POSTGRES_DSN").expect("GPROXY_TEST_POSTGRES_DSN");
        let pool = super::Postgres::connect(&dsn, 8, Duration::from_secs(2))
            .await
            .unwrap();
        let backend = || async {
            pool.execute(Statement::plain("SELECT pg_backend_pid()::bigint AS value"))
                .await
                .unwrap()
                .rows[0]
                .i64("value")
                .unwrap()
        };
        let first = backend().await;
        // The divisor is a parameter so the planner cannot reject this while
        // preparing it: the connection must survive a failure raised mid-query.
        let divide = || {
            Statement::with_args(
                "SELECT 1::bigint / $1::bigint AS value",
                vec![DbValue::Integer(0)],
            )
        };
        assert!(pool.execute(divide()).await.is_err());
        assert_eq!(backend().await, first);
        assert!(pool.batch(vec![divide()]).await.is_err());
        assert_eq!(backend().await, first);
        let mut lease = pool.checkout().await.unwrap();
        assert!(
            lease
                .statements
                .get("SELECT 1::bigint / $1::bigint AS value")
                .is_some()
        );
        assert!(
            lease
                .statements
                .get("SELECT pg_backend_pid()::bigint AS value")
                .is_some()
        );
        lease.reusable = true;
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL via GPROXY_TEST_POSTGRES_DSN"]
    async fn postgres_statement_cache_bounds_server_resources_and_reprepares_evicted_sql() {
        use crate::backend::Executor;
        let dsn = std::env::var("GPROXY_TEST_POSTGRES_DSN").expect("GPROXY_TEST_POSTGRES_DSN");
        let pool = super::Postgres::connect(&dsn, 8, Duration::from_secs(2))
            .await
            .unwrap();
        for value in 0..super::STATEMENT_CACHE_CAPACITY + 32 {
            let result = pool
                .execute(crate::backend::Statement::plain(format!(
                    "SELECT {value}::bigint AS value"
                )))
                .await
                .unwrap();
            assert_eq!(result.rows[0].i64("value").unwrap(), value as i64);
        }
        {
            let mut lease = pool.checkout().await.unwrap();
            assert_eq!(
                lease.statements.entries.len(),
                super::STATEMENT_CACHE_CAPACITY
            );
            assert_eq!(
                lease.statements.order.len(),
                super::STATEMENT_CACHE_CAPACITY
            );
            assert!(lease.statements.get("SELECT 0::bigint AS value").is_none());
            let row = lease
                .client
                .simple_query("SELECT count(*) FROM pg_prepared_statements")
                .await
                .unwrap();
            let row = row
                .iter()
                .find_map(|message| match message {
                    tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                    _ => None,
                })
                .expect("count row");
            assert_eq!(
                row.get(0).unwrap().parse::<usize>().unwrap(),
                super::STATEMENT_CACHE_CAPACITY
            );
            lease.reusable = true;
        }
        assert_eq!(
            pool.execute(crate::backend::Statement::plain(
                "SELECT 0::bigint AS value"
            ))
            .await
            .unwrap()
            .rows[0]
                .i64("value")
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL via GPROXY_TEST_POSTGRES_DSN"]
    async fn postgres_cancelled_transaction_releases_its_server_lock() {
        use crate::backend::Executor;
        let dsn = std::env::var("GPROXY_TEST_POSTGRES_DSN").expect("GPROXY_TEST_POSTGRES_DSN");
        let pool = Arc::new(
            super::Postgres::connect(&dsn, 8, Duration::from_secs(2))
                .await
                .unwrap(),
        );
        let (locked, waiting) = tokio::sync::oneshot::channel();
        let executing = pool.clone();
        let task = tokio::spawn(async move {
            let mut lease = executing.checkout().await.unwrap();
            let transaction = lease.client.transaction().await.unwrap();
            transaction
                .batch_execute("SELECT pg_advisory_xact_lock(20260909035)")
                .await
                .unwrap();
            locked.send(()).unwrap();
            transaction
                .batch_execute("SELECT pg_sleep(30)")
                .await
                .unwrap();
        });
        waiting.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(pool.inner.capacity.available_permits(), 8);
        // A new checkout must execute promptly, and cancellation must release
        // server-side transaction locks as well as local pool capacity.
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let result = pool.execute(crate::backend::Statement::plain("SELECT CASE WHEN pg_try_advisory_lock(20260909035) THEN 1 ELSE 0 END AS acquired")).await.unwrap();
                if result.rows[0].i64("acquired").unwrap() == 1 { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
    }
}
