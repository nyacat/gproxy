use std::time::Duration;

use gproxy_core::CacheBackend;
use gproxy_core::channel_api::BoxFuture;

use crate::Store;
use crate::backend::{DbValue, Statement};

use super::error;

type Error = gproxy_core::error::StoreError;

// SQLite promotes an overflowing integer sum to REAL instead of failing.
// Keep increments checked inside the write statement so a failed guarded
// increment also rolls back its state transition in the enclosing batch.
macro_rules! checked_increment_sql {
    ($prefix:literal, $suffix:literal) => {
        concat!(
            $prefix,
            "CASE WHEN excluded.v>0 AND CAST(gproxy_kv.v AS INTEGER)>9223372036854775807-excluded.v THEN abs(-9223372036854775808)",
            " WHEN excluded.v<0 AND CAST(gproxy_kv.v AS INTEGER)<(-9223372036854775808)-excluded.v THEN abs(-9223372036854775808)",
            " ELSE CAST(gproxy_kv.v AS INTEGER)+excluded.v END",
            $suffix
        )
    };
}

// Materialize the checked sum before applying quota predicates: SQLite would
// otherwise promote an overflowing i64 addition to REAL or skip its evaluation
// on an exhausted quota. Both reserve APIs share checked pending arithmetic and
// the same saturating used+pending comparisons as gproxy_core::spend_fits.
macro_rules! reserve_sql {
    ($state_guard:literal) => {
        concat!(
            r#"WITH inputs AS (
                SELECT CAST(v AS INTEGER) AS used,
                       COALESCE((SELECT CAST(v AS INTEGER) FROM gproxy_kv
                                 WHERE k=? AND (expires_ms IS NULL OR expires_ms>?)),0) AS pending,
                       ? AS estimate, ? AS quota_limit
                FROM gproxy_kv WHERE k=? AND (expires_ms IS NULL OR expires_ms>?) "#,
            $state_guard,
            r#"), checked AS MATERIALIZED (
                SELECT used, pending, quota_limit,
                       CASE WHEN estimate>0 AND pending>9223372036854775807-estimate
                                  THEN abs(-9223372036854775808)
                            WHEN estimate<0 AND pending<(-9223372036854775808)-estimate
                                  THEN abs(-9223372036854775808)
                            ELSE pending+estimate END AS pending_after
                FROM inputs
            ), bounds AS (
                SELECT used, quota_limit, pending_after,
                       MAX(0,pending) AS before, MAX(0,pending_after) AS projected
                FROM checked
            )
            INSERT INTO gproxy_kv(k,v,expires_ms)
            SELECT ?,pending_after,? FROM bounds
            WHERE (CASE WHEN used>9223372036854775807-before THEN 9223372036854775807
                        ELSE used+before END)<quota_limit
              AND (CASE WHEN used>9223372036854775807-projected THEN 9223372036854775807
                        ELSE used+projected END)<=quota_limit
            ON CONFLICT(k) DO UPDATE SET v=excluded.v,expires_ms=excluded.expires_ms
            RETURNING 1 AS reserved"#
        )
    };
}

const RESERVE_SQL: &str = reserve_sql!("");
const RESERVE_AND_SET_SQL: &str = reserve_sql!(
    "AND EXISTS(SELECT 1 FROM gproxy_kv WHERE k=? AND v=? AND v<>? AND (expires_ms IS NULL OR expires_ms>?))"
);

pub struct LibsqlCache {
    store: Store,
}

impl LibsqlCache {
    pub async fn connect(store: Store) -> Result<Self, Error> {
        store.backend().batch(vec![
            Statement::plain("CREATE TABLE IF NOT EXISTS gproxy_kv (k TEXT PRIMARY KEY, v BLOB NOT NULL, expires_ms INTEGER)"),
            Statement::plain("CREATE INDEX IF NOT EXISTS gproxy_kv_expires_ms_idx ON gproxy_kv(expires_ms)"),
        ]).await.map_err(|_| error("libSQL", "initialization"))?;
        Ok(Self { store })
    }

    async fn execute(
        &self,
        sql: &str,
        args: Vec<DbValue>,
        operation: &'static str,
    ) -> Result<crate::backend::QueryResult, Error> {
        self.store
            .backend()
            .execute(Statement::with_args(sql, args))
            .await
            .map_err(|_| error("libSQL", operation))
    }
}

impl CacheBackend for LibsqlCache {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, Error>> {
        Box::pin(async move {
            let result = self.execute("SELECT v FROM gproxy_kv WHERE k = ? AND (expires_ms IS NULL OR expires_ms > ?)", vec![text(key), integer(now_ms())], "get").await?;
            result
                .rows
                .into_iter()
                .next()
                .map(|row| {
                    row.blob("v")
                        .map(Vec::from)
                        .map_err(|_| error("libSQL", "get"))
                })
                .transpose()
        })
    }

    fn set<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.execute("INSERT INTO gproxy_kv(k,v,expires_ms) VALUES(?,?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v, expires_ms=excluded.expires_ms", vec![text(key), DbValue::Blob(value), expiry(ttl)], "set").await.map(|_| ())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.execute(
                "DELETE FROM gproxy_kv WHERE k = ?",
                vec![text(key)],
                "delete",
            )
            .await
            .map(|_| ())
        })
    }

    fn incr<'a>(
        &'a self,
        key: &'a str,
        by: i64,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<i64, Error>> {
        Box::pin(async move {
            let now = now_ms();
            let result = self.execute(
                checked_increment_sql!(
                    "INSERT INTO gproxy_kv(k,v,expires_ms) VALUES(?,?,?) ON CONFLICT(k) DO UPDATE SET v=CASE WHEN gproxy_kv.expires_ms IS NOT NULL AND gproxy_kv.expires_ms<=? THEN excluded.v ELSE ",
                    " END, expires_ms=CASE WHEN gproxy_kv.expires_ms IS NOT NULL AND gproxy_kv.expires_ms<=? THEN excluded.expires_ms ELSE gproxy_kv.expires_ms END RETURNING CAST(v AS INTEGER) AS value"
                ),
                vec![text(key), integer(by), expiry(ttl), integer(now), integer(now)], "increment").await?;
            result
                .rows
                .first()
                .ok_or_else(|| error("libSQL", "increment"))?
                .i64("value")
                .map_err(|_| error("libSQL", "increment"))
        })
    }

    fn compare_incr_and_set<'a>(
        &'a self,
        counter_key: &'a str,
        by: i64,
        state_key: &'a str,
        expected: Vec<u8>,
        state: Vec<u8>,
    ) -> BoxFuture<'a, Result<Option<i64>, Error>> {
        Box::pin(async move {
            let statements = vec![
                Statement::with_args(
                    "UPDATE gproxy_kv SET v=?, expires_ms=NULL WHERE k=? AND v=? AND (expires_ms IS NULL OR expires_ms>?)",
                    vec![
                        DbValue::Blob(state),
                        text(state_key),
                        DbValue::Blob(expected),
                        integer(now_ms()),
                    ],
                ),
                Statement::with_args(
                    checked_increment_sql!(
                        "INSERT INTO gproxy_kv(k,v,expires_ms) SELECT ?,?,NULL WHERE changes()=1 ON CONFLICT(k) DO UPDATE SET v=CASE WHEN gproxy_kv.expires_ms IS NOT NULL AND gproxy_kv.expires_ms<=? THEN excluded.v ELSE ",
                        " END,expires_ms=NULL RETURNING CAST(v AS INTEGER) AS value"
                    ),
                    vec![text(counter_key), integer(by), integer(now_ms())],
                ),
                Statement::with_args(
                    "UPDATE gproxy_kv SET expires_ms=? WHERE k=? AND CAST(v AS INTEGER)=0 AND changes()=1",
                    vec![expiry(Some(Duration::from_secs(3600))), text(counter_key)],
                ),
            ];
            let results = self
                .store
                .backend()
                .batch(statements)
                .await
                .map_err(|_| error("libSQL", "compare increment"))?;
            results
                .get(1)
                .and_then(|result| result.rows.first())
                .map(|row| {
                    row.i64("value")
                        .map_err(|_| error("libSQL", "compare increment"))
                })
                .transpose()
        })
    }

    fn compare_and_swap<'a>(
        &'a self,
        key: &'a str,
        expected: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, Error>> {
        Box::pin(async move {
            let now = now_ms();
            let (sql, args) = match (expected, value) {
                (None, Some(value)) => (
                    "INSERT INTO gproxy_kv(k,v,expires_ms) VALUES(?,?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v, expires_ms=excluded.expires_ms WHERE gproxy_kv.expires_ms IS NOT NULL AND gproxy_kv.expires_ms<=? RETURNING 1 AS swapped",
                    vec![text(key), DbValue::Blob(value), expiry(ttl), integer(now)],
                ),
                (Some(expected), Some(value)) => (
                    "UPDATE gproxy_kv SET v=?,expires_ms=? WHERE k=? AND v=? AND (expires_ms IS NULL OR expires_ms>?) RETURNING 1 AS swapped",
                    vec![
                        DbValue::Blob(value),
                        expiry(ttl),
                        text(key),
                        DbValue::Blob(expected),
                        integer(now),
                    ],
                ),
                (Some(expected), None) => (
                    "DELETE FROM gproxy_kv WHERE k=? AND v=? AND (expires_ms IS NULL OR expires_ms>?) RETURNING 1 AS swapped",
                    vec![text(key), DbValue::Blob(expected), integer(now)],
                ),
                (None, None) => (
                    "SELECT 1 AS swapped WHERE NOT EXISTS(SELECT 1 FROM gproxy_kv WHERE k=? AND (expires_ms IS NULL OR expires_ms>?))",
                    vec![text(key), integer(now)],
                ),
            };
            Ok(!self
                .execute(sql, args, "compare and swap")
                .await?
                .rows
                .is_empty())
        })
    }

    fn seed_counter<'a>(
        &'a self,
        key: &'a str,
        value: i64,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, Error>> {
        Box::pin(async move {
            let result = self.execute(
                "INSERT INTO gproxy_kv(k,v,expires_ms) VALUES(?,?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v, expires_ms=excluded.expires_ms WHERE gproxy_kv.expires_ms IS NOT NULL AND gproxy_kv.expires_ms<=? RETURNING 1 AS seeded",
                vec![text(key), integer(value), expiry(ttl), integer(now_ms())], "seed counter",
            ).await?;
            Ok(!result.rows.is_empty())
        })
    }

    fn reserve_spend<'a>(
        &'a self,
        used_key: &'a str,
        pending_key: &'a str,
        estimate: i64,
        limit: i64,
        pending_ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<gproxy_core::SpendReserve, Error>> {
        Box::pin(async move {
            // One conditional write checks both counters and reserves room.
            // Denials never publish a speculative pending increment.
            let now = now_ms();
            let results = self.store.backend().batch(vec![
                Statement::with_args(
                    RESERVE_SQL,
                    vec![text(pending_key), integer(now), integer(estimate), integer(limit), text(used_key), integer(now), text(pending_key), expiry(pending_ttl)],
                ),
                Statement::with_args(
                    "SELECT 1 AS present FROM gproxy_kv WHERE k=? AND (expires_ms IS NULL OR expires_ms>?)",
                    vec![text(used_key), integer(now)],
                ),
            ]).await.map_err(|_| error("libSQL", "reserve spend"))?;
            Ok(if !results[0].rows.is_empty() {
                gproxy_core::SpendReserve::Allowed
            } else if results[1].rows.is_empty() {
                gproxy_core::SpendReserve::MissingUsed
            } else {
                gproxy_core::SpendReserve::Denied
            })
        })
    }

    fn reserve_spend_and_set<'a>(
        &'a self,
        used_key: &'a str,
        pending_key: &'a str,
        estimate: i64,
        limit: i64,
        state_key: &'a str,
        expected_state: Vec<u8>,
        state: Vec<u8>,
    ) -> BoxFuture<'a, Result<Option<gproxy_core::SpendReserve>, Error>> {
        Box::pin(async move {
            let now = now_ms();
            let results = self.store.backend().batch(vec![
                Statement::with_args(
                    "SELECT CASE WHEN EXISTS(SELECT 1 FROM gproxy_kv WHERE k=? AND v=? AND (expires_ms IS NULL OR expires_ms>?)) THEN 1 WHEN NOT EXISTS(SELECT 1 FROM gproxy_kv WHERE k=? AND v=? AND (expires_ms IS NULL OR expires_ms>?)) THEN -2 WHEN NOT EXISTS(SELECT 1 FROM gproxy_kv WHERE k=? AND (expires_ms IS NULL OR expires_ms>?)) THEN -1 ELSE 0 END AS outcome",
                    vec![text(state_key), DbValue::Blob(state.clone()), integer(now), text(state_key), DbValue::Blob(expected_state.clone()), integer(now), text(used_key), integer(now)],
                ),
                Statement::with_args(
                    RESERVE_AND_SET_SQL,
                    vec![text(pending_key), integer(now), integer(estimate), integer(limit), text(used_key), integer(now), text(state_key), DbValue::Blob(expected_state), DbValue::Blob(state.clone()), integer(now), text(pending_key), DbValue::Null],
                ),
                Statement::with_args(
                    "UPDATE gproxy_kv SET v=?,expires_ms=NULL WHERE k=? AND changes()=1",
                    vec![DbValue::Blob(state), text(state_key)],
                ),
            ]).await.map_err(|_| error("libSQL", "reserve spend and set"))?;
            let outcome = results[0].rows[0]
                .i64("outcome")
                .map_err(|_| error("libSQL", "reserve spend and set"))?;
            Ok(match outcome {
                1 => Some(gproxy_core::SpendReserve::Allowed),
                -2 => None,
                -1 => Some(gproxy_core::SpendReserve::MissingUsed),
                _ if !results[1].rows.is_empty() => Some(gproxy_core::SpendReserve::Allowed),
                _ => Some(gproxy_core::SpendReserve::Denied),
            })
        })
    }

    fn raise_counter<'a>(
        &'a self,
        key: &'a str,
        floor: i64,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.execute(
                "INSERT INTO gproxy_kv(k,v,expires_ms) VALUES(?,?,?) ON CONFLICT(k) DO UPDATE SET v=CASE WHEN gproxy_kv.expires_ms IS NOT NULL AND gproxy_kv.expires_ms<=? THEN excluded.v ELSE MAX(CAST(gproxy_kv.v AS INTEGER),excluded.v) END,expires_ms=excluded.expires_ms",
                vec![text(key), integer(floor), expiry(ttl), integer(now_ms())], "raise counter",
            ).await?;
            Ok(())
        })
    }
}

fn now_ms() -> i64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn expiry(ttl: Option<Duration>) -> DbValue {
    ttl.and_then(|ttl| now_ms().checked_add(i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX)))
        .map_or(DbValue::Null, DbValue::Integer)
}

fn text(value: &str) -> DbValue {
    DbValue::Text(value.into())
}
fn integer(value: i64) -> DbValue {
    DbValue::Integer(value)
}
