use std::sync::Arc;

use gproxy_core::CacheBackend;

use super::libsql_store;

type SharedCache = Arc<dyn CacheBackend + Send + Sync>;

#[tokio::test]
async fn in_process_and_libsql_cache_operations_are_atomic() {
    let directory = tempfile::tempdir().expect("cache tempdir");
    let (store, _) = libsql_store(directory.path().join("cache.db"))
        .await
        .expect("libSQL store");
    let libsql = crate::LibsqlCache::connect(store)
        .await
        .expect("libSQL cache");
    for cache in [
        Arc::new(crate::InProcessCache::default()) as SharedCache,
        Arc::new(libsql) as SharedCache,
    ] {
        exercise_atomicity(cache).await;
    }
}

#[tokio::test]
#[ignore = "requires live Redis via GPROXY_TEST_REDIS_URL"]
async fn redis_cache_operations_are_atomic() {
    let url = std::env::var("GPROXY_TEST_REDIS_URL").expect("GPROXY_TEST_REDIS_URL");
    let cache = crate::RedisCache::connect(&url).await.expect("Redis cache");
    exercise_atomicity(Arc::new(cache)).await;
    let first = crate::RedisCache::connect(&url)
        .await
        .expect("first Redis instance");
    let second = crate::RedisCache::connect(&url)
        .await
        .expect("second Redis instance");
    let lease = format!("gproxy:test:{}:refresh", std::process::id());
    first.delete(&lease).await.expect("clear refresh lease");
    assert_eq!(first.incr(&lease, 1, None).await.expect("first lease"), 1);
    assert_eq!(second.incr(&lease, 1, None).await.expect("second lease"), 2);
    first.delete(&lease).await.expect("remove refresh lease");

    // Exercise Upstash's actual HTTP encoding/decoding against Redis too.
    // This catches JSON number rounding independently of native RESP replies.
    let rest = RedisRestBridge::start(&url).await;
    exercise_atomicity(Arc::new(crate::UpstashCache::new(
        rest.url.clone(),
        "cache-test-token".into(),
    )))
    .await;
}

#[tokio::test]
#[ignore = "requires live Upstash via GPROXY_TEST_UPSTASH_URL and GPROXY_TEST_UPSTASH_TOKEN"]
async fn upstash_cache_operations_are_atomic() {
    let url = std::env::var("GPROXY_TEST_UPSTASH_URL").expect("GPROXY_TEST_UPSTASH_URL");
    let token = std::env::var("GPROXY_TEST_UPSTASH_TOKEN").expect("GPROXY_TEST_UPSTASH_TOKEN");
    exercise_atomicity(Arc::new(crate::UpstashCache::new(url, token))).await;
}

async fn exercise_atomicity(cache: SharedCache) {
    let prefix = format!("gproxy:test:{}", std::process::id());
    let counter = format!("{prefix}:counter");
    cache.delete(&counter).await.expect("clear counter");
    let mut calls = Vec::new();
    for _ in 0..32 {
        let cache = cache.clone();
        let counter = counter.clone();
        calls.push(tokio::spawn(async move {
            cache.incr(&counter, 1, None).await.expect("increment")
        }));
    }
    let mut values = Vec::new();
    for call in calls {
        values.push(call.await.expect("increment task"));
    }
    values.sort_unstable();
    assert_eq!(values, (1..=32).collect::<Vec<_>>());

    let lease = format!("{prefix}:lease");
    cache.delete(&lease).await.expect("clear lease");
    assert!(
        cache
            .compare_and_swap(&lease, None, None, None)
            .await
            .unwrap()
    );
    let left = cache.compare_and_swap(&lease, None, Some(b"left".to_vec()), None);
    let right = cache.compare_and_swap(&lease, None, Some(b"right".to_vec()), None);
    let (left, right) = tokio::join!(left, right);
    assert_ne!(
        left.expect("left contender"),
        right.expect("right contender")
    );
    assert!(
        !cache
            .compare_and_swap(&lease, None, None, None)
            .await
            .unwrap()
    );
    cache
        .set(
            &lease,
            b"expires".to_vec(),
            Some(std::time::Duration::from_millis(1)),
        )
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while cache.get(&lease).await.unwrap().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        cache
            .compare_and_swap(&lease, None, None, None)
            .await
            .unwrap()
    );
    cache.delete(&counter).await.expect("remove counter");
    cache.delete(&lease).await.expect("remove lease");

    let used = format!("{prefix}:used");
    let pending = format!("{prefix}:pending");
    cache.delete(&used).await.expect("clear used");
    cache.delete(&pending).await.expect("clear pending");
    assert_eq!(
        cache
            .reserve_spend(&used, &pending, 1, 2, None)
            .await
            .expect("missing used"),
        gproxy_core::SpendReserve::MissingUsed
    );
    assert!(cache.seed_counter(&used, 0, None).await.expect("seed used"));
    assert!(
        !cache
            .seed_counter(&used, 99, None)
            .await
            .expect("seed is once")
    );
    let mut calls = Vec::new();
    for _ in 0..8 {
        let cache = cache.clone();
        let used = used.clone();
        let pending = pending.clone();
        calls.push(tokio::spawn(async move {
            cache
                .reserve_spend(&used, &pending, 1, 3, None)
                .await
                .expect("reserve")
        }));
    }
    let mut allowed = 0;
    let mut denied = 0;
    for call in calls {
        match call.await.expect("reserve task") {
            gproxy_core::SpendReserve::Allowed => allowed += 1,
            gproxy_core::SpendReserve::Denied => denied += 1,
            gproxy_core::SpendReserve::MissingUsed => panic!("used was seeded"),
        }
    }
    assert_eq!(allowed, 3);
    assert_eq!(denied, 5);
    let state = format!("{prefix}:state");
    cache.set(&state, b"reserved".to_vec(), None).await.unwrap();
    let (left, right) = tokio::join!(
        cache.raise_counter(&used, 7, None),
        cache.raise_counter(&used, 7, None),
    );
    left.unwrap();
    right.unwrap();
    cache.raise_counter(&used, 5, None).await.unwrap();
    assert_eq!(cache.incr(&used, 0, None).await.unwrap(), 7);
    let (left, right) = tokio::join!(
        cache.compare_incr_and_set(
            &pending,
            -1,
            &state,
            b"reserved".to_vec(),
            b"released".to_vec()
        ),
        cache.compare_incr_and_set(
            &pending,
            -1,
            &state,
            b"reserved".to_vec(),
            b"released".to_vec()
        ),
    );
    assert_ne!(left.unwrap().is_some(), right.unwrap().is_some());
    assert_eq!(cache.incr(&pending, 0, None).await.unwrap(), 2);
    cache.delete(&state).await.unwrap();
    assert_eq!(
        cache
            .compare_incr_and_set(
                &pending,
                -1,
                &state,
                b"reserved".to_vec(),
                b"released".to_vec()
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(cache.incr(&pending, 0, None).await.unwrap(), 2);
    cache.delete(&used).await.unwrap();
    cache.raise_counter(&used, 9, None).await.unwrap();
    assert_eq!(cache.incr(&used, 0, None).await.unwrap(), 9);
    cache.delete(&used).await.unwrap();
    cache.delete(&pending).await.unwrap();
    exercise_counter_bounds(cache.clone(), &prefix).await;
    exercise_reservation_state(cache.clone(), &prefix).await;
    exercise_reservation_bounds(cache, &prefix).await;
}

async fn exercise_counter_bounds(cache: SharedCache, prefix: &str) {
    let counter = format!("{prefix}:exact-counter");
    let state = format!("{prefix}:exact-counter-state");
    let ready = b"ready".to_vec();
    let updated = b"updated".to_vec();
    let cases = [
        (9_007_199_254_740_992, 1),
        (-9_007_199_254_740_992, -1),
        (i64::MAX - 1, 1),
        (i64::MIN + 1, -1),
        (i64::MAX, i64::MIN),
        (i64::MIN, i64::MAX),
        (0, i64::MAX),
        (0, i64::MIN),
    ];
    for guarded in [false, true] {
        for (current, by) in cases {
            cache.delete(&counter).await.unwrap();
            cache.seed_counter(&counter, current, None).await.unwrap();
            cache.set(&state, ready.clone(), None).await.unwrap();
            let next = if guarded {
                cache
                    .compare_incr_and_set(&counter, by, &state, ready.clone(), updated.clone())
                    .await
                    .unwrap()
                    .unwrap()
            } else {
                cache.incr(&counter, by, None).await.unwrap()
            };
            assert_eq!(next, current.checked_add(by).unwrap());
            assert_eq!(cache.incr(&counter, 0, None).await.unwrap(), next);
            assert_eq!(
                cache.get(&state).await.unwrap(),
                Some(if guarded {
                    updated.clone()
                } else {
                    ready.clone()
                })
            );
        }
        for (current, by) in [(i64::MAX, 1), (i64::MIN, -1)] {
            cache.delete(&counter).await.unwrap();
            cache.seed_counter(&counter, current, None).await.unwrap();
            cache.set(&state, ready.clone(), None).await.unwrap();
            let result = if guarded {
                cache
                    .compare_incr_and_set(&counter, by, &state, ready.clone(), updated.clone())
                    .await
            } else {
                cache.incr(&counter, by, None).await.map(Some)
            };
            assert!(result.is_err(), "counter overflow must fail: {result:?}");
            assert_eq!(cache.incr(&counter, 0, None).await.unwrap(), current);
            assert_eq!(cache.get(&state).await.unwrap(), Some(ready.clone()));
            assert_eq!(
                cache
                    .compare_incr_and_set(&counter, by, &state, updated.clone(), ready.clone())
                    .await
                    .unwrap(),
                None,
                "a stale state must not evaluate the overflowing counter"
            );
        }
    }
    for (current, floor) in [(1, -2), (-2, -3), (-10, -1), (i64::MAX, i64::MIN)] {
        cache.delete(&counter).await.unwrap();
        cache.seed_counter(&counter, current, None).await.unwrap();
        cache.raise_counter(&counter, floor, None).await.unwrap();
        assert_eq!(
            cache.incr(&counter, 0, None).await.unwrap(),
            current.max(floor)
        );
    }
    cache.delete(&counter).await.unwrap();
    cache.delete(&state).await.unwrap();
}

async fn exercise_reservation_state(cache: SharedCache, prefix: &str) {
    use gproxy_core::SpendReserve;

    let used = format!("{prefix}:atomic-used");
    let pending = format!("{prefix}:atomic-pending");
    let state = format!("{prefix}:atomic-state");
    for key in [&used, &pending, &state] {
        cache.delete(key).await.unwrap();
    }
    assert_eq!(
        cache
            .reserve_spend_and_set(
                &used,
                &pending,
                7,
                10,
                &state,
                b"ready".to_vec(),
                b"reserved".to_vec()
            )
            .await
            .unwrap(),
        None,
    );
    cache.set(&state, b"ready".to_vec(), None).await.unwrap();
    assert_eq!(
        cache
            .reserve_spend_and_set(
                &used,
                &pending,
                7,
                10,
                &state,
                b"ready".to_vec(),
                b"reserved".to_vec()
            )
            .await
            .unwrap(),
        Some(SpendReserve::MissingUsed),
    );
    assert_eq!(cache.get(&state).await.unwrap(), Some(b"ready".to_vec()));
    assert!(cache.get(&pending).await.unwrap().is_none());
    cache.seed_counter(&used, 3, None).await.unwrap();
    assert_eq!(
        cache
            .reserve_spend_and_set(
                &used,
                &pending,
                8,
                10,
                &state,
                b"ready".to_vec(),
                b"reserved".to_vec()
            )
            .await
            .unwrap(),
        Some(SpendReserve::Denied),
    );
    assert_eq!(cache.get(&state).await.unwrap(), Some(b"ready".to_vec()));
    assert!(cache.get(&pending).await.unwrap().is_none());

    // Lost replies and simultaneous retries must all observe the same one
    // committed reservation, even when it consumes the entire available quota.
    let mut calls = Vec::new();
    for _ in 0..16 {
        let cache = cache.clone();
        let (used, pending, state) = (used.clone(), pending.clone(), state.clone());
        calls.push(tokio::spawn(async move {
            cache
                .reserve_spend_and_set(
                    &used,
                    &pending,
                    7,
                    10,
                    &state,
                    b"ready".to_vec(),
                    b"reserved".to_vec(),
                )
                .await
                .unwrap()
        }));
    }
    for call in calls {
        assert_eq!(call.await.unwrap(), Some(SpendReserve::Allowed));
    }
    assert_eq!(cache.incr(&pending, 0, None).await.unwrap(), 7);
    assert_eq!(cache.get(&state).await.unwrap(), Some(b"reserved".to_vec()));
    cache.delete(&used).await.unwrap();
    assert_eq!(
        cache
            .reserve_spend_and_set(
                &used,
                &pending,
                7,
                10,
                &state,
                b"ready".to_vec(),
                b"reserved".to_vec()
            )
            .await
            .unwrap(),
        Some(SpendReserve::Allowed),
    );
    assert_eq!(cache.incr(&pending, 0, None).await.unwrap(), 7);
    cache.set(&state, b"released".to_vec(), None).await.unwrap();
    assert_eq!(
        cache
            .reserve_spend_and_set(
                &used,
                &pending,
                7,
                10,
                &state,
                b"ready".to_vec(),
                b"reserved".to_vec()
            )
            .await
            .unwrap(),
        None,
    );
    assert_eq!(cache.incr(&pending, 0, None).await.unwrap(), 7);
    assert_eq!(cache.get(&state).await.unwrap(), Some(b"released".to_vec()));

    // Redis Lua must not round a one-unit reservation at the f64 boundary.
    cache.delete(&pending).await.unwrap();
    cache
        .seed_counter(&used, 9_007_199_254_740_992, None)
        .await
        .unwrap();
    cache.set(&state, b"ready".to_vec(), None).await.unwrap();
    assert_eq!(
        cache
            .reserve_spend_and_set(
                &used,
                &pending,
                1,
                9_007_199_254_740_993,
                &state,
                b"ready".to_vec(),
                b"reserved".to_vec()
            )
            .await
            .unwrap(),
        Some(SpendReserve::Allowed),
    );
    assert_eq!(
        cache
            .reserve_spend_and_set(
                &used,
                &pending,
                0,
                9_007_199_254_740_993,
                &state,
                b"reserved".to_vec(),
                b"advanced".to_vec()
            )
            .await
            .unwrap(),
        Some(SpendReserve::Denied),
    );
    assert_eq!(cache.incr(&pending, 0, None).await.unwrap(), 1);
    cache.delete(&pending).await.unwrap();
    assert_eq!(
        cache
            .reserve_spend(&used, &pending, 1, 9_007_199_254_740_993, None)
            .await
            .unwrap(),
        SpendReserve::Allowed,
    );
    assert_eq!(
        cache
            .reserve_spend(&used, &pending, 1, 9_007_199_254_740_993, None)
            .await
            .unwrap(),
        SpendReserve::Denied,
    );
    assert_eq!(cache.incr(&pending, 0, None).await.unwrap(), 1);

    // A denied large pending increment must restore its exact prior value:
    // after refusing two units there must still be room for exactly one.
    cache.delete(&used).await.unwrap();
    cache.delete(&pending).await.unwrap();
    cache.seed_counter(&used, 0, None).await.unwrap();
    cache
        .seed_counter(&pending, 9_007_199_254_740_992, None)
        .await
        .unwrap();
    assert_eq!(
        cache
            .reserve_spend(&used, &pending, 2, 9_007_199_254_740_993, None)
            .await
            .unwrap(),
        SpendReserve::Denied,
    );
    assert_eq!(
        cache
            .reserve_spend(&used, &pending, 1, 9_007_199_254_740_993, None)
            .await
            .unwrap(),
        SpendReserve::Allowed,
    );
    assert_eq!(
        cache
            .reserve_spend(&used, &pending, 0, 9_007_199_254_740_993, None)
            .await
            .unwrap(),
        SpendReserve::Denied,
    );
    for key in [&used, &pending, &state] {
        cache.delete(key).await.unwrap();
    }
}

async fn exercise_reservation_bounds(cache: SharedCache, prefix: &str) {
    use gproxy_core::SpendReserve;

    let used = format!("{prefix}:bounded-used");
    let pending = format!("{prefix}:bounded-pending");
    let state = format!("{prefix}:bounded-state");
    let cases = [
        // Pending arithmetic is checked even when the quota is exhausted.
        (0, i64::MAX, 1, i64::MAX, None),
        (-i64::MAX, i64::MAX, 1, i64::MAX, None),
        (0, i64::MIN, -1, i64::MAX, None),
        // used+pending saturates, matching the shared spend_fits contract.
        (i64::MAX - 10, 0, 11, i64::MAX, Some(true)),
        (i64::MAX - 1, 1, 0, i64::MAX, Some(false)),
        (i64::MIN, i64::MAX, 0, 0, Some(true)),
        (0, i64::MIN + 1, -1, 1, Some(true)),
        (0, -5, 3, 1, Some(true)),
        (i64::MAX - 10, 5, 4, i64::MAX - 1, Some(true)),
        (i64::MAX - 10, 5, 5, i64::MAX - 1, Some(false)),
        // A denied negative reservation must not attempt to negate i64::MIN.
        (1, 0, i64::MIN, 1, Some(false)),
    ];
    for guarded in [false, true] {
        for (used_value, pending_value, estimate, limit, allowed) in cases {
            for key in [&used, &pending, &state] {
                cache.delete(key).await.unwrap();
            }
            cache.seed_counter(&used, used_value, None).await.unwrap();
            cache
                .seed_counter(&pending, pending_value, None)
                .await
                .unwrap();
            cache.set(&state, b"ready".to_vec(), None).await.unwrap();
            let result = if guarded {
                cache
                    .reserve_spend_and_set(
                        &used,
                        &pending,
                        estimate,
                        limit,
                        &state,
                        b"ready".to_vec(),
                        b"reserved".to_vec(),
                    )
                    .await
            } else {
                cache
                    .reserve_spend(&used, &pending, estimate, limit, None)
                    .await
                    .map(Some)
            };
            match allowed {
                None => assert!(result.is_err(), "pending overflow must fail: {result:?}"),
                Some(true) => assert_eq!(result.unwrap(), Some(SpendReserve::Allowed)),
                Some(false) => assert_eq!(result.unwrap(), Some(SpendReserve::Denied)),
            }
            let expected_state = if guarded && allowed == Some(true) {
                b"reserved".to_vec()
            } else {
                b"ready".to_vec()
            };
            assert_eq!(cache.get(&state).await.unwrap(), Some(expected_state));
            if guarded && allowed.is_none() {
                // Completed or conflicting state wins before evaluating any
                // counters, including an otherwise overflowing increment.
                for (value, outcome) in [
                    (b"reserved".to_vec(), Some(SpendReserve::Allowed)),
                    (b"advanced".to_vec(), None),
                ] {
                    cache.set(&state, value, None).await.unwrap();
                    assert_eq!(
                        cache
                            .reserve_spend_and_set(
                                &used,
                                &pending,
                                estimate,
                                limit,
                                &state,
                                b"ready".to_vec(),
                                b"reserved".to_vec(),
                            )
                            .await
                            .unwrap(),
                        outcome,
                    );
                }
            }
            let expected_pending = if allowed == Some(true) {
                pending_value.checked_add(estimate).unwrap()
            } else {
                pending_value
            };
            assert_eq!(
                cache.incr(&pending, 0, None).await.unwrap(),
                expected_pending
            );
        }
    }
    for key in [&used, &pending, &state] {
        cache.delete(key).await.unwrap();
    }
}

struct RedisRestBridge {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl RedisRestBridge {
    async fn start(redis_url: &str) -> Self {
        let connection =
            redis::aio::ConnectionManager::new(redis::Client::open(redis_url).unwrap())
                .await
                .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.unwrap();
                        requests.spawn(redis_rest_request(stream, connection.clone()));
                    }
                    Some(result) = requests.join_next() => result.unwrap(),
                }
            }
        });
        Self { url, task }
    }
}

impl Drop for RedisRestBridge {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn redis_rest_request(
    stream: tokio::net::TcpStream,
    mut connection: redis::aio::ConnectionManager,
) {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    let mut reader = BufReader::new(stream);
    let mut length = None;
    loop {
        let mut line = String::new();
        assert!(reader.read_line(&mut line).await.unwrap() > 0);
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = Some(value.trim().parse::<usize>().unwrap());
        }
    }
    let mut body = vec![0; length.expect("JSON request has content length")];
    reader.read_exact(&mut body).await.unwrap();
    let arguments: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
    let mut command = redis::cmd(arguments[0].as_str().unwrap());
    for argument in &arguments[1..] {
        let value = match argument {
            serde_json::Value::String(value) => value.clone(),
            serde_json::Value::Number(number) => {
                // REST servers may parse JSON through IEEE-754 numbers. Only
                // small control arguments such as key counts may use numbers.
                let integer = number.as_i64().expect("integer Redis argument");
                assert!(integer.unsigned_abs() <= 9_007_199_254_740_992);
                integer.to_string()
            }
            _ => panic!("unexpected Redis argument: {argument}"),
        };
        command.arg(value);
    }
    let response = match command.query_async::<redis::Value>(&mut connection).await {
        Ok(value) => {
            let result = match value {
                redis::Value::Nil => serde_json::Value::Null,
                redis::Value::Int(value) => serde_json::json!(value),
                redis::Value::BulkString(value) => {
                    serde_json::json!(String::from_utf8(value).unwrap())
                }
                redis::Value::SimpleString(value) => serde_json::json!(value),
                redis::Value::Boolean(false) => serde_json::Value::Null,
                redis::Value::Boolean(true) => serde_json::json!(1),
                redis::Value::Okay => serde_json::json!("OK"),
                other => panic!("unexpected Redis response: {other:?}"),
            };
            serde_json::json!({"result": result})
        }
        Err(error) => serde_json::json!({"error": error.to_string()}),
    };
    let response = serde_json::to_vec(&response).unwrap();
    reader
        .get_mut()
        .write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response.len()).as_bytes())
        .await
        .unwrap();
    reader.get_mut().write_all(&response).await.unwrap();
}
