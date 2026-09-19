#[cfg(test)]
mod testing;

use std::time::Duration;

use gproxy_core::CacheBackend;
use gproxy_core::channel_api::BoxFuture;

type Error = gproxy_core::error::StoreError;

#[cfg(not(target_arch = "wasm32"))]
type SharedCache = std::sync::Arc<dyn CacheBackend + Send + Sync>;
#[cfg(target_arch = "wasm32")]
type SharedCache = std::rc::Rc<dyn CacheBackend>;

#[derive(Clone)]
pub(crate) struct AppCache {
    inner: SharedCache,
    #[cfg(test)]
    pub(crate) testing: std::sync::Arc<testing::Faults>,
}

impl AppCache {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn new(cache: impl CacheBackend + Send + Sync + 'static) -> Self {
        Self {
            inner: std::sync::Arc::new(cache),
            #[cfg(test)]
            testing: Default::default(),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) fn new(cache: impl CacheBackend + 'static) -> Self {
        Self {
            inner: std::rc::Rc::new(cache),
            #[cfg(test)]
            testing: Default::default(),
        }
    }
}

impl CacheBackend for AppCache {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, Error>> {
        #[cfg(test)]
        return self
            .testing
            .run("get", key, None, move || self.inner.get(key));
        #[cfg(not(test))]
        self.inner.get(key)
    }

    fn set<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        #[cfg(test)]
        return self.testing.run("set", key, Some(ttl), move || {
            self.inner.set(key, value, ttl)
        });
        #[cfg(not(test))]
        self.inner.set(key, value, ttl)
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        self.inner.delete(key)
    }

    fn incr<'a>(
        &'a self,
        key: &'a str,
        by: i64,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<i64, Error>> {
        #[cfg(test)]
        return self
            .testing
            .run("incr", key, None, move || self.inner.incr(key, by, ttl));
        #[cfg(not(test))]
        self.inner.incr(key, by, ttl)
    }

    fn compare_incr_and_set<'a>(
        &'a self,
        counter_key: &'a str,
        by: i64,
        state_key: &'a str,
        expected_state: Vec<u8>,
        state: Vec<u8>,
    ) -> BoxFuture<'a, Result<Option<i64>, Error>> {
        #[cfg(test)]
        return self.testing.run("compare_incr", state_key, None, move || {
            self.inner
                .compare_incr_and_set(counter_key, by, state_key, expected_state, state)
        });
        #[cfg(not(test))]
        self.inner
            .compare_incr_and_set(counter_key, by, state_key, expected_state, state)
    }

    fn compare_and_swap<'a>(
        &'a self,
        key: &'a str,
        expected: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, Error>> {
        #[cfg(test)]
        return self.testing.run("compare_swap", key, Some(ttl), move || {
            self.inner.compare_and_swap(key, expected, value, ttl)
        });
        #[cfg(not(test))]
        self.inner.compare_and_swap(key, expected, value, ttl)
    }

    fn seed_counter<'a>(
        &'a self,
        key: &'a str,
        value: i64,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, Error>> {
        self.inner.seed_counter(key, value, ttl)
    }

    fn reserve_spend<'a>(
        &'a self,
        used_key: &'a str,
        pending_key: &'a str,
        estimate: i64,
        limit: i64,
        pending_ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<gproxy_core::SpendReserve, Error>> {
        self.inner
            .reserve_spend(used_key, pending_key, estimate, limit, pending_ttl)
    }

    fn reserve_spend_and_set<'a>(
        &'a self,
        used_key: &'a str,
        pending_key: &'a str,
        estimate: i64,
        limit: i64,
        pending_ttl: Option<Duration>,
        state_key: &'a str,
        expected_state: Vec<u8>,
        state: Vec<u8>,
    ) -> BoxFuture<'a, Result<Option<gproxy_core::SpendReserve>, Error>> {
        #[cfg(test)]
        return self.testing.run("reserve_state", state_key, None, move || {
            self.inner.reserve_spend_and_set(
                used_key,
                pending_key,
                estimate,
                limit,
                pending_ttl,
                state_key,
                expected_state,
                state,
            )
        });
        #[cfg(not(test))]
        self.inner.reserve_spend_and_set(
            used_key,
            pending_key,
            estimate,
            limit,
            pending_ttl,
            state_key,
            expected_state,
            state,
        )
    }

    fn raise_counter<'a>(
        &'a self,
        key: &'a str,
        floor: i64,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        #[cfg(test)]
        return self.testing.run("raise", key, None, move || {
            self.inner.raise_counter(key, floor, ttl)
        });
        #[cfg(not(test))]
        self.inner.raise_counter(key, floor, ttl)
    }
}
