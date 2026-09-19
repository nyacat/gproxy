use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gproxy_core::CacheBackend;
use gproxy_core::channel_api::BoxFuture;
use web_time::Instant;

type Error = gproxy_core::error::StoreError;

fn cache_error(message: &str) -> Error {
    gproxy_core::error::StoreError(message.into())
}

#[derive(Clone, Default)]
pub struct InProcessCache {
    entries: Arc<Mutex<Entries>>,
}

#[derive(Default)]
struct Entries {
    values: HashMap<String, Entry>,
    expirations: BTreeSet<(Instant, String)>,
}

struct Entry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

impl Entries {
    fn get(&self, key: &str) -> Option<&Entry> {
        self.values.get(key)
    }

    fn contains_key(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }

    fn insert(&mut self, key: String, entry: Entry) {
        use std::collections::hash_map::Entry;
        let slot = self.values.entry(key);
        let previous = match &slot {
            Entry::Occupied(slot) => slot.get().expires_at,
            Entry::Vacant(_) => None,
        };
        if previous != entry.expires_at {
            if let Some(at) = previous {
                self.expirations.remove(&(at, slot.key().clone()));
            }
            if let Some(at) = entry.expires_at {
                self.expirations.insert((at, slot.key().clone()));
            }
        }
        slot.insert_entry(entry);
    }

    fn remove(&mut self, key: &str) {
        if let Some(entry) = self.values.remove(key)
            && let Some(at) = entry.expires_at
        {
            self.expirations.remove(&(at, key.to_owned()));
        }
    }
}

impl CacheBackend for InProcessCache {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, Error>> {
        let result = self.with_entries(|entries| {
            expire(entries, key);
            Ok(entries.get(key).map(|entry| entry.value.clone()))
        });
        Box::pin(async move { result })
    }

    fn set<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        let result = self.with_entries(|entries| {
            entries.insert(
                key.into(),
                Entry {
                    value,
                    expires_at: expiry(ttl)?,
                },
            );
            Ok(())
        });
        Box::pin(async move { result })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        let result = self.with_entries(|entries| {
            entries.remove(key);
            Ok(())
        });
        Box::pin(async move { result })
    }

    fn incr<'a>(
        &'a self,
        key: &'a str,
        by: i64,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<i64, Error>> {
        let result = self.with_entries(|entries| {
            expire(entries, key);
            let (current, expires_at) = match entries.get(key) {
                Some(entry) => (decode_counter(&entry.value)?, entry.expires_at),
                None => (0, expiry(ttl)?),
            };
            let value = current.checked_add(by).ok_or_else(overflow)?;
            entries.insert(
                key.into(),
                Entry {
                    value: value.to_be_bytes().to_vec(),
                    expires_at,
                },
            );
            Ok(value)
        });
        Box::pin(async move { result })
    }

    fn compare_incr_and_set<'a>(
        &'a self,
        counter_key: &'a str,
        by: i64,
        state_key: &'a str,
        expected: Vec<u8>,
        state: Vec<u8>,
    ) -> BoxFuture<'a, Result<Option<i64>, Error>> {
        let result = self.with_entries(|entries| {
            expire(entries, state_key);
            if entries.get(state_key).map(|entry| &entry.value) != Some(&expected) {
                return Ok(None);
            }
            // Neither write changes an existing expiry; a counter with nothing
            // reserved against it is retired instead of being made permanent.
            let state_expires_at = entries.get(state_key).and_then(|entry| entry.expires_at);
            expire(entries, counter_key);
            let counter = entries.get(counter_key);
            let current = counter.map_or(Ok(0), |entry| decode_counter(&entry.value))?;
            let counter_expires_at = counter.and_then(|entry| entry.expires_at);
            let next = current.checked_add(by).ok_or_else(overflow)?;
            entries.insert(
                counter_key.into(),
                Entry {
                    value: next.to_be_bytes().to_vec(),
                    expires_at: if next <= 0 {
                        expiry(Some(Duration::from_secs(3600)))?
                    } else {
                        counter_expires_at
                    },
                },
            );
            entries.insert(
                state_key.into(),
                Entry {
                    value: state,
                    expires_at: state_expires_at,
                },
            );
            Ok(Some(next))
        });
        Box::pin(async move { result })
    }

    fn compare_and_swap<'a>(
        &'a self,
        key: &'a str,
        expected: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, Error>> {
        let result = self.with_entries(|entries| {
            expire(entries, key);
            if entries.get(key).map(|entry| &entry.value) != expected.as_ref() {
                return Ok(false);
            }
            match value {
                Some(value) => {
                    entries.insert(
                        key.into(),
                        Entry {
                            value,
                            expires_at: expiry(ttl)?,
                        },
                    );
                }
                None => {
                    entries.remove(key);
                }
            }
            Ok(true)
        });
        Box::pin(async move { result })
    }

    fn seed_counter<'a>(
        &'a self,
        key: &'a str,
        value: i64,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, Error>> {
        let result = self.with_entries(|entries| {
            expire(entries, key);
            if entries.contains_key(key) {
                return Ok(false);
            }
            entries.insert(
                key.into(),
                Entry {
                    value: value.to_be_bytes().to_vec(),
                    expires_at: expiry(ttl)?,
                },
            );
            Ok(true)
        });
        Box::pin(async move { result })
    }

    fn reserve_spend<'a>(
        &'a self,
        used_key: &'a str,
        pending_key: &'a str,
        estimate: i64,
        limit: i64,
        pending_ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<gproxy_core::SpendReserve, Error>> {
        let result = self.with_entries(|entries| {
            expire(entries, used_key);
            expire(entries, pending_key);
            let Some(used) = entries
                .get(used_key)
                .map(|entry| decode_counter(&entry.value))
                .transpose()?
            else {
                return Ok(gproxy_core::SpendReserve::MissingUsed);
            };
            let pending = entries
                .get(pending_key)
                .map_or(Ok(0), |entry| decode_counter(&entry.value))?
                .checked_add(estimate)
                .ok_or_else(overflow)?;
            if !gproxy_core::spend_fits(used, pending, estimate, limit) {
                return Ok(gproxy_core::SpendReserve::Denied);
            }
            let expires_at = expiry(pending_ttl)?;
            entries.insert(
                pending_key.into(),
                Entry {
                    value: pending.to_be_bytes().to_vec(),
                    expires_at,
                },
            );
            Ok(gproxy_core::SpendReserve::Allowed)
        });
        Box::pin(async move { result })
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
        let result = self.with_entries(|entries| {
            expire(entries, state_key);
            let current = entries.get(state_key).map(|entry| &entry.value);
            if current == Some(&state) {
                return Ok(Some(gproxy_core::SpendReserve::Allowed));
            }
            if current != Some(&expected_state) {
                return Ok(None);
            }
            let state_expires_at = entries.get(state_key).and_then(|entry| entry.expires_at);
            expire(entries, used_key);
            expire(entries, pending_key);
            let Some(used) = entries
                .get(used_key)
                .map(|entry| decode_counter(&entry.value))
                .transpose()?
            else {
                return Ok(Some(gproxy_core::SpendReserve::MissingUsed));
            };
            let pending = entries
                .get(pending_key)
                .map_or(Ok(0), |entry| decode_counter(&entry.value))?
                .checked_add(estimate)
                .ok_or_else(overflow)?;
            if !gproxy_core::spend_fits(used, pending, estimate, limit) {
                return Ok(Some(gproxy_core::SpendReserve::Denied));
            }
            // Every accepted reservation refreshes the pending expiry; the
            // admission state keeps the expiry its owner installed.
            entries.insert(
                pending_key.into(),
                Entry {
                    value: pending.to_be_bytes().to_vec(),
                    expires_at: expiry(pending_ttl)?,
                },
            );
            entries.insert(
                state_key.into(),
                Entry {
                    value: state,
                    expires_at: state_expires_at,
                },
            );
            Ok(Some(gproxy_core::SpendReserve::Allowed))
        });
        Box::pin(async move { result })
    }

    fn raise_counter<'a>(
        &'a self,
        key: &'a str,
        floor: i64,
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        let result = self.with_entries(|entries| {
            expire(entries, key);
            let value = entries
                .get(key)
                .map(|entry| decode_counter(&entry.value))
                .transpose()?
                .map_or(floor, |current| current.max(floor));
            entries.insert(
                key.into(),
                Entry {
                    value: value.to_be_bytes().to_vec(),
                    expires_at: expiry(ttl)?,
                },
            );
            Ok(())
        });
        Box::pin(async move { result })
    }
}

impl InProcessCache {
    fn with_entries<T>(
        &self,
        operation: impl FnOnce(&mut Entries) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| cache_error("cache lock poisoned"))?;
        sweep(&mut entries);
        operation(&mut entries)
    }
}

fn expire(entries: &mut Entries, key: &str) {
    if entries
        .get(key)
        .and_then(|entry| entry.expires_at)
        .is_some_and(|expiry| expiry <= Instant::now())
    {
        entries.remove(key);
    }
}

const SWEEP_BATCH: usize = 32;

fn sweep(entries: &mut Entries) {
    if entries.expirations.is_empty() {
        return;
    }
    let now = Instant::now();
    for _ in 0..SWEEP_BATCH {
        if entries.expirations.first().is_none_or(|(at, _)| *at > now) {
            break;
        }
        let (_, key) = entries.expirations.pop_first().expect("due expiration");
        entries.values.remove(&key);
    }
}

fn expiry(ttl: Option<Duration>) -> Result<Option<Instant>, Error> {
    ttl.map(|ttl| {
        Instant::now()
            .checked_add(ttl)
            .ok_or_else(|| cache_error("cache TTL exceeds clock range"))
    })
    .transpose()
}

fn decode_counter(value: &[u8]) -> Result<i64, Error> {
    value
        .try_into()
        .map(i64::from_be_bytes)
        .map_err(|_| cache_error("cache value is not a counter"))
}

fn overflow() -> Error {
    cache_error("cache counter overflow")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn expiry_cleanup_is_bounded_and_visits_untouched_keys() {
        let cache = InProcessCache::default();
        {
            let mut entries = cache.entries.lock().unwrap();
            for i in 0..512 {
                entries.insert(
                    format!("expired-{i:04}"),
                    Entry {
                        value: vec![1],
                        expires_at: Some(Instant::now()),
                    },
                );
            }
        }
        cache.get("absent").await.unwrap();
        assert_eq!(
            cache.entries.lock().unwrap().values.len(),
            512 - SWEEP_BATCH
        );
        for _ in 0..16 {
            cache.get("absent").await.unwrap();
        }
        assert!(cache.entries.lock().unwrap().values.is_empty());
    }

    #[tokio::test]
    async fn replacing_and_deleting_ttls_keeps_one_expiration_per_key() {
        let cache = InProcessCache::default();
        for _ in 0..512 {
            cache
                .set("refresh", vec![1], Some(Duration::from_secs(60)))
                .await
                .unwrap();
        }
        assert_eq!(cache.entries.lock().unwrap().expirations.len(), 1);
        cache.set("refresh", vec![2], None).await.unwrap();
        assert!(cache.entries.lock().unwrap().expirations.is_empty());
        assert_eq!(cache.get("refresh").await.unwrap(), Some(vec![2]));
        cache
            .set("refresh", vec![3], Some(Duration::from_secs(60)))
            .await
            .unwrap();
        cache.delete("refresh").await.unwrap();
        assert!(cache.entries.lock().unwrap().expirations.is_empty());
    }

    #[tokio::test]
    async fn renewing_a_counter_removes_its_old_expiration() {
        let cache = InProcessCache::default();
        cache
            .set("used", 0_i64.to_be_bytes().to_vec(), None)
            .await
            .unwrap();
        cache
            .incr("pending", 1, Some(Duration::from_secs(60)))
            .await
            .unwrap();
        let previous = cache
            .entries
            .lock()
            .unwrap()
            .expirations
            .first()
            .cloned()
            .unwrap();
        cache
            .reserve_spend("used", "pending", 1, 10, None)
            .await
            .unwrap();
        let entries = cache.entries.lock().unwrap();
        assert!(!entries.expirations.contains(&previous));
        assert_eq!(entries.get("pending").unwrap().expires_at, None);
    }

    #[tokio::test]
    async fn atomic_reservation_refreshes_pending_and_keeps_the_state_expiry() {
        let cache = InProcessCache::default();
        cache.seed_counter("used", 0, None).await.unwrap();
        cache
            .incr("pending", 0, Some(Duration::from_secs(60)))
            .await
            .unwrap();
        cache
            .set("state", b"ready".to_vec(), Some(Duration::from_secs(60)))
            .await
            .unwrap();
        let state_expiry = cache
            .entries
            .lock()
            .unwrap()
            .get("state")
            .unwrap()
            .expires_at;
        let short = Instant::now() + Duration::from_secs(120);
        assert_eq!(
            cache
                .reserve_spend_and_set(
                    "used",
                    "pending",
                    1,
                    10,
                    Some(Duration::from_secs(600)),
                    "state",
                    b"ready".to_vec(),
                    b"reserved".to_vec(),
                )
                .await
                .unwrap(),
            Some(gproxy_core::SpendReserve::Allowed),
        );
        let entries = cache.entries.lock().unwrap();
        // A reservation pushes its own backstop further out, but it may neither
        // extend nor drop the expiry of the token that releases it.
        assert!(entries.get("pending").unwrap().expires_at.unwrap() > short);
        assert_eq!(entries.get("state").unwrap().expires_at, state_expiry);
    }
}
