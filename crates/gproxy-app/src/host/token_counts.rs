use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

const MAX_REQUESTS: usize = 8192;
const MAX_VARIANTS: usize = 128;

#[derive(Clone, Default)]
pub(crate) struct TokenCountCache {
    entries: Arc<Mutex<HashMap<String, RequestTokenCounts>>>,
}

#[derive(Clone, Default)]
pub(crate) struct RequestTokenCounts {
    entries: Arc<Mutex<Counts>>,
}

#[derive(Default)]
struct Counts {
    values: Vec<Count>,
    // Keep the allocation alive so its pointer cannot be reused for new bytes.
    // Only the most recent body is retained, regardless of variant count.
    last_body: Option<(Bytes, u64)>,
}

struct Count {
    model: String,
    map: Option<serde_json::Value>,
    body: u64,
    tokens: u64,
}

impl Counts {
    fn body_hash(&self, body: &Bytes) -> Option<u64> {
        self.last_body.as_ref().and_then(|(cached, hash)| {
            (cached.as_ptr() == body.as_ptr() && cached.len() == body.len()).then_some(*hash)
        })
    }

    fn get(&self, model: &str, map: Option<&serde_json::Value>, body: u64) -> Option<u64> {
        self.values
            .iter()
            .rev()
            .find(|entry| entry.body == body && entry.model == model && entry.map.as_ref() == map)
            .map(|entry| entry.tokens)
    }
}

impl TokenCountCache {
    // Capture this handle before spawning blocking work. A cancelled request
    // can remove it without a late tokenizer task re-inserting the request.
    pub(crate) fn request(&self, request_id: &str) -> RequestTokenCounts {
        let mut entries = self.entries.lock().expect("token count cache");
        if let Some(entry) = entries.get(request_id) {
            return entry.clone();
        }
        if entries.len() >= MAX_REQUESTS
            && let Some(old) = entries.keys().next().cloned()
        {
            entries.remove(&old);
        }
        entries.entry(request_id.to_owned()).or_default().clone()
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, request_id: &str) -> bool {
        self.entries.lock().unwrap().contains_key(request_id)
    }

    pub(crate) fn forget(&self, request_id: &str) {
        self.entries
            .lock()
            .expect("token count cache")
            .remove(request_id);
    }

    pub(crate) fn scope<'a>(&'a self, request_id: &'a str) -> AdmissionScope<'a> {
        AdmissionScope {
            cache: self,
            request_id,
            admitted: false,
        }
    }
}

pub(crate) struct AdmissionScope<'a> {
    cache: &'a TokenCountCache,
    request_id: &'a str,
    admitted: bool,
}

impl AdmissionScope<'_> {
    pub(crate) fn keep(&mut self) {
        self.admitted = true;
    }
}

impl Drop for AdmissionScope<'_> {
    fn drop(&mut self) {
        if !self.admitted {
            self.cache.forget(self.request_id);
        }
    }
}

impl RequestTokenCounts {
    /// Cheap enough for the async path: no serialization, body scan, allocation,
    /// or dispatch to the blocking pool when a Bytes clone is already cached.
    pub(crate) fn get(
        &self,
        model: &str,
        map: Option<&serde_json::Value>,
        body: &Bytes,
    ) -> Option<u64> {
        let entries = self.entries.lock().expect("token counts");
        entries.get(model, map, entries.body_hash(body)?)
    }

    pub(crate) fn get_or_insert(
        &self,
        model: &str,
        map: Option<&serde_json::Value>,
        body: &Bytes,
        compute: impl FnOnce() -> u64,
    ) -> u64 {
        let hash = {
            let mut entries = self.entries.lock().expect("token counts");
            let hash = entries.body_hash(body).unwrap_or_else(|| body_hash(body));
            entries.last_body = Some((body.clone(), hash));
            if let Some(tokens) = entries.get(model, map, hash) {
                return tokens;
            }
            hash
        };
        let tokens = compute();
        let mut entries = self.entries.lock().expect("token counts");
        if let Some(tokens) = entries.get(model, map, hash) {
            return tokens;
        }
        if entries.values.len() >= MAX_VARIANTS {
            entries.values.remove(0);
        }
        entries.values.push(Count {
            model: model.to_owned(),
            map: map.cloned(),
            body: hash,
            tokens,
        });
        tokens
    }
}

fn body_hash(body: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    body.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    #[test]
    fn counts_once_per_request_model_and_map() {
        let cache = super::TokenCountCache::default();
        let mut computed = 0;
        let first =
            cache
                .request("r1")
                .get_or_insert("model", None, &Bytes::from_static(b"same"), || {
                    computed += 1;
                    11
                });
        let second =
            cache
                .request("r1")
                .get_or_insert("model", None, &Bytes::from_static(b"same"), || {
                    computed += 1;
                    99
                });
        assert_eq!((first, second, computed), (11, 11, 1));
        let other =
            cache
                .request("r1")
                .get_or_insert("model", None, &Bytes::from_static(b"other"), || {
                    computed += 1;
                    5
                });
        assert_eq!((other, computed), (5, 2));
        cache.forget("r1");
        let third =
            cache
                .request("r1")
                .get_or_insert("model", None, &Bytes::from_static(b"same"), || {
                    computed += 1;
                    7
                });
        assert_eq!((third, computed), (7, 3));
    }

    #[test]
    fn late_tokenizer_work_cannot_restore_a_cancelled_request() {
        let cache = super::TokenCountCache::default();
        let handle;
        {
            let _admission = cache.scope("cancelled");
            handle = cache.request("cancelled");
            assert!(cache.contains("cancelled"));
        }
        handle.get_or_insert("model", None, &Bytes::from_static(b"late"), || 5);
        assert!(!cache.contains("cancelled"));
        assert_eq!(
            cache.request("cancelled").get_or_insert(
                "model",
                None,
                &Bytes::from_static(b"late"),
                || 7
            ),
            7
        );
    }

    #[test]
    fn long_sessions_and_abandoned_requests_have_bounded_cache_space() {
        let cache = super::TokenCountCache::default();
        for i in 0..super::MAX_REQUESTS + 10 {
            cache.request(&i.to_string());
        }
        assert_eq!(cache.entries.lock().unwrap().len(), super::MAX_REQUESTS);
        let request = cache.request("long-session");
        for i in 0..super::MAX_VARIANTS + 10 {
            request.get_or_insert(
                "model",
                None,
                &Bytes::copy_from_slice(&i.to_be_bytes()),
                || 1,
            );
        }
        assert_eq!(
            request.entries.lock().unwrap().values.len(),
            super::MAX_VARIANTS
        );
    }

    #[test]
    fn fast_lookup_reuses_clones_and_distinguishes_models_maps_and_body_slices() {
        let cache = super::RequestTokenCounts::default();
        let body = Bytes::from(vec![b'x'; 1024 * 1024]);
        let map = serde_json::json!({"model": "tokenizer"});
        cache.get_or_insert("model", Some(&map), &body, || 7);
        assert_eq!(
            cache.get("model", Some(&map.clone()), &body.clone()),
            Some(7)
        );
        assert_eq!(cache.get("other", Some(&map), &body), None);
        assert_eq!(cache.get("model", None, &body), None);
        assert_eq!(cache.get("model", Some(&map), &body.slice(..10)), None);
        let copy = Bytes::copy_from_slice(&body);
        assert_eq!(
            cache.get_or_insert("model", Some(&map), &copy, || panic!("same content")),
            7
        );
        assert_eq!(cache.get("model", Some(&map), &copy), Some(7));
    }
}
