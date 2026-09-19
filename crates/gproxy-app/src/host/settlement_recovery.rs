use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

use gproxy_core::{CacheBackend, CoreError};

use super::AppHost;
use super::sinks::UsageReplay;

const IO_TIMEOUT: Duration = Duration::from_secs(5);
const CACHE_BUCKETS: u64 = 32;
const CACHE_BUCKET_BYTES: usize = 1024 * 1024;
const CACHE_BUCKET_ITEMS: usize = 256;
const PAGE_SIZE: u32 = 4;
#[cfg(not(target_arch = "wasm32"))]
const REPLAY_CONCURRENCY: usize = 4;

#[derive(Default)]
pub(crate) struct RecoveryState {
    cursor: std::sync::Arc<std::sync::Mutex<Cursor>>,
    #[cfg(all(test, not(target_arch = "wasm32")))]
    probe: std::sync::Mutex<Option<std::sync::Arc<tests::ReplayProbe>>>,
}

#[derive(Default)]
struct Cursor {
    after: Option<String>,
    slot: u64,
    running: bool,
    next_poll: Option<web_time::Instant>,
    known: std::collections::BTreeSet<String>,
    overflow: bool,
    activity: u64,
    storing: usize,
    empty_buckets: u32,
}

impl RecoveryState {
    fn remember(&self, request_id: &str) {
        let mut cursor = self.cursor.lock().expect("settlement recovery cursor");
        if cursor.known.len() < 8192 {
            cursor.known.insert(request_id.into());
        } else {
            // Preserve tracked requests. A complete empty sweep disables this
            // fallback once the exceptional backlog has drained.
            cursor.overflow = true;
        }
    }

    pub(super) fn knows(&self, request_id: &str) -> bool {
        let cursor = self.cursor.lock().expect("settlement recovery cursor");
        cursor.overflow || cursor.known.contains(request_id)
    }

    fn forget(&self, request_id: &str) {
        self.cursor
            .lock()
            .expect("settlement recovery cursor")
            .known
            .remove(request_id);
    }
}

struct Retaining(std::sync::Arc<std::sync::Mutex<Cursor>>);

impl Drop for Retaining {
    fn drop(&mut self) {
        let mut cursor = self.0.lock().expect("settlement recovery cursor");
        cursor.storing -= 1;
        cursor.activity = cursor.activity.wrapping_add(1);
        cursor.empty_buckets = 0;
    }
}

struct Pass {
    cursor: std::sync::Arc<std::sync::Mutex<Cursor>>,
    progressed: std::sync::atomic::AtomicBool,
}

impl Drop for Pass {
    fn drop(&mut self) {
        let mut cursor = self.cursor.lock().expect("settlement recovery cursor");
        cursor.running = false;
        cursor.next_poll = (!self.progressed.load(std::sync::atomic::Ordering::Relaxed))
            .then(|| web_time::Instant::now() + Duration::from_secs(1));
    }
}

pub(super) fn store_error(error: gproxy_store::StoreError) -> CoreError {
    CoreError::Store(gproxy_core::error::StoreError(error.to_string()))
}

fn recovery_error(message: &str) -> CoreError {
    CoreError::Store(gproxy_core::error::StoreError(message.into()))
}

pub(super) async fn bounded<T>(
    operation: impl Future<Output = Result<T, CoreError>>,
) -> Result<T, CoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        tokio::time::timeout(IO_TIMEOUT, operation)
            .await
            .map_err(|_| recovery_error("settlement storage deadline exceeded"))?
    }
    #[cfg(target_arch = "wasm32")]
    {
        let timeout = gloo_timers::future::TimeoutFuture::new(IO_TIMEOUT.as_millis() as u32);
        futures_util::pin_mut!(operation, timeout);
        match futures_util::future::select(operation, timeout).await {
            futures_util::future::Either::Left((result, _)) => result,
            futures_util::future::Either::Right(_) => {
                Err(recovery_error("settlement storage deadline exceeded"))
            }
        }
    }
}

/// Only incomplete settlements enter recovery. SQL is authoritative; a bounded
/// cache bucket contains both the index and full payload if SQL is unavailable.
pub(super) async fn retain(host: &AppHost, replay: &UsageReplay) -> Result<(), CoreError> {
    let request_id = &replay.settlement.request_id;
    match serde_json::to_value(replay) {
        Ok(payload) => retain_payload(host, request_id, &payload).await,
        Err(error) => {
            tracing::error!(request_id, error = %error, "settlement recovery serialization failed");
            Err(CoreError::Internal(error.to_string()))
        }
    }
}

pub(super) async fn retain_refund(host: &AppHost, request_id: &str) {
    let payload = serde_json::json!({"version": 1, "refund_request_id": request_id});
    let _ = retain_payload(host, request_id, &payload).await;
}

async fn retain_payload(
    host: &AppHost,
    request_id: &str,
    payload: &serde_json::Value,
) -> Result<(), CoreError> {
    let tracking = host.services.settlement_recovery.cursor.clone();
    {
        let mut cursor = tracking.lock().expect("settlement recovery cursor");
        cursor.activity = cursor.activity.wrapping_add(1);
        cursor.empty_buckets = 0;
        cursor.storing += 1;
    }
    let _retaining = Retaining(tracking);
    host.services.settlement_recovery.remember(request_id);
    let result = bounded(enqueue_payload(host, request_id, payload)).await;
    match result {
        Ok(()) => {
            tracing::warn!(
                request_id,
                "incomplete settlement queued in SQL for recovery"
            );
            Ok(())
        }
        Err(sql_error) => match bounded(cache_insert(host, request_id, payload)).await {
            Ok(()) => {
                tracing::warn!(request_id, error = %sql_error,
                "incomplete settlement queued in cache; durability depends on the configured cache");
                Ok(())
            }
            Err(cache_error) => {
                tracing::error!(request_id, sql_error = %sql_error, cache_error = %cache_error,
                "settlement recovery payload retention could not be confirmed; admission remains unresolved");
                Err(cache_error)
            }
        },
    }
}

async fn enqueue_payload(
    host: &AppHost,
    request_id: &str,
    payload: &serde_json::Value,
) -> Result<(), CoreError> {
    host.services
        .store
        .enqueue_settlement_replay(request_id, payload)
        .await
        .map_err(store_error)?;
    // A refund cannot hide an actual billable settlement for the same request.
    // Replacement is conditional so concurrent phase advancement never regresses.
    {
        for _ in 0..8 {
            let Some(existing) = host
                .services
                .store
                .get_settlement_replay(request_id)
                .await
                .map_err(store_error)?
            else {
                return Err(recovery_error("settlement recovery entry disappeared"));
            };
            let merged = merge_progress(&existing, payload)?;
            if merged == existing {
                if existing
                    .get("completed")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
                {
                    host.services.settlement_recovery.forget(request_id);
                }
                return Ok(());
            }
            if host
                .services
                .store
                .replace_settlement_replay(request_id, &existing, &merged)
                .await
                .map_err(store_error)?
            {
                return Ok(());
            }
        }
        Err(recovery_error("settlement recovery upgrade contention"))
    }
}

fn merge_progress(
    existing: &serde_json::Value,
    incoming: &serde_json::Value,
) -> Result<serde_json::Value, CoreError> {
    if existing
        .get("completed")
        .and_then(serde_json::Value::as_bool)
        == Some(true)
        || incoming.get("settlement").is_none()
    {
        return Ok(existing.clone());
    }
    if existing.get("refund_request_id").is_some() {
        return Ok(incoming.clone());
    }
    if existing.get("settlement") != incoming.get("settlement") {
        return Err(CoreError::Internal(
            "conflicting settlements share a recovery request id".into(),
        ));
    }
    let mut merged = existing.clone();
    for field in ["inputs", "credential_charges"] {
        if merged.get(field).is_none_or(serde_json::Value::is_null)
            && incoming.get(field).is_some_and(|value| !value.is_null())
        {
            merged[field] = incoming[field].clone();
        }
    }
    for field in ["usage_recorded", "identity_required"] {
        if incoming.get(field).and_then(serde_json::Value::as_bool) == Some(true) {
            merged[field] = serde_json::json!(true);
        }
    }
    Ok(merged)
}

pub(super) async fn load_known(
    host: &AppHost,
    request_id: &str,
) -> Result<Option<serde_json::Value>, CoreError> {
    bounded(async {
        if let Some(payload) = host
            .services
            .store
            .get_settlement_replay(request_id)
            .await
            .map_err(store_error)?
        {
            return Ok(Some(payload));
        }
        let key = bucket_key(bucket(request_id));
        let bytes = host.services.cache.get(&key).await?;
        Ok(decode_bucket(bytes.as_deref())?.remove(request_id))
    })
    .await
}

fn bucket(request_id: &str) -> u64 {
    request_id
        .bytes()
        .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
        % CACHE_BUCKETS
}

fn bucket_key(bucket: u64) -> String {
    format!("gproxy:settlement-recovery:v1:{bucket}")
}

fn decode_bucket(bytes: Option<&[u8]>) -> Result<BTreeMap<String, serde_json::Value>, CoreError> {
    match bytes {
        Some(bytes) if bytes.len() > CACHE_BUCKET_BYTES => Err(recovery_error(
            "settlement recovery bucket exceeds its size limit",
        )),
        Some(bytes) => serde_json::from_slice(bytes).map_err(|error| {
            CoreError::Internal(format!("decode settlement recovery bucket: {error}"))
        }),
        None => Ok(BTreeMap::new()),
    }
}

async fn cache_insert(
    host: &AppHost,
    request_id: &str,
    payload: &serde_json::Value,
) -> Result<(), CoreError> {
    let key = bucket_key(bucket(request_id));
    for _ in 0..8 {
        let expected = host.services.cache.get(&key).await?;
        let mut entries = decode_bucket(expected.as_deref())?;
        let present = entries.get(request_id);
        let payload = match present {
            Some(existing) => {
                let merged = merge_progress(existing, payload)?;
                if merged == *existing {
                    return Ok(());
                }
                merged
            }
            None => payload.clone(),
        };
        if present.is_none() && entries.len() >= CACHE_BUCKET_ITEMS {
            return Err(recovery_error("settlement recovery cache bucket is full"));
        }
        entries.insert(request_id.to_owned(), payload);
        let updated =
            serde_json::to_vec(&entries).map_err(|error| CoreError::Internal(error.to_string()))?;
        if updated.len() > CACHE_BUCKET_BYTES {
            return Err(recovery_error(
                "settlement recovery cache byte limit reached",
            ));
        }
        if host
            .services
            .cache
            .compare_and_swap(&key, expected, Some(updated), None)
            .await?
        {
            return Ok(());
        }
    }
    Err(recovery_error("settlement recovery cache contention"))
}

async fn cache_remove(
    host: &AppHost,
    key: &str,
    request_id: &str,
    payload: &serde_json::Value,
) -> Result<(), CoreError> {
    for _ in 0..8 {
        let expected = host.services.cache.get(key).await?;
        let mut entries = decode_bucket(expected.as_deref())?;
        if entries.get(request_id) != Some(payload) {
            return Ok(());
        }
        entries.remove(request_id);
        let updated = if entries.is_empty() {
            None
        } else {
            Some(
                serde_json::to_vec(&entries)
                    .map_err(|error| CoreError::Internal(error.to_string()))?,
            )
        };
        if host
            .services
            .cache
            .compare_and_swap(key, expected, updated, None)
            .await?
        {
            return Ok(());
        }
    }
    Err(recovery_error(
        "settlement recovery cache removal contention",
    ))
}

/// A cache entry is removed only after SQL accepts it. Insert-if-absent never
/// overwrites progress when an earlier SQL write committed but lost its reply.
async fn promote_cache(host: &AppHost, slot: u64) -> Result<bool, CoreError> {
    let key = bucket_key(slot);
    let bytes =
        bounded(async { host.services.cache.get(&key).await.map_err(CoreError::from) }).await?;
    let entries = decode_bucket(bytes.as_deref())?;
    let emptied = entries.len() <= PAGE_SIZE as usize;
    for (request_id, payload) in entries.into_iter().take(PAGE_SIZE as usize) {
        bounded(enqueue_payload(host, &request_id, &payload)).await?;
        bounded(cache_remove(host, &key, &request_id, &payload)).await?;
    }
    Ok(emptied)
}

async fn replay_one(
    host: &AppHost,
    request_id: &str,
    payload: serde_json::Value,
) -> Result<(), CoreError> {
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _test_replay = {
        let probe = host
            .services
            .settlement_recovery
            .probe
            .lock()
            .unwrap()
            .clone();
        match probe {
            Some(probe) => Some(probe.enter(request_id).await),
            None => None,
        }
    };
    if let Some(refund_request_id) = payload.get("refund_request_id") {
        if refund_request_id.as_str() != Some(request_id)
            || payload.get("version").and_then(serde_json::Value::as_u64) != Some(1)
        {
            return Err(CoreError::Internal(
                "invalid admission refund recovery payload".into(),
            ));
        }
        bounded(super::admission::finish_checked(host, request_id, None)).await?;
        return complete_progress(host, request_id, &payload).await;
    }
    let mut replay: UsageReplay = serde_json::from_value(payload).map_err(|error| {
        CoreError::Internal(format!("decode settlement recovery payload: {error}"))
    })?;
    if replay.settlement.request_id != request_id {
        return Err(CoreError::Internal(
            "settlement recovery request id mismatch".into(),
        ));
    }
    bounded(replay.prepare(host)).await?;
    // Freeze identity before writes. A crash after admission release must not
    // require the now-deleted identity to reconstruct usage.
    persist_progress(host, &replay).await?;
    if !replay.usage_recorded {
        bounded(replay.apply(host)).await?;
        replay.usage_recorded = true;
        // Persist the phase before releasing credential windows. Replaying a
        // completed record must never choose a new window from current time.
        persist_progress(host, &replay).await?;
    }
    bounded(super::admission::finish_checked(
        host,
        request_id,
        Some(&replay.settlement),
    ))
    .await?;
    let payload =
        serde_json::to_value(&replay).map_err(|error| CoreError::Internal(error.to_string()))?;
    complete_progress(host, request_id, &payload).await?;
    tracing::info!(request_id, "incomplete settlement recovered");
    Ok(())
}

async fn complete_progress(
    host: &AppHost,
    request_id: &str,
    payload: &serde_json::Value,
) -> Result<(), CoreError> {
    bounded(async {
        if host
            .services
            .store
            .complete_settlement_replay_if(request_id, payload)
            .await
            .map_err(store_error)?
        {
            host.services.settlement_recovery.forget(request_id);
            return Ok(());
        }
        let current = host
            .services
            .store
            .get_settlement_replay(request_id)
            .await
            .map_err(store_error)?;
        if current
            .as_ref()
            .and_then(|value| value.get("completed"))
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            host.services.settlement_recovery.forget(request_id);
            Ok(())
        } else {
            Err(recovery_error(
                "settlement recovery task changed before completion",
            ))
        }
    })
    .await
}

async fn persist_progress(host: &AppHost, replay: &UsageReplay) -> Result<(), CoreError> {
    let payload =
        serde_json::to_value(replay).map_err(|error| CoreError::Internal(error.to_string()))?;
    bounded(enqueue_payload(
        host,
        &replay.settlement.request_id,
        &payload,
    ))
    .await
}

impl crate::AppHandle {
    /// Performs one bounded recovery pass. Edge hosts retain this future using
    /// their request continuation; native hosts also schedule it at startup.
    pub async fn recover_pending_settlements(&self, limit: u32) -> Result<u32, CoreError> {
        recover_pending(&self.inner.host, limit).await
    }
}

async fn recover_pending(host: &AppHost, limit: u32) -> Result<u32, CoreError> {
    if limit == 0 {
        return Ok(0);
    }
    let state = &host.services.settlement_recovery;
    let (after, slot, activity) = {
        let mut cursor = state.cursor.lock().expect("settlement recovery cursor");
        if cursor.running
            || cursor
                .next_poll
                .is_some_and(|at| at > web_time::Instant::now())
        {
            return Ok(0);
        }
        cursor.running = true;
        let slot = cursor.slot;
        cursor.slot = (slot + 1) % CACHE_BUCKETS;
        (cursor.after.clone(), slot, cursor.activity)
    };
    let pass = std::sync::Arc::new(Pass {
        cursor: state.cursor.clone(),
        progressed: std::sync::atomic::AtomicBool::new(false),
    });
    let cache_empty = match promote_cache(host, slot).await {
        Ok(empty) => empty,
        Err(error) => {
            tracing::warn!(error = %error, "settlement recovery cache scan failed");
            false
        }
    };
    let page = bounded(async {
        host.services
            .store
            .list_settlement_replays(after.as_deref(), limit.min(32))
            .await
            .map_err(store_error)
    })
    .await?;
    {
        let mut cursor = state.cursor.lock().expect("settlement recovery cursor");
        cursor.after = page.last().map(|(id, _)| id.clone());
        if cursor.activity == activity {
            if cache_empty {
                cursor.empty_buckets |= 1 << slot;
            } else {
                cursor.empty_buckets &= !(1 << slot);
            }
            if after.is_none()
                && page.is_empty()
                && cursor.empty_buckets == u32::MAX
                && cursor.known.is_empty()
                && cursor.storing == 0
            {
                cursor.overflow = false;
            }
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        use futures_util::{StreamExt, stream};

        // Only polled entries spawn work. Dropping this stream stops dispatching
        // the rest of the SQL page, while started writes remain tracked.
        let replays = stream::iter(page)
            .map(|(request_id, payload)| {
                let task_host = host.clone();
                let task_pass = pass.clone();
                async move {
                    task_host.services.settlement_recovery.remember(&request_id);
                    let log_id = request_id.clone();
                    let replay_host = task_host.clone();
                    let result = task_host
                        .services
                        .spawner
                        .spawn_tracked(async move {
                            let result = replay_one(&replay_host, &request_id, payload).await;
                            if result.is_ok() {
                                task_pass
                                    .progressed
                                    .store(true, std::sync::atomic::Ordering::Relaxed);
                            } else if let Err(error) = &result {
                                tracing::warn!(request_id, error = %error,
                                    "settlement remains queued for recovery");
                            }
                            // The last started write, not its caller, releases
                            // single-flight ownership after cancellation.
                            drop(task_pass);
                            result
                        })
                        .await;
                    match result {
                        Ok(result) => result.is_ok(),
                        Err(error) => {
                            tracing::warn!(request_id = log_id, error = %error,
                                "settlement recovery task failed; entry remains queued");
                            false
                        }
                    }
                }
            })
            .buffer_unordered(REPLAY_CONCURRENCY);
        futures_util::pin_mut!(replays);
        let mut completed = 0;
        while let Some(succeeded) = replays.next().await {
            completed += u32::from(succeeded);
        }
        Ok(completed)
    }
    #[cfg(target_arch = "wasm32")]
    {
        let mut completed = 0;
        for (request_id, payload) in page {
            state.remember(&request_id);
            match replay_one(host, &request_id, payload).await {
                Ok(()) => {
                    completed += 1;
                    pass.progressed
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
                Err(error) => {
                    tracing::warn!(request_id, error = %error,
                        "settlement remains queued for recovery");
                }
            }
        }
        Ok(completed)
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn start(app: &crate::AppHandle) {
    let host = app.inner.host.clone();
    app.inner
        .host
        .services
        .spawner
        .spawn_maintenance(app.inner.shutdown.subscribe(), async move {
            loop {
                match recover_pending(&host, 32).await {
                    Ok(completed) if completed > 0 => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Ok(_) => {}
                    Err(error) => tracing::warn!(error = %error, "settlement recovery scan failed"),
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn start(_app: &crate::AppHandle) {}

#[cfg(test)]
mod tests;
