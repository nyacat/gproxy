use std::time::Duration;

use gproxy_core::{CacheBackend, CoreError, SpendReserve};
use gproxy_store::records::{QuotaWindowKind, QuotaWindowRecord};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::super::AppHost;

const VIEW_TTL: Duration = Duration::from_secs(3600);
/// A reservation outlives the used view: it is released by its own request, not
/// by a clock. This expiry is only the backstop for a host that was killed
/// before it could settle, whose share of pending would otherwise shrink the
/// effective limit of a window that never resets until someone edits Redis by
/// hand. Every accepted reservation refreshes it, so it can only elapse once a
/// window has stopped admitting entirely — which is exactly the state a stranded
/// reservation causes. Keep it far longer than any request may plausibly run:
/// expiring pending under a live request would release its reservation early.
pub(super) const RESERVATION_TTL: Duration = Duration::from_secs(6 * 3600);
const BIND_RETRIES: usize = 8;

#[derive(Serialize, Deserialize)]
struct WindowMeta {
    id: i64,
    reset_at: Option<i64>,
}

pub(super) fn pending_key(window_id: i64) -> String {
    format!("gproxy:quota-pending:{window_id}")
}

fn used_key(window_id: i64) -> String {
    format!("gproxy:quota-used:{window_id}")
}

fn meta_key(quota_id: i64, kind: QuotaWindowKind) -> String {
    format!("gproxy:quota-window:{quota_id}:{}", kind.as_str())
}

pub(super) fn failure_key(quota_id: i64) -> String {
    format!("gproxy:quota-failed:{quota_id}")
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn reserve(
    host: &AppHost,
    quota_id: i64,
    kind: QuotaWindowKind,
    limit: Decimal,
    estimate: i64,
    now: i64,
    state_key: &str,
    update: impl FnOnce(i64) -> (Vec<u8>, Vec<u8>),
) -> Result<i64, CoreError> {
    let limit = gproxy_core::usage::cost_to_micros(limit)
        .ok_or_else(|| CoreError::Internal("quota limit exceeds counter".into()))?;
    let window = super::super::settlement_retry::deadline(bind(host, quota_id, kind, now)).await?;
    let pending = pending_key(window.id);
    let used = used_key(window.id);
    let (expected, updated) = update(window.id);
    // The reservation and its refund token commit together. Repeating the exact
    // transition after a lost reply cannot increase pending a second time.
    super::super::settlement_retry::run(host, || async {
        for _ in 0..BIND_RETRIES {
            match host
                .services
                .cache
                .reserve_spend_and_set(
                    &used,
                    &pending,
                    estimate,
                    limit,
                    Some(RESERVATION_TTL),
                    state_key,
                    expected.clone(),
                    updated.clone(),
                )
                .await?
            {
                Some(SpendReserve::Allowed) => return Ok(()),
                Some(SpendReserve::Denied) => return Err(CoreError::QuotaExceeded),
                None => {
                    return Err(CoreError::Internal(
                        "admission changed during quota reservation".into(),
                    ));
                }
                Some(SpendReserve::MissingUsed) => seed_used(host, window.id).await?,
            }
        }
        Err(CoreError::Store(gproxy_core::error::StoreError(
            "quota used counter remained unset".into(),
        )))
    })
    .await?;
    Ok(window.id)
}

pub(super) async fn commit(
    host: &AppHost,
    request_id: &str,
    window_id: i64,
    quota_id: i64,
    cost: Decimal,
) -> Result<(), CoreError> {
    let result = async {
        // The ledger deduplicates by request and window. Publishing its total
        // before releasing pending is conservative even if these steps retry.
        let window = match host
            .services
            .store
            .add_quota_cost(request_id, window_id, cost)
            .await
        {
            Ok(window) => window,
            Err(gproxy_store::StoreError::QuotaWindowMissing(_)) => {
                // Deleting a quota also deletes its ledger. Only its own
                // reservations are retired; other quotas still settle normally.
                if !host
                    .services
                    .store
                    .quota_exists(quota_id)
                    .await
                    .map_err(store_error)?
                {
                    return failure(host, quota_id, request_id, window_id, false).await;
                }
                return Err(CoreError::Internal(format!(
                    "quota {quota_id} still exists but window {window_id} is missing"
                )));
            }
            Err(error) => return Err(store_error(error)),
        };
        let used = gproxy_core::usage::cost_to_micros(window.cost_used)
            .ok_or_else(|| CoreError::Internal("quota used exceeds counter".into()))?;
        host.services
            .cache
            .raise_counter(&used_key(window_id), used, Some(VIEW_TTL))
            .await?;
        failure(host, quota_id, request_id, window_id, false).await
    }
    .await;
    if let Err(error) = &result {
        tracing::debug!(request_id, quota_id, window_id, error = %error,
            "quota window settlement attempt failed");
    }
    if result.is_err()
        && let Err(error) = failure(host, quota_id, request_id, window_id, true).await
    {
        tracing::error!(request_id, quota_id, error = %error, "mark quota accounting failure failed");
    }
    result
}

// Failures block new admission until every failed settlement has recovered.
// A successful request cannot clear another request's accounting failure.
async fn failure(
    host: &AppHost,
    quota_id: i64,
    request: &str,
    window_id: i64,
    failed: bool,
) -> Result<(), CoreError> {
    let key = failure_key(quota_id);
    for _ in 0..BIND_RETRIES {
        let expected = host.services.cache.get(&key).await?;
        let mut requests: std::collections::BTreeSet<(String, i64)> = match &expected {
            Some(bytes) => match serde_json::from_slice(bytes) {
                Ok(requests) => requests,
                // Preserve older manually managed failure markers.
                Err(_) => return Ok(()),
            },
            None => Default::default(),
        };
        let changed = if failed {
            requests.insert((request.to_owned(), window_id))
        } else {
            requests.remove(&(request.to_owned(), window_id))
        };
        if !changed {
            return Ok(());
        }
        let value = if requests.is_empty() {
            None
        } else {
            Some(serde_json::to_vec(&requests).expect("failure set serializes"))
        };
        if host
            .services
            .cache
            .compare_and_swap(&key, expected, value, None)
            .await?
        {
            return Ok(());
        }
    }
    Err(CoreError::Store(gproxy_core::error::StoreError(
        "quota failure state remained contended".into(),
    )))
}

async fn bind(
    host: &AppHost,
    quota_id: i64,
    kind: QuotaWindowKind,
    now: i64,
) -> Result<QuotaWindowRecord, CoreError> {
    let meta_key = meta_key(quota_id, kind);
    if let Some(bytes) = host.services.cache.get(&meta_key).await?
        && let Ok(meta) = serde_json::from_slice::<WindowMeta>(&bytes)
        && meta.reset_at.is_none_or(|reset| reset > now)
    {
        return Ok(QuotaWindowRecord {
            id: meta.id,
            quota_id,
            window_kind: kind,
            window_start: 0,
            reset_at: meta.reset_at,
            cost_used: Decimal::ZERO,
        });
    }
    let window = host
        .services
        .store
        .ensure_quota_window(quota_id, kind, now)
        .await
        .map_err(store_error)?;
    let encoded = serde_json::to_vec(&WindowMeta {
        id: window.id,
        reset_at: window.reset_at,
    })
    .map_err(|error| CoreError::Internal(format!("serialize quota window: {error}")))?;
    host.services
        .cache
        .set(&meta_key, encoded, Some(VIEW_TTL))
        .await?;
    seed_used(host, window.id).await?;
    Ok(window)
}

async fn seed_used(host: &AppHost, window_id: i64) -> Result<(), CoreError> {
    let window = host
        .services
        .store
        .quota_window(window_id)
        .await
        .map_err(store_error)?
        .ok_or_else(|| {
            CoreError::Store(gproxy_core::error::StoreError(
                "quota window vanished after reservation".into(),
            ))
        })?;
    let used = gproxy_core::usage::cost_to_micros(window.cost_used)
        .ok_or_else(|| CoreError::Internal("quota used exceeds counter".into()))?;
    host.services
        .cache
        .seed_counter(&used_key(window_id), used, Some(VIEW_TTL))
        .await?;
    Ok(())
}

fn store_error(error: gproxy_store::StoreError) -> CoreError {
    match error {
        gproxy_store::StoreError::InvalidData { .. } => CoreError::Internal(error.to_string()),
        _ => CoreError::Store(gproxy_core::error::StoreError(error.to_string())),
    }
}
