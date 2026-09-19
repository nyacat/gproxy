use gproxy_channel_api::BoxFuture;
use gproxy_core::{CacheBackend, CoreError, Settlement};

use super::super::AppHost;
use super::types::{AdmissionState, reservation_key};

pub(in crate::host) fn finish<'a>(
    host: &'a AppHost,
    request_id: &'a str,
    settlement: Option<&'a Settlement>,
) -> BoxFuture<'a, ()> {
    Box::pin(async move {
        if let Err(error) = finish_checked(host, request_id, settlement).await {
            tracing::error!(request_id, cost = ?settlement.map(|value| value.cost),
                error = %error, "quota settlement requires replay; unreleased reservations retained");
            if settlement.is_none() {
                super::super::settlement_recovery::retain_refund(host, request_id).await;
            }
        }
    })
}

pub(in crate::host) fn finish_checked<'a>(
    host: &'a AppHost,
    request_id: &'a str,
    settlement: Option<&'a Settlement>,
) -> BoxFuture<'a, Result<(), CoreError>> {
    Box::pin(async move {
        host.services.token_counts.forget(request_id);
        super::super::settlement_retry::run(host, || finish_once(host, request_id, settlement))
            .await
    })
}

// A failed fallback only refunds its newly appended reservations. Earlier
// attempts retain their original windows until the final settlement.
pub(super) async fn refund_from(
    host: &AppHost,
    request_id: &str,
    start: usize,
) -> Result<(), CoreError> {
    super::super::settlement_retry::run(host, || refund_once(host, request_id, start)).await
}

async fn refund_once(host: &AppHost, request_id: &str, start: usize) -> Result<(), CoreError> {
    let key = reservation_key(request_id);
    for _ in 0..64 {
        let Some(mut state) = load(host, request_id).await? else {
            return Ok(());
        };
        let expected = serde_json::to_vec(&state).expect("admission state serializes");
        let Some(reservation) = state
            .reservations
            .iter_mut()
            .skip(start)
            .find(|r| !r.released)
        else {
            return Ok(());
        };
        reservation.released = true;
        let pending = reservation.cache_key.clone();
        let refund = -reservation.estimated_cost_micros;
        let updated = serde_json::to_vec(&state).expect("admission state serializes");
        host.services
            .cache
            .compare_incr_and_set(&pending, refund, &key, expected, updated)
            .await?;
    }
    Err(CoreError::Store(gproxy_core::error::StoreError(
        "admission rollback remained contended".into(),
    )))
}

async fn finish_once(
    host: &AppHost,
    request_id: &str,
    settlement: Option<&Settlement>,
) -> Result<(), CoreError> {
    // Included in admission replay: disabled usage and failed/cancelled attempts
    // still end, while an unresolved charge continues to invalidate estimates.
    host.services
        .store
        .finish_credential_usage(request_id)
        .await
        .map_err(super::super::settlement_recovery::store_error)?;
    super::credential_budget::release(host, request_id, settlement).await?;
    let key = reservation_key(request_id);
    let mut conflicts = 0;
    while conflicts < 8 {
        let Some(mut state) = load(host, request_id).await? else {
            return Ok(());
        };
        let expected = serde_json::to_vec(&state).expect("admission state serializes");
        let Some(index) = state
            .reservations
            .iter()
            .position(|reservation| !reservation.released)
        else {
            if host
                .services
                .cache
                .compare_and_swap(&key, Some(expected), None, None)
                .await?
            {
                return Ok(());
            }
            conflicts += 1;
            continue;
        };
        let reservation = &mut state.reservations[index];
        if let Some(settlement) = settlement {
            super::window::commit(
                host,
                request_id,
                reservation.window_id,
                reservation.quota_id,
                settlement.cost,
            )
            .await?;
            reservation.cost_recorded = true;
        }
        reservation.released = true;
        let pending = reservation.cache_key.clone();
        let refund = -reservation.estimated_cost_micros;
        let updated = serde_json::to_vec(&state).expect("admission state serializes");
        // The state comparison is also the deduplication token for the refund.
        // Cache errors leave both the state and pending intact for retry.
        if host
            .services
            .cache
            .compare_incr_and_set(&pending, refund, &key, expected, updated)
            .await?
            .is_some()
        {
            conflicts = 0;
        } else {
            conflicts += 1;
        }
    }
    // More reservations or a concurrent finisher: retain progress and retry.
    Err(CoreError::Store(gproxy_core::error::StoreError(
        "admission settlement has remaining reservations".into(),
    )))
}

pub(in crate::host) async fn load(
    host: &AppHost,
    request_id: &str,
) -> Result<Option<AdmissionState>, CoreError> {
    host.services
        .cache
        .get(&reservation_key(request_id))
        .await?
        .map(|bytes| {
            serde_json::from_slice(&bytes)
                .map_err(|error| CoreError::Internal(format!("decode admission: {error}")))
        })
        .transpose()
}
