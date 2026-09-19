use gproxy_core::{CacheBackend, ControlPlane, CoreError, Settlement, Target};
use gproxy_protocol::SettleMode;
use serde::{Deserialize, Serialize};

use super::super::AppHost;
use super::auth::unix_now;

/// Room taken in one budget window for a request that has not settled yet.
/// The pending counter is the same one user quotas use, so a window's
/// in-flight spend is one number whoever reserved it.
#[derive(Serialize, Deserialize)]
struct Reservation {
    #[serde(default)]
    window_id: i64,
    #[serde(default)]
    quota_id: i64,
    #[serde(default)]
    slot: u32,
    credential_id: i64,
    #[serde(default)]
    released: bool,
    cache_key: String,
    estimated_cost_micros: i64,
}

/// Reserve the request's estimated cost against the credential's budget
/// before it is sent. Settled spend alone would let concurrent requests
/// overrun the limit by everything in flight; the estimate closes that gap
/// and is released when the request settles or is abandoned.
pub(super) async fn reserve(
    host: &AppHost,
    request_id: &str,
    target: &Target,
    body: &bytes::Bytes,
    settle: SettleMode,
) -> Result<(), CoreError> {
    if settle == SettleMode::Free {
        return Ok(());
    }
    let snapshot = host.services.control.current();
    let Some(quota) = snapshot.quotas.iter().find(|quota| {
        quota.enabled
            && quota.subject_kind == "credential"
            && quota.subject_id == target.credential.0
    }) else {
        return Ok(());
    };
    if quota.limits().next().is_none() {
        return Ok(());
    }
    if failed(host, quota.id).await? {
        return Err(CoreError::Store(gproxy_core::error::StoreError(
            "credential budget settlement failed; repair accounting before retrying".into(),
        )));
    }
    if host
        .services
        .control
        .pricing(&target.provider, &target.upstream_model)
        .is_none()
    {
        return Err(CoreError::Internal(
            "credential cost limit requires model pricing".into(),
        ));
    }
    let estimate =
        super::quota::estimated_target_cost_micros(host, request_id, body, target).await?;
    let now = unix_now();
    let key = reservation_key(request_id);
    let mut reservations = load(host, request_id).await?;
    if reservations.is_empty() {
        let bytes = serde_json::to_vec(&reservations).expect("credential reservations serialize");
        super::reserve::initialize_state(host, &key, bytes).await?;
    }
    let start = reservations.len();
    for (kind, limit) in quota.limits() {
        let result = super::window::reserve(
            host,
            quota.id,
            kind,
            limit,
            estimate,
            now,
            &key,
            |window_id| {
                let expected =
                    serde_json::to_vec(&reservations).expect("credential reservations serialize");
                reservations.push(Reservation {
                    window_id,
                    quota_id: quota.id,
                    credential_id: target.credential.0,
                    released: false,
                    slot: reservations.len() as u32,
                    cache_key: super::window::pending_key(window_id),
                    estimated_cost_micros: estimate,
                });
                let updated =
                    serde_json::to_vec(&reservations).expect("credential reservations serialize");
                (expected, updated)
            },
        )
        .await;
        if let Err(error) = result {
            if let Err(rollback) = super::super::settlement_retry::run(host, || {
                release_from(host, request_id, None, start)
            })
            .await
            {
                tracing::error!(request_id, error = %rollback, "credential reservation rollback requires replay");
            }
            return Err(error);
        }
    }
    Ok(())
}

/// Give back every reservation the request holds. Failover may have
/// reserved against more than one credential; all of them are released.
pub(super) async fn release(
    host: &AppHost,
    request_id: &str,
    settlement: Option<&Settlement>,
) -> Result<(), CoreError> {
    release_from(host, request_id, settlement, 0).await
}

async fn release_from(
    host: &AppHost,
    request_id: &str,
    settlement: Option<&Settlement>,
    start: usize,
) -> Result<(), CoreError> {
    let key = reservation_key(request_id);
    let mut conflicts = 0;
    while conflicts < 8 {
        let Some(expected) = host.services.cache.get(&key).await? else {
            return Ok(());
        };
        let mut held = decode(Some(&expected))?;
        let Some(reservation) = held
            .iter_mut()
            .skip(start)
            .find(|reservation| !reservation.released)
        else {
            if start > 0 {
                return Ok(());
            }
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
        if let Some(settlement) =
            settlement.filter(|settlement| settlement.credential_id.0 == reservation.credential_id)
        {
            super::window::commit(
                host,
                request_id,
                reservation.window_id,
                reservation.quota_id,
                settlement.cost,
            )
            .await?;
        }
        reservation.released = true;
        let pending = reservation.cache_key.clone();
        let refund = -reservation.estimated_cost_micros;
        let updated = serde_json::to_vec(&held).expect("credential reservations serialize");
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
    Err(CoreError::Store(gproxy_core::error::StoreError(
        "credential settlement has remaining reservations".into(),
    )))
}

#[derive(Clone, Serialize, Deserialize)]
pub(in crate::host) struct CredentialCharge {
    pub window_id: i64,
    pub quota_id: i64,
}

// Freeze attribution before any durable charge. Recovery must not select a new
// window after a failed usage write crosses a period boundary.
pub(in crate::host) async fn prepare_record(
    host: &AppHost,
    settlement: &Settlement,
) -> Result<Vec<CredentialCharge>, CoreError> {
    let held = load(host, &settlement.request_id).await?;
    let mut seen = std::collections::BTreeSet::new();
    let matching: Vec<_> = held
        .iter()
        .filter(|reservation| {
            !reservation.released && reservation.credential_id == settlement.credential_id.0
        })
        .filter(|reservation| seen.insert(reservation.window_id))
        .map(|reservation| CredentialCharge {
            window_id: reservation.window_id,
            quota_id: reservation.quota_id,
        })
        .collect();
    if !matching.is_empty() {
        return Ok(matching);
    }
    let snapshot = host.services.control.current();
    let mut targets = Vec::new();
    let now = unix_now();
    // Disabled limits still accumulate spend; deleting a quota retires it.
    for quota in snapshot.quotas.iter().filter(|quota| {
        quota.subject_kind == "credential" && quota.subject_id == settlement.credential_id.0
    }) {
        if !host
            .services
            .store
            .quota_exists(quota.id)
            .await
            .map_err(|error| CoreError::Store(gproxy_core::error::StoreError(error.to_string())))?
        {
            continue;
        }
        for (kind, _) in quota.limits() {
            let window = host
                .services
                .store
                .ensure_quota_window(quota.id, kind, now)
                .await
                .map_err(|error| {
                    CoreError::Store(gproxy_core::error::StoreError(error.to_string()))
                })?;
            if seen.insert(window.id) {
                targets.push(CredentialCharge {
                    window_id: window.id,
                    quota_id: quota.id,
                });
            }
        }
    }
    Ok(targets)
}

pub(in crate::host) async fn record_prepared(
    host: &AppHost,
    settlement: &Settlement,
    targets: &[CredentialCharge],
) -> Result<(), CoreError> {
    for target in targets {
        super::window::commit(
            host,
            &settlement.request_id,
            target.window_id,
            target.quota_id,
            settlement.cost,
        )
        .await?;
    }
    Ok(())
}

async fn load(host: &AppHost, request_id: &str) -> Result<Vec<Reservation>, CoreError> {
    decode(
        host.services
            .cache
            .get(&reservation_key(request_id))
            .await?
            .as_deref(),
    )
}

fn decode(bytes: Option<&[u8]>) -> Result<Vec<Reservation>, CoreError> {
    bytes
        .map(serde_json::from_slice)
        .transpose()
        .map_err(|error| CoreError::Internal(format!("decode credential reservation: {error}")))
        .map(Option::unwrap_or_default)
}

fn reservation_key(request_id: &str) -> String {
    format!("gproxy:credential-admission:{request_id}")
}

/// Nothing here writes `gproxy:credential-budget-failed:`; a tripped budget is
/// recorded per request in the shared quota failure set instead. The marker is
/// still honoured because a node on the released build, or a rollback to it,
/// writes exactly that key and never clears it on its own: dropping the read
/// would resume admitting against a budget whose accounting is known broken.
/// Both keys are read together so the ordinary case where neither exists costs
/// one round trip rather than two.
async fn failed(host: &AppHost, quota_id: i64) -> Result<bool, CoreError> {
    let legacy = format!("gproxy:credential-budget-failed:{quota_id}");
    let (current, legacy) = futures_util::future::join(
        host.services
            .cache
            .get(&super::window::failure_key(quota_id)),
        host.services.cache.get(&legacy),
    )
    .await;
    Ok(current?.is_some() || legacy?.is_some())
}
