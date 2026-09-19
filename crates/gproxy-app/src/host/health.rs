use gproxy_core::Host;
use gproxy_store::records::{CredentialHealthInput, CredentialHealthState};

use super::AppHost;

/// Success is ordered at the start of its own upstream attempt. Failure is
/// ordered when observed, so a slow success cannot erase an intervening failure,
/// even if another worker persisted it or its local snapshot is stale.
/// The zero sequence also rejects recovery against failures in the same ms.
pub(super) fn success_version(started_at_ms: i64) -> Option<i64> {
    (started_at_ms >= 0)
        .then_some(started_at_ms)?
        .checked_mul(1_000_000)
}

pub(super) async fn record(host: &AppHost, input: CredentialHealthInput) {
    let credential = gproxy_channel_api::CredentialId(input.credential_id);
    let model = input.model.as_str();
    let credential_version = input.credential_version;
    let state = input.state;
    let unchanged = host
        .services
        .control
        .credential_health_state(credential, model)
        == Some((credential_version, state));
    // Failed attempts advance persistent backoff even if the state is
    // still degraded. Only repeated healthy evidence may be coalesced.
    let recovering_account = state == CredentialHealthState::Healthy
        && model != "*"
        && host
            .services
            .control
            .credential_health_state(credential, "*")
            == Some((
                credential_version,
                gproxy_store::records::CredentialHealthState::Degraded,
            ));
    let refresh = unchanged && state == CredentialHealthState::Healthy && !recovering_account;
    if refresh
        && !host
            .services
            .control
            .health_refresh_due(credential, model, input.observed_at)
    {
        return;
    }
    match host.spawner().filter(|_| refresh) {
        Some(spawner) => {
            let host = host.clone();
            spawner.spawn(Box::pin(async move {
                persist_credential_health(&host, &input).await;
            }));
        }
        None => {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let owned = host.clone();
                let writing = host.services.spawner.spawn_tracked(async move {
                    persist_credential_health(&owned, &input).await;
                });
                if let Err(error) = writing.await {
                    tracing::error!(error = %error, "credential health task failed");
                }
            }
            #[cfg(target_arch = "wasm32")]
            persist_credential_health(host, &input).await;
        }
    }
}

pub(super) fn observation_version(sequence: &std::sync::atomic::AtomicU64) -> Option<i64> {
    let elapsed = web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .ok()?;
    let millis = i64::try_from(elapsed.as_millis()).ok()?;
    let sequence = sequence.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % 1_000_000;
    Some(
        millis
            .saturating_mul(1_000_000)
            .saturating_add(sequence as i64),
    )
}

async fn persist_credential_health(
    host: &AppHost,
    input: &gproxy_store::records::CredentialHealthInput,
) {
    persist_health_row(host, input).await;
    // A completed model request is evidence that a transient account-wide
    // failure recovered. It does not clear another model, a newer observation,
    // another credential version, or a permanent account failure.
    if input.state == gproxy_store::records::CredentialHealthState::Healthy
        && input.model != "*"
        && host
            .services
            .control
            .credential_health_observation(
                gproxy_channel_api::CredentialId(input.credential_id),
                "*",
            )
            .is_some_and(|record| {
                record.credential_version == input.credential_version
                    && record.version < input.version
                    && record.state == gproxy_store::records::CredentialHealthState::Degraded
            })
    {
        let mut recovered = input.clone();
        recovered.model = "*".into();
        recovered.detail = Some(format!("successful response from {}", input.model));
        persist_health_row_inner(host, &recovered, true).await;
    }
}

async fn persist_health_row(host: &AppHost, input: &gproxy_store::records::CredentialHealthInput) {
    persist_health_row_inner(host, input, false).await;
}

async fn persist_health_row_inner(
    host: &AppHost,
    input: &gproxy_store::records::CredentialHealthInput,
    recover_degraded_only: bool,
) {
    let credential = gproxy_channel_api::CredentialId(input.credential_id);
    let previous = host
        .services
        .control
        .credential_health_observation(credential, &input.model);
    let result = if recover_degraded_only {
        host.services
            .store
            .recover_degraded_credential_health(input)
            .await
    } else {
        host.services.store.record_credential_health(input).await
    };
    if let Err(error) = result {
        tracing::error!(error = %error, "credential health persistence failed");
    } else {
        // Broadcast a durable failure/recovery even if its local readback fails.
        // Native peers poll this version, and edge hosts sync it before dispatch.
        let notify = input.state != gproxy_store::records::CredentialHealthState::Healthy
            || previous.as_ref().is_some_and(|record| {
                record.state != gproxy_store::records::CredentialHealthState::Healthy
                    || record.credential_version != input.credential_version
            });
        if notify && let Err(error) = crate::invalidation::bump(&host.services.cache).await {
            tracing::warn!(error = %error, "credential health invalidation failed");
        }
        // An older observation may have been rejected, and storage owns the
        // failure count. Publish the accepted row instead of the attempted write.
        if let Err(error) = host
            .services
            .control
            .refresh_credential_health(
                gproxy_channel_api::CredentialId(input.credential_id),
                &input.model,
            )
            .await
        {
            tracing::error!(error = %error, "credential health readback failed");
        }
    }
}
