use super::{internal, now_ms};
use crate::AppHandle;
use gproxy_admin::AdminError;
use gproxy_channel_api::{QuotaRefreshError, QuotaSource, QuotaSourceState};
use gproxy_core::{CacheBackend, CoreError, CredentialId, Host, ProviderRef};
use std::time::Duration;

pub(super) async fn refresh(
    app: &AppHandle,
    provider: &ProviderRef,
    id: i64,
    version: u64,
    capability: QuotaSource,
    force: bool,
    owner: &[u8],
) -> Result<Option<(u64, String)>, AdminError> {
    let cache = &app.inner.host.services.cache;
    let key = format!("quota:source:{id}:v{version}:{}", capability.id);
    if retry_blocked(cache, &key, force).await? {
        return Ok(None);
    }
    let prepared = app
        .inner
        .core
        .prepare_quota_source(provider, CredentialId(id), version, &capability.id)
        .await;
    // The per-credential 300-second lease already serializes preparation.
    // Reserve the shorter global egress slot after token refresh completes.
    let slot = super::lease::acquire(app, owner).await?;
    let (version, key, capability, prepared) = match prepared {
        Ok(prepared) => {
            let version = prepared.credential_version();
            let (current_provider, current_version, capabilities) =
                super::super::quota_snapshot::sources(app, id).await?;
            if current_version != version || !same_provider(provider, &current_provider) {
                slot.release().await?;
                return Err(AdminError::Conflict(
                    "credential configuration changed during quota preparation".into(),
                ));
            }
            let capability = capabilities
                .into_iter()
                .find(|source| source.id == capability.id)
                .filter(|source| {
                    source.support == gproxy_channel_api::QuotaSupport::Ready
                        && source.mode == gproxy_channel_api::QuotaQueryMode::Probe
                })
                .ok_or_else(|| {
                    AdminError::Conflict("quota authorization changed during preparation".into())
                })?;
            let key = format!("quota:source:{id}:v{version}:{}", capability.id);
            if retry_blocked(cache, &key, force).await? {
                slot.release().await?;
                return Ok(None);
            }
            (version, key, capability, Ok(prepared))
        }
        Err(error) => {
            let current = app
                .inner
                .host
                .services
                .store
                .credential(id)
                .await?
                .ok_or(AdminError::NotFound)?;
            if current.version != version {
                // OAuth may already have rotated before quota request building
                // failed. Never attach that failure or its retry cache to the
                // earlier authentication version.
                slot.release().await?;
                return Err(AdminError::Conflict(
                    "credential changed during quota preparation; retry query".into(),
                ));
            }
            (version, key, capability, Err(error))
        }
    };
    let attempted_at_ms = now_ms();
    let result = async {
        let prepared = prepared?;
        let fetch = app.inner.core.execute_quota_source(prepared);
        let timeout = app.inner.host.wait(Duration::from_secs(45));
        futures_util::pin_mut!(fetch, timeout);
        match futures_util::future::select(fetch, timeout).await {
            futures_util::future::Either::Left((result, _)) => result,
            futures_util::future::Either::Right(_) => Err(CoreError::UpstreamExhausted(
                "quota refresh timed out".into(),
            )),
        }
    }
    .await;
    slot.release().await?;
    let mut state = QuotaSourceState {
        capability,
        attempted_at_ms: Some(attempted_at_ms),
        observed_at_ms: None,
        error: None,
        reset_credits: None,
    };
    match result {
        Ok(result) => {
            state.observed_at_ms = Some(result.observed_at_ms);
            state.reset_credits = result.reset_credits;
            let result_version = result.credential_version;
            app.inner
                .host
                .services
                .store
                .save_credential_quota_source(id, result_version, &state, Some(&result.entries))
                .await?;
            cache
                .delete(&format!("{key}:failures"))
                .await
                .map_err(internal)?;
            cache
                .delete(&format!("{key}:retry"))
                .await
                .map_err(internal)?;
            Ok(Some((result_version, result.raw)))
        }
        Err(error) => {
            if let CoreError::RateLimited { retry_after_secs } = &error {
                cache
                    .set(
                        &format!("{key}:upstream-retry"),
                        vec![1],
                        Some(Duration::from_secs(u64::from(*retry_after_secs))),
                    )
                    .await
                    .map_err(internal)?;
            }
            state.error = Some(public_error(&error));
            app.inner
                .host
                .services
                .store
                .save_credential_quota_source(id, version, &state, None)
                .await?;
            let failures = cache
                .incr(
                    &format!("{key}:failures"),
                    1,
                    Some(Duration::from_secs(86400)),
                )
                .await
                .map_err(internal)?;
            let delay = (600 * (1u64 << (failures - 1).clamp(0, 3))).min(3600);
            cache
                .set(
                    &format!("{key}:retry"),
                    vec![1],
                    Some(Duration::from_secs(delay)),
                )
                .await
                .map_err(internal)?;
            Ok(None)
        }
    }
}

fn public_error(error: &CoreError) -> QuotaRefreshError {
    let (code, message) = match error {
        CoreError::RateLimited { .. } => (
            "rate_limited",
            "Upstream requested a longer retry interval.",
        ),
        CoreError::Unsupported => (
            "unsupported",
            "This credential cannot query this quota source.",
        ),
        CoreError::UpstreamExhausted(message) => ("upstream", message.as_str()),
        _ => (
            "unavailable",
            "Quota query failed. Check credential permissions and provider connectivity.",
        ),
    };
    QuotaRefreshError {
        code: code.into(),
        message: message.into(),
    }
}

async fn retry_blocked(
    cache: &impl CacheBackend,
    key: &str,
    force: bool,
) -> Result<bool, AdminError> {
    Ok(cache
        .get(&format!("{key}:upstream-retry"))
        .await
        .map_err(internal)?
        .is_some()
        || (!force
            && cache
                .get(&format!("{key}:retry"))
                .await
                .map_err(internal)?
                .is_some()))
}

fn same_provider(before: &ProviderRef, after: &ProviderRef) -> bool {
    let fingerprint = match (&before.fingerprint, &after.fingerprint) {
        (None, None) => true,
        (
            Some(gproxy_core::ConfiguredFingerprint::Usable(before)),
            Some(gproxy_core::ConfiguredFingerprint::Usable(after)),
        ) => before.headers == after.headers && before.profile == after.profile,
        (
            Some(gproxy_core::ConfiguredFingerprint::Invalid(before)),
            Some(gproxy_core::ConfiguredFingerprint::Invalid(after)),
        ) => before == after,
        _ => false,
    };
    before.id == after.id
        && before.channel == after.channel
        && before.settings == after.settings
        && before.proxy_url == after.proxy_url
        && fingerprint
}
