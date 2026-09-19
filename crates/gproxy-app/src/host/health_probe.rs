use std::time::Duration;

use base64::Engine as _;
use gproxy_channel_api::CredentialId;
use gproxy_core::{CacheBackend, CoreError, Target, host::CredentialHealthLease};
use gproxy_store::records::{CredentialHealthRecord, CredentialHealthState};
use sha2::{Digest, Sha256};

use super::{AppHost, settlement_recovery::store_error};

const LEASE_TTL: Duration = Duration::from_secs(120);
#[cfg(not(target_arch = "wasm32"))]
const RENEW_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(not(target_arch = "wasm32"))]
const CACHE_TIMEOUT: Duration = Duration::from_secs(5);
const CANCELLED_PROBE_DELAY: u32 = 30;
#[cfg(not(target_arch = "wasm32"))]
const COOLDOWN_SENTINEL: &[u8] = b"cooldown";

pub(super) async fn begin(
    host: &AppHost,
    request_id: &str,
    target: &Target,
    credential_version: u64,
) -> Result<Option<CredentialHealthLease>, CoreError> {
    if probe_scope(host, target, credential_version)?.is_none() {
        return Ok(None);
    }
    // The task owns the guard until the cache write replies. Cancellation of
    // the caller must not release a key before its delayed acquisition commits.
    #[cfg(not(target_arch = "wasm32"))]
    {
        let acquiring = host.clone();
        let request_id = request_id.to_owned();
        let target = target.clone();
        host.services
            .spawner
            .spawn_tracked(async move {
                begin_owned(&acquiring, &request_id, &target, credential_version).await
            })
            .await
            .map_err(|error| {
                CoreError::Internal(format!("credential health probe task failed: {error}"))
            })?
    }
    #[cfg(target_arch = "wasm32")]
    begin_owned(host, request_id, target, credential_version).await
}

async fn begin_owned(
    host: &AppHost,
    request_id: &str,
    target: &Target,
    credential_version: u64,
) -> Result<Option<CredentialHealthLease>, CoreError> {
    // A concurrent reset or a newer global observation can change the scope
    // while the shared lease is being acquired. Re-select after refreshing both
    // applicable rows; never probe a model through a newly dead account.
    for _ in 0..3 {
        let Some(observation) = probe_scope(host, target, credential_version)? else {
            return Ok(None);
        };
        let guard = Guard::acquire(
            host,
            request_id,
            target.credential,
            credential_version,
            &observation.model,
        )
        .await?;
        refresh_health(host, target.credential, "*").await?;
        if target.upstream_model != "*" {
            refresh_health(host, target.credential, &target.upstream_model).await?;
        }
        let Some(current) = probe_scope_after_acquisition(host, target, credential_version)? else {
            return Ok(None);
        };
        if current.model != observation.model {
            drop(guard);
            continue;
        }
        // A delayed database read may outlive the original TTL. Check that
        // this owner still holds the lease immediately before upstream work.
        if !host
            .services
            .cache
            .compare_and_swap(
                &guard.key,
                Some(guard.owner.clone()),
                Some(guard.owner.clone()),
                Some(LEASE_TTL),
            )
            .await
            .map_err(CoreError::Store)?
        {
            return Err(cooling(CANCELLED_PROBE_DELAY));
        }
        return Ok(Some(guard.into_lease()));
    }
    Err(cooling(CANCELLED_PROBE_DELAY))
}

fn probe_scope(
    host: &AppHost,
    target: &Target,
    credential_version: u64,
) -> Result<Option<CredentialHealthRecord>, CoreError> {
    let observation = probe_scope_after_acquisition(host, target, credential_version)?;
    if observation.as_ref().is_some_and(|observation| {
        !host.services.control.health_probe_ready(
            target.credential,
            &observation.model,
            credential_version,
            now_seconds(),
        )
    }) {
        return Err(cooling(CANCELLED_PROBE_DELAY));
    }
    Ok(observation)
}

fn probe_scope_after_acquisition(
    host: &AppHost,
    target: &Target,
    credential_version: u64,
) -> Result<Option<CredentialHealthRecord>, CoreError> {
    let mut selected = None;
    let mut retry_after = 0;
    let now = now_seconds();
    for model in ["*", target.upstream_model.as_str()] {
        let Some(observation) = host
            .services
            .control
            .credential_health_observation(target.credential, model)
            .filter(|observation| observation.credential_version == credential_version)
        else {
            continue;
        };
        match observation.state {
            CredentialHealthState::Dead => return Err(CoreError::NoCredentials),
            CredentialHealthState::Healthy => {}
            CredentialHealthState::Degraded => {
                retry_after = retry_after.max(cooldown_remaining(&observation, now));
                if selected.is_none() {
                    selected = Some(observation);
                }
            }
        }
    }
    if retry_after > 0 {
        return Err(cooling(retry_after));
    }
    Ok(selected)
}

async fn refresh_health(
    host: &AppHost,
    credential: CredentialId,
    model: &str,
) -> Result<(), CoreError> {
    host.services
        .control
        .refresh_credential_health(credential, model)
        .await
        .map_err(store_error)
}

fn cooldown_remaining(observation: &CredentialHealthRecord, now: i64) -> u32 {
    crate::control::SnapshotControl::health_retry_after(observation, now)
}

fn cooling(retry_after_secs: u32) -> CoreError {
    CoreError::CredentialCoolingDown { retry_after_secs }
}

fn now_seconds() -> i64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs().min(i64::MAX as u64) as i64)
}

fn lease_key(credential: CredentialId, credential_version: u64, scope: &str) -> String {
    format!(
        "health:probe:{}:{credential_version}:{}",
        credential.0,
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(scope.as_bytes()))
    )
}

struct Guard {
    host: AppHost,
    credential: CredentialId,
    credential_version: u64,
    scope: String,
    key: String,
    owner: Vec<u8>,
    release_shared: bool,
    #[cfg(not(target_arch = "wasm32"))]
    stop_renewing: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Guard {
    async fn acquire(
        host: &AppHost,
        request_id: &str,
        credential: CredentialId,
        credential_version: u64,
        scope: &str,
    ) -> Result<Self, CoreError> {
        let mut randomness = [0_u8; 32];
        getrandom::fill(&mut randomness).map_err(|error| {
            CoreError::Internal(format!(
                "credential health lease randomness failed: {error}"
            ))
        })?;
        let owner = Sha256::new()
            .chain_update(randomness)
            .chain_update(request_id.as_bytes())
            .finalize()
            .to_vec();
        if !host
            .services
            .control
            .try_begin_health_probe(credential, scope, credential_version)
        {
            return Err(cooling(CANCELLED_PROBE_DELAY));
        }
        let mut guard = Self {
            host: host.clone(),
            credential,
            credential_version,
            scope: scope.to_owned(),
            key: lease_key(credential, credential_version, scope),
            owner,
            // A cache error can mean that the write committed but its reply
            // was lost. Drop still performs an owner-checked cleanup then.
            release_shared: true,
            #[cfg(not(target_arch = "wasm32"))]
            stop_renewing: None,
        };
        let acquired = guard
            .host
            .services
            .cache
            .compare_and_swap(&guard.key, None, Some(guard.owner.clone()), Some(LEASE_TTL))
            .await
            .map_err(CoreError::Store)?;
        if !acquired {
            guard.release_shared = false;
            return Err(cooling(CANCELLED_PROBE_DELAY));
        }
        Ok(guard)
    }

    fn into_lease(self) -> CredentialHealthLease {
        #[cfg(not(target_arch = "wasm32"))]
        let lease = {
            let mut lease = self;
            let (stop, stopped) = tokio::sync::oneshot::channel();
            lease.stop_renewing = Some(stop);
            let host = lease.host.clone();
            let key = lease.key.clone();
            let owner = lease.owner.clone();
            drop(lease.host.services.spawner.spawn_tracked(async move {
                renew_until_released(&host, &key, &owner, stopped, RENEW_INTERVAL).await;
            }));
            lease
        };
        #[cfg(target_arch = "wasm32")]
        let lease = self;
        crate::Shared::new(lease)
    }
}

impl gproxy_core::host::CredentialHealthActivity for Guard {}

impl Drop for Guard {
    fn drop(&mut self) {
        self.host.services.control.finish_health_probe(
            self.credential,
            &self.scope,
            self.credential_version,
        );
        if !self.release_shared {
            return;
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Some(stop) = self.stop_renewing.take() {
                let _ = stop.send(());
            } else {
                let host = self.host.clone();
                let key = self.key.clone();
                let owner = self.owner.clone();
                drop(self.host.services.spawner.spawn_tracked(async move {
                    release_key(&host, &key, &owner).await;
                }));
            }
        }
        // Edge hosts have no background spawner. Their shared ownership still
        // expires even if the isolate is interrupted before settlement.
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn renew_until_released(
    host: &AppHost,
    key: &str,
    owner: &[u8],
    mut stopped: tokio::sync::oneshot::Receiver<()>,
    interval: Duration,
) {
    loop {
        tokio::select! {
            biased;
            _ = &mut stopped => break,
            () = tokio::time::sleep(interval) => {}
        }
        let renewed = tokio::time::timeout(
            CACHE_TIMEOUT,
            host.services.cache.compare_and_swap(
                key,
                Some(owner.to_vec()),
                Some(owner.to_vec()),
                Some(LEASE_TTL),
            ),
        )
        .await;
        match renewed {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => {
                tracing::warn!(key, "credential health probe lease ownership was lost");
                break;
            }
            Ok(Err(error)) => {
                tracing::warn!(key, error = %error, "credential health probe lease renewal failed");
            }
            Err(_) => {
                tracing::warn!(key, "credential health probe lease renewal timed out");
            }
        }
    }
    release_key(host, key, owner).await;
}

#[cfg(not(target_arch = "wasm32"))]
async fn release_key(host: &AppHost, key: &str, owner: &[u8]) {
    match tokio::time::timeout(
        CACHE_TIMEOUT,
        host.services.cache.compare_and_swap(
            key,
            Some(owner.to_vec()),
            Some(COOLDOWN_SENTINEL.to_vec()),
            Some(Duration::from_secs(u64::from(CANCELLED_PROBE_DELAY))),
        ),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            tracing::warn!(key, error = %error, "credential health probe lease cleanup failed");
        }
        Err(_) => {
            tracing::warn!(key, "credential health probe lease cleanup timed out");
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
