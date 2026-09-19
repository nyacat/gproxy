use std::time::Duration;

use web_time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use gproxy_channel_api::{BoxFuture, Channel, ChannelError, SimpleHttp};

use crate::control::ProviderRef;
use crate::error::CoreError;
use crate::host::{
    CacheBackend, CredentialId, CredentialRecord, CredentialStore, Host, UpstreamTransport,
};

pub(crate) const REFRESH_LEASE_TTL: Duration = Duration::from_secs(120);
const REFRESH_RENEW_INTERVAL: Duration = Duration::from_secs(30);
const REFRESH_HTTP_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const REFRESH_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// How long a caller waits for whoever holds the lease. Waiting exists to pick
/// up a peer's rotated token, not to outlive the peer: a lease only becomes
/// free again once `REFRESH_LEASE_TTL` elapses, so waiting that long would keep
/// a live request — and the execution slot it occupies — parked for two minutes
/// on the chance that its holder died. A peer that has not published within
/// this budget is better left to the next request, which finds either a fresh
/// token or an expired lease it can take over immediately.
pub(crate) const REFRESH_WAIT_BUDGET: Duration = Duration::from_secs(20);
const REFRESH_BACKOFF_TTL: Duration = Duration::from_secs(30 * 60);
const REFRESH_BACKOFF: [u64; 5] = [30, 60, 120, 240, 300];

// A zero interval divides by zero below, and a budget shorter than one interval
// would give up before ever trying to take the lease.
const _: () = assert!(REFRESH_POLL_INTERVAL.as_secs() > 0);
const _: () = assert!(REFRESH_WAIT_BUDGET.as_secs() >= REFRESH_POLL_INTERVAL.as_secs());
const _: () = assert!(REFRESH_WAIT_BUDGET.as_secs() < REFRESH_LEASE_TTL.as_secs());

pub(crate) async fn load_fresh<H: Host>(
    core: &crate::Core<H>,
    channel: &dyn Channel,
    id: CredentialId,
    provider: &ProviderRef,
) -> Result<CredentialRecord, CoreError> {
    let channel = core
        .channels
        .shared(channel.descriptor().id)
        .ok_or(CoreError::Unsupported)?;
    load_fresh_shared(&core.host, channel, id, provider).await
}

pub(crate) async fn load_fresh_shared<H: Host>(
    host: &crate::Shared<H>,
    channel: std::sync::Arc<dyn Channel>,
    id: CredentialId,
    provider: &ProviderRef,
) -> Result<CredentialRecord, CoreError> {
    let mut record = load_checked(host.as_ref(), id, channel.descriptor().id).await?;
    if !refresh_due(channel.as_ref(), &record, unix_now()?) {
        return Ok(record);
    }
    if RefreshBackoffSnapshot::read(host.as_ref(), id, record.version)
        .await?
        .remaining()?
        .is_some()
    {
        // A peer may already have rotated this cached row. An old version's
        // refresh failure must never block newly saved authentication.
        record = load_current_checked(host.as_ref(), id, channel.descriptor().id).await?;
        if !refresh_due(channel.as_ref(), &record, unix_now()?) {
            return Ok(record);
        }
        if let Some(retry_after_secs) =
            RefreshBackoffSnapshot::read(host.as_ref(), id, record.version)
                .await?
                .remaining()?
        {
            return Err(CoreError::CredentialRefreshCoolingDown { retry_after_secs });
        }
    }
    run(host, channel, id, provider, record.version, false)
        .await
        .map(|result| result.credential)
}

pub(crate) async fn run<H: Host>(
    host: &crate::Shared<H>,
    channel: std::sync::Arc<dyn Channel>,
    id: CredentialId,
    provider: &ProviderRef,
    observed_version: u64,
    force: bool,
) -> Result<crate::CredentialRefreshResult, CoreError> {
    let owned_host = host.clone();
    let provider = provider.clone();
    let operation = async move {
        refresh_owned(
            &owned_host,
            channel.as_ref(),
            id,
            &provider,
            observed_version,
            force,
        )
        .await
    };
    // Once refresh starts, a rotated upstream token must reach persistence even
    // when the client disconnects. Native hosts also drain this task on shutdown.
    if let Some(spawner) = host.spawner() {
        let (complete, completed) = futures_channel::oneshot::channel();
        spawner.spawn(Box::pin(async move {
            let result = operation.await;
            let _ = complete.send(result);
        }));
        completed
            .await
            .map_err(|_| CoreError::Internal("credential refresh task failed".into()))?
    } else {
        operation.await
    }
}

async fn refresh_owned<H: Host>(
    host: &crate::Shared<H>,
    channel: &dyn Channel,
    id: CredentialId,
    provider: &ProviderRef,
    mut observed_version: u64,
    force: bool,
) -> Result<crate::CredentialRefreshResult, CoreError> {
    let mut owner = [0_u8; 32];
    getrandom::fill(&mut owner)
        .map_err(|_| CoreError::Internal("credential refresh randomness failed".into()))?;
    let mut guard = RefreshGuard {
        host: host.clone(),
        id,
        owner: owner.to_vec(),
        released: false,
    };
    let channel_id = channel.descriptor().id;
    let mut acquired = false;
    let polls = REFRESH_WAIT_BUDGET.as_secs() / REFRESH_POLL_INTERVAL.as_secs();
    for _ in 0..polls {
        if host
            .credentials()
            .lease_refresh(id, &owner, REFRESH_LEASE_TTL)
            .await?
        {
            acquired = true;
            break;
        }
        host.wait(REFRESH_POLL_INTERVAL).await;
        let peer = load_current_checked(host.as_ref(), id, channel_id).await?;
        if peer.version != observed_version {
            if force || !refresh_due(channel, &peer, unix_now()?) {
                return Ok(peer_result(peer));
            }
            observed_version = peer.version;
        }
    }
    if !acquired {
        return Err(ChannelError::Refresh(
            "another credential refresh is still in progress".into(),
        )
        .into());
    }
    let operation = refresh_under_lease(
        host.as_ref(),
        channel,
        id,
        provider,
        observed_version,
        force,
        &owner,
    );
    let result = renew_while_running(host.as_ref(), id, &owner, operation).await;
    guard.release().await;
    result
}

async fn refresh_under_lease<H: Host>(
    host: &H,
    channel: &dyn Channel,
    id: CredentialId,
    provider: &ProviderRef,
    observed_version: u64,
    force: bool,
    owner: &[u8],
) -> Result<crate::CredentialRefreshResult, CoreError> {
    let channel_id = channel.descriptor().id;
    let current = load_current_checked(host, id, channel_id).await?;
    if (current.version != observed_version && force)
        || (!force && !refresh_due(channel, &current, unix_now()?))
    {
        return Ok(peer_result(current));
    }
    let backoff = RefreshBackoffSnapshot::read(host, id, current.version).await?;
    if !force && let Some(retry_after_secs) = backoff.remaining()? {
        return Err(CoreError::CredentialRefreshCoolingDown { retry_after_secs });
    }
    // Ownership can expire during a delayed authoritative read. Never start a
    // second upstream rotation unless this request still owns the lease.
    if !host
        .credentials()
        .renew_refresh(id, owner, REFRESH_LEASE_TTL)
        .await?
    {
        return Err(ChannelError::Refresh(
            "credential refresh lease was lost before sending".into(),
        )
        .into());
    }
    let http = BufferedHttp(host, provider);
    let refresh = channel
        .refresh(&current.secret, &provider.settings, &http)
        .ok_or_else(|| {
            ChannelError::Refresh("channel did not provide a refresh operation".into())
        })?;
    let replacement = match refresh.await {
        Ok(replacement) => replacement,
        Err(error) => {
            let _ = backoff.failure(host, id, owner).await;
            host.record_credential_health(
                id,
                "*",
                current.version,
                crate::CredentialHealth::Degraded,
                None,
                "credential refresh failed",
            )
            .await;
            return Err(error.into());
        }
    };
    let result = persist_refreshed(host, channel, current, replacement, force).await;
    if result.is_ok() {
        let _ = backoff.clear(host).await;
    }
    result
}

async fn persist_refreshed<H: Host>(
    host: &H,
    channel: &dyn Channel,
    original: CredentialRecord,
    replacement: gproxy_channel_api::RefreshResult,
    force: bool,
) -> Result<crate::CredentialRefreshResult, CoreError> {
    let mut version = original.version;
    for _ in 0..3 {
        match host
            .credentials()
            .persist_rotation(original.id, replacement.secret.clone(), version)
            .await
        {
            Ok(()) => {
                let credential = load_current_checked(host, original.id, &original.channel).await?;
                let refresh_token = (credential.version == version.saturating_add(1))
                    .then_some(replacement.refresh_token);
                return Ok(crate::CredentialRefreshResult {
                    credential,
                    refresh_token,
                });
            }
            Err(error) => {
                let peer = load_current_checked(host, original.id, &original.channel).await?;
                if peer.version == version {
                    return Err(error.into());
                }
                if same_authentication(&original, &peer) {
                    // A metadata or quota-secret edit increments the row version
                    // without replacing the token being rotated. Retry its CAS
                    // so that an already-issued refresh token is never discarded.
                    version = peer.version;
                    continue;
                }
                if force || !refresh_due(channel, &peer, unix_now()?) {
                    return Ok(peer_result(peer));
                }
                return Err(CoreError::CredentialVersionConflict);
            }
        }
    }
    Err(CoreError::CredentialVersionConflict)
}

fn same_authentication(original: &CredentialRecord, current: &CredentialRecord) -> bool {
    let without_quota = |secret: &serde_json::Value| {
        secret.as_object().map(|object| {
            object
                .iter()
                .filter(|(key, _)| !key.starts_with("quota_"))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<serde_json::Map<String, serde_json::Value>>()
        })
    };
    original.kind == current.kind
        && original.channel == current.channel
        && without_quota(&original.secret).is_some()
        && without_quota(&original.secret) == without_quota(&current.secret)
}

fn peer_result(credential: CredentialRecord) -> crate::CredentialRefreshResult {
    crate::CredentialRefreshResult {
        credential,
        refresh_token: None,
    }
}

async fn renew_while_running<H: Host, T>(
    host: &H,
    id: CredentialId,
    owner: &[u8],
    operation: impl std::future::Future<Output = T>,
) -> T {
    use futures_util::future::{Either, select};
    futures_util::pin_mut!(operation);
    loop {
        let renew = async {
            host.wait(REFRESH_RENEW_INTERVAL).await;
            host.credentials()
                .renew_refresh(id, owner, REFRESH_LEASE_TTL)
                .await
        };
        futures_util::pin_mut!(renew);
        match select(operation, renew).await {
            Either::Left((result, _)) => return result,
            Either::Right((renewed, continuing)) => {
                operation = continuing;
                if !matches!(renewed, Ok(true)) {
                    // Dropping an upstream request here could discard a token it
                    // has already rotated. Finish reading and persist with CAS.
                    tracing::warn!(
                        credential_id = id.0,
                        "credential refresh lease renewal failed"
                    );
                }
            }
        }
    }
}

fn refresh_backoff_key(id: CredentialId, version: u64) -> String {
    format!("gproxy:credential-refresh:{}:{}", id.0, version)
}

#[derive(serde::Deserialize, serde::Serialize)]
struct RefreshBackoff {
    failures: u8,
    retry_at: i64,
    // Distinguishes successive failures even after the counter saturates and
    // two manual retries finish within the same clock second.
    owner: Vec<u8>,
}

struct RefreshBackoffSnapshot {
    key: String,
    value: Option<Vec<u8>>,
    state: Option<RefreshBackoff>,
}

impl RefreshBackoffSnapshot {
    async fn read<H: Host>(host: &H, id: CredentialId, version: u64) -> Result<Self, CoreError> {
        let key = refresh_backoff_key(id, version);
        let value = host.cache().get(&key).await?;
        let state = value
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()
            .map_err(|_| {
                CoreError::Store(crate::error::StoreError(
                    "credential refresh backoff is invalid".into(),
                ))
            })?;
        Ok(Self { key, value, state })
    }

    fn remaining(&self) -> Result<Option<u32>, CoreError> {
        let remaining = self
            .state
            .as_ref()
            .map_or(0, |state| state.retry_at)
            .saturating_sub(unix_now()?);
        Ok((remaining > 0).then_some(remaining.min(u32::MAX as i64) as u32))
    }

    async fn failure<H: Host>(
        &self,
        host: &H,
        id: CredentialId,
        owner: &[u8],
    ) -> Result<(), CoreError> {
        if !host
            .credentials()
            .renew_refresh(id, owner, REFRESH_LEASE_TTL)
            .await?
        {
            return Ok(());
        }
        let failures = self.state.as_ref().map_or(0, |state| state.failures);
        let retry_after = REFRESH_BACKOFF[usize::from(failures).min(REFRESH_BACKOFF.len() - 1)];
        let state = RefreshBackoff {
            failures: failures.saturating_add(1),
            retry_at: unix_now()?.saturating_add(retry_after as i64),
            owner: owner.to_vec(),
        };
        let value = serde_json::to_vec(&state).map_err(|_| {
            CoreError::Internal("credential refresh backoff encoding failed".into())
        })?;
        host.cache()
            .compare_and_swap(
                &self.key,
                self.value.clone(),
                Some(value),
                Some(REFRESH_BACKOFF_TTL),
            )
            .await?;
        Ok(())
    }

    async fn clear<H: Host>(&self, host: &H) -> Result<(), CoreError> {
        // Clear only the state this operation observed before sending. A slow
        // older owner cannot erase a later owner's failure/cooldown.
        host.cache()
            .compare_and_swap(&self.key, self.value.clone(), None, None)
            .await?;
        Ok(())
    }
}

struct RefreshGuard<H: Host> {
    host: crate::Shared<H>,
    id: CredentialId,
    owner: Vec<u8>,
    released: bool,
}

impl<H: Host> RefreshGuard<H> {
    async fn release(&mut self) {
        if self
            .host
            .credentials()
            .release_refresh(self.id, &self.owner)
            .await
            .is_ok()
        {
            self.released = true;
        }
    }
}

impl<H: Host> Drop for RefreshGuard<H> {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        if let Some(spawner) = self.host.spawner() {
            let host = self.host.clone();
            let id = self.id;
            let owner = self.owner.clone();
            spawner.spawn(Box::pin(async move {
                let _ = host.credentials().release_refresh(id, &owner).await;
            }));
        }
        // Hosts without a spawner rely on the finite TTL if cancelled.
    }
}

async fn load_checked<H: Host>(
    host: &H,
    id: CredentialId,
    channel: &str,
) -> Result<CredentialRecord, CoreError> {
    check_record(host.credentials().load(id).await?, id, channel)
}

async fn load_current_checked<H: Host>(
    host: &H,
    id: CredentialId,
    channel: &str,
) -> Result<CredentialRecord, CoreError> {
    check_record(host.credentials().load_current(id).await?, id, channel)
}

fn check_record(
    record: CredentialRecord,
    id: CredentialId,
    channel: &str,
) -> Result<CredentialRecord, CoreError> {
    if record.id != id {
        return Err(CoreError::Internal(
            "credential store returned the wrong credential".into(),
        ));
    }
    if record.channel != channel {
        return Err(CoreError::Internal(
            "credential channel does not match its provider".into(),
        ));
    }
    Ok(record)
}

fn refresh_due(channel: &dyn Channel, record: &CredentialRecord, now: i64) -> bool {
    channel
        .refresh_due(&record.secret)
        .is_some_and(|due| due <= now)
}

fn unix_now() -> Result<i64, CoreError> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CoreError::Internal("system clock is before the Unix epoch".into()))?
        .as_secs();
    i64::try_from(seconds).map_err(|_| CoreError::Internal("Unix time exceeds i64".into()))
}

struct BufferedHttp<'a, H>(&'a H, &'a ProviderRef);

impl<H: Host> SimpleHttp for BufferedHttp<'_, H> {
    fn send<'a>(
        &'a self,
        mut request: http::Request<Bytes>,
    ) -> BoxFuture<'a, Result<http::Response<Bytes>, ChannelError>> {
        if let Err(error) = crate::fingerprint::apply_request(&mut request, self.1) {
            return Box::pin(async move { Err(ChannelError::Refresh(error.to_string())) });
        }
        let send = self.0.transport().send(request);
        let exchange = async move {
            let response = send
                .await
                .map_err(|error| ChannelError::Refresh(error.to_string()))?;
            let (parts, mut stream) = response.into_parts();
            let mut body = BytesMut::new();
            while let Some(chunk) = stream.next().await {
                body.extend_from_slice(
                    &chunk.map_err(|error| ChannelError::Refresh(error.to_string()))?,
                );
            }
            Ok(http::Response::from_parts(parts, body.freeze()))
        };
        Box::pin(async move {
            let timeout = async { self.0.wait(REFRESH_HTTP_TIMEOUT).await };
            futures_util::pin_mut!(exchange, timeout);
            match futures_util::future::select(exchange, timeout).await {
                futures_util::future::Either::Left((result, _)) => result,
                futures_util::future::Either::Right(_) => {
                    Err(ChannelError::Refresh("credential refresh timed out".into()))
                }
            }
        })
    }
}
