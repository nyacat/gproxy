use std::time::Duration;

use gproxy_channel_api::{BoxFuture, CallerIdentity};
use gproxy_core::{CacheBackend, CoreError, Plan, RequestCtx};
use gproxy_protocol::OperationKey;

use super::super::AppHost;
use super::auth::{authorize, subject_matches, unix_now};
use super::types::{AdmissionState, CounterCharge, IdentityState, reservation_key};

pub(in crate::host) fn admit<'a>(
    host: &'a AppHost,
    identity: &'a CallerIdentity,
    request: &'a RequestCtx,
    operation: Option<OperationKey>,
    model: Option<&'a str>,
    plan: &'a Plan,
) -> BoxFuture<'a, Result<Plan, CoreError>> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let host = host.clone();
        let identity = identity.clone();
        let request = request.clone();
        let model = model.map(str::to_owned);
        let plan = plan.clone();
        Box::pin(async move {
            let task_host = host.clone();
            let task = host.services.spawner.spawn_tracked(async move {
                let host = task_host;
                let plan = admit_inner(
                    &host,
                    &identity,
                    &request,
                    operation,
                    model.as_deref(),
                    &plan,
                )
                .await?;
                Ok::<_, CoreError>(AdmittedPlan {
                    host,
                    request_id: request.request_id,
                    plan: Some(plan),
                })
            });
            let mut admitted = task.await.map_err(|error| {
                CoreError::Internal(format!("admission task failed: {error}"))
            })??;
            Ok(admitted.plan.take().expect("admitted plan"))
        })
    }
    #[cfg(target_arch = "wasm32")]
    admit_inner(host, identity, request, operation, model, plan)
}

// If the caller disappears while admission is awaiting storage, let the write
// finish and refund the resulting reservation. This also handles cancellation
// after the task sent its result but before the caller consumed it.
#[cfg(not(target_arch = "wasm32"))]
struct AdmittedPlan {
    host: AppHost,
    request_id: String,
    plan: Option<Plan>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for AdmittedPlan {
    fn drop(&mut self) {
        if self.plan.is_some() {
            let host = self.host.clone();
            let request_id = self.request_id.clone();
            let task_host = host.clone();
            drop(host.services.spawner.spawn_tracked(async move {
                super::finish::finish(&task_host, &request_id, None).await;
            }));
        }
    }
}

fn admit_inner<'a>(
    host: &'a AppHost,
    identity: &'a CallerIdentity,
    request: &'a RequestCtx,
    operation: Option<OperationKey>,
    model: Option<&'a str>,
    plan: &'a Plan,
) -> BoxFuture<'a, Result<Plan, CoreError>> {
    Box::pin(async move {
        let mut token_scope = host.services.token_counts.scope(&request.request_id);
        let snapshot = host.services.control.current();
        let oauth_identity = super::auth::oauth_admission(host, identity, operation).await?;
        let identity = oauth_identity.as_ref().unwrap_or(identity);
        let model = host
            .services
            .control
            .authorization_model(model, &request.mode);
        let plan = authorize(&snapshot, identity, operation, model.as_deref(), plan)?;
        let now = unix_now();
        let mut state = AdmissionState {
            identity: IdentityState::from(identity),
            model,
            operation: operation.map(|key| key.operation().id().to_owned()),
            reservations: Vec::new(),
        };
        let key = reservation_key(&request.request_id);
        let bytes = serde_json::to_vec(&state).expect("admission state serializes");
        if let Err(error) = initialize_state(host, &key, bytes).await {
            if matches!(error, CoreError::Store(_)) {
                super::finish::finish(host, &request.request_id, None).await;
            }
            return Err(error);
        }
        if let Err(error) =
            super::quota::reserve(host, identity, request, operation, &plan, now, &mut state).await
        {
            super::finish::finish(host, &request.request_id, None).await;
            return Err(error);
        }
        let mut charged = Vec::new();

        for limit in snapshot
            .rate_limits
            .iter()
            .filter(|limit| subject_matches(&limit.subject_kind, limit.subject_id, identity))
        {
            let start = window_start(now, limit.window_seconds);
            let key = format!("gproxy:rate:{}:{start}", limit.id);
            let count = match increment_window(host, &key, 1, limit.window_seconds, now).await {
                Ok(count) => count,
                Err(error) => {
                    super::finish::finish(host, &request.request_id, None).await;
                    return rollback_error(host, charged, error).await;
                }
            };
            charged.push(CounterCharge { key, amount: 1 });
            if count > i64::try_from(limit.requests).expect("stored rate limit fits i64") {
                super::finish::finish(host, &request.request_id, None).await;
                return rollback_error(
                    host,
                    charged,
                    CoreError::RateLimited {
                        retry_after_secs: u32::try_from(limit.window_seconds).unwrap_or(u32::MAX),
                    },
                )
                .await;
            }
        }

        token_scope.keep();
        Ok(plan)
    })
}

// Only an empty state is installed here; all monetary writes subsequently
// compare and replace it in the same atomic operation as their counter charge.
pub(super) async fn initialize_state(
    host: &AppHost,
    key: &str,
    bytes: Vec<u8>,
) -> Result<(), CoreError> {
    super::super::settlement_retry::run(host, || async {
        if host
            .services
            .cache
            .compare_and_swap(key, None, Some(bytes.clone()), None)
            .await?
        {
            return Ok(());
        }
        if host.services.cache.get(key).await?.as_ref() == Some(&bytes) {
            return Ok(());
        }
        Err(CoreError::Internal("admission state already exists".into()))
    })
    .await
}

pub(super) async fn increment_window(
    host: &AppHost,
    key: &str,
    amount: i64,
    window_seconds: u64,
    now: i64,
) -> Result<i64, CoreError> {
    let start = window_start(now, window_seconds);
    let seconds = i64::try_from(window_seconds).expect("stored window fits i64");
    let end = start.saturating_add(seconds);
    let ttl = u64::try_from(end.saturating_sub(now)).unwrap_or(1).max(1);
    Ok(host
        .services
        .cache
        .incr(key, amount, Some(Duration::from_secs(ttl)))
        .await?)
}

pub(super) async fn rollback_error<T>(
    host: &AppHost,
    charges: Vec<CounterCharge>,
    error: CoreError,
) -> Result<T, CoreError> {
    for charge in charges.into_iter().rev() {
        if let Err(rollback) = host
            .services
            .cache
            .incr(&charge.key, -charge.amount, None)
            .await
        {
            tracing::error!(error = %rollback, "admission rollback failed");
        }
    }
    Err(error)
}

fn window_start(now: i64, seconds: u64) -> i64 {
    let seconds = i64::try_from(seconds).expect("stored window fits i64");
    now - now.rem_euclid(seconds)
}
