use crate::AppHandle;
use futures_util::{StreamExt, stream};
use gproxy_core::{CacheBackend, Host};
use std::time::Duration;

pub(crate) fn now() -> i64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_secs() as i64
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn schedule(app: &AppHandle) {
    let inner = std::sync::Arc::downgrade(&app.inner);
    app.inner
        .host
        .services
        .spawner
        .spawn_maintenance(app.inner.shutdown.subscribe(), async move {
            let mut rebuild_after = 0;
            loop {
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                let task_app = AppHandle { inner };
                if let Err(error) = sweep(&task_app, &mut rebuild_after).await {
                    tracing::warn!(error = %error, "quota maintenance failed");
                }
                drop(task_app);
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn schedule(_app: &AppHandle) {}

async fn sweep(app: &AppHandle, rebuild_after: &mut i64) -> Result<(), gproxy_admin::AdminError> {
    let now = now();
    let store = &app.inner.host.services.store;
    let pending = store
        .pending_credential_quota_rebuilds(None, *rebuild_after)
        .await?;
    *rebuild_after = pending.last().copied().unwrap_or(0);
    for id in pending {
        if let Err(error) = store.repair_credential_quota_cycle(id).await {
            tracing::warn!(cycle_id = id, error = %error, "quota cycle rebuild remains pending");
        }
    }
    let active = store.active_usage_credentials(now - 1800).await?;
    let snapshot = app.inner.host.services.control.current();
    for cycle in store.unclosed_credential_quota_cycles(None).await? {
        if cycle.accounting_end_ms.is_some_and(|end| end <= now * 1000) {
            let end = cycle.accounting_end_ms.expect("checked end") / 1000;
            store
                .close_credential_quota_cycle(
                    cycle.id,
                    gproxy_store::records::QuotaCycleCloseReason::BoundaryCrossed,
                    end,
                )
                .await?;
        }
    }
    for id in &active {
        store.repair_credential_quota(*id, now).await?;
    }
    let credentials = snapshot
        .credentials
        .iter()
        .filter(|credential| {
            credential.enabled
                && active.contains(&credential.id)
                && snapshot
                    .providers
                    .iter()
                    .any(|provider| provider.id == credential.provider_id && provider.enabled)
        })
        .map(|credential| credential.id)
        .collect::<Vec<_>>();
    let results = stream::iter(credentials)
        .map(|id| refresh(app, id, now))
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await;
    for result in results {
        result?;
    }
    Ok(())
}

async fn refresh(app: &AppHandle, id: i64, _now: i64) -> Result<(), gproxy_admin::AdminError> {
    if let Err(error) = crate::admin::quota_probe::automatic(app, id).await {
        tracing::warn!(credential_id = id, error = %error, "quota refresh unavailable");
    }
    Ok(())
}

pub(crate) async fn opportunistic(app: &AppHandle) {
    if app.inner.host.spawner().is_some() {
        return;
    }
    let due = app
        .inner
        .host
        .services
        .cache
        .compare_and_swap(
            "quota:maintenance:due",
            None,
            Some(vec![1]),
            Some(Duration::from_secs(30)),
        )
        .await;
    if matches!(due, Ok(true))
        && let Err(error) = sweep(app, &mut 0).await
    {
        tracing::warn!(error = %error, "opportunistic quota maintenance failed");
    }
}
