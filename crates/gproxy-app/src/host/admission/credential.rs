use gproxy_channel_api::BoxFuture;
use gproxy_core::{CoreError, Target};

use super::super::AppHost;
use super::auth::unix_now;
use super::reserve::{increment_window, rollback_error};
use super::types::CounterCharge;

pub(in crate::host) fn admit<'a>(
    host: &'a AppHost,
    request_id: &'a str,
    target: &'a Target,
    body: &'a bytes::Bytes,
    settle: gproxy_protocol::SettleMode,
) -> BoxFuture<'a, Result<(), CoreError>> {
    Box::pin(async move {
        super::credential_budget::reserve(host, request_id, target, body, settle).await?;
        let snapshot = host.services.control.current();
        let Some(credential) = snapshot
            .credentials
            .iter()
            .find(|credential| credential.id == target.credential.0)
        else {
            return Ok(());
        };
        let tokens = match credential.tpm_limit {
            Some(_) => count(host, request_id, target, body).await?,
            None => 0,
        };
        if credential.tpm_limit.is_some_and(|limit| tokens > limit) {
            return Err(rate_limited());
        }
        let now = unix_now();
        let window = now - now.rem_euclid(60);
        let mut charged = Vec::new();
        for (kind, amount, limit) in [
            ("rpm", 1, credential.rpm_limit.map(i64::from)),
            (
                "tpm",
                i64::try_from(tokens).unwrap_or(i64::MAX),
                credential
                    .tpm_limit
                    .and_then(|limit| i64::try_from(limit).ok()),
            ),
        ] {
            let Some(limit) = limit else { continue };
            let key = format!("gproxy:credential-rate:{}:{kind}:{window}", credential.id);
            let value = match increment_window(host, &key, amount, 60, now).await {
                Ok(value) => value,
                Err(error) => return rollback_error(host, charged, error).await,
            };
            charged.push(CounterCharge { key, amount });
            if value > limit {
                return rollback_error(host, charged, rate_limited()).await;
            }
        }
        Ok(())
    })
}

async fn count(
    host: &AppHost,
    request_id: &str,
    target: &Target,
    body: &bytes::Bytes,
) -> Result<u64, CoreError> {
    let cache = host.services.token_counts.request(request_id);
    let map = target.provider.settings.get("tokenizer_map");
    if let Some(tokens) = cache.get(&target.upstream_model, map, body) {
        return Ok(tokens);
    }
    let model = target.upstream_model.clone();
    let map = map.cloned();
    let body = body.clone();
    #[cfg(not(target_arch = "wasm32"))]
    {
        let registry = host.services.tokenizers.clone();
        tokio::task::spawn_blocking(move || {
            cache.get_or_insert(&model, map.as_ref(), &body, || {
                gproxy_tokenize::count(&model, &body, map.as_ref(), &registry)
            })
        })
        .await
        .map_err(|error| CoreError::Internal(format!("tokenizer task failed: {error}")))
    }
    #[cfg(target_arch = "wasm32")]
    {
        Ok(cache.get_or_insert(&model, map.as_ref(), &body, || {
            gproxy_tokenize::count(&model, &body, map.as_ref(), ())
        }))
    }
}

fn rate_limited() -> CoreError {
    CoreError::RateLimited {
        retry_after_secs: 60,
    }
}
