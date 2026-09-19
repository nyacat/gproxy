use gproxy_channel_api::CallerIdentity;
use gproxy_core::{
    CacheBackend, ControlPlane, CoreError, NormalizedUsage, Plan, Pricing, RequestCtx,
};
use gproxy_protocol::{OperationKey, SettleMode};

use super::super::AppHost;
use super::auth::subject_matches;
use super::types::{AdmissionState, QuotaReservation, reservation_key};

pub(super) async fn reserve(
    host: &AppHost,
    identity: &CallerIdentity,
    request: &RequestCtx,
    operation: Option<OperationKey>,
    plan: &Plan,
    now: i64,
    state: &mut AdmissionState,
) -> Result<(), CoreError> {
    if !operation.is_some_and(|key| key.operation().spec().settle != SettleMode::Free) {
        return Ok(());
    }
    if !has_spend_limit(host, identity) {
        return Ok(());
    }
    let estimate = estimated_cost_micros(host, request, plan).await?;
    reserve_cost(host, identity, &request.request_id, estimate, now, state).await
}

pub(super) async fn reserve_retry(
    host: &AppHost,
    identity: &CallerIdentity,
    request_id: &str,
    body: &bytes::Bytes,
    target: &gproxy_core::Target,
    state: &mut AdmissionState,
) -> Result<(), CoreError> {
    if !has_spend_limit(host, identity) {
        return Ok(());
    }
    let estimate = estimated_target_cost_micros(host, request_id, body, target).await?;
    reserve_cost(
        host,
        identity,
        request_id,
        estimate,
        super::auth::unix_now(),
        state,
    )
    .await
}

fn has_spend_limit(host: &AppHost, identity: &CallerIdentity) -> bool {
    host.services.control.current().quotas.iter().any(|quota| {
        quota.enabled
            && subject_matches(&quota.subject_kind, quota.subject_id, identity)
            && quota.limits().next().is_some()
    })
}

async fn reserve_cost(
    host: &AppHost,
    identity: &CallerIdentity,
    request_id: &str,
    estimate: i64,
    now: i64,
    state: &mut AdmissionState,
) -> Result<(), CoreError> {
    let key = reservation_key(request_id);
    let snapshot = host.services.control.current();
    for quota in snapshot.quotas.iter().filter(|quota| {
        quota.enabled && subject_matches(&quota.subject_kind, quota.subject_id, identity)
    }) {
        if host
            .services
            .cache
            .get(&super::window::failure_key(quota.id))
            .await?
            .is_some()
        {
            return Err(CoreError::Store(gproxy_core::error::StoreError(
                "quota settlement failed; repair accounting before retrying".into(),
            )));
        }
        for (kind, limit) in quota.limits() {
            super::window::reserve(
                host,
                quota.id,
                kind,
                limit,
                estimate,
                now,
                &key,
                |window_id| {
                    let expected = serde_json::to_vec(state).expect("admission state serializes");
                    state.reservations.push(QuotaReservation {
                        window_id,
                        quota_id: quota.id,
                        slot: state.reservations.len() as u32,
                        cache_key: super::window::pending_key(window_id),
                        estimated_cost_micros: estimate,
                        cost_recorded: false,
                        released: false,
                    });
                    let updated = serde_json::to_vec(state).expect("admission state serializes");
                    (expected, updated)
                },
            )
            .await?;
        }
    }
    Ok(())
}

async fn estimated_cost_micros(
    host: &AppHost,
    request: &RequestCtx,
    plan: &Plan,
) -> Result<i64, CoreError> {
    let mut seen = std::collections::BTreeSet::new();
    let candidates = plan
        .targets
        .iter()
        .filter(|target| seen.insert((target.provider.id, target.upstream_model.clone())))
        .filter_map(|target| candidate(host, target))
        .collect::<Vec<_>>();
    estimate_micros(host, &request.request_id, &request.body, candidates).await
}

/// The cost this request would settle at on one target, in micro-units.
pub(super) async fn estimated_target_cost_micros(
    host: &AppHost,
    request_id: &str,
    body: &bytes::Bytes,
    target: &gproxy_core::Target,
) -> Result<i64, CoreError> {
    let candidates = candidate(host, target).into_iter().collect();
    estimate_micros(host, request_id, body, candidates).await
}

fn candidate(
    host: &AppHost,
    target: &gproxy_core::Target,
) -> Option<(String, Option<serde_json::Value>, Pricing)> {
    let pricing = host
        .services
        .control
        .pricing(&target.provider, &target.upstream_model)?;
    Some((
        target.upstream_model.clone(),
        target.provider.settings.get("tokenizer_map").cloned(),
        pricing,
    ))
}

async fn estimate_micros(
    host: &AppHost,
    request_id: &str,
    body: &bytes::Bytes,
    candidates: Vec<(String, Option<serde_json::Value>, Pricing)>,
) -> Result<i64, CoreError> {
    let cost = host
        .maximum_candidate_cost(request_id, body.clone(), candidates)
        .await?;
    gproxy_core::usage::cost_to_micros(cost)
        .ok_or_else(|| CoreError::Internal("admission cost estimate exceeds counter".into()))
}

impl AppHost {
    /// Tokenizing blocks, so a native host hands it to the blocking pool; the
    /// edge host has no vocabularies and no pool to hand it to.
    async fn maximum_candidate_cost(
        &self,
        request_id: &str,
        body: bytes::Bytes,
        candidates: Vec<(String, Option<serde_json::Value>, Pricing)>,
    ) -> Result<rust_decimal::Decimal, CoreError> {
        let cache = self.services.token_counts.request(request_id);
        let cached = candidates.iter().try_fold(
            rust_decimal::Decimal::ZERO,
            |maximum, (model, map, pricing)| {
                let input_tokens = cache.get(model, map.as_ref(), &body)?;
                let usage = NormalizedUsage {
                    input_tokens,
                    ..Default::default()
                };
                Some(maximum.max(pricing.clone().for_request(&body).cost(&usage)))
            },
        );
        if let Some(cost) = cached {
            return Ok(cost);
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let registry = self.services.tokenizers.clone();
            tokio::task::spawn_blocking(move || maximum_cost(&body, &candidates, &registry, &cache))
                .await
                .map_err(|error| CoreError::Internal(format!("tokenizer task failed: {error}")))
        }
        #[cfg(target_arch = "wasm32")]
        {
            Ok(maximum_cost(&body, &candidates, (), &cache))
        }
    }
}

fn maximum_cost(
    body: &bytes::Bytes,
    candidates: &[(String, Option<serde_json::Value>, Pricing)],
    registry: gproxy_tokenize::RegistryHandle<'_>,
    cache: &super::super::token_counts::RequestTokenCounts,
) -> rust_decimal::Decimal {
    if candidates.is_empty() {
        return rust_decimal::Decimal::ZERO;
    }
    candidates
        .iter()
        .map(|(model, map, pricing)| {
            let input_tokens = cache.get_or_insert(model, map.as_ref(), body, || {
                gproxy_tokenize::count(model, body, map.as_ref(), registry)
            });
            let usage = NormalizedUsage {
                input_tokens,
                ..Default::default()
            };
            pricing.clone().for_request(body).cost(&usage)
        })
        .max()
        .unwrap_or_default()
}
