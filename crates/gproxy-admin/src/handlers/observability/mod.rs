mod map;
pub(super) mod pages;
pub(super) mod records;

use std::collections::BTreeMap;

use bytes::Bytes;
use gproxy_store::records::{QuotaRecord, QuotaWindowKind, UsageAggregateQuery, UsageGroupBy};
use http::request::Parts;
use http::{Response, StatusCode};

use crate::dto::{
    QuotaWindowDto, UsageGroupByDto, UsageQueryDto, UsageStatisticsDto, UsageTrendPointDto,
    UsageTrendQueryDto,
};
use crate::handlers::util;
use crate::{AdminError, State, response};

pub(super) async fn usage(
    state: &impl State,
    parts: &Parts,
) -> Result<Response<Bytes>, AdminError> {
    let query = serde_urlencoded::from_str::<UsageQueryDto>(parts.uri.query().unwrap_or_default())
        .map_err(|error| AdminError::BadRequest(error.to_string()))?;
    let (from, to) = range(query.from, query.to)?;
    let group_by = match query.group_by {
        Some(UsageGroupByDto::UserKey) => UsageGroupBy::UserKey,
        Some(UsageGroupByDto::User) => UsageGroupBy::User,
        Some(UsageGroupByDto::Provider) => UsageGroupBy::Provider,
        Some(UsageGroupByDto::Model) => UsageGroupBy::Model,
        None => UsageGroupBy::Dimensions,
    };
    let records = state
        .store()
        .usage_aggregate(&UsageAggregateQuery {
            from,
            to,
            group_by,
            user_key_id: query.user_key_id,
            user_id: query.user_id,
            provider_id: query.provider_id,
            credential_id: query.credential_id,
            model: query.model,
        })
        .await?;
    let records = records
        .into_iter()
        .map(|record| UsageStatisticsDto {
            user_key_id: matches!(group_by, UsageGroupBy::UserKey | UsageGroupBy::Dimensions)
                .then_some(record.user_key_id)
                .flatten(),
            user_id: matches!(group_by, UsageGroupBy::User | UsageGroupBy::Dimensions)
                .then_some(record.user_id)
                .flatten(),
            provider_id: matches!(group_by, UsageGroupBy::Provider | UsageGroupBy::Dimensions)
                .then_some(record.provider_id),
            model: matches!(group_by, UsageGroupBy::Model | UsageGroupBy::Dimensions)
                .then_some(record.model),
            requests: record.requests,
            input_tokens: record.input_tokens,
            output_tokens: record.output_tokens,
            cached_input_tokens: record.cached_input_tokens,
            cache_creation_5m_tokens: record.cache_creation_5m_tokens,
            cache_creation_30m_tokens: record.cache_creation_30m_tokens,
            cache_creation_1h_tokens: record.cache_creation_1h_tokens,
            cost: record.cost.normalize().to_string(),
        })
        .collect::<Vec<_>>();
    response::json(StatusCode::OK, &records)
}

pub(super) async fn usage_trend(
    state: &impl State,
    parts: &Parts,
) -> Result<Response<Bytes>, AdminError> {
    let query =
        serde_urlencoded::from_str::<UsageTrendQueryDto>(parts.uri.query().unwrap_or_default())
            .map_err(|error| AdminError::BadRequest(error.to_string()))?;
    let (from, to) = range(query.from, query.to)?;
    let points = state
        .store()
        .usage_trend(from, to)
        .await?
        .into_iter()
        .map(|point| UsageTrendPointDto {
            bucket_start: point.bucket_start,
            requests: point.requests,
            input_tokens: point.input_tokens,
            output_tokens: point.output_tokens,
            cached_input_tokens: point.cached_input_tokens,
            cost: point.cost.normalize().to_string(),
        })
        .collect::<Vec<_>>();
    response::json(StatusCode::OK, &points)
}

pub(super) async fn quota_windows(
    state: &impl State,
    parts: &Parts,
) -> Result<Response<Bytes>, AdminError> {
    let query = util::query(parts);
    let subject_id = util::parse_i64(util::value(&query, "subject_id"), "subject_id")?;
    let subject_kind = util::value(&query, "subject_kind");
    let snapshot = state.store().control_snapshot().await?;
    let quotas = snapshot
        .quotas
        .iter()
        .filter(|quota| quota.enabled)
        .filter(|quota| subject_id.is_none_or(|id| quota.subject_id == id))
        .filter(|quota| subject_kind.is_none_or(|kind| quota.subject_kind == kind))
        .cloned()
        .collect::<Vec<_>>();
    let values = materialize_quota_windows(state, &quotas)
        .await?
        .into_iter()
        .map(|(_, window)| window)
        .collect::<Vec<_>>();
    response::json(StatusCode::OK, &values)
}

pub(crate) async fn materialize_quota_windows(
    state: &impl State,
    quotas: &[QuotaRecord],
) -> Result<Vec<(QuotaWindowKind, QuotaWindowDto)>, AdminError> {
    let now = crate::auth::now()?;
    let mut active = state
        .store()
        .active_quota_windows()
        .await?
        .into_iter()
        .filter(|window| window.reset_at.is_none_or(|reset| reset > now))
        .map(|window| ((window.quota_id, window.window_kind), window))
        .collect::<BTreeMap<_, _>>();
    let mut values = Vec::new();
    for quota in quotas {
        for kind in map::configured_windows(quota) {
            let value = active
                .remove(&(quota.id, kind))
                .as_ref()
                .and_then(|window| map::quota_window(quota, window))
                .or_else(|| map::unstarted_window(quota, kind, now));
            if let Some(value) = value {
                values.push((kind, value));
            }
        }
    }
    Ok(values)
}

pub(super) async fn credential_cycles(
    state: &impl State,
    parts: &Parts,
) -> Result<Response<Bytes>, AdminError> {
    let query = util::query(parts);
    let from = util::parse_i64(util::value(&query, "from"), "from")?
        .ok_or_else(|| AdminError::BadRequest("from is required".into()))?;
    let to = util::parse_i64(util::value(&query, "to"), "to")?
        .ok_or_else(|| AdminError::BadRequest("to is required".into()))?;
    let (from, to) = range(from, to)?;
    let credential_id = util::parse_i64(util::value(&query, "credential_id"), "credential_id")?;
    let provider_id = util::parse_i64(util::value(&query, "provider_id"), "provider_id")?;
    let include_history = util::value(&query, "include_history")
        .map(|value| {
            value
                .parse::<bool>()
                .map_err(|_| AdminError::BadRequest("include_history must be true or false".into()))
        })
        .transpose()?
        .unwrap_or(false);
    let include_estimate = util::value(&query, "include_estimate")
        .map(|value| {
            value.parse::<bool>().map_err(|_| {
                AdminError::BadRequest("include_estimate must be true or false".into())
            })
        })
        .transpose()?
        .unwrap_or(true);
    read_cycles(
        state,
        parts,
        crate::dto::CredentialCycleReadRequest {
            from,
            to,
            credential_id,
            provider_id,
            include_history,
            include_estimate,
            ..Default::default()
        },
        false,
    )
    .await
}

pub(super) async fn credential_cycles_query(
    state: &impl State,
    parts: &Parts,
    body: &Bytes,
) -> Result<Response<Bytes>, AdminError> {
    let mut request: crate::dto::CredentialCycleReadRequest = util::parse(body)?;
    range(request.from, request.to)?;
    if let Some(ids) = &mut request.cycle_ids {
        if ids.len() > 5_000 || ids.iter().any(|id| *id <= 0) {
            return Err(AdminError::BadRequest(
                "cycle_ids must contain at most 5000 positive ids".into(),
            ));
        }
        ids.sort_unstable();
        ids.dedup();
    }
    read_cycles(state, parts, request, true).await
}

async fn read_cycles(
    state: &impl State,
    parts: &Parts,
    request: crate::dto::CredentialCycleReadRequest,
    bounded: bool,
) -> Result<Response<Bytes>, AdminError> {
    use tracing::Instrument;
    let range_ms = if bounded {
        Some((
            request
                .from
                .checked_mul(1000)
                .ok_or_else(|| AdminError::BadRequest("from is out of range".into()))?,
            request
                .to
                .checked_mul(1000)
                .ok_or_else(|| AdminError::BadRequest("to is out of range".into()))?,
        ))
    } else {
        None
    };
    let options = gproxy_store::records::CredentialQuotaStatisticsOptions {
        cycle_ids: request.cycle_ids.clone(),
        current_only: request.current_only,
        observation_range_ms: range_ms,
    };
    let source = if bounded {
        "admin.credential_cycles.query"
    } else {
        "admin.credential_cycles"
    };
    let client_view = parts
        .headers
        .get("x-gproxy-console-view")
        .and_then(|v| v.to_str().ok())
        .filter(|view| {
            matches!(
                *view,
                "overview" | "providers" | "quota-history" | "quota-details"
            )
        })
        .unwrap_or("unknown");
    let request_id = parts
        .extensions
        .get::<crate::RequestId>()
        .map(|id| id.0.as_str())
        .unwrap_or("unavailable");
    let span = tracing::info_span!("quota.statistics", source, client_view, request_id,
        credential_id = ?request.credential_id, provider_id = ?request.provider_id,
        from = request.from, to = request.to, include_history = request.include_history,
        include_estimate = request.include_estimate, current_only = request.current_only,
        selected_cycles = ?request.cycle_ids.as_ref().map(Vec::len));
    async {
        let mut timing = StatisticsTiming {
            started: web_time::Instant::now(),
            outcome: "cancelled",
            source,
        };
        let result = async {
            let usage_disabled = state
                .store()
                .setting(gproxy_store::records::ENABLE_USAGE)
                .await?
                == Some(serde_json::json!(false));
            let records = state
                .store()
                .credential_quota_statistics_with_options(
                    &gproxy_store::records::CredentialQuotaCycleQuery {
                        credential_id: request.credential_id,
                        provider_id: request.provider_id,
                        from: request.from,
                        to: request.to,
                        calculate: request.include_estimate && !usage_disabled,
                        history: request.include_history,
                    },
                    &options,
                )
                .await?;
            let mut values = records
                .into_iter()
                .map(|record| {
                    let mut value = map::credential_cycle(&record.cycle);
                    value.observations = record.observations.into_iter().map(Into::into).collect();
                    value
                })
                .collect::<Vec<_>>();
            if usage_disabled {
                map::hide_local_usage(&mut values);
            }
            response::json(StatusCode::OK, &values)
        }
        .await;
        timing.outcome = if result.is_ok() { "ok" } else { "error" };
        result
    }
    .instrument(span)
    .await
}

// Drop also covers cancelled requests. The surrounding span carries the
// whitelisted console view and request id even with INFO production logging.
struct StatisticsTiming {
    started: web_time::Instant,
    outcome: &'static str,
    source: &'static str,
}
impl Drop for StatisticsTiming {
    fn drop(&mut self) {
        let elapsed_ms = self.started.elapsed().as_millis() as u64;
        if elapsed_ms >= 200 || self.outcome != "ok" {
            tracing::warn!(
                elapsed_ms,
                outcome = self.outcome,
                source = self.source,
                "admin.statistics.completed"
            );
        } else {
            tracing::debug!(
                elapsed_ms,
                outcome = self.outcome,
                source = self.source,
                "admin.statistics.completed"
            );
        }
    }
}

pub(crate) fn range(from: i64, to: i64) -> Result<(i64, i64), AdminError> {
    if from >= to {
        Err(AdminError::BadRequest("from must be before to".into()))
    } else if to.saturating_sub(from) > 366 * 24 * 60 * 60 {
        Err(AdminError::BadRequest(
            "time range must not exceed 366 days".into(),
        ))
    } else {
        Ok((from, to))
    }
}
