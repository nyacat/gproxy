use bytes::Bytes;
use gproxy_store::records::{PriceRateInput, PriceRuleInput};
use http::{Response, StatusCode};

use crate::dto::{
    PriceCatalogDto, PriceMetricDto, PriceProfileDto, PriceProfileKindDto, PriceRateDto,
    PriceRateWriteRequest, PriceRuleDto, PriceRuleWriteRequest,
};
use crate::handlers::util;
use crate::route::Entity;
use crate::{AdminError, State, response};

const MILLION: u64 = 1_000_000;

pub(super) fn catalog() -> Result<Response<Bytes>, AdminError> {
    use PriceProfileKindDto::*;
    let profile = |kind, metrics: &[(&str, u64)]| PriceProfileDto {
        kind,
        metrics: metrics
            .iter()
            .map(|(metric, unit_size)| PriceMetricDto {
                metric: (*metric).into(),
                unit_size: *unit_size,
            })
            .collect(),
    };
    response::json(
        StatusCode::OK,
        &PriceCatalogDto {
            service_tiers: gproxy_core::PRICING_SERVICE_TIERS
                .into_iter()
                .map(str::to_owned)
                .collect(),
            profiles: vec![
                profile(
                    Generation,
                    &[
                        ("input_tokens", MILLION),
                        ("output_tokens", MILLION),
                        ("cached_input_tokens", MILLION),
                        ("cache_creation_5m_tokens", MILLION),
                        ("cache_creation_30m_tokens", MILLION),
                        ("cache_creation_1h_tokens", MILLION),
                        ("reasoning_tokens", MILLION),
                    ],
                ),
                profile(
                    Embedding,
                    &[
                        ("input_tokens", MILLION),
                        ("audio_input_tokens", MILLION),
                        ("image_input_tokens", MILLION),
                        ("video_input_tokens", MILLION),
                    ],
                ),
                profile(Rerank, &[("input_tokens", MILLION), ("search_units", 1)]),
                profile(
                    Image,
                    &[
                        ("input_tokens", MILLION),
                        ("image_input_tokens", MILLION),
                        ("image_output_tokens", MILLION),
                        ("image_outputs", 1),
                    ],
                ),
                profile(
                    Audio,
                    &[
                        ("input_tokens", MILLION),
                        ("output_tokens", MILLION),
                        ("audio_input_tokens", MILLION),
                        ("cached_audio_input_tokens", MILLION),
                        ("audio_output_tokens", MILLION),
                        ("audio_seconds", 1),
                    ],
                ),
                profile(
                    Video,
                    &[
                        ("video_input_tokens", MILLION),
                        ("video_tokens", MILLION),
                        ("video_seconds", 1),
                        ("video_outputs", 1),
                    ],
                ),
                profile(Tools, &[("web_searches", 1), ("web_fetches", 1)]),
            ],
        },
    )
}

pub(super) async fn list(
    state: &impl State,
    entity: Entity,
) -> Result<Response<Bytes>, AdminError> {
    let snapshot = state.store().control_snapshot().await?;
    match entity {
        Entity::PriceRules => response::json(
            StatusCode::OK,
            &snapshot
                .price_rules
                .iter()
                .map(|value| PriceRuleDto {
                    id: value.id,
                    provider_id: value.provider_id,
                    model_pattern: value.model_pattern.clone(),
                    tiers: value.tiers.clone(),
                    priority: value.priority,
                    enabled: value.enabled,
                })
                .collect::<Vec<_>>(),
        ),
        Entity::PriceRates => response::json(
            StatusCode::OK,
            &snapshot
                .price_rates
                .iter()
                .map(|value| PriceRateDto {
                    id: value.id,
                    rule_id: value.rule_id,
                    metric: value.metric.clone(),
                    unit_size: value.unit_size,
                    price: value.price.normalize().to_string(),
                    conditions: value.conditions.clone(),
                    priority: value.priority,
                })
                .collect::<Vec<_>>(),
        ),
        _ => Err(AdminError::NotFound),
    }
}

pub(super) async fn create(
    state: &impl State,
    entity: Entity,
    body: &Bytes,
) -> Result<Response<Bytes>, AdminError> {
    let id = match entity {
        Entity::PriceRules => {
            let input = rule(util::parse(body)?)?;
            super::control::validators::price_rule(state, input.provider_id).await?;
            state.store().insert_price_rule(&input).await?
        }
        Entity::PriceRates => {
            let input = rate(util::parse(body)?)?;
            super::control::validators::price_rate(state, input.rule_id).await?;
            state.store().insert_price_rate(&input).await?
        }
        _ => return Err(AdminError::NotFound),
    };
    util::created(state, id).await
}

pub(super) async fn update(
    state: &impl State,
    entity: Entity,
    id: i64,
    body: &Bytes,
) -> Result<Response<Bytes>, AdminError> {
    let applied = match entity {
        Entity::PriceRules => {
            let input = rule(util::parse(body)?)?;
            super::control::validators::price_rule(state, input.provider_id).await?;
            state.store().update_price_rule(id, &input).await?
        }
        Entity::PriceRates => {
            let input = rate(util::parse(body)?)?;
            super::control::validators::price_rate(state, input.rule_id).await?;
            state.store().update_price_rate(id, &input).await?
        }
        _ => return Err(AdminError::NotFound),
    };
    util::updated(state, applied).await
}

pub(super) async fn delete(
    state: &impl State,
    entity: Entity,
    id: i64,
) -> Result<Response<Bytes>, AdminError> {
    let applied = match entity {
        Entity::PriceRules => state.store().delete_price_rule(id).await?,
        Entity::PriceRates => state.store().delete_price_rate(id).await?,
        _ => return Err(AdminError::NotFound),
    };
    util::updated(state, applied).await
}

pub(super) fn rule(request: PriceRuleWriteRequest) -> Result<PriceRuleInput, AdminError> {
    if request.model_pattern.trim().is_empty() {
        return Err(AdminError::BadRequest(
            "price rule model_pattern must not be blank".into(),
        ));
    }
    Ok(PriceRuleInput {
        provider_id: request.provider_id,
        model_pattern: request.model_pattern,
        tiers: request.tiers,
        priority: request.priority,
        enabled: request.enabled,
    })
}

pub(super) fn rate(request: PriceRateWriteRequest) -> Result<PriceRateInput, AdminError> {
    if request.metric.trim().is_empty() || request.unit_size == 0 {
        return Err(AdminError::BadRequest(
            "price rate metric must not be blank and unit_size must be positive".into(),
        ));
    }
    let price = request
        .price
        .parse::<rust_decimal::Decimal>()
        .map_err(|_| AdminError::BadRequest("price must be a decimal".into()))?;
    if price < rust_decimal::Decimal::ZERO {
        return Err(AdminError::BadRequest("price must not be negative".into()));
    }
    if let Some(conditions) = &request.conditions {
        let object = conditions.as_object().ok_or_else(|| {
            AdminError::BadRequest("price rate conditions must be an object".into())
        })?;
        if object.values().any(|value| {
            !matches!(
                value,
                serde_json::Value::String(_)
                    | serde_json::Value::Number(_)
                    | serde_json::Value::Bool(_)
            )
        }) {
            return Err(AdminError::BadRequest(
                "price rate conditions must contain scalar values".into(),
            ));
        }
    } else if matches!(
        request.metric.as_str(),
        "input_tokens" | "output_tokens" | "cached_input_tokens"
    ) && price
        .checked_mul(rust_decimal::Decimal::from(MILLION))
        .is_none()
    {
        return Err(AdminError::BadRequest(
            "price exceeds the supported per-million range".into(),
        ));
    }
    Ok(PriceRateInput {
        rule_id: request.rule_id,
        metric: request.metric,
        unit_size: request.unit_size,
        price,
        conditions: request.conditions,
        priority: request.priority,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn price_rate_request() -> PriceRateWriteRequest {
        PriceRateWriteRequest {
            rule_id: 1,
            metric: "input_tokens".into(),
            unit_size: MILLION,
            price: "1".into(),
            conditions: None,
            priority: 0,
        }
    }

    #[test]
    fn price_rate_rejects_conditions_that_snapshot_compilation_cannot_read() {
        for conditions in [
            serde_json::json!([]),
            serde_json::json!("fast"),
            serde_json::json!({"tier": null}),
            serde_json::json!({"tier": []}),
            serde_json::json!({"tier": {"name": "fast"}}),
        ] {
            let mut request = price_rate_request();
            request.conditions = Some(conditions);
            assert!(matches!(rate(request), Err(AdminError::BadRequest(_))));
        }
        let mut request = price_rate_request();
        let conditions = serde_json::json!({"tier": "fast", "cached": true, "tokens": 10});
        request.conditions = Some(conditions.clone());
        assert_eq!(rate(request).unwrap().conditions, Some(conditions));
    }

    #[test]
    fn price_rate_rejects_per_million_overflow_before_a_write() {
        for metric in ["input_tokens", "output_tokens", "cached_input_tokens"] {
            let mut request = price_rate_request();
            request.metric = metric.into();
            request.price = rust_decimal::Decimal::MAX.to_string();
            assert!(matches!(rate(request), Err(AdminError::BadRequest(_))));
        }
        let request = price_rate_request();
        assert_eq!(rate(request).unwrap().price, rust_decimal::Decimal::ONE);

        // Conditional rates are compiled directly per unit, without the
        // per-million multiplication used by the three unconditional metrics.
        let mut request = price_rate_request();
        request.price = rust_decimal::Decimal::MAX.to_string();
        request.conditions = Some(serde_json::json!({"tier": "fast"}));
        assert_eq!(rate(request).unwrap().price, rust_decimal::Decimal::MAX);
    }

    #[test]
    fn price_catalog_covers_model_and_hosted_tool_usage_shapes() {
        let response = catalog().expect("price catalog");
        let catalog: PriceCatalogDto =
            serde_json::from_slice(response.body()).expect("catalog body");
        assert_eq!(catalog.service_tiers, gproxy_core::PRICING_SERVICE_TIERS);
        assert_eq!(catalog.profiles.len(), 7);
        for kind in [
            PriceProfileKindDto::Generation,
            PriceProfileKindDto::Embedding,
            PriceProfileKindDto::Rerank,
            PriceProfileKindDto::Image,
            PriceProfileKindDto::Audio,
            PriceProfileKindDto::Video,
            PriceProfileKindDto::Tools,
        ] {
            assert!(catalog.profiles.iter().any(|profile| profile.kind == kind));
        }
        let tools = catalog
            .profiles
            .iter()
            .find(|profile| profile.kind == PriceProfileKindDto::Tools)
            .expect("tools profile");
        assert_eq!(
            tools
                .metrics
                .iter()
                .map(|metric| metric.metric.as_str())
                .collect::<Vec<_>>(),
            ["web_searches", "web_fetches"]
        );
    }
}
