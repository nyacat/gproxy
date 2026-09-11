use gproxy_store::records::{ProviderRuleSetInput, RecordBatch};
use serde::{Serialize, de::DeserializeOwned};

use crate::dto::ProviderRuleSetWriteRequest;
use crate::handlers::{control, identity, pricing, rules};
use crate::route::Entity;
use crate::{AdminError, State};

pub(super) fn record(
    state: &impl State,
    entity: Entity,
    value: &impl Serialize,
) -> Result<RecordBatch, AdminError> {
    Ok(match entity {
        Entity::Organizations => {
            RecordBatch::Organizations(vec![identity::inputs::organization(convert(value)?)?])
        }
        Entity::Teams => RecordBatch::Teams(vec![identity::inputs::team(convert(value)?)?]),
        Entity::Users => RecordBatch::Users(vec![identity::inputs::user(convert(value)?)?]),
        Entity::Providers => {
            RecordBatch::Providers(vec![control::inputs::provider(state, convert(value)?)?])
        }
        Entity::Routes => RecordBatch::Routes(vec![control::inputs::route(convert(value)?)?]),
        Entity::RouteMembers => {
            RecordBatch::RouteMembers(vec![control::inputs::route_member(convert(value)?)?])
        }
        Entity::Aliases => RecordBatch::Aliases(vec![control::inputs::alias(convert(value)?)?]),
        Entity::ModelAliases => {
            RecordBatch::ExposedModels(vec![control::inputs::model_alias(convert(value)?)?])
        }
        Entity::Quotas => RecordBatch::Quotas(vec![identity::inputs::quota(convert(value)?)?]),
        Entity::PriceRules => RecordBatch::PriceRules(vec![pricing::rule(convert(value)?)?]),
        Entity::PriceRates => RecordBatch::PriceRates(vec![pricing::rate(convert(value)?)?]),
        Entity::RoutingRules => {
            RecordBatch::RoutingRules(vec![rules::routing_record(convert(value)?)?])
        }
        Entity::RuleSets => RecordBatch::RuleSets(vec![rules::rule_set_input(convert(value)?)?]),
        Entity::Rules => RecordBatch::Rules(vec![rules::rule_record(convert(value)?)?]),
        Entity::ProviderRuleSets => {
            let request: ProviderRuleSetWriteRequest = convert(value)?;
            RecordBatch::ProviderRuleSets(vec![ProviderRuleSetInput {
                provider_id: request.provider_id,
                rule_set_id: request.rule_set_id,
                sort_order: request.sort_order,
                enabled: request.enabled,
            }])
        }
        _ => return Err(AdminError::Internal("unsupported import entity".into())),
    })
}

pub(super) fn convert<T: DeserializeOwned>(value: &impl Serialize) -> Result<T, AdminError> {
    serde_json::to_value(value)
        .and_then(serde_json::from_value)
        .map_err(|error| AdminError::BadRequest(error.to_string()))
}

pub(super) async fn insert(state: &impl State, batch: RecordBatch) -> Result<i64, AdminError> {
    // Inputs have been checked before any write. Calling the storage methods
    // directly keeps per-row admin handlers from rebuilding the whole snapshot.
    let store = state.store();
    Ok(match batch {
        RecordBatch::Organizations(values) => store.insert_organization(&values[0]).await?,
        RecordBatch::Teams(values) => store.insert_team(&values[0]).await?,
        RecordBatch::Users(values) => store.insert_user(&values[0]).await?,
        RecordBatch::Providers(values) => store.insert_provider(&values[0]).await?,
        RecordBatch::Routes(values) => store.insert_route(&values[0]).await?,
        RecordBatch::RouteMembers(values) => store.insert_route_member(&values[0]).await?,
        RecordBatch::Aliases(values) => store.insert_alias(&values[0]).await?,
        RecordBatch::ExposedModels(values) => store.insert_exposed_model(&values[0]).await?,
        RecordBatch::Quotas(values) => store.insert_quota(&values[0]).await?,
        RecordBatch::PriceRules(values) => store.insert_price_rule(&values[0]).await?,
        RecordBatch::PriceRates(values) => store.insert_price_rate(&values[0]).await?,
        RecordBatch::RoutingRules(values) => store.insert_routing_rule(&values[0]).await?,
        RecordBatch::RuleSets(values) => store.insert_rule_set(&values[0]).await?,
        RecordBatch::Rules(values) => store.insert_rule(&values[0]).await?,
        RecordBatch::ProviderRuleSets(values) => store.insert_provider_rule_set(&values[0]).await?,
        _ => return Err(AdminError::Internal("unsupported import batch".into())),
    })
}
