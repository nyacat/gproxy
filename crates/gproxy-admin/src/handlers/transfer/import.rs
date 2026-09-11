use bytes::Bytes;
use http::{Response, StatusCode};

use super::import_prepare::prepare;
use super::import_records::convert;
use super::import_support::*;
use crate::dto::*;
use crate::handlers::util;
use crate::route::Entity;
use crate::{AdminError, State, response};

pub(super) async fn run(state: &impl State, body: &Bytes) -> Result<Response<Bytes>, AdminError> {
    let request: ConfigurationImportRequest = util::parse(body)?;
    if request.export.format_version != 1 {
        return Err(AdminError::BadRequest(
            "unsupported export format_version".into(),
        ));
    }
    let included = request.export.secrets == SecretExportDto::Included;
    let source = match (included, request.export.source_key.as_ref()) {
        (true, Some(source)) => Some(source),
        (true, None) => {
            return Err(AdminError::BadRequest(
                "secret-bearing export has no source_key".into(),
            ));
        }
        (false, None) => None,
        (false, Some(_)) => {
            return Err(AdminError::BadRequest(
                "config-only export must not declare a source_key".into(),
            ));
        }
    };
    let mut imported = 0_u64;
    let existing = state.store().control_snapshot().await?;
    let data = request.export.data;
    // Content and secret failures must be found before the first write. The
    // storage API still commits individual inserts: database failures are not
    // covered by a cross-entity transaction.
    let mut prepared = prepare(
        state,
        &data,
        source,
        request.source_master_key.as_deref(),
        &existing,
    )?;
    drop(existing);
    let mut maps = std::mem::take(&mut prepared.reused);
    for value in data.organizations {
        if maps.organizations.contains_key(&value.id) {
            continue;
        }
        map_create(
            state,
            Entity::Organizations,
            value.id,
            &value,
            &mut maps.organizations,
        )
        .await?;
        imported += 1;
    }
    for mut value in data.teams {
        value.organization_id = mapped(&maps.organizations, value.organization_id)?;
        if maps.teams.contains_key(&value.id) {
            continue;
        }
        map_create(state, Entity::Teams, value.id, &value, &mut maps.teams).await?;
        imported += 1;
    }
    for mut value in data.users {
        value.organization_id = optional(&maps.organizations, value.organization_id)?;
        value.team_id = optional(&maps.teams, value.team_id)?;
        if maps.users.contains_key(&value.id) {
            continue;
        }
        map_create(state, Entity::Users, value.id, &value, &mut maps.users).await?;
        imported += 1;
    }
    let channels = state.channel_catalogue();
    let mut provider_defaults = Vec::new();
    for value in data.providers {
        map_create(
            state,
            Entity::Providers,
            value.id,
            &value,
            &mut maps.providers,
        )
        .await?;
        let channel = channels
            .iter()
            .find(|channel| channel.id == value.channel)
            .ok_or_else(|| AdminError::BadRequest("unknown runtime channel".into()))?;
        provider_defaults.push((mapped(&maps.providers, value.id)?, value.name, channel));
        imported += 1;
    }
    let (credential_count, skipped_credentials) = import_credentials(
        state,
        data.credentials,
        &mut prepared.credentials,
        &mut maps,
    )
    .await?;
    imported += credential_count;
    for value in data.routes {
        map_create(state, Entity::Routes, value.id, &value, &mut maps.routes).await?;
        imported += 1;
    }
    for mut value in data.route_members {
        value.route_id = mapped(&maps.routes, value.route_id)?;
        value.provider_id = mapped(&maps.providers, value.provider_id)?;
        create(state, Entity::RouteMembers, &value).await?;
        imported += 1;
    }
    for mut value in data.aliases {
        value.provider_id = optional(&maps.providers, value.provider_id)?;
        create(state, Entity::Aliases, &value).await?;
        imported += 1;
    }
    for mut value in data.model_aliases {
        value.route_id = mapped(&maps.routes, value.route_id)?;
        create(state, Entity::ModelAliases, &value).await?;
        imported += 1;
    }
    let (user_key_count, skipped_user_keys) =
        import_user_keys(state, data.user_keys, &mut prepared.user_keys, &mut maps).await?;
    imported += user_key_count;
    for mut value in data.quotas {
        let Some(id) = subject(&maps, &value.subject_kind, value.subject_id)? else {
            continue;
        };
        value.subject_id = id;
        create(state, Entity::Quotas, &value).await?;
        imported += 1;
    }
    for mut value in data.price_rules {
        value.provider_id = optional(&maps.providers, value.provider_id)?;
        map_create(
            state,
            Entity::PriceRules,
            value.id,
            &value,
            &mut maps.price_rules,
        )
        .await?;
        imported += 1;
    }
    for mut value in data.price_rates {
        value.rule_id = mapped(&maps.price_rules, value.rule_id)?;
        create(state, Entity::PriceRates, &value).await?;
        imported += 1;
    }
    for mut value in data.routing_rules {
        value.provider_id = mapped(&maps.providers, value.provider_id)?;
        if value.inherited {
            let input = crate::handlers::rules::routing_record(convert(&value)?)?;
            state.store().insert_routing_default(&input).await?;
        } else {
            create(state, Entity::RoutingRules, &value).await?;
        }
        imported += 1;
    }
    for mut value in data.rule_sets {
        if let Some(source_provider_id) = provider_default_owner(&value) {
            let provider_id = mapped(&maps.providers, source_provider_id)?;
            value.description = Some(format!("gproxy:provider-default:{provider_id}"));
        }
        map_create(
            state,
            Entity::RuleSets,
            value.id,
            &value,
            &mut maps.rule_sets,
        )
        .await?;
        imported += 1;
    }
    for mut value in data.rules {
        value.rule_set_id = mapped(&maps.rule_sets, value.rule_set_id)?;
        create(state, Entity::Rules, &value).await?;
        imported += 1;
    }
    for mut value in data.provider_rule_sets {
        value.provider_id = mapped(&maps.providers, value.provider_id)?;
        value.rule_set_id = mapped(&maps.rule_sets, value.rule_set_id)?;
        create(state, Entity::ProviderRuleSets, &value).await?;
        imported += 1;
    }
    // Populate defaults after source rules and attachments are present. Default
    // inserts preserve operator overrides and cannot claim their unique keys.
    for (provider_id, name, channel) in provider_defaults {
        crate::seed_provider_defaults(state.store(), provider_id, &name, channel).await?;
    }
    state.reload().await?;
    response::json(
        StatusCode::OK,
        &ConfigurationImportResponse {
            imported,
            skipped_credentials,
            skipped_user_keys,
        },
    )
}

pub(super) fn provider_default_owner(rule_set: &RuleSetDto) -> Option<i64> {
    rule_set
        .description
        .as_deref()?
        .strip_prefix("gproxy:provider-default:")?
        .parse()
        .ok()
}
