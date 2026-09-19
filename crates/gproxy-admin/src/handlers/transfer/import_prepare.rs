use std::collections::{BTreeMap, BTreeSet};

use gproxy_store::records::{ControlSnapshot, CredentialInput, RecordBatch, UserKeyInput};

use super::import_records::{convert, record};
use super::import_support::{IdMaps, mapped, optional};
use crate::dto::*;
use crate::route::Entity;
use crate::{AdminError, State};

#[derive(Default)]
pub(super) struct Prepared {
    pub reused: IdMaps,
    pub credentials: BTreeMap<i64, CredentialInput>,
    pub user_keys: BTreeMap<i64, UserKeyInput>,
}

pub(super) fn prepare(
    state: &impl State,
    data: &ConfigurationDataDto,
    source: Option<&ExportSourceKeyDto>,
    source_master_key: Option<&str>,
    existing: &ControlSnapshot,
) -> Result<Prepared, AdminError> {
    let graph = source_graph(data)?;
    validate_records(state, data)?;
    validate_references(data, &graph)?;
    let mut prepared = Prepared::default();
    validate_conflicts(data, existing, &mut prepared.reused)?;
    for value in &data.credentials {
        let config = &value.config;
        crate::handlers::control::validators::credential_settings(&convert(config)?)?;
        let Some(secret) = &value.secret else {
            continue;
        };
        let source = source.ok_or_else(|| {
            AdminError::BadRequest("config-only export contains a credential secret".into())
        })?;
        let secret =
            state.open_imported_credential(&secret.clone().into(), source, source_master_key)?;
        let input = CredentialInput {
            provider_id: config.provider_id,
            label: config
                .label
                .clone()
                .or_else(|| crate::default_credential_label(&config.kind, &secret)),
            kind: config.kind.clone(),
            envelope: state.seal_credential(&secret)?,
            enabled: config.enabled,
            weight: config.weight,
            rpm_limit: config.rpm_limit,
            tpm_limit: config.tpm_limit,
            proxy_url: config.proxy_url.clone(),
            tls_fingerprint: config
                .tls_fingerprint
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|error| AdminError::BadRequest(error.to_string()))?,
        };
        validate(&RecordBatch::Credentials(vec![input.clone()]))?;
        prepared.credentials.insert(config.id, input);
    }
    for value in &data.user_keys {
        let Some(secret) = &value.secret else {
            continue;
        };
        let source = source.ok_or_else(|| {
            AdminError::BadRequest("config-only export contains a user-key secret".into())
        })?;
        let envelope =
            state.reseal_imported_user_key(&secret.clone().into(), source, source_master_key)?;
        let config = &value.config;
        let input = UserKeyInput {
            user_id: config.user_id,
            digest: value.digest.clone(),
            digest_version: value.digest_version,
            prefix: config
                .prefix
                .clone()
                .ok_or_else(|| AdminError::BadRequest("exported user key has no prefix".into()))?,
            envelope,
            label: config.label.clone(),
            expires_at: config.expires_at,
            enabled: config.enabled,
        };
        validate(&RecordBatch::UserKeys(vec![input.clone()]))?;
        prepared.user_keys.insert(config.id, input);
    }
    Ok(prepared)
}

fn validate_records(state: &impl State, data: &ConfigurationDataDto) -> Result<(), AdminError> {
    macro_rules! check {
        ($field:ident, $entity:ident) => {
            for value in &data.$field {
                validate(&record(state, Entity::$entity, value)?)?;
            }
        };
    }
    check!(organizations, Organizations);
    check!(teams, Teams);
    check!(users, Users);
    check!(providers, Providers);
    check!(routes, Routes);
    check!(route_members, RouteMembers);
    check!(aliases, Aliases);
    check!(model_aliases, ModelAliases);
    check!(quotas, Quotas);
    check!(price_rules, PriceRules);
    check!(price_rates, PriceRates);
    check!(routing_rules, RoutingRules);
    check!(rule_sets, RuleSets);
    check!(rules, Rules);
    check!(provider_rule_sets, ProviderRuleSets);

    Ok(())
}

fn source_graph(data: &ConfigurationDataDto) -> Result<IdMaps, AdminError> {
    macro_rules! ids {
        ($field:ident) => {
            unique(data.$field.iter().map(|value| value.id), stringify!($field))?
                .into_iter()
                .map(|id| (id, id))
                .collect()
        };
    }
    let maps = IdMaps {
        organizations: ids!(organizations),
        teams: ids!(teams),
        users: ids!(users),
        providers: ids!(providers),
        credentials: unique(
            data.credentials.iter().map(|value| value.config.id),
            "credentials",
        )?
        .into_iter()
        .map(|id| (id, id))
        .collect(),
        user_keys: unique(
            data.user_keys.iter().map(|value| value.config.id),
            "user keys",
        )?
        .into_iter()
        .map(|id| (id, id))
        .collect(),
        routes: ids!(routes),
        price_rules: ids!(price_rules),
        rule_sets: ids!(rule_sets),
    };
    unique(
        data.route_members.iter().map(|value| value.id),
        "route members",
    )?;
    unique(data.aliases.iter().map(|value| value.id), "aliases")?;
    unique(
        data.model_aliases.iter().map(|value| value.id),
        "model aliases",
    )?;
    unique(data.quotas.iter().map(|value| value.id), "quotas")?;
    unique(data.price_rates.iter().map(|value| value.id), "price rates")?;
    unique(
        data.routing_rules.iter().map(|value| value.id),
        "routing rules",
    )?;
    unique(data.rules.iter().map(|value| value.id), "rules")?;
    unique(
        data.provider_rule_sets.iter().map(|value| value.id),
        "provider rule sets",
    )?;
    Ok(maps)
}

fn validate_references(data: &ConfigurationDataDto, maps: &IdMaps) -> Result<(), AdminError> {
    let teams = data
        .teams
        .iter()
        .map(|team| (team.id, team))
        .collect::<BTreeMap<_, _>>();
    for value in &data.teams {
        mapped(&maps.organizations, value.organization_id)?;
    }
    for value in &data.users {
        optional(&maps.organizations, value.organization_id)?;
        optional(&maps.teams, value.team_id)?;
        if let Some(team_id) = value.team_id
            && value.organization_id != Some(teams[&team_id].organization_id)
        {
            return Err(AdminError::BadRequest(
                "team_id requires its matching organization_id".into(),
            ));
        }
    }
    for value in &data.credentials {
        mapped(&maps.providers, value.config.provider_id)?;
    }
    for value in &data.user_keys {
        mapped(&maps.users, value.config.user_id)?;
    }
    for value in &data.route_members {
        mapped(&maps.providers, value.provider_id)?;
        mapped(&maps.routes, value.route_id)?;
    }
    for value in &data.aliases {
        optional(&maps.providers, value.provider_id)?;
    }
    for value in &data.model_aliases {
        mapped(&maps.routes, value.route_id)?;
    }
    for value in &data.quotas {
        mapped(subject_map(maps, &value.subject_kind)?, value.subject_id)?;
    }
    for value in &data.price_rules {
        optional(&maps.providers, value.provider_id)?;
    }
    for value in &data.price_rates {
        mapped(&maps.price_rules, value.rule_id)?;
    }
    for value in &data.routing_rules {
        mapped(&maps.providers, value.provider_id)?;
    }
    for value in &data.rule_sets {
        if let Some(owner) = super::import::provider_default_owner(value) {
            mapped(&maps.providers, owner)?;
        }
    }
    for value in &data.rules {
        mapped(&maps.rule_sets, value.rule_set_id)?;
    }
    for value in &data.provider_rule_sets {
        mapped(&maps.providers, value.provider_id)?;
        mapped(&maps.rule_sets, value.rule_set_id)?;
    }
    Ok(())
}

fn validate_conflicts(
    data: &ConfigurationDataDto,
    existing: &ControlSnapshot,
    reused: &mut IdMaps,
) -> Result<(), AdminError> {
    unique(
        data.organizations.iter().map(|value| &value.name),
        "organization names",
    )?;
    unique(
        data.teams
            .iter()
            .map(|value| (value.organization_id, &value.name)),
        "team names",
    )?;
    unique(data.users.iter().map(|value| &value.name), "user names")?;
    let organizations = existing
        .organizations
        .iter()
        .map(|value| (&value.name, value.id))
        .collect::<BTreeMap<_, _>>();
    for value in &data.organizations {
        if let Some(id) = organizations.get(&value.name) {
            reused.organizations.insert(value.id, *id);
        }
    }
    let teams = existing
        .teams
        .iter()
        .map(|value| ((value.organization_id, &value.name), value.id))
        .collect::<BTreeMap<_, _>>();
    for value in &data.teams {
        if let Some(organization_id) = reused.organizations.get(&value.organization_id)
            && let Some(id) = teams.get(&(*organization_id, &value.name))
        {
            reused.teams.insert(value.id, *id);
        }
    }
    let users = existing
        .users
        .iter()
        .map(|value| (&value.name, value))
        .collect::<BTreeMap<_, _>>();
    for value in &data.users {
        if let Some(current) = users.get(&value.name) {
            if current.is_admin != value.is_admin {
                return Err(conflict("user name"));
            }
            reused.users.insert(value.id, current.id);
        }
    }
    fresh_names(
        data.providers.iter().map(|value| &value.name),
        existing.providers.iter().map(|value| &value.name),
        "provider names",
    )?;
    fresh_names(
        data.routes.iter().map(|value| &value.name),
        existing.routes.iter().map(|value| &value.name),
        "route names",
    )?;
    fresh_names(
        data.model_aliases.iter().map(|value| &value.name),
        existing.exposed_models.iter().map(|value| &value.name),
        "model alias names",
    )?;
    fresh_names(
        data.rule_sets.iter().map(|value| &value.name),
        existing.rule_sets.iter().map(|value| &value.name),
        "rule set names",
    )?;
    unique(
        data.routing_rules
            .iter()
            .map(|value| (value.provider_id, &value.operation, &value.kind)),
        "routing rule keys",
    )?;
    unique(
        data.provider_rule_sets
            .iter()
            .map(|value| (value.provider_id, value.rule_set_id)),
        "provider rule-set attachments",
    )?;
    unique(
        data.rule_sets
            .iter()
            .filter_map(super::import::provider_default_owner),
        "provider default rule-set owners",
    )?;
    unique(
        data.user_keys.iter().map(|value| &value.digest),
        "user key digests",
    )?;
    let keys = existing
        .user_keys
        .iter()
        .map(|value| (&value.digest, value))
        .collect::<BTreeMap<_, _>>();
    for value in &data.user_keys {
        if let Some(current) = keys.get(&value.digest) {
            if reused.users.get(&value.config.user_id) != Some(&current.user_id)
                || current.digest_version != value.digest_version
            {
                return Err(conflict("user key digest"));
            }
            reused.user_keys.insert(value.config.id, current.id);
        }
    }
    unique(
        data.quotas
            .iter()
            .map(|value| (&value.subject_kind, value.subject_id)),
        "quota subjects",
    )?;
    let quotas = existing
        .quotas
        .iter()
        .map(|value| (&value.subject_kind, value.subject_id))
        .collect::<BTreeSet<_>>();
    for value in &data.quotas {
        if let Some(subject_id) = subject_map(reused, &value.subject_kind)?.get(&value.subject_id)
            && quotas.contains(&(&value.subject_kind, *subject_id))
        {
            return Err(conflict("quota subject"));
        }
    }
    Ok(())
}

fn subject_map<'a>(maps: &'a IdMaps, kind: &str) -> Result<&'a BTreeMap<i64, i64>, AdminError> {
    match kind {
        "credential" => Ok(&maps.credentials),
        "organization" => Ok(&maps.organizations),
        "team" => Ok(&maps.teams),
        "user" => Ok(&maps.users),
        "user_key" => Ok(&maps.user_keys),
        _ => Err(AdminError::BadRequest(
            "export contains an unknown quota subject kind".into(),
        )),
    }
}

fn validate(batch: &RecordBatch) -> Result<(), AdminError> {
    batch
        .validate()
        .map_err(|error| AdminError::BadRequest(error.to_string()))
}

fn unique<T: Ord>(
    values: impl IntoIterator<Item = T>,
    field: &str,
) -> Result<BTreeSet<T>, AdminError> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(AdminError::BadRequest(format!(
                "export contains duplicate {field}"
            )));
        }
    }
    Ok(seen)
}

fn fresh_names<'a>(
    incoming: impl Iterator<Item = &'a String>,
    existing: impl Iterator<Item = &'a String>,
    field: &str,
) -> Result<(), AdminError> {
    let incoming = unique(incoming, field)?;
    let existing = existing.collect::<BTreeSet<_>>();
    if !incoming.is_disjoint(&existing) {
        return Err(conflict(field));
    }
    Ok(())
}

fn conflict(field: &str) -> AdminError {
    AdminError::BadRequest(format!("export conflicts with an existing {field}"))
}
