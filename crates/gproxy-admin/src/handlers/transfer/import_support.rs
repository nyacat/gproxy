use std::collections::BTreeMap;

use gproxy_store::records::{CredentialInput, UserKeyInput};
use serde::Serialize;

use crate::dto::*;
use crate::route::Entity;
use crate::{AdminError, State};

#[derive(Default)]
pub(super) struct IdMaps {
    pub organizations: BTreeMap<i64, i64>,
    pub teams: BTreeMap<i64, i64>,
    pub users: BTreeMap<i64, i64>,
    pub providers: BTreeMap<i64, i64>,
    pub credentials: BTreeMap<i64, i64>,
    pub user_keys: BTreeMap<i64, i64>,
    pub routes: BTreeMap<i64, i64>,
    pub price_rules: BTreeMap<i64, i64>,
    pub rule_sets: BTreeMap<i64, i64>,
}

pub(super) async fn import_credentials(
    state: &impl State,
    values: Vec<ExportCredentialDto>,
    prepared: &mut BTreeMap<i64, CredentialInput>,
    maps: &mut IdMaps,
) -> Result<(u64, u64), AdminError> {
    let mut imported = 0;
    let mut skipped = 0;
    for value in values {
        let Some(mut input) = prepared.remove(&value.config.id) else {
            skipped += 1;
            continue;
        };
        input.provider_id = mapped(&maps.providers, input.provider_id)?;
        let id = state.store().insert_credential(&input).await?;
        maps.credentials.insert(value.config.id, id);
        imported += 1;
    }
    Ok((imported, skipped))
}

pub(super) async fn import_user_keys(
    state: &impl State,
    values: Vec<ExportUserKeyDto>,
    prepared: &mut BTreeMap<i64, UserKeyInput>,
    maps: &mut IdMaps,
) -> Result<(u64, u64), AdminError> {
    let mut imported = 0;
    let mut skipped = 0;
    for value in values {
        if maps.user_keys.contains_key(&value.config.id) {
            continue;
        }
        let Some(mut input) = prepared.remove(&value.config.id) else {
            skipped += 1;
            continue;
        };
        input.user_id = mapped(&maps.users, input.user_id)?;
        let id = state.store().insert_user_key(&input).await?;
        maps.user_keys.insert(value.config.id, id);
        imported += 1;
    }
    Ok((imported, skipped))
}

pub(super) async fn create(
    state: &impl State,
    entity: Entity,
    value: &impl Serialize,
) -> Result<i64, AdminError> {
    super::import_records::insert(state, super::import_records::record(state, entity, value)?).await
}

pub(super) async fn map_create(
    state: &impl State,
    entity: Entity,
    old: i64,
    value: &impl Serialize,
    map: &mut BTreeMap<i64, i64>,
) -> Result<(), AdminError> {
    map.insert(old, create(state, entity, value).await?);
    Ok(())
}

pub(super) fn mapped(map: &BTreeMap<i64, i64>, id: i64) -> Result<i64, AdminError> {
    map.get(&id)
        .copied()
        .ok_or_else(|| AdminError::BadRequest(format!("export references missing id {id}")))
}

pub(super) fn optional(
    map: &BTreeMap<i64, i64>,
    id: Option<i64>,
) -> Result<Option<i64>, AdminError> {
    id.map(|id| mapped(map, id)).transpose()
}

pub(super) fn subject(maps: &IdMaps, kind: &str, id: i64) -> Result<Option<i64>, AdminError> {
    match kind {
        "credential" => Ok(maps.credentials.get(&id).copied()),
        "organization" => mapped(&maps.organizations, id).map(Some),
        "team" => mapped(&maps.teams, id).map(Some),
        "user" => mapped(&maps.users, id).map(Some),
        "user_key" => Ok(maps.user_keys.get(&id).copied()),
        _ => Err(AdminError::BadRequest(
            "export contains an unknown quota subject kind".into(),
        )),
    }
}
