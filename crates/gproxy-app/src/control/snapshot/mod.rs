mod authorization;
mod balance;
mod build;
mod capability;
mod health;
mod index;
mod materialize;
mod pressure;
mod pricing;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod reload_tests;
mod resolve;
mod rules;
mod types;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use gproxy_core::{
    ControlPlane, CoreError, CredentialRecord, Plan, Pricing, ProviderRef, RoutingMode,
};
use gproxy_store::records::{
    ControlSnapshot, CredentialQuotaCycleRecord, QuotaBoundarySource, QuotaCycleStatus,
};
use gproxy_store::{Store, StoreError};
use rust_decimal::Decimal;

use types::CredentialHealthMap;
pub(crate) use types::KeyIdentity;
use types::{CompiledSnapshot, CredentialPressure, CredentialPressureMap};

const PROBE_STALE_SECONDS: i64 = 120;

#[derive(Clone)]
pub(crate) struct SnapshotControl {
    store: Store,
    runtime: super::settings::RuntimeOverrides,
    snapshot: Arc<ArcSwap<CompiledSnapshot>>,
    credential_pressure: Arc<ArcSwap<CredentialPressureMap>>,
    credential_health: Arc<ArcSwap<CredentialHealthMap>>,
    /// Serialize database reads through publication, including resets/reloads.
    health_sync: Arc<futures_util::lock::Mutex<()>>,
    health_probes: Arc<Mutex<health::HealthProbes>>,
    rotation: Arc<balance::RotationCounters>,
    oauth_keys: Arc<ArcSwap<std::collections::BTreeSet<i64>>>,
    /// Decrypted credentials, keyed by id. Every path that changes a stored
    /// credential ends in `reload`, which drops the whole map; a rotation
    /// this instance performs forgets its own entry immediately.
    credential_records: Arc<Mutex<CredentialCache>>,
    health_persisted_at: Arc<Mutex<HashMap<(gproxy_channel_api::CredentialId, String), i64>>>,
    /// Last successful probe persist per credential. Response-header snapshots
    /// only hit the store when this is older than [`PROBE_STALE_SECONDS`].
    probe_ok_at: Arc<Mutex<HashMap<i64, (u64, i64)>>>,
    /// Reloads may overlap while waiting on storage. A completed read can
    /// publish unless a newer read has already published its snapshot.
    reload_generation: Arc<Mutex<ReloadGeneration>>,
    #[cfg(all(test, not(target_arch = "wasm32")))]
    reload_pause: Arc<Mutex<Option<reload_tests::Pause>>>,
    #[cfg(all(test, not(target_arch = "wasm32")))]
    credential_read_pause: Arc<Mutex<Option<reload_tests::Pause>>>,
    #[cfg(all(test, not(target_arch = "wasm32")))]
    credential_rotation_pause: Arc<Mutex<Option<reload_tests::Pause>>>,
}

#[derive(Default)]
struct CredentialCache {
    /// A database read may finish after a reload or rotation invalidates it.
    /// The epoch and records share a lock so checking it and publishing a read
    /// cannot race another invalidation.
    epoch: u64,
    records: HashMap<i64, CredentialRecord>,
}

impl CredentialCache {
    fn invalidate(&mut self) {
        self.epoch = self
            .epoch
            .checked_add(1)
            .expect("credential epoch exhausted");
    }
}

#[derive(Default)]
struct ReloadGeneration {
    next: u64,
    published: u64,
}

impl SnapshotControl {
    pub(crate) fn has_named_target(&self, name: &str) -> bool {
        self.snapshot.load().has_named_target(name)
    }

    pub(crate) async fn new(
        store: Store,
        runtime: super::settings::RuntimeOverrides,
    ) -> Result<Self, StoreError> {
        let stored = store.control_snapshot().await?;
        let snapshot = CompiledSnapshot::build(stored, &runtime)?;
        let credential_pressure = load_pressure(&store).await?;
        let credential_health = load_health(&store).await?;
        let oauth_keys = store.oauth_user_key_ids().await?.into_iter().collect();
        Ok(Self {
            store,
            runtime,
            snapshot: Arc::new(ArcSwap::from_pointee(snapshot)),
            credential_pressure: Arc::new(ArcSwap::from_pointee(credential_pressure)),
            credential_health: Arc::new(ArcSwap::from_pointee(credential_health)),
            health_sync: Arc::default(),
            health_probes: Arc::default(),
            rotation: Arc::new(balance::RotationCounters::default()),
            oauth_keys: Arc::new(ArcSwap::from_pointee(oauth_keys)),
            credential_records: Arc::default(),
            health_persisted_at: Arc::default(),
            probe_ok_at: Arc::default(),
            reload_generation: Arc::default(),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            reload_pause: Arc::default(),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            credential_read_pause: Arc::default(),
            #[cfg(all(test, not(target_arch = "wasm32")))]
            credential_rotation_pause: Arc::default(),
        })
    }

    pub(crate) async fn reload(&self) -> Result<(), StoreError> {
        let generation = {
            let mut generation = self.reload_generation.lock().expect("snapshot generation");
            generation.next = generation
                .next
                .checked_add(1)
                .expect("snapshot generation exhausted");
            generation.next
        };
        let stored = self.store.control_snapshot().await?;
        #[cfg(all(test, not(target_arch = "wasm32")))]
        {
            let pause = self.reload_pause.lock().unwrap().take();
            if let Some(pause) = pause {
                let _ = pause.read.send(());
                let _ = pause.resume.await;
            }
        }
        let oauth_keys = self.store.oauth_user_key_ids().await?.into_iter().collect();
        let compiled = Arc::new(CompiledSnapshot::build(stored, &self.runtime)?);
        let _health_sync = self.health_sync.lock().await;
        let health = load_health(&self.store).await?;
        // A second reload can finish while this one is waiting on another
        // backend read. Its generation wins; publishing this older snapshot
        // would roll the routing table and health state backwards.
        let mut current_generation = self.reload_generation.lock().expect("snapshot generation");
        if current_generation.published > generation {
            return Ok(());
        }
        let mut credentials = self.credential_records.lock().expect("credential cache");
        self.oauth_keys.store(Arc::new(oauth_keys));
        self.snapshot.store(compiled);
        self.credential_health.store(Arc::new(health));
        credentials.records.clear();
        credentials.invalidate();
        current_generation.published = generation;
        Ok(())
    }

    pub(crate) fn cached_credential(&self, id: i64) -> Option<CredentialRecord> {
        self.credential_records
            .lock()
            .expect("credential cache")
            .records
            .get(&id)
            .cloned()
    }

    /// The loaded record may be newer than the compiled control snapshot after
    /// OAuth rotation. Read only its version without cloning the decrypted secret.
    pub(crate) fn known_credential_version(&self, id: i64) -> Option<u64> {
        self.credential_records
            .lock()
            .expect("credential cache")
            .records
            .get(&id)
            .map(|record| record.version)
            .or_else(|| self.snapshot.load().credential_versions.get(&id).copied())
    }

    pub(crate) fn credential_for_load(&self, id: i64) -> (Option<CredentialRecord>, u64) {
        let cache = self.credential_records.lock().expect("credential cache");
        (cache.records.get(&id).cloned(), cache.epoch)
    }

    /// Publish a record loaded from storage only when no reload/rotation
    /// invalidated the read while it was in flight. Parallel reads also cannot
    /// replace a credential whose version is newer than their result.
    pub(crate) fn cache_credential_if_current(
        &self,
        record: &CredentialRecord,
        epoch: u64,
    ) -> bool {
        let mut cache = self.credential_records.lock().expect("credential cache");
        if cache.epoch != epoch
            || cache
                .records
                .get(&record.id.0)
                .is_some_and(|cached| cached.version > record.version)
        {
            return false;
        }
        cache.records.insert(record.id.0, record.clone());
        true
    }

    pub(crate) fn forget_credential(&self, id: i64) {
        let mut cache = self.credential_records.lock().expect("credential cache");
        cache.records.remove(&id);
        cache.invalidate();
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) async fn pause_credential_read(&self) {
        let pause = self.credential_read_pause.lock().unwrap().take();
        if let Some(pause) = pause {
            let _ = pause.read.send(());
            let _ = pause.resume.await;
        }
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) async fn pause_credential_rotation(&self) {
        let pause = self.credential_rotation_pause.lock().unwrap().take();
        if let Some(pause) = pause {
            let _ = pause.read.send(());
            let _ = pause.resume.await;
        }
    }

    /// Whether an unchanged health observation is worth persisting again:
    /// the row only carries `observed_at`, so refreshing it more than once per
    /// interval buys nothing and costs a commit per request under load.
    pub(crate) fn health_refresh_due(
        &self,
        credential: gproxy_channel_api::CredentialId,
        model: &str,
        now: i64,
    ) -> bool {
        const HEALTH_REFRESH_INTERVAL_SECS: i64 = 30;
        let mut persisted = self.health_persisted_at.lock().expect("health cache");
        let key = (credential, model.to_owned());
        match persisted.get(&key) {
            Some(at) if now - at < HEALTH_REFRESH_INTERVAL_SECS => false,
            _ => {
                persisted.insert(key, now);
                true
            }
        }
    }

    pub(crate) fn apply_live_pressure(
        &self,
        observation: &gproxy_store::records::CredentialQuotaObservation,
    ) {
        let Some(used_percent) = observation.used_percent.or_else(|| {
            let used = observation.upstream_used?;
            let limit = observation.upstream_limit?;
            (limit > Decimal::ZERO).then(|| used / limit * Decimal::ONE_HUNDRED)
        }) else {
            return;
        };
        let credential = gproxy_channel_api::CredentialId(observation.credential_id);
        let window_key = observation.window_key.clone();
        let last_observed_at = observation.observed_at;
        let period_end = (observation.boundary_source == QuotaBoundarySource::Upstream)
            .then_some(observation.period_end)
            .flatten();
        self.credential_pressure.rcu(|current| {
            let mut updated = (**current).clone();
            let windows = updated.entry(credential).or_default();
            let replace = windows
                .get(&window_key)
                .is_none_or(|stored| stored.last_observed_at <= last_observed_at);
            if replace {
                let (cycle_id, version) = windows
                    .get(&window_key)
                    .map(|stored| (stored.cycle_id, stored.version))
                    .unwrap_or((0, 0));
                windows.insert(
                    window_key.clone(),
                    CredentialPressure {
                        cycle_id,
                        version,
                        last_observed_at,
                        used_percent,
                        period_end,
                    },
                );
            }
            Arc::new(updated)
        });
    }

    pub(crate) fn note_probe_ok(&self, credential_id: i64, version: u64, observed_at: i64) {
        self.probe_ok_at
            .lock()
            .expect("probe cache")
            .insert(credential_id, (version, observed_at));
    }

    pub(crate) fn probe_persist_due(&self, credential_id: i64, version: u64, now: i64) -> bool {
        self.probe_ok_at
            .lock()
            .expect("probe cache")
            .get(&credential_id)
            .is_none_or(|(saved_version, at)| {
                *saved_version != version || now.saturating_sub(*at) >= PROBE_STALE_SECONDS
            })
    }

    pub(crate) async fn observe_credential_quota_cycle(
        &self,
        observation: &gproxy_store::records::CredentialQuotaObservation,
    ) -> Result<gproxy_store::records::CredentialQuotaCycleRecord, StoreError> {
        if !self
            .snapshot
            .load()
            .stored
            .credentials
            .iter()
            .any(|credential| credential.id == observation.credential_id)
        {
            return Err(StoreError::InvalidData {
                field: "credential_id",
                message: format!(
                    "credential {} is absent from the control snapshot",
                    observation.credential_id
                ),
            });
        }
        let cycle = self
            .store
            .observe_credential_quota_cycle(observation)
            .await?;
        self.update_pressure(&cycle);
        Ok(cycle)
    }

    pub(crate) async fn observe_credential_quota_cycle_for_version(
        &self,
        observation: &gproxy_store::records::CredentialQuotaObservation,
        expected_version: u64,
    ) -> Result<Option<gproxy_store::records::CredentialQuotaCycleRecord>, StoreError> {
        let cycle = self
            .store
            .observe_credential_quota_cycle_for_version(observation, expected_version)
            .await?;
        if let Some(cycle) = &cycle
            && self.known_credential_version(observation.credential_id) == Some(expected_version)
        {
            self.update_pressure(cycle);
        }
        Ok(cycle)
    }

    pub(crate) async fn close_credential_quota_cycle(
        &self,
        id: i64,
        reason: gproxy_store::records::QuotaCycleCloseReason,
        closed_at: i64,
    ) -> Result<Option<CredentialQuotaCycleRecord>, StoreError> {
        let cycle = self
            .store
            .close_credential_quota_cycle(id, reason, closed_at)
            .await?;
        if let Some(cycle) = cycle.as_ref() {
            self.update_pressure(cycle);
        }
        Ok(cycle)
    }

    pub(crate) fn current(&self) -> Arc<ControlSnapshot> {
        self.snapshot.load().stored.clone()
    }

    pub(crate) fn runtime_settings(&self) -> Arc<gproxy_admin::dto::RuntimeSettingsStatusDto> {
        self.snapshot.load().settings.runtime.clone()
    }

    pub(crate) fn instance_name(&self) -> String {
        self.snapshot.load().settings.instance_name.clone()
    }

    pub(crate) fn runtime_settings_status(
        &self,
        configured: gproxy_admin::dto::RuntimeSettingsDto,
    ) -> gproxy_admin::dto::RuntimeSettingsStatusDto {
        self.runtime.status(configured)
    }

    pub(crate) fn settings(&self) -> super::settings::EffectiveSettings {
        self.snapshot.load().settings.clone()
    }

    pub(crate) fn provider(&self, id: i64) -> Option<ProviderRef> {
        self.snapshot.load().providers.get(&id).cloned()
    }

    pub(crate) fn key_identity(&self, version: u32, digest: &[u8]) -> Option<KeyIdentity> {
        let digest: [u8; 32] = digest.try_into().ok()?;
        self.snapshot
            .load()
            .identities
            .get(&(version, digest))
            .cloned()
            .filter(|identity| !self.is_oauth_key(identity.caller.user_key_id))
    }

    pub(crate) fn is_oauth_key(&self, id: i64) -> bool {
        self.oauth_keys.load().contains(&id)
    }

    fn update_pressure(&self, cycle: &CredentialQuotaCycleRecord) {
        let credential = gproxy_channel_api::CredentialId(cycle.credential_id);
        let window_key = cycle.window_key.clone();
        let next = cycle_pressure(cycle);
        self.credential_pressure.rcu(|current| {
            let mut updated = (**current).clone();
            let windows = updated.entry(credential).or_default();
            let replace = windows.get(&window_key).is_none_or(|stored| {
                (stored.last_observed_at, stored.cycle_id, stored.version)
                    <= (cycle.last_observed_at, cycle.id, cycle.version)
            });
            if replace {
                match next.clone() {
                    Some(next) => {
                        windows.insert(window_key.clone(), next);
                    }
                    None => {
                        windows.remove(&window_key);
                    }
                }
            }
            if windows.is_empty() {
                updated.remove(&credential);
            }
            Arc::new(updated)
        });
    }
}

impl ControlPlane for SnapshotControl {
    fn resolve_alias(&self, model: &str, mode: &RoutingMode) -> String {
        self.snapshot.load().resolve_alias(model, mode)
    }

    fn resolve_variant(&self, model: &str, mode: &RoutingMode) -> Option<String> {
        self.snapshot.load().resolve_variant(model, mode)
    }

    fn resolve_preprocessed(
        &self,
        model: Option<&str>,
        mode: &RoutingMode,
        affinity: Option<i64>,
    ) -> Result<Plan, CoreError> {
        let mut plan = self.snapshot.load().resolve_preprocessed(
            model,
            mode,
            affinity,
            &self.credential_health.load(),
            &self.rotation,
        )?;
        let now = unix_now();
        pressure::apply(&mut plan, &self.credential_pressure.load(), now);
        self.prioritize_health_probe(&mut plan, now);
        Ok(plan)
    }

    fn pricing(&self, provider: &ProviderRef, upstream_model: &str) -> Option<Pricing> {
        pricing::resolve(&self.snapshot.load().pricing, provider.id, upstream_model)
    }

    fn shared(&self) -> Option<Arc<dyn ControlPlane>> {
        Some(Arc::new(self.clone()))
    }

    fn provider_catalogue(&self) -> Vec<gproxy_core::ExposedModel> {
        self.snapshot.load().provider_catalogue.clone()
    }

    fn catalogue_mode(&self, mode: &RoutingMode) -> RoutingMode {
        let snapshot = self.snapshot.load();
        if let RoutingMode::Named { name } = mode
            && !snapshot.namespaces.contains_key(&name.to_ascii_lowercase())
            && !snapshot.route_names.contains_key(name)
            && snapshot.provider_names.contains_key(name)
        {
            return RoutingMode::Scoped {
                provider: name.clone(),
            };
        }
        mode.clone()
    }

    fn exposed_models(&self) -> Vec<gproxy_core::ExposedModel> {
        self.snapshot
            .load()
            .model_catalogue
            .values()
            .cloned()
            .collect()
    }

    fn catalogue_visible(
        &self,
        identity: &gproxy_channel_api::CallerIdentity,
        model: Option<&str>,
        mode: &RoutingMode,
    ) -> bool {
        let Ok(plan) = self.catalogue_plan(model, mode, Some(identity.user_key_id)) else {
            return false;
        };
        let snapshot = self.current();
        let oauth = self.is_oauth_key(identity.user_key_id);
        let model = self.authorization_model(model, mode);
        plan.targets.iter().any(|target| {
            crate::host::catalogue_permitted(
                &snapshot,
                identity,
                target.provider.id,
                oauth,
                model.as_deref(),
            )
        })
    }

    fn detached(&self) -> Box<dyn ControlPlane> {
        Box::new(self.clone())
    }
}

async fn load_health(store: &Store) -> Result<CredentialHealthMap, StoreError> {
    let mut health = CredentialHealthMap::new();
    for record in store.credential_health().await? {
        health
            .entry(gproxy_channel_api::CredentialId(record.credential_id))
            .or_default()
            .insert(record.model.clone(), record);
    }
    Ok(health)
}

async fn load_pressure(store: &Store) -> Result<CredentialPressureMap, StoreError> {
    let mut by_credential = CredentialPressureMap::new();
    for pressure in store.credential_quota_pressures(unix_now()).await? {
        by_credential
            .entry(gproxy_channel_api::CredentialId(pressure.credential_id))
            .or_default()
            .insert(
                pressure.window_key,
                CredentialPressure {
                    cycle_id: pressure.cycle_id,
                    version: pressure.version,
                    last_observed_at: pressure.last_observed_at,
                    used_percent: pressure.used_percent,
                    period_end: pressure.period_end,
                },
            );
    }
    Ok(by_credential)
}

fn cycle_pressure(cycle: &CredentialQuotaCycleRecord) -> Option<CredentialPressure> {
    if cycle.status != QuotaCycleStatus::Open {
        return None;
    }
    let used_percent = cycle.used_percent.or_else(|| {
        let used = cycle.upstream_used?;
        let limit = cycle.upstream_limit?;
        (limit > Decimal::ZERO).then(|| used / limit * Decimal::ONE_HUNDRED)
    })?;
    Some(CredentialPressure {
        cycle_id: cycle.id,
        version: cycle.version,
        last_observed_at: cycle.last_observed_at,
        used_percent,
        period_end: (cycle.boundary_source == QuotaBoundarySource::Upstream)
            .then_some(cycle.period_end)
            .flatten(),
    })
}

fn unix_now() -> i64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .expect("system clock is before unix epoch")
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX)
}
