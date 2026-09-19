use std::collections::HashMap;
use std::sync::Arc;

use gproxy_channel_api::CredentialId;
use gproxy_core::Plan;
use gproxy_store::records::{CredentialHealthRecord, CredentialHealthState};

use super::{SnapshotControl, unix_now};

#[derive(Default)]
pub(super) struct HealthProbes {
    slots: HashMap<(CredentialId, String, u64), ProbeSlot>,
}

struct ProbeSlot {
    active: bool,
    next_at: i64,
}

impl SnapshotControl {
    pub(crate) fn credential_health_state(
        &self,
        credential: CredentialId,
        model: &str,
    ) -> Option<(u64, CredentialHealthState)> {
        self.credential_health
            .load()
            .get(&credential)?
            .get(model)
            .map(|record| (record.credential_version, record.state))
    }

    pub(crate) fn credential_health_observation(
        &self,
        credential: CredentialId,
        model: &str,
    ) -> Option<CredentialHealthRecord> {
        self.credential_health
            .load()
            .get(&credential)?
            .get(model)
            .cloned()
    }

    pub(crate) async fn refresh_credential_health(
        &self,
        credential: CredentialId,
        model: &str,
    ) -> Result<(), gproxy_store::StoreError> {
        let _sync = self.health_sync.lock().await;
        let previous = self.credential_health_observation(credential, model);
        match self
            .store
            .credential_model_health(credential.0, model)
            .await?
        {
            Some(record) => self.observe_stored_credential_health(&record),
            None => {
                if let Some(previous) = previous {
                    self.clear_cached_credential_health(
                        credential,
                        model,
                        previous.credential_version,
                        previous.version,
                    );
                }
            }
        }
        Ok(())
    }

    /// Publish the row accepted by storage, including its atomic failure count.
    /// Parallel or replayed observations cannot roll the routing state back.
    pub(crate) fn observe_stored_credential_health(&self, record: &CredentialHealthRecord) {
        let credential = CredentialId(record.credential_id);
        self.credential_health.rcu(|current| {
            if current
                .get(&credential)
                .and_then(|models| models.get(&record.model))
                .is_some_and(|stored| {
                    (stored.credential_version, stored.version)
                        >= (record.credential_version, record.version)
                })
            {
                return current.clone();
            }
            let mut updated = (**current).clone();
            updated
                .entry(credential)
                .or_default()
                .insert(record.model.clone(), record.clone());
            Arc::new(updated)
        });
    }

    pub(crate) fn clear_cached_credential_health(
        &self,
        credential: CredentialId,
        model: &str,
        expected_credential_version: u64,
        expected_observation_version: i64,
    ) {
        self.credential_health.rcu(|current| {
            if current
                .get(&credential)
                .and_then(|models| models.get(model))
                .is_none_or(|stored| {
                    (stored.credential_version, stored.version)
                        != (expected_credential_version, expected_observation_version)
                })
            {
                return current.clone();
            }
            let mut updated = (**current).clone();
            let models = updated.get_mut(&credential).expect("observed credential");
            models.remove(model);
            if models.is_empty() {
                updated.remove(&credential);
            }
            Arc::new(updated)
        });
    }

    pub(crate) fn health_retry_after(record: &CredentialHealthRecord, now: i64) -> u32 {
        let exponent = record.consecutive_failures.saturating_sub(1).min(4);
        let delay = (30_u32 << exponent).min(300);
        record
            .observed_at
            .saturating_add(i64::from(delay))
            .saturating_sub(now)
            .clamp(0, i64::from(u32::MAX)) as u32
    }

    pub(crate) fn health_probe_ready(
        &self,
        credential: CredentialId,
        scope: &str,
        credential_version: u64,
        now: i64,
    ) -> bool {
        self.health_probes
            .lock()
            .expect("health probes")
            .slots
            .get(&(credential, scope.to_owned(), credential_version))
            .is_none_or(|slot| !slot.active && slot.next_at <= now)
    }

    pub(crate) fn try_begin_health_probe(
        &self,
        credential: CredentialId,
        scope: &str,
        credential_version: u64,
    ) -> bool {
        let now = unix_now();
        let mut probes = self.health_probes.lock().expect("health probes");
        // Expired inactive slots need no permanent per-model bookkeeping.
        probes
            .slots
            .retain(|_, slot| slot.active || slot.next_at > now);
        let slot = probes
            .slots
            .entry((credential, scope.to_owned(), credential_version))
            .or_insert(ProbeSlot {
                active: false,
                next_at: 0,
            });
        if slot.active || slot.next_at > now {
            return false;
        }
        slot.active = true;
        true
    }

    pub(crate) fn finish_health_probe(
        &self,
        credential: CredentialId,
        scope: &str,
        credential_version: u64,
    ) {
        let mut probes = self.health_probes.lock().expect("health probes");
        if let Some(slot) =
            probes
                .slots
                .get_mut(&(credential, scope.to_owned(), credential_version))
        {
            slot.active = false;
            // A cancelled or incomplete response supplies no health evidence.
            // Delay the next real request without pretending that it succeeded.
            slot.next_at = unix_now().saturating_add(30);
        }
    }

    /// Give an expired degraded model one real attempt even while another
    /// account is healthy. Acquisition happens later, immediately before the
    /// upstream request, so merely resolving a plan never occupies a probe.
    pub(crate) fn prioritize_health_probe(&self, plan: &mut Plan, now: i64) {
        let Some(primary) = plan.targets.first() else {
            return;
        };
        let primary_tier = primary.tier;
        let pressure = self.credential_pressure.load();
        let primary_pressure = super::pressure::tier(pressure.get(&primary.credential), now);
        let health = self.credential_health.load();
        let index = plan
            .targets
            .iter()
            .enumerate()
            .filter(|(_, target)| {
                target.tier == primary_tier
                    && super::pressure::tier(pressure.get(&target.credential), now)
                        <= primary_pressure
            })
            .filter_map(|(index, target)| {
                let models = health.get(&target.credential)?;
                if !models
                    .values()
                    .any(|record| record.state == CredentialHealthState::Degraded)
                {
                    return None;
                }
                let version = self.known_credential_version(target.credential.0)?;
                let mut scope = None;
                let mut due_at = i64::MIN;
                for model in ["*", target.upstream_model.as_str()] {
                    let Some(record) = models
                        .get(model)
                        .filter(|record| record.credential_version == version)
                    else {
                        continue;
                    };
                    match record.state {
                        CredentialHealthState::Dead => return None,
                        CredentialHealthState::Healthy => {}
                        CredentialHealthState::Degraded => {
                            if Self::health_retry_after(record, now) > 0 {
                                return None;
                            }
                            // Account scope is inspected first and owns a shared
                            // probe if both it and the concrete model degraded.
                            scope.get_or_insert(model);
                            due_at = due_at.max(record.observed_at);
                        }
                    }
                }
                let scope = scope?;
                self.health_probe_ready(target.credential, scope, version, now)
                    .then_some((index, due_at))
            })
            .min_by_key(|(_, due_at)| *due_at)
            .map(|(index, _)| index);
        if let Some(index) = index {
            let mut target = plan.targets.remove(index);
            target.rules.session_affinity = false;
            plan.targets.insert(0, target);
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
