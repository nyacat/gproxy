use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use gproxy_channel_api::QuotaEntry;
use gproxy_core::Host;
use gproxy_store::records::CredentialQuotaObservation;

use super::AppHost;

#[derive(Default)]
pub(crate) struct QuotaObserveQueue {
    latest: Mutex<HashMap<(i64, u64), QuotaBatch>>,
    inflight: AtomicBool,
}

#[derive(Default)]
struct QuotaBatch {
    observations: HashMap<String, CredentialQuotaObservation>,
    entries: HashMap<(String, String), QuotaEntry>,
}

impl QuotaObserveQueue {
    fn push(&self, credential_version: u64, observation: CredentialQuotaObservation) {
        let mut pending = self.latest.lock().expect("quota observe queue");
        let batch = pending
            .entry((observation.credential_id, credential_version))
            .or_default();
        if batch
            .observations
            .get(&observation.window_key)
            .is_none_or(|stored| stored.sample.received_at_ms <= observation.sample.received_at_ms)
        {
            batch
                .observations
                .insert(observation.window_key.clone(), observation);
        }
    }

    fn push_entries(&self, credential_id: i64, credential_version: u64, entries: Vec<QuotaEntry>) {
        if entries.is_empty() {
            return;
        }
        let mut pending = self.latest.lock().expect("quota observe queue");
        let batch = pending
            .entry((credential_id, credential_version))
            .or_default();
        for entry in entries {
            let key = (entry.source_id.clone(), entry.id.clone());
            if batch
                .entries
                .get(&key)
                .is_none_or(|stored| stored.observed_at_ms <= entry.observed_at_ms)
            {
                batch.entries.insert(key, entry);
            }
        }
    }

    fn take(&self) -> HashMap<(i64, u64), QuotaBatch> {
        std::mem::take(&mut *self.latest.lock().expect("quota observe queue"))
    }

    fn is_empty(&self) -> bool {
        self.latest.lock().expect("quota observe queue").is_empty()
    }
}

pub(super) fn enqueue(
    host: &AppHost,
    credential_version: u64,
    observation: CredentialQuotaObservation,
) {
    host.services
        .quota_observe
        .push(credential_version, observation);
    schedule(host);
}

pub(super) fn enqueue_entries(
    host: &AppHost,
    credential_id: i64,
    credential_version: u64,
    entries: Vec<QuotaEntry>,
) {
    host.services
        .quota_observe
        .push_entries(credential_id, credential_version, entries);
    schedule(host);
}

fn schedule(host: &AppHost) {
    if host
        .services
        .quota_observe
        .inflight
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    let Some(spawner) = host.spawner() else {
        host.services
            .quota_observe
            .inflight
            .store(false, Ordering::Release);
        return;
    };
    let host = host.clone();
    spawner.spawn(Box::pin(async move {
        drain(host).await;
    }));
}

async fn drain(host: AppHost) {
    loop {
        let pending = host.services.quota_observe.take();
        if pending.is_empty() {
            host.services
                .quota_observe
                .inflight
                .store(false, Ordering::Release);
            if host.services.quota_observe.is_empty() {
                return;
            }
            if host
                .services
                .quota_observe
                .inflight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return;
            }
            continue;
        }
        for ((credential_id, credential_version), batch) in pending {
            persist(&host, credential_id, credential_version, batch).await;
        }
    }
}

async fn persist(host: &AppHost, credential_id: i64, credential_version: u64, batch: QuotaBatch) {
    // Every write enforces the credential version in its own transaction.
    // A rotation between queueing and draining cannot publish an old reading.
    if !batch.entries.is_empty() {
        let entries = batch.entries.into_values().collect::<Vec<_>>();
        if let Err(error) = host
            .services
            .store
            .observe_credential_quota_entries(credential_id, credential_version, &entries)
            .await
        {
            tracing::warn!(credential_id, error = %error, "quota response snapshot failed");
        }
    }
    for observation in batch.observations.into_values() {
        if let Err(error) = host
            .services
            .control
            .observe_credential_quota_cycle_for_version(&observation, credential_version)
            .await
        {
            tracing::warn!(
                error = %error,
                credential_id,
                window = %observation.window_key,
                "credential quota observation failed"
            );
        }
    }
}

#[cfg(test)]
mod tests;
