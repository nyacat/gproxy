mod activity;
mod admission;
mod bindings;
mod continuations;
mod credentials;
mod health;
mod health_probe;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod health_tests;
pub(crate) mod oauth;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod quota_observe;
pub(crate) mod settlement_recovery;
mod settlement_retry;
mod sinks;
#[cfg(not(target_arch = "wasm32"))]
mod tasks;
mod token_counts;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod tokenizers;
mod usage_view;

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
use std::time::Duration;

use gproxy_channel_api::{BindingStore, BoxFuture, CallerIdentity, UsageView};
use gproxy_core::{CredentialHealth, Host, Plan, ProviderRef, RequestCtx, Spawner};

use crate::Shared;
use crate::cache::AppCache;
use crate::control::SnapshotControl;

use crate::secrets::EnvelopeCipher;
#[cfg(test)]
pub(crate) use admission::authenticate_headers;
#[cfg(test)]
pub(crate) use admission::authorize;
pub(crate) use admission::{catalogue_permitted, provider_permitted};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use tasks::TokioSpawner;

pub(crate) struct Services {
    pub store: gproxy_store::Store,
    pub cache: AppCache,
    pub cipher: EnvelopeCipher,
    pub control: SnapshotControl,
    pub transport: gproxy_upstream::Transport,
    pub health_sequence: std::sync::atomic::AtomicU64,
    #[cfg(not(target_arch = "wasm32"))]
    pub tokenizers: Arc<gproxy_tokenize::TokenizerRegistry>,
    #[cfg(not(target_arch = "wasm32"))]
    pub spawner: TokioSpawner,
    #[cfg(not(target_arch = "wasm32"))]
    pub continuations: continuations::LocalContinuations,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) quota_observe: quota_observe::QuotaObserveQueue,
    pub(crate) token_counts: token_counts::TokenCountCache,
    pub(crate) settlement_recovery: settlement_recovery::RecoveryState,
}

#[derive(Clone)]
pub(crate) struct AppHost {
    pub services: Shared<Services>,
}

impl Host for AppHost {
    type Credentials = Self;
    type Cache = AppCache;
    type Transport = gproxy_upstream::Transport;
    type Usage = Self;
    type Capture = Self;

    fn begin_credential_usage<'a>(
        &'a self,
        request_id: &'a str,
        target: &'a gproxy_core::Target,
        started_at_ms: i64,
    ) -> BoxFuture<'a, Result<(), gproxy_core::CoreError>> {
        Box::pin(async move {
            self.services
                .store
                .begin_credential_usage(
                    request_id,
                    target.credential.0,
                    &target.upstream_model,
                    started_at_ms,
                )
                .await
                .map_err(|error| gproxy_core::CoreError::Internal(error.to_string()))
        })
    }

    fn track_credential_usage<'a>(
        &'a self,
        parent: &'a str,
        request: &'a str,
        target: &'a gproxy_core::Target,
        started: i64,
    ) -> BoxFuture<
        'a,
        Result<Option<gproxy_core::host::CredentialUsageLease>, gproxy_core::CoreError>,
    > {
        Box::pin(activity::begin(self, parent, request, target, started))
    }

    fn begin_credential_health_attempt<'a>(
        &'a self,
        request_id: &'a str,
        target: &'a gproxy_core::Target,
        credential_version: u64,
    ) -> BoxFuture<
        'a,
        Result<Option<gproxy_core::host::CredentialHealthLease>, gproxy_core::CoreError>,
    > {
        Box::pin(health_probe::begin(
            self,
            request_id,
            target,
            credential_version,
        ))
    }

    fn credentials(&self) -> &Self::Credentials {
        self
    }

    fn cache(&self) -> &Self::Cache {
        &self.services.cache
    }

    fn transport(&self) -> &Self::Transport {
        &self.services.transport
    }

    fn usage(&self) -> &Self::Usage {
        self
    }

    fn capture(&self) -> &Self::Capture {
        self
    }

    fn authenticate<'a>(
        &'a self,
        request: &'a RequestCtx,
    ) -> BoxFuture<'a, Result<CallerIdentity, gproxy_core::CoreError>> {
        admission::authenticate(self, request)
    }

    fn admit<'a>(
        &'a self,
        identity: &'a CallerIdentity,
        request: &'a RequestCtx,
        operation: Option<gproxy_protocol::OperationKey>,
        model: Option<&'a str>,
        plan: &'a Plan,
    ) -> BoxFuture<'a, Result<Plan, gproxy_core::CoreError>> {
        admission::admit(self, identity, request, operation, model, plan)
    }

    fn finish_admission<'a>(
        &'a self,
        request_id: &'a str,
        settlement: Option<&'a gproxy_core::Settlement>,
    ) -> BoxFuture<'a, ()> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let host = self.clone();
            let request_id = request_id.to_owned();
            let settlement = settlement.cloned();
            Box::pin(async move {
                let task_host = host.clone();
                let task = host.services.spawner.spawn_tracked(async move {
                    sinks::finish_settlement(&task_host, &request_id, settlement.as_ref()).await;
                });
                if let Err(error) = task.await {
                    tracing::error!(error = %error, "admission settlement task failed");
                }
            })
        }
        #[cfg(target_arch = "wasm32")]
        Box::pin(sinks::finish_settlement(self, request_id, settlement))
    }

    fn admit_credential<'a>(
        &'a self,
        request_id: &'a str,
        target: &'a gproxy_core::Target,
        body: &'a bytes::Bytes,
        settle: gproxy_protocol::SettleMode,
    ) -> BoxFuture<'a, Result<(), gproxy_core::CoreError>> {
        admission::admit_credential(self, request_id, target, body, settle)
    }

    fn admit_retry<'a>(
        &'a self,
        request_id: &'a str,
        target: &'a gproxy_core::Target,
        body: &'a bytes::Bytes,
        settle: gproxy_protocol::SettleMode,
    ) -> BoxFuture<'a, Result<(), gproxy_core::CoreError>> {
        Box::pin(admission::retry::admit(
            self, request_id, target, body, settle,
        ))
    }

    fn count_tokens<'a>(
        &'a self,
        model: &'a str,
        body: &'a bytes::Bytes,
        tokenizer_map: Option<&'a serde_json::Value>,
    ) -> BoxFuture<'a, Result<u64, gproxy_core::CoreError>> {
        let model = model.to_owned();
        let body = body.clone();
        let tokenizer_map = tokenizer_map.cloned();
        #[cfg(not(target_arch = "wasm32"))]
        {
            let registry = self.services.tokenizers.clone();
            Box::pin(async move {
                tokio::task::spawn_blocking(move || {
                    gproxy_tokenize::count(&model, &body, tokenizer_map.as_ref(), &registry)
                })
                .await
                .map_err(|error| {
                    gproxy_core::CoreError::Internal(format!("tokenizer task failed: {error}"))
                })
            })
        }
        #[cfg(target_arch = "wasm32")]
        {
            Box::pin(async move {
                Ok(gproxy_tokenize::count(
                    &model,
                    &body,
                    tokenizer_map.as_ref(),
                    (),
                ))
            })
        }
    }

    fn record_credential_health<'a>(
        &'a self,
        credential: gproxy_channel_api::CredentialId,
        model: &'a str,
        credential_version: u64,
        health: CredentialHealth,
        response_status: Option<http::StatusCode>,
        detail: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let state = match health {
                CredentialHealth::Healthy => gproxy_store::records::CredentialHealthState::Healthy,
                CredentialHealth::Degraded => {
                    gproxy_store::records::CredentialHealthState::Degraded
                }
                CredentialHealth::Dead => gproxy_store::records::CredentialHealthState::Dead,
            };
            let input = gproxy_store::records::CredentialHealthInput {
                credential_id: credential.0,
                model: model.to_owned(),
                credential_version,
                version: match health::observation_version(&self.services.health_sequence) {
                    Some(version) => version,
                    None => return,
                },
                state,
                observed_at: admission::unix_now(),
                response_status: response_status.map(|status| status.as_u16()),
                detail: Some(detail.into()),
            };
            health::record(self, input).await;
        })
    }

    fn record_credential_health_success<'a>(
        &'a self,
        credential: gproxy_channel_api::CredentialId,
        model: &'a str,
        credential_version: u64,
        started_at_ms: i64,
        response_status: Option<http::StatusCode>,
        detail: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let Some(version) = health::success_version(started_at_ms) else {
                return;
            };
            health::record(
                self,
                gproxy_store::records::CredentialHealthInput {
                    credential_id: credential.0,
                    model: model.to_owned(),
                    credential_version,
                    version,
                    state: gproxy_store::records::CredentialHealthState::Healthy,
                    observed_at: admission::unix_now(),
                    response_status: response_status.map(|status| status.as_u16()),
                    detail: Some(detail.into()),
                },
            )
            .await;
        })
    }

    fn observe_credential_quota_entries<'a>(
        &'a self,
        credential: gproxy_channel_api::CredentialId,
        credential_version: u64,
        entries: Vec<gproxy_channel_api::QuotaEntry>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            #[cfg(not(target_arch = "wasm32"))]
            if self.spawner().is_some() {
                quota_observe::enqueue_entries(self, credential.0, credential_version, entries);
                return;
            }
            if let Err(error) = self
                .services
                .store
                .observe_credential_quota_entries(credential.0, credential_version, &entries)
                .await
            {
                tracing::warn!(credential_id = credential.0, error = %error, "quota response snapshot failed");
            }
        })
    }

    fn observe_credential_quota<'a>(
        &'a self,
        credential: gproxy_channel_api::CredentialId,
        credential_version: u64,
        observations: Vec<gproxy_channel_api::QuotaObservation>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let observed_at = admission::unix_now();
            for value in observations {
                // The channel reported only wire facts: an upstream-declared
                // period end is an upstream boundary; a start computed from
                // end minus window length is derived, not exact.
                let (source, confidence) = match (value.period_start, value.period_end) {
                    (Some(_), Some(_)) => (
                        gproxy_store::records::QuotaBoundarySource::Upstream,
                        gproxy_store::records::QuotaBoundaryConfidence::Derived,
                    ),
                    (None, Some(_)) => (
                        gproxy_store::records::QuotaBoundarySource::Upstream,
                        gproxy_store::records::QuotaBoundaryConfidence::Partial,
                    ),
                    _ => (
                        gproxy_store::records::QuotaBoundarySource::Unknown,
                        gproxy_store::records::QuotaBoundaryConfidence::Unknown,
                    ),
                };
                let sample = value
                    .sample
                    .expect("upstream quota carries its sampling interval");
                let observation = gproxy_store::records::CredentialQuotaObservation {
                    unit: value.unit,
                    reset_behavior: value.reset_behavior,
                    scope: value.scope,
                    sample,
                    credential_id: credential.0,
                    window_key: value.window_key,
                    label: value.label,
                    period_start: value.period_start,
                    period_end: value.period_end,
                    boundary_source: source,
                    boundary_confidence: confidence,
                    observed_at: sample.received_at_ms / 1000,
                    upstream_used: value.upstream_used,
                    upstream_limit: value.upstream_limit,
                    used_percent: value.used_percent,
                };
                match sample.source {
                    gproxy_channel_api::QuotaSampleSource::Response => {
                        if self.services.control.known_credential_version(credential.0)
                            == Some(credential_version)
                        {
                            self.services.control.apply_live_pressure(&observation);
                        }
                        if !self.services.control.probe_persist_due(
                            credential.0,
                            credential_version,
                            observed_at,
                        ) {
                            continue;
                        }
                        #[cfg(not(target_arch = "wasm32"))]
                        if self.spawner().is_some() {
                            quota_observe::enqueue(self, credential_version, observation);
                            continue;
                        }
                    }
                    gproxy_channel_api::QuotaSampleSource::Probe => {}
                    gproxy_channel_api::QuotaSampleSource::Unknown => {}
                }
                match self
                    .services
                    .control
                    .observe_credential_quota_cycle_for_version(&observation, credential_version)
                    .await
                {
                    Ok(Some(_)) => {
                        if sample.source == gproxy_channel_api::QuotaSampleSource::Probe {
                            self.services.control.note_probe_ok(
                                credential.0,
                                credential_version,
                                observation.observed_at,
                            );
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(error = %error, "credential quota observation failed");
                    }
                }
            }
        })
    }

    fn wait<'a>(&'a self, duration: Duration) -> BoxFuture<'a, ()> {
        #[cfg(not(target_arch = "wasm32"))]
        return Box::pin(tokio::time::sleep(duration));
        #[cfg(target_arch = "wasm32")]
        Box::pin(gloo_timers::future::TimeoutFuture::new(
            duration.as_millis().min(u128::from(u32::MAX)) as u32,
        ))
    }

    fn surface_usage<'a>(
        &'a self,
        identity: &'a CallerIdentity,
        provider: &'a ProviderRef,
        credential: gproxy_channel_api::CredentialId,
    ) -> Box<dyn UsageView + 'a> {
        Box::new(usage_view::AppUsageView::new(
            self.clone(),
            identity.clone(),
            provider.id,
            credential,
        ))
    }

    fn spawner(&self) -> Option<&dyn Spawner> {
        #[cfg(not(target_arch = "wasm32"))]
        return Some(&self.services.spawner);
        #[cfg(target_arch = "wasm32")]
        None
    }

    fn bindings(&self) -> Option<&dyn BindingStore> {
        Some(self)
    }

    fn oauth(&self) -> Option<&dyn gproxy_channel_api::OAuthService> {
        Some(self)
    }

    fn continuations(&self) -> Option<&dyn gproxy_core::ContinuationStore> {
        #[cfg(not(target_arch = "wasm32"))]
        return Some(&self.services.continuations);
        #[cfg(target_arch = "wasm32")]
        None
    }
}
