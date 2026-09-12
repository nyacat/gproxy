use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use gproxy_channel_api::{
    Binding, BindingStore, BoxFuture, CallerIdentity, QuotaWindow, UsageView,
};
use gproxy_protocol::OperationKey;
use http::StatusCode;
use rust_decimal::Decimal;
use serde_json::json;

use crate::boundary::RoutingMode;
use crate::continuation::{Continuation, ContinuationKey};
use crate::control::{ControlPlane, ExposedModel, Plan, Pricing, ProviderRef};
use crate::error::CoreError;
use crate::host::{CredentialHealth, CredentialId, CredentialRecord, Host};
use crate::usage::Settlement;

#[derive(Clone)]
pub(super) struct MemoryHost {
    pub(super) state: Arc<Mutex<State>>,
}

pub(super) struct State {
    pub(super) credential: CredentialRecord,
    pub(super) cached_credential: Option<CredentialRecord>,
    pub(super) conflict: bool,
    pub(super) peer_refresh_on_wait: bool,
    pub(super) lease_calls: usize,
    pub(super) wait_calls: usize,
    pub(super) rotations: Vec<u64>,
    pub(super) health: Vec<(CredentialId, String, CredentialHealth)>,
    pub(super) health_writes_pending: bool,
    pub(super) track_health_attempts: bool,
    pub(super) health_attempts: Vec<(CredentialId, String, u64)>,
    pub(super) health_releases: Vec<(CredentialId, String, u64)>,
    pub(super) health_leases: BTreeSet<(CredentialId, String, u64)>,
    pub(super) cooling_model_pairs: Vec<(CredentialId, String)>,
    pub(super) unavailable_model_pairs: Vec<(CredentialId, String)>,
    pub(super) authorizations: Vec<String>,
    pub(super) upstream_requests: Vec<(http::HeaderMap, String)>,
    pub(super) upstream_bodies: Vec<Bytes>,
    pub(super) fingerprint_headers: Vec<String>,
    pub(super) loaded_credentials: Vec<CredentialId>,
    pub(super) settlements: Vec<Settlement>,
    pub(super) fail_usage: bool,
    pub(super) fail_usage_begin: bool,
    pub(super) captures: Vec<Captured>,
    pub(super) capture_response_body: bool,
    pub(super) auth_calls: usize,
    pub(super) admit_calls: usize,
    pub(super) exhausted_credentials: Vec<CredentialId>,
    pub(super) plan: Option<Plan>,
    pub(super) statuses: VecDeque<StatusCode>,
    pub(super) scripted: VecDeque<(StatusCode, Vec<Bytes>)>,
    pub(super) scripted_pending_at: Option<usize>,
    pub(super) model_prices: BTreeMap<String, Pricing>,
    pub(super) resolved_models: Vec<Option<String>>,
    pub(super) resolved_affinities: Vec<Option<i64>>,
    pub(super) aliases: BTreeMap<String, String>,
    pub(super) variants: BTreeMap<String, String>,
    pub(super) exposed_models: Vec<ExposedModel>,
    pub(super) admission_finishes: Vec<bool>,
    pub(super) bindings_enabled: bool,
    pub(super) bindings: BTreeMap<(i64, i64, String, String), Binding>,
    pub(super) cache: BTreeMap<String, Vec<u8>>,
    pub(super) fail_cache_set: bool,
    pub(super) cache_ttls: BTreeMap<String, u64>,
    pub(super) caller_user_id: i64,
    pub(super) caller_key_id: i64,
    pub(super) socket_opens: usize,
    pub(super) socket_closed: bool,
    pub(super) socket_frames: VecDeque<gproxy_channel_api::WsFrame>,
    pub(super) socket_sent: Vec<String>,
    pub(super) socket_statuses: VecDeque<u16>,
    pub(super) run_spawned: bool,
    pub(super) defer_spawned: bool,
    pub(super) spawned_tasks: Vec<BoxFuture<'static, ()>>,
    pub(super) drop_spawn_once: bool,
    pub(super) omit_usage: bool,
    pub(super) quota_windows: Vec<QuotaWindow>,
    pub(super) continuations_enabled: bool,
    pub(super) continuations: HashMap<ContinuationKey, Continuation>,
}

pub(super) struct Captured {
    pub(super) status: Option<StatusCode>,
    pub(super) body: Option<Bytes>,
    pub(super) provider_id: Option<i64>,
    pub(super) credential_id: Option<CredentialId>,
}

struct HealthActivity {
    state: Arc<Mutex<State>>,
    key: (CredentialId, String, u64),
}

impl crate::CredentialHealthActivity for HealthActivity {}

impl Drop for HealthActivity {
    fn drop(&mut self) {
        let mut state = self.state.lock().expect("state lock");
        assert!(state.health_leases.remove(&self.key));
        state.health_releases.push(self.key.clone());
    }
}

impl MemoryHost {
    pub(super) fn new(conflict: bool) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                credential: CredentialRecord {
                    id: CredentialId(7),
                    channel: "memory".into(),
                    kind: "oauth".into(),
                    secret: json!({"access_token": "old", "expires_at": 0}),
                    version: 4,
                },
                cached_credential: None,
                conflict,
                peer_refresh_on_wait: false,
                lease_calls: 0,
                wait_calls: 0,
                rotations: Vec::new(),
                health: Vec::new(),
                health_writes_pending: false,
                track_health_attempts: false,
                health_attempts: Vec::new(),
                health_releases: Vec::new(),
                health_leases: BTreeSet::new(),
                cooling_model_pairs: Vec::new(),
                unavailable_model_pairs: Vec::new(),
                authorizations: Vec::new(),
                upstream_requests: Vec::new(),
                upstream_bodies: Vec::new(),
                fingerprint_headers: Vec::new(),
                loaded_credentials: Vec::new(),
                settlements: Vec::new(),
                fail_usage: false,
                fail_usage_begin: false,
                captures: Vec::new(),
                capture_response_body: true,
                auth_calls: 0,
                admit_calls: 0,
                exhausted_credentials: Vec::new(),
                plan: None,
                statuses: VecDeque::new(),
                scripted: VecDeque::new(),
                scripted_pending_at: None,
                model_prices: BTreeMap::new(),
                resolved_models: Vec::new(),
                resolved_affinities: Vec::new(),
                aliases: BTreeMap::new(),
                variants: BTreeMap::new(),
                exposed_models: vec![ExposedModel {
                    id: "alias".into(),
                    display_name: None,
                    context_window: None,
                    max_output_tokens: None,
                    thinking_supported: None,
                    thinking_adaptive_supported: None,
                    thinking_enabled_supported: None,
                    metadata: Default::default(),
                }],
                admission_finishes: Vec::new(),
                bindings_enabled: true,
                bindings: BTreeMap::new(),
                cache: BTreeMap::new(),
                fail_cache_set: false,
                cache_ttls: BTreeMap::new(),
                caller_user_id: 1,
                caller_key_id: 2,
                socket_opens: 0,
                socket_closed: false,
                socket_frames: VecDeque::new(),
                socket_sent: Vec::new(),
                socket_statuses: VecDeque::new(),
                run_spawned: false,
                defer_spawned: false,
                spawned_tasks: Vec::new(),
                drop_spawn_once: false,
                omit_usage: false,
                quota_windows: Vec::new(),
                continuations_enabled: false,
                continuations: HashMap::new(),
            })),
        }
    }

    pub(super) fn without_bindings() -> Self {
        let host = Self::new(false);
        host.state.lock().expect("state lock").bindings_enabled = false;
        host
    }

    pub(super) fn with_continuations() -> Self {
        let host = Self::new(false);
        host.state.lock().expect("state lock").continuations_enabled = true;
        host
    }

    pub(super) fn with_session_spawner() -> Self {
        let host = Self::with_continuations();
        host.state.lock().expect("state lock").run_spawned = true;
        host
    }

    pub(super) fn with_cancelling_session_spawner() -> Self {
        let host = Self::with_continuations();
        host.state.lock().expect("state lock").drop_spawn_once = true;
        host
    }
}

impl Host for MemoryHost {
    type Credentials = Self;
    type Cache = Self;
    type Transport = Self;
    type Usage = Self;
    type Capture = Self;

    fn begin_credential_usage<'a>(
        &'a self,
        _request: &'a str,
        _target: &'a crate::control::Target,
        _started: i64,
    ) -> BoxFuture<'a, Result<(), CoreError>> {
        Box::pin(async move {
            if self.state.lock().unwrap().fail_usage_begin {
                Err(CoreError::Store(crate::error::StoreError(
                    "activity storage unavailable".into(),
                )))
            } else {
                Ok(())
            }
        })
    }
    fn credentials(&self) -> &Self::Credentials {
        self
    }
    fn cache(&self) -> &Self::Cache {
        self
    }
    fn transport(&self) -> &Self::Transport {
        self
    }
    fn usage(&self) -> &Self::Usage {
        self
    }
    fn capture(&self) -> &Self::Capture {
        self
    }
    fn authenticate<'a>(
        &'a self,
        _: &'a crate::boundary::RequestCtx,
    ) -> BoxFuture<'a, Result<CallerIdentity, CoreError>> {
        let mut state = self.state.lock().expect("state lock");
        state.auth_calls += 1;
        let identity = CallerIdentity {
            oauth_access_digest: None,
            user_id: state.caller_user_id,
            user_key_id: state.caller_key_id,
            org_id: None,
            team_id: None,
        };
        Box::pin(async move { Ok(identity) })
    }
    fn admit<'a>(
        &'a self,
        _: &'a CallerIdentity,
        _: &'a crate::boundary::RequestCtx,
        _: Option<OperationKey>,
        _: Option<&'a str>,
        plan: &'a Plan,
    ) -> BoxFuture<'a, Result<Plan, CoreError>> {
        self.state.lock().expect("state lock").admit_calls += 1;
        Box::pin(async { Ok(plan.clone()) })
    }
    fn finish_admission<'a>(
        &'a self,
        _: &'a str,
        settlement: Option<&'a Settlement>,
    ) -> BoxFuture<'a, ()> {
        self.state
            .lock()
            .expect("state lock")
            .admission_finishes
            .push(settlement.is_some());
        Box::pin(async {})
    }

    fn admit_credential<'a>(
        &'a self,
        _request_id: &'a str,
        target: &'a crate::Target,
        _: &'a Bytes,
        settle: gproxy_protocol::SettleMode,
    ) -> BoxFuture<'a, Result<(), CoreError>> {
        let exhausted = settle != gproxy_protocol::SettleMode::Free
            && self
                .state
                .lock()
                .expect("state lock")
                .exhausted_credentials
                .contains(&target.credential);
        Box::pin(async move {
            if exhausted {
                Err(CoreError::QuotaExceeded)
            } else {
                Ok(())
            }
        })
    }
    fn count_tokens<'a>(
        &'a self,
        _: &'a str,
        body: &'a Bytes,
        _: Option<&'a serde_json::Value>,
    ) -> BoxFuture<'a, Result<u64, CoreError>> {
        let tokens = crate::usage::estimate_input_tokens(body);
        Box::pin(async move { Ok(tokens) })
    }

    fn begin_credential_health_attempt<'a>(
        &'a self,
        _: &'a str,
        target: &'a crate::Target,
        credential_version: u64,
    ) -> BoxFuture<'a, Result<Option<crate::CredentialHealthLease>, CoreError>> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("state lock");
            let key = (
                target.credential,
                target.upstream_model.clone(),
                credential_version,
            );
            if state
                .unavailable_model_pairs
                .contains(&(target.credential, target.upstream_model.clone()))
            {
                return Err(CoreError::NoCredentials);
            }
            if state
                .cooling_model_pairs
                .contains(&(target.credential, target.upstream_model.clone()))
                || state.health_leases.contains(&key)
            {
                return Err(CoreError::CredentialCoolingDown {
                    retry_after_secs: 30,
                });
            }
            if !state.track_health_attempts {
                return Ok(None);
            }
            state.health_attempts.push(key.clone());
            state.health_leases.insert(key.clone());
            Ok(Some(Arc::new(HealthActivity {
                state: self.state.clone(),
                key,
            }) as crate::CredentialHealthLease))
        })
    }

    fn record_credential_health<'a>(
        &'a self,
        credential: CredentialId,
        model: &'a str,
        _: u64,
        health: CredentialHealth,
        _: Option<http::StatusCode>,
        _: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            std::future::poll_fn(|_| {
                if self.state.lock().expect("state lock").health_writes_pending {
                    std::task::Poll::Pending
                } else {
                    std::task::Poll::Ready(())
                }
            })
            .await;
            self.state.lock().expect("state lock").health.push((
                credential,
                model.to_owned(),
                health,
            ));
        })
    }
    fn wait<'a>(&'a self, _: std::time::Duration) -> BoxFuture<'a, ()> {
        let mut state = self.state.lock().expect("state lock");
        state.wait_calls += 1;
        if state.peer_refresh_on_wait {
            state.peer_refresh_on_wait = false;
            state.credential.secret = json!({
                "access_token": "peer",
                "expires_at": i64::MAX
            });
            state.credential.version += 1;
        }
        Box::pin(async {})
    }
    fn surface_usage<'a>(
        &'a self,
        _: &'a CallerIdentity,
        _: &'a ProviderRef,
        _: CredentialId,
    ) -> Box<dyn UsageView + 'a> {
        Box::new(self.clone())
    }
    fn bindings(&self) -> Option<&dyn BindingStore> {
        self.state
            .lock()
            .expect("state lock")
            .bindings_enabled
            .then_some(self as &dyn BindingStore)
    }
    fn spawner(&self) -> Option<&dyn crate::host::Spawner> {
        self.state
            .lock()
            .expect("state lock")
            .continuations_enabled
            .then_some(self as &dyn crate::host::Spawner)
    }
    fn continuations(&self) -> Option<&dyn crate::ContinuationStore> {
        self.state
            .lock()
            .expect("state lock")
            .continuations_enabled
            .then_some(self as &dyn crate::ContinuationStore)
    }
}

impl ControlPlane for MemoryHost {
    fn resolve_alias(&self, model: &str, _: &RoutingMode) -> String {
        self.state
            .lock()
            .expect("state lock")
            .aliases
            .get(model)
            .cloned()
            .unwrap_or_else(|| model.to_owned())
    }

    fn resolve_preprocessed(
        &self,
        model: Option<&str>,
        _: &RoutingMode,
        affinity: Option<i64>,
    ) -> Result<Plan, CoreError> {
        let mut state = self.state.lock().expect("state lock");
        state.resolved_models.push(model.map(str::to_owned));
        state.resolved_affinities.push(affinity);
        state
            .plan
            .clone()
            .ok_or_else(|| CoreError::UnknownRoute("unused".into()))
    }

    fn resolve_variant(&self, model: &str, _: &RoutingMode) -> Option<String> {
        self.state
            .lock()
            .expect("state lock")
            .variants
            .get(model)
            .cloned()
    }

    fn shared(&self) -> Option<Arc<dyn ControlPlane>> {
        Some(Arc::new(self.clone()))
    }

    fn pricing(&self, _: &ProviderRef, upstream_model: &str) -> Option<Pricing> {
        if let Some(price) = self
            .state
            .lock()
            .expect("state")
            .model_prices
            .get(upstream_model)
        {
            return Some(price.clone());
        }
        let mut pricing = Pricing {
            input_per_million: match upstream_model {
                "transcription-model" => Decimal::from(3),
                "transcription-model-2" => Decimal::from(5),
                _ => Decimal::ONE,
            },
            output_per_million: Decimal::from(2),
            cached_input_per_million: None,
            service_tier: None,
            tiers: Vec::new(),
            metric_rates: BTreeMap::new(),
            conditional_metric_rates: BTreeMap::new(),
        };
        if upstream_model == "tier-model" {
            pricing.tiers.push(crate::control::PricingTier {
                service_tier: Some("auto".into()),
                multiplier: Some(Decimal::from(3)),
                ..Default::default()
            });
        }
        Some(pricing)
    }

    fn exposed_models(&self) -> Vec<ExposedModel> {
        self.state
            .lock()
            .expect("state lock")
            .exposed_models
            .clone()
    }

    fn detached(&self) -> Box<dyn ControlPlane> {
        Box::new(self.clone())
    }
}
