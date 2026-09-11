use gproxy_channel_api::BoxFuture;
use gproxy_core::{CaptureSink, CoreError, Ended, Settlement, UsageSink, UsageSource};
use gproxy_store::records::UsageInput;
use serde::{Deserialize, Serialize};

use super::{AppHost, settlement_recovery};

impl UsageSink for AppHost {
    fn record<'a>(&'a self, settlement: &'a Settlement) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Err(error) = self.record_checked(settlement).await {
                tracing::error!(request_id = %settlement.request_id, error = %error,
                    "usage settlement incomplete; admission remains pending");
            }
        })
    }

    fn record_checked<'a>(
        &'a self,
        settlement: &'a Settlement,
    ) -> BoxFuture<'a, Result<(), CoreError>> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let host = self.clone();
            let settlement = settlement.clone();
            Box::pin(async move {
                let task_host = host.clone();
                host.services
                    .spawner
                    .spawn_tracked(async move { record_settlement(&task_host, &settlement).await })
                    .await
                    .map_err(|error| CoreError::Internal(format!("usage task failed: {error}")))?
            })
        }
        #[cfg(target_arch = "wasm32")]
        Box::pin(record_settlement(self, settlement))
    }
}

/// Recovery owns complete usage rows, so neither identity nor instance metadata
/// changes when a retry runs after admission was released or settings reloaded.
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct UsageReplay {
    pub version: u8,
    pub settlement: Settlement,
    pub inputs: Option<Vec<UsageInput>>,
    #[serde(default)]
    pub credential_charges: Option<Vec<super::admission::credential_budget::CredentialCharge>>,
    pub usage_recorded: bool,
    identity_required: bool,
    at: i64,
    instance_name: String,
    instance_id: String,
    enable_usage: bool,
}

impl UsageReplay {
    fn new(host: &AppHost, settlement: &Settlement, usage_recorded: bool) -> Self {
        let settings = host.services.control.settings();
        Self {
            version: 1,
            settlement: settlement.clone(),
            inputs: None,
            credential_charges: None,
            usage_recorded,
            identity_required: false,
            at: unix_now(),
            instance_name: settings.instance_name,
            instance_id: settings.instance_id.to_string(),
            enable_usage: settings.enable_usage,
        }
    }

    pub(super) async fn prepare(&mut self, host: &AppHost) -> Result<(), CoreError> {
        self.prepare_inputs(host).await?;
        if !self.usage_recorded && self.credential_charges.is_none() {
            self.credential_charges = Some(
                super::admission::credential_budget::prepare_record(host, &self.settlement).await?,
            );
        }
        Ok(())
    }

    async fn prepare_inputs(&mut self, host: &AppHost) -> Result<(), CoreError> {
        if self.version != 1 {
            return Err(CoreError::Internal(
                "unsupported settlement recovery version".into(),
            ));
        }
        if self.usage_recorded {
            return Ok(());
        }
        if self.inputs.is_some() {
            return Ok(());
        }
        if !self.enable_usage {
            self.inputs = Some(Vec::new());
            return Ok(());
        }
        let identity_required = self.identity_required;
        // Set this before awaiting so even a timed-out read cannot later turn
        // into an anonymous usage row after its admission key disappears.
        self.identity_required = true;
        let state = match super::admission::load(host, &self.settlement.request_id).await {
            Ok(state) => state,
            Err(error) => {
                // A failed read is never evidence of an anonymous request.
                self.identity_required = true;
                return Err(error);
            }
        };
        if state.is_none() && identity_required {
            return Err(CoreError::Internal(
                "usage identity is missing during recovery".into(),
            ));
        }
        let identity = state.as_ref().map(|state| &state.identity);
        let settlement = &self.settlement;
        let mut dimensions = settlement.usage.dimensions.clone();
        dimensions.insert("instance_name".into(), self.instance_name.clone());
        dimensions.insert("instance_id".into(), self.instance_id.clone());
        let mut input = UsageInput {
            upstream_started_at_ms: settlement.upstream_started_at_ms,
            request_id: settlement.request_id.clone(),
            at: self.at,
            provider_id: settlement.provider_id,
            credential_id: settlement.credential_id.0,
            organization_id: identity.and_then(|identity| identity.org_id),
            team_id: identity.and_then(|identity| identity.team_id),
            user_id: identity.map(|identity| identity.user_id),
            user_key_id: identity.map(|identity| identity.user_key_id),
            operation: state.and_then(|state| state.operation),
            upstream_model: settlement.upstream_model.clone(),
            input_tokens: settlement.usage.input_tokens,
            output_tokens: settlement.usage.output_tokens,
            cached_input_tokens: settlement.usage.cached_input_tokens,
            metrics: serde_json::to_value(&settlement.usage.metrics)
                .expect("decimal metrics serialize"),
            dimensions: serde_json::to_value(dimensions).expect("string dimensions serialize"),
            cost: settlement.cost,
            usage_source: usage_source(settlement.source).into(),
            ended: match settlement.ended {
                Ended::Complete => "complete",
                Ended::Interrupted => "interrupted",
            }
            .into(),
            latency_ms: settlement.latency_ms,
        };
        let mut inputs = Vec::with_capacity(settlement.attempts.len().max(1));
        if settlement.attempts.is_empty() {
            inputs.push(input);
        } else {
            for (index, attempt) in settlement.attempts.iter().enumerate() {
                input.request_id = if index == 0 {
                    settlement.request_id.clone()
                } else {
                    format!("{}:attempt:{index}", settlement.request_id)
                };
                input.upstream_model.clone_from(&attempt.upstream_model);
                input.upstream_started_at_ms = attempt.started_at_ms;
                input.input_tokens = attempt.usage.input_tokens;
                input.output_tokens = attempt.usage.output_tokens;
                input.cached_input_tokens = attempt.usage.cached_input_tokens;
                input.metrics = serde_json::to_value(&attempt.usage.metrics)
                    .expect("decimal metrics serialize");
                let mut dimensions = attempt.usage.dimensions.clone();
                dimensions.insert("instance_name".into(), self.instance_name.clone());
                dimensions.insert("instance_id".into(), self.instance_id.clone());
                dimensions.insert("parent_request_id".into(), settlement.request_id.clone());
                dimensions.insert("billable".into(), attempt.billable.to_string());
                input.dimensions =
                    serde_json::to_value(dimensions).expect("string dimensions serialize");
                input.cost = attempt.cost;
                input.usage_source = usage_source(attempt.source).into();
                inputs.push(input.clone());
            }
        }
        self.inputs = Some(inputs);
        Ok(())
    }

    pub(super) async fn apply(&self, host: &AppHost) -> Result<(), CoreError> {
        if self.usage_recorded {
            return Ok(());
        }
        super::admission::credential_budget::record_prepared(
            host,
            &self.settlement,
            self.credential_charges.as_deref().ok_or_else(|| {
                CoreError::Internal("usage recovery has no credential window snapshot".into())
            })?,
        )
        .await?;
        for input in self
            .inputs
            .as_ref()
            .ok_or_else(|| CoreError::Internal("usage recovery has no identity snapshot".into()))?
        {
            host.services
                .store
                .record_usage(input)
                .await
                .map_err(settlement_recovery::store_error)?;
        }
        Ok(())
    }
}

async fn record_settlement(host: &AppHost, settlement: &Settlement) -> Result<(), CoreError> {
    let mut replay = UsageReplay::new(host, settlement, false);
    let known = host
        .services
        .settlement_recovery
        .knows(&settlement.request_id);
    if known
        && let Some(payload) = settlement_recovery::load_known(host, &settlement.request_id).await?
    {
        if payload
            .get("completed")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            return Ok(());
        }
        if let Some(original) = payload.get("settlement") {
            if serde_json::to_value(settlement)
                .map_err(|error| CoreError::Internal(error.to_string()))?
                != *original
            {
                return Err(CoreError::Internal(
                    "conflicting settlements share a recovery request id".into(),
                ));
            }
            replay = serde_json::from_value(payload)
                .map_err(|error| CoreError::Internal(error.to_string()))?;
        }
    }
    let result = settlement_recovery::bounded(async {
        replay.prepare(host).await?;
        super::settlement_retry::run(host, || replay.apply(host)).await
    })
    .await;
    if let Err(error) = result {
        let _ = settlement_recovery::retain(host, &replay).await;
        return Err(error);
    }
    if known {
        replay.usage_recorded = true;
        settlement_recovery::retain(host, &replay).await?;
    }
    Ok(())
}

pub(super) async fn retain_failed_settlement(host: &AppHost, settlement: &Settlement) {
    // The funnel reaches finish only after usage committed; replay must not
    // derive a new credential window after the old reservations were released.
    let _ = settlement_recovery::retain(host, &UsageReplay::new(host, settlement, true)).await;
}

pub(super) async fn finish_settlement(
    host: &AppHost,
    request_id: &str,
    settlement: Option<&Settlement>,
) {
    if let Err(error) = settlement_recovery::bounded(super::admission::finish_checked(
        host, request_id, settlement,
    ))
    .await
    {
        tracing::error!(request_id, error = %error, "admission settlement incomplete");
        match settlement {
            Some(settlement) => retain_failed_settlement(host, settlement).await,
            None => settlement_recovery::retain_refund(host, request_id).await,
        }
    }
}

fn usage_source(source: UsageSource) -> &'static str {
    match source {
        UsageSource::Upstream => "upstream",
        UsageSource::Estimated => "estimated",
    }
}

impl CaptureSink for AppHost {
    fn captures_response_body(&self) -> bool {
        let policy = crate::logging::Policy::read(&self.services.control.current().settings);
        policy.upstream && policy.upstream_body
    }

    fn record<'a>(&'a self, capture: &'a gproxy_core::host::Capture) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let policy = crate::logging::Policy::read(&self.services.control.current().settings);
            if !policy.upstream {
                return;
            }
            let input =
                gproxy_store::records::CaptureInput {
                    request_id: capture.request_id.clone(),
                    at: unix_now(),
                    provider_id: capture.provider_id,
                    credential_id: capture.credential_id.map(|credential| credential.0),
                    upstream_url: capture
                        .upstream_url
                        .as_deref()
                        .map(|url| crate::logging::redaction::url_string(url, policy.redact)),
                    request_method: capture.request_method.as_ref().map(ToString::to_string),
                    request_headers: capture.request_headers.as_ref().map(|headers| {
                        crate::logging::redaction::headers_json(headers, policy.redact)
                    }),
                    response_status: capture.response_status.map(|status| status.as_u16()),
                    response_headers: capture.response_headers.as_ref().map(|headers| {
                        crate::logging::redaction::headers_json(headers, policy.redact)
                    }),
                    request_body: policy.upstream_body.then(|| {
                        crate::logging::redaction::body_bytes(&capture.request_body, policy.redact)
                    }),
                    response_body: capture
                        .response_body
                        .as_ref()
                        .filter(|_| policy.upstream_body)
                        .map(|body| crate::logging::redaction::body_bytes(body, policy.redact)),
                };
            if let Err(error) = self.services.store.record_capture(&input).await {
                tracing::error!(request_id = %capture.request_id, error = %error, "persist capture failed");
            }
        })
    }
}

fn unix_now() -> i64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .expect("system time is after Unix epoch")
        .as_secs() as i64
}
