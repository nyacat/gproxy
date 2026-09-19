use gproxy_channel_api::{Channel, Disposition};

use crate::control::Target;
use crate::host::Host;

pub(crate) async fn response(
    host: &impl Host,
    channel: &dyn Channel,
    facts: &super::FunnelCtx,
    disposition: Disposition,
    status: http::StatusCode,
    headers: &http::HeaderMap,
) {
    observe_quota(host, channel, facts, headers).await;
    record_response(host, facts, disposition, status).await;
}

/// A successful status opens a stream; it does not establish that the
/// response completed. Quota headers are still useful immediately.
pub(crate) async fn stream_response(
    host: &impl Host,
    channel: &dyn Channel,
    facts: &super::FunnelCtx,
    disposition: Disposition,
    status: http::StatusCode,
    headers: &http::HeaderMap,
) {
    observe_quota(host, channel, facts, headers).await;
    if disposition != Disposition::Success {
        record_response(host, facts, disposition, status).await;
    }
}

pub(crate) async fn observe_quota(
    host: &impl Host,
    channel: &dyn Channel,
    facts: &super::FunnelCtx,
    headers: &http::HeaderMap,
) {
    if facts.health_delegated {
        return;
    }
    let target = &facts.target;
    let mut observations = channel.observe_quota(headers);
    let received_at_ms = crate::quota::now_ms();
    for observation in &mut observations {
        observation.sample = Some(gproxy_channel_api::QuotaSample {
            source: gproxy_channel_api::QuotaSampleSource::Response,
            started_at_ms: facts
                .upstream_started_at_ms
                .expect("upstream response has a send time"),
            received_at_ms,
        });
    }
    let mut entries = observations
        .iter()
        .map(|observation| gproxy_channel_api::QuotaEntry::from_window(observation, received_at_ms))
        .collect::<Vec<_>>();
    if !observations.is_empty()
        && let Some(version) = facts.credential_version
    {
        host.observe_credential_quota(target.credential, version, observations)
            .await;
    }
    entries.extend(channel.quota_response_entries(headers, &target.upstream_model));
    for entry in &mut entries {
        entry.observed_at_ms = received_at_ms;
    }
    if !entries.is_empty()
        && let Some(version) = facts.credential_version
    {
        host.observe_credential_quota_entries(target.credential, version, entries)
            .await;
    }
}

pub(crate) async fn record_response(
    host: &impl Host,
    facts: &super::FunnelCtx,
    disposition: Disposition,
    status: http::StatusCode,
) {
    if facts.health_delegated {
        return;
    }
    let target = &facts.target;
    let Some(credential_version) = facts.credential_version else {
        return;
    };
    let (health, detail) = match disposition {
        Disposition::Success => (
            crate::CredentialHealth::Healthy,
            "upstream request succeeded",
        ),
        Disposition::Retryable => (
            crate::CredentialHealth::Degraded,
            "retryable upstream response",
        ),
        Disposition::Terminal => return,
        Disposition::CredentialDead => (
            crate::CredentialHealth::Dead,
            "credential rejected upstream",
        ),
    };
    host.record_credential_health(
        target.credential,
        &target.upstream_model,
        credential_version,
        health,
        Some(status),
        detail,
    )
    .await;
}

pub(crate) async fn record_failure(
    host: &impl Host,
    facts: &super::FunnelCtx,
    status: http::StatusCode,
    failure: &gproxy_channel_api::UpstreamFailure,
) {
    if facts.health_delegated {
        return;
    }
    let Some(version) = facts.credential_version else {
        return;
    };
    let health = match failure.disposition {
        Disposition::Retryable => crate::CredentialHealth::Degraded,
        Disposition::CredentialDead => crate::CredentialHealth::Dead,
        Disposition::Success | Disposition::Terminal => return,
    };
    host.record_credential_health(
        facts.target.credential,
        &facts.target.upstream_model,
        version,
        health,
        Some(status),
        &failure.health_detail(),
    )
    .await;
}

pub(crate) async fn degraded(
    host: &impl Host,
    target: &Target,
    credential_version: Option<u64>,
    status: Option<http::StatusCode>,
    detail: &str,
) {
    let Some(credential_version) = credential_version else {
        return;
    };
    host.record_credential_health(
        target.credential,
        &target.upstream_model,
        credential_version,
        crate::CredentialHealth::Degraded,
        status,
        detail,
    )
    .await;
}

pub(crate) async fn dead(
    host: &impl Host,
    target: &Target,
    credential_version: Option<u64>,
    detail: &str,
) {
    let Some(credential_version) = credential_version else {
        return;
    };
    host.record_credential_health(
        target.credential,
        &target.upstream_model,
        credential_version,
        crate::CredentialHealth::Dead,
        None,
        detail,
    )
    .await;
}
