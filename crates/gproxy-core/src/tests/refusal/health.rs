use bytes::Bytes;
use futures_util::{FutureExt, StreamExt};
use http::StatusCode;
use serde_json::json;

use super::{block_on, enqueue, execute, message, request, setup};
use crate::{CredentialHealth, ResponseBody};

fn settings() -> serde_json::Value {
    json!({"base_url":"https://upstream.example", "claude_fallback_mode":"models",
        "claude_fallback_models":["claude-opus-4-8"]})
}

#[test]
fn recovery_observations_follow_each_actual_fallback_model() {
    for streaming in [false, true] {
        let (host, core) = setup(gproxy_channels::AzureChannel, settings());
        host.state.lock().unwrap().track_health_attempts = true;
        enqueue(
            &host,
            message("claude-fable-5", "refusal", "", 10, 0),
            streaming,
        );
        enqueue(
            &host,
            message("claude-opus-4-8", "end_turn", "answer", 8, 5),
            streaming,
        );
        let (_, body) = execute(&host, &core, streaming);
        assert_eq!(body["model"], "claude-opus-4-8");
        let state = host.state.lock().unwrap();
        let id = state.credential.id;
        assert_eq!(
            state.health,
            [
                (id, "claude-fable-5".into(), CredentialHealth::Healthy),
                (id, "claude-opus-4-8".into(), CredentialHealth::Healthy),
            ]
        );
        assert_eq!(state.health_attempts.len(), 2);
        assert_eq!(state.health_releases.len(), 2);
        assert!(state.health_leases.is_empty());
    }
}

#[test]
fn an_overloaded_fallback_does_not_degrade_the_original_model_or_recover_from_headers() {
    for streaming in [false, true] {
        let (host, core) = setup(gproxy_channels::AzureChannel, settings());
        host.state.lock().unwrap().track_health_attempts = true;
        enqueue(
            &host,
            message("claude-fable-5", "refusal", "", 10, 0),
            streaming,
        );
        host.state.lock().unwrap().scripted.push_back((StatusCode::SERVICE_UNAVAILABLE, vec![
            Bytes::from_static(b"{\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}"),
        ]));
        let (_, body) = execute(&host, &core, streaming);
        assert_eq!(body["stop_reason"], "refusal");
        let state = host.state.lock().unwrap();
        let id = state.credential.id;
        assert_eq!(
            state.health,
            [
                (id, "claude-fable-5".into(), CredentialHealth::Healthy),
                (id, "claude-opus-4-8".into(), CredentialHealth::Degraded),
            ]
        );
        assert!(state.health_leases.is_empty());
    }
}

#[test]
fn a_cooling_fallback_preserves_the_original_response_without_a_failed_attempt() {
    for streaming in [false, true] {
        let (host, core) = setup(gproxy_channels::AzureChannel, settings());
        {
            let mut state = host.state.lock().unwrap();
            state.track_health_attempts = true;
            let id = state.credential.id;
            state
                .cooling_model_pairs
                .push((id, "claude-opus-4-8".into()));
        }
        enqueue(
            &host,
            message("claude-fable-5", "refusal", "", 10, 0),
            streaming,
        );
        let (_, body) = execute(&host, &core, streaming);
        assert_eq!(body["model"], "claude-fable-5");
        assert_eq!(body["stop_reason"], "refusal");
        let state = host.state.lock().unwrap();
        assert_eq!(state.upstream_requests.len(), 1);
        assert_eq!(state.health_attempts.len(), 1);
        assert_eq!(state.health_attempts, state.health_releases);
        assert_eq!(
            state.health,
            [(
                state.credential.id,
                "claude-fable-5".into(),
                CredentialHealth::Healthy
            )]
        );
    }
}

#[test]
fn cancelling_an_open_fallback_stream_releases_both_model_leases() {
    let (host, core) = setup(gproxy_channels::AzureChannel, settings());
    host.state.lock().unwrap().track_health_attempts = true;
    enqueue(&host, message("claude-fable-5", "refusal", "", 10, 0), true);
    {
        let mut state = host.state.lock().unwrap();
        state.scripted.push_back((StatusCode::OK, Vec::new()));
        state.scripted_pending_at = Some(2);
    }
    let mut input = request(true, "fallback-health-cancel");
    input.path = "/v1/messages".into();
    input.body = Bytes::from(
        json!({"model":"claude-fable-5","max_tokens":128,"stream":true,
        "messages":[{"role":"user","content":"hello"}]})
        .to_string(),
    );
    let outcome = block_on(core.execute(&host, input)).unwrap();
    let cancel = outcome.stream_cancellation.unwrap();
    let ResponseBody::Stream(mut stream) = outcome.body else {
        panic!("stream")
    };
    while let Some(Some(_)) = stream.next().now_or_never() {}
    {
        let state = host.state.lock().unwrap();
        assert_eq!(state.upstream_requests.len(), 2);
        assert_eq!(state.health_leases.len(), 2);
        assert_eq!(
            state.health,
            [(
                state.credential.id,
                "claude-fable-5".into(),
                CredentialHealth::Healthy
            )]
        );
    }
    cancel.cancel();
    block_on(async { while stream.next().await.is_some() {} });
    drop(stream);
    let state = host.state.lock().unwrap();
    assert_eq!(state.health_attempts.len(), 2);
    assert_eq!(state.health_releases.len(), 2);
    assert!(state.health_leases.is_empty());
    assert_eq!(
        state.health.len(),
        1,
        "an open fallback has no success observation"
    );
}

#[test]
fn same_model_credit_retry_retains_its_existing_probe_lease() {
    let (host, core) = setup(gproxy_channels::AzureChannel, settings());
    host.state.lock().unwrap().track_health_attempts = true;
    let mut refused = message("claude-fable-5", "refusal", "partial", 10, 2);
    refused["stop_details"]["fallback_credit_token"] = json!("credit");
    refused["stop_details"]["fallback_has_prefill_claim"] = json!(true);
    enqueue(&host, refused, false);
    host.state.lock().unwrap().scripted.push_back((StatusCode::BAD_REQUEST, vec![
        Bytes::from_static(b"{\"error\":{\"type\":\"invalid_request_error\",\"message\":\"redemption temporarily unavailable\"}}"),
    ]));
    enqueue(
        &host,
        message("claude-opus-4-8", "end_turn", "answer", 8, 5),
        false,
    );
    let (_, body) = execute(&host, &core, false);
    assert_eq!(body["model"], "claude-opus-4-8");
    let state = host.state.lock().unwrap();
    assert_eq!(state.upstream_requests.len(), 3);
    assert_eq!(
        state.health_attempts.len(),
        2,
        "same-model retry reuses its live lease"
    );
    assert_eq!(state.health_releases.len(), 2);
    assert!(state.health_leases.is_empty());
}

#[test]
fn an_error_inside_a_successful_sse_response_degrades_only_the_fallback_model() {
    let (host, core) = setup(gproxy_channels::AzureChannel, settings());
    host.state.lock().unwrap().track_health_attempts = true;
    enqueue(&host, message("claude-fable-5", "refusal", "", 10, 0), true);
    host.state.lock().unwrap().scripted.push_back((StatusCode::OK, vec![
        Bytes::from_static(b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"),
    ]));
    let mut input = request(true, "fallback-health-error-event");
    input.path = "/v1/messages".into();
    input.body = Bytes::from(
        json!({"model":"claude-fable-5","max_tokens":128,"stream":true,
        "messages":[{"role":"user","content":"hello"}]})
        .to_string(),
    );
    let outcome = block_on(core.execute(&host, input)).unwrap();
    let ResponseBody::Stream(mut stream) = outcome.body else {
        panic!("stream")
    };
    block_on(async { while stream.next().await.is_some() {} });
    drop(stream);
    let state = host.state.lock().unwrap();
    let id = state.credential.id;
    assert_eq!(
        state.health,
        [
            (id, "claude-fable-5".into(), CredentialHealth::Healthy),
            (id, "claude-opus-4-8".into(), CredentialHealth::Degraded),
        ]
    );
    assert!(state.health_leases.is_empty());
}

#[test]
fn a_pinned_followup_acquires_the_actual_model_not_the_original_route_model() {
    let (host, core) = setup(gproxy_channels::AzureChannel, settings());
    host.state.lock().unwrap().track_health_attempts = true;
    enqueue(
        &host,
        message("claude-fable-5", "refusal", "", 10, 0),
        false,
    );
    enqueue(
        &host,
        message("claude-opus-4-8", "end_turn", "answer", 8, 5),
        false,
    );
    execute(&host, &core, false);
    {
        let mut state = host.state.lock().unwrap();
        let id = state.credential.id;
        state
            .cooling_model_pairs
            .push((id, "claude-fable-5".into()));
    }
    enqueue(
        &host,
        message("claude-opus-4-8", "end_turn", "followup", 8, 5),
        false,
    );
    let (_, response) = execute(&host, &core, false);
    assert_eq!(response["model"], "claude-opus-4-8");
    let state = host.state.lock().unwrap();
    let models = state
        .health_attempts
        .iter()
        .map(|(_, model, _)| model.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        models,
        ["claude-fable-5", "claude-opus-4-8", "claude-opus-4-8"]
    );
    assert_eq!(state.health_releases.len(), 3);
    assert!(state.health_leases.is_empty());
}

#[test]
fn a_failure_after_a_fallback_success_terminal_overrides_success_and_stays_sticky() {
    let (host, core) = setup(gproxy_channels::AzureChannel, settings());
    enqueue(&host, message("claude-fable-5", "refusal", "", 10, 0), true);
    enqueue(
        &host,
        message("claude-opus-4-8", "end_turn", "answer", 8, 5),
        true,
    );
    host.state.lock().unwrap().scripted.back_mut().unwrap().1.extend([
        Bytes::from_static(b"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"),
        Bytes::from_static(b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"),
    ]);
    let mut input = request(true, "fallback-health-late-error");
    input.path = "/v1/messages".into();
    input.body = Bytes::from(
        json!({"model":"claude-fable-5","max_tokens":128,"stream":true,
        "messages":[{"role":"user","content":"hello"}]})
        .to_string(),
    );
    let outcome = block_on(core.execute(&host, input)).unwrap();
    let ResponseBody::Stream(mut stream) = outcome.body else {
        panic!("stream")
    };
    block_on(async { while stream.next().await.is_some() {} });
    let state = host.state.lock().unwrap();
    let id = state.credential.id;
    assert_eq!(
        state.health,
        [
            (id, "claude-fable-5".into(), CredentialHealth::Healthy),
            (id, "claude-opus-4-8".into(), CredentialHealth::Healthy),
            (id, "claude-opus-4-8".into(), CredentialHealth::Degraded),
        ]
    );
}
