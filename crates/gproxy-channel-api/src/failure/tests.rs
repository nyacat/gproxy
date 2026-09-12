use super::*;
use serde_json::json;

fn diagnose(value: Value) -> UpstreamFailure {
    UpstreamFailure::from_value("test", &HeaderMap::new(), None, &value, None).unwrap()
}

#[test]
fn protocol_error_matrix() {
    for (value, category, disposition) in [
        (
            json!({"type":"error","message":"failed"}),
            "unknown",
            Disposition::Retryable,
        ),
        (
            json!({"type":"error","code":null,"message":"failed"}),
            "unknown",
            Disposition::Retryable,
        ),
        (
            json!({"error":{"code":429,"message":"busy"}}),
            "rate_limit",
            Disposition::Retryable,
        ),
        (
            json!({"type":"error","error":{"type":"overloaded_error","message":"busy"}}),
            "upstream",
            Disposition::Retryable,
        ),
        (
            json!({"type":"error","code":"server_is_overloaded","message":"busy"}),
            "upstream",
            Disposition::Retryable,
        ),
        (
            json!({"type":"response.failed","response":{"status":"failed","error":{"code":"server_is_overloaded","message":"busy"}}}),
            "upstream",
            Disposition::Retryable,
        ),
        (
            json!({"error":{"code":400,"status":"INVALID_ARGUMENT","message":"bad"}}),
            "input",
            Disposition::Terminal,
        ),
        (
            json!({"error":{"code":403,"status":"UNAUTHENTICATED","message":"expired"}}),
            "authentication",
            Disposition::CredentialDead,
        ),
        (
            json!({"type":"response.failed","response":{"error":{"code":"context_length_exceeded","message":"bad"}}}),
            "input",
            Disposition::Terminal,
        ),
        (
            json!({"type":"response.done","response":{"status":"failed","status_details":{"error":{"code":"server_error","message":"bad"}}}}),
            "upstream",
            Disposition::Retryable,
        ),
        (
            json!({"type":"response.incomplete","response":{"incomplete_details":{"reason":"max_output_tokens"}}}),
            "incomplete",
            Disposition::Terminal,
        ),
        (
            json!({"promptFeedback":{"blockReason":"SAFETY"}}),
            "policy",
            Disposition::Terminal,
        ),
        (
            json!({"error":{"__type":"com.aws#ValidationException","message":"bad"}}),
            "input",
            Disposition::Terminal,
        ),
        (
            json!({"error":{"code":"future_error","message":"bad"}}),
            "unknown",
            Disposition::Retryable,
        ),
    ] {
        let failure = diagnose(value);
        assert_eq!(
            (failure.category, failure.disposition),
            (category, disposition),
            "{failure:?}"
        );
    }
}

#[test]
fn ordinary_content_and_tool_failures_are_not_request_errors() {
    for value in [
        json!({"choices":[{"message":{"content":"error"},"finish_reason":"tool_calls"}]}),
        json!({"type":"content_block_delta","delta":{"error":{"code":"server_error"}}}),
        json!({"type":"response.mcp_call.failed","item":{"error":"failed"}}),
        json!({"response":{"status":"completed","error":null}}),
    ] {
        assert!(
            UpstreamFailure::from_value("test", &HeaderMap::new(), None, &value, None).is_none()
        );
    }
}

#[test]
fn messages_are_redacted_before_utf8_truncation() {
    let message = format!(
        "Bearer bearer-secret sk-private-key token=private-token cookie=session=secret\nuser@example.com https://example.com/path?signature=secret \"private prompt\" {}",
        "字".repeat(1000)
    );
    let failure = diagnose(json!({"type":"error","code":"future_error","message":message}));
    let printed = format!("{failure:?}");
    for secret in [
        "bearer-secret",
        "sk-private-key",
        "private-token",
        "session=secret",
        "user@example.com",
        "signature=secret",
        "private prompt",
    ] {
        assert!(!printed.contains(secret), "{secret} appeared in {printed}");
    }
    assert!(failure.message.as_ref().unwrap().len() <= 1024);
    assert!(failure.message_truncated);
}

#[test]
fn upstream_request_id_has_explicit_provenance() {
    let value = json!({"type":"error","message":"Please include the request ID eae2fefa-6696-4225-8b80-63556aa6c292 in your message.","request_id":"event-id"});
    let mut headers = HeaderMap::new();
    headers.insert("x-request-id", "header-id".parse().unwrap());
    let failure = UpstreamFailure::from_value("codex", &headers, None, &value, None).unwrap();
    assert_eq!(failure.request_id.as_deref(), Some("header-id"));
    assert_eq!(failure.request_id_source, Some("x-request-id"));
    let failure = diagnose(
        json!({"type":"error","message":value["message"],"response_id":"not-a-request-id"}),
    );
    assert_eq!(
        failure.request_id.as_deref(),
        Some("eae2fefa-6696-4225-8b80-63556aa6c292")
    );
    assert_eq!(failure.request_id_source, Some("error.message"));
}

#[test]
fn first_failure_survives_synthetic_success() {
    let mut state = FailureState::new("claude", &HeaderMap::new());
    state.observe(
        None,
        &json!({"type":"error","error":{"type":"invalid_request_error","message":"bad"}}),
    );
    state.observe(None, &json!({"type":"message_stop"}));
    assert_eq!(state.disposition(), Some(Disposition::Terminal));
    assert_eq!(state.failure().unwrap().category, "input");
}

#[test]
fn permissions_and_policy_do_not_kill_credentials() {
    assert_eq!(
        classify(None, None, Some(StatusCode::FORBIDDEN), false).1,
        Disposition::Terminal
    );
    assert_eq!(
        classify(
            Some("misalignment_policy_violation"),
            None,
            Some(StatusCode::FORBIDDEN),
            false
        )
        .1,
        Disposition::Terminal
    );
    assert_eq!(
        classify(None, None, Some(StatusCode::UNAUTHORIZED), false).1,
        Disposition::CredentialDead
    );
}

#[test]
fn semantic_http_errors_preserve_provider_fallback_for_unknown_denials() {
    let headers = HeaderMap::new();
    for body in [b"{}".as_slice(), br#"{"error":{"code":"future_error"}}"#] {
        for fallback in [Disposition::Terminal, Disposition::CredentialDead] {
            assert_eq!(
                http_disposition_with_fallback(
                    crate::ResponseView {
                        status: StatusCode::FORBIDDEN,
                        headers: &headers,
                        body,
                    },
                    fallback,
                ),
                fallback,
            );
        }
    }
    for (status, body, expected) in [
        (
            StatusCode::FORBIDDEN,
            br#"{"error":{"code":"misalignment_policy_violation"}}"#.as_slice(),
            Disposition::Terminal,
        ),
        (
            StatusCode::FORBIDDEN,
            br#"{"__type":"com.aws#ValidationException","message":"invalid input"}"#,
            Disposition::Terminal,
        ),
        (
            StatusCode::BAD_REQUEST,
            br#"{"error":{"code":"invalid_api_key"}}"#,
            Disposition::CredentialDead,
        ),
        (
            StatusCode::OK,
            br#"{"error":{"code":"future_error"}}"#,
            Disposition::Retryable,
        ),
        (
            StatusCode::BAD_REQUEST,
            br#"{"error":{"code":"server_is_overloaded"}}"#,
            Disposition::Retryable,
        ),
        (
            StatusCode::REQUEST_TIMEOUT,
            b"request timed out",
            Disposition::Retryable,
        ),
    ] {
        assert_eq!(
            http_disposition_with_fallback(
                crate::ResponseView {
                    status,
                    headers: &headers,
                    body
                },
                Disposition::CredentialDead,
            ),
            expected,
        );
    }
}

#[test]
fn authorization_and_cookie_headers_cannot_leak_their_trailing_values() {
    for message in [
        "Authorization: Bearer bearer-secret",
        "Proxy-Authorization: Basic basic-secret",
        "Cookie: first=one-secret; second=two-secret",
        "access-token=token-secret",
    ] {
        let failure = diagnose(json!({"type":"error","message":message}));
        assert!(!format!("{failure:?}").contains("-secret"));
    }
}
