use gproxy_channel_api::UpstreamFailure;

/// Error envelopes cannot pass through a content-response converter. Preserve
/// their upstream message for the client; only the separate diagnostic is redacted.
pub(crate) fn error_body(
    kind: gproxy_protocol::OperationKind,
    body: &bytes::Bytes,
    failure: &UpstreamFailure,
) -> bytes::Bytes {
    use gproxy_protocol::{ContentGenerationKind as Kind, OperationKind};
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return body.clone();
    };
    let error = value
        .get("error")
        .or_else(|| value.pointer("/response/error"))
        .unwrap_or(&value);
    let message = error
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Upstream request failed");
    let code = error
        .get("code")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let error_type = error
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(failure.category);
    let result = match kind {
        OperationKind::ContentGeneration(Kind::ClaudeMessages) => {
            serde_json::json!({"type":"error","error":{"type":error_type,"message":message,"code":code}})
        }
        OperationKind::ContentGeneration(Kind::GeminiGenerateContent) => {
            let (code, status) = match failure.category {
                "input" | "policy" => (400, "INVALID_ARGUMENT"),
                "permission" => (403, "PERMISSION_DENIED"),
                "authentication" => (401, "UNAUTHENTICATED"),
                "rate_limit" => (429, "RESOURCE_EXHAUSTED"),
                _ => (503, "UNAVAILABLE"),
            };
            serde_json::json!({"error":{"code":code,"status":status,"message":message}})
        }
        _ => {
            serde_json::json!({"error":{"type":error_type,"code":code,"message":message,"param":error.get("param")}})
        }
    };
    bytes::Bytes::from(serde_json::to_vec(&result).expect("error envelope serializes"))
}

pub(crate) fn log(
    ctx: &super::FunnelCtx,
    status: http::StatusCode,
    failure: &UpstreamFailure,
    stream: bool,
    usage_received: bool,
) {
    let message = if stream {
        "stream.terminal"
    } else {
        "upstream.failure"
    };
    tracing::info!(
        request_id = %ctx.request_id,
        provider_id = ctx.target.provider.id,
        credential_id = ctx.target.credential.0,
        channel = %ctx.target.provider.channel,
        model = %ctx.target.upstream_model,
        status = status.as_u16(),
        disposition = ?failure.disposition,
        reason = "upstream_terminal_event",
        protocol = %failure.protocol,
        event_type = %failure.event_type,
        error_category = failure.category,
        error_code = failure.code.as_deref(),
        error_type = failure.error_type.as_deref(),
        error_message = failure.message.as_deref(),
        upstream_request_id = failure.request_id.as_deref(),
        upstream_request_id_source = failure.request_id_source,
        message_truncated = failure.message_truncated,
        usage_received,
        "{message}"
    );
}
