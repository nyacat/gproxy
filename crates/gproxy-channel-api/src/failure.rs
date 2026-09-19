//! Protocol error metadata. Values are sanitized before they can enter tracing
//! or credential health records; content and tool-result errors are not inspected.
mod sanitize;
#[cfg(test)]
mod tests;

use http::{HeaderMap, StatusCode};
use serde_json::Value;

use crate::Disposition;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamFailure {
    pub protocol: String,
    pub event_type: String,
    pub error_envelope: bool,
    pub category: &'static str,
    pub code: Option<String>,
    pub error_type: Option<String>,
    pub message: Option<String>,
    pub request_id: Option<String>,
    pub request_id_source: Option<&'static str>,
    pub message_truncated: bool,
    pub disposition: Disposition,
}

impl UpstreamFailure {
    pub fn from_http(
        protocol: &str,
        status: StatusCode,
        headers: &HeaderMap,
        body: &[u8],
    ) -> Option<Self> {
        let value = serde_json::from_slice::<Value>(body).unwrap_or(Value::Null);
        Self::from_value(protocol, headers, None, &value, Some(status))
    }

    pub fn from_value(
        protocol: &str,
        headers: &HeaderMap,
        event: Option<&str>,
        value: &Value,
        status: Option<StatusCode>,
    ) -> Option<Self> {
        Self::from_value_with_semantics(protocol, headers, event, value, status)
            .map(|(failure, _)| failure)
    }

    fn from_value_with_semantics(
        protocol: &str,
        headers: &HeaderMap,
        event: Option<&str>,
        value: &Value,
        status: Option<StatusCode>,
    ) -> Option<(Self, bool)> {
        let event = value
            .get("type")
            .and_then(Value::as_str)
            .or(event)
            .unwrap_or("http");
        // These report an operation within a live response, not failure of
        // the generation itself. In particular, a rejected steer may be
        // followed by a successful completion of the original response.
        if matches!(
            event,
            "response.inject.failed"
                | "response.steer.failed"
                | "response.mcp_call.failed"
                | "response.mcp_list_tools.failed"
        ) {
            return None;
        }
        let response = value.get("response").unwrap_or(value);
        let failed = matches!(event, "error" | "response.failed")
            || response.get("status").and_then(Value::as_str) == Some("failed");
        let incomplete = event == "response.incomplete"
            || response.get("status").and_then(Value::as_str) == Some("incomplete");
        let error = value.get("error").filter(|v| !v.is_null()).or_else(|| {
            if failed {
                response
                    .get("error")
                    .filter(|v| !v.is_null())
                    .or_else(|| response.pointer("/status_details/error"))
            } else {
                None
            }
        });
        let block = value
            .pointer("/promptFeedback/blockReason")
            .and_then(Value::as_str)
            .filter(|v| *v != "BLOCK_REASON_UNSPECIFIED");
        let finish = value
            .get("candidates")
            .and_then(Value::as_array)
            .and_then(|candidates| {
                candidates
                    .iter()
                    .filter_map(|c| c.get("finishReason").and_then(Value::as_str))
                    .find(|reason| {
                        matches!(
                            *reason,
                            "MAX_TOKENS"
                                | "SAFETY"
                                | "RECITATION"
                                | "BLOCKLIST"
                                | "PROHIBITED_CONTENT"
                                | "SPII"
                                | "MALFORMED_FUNCTION_CALL"
                        )
                    })
            });
        let chat_finish = value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| {
                choices
                    .iter()
                    .filter_map(|c| c.get("finish_reason").and_then(Value::as_str))
                    .find(|reason| matches!(*reason, "length" | "content_filter"))
            });
        if error.is_none()
            && !failed
            && !incomplete
            && block.is_none()
            && finish.is_none()
            && chat_finish.is_none()
            && status.is_none_or(|s| !s.is_client_error() && !s.is_server_error())
        {
            return None;
        }
        let payload = error.unwrap_or(value);
        let raw_code = scalar(if event == "error" && value.get("code").is_some() {
            value.get("code")
        } else {
            payload.get("code")
        })
        .or_else(|| scalar(payload.get("__type")))
        .or_else(|| {
            response
                .pointer("/incomplete_details/reason")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| block.or(finish).or(chat_finish).map(str::to_owned));
        let raw_type = scalar(payload.get("type"))
            .filter(|t| t != "error")
            .or_else(|| scalar(payload.get("status")));
        let raw_message = (if event == "error" && value.get("message").is_some() {
            value.get("message")
        } else {
            payload.get("message")
        })
        .or_else(|| payload.get("Message"))
        .or_else(|| payload.get("reason"))
        .and_then(Value::as_str)
        .or_else(|| error.and_then(Value::as_str));
        let (category, disposition) =
            classify(raw_code.as_deref(), raw_type.as_deref(), status, incomplete);
        let has_semantics =
            classify(raw_code.as_deref(), raw_type.as_deref(), None, incomplete).0 != "unknown";
        let (message, message_truncated) = raw_message.map(sanitize::message).unzip();
        let (request_id, request_id_source) = request_id(headers, value, payload, raw_message);
        Some((
            Self {
                protocol: sanitize::label(protocol, 128),
                event_type: sanitize::label(event, 128),
                error_envelope: error.is_some()
                    || failed
                    || status.is_some_and(|s| s.is_client_error() || s.is_server_error()),
                category,
                code: raw_code.as_deref().map(|v| sanitize::label(v, 128)),
                error_type: raw_type.as_deref().map(|v| sanitize::label(v, 128)),
                message,
                request_id,
                request_id_source,
                message_truncated: message_truncated.unwrap_or(false),
                disposition,
            },
            has_semantics,
        ))
    }

    pub fn health_detail(&self) -> String {
        format!(
            "upstream {}{}{}",
            self.category,
            self.code
                .as_ref()
                .map(|v| format!(" code={v}"))
                .unwrap_or_default(),
            self.request_id
                .as_ref()
                .map(|v| format!(" request_id={v}"))
                .unwrap_or_default()
        )
    }
}

fn scalar(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(v) => Some(v.clone()),
        Value::Number(v) => Some(v.to_string()),
        _ => None,
    }
}

pub fn http_disposition(response: crate::ResponseView<'_>) -> Disposition {
    UpstreamFailure::from_http("http", response.status, response.headers, response.body)
        .map_or(Disposition::Success, |failure| failure.disposition)
}

/// Prefer a recognized protocol error over a provider's status-only policy.
/// Unknown error envelopes on successful HTTP responses are still failures,
/// while unstructured denials keep the channel's established 401/402/403 policy.
pub fn http_disposition_with_fallback(
    response: crate::ResponseView<'_>,
    fallback: Disposition,
) -> Disposition {
    let value = serde_json::from_slice::<Value>(response.body).unwrap_or(Value::Null);
    UpstreamFailure::from_value_with_semantics(
        "http",
        response.headers,
        None,
        &value,
        Some(response.status),
    )
    .map_or(fallback, |(failure, has_semantics)| {
        if has_semantics
            || response.status.is_success()
            || response.status == StatusCode::REQUEST_TIMEOUT
        {
            failure.disposition
        } else {
            fallback
        }
    })
}

pub fn classify(
    code: Option<&str>,
    error_type: Option<&str>,
    status: Option<StatusCode>,
    incomplete: bool,
) -> (&'static str, Disposition) {
    let labels = [code, error_type].map(|label| {
        label.map(|label| {
            label
                .rsplit(['#', ':'])
                .next()
                .unwrap_or(label)
                .to_ascii_lowercase()
        })
    });
    // Some APIs echo the HTTP status in `code` but carry the actual cause in
    // `type` or `status`. Prefer that cause over an ambiguous numeric denial.
    let numeric = |label: &&String| label.bytes().all(|byte| byte.is_ascii_digit());
    for label in labels
        .iter()
        .flatten()
        .filter(|label| !numeric(label))
        .chain(labels.iter().flatten().filter(numeric))
    {
        let known = match label.as_str() {
            "invalid_api_key"
            | "invalid_authentication"
            | "authentication_error"
            | "unauthenticated"
            | "invalid_token"
            | "token_expired"
            | "token_revoked"
            | "expiredtokenexception"
            | "unrecognizedclientexception"
            | "401" => Some(("authentication", Disposition::CredentialDead)),
            "rate_limit_exceeded"
            | "rate_limit_error"
            | "resource_exhausted"
            | "throttlingexception"
            | "insufficient_quota"
            | "429" => Some(("rate_limit", Disposition::Retryable)),
            "server_error"
            | "api_error"
            | "internal_server_error"
            | "internalserverexception"
            | "internal"
            | "overloaded_error"
            | "service_unavailable"
            | "serviceunavailableexception"
            | "unavailable"
            | "model_stream_error"
            | "modelstreamerrorexception"
            | "500"
            | "502"
            | "503"
            | "529" => Some(("upstream", Disposition::Retryable)),
            "timeout_error"
            | "request_timeout"
            | "deadline_exceeded"
            | "vector_store_timeout"
            | "modeltimeoutexception"
            | "408"
            | "504" => Some(("timeout", Disposition::Retryable)),
            "context_length_exceeded"
            | "invalid_prompt"
            | "invalid_request_error"
            | "invalid_argument"
            | "validation_error"
            | "validationexception"
            | "bad_request"
            | "malformed_function_call"
            | "invalidstateevent"
            | "data_residency_mismatch"
            | "400"
            | "413"
            | "422"
            | "invalid_image"
            | "invalid_image_format"
            | "invalid_base64_image"
            | "invalid_image_url"
            | "image_too_large"
            | "image_too_small"
            | "image_parse_error"
            | "invalid_image_mode"
            | "image_file_too_large"
            | "unsupported_image_media_type"
            | "empty_image_file"
            | "failed_to_download_image"
            | "image_file_not_found" => Some(("input", Disposition::Terminal)),
            "permission_error"
            | "permission_denied"
            | "accessdeniedexception"
            | "not_found_error"
            | "not_found"
            | "403"
            | "404" => Some(("permission", Disposition::Terminal)),
            "content_filter"
            | "safety"
            | "recitation"
            | "blocklist"
            | "prohibited_content"
            | "spii"
            | "image_content_policy_violation"
            | "misalignment_policy_violation"
            | "refusal" => Some(("policy", Disposition::Terminal)),
            "max_output_tokens" | "max_tokens" | "length" | "steered" => {
                Some(("incomplete", Disposition::Terminal))
            }
            _ => None,
        };
        if let Some(known) = known {
            return known;
        }
    }
    if incomplete {
        return ("incomplete", Disposition::Terminal);
    }
    match status.map(|s| s.as_u16()) {
        Some(401) => ("authentication", Disposition::CredentialDead),
        Some(403 | 404) => ("permission", Disposition::Terminal),
        Some(408 | 504) => ("timeout", Disposition::Retryable),
        Some(429) => ("rate_limit", Disposition::Retryable),
        Some(500..=599) => ("upstream", Disposition::Retryable),
        Some(400..=499) => ("input", Disposition::Terminal),
        _ => ("unknown", Disposition::Retryable),
    }
}

fn request_id(
    headers: &HeaderMap,
    value: &Value,
    error: &Value,
    message: Option<&str>,
) -> (Option<String>, Option<&'static str>) {
    for name in [
        "x-request-id",
        "request-id",
        "x-amzn-requestid",
        "x-amz-request-id",
        "x-goog-request-id",
    ] {
        if let Some(id) = headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(sanitize::request_id)
        {
            return (Some(id), Some(name));
        }
    }
    for (object, source) in [(error, "error.request_id"), (value, "event.request_id")] {
        if let Some(id) = object
            .get("request_id")
            .or_else(|| object.get("requestId"))
            .and_then(Value::as_str)
            .and_then(sanitize::request_id)
        {
            return (Some(id), Some(source));
        }
    }
    let id = message.and_then(sanitize::message_request_id);
    let source = id.as_ref().map(|_| "error.message");
    (id, source)
}

#[derive(Debug, Clone, Default)]
pub struct FailureState {
    failure: Option<UpstreamFailure>,
    completed: bool,
    protocol: String,
    headers: HeaderMap,
}

impl FailureState {
    pub fn new(protocol: &str, headers: &HeaderMap) -> Self {
        // Only retain the correlation headers, never authorization or cookies.
        let mut selected = HeaderMap::new();
        for name in [
            "x-request-id",
            "request-id",
            "x-amzn-requestid",
            "x-amz-request-id",
            "x-goog-request-id",
        ] {
            if let Some(value) = headers.get(name) {
                selected.insert(name, value.clone());
            }
        }
        Self {
            protocol: protocol.into(),
            headers: selected,
            ..Default::default()
        }
    }

    pub fn observe(&mut self, event: Option<&str>, value: &Value) {
        if self.failure.is_none() {
            self.failure =
                UpstreamFailure::from_value(&self.protocol, &self.headers, event, value, None);
        }
        self.completed |= matches!(
            value.get("type").and_then(Value::as_str).or(event),
            Some("response.completed" | "message_stop")
        );
    }

    pub fn exception(&mut self, kind: &str, value: &Value) {
        let mut error = value.as_object().cloned().unwrap_or_default();
        error
            .entry("code".to_owned())
            .or_insert_with(|| Value::String(kind.into()));
        let event = serde_json::json!({"error": error});
        if self.failure.is_none() {
            self.failure = UpstreamFailure::from_value(
                &self.protocol,
                &self.headers,
                Some(kind),
                &event,
                None,
            );
        }
    }

    pub fn failure(&self) -> Option<&UpstreamFailure> {
        self.failure.as_ref()
    }
    pub fn disposition(&self) -> Option<Disposition> {
        self.failure
            .as_ref()
            .map(|v| v.disposition)
            .or(self.completed.then_some(Disposition::Success))
    }
}
