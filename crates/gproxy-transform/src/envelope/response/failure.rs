use crate::{TransformError, envelope::SseFrame};
use bytes::Bytes;
use gproxy_protocol::{ContentGenerationKind as Kind, OperationKind};
use serde_json::{Value, json};

// Intercept actual protocol error envelopes before content converters. Normal
// incomplete, policy finish reasons and failed tool suboperations retain their
// content semantics. Upstream messages here are client payload, never logs.
pub(super) fn frame(
    kind: OperationKind,
    frame: &SseFrame,
) -> Result<Option<Bytes>, TransformError> {
    let Ok(value) = serde_json::from_str::<Value>(&frame.data) else {
        return Ok(None);
    };
    let event = value
        .get("type")
        .and_then(Value::as_str)
        .or(frame.event.as_deref());
    if matches!(
        event,
        Some(
            "response.inject.failed"
                | "response.steer.failed"
                | "response.mcp_call.failed"
                | "response.mcp_list_tools.failed"
        )
    ) {
        return Ok(None);
    }
    let response = value.get("response").unwrap_or(&value);
    let failed = event == Some("response.failed")
        || response.get("status").and_then(Value::as_str) == Some("failed");
    let error = value
        .get("error")
        .filter(|e| !e.is_null())
        .or_else(|| failed.then(|| response.get("error")).flatten());
    if error.is_none() && !failed && event != Some("error") {
        return Ok(None);
    }
    let error = error.unwrap_or(&value);
    let message = value
        .get("message")
        .or_else(|| error.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("Upstream request failed");
    let code = value
        .get("code")
        .or_else(|| error.get("code"))
        .cloned()
        .unwrap_or(Value::Null);
    let error_type = error
        .get("type")
        .and_then(Value::as_str)
        .filter(|v| *v != "error")
        .unwrap_or("api_error");
    let (event, value) = match kind {
        OperationKind::ContentGeneration(Kind::ClaudeMessages) => (
            Some("error"),
            json!({"type":"error","error":{"type":error_type,"code":code,"message":message}}),
        ),
        OperationKind::ContentGeneration(
            Kind::OpenAiResponses | Kind::OpenAiResponsesWebSocket,
        ) => (
            Some("error"),
            json!({"type":"error","code":code,"message":message,"param":null,"sequence_number":0}),
        ),
        OperationKind::ContentGeneration(Kind::GeminiGenerateContent) => (
            None,
            json!({"error":{"code":code.as_u64().unwrap_or(500),"status":"INTERNAL","message":message}}),
        ),
        _ => (
            None,
            json!({"error":{"type":error_type,"code":code,"message":message,"param":null}}),
        ),
    };
    Ok(Some(SseFrame::encode(
        event,
        &serde_json::to_string(&value)?,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gproxy_protocol::{Operation, OperationKey};
    #[test]
    fn errors_cross_every_content_pair_without_a_success_tail() {
        for source in [
            Kind::OpenAiChat,
            Kind::OpenAiResponses,
            Kind::ClaudeMessages,
            Kind::GeminiGenerateContent,
        ] {
            for target in [
                Kind::OpenAiChat,
                Kind::OpenAiResponses,
                Kind::ClaudeMessages,
                Kind::GeminiGenerateContent,
            ] {
                if source == target {
                    continue;
                }
                let key = |kind| OperationKey::content(Operation::StreamGenerateContent, kind);
                let mut stream = crate::response_stream(key(source), key(target)).unwrap();
                let mut output = stream.push(Bytes::from_static(b"data: {\"type\":\"error\",\"error\":{\"code\":\"server_error\",\"message\":\"upstream busy\"}}\n\n")).unwrap();
                output.extend(stream.finish().unwrap());
                let output = output
                    .iter()
                    .map(|b| String::from_utf8_lossy(b))
                    .collect::<String>();
                assert!(output.contains("upstream busy"), "{source:?} {target:?}");
                assert!(
                    !output.contains("message_stop")
                        && !output.contains("response.completed")
                        && !output.contains("finish_reason")
                );
            }
        }
    }
}
