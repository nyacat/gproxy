use gproxy_channel_api::{ChannelError, StreamDecodeError, channel::StreamDecodeDiagnostic};
use gproxy_protocol::openai::generate_content::responses::ResponseStreamEvent;
use serde_json::Value;

pub(super) fn decode(
    data: &str,
    event: Option<&str>,
) -> Result<(ResponseStreamEvent, Value), StreamDecodeError> {
    let mut value: Value =
        serde_json::from_str(data).map_err(|error| failure(error, data.len(), event, None))?;
    // Some upstreams wrap error metadata. Existing top-level fields, including
    // explicit null, always win; preserve the original envelope as an extension.
    if value.get("type").and_then(Value::as_str) == Some("error")
        && let Some(nested) = value.get("error").and_then(Value::as_object).cloned()
        && let Some(object) = value.as_object_mut()
    {
        for key in ["code", "message", "param"] {
            if let Some(field) = nested.get(key) {
                object
                    .entry(key.to_owned())
                    .or_insert_with(|| field.clone());
            }
        }
    }
    let parsed = serde_json::from_value(value.clone())
        .map_err(|error| failure(error, data.len(), event, Some(&value)))?;
    Ok((parsed, value))
}

fn label(value: &str) -> String {
    value
        .chars()
        .take(128)
        .map(|c| {
            if c.is_ascii_alphanumeric() || "._-".contains(c) {
                c
            } else {
                '?'
            }
        })
        .collect()
}

fn failure(
    error: serde_json::Error,
    size: usize,
    event: Option<&str>,
    value: Option<&Value>,
) -> StreamDecodeError {
    let raw_error = error.to_string();
    let (path, message) = raw_error
        .split_once(": ")
        .unwrap_or(("", raw_error.as_str()));
    let mut path = path
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || "._[]".contains(*c))
        .take(128)
        .collect::<String>();
    if path == "." {
        path.clear();
    }
    // Serde's invalid-type messages can include the offending string value.
    // Keep schema field names and a bounded path, never an upstream value.
    let safe_message = if let Some(start) = message.find("missing field `") {
        let missing = &message[start..];
        missing
            .find('`')
            .and_then(|first| missing[first + 1..].find('`').map(|last| first + last + 2))
            .map(|end| missing[..end].to_owned())
            .unwrap_or_else(|| "missing event field".into())
    } else {
        match error.classify() {
            serde_json::error::Category::Syntax | serde_json::error::Category::Eof => format!(
                "invalid JSON at line {}, column {}",
                error.line(),
                error.column()
            ),
            _ => "invalid event field type or value".into(),
        }
    };
    if let Some(field) = safe_message
        .strip_prefix("missing field `")
        .and_then(|s| s.strip_suffix('`'))
    {
        if !path.is_empty() {
            path.push('.');
        }
        path.push_str(field);
    }
    let mut fields = Vec::new();
    if let Some(value) = value {
        // Record only known field names and JSON types, never field values.
        for path in [
            "/code",
            "/message",
            "/param",
            "/error",
            "/error/code",
            "/response",
            "/response/error",
            "/response/error/code",
            "/response/usage",
            "/delta",
            "/output_index",
        ] {
            if let Some(field) = value.pointer(path) {
                let kind = match field {
                    Value::Null => "null",
                    Value::Bool(_) => "bool",
                    Value::Number(_) => "number",
                    Value::String(_) => "string",
                    Value::Array(_) => "array",
                    Value::Object(_) => "object",
                };
                fields.push(format!("{path}:{kind}"));
            }
        }
    }
    StreamDecodeError {
        error: ChannelError::Decode(format!("Responses event JSON: {path}: {safe_message}")),
        frames: Vec::new(),
        diagnostic: Some(Box::new(StreamDecodeDiagnostic {
            event: event.map(label),
            event_type: value
                .and_then(|v| v.get("type"))
                .and_then(Value::as_str)
                .map(label),
            payload_bytes: size,
            field_path: path,
            fields,
        })),
    }
}
