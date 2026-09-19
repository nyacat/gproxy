use std::borrow::Cow;

use gproxy_channel_api::{
    ChannelError, FailureState, StreamCtx,
    channel::{StreamStart, StreamStartState},
};
use serde::Deserialize;
use serde_json::{Map, Value, value::RawValue};

use super::framing::delimiter;

#[cfg(test)]
mod tests;

/// The engine owns the only accumulated prefix. This classifier borrows JSON
/// values and skips instructions, tool schemas and extensions without building
/// a Value tree, lifecycle state, output meter or serialized response frames.
pub(in crate::codex) struct CodexStreamStart {
    start: usize,
    scan: usize,
    ready: bool,
    failure: FailureState,
}

impl CodexStreamStart {
    pub(in crate::codex) fn for_operation(ctx: StreamCtx<'_>) -> Option<Self> {
        use gproxy_protocol::{ContentGenerationKind, Operation, OperationKind, StreamFraming};
        if ctx.framing != StreamFraming::Sse
            || !matches!(
                ctx.key.operation(),
                Operation::StreamGenerateContent
                    | Operation::GuardianReview
                    | Operation::GuardianClassify
            )
            || ctx.key.kind()
                != OperationKind::ContentGeneration(ContentGenerationKind::OpenAiResponses)
        {
            return None;
        }
        Some(Self {
            start: 0,
            scan: 0,
            ready: false,
            failure: FailureState::new("codex", ctx.response_headers),
        })
    }

    fn frame(&mut self, raw: &[u8]) -> Result<(), ChannelError> {
        let text = std::str::from_utf8(raw).map_err(|_| invalid())?;
        let mut event = None;
        let mut data = Cow::Borrowed("");
        let mut lines = text.lines().filter_map(|line| {
            if let Some(value) = line.strip_prefix("event:") {
                event = Some(value.trim_start());
            }
            line.strip_prefix("data:")
                .map(|value| value.strip_prefix(' ').unwrap_or(value))
        });
        if let Some(first) = lines.next() {
            data = Cow::Borrowed(first);
            if let Some(second) = lines.next() {
                // One allocation, at most the wire frame length; no Vec of
                // data lines or geometrically growing join buffer.
                let mut joined = String::with_capacity(raw.len());
                joined.push_str(first);
                joined.push('\n');
                joined.push_str(second);
                for line in lines.by_ref() {
                    joined.push('\n');
                    joined.push_str(line);
                }
                data = Cow::Owned(joined);
            }
        }
        drop(lines);
        if data.is_empty() {
            return Ok(());
        }
        if data.trim() == "[DONE]" {
            self.ready |= self.failure.failure().is_none();
            return Ok(());
        }
        let metadata: Metadata<'_> = serde_json::from_str(&data).map_err(|_| invalid())?;
        let kind = scalar(metadata.kind, false)?;
        let kind = kind.as_str().or(event);
        if !matches!(
            kind,
            Some(
                "response.created"
                    | "response.queued"
                    | "response.in_progress"
                    | "response.failed"
                    | "error"
            )
        ) {
            self.ready = true;
            return Ok(());
        }
        let response = metadata.response.object()?;
        let scope = response.as_ref().unwrap_or(&metadata);
        if !scope.output.empty_output() || !scope.usage.absent_or_null() {
            self.ready = true;
            return Ok(());
        }
        let mut value = metadata.scalars()?;
        metadata.insert_error(&mut value)?;
        if let Some(response) = response {
            let mut projected = response.scalars()?;
            response.insert_error(&mut projected)?;
            value.insert("response".into(), Value::Object(projected));
        }
        self.failure.observe(event, &Value::Object(value));
        Ok(())
    }
}

impl StreamStart for CodexStreamStart {
    fn scratch_bytes(&self, prefix: &[u8]) -> usize {
        // A multiline join uses <= n bytes. Serde's scratch for escaped keys
        // or ignored nesting uses <= 2n capacity, and <= 3n while a growing
        // allocation and its predecessor coexist. Selected metadata is bounded
        // independently and covered by the engine's fixed reservation.
        prefix.len().saturating_sub(self.start).saturating_mul(4)
    }

    fn inspect(&mut self, prefix: &[u8], eof: bool) -> Result<StreamStartState, ChannelError> {
        while !self.ready {
            let Some((relative, size)) = delimiter(&prefix[self.scan..]) else {
                break;
            };
            let end = self.scan + relative;
            self.frame(&prefix[self.start..end])?;
            self.start = end + size;
            self.scan = self.start;
        }
        self.scan = self.start.max(prefix.len().saturating_sub(3));
        if eof && !self.ready && self.start < prefix.len() {
            self.frame(&prefix[self.start..])?;
            self.start = prefix.len();
            self.scan = self.start;
        }
        Ok(if self.ready {
            StreamStartState::Ready
        } else if let Some(failure) = self.failure.failure() {
            StreamStartState::Failed(failure.clone())
        } else {
            StreamStartState::Pending
        })
    }
}

// Preserve explicit null versus an absent field (top-level error metadata
// wins over wrapped fields, even when explicitly null).
#[derive(Clone, Copy, Default)]
struct Raw<'a>(Option<&'a RawValue>);

impl<'de: 'a, 'a> Deserialize<'de> for Raw<'a> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <&'a RawValue>::deserialize(deserializer).map(|value| Self(Some(value)))
    }
}

impl<'a> Raw<'a> {
    fn object(self) -> Result<Option<Metadata<'a>>, ChannelError> {
        self.0
            .filter(|raw| raw.get().starts_with('{'))
            .map(|raw| serde_json::from_str(raw.get()).map_err(|_| invalid()))
            .transpose()
    }

    fn absent_or_null(self) -> bool {
        self.0.is_none_or(|v| v.get() == "null")
    }

    fn empty_output(self) -> bool {
        self.0.is_none_or(|v| {
            v.get()
                .strip_prefix('[')
                .and_then(|s| s.trim_start().strip_prefix(']'))
                .is_some_and(|s| s.trim().is_empty())
        })
    }
}

#[derive(Deserialize, Default)]
struct Metadata<'a> {
    #[serde(borrow, default, rename = "type")]
    kind: Raw<'a>,
    #[serde(borrow, default)]
    response: Raw<'a>,
    #[serde(borrow, default)]
    output: Raw<'a>,
    #[serde(borrow, default)]
    usage: Raw<'a>,
    #[serde(borrow, default)]
    status: Raw<'a>,
    #[serde(borrow, default)]
    error: Raw<'a>,
    #[serde(borrow, default)]
    status_details: Raw<'a>,
    #[serde(borrow, default)]
    code: Raw<'a>,
    #[serde(borrow, default)]
    message: Raw<'a>,
    #[serde(borrow, default, rename = "Message")]
    upper_message: Raw<'a>,
    #[serde(borrow, default)]
    reason: Raw<'a>,
    #[serde(borrow, default, rename = "__type")]
    exception_type: Raw<'a>,
    #[serde(borrow, default)]
    request_id: Raw<'a>,
    #[serde(borrow, default, rename = "requestId")]
    camel_request_id: Raw<'a>,
}

impl Metadata<'_> {
    fn scalars(&self) -> Result<Map<String, Value>, ChannelError> {
        let mut value = Map::new();
        for (name, raw) in [
            ("type", self.kind),
            ("code", self.code),
            ("status", self.status),
            ("message", self.message),
            ("Message", self.upper_message),
            ("reason", self.reason),
            ("__type", self.exception_type),
            ("request_id", self.request_id),
            ("requestId", self.camel_request_id),
        ] {
            if raw.0.is_some() {
                value.insert(
                    name.into(),
                    scalar(raw, matches!(name, "message" | "Message" | "reason"))?,
                );
            }
        }
        Ok(value)
    }

    fn insert_error(&self, value: &mut Map<String, Value>) -> Result<(), ChannelError> {
        if self.error.0.is_some() {
            value.insert("error".into(), error_value(self.error)?);
        }
        if let Some(details) = self.status_details.object()? {
            value.insert(
                "status_details".into(),
                serde_json::json!({"error": error_value(details.error)?}),
            );
        }
        Ok(())
    }
}

fn error_value(raw: Raw<'_>) -> Result<Value, ChannelError> {
    match raw.object()? {
        Some(error) => Ok(Value::Object(error.scalars()?)),
        None => scalar(raw, true),
    }
}

fn scalar(raw: Raw<'_>, diagnostic: bool) -> Result<Value, ChannelError> {
    let Some(raw) = raw.0 else {
        return Ok(Value::Null);
    };
    let raw = raw.get();
    if raw.starts_with(['{', '[']) {
        return Ok(Value::Null);
    }
    if raw.len() > 1024 {
        // Classification fields cannot be truncated into a different code.
        // Oversized free-form diagnostics may be omitted; structured failure
        // classification still uses the original code/type/status fields.
        return if diagnostic {
            Ok(Value::Null)
        } else {
            Err(invalid())
        };
    }
    serde_json::from_str(raw).map_err(|_| invalid())
}

fn invalid() -> ChannelError {
    ChannelError::Decode("invalid or oversized Codex stream start metadata".into())
}
