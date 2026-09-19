mod diagnostic;
mod event;
mod framing;
mod lifecycle;
pub(super) mod start;
mod tools;

#[cfg(test)]
mod tests;

use bytes::{Bytes, BytesMut};
use gproxy_channel_api::StreamDecodeError;
use gproxy_channel_api::{
    ChannelError, Disposition, Frame, NormalizedUsage, StreamCtx, StreamDecoder, StreamEnd,
    StreamTail,
};
use gproxy_protocol::openai::generate_content::responses::ResponseStreamEvent;
use gproxy_protocol::{ContentGenerationKind, Operation, OperationKind};

use framing::{delimiter, encode, parse};

pub(super) struct CodexSseDecoder {
    buffer: BytesMut,
    scan_from: usize,
    lifecycle: lifecycle::Lifecycle,
    tools: tools::ToolAliases,
    usage: Option<NormalizedUsage>,
    actual_service_tier: Option<String>,
    done_seen: bool,
    replay_safe: bool,
    failure: gproxy_channel_api::FailureState,
    output_meter: Option<crate::shared::responses_meter::ResponsesMeter>,
}

impl CodexSseDecoder {
    pub(super) fn for_operation(ctx: StreamCtx<'_>) -> Option<Self> {
        (matches!(
            ctx.key.operation(),
            Operation::StreamGenerateContent
                | Operation::GuardianReview
                | Operation::GuardianClassify
        ) && ctx.key.kind()
            == OperationKind::ContentGeneration(ContentGenerationKind::OpenAiResponses))
        .then(|| Self {
            buffer: BytesMut::new(),
            scan_from: 0,
            lifecycle: Default::default(),
            tools: Default::default(),
            usage: None,
            actual_service_tier: None,
            done_seen: false,
            replay_safe: true,
            failure: gproxy_channel_api::FailureState::new("codex", ctx.response_headers),
            output_meter: Some(Default::default()),
        })
    }

    fn drain(&mut self) -> Result<Vec<Frame>, StreamDecodeError> {
        let mut output = Vec::new();
        while let Some((relative, delimiter)) = delimiter(&self.buffer[self.scan_from..]) {
            let end = self.scan_from + relative;
            if end > 100 * 1024 * 1024 {
                return Err(StreamDecodeError::from(ChannelError::Decode(
                    "Codex Responses SSE frame exceeds 100 MiB".into(),
                ))
                .prepend(output));
            }
            let raw = self.buffer.split_to(end + delimiter);
            self.scan_from = 0;
            match self.frame(&raw[..end]) {
                Ok(frames) => output.extend(frames),
                Err(error) => return Err(error.prepend(output)),
            }
        }
        if self.buffer.len() > 100 * 1024 * 1024 {
            return Err(StreamDecodeError::from(ChannelError::Decode(
                "Codex Responses SSE frame exceeds 100 MiB".into(),
            ))
            .prepend(output));
        }
        // Only delimiter bytes overlapping the next fragment need rescanning.
        self.scan_from = self.buffer.len().saturating_sub(3);
        Ok(output)
    }

    fn frame(&mut self, raw: &[u8]) -> Result<Vec<Frame>, StreamDecodeError> {
        let Some(frame) = parse(raw)? else {
            return Ok(Vec::new());
        };
        if frame.data.trim() == "[DONE]" {
            self.done_seen = true;
            self.replay_safe &= self.failure.failure().is_some();
            return Ok(Vec::new());
        }
        let (event, value) = diagnostic::decode(&frame.data, frame.event.as_deref())?;
        self.replay_safe &= replay_safe_event(&value, frame.event.as_deref());
        // Observe before normalizers synthesize snapshots or rewrite tools.
        if let Some(meter) = self.output_meter.as_mut() {
            meter.observe(&value);
        }
        self.failure.observe(frame.event.as_deref(), &value);
        let events = self
            .lifecycle
            .normalize(event)
            .map_err(ChannelError::Decode)?;
        self.emit(events, frame.event.as_deref())
    }

    fn emit(
        &mut self,
        events: Vec<ResponseStreamEvent>,
        fallback_event: Option<&str>,
    ) -> Result<Vec<Frame>, StreamDecodeError> {
        let mut output = Vec::new();
        for event in events {
            let events = match self.tools.normalize(event) {
                Ok(events) => events,
                Err(error) => return Err(StreamDecodeError::from(error).prepend(output)),
            };
            for event in events {
                if let ResponseStreamEvent::Known(known) = &event
                    && let Some(response) = event::response(known)
                {
                    if let Some(tier) = response.service_tier.as_ref() {
                        self.actual_service_tier = Some(tier.as_str().into());
                    }
                    if let Some(usage) = response.usage.as_ref() {
                        self.usage = Some(super::usage::from_response_with_tier(
                            usage,
                            response.service_tier.as_ref(),
                        ));
                    }
                }
                let name = event.event_name().or(fallback_event);
                let data = serde_json::to_string(&event)
                    .map_err(|error| ChannelError::Decode(error.to_string()))?;
                output.push(Frame(encode(name, &data)));
            }
        }
        Ok(output)
    }
}

impl StreamDecoder for CodexSseDecoder {
    fn replay_safe(&self) -> bool {
        self.replay_safe
    }

    fn terminal_failure(&self) -> Option<&gproxy_channel_api::UpstreamFailure> {
        self.failure.failure()
    }

    fn terminal_disposition(&self) -> Option<Disposition> {
        self.failure.disposition()
    }

    fn push(&mut self, chunk: Bytes) -> Result<Vec<Frame>, StreamDecodeError> {
        self.buffer.extend_from_slice(&chunk);
        self.drain()
    }

    fn finish(&mut self, end: StreamEnd) -> Result<StreamTail, StreamDecodeError> {
        if end == StreamEnd::Interrupted {
            self.buffer.clear();
            self.scan_from = 0;
            return Ok(StreamTail {
                estimated_output_chars: self.output_meter.take().map(|meter| meter.characters()),
                frames: Vec::new(),
                usage: self.usage.take(),
                actual_service_tier: self.actual_service_tier.take(),
            });
        }
        let mut frames = if self.buffer.is_empty() {
            Vec::new()
        } else {
            let raw = std::mem::take(&mut self.buffer);
            self.scan_from = 0;
            self.frame(&raw)?
        };
        if !self.lifecycle.is_terminal() {
            return Err(StreamDecodeError::from(ChannelError::Decode(
                "Codex Responses stream ended without a terminal response event".into(),
            ))
            .prepend(frames));
        }
        if self.done_seen {
            frames.push(Frame(encode(None, "[DONE]")));
        }
        Ok(StreamTail {
            estimated_output_chars: self.output_meter.take().map(|meter| meter.characters()),
            frames,
            usage: self.usage.take(),
            actual_service_tier: self.actual_service_tier.take(),
        })
    }

    fn recover_tail(&mut self) -> StreamTail {
        StreamTail {
            estimated_output_chars: self.output_meter.take().map(|meter| meter.characters()),
            usage: self.usage.take(),
            actual_service_tier: self.actual_service_tier.take(),
            ..Default::default()
        }
    }
}

fn replay_safe_event(value: &serde_json::Value, event: Option<&str>) -> bool {
    let event = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .or(event);
    if !matches!(
        event,
        Some(
            "response.created"
                | "response.queued"
                | "response.in_progress"
                | "response.failed"
                | "error"
        )
    ) {
        return false;
    }
    let response = value.get("response").unwrap_or(value);
    response
        .get("output")
        .is_none_or(|output| output.as_array().is_some_and(Vec::is_empty))
        && response.get("usage").is_none_or(serde_json::Value::is_null)
}
