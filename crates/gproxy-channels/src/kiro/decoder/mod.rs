mod events;
mod terminal;

use bytes::Bytes;
use gproxy_channel_api::StreamDecodeError;
use gproxy_channel_api::{ChannelError, Frame, StreamCtx, StreamDecoder, StreamEnd, StreamTail};
use gproxy_protocol::{ContentGenerationKind, Operation, OperationKind};
use serde_json::{Value, json};

pub(super) struct KiroDecoder {
    parser: crate::shared::aws_eventstream::FrameParser,
    response_id: String,
    message_id: String,
    reasoning_id: String,
    model: String,
    content: String,
    reasoning: String,
    last_content: String,
    last_reasoning: String,
    usage: Option<gproxy_channel_api::NormalizedUsage>,
    started: bool,
    content_started: bool,
    reasoning_started: bool,
    pub(super) failure: gproxy_channel_api::FailureState,
    tools: super::tool_stream::Tracker,
    sequence: u64,
}

impl KiroDecoder {
    pub(super) fn for_operation(ctx: StreamCtx<'_>) -> Option<Self> {
        let supported = ctx.key.operation() == Operation::StreamGenerateContent
            && ctx.key.kind()
                == OperationKind::ContentGeneration(ContentGenerationKind::OpenAiResponses);
        supported.then(|| {
            let request: Value = serde_json::from_slice(ctx.request_body)
                .expect("prepared Kiro request remains valid JSON");
            let response_id = request
                .pointer("/conversationState/conversationId")
                .and_then(Value::as_str)
                .expect("prepared Kiro request has a conversation id")
                .to_owned();
            let model = request
                .pointer("/conversationState/currentMessage/userInputMessage/modelId")
                .and_then(Value::as_str)
                .expect("prepared Kiro request has a model id")
                .to_owned();
            Self {
                message_id: super::sse::id("msg", &response_id),
                reasoning_id: super::sse::id("rs", &response_id),
                response_id,
                model,
                parser: crate::shared::aws_eventstream::FrameParser::new(),
                content: String::new(),
                reasoning: String::new(),
                last_content: String::new(),
                last_reasoning: String::new(),
                usage: None,
                started: false,
                content_started: false,
                reasoning_started: false,
                failure: gproxy_channel_api::FailureState::new("kiro", ctx.response_headers),
                tools: Default::default(),
                sequence: 0,
            }
        })
    }

    fn ensure_started(&mut self, output: &mut Vec<Frame>) {
        if self.started {
            return;
        }
        self.started = true;
        let sequence = self.take();
        let response =
            super::sse::response(&self.response_id, &self.model, "in_progress", Vec::new());
        output.push(super::sse::frame(json!({
            "type":"response.created","sequence_number":sequence,"response":response
        })));
    }

    fn take(&mut self) -> u64 {
        let sequence = self.sequence;
        self.sequence += 1;
        sequence
    }
}

impl StreamDecoder for KiroDecoder {
    fn terminal_failure(&self) -> Option<&gproxy_channel_api::UpstreamFailure> {
        self.failure.failure()
    }

    fn push(&mut self, chunk: Bytes) -> Result<Vec<Frame>, StreamDecodeError> {
        let parsed = self.parser.push(chunk);
        let mut output = Vec::new();
        for frame in parsed.frames {
            if let Err(error) = events::handle(self, frame, &mut output) {
                return Err(StreamDecodeError::from(error).prepend(output));
            }
        }
        if let Some(error) = parsed.error {
            return Err(StreamDecodeError::from(error).prepend(output));
        }
        Ok(output)
    }

    fn finish(&mut self, end: StreamEnd) -> Result<StreamTail, StreamDecodeError> {
        if end == StreamEnd::Interrupted {
            return Ok(StreamTail {
                estimated_output_chars: None,
                frames: Vec::new(),
                usage: self.usage.take(),
                actual_service_tier: None,
            });
        }
        self.parser.finish()?;
        if self.failure.failure().is_some() {
            return Ok(StreamTail {
                usage: self.usage.take(),
                ..Default::default()
            });
        }
        if !self.tools.is_complete() {
            return Err(ChannelError::Decode(
                "Kiro stream ended before a tool call stopped".into(),
            )
            .into());
        }
        if !self.started {
            return Err(ChannelError::Decode("Kiro stream produced no events".into()).into());
        }
        let frames = terminal::finish(self);
        let usage = self.usage.take();
        Ok(StreamTail {
            estimated_output_chars: None,
            frames,
            usage,
            actual_service_tier: None,
        })
    }

    fn recover_tail(&mut self) -> StreamTail {
        StreamTail {
            usage: self.usage.take(),
            ..Default::default()
        }
    }
}
