mod blocks;
mod decode;
mod events;
mod finish;
pub(super) mod invoke;
mod wire;

use bytes::Bytes;
use gproxy_channel_api::StreamDecodeError;
use gproxy_channel_api::{ChannelError, Frame, StreamDecoder, StreamEnd, StreamTail};

pub(super) struct BedrockStreamDecoder {
    parser: crate::shared::aws_eventstream::FrameParser,
    state: events::State,
    failure: gproxy_channel_api::FailureState,
}

impl BedrockStreamDecoder {
    pub(super) fn with_headers(mut self, headers: &http::HeaderMap) -> Self {
        self.failure = gproxy_channel_api::FailureState::new("bedrock", headers);
        self
    }

    pub(super) fn new() -> Self {
        Self {
            parser: crate::shared::aws_eventstream::FrameParser::new(),
            state: events::State::default(),
            failure: gproxy_channel_api::FailureState::new("bedrock", &http::HeaderMap::new()),
        }
    }

    fn frame(
        &mut self,
        frame: crate::shared::aws_eventstream::Frame,
    ) -> Result<Vec<Frame>, ChannelError> {
        let kind = frame
            .exception_type
            .as_deref()
            .or(frame.event_type.as_deref())
            .unwrap_or_default();
        if frame.exception_type.is_some()
            || frame.message_type.as_deref() == Some("exception")
            || kind.ends_with("Exception")
        {
            let value: serde_json::Value = serde_json::from_slice(&frame.payload)
                .map_err(|_| ChannelError::Decode("invalid Bedrock exception JSON".into()))?;
            self.failure.exception(kind, &value);
            return Ok(vec![wire::error(
                kind,
                value
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("Bedrock stream failed"),
            )?]);
        }
        self.state.handle(decode::frame(frame)?)
    }
}

impl StreamDecoder for BedrockStreamDecoder {
    fn terminal_failure(&self) -> Option<&gproxy_channel_api::UpstreamFailure> {
        self.failure.failure()
    }

    fn push(&mut self, chunk: Bytes) -> Result<Vec<Frame>, StreamDecodeError> {
        let parsed = self.parser.push(chunk);
        let mut output = Vec::new();
        for frame in parsed.frames {
            match self.frame(frame) {
                Ok(frames) => output.extend(frames),
                Err(error) => return Err(StreamDecodeError::from(error).prepend(output)),
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
                usage: self.state.normalized.take(),
                actual_service_tier: None,
            });
        }
        self.parser.finish()?;
        if self.failure.failure().is_none()
            && (!self.state.terminal
                || !self.state.started
                || !self.state.message_stopped
                || !self.state.metadata_seen)
        {
            return Err(ChannelError::Decode(
                "Bedrock stream ended before messageStop and metadata".into(),
            )
            .into());
        }
        Ok(StreamTail {
            estimated_output_chars: None,
            frames: Vec::new(),
            usage: self.state.normalized.take(),
            actual_service_tier: None,
        })
    }

    fn recover_tail(&mut self) -> StreamTail {
        StreamTail {
            usage: self.state.normalized.take(),
            ..Default::default()
        }
    }
}
