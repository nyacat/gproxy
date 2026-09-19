use base64::Engine;
use bytes::Bytes;
use gproxy_channel_api::StreamDecodeError;
use gproxy_channel_api::{ChannelError, Frame, StreamCtx, StreamDecoder, StreamEnd, StreamTail};
use serde_json::Value;

pub(in crate::aws_bedrock) struct InvokeDecoder {
    parser: crate::shared::aws_eventstream::FrameParser,
    usage: crate::shared::claude::sse::ClaudeSseDecoder,
    stopped: bool,
    failure: gproxy_channel_api::FailureState,
}

impl InvokeDecoder {
    pub(in crate::aws_bedrock) fn new(ctx: StreamCtx<'_>) -> Self {
        Self {
            parser: Default::default(),
            failure: gproxy_channel_api::FailureState::new("bedrock_invoke", ctx.response_headers),
            usage: crate::shared::claude::sse::ClaudeSseDecoder::for_operation(ctx)
                .expect("Claude stream"),
            stopped: false,
        }
    }

    fn frame(
        &mut self,
        frame: crate::shared::aws_eventstream::Frame,
    ) -> Result<Vec<Frame>, StreamDecodeError> {
        let kind = frame
            .exception_type
            .as_deref()
            .or(frame.event_type.as_deref())
            .unwrap_or_default();
        if frame.exception_type.is_some()
            || frame.message_type.as_deref() == Some("exception")
            || kind.ends_with("Exception")
        {
            let value: Value = serde_json::from_slice(&frame.payload)
                .map_err(|_| ChannelError::Decode("invalid Bedrock exception JSON".into()))?;
            self.failure.exception(kind, &value);
            self.stopped = true;
            return Ok(vec![super::wire::error(
                kind,
                value
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("Bedrock stream failed"),
            )?]);
        }
        if frame.event_type.as_deref() != Some("chunk") {
            return Ok(Vec::new());
        }
        let payload: Value = serde_json::from_slice(&frame.payload)
            .map_err(|error| ChannelError::Decode(error.to_string()))?;
        let encoded = payload
            .get("bytes")
            .and_then(Value::as_str)
            .ok_or_else(|| ChannelError::Decode("Bedrock chunk has no bytes".into()))?;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|error| ChannelError::Decode(error.to_string()))?;
        let event: Value = serde_json::from_slice(&decoded)
            .map_err(|error| ChannelError::Decode(error.to_string()))?;
        self.stopped |= event["type"] == "message_stop";
        self.usage.push(Bytes::from(format!("data: {event}\n\n")))
    }
}

impl StreamDecoder for InvokeDecoder {
    fn terminal_failure(&self) -> Option<&gproxy_channel_api::UpstreamFailure> {
        self.failure
            .failure()
            .or_else(|| self.usage.terminal_failure())
    }

    fn push(&mut self, chunk: Bytes) -> Result<Vec<Frame>, StreamDecodeError> {
        let parsed = self.parser.push(chunk);
        let mut output = Vec::new();
        for frame in parsed.frames {
            match self.frame(frame) {
                Ok(frames) => output.extend(frames),
                Err(error) => return Err(error.prepend(output)),
            }
        }
        if let Some(error) = parsed.error {
            return Err(StreamDecodeError::from(error).prepend(output));
        }
        Ok(output)
    }

    fn finish(&mut self, end: StreamEnd) -> Result<StreamTail, StreamDecodeError> {
        if end == StreamEnd::Complete {
            self.parser.finish()?;
            if !self.stopped && self.terminal_failure().is_none() {
                return Err(ChannelError::Decode(
                    "Bedrock stream ended before message_stop".into(),
                )
                .into());
            }
        }
        self.usage.finish(end)
    }

    fn recover_tail(&mut self) -> StreamTail {
        self.usage.recover_tail()
    }
}
