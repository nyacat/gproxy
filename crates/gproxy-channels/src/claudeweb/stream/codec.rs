use bytes::Bytes;
use gproxy_channel_api::{
    ChannelError, Frame, OperationStream, Pause, StreamDecodeError, StreamEnd, StreamOutput,
};
use serde_json::{Value, json};

use super::SessionState;
use super::sse::{Decoder, encode};

pub(in crate::claudeweb) struct Codec {
    pub(super) decoder: Decoder,
    pub(super) state: SessionState,
    pub(super) output: String,
    pub(super) tool_id: Option<String>,
    pub(super) tool_index: Option<u64>,
    pub(super) skipped_result: Option<u64>,
    pub(super) legacy: bool,
    pub(super) started: bool,
    pub(super) stopped: bool,
    failure: gproxy_channel_api::FailureState,
    pub(super) resume_start: bool,
}

impl Codec {
    pub(in crate::claudeweb) fn new(state: SessionState, resume: bool) -> Self {
        Self {
            decoder: Decoder::default(),
            state,
            output: String::new(),
            tool_id: None,
            tool_index: None,
            skipped_result: None,
            legacy: false,
            started: false,
            stopped: false,
            failure: gproxy_channel_api::FailureState::new("claudeweb", &http::HeaderMap::new()),
            resume_start: resume,
        }
    }

    fn events(&mut self, eof: bool) -> Result<StreamOutput, StreamDecodeError> {
        let mut frames = Vec::new();
        if self.resume_start {
            self.resume_start = false;
            self.started = true;
            frames.push(Frame(self.message_start()));
        }
        loop {
            let event = match self.decoder.next(eof) {
                Ok(Some(event)) => event,
                Ok(None) => break,
                Err(error) => return Err(StreamDecodeError::from(error).prepend(frames)),
            };
            let Some(mut value) = event.value else {
                frames.push(Frame(event.raw));
                continue;
            };
            self.failure.observe(None, &value);
            if self.failure.failure().is_some() {
                frames.push(Frame(encode(&value)));
                self.stopped = true;
                continue;
            }
            if value.get("type").is_none() {
                self.legacy = true;
                self.legacy_event(&value, &mut frames);
                continue;
            }
            let pause = match self.modern(&mut value, &mut frames) {
                Ok(pause) => pause,
                Err(error) => return Err(StreamDecodeError::from(error).prepend(frames)),
            };
            if let Some(pause) = pause {
                let pending = self.decoder.take_pending().into_iter().collect();
                let state = match serde_json::to_value(&self.state) {
                    Ok(state) => state,
                    Err(error) => {
                        return Err(StreamDecodeError::from(ChannelError::Decode(
                            error.to_string(),
                        ))
                        .prepend(frames));
                    }
                };
                return Ok(StreamOutput {
                    frames,
                    pause: Some(Pause {
                        id: pause,
                        state,
                        pending,
                    }),
                });
            }
        }
        Ok(StreamOutput::frames(frames))
    }

    fn legacy_event(&mut self, value: &Value, frames: &mut Vec<Frame>) {
        if !self.started {
            self.started = true;
            frames.push(Frame(self.message_start()));
            frames.push(Frame(encode(&json!({
                "type":"content_block_start","index":0,
                "content_block":{"type":"text","text":""}
            }))));
        }
        if let Some(delta) = value.get("completion").and_then(Value::as_str) {
            self.output.push_str(delta);
            frames.push(Frame(encode(&json!({
                "type":"content_block_delta","index":0,
                "delta":{"type":"text_delta","text":delta}
            }))));
        }
    }

    pub(super) fn message_start(&self) -> Bytes {
        encode(&json!({
            "type":"message_start",
            "message":{"id":self.state.message_id,"type":"message","role":"assistant",
                "content":[],"model":self.state.model,"stop_reason":null,"stop_sequence":null,
                "usage":{"input_tokens":self.state.input_tokens,"output_tokens":0}}
        }))
    }

    pub(super) fn output_tokens(&self) -> u64 {
        u64::try_from(self.output.chars().count())
            .unwrap_or(u64::MAX)
            .div_ceil(4)
    }
}

impl OperationStream for Codec {
    fn terminal_failure(&self) -> Option<&gproxy_channel_api::UpstreamFailure> {
        self.failure.failure()
    }

    fn push(&mut self, chunk: Bytes) -> Result<StreamOutput, StreamDecodeError> {
        self.decoder.push(&chunk)?;
        self.events(false)
    }

    fn finish(&mut self, end: StreamEnd) -> Result<Vec<Frame>, StreamDecodeError> {
        if end == StreamEnd::Interrupted {
            return Ok(Vec::new());
        }
        let mut output = self.events(true)?.frames;
        if self.legacy && !self.stopped {
            output.push(Frame(encode(
                &json!({"type":"content_block_stop","index":0}),
            )));
        }
        if !self.stopped {
            output.push(Frame(encode(&json!({
                "type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},
                "usage":{"output_tokens":self.output_tokens()}
            }))));
            output.push(Frame(encode(&json!({"type":"message_stop"}))));
            self.stopped = true;
        }
        Ok(output)
    }
}
