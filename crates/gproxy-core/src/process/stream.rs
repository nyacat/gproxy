mod json_array;
mod sse;

use gproxy_channel_api::StreamDecodeError;
use std::sync::Arc;

use bytes::Bytes;
use gproxy_channel_api::{ChannelError, Frame, StreamDecoder, StreamEnd, StreamTail};

use self::json_array::JsonArrayCodec;
use self::sse::SseCodec;
use super::{CompiledRule, RuleModels, apply_response};

pub struct ResponseRuleDecoder {
    upstream: Option<Box<dyn StreamDecoder>>,
    codec: Codec,
    rules: Arc<[CompiledRule]>,
    operation: gproxy_protocol::OperationKey,
    primary_model: String,
    alternate_model: Option<String>,
    client_headers: http::HeaderMap,
    pending_tail: Option<StreamTail>,
}

enum Codec {
    Sse(SseCodec),
    JsonArray(JsonArrayCodec),
}

impl ResponseRuleDecoder {
    pub fn new(
        upstream: Option<Box<dyn StreamDecoder>>,
        rules: Arc<[CompiledRule]>,
        operation: gproxy_protocol::OperationKey,
        framing: gproxy_protocol::StreamFraming,
        models: RuleModels<'_>,
        client_headers: http::HeaderMap,
    ) -> Result<Self, ChannelError> {
        let codec = match framing {
            gproxy_protocol::StreamFraming::Sse => Codec::Sse(SseCodec::default()),
            gproxy_protocol::StreamFraming::JsonArray => {
                Codec::JsonArray(JsonArrayCodec::default())
            }
            gproxy_protocol::StreamFraming::WebSocket => {
                return Err(ChannelError::Decode(
                    "process response rules do not accept websocket framing".into(),
                ));
            }
        };
        let (primary_model, alternate_model) = models.owned();
        Ok(Self {
            upstream,
            codec,
            rules,
            operation,
            primary_model,
            alternate_model,
            client_headers,
            pending_tail: None,
        })
    }

    fn rewrite(&self, body: Bytes) -> Bytes {
        if body.as_ref() == b"[DONE]" {
            return body;
        }
        apply_response(
            &self.rules,
            self.operation,
            RuleModels::new(&self.primary_model, self.alternate_model.as_deref()),
            &self.client_headers,
            body,
        )
    }

    fn decode(&mut self, frames: Vec<Frame>) -> Result<Vec<Frame>, StreamDecodeError> {
        let mut output = Vec::new();
        for frame in frames {
            let decoded = match match &mut self.codec {
                Codec::Sse(codec) => codec.push(frame.0),
                Codec::JsonArray(codec) => codec.push(frame.0),
            } {
                Ok(frames) => frames,
                Err(error) => return Err(self.codec_error(error, output)),
            };
            for frame in decoded {
                output.push(Frame(frame.map(|body| self.rewrite(body))));
            }
        }
        Ok(output)
    }

    fn codec_error(&self, error: CodecError, mut frames: Vec<Frame>) -> StreamDecodeError {
        frames.extend(
            error
                .frames
                .into_iter()
                .map(|frame| Frame(frame.map(|body| self.rewrite(body)))),
        );
        StreamDecodeError::from(error.error).prepend(frames)
    }

    fn finish_frames(&mut self) -> Result<Vec<Frame>, StreamDecodeError> {
        let frames = match &mut self.codec {
            Codec::Sse(codec) => codec.finish(),
            Codec::JsonArray(codec) => codec.finish(),
        }
        .map_err(|error| self.codec_error(error, Vec::new()))?;
        Ok(frames
            .into_iter()
            .map(|frame| Frame(frame.map(|body| self.rewrite(body))))
            .collect())
    }

    fn failed_frames(&mut self, mut error: StreamDecodeError) -> StreamDecodeError {
        let mut frames = match self.decode(std::mem::take(&mut error.frames)) {
            Ok(frames) => frames,
            Err(error) => return error,
        };
        // Upstream EOF validation may fail after completing a final event
        // without its delimiter. Rewriting must not leave that event buffered.
        frames.extend(self.finish_frames().unwrap_or_else(|error| error.frames));
        error.prepend(frames)
    }
}

impl StreamDecoder for ResponseRuleDecoder {
    fn terminal_failure(&self) -> Option<&gproxy_channel_api::UpstreamFailure> {
        self.upstream
            .as_ref()
            .and_then(|decoder| decoder.terminal_failure())
    }

    fn terminal_disposition(&self) -> Option<gproxy_channel_api::Disposition> {
        self.upstream
            .as_ref()
            .and_then(|decoder| decoder.terminal_disposition())
    }

    fn push(&mut self, chunk: Bytes) -> Result<Vec<Frame>, StreamDecodeError> {
        let frames = match self.upstream.as_mut() {
            Some(upstream) => match upstream.push(chunk) {
                Ok(frames) => frames,
                Err(error) => return Err(self.failed_frames(error)),
            },
            None => vec![Frame(chunk)],
        };
        self.decode(frames)
    }

    fn finish(&mut self, end: StreamEnd) -> Result<StreamTail, StreamDecodeError> {
        let mut tail = match self.upstream.as_mut() {
            Some(upstream) => match upstream.finish(end) {
                Ok(tail) => tail,
                Err(mut error) => {
                    if end == StreamEnd::Interrupted {
                        error.frames.clear();
                        return Err(error);
                    }
                    return Err(self.failed_frames(error));
                }
            },
            None => StreamTail::default(),
        };
        if end == StreamEnd::Interrupted {
            tail.frames.clear();
            return Ok(tail);
        }
        let tail_frames = std::mem::take(&mut tail.frames);
        self.pending_tail = Some(tail);
        let mut frames = self.decode(tail_frames)?;
        match self.finish_frames() {
            Ok(tail) => frames.extend(tail),
            Err(error) => return Err(error.prepend(frames)),
        }
        let mut tail = self.pending_tail.take().expect("finished upstream tail");
        tail.frames = frames;
        Ok(tail)
    }

    fn recover_tail(&mut self) -> StreamTail {
        self.pending_tail.take().unwrap_or_else(|| {
            self.upstream
                .as_mut()
                .map_or_else(StreamTail::default, |upstream| upstream.recover_tail())
        })
    }
}

#[derive(Debug)]
enum EncodedFrame {
    Sse { event: Option<String>, data: Bytes },
    Json { prefix: &'static [u8], data: Bytes },
    Raw(Bytes),
}

#[derive(Debug)]
struct CodecError {
    error: ChannelError,
    frames: Vec<EncodedFrame>,
}

impl From<ChannelError> for CodecError {
    fn from(error: ChannelError) -> Self {
        Self {
            error,
            frames: Vec::new(),
        }
    }
}

impl EncodedFrame {
    fn map(self, rewrite: impl FnOnce(Bytes) -> Bytes) -> Bytes {
        match self {
            Self::Sse { event, data } => sse::encode(event.as_deref(), rewrite(data)),
            Self::Json { prefix, data } => {
                let mut output = Vec::with_capacity(prefix.len() + data.len());
                output.extend_from_slice(prefix);
                output.extend_from_slice(&rewrite(data));
                Bytes::from(output)
            }
            Self::Raw(body) => body,
        }
    }
}

#[cfg(test)]
mod partial_tests {
    use super::*;

    #[test]
    fn rules_preserve_frames_before_invalid_utf8_in_the_same_chunk() {
        let key = gproxy_protocol::OperationKey::content(
            gproxy_protocol::Operation::StreamGenerateContent,
            gproxy_protocol::ContentGenerationKind::OpenAiResponses,
        );
        let mut decoder = ResponseRuleDecoder::new(
            None,
            Arc::from([]),
            key,
            gproxy_protocol::StreamFraming::Sse,
            RuleModels::new("model", None),
            http::HeaderMap::new(),
        )
        .unwrap();
        let mut bytes =
            b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n".to_vec();
        bytes.extend_from_slice(b"data: \xff\n\n");
        let error = decoder.push(Bytes::from(bytes)).unwrap_err();
        assert_eq!(error.frames.len(), 1);
        assert!(
            std::str::from_utf8(&error.frames[0].0)
                .unwrap()
                .contains("hello")
        );
    }
}
