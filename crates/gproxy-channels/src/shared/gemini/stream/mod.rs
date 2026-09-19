mod json_array;
mod sse;

use gproxy_channel_api::StreamDecodeError;
use std::collections::BTreeMap;

use bytes::Bytes;
use gproxy_channel_api::{ChannelError, Frame, StreamCtx, StreamDecoder, StreamEnd, StreamTail};
use gproxy_protocol::gemini::{
    BlockReason, BlockReasonKnown, FinishReason, FinishReasonKnown, GenerateContentResponse,
};
use gproxy_protocol::{ContentGenerationKind, Operation, OperationKind, StreamFraming};

pub(crate) struct GeminiStreamDecoder {
    parser: Parser,
    failure: gproxy_channel_api::FailureState,
    terminal: Terminal,
    response_tier: Option<String>,
    usage: Option<gproxy_channel_api::NormalizedUsage>,
}

enum Parser {
    Sse(sse::Decoder),
    JsonArray(json_array::Decoder),
}

struct ParsedChunk {
    raw: Bytes,
    value: Option<serde_json::Value>,
}

impl GeminiStreamDecoder {
    pub(crate) fn for_operation(ctx: StreamCtx<'_>) -> Option<Self> {
        if ctx.key.operation() != Operation::StreamGenerateContent
            || ctx.key.kind()
                != OperationKind::ContentGeneration(ContentGenerationKind::GeminiGenerateContent)
        {
            return None;
        }
        let parser = match ctx.framing {
            StreamFraming::Sse => Parser::Sse(sse::Decoder::default()),
            StreamFraming::JsonArray => Parser::JsonArray(json_array::Decoder::default()),
            StreamFraming::WebSocket => return None,
        };
        Some(Self {
            parser,
            failure: gproxy_channel_api::FailureState::new("gemini", ctx.response_headers),
            terminal: Terminal::default(),
            response_tier: super::usage::response_tier(ctx.response_headers),
            usage: None,
        })
    }

    fn drain(&mut self, eof: bool) -> Result<Vec<Frame>, StreamDecodeError> {
        let mut output = Vec::new();
        loop {
            let parsed = match &mut self.parser {
                Parser::Sse(parser) => parser.next(eof),
                Parser::JsonArray(parser) => parser.next(eof),
            };
            let chunk = match parsed {
                Ok(Some(chunk)) => chunk,
                Ok(None) => return Ok(output),
                Err(error) => return Err(StreamDecodeError::from(error).prepend(output)),
            };
            if let Some(value) = chunk.value
                && let Err(error) = self.observe(value)
            {
                return Err(StreamDecodeError::from(error).prepend(output));
            }
            output.push(Frame(chunk.raw));
        }
    }

    fn observe(&mut self, value: serde_json::Value) -> Result<(), ChannelError> {
        self.failure.observe(None, &value);
        if value.get("error").is_some_and(|error| !error.is_null()) {
            return Ok(());
        }
        let chunk: GenerateContentResponse = serde_json::from_value(value)
            .map_err(|_| ChannelError::Decode("invalid Gemini content response".into()))?;
        self.terminal.observe(&chunk)?;
        if let Some(metadata) = chunk.usage_metadata.as_ref() {
            // Code Assist can attach prompt-only usage to early thinking
            // and tool-call frames. Only complete usage replaces the previous
            // cumulative sample, after the event itself has been validated.
            let usage = (metadata.prompt_token_count.is_some()
                && metadata.candidates_token_count.is_some())
            .then(|| super::usage::normalize(metadata))
            .transpose()
            .map_err(|error| ChannelError::Decode(format!("Gemini usage: {error}")))?;
            if self.response_tier.is_none() {
                self.response_tier = metadata
                    .service_tier
                    .as_ref()
                    .and_then(super::usage::tier_name);
            }
            if let Some(mut usage) = usage {
                if let Some(tier) = self.response_tier.as_ref() {
                    usage.dimensions.insert("service_tier".into(), tier.clone());
                }
                self.usage = Some(usage);
            }
        }
        Ok(())
    }
}

impl StreamDecoder for GeminiStreamDecoder {
    fn terminal_failure(&self) -> Option<&gproxy_channel_api::UpstreamFailure> {
        self.failure.failure()
    }

    fn push(&mut self, chunk: Bytes) -> Result<Vec<Frame>, StreamDecodeError> {
        let pending = match &mut self.parser {
            Parser::Sse(parser) => {
                let pending = parser.pending_len();
                parser.push(&chunk);
                pending
            }
            Parser::JsonArray(parser) => {
                let pending = parser.pending_len();
                parser.push(&chunk);
                pending
            }
        };
        let mut result = self.drain(false);
        // A transport chunk containing complete events still travels without
        // reframing or copying. Only an event spanning transport chunks needs
        // the parser's assembled bytes; malformed suffixes are never forwarded.
        if pending == 0 {
            let frames = match &mut result {
                Ok(frames) => frames,
                Err(error) => &mut error.frames,
            };
            if !frames.is_empty() {
                let length: usize = frames.iter().map(|frame| frame.0.len()).sum();
                *frames = vec![Frame(chunk.slice(..length))];
            }
        }
        result
    }

    fn finish(&mut self, end: StreamEnd) -> Result<StreamTail, StreamDecodeError> {
        if end == StreamEnd::Interrupted {
            return Ok(StreamTail {
                estimated_output_chars: None,
                frames: Vec::new(),
                usage: self.usage.take(),
                actual_service_tier: self.response_tier.take(),
            });
        }
        let frames = self.drain(true)?;
        if let Parser::JsonArray(parser) = &self.parser
            && let Err(error) = parser.finish()
        {
            return Err(StreamDecodeError::from(error).prepend(frames));
        }
        if self.failure.failure().is_none() && !self.terminal.is_complete() {
            return Err(StreamDecodeError::from(ChannelError::Decode(
                "Gemini stream ended without terminal candidate or block reason".into(),
            ))
            .prepend(frames));
        }
        Ok(StreamTail {
            estimated_output_chars: None,
            frames,
            usage: self.usage.take(),
            actual_service_tier: self.response_tier.take(),
        })
    }

    fn recover_tail(&mut self) -> StreamTail {
        StreamTail {
            usage: self.usage.take(),
            actual_service_tier: self.response_tier.take(),
            ..Default::default()
        }
    }
}

#[derive(Default)]
struct Terminal {
    candidates: BTreeMap<i32, bool>,
    blocked: bool,
    response_id: Option<String>,
    model_version: Option<String>,
}

impl Terminal {
    fn observe(&mut self, chunk: &GenerateContentResponse) -> Result<(), ChannelError> {
        set_identity(
            &mut self.response_id,
            chunk.response_id.as_ref(),
            "responseId",
        )?;
        set_identity(
            &mut self.model_version,
            chunk.model_version.as_ref(),
            "modelVersion",
        )?;
        for (fallback, candidate) in chunk.candidates.iter().enumerate() {
            let index = match candidate.index {
                Some(index) if index >= 0 => index,
                Some(_) => {
                    return Err(ChannelError::Decode(
                        "Gemini candidate index is negative".into(),
                    ));
                }
                None => i32::try_from(fallback).map_err(|_| {
                    ChannelError::Decode("Gemini candidate index exceeds i32".into())
                })?,
            };
            if self.candidates.get(&index).copied() == Some(true) {
                return Err(ChannelError::Decode(
                    "Gemini candidate data followed finishReason".into(),
                ));
            }
            if matches!(
                candidate.finish_reason.as_ref(),
                Some(FinishReason::Known(
                    FinishReasonKnown::FinishReasonUnspecified
                ))
            ) {
                return Err(ChannelError::Decode(
                    "Gemini candidate finishReason is unspecified".into(),
                ));
            }
            self.candidates
                .insert(index, candidate.finish_reason.is_some());
        }
        let block_reason = chunk
            .prompt_feedback
            .as_ref()
            .and_then(|feedback| feedback.block_reason.as_ref());
        if matches!(
            block_reason,
            Some(BlockReason::Known(BlockReasonKnown::BlockReasonUnspecified))
        ) {
            return Err(ChannelError::Decode(
                "Gemini prompt blockReason is unspecified".into(),
            ));
        }
        if block_reason.is_some() {
            self.blocked = true;
        }
        Ok(())
    }

    fn is_complete(&self) -> bool {
        (!self.candidates.is_empty() && self.candidates.values().all(|finished| *finished))
            || (self.candidates.is_empty() && self.blocked)
    }
}

fn set_identity(
    target: &mut Option<String>,
    update: Option<&String>,
    field: &'static str,
) -> Result<(), ChannelError> {
    if let Some(update) = update {
        if target.as_ref().is_some_and(|current| current != update) {
            return Err(ChannelError::Decode(format!(
                "Gemini {field} changed during the stream"
            )));
        }
        *target = Some(update.clone());
    }
    Ok(())
}
