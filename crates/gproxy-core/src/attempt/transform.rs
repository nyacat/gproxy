use bytes::Bytes;
use gproxy_channel_api::StreamDecodeError;
use gproxy_channel_api::{ChannelError, Frame, StreamDecoder, StreamEnd, StreamTail};
use gproxy_protocol::{OperationKey, StreamFraming};

pub(crate) struct TransformDecoder {
    upstream: Option<Box<dyn StreamDecoder>>,
    converter: gproxy_transform::ResponseStream,
    pending_tail: Option<StreamTail>,
}

impl TransformDecoder {
    pub(crate) fn new(
        source: OperationKey,
        target: OperationKey,
        source_framing: StreamFraming,
        target_framing: StreamFraming,
        upstream: Option<Box<dyn StreamDecoder>>,
    ) -> Self {
        Self {
            upstream,
            converter: gproxy_transform::response_stream_framed(
                source,
                target,
                source_framing,
                target_framing,
            )
            .expect("declared streaming transform remains wired"),
            pending_tail: None,
        }
    }

    fn convert(&mut self, frames: Vec<Frame>) -> Result<Vec<Frame>, StreamDecodeError> {
        let mut output = Vec::new();
        for frame in frames {
            match self.converter.push(frame.0) {
                Ok(frames) => output.extend(frames.into_iter().map(Frame)),
                Err(error) => return Err(decode(error).prepend(output)),
            }
        }
        Ok(output)
    }

    fn failed_frames(&mut self, mut error: StreamDecodeError) -> StreamDecodeError {
        let mut frames = match self.convert(std::mem::take(&mut error.frames)) {
            Ok(frames) => frames,
            Err(error) => return error,
        };
        // Error frames are complete semantic input, but their wire framing may
        // omit the last SSE delimiter or JSON-array bracket. Flush content
        // without asking the converter to invent successful terminal events.
        let tail = self
            .converter
            .finish_prefix()
            .unwrap_or_else(|error| error.frames);
        frames.extend(tail.into_iter().map(Frame));
        error.prepend(frames)
    }
}

impl StreamDecoder for TransformDecoder {
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
        self.convert(frames)
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
            return Ok(StreamTail {
                estimated_output_chars: tail.estimated_output_chars,
                frames: Vec::new(),
                usage: tail.usage,
                actual_service_tier: tail.actual_service_tier,
            });
        }
        let tail_frames = std::mem::take(&mut tail.frames);
        self.pending_tail = Some(tail);
        let mut frames = self.convert(tail_frames)?;
        match self.converter.finish() {
            Ok(tail) => frames.extend(tail.into_iter().map(Frame)),
            Err(error) => return Err(decode(error).prepend(frames)),
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

fn decode(error: gproxy_transform::StreamTransformError) -> StreamDecodeError {
    StreamDecodeError::from(ChannelError::Decode(error.error.to_string()))
        .prepend(error.frames.into_iter().map(Frame).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gproxy_channel_api::NormalizedUsage;
    use gproxy_protocol::{ContentGenerationKind, Operation};

    struct FinalFrame;

    impl StreamDecoder for FinalFrame {
        fn push(&mut self, _: Bytes) -> Result<Vec<Frame>, StreamDecodeError> {
            Ok(Vec::new())
        }

        fn finish(&mut self, _: StreamEnd) -> Result<StreamTail, StreamDecodeError> {
            Ok(StreamTail {
                estimated_output_chars: None,
                frames: vec![Frame(Bytes::from_static(b"data: {}\n\n"))],
                usage: Some(NormalizedUsage {
                    input_tokens: 13,
                    output_tokens: 7,
                    ..Default::default()
                }),
                actual_service_tier: Some("priority".into()),
            })
        }
    }

    #[test]
    fn final_frame_conversion_failure_returns_metadata_only_once() {
        let key = |kind| OperationKey::content(Operation::StreamGenerateContent, kind);
        let mut decoder = TransformDecoder::new(
            key(ContentGenerationKind::ClaudeMessages),
            key(ContentGenerationKind::OpenAiChat),
            StreamFraming::Sse,
            StreamFraming::Sse,
            Some(Box::new(FinalFrame)),
        );
        let error = decoder.finish(StreamEnd::Complete).unwrap_err();
        assert!(error.to_string().contains("missing field"), "{error}");
        let tail = decoder.recover_tail();
        assert!(tail.frames.is_empty());
        assert_eq!(tail.usage.unwrap().input_tokens, 13);
        assert_eq!(tail.actual_service_tier.as_deref(), Some("priority"));
        let tail = decoder.recover_tail();
        assert!(tail.usage.is_none());
        assert!(tail.actual_service_tier.is_none());
        assert!(tail.frames.is_empty());
    }

    #[test]
    fn gemini_unterminated_final_event_survives_rules_and_transform_before_error() {
        use gproxy_channel_api::{Channel, StreamCtx};
        use std::sync::Arc;

        let key = |kind| OperationKey::content(Operation::StreamGenerateContent, kind);
        let target = key(ContentGenerationKind::GeminiGenerateContent);
        let value = br#"{"responseId":"r1","modelVersion":"gemini","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"hello"}]}}],"usageMetadata":{"promptTokenCount":7,"candidatesTokenCount":3,"totalTokenCount":10}}"#;
        for framing in [StreamFraming::Sse, StreamFraming::JsonArray] {
            let wire = [
                if framing == StreamFraming::Sse {
                    b"data: ".as_slice()
                } else {
                    b"["
                },
                value,
            ]
            .concat();
            for rules in [false, true] {
                for split in 0..=wire.len() {
                    let headers = http::HeaderMap::new();
                    let request = Bytes::new();
                    let mut upstream = gproxy_channels::AiStudioChannel.stream_decoder(StreamCtx {
                        key: target,
                        framing,
                        request_body: &request,
                        response_headers: &headers,
                    });
                    if rules {
                        upstream = Some(Box::new(
                            crate::process::ResponseRuleDecoder::new(
                                upstream,
                                Arc::from([]),
                                target,
                                framing,
                                crate::process::RuleModels::new("gemini", None),
                                headers,
                            )
                            .unwrap(),
                        ));
                    }
                    let mut decoder = TransformDecoder::new(
                        key(ContentGenerationKind::OpenAiChat),
                        target,
                        StreamFraming::Sse,
                        framing,
                        upstream,
                    );
                    let mut output = Vec::new();
                    for chunk in [&wire[..split], &wire[split..]] {
                        output.extend(decoder.push(Bytes::copy_from_slice(chunk)).unwrap());
                    }
                    let error = decoder.finish(StreamEnd::Complete).unwrap_err();
                    assert!(error.to_string().contains("ended"), "{error}");
                    output.extend(error.frames);
                    let text = String::from_utf8(
                        output
                            .into_iter()
                            .flat_map(|frame| frame.0.to_vec())
                            .collect(),
                    )
                    .unwrap();
                    assert_eq!(
                        text.matches("hello").count(),
                        1,
                        "framing={framing:?}, rules={rules}, split={split}: {text}"
                    );
                    assert!(
                        !text.contains("[DONE]"),
                        "must not synthesize success: {text}"
                    );
                    assert!(!text.contains("\"finish_reason\":\"stop\""), "{text}");
                    let tail = decoder.recover_tail();
                    let usage = tail.usage.unwrap();
                    assert_eq!((usage.input_tokens, usage.output_tokens), (7, 3));
                    assert!(tail.frames.is_empty());
                    assert!(decoder.recover_tail().usage.is_none());
                }
            }
        }
    }
}
