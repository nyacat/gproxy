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
                Err(error) => return Err(StreamDecodeError::from(decode(error)).prepend(output)),
            }
        }
        Ok(output)
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
                Err(mut error) => {
                    let frames = self.convert(std::mem::take(&mut error.frames))?;
                    return Err(error.prepend(frames));
                }
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
                    let frames = self.convert(std::mem::take(&mut error.frames))?;
                    return Err(error.prepend(frames));
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
            Err(error) => return Err(StreamDecodeError::from(decode(error)).prepend(frames)),
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

fn decode(error: gproxy_transform::TransformError) -> ChannelError {
    ChannelError::Decode(error.to_string())
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
}
