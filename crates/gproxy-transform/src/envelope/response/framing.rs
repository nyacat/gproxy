use bytes::Bytes;
use gproxy_protocol::StreamFraming;

use super::super::json_array::{JsonArrayDecoder, JsonArrayEncoder};
use super::super::{SseDecoder, SseFrame};
use crate::{StreamTransformError, TransformError};

pub(super) enum FrameDecoder {
    Sse(SseDecoder),
    JsonArray(JsonArrayDecoder),
}

pub(super) enum FrameEncoder {
    Sse,
    JsonArray {
        decoder: SseDecoder,
        encoder: JsonArrayEncoder,
        done: bool,
    },
}

impl FrameDecoder {
    pub(super) fn new(framing: StreamFraming) -> Result<Self, TransformError> {
        match framing {
            StreamFraming::Sse => Ok(Self::Sse(SseDecoder::default())),
            StreamFraming::JsonArray => Ok(Self::JsonArray(JsonArrayDecoder::default())),
            StreamFraming::WebSocket => Err(TransformError::shape(
                "stream framing",
                "websocket framing requires a websocket transform",
            )),
        }
    }

    pub(super) fn push(
        &mut self,
        chunk: &[u8],
    ) -> Result<Vec<SseFrame>, StreamTransformError<SseFrame>> {
        match self {
            Self::Sse(decoder) => decoder.push(chunk),
            Self::JsonArray(decoder) => decoder.push(chunk),
        }
    }

    pub(super) fn finish(&mut self) -> Result<Vec<SseFrame>, StreamTransformError<SseFrame>> {
        match self {
            Self::Sse(decoder) => Ok(decoder.finish()?.into_iter().collect()),
            Self::JsonArray(decoder) => decoder.finish(),
        }
    }
}

impl FrameEncoder {
    pub(super) fn new(framing: StreamFraming) -> Result<Self, TransformError> {
        match framing {
            StreamFraming::Sse => Ok(Self::Sse),
            StreamFraming::JsonArray => Ok(Self::JsonArray {
                decoder: SseDecoder::default(),
                encoder: JsonArrayEncoder::default(),
                done: false,
            }),
            StreamFraming::WebSocket => Err(TransformError::shape(
                "stream framing",
                "websocket framing requires a websocket transform",
            )),
        }
    }

    pub(super) fn push(&mut self, chunks: Vec<Bytes>) -> Result<Vec<Bytes>, StreamTransformError> {
        match self {
            Self::Sse => Ok(chunks),
            Self::JsonArray {
                decoder,
                encoder,
                done,
            } => {
                let mut output = Vec::new();
                for chunk in chunks {
                    let (frames, failure) = match decoder.push(&chunk) {
                        Ok(frames) => (frames, None),
                        Err(error) => (error.frames, Some(error.error)),
                    };
                    for frame in frames {
                        if let Err(error) = encode_array_frame(encoder, done, frame, &mut output) {
                            return Err(StreamTransformError {
                                error,
                                frames: output,
                            });
                        }
                    }
                    if let Some(error) = failure {
                        return Err(StreamTransformError {
                            error,
                            frames: output,
                        });
                    }
                }
                Ok(output)
            }
        }
    }

    pub(super) fn finish(&mut self) -> Result<Vec<Bytes>, StreamTransformError> {
        match self {
            Self::Sse => Ok(Vec::new()),
            Self::JsonArray {
                decoder,
                encoder,
                done,
            } => {
                let mut output = Vec::new();
                if let Some(frame) = decoder.finish()?
                    && let Err(error) = encode_array_frame(encoder, done, frame, &mut output)
                {
                    return Err(StreamTransformError {
                        error,
                        frames: output,
                    });
                }
                match encoder.finish() {
                    Ok(frame) => output.push(frame),
                    Err(error) => {
                        return Err(StreamTransformError {
                            error,
                            frames: output,
                        });
                    }
                }
                Ok(output)
            }
        }
    }
}

fn encode_array_frame(
    encoder: &mut JsonArrayEncoder,
    done: &mut bool,
    frame: SseFrame,
    output: &mut Vec<Bytes>,
) -> Result<(), TransformError> {
    if frame.data == "[DONE]" {
        if *done {
            return Err(TransformError::shape(
                "stream framing",
                "duplicate [DONE] sentinel",
            ));
        }
        *done = true;
        return Ok(());
    }
    if *done {
        return Err(TransformError::shape(
            "stream framing",
            "data followed [DONE] sentinel",
        ));
    }
    output.push(encoder.push(&frame.data)?);
    Ok(())
}
