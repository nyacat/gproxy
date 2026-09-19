use gproxy_protocol::OperationKey;

/// A stream operation may complete output before a later event fails.  The
/// caller must relay these frames before reporting the error; they are owned
/// by the error and must never be emitted again from `finish`.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct StreamTransformError<T = bytes::Bytes> {
    pub error: TransformError,
    pub frames: Vec<T>,
}

impl<T> From<TransformError> for StreamTransformError<T> {
    fn from(error: TransformError) -> Self {
        Self {
            error,
            frames: Vec::new(),
        }
    }
}

impl<T> StreamTransformError<T> {
    pub fn prepend(mut self, mut frames: Vec<T>) -> Self {
        frames.append(&mut self.frames);
        self.frames = frames;
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TransformError {
    #[error("unsupported transform pair: {source_key:?} -> {target_key:?}")]
    UnsupportedPair {
        source_key: OperationKey,
        target_key: OperationKey,
    },
    #[error("invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("invalid {wire} wire shape: {message}")]
    InvalidShape { wire: &'static str, message: String },
    #[error("unsupported {wire} field or event: {name}")]
    Unsupported { wire: &'static str, name: String },
    #[error("stream ended with an incomplete SSE frame")]
    IncompleteStream,
}

impl TransformError {
    pub(crate) fn shape(wire: &'static str, message: impl Into<String>) -> Self {
        Self::InvalidShape {
            wire,
            message: message.into(),
        }
    }

    pub(crate) fn unsupported(wire: &'static str, name: impl Into<String>) -> Self {
        Self::Unsupported {
            wire,
            name: name.into(),
        }
    }
}
