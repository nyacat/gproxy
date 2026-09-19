//! Transform-after operation orchestration without exposing transport.

use bytes::Bytes;
use serde_json::Value;

use crate::{ChannelError, Frame, PreparedRequest, StreamDecodeError, StreamEnd};

pub struct StepResponse {
    pub status: http::StatusCode,
    pub headers: http::HeaderMap,
    pub body: Bytes,
}

pub enum DriverInput {
    Response(StepResponse),
    Continuation(Value),
}

pub enum OperationStep {
    Call {
        label: &'static str,
        request: Box<PreparedRequest>,
    },
    Claim {
        id: String,
    },
    Final {
        label: &'static str,
        request: Box<PreparedRequest>,
        stream: Box<dyn OperationStream>,
        cleanup: Box<PreparedRequest>,
        ttl_secs: u64,
    },
    Resume {
        stream: Box<dyn OperationStream>,
        cleanup: Box<PreparedRequest>,
        ttl_secs: u64,
    },
}

pub trait OperationDriver: Send {
    fn claim_id(&self) -> Option<&str> {
        None
    }

    fn next(&mut self, input: Option<DriverInput>) -> Result<OperationStep, ChannelError>;

    fn abort(&mut self) -> Option<PreparedRequest> {
        None
    }
}

pub struct StreamOutput {
    pub frames: Vec<Frame>,
    pub pause: Option<Pause>,
}

impl StreamOutput {
    pub fn frames(frames: Vec<Frame>) -> Self {
        Self {
            frames,
            pause: None,
        }
    }
}

pub struct Pause {
    pub id: String,
    pub state: Value,
    pub pending: Vec<Bytes>,
}

pub trait OperationStream: Send {
    fn terminal_failure(&self) -> Option<&crate::UpstreamFailure> {
        None
    }

    /// Preserve completed output when a later event in the same chunk fails,
    /// with the same ownership rules as [`crate::StreamDecoder`].
    fn push(&mut self, chunk: Bytes) -> Result<StreamOutput, StreamDecodeError>;
    fn finish(&mut self, end: StreamEnd) -> Result<Vec<Frame>, StreamDecodeError>;

    /// Finish while retaining a terminal pause discovered in an unterminated
    /// transport chunk. Implementations that do not support pauses keep the
    /// original frame-only behavior.
    fn finish_output(&mut self, end: StreamEnd) -> Result<StreamOutput, StreamDecodeError> {
        self.finish(end).map(StreamOutput::frames)
    }
}
