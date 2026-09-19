use bytes::Bytes;
use gproxy_channel_api::StreamDecodeError;
use gproxy_channel_api::{ChannelError, Frame, StreamCtx, StreamDecoder, StreamEnd, StreamTail};
use serde_json::Value;

pub(crate) fn decoder(ctx: StreamCtx<'_>) -> Option<Box<dyn StreamDecoder>> {
    let framing = ctx.framing;
    let inner = crate::shared::gemini::stream::GeminiStreamDecoder::for_operation(ctx)?;
    Some(match framing {
        gproxy_protocol::StreamFraming::Sse => Box::new(CodeAssistSse {
            inner,
            buffer: Vec::new(),
        }),
        gproxy_protocol::StreamFraming::JsonArray => Box::new(CodeAssistJsonArray {
            inner,
            buffer: Vec::new(),
        }),
        gproxy_protocol::StreamFraming::WebSocket => return None,
    })
}

struct CodeAssistJsonArray {
    inner: crate::shared::gemini::stream::GeminiStreamDecoder,
    buffer: Vec<u8>,
}

impl StreamDecoder for CodeAssistJsonArray {
    fn terminal_failure(&self) -> Option<&gproxy_channel_api::UpstreamFailure> {
        self.inner.terminal_failure()
    }
    fn terminal_disposition(&self) -> Option<gproxy_channel_api::Disposition> {
        self.inner.terminal_disposition()
    }

    fn push(&mut self, chunk: Bytes) -> Result<Vec<Frame>, StreamDecodeError> {
        self.buffer.extend_from_slice(&chunk);
        if self.buffer.len() > 100 * 1024 * 1024 {
            return Err(
                ChannelError::Decode("Code Assist JSON response exceeds 100 MiB".into()).into(),
            );
        }
        Ok(Vec::new())
    }

    fn finish(&mut self, end: StreamEnd) -> Result<StreamTail, StreamDecodeError> {
        if end == StreamEnd::Interrupted {
            self.buffer.clear();
            return self.inner.finish(end);
        }
        let value: Value = serde_json::from_slice(&std::mem::take(&mut self.buffer))
            .map_err(|error| ChannelError::Decode(format!("Code Assist JSON: {error}")))?;
        let frame = Bytes::from(
            serde_json::to_vec(&[super::unwrap_value(&value)])
                .map_err(|error| ChannelError::Decode(error.to_string()))?,
        );
        let mut frames = self.inner.push(frame)?;
        let mut tail = match self.inner.finish(end) {
            Ok(tail) => tail,
            Err(error) => return Err(error.prepend(frames)),
        };
        frames.append(&mut tail.frames);
        tail.frames = frames;
        Ok(tail)
    }

    fn recover_tail(&mut self) -> StreamTail {
        self.inner.recover_tail()
    }
}

struct CodeAssistSse {
    inner: crate::shared::gemini::stream::GeminiStreamDecoder,
    buffer: Vec<u8>,
}

impl CodeAssistSse {
    fn drain(&mut self) -> Result<Vec<Frame>, StreamDecodeError> {
        let mut output = Vec::new();
        while let Some((end, delimiter)) = delimiter(&self.buffer) {
            let raw = self.buffer.drain(..end + delimiter).collect::<Vec<_>>();
            let frame = match canonical(&raw[..end]) {
                Ok(frame) => frame,
                Err(error) => return Err(StreamDecodeError::from(error).prepend(output)),
            };
            if let Some(frame) = frame {
                match self.inner.push(frame) {
                    Ok(frames) => output.extend(frames),
                    Err(error) => return Err(error.prepend(output)),
                }
            }
        }
        Ok(output)
    }
}

impl StreamDecoder for CodeAssistSse {
    fn terminal_failure(&self) -> Option<&gproxy_channel_api::UpstreamFailure> {
        self.inner.terminal_failure()
    }
    fn terminal_disposition(&self) -> Option<gproxy_channel_api::Disposition> {
        self.inner.terminal_disposition()
    }

    fn push(&mut self, chunk: Bytes) -> Result<Vec<Frame>, StreamDecodeError> {
        self.buffer.extend_from_slice(&chunk);
        if self.buffer.len() > 100 * 1024 * 1024 {
            return Err(
                ChannelError::Decode("Code Assist SSE frame exceeds 100 MiB".into()).into(),
            );
        }
        self.drain()
    }

    fn finish(&mut self, end: StreamEnd) -> Result<StreamTail, StreamDecodeError> {
        let mut frames = if end == StreamEnd::Complete && !self.buffer.is_empty() {
            match canonical(&std::mem::take(&mut self.buffer))? {
                Some(frame) => self.inner.push(frame)?,
                None => Vec::new(),
            }
        } else {
            self.buffer.clear();
            Vec::new()
        };
        let mut tail = match self.inner.finish(end) {
            Ok(tail) => tail,
            Err(error) => return Err(error.prepend(frames)),
        };
        frames.append(&mut tail.frames);
        tail.frames = frames;
        Ok(tail)
    }

    fn recover_tail(&mut self) -> StreamTail {
        self.inner.recover_tail()
    }
}

fn canonical(raw: &[u8]) -> Result<Option<Bytes>, ChannelError> {
    let text = std::str::from_utf8(raw)
        .map_err(|_| ChannelError::Decode("Code Assist SSE is not UTF-8".into()))?;
    let data = text
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|value| value.strip_prefix(' ').unwrap_or(value))
        .collect::<Vec<_>>();
    if data.is_empty() {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(&data.join("\n"))
        .map_err(|error| ChannelError::Decode(format!("Code Assist SSE JSON: {error}")))?;
    let data = serde_json::to_vec(super::unwrap_value(&value))
        .map_err(|error| ChannelError::Decode(error.to_string()))?;
    let mut output = Vec::with_capacity(data.len() + 8);
    output.extend_from_slice(b"data: ");
    output.extend_from_slice(&data);
    output.extend_from_slice(b"\n\n");
    Ok(Some(Bytes::from(output)))
}

fn delimiter(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = find(buffer, b"\n\n").map(|index| (index, 2));
    let crlf = find(buffer, b"\r\n\r\n").map(|index| (index, 4));
    match (lf, crlf) {
        (Some(left), Some(right)) => Some(if left.0 <= right.0 { left } else { right }),
        (left, right) => left.or(right),
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|value| value == needle)
}
