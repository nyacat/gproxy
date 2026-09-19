//! SSE framing used by Gemini streaming responses.

use gproxy_channel_api::ChannelError;
use serde_json::Value;

use super::ParsedChunk;

const MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

#[derive(Default)]
pub(super) struct Decoder {
    buffer: Vec<u8>,
}

impl Decoder {
    pub(super) fn pending_len(&self) -> usize {
        self.buffer.len()
    }

    pub(super) fn push(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
    }

    pub(super) fn next(&mut self, eof: bool) -> Result<Option<ParsedChunk>, ChannelError> {
        let length = match delimiter(&self.buffer) {
            Some((end, delimiter)) => end + delimiter,
            None if eof => self.buffer.len(),
            None if self.buffer.len() <= MAX_FRAME_BYTES => return Ok(None),
            None => {
                return Err(ChannelError::Decode(
                    "Gemini SSE frame exceeds 100 MiB".into(),
                ));
            }
        };
        if length > MAX_FRAME_BYTES {
            return Err(ChannelError::Decode(
                "Gemini SSE frame exceeds 100 MiB".into(),
            ));
        }
        if length == 0 {
            return Ok(None);
        }
        let value = parse(&self.buffer[..length])?;
        let raw = self.buffer.drain(..length).collect::<Vec<_>>().into();
        Ok(Some(ParsedChunk { raw, value }))
    }
}

fn parse(raw: &[u8]) -> Result<Option<Value>, ChannelError> {
    let text = std::str::from_utf8(raw)
        .map_err(|_| ChannelError::Decode("Gemini SSE frame is not UTF-8".into()))?;
    let data = text
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|value| value.strip_prefix(' ').unwrap_or(value))
        .collect::<Vec<_>>();
    if data.is_empty() {
        return Ok(None);
    }
    serde_json::from_str(&data.join("\n"))
        .map(Some)
        .map_err(|error| ChannelError::Decode(format!("Gemini SSE event JSON: {error}")))
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
        .position(|candidate| candidate == needle)
}
