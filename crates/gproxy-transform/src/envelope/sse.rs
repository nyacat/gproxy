use bytes::Bytes;

use crate::{StreamTransformError, TransformError};

#[derive(Debug)]
pub struct SseFrame {
    pub event: Option<String>,
    pub data: String,
}

impl SseFrame {
    pub fn typed<T: serde::Serialize>(
        event: Option<&str>,
        value: &T,
    ) -> Result<Bytes, TransformError> {
        Ok(Self::encode(event, &serde_json::to_string(value)?))
    }

    pub fn encode(event: Option<&str>, data: &str) -> Bytes {
        let mut output = String::new();
        if let Some(event) = event {
            output.push_str("event: ");
            output.push_str(event);
            output.push('\n');
        }
        for line in data.lines() {
            output.push_str("data: ");
            output.push_str(line);
            output.push('\n');
        }
        output.push('\n');
        Bytes::from(output)
    }
}

#[derive(Default)]
pub struct SseDecoder {
    buffer: Vec<u8>,
}

impl SseDecoder {
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<SseFrame>, StreamTransformError<SseFrame>> {
        self.buffer.extend_from_slice(chunk);
        let mut frames = Vec::new();
        let mut cursor = 0;
        while let Some((end, delimiter)) = delimiter(&self.buffer[cursor..]) {
            if end > 100 * 1024 * 1024 {
                self.buffer.clear();
                return Err(StreamTransformError {
                    error: TransformError::shape("SSE", "frame exceeds 100 MiB"),
                    frames,
                });
            }
            match parse(&self.buffer[cursor..cursor + end]) {
                Ok(Some(frame)) => frames.push(frame),
                Ok(None) => {}
                Err(error) => {
                    self.buffer.clear();
                    return Err(StreamTransformError { error, frames });
                }
            }
            cursor += end + delimiter;
        }
        self.buffer.drain(..cursor);
        if self.buffer.len() > 100 * 1024 * 1024 {
            self.buffer.clear();
            return Err(StreamTransformError {
                error: TransformError::shape("SSE", "frame exceeds 100 MiB"),
                frames,
            });
        }
        Ok(frames)
    }

    pub fn finish(&mut self) -> Result<Option<SseFrame>, TransformError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        let raw = std::mem::take(&mut self.buffer);
        parse(&raw)
    }
}

fn delimiter(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = find(buffer, b"\n\n").map(|index| (index, 2));
    let crlf = find(buffer, b"\r\n\r\n").map(|index| (index, 4));
    match (lf, crlf) {
        (Some(left), Some(right)) => Some(if left.0 <= right.0 { left } else { right }),
        (left, right) => left.or(right),
    }
}

fn parse(raw: &[u8]) -> Result<Option<SseFrame>, TransformError> {
    let text =
        std::str::from_utf8(raw).map_err(|_| TransformError::shape("SSE", "frame is not UTF-8"))?;
    let mut event = None;
    let mut data = Vec::new();
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("event:") {
            event = Some(value.trim_start().to_owned());
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.strip_prefix(' ').unwrap_or(value));
        }
    }
    Ok((!data.is_empty()).then(|| SseFrame {
        event,
        data: data.join("\n"),
    }))
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|candidate| candidate == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_later_frame_preserves_the_completed_prefix() {
        let mut input = b"data: {\"text\":\"kept\"}\n\ndata: ".to_vec();
        input.resize(input.len() + 100 * 1024 * 1024, b'x');
        let mut decoder = SseDecoder::default();
        let error = decoder.push(&input).unwrap_err();
        assert!(error.to_string().contains("exceeds 100 MiB"));
        assert_eq!(error.frames.len(), 1);
        assert_eq!(error.frames[0].data, r#"{"text":"kept"}"#);
        assert!(decoder.finish().unwrap().is_none());
    }
}
