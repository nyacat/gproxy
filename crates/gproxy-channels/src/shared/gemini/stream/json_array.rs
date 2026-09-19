//! Incremental JSON-array framing used by Gemini streaming responses.

use gproxy_channel_api::ChannelError;
use serde_json::Value;

use super::ParsedChunk;

const MAX_BUFFER_BYTES: usize = 100 * 1024 * 1024;

#[derive(Default)]
pub(super) struct Decoder {
    buffer: Vec<u8>,
    state: State,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum State {
    #[default]
    Start,
    FirstOrEnd,
    Value,
    Separator,
    End,
}

impl Decoder {
    pub(super) fn pending_len(&self) -> usize {
        self.buffer.len()
    }

    pub(super) fn push(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
    }

    pub(super) fn finish(&self) -> Result<(), ChannelError> {
        if self.state == State::End {
            Ok(())
        } else {
            Err(decode("stream ended before closing ']'"))
        }
    }

    pub(super) fn next(&mut self, eof: bool) -> Result<Option<ParsedChunk>, ChannelError> {
        let mut cursor = 0;
        let mut state = self.state;
        loop {
            cursor += whitespace_len(&self.buffer[cursor..]);
            match state {
                State::Start => match self.buffer.get(cursor) {
                    Some(b'[') => {
                        cursor += 1;
                        state = State::FirstOrEnd;
                    }
                    Some(_) => return Err(decode("expected opening '['")),
                    None => break,
                },
                State::FirstOrEnd => match self.buffer.get(cursor) {
                    Some(b']') => {
                        cursor += 1;
                        self.state = State::End;
                        return Ok(Some(self.take(cursor, None)));
                    }
                    Some(_) => state = State::Value,
                    None => break,
                },
                State::Value => {
                    let Some((length, value)) = parse_value(&self.buffer[cursor..])? else {
                        break;
                    };
                    let end = cursor + length;
                    let separator = end + whitespace_len(&self.buffer[end..]);
                    match self.buffer.get(separator).copied() {
                        Some(b',') => self.state = State::Value,
                        Some(b']') => self.state = State::End,
                        Some(_) => return Err(decode("expected ',' or ']' after an element")),
                        None if eof => {
                            // A complete final response is still useful when its
                            // closing array delimiter is truncated. Surface it
                            // before finish reports the framing failure.
                            self.state = State::Separator;
                            return Ok(Some(self.take(separator, Some(value))));
                        }
                        None => break,
                    }
                    return Ok(Some(self.take(separator + 1, Some(value))));
                }
                State::Separator => break,
                State::End => {
                    if cursor == self.buffer.len() {
                        return Ok((cursor > 0).then(|| self.take(cursor, None)));
                    }
                    return Err(decode("data followed closing ']'"));
                }
            }
        }
        if self.buffer.len() > MAX_BUFFER_BYTES {
            return Err(decode("buffer exceeds 100 MiB"));
        }
        Ok(None)
    }

    fn take(&mut self, length: usize, value: Option<Value>) -> ParsedChunk {
        ParsedChunk {
            raw: self.buffer.drain(..length).collect::<Vec<_>>().into(),
            value,
        }
    }
}

fn parse_value(buffer: &[u8]) -> Result<Option<(usize, Value)>, ChannelError> {
    let mut values = serde_json::Deserializer::from_slice(buffer).into_iter::<Value>();
    match values.next() {
        Some(Ok(value)) => {
            let end = values.byte_offset();
            if end > MAX_BUFFER_BYTES {
                return Err(decode("element exceeds 100 MiB"));
            }
            Ok(Some((end, value)))
        }
        Some(Err(error)) if error.is_eof() => Ok(None),
        Some(Err(error)) => Err(decode(&format!("invalid array element: {error}"))),
        None => Ok(None),
    }
}

fn whitespace_len(buffer: &[u8]) -> usize {
    buffer
        .iter()
        .position(|byte| !matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
        .unwrap_or(buffer.len())
}

fn decode(message: &str) -> ChannelError {
    ChannelError::Decode(format!("Gemini JSON-array stream: {message}"))
}
