use super::*;
use gproxy_protocol::{ContentGenerationKind as Kind, Operation};

fn key(kind: Kind) -> OperationKey {
    OperationKey::content(Operation::StreamGenerateContent, kind)
}

fn failed_output(
    make: impl Fn() -> ResponseStream,
    input: &[u8],
    split: usize,
) -> (Vec<u8>, String) {
    let mut stream = make();
    let mut output = Vec::new();
    for chunk in [&input[..split], &input[split..]] {
        match stream.push(Bytes::copy_from_slice(chunk)) {
            Ok(frames) => output.extend(frames),
            Err(error) => {
                output.extend(error.frames);
                return (output.concat(), error.error.to_string());
            }
        }
    }
    let error = stream.finish().expect_err("fixture has a malformed tail");
    output.extend(error.frames);
    (output.concat(), error.error.to_string())
}

fn assert_all_splits(make: impl Fn() -> ResponseStream, input: &[u8], expected: &[u8]) {
    for split in 0..=input.len() {
        let (output, error) = failed_output(&make, input, split);
        assert!(!error.is_empty());
        assert_eq!(output, expected, "transport split at {split}");
    }
}

#[test]
fn sse_parser_error_preserves_valid_prefix_for_every_transport_split() {
    let prefix = b"data: {\"text\":\"hello\"}\n\n";
    let input = [prefix.as_slice(), b"data: \xff\n\n"].concat();
    let operation = key(Kind::OpenAiChat);
    assert_all_splits(
        || ResponseStream::new(operation, operation).unwrap(),
        &input,
        prefix,
    );
}

#[test]
fn json_array_parser_error_preserves_valid_prefix_for_every_transport_split() {
    let operation = key(Kind::GeminiGenerateContent);
    assert_all_splits(
        || {
            ResponseStream::new_framed(
                operation,
                operation,
                StreamFraming::Sse,
                StreamFraming::JsonArray,
            )
            .unwrap()
        },
        b"[{\"text\":\"hello\"},!]",
        b"data: {\"text\":\"hello\"}\n\n",
    );
}

#[test]
fn json_array_encoder_error_preserves_valid_prefix_for_every_transport_split() {
    let operation = key(Kind::GeminiGenerateContent);
    assert_all_splits(
        || {
            ResponseStream::new_framed(
                operation,
                operation,
                StreamFraming::JsonArray,
                StreamFraming::Sse,
            )
            .unwrap()
        },
        b"data: {\"text\":\"hello\"}\n\ndata: invalid-json\n\n",
        b"[{\"text\":\"hello\"}",
    );
}

#[test]
fn typed_conversion_error_preserves_valid_prefix_for_every_transport_split() {
    let make = || ResponseStream::new(key(Kind::ClaudeMessages), key(Kind::OpenAiChat)).unwrap();
    let prefix = b"data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\n\n";
    let expected = make().push(Bytes::from_static(prefix)).unwrap().concat();
    assert!(std::str::from_utf8(&expected).unwrap().contains("hello"));
    let input = [
        prefix.as_slice(),
        b"data: {\"choices\":\"wrong-shape\"}\n\n",
    ]
    .concat();
    assert_all_splits(make, &input, &expected);
}
