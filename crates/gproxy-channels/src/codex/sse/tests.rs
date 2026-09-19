use bytes::Bytes;
use gproxy_channel_api::{StreamCtx, StreamDecoder, StreamEnd};
use gproxy_protocol::{ContentGenerationKind, Operation, OperationKey};

use super::CodexSseDecoder;

#[test]
fn interrupted_finish_never_synthesizes_a_terminal_event() {
    let mut decoder = CodexSseDecoder::for_operation(StreamCtx {
        key: OperationKey::content(
            Operation::StreamGenerateContent,
            ContentGenerationKind::OpenAiResponses,
        ),
        framing: gproxy_protocol::StreamFraming::Sse,
        request_body: &Bytes::new(),
        response_headers: &http::HeaderMap::new(),
    })
    .unwrap();
    decoder
        .push(Bytes::from_static(
            b"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"item_id\":\"m1\",\"delta\":\"partial\"}\n\n",
        ))
        .unwrap();
    let tail = decoder.finish(StreamEnd::Interrupted).unwrap();
    assert!(tail.frames.is_empty());

    let mut truncated = CodexSseDecoder::for_operation(StreamCtx {
        key: OperationKey::content(
            Operation::StreamGenerateContent,
            ContentGenerationKind::OpenAiResponses,
        ),
        framing: gproxy_protocol::StreamFraming::Sse,
        request_body: &Bytes::new(),
        response_headers: &http::HeaderMap::new(),
    })
    .unwrap();
    truncated
        .push(Bytes::from_static(
            b"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"item_id\":\"m1\",\"content_index\":0,\"delta\":\"partial\"}\n\n",
        ))
        .unwrap();
    assert!(truncated.finish(StreamEnd::Complete).is_err());
}

fn decoder() -> CodexSseDecoder {
    CodexSseDecoder::for_operation(StreamCtx {
        key: OperationKey::content(
            Operation::StreamGenerateContent,
            ContentGenerationKind::OpenAiResponses,
        ),
        framing: gproxy_protocol::StreamFraming::Sse,
        request_body: &Bytes::new(),
        response_headers: &http::HeaderMap::new(),
    })
    .unwrap()
}

#[test]
fn retry_safety_stops_at_output_tools_usage_or_unknown_events() {
    use serde_json::json;

    let failure =
        "data: {\"type\":\"error\",\"code\":\"server_is_overloaded\",\"message\":\"busy\"}\n\n";
    for (event, safe) in [
        (
            json!({"type":"response.created","response":{"id":"r","object":"response","created_at":1,"status":"in_progress","output":[],"usage":null}}),
            true,
        ),
        (
            json!({"type":"response.failed","response":{"id":"r","object":"response","created_at":1,"status":"failed","output":[],"error":{"code":"server_is_overloaded","message":"busy"}}}),
            true,
        ),
        (
            json!({"type":"response.failed","response":{"id":"r","object":"response","created_at":1,"status":"failed","output":[],"usage":{"input_tokens":13,"output_tokens":7,"total_tokens":20},"error":{"code":"server_is_overloaded","message":"busy"}}}),
            false,
        ),
        (
            json!({"type":"response.output_text.delta","output_index":0,"item_id":"m","delta":"visible"}),
            false,
        ),
        (
            json!({"type":"response.function_call_arguments.delta","output_index":0,"item_id":"tool","delta":"{}"}),
            false,
        ),
        (json!({"type":"response.future_event"}), false),
    ] {
        let wire = format!("data: {event}\n\n{failure}data: [DONE]\n\n");
        for chunk_size in [1, wire.len()] {
            let mut decoder = decoder();
            for chunk in wire.as_bytes().chunks(chunk_size) {
                decoder.push(Bytes::copy_from_slice(chunk)).unwrap();
            }
            assert_eq!(decoder.replay_safe(), safe, "{event}");
            decoder.finish(StreamEnd::Complete).unwrap();
            assert_eq!(decoder.replay_safe(), safe, "{event}");
            assert_eq!(
                decoder.terminal_disposition(),
                Some(gproxy_channel_api::Disposition::Retryable)
            );
        }
    }
}

#[test]
fn malformed_event_preserves_prefix_across_every_chunk_boundary() {
    let valid = "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"item_id\":\"m1\",\"delta\":\"你好hello\"}\n\n";
    let invalid = "event: response.code_interpreter_call_code.done\ndata: {\"type\":\"response.code_interpreter_call_code.done\",\"item_id\":\"tool\",\"output_index\":1}\n\n";
    let bytes = format!("{valid}{invalid}").into_bytes();
    let expected = decoder()
        .push(Bytes::from(valid))
        .unwrap()
        .into_iter()
        .map(|f| f.0)
        .collect::<Vec<_>>();
    for split in 0..=bytes.len() {
        let mut decoder = decoder();
        let mut delivered = Vec::new();
        let mut failures = 0;
        for chunk in [&bytes[..split], &bytes[split..]] {
            match decoder.push(Bytes::copy_from_slice(chunk)) {
                Ok(frames) => delivered.extend(frames.into_iter().map(|f| f.0)),
                Err(error) => {
                    assert!(error.to_string().contains("missing field `code`"));
                    let diagnostic = error.diagnostic.as_ref().unwrap();
                    assert_eq!(
                        diagnostic.event_type.as_deref(),
                        Some("response.code_interpreter_call_code.done")
                    );
                    assert!(!format!("{diagnostic:?}").contains("你好hello"));
                    delivered.extend(error.frames.into_iter().map(|f| f.0));
                    failures += 1;
                }
            }
        }
        assert_eq!(failures, 1);
        assert_eq!(delivered, expected, "split={split}");
        let tail = decoder.finish(StreamEnd::Interrupted).unwrap();
        assert_eq!(tail.estimated_output_chars, Some(7));
        assert!(tail.frames.is_empty());
        assert!(decoder.recover_tail().estimated_output_chars.is_none());
    }
}

#[test]
fn missing_error_metadata_and_nested_errors_remain_failures() {
    use serde_json::json;
    for event in [
        json!({"type":"error", "message":"failed"}),
        json!({"type":"error", "code":null, "param":null, "message":"failed"}),
        json!({"type":"error", "error":{"code":"server_error", "message":"failed"}}),
        json!({"type":"response.failed", "response":{"id":"r1", "object":"response", "output":[], "error":{"message":"failed"}}}),
    ] {
        let mut decoder = decoder();
        let frames = decoder
            .push(Bytes::from(format!("data: {event}\n\n")))
            .unwrap();
        assert!(!frames.is_empty());
        assert_eq!(
            decoder.terminal_disposition(),
            Some(gproxy_channel_api::Disposition::Retryable)
        );
        decoder.finish(StreamEnd::Complete).unwrap();
    }
    let mut decoder = decoder();
    let event = json!({"type":"error", "code":"invalid_prompt", "message":"top", "error":{"code":"server_error", "message":"nested"}});
    let frames = decoder
        .push(Bytes::from(format!("data: {event}\n\n")))
        .unwrap();
    assert!(
        std::str::from_utf8(&frames[0].0)
            .unwrap()
            .contains("\"message\":\"top\"")
    );
    assert_eq!(
        decoder.terminal_disposition(),
        Some(gproxy_channel_api::Disposition::Terminal)
    );
}

#[test]
fn unterminated_valid_frame_is_preserved_on_missing_terminal_error() {
    let mut decoder = decoder();
    decoder.push(Bytes::from_static(b"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"item_id\":\"m1\",\"delta\":\"hello\"}")).unwrap();
    let error = decoder.finish(StreamEnd::Complete).unwrap_err();
    assert_eq!(error.frames.len(), 2);
    assert_eq!(decoder.recover_tail().estimated_output_chars, Some(5));
    assert!(decoder.recover_tail().estimated_output_chars.is_none());
}

#[test]
fn decode_diagnostics_do_not_echo_invalid_field_values() {
    let event = serde_json::json!({"type":"response.output_text.delta", "output_index":"private-payload-marker", "item_id":"m1", "delta":"private-body"});
    let error = decoder()
        .push(Bytes::from(format!("data: {event}\n\n")))
        .unwrap_err();
    assert!(!format!("{error:?}").contains("private-payload-marker"));
    assert!(!format!("{error:?}").contains("private-body"));
    assert!(
        error
            .diagnostic
            .unwrap()
            .field_path
            .contains("output_index")
    );
}
