use bytes::Bytes;
use gproxy_channel_api::{StreamDecoder, StreamEnd};
use gproxy_protocol::{ContentGenerationKind, Operation, OperationKey, StreamFraming};
use serde_json::json;

use super::*;

fn ctx() -> StreamCtx<'static> {
    static BODY: Bytes = Bytes::new();
    static HEADERS: std::sync::LazyLock<http::HeaderMap> =
        std::sync::LazyLock::new(http::HeaderMap::new);
    StreamCtx {
        key: OperationKey::content(
            Operation::StreamGenerateContent,
            ContentGenerationKind::OpenAiResponses,
        ),
        framing: StreamFraming::Sse,
        request_body: &BODY,
        response_headers: &HEADERS,
    }
}

#[test]
fn projection_matches_native_failure_and_replay_classification() {
    let created = json!({"type":"response.created","response":{
        "id":"r", "object":"response", "created_at":1,"status":"in_progress", "output":[],
        "instructions":"quoted \\\"text\\\"", "tools":[{"type":"function","name":"example","parameters":{"default":[0,1,null]}}]
    }});
    let capacity = json!({"type":"error","error":{"code":"server_is_overloaded","type":"service_unavailable_error","message":"busy"}});
    let delta = json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"message","delta":"visible"});
    let tool = json!({"type":"response.output_item.added","output_index":0,"item":{
        "type":"function_call", "id":"tool", "call_id":"call", "name":"example", "arguments":"", "status":"in_progress"
    }});
    let failed = json!({"type":"response.failed","response":{
        "id":"r", "object":"response", "created_at":1,"status":"failed", "output":[],
        "error":{"code":"server_is_overloaded","message":"busy"}
    }});
    let mut with_usage = failed.clone();
    with_usage["response"]["usage"] = json!({"input_tokens":1,"output_tokens":0,"total_tokens":1});
    let mut with_output = failed.clone();
    with_output["response"]["output"] =
        json!([{"type":"message","id":"m","role":"assistant","status":"completed","content":[]}]);
    let mut invalid_prompt = capacity.clone();
    invalid_prompt["error"]["code"] = json!("invalid_prompt");
    let mut explicit_null = capacity.clone();
    explicit_null["code"] = Value::Null;
    for events in [
        vec![created.clone()],
        vec![created.clone(), capacity.clone()],
        vec![failed],
        vec![with_usage],
        vec![with_output],
        vec![explicit_null],
        vec![invalid_prompt],
        vec![capacity.clone(), delta.clone()],
        vec![created.clone(), delta, capacity.clone()],
        vec![created, tool, capacity],
    ] {
        for multiline in [false, true] {
            let wire = events
                .iter()
                .map(|event| {
                    let text = if multiline {
                        serde_json::to_string_pretty(event).unwrap()
                    } else {
                        event.to_string()
                    };
                    format!(
                        "event: {}\ndata: {}\n\n",
                        event["type"].as_str().unwrap(),
                        text.replace('\n', "\ndata: ")
                    )
                })
                .collect::<String>();
            let mut native = super::super::CodexSseDecoder::for_operation(ctx()).unwrap();
            native
                .push(Bytes::copy_from_slice(wire.as_bytes()))
                .unwrap();
            // Keep extending even after an intermediate failure, as when a
            // transport coalesces that failure and subsequent activity.
            for chunk_size in [1, 13, wire.len()] {
                let mut probe = CodexStreamStart::for_operation(ctx()).unwrap();
                let mut state = StreamStartState::Pending;
                for length in (chunk_size..wire.len())
                    .step_by(chunk_size)
                    .chain([wire.len()])
                {
                    state = probe.inspect(&wire.as_bytes()[..length], false).unwrap();
                }
                if !native.replay_safe() {
                    assert!(matches!(state, StreamStartState::Ready), "{wire}");
                } else if let Some(expected) = native.terminal_failure() {
                    let StreamStartState::Failed(actual) = state else {
                        panic!("missing failure: {wire}");
                    };
                    assert_eq!(&actual, expected, "{wire}");
                } else {
                    assert!(matches!(state, StreamStartState::Pending), "{wire}");
                }
            }
        }
    }
}

#[test]
fn comments_done_crlf_escaped_keys_and_unterminated_final_events() {
    for suffix in ["", "\n\n", "\n\ndata: [DONE]\n\n"] {
        for delimiter in ["\n", "\r\n"] {
            let wire = format!(": heartbeat\n\nevent: error\ndata: {{\"ty\\u0070e\":\"error\",\"co\\u0064e\":\"server_is_overloaded\",\"message\":\"busy\"}}{suffix}").replace('\n', delimiter);
            let mut probe = CodexStreamStart::for_operation(ctx()).unwrap();
            for length in 0..wire.len() {
                probe.inspect(&wire.as_bytes()[..length], false).unwrap();
            }
            let StreamStartState::Failed(failure) = probe.inspect(wire.as_bytes(), true).unwrap()
            else {
                panic!("{wire}");
            };
            let mut native = super::super::CodexSseDecoder::for_operation(ctx()).unwrap();
            native
                .push(Bytes::copy_from_slice(wire.as_bytes()))
                .unwrap();
            native.finish(StreamEnd::Complete).unwrap();
            assert_eq!(Some(&failure), native.terminal_failure());
        }
    }
}

#[test]
fn invalid_and_oversized_classification_fields_fail_closed() {
    for data in [
        "{".to_owned(),
        "{\"type\":\"error\",\"code\":\"server_is_overloaded\",}".to_owned(),
        json!({"type":"error","code":"x".repeat(2048)}).to_string(),
    ] {
        let mut probe = CodexStreamStart::for_operation(ctx()).unwrap();
        assert!(
            probe
                .inspect(format!("data: {data}\n\n").as_bytes(), false)
                .is_err()
        );
    }
    let wire = format!(
        "data: {}\n\n",
        json!({"type":"error","code":"server_is_overloaded","message":"x".repeat(2 * 1024 * 1024)})
    );
    let mut probe = CodexStreamStart::for_operation(ctx()).unwrap();
    let StreamStartState::Failed(failure) = probe.inspect(wire.as_bytes(), false).unwrap() else {
        panic!("expected failure");
    };
    assert_eq!(
        failure.disposition,
        gproxy_channel_api::Disposition::Retryable
    );
    assert!(failure.message.is_none());
}
