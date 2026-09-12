use std::future::Future;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use gproxy_channel_api::{
    BoxFuture, Channel, ChannelError, ClientProfilePreset, CookieExchangeCtx, DriverInput,
    OperationStep, OperationStream, PrepareCtx, SimpleHttp, StepResponse,
};
use gproxy_protocol::{ContentGenerationKind as Kind, Operation, OperationKey};
use http::{HeaderMap, Method, StatusCode};
use serde_json::{Value, json};

use super::ClaudeWebChannel;
use super::stream::{Codec, SessionState};

const fn content(operation: Operation, kind: Kind) -> OperationKey {
    OperationKey::content(operation, kind)
}

fn secret() -> Value {
    json!({
        "cookie":"session-cookie",
        "account_uuid":"org-1",
        "capabilities":["chat","pro"]
    })
}

#[test]
fn declares_eight_transform_after_content_operations() {
    assert!(ClaudeWebChannel.requires_continuations());
}

#[test]
fn cookie_login_discovers_the_account_uuid() {
    let http = LoginHttp::default();
    let settings = json!({ "base_url": "https://claude.example" });
    let login = ClaudeWebChannel.login().unwrap();
    let secret = ready(login.adapter.cookie_exchange(
        &http,
        CookieExchangeCtx {
            provider_settings: &settings,
            cookie: "Cookie: cf_clearance=clear; sessionKey=sk-ant-sid01-example",
        },
    ))
    .unwrap();

    assert_eq!(
        login.descriptor.modes,
        [gproxy_channel_api::LoginMode::Cookie]
    );
    assert_eq!(secret.kind, gproxy_channel_api::CredentialKind::Cookie);
    assert_eq!(secret.secret["account_uuid"], "org-chat");
    assert_eq!(secret.secret["user_email"], "user@example.com");
    assert_eq!(
        secret.secret["cookie"],
        "cf_clearance=clear; sessionKey=sk-ant-sid01-example"
    );
    let request = http.request.lock().unwrap();
    let request = request.as_ref().unwrap();
    assert_eq!(request.uri(), "https://claude.example/api/bootstrap");
    assert_eq!(
        request.headers()["cookie"],
        secret.secret["cookie"].as_str().unwrap()
    );
    assert_eq!(
        request
            .extensions()
            .get::<gproxy_channel_api::ClientProfile>()
            .and_then(|profile| profile.preset),
        Some(ClientProfilePreset::Chrome148)
    );
    assert_eq!(ClaudeWebChannel.descriptor().credential_fields.len(), 1);
    assert_eq!(
        ClaudeWebChannel.descriptor().credential_fields[0].key,
        "cookie"
    );
}

#[test]
fn driver_uses_default_and_exact_step_urls() {
    let body =
        Bytes::from_static(br#"{"model":"claude","messages":[{"role":"user","content":"hello"}]}"#);
    let headers = HeaderMap::new();
    let secret = secret();
    let defaults = json!({});
    let mut default = ClaudeWebChannel
        .operation_driver(PrepareCtx {
            session_id: None,
            key: content(Operation::GenerateContent, Kind::ClaudeMessages),
            stream: false,
            method: &Method::POST,
            path: "/v1/messages",
            query: None,
            headers: &headers,
            body: &body,
            upstream_model: "claude-sonnet-4-6",
            provider_settings: &defaults,
            secret: &secret,
        })
        .unwrap()
        .unwrap();
    let OperationStep::Call { request, .. } = default.next(None).unwrap() else {
        panic!("new turn must create a conversation")
    };
    assert_eq!(
        request.request.uri().path(),
        "/api/organizations/org-1/chat_conversations"
    );

    let settings = json!({
        "endpoints":{
            "claudeweb_conversation_create":"https://override.example/org/{organization}/new",
            "claudeweb_conversation_settings":"https://override.example/c/{conversation}/settings",
            "claudeweb_completion":"https://override.example/c/{conversation}/completion"
        }
    });
    let mut driver = ClaudeWebChannel
        .operation_driver(PrepareCtx {
            session_id: None,
            key: content(Operation::GenerateContent, Kind::ClaudeMessages),
            stream: false,
            method: &Method::POST,
            path: "/v1/messages",
            query: None,
            headers: &headers,
            body: &body,
            upstream_model: "claude-sonnet-4-6",
            provider_settings: &settings,
            secret: &secret,
        })
        .unwrap()
        .unwrap();
    let OperationStep::Call { request, .. } = driver.next(None).unwrap() else {
        panic!("create step")
    };
    assert_eq!(request.request.uri().host(), Some("override.example"));
    assert!(request.request.uri().path().starts_with("/org/org-1/new"));
    let ok = || {
        DriverInput::Response(StepResponse {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            body: Bytes::new(),
        })
    };
    let OperationStep::Call { request, .. } = driver.next(Some(ok())).unwrap() else {
        panic!("settings step")
    };
    assert!(request.request.uri().path().ends_with("/settings"));
    let OperationStep::Final { request, .. } = driver.next(Some(ok())).unwrap() else {
        panic!("completion step")
    };
    assert!(request.request.uri().path().ends_with("/completion"));
}

#[test]
fn shapes_web_turn_and_parks_then_resumes_tool_stream() {
    let request = json!({
        "system":"be concise",
        "messages":[{"role":"user","content":"use weather"}],
        "tools":[{"type":"custom","name":"weather","input_schema":{"type":"object"}}]
    });
    let web = super::request::build(&request, "claude-opus-4-8-thinking", "", "UTC").unwrap();
    assert_eq!(web.body["model"], "claude-opus-4-8");
    assert_eq!(web.body["thinking_mode"], "auto");
    assert_eq!(web.body["tools"][0]["name"], "weather");
    assert!(web.body["tools"][0].get("type").is_none());
    assert!(web.body["attachments"].as_array().unwrap().is_empty());
    assert!(web.body["prompt"].as_str().unwrap().contains("use weather"));

    let state = SessionState {
        conversation: "conversation-1".into(),
        model: "claude-opus-4-8".into(),
        message_id: "msg-1".into(),
        input_tokens: 8,
    };
    let mut codec = Codec::new(state, false);
    let output = codec.push(Bytes::from_static(
        b"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-up\",\"content\":[]}}\n\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu-1\",\"name\":\"weather\"}}\n\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{}\"}}\n\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
    )).unwrap();
    let pause = output.pause.expect("tool boundary pauses");
    assert_eq!(pause.id, "toolu-1");
    let text = output
        .frames
        .iter()
        .map(|frame| String::from_utf8_lossy(&frame.0))
        .collect::<String>();
    assert!(text.contains("message_stop"));

    let state: SessionState = serde_json::from_value(pause.state).unwrap();
    let mut resumed = Codec::new(state, true);
    let output = resumed.push(Bytes::from_static(
        b"data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_result\",\"tool_use_id\":\"toolu-1\"}}\n\ndata: {\"type\":\"content_block_stop\",\"index\":1}\n\ndata: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\ndata: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"text_delta\",\"text\":\"Sunny\"}}\n\n",
    )).unwrap();
    let text = output
        .frames
        .iter()
        .map(|frame| String::from_utf8_lossy(&frame.0))
        .collect::<String>();
    assert!(text.contains("message_start"));
    assert!(text.contains("Sunny"));
    assert!(!text.contains("tool_result"));
}

#[test]
fn modern_web_stream_without_message_start_gets_a_canonical_start() {
    let state = SessionState {
        conversation: "conversation-1".into(),
        model: "claude-haiku-4-5-20251001".into(),
        message_id: "msg-synthesized".into(),
        input_tokens: 5,
    };
    let mut codec = Codec::new(state, false);
    let output = codec
        .push(Bytes::from_static(
            b"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
        ))
        .unwrap();
    let text = output
        .frames
        .iter()
        .map(|frame| String::from_utf8_lossy(&frame.0))
        .collect::<String>();
    assert!(text.starts_with("event: message_start"));
    assert!(text.contains("msg-synthesized"));
    assert!(text.contains("content_block_start"));
    assert!(text.contains("hello"));
}

#[test]
fn modern_web_stream_preserves_message_identity_and_terminal_reason() {
    let mut codec = Codec::new(
        SessionState {
            conversation: "conversation-1".into(),
            model: "claude-opus-4-8".into(),
            message_id: "msg-fallback".into(),
            input_tokens: 8,
        },
        false,
    );
    let mut output = codec
        .push(Bytes::from_static(
            b"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-upstream\",\"content\":[]}}\n\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"},\"usage\":{\"output_tokens\":2}}\n\ndata: {\"type\":\"message_stop\"}\n\n",
        ))
        .unwrap()
        .frames;
    output.extend(
        codec
            .finish(gproxy_channel_api::StreamEnd::Complete)
            .unwrap(),
    );
    let events = output
        .iter()
        .flat_map(|frame| std::str::from_utf8(&frame.0).unwrap().lines())
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|data| serde_json::from_str::<Value>(data).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(events[0]["message"]["id"], "msg-upstream");
    assert_eq!(events[0]["message"]["usage"]["input_tokens"], 8);
    assert_eq!(events[2]["delta"]["stop_reason"], "max_tokens");
    assert_eq!(events[2]["usage"]["output_tokens"], 2);
    assert_eq!(events.len(), 4);
    assert_eq!(events[3]["type"], "message_stop");
}

#[test]
fn malformed_web_event_type_returns_an_error_after_the_completed_prefix() {
    for invalid_type in [Value::Null, json!(42), json!({"type":"nested"})] {
        let mut codec = Codec::new(
            SessionState {
                conversation: "conversation-1".into(),
                model: "claude-opus-4-8".into(),
                message_id: "msg-fallback".into(),
                input_tokens: 8,
            },
            false,
        );
        let wire = format!(
            "data: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"text\":\"visible\"}}}}\n\ndata: {}\n\n",
            json!({"type":invalid_type})
        );
        let error = codec
            .push(Bytes::from(wire))
            .err()
            .expect("invalid event type");
        assert!(error.to_string().contains("event type must be a string"));
        assert!(
            error
                .frames
                .iter()
                .any(|frame| { std::str::from_utf8(&frame.0).unwrap().contains("visible") })
        );
    }
}

#[test]
fn web_codec_keeps_completed_frames_when_a_later_event_is_malformed() {
    let codec = || {
        Codec::new(
            SessionState {
                conversation: "conversation-1".into(),
                model: "claude-opus-4-8".into(),
                message_id: "msg-1".into(),
                input_tokens: 8,
            },
            false,
        )
    };
    let valid = b"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"visible\"}}\n\n";
    let wire = [valid.as_slice(), b"data: {\n\n"].concat();
    let expected = codec()
        .push(Bytes::copy_from_slice(valid))
        .unwrap()
        .frames
        .into_iter()
        .map(|frame| frame.0)
        .collect::<Vec<_>>();
    assert!(!expected.is_empty());
    for split in 0..=wire.len() {
        let mut codec = codec();
        let mut delivered = Vec::new();
        let mut failed = false;
        for chunk in [&wire[..split], &wire[split..]] {
            let frames = match codec.push(Bytes::copy_from_slice(chunk)) {
                Ok(output) => output.frames,
                Err(error) => {
                    failed = true;
                    error.frames
                }
            };
            delivered.extend(frames.into_iter().map(|frame| frame.0));
            if failed {
                break;
            }
        }
        assert!(failed, "split={split}");
        assert_eq!(delivered, expected, "split={split}");
    }
}

#[test]
fn tool_pause_retains_the_unparsed_suffix_for_the_resumed_codec() {
    let mut codec = Codec::new(
        SessionState {
            conversation: "conversation-1".into(),
            model: "claude-opus-4-8".into(),
            message_id: "msg-1".into(),
            input_tokens: 8,
        },
        false,
    );
    let invalid = b"data: {\n\n";
    let wire = [
        b"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool-1\",\"name\":\"tool\"}}\n\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n".as_slice(),
        invalid,
    ].concat();
    let output = codec.push(Bytes::from(wire)).unwrap();
    let pause = output.pause.unwrap();
    assert_eq!(pause.id, "tool-1");
    assert_eq!(pause.pending.concat(), invalid);
    let state = serde_json::from_value(pause.state).unwrap();
    let mut resumed = Codec::new(state, true);
    assert!(resumed.push(pause.pending[0].clone()).is_err());
}

#[derive(Default)]
struct LoginHttp {
    request: Mutex<Option<http::Request<Bytes>>>,
}

impl SimpleHttp for LoginHttp {
    fn send<'a>(
        &'a self,
        request: http::Request<Bytes>,
    ) -> BoxFuture<'a, Result<http::Response<Bytes>, ChannelError>> {
        *self.request.lock().unwrap() = Some(request);
        Box::pin(async {
            Ok(http::Response::new(Bytes::from_static(
                br#"{"account":{"email_address":"user@example.com","memberships":[{"organization":{"uuid":"org-api","capabilities":["api"]}},{"organization":{"uuid":"org-chat","capabilities":["chat","claude_pro"]}}]}}"#,
            )))
        })
    }
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("test future unexpectedly pending"),
    }
}
