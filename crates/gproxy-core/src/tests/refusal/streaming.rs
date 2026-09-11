use bytes::Bytes;
use futures_util::{FutureExt, StreamExt};
use serde_json::json;

use super::{block_on, enqueue, execute, message, request, setup};
use crate::ResponseBody;

#[test]
fn text_arrives_before_upstream_eof_on_both_initial_and_fallback_attempts() {
    for fallback in [false, true] {
        let (host, core) = setup(
            gproxy_channels::AzureChannel,
            json!({
                "base_url":"https://upstream.example", "claude_fallback_mode":"models", "claude_fallback_models":["claude-opus-4-8"]
            }),
        );
        if fallback {
            enqueue(&host, message("claude-fable-5", "refusal", "", 10, 0), true);
        }
        enqueue(
            &host,
            message(
                if fallback {
                    "claude-opus-4-8"
                } else {
                    "claude-fable-5"
                },
                "end_turn",
                "live answer",
                8,
                5,
            ),
            true,
        );
        host.state.lock().unwrap().scripted_pending_at = Some(if fallback { 2 } else { 1 });
        let mut input = request(true, "live-fallback");
        input.path = "/v1/messages".into();
        input.body = Bytes::from(json!({"model":"claude-fable-5","max_tokens":128,"stream":true,"messages":[{"role":"user","content":"hello"}]}).to_string());
        let result = block_on(core.execute(&host, input)).unwrap();
        let ResponseBody::Stream(mut stream) = result.body else {
            panic!("expected live stream")
        };
        let mut received = String::new();
        for _ in 0..8 {
            let chunk = stream
                .next()
                .now_or_never()
                .expect("output must not wait for upstream EOF")
                .unwrap()
                .unwrap();
            received.push_str(&String::from_utf8_lossy(&chunk));
            if received.contains("live answer") {
                break;
            }
        }
        assert!(received.contains("live answer"));
        assert_eq!(received.matches("\"type\":\"message_start\"").count(), 1);
        if fallback {
            assert!(received.contains("\"type\":\"fallback\""));
        }
    }
}

#[test]
fn a_late_refusal_without_continuation_never_replays_already_delivered_text() {
    let (host, core) = setup(
        gproxy_channels::AzureChannel,
        json!({
            "base_url":"https://upstream.example", "claude_fallback_mode":"models", "claude_fallback_models":["claude-opus-4-8"]
        }),
    );
    enqueue(
        &host,
        message("claude-fable-5", "refusal", "already streamed", 10, 3),
        true,
    );
    let (_, body) = execute(&host, &core, true);
    assert_eq!(body["stop_reason"], "refusal");
    assert_eq!(body["content"][0]["text"], "already streamed");
    let state = host.state.lock().unwrap();
    assert_eq!(state.upstream_bodies.len(), 1);
    assert!(!state.settlements[0].cost.is_zero());
}

#[test]
fn fallback_capture_body_policy_preserves_messages_and_attempt_billing() {
    let mut previous = None;
    for capture_body in [true, false] {
        let (host, core) = setup(
            gproxy_channels::AzureChannel,
            json!({
                "base_url":"https://upstream.example", "claude_fallback_mode":"models", "claude_fallback_models":["claude-opus-4-8"]
            }),
        );
        host.state.lock().unwrap().capture_response_body = capture_body;
        enqueue(&host, message("claude-fable-5", "refusal", "", 10, 0), true);
        enqueue(
            &host,
            message("claude-opus-4-8", "end_turn", "live answer", 8, 5),
            true,
        );
        let (status, body) = execute(&host, &core, true);
        assert!(status.is_success());
        let state = host.state.lock().unwrap();
        assert_eq!(state.upstream_bodies.len(), 2);
        assert_eq!(state.settlements.len(), 1);
        let settled = &state.settlements[0];
        assert_eq!(settled.attempts.len(), 2);
        assert_eq!(settled.usage.input_tokens, 8);
        assert_eq!(settled.usage.output_tokens, 5);
        assert_eq!(settled.upstream_model, "claude-opus-4-8");
        assert_eq!(
            state
                .captures
                .iter()
                .filter(|capture| capture.body.is_some())
                .count(),
            if capture_body { 2 } else { 0 }
        );
        assert!(
            state
                .captures
                .iter()
                .all(|capture| capture.status == Some(http::StatusCode::OK))
        );
        let snapshot = (
            body,
            settled.cost,
            settled
                .attempts
                .iter()
                .map(|attempt| (attempt.upstream_model.clone(), attempt.cost))
                .collect::<Vec<_>>(),
            state.captures.len(),
        );
        if let Some(previous) = previous.as_ref() {
            assert_eq!(&snapshot, previous);
        } else {
            previous = Some(snapshot);
        }
    }
}

#[test]
fn fallback_without_body_capture_releases_input_chunks_before_eof() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    struct ObservedBytes {
        bytes: Vec<u8>,
        released: Arc<AtomicBool>,
    }

    impl AsRef<[u8]> for ObservedBytes {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for ObservedBytes {
        fn drop(&mut self) {
            self.released.store(true, Ordering::SeqCst);
        }
    }

    for capture_body in [true, false] {
        let (host, core) = setup(
            gproxy_channels::AzureChannel,
            json!({
                "base_url":"https://upstream.example", "claude_fallback_mode":"models", "claude_fallback_models":["claude-opus-4-8"]
            }),
        );
        let frames = gproxy_transform::synthesize_response(
            gproxy_protocol::ContentGenerationKind::ClaudeMessages,
            Bytes::from(message("claude-fable-5", "end_turn", "live answer", 8, 5).to_string()),
            gproxy_protocol::StreamFraming::Sse,
        )
        .unwrap();
        let released = Arc::new(AtomicBool::new(false));
        let bytes = Bytes::from_owner(ObservedBytes {
            bytes: frames
                .into_iter()
                .flat_map(|frame| frame.to_vec())
                .collect(),
            released: released.clone(),
        });
        {
            let mut state = host.state.lock().unwrap();
            state.capture_response_body = capture_body;
            state
                .scripted
                .push_back((http::StatusCode::OK, vec![bytes]));
            state.scripted_pending_at = Some(1);
            state.continuations_enabled = true;
            state.run_spawned = true;
        }
        let mut input = request(true, "stream-chunk-retention");
        input.path = "/v1/messages".into();
        input.body = Bytes::from(json!({"model":"claude-fable-5","max_tokens":128,"stream":true,"messages":[{"role":"user","content":"hello"}]}).to_string());
        let outcome = block_on(core.execute(&host, input)).unwrap();
        let ResponseBody::Stream(mut stream) = outcome.body else {
            panic!("expected stream");
        };
        let mut output = String::new();
        while let Some(chunk) = stream.next().now_or_never() {
            let chunk = chunk.expect("upstream remains open").unwrap();
            output.push_str(std::str::from_utf8(&chunk).unwrap());
        }
        assert!(output.contains("live answer"));
        assert_eq!(released.load(Ordering::SeqCst), !capture_body);
        drop(stream);
        assert!(released.load(Ordering::SeqCst));
        assert_eq!(host.state.lock().unwrap().settlements.len(), 1);
    }
}
