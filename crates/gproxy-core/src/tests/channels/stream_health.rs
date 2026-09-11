use bytes::Bytes;
use futures_util::StreamExt;
use http::StatusCode;
use serde_json::{Value, json};

use super::{codex_core, codex_target};
use crate::control::{FailoverBudget, Plan};
use crate::tests::{block_on, memory::MemoryHost, request};
use crate::{CredentialHealth, Ended, ResponseBody, UsageSource};

fn response_event(event_type: &str, code: Option<&str>) -> Value {
    json!({
        "type": event_type,
        "response": {
            "id": "terminal-health", "object": "response", "created_at": 1,
            "status": match event_type {
                "response.completed" => "completed",
                "response.incomplete" => "incomplete",
                _ => "failed",
            },
            "model": "gpt-test", "output": [], "service_tier": "priority",
            "error": code.map(|code| json!({"code": code, "message": "request failed"})),
            "usage": {"input_tokens": 13, "output_tokens": 7, "total_tokens": 20}
        }
    })
}

fn host_with_event(event: Value, detached: bool, delimited: bool) -> MemoryHost {
    let host = if detached {
        MemoryHost::with_session_spawner()
    } else {
        MemoryHost::new(false)
    };
    {
        let mut state = host.state.lock().unwrap();
        state.credential.channel = "codex".into();
        state.credential.secret["expires_at"] = json!(i64::MAX);
        let selected = codex_target();
        state.plan = Some(Plan {
            targets: vec![selected.clone(), selected],
            budget: FailoverBudget { max_attempts: 2 },
        });
        let suffix = if delimited { "\n\n" } else { "" };
        state.scripted.push_back((
            StatusCode::OK,
            vec![Bytes::from(format!("data: {event}{suffix}"))],
        ));
    }
    host
}

#[test]
fn claude_error_conversion_preserves_terminal_health_and_usage() {
    // Both delimited events and events finalized at EOF carry an error envelope.
    for delimited in [true, false] {
        for event_type in ["response.failed", "error"] {
            for (code, degrades) in [
                ("invalid_prompt", false),
                ("invalid_image", false),
                ("misalignment_policy_violation", false),
                ("rate_limit_exceeded", true),
                ("future_error", true),
            ] {
                let event = if event_type == "error" {
                    json!({"type": "error", "code": code, "message": "request failed", "param": null})
                } else {
                    response_event(event_type, Some(code))
                };
                let host = host_with_event(event, false, delimited);
                let selected = codex_target();
                let prior = (
                    selected.credential,
                    selected.upstream_model,
                    CredentialHealth::Healthy,
                );
                host.state.lock().unwrap().health.push(prior.clone());
                let core = codex_core(&host).unwrap();
                let mut input = request(true, "claude-terminal-health");
                input.path = "/v1/messages".into();
                input.body = Bytes::from(
                    json!({
                        "model": "alias", "stream": true, "max_tokens": 32,
                        "messages": [{"role": "user", "content": "hello"}]
                    })
                    .to_string(),
                );
                let outcome = block_on(core.execute(&host, input)).unwrap();
                let ResponseBody::Stream(mut stream) = outcome.body else {
                    panic!("expected Claude stream");
                };
                let mut output = Vec::new();
                block_on(async {
                    while let Some(frame) = stream.next().await {
                        output.extend_from_slice(&frame.unwrap());
                    }
                });
                let output = String::from_utf8(output).unwrap();
                assert!(output.contains("event: error"), "{output}");
                assert!(!output.contains("message_stop"), "{output}");
                let state = host.state.lock().unwrap();
                let mut expected = vec![prior.clone()];
                if degrades {
                    expected.push((prior.0, prior.1, CredentialHealth::Degraded));
                }
                assert_eq!(
                    state.health, expected,
                    "{event_type}, {code}, delimited={delimited}"
                );
                assert_eq!(
                    state.upstream_requests.len(),
                    1,
                    "committed responses must not retry"
                );
                assert_eq!(state.settlements.len(), 1);
                let settlement = &state.settlements[0];
                assert_eq!(settlement.ended, Ended::Complete);
                if event_type == "response.failed" {
                    assert_eq!(settlement.source, UsageSource::Upstream);
                    assert_eq!(settlement.usage.input_tokens, 13);
                    assert_eq!(settlement.usage.output_tokens, 7);
                    assert_eq!(settlement.usage.dimensions["service_tier"], "priority");
                }
            }
        }
    }
}

#[test]
fn codex_terminal_health_survives_native_drop_and_inline_cancellation() {
    for detached in [true, false] {
        for (event_type, code, prior_health, health_update) in [
            (
                "response.completed",
                None,
                CredentialHealth::Degraded,
                Some(CredentialHealth::Healthy),
            ),
            (
                "response.failed",
                Some("rate_limit_exceeded"),
                CredentialHealth::Healthy,
                Some(CredentialHealth::Degraded),
            ),
            (
                "response.failed",
                Some("invalid_prompt"),
                CredentialHealth::Degraded,
                None,
            ),
            (
                "response.incomplete",
                None,
                CredentialHealth::Degraded,
                None,
            ),
        ] {
            let host = host_with_event(response_event(event_type, code), detached, true);
            let selected = codex_target();
            let prior = (selected.credential, selected.upstream_model, prior_health);
            host.state.lock().unwrap().health.push(prior.clone());
            let core = codex_core(&host).unwrap();
            let mut input = request(true, "cancel-after-terminal");
            input.body = Bytes::from(
                json!({"model": "alias", "stream": true, "input": "hello"}).to_string(),
            );
            let outcome = block_on(core.execute(&host, input)).unwrap();
            let ResponseBody::Stream(mut stream) = outcome.body else {
                panic!("expected Responses stream");
            };
            let frame = block_on(stream.next()).unwrap().unwrap();
            assert!(String::from_utf8_lossy(&frame).contains(event_type));
            assert_eq!(
                host.state.lock().unwrap().health.as_slice(),
                std::slice::from_ref(&prior)
            );
            if detached {
                assert!(outcome.stream_cancellation.is_none());
            } else {
                outcome.stream_cancellation.unwrap().cancel();
                assert!(block_on(stream.next()).unwrap().is_err());
                assert!(block_on(stream.next()).is_none());
            }
            // The native path settles on drop without polling upstream EOF.
            drop(stream);
            let state = host.state.lock().unwrap();
            let mut expected = vec![prior.clone()];
            if let Some(health) = health_update {
                expected.push((prior.0, prior.1, health));
            }
            assert_eq!(state.health, expected, "{event_type}, detached={detached}");
            assert_eq!(state.upstream_requests.len(), 1);
            assert_eq!(state.settlements.len(), 1);
            let settlement = &state.settlements[0];
            assert_eq!(settlement.ended, Ended::Interrupted);
            assert_eq!(settlement.source, UsageSource::Upstream);
            assert_eq!(settlement.usage.input_tokens, 13);
            assert_eq!(settlement.usage.output_tokens, 7);
            assert_eq!(settlement.usage.dimensions["service_tier"], "priority");
        }
    }
}

#[test]
fn discarding_converted_terminal_tail_preserves_upstream_success() {
    for detached in [true, false] {
        // The terminal event is decoded at EOF and becomes several Claude
        // frames. Closing after the first frame discards the rest of the tail.
        let host = host_with_event(response_event("response.completed", None), detached, false);
        let selected = codex_target();
        let prior = (
            selected.credential,
            selected.upstream_model,
            CredentialHealth::Degraded,
        );
        host.state.lock().unwrap().health.push(prior.clone());
        let core = codex_core(&host).unwrap();
        let mut input = request(true, "cancel-terminal-tail");
        input.path = "/v1/messages".into();
        input.body = Bytes::from(
            json!({
                "model": "alias", "stream": true, "max_tokens": 32,
                "messages": [{"role": "user", "content": "hello"}]
            })
            .to_string(),
        );
        let outcome = block_on(core.execute(&host, input)).unwrap();
        let ResponseBody::Stream(mut stream) = outcome.body else {
            panic!("expected Claude stream");
        };
        let frame = block_on(stream.next()).unwrap().unwrap();
        let frame = String::from_utf8_lossy(&frame);
        assert!(frame.contains("message_start"));
        assert!(!frame.contains("message_stop"));
        assert_eq!(
            host.state.lock().unwrap().health.as_slice(),
            std::slice::from_ref(&prior)
        );
        if !detached {
            outcome.stream_cancellation.unwrap().cancel();
            assert!(block_on(stream.next()).is_none());
        }
        drop(stream);
        let state = host.state.lock().unwrap();
        assert_eq!(
            state.health,
            [prior.clone(), (prior.0, prior.1, CredentialHealth::Healthy)]
        );
        assert_eq!(state.settlements.len(), 1);
        let settlement = &state.settlements[0];
        assert_eq!(settlement.ended, Ended::Interrupted);
        assert_eq!(settlement.source, UsageSource::Upstream);
        assert_eq!(settlement.usage.input_tokens, 13);
        assert_eq!(settlement.usage.output_tokens, 7);
        assert_eq!(settlement.usage.dimensions["service_tier"], "priority");
    }
}

#[test]
fn decoder_failure_delivers_prefix_and_settles_semantic_usage_for_all_chunkings() {
    let created = json!({"type":"response.created", "response":{"id":"partial-r", "object":"response", "created_at":1, "status":"in_progress", "model":"gpt-test", "output":[]}});
    let delta = json!({"type":"response.output_text.delta", "item_id":"m1", "output_index":0, "content_index":0, "delta":"你好hello"});
    let malformed = json!({"type":"response.code_interpreter_call_code.done", "item_id":"tool", "output_index":1});
    let events = [created, delta, malformed].map(|e| format!("data: {e}\n\n"));
    for claude in [false, true] {
        let mut expected = None;
        for chunk_size in [1, 7, usize::MAX] {
            let host = host_with_event(json!({}), false, true);
            let bytes = events.concat().into_bytes();
            {
                let mut state = host.state.lock().unwrap();
                state.scripted.clear();
                state.scripted.push_back((
                    StatusCode::OK,
                    bytes
                        .chunks(chunk_size.min(bytes.len()))
                        .map(Bytes::copy_from_slice)
                        .collect(),
                ));
            }
            let core = codex_core(&host).unwrap();
            let mut input = request(true, "prefix-estimate");
            input.path = if claude {
                "/v1/messages"
            } else {
                "/v1/responses"
            }
            .into();
            input.body = Bytes::from(if claude {
                json!({"model":"alias", "stream":true, "max_tokens":100, "messages":[{"role":"user", "content":"hello"}]})
            } else { json!({"model":"alias", "stream":true, "input":"hello"}) }.to_string());
            let outcome = block_on(core.execute(&host, input)).unwrap();
            let ResponseBody::Stream(mut stream) = outcome.body else {
                panic!("expected stream");
            };
            let mut output = Vec::new();
            let mut errors = 0;
            block_on(async {
                while let Some(frame) = stream.next().await {
                    match frame {
                        Ok(frame) => output.extend_from_slice(&frame),
                        Err(error) => {
                            assert!(error.to_string().contains("missing field `code`"));
                            errors += 1;
                        }
                    }
                }
            });
            assert_eq!(errors, 1);
            assert!(std::str::from_utf8(&output).unwrap().contains("你好hello"));
            if let Some(expected) = &expected {
                assert_eq!(&output, expected);
            } else {
                expected = Some(output);
            }
            let state = host.state.lock().unwrap();
            assert_eq!(state.upstream_requests.len(), 1);
            assert_eq!(state.settlements.len(), 1);
            let settled = &state.settlements[0];
            assert_eq!(settled.ended, Ended::Interrupted);
            assert_eq!(settled.source, UsageSource::Estimated);
            assert_eq!(settled.usage.output_tokens, 4);
            assert_eq!(
                settled.usage.dimensions["output_estimate_basis"],
                "content_chars"
            );
            assert_eq!(settled.usage.dimensions["usage_incomplete"], "true");
        }
    }
}

#[test]
fn terminal_info_log_is_structured_redacted_and_emitted_once() {
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    #[derive(Clone)]
    struct Writer(Arc<Mutex<Vec<u8>>>);
    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let writer = Writer(Arc::default());
    let sink = writer.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_max_level(tracing::Level::INFO)
        .with_writer(move || sink.clone())
        .finish();
    // Keep one subscriber registered while parallel tests register callsites.
    // Filter by this request below so concurrent executions cannot affect counts.
    tracing::subscriber::set_global_default(subscriber).unwrap();
    {
        let mut event = response_event("response.failed", Some("server_error"));
        event["response"]["error"]["message"] =
            json!("Bearer private-secret; include the request ID request-abc123 in your message");
        let host = host_with_event(event, false, true);
        let core = codex_core(&host).unwrap();
        let mut input = request(true, "diagnostic-local-request");
        input.body =
            Bytes::from(json!({"model":"alias","stream":true,"input":"hello"}).to_string());
        let outcome = block_on(core.execute(&host, input)).unwrap();
        let ResponseBody::Stream(mut stream) = outcome.body else {
            panic!("stream");
        };
        block_on(async {
            while let Some(frame) = stream.next().await {
                frame.unwrap();
            }
        });
        drop(stream);
    }
    let bytes = writer.0.lock().unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert!(!text.contains("private-secret"));
    let logs: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let terminal: Vec<_> = logs
        .iter()
        .filter(|v| {
            v["fields"]["message"] == "stream.terminal"
                && v["fields"]["request_id"] == "request-diagnostic-local-request"
        })
        .collect();
    assert_eq!(terminal.len(), 1, "{text}");
    let fields = &terminal[0]["fields"];
    assert_eq!(terminal[0]["level"], "INFO");
    assert_eq!(fields["error_code"], "server_error");
    assert_eq!(fields["upstream_request_id"], "request-abc123");
    assert_eq!(fields["upstream_request_id_source"], "error.message");
    assert_eq!(fields["usage_received"], true);
    assert_eq!(fields["disposition"], "Retryable");
}
