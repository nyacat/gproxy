use bytes::Bytes;
use futures_util::StreamExt;
use gproxy_channel_api::{Channel, ChannelRegistry};
use http::StatusCode;
use serde_json::json;

use super::memory::MemoryHost;
use super::{block_on, request, target};
use crate::control::{FailoverBudget, Plan};
use crate::{Core, Ended, ResponseBody, UsageSource};

#[test]
fn streaming_headers_do_not_clear_degraded_health_before_the_body_finishes() {
    let host = MemoryHost::new(false);
    let core = super::core(&host).unwrap();
    let selected = target();
    let prior = (
        selected.credential,
        selected.upstream_model.clone(),
        crate::CredentialHealth::Degraded,
    );
    host.state.lock().unwrap().health.push(prior.clone());
    let outcome =
        block_on(core.invoke(&host, &selected, request(true, "deferred-health"))).unwrap();
    assert_eq!(
        host.state.lock().unwrap().health.as_slice(),
        std::slice::from_ref(&prior)
    );
    let ResponseBody::Stream(mut stream) = outcome.body else {
        panic!("expected stream");
    };
    assert!(block_on(stream.next()).unwrap().is_ok());
    assert_eq!(host.state.lock().unwrap().health, [prior]);
    block_on(async {
        while let Some(chunk) = stream.next().await {
            chunk.unwrap();
        }
    });
    let state = host.state.lock().unwrap();
    assert_eq!(state.health.len(), 2);
    assert_eq!(state.health[1].2, crate::CredentialHealth::Healthy);
    assert_eq!(state.settlements[0].ended, Ended::Complete);
}

#[test]
fn stream_finish_errors_keep_upstream_usage_and_service_tier() {
    for (terminated, has_usage, rules, transformed) in [
        (false, true, false, true),
        (true, true, false, true),
        (false, false, false, true),
        (false, true, true, false),
        (false, true, true, true),
    ] {
        let host = MemoryHost::new(false);
        let mut selected = target();
        selected.provider.channel = "openai".into();
        selected.provider.settings = json!({"base_url":"https://upstream.example"});
        if rules {
            selected.rules.process = crate::process::compile_all(&[crate::process::RuleSpec {
                id: 1,
                kind: "transform".into(),
                config: json!({"phase":"response", "locate":{"path":"model"},
                    "actions":[{"op":"replace_text","with":"rewritten"}]}),
                filter_model_pattern: None,
                filter_operations: None,
                filter_header_pattern: None,
                sort_order: 0,
                enabled: true,
            }])
            .unwrap()
            .into();
        }
        let usage_frame = Bytes::from(format!(
            "data: {}\n\n",
            json!({
                "id":"chat_usage", "object":"chat.completion.chunk", "created":0,
                "model":"upstream-model", "choices":[], "service_tier":"priority",
                "usage":has_usage.then(|| json!({"prompt_tokens":13,"completion_tokens":7,"total_tokens":20}))
            })
        ));
        {
            let mut state = host.state.lock().unwrap();
            state.credential.channel = "openai".into();
            state.credential.kind = "api_key".into();
            state.credential.secret = json!({"api_key":"test-key"});
            state.plan = Some(Plan {
                targets: vec![selected],
                budget: FailoverBudget { max_attempts: 1 },
            });
            let mut chunks = vec![usage_frame];
            if terminated {
                chunks.push(Bytes::from_static(b"data: [DONE]\n\n"));
            }
            if rules {
                // The upstream ends partway through a UTF-8 code point after
                // an earlier complete usage event. The rule codec checks its
                // final unterminated SSE frame at EOF.
                chunks.push(Bytes::from_static(b"data: \xe4"));
            }
            state.scripted.push_back((StatusCode::OK, chunks));
        }
        let core = Core::new(
            host.clone(),
            ChannelRegistry::new([Box::new(gproxy_channels::OpenAiChannel) as Box<dyn Channel>])
                .unwrap(),
        )
        .unwrap();
        let mut input = request(true, "usage-before-eof");
        input.path = if transformed {
            "/v1/messages"
        } else {
            "/v1/chat/completions"
        }
        .into();
        input.body = Bytes::from(
            json!({"model":"alias", "max_tokens":32, "stream":true,
                "messages":[{"role":"user","content":"hello"}]})
            .to_string(),
        );
        let outcome = block_on(core.execute(&host, input)).unwrap();
        assert!(host.state.lock().unwrap().health.is_empty());
        let ResponseBody::Stream(mut stream) = outcome.body else {
            panic!("expected transformed stream");
        };
        let mut errors = 0;
        block_on(async {
            while let Some(chunk) = stream.next().await {
                if let Err(error) = chunk {
                    let expected = if rules {
                        "process SSE frame is not UTF-8"
                    } else {
                        "incomplete"
                    };
                    assert!(error.to_string().contains(expected), "{error}");
                    errors += 1;
                }
            }
        });
        assert_eq!(errors, usize::from(!terminated));
        drop(stream);
        let state = host.state.lock().unwrap();
        assert_eq!(state.health.len(), 1);
        assert_eq!(
            state.health[0].2,
            if terminated {
                crate::CredentialHealth::Healthy
            } else {
                crate::CredentialHealth::Degraded
            }
        );
        assert_eq!(state.settlements.len(), 1);
        assert_eq!(state.admission_finishes, [true]);
        let settlement = &state.settlements[0];
        if has_usage {
            assert_eq!(settlement.usage.input_tokens, 13);
            assert_eq!(settlement.usage.output_tokens, 7);
            assert_eq!(settlement.source, UsageSource::Upstream);
        } else {
            assert!(settlement.usage.input_tokens > 0);
            assert_eq!(settlement.source, UsageSource::Estimated);
        }
        assert_eq!(settlement.usage.dimensions["service_tier"], "priority");
        assert_eq!(
            settlement.ended,
            if terminated {
                Ended::Complete
            } else {
                Ended::Interrupted
            }
        );
    }
}

#[test]
fn failed_usage_sink_preserves_admission_for_buffered_and_streaming_responses() {
    for streaming in [false, true] {
        let host = MemoryHost::new(false);
        {
            let mut state = host.state.lock().unwrap();
            state.fail_usage = true;
            state.plan = Some(Plan {
                targets: vec![target()],
                budget: FailoverBudget { max_attempts: 1 },
            });
        }
        let core = super::core(&host).unwrap();
        let result = block_on(core.execute(&host, request(streaming, "failed-usage"))).unwrap();
        if let ResponseBody::Stream(mut stream) = result.body {
            block_on(async {
                while let Some(chunk) = stream.next().await {
                    chunk.unwrap();
                }
            });
        }
        let state = host.state.lock().unwrap();
        assert_eq!(state.admit_calls, 1);
        assert!(state.settlements.is_empty());
        assert!(state.admission_finishes.is_empty());
        assert_eq!(state.captures.len(), 1);
    }
}

#[test]
fn forward_socket_close_hands_settlement_to_spawner_before_drop() {
    use super::surface_harness::{outcome, plan};
    use gproxy_channel_api::Binding;

    let host = MemoryHost::with_session_spawner();
    {
        let mut state = host.state.lock().unwrap();
        state.credential.secret["expires_at"] = json!(i64::MAX);
        state.defer_spawned = true;
        state.plan = Some(plan(vec![target()]));
        state.bindings.insert(
            (3, 1, "task".into(), "bound".into()),
            Binding {
                provider_id: 3,
                owner_user_id: 1,
                kind: "task".into(),
                id: "bound".into(),
                credential: crate::CredentialId(7),
                summary: json!({}),
                created_at_unix: 0,
            },
        );
    }
    let core = super::core(&host).unwrap();
    let result = outcome(
        &core,
        &host,
        http::Method::GET,
        "/surface/socket/bound",
        None,
        None,
        true,
    )
    .unwrap();
    let ResponseBody::WebSocket(mut socket) = result.body else {
        panic!("expected websocket");
    };
    assert!(matches!(
        block_on(socket.recv()).unwrap(),
        Some(crate::WsFrame::Close(_))
    ));
    drop(socket);
    let tasks = {
        let mut state = host.state.lock().unwrap();
        assert!(state.admission_finishes.is_empty());
        assert!(state.captures.is_empty());
        assert_eq!(state.spawned_tasks.len(), 1);
        std::mem::take(&mut state.spawned_tasks)
    };
    for task in tasks {
        block_on(task);
    }
    let state = host.state.lock().unwrap();
    assert_eq!(state.admission_finishes, [true]);
    assert_eq!(state.captures.len(), 1);
}
