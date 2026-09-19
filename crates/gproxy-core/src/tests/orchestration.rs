use bytes::Bytes;
use futures_util::StreamExt;
use gproxy_channel_api::{Channel, ChannelRegistry};
use serde_json::json;

use super::block_on;
use super::memory::MemoryHost;
use crate::boundary::{RequestCtx, ResponseBody, RoutingMode};
use crate::control::{FailoverBudget, Plan, ProviderRef, Target};
use crate::host::CredentialId;
use crate::{Core, InitError};

#[test]
fn continuation_channels_fail_loudly_without_host_state() {
    let registry =
        gproxy_channel_api::ChannelRegistry::new([
            Box::new(super::channel::NeedsContinuation) as Box<dyn Channel>
        ])
        .expect("registry");
    let error = match Core::new(MemoryHost::new(false), registry) {
        Ok(_) => panic!("missing continuation store was accepted"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        InitError::ContinuationsUnavailable {
            channel: "continuation-test"
        }
    ));
}

fn claudeweb_core() -> (MemoryHost, Core<MemoryHost>) {
    let host = MemoryHost::with_continuations();
    let target = Target {
        provider: ProviderRef {
            id: 44,
            name: "claude-web".into(),
            channel: "claudeweb".into(),
            settings: json!({"base_url":"https://upstream.test"}),
            fingerprint: None,
            proxy_url: None,
            traffic_blacklist: Default::default(),
        },
        credential: CredentialId(7),
        upstream_model: "claude-opus-4-8".into(),
        tier: 0,
        rules: Default::default(),
    };
    let plan = Plan {
        targets: vec![target],
        budget: FailoverBudget { max_attempts: 1 },
    };
    {
        let mut state = host.state.lock().expect("state lock");
        state.credential.channel = "claudeweb".into();
        state.credential.secret = json!({
            "cookie":"session-cookie",
            "account_uuid":"org-1",
            "capabilities":["chat","pro"],
            "validated_at_ms":i64::MAX
        });
        state.plan = Some(plan);
    }
    let channels =
        ChannelRegistry::new([Box::new(gproxy_channels::ClaudeWebChannel) as Box<dyn Channel>])
            .expect("channel registry");
    let core = Core::new(host.clone(), channels).expect("continuation-capable core");
    (host, core)
}

#[test]
fn claudeweb_new_and_resume_turns_transfer_one_scoped_stream() {
    let (host, core) = claudeweb_core();
    let first = request(
        "web-first",
        json!({
            "model":"claude-opus-4-8",
            "stream":true,
            "messages":[{"role":"user","content":"use weather"}],
            "tools":[{"name":"weather","input_schema":{"type":"object"}}]
        }),
    );
    let outcome = block_on(core.execute(&host, first)).expect("new turn");
    let ResponseBody::Stream(mut body) = outcome.body else {
        panic!("new turn was not streaming")
    };
    let first = block_on(async move {
        let mut output = Vec::new();
        while let Some(chunk) = body.next().await {
            output.extend_from_slice(&chunk.expect("new turn chunk"));
        }
        String::from_utf8(output).expect("new turn UTF-8")
    });
    assert!(first.contains("message_stop"));
    assert_eq!(
        host.state.lock().expect("state lock").continuations.len(),
        1
    );

    let second = request(
        "web-resume",
        json!({
            "model":"claude-opus-4-8",
            "stream":true,
            "messages":[
                {"role":"assistant","content":[{"type":"tool_use","id":"toolu-web","name":"weather","input":{}}]},
                {"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu-web","content":"sunny"}]}
            ]
        }),
    );
    let outcome = block_on(core.execute(&host, second)).expect("resume turn");
    let ResponseBody::Stream(mut body) = outcome.body else {
        panic!("resume turn was not streaming")
    };
    let second = block_on(async move {
        let mut output = Vec::new();
        while let Some(chunk) = body.next().await {
            output.extend_from_slice(&chunk.expect("resume chunk"));
        }
        String::from_utf8(output).expect("resume UTF-8")
    });
    assert!(second.contains("Sunny"));
    let state = host.state.lock().expect("state lock");
    assert!(state.continuations.is_empty());
    assert_eq!(state.captures.len(), 3);
    assert!(
        state
            .captures
            .iter()
            .all(|capture| capture.provider_id == Some(44))
    );
}

#[test]
fn claudeweb_late_decode_error_relays_completed_output_before_settling_interrupted() {
    let body = b"data: {\"completion\":\"kept-prefix\"}\n\ndata: {broken}\n\n";
    for chunk_size in [1, body.len()] {
        let (host, core) = claudeweb_core();
        host.state.lock().unwrap().run_spawned = true;
        host.state.lock().unwrap().scripted.extend([
            (http::StatusCode::OK, vec![Bytes::from_static(b"{}")]),
            (http::StatusCode::OK, vec![Bytes::from_static(b"{}")]),
            (
                http::StatusCode::OK,
                body.chunks(chunk_size)
                    .map(Bytes::copy_from_slice)
                    .collect(),
            ),
        ]);
        let outcome = block_on(core.execute(
            &host,
            request(
                "web-partial",
                json!({
                    "model":"claude-opus-4-8",
                    "stream":true,
                    "messages":[{"role":"user","content":"hello"}]
                }),
            ),
        ))
        .unwrap();
        let ResponseBody::Stream(mut stream) = outcome.body else {
            panic!("expected an operation stream");
        };
        let (output, errors) = block_on(async {
            let mut output = Vec::new();
            let mut errors = Vec::new();
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(chunk) => {
                        assert!(errors.is_empty(), "output followed the terminal error");
                        output.extend_from_slice(&chunk);
                    }
                    Err(error) => errors.push(error.to_string()),
                }
            }
            (String::from_utf8(output).unwrap(), errors)
        });
        assert_eq!(output.matches("kept-prefix").count(), 1, "{output}");
        assert!(!output.contains("message_stop"), "{output}");
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("ClaudeWeb SSE JSON"), "{errors:?}");
        let state = host.state.lock().unwrap();
        assert_eq!(state.settlements.len(), 1);
        assert_eq!(state.settlements[0].ended, crate::Ended::Interrupted);
        assert_eq!(
            state.health.last().unwrap().2,
            crate::CredentialHealth::Degraded
        );
        assert!(state.continuations.is_empty());
    }
}

fn request(id: &str, body: serde_json::Value) -> RequestCtx {
    RequestCtx {
        request_id: id.into(),
        client_ip: None,
        method: http::Method::POST,
        path: "/v1/messages".into(),
        query: None,
        headers: http::HeaderMap::new(),
        body: Bytes::from(body.to_string()),
        upgrade: false,
        force_model_refresh: false,
        mode: RoutingMode::Aggregated,
    }
}
