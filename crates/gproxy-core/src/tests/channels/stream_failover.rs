use bytes::Bytes;
use futures_util::StreamExt;
use http::StatusCode;
use serde_json::json;

use super::{codex_core, codex_target};
use crate::control::{FailoverBudget, Plan};
use crate::tests::{block_on, memory::MemoryHost, request};
use crate::{CoreError, CredentialHealth, CredentialId, ResponseBody};

const OVERLOADED: &str = "event: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"server_is_overloaded\",\"type\":\"service_unavailable_error\",\"message\":\"Our servers are currently overloaded. Please try again later.\"}}\n\n";
const PRELUDE: &str = "data: {\"type\":\"response.created\",\"response\":{\"id\":\"first\",\"object\":\"response\",\"created_at\":1,\"model\":\"upstream-model\",\"status\":\"in_progress\",\"output\":[]}}\n\n";
const OUTPUT: &str = "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"item_id\":\"message\",\"delta\":\"visible\"}\n\n";

pub(super) fn output_prefix() -> Bytes {
    Bytes::from(format!("{PRELUDE}{OUTPUT}"))
}

fn host(chunks: Vec<Bytes>, budget: u32) -> MemoryHost {
    let host = MemoryHost::new(false);
    {
        let mut state = host.state.lock().unwrap();
        state.credential.channel = "codex".into();
        state.credential.secret["expires_at"] = json!(i64::MAX);
        let first = codex_target();
        let mut second = first.clone();
        second.credential = CredentialId(first.credential.0 + 1);
        state.plan = Some(Plan {
            targets: vec![first, second],
            budget: FailoverBudget {
                max_attempts: budget,
            },
        });
        state.scripted.push_back((StatusCode::OK, chunks));
    }
    host
}

fn input(path: &str) -> crate::RequestCtx {
    let mut input = request(true, "codex-stream-capacity");
    input.path = path.into();
    input.body = Bytes::from(match path {
        "/v1/messages" => json!({"model":"alias","stream":true,"max_tokens":32,"messages":[{"role":"user","content":"hello"}]}),
        "/v1/chat/completions" => json!({"model":"alias","stream":true,"messages":[{"role":"user","content":"hello"}]}),
        _ => json!({"model":"alias","stream":true,"input":"hello"}),
    }.to_string());
    input
}

fn consume(outcome: crate::ExecOutcome) -> String {
    let ResponseBody::Stream(mut stream) = outcome.body else {
        panic!("expected a streaming response");
    };
    let mut output = Vec::new();
    block_on(async {
        while let Some(frame) = stream.next().await {
            output.extend_from_slice(&frame.unwrap());
        }
    });
    String::from_utf8(output).unwrap()
}

#[test]
fn codex_capacity_before_output_degrades_only_the_selected_model_and_switches_accounts() {
    for path in ["/v1/responses", "/v1/chat/completions", "/v1/messages"] {
        for prefix in ["", PRELUDE] {
            for delimiter in ["\n", "\r\n"] {
                let wire = format!(": heartbeat\n\n{prefix}{OVERLOADED}").replace('\n', delimiter);
                for chunk_size in [1, 13, wire.len()] {
                    let host = host(
                        wire.as_bytes()
                            .chunks(chunk_size)
                            .map(Bytes::copy_from_slice)
                            .collect(),
                        2,
                    );
                    let core = codex_core(&host).unwrap();
                    let outcome = block_on(core.execute(&host, input(path))).unwrap();
                    let first = codex_target();
                    assert_eq!(
                        host.state.lock().unwrap().health,
                        [(
                            first.credential,
                            first.upstream_model.clone(),
                            CredentialHealth::Degraded
                        )]
                    );
                    let output = consume(outcome);
                    assert!(output.contains("ok"), "{path}: {output}");
                    assert!(!output.contains("server_is_overloaded"), "{output}");
                    assert!(
                        !output.contains("\"first\""),
                        "failed attempt prelude leaked: {output}"
                    );
                    let state = host.state.lock().unwrap();
                    assert_eq!(state.upstream_requests.len(), 2);
                    assert_eq!(
                        state.health,
                        [
                            (
                                first.credential,
                                first.upstream_model.clone(),
                                CredentialHealth::Degraded
                            ),
                            (
                                CredentialId(first.credential.0 + 1),
                                first.upstream_model,
                                CredentialHealth::Healthy
                            ),
                        ]
                    );
                    assert_eq!(state.settlements.len(), 1);
                    assert_eq!(
                        state.settlements[0].credential_id,
                        CredentialId(first.credential.0 + 1)
                    );
                    assert!(state.captures[0].body.as_ref().is_some_and(|body| {
                        String::from_utf8_lossy(body).contains("server_is_overloaded")
                    }));
                }
            }
        }
    }
}

#[test]
fn codex_initial_capacity_respects_the_attempt_budget_and_does_not_wait_for_eof() {
    for (wire, delimited) in [
        (OVERLOADED.to_owned(), true),
        (OVERLOADED.trim_end().to_owned(), false),
        (format!("{OVERLOADED}data: [DONE]\n\n"), true),
    ] {
        let host = host(vec![Bytes::from(wire)], 1);
        if delimited {
            host.state.lock().unwrap().scripted_pending_at = Some(1);
        }
        let core = codex_core(&host).unwrap();
        let result = block_on(core.execute(&host, input("/v1/responses")));
        assert!(
            matches!(result, Err(CoreError::UpstreamExhausted(_))),
            "{result:?}"
        );
        let state = host.state.lock().unwrap();
        assert_eq!(state.upstream_requests.len(), 1);
        assert_eq!(state.health[0].2, CredentialHealth::Degraded);
        assert!(state.settlements.is_empty());
        assert_eq!(state.admission_finishes, [false]);
    }
}

#[test]
fn codex_input_errors_before_output_do_not_retry_or_degrade_the_account() {
    let wire = OVERLOADED.replace("server_is_overloaded", "invalid_prompt");
    let host = host(vec![Bytes::from(wire)], 2);
    let core = codex_core(&host).unwrap();
    let output = consume(block_on(core.execute(&host, input("/v1/responses"))).unwrap());
    assert!(output.contains("invalid_prompt"));
    let state = host.state.lock().unwrap();
    assert_eq!(state.upstream_requests.len(), 1);
    assert!(state.health.is_empty());
}

#[test]
fn codex_prefix_timeout_releases_the_response_without_losing_buffered_events() {
    use futures_util::FutureExt;

    let host = host(vec![Bytes::from_static(PRELUDE.as_bytes())], 2);
    // MemoryHost's timer resolves immediately when the body is pending.
    host.state.lock().unwrap().scripted_pending_at = Some(1);
    let core = codex_core(&host).unwrap();
    let outcome = core
        .execute(&host, input("/v1/responses"))
        .now_or_never()
        .expect("the prefix timer must release a stalled stream")
        .unwrap();
    let ResponseBody::Stream(mut stream) = outcome.body else {
        panic!("stream")
    };
    let first = block_on(stream.next()).unwrap().unwrap();
    assert!(String::from_utf8_lossy(&first).contains("response.created"));
    outcome.stream_cancellation.unwrap().cancel();
    block_on(async { while stream.next().await.is_some() {} });
    let state = host.state.lock().unwrap();
    assert_eq!(state.upstream_requests.len(), 1);
    assert!(state.health.is_empty());
}

#[test]
fn codex_large_prefix_keeps_the_original_stream_and_does_not_retry() {
    let wire = format!(": {}\n\n{PRELUDE}{OVERLOADED}", "x".repeat(65 * 1024));
    let host = host(vec![Bytes::from(wire)], 2);
    let core = codex_core(&host).unwrap();
    let output = consume(block_on(core.execute(&host, input("/v1/responses"))).unwrap());
    assert!(output.contains("response.created"));
    assert!(output.contains("server_is_overloaded"));
    let state = host.state.lock().unwrap();
    assert_eq!(state.upstream_requests.len(), 1);
    assert_eq!(state.health[0].2, CredentialHealth::Degraded);
}

#[test]
fn codex_capacity_after_output_is_forwarded_without_replaying_the_request() {
    for coalesced in [false, true] {
        let chunks = if coalesced {
            vec![Bytes::from(format!("{OUTPUT}{OVERLOADED}"))]
        } else {
            vec![
                Bytes::from_static(OUTPUT.as_bytes()),
                Bytes::from_static(OVERLOADED.as_bytes()),
            ]
        };
        let host = host(chunks, 2);
        let core = codex_core(&host).unwrap();
        let output = consume(block_on(core.execute(&host, input("/v1/responses"))).unwrap());
        assert!(output.contains("visible"));
        assert!(output.contains("server_is_overloaded"));
        let state = host.state.lock().unwrap();
        assert_eq!(state.upstream_requests.len(), 1);
        assert_eq!(state.health.len(), 1);
        assert_eq!(state.health[0].2, CredentialHealth::Degraded);
    }
}
