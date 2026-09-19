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
        state.long_waits_pending = true;
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
fn codex_prefix_timeout_retries_without_releasing_the_unchecked_response() {
    use futures_util::FutureExt;

    let host = host(vec![Bytes::from_static(PRELUDE.as_bytes())], 2);
    // MemoryHost's timer resolves immediately when the body is pending.
    {
        let mut state = host.state.lock().unwrap();
        state.scripted_pending_at = Some(1);
        state.long_waits_pending = false;
    }
    let core = codex_core(&host).unwrap();
    let outcome = core
        .execute(&host, input("/v1/responses"))
        .now_or_never()
        .expect("the prefix timer must abandon the stalled attempt")
        .unwrap();
    let output = consume(outcome);
    assert!(output.contains("ok"));
    assert!(!output.contains("\"first\""));
    let state = host.state.lock().unwrap();
    assert_eq!(state.upstream_requests.len(), 2);
    assert_eq!(state.health[0].2, CredentialHealth::Degraded);
    assert_eq!(
        state.settlements[0].credential_id,
        CredentialId(codex_target().credential.0 + 1)
    );
}

#[test]
fn codex_capacity_after_large_comments_still_retries() {
    let wire = format!(": {}\n\n{PRELUDE}{OVERLOADED}", "x".repeat(2 * 1024 * 1024));
    let host = host(vec![Bytes::from(wire)], 2);
    let core = codex_core(&host).unwrap();
    let output = consume(block_on(core.execute(&host, input("/v1/responses"))).unwrap());
    assert!(output.contains("ok"));
    assert!(!output.contains("server_is_overloaded"));
    assert!(!output.contains("\"first\""));
    let state = host.state.lock().unwrap();
    assert_eq!(state.upstream_requests.len(), 2);
    assert_eq!(state.health[0].2, CredentialHealth::Degraded);
}

#[test]
fn codex_capacity_after_large_lifecycle_metadata_switches_accounts() {
    for path in ["/v1/responses", "/v1/chat/completions", "/v1/messages"] {
        // Cover both cumulative metadata and an individual frame exceeding the
        // former 64 KiB limit. Neither instructions nor tool schemas are output.
        for instruction_bytes in [40 * 1024, 70 * 1024, 2 * 1024 * 1024] {
            let mut value: serde_json::Value =
                serde_json::from_str(PRELUDE.trim().strip_prefix("data: ").unwrap()).unwrap();
            value["response"]["instructions"] = json!("x".repeat(instruction_bytes));
            value["response"]["tools"] = json!([{
                "type":"function", "name":"example", "parameters":{"type":"object"}
            }]);
            let created = format!("data: {value}\n\n");
            value["type"] = json!("response.in_progress");
            let wire = format!("{created}data: {value}\n\n{OVERLOADED}");
            for chunk_size in [1024, 8192, wire.len()] {
                let host = host(
                    wire.as_bytes()
                        .chunks(chunk_size)
                        .map(Bytes::copy_from_slice)
                        .collect(),
                    2,
                );
                let core = codex_core(&host).unwrap();
                let output = consume(block_on(core.execute(&host, input(path))).unwrap());
                assert!(output.contains("ok"), "{path}: {output}");
                assert!(!output.contains("server_is_overloaded"));
                assert!(!output.contains("\"first\""));
                let state = host.state.lock().unwrap();
                let first = codex_target();
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
            }
        }
    }
}

#[test]
fn codex_fragment_count_does_not_disable_capacity_failover() {
    let wire = format!(": {}\n\n{PRELUDE}{OVERLOADED}", "x".repeat(70 * 1024));
    for chunk_size in [1, 7] {
        let host = host(
            wire.as_bytes()
                .chunks(chunk_size)
                .map(Bytes::copy_from_slice)
                .collect(),
            2,
        );
        let core = codex_core(&host).unwrap();
        let output = consume(block_on(core.execute(&host, input("/v1/responses"))).unwrap());
        assert!(output.contains("ok"));
        assert!(!output.contains("server_is_overloaded"));
        assert_eq!(host.state.lock().unwrap().upstream_requests.len(), 2);
    }
}

#[test]
fn codex_buffer_failure_returns_a_local_error_without_releasing_or_retrying_the_response() {
    let host = host(
        vec![
            Bytes::from_static(PRELUDE.as_bytes()),
            Bytes::from_static(OVERLOADED.as_bytes()),
        ],
        2,
    );
    {
        let mut state = host.state.lock().unwrap();
        state.stream_start_budget = crate::host::StreamStartBudget::new(1);
        state.track_health_attempts = true;
    }
    let core = codex_core(&host).unwrap();
    let result = block_on(core.execute(&host, input("/v1/responses")));
    assert!(matches!(result, Err(CoreError::StreamStartOverloaded)));
    let state = host.state.lock().unwrap();
    assert_eq!(state.upstream_requests.len(), 1);
    assert!(state.health.is_empty());
    assert!(state.settlements.is_empty());
    assert_eq!(state.admission_finishes, [false]);
    assert!(state.health_leases.is_empty());
}

#[test]
fn codex_concurrent_large_prefixes_reject_at_the_shared_budget_and_cancel_cleanly() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let budget = crate::host::StreamStartBudget::new(16 * 1024 * 1024);
    let fragment = Bytes::from(format!(":{}", "x".repeat(2 * 1024 * 1024 - 1)));
    let hosts: Vec<_> = (0..32)
        .map(|_| {
            let host = host(vec![fragment.clone()], 2);
            {
                let mut state = host.state.lock().unwrap();
                state.stream_start_budget = budget.clone();
                state.scripted_pending_at = Some(1);
                state.capture_response_body = false;
                state.track_health_attempts = true;
            }
            host
        })
        .collect();
    let cores: Vec<_> = hosts.iter().map(|host| codex_core(host).unwrap()).collect();
    let mut pending = Vec::new();
    let mut rejected = 0;
    let mut cx = Context::from_waker(Waker::noop());
    for (host, core) in hosts.iter().zip(&cores) {
        let mut future = Box::pin(core.execute(host, input("/v1/responses")));
        match future.as_mut().poll(&mut cx) {
            Poll::Pending => pending.push(future),
            Poll::Ready(Err(error @ CoreError::StreamStartOverloaded)) => {
                rejected += 1;
                assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
                assert_eq!(error.body_json()["error"]["code"], "gateway_overloaded");
                let state = host.state.lock().unwrap();
                assert_eq!(state.upstream_requests.len(), 1);
                assert!(state.health.is_empty());
                assert!(state.health_leases.is_empty());
                assert_eq!(state.admission_finishes, [false]);
            }
            Poll::Ready(other) => panic!("unexpected inspection result: {other:?}"),
        }
        assert!(budget.in_use() <= budget.limit());
    }
    assert!(!pending.is_empty());
    assert!(rejected > 0);
    println!(
        "pending={} rejected={rejected} reserved_bytes={} budget_bytes={}",
        pending.len(),
        budget.in_use(),
        budget.limit()
    );
    drop(pending);
    assert_eq!(
        budget.in_use(),
        0,
        "cancellation must release every reservation"
    );
    for host in hosts {
        assert!(host.state.lock().unwrap().health_leases.is_empty());
    }

    let host = host(vec![Bytes::from_static(OVERLOADED.as_bytes())], 2);
    {
        let mut state = host.state.lock().unwrap();
        state.stream_start_budget = budget.clone();
        state.capture_response_body = false;
    }
    let core = codex_core(&host).unwrap();
    assert!(consume(block_on(core.execute(&host, input("/v1/responses"))).unwrap()).contains("ok"));
    assert_eq!(host.state.lock().unwrap().upstream_requests.len(), 2);
    assert_eq!(
        budget.in_use(),
        0,
        "pool must remain usable after pressure and retries"
    );
}

#[test]
fn codex_logging_does_not_retain_a_large_failed_prefix() {
    let wire = format!(": {}\n\n{OVERLOADED}", "x".repeat(2 * 1024 * 1024));
    let host = host(vec![Bytes::from(wire)], 1);
    let budget = host.state.lock().unwrap().stream_start_budget.clone();
    let core = codex_core(&host).unwrap();
    assert!(matches!(
        block_on(core.execute(&host, input("/v1/responses"))),
        Err(CoreError::UpstreamExhausted(_))
    ));
    assert_eq!(budget.in_use(), 0);
    let state = host.state.lock().unwrap();
    assert!(!state.captures.is_empty());
    assert!(
        state
            .captures
            .iter()
            .all(|capture| capture.body.as_ref().is_none_or(Bytes::is_empty))
    );
    assert_eq!(state.health[0].2, CredentialHealth::Degraded);
}

#[test]
fn codex_large_requests_retry_within_the_budget_and_fail_locally_when_it_is_exhausted() {
    for (size, fits) in [(20 * 1024 * 1024, true), (40 * 1024 * 1024, false)] {
        let mut wire = String::with_capacity(size + 512);
        wire.push_str(
            "data: {\"type\":\"response.created\",\"response\":{\"output\":[],\"instructions\":\"",
        );
        wire.extend(std::iter::repeat_n('x', size));
        wire.push_str("\"}}\n\n");
        wire.push_str(OVERLOADED);
        let host = host(vec![Bytes::from(wire)], 2);
        let budget = crate::host::StreamStartBudget::new(128 * 1024 * 1024);
        {
            let mut state = host.state.lock().unwrap();
            state.stream_start_budget = budget.clone();
            state.capture_response_body = false;
            state.track_health_attempts = true;
        }
        let core = codex_core(&host).unwrap();
        let result = block_on(core.execute(&host, input("/v1/responses")));
        if fits {
            let output = consume(result.unwrap());
            assert!(output.contains("ok"));
            assert!(!output.contains("server_is_overloaded"));
            let state = host.state.lock().unwrap();
            assert_eq!(state.upstream_requests.len(), 2);
            assert_eq!(state.health[0].2, CredentialHealth::Degraded);
        } else {
            assert!(matches!(result, Err(CoreError::StreamStartOverloaded)));
            let state = host.state.lock().unwrap();
            assert_eq!(state.upstream_requests.len(), 1);
            assert!(state.health.is_empty());
        }
        assert_eq!(budget.in_use(), 0);
        assert!(host.state.lock().unwrap().health_leases.is_empty());
    }
}

#[test]
fn codex_opening_timeout_respects_the_attempt_budget() {
    let host = host(vec![Bytes::from_static(PRELUDE.as_bytes())], 1);
    {
        let mut state = host.state.lock().unwrap();
        state.scripted_pending_at = Some(1);
        state.long_waits_pending = false;
    }
    let core = codex_core(&host).unwrap();
    assert!(matches!(
        block_on(core.execute(&host, input("/v1/responses"))),
        Err(CoreError::UpstreamExhausted(_))
    ));
    let state = host.state.lock().unwrap();
    assert_eq!(state.upstream_requests.len(), 1);
    assert!(state.settlements.is_empty());
    assert_eq!(state.admission_finishes, [false]);
}

#[test]
fn codex_observed_output_cannot_be_retried_due_to_a_deadline() {
    let host = host(vec![Bytes::from(format!("{OUTPUT}{OVERLOADED}"))], 2);
    host.state.lock().unwrap().long_waits_pending = false;
    let core = codex_core(&host).unwrap();
    let output = consume(block_on(core.execute(&host, input("/v1/responses"))).unwrap());
    assert!(output.contains("visible"));
    assert!(output.contains("server_is_overloaded"));
    assert_eq!(host.state.lock().unwrap().upstream_requests.len(), 1);
}

#[test]
fn codex_early_eof_retries_before_returning_a_response() {
    let host = host(vec![Bytes::from_static(PRELUDE.as_bytes())], 2);
    let core = codex_core(&host).unwrap();
    let output = consume(block_on(core.execute(&host, input("/v1/responses"))).unwrap());
    assert!(output.contains("ok"));
    assert!(!output.contains("\"first\""));
    let state = host.state.lock().unwrap();
    assert_eq!(state.upstream_requests.len(), 2);
    assert_eq!(state.health[0].2, CredentialHealth::Degraded);
    assert_eq!(state.settlements.len(), 1);
}

#[test]
fn codex_large_successful_prefix_preserves_metadata_and_emits_output_once() {
    let mut value: serde_json::Value =
        serde_json::from_str(PRELUDE.trim().strip_prefix("data: ").unwrap()).unwrap();
    let instructions = "large-instructions-".repeat(128 * 1024);
    value["response"]["instructions"] = json!(instructions);
    let completed = json!({"type":"response.completed", "response":{
        "id":"first", "object":"response", "status":"completed", "output":[],
        "usage":{"input_tokens":1,"output_tokens":7,"total_tokens":8}
    }});
    let wire = format!("data: {value}\n\n{OUTPUT}data: {completed}\n\n");
    let host = host(
        wire.as_bytes()
            .chunks(8192)
            .map(Bytes::copy_from_slice)
            .collect(),
        2,
    );
    let core = codex_core(&host).unwrap();
    let output = consume(block_on(core.execute(&host, input("/v1/responses"))).unwrap());
    assert!(output.contains(&instructions));
    assert_eq!(output.matches("\"delta\":\"visible\"").count(), 1);
    assert!(output.contains("response.completed"));
    let state = host.state.lock().unwrap();
    assert_eq!(state.upstream_requests.len(), 1);
    assert_eq!(state.health[0].2, CredentialHealth::Healthy);
    assert_eq!(state.settlements.len(), 1);
    assert_eq!(state.settlements[0].usage.output_tokens, 7);
}

#[test]
fn codex_empty_tool_activity_is_not_retried_even_without_visible_text() {
    let tool = json!({"type":"response.output_item.added", "output_index":0, "item":{
        "type":"function_call", "id":"tool", "call_id":"call", "name":"example",
        "arguments":"", "status":"in_progress"
    }});
    for chunk_size in [1, 1024] {
        let wire = format!("{PRELUDE}data: {tool}\n\n{OVERLOADED}");
        let host = host(
            wire.as_bytes()
                .chunks(chunk_size)
                .map(Bytes::copy_from_slice)
                .collect(),
            2,
        );
        let core = codex_core(&host).unwrap();
        let output = consume(block_on(core.execute(&host, input("/v1/responses"))).unwrap());
        assert!(output.contains("server_is_overloaded"));
        assert_eq!(host.state.lock().unwrap().upstream_requests.len(), 1);
    }
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
