use bytes::Bytes;
use gproxy_channel_api::{Channel, WsDuplex, WsFrame};
use http::Method;
use serde_json::{Value, json};

use super::super::{block_on, memory::MemoryHost, request};
use super::{core, response_event, response_event_for, target};
use crate::control::{FailoverBudget, Plan};
use crate::{Core, CredentialHealth, CredentialId, Ended, ResponseBody};

fn setup(channel: &str) -> (MemoryHost, Core<MemoryHost>) {
    let host = MemoryHost::new(false);
    {
        let mut state = host.state.lock().unwrap();
        state.credential.channel = channel.into();
        state.credential.kind = if channel == "codex" {
            "oauth"
        } else {
            "api_key"
        }
        .into();
        state.credential.secret = json!({
            "access_token": "fresh", "expires_at": i64::MAX, "api_key": "upstream-secret"
        });
        state.track_health_attempts = true;
        state.plan = Some(Plan {
            targets: vec![target(channel, 6)],
            budget: FailoverBudget { max_attempts: 1 },
        });
    }
    let channel: Box<dyn Channel> = match channel {
        "codex" => Box::new(gproxy_channels::CodexChannel),
        _ => Box::new(gproxy_channels::OpenAiChannel),
    };
    let core = core(&host, channel).unwrap();
    (host, core)
}

fn connect(host: &MemoryHost, core: &Core<MemoryHost>, id: &str) -> Box<dyn WsDuplex> {
    let mut input = request(false, id);
    input.method = Method::GET;
    input.body = Bytes::new();
    input.upgrade = true;
    let outcome = block_on(core.execute(host, input)).unwrap();
    let ResponseBody::WebSocket(socket) = outcome.body else {
        panic!("Responses request did not upgrade")
    };
    socket
}

fn start(socket: &mut dyn WsDuplex) {
    block_on(socket.send(WsFrame::Text(
        json!({"type":"response.create","model":"public-alias","input":"hi"}).to_string(),
    )))
    .unwrap();
}

fn receive(socket: &mut dyn WsDuplex, expected_type: &str) {
    let Some(WsFrame::Text(text)) = block_on(socket.recv()).unwrap() else {
        panic!("missing {expected_type}")
    };
    assert_eq!(
        serde_json::from_str::<Value>(&text).unwrap()["type"],
        expected_type
    );
}

fn health(value: CredentialHealth) -> (CredentialId, String, CredentialHealth) {
    (CredentialId(7), "upstream-model".into(), value)
}

fn failure_event(kind: &str, code: &str) -> String {
    if kind == "error" {
        return json!({"type":"error","code":code,"message":"request failed"}).to_string();
    }
    let mut event: Value = serde_json::from_str(&response_event(kind, 1)).unwrap();
    event["response"]["status"] = json!("failed");
    event["response"]["error"] = json!({"code":code,"message":"request failed"});
    event.to_string()
}

#[test]
fn overloaded_model_recovers_after_success_on_reused_or_new_socket() {
    for channel in ["codex", "openai"] {
        for failure_type in ["error", "response.failed"] {
            for reconnect in [false, true] {
                let (host, core) = setup(channel);
                // Other models and credentials must never receive the health update.
                let mut expected = vec![
                    (
                        CredentialId(7),
                        "other-model".into(),
                        CredentialHealth::Healthy,
                    ),
                    (
                        CredentialId(8),
                        "upstream-model".into(),
                        CredentialHealth::Healthy,
                    ),
                ];
                {
                    let mut state = host.state.lock().unwrap();
                    state.health = expected.clone();
                    state.socket_frames = [
                        WsFrame::Text(response_event("response.created", 0)),
                        WsFrame::Text(failure_event(failure_type, "server_is_overloaded")),
                    ]
                    .into();
                }
                let mut socket = connect(&host, &core, "ws-overloaded");
                start(socket.as_mut());
                assert_eq!(host.state.lock().unwrap().health, expected);
                receive(socket.as_mut(), "response.created");
                assert_eq!(host.state.lock().unwrap().health, expected);
                receive(socket.as_mut(), failure_type);
                expected.push(health(CredentialHealth::Degraded));
                assert_eq!(host.state.lock().unwrap().health, expected);

                if reconnect {
                    drop(socket);
                    socket = connect(&host, &core, "ws-recovered");
                }
                host.state.lock().unwrap().socket_frames = [
                    WsFrame::Text(response_event("response.created", 0)),
                    WsFrame::Text(response_event("response.completed", 1)),
                ]
                .into();
                start(socket.as_mut());
                receive(socket.as_mut(), "response.created");
                assert_eq!(host.state.lock().unwrap().health, expected);
                receive(socket.as_mut(), "response.completed");
                expected.push(health(CredentialHealth::Healthy));
                let state = host.state.lock().unwrap();
                assert_eq!(
                    state.health, expected,
                    "{channel} {failure_type} {reconnect}"
                );
                assert_eq!(state.socket_opens, if reconnect { 2 } else { 1 });
                assert_eq!(state.settlements.len(), 2);
                assert_eq!(state.admission_finishes, [true, true]);
                assert_eq!(state.health_attempts.len(), 2);
                assert_eq!(state.health_releases, state.health_attempts);
            }
        }
    }
}

#[test]
fn completed_response_recovers_without_usage_metadata() {
    let (host, core) = setup("codex");
    let prior = health(CredentialHealth::Degraded);
    let mut event: Value = serde_json::from_str(&response_event("response.completed", 1)).unwrap();
    event["response"].as_object_mut().unwrap().remove("usage");
    {
        let mut state = host.state.lock().unwrap();
        state.health.push(prior.clone());
        state.socket_frames = [WsFrame::Text(event.to_string())].into();
    }
    let mut socket = connect(&host, &core, "ws-no-usage");
    start(socket.as_mut());
    assert_eq!(
        host.state.lock().unwrap().health.as_slice(),
        std::slice::from_ref(&prior)
    );
    receive(socket.as_mut(), "response.completed");
    assert_eq!(
        host.state.lock().unwrap().health,
        [prior, health(CredentialHealth::Healthy)]
    );
}

#[test]
fn a_failed_generation_cannot_recover_from_a_later_synthetic_completion() {
    let (host, core) = setup("codex");
    host.state.lock().unwrap().socket_frames = [
        WsFrame::Text(response_event("response.created", 0)),
        WsFrame::Text(failure_event("response.failed", "server_is_overloaded")),
        WsFrame::Text(response_event("response.completed", 2)),
        WsFrame::Text(
            json!({"type":"response.inject.created","sequence_number":3,"response_id":"resp_ws"})
                .to_string(),
        ),
    ]
    .into();
    let mut socket = connect(&host, &core, "ws-sticky-failure");
    start(socket.as_mut());
    receive(socket.as_mut(), "response.created");
    block_on(
        socket.send(WsFrame::Text(
            json!({
                "type":"response.inject","response_id":"resp_ws",
                "input":[{"type":"function_call_output","call_id":"call_1","output":"ok"}]
            })
            .to_string(),
        )),
    )
    .unwrap();
    receive(socket.as_mut(), "response.failed");
    receive(socket.as_mut(), "response.completed");
    receive(socket.as_mut(), "response.inject.created");
    let state = host.state.lock().unwrap();
    assert_eq!(state.health, [health(CredentialHealth::Degraded)]);
    assert_eq!(state.settlements.len(), 1);
}

#[test]
fn steering_recovery_requires_a_new_successful_response_after_only_a_steer_interruption() {
    for case in [
        "same_id",
        "other_incomplete",
        "overload",
        "overload_after_steer",
        "stale_completion",
        "continuation_incomplete",
        "continuation_interrupted",
    ] {
        let (host, core) = setup("codex");
        let prior = health(CredentialHealth::Degraded);
        {
            let mut state = host.state.lock().unwrap();
            state.health.push(prior.clone());
            state.socket_frames = [WsFrame::Text(response_event("response.created", 0))].into();
        }
        let mut socket = connect(&host, &core, "ws-steer-preserves-failure");
        start(socket.as_mut());
        receive(socket.as_mut(), "response.created");
        block_on(
            socket.send(WsFrame::Text(
                json!({
                    "type":"response.steer","previous_response_id":"resp_ws","input":"continue"
                })
                .to_string(),
            )),
        )
        .unwrap();
        let first_terminal = if case == "overload" {
            failure_event("response.failed", "server_is_overloaded")
        } else {
            let mut incomplete: Value =
                serde_json::from_str(&response_event("response.incomplete", 1)).unwrap();
            incomplete["response"]["status"] = json!("incomplete");
            incomplete["response"]["incomplete_details"] = json!({
                "reason": if case == "other_incomplete" { "max_output_tokens" } else { "steered" }
            });
            incomplete.to_string()
        };
        let next_id = if case == "same_id" {
            "resp_ws"
        } else {
            "resp_next"
        };
        {
            let mut state = host.state.lock().unwrap();
            state.socket_frames.push_back(WsFrame::Text(first_terminal));
            if case == "overload_after_steer" {
                state.socket_frames.push_back(WsFrame::Text(failure_event(
                    "response.failed",
                    "server_is_overloaded",
                )));
            }
            state
                .socket_frames
                .push_back(WsFrame::Text(response_event_for(
                    "response.created",
                    next_id,
                    2,
                )));
            if case == "continuation_incomplete" {
                let mut incomplete: Value =
                    serde_json::from_str(&response_event_for("response.incomplete", next_id, 3))
                        .unwrap();
                incomplete["response"]["status"] = json!("incomplete");
                incomplete["response"]["incomplete_details"] =
                    json!({"reason":"max_output_tokens"});
                state
                    .socket_frames
                    .push_back(WsFrame::Text(incomplete.to_string()));
            } else if case != "continuation_interrupted" {
                state
                    .socket_frames
                    .push_back(WsFrame::Text(response_event_for(
                        "response.completed",
                        if case == "stale_completion" {
                            "resp_ws"
                        } else {
                            next_id
                        },
                        3,
                    )));
            }
            state.socket_frames.push_back(WsFrame::Close(Some(1000)));
        }
        while block_on(socket.recv()).unwrap().is_some() {}
        let mut expected = vec![prior];
        if matches!(case, "overload" | "overload_after_steer") {
            expected.push(health(CredentialHealth::Degraded));
        }
        let state = host.state.lock().unwrap();
        assert_eq!(state.health, expected, "{case}");
        assert_eq!(state.settlements.len(), 1, "{case}");
        assert_eq!(state.health_releases, state.health_attempts, "{case}");
    }
}

#[test]
fn incomplete_or_interrupted_responses_do_not_recover_degraded_model() {
    for ending in [
        "incomplete",
        "input_error",
        "client_close",
        "peer_close",
        "eof",
        "malformed",
    ] {
        let (host, core) = setup("codex");
        let prior = health(CredentialHealth::Degraded);
        {
            let mut state = host.state.lock().unwrap();
            state.health.push(prior.clone());
            state.socket_frames = [WsFrame::Text(response_event("response.created", 0))].into();
        }
        let mut socket = connect(&host, &core, "ws-incomplete");
        start(socket.as_mut());
        receive(socket.as_mut(), "response.created");
        let expected_end = match ending {
            "incomplete" => {
                let mut event: Value =
                    serde_json::from_str(&response_event("response.incomplete", 1)).unwrap();
                event["response"]["status"] = json!("incomplete");
                event["response"]["incomplete_details"] = json!({"reason":"max_output_tokens"});
                host.state
                    .lock()
                    .unwrap()
                    .socket_frames
                    .push_back(WsFrame::Text(event.to_string()));
                receive(socket.as_mut(), "response.incomplete");
                Ended::Complete
            }
            "input_error" => {
                host.state
                    .lock()
                    .unwrap()
                    .socket_frames
                    .push_back(WsFrame::Text(failure_event(
                        "response.failed",
                        "invalid_prompt",
                    )));
                receive(socket.as_mut(), "response.failed");
                Ended::Complete
            }
            "client_close" => {
                block_on(socket.send(WsFrame::Close(Some(1000)))).unwrap();
                Ended::Interrupted
            }
            "peer_close" => {
                host.state
                    .lock()
                    .unwrap()
                    .socket_frames
                    .push_back(WsFrame::Close(Some(1000)));
                assert!(matches!(
                    block_on(socket.recv()).unwrap(),
                    Some(WsFrame::Close(_))
                ));
                Ended::Interrupted
            }
            "malformed" => {
                host.state
                    .lock()
                    .unwrap()
                    .socket_frames
                    .push_back(WsFrame::Text(
                        json!({"type":"response.completed"}).to_string(),
                    ));
                receive(socket.as_mut(), "response.completed");
                block_on(socket.send(WsFrame::Close(Some(1000)))).unwrap();
                Ended::Interrupted
            }
            _ => {
                host.state.lock().unwrap().socket_closed = true;
                assert!(block_on(socket.recv()).unwrap().is_none());
                Ended::Interrupted
            }
        };
        let state = host.state.lock().unwrap();
        assert_eq!(state.health, [prior], "{ending}");
        assert_eq!(state.settlements.len(), 1, "{ending}");
        assert_eq!(state.settlements[0].ended, expected_end, "{ending}");
        assert_eq!(state.health_releases, state.health_attempts, "{ending}");
    }
}
