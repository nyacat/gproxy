use super::*;

#[test]
fn native_websocket_skips_cooling_candidates_without_spending_the_budget() {
    for dead in [false, true] {
        let host = MemoryHost::new(false);
        {
            let mut state = host.state.lock().unwrap();
            state.credential.channel = "openai".into();
            state.credential.kind = "api_key".into();
            state.credential.secret = json!({"api_key":"upstream-secret"});
            state.track_health_attempts = true;
            let mut first = target("openai", 6);
            first.credential = CredentialId(6);
            let unavailable = (first.credential, first.upstream_model.clone());
            if dead {
                state.unavailable_model_pairs.push(unavailable);
            } else {
                state.cooling_model_pairs.push(unavailable);
            }
            state.plan = Some(Plan {
                targets: vec![first, target("openai", 7)],
                budget: FailoverBudget { max_attempts: 1 },
            });
        }
        let core = core(&host, Box::new(gproxy_channels::OpenAiChannel)).unwrap();
        let mut input = request(false, "ws-probe-budget");
        input.method = Method::GET;
        input.body = Bytes::new();
        input.upgrade = true;
        let outcome = block_on(core.execute(&host, input)).unwrap();
        let ResponseBody::WebSocket(mut socket) = outcome.body else {
            panic!("socket")
        };
        block_on(socket.send(gproxy_channel_api::WsFrame::Text(
            json!({"type":"response.create","model":"upstream-model","input":"hi"}).to_string(),
        )))
        .unwrap();
        {
            let state = host.state.lock().unwrap();
            assert_eq!(state.socket_opens, 1);
            assert_eq!(
                state.health_attempts,
                [(CredentialId(7), "upstream-model".into(), 4)]
            );
            assert_eq!(state.health_leases.len(), 1);
        }
        drop(socket);
        let state = host.state.lock().unwrap();
        assert_eq!(state.health_attempts, state.health_releases);
        assert!(state.health_leases.is_empty());
    }
}

#[test]
fn failed_native_attempts_reduce_the_http_fallback_budget() {
    let host = MemoryHost::new(false);
    {
        let mut state = host.state.lock().unwrap();
        state.credential.channel = "openai".into();
        state.credential.kind = "api_key".into();
        state.credential.secret = json!({"api_key":"upstream-secret"});
        state.plan = Some(Plan {
            targets: vec![target("openai", 7)],
            budget: FailoverBudget { max_attempts: 1 },
        });
        state.socket_statuses = [500].into();
    }
    let core = core(&host, Box::new(gproxy_channels::OpenAiChannel)).unwrap();
    let mut input = request(false, "ws-http-budget");
    input.method = Method::GET;
    input.body = Bytes::new();
    input.upgrade = true;
    let outcome = block_on(core.execute(&host, input)).unwrap();
    let ResponseBody::WebSocket(mut socket) = outcome.body else {
        panic!("socket")
    };
    assert!(
        block_on(socket.send(gproxy_channel_api::WsFrame::Text(
            json!({"type":"response.create","model":"upstream-model","input":"hi"}).to_string(),
        )))
        .is_err()
    );
    let state = host.state.lock().unwrap();
    assert_eq!(state.socket_opens, 1);
    assert!(
        state.upstream_requests.is_empty(),
        "the real native attempt exhausted this request's budget"
    );
}
