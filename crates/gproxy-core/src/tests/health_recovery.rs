use futures_util::FutureExt;

use super::*;

#[test]
fn cooling_models_do_not_consume_the_upstream_attempt_budget() {
    for all_cooling in [false, true] {
        let host = MemoryHost::new(false);
        {
            let mut state = host.state.lock().unwrap();
            state.credential.secret = json!({"access_token":"fresh","expires_at":i64::MAX});
            state.track_health_attempts = true;
            let first = target();
            let mut second = first.clone();
            second.credential = CredentialId(8);
            state.cooling_model_pairs = vec![(first.credential, first.upstream_model.clone())];
            if all_cooling {
                state
                    .cooling_model_pairs
                    .push((second.credential, second.upstream_model.clone()));
            }
            state.plan = Some(Plan {
                targets: vec![first, second],
                budget: FailoverBudget { max_attempts: 1 },
            });
        }
        let core = core(&host).unwrap();
        let result = block_on(core.execute(&host, request(false, "health-budget")));
        let state = host.state.lock().unwrap();
        if all_cooling {
            let Err(error) = result else {
                panic!("all models are cooling down");
            };
            assert!(matches!(
                error,
                CoreError::CredentialCoolingDown {
                    retry_after_secs: 30
                }
            ));
            assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert!(state.health_attempts.is_empty());
            assert!(state.upstream_requests.is_empty());
            assert!(state.health.is_empty());
        } else {
            assert_eq!(result.unwrap().status, StatusCode::OK);
            assert_eq!(
                state.health_attempts,
                [(CredentialId(8), "upstream-model".into(), 4)]
            );
            assert_eq!(state.health_attempts, state.health_releases);
            assert!(state.health_leases.is_empty());
            assert_eq!(state.upstream_requests.len(), 1);
            assert_eq!(state.settlements[0].credential_id, CredentialId(8));
        }
    }
}

#[test]
fn cancelling_a_buffered_attempt_releases_its_health_lease_without_recovery() {
    let host = MemoryHost::new(false);
    {
        let mut state = host.state.lock().unwrap();
        state.credential.secret = json!({"access_token":"fresh","expires_at":i64::MAX});
        state.track_health_attempts = true;
        state
            .scripted
            .push_back((StatusCode::OK, vec![Bytes::from_static(b"{")]));
        state.scripted_pending_at = Some(1);
    }
    let core = core(&host).unwrap();
    let selected = target();
    let result = core
        .invoke(&host, &selected, request(false, "health-cancel-buffered"))
        .now_or_never();
    assert!(result.is_none(), "the upstream body is still pending");
    let state = host.state.lock().unwrap();
    assert_eq!(state.health_attempts.len(), 1);
    assert_eq!(state.health_attempts, state.health_releases);
    assert!(state.health_leases.is_empty());
    assert!(state.health.is_empty());
}

#[test]
fn a_credential_that_died_after_planning_is_skipped_before_egress() {
    let host = MemoryHost::new(false);
    {
        let mut state = host.state.lock().unwrap();
        state.credential.secret = json!({"access_token":"fresh","expires_at":i64::MAX});
        let first = target();
        let mut second = first.clone();
        second.credential = CredentialId(8);
        state
            .unavailable_model_pairs
            .push((first.credential, first.upstream_model.clone()));
        state.plan = Some(Plan {
            targets: vec![first, second],
            budget: FailoverBudget { max_attempts: 1 },
        });
    }
    let core = core(&host).unwrap();
    let outcome = block_on(core.execute(&host, request(false, "health-dead-after-plan"))).unwrap();
    assert_eq!(outcome.status, StatusCode::OK);
    let state = host.state.lock().unwrap();
    assert_eq!(state.upstream_requests.len(), 1);
    assert_eq!(state.settlements[0].credential_id, CredentialId(8));
}

#[test]
fn dropping_or_cancelling_a_stream_releases_its_health_lease_without_recovery() {
    for cancel_inline in [false, true] {
        let host = MemoryHost::new(false);
        {
            let mut state = host.state.lock().unwrap();
            state.credential.secret = json!({"access_token":"fresh","expires_at":i64::MAX});
            state.track_health_attempts = true;
            state.scripted.push_back((StatusCode::OK, Vec::new()));
            state.scripted_pending_at = Some(1);
        }
        let core = core(&host).unwrap();
        let outcome =
            block_on(core.invoke(&host, &target(), request(true, "health-cancel-stream"))).unwrap();
        assert_eq!(host.state.lock().unwrap().health_leases.len(), 1);
        if cancel_inline {
            block_on(crate::funnel::cancel_outcome(outcome));
        } else {
            drop(outcome);
        }
        let state = host.state.lock().unwrap();
        assert_eq!(state.health_attempts.len(), 1);
        assert_eq!(state.health_attempts, state.health_releases);
        assert!(state.health_leases.is_empty());
        assert!(state.health.is_empty());
    }
}
