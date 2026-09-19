use std::future::Future as _;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use serde_json::json;

use super::{block_on, core, memory::MemoryHost, request, target};
use crate::{CacheBackend as _, CoreError, CredentialId, CredentialStore as _};

fn host() -> MemoryHost {
    let host = MemoryHost::new(false);
    host.state.lock().unwrap().credential.secret = json!({
        "access_token": "old", "refresh_token": "saved-refresh", "expires_at": i64::MAX,
    });
    host
}

#[test]
fn manual_refresh_uses_saved_token_even_before_expiry_and_rejects_stale_version() {
    let host = host();
    let core = core(&host).unwrap();
    let refreshed =
        block_on(core.refresh_credential(&target().provider, CredentialId(7), 4)).unwrap();
    assert_eq!(refreshed.credential.secret["access_token"], "fresh");
    assert_eq!(refreshed.credential.version, 5);
    assert_eq!(
        refreshed.refresh_token,
        Some(gproxy_channel_api::RefreshTokenStatus::NotReturned)
    );
    assert!(matches!(
        block_on(core.refresh_credential(&target().provider, CredentialId(7), 4)),
        Err(CoreError::CredentialVersionConflict)
    ));
    let state = host.state.lock().unwrap();
    assert_eq!(state.refresh_calls, 1);
    assert!(!state.cache.contains_key("refresh:7"));
}

#[test]
fn manual_refresh_rejects_wrong_channel_or_missing_refresh_token_before_upstream() {
    let host = host();
    let core = core(&host).unwrap();
    host.state.lock().unwrap().credential.channel = "different".into();
    assert!(matches!(
        block_on(core.refresh_credential(&target().provider, CredentialId(7), 4)),
        Err(CoreError::Unsupported)
    ));
    {
        let mut state = host.state.lock().unwrap();
        state.credential.channel = "memory".into();
        state
            .credential
            .secret
            .as_object_mut()
            .unwrap()
            .remove("refresh_token");
    }
    assert!(matches!(
        block_on(core.refresh_credential(&target().provider, CredentialId(7), 4)),
        Err(CoreError::Unsupported)
    ));
    assert_eq!(host.state.lock().unwrap().refresh_calls, 0);
}

#[test]
fn parallel_manual_refreshes_reuse_the_first_rotation() {
    let host = host();
    {
        let mut state = host.state.lock().unwrap();
        state.continuations_enabled = true;
        state.defer_spawned = true;
    }
    let core = core(&host).unwrap();
    let target = target();
    let mut first = Box::pin(core.refresh_credential(&target.provider, CredentialId(7), 4));
    let mut second = Box::pin(core.refresh_credential(&target.provider, CredentialId(7), 4));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    let tasks = std::mem::take(&mut host.state.lock().unwrap().spawned_tasks);
    assert_eq!(tasks.len(), 2);
    for task in tasks {
        block_on(task);
    }
    let first = block_on(first).unwrap();
    let second = block_on(second).unwrap();
    assert!(first.refresh_token.is_some());
    assert_eq!(second.refresh_token, None);
    assert_eq!(first.credential.version, second.credential.version);
    assert_eq!(host.state.lock().unwrap().refresh_calls, 1);
}

#[test]
fn an_automatic_refresh_finishes_after_request_cancellation() {
    let host = host();
    {
        let mut state = host.state.lock().unwrap();
        state.credential.secret["expires_at"] = json!(0);
        state.continuations_enabled = true;
        state.defer_spawned = true;
        state.refresh_pending = true;
        state.long_waits_pending = true;
    }
    let core = core(&host).unwrap();
    let target = target();
    let mut caller = Box::pin(core.invoke(&host, &target, request(false, "cancel-refresh")));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(caller.as_mut().poll(&mut cx).is_pending());
    let mut tasks = std::mem::take(&mut host.state.lock().unwrap().spawned_tasks);
    assert_eq!(tasks.len(), 1);
    let mut refreshing = tasks.remove(0);
    assert!(refreshing.as_mut().poll(&mut cx).is_pending());
    assert_eq!(host.state.lock().unwrap().refresh_calls, 1);
    drop(caller);
    host.state.lock().unwrap().refresh_pending = false;
    block_on(refreshing);
    let state = host.state.lock().unwrap();
    assert_eq!(state.credential.version, 5);
    assert_eq!(state.credential.secret["access_token"], "fresh");
    assert!(!state.cache.contains_key("refresh:7"));
}

#[test]
fn token_rotation_retries_metadata_and_quota_conflicts_without_another_upstream_call() {
    let host = host();
    host.state.lock().unwrap().metadata_conflicts = 2;
    let core = core(&host).unwrap();
    let refreshed =
        block_on(core.refresh_credential(&target().provider, CredentialId(7), 4)).unwrap();
    assert_eq!(refreshed.credential.version, 7);
    assert_eq!(refreshed.credential.secret["access_token"], "fresh");
    assert_eq!(
        refreshed.credential.secret["quota_api_key"],
        "new-quota-key"
    );
    assert!(refreshed.refresh_token.is_some());
    let state = host.state.lock().unwrap();
    assert_eq!(state.refresh_calls, 1);
    assert_eq!(state.rotations, [4, 5, 6]);
}

#[test]
fn token_rotation_does_not_overwrite_a_concurrently_replaced_token() {
    let host = host();
    host.state.lock().unwrap().conflict = true;
    let core = core(&host).unwrap();
    let refreshed =
        block_on(core.refresh_credential(&target().provider, CredentialId(7), 4)).unwrap();
    assert_eq!(refreshed.credential.secret["access_token"], "peer");
    assert_eq!(refreshed.refresh_token, None);
    assert_eq!(host.state.lock().unwrap().rotations, [4]);
}

#[test]
fn refresh_failure_preserves_the_credential_and_releases_its_lease() {
    let host = host();
    host.state.lock().unwrap().refresh_error = true;
    let core = core(&host).unwrap();
    assert!(block_on(core.refresh_credential(&target().provider, CredentialId(7), 4)).is_err());
    {
        let state = host.state.lock().unwrap();
        assert_eq!(state.credential.version, 4);
        assert_eq!(state.credential.secret["refresh_token"], "saved-refresh");
        assert!(state.rotations.is_empty());
        assert!(!state.cache.contains_key("refresh:7"));
    }
    host.state.lock().unwrap().refresh_error = false;
    assert!(block_on(core.refresh_credential(&target().provider, CredentialId(7), 4)).is_ok());
}

#[test]
fn automatic_refresh_failure_is_backed_off_before_the_next_upstream_send() {
    let host = host();
    {
        let mut state = host.state.lock().unwrap();
        state.credential.secret["expires_at"] = json!(0);
        state.refresh_error = true;
    }
    let core = core(&host).unwrap();
    let target = target();
    for _ in 0..3 {
        assert!(
            block_on(crate::execution::credential::load_fresh(
                &core,
                core.channels.get("memory").unwrap(),
                CredentialId(7),
                &target.provider,
            ))
            .is_err()
        );
    }
    assert_eq!(host.state.lock().unwrap().refresh_calls, 1);
}

#[test]
fn explicit_refresh_bypasses_cooldown_and_success_removes_the_old_version_backoff() {
    let host = host();
    {
        let mut state = host.state.lock().unwrap();
        state.credential.secret["expires_at"] = json!(0);
        state.refresh_error = true;
    }
    let core = core(&host).unwrap();
    let target = target();
    assert!(
        block_on(crate::execution::credential::load_fresh(
            &core,
            core.channels.get("memory").unwrap(),
            CredentialId(7),
            &target.provider,
        ))
        .is_err()
    );
    assert!(
        block_on(host.get("gproxy:credential-refresh:7:4"))
            .unwrap()
            .is_some()
    );
    host.state.lock().unwrap().refresh_error = false;
    let result = block_on(core.refresh_credential(&target.provider, CredentialId(7), 4)).unwrap();
    assert_eq!(result.credential.version, 5);
    assert!(
        block_on(host.get("gproxy:credential-refresh:7:4"))
            .unwrap()
            .is_none()
    );
    assert_eq!(host.state.lock().unwrap().refresh_calls, 2);
}

#[test]
fn an_authoritative_peer_rotation_bypasses_an_old_cached_versions_refresh_backoff() {
    let host = host();
    {
        let mut state = host.state.lock().unwrap();
        state.credential.secret["expires_at"] = json!(0);
        state.refresh_error = true;
    }
    let core = core(&host).unwrap();
    let target = target();
    assert!(
        block_on(crate::execution::credential::load_fresh(
            &core,
            core.channels.get("memory").unwrap(),
            CredentialId(7),
            &target.provider,
        ))
        .is_err()
    );
    {
        let mut state = host.state.lock().unwrap();
        state.cached_credential = Some(state.credential.clone());
        state.credential.version += 1;
        state.credential.secret =
            json!({"access_token":"peer", "refresh_token":"new", "expires_at": i64::MAX});
    }
    let result = block_on(crate::execution::credential::load_fresh(
        &core,
        core.channels.get("memory").unwrap(),
        CredentialId(7),
        &target.provider,
    ))
    .unwrap();
    assert_eq!(result.version, 5);
    assert_eq!(result.secret["access_token"], "peer");
    assert_eq!(host.state.lock().unwrap().refresh_calls, 1);
}

#[test]
fn automatic_refreshes_already_queued_recheck_backoff_after_taking_the_lease() {
    let host = host();
    {
        let mut state = host.state.lock().unwrap();
        state.credential.secret["expires_at"] = json!(0);
        state.refresh_error = true;
        state.continuations_enabled = true;
        state.defer_spawned = true;
    }
    let core = core(&host).unwrap();
    let target = target();
    let channel = core.channels.get("memory").unwrap();
    let mut first = Box::pin(crate::execution::credential::load_fresh(
        &core,
        channel,
        CredentialId(7),
        &target.provider,
    ));
    let mut second = Box::pin(crate::execution::credential::load_fresh(
        &core,
        channel,
        CredentialId(7),
        &target.provider,
    ));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    let tasks = std::mem::take(&mut host.state.lock().unwrap().spawned_tasks);
    for task in tasks {
        block_on(task);
    }
    assert!(block_on(first).is_err());
    assert!(matches!(
        block_on(second),
        Err(CoreError::CredentialRefreshCoolingDown {
            retry_after_secs: 1..=30
        })
    ));
    let state = host.state.lock().unwrap();
    assert_eq!(state.refresh_calls, 1);
    assert_eq!(state.health.len(), 1);
}

#[test]
fn late_success_from_a_previous_owner_cannot_clear_a_newer_failure_backoff() {
    let host = host();
    {
        let mut state = host.state.lock().unwrap();
        state.continuations_enabled = true;
        state.defer_spawned = true;
        state.refresh_pending = true;
        state.long_waits_pending = true;
    }
    let core = core(&host).unwrap();
    let target = target();
    let mut first = Box::pin(core.refresh_credential(&target.provider, CredentialId(7), 4));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    let mut first_task = host.state.lock().unwrap().spawned_tasks.remove(0);
    assert!(first_task.as_mut().poll(&mut cx).is_pending());
    block_on(host.delete("refresh:7")).unwrap();
    {
        let mut state = host.state.lock().unwrap();
        state.refresh_pending = false;
        state.refresh_error = true;
    }
    let mut second = Box::pin(core.refresh_credential(&target.provider, CredentialId(7), 4));
    assert!(second.as_mut().poll(&mut cx).is_pending());
    let second_task = host.state.lock().unwrap().spawned_tasks.remove(0);
    block_on(second_task);
    assert!(block_on(second).is_err());
    let newer = block_on(host.get("gproxy:credential-refresh:7:4"))
        .unwrap()
        .unwrap();
    host.state.lock().unwrap().refresh_error = false;
    block_on(first_task);
    assert!(block_on(first).is_ok());
    assert_eq!(
        block_on(host.get("gproxy:credential-refresh:7:4")).unwrap(),
        Some(newer)
    );
}

#[test]
fn automatic_refresh_backoff_increases_to_five_minutes_and_expiry_allows_another_attempt() {
    let host = host();
    {
        let mut state = host.state.lock().unwrap();
        state.credential.secret["expires_at"] = json!(0);
        state.refresh_error = true;
    }
    let core = core(&host).unwrap();
    let target = target();
    for expected_delay in [30, 60, 120, 240, 300, 300] {
        assert!(
            block_on(crate::execution::credential::load_fresh(
                &core,
                core.channels.get("memory").unwrap(),
                CredentialId(7),
                &target.provider,
            ))
            .is_err()
        );
        let cooling = block_on(crate::execution::credential::load_fresh(
            &core,
            core.channels.get("memory").unwrap(),
            CredentialId(7),
            &target.provider,
        ));
        let Err(CoreError::CredentialRefreshCoolingDown { retry_after_secs }) = cooling else {
            panic!("refresh was not cooled down");
        };
        assert!((expected_delay - 1..=expected_delay).contains(&retry_after_secs));
        let mut state = host.state.lock().unwrap();
        let backoff = state
            .cache
            .get_mut("gproxy:credential-refresh:7:4")
            .unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(backoff).unwrap();
        value["retry_at"] = json!(0);
        *backoff = serde_json::to_vec(&value).unwrap();
    }
    assert_eq!(host.state.lock().unwrap().refresh_calls, 6);
}

#[test]
fn refresh_cooldown_candidates_do_not_consume_the_generation_attempt_budget() {
    let host = host();
    {
        let mut state = host.state.lock().unwrap();
        state.credential.secret["expires_at"] = json!(0);
        state.refresh_error = true;
    }
    let core = core(&host).unwrap();
    let first = target();
    assert!(
        block_on(crate::execution::credential::load_fresh(
            &core,
            core.channels.get("memory").unwrap(),
            CredentialId(7),
            &first.provider,
        ))
        .is_err()
    );
    {
        let mut state = host.state.lock().unwrap();
        let mut second = first.clone();
        second.credential = CredentialId(8);
        state.refresh_error = false;
        state.plan = Some(crate::Plan {
            targets: vec![first, second],
            budget: crate::control::FailoverBudget { max_attempts: 1 },
        });
    }
    let response = block_on(core.execute(&host, request(false, "refresh-backoff-budget"))).unwrap();
    assert_eq!(response.status, http::StatusCode::OK);
    let state = host.state.lock().unwrap();
    assert_eq!(state.refresh_calls, 2);
    assert_eq!(state.upstream_requests.len(), 1);
    assert_eq!(state.settlements[0].credential_id, CredentialId(8));
}

#[test]
fn refresh_waiters_are_bounded_and_cannot_release_another_owner() {
    let host = host();
    let owner = b"another-owner";
    assert!(
        block_on(host.lease_refresh(CredentialId(7), owner, Duration::from_secs(120))).unwrap()
    );
    let core = core(&host).unwrap();
    assert!(block_on(core.refresh_credential(&target().provider, CredentialId(7), 4)).is_err());
    assert_eq!(
        block_on(host.get("refresh:7")).unwrap(),
        Some(owner.to_vec())
    );
    assert_eq!(host.state.lock().unwrap().wait_calls, 120);
    assert_eq!(host.state.lock().unwrap().refresh_calls, 0);
    assert!(
        !block_on(host.renew_refresh(CredentialId(7), b"wrong-owner", Duration::from_secs(120)))
            .unwrap()
    );
    block_on(host.release_refresh(CredentialId(7), b"wrong-owner")).unwrap();
    assert_eq!(
        block_on(host.get("refresh:7")).unwrap(),
        Some(owner.to_vec())
    );
    block_on(host.release_refresh(CredentialId(7), owner)).unwrap();
    assert_eq!(block_on(host.get("refresh:7")).unwrap(), None);
}

#[test]
fn concurrent_manual_and_automatic_refresh_share_the_same_rotation() {
    let host = host();
    {
        let mut state = host.state.lock().unwrap();
        state.credential.secret["expires_at"] = json!(0);
        state.continuations_enabled = true;
        state.defer_spawned = true;
    }
    let core = core(&host).unwrap();
    let target = target();
    let mut automatic = Box::pin(crate::execution::credential::load_fresh(
        &core,
        core.channels.get("memory").unwrap(),
        CredentialId(7),
        &target.provider,
    ));
    let mut manual = Box::pin(core.refresh_credential(&target.provider, CredentialId(7), 4));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(automatic.as_mut().poll(&mut cx).is_pending());
    assert!(manual.as_mut().poll(&mut cx).is_pending());
    let tasks = std::mem::take(&mut host.state.lock().unwrap().spawned_tasks);
    for task in tasks {
        block_on(task);
    }
    assert_eq!(block_on(automatic).unwrap().version, 5);
    let Poll::Ready(Ok(manual)) = manual.as_mut().poll(&mut cx) else {
        panic!("manual waiter did not finish");
    };
    assert_eq!(manual.credential.version, 5);
    assert_eq!(manual.refresh_token, None);
    assert_eq!(host.state.lock().unwrap().refresh_calls, 1);
}
