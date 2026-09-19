use super::*;
use gproxy_core::{ControlPlane, CredentialStore};
use gproxy_store::records::CredentialHealthInput;

async fn fixture() -> (crate::tests::setup::Fixture, Target, u64) {
    let fixture = crate::tests::setup::fixture().await;
    fixture.app.shutdown();
    fixture.app.drain_background().await;
    let host = &fixture.app.inner.host;
    let target = host
        .services
        .control
        .resolve(
            Some("public-model"),
            &gproxy_core::RoutingMode::Aggregated,
            None,
        )
        .unwrap()
        .targets
        .remove(0);
    let version = host.load(target.credential).await.unwrap().version;
    (fixture, target, version)
}

#[allow(clippy::too_many_arguments)]
async fn observe(
    host: &AppHost,
    target: &Target,
    credential_version: u64,
    state: CredentialHealthState,
    age: i64,
    version: i64,
    publish: bool,
) -> CredentialHealthRecord {
    let input = CredentialHealthInput {
        credential_id: target.credential.0,
        model: target.upstream_model.clone(),
        credential_version,
        version,
        state,
        observed_at: now_seconds() - age,
        response_status: (state == CredentialHealthState::Degraded).then_some(503),
        detail: (state == CredentialHealthState::Degraded).then(|| "server_is_overloaded".into()),
    };
    host.services
        .store
        .record_credential_health(&input)
        .await
        .unwrap();
    let record = host
        .services
        .store
        .credential_model_health(target.credential.0, &target.upstream_model)
        .await
        .unwrap()
        .unwrap();
    if publish {
        host.services
            .control
            .observe_stored_credential_health(&record);
    }
    record
}

#[tokio::test]
async fn overloaded_model_does_not_gate_other_models_or_rotated_credentials() {
    let (fixture, target, version) = fixture().await;
    let host = &fixture.app.inner.host;
    assert!(
        begin(host, "healthy", &target, version)
            .await
            .unwrap()
            .is_none()
    );
    observe(
        host,
        &target,
        version,
        CredentialHealthState::Degraded,
        0,
        1,
        true,
    )
    .await;
    assert!(matches!(
        begin(host, "overloaded", &target, version).await,
        Err(CoreError::CredentialCoolingDown {
            retry_after_secs: 1..=30
        })
    ));
    let mut other = target.clone();
    other.upstream_model = "another-upstream-model".into();
    assert!(
        begin(host, "other-model", &other, version)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        begin(host, "rotated", &target, version + 1)
            .await
            .unwrap()
            .is_none()
    );
    observe(
        host,
        &target,
        version,
        CredentialHealthState::Dead,
        0,
        2,
        true,
    )
    .await;
    assert!(matches!(
        begin(host, "dead", &target, version).await,
        Err(CoreError::NoCredentials)
    ));
    assert!(
        begin(host, "other-still-healthy", &other, version)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn exactly_one_probe_per_model_and_no_fake_recovery_on_cancellation() {
    let (fixture, target, version) = fixture().await;
    let host = &fixture.app.inner.host;
    observe(
        host,
        &target,
        version,
        CredentialHealthState::Degraded,
        1_000,
        1,
        true,
    )
    .await;
    let mut other = target.clone();
    other.upstream_model = "another-upstream-model".into();
    observe(
        host,
        &other,
        version,
        CredentialHealthState::Degraded,
        1_000,
        2,
        true,
    )
    .await;
    let first = begin(host, "first", &target, version)
        .await
        .unwrap()
        .unwrap();
    let duplicate = first.clone();
    drop(first);
    assert!(matches!(
        begin(host, "same-model", &target, version).await,
        Err(CoreError::CredentialCoolingDown { .. })
    ));
    let second = begin(host, "other-model", &other, version)
        .await
        .unwrap()
        .unwrap();
    drop(second);
    drop(duplicate);
    fixture.app.drain_background().await;
    assert!(matches!(
        begin(host, "cancelled-model", &target, version).await,
        Err(CoreError::CredentialCoolingDown { .. })
    ));
    assert_eq!(
        host.services
            .control
            .credential_health_observation(target.credential, &target.upstream_model)
            .unwrap()
            .state,
        CredentialHealthState::Degraded
    );
    let key = lease_key(target.credential, version, &target.upstream_model);
    assert_eq!(
        host.services.cache.get(&key).await.unwrap(),
        Some(COOLDOWN_SENTINEL.to_vec())
    );
    assert_eq!(
        host.services.cache.testing.ttl(&key),
        Some(Some(Duration::from_secs(30)))
    );

    observe(
        host,
        &target,
        version,
        CredentialHealthState::Healthy,
        0,
        3,
        true,
    )
    .await;
    assert!(
        begin(host, "real-success", &target, version)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn account_scope_excludes_probes_across_models() {
    let (fixture, target, version) = fixture().await;
    let host = &fixture.app.inner.host;
    let mut global = target.clone();
    global.upstream_model = "*".into();
    observe(
        host,
        &global,
        version,
        CredentialHealthState::Degraded,
        1_000,
        1,
        true,
    )
    .await;
    let first = begin(host, "first", &target, version)
        .await
        .unwrap()
        .unwrap();
    let mut other = target.clone();
    other.upstream_model = "another-upstream-model".into();
    assert!(matches!(
        begin(host, "other-model", &other, version).await,
        Err(CoreError::CredentialCoolingDown { .. })
    ));
    let key = lease_key(target.credential, version, "*");
    assert!(host.services.cache.get(&key).await.unwrap().is_some());
    drop(first);
    fixture.app.drain_background().await;
}

#[tokio::test]
async fn fresh_persisted_failure_blocks_a_probe_selected_from_stale_memory() {
    let (fixture, target, version) = fixture().await;
    let host = &fixture.app.inner.host;
    observe(
        host,
        &target,
        version,
        CredentialHealthState::Degraded,
        1_000,
        1,
        true,
    )
    .await;
    observe(
        host,
        &target,
        version,
        CredentialHealthState::Degraded,
        0,
        2,
        false,
    )
    .await;
    assert!(matches!(
        begin(host, "stale-snapshot", &target, version).await,
        Err(CoreError::CredentialCoolingDown {
            retry_after_secs: 31..=60
        })
    ));
    let current = host
        .services
        .control
        .credential_health_observation(target.credential, &target.upstream_model)
        .unwrap();
    assert_eq!(current.consecutive_failures, 2);
    assert_eq!(current.version, 2);
    fixture.app.drain_background().await;
    let key = lease_key(target.credential, version, &target.upstream_model);
    assert_eq!(
        host.services.cache.get(&key).await.unwrap(),
        Some(COOLDOWN_SENTINEL.to_vec())
    );
}

#[tokio::test]
async fn fresh_account_failure_is_checked_after_model_lease_acquisition() {
    let (fixture, target, version) = fixture().await;
    let host = &fixture.app.inner.host;
    observe(
        host,
        &target,
        version,
        CredentialHealthState::Degraded,
        1_000,
        1,
        true,
    )
    .await;
    let mut global = target.clone();
    global.upstream_model = "*".into();
    observe(
        host,
        &global,
        version,
        CredentialHealthState::Dead,
        0,
        2,
        false,
    )
    .await;
    assert!(matches!(
        begin(host, "stale-global", &target, version).await,
        Err(CoreError::NoCredentials)
    ));
    fixture.app.drain_background().await;
}

#[tokio::test]
async fn persisted_manual_reset_clears_only_the_stale_model_observation() {
    let (fixture, target, version) = fixture().await;
    let host = &fixture.app.inner.host;
    observe(
        host,
        &target,
        version,
        CredentialHealthState::Degraded,
        1_000,
        1,
        true,
    )
    .await;
    let mut other = target.clone();
    other.upstream_model = "another-upstream-model".into();
    observe(
        host,
        &other,
        version,
        CredentialHealthState::Degraded,
        0,
        2,
        true,
    )
    .await;
    host.services
        .store
        .clear_credential_model_health(target.credential.0, &target.upstream_model)
        .await
        .unwrap();
    assert!(
        begin(host, "reset", &target, version)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        host.services
            .control
            .credential_health_observation(target.credential, &target.upstream_model)
            .is_none()
    );
    assert_eq!(
        host.services
            .control
            .credential_health_observation(target.credential, &other.upstream_model)
            .unwrap()
            .state,
        CredentialHealthState::Degraded
    );
    fixture.app.drain_background().await;
}

#[tokio::test]
async fn another_instance_owner_and_cache_errors_never_admit_a_probe() {
    let (fixture, target, version) = fixture().await;
    let host = &fixture.app.inner.host;
    observe(
        host,
        &target,
        version,
        CredentialHealthState::Degraded,
        1_000,
        1,
        true,
    )
    .await;
    let key = lease_key(target.credential, version, &target.upstream_model);
    host.services
        .cache
        .set(&key, b"another-instance".to_vec(), Some(LEASE_TTL))
        .await
        .unwrap();
    assert!(matches!(
        begin(host, "contender", &target, version).await,
        Err(CoreError::CredentialCoolingDown { .. })
    ));
    assert_eq!(
        host.services.cache.get(&key).await.unwrap(),
        Some(b"another-instance".to_vec())
    );

    let mut other = target.clone();
    other.upstream_model = "another-upstream-model".into();
    observe(
        host,
        &other,
        version,
        CredentialHealthState::Degraded,
        1_000,
        2,
        true,
    )
    .await;
    let other_key = lease_key(target.credential, version, &other.upstream_model);
    host.services
        .cache
        .testing
        .fail_once("compare_swap", &other_key, true);
    assert!(matches!(
        begin(host, "lost-cas-reply", &other, version).await,
        Err(CoreError::Store(_))
    ));
    fixture.app.drain_background().await;
    assert_eq!(
        host.services.cache.get(&other_key).await.unwrap(),
        Some(COOLDOWN_SENTINEL.to_vec())
    );
}

#[tokio::test]
async fn cancelled_acquisition_keeps_its_guard_until_a_delayed_cas_replies() {
    let (fixture, target, version) = fixture().await;
    let host = &fixture.app.inner.host;
    observe(
        host,
        &target,
        version,
        CredentialHealthState::Degraded,
        1_000,
        1,
        true,
    )
    .await;
    let key = lease_key(target.credential, version, &target.upstream_model);
    let (before, commit) = host
        .services
        .cache
        .testing
        .pause_before("compare_swap", &key);
    let acquiring = host.clone();
    let request =
        tokio::spawn(async move { begin(&acquiring, "cancelled", &target, version).await });
    tokio::time::timeout(Duration::from_secs(5), before.notified())
        .await
        .unwrap();
    request.abort();
    match request.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("caller returned before the delayed cache write"),
    }
    let (after, reply) = host
        .services
        .cache
        .testing
        .pause_after("compare_swap", &key);
    commit.notify_one();
    tokio::time::timeout(Duration::from_secs(5), after.notified())
        .await
        .unwrap();
    let owner = host.services.cache.get(&key).await.unwrap().unwrap();
    assert_ne!(owner, COOLDOWN_SENTINEL);
    let draining = fixture.app.drain_background();
    futures_util::pin_mut!(draining);
    assert!(futures_util::poll!(&mut draining).is_pending());
    reply.notify_one();
    tokio::time::timeout(Duration::from_secs(5), draining)
        .await
        .unwrap();
    assert_eq!(
        host.services.cache.get(&key).await.unwrap(),
        Some(COOLDOWN_SENTINEL.to_vec())
    );
}

#[tokio::test]
async fn a_stale_owner_cannot_release_a_replacement_lease() {
    let (fixture, target, version) = fixture().await;
    let host = &fixture.app.inner.host;
    let guard = Guard::acquire(
        host,
        "first",
        target.credential,
        version,
        &target.upstream_model,
    )
    .await
    .unwrap();
    let key = guard.key.clone();
    host.services
        .cache
        .set(&key, b"replacement-owner".to_vec(), Some(LEASE_TTL))
        .await
        .unwrap();
    drop(guard);
    fixture.app.drain_background().await;
    assert_eq!(
        host.services.cache.get(&key).await.unwrap(),
        Some(b"replacement-owner".to_vec())
    );
}

#[tokio::test]
async fn a_probe_that_lost_ownership_during_validation_is_not_admitted() {
    let (fixture, target, version) = fixture().await;
    let host = &fixture.app.inner.host;
    observe(
        host,
        &target,
        version,
        CredentialHealthState::Degraded,
        1_000,
        1,
        true,
    )
    .await;
    let key = lease_key(target.credential, version, &target.upstream_model);
    let (acquired, reply) = host
        .services
        .cache
        .testing
        .pause_after("compare_swap", &key);
    let acquiring = host.clone();
    let task = tokio::spawn(async move {
        begin(&acquiring, "expired-during-validation", &target, version).await
    });
    tokio::time::timeout(Duration::from_secs(5), acquired.notified())
        .await
        .unwrap();
    host.services
        .cache
        .set(&key, b"replacement-owner".to_vec(), Some(LEASE_TTL))
        .await
        .unwrap();
    reply.notify_one();
    assert!(matches!(
        task.await.unwrap(),
        Err(CoreError::CredentialCoolingDown { .. })
    ));
    fixture.app.drain_background().await;
    assert_eq!(
        host.services.cache.get(&key).await.unwrap(),
        Some(b"replacement-owner".to_vec())
    );
}

#[tokio::test]
async fn live_probe_renews_ownership_and_stops_renewing_before_release() {
    let (fixture, target, version) = fixture().await;
    let host = &fixture.app.inner.host;
    let key = lease_key(target.credential, version, &target.upstream_model);
    let owner = b"long-running-owner".to_vec();
    host.services
        .cache
        .set(&key, owner.clone(), Some(Duration::from_secs(1)))
        .await
        .unwrap();
    let (renewed, reply) = host
        .services
        .cache
        .testing
        .pause_after("compare_swap", &key);
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let renewing = host.clone();
    let renewing_key = key.clone();
    let renewing_owner = owner.clone();
    let task = tokio::spawn(async move {
        renew_until_released(
            &renewing,
            &renewing_key,
            &renewing_owner,
            stopped,
            Duration::from_millis(1),
        )
        .await;
    });
    tokio::time::timeout(Duration::from_secs(5), renewed.notified())
        .await
        .unwrap();
    assert_eq!(host.services.cache.get(&key).await.unwrap(), Some(owner));
    assert_eq!(host.services.cache.testing.ttl(&key), Some(Some(LEASE_TTL)));
    stop.send(()).unwrap();
    reply.notify_one();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        host.services.cache.get(&key).await.unwrap(),
        Some(COOLDOWN_SENTINEL.to_vec())
    );
    assert_eq!(
        host.services.cache.testing.ttl(&key),
        Some(Some(Duration::from_secs(30)))
    );
}

#[test]
fn repeated_failures_back_off_without_overflow() {
    let mut observation = CredentialHealthRecord {
        credential_id: 1,
        model: "model".into(),
        credential_version: 1,
        version: 1,
        state: CredentialHealthState::Degraded,
        consecutive_failures: 1,
        observed_at: 1_000,
        response_status: Some(503),
        detail: None,
    };
    for (failures, delay) in [
        (1, 30),
        (2, 60),
        (3, 120),
        (4, 240),
        (5, 300),
        (u32::MAX, 300),
    ] {
        observation.consecutive_failures = failures;
        assert_eq!(cooldown_remaining(&observation, 1_000), delay);
        assert_eq!(
            cooldown_remaining(&observation, 1_000 + i64::from(delay)),
            0
        );
    }
    observation.observed_at = i64::MAX;
    assert_eq!(cooldown_remaining(&observation, 0), u32::MAX);
}
