use gproxy_core::RoutingMode;
use gproxy_store::records::{
    CredentialHealthInput, CredentialQuotaObservation, ProviderInput, QuotaBoundaryConfidence,
    QuotaBoundarySource, RouteMemberInput,
};
use rust_decimal::Decimal;
use serde_json::json;

use super::*;
use crate::tests::setup;

async fn fixture() -> (setup::Fixture, CredentialId) {
    let fixture = setup::fixture().await;
    fixture.app.shutdown();
    fixture.app.drain_background().await;
    fixture
        .app
        .inner
        .host
        .services
        .store
        .update_provider(
            fixture.provider,
            &ProviderInput {
                name: "provider".into(),
                label: None,
                channel: "openai".into(),
                settings: json!({}),
                credential_strategy: "sticky".into(),
                proxy_url: None,
                tls_fingerprint: None,
                enabled: true,
            },
        )
        .await
        .unwrap();
    let crate::MutationResult::Id(secondary) = fixture
        .app
        .mutate(crate::ControlMutation::Credential {
            provider_id: fixture.provider,
            label: None,
            secret: json!({"api_key": "second-test-credential"}),
            enabled: true,
        })
        .await
        .unwrap()
    else {
        panic!("credential mutation returns an id");
    };
    (fixture, CredentialId(secondary))
}

fn degraded(
    control: &SnapshotControl,
    credential: CredentialId,
    model: &str,
    observed_at: i64,
) -> CredentialHealthRecord {
    CredentialHealthRecord {
        credential_id: credential.0,
        model: model.into(),
        credential_version: control.known_credential_version(credential.0).unwrap(),
        version: 1,
        state: CredentialHealthState::Degraded,
        consecutive_failures: 1,
        observed_at,
        response_status: Some(503),
        detail: Some("server_is_overloaded".into()),
    }
}

fn plan(control: &SnapshotControl, model: &str, now: i64) -> Plan {
    let mut plan = control
        .snapshot
        .load()
        .resolve_preprocessed(
            Some(model),
            &RoutingMode::Aggregated,
            Some(42),
            &control.credential_health.load(),
            &control.rotation,
        )
        .unwrap();
    super::super::pressure::apply(&mut plan, &control.credential_pressure.load(), now);
    plan
}

#[tokio::test]
async fn model_overload_leaves_other_models_and_other_accounts_healthy() {
    let (fixture, secondary) = fixture().await;
    let control = &fixture.app.inner.host.services.control;
    let credential = CredentialId(fixture.credential);
    let now = unix_now();
    let observation = degraded(control, credential, "upstream-model", now);
    control.observe_stored_credential_health(&observation);

    let mut affected = plan(control, "public-model", now);
    control.prioritize_health_probe(&mut affected, now);
    assert_eq!(affected.targets[0].credential, secondary);
    assert!(affected.targets[0].rules.session_affinity);
    assert_eq!(affected.targets[1].credential, credential);
    assert!(!affected.targets[1].rules.session_affinity);

    let mut unaffected = plan(control, "provider/other-model", now + 30);
    control.prioritize_health_probe(&mut unaffected, now + 30);
    assert_eq!(unaffected.targets.len(), 2);
    assert!(
        unaffected
            .targets
            .iter()
            .all(|target| target.rules.session_affinity)
    );
    assert!(
        unaffected
            .targets
            .iter()
            .any(|target| target.credential == credential)
    );
    assert!(
        control
            .credential_health_observation(credential, "*")
            .is_none()
    );
    assert!(
        control
            .credential_health_observation(credential, "other-model")
            .is_none()
    );
    assert!(
        control
            .credential_health_observation(secondary, "upstream-model")
            .is_none()
    );
}

#[tokio::test]
async fn expired_model_gets_one_probe_ahead_of_a_healthy_account() {
    let (fixture, secondary) = fixture().await;
    let control = &fixture.app.inner.host.services.control;
    let credential = CredentialId(fixture.credential);
    let now = unix_now() - 30;
    let observation = degraded(control, credential, "upstream-model", now);
    let version = observation.credential_version;
    control.observe_stored_credential_health(&observation);

    let mut cooling = plan(control, "public-model", now + 29);
    control.prioritize_health_probe(&mut cooling, now + 29);
    assert_eq!(cooling.targets[0].credential, secondary);
    for _ in 0..2 {
        let mut due = plan(control, "public-model", now + 30);
        control.prioritize_health_probe(&mut due, now + 30);
        assert_eq!(due.targets[0].credential, credential);
        assert!(!due.targets[0].rules.session_affinity);
        assert!(control.health_probe_ready(credential, "upstream-model", version, now + 30));
    }

    assert!(control.try_begin_health_probe(credential, "upstream-model", version));
    assert!(!control.try_begin_health_probe(credential, "upstream-model", version));
    let mut active = plan(control, "public-model", now + 30);
    control.prioritize_health_probe(&mut active, now + 30);
    assert_eq!(active.targets[0].credential, secondary);
    assert!(control.try_begin_health_probe(credential, "other-model", version));
}

#[tokio::test]
async fn cancelled_probe_keeps_degradation_and_delays_the_next_attempt() {
    let (fixture, secondary) = fixture().await;
    let control = &fixture.app.inner.host.services.control;
    let credential = CredentialId(fixture.credential);
    let observation = degraded(control, credential, "upstream-model", unix_now() - 60);
    let version = observation.credential_version;
    control.observe_stored_credential_health(&observation);
    assert!(control.try_begin_health_probe(credential, "upstream-model", version));
    let before = unix_now();
    control.finish_health_probe(credential, "upstream-model", version);
    let after = unix_now();

    assert_eq!(
        control.credential_health_observation(credential, "upstream-model"),
        Some(observation)
    );
    assert!(!control.try_begin_health_probe(credential, "upstream-model", version));
    let mut cooling = plan(control, "public-model", before + 29);
    control.prioritize_health_probe(&mut cooling, before + 29);
    assert_eq!(cooling.targets[0].credential, secondary);
    let mut due = plan(control, "public-model", after + 30);
    control.prioritize_health_probe(&mut due, after + 30);
    assert_eq!(due.targets[0].credential, credential);
}

#[tokio::test]
async fn credential_rotation_isolates_old_health_and_active_probes() {
    let (fixture, _) = fixture().await;
    let host = &fixture.app.inner.host;
    let control = &host.services.control;
    let credential = CredentialId(fixture.credential);
    let observation = degraded(control, credential, "upstream-model", unix_now() - 60);
    let version = observation.credential_version;
    host.services
        .store
        .record_credential_health(&CredentialHealthInput {
            credential_id: credential.0,
            model: observation.model.clone(),
            credential_version: version,
            version: observation.version,
            state: observation.state,
            observed_at: observation.observed_at,
            response_status: observation.response_status,
            detail: observation.detail.clone(),
        })
        .await
        .unwrap();
    control.observe_stored_credential_health(&observation);
    assert!(control.try_begin_health_probe(credential, "upstream-model", version));
    let envelope = host
        .services
        .cipher
        .seal(&json!({"api_key":"rotated-test-credential"}))
        .unwrap();
    host.services
        .store
        .persist_credential_rotation(credential.0, &envelope, version)
        .await
        .unwrap();
    control.reload().await.unwrap();

    assert_eq!(
        control.known_credential_version(credential.0),
        Some(version + 1)
    );
    let mut resolved = plan(control, "public-model", unix_now());
    control.prioritize_health_probe(&mut resolved, unix_now());
    assert!(
        resolved
            .targets
            .iter()
            .all(|target| target.rules.session_affinity)
    );
    assert!(control.try_begin_health_probe(credential, "upstream-model", version + 1));
    control.finish_health_probe(credential, "upstream-model", version);
    assert!(!control.health_probe_ready(
        credential,
        "upstream-model",
        version + 1,
        unix_now() + 30
    ));
}

#[test]
fn consecutive_failures_back_off_to_a_five_minute_limit() {
    let mut record = CredentialHealthRecord {
        credential_id: 1,
        model: "upstream-model".into(),
        credential_version: 0,
        version: 1,
        state: CredentialHealthState::Degraded,
        consecutive_failures: 1,
        observed_at: 1000,
        response_status: Some(503),
        detail: None,
    };
    for (failures, delay) in [
        (0, 30),
        (1, 30),
        (2, 60),
        (3, 120),
        (4, 240),
        (5, 300),
        (u32::MAX, 300),
    ] {
        record.consecutive_failures = failures;
        assert_eq!(SnapshotControl::health_retry_after(&record, 1000), delay);
        assert_eq!(
            SnapshotControl::health_retry_after(&record, 1000 + i64::from(delay) - 1),
            1
        );
        assert_eq!(
            SnapshotControl::health_retry_after(&record, 1000 + i64::from(delay)),
            0
        );
    }
}

#[tokio::test]
async fn recovery_probes_respect_route_tier_and_quota_pressure() {
    let (fixture, secondary) = fixture().await;
    let control = &fixture.app.inner.host.services.control;
    let credential = CredentialId(fixture.credential);
    fixture
        .app
        .mutate(crate::ControlMutation::RouteMember(RouteMemberInput {
            route_id: fixture.route,
            provider_id: fixture.provider,
            upstream_model: "lower-tier-model".into(),
            tier: 1,
            weight: 100,
            enabled: true,
        }))
        .await
        .unwrap();
    let now = unix_now();
    control.observe_stored_credential_health(&degraded(
        control,
        credential,
        "lower-tier-model",
        now - 60,
    ));
    let mut lower_tier = plan(control, "public-model", now);
    control.prioritize_health_probe(&mut lower_tier, now);
    assert_eq!(lower_tier.targets[0].tier, 0);
    assert_eq!(lower_tier.targets[0].upstream_model, "upstream-model");
    assert!(
        lower_tier
            .targets
            .iter()
            .any(|target| target.credential == credential
                && target.upstream_model == "lower-tier-model"
                && target.tier == 1)
    );

    control.observe_stored_credential_health(&degraded(
        control,
        credential,
        "upstream-model",
        now - 60,
    ));

    control.apply_live_pressure(&CredentialQuotaObservation {
        unit: None,
        reset_behavior: gproxy_core::QuotaResetBehavior::Periodic,
        scope: gproxy_core::QuotaScope::All,
        sample: gproxy_core::QuotaSample {
            source: gproxy_core::QuotaSampleSource::Response,
            started_at_ms: now * 1000,
            received_at_ms: now * 1000,
        },
        credential_id: credential.0,
        window_key: "five-hour".into(),
        label: None,
        period_start: Some(now - 60),
        period_end: Some(now + 300),
        boundary_source: QuotaBoundarySource::Upstream,
        boundary_confidence: QuotaBoundaryConfidence::Exact,
        observed_at: now,
        upstream_used: Some(Decimal::from(95)),
        upstream_limit: Some(Decimal::from(100)),
        used_percent: Some(Decimal::from(95)),
    });
    let mut pressured = plan(control, "public-model", now);
    control.prioritize_health_probe(&mut pressured, now);
    assert_eq!(pressured.targets[0].credential, secondary);
    let mut reset_window = plan(control, "public-model", now + 300);
    control.prioritize_health_probe(&mut reset_window, now + 300);
    assert_eq!(reset_window.targets[0].credential, credential);
}

#[tokio::test]
async fn older_observations_cannot_overwrite_newer_health_or_credential_versions() {
    let (fixture, _) = fixture().await;
    let control = &fixture.app.inner.host.services.control;
    let credential = CredentialId(fixture.credential);
    let old = degraded(control, credential, "upstream-model", unix_now());
    let mut recovered = old.clone();
    recovered.version += 1;
    recovered.state = CredentialHealthState::Healthy;
    recovered.consecutive_failures = 0;
    control.observe_stored_credential_health(&recovered);
    control.observe_stored_credential_health(&old);
    assert_eq!(
        control.credential_health_observation(credential, "upstream-model"),
        Some(recovered.clone())
    );
    let mut replay = old.clone();
    replay.version = recovered.version;
    control.observe_stored_credential_health(&replay);
    assert_eq!(
        control.credential_health_observation(credential, "upstream-model"),
        Some(recovered)
    );

    let mut rotated = old.clone();
    rotated.credential_version += 1;
    control.observe_stored_credential_health(&rotated);
    replay.version = i64::MAX;
    control.observe_stored_credential_health(&replay);
    assert_eq!(
        control.credential_health_observation(credential, "upstream-model"),
        Some(rotated)
    );
}

#[tokio::test]
async fn exact_cached_reset_preserves_new_observations_and_other_scopes() {
    let (fixture, secondary) = fixture().await;
    let control = &fixture.app.inner.host.services.control;
    let credential = CredentialId(fixture.credential);
    let now = unix_now();
    let original = degraded(control, credential, "upstream-model", now);
    let other_model = degraded(control, credential, "other-model", now);
    let account = degraded(control, credential, "*", now);
    let other_account = degraded(control, secondary, "upstream-model", now);
    for observation in [&original, &other_model, &account, &other_account] {
        control.observe_stored_credential_health(observation);
    }
    let mut newer = original.clone();
    newer.version += 1;
    control.observe_stored_credential_health(&newer);
    control.clear_cached_credential_health(
        credential,
        "upstream-model",
        original.credential_version,
        original.version,
    );
    assert_eq!(
        control.credential_health_observation(credential, "upstream-model"),
        Some(newer.clone())
    );
    control.clear_cached_credential_health(
        credential,
        "upstream-model",
        newer.credential_version,
        newer.version,
    );
    assert!(
        control
            .credential_health_observation(credential, "upstream-model")
            .is_none()
    );
    assert_eq!(
        control.credential_health_observation(credential, "other-model"),
        Some(other_model)
    );
    assert_eq!(
        control.credential_health_observation(credential, "*"),
        Some(account)
    );
    assert_eq!(
        control.credential_health_observation(secondary, "upstream-model"),
        Some(other_account)
    );

    let mut rotated = original.clone();
    rotated.credential_version += 1;
    control.observe_stored_credential_health(&rotated);
    control.clear_cached_credential_health(
        credential,
        "upstream-model",
        original.credential_version,
        original.version,
    );
    assert_eq!(
        control.credential_health_observation(credential, "upstream-model"),
        Some(rotated)
    );
}

#[tokio::test]
async fn both_account_and_model_cooldowns_must_expire_before_a_probe() {
    let (fixture, secondary) = fixture().await;
    let control = &fixture.app.inner.host.services.control;
    let credential = CredentialId(fixture.credential);
    let now = unix_now() - 30;
    let account = degraded(control, credential, "*", now - 30);
    let model = degraded(control, credential, "upstream-model", now);
    control.observe_stored_credential_health(&account);
    control.observe_stored_credential_health(&model);
    let mut still_cooling = plan(control, "public-model", now);
    control.prioritize_health_probe(&mut still_cooling, now);
    assert_eq!(still_cooling.targets[0].credential, secondary);
    let mut due = plan(control, "public-model", now + 30);
    control.prioritize_health_probe(&mut due, now + 30);
    assert_eq!(due.targets[0].credential, credential);
    assert!(control.try_begin_health_probe(credential, "*", account.credential_version));
    let mut active = plan(control, "public-model", now + 30);
    control.prioritize_health_probe(&mut active, now + 30);
    assert_eq!(active.targets[0].credential, secondary);
}
