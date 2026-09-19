use gproxy_core::{CredentialHealth, CredentialId, CredentialStore, Host};
use gproxy_store::records::{CredentialHealthInput, CredentialHealthRecord, CredentialHealthState};
use serde_json::json;

use super::AppHost;
use crate::tests::setup;

async fn fixture() -> (setup::Fixture, CredentialId, u64) {
    let fixture = setup::fixture().await;
    fixture.app.shutdown();
    fixture.app.drain_background().await;
    let credential = CredentialId(fixture.credential);
    let version = fixture
        .app
        .inner
        .host
        .load(credential)
        .await
        .unwrap()
        .version;
    (fixture, credential, version)
}

async fn read(host: &AppHost, credential: CredentialId, model: &str) -> CredentialHealthRecord {
    let record = host
        .services
        .store
        .credential_model_health(credential.0, model)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        host.services
            .control
            .credential_health_observation(credential, model),
        Some(record.clone()),
        "routing must publish the row accepted by storage"
    );
    record
}

async fn record(
    fixture: &setup::Fixture,
    credential: CredentialId,
    model: &str,
    version: u64,
    health: CredentialHealth,
) -> CredentialHealthRecord {
    let (status, detail) = match health {
        CredentialHealth::Healthy => (http::StatusCode::OK, "upstream response completed"),
        CredentialHealth::Degraded => (
            http::StatusCode::SERVICE_UNAVAILABLE,
            "server_is_overloaded",
        ),
        CredentialHealth::Dead => (http::StatusCode::UNAUTHORIZED, "invalid credential"),
    };
    fixture
        .app
        .inner
        .host
        .record_credential_health(credential, model, version, health, Some(status), detail)
        .await;
    fixture.app.drain_background().await;
    read(&fixture.app.inner.host, credential, model).await
}

#[tokio::test]
async fn model_health_persists_each_failure_and_recovers_only_the_successful_model() {
    let (fixture, credential, version) = fixture().await;
    let crate::MutationResult::Id(secondary) = fixture
        .app
        .mutate(crate::ControlMutation::Credential {
            provider_id: fixture.provider,
            label: Some("second account".into()),
            secret: json!({"api_key": "second-test-credential"}),
            enabled: true,
        })
        .await
        .unwrap()
    else {
        panic!("credential mutation returns an id");
    };
    let secondary = CredentialId(secondary);
    let host = &fixture.app.inner.host;
    let secondary_version = host.load(secondary).await.unwrap().version;
    let other_model = record(
        &fixture,
        credential,
        "other-model",
        version,
        CredentialHealth::Degraded,
    )
    .await;
    let other_account = record(
        &fixture,
        secondary,
        "upstream-model",
        secondary_version,
        CredentialHealth::Healthy,
    )
    .await;
    let mut previous = None;
    for failures in 1..=3 {
        let row = record(
            &fixture,
            credential,
            "upstream-model",
            version,
            CredentialHealth::Degraded,
        )
        .await;
        assert_eq!(row.credential_version, version);
        assert_eq!(row.state, CredentialHealthState::Degraded);
        assert_eq!(row.consecutive_failures, failures);
        assert_eq!(row.response_status, Some(503));
        assert_eq!(row.detail.as_deref(), Some("server_is_overloaded"));
        if let Some(previous) = previous {
            assert!(row.version > previous);
        }
        previous = Some(row.version);
    }
    let recovered = record(
        &fixture,
        credential,
        "upstream-model",
        version,
        CredentialHealth::Healthy,
    )
    .await;
    assert_eq!(recovered.state, CredentialHealthState::Healthy);
    assert_eq!(recovered.consecutive_failures, 0);
    assert_eq!(recovered.response_status, Some(200));
    assert!(recovered.version > previous.unwrap());
    assert_eq!(read(host, credential, "other-model").await, other_model);
    assert_eq!(read(host, secondary, "upstream-model").await, other_account);
    assert!(
        host.services
            .store
            .credential_model_health(credential.0, "*")
            .await
            .unwrap()
            .is_none(),
        "model failures must not create an account-wide record"
    );
}

#[tokio::test]
async fn health_changes_invalidate_other_workers_but_healthy_refreshes_do_not() {
    let (fixture, credential, version) = fixture().await;
    let cache = &fixture.app.inner.host.services.cache;
    let initial = crate::invalidation::current(cache).await.unwrap();
    record(
        &fixture,
        credential,
        "upstream-model",
        version,
        CredentialHealth::Degraded,
    )
    .await;
    let degraded = crate::invalidation::current(cache).await.unwrap();
    assert!(degraded > initial);
    record(
        &fixture,
        credential,
        "upstream-model",
        version,
        CredentialHealth::Degraded,
    )
    .await;
    let failed_again = crate::invalidation::current(cache).await.unwrap();
    assert!(
        failed_again > degraded,
        "new backoff must reach other workers"
    );
    record(
        &fixture,
        credential,
        "upstream-model",
        version,
        CredentialHealth::Healthy,
    )
    .await;
    let recovered = crate::invalidation::current(cache).await.unwrap();
    assert!(recovered > failed_again);
    for _ in 0..3 {
        let row = record(
            &fixture,
            credential,
            "upstream-model",
            version,
            CredentialHealth::Healthy,
        )
        .await;
        assert_eq!(row.state, CredentialHealthState::Healthy);
        assert_eq!(row.consecutive_failures, 0);
        assert_eq!(
            crate::invalidation::current(cache).await.unwrap(),
            recovered
        );
    }
}

#[tokio::test]
async fn successful_model_recovers_account_degradation_even_when_model_was_already_healthy() {
    let (fixture, credential, version) = fixture().await;
    for _ in 0..2 {
        record(
            &fixture,
            credential,
            "upstream-model",
            version,
            CredentialHealth::Healthy,
        )
        .await;
    }
    let other_model = record(
        &fixture,
        credential,
        "other-model",
        version,
        CredentialHealth::Degraded,
    )
    .await;
    record(
        &fixture,
        credential,
        "*",
        version,
        CredentialHealth::Degraded,
    )
    .await;
    let account = record(
        &fixture,
        credential,
        "*",
        version,
        CredentialHealth::Degraded,
    )
    .await;
    assert_eq!(account.consecutive_failures, 2);
    record(
        &fixture,
        credential,
        "upstream-model",
        version,
        CredentialHealth::Healthy,
    )
    .await;
    let host = &fixture.app.inner.host;
    let account = read(host, credential, "*").await;
    assert_eq!(account.state, CredentialHealthState::Healthy);
    assert_eq!(account.consecutive_failures, 0);
    assert_eq!(read(host, credential, "other-model").await, other_model);
}

#[tokio::test]
async fn successful_model_does_not_clear_account_dead_state() {
    let (fixture, credential, version) = fixture().await;
    let account = record(&fixture, credential, "*", version, CredentialHealth::Dead).await;
    let model = record(
        &fixture,
        credential,
        "upstream-model",
        version,
        CredentialHealth::Healthy,
    )
    .await;
    assert_eq!(model.state, CredentialHealthState::Healthy);
    assert_eq!(model.consecutive_failures, 0);
    assert_eq!(
        read(&fixture.app.inner.host, credential, "*").await,
        account
    );
}

#[tokio::test]
async fn successful_model_cannot_recover_dead_account_from_a_stale_local_snapshot() {
    let (fixture, credential, version) = fixture().await;
    let degraded = record(
        &fixture,
        credential,
        "*",
        version,
        CredentialHealth::Degraded,
    )
    .await;
    let host = &fixture.app.inner.host;
    host.services
        .store
        .record_credential_health(&CredentialHealthInput {
            credential_id: credential.0,
            model: "*".into(),
            credential_version: version,
            version: super::health_version(&host.services.health_sequence).unwrap(),
            state: CredentialHealthState::Dead,
            observed_at: degraded.observed_at,
            response_status: Some(401),
            detail: Some("credential rejected on another worker".into()),
        })
        .await
        .unwrap();
    let dead = host
        .services
        .store
        .credential_model_health(credential.0, "*")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dead.state, CredentialHealthState::Dead);
    assert_eq!(
        host.services
            .control
            .credential_health_observation(credential, "*"),
        Some(degraded),
        "the success handler starts with a stale local degraded record"
    );
    let success = record(
        &fixture,
        credential,
        "upstream-model",
        version,
        CredentialHealth::Healthy,
    )
    .await;
    assert!(success.version > dead.version);
    assert_eq!(read(host, credential, "*").await, dead);
}
