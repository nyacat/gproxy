use gproxy_core::{CacheBackend, ControlPlane, Host, UsageSink};
use rust_decimal::Decimal;

use super::setup;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an empty PostgreSQL database and dedicated Redis database via GPROXY_TEST_POSTGRES_DSN and GPROXY_TEST_REDIS_URL"]
async fn postgres_redis_credential_health_rejects_success_started_before_newer_failures() {
    use gproxy_core::{CredentialId, CredentialStore};
    use gproxy_store::records::{CredentialHealthInput, CredentialHealthState};

    let fixture = setup::fixture_with_backends(
        Some(std::env::var("GPROXY_TEST_POSTGRES_DSN").expect("test PostgreSQL database")),
        Some(std::env::var("GPROXY_TEST_REDIS_URL").expect("test Redis database")),
    )
    .await;
    fixture.app.shutdown();
    fixture.app.drain_background().await;
    let host = &fixture.app.inner.host;
    let credential = CredentialId(fixture.credential);
    let version = host.load(credential).await.unwrap().version;
    let started_at_ms = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 60)
        * 1_000;
    let mut failures = Vec::new();
    for scope in ["upstream-model", "*"] {
        host.services
            .store
            .record_credential_health(&CredentialHealthInput {
                credential_id: credential.0,
                model: scope.into(),
                credential_version: version,
                version: (started_at_ms - 1_000) * 1_000_000,
                state: CredentialHealthState::Degraded,
                observed_at: started_at_ms / 1_000 - 1,
                response_status: Some(503),
                detail: Some("earlier failure".into()),
            })
            .await
            .unwrap();
        host.services
            .control
            .refresh_credential_health(credential, scope)
            .await
            .unwrap();
        failures.push(
            host.services
                .store
                .credential_model_health(credential.0, scope)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    // Simulate another worker recording failures after this attempt starts,
    // while this worker still holds the earlier routing snapshot.
    for row in &mut failures {
        host.services
            .store
            .record_credential_health(&CredentialHealthInput {
                credential_id: credential.0,
                model: row.model.clone(),
                credential_version: version,
                version: (started_at_ms + 1) * 1_000_000,
                state: CredentialHealthState::Degraded,
                observed_at: row.observed_at + 1,
                response_status: Some(503),
                detail: Some("newer failure from another worker".into()),
            })
            .await
            .unwrap();
        *row = host
            .services
            .store
            .credential_model_health(credential.0, &row.model)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.consecutive_failures, 2);
    }
    for attempt_start in [started_at_ms, started_at_ms + 1] {
        host.record_credential_health_success(
            credential,
            "upstream-model",
            version,
            attempt_start,
            Some(http::StatusCode::OK),
            "old request completed successfully",
        )
        .await;
        fixture.app.drain_background().await;
        for failure in &failures {
            assert_eq!(
                host.services
                    .store
                    .credential_model_health(credential.0, &failure.model)
                    .await
                    .unwrap(),
                Some(failure.clone())
            );
            assert_eq!(
                host.services
                    .control
                    .credential_health_observation(credential, &failure.model),
                Some(failure.clone())
            );
        }
    }
    host.record_credential_health_success(
        credential,
        "upstream-model",
        version,
        started_at_ms + 60_001,
        Some(http::StatusCode::OK),
        "new recovery probe completed successfully",
    )
    .await;
    fixture.app.drain_background().await;
    for scope in ["upstream-model", "*"] {
        let recovered = host
            .services
            .store
            .credential_model_health(credential.0, scope)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.state, CredentialHealthState::Healthy);
        assert_eq!(recovered.consecutive_failures, 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires an empty PostgreSQL database and dedicated Redis database via GPROXY_TEST_POSTGRES_DSN and GPROXY_TEST_REDIS_URL"]
async fn postgres_redis_preserve_usage_identity_and_settle_concurrent_requests_once() {
    let fixture = setup::fixture_with_backends(
        Some(std::env::var("GPROXY_TEST_POSTGRES_DSN").expect("test PostgreSQL database")),
        Some(std::env::var("GPROXY_TEST_REDIS_URL").expect("test Redis database")),
    )
    .await;
    super::setting(&fixture.app, "enable_usage", serde_json::json!(true)).await;
    let quota = fixture
        .app
        .inner
        .host
        .services
        .control
        .current()
        .quotas
        .iter()
        .find(|row| row.id == fixture.quota)
        .unwrap()
        .clone();
    fixture
        .app
        .inner
        .host
        .services
        .store
        .update_quota(
            fixture.quota,
            &gproxy_store::records::QuotaInput {
                subject_kind: quota.subject_kind,
                subject_id: quota.subject_id,
                quota_total: Some(Decimal::from(1_000)),
                quota_5h: Some(Decimal::from(1_000)),
                quota_daily: None,
                quota_weekly: None,
                quota_monthly: None,
                quota_7d: None,
                enabled: true,
            },
        )
        .await
        .unwrap();
    fixture.app.reload().await.unwrap();
    let host = &fixture.app.inner.host;
    let plan = host
        .services
        .control
        .resolve(
            Some("public-model"),
            &gproxy_core::RoutingMode::Aggregated,
            None,
        )
        .unwrap();
    let identity = host
        .authenticate(&setup::request("identity", "x", &fixture.client_key))
        .await
        .unwrap();
    let calls = (0..8).map(|index| {
        let host = host.clone();
        let plan = plan.clone();
        let identity = identity.clone();
        let request = setup::request(&format!("pg-redis-{index}"), "x", &fixture.client_key);
        async move {
            let plan = host
                .admit(
                    &identity,
                    &request,
                    Some(super::generation_operation()),
                    Some("public-model"),
                    &plan,
                )
                .await
                .unwrap();
            let target = &plan.targets[0];
            host.admit_credential(
                &request.request_id,
                target,
                &request.body,
                gproxy_protocol::SettleMode::OnResponse,
            )
            .await
            .unwrap();
            let started = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            host.begin_credential_usage(&request.request_id, target, started)
                .await
                .unwrap();
            let settlement = gproxy_core::Settlement {
                request_id: request.request_id.clone(),
                provider_id: target.provider.id,
                credential_id: target.credential,
                upstream_model: target.upstream_model.clone(),
                upstream_started_at_ms: Some(started),
                usage: gproxy_core::NormalizedUsage {
                    input_tokens: 1,
                    output_tokens: 2,
                    ..Default::default()
                },
                cost: Decimal::new(1, 2),
                source: gproxy_core::UsageSource::Upstream,
                ended: gproxy_core::Ended::Complete,
                latency_ms: 1,
                attempts: Vec::new(),
            };
            // A repeated durable write must not increment usage rollups twice.
            host.record_checked(&settlement).await.unwrap();
            host.record_checked(&settlement).await.unwrap();
            host.finish_admission(&request.request_id, Some(&settlement))
                .await;
            host.finish_admission(&request.request_id, Some(&settlement))
                .await;
            let row = host
                .services
                .store
                .usage_by_request(&request.request_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.usage.user_id, Some(identity.user_id));
            assert_eq!(row.usage.user_key_id, Some(identity.user_key_id));
            assert_eq!(row.usage.cost, settlement.cost);
            assert!(
                host.services
                    .cache
                    .get(&format!("gproxy:admission:{}", request.request_id))
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    });
    futures_util::future::join_all(calls).await;
    assert_eq!(host.services.store.usage_count().await.unwrap(), 8);
    for window in fixture
        .app
        .quota_windows()
        .await
        .unwrap()
        .into_iter()
        .filter(|window| window.quota_id == fixture.quota)
    {
        assert_eq!(window.cost_used, Decimal::new(8, 2));
        assert_eq!(
            host.services
                .cache
                .incr(&format!("gproxy:quota-pending:{}", window.id), 0, None)
                .await
                .unwrap(),
            0
        );
    }
    fixture.app.shutdown();
    fixture.app.drain_background().await;
}
