use bytes::Bytes;
use gproxy_core::{CacheBackend as _, ControlPlane, CoreError, Host, UsageSink};
use gproxy_protocol::SettleMode;
use gproxy_store::records::{QuotaInput, QuotaWindowKind};
use rust_decimal::Decimal;

use super::setup;
use crate::ControlMutation;

fn budget(credential: i64) -> QuotaInput {
    QuotaInput {
        subject_kind: "credential".into(),
        subject_id: credential,
        quota_total: None,
        quota_monthly: None,
        quota_weekly: None,
        quota_daily: None,
        quota_5h: None,
        quota_7d: None,
        enabled: true,
    }
}

#[tokio::test]
async fn deleting_a_credential_budget_releases_it_and_still_settles_user_quotas() {
    let fixture = setup::fixture().await;
    let mut input = budget(fixture.credential);
    input.quota_total = Some(Decimal::from(1_000));
    let quota_id = setup::id(
        fixture
            .app
            .mutate(ControlMutation::Quota(input))
            .await
            .unwrap(),
    );
    let host = &fixture.app.inner.host;
    let request = setup::request("deleted-credential-budget", "hello", &fixture.client_key);
    let identity = host.authenticate(&request).await.unwrap();
    let plan = host
        .services
        .control
        .resolve(
            Some("public-model"),
            &gproxy_core::RoutingMode::Aggregated,
            None,
        )
        .unwrap();
    host.admit(
        &identity,
        &request,
        Some(super::generation_operation()),
        Some("public-model"),
        &plan,
    )
    .await
    .unwrap();
    host.admit_credential(
        &request.request_id,
        &plan.targets[0],
        &request.body,
        SettleMode::OnResponse,
    )
    .await
    .unwrap();
    let windows = host.services.store.quota_windows().await.unwrap();
    host.services.store.delete_quota(quota_id).await.unwrap();
    let settlement = gproxy_core::Settlement {
        attempts: Vec::new(),
        upstream_started_at_ms: Some(crate::quota_refresh::now() * 1000),
        request_id: request.request_id.clone(),
        provider_id: fixture.provider,
        credential_id: gproxy_core::CredentialId(fixture.credential),
        upstream_model: "upstream-model".into(),
        usage: Default::default(),
        cost: Decimal::new(2, 1),
        source: gproxy_core::UsageSource::Upstream,
        ended: gproxy_core::Ended::Complete,
        latency_ms: 1,
    };
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        host.record(&settlement).await;
        host.finish_admission(&request.request_id, Some(&settlement))
            .await;
        // The stale control snapshot must not recreate a deleted budget on replay.
        host.record(&settlement).await;
    })
    .await
    .unwrap();
    assert!(
        !fixture
            .app
            .admission_pending(&request.request_id)
            .await
            .unwrap()
    );
    for window in windows {
        assert_eq!(setup::counter(host, window.id).await, 0);
    }
    for window in host.services.store.quota_windows().await.unwrap() {
        assert_ne!(window.quota_id, quota_id);
        assert_eq!(window.cost_used, settlement.cost);
    }
    assert!(
        host.services
            .cache
            .get(&format!("gproxy:quota-failed:{quota_id}"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        host.services
            .store
            .usage_by_request(&request.request_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn same_credential_retry_releases_every_reservation() {
    let fixture = setup::fixture().await;
    let mut input = budget(fixture.credential);
    input.quota_total = Some(Decimal::from(1_000));
    let id = setup::id(
        fixture
            .app
            .mutate(ControlMutation::Quota(input))
            .await
            .unwrap(),
    );
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
    let target = &plan.targets[0];
    let body = Bytes::from_static(
        br#"{"model":"public-model","messages":[{"role":"user","content":"Hello"}]}"#,
    );
    host.admit_credential("review-repeat", target, &body, SettleMode::OnResponse)
        .await
        .unwrap();
    host.admit_credential("review-repeat", target, &body, SettleMode::OnResponse)
        .await
        .unwrap();
    let window = host
        .services
        .store
        .quota_windows()
        .await
        .unwrap()
        .into_iter()
        .find(|w| w.quota_id == id)
        .unwrap();
    host.finish_admission("review-repeat", None).await;
    assert_eq!(
        setup::counter(host, window.id).await,
        0,
        "all retry reservations must be released"
    );
}

#[tokio::test]
async fn failover_charges_only_the_settled_credential() {
    let fixture = setup::fixture().await;
    super::setting(&fixture.app, "enable_usage", serde_json::json!(false)).await;
    let second = setup::id(
        fixture
            .app
            .mutate(ControlMutation::Credential {
                provider_id: fixture.provider,
                label: None,
                secret: serde_json::json!({"api_key": "synthetic-review-key"}),
                enabled: true,
            })
            .await
            .unwrap(),
    );
    let mut quota_ids = Vec::new();
    for credential in [fixture.credential, second] {
        let mut input = budget(credential);
        input.quota_total = Some(Decimal::from(1_000));
        quota_ids.push(setup::id(
            fixture
                .app
                .mutate(ControlMutation::Quota(input))
                .await
                .unwrap(),
        ));
    }
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
    let mut first_target = plan.targets[0].clone();
    first_target.credential = gproxy_core::CredentialId(fixture.credential);
    let mut second_target = first_target.clone();
    second_target.credential = gproxy_core::CredentialId(second);
    host.admit_credential(
        "review-failover",
        &first_target,
        &Bytes::new(),
        SettleMode::OnResponse,
    )
    .await
    .unwrap();
    host.admit_credential(
        "review-failover",
        &second_target,
        &Bytes::new(),
        SettleMode::OnResponse,
    )
    .await
    .unwrap();
    let settlement = gproxy_core::Settlement {
        attempts: Vec::new(),
        upstream_started_at_ms: None,
        request_id: "review-failover".into(),
        provider_id: fixture.provider,
        credential_id: second_target.credential,
        upstream_model: second_target.upstream_model.clone(),
        usage: Default::default(),
        cost: Decimal::ONE,
        source: gproxy_core::UsageSource::Upstream,
        ended: gproxy_core::Ended::Complete,
        latency_ms: 1,
    };
    host.record(&settlement).await;
    host.finish_admission("review-failover", Some(&settlement))
        .await;
    let windows = host.services.store.quota_windows().await.unwrap();
    assert_eq!(
        windows
            .iter()
            .find(|w| w.quota_id == quota_ids[1])
            .unwrap()
            .cost_used,
        Decimal::ONE
    );
    assert_eq!(
        windows
            .iter()
            .find(|w| w.quota_id == quota_ids[0])
            .unwrap()
            .cost_used,
        Decimal::ZERO,
        "the failed credential must not receive the successful credential's cost"
    );
}

#[tokio::test]
async fn credential_budget_settles_without_usage_logs_and_blocks_each_limit() {
    let fixture = setup::fixture().await;
    super::setting(&fixture.app, "enable_usage", serde_json::json!(false)).await;
    let mut input = budget(fixture.credential);
    input.quota_total = Some(Decimal::ONE);
    input.quota_monthly = Some(Decimal::ONE);
    input.quota_weekly = Some(Decimal::ONE);
    input.quota_daily = Some(Decimal::ONE);
    let id = setup::id(
        fixture
            .app
            .mutate(ControlMutation::Quota(input.clone()))
            .await
            .unwrap(),
    );
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
    let target = &plan.targets[0];
    host.admit_credential(
        "budget-request",
        target,
        &Bytes::new(),
        SettleMode::OnResponse,
    )
    .await
    .unwrap();
    let settlement = gproxy_core::Settlement {
        attempts: Vec::new(),
        upstream_started_at_ms: None,
        request_id: "credential-budget-spend".into(),
        provider_id: fixture.provider,
        credential_id: target.credential,
        upstream_model: target.upstream_model.clone(),
        usage: Default::default(),
        cost: Decimal::ONE,
        source: gproxy_core::UsageSource::Upstream,
        ended: gproxy_core::Ended::Interrupted,
        latency_ms: 1,
    };
    tokio::join!(host.record(&settlement), host.record(&settlement));
    let windows: Vec<_> = host
        .services
        .store
        .quota_windows()
        .await
        .unwrap()
        .into_iter()
        .filter(|window| window.quota_id == id)
        .collect();
    assert_eq!(windows.len(), 4);
    assert!(
        windows
            .iter()
            .all(|window| window.cost_used == Decimal::ONE)
    );
    for kind in [
        QuotaWindowKind::Total,
        QuotaWindowKind::Monthly,
        QuotaWindowKind::Weekly,
        QuotaWindowKind::Daily,
    ] {
        let mut input = budget(fixture.credential);
        match kind {
            QuotaWindowKind::Total => input.quota_total = Some(Decimal::ONE),
            QuotaWindowKind::Monthly => input.quota_monthly = Some(Decimal::ONE),
            QuotaWindowKind::Weekly => input.quota_weekly = Some(Decimal::ONE),
            QuotaWindowKind::Daily => input.quota_daily = Some(Decimal::ONE),
            QuotaWindowKind::FiveHour | QuotaWindowKind::SevenDay => {
                panic!("not a calendar budget")
            }
        }
        host.services.store.update_quota(id, &input).await.unwrap();
        fixture.app.reload().await.unwrap();
        assert!(
            matches!(
                host.admit_credential(
                    "budget-request",
                    target,
                    &Bytes::new(),
                    SettleMode::OnResponse
                )
                .await,
                Err(CoreError::QuotaExceeded)
            ),
            "{kind:?}"
        );
        host.admit_credential("budget-request", target, &Bytes::new(), SettleMode::Free)
            .await
            .unwrap();
        let mut other = target.clone();
        other.credential = gproxy_core::CredentialId(fixture.credential + 100);
        host.admit_credential(
            "budget-request",
            &other,
            &Bytes::new(),
            SettleMode::OnResponse,
        )
        .await
        .unwrap();
        input.enabled = false;
        host.services.store.update_quota(id, &input).await.unwrap();
        fixture.app.reload().await.unwrap();
        host.admit_credential(
            "budget-request",
            target,
            &Bytes::new(),
            SettleMode::OnResponse,
        )
        .await
        .unwrap();
    }
    // Lifetime spend survives periodic rollovers and control-plane reloads.
    for window in windows {
        let next = host
            .services
            .store
            .ensure_quota_window(
                id,
                window.window_kind,
                window.reset_at.unwrap_or(2_000_000_000),
            )
            .await
            .unwrap();
        assert_eq!(
            next.cost_used,
            if window.window_kind == QuotaWindowKind::Total {
                Decimal::ONE
            } else {
                Decimal::ZERO
            }
        );
    }
}

#[tokio::test]
async fn credential_budget_zero_and_missing_prices_cannot_send_paid_requests() {
    let fixture = setup::fixture().await;
    let mut input = budget(fixture.credential);
    input.quota_daily = Some(Decimal::ZERO);
    let id = setup::id(
        fixture
            .app
            .mutate(ControlMutation::Quota(input.clone()))
            .await
            .unwrap(),
    );
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
    let mut target = plan.targets[0].clone();
    assert!(matches!(
        host.admit_credential(
            "budget-request",
            &target,
            &Bytes::new(),
            SettleMode::OnResponse
        )
        .await,
        Err(CoreError::QuotaExceeded)
    ));
    input.quota_daily = Some(Decimal::ONE);
    host.services.store.update_quota(id, &input).await.unwrap();
    fixture.app.reload().await.unwrap();
    target.upstream_model = "no-price".into();
    assert!(
        matches!(host.admit_credential("budget-request", &target, &Bytes::new(), SettleMode::OnResponse).await, Err(CoreError::Internal(message)) if message.contains("requires model pricing"))
    );
    host.admit_credential("budget-request", &target, &Bytes::new(), SettleMode::Free)
        .await
        .unwrap();
}

#[tokio::test]
async fn credential_budget_reserves_estimated_cost_until_the_request_settles() {
    let fixture = setup::fixture().await;
    let mut input = budget(fixture.credential);
    input.quota_total = Some(Decimal::from(1_000));
    let id = setup::id(
        fixture
            .app
            .mutate(ControlMutation::Quota(input.clone()))
            .await
            .unwrap(),
    );
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
    let target = &plan.targets[0];
    let body = Bytes::from_static(
        br#"{"model":"public-model","messages":[{"role":"user","content":"Summarise the quarterly report in three bullet points, then list the open risks."}]}"#,
    );
    host.admit_credential("reserve-1", target, &body, SettleMode::OnResponse)
        .await
        .unwrap();
    let window = host
        .services
        .store
        .quota_windows()
        .await
        .unwrap()
        .into_iter()
        .find(|window| window.quota_id == id)
        .expect("total window");
    let pending = host
        .services
        .cache
        .get(&format!("gproxy:quota-pending:{}", window.id))
        .await
        .unwrap()
        .expect("reservation counter");
    let estimate = i64::from_be_bytes(pending.as_slice().try_into().unwrap());
    assert!(estimate > 0, "estimate must charge the request's input");
    // Room for one request and a half: a second reservation must not fit.
    input.quota_total = Some(gproxy_core::usage::micros_to_cost(estimate * 3 / 2));
    host.services.store.update_quota(id, &input).await.unwrap();
    fixture.app.reload().await.unwrap();
    assert!(matches!(
        host.admit_credential("reserve-2", target, &body, SettleMode::OnResponse)
            .await,
        Err(CoreError::QuotaExceeded)
    ));
    host.finish_admission("reserve-1", None).await;
    let released = host
        .services
        .cache
        .get(&format!("gproxy:quota-pending:{}", window.id))
        .await
        .unwrap()
        .map(|bytes| i64::from_be_bytes(bytes.as_slice().try_into().unwrap()))
        .unwrap_or_default();
    assert_eq!(released, 0);
    host.admit_credential("reserve-2", target, &body, SettleMode::OnResponse)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires an empty PostgreSQL database via GPROXY_TEST_APP_POSTGRES_DSN"]
async fn postgres_failover_and_cache_recovery_keep_exact_spend() {
    let fixture = setup::fixture_with_postgres(Some(
        std::env::var("GPROXY_TEST_APP_POSTGRES_DSN").expect("GPROXY_TEST_APP_POSTGRES_DSN"),
    ))
    .await;
    super::setting(&fixture.app, "enable_usage", serde_json::json!(false)).await;
    let second = setup::id(
        fixture
            .app
            .mutate(ControlMutation::Credential {
                provider_id: fixture.provider,
                label: None,
                enabled: true,
                secret: serde_json::json!({"api_key": setup::random_key()}),
            })
            .await
            .unwrap(),
    );
    let mut quotas = Vec::new();
    for credential in [fixture.credential, second] {
        let mut input = budget(credential);
        input.quota_total = Some(Decimal::from(1000));
        quotas.push(setup::id(
            fixture
                .app
                .mutate(ControlMutation::Quota(input))
                .await
                .unwrap(),
        ));
    }
    let host = &fixture.app.inner.host;
    let request = setup::request("postgres-recovery", "hello", &fixture.client_key);
    let identity = host.authenticate(&request).await.unwrap();
    let plan = host
        .services
        .control
        .resolve(
            Some("public-model"),
            &gproxy_core::RoutingMode::Aggregated,
            None,
        )
        .unwrap();
    host.admit(
        &identity,
        &request,
        Some(super::generation_operation()),
        Some("public-model"),
        &plan,
    )
    .await
    .unwrap();
    let mut target = plan.targets[0].clone();
    target.credential = gproxy_core::CredentialId(fixture.credential);
    for _ in 0..2 {
        host.admit_credential(
            &request.request_id,
            &target,
            &request.body,
            SettleMode::OnResponse,
        )
        .await
        .unwrap();
    }
    target.credential = gproxy_core::CredentialId(second);
    host.admit_credential(
        &request.request_id,
        &target,
        &request.body,
        SettleMode::OnResponse,
    )
    .await
    .unwrap();
    let settlement = gproxy_core::Settlement {
        request_id: request.request_id.clone(),
        provider_id: fixture.provider,
        credential_id: target.credential,
        upstream_model: target.upstream_model,
        attempts: Vec::new(),
        upstream_started_at_ms: None,
        usage: Default::default(),
        cost: Decimal::new(2, 1),
        source: gproxy_core::UsageSource::Upstream,
        ended: gproxy_core::Ended::Complete,
        latency_ms: 1,
    };
    host.services
        .cache
        .testing
        .fail_once("raise", "gproxy:", false);
    tokio::time::timeout(std::time::Duration::from_secs(5), host.record(&settlement))
        .await
        .unwrap();
    host.services
        .cache
        .testing
        .fail_once("compare_incr", "gproxy:credential-admission:", true);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            host.finish_admission(&request.request_id, Some(&settlement)),
            host.finish_admission(&request.request_id, Some(&settlement))
        );
    })
    .await
    .unwrap();
    assert!(
        !fixture
            .app
            .admission_pending(&request.request_id)
            .await
            .unwrap()
    );
    for window in host.services.store.quota_windows().await.unwrap() {
        let expected = if window.quota_id == quotas[0] {
            Decimal::ZERO
        } else {
            Decimal::new(2, 1)
        };
        assert_eq!(window.cost_used, expected);
        assert_eq!(setup::counter(host, window.id).await, 0);
        assert_eq!(
            host.services
                .cache
                .incr(&format!("gproxy:quota-used:{}", window.id), 0, None)
                .await
                .unwrap(),
            gproxy_core::usage::cost_to_micros(expected).unwrap()
        );
    }
}

#[tokio::test]
async fn credential_reservation_unknown_commit_does_not_leak_or_refund_twice() {
    let fixture = setup::fixture().await;
    let mut input = budget(fixture.credential);
    input.quota_total = Some(Decimal::from(1_000));
    let quota_id = setup::id(
        fixture
            .app
            .mutate(ControlMutation::Quota(input))
            .await
            .unwrap(),
    );
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
    let body = Bytes::from_static(
        br#"{"model":"public-model","messages":[{"role":"user","content":"Hello"}]}"#,
    );
    let request_id = "credential-lost-reply";
    host.admit_credential(request_id, &plan.targets[0], &body, SettleMode::OnResponse)
        .await
        .unwrap();
    let windows: Vec<_> = host
        .services
        .store
        .quota_windows()
        .await
        .unwrap()
        .into_iter()
        .filter(|w| w.quota_id == quota_id)
        .collect();
    let mut before = Vec::new();
    for window in &windows {
        before.push(setup::counter(host, window.id).await);
    }
    host.services.cache.testing.fail_times(
        "reserve_state",
        "gproxy:credential-admission:",
        true,
        8,
    );
    assert!(
        host.admit_credential(request_id, &plan.targets[0], &body, SettleMode::OnResponse)
            .await
            .is_err()
    );
    for (window, before) in windows.iter().zip(before) {
        assert_eq!(setup::counter(host, window.id).await, before);
    }
    host.finish_admission(request_id, None).await;
    host.finish_admission(request_id, None).await;
    for window in windows {
        assert_eq!(setup::counter(host, window.id).await, 0);
    }
}
