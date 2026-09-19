use gproxy_core::{CacheBackend, ControlPlane, Host};
use rust_decimal::Decimal;

use super::setup;

const QUOTA_INPUT: &str = "one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty";

#[tokio::test]
async fn admission_and_retry_without_spend_limits_do_not_tokenize() {
    let fixture = setup::fixture().await;
    let app = &fixture.app;
    let host = &app.inner.host;
    host.services
        .store
        .delete_quota(fixture.quota)
        .await
        .unwrap();
    app.reload().await.unwrap();
    let request = setup::request("unlimited-retry", QUOTA_INPUT, &fixture.client_key);
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
    let operation = super::generation_operation();
    host.admit(
        &identity,
        &request,
        Some(operation),
        Some("public-model"),
        &plan,
    )
    .await
    .unwrap();
    assert!(!host.services.token_counts.contains(&request.request_id));
    host.admit_retry(
        &request.request_id,
        &plan.targets[0],
        &request.body,
        operation.operation().spec().settle,
    )
    .await
    .unwrap();
    assert!(!host.services.token_counts.contains(&request.request_id));
    host.finish_admission(&request.request_id, None).await;
}

#[tokio::test]
async fn disabled_credentials_keep_quota_metadata_but_cannot_send_requests() {
    use crate::{ControlMutation, MutationResult};
    use gproxy_admin::State;
    use gproxy_core::CredentialStore;

    let fixture = setup::fixture().await;
    assert!(
        !fixture
            .app
            .channel_catalogue()
            .iter()
            .any(|channel| channel.id == "groq")
    );
    for (channel, subscription) in [
        ("openai", false),
        ("codex", true),
        ("removed-channel", false),
    ] {
        let MutationResult::Id(provider) = fixture
            .app
            .mutate(ControlMutation::Provider(
                gproxy_store::records::ProviderInput {
                    name: format!("disabled-{channel}"),
                    label: None,
                    channel: channel.into(),
                    settings: serde_json::json!({}),
                    credential_strategy: "round_robin".into(),
                    proxy_url: None,
                    tls_fingerprint: None,
                    enabled: true,
                },
            ))
            .await
            .unwrap()
        else {
            panic!("provider id")
        };
        let MutationResult::Id(credential) = fixture
            .app
            .mutate(ControlMutation::Credential {
                provider_id: provider,
                label: None,
                secret: serde_json::json!({"api_key": setup::random_key()}),
                enabled: false,
            })
            .await
            .unwrap()
        else {
            panic!("credential id")
        };
        let capability = fixture
            .app
            .credential_quota_capabilities(credential)
            .await
            .unwrap();
        assert_eq!(capability.unwrap().probe, subscription);
        let snapshot = fixture
            .app
            .credential_quota_snapshot(credential)
            .await
            .unwrap();
        if channel == "removed-channel" {
            assert_eq!(
                snapshot.sources[0].capability.support,
                gproxy_channel_api::QuotaSupport::Unsupported
            );
            assert!(snapshot.entries.is_empty());
        }
        if subscription {
            assert_reset_credits_need_full_probe(&fixture.app, credential).await;
        }
        assert!(
            fixture
                .app
                .inner
                .host
                .load(gproxy_core::CredentialId(credential))
                .await
                .is_err()
        );
    }
    let MutationResult::Id(orphan) = fixture
        .app
        .mutate(ControlMutation::Credential {
            provider_id: i64::MAX,
            label: None,
            secret: serde_json::json!({"api_key": setup::random_key()}),
            enabled: false,
        })
        .await
        .unwrap()
    else {
        panic!("orphan credential id")
    };
    assert!(
        fixture
            .app
            .credential_quota_capabilities(orphan)
            .await
            .unwrap()
            .is_none()
    );
}

async fn assert_reset_credits_need_full_probe(app: &crate::AppHandle, credential: i64) {
    use gproxy_admin::State;
    use gproxy_channel_api::{QuotaRefreshError, QuotaResetCredits, QuotaSourceState};
    use gproxy_core::CacheBackend;

    let snapshot = app.credential_quota_snapshot(credential).await.unwrap();
    let source = snapshot
        .sources
        .into_iter()
        .find(|source| source.capability.id == "subscription")
        .unwrap();
    let version = app
        .store()
        .credential(credential)
        .await
        .unwrap()
        .unwrap()
        .version;
    let now = crate::quota_refresh::now() * 1000;
    let mut state = QuotaSourceState {
        capability: source.capability,
        attempted_at_ms: Some(now - 1000),
        observed_at_ms: Some(now - 1000),
        error: None,
        reset_credits: Some(QuotaResetCredits {
            available_count: 2,
            expires_at: None,
        }),
    };
    app.store()
        .save_credential_quota_source(credential, version, &state, Some(&[]))
        .await
        .unwrap();
    state.attempted_at_ms = Some(now);
    state.error = Some(QuotaRefreshError {
        code: "rate_limited".into(),
        message: "retry later".into(),
    });
    state.reset_credits = None;
    app.store()
        .save_credential_quota_source(credential, version, &state, None)
        .await
        .unwrap();
    app.inner
        .host
        .services
        .cache
        .set(
            &format!("quota:source:{credential}:v{version}:subscription:upstream-retry"),
            vec![1],
            Some(std::time::Duration::from_secs(60)),
        )
        .await
        .unwrap();
    let result = app.quota_probe(credential, true).await.unwrap();
    assert_eq!(result.reset_credits.unwrap().available_count, 2);
    assert_eq!(
        result.snapshot.sources[0].error.as_ref().unwrap().code,
        "rate_limited"
    );
    assert_eq!(result.snapshot.sources[0].attempted_at_ms, Some(now));
}

#[tokio::test]
async fn admission_refunds_reconciles_and_leaves_no_failed_reservation() {
    let setup::Fixture {
        app,
        provider,
        credential,
        route: _,
        quota,
        client_key,
        _directory,
    } = setup::fixture().await;

    let host = &app.inner.host;
    let plan = host
        .services
        .control
        .resolve(
            Some("public-model"),
            &gproxy_core::RoutingMode::Aggregated,
            None,
        )
        .expect("plan");
    let first = setup::request("refund", QUOTA_INPUT, &client_key);
    let identity = host.authenticate(&first).await.expect("authenticate");
    let operation = super::generation_operation();
    host.admit(&identity, &first, Some(operation), None, &plan)
        .await
        .expect("first admission");
    assert!(app.admission_pending(&first.request_id).await.unwrap());
    let overlap = setup::request("overlap", QUOTA_INPUT, &client_key);
    assert!(matches!(
        host.admit(&identity, &overlap, Some(operation), None, &plan)
            .await,
        Err(gproxy_core::CoreError::QuotaExceeded)
    ));
    assert!(!app.admission_pending(&overlap.request_id).await.unwrap());
    host.finish_admission(&first.request_id, None).await;
    assert!(!app.admission_pending(&first.request_id).await.unwrap());

    let second = setup::request("settle", QUOTA_INPUT, &client_key);
    host.admit(&identity, &second, Some(operation), None, &plan)
        .await
        .expect("second admission");
    let settlement = gproxy_core::Settlement {
        attempts: Vec::new(),
        upstream_started_at_ms: None,
        request_id: second.request_id.clone(),
        provider_id: provider,
        credential_id: gproxy_core::CredentialId(credential),
        upstream_model: "upstream-model".into(),
        usage: gproxy_core::NormalizedUsage {
            input_tokens: 10,
            output_tokens: 5,
            ..Default::default()
        },
        cost: Decimal::new(2, 1),
        source: gproxy_core::UsageSource::Upstream,
        ended: gproxy_core::Ended::Complete,
        latency_ms: 1,
    };
    tokio::join!(
        host.finish_admission(&second.request_id, Some(&settlement)),
        host.finish_admission(&second.request_id, Some(&settlement)),
    );
    assert!(!app.admission_pending(&second.request_id).await.unwrap());

    let windows: Vec<_> = app
        .quota_windows()
        .await
        .unwrap()
        .into_iter()
        .filter(|window| window.quota_id == quota)
        .collect();
    assert_eq!(windows.len(), 2);
    assert!(windows.iter().any(|window| window.reset_at.is_none()));
    assert!(windows.iter().any(|window| window.reset_at.is_some()));
    for window in &windows {
        assert_eq!(window.cost_used, Decimal::new(2, 1));
        assert_eq!(setup::counter(host, window.id).await, 0);
    }

    let rejected = setup::request("reject", QUOTA_INPUT, &client_key);
    assert!(matches!(
        host.admit(&identity, &rejected, Some(operation), None, &plan)
            .await,
        Err(gproxy_core::CoreError::QuotaExceeded)
    ));
    assert!(!app.admission_pending(&rejected.request_id).await.unwrap());
    for window in &windows {
        assert_eq!(setup::counter(host, window.id).await, 0);
    }
}

#[tokio::test]
async fn concurrent_admissions_cannot_overrun_the_cached_used_view() {
    let setup::Fixture {
        app,
        client_key,
        _directory,
        ..
    } = setup::fixture().await;
    let host = &app.inner.host;
    let plan = host
        .services
        .control
        .resolve(
            Some("public-model"),
            &gproxy_core::RoutingMode::Aggregated,
            None,
        )
        .expect("plan");
    let identity = host
        .authenticate(&setup::request("concurrent", QUOTA_INPUT, &client_key))
        .await
        .expect("authenticate");
    let operation = super::generation_operation();
    let mut calls = Vec::new();
    for index in 0..8 {
        let host = host.clone();
        let plan = plan.clone();
        let identity = identity.clone();
        let request = setup::request(&format!("concurrent-{index}"), QUOTA_INPUT, &client_key);
        calls.push(async move {
            host.admit(
                &identity,
                &request,
                Some(operation),
                Some("public-model"),
                &plan,
            )
            .await
        });
    }
    let results = futures_util::future::join_all(calls).await;
    let allowed = results.iter().filter(|result| result.is_ok()).count();
    let denied = results
        .iter()
        .filter(|result| matches!(result, Err(gproxy_core::CoreError::QuotaExceeded)))
        .count();
    assert_eq!(allowed + denied, 8);
    assert_eq!(allowed, 1);
    assert_eq!(denied, 7);
}

#[tokio::test]
async fn missing_used_counter_reloads_exhausted_total_from_store() {
    let setup::Fixture {
        app,
        quota,
        client_key,
        _directory,
        ..
    } = setup::fixture().await;
    let record = app
        .inner
        .host
        .services
        .control
        .current()
        .quotas
        .iter()
        .find(|record| record.id == quota)
        .unwrap()
        .clone();
    app.inner
        .host
        .services
        .store
        .update_quota(
            quota,
            &gproxy_store::records::QuotaInput {
                subject_kind: record.subject_kind,
                subject_id: record.subject_id,
                quota_total: Some(Decimal::new(3, 1)),
                quota_monthly: None,
                quota_weekly: None,
                quota_daily: None,
                quota_5h: None,
                quota_7d: None,
                enabled: true,
            },
        )
        .await
        .unwrap();
    app.reload().await.unwrap();
    let host = &app.inner.host;
    let plan = host
        .services
        .control
        .resolve(
            Some("public-model"),
            &gproxy_core::RoutingMode::Aggregated,
            None,
        )
        .expect("plan");
    let identity = host
        .authenticate(&setup::request("seed", QUOTA_INPUT, &client_key))
        .await
        .expect("authenticate");
    let operation = super::generation_operation();
    let first = setup::request("seed-first", QUOTA_INPUT, &client_key);
    host.admit(
        &identity,
        &first,
        Some(operation),
        Some("public-model"),
        &plan,
    )
    .await
    .expect("first admission");
    let settlement = gproxy_core::Settlement {
        attempts: Vec::new(),
        upstream_started_at_ms: None,
        request_id: first.request_id.clone(),
        provider_id: 0,
        credential_id: gproxy_core::CredentialId(0),
        upstream_model: "upstream-model".into(),
        usage: Default::default(),
        cost: Decimal::new(3, 1),
        source: gproxy_core::UsageSource::Upstream,
        ended: gproxy_core::Ended::Complete,
        latency_ms: 1,
    };
    host.finish_admission(&first.request_id, Some(&settlement))
        .await;
    let window = app
        .quota_windows()
        .await
        .unwrap()
        .into_iter()
        .find(|window| window.quota_id == quota && window.reset_at.is_none())
        .expect("total window");
    host.services
        .cache
        .delete(&format!("gproxy:quota-used:{}", window.id))
        .await
        .unwrap();
    let rejected = setup::request("seed-reject", QUOTA_INPUT, &client_key);
    assert!(matches!(
        host.admit(
            &identity,
            &rejected,
            Some(operation),
            Some("public-model"),
            &plan
        )
        .await,
        Err(gproxy_core::CoreError::QuotaExceeded)
    ));
}

#[tokio::test]
async fn cache_failures_and_lost_replies_retry_without_losing_or_duplicating_spend() {
    for (operation, after) in [
        ("raise", false),
        ("raise", true),
        ("compare_incr", false),
        ("compare_incr", true),
    ] {
        let fixture = setup::fixture().await;
        let host = &fixture.app.inner.host;
        let request = setup::request("recover-settlement", "hello", &fixture.client_key);
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
        // An active request keeps its identity and reservations for its whole
        // lifetime; a long stream/session must not receive a one-hour expiry.
        // Only the ceiling for a state that outlived its host applies, and the
        // reservation written after it preserves that expiry instead of
        // replacing it.
        assert_eq!(
            host.services
                .cache
                .testing
                .ttl(&format!("gproxy:admission:{}", request.request_id)),
            Some(Some(crate::host::ADMISSION_TTL))
        );
        host.services
            .cache
            .testing
            .fail_once(operation, "gproxy:", after);
        let settlement = gproxy_core::Settlement {
            request_id: request.request_id.clone(),
            provider_id: fixture.provider,
            credential_id: gproxy_core::CredentialId(fixture.credential),
            upstream_model: "upstream-model".into(),
            upstream_started_at_ms: None,
            attempts: Vec::new(),
            usage: Default::default(),
            cost: Decimal::new(2, 1),
            source: gproxy_core::UsageSource::Upstream,
            ended: gproxy_core::Ended::Complete,
            latency_ms: 1,
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            host.finish_admission(&request.request_id, Some(&settlement)),
        )
        .await
        .unwrap();
        host.finish_admission(&request.request_id, Some(&settlement))
            .await;
        assert!(
            !fixture
                .app
                .admission_pending(&request.request_id)
                .await
                .unwrap()
        );
        for window in host
            .services
            .store
            .quota_windows()
            .await
            .unwrap()
            .into_iter()
            .filter(|window| window.quota_id == fixture.quota)
        {
            assert_eq!(
                window.cost_used,
                Decimal::new(2, 1),
                "{operation}, after={after}"
            );
            assert_eq!(setup::counter(host, window.id).await, 0);
            assert_eq!(
                host.services
                    .cache
                    .incr(&format!("gproxy:quota-used:{}", window.id), 0, None)
                    .await
                    .unwrap(),
                200_000
            );
        }
        assert!(
            host.services
                .cache
                .get(&format!("gproxy:quota-failed:{}", fixture.quota))
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn rejected_admission_releases_token_counts_and_pending_spend() {
    let fixture = setup::fixture().await;
    let host = &fixture.app.inner.host;
    let request = setup::request("rate-reject", "hello", &fixture.client_key);
    let identity = host.authenticate(&request).await.unwrap();
    let rate_id = setup::id(
        fixture
            .app
            .mutate(crate::ControlMutation::RateLimit(
                gproxy_store::records::RateLimitInput {
                    subject_kind: "user".into(),
                    subject_id: identity.user_id,
                    requests: 1,
                    window_seconds: 60,
                },
            ))
            .await
            .unwrap(),
    );
    let plan = host
        .services
        .control
        .resolve(
            Some("public-model"),
            &gproxy_core::RoutingMode::Aggregated,
            None,
        )
        .unwrap();
    let first = setup::request("rate-first", "hello", &fixture.client_key);
    host.admit(
        &identity,
        &first,
        Some(super::generation_operation()),
        Some("public-model"),
        &plan,
    )
    .await
    .unwrap();
    host.finish_admission(&first.request_id, None).await;
    assert!(matches!(
        host.admit(
            &identity,
            &request,
            Some(super::generation_operation()),
            Some("public-model"),
            &plan
        )
        .await,
        Err(gproxy_core::CoreError::RateLimited { .. })
    ));
    assert!(!host.services.token_counts.contains(&request.request_id));
    // A cache error during the rate check must perform the same cleanup.
    host.services
        .cache
        .testing
        .fail_once("incr", &format!("gproxy:rate:{rate_id}:"), false);
    assert!(
        host.admit(
            &identity,
            &request,
            Some(super::generation_operation()),
            Some("public-model"),
            &plan
        )
        .await
        .is_err()
    );
    assert!(!host.services.token_counts.contains(&request.request_id));
    // Use a new rate window for the state-write failure, without wall-clock sleeps.
    let now = crate::quota_refresh::now();
    host.services
        .cache
        .delete(&format!(
            "gproxy:rate:{rate_id}:{}",
            now - now.rem_euclid(60)
        ))
        .await
        .unwrap();
    host.services
        .cache
        .testing
        .fail_times("compare_swap", "gproxy:admission:", false, 8);
    assert!(
        host.admit(
            &identity,
            &request,
            Some(super::generation_operation()),
            Some("public-model"),
            &plan
        )
        .await
        .is_err()
    );
    assert!(!host.services.token_counts.contains(&request.request_id));
    assert!(
        !fixture
            .app
            .admission_pending(&request.request_id)
            .await
            .unwrap()
    );
    for window in host.services.store.quota_windows().await.unwrap() {
        assert_eq!(setup::counter(host, window.id).await, 0);
    }
}

#[tokio::test]
async fn cancelled_native_admission_refunds_a_committed_reservation() {
    let fixture = setup::fixture().await;
    let host = &fixture.app.inner.host;
    let request = setup::request("cancelled-admission", "hello", &fixture.client_key);
    let request_id = request.request_id.clone();
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
    let (entered, resume) = host
        .services
        .cache
        .testing
        .pause_after("reserve_state", "gproxy:admission:");
    let admitting = host.clone();
    let task = tokio::spawn(async move {
        admitting
            .admit(
                &identity,
                &request,
                Some(super::generation_operation()),
                Some("public-model"),
                &plan,
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    assert!(fixture.app.admission_pending(&request_id).await.unwrap());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    resume.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while fixture.app.admission_pending(&request_id).await.unwrap() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!host.services.token_counts.contains(&request_id));
    for window in host.services.store.quota_windows().await.unwrap() {
        assert_eq!(setup::counter(host, window.id).await, 0);
    }
}

#[tokio::test]
async fn deleting_a_quota_during_a_request_does_not_retry_forever() {
    let fixture = setup::fixture().await;
    let host = &fixture.app.inner.host;
    let request = setup::request("deleted-quota", "hello", &fixture.client_key);
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
    let windows = host.services.store.quota_windows().await.unwrap();
    host.services
        .store
        .delete_quota(fixture.quota)
        .await
        .unwrap();
    let settlement = gproxy_core::Settlement {
        attempts: Vec::new(),
        upstream_started_at_ms: None,
        request_id: request.request_id.clone(),
        provider_id: fixture.provider,
        credential_id: gproxy_core::CredentialId(fixture.credential),
        upstream_model: "upstream-model".into(),
        usage: gproxy_core::NormalizedUsage::default(),
        cost: Decimal::new(2, 1),
        source: gproxy_core::UsageSource::Upstream,
        ended: gproxy_core::Ended::Complete,
        latency_ms: 1,
    };
    let finished = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        host.finish_admission(&request.request_id, Some(&settlement)),
    )
    .await;
    assert!(
        finished.is_ok(),
        "deleted quota windows cannot recover through retry"
    );
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
    assert!(
        host.services
            .cache
            .get(&format!("gproxy:quota-failed:{}", fixture.quota))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn exhausted_settlement_retries_preserve_state_for_idempotent_replay() {
    let fixture = setup::fixture().await;
    let host = &fixture.app.inner.host;
    let request = setup::request("replay-after-outage", "hello", &fixture.client_key);
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
    let settlement = gproxy_core::Settlement {
        attempts: Vec::new(),
        upstream_started_at_ms: None,
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
    host.services
        .cache
        .testing
        .fail_times("raise", "gproxy:", false, 8);
    tokio::time::timeout(
        std::time::Duration::from_secs(6),
        host.finish_admission(&request.request_id, Some(&settlement)),
    )
    .await
    .unwrap();
    assert!(
        fixture
            .app
            .admission_pending(&request.request_id)
            .await
            .unwrap()
    );
    assert!(
        host.services
            .cache
            .get(&format!("gproxy:quota-failed:{}", fixture.quota))
            .await
            .unwrap()
            .is_some()
    );
    let windows = host.services.store.quota_windows().await.unwrap();
    assert!(
        windows
            .iter()
            .any(|window| window.cost_used == settlement.cost)
    );
    for window in &windows {
        assert!(setup::counter(host, window.id).await > 0);
    }
    host.finish_admission(&request.request_id, Some(&settlement))
        .await;
    assert!(
        !fixture
            .app
            .admission_pending(&request.request_id)
            .await
            .unwrap()
    );
    for window in host.services.store.quota_windows().await.unwrap() {
        assert_eq!(window.cost_used, settlement.cost);
        assert_eq!(setup::counter(host, window.id).await, 0);
    }
    assert!(
        host.services
            .cache
            .get(&format!("gproxy:quota-failed:{}", fixture.quota))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn reservation_lost_replies_preserve_exact_pending_and_refund_unknown_commits() {
    for (after, failures) in [(false, 1), (true, 1), (false, 8), (true, 8)] {
        let fixture = setup::fixture().await;
        let host = &fixture.app.inner.host;
        let request = setup::request("reserve-lost-reply", "hello", &fixture.client_key);
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
        host.services.cache.testing.fail_times(
            "reserve_state",
            "gproxy:admission:",
            after,
            failures,
        );
        let result = host
            .admit(
                &identity,
                &request,
                Some(super::generation_operation()),
                Some("public-model"),
                &plan,
            )
            .await;
        assert_eq!(
            result.is_ok(),
            failures == 1,
            "after={after}, failures={failures}"
        );
        if result.is_ok() {
            let raw = host
                .services
                .cache
                .get(&format!("gproxy:admission:{}", request.request_id))
                .await
                .unwrap()
                .unwrap();
            let state: serde_json::Value = serde_json::from_slice(&raw).unwrap();
            for reservation in state["reservations"].as_array().unwrap() {
                assert_eq!(
                    setup::counter(host, reservation["window_id"].as_i64().unwrap()).await,
                    reservation["estimated_cost_micros"].as_i64().unwrap()
                );
            }
            host.finish_admission(&request.request_id, None).await;
        }
        assert!(
            !fixture
                .app
                .admission_pending(&request.request_id)
                .await
                .unwrap()
        );
        for window in host.services.store.quota_windows().await.unwrap() {
            let pending = host
                .services
                .cache
                .get(&format!("gproxy:quota-pending:{}", window.id))
                .await
                .unwrap();
            assert!(
                pending.is_none_or(|bytes| i64::from_be_bytes(
                    bytes.as_slice().try_into().unwrap()
                ) == 0),
                "after={after}, failures={failures}"
            );
        }
    }
}

#[tokio::test]
async fn fallback_unknown_commit_refunds_only_its_new_reservations() {
    let fixture = setup::fixture().await;
    let host = &fixture.app.inner.host;
    let request = setup::request("fallback-lost-reply", "hello", &fixture.client_key);
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
    let windows = host.services.store.quota_windows().await.unwrap();
    let mut before = Vec::new();
    for window in &windows {
        before.push(setup::counter(host, window.id).await);
    }
    host.services
        .cache
        .testing
        .fail_times("reserve_state", "gproxy:admission:", true, 8);
    assert!(
        host.admit_retry(
            &request.request_id,
            &plan.targets[0],
            &request.body,
            gproxy_protocol::SettleMode::OnResponse
        )
        .await
        .is_err()
    );
    for (window, before) in windows.iter().zip(before) {
        assert_eq!(setup::counter(host, window.id).await, before);
    }
    host.finish_admission(&request.request_id, None).await;
    for window in windows {
        assert_eq!(setup::counter(host, window.id).await, 0);
    }
}

#[tokio::test]
async fn cancelled_native_admission_bounds_window_lookup() {
    let fixture = setup::fixture().await;
    let host = &fixture.app.inner.host;
    let request = setup::request("cancelled-window-lookup", "hello", &fixture.client_key);
    let request_id = request.request_id.clone();
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
    let (entered, _resume) = host
        .services
        .cache
        .testing
        .pause_after("get", "gproxy:quota-window:");
    let admitting = host.clone();
    let task = tokio::spawn(async move {
        admitting
            .admit(
                &identity,
                &request,
                Some(super::generation_operation()),
                Some("public-model"),
                &plan,
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(6), async {
        while fixture.app.admission_pending(&request_id).await.unwrap() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled admission must stop its window lookup and release state");
    assert!(!host.services.token_counts.contains(&request_id));
}
