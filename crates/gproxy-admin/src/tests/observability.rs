use super::*;
use gproxy_store::records::{
    CredentialInput, CredentialQuotaObservation, QuotaBoundaryConfidence, QuotaBoundarySource,
    SettingInput,
};

#[tokio::test]
async fn cycle_pages_preserve_disabled_usage_and_validate_cursor_limits() {
    let state = state().await;
    seed_admin_key(&state).await;
    for window in ["primary", "weekly"] {
        state
            .store
            .observe_credential_quota_cycle(&CredentialQuotaObservation {
                credential_id: 7,
                window_key: window.into(),
                label: None,
                period_start: Some(0),
                period_end: Some(100),
                observed_at: 10,
                boundary_source: QuotaBoundarySource::Upstream,
                boundary_confidence: QuotaBoundaryConfidence::Exact,
                sample: gproxy_core::QuotaSample {
                    source: gproxy_core::QuotaSampleSource::Unknown,
                    started_at_ms: 10_000,
                    received_at_ms: 10_000,
                },
                scope: gproxy_core::QuotaScope::All,
                reset_behavior: gproxy_core::QuotaResetBehavior::Periodic,
                unit: None,
                upstream_used: None,
                upstream_limit: None,
                used_percent: Some(10.into()),
            })
            .await
            .unwrap();
    }
    state
        .store
        .record_usage(&gproxy_store::records::UsageInput {
            upstream_started_at_ms: Some(11_000),
            request_id: "cycle-page-usage".into(),
            at: 12,
            provider_id: 1,
            credential_id: 7,
            organization_id: None,
            team_id: None,
            user_id: None,
            user_key_id: None,
            operation: Some("generate_content".into()),
            upstream_model: "page-model".into(),
            input_tokens: 3,
            output_tokens: 2,
            cached_input_tokens: 0,
            metrics: serde_json::json!({}),
            dimensions: serde_json::json!({}),
            cost: 1.into(),
            usage_source: "upstream".into(),
            ended: "complete".into(),
            latency_ms: 1,
        })
        .await
        .unwrap();
    let parts = admin_parts(Method::POST, "/admin/api/credential-cycles/page");
    let first = crate::dispatch(
        &state,
        &parts,
        Bytes::from_static(br#"{"from":0,"to":100,"limit":1}"#),
    )
    .await
    .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first: crate::dto::CredentialCyclePageDto = serde_json::from_slice(first.body()).unwrap();
    assert_eq!(first.items.len(), 1);
    assert!(!first.items[0].metrics.as_object().unwrap().is_empty());
    assert_eq!(first.items[0].models[0].model, "page-model");
    assert!(first.items[0].observations.is_empty());
    assert!(first.items[0].estimate.is_none());
    state
        .store
        .set_setting(&SettingInput {
            key: gproxy_store::records::ENABLE_USAGE.into(),
            value: serde_json::json!(false),
        })
        .await
        .unwrap();
    let body = serde_json::to_vec(
        &serde_json::json!({"from":0,"to":100,"limit":1,"cursor":first.next_cursor}),
    )
    .unwrap();
    let next = crate::dispatch(&state, &parts, Bytes::from(body))
        .await
        .unwrap();
    assert_eq!(next.status(), StatusCode::OK);
    let next: crate::dto::CredentialCyclePageDto = serde_json::from_slice(next.body()).unwrap();
    assert_eq!(next.items.len(), 1);
    assert_ne!(next.items[0].id, first.items[0].id);
    assert!(next.next_cursor.is_none());
    assert_eq!(next.items[0].metrics, serde_json::json!({}));
    assert!(next.items[0].models.is_empty());
    assert_eq!(
        next.items[0].estimate.as_ref().unwrap().reason.as_deref(),
        Some("usage_disabled")
    );
    assert_eq!(next.items[0].used_percent.as_deref(), Some("10"));
    assert!(state.store.audit_events(100).await.unwrap().is_empty());
    for invalid in [
        serde_json::json!({"from":0,"to":100,"limit":0}),
        serde_json::json!({"from":0,"to":100,"limit":101}),
        serde_json::json!({"from":0,"to":100,"cursor":{"id":0,"last_observed_at":10}}),
    ] {
        let response = crate::dispatch(
            &state,
            &parts,
            Bytes::from(serde_json::to_vec(&invalid).unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn usage_record_pages_can_skip_counts_without_changing_legacy_totals_or_filters() {
    let state = state().await;
    seed_admin_key(&state).await;
    for n in 0..11 {
        state
            .store
            .record_usage(&gproxy_store::records::UsageInput {
                upstream_started_at_ms: Some(9_000),
                request_id: format!("page-{n}"),
                at: 10,
                provider_id: 1,
                credential_id: 7,
                organization_id: None,
                team_id: None,
                user_id: None,
                user_key_id: None,
                operation: Some("generate_content".into()),
                upstream_model: "model".into(),
                input_tokens: 3,
                output_tokens: 2,
                cached_input_tokens: 1,
                metrics: serde_json::json!({"cache_creation_5m_tokens":"0.25"}),
                dimensions: serde_json::json!({}),
                cost: "0.00000001".parse().unwrap(),
                usage_source: "upstream".into(),
                ended: "complete".into(),
                latency_ms: 1,
            })
            .await
            .unwrap();
    }
    for (suffix, total, count, more) in [
        ("", Some(11), 10, true),
        ("&include_total=false", None, 10, true),
        ("&include_total=false&page=2", None, 1, false),
        ("&include_total=true&request_id=page-0", Some(1), 1, false),
        ("&include_total=false&ended=interrupted", None, 0, false),
    ] {
        let response = crate::dispatch(
            &state,
            &admin_parts(
                Method::GET,
                &format!("/admin/api/usage-records?from=0&to=20{suffix}"),
            ),
            Bytes::new(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let page: crate::dto::UsageRecordPageDto = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(page.total, total);
        assert_eq!(page.items.len(), count);
        assert_eq!(page.has_more, more);
    }
    let response = crate::dispatch(
        &state,
        &admin_parts(Method::GET, "/admin/api/usage-summary?from=0&to=20"),
        Bytes::new(),
    )
    .await
    .unwrap();
    let summary: crate::dto::UsageSummaryDto = serde_json::from_slice(response.body()).unwrap();
    assert_eq!(summary.requests, 11);
    assert_eq!(summary.total_tokens, "57.75");
    assert_eq!(summary.cost, "0.00000011");
}

#[tokio::test]
async fn cycle_history_is_opt_in_and_usage_disabled_keeps_only_upstream_readings() {
    let state = state().await;
    seed_admin_key(&state).await;
    state
        .store
        .observe_credential_quota_cycle(&CredentialQuotaObservation {
            credential_id: 7,
            window_key: "primary".into(),
            label: None,
            period_start: Some(0),
            period_end: Some(100),
            observed_at: 10,
            boundary_source: QuotaBoundarySource::Upstream,
            boundary_confidence: QuotaBoundaryConfidence::Exact,
            sample: gproxy_core::QuotaSample {
                source: gproxy_core::QuotaSampleSource::Unknown,
                started_at_ms: 10_000,
                received_at_ms: 10_000,
            },
            scope: gproxy_core::QuotaScope::All,
            reset_behavior: gproxy_core::QuotaResetBehavior::Periodic,
            unit: None,
            upstream_used: None,
            upstream_limit: None,
            used_percent: Some(10.into()),
        })
        .await
        .unwrap();
    for (suffix, expected) in [
        ("", 0),
        ("&include_history=false", 0),
        ("&include_history=true", 1),
    ] {
        let response = crate::dispatch(
            &state,
            &admin_parts(
                Method::GET,
                &format!("/admin/api/credential-cycles?from=0&to=100{suffix}"),
            ),
            Bytes::new(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let cycles: Vec<crate::dto::CredentialQuotaCycleDto> =
            serde_json::from_slice(response.body()).unwrap();
        assert_eq!(cycles.len(), 1);
        assert_eq!(cycles[0].observations.len(), expected);
        if expected == 1 {
            assert!(cycles[0].observations[0].estimate.is_some());
        }
    }
    let response = crate::dispatch(&state, &admin_parts(Method::GET,
        "/admin/api/credential-cycles?from=0&to=100&include_history=true&include_estimate=false"), Bytes::new()).await.unwrap();
    let cycles: Vec<crate::dto::CredentialQuotaCycleDto> =
        serde_json::from_slice(response.body()).unwrap();
    assert!(cycles[0].estimate.is_none());
    assert_eq!(cycles[0].observations.len(), 1);
    assert!(cycles[0].observations[0].estimate.is_none());
    state
        .store
        .set_setting(&SettingInput {
            key: gproxy_store::records::ENABLE_USAGE.into(),
            value: serde_json::json!(false),
        })
        .await
        .unwrap();
    let response = crate::dispatch(
        &state,
        &admin_parts(
            Method::GET,
            "/admin/api/credential-cycles?from=0&to=100&include_history=true",
        ),
        Bytes::new(),
    )
    .await
    .unwrap();
    let cycles: Vec<crate::dto::CredentialQuotaCycleDto> =
        serde_json::from_slice(response.body()).unwrap();
    assert_eq!(
        cycles[0].observations[0].used_percent.as_deref(),
        Some("10")
    );
    assert!(cycles[0].observations[0].estimate.is_none());
    assert_eq!(
        cycles[0].estimate.as_ref().unwrap().reason.as_deref(),
        Some("usage_disabled")
    );
    let invalid = crate::dispatch(
        &state,
        &admin_parts(
            Method::GET,
            "/admin/api/credential-cycles?from=0&to=100&include_history=invalid",
        ),
        Bytes::new(),
    )
    .await;
    assert_eq!(invalid.unwrap().status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn cycle_history_filters_provider_and_credential_as_an_intersection() {
    let state = state().await;
    seed_admin_key(&state).await;
    let mut provider_ids = Vec::new();
    let mut credential_ids = Vec::new();
    for name in ["first-provider", "second-provider"] {
        let provider_id = state
            .store
            .insert_provider(&ProviderInput {
                name: name.into(),
                label: None,
                channel: "test".into(),
                settings: serde_json::json!({}),
                credential_strategy: "round_robin".into(),
                proxy_url: None,
                tls_fingerprint: None,
                enabled: true,
            })
            .await
            .unwrap();
        provider_ids.push(provider_id);
        let credential_id = state
            .store
            .insert_credential(&CredentialInput {
                provider_id,
                label: None,
                kind: "api_key".into(),
                envelope: envelope(),
                enabled: true,
                weight: 1,
                rpm_limit: None,
                tpm_limit: None,
                proxy_url: None,
                tls_fingerprint: None,
            })
            .await
            .unwrap();
        credential_ids.push(credential_id);
        state
            .store
            .observe_credential_quota_cycle(&CredentialQuotaObservation {
                credential_id,
                window_key: "primary".into(),
                label: None,
                period_start: Some(0),
                period_end: Some(100),
                observed_at: 10,
                boundary_source: QuotaBoundarySource::Upstream,
                boundary_confidence: QuotaBoundaryConfidence::Exact,
                sample: gproxy_core::QuotaSample {
                    source: gproxy_core::QuotaSampleSource::Unknown,
                    started_at_ms: 10_000,
                    received_at_ms: 10_000,
                },
                scope: gproxy_core::QuotaScope::All,
                reset_behavior: gproxy_core::QuotaResetBehavior::Periodic,
                unit: None,
                upstream_used: None,
                upstream_limit: None,
                used_percent: Some(10.into()),
            })
            .await
            .unwrap();
    }
    let first_provider = provider_ids[0];
    let first_credential = credential_ids[0];
    let second_credential = credential_ids[1];
    for (filter, mut expected) in [
        (String::new(), credential_ids.clone()),
        (
            format!("&provider_id={first_provider}"),
            vec![first_credential],
        ),
        (
            format!("&provider_id={first_provider}&credential_id={first_credential}"),
            vec![first_credential],
        ),
        (
            format!("&provider_id={first_provider}&credential_id={second_credential}"),
            vec![],
        ),
        ("&provider_id=999999".into(), vec![]),
    ] {
        let response = crate::dispatch(
            &state,
            &admin_parts(
                Method::GET,
                &format!("/admin/api/credential-cycles?from=0&to=100&include_history=true{filter}"),
            ),
            Bytes::new(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{filter}");
        let cycles: Vec<crate::dto::CredentialQuotaCycleDto> =
            serde_json::from_slice(response.body()).unwrap();
        assert!(cycles.iter().all(|cycle| cycle.observations.len() == 1));
        let mut actual = cycles
            .iter()
            .map(|cycle| cycle.credential_id)
            .collect::<Vec<_>>();
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected, "{filter}");
    }
    let response = crate::dispatch(
        &state,
        &admin_parts(
            Method::GET,
            "/admin/api/credential-cycles?from=0&to=100&provider_id=invalid",
        ),
        Bytes::new(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn cycle_post_is_a_bounded_authenticated_read_with_lightweight_defaults() {
    use serde_json::json;
    let state = state().await;
    seed_admin_key(&state).await;
    for credential_id in [7, 8] {
        for at in [10, 20, 30] {
            state
                .store
                .observe_credential_quota_cycle(&CredentialQuotaObservation {
                    credential_id,
                    window_key: "primary".into(),
                    label: None,
                    period_start: Some(0),
                    period_end: Some(100),
                    observed_at: at,
                    boundary_source: QuotaBoundarySource::Upstream,
                    boundary_confidence: QuotaBoundaryConfidence::Exact,
                    sample: gproxy_core::QuotaSample {
                        source: gproxy_core::QuotaSampleSource::Unknown,
                        started_at_ms: at * 1000,
                        received_at_ms: at * 1000,
                    },
                    scope: gproxy_core::QuotaScope::All,
                    reset_behavior: gproxy_core::QuotaResetBehavior::Periodic,
                    unit: None,
                    upstream_used: None,
                    upstream_limit: None,
                    used_percent: Some(at.into()),
                })
                .await
                .unwrap();
        }
    }
    let url = "/admin/api/credential-cycles/query";
    let response = crate::dispatch(
        &state,
        &admin_parts(Method::POST, url),
        Bytes::from(json!({"from":0,"to":100}).to_string()),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let cycles: Vec<crate::dto::CredentialQuotaCycleDto> =
        serde_json::from_slice(response.body()).unwrap();
    assert_eq!(cycles.len(), 2);
    assert!(
        cycles
            .iter()
            .all(|c| c.estimate.is_none() && c.observations.is_empty())
    );
    let id = cycles.iter().find(|c| c.credential_id == 7).unwrap().id;
    for (filter, expected) in [
        (json!({"cycle_ids":[id,id],"credential_id":7}), 1),
        (json!({"cycle_ids":[id],"credential_id":8}), 0),
        (json!({"cycle_ids":[]}), 0),
        (json!({"cycle_ids":[id],"provider_id":99999}), 0),
    ] {
        let mut body = json!({"from":15,"to":25,"include_history":true});
        body.as_object_mut()
            .unwrap()
            .extend(filter.as_object().unwrap().clone());
        let response = crate::dispatch(
            &state,
            &admin_parts(Method::POST, url),
            Bytes::from(body.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let values: Vec<crate::dto::CredentialQuotaCycleDto> =
            serde_json::from_slice(response.body()).unwrap();
        assert_eq!(values.len(), expected);
        for value in values {
            assert_eq!(value.observations.len(), 1);
            assert_eq!(value.observations[0].observed_at_ms, 20000);
            assert!(value.observations[0].estimate.is_none());
        }
    }
    for invalid in [
        json!({"from":5,"to":5}),
        json!({"from":0,"to":400*86400}),
        json!({"from":0,"to":100,"cycle_ids":[0]}),
        json!({"from":0,"to":100,"cycle_ids":vec![1;5001]}),
        json!({"from":0,"to":100,"include_estimate":"false"}),
        json!({"from":i64::MAX-1,"to":i64::MAX}),
    ] {
        let response = crate::dispatch(
            &state,
            &admin_parts(Method::POST, url),
            Bytes::from(invalid.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    let mut anonymous = admin_parts(Method::POST, url);
    anonymous.headers.remove(http::header::AUTHORIZATION);
    let response = crate::dispatch(
        &state,
        &anonymous,
        Bytes::from_static(br#"{"from":0,"to":100}"#),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(
        state.store.audit_events(100).await.unwrap().is_empty(),
        "POST query must not generate a mutation audit"
    );
}
