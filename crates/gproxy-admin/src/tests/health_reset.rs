use super::*;
use gproxy_store::records::{CredentialHealthInput, CredentialHealthState, CredentialInput};

async fn seed_health(state: &TestState) -> [i64; 2] {
    let provider_id = provider(state).await;
    let mut ids = [0; 2];
    for id in &mut ids {
        *id = state
            .store
            .insert_credential(&CredentialInput {
                provider_id,
                label: None,
                kind: "api_key".into(),
                envelope: envelope(),
                enabled: true,
                weight: 100,
                rpm_limit: None,
                tpm_limit: None,
                proxy_url: None,
                tls_fingerprint: None,
            })
            .await
            .unwrap();
        for model in ["model-a", "model-b", "*", ""] {
            state
                .store
                .record_credential_health(&CredentialHealthInput {
                    credential_id: *id,
                    model: model.into(),
                    credential_version: 0,
                    version: 1,
                    state: CredentialHealthState::Degraded,
                    observed_at: 100,
                    response_status: Some(503),
                    detail: Some("server_is_overloaded".into()),
                })
                .await
                .unwrap();
        }
    }
    ids
}

#[tokio::test]
async fn credential_health_reset_isolates_models_and_preserves_full_reset() {
    let state = state().await;
    seed_admin_key(&state).await;
    let [id, other_id] = seed_health(&state).await;
    let path = format!("/admin/api/credentials/{id}/health-reset");
    let mut expected = state.store.credential_health().await.unwrap();

    for model in ["model-a", "*", ""] {
        let response = crate::dispatch(
            &state,
            &admin_parts(Method::POST, &path),
            Bytes::from(serde_json::to_vec(&serde_json::json!({ "model": model })).unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        expected.retain(|entry| entry.credential_id != id || entry.model != model);
        assert_eq!(state.store.credential_health().await.unwrap(), expected);
    }

    let response = crate::dispatch(
        &state,
        &admin_parts(Method::POST, &path),
        Bytes::from_static(b"{}"),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    expected.retain(|entry| entry.credential_id != id);
    assert_eq!(state.store.credential_health().await.unwrap(), expected);
    let audit = state.store.audit_events(1).await.unwrap();
    assert_eq!(audit[0].event.action, "credential.health_reset");
    assert_eq!(audit[0].event.target_id, Some(id));

    let response = crate::dispatch(
        &state,
        &admin_parts(
            Method::POST,
            &format!("/admin/api/credentials/{other_id}/health-reset"),
        ),
        Bytes::new(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(state.store.credential_health().await.unwrap().is_empty());
}

#[tokio::test]
async fn credential_health_reset_rejects_invalid_requests_without_clearing_health() {
    let state = state().await;
    seed_admin_key(&state).await;
    let [id, _] = seed_health(&state).await;
    let path = format!("/admin/api/credentials/{id}/health-reset");
    let expected = state.store.credential_health().await.unwrap();

    for body in [
        r#"{"mode":"model-a"}"#,
        r#"{"model":"model-a","unexpected":true}"#,
        r#"{"model":null}"#,
        r#"{"model":123}"#,
        r#"{"model":false}"#,
        "null",
        "[]",
        "{",
    ] {
        let response = crate::dispatch(
            &state,
            &admin_parts(Method::POST, &path),
            Bytes::from_static(body.as_bytes()),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(state.store.credential_health().await.unwrap(), expected);
    }

    for (method, path) in [
        (Method::GET, path.clone()),
        (Method::DELETE, path.clone()),
        (Method::POST, format!("{path}/model-a")),
        (
            Method::POST,
            "/admin/api/credentials/no-id/health-reset".into(),
        ),
    ] {
        let response = crate::dispatch(
            &state,
            &admin_parts(method, &path),
            Bytes::from_static(b"{}"),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(state.store.credential_health().await.unwrap(), expected);
    }
}
