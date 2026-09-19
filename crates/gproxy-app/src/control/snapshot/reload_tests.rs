use gproxy_core::{ControlPlane, CredentialId, CredentialStore, RoutingMode};
use tokio::sync::oneshot;

pub(super) struct Pause {
    pub read: oneshot::Sender<()>,
    pub resume: oneshot::Receiver<()>,
}

#[tokio::test]
async fn an_older_reload_cannot_overwrite_a_completed_mutation() {
    let fixture = crate::tests::setup::fixture().await;
    let app = &fixture.app;
    app.shutdown();
    app.drain_background().await;
    let control = &app.inner.host.services.control;
    let (read, read_done) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    *control.reload_pause.lock().unwrap() = Some(Pause {
        read,
        resume: resumed,
    });
    let old_control = control.clone();
    let old_reload = tokio::spawn(async move { old_control.reload().await });
    read_done.await.unwrap();
    app.mutate(crate::ControlMutation::ExposedModel(
        gproxy_store::records::ExposedModelInput {
            name: "new-exact-route".into(),
            route_id: fixture.route,
            enabled: true,
        },
    ))
    .await
    .unwrap();
    assert!(
        control
            .resolve(Some("new-exact-route"), &RoutingMode::Aggregated, None)
            .is_ok()
    );
    resume.send(()).unwrap();
    old_reload.await.unwrap().unwrap();
    let plan = control
        .resolve(Some("new-exact-route"), &RoutingMode::Aggregated, None)
        .unwrap();
    assert_eq!(plan.targets[0].upstream_model, "upstream-model");
}

#[tokio::test]
async fn credential_load_rechecks_a_read_that_finished_after_reload() {
    let fixture = crate::tests::setup::fixture().await;
    fixture.app.shutdown();
    fixture.app.drain_background().await;
    let host = &fixture.app.inner.host;
    let control = &host.services.control;
    let id = CredentialId(fixture.credential);
    let old_version = host
        .services
        .store
        .credential(id.0)
        .await
        .unwrap()
        .unwrap()
        .version;
    let (read, read_done) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    *control.credential_read_pause.lock().unwrap() = Some(Pause {
        read,
        resume: resumed,
    });
    let reading_host = host.clone();
    let loading = tokio::spawn(async move { reading_host.load(id).await });
    read_done.await.unwrap();
    let replacement = serde_json::json!({"api_key": "replacement"});
    let envelope = host.services.cipher.seal(&replacement).unwrap();
    host.services
        .store
        .persist_credential_rotation(id.0, &envelope, old_version)
        .await
        .unwrap();
    control.reload().await.unwrap();
    resume.send(()).unwrap();
    let record = loading.await.unwrap().unwrap();
    assert_eq!(record.version, old_version + 1);
    assert_eq!(record.secret, replacement);
    assert_eq!(host.load(id).await.unwrap().secret, replacement);
}

#[tokio::test]
async fn cancelled_rotation_invalidates_a_concurrent_old_cache_fill() {
    let fixture = crate::tests::setup::fixture().await;
    fixture.app.shutdown();
    fixture.app.drain_background().await;
    let host = &fixture.app.inner.host;
    let control = &host.services.control;
    let id = CredentialId(fixture.credential);
    let original = host.load(id).await.unwrap();
    let invalidation = crate::invalidation::current(&host.services.cache)
        .await
        .unwrap();
    let replacement = serde_json::json!({"api_key": "rotated"});
    let (read, read_done) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    *control.credential_rotation_pause.lock().unwrap() = Some(Pause {
        read,
        resume: resumed,
    });
    let rotating_host = host.clone();
    let secret = replacement.clone();
    let rotating = tokio::spawn(async move {
        rotating_host
            .persist_rotation(id, secret, original.version)
            .await
    });
    read_done.await.unwrap();
    assert_eq!(host.load(id).await.unwrap().version, original.version);
    rotating.abort();
    assert!(rotating.await.unwrap_err().is_cancelled());
    resume.send(()).unwrap();
    fixture.app.drain_background().await;
    let record = host.load(id).await.unwrap();
    assert_eq!(record.version, original.version + 1);
    assert_eq!(record.secret, replacement);
    assert_eq!(control.known_credential_version(id.0), Some(record.version));
    assert!(
        crate::invalidation::current(&host.services.cache)
            .await
            .unwrap()
            > invalidation
    );
}

#[tokio::test]
async fn authoritative_credential_load_observes_peer_rotation_and_revocation_without_polling() {
    let fixture = crate::tests::setup::fixture().await;
    fixture.app.shutdown();
    fixture.app.drain_background().await;
    let host = &fixture.app.inner.host;
    let id = CredentialId(fixture.credential);
    let cached = host.load(id).await.unwrap();
    // A different instance can update the shared database while this one has
    // no invalidation poll (as on Edge during an active refresh wait).
    let replacement = serde_json::json!({"api_key": "peer-token"});
    let envelope = host.services.cipher.seal(&replacement).unwrap();
    host.services
        .store
        .persist_credential_rotation(id.0, &envelope, cached.version)
        .await
        .unwrap();
    assert_eq!(host.load(id).await.unwrap().version, cached.version);
    let current = host.load_current(id).await.unwrap();
    assert_eq!(current.version, cached.version + 1);
    assert_eq!(current.secret, replacement);
    assert_eq!(host.load(id).await.unwrap().secret, replacement);
    assert_eq!(
        host.services.control.known_credential_version(id.0),
        Some(current.version)
    );
    host.services.store.delete_credential(id.0).await.unwrap();
    assert!(host.load_current(id).await.is_err());
    assert!(host.load(id).await.is_err());
}

#[tokio::test]
async fn rotation_retry_preserves_a_concurrent_quota_authorization_deletion() {
    let fixture = crate::tests::setup::fixture().await;
    fixture.app.shutdown();
    fixture.app.drain_background().await;
    let host = &fixture.app.inner.host;
    let id = CredentialId(fixture.credential);
    let original = host.load_current(id).await.unwrap();
    let mut secret = original.secret.clone();
    secret["quota_api_key"] = serde_json::json!("removed-quota-authorization");
    secret["quota_organization_id"] = serde_json::json!("old-organization");
    host.services
        .store
        .persist_credential_rotation(
            id.0,
            &host.services.cipher.seal(&secret).unwrap(),
            original.version,
        )
        .await
        .unwrap();
    let refreshing = host.load_current(id).await.unwrap();
    let mut replacement = refreshing.secret.clone();
    replacement["api_key"] = serde_json::json!("new-inference-authorization");

    secret.as_object_mut().unwrap().remove("quota_api_key");
    secret["quota_organization_id"] = serde_json::json!("new-organization");
    let metadata = host
        .services
        .control
        .current()
        .credentials
        .iter()
        .find(|credential| credential.id == id.0)
        .unwrap()
        .clone();
    assert!(
        host.services
            .store
            .update_credential_version(
                id.0,
                &gproxy_store::records::CredentialUpdateInput {
                    provider_id: fixture.provider,
                    label: Some("edited during refresh".into()),
                    kind: refreshing.kind.clone(),
                    envelope: Some(host.services.cipher.seal(&secret).unwrap()),
                    enabled: true,
                    weight: metadata.weight,
                    rpm_limit: metadata.rpm_limit,
                    tpm_limit: metadata.tpm_limit,
                    proxy_url: metadata.proxy_url,
                    tls_fingerprint: metadata.tls_fingerprint,
                },
                refreshing.version,
                true,
            )
            .await
            .unwrap()
    );
    assert!(
        host.persist_rotation(id, replacement.clone(), refreshing.version)
            .await
            .is_err()
    );
    let edited = host.load_current(id).await.unwrap();
    host.persist_rotation(id, replacement, edited.version)
        .await
        .unwrap();
    let rotated = host.load_current(id).await.unwrap();
    assert_eq!(rotated.version, refreshing.version + 2);
    assert_eq!(rotated.secret["api_key"], "new-inference-authorization");
    assert!(rotated.secret.get("quota_api_key").is_none());
    assert_eq!(rotated.secret["quota_organization_id"], "new-organization");
    assert_eq!(
        host.services
            .store
            .credential(id.0)
            .await
            .unwrap()
            .unwrap()
            .label
            .as_deref(),
        Some("edited during refresh")
    );
}
