use super::*;

#[tokio::test]
async fn credential_mutation_and_import_apply_default_labels() {
    let source_directory = tempfile::tempdir().unwrap();
    let destination_directory = tempfile::tempdir().unwrap();
    let source_key = [51; 32];
    let destination_key = [73; 32];
    let source = crate::App::start(test_config(
        source_directory.path(),
        crate::MasterKeyConfig::new(Some(source_key)),
    ))
    .await
    .unwrap();
    let provider = setup::id(
        source
            .mutate(crate::ControlMutation::Provider(
                gproxy_store::records::ProviderInput {
                    name: "export-provider".into(),
                    label: None,
                    channel: "openai".into(),
                    settings: json!({}),
                    credential_strategy: "round_robin".into(),
                    proxy_url: None,
                    tls_fingerprint: None,
                    enabled: true,
                },
            ))
            .await
            .unwrap(),
    );
    let credential_id = setup::id(
        source
            .mutate(crate::ControlMutation::Credential {
                provider_id: provider,
                label: None,
                secret: json!({"api_key": "source-secret"}),
                enabled: true,
            })
            .await
            .unwrap(),
    );
    let source_credential = source
        .inner
        .host
        .services
        .store
        .credential(credential_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source_credential.label.as_deref(), Some("sourc…cret"));
    seed_admin_key(&source).await;
    let export = source
        .admin_dispatch(
            &admin_parts(http::Method::POST, "/admin/api/export"),
            Bytes::from_static(br#"{"include_secrets":true}"#),
        )
        .await
        .unwrap();
    assert_eq!(export.status(), http::StatusCode::OK);
    let mut export: gproxy_admin::dto::ConfigurationExportDto =
        serde_json::from_slice(export.body()).unwrap();
    export.data.credentials[0].config.label = None;

    let destination = crate::App::start(test_config(
        destination_directory.path(),
        crate::MasterKeyConfig::new(Some(destination_key)),
    ))
    .await
    .unwrap();
    seed_admin_key(&destination).await;
    let body = serde_json::to_vec(&gproxy_admin::dto::ConfigurationImportRequest {
        export,
        source_master_key: Some(base64::engine::general_purpose::STANDARD.encode(source_key)),
    })
    .unwrap();
    let imported = destination
        .admin_dispatch(
            &admin_parts(http::Method::POST, "/admin/api/import"),
            Bytes::from(body),
        )
        .await
        .unwrap();
    assert_eq!(imported.status(), http::StatusCode::OK);
    let credential = destination
        .inner
        .host
        .services
        .store
        .credential(1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(credential.label.as_deref(), Some("sourc…cret"));
    assert_eq!(
        destination
            .inner
            .host
            .services
            .cipher
            .open(&credential.envelope)
            .unwrap(),
        json!({"api_key": "source-secret"})
    );
    assert!(
        crate::secrets::EnvelopeCipher::new(Some(source_key))
            .open(&credential.envelope)
            .is_err()
    );
}

fn admin_parts(method: http::Method, path: &str) -> http::request::Parts {
    http::Request::builder()
        .method(method)
        .uri(path)
        .header(
            http::header::AUTHORIZATION,
            format!("Bearer {}", admin_key()),
        )
        .body(())
        .unwrap()
        .into_parts()
        .0
}

async fn seed_admin_key(app: &crate::AppHandle) {
    let store = &app.inner.host.services.store;
    let id = gproxy_admin::seed_first_admin(store, "transfer-admin", &setup::random_key())
        .await
        .unwrap()
        .unwrap();
    store
        .insert_user_key(&gproxy_store::records::UserKeyInput {
            user_id: id,
            digest: Sha256::digest(admin_key().as_bytes()).to_vec(),
            digest_version: crate::control::USER_KEY_DIGEST_VERSION,
            prefix: "transfer-adm".into(),
            envelope: app
                .inner
                .host
                .services
                .cipher
                .seal_user_key(&json!(admin_key()))
                .unwrap(),
            label: None,
            expires_at: None,
            enabled: true,
        })
        .await
        .unwrap();
}

fn admin_key() -> &'static str {
    static KEY: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    KEY.get_or_init(setup::random_key)
}

async fn transfer_app() -> (tempfile::TempDir, crate::AppHandle) {
    let directory = tempfile::tempdir().unwrap();
    let app = crate::App::start(test_config(
        directory.path(),
        crate::MasterKeyConfig::new(Some([51; 32])),
    ))
    .await
    .unwrap();
    seed_admin_key(&app).await;
    (directory, app)
}

async fn transfer_export(
    source: &crate::AppHandle,
) -> gproxy_admin::dto::ConfigurationImportRequest {
    source
        .mutate(crate::ControlMutation::Provider(
            gproxy_store::records::ProviderInput {
                name: "incoming-provider".into(),
                label: None,
                channel: "openai".into(),
                settings: json!({}),
                credential_strategy: "round_robin".into(),
                proxy_url: None,
                tls_fingerprint: None,
                enabled: true,
            },
        ))
        .await
        .unwrap();
    source
        .mutate(crate::ControlMutation::Credential {
            provider_id: 1,
            label: None,
            secret: json!({"api_key": "transfer-secret"}),
            enabled: true,
        })
        .await
        .unwrap();
    source
        .mutate(crate::ControlMutation::Organization(
            gproxy_store::records::OrganizationInput {
                name: "incoming-organization".into(),
                enabled: true,
            },
        ))
        .await
        .unwrap();
    exported_request(source).await
}

async fn exported_request(
    source: &crate::AppHandle,
) -> gproxy_admin::dto::ConfigurationImportRequest {
    let response = source
        .admin_dispatch(
            &admin_parts(http::Method::POST, "/admin/api/export"),
            Bytes::from_static(br#"{"include_secrets":true}"#),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    gproxy_admin::dto::ConfigurationImportRequest {
        export: serde_json::from_slice(response.body()).unwrap(),
        source_master_key: Some(base64::engine::general_purpose::STANDARD.encode([51; 32])),
    }
}

async fn import_request(
    destination: &crate::AppHandle,
    request: &gproxy_admin::dto::ConfigurationImportRequest,
) -> http::Response<Bytes> {
    destination
        .admin_dispatch(
            &admin_parts(http::Method::POST, "/admin/api/import"),
            Bytes::from(serde_json::to_vec(request).unwrap()),
        )
        .await
        .unwrap()
}

async fn assert_preflight_failure(
    destination: &crate::AppHandle,
    request: &gproxy_admin::dto::ConfigurationImportRequest,
) {
    let services = &destination.inner.host.services;
    let before = services.store.control_snapshot().await.unwrap();
    let before_version = crate::invalidation::current(&services.cache).await.unwrap();
    assert!(!services.control.has_named_target("incoming-provider"));
    let response = import_request(destination, request).await;
    assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
    assert!(
        services.store.control_snapshot().await.unwrap() == before,
        "failed import changed persisted configuration"
    );
    assert_eq!(
        crate::invalidation::current(&services.cache).await.unwrap(),
        before_version
    );
    assert!(!services.control.has_named_target("incoming-provider"));
}

#[tokio::test]
async fn import_wrong_source_key_preserves_existing_configuration_and_can_retry() {
    let (_source_directory, source) = transfer_app().await;
    let (_destination_directory, destination) = transfer_app().await;
    let request = transfer_export(&source).await;
    let mut invalid = request.clone();
    invalid.source_master_key = Some(base64::engine::general_purpose::STANDARD.encode([52; 32]));
    assert_preflight_failure(&destination, &invalid).await;

    let before_version = crate::invalidation::current(&destination.inner.host.services.cache)
        .await
        .unwrap();
    let response = import_request(&destination, &request).await;
    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(
        crate::invalidation::current(&destination.inner.host.services.cache)
            .await
            .unwrap(),
        before_version + 1,
        "one completed import publishes exactly one new snapshot"
    );
    assert!(
        destination
            .inner
            .host
            .services
            .control
            .has_named_target("incoming-provider")
    );
}

#[tokio::test]
async fn import_late_bad_reference_preserves_existing_configuration_and_can_retry() {
    let (_source_directory, source) = transfer_app().await;
    let (_destination_directory, destination) = transfer_app().await;
    let request = transfer_export(&source).await;
    let mut invalid = request.clone();
    invalid
        .export
        .data
        .provider_rule_sets
        .last_mut()
        .unwrap()
        .rule_set_id = i64::MAX;
    assert_preflight_failure(&destination, &invalid).await;
    assert_eq!(
        import_request(&destination, &request).await.status(),
        http::StatusCode::OK
    );
}

#[tokio::test]
async fn import_checks_late_values_duplicate_ids_and_every_secret_before_writing() {
    let (_source_directory, source) = transfer_app().await;
    let (_destination_directory, destination) = transfer_app().await;
    let request = transfer_export(&source).await;

    let mut duplicate = request.clone();
    duplicate
        .export
        .data
        .providers
        .push(duplicate.export.data.providers[0].clone());
    assert_preflight_failure(&destination, &duplicate).await;

    let mut corrupt_credential = request.clone();
    let mut second_credential = corrupt_credential.export.data.credentials[0].clone();
    second_credential.config.id += 100;
    *second_credential
        .secret
        .as_mut()
        .unwrap()
        .ciphertext
        .last_mut()
        .unwrap() ^= 1;
    corrupt_credential
        .export
        .data
        .credentials
        .push(second_credential);
    assert_preflight_failure(&destination, &corrupt_credential).await;

    let mut corrupt_user_key = request.clone();
    *corrupt_user_key.export.data.user_keys[0]
        .secret
        .as_mut()
        .unwrap()
        .ciphertext
        .last_mut()
        .unwrap() ^= 1;
    assert_preflight_failure(&destination, &corrupt_user_key).await;

    let mut invalid_price = request.clone();
    invalid_price
        .export
        .data
        .price_rules
        .push(gproxy_admin::dto::PriceRuleDto {
            id: 9_000,
            provider_id: Some(1),
            model_pattern: "*".into(),
            tiers: None,
            priority: 0,
            enabled: true,
        });
    invalid_price
        .export
        .data
        .price_rates
        .push(gproxy_admin::dto::PriceRateDto {
            id: 9_001,
            rule_id: 9_000,
            metric: "input_tokens".into(),
            unit_size: 1_000_000,
            price: "1".into(),
            conditions: Some(json!({"service_tier": []})),
            priority: 0,
        });
    assert_preflight_failure(&destination, &invalid_price).await;
    invalid_price.export.data.price_rates[0].conditions = None;
    invalid_price.export.data.price_rates[0].unit_size = u64::MAX;
    assert_preflight_failure(&destination, &invalid_price).await;
    invalid_price.export.data.price_rates[0].unit_size = 1_000_000;
    invalid_price.export.data.price_rates[0].price = rust_decimal::Decimal::MAX.to_string();
    assert_preflight_failure(&destination, &invalid_price).await;

    assert_eq!(
        import_request(&destination, &request).await.status(),
        http::StatusCode::OK
    );
}

#[tokio::test]
async fn import_checks_decrypted_credential_shape_before_writing() {
    let (_source_directory, source) = transfer_app().await;
    let (_destination_directory, destination) = transfer_app().await;
    let mut request = transfer_export(&source).await;
    request.export.data.credentials[0].secret = Some(
        source
            .inner
            .host
            .services
            .cipher
            .seal(&json!(["not a credential object"]))
            .unwrap()
            .into(),
    );
    assert_preflight_failure(&destination, &request).await;
}

#[tokio::test]
async fn import_reseals_primary_and_quota_authorization_together() {
    let (_source_directory, source) = transfer_app().await;
    let (_destination_directory, destination) = transfer_app().await;
    let provider_id = setup::id(
        source
            .mutate(crate::ControlMutation::Provider(
                gproxy_store::records::ProviderInput {
                    name: "quota-transfer-provider".into(),
                    label: None,
                    channel: "openrouter".into(),
                    settings: json!({}),
                    credential_strategy: "round_robin".into(),
                    proxy_url: None,
                    tls_fingerprint: None,
                    enabled: true,
                },
            ))
            .await
            .unwrap(),
    );
    let response = source
        .admin_dispatch(
            &admin_parts(http::Method::POST, "/admin/api/credentials"),
            Bytes::from(
                json!({
                    "provider_id": provider_id,
                    "kind": "api_key",
                    "secret": {"api_key": "primary-inference-key"},
                    "quota_secret": {"quota_api_key": "management-query-key"},
                    "enabled": true,
                    "weight": 100
                })
                .to_string(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        http::StatusCode::CREATED,
        "{:?}",
        response.body()
    );
    let request = exported_request(&source).await;
    let response = import_request(&destination, &request).await;
    assert_eq!(
        response.status(),
        http::StatusCode::OK,
        "{:?}",
        response.body()
    );
    let snapshot = destination
        .inner
        .host
        .services
        .store
        .control_snapshot()
        .await
        .unwrap();
    let credential = snapshot.credentials.first().unwrap();
    let secret = gproxy_admin::State::reveal_credential_secret(&destination, credential.id)
        .await
        .unwrap();
    assert_eq!(
        secret,
        json!({
            "api_key": "primary-inference-key",
            "quota_api_key": "management-query-key",
            "quota_channel": "openrouter"
        })
    );
    let quota = gproxy_admin::State::credential_quota_snapshot(&destination, credential.id)
        .await
        .unwrap();
    assert!(
        quota
            .sources
            .iter()
            .any(|source| { source.capability.support == gproxy_channel_api::QuotaSupport::Ready })
    );
}

#[tokio::test]
async fn import_default_rule_set_keeps_source_configuration_with_remapped_owner() {
    let (_source_directory, source) = transfer_app().await;
    let (_destination_directory, destination) = transfer_app().await;
    let mut request = transfer_export(&source).await;
    let source_default = request
        .export
        .data
        .rule_sets
        .iter_mut()
        .find(|value| value.description.as_deref() == Some("gproxy:provider-default:1"))
        .unwrap();
    source_default.name = "renamed-source-defaults".into();
    source_default.enabled = false;
    destination
        .mutate(crate::ControlMutation::Provider(
            gproxy_store::records::ProviderInput {
                name: "existing-provider".into(),
                label: None,
                channel: "openai".into(),
                settings: json!({}),
                credential_strategy: "round_robin".into(),
                proxy_url: None,
                tls_fingerprint: None,
                enabled: true,
            },
        ))
        .await
        .unwrap();
    let before = destination
        .inner
        .host
        .services
        .store
        .control_snapshot()
        .await
        .unwrap();
    let response = import_request(&destination, &request).await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let after = destination
        .inner
        .host
        .services
        .store
        .control_snapshot()
        .await
        .unwrap();
    assert_eq!(after.providers[0], before.providers[0]);
    assert_eq!(after.rule_sets[0], before.rule_sets[0]);
    assert_eq!(after.users, before.users);
    assert_eq!(after.user_keys, before.user_keys);
    let incoming = after
        .providers
        .iter()
        .find(|provider| provider.name == "incoming-provider")
        .unwrap();
    assert_ne!(incoming.id, 1);
    let description = format!("gproxy:provider-default:{}", incoming.id);
    let sets = after
        .rule_sets
        .iter()
        .filter(|value| value.description.as_deref() == Some(description.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(sets.len(), 1);
    assert_eq!(sets[0].name, "renamed-source-defaults");
    assert!(!sets[0].enabled);
    assert_eq!(
        after
            .provider_rule_sets
            .iter()
            .filter(|attachment| {
                attachment.provider_id == incoming.id && attachment.rule_set_id == sets[0].id
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn config_only_import_skips_omitted_secrets_and_their_quotas() {
    let (_source_directory, source) = transfer_app().await;
    let (_destination_directory, destination) = transfer_app().await;
    transfer_export(&source).await;
    let key_id = setup::id(
        source
            .mutate(crate::ControlMutation::UserKey {
                user_id: 1,
                api_key: setup::random_key(),
                label: None,
                expires_at: None,
                enabled: true,
            })
            .await
            .unwrap(),
    );
    let mut request = exported_request(&source).await;
    request.export.secrets = gproxy_admin::dto::SecretExportDto::Omitted;
    request.export.source_key = None;
    request.source_master_key = None;
    for credential in &mut request.export.data.credentials {
        credential.secret = None;
    }
    for key in &mut request.export.data.user_keys {
        key.secret = None;
    }
    for (id, kind, subject_id) in [(1, "credential", 1), (2, "user_key", key_id)] {
        request
            .export
            .data
            .quotas
            .push(gproxy_admin::dto::QuotaDto {
                id,
                subject_kind: kind.into(),
                subject_id,
                quota_total: Some("1".into()),
                quota_daily: None,
                quota_weekly: None,
                quota_monthly: None,
                quota_5h: None,
                quota_7d: None,
                enabled: true,
            });
    }
    let response = import_request(&destination, &request).await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let outcome: gproxy_admin::dto::ConfigurationImportResponse =
        serde_json::from_slice(response.body()).unwrap();
    assert_eq!(outcome.skipped_credentials, 1);
    assert_eq!(outcome.skipped_user_keys, 1);
    let stored = destination
        .inner
        .host
        .services
        .store
        .control_snapshot()
        .await
        .unwrap();
    assert!(stored.credentials.is_empty());
    assert!(stored.quotas.is_empty());
    assert_eq!(stored.user_keys.len(), 1);
}

#[tokio::test]
async fn export_omits_oauth_key_quotas_and_imports_persistent_key_quotas() {
    let (_source_directory, source) = transfer_app().await;
    let (_destination_directory, destination) = transfer_app().await;
    transfer_export(&source).await;
    let mut key_ids = Vec::new();
    for label in ["session key", "persistent key"] {
        let key_id = setup::id(
            source
                .mutate(crate::ControlMutation::UserKey {
                    user_id: 1,
                    api_key: setup::random_key(),
                    label: Some(label.into()),
                    expires_at: None,
                    enabled: true,
                })
                .await
                .unwrap(),
        );
        source
            .inner
            .host
            .services
            .store
            .insert_quota(&gproxy_store::records::QuotaInput {
                subject_kind: "user_key".into(),
                subject_id: key_id,
                quota_total: Some(rust_decimal::Decimal::ONE),
                quota_daily: None,
                quota_weekly: None,
                quota_monthly: None,
                quota_5h: None,
                quota_7d: None,
                enabled: true,
            })
            .await
            .unwrap();
        key_ids.push(key_id);
    }
    source
        .inner
        .host
        .services
        .store
        .insert_oauth_grant(&gproxy_store::records::OAuthGrantInput {
            user_id: 1,
            user_key_id: key_ids[0],
            provider_id: None,
            client_id: gproxy_channel_api::CODEX_OAUTH_CLIENT_ID.into(),
            scopes: "openid profile offline_access".into(),
            chatgpt_user_id: "transfer-user".into(),
            chatgpt_account_id: "transfer-account".into(),
            created_at: 1,
        })
        .await
        .unwrap();

    let request = exported_request(&source).await;
    assert_eq!(
        import_request(&destination, &request).await.status(),
        http::StatusCode::OK,
        "the server's own export must not reference omitted session keys"
    );
    assert!(
        !request
            .export
            .data
            .user_keys
            .iter()
            .any(|key| key.config.id == key_ids[0])
    );
    assert_eq!(request.export.data.quotas.len(), 1);
    assert_eq!(request.export.data.quotas[0].subject_id, key_ids[1]);
    let imported = destination
        .inner
        .host
        .services
        .store
        .control_snapshot()
        .await
        .unwrap();
    let key = imported
        .user_keys
        .iter()
        .find(|key| key.label.as_deref() == Some("persistent key"))
        .unwrap();
    assert_eq!(imported.quotas.len(), 1);
    assert_eq!(imported.quotas[0].subject_id, key.id);
}

#[tokio::test]
async fn import_seeded_routing_defaults_preserves_operator_overrides_and_origins() {
    let (_source_directory, source) = transfer_app().await;
    let (_destination_directory, destination) = transfer_app().await;
    transfer_export(&source).await;
    let store = &source.inner.host.services.store;
    let channel = gproxy_admin::State::channel_catalogue(&source)
        .into_iter()
        .find(|channel| channel.id == "openai")
        .unwrap();
    gproxy_admin::seed_provider_defaults(store, 1, "incoming-provider", &channel)
        .await
        .unwrap();
    let original = store
        .control_snapshot()
        .await
        .unwrap()
        .routing_rules
        .remove(0);
    store
        .update_routing_rule(
            original.id,
            &gproxy_store::records::RoutingRuleInput {
                provider_id: original.provider_id,
                operation: original.operation.clone(),
                kind: original.kind.clone(),
                implementation: "unsupported".into(),
                dest_operation: None,
                dest_kind: None,
                sort_order: original.sort_order,
                enabled: true,
            },
        )
        .await
        .unwrap();
    let request = exported_request(&source).await;
    assert!(
        request
            .export
            .data
            .routing_rules
            .iter()
            .any(|rule| rule.inherited)
    );
    let response = import_request(&destination, &request).await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let rules = destination
        .inner
        .host
        .services
        .store
        .control_snapshot()
        .await
        .unwrap()
        .routing_rules;
    assert_eq!(rules.len(), request.export.data.routing_rules.len());
    let override_rule = rules
        .iter()
        .find(|rule| rule.operation == original.operation && rule.kind == original.kind)
        .unwrap();
    assert_eq!(override_rule.implementation, "unsupported");
    assert_eq!(override_rule.origin, "operator");
    assert!(rules.iter().any(|rule| rule.origin == "channel_default"));
}
