use gproxy_core::{ConfiguredFingerprint, ControlPlane, RoutingMode};
use serde_json::json;

mod catalogue;
mod pooling;

#[tokio::test]
async fn native_transport_starts_tls_with_exported_fingerprints() {
    use std::io::ErrorKind;
    use std::time::Duration;

    use gproxy_core::UpstreamTransport;

    let fixture = super::setup::fixture().await;
    let transport = gproxy_upstream::WreqTransport::new();
    for preset in gproxy_admin::State::tls_presets(&fixture.app) {
        let value = serde_json::to_value(&preset.fingerprint).unwrap();
        let Some(ConfiguredFingerprint::Usable(configured)) =
            crate::control::fingerprint::parse(Some(&value))
        else {
            panic!("invalid preset {}", preset.id);
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut request =
            http::Request::get(format!("https://{}/", listener.local_addr().unwrap()))
                .body(bytes::Bytes::new())
                .unwrap();
        request.extensions_mut().insert(configured.profile.unwrap());
        let server = async {
            let (socket, _) = listener.accept().await.unwrap();
            let mut record_header = [0_u8; 5];
            let mut received = 0;
            while received < record_header.len() {
                socket.readable().await.unwrap();
                match socket.try_read(&mut record_header[received..]) {
                    Ok(0) => panic!("{} closed before sending TLS", preset.id),
                    Ok(count) => received += count,
                    Err(error) if error.kind() == ErrorKind::WouldBlock => continue,
                    Err(error) => panic!("{}: {error}", preset.id),
                }
            }
            assert_eq!(
                &record_header[..2],
                &[22, 3],
                "{} did not send TLS",
                preset.id
            );
            // Drop the peer before any certificate or HTTP exchange. Reaching
            // this point proves wreq accepted the exported cipher/signature
            // configuration, rather than failing while building its connector.
        };
        let (_, response) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(server, transport.send(request))
        })
        .await
        .unwrap_or_else(|_| panic!("{} did not start TLS", preset.id));
        assert!(
            response.is_err(),
            "the local peer deliberately closes during TLS"
        );
    }
}

#[tokio::test]
async fn exported_tls_presets_round_trip_registered_channel_defaults() {
    let fixture = super::setup::fixture().await;
    let app = &fixture.app;
    let presets = gproxy_admin::State::tls_presets(app);
    let mut ids = presets
        .iter()
        .map(|preset| preset.id.as_str())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(
        ids,
        [
            "antigravity",
            "claude",
            "codex",
            "copilot",
            "gemini",
            "kiro"
        ]
    );

    for channel in app.inner.core.channels() {
        let Some(defaults) = channel.client_fingerprint() else {
            continue;
        };
        let preset = presets
            .iter()
            .find(|preset| preset.id == defaults.id)
            .unwrap();
        // Exercise the public JSON representation and the actual configuration
        // parser; losing cipher suites or optional settings used to happen here.
        let value = serde_json::to_value(&preset.fingerprint).unwrap();
        let Some(ConfiguredFingerprint::Usable(configured)) =
            crate::control::fingerprint::parse(Some(&value))
        else {
            panic!("{} cannot be loaded as a configured fingerprint", preset.id);
        };
        assert_eq!(
            configured.profile.as_ref(),
            Some(defaults.profile),
            "{}",
            preset.id
        );
        assert_eq!(configured.headers, defaults.headers, "{}", preset.id);
        for forbidden in [
            "authorization",
            "x-api-key",
            "session-id",
            "x-client-request-id",
            "x-claude-code-session-id",
            "chatgpt-account-id",
        ] {
            assert!(
                !configured.headers.contains_key(forbidden),
                "{} exports {forbidden}",
                preset.id
            );
        }
    }
}

#[tokio::test]
async fn explicit_fingerprints_need_no_instance_gate_and_credential_wins() {
    let super::setup::Fixture {
        app,
        provider,
        credential,
        ..
    } = super::setup::fixture().await;
    let store = &app.inner.host.services.store;
    store
        .update_provider(
            provider,
            &gproxy_store::records::ProviderInput {
                name: "provider".into(),
                label: None,
                channel: "openai".into(),
                settings: json!({}),
                credential_strategy: "round_robin".into(),
                proxy_url: None,
                tls_fingerprint: Some(json!({"headers": {"x-fingerprint-owner": "provider"}})),
                enabled: true,
            },
        )
        .await
        .unwrap();
    app.reload().await.unwrap();
    assert_eq!(fingerprint_owner(&app), "provider");

    let stored = store
        .admin_credentials()
        .await
        .unwrap()
        .into_iter()
        .find(|value| value.id == credential)
        .unwrap();
    store
        .update_credential(
            credential,
            &gproxy_store::records::CredentialUpdateInput {
                provider_id: provider,
                label: stored.label,
                kind: stored.kind,
                envelope: None,
                enabled: stored.enabled,
                weight: stored.weight,
                rpm_limit: stored.rpm_limit,
                tpm_limit: stored.tpm_limit,
                proxy_url: stored.proxy_url,
                tls_fingerprint: Some(json!({"headers": {"x-fingerprint-owner": "credential"}})),
            },
        )
        .await
        .unwrap();
    app.reload().await.unwrap();
    assert_eq!(fingerprint_owner(&app), "credential");
}

fn fingerprint_owner(app: &crate::AppHandle) -> String {
    let plan = app
        .inner
        .host
        .services
        .control
        .resolve(Some("public-model"), &RoutingMode::Aggregated, None)
        .unwrap();
    let Some(ConfiguredFingerprint::Usable(fingerprint)) = &plan.targets[0].provider.fingerprint
    else {
        panic!("explicit fingerprint was not compiled")
    };
    fingerprint.headers["x-fingerprint-owner"]
        .to_str()
        .unwrap()
        .to_owned()
}
