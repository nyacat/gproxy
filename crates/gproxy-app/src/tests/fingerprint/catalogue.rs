use bytes::Bytes;
use gproxy_admin::dto::{ProviderDto, TlsPresetDto};
use gproxy_core::ConfiguredFingerprint;
use http::{Method, StatusCode};
use serde_json::{Value, json};

#[tokio::test]
async fn admin_fingerprint_catalogue_survives_save_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let config = || {
        super::super::test_config(directory.path(), crate::MasterKeyConfig::new(None))
            .with_native_options(crate::config::NativeOptions {
                admin_user: "operator".into(),
                admin_password: Some("operator-password".into()),
                bootstrap_admin_api_key: Some("sk-fingerprint-catalogue-test".into()),
                ..Default::default()
            })
    };
    let app = crate::App::start(config()).await.unwrap();
    let response = admin(&app, Method::GET, "tls-presets", None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let presets: Vec<TlsPresetDto> = serde_json::from_slice(response.body()).unwrap();
    assert_eq!(presets.len(), 6);
    assert_eq!(
        serde_json::to_value(&presets).unwrap(),
        serde_json::to_value(gproxy_admin::State::tls_presets(&app)).unwrap()
    );

    for preset in &presets {
        let channel = app
            .inner
            .core
            .channels()
            .find(|channel| {
                channel
                    .client_fingerprint()
                    .is_some_and(|value| value.id == preset.id)
            })
            .unwrap()
            .descriptor()
            .id;
        let response = admin(
            &app,
            Method::POST,
            "providers",
            Some(json!({
                "name": format!("preset-{}", preset.id),
                "channel": channel,
                "settings": {},
                "credential_strategy": "round_robin",
                "tls_fingerprint": preset.fingerprint,
                "enabled": false,
            })),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED, "{response:?}");
    }

    // Saved presets are operator-owned snapshots; app startup must preserve
    // their complete transport options and static headers.
    drop(app);
    let app = crate::App::start(config()).await.unwrap();
    let response = admin(&app, Method::GET, "providers", None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let providers: Vec<ProviderDto> = serde_json::from_slice(response.body()).unwrap();
    assert_eq!(providers.len(), presets.len());
    let stored = app
        .inner
        .host
        .services
        .store
        .control_snapshot()
        .await
        .unwrap();

    for preset in presets {
        let provider = providers
            .iter()
            .find(|provider| provider.name == format!("preset-{}", preset.id))
            .unwrap();
        assert!(provider.tls_fingerprint_error.is_none(), "{provider:?}");
        assert!(provider.invalid_tls_fingerprint.is_none(), "{provider:?}");
        assert_eq!(provider.tls_fingerprint.as_ref(), Some(&preset.fingerprint));
        let record = stored
            .providers
            .iter()
            .find(|record| record.id == provider.id)
            .unwrap();
        let Some(ConfiguredFingerprint::Usable(configured)) =
            crate::control::fingerprint::parse(record.tls_fingerprint.as_ref())
        else {
            panic!("{} lost its saved fingerprint", preset.id);
        };
        let defaults = app
            .inner
            .core
            .channels()
            .find(|channel| channel.descriptor().id == provider.channel)
            .unwrap()
            .client_fingerprint()
            .unwrap();
        assert_eq!(
            configured.profile.as_ref(),
            Some(defaults.profile),
            "{}",
            preset.id
        );
        assert_eq!(configured.headers, defaults.headers, "{}", preset.id);
    }
}

async fn admin(
    app: &crate::AppHandle,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> http::Response<Bytes> {
    let request = http::Request::builder()
        .method(method)
        .uri(format!("/admin/api/{path}"))
        .header(
            http::header::AUTHORIZATION,
            "Bearer sk-fingerprint-catalogue-test",
        )
        .body(())
        .unwrap();
    let body = body.map_or_else(Bytes::new, |value| Bytes::from(value.to_string()));
    app.admin_dispatch(&request.into_parts().0, body)
        .await
        .unwrap()
}
