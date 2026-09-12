use super::*;
use bytes::Bytes;
use gproxy_admin::State;
use gproxy_store::records::{CredentialInput, CredentialUpdateInput, ProviderInput};
use http::{Method, StatusCode};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ADMIN_KEY: &str = "sk-manual-refresh-test-admin-key";

struct Fixture {
    app: AppHandle,
    provider: i64,
    credential: i64,
    _directory: tempfile::TempDir,
}

async fn fixture(token_url: &str) -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let app = crate::App::start(
        crate::Config::sqlite(
            "127.0.0.1:0".parse().unwrap(),
            directory.path().into(),
            crate::MasterKeyConfig::new(Some([53; 32])),
        )
        .with_native_options(crate::config::NativeOptions {
            admin_user: "refresh-operator".into(),
            admin_password: Some("refresh-operator-password".into()),
            bootstrap_admin_api_key: Some(ADMIN_KEY.into()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    app.shutdown();
    app.drain_background().await;
    let provider = app
        .store()
        .insert_provider(&ProviderInput {
            name: "manual-oauth-refresh".into(),
            label: None,
            channel: "geminicli".into(),
            settings: json!({"oauth_token_url": token_url}),
            credential_strategy: "round_robin".into(),
            proxy_url: None,
            tls_fingerprint: None,
            enabled: true,
        })
        .await
        .unwrap();
    let credential = insert(
        &app,
        provider,
        "oauth",
        json!({
            "access_token": "initial-access-private",
            "refresh_token": "initial-refresh-private",
            "project_id": "refresh-project",
            "expires_at_ms": i64::MAX,
            "quota_management_key": "independent-quota-private"
        }),
        true,
    )
    .await;
    app.reload().await.unwrap();
    Fixture {
        app,
        provider,
        credential,
        _directory: directory,
    }
}

async fn insert(app: &AppHandle, provider: i64, kind: &str, secret: Value, enabled: bool) -> i64 {
    app.store()
        .insert_credential(&CredentialInput {
            provider_id: provider,
            label: None,
            kind: kind.into(),
            envelope: app.seal_credential(&secret).unwrap(),
            enabled,
            weight: 100,
            rpm_limit: None,
            tpm_limit: None,
            proxy_url: None,
            tls_fingerprint: None,
        })
        .await
        .unwrap()
}

async fn request(
    app: &AppHandle,
    method: Method,
    path: &str,
    body: &str,
    authenticated: bool,
) -> http::Response<Bytes> {
    let mut request = http::Request::builder().method(method).uri(path);
    if authenticated {
        request = request.header(http::header::AUTHORIZATION, format!("Bearer {ADMIN_KEY}"));
    }
    let parts = request.body(()).unwrap().into_parts().0;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        app.admin_dispatch(&parts, Bytes::copy_from_slice(body.as_bytes())),
    )
    .await
    .unwrap()
    .unwrap()
}

async fn endpoint(
    replies: Vec<(u16, Value)>,
) -> (String, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/token", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let requests = seen.clone();
    let task = tokio::spawn(async move {
        for (status, reply) in replies {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut buffer = [0; 2048];
                let read = stream.read(&mut buffer).await.unwrap();
                assert!(read > 0);
                bytes.extend_from_slice(&buffer[..read]);
                let Some(headers) = bytes.windows(4).position(|value| value == b"\r\n\r\n") else {
                    continue;
                };
                let head = std::str::from_utf8(&bytes[..headers]).unwrap();
                let length: usize = head
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                if bytes.len() >= headers + 4 + length {
                    assert!(head.starts_with("POST /token HTTP/1.1"));
                    requests
                        .lock()
                        .unwrap()
                        .push(String::from_utf8(bytes[headers + 4..].to_vec()).unwrap());
                    break;
                }
            }
            let body = reply.to_string();
            let reason = StatusCode::from_u16(status)
                .unwrap()
                .canonical_reason()
                .unwrap();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    (url, seen, task)
}

#[tokio::test]
async fn manual_refresh_rotates_unexpired_tokens_and_reports_missing_or_unchanged_refresh_tokens() {
    let (url, seen, server) = endpoint(vec![
        (200, json!({"access_token":"access-one-private", "refresh_token":"refresh-one-private", "expires_in":3600})),
        (200, json!({"access_token":"access-two-private", "expires_in":3600})),
        (200, json!({"access_token":"access-three-private", "refresh_token":"refresh-one-private", "expires_in":3600})),
    ]).await;
    let Fixture {
        app,
        credential,
        _directory,
        ..
    } = fixture(&url).await;
    assert!(app.credential_refresh_supported(credential).await.unwrap());
    let listing = request(&app, Method::GET, "/admin/api/credentials", "", true).await;
    assert_eq!(listing.status(), StatusCode::OK);
    let listed: Vec<gproxy_admin::dto::CredentialDto> =
        serde_json::from_slice(listing.body()).unwrap();
    assert!(
        listed
            .iter()
            .find(|value| value.id == credential)
            .unwrap()
            .refresh_supported
    );

    let path = format!("/admin/api/credentials/{credential}/refresh");
    for (version, (status, access)) in [
        (
            CredentialRefreshTokenStatusDto::Updated,
            "access-one-private",
        ),
        (
            CredentialRefreshTokenStatusDto::NotReturned,
            "access-two-private",
        ),
        (
            CredentialRefreshTokenStatusDto::Unchanged,
            "access-three-private",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let response = request(
            &app,
            Method::POST,
            &path,
            &json!({"version":version}).to_string(),
            true,
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(response.body())
        );
        assert!(!String::from_utf8_lossy(response.body()).contains("private"));
        assert_eq!(
            response.headers().get(http::header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        let result: CredentialRefreshResponse = serde_json::from_slice(response.body()).unwrap();
        assert_eq!(result.credential_id, credential);
        assert_eq!(result.credential_version, version as u64 + 1);
        assert_eq!(result.refresh_token_status, status);
        let secret = app.reveal_credential_secret(credential).await.unwrap();
        assert_eq!(secret["access_token"], access);
        assert_eq!(secret["refresh_token"], "refresh-one-private");
        assert_eq!(secret["quota_management_key"], "independent-quota-private");
        assert_eq!(secret["project_id"], "refresh-project");
    }
    server.await.unwrap();
    {
        let requests = seen.lock().unwrap();
        assert_eq!(requests.len(), 3);
        for (index, body) in requests.iter().enumerate() {
            let fields: std::collections::HashMap<String, String> =
                serde_urlencoded::from_str(body).unwrap();
            assert_eq!(fields["grant_type"], "refresh_token");
            assert_eq!(
                fields["refresh_token"],
                if index == 0 {
                    "initial-refresh-private"
                } else {
                    "refresh-one-private"
                }
            );
        }
    }
    let audit = app.store().audit_events(3).await.unwrap();
    assert_eq!(audit.len(), 3);
    assert!(
        audit
            .iter()
            .all(|value| value.event.action == "credential.refresh"
                && value.event.target_id == Some(credential)
                && value.event.details.is_none())
    );
}

#[tokio::test]
async fn manual_refresh_failure_preserves_credentials_and_does_not_expose_upstream_secrets() {
    let (url, seen, server) = endpoint(vec![
        (
            400,
            json!({"error":"invalid_grant", "description":"refresh_token=upstream-private-value"}),
        ),
        (
            200,
            json!({"refresh_token":"replacement-that-must-not-be-saved"}),
        ),
    ])
    .await;
    let Fixture {
        app,
        credential,
        _directory,
        ..
    } = fixture(&url).await;
    let before = app.store().credential(credential).await.unwrap().unwrap();
    let path = format!("/admin/api/credentials/{credential}/refresh");
    for _ in 0..2 {
        let response = request(&app, Method::POST, &path, r#"{"version":0}"#, true).await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert!(!String::from_utf8_lossy(response.body()).contains("private"));
        assert_eq!(
            app.store().credential(credential).await.unwrap().unwrap(),
            before
        );
    }
    server.await.unwrap();
    assert_eq!(seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn manual_refresh_validates_authentication_request_and_current_configuration_before_egress() {
    let (url, seen, server) =
        endpoint(vec![(200, json!({"access_token":"should-not-be-used"}))]).await;
    let Fixture {
        app,
        provider,
        credential,
        _directory,
    } = fixture(&url).await;
    let path = format!("/admin/api/credentials/{credential}/refresh");
    assert_eq!(
        request(&app, Method::POST, &path, r#"{"version":0}"#, false)
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    for body in [
        "",
        "{}",
        "null",
        "[]",
        r#"{"version":null}"#,
        r#"{"version":-1}"#,
        r#"{"version":1.5}"#,
        r#"{"version":"0"}"#,
        r#"{"version":0,"refresh_token":"private"}"#,
    ] {
        assert_eq!(
            request(&app, Method::POST, &path, body, true)
                .await
                .status(),
            StatusCode::BAD_REQUEST,
            "{body}"
        );
    }
    assert_eq!(
        request(&app, Method::GET, &path, "", true).await.status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        request(&app, Method::POST, &path, r#"{"version":1}"#, true)
            .await
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        request(
            &app,
            Method::POST,
            "/admin/api/credentials/999999/refresh",
            r#"{"version":0}"#,
            true
        )
        .await
        .status(),
        StatusCode::NOT_FOUND
    );

    let missing = insert(
        &app,
        provider,
        "oauth",
        json!({"access_token":"access-only"}),
        true,
    )
    .await;
    assert!(!app.credential_refresh_supported(missing).await.unwrap());
    assert_eq!(
        request(
            &app,
            Method::POST,
            &format!("/admin/api/credentials/{missing}/refresh"),
            r#"{"version":0}"#,
            true
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST
    );
    let disabled = insert(
        &app,
        provider,
        "oauth",
        json!({"refresh_token":"disabled-refresh"}),
        false,
    )
    .await;
    assert_eq!(
        request(
            &app,
            Method::POST,
            &format!("/admin/api/credentials/{disabled}/refresh"),
            r#"{"version":0}"#,
            true
        )
        .await
        .status(),
        StatusCode::CONFLICT
    );

    let input = app
        .store()
        .control_snapshot()
        .await
        .unwrap()
        .providers
        .into_iter()
        .find(|value| value.id == provider)
        .unwrap();
    app.store()
        .update_provider(
            provider,
            &ProviderInput {
                name: input.name,
                label: input.label,
                channel: input.channel,
                settings: input.settings,
                credential_strategy: input.credential_strategy,
                proxy_url: input.proxy_url,
                tls_fingerprint: input.tls_fingerprint,
                enabled: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        request(&app, Method::POST, &path, r#"{"version":0}"#, true)
            .await
            .status(),
        StatusCode::CONFLICT
    );
    assert!(seen.lock().unwrap().is_empty());
    server.abort();
}

#[tokio::test]
async fn manual_refresh_rejects_a_credential_moved_to_another_provider_since_the_page_loaded() {
    let (url, seen, server) =
        endpoint(vec![(200, json!({"access_token":"should-not-be-used"}))]).await;
    let Fixture {
        app,
        credential,
        _directory,
        ..
    } = fixture(&url).await;
    let provider = app
        .store()
        .insert_provider(&ProviderInput {
            name: "new-provider".into(),
            label: None,
            channel: "openai".into(),
            settings: json!({}),
            credential_strategy: "round_robin".into(),
            proxy_url: None,
            tls_fingerprint: None,
            enabled: true,
        })
        .await
        .unwrap();
    app.store()
        .update_credential(
            credential,
            &CredentialUpdateInput {
                provider_id: provider,
                label: None,
                kind: "api_key".into(),
                envelope: Some(
                    app.seal_credential(&json!({"api_key":"new-provider-key"}))
                        .unwrap(),
                ),
                enabled: true,
                weight: 100,
                rpm_limit: None,
                tpm_limit: None,
                proxy_url: None,
                tls_fingerprint: None,
            },
        )
        .await
        .unwrap();
    let response = request(
        &app,
        Method::POST,
        &format!("/admin/api/credentials/{credential}/refresh"),
        r#"{"version":0}"#,
        true,
    )
    .await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(seen.lock().unwrap().is_empty());
    assert!(!app.credential_refresh_supported(credential).await.unwrap());
    server.abort();
}

#[test]
fn upstream_refresh_errors_do_not_expose_token_material() {
    for error in [
        CoreError::Channel(gproxy_channel_api::ChannelError::Refresh(
            "refresh_token=upstream-private-value".into(),
        )),
        CoreError::Transport(gproxy_channel_api::TransportError::Connect(
            "https://token.invalid?token=upstream-private-value".into(),
        )),
        CoreError::Store(gproxy_core::error::StoreError(
            "ciphertext=upstream-private-value".into(),
        )),
    ] {
        assert!(
            !refresh_error(error)
                .to_string()
                .contains("upstream-private-value")
        );
    }
}
