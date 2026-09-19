use super::setup;
use crate::{AppHandle, ControlMutation, MutationResult};
use gproxy_admin::State;
use gproxy_channel_api::QuotaValue;
use gproxy_core::{CacheBackend, CredentialId, CredentialStore};
use http::StatusCode;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Reply {
    path: &'static str,
    status: StatusCode,
    body: Value,
    retry_after: Option<u32>,
}

struct Seen {
    headers: String,
    body: String,
}

fn token_reply() -> Reply {
    Reply {
        path: "/token",
        status: StatusCode::OK,
        body: json!({"access_token":"fresh-access","refresh_token":"rotated-refresh","expires_in":3600}),
        retry_after: None,
    }
}

fn usage_reply(status: StatusCode) -> Reply {
    Reply {
        path: "/v1internal:retrieveUserQuota",
        status,
        body: json!({"buckets":[{"modelId":"gemini-2.5-pro","tokenType":"REQUESTS","remainingFraction":0.75,"quotaAmount":100}]}),
        retry_after: (status == StatusCode::TOO_MANY_REQUESTS).then_some(300),
    }
}

async fn endpoint(
    replies: Vec<Reply>,
) -> (String, Arc<Mutex<Vec<Seen>>>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let requests = seen.clone();
    let task = tokio::spawn(async move {
        for reply in replies {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut bytes = Vec::new();
            loop {
                let mut chunk = [0; 4096];
                let count = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut chunk))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&chunk[..count]);
                let Some(end) = bytes.windows(4).position(|value| value == b"\r\n\r\n") else {
                    continue;
                };
                let headers = std::str::from_utf8(&bytes[..end]).unwrap();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                if bytes.len() < end + 4 + length {
                    continue;
                }
                assert!(headers.starts_with(&format!("POST {} HTTP/1.1", reply.path)));
                requests.lock().unwrap().push(Seen {
                    headers: headers.to_ascii_lowercase(),
                    body: String::from_utf8(bytes[end + 4..end + 4 + length].to_vec()).unwrap(),
                });
                break;
            }
            let body = reply.body.to_string();
            let retry = reply
                .retry_after
                .map(|seconds| format!("Retry-After: {seconds}\r\n"))
                .unwrap_or_default();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{retry}\r\n{body}",
                        reply.status, body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    (url, seen, task)
}

async fn credential(app: &AppHandle, base: &str) -> i64 {
    let MutationResult::Id(provider) = app
        .mutate(ControlMutation::Provider(
            gproxy_store::records::ProviderInput {
                name: "quota-oauth-refresh".into(),
                label: None,
                channel: "geminicli".into(),
                settings: json!({"base_url":base,"oauth_token_url":format!("{base}/token")}),
                credential_strategy: "round_robin".into(),
                proxy_url: None,
                tls_fingerprint: None,
                enabled: true,
            },
        ))
        .await
        .unwrap()
    else {
        panic!("provider");
    };
    let MutationResult::Id(id) = app
        .mutate(ControlMutation::Credential {
            provider_id: provider,
            label: None,
            secret: json!({
                "access_token":"expired-access","refresh_token":"saved-refresh",
                "expires_at_ms":1,"project_id":"quota-project",
                "quota_api_key":"independent-quota-key"
            }),
            enabled: true,
        })
        .await
        .unwrap()
    else {
        panic!("credential");
    };
    id
}

#[tokio::test]
async fn quota_probe_refreshes_oauth_and_saves_usage_under_the_rotated_version() {
    let fixture = setup::fixture().await;
    let app = &fixture.app;
    app.shutdown();
    app.drain_background().await;
    let (url, seen, server) = endpoint(vec![token_reply(), usage_reply(StatusCode::OK)]).await;
    let id = credential(app, &url).await;
    let before = app.store().credential(id).await.unwrap().unwrap().version;
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        app.quota_probe_lightweight(id, true),
    )
    .await
    .unwrap()
    .unwrap();
    server.await.unwrap();
    assert_eq!(response.credential_version, before + 1);
    assert_eq!(response.snapshot.entries.len(), 1);
    assert!(response.snapshot.sources[0].error.is_none());
    let QuotaValue::Window(window) = &response.snapshot.entries[0].value else {
        panic!("subscription window");
    };
    assert_eq!(window.used, Some(25.into()));
    assert_eq!(window.limit, Some(100.into()));
    assert_eq!(
        app.credential_quota_snapshot(id).await.unwrap(),
        response.snapshot
    );
    let stored = app.inner.host.load_current(CredentialId(id)).await.unwrap();
    assert_eq!(stored.version, response.credential_version);
    assert_eq!(stored.secret["refresh_token"], "rotated-refresh");
    assert_eq!(stored.secret["quota_api_key"], "independent-quota-key");
    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].body.contains("refresh_token=saved-refresh"));
    assert!(
        requests[1]
            .headers
            .contains("authorization: bearer fresh-access")
    );
    assert!(!requests[1].headers.contains("expired-access"));
    assert_eq!(
        serde_json::from_str::<Value>(&requests[1].body).unwrap()["project"],
        "quota-project"
    );
}

#[tokio::test]
async fn quota_failure_after_oauth_rotation_uses_the_new_version_for_state_and_backoff() {
    let fixture = setup::fixture().await;
    let app = &fixture.app;
    app.shutdown();
    app.drain_background().await;
    let (url, seen, server) = endpoint(vec![
        token_reply(),
        usage_reply(StatusCode::TOO_MANY_REQUESTS),
    ])
    .await;
    let id = credential(app, &url).await;
    let before = app.store().credential(id).await.unwrap().unwrap().version;
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        app.quota_probe_lightweight(id, true),
    )
    .await
    .unwrap()
    .unwrap();
    server.await.unwrap();
    assert_eq!(response.credential_version, before + 1);
    assert_eq!(
        response.snapshot.sources[0].error.as_ref().unwrap().code,
        "rate_limited"
    );
    assert!(response.snapshot.sources[0].attempted_at_ms.is_some());
    assert!(response.snapshot.entries.is_empty());
    let cache = &app.inner.host.services.cache;
    for suffix in ["upstream-retry", "retry", "failures"] {
        assert!(
            cache
                .get(&format!(
                    "quota:source:{id}:v{before}:subscription:{suffix}"
                ))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            cache
                .get(&format!(
                    "quota:source:{id}:v{}:subscription:{suffix}",
                    before + 1
                ))
                .await
                .unwrap()
                .is_some()
        );
    }
    let repeated = app.quota_probe_lightweight(id, true).await.unwrap();
    assert_eq!(repeated.credential_version, response.credential_version);
    assert_eq!(repeated.snapshot, response.snapshot);
    assert_eq!(seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn concurrent_quota_probes_share_rotation_and_query_with_the_saved_new_token() {
    let fixture = setup::fixture().await;
    let app = &fixture.app;
    app.shutdown();
    app.drain_background().await;
    let (url, seen, server) = endpoint(vec![
        token_reply(),
        usage_reply(StatusCode::OK),
        usage_reply(StatusCode::OK),
    ])
    .await;
    let id = credential(app, &url).await;
    let before = app.store().credential(id).await.unwrap().unwrap().version;
    let (first, second) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            app.quota_probe_lightweight(id, true),
            app.quota_probe_lightweight(id, true)
        )
    })
    .await
    .unwrap();
    for response in [first.unwrap(), second.unwrap()] {
        assert_eq!(response.credential_version, before + 1);
        assert_eq!(response.snapshot.entries.len(), 1);
        assert!(response.snapshot.sources[0].error.is_none());
    }
    server.await.unwrap();
    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].body.contains("refresh_token=saved-refresh"));
    for request in &requests[1..] {
        assert!(
            request
                .headers
                .contains("authorization: bearer fresh-access")
        );
        assert!(!request.headers.contains("expired-access"));
    }
}
