use std::collections::VecDeque;
use std::future::Future;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

use gproxy_channel_api::{BoxFuture, ClientProfilePreset, SimpleHttp};

use super::*;

#[test]
fn preserves_full_cookie_headers_and_normalizes_bare_keys() {
    assert_eq!(
        crate::shared::claude::cookie::normalize(
            "Cookie: cf_clearance=clear; sessionKey=sk-ant-sid01-example; __cf_bm=bm"
        )
        .as_deref(),
        Some("cf_clearance=clear; sessionKey=sk-ant-sid01-example; __cf_bm=bm")
    );
    assert_eq!(
        crate::shared::claude::cookie::normalize("sk-ant-sid02-example").as_deref(),
        Some("sessionKey=sk-ant-sid02-example")
    );
}

#[test]
fn cookie_only_refresh_retries_bootstrap_and_mints_oauth_secret() {
    let http = MockHttp::new([
        (
            http::StatusCode::FORBIDDEN,
            br#"<title>Just a moment...</title>"#.as_slice(),
        ),
        (
            http::StatusCode::OK,
            br#"{"usage":{}} {"account":{"memberships":[{"organization":{"uuid":"org-api","capabilities":["api"]}},{"organization":{"uuid":"org-sub","capabilities":["claude_max"]}}]}}"#,
        ),
        (
            http::StatusCode::OK,
            br#"{"redirect_uri":"https://platform.claude.com/oauth/code/callback?code=code-1&state=__STATE__"}"#,
        ),
        (
            http::StatusCode::OK,
            br#"{"access_token":"fresh","expires_in":3600,"scope":"user:inference user:file_upload"}"#,
        ),
        (
            http::StatusCode::OK,
            br#"{"account":{"uuid":"account-1","email":"user@example.com"},"organization":{"uuid":"org-sub","organization_type":"claude_max"}}"#,
        ),
    ]);
    let old = json!({
        "cookie": "cf_clearance=clear; sessionKey=sk-ant-sid01-example",
        "operator_note": "keep"
    });
    let result = ready(super::super::auth::refresh(&old, &http)).unwrap();
    assert_eq!(
        result.refresh_token,
        gproxy_channel_api::RefreshTokenStatus::NotReturned
    );
    let refreshed = result.secret;
    assert_eq!(refreshed["access_token"], "fresh");
    assert_eq!(refreshed["account_uuid"], "account-1");
    assert_eq!(refreshed["organization_uuid"], "org-sub");
    assert_eq!(refreshed["operator_note"], "keep");
    assert!(refreshed.get("refresh_token").is_none());
    assert!(refreshed["device_id"].as_str().is_some());

    let captured = http.captured.lock().unwrap();
    assert_eq!(captured.len(), 5);
    assert_eq!(captured[0].uri, "https://claude.ai/api/bootstrap");
    assert_eq!(captured[1].uri, "https://claude.ai/api/bootstrap");
    assert_eq!(
        captured[1].headers["cookie"],
        "cf_clearance=clear; sessionKey=sk-ant-sid01-example"
    );
    assert_eq!(
        captured[2].uri,
        "https://api.anthropic.com/v1/oauth/org-sub/authorize"
    );
    assert_eq!(captured[3].uri, auth::COOKIE_TOKEN_URL);
    assert!(
        std::str::from_utf8(&captured[2].body)
            .unwrap()
            .contains("user:inference")
    );
    assert!(
        std::str::from_utf8(&captured[3].body)
            .unwrap()
            .contains("grant_type=authorization_code")
    );
    assert!(captured[..4].iter().all(|request| {
        request.preset == Some(ClientProfilePreset::Chrome148) && request.required
    }));
    assert_eq!(captured[4].preset, None);
    assert!(!captured[4].required);
}

#[test]
fn cookie_refresh_reports_new_token_before_overlaying_minted_secret() {
    let http = MockHttp::new([
        (
            http::StatusCode::OK,
            br#"{"account":{"memberships":[{"organization":{"uuid":"org-sub","capabilities":["claude_max"]}}]}}"#.as_slice(),
        ),
        (
            http::StatusCode::OK,
            br#"{"redirect_uri":"https://platform.claude.com/oauth/code/callback?code=code-1&state=__STATE__"}"#,
        ),
        (
            http::StatusCode::OK,
            br#"{"access_token":"fresh","refresh_token":"rotated","expires_in":3600}"#,
        ),
        (http::StatusCode::OK, br#"{}"#),
    ]);
    let old = json!({
        "cookie":"sessionKey=sk-ant-sid01-example",
        "quota_api_key":"quota-key",
        "operator_note":"keep"
    });
    let result = ready(super::super::auth::refresh(&old, &http)).unwrap();
    assert_eq!(
        result.refresh_token,
        gproxy_channel_api::RefreshTokenStatus::Updated
    );
    assert_eq!(result.secret["refresh_token"], "rotated");
    assert_eq!(result.secret["quota_api_key"], "quota-key");
    assert_eq!(result.secret["operator_note"], "keep");
    assert!(old.get("refresh_token").is_none());
}

#[test]
fn cookie_refresh_keeps_saved_identity_when_profile_lookup_fails() {
    for device in [Value::Null, json!(" \t "), json!("fixed-device")] {
        let http = MockHttp::new([
            (
                http::StatusCode::OK,
                br#"{"account":{"memberships":[{"organization":{"uuid":"org-sub","capabilities":["claude_max"]}}]}}"#.as_slice(),
            ),
            (
                http::StatusCode::OK,
                br#"{"redirect_uri":"https://platform.claude.com/oauth/code/callback?code=code-1&state=__STATE__"}"#,
            ),
            (
                http::StatusCode::OK,
                br#"{"access_token":"fresh","expires_in":3600}"#,
            ),
            (http::StatusCode::SERVICE_UNAVAILABLE, br#"{}"#),
        ]);
        let old = json!({
            "cookie":"sessionKey=sk-ant-sid01-example",
            "access_token":"old-access",
            "account_uuid":"account-1",
            "organization_uuid":"org-sub",
            "device_id":device,
            "quota_api_key":"quota-key"
        });
        let device = auth::device_id(&old);
        let result = ready(super::refresh(&old, &http)).unwrap();
        assert_eq!(result.secret["account_uuid"], "account-1");
        assert_eq!(result.secret["organization_uuid"], "org-sub");
        assert_eq!(result.secret["device_id"], device);
        assert_eq!(result.secret["quota_api_key"], "quota-key");
    }
}

#[test]
fn cookie_login_keeps_bootstrap_account_separate_from_organization_without_profile() {
    let http = MockHttp::new([
        (
            http::StatusCode::OK,
            br#"{"account":{"uuid":"account-1","memberships":[{"organization":{"uuid":"org-sub","capabilities":["claude_max"]}}]}}"#.as_slice(),
        ),
        (
            http::StatusCode::OK,
            br#"{"redirect_uri":"https://platform.claude.com/oauth/code/callback?code=code-1&state=__STATE__"}"#,
        ),
        (
            http::StatusCode::OK,
            br#"{"access_token":"fresh","expires_in":3600}"#,
        ),
        (http::StatusCode::SERVICE_UNAVAILABLE, br#"{}"#),
    ]);
    let secret = ready(super::exchange(&http, "sessionKey=sk-ant-sid01-example")).unwrap();
    assert_eq!(secret["account_uuid"], "account-1");
    assert_eq!(secret["organization_uuid"], "org-sub");
    let captured = http.captured.lock().unwrap();
    let body: Value = serde_json::from_slice(&captured[1].body).unwrap();
    assert_eq!(body["organization_uuid"], "org-sub");
}

#[test]
fn cookie_exchange_rejects_blank_access_tokens() {
    let http = MockHttp::new([(
        http::StatusCode::OK,
        br#"{"access_token":" \t\n ","refresh_token":"rotated","expires_in":3600}"#.as_slice(),
    )]);
    assert!(ready(super::token_exchange(&http, "verifier", "state", "code")).is_err());
}

#[test]
fn cookie_token_exchange_does_not_invent_expiry_when_provider_omits_it() {
    let http = MockHttp::new([(
        http::StatusCode::OK,
        br#"{"access_token":"fresh","refresh_token":"rotated"}"#.as_slice(),
    )]);
    let secret = ready(super::token_exchange(&http, "verifier", "state", "code")).unwrap();
    assert_eq!(secret["access_token"], "fresh");
    assert_eq!(secret["refresh_token"], "rotated");
    assert!(secret.get("expires_at_ms").is_none());
    assert!(secret["token_received_at_ms"].as_i64().unwrap() > 0);
}

#[test]
fn cookie_refresh_preserves_selected_organization_and_clears_unknown_expiry() {
    let http = MockHttp::new([
        (
            http::StatusCode::OK,
            br#"{"account":{"uuid":"acct-1","memberships":[{"organization":{"uuid":"org-other","capabilities":["claude_max"]}},{"organization":{"uuid":"org-selected","capabilities":["claude_pro"]}}]}}"#.as_slice(),
        ),
        (
            http::StatusCode::OK,
            br#"{"redirect_uri":"https://platform.claude.com/oauth/code/callback?code=code%2Bwith%2Fsymbols%3D&state=__STATE__"}"#,
        ),
        (http::StatusCode::OK, br#"{"access_token":"fresh","refresh_token":"rotated"}"#),
        (http::StatusCode::SERVICE_UNAVAILABLE, br#"{}"#),
    ]);
    let old = json!({
        "cookie":"sessionKey=sk-ant-sid01-example",
        "account_uuid":"acct-1",
        "organization_uuid":"org-selected",
        "device_id":"fixed-device",
        "expires_at_ms":1,
        "token_received_at_ms":1
    });
    let result = ready(super::refresh(&old, &http)).unwrap();
    assert_eq!(result.secret["organization_uuid"], "org-selected");
    assert_eq!(result.secret["account_uuid"], "acct-1");
    assert_eq!(result.secret["device_id"], "fixed-device");
    assert_eq!(result.secret["refresh_token"], "rotated");
    assert!(result.secret.get("expires_at_ms").is_none());
    assert!(result.secret["token_received_at_ms"].as_i64().unwrap() > 1);
    let captured = http.captured.lock().unwrap();
    assert!(captured[1].uri.ends_with("/org-selected/authorize"));
    let fields = form_urlencoded::parse(&captured[2].body).collect::<Vec<_>>();
    assert!(
        fields
            .iter()
            .any(|(key, value)| key == "code" && value == "code+with/symbols=")
    );
    let authorize: Value = serde_json::from_slice(&captured[1].body).unwrap();
    assert!(
        fields
            .iter()
            .any(|(key, value)| key == "state" && value == authorize["state"].as_str().unwrap())
    );
}

#[test]
fn cookie_refresh_fails_before_authorization_when_saved_organization_disappears() {
    let http = MockHttp::new([(
        http::StatusCode::OK,
        br#"{"account":{"memberships":[{"organization":{"uuid":"org-other","capabilities":["claude_max"]}}]}}"#.as_slice(),
    )]);
    let secret = json!({
        "cookie":"sessionKey=sk-ant-sid01-example",
        "organization_uuid":"org-selected"
    });
    assert!(ready(super::refresh(&secret, &http)).is_err());
    assert_eq!(http.captured.lock().unwrap().len(), 1);
}

#[test]
fn cookie_refresh_rejects_token_for_a_different_organization() {
    let http = MockHttp::new([
        (
            http::StatusCode::OK,
            br#"{"account":{"memberships":[{"organization":{"uuid":"org-selected","capabilities":["claude_max"]}}]}}"#.as_slice(),
        ),
        (
            http::StatusCode::OK,
            br#"{"redirect_uri":"https://platform.claude.com/oauth/code/callback?code=code-1&state=__STATE__"}"#,
        ),
        (
            http::StatusCode::OK,
            br#"{"access_token":"fresh","organization":{"uuid":"org-other"}}"#,
        ),
        (http::StatusCode::SERVICE_UNAVAILABLE, br#"{}"#),
    ]);
    let secret = json!({
        "cookie":"sessionKey=sk-ant-sid01-example",
        "organization_uuid":"org-selected"
    });
    assert!(ready(super::refresh(&secret, &http)).is_err());
    assert_eq!(secret["organization_uuid"], "org-selected");
}

#[test]
fn authorization_callback_requires_expected_location_and_unique_matching_state() {
    for uri in [
        "https://example.test/oauth/code/callback?code=code&state=expected",
        "http://platform.claude.com/oauth/code/callback?code=code&state=expected",
        "https://platform.claude.com/wrong?code=code&state=expected",
        "https://platform.claude.com/oauth/code/callback?code=code&state=wrong",
        "https://platform.claude.com/oauth/code/callback?code=code",
        "https://platform.claude.com/oauth/code/callback?code=&state=expected",
        "https://platform.claude.com/oauth/code/callback?code=code&code=other&state=expected",
        "https://platform.claude.com/oauth/code/callback?code=code&state=expected&state=expected",
        "https://platform.claude.com/oauth/code/callback?code=code&state=expected#fragment",
        "https://platform.claude.com/oauth/code/callback?error=access_denied&code=code&state=expected",
    ] {
        let response = json!({"redirect_uri":uri}).to_string();
        let http = MockHttp::new([(http::StatusCode::OK, response.as_bytes())]);
        assert!(
            ready(super::authorize(
                &http,
                "sessionKey=example",
                "org",
                "expected",
                "challenge"
            ))
            .is_err()
        );
        assert_eq!(http.captured.lock().unwrap().len(), 1);
    }
}

#[test]
fn cookie_refresh_errors_do_not_echo_upstream_bodies() {
    let cookie = "sessionKey=sk-ant-sid01-sensitive-cookie";
    let http = MockHttp::new([(
        http::StatusCode::UNAUTHORIZED,
        br#"{"session_key":"sensitive-upstream-value"}"#.as_slice(),
    )]);
    let Err(error) = ready(super::discover_identity(&http, cookie, None)) else {
        panic!("unauthorized bootstrap accepted")
    };
    assert!(!error.to_string().contains("sensitive-upstream-value"));
    assert!(!error.to_string().contains(cookie));

    for (status, body) in [
        (
            http::StatusCode::UNAUTHORIZED,
            br#"{"refresh_token":"sensitive-upstream-value"}"#.as_slice(),
        ),
        (
            http::StatusCode::OK,
            br#"{"redirect_uri":"sensitive-upstream-value" broken}"#,
        ),
    ] {
        let http = MockHttp::new([(status, body)]);
        let Err(error) = ready(super::authorize(
            &http,
            cookie,
            "org-sub",
            "state",
            "challenge",
        )) else {
            panic!("invalid authorize response accepted")
        };
        assert!(!error.to_string().contains("sensitive-upstream-value"));
    }
    for (status, body) in [
        (
            http::StatusCode::UNAUTHORIZED,
            br#"{"refresh_token":"sensitive-upstream-value"}"#.as_slice(),
        ),
        (
            http::StatusCode::OK,
            br#"{"access_token":"sensitive-upstream-value" broken}"#,
        ),
    ] {
        let http = MockHttp::new([(status, body)]);
        let Err(error) = ready(super::token_exchange(&http, "verifier", "state", "code")) else {
            panic!("invalid token response accepted")
        };
        assert!(!error.to_string().contains("sensitive-upstream-value"));
    }
}

struct Captured {
    uri: String,
    headers: http::HeaderMap,
    body: Bytes,
    preset: Option<ClientProfilePreset>,
    required: bool,
}

struct MockHttp {
    responses: Mutex<VecDeque<(http::StatusCode, Bytes)>>,
    captured: Mutex<Vec<Captured>>,
}

impl MockHttp {
    fn new<const N: usize>(responses: [(http::StatusCode, &[u8]); N]) -> Self {
        Self {
            responses: Mutex::new(
                responses
                    .into_iter()
                    .map(|(status, body)| (status, Bytes::copy_from_slice(body)))
                    .collect(),
            ),
            captured: Mutex::new(Vec::new()),
        }
    }
}

impl SimpleHttp for MockHttp {
    fn send<'a>(
        &'a self,
        request: http::Request<Bytes>,
    ) -> BoxFuture<'a, Result<http::Response<Bytes>, ChannelError>> {
        let callback_state = request
            .uri()
            .path()
            .ends_with("/authorize")
            .then(|| serde_json::from_slice::<Value>(request.body()).unwrap()["state"].clone())
            .and_then(|state| state.as_str().map(str::to_owned));
        let preset = request
            .extensions()
            .get::<ClientProfile>()
            .and_then(|profile| profile.preset);
        let required = request
            .extensions()
            .get::<RequiredClientProfile>()
            .is_some();
        let (parts, body) = request.into_parts();
        self.captured.lock().unwrap().push(Captured {
            uri: parts.uri.to_string(),
            headers: parts.headers,
            body,
            preset,
            required,
        });
        let (status, mut body) = self.responses.lock().unwrap().pop_front().unwrap();
        if let Some(state) = callback_state {
            body = Bytes::from(
                String::from_utf8_lossy(&body)
                    .replace("__STATE__", &crate::shared::http::encode_component(&state)),
            );
        }
        Box::pin(async move {
            http::Response::builder()
                .status(status)
                .body(body)
                .map_err(|error| ChannelError::Login(error.to_string()))
        })
    }
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("test future unexpectedly pending"),
    }
}
