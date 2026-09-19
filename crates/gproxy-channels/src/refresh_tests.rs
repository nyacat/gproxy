use std::future::Future;
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use gproxy_channel_api::{
    BoxFuture, Channel, ChannelError, RefreshResult, RefreshTokenStatus, SimpleHttp,
};
use serde_json::{Value, json};

fn oauth_channels() -> Vec<Box<dyn Channel>> {
    vec![
        Box::new(crate::CodexChannel),
        Box::new(crate::ClaudeCodeChannel),
        Box::new(crate::ClineChannel),
        Box::new(crate::KimiChannel),
        Box::new(crate::KiroChannel),
        Box::new(crate::GrokBuildChannel),
        Box::new(crate::OpenCodeChannel),
        Box::new(crate::GeminiCliChannel),
        Box::new(crate::AntigravityChannel),
        Box::new(crate::WorkBuddyChannel),
    ]
}

#[test]
fn manual_refresh_support_depends_on_saved_token_not_expiry() {
    for channel in oauth_channels() {
        let id = channel.descriptor().id;
        for expiry in [None, Some(json!(0)), Some(json!(2_000_000_000_000_i64))] {
            let mut secret = json!({"access_token":"access", "refresh_token":"refresh"});
            if let Some(expiry) = expiry {
                secret["expires_at_ms"] = expiry;
            }
            assert!(channel.can_refresh(&secret), "{id}");
        }
        for token in [
            None,
            Some(Value::Null),
            Some(json!("")),
            Some(json!(" \t\n ")),
        ] {
            let mut secret = json!({"access_token":"access", "expires_at_ms":1});
            if let Some(token) = token {
                secret["refresh_token"] = token;
            }
            assert!(!channel.can_refresh(&secret), "{id}");
        }
    }
}

#[test]
fn non_oauth_refresh_paths_do_not_offer_refresh_token_grants() {
    let channels: Vec<Box<dyn Channel>> = vec![
        Box::new(crate::VertexChannel),
        Box::new(crate::CopilotCliChannel),
        Box::new(crate::OpenAiChannel),
        Box::new(crate::ClaudeApiChannel),
    ];
    let secret = json!({"refresh_token":"refresh"});
    for channel in channels {
        assert!(!channel.can_refresh(&secret), "{}", channel.descriptor().id);
    }
    #[cfg(not(target_arch = "wasm32"))]
    assert!(!crate::ClaudeWebChannel.can_refresh(&secret));
    assert!(!crate::ClaudeCodeChannel.can_refresh(&json!({"cookie":"sessionKey=example"})));
}

#[test]
fn oauth_refresh_reports_returned_token_before_merging_and_preserves_other_secrets() {
    for channel in oauth_channels() {
        let id = channel.descriptor().id;
        for (returned, status, expected_token) in [
            (
                Some(json!("new-refresh")),
                RefreshTokenStatus::Updated,
                "new-refresh",
            ),
            (
                Some(json!("old-refresh")),
                RefreshTokenStatus::Unchanged,
                "old-refresh",
            ),
            (
                Some(json!(" old-refresh ")),
                RefreshTokenStatus::Unchanged,
                "old-refresh",
            ),
            (None, RefreshTokenStatus::NotReturned, "old-refresh"),
            (
                Some(Value::Null),
                RefreshTokenStatus::NotReturned,
                "old-refresh",
            ),
            (
                Some(json!("")),
                RefreshTokenStatus::NotReturned,
                "old-refresh",
            ),
            (
                Some(json!(" \t\n ")),
                RefreshTokenStatus::NotReturned,
                "old-refresh",
            ),
        ] {
            let secret = json!({
                "access_token":"old-access",
                "refresh_token":"old-refresh",
                "device_id":"device-1",
                "quota_api_key":"independent-quota-key",
                "quota_nested":{"preserved":true},
                "operator_note":"keep"
            });
            let before = secret.clone();
            let response = token_response(id, returned, Some("new-access"));
            let http = RefreshHttp {
                status: 200,
                response,
            };
            let settings = json!({});
            let result = run(channel
                .refresh(&secret, &settings, &http)
                .expect("refresh supported"))
            .unwrap_or_else(|error| panic!("{id}: {error}"));
            assert_eq!(result.refresh_token, status, "{id}");
            assert_eq!(result.secret["access_token"], "new-access", "{id}");
            assert_eq!(result.secret["refresh_token"], expected_token, "{id}");
            for name in [
                "device_id",
                "quota_api_key",
                "quota_nested",
                "operator_note",
            ] {
                assert_eq!(result.secret[name], before[name], "{id}: {name}");
            }
            assert_eq!(secret, before, "{id}");
        }
    }
}

#[test]
fn invalid_access_token_does_not_produce_a_replacement_secret() {
    for channel in oauth_channels() {
        let id = channel.descriptor().id;
        for access in [None, Some(""), Some(" \t\n ")] {
            let secret = json!({
                "access_token":"old-access", "refresh_token":"old-refresh", "device_id":"device-1"
            });
            let before = secret.clone();
            let response = token_response(id, Some(json!("new-refresh")), access);
            let http = RefreshHttp {
                status: 200,
                response,
            };
            let settings = json!({});
            assert!(
                run(channel.refresh(&secret, &settings, &http).unwrap()).is_err(),
                "{id}"
            );
            assert_eq!(secret, before, "{id}");
        }
    }
}

#[test]
fn copilot_auto_refresh_reports_not_applicable() {
    let secret = json!({"github_token":"github", "operator_note":"keep"});
    let settings = json!({});
    let http = RefreshHttp {
        status: 200,
        response: json!({"token":"copilot", "expires_at":2_000_000_000_i64}),
    };
    let result = run(crate::CopilotCliChannel
        .refresh(&secret, &settings, &http)
        .unwrap())
    .unwrap();
    assert_eq!(result.refresh_token, RefreshTokenStatus::NotApplicable);
    assert_eq!(result.secret["copilot_token"], "copilot");
    assert_eq!(result.secret["github_token"], "github");
    assert_eq!(result.secret["operator_note"], "keep");
}

fn token_response(id: &str, refresh: Option<Value>, access: Option<&str>) -> Value {
    let camel_case = matches!(id, "cline" | "kiro" | "workbuddy");
    let mut token = if camel_case {
        json!({"expiresIn":3600})
    } else {
        json!({"expires_in":3600})
    };
    if let Some(access) = access {
        token[if camel_case {
            "accessToken"
        } else {
            "access_token"
        }] = json!(access);
    }
    if let Some(refresh) = refresh {
        token[if camel_case {
            "refreshToken"
        } else {
            "refresh_token"
        }] = refresh;
    }
    match id {
        "cline" => json!({"success":true, "data":token}),
        "workbuddy" => json!({"code":0, "data":token}),
        _ => token,
    }
}

struct RefreshHttp {
    status: u16,
    response: Value,
}

impl SimpleHttp for RefreshHttp {
    fn send<'a>(
        &'a self,
        _request: http::Request<Bytes>,
    ) -> BoxFuture<'a, Result<http::Response<Bytes>, ChannelError>> {
        Box::pin(async move {
            Ok(http::Response::builder()
                .status(self.status)
                .body(Bytes::from(self.response.to_string()))
                .unwrap())
        })
    }
}

fn run(
    future: impl Future<Output = Result<RefreshResult, ChannelError>>,
) -> Result<RefreshResult, ChannelError> {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("mock refresh should complete immediately"),
    }
}
