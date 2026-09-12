use base64::Engine as _;
use bytes::Bytes;
use gproxy_channel_api::{
    AuthCodeExchangeCtx, AuthCodeStartCtx, BoxFuture, Channel, ChannelError, CredentialKind,
    DevicePoll, DevicePollCtx, DeviceStartCtx, PrepareCtx, RefreshResult, RefreshTokenStatus,
    SimpleHttp,
};
use gproxy_protocol::{Operation, OperationKey, WireFamily};
use http::{HeaderMap, Method};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::Mutex;

#[test]
fn model_discovery_uses_upstream_cli_identity() {
    let settings = json!({});
    let secret = json!({"access_token":"token"});
    let headers = HeaderMap::new();
    let body = Bytes::new();
    let prepared = super::super::CodexChannel
        .prepare(PrepareCtx {
            session_id: None,
            key: OperationKey::family(Operation::ListModels, WireFamily::OpenAi),
            stream: false,
            method: &Method::GET,
            path: "/v1/models",
            query: None,
            headers: &headers,
            body: &body,
            upstream_model: "",
            provider_settings: &settings,
            secret: &secret,
        })
        .unwrap();

    assert_eq!(
        prepared.request.uri(),
        "https://chatgpt.com/backend-api/codex/models?client_version=0.154.0"
    );
    assert_eq!(prepared.request.headers()["version"], "0.154.0");
    assert_eq!(
        prepared.request.headers()[http::header::USER_AGENT],
        "codex_cli_rs/0.154.0 (Debian 13.0.0; x86_64) xterm-256color"
    );
    let defaults = super::super::CodexChannel.client_fingerprint().unwrap();
    assert_eq!(prepared.profile, Some(defaults.profile));
    for (name, value) in &defaults.headers {
        assert_eq!(prepared.request.headers()[name], *value, "{name}");
    }
}

#[test]
fn default_fingerprint_keeps_caller_client_identity() {
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("user-agent", "my-codex-client/1.0"),
        ("originator", "my-codex-client"),
        ("version", "0.153.2"),
        ("x-client-request-id", "caller-request"),
    ] {
        headers.insert(name, value.parse().unwrap());
    }
    let original = headers.clone();
    super::super::auth::apply_headers(&mut headers, &json!({"access_token":"token"}), "session")
        .unwrap();
    for (name, value) in &original {
        assert_eq!(headers[name], *value, "{name}");
    }
}

#[test]
fn login_secret_retains_jwt_account_identity() {
    let claims = json!({
        "email": "user@example.com",
        "https://api.openai.com/auth": {
            "chatgpt_account_id": "acct-1",
            "chatgpt_account_is_fedramp": true
        }
    });
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.to_string());
    let secret = super::super::auth::login_secret(&json!({
        "access_token": "access",
        "refresh_token": "refresh",
        "id_token": format!("header.{payload}.signature")
    }))
    .unwrap();

    assert_eq!(secret["user_email"], "user@example.com");
    assert_eq!(secret["account_id"], "acct-1");
    assert_eq!(secret["chatgpt_account_is_fedramp"], true);
}

#[test]
fn imported_tokens_use_access_jwt_expiry_without_a_stored_deadline() {
    let access = jwt(json!({"exp": 2_000_000_000_i64}));
    for stored in [
        None,
        Some(Value::Null),
        Some(json!(0)),
        Some(json!(-1)),
        Some(json!("bad")),
        Some(json!(true)),
        Some(json!(1.5)),
    ] {
        let mut secret = json!({"access_token":access,"refresh_token":"refresh"});
        if let Some(stored) = stored {
            secret["expires_at_ms"] = stored;
        }
        assert_eq!(
            super::super::auth::refresh_due(&secret),
            Some(1_999_999_700)
        );
    }
    assert_eq!(
        super::super::auth::refresh_due(&json!({
            "access_token":jwt(json!({"exp": 1_000})),"refresh_token":"refresh"
        })),
        Some(700)
    );
    assert_eq!(
        super::super::auth::refresh_due(&json!({
            "access_token":access,"refresh_token":"refresh","expires_at_ms":3_000_000_i64
        })),
        Some(2_700)
    );
}

#[test]
fn automatic_refresh_requires_a_saved_refresh_token_and_never_uses_id_token_expiry() {
    for refresh in [
        None,
        Some(Value::Null),
        Some(json!("")),
        Some(json!(" \t\n ")),
        Some(json!(7)),
        Some(json!({})),
    ] {
        let mut secret = json!({"access_token":jwt(json!({"exp":1})),"expires_at_ms":1});
        if let Some(refresh) = refresh {
            secret["refresh_token"] = refresh;
        }
        assert_eq!(super::super::auth::refresh_due(&secret), None);
    }
    assert_eq!(
        super::super::auth::refresh_due(&json!({"refresh_token":"refresh"})),
        Some(i64::MIN)
    );
    assert_eq!(
        super::super::auth::refresh_due(&json!({
            "access_token":"opaque", "refresh_token":"refresh", "id_token":jwt(json!({"exp":1}))
        })),
        None
    );
}

#[test]
fn invalid_access_jwt_expiry_stays_unknown() {
    for claims in [
        json!({}),
        json!({"exp":null}),
        json!({"exp":-1}),
        json!({"exp":"123"}),
        json!({"exp":true}),
        json!({"exp":1.5}),
        json!({"exp":u64::MAX}),
        json!({"exp":i64::MAX}),
        json!([]),
    ] {
        let secret = json!({"access_token":jwt(claims),"refresh_token":"refresh"});
        assert_eq!(super::super::auth::refresh_due(&secret), None);
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"exp":1}"#);
    for access in [String::new(), " \t ".into()] {
        assert_eq!(
            super::super::auth::refresh_due(
                &json!({"access_token":access,"refresh_token":"refresh"})
            ),
            Some(i64::MIN)
        );
    }
    for access in [
        "opaque".into(),
        format!("header.{payload}"),
        format!("header.{payload}.signature.extra"),
        format!(".{payload}.signature"),
        format!("header.{payload}."),
        "header.invalid!.signature".into(),
    ] {
        assert_eq!(
            super::super::auth::refresh_due(
                &json!({"access_token":access,"refresh_token":"refresh"})
            ),
            None
        );
    }
}

#[test]
fn returned_expiry_uses_positive_lifetime_then_new_access_jwt() {
    let access = jwt(json!({"exp":2_000_000_000_i64}));
    for expires in [
        None,
        Some(Value::Null),
        Some(json!(0)),
        Some(json!(-10)),
        Some(json!("3600")),
        Some(json!(true)),
        Some(json!(1.5)),
        Some(json!(i64::MAX)),
    ] {
        let mut reply = json!({"access_token":access,"refresh_token":"rotated"});
        if let Some(expires) = expires {
            reply["expires_in"] = expires;
        }
        let refreshed = refresh(
            &json!({"access_token":"old","refresh_token":"saved","expires_at_ms":1}),
            reply,
        );
        assert_eq!(refreshed.secret["expires_at_ms"], 2_000_000_000_000_i64);
        assert_eq!(refreshed.secret["refresh_token"], "rotated");
        assert!(refreshed.secret["token_received_at_ms"].as_i64().is_some());
    }
    let refreshed = refresh(
        &json!({"refresh_token":"saved"}),
        json!({"access_token":access,"expires_in":3600}),
    );
    let received = refreshed.secret["token_received_at_ms"].as_i64().unwrap();
    assert_eq!(
        refreshed.secret["expires_at_ms"].as_i64().unwrap(),
        received + 3_600_000
    );
    let due = super::super::auth::refresh_due(&refreshed.secret).unwrap();
    assert!((received / 1_000 + 3_300..=received / 1_000 + 3_301).contains(&due));
}

#[test]
fn missing_or_invalid_returned_expiry_clears_old_deadlines_and_keeps_rotation() {
    for expires in [
        None,
        Some(Value::Null),
        Some(json!(0)),
        Some(json!(-1)),
        Some(json!("bad")),
        Some(json!(i64::MAX)),
    ] {
        let mut reply = json!({"access_token":"new-opaque","refresh_token":"rotated"});
        if let Some(expires) = expires {
            reply["expires_in"] = expires;
        }
        let refreshed = refresh(
            &json!({
                "access_token":"old", "refresh_token":"saved", "expires_at_ms":1,
                "token_received_at_ms":1,"refresh_at_ms":1,"expiry_unknown":false
            }),
            reply,
        );
        assert_eq!(refreshed.secret["access_token"], "new-opaque");
        assert_eq!(refreshed.secret["refresh_token"], "rotated");
        assert!(refreshed.secret.get("expires_at_ms").is_none());
        assert_eq!(super::super::auth::refresh_due(&refreshed.secret), None);
        assert!(super::super::CodexChannel.can_refresh(&refreshed.secret));
    }
}

#[test]
fn newly_received_short_lived_tokens_are_not_immediately_refreshed_again() {
    for lifetime in [1, 2, 60, 300, 600] {
        let refreshed = refresh(
            &json!({"refresh_token":"saved"}),
            json!({"access_token":"new","expires_in":lifetime}),
        );
        let received = refreshed.secret["token_received_at_ms"].as_i64().unwrap();
        let expiry = refreshed.secret["expires_at_ms"].as_i64().unwrap();
        let due = super::super::auth::refresh_due(&refreshed.secret).unwrap();
        assert!(due > received / 1_000, "lifetime={lifetime}");
        assert!(due <= expiry / 1_000, "lifetime={lifetime}");
    }
}

#[test]
fn successive_refreshes_send_the_rotated_key_and_exact_codex_grant() {
    let http = RefreshHttp::new(vec![
        response(
            200,
            json!({"access_token":"access-1","refresh_token":"rotated"}),
        ),
        response(200, json!({"access_token":"access-2"})),
    ]);
    let original =
        json!({"access_token":"before","refresh_token":" saved ","quota_api_key":"quota"});
    let first = super::ready(super::super::auth::refresh(&original, &http)).unwrap();
    let second = super::ready(super::super::auth::refresh(&first.secret, &http)).unwrap();
    assert_eq!(first.refresh_token, RefreshTokenStatus::Updated);
    assert_eq!(second.refresh_token, RefreshTokenStatus::NotReturned);
    assert_eq!(second.secret["refresh_token"], "rotated");
    assert_eq!(second.secret["quota_api_key"], "quota");
    for (request, token) in http
        .requests
        .lock()
        .unwrap()
        .iter()
        .zip(["saved", "rotated"])
    {
        assert_eq!(request.method(), Method::POST);
        assert_eq!(request.uri(), super::super::auth::TOKEN_URL);
        assert_eq!(
            request.headers()[http::header::CONTENT_TYPE],
            "application/json"
        );
        assert_eq!(request.headers()[http::header::ACCEPT], "application/json");
        assert_eq!(
            request
                .extensions()
                .get::<gproxy_channel_api::ClientProfile>(),
            Some(&super::super::profile::CLIENT_PROFILE)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(request.body()).unwrap(),
            json!({
                "client_id":super::super::auth::CLIENT_ID,"grant_type":"refresh_token","refresh_token":token
            })
        );
    }
}

#[test]
fn all_refresh_failures_hide_body_and_transport_secrets() {
    for reply in [
        response(
            400,
            json!({"error":"refresh_token=private-value","access_token":"private-value"}),
        ),
        Ok(http::Response::new(Bytes::from_static(
            b"{ private-value broken-json",
        ))),
        Err(ChannelError::Refresh(
            "proxy=https://user:private-value@example.test".into(),
        )),
    ] {
        let http = RefreshHttp::new(vec![reply]);
        let error = match super::ready(super::super::auth::refresh(
            &json!({"refresh_token":"saved"}),
            &http,
        )) {
            Ok(_) => panic!("refresh unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(!error.to_string().contains("private-value"));
    }
}

#[test]
fn fresh_id_token_clears_obsolete_email_and_fedramp_routing() {
    let old_id = jwt(
        json!({"email":"old@example.com","https://api.openai.com/auth":{"chatgpt_account_id":"acct-old","chatgpt_account_is_fedramp":true}}),
    );
    let original = json!({"access_token":"before","refresh_token":"saved","id_token":old_id,"account_id":"acct-old","user_email":"old@example.com","chatgpt_account_is_fedramp":true});
    let next_id = jwt(json!({"https://api.openai.com/auth":{"chatgpt_account_id":"acct-new"}}));
    let refreshed = refresh(
        &original,
        json!({"access_token":"after","refresh_token":"rotated","id_token":next_id}),
    );
    assert_eq!(refreshed.secret["account_id"], "acct-new");
    assert_eq!(refreshed.secret["id_token"], next_id);
    assert!(refreshed.secret.get("user_email").is_none());
    assert_eq!(refreshed.secret["chatgpt_account_is_fedramp"], false);
    let mut headers = HeaderMap::new();
    headers.insert("x-openai-fedramp", "true".parse().unwrap());
    super::super::auth::apply_headers(&mut headers, &refreshed.secret, "session").unwrap();
    assert_eq!(headers["chatgpt-account-id"], "acct-new");
    assert!(headers.get("x-openai-fedramp").is_none());

    let refreshed = refresh(
        &original,
        json!({"access_token":"after","id_token":jwt(json!({
            "email":null,"https://api.openai.com/profile":{"email":" new@example.com "}
        }))}),
    );
    assert_eq!(refreshed.secret["account_id"], "acct-old");
    assert_eq!(refreshed.secret["user_email"], "new@example.com");
    assert_eq!(refreshed.secret["chatgpt_account_is_fedramp"], false);
}

#[test]
fn absent_or_malformed_id_tokens_do_not_replace_valid_stored_identity() {
    let original = json!({"access_token":"before","refresh_token":"saved","id_token":jwt(json!({"email":"old@example.com"})),"account_id":"acct-old","user_email":"old@example.com","chatgpt_account_is_fedramp":true});
    for id in [
        None,
        Some(Value::Null),
        Some(json!("")),
        Some(json!(" \t ")),
        Some(json!("malformed")),
        Some(json!(jwt(json!([])))),
        Some(json!(7)),
    ] {
        let mut reply = json!({"access_token":"after","refresh_token":"rotated"});
        if let Some(id) = id {
            reply["id_token"] = id;
        }
        let refreshed = refresh(&original, reply);
        for name in [
            "id_token",
            "account_id",
            "user_email",
            "chatgpt_account_is_fedramp",
        ] {
            assert_eq!(refreshed.secret[name], original[name]);
        }
        assert_eq!(refreshed.secret["refresh_token"], "rotated");
    }
}

#[test]
fn authcode_login_and_first_refresh_share_identity_expiry_and_oauth_profile() {
    let channel = super::super::CodexChannel;
    let settings = json!({});
    let params = json!({});
    let access = jwt(json!({"exp":2_000_000_000_i64}));
    let http = RefreshHttp::new(vec![
        response(
            200,
            json!({
                "access_token":access,"refresh_token":"login-refresh",
                "id_token":jwt(json!({
                    "email":"user@example.com",
                    "https://api.openai.com/auth":{"chatgpt_account_id":"account"}
                }))
            }),
        ),
        response(
            200,
            json!({"access_token":"refreshed-access","refresh_token":"next-refresh","expires_in":60}),
        ),
    ]);
    let login = channel.login().unwrap();
    let started = super::ready(login.adapter.authcode_start(
        &http,
        AuthCodeStartCtx {
            provider_settings: &settings,
            params: &params,
            redirect_uri: "http://localhost:1455/auth/callback",
            state: "state+with space",
            pkce_challenge: "challenge",
        },
    ))
    .unwrap()
    .unwrap();
    let authorize: http::Uri = started.authorize_url.parse().unwrap();
    assert_eq!(authorize.host(), Some("auth.openai.com"));
    assert_eq!(authorize.path(), "/oauth/authorize");
    let query: std::collections::BTreeMap<String, String> =
        serde_urlencoded::from_str(authorize.query().unwrap()).unwrap();
    assert_eq!(query["state"], "state+with space");
    assert_eq!(query["client_id"], super::super::auth::CLIENT_ID);
    assert_eq!(query["code_challenge"], "challenge");
    assert_eq!(query["code_challenge_method"], "S256");
    assert!(
        query["scope"]
            .split_whitespace()
            .any(|scope| scope == "offline_access")
    );
    assert!(http.requests.lock().unwrap().is_empty());

    let acquired = super::ready(login.adapter.authcode_exchange(
        &http,
        AuthCodeExchangeCtx {
            provider_settings: &settings,
            code: "authorization+code",
            verifier: "pkce-verifier",
            redirect_uri: &started.redirect_uri,
            extra: started.extra.as_ref(),
        },
    ))
    .unwrap();
    assert_eq!(acquired.kind, CredentialKind::Oauth);
    assert_eq!(acquired.secret["expires_at_ms"], 2_000_000_000_000_i64);
    assert_eq!(acquired.secret["account_id"], "account");
    assert_eq!(acquired.secret["user_email"], "user@example.com");
    let refreshed =
        super::ready(channel.refresh(&acquired.secret, &settings, &http).unwrap()).unwrap();
    assert_eq!(refreshed.secret["access_token"], "refreshed-access");
    assert_eq!(refreshed.secret["refresh_token"], "next-refresh");
    assert_eq!(refreshed.secret["account_id"], "account");
    let received = refreshed.secret["token_received_at_ms"].as_i64().unwrap();
    assert!(channel.refresh_due(&refreshed.secret).unwrap() > received / 1_000);

    let requests = http.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for request in requests.iter() {
        assert_oauth_profile(request);
        assert_eq!(request.uri(), super::super::auth::TOKEN_URL);
    }
    assert_eq!(
        requests[0].headers()[http::header::CONTENT_TYPE],
        "application/x-www-form-urlencoded"
    );
    let form: std::collections::BTreeMap<String, String> =
        serde_urlencoded::from_bytes(requests[0].body()).unwrap();
    assert_eq!(form["grant_type"], "authorization_code");
    assert_eq!(form["code"], "authorization+code");
    assert_eq!(form["code_verifier"], "pkce-verifier");
    assert_eq!(form["redirect_uri"], started.redirect_uri);
    assert_eq!(form["client_id"], super::super::auth::CLIENT_ID);
    assert_eq!(
        serde_json::from_slice::<Value>(requests[1].body()).unwrap()["refresh_token"],
        "login-refresh"
    );
}

#[test]
fn device_login_poll_exchange_and_first_refresh_use_the_codex_oauth_profile() {
    let channel = super::super::CodexChannel;
    let settings = json!({});
    let params = json!({});
    let http = RefreshHttp::new(vec![
        response(
            200,
            json!({"device_auth_id":"device-id","user_code":"ABCD-EFGH","interval":1}),
        ),
        response(403, json!({"error":"pending"})),
        response(404, json!({"error":"pending"})),
        response(
            200,
            json!({"authorization_code":"device-code","code_verifier":"device-verifier"}),
        ),
        response(
            200,
            json!({"access_token":"access","refresh_token":"device-refresh","expires_in":2}),
        ),
        response(
            200,
            json!({"access_token":"refreshed-access","refresh_token":"next-refresh"}),
        ),
    ]);
    let login = channel.login().unwrap();
    let started = super::ready(login.adapter.device_start(
        &http,
        DeviceStartCtx {
            provider_settings: &settings,
            params: &params,
        },
    ))
    .unwrap();
    assert_eq!(started.user_code, "ABCD-EFGH");
    assert_eq!(
        started.verification_uri,
        "https://auth.openai.com/codex/device"
    );
    assert_eq!(started.interval_secs, 1);
    for _ in 0..2 {
        assert!(matches!(
            super::ready(login.adapter.device_poll(
                &http,
                DevicePollCtx {
                    provider_settings: &settings,
                    device_code: &started.device_code,
                }
            ))
            .unwrap(),
            DevicePoll::Pending
        ));
    }
    let DevicePoll::Ready(acquired) = super::ready(login.adapter.device_poll(
        &http,
        DevicePollCtx {
            provider_settings: &settings,
            device_code: &started.device_code,
        },
    ))
    .unwrap() else {
        panic!("device login did not complete");
    };
    assert_eq!(acquired.kind, CredentialKind::Oauth);
    let received = acquired.secret["token_received_at_ms"].as_i64().unwrap();
    assert!(channel.refresh_due(&acquired.secret).unwrap() > received / 1_000);
    let refreshed =
        super::ready(channel.refresh(&acquired.secret, &settings, &http).unwrap()).unwrap();
    assert_eq!(refreshed.secret["access_token"], "refreshed-access");
    assert_eq!(refreshed.secret["refresh_token"], "next-refresh");
    assert!(refreshed.secret.get("expires_at_ms").is_none());

    let requests = http.requests.lock().unwrap();
    assert_eq!(requests.len(), 6);
    for request in requests.iter() {
        assert_oauth_profile(request);
    }
    assert_eq!(
        requests[0].uri(),
        "https://auth.openai.com/api/accounts/deviceauth/usercode"
    );
    assert_eq!(
        serde_json::from_slice::<Value>(requests[0].body()).unwrap(),
        json!({"client_id":super::super::auth::CLIENT_ID})
    );
    for request in &requests[1..4] {
        assert_eq!(
            request.uri(),
            "https://auth.openai.com/api/accounts/deviceauth/token"
        );
        assert_eq!(
            request.headers()[http::header::CONTENT_TYPE],
            "application/json"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(request.body()).unwrap(),
            json!({
                "device_auth_id":"device-id","user_code":"ABCD-EFGH"
            })
        );
    }
    assert_eq!(requests[4].uri(), super::super::auth::TOKEN_URL);
    let form: std::collections::BTreeMap<String, String> =
        serde_urlencoded::from_bytes(requests[4].body()).unwrap();
    assert_eq!(form["grant_type"], "authorization_code");
    assert_eq!(form["code"], "device-code");
    assert_eq!(form["code_verifier"], "device-verifier");
    assert_eq!(
        form["redirect_uri"],
        "https://auth.openai.com/deviceauth/callback"
    );
    assert_eq!(form["client_id"], super::super::auth::CLIENT_ID);
    assert_eq!(requests[5].uri(), super::super::auth::TOKEN_URL);
    assert_eq!(
        serde_json::from_slice::<Value>(requests[5].body()).unwrap()["refresh_token"],
        "device-refresh"
    );
}

#[test]
fn every_login_network_stage_hides_transport_secrets() {
    let channel = super::super::CodexChannel;
    let settings = json!({});
    let params = json!({});
    let login = channel.login().unwrap();
    for stage in ["authcode", "device-start", "device-poll", "device-exchange"] {
        let mut replies = Vec::new();
        if stage == "device-exchange" {
            replies.push(response(
                200,
                json!({
                    "authorization_code":"device-code","code_verifier":"device-verifier"
                }),
            ));
        }
        replies.push(Err(ChannelError::Refresh(
            "proxy=https://user:private-value@example.test".into(),
        )));
        let http = RefreshHttp::new(replies);
        let result = match stage {
            "authcode" => super::ready(login.adapter.authcode_exchange(
                &http,
                AuthCodeExchangeCtx {
                    provider_settings: &settings,
                    code: "code",
                    verifier: "verifier",
                    redirect_uri: "http://localhost:1455/auth/callback",
                    extra: None,
                },
            ))
            .map(|_| ()),
            "device-start" => super::ready(login.adapter.device_start(
                &http,
                DeviceStartCtx {
                    provider_settings: &settings,
                    params: &params,
                },
            ))
            .map(|_| ()),
            _ => super::ready(login.adapter.device_poll(
                &http,
                DevicePollCtx {
                    provider_settings: &settings,
                    device_code: r#"{"device_auth_id":"device-id","user_code":"ABCD-EFGH"}"#,
                },
            ))
            .map(|_| ()),
        };
        let Err(error) = result else {
            panic!("expected transport failure at stage={stage}");
        };
        assert!(matches!(&error, ChannelError::Login(_)));
        assert!(
            !error.to_string().contains("private-value"),
            "stage={stage}"
        );
    }
}

fn assert_oauth_profile(request: &http::Request<Bytes>) {
    assert_eq!(request.method(), Method::POST);
    assert_eq!(request.headers()[http::header::ACCEPT], "application/json");
    assert_eq!(
        request
            .extensions()
            .get::<gproxy_channel_api::ClientProfile>(),
        Some(&super::super::profile::CLIENT_PROFILE)
    );
}

fn jwt(claims: Value) -> String {
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.to_string());
    format!("header.{payload}.signature")
}

fn refresh(secret: &Value, reply: Value) -> RefreshResult {
    super::ready(super::super::auth::refresh(
        secret,
        &RefreshHttp::new(vec![response(200, reply)]),
    ))
    .unwrap()
}

fn response(status: u16, body: Value) -> Result<http::Response<Bytes>, ChannelError> {
    Ok(http::Response::builder()
        .status(status)
        .body(Bytes::from(body.to_string()))
        .unwrap())
}

struct RefreshHttp {
    replies: Mutex<VecDeque<Result<http::Response<Bytes>, ChannelError>>>,
    requests: Mutex<Vec<http::Request<Bytes>>>,
}

impl RefreshHttp {
    fn new(replies: Vec<Result<http::Response<Bytes>, ChannelError>>) -> Self {
        Self {
            replies: Mutex::new(replies.into()),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl SimpleHttp for RefreshHttp {
    fn send<'a>(
        &'a self,
        request: http::Request<Bytes>,
    ) -> BoxFuture<'a, Result<http::Response<Bytes>, ChannelError>> {
        self.requests.lock().unwrap().push(request);
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected token request");
        Box::pin(async move { reply })
    }
}
