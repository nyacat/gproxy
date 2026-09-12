use base64::Engine as _;
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use bytes::Bytes;
use gproxy_channel_api::{BoxFuture, ChannelError, SimpleHttp};
use http::header::{AUTHORIZATION, HeaderName, HeaderValue};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub(super) const DEFAULT_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
pub(super) const ORIGINATOR: &str = "codex_cli_rs";
// Catalog compatibility is keyed by Codex identity, not by gproxy's release version.
// Release baseline: https://developers.openai.com/codex/changelog/ (2026-09-11).
pub(super) const CODEX_CLI_VERSION: &str = "0.154.0";
// Keep the existing preset's simulated desktop environment in Codex's UA format.
const CLI_ENVIRONMENT: &str = "(Debian 13.0.0; x86_64) xterm-256color";

pub(super) const TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
pub(super) use gproxy_channel_api::CODEX_OAUTH_CLIENT_ID as CLIENT_ID;
const EXPIRY_SKEW_SECONDS: i64 = 5 * 60;

pub(super) fn fallback_user_agent() -> String {
    format!("{ORIGINATOR}/{CODEX_CLI_VERSION} {CLI_ENVIRONMENT}")
}

pub(super) fn fingerprint_headers() -> http::HeaderMap {
    http::HeaderMap::from_iter([
        (
            http::header::USER_AGENT,
            HeaderValue::from_str(&fallback_user_agent()).expect("built-in user-agent is valid"),
        ),
        (
            HeaderName::from_static("originator"),
            HeaderValue::from_static(ORIGINATOR),
        ),
        (
            HeaderName::from_static("version"),
            HeaderValue::from_static(CODEX_CLI_VERSION),
        ),
    ])
}

pub(super) fn access_token(secret: &Value) -> Result<&str, ChannelError> {
    secret_string(secret, "access_token")
        .ok_or_else(|| ChannelError::Secret("access_token missing".into()))
}

pub(super) fn account_id(secret: &Value) -> Option<&str> {
    secret_string(secret, "account_id")
}

pub(super) fn refresh_due(secret: &Value) -> Option<i64> {
    secret_string(secret, "refresh_token")?;
    let Some(access) = secret_string(secret, "access_token") else {
        return Some(i64::MIN);
    };
    let expires_at_ms = secret
        .get("expires_at_ms")
        .and_then(Value::as_i64)
        .filter(|expires| *expires > 0)
        .or_else(|| access_expiry(access))?;
    Some(crate::shared::oauth_expiry::refresh_due(
        secret,
        expires_at_ms,
        EXPIRY_SKEW_SECONDS * 1_000,
    ))
}

pub(super) fn refresh<'a>(
    secret: &'a Value,
    http: &'a dyn SimpleHttp,
) -> BoxFuture<'a, Result<gproxy_channel_api::RefreshResult, ChannelError>> {
    let request = (|| {
        let refresh_token = secret_string(secret, "refresh_token")
            .ok_or_else(|| ChannelError::Refresh("refresh_token missing".into()))?;
        let body = serde_json::to_vec(&serde_json::json!({
            "client_id": CLIENT_ID,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }))
        .map_err(|_| ChannelError::Refresh("invalid refresh request".into()))?;
        let mut request = http::Request::post(TOKEN_URL)
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(http::header::ACCEPT, "application/json")
            .body(Bytes::from(body))
            .map_err(|_| ChannelError::Refresh("invalid token endpoint request".into()))?;
        request
            .extensions_mut()
            .insert(super::profile::CLIENT_PROFILE.clone());
        Ok(request)
    })();
    let request = match request {
        Ok(request) => request,
        Err(error) => return Box::pin(async move { Err(error) }),
    };
    let send = http.send(request);
    Box::pin(async move {
        let response = send
            .await
            .map_err(|_| ChannelError::Refresh("token endpoint request failed".into()))?;
        if !response.status().is_success() {
            return Err(ChannelError::Refresh(format!(
                "token endpoint returned {}",
                response.status()
            )));
        }
        let token: Value = serde_json::from_slice(response.body())
            .map_err(|_| ChannelError::Refresh("invalid token response".into()))?;
        rotate(secret, &token)
    })
}

pub(super) fn session_id(secret: &Value, headers: &http::HeaderMap) -> String {
    if let Some(value) = headers
        .get("session-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return value.to_owned();
    }
    let window = unix_now_ms() / (20 * 60 * 1_000);
    let token = secret_string(secret, "access_token").unwrap_or_default();
    let digest = Sha256::digest(format!("codex-session:{token}:{window}"));
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{}-{}-{}-{}-{}",
        hex(&bytes[..4]),
        hex(&bytes[4..6]),
        hex(&bytes[6..8]),
        hex(&bytes[8..10]),
        hex(&bytes[10..])
    )
}

pub(super) fn apply_headers(
    headers: &mut http::HeaderMap,
    secret: &Value,
    session_id: &str,
) -> Result<(), ChannelError> {
    insert(
        headers,
        AUTHORIZATION,
        &format!("Bearer {}", access_token(secret)?),
    )?;
    for (name, value) in &fingerprint_headers() {
        headers.entry(name).or_insert_with(|| value.clone());
    }
    insert(headers, HeaderName::from_static("session-id"), session_id)?;
    if !headers.contains_key("x-client-request-id") {
        insert(
            headers,
            HeaderName::from_static("x-client-request-id"),
            session_id,
        )?;
    }
    if let Some(account_id) = account_id(secret) {
        insert(
            headers,
            HeaderName::from_static("chatgpt-account-id"),
            account_id,
        )?;
    }
    headers.remove("x-openai-fedramp");
    if secret
        .get("chatgpt_account_is_fedramp")
        .and_then(Value::as_bool)
        == Some(true)
    {
        headers.insert(
            HeaderName::from_static("x-openai-fedramp"),
            HeaderValue::from_static("true"),
        );
    }
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Ok(())
}

fn rotate(
    secret: &Value,
    token: &Value,
) -> Result<gproxy_channel_api::RefreshResult, ChannelError> {
    let access = secret_string(token, "access_token")
        .ok_or_else(|| ChannelError::Refresh("token response missing access_token".into()))?;
    let received_at_ms = unix_now_ms();
    let expires_at_ms = crate::shared::oauth_expiry::from_lifetime(
        received_at_ms,
        token.get("expires_in").and_then(Value::as_i64),
    )
    .or_else(|| access_expiry(access));
    let mut output = secret.clone();
    let object = output
        .as_object_mut()
        .ok_or_else(|| ChannelError::Refresh("secret must be a JSON object".into()))?;
    object.insert("access_token".into(), Value::String(access.into()));
    if let Some(id_token) = secret_string(token, "id_token")
        && let Some(claims) = jwt_claims(id_token)
    {
        object.insert("id_token".into(), Value::String(id_token.into()));
        if let Some(account_id) = claims
            .get("https://api.openai.com/auth")
            .and_then(|auth| secret_string(auth, "chatgpt_account_id"))
        {
            object.insert("account_id".into(), Value::String(account_id.into()));
        }
        if let Some(email) = secret_string(&claims, "email").or_else(|| {
            claims
                .get("https://api.openai.com/profile")
                .and_then(|profile| secret_string(profile, "email"))
        }) {
            object.insert("user_email".into(), Value::String(email.into()));
        } else {
            object.remove("user_email");
        }
        // A fresh ID token is authoritative for this optional claim. Keeping
        // an old true value would keep sending the FedRAMP routing header.
        let fedramp = claims
            .pointer("/https:~1~1api.openai.com~1auth/chatgpt_account_is_fedramp")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        object.insert("chatgpt_account_is_fedramp".into(), Value::Bool(fedramp));
    }
    crate::shared::oauth_expiry::apply(object, received_at_ms, expires_at_ms);
    crate::shared::refresh::oauth(output, token.get("refresh_token").and_then(Value::as_str))
}

pub(super) fn login_secret(token: &Value) -> Result<Value, ChannelError> {
    rotate(&serde_json::json!({}), token)
        .map(|result| result.secret)
        .map_err(|_| ChannelError::Login("invalid token response".into()))
}

fn access_expiry(token: &str) -> Option<i64> {
    jwt_claims(token)?
        .get("exp")?
        .as_i64()
        .filter(|seconds| *seconds >= 0)?
        .checked_mul(1_000)
}

fn jwt_claims(token: &str) -> Option<Value> {
    // These claims only supply metadata and refresh timing. The upstream
    // authenticates the bearer token; this is not a JWT verification boundary.
    let mut segments = token.split('.');
    if segments.next()?.is_empty() {
        return None;
    }
    let payload = segments.next()?;
    if segments.next()?.is_empty() || segments.next().is_some() {
        return None;
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| URL_SAFE.decode(payload))
        .ok()?;
    let claims: Value = serde_json::from_slice(&decoded).ok()?;
    claims.is_object().then_some(claims)
}

fn secret_string<'a>(value: &'a Value, name: &str) -> Option<&'a str> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn insert(
    headers: &mut http::HeaderMap,
    name: HeaderName,
    value: &str,
) -> Result<(), ChannelError> {
    headers.insert(
        name,
        HeaderValue::from_str(value)
            .map_err(|error| ChannelError::Prepare(format!("invalid header: {error}")))?,
    );
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String succeeds");
    }
    output
}

fn unix_now_ms() -> i64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .expect("system clock is before the Unix epoch")
        .as_millis()
        .try_into()
        .expect("Unix milliseconds fit in i64")
}
