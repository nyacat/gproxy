use base64::Engine as _;
use bytes::Bytes;
use gproxy_channel_api::{
    BoxFuture, ChannelError, ClientProfile, ClientProfilePreset, RequiredClientProfile, SimpleHttp,
};
use http::header::{ACCEPT, CONTENT_TYPE, USER_AGENT};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{account, auth};

const COOKIE_MAX_ATTEMPTS: u32 = 5;
const SUBSCRIPTION_CAPABILITIES: &[&str] = &[
    "claude_pro",
    "claude_max",
    "claude_team",
    "claude_enterprise",
];
static BROWSER_PROFILE: ClientProfile = ClientProfile::preset(ClientProfilePreset::Chrome148);

pub(super) fn exchange<'a>(
    http: &'a dyn SimpleHttp,
    input: &'a str,
) -> BoxFuture<'a, Result<Value, ChannelError>> {
    exchange_for_organization(http, input, None)
}

fn exchange_for_organization<'a>(
    http: &'a dyn SimpleHttp,
    input: &'a str,
    organization_uuid: Option<&'a str>,
) -> BoxFuture<'a, Result<Value, ChannelError>> {
    Box::pin(async move {
        let cookie = crate::shared::claude::cookie::normalize(input)
            .ok_or_else(|| ChannelError::Login("cookie is missing sessionKey".into()))?;
        let identity = discover_identity(http, &cookie, organization_uuid).await?;
        let (verifier, challenge, state) = pkce()?;
        let code = authorize(
            http,
            &cookie,
            &identity.organization_uuid,
            &state,
            &challenge,
        )
        .await?;
        let mut secret = token_exchange(http, &verifier, &state, &code).await?;
        secret["cookie"] = Value::String(cookie);
        if secret.get("organization_uuid").is_none() {
            secret["organization_uuid"] = Value::String(identity.organization_uuid);
        }
        if let Some(account_uuid) = identity.account_uuid
            && secret.get("account_uuid").is_none()
        {
            secret["account_uuid"] = Value::String(account_uuid);
        }
        account::enrich(http, &mut secret).await;
        if organization_uuid.is_some()
            && secret.get("organization_uuid").and_then(Value::as_str) != organization_uuid
        {
            return Err(ChannelError::Login(
                "cookie refresh returned a different organization".into(),
            ));
        }
        ensure_device_id(&mut secret);
        Ok(secret)
    })
}

pub(super) fn refresh<'a>(
    secret: &'a Value,
    http: &'a dyn SimpleHttp,
) -> BoxFuture<'a, Result<gproxy_channel_api::RefreshResult, ChannelError>> {
    let cookie = secret
        .get("cookie")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let organization_uuid = secret
        .get("organization_uuid")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    Box::pin(async move {
        let cookie = cookie.ok_or_else(|| ChannelError::Refresh("cookie missing".into()))?;
        let minted = exchange_for_organization(http, cookie, organization_uuid)
            .await
            .map_err(|error| ChannelError::Refresh(error.to_string()))?;
        let returned = minted.get("refresh_token").and_then(Value::as_str);
        let replacement = overlay_without_refresh_token(secret, &minted);
        crate::shared::refresh::oauth(replacement, returned)
    })
}

struct CookieIdentity {
    account_uuid: Option<String>,
    organization_uuid: String,
}

async fn discover_identity(
    http: &dyn SimpleHttp,
    cookie: &str,
    preferred_organization: Option<&str>,
) -> Result<CookieIdentity, ChannelError> {
    let body = send_ok(http, "bootstrap", || {
        let request = http::Request::get(format!("{}/api/bootstrap", auth::CLAUDE_AI_BASE_URL))
            .header(ACCEPT, "application/json")
            .header("accept-language", "en-US,en;q=0.9")
            .header("cache-control", "no-cache")
            .header("cookie", cookie)
            .header("origin", auth::CLAUDE_AI_BASE_URL)
            .header("referer", format!("{}/new", auth::CLAUDE_AI_BASE_URL))
            .body(Bytes::new())
            .map_err(|error| ChannelError::Login(error.to_string()))?;
        Ok(browser_request(request))
    })
    .await?;
    let value = parse_bootstrap(&body)?;
    let organization_uuid = value
        .get("account")
        .and_then(|account| account.get("memberships"))
        .and_then(Value::as_array)
        .and_then(|memberships| {
            memberships
                .iter()
                .filter_map(|membership| membership.get("organization"))
                .filter(|organization| has_subscription(organization))
                .filter_map(|organization| organization.get("uuid"))
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .find(|uuid| preferred_organization.is_none_or(|preferred| preferred == *uuid))
        })
        .map(str::to_owned)
        .ok_or_else(|| {
            ChannelError::Login(
                if preferred_organization.is_some() {
                    "saved organization is unavailable for cookie refresh"
                } else {
                    "cookie has no subscription-capable organization"
                }
                .into(),
            )
        })?;
    let account_uuid = value
        .pointer("/account/uuid")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    Ok(CookieIdentity {
        account_uuid,
        organization_uuid,
    })
}

async fn authorize(
    http: &dyn SimpleHttp,
    cookie: &str,
    organization: &str,
    state: &str,
    challenge: &str,
) -> Result<String, ChannelError> {
    let payload = json!({
        "response_type": "code",
        "client_id": auth::CLIENT_ID,
        "organization_uuid": organization,
        "redirect_uri": auth::DEFAULT_REDIRECT_URI,
        "scope": auth::OAUTH_SCOPE,
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    });
    let body =
        serde_json::to_vec(&payload).map_err(|error| ChannelError::Login(error.to_string()))?;
    let uri = format!(
        "{}/v1/oauth/{organization}/authorize",
        auth::DEFAULT_BASE_URL
    );
    let response = send_ok(http, "authorize", || {
        let request = http::Request::post(&uri)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .header("cookie", cookie)
            .header("origin", auth::CLAUDE_AI_BASE_URL)
            .header("anthropic-version", auth::ANTHROPIC_VERSION)
            .header("anthropic-beta", auth::OAUTH_BETA)
            .header(USER_AGENT, auth::fallback_user_agent())
            .body(Bytes::copy_from_slice(&body))
            .map_err(|error| ChannelError::Login(error.to_string()))?;
        Ok(browser_request(request))
    })
    .await?;
    let response: Value = serde_json::from_slice(&response)
        .map_err(|_| ChannelError::Login("invalid authorize response".into()))?;
    let uri = response
        .get("redirect_uri")
        .and_then(Value::as_str)
        .ok_or_else(|| ChannelError::Login("authorize response missing redirect_uri".into()))?;
    authorization_code(uri, state)
}

async fn token_exchange(
    http: &dyn SimpleHttp,
    verifier: &str,
    state: &str,
    code: &str,
) -> Result<Value, ChannelError> {
    let body = crate::shared::http::form(&[
        ("grant_type", "authorization_code"),
        ("client_id", auth::CLIENT_ID),
        ("code", code),
        ("redirect_uri", auth::DEFAULT_REDIRECT_URI),
        ("code_verifier", verifier),
        ("state", state),
    ]);
    let request = http::Request::post(auth::COOKIE_TOKEN_URL)
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header(ACCEPT, "application/json, text/plain, */*")
        .header("anthropic-version", auth::ANTHROPIC_VERSION)
        .header("anthropic-beta", auth::OAUTH_BETA)
        .header("origin", auth::CLAUDE_AI_BASE_URL)
        .header(USER_AGENT, auth::fallback_user_agent())
        .body(Bytes::from(body))
        .map_err(|error| ChannelError::Login(error.to_string()))?;
    let response = http
        .send(browser_request(request))
        .await
        .map_err(|_| ChannelError::Login("token endpoint request failed".into()))?;
    if !response.status().is_success() {
        return Err(endpoint_error("token", response.status()));
    }
    let token: Value = serde_json::from_slice(response.body())
        .map_err(|_| ChannelError::Login("invalid token response".into()))?;
    let access_token = required(&token, "access_token")?;
    let mut secret = json!({
        "access_token": access_token,
    });
    if let Some(refresh_token) = token
        .get("refresh_token")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        secret["refresh_token"] = Value::String(refresh_token.into());
    }
    auth::update_token_metadata(
        secret.as_object_mut().expect("cookie secret is an object"),
        &token,
        auth::unix_now_ms(),
    );
    Ok(secret)
}

async fn send_ok<F>(http: &dyn SimpleHttp, name: &str, build: F) -> Result<Bytes, ChannelError>
where
    F: Fn() -> Result<http::Request<Bytes>, ChannelError>,
{
    let mut challenge = None;
    for _ in 0..COOKIE_MAX_ATTEMPTS {
        let response = http
            .send(build()?)
            .await
            .map_err(|_| ChannelError::Login(format!("{name} endpoint request failed")))?;
        if response.status().is_success() {
            return Ok(response.into_body());
        }
        if is_cloudflare_challenge(response.status(), response.body()) {
            challenge = Some(response.status());
            continue;
        }
        return Err(endpoint_error(name, response.status()));
    }
    let status = challenge.expect("only challenges exhaust the retry loop");
    Err(endpoint_error(name, status))
}

fn browser_request(mut request: http::Request<Bytes>) -> http::Request<Bytes> {
    request.extensions_mut().insert(BROWSER_PROFILE.clone());
    request.extensions_mut().insert(RequiredClientProfile);
    request
}

fn parse_bootstrap(body: &[u8]) -> Result<Value, ChannelError> {
    let mut first = None;
    for value in serde_json::Deserializer::from_slice(body)
        .into_iter::<Value>()
        .flatten()
    {
        if value.get("account").and_then(Value::as_object).is_some() {
            return Ok(value);
        }
        first.get_or_insert(value);
    }
    first.ok_or_else(|| ChannelError::Login("bootstrap response is empty".into()))
}

fn has_subscription(organization: &Value) -> bool {
    organization
        .get("capabilities")
        .and_then(Value::as_array)
        .is_some_and(|capabilities| {
            capabilities
                .iter()
                .filter_map(Value::as_str)
                .any(|value| SUBSCRIPTION_CAPABILITIES.contains(&value))
        })
}

fn is_cloudflare_challenge(status: http::StatusCode, body: &[u8]) -> bool {
    if !matches!(
        status,
        http::StatusCode::FORBIDDEN | http::StatusCode::SERVICE_UNAVAILABLE
    ) {
        return false;
    }
    let text = String::from_utf8_lossy(&body[..body.len().min(1_024)]);
    ["Just a moment", "challenge-platform", "cf-chl", "cf_chl"]
        .iter()
        .any(|marker| text.contains(marker))
}

fn endpoint_error(name: &str, status: http::StatusCode) -> ChannelError {
    ChannelError::Login(format!("{name} endpoint returned {status}"))
}

fn authorization_code(uri: &str, expected_state: &str) -> Result<String, ChannelError> {
    let invalid = || ChannelError::Login("invalid authorization callback".into());
    if uri.contains('#') {
        return Err(invalid());
    }
    let callback: http::Uri = uri.parse().map_err(|_| invalid())?;
    let expected: http::Uri = auth::DEFAULT_REDIRECT_URI
        .parse()
        .expect("built-in callback is valid");
    if callback.scheme() != expected.scheme()
        || callback.authority() != expected.authority()
        || callback.path() != expected.path()
    {
        return Err(invalid());
    }
    let mut code = None;
    let mut state = None;
    for (key, value) in form_urlencoded::parse(callback.query().unwrap_or_default().as_bytes()) {
        let target = match key.as_ref() {
            "code" => &mut code,
            "state" => &mut state,
            "error" => return Err(ChannelError::Login("authorization was rejected".into())),
            _ => continue,
        };
        if target.is_some() || value.trim().is_empty() {
            return Err(invalid());
        }
        *target = Some(value.into_owned());
    }
    if state.as_deref() != Some(expected_state) || expected_state.is_empty() {
        return Err(invalid());
    }
    code.ok_or_else(invalid)
}

fn pkce() -> Result<(String, String, String), ChannelError> {
    let mut verifier = [0_u8; 32];
    let mut state = [0_u8; 24];
    getrandom::fill(&mut verifier)
        .and_then(|()| getrandom::fill(&mut state))
        .map_err(|_| ChannelError::Login("secure randomness unavailable".into()))?;
    let verifier = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(verifier);
    let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(Sha256::digest(verifier.as_bytes()));
    let state = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(state);
    Ok((verifier, challenge, state))
}

fn required<'a>(value: &'a Value, name: &str) -> Result<&'a str, ChannelError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ChannelError::Login(format!("token response missing {name}")))
}

fn ensure_device_id(secret: &mut Value) {
    if secret.get("device_id").and_then(Value::as_str).is_none() {
        secret["device_id"] = Value::String(auth::device_id(secret));
    }
}

fn overlay_without_refresh_token(old: &Value, minted: &Value) -> Value {
    let mut output = old.clone();
    if let (Some(output), Some(minted)) = (output.as_object_mut(), minted.as_object()) {
        for (key, value) in minted {
            if key != "refresh_token" && key != "device_id" {
                output.insert(key.clone(), value.clone());
            }
        }
        if let Some(received) = minted.get("token_received_at_ms").and_then(Value::as_i64) {
            crate::shared::oauth_expiry::apply(
                output,
                received,
                minted.get("expires_at_ms").and_then(Value::as_i64),
            );
        }
        output.insert("device_id".into(), Value::String(auth::device_id(old)));
    }
    output
}

#[cfg(test)]
mod tests;
