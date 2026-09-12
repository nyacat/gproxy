use gproxy_channel_api::{ChannelError, RefreshResult, RefreshTokenStatus};
use serde_json::Value;

pub(crate) fn can_refresh(secret: &Value) -> bool {
    token(secret.get("refresh_token").and_then(Value::as_str)).is_some()
}

pub(crate) fn oauth(
    mut secret: Value,
    returned: Option<&str>,
) -> Result<RefreshResult, ChannelError> {
    let object = secret
        .as_object_mut()
        .ok_or_else(|| ChannelError::Refresh("secret must be an object".into()))?;
    let refresh_token = match token(returned) {
        Some(returned) => {
            let previous = token(object.get("refresh_token").and_then(Value::as_str));
            let status = if previous == Some(returned) {
                RefreshTokenStatus::Unchanged
            } else {
                RefreshTokenStatus::Updated
            };
            object.insert("refresh_token".into(), Value::String(returned.into()));
            status
        }
        None => RefreshTokenStatus::NotReturned,
    };
    Ok(RefreshResult {
        secret,
        refresh_token,
    })
}

fn token(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}
