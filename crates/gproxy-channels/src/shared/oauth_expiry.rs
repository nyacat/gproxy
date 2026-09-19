use serde_json::{Map, Value};

const MILLIS_PER_SECOND: i64 = 1_000;

pub(crate) fn from_lifetime(now_ms: i64, expires_in_seconds: Option<i64>) -> Option<i64> {
    let seconds = expires_in_seconds.filter(|seconds| *seconds > 0)?;
    now_ms.checked_add(seconds.checked_mul(MILLIS_PER_SECOND)?)
}

/// The receipt time belongs to this token. Missing expiry information must
/// not retain the previous access token's deadline.
pub(crate) fn apply(object: &mut Map<String, Value>, now_ms: i64, expires_at_ms: Option<i64>) {
    object.insert("token_received_at_ms".into(), Value::from(now_ms));
    if let Some(expires_at_ms) = expires_at_ms {
        object.insert("expires_at_ms".into(), Value::from(expires_at_ms));
    } else {
        object.remove("expires_at_ms");
    }
    object.remove("refresh_at_ms");
    object.remove("expiry_unknown");
}

/// A new token uses at most half its lifetime as the refresh margin. Records
/// imported without a receipt time retain the configured channel margin.
pub(crate) fn refresh_due(secret: &Value, expires_at_ms: i64, skew_ms: i64) -> i64 {
    let skew_ms = skew_ms.max(0);
    let skew_ms = match secret
        .get("token_received_at_ms")
        .and_then(Value::as_i64)
        .filter(|received| *received >= 0 && *received <= expires_at_ms)
    {
        Some(received) => skew_ms.min(expires_at_ms.saturating_sub(received) / 2),
        None => skew_ms,
    };
    ceil_seconds(expires_at_ms.saturating_sub(skew_ms))
        .min(expires_at_ms.div_euclid(MILLIS_PER_SECOND))
}

fn ceil_seconds(milliseconds: i64) -> i64 {
    milliseconds.div_euclid(MILLIS_PER_SECOND)
        + i64::from(milliseconds.rem_euclid(MILLIS_PER_SECOND) != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn short_lifetimes_refresh_after_receipt_and_no_later_than_expiry() {
        for fraction in [0, 1, 500, 999] {
            let received = 1_000_000 + fraction;
            for lifetime in [1, 2, 60, 300, 600, 3_600] {
                let expires = from_lifetime(received, Some(lifetime)).unwrap();
                let mut secret = json!({});
                apply(secret.as_object_mut().unwrap(), received, Some(expires));
                let due = refresh_due(&secret, expires, 300_000);
                assert!(due > received / 1_000);
                assert!(due <= expires / 1_000);
                assert!(due <= (received + lifetime * 1_000) / 1_000);
            }
        }
    }

    #[test]
    fn invalid_lifetimes_never_fabricate_expiry() {
        for value in [None, Some(0), Some(-1), Some(i64::MAX)] {
            assert!(from_lifetime(1_000_000, value).is_none());
        }
        let mut secret = json!({"expires_at_ms":9_999_999});
        apply(secret.as_object_mut().unwrap(), 1_000, None);
        assert!(secret.get("expires_at_ms").is_none());
        assert_eq!(secret["token_received_at_ms"], 1_000);
    }

    #[test]
    fn legacy_records_use_the_configured_margin() {
        assert_eq!(refresh_due(&json!({}), 1_000_000, 300_000), 700);
    }
}
