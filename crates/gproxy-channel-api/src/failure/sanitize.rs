use regex::Regex;
use std::sync::LazyLock;

static SECRETS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
    r#"(?ix)(?:bearer\s+[^\s,;\"']+)|(?:\b(?:sk|sess)-[a-z0-9_-]+)|(?:\beyJ[a-z0-9_-]+\.[a-z0-9_-]+(?:\.[a-z0-9_-]+)?)|(?:(?:cookie|set-cookie)\s*[:=]\s*[^\r\n]+)|(?:(?:authorization|proxy-authorization|api[_-]?key|(?:access[_-]|refresh[_-]|id[_-])?token|password|secret|signature)\s*[:=]\s*(?:(?:bearer|basic)\s+[^\s,;\"']+|\"[^\"]*\"|'[^']*'|[^\s,;]+))|(?:https?://[^\s\"']+\?[^\s\"']+)|(?:[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})"#
).expect("static secret patterns")
});
static QUOTED: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"\"[^\"]*\"|'[^']*'|`[^`]*`"#).expect("quoted literals"));
static REQUEST_ID: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\brequest[ _-]?id\s*[:=]?\s+([a-z0-9][a-z0-9_.:-]{5,255})")
        .expect("request id")
});

pub(super) fn label(value: &str, limit: usize) -> String {
    let safe = SECRETS.replace_all(value, "redacted");
    safe.chars()
        .take(limit)
        .map(|c| {
            if c.is_ascii_alphanumeric() || "._-:/".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect()
}

pub(super) fn request_id(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"._:-".contains(&c))
        && !SECRETS.is_match(value))
    .then(|| value.to_owned())
}

pub(super) fn message_request_id(value: &str) -> Option<String> {
    REQUEST_ID
        .captures(value)
        .and_then(|m| request_id(m[1].trim_end_matches('.')))
}

pub(super) fn message(value: &str) -> (String, bool) {
    let safe = SECRETS.replace_all(value, "[redacted]");
    let safe = QUOTED.replace_all(&safe, "[redacted]");
    let mut output = String::new();
    let mut truncated = false;
    for c in safe.chars() {
        let c = if c.is_control() { ' ' } else { c };
        if output.len() + c.len_utf8() > 1024 {
            truncated = true;
            break;
        }
        output.push(c);
    }
    (output, truncated)
}
