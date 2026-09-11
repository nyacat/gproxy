use gproxy_channel_api::{Disposition, ResponseView};

pub(crate) fn classify(response: ResponseView<'_>) -> Disposition {
    crate::shared::disposition::unauthorized_or_forbidden(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn misalignment_block_is_terminal_without_killing_the_credential() {
        let headers = http::HeaderMap::new();
        let body =
            br#"{"error":{"type":"invalid_request_error","code":"misalignment_policy_violation"}}"#;
        assert_eq!(
            classify(ResponseView {
                status: http::StatusCode::FORBIDDEN,
                headers: &headers,
                body,
            }),
            Disposition::Terminal
        );
        assert_eq!(
            classify(ResponseView {
                status: http::StatusCode::FORBIDDEN,
                headers: &headers,
                body: b"{}",
            }),
            Disposition::CredentialDead
        );
    }
}
