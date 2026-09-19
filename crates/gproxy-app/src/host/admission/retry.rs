use gproxy_channel_api::CallerIdentity;
use gproxy_core::{CoreError, Target};
use gproxy_protocol::SettleMode;

use super::super::AppHost;

pub(in crate::host) async fn admit(
    host: &AppHost,
    request_id: &str,
    target: &Target,
    body: &bytes::Bytes,
    settle: SettleMode,
) -> Result<(), CoreError> {
    if settle != SettleMode::Free
        && let Some(mut state) = super::finish::load(host, request_id).await?
    {
        let identity = CallerIdentity {
            oauth_access_digest: None,
            user_id: state.identity.user_id,
            user_key_id: state.identity.user_key_id,
            org_id: state.identity.org_id,
            team_id: state.identity.team_id,
        };
        let operation = state
            .operation
            .as_deref()
            .and_then(gproxy_protocol::Operation::from_id)
            .map(|operation| {
                gproxy_protocol::OperationKey::content(
                    operation,
                    gproxy_protocol::ContentGenerationKind::ClaudeMessages,
                )
            });
        super::auth::authorize(
            &host.services.control.current(),
            &identity,
            operation,
            state.model.as_deref(),
            &gproxy_core::Plan {
                targets: vec![target.clone()],
                budget: gproxy_core::control::FailoverBudget { max_attempts: 1 },
            },
        )?;
        let start = state.reservations.len();
        if let Err(error) =
            super::quota::reserve_retry(host, &identity, request_id, body, target, &mut state).await
        {
            if let Err(rollback) = super::finish::refund_from(host, request_id, start).await {
                tracing::error!(request_id, error = %rollback, "fallback reservation rollback requires replay");
            }
            return Err(error);
        }
    }
    super::credential::admit(host, request_id, target, body, settle).await
}
