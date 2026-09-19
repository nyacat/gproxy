use gproxy_channel_api::RefreshTokenStatus;

use crate::{Core, CoreError, CredentialId, CredentialRecord, CredentialStore, Host, ProviderRef};

#[derive(Clone)]
pub struct CredentialRefreshResult {
    pub credential: CredentialRecord,
    /// None means another operation updated the credential after this request
    /// began; this caller did not receive an upstream token response.
    pub refresh_token: Option<RefreshTokenStatus>,
}

impl<H: Host> Core<H> {
    pub fn credential_refresh_supported(&self, channel: &str, secret: &serde_json::Value) -> bool {
        self.channels
            .get(channel)
            .is_some_and(|channel| channel.can_refresh(secret))
    }

    pub async fn refresh_credential(
        &self,
        provider: &ProviderRef,
        credential: CredentialId,
        expected_version: u64,
    ) -> Result<CredentialRefreshResult, CoreError> {
        let channel = self
            .channels
            .shared(&provider.channel)
            .ok_or(CoreError::Unsupported)?;
        let current = self.host.credentials().load_current(credential).await?;
        if current.version != expected_version {
            return Err(CoreError::CredentialVersionConflict);
        }
        if current.id != credential
            || current.channel != provider.channel
            || !channel.can_refresh(&current.secret)
        {
            return Err(CoreError::Unsupported);
        }
        crate::execution::credential::run(
            &self.host,
            channel,
            credential,
            provider,
            current.version,
            true,
        )
        .await
    }
}
