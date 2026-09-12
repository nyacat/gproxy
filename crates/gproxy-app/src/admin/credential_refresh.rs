use gproxy_admin::{
    AdminError,
    dto::{
        ConnectivityScopeDto, ConnectivityTestRequest, CredentialRefreshResponse,
        CredentialRefreshTokenStatusDto,
    },
};
use gproxy_channel_api::RefreshTokenStatus;
use gproxy_core::{CoreError, CredentialId};

use crate::AppHandle;

pub(super) async fn supported(app: &AppHandle, id: i64) -> Result<bool, AdminError> {
    let services = &app.inner.host.services;
    let Some(credential) = services.store.credential(id).await? else {
        return Ok(false);
    };
    if !credential.enabled {
        return Ok(false);
    }
    let secret = services
        .cipher
        .open(&credential.envelope)
        .map_err(|_| AdminError::Internal("credential could not be decrypted".into()))?;
    Ok(app.inner.core.credential_refresh_supported(
        gproxy_channels::canonical_channel_id(&credential.channel),
        &secret,
    ))
}

pub(super) async fn run(
    app: &AppHandle,
    id: i64,
    expected_version: u64,
) -> Result<CredentialRefreshResponse, AdminError> {
    let services = &app.inner.host.services;
    services
        .control
        .reload()
        .await
        .map_err(|_| AdminError::Internal("credential configuration could not be read".into()))?;
    let snapshot = services.control.current();
    let credential = current(app, id, expected_version).await?;
    let meta = snapshot
        .credentials
        .iter()
        .find(|value| value.id == id)
        .ok_or_else(configuration_changed)?;
    if meta.version != credential.version || meta.provider_id != credential.provider_id {
        return Err(configuration_changed());
    }
    let provider = snapshot
        .providers
        .iter()
        .find(|value| value.id == credential.provider_id)
        .ok_or_else(configuration_changed)?;
    if !provider.enabled {
        return Err(AdminError::Conflict("provider is disabled".into()));
    }
    let (provider, _) = super::connectivity::target::resolve_from(
        app,
        &ConnectivityTestRequest {
            scope: ConnectivityScopeDto::Credential,
            provider_id: None,
            credential_id: Some(id),
            proxy_url: None,
        },
        &snapshot,
    )
    .map_err(|_| AdminError::BadRequest("credential provider configuration is invalid".into()))?;
    let result = match app
        .inner
        .core
        .refresh_credential(&provider, CredentialId(id), expected_version)
        .await
    {
        Ok(result) => result,
        Err(error) => {
            current(app, id, expected_version).await?;
            return Err(refresh_error(error));
        }
    };
    Ok(CredentialRefreshResponse {
        credential_id: result.credential.id.0,
        credential_version: result.credential.version,
        refresh_token_status: match result.refresh_token {
            Some(RefreshTokenStatus::Updated) => CredentialRefreshTokenStatusDto::Updated,
            Some(RefreshTokenStatus::Unchanged) => CredentialRefreshTokenStatusDto::Unchanged,
            Some(RefreshTokenStatus::NotReturned) => CredentialRefreshTokenStatusDto::NotReturned,
            Some(RefreshTokenStatus::NotApplicable) => {
                CredentialRefreshTokenStatusDto::NotApplicable
            }
            None => CredentialRefreshTokenStatusDto::UpdatedElsewhere,
        },
    })
}

async fn current(
    app: &AppHandle,
    id: i64,
    expected_version: u64,
) -> Result<gproxy_store::records::CredentialRecord, AdminError> {
    let credential = app
        .inner
        .host
        .services
        .store
        .credential(id)
        .await?
        .ok_or(AdminError::NotFound)?;
    if !credential.enabled {
        return Err(AdminError::Conflict("credential is disabled".into()));
    }
    if credential.version != expected_version {
        return Err(configuration_changed());
    }
    Ok(credential)
}

fn configuration_changed() -> AdminError {
    AdminError::Conflict("credential configuration changed; reload and retry".into())
}

fn refresh_error(error: CoreError) -> AdminError {
    match error {
        CoreError::CredentialVersionConflict => configuration_changed(),
        CoreError::Unsupported => AdminError::BadRequest(
            "credential does not support refresh or has no saved refresh token".into(),
        ),
        CoreError::NoCredentials => AdminError::Conflict("credential is unavailable".into()),
        CoreError::Channel(_) | CoreError::Transport(_) | CoreError::UpstreamExhausted(_) => {
            AdminError::BadGateway("upstream credential refresh failed; retry or sign in again")
        }
        _ => AdminError::Internal("credential refresh could not be completed".into()),
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
