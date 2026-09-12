use crate::backend::Row;
use crate::query::runtime;
use crate::records::{CredentialHealthInput, CredentialHealthRecord, CredentialHealthState};
use crate::{Store, StoreError};

impl Store {
    pub async fn record_credential_health(
        &self,
        input: &CredentialHealthInput,
    ) -> Result<(), StoreError> {
        self.backend()
            .execute(runtime::upsert_credential_health(input)?)
            .await?;
        Ok(())
    }

    /// Recover only an existing degraded row from the same credential version.
    /// Stale snapshots cannot turn a newly dead credential or a reset row healthy.
    pub async fn recover_degraded_credential_health(
        &self,
        input: &CredentialHealthInput,
    ) -> Result<(), StoreError> {
        self.backend()
            .execute(runtime::recover_degraded_credential_health(input)?)
            .await?;
        Ok(())
    }

    pub async fn credential_health(&self) -> Result<Vec<CredentialHealthRecord>, StoreError> {
        self.backend()
            .execute(runtime::select_credential_health()?)
            .await?
            .rows
            .into_iter()
            .map(credential_health_record)
            .collect()
    }

    pub async fn credential_model_health(
        &self,
        credential_id: i64,
        model: &str,
    ) -> Result<Option<CredentialHealthRecord>, StoreError> {
        self.backend()
            .execute(runtime::select_credential_model_health(
                credential_id,
                model,
            )?)
            .await?
            .rows
            .into_iter()
            .next()
            .map(credential_health_record)
            .transpose()
    }

    pub async fn clear_credential_health(&self, credential_id: i64) -> Result<(), StoreError> {
        self.clear_credential_health_scope(credential_id, None)
            .await
    }

    pub async fn clear_credential_model_health(
        &self,
        credential_id: i64,
        model: &str,
    ) -> Result<(), StoreError> {
        self.clear_credential_health_scope(credential_id, Some(model))
            .await
    }

    async fn clear_credential_health_scope(
        &self,
        credential_id: i64,
        model: Option<&str>,
    ) -> Result<(), StoreError> {
        self.backend()
            .execute(runtime::delete_credential_health(credential_id, model)?)
            .await?;
        Ok(())
    }
}

fn credential_health_record(row: Row) -> Result<CredentialHealthRecord, StoreError> {
    let state = row.text("state")?;
    Ok(CredentialHealthRecord {
        credential_id: row.i64("credential_id")?,
        model: row.text("model")?.to_owned(),
        credential_version: u64::try_from(row.i64("credential_version")?).map_err(|error| {
            StoreError::InvalidData {
                field: "credential health version",
                message: error.to_string(),
            }
        })?,
        version: row.i64("version")?,
        state: CredentialHealthState::from_name(state).ok_or_else(|| StoreError::InvalidData {
            field: "credential health state",
            message: format!("unknown state `{state}`"),
        })?,
        consecutive_failures: u32::try_from(row.i64("consecutive_failures")?).map_err(|error| {
            StoreError::InvalidData {
                field: "credential health consecutive_failures",
                message: error.to_string(),
            }
        })?,
        observed_at: row.i64("observed_at")?,
        response_status: row
            .optional_i64("response_status")?
            .map(|value| {
                u16::try_from(value).map_err(|error| StoreError::InvalidData {
                    field: "credential health response_status",
                    message: error.to_string(),
                })
            })
            .transpose()?,
        detail: row.optional_text("detail")?.map(str::to_owned),
    })
}
