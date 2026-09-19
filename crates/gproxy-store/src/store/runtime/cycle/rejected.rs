use crate::query::runtime;
use crate::records::{CredentialQuotaCycleRecord, CredentialQuotaObservation};
use crate::{Store, StoreError};

impl Store {
    pub(super) async fn record_rejected_quota(
        &self,
        cycle: &mut CredentialQuotaCycleRecord,
        raw: &CredentialQuotaObservation,
        credential_version: Option<u64>,
    ) -> Result<bool, StoreError> {
        let expected = cycle.version;
        cycle.version += 1;
        Ok(self
            .write_quota_observation(
                cycle.credential_id,
                credential_version,
                vec![
                    runtime::update_tracked_cycle_for_version(cycle, expected, credential_version)?,
                    runtime::insert_cycle_observation(cycle, raw, true, credential_version)?,
                ],
            )
            .await?[0]
            .affected_rows
            == 1)
    }
}
