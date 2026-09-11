use serde_json::Value;

use crate::backend::Row;
use crate::query::settlement_recovery;
use crate::{Store, StoreError};

impl Store {
    pub async fn enqueue_settlement_replay(
        &self,
        request_id: &str,
        payload: &Value,
    ) -> Result<(), StoreError> {
        self.backend()
            .execute(settlement_recovery::enqueue(request_id, payload)?)
            .await?;
        Ok(())
    }

    pub async fn put_settlement_replay(
        &self,
        request_id: &str,
        payload: &Value,
    ) -> Result<(), StoreError> {
        self.backend()
            .execute(settlement_recovery::put(request_id, payload)?)
            .await?;
        Ok(())
    }

    pub async fn list_settlement_replays(
        &self,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<(String, Value)>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.backend()
            .execute(settlement_recovery::list(after, limit)?)
            .await?
            .rows
            .into_iter()
            .map(|row| {
                let request_id = row.text("request_id")?.to_owned();
                Ok((request_id, parse_payload(&row)?))
            })
            .collect()
    }

    pub async fn get_settlement_replay(
        &self,
        request_id: &str,
    ) -> Result<Option<Value>, StoreError> {
        self.backend()
            .execute(settlement_recovery::get(request_id)?)
            .await?
            .rows
            .first()
            .map(parse_payload)
            .transpose()
    }

    pub async fn delete_settlement_replay(&self, request_id: &str) -> Result<(), StoreError> {
        self.backend()
            .execute(settlement_recovery::delete(request_id)?)
            .await?;
        Ok(())
    }

    pub async fn complete_settlement_replay(&self, request_id: &str) -> Result<(), StoreError> {
        self.backend()
            .execute(settlement_recovery::complete(request_id, None)?)
            .await?;
        Ok(())
    }

    pub async fn complete_settlement_replay_if(
        &self,
        request_id: &str,
        expected: &Value,
    ) -> Result<bool, StoreError> {
        Ok(self
            .backend()
            .execute(settlement_recovery::complete(request_id, Some(expected))?)
            .await?
            .affected_rows
            == 1)
    }

    pub async fn replace_settlement_replay(
        &self,
        request_id: &str,
        expected: &Value,
        replacement: &Value,
    ) -> Result<bool, StoreError> {
        Ok(self
            .backend()
            .execute(settlement_recovery::replace(
                request_id,
                expected,
                replacement,
            )?)
            .await?
            .affected_rows
            == 1)
    }
}

fn parse_payload(row: &Row) -> Result<Value, StoreError> {
    serde_json::from_str(row.text("payload_json")?).map_err(|error| StoreError::InvalidData {
        field: "settlement replay payload",
        message: error.to_string(),
    })
}
