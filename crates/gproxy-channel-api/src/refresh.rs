use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshTokenStatus {
    Updated,
    Unchanged,
    NotReturned,
    NotApplicable,
}

pub struct RefreshResult {
    pub secret: Value,
    pub refresh_token: RefreshTokenStatus,
}
