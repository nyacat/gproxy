use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One realtime read shared by a cycle's summary and optional history.
#[derive(Debug, Clone)]
pub struct CredentialQuotaCycleStatistics {
    pub cycle: CredentialQuotaCycleRecord,
    pub observations: Vec<super::CycleObservationRecord>,
}

#[derive(Debug, Clone, Copy)]
pub struct CredentialQuotaCycleQuery {
    pub credential_id: Option<i64>,
    pub provider_id: Option<i64>,
    pub from: i64,
    pub to: i64,
    pub calculate: bool,
    pub history: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialQuotaCycleCursor {
    pub last_observed_at: i64,
    pub id: i64,
}

/// A page of cycle summaries. Historical observations and estimates are read
/// separately after a caller selects a cycle.
#[derive(Debug, Clone)]
pub struct CredentialQuotaCyclePageQuery {
    pub from: i64,
    pub to: i64,
    pub credential_id: Option<i64>,
    pub provider_id: Option<i64>,
    pub window_key: Option<String>,
    pub cursor: Option<CredentialQuotaCycleCursor>,
    pub limit: u32,
}

#[derive(Debug, Clone)]
pub struct CredentialQuotaCyclePage {
    pub items: Vec<CredentialQuotaCycleRecord>,
    pub next_cursor: Option<CredentialQuotaCycleCursor>,
}

/// Additive options for bounded console reads. The legacy query keeps its
/// full-cycle history semantics when these options are omitted.
#[derive(Debug, Clone, Default)]
pub struct CredentialQuotaStatisticsOptions {
    pub cycle_ids: Option<Vec<i64>>,
    pub current_only: bool,
    pub observation_range_ms: Option<(i64, i64)>,
}

/// Routing reads must not deserialize model metrics or observation history.
#[derive(Debug, Clone)]
pub struct CredentialQuotaWindowState {
    pub id: i64,
    pub version: u64,
    pub credential_id: i64,
    pub window_key: String,
    pub period_start: Option<i64>,
    pub period_end: Option<i64>,
    pub boundary_source: QuotaBoundarySource,
    pub last_observed_at: i64,
    pub upstream_used: Option<Decimal>,
    pub upstream_limit: Option<Decimal>,
    pub used_percent: Option<Decimal>,
}

impl CredentialQuotaWindowState {
    pub fn reset_at(&self) -> Option<i64> {
        (self.boundary_source == QuotaBoundarySource::Upstream)
            .then_some(self.period_end)
            .flatten()
    }

    pub fn pressure(&self) -> Option<Decimal> {
        self.used_percent.or_else(|| {
            let limit = self.upstream_limit?;
            let used = self.upstream_used?;
            (limit > Decimal::ZERO).then(|| used / limit * Decimal::ONE_HUNDRED)
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaBoundarySource {
    Upstream,
    Inferred,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaBoundaryConfidence {
    Exact,
    Derived,
    Partial,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaCycleStatus {
    Open,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaCycleCloseReason {
    BoundaryCrossed,
    ManualReset,
    UsageDecreased,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaCoverage {
    FullPeriodLowerBound,
    PartialLowerBound,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CredentialQuotaObservation {
    pub unit: Option<String>,
    pub reset_behavior: gproxy_core::QuotaResetBehavior,
    pub scope: gproxy_core::QuotaScope,
    pub sample: gproxy_core::QuotaSample,
    pub credential_id: i64,
    pub window_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period_start: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period_end: Option<i64>,
    pub boundary_source: QuotaBoundarySource,
    pub boundary_confidence: QuotaBoundaryConfidence,
    pub observed_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_used: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_limit: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub used_percent: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CredentialQuotaCycleRecord {
    pub accounting_start_ms: i64,
    pub accounting_end_ms: Option<i64>,
    pub tracking: super::CycleTracking,
    #[serde(default)]
    pub estimate: Option<super::CycleEstimate>,
    pub id: i64,
    pub version: u64,
    pub credential_id: i64,
    pub window_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period_start: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period_end: Option<i64>,
    pub boundary_source: QuotaBoundarySource,
    pub boundary_confidence: QuotaBoundaryConfidence,
    pub status: QuotaCycleStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<QuotaCycleCloseReason>,
    pub last_observed_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_used: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_limit: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub used_percent: Option<Decimal>,
    pub coverage: QuotaCoverage,
    pub metrics: Value,
    pub models: Vec<CredentialQuotaCycleModelRecord>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CredentialQuotaCycleModelRecord {
    pub model: String,
    pub metrics: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CredentialQuotaPressure {
    pub cycle_id: i64,
    pub credential_id: i64,
    pub window_key: String,
    pub version: u64,
    pub last_observed_at: i64,
    pub used_percent: Decimal,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub period_end: Option<i64>,
}
