use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageFilter {
    pub from: i64,
    pub to: i64,
    pub user_key_id: Option<i64>,
    pub user_id: Option<i64>,
    pub provider_id: Option<i64>,
    pub credential_id: Option<i64>,
    pub model: Option<String>,
    pub request_id: Option<String>,
    pub operation: Option<String>,
    pub usage_source: Option<String>,
    pub ended: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UsageTotals {
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_input_tokens: u64,
    pub cost: Decimal,
    pub metrics: BTreeMap<String, Decimal>,
}

impl UsageTotals {
    pub fn add(&mut self, usage: &super::UsageInput) -> Result<(), crate::StoreError> {
        let metrics =
            BTreeMap::<String, Decimal>::deserialize(&usage.metrics).map_err(|error| {
                crate::StoreError::InvalidData {
                    field: "metrics_json",
                    message: error.to_string(),
                }
            })?;
        self.add_values(
            usage.input_tokens,
            usage.output_tokens,
            usage.cached_input_tokens,
            usage.cost,
            metrics,
        )
    }

    pub(crate) fn add_values(
        &mut self,
        input_tokens: u64,
        output_tokens: u64,
        cached_input_tokens: u64,
        cost: Decimal,
        mut metrics: BTreeMap<String, Decimal>,
    ) -> Result<(), crate::StoreError> {
        let requests = checked_count(self.requests, 1, "requests")?;
        let input_tokens = checked_count(self.input_tokens, input_tokens, "input_tokens")?;
        let output_tokens = checked_count(self.output_tokens, output_tokens, "output_tokens")?;
        let cached_input_tokens = checked_count(
            self.cached_input_tokens,
            cached_input_tokens,
            "cached_input_tokens",
        )?;
        let cost = checked_decimal(self.cost, cost, "cost")?;
        for (name, amount) in &mut metrics {
            *amount = checked_decimal(
                self.metrics.get(name).copied().unwrap_or_default(),
                *amount,
                "metrics_json",
            )?;
        }
        // Validate the derived total too, before publishing any part of this
        // update. Individually valid token metrics can overflow when combined.
        let mut cache_tokens = Decimal::ZERO;
        for key in [
            "cache_creation_5m_tokens",
            "cache_creation_30m_tokens",
            "cache_creation_1h_tokens",
        ] {
            let amount = metrics.get(key).or_else(|| self.metrics.get(key));
            if let Some(amount) = amount {
                cache_tokens = checked_decimal(cache_tokens, *amount, "total_tokens")?;
            }
        }
        checked_decimal(
            Decimal::from(input_tokens) + Decimal::from(output_tokens),
            cache_tokens,
            "total_tokens",
        )?;
        self.requests = requests;
        self.input_tokens = input_tokens;
        self.output_tokens = output_tokens;
        self.cached_input_tokens = cached_input_tokens;
        self.cost = cost;
        self.metrics.extend(metrics);
        Ok(())
    }

    pub fn total_tokens(&self) -> Decimal {
        Decimal::from(self.input_tokens)
            + Decimal::from(self.output_tokens)
            + [
                "cache_creation_5m_tokens",
                "cache_creation_30m_tokens",
                "cache_creation_1h_tokens",
            ]
            .iter()
            .filter_map(|key| self.metrics.get(*key))
            .copied()
            .sum::<Decimal>()
    }
}

fn checked_count(left: u64, right: u64, field: &'static str) -> Result<u64, crate::StoreError> {
    left.checked_add(right)
        .ok_or_else(|| crate::StoreError::InvalidData {
            field,
            message: "usage total exceeds u64".into(),
        })
}

fn checked_decimal(
    left: Decimal,
    right: Decimal,
    field: &'static str,
) -> Result<Decimal, crate::StoreError> {
    left.checked_add(right)
        .ok_or_else(|| crate::StoreError::InvalidData {
            field,
            message: "usage total exceeds Decimal".into(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_overflow(mut totals: UsageTotals, field: &'static str) {
        let before = totals.clone();
        let result = totals.add_values(
            1,
            1,
            1,
            Decimal::ONE,
            BTreeMap::from([("custom_metric".into(), Decimal::ONE)]),
        );
        assert!(matches!(
            result,
            Err(crate::StoreError::InvalidData { field: actual, .. }) if actual == field
        ));
        assert_eq!(
            totals, before,
            "failed accumulation must not publish partial totals"
        );
    }

    #[test]
    fn usage_totals_return_errors_for_integer_overflow_without_partial_updates() {
        for field in [
            "requests",
            "input_tokens",
            "output_tokens",
            "cached_input_tokens",
        ] {
            let mut totals = UsageTotals::default();
            match field {
                "requests" => totals.requests = u64::MAX,
                "input_tokens" => totals.input_tokens = u64::MAX,
                "output_tokens" => totals.output_tokens = u64::MAX,
                _ => totals.cached_input_tokens = u64::MAX,
            }
            assert_overflow(totals, field);
        }
    }

    #[test]
    fn usage_totals_return_errors_for_decimal_overflow_without_partial_updates() {
        assert_overflow(
            UsageTotals {
                cost: Decimal::MAX,
                ..Default::default()
            },
            "cost",
        );
        assert_overflow(
            UsageTotals {
                metrics: BTreeMap::from([("custom_metric".into(), Decimal::MAX)]),
                ..Default::default()
            },
            "metrics_json",
        );
        assert_overflow(
            UsageTotals {
                metrics: BTreeMap::from([("cache_creation_5m_tokens".into(), Decimal::MAX)]),
                ..Default::default()
            },
            "total_tokens",
        );
    }

    #[test]
    fn usage_totals_combine_existing_and_new_dynamic_metrics_exactly() {
        let mut totals = UsageTotals::default();
        totals
            .add_values(
                3,
                2,
                1,
                Decimal::new(1, 8),
                BTreeMap::from([
                    ("first_metric".into(), Decimal::new(125, 3)),
                    ("cache_creation_5m_tokens".into(), Decimal::new(25, 2)),
                ]),
            )
            .unwrap();
        totals
            .add_values(
                1,
                0,
                0,
                Decimal::new(1, 8),
                BTreeMap::from([
                    ("second_metric".into(), Decimal::new(5, 1)),
                    ("cache_creation_5m_tokens".into(), Decimal::new(25, 2)),
                ]),
            )
            .unwrap();
        assert_eq!(totals.cost, Decimal::new(2, 8));
        assert_eq!(totals.metrics["first_metric"], Decimal::new(125, 3));
        assert_eq!(totals.metrics["second_metric"], Decimal::new(5, 1));
        assert_eq!(totals.total_tokens(), Decimal::new(65, 1));
    }
}
