use rust_decimal::Decimal;
use serde_json::json;

use crate::StoreError;
use crate::backend::{DbValue, Statement};
use crate::records::{QuotaInput, QuotaWindowKind, UsageAggregateQuery, UsageGroupBy};

#[tokio::test]
async fn durable_quota_and_usage_totals_reject_decimal_overflow() {
    for remote in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("numeric-bounds.db");
        let (store, _) = if remote {
            super::libsql_store(path).await.unwrap()
        } else {
            super::native_store(path).await.unwrap()
        };
        let quota = store
            .insert_quota(&QuotaInput {
                subject_kind: "credential".into(),
                subject_id: 1,
                quota_total: Some(Decimal::MAX),
                quota_daily: None,
                quota_weekly: None,
                quota_monthly: None,
                quota_5h: None,
                quota_7d: None,
                enabled: true,
            })
            .await
            .unwrap();
        let window = store
            .ensure_quota_window(quota, QuotaWindowKind::Total, 0)
            .await
            .unwrap();
        store
            .add_quota_cost("at-maximum", window.id, Decimal::MAX)
            .await
            .unwrap();
        assert!(matches!(
            store
                .add_quota_cost("overflow", window.id, Decimal::ONE)
                .await,
            Err(StoreError::InvalidData {
                field: "cost_used",
                ..
            })
        ));
        assert_eq!(
            store
                .quota_window(window.id)
                .await
                .unwrap()
                .unwrap()
                .cost_used,
            Decimal::MAX
        );
        assert!(
            !store
                .quota_settlement_exists("overflow", window.id)
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .add_quota_cost("at-maximum", window.id, Decimal::MAX)
                .await
                .unwrap()
                .cost_used,
            Decimal::MAX
        );

        for (request, cost) in [("large", Decimal::MAX), ("small", Decimal::ONE)] {
            store.backend().execute(Statement::with_args(
                "INSERT INTO usage_rows (request_id,at,upstream_started_at_ms,provider_id,credential_id,upstream_model,input_tokens,output_tokens,cached_input_tokens,metrics_json,dimensions_json,cost,usage_source,ended,latency_ms) VALUES (?,1,1000,1,1,'model',1,0,0,'{}','{}',?,'upstream','complete',1)",
                vec![DbValue::Text(request.into()), DbValue::Text(cost.to_string())],
            )).await.unwrap();
            store.backend().execute(Statement::with_args(
                "INSERT INTO usage_rollups (granularity,bucket_start,dimension_key,provider_id,requests,input_tokens,output_tokens,cached_input_tokens,metrics_json,cost,version) VALUES ('hour',0,?,1,1,1,0,0,'{}',?,1)",
                vec![DbValue::Text(request.into()), DbValue::Text(cost.to_string())],
            )).await.unwrap();
        }
        assert!(matches!(
            store
                .usage_aggregate(&UsageAggregateQuery {
                    from: 0,
                    to: 100,
                    group_by: UsageGroupBy::Provider,
                    user_key_id: None,
                    user_id: None,
                    provider_id: None,
                    credential_id: None,
                    model: None,
                })
                .await,
            Err(StoreError::InvalidData { field: "cost", .. })
        ));
        assert!(matches!(
            store.usage_trend(0, 100).await,
            Err(StoreError::InvalidData { field: "cost", .. })
        ));

        // A failed aggregate must not affect the exact durable usage rows.
        assert_eq!(
            store
                .usage_by_request("large")
                .await
                .unwrap()
                .unwrap()
                .usage
                .cost,
            Decimal::MAX
        );
        assert_eq!(
            store
                .usage_by_request("small")
                .await
                .unwrap()
                .unwrap()
                .usage
                .metrics,
            json!({})
        );
    }
}
