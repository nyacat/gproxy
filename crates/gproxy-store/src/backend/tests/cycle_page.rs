use std::collections::BTreeMap;

use sea_query::{Alias, Query};

use crate::Store;
use crate::backend::Statement;
use crate::records::{
    CredentialEnvelope, CredentialInput, CredentialQuotaCycleCursor, CredentialQuotaCyclePageQuery,
    CycleTracking, ProviderInput,
};

async fn seed(store: &Store) -> (i64, i64, i64, i64) {
    let mut providers = Vec::new();
    let mut credentials = Vec::new();
    for name in ["page-provider-a", "page-provider-b"] {
        let provider = store
            .insert_provider(&ProviderInput {
                name: name.into(),
                label: None,
                channel: "test".into(),
                settings: serde_json::json!({}),
                credential_strategy: "round_robin".into(),
                proxy_url: None,
                tls_fingerprint: None,
                enabled: true,
            })
            .await
            .unwrap();
        providers.push(provider);
        credentials.push(
            store
                .insert_credential(&CredentialInput {
                    provider_id: provider,
                    label: None,
                    kind: "api_key".into(),
                    envelope: CredentialEnvelope {
                        ciphertext: vec![],
                        wrapped_key: vec![],
                        payload_nonce: vec![],
                        key_nonce: vec![],
                    },
                    enabled: true,
                    weight: 1,
                    rpm_limit: None,
                    tpm_limit: None,
                    proxy_url: None,
                    tls_fingerprint: None,
                })
                .await
                .unwrap(),
        );
    }
    // More than the old per-window cap, with every timestamp equal, so a
    // timestamp-only cursor would silently skip most of this history.
    for id in 1..=125 {
        cycle(store, id, credentials[0], "daily", 1_000, 100_000).await;
    }
    cycle(store, 126, credentials[0], "weekly", 1_001, 100_000).await;
    cycle(store, 127, credentials[1], "daily", 1_002, 100_000).await;
    cycle(store, 128, credentials[0], "daily", 500, 100_000).await;
    cycle(store, 129, credentials[0], "daily", 1_100, 900_000).await;
    cycle(store, 130, credentials[0], "daily", 1_200, 899_999).await;
    // Invalid historical samples must not be parsed by the summary page.
    store.backend().execute(Statement::plain(
        "INSERT INTO credential_quota_observations (cycle_id, observed_at_ms, started_at_ms, snapshot_json) VALUES (1,1000000,1000000,'invalid historical JSON')"
    )).await.unwrap();
    (providers[0], providers[1], credentials[0], credentials[1])
}

async fn cycle(
    store: &Store,
    id: i64,
    credential_id: i64,
    window: &str,
    observed: i64,
    accounting_start: i64,
) {
    let tracking = CycleTracking {
        pending_observation: None,
        unit: None,
        reset_behavior: gproxy_core::QuotaResetBehavior::Periodic,
        models: BTreeMap::from([(
            "known-model".into(),
            serde_json::json!({"input_tokens":"3"}),
        )]),
        needs_rebuild: false,
        rebuild_after: None,
        scope: if window == "weekly" {
            gproxy_core::QuotaScope::Unknown
        } else {
            gproxy_core::QuotaScope::All
        },
        sample: gproxy_core::QuotaSample {
            source: gproxy_core::QuotaSampleSource::Unknown,
            started_at_ms: observed * 1_000,
            received_at_ms: observed * 1_000,
        },
        baseline_at_ms: accounting_start,
        baseline_percent: Some(0.into()),
        baseline_limit: None,
        uncertain: false,
        local_boundary: false,
    };
    let mut insert = Query::insert();
    insert
        .into_table(Alias::new("credential_quota_cycles"))
        .columns(
            [
                "id",
                "accounting_start_ms",
                "tracking_json",
                "version",
                "credential_id",
                "window_key",
                "boundary_source",
                "boundary_confidence",
                "status",
                "last_observed_at",
                "coverage",
                "metrics_json",
            ]
            .map(Alias::new),
        )
        .values_panic([
            id.into(),
            accounting_start.into(),
            serde_json::to_string(&tracking).unwrap().into(),
            1.into(),
            credential_id.into(),
            window.into(),
            "upstream".into(),
            "exact".into(),
            "closed".into(),
            observed.into(),
            "partial_lower_bound".into(),
            r#"{"input_tokens":"3"}"#.into(),
        ]);
    store
        .backend()
        .execute(Statement::query(&insert).unwrap())
        .await
        .unwrap();
}

async fn exercise(store: &Store) {
    let (provider, other_provider, credential, other_credential) = seed(store).await;
    let query = CredentialQuotaCyclePageQuery {
        from: 600,
        to: 900,
        credential_id: Some(credential),
        provider_id: Some(provider),
        window_key: Some("daily".into()),
        cursor: None,
        limit: 10,
    };
    let mut ids = Vec::new();
    let mut next = query.clone();
    loop {
        let page = store.credential_quota_cycle_page(&next).await.unwrap();
        assert!(page.items.len() <= 10);
        assert!(page.items.iter().all(|cycle| cycle.estimate.is_none()));
        assert!(page.items.iter().all(|cycle| cycle.models.len() == 1));
        assert!(
            page.items
                .iter()
                .all(|cycle| cycle.metrics["input_tokens"] == "3")
        );
        ids.extend(page.items.iter().map(|cycle| cycle.id));
        if page.next_cursor.is_none() {
            break;
        }
        assert!(ids.len() <= 126, "cursor must advance on every page");
        next.cursor = page.next_cursor;
    }
    let expected = std::iter::once(130)
        .chain((1..=125).rev())
        .collect::<Vec<_>>();
    assert_eq!(
        ids, expected,
        "page through all overlapping cycles, including those beyond the old 100/window cap"
    );

    // Exactly two pages of 63: the final full page must not advertise a third.
    let first = store
        .credential_quota_cycle_page(&CredentialQuotaCyclePageQuery {
            limit: 63,
            ..query.clone()
        })
        .await
        .unwrap();
    assert_eq!(first.items.len(), 63);
    let second = store
        .credential_quota_cycle_page(&CredentialQuotaCyclePageQuery {
            limit: 63,
            cursor: first.next_cursor,
            ..query.clone()
        })
        .await
        .unwrap();
    assert_eq!(second.items.len(), 63);
    assert!(second.next_cursor.is_none());

    let provider_page = store
        .credential_quota_cycle_page(&CredentialQuotaCyclePageQuery {
            provider_id: Some(other_provider),
            credential_id: None,
            window_key: None,
            ..query.clone()
        })
        .await
        .unwrap();
    assert_eq!(
        provider_page
            .items
            .iter()
            .map(|cycle| cycle.id)
            .collect::<Vec<_>>(),
        vec![127]
    );
    assert_eq!(provider_page.items[0].credential_id, other_credential);
    assert!(provider_page.next_cursor.is_none());
    let mismatch = store
        .credential_quota_cycle_page(&CredentialQuotaCyclePageQuery {
            provider_id: Some(other_provider),
            ..query.clone()
        })
        .await
        .unwrap();
    assert!(mismatch.items.is_empty());
    assert!(mismatch.next_cursor.is_none());

    let unknown_scope = store
        .credential_quota_cycle_page(&CredentialQuotaCyclePageQuery {
            window_key: Some("weekly".into()),
            ..query.clone()
        })
        .await
        .unwrap();
    assert_eq!(unknown_scope.items.len(), 1);
    assert!(unknown_scope.items[0].models.is_empty());
    assert_eq!(unknown_scope.items[0].metrics, serde_json::json!({}));

    for limit in [0, 101, u32::MAX] {
        assert!(
            store
                .credential_quota_cycle_page(&CredentialQuotaCyclePageQuery {
                    limit,
                    ..query.clone()
                })
                .await
                .is_err()
        );
    }
    let empty = store
        .credential_quota_cycle_page(&CredentialQuotaCyclePageQuery {
            cursor: Some(CredentialQuotaCycleCursor {
                last_observed_at: 1_000,
                id: 1,
            }),
            ..query.clone()
        })
        .await
        .unwrap();
    assert!(empty.items.is_empty());
    assert!(empty.next_cursor.is_none());
    let invalid_range = store
        .credential_quota_cycle_page(&CredentialQuotaCyclePageQuery {
            from: query.to,
            ..query
        })
        .await
        .unwrap();
    assert!(invalid_range.items.is_empty());
    assert!(invalid_range.next_cursor.is_none());
}

#[tokio::test]
async fn history_pages_past_window_cap_preserve_filters_scope_and_equal_timestamp_rows() {
    for remote in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cycle-page.db");
        let (store, _) = if remote {
            super::libsql_store(path).await.unwrap()
        } else {
            super::native_store(path).await.unwrap()
        };
        exercise(&store).await;
    }
}

#[tokio::test]
#[ignore = "requires an empty PostgreSQL database via GPROXY_TEST_POSTGRES_DSN"]
async fn postgres_history_pages_past_window_cap_preserve_filters_scope_and_equal_timestamp_rows() {
    let store = Store::open(crate::BackendConfig::Postgres {
        dsn: std::env::var("GPROXY_TEST_POSTGRES_DSN").unwrap(),
        pool_size: 4,
        checkout_timeout: std::time::Duration::from_secs(5),
    })
    .await
    .unwrap();
    exercise(&store).await;
}
