use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use gproxy_core::{QuotaResetBehavior, QuotaSample, QuotaScope};

use crate::Store;
use crate::backend::{DbFuture, Executor, QueryResult, SharedExecutor, Statement};
use crate::records::{
    CredentialEnvelope, CredentialInput, CredentialQuotaObservation, ProviderInput,
    QuotaBoundaryConfidence, QuotaBoundarySource,
};

/// Rotate after the observer has read the old version/cycle, immediately
/// before its write transaction. This exercises the durable guard, not just
/// the cheap initial version check.
struct RotateBeforeWrite {
    inner: SharedExecutor,
    credential: i64,
    armed: AtomicBool,
}

impl Executor for RotateBeforeWrite {
    fn execute<'a>(&'a self, statement: Statement) -> DbFuture<'a, QueryResult> {
        self.inner.execute(statement)
    }

    fn batch<'a>(&'a self, statements: Vec<Statement>) -> DbFuture<'a, Vec<QueryResult>> {
        Box::pin(async move {
            let observation = statements.iter().any(|statement| {
                statement
                    .sql
                    .contains("INSERT INTO \"credential_quota_observations\"")
            });
            if observation && self.armed.swap(false, Ordering::SeqCst) {
                self.inner
                    .execute(Statement::with_args(
                        "UPDATE credentials SET version = version + 1 WHERE id = ?",
                        vec![crate::backend::DbValue::Integer(self.credential)],
                    ))
                    .await?;
            }
            self.inner.batch(statements).await
        })
    }
}

#[tokio::test]
async fn native_and_libsql_quota_observations_cannot_cross_credential_rotation() {
    let directory = tempfile::tempdir().unwrap();
    let (native, _) = super::native_store(directory.path().join("native.db"))
        .await
        .unwrap();
    let (libsql, _) = super::libsql_store(directory.path().join("libsql.db"))
        .await
        .unwrap();
    for mut store in [native, libsql] {
        let credential = seed(&store).await;
        let schedule = Arc::new(RotateBeforeWrite {
            inner: store.executor.clone(),
            credential,
            armed: AtomicBool::new(false),
        });
        store.executor = schedule.clone();

        // New cycle creation must not resurrect a response from before a
        // credential replacement, even if the version was valid when read.
        let first = observation(credential, 10_000, 10);
        schedule.armed.store(true, Ordering::SeqCst);
        assert!(
            store
                .observe_credential_quota_cycle_for_version(&first, 0)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .credential_quota_cycle_history(credential, "primary")
                .await
                .unwrap()
                .is_empty()
        );

        let saved = store
            .observe_credential_quota_cycle_for_version(&first, 1)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.upstream_used, Some(10.into()));

        // Normal updates, decreased counters, and rejected older observations
        // all write different paths. None may mutate this preserved history.
        for next in [
            observation(credential, 20_000, 20),
            observation(credential, 20_000, 5),
            observation(credential, 9_000, 9),
        ] {
            let before = store
                .credential_quota_cycle_history(credential, "primary")
                .await
                .unwrap();
            let samples = store
                .credential_quota_observations(&before[0], false)
                .await
                .unwrap();
            let version = store.credential(credential).await.unwrap().unwrap().version;
            schedule.armed.store(true, Ordering::SeqCst);
            assert!(
                store
                    .observe_credential_quota_cycle_for_version(&next, version)
                    .await
                    .unwrap()
                    .is_none()
            );
            let after = store
                .credential_quota_cycle_history(credential, "primary")
                .await
                .unwrap();
            assert_eq!(after, before);
            assert_eq!(
                store
                    .credential_quota_observations(&after[0], false)
                    .await
                    .unwrap(),
                samples
            );
        }

        // Manual entries remain supported, and an already-stale version is a
        // harmless no-op even when it would otherwise create another window.
        let mut manual = observation(credential, 30_000, 30);
        manual.window_key = "manual".into();
        store.observe_credential_quota_cycle(&manual).await.unwrap();
        manual.window_key = "stale".into();
        assert!(
            store
                .observe_credential_quota_cycle_for_version(&manual, 0)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .credential_quota_cycle_history(credential, "stale")
                .await
                .unwrap()
                .is_empty()
        );
    }
}

async fn seed(store: &Store) -> i64 {
    let provider_id = store
        .insert_provider(&ProviderInput {
            name: "versioned-observation".into(),
            label: None,
            channel: "openai".into(),
            settings: serde_json::json!({}),
            credential_strategy: "round_robin".into(),
            proxy_url: None,
            tls_fingerprint: None,
            enabled: true,
        })
        .await
        .unwrap();
    store
        .insert_credential(&CredentialInput {
            provider_id,
            label: None,
            kind: "api_key".into(),
            envelope: CredentialEnvelope {
                ciphertext: vec![1; 8],
                wrapped_key: vec![1; 8],
                payload_nonce: vec![1; 12],
                key_nonce: vec![1; 12],
            },
            enabled: true,
            weight: 100,
            rpm_limit: None,
            tpm_limit: None,
            proxy_url: None,
            tls_fingerprint: None,
        })
        .await
        .unwrap()
}

fn observation(credential_id: i64, at: i64, used: i64) -> CredentialQuotaObservation {
    CredentialQuotaObservation {
        unit: Some("requests".into()),
        reset_behavior: QuotaResetBehavior::Periodic,
        scope: QuotaScope::All,
        sample: QuotaSample {
            source: gproxy_core::QuotaSampleSource::Response,
            started_at_ms: at - 1,
            received_at_ms: at,
        },
        credential_id,
        window_key: "primary".into(),
        label: None,
        period_start: Some(0),
        period_end: Some(100),
        boundary_source: QuotaBoundarySource::Upstream,
        boundary_confidence: QuotaBoundaryConfidence::Exact,
        observed_at: at / 1_000,
        upstream_used: Some(used.into()),
        upstream_limit: Some(100.into()),
        used_percent: Some(used.into()),
    }
}
