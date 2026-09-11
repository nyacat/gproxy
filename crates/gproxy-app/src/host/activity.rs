use gproxy_core::{CoreError, Target, host::CredentialUsageLease};

use super::{AppHost, settlement_recovery::store_error};

pub(super) async fn begin(
    host: &AppHost,
    parent: &str,
    request: &str,
    target: &Target,
    started: i64,
) -> Result<Option<CredentialUsageLease>, CoreError> {
    let host = host.clone();
    let parent = parent.to_owned();
    let request = request.to_owned();
    let credential = target.credential.0;
    let model = target.upstream_model.clone();
    let write_host = host.clone();
    let begin = async move {
        // Retain the lease through the INSERT even when its caller disappears.
        // Otherwise a cancelled INSERT can commit after the cleanup UPDATE.
        #[cfg(not(target_arch = "wasm32"))]
        let lease = std::sync::Arc::new(Activity {
            host: write_host.clone(),
            request: request.clone(),
            credential,
            started,
        }) as CredentialUsageLease;
        write_host
            .services
            .store
            .begin_credential_attempt(&request, &parent, credential, &model, started)
            .await
            .map_err(store_error)?;
        #[cfg(not(target_arch = "wasm32"))]
        return Ok(Some(lease));
        // Edge hosts drain inline; admission completion closes the parent.
        #[cfg(target_arch = "wasm32")]
        Ok(None)
    };
    #[cfg(not(target_arch = "wasm32"))]
    return host
        .services
        .spawner
        .spawn_tracked(begin)
        .await
        .map_err(|e| CoreError::Internal(format!("quota activity task failed: {e}")))?;
    #[cfg(target_arch = "wasm32")]
    begin.await
}

#[cfg(not(target_arch = "wasm32"))]
struct Activity {
    host: AppHost,
    request: String,
    credential: i64,
    started: i64,
}

#[cfg(not(target_arch = "wasm32"))]
impl gproxy_core::host::CredentialUsageActivity for Activity {}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for Activity {
    fn drop(&mut self) {
        use gproxy_core::Spawner;
        let host = self.host.clone();
        let request = self.request.clone();
        let credential = self.credential;
        let started = self.started;
        self.host.services.spawner.spawn(Box::pin(async move {
            let result = super::settlement_retry::run(&host, || async {
                host.services
                    .store
                    .finish_credential_attempt(&request, credential, started)
                    .await
                    .map_err(store_error)
            })
            .await;
            if let Err(error) = result {
                // No successful cleanup is inferred from time or a failed write.
                // The unresolved usage remains conservative for quota estimates.
                tracing::error!(request_id = request, credential_id = credential,
                    started_at_ms = started, error = %error, "quota.activity.finish_failed");
            }
        }));
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use gproxy_core::{ControlPlane, Host};

    #[tokio::test]
    async fn cancellation_and_late_drop_preserve_attempt_identity_and_settlement() {
        let fixture = crate::tests::setup::fixture().await;
        let host = &fixture.app.inner.host;
        let plan = host
            .services
            .control
            .resolve(
                Some("public-model"),
                &gproxy_core::RoutingMode::Aggregated,
                None,
            )
            .unwrap();
        let target = &plan.targets[0];
        let first = host
            .track_credential_usage("activity", "activity", target, 10)
            .await
            .unwrap();
        let duplicate = first.clone();
        drop(first);
        let connection =
            tokio_rusqlite::Connection::open(fixture._directory.path().join("gproxy.db"))
                .await
                .unwrap();
        let read = || {
            connection.call(|db| -> Result<_, tokio_rusqlite::rusqlite::Error> {
                db.query_row(
                    "SELECT state FROM credential_quota_activity WHERE request_id = 'activity'",
                    [],
                    |r| r.get::<_, String>(0),
                )
            })
        };
        assert_eq!(read().await.unwrap(), "in_flight");
        drop(duplicate); // cancellation, before any usage settlement
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while read().await.unwrap() != "unresolved" {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let lease = host
            .track_credential_usage("activity", "activity:attempt:1", target, 11)
            .await
            .unwrap();
        let input = serde_json::from_value(serde_json::json!({
            "request_id": "activity:attempt:1", "credential_id": target.credential.0,
            "provider_id": target.provider.id, "upstream_started_at_ms": 11, "at": 10,
            "upstream_model": target.upstream_model, "input_tokens": 3, "output_tokens": 2,
            "cached_input_tokens": 0, "metrics": {}, "dimensions": {}, "cost": "0.01",
            "usage_source": "upstream", "ended": "complete", "latency_ms": 1
        }))
        .unwrap();
        host.services.store.record_usage(&input).await.unwrap();
        host.finish_admission("activity", None).await;
        drop(lease);
        fixture.app.shutdown();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fixture.app.drain_background(),
        )
        .await
        .unwrap();
        let rows = connection.call(|db| -> Result<_, tokio_rusqlite::rusqlite::Error> {
            db.prepare("SELECT request_id, parent_request_id, state FROM credential_quota_activity ORDER BY started_at_ms")?
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)))?
                .collect::<Result<Vec<_>, _>>()
        }).await.unwrap();
        assert_eq!(
            rows,
            [
                ("activity".into(), "activity".into(), "unresolved".into()),
                (
                    "activity:attempt:1".into(),
                    "activity".into(),
                    "settled".into()
                )
            ]
        );
    }
}
