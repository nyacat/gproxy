use super::*;
use gproxy_admin::State;
use gproxy_channel_api::{
    QuotaAllowance, QuotaObservation, QuotaResetBehavior, QuotaSample, QuotaSampleSource,
    QuotaScope, QuotaSubject, QuotaValue,
};
use gproxy_store::records::{QuotaBoundaryConfidence, QuotaBoundarySource};
use std::time::Duration;

fn entry(source: &str, id: &str, observed_at_ms: i64) -> QuotaEntry {
    QuotaEntry {
        id: id.into(),
        source_id: source.into(),
        label: None,
        subject: QuotaSubject::Key,
        model_scope: QuotaScope::All,
        observed_at_ms,
        value: QuotaValue::RateLimit(QuotaAllowance {
            used: Some(5.into()),
            limit: Some(100.into()),
            ..Default::default()
        }),
    }
}

fn observation(credential_id: i64, received_at_ms: i64) -> CredentialQuotaObservation {
    CredentialQuotaObservation {
        credential_id,
        window_key: "window".into(),
        unit: Some("requests".into()),
        reset_behavior: QuotaResetBehavior::Periodic,
        scope: QuotaScope::All,
        sample: QuotaSample {
            source: QuotaSampleSource::Response,
            started_at_ms: received_at_ms - 1,
            received_at_ms,
        },
        label: None,
        period_start: Some(received_at_ms / 1000 - 60),
        period_end: Some(received_at_ms / 1000 + 3600),
        boundary_source: QuotaBoundarySource::Upstream,
        boundary_confidence: QuotaBoundaryConfidence::Derived,
        observed_at: received_at_ms / 1000,
        upstream_used: Some(5.into()),
        upstream_limit: Some(100.into()),
        used_percent: Some(5.into()),
    }
}

#[test]
fn coalescing_keeps_latest_per_source_and_credential_version() {
    let queue = QuotaObserveQueue::default();
    queue.push(1, observation(7, 20));
    queue.push(1, observation(7, 10));
    queue.push(2, observation(7, 30));
    queue.push_entries(7, 1, vec![entry("first", "shared", 20)]);
    queue.push_entries(7, 1, vec![entry("first", "shared", 10)]);
    queue.push_entries(7, 1, vec![entry("second", "shared", 15)]);
    queue.push_entries(7, 2, vec![entry("first", "shared", 30)]);
    let mut pending = queue.take();
    assert_eq!(pending.len(), 2);
    let first = pending.remove(&(7, 1)).unwrap();
    assert_eq!(first.observations["window"].sample.received_at_ms, 20);
    assert_eq!(first.entries.len(), 2);
    assert_eq!(
        first.entries[&("first".into(), "shared".into())].observed_at_ms,
        20
    );
    let second = pending.remove(&(7, 2)).unwrap();
    assert_eq!(second.observations["window"].sample.received_at_ms, 30);
    assert_eq!(second.entries.len(), 1);
    assert!(queue.is_empty());
}

#[tokio::test]
async fn response_observations_return_while_storage_is_locked_and_drain_afterwards() {
    let fixture = crate::tests::setup::fixture().await;
    let app = &fixture.app;
    app.shutdown();
    app.drain_background().await;
    let host = &app.inner.host;
    let id = fixture.credential;
    let version = app.store().credential(id).await.unwrap().unwrap().version;
    let locked = tokio_rusqlite::Connection::open(fixture._directory.path().join("gproxy.db"))
        .await
        .unwrap();
    locked
        .call(|connection| connection.execute_batch("BEGIN IMMEDIATE"))
        .await
        .unwrap();
    let now = crate::quota_refresh::now() * 1000;
    let reading = observation(id, now);
    let wire = QuotaObservation {
        unit: reading.unit.clone(),
        reset_behavior: reading.reset_behavior,
        scope: reading.scope.clone(),
        sample: Some(reading.sample),
        window_key: reading.window_key.clone(),
        label: reading.label.clone(),
        period_start: reading.period_start,
        period_end: reading.period_end,
        used_percent: reading.used_percent,
        upstream_used: reading.upstream_used,
        upstream_limit: reading.upstream_limit,
    };
    tokio::time::timeout(Duration::from_millis(250), async {
        host.observe_credential_quota(gproxy_core::CredentialId(id), version, vec![wire])
            .await;
        host.observe_credential_quota_entries(
            gproxy_core::CredentialId(id),
            version,
            vec![entry("rate_limits", "model", now)],
        )
        .await;
    })
    .await
    .expect("response quota writes must stay off the request path");
    locked
        .call(|connection| connection.execute_batch("COMMIT"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), app.drain_background())
        .await
        .unwrap();
    assert_eq!(
        app.store()
            .credential_quota_snapshot(id)
            .await
            .unwrap()
            .entries
            .len(),
        1
    );
    assert_eq!(
        app.store()
            .credential_quota_windows(Some(id), now / 1000)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn queued_observations_cannot_write_after_credential_rotation() {
    let fixture = crate::tests::setup::fixture().await;
    let app = &fixture.app;
    app.shutdown();
    app.drain_background().await;
    let host = &app.inner.host;
    let credential = app
        .store()
        .credential(fixture.credential)
        .await
        .unwrap()
        .unwrap();
    let now = crate::quota_refresh::now() * 1000;
    host.services
        .quota_observe
        .push(credential.version, observation(credential.id, now));
    host.services.quota_observe.push_entries(
        credential.id,
        credential.version,
        vec![entry("rate_limits", "model", now)],
    );
    app.store()
        .persist_credential_rotation(credential.id, &credential.envelope, credential.version)
        .await
        .unwrap();
    drain(host.clone()).await;
    assert!(
        app.store()
            .credential_quota_snapshot(credential.id)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    assert!(
        app.store()
            .credential_quota_windows(Some(credential.id), now / 1000)
            .await
            .unwrap()
            .is_empty()
    );
}
