use gproxy_core::{ControlPlane, Host, UsageSink};
use rust_decimal::Decimal;

use super::*;

async fn assert_completed(host: &AppHost, request_id: &str) {
    assert_eq!(
        host.services
            .store
            .get_settlement_replay(request_id)
            .await
            .unwrap()
            .unwrap()["completed"],
        true
    );
    assert!(
        host.services
            .store
            .list_settlement_replays(None, 100)
            .await
            .unwrap()
            .iter()
            .all(|(id, _)| id != request_id)
    );
}

async fn admitted(
    id: &str,
) -> (
    crate::tests::setup::Fixture,
    gproxy_core::Settlement,
    gproxy_core::CallerIdentity,
) {
    let fixture = crate::tests::setup::fixture().await;
    fixture.app.shutdown();
    fixture.app.drain_background().await;
    let (settlement, identity) = admit_on(&fixture, id).await;
    (fixture, settlement, identity)
}

async fn admit_on(
    fixture: &crate::tests::setup::Fixture,
    id: &str,
) -> (gproxy_core::Settlement, gproxy_core::CallerIdentity) {
    let host = &fixture.app.inner.host;
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        format!("Bearer {}", fixture.client_key).parse().unwrap(),
    );
    let request = gproxy_core::RequestCtx {
        request_id: id.into(),
        client_ip: None,
        method: http::Method::POST,
        path: "/v1/chat/completions".into(),
        query: None,
        headers,
        body: bytes::Bytes::from_static(
            br#"{"model":"public-model","messages":[{"role":"user","content":"hello"}]}"#,
        ),
        upgrade: false,
        force_model_refresh: false,
        mode: gproxy_core::RoutingMode::Aggregated,
    };
    let identity = host.authenticate(&request).await.unwrap();
    let plan = host
        .services
        .control
        .resolve(Some("public-model"), &request.mode, None)
        .unwrap();
    let operation = gproxy_protocol::OperationKey::content(
        gproxy_protocol::Operation::GenerateContent,
        gproxy_protocol::ContentGenerationKind::OpenAiChat,
    );
    host.admit(&identity, &request, Some(operation), None, &plan)
        .await
        .unwrap();
    let settlement = gproxy_core::Settlement {
        request_id: id.into(),
        provider_id: fixture.provider,
        credential_id: gproxy_core::CredentialId(fixture.credential),
        upstream_model: "upstream-model".into(),
        upstream_started_at_ms: Some(crate::quota_refresh::now() * 1000),
        usage: gproxy_core::NormalizedUsage {
            input_tokens: 3,
            output_tokens: 5,
            ..Default::default()
        },
        cost: Decimal::new(1, 2),
        source: gproxy_core::UsageSource::Upstream,
        ended: gproxy_core::Ended::Complete,
        latency_ms: 17,
        attempts: vec![],
    };
    (settlement, identity)
}

#[tokio::test]
async fn failed_identity_read_retains_payload_and_replay_restores_original_owner() {
    let (fixture, settlement, identity) = admitted("recover-identity").await;
    let host = &fixture.app.inner.host;
    host.services
        .cache
        .testing
        .fail_once("get", "gproxy:admission:", false);
    assert!(host.record_checked(&settlement).await.is_err());
    assert!(
        fixture
            .app
            .admission_pending(&settlement.request_id)
            .await
            .unwrap()
    );
    assert!(
        fixture
            .app
            .usage_by_request(&settlement.request_id)
            .await
            .unwrap()
            .is_none()
    );
    let payload = host
        .services
        .store
        .get_settlement_replay(&settlement.request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(payload["identity_required"], true);
    assert!(payload["inputs"].is_null());
    replay_one(host, &settlement.request_id, payload)
        .await
        .unwrap();
    let row = fixture
        .app
        .usage_by_request(&settlement.request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.usage.user_id, Some(identity.user_id));
    assert_eq!(row.usage.user_key_id, Some(identity.user_key_id));
    assert_eq!(row.usage.cost, settlement.cost);
    assert!(
        !fixture
            .app
            .admission_pending(&settlement.request_id)
            .await
            .unwrap()
    );
    assert_completed(host, &settlement.request_id).await;
}

#[tokio::test]
async fn missing_identity_after_failed_read_never_becomes_anonymous_usage() {
    let (fixture, settlement, _) = admitted("recover-missing-identity").await;
    let host = &fixture.app.inner.host;
    host.services
        .cache
        .testing
        .fail_once("get", "gproxy:admission:", false);
    assert!(host.record_checked(&settlement).await.is_err());
    host.services
        .cache
        .delete(&format!("gproxy:admission:{}", settlement.request_id))
        .await
        .unwrap();
    let payload = host
        .services
        .store
        .get_settlement_replay(&settlement.request_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        replay_one(host, &settlement.request_id, payload)
            .await
            .is_err()
    );
    assert!(
        fixture
            .app
            .usage_by_request(&settlement.request_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        host.services
            .store
            .get_settlement_replay(&settlement.request_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn rejected_usage_write_is_durable_and_keeps_identity_until_repaired() {
    let (fixture, mut settlement, identity) = admitted("recover-write").await;
    let host = &fixture.app.inner.host;
    let started_at = settlement.upstream_started_at_ms;
    settlement.upstream_started_at_ms = None;
    assert!(host.record_checked(&settlement).await.is_err());
    assert!(
        fixture
            .app
            .admission_pending(&settlement.request_id)
            .await
            .unwrap()
    );
    let mut payload = host
        .services
        .store
        .get_settlement_replay(&settlement.request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(payload["inputs"][0]["user_id"], identity.user_id);
    assert_eq!(payload["usage_recorded"], false);
    payload["inputs"][0]["upstream_started_at_ms"] = serde_json::json!(started_at);
    payload["settlement"]["upstream_started_at_ms"] = serde_json::json!(started_at);
    host.services
        .store
        .put_settlement_replay(&settlement.request_id, &payload)
        .await
        .unwrap();
    replay_one(host, &settlement.request_id, payload)
        .await
        .unwrap();
    assert!(
        fixture
            .app
            .usage_by_request(&settlement.request_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        !fixture
            .app
            .admission_pending(&settlement.request_id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn cache_recovery_is_enumerable_and_cannot_overwrite_sql_progress() {
    let (fixture, settlement, _) = admitted("recover-cache").await;
    let host = &fixture.app.inner.host;
    host.services
        .cache
        .testing
        .fail_once("get", "gproxy:admission:", false);
    assert!(host.record_checked(&settlement).await.is_err());
    let payload = host
        .services
        .store
        .get_settlement_replay(&settlement.request_id)
        .await
        .unwrap()
        .unwrap();
    host.services
        .store
        .delete_settlement_replay(&settlement.request_id)
        .await
        .unwrap();
    cache_insert(host, &settlement.request_id, &payload)
        .await
        .unwrap();
    assert!(
        host.services
            .cache
            .get(&bucket_key(bucket(&settlement.request_id)))
            .await
            .unwrap()
            .is_some()
    );
    promote_cache(host, bucket(&settlement.request_id))
        .await
        .unwrap();
    let promoted = host
        .services
        .store
        .get_settlement_replay(&settlement.request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(promoted, payload);
    assert!(
        host.services
            .cache
            .get(&bucket_key(bucket(&settlement.request_id)))
            .await
            .unwrap()
            .is_none()
    );
    let mut advanced = payload.clone();
    advanced["usage_recorded"] = serde_json::json!(true);
    host.services
        .store
        .put_settlement_replay(&settlement.request_id, &advanced)
        .await
        .unwrap();
    cache_insert(host, &settlement.request_id, &payload)
        .await
        .unwrap();
    promote_cache(host, bucket(&settlement.request_id))
        .await
        .unwrap();
    assert_eq!(
        host.services
            .store
            .get_settlement_replay(&settlement.request_id)
            .await
            .unwrap(),
        Some(advanced)
    );
}

#[tokio::test]
async fn interrupted_admission_refund_replays_without_creating_usage() {
    let (fixture, settlement, _) = admitted("recover-refund").await;
    let host = &fixture.app.inner.host;
    retain_refund(host, &settlement.request_id).await;
    let payload = host
        .services
        .store
        .get_settlement_replay(&settlement.request_id)
        .await
        .unwrap()
        .unwrap();
    replay_one(host, &settlement.request_id, payload)
        .await
        .unwrap();
    assert!(
        !fixture
            .app
            .admission_pending(&settlement.request_id)
            .await
            .unwrap()
    );
    assert!(
        fixture
            .app
            .usage_by_request(&settlement.request_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_completed(host, &settlement.request_id).await;
}

#[tokio::test]
async fn stale_cache_copy_cannot_resurrect_a_completed_settlement() {
    let (fixture, settlement, _) = admitted("recover-stale-cache").await;
    let host = &fixture.app.inner.host;
    host.services
        .cache
        .testing
        .fail_once("get", "gproxy:admission:", false);
    assert!(host.record_checked(&settlement).await.is_err());
    let original = host
        .services
        .store
        .get_settlement_replay(&settlement.request_id)
        .await
        .unwrap()
        .unwrap();
    cache_insert(host, &settlement.request_id, &original)
        .await
        .unwrap();
    host.services
        .cache
        .testing
        .fail_once("compare_swap", "gproxy:settlement-recovery:", false);
    assert!(
        promote_cache(host, bucket(&settlement.request_id))
            .await
            .is_err()
    );
    replay_one(host, &settlement.request_id, original.clone())
        .await
        .unwrap();
    assert_completed(host, &settlement.request_id).await;
    promote_cache(host, bucket(&settlement.request_id))
        .await
        .unwrap();
    assert_completed(host, &settlement.request_id).await;
    // A timeout may even deliver the backup write after SQL has completed.
    cache_insert(host, &settlement.request_id, &original)
        .await
        .unwrap();
    promote_cache(host, bucket(&settlement.request_id))
        .await
        .unwrap();
    assert_completed(host, &settlement.request_id).await;
    assert!(
        host.services
            .cache
            .get(&bucket_key(bucket(&settlement.request_id)))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn actual_settlement_upgrades_refund_without_losing_progress() {
    let (fixture, settlement, _) = admitted("recover-upgrade").await;
    let host = &fixture.app.inner.host;
    retain_refund(host, &settlement.request_id).await;
    host.services
        .cache
        .testing
        .fail_once("get", "gproxy:admission:", false);
    assert!(host.record_checked(&settlement).await.is_err());
    let actual = host
        .services
        .store
        .get_settlement_replay(&settlement.request_id)
        .await
        .unwrap()
        .unwrap();
    assert!(actual.get("refund_request_id").is_none());
    assert_eq!(actual["settlement"]["request_id"], settlement.request_id);
    retain_refund(host, &settlement.request_id).await;
    assert_eq!(
        host.services
            .store
            .get_settlement_replay(&settlement.request_id)
            .await
            .unwrap(),
        Some(actual.clone())
    );
    replay_one(host, &settlement.request_id, actual)
        .await
        .unwrap();
    assert!(
        fixture
            .app
            .usage_by_request(&settlement.request_id)
            .await
            .unwrap()
            .is_some()
    );
    assert_completed(host, &settlement.request_id).await;
}

#[tokio::test]
async fn failed_usage_replay_keeps_the_original_credential_window_after_rollover() {
    use gproxy_store::records::{QuotaInput, QuotaWindowKind};
    let (fixture, mut settlement, _) = admitted("recover-window").await;
    let crate::MutationResult::Id(quota_id) = fixture
        .app
        .mutate(crate::ControlMutation::Quota(QuotaInput {
            subject_kind: "credential".into(),
            subject_id: fixture.credential,
            quota_total: None,
            quota_monthly: None,
            quota_weekly: None,
            quota_daily: Some(100.into()),
            quota_5h: None,
            quota_7d: None,
            enabled: false,
        }))
        .await
        .unwrap()
    else {
        panic!("quota id")
    };
    let host = &fixture.app.inner.host;
    let started_at = settlement.upstream_started_at_ms;
    settlement.upstream_started_at_ms = None;
    assert!(host.record_checked(&settlement).await.is_err());
    let mut payload = host
        .services
        .store
        .get_settlement_replay(&settlement.request_id)
        .await
        .unwrap()
        .unwrap();
    let original_id = payload["credential_charges"][0]["window_id"]
        .as_i64()
        .unwrap();
    let original = host
        .services
        .store
        .quota_window(original_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.cost_used, settlement.cost);
    let next = host
        .services
        .store
        .ensure_quota_window(quota_id, QuotaWindowKind::Daily, original.reset_at.unwrap())
        .await
        .unwrap();
    assert_ne!(original_id, next.id);
    payload["inputs"][0]["upstream_started_at_ms"] = serde_json::json!(started_at);
    host.services
        .store
        .put_settlement_replay(&settlement.request_id, &payload)
        .await
        .unwrap();
    replay_one(host, &settlement.request_id, payload)
        .await
        .unwrap();
    assert_eq!(
        host.services
            .store
            .quota_window(original_id)
            .await
            .unwrap()
            .unwrap()
            .cost_used,
        settlement.cost
    );
    assert_eq!(
        host.services
            .store
            .quota_window(next.id)
            .await
            .unwrap()
            .unwrap()
            .cost_used,
        Decimal::ZERO
    );
    assert_completed(host, &settlement.request_id).await;
}

#[tokio::test]
async fn startup_schedule_automatically_replays_an_existing_queue() {
    let (fixture, settlement, _) = admitted("recover-startup").await;
    let host = &fixture.app.inner.host;
    retain_refund(host, &settlement.request_id).await;
    fixture.app.inner.shutdown.send_replace(false);
    start(&fixture.app);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let current = host
                .services
                .store
                .get_settlement_replay(&settlement.request_id)
                .await
                .unwrap()
                .unwrap();
            if current
                .get("completed")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    fixture.app.shutdown();
    fixture.app.drain_background().await;
    assert!(
        !fixture
            .app
            .admission_pending(&settlement.request_id)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn successful_repeat_callback_advances_an_existing_recovery_payload() {
    let (fixture, settlement, identity) = admitted("recover-repeat-record").await;
    let host = &fixture.app.inner.host;
    host.services
        .cache
        .testing
        .fail_once("get", "gproxy:admission:", false);
    assert!(host.record_checked(&settlement).await.is_err());
    host.record_checked(&settlement).await.unwrap();
    host.finish_admission(&settlement.request_id, Some(&settlement))
        .await;
    assert!(
        !fixture
            .app
            .admission_pending(&settlement.request_id)
            .await
            .unwrap()
    );
    let queued = host
        .services
        .store
        .get_settlement_replay(&settlement.request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(queued["usage_recorded"], true);
    assert_eq!(queued["inputs"][0]["user_id"], identity.user_id);
    replay_one(host, &settlement.request_id, queued)
        .await
        .unwrap();
    assert_completed(host, &settlement.request_id).await;
}

#[test]
fn recovery_progress_never_rebinds_prepared_windows_or_merges_conflicting_usage() {
    let original = serde_json::json!({
        "settlement": {"request_id": "same", "cost": "1"},
        "inputs": null, "credential_charges": [{"window_id": 1, "quota_id": 2}],
        "usage_recorded": false
    });
    let later = serde_json::json!({
        "settlement": {"request_id": "same", "cost": "1"},
        "inputs": [{"user_id": 3}], "credential_charges": [{"window_id": 9, "quota_id": 2}],
        "usage_recorded": true
    });
    let merged = merge_progress(&original, &later).unwrap();
    assert_eq!(merged["credential_charges"][0]["window_id"], 1);
    assert_eq!(merged["inputs"][0]["user_id"], 3);
    assert_eq!(merged["usage_recorded"], true);
    let mut conflict = later;
    conflict["settlement"]["cost"] = serde_json::json!("2");
    assert!(merge_progress(&original, &conflict).is_err());
}

#[tokio::test]
async fn overflow_tracking_returns_to_the_normal_hot_path_after_a_complete_empty_sweep() {
    let fixture = crate::tests::setup::fixture().await;
    fixture.app.shutdown();
    fixture.app.drain_background().await;
    let host = &fixture.app.inner.host;
    let state = &host.services.settlement_recovery;
    {
        let mut cursor = state.cursor.lock().unwrap();
        cursor.overflow = true;
        cursor.known.insert("still-pending".into());
    }
    for _ in 0..CACHE_BUCKETS {
        state.cursor.lock().unwrap().next_poll = None;
        fixture.app.recover_pending_settlements(1).await.unwrap();
    }
    assert!(state.knows("untracked-request"));
    state.forget("still-pending");
    {
        let mut cursor = state.cursor.lock().unwrap();
        cursor.empty_buckets = 0;
        cursor.next_poll = None;
    }
    for _ in 0..CACHE_BUCKETS - 1 {
        state.cursor.lock().unwrap().next_poll = None;
        fixture.app.recover_pending_settlements(1).await.unwrap();
        assert!(state.knows("untracked-request"));
    }
    state.cursor.lock().unwrap().next_poll = None;
    fixture.app.recover_pending_settlements(1).await.unwrap();
    assert!(!state.knows("untracked-request"));
}

/// A test-owned barrier before recovery touches accounting. Each permit admits
/// one replay; its guard measures the whole task, including settlement cleanup.
pub(super) struct ReplayProbe {
    counts: std::sync::Mutex<ReplayCounts>,
    started: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

#[derive(Default)]
struct ReplayCounts {
    requests: Vec<String>,
    active: usize,
    peak: usize,
}

pub(super) struct ReplayGuard {
    probe: std::sync::Arc<ReplayProbe>,
}

impl Drop for ReplayGuard {
    fn drop(&mut self) {
        self.probe.counts.lock().unwrap().active -= 1;
        self.probe.started.notify_one();
    }
}

impl ReplayProbe {
    fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            counts: Default::default(),
            started: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        })
    }

    pub(super) async fn enter(self: &std::sync::Arc<Self>, request_id: &str) -> ReplayGuard {
        {
            let mut counts = self.counts.lock().unwrap();
            counts.requests.push(request_id.into());
            counts.active += 1;
            counts.peak = counts.peak.max(counts.active);
        }
        let guard = ReplayGuard {
            probe: self.clone(),
        };
        self.started.notify_one();
        self.release.acquire().await.unwrap().forget();
        guard
    }

    async fn wait_started(&self, expected: usize) {
        loop {
            let changed = self.started.notified();
            if self.counts.lock().unwrap().requests.len() >= expected {
                return;
            }
            changed.await;
        }
    }

    async fn expect_started(&self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(5), self.wait_started(expected))
            .await
            .expect("recovery tasks should reach the entry barrier");
    }

    async fn wait_active(&self, expected: usize) {
        loop {
            let changed = self.started.notified();
            if self.counts.lock().unwrap().active == expected {
                return;
            }
            changed.await;
        }
    }

    fn snapshot(&self) -> (Vec<String>, usize, usize) {
        let counts = self.counts.lock().unwrap();
        (counts.requests.clone(), counts.active, counts.peak)
    }
}

async fn queued_parallel_replays(
    prefix: &str,
    count: usize,
) -> (
    crate::tests::setup::Fixture,
    Vec<gproxy_core::Settlement>,
    i64,
) {
    use gproxy_store::records::QuotaInput;

    assert!(count > 0);
    let (fixture, first, identity) = admitted(&format!("{prefix}-00")).await;
    let host = &fixture.app.inner.host;
    host.services
        .store
        .update_quota(
            fixture.quota,
            &QuotaInput {
                subject_kind: "user_key".into(),
                subject_id: identity.user_key_id,
                quota_total: Some(1000.into()),
                quota_daily: None,
                quota_weekly: None,
                quota_monthly: None,
                quota_5h: Some(1000.into()),
                quota_7d: None,
                enabled: true,
            },
        )
        .await
        .unwrap();
    fixture.app.reload().await.unwrap();
    let crate::MutationResult::Id(credential_quota) = fixture
        .app
        .mutate(crate::ControlMutation::Quota(QuotaInput {
            subject_kind: "credential".into(),
            subject_id: fixture.credential,
            quota_total: Some(1000.into()),
            quota_daily: None,
            quota_weekly: None,
            quota_monthly: None,
            quota_5h: None,
            quota_7d: None,
            enabled: false,
        }))
        .await
        .unwrap()
    else {
        panic!("credential quota id");
    };
    let mut settlements = vec![first];
    for index in 1..count {
        settlements.push(admit_on(&fixture, &format!("{prefix}-{index:02}")).await.0);
    }
    for settlement in &settlements {
        host.services
            .cache
            .testing
            .fail_once("get", "gproxy:admission:", false);
        assert!(host.record_checked(settlement).await.is_err());
    }
    {
        let mut cursor = host.services.settlement_recovery.cursor.lock().unwrap();
        cursor.after = None;
        cursor.next_poll = None;
    }
    (fixture, settlements, credential_quota)
}

async fn assert_parallel_accounting(
    fixture: &crate::tests::setup::Fixture,
    settlements: &[gproxy_core::Settlement],
    credential_quota: i64,
) {
    let host = &fixture.app.inner.host;
    let expected: Decimal = settlements.iter().map(|settlement| settlement.cost).sum();
    let windows: Vec<_> = host
        .services
        .store
        .quota_windows()
        .await
        .unwrap()
        .into_iter()
        .filter(|window| [fixture.quota, credential_quota].contains(&window.quota_id))
        .collect();
    assert_eq!(
        windows.len(),
        3,
        "two user windows and one credential window"
    );
    for window in windows {
        assert_eq!(window.cost_used, expected, "window {}", window.id);
        for settlement in settlements {
            assert!(
                host.services
                    .store
                    .quota_settlement_exists(&settlement.request_id, window.id)
                    .await
                    .unwrap()
            );
        }
    }
    for settlement in settlements {
        assert_completed(host, &settlement.request_id).await;
        assert!(
            !fixture
                .app
                .admission_pending(&settlement.request_id)
                .await
                .unwrap()
        );
        let usage = fixture
            .app
            .usage_by_request(&settlement.request_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(usage.usage.cost, settlement.cost);
    }
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn native_recovery_runs_four_at_once_and_settles_shared_windows_exactly() {
    let (fixture, settlements, credential_quota) =
        queued_parallel_replays("parallel-window", 8).await;
    let host = &fixture.app.inner.host;
    let probe = ReplayProbe::new();
    *host.services.settlement_recovery.probe.lock().unwrap() = Some(probe.clone());
    let app = fixture.app.clone();
    let recovery = tokio::spawn(async move { app.recover_pending_settlements(8).await });

    probe.expect_started(4).await;
    let (started, active, peak) = probe.snapshot();
    assert_eq!(started.len(), 4);
    assert_eq!(
        active, 4,
        "four replays must make progress before any is released"
    );
    assert_eq!(peak, 4);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), probe.wait_started(5))
            .await
            .is_err(),
        "the fifth request must stay queued while the first four are blocked"
    );

    probe.release.add_permits(4);
    probe.expect_started(8).await;
    assert_eq!(probe.snapshot().1, 4);
    assert_eq!(probe.snapshot().2, 4);
    probe.release.add_permits(4);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), recovery)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        8
    );
    assert_eq!(probe.snapshot().1, 0);
    assert_eq!(probe.snapshot().2, 4);
    assert_parallel_accounting(&fixture, &settlements, credential_quota).await;
    for window in host.services.store.quota_windows().await.unwrap() {
        if ![fixture.quota, credential_quota].contains(&window.quota_id) {
            continue;
        }
        let bytes = host
            .services
            .cache
            .get(&format!("gproxy:quota-pending:{}", window.id))
            .await
            .unwrap();
        let pending = bytes.map_or(0, |bytes| i64::from_be_bytes(bytes.try_into().unwrap()));
        assert_eq!(
            pending, 0,
            "window {} must release all reservations",
            window.id
        );
    }
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn one_failed_parallel_replay_does_not_stop_the_other_requests() {
    let (fixture, settlements, credential_quota) =
        queued_parallel_replays("parallel-failure", 6).await;
    let host = &fixture.app.inner.host;
    let failed_id = &settlements[0].request_id;
    let mut invalid = host
        .services
        .store
        .get_settlement_replay(failed_id)
        .await
        .unwrap()
        .unwrap();
    invalid["version"] = serde_json::json!(99);
    host.services
        .store
        .put_settlement_replay(failed_id, &invalid)
        .await
        .unwrap();

    assert_eq!(fixture.app.recover_pending_settlements(6).await.unwrap(), 5);
    let pending = host
        .services
        .store
        .list_settlement_replays(None, 100)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].0, *failed_id);
    assert_eq!(pending[0].1["version"], 99);
    assert!(fixture.app.admission_pending(failed_id).await.unwrap());
    assert!(
        fixture
            .app
            .usage_by_request(failed_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_parallel_accounting(&fixture, &settlements[1..], credential_quota).await;
}

#[tokio::test]
#[cfg(not(target_arch = "wasm32"))]
async fn cancelling_a_parallel_pass_keeps_unstarted_rows_and_drains_started_tasks() {
    let (fixture, settlements, credential_quota) =
        queued_parallel_replays("parallel-cancel", 8).await;
    let host = &fixture.app.inner.host;
    let probe = ReplayProbe::new();
    *host.services.settlement_recovery.probe.lock().unwrap() = Some(probe.clone());
    let app = fixture.app.clone();
    let recovery = tokio::spawn(async move { app.recover_pending_settlements(8).await });
    probe.expect_started(4).await;
    recovery.abort();
    assert!(recovery.await.unwrap_err().is_cancelled());

    let cursor_before = {
        let cursor = host.services.settlement_recovery.cursor.lock().unwrap();
        assert!(cursor.running, "started tasks must retain the pass guard");
        (cursor.after.clone(), cursor.slot)
    };
    assert_eq!(fixture.app.recover_pending_settlements(8).await.unwrap(), 0);
    {
        let cursor = host.services.settlement_recovery.cursor.lock().unwrap();
        assert_eq!((cursor.after.clone(), cursor.slot), cursor_before);
        assert!(cursor.running);
    }
    assert_eq!(probe.snapshot().0.len(), 4);
    assert_eq!(
        host.services
            .store
            .list_settlement_replays(None, 100)
            .await
            .unwrap()
            .len(),
        8,
        "cancelling the caller must not remove queued or blocked work"
    );

    let drain = fixture.app.drain_background();
    tokio::pin!(drain);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut drain)
            .await
            .is_err(),
        "drain must wait for all four started replays"
    );
    assert_eq!(probe.snapshot().1, 4);
    probe.release.add_permits(3);
    tokio::time::timeout(Duration::from_secs(5), probe.wait_active(1))
        .await
        .expect("three replays should finish while the last remains blocked");
    let mut completed = 0;
    for settlement in &settlements[..4] {
        let payload = host
            .services
            .store
            .get_settlement_replay(&settlement.request_id)
            .await
            .unwrap()
            .unwrap();
        completed += usize::from(
            payload
                .get("completed")
                .and_then(serde_json::Value::as_bool)
                == Some(true),
        );
    }
    assert_eq!(completed, 3);
    assert!(
        host.services
            .settlement_recovery
            .cursor
            .lock()
            .unwrap()
            .running
    );
    assert_eq!(fixture.app.recover_pending_settlements(8).await.unwrap(), 0);
    assert_eq!(probe.snapshot().0.len(), 4);
    assert_eq!(probe.snapshot().1, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut drain)
            .await
            .is_err(),
        "three completed children must not release the pass or finish drain"
    );
    probe.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), &mut drain)
        .await
        .unwrap();
    assert_eq!(
        probe.snapshot().0.len(),
        4,
        "unstarted work must remain lazy"
    );
    assert_eq!(probe.snapshot().1, 0);
    assert!(
        !host
            .services
            .settlement_recovery
            .cursor
            .lock()
            .unwrap()
            .running
    );

    assert_parallel_accounting(&fixture, &settlements[..4], credential_quota).await;
    let pending = host
        .services
        .store
        .list_settlement_replays(None, 100)
        .await
        .unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<Vec<_>>(),
        settlements[4..]
            .iter()
            .map(|settlement| settlement.request_id.as_str())
            .collect::<Vec<_>>()
    );
    for (request_id, payload) in pending {
        assert!(payload["inputs"].is_null());
        assert_eq!(payload["usage_recorded"], false);
        assert!(fixture.app.admission_pending(&request_id).await.unwrap());
        assert!(
            fixture
                .app
                .usage_by_request(&request_id)
                .await
                .unwrap()
                .is_none()
        );
    }
}
