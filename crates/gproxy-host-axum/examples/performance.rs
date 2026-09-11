//! Reproducible local PostgreSQL + Redis exercise; configured by scripts/perf/run.py.
//! The mock, gateway and closed-loop load generator share one process, so CPU/RSS
//! describe that complete process. This is not an open-loop saturation benchmark.

#[path = "../tests/boot/seed.rs"]
mod seed;

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use base64::Engine as _;
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use gproxy_app::{App, AppHandle, Config, ControlMutation};
use gproxy_store::records::{QuotaInput, SettingInput, UsageFilter};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{Value, json};

type Error = Box<dyn std::error::Error + Send + Sync>;
type MockTask = tokio::task::JoinHandle<Result<(), std::io::Error>>;

fn main() -> Result<(), Error> {
    let workers = number("PERF_WORKERS", 8_usize)?.max(1);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Error> {
    if std::env::var("GPROXY_PERSISTENCE").as_deref() != Ok("postgres")
        || std::env::var("GPROXY_REDIS_URL")
            .unwrap_or_default()
            .is_empty()
    {
        return Err("set GPROXY_PERSISTENCE=postgres, GPROXY_DSN and GPROXY_REDIS_URL".into());
    }
    let config = Config::from_env()?;
    let backend = config.backend_config();
    let gproxy_store::BackendConfig::Postgres { pool_size, .. } = &backend else {
        return Err("performance runs require a PostgreSQL backend".into());
    };
    let pool_size = *pool_size;
    let mode = std::env::var("PERF_MODE").unwrap_or_else(|_| "gateway".into());
    if mode == "init" || mode == "summary" {
        let store = gproxy_store::Store::open(backend).await?;
        let report = if mode == "summary" {
            summary(&store).await?
        } else {
            json!({"initialized": true})
        };
        return save(report);
    }
    if mode != "gateway" {
        return Err("PERF_MODE must be gateway, init or summary".into());
    }
    let scenario = Scenario::read()?;
    let concurrency = number("PERF_CONCURRENCY", 32_usize)?;
    let requests = number("PERF_REQUESTS", 1_000_usize)?;
    let warmup = number("PERF_WARMUP", 100_usize)?;
    if concurrency == 0 || requests == 0 {
        return Err("PERF_CONCURRENCY and PERF_REQUESTS must be positive".into());
    }
    let upstream_key = random_key()?;
    let client_key = random_key()?;
    let (upstream, stop_mock, mock_task) = start_mock(upstream_key.clone()).await?;
    let app = App::start(config.clone()).await?;
    let quota_id = seed::operational(&app, upstream, upstream_key.clone(), &client_key).await;
    // The observer is idle during load. Update only this isolated fixture before
    // serving requests, then reload the application's control snapshot.
    let store = gproxy_store::Store::open(backend).await?;
    if store.usage_count().await? != 0 {
        return Err("the performance database must be empty before this run".into());
    }
    let snapshot = store.control_snapshot().await?;
    let quota = snapshot
        .quotas
        .iter()
        .find(|quota| quota.id == quota_id)
        .ok_or("missing fixture quota")?;
    let user_key_id = quota.subject_id;
    let user_id = snapshot
        .user_keys
        .iter()
        .find(|key| key.id == user_key_id)
        .ok_or("missing fixture user key")?
        .user_id;
    let identity = Identity {
        quota_id,
        user_id,
        user_key_id,
    };
    let quota_limit = number("PERF_QUOTA_LIMIT", Decimal::from(1000))?;
    let maximum_cost = Decimal::new(2, 5) * Decimal::from(requests.saturating_add(warmup) as u64);
    if quota_limit <= maximum_cost {
        return Err("PERF_QUOTA_LIMIT must exceed the total expected fixture cost".into());
    }
    store
        .update_quota(
            quota_id,
            &QuotaInput {
                subject_kind: quota.subject_kind.clone(),
                subject_id: user_key_id,
                quota_total: Some(quota_limit),
                quota_daily: Some(quota_limit),
                quota_weekly: None,
                quota_monthly: None,
                quota_5h: None,
                quota_7d: None,
                enabled: true,
            },
        )
        .await?;
    app.reload().await?;
    for (key, value) in [
        ("enable_usage", json!(true)),
        ("enable_tokenizer_download", json!(false)),
    ] {
        app.mutate(ControlMutation::Setting(SettingInput {
            key: key.into(),
            value,
        }))
        .await?;
    }
    let server = gproxy_host_axum::AxumServer::bind(app.clone(), config.listen_addr()).await?;
    let gateway_url = format!("http://{}{}", server.local_addr(), scenario.path());
    let direct_url = format!("http://{upstream}/v1/chat/completions");
    let client = wreq::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(60))
        .build()?;
    let direct = Target::new(&client, direct_url, upstream_key, scenario, true);
    let gateway = Target::new(&client, gateway_url, client_key, scenario, false);

    let direct_warmup = load(&direct, concurrency, warmup).await;
    let gateway_warmup = load(&gateway, concurrency, warmup).await;
    let warmup_reconciliation = reconcile(&app, &store, &identity, &gateway_warmup.samples).await?;
    if direct_warmup.errors() != 0
        || gateway_warmup.errors() != 0
        || !warmup_reconciliation["ok"].as_bool().unwrap_or(false)
    {
        return Err(format!(
            "warmup failed: direct={}, gateway={}, reconciliation={warmup_reconciliation}",
            direct_warmup.errors(),
            gateway_warmup.errors()
        )
        .into());
    }
    let direct_result = load(&direct, concurrency, requests).await;
    let gateway_result = load(&gateway, concurrency, requests).await;
    let all_samples: Vec<_> = gateway_warmup
        .samples
        .iter()
        .chain(&gateway_result.samples)
        .cloned()
        .collect();
    let reconciliation = reconcile(&app, &store, &identity, &all_samples).await?;
    let ok =
        direct_result.errors() == 0 && gateway_result.errors() == 0 && reconciliation["ok"] == true;
    let shutdown_at = Instant::now();
    tokio::time::timeout(Duration::from_secs(60), server.shutdown()).await??;
    let shutdown_ms = shutdown_at.elapsed().as_secs_f64() * 1_000.0;
    let _ = stop_mock.send(());
    mock_task.await??;
    let report = json!({
        "schema_version": 1, "mode": mode, "ok": ok,
        "scenario": scenario.name(), "concurrency": concurrency, "requests": requests,
        "warmup_requests": warmup, "workers": number("PERF_WORKERS", 8_usize)?.max(1),
        "pg_pool": pool_size,
        "observer_pg_pool": pool_size,
        "max_in_flight": config.max_in_flight(), "usage_persistence": true,
        "quota_limit": quota_limit,
        "stream_chunk_delay_us": number("PERF_STREAM_DELAY_US", 0_u64)?,
        "direct": direct_result.report(), "gateway": gateway_result.report(),
        "reconciliation": reconciliation, "shutdown_ms": shutdown_ms,
        "measurement_scope": "mock + gateway + closed-loop client in one process; no arrival-rate correction",
        "first_event_definition": "time from request start until the first complete SSE frame (blank-line delimiter)",
        "pool_wait_ms": Value::Null,
        "pool_wait_note": "the public application API does not expose checkout wait instrumentation",
    });
    save(report)?;
    if !ok {
        return Err("request or settlement reconciliation failed; see the JSON artifact".into());
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Scenario {
    Buffered,
    Stream,
    Convert,
}

impl Scenario {
    fn read() -> Result<Self, Error> {
        match std::env::var("PERF_SCENARIO")
            .as_deref()
            .unwrap_or("buffered")
        {
            "buffered" => Ok(Self::Buffered),
            "stream" => Ok(Self::Stream),
            "convert" => Ok(Self::Convert),
            _ => Err("PERF_SCENARIO must be buffered, stream or convert".into()),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::Stream => "stream",
            Self::Convert => "convert",
        }
    }
    fn path(self) -> &'static str {
        match self {
            Self::Convert => "/v1/messages",
            _ => "/v1/chat/completions",
        }
    }
    fn streaming(self) -> bool {
        !matches!(self, Self::Buffered)
    }
}

struct Target {
    client: wreq::Client,
    url: String,
    key: String,
    body: String,
    streaming: bool,
    converted: bool,
    direct: bool,
}

impl Target {
    fn new(
        client: &wreq::Client,
        url: String,
        key: String,
        scenario: Scenario,
        direct: bool,
    ) -> Self {
        let mut body = json!({
            "model": if direct { "upstream-model" } else { "public-model" },
            "messages": [{"role": "user", "content": "hello"}],
            "max_tokens": 16,
            "stream": scenario.streaming(),
        });
        if scenario.streaming() && (direct || !matches!(scenario, Scenario::Convert)) {
            body["stream_options"] = json!({"include_usage": true});
        }
        Self {
            client: client.clone(),
            url,
            key,
            body: body.to_string(),
            streaming: scenario.streaming(),
            converted: matches!(scenario, Scenario::Convert) && !direct,
            direct,
        }
    }
}

#[derive(Clone, Serialize)]
struct Sample {
    elapsed_ms: f64,
    headers_ms: Option<f64>,
    first_event_ms: Option<f64>,
    status: Option<u16>,
    bytes: usize,
    request_id: Option<String>,
    error: Option<String>,
}

async fn request(target: &Target) -> Sample {
    let start = Instant::now();
    let mut sample = Sample {
        elapsed_ms: 0.0,
        headers_ms: None,
        first_event_ms: None,
        status: None,
        bytes: 0,
        request_id: None,
        error: None,
    };
    let result: Result<(), Error> = async {
        let response = target
            .client
            .post(&target.url)
            .bearer_auth(&target.key)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(target.body.clone())
            .send()
            .await?;
        sample.headers_ms = Some(start.elapsed().as_secs_f64() * 1_000.0);
        sample.status = Some(response.status().as_u16());
        sample.request_id = response
            .headers()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let mut stream = response.bytes_stream();
        let mut body = Vec::new();
        while let Some(chunk) = stream.next().await {
            body.extend_from_slice(&chunk?);
            if body.len() > 1_048_576 {
                return Err("unexpected response above 1 MiB".into());
            }
            if target.streaming
                && sample.first_event_ms.is_none()
                && body.windows(2).any(|bytes| bytes == b"\n\n")
            {
                sample.first_event_ms = Some(start.elapsed().as_secs_f64() * 1_000.0);
            }
        }
        sample.bytes = body.len();
        if sample.status != Some(200) {
            return Err(format!("HTTP {}", sample.status.unwrap_or_default()).into());
        }
        if !target.direct && sample.request_id.is_none() {
            return Err("gateway response omitted x-request-id".into());
        }
        if target.streaming {
            let text = std::str::from_utf8(&body)?;
            let terminal = if target.converted {
                "message_stop"
            } else {
                "[DONE]"
            };
            if !text.contains("booted")
                || !text.contains(terminal)
                || sample.first_event_ms.is_none()
            {
                return Err("missing stream content or terminal event".into());
            }
        } else if serde_json::from_slice::<Value>(&body)?["choices"][0]["message"]["content"]
            != "booted"
        {
            return Err("unexpected buffered response".into());
        }
        Ok(())
    }
    .await;
    sample.elapsed_ms = start.elapsed().as_secs_f64() * 1_000.0;
    sample.error = result.err().map(|error| error.to_string());
    sample
}

struct LoadResult {
    elapsed_seconds: f64,
    samples: Vec<Sample>,
    before: Value,
    after: Value,
}

impl LoadResult {
    fn errors(&self) -> usize {
        self.samples
            .iter()
            .filter(|sample| sample.error.is_some())
            .count()
    }
    fn report(&self) -> Value {
        let mut statuses = BTreeMap::<String, usize>::new();
        for sample in &self.samples {
            *statuses
                .entry(
                    sample
                        .status
                        .map_or_else(|| "transport".into(), |v| v.to_string()),
                )
                .or_default() += 1;
        }
        let successful: Vec<_> = self
            .samples
            .iter()
            .filter(|sample| sample.error.is_none())
            .collect();
        let ticks_before = self.before["cpu_ticks"].as_u64();
        let ticks_after = self.after["cpu_ticks"].as_u64();
        let tick_hz = number("PERF_CLOCK_TICKS", 100_u64).unwrap_or(100).max(1);
        let cpu_ms = ticks_before
            .zip(ticks_after)
            .map(|(before, after)| after.saturating_sub(before) as f64 * 1_000.0 / tick_hz as f64);
        json!({
            "seconds": self.elapsed_seconds, "completed": self.samples.len(), "errors": self.errors(),
            "successful": successful.len(), "throughput_rps": self.samples.len() as f64 / self.elapsed_seconds,
            "successful_rps": successful.len() as f64 / self.elapsed_seconds,
            "all_latency_ms": percentiles(self.samples.iter().map(|s| s.elapsed_ms).collect()),
            "successful_latency_ms": percentiles(successful.iter().map(|s| s.elapsed_ms).collect()),
            "first_event_ms": percentiles(successful.iter().filter_map(|s| s.first_event_ms).collect()),
            "statuses": statuses, "cpu_ms": cpu_ms, "resources_before": self.before, "resources_after": self.after,
            "samples": self.samples,
        })
    }
}

async fn load(target: &Target, concurrency: usize, count: usize) -> LoadResult {
    let before = resources();
    let start = Instant::now();
    let samples = stream::iter(0..count)
        .map(|_| request(target))
        .buffer_unordered(concurrency)
        .collect()
        .await;
    LoadResult {
        elapsed_seconds: start.elapsed().as_secs_f64(),
        samples,
        before,
        after: resources(),
    }
}

struct Identity {
    quota_id: i64,
    user_id: i64,
    user_key_id: i64,
}

async fn reconcile(
    app: &AppHandle,
    store: &gproxy_store::Store,
    identity: &Identity,
    samples: &[Sample],
) -> Result<Value, Error> {
    let start = Instant::now();
    let expected = samples
        .iter()
        .filter(|sample| sample.error.is_none())
        .count() as u64;
    let mut pending: Vec<&str> = samples
        .iter()
        .filter_map(|s| s.request_id.as_deref())
        .collect();
    loop {
        let checks = stream::iter(pending)
            .map(|id| async move {
                Ok::<_, gproxy_app::AppError>((id, app.admission_pending(id).await?))
            })
            .buffer_unordered(32)
            .collect::<Vec<_>>()
            .await;
        pending = Vec::new();
        for check in checks {
            let (id, exists) = check?;
            if exists {
                pending.push(id);
            }
        }
        if pending.is_empty() || start.elapsed() >= Duration::from_secs(60) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let drain_ms = start.elapsed().as_secs_f64() * 1_000.0;
    let totals = store.usage_summary(&all_usage()).await?;
    let windows = app.quota_windows().await?;
    // Sum retained windows so a run crossing midnight still reconciles exactly.
    let quota_cost = |kind| {
        windows
            .iter()
            .filter(|window| window.quota_id == identity.quota_id && window.window_kind == kind)
            .map(|window| window.cost_used)
            .sum::<Decimal>()
    };
    let daily_cost = quota_cost(gproxy_store::records::QuotaWindowKind::Daily);
    let total_cost = quota_cost(gproxy_store::records::QuotaWindowKind::Total);
    let expected_cost = Decimal::new(2, 5) * Decimal::from(expected);
    let attributed = store
        .usage_summary(&UsageFilter {
            user_id: Some(identity.user_id),
            user_key_id: Some(identity.user_key_id),
            usage_source: Some("upstream".into()),
            ended: Some("complete".into()),
            ..all_usage()
        })
        .await?;
    let ok = pending.is_empty()
        && totals.requests == expected
        && totals.input_tokens == expected * 10
        && totals.output_tokens == expected * 5
        && totals.cost == expected_cost
        && daily_cost == expected_cost
        && total_cost == expected_cost
        && attributed == totals;
    Ok(
        json!({"ok": ok, "expected_successful_requests_including_warmup": expected, "totals": totals,
        "expected_cost": expected_cost, "daily_quota_cost": daily_cost, "total_quota_cost": total_cost,
        "pending_admissions": pending.len(), "drain_ms": drain_ms,
        "drain_definition": "time to verify every admission is absent; includes observer Redis reads",
        "identity_and_completion_checked_rows": attributed.requests,
        "expected_user_id": identity.user_id, "expected_user_key_id": identity.user_key_id}),
    )
}

fn all_usage() -> UsageFilter {
    UsageFilter {
        from: 0,
        to: i64::MAX,
        ..Default::default()
    }
}

async fn summary(store: &gproxy_store::Store) -> Result<Value, Error> {
    let count = store.usage_count().await?;
    if count == 0 {
        return Err("seed usage_rows before PERF_MODE=summary".into());
    }
    let rounds = number("PERF_ROUNDS", 3_usize)?.max(1);
    // Warm all three operations; keep first-touch IO outside the comparison.
    store.usage_summary(&all_usage()).await?;
    store.usage_records(&all_usage(), 1, 100).await?;
    store
        .usage_records(&all_usage(), count.div_ceil(100), 100)
        .await?;
    let mut measurements = Vec::new();
    for round in 1..=rounds {
        let before = resources();
        let start = Instant::now();
        let totals = store.usage_summary(&all_usage()).await?;
        let summary_ms = start.elapsed().as_secs_f64() * 1_000.0;
        let start = Instant::now();
        let (_, first_count) = store.usage_records(&all_usage(), 1, 100).await?;
        let first_page_ms = start.elapsed().as_secs_f64() * 1_000.0;
        let start = Instant::now();
        let (last, last_count) = store
            .usage_records(&all_usage(), count.div_ceil(100), 100)
            .await?;
        let last_page_ms = start.elapsed().as_secs_f64() * 1_000.0;
        if totals.requests != count
            || totals.input_tokens != count * 10
            || totals.output_tokens != count * 5
            || totals.cost != Decimal::new(2, 5) * Decimal::from(count)
            || first_count != count
            || last_count != count
            || last.is_empty()
        {
            return Err("summary or pagination totals do not match the fixture".into());
        }
        measurements.push(json!({"round": round, "summary_ms": summary_ms, "first_page_ms": first_page_ms,
            "last_page_ms": last_page_ms, "totals": totals, "resources_before": before, "resources_after": resources()}));
    }
    Ok(
        json!({"schema_version": 1, "mode": "summary", "ok": true, "rows": count, "rounds": measurements}),
    )
}

#[derive(Clone)]
struct MockState {
    authorization: String,
    chunks: Arc<Vec<Bytes>>,
    delay: Duration,
}

async fn start_mock(
    key: String,
) -> Result<
    (
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        MockTask,
    ),
    Error,
> {
    let chunks = vec![
        json!({"id":"chatcmpl-perf","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[{"index":0,"delta":{"role":"assistant","content":"booted"},"finish_reason":null}]}),
        json!({"id":"chatcmpl-perf","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
        json!({"id":"chatcmpl-perf","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}),
    ].into_iter().map(|event| Bytes::from(format!("data: {event}\n\n"))).chain(std::iter::once(Bytes::from_static(b"data: [DONE]\n\n"))).collect();
    let state = MockState {
        authorization: format!("Bearer {key}"),
        chunks: Arc::new(chunks),
        delay: Duration::from_micros(number("PERF_STREAM_DELAY_US", 0_u64)?),
    };
    let router = Router::new()
        .route("/v1/chat/completions", axum::routing::post(mock))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown, receiver) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = receiver.await;
            })
            .await
    });
    Ok((address, shutdown, task))
}

async fn mock(
    State(state): State<MockState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if headers
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        != Some(&state.authorization)
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if body["model"] != "upstream-model" {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if body["stream"] == true {
        let chunks = state.chunks.as_ref().clone();
        let stream = stream::iter(chunks).then(move |chunk| async move {
            if !state.delay.is_zero() {
                tokio::time::sleep(state.delay).await;
            }
            Ok::<_, Infallible>(chunk)
        });
        return (
            [(http::header::CONTENT_TYPE, "text/event-stream")],
            axum::body::Body::from_stream(stream),
        )
            .into_response();
    }
    Json(json!({"id":"chatcmpl-perf","object":"chat.completion","created":1,"model":"upstream-model",
        "choices":[{"index":0,"message":{"role":"assistant","content":"booted"},"finish_reason":"stop"}],
        "usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}})).into_response()
}

fn percentiles(mut values: Vec<f64>) -> Value {
    if values.is_empty() {
        return Value::Null;
    }
    values.sort_by(f64::total_cmp);
    let p = |fraction: f64| {
        values[((values.len() as f64 * fraction).ceil() as usize)
            .saturating_sub(1)
            .min(values.len() - 1)]
    };
    json!({"p50": p(0.50), "p95": p(0.95), "p99": p(0.99), "max": values.last(), "samples": values.len()})
}

fn resources() -> Value {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let kb = |name: &str| {
        status.lines().find_map(|line| {
            line.strip_prefix(name)?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
        })
    };
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let fields: Vec<_> = stat
        .rsplit_once(") ")
        .map(|(_, rest)| rest.split_whitespace().collect())
        .unwrap_or_default();
    let ticks = fields
        .get(11)
        .and_then(|v| v.parse::<u64>().ok())
        .zip(fields.get(12).and_then(|v| v.parse::<u64>().ok()))
        .map(|(a, b)| a + b);
    json!({"rss_kib": kb("VmRSS:"), "process_peak_rss_kib": kb("VmHWM:"), "cpu_ticks": ticks})
}

fn number<T: std::str::FromStr>(name: &str, default: T) -> Result<T, Error> {
    match std::env::var(name) {
        Ok(value) => value.parse().map_err(|_| format!("invalid {name}").into()),
        Err(_) => Ok(default),
    }
}

fn random_key() -> Result<String, Error> {
    let mut bytes = [0_u8; 24];
    getrandom::fill(&mut bytes).map_err(|_| "random key generation failed")?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn save(report: Value) -> Result<(), Error> {
    let path = std::path::PathBuf::from(
        std::env::var("PERF_OUTPUT").unwrap_or_else(|_| "target/perf-review/result.json".into()),
    );
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, serde_json::to_vec_pretty(&report)?)?;
    println!("{}", json!({"artifact": path, "ok": report.get("ok")}));
    Ok(())
}
