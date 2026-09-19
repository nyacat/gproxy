use gproxy_channel_api::StreamDecodeError;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::task::ArcWake;
use gproxy_channel_api::{Frame, StreamDecoder, StreamEnd, StreamTail, TransportError};
use gproxy_protocol::{SettleMode, StreamFraming};
use http::{HeaderMap, StatusCode};
use web_time::Instant;

use super::memory::MemoryHost;
use super::{block_on, target};
use crate::funnel::inline::InlineCompletion;
use crate::funnel::{self, FunnelCtx};
use crate::{ByteStream, Disposition, Ended, NormalizedUsage, ResponseBody, UsageSource};

struct Upstream {
    chunks: VecDeque<Result<Bytes, TransportError>>,
    pending: bool,
    polls: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
}

impl Stream for Upstream {
    type Item = Result<Bytes, TransportError>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if let Some(chunk) = self.chunks.pop_front() {
            Poll::Ready(Some(chunk))
        } else if self.pending {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }
}

impl Drop for Upstream {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl ArcWake for WakeCount {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct Decoder;

impl StreamDecoder for Decoder {
    fn push(&mut self, bytes: Bytes) -> Result<Vec<Frame>, StreamDecodeError> {
        Ok(vec![Frame(bytes)])
    }

    fn finish(&mut self, end: StreamEnd) -> Result<StreamTail, StreamDecodeError> {
        Ok(StreamTail {
            estimated_output_chars: None,
            frames: if end == StreamEnd::Complete {
                vec![
                    Frame(Bytes::from_static(b"tail 1")),
                    Frame(Bytes::from_static(b"tail 2")),
                ]
            } else {
                Vec::new()
            },
            usage: Some(NormalizedUsage {
                input_tokens: 13,
                output_tokens: 7,
                ..Default::default()
            }),
            actual_service_tier: Some("priority".into()),
        })
    }
}

fn context() -> FunnelCtx {
    FunnelCtx {
        activity: None,
        health_activity: None,
        health_delegated: false,
        upstream_started_at_ms: Some(1),
        request_id: "cancel-inline".into(),
        target: target(),
        credential_version: Some(4),
        source_key: None,
        key: None,
        source_framing: StreamFraming::Sse,
        target_framing: StreamFraming::Sse,
        settle: SettleMode::OnResponse,
        pricing: None,
        pricing_control: None,
        usage_channel: None,
        started: Instant::now(),
        upstream_url: Some("https://upstream.example/test".into()),
        request_method: Some(http::Method::POST),
        request_body: Bytes::from_static(b"test request"),
        request_headers: None,
        client_headers: HeaderMap::new(),
        requested_model: None,
        response_headers: None,
        dedupe_key: None,
        owner_user_id: None,
        resource: None,
        admitted: true,
        surface_label: None,
        traffic_policy: None,
        traffic_blacklist: None,
    }
}

fn outcome(host: &MemoryHost, stream: ByteStream) -> crate::ExecOutcome {
    block_on(funnel::streaming(
        Arc::new(host.clone()),
        context(),
        http::Response::new(stream),
        Disposition::Success,
        Some(Box::new(Decoder)),
    ))
}

#[test]
fn cancellation_wakes_pending_upstream_drops_it_and_settles_inline_once() {
    for poll_upstream in [false, true] {
        let host = MemoryHost::new(false);
        let polls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        let outcome = outcome(
            &host,
            Box::pin(Upstream {
                chunks: VecDeque::new(),
                pending: true,
                polls: polls.clone(),
                dropped: dropped.clone(),
            }),
        );
        let cancel = outcome
            .stream_cancellation
            .expect("inline cancellation handle");
        let ResponseBody::Stream(mut stream) = outcome.body else {
            panic!("stream");
        };
        let wake_count = Arc::new(WakeCount::default());
        let waker = futures_util::task::waker(wake_count.clone());
        let mut cx = Context::from_waker(&waker);
        if poll_upstream {
            assert!(stream.as_mut().poll_next(&mut cx).is_pending());
        }
        cancel.cancel();
        cancel.cancel();
        assert!(cancel.is_cancelled());
        if poll_upstream {
            assert!(wake_count.0.load(Ordering::SeqCst) > 0);
        }
        assert!(matches!(
            stream.as_mut().poll_next(&mut cx),
            Poll::Ready(Some(Err(_)))
        ));
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(polls.load(Ordering::SeqCst), usize::from(poll_upstream));
        assert!(matches!(
            stream.as_mut().poll_next(&mut cx),
            Poll::Ready(None)
        ));
        drop(stream);

        let state = host.state.lock().unwrap();
        assert!(
            state.health.is_empty(),
            "client cancellation must not change health"
        );
        assert_eq!(state.settlements.len(), 1);
        assert_eq!(state.admission_finishes, [true]);
        let settlement = &state.settlements[0];
        assert_eq!(settlement.ended, Ended::Interrupted);
        assert_eq!(settlement.source, UsageSource::Upstream);
        assert_eq!(settlement.usage.input_tokens, 13);
        assert_eq!(settlement.usage.output_tokens, 7);
        assert_eq!(settlement.usage.dimensions["service_tier"], "priority");
    }
}

#[test]
fn cancellation_after_error_or_eof_does_not_repeat_inline_settlement() {
    for fail in [false, true] {
        let host = MemoryHost::new(false);
        let chunks: Vec<Result<Bytes, TransportError>> = if fail {
            vec![Err(TransportError::Interrupted("upstream failed".into()))]
        } else {
            Vec::new()
        };
        let outcome = outcome(&host, Box::pin(futures_util::stream::iter(chunks)));
        let cancel = outcome.stream_cancellation.unwrap();
        let ResponseBody::Stream(mut stream) = outcome.body else {
            panic!("stream");
        };
        block_on(async { while stream.next().await.is_some() {} });
        cancel.cancel();
        assert!(block_on(stream.next()).is_none());
        drop(stream);
        let state = host.state.lock().unwrap();
        assert_eq!(state.settlements.len(), 1);
        assert_eq!(state.admission_finishes, [true]);
        assert_eq!(
            state.settlements[0].ended,
            if fail {
                Ended::Interrupted
            } else {
                Ended::Complete
            }
        );
    }
}

#[test]
fn cancellation_discards_decoder_tail_without_losing_collected_usage() {
    let host = MemoryHost::new(false);
    let outcome = outcome(&host, Box::pin(futures_util::stream::empty()));
    let cancel = outcome.stream_cancellation.unwrap();
    let ResponseBody::Stream(mut stream) = outcome.body else {
        panic!("stream");
    };
    assert_eq!(
        block_on(stream.next()).unwrap().unwrap(),
        Bytes::from_static(b"tail 1")
    );
    assert!(host.state.lock().unwrap().settlements.is_empty());
    cancel.cancel();
    assert!(block_on(stream.next()).is_none());
    let state = host.state.lock().unwrap();
    assert_eq!(state.settlements.len(), 1);
    assert_eq!(state.settlements[0].ended, Ended::Interrupted);
    assert_eq!(state.settlements[0].usage.output_tokens, 7);
    assert!(
        state.health.is_empty(),
        "discarded tails must not restore healthy"
    );
}

#[test]
fn upstream_transport_failure_degrades_and_retains_emitted_output_and_usage() {
    for detached in [false, true] {
        let host = if detached {
            MemoryHost::with_session_spawner()
        } else {
            MemoryHost::new(false)
        };
        let outcome = outcome(
            &host,
            Box::pin(futures_util::stream::iter([
                Ok(Bytes::from_static(b"already emitted")),
                Err(TransportError::Timeout),
            ])),
        );
        let ResponseBody::Stream(mut stream) = outcome.body else {
            panic!("stream")
        };
        assert_eq!(block_on(stream.next()).unwrap().unwrap(), "already emitted");
        assert!(host.state.lock().unwrap().health.is_empty());
        assert!(matches!(
            block_on(stream.next()),
            Some(Err(TransportError::Timeout))
        ));
        assert!(block_on(stream.next()).is_none());
        drop(stream);
        let state = host.state.lock().unwrap();
        assert_eq!(state.health.len(), 1);
        assert_eq!(state.health[0].2, crate::CredentialHealth::Degraded);
        assert_eq!(state.settlements.len(), 1);
        assert_eq!(state.settlements[0].ended, Ended::Interrupted);
        assert_eq!(state.settlements[0].usage.output_tokens, 7);
    }
}

#[test]
fn dropping_a_native_stream_preserves_previous_degraded_health() {
    let host = MemoryHost::with_session_spawner();
    let prior = (
        target().credential,
        "upstream-model".into(),
        crate::CredentialHealth::Degraded,
    );
    host.state.lock().unwrap().health.push(prior.clone());
    drop(outcome(&host, Box::pin(futures_util::stream::pending())));
    let state = host.state.lock().unwrap();
    assert_eq!(state.health, [prior]);
    assert_eq!(state.settlements[0].ended, Ended::Interrupted);
}

#[test]
fn codex_missing_terminal_and_malformed_events_degrade_but_valid_completion_recovers() {
    use gproxy_channel_api::{Channel, StreamCtx};
    use gproxy_protocol::{ContentGenerationKind, Operation, OperationKey};
    for (body, complete) in [
        (
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"object\":\"response\",\"created_at\":1,\"status\":\"in_progress\",\"output\":[]}}\n\n",
            false,
        ),
        ("data: invalid-json\n\n", false),
        (
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"object\":\"response\",\"created_at\":1,\"status\":\"completed\",\"output\":[]}}\n\n",
            true,
        ),
    ] {
        let host = MemoryHost::new(false);
        let key = OperationKey::content(
            Operation::StreamGenerateContent,
            ContentGenerationKind::OpenAiResponses,
        );
        let mut ctx = context();
        ctx.key = Some(key);
        let decoder = gproxy_channels::CodexChannel
            .stream_decoder(StreamCtx {
                key,
                framing: StreamFraming::Sse,
                request_body: &Bytes::new(),
                response_headers: &HeaderMap::new(),
            })
            .unwrap();
        let stream: ByteStream = Box::pin(futures_util::stream::iter([Ok(Bytes::from_static(
            body.as_bytes(),
        ))]));
        let result = block_on(funnel::streaming(
            Arc::new(host.clone()),
            ctx,
            http::Response::new(stream),
            Disposition::Success,
            Some(decoder),
        ));
        let ResponseBody::Stream(mut stream) = result.body else {
            panic!("stream")
        };
        let mut errors = 0;
        block_on(async {
            while let Some(chunk) = stream.next().await {
                errors += usize::from(chunk.is_err());
            }
        });
        assert_eq!(errors, usize::from(!complete));
        let state = host.state.lock().unwrap();
        assert_eq!(state.health.len(), 1);
        assert_eq!(
            state.health[0].2,
            if complete {
                crate::CredentialHealth::Healthy
            } else {
                crate::CredentialHealth::Degraded
            }
        );
        assert_eq!(
            state.settlements[0].ended,
            if complete {
                Ended::Complete
            } else {
                Ended::Interrupted
            }
        );
    }
}

#[test]
fn detached_native_streams_keep_the_existing_drop_settlement_path() {
    let host = MemoryHost::with_session_spawner();
    let outcome = outcome(&host, Box::pin(futures_util::stream::pending()));
    assert!(outcome.stream_cancellation.is_none());
    drop(outcome);
    let state = host.state.lock().unwrap();
    assert_eq!(state.settlements.len(), 1);
    assert_eq!(state.settlements[0].ended, Ended::Interrupted);
}

#[test]
fn free_streams_can_be_interrupted_and_finish_admission_without_usage_rows() {
    let host = MemoryHost::new(false);
    let mut ctx = context();
    ctx.settle = SettleMode::Free;
    let outcome = block_on(funnel::free_streaming(
        Arc::new(host.clone()),
        ctx,
        StatusCode::OK,
        HeaderMap::new(),
        Box::pin(futures_util::stream::pending()),
        Disposition::Success,
    ));
    let cancel = outcome.stream_cancellation.unwrap();
    let ResponseBody::Stream(mut stream) = outcome.body else {
        panic!("stream");
    };
    cancel.cancel();
    assert!(matches!(
        stream
            .as_mut()
            .poll_next(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Some(Err(_)))
    ));
    assert!(block_on(stream.next()).is_none());
    let state = host.state.lock().unwrap();
    assert!(state.settlements.is_empty());
    assert_eq!(state.admission_finishes, [true]);
}

#[test]
fn nested_inline_stream_cancellation_finishes_child_and_parent_once() {
    for completed_child in [false, true] {
        let host = MemoryHost::new(false);
        let upstream: ByteStream = if completed_child {
            Box::pin(futures_util::stream::empty())
        } else {
            Box::pin(futures_util::stream::pending())
        };
        let child = outcome(&host, upstream);
        let ResponseBody::Stream(child_body) = child.body else {
            panic!("child stream");
        };
        let (mut child_body, completion) =
            InlineCompletion::wrap(child_body, child.stream_cancellation.unwrap());
        if completed_child {
            block_on(async { while child_body.next().await.is_some() {} });
        }
        let mut ctx = context();
        ctx.request_id = "parent".into();
        ctx.settle = SettleMode::Free;
        let parent = block_on(funnel::free_streaming_with_completions(
            Arc::new(host.clone()),
            ctx,
            StatusCode::OK,
            HeaderMap::new(),
            child_body,
            Disposition::Success,
            vec![completion],
        ));
        let cancel = parent.stream_cancellation.unwrap();
        let ResponseBody::Stream(mut body) = parent.body else {
            panic!("parent stream");
        };
        if !completed_child {
            assert!(
                body.as_mut()
                    .poll_next(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
        }
        cancel.cancel();
        assert!(block_on(body.next()).unwrap().is_err());
        assert!(block_on(body.next()).is_none());
        let state = host.state.lock().unwrap();
        assert_eq!(state.admission_finishes, [true, true]);
        assert_eq!(state.settlements.len(), 1);
        assert_eq!(
            state.settlements[0].ended,
            if completed_child {
                Ended::Complete
            } else {
                Ended::Interrupted
            }
        );
        assert_eq!(state.settlements[0].usage.output_tokens, 7);
    }
}

#[test]
fn cancellation_finishes_all_discarded_children_without_polling_arbitrary_parent_stream() {
    let host = MemoryHost::new(false);
    let mut completions = Vec::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let polls = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        let child = outcome(&host, Box::pin(futures_util::stream::pending()));
        let ResponseBody::Stream(body) = child.body else {
            panic!("child stream");
        };
        let (body, completion) = InlineCompletion::wrap(body, child.stream_cancellation.unwrap());
        drop(body);
        completions.push(completion);
    }
    let mut ctx = context();
    ctx.settle = SettleMode::Free;
    let parent = block_on(funnel::free_streaming_with_completions(
        Arc::new(host.clone()),
        ctx,
        StatusCode::OK,
        HeaderMap::new(),
        Box::pin(Upstream {
            chunks: VecDeque::new(),
            pending: true,
            polls: polls.clone(),
            dropped: dropped.clone(),
        }),
        Disposition::Success,
        completions,
    ));
    parent.stream_cancellation.unwrap().cancel();
    let ResponseBody::Stream(mut body) = parent.body else {
        panic!("parent stream");
    };
    block_on(async { while body.next().await.is_some() {} });
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    let state = host.state.lock().unwrap();
    assert_eq!(state.settlements.len(), 2);
    assert_eq!(state.admission_finishes, [true, true, true]);
    assert!(
        state
            .settlements
            .iter()
            .all(|settlement| settlement.ended == Ended::Interrupted)
    );
}

#[test]
fn buffered_or_failed_synthesis_finishes_its_discarded_inline_children() {
    let host = MemoryHost::new(false);
    let child = outcome(&host, Box::pin(futures_util::stream::pending()));
    let ResponseBody::Stream(body) = child.body else {
        panic!("child stream");
    };
    let (body, completion) = InlineCompletion::wrap(body, child.stream_cancellation.unwrap());
    drop(body);
    block_on(InlineCompletion::finish_all(vec![completion]));
    let state = host.state.lock().unwrap();
    assert_eq!(state.settlements.len(), 1);
    assert_eq!(state.settlements[0].ended, Ended::Interrupted);
    assert_eq!(state.admission_finishes, [true]);
}

#[test]
fn affinity_commit_failure_cancels_nested_streams_before_returning_the_error() {
    let host = MemoryHost::new(false);
    {
        let mut state = host.state.lock().unwrap();
        state.credential.secret =
            serde_json::json!({"access_token":"fresh", "expires_at":i64::MAX});
        state.plan = Some(super::surface_harness::plan(vec![target()]));
        state.fail_cache_set = true;
        state
            .scripted
            .push_back((StatusCode::OK, vec![Bytes::from_static(b"unused body")]));
        state.scripted_pending_at = Some(0);
    }
    let core = super::core(&host).unwrap();
    let result = super::surface_harness::outcome(
        &core,
        &host,
        http::Method::GET,
        "/surface/invoke-pinned",
        Some(("x-session", "pin-failure")),
        None,
        false,
    );
    assert!(result.unwrap_err().to_string().contains("cache set failed"));
    let state = host.state.lock().unwrap();
    assert_eq!(state.admission_finishes, [true, true]);
    assert_eq!(state.captures.len(), 2);
    assert!(state.settlements.is_empty());
}

#[test]
fn slow_child_settlement_does_not_delay_interrupting_other_children() {
    let interrupted = Arc::new(AtomicUsize::new(0));
    let mut completions = Vec::new();
    for _ in 0..2 {
        let (cancellation, registration) = crate::StreamCancellation::pair();
        let count = interrupted.clone();
        let body = futures_util::stream::once(async move {
            let _ =
                futures_util::future::Abortable::new(std::future::pending::<()>(), registration)
                    .await;
            count.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
            Ok(Bytes::new())
        });
        let (body, completion) = InlineCompletion::wrap(Box::pin(body), cancellation);
        drop(body);
        completion.cancel();
        completions.push(completion);
    }
    assert!(
        InlineCompletion::poll_all(&mut completions, &mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(interrupted.load(Ordering::SeqCst), 2);
}
