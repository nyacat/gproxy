use std::time::Duration;

use bytes::Bytes;
use futures_util::{
    StreamExt,
    future::{Either, select},
};
use gproxy_channel_api::{
    Disposition, StreamCtx, TransportError,
    channel::{StreamStart, StreamStartState},
};

use crate::host::{CaptureSink, Host, StreamStartBudget, stream_start::PrefixBuffer};
use crate::{CoreError, api::Core, boundary::ByteStream, funnel};

use super::{AttemptBody, Completed, Failure};

const PREFIX_TIMEOUT: Duration = Duration::from_secs(30);
const CLASSIFIER_BYTES: usize = 128 * 1024;
const CAPTURE_BYTES: usize = 64 * 1024;

/// HTTP 200 only opens a stream. Keep ownership until the channel proves
/// generation started or reports a failure. Resource exhaustion is a local
/// 503, never permission to skip inspection or retry another credential.
pub(crate) async fn inspect<H: Host>(
    core: &Core<H>,
    completed: Completed,
) -> Result<Completed, Box<Failure>> {
    if completed.disposition != Disposition::Success || completed.facts.health_delegated {
        return Ok(completed);
    }
    let AttemptBody::Streaming(ref response, _) = completed.body else {
        return Ok(completed);
    };
    let channel = core
        .channels
        .get(completed.channel)
        .expect("completed channel");
    let Some(mut probe) = channel.stream_start(StreamCtx {
        key: completed.facts.key.expect("upstream operation"),
        framing: completed.facts.target_framing,
        request_body: &completed.facts.request_body,
        response_headers: response.headers(),
    }) else {
        return Ok(completed);
    };
    let budget = core.host.stream_start_budget();
    let _metadata = budget.reserve(CLASSIFIER_BYTES).map_err(|error| {
        tracing::info!(
            request_id = %completed.facts.request_id,
            credential_id = completed.facts.target.credential.0,
            model = %completed.facts.target.upstream_model,
            prefix_bytes = 0, prefix_chunks = 0,
            budget_used = budget.in_use(), budget_limit = budget.limit(),
            reason = "memory_budget", "stream.start_failed"
        );
        Box::new(Failure::Local { error })
    })?;
    let AttemptBody::Streaming(response, decoder) = completed.body else {
        unreachable!("streaming attempt");
    };
    let (parts, mut upstream) = response.into_parts();
    let mut prefix = PrefixBuffer::new(&budget);
    let mut chunks = 0u64;
    let started = web_time::Instant::now();
    let reading = read_prefix(
        &mut upstream,
        probe.as_mut(),
        &mut prefix,
        &budget,
        &mut chunks,
    );
    let result = match select(Box::pin(reading), core.host.wait(PREFIX_TIMEOUT)).await {
        Either::Left((result, _)) => result,
        Either::Right(_) => Err(CoreError::Transport(TransportError::Timeout)),
    };
    // The classifier never accompanies the returned stream.
    drop(probe);
    let state = match result {
        Ok(state) => state,
        Err(error) => {
            tracing::info!(
                request_id = %completed.facts.request_id,
                credential_id = completed.facts.target.credential.0,
                model = %completed.facts.target.upstream_model,
                prefix_bytes = prefix.as_slice().len(), prefix_chunks = chunks,
                budget_used = budget.in_use(), budget_limit = budget.limit(),
                reason = match &error {
                    CoreError::Transport(_) => "upstream_transport",
                    CoreError::Channel(_) => "decode_error",
                    CoreError::StreamStartOverloaded => "memory_budget",
                    _ => "local_error",
                },
                "stream.start_failed"
            );
            drop(upstream);
            if let CoreError::Transport(error) = error {
                let body = capture_prefix(core, prefix, &completed.facts.request_id);
                funnel::health::degraded(
                    core.host.as_ref(),
                    &completed.facts.target,
                    completed.facts.credential_version,
                    Some(parts.status),
                    "upstream response interrupted before generation",
                )
                .await;
                return Err(Box::new(Failure::Interrupted {
                    channel: completed.channel,
                    facts: completed.facts,
                    status: parts.status,
                    headers: parts.headers,
                    body,
                    error,
                }));
            }
            drop(prefix);
            if matches!(error, CoreError::Channel(_)) {
                funnel::health::degraded(
                    core.host.as_ref(),
                    &completed.facts.target,
                    completed.facts.credential_version,
                    Some(parts.status),
                    "upstream stream start could not be decoded",
                )
                .await;
            }
            return Err(Box::new(Failure::Local { error }));
        }
    };
    let reason = match &state {
        StreamStartState::Failed(_) => "terminal_event",
        StreamStartState::Ready => "output_or_activity",
        StreamStartState::Pending => "incomplete",
    };
    let failure = match state {
        StreamStartState::Failed(failure) if failure.disposition.should_failover() => Some(failure),
        _ => None,
    };
    tracing::info!(
        request_id = %completed.facts.request_id,
        provider_id = completed.facts.target.provider.id,
        credential_id = completed.facts.target.credential.0,
        channel = completed.channel, model = %completed.facts.target.upstream_model,
        prefix_bytes = prefix.as_slice().len(), prefix_chunks = chunks,
        prefix_elapsed_ms = started.elapsed().as_millis() as u64,
        reason,
        failover = failure.is_some(), "stream.start"
    );
    if let Some(failure) = failure {
        drop(upstream);
        funnel::diagnostic::log(&completed.facts, parts.status, &failure, true, false);
        funnel::health::record_failure(
            core.host.as_ref(),
            &completed.facts,
            parts.status,
            &failure,
        )
        .await;
        let body = capture_prefix(core, prefix, &completed.facts.request_id);
        return Ok(Completed {
            disposition: failure.disposition,
            body: AttemptBody::Buffered(funnel::BufferedRelay::native(http::Response::from_parts(
                parts, body,
            ))),
            ..completed
        });
    }
    // Replay the original bytes through the real decoder/transform once. The
    // allocation stays charged until every replay/capture clone is released.
    let replay = futures_util::stream::unfold(prefix.into_bytes(), |mut bytes| async move {
        if bytes.is_empty() {
            return None;
        }
        let chunk = bytes.split_to(bytes.len().min(64 * 1024));
        Some((Ok(chunk), bytes))
    });
    let upstream = Box::pin(replay.chain(upstream)) as ByteStream;
    Ok(Completed {
        body: AttemptBody::Streaming(http::Response::from_parts(parts, upstream), decoder),
        ..completed
    })
}

fn capture_prefix<H: Host>(core: &Core<H>, prefix: PrefixBuffer, request_id: &str) -> Bytes {
    if !core.host.capture().captures_response_body() {
        return Bytes::new();
    }
    if prefix.as_slice().len() > CAPTURE_BYTES {
        // Do not retain a large allocation through an asynchronous log sink or
        // present a truncated SSE/JSON body as a complete capture. Structured
        // failure diagnostics are recorded separately regardless of this flag.
        tracing::info!(
            request_id,
            prefix_bytes = prefix.as_slice().len(),
            reason = "size_limit",
            "stream.start_capture_skipped"
        );
        return Bytes::new();
    }
    prefix.into_bytes()
}

async fn read_prefix(
    upstream: &mut ByteStream,
    probe: &mut dyn StreamStart,
    prefix: &mut PrefixBuffer,
    budget: &std::sync::Arc<StreamStartBudget>,
    chunks: &mut u64,
) -> Result<StreamStartState, CoreError> {
    loop {
        let eof = match upstream.next().await {
            Some(Ok(chunk)) => {
                *chunks = chunks.saturating_add(1);
                // Charge the transport fragment while copying it into owned
                // prefix storage, as well as both allocations during growth.
                let incoming = budget.reserve(chunk.len())?;
                let copied = prefix.append(&chunk);
                drop(chunk);
                drop(incoming);
                copied?;
                false
            }
            Some(Err(error)) => return Err(CoreError::Transport(error)),
            None => {
                // Not every transport tolerates a second poll after EOF.
                *upstream = Box::pin(futures_util::stream::empty());
                true
            }
        };
        let scratch = budget.reserve(probe.scratch_bytes(prefix.as_slice()))?;
        let state = probe
            .inspect(prefix.as_slice(), eof)
            .map_err(CoreError::Channel)?;
        drop(scratch);
        if !matches!(state, StreamStartState::Pending) {
            return Ok(state);
        }
        if eof {
            return Err(CoreError::Transport(TransportError::Interrupted(
                "upstream stream ended before generation".into(),
            )));
        }
        // Always-ready tiny/empty fragments must not starve cancellation or the
        // deadline. A fragment count never changes the semantic commit point.
        if chunks.is_multiple_of(1024) {
            let mut yielded = false;
            std::future::poll_fn(|cx| {
                if yielded {
                    std::task::Poll::Ready(())
                } else {
                    yielded = true;
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                }
            })
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use gproxy_channel_api::Channel;
    use gproxy_protocol::{ContentGenerationKind, Operation, OperationKey, StreamFraming};

    #[test]
    fn eof_is_not_polled_twice_and_failed_inspection_releases_the_prefix() {
        let wire = Bytes::from_static(b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"status\":\"in_progress\",\"output\":[]}}\n\n");
        let mut chunk = Some(wire);
        let mut ended = false;
        let mut upstream = Box::pin(futures_util::stream::poll_fn(move |_| {
            assert!(!ended, "upstream was polled after EOF");
            let next = chunk.take().map(Ok);
            ended = next.is_none();
            std::task::Poll::Ready(next)
        })) as ByteStream;
        let mut probe = gproxy_channels::CodexChannel
            .stream_start(StreamCtx {
                key: OperationKey::content(
                    Operation::StreamGenerateContent,
                    ContentGenerationKind::OpenAiResponses,
                ),
                framing: StreamFraming::Sse,
                request_body: &Bytes::new(),
                response_headers: &http::HeaderMap::new(),
            })
            .unwrap();
        let budget = StreamStartBudget::new(8192);
        let mut prefix = PrefixBuffer::new(&budget);
        assert!(matches!(
            read_prefix(&mut upstream, probe.as_mut(), &mut prefix, &budget, &mut 0)
                .now_or_never()
                .unwrap(),
            Err(CoreError::Transport(TransportError::Interrupted(_)))
        ));
        assert!(upstream.next().now_or_never().unwrap().is_none());
        drop(prefix);
        assert_eq!(budget.in_use(), 0);
    }
}
