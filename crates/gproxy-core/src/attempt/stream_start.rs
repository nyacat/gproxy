use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{
    StreamExt,
    future::{Either, select},
};
use gproxy_channel_api::{
    Disposition, StreamCtx, StreamDecoder, StreamEnd, TransportError, UpstreamFailure,
};

use crate::api::Core;
use crate::boundary::ByteStream;
use crate::funnel;
use crate::host::Host;

use super::{AttemptBody, Completed};

const MAX_PREFIX_BYTES: usize = 64 * 1024;
const MAX_PREFIX_CHUNKS: usize = 64 * 1024;
const PREFIX_TIMEOUT: Duration = Duration::from_secs(30);

/// HTTP 200 only opens a stream. An opted-in channel can still reject the
/// request before generation starts, while the failover loop owns the response.
pub(crate) async fn inspect<H: Host>(core: &Core<H>, completed: Completed) -> Completed {
    if completed.disposition != Disposition::Success || completed.facts.health_delegated {
        return completed;
    }
    let AttemptBody::Streaming(ref response, _) = completed.body else {
        return completed;
    };
    let channel = core
        .channels
        .get(completed.channel)
        .expect("completed channel");
    let Some(mut probe) = channel.stream_decoder(StreamCtx {
        key: completed.facts.key.expect("upstream operation"),
        framing: completed.facts.target_framing,
        request_body: &completed.facts.request_body,
        response_headers: response.headers(),
    }) else {
        return completed;
    };
    if !probe.replay_safe() {
        return completed;
    }
    let AttemptBody::Streaming(response, decoder) = completed.body else {
        unreachable!("streaming attempt");
    };
    let (parts, mut upstream) = response.into_parts();
    let mut prefix = Vec::new();
    let reading = read_prefix(&mut upstream, probe.as_mut(), &mut prefix);
    let timeout = core.host.wait(PREFIX_TIMEOUT);
    let failure = match select(Box::pin(reading), timeout).await {
        Either::Left((failure, _)) => failure,
        Either::Right(_) => None,
    };
    if let Some(failure) = failure {
        funnel::diagnostic::log(&completed.facts, parts.status, &failure, true, false);
        funnel::health::record_failure(
            core.host.as_ref(),
            &completed.facts,
            parts.status,
            &failure,
        )
        .await;
        let mut body = BytesMut::new();
        for chunk in prefix.into_iter().flatten() {
            body.extend_from_slice(&chunk);
        }
        return Completed {
            disposition: failure.disposition,
            body: AttemptBody::Buffered(funnel::BufferedRelay::native(http::Response::from_parts(
                parts,
                body.freeze(),
            ))),
            ..completed
        };
    }
    // The probe owns separate decoder state. Replay the original bytes through
    // the real decoder, rules, and transform exactly once, including errors.
    let upstream = Box::pin(futures_util::stream::iter(prefix).chain(upstream)) as ByteStream;
    Completed {
        body: AttemptBody::Streaming(http::Response::from_parts(parts, upstream), decoder),
        ..completed
    }
}

async fn read_prefix(
    upstream: &mut ByteStream,
    decoder: &mut dyn StreamDecoder,
    prefix: &mut Vec<Result<Bytes, TransportError>>,
) -> Option<UpstreamFailure> {
    let mut bytes = 0;
    for _ in 0..MAX_PREFIX_CHUNKS {
        let Some(chunk) = upstream.next().await else {
            // A stream need not tolerate another poll after EOF. The replay
            // contains all its bytes; only the real decoder still needs EOF.
            *upstream = Box::pin(futures_util::stream::empty());
            let _ = decoder.finish(StreamEnd::Complete);
            return failure_before_output(decoder);
        };
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                prefix.push(Err(error));
                return None;
            }
        };
        bytes += chunk.len();
        prefix.push(Ok(chunk.clone()));
        if bytes > MAX_PREFIX_BYTES {
            return None;
        }
        let decoded = decoder.push(chunk);
        if !decoder.replay_safe() {
            return None;
        }
        if decoder.terminal_failure().is_some() {
            return failure_before_output(decoder);
        }
        if decoded.is_err() {
            return None;
        }
    }
    None
}

fn failure_before_output(decoder: &dyn StreamDecoder) -> Option<UpstreamFailure> {
    decoder
        .replay_safe()
        .then(|| decoder.terminal_failure())
        .flatten()
        .filter(|failure| failure.disposition.should_failover())
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;
    use gproxy_channel_api::Channel;
    use gproxy_protocol::{ContentGenerationKind, Operation, OperationKey, StreamFraming};

    #[test]
    fn an_incomplete_prefix_does_not_poll_the_original_stream_again_after_eof() {
        let wire = Bytes::from_static(b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"object\":\"response\",\"created_at\":1,\"status\":\"in_progress\",\"output\":[]}}\n\n");
        let mut chunk = Some(wire.clone());
        let mut ended = false;
        let mut upstream = Box::pin(futures_util::stream::poll_fn(move |_| {
            assert!(!ended, "upstream was polled after EOF");
            let next = chunk.take().map(Ok);
            ended = next.is_none();
            std::task::Poll::Ready(next)
        })) as ByteStream;
        let mut decoder = gproxy_channels::CodexChannel
            .stream_decoder(StreamCtx {
                key: OperationKey::content(
                    Operation::StreamGenerateContent,
                    ContentGenerationKind::OpenAiResponses,
                ),
                framing: StreamFraming::Sse,
                request_body: &Bytes::new(),
                response_headers: &http::HeaderMap::new(),
            })
            .unwrap();
        let mut prefix = Vec::new();
        assert!(
            read_prefix(&mut upstream, decoder.as_mut(), &mut prefix)
                .now_or_never()
                .expect("ready prefix")
                .is_none()
        );
        let mut replay = futures_util::stream::iter(prefix).chain(upstream);
        assert_eq!(
            replay.next().now_or_never().unwrap().unwrap().unwrap(),
            wire
        );
        assert!(replay.next().now_or_never().unwrap().is_none());
    }
}
