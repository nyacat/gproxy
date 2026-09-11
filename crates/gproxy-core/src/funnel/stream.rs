use std::collections::VecDeque;
use std::future::{Future, Pending, pending};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use futures_util::future::Abortable;
use gproxy_channel_api::{BoxFuture, Disposition, StreamDecoder, StreamEnd, TransportError};
use gproxy_protocol::SettleMode;

use crate::Shared;
use crate::host::Host;
use crate::usage::Ended;

use super::{FunnelCtx, complete_stream};

pub(crate) struct FunnelStream<H: Host> {
    upstream: crate::boundary::ByteStream,
    cancellation: Option<crate::boundary::StreamCancellation>,
    interrupted: Option<Abortable<Pending<()>>>,
    inline_completions: Vec<super::inline::InlineCompletion>,
    decoder: Option<Box<dyn StreamDecoder>>,
    pending: VecDeque<Bytes>,
    host: Option<Shared<H>>,
    ctx: Option<FunnelCtx>,
    status: http::StatusCode,
    state: State,
    ended: Option<Ended>,
    tail_usage: Option<gproxy_channel_api::NormalizedUsage>,
    actual_service_tier: Option<String>,
    output_chars: u64,
    estimated_output_chars: Option<u64>,
    terminal_error: Option<TransportError>,
    terminal_disposition: Option<Disposition>,
    upstream_failure: Option<gproxy_channel_api::UpstreamFailure>,
    fallback_health: Option<(crate::CredentialHealth, &'static str)>,
    /// Backlog room reserved when the stream opened; travels with the
    /// settlement so the slot frees only once the usage row has landed.
    permit: Option<crate::host::SettlementPermit>,
}

enum State {
    Relaying,
    Draining,
    CompletingChildren,
    Settling(BoxFuture<'static, ()>),
    Done,
}

#[derive(Clone, Copy)]
enum Interruption {
    Transport,
    DecoderPush,
    DecoderFinish,
    ClientCancelled,
}

impl Interruption {
    fn reason(self) -> &'static str {
        match self {
            Self::Transport => "upstream_transport",
            Self::DecoderPush => "decoder_push",
            Self::DecoderFinish => "decoder_finish",
            Self::ClientCancelled => "client_cancelled",
        }
    }
}

impl<H: Host> FunnelStream<H> {
    pub(crate) fn new(
        upstream: crate::boundary::ByteStream,
        decoder: Option<Box<dyn StreamDecoder>>,
        host: Shared<H>,
        ctx: FunnelCtx,
        status: http::StatusCode,
        permit: Option<crate::host::SettlementPermit>,
    ) -> Self {
        let cancellation = host
            .spawner()
            .is_none()
            .then(crate::boundary::StreamCancellation::pair);
        let (cancellation, interrupted) = match cancellation {
            Some((handle, registration)) => {
                (Some(handle), Some(Abortable::new(pending(), registration)))
            }
            None => (None, None),
        };
        Self {
            upstream,
            cancellation,
            interrupted,
            inline_completions: Vec::new(),
            decoder,
            pending: VecDeque::new(),
            host: Some(host),
            ctx: Some(ctx),
            status,
            state: State::Relaying,
            ended: None,
            tail_usage: None,
            actual_service_tier: None,
            output_chars: 0,
            estimated_output_chars: None,
            terminal_error: None,
            terminal_disposition: None,
            upstream_failure: None,
            fallback_health: None,
            permit,
        }
    }

    pub(crate) fn cancellation(&self) -> Option<crate::boundary::StreamCancellation> {
        self.cancellation.clone()
    }

    pub(crate) fn with_inline_completions(
        mut self,
        completions: Vec<super::inline::InlineCompletion>,
    ) -> Self {
        self.inline_completions = completions;
        self
    }

    fn poll_interruption(&mut self, cx: &mut Context<'_>) {
        if !matches!(self.state, State::Relaying | State::Draining) {
            return;
        }
        let Some(interrupted) = &mut self.interrupted else {
            return;
        };
        if Pin::new(interrupted).poll(cx).is_pending() {
            return;
        }
        self.interrupted = None;
        // Dropping a fetch body cancels its reader, so cancellation never
        // depends on another upstream chunk arriving.
        self.upstream = Box::pin(futures_util::stream::empty());
        let had_pending = !self.pending.is_empty();
        self.pending.clear();
        if matches!(self.state, State::Relaying) {
            self.abort_relay(
                TransportError::Interrupted("response stream cancelled".into()),
                Interruption::ClientCancelled,
            );
        } else if had_pending {
            self.ended = Some(Ended::Interrupted);
            if self
                .fallback_health
                .is_some_and(|(health, _)| health == crate::CredentialHealth::Healthy)
            {
                self.fallback_health = None;
            }
        }
    }

    fn finish_relay(&mut self) {
        if let Some(mut decoder) = self.decoder.take() {
            let result = decoder.finish(StreamEnd::Complete);
            // A wrapper may fail after its upstream decoded the terminal event.
            self.terminal_disposition = decoder.terminal_disposition();
            self.upstream_failure = decoder.terminal_failure().cloned();
            let tail = match result {
                Ok(tail) => tail,
                Err(error) => {
                    let tail = decoder.recover_tail();
                    self.estimated_output_chars = tail.estimated_output_chars;
                    self.tail_usage = tail.usage;
                    self.actual_service_tier = tail.actual_service_tier;
                    self.abort_decode(error, Interruption::DecoderFinish);
                    return;
                }
            };
            self.pending
                .extend(tail.frames.into_iter().map(|frame| frame.0));
            self.tail_usage = tail.usage;
            self.actual_service_tier = tail.actual_service_tier;
            self.estimated_output_chars = tail.estimated_output_chars;
        }
        self.ended = Some(Ended::Complete);
        self.fallback_health = Some((
            crate::CredentialHealth::Healthy,
            "upstream stream completed",
        ));
        self.state = State::Draining;
    }

    fn interrupt_decoder(&mut self) {
        if let Some(mut decoder) = self.decoder.take() {
            let tail = decoder
                .finish(StreamEnd::Interrupted)
                .unwrap_or_else(|_| decoder.recover_tail());
            self.terminal_disposition = decoder.terminal_disposition();
            self.upstream_failure = decoder.terminal_failure().cloned();
            self.tail_usage = tail.usage;
            self.actual_service_tier = tail.actual_service_tier;
            self.estimated_output_chars = tail.estimated_output_chars;
        }
    }

    fn abort_decode(
        &mut self,
        error: gproxy_channel_api::StreamDecodeError,
        interruption: Interruption,
    ) {
        if let (Some(ctx), Some(diagnostic)) = (&self.ctx, &error.diagnostic) {
            tracing::warn!(request_id = %ctx.request_id,
                event = ?diagnostic.event, event_type = ?diagnostic.event_type,
                payload_bytes = diagnostic.payload_bytes, fields = ?diagnostic.fields,
                field_path = %diagnostic.field_path,
                error = %error, "stream.decode.failed");
        }
        self.pending
            .extend(error.frames.into_iter().map(|frame| frame.0));
        self.abort_relay(
            TransportError::Interrupted(error.error.to_string()),
            interruption,
        );
    }

    fn abort_relay(&mut self, error: TransportError, interruption: Interruption) {
        self.interrupt_decoder();
        self.fallback_health = match interruption {
            Interruption::ClientCancelled => None,
            Interruption::Transport => Some((
                crate::CredentialHealth::Degraded,
                "upstream stream transport failed",
            )),
            Interruption::DecoderPush | Interruption::DecoderFinish => Some((
                crate::CredentialHealth::Degraded,
                "upstream stream decoding failed",
            )),
        };
        if let Some(ctx) = &self.ctx {
            tracing::info!(
                request_id = %ctx.request_id,
                provider_id = ctx.target.provider.id,
                credential_id = ctx.target.credential.0,
                model = %ctx.target.upstream_model,
                status = self.status.as_u16(),
                reason = interruption.reason(),
                error_kind = super::error::transport_error_kind(&error),
                error = %error,
                usage_received = self.tail_usage.is_some(),
                output_chars = self.output_chars,
                latency_ms = ctx.started.elapsed().as_millis() as u64,
                "stream.interrupted"
            );
        }
        self.ended = Some(Ended::Interrupted);
        self.terminal_error = Some(error);
        self.state = State::Draining;
    }

    fn begin_settle(&mut self) {
        let host = self.host.take().expect("stream host is present");
        let ctx = self.ctx.take().expect("stream funnel context is present");
        let ended = self.ended.take().expect("stream end is present");
        let usage = matches!(ctx.settle, SettleMode::OnResponse)
            .then(|| self.tail_usage.take())
            .flatten();
        let future = self.settlement(host.clone(), ctx, usage, ended);
        if let Some(spawner) = host.spawner() {
            spawner.spawn(future);
            self.state = State::Done;
        } else {
            self.state = State::Settling(future);
        }
    }

    fn settlement(
        &mut self,
        host: Shared<H>,
        ctx: FunnelCtx,
        usage: Option<gproxy_channel_api::NormalizedUsage>,
        ended: Ended,
    ) -> BoxFuture<'static, ()> {
        let status = self.status;
        let upstream_failure = self.upstream_failure.take();
        if let Some(failure) = &upstream_failure {
            super::diagnostic::log(&ctx, status, failure, true, usage.is_some());
        }
        // Once an upstream terminal result is known, a conversion error or
        // downstream cancellation must not replace its health classification.
        let health = match self.terminal_disposition {
            Some(Disposition::Success) => Some((
                crate::CredentialHealth::Healthy,
                "upstream stream completed",
            )),
            Some(Disposition::Retryable) => Some((
                crate::CredentialHealth::Degraded,
                "upstream stream reported a retryable failure",
            )),
            Some(Disposition::CredentialDead) => Some((
                crate::CredentialHealth::Dead,
                "upstream stream rejected the credential",
            )),
            Some(Disposition::Terminal) => None,
            None => self.fallback_health.take(),
        };
        let actual_service_tier = self.actual_service_tier.take();
        let terminal_disposition = self.terminal_disposition;
        let output_chars = self
            .estimated_output_chars
            .map(crate::usage::OutputEstimate::Content)
            .unwrap_or(crate::usage::OutputEstimate::Wire(self.output_chars));
        let permit = self.permit.take();
        Box::pin(async move {
            // Non-2xx responses were classified before entering the funnel.
            // Cancellation without a terminal result has no health observation
            // and must not clear a previous failure.
            if status.is_success()
                && let Some(version) = ctx.credential_version
                && let Some((health, detail)) = health
            {
                host.record_credential_health(
                    ctx.target.credential,
                    &ctx.target.upstream_model,
                    version,
                    health,
                    Some(status),
                    upstream_failure
                        .as_ref()
                        .map(|failure| failure.health_detail())
                        .as_deref()
                        .unwrap_or(detail),
                )
                .await;
            }
            complete_stream(
                host,
                ctx,
                status,
                crate::funnel::StreamDetails {
                    usage,
                    actual_service_tier,
                    estimated_output_chars: Some(output_chars),
                    terminal_disposition,
                },
                ended,
            )
            .await;
            drop(permit);
        })
    }

    fn poll_terminal(&mut self) -> Poll<Option<Result<Bytes, TransportError>>> {
        Poll::Ready(self.terminal_error.take().map(Err))
    }
}

impl<H: Host> Stream for FunnelStream<H> {
    type Item = Result<Bytes, TransportError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.poll_interruption(cx);
        loop {
            if let Some(frame) = this.pending.pop_front() {
                return Poll::Ready(Some(Ok(frame)));
            }
            match &mut this.state {
                State::Relaying => match this.upstream.as_mut().poll_next(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Ok(chunk))) => {
                        this.output_chars = this
                            .output_chars
                            .saturating_add(crate::usage::utf8_chars(&chunk));
                        let Some(decoder) = this.decoder.as_mut() else {
                            return Poll::Ready(Some(Ok(chunk)));
                        };
                        match decoder.push(chunk) {
                            Ok(frames) => {
                                this.pending.extend(frames.into_iter().map(|frame| frame.0))
                            }
                            Err(error) => this.abort_decode(error, Interruption::DecoderPush),
                        }
                    }
                    Poll::Ready(Some(Err(error))) => {
                        this.abort_relay(error, Interruption::Transport);
                    }
                    Poll::Ready(None) => this.finish_relay(),
                },
                State::Draining => {
                    if this.inline_completions.is_empty() {
                        this.begin_settle();
                    } else {
                        for completion in &this.inline_completions {
                            completion.cancel();
                        }
                        this.state = State::CompletingChildren;
                    }
                }
                State::CompletingChildren => {
                    if super::inline::InlineCompletion::poll_all(&mut this.inline_completions, cx)
                        .is_pending()
                    {
                        return Poll::Pending;
                    }
                    this.begin_settle();
                }
                State::Settling(future) => match future.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        this.state = State::Done;
                    }
                },
                State::Done => return this.poll_terminal(),
            }
        }
    }
}

impl<H: Host> Drop for FunnelStream<H> {
    fn drop(&mut self) {
        if matches!(self.state, State::Settling(_) | State::Done) {
            return;
        }
        let (Some(host), Some(ctx)) = (self.host.take(), self.ctx.take()) else {
            return;
        };
        if matches!(self.state, State::Relaying) {
            tracing::info!(
                request_id = %ctx.request_id,
                provider_id = ctx.target.provider.id,
                credential_id = ctx.target.credential.0,
                model = %ctx.target.upstream_model,
                reason = "downstream_dropped",
                output_chars = self.output_chars,
                latency_ms = ctx.started.elapsed().as_millis() as u64,
                "stream.dropped"
            );
        }
        if matches!(self.state, State::Relaying) {
            self.interrupt_decoder();
        }
        let usage = matches!(ctx.settle, SettleMode::OnResponse)
            .then(|| self.tail_usage.take())
            .flatten();
        let ended = if self.pending.is_empty() {
            self.ended.take().unwrap_or(Ended::Interrupted)
        } else {
            Ended::Interrupted
        };
        if ended == Ended::Interrupted
            && self
                .fallback_health
                .is_some_and(|(health, _)| health == crate::CredentialHealth::Healthy)
        {
            self.fallback_health = None;
        }
        if let Some(spawner) = host.spawner() {
            spawner.spawn(self.settlement(host.clone(), ctx, usage, ended));
        } else {
            tracing::warn!(
                request_id = %ctx.request_id,
                "stream dropped before inline settlement could complete"
            );
        }
    }
}
