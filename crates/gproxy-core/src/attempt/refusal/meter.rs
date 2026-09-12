use gproxy_channel_api::StreamDecodeError;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use gproxy_channel_api::{
    Frame, NormalizedUsage, StreamDecoder, StreamEnd, StreamTail, UsageAttempt,
};

#[derive(Clone)]
pub(super) struct Meter(Arc<Mutex<State>>);

#[derive(Default)]
struct State {
    failure: Option<gproxy_channel_api::UpstreamFailure>,
    disposition: Option<gproxy_channel_api::Disposition>,
    decoder: Option<Box<dyn StreamDecoder>>,
    attempts: Vec<UsageAttempt>,
    latest: Option<NormalizedUsage>,
    actual_service_tier: Option<String>,
    estimated_output_chars: Option<u64>,
    recovery_pending: bool,
    model: String,
    started: Option<i64>,
    input: Bytes,
    received: u64,
}

impl Meter {
    pub(super) fn new() -> Self {
        Self(Arc::new(Mutex::new(State::default())))
    }

    pub(super) fn start(
        &self,
        decoder: Option<Box<dyn StreamDecoder>>,
        model: String,
        started: i64,
        input: Bytes,
    ) {
        let mut state = self.0.lock().expect("fallback meter lock");
        state.decoder = decoder;
        state.failure = None;
        state.disposition = None;
        state.model = model;
        state.started = Some(started);
        state.input = input;
        state.received = 0;
        state.actual_service_tier = None;
        state.estimated_output_chars = None;
        state.recovery_pending = false;
    }

    pub(super) fn push(&self, chunk: Bytes) -> Result<Vec<Frame>, StreamDecodeError> {
        let mut state = self.0.lock().expect("fallback meter lock");
        state.received = state
            .received
            .saturating_add(crate::usage::utf8_chars(&chunk));
        let Some(decoder) = state.decoder.as_mut() else {
            return Ok(vec![Frame(chunk)]);
        };
        let result = decoder.push(chunk);
        let disposition = decoder.terminal_disposition();
        let failure = decoder.terminal_failure().cloned();
        state.disposition = disposition;
        state.failure = failure;
        result
    }

    pub(super) fn finish(
        &self,
        end: StreamEnd,
        refused: bool,
    ) -> Result<Vec<Frame>, StreamDecodeError> {
        let mut state = self.0.lock().expect("fallback meter lock");
        let Some(mut decoder) = state.decoder.take() else {
            return Ok(Vec::new());
        };
        let result = decoder.finish(end);
        state.failure = decoder.terminal_failure().cloned();
        state.disposition = decoder.terminal_disposition();
        let tail = match result {
            Ok(tail) => tail,
            Err(error) => {
                let tail = decoder.recover_tail();
                state.actual_service_tier = tail.actual_service_tier;
                state.estimated_output_chars = tail.estimated_output_chars;
                if let Some(usage) = tail.usage {
                    state.record(usage, refused, false);
                } else {
                    // A malformed terminal frame can leave the decoder with
                    // no provider usage at all. Still close this attempt with
                    // an estimate; otherwise the retry's admission remains
                    // reserved and its usage is silently attributed to the
                    // previous model/attempt.
                    let input_tokens = crate::usage::estimate_input_tokens(&state.input);
                    let output_tokens = state
                        .estimated_output_chars
                        .unwrap_or(state.received)
                        .div_ceil(2);
                    state.record(
                        NormalizedUsage {
                            input_tokens,
                            output_tokens,
                            ..Default::default()
                        },
                        refused,
                        true,
                    );
                }
                state.recovery_pending = true;
                return Err(error);
            }
        };
        state.actual_service_tier = tail.actual_service_tier;
        state.estimated_output_chars = tail.estimated_output_chars;
        let estimated = tail.usage.is_none();
        let usage = tail.usage.unwrap_or_else(|| NormalizedUsage {
            input_tokens: crate::usage::estimate_input_tokens(&state.input),
            output_tokens: state
                .estimated_output_chars
                .unwrap_or(state.received)
                .div_ceil(2),
            ..Default::default()
        });
        state.record(usage, refused, estimated);
        Ok(tail.frames)
    }

    pub(super) fn record(
        &self,
        usage: NormalizedUsage,
        model: String,
        started: i64,
        refused: bool,
        estimated: bool,
    ) {
        let mut state = self.0.lock().expect("fallback meter lock");
        state.model = model;
        state.started = Some(started);
        state.record(usage, refused, estimated);
    }

    pub(super) fn usage(&self) -> Option<NormalizedUsage> {
        let state = self.0.lock().expect("fallback meter lock");
        state.usage()
    }

    fn tail(&self, recovering: bool) -> StreamTail {
        let mut state = self.0.lock().expect("fallback meter lock");
        let pending = std::mem::take(&mut state.recovery_pending);
        if recovering && !pending {
            return StreamTail::default();
        }
        StreamTail {
            estimated_output_chars: state.estimated_output_chars.take(),
            usage: state.usage(),
            actual_service_tier: state.actual_service_tier.take(),
            ..Default::default()
        }
    }

    pub(super) fn len(&self) -> usize {
        self.0.lock().expect("meter").attempts.len()
    }

    pub(super) fn health(
        &self,
    ) -> (
        Option<gproxy_channel_api::Disposition>,
        Option<gproxy_channel_api::UpstreamFailure>,
    ) {
        let state = self.0.lock().expect("fallback meter lock");
        (state.disposition, state.failure.clone())
    }

    pub(super) fn reset_health(&self) {
        let mut state = self.0.lock().expect("fallback meter lock");
        state.disposition = None;
        state.failure = None;
    }

    pub(super) fn reject(&self, model: String, started: i64, status: u16) {
        let mut usage = NormalizedUsage::default();
        usage
            .dimensions
            .insert("http_status".into(), status.to_string());
        self.record(usage, model, started, true, false);
    }

    pub(super) fn decoder(&self) -> Box<dyn StreamDecoder> {
        Box::new(Observer(self.clone(), None))
    }
}

impl State {
    fn usage(&self) -> Option<NormalizedUsage> {
        self.latest.clone().map(|mut usage| {
            usage.attempts = self.attempts.clone();
            usage
        })
    }

    fn record(&mut self, mut usage: NormalizedUsage, refused: bool, estimated: bool) {
        let mut attempts = std::mem::take(&mut usage.attempts);
        if attempts.is_empty() {
            attempts.push(UsageAttempt {
                model: self.model.clone(),
                usage: Box::new(usage.clone()),
                billable: !refused || usage.output_tokens > 0,
                estimated,
                started_at_ms: self.started,
            });
        }
        for attempt in &mut attempts {
            if attempt.model.is_empty() {
                attempt.model.clone_from(&self.model);
            }
            attempt.started_at_ms = attempt.started_at_ms.or(self.started);
        }
        self.attempts.extend(attempts);
        self.latest = Some(usage);
    }
}

struct Observer(Meter, Option<gproxy_channel_api::UpstreamFailure>);

impl StreamDecoder for Observer {
    fn terminal_failure(&self) -> Option<&gproxy_channel_api::UpstreamFailure> {
        self.1.as_ref()
    }

    fn push(&mut self, chunk: Bytes) -> Result<Vec<Frame>, StreamDecodeError> {
        Ok(vec![Frame(chunk)])
    }
    fn finish(&mut self, end: StreamEnd) -> Result<StreamTail, StreamDecodeError> {
        let result = self.0.finish(end, false);
        self.1 = self.0.0.lock().expect("fallback meter").failure.clone();
        result?;
        Ok(self.0.tail(false))
    }

    fn recover_tail(&mut self) -> StreamTail {
        self.0.tail(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gproxy_channel_api::{Channel, StreamCtx};
    use gproxy_protocol::{ContentGenerationKind, Operation, OperationKey, StreamFraming};

    #[test]
    fn observer_recovers_rule_tail_usage_and_attempts_once() {
        let key = OperationKey::content(
            Operation::StreamGenerateContent,
            ContentGenerationKind::OpenAiChat,
        );
        let headers = http::HeaderMap::new();
        let request = Bytes::new();
        let upstream = gproxy_channels::OpenAiChannel.stream_decoder(StreamCtx {
            key,
            framing: StreamFraming::Sse,
            request_body: &request,
            response_headers: &headers,
        });
        let rules = crate::process::ResponseRuleDecoder::new(
            upstream,
            Arc::from([]),
            key,
            StreamFraming::Sse,
            crate::process::RuleModels::new("test-model", None),
            headers,
        )
        .unwrap();
        let meter = Meter::new();
        meter.start(Some(Box::new(rules)), "test-model".into(), 1, request);
        meter.push(Bytes::from_static(b"data: {\"usage\":{\"prompt_tokens\":13,\"completion_tokens\":7,\"total_tokens\":20},\"service_tier\":\"priority\"}\n\n")).unwrap();
        meter.push(Bytes::from_static(b"data: \xe4")).unwrap();
        let mut observer = meter.decoder();
        let error = observer.finish(StreamEnd::Complete).unwrap_err();
        assert!(error.to_string().contains("not UTF-8"), "{error}");
        let tail = observer.recover_tail();
        assert!(tail.frames.is_empty());
        assert_eq!(tail.actual_service_tier.as_deref(), Some("priority"));
        let usage = tail.usage.unwrap();
        assert_eq!(usage.input_tokens, 13);
        assert_eq!(usage.output_tokens, 7);
        assert_eq!(usage.attempts.len(), 1);
        assert!(!usage.attempts[0].estimated);
        assert_eq!(usage.attempts[0].model, "test-model");
        let tail = observer.recover_tail();
        assert!(tail.usage.is_none());
        assert!(tail.actual_service_tier.is_none());
    }

    #[test]
    fn failed_tail_without_usage_still_records_the_current_attempt_estimate() {
        let key = OperationKey::content(
            Operation::StreamGenerateContent,
            ContentGenerationKind::OpenAiChat,
        );
        let headers = http::HeaderMap::new();
        let request = Bytes::from_static(b"test");
        let upstream = gproxy_channels::OpenAiChannel.stream_decoder(StreamCtx {
            key,
            framing: StreamFraming::Sse,
            request_body: &request,
            response_headers: &headers,
        });
        let rules = crate::process::ResponseRuleDecoder::new(
            upstream,
            Arc::from([]),
            key,
            StreamFraming::Sse,
            crate::process::RuleModels::new("second-model", None),
            headers,
        )
        .unwrap();
        let meter = Meter::new();
        meter.record(
            NormalizedUsage {
                input_tokens: 20,
                output_tokens: 1,
                ..Default::default()
            },
            "first-model".into(),
            1,
            true,
            false,
        );
        meter.start(Some(Box::new(rules)), "second-model".into(), 2, request);
        meter
            .push(Bytes::from_static(
                b"data: {\"choices\":[{\"delta\":{\"content\":\"hello!!!\"}}]}\n\n",
            ))
            .unwrap();
        meter.push(Bytes::from_static(b"data: \xe4")).unwrap();
        let mut observer = meter.decoder();
        assert!(observer.finish(StreamEnd::Complete).is_err());
        let usage = observer.recover_tail().usage.unwrap();
        assert_eq!(usage.attempts.len(), 2);
        let attempt = &usage.attempts[1];
        assert_eq!(attempt.model, "second-model");
        assert_eq!(attempt.started_at_ms, Some(2));
        assert!(attempt.estimated);
        assert_eq!(attempt.usage.input_tokens, 2);
        // Chat currently uses the existing wire-character estimate; only the
        // Responses decoder exposes a semantic content-character estimate.
        assert_eq!(attempt.usage.output_tokens, 31);
        assert!(observer.recover_tail().usage.is_none());
    }
}
