use bytes::Bytes;
use rust_decimal::Decimal;

use super::RealtimeMeter;
use crate::{SessionObservation, SessionUsage, SessionUsageKind, WsFrame};

fn meter() -> RealtimeMeter {
    RealtimeMeter::new(
        br#"{"sdp":"v=0","session":{"type":"realtime","model":"route","audio":{"input":{"transcription":{"model":"client-transcribe"}}}}}"#,
        "gpt-realtime",
    )
}

fn text(value: &str) -> WsFrame {
    WsFrame::Text(value.into())
}

fn usage(observation: SessionObservation) -> SessionUsage {
    let SessionObservation::Usage(usage) = observation else {
        panic!("expected metered usage");
    };
    usage
}

#[test]
fn response_done_preserves_every_token_detail() {
    let mut meter = meter();
    assert!(matches!(
        meter.observe(&text(
            r#"{"type":"session.created","session":{"type":"realtime","model":"gpt-realtime-actual","audio":{"input":{"transcription":{"model":"server-transcribe"}}}}}"#,
        )),
        SessionObservation::None
    ));
    assert!(meter.ready());
    let done = r#"{"type":"response.done","response":{"id":"resp_1","usage":{"total_tokens":18,"input_tokens":11,"output_tokens":7,"input_token_details":{"text_tokens":3,"audio_tokens":5,"image_tokens":2,"cached_tokens":1,"cached_tokens_details":{"audio_tokens":1}},"output_token_details":{"text_tokens":4,"audio_tokens":3}}}}"#;
    let sample = usage(meter.observe(&text(done)));
    assert_eq!(sample.kind, SessionUsageKind::Primary);
    assert_eq!(sample.model, "gpt-realtime-actual");
    assert_eq!(sample.usage.input_tokens, 11);
    assert_eq!(sample.usage.output_tokens, 7);
    assert_eq!(sample.usage.cached_input_tokens, 1);
    assert_eq!(sample.usage.metrics["cached_input_tokens"], Decimal::ONE);
    assert_eq!(sample.usage.metrics["text_input_tokens"], Decimal::from(3));
    assert_eq!(sample.usage.metrics["audio_input_tokens"], Decimal::from(5));
    assert_eq!(sample.usage.metrics["image_input_tokens"], Decimal::from(2));
    assert_eq!(
        sample.usage.metrics["cached_audio_input_tokens"],
        Decimal::ONE
    );
    assert_eq!(sample.usage.metrics["text_output_tokens"], Decimal::from(4));
    assert_eq!(
        sample.usage.metrics["audio_output_tokens"],
        Decimal::from(3)
    );
    assert!(matches!(
        meter.observe(&text(done)),
        SessionObservation::None
    ));
}

#[test]
fn transcription_uses_its_server_model_and_own_usage_shape() {
    let mut meter = meter();
    assert!(matches!(
        meter.observe(&text(
            r#"{"type":"session.updated","session":{"type":"realtime","model":"gpt-realtime","audio":{"input":{"transcription":{"model":"gpt-4o-transcribe"}}}}}"#,
        )),
        SessionObservation::None
    ));
    let tokens = usage(meter.observe(&text(
            r#"{"type":"conversation.item.input_audio_transcription.completed","item_id":"i1","transcript":"hi","usage":{"type":"tokens","input_tokens":8,"output_tokens":2,"total_tokens":10,"input_token_details":{"audio_tokens":6,"text_tokens":2}}}"#,
        )));
    assert_eq!(tokens.kind, SessionUsageKind::Transcription);
    assert_eq!(tokens.model, "gpt-4o-transcribe");
    assert_eq!(tokens.usage.metrics["audio_input_tokens"], Decimal::from(6));

    let duration = usage(meter.observe(&text(
            r#"{"type":"conversation.item.input_audio_transcription.completed","item_id":"i2","transcript":"bye","usage":{"type":"duration","seconds":1.25}}"#,
        )));
    assert_eq!(
        duration.usage.metrics["audio_seconds"],
        Decimal::new(125, 2)
    );
}

#[test]
fn only_valid_server_usage_events_are_metered() {
    let mut meter = meter();
    for ignored in [
        r#"{"type":"response.hologram.delta","usage":{"input_tokens":999}}"#,
        r#"{"type":"session.update","usage":{"input_tokens":999}}"#,
    ] {
        assert!(matches!(
            meter.observe(&text(ignored)),
            SessionObservation::None
        ));
    }
    for compromised in [
        r#"{"type":"response.done","response":{"output":[]}}"#,
        r#"{"type":"response.done","future":true}"#,
        r#"{"type":"session.updated","future":true}"#,
    ] {
        assert!(matches!(
            meter.observe(&text(compromised)),
            SessionObservation::Compromised { .. }
        ));
    }
    assert!(matches!(
        meter.observe(&text(
            r#"{"type":"session.updated","session":{"type":"realtime","audio":{"input":{}}}}"#,
        )),
        SessionObservation::None
    ));
    assert!(matches!(
        meter.observe(&text(
                r#"{"type":"conversation.item.input_audio_transcription.completed","item_id":"i","transcript":"x","usage":{"type":"tokens","input_tokens":1,"output_tokens":0,"total_tokens":1}}"#,
            )),
        SessionObservation::Compromised { .. }
    ));
    assert!(matches!(
        meter.observe(&WsFrame::Binary(Bytes::from_static(b"x"))),
        SessionObservation::Compromised { .. }
    ));
}

#[test]
fn upstream_failures_are_deduplicated_without_poisoning_later_responses() {
    let mut meter = meter();
    let failure = text(
        r#"{"type":"error","event_id":"evt-1","error":{"code":"invalid_request_error","message":"bad"}}"#,
    );
    assert!(matches!(meter.observe(&failure), SessionObservation::None));
    assert_eq!(
        meter.take_failure().unwrap().disposition,
        crate::Disposition::Terminal
    );
    assert!(matches!(meter.observe(&failure), SessionObservation::None));
    assert!(meter.take_failure().is_none());
    assert!(matches!(meter.observe(&text(r#"{"type":"response.done","response":{"id":"failed-1","status":"failed","status_details":{"error":{"code":"server_error","message":"busy"}}}}"#)), SessionObservation::None));
    assert_eq!(
        meter.take_failure().unwrap().disposition,
        crate::Disposition::Retryable
    );
    let sample = usage(meter.observe(&text(r#"{"type":"response.done","response":{"id":"failed-2","status":"failed","status_details":{"error":{"code":"server_error","message":"busy"}},"usage":{"total_tokens":5,"input_tokens":3,"output_tokens":2}}}"#)));
    assert_eq!(sample.usage.output_tokens, 2);
    assert!(meter.take_failure().is_some());
    let sample = usage(meter.observe(&text(r#"{"type":"response.done","response":{"id":"success","status":"completed","usage":{"total_tokens":5,"input_tokens":3,"output_tokens":2}}}"#)));
    assert_eq!(sample.usage.input_tokens, 3);
    assert!(meter.take_failure().is_none());
}

#[test]
fn success_requires_completed_status_and_is_scoped_to_the_server_model() {
    let mut meter = meter();
    meter.observe(&text(
        r#"{"type":"session.created","session":{"type":"realtime","model":"session-model","audio":{"input":{"transcription":{"model":"transcription-model"}}}}}"#,
    ));
    assert!(meter.take_successful_model().is_none());
    for (index, status) in [
        "failed",
        "incomplete",
        "cancelled",
        "in_progress",
        "future-status",
    ]
    .into_iter()
    .enumerate()
    {
        let frame = text(&serde_json::json!({
            "type":"response.done",
            "response":{"id":format!("response-{index}"),"status":status,"model":"response-model",
                "usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}
        }).to_string());
        assert!(matches!(
            meter.observe(&frame),
            SessionObservation::Usage(_)
        ));
        assert!(meter.take_successful_model().is_none(), "{status}");
        assert_eq!(meter.observation_model(), "response-model");
    }
    let completed = text(
        r#"{"type":"response.done","response":{"id":"success","status":"completed","model":"response-model","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}"#,
    );
    assert!(matches!(
        meter.observe(&completed),
        SessionObservation::Usage(_)
    ));
    assert_eq!(
        meter.take_successful_model().as_deref(),
        Some("response-model")
    );
    assert!(meter.take_successful_model().is_none());
    meter.observe(&text(
        r#"{"type":"error","error":{"code":"server_is_overloaded"}}"#,
    ));
    assert_eq!(meter.take_failure().unwrap().category, "upstream");
    assert!(matches!(
        meter.observe(&completed),
        SessionObservation::None
    ));
    assert!(
        meter.take_successful_model().is_none(),
        "replayed completion must not recover a later failure"
    );
    meter.observe(&text(r#"{"type":"response.done","response":{"id":"next-success","status":"completed","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}"#));
    assert_eq!(
        meter.take_successful_model().as_deref(),
        Some("session-model")
    );
    meter.observe(&text(r#"{"type":"conversation.item.input_audio_transcription.completed","item_id":"transcription-1","transcript":"ok","usage":{"type":"tokens","input_tokens":1,"output_tokens":0,"total_tokens":1}}"#));
    assert_eq!(
        meter.take_successful_model().as_deref(),
        Some("transcription-model")
    );
}

#[test]
fn invalid_or_untrusted_terminal_events_never_report_success() {
    for ready in [false, true] {
        let mut meter = meter();
        if ready {
            meter.observe(&text(r#"{"type":"session.created","session":{"type":"realtime","model":"session-model"}}"#));
        }
        for frame in [
            r#"{"type":"response.done","response":{"id":"missing-usage","status":"completed"}}"#,
            r#"{"type":"response.done","response":{"id":"bad-usage","status":"completed","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":1}}}"#,
            r#"{"type":"response.done","response":{"id":"missing-status","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}"#,
            r#"{"type":"response.done","response":{"id":"contradictory-status","status":"completed","status_details":{"error":{"code":"server_is_overloaded"}},"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}"#,
        ] {
            meter.observe(&text(frame));
            assert!(
                meter.take_successful_model().is_none(),
                "ready={ready}, {frame}"
            );
        }
    }
}
