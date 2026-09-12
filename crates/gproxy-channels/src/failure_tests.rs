use bytes::Bytes;
use gproxy_channel_api::{Channel, Disposition, ResponseView, StreamCtx, StreamEnd};
use gproxy_protocol::{ContentGenerationKind as Kind, Operation, OperationKey, StreamFraming};
use serde_json::json;

#[test]
fn http_failure_metadata_refines_provider_status_policy() {
    let headers = http::HeaderMap::new();
    let channels: [(Box<dyn Channel>, Disposition); 4] = [
        (Box::new(crate::ClaudeApiChannel), Disposition::Terminal),
        (Box::new(crate::AwsBedrockChannel), Disposition::Terminal),
        (
            Box::new(crate::ClaudeCodeChannel),
            Disposition::CredentialDead,
        ),
        (Box::new(crate::OpenAiChannel), Disposition::CredentialDead),
    ];
    for (channel, forbidden) in channels {
        for body in [b"{}".as_slice(), br#"{"error":{"code":"future_error"}}"#] {
            assert_eq!(
                channel.classify(ResponseView {
                    status: http::StatusCode::FORBIDDEN,
                    headers: &headers,
                    body,
                }),
                forbidden,
                "{} preserves its 403 fallback",
                channel.descriptor().id
            );
            assert_eq!(
                channel
                    .response_failure(ResponseView {
                        status: http::StatusCode::FORBIDDEN,
                        headers: &headers,
                        body,
                    })
                    .map(|failure| failure.disposition),
                Some(forbidden),
                "{} uses the same policy for credential health and failover",
                channel.descriptor().id
            );
        }
        for (code, disposition) in [
            ("invalid_api_key", Disposition::CredentialDead),
            ("rate_limit_exceeded", Disposition::Retryable),
            ("permission_denied", Disposition::Terminal),
            ("misalignment_policy_violation", Disposition::Terminal),
        ] {
            let body = serde_json::to_vec(&json!({"error": {"code": code}})).unwrap();
            assert_eq!(
                channel.classify(ResponseView {
                    status: http::StatusCode::FORBIDDEN,
                    headers: &headers,
                    body: &body,
                }),
                disposition,
                "{} recognizes {code}",
                channel.descriptor().id
            );
        }
        assert_eq!(
            channel.classify(ResponseView {
                status: http::StatusCode::OK,
                headers: &headers,
                body: br#"{"error":{"code":"future_error"}}"#,
            }),
            Disposition::Retryable,
            "{} does not accept a 200 error envelope",
            channel.descriptor().id
        );
    }
}

#[test]
fn protocol_adapters_preserve_errors_across_fragmentation_and_cancellation() {
    let channels: Vec<(Box<dyn Channel>, Kind, bool)> = vec![
        (Box::new(crate::OpenAiChannel), Kind::OpenAiResponses, false),
        (Box::new(crate::CodexChannel), Kind::OpenAiResponses, false),
        (Box::new(crate::OpenAiChannel), Kind::OpenAiChat, false),
        (Box::new(crate::KimiChannel), Kind::OpenAiChat, false),
        (Box::new(crate::XaiChannel), Kind::OpenAiChat, false),
        (Box::new(crate::OpenRouterChannel), Kind::OpenAiChat, false),
        (Box::new(crate::DeepSeekChannel), Kind::OpenAiChat, false),
        (
            Box::new(crate::ClaudeApiChannel),
            Kind::ClaudeMessages,
            false,
        ),
        (
            Box::new(crate::ClaudeCodeChannel),
            Kind::ClaudeMessages,
            false,
        ),
        (Box::new(crate::VertexChannel), Kind::ClaudeMessages, false),
        (
            Box::new(crate::AiStudioChannel),
            Kind::GeminiGenerateContent,
            false,
        ),
        (
            Box::new(crate::VertexExpressChannel),
            Kind::GeminiGenerateContent,
            false,
        ),
        (
            Box::new(crate::GeminiCliChannel),
            Kind::GeminiGenerateContent,
            true,
        ),
        (
            Box::new(crate::AntigravityChannel),
            Kind::GeminiGenerateContent,
            true,
        ),
    ];
    let headers = http::HeaderMap::from_iter([(
        http::HeaderName::from_static("x-request-id"),
        "upstream-123".parse().unwrap(),
    )]);
    for (channel, kind, wrapped) in channels {
        for (code, expected) in [
            ("server_error", Disposition::Retryable),
            ("invalid_prompt", Disposition::Terminal),
            ("invalid_api_key", Disposition::CredentialDead),
            ("future_error", Disposition::Retryable),
        ] {
            let error =
                json!({"type":"error","error":{"code":code,"message":"Bearer private-value"}});
            let error = if wrapped {
                json!({"response": error})
            } else {
                error
            };
            let wire = format!("data: {error}\n\n");
            for chunk_size in [1, 11, wire.len()] {
                for end in [StreamEnd::Complete, StreamEnd::Interrupted] {
                    let mut decoder = channel
                        .stream_decoder(StreamCtx {
                            key: OperationKey::content(Operation::StreamGenerateContent, kind),
                            framing: StreamFraming::Sse,
                            request_body: &Bytes::from_static(b"{}"),
                            response_headers: &headers,
                        })
                        .unwrap_or_else(|| {
                            panic!("missing decoder {} {kind:?}", channel.descriptor().id)
                        });
                    for chunk in wire.as_bytes().chunks(chunk_size) {
                        decoder.push(Bytes::copy_from_slice(chunk)).unwrap();
                    }
                    decoder.finish(end).unwrap();
                    assert_eq!(
                        decoder.terminal_disposition(),
                        Some(expected),
                        "{} {kind:?}",
                        channel.descriptor().id
                    );
                    let failure = decoder.terminal_failure().unwrap();
                    assert_eq!(failure.request_id.as_deref(), Some("upstream-123"));
                    assert!(!failure.message.as_ref().unwrap().contains("private-value"));
                }
            }
        }
    }
}

#[test]
fn gemini_json_errors_are_complete_and_keep_request_identity() {
    let headers = http::HeaderMap::new();
    let mut decoder = crate::AiStudioChannel
        .stream_decoder(StreamCtx {
            key: OperationKey::content(
                Operation::StreamGenerateContent,
                Kind::GeminiGenerateContent,
            ),
            framing: StreamFraming::JsonArray,
            request_body: &Bytes::from_static(b"{}"),
            response_headers: &headers,
        })
        .unwrap();
    for byte in br#"[{"error":{"code":429,"status":"RESOURCE_EXHAUSTED","message":"busy","request_id":"req-456"}}]"# {
        decoder.push(Bytes::copy_from_slice(&[*byte])).unwrap();
    }
    decoder.finish(StreamEnd::Complete).unwrap();
    let failure = decoder.terminal_failure().unwrap();
    assert_eq!(failure.disposition, Disposition::Retryable);
    assert_eq!(failure.request_id.as_deref(), Some("req-456"));
}

#[test]
fn code_assist_keeps_canonical_prefix_and_tail_metadata_when_decoding_fails() {
    let channels: [Box<dyn Channel>; 2] = [
        Box::new(crate::GeminiCliChannel),
        Box::new(crate::AntigravityChannel),
    ];
    let event = json!({"response": {
        "candidates":[{"content":{"role":"model","parts":[{"text":"visible"}]}}],
        "usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2,"totalTokenCount":6}
    }});
    let valid = format!("data: {event}\n\n");
    let wire = format!("{valid}data: {{\n\n");
    for channel in channels {
        let decoder = || {
            channel
                .stream_decoder(StreamCtx {
                    key: OperationKey::content(
                        Operation::StreamGenerateContent,
                        Kind::GeminiGenerateContent,
                    ),
                    framing: StreamFraming::Sse,
                    request_body: &Bytes::from_static(b"{}"),
                    response_headers: &http::HeaderMap::new(),
                })
                .unwrap()
        };
        let expected = decoder()
            .push(Bytes::from(valid.clone()))
            .unwrap()
            .into_iter()
            .map(|frame| frame.0)
            .collect::<Vec<_>>();
        assert!(!expected.is_empty());
        for split in 0..=wire.len() {
            let mut decoder = decoder();
            let mut delivered = Vec::new();
            let mut failed = false;
            for chunk in [&wire.as_bytes()[..split], &wire.as_bytes()[split..]] {
                let frames = match decoder.push(Bytes::copy_from_slice(chunk)) {
                    Ok(frames) => frames,
                    Err(error) => {
                        failed = true;
                        error.frames
                    }
                };
                delivered.extend(frames.into_iter().map(|frame| frame.0));
                if failed {
                    break;
                }
            }
            assert!(failed, "{} split={split}", channel.descriptor().id);
            assert_eq!(
                delivered,
                expected,
                "{} split={split}",
                channel.descriptor().id
            );
        }
        let mut decoder = decoder();
        assert!(
            decoder
                .push(Bytes::from(format!("data: {event}")))
                .unwrap()
                .is_empty()
        );
        let error = decoder.finish(StreamEnd::Complete).unwrap_err();
        assert_eq!(
            error
                .frames
                .into_iter()
                .map(|frame| frame.0)
                .collect::<Vec<_>>(),
            expected
        );
        let usage = decoder.recover_tail().usage.unwrap();
        assert_eq!((usage.input_tokens, usage.output_tokens), (4, 2));
        assert!(decoder.recover_tail().usage.is_none());
    }
}

#[test]
fn gemini_keeps_valid_prefix_and_usage_before_later_invalid_events() {
    let event = json!({
        "candidates":[{"content":{"role":"model","parts":[{"text":"visible"}]}}],
        "usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2,"totalTokenCount":6}
    });
    for framing in [StreamFraming::Sse, StreamFraming::JsonArray] {
        for invalid in [
            "{",
            r#"{"candidates":"invalid"}"#,
            r#"{"candidates":[{"index":-1}],"usageMetadata":{"promptTokenCount":99,"candidatesTokenCount":99,"totalTokenCount":198}}"#,
        ] {
            let (prefix, wire) = match framing {
                StreamFraming::Sse => {
                    let prefix = format!("data: {event}\n\n");
                    let wire = format!("{prefix}data: {invalid}\n\n");
                    (prefix, wire)
                }
                StreamFraming::JsonArray => {
                    let prefix = format!("[{event},");
                    let wire = format!("{prefix}{invalid}]");
                    (prefix, wire)
                }
                _ => unreachable!(),
            };
            for split in 0..=wire.len() {
                let mut decoder = crate::AiStudioChannel
                    .stream_decoder(StreamCtx {
                        key: OperationKey::content(
                            Operation::StreamGenerateContent,
                            Kind::GeminiGenerateContent,
                        ),
                        framing,
                        request_body: &Bytes::from_static(b"{}"),
                        response_headers: &http::HeaderMap::new(),
                    })
                    .unwrap();
                let mut delivered = Vec::new();
                let mut failed = false;
                for chunk in [&wire.as_bytes()[..split], &wire.as_bytes()[split..]] {
                    let frames = match decoder.push(Bytes::copy_from_slice(chunk)) {
                        Ok(frames) => frames,
                        Err(error) => {
                            failed = true;
                            error.frames
                        }
                    };
                    for frame in frames {
                        delivered.extend_from_slice(&frame.0);
                    }
                    if failed {
                        break;
                    }
                }
                assert!(failed, "{framing:?} split={split} invalid={invalid}");
                assert_eq!(delivered, prefix.as_bytes(), "{framing:?} split={split}");
                let usage = decoder.recover_tail().usage.unwrap();
                assert_eq!((usage.input_tokens, usage.output_tokens), (4, 2));
                assert!(decoder.recover_tail().usage.is_none());
            }
        }
    }
}

#[test]
fn gemini_retains_a_complete_final_event_when_eof_validation_fails() {
    let event = json!({
        "candidates":[{"content":{"role":"model","parts":[{"text":"tail"}]}}],
        "usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":3,"totalTokenCount":8}
    });
    for framing in [StreamFraming::Sse, StreamFraming::JsonArray] {
        let wire = match framing {
            StreamFraming::Sse => format!("data: {event}"),
            StreamFraming::JsonArray => format!("[{event}"),
            _ => unreachable!(),
        };
        let mut decoder = crate::AiStudioChannel
            .stream_decoder(StreamCtx {
                key: OperationKey::content(
                    Operation::StreamGenerateContent,
                    Kind::GeminiGenerateContent,
                ),
                framing,
                request_body: &Bytes::from_static(b"{}"),
                response_headers: &http::HeaderMap::new(),
            })
            .unwrap();
        assert!(decoder.push(Bytes::from(wire.clone())).unwrap().is_empty());
        let error = decoder.finish(StreamEnd::Complete).unwrap_err();
        let output = error
            .frames
            .into_iter()
            .flat_map(|frame| frame.0.to_vec())
            .collect::<Vec<_>>();
        assert_eq!(output, wire.as_bytes());
        let usage = decoder.recover_tail().usage.unwrap();
        assert_eq!((usage.input_tokens, usage.output_tokens), (5, 3));
        assert!(decoder.recover_tail().usage.is_none());
    }
}
