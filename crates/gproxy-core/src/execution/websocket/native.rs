use gproxy_channel_api::{TransportError, WsFrame};
use gproxy_protocol::{ContentGenerationKind, OperationKind};
use web_time::Instant;

use super::{ActiveResponse, Classified, Plan, RequestCtx, ResponsesBridge};
use crate::attempt::{self, AdmissionCtx, Egress};
use crate::host::{Host, UpstreamTransport};

impl<H: Host> ResponsesBridge<H> {
    pub(super) async fn connect_native(
        &mut self,
        request: &RequestCtx,
        fallback_plan: &mut Plan,
        classified: &Classified,
    ) -> Result<bool, TransportError> {
        let mut native_plan = fallback_plan.clone();
        native_plan.targets.retain(|target| {
            attempt::support(&self.core, target, classified.key)
                .ok()
                .flatten()
                .is_some_and(|support| {
                    support.target.kind()
                        == OperationKind::ContentGeneration(
                            ContentGenerationKind::OpenAiResponsesWebSocket,
                        )
                })
        });
        if native_plan.targets.is_empty() {
            return Ok(false);
        }
        let plan = match self
            .core
            .host
            .admit(
                &self.identity,
                request,
                Some(classified.key),
                classified.requested_model.as_deref(),
                &native_plan,
            )
            .await
        {
            Ok(plan) => plan,
            // HTTP fallback may still have authorized providers.
            Err(crate::error::CoreError::Forbidden(_)) => return Ok(false),
            Err(error) => return Err(super::transport(error)),
        };
        let mut attempts = 0;
        for target in &plan.targets {
            if attempts >= plan.budget.max_attempts {
                break;
            }
            let prepared = match attempt::prepare(
                &self.core,
                self.control.as_ref(),
                target,
                request,
                classified,
                AdmissionCtx {
                    admitted: true,
                    owner_user_id: Some(self.identity.user_id),
                },
                Instant::now(),
            )
            .await
            {
                Ok(prepared) => prepared,
                Err(
                    crate::CoreError::CredentialCoolingDown { .. }
                    | crate::CoreError::CredentialRefreshCoolingDown { .. }
                    | crate::CoreError::NoCredentials
                    | crate::CoreError::Unsupported
                    | crate::CoreError::Channel(
                        gproxy_channel_api::ChannelError::Secret(_)
                        | gproxy_channel_api::ChannelError::Refresh(_)
                        | gproxy_channel_api::ChannelError::Prepare(_),
                    ),
                ) => continue,
                Err(error) => {
                    self.core
                        .host
                        .finish_admission(&request.request_id, None)
                        .await;
                    return Err(super::transport(error));
                }
            };
            let attempt::Prepared {
                egress: Egress::WebSocket(upstream_request),
                mut facts,
                ..
            } = prepared
            else {
                continue;
            };
            let frame = super::request_text(upstream_request.body())?;
            attempts += 1;
            let mut socket = match self
                .core
                .host
                .transport()
                .open_websocket(*upstream_request)
                .await
            {
                Ok(socket) => socket,
                Err(error) => {
                    self.native_attempt_failed(&facts, &error).await;
                    continue;
                }
            };
            facts.upstream_started_at_ms = Some(crate::quota::now_ms());
            if let Err(error) = socket.send(WsFrame::Text(frame)).await {
                self.native_attempt_failed(&facts, &error).await;
                continue;
            }
            let version = facts
                .credential_version
                .expect("native websocket loaded a credential");
            self.pinned = Some((facts.target.clone(), version));
            self.active = Some(ActiveResponse::new(facts));
            self.native = Some(socket);
            return Ok(true);
        }
        self.core
            .host
            .finish_admission(&request.request_id, None)
            .await;
        fallback_plan.budget.max_attempts =
            fallback_plan.budget.max_attempts.saturating_sub(attempts);
        Ok(false)
    }

    pub(super) async fn prepare_pinned(
        &mut self,
        request: &RequestCtx,
        plan: &Plan,
        classified: &Classified,
    ) -> Result<(), TransportError> {
        let plan = self
            .core
            .host
            .admit(
                &self.identity,
                request,
                Some(classified.key),
                classified.requested_model.as_deref(),
                plan,
            )
            .await
            .map_err(super::transport)?;
        let prepared = match attempt::prepare(
            &self.core,
            self.control.as_ref(),
            &plan.targets[0],
            request,
            classified,
            AdmissionCtx {
                admitted: true,
                owner_user_id: Some(self.identity.user_id),
            },
            Instant::now(),
        )
        .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                self.core
                    .host
                    .finish_admission(&request.request_id, None)
                    .await;
                return Err(super::transport(error));
            }
        };
        let attempt::Prepared {
            egress: Egress::WebSocket(frame),
            mut facts,
            ..
        } = prepared
        else {
            self.core
                .host
                .finish_admission(&request.request_id, None)
                .await;
            return Err(TransportError::Interrupted(
                "pinned channel stopped supporting websocket".into(),
            ));
        };
        if facts.credential_version != self.pinned.as_ref().map(|(_, version)| *version) {
            crate::funnel::complete_stream(
                self.core.host.clone(),
                facts,
                http::StatusCode::SWITCHING_PROTOCOLS,
                crate::funnel::StreamDetails::default(),
                crate::usage::Ended::Interrupted,
            )
            .await;
            return Err(TransportError::Interrupted(
                "websocket credential rotated; reconnect required".into(),
            ));
        }
        facts.upstream_started_at_ms = Some(crate::quota::now_ms());
        let sent = self
            .native
            .as_mut()
            .expect("pinned connection has native socket")
            .send(WsFrame::Text(super::request_text(frame.body())?))
            .await;
        if let Err(error) = sent {
            self.native_attempt_failed(&facts, &error).await;
            crate::funnel::complete_stream(
                self.core.host.clone(),
                facts,
                http::StatusCode::SWITCHING_PROTOCOLS,
                crate::funnel::StreamDetails::default(),
                crate::usage::Ended::Interrupted,
            )
            .await;
            return Err(error);
        }
        self.active = Some(ActiveResponse::new(facts));
        Ok(())
    }

    async fn native_attempt_failed(
        &mut self,
        facts: &crate::funnel::FunnelCtx,
        error: &TransportError,
    ) {
        if let TransportError::Status(status) = error
            && let Ok(status) = http::StatusCode::from_u16(*status)
        {
            let channel = self
                .core
                .channels
                .get(&facts.target.provider.channel)
                .expect("prepared channel");
            let headers = http::HeaderMap::new();
            let disposition = channel.classify(gproxy_channel_api::ResponseView {
                status,
                headers: &headers,
                body: &[],
            });
            crate::funnel::health::record_response(
                self.core.host.as_ref(),
                facts,
                disposition,
                status,
            )
            .await;
        } else {
            crate::funnel::health::degraded(
                self.core.host.as_ref(),
                &facts.target,
                facts.credential_version,
                None,
                "upstream websocket request failed",
            )
            .await;
        }
        crate::funnel::error::attempt_transport(self.core.host.as_ref(), facts, error).await;
    }
}
