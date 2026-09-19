use gproxy_channel_api::{Disposition, ResponseView};

use crate::host::Host;

use super::retry::Runner;

impl<H: Host> Runner<H> {
    pub(super) async fn collect_response(
        &mut self,
        response: http::Response<crate::ByteStream>,
    ) -> Result<http::Response<bytes::Bytes>, crate::CoreError> {
        let response = match crate::attempt::body::collect(response).await {
            Ok(response) => response,
            Err(failure) => {
                self.record_stream_health(
                    failure.status,
                    false,
                    Some("upstream fallback response interrupted"),
                )
                .await;
                return Err(failure.error.into());
            }
        };
        self.record_buffered_health(response.status(), response.headers(), response.body())
            .await;
        Ok(response)
    }

    pub(super) async fn record_buffered_health(
        &mut self,
        status: http::StatusCode,
        headers: &http::HeaderMap,
        body: &[u8],
    ) {
        let channel = self
            .core
            .channels
            .get(&self.facts.target.provider.channel)
            .expect("prepared channel");
        let failure = channel.response_failure(ResponseView {
            status,
            headers,
            body,
        });
        let disposition = failure.as_ref().map_or_else(
            || {
                channel.classify(ResponseView {
                    status,
                    headers,
                    body,
                })
            },
            |failure| failure.disposition,
        );
        if !self.should_record_health(disposition) {
            return;
        }
        if let Some(failure) = failure {
            crate::funnel::diagnostic::log(&self.facts, status, &failure, false, false);
            crate::funnel::health::record_failure(
                self.core.host.as_ref(),
                &self.facts,
                status,
                &failure,
            )
            .await;
        } else {
            crate::funnel::health::record_response(
                self.core.host.as_ref(),
                &self.facts,
                disposition,
                status,
            )
            .await;
        }
        self.last_health = Some(disposition);
    }

    pub(super) async fn record_stream_health(
        &mut self,
        status: http::StatusCode,
        completed: bool,
        interruption: Option<&str>,
    ) {
        let (terminal, failure) = self.meter.health();
        let disposition = failure
            .as_ref()
            .map(|failure| failure.disposition)
            .or(terminal)
            .or(completed.then_some(Disposition::Success))
            .or(interruption.map(|_| Disposition::Retryable));
        let Some(disposition) =
            disposition.filter(|disposition| self.should_record_health(*disposition))
        else {
            return;
        };
        if let Some(failure) = failure {
            crate::funnel::diagnostic::log(&self.facts, status, &failure, true, false);
            crate::funnel::health::record_failure(
                self.core.host.as_ref(),
                &self.facts,
                status,
                &failure,
            )
            .await;
        } else if let Some(disposition) = terminal.or(completed.then_some(Disposition::Success)) {
            crate::funnel::health::record_response(
                self.core.host.as_ref(),
                &self.facts,
                disposition,
                status,
            )
            .await;
        } else if let Some(detail) = interruption {
            crate::funnel::health::degraded(
                self.core.host.as_ref(),
                &self.facts.target,
                self.facts.credential_version,
                Some(status),
                detail,
            )
            .await;
        } else {
            return;
        }
        self.last_health = Some(disposition);
    }

    pub(super) async fn record_interrupted_health(&mut self, detail: &str) {
        self.record_stream_health(http::StatusCode::OK, false, Some(detail))
            .await;
    }

    pub(super) async fn record_transport_health(&mut self) {
        crate::funnel::health::degraded(
            self.core.host.as_ref(),
            &self.facts.target,
            self.facts.credential_version,
            None,
            "upstream fallback transport failed",
        )
        .await;
        self.last_health = Some(Disposition::Retryable);
    }

    fn should_record_health(&self, disposition: Disposition) -> bool {
        self.last_health.is_none()
            || self.last_health == Some(Disposition::Success) && disposition != Disposition::Success
    }
}
