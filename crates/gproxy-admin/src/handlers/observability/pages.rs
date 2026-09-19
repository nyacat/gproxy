use bytes::Bytes;
use http::{Response, StatusCode};

use crate::dto::{CredentialCycleCursorDto, CredentialCyclePageDto, CredentialCyclePageRequest};
use crate::handlers::util;
use crate::{AdminError, State, response};

pub(in crate::handlers) async fn cycles(
    state: &impl State,
    parts: &http::request::Parts,
    body: &Bytes,
) -> Result<Response<Bytes>, AdminError> {
    use tracing::Instrument;
    let source = "admin.credential_cycles.page";
    let request_id = parts
        .extensions
        .get::<crate::RequestId>()
        .map(|id| id.0.as_str())
        .unwrap_or("unavailable");
    async {
        let mut timing = super::StatisticsTiming {
            started: web_time::Instant::now(),
            outcome: "cancelled",
            source,
        };
        let result = read(state, body).await;
        timing.outcome = if result.is_ok() { "ok" } else { "error" };
        result
    }
    .instrument(tracing::info_span!("quota.statistics", source, request_id))
    .await
}

async fn read(state: &impl State, body: &Bytes) -> Result<Response<Bytes>, AdminError> {
    let request: CredentialCyclePageRequest = util::parse(body)?;
    super::range(request.from, request.to)?;
    if !(1..=100).contains(&request.limit)
        || request.cursor.is_some_and(|cursor| cursor.id <= 0)
        || request.to.checked_mul(1_000).is_none()
        || request.from.checked_mul(1_000).is_none()
    {
        return Err(AdminError::BadRequest(
            "invalid cycle page limit, cursor or range".into(),
        ));
    }
    let page = state
        .store()
        .credential_quota_cycle_page(&gproxy_store::records::CredentialQuotaCyclePageQuery {
            from: request.from,
            to: request.to,
            credential_id: request.credential_id,
            provider_id: request.provider_id,
            window_key: request.window_key,
            cursor: request.cursor.map(|cursor| {
                gproxy_store::records::CredentialQuotaCycleCursor {
                    last_observed_at: cursor.last_observed_at,
                    id: cursor.id,
                }
            }),
            limit: request.limit,
        })
        .await?;
    let mut items = page
        .items
        .iter()
        .map(super::map::credential_cycle)
        .collect::<Vec<_>>();
    if state
        .store()
        .setting(gproxy_store::records::ENABLE_USAGE)
        .await?
        == Some(serde_json::json!(false))
    {
        super::map::hide_local_usage(&mut items);
    }
    response::json(
        StatusCode::OK,
        &CredentialCyclePageDto {
            items,
            next_cursor: page.next_cursor.map(|cursor| CredentialCycleCursorDto {
                last_observed_at: cursor.last_observed_at,
                id: cursor.id,
            }),
        },
    )
}
