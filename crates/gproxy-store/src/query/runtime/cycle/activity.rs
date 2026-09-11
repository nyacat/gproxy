use crate::{StoreError, backend::Statement};
use sea_query::{Alias, Expr, ExprTrait, OnConflict, Query};

pub(crate) fn begin_usage(
    request: &str,
    parent: &str,
    credential: i64,
    model: &str,
    at_ms: i64,
) -> Result<Statement, StoreError> {
    let mut query = Query::insert();
    query
        .into_table(Alias::new("credential_quota_activity"))
        .columns(
            [
                "request_id",
                "parent_request_id",
                "credential_id",
                "model",
                "started_at_ms",
                "state",
            ]
            .map(Alias::new),
        )
        .values_panic([
            request.into(),
            parent.into(),
            credential.into(),
            model.into(),
            at_ms.into(),
            "in_flight".into(),
        ])
        .on_conflict(
            OnConflict::columns(["request_id", "credential_id", "started_at_ms"].map(Alias::new))
                .do_nothing()
                .to_owned(),
        );
    Statement::query(&query)
}

// Completion is distinct from metering. An ended attempt with no usage remains
// an uncertainty in its original credential's window, even after failover.
pub(crate) fn finish_usage(
    request: &str,
    credential: i64,
    started: i64,
) -> Result<Statement, StoreError> {
    Statement::query(
        Query::update()
            .table(Alias::new("credential_quota_activity"))
            .value(Alias::new("state"), "unresolved")
            .and_where(Expr::col(Alias::new("request_id")).eq(request))
            .and_where(Expr::col(Alias::new("credential_id")).eq(credential))
            .and_where(Expr::col(Alias::new("started_at_ms")).eq(started))
            .and_where(Expr::col(Alias::new("state")).eq("in_flight")),
    )
}

pub(crate) fn finish_usage_request(parent: &str) -> Result<Statement, StoreError> {
    Statement::query(
        Query::update()
            .table(Alias::new("credential_quota_activity"))
            .value(Alias::new("state"), "unresolved")
            .and_where(Expr::col(Alias::new("parent_request_id")).eq(parent))
            .and_where(Expr::col(Alias::new("state")).eq("in_flight")),
    )
}

pub(crate) fn settled_usage_exists() -> sea_query::SelectStatement {
    let mut settled = Query::select();
    settled.expr(Expr::val(1)).from(Alias::new("usage_rows"));
    for (left, right) in [
        ("request_id", "request_id"),
        ("credential_id", "credential_id"),
        ("upstream_started_at_ms", "started_at_ms"),
    ] {
        settled.and_where(
            Expr::col((Alias::new("usage_rows"), Alias::new(left)))
                .equals((Alias::new("credential_quota_activity"), Alias::new(right))),
        );
    }
    settled
}

pub(crate) fn settle_usage(request: Option<&str>) -> Result<Statement, StoreError> {
    let mut update = Query::update();
    update
        .table(Alias::new("credential_quota_activity"))
        .value(Alias::new("state"), "settled")
        .and_where(Expr::col(Alias::new("state")).ne("settled"))
        // A duplicate request id may belong to another credential. Only the
        // actual durable row proves that this precise attempt was metered.
        .and_where(Expr::exists(settled_usage_exists()));
    if let Some(request) = request {
        update.and_where(Expr::col(Alias::new("request_id")).eq(request));
    }
    Statement::query(&update)
}
