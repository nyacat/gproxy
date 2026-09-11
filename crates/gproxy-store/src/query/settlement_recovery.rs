use sea_query::{Alias, Expr, ExprTrait, OnConflict, Order, Query};
use serde_json::Value;

use crate::StoreError;
use crate::backend::Statement;
use crate::query::common::{json, value};

pub(crate) fn put(request_id: &str, payload: &Value) -> Result<Statement, StoreError> {
    let payload = json(payload, "settlement replay payload")?;
    // MySQL ignores conflict action WHERE clauses, so guard the replacement
    // value itself to keep completed records intact on every backend.
    let replacement = Expr::case(
        Expr::col((Alias::new("settlement_replays"), Alias::new("completed"))).eq(0),
        value(payload.clone()),
    )
    .finally(Expr::col((
        Alias::new("settlement_replays"),
        Alias::new("payload_json"),
    )));
    insert(
        request_id,
        payload,
        OnConflict::column(Alias::new("request_id"))
            .value(Alias::new("payload_json"), replacement)
            .to_owned(),
    )
}

pub(crate) fn enqueue(request_id: &str, payload: &Value) -> Result<Statement, StoreError> {
    insert(
        request_id,
        json(payload, "settlement replay payload")?,
        OnConflict::column(Alias::new("request_id"))
            .do_nothing_on([Alias::new("request_id")])
            .to_owned(),
    )
}

fn insert(
    request_id: &str,
    payload: String,
    on_conflict: OnConflict,
) -> Result<Statement, StoreError> {
    let query = Query::insert()
        .into_table(Alias::new("settlement_replays"))
        .columns([Alias::new("request_id"), Alias::new("payload_json")])
        .values_panic([value(request_id), value(payload)])
        .on_conflict(on_conflict)
        .to_owned();
    Statement::query(&query)
}

pub(crate) fn list(after: Option<&str>, limit: u32) -> Result<Statement, StoreError> {
    let mut query = Query::select();
    query
        .columns([Alias::new("request_id"), Alias::new("payload_json")])
        .from(Alias::new("settlement_replays"))
        .and_where(Expr::col(Alias::new("completed")).eq(0))
        .order_by(Alias::new("request_id"), Order::Asc)
        .limit(u64::from(limit));
    if let Some(after) = after {
        query.and_where(Expr::col(Alias::new("request_id")).gt(after));
    }
    Statement::query(&query)
}

pub(crate) fn get(request_id: &str) -> Result<Statement, StoreError> {
    let query = Query::select()
        .column(Alias::new("payload_json"))
        .from(Alias::new("settlement_replays"))
        .and_where(Expr::col(Alias::new("request_id")).eq(request_id))
        .to_owned();
    Statement::query(&query)
}

pub(crate) fn delete(request_id: &str) -> Result<Statement, StoreError> {
    let query = Query::delete()
        .from_table(Alias::new("settlement_replays"))
        .and_where(Expr::col(Alias::new("request_id")).eq(request_id))
        .to_owned();
    Statement::query(&query)
}

pub(crate) fn complete(
    request_id: &str,
    expected: Option<&Value>,
) -> Result<Statement, StoreError> {
    let mut query = Query::update()
        .table(Alias::new("settlement_replays"))
        .value(Alias::new("completed"), 1)
        .value(
            Alias::new("payload_json"),
            r#"{"version":1,"completed":true}"#,
        )
        .and_where(Expr::col(Alias::new("request_id")).eq(request_id))
        .to_owned();
    if let Some(expected) = expected {
        query
            .and_where(Expr::col(Alias::new("completed")).eq(0))
            .and_where(
                Expr::col(Alias::new("payload_json"))
                    .eq(json(expected, "settlement replay payload")?),
            );
    }
    Statement::query(&query)
}

pub(crate) fn replace(
    request_id: &str,
    expected: &Value,
    replacement: &Value,
) -> Result<Statement, StoreError> {
    let query = Query::update()
        .table(Alias::new("settlement_replays"))
        .value(
            Alias::new("payload_json"),
            json(replacement, "settlement replay payload")?,
        )
        .and_where(Expr::col(Alias::new("request_id")).eq(request_id))
        .and_where(Expr::col(Alias::new("completed")).eq(0))
        .and_where(
            Expr::col(Alias::new("payload_json")).eq(json(expected, "settlement replay payload")?),
        )
        .to_owned();
    Statement::query(&query)
}
