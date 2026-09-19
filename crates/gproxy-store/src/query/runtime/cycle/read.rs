use sea_query::{Alias, Expr, ExprTrait, Order, OverStatement, Query, WindowStatement};

use super::COLUMNS;
use crate::StoreError;
use crate::backend::Statement;

pub(crate) fn read_credential_quota_cycle(id: i64) -> Result<Statement, StoreError> {
    let mut query = cycle_select();
    query.and_where(Expr::col(Alias::new("id")).eq(id)).limit(1);
    Statement::query(&query)
}

pub(crate) fn read_open_credential_quota_cycle(
    credential_id: i64,
    window_key: &str,
) -> Result<Statement, StoreError> {
    let mut query = cycle_select();
    query
        .and_where(Expr::col(Alias::new("credential_id")).eq(credential_id))
        .and_where(Expr::col(Alias::new("window_key")).eq(window_key))
        .and_where(Expr::col(Alias::new("open_slot")).eq(1))
        .limit(1);
    Statement::query(&query)
}

pub(crate) fn read_latest_credential_quota_cycle(
    credential_id: i64,
    window_key: &str,
) -> Result<Statement, StoreError> {
    let mut query = cycle_select();
    query
        .and_where(Expr::col(Alias::new("credential_id")).eq(credential_id))
        .and_where(Expr::col(Alias::new("window_key")).eq(window_key))
        .order_by(Alias::new("id"), Order::Desc)
        .limit(1);
    Statement::query(&query)
}

pub(crate) fn select_open_credential_quota_cycles(
    credential_id: Option<i64>,
) -> Result<Statement, StoreError> {
    let mut query = cycle_select();
    query.and_where(Expr::col(Alias::new("open_slot")).eq(1));
    if let Some(credential_id) = credential_id {
        query.and_where(Expr::col(Alias::new("credential_id")).eq(credential_id));
    }
    query.order_by(Alias::new("id"), Order::Asc);
    Statement::query(&query)
}

pub(crate) fn select_credential_quota_cycle_history(
    credential_id: i64,
    window_key: &str,
) -> Result<Statement, StoreError> {
    select_credential_quota_cycle_history_limited(credential_id, window_key, None)
}

pub(crate) const REPAIR_HISTORY: u64 = 16;

pub(crate) fn pending_credential_quota_rebuilds(
    credential_id: Option<i64>,
    after: i64,
) -> Result<Statement, StoreError> {
    let mut query = Query::select();
    query
        .column(Alias::new("id"))
        .from(Alias::new("credential_quota_cycles"))
        .and_where(Expr::col(Alias::new("needs_rebuild")).eq(1))
        .and_where(Expr::col(Alias::new("id")).gt(after))
        .order_by(Alias::new("id"), Order::Asc)
        .limit(REPAIR_HISTORY);
    if let Some(credential_id) = credential_id {
        query.and_where(Expr::col(Alias::new("credential_id")).eq(credential_id));
    }
    Statement::query(&query)
}

pub(crate) fn select_credential_quota_cycle_history_limited(
    credential_id: i64,
    window_key: &str,
    limit: Option<u64>,
) -> Result<Statement, StoreError> {
    let mut query = cycle_select();
    query
        .and_where(Expr::col(Alias::new("credential_id")).eq(credential_id))
        .and_where(Expr::col(Alias::new("window_key")).eq(window_key))
        .order_by(Alias::new("id"), Order::Desc);
    if let Some(limit) = limit {
        query.limit(limit);
    }
    Statement::query(&query)
}

/// A busy window must not push quieter ones out of the page, so the row budget
/// is spent per `(credential, window)` rather than across the whole range.
const CYCLES_PER_WINDOW: u64 = 100;
const CYCLE_ROWS: u64 = 5_000;

pub(crate) fn select_credential_quota_cycles(
    credential_id: Option<i64>,
    from: i64,
    to: i64,
) -> Result<Statement, StoreError> {
    select_filtered_credential_quota_cycles(credential_id, None, from, to)
}

pub(crate) fn select_filtered_credential_quota_cycles(
    credential_id: Option<i64>,
    provider_id: Option<i64>,
    from: i64,
    to: i64,
) -> Result<Statement, StoreError> {
    select_quota_statistics_cycles(credential_id, provider_id, from, to, &Default::default())
}

pub(crate) fn select_quota_statistics_cycles(
    credential_id: Option<i64>,
    provider_id: Option<i64>,
    from: i64,
    to: i64,
    options: &crate::records::CredentialQuotaStatisticsOptions,
) -> Result<Statement, StoreError> {
    // Rank only keys. Historical tracking/metrics JSON must not participate in
    // sorting; fetch complete records after the per-window selection.
    let mut ranked = Query::select();
    ranked
        .columns(["id", "last_observed_at"].map(Alias::new))
        .from(Alias::new("credential_quota_cycles"));
    if let Some(ids) = &options.cycle_ids {
        ranked.and_where(Expr::col(Alias::new("id")).is_in(ids.iter().copied()));
    }
    if let Some(credential_id) = credential_id {
        ranked.and_where(Expr::col(Alias::new("credential_id")).eq(credential_id));
    }
    if let Some(provider_id) = provider_id {
        let mut credentials = Query::select();
        credentials
            .column(Alias::new("id"))
            .from(Alias::new("credentials"))
            .and_where(Expr::col(Alias::new("provider_id")).eq(provider_id));
        ranked.and_where(Expr::col(Alias::new("credential_id")).in_subquery(credentials));
    }
    let mut window = WindowStatement::partition_by(Alias::new("credential_id"));
    OverStatement::partition_by(&mut window, Alias::new("window_key"));
    window
        .order_by(Alias::new("last_observed_at"), Order::Desc)
        .order_by(Alias::new("id"), Order::Desc);
    ranked.and_where(Expr::col(Alias::new("last_observed_at")).gte(from));
    if options.observation_range_ms.is_some() {
        // A cycle can have visible observations even when newer observations
        // exist beyond the selected chart range.
        ranked.and_where(Expr::col(Alias::new("accounting_start_ms")).lt(to.saturating_mul(1_000)));
    } else {
        ranked.and_where(Expr::col(Alias::new("last_observed_at")).lt(to));
    }
    ranked.expr_window_as(
        Expr::cust("ROW_NUMBER()"),
        window,
        Alias::new("window_rank"),
    );

    let mut selected = Query::select();
    selected
        .column(Alias::new("id"))
        .from_subquery(ranked, Alias::new("ranked"))
        .and_where(
            Expr::col(Alias::new("window_rank")).lte(if options.current_only {
                1
            } else {
                CYCLES_PER_WINDOW
            }),
        )
        .order_by(Alias::new("last_observed_at"), Order::Desc)
        .order_by(Alias::new("id"), Order::Desc)
        .limit(CYCLE_ROWS);
    let mut query = cycle_select();
    query
        .and_where(Expr::col(Alias::new("id")).in_subquery(selected))
        .order_by(Alias::new("last_observed_at"), Order::Desc)
        .order_by(Alias::new("id"), Order::Desc);
    Statement::query(&query)
}

pub(crate) fn recent_quota_observation(
    credential: i64,
    after: i64,
    before: i64,
) -> Result<Statement, StoreError> {
    Statement::query(
        Query::select()
            .expr(Expr::val(1))
            .from(Alias::new("credential_quota_cycles"))
            .and_where(Expr::col(Alias::new("credential_id")).eq(credential))
            .and_where(Expr::col(Alias::new("last_observed_at")).gt(after))
            .and_where(Expr::col(Alias::new("last_observed_at")).lt(before))
            .limit(1),
    )
}

fn cycle_select() -> sea_query::SelectStatement {
    let mut query = Query::select();
    query
        .columns(COLUMNS.iter().copied().map(Alias::new))
        .from(Alias::new("credential_quota_cycles"));
    query.to_owned()
}

pub(crate) fn quota_window_states(credential: Option<i64>) -> Result<Statement, StoreError> {
    let mut query = Query::select();
    query
        .columns(
            [
                "id",
                "version",
                "credential_id",
                "window_key",
                "period_start",
                "period_end",
                "boundary_source",
                "last_observed_at",
                "upstream_used",
                "upstream_limit",
                "used_percent",
            ]
            .map(Alias::new),
        )
        .from(Alias::new("credential_quota_cycles"))
        .and_where(Expr::col(Alias::new("open_slot")).eq(1))
        .order_by(Alias::new("id"), Order::Asc);
    if let Some(credential) = credential {
        query.and_where(Expr::col(Alias::new("credential_id")).eq(credential));
    }
    Statement::query(&query)
}
