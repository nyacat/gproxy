use sea_query::{Alias, Cond, Expr, ExprTrait, Order, Query, UnionType};

use crate::{StoreError, backend::Statement};

pub(crate) const ESTIMATE_PAGE_SIZE: usize = 1_024;
pub(crate) const OBSERVATION_BATCH_SIZE: usize = 128;

#[derive(Clone, Copy)]
pub(crate) struct ObservationSlice {
    pub cycle: i64,
    pub from: i64,
    pub to: i64,
    pub before: Option<(i64, i64)>,
}

pub(crate) fn observation_slice(
    reads: &[ObservationSlice],
    per_cycle: Option<u64>,
) -> Result<Statement, StoreError> {
    let columns = [
        "cycle_id",
        "observed_at_ms",
        "started_at_ms",
        "snapshot_json",
    ];
    // Each branch must apply its LIMIT at the index scan, before combining
    // cycles. A partitioned ROW_NUMBER ranks the remaining history on every
    // page and can spill even when the caller only receives a few rows.
    let mut branches = reads.iter().map(|read| {
        let mut selected = Query::select();
        selected
            .columns(columns.map(Alias::new))
            .from(Alias::new("credential_quota_observations"))
            .and_where(Expr::col(Alias::new("cycle_id")).eq(read.cycle))
            .and_where(Expr::col(Alias::new("observed_at_ms")).gte(read.from))
            .and_where(Expr::col(Alias::new("observed_at_ms")).lt(read.to));
        if let Some((at, started)) = read.before {
            selected.and_where(
                Expr::tuple(["observed_at_ms", "started_at_ms"].map(|c| Expr::col(Alias::new(c))))
                    .lt(Expr::tuple([Expr::val(at), Expr::val(started)])),
            );
        }
        if let Some(limit) = per_cycle {
            selected
                .order_by(Alias::new("observed_at_ms"), Order::Desc)
                .order_by(Alias::new("started_at_ms"), Order::Desc)
                .limit(limit);
        }
        Query::select()
            .columns(columns.map(Alias::new))
            .from_subquery(selected, Alias::new("slice"))
            .to_owned()
    });
    let mut union = branches.next().unwrap_or_else(|| {
        Query::select()
            .columns(columns.map(Alias::new))
            .from(Alias::new("credential_quota_observations"))
            .and_where(Expr::val(1).eq(0))
            .to_owned()
    });
    for branch in branches {
        union.union(UnionType::All, branch);
    }
    let mut query = Query::select();
    query
        .columns(columns.map(Alias::new))
        .from_subquery(union, Alias::new("samples"))
        .order_by(Alias::new("cycle_id"), Order::Asc)
        .order_by(Alias::new("observed_at_ms"), Order::Asc)
        .order_by(Alias::new("started_at_ms"), Order::Asc);
    Statement::query(&query)
}

fn intervals(column: &'static str, ranges: &[(i64, i64)]) -> Cond {
    ranges.iter().fold(Cond::any(), |condition, &(from, to)| {
        condition.add(
            Cond::all()
                .add(Expr::col(Alias::new(column)).gte(from))
                .add(Expr::col(Alias::new(column)).lt(to)),
        )
    })
}

pub(crate) fn estimate_usage(
    credential: i64,
    ranges: &[(i64, i64)],
    after: Option<(i64, i64)>,
) -> Result<Statement, StoreError> {
    let mut query = Query::select();
    query
        .columns(
            [
                "id",
                "upstream_started_at_ms",
                "upstream_model",
                "input_tokens",
                "output_tokens",
                "cached_input_tokens",
                "metrics_json",
                "dimensions_json",
                "cost",
            ]
            .map(Alias::new),
        )
        .from(Alias::new("usage_rows"))
        .and_where(Expr::col(Alias::new("credential_id")).eq(credential))
        .cond_where(intervals("upstream_started_at_ms", ranges))
        .order_by(Alias::new("upstream_started_at_ms"), Order::Asc)
        .order_by(Alias::new("id"), Order::Asc)
        .limit((ESTIMATE_PAGE_SIZE + 1) as u64);
    if let Some((at, id)) = after {
        query.and_where(
            Expr::tuple(["upstream_started_at_ms", "id"].map(|c| Expr::col(Alias::new(c))))
                .gt(Expr::tuple([Expr::val(at), Expr::val(id)])),
        );
    }
    Statement::query(&query)
}

pub(crate) fn estimate_pending(
    credential: i64,
    ranges: &[(i64, i64)],
) -> Result<Statement, StoreError> {
    Statement::query(
        Query::select()
            .columns(["started_at_ms", "model"].map(Alias::new))
            .from(Alias::new("credential_quota_activity"))
            .and_where(Expr::col(Alias::new("state")).is_in(["in_flight", "unresolved"]))
            .and_where(Expr::col(Alias::new("credential_id")).eq(credential))
            .cond_where(intervals("started_at_ms", ranges))
            .and_where(Expr::exists(super::activity::settled_usage_exists()).not()),
    )
}
