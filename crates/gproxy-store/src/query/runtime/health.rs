use sea_query::{Alias, Cond, Expr, ExprTrait, OnConflict, Query, SelectStatement};

use crate::StoreError;
use crate::backend::Statement;
use crate::query::common::value;
use crate::records::{CredentialHealthInput, CredentialHealthState};

pub(crate) fn upsert(input: &CredentialHealthInput) -> Result<Statement, StoreError> {
    let mut query = Query::insert();
    query
        .into_table(Alias::new("credential_health"))
        .columns(
            [
                "credential_id",
                "model",
                "credential_version",
                "version",
                "state",
                "consecutive_failures",
                "observed_at",
                "response_status",
                "detail",
            ]
            .into_iter()
            .map(Alias::new),
        )
        .values_panic([
            value(input.credential_id),
            value(input.model.clone()),
            value(i64::try_from(input.credential_version).map_err(|_| {
                StoreError::InvalidData {
                    field: "credential health version",
                    message: "version exceeds SQLite integer range".into(),
                }
            })?),
            value(input.version),
            value(input.state.as_str()),
            value(i64::from(input.state == CredentialHealthState::Degraded)),
            value(input.observed_at),
            value(input.response_status.map(i64::from)),
            value(input.detail.clone()),
        ])
        .on_conflict(
            OnConflict::columns([Alias::new("credential_id"), Alias::new("model")])
                .value(Alias::new("consecutive_failures"), next_failures())
                .update_columns([
                    Alias::new("state"),
                    Alias::new("credential_version"),
                    Alias::new("version"),
                    Alias::new("observed_at"),
                    Alias::new("response_status"),
                    Alias::new("detail"),
                ])
                .action_cond_where(
                    Cond::any()
                        .add(
                            Expr::col((Alias::new("excluded"), Alias::new("credential_version")))
                                .gt(Expr::col((
                                    Alias::new("credential_health"),
                                    Alias::new("credential_version"),
                                ))),
                        )
                        .add(
                            Cond::all()
                                .add(
                                    Expr::col((
                                        Alias::new("excluded"),
                                        Alias::new("credential_version"),
                                    ))
                                    .eq(Expr::col((
                                        Alias::new("credential_health"),
                                        Alias::new("credential_version"),
                                    ))),
                                )
                                .add(
                                    Expr::col((Alias::new("excluded"), Alias::new("version"))).gt(
                                        Expr::col((
                                            Alias::new("credential_health"),
                                            Alias::new("version"),
                                        )),
                                    ),
                                ),
                        ),
                )
                .to_owned(),
        );
    Statement::query(&query)
}

pub(crate) fn recover_degraded(input: &CredentialHealthInput) -> Result<Statement, StoreError> {
    if input.state != CredentialHealthState::Healthy {
        return Err(StoreError::InvalidData {
            field: "credential health state",
            message: "recovery requires a healthy observation".into(),
        });
    }
    let credential_version =
        i64::try_from(input.credential_version).map_err(|_| StoreError::InvalidData {
            field: "credential health version",
            message: "version exceeds SQLite integer range".into(),
        })?;
    Statement::query(
        Query::update()
            .table(Alias::new("credential_health"))
            .values([
                (
                    Alias::new("state"),
                    value(CredentialHealthState::Healthy.as_str()),
                ),
                (Alias::new("consecutive_failures"), value(0_i64)),
                (Alias::new("version"), value(input.version)),
                (Alias::new("observed_at"), value(input.observed_at)),
                (
                    Alias::new("response_status"),
                    value(input.response_status.map(i64::from)),
                ),
                (Alias::new("detail"), value(input.detail.clone())),
            ])
            .and_where(Expr::col(Alias::new("credential_id")).eq(input.credential_id))
            .and_where(Expr::col(Alias::new("model")).eq(input.model.clone()))
            .and_where(Expr::col(Alias::new("credential_version")).eq(credential_version))
            .and_where(Expr::col(Alias::new("state")).eq(CredentialHealthState::Degraded.as_str()))
            .and_where(Expr::col(Alias::new("version")).lt(input.version)),
    )
}

pub(crate) fn select_all() -> Result<Statement, StoreError> {
    Statement::query(
        select()
            .order_by(Alias::new("credential_id"), sea_query::Order::Asc)
            .order_by(Alias::new("model"), sea_query::Order::Asc),
    )
}

pub(crate) fn select_one(credential_id: i64, model: &str) -> Result<Statement, StoreError> {
    Statement::query(
        select()
            .and_where(Expr::col(Alias::new("credential_id")).eq(credential_id))
            .and_where(Expr::col(Alias::new("model")).eq(model)),
    )
}

fn select() -> SelectStatement {
    let mut query = Query::select();
    query
        .columns(
            [
                "credential_id",
                "model",
                "credential_version",
                "version",
                "state",
                "consecutive_failures",
                "observed_at",
                "response_status",
                "detail",
            ]
            .into_iter()
            .map(Alias::new),
        )
        .from(Alias::new("credential_health"));
    query
}

fn next_failures() -> Expr {
    let previous = |column| Expr::col((Alias::new("credential_health"), Alias::new(column)));
    let incoming = |column| Expr::col((Alias::new("excluded"), Alias::new(column)));
    // Static literals keep the conflict action free of bind parameters, since
    // MySQL renders that action with its own conditional-update syntax.
    Expr::case(
        incoming("state").eq(Expr::cust("'degraded'")),
        Expr::case(
            Cond::all()
                .add(incoming("credential_version").eq(previous("credential_version")))
                .add(previous("state").eq(Expr::cust("'degraded'"))),
            Expr::case(
                previous("consecutive_failures").lt(Expr::cust("16")),
                previous("consecutive_failures").add(Expr::cust("1")),
            )
            .finally(Expr::cust("16")),
        )
        .finally(Expr::cust("1")),
    )
    .finally(Expr::cust("0"))
    .into()
}

pub(crate) fn delete(credential_id: i64, model: Option<&str>) -> Result<Statement, StoreError> {
    let mut query = Query::delete();
    query
        .from_table(Alias::new("credential_health"))
        .and_where(sea_query::Expr::col(Alias::new("credential_id")).eq(credential_id));
    if let Some(model) = model {
        query.and_where(Expr::col(Alias::new("model")).eq(model));
    }
    Statement::query(&query)
}
