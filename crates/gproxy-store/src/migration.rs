mod branch_history;

use sea_query::{
    Alias, ColumnDef, Expr, ExprTrait, MysqlQueryBuilder, PostgresQueryBuilder, Query,
    SqliteQueryBuilder, Table,
};

use crate::StoreError;
use crate::backend::{Executor, Statement};
use crate::schema::{Dialect, SchemaVersion, migration_statements};

pub(crate) async fn migrate(executor: &dyn Executor, dialect: Dialect) -> Result<(), StoreError> {
    migrate_to(executor, dialect, SchemaVersion::LATEST).await
}

pub(crate) async fn migrate_to(
    executor: &dyn Executor,
    dialect: Dialect,
    target: SchemaVersion,
) -> Result<(), StoreError> {
    executor.execute(migration_table(dialect)).await?;
    let applied = applied_versions(executor).await?;
    for (index, version) in applied.iter().enumerate() {
        if *version != index as i64 + 1 {
            return Err(StoreError::Database(
                "schema migration history is not contiguous".into(),
            ));
        }
    }
    if applied.last().copied().unwrap_or_default() > target.number() {
        return Err(StoreError::Database(
            "database schema is newer than this binary".into(),
        ));
    }
    branch_history::reconcile(
        executor,
        dialect,
        applied.last().copied().unwrap_or_default(),
        target,
    )
    .await?;
    for version in SchemaVersion::ALL {
        if version.number() <= applied.last().copied().unwrap_or_default()
            || version.number() > target.number()
        {
            continue;
        }
        let mut statements = if matches!(
            version,
            SchemaVersion::ModelPermissions
                | SchemaVersion::RouteStrategies
                | SchemaVersion::QuotaSnapshots
                | SchemaVersion::QuotaRebuildIndex
                | SchemaVersion::SettlementRecovery
                | SchemaVersion::QuotaActivityLifecycle
        ) {
            branch_history::additive_statements(executor, dialect, version).await?
        } else {
            migration_statements(version, dialect)
                .into_iter()
                .map(Statement::plain)
                .collect()
        };
        if statements
            .first()
            .is_some_and(|statement| statement.sql.starts_with("PRAGMA "))
        {
            executor.execute(statements.remove(0)).await?;
        }
        if version == SchemaVersion::OAuthSessions {
            statements.extend(crate::oauth_migration::statements()?);
        }
        if version == SchemaVersion::RouteOwnership {
            statements.extend(orphaned_route_rows());
        }
        if version == SchemaVersion::OwnedRows {
            // Orphans can own orphans: two passes settle any declared depth.
            let sweep = crate::query::orphan_sweep(version)?;
            statements.extend(sweep.iter().cloned());
            statements.extend(sweep);
        }
        if version == SchemaVersion::QuotaRebuildIndex {
            // Tracking is written by serde_json without whitespace. Backfill
            // once on upgrade; maintenance subsequently uses the index.
            statements.push(Statement::query(
                Query::update()
                    .table(Alias::new("credential_quota_cycles"))
                    .value(Alias::new("needs_rebuild"), 1_i64)
                    .and_where(
                        Expr::col(Alias::new("tracking_json")).like("%\"needs_rebuild\":true%"),
                    ),
            )?);
        }
        if version == SchemaVersion::QuotaActivityLifecycle {
            // A retry may find the DDL already committed (notably on MySQL).
            // Backfills must therefore be safe both for partially upgraded rows
            // and for existing self v12 rows with real attempt/parent identities.
            statements.push(Statement::query(
                Query::update()
                    .table(Alias::new("credential_quota_activity"))
                    .value(
                        Alias::new("parent_request_id"),
                        Expr::col(Alias::new("request_id")),
                    )
                    .and_where(Expr::col(Alias::new("parent_request_id")).is_null()),
            )?);
            statements.push(crate::query::runtime::settle_usage(None)?);
        }
        statements.push(record_version(version)?);
        executor.batch(statements).await?;
    }
    Ok(())
}

fn migration_table(dialect: Dialect) -> Statement {
    let mut version = ColumnDef::new(Alias::new("version"));
    let mut applied_at = ColumnDef::new(Alias::new("applied_at"));
    if matches!(dialect, Dialect::Postgres | Dialect::Mysql) {
        version.big_integer();
        applied_at.big_integer();
    } else {
        version.integer();
        applied_at.integer();
    }
    let table = Table::create()
        .table(Alias::new("schema_migrations"))
        .if_not_exists()
        .col(version.not_null().primary_key())
        .col(applied_at.not_null())
        .to_owned();
    let sql = match dialect {
        Dialect::NativeSqlite | Dialect::Libsql => table.to_string(SqliteQueryBuilder),
        Dialect::Postgres => table.to_string(PostgresQueryBuilder),
        Dialect::Mysql => table.to_string(MysqlQueryBuilder),
    };
    Statement::plain(sql)
}

async fn applied_versions(executor: &dyn Executor) -> Result<Vec<i64>, StoreError> {
    let query = Query::select()
        .column(Alias::new("version"))
        .from(Alias::new("schema_migrations"))
        .order_by(Alias::new("version"), sea_query::Order::Asc)
        .to_owned();
    let result = executor.execute(Statement::query(&query)?).await?;
    result.rows.iter().map(|row| row.i64("version")).collect()
}

fn record_version(version: SchemaVersion) -> Result<Statement, StoreError> {
    let now = web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_err(|error| StoreError::Database(error.to_string()))?
        .as_secs() as i64;
    let statement = Query::insert()
        .into_table(Alias::new("schema_migrations"))
        .columns([Alias::new("version"), Alias::new("applied_at")])
        .values_panic([Expr::value(version.number()), Expr::value(now)])
        .to_owned();
    Statement::query(&statement)
}

fn orphaned_route_rows() -> [Statement; 2] {
    [
        Statement::plain("DELETE FROM route_members WHERE route_id NOT IN (SELECT id FROM routes)"),
        Statement::plain(
            "DELETE FROM exposed_models WHERE route_id NOT IN (SELECT id FROM routes)",
        ),
    ]
}
