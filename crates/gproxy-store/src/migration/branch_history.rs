use std::collections::HashSet;

use crate::StoreError;
use crate::backend::{Executor, Statement};
use crate::schema::{self, Dialect, SchemaVersion};

/// Older self builds used version 8 for the rebuild index, then versions
/// 10/11/12 for the rebuild index, settlement replay and activity lifecycle.
/// Upstream independently assigned 8 to model permissions and 10 to snapshots.
/// A numeric history cannot identify those schemas. Complete missing upstream
/// steps before continuing with the canonical history, without renumbering the
/// stored history or overwriting data already migrated by the old self binary.
pub(super) async fn reconcile(
    executor: &dyn Executor,
    dialect: Dialect,
    applied: i64,
    target: SchemaVersion,
) -> Result<(), StoreError> {
    if !(8..=12).contains(&applied)
        || !columns(executor, dialect, "credential_quota_cycles")
            .await?
            .contains("needs_rebuild")
    {
        return Ok(());
    }
    for version in [
        SchemaVersion::ModelPermissions,
        SchemaVersion::QuotaSnapshots,
    ] {
        if version.number() <= applied && version.number() <= target.number() {
            let statements = additive_statements(executor, dialect, version).await?;
            if !statements.is_empty() {
                executor.batch(statements).await?;
            }
        }
    }
    Ok(())
}

/// The post-divergence steps are additive. Generate them from the same schema
/// catalogue as fresh installations, checking each object independently so a
/// completed legacy step or interrupted DDL batch is safe to retry.
pub(super) async fn additive_statements(
    executor: &dyn Executor,
    dialect: Dialect,
    version: SchemaVersion,
) -> Result<Vec<Statement>, StoreError> {
    debug_assert!(version.number() >= SchemaVersion::ModelPermissions.number());
    let mut statements = Vec::new();
    for table in schema::tables().filter(|table| {
        table.version == version
            || table
                .columns
                .iter()
                .any(|col| col.added_in == Some(version))
            || table
                .indexes
                .iter()
                .any(|index| index.added_in == Some(version))
    }) {
        let existing_columns = columns(executor, dialect, table.name).await?;
        let creating = table.version == version && existing_columns.is_empty();
        if creating {
            statements.push(Statement::plain(schema::create_table(
                table, version, dialect,
            )));
        } else {
            for column in table.columns.iter().filter(|column| {
                column.added_in == Some(version) && !existing_columns.contains(column.name)
            }) {
                statements.push(Statement::plain(schema::add_column(table, column, dialect)));
            }
        }
        let existing_indexes = if creating {
            HashSet::new()
        } else {
            indexes(executor, dialect, table.name).await?
        };
        for index in table.indexes.iter().filter(|index| {
            (index.added_in == Some(version)
                || (table.version == version
                    && index
                        .added_in
                        .is_none_or(|added| added.number() <= version.number())))
                && !existing_indexes.contains(index.name)
        }) {
            statements.push(Statement::plain(schema::create_index(
                table.name, index, dialect,
            )));
        }
    }
    Ok(statements)
}

async fn columns(
    executor: &dyn Executor,
    dialect: Dialect,
    table: &'static str,
) -> Result<HashSet<String>, StoreError> {
    // All identifiers are fixed schema names from the catalogue.
    let sql = match dialect {
        Dialect::NativeSqlite | Dialect::Libsql => {
            format!("SELECT name FROM pragma_table_info('{table}')")
        }
        Dialect::Postgres => format!(
            "SELECT column_name::text AS name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = '{table}'"
        ),
        Dialect::Mysql => format!(
            "SELECT column_name AS name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = '{table}'"
        ),
    };
    names(executor, sql).await
}

async fn indexes(
    executor: &dyn Executor,
    dialect: Dialect,
    table: &'static str,
) -> Result<HashSet<String>, StoreError> {
    let sql = match dialect {
        Dialect::NativeSqlite | Dialect::Libsql => {
            format!("SELECT name FROM pragma_index_list('{table}')")
        }
        Dialect::Postgres => format!(
            "SELECT indexname::text AS name FROM pg_indexes WHERE schemaname = current_schema() AND tablename = '{table}'"
        ),
        Dialect::Mysql => format!(
            "SELECT DISTINCT index_name AS name FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = '{table}'"
        ),
    };
    names(executor, sql).await
}

async fn names(executor: &dyn Executor, sql: String) -> Result<HashSet<String>, StoreError> {
    executor
        .execute(Statement::plain(sql))
        .await?
        .rows
        .into_iter()
        .map(|row| row.text("name").map(str::to_owned))
        .collect()
}
