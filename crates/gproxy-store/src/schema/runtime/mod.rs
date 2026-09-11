mod binding;
mod log;
mod quota;
mod quota_snapshot;
mod quota_tracking;
mod settlement_recovery;
mod usage;

use super::TableSpec;

pub(super) fn tables() -> impl Iterator<Item = &'static TableSpec> {
    quota::TABLES
        .iter()
        .chain(usage::TABLES)
        .chain(log::TABLES)
        .chain(binding::TABLES)
        .chain(quota_tracking::TABLES)
        .chain(quota_snapshot::TABLES)
        .chain(settlement_recovery::TABLES)
}
