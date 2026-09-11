use super::super::{ColumnKind::*, ColumnSpec as Col, IndexSpec, SchemaVersion, TableSpec};

pub(super) const TABLES: &[TableSpec] = &[TableSpec {
    version: SchemaVersion::SettlementRecovery,
    name: "settlement_replays",
    columns: &[
        Col::required("request_id", Text).primary(),
        Col::required("payload_json", Text),
        Col::required("completed", Integer).default("0"),
    ],
    owns: &[],
    indexes: &[IndexSpec {
        name: "ix_settlement_replays_pending",
        columns: &["completed", "request_id"],
        unique: false,
        added_in: None,
    }],
}];
