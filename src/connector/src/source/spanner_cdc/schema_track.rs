// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Schema tracker for Spanner CDC.
//!
//! Spanner change streams include `column_types` metadata with every data change record.
//! This module tracks schema per table and emits Debezium-compatible schema change messages
//! when the schema evolves.
//!
//! Within a single partition, Spanner guarantees commit-timestamp order, so no
//! cross-partition guard is needed. The tracker is per-partition (no locks, no sharing).

use std::collections::HashMap;

use crate::source::spanner_cdc::types::{ColumnType, DataChangeRecord, SpannerType};

/// Schema information derived from Spanner's column_types
#[derive(Debug, Clone, PartialEq, Eq)]
struct TableSchema {
    columns: Vec<ColumnSchema>,
}

/// Column schema derived from Spanner ColumnType
#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnSchema {
    name: String,
    spanner_type: SpannerType,
}

impl ColumnSchema {
    fn from_column_type(ct: &ColumnType) -> Self {
        Self {
            name: ct.name.clone(),
            spanner_type: ct.spanner_type.clone(),
        }
    }
}

/// Schema tracker.
///
/// Owned by a single partition task — no locks, no sharing.
/// Each partition task creates its own tracker.
pub(crate) struct SchemaTracker {
    /// table_name → schema
    schemas: HashMap<String, TableSchema>,
}

impl SchemaTracker {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    /// Check a data change record for schema changes.
    ///
    /// Returns `Some(SourceMessage)` if a schema change should be emitted before the data
    /// records, `None` otherwise.
    pub fn check_and_evolve(
        &mut self,
        data_change: &DataChangeRecord,
    ) -> Option<SchemaChangePayload> {
        let table_name = &data_change.table_name;
        let new_columns: Vec<ColumnSchema> = data_change
            .column_types
            .iter()
            .map(ColumnSchema::from_column_type)
            .collect();

        match self.schemas.get(table_name) {
            None => {
                // First encounter: always emit (false positive, let meta deduplicate — same as Postgres).
                let payload = self.make_payload(table_name, &new_columns);
                self.schemas.insert(
                    table_name.to_string(),
                    TableSchema {
                        columns: new_columns,
                    },
                );
                Some(payload)
            }
            Some(entry) => {
                // Content check: emit only if schema actually changed.
                if schemas_equal(&entry.columns, &new_columns) {
                    return None;
                }
                let payload = self.make_payload(table_name, &new_columns);
                self.schemas.insert(
                    table_name.to_string(),
                    TableSchema {
                        columns: new_columns,
                    },
                );
                Some(payload)
            }
        }
    }

    /// Build a Debezium-compatible schema change JSON payload.
    fn make_payload(&self, table_name: &str, columns: &[ColumnSchema]) -> SchemaChangePayload {
        let cols: Vec<serde_json::Value> = columns
            .iter()
            .map(|col| {
                serde_json::json!({
                    "name": col.name,
                    "typeName": col.spanner_type.to_type_string(),
                })
            })
            .collect();

        let payload = serde_json::json!({
            "ddl": "UNKNOWN_DDL",
            "tableChanges": [{
                "id": table_name,
                "type": "ALTER",
                "table": { "columns": cols },
            }],
        });

        SchemaChangePayload {
            json: serde_json::to_vec(&payload)
                .expect("schema change JSON serialization is infallible"),
        }
    }
}

/// Result from `SchemaTracker::check_and_evolve`.
pub(crate) struct SchemaChangePayload {
    pub json: Vec<u8>,
}

/// Compare column schemas by name and type (cheap, no allocation).
fn schemas_equal(old: &[ColumnSchema], new: &[ColumnSchema]) -> bool {
    if old.len() != new.len() {
        return false;
    }
    old.iter()
        .zip(new.iter())
        .all(|(a, b)| a.name == b.name && a.spanner_type == b.spanner_type)
}

#[cfg(test)]
mod tests {
    use time::OffsetDateTime;

    use super::*;
    use crate::source::spanner_cdc::types::{SpannerType, TypeCode};

    fn col(name: &str, code: TypeCode) -> ColumnType {
        ColumnType {
            name: name.to_string(),
            spanner_type: SpannerType::simple(code),
            is_primary_key: false,
            ordinal_position: 0,
        }
    }

    fn data_change(table: &str, ts: i64, cols: Vec<ColumnType>) -> DataChangeRecord {
        DataChangeRecord {
            commit_timestamp: OffsetDateTime::from_unix_timestamp(ts).unwrap(),
            record_sequence: "0".to_string(),
            server_transaction_id: "txn".to_string(),
            is_last_record_in_transaction_in_partition: true,
            table_name: table.to_string(),
            value_capture_type: "NEW_ROW".to_string(),
            column_types: cols,
            mods: vec![],
            mod_type: "INSERT".to_string(),
            number_of_records_in_transaction: 1,
            number_of_partitions_in_transaction: 1,
            transaction_tag: String::new(),
            is_system_transaction: false,
        }
    }

    #[test]
    fn first_encounter_emits() {
        let mut tracker = SchemaTracker::new();
        let dc = data_change("t", 100, vec![col("id", TypeCode::Int64)]);
        assert!(tracker.check_and_evolve(&dc).is_some());
    }

    #[test]
    fn same_schema_does_not_reemit() {
        let mut tracker = SchemaTracker::new();
        let cols = vec![col("id", TypeCode::Int64)];
        let dc1 = data_change("t", 100, cols.clone());
        let dc2 = data_change("t", 200, cols);
        assert!(tracker.check_and_evolve(&dc1).is_some());
        assert!(tracker.check_and_evolve(&dc2).is_none());
    }

    #[test]
    fn schema_change_emits() {
        let mut tracker = SchemaTracker::new();
        let dc1 = data_change("t", 100, vec![col("id", TypeCode::Int64)]);
        assert!(tracker.check_and_evolve(&dc1).is_some());

        let dc2 = data_change(
            "t",
            200,
            vec![col("id", TypeCode::Int64), col("name", TypeCode::String)],
        );
        assert!(tracker.check_and_evolve(&dc2).is_some());
    }

    #[test]
    fn different_tables_independent() {
        let mut tracker = SchemaTracker::new();

        let dc_a = data_change("a", 100, vec![col("id", TypeCode::Int64)]);
        let dc_b = data_change("b", 100, vec![col("id", TypeCode::String)]);
        assert!(tracker.check_and_evolve(&dc_a).is_some());
        assert!(tracker.check_and_evolve(&dc_b).is_some());
    }
}
