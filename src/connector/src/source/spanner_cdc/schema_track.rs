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
//! The tracker is shared across all partition tasks via `Arc<Mutex<>>`. It acts as a
//! schema registry: each table's schema is stored by value. The first partition to
//! encounter a given schema emits the change event; subsequent partitions with the same
//! schema skip it. When a partition sees a different schema, the commit timestamp is
//! used to distinguish real DDL evolution (newer timestamp → emit) from a stale partition
//! seeing an older schema (older or equal timestamp → skip, adopt stored schema).
//!
//! Edge case: if two partitions see different schemas at the exact same commit timestamp,
//! the first one to acquire the lock wins (`<=` check). This is safe because Spanner DDL
//! produces records with the old schema at timestamps < T_DDL and the new schema at ≥ T_DDL,
//! so same-timestamp schema conflicts don't occur in practice. If they did, the losing
//! partition's schema would be silently adopted without emission — self-healing when any
//! partition processes a record at a newer timestamp.

use std::collections::HashMap;

use time::OffsetDateTime;

use crate::source::spanner_cdc::types::{ColumnType, SpannerType};

/// Column schema derived from Spanner ColumnType.
///
/// Only `name` and `spanner_type` are compared. `is_primary_key` and `ordinal_position`
/// are ignored because they don't affect downstream schema processing — the Debezium
/// schema change message only carries column names and type names.
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

/// Schema registry shared across all partition tasks via `Arc<Mutex<>>`.
///
/// Lock is acquired on every data change record. The hot path (schema unchanged) is
/// ~100ns: one `HashMap::get` + one slice comparison. Allocation only happens on first
/// encounter or real schema change (rare).
pub(crate) struct SchemaTracker {
    /// table_name → (schema, last commit timestamp that produced this schema)
    schemas: HashMap<String, (Vec<ColumnSchema>, OffsetDateTime)>,
}

impl SchemaTracker {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    /// Check a data change record for schema changes.
    ///
    /// Returns `Some(SchemaChangePayload)` if a schema change should be emitted before
    /// the data records, `None` otherwise.
    ///
    /// Comparison is done directly against `column_types` (no allocation on hot path).
    /// When the stored schema differs, the commit timestamp determines the action:
    /// - Incoming timestamp > stored → real DDL evolution → emit + update.
    /// - Incoming timestamp <= stored → stale partition → skip, adopt stored schema.
    pub fn check_and_evolve(
        &mut self,
        table_name: &str,
        column_types: &[ColumnType],
        commit_timestamp: OffsetDateTime,
    ) -> Option<SchemaChangePayload> {
        match self.schemas.get(table_name) {
            None => {
                // First encounter: emit and register.
                let columns: Vec<ColumnSchema> =
                    column_types.iter().map(ColumnSchema::from_column_type).collect();
                let payload = Self::make_payload(table_name, &columns, true);
                self.schemas
                    .insert(table_name.to_string(), (columns, commit_timestamp));
                Some(payload)
            }
            Some((stored, stored_ts)) => {
                // Hot path: same schema → skip (no allocation).
                if schemas_equal_from_column_types(stored, column_types) {
                    return None;
                }
                // Schema differs. Check timestamp to distinguish DDL from stale partition.
                if commit_timestamp <= *stored_ts {
                    // Stale partition seeing an older schema. Skip emission.
                    return None;
                }
                // Real schema evolution at a newer timestamp. Emit and update.
                let columns: Vec<ColumnSchema> =
                    column_types.iter().map(ColumnSchema::from_column_type).collect();
                let payload = Self::make_payload(table_name, &columns, false);
                self.schemas
                    .insert(table_name.to_string(), (columns, commit_timestamp));
                Some(payload)
            }
        }
    }

    /// Build a Debezium-compatible schema change JSON payload.
    ///
    /// `is_first` distinguishes first-encounter registration from real schema evolution.
    fn make_payload(
        table_name: &str,
        columns: &[ColumnSchema],
        is_first: bool,
    ) -> SchemaChangePayload {
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
                "type": if is_first { "CREATE" } else { "ALTER" },
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

/// Compare stored `ColumnSchema`s against raw `ColumnType`s without allocating.
///
/// Only compares `name` and `spanner_type` — `is_primary_key` and `ordinal_position`
/// are intentionally ignored (see `ColumnSchema` doc).
fn schemas_equal_from_column_types(old: &[ColumnSchema], new: &[ColumnType]) -> bool {
    if old.len() != new.len() {
        return false;
    }
    old.iter()
        .zip(new.iter())
        .all(|(a, b)| a.name == b.name && a.spanner_type == b.spanner_type)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

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

    fn ts(secs: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(secs).unwrap()
    }

    #[test]
    fn first_encounter_emits() {
        let mut tracker = SchemaTracker::new();
        let cols = vec![col("id", TypeCode::Int64)];
        assert!(tracker.check_and_evolve("t", &cols, ts(100)).is_some());
    }

    #[test]
    fn same_schema_does_not_reemit() {
        let mut tracker = SchemaTracker::new();
        let cols = vec![col("id", TypeCode::Int64)];
        assert!(tracker.check_and_evolve("t", &cols, ts(100)).is_some());
        assert!(tracker.check_and_evolve("t", &cols, ts(200)).is_none());
    }

    #[test]
    fn real_ddl_at_newer_timestamp_emits() {
        let mut tracker = SchemaTracker::new();
        let cols1 = vec![col("id", TypeCode::Int64)];
        assert!(tracker.check_and_evolve("t", &cols1, ts(100)).is_some());

        let cols2 = vec![col("id", TypeCode::Int64), col("name", TypeCode::String)];
        assert!(tracker.check_and_evolve("t", &cols2, ts(200)).is_some());
    }

    #[test]
    fn stale_partition_with_old_schema_skips() {
        let mut tracker = SchemaTracker::new();
        // Partition A sees new schema at T200.
        let cols_new = vec![col("id", TypeCode::Int64), col("name", TypeCode::String)];
        assert!(tracker.check_and_evolve("t", &cols_new, ts(200)).is_some());

        // Partition B sees old schema at T100 (stale, behind partition A).
        let cols_old = vec![col("id", TypeCode::Int64)];
        assert!(tracker.check_and_evolve("t", &cols_old, ts(100)).is_none());
    }

    #[test]
    fn stale_partition_at_same_timestamp_skips() {
        let mut tracker = SchemaTracker::new();
        let cols_new = vec![col("id", TypeCode::Int64), col("name", TypeCode::String)];
        assert!(tracker.check_and_evolve("t", &cols_new, ts(100)).is_some());

        let cols_old = vec![col("id", TypeCode::Int64)];
        assert!(tracker.check_and_evolve("t", &cols_old, ts(100)).is_none());
    }

    #[test]
    fn different_tables_independent() {
        let mut tracker = SchemaTracker::new();
        let cols_a = vec![col("id", TypeCode::Int64)];
        let cols_b = vec![col("id", TypeCode::String)];
        assert!(tracker.check_and_evolve("a", &cols_a, ts(100)).is_some());
        assert!(tracker.check_and_evolve("b", &cols_b, ts(100)).is_some());
    }

    #[test]
    fn same_schema_repeatedly_returns_none() {
        let mut tracker = SchemaTracker::new();
        let cols = vec![col("id", TypeCode::Int64), col("name", TypeCode::String)];
        assert!(tracker.check_and_evolve("t", &cols, ts(100)).is_some());
        assert!(tracker.check_and_evolve("t", &cols, ts(200)).is_none());
        assert!(tracker.check_and_evolve("t", &cols, ts(300)).is_none());
    }

    #[test]
    fn concurrent_first_encounter_only_one_emits() {
        // Simulate multiple partition tasks discovering the same table concurrently.
        let tracker = Arc::new(Mutex::new(SchemaTracker::new()));
        let cols = vec![col("id", TypeCode::Int64), col("name", TypeCode::String)];

        let mut handles = vec![];
        for _ in 0..10 {
            let tracker = tracker.clone();
            let cols = cols.clone();
            handles.push(std::thread::spawn(move || {
                tracker.lock().unwrap().check_and_evolve("t", &cols, ts(100))
            }));
        }

        let emit_count: usize = handles
            .into_iter()
            .map(|h| h.join().unwrap().is_some() as usize)
            .sum();
        assert_eq!(emit_count, 1, "exactly one task should emit the schema change");
    }

    #[test]
    fn column_drop_at_newer_timestamp_emits() {
        let mut tracker = SchemaTracker::new();
        let cols1 = vec![
            col("id", TypeCode::Int64),
            col("name", TypeCode::String),
            col("email", TypeCode::String),
        ];
        assert!(tracker.check_and_evolve("t", &cols1, ts(100)).is_some());

        // Column dropped at newer timestamp.
        let cols2 = vec![col("id", TypeCode::Int64), col("name", TypeCode::String)];
        assert!(tracker.check_and_evolve("t", &cols2, ts(200)).is_some());
    }

    #[test]
    fn column_type_change_at_newer_timestamp_emits() {
        let mut tracker = SchemaTracker::new();
        let cols1 = vec![col("id", TypeCode::Int64)];
        assert!(tracker.check_and_evolve("t", &cols1, ts(100)).is_some());

        // Same column name, different type.
        let cols2 = vec![col("id", TypeCode::String)];
        assert!(tracker.check_and_evolve("t", &cols2, ts(200)).is_some());
    }

    #[test]
    fn multi_round_ddl_evolution() {
        let mut tracker = SchemaTracker::new();
        let cols_a = vec![col("id", TypeCode::Int64)];
        let cols_b = vec![col("id", TypeCode::Int64), col("name", TypeCode::String)];
        let cols_c = vec![
            col("id", TypeCode::Int64),
            col("name", TypeCode::String),
            col("email", TypeCode::String),
        ];

        assert!(tracker.check_and_evolve("t", &cols_a, ts(100)).is_some());
        assert!(tracker.check_and_evolve("t", &cols_b, ts(200)).is_some());
        assert!(tracker.check_and_evolve("t", &cols_c, ts(300)).is_some());
        // Same schema repeated → no emit.
        assert!(tracker.check_and_evolve("t", &cols_c, ts(400)).is_none());
    }
}
