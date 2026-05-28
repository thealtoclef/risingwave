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

//! Schema evolution for Spanner CDC using embedded schema in change records.
//!
//! Spanner change streams include `column_types` metadata with every data change record.
//! This module tracks schema by comparing the `column_types` in each record with the
//! previously known schema, triggering schema evolution when differences are detected.
//!
//! This approach is event-based (not polling) and ensures schema changes are detected
//! immediately when the first data record with the new schema arrives, preventing any
//! data loss from race conditions.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use time::OffsetDateTime;
use tokio::sync::RwLock;

use crate::error::ConnectorResult;
use crate::source::spanner_cdc::types::{ColumnType, DataChangeRecord, SpannerType};

/// Schema information derived from Spanner's column_types
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<ColumnSchema>,
}

/// Column schema derived from Spanner ColumnType
///
/// This struct wraps ColumnType with additional comparison capabilities
/// for schema evolution detection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    pub name: String,
    /// The Spanner type (directly from ColumnType)
    pub spanner_type: SpannerType,
    pub is_primary_key: bool,
    pub ordinal_position: i64,
}

impl ColumnSchema {
    /// Create a ColumnSchema from Spanner's ColumnType
    pub fn from_column_type(ct: &ColumnType) -> ConnectorResult<Self> {
        Ok(Self {
            name: ct.name.clone(),
            spanner_type: ct.spanner_type.clone(),
            is_primary_key: ct.is_primary_key,
            ordinal_position: ct.ordinal_position,
        })
    }
}

/// Tracked schema with the commit-timestamp watermark of the last record that
/// advanced it. The watermark gates schema transitions so that stale records
/// from a lagging partition cannot flip the schema backwards.
#[derive(Debug, Clone)]
struct TrackedSchema {
    schema: TableSchema,
    last_commit_ts: OffsetDateTime,
}

/// Schema tracker that compares column_types from each record.
///
/// Spanner change-stream partitions are read concurrently and there is no
/// cross-partition commit-timestamp ordering. To prevent a slower partition's
/// older-schema record from reverting the tracker after a faster partition has
/// already advanced to a new schema, every stored entry carries a per-table
/// commit-timestamp watermark. Records with `commit_timestamp` strictly less
/// than the watermark are dropped without emission.
pub struct SchemaTracker {
    /// Map of table_name -> tracked schema + watermark
    known_schemas: Arc<RwLock<HashMap<String, TrackedSchema>>>,
}

impl SchemaTracker {
    /// Create a new schema tracker
    pub fn new() -> Self {
        Self {
            known_schemas: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check a data change record for schema changes.
    ///
    /// Returns `Some(json_bytes)` if a schema change should be emitted, `None` otherwise.
    ///
    /// On first encounter with a table, always emits a schema change (false positive approach,
    /// let meta deduplicate — same as Postgres).
    /// On subsequent encounters, emits only if `compare_schemas` detects a change AND the
    /// incoming record's commit timestamp is not older than the stored watermark.
    pub async fn check_and_evolve(
        &self,
        record: &DataChangeRecord,
    ) -> ConnectorResult<Option<Vec<u8>>> {
        let table_name = &record.table_name;
        let new_ts = record.commit_time();
        let new_schema = Self::extract_schema_from_record(record)?;

        // Single write lock: removes the read→write TOCTOU between partitions.
        let mut schemas = self.known_schemas.write().await;

        match schemas.entry(table_name.clone()) {
            Entry::Vacant(slot) => {
                // Format before insert so a serialization failure leaves the
                // tracker untouched (no emit, no state change).
                let json = Self::format_as_debezium_json(table_name, &new_schema)?;
                slot.insert(TrackedSchema {
                    schema: new_schema,
                    last_commit_ts: new_ts,
                });
                Ok(Some(json))
            }
            Entry::Occupied(mut slot) => {
                let tracked = slot.get_mut();
                // Strict `<` (not `<=`): Spanner guarantees two DataChangeRecords
                // for the same table at the same commit_timestamp carry identical
                // column_types, because DDL commits at its own distinct timestamp.
                // Treating equality as fresh therefore cannot cause flapping.
                if new_ts < tracked.last_commit_ts {
                    // Only log when a stale record would have reverted the schema —
                    // that's the race this gate exists to prevent. Same-schema
                    // stale records are normal under concurrent partitions and
                    // would otherwise produce constant log noise.
                    if Self::schemas_differ(&tracked.schema, &new_schema) {
                        tracing::warn!(
                            table = %table_name,
                            ?new_ts,
                            stored_ts = ?tracked.last_commit_ts,
                            "dropped stale schema record that would have reverted the tracked schema"
                        );
                    }
                    return Ok(None);
                }

                if Self::schemas_differ(&tracked.schema, &new_schema) {
                    // Format before mutating so a serialization failure leaves
                    // the tracker untouched — the next newer record retries.
                    let json = Self::format_as_debezium_json(table_name, &new_schema)?;
                    tracked.last_commit_ts = new_ts;
                    tracked.schema = new_schema;
                    Ok(Some(json))
                } else {
                    tracked.last_commit_ts = new_ts;
                    Ok(None)
                }
            }
        }
    }

    /// Format the schema as a Debezium-compatible JSON schema change message.
    ///
    /// Emits a JSON structure compatible with the Debezium schema change format so that
    /// `parse_schema_change` in `debezium.rs` can process it directly.
    fn format_as_debezium_json(
        table_name: &str,
        schema: &TableSchema,
    ) -> ConnectorResult<Vec<u8>> {
        use serde_json::json;

        let columns: Vec<serde_json::Value> = schema
            .columns
            .iter()
            .map(|col| {
                json!({
                    "name": col.name,
                    "typeName": col.spanner_type.to_type_string(),
                })
            })
            .collect();

        let payload = json!({
            "ddl": "UNKNOWN_DDL",
            "tableChanges": [{
                "id": table_name,
                "type": "ALTER",
                "table": {
                    "columns": columns,
                },
            }],
        });

        serde_json::to_vec(&payload)
            .map_err(|e| anyhow::anyhow!("Failed to serialize schema change JSON: {}", e).into())
    }

    /// Extract schema from a data change record's column_types
    fn extract_schema_from_record(record: &DataChangeRecord) -> ConnectorResult<TableSchema> {
        let columns = record
            .column_types
            .iter()
            .map(ColumnSchema::from_column_type)
            .collect::<ConnectorResult<Vec<_>>>()?;

        Ok(TableSchema {
            table_name: record.table_name.clone(),
            columns,
        })
    }

    /// Compare two schemas. Returns `true` if any column was added, dropped, or changed type.
    fn schemas_differ(old_schema: &TableSchema, new_schema: &TableSchema) -> bool {
        let old_columns: HashMap<&str, &ColumnSchema> = old_schema
            .columns
            .iter()
            .map(|col| (col.name.as_str(), col))
            .collect();

        let new_columns: HashMap<&str, &ColumnSchema> = new_schema
            .columns
            .iter()
            .map(|col| (col.name.as_str(), col))
            .collect();

        let all_names: HashSet<&str> = old_columns
            .keys()
            .chain(new_columns.keys())
            .copied()
            .collect();

        for name in all_names {
            match (old_columns.get(name), new_columns.get(name)) {
                (Some(old_col), Some(new_col)) => {
                    if old_col.spanner_type != new_col.spanner_type {
                        return true;
                    }
                }
                (None, Some(_)) | (Some(_), None) => return true,
                (None, None) => unreachable!(),
            }
        }
        false
    }

    /// Get the current known schema for a table (for testing/debugging)
    pub async fn get_schema(&self, table_name: &str) -> Option<TableSchema> {
        let schemas = self.known_schemas.read().await;
        schemas.get(table_name).map(|t| t.schema.clone())
    }

    /// Manually set a schema with its commit-timestamp watermark
    /// (useful for initialization or testing).
    pub async fn set_schema(&self, schema: TableSchema, last_commit_ts: OffsetDateTime) {
        let mut schemas = self.known_schemas.write().await;
        schemas.insert(
            schema.table_name.clone(),
            TrackedSchema {
                schema,
                last_commit_ts,
            },
        );
    }
}


impl Default for SchemaTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::spanner_cdc::types::{Mod, SpannerType, TypeCode};

    fn col(name: &str, code: TypeCode, ordinal: i64, pk: bool) -> ColumnType {
        ColumnType {
            name: name.to_string(),
            spanner_type: SpannerType::simple(code),
            is_primary_key: pk,
            ordinal_position: ordinal,
        }
    }

    fn record_at(
        table: &str,
        commit_ts: OffsetDateTime,
        column_types: Vec<ColumnType>,
    ) -> DataChangeRecord {
        DataChangeRecord {
            commit_timestamp: commit_ts,
            record_sequence: "0".to_string(),
            server_transaction_id: "txn".to_string(),
            is_last_record_in_transaction_in_partition: true,
            table_name: table.to_string(),
            value_capture_type: "NEW_ROW".to_string(),
            column_types,
            mods: Vec::<Mod>::new(),
            mod_type: "INSERT".to_string(),
            number_of_records_in_transaction: 1,
            number_of_partitions_in_transaction: 1,
            transaction_tag: String::new(),
            is_system_transaction: false,
        }
    }

    #[test]
    fn test_schema_comparison() {
        let old_schema = TableSchema {
            table_name: "test_table".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    spanner_type: SpannerType::simple(TypeCode::Int64),
                    is_primary_key: true,
                    ordinal_position: 1,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    spanner_type: SpannerType::simple(TypeCode::String),
                    is_primary_key: false,
                    ordinal_position: 2,
                },
            ],
        };

        let new_schema = TableSchema {
            table_name: "test_table".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    spanner_type: SpannerType::simple(TypeCode::Int64),
                    is_primary_key: true,
                    ordinal_position: 1,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    spanner_type: SpannerType::simple(TypeCode::String), // Same type
                    is_primary_key: false,
                    ordinal_position: 2,
                },
                ColumnSchema {
                    name: "age".to_string(),
                    spanner_type: SpannerType::simple(TypeCode::Int64), // New column
                    is_primary_key: false,
                    ordinal_position: 3,
                },
            ],
        };

        assert!(SchemaTracker::schemas_differ(&old_schema, &new_schema));
    }

    #[test]
    fn test_schema_no_change() {
        let schema = TableSchema {
            table_name: "test_table".to_string(),
            columns: vec![ColumnSchema {
                name: "id".to_string(),
                spanner_type: SpannerType::simple(TypeCode::Int64),
                is_primary_key: true,
                ordinal_position: 1,
            }],
        };

        assert!(!SchemaTracker::schemas_differ(&schema, &schema));
    }

    #[tokio::test]
    async fn test_first_encounter_still_emits() {
        let tracker = SchemaTracker::new();
        let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let record = record_at(
            "t",
            ts,
            vec![col("id", TypeCode::Int64, 1, true)],
        );

        let result = tracker.check_and_evolve(&record).await.unwrap();
        assert!(result.is_some(), "first encounter must emit a schema change");

        let stored = tracker.get_schema("t").await.unwrap();
        assert_eq!(stored.columns.len(), 1);
    }

    #[tokio::test]
    async fn test_stale_record_does_not_revert_schema() {
        let tracker = SchemaTracker::new();
        let t1 = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let t_mid = OffsetDateTime::from_unix_timestamp(1_700_000_050).unwrap();
        let t2 = OffsetDateTime::from_unix_timestamp(1_700_000_100).unwrap();

        let old_cols = vec![col("id", TypeCode::Int64, 1, true)];
        let new_cols = vec![
            col("id", TypeCode::Int64, 1, true),
            col("name", TypeCode::String, 2, false),
        ];

        // Partition A delivers the newer (post-DDL) record first.
        let r_new = record_at("t", t2, new_cols);
        let emitted_new = tracker.check_and_evolve(&r_new).await.unwrap();
        assert!(emitted_new.is_some(), "first encounter emits");

        // Partition B then delivers an older (pre-DDL) record. It must NOT
        // flip the tracker back to the old schema.
        let r_old = record_at("t", t1, old_cols.clone());
        let emitted_old = tracker.check_and_evolve(&r_old).await.unwrap();
        assert!(
            emitted_old.is_none(),
            "stale record (older commit_ts) must not emit a schema change"
        );

        let stored = tracker.get_schema("t").await.unwrap();
        assert_eq!(
            stored.columns.len(),
            2,
            "tracker should still hold the newer schema"
        );

        // Watermark invariant: a stale record must not have advanced the
        // watermark. A second record at t_mid (still < t2) must also be
        // rejected, proving the watermark is still at t2.
        let r_mid = record_at("t", t_mid, old_cols);
        let emitted_mid = tracker.check_and_evolve(&r_mid).await.unwrap();
        assert!(
            emitted_mid.is_none(),
            "watermark must still be at t2; record at t_mid (< t2) must be stale"
        );
        let stored = tracker.get_schema("t").await.unwrap();
        assert_eq!(stored.columns.len(), 2, "schema still unchanged");
    }

    #[tokio::test]
    async fn test_monotonic_advance_without_schema_change() {
        let tracker = SchemaTracker::new();
        let t1 = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let t_mid = OffsetDateTime::from_unix_timestamp(1_700_000_050).unwrap();
        let t2 = OffsetDateTime::from_unix_timestamp(1_700_000_100).unwrap();

        let cols = vec![col("id", TypeCode::Int64, 1, true)];
        let cols_changed = vec![
            col("id", TypeCode::Int64, 1, true),
            col("name", TypeCode::String, 2, false),
        ];

        let r1 = record_at("t", t1, cols.clone());
        let r2 = record_at("t", t2, cols);

        assert!(tracker.check_and_evolve(&r1).await.unwrap().is_some());
        assert!(
            tracker.check_and_evolve(&r2).await.unwrap().is_none(),
            "identical schema with newer ts must not re-emit"
        );

        // Watermark must have advanced from t1 to t2 on the same-schema r2.
        // Send a *different-schema* record at t_mid where t1 < t_mid < t2:
        //   - If watermark is t2 (correct): t_mid < t2 → dropped → None.
        //   - If watermark is still t1 (bug, advancement skipped on no-diff):
        //     t_mid >= t1 → fresh → schema differs → would emit Some.
        // Using a differing schema is what distinguishes the two cases; a
        // same-schema record at t_mid would return None either way.
        let r_mid = record_at("t", t_mid, cols_changed);
        assert!(
            tracker.check_and_evolve(&r_mid).await.unwrap().is_none(),
            "watermark must have advanced to t2; t_mid record must be stale"
        );
        let stored = tracker.get_schema("t").await.unwrap();
        assert_eq!(
            stored.columns.len(),
            1,
            "tracker must still hold the original (unchanged) schema"
        );
    }

    #[tokio::test]
    async fn test_real_schema_change_at_or_after_watermark_emits() {
        // "After" subcase: schema change with a strictly newer commit_ts.
        {
            let tracker = SchemaTracker::new();
            let t1 = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
            let t2 = OffsetDateTime::from_unix_timestamp(1_700_000_100).unwrap();

            let old_cols = vec![col("id", TypeCode::Int64, 1, true)];
            let new_cols = vec![
                col("id", TypeCode::Int64, 1, true),
                col("name", TypeCode::String, 2, false),
            ];

            let r1 = record_at("t", t1, old_cols);
            let r2 = record_at("t", t2, new_cols);

            assert!(tracker.check_and_evolve(&r1).await.unwrap().is_some());
            assert!(
                tracker.check_and_evolve(&r2).await.unwrap().is_some(),
                "real schema change with newer commit_ts must emit"
            );
            let stored = tracker.get_schema("t").await.unwrap();
            assert_eq!(stored.columns.len(), 2);
        }

        // "At" subcase: schema change at the exact same commit_ts as the
        // current watermark must still emit (strict-less-than gate, not
        // less-or-equal). This shouldn't happen with real Spanner data, but
        // the gate behavior at equality is part of the contract.
        {
            let tracker = SchemaTracker::new();
            let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();

            let old_cols = vec![col("id", TypeCode::Int64, 1, true)];
            let new_cols = vec![
                col("id", TypeCode::Int64, 1, true),
                col("name", TypeCode::String, 2, false),
            ];

            let r1 = record_at("t", ts, old_cols);
            let r2 = record_at("t", ts, new_cols);

            assert!(tracker.check_and_evolve(&r1).await.unwrap().is_some());
            assert!(
                tracker.check_and_evolve(&r2).await.unwrap().is_some(),
                "real schema change at equal commit_ts must also emit"
            );
            let stored = tracker.get_schema("t").await.unwrap();
            assert_eq!(stored.columns.len(), 2);
        }
    }
}
