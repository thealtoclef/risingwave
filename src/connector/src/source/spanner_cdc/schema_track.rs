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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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

/// Schema tracker that compares column_types from each record
pub struct SchemaTracker {
    /// Map of table_name -> known schema
    known_schemas: Arc<RwLock<HashMap<String, TableSchema>>>,
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
    /// On subsequent encounters, emits only if `compare_schemas` detects a change.
    pub async fn check_and_evolve(
        &self,
        record: &DataChangeRecord,
    ) -> ConnectorResult<Option<Vec<u8>>> {
        let table_name = record.table_name.clone();

        // Extract schema from column_types
        let new_schema = Self::extract_schema_from_record(record)?;

        // Get the old schema (if any)
        let old_schema = {
            let schemas = self.known_schemas.read().await;
            schemas.get(&table_name).cloned()
        };

        match old_schema {
            None => {
                // First encounter: always emit schema change (false positive, let meta deduplicate)
                let mut schemas = self.known_schemas.write().await;
                schemas.insert(table_name.clone(), new_schema.clone());
                let json = Self::format_as_debezium_json(&table_name, &new_schema)?;
                Ok(Some(json))
            }
            Some(old_schema) => {
                // Subsequent encounter: emit only if schema changed
                if Self::schemas_differ(&old_schema, &new_schema) {
                    let mut schemas = self.known_schemas.write().await;
                    schemas.insert(table_name.clone(), new_schema.clone());
                    let json = Self::format_as_debezium_json(&table_name, &new_schema)?;
                    Ok(Some(json))
                } else {
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
        schemas.get(table_name).cloned()
    }

    /// Manually set a schema (useful for initialization or testing)
    pub async fn set_schema(&self, schema: TableSchema) {
        let mut schemas = self.known_schemas.write().await;
        schemas.insert(schema.table_name.clone(), schema);
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
    use crate::source::spanner_cdc::types::{SpannerType, TypeCode};

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
}
