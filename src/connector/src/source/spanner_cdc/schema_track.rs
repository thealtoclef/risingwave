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
use std::str::FromStr;
use std::sync::Arc;

use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
use tokio::sync::RwLock;

use crate::error::ConnectorResult;
use crate::parser::schema_change::{
    SchemaChangeEnvelope, TableSchemaChange,
};
use crate::source::cdc::build_cdc_table_id;
use crate::source::spanner_cdc::types::{ColumnType, DataChangeRecord, SpannerType};
use risingwave_common::id::SourceId;

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

/// Detected schema changes
#[derive(Debug, Clone)]
pub struct SchemaChanges {
    pub added_columns: Vec<ColumnSchema>,
    pub dropped_columns: Vec<ColumnSchema>,
    pub type_changed_columns: Vec<(ColumnSchema, ColumnSchema)>, // (old, new)
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

    /// Check a data change record for schema changes
    /// Returns Some(SchemaChangeEnvelope) if schema changed, None otherwise
    pub async fn check_and_evolve(
        &self,
        record: &DataChangeRecord,
        source_id: u32,
        _source_name: &str,
    ) -> ConnectorResult<Option<SchemaChangeEnvelope>> {
        let table_name = record.table_name.clone();

        // Extract schema from column_types
        let new_schema = Self::extract_schema_from_record(record)?;

        // Get the old schema (if any)
        let old_schema = {
            let schemas = self.known_schemas.read().await;
            schemas.get(&table_name).cloned()
        };

        // If this is the first time we see this table, cache it and return no changes
        let old_schema = match old_schema {
            Some(schema) => schema,
            None => {
                let mut schemas = self.known_schemas.write().await;
                schemas.insert(table_name.clone(), new_schema.clone());
                return Ok(None);
            }
        };

        // Compare schemas
        if let Some(changes) = Self::compare_schemas(&old_schema, &new_schema) {
            // Update known schema
            {
                let mut schemas = self.known_schemas.write().await;
                schemas.insert(table_name.clone(), new_schema.clone());
            }

            // Build column catalogs from the new schema
            let column_catalogs: Vec<ColumnCatalog> = new_schema
                .columns
                .iter()
                .map(|col| Self::build_column_catalog(col))
                .collect::<Result<Vec<_>, _>>()?;

            // Create the schema change envelope
            let table_change = TableSchemaChange {
                cdc_table_id: build_cdc_table_id(SourceId::new(source_id), &table_name),
                columns: column_catalogs,
                change_type: crate::parser::schema_change::TableChangeType::Alter,
                upstream_ddl: Self::format_upstream_ddl(&table_name, &changes),
            };

            return Ok(Some(SchemaChangeEnvelope {
                table_changes: vec![table_change],
            }));
        }

        Ok(None)
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

    /// Compare two schemas and detect changes
    fn compare_schemas(old_schema: &TableSchema, new_schema: &TableSchema) -> Option<SchemaChanges> {
        // Build maps for quick lookup
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

        // Get all unique column names
        let all_names: HashSet<&str> = old_columns
            .keys()
            .chain(new_columns.keys())
            .copied()
            .collect();

        let mut added_columns = Vec::new();
        let mut dropped_columns = Vec::new();
        let mut type_changed_columns = Vec::new();

        for name in all_names {
            match (old_columns.get(name), new_columns.get(name)) {
                (Some(old_col), Some(new_col)) => {
                    // Column exists in both - check for type changes
                    if old_col.spanner_type != new_col.spanner_type {
                        type_changed_columns.push(((*old_col).clone(), (*new_col).clone()));
                    }
                }
                (None, Some(new_col)) => {
                    // Column was added
                    added_columns.push((*new_col).clone());
                }
                (Some(old_col), None) => {
                    // Column was dropped
                    dropped_columns.push((*old_col).clone());
                }
                (None, None) => {
                    // Impossible
                }
            }
        }

        if added_columns.is_empty()
            && dropped_columns.is_empty()
            && type_changed_columns.is_empty()
        {
            None
        } else {
            Some(SchemaChanges {
                added_columns,
                dropped_columns,
                type_changed_columns,
            })
        }
    }

    /// Build a ColumnDesc from a ColumnSchema
    fn build_column_schema(col: &ColumnSchema) -> ConnectorResult<ColumnDesc> {
        // Direct conversion from SpannerType to DataType
        let data_type = col.spanner_type.to_rw_type();

        Ok(ColumnDesc::named(
            col.name.clone(),
            ColumnId::placeholder(),
            data_type,
        ))
    }

    /// Build a ColumnCatalog from a ColumnSchema
    fn build_column_catalog(col: &ColumnSchema) -> ConnectorResult<ColumnCatalog> {
        let column_desc = Self::build_column_schema(col)?;
        Ok(ColumnCatalog {
            column_desc,
            is_hidden: false,
        })
    }

    /// Format the detected changes as an upstream DDL statement
    ///
    /// This shows the Spanner DDL format (e.g., "STRING", "INT64") as it was executed
    /// in the upstream Spanner database, not RisingWave's type format.
    fn format_upstream_ddl(table_name: &str, changes: &SchemaChanges) -> String {
        let mut parts = Vec::new();

        for col in &changes.added_columns {
            parts.push(format!("ADD COLUMN {} {}", col.name, col.spanner_type.to_type_string()));
        }

        for col in &changes.dropped_columns {
            parts.push(format!("DROP COLUMN {}", col.name));
        }

        for (old_col, new_col) in &changes.type_changed_columns {
            parts.push(format!(
                "ALTER COLUMN {} TYPE {} -> {}",
                old_col.name,
                old_col.spanner_type.to_type_string(),
                new_col.spanner_type.to_type_string()
            ));
        }

        format!("ALTER TABLE {} {}", table_name, parts.join(", "))
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

/// Convert a schema change envelope to JSON format for transmission.
///
/// This creates a custom Spanner CDC schema change format that can be
/// natively deserialized in the plain parser.
pub fn schema_change_to_json(envelope: &SchemaChangeEnvelope) -> ConnectorResult<Vec<u8>> {
    use serde_json::json;

    let table_changes: Vec<serde_json::Value> = envelope
        .table_changes
        .iter()
        .map(|change| {
            let change_type = match change.change_type {
                crate::parser::schema_change::TableChangeType::Alter => "ALTER",
                crate::parser::schema_change::TableChangeType::Create => "CREATE",
                crate::parser::schema_change::TableChangeType::Drop => "DROP",
                crate::parser::schema_change::TableChangeType::Unspecified => "ALTER",
            };

            let columns: Vec<serde_json::Value> = change
                .columns
                .iter()
                .map(|col| {
                    let column_desc = &col.column_desc;
                    json!({
                        "name": column_desc.name,
                        "type": column_desc.data_type.to_string(),
                    })
                })
                .collect();

            json!({
                "change_type": change_type,
                "cdc_table_id": change.cdc_table_id,
                "columns": columns,
                "upstream_ddl": change.upstream_ddl,
            })
        })
        .collect();

    let spanner_json = json!({
        "table_changes": table_changes,
    });

    serde_json::to_vec(&spanner_json)
        .map_err(|e| anyhow::anyhow!("Failed to serialize schema change JSON: {}", e).into())
}

/// Parse Spanner CDC schema change from JSON.
///
/// This is the native Spanner CDC parser that deserializes the JSON
/// created by `schema_change_to_json`.
pub fn parse_spanner_schema_change(
    data: &[u8],
) -> ConnectorResult<crate::parser::schema_change::SchemaChangeEnvelope> {
    use crate::parser::schema_change::{SchemaChangeEnvelope, TableChangeType, TableSchemaChange};
    use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
    use risingwave_common::types::DataType;

    let json: serde_json::Value = serde_json::from_slice(data)
        .map_err(|e| anyhow::anyhow!("Failed to parse schema change JSON: {}", e))?;

    let table_changes = json["table_changes"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("Missing table_changes array"))?;

    let mut result = Vec::new();

    for table_change in table_changes {
        let cdc_table_id = table_change["cdc_table_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing cdc_table_id"))?
            .to_string();

        let change_type_str = table_change["change_type"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing change_type"))?;
        let change_type = TableChangeType::from(change_type_str);

        let upstream_ddl = table_change["upstream_ddl"]
            .as_str()
            .unwrap_or("")
            .to_string();

        let columns_array = table_change["columns"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Missing columns array"))?;

        let mut columns = Vec::new();
        for col in columns_array {
            let name = col["name"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Missing column name"))?
                .to_string();

            let type_str = col["type"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Missing column type"))?;

            let data_type = DataType::from_str(type_str)
                .map_err(|e| anyhow::anyhow!("Failed to parse data type '{}': {}", type_str, e))?;

            let column_desc = ColumnDesc::named(name, ColumnId::placeholder(), data_type);
            columns.push(ColumnCatalog {
                column_desc,
                is_hidden: false,
            });
        }

        result.push(TableSchemaChange {
            cdc_table_id,
            columns,
            change_type,
            upstream_ddl,
        });
    }

    Ok(SchemaChangeEnvelope {
        table_changes: result,
    })
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

        let changes = SchemaTracker::compare_schemas(&old_schema, &new_schema).unwrap();

        assert_eq!(changes.added_columns.len(), 1);
        assert_eq!(changes.added_columns[0].name, "age");
        assert_eq!(changes.type_changed_columns.len(), 0);
        assert_eq!(changes.dropped_columns.len(), 0);
    }

    #[test]
    fn test_schema_no_change() {
        let schema = TableSchema {
            table_name: "test_table".to_string(),
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    spanner_type: SpannerType::simple(TypeCode::Int64),
                    is_primary_key: true,
                    ordinal_position: 1,
                },
            ],
        };

        let changes = SchemaTracker::compare_schemas(&schema, &schema);
        assert!(changes.is_none());
    }
}
