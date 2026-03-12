// Copyright 2022 RisingWave Labs
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

//! Spanner CDC data types
//!
//! This module defines all Spanner change stream data structures following the official
//! Spanner API specification at:
//! https://cloud.google.com/spanner/docs/change-streams/details
//!
//! The reader parses change stream data directly into these structs, providing a
//! strongly-typed interface that matches Spanner's API exactly.

use google_cloud_spanner::row::{Error as RowError, Struct, TryFromStruct};
use parse_display::{Display, FromStr};
use risingwave_common::types::{DataType, ListType};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value as JsonValue;
use time::OffsetDateTime;

// ============================================================================
// Spanner Type System (matches Spanner REST API v1)
// ============================================================================

/// TypeCode represents the type code for a Spanner type.
///
/// See: https://cloud.google.com/spanner/docs/reference/rest/v1/Type#TypeCode
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Display, FromStr)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TypeCode {
    #[serde(alias = "TYPE_CODE_UNSPECIFIED")]
    #[display("STRING")]
    TypeCodeUnspecified,
    #[display("BOOL")]
    Bool,
    #[display("INT64")]
    Int64,
    #[display("FLOAT64")]
    Float64,
    #[display("FLOAT32")]
    Float32,
    #[display("TIMESTAMP")]
    Timestamp,
    #[display("DATE")]
    Date,
    #[display("STRING")]
    String,
    #[display("BYTES")]
    Bytes,
    #[display("ARRAY")]
    Array,
    #[display("STRUCT")]
    Struct,
    #[display("NUMERIC")]
    Numeric,
    #[display("JSON")]
    Json,
    #[display("PROTO")]
    Proto,
    #[display("ENUM")]
    Enum,
}

/// TypeAnnotationCode disambiguates SQL types for Spanner values.
///
/// See: https://cloud.google.com/spanner/docs/reference/rest/v1/Type#TypeAnnotationCode
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TypeAnnotationCode {
    #[serde(alias = "TYPE_ANNOTATION_CODE_UNSPECIFIED")]
    TypeAnnotationCodeUnspecified,
    PgNumeric,
    PgJsonb,
    PgOid,
}

/// Spanner Type represents the type of a Cloud Spanner value.
///
/// See: https://cloud.google.com/spanner/docs/reference/rest/v1/Type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpannerType {
    /// Required. The TypeCode for this type
    pub code: TypeCode,
    /// If code == ARRAY, then arrayElementType is the type of the array elements
    #[serde(skip_serializing_if = "Option::is_none")]
    pub array_element_type: Option<Box<SpannerType>>,
    /// If code == STRUCT, then structType provides type information for the struct's fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub struct_type: Option<StructType>,
    /// The TypeAnnotationCode that disambiguates SQL type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_annotation: Option<TypeAnnotationCode>,
    /// If code == PROTO or code == ENUM, then protoTypeFqn is the fully qualified name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proto_type_fqn: Option<String>,
}

/// StructType provides type information for STRUCT types.
///
/// See: https://cloud.google.com/spanner/docs/reference/rest/v1/Type#StructType
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructType {
    /// The fields of the struct
    pub fields: Vec<StructField>,
}

/// A field in a STRUCT type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructField {
    /// The field name
    pub name: String,
    /// The field type
    #[serde(rename = "type")]
    pub field_type: SpannerType,
}

impl SpannerType {
    /// Create a simple type from a TypeCode
    pub fn simple(code: TypeCode) -> Self {
        Self {
            code,
            array_element_type: None,
            struct_type: None,
            type_annotation: None,
            proto_type_fqn: None,
        }
    }

    /// Create an ARRAY type with the given element type
    pub fn array(element_type: SpannerType) -> Self {
        Self {
            code: TypeCode::Array,
            array_element_type: Some(Box::new(element_type)),
            struct_type: None,
            type_annotation: None,
            proto_type_fqn: None,
        }
    }

    /// Get a string representation of this type for use in DDL/upstream formatting
    ///
    /// This returns the Spanner type name as a string (e.g., "STRING", "INT64", "ARRAY<STRING>")
    /// which matches the format used in Spanner DDL statements.
    pub fn to_type_string(&self) -> String {
        match &self.code {
            TypeCode::Array => {
                if let Some(element_type) = &self.array_element_type {
                    format!("ARRAY<{}>", element_type.to_type_string())
                } else {
                    "ARRAY<STRING>".to_string()
                }
            }
            TypeCode::Struct => {
                if let Some(struct_type) = &self.struct_type {
                    let fields: Vec<String> = struct_type
                        .fields
                        .iter()
                        .map(|f| format!("{} {}", f.name, f.field_type.to_type_string()))
                        .collect();
                    format!("STRUCT<{}>", fields.join(", "))
                } else {
                    "STRUCT".to_string()
                }
            }
            _ => format!("{}", self.code),
        }
    }

    /// Convert SpannerType to RisingWave DataType.
    ///
    /// This handles complex types (Array, Struct) properly by recursively converting.
    pub fn to_rw_type(&self) -> DataType {
        match &self.code {
            TypeCode::Bool => DataType::Boolean,
            TypeCode::Int64 => DataType::Int64,
            TypeCode::Float64 => DataType::Float64,
            TypeCode::Float32 => DataType::Float32,
            TypeCode::Timestamp => DataType::Timestamptz,
            TypeCode::Date => DataType::Date,
            TypeCode::String => DataType::Varchar,
            TypeCode::Bytes => DataType::Bytea,
            TypeCode::Numeric => DataType::Decimal,
            TypeCode::Json => DataType::Jsonb,
            TypeCode::TypeCodeUnspecified => DataType::Varchar,
            TypeCode::Array => {
                if let Some(element_type) = &self.array_element_type {
                    DataType::List(ListType::new(element_type.to_rw_type()))
                } else {
                    DataType::List(ListType::new(DataType::Varchar))
                }
            }
            TypeCode::Struct => {
                // Struct types - for now map to JSONB as a fallback
                // In the future, we might want to create proper StructType
                DataType::Jsonb
            }
            TypeCode::Proto | TypeCode::Enum => DataType::Varchar,
        }
    }
}

/// Custom serializer/deserializer for SpannerType in ColumnType
///
/// The Google Cloud Spanner library returns the type field as a JSON string,
/// but we want to work with SpannerType directly. This module handles the conversion.
mod spanner_type_serde {
    use super::*;

    pub fn serialize<S>(spanner_type: &SpannerType, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize SpannerType directly as a JSON object
        spanner_type.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SpannerType, D::Error>
    where
        D: Deserializer<'de>,
    {
        // The Spanner library returns a JSON string, deserialize it first then parse to SpannerType
        let json_string = String::deserialize(deserializer)?;
        serde_json::from_str(&json_string).map_err(serde::de::Error::custom)
    }
}

// ============================================================================
// Spanner Change Stream Data Types
// ============================================================================
// Documentation: https://cloud.google.com/spanner/docs/change-streams/details
//
// Change stream records are returned as:
// STRUCT<
//   data_change_record ARRAY<STRUCT<...>>,
//   heartbeat_record ARRAY<STRUCT<...>>,
//   child_partitions_record ARRAY<STRUCT<...>>
// >
// Only one of these three fields contains data in any given record.
// ============================================================================

/// Spanner change stream record
///
/// This is the top-level record returned by change stream queries.
/// Only one of the three record arrays will be non-empty in any given record.
#[derive(Debug, Clone)]
pub struct ChangeStreamRecord {
    pub data_change_record: Vec<DataChangeRecord>,
    pub heartbeat_record: Vec<HeartbeatRecord>,
    pub child_partitions_record: Vec<ChildPartitionsRecord>,
}

impl TryFromStruct for ChangeStreamRecord {
    fn try_from_struct(s: Struct<'_>) -> Result<Self, RowError> {
        Ok(Self {
            data_change_record: s.column_by_name("data_change_record")?,
            heartbeat_record: s.column_by_name("heartbeat_record").unwrap_or_default(),
            child_partitions_record: s.column_by_name("child_partitions_record")?,
        })
    }
}

/// Data change record representing a change to a table
///
/// Contains all changes to a table with the same modification type (insert, update, or delete)
/// committed at the same commit timestamp in one change stream partition for the same transaction.
///
/// See: https://cloud.google.com/spanner/docs/change-streams/details#data_change_records
#[derive(Debug, Clone)]
pub struct DataChangeRecord {
    /// Indicates the timestamp in which the change was committed
    pub commit_timestamp: OffsetDateTime,
    /// Sequence number for the record within the transaction
    /// Unique and monotonically increasing (but not necessarily contiguous) within a transaction
    pub record_sequence: String,
    /// Globally unique string representing the transaction
    pub server_transaction_id: String,
    /// Whether this is the last record for a transaction in the current partition
    pub is_last_record_in_transaction_in_partition: bool,
    /// Name of the table affected by the change
    pub table_name: String,
    /// Describes the value capture type specified in the change stream configuration
    /// One of: OLD_AND_NEW_VALUES, NEW_ROW, NEW_VALUES, NEW_ROW_AND_OLD_VALUES
    pub value_capture_type: String,
    /// Column types for this table - critical for schema evolution
    pub column_types: Vec<ColumnType>,
    /// The actual data changes (keys, old values, new values)
    pub mods: Vec<Mod>,
    /// Type of change: INSERT, UPDATE, or DELETE
    pub mod_type: String,
    /// Number of data change records in this transaction across all partitions
    pub number_of_records_in_transaction: i64,
    /// Number of partitions returning data for this transaction
    pub number_of_partitions_in_transaction: i64,
    /// Transaction tag associated with this transaction
    pub transaction_tag: String,
    /// Whether this is a system transaction
    pub is_system_transaction: bool,
}

impl TryFromStruct for DataChangeRecord {
    fn try_from_struct(s: Struct<'_>) -> Result<Self, RowError> {
        Ok(Self {
            commit_timestamp: s.column_by_name("commit_timestamp")?,
            record_sequence: s.column_by_name("record_sequence")?,
            server_transaction_id: s.column_by_name("server_transaction_id")?,
            is_last_record_in_transaction_in_partition: s
                .column_by_name("is_last_record_in_transaction_in_partition")?,
            table_name: s.column_by_name("table_name")?,
            value_capture_type: s.column_by_name("value_capture_type")?,
            column_types: s.column_by_name("column_types")?,
            mods: s.column_by_name("mods")?,
            mod_type: s.column_by_name("mod_type")?,
            number_of_records_in_transaction: s
                .column_by_name("number_of_records_in_transaction")?,
            number_of_partitions_in_transaction: s
                .column_by_name("number_of_partitions_in_transaction")?,
            transaction_tag: s.column_by_name("transaction_tag")?,
            is_system_transaction: s.column_by_name("is_system_transaction")?,
        })
    }
}

/// Column type information from Spanner change stream column_types array
///
/// This struct matches the Spanner API specification exactly:
/// https://cloud.google.com/spanner/docs/change-streams/details#data_change_records
///
/// The column_types array contains:
/// ```json
/// [
///   {
///     "name": "column_name",
///     "type": {
///       "code": "STRING"
///     },
///     "is_primary_key": false,
///     "ordinal_position": 1
///   }
/// ]
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnType {
    /// Name of the column
    pub name: String,
    /// The column type as a SpannerType struct
    ///
    /// Uses custom serde to handle JSON string <-> JSON object conversion
    /// since the Spanner library returns this as a JSON string.
    #[serde(with = "spanner_type_serde")]
    pub spanner_type: SpannerType,
    /// Whether this column is a primary key
    pub is_primary_key: bool,
    /// Position of the column as defined in the schema (1-indexed)
    pub ordinal_position: i64,
}

impl ColumnType {
    /// Get the type code for this column
    pub fn type_code(&self) -> TypeCode {
        self.spanner_type.code.clone()
    }
}

impl TryFromStruct for ColumnType {
    fn try_from_struct(s: Struct<'_>) -> Result<Self, RowError> {
        // Read the type field as a JSON string, then deserialize to SpannerType
        let type_json: String = s.column_by_name("type")?;
        let spanner_type: SpannerType = serde_json::from_str(&type_json)
            .map_err(|e| RowError::CustomParseError(format!("Failed to parse SpannerType: {}", e)))?;

        Ok(Self {
            name: s.column_by_name("name")?,
            spanner_type,
            is_primary_key: s.column_by_name("is_primary_key")?,
            ordinal_position: s.column_by_name("ordinal_position")?,
        })
    }
}

/// Modification record containing keys and values
///
/// Describes the changes made, including primary key values, old values, and new values.
/// The availability and content of old and new values depends on value_capture_type.
///
/// IMPORTANT: According to Spanner change stream documentation:
/// - `keys` contains the primary key values
/// - `new_values` and `old_values` only contain NON-KEY columns
/// - We MUST merge keys with new_values/old_values to get the full row
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mod {
    /// Primary key values as JSON string
    pub keys: Option<String>,
    /// Non-key column new values as JSON string
    pub new_values: Option<String>,
    /// Non-key column old values as JSON string
    pub old_values: Option<String>,
}

impl TryFromStruct for Mod {
    fn try_from_struct(s: Struct<'_>) -> Result<Self, RowError> {
        Ok(Self {
            keys: s.column_by_name("keys").ok(),
            new_values: s.column_by_name("new_values").ok(),
            old_values: s.column_by_name("old_values").ok(),
        })
    }
}

impl Mod {
    /// Convert Mod to a JSON map for the payload column.
    ///
    /// The payload column uses Debezium envelope format with:
    /// - `before`: old values (null for INSERT, populated for UPDATE/DELETE)
    /// - `after`: new values (populated for INSERT/UPDATE, null for DELETE)
    /// - `op`: operation type ('c' for create, 'u' for update, 'd' for delete)
    ///
    /// Note: `_rw_offset` and `_rw_table_name` are separate SOURCE columns,
    /// not part of the payload JSON.
    ///
    /// The `data_change_record` parameter provides column type information to correctly
    /// convert values (e.g., keep zipcode "02101" as string, not convert to number 2101).
    pub fn to_json_map(
        &self,
        mod_type: &str,
        data_change_record: &DataChangeRecord,
    ) -> Result<serde_json::Map<String, JsonValue>, serde_json::Error> {
        let mut result = serde_json::Map::new();

        // Build a map of column name to type code for type-aware conversion
        let column_types: std::collections::HashMap<String, TypeCode> = data_change_record
            .column_types
            .iter()
            .map(|ct| (ct.name.clone(), ct.type_code()))
            .collect();

        // Helper to convert string values to their proper types based on column type.
        // Spanner returns all values as JSON strings, but we need numbers for numeric columns.
        // We use the actual column type to determine conversion, not heuristics.
        let convert_value = |column_name: &str, value: JsonValue| -> JsonValue {
            match value {
                JsonValue::String(s) => {
                    // Check the actual column type
                    let should_convert = column_types
                        .get(column_name)
                        .map(|type_code| {
                            matches!(
                                type_code,
                                TypeCode::Int64
                                    | TypeCode::Float64
                                    | TypeCode::Float32
                                    | TypeCode::Numeric
                            )
                        })
                        .unwrap_or(false);

                    if should_convert {
                        // Check if it's a pure numeric string (integer or float)
                        let is_numeric = s.chars().all(|c: char| {
                            c.is_ascii_digit()
                                || c == '-'
                                || c == '+'
                                || c == '.'
                                || c == 'e'
                                || c == 'E'
                        });
                        // Also check length to avoid converting long IDs (like timestamps)
                        let is_short = s.len() <= 20;

                        if is_numeric && is_short {
                            // Try to parse as integer first
                            if let Ok(n) = s.parse::<i64>() {
                                return JsonValue::Number(serde_json::Number::from(n));
                            }
                            // Try to parse as float
                            if let Ok(f) = s.parse::<f64>() {
                                if let Some(n) = serde_json::Number::from_f64(f) {
                                    return JsonValue::Number(n);
                                }
                            }
                        }
                    }
                    // Keep as string (either not numeric column, or parsing failed)
                    JsonValue::String(s)
                }
                _ => value,
            }
        };

        // Helper to merge keys with values (new_values or old_values)
        // According to Spanner docs, keys contain primary key columns,
        // while new_values/old_values only contain non-key columns
        let merge_keys_and_values = |keys: &Option<String>,
                                     values: &Option<String>|
         -> Result<serde_json::Map<String, JsonValue>, serde_json::Error> {
            let mut merged = serde_json::Map::new();

            // First, add keys (primary key columns) - keep as strings, no conversion
            if let Some(k) = keys {
                let keys_map: serde_json::Map<String, JsonValue> = serde_json::from_str(k)?;
                for (key, value) in keys_map {
                    // Don't convert key values - keep them as-is strings
                    merged.insert(key, value);
                }
            }

            // Then, add non-key columns from values - convert based on column type
            if let Some(v) = values {
                let values_map: serde_json::Map<String, JsonValue> = serde_json::from_str(v)?;
                for (key, value) in values_map {
                    merged.insert(key.clone(), convert_value(&key, value));
                }
            }

            Ok(merged)
        };

        // Parse before and after values based on operation type
        let (before, after, op) = match mod_type {
            "INSERT" => {
                // INSERT: before is null, after has keys + new_values
                let after_data = merge_keys_and_values(&self.keys, &self.new_values)?;
                (JsonValue::Null, JsonValue::Object(after_data), "c")
            }
            "UPDATE" => {
                // UPDATE: before has keys + old_values (if available), after has keys + new_values
                let after_data = merge_keys_and_values(&self.keys, &self.new_values)?;

                // For NEW_ROW capture type, old_values is None, empty string, or empty JSON object.
                // In this case, set before to null AND change op to "c" (CREATE) to ensure
                // the DebeziumParser treats this as an INSERT operation, not an UPDATE with retract.
                let has_old_values = self.old_values.as_ref()
                    .map_or(false, |v| {
                        // Check if the JSON string is non-empty and not just "{}"
                        !v.is_empty() && v != "{}"
                    });

                let (before_data, op_val) = if has_old_values {
                    let merged = merge_keys_and_values(&self.keys, &self.old_values)?;
                    (JsonValue::Object(merged), "u")
                } else {
                    // NEW_ROW capture type: no old values, treat as INSERT
                    (JsonValue::Null, "c")
                };

                (before_data, JsonValue::Object(after_data), op_val)
            }
            "DELETE" => {
                // DELETE: before has keys + old_values (or just keys if old_values not available), after is null
                let before_data = merge_keys_and_values(&self.keys, &self.old_values)?;
                (JsonValue::Object(before_data), JsonValue::Null, "d")
            }
            _ => {
                // Unknown operation type - treat as INSERT
                let data = merge_keys_and_values(&self.keys, &self.new_values)?;
                (JsonValue::Null, JsonValue::Object(data), "c")
            }
        };

        result.insert("before".to_string(), before);
        result.insert("after".to_string(), after);
        result.insert("op".to_string(), JsonValue::String(String::from(op)));

        Ok(result)
    }
}

/// Heartbeat record for partition health monitoring
///
/// Indicates that all changes with commit_timestamp less than or equal to
/// the heartbeat timestamp have been returned. Used to synchronize readers
/// across all partitions.
///
/// See: https://cloud.google.com/spanner/docs/change-streams/details#heartbeat_records
#[derive(Debug, Clone)]
pub struct HeartbeatRecord {
    /// The heartbeat timestamp
    pub timestamp: OffsetDateTime,
}

impl TryFromStruct for HeartbeatRecord {
    fn try_from_struct(s: Struct<'_>) -> Result<Self, RowError> {
        Ok(Self {
            timestamp: s.column_by_name("timestamp")?,
        })
    }
}

/// Child partitions record for partition splits
///
/// Returns information about child partitions: their partition tokens,
/// the tokens of their parent partitions, and the start_timestamp.
///
/// See: https://cloud.google.com/spanner/docs/change-streams/details#child_partition_records
#[derive(Debug, Clone)]
pub struct ChildPartitionsRecord {
    /// Indicates that data change records returned from child partitions
    /// have a commit timestamp greater than or equal to start_timestamp
    pub start_timestamp: OffsetDateTime,
    /// Monotonically increasing sequence number for ordering
    /// when multiple child partition records have the same start_timestamp
    pub record_sequence: String,
    /// Set of child partitions and their information
    pub child_partitions: Vec<ChildPartition>,
}

impl TryFromStruct for ChildPartitionsRecord {
    fn try_from_struct(s: Struct<'_>) -> Result<Self, RowError> {
        Ok(Self {
            start_timestamp: s.column_by_name("start_timestamp")?,
            record_sequence: s.column_by_name("record_sequence")?,
            child_partitions: s.column_by_name("child_partitions")?,
        })
    }
}

/// Child partition information
///
/// Contains the partition token string used to identify the child partition
/// in queries, as well as the tokens of its parent partitions.
#[derive(Debug, Clone)]
pub struct ChildPartition {
    /// Partition token string used to identify this child partition in queries
    pub token: String,
    /// Tokens of this partition's parent partitions
    pub parent_partition_tokens: Vec<String>,
}

impl TryFromStruct for ChildPartition {
    fn try_from_struct(s: Struct<'_>) -> Result<Self, RowError> {
        Ok(Self {
            token: s.column_by_name("token")?,
            parent_partition_tokens: s.column_by_name("parent_partition_tokens")?,
        })
    }
}

// ============================================================================
// Helper Methods
// ============================================================================

impl DataChangeRecord {
    /// Get commit timestamp
    pub fn commit_time(&self) -> OffsetDateTime {
        self.commit_timestamp
    }
}

impl ChildPartitionsRecord {
    /// Get start timestamp
    pub fn start_time(&self) -> OffsetDateTime {
        self.start_timestamp
    }
}

impl HeartbeatRecord {
    /// Get heartbeat timestamp
    pub fn heartbeat_time(&self) -> OffsetDateTime {
        self.timestamp
    }
}
