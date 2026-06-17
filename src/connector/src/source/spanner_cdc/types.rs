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
//! Parses change-stream records returned by the `READ_<stream>` TVF into
//! strongly-typed AST nodes. Each row contains a single `ChangeRecord`
//! column of type `ARRAY<STRUCT<...>>`; the googleapis SDK decodes it to
//! a `serde_json::Value` preserving STRUCT field names via runtime `Type`
//! metadata.
//!
//! Reference: <https://cloud.google.com/spanner/docs/change-streams/details>

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
/// See: <https://cloud.google.com/spanner/docs/reference/rest/v1/Type#TypeCode>
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpannerType {
    pub code: TypeCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub array_element_type: Option<Box<SpannerType>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub struct_type: Option<StructType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub type_annotation: Option<TypeAnnotationCode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proto_type_fqn: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructType {
    pub fields: Vec<StructField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: SpannerType,
}

impl SpannerType {
    pub fn simple(code: TypeCode) -> Self {
        Self {
            code,
            array_element_type: None,
            struct_type: None,
            type_annotation: None,
            proto_type_fqn: None,
        }
    }

    pub fn array(element_type: SpannerType) -> Self {
        Self {
            code: TypeCode::Array,
            array_element_type: Some(Box::new(element_type)),
            struct_type: None,
            type_annotation: None,
            proto_type_fqn: None,
        }
    }

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
            TypeCode::Struct => DataType::Jsonb,
            TypeCode::Proto | TypeCode::Enum => DataType::Varchar,
        }
    }
}

/// Map a Spanner type name string (as produced by `SpannerType::to_type_string()`) to a RisingWave DataType.
pub fn spanner_type_name_to_rw_type(type_name: &str) -> Option<DataType> {
    if let Some(inner) = type_name
        .strip_prefix("ARRAY<")
        .and_then(|s| s.strip_suffix('>'))
    {
        return spanner_type_name_to_rw_type(inner)
            .map(|elem_type| DataType::List(ListType::new(elem_type)));
    }
    match type_name {
        "BOOL" => Some(DataType::Boolean),
        "INT64" => Some(DataType::Int64),
        "FLOAT64" => Some(DataType::Float64),
        "FLOAT32" => Some(DataType::Float32),
        "TIMESTAMP" => Some(DataType::Timestamptz),
        "DATE" => Some(DataType::Date),
        "STRING" => Some(DataType::Varchar),
        "BYTES" => Some(DataType::Bytea),
        "NUMERIC" => Some(DataType::Decimal),
        "JSON" => Some(DataType::Jsonb),
        _ => None,
    }
}

/// Custom serializer/deserializer for `SpannerType` in `ColumnType`.
///
/// The googleapis SDK returns the `type` cell of a `column_types` row as
/// either a JSON object (when the type metadata is available) or a JSON
/// string. We accept both.
mod spanner_type_serde {
    use super::*;

    pub fn serialize<S>(spanner_type: &SpannerType, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        spanner_type.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SpannerType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = JsonValue::deserialize(deserializer)?;
        match v {
            JsonValue::Object(_) => serde_json::from_value(v).map_err(serde::de::Error::custom),
            JsonValue::String(s) => serde_json::from_str(&s).map_err(serde::de::Error::custom),
            other => Err(serde::de::Error::custom(format!(
                "expected JSON object or string for SpannerType, got: {}",
                other
            ))),
        }
    }
}

// ============================================================================
// Spanner Change Stream Data Types
// ============================================================================
//
// Change-stream TVF result row shape:
//   STRUCT<
//     data_change_record ARRAY<STRUCT<...>>,
//     heartbeat_record ARRAY<STRUCT<...>>,
//     child_partitions_record ARRAY<STRUCT<...>>
//   >
// One element per row. Only one of the three arrays is non-empty per record.
// ============================================================================

#[derive(Debug, Clone)]
pub struct ChangeStreamRecord {
    pub data_change_record: Vec<DataChangeRecord>,
    pub heartbeat_record: Vec<HeartbeatRecord>,
    pub child_partitions_record: Vec<ChildPartitionsRecord>,
}

#[derive(Debug, Clone)]
pub struct DataChangeRecord {
    pub commit_timestamp: OffsetDateTime,
    pub record_sequence: String,
    pub server_transaction_id: String,
    pub is_last_record_in_transaction_in_partition: bool,
    pub table_name: String,
    pub value_capture_type: String,
    pub column_types: Vec<ColumnType>,
    pub mods: Vec<Mod>,
    pub mod_type: String,
    pub number_of_records_in_transaction: i64,
    pub number_of_partitions_in_transaction: i64,
    pub transaction_tag: String,
    pub is_system_transaction: bool,
}

/// `column_types` row in a `data_change_record`. JSON shape:
///
/// ```json
/// {
///   "name": "column_name",
///   "type": {"code": "STRING"},
///   "is_primary_key": false,
///   "ordinal_position": 1
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnType {
    pub name: String,
    #[serde(with = "spanner_type_serde")]
    pub spanner_type: SpannerType,
    pub is_primary_key: bool,
    pub ordinal_position: i64,
}

impl ColumnType {
    pub fn type_code(&self) -> TypeCode {
        self.spanner_type.code.clone()
    }
}

/// `mods` row in a `data_change_record`. The change-stream TVF encodes all
/// cell values as JSON strings (regardless of column type). `keys`/`new_values`/
/// `old_values` are each `MAP<column_name, value>` serialized as a JSON
/// object-as-string.
///
/// `action_id`-style columns are preserved: they appear as keys in the map
/// and pass through `to_json_map` unmodified.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mod {
    pub keys: Option<String>,
    pub new_values: Option<String>,
    pub old_values: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HeartbeatRecord {
    pub timestamp: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct ChildPartitionsRecord {
    pub start_timestamp: OffsetDateTime,
    pub record_sequence: String,
    pub child_partitions: Vec<ChildPartition>,
}

#[derive(Debug, Clone)]
pub struct ChildPartition {
    pub token: String,
    pub parent_partition_tokens: Vec<String>,
}

// ============================================================================
// Parsing
// ============================================================================

/// Parse the `ChangeRecord` column of a change-stream result row.
///
/// The SDK returns `ARRAY<STRUCT<...>>` as a `JsonValue::Array` of objects
/// with at most one element per row (Spanner emits at most one record per
/// change-stream TVF row).
pub fn parse_change_record_column(
    row: &google_cloud_spanner::result::Row,
    idx: usize,
) -> anyhow::Result<Vec<ChangeStreamRecord>> {
    let json: JsonValue = row
        .try_get(idx)
        .map_err(|e| anyhow::anyhow!("failed to get ChangeRecord column: {}", e))?;
    let inner = match json {
        JsonValue::Array(mut arr) if !arr.is_empty() => arr.remove(0),
        JsonValue::Object(_) => json,
        other => {
            return Err(anyhow::anyhow!(
                "ChangeRecord is neither ARRAY<STRUCT> nor STRUCT, got: {}",
                other
            ));
        }
    };
    let obj = inner
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("ChangeRecord inner element is not an object: {}", inner))?;

    Ok(vec![ChangeStreamRecord::from_json_object(obj)?])
}

impl ChangeStreamRecord {
    fn from_json_object(obj: &serde_json::Map<String, JsonValue>) -> anyhow::Result<Self> {
        let parse_array = |key: &str| -> anyhow::Result<JsonValue> {
            Ok(obj.get(key).cloned().unwrap_or(JsonValue::Array(vec![])))
        };

        let mut data_change_record = Vec::new();
        for v in parse_array("data_change_record")?
            .as_array()
            .cloned()
            .unwrap_or_default()
        {
            data_change_record.push(DataChangeRecord::from_json(&v)?);
        }

        let mut heartbeat_record = Vec::new();
        for v in parse_array("heartbeat_record")?
            .as_array()
            .cloned()
            .unwrap_or_default()
        {
            heartbeat_record.push(HeartbeatRecord::from_json(&v)?);
        }

        let mut child_partitions_record = Vec::new();
        for v in parse_array("child_partitions_record")?
            .as_array()
            .cloned()
            .unwrap_or_default()
        {
            child_partitions_record.push(ChildPartitionsRecord::from_json(&v)?);
        }

        Ok(Self {
            data_change_record,
            heartbeat_record,
            child_partitions_record,
        })
    }
}

impl DataChangeRecord {
    fn from_json(v: &JsonValue) -> anyhow::Result<Self> {
        let obj = v
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("DataChangeRecord is not an object, got: {}", v))?;
        let get_str = |k: &str| -> anyhow::Result<String> {
            obj.get(k)
                .and_then(|v| v.as_str())
                .map(String::from)
                .ok_or_else(|| anyhow::anyhow!("DataChangeRecord.{}: missing or not a string", k))
        };
        let get_bool = |k: &str| -> anyhow::Result<bool> {
            obj.get(k)
                .and_then(|v| v.as_bool())
                .ok_or_else(|| anyhow::anyhow!("DataChangeRecord.{}: missing or not a bool", k))
        };
        let get_i64 = |k: &str| -> anyhow::Result<i64> {
            obj.get(k)
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                })
                .ok_or_else(|| anyhow::anyhow!("DataChangeRecord.{}: missing or not an i64", k))
        };

        let commit_ts_str = get_str("commit_timestamp")?;
        let commit_timestamp = OffsetDateTime::parse(
            &commit_ts_str,
            &time::format_description::well_known::Rfc3339,
        )
        .map_err(|e| anyhow::anyhow!("invalid commit_timestamp '{}': {}", commit_ts_str, e))?;

        let column_types = match obj.get("column_types") {
            Some(JsonValue::Array(a)) => a
                .iter()
                .map(ColumnType::from_json)
                .collect::<anyhow::Result<Vec<_>>>()?,
            _ => vec![],
        };
        let mods = match obj.get("mods") {
            Some(JsonValue::Array(a)) => a
                .iter()
                .map(Mod::from_json)
                .collect::<anyhow::Result<Vec<_>>>()?,
            _ => vec![],
        };

        Ok(Self {
            commit_timestamp,
            record_sequence: get_str("record_sequence")?,
            server_transaction_id: get_str("server_transaction_id")?,
            is_last_record_in_transaction_in_partition: get_bool(
                "is_last_record_in_transaction_in_partition",
            )?,
            table_name: get_str("table_name")?,
            value_capture_type: get_str("value_capture_type")?,
            column_types,
            mods,
            mod_type: get_str("mod_type")?,
            number_of_records_in_transaction: get_i64("number_of_records_in_transaction")?,
            number_of_partitions_in_transaction: get_i64("number_of_partitions_in_transaction")?,
            transaction_tag: get_str("transaction_tag")?,
            is_system_transaction: get_bool("is_system_transaction")?,
        })
    }
}

impl ColumnType {
    fn from_json(v: &JsonValue) -> anyhow::Result<Self> {
        let obj = v
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("ColumnType is not an object, got: {}", v))?;
        let get_str = |k: &str| -> anyhow::Result<String> {
            obj.get(k)
                .and_then(|v| v.as_str())
                .map(String::from)
                .ok_or_else(|| anyhow::anyhow!("ColumnType.{}: missing or not a string", k))
        };
        let get_bool = |k: &str| -> anyhow::Result<bool> {
            obj.get(k)
                .and_then(|v| v.as_bool())
                .ok_or_else(|| anyhow::anyhow!("ColumnType.{}: missing or not a bool", k))
        };
        let get_i64 = |k: &str| -> anyhow::Result<i64> {
            obj.get(k)
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                })
                .ok_or_else(|| anyhow::anyhow!("ColumnType.{}: missing or not an i64", k))
        };

        // `type` may arrive as a JSON object (TypeCode) or a JSON-string-encoded
        // TypeCode object. The Spanner SDK emits JSON objects for STRUCT-typed
        // values, but change-stream columns may also emit the type as a string.
        let spanner_type = match obj.get("type") {
            Some(JsonValue::Object(_)) => serde_json::from_value(obj.get("type").unwrap().clone())
                .map_err(|e| anyhow::anyhow!("ColumnType.type: invalid TypeCode: {}", e))?,
            Some(JsonValue::String(s)) => serde_json::from_str(s)
                .map_err(|e| anyhow::anyhow!("ColumnType.type: invalid TypeCode string: {}", e))?,
            other => {
                return Err(anyhow::anyhow!(
                    "ColumnType.type: expected JSON object or string, got: {:?}",
                    other
                ));
            }
        };

        Ok(Self {
            name: get_str("name")?,
            spanner_type,
            is_primary_key: get_bool("is_primary_key")?,
            ordinal_position: get_i64("ordinal_position")?,
        })
    }
}

impl Mod {
    fn from_json(v: &JsonValue) -> anyhow::Result<Self> {
        let obj = v
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("Mod is not an object, got: {}", v))?;
        let cell_to_string = |k: &str| -> Option<String> {
            // Cell values arrive as JSON objects (MAP<name, value>) from the
            // SDK's STRUCT decoding, or as JSON-string-encoded objects from
            // older wire formats. Preserve the existing string-of-JSON shape
            // because `to_json_map` parses it as JSON.
            match obj.get(k) {
                Some(JsonValue::String(s)) => Some(s.clone()),
                Some(v @ JsonValue::Object(_)) => Some(v.to_string()),
                Some(JsonValue::Null) | None => None,
                Some(other) => Some(other.to_string()),
            }
        };

        Ok(Self {
            keys: cell_to_string("keys"),
            new_values: cell_to_string("new_values"),
            old_values: cell_to_string("old_values"),
        })
    }
}

impl HeartbeatRecord {
    fn from_json(v: &JsonValue) -> anyhow::Result<Self> {
        let obj = v
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("HeartbeatRecord is not an object, got: {}", v))?;
        let s = obj
            .get("timestamp")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("HeartbeatRecord.timestamp: missing or not a string"))?;
        let timestamp = OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
            .map_err(|e| anyhow::anyhow!("HeartbeatRecord.timestamp: invalid RFC3339: {}", e))?;
        Ok(Self { timestamp })
    }
}

impl ChildPartitionsRecord {
    fn from_json(v: &JsonValue) -> anyhow::Result<Self> {
        let obj = v
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("ChildPartitionsRecord is not an object, got: {}", v))?;
        let get_str = |k: &str| -> anyhow::Result<String> {
            obj.get(k)
                .and_then(|v| v.as_str())
                .map(String::from)
                .ok_or_else(|| {
                    anyhow::anyhow!("ChildPartitionsRecord.{}: missing or not a string", k)
                })
        };
        let start_ts_str = get_str("start_timestamp")?;
        let start_timestamp = OffsetDateTime::parse(
            &start_ts_str,
            &time::format_description::well_known::Rfc3339,
        )
        .map_err(|e| {
            anyhow::anyhow!(
                "ChildPartitionsRecord.start_timestamp: invalid RFC3339: {}",
                e
            )
        })?;

        let child_partitions = match obj.get("child_partitions") {
            Some(JsonValue::Array(a)) => a
                .iter()
                .map(ChildPartition::from_json)
                .collect::<anyhow::Result<Vec<_>>>()?,
            _ => vec![],
        };

        Ok(Self {
            start_timestamp,
            record_sequence: get_str("record_sequence")?,
            child_partitions,
        })
    }
}

impl ChildPartition {
    fn from_json(v: &JsonValue) -> anyhow::Result<Self> {
        let obj = v
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("ChildPartition is not an object, got: {}", v))?;
        let token = obj
            .get("token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("ChildPartition.token: missing or not a string"))?
            .to_string();
        let parent_partition_tokens = match obj.get("parent_partition_tokens") {
            Some(JsonValue::Array(a)) => a
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect(),
            _ => vec![],
        };
        Ok(Self {
            token,
            parent_partition_tokens,
        })
    }
}

impl Mod {
    /// Convert `Mod` to a JSON map for the payload column.
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

        // Spanner change streams encode all values as JSON strings regardless of the
        // underlying column type. Convert numeric types (INT64, FLOAT64, etc.) back
        // to JSON numbers using the `column_types` metadata.
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

        // Merge keys (PK columns) with values (non-key columns) into a single map.
        // Spanner change streams encode ALL values as JSON strings regardless of the
        // underlying column type (INT64, FLOAT64, BOOL, etc.). We use `convert_value`
        // to restore proper JSON types based on the `column_types` metadata.
        let merge_keys_and_values =
            |keys: &Option<String>,
             values: &Option<String>|
             -> Result<serde_json::Map<String, JsonValue>, serde_json::Error> {
                let mut merged = serde_json::Map::new();

                if let Some(k) = keys {
                    let keys_map: serde_json::Map<String, JsonValue> = serde_json::from_str(k)?;
                    for (key, value) in keys_map {
                        merged.insert(key.clone(), convert_value(&key, value));
                    }
                }

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
                let has_old_values = self.old_values.as_ref().map_or(false, |v| {
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

// ============================================================================
// Helper Methods
// ============================================================================

impl DataChangeRecord {
    pub fn commit_time(&self) -> OffsetDateTime {
        self.commit_timestamp
    }
}

impl ChildPartitionsRecord {
    pub fn start_time(&self) -> OffsetDateTime {
        self.start_timestamp
    }
}

impl HeartbeatRecord {
    pub fn heartbeat_time(&self) -> OffsetDateTime {
        self.timestamp
    }
}
