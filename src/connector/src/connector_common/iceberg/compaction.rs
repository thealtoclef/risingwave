// Copyright 2025 RisingWave Labs
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

use iceberg::spec::{Literal, Struct, StructType};

use crate::sink::catalog::SinkId;

pub struct IcebergSinkCompactionUpdate {
    // runtime event information
    pub sink_id: SinkId,
    pub force_compaction: bool,
    pub dirty_partitions: Vec<Struct>,
    pub commit_sequence_number: i64,
}

/// Serialize a partition `Struct` to bytes using iceberg's own `Literal::try_into_json`.
pub fn struct_to_bytes(s: &Struct, partition_type: &StructType) -> Option<Vec<u8>> {
    let fields: Option<Vec<serde_json::Value>> = s
        .iter()
        .zip(partition_type.fields())
        .map(|(opt_lit, field)| match opt_lit {
            Some(lit) => lit.clone().try_into_json(&field.field_type).ok(),
            None => Some(serde_json::Value::Null),
        })
        .collect();

    let fields = fields?;
    serde_json::to_vec(&serde_json::Value::Array(fields)).ok()
}

/// Deserialize bytes back to a partition `Struct` using iceberg's own `Literal::try_from_json`.
pub fn bytes_to_struct(bytes: &[u8], partition_type: &StructType) -> Option<Struct> {
    let json_val: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let arr = json_val.as_array()?;

    let fields: Option<Vec<Option<Literal>>> = arr
        .iter()
        .zip(partition_type.fields())
        .map(|(json_val, field)| match Literal::try_from_json(json_val.clone(), &field.field_type) {
            Ok(lit) => Some(lit),
            Err(_) => None,
        })
        .collect();

    Some(Struct::from_iter(fields?))
}
