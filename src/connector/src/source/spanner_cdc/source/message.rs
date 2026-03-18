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

use risingwave_pb::connector_service::{SourceType, cdc_message};

use crate::source::SourceMeta;
use crate::source::base::SourceMessage;
use crate::source::cdc::DebeziumCdcMeta;
use crate::source::spanner_cdc::types::{DataChangeRecord, Mod};
use crate::source::{SplitId};

/// Tagged change record with split information
#[derive(Debug, Clone)]
pub struct TaggedChangeRecord {
    pub split_id: SplitId,
    pub data_change: DataChangeRecord,
    pub modification: Mod,
}

impl From<TaggedChangeRecord> for SourceMessage {
    fn from(record: TaggedChangeRecord) -> Self {
        let commit_timestamp = record.data_change.commit_time();
        // The offset will be set by the reader to include full partition state
        // Default to the commit timestamp nanos
        let offset = commit_timestamp.unix_timestamp_nanos().to_string();
        // Wrap in a Debezium-compatible envelope: {"payload": {"before":..,"after":..,"op":..}}
        // The shared CDC source schema has a `payload` JSONB column; the parser extracts
        // the top-level "payload" field.  Without this wrapper the field is missing and the
        // column is padded with NULL, which causes the backfill executor to panic.
        let mod_json = record
            .modification
            .to_json_map(&record.data_change.mod_type, &record.data_change)
            .and_then(|inner| {
                let mut envelope = serde_json::Map::new();
                envelope.insert("payload".to_string(), serde_json::Value::Object(inner));
                serde_json::to_vec(&envelope).map_err(Into::into)
            })
            .expect("Spanner change record serialization to JSON should never fail");

        let source_ts_ms =
            (commit_timestamp.unix_timestamp_nanos() / 1_000_000) as i64;

        SourceMessage {
            key: None,
            payload: Some(mod_json.into()),
            offset,
            split_id: record.split_id,
            meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
                record.data_change.table_name.clone(),
                source_ts_ms,
                cdc_message::CdcMessageType::Data,
                SourceType::Unspecified,
            )),
        }
    }
}
