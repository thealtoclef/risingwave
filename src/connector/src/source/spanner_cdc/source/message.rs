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
    pub database_name: String,
    pub data_change: DataChangeRecord,
    pub modification: Mod,
}

impl From<TaggedChangeRecord> for SourceMessage {
    fn from(record: TaggedChangeRecord) -> Self {
        let commit_timestamp = record.data_change.commit_time();
        let source_ts_ms = (commit_timestamp.unix_timestamp_nanos() / 1_000_000) as i64;
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
            .and_then(|mut inner| {
                let mut source = serde_json::Map::new();
                source.insert("ts_ms".to_string(), serde_json::Value::from(source_ts_ms));
                source.insert(
                    "db".to_string(),
                    serde_json::Value::String(record.database_name.clone()),
                );
                source.insert(
                    "table".to_string(),
                    serde_json::Value::String(record.data_change.table_name.clone()),
                );
                inner.insert("source".to_string(), serde_json::Value::Object(source));

                let mut envelope = serde_json::Map::new();
                envelope.insert("payload".to_string(), serde_json::Value::Object(inner));
                serde_json::to_vec(&envelope).map_err(Into::into)
            })
            .expect("Spanner change record serialization to JSON should never fail");

        SourceMessage {
            key: None,
            payload: Some(mod_json.into()),
            offset,
            split_id: record.split_id,
            meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new_with_database_name(
                record.data_change.table_name.clone(),
                source_ts_ms,
                cdc_message::CdcMessageType::Data,
                SourceType::Unspecified,
                Some(record.database_name.clone()),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use risingwave_common::types::ScalarRefImpl;
    use time::OffsetDateTime;

    use super::TaggedChangeRecord;
    use crate::source::SourceMeta;
    use crate::source::base::SourceMessage;
    use crate::source::spanner_cdc::types::{ColumnType, DataChangeRecord, Mod, SpannerType, TypeCode};
    use crate::source::SplitId;

    #[test]
    fn test_spanner_message_payload_includes_debezium_source_metadata() {
        let commit_timestamp = OffsetDateTime::from_unix_timestamp(1_715_759_200).unwrap();
        let record = TaggedChangeRecord {
            split_id: SplitId::from("0"),
            database_name: "test-db".to_owned(),
            data_change: DataChangeRecord {
                commit_timestamp,
                record_sequence: "0001".to_owned(),
                server_transaction_id: "txn-1".to_owned(),
                is_last_record_in_transaction_in_partition: true,
                table_name: "orders".to_owned(),
                value_capture_type: "NEW_ROW".to_owned(),
                column_types: vec![
                    ColumnType {
                        name: "id".to_owned(),
                        spanner_type: SpannerType::simple(TypeCode::String),
                        is_primary_key: true,
                        ordinal_position: 1,
                    },
                    ColumnType {
                        name: "amount".to_owned(),
                        spanner_type: SpannerType::simple(TypeCode::Int64),
                        is_primary_key: false,
                        ordinal_position: 2,
                    },
                ],
                mods: vec![],
                mod_type: "INSERT".to_owned(),
                number_of_records_in_transaction: 1,
                number_of_partitions_in_transaction: 1,
                transaction_tag: String::new(),
                is_system_transaction: false,
            },
            modification: Mod {
                keys: Some(r#"{"id":"A-1"}"#.to_owned()),
                new_values: Some(r#"{"amount":"42"}"#.to_owned()),
                old_values: None,
            },
        };

        let source_ts_ms = (commit_timestamp.unix_timestamp_nanos() / 1_000_000) as i64;
        let message = SourceMessage::from(record);

        let payload: Value = serde_json::from_slice(message.payload.as_ref().unwrap().as_ref()).unwrap();
        assert_eq!(payload["payload"]["source"]["ts_ms"], Value::from(source_ts_ms));
        assert_eq!(payload["payload"]["source"]["db"], Value::from("test-db"));
        assert_eq!(payload["payload"]["source"]["table"], Value::from("orders"));
        assert_eq!(payload["payload"]["after"]["id"], Value::from("A-1"));
        assert_eq!(payload["payload"]["after"]["amount"], Value::from(42));

        let SourceMeta::DebeziumCdc(meta) = message.meta else {
            panic!("expected debezium cdc meta");
        };
        assert_eq!(meta.source_ts_ms, source_ts_ms);
        assert_eq!(meta.extract_database_name(), Some(ScalarRefImpl::Utf8("test-db")));
        assert_eq!(meta.extract_table_name(), Some(ScalarRefImpl::Utf8("orders")));
    }
}
