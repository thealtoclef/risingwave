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

use std::collections::HashMap;

use risingwave_pb::connector_service::{SourceType, cdc_message};
use time::OffsetDateTime;

use crate::source::base::SourceMessage;
use crate::source::cdc::DebeziumCdcMeta;
use crate::source::spanner_cdc::types::{DataChangeRecord, Mod, TypeCode};
use crate::source::{SourceMeta, SplitId};

/// The parts of a `DataChangeRecord` needed to turn each of its `Mod`s into a
/// [`SourceMessage`], borrowed rather than owned.
///
/// A `DataChangeRecord` owns its whole `mods` vector, so cloning one per modification
/// copies the entire record payload once per row it contains. Nothing downstream reads
/// `mods`, so this context deliberately excludes it and holds only borrows: it is built
/// once per record and shared by every mod.
pub struct ChangeRecordContext<'a> {
    pub database_name: &'a str,
    pub table_name: &'a str,
    pub mod_type: &'a str,
    pub commit_timestamp: OffsetDateTime,
    pub column_types: &'a HashMap<&'a str, TypeCode>,
}

impl<'a> ChangeRecordContext<'a> {
    pub fn new(
        database_name: &'a str,
        data_change: &'a DataChangeRecord,
        column_types: &'a HashMap<&'a str, TypeCode>,
    ) -> Self {
        Self {
            database_name,
            table_name: &data_change.table_name,
            mod_type: &data_change.mod_type,
            commit_timestamp: data_change.commit_time(),
            column_types,
        }
    }
}

/// Build the `SourceMessage` for a single modification of a change record.
///
/// `offset` is the partition-watermark offset computed by the reader for the whole record.
pub fn build_source_message(
    split_id: &SplitId,
    ctx: &ChangeRecordContext<'_>,
    modification: &Mod,
    offset: &str,
) -> SourceMessage {
    let source_ts_ms = (ctx.commit_timestamp.unix_timestamp_nanos() / 1_000_000) as i64;
    // Wrap in a Debezium-compatible envelope: {"payload": {"before":..,"after":..,"op":..,"source":..}}
    // The shared CDC source schema has a `payload` JSONB column; the parser extracts
    // the top-level "payload" field.  Without this wrapper the field is missing and the
    // column is padded with NULL, which causes the backfill executor to panic.
    // The `source` sub-object mirrors the Debezium source envelope so that INCLUDE TIMESTAMP,
    // INCLUDE database_name, and INCLUDE table_name columns resolve correctly when
    // parse_debezium_chunk re-parses the stored payload without row_meta.
    let mod_json = modification
        .to_json_map(ctx.mod_type, ctx.column_types)
        .and_then(|mut inner| {
            let mut source = serde_json::Map::new();
            source.insert("ts_ms".to_string(), serde_json::Value::from(source_ts_ms));
            source.insert(
                "db".to_string(),
                serde_json::Value::String(ctx.database_name.to_owned()),
            );
            source.insert(
                "table".to_string(),
                serde_json::Value::String(ctx.table_name.to_owned()),
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
        offset: offset.to_owned(),
        split_id: split_id.clone(),
        meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
            ctx.table_name.to_owned(),
            source_ts_ms,
            cdc_message::CdcMessageType::Data,
            SourceType::Unspecified,
        )),
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::util::iter_util::ZipEqFast;
    use time::OffsetDateTime;

    use super::*;
    use crate::source::cdc::CdcMessageType;
    use crate::source::spanner_cdc::types::{ColumnType, SpannerType, TypeCode};

    const DB: &str = "test-db";
    const TABLE: &str = "accounts";
    const COMMIT_UNIX: i64 = 1_700_000_000;
    const OFFSET: &str = r#"{"Spanner":{"micros":1700000000000000}}"#;

    fn column(name: &str, code: TypeCode, ordinal: i64) -> ColumnType {
        ColumnType {
            name: name.to_owned(),
            spanner_type: SpannerType::simple(code),
            is_primary_key: ordinal == 1,
            ordinal_position: ordinal,
        }
    }

    fn insert_mod(id: i64, owner: &str) -> Mod {
        Mod {
            keys: Some(format!(r#"{{"id":"{id}"}}"#)),
            new_values: Some(format!(r#"{{"owner":"{owner}"}}"#)),
            old_values: None,
        }
    }

    /// One record carrying three modifications, as Spanner emits for a
    /// multi-row transaction.
    fn multi_mod_record() -> DataChangeRecord {
        DataChangeRecord {
            commit_timestamp: OffsetDateTime::from_unix_timestamp(COMMIT_UNIX).unwrap(),
            record_sequence: "0".to_owned(),
            server_transaction_id: "txn-1".to_owned(),
            is_last_record_in_transaction_in_partition: true,
            table_name: TABLE.to_owned(),
            value_capture_type: "OLD_AND_NEW_VALUES".to_owned(),
            column_types: vec![
                column("id", TypeCode::Int64, 1),
                column("owner", TypeCode::String, 2),
            ],
            mods: vec![
                insert_mod(1, "alice"),
                insert_mod(2, "bob"),
                insert_mod(3, "carol"),
            ],
            mod_type: "INSERT".to_owned(),
            // Counts records in the transaction, not rows: this is one record.
            number_of_records_in_transaction: 1,
            number_of_partitions_in_transaction: 1,
            transaction_tag: String::new(),
            is_system_transaction: false,
        }
    }

    fn build_all(record: &DataChangeRecord) -> Vec<SourceMessage> {
        let split_id: SplitId = "0".into();
        let column_types = record.column_type_map();
        let ctx = ChangeRecordContext::new(DB, record, &column_types);
        record
            .mods
            .iter()
            .map(|m| build_source_message(&split_id, &ctx, m, OFFSET))
            .collect()
    }

    fn payload_of(msg: &SourceMessage) -> serde_json::Value {
        let bytes = msg.payload.as_ref().expect("payload must be present");
        let envelope: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        envelope["payload"].clone()
    }

    /// Every mod must produce its own message with its own row data — the
    /// per-record context is shared, the row data is not.
    #[test]
    fn test_multi_mod_record_yields_independent_messages() {
        let record = multi_mod_record();
        let msgs = build_all(&record);

        assert_eq!(msgs.len(), 3);

        let expected = [(1, "alice"), (2, "bob"), (3, "carol")];
        for (msg, (id, owner)) in msgs.iter().zip_eq_fast(expected) {
            let payload = payload_of(msg);
            assert_eq!(payload["op"], serde_json::Value::String("c".to_owned()));
            assert_eq!(payload["before"], serde_json::Value::Null);
            // INT64 key coerced to a JSON number via the shared type map.
            assert_eq!(payload["after"]["id"], serde_json::Value::from(id));
            assert_eq!(
                payload["after"]["owner"],
                serde_json::Value::String(owner.to_owned())
            );
        }
    }

    /// The fields derived from the record header must be identical across every
    /// message, and must match the record they came from.
    #[test]
    fn test_multi_mod_record_shares_record_level_fields() {
        let record = multi_mod_record();
        let msgs = build_all(&record);
        let expected_ts_ms = COMMIT_UNIX * 1_000;

        for msg in &msgs {
            let payload = payload_of(msg);
            let source = &payload["source"];
            assert_eq!(source["db"], serde_json::Value::String(DB.to_owned()));
            assert_eq!(source["table"], serde_json::Value::String(TABLE.to_owned()));
            assert_eq!(source["ts_ms"], serde_json::Value::from(expected_ts_ms));

            assert_eq!(msg.offset, OFFSET);
            assert_eq!(msg.split_id.as_ref(), "0");
            let SourceMeta::DebeziumCdc(meta) = &msg.meta else {
                panic!("expected DebeziumCdc meta");
            };
            assert_eq!(meta.full_table_name, TABLE);
            assert_eq!(meta.source_ts_ms, expected_ts_ms);
            // `CdcMessageType` derives only Clone + Debug, so match instead of compare.
            assert!(matches!(meta.msg_type, CdcMessageType::Data));
        }
    }

    /// A record whose mods carry different shapes must not leak one row's values
    /// into another — the regression a reintroduced per-record clone or a hoisted
    /// buffer would cause.
    #[test]
    fn test_mods_do_not_leak_between_messages() {
        let mut record = multi_mod_record();
        record.mods = vec![
            Mod {
                keys: Some(r#"{"id":"1"}"#.to_owned()),
                new_values: Some(r#"{"owner":"alice"}"#.to_owned()),
                old_values: None,
            },
            Mod {
                keys: Some(r#"{"id":"2"}"#.to_owned()),
                // No `owner` at all for this row.
                new_values: Some("{}".to_owned()),
                old_values: None,
            },
        ];

        let msgs = build_all(&record);
        let first = payload_of(&msgs[0]);
        let second = payload_of(&msgs[1]);

        assert_eq!(
            first["after"]["owner"],
            serde_json::Value::String("alice".to_owned())
        );
        assert_eq!(second["after"]["id"], serde_json::Value::from(2));
        assert!(
            second["after"].get("owner").is_none(),
            "second row must not inherit the first row's values, got {second}"
        );
    }
}
