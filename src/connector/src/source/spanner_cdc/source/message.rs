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

use time::OffsetDateTime;

use crate::source::base::SourceMeta;
use crate::source::cdc::CdcMessageType;
use risingwave_common::types::{DatumRef, ScalarRefImpl};
use crate::source::spanner_cdc::types::{DataChangeRecord, Mod};
use crate::source::{SourceMessage, SplitId};

/// Spanner CDC metadata keys
pub const SPANNER_CDC_COMMIT_TIMESTAMP: &str = "spanner_commit_timestamp";
pub const SPANNER_CDC_MOD_TYPE: &str = "spanner_cdc_mod_type";
pub const SPANNER_CDC_TABLE_NAME: &str = "spanner_table_name";
pub const SPANNER_CDC_SERVER_TXN_ID: &str = "spanner_cdc_server_transaction_id";
pub const SPANNER_CDC_RECORD_SEQUENCE: &str = "spanner_cdc_record_sequence";

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
        let mod_json = record
            .modification
            .to_json_map(&record.data_change.mod_type, &record.data_change)
            .and_then(|map| serde_json::to_vec(&map).map_err(Into::into))
            .map_err(|e| anyhow::anyhow!(
                "Failed to serialize Spanner change record to JSON for table '{}', mod_type='{}': {}",
                record.data_change.table_name,
                record.data_change.mod_type,
                e
            ))
            .unwrap();

        SourceMessage {
            key: None,
            payload: Some(mod_json.into()),
            offset,
            split_id: record.split_id,
            meta: SourceMeta::SpannerCdc(SpannerCdcMeta::new_data(
                commit_timestamp,
                record.data_change.mod_type.clone(),
                record.data_change.table_name.clone(),
                record.data_change.server_transaction_id.clone(),
                record.data_change.record_sequence.clone(),
            )),
        }
    }
}

/// Spanner CDC metadata
#[derive(Debug, Clone)]
pub struct SpannerCdcMeta {
    pub commit_timestamp: OffsetDateTime,
    pub mod_type: String,
    pub table_name: String,
    pub server_transaction_id: String,
    pub record_sequence: String,
    /// Message type to follow standard CDC architecture
    /// Used by PlainParser to determine if this is a data, heartbeat, or schema change message
    pub msg_type: CdcMessageType,
}

impl SpannerCdcMeta {
    /// Extract the table name for the CDC source schema.
    /// This is used by the parser to populate the `_rw_table_name` column.
    pub fn extract_table_name(&self) -> DatumRef<'_> {
        Some(ScalarRefImpl::Utf8(&self.table_name))
    }

    /// Create a new SpannerCdcMeta for data messages
    pub fn new_data(
        commit_timestamp: OffsetDateTime,
        mod_type: String,
        table_name: String,
        server_transaction_id: String,
        record_sequence: String,
    ) -> Self {
        Self {
            commit_timestamp,
            mod_type,
            table_name,
            server_transaction_id,
            record_sequence,
            msg_type: CdcMessageType::Data,
        }
    }

    /// Create a new SpannerCdcMeta for schema change messages
    pub fn new_schema_change(
        table_name: String,
        commit_timestamp: OffsetDateTime,
    ) -> Self {
        Self {
            commit_timestamp,
            mod_type: String::new(),
            table_name,
            server_transaction_id: String::new(),
            record_sequence: String::new(),
            msg_type: CdcMessageType::SchemaChange,
        }
    }
}
