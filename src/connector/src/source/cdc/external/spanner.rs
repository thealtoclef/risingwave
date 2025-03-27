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

use std::cmp::Ordering;

use anyhow::Context;
use futures::stream::BoxStream;
use risingwave_common::catalog::{ColumnDesc, Schema};
use risingwave_common::row::OwnedRow;
use serde_derive::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::cdc::external::{
    CdcOffset, CdcOffsetParseFunc, DebeziumOffset, ExternalTableConfig, ExternalTableReader,
    SchemaTableName,
};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SpannerOffset {
    // Spanner uses a token for tracking the read position in the change stream
    pub token: String,
    // Spanner also uses read_at_timestamp for tracking when the change was read
    pub read_at_timestamp: i64,
}

// In Spanner, we compare offsets based on the read_at_timestamp
impl PartialOrd for SpannerOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.read_at_timestamp.partial_cmp(&other.read_at_timestamp)
    }
}

impl SpannerOffset {
    pub fn parse_debezium_offset(offset: &str) -> ConnectorResult<Self> {
        let dbz_offset: DebeziumOffset = serde_json::from_str(offset)
            .with_context(|| format!("invalid upstream offset: {}", offset))?;

        // Extract the token for tracking read position
        let token = dbz_offset.source_offset.token.unwrap_or_default();
        
        // Default to current timestamp if not available
        let read_at_timestamp = dbz_offset
            .source_offset
            .read_at_timestamp
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

        Ok(Self {
            token,
            read_at_timestamp,
        })
    }
}

pub struct SpannerExternalTableReader {
    rw_schema: Schema,
    pk_indices: Vec<usize>,
    project_id: String,
    instance_id: String,
    database_id: String,
    change_stream_name: String,
}

impl ExternalTableReader for SpannerExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        // In a real implementation, we would fetch the current token from Spanner
        // For now, return a default offset with the current timestamp
        let spanner_offset = SpannerOffset {
            token: String::new(),
            read_at_timestamp: chrono::Utc::now().timestamp_millis(),
        };
        Ok(CdcOffset::Spanner(spanner_offset))
    }

    fn snapshot_read(
        &self,
        _table_name: SchemaTableName,
        _start_pk: Option<OwnedRow>,
        _primary_keys: Vec<String>,
        _limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        // Debezium Spanner connector doesn't support snapshot mode
        // This is consistent with the limitation noted in the Debezium docs
        Box::pin(futures::stream::empty())
    }
}

impl SpannerExternalTableReader {
    pub async fn new(
        config: ExternalTableConfig,
        rw_schema: Schema,
        pk_indices: Vec<usize>,
    ) -> ConnectorResult<Self> {
        tracing::info!(
            ?rw_schema,
            ?pk_indices,
            "create spanner external table reader"
        );

        // Extract required Spanner configuration
        // Note: For Spanner, we use standard fields in a non-standard way:
        // - database field stores the Spanner database ID
        // - host field stores the project ID
        // - port field can store the instance ID
        // - schema field can optionally store the change stream name
        let project_id = config.host.clone();
        let instance_id = config.port.clone();
        let database_id = config.database.clone();
        let change_stream_name = config.schema.clone();

        Ok(Self {
            rw_schema,
            pk_indices,
            project_id,
            instance_id,
            database_id,
            change_stream_name,
        })
    }

    pub fn get_normalized_table_name(table_name: &SchemaTableName) -> String {
        format!(
            "{}.{}",
            table_name.schema_name, table_name.table_name
        )
    }

    pub fn get_cdc_offset_parser() -> CdcOffsetParseFunc {
        Box::new(move |offset| {
            Ok(CdcOffset::Spanner(SpannerOffset::parse_debezium_offset(
                offset,
            )?))
        })
    }
}

pub struct SpannerExternalTable {
    col_descs: Vec<ColumnDesc>,
    pk_names: Vec<String>,
    project_id: String,
    instance_id: String,
    database_id: String,
    change_stream_name: String,
}

impl SpannerExternalTable {
    pub async fn connect(
        config: ExternalTableConfig,
    ) -> ConnectorResult<Self> {
        // Extract required Spanner configuration
        // Note: For Spanner, we use standard fields in a non-standard way:
        // - database field stores the Spanner database ID
        // - host field stores the project ID
        // - port field can store the instance ID
        // - schema field can optionally store the change stream name
        let project_id = config.host.clone();
        let instance_id = config.port.clone();
        let database_id = config.database.clone();
        let change_stream_name = config.schema.clone();
        
        tracing::info!(
            project_id = %project_id,
            instance_id = %instance_id,
            database_id = %database_id,
            change_stream_name = %change_stream_name,
            "connecting to Spanner"
        );
        
        // In a real implementation, we would:
        // 1. Connect to Spanner
        // 2. Retrieve column information from the schema
        // 3. Determine primary keys
        
        // For now, we'll just return empty data
        let col_descs = vec![];
        let pk_names = vec![];
        
        Ok(Self {
            col_descs,
            pk_names,
            project_id,
            instance_id,
            database_id,
            change_stream_name,
        })
    }
    
    pub fn column_descs(&self) -> &Vec<ColumnDesc> {
        &self.col_descs
    }
    
    pub fn pk_names(&self) -> &Vec<String> {
        &self.pk_names
    }
} 