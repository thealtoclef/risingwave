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

use anyhow::Context;
use async_trait::async_trait;
use google_cloud_spanner::statement::Statement;
use risingwave_common::bail;
use time::OffsetDateTime;

use crate::error::ConnectorResult;
use crate::source::SourceEnumeratorContextRef;
use crate::source::base::SplitEnumerator;
use crate::source::spanner_cdc::{SpannerCdcProperties, SpannerCdcSplit};

pub struct SpannerCdcSplitEnumerator {
    stream_name: String,
    #[allow(dead_code)] // Used for error messages
    database_name: String,
    split_count: u32,
    start_time: Option<OffsetDateTime>,
}

#[async_trait]
impl SplitEnumerator for SpannerCdcSplitEnumerator {
    type Properties = SpannerCdcProperties;
    type Split = SpannerCdcSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<SpannerCdcSplitEnumerator> {
        let split_count = properties.parallelism.unwrap_or(1);
        if split_count < 1 {
            bail!("parallelism must be >= 1");
        }

        // Note: Authentication is handled by create_client() which supports:
        // 1. emulator_host - for testing with emulator
        // 2. credentials_path - for file-based service account credentials
        // 3. credentials - for JSON string credentials
        // 4. ADC (Application Default Credentials) - if none of the above are set,
        //    Google Cloud SDK will automatically use ADC from the environment

        // Create client to validate connection
        let client = properties.create_client().await?;

        // Validate that the change stream exists by attempting a simple query
        let stmt = Statement::new(format!(
            "SELECT 1 FROM INFORMATION_SCHEMA.CHANGE_STREAMS WHERE CHANGE_STREAM_NAME = '{}'",
            properties.stream_name
        ));

        let mut txn = client
            .single()
            .await
            .map_err(|e| anyhow::anyhow!("failed to create transaction: {}", e))?;
        let mut result_set = txn
            .query(stmt)
            .await
            .map_err(|e| anyhow::anyhow!("failed to execute query: {}", e))?;

        let exists = result_set
            .next()
            .await
            .map_err(|e| anyhow::anyhow!("failed to get next row: {}", e))?
            .is_some();

        if !exists {
            bail!(
                "change stream '{}' does not exist in database '{}'",
                properties.stream_name,
                properties.database
            );
        }

        let start_time = properties
            .parse_start_time()
            .context("failed to parse start_time")?;

        Ok(Self {
            stream_name: properties.stream_name,
            database_name: properties.database,
            split_count,
            start_time,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<SpannerCdcSplit>> {
        // Use current time if start_time is not specified
        let start_timestamp = self.start_time.unwrap_or_else(OffsetDateTime::now_utc);

        // Create initial root split
        // The root partition starts with no partition token and no parents
        let root_split = SpannerCdcSplit::new_root(
            start_timestamp,
            self.stream_name.clone(),
            0, // Root split index
        );

        tracing::info!(
            "Created root Spanner CDC split: start_time={:?}, stream={}",
            start_timestamp,
            self.stream_name
        );

        Ok(vec![root_split])
    }
}
