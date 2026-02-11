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

use async_trait::async_trait;
use google_cloud_spanner::statement::Statement;
use risingwave_common::bail;
use time::OffsetDateTime;

use crate::error::ConnectorResult;
use crate::source::SourceEnumeratorContextRef;
use crate::source::base::SplitEnumerator;
use crate::source::spanner_cdc::{SpannerCdcProperties, SpannerCdcSplit};

pub struct SpannerCdcSplitEnumerator {
    properties: SpannerCdcProperties,
}

#[async_trait]
impl SplitEnumerator for SpannerCdcSplitEnumerator {
    type Properties = SpannerCdcProperties;
    type Split = SpannerCdcSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<SpannerCdcSplitEnumerator> {
        let client = properties.create_client().await?;

        // Validate that the change stream exists.
        let stmt = Statement::new(format!(
            "SELECT 1 FROM INFORMATION_SCHEMA.CHANGE_STREAMS WHERE CHANGE_STREAM_NAME = '{}'",
            properties.stream_name
        ));
        let mut txn = client
            .single()
            .await
            .map_err(|e| anyhow::anyhow!("failed to create transaction: {}", e))?;
        let mut rows = txn
            .query(stmt)
            .await
            .map_err(|e| anyhow::anyhow!("failed to query change streams: {}", e))?;

        if rows
            .next()
            .await
            .map_err(|e| anyhow::anyhow!("failed to read row: {}", e))?
            .is_none()
        {
            bail!(
                "change stream '{}' does not exist in database '{}'",
                properties.stream_name,
                properties.database
            );
        }

        Ok(Self { properties })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<SpannerCdcSplit>> {
        // Query CURRENT_TIMESTAMP() from Spanner so the CDC start offset is from
        // the same clock domain as commit timestamps in the change stream.
        let client = self.properties.create_client().await?;
        let mut txn = client
            .single()
            .await
            .map_err(|e| anyhow::anyhow!("transaction: {}", e))?;

        let mut rows = txn
            .query(Statement::new("SELECT CURRENT_TIMESTAMP()"))
            .await
            .map_err(|e| anyhow::anyhow!("CURRENT_TIMESTAMP query: {}", e))?;

        let row = rows
            .next()
            .await
            .map_err(|e| anyhow::anyhow!("timestamp read: {}", e))?
            .ok_or_else(|| anyhow::anyhow!("CURRENT_TIMESTAMP returned no rows"))?;

        let offset: OffsetDateTime = row
            .column(0)
            .map_err(|e| anyhow::anyhow!("timestamp column: {}", e))?;

        let split = SpannerCdcSplit::new_root(
            self.properties.stream_name.clone(),
            0,
            offset,
        );

        tracing::info!(
            ?offset,
            stream = %self.properties.stream_name,
            "created root CDC split"
        );

        Ok(vec![split])
    }
}
