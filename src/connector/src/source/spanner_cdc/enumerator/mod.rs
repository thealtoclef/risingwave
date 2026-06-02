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

use std::sync::Arc;

use async_trait::async_trait;
use google_cloud_spanner::statement::Statement;
use risingwave_common::bail;
use risingwave_common::id::SourceId;
use time::OffsetDateTime;

use crate::error::ConnectorResult;
use crate::source::SourceEnumeratorContextRef;
use crate::source::base::SplitEnumerator;
use crate::source::monitor::metrics::EnumeratorMetrics;
use crate::source::spanner_cdc::{SpannerCdcProperties, SpannerCdcSplit};

pub struct SpannerCdcSplitEnumerator {
    source_id: SourceId,
    properties: SpannerCdcProperties,
    metrics: Arc<EnumeratorMetrics>,
}

#[async_trait]
impl SplitEnumerator for SpannerCdcSplitEnumerator {
    type Properties = SpannerCdcProperties;
    type Split = SpannerCdcSplit;

    async fn new(
        properties: Self::Properties,
        context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<SpannerCdcSplitEnumerator> {
        let source_id = context.info.source_id;
        let client = properties.create_client().await?;

        // Validate that the change stream exists.
        let mut stmt = Statement::new(
            "SELECT 1 FROM INFORMATION_SCHEMA.CHANGE_STREAMS WHERE CHANGE_STREAM_NAME = @name",
        );
        stmt.add_param("name", &properties.change_stream_name);
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
                properties.change_stream_name,
                properties.database
            );
        }

        Ok(Self {
            source_id,
            properties,
            metrics: context.metrics.clone(),
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<SpannerCdcSplit>> {
        // Use start_ts from properties (user-provided or auto-generated at CREATE SOURCE).
        let start_ts = self.properties.start_ts
            .ok_or_else(|| anyhow::anyhow!("spanner.start_timestamp must be set during CREATE SOURCE"))?;
        let offset = crate::source::cdc::external::spanner::micros_to_offset_datetime(start_ts)?;

        let split = SpannerCdcSplit::new_root(
            self.properties.change_stream_name.clone(),
            self.source_id.as_raw_id(),
            offset,
        );

        // Report the current Spanner timestamp as the source position metric.
        // This queries Spanner's CURRENT_TIMESTAMP() to reflect how far along the
        // change stream source is, assuming the reader always catches up by design.
        let client = self.properties.create_client().await?;
        let mut txn = client
            .single()
            .await
            .map_err(|e| anyhow::anyhow!("transaction: {}", e))?;
        let mut rows = txn
            .query(Statement::new("SELECT CURRENT_TIMESTAMP()"))
            .await
            .map_err(|e| anyhow::anyhow!("CURRENT_TIMESTAMP query: {}", e))?;
        if let Some(row) = rows
            .next()
            .await
            .map_err(|e| anyhow::anyhow!("timestamp read: {}", e))?
        {
            let now: OffsetDateTime = row
                .column(0)
                .map_err(|e| anyhow::anyhow!("timestamp column: {}", e))?;
            let ts_micros = (now.unix_timestamp_nanos() / 1_000) as i64;
            self.metrics
                .spanner_cdc_change_stream_timestamp
                .with_guarded_label_values(&[&self.source_id.to_string()])
                .set(ts_micros);
        }

        tracing::debug!(
            ?offset,
            change_stream = %self.properties.change_stream_name,
            "created root CDC split"
        );

        Ok(vec![split])
    }
}
