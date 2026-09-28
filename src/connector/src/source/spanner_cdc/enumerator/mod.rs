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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use google_cloud_spanner::client::DatabaseClient;
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
    client: DatabaseClient,
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
        let stmt = Statement::builder(
            "SELECT 1 FROM INFORMATION_SCHEMA.CHANGE_STREAMS WHERE CHANGE_STREAM_NAME = @name",
        )
        .add_param("name", &properties.change_stream_name)
        .build();
        let mut rows = client
            .single_use()
            .build()
            .execute_query(stmt)
            .await
            .map_err(|e| anyhow::anyhow!("failed to query change streams: {}", e))?;

        if rows
            .next()
            .await
            .transpose()
            .map_err(|e| anyhow::anyhow!("failed to read row: {}", e))?
            .is_none()
        {
            bail!(
                "change stream '{}' does not exist in database '{}'",
                properties.change_stream_name,
                properties.database
            );
        }

        let options = fetch_change_stream_options(&client, &properties.change_stream_name).await?;
        validate_change_stream_options(&properties.change_stream_name, &options)?;

        Ok(Self {
            source_id,
            properties,
            metrics: context.metrics.clone(),
            client,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<SpannerCdcSplit>> {
        // Use start_ts from properties (user-provided or auto-generated at CREATE SOURCE).
        let start_ts = self.properties.start_ts.ok_or_else(|| {
            anyhow::anyhow!("spanner.start_timestamp must be set during CREATE SOURCE")
        })?;
        let offset = crate::source::cdc::external::spanner::micros_to_offset_datetime(start_ts)?;

        let split = SpannerCdcSplit::new_root(
            self.properties.change_stream_name.clone(),
            self.source_id.as_raw_id(),
            offset,
        );

        // Report the current Spanner timestamp as the source position metric.
        // This queries Spanner's CURRENT_TIMESTAMP() to reflect how far along the
        // change stream source is, assuming the reader always catches up by design.
        let mut rows = self
            .client
            .single_use()
            .build()
            .execute_query(Statement::builder("SELECT CURRENT_TIMESTAMP()").build())
            .await
            .map_err(|e| anyhow::anyhow!("CURRENT_TIMESTAMP query: {}", e))?;
        if let Some(row) = rows
            .next()
            .await
            .transpose()
            .map_err(|e| anyhow::anyhow!("timestamp read: {}", e))?
        {
            let now: OffsetDateTime = row
                .try_get(0)
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

/// Value capture types whose records the reader can turn into correct rows.
///
/// `NEW_ROW` and `NEW_ROW_AND_OLD_VALUES` carry every watched column in `new_values`.
/// `OLD_AND_NEW_VALUES` (Spanner's default) and `NEW_VALUES` carry only the modified
/// columns on UPDATE, so every unmodified column would be written as NULL. Their
/// `column_types` also lists only the key and modified columns, which the schema
/// tracker would read as columns being dropped.
pub(crate) const SUPPORTED_VALUE_CAPTURE_TYPES: [&str; 2] = ["NEW_ROW", "NEW_ROW_AND_OLD_VALUES"];

/// Spanner's value capture type when the option is not set.
const DEFAULT_VALUE_CAPTURE_TYPE: &str = "OLD_AND_NEW_VALUES";

/// Read the options explicitly set on a change stream, keyed by lower-case option name.
///
/// `INFORMATION_SCHEMA.CHANGE_STREAM_OPTIONS` only has rows for options that were set;
/// an absent option takes Spanner's default.
async fn fetch_change_stream_options(
    client: &DatabaseClient,
    change_stream_name: &str,
) -> ConnectorResult<HashMap<String, String>> {
    let stmt = Statement::builder(
        "SELECT OPTION_NAME, OPTION_VALUE FROM INFORMATION_SCHEMA.CHANGE_STREAM_OPTIONS \
         WHERE CHANGE_STREAM_NAME = @name",
    )
    .add_param("name", change_stream_name)
    .build();
    let mut rows = client
        .single_use()
        .build()
        .execute_query(stmt)
        .await
        .map_err(|e| anyhow::anyhow!("failed to query change stream options: {}", e))?;

    let mut options = HashMap::new();
    while let Some(row) = rows
        .next()
        .await
        .transpose()
        .map_err(|e| anyhow::anyhow!("failed to read change stream option: {}", e))?
    {
        let name: String = row
            .try_get(0)
            .map_err(|e| anyhow::anyhow!("OPTION_NAME: {}", e))?;
        let value: String = row
            .try_get(1)
            .map_err(|e| anyhow::anyhow!("OPTION_VALUE: {}", e))?;
        options.insert(name.to_ascii_lowercase(), value);
    }
    Ok(options)
}

/// Reject change streams whose records this connector would mishandle.
fn validate_change_stream_options(
    change_stream_name: &str,
    options: &HashMap<String, String>,
) -> ConnectorResult<()> {
    let value_capture_type = options
        .get("value_capture_type")
        .map(|v| normalize_option_value(v))
        .unwrap_or_else(|| DEFAULT_VALUE_CAPTURE_TYPE.to_owned());
    if !SUPPORTED_VALUE_CAPTURE_TYPES.contains(&value_capture_type.as_str()) {
        bail!(
            "change stream '{}' uses value_capture_type '{}', which is not supported: it omits \
             unmodified columns on UPDATE. Use one of {:?}, e.g. \
             ALTER CHANGE STREAM {} SET OPTIONS (value_capture_type = 'NEW_ROW')",
            change_stream_name,
            value_capture_type,
            SUPPORTED_VALUE_CAPTURE_TYPES,
            change_stream_name,
        );
    }

    // Streams in MUTABLE_KEY_RANGE mode return a different record model
    // (PartitionStart/End/Event records instead of ChildPartitionsRecord). An absent
    // option means IMMUTABLE_KEY_RANGE, the model this reader implements.
    if let Some(partition_mode) = options
        .get("partition_mode")
        .map(|v| normalize_option_value(v))
        && partition_mode != "IMMUTABLE_KEY_RANGE"
    {
        bail!(
            "change stream '{}' uses partition_mode '{}', which is not supported; only \
             IMMUTABLE_KEY_RANGE change streams can be read",
            change_stream_name,
            partition_mode,
        );
    }
    Ok(())
}

/// Option values are compared case-insensitively, as Beam's `SpannerIO` does, since the
/// stored spelling is not documented.
fn normalize_option_value(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn test_value_capture_type() {
        for capture in ["NEW_ROW", "NEW_ROW_AND_OLD_VALUES", "new_row"] {
            validate_change_stream_options("s", &options(&[("value_capture_type", capture)]))
                .unwrap_or_else(|e| panic!("{capture} should be accepted: {e}"));
        }
        for capture in ["OLD_AND_NEW_VALUES", "NEW_VALUES"] {
            let err =
                validate_change_stream_options("s", &options(&[("value_capture_type", capture)]))
                    .unwrap_err();
            assert!(err.to_string().contains(capture), "unexpected error: {err}");
        }
        // No option row means Spanner's default, `OLD_AND_NEW_VALUES`.
        let err = validate_change_stream_options("s", &options(&[])).unwrap_err();
        assert!(
            err.to_string().contains("OLD_AND_NEW_VALUES"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_partition_mode() {
        let ok = [("value_capture_type", "NEW_ROW")];
        validate_change_stream_options("s", &options(&ok)).unwrap();
        validate_change_stream_options(
            "s",
            &options(&[ok[0], ("partition_mode", "IMMUTABLE_KEY_RANGE")]),
        )
        .unwrap();

        let err = validate_change_stream_options(
            "s",
            &options(&[ok[0], ("partition_mode", "MUTABLE_KEY_RANGE")]),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("MUTABLE_KEY_RANGE"),
            "unexpected error: {err}"
        );
    }
}
