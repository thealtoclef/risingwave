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

//! Spanner CDC split reader with partition coordination.
//!
//! This implementation follows the Spanner change stream documentation:
//! https://docs.cloud.google.com/spanner/docs/change-streams/details
//!
//! Key features:
//! - Parent-child partition coordination (children wait for ALL parents to finish)
//! - Watermark tracking for timestamp-ordered processing
//! - Heartbeat records used to advance watermark
//! - Partition deduplication (multiple parents can return same child)
//! - Graceful shutdown with checkpoint saving

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use futures_async_stream::try_stream;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::statement::Statement;
use risingwave_common::ensure;
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

use super::TaggedChangeRecord;
use crate::error::{ConnectorError, ConnectorResult as Result};
use crate::parser::ParserConfig;
use crate::source::spanner_cdc::partition_manager::PartitionManager;
use crate::source::spanner_cdc::schema_track::{schema_change_to_json, SchemaTracker};
use crate::source::spanner_cdc::types::ChangeStreamRecord;
use crate::source::spanner_cdc::{SpannerCdcProperties, SpannerCdcSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitMetaData,
    SplitReader, SourceMeta, into_chunk_stream,
};

/// Context needed for spawning child partitions
struct ChildPartitionContext {
    client: Client,
    stream_name: String,
    table_name_filter: Option<String>,
    heartbeat_interval_ms: i64,
    end_time: Option<OffsetDateTime>,
    retry_attempts: u32,
    retry_backoff: std::time::Duration,
    retry_backoff_max_delay_ms: u64,
    retry_backoff_factor: u64,
    schema_tracker: Arc<SchemaTracker>,
    source_id: u32,
    source_name: String,
    partition_manager: Arc<PartitionManager>,
    shutdown_token: CancellationToken,
}

pub struct SpannerCdcSplitReader {
    client: Client,
    splits: Vec<SpannerCdcSplit>,
    stream_name: String,
    table_name_filter: Option<String>,
    heartbeat_interval_ms: i64,
    end_time: Option<OffsetDateTime>,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,

    // Retry configuration
    retry_attempts: u32,
    retry_backoff: std::time::Duration,
    retry_backoff_max_delay_ms: u64,
    retry_backoff_factor: u64,

    // Schema tracker for event-based schema evolution
    schema_tracker: Arc<SchemaTracker>,

    // Partition manager for coordination
    partition_manager: Arc<PartitionManager>,

    // Cancellation token for graceful shutdown
    shutdown_token: CancellationToken,
}

impl SpannerCdcSplitReader {
    #[try_stream(ok = Vec<SourceMessage>, error = ConnectorError)]
    async fn into_data_stream(self) {
        let partition_manager = self.partition_manager.clone();

        // Get source context info needed for schema changes
        let source_id = self.source_ctx.source_id.as_raw_id();
        let source_name = self.source_ctx.source_name.clone();
        let source_name_for_initial = source_name.clone();

        // Restore all splits to partition manager
        // This handles both initial startup and checkpoint recovery
        for split in &self.splits {
            let token_key = split.partition_token.clone();

            if split.is_root() {
                // For root partition, use initialize_root
                partition_manager
                    .initialize_root(split.start_timestamp, split.id())
                    .await;

                // If the split has a watermark from a previous run, update it
                if split.watermark != split.start_timestamp {
                    // Update to finished state with watermark
                    partition_manager
                        .mark_finished(&token_key, split.watermark)
                        .await;
                }
            } else {
                // For child partitions, restore them with their checkpoint state
                let is_new = partition_manager
                    .restore_child_partition(
                        split.partition_token.clone().unwrap_or_default(),
                        split.parent_partition_tokens.clone(),
                        split.start_timestamp,
                        split.watermark,
                        split.state,
                        split.id(),
                    )
                    .await;

                if !is_new {
                    // Child partition already tracked
                }
            }
        }

        let mut partition_streams = FuturesUnordered::new();
        let active_partition_tokens: Arc<Mutex<HashSet<Option<String>>>> =
            Arc::new(Mutex::new(HashSet::new()));

        // Context for spawning children
        let child_ctx = Arc::new(ChildPartitionContext {
            client: self.client.clone(),
            stream_name: self.stream_name.clone(),
            table_name_filter: self.table_name_filter.clone(),
            heartbeat_interval_ms: self.heartbeat_interval_ms,
            end_time: self.end_time,
            retry_attempts: self.retry_attempts,
            retry_backoff: self.retry_backoff,
            retry_backoff_max_delay_ms: self.retry_backoff_max_delay_ms,
            retry_backoff_factor: self.retry_backoff_factor,
            schema_tracker: self.schema_tracker.clone(),
            source_id,
            source_name,
            partition_manager: partition_manager.clone(),
            shutdown_token: self.shutdown_token.clone(),
        });

        // Start reading from initial splits
        // For normal startup: only root partitions
        // For checkpoint recovery: all restored splits (including children that weren't finished)
        for split in &self.splits {
            // Skip splits that are already finished
            if split.is_finished() {
                continue;
            }

            // For normal startup, only start root partitions
            // For checkpoint recovery, start any unfinished split
            let should_start = if split.watermark == split.start_timestamp {
                // Normal startup - only root partitions
                split.is_root()
            } else {
                // Checkpoint recovery - start any unfinished partition
                true
            };

            if !should_start {
                continue;
            }

            // Track initial partition tokens
            {
                let mut tokens = active_partition_tokens.lock().await;
                tokens.insert(split.partition_token.clone());
            }

            let client = self.client.clone();
            let stream_name = self.stream_name.clone();
            let table_name_filter = self.table_name_filter.clone();
            let heartbeat_interval_ms = self.heartbeat_interval_ms;
            let end_time = self.end_time;
            let split_id = split.id();
            let retry_attempts = self.retry_attempts;
            let retry_backoff = self.retry_backoff;
            let retry_backoff_max_delay_ms = self.retry_backoff_max_delay_ms;
            let retry_backoff_factor = self.retry_backoff_factor;
            let source_name_clone = source_name_for_initial.clone();
            let schema_tracker = self.schema_tracker.clone();
            let pm = partition_manager.clone();
            let shutdown_token = self.shutdown_token.clone();
            let split_clone = split.clone();

            partition_streams.push(tokio::spawn(async move {
                Self::read_partition(
                    client,
                    split_clone,
                    stream_name,
                    table_name_filter,
                    heartbeat_interval_ms,
                    end_time,
                    split_id,
                    retry_attempts,
                    retry_backoff,
                    retry_backoff_max_delay_ms,
                    retry_backoff_factor,
                    schema_tracker,
                    source_id,
                    source_name_clone,
                    pm,
                    shutdown_token,
                )
                .await
            }));
        }

        // Yield an initial empty chunk to signal the stream is alive
        yield Vec::new();

        // Process messages from all partitions and spawn children inline
        while let Some(result) = partition_streams.next().await {
            match result {
                Ok(Ok(messages)) => {
                    if !messages.is_empty() {
                        yield messages;
                    }
                }
                Ok(Err(e)) => {
                    tracing::error!("Error reading partition: {:?}", e);
                    return Err(e);
                }
                Err(e) => {
                    tracing::error!("Partition task panicked: {:?}", e);
                    return Err(ConnectorError::from(anyhow::anyhow!(
                        "partition task panicked: {}",
                        e
                    )));
                }
            }

            // Check for ready child partitions and spawn them
            // We do this after handling results to maintain ordering
            let ready_children = partition_manager.get_ready_children().await;

            for child_token in ready_children {
                // Check for deduplication
                let child_token_key = Some(child_token.clone());
                let should_spawn = {
                    let mut tokens = active_partition_tokens.lock().await;
                    if tokens.contains(&child_token_key) {
                        false
                    } else {
                        tokens.insert(child_token_key.clone());
                        true
                    }
                };

                if !should_spawn {
                    continue;
                }

                // Get partition info
                let Some(info) = partition_manager.get_partition(&child_token_key).await else {
                    tracing::warn!("Child partition {:?} not found in manager", child_token);
                    continue;
                };

                let client = child_ctx.client.clone();
                let stream_name = child_ctx.stream_name.clone();
                let table_name_filter = child_ctx.table_name_filter.clone();
                let heartbeat_interval_ms = child_ctx.heartbeat_interval_ms;
                let end_time = child_ctx.end_time;
                let split_id = info.split_id.clone();
                let retry_attempts = child_ctx.retry_attempts;
                let retry_backoff = child_ctx.retry_backoff;
                let retry_backoff_max_delay_ms = child_ctx.retry_backoff_max_delay_ms;
                let retry_backoff_factor = child_ctx.retry_backoff_factor;
                let source_name_clone = child_ctx.source_name.clone();
                let schema_tracker = child_ctx.schema_tracker.clone();
                let pm = child_ctx.partition_manager.clone();
                let _ctx = child_ctx.clone();

                // Mark as running
                pm.mark_running(&child_token_key).await;

                // Create child split
                let child_split = SpannerCdcSplit::new_child(
                    child_token.clone(),
                    info.parent_tokens.clone(),
                    info.start_timestamp,
                    stream_name.clone(),
                    0,
                );

                partition_streams.push(tokio::spawn(async move {
                    Self::read_partition(
                        client,
                        child_split,
                        stream_name,
                        table_name_filter,
                        heartbeat_interval_ms,
                        end_time,
                        split_id,
                        retry_attempts,
                        retry_backoff,
                        retry_backoff_max_delay_ms,
                        retry_backoff_factor,
                        schema_tracker,
                        source_id,
                        source_name_clone,
                        pm,
                        _ctx.shutdown_token.clone(),
                    )
                    .await
                }));
            }
        }

        tracing::info!("Spanner CDC reader finished");
    }

    async fn read_partition(
        client: Client,
        mut split: SpannerCdcSplit,
        stream_name: String,
        table_name_filter: Option<String>,
        heartbeat_interval_ms: i64,
        end_time: Option<OffsetDateTime>,
        split_id: crate::source::SplitId,
        retry_attempts: u32,
        retry_backoff: std::time::Duration,
        retry_backoff_max_delay_ms: u64,
        retry_backoff_factor: u64,
        schema_tracker: Arc<SchemaTracker>,
        source_id: u32,
        source_name: String,
        partition_manager: Arc<PartitionManager>,
        shutdown_token: CancellationToken,
    ) -> Result<Vec<SourceMessage>> {
        // Mark as running
        split.mark_running();
        partition_manager
            .mark_running(&split.partition_token)
            .await;

        let mut last_error = None;

        // Use watermark as start timestamp (resume point)
        let start_ts = split.watermark;

        // Build the change stream query
        let mut stmt = Statement::new(format!(
            r#"
            SELECT ChangeRecord
            FROM READ_{} (
                @start_timestamp,
                @end_timestamp,
                @partition_token,
                @heartbeat_milliseconds
            )
            "#,
            stream_name
        ));

        stmt.add_param("start_timestamp", &start_ts);
        if let Some(end_ts) = end_time {
            stmt.add_param("end_timestamp", &end_ts);
        } else {
            stmt.add_param("end_timestamp", &Option::<OffsetDateTime>::None);
        }
        if let Some(ref token) = split.partition_token {
            stmt.add_param("partition_token", token);
        } else {
            stmt.add_param("partition_token", &Option::<String>::None);
        }
        stmt.add_param("heartbeat_milliseconds", &heartbeat_interval_ms);

        // Exponential backoff retry strategy (following RisingWave's pattern)
        // Uses tokio_retry with jitter to prevent thundering herd
        // All parameters are configurable via spanner.retry_backoff_* properties
        let retry_strategy = ExponentialBackoff::from_millis(retry_backoff.as_millis() as u64)
            .max_delay(Duration::from_millis(retry_backoff_max_delay_ms))
            .factor(retry_backoff_factor)
            .take(retry_attempts as usize)
            .map(jitter); // Add randomness to prevent synchronized retries

        for (attempt_index, delay) in retry_strategy.enumerate() {
            // Check for cancellation before attempting query
            if shutdown_token.is_cancelled() {
                // Save final watermark before shutdown
                partition_manager
                    .mark_finished(&split.partition_token, split.watermark)
                    .await;
                return Ok(Vec::new());
            }

            let result = Self::execute_query_with_retry(
                &client,
                &stmt,
                &mut split,
                &split_id,
                &stream_name,
                &table_name_filter,
                schema_tracker.clone(),
                source_id,
                &source_name,
                partition_manager.clone(),
                shutdown_token.clone(),
            )
            .await;

            match result {
                Ok(messages) => {
                    // Mark partition as finished with final watermark
                    partition_manager
                        .mark_finished(&split.partition_token, split.watermark)
                        .await;

                    return Ok(messages);
                }
                Err(e) => {
                    last_error = Some(e);
                    tracing::warn!(
                        "Query failed (attempt {}/{}), retrying after {:?}: {}",
                        attempt_index + 1,
                        retry_attempts + 1,
                        delay,
                        last_error.as_ref().unwrap()
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }

        // Mark as failed
        partition_manager
            .mark_finished(&split.partition_token, split.watermark)
            .await;

        Err(last_error.unwrap())
    }

    async fn execute_query_with_retry(
        client: &Client,
        stmt: &Statement,
        split: &mut SpannerCdcSplit,
        split_id: &crate::source::SplitId,
        _stream_name: &str,
        table_name_filter: &Option<String>,
        schema_tracker: Arc<SchemaTracker>,
        source_id: u32,
        source_name: &str,
        partition_manager: Arc<PartitionManager>,
        shutdown_token: CancellationToken,
    ) -> Result<Vec<SourceMessage>> {
        let mut txn = client
            .single()
            .await
            .map_err(|e| anyhow::anyhow!("failed to create transaction: {}", e))?;
        let mut result_set = txn
            .query(stmt.clone())
            .await
            .map_err(|e| anyhow::anyhow!("failed to execute query: {}", e))?;

        let mut messages = Vec::new();

        // Process each row from the change stream
        while let Some(row) = result_set
            .next()
            .await
            .map_err(|e| anyhow::anyhow!("failed to get next row: {}", e))?
        {
            // Check for graceful shutdown
            if shutdown_token.is_cancelled() {
                // Return messages processed so far with current watermark for checkpointing
                return Ok(messages);
            }

            let change_records: Vec<ChangeStreamRecord> = row
                .column(0)
                .map_err(|e| anyhow::anyhow!("failed to get ChangeRecord column: {}", e))?;

            for record in change_records {
                // Process data change records
                for data_change in &record.data_change_record {
                    // Update watermark with commit timestamp
                    split.update_watermark(data_change.commit_time());
                    split.messages_processed += 1;

                    // Validate value capture type
                    match data_change.value_capture_type.as_str() {
                        "NEW_ROW" | "NEW_ROW_AND_OLD_VALUES" => {
                            // Supported types
                        }
                        unsupported_type => {
                            return Err(anyhow::anyhow!(
                                "Unsupported value capture type '{}'. Only NEW_ROW and NEW_ROW_AND_OLD_VALUES are supported",
                                unsupported_type
                            ).into());
                        }
                    }

                    // Check for schema changes BEFORE filtering by table name
                    // Following standard CDC architecture: emit schema change as a SourceMessage
                    match schema_tracker
                        .check_and_evolve(data_change, source_id, source_name)
                        .await
                    {
                        Ok(Some(schema_change)) => {
                            // Convert schema change to JSON format (native serialization)
                            let schema_change_json = schema_change_to_json(&schema_change)
                                .map_err(|e| anyhow::anyhow!("failed to convert schema change to JSON: {}", e))?;

                            // Emit schema change as a SourceMessage following standard CDC architecture
                            // Include full partition state for checkpointing
                            let spanner_offset = crate::source::cdc::external::spanner::SpannerOffset::with_partition(
                                split.watermark_as_micros(),
                                split.partition_token.clone(),
                                split.parent_partition_tokens.clone(),
                                split.watermark_as_micros(),
                                split.stream_name.clone(),
                                split.index,
                            );
                            let schema_change_msg = SourceMessage {
                                key: None,
                                payload: Some(schema_change_json.into()),
                                offset: serde_json::to_string(&spanner_offset)
                                    .unwrap_or_else(|_| OffsetDateTime::now_utc().unix_timestamp_nanos().to_string()),
                                split_id: split_id.clone(),
                                meta: SourceMeta::SpannerCdc(
                                    crate::source::spanner_cdc::SpannerCdcMeta::new_schema_change(
                                        data_change.table_name.clone(),
                                        data_change.commit_time(),
                                    )
                                ),
                            };

                            messages.push(schema_change_msg);
                        }
                        Err(e) => {
                            tracing::error!(
                                target: "spanner_cdc_schema_evolution",
                                error = %e,
                                "Schema change check failed for table '{}'",
                                data_change.table_name
                            );
                            return Err(e);
                        }
                        Ok(None) => {
                            // No schema change detected
                        }
                    }

                    // Filter by table name if specified
                    if let Some(filter_table) = table_name_filter {
                        if &data_change.table_name != filter_table {
                            continue;
                        }
                    }

                    for modification in &data_change.mods {
                        let tagged_record = TaggedChangeRecord {
                            split_id: split_id.clone(),
                            data_change: data_change.clone(),
                            modification: modification.clone(),
                        };

                        // Create SourceMessage with full partition state for checkpointing
                        let mut source_msg = SourceMessage::from(tagged_record);

                        // Update the offset to include full partition state for proper checkpoint/restoration
                        let spanner_offset = crate::source::cdc::external::spanner::SpannerOffset::with_partition(
                            split.watermark_as_micros(),
                            split.partition_token.clone(),
                            split.parent_partition_tokens.clone(),
                            split.watermark_as_micros(),
                            split.stream_name.clone(),
                            split.index,
                        );
                        source_msg.offset = serde_json::to_string(&spanner_offset)
                            .unwrap_or_else(|_| split.watermark_as_micros().to_string());

                        messages.push(source_msg);
                    }
                }

                // Process heartbeat records - advance watermark
                for heartbeat in &record.heartbeat_record {
                    // Update watermark with heartbeat timestamp
                    split.update_watermark(heartbeat.heartbeat_time());
                }

                // Handle child partition records
                for child_partition_record in &record.child_partitions_record {
                    let child_start_time = child_partition_record.start_time();

                    for child_partition in &child_partition_record.child_partitions {
                        let child_token = child_partition.token.clone();

                        // Add to partition manager (deduplicated internally)
                        let _is_new = partition_manager
                            .add_child_partition(
                                child_token.clone(),
                                child_partition.parent_partition_tokens.clone(),
                                child_start_time,
                                format!("{}-child", split_id as &str).into(),
                            )
                            .await;
                    }
                }
            }
        }

        Ok(messages)
    }
}

#[async_trait]
impl SplitReader for SpannerCdcSplitReader {
    type Properties = SpannerCdcProperties;
    type Split = SpannerCdcSplit;

    async fn new(
        properties: SpannerCdcProperties,
        splits: Vec<SpannerCdcSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        ensure!(
            !splits.is_empty(),
            "spanner cdc reader requires at least one split"
        );

        let client = properties.create_client().await?;
        let heartbeat_interval_ms = properties.heartbeat_interval_ms()?;
        let end_time = properties.parse_end_time()?;
        let stream_name = properties.stream_name.clone();
        let table_name_filter = properties.table_name.clone();
        let retry_attempts = properties.get_retry_attempts();
        let retry_backoff = properties.get_retry_backoff();
        let retry_backoff_max_delay_ms = properties.get_retry_backoff_max_delay_ms();
        let retry_backoff_factor = properties.get_retry_backoff_factor();

        // Create partition manager for coordination
        let partition_manager = Arc::new(PartitionManager::new());

        // Create schema tracker
        let schema_tracker = Arc::new(SchemaTracker::new());

        // Create cancellation token for graceful shutdown
        let shutdown_token = CancellationToken::new();

        Ok(Self {
            client,
            splits,
            stream_name,
            table_name_filter,
            heartbeat_interval_ms,
            end_time,
            parser_config,
            source_ctx,
            retry_attempts,
            retry_backoff,
            retry_backoff_max_delay_ms,
            retry_backoff_factor,
            schema_tracker,
            partition_manager,
            shutdown_token,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}
