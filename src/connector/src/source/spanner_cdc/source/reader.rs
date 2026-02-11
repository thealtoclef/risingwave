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

//! Spanner CDC split reader with partition coordination.
//!
//! Implements the Spanner change stream query model:
//! <https://cloud.google.com/spanner/docs/change-streams/details>
//!
//! Architecture:
//! - Each partition is a `tokio::spawn` task executing a change stream query.
//! - Tasks send [`SourceMessage`]s through an mpsc channel for immediate delivery
//!   (Spanner streams with `end_timestamp = NULL` run indefinitely).
//! - The main loop (`into_data_stream`) uses `futures::future::select` to concurrently
//!   yield messages and spawn child partitions when parents finish.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use futures::future::Either;
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
    BackfillInfo, BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId,
    SplitMetaData, SplitReader, SourceMeta, into_chunk_stream,
};

/// Shared context for spawning partition tasks (avoids capturing `self`).
struct PartitionContext {
    client: Client,
    stream_name: String,
    table_name_filter: Option<String>,
    heartbeat_interval_ms: i64,
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
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,

    retry_attempts: u32,
    retry_backoff: std::time::Duration,
    retry_backoff_max_delay_ms: u64,
    retry_backoff_factor: u64,

    schema_tracker: Arc<SchemaTracker>,
    partition_manager: Arc<PartitionManager>,
    shutdown_token: CancellationToken,
}

impl SpannerCdcSplitReader {
    /// Spawn a partition task that reads a change stream query and sends messages
    /// through `msg_tx`. Returns a `JoinHandle` that resolves when the query ends.
    fn spawn_partition_task(
        ctx: &Arc<PartitionContext>,
        split: SpannerCdcSplit,
        split_id: SplitId,
        msg_tx: tokio::sync::mpsc::UnboundedSender<Vec<SourceMessage>>,
        partition_streams: &mut FuturesUnordered<tokio::task::JoinHandle<Result<()>>>,
    ) {
        let client = ctx.client.clone();
        let stream_name = ctx.stream_name.clone();
        let table_name_filter = ctx.table_name_filter.clone();
        let heartbeat_interval_ms = ctx.heartbeat_interval_ms;
        let retry_attempts = ctx.retry_attempts;
        let retry_backoff = ctx.retry_backoff;
        let retry_backoff_max_delay_ms = ctx.retry_backoff_max_delay_ms;
        let retry_backoff_factor = ctx.retry_backoff_factor;
        let schema_tracker = ctx.schema_tracker.clone();
        let source_id = ctx.source_id;
        let source_name = ctx.source_name.clone();
        let pm = ctx.partition_manager.clone();
        let shutdown_token = ctx.shutdown_token.clone();

        partition_streams.push(tokio::spawn(async move {
            Self::read_partition(
                client,
                split,
                stream_name,
                table_name_filter,
                heartbeat_interval_ms,
                split_id,
                retry_attempts,
                retry_backoff,
                retry_backoff_max_delay_ms,
                retry_backoff_factor,
                schema_tracker,
                source_id,
                source_name,
                pm,
                shutdown_token,
                msg_tx,
            )
            .await
        }));
    }

    #[try_stream(ok = Vec<SourceMessage>, error = ConnectorError)]
    async fn into_data_stream(self) {
        let partition_manager = self.partition_manager.clone();

        // Restore splits into the partition manager (handles both fresh start and recovery).
        for split in &self.splits {
            let token_key = split.partition_token.clone();
            if split.is_root() {
                partition_manager
                    .initialize_root(split.offset, split.id())
                    .await;
                if split.is_finished() {
                    partition_manager
                        .mark_finished(&token_key, split.offset)
                        .await;
                }
            } else if let Some(wm) = split.offset {
                partition_manager
                    .restore_child_partition(
                        split.partition_token.clone().unwrap_or_default(),
                        split.parent_partition_tokens.clone(),
                        wm,
                        split.state,
                        split.id(),
                    )
                    .await;
            } else {
                tracing::warn!(
                    partition_token = ?split.partition_token,
                    "child partition has no offset, skipping"
                );
            }
        }

        // Signal the stream is alive.
        yield Vec::new();

        let mut partition_streams: FuturesUnordered<tokio::task::JoinHandle<Result<()>>> =
            FuturesUnordered::new();
        let active_tokens: Arc<Mutex<HashSet<Option<String>>>> =
            Arc::new(Mutex::new(HashSet::new()));
        let (msg_tx, mut msg_rx) =
            tokio::sync::mpsc::unbounded_channel::<Vec<SourceMessage>>();

        let ctx = Arc::new(PartitionContext {
            client: self.client.clone(),
            stream_name: self.stream_name.clone(),
            table_name_filter: self.table_name_filter.clone(),
            heartbeat_interval_ms: self.heartbeat_interval_ms,
            retry_attempts: self.retry_attempts,
            retry_backoff: self.retry_backoff,
            retry_backoff_max_delay_ms: self.retry_backoff_max_delay_ms,
            retry_backoff_factor: self.retry_backoff_factor,
            schema_tracker: self.schema_tracker.clone(),
            source_id: self.source_ctx.source_id.as_raw_id(),
            source_name: self.source_ctx.source_name.clone(),
            partition_manager: partition_manager.clone(),
            shutdown_token: self.shutdown_token.clone(),
        });

        // Spawn initial partition tasks.
        for split in &self.splits {
            if split.is_finished() {
                continue;
            }
            let should_start = if split.offset.is_none() {
                split.is_root()
            } else {
                true
            };
            if !should_start {
                continue;
            }
            active_tokens.lock().await.insert(split.partition_token.clone());
            Self::spawn_partition_task(
                &ctx,
                split.clone(),
                split.id(),
                msg_tx.clone(),
                &mut partition_streams,
            );
        }

        tracing::info!(
            initial_partitions = partition_streams.len(),
            "CDC streaming started"
        );

        // Main event loop: concurrently yield messages and handle partition lifecycle.
        //
        // Uses `futures::future::select` instead of `tokio::select!` because the
        // `#[try_stream]` generator is incompatible with `tokio::select!`'s
        // multi-await expansion.
        loop {
            if self.shutdown_token.is_cancelled() {
                tracing::info!("shutdown requested");
                break;
            }

            // Convert to owned enum so boxed futures are dropped before next iteration.
            enum Event {
                Messages(Vec<SourceMessage>),
                ChannelClosed,
                PartitionDone(std::result::Result<Result<()>, tokio::task::JoinError>),
                AllDone,
            }

            let event = match futures::future::select(
                Box::pin(msg_rx.recv()),
                Box::pin(partition_streams.next()),
            )
            .await
            {
                Either::Left((Some(msgs), _)) => Event::Messages(msgs),
                Either::Left((None, _)) => Event::ChannelClosed,
                Either::Right((Some(r), _)) => Event::PartitionDone(r),
                Either::Right((None, _)) => Event::AllDone,
            };

            match event {
                Event::Messages(messages) => {
                    if !messages.is_empty() {
                        yield messages;
                    }
                }

                Event::ChannelClosed => {
                    tracing::info!("message channel closed");
                    break;
                }

                Event::PartitionDone(result) => {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => return Err(e),
                        Err(e) => {
                            return Err(ConnectorError::from(anyhow::anyhow!(
                                "partition task panicked: {}", e
                            )));
                        }
                    }

                    // Spawn ready child partitions.
                    for child_token in partition_manager.get_ready_children().await {
                        let child_token_key = Some(child_token.clone());
                        {
                            let mut tokens = active_tokens.lock().await;
                            if !tokens.insert(child_token_key.clone()) {
                                continue; // already active
                            }
                        }

                        let Some(info) = partition_manager.get_partition(&child_token_key).await
                        else {
                            tracing::warn!(token = ?child_token, "child partition not found");
                            continue;
                        };

                        partition_manager.mark_running(&child_token_key).await;

                        let offset = info.offset.unwrap_or(OffsetDateTime::UNIX_EPOCH);
                        let child_split = SpannerCdcSplit::new_child(
                            child_token.clone(),
                            info.parent_tokens.clone(),
                            offset,
                            ctx.stream_name.clone(),
                            0,
                        );

                        Self::spawn_partition_task(
                            &ctx,
                            child_split,
                            info.split_id.clone(),
                            msg_tx.clone(),
                            &mut partition_streams,
                        );
                        tracing::info!(token = ?child_token, "child partition spawned");
                    }
                }

                Event::AllDone => {
                    // Drain remaining buffered messages.
                    while let Ok(messages) = msg_rx.try_recv() {
                        if !messages.is_empty() {
                            yield messages;
                        }
                    }
                    break;
                }
            }
        }
    }

    /// Execute a change stream query for one partition with retry.
    ///
    /// Data change messages are sent through `msg_tx` immediately.
    /// Returns `Ok(())` when the query finishes (partition split or stream end).
    async fn read_partition(
        client: Client,
        mut split: SpannerCdcSplit,
        stream_name: String,
        table_name_filter: Option<String>,
        heartbeat_interval_ms: i64,
        split_id: SplitId,
        retry_attempts: u32,
        retry_backoff: std::time::Duration,
        retry_backoff_max_delay_ms: u64,
        retry_backoff_factor: u64,
        schema_tracker: Arc<SchemaTracker>,
        source_id: u32,
        source_name: String,
        partition_manager: Arc<PartitionManager>,
        shutdown_token: CancellationToken,
        msg_tx: tokio::sync::mpsc::UnboundedSender<Vec<SourceMessage>>,
    ) -> Result<()> {
        let start_ts = split.offset.ok_or_else(|| {
            ConnectorError::from(anyhow::anyhow!(
                "offset is None for split_id={}, stream={}",
                split_id, stream_name
            ))
        })?;

        split.mark_running();
        partition_manager
            .mark_running(&split.partition_token)
            .await;

        let mut stmt = Statement::new(format!(
            "SELECT ChangeRecord FROM READ_{} (\
                @start_timestamp, @end_timestamp, @partition_token, @heartbeat_milliseconds\
            )",
            stream_name
        ));
        stmt.add_param("start_timestamp", &start_ts);
        stmt.add_param("end_timestamp", &Option::<OffsetDateTime>::None);
        if let Some(ref token) = split.partition_token {
            stmt.add_param("partition_token", token);
        } else {
            stmt.add_param("partition_token", &Option::<String>::None);
        }
        stmt.add_param("heartbeat_milliseconds", &heartbeat_interval_ms);

        tracing::info!(
            %split_id,
            %start_ts,
            partition_token = ?split.partition_token,
            "change stream query starting"
        );

        // Exponential backoff with jitter.
        let retry_strategy = ExponentialBackoff::from_millis(retry_backoff.as_millis() as u64)
            .max_delay(Duration::from_millis(retry_backoff_max_delay_ms))
            .factor(retry_backoff_factor)
            .take(retry_attempts as usize)
            .map(jitter);

        let mut last_error = None;

        for (attempt, delay) in retry_strategy.enumerate() {
            if shutdown_token.is_cancelled() {
                partition_manager
                    .mark_finished(&split.partition_token, split.offset)
                    .await;
                return Ok(());
            }

            match Self::execute_change_stream_query(
                &client, &stmt, &mut split, &split_id, &table_name_filter,
                schema_tracker.clone(), source_id, &source_name,
                partition_manager.clone(), shutdown_token.clone(), &msg_tx,
            )
            .await
            {
                Ok(()) => {
                    partition_manager
                        .mark_finished(&split.partition_token, split.offset)
                        .await;
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(
                        %split_id,
                        attempt = attempt + 1,
                        max_attempts = retry_attempts + 1,
                        ?delay,
                        error = %e,
                        "query failed, retrying"
                    );
                    last_error = Some(e);
                    tokio::time::sleep(delay).await;
                }
            }
        }

        partition_manager
            .mark_finished(&split.partition_token, split.offset)
            .await;
        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!("change stream query failed with no error recorded").into()
        }))
    }

    /// Execute one change stream query, sending messages immediately via `msg_tx`.
    async fn execute_change_stream_query(
        client: &Client,
        stmt: &Statement,
        split: &mut SpannerCdcSplit,
        split_id: &SplitId,
        table_name_filter: &Option<String>,
        schema_tracker: Arc<SchemaTracker>,
        source_id: u32,
        source_name: &str,
        partition_manager: Arc<PartitionManager>,
        shutdown_token: CancellationToken,
        msg_tx: &tokio::sync::mpsc::UnboundedSender<Vec<SourceMessage>>,
    ) -> Result<()> {
        let mut txn = client
            .single()
            .await
            .map_err(|e| anyhow::anyhow!("failed to create transaction: {}", e))?;
        let mut result_set = txn
            .query(stmt.clone())
            .await
            .map_err(|e| anyhow::anyhow!("failed to execute query: {}", e))?;

        while let Some(row) = result_set
            .next()
            .await
            .map_err(|e| anyhow::anyhow!("failed to get next row: {}", e))?
        {
            if shutdown_token.is_cancelled() {
                return Ok(());
            }

            let change_records: Vec<ChangeStreamRecord> = row
                .column(0)
                .map_err(|e| anyhow::anyhow!("failed to get ChangeRecord column: {}", e))?;

            for record in change_records {
                let mut messages = Vec::new();

                for data_change in &record.data_change_record {
                    // Validate value capture type.
                    match data_change.value_capture_type.as_str() {
                        "NEW_ROW" | "NEW_ROW_AND_OLD_VALUES" => {}
                        other => {
                            return Err(anyhow::anyhow!(
                                "unsupported value_capture_type '{}'", other
                            ).into());
                        }
                    }

                    split.advance_offset(data_change.commit_time());
                    split.messages_processed += 1;

                    // Check for schema evolution before table filtering.
                    match schema_tracker
                        .check_and_evolve(data_change, source_id, source_name)
                        .await
                    {
                        Ok(Some(schema_change)) => {
                            let json = schema_change_to_json(&schema_change)
                                .map_err(|e| anyhow::anyhow!("schema change to JSON: {}", e))?;
                            messages.push(self::make_schema_change_msg(
                                split, split_id, json, data_change,
                            ));
                        }
                        Ok(None) => {}
                        Err(e) => return Err(e),
                    }

                    // Filter by table name.
                    if let Some(filter) = table_name_filter {
                        if &data_change.table_name != filter {
                            continue;
                        }
                    }

                    for modification in &data_change.mods {
                        let tagged = TaggedChangeRecord {
                            split_id: split_id.clone(),
                            data_change: data_change.clone(),
                            modification: modification.clone(),
                        };
                        let mut msg = SourceMessage::from(tagged);
                        msg.offset = make_offset_string(split);
                        messages.push(msg);
                    }
                }

                // Send data change messages immediately.
                if !messages.is_empty() {
                    if msg_tx.send(messages).is_err() {
                        return Ok(()); // receiver dropped
                    }
                }

                // Advance offset from heartbeats.
                for heartbeat in &record.heartbeat_record {
                    split.advance_offset(heartbeat.heartbeat_time());
                }

                // Register child partitions.
                for cpr in &record.child_partitions_record {
                    let start_time = cpr.start_time();
                    for cp in &cpr.child_partitions {
                        partition_manager
                            .add_child_partition(
                                cp.token.clone(),
                                cp.parent_partition_tokens.clone(),
                                start_time,
                                format!("{}-child", split_id as &str).into(),
                            )
                            .await;
                    }
                }
            }
        }

        tracing::info!(
            %split_id,
            final_offset = ?split.offset,
            "change stream result set exhausted"
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_offset_string(split: &SpannerCdcSplit) -> String {
    let spanner_offset = crate::source::cdc::external::spanner::SpannerOffset::with_partition(
        split.offset_as_micros(),
        split.partition_token.clone(),
        split.parent_partition_tokens.clone(),
        split.offset_as_micros(),
        split.stream_name.clone(),
        split.index,
    );
    let cdc_offset = crate::source::cdc::external::CdcOffset::Spanner(spanner_offset);
    serde_json::to_string(&cdc_offset)
        .unwrap_or_else(|_| split.offset_as_micros().to_string())
}

fn make_schema_change_msg(
    split: &SpannerCdcSplit,
    split_id: &SplitId,
    payload: Vec<u8>,
    data_change: &crate::source::spanner_cdc::types::DataChangeRecord,
) -> SourceMessage {
    SourceMessage {
        key: None,
        payload: Some(payload.into()),
        offset: make_offset_string(split),
        split_id: split_id.clone(),
        meta: SourceMeta::SpannerCdc(
            crate::source::spanner_cdc::SpannerCdcMeta::new_schema_change(
                data_change.table_name.clone(),
                data_change.commit_time(),
            ),
        ),
    }
}

// ---------------------------------------------------------------------------
// SplitReader trait
// ---------------------------------------------------------------------------

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
        ensure!(!splits.is_empty(), "requires at least one split");

        let client = properties.create_client().await?;
        Ok(Self {
            client,
            splits,
            stream_name: properties.stream_name.clone(),
            table_name_filter: properties.table_name.clone(),
            heartbeat_interval_ms: properties.heartbeat_interval_ms()?,
            parser_config,
            source_ctx,
            retry_attempts: properties.get_retry_attempts(),
            retry_backoff: properties.get_retry_backoff(),
            retry_backoff_max_delay_ms: properties.get_retry_backoff_max_delay_ms(),
            retry_backoff_factor: properties.get_retry_backoff_factor(),
            schema_tracker: Arc::new(SchemaTracker::new()),
            partition_manager: Arc::new(PartitionManager::new()),
            shutdown_token: CancellationToken::new(),
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }

    fn backfill_info(&self) -> std::collections::HashMap<SplitId, BackfillInfo> {
        std::collections::HashMap::new()
    }
}
