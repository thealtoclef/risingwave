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

//! Spanner CDC split reader with shared reader architecture.
//!
//! Implements the Spanner change stream query model:
//! <https://cloud.google.com/spanner/docs/change-streams/details>
//!
//! ## Architecture: Shared Reader Pattern
//!
//! - **One shared reader instance per source** (keyed by source_id)
//! - Multiple table actors `FROM` the same source share the reader
//! - The shared reader broadcasts all change events via a broadcast channel
//! - Each table actor filters events by its `table_name` filter
//!
//! ## Change Stream Query Model
//!
//! The reader handles:
//! - **Partition management**: Tracks partition tokens and handles splits/merges
//! - **Parent-child coordination**: Child partitions are queued until parent finish time
//! - **Heartbeat handling**: Updates offset based on heartbeat records to maintain progress
//! - **Schema evolution**: Detects and propagates schema changes to subscribers
//!
//! ## Benefits
//!
//! - **No duplicate reads**: Each change stream record is read exactly once
//! - **Resource efficient**: One set of partition queries per source, regardless of table count
//! - **Automatic sharing**: Works transparently when creating multiple tables FROM a source

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use futures::{StreamExt, stream::FuturesUnordered};
use futures_async_stream::try_stream;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::statement::Statement;
use risingwave_common::ensure;
use time::OffsetDateTime;
use tokio::sync::{Mutex, broadcast};
use tokio_util::sync::CancellationToken;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

use super::TaggedChangeRecord;
use crate::error::{ConnectorError, ConnectorResult as Result};
use crate::parser::ParserConfig;
use crate::source::spanner_cdc::partition_manager::PartitionManager;
use crate::source::spanner_cdc::schema_track::{schema_change_to_json, SchemaTracker};
use crate::source::spanner_cdc::split::PartitionState;
use crate::source::spanner_cdc::types::ChangeStreamRecord;
use crate::source::spanner_cdc::{SpannerCdcProperties, SpannerCdcSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId,
    SplitMetaData, SplitReader, SourceMeta, into_chunk_stream,
};

/// Global registry of shared Spanner CDC readers, keyed by source_id.
///
/// Enables multiple table actors from the same source to share a single reader:
/// - First subscriber creates the shared reader and spawns partition tasks
/// - Subsequent subscribers subscribe to the existing broadcast channel
/// - Last subscriber dropping triggers cleanup of the shared reader
static SHARED_READERS: once_cell::sync::Lazy<
    DashMap<u32, Arc<Mutex<SharedReaderState>>>
> = once_cell::sync::Lazy::new(DashMap::new);

/// State for a shared Spanner CDC reader
struct SharedReaderState {
    /// The source ID for this reader
    source_id: u32,
    /// The broadcast sender for change events
    tx: broadcast::Sender<Vec<SourceMessage>>,
    /// Cancellation token for the reader task
    shutdown_token: CancellationToken,
    /// Number of active subscribers (table actors)
    subscriber_count: usize,
}

impl Drop for SharedReaderState {
    fn drop(&mut self) {
        // Cancel the reader task
        self.shutdown_token.cancel();

        // Remove from the global registry
        SHARED_READERS.remove(&self.source_id);

        tracing::info!(source_id = self.source_id, "removed shared Spanner CDC reader from registry");
    }
}

/// Context for spawning partition tasks in the shared reader
struct PartitionContext {
    client: Client,
    change_stream_name: String,
    max_concurrent_change_stream_partitions: usize,
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
    tx: broadcast::Sender<Vec<SourceMessage>>,
}

pub struct SpannerCdcSplitReader {
    /// The broadcast receiver for this subscriber
    rx: broadcast::Receiver<Vec<SourceMessage>>,
    /// Table name filter for this subscriber
    table_name_filter: Option<String>,
    /// Parser config
    parser_config: ParserConfig,
    /// Source context
    source_ctx: SourceContextRef,
    /// Reference to the shared reader state for cleanup
    _shared_state: Arc<Mutex<SharedReaderState>>,
}

impl SpannerCdcSplitReader {
    /// Get or create a shared reader for the given source_id
    async fn get_or_create_shared_reader(
        source_id: u32,
        properties: &SpannerCdcProperties,
        splits: &[SpannerCdcSplit],
        source_name: String,
    ) -> Result<(
        broadcast::Receiver<Vec<SourceMessage>>,
        Option<String>,
        Arc<Mutex<SharedReaderState>>,
    )> {
        use dashmap::mapref::entry::Entry;

        match SHARED_READERS.entry(source_id) {
            Entry::Occupied(entry) => {
                // Reader already exists, subscribe to it
                let state = entry.get().clone();
                let mut state = state.lock().await;
                state.subscriber_count += 1;
                let rx = state.tx.subscribe();
                let table_filter = properties.table_name.clone();
                drop(state);
                Ok((rx, table_filter, entry.get().clone()))
            }
            Entry::Vacant(entry) => {
                // Create a new shared reader
                let client = properties.create_client().await?;
                let heartbeat_interval_ms = properties.heartbeat_interval_ms()?;
                let retry_attempts = properties.get_retry_attempts();
                let retry_backoff = properties.get_retry_backoff();
                let retry_backoff_max_delay_ms = properties.get_retry_backoff_max_delay_ms();
                let retry_backoff_factor = properties.get_retry_backoff_factor();
                let buffer_size = properties.get_buffer_size();

                // Create broadcast channel
                let (tx, _rx) = broadcast::channel(buffer_size);

                let shutdown_token = CancellationToken::new();
                let partition_manager = Arc::new(PartitionManager::new());
                let schema_tracker = Arc::new(SchemaTracker::new());

                // Initialize partition manager with splits
                for split in splits {
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
                    } else if let Some(offset) = split.offset {
                        partition_manager
                            .restore_child_partition(
                                split.partition_token.clone().unwrap_or_default(),
                                split.parent_partition_tokens.clone(),
                                offset,
                                split.state,
                                split.id(),
                            )
                            .await;
                    }
                }

                let ctx = Arc::new(PartitionContext {
                    client,
                    change_stream_name: properties.change_stream_name.clone(),
                    heartbeat_interval_ms,
                    max_concurrent_change_stream_partitions: properties.get_change_stream_max_concurrent_partitions(),
                    retry_attempts,
                    retry_backoff,
                    retry_backoff_max_delay_ms,
                    retry_backoff_factor,
                    schema_tracker: schema_tracker.clone(),
                    source_id,
                    source_name,
                    partition_manager: partition_manager.clone(),
                    shutdown_token: shutdown_token.clone(),
                    tx: tx.clone(),
                });

                // Spawn the shared reader task
                let _reader_handle = tokio::spawn(async move {
                    Self::run_shared_reader(ctx).await
                });

                let state = Arc::new(Mutex::new(SharedReaderState {
                    source_id,
                    tx,
                    shutdown_token,
                    subscriber_count: 1,
                }));

                let rx = state.lock().await.tx.subscribe();
                let table_filter = properties.table_name.clone();
                entry.insert(state.clone());

                tracing::info!(source_id, "created shared Spanner CDC reader");
                Ok((rx, table_filter, state))
            }
        }
    }

    /// The main shared reader loop that reads from Spanner and broadcasts to all subscribers
    async fn run_shared_reader(
        ctx: Arc<PartitionContext>,
    ) -> Result<()> {
        let partition_manager = ctx.partition_manager.clone();
        let mut partition_streams: FuturesUnordered<tokio::task::JoinHandle<Result<()>>> =
            FuturesUnordered::new();
        let active_tokens: Arc<Mutex<HashSet<Option<String>>>> =
            Arc::new(Mutex::new(HashSet::new()));

        let max_concurrent = ctx.max_concurrent_change_stream_partitions;
        let mut active_count: usize = 0;
        let mut pending: std::collections::VecDeque<(SpannerCdcSplit, SplitId)> =
            std::collections::VecDeque::new();

        // Get initial splits from partition manager
        let all_tokens = partition_manager.all_tokens().await;
        for token in &all_tokens {
            if let Some(info) = partition_manager.get_partition(token).await {
                let split = SpannerCdcSplit {
                    partition_token: info.partition_token.clone(),
                    parent_partition_tokens: info.parent_tokens.clone(),
                    offset: info.offset,
                    change_stream_name: ctx.change_stream_name.clone(),
                    index: 0,
                    state: info.state,
                    messages_processed: 0,
                    snapshot_done: info.state == PartitionState::Finished,
                };

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

                let split_id = split.id();
                active_tokens.lock().await.insert(split.partition_token.clone());
                if active_count < max_concurrent {
                    active_count += 1;
                    Self::spawn_partition_task(&ctx, split, split_id, &mut partition_streams);
                } else {
                    partition_manager.mark_scheduled(&split.partition_token).await;
                    pending.push_back((split, split_id));
                }
            }
        }

        tracing::info!(
            initial_partitions = partition_streams.len(),
            deferred_partitions = pending.len(),
            max_concurrent,
            "shared Spanner CDC reader started"
        );

        // Main event loop
        loop {
            if ctx.shutdown_token.is_cancelled() {
                tracing::info!("shared reader shutdown requested");
                break;
            }

            enum Event {
                PartitionDone(std::result::Result<Result<()>, tokio::task::JoinError>),
                AllDone,
            }

            let event = match partition_streams.next().await {
                Some(r) => Event::PartitionDone(r),
                None => Event::AllDone,
            };

            match event {
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

                    active_count = active_count.saturating_sub(1);

                    // Drain deferred pending partitions first
                    while active_count < max_concurrent {
                        if let Some((deferred_split, deferred_id)) = pending.pop_front() {
                            partition_manager.mark_running(&deferred_split.partition_token).await;
                            active_count += 1;
                            Self::spawn_partition_task(&ctx, deferred_split, deferred_id, &mut partition_streams);
                        } else {
                            break;
                        }
                    }

                    // Then enqueue newly-ready child partitions
                    for child_token in partition_manager.get_ready_children().await {
                        let child_token_key = Some(child_token.clone());
                        {
                            let mut tokens = active_tokens.lock().await;
                            if !tokens.insert(child_token_key.clone()) {
                                continue;
                            }
                        }

                        let Some(info) = partition_manager.get_partition(&child_token_key).await else {
                            tracing::warn!(token = ?child_token, "child partition not found");
                            continue;
                        };

                        let offset = info.offset.unwrap_or(OffsetDateTime::UNIX_EPOCH);
                        let child_split = SpannerCdcSplit::new_child(
                            child_token.clone(),
                            info.parent_tokens.clone(),
                            offset,
                            ctx.change_stream_name.clone(),
                            0,
                        );
                        let child_split_id = info.split_id.clone();

                        if active_count < max_concurrent {
                            partition_manager.mark_running(&child_token_key).await;
                            active_count += 1;
                            Self::spawn_partition_task(&ctx, child_split, child_split_id, &mut partition_streams);
                            tracing::info!(token = ?child_token, "child partition spawned");
                        } else {
                            partition_manager.mark_scheduled(&child_token_key).await;
                            pending.push_back((child_split, child_split_id));
                            tracing::debug!(token = ?child_token, "child partition queued (at limit)");
                        }
                    }
                }

                Event::AllDone => {
                    let mut started = false;
                    while active_count < max_concurrent {
                        if let Some((deferred_split, deferred_id)) = pending.pop_front() {
                            partition_manager.mark_running(&deferred_split.partition_token).await;
                            active_count += 1;
                            Self::spawn_partition_task(&ctx, deferred_split, deferred_id, &mut partition_streams);
                            started = true;
                        } else {
                            break;
                        }
                    }
                    if started {
                        continue;
                    }
                    break;
                }
            }
        }

        Ok(())
    }

    /// Spawn a partition task that reads a change stream query and broadcasts messages
    fn spawn_partition_task(
        ctx: &Arc<PartitionContext>,
        split: SpannerCdcSplit,
        split_id: SplitId,
        partition_streams: &mut FuturesUnordered<tokio::task::JoinHandle<Result<()>>>,
    ) {
        let client = ctx.client.clone();
        let change_stream_name = ctx.change_stream_name.clone();
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
        let tx = ctx.tx.clone();

        partition_streams.push(tokio::spawn(async move {
            Self::read_partition(
                client,
                split,
                change_stream_name,
                None, // No table filter at partition level - filter at subscriber level
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
                tx,
            )
            .await
        }));
    }

    /// Execute a change stream query for one partition and broadcast results
    async fn read_partition(
        client: Client,
        mut split: SpannerCdcSplit,
        change_stream_name: String,
        _table_name_filter: Option<String>,
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
        tx: broadcast::Sender<Vec<SourceMessage>>,
    ) -> Result<()> {
        let start_ts = split.offset.ok_or_else(|| {
            ConnectorError::from(anyhow::anyhow!(
                "offset is None for split_id={}, change_stream={}",
                split_id, change_stream_name
            ))
        })?;

        split.mark_running();
        partition_manager.mark_running(&split.partition_token).await;

        let mut stmt = Statement::new(format!(
            "SELECT ChangeRecord FROM READ_{} (\
                @start_timestamp, @end_timestamp, @partition_token, @heartbeat_milliseconds\
            )",
            change_stream_name
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

        let retry_strategy = ExponentialBackoff::from_millis(retry_backoff.as_millis() as u64)
            .max_delay(tokio::time::Duration::from_millis(retry_backoff_max_delay_ms))
            .factor(retry_backoff_factor)
            .take(retry_attempts as usize)
            .map(jitter);

        let mut last_error = None;

        for (attempt, delay) in retry_strategy.enumerate() {
            if shutdown_token.is_cancelled() {
                partition_manager.mark_finished(&split.partition_token, split.offset).await;
                return Ok(());
            }

            match Self::execute_change_stream_query(
                &client, &stmt, &mut split, &split_id, &schema_tracker,
                source_id, &source_name, partition_manager.clone(),
                shutdown_token.clone(), &tx,
            )
            .await
            {
                Ok(()) => {
                    partition_manager.mark_finished(&split.partition_token, split.offset).await;
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

        partition_manager.mark_finished(&split.partition_token, split.offset).await;
        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!("change stream query failed with no error recorded").into()
        }))
    }

    /// Execute one change stream query and broadcast results
    async fn execute_change_stream_query(
        client: &Client,
        stmt: &Statement,
        split: &mut SpannerCdcSplit,
        split_id: &SplitId,
        schema_tracker: &Arc<SchemaTracker>,
        source_id: u32,
        source_name: &str,
        partition_manager: Arc<PartitionManager>,
        shutdown_token: CancellationToken,
        tx: &broadcast::Sender<Vec<SourceMessage>>,
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
                    tracing::info!(
                        split_id = %split_id,
                        table_name = %data_change.table_name,
                        commit_time = ?data_change.commit_time(),
                        mod_count = data_change.mods.len(),
                        "received data change from Spanner change stream"
                    );
                    split.advance_offset(data_change.commit_time());
                    split.messages_processed += 1;

                    // Handle schema changes
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

                    // Create messages for all modifications (no table filtering here)
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

                // Broadcast messages to all subscribers
                if !messages.is_empty() {
                    tracing::info!(
                        split_id = %split_id,
                        message_count = messages.len(),
                        "broadcasting CDC messages to subscribers"
                    );
                    if tx.send(messages).is_err() {
                        // All receivers dropped, stop reading
                        return Ok(());
                    }
                }

                // Handle heartbeats
                for heartbeat in &record.heartbeat_record {
                    tracing::debug!(
                        split_id = %split_id,
                        heartbeat_time = ?heartbeat.heartbeat_time(),
                        "received heartbeat from change stream"
                    );
                    split.advance_offset(heartbeat.heartbeat_time());
                }

                // Handle child partition discovery
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
// SplitReader trait implementation
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

        let source_id = source_ctx.source_id.as_raw_id();
        let source_name = source_ctx.source_name.clone();

        let (rx, table_name_filter, shared_state) = Self::get_or_create_shared_reader(
            source_id,
            &properties,
            &splits,
            source_name,
        )
        .await?;

        Ok(Self {
            rx,
            table_name_filter,
            parser_config,
            source_ctx,
            _shared_state: shared_state,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        let table_name_filter = self.table_name_filter.clone();
        into_chunk_stream(self.into_data_stream(table_name_filter), parser_config, source_context)
    }
}

impl SpannerCdcSplitReader {
    #[try_stream(ok = Vec<SourceMessage>, error = ConnectorError)]
    async fn into_data_stream(mut self, table_name_filter: Option<String>) {
        let source_name = self.source_ctx.source_name.clone();

        loop {
            match self.rx.recv().await {
                Ok(messages) => {
                    // Filter messages by table name if a filter is set
                    let filtered = if let Some(ref filter) = table_name_filter {
                        messages
                            .into_iter()
                            .filter(|msg| {
                                if let SourceMeta::SpannerCdc(ref meta) = msg.meta {
                                    &meta.table_name == filter
                                } else {
                                    true // Keep messages without SpannerCdc meta
                                }
                            })
                            .collect()
                    } else {
                        messages
                    };

                    if !filtered.is_empty() {
                        yield filtered;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(ConnectorError::from(anyhow::anyhow!("shared reader channel closed")));
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(source_name, "subscriber lagged by {} messages", n);
                    // Continue receiving - don't break the stream
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Drop implementation for cleanup
// ---------------------------------------------------------------------------

impl Drop for SpannerCdcSplitReader {
    fn drop(&mut self) {
        // The _shared_state field holds an Arc to the SharedReaderState.
        // When the last subscriber (SpannerCdcSplitReader) is dropped,
        // the Arc count will reach 1 (only the registry entry holds a reference).
        // At that point, the SharedReaderState::drop will be called,
        // which cancels the reader task and removes the entry from SHARED_READERS.
        //
        // We decrement the subscriber count here so the shared reader knows
        // when all subscribers have disconnected.
        let shared_state = self._shared_state.clone();
        tokio::spawn(async move {
            let mut state = shared_state.lock().await;
            state.subscriber_count = state.subscriber_count.saturating_sub(1);
            tracing::info!(
                source_id = state.source_id,
                remaining_subscribers = state.subscriber_count,
                "subscriber dropped"
            );
            // When subscriber_count reaches 0, the shared reader task will stop
            // (due to all broadcast receivers being dropped), and the
            // SharedReaderState::drop will clean up the registry entry.
        });
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
        split.change_stream_name.clone(),
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
