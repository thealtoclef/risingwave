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

//! Spanner CDC split reader.
//!
//! Follows the same architecture as the Debezium CDC reader (`CdcSplitReader`):
//!
//! 1. `SplitReader::new()` spawns a background task that reads from Spanner
//! 2. The background task sends `Vec<SourceMessage>` through an `mpsc` channel
//! 3. `into_data_stream()` calls `rx.recv()` and yields messages
//! 4. `into_stream()` wraps with `into_chunk_stream` (parser)
//!
//! ## ReorderBuffer cross-partition ordering
//!
//! Spanner change-stream partitions are read concurrently with no cross-partition
//! commit-timestamp ordering. Partition tasks send `PartitionRecord`s to a shared
//! record channel. The `run_reader` main loop runs a `ReorderBuffer` that:
//!
//! 1. Tracks per-partition high watermarks (data records + heartbeats)
//! 2. Computes a global watermark = min(active partitions' watermarks)
//! 3. Buffers records and emits them in commit-ts order only when the global
//!    watermark has advanced past their timestamp
//!
//! This ensures schema changes are always seen in commit-ts order, making it
//! impossible for a lagging partition to regress the tracked schema.

use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use futures_async_stream::try_stream;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::statement::Statement;
use risingwave_common::ensure;
use risingwave_pb::connector_service::{SourceType, cdc_message};
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

use super::TaggedChangeRecord;
use crate::error::{ConnectorError, ConnectorResult as Result};
use crate::parser::ParserConfig;
use crate::source::cdc::DebeziumCdcMeta;
use crate::source::spanner_cdc::reorder_buffer::{
    PartitionRecord, ReorderBuffer, ReorderBufferEvent,
};
use crate::source::spanner_cdc::split::{PartitionState, PerPartitionProgress};
use crate::source::spanner_cdc::types::ChangeStreamRecord;
use crate::source::spanner_cdc::{SpannerCdcProperties, SpannerCdcSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SourceMeta, SplitId,
    SplitReader, into_chunk_stream,
};

const DEFAULT_CHANNEL_SIZE: usize = 16;

/// Spanner CDC split reader — same pattern as Debezium's `CdcSplitReader`.
///
/// The background task reads from the Spanner change stream and sends messages
/// through an `mpsc` channel. This reader just receives and yields them.
pub struct SpannerCdcSplitReader {
    /// Receives `Vec<SourceMessage>` from the background reader task.
    rx: mpsc::Receiver<Vec<SourceMessage>>,
    /// Parser config
    parser_config: ParserConfig,
    /// Source context
    source_ctx: SourceContextRef,
}

// ---------------------------------------------------------------------------
// SplitReader trait implementation (matches Debezium CdcSplitReader)
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
        let (tx, rx) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

        // Extract the checkpointed offset from splits.
        // Use lagging_edge_micros (not offset_as_micros) because the checkpointed
        // value represents the lagging edge persisted by encode_to_json.
        let checkpointed_offset = splits
            .iter()
            .find(|s| s.index == source_id)
            .map(|s| s.lagging_edge_micros())
            .filter(|&m| m > 0)
            .and_then(|micros| {
                OffsetDateTime::from_unix_timestamp_nanos((micros as i128) * 1000).ok()
            });

        // Create the Spanner client and reader context
        let client = properties.create_client().await?;
        let heartbeat_interval_ms = properties.heartbeat_interval_ms()?;

        let ctx = ReaderContext {
            client,
            database_name: properties.database.clone(),
            change_stream_name: properties.change_stream_name.clone(),
            max_concurrent_partitions: properties.get_change_stream_max_concurrent_partitions(),
            heartbeat_interval_ms,
            retry_attempts: properties.get_retry_attempts(),
            retry_backoff: properties.get_retry_backoff(),
            retry_backoff_max_delay_ms: properties.get_retry_backoff_max_delay_ms(),
            retry_backoff_factor: properties.get_retry_backoff_factor(),
            source_id,
            checkpointed_offset,
        };

        // Spawn background task — like Debezium spawns the JNI thread
        tokio::spawn(async move {
            if let Err(e) = run_reader(ctx, tx).await {
                tracing::error!(error = %e, "Spanner CDC reader task failed");
            }
        });

        tracing::info!(source_id, "Spanner CDC reader started");

        Ok(Self {
            rx,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}

impl SpannerCdcSplitReader {
    /// Identical pattern to `CdcSplitReader::into_data_stream` — just recv from mpsc.
    #[try_stream(ok = Vec<SourceMessage>, error = ConnectorError)]
    async fn into_data_stream(mut self) {
        let source_id = self.source_ctx.source_id.to_string();

        while let Some(messages) = self.rx.recv().await {
            if !messages.is_empty() {
                yield messages;
            }
        }
        // Sender dropped — reader task exited. Report error metric
        // same as Debezium's CdcSplitReader does on channel errors.
        risingwave_common::metrics::GLOBAL_ERROR_METRICS
            .user_source_error
            .report([
                "spanner_cdc_source".to_owned(),
                source_id,
                self.source_ctx.source_name.clone(),
                self.source_ctx.fragment_id.to_string(),
            ]);
        return Err(ConnectorError::from(anyhow::anyhow!(
            "Spanner CDC reader channel closed"
        )));
    }
}

// ---------------------------------------------------------------------------
// Background reader task (equivalent to Debezium's JNI thread)
// ---------------------------------------------------------------------------

/// Context for the background reader task.
struct ReaderContext {
    client: Client,
    database_name: String,
    change_stream_name: String,
    max_concurrent_partitions: usize,
    heartbeat_interval_ms: i64,
    retry_attempts: u32,
    retry_backoff: std::time::Duration,
    retry_backoff_max_delay_ms: u64,
    retry_backoff_factor: u64,
    source_id: u32,
    checkpointed_offset: Option<OffsetDateTime>,
}

/// Result from each partition task.
struct PartitionResult {
    partition_token: Option<String>,
    final_offset: Option<OffsetDateTime>,
}

/// Tagged wrapper so the ReorderBuffer knows which partition a record came from.
struct TaggedPartitionRecord {
    partition_token: Option<String>,
    record: PartitionRecord,
}

/// Key for grouping child partitions into sibling groups.
/// Children from the same parent split/merge share the same (parents, start_timestamp).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SiblingKey {
    parent_tokens: Vec<String>, // sorted
    start_ts: OffsetDateTime,
}

/// Collect all ready children from `pending_children` and `child_discovery_rx`.
/// "Ready" means all parent partitions have finished.
fn collect_ready_children(
    pending_children: &mut VecDeque<SpannerCdcSplit>,
    child_discovery_rx: &mut tokio::sync::mpsc::UnboundedReceiver<SpannerCdcSplit>,
    partition_progress: &mut HashMap<String, PerPartitionProgress>,
) -> Vec<SpannerCdcSplit> {
    let mut ready = Vec::new();
    let mut still_pending = VecDeque::new();

    // From pending_children
    for child in pending_children.drain(..) {
        if parents_all_finished(&child.parent_partition_tokens, partition_progress) {
            ready.push(child);
        } else {
            still_pending.push_back(child);
        }
    }
    *pending_children = still_pending;

    // Drain any newly arrived children from the channel
    while let Ok(child) = child_discovery_rx.try_recv() {
        if let Some(ref token) = child.partition_token {
            let start_offset = child.offset.unwrap_or_else(OffsetDateTime::now_utc);
            let parents = child.parent_partition_tokens.clone();
            if !register_child(partition_progress, token.clone(), parents, start_offset) {
                continue; // duplicate
            }
        }
        if parents_all_finished(&child.parent_partition_tokens, partition_progress) {
            ready.push(child);
        } else {
            pending_children.push_back(child);
        }
    }

    ready
}

/// Group children by (sorted parent tokens, start_timestamp) → sibling groups.
fn group_siblings(children: &[SpannerCdcSplit]) -> HashMap<SiblingKey, Vec<SpannerCdcSplit>> {
    let mut groups: HashMap<SiblingKey, Vec<SpannerCdcSplit>> = HashMap::new();
    for child in children {
        let mut parents = child.parent_partition_tokens.clone();
        parents.sort();
        let key = SiblingKey {
            parent_tokens: parents,
            start_ts: child.offset.unwrap_or_else(OffsetDateTime::now_utc),
        };
        groups.entry(key).or_default().push(child.clone());
    }
    groups
}

async fn run_reader(ctx: ReaderContext, tx: mpsc::Sender<Vec<SourceMessage>>) -> Result<()> {
    let mut partition_streams: FuturesUnordered<tokio::task::JoinHandle<Result<PartitionResult>>> =
        FuturesUnordered::new();

    let max_concurrent = ctx.max_concurrent_partitions;
    let mut active_count: usize = 0;

    // Local HashMap for dedup and parent coordination. Not used for recovery.
    let mut partition_progress: HashMap<String, PerPartitionProgress> = HashMap::new();

    let mut pending_children: VecDeque<SpannerCdcSplit> = VecDeque::new();
    let (child_discovery_tx, mut child_discovery_rx) =
        tokio::sync::mpsc::unbounded_channel::<SpannerCdcSplit>();

    // Shared record channel: partition tasks → ReorderBuffer
    let (record_tx, mut record_rx) = mpsc::channel::<TaggedPartitionRecord>(DEFAULT_CHANNEL_SIZE);

    let split_id = SplitId::from(ctx.source_id.to_string());
    let root_offset = ctx
        .checkpointed_offset
        .unwrap_or_else(OffsetDateTime::now_utc);
    let mut reorder_buf = ReorderBuffer::new();

    let mut next_sibling_group_id: u64 = 1;

    tracing::info!(starting_offset = ?root_offset, "starting Spanner CDC reader with root partition");

    let root_split =
        SpannerCdcSplit::new_root(ctx.change_stream_name.clone(), ctx.source_id, root_offset);
    reorder_buf.handle(ReorderBufferEvent::PartitionStarted {
        token: root_split.partition_token.clone(),
        start_ts: root_offset,
        group_id: None,
    });

    active_count += 1;
    spawn_partition_task(
        &ctx,
        root_split,
        &split_id,
        &mut partition_streams,
        child_discovery_tx.clone(),
        &record_tx,
    );

    // Main event loop
    loop {
        if tx.is_closed() {
            // Graceful shutdown: RisingWave cancelled us (source dropped, rebalance, etc).
            // Drain the ReorderBuffer and attempt a final send so the downstream
            // can checkpoint as far as possible before we exit.
            let remaining = reorder_buf.drain();
            if !remaining.is_empty() {
                tracing::info!(
                    count = remaining.len(),
                    "graceful shutdown: emitting buffered records"
                );
                // tx is closed, so send will fail — but the try makes the intent clear.
                // Records will be re-delivered from the last checkpoint on restart.
                let _ = tx.send(remaining).await;
            }
            tracing::info!("reader channel closed, stopping");
            break;
        }

        tokio::select! {
            result = partition_streams.next() => {
                match result {
                    Some(Ok(Ok(pr))) => {
                        reorder_buf.handle(ReorderBufferEvent::PartitionFinished {
                            token: pr.partition_token.clone(),
                        });

                        if let Some(ref token) = pr.partition_token {
                            if let Some(entry) = partition_progress.get_mut(token) {
                                entry.state = PartitionState::Finished;
                                if let Some(off) = pr.final_offset { entry.offset = Some(off); }
                            }
                        }

                        // Collect ready children and group by sibling key
                        let ready_children = collect_ready_children(
                            &mut pending_children, &mut child_discovery_rx,
                            &mut partition_progress,
                        );

                        // Group by (sorted parent tokens, start_timestamp) → sibling groups
                        let groups = group_siblings(&ready_children);
                        for (_key, children) in groups {
                            let group_id = next_sibling_group_id;
                            next_sibling_group_id += 1;

                            let tokens: Vec<Option<String>> = children.iter()
                                .map(|c| c.partition_token.clone()).collect();
                            reorder_buf.handle(ReorderBufferEvent::SiblingGroupDeclared {
                                group_id, tokens,
                            });

                            for child in children {
                                if active_count >= max_concurrent {
                                    pending_children.push_front(child);
                                    break;
                                }
                                let child_start_ts = child.offset.unwrap_or(root_offset);
                                if let Some(ref token) = child.partition_token {
                                    set_running(&mut partition_progress, token);
                                }
                                reorder_buf.handle(ReorderBufferEvent::PartitionStarted {
                                    token: child.partition_token.clone(),
                                    start_ts: child_start_ts,
                                    group_id: Some(group_id),
                                });
                                active_count += 1;
                                spawn_partition_task(
                                    &ctx, child, &split_id, &mut partition_streams,
                                    child_discovery_tx.clone(), &record_tx,
                                );
                            }
                        }

                        // Emit control message + any buffered records now ready
                        let mut batch = reorder_buf.drain();
                        if let Some(ref token) = pr.partition_token {
                            let ctrl = make_partition_control_message(
                                &split_id, token,
                                &partition_progress.get(token).map(|e| e.parent_tokens.clone()).unwrap_or_default(),
                                pr.final_offset.map(|o| (o.unix_timestamp_nanos() / 1000) as i64).unwrap_or(0),
                                true,
                            );
                            batch.push(ctrl);
                        } else {
                            let ctrl = make_root_finished_message(
                                &split_id,
                                pr.final_offset.map(|o| (o.unix_timestamp_nanos() / 1000) as i64).unwrap_or(0),
                            );
                            batch.push(ctrl);
                        }
                        if !batch.is_empty() && tx.send(batch).await.is_err() { break; }
                    }
                    Some(Ok(Err(e))) => return Err(e),
                    Some(Err(e)) => {
                        return Err(ConnectorError::from(anyhow::anyhow!("partition task panicked: {}", e)));
                    }
                    None => {
                        // All partition streams finished
                        let batch = reorder_buf.drain();
                        if !batch.is_empty() && tx.send(batch).await.is_err() { break; }
                        tracing::warn!(%split_id, pending = pending_children.len(), "all partitions finished");
                        break;
                    }
                }

                active_count = active_count.saturating_sub(1);

                // Try starting pending children (non-parent-triggered path)
                while active_count < max_concurrent {
                    if let Some(child) = pending_children.pop_front() {
                        if parents_all_finished(&child.parent_partition_tokens, &partition_progress) {
                            if let Some(ref token) = child.partition_token { set_running(&mut partition_progress, token); }
                            active_count += 1;
                            spawn_partition_task(&ctx, child, &split_id, &mut partition_streams, child_discovery_tx.clone(), &record_tx);
                        } else { pending_children.push_front(child); break; }
                    } else { break; }
                }
            }

            Some(child) = child_discovery_rx.recv() => {
                if let Some(ref token) = child.partition_token {
                    let start_offset = child.offset.unwrap_or(root_offset);
                    let parents = child.parent_partition_tokens.clone();

                    if !register_child(&mut partition_progress, token.clone(), parents.clone(), start_offset) {
                        tracing::debug!(token = %token, "duplicate child partition, skipping");
                        continue;
                    }

                    // No registration message sent to the executor. The executor
                    // learns about this child when it starts producing data (or
                    // when it finishes). This avoids anchoring the lagging edge
                    // to discovery_time before real data is consumed.
                }
                pending_children.push_back(child);
            }

            Some(tagged) = record_rx.recv() => {
                let messages = reorder_buf.handle(ReorderBufferEvent::Record {
                    partition_token: tagged.partition_token,
                    record: tagged.record,
                });
                if !messages.is_empty() {
                    if tx.send(messages).await.is_err() { break; }
                }
            }
        }
    }

    // Safety net: drain anything still buffered (e.g., after a `break` from
    // a send error). Partition tasks have already exited or will exit when
    // `record_tx` is dropped.
    let remaining = reorder_buf.drain();
    if !remaining.is_empty() {
        tracing::info!(
            count = remaining.len(),
            "final drain: emitting remaining buffered records"
        );
        // Best-effort send. If `tx` is closed, these records are re-delivered
        // from the last checkpoint on restart.
        let _ = tx.send(remaining).await;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Partition coordination helpers
// ---------------------------------------------------------------------------

/// Register a child partition. Returns `true` if new, `false` if duplicate.
fn register_child(
    partition_progress: &mut HashMap<String, PerPartitionProgress>,
    token: String,
    parent_tokens: Vec<String>,
    start_offset: OffsetDateTime,
) -> bool {
    use std::collections::hash_map::Entry;
    match partition_progress.entry(token) {
        Entry::Vacant(e) => {
            e.insert(PerPartitionProgress {
                offset: Some(start_offset),
                parent_tokens,
                state: PartitionState::Pending,
            });
            true
        }
        Entry::Occupied(_) => false,
    }
}

fn parents_all_finished(
    parent_tokens: &[String],
    partition_progress: &HashMap<String, PerPartitionProgress>,
) -> bool {
    parent_tokens.iter().all(|p| {
        partition_progress
            .get(p)
            .map(|pp| pp.state == PartitionState::Finished)
            .unwrap_or(false)
    })
}

fn set_running(partition_progress: &mut HashMap<String, PerPartitionProgress>, token: &str) {
    if let Some(entry) = partition_progress.get_mut(token) {
        entry.state = PartitionState::Running;
    }
}

// ---------------------------------------------------------------------------
// Partition task management
// ---------------------------------------------------------------------------

fn spawn_partition_task(
    ctx: &ReaderContext,
    split: SpannerCdcSplit,
    split_id: &SplitId,
    partition_streams: &mut FuturesUnordered<tokio::task::JoinHandle<Result<PartitionResult>>>,
    child_discovery_tx: tokio::sync::mpsc::UnboundedSender<SpannerCdcSplit>,
    record_tx: &mpsc::Sender<TaggedPartitionRecord>,
) {
    let client = ctx.client.clone();
    let database_name = ctx.database_name.clone();
    let change_stream_name = ctx.change_stream_name.clone();
    let heartbeat_interval_ms = ctx.heartbeat_interval_ms;
    let retry_attempts = ctx.retry_attempts;
    let retry_backoff = ctx.retry_backoff;
    let retry_backoff_max_delay_ms = ctx.retry_backoff_max_delay_ms;
    let retry_backoff_factor = ctx.retry_backoff_factor;
    let record_tx = record_tx.clone();
    let split_id = split_id.clone();
    let partition_token = split.partition_token.clone();

    partition_streams.push(tokio::spawn(async move {
        let result = read_partition(
            client,
            database_name,
            split,
            change_stream_name,
            heartbeat_interval_ms,
            split_id,
            retry_attempts,
            retry_backoff,
            retry_backoff_max_delay_ms,
            retry_backoff_factor,
            record_tx,
            child_discovery_tx,
        )
        .await;
        match result {
            Ok(final_offset) => Ok(PartitionResult {
                partition_token,
                final_offset: Some(final_offset),
            }),
            Err(e) => Err(e),
        }
    }));
}

// ---------------------------------------------------------------------------
// Change stream query execution
// ---------------------------------------------------------------------------

/// Read a single partition to completion. Returns the final offset.
async fn read_partition(
    client: Client,
    database_name: String,
    mut split: SpannerCdcSplit,
    change_stream_name: String,
    heartbeat_interval_ms: i64,
    split_id: SplitId,
    retry_attempts: u32,
    retry_backoff: std::time::Duration,
    retry_backoff_max_delay_ms: u64,
    retry_backoff_factor: u64,
    record_tx: mpsc::Sender<TaggedPartitionRecord>,
    child_discovery_tx: tokio::sync::mpsc::UnboundedSender<SpannerCdcSplit>,
) -> Result<OffsetDateTime> {
    let start_ts = split.offset.ok_or_else(|| {
        ConnectorError::from(anyhow::anyhow!(
            "offset is None for split_id={}, change_stream={}",
            split_id,
            change_stream_name
        ))
    })?;

    let mut stmt = Statement::new(format!(
        "SELECT ChangeRecord FROM READ_{} (@start_timestamp, @end_timestamp, @partition_token, @heartbeat_milliseconds)",
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

    tracing::info!(%split_id, %start_ts, partition_token = ?split.partition_token, "change stream query starting");

    let retry_strategy = ExponentialBackoff::from_millis(retry_backoff.as_millis() as u64)
        .max_delay(tokio::time::Duration::from_millis(
            retry_backoff_max_delay_ms,
        ))
        .factor(retry_backoff_factor)
        .take(retry_attempts as usize)
        .map(jitter);

    let mut last_error = None;

    for (attempt, delay) in retry_strategy.enumerate() {
        if record_tx.is_closed() {
            return Ok(split.offset.unwrap_or(start_ts));
        }
        match execute_query(
            &client,
            &database_name,
            &stmt,
            &mut split,
            &split_id,
            &record_tx,
            &child_discovery_tx,
            &change_stream_name,
        )
        .await
        {
            Ok(()) => return Ok(split.offset.unwrap_or(start_ts)),
            Err(e) => {
                tracing::warn!(%split_id, attempt = attempt + 1, max_attempts = retry_attempts + 1, ?delay, error = %e, "query failed, retrying");
                last_error = Some(e);
                tokio::time::sleep(delay).await;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        anyhow::anyhow!("change stream query failed with no error recorded").into()
    }))
}

async fn execute_query(
    client: &Client,
    database_name: &str,
    stmt: &Statement,
    split: &mut SpannerCdcSplit,
    split_id: &SplitId,
    record_tx: &mpsc::Sender<TaggedPartitionRecord>,
    child_discovery_tx: &tokio::sync::mpsc::UnboundedSender<SpannerCdcSplit>,
    change_stream_name: &str,
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
        if record_tx.is_closed() {
            return Ok(());
        }

        let change_records: Vec<ChangeStreamRecord> = row
            .column(0)
            .map_err(|e| anyhow::anyhow!("failed to get ChangeRecord column: {}", e))?;

        for record in change_records {
            for data_change in &record.data_change_record {
                tracing::debug!(split_id = %split_id, table_name = %data_change.table_name, commit_time = ?data_change.commit_time(), mod_count = data_change.mods.len(), "received data change");
                split.advance_offset(data_change.commit_time());

                let mut data_msgs = Vec::new();
                for modification in &data_change.mods {
                    let tagged = TaggedChangeRecord {
                        split_id: split_id.clone(),
                        database_name: database_name.to_owned(),
                        data_change: data_change.clone(),
                        modification: modification.clone(),
                    };
                    let mut msg = SourceMessage::from(tagged);
                    msg.offset = make_offset_string(split);
                    data_msgs.push(msg);
                }

                let tagged = TaggedPartitionRecord {
                    partition_token: split.partition_token.clone(),
                    record: PartitionRecord::DataChange {
                        commit_ts: data_change.commit_time(),
                        table_name: data_change.table_name.clone(),
                        column_types: data_change.column_types.clone(),
                        split_id: split_id.clone(),
                        database_name: database_name.to_string(),
                        offset: make_offset_string(split),
                        data_msgs,
                    },
                };
                if record_tx.send(tagged).await.is_err() {
                    return Ok(());
                }
            }

            // Heartbeats advance the partition watermark even when no data changes
            // are occurring. Sent to the ReorderBuffer so the global watermark can progress.
            for heartbeat in &record.heartbeat_record {
                tracing::debug!(split_id = %split_id, heartbeat_time = ?heartbeat.heartbeat_time(), "received heartbeat");
                split.advance_offset(heartbeat.heartbeat_time());

                let hb_msg = SourceMessage {
                    key: None,
                    payload: None,
                    offset: make_offset_string(split),
                    split_id: split_id.clone(),
                    meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
                        String::new(),
                        (heartbeat.heartbeat_time().unix_timestamp_nanos() / 1_000_000) as i64,
                        cdc_message::CdcMessageType::Heartbeat,
                        SourceType::Unspecified,
                    )),
                };
                let tagged = TaggedPartitionRecord {
                    partition_token: split.partition_token.clone(),
                    record: PartitionRecord::Heartbeat {
                        commit_ts: heartbeat.heartbeat_time(),
                        msg: hb_msg,
                    },
                };
                if record_tx.send(tagged).await.is_err() {
                    return Ok(());
                }
            }

            // Child partition discovery
            for cpr in &record.child_partitions_record {
                let start_time = cpr.start_time();
                for cp in &cpr.child_partitions {
                    let child_split = SpannerCdcSplit::new_child(
                        cp.token.clone(),
                        cp.parent_partition_tokens.clone(),
                        start_time,
                        change_stream_name.to_string(),
                        0,
                    );
                    let _ = child_discovery_tx.send(child_split);
                }
            }
        }
    }

    tracing::info!(%split_id, final_offset = ?split.offset, "change stream result set exhausted");
    Ok(())
}

// ---------------------------------------------------------------------------
// Offset / message helpers
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
    serde_json::to_string(&cdc_offset).unwrap_or_else(|_| split.offset_as_micros().to_string())
}

/// Control message for child partition lifecycle (finished only).
///
/// Sent with `payload: None` as a heartbeat-type message. These travel through
/// the plain parser like Debezium heartbeat NULL-rows — the executor's
/// `update_offsets_from_chunk` picks up the offset for split state, and the
/// resulting chunk row has only offset/split metadata (no user data).
fn make_partition_control_message(
    split_id: &SplitId,
    partition_token: &str,
    parent_tokens: &[String],
    offset_micros: i64,
    is_finished: bool,
) -> SourceMessage {
    let mut spanner_offset = crate::source::cdc::external::spanner::SpannerOffset::with_partition(
        offset_micros,
        Some(partition_token.to_string()),
        parent_tokens.to_vec(),
        offset_micros,
        String::new(),
        0,
    );
    if is_finished {
        spanner_offset.mark_finished();
    }
    let cdc_offset = crate::source::cdc::external::CdcOffset::Spanner(spanner_offset);
    SourceMessage {
        key: None,
        payload: None,
        offset: serde_json::to_string(&cdc_offset).unwrap(),
        split_id: split_id.clone(),
        meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
            String::new(),
            offset_micros / 1000,
            cdc_message::CdcMessageType::Heartbeat,
            SourceType::Unspecified,
        )),
    }
}

fn make_root_finished_message(split_id: &SplitId, final_offset_micros: i64) -> SourceMessage {
    let mut spanner_offset =
        crate::source::cdc::external::spanner::SpannerOffset::new(final_offset_micros);
    spanner_offset.mark_finished();
    let cdc_offset = crate::source::cdc::external::CdcOffset::Spanner(spanner_offset);
    SourceMessage {
        key: None,
        payload: None,
        offset: serde_json::to_string(&cdc_offset).unwrap(),
        split_id: split_id.clone(),
        meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
            String::new(),
            final_offset_micros / 1000,
            cdc_message::CdcMessageType::Heartbeat,
            SourceType::Unspecified,
        )),
    }
}
