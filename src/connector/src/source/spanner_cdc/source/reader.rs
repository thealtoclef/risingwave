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
//! 1. Tracks per-partition offsets (data records + heartbeats)
//! 2. Computes a watermark = min(all partition offsets)
//! 3. Buffers records and emits them in commit-ts order only when the watermark
//!    has advanced past their timestamp
//!
//! This ensures schema changes are always seen in commit-ts order, making it
//! impossible for a lagging partition to regress the tracked schema.

use std::collections::HashMap;

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
use crate::source::spanner_cdc::split::PartitionState;
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
        let checkpointed_offset = splits
            .iter()
            .find(|s| s.index == source_id)
            .map(|s| s.offset_as_micros())
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
}

/// Tagged wrapper so the ReorderBuffer knows which partition a record came from.
struct TaggedPartitionRecord {
    partition_token: Option<String>,
    record: PartitionRecord,
}

async fn run_reader(ctx: ReaderContext, tx: mpsc::Sender<Vec<SourceMessage>>) -> Result<()> {
    let mut partition_streams: FuturesUnordered<tokio::task::JoinHandle<Result<PartitionResult>>> =
        FuturesUnordered::new();

    // Local HashMap for dedup and parent coordination. Not used for recovery.
    let mut partition_progress: HashMap<String, PartitionState> = HashMap::new();

    // Children waiting for their parents to finish.  Kept as a simple Vec;
    // no priority scheduling needed since all children start immediately
    // when their parents complete.
    let mut waiting_children: Vec<SpannerCdcSplit> = Vec::new();

    // Track whether the root partition (token=None) has finished.
    // The root is not tracked in partition_progress (which uses String keys),
    // so we need a separate flag for parents_all_finished.
    let mut root_finished: bool = false;

    let (child_discovery_tx, mut child_discovery_rx) =
        tokio::sync::mpsc::unbounded_channel::<SpannerCdcSplit>();

    // Shared record channel: partition tasks → ReorderBuffer
    let (record_tx, mut record_rx) = mpsc::channel::<TaggedPartitionRecord>(DEFAULT_CHANNEL_SIZE);

    let split_id = SplitId::from(ctx.source_id.to_string());
    let root_offset = ctx
        .checkpointed_offset
        .unwrap_or_else(OffsetDateTime::now_utc);
    let mut reorder_buf = ReorderBuffer::new();

    tracing::info!(starting_offset = ?root_offset, "starting Spanner CDC reader with root partition");

    let root_split =
        SpannerCdcSplit::new_root(ctx.change_stream_name.clone(), ctx.source_id, root_offset);
    reorder_buf.handle(ReorderBufferEvent::PartitionStarted {
        token: root_split.partition_token.clone(),
        start_ts: root_offset,
    });

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
            let mut remaining = reorder_buf.drain();
            stamp_watermark(&mut remaining, &reorder_buf);
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
            biased;
            result = partition_streams.next() => {
                match result {
                    Some(Ok(Ok(pr))) => {
                        reorder_buf.handle(ReorderBufferEvent::PartitionFinished {
                            token: pr.partition_token.clone(),
                        });

                        if let Some(ref token) = pr.partition_token {
                            if let Some(state) = partition_progress.get_mut(token) {
                                *state = PartitionState::Finished;
                            }
                        } else {
                            // Root partition (token=None) finished
                            root_finished = true;
                        }

                        // Drain child_discovery_rx (non-blocking) to collect
                        // any children discovered while the parent was running.
                        while let Ok(split) = child_discovery_rx.try_recv() {
                            if let Some(ref token) = split.partition_token {
                                if !register_child(&mut partition_progress, token.clone()) {
                                    continue;
                                }
                            }
                            waiting_children.push(split);
                        }

                        // Start ALL children whose parents are ALL finished.
                        // Registration in the reorder buffer is atomic — all
                        // siblings get PartitionStarted before any task is
                        // spawned, so the watermark pins at start_ts until
                        // all siblings advance past it.
                        let ready: Vec<SpannerCdcSplit> = waiting_children
                            .drain(..)
                            .collect();
                        let mut still_waiting = Vec::new();
                        for child in ready {
                            if parents_all_finished(&child.parent_partition_tokens, &partition_progress, root_finished) {
                                let start_ts = child.offset.unwrap_or(root_offset);
                                reorder_buf.handle(ReorderBufferEvent::PartitionStarted {
                                    token: child.partition_token.clone(),
                                    start_ts,
                                });
                                if let Some(ref token) = child.partition_token {
                                    set_running(&mut partition_progress, token);
                                }
                                spawn_partition_task(
                                    &ctx,
                                    child,
                                    &split_id,
                                    &mut partition_streams,
                                    child_discovery_tx.clone(),
                                    &record_tx,
                                );
                            } else {
                                still_waiting.push(child);
                            }
                        }
                        waiting_children = still_waiting;
                    }
                    Some(Ok(Err(e))) => return Err(e),
                    Some(Err(e)) => {
                        return Err(ConnectorError::from(anyhow::anyhow!("partition task panicked: {}", e)));
                    }
                    None => {
                        tracing::warn!(%split_id, pending = waiting_children.len(), "all partitions finished");
                        break;
                    }
                }
            }

            Some(child) = child_discovery_rx.recv() => {
                if let Some(ref token) = child.partition_token {
                    if !register_child(&mut partition_progress, token.clone()) {
                        tracing::debug!(token = %token, "duplicate child partition, skipping");
                        continue;
                    }
                }

                // If all parents are finished, start immediately.
                // Otherwise, hold in waiting_children — the parent's
                // offset naturally pins the watermark at >= start_ts.
                if parents_all_finished(&child.parent_partition_tokens, &partition_progress, root_finished) {
                    let start_ts = child.offset.unwrap_or(root_offset);
                    reorder_buf.handle(ReorderBufferEvent::PartitionStarted {
                        token: child.partition_token.clone(),
                        start_ts,
                    });
                    if let Some(ref token) = child.partition_token {
                        set_running(&mut partition_progress, token);
                    }
                    spawn_partition_task(
                        &ctx,
                        child,
                        &split_id,
                        &mut partition_streams,
                        child_discovery_tx.clone(),
                        &record_tx,
                    );
                } else {
                    waiting_children.push(child);
                }
            }

            Some(tagged) = record_rx.recv() => {
                reorder_buf.handle(ReorderBufferEvent::Record {
                    partition_token: tagged.partition_token,
                    record: tagged.record,
                });
                // Batch: drain remaining messages without blocking
                while let Ok(tagged) = record_rx.try_recv() {
                    reorder_buf.handle(ReorderBufferEvent::Record {
                        partition_token: tagged.partition_token,
                        record: tagged.record,
                    });
                }
            }
        }

        // Drain and emit after every event — single drain point, no data loss
        let mut batch = reorder_buf.drain();
        stamp_watermark(&mut batch, &reorder_buf);
        if !batch.is_empty() && tx.send(batch).await.is_err() {
            break;
        }
    }

    // Safety net: drain anything still buffered (e.g., after a `break` from
    // a send error). Partition tasks have already exited or will exit when
    // `record_tx` is dropped.
    let mut remaining = reorder_buf.drain();
    stamp_watermark(&mut remaining, &reorder_buf);
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
fn register_child(partition_progress: &mut HashMap<String, PartitionState>, token: String) -> bool {
    use std::collections::hash_map::Entry;
    match partition_progress.entry(token) {
        Entry::Vacant(e) => {
            e.insert(PartitionState::Pending);
            true
        }
        Entry::Occupied(_) => false,
    }
}

fn parents_all_finished(
    parent_tokens: &[String],
    partition_progress: &HashMap<String, PartitionState>,
    root_finished: bool,
) -> bool {
    parent_tokens.iter().all(|p| {
        match partition_progress.get(p) {
            Some(state) => *state == PartitionState::Finished,
            None => {
                // Unknown token — assume it's the root partition's
                // Spanner-assigned token (root has token=None internally).
                if !root_finished {
                    tracing::debug!(
                        parent_token = %p,
                        "unknown parent token, assuming root partition (not yet finished)"
                    );
                }
                root_finished
            }
        }
    })
}

fn set_running(partition_progress: &mut HashMap<String, PartitionState>, token: &str) {
    if let Some(state) = partition_progress.get_mut(token) {
        *state = PartitionState::Running;
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
        read_partition(
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
        .await
        .map(|()| PartitionResult { partition_token })
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
) -> Result<()> {
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
    stmt.add_param("end_timestamp", &Option::<OffsetDateTime>::None);
    if let Some(ref token) = split.partition_token {
        stmt.add_param("partition_token", token);
    } else {
        stmt.add_param("partition_token", &Option::<String>::None);
    }
    stmt.add_param("heartbeat_milliseconds", &heartbeat_interval_ms);

    tracing::info!(%split_id, %start_ts, partition_token = ?split.partition_token, "change stream query starting");

    // Edge case: retry_attempts=0 means no retries, execute once and return
    if retry_attempts == 0 {
        if record_tx.is_closed() {
            return Ok(());
        }
        stmt.add_param("start_timestamp", &start_ts);
        return execute_query(
            &client,
            &database_name,
            &stmt,
            &mut split,
            &split_id,
            &record_tx,
            &child_discovery_tx,
            &change_stream_name,
        )
        .await;
    }

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
            return Ok(());
        }
        // Resume from the latest successfully processed position (falls back
        // to original start_ts on first attempt).
        let resume_ts = split
            .offset
            .expect("offset validated at entry and only set to Some by advance_offset");
        stmt.add_param("start_timestamp", &resume_ts);
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
            Ok(()) => return Ok(()),
            Err(e) => {
                let will_retry = attempt + 1 < retry_attempts as usize;
                tracing::warn!(%split_id, attempt = attempt + 1, max_attempts = retry_attempts, ?delay, error = %e, resume_ts = ?resume_ts, will_retry, "query failed");
                last_error = Some(e);
                if !will_retry {
                    break;
                }
                tokio::time::sleep(delay).await;
            }
        }
    }
    Err(last_error.expect("loop body sets last_error on each failed attempt"))
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
                    // Placeholder offset — overwritten by stamp_watermark after drain
                    msg.offset = String::new();
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
                        offset: String::new(), // overwritten by stamp_watermark after drain
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
                    offset: String::new(), // overwritten by stamp_watermark after drain
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

/// Build an offset string from the ReorderBuffer's watermark timestamp.
/// This is the correct "consumed up to" position — the executor receives a
/// single monotonically advancing offset, like other CDC sources.
fn make_watermark_offset_string(watermark_micros: i64) -> String {
    let spanner_offset =
        crate::source::cdc::external::spanner::SpannerOffset::new(watermark_micros);
    let cdc_offset = crate::source::cdc::external::CdcOffset::Spanner(spanner_offset);
    serde_json::to_string(&cdc_offset).unwrap_or_else(|_| watermark_micros.to_string())
}

/// Stamp all messages in a batch with the ReorderBuffer's watermark.
/// The executor's split tracks a single offset (like other CDC sources),
/// so every message must carry the watermark, not per-partition offsets.
fn stamp_watermark(batch: &mut [SourceMessage], reorder_buf: &ReorderBuffer) {
    let wm_micros = reorder_buf
        .last_emitted_watermark()
        .map(|wm| (wm.unix_timestamp_nanos() / 1000) as i64)
        .unwrap_or(0);
    if wm_micros > 0 {
        let offset_str = make_watermark_offset_string(wm_micros);
        for msg in batch.iter_mut() {
            msg.offset = offset_str.clone();
        }
    } else if !batch.is_empty() {
        tracing::error!("stamp_watermark called with wm_micros=0 on non-empty batch — this indicates a logic error in drain/watermark coordination");
    }
}
