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
//! ## Partition model
//!
//! Each partition reads its key range independently, sending `SourceMessage`s
//! directly through the shared `mpsc` channel. Per-key ordering is guaranteed
//! by Spanner's non-overlapping key ranges + parent-before-child spawning.
//!
//! The main loop manages partition lifecycle only:
//! - Partition completions → spawn next children from priority queue
//! - Child discovery → register + enqueue
//!
//! ## Watermark & checkpoint
//!
//! The watermark = min(offset) across all registered, un-finished partitions.
//! It is the safe recovery point. On restart, the root query restarts from
//! the watermark — all partitions are re-discovered from scratch.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use futures_async_stream::try_stream;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::statement::Statement;
use risingwave_common::bail;
use risingwave_common::ensure;
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_pb::connector_service::{SourceType, cdc_message};
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

use super::TaggedChangeRecord;
use crate::error::{ConnectorError, ConnectorResult as Result};
use crate::parser::ParserConfig;
use crate::source::cdc::DebeziumCdcMeta;
use crate::source::spanner_cdc::schema_track::SchemaTracker;
use crate::source::spanner_cdc::types::ChangeStreamRecord;
use crate::source::spanner_cdc::{SpannerCdcProperties, SpannerCdcSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SourceMeta, SplitId, SplitReader,
    into_chunk_stream,
};

const DEFAULT_CHANNEL_SIZE: usize = 16;

/// Spanner CDC split reader — same pattern as Debezium's `CdcSplitReader`.
pub struct SpannerCdcSplitReader {
    /// Receives batches of `SourceMessage` from the background reader task.
    rx: mpsc::Receiver<Vec<SourceMessage>>,
    parser_config: ParserConfig,
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

        let checkpointed_offset = splits
            .iter()
            .find(|s| s.index == source_id)
            .and_then(|s| s.offset);

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
        GLOBAL_ERROR_METRICS.user_source_error.report([
            "spanner_cdc_source".to_owned(),
            source_id,
            self.source_ctx.source_name.clone(),
            self.source_ctx.fragment_id.to_string(),
        ]);
        bail!("Spanner CDC reader channel closed");
    }
}

// ---------------------------------------------------------------------------
// Background reader task (equivalent to Debezium's JNI thread)
// ---------------------------------------------------------------------------

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

/// Partition offset tracking with O(1) watermark.
///
/// Uses two maps behind a single lock:
/// - `offsets`: token → current offset (O(1) lookup)
/// - `counts`: offset → count of partitions at that offset (O(1) watermark via first key)
///
/// Shared between the main loop and partition tasks via `Arc`.
/// Partition tasks update their offset as they process records.
/// The main loop computes the watermark from this map.
///
/// A partition's entry is removed when it finishes, so the watermark
/// only reflects un-finished partitions.
struct PartitionOffsets {
    inner: std::sync::Mutex<PartitionOffsetsInner>,
}

struct PartitionOffsetsInner {
    offsets: HashMap<Option<String>, OffsetDateTime>,
    counts: BTreeMap<OffsetDateTime, usize>,
}

impl PartitionOffsets {
    fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(PartitionOffsetsInner {
                offsets: HashMap::new(),
                counts: BTreeMap::new(),
            }),
        }
    }

    /// Register a partition with its start offset.
    fn register(&self, token: Option<String>, start_ts: OffsetDateTime) {
        let mut inner = self.inner.lock().unwrap();
        inner.offsets.insert(token, start_ts);
        *inner.counts.entry(start_ts).or_insert(0) += 1;
    }

    /// Update a partition's offset (called by partition tasks).
    fn update(&self, token: &Option<String>, offset: OffsetDateTime) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.offsets.get_mut(token) {
            if offset > *entry {
                let old = *entry;
                *entry = offset;

                // Update counts: decrement old, increment new.
                if let Some(count) = inner.counts.get_mut(&old) {
                    *count -= 1;
                    if *count == 0 {
                        inner.counts.remove(&old);
                    }
                }
                *inner.counts.entry(offset).or_insert(0) += 1;
            }
        }
    }

    /// Remove a finished partition.
    fn remove(&self, token: &Option<String>) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(offset) = inner.offsets.remove(token) {
            if let Some(count) = inner.counts.get_mut(&offset) {
                *count -= 1;
                if *count == 0 {
                    inner.counts.remove(&offset);
                }
            }
        }
    }

    /// Watermark = min(offset) across all registered (un-finished) partitions. O(1).
    fn watermark(&self) -> Option<OffsetDateTime> {
        self.inner.lock().unwrap().counts.keys().next().copied()
    }
}

/// Main reader loop — equivalent to Debezium's JNI thread that reads from the WAL.
async fn run_reader(ctx: ReaderContext, tx: mpsc::Sender<Vec<SourceMessage>>) -> Result<()> {
    let mut partition_streams: FuturesUnordered<tokio::task::JoinHandle<Result<PartitionResult>>> =
        FuturesUnordered::new();

    // Ready partitions (parents all finished) — HashMap for O(1) duplicate detection.
    let mut ready_pool: HashMap<Option<String>, SpannerCdcSplit> = HashMap::new();
    let mut active_count: usize = 0;

    // Deferred children — HashMap for O(1) duplicate detection by token.
    let mut deferred: HashMap<Option<String>, SpannerCdcSplit> = HashMap::new();

    // Index: parent_token → child tokens waiting for that parent.
    // Enables O(1) lookup of affected children when a partition finishes,
    // instead of scanning all deferred children.
    let mut children_by_parent: HashMap<Option<String>, Vec<Option<String>>> = HashMap::new();

    // Track finished partitions. Entries are never removed — bounded by total partition
    // count (tens to low hundreds, ~48 bytes each). This is correct for multi-parent
    // children: a child with parents [P1, P2] needs both entries in `finished` to be
    // promoted, and P1's entry must survive until P2 also finishes.
    let mut finished: HashMap<Option<String>, bool> = HashMap::new();

    let (child_discovery_tx, mut child_discovery_rx) =
        tokio::sync::mpsc::unbounded_channel::<SpannerCdcSplit>();

    // Shared partition offset tracking.
    let offsets = Arc::new(PartitionOffsets::new());

    // Shared schema tracker — deduplicates schema change events across partitions.
    // Lock fires on every data change record, but hot path is ~100ns (HashMap.get + compare).
    let shared_schema = Arc::new(std::sync::Mutex::new(SchemaTracker::new()));

    let split_id = SplitId::from(ctx.source_id.to_string());
    let root_offset = ctx
        .checkpointed_offset
        .unwrap_or_else(OffsetDateTime::now_utc);

    tracing::info!(starting_offset = ?root_offset, "starting Spanner CDC reader");

    // Spawn root partition.
    let root_split =
        SpannerCdcSplit::new_root(ctx.change_stream_name.clone(), ctx.source_id, root_offset);
    let root_token = root_split.partition_token.clone();
    offsets.register(root_token.clone(), root_offset);
    finished.insert(root_token, false);
    spawn_partition_task(
        &ctx,
        root_split,
        &split_id,
        &offsets,
        &shared_schema,
        &tx,
        &mut partition_streams,
        child_discovery_tx.clone(),
    );
    active_count += 1;

    // Main event loop — partition lifecycle management only.
    // Records flow directly from partition tasks → tx.
    loop {
        if tx.is_closed() {
            tracing::info!("reader channel closed, stopping");
            break;
        }

        // Biased: prioritize partition completions (data progress) over child discovery.
        tokio::select! {
                biased;

            result = partition_streams.next() => {
                match result {
                    Some(Ok(Ok(pr))) => {
                        // Partition finished — remove from offsets (excludes from watermark).
                        offsets.remove(&pr.partition_token);
                        let finished_token = pr.partition_token.clone();
                        if let Some(ref token) = finished_token {
                            finished.insert(Some(token.clone()), true);
                        } else {
                            finished.insert(None, true);
                        }

                        // Promote only children waiting for this specific parent.
                        promote_children_of(
                            &finished_token,
                            &mut deferred,
                            &mut ready_pool,
                            &mut children_by_parent,
                            &finished,
                        );

                        active_count -= 1;
                        spawn_from_pool(
                            &mut ready_pool,
                            &mut active_count,
                            &ctx,
                            &split_id,
                            &offsets,
                            &shared_schema,
                            &tx,
                            &mut partition_streams,
                            &child_discovery_tx,
                        );
                    }
                    Some(Ok(Err(e))) => return Err(e),
                    Some(Err(e)) => {
                        return Err(ConnectorError::from(anyhow::anyhow!(
                            "partition task panicked: {}", e
                        )));
                    }
                    None => {
                        // All tasks done — try pending children.
                        spawn_from_pool(
                            &mut ready_pool,
                            &mut active_count,
                            &ctx,
                            &split_id,
                            &offsets,
                            &shared_schema,
                            &tx,
                            &mut partition_streams,
                            &child_discovery_tx,
                        );
                        if ready_pool.is_empty() && deferred.is_empty() {
                            tracing::info!(%split_id, "all partitions finished");
                            break;
                        }
                        if active_count == 0 {
                            return Err(ConnectorError::from(anyhow::anyhow!(
                                "deadlock: no active tasks but {} children still waiting",
                                deferred.len()
                            )));
                        }
                    }
                }
            }

            Some(child) = child_discovery_rx.recv() => {
                // Batch-drain pending siblings.
                let mut batch = vec![child];
                while let Ok(next) = child_discovery_rx.try_recv() {
                    batch.push(next);
                }
                for child in batch {
                    let token = child.partition_token.clone();
                    // O(1) duplicate detection via HashMap::contains_key.
                    if finished.contains_key(&token)
                        || deferred.contains_key(&token)
                        || ready_pool.contains_key(&token)
                    {
                        continue;
                    }
                    tracing::debug!(
                        %split_id,
                        token = ?token,
                        parents = ?child.parent_partition_tokens,
                        "discovered child partition"
                    );
                    offsets.register(
                        token.clone(),
                        child.offset.expect("new_child always sets offset"),
                    );
                    finished.insert(token.clone(), false);

                    // Index this child by its parents for O(1) lookup when parents finish.
                    for parent in &child.parent_partition_tokens {
                        children_by_parent
                            .entry(Some(parent.clone()))
                            .or_default()
                            .push(token.clone());
                    }

                    if parents_all_finished(&child.parent_partition_tokens, &finished) {
                        ready_pool.insert(token, child);
                    } else {
                        deferred.insert(token, child);
                    }
                }
                spawn_from_pool(
                    &mut ready_pool,
                    &mut active_count,
                    &ctx,
                    &split_id,
                    &offsets,
                    &shared_schema,
                    &tx,
                    &mut partition_streams,
                    &child_discovery_tx,
                );
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Partition coordination
// ---------------------------------------------------------------------------

fn parents_all_finished(
    parent_tokens: &[String],
    finished: &HashMap<Option<String>, bool>,
) -> bool {
    // Root partition has no parent tokens — check if root (token=None) finished.
    if parent_tokens.is_empty() {
        return finished.get(&None).copied().unwrap_or(false);
    }
    parent_tokens
        .iter()
        .all(|p| finished.get(&Some(p.clone())).copied().unwrap_or(false))
}

/// Promote deferred children of a specific finished parent.
///
/// Instead of scanning all deferred children (O(n)), looks up only the children
/// waiting for this parent via `children_by_parent` index (O(1) lookup + O(k)
/// where k = children of this parent).
fn promote_children_of(
    finished_token: &Option<String>,
    deferred: &mut HashMap<Option<String>, SpannerCdcSplit>,
    ready_pool: &mut HashMap<Option<String>, SpannerCdcSplit>,
    children_by_parent: &mut HashMap<Option<String>, Vec<Option<String>>>,
    finished: &HashMap<Option<String>, bool>,
) {
    // Take the children list for this parent (avoids borrow conflict).
    let child_tokens = match children_by_parent.remove(finished_token) {
        Some(tokens) => tokens,
        None => return,
    };

    let mut unprocessed = Vec::new();

    for child_token in child_tokens {
        // Child may have already been promoted by another parent finishing.
        let child = match deferred.remove(&child_token) {
            Some(c) => c,
            None => continue,
        };

        if parents_all_finished(&child.parent_partition_tokens, finished) {
            ready_pool.insert(child_token.clone(), child);
        } else {
            // Still waiting for other parents — put back.
            deferred.insert(child_token.clone(), child);
            unprocessed.push(child_token);
        }
    }

    // Re-insert remaining children for this parent, or clean up.
    if !unprocessed.is_empty() {
        children_by_parent.insert(finished_token.clone(), unprocessed);
    }
}

fn spawn_from_pool(
    ready_pool: &mut HashMap<Option<String>, SpannerCdcSplit>,
    active_count: &mut usize,
    ctx: &ReaderContext,
    split_id: &SplitId,
    offsets: &Arc<PartitionOffsets>,
    shared_schema: &Arc<std::sync::Mutex<SchemaTracker>>,
    tx: &mpsc::Sender<Vec<SourceMessage>>,
    partition_streams: &mut FuturesUnordered<tokio::task::JoinHandle<Result<PartitionResult>>>,
    child_discovery_tx: &tokio::sync::mpsc::UnboundedSender<SpannerCdcSplit>,
) {
    for (_, split) in ready_pool.drain() {
        spawn_partition_task(
            ctx,
            split,
            split_id,
            offsets,
            shared_schema,
            tx,
            partition_streams,
            child_discovery_tx.clone(),
        );
        *active_count += 1;
    }
}

// ---------------------------------------------------------------------------
// Partition task management
// ---------------------------------------------------------------------------

fn spawn_partition_task(
    ctx: &ReaderContext,
    split: SpannerCdcSplit,
    split_id: &SplitId,
    offsets: &Arc<PartitionOffsets>,
    shared_schema: &Arc<std::sync::Mutex<SchemaTracker>>,
    tx: &mpsc::Sender<Vec<SourceMessage>>,
    partition_streams: &mut FuturesUnordered<tokio::task::JoinHandle<Result<PartitionResult>>>,
    child_discovery_tx: tokio::sync::mpsc::UnboundedSender<SpannerCdcSplit>,
) {
    let client = ctx.client.clone();
    let database_name = ctx.database_name.clone();
    let change_stream_name = ctx.change_stream_name.clone();
    let heartbeat_interval_ms = ctx.heartbeat_interval_ms;
    let retry_attempts = ctx.retry_attempts;
    let retry_backoff = ctx.retry_backoff;
    let retry_backoff_max_delay_ms = ctx.retry_backoff_max_delay_ms;
    let retry_backoff_factor = ctx.retry_backoff_factor;
    let offsets = offsets.clone();
    let shared_schema = shared_schema.clone();
    let tx = tx.clone();
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
            offsets,
            shared_schema,
            tx,
            child_discovery_tx,
        )
        .await
        .map(|()| PartitionResult { partition_token })
    }));
}

// ---------------------------------------------------------------------------
// Change stream query execution
// ---------------------------------------------------------------------------

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
    offsets: Arc<PartitionOffsets>,
    shared_schema: Arc<std::sync::Mutex<SchemaTracker>>,
    tx: mpsc::Sender<Vec<SourceMessage>>,
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

    if retry_attempts == 0 {
        stmt.add_param("start_timestamp", &start_ts);
        return execute_query(
            &client,
            &database_name,
            &stmt,
            &mut split,
            &split_id,
            &offsets,
            &shared_schema,
            &tx,
            &child_discovery_tx,
            &change_stream_name,
        )
        .await;
    }

    let retry_strategy = ExponentialBackoff::from_millis(retry_backoff.as_millis() as u64)
        .max_delay(tokio::time::Duration::from_millis(retry_backoff_max_delay_ms))
        .factor(retry_backoff_factor)
        .take(retry_attempts as usize)
        .map(jitter);

    let mut last_error = None;

    for (attempt, delay) in retry_strategy.enumerate() {
        let resume_ts = split
            .offset
            .expect("offset validated at entry and only advanced by advance_offset");
        stmt.add_param("start_timestamp", &resume_ts);
        match execute_query(
            &client,
            &database_name,
            &stmt,
            &mut split,
            &split_id,
            &offsets,
            &shared_schema,
            &tx,
            &child_discovery_tx,
            &change_stream_name,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(e) => {
                let will_retry = attempt + 1 < retry_attempts as usize;
                tracing::warn!(
                    %split_id,
                    attempt = attempt + 1,
                    max_attempts = retry_attempts,
                    ?delay,
                    error = %e,
                    resume_ts = ?resume_ts,
                    will_retry,
                    "query failed"
                );
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
    offsets: &PartitionOffsets,
    shared_schema: &std::sync::Mutex<SchemaTracker>,
    tx: &mpsc::Sender<Vec<SourceMessage>>,
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
        if tx.is_closed() {
            return Ok(());
        }

        let change_records: Vec<ChangeStreamRecord> = row
            .column(0)
            .map_err(|e| anyhow::anyhow!("failed to get ChangeRecord column: {}", e))?;

        for record in change_records {
            let mut messages = Vec::new();

            for data_change in &record.data_change_record {
                tracing::debug!(
                    %split_id,
                    table_name = %data_change.table_name,
                    commit_time = ?data_change.commit_time(),
                    mod_count = data_change.mods.len(),
                    "received data change"
                );

                let commit_ts = data_change.commit_time();
                split.advance_offset(commit_ts);
                offsets.update(&split.partition_token, commit_ts);
                // Use watermark (min of all un-finished partitions) for checkpoint offset.
                let wm = offsets
                    .watermark()
                    .expect("partition active, watermark must exist");
                let offset_str = make_offset_string((wm.unix_timestamp_nanos() / 1000) as i64);

                // Schema evolution: emit schema change before data records,
                // mimicking Debezium's Relation messages that precede DML events.
                let schema_payload = shared_schema.lock().unwrap().check_and_evolve(
                    &data_change.table_name,
                    &data_change.column_types,
                    commit_ts,
                );
                if let Some(schema_payload) = schema_payload {
                    if !messages.is_empty() {
                        if tx.send(std::mem::take(&mut messages)).await.is_err() {
                            return Ok(());
                        }
                    }
                    let schema_msg = make_schema_change_msg(
                        split_id,
                        schema_payload.json,
                        data_change,
                        database_name,
                        &offset_str,
                    );
                    if tx.send(vec![schema_msg]).await.is_err() {
                        return Ok(());
                    }
                }

                for modification in &data_change.mods {
                    let tagged = TaggedChangeRecord {
                        split_id: split_id.clone(),
                        database_name: database_name.to_owned(),
                        data_change: data_change.clone(),
                        modification: modification.clone(),
                    };
                    let mut msg = SourceMessage::from(tagged);
                    msg.offset = offset_str.clone();
                    messages.push(msg);
                }
            }

            // Heartbeats
            for heartbeat in &record.heartbeat_record {
                tracing::debug!(
                    %split_id,
                    heartbeat_time = ?heartbeat.heartbeat_time(),
                    "received heartbeat"
                );
                let hb_ts = heartbeat.heartbeat_time();
                split.advance_offset(hb_ts);
                offsets.update(&split.partition_token, hb_ts);
                // Use watermark (min of all un-finished partitions) for checkpoint offset.
                let wm = offsets
                    .watermark()
                    .expect("partition active, watermark must exist");
                let offset_str = make_offset_string((wm.unix_timestamp_nanos() / 1000) as i64);

                messages.push(SourceMessage {
                    key: None,
                    payload: None,
                    offset: offset_str,
                    split_id: split_id.clone(),
                    meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
                        String::new(),
                        (hb_ts.unix_timestamp_nanos() / 1_000_000) as i64,
                        cdc_message::CdcMessageType::Heartbeat,
                        SourceType::Unspecified,
                    )),
                });
            }

            // Send batch
            if !messages.is_empty() {
                tracing::debug!(%split_id, count = messages.len(), "sending CDC messages");
                if tx.send(messages).await.is_err() {
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
// Helpers
// ---------------------------------------------------------------------------

fn make_offset_string(offset_micros: i64) -> String {
    let spanner_offset =
        crate::source::cdc::external::spanner::SpannerOffset::new(offset_micros);
    let cdc_offset = crate::source::cdc::external::CdcOffset::Spanner(spanner_offset);
    serde_json::to_string(&cdc_offset).unwrap_or_else(|_| offset_micros.to_string())
}

fn make_schema_change_msg(
    split_id: &SplitId,
    payload: Vec<u8>,
    data_change: &crate::source::spanner_cdc::types::DataChangeRecord,
    database_name: &str,
    offset_str: &str,
) -> SourceMessage {
    SourceMessage {
        key: None,
        payload: Some(payload.into()),
        offset: offset_str.to_owned(),
        split_id: split_id.clone(),
        meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new_with_database_name(
            data_change.table_name.clone(),
            (data_change.commit_time().unix_timestamp_nanos() / 1_000_000) as i64,
            cdc_message::CdcMessageType::SchemaChange,
            SourceType::Unspecified,
            Some(database_name.to_owned()),
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    /// Helper: create a child split with given token, parents, and offset.
    fn make_child(token: &str, parents: Vec<&str>, offset: OffsetDateTime) -> SpannerCdcSplit {
        SpannerCdcSplit::new_child(
            token.to_string(),
            parents.into_iter().map(String::from).collect(),
            offset,
            "test-stream".to_string(),
            0,
        )
    }

    // -----------------------------------------------------------------------
    // Multi-parent promotion: child stays deferred until ALL parents finish.
    // This is the core correctness property of the children_by_parent index.
    // -----------------------------------------------------------------------

    #[test]
    fn test_multi_parent_child_deferred_until_all_parents_done() {
        let ts = datetime!(2025-01-01 0:00 UTC);
        let child = make_child("C1", vec!["P1", "P2"], ts);

        let mut deferred = HashMap::new();
        deferred.insert(Some("C1".to_string()), child);

        let mut ready_pool: HashMap<Option<String>, SpannerCdcSplit> = HashMap::new();
        let mut children_by_parent = HashMap::new();
        children_by_parent.insert(Some("P1".to_string()), vec![Some("C1".to_string())]);
        children_by_parent.insert(Some("P2".to_string()), vec![Some("C1".to_string())]);

        let mut finished = HashMap::new();
        finished.insert(Some("P1".to_string()), true);
        finished.insert(Some("P2".to_string()), false);

        // P1 finishes — C1 must stay deferred (P2 not done).
        promote_children_of(
            &Some("P1".to_string()),
            &mut deferred,
            &mut ready_pool,
            &mut children_by_parent,
            &finished,
        );
        assert!(
            deferred.contains_key(&Some("C1".to_string())),
            "child must stay deferred until all parents finish"
        );
        assert!(ready_pool.is_empty());

        // P2 finishes — now C1 is promoted.
        finished.insert(Some("P2".to_string()), true);
        promote_children_of(
            &Some("P2".to_string()),
            &mut deferred,
            &mut ready_pool,
            &mut children_by_parent,
            &finished,
        );
        assert!(deferred.is_empty());
        assert!(ready_pool.contains_key(&Some("C1".to_string())));
    }

    // -----------------------------------------------------------------------
    // Finished entries are NEVER removed — proves the fix prevents deadlock.
    //
    // If finished[P1] were removed after P1's promote_children_of, then when
    // P2 finishes, parents_all_finished would see P1 missing → false → C1
    // stays deferred forever → deadlock.
    // -----------------------------------------------------------------------

    #[test]
    fn test_finished_entry_survives_promotion() {
        let ts = datetime!(2025-01-01 0:00 UTC);
        let child = make_child("C1", vec!["P1", "P2"], ts);
        let parent_tokens = child.parent_partition_tokens.clone();

        let mut deferred = HashMap::new();
        deferred.insert(Some("C1".to_string()), child);

        let mut ready_pool: HashMap<Option<String>, SpannerCdcSplit> = HashMap::new();
        let mut children_by_parent = HashMap::new();
        children_by_parent.insert(Some("P1".to_string()), vec![Some("C1".to_string())]);
        children_by_parent.insert(Some("P2".to_string()), vec![Some("C1".to_string())]);

        let mut finished = HashMap::new();
        finished.insert(Some("P1".to_string()), true);
        finished.insert(Some("P2".to_string()), false);

        // P1 finishes — C1 stays deferred.
        promote_children_of(
            &Some("P1".to_string()),
            &mut deferred,
            &mut ready_pool,
            &mut children_by_parent,
            &finished,
        );
        assert!(deferred.contains_key(&Some("C1".to_string())));
        // finished[P1] must still exist — this is the invariant the fix maintains.
        assert!(
            finished.contains_key(&Some("P1".to_string())),
            "finished entry must survive — removing it causes deadlock for multi-parent children"
        );

        // Verify: if we WERE to remove finished[P1], parents_all_finished would fail.
        // (This is what the old buggy code would do.)
        finished.remove(&Some("P1".to_string()));
        assert!(
            !parents_all_finished(&parent_tokens, &finished),
            "without finished[P1], parents_all_finished returns false → child stuck forever"
        );
    }

    // -----------------------------------------------------------------------
    // promote_children_of: idempotent when child already promoted by another parent.
    // -----------------------------------------------------------------------

    #[test]
    fn test_promote_idempotent_for_already_promoted_child() {
        let ts = datetime!(2025-01-01 0:00 UTC);
        let child = make_child("C1", vec!["P1", "P2"], ts);

        let mut deferred = HashMap::new();
        deferred.insert(Some("C1".to_string()), child);

        let mut ready_pool: HashMap<Option<String>, SpannerCdcSplit> = HashMap::new();
        let mut children_by_parent = HashMap::new();
        children_by_parent.insert(Some("P1".to_string()), vec![Some("C1".to_string())]);
        children_by_parent.insert(Some("P2".to_string()), vec![Some("C1".to_string())]);

        let mut finished = HashMap::new();
        finished.insert(Some("P1".to_string()), true);
        finished.insert(Some("P2".to_string()), true);

        // P1 promotes C1.
        promote_children_of(
            &Some("P1".to_string()),
            &mut deferred,
            &mut ready_pool,
            &mut children_by_parent,
            &finished,
        );
        assert_eq!(ready_pool.len(), 1);

        // P2 tries to promote C1 — already gone from deferred, no-op.
        promote_children_of(
            &Some("P2".to_string()),
            &mut deferred,
            &mut ready_pool,
            &mut children_by_parent,
            &finished,
        );
        assert_eq!(ready_pool.len(), 1, "must not duplicate already-promoted child");
    }

    // -----------------------------------------------------------------------
    // PartitionOffsets: watermark = min(offset) across all registered partitions.
    // -----------------------------------------------------------------------

    #[test]
    fn test_partition_offsets_watermark_is_min() {
        let offsets = PartitionOffsets::new();
        assert_eq!(offsets.watermark(), None);

        let t1 = datetime!(2025-01-01 0:00 UTC);
        let t2 = datetime!(2025-01-02 0:00 UTC);
        let t3 = datetime!(2025-01-03 0:00 UTC);

        offsets.register(Some("A".to_string()), t1);
        offsets.register(Some("B".to_string()), t3);
        assert_eq!(offsets.watermark(), Some(t1), "watermark = min(t1, t3) = t1");

        // Update A past B — watermark becomes B's offset.
        offsets.update(&Some("A".to_string()), t2);
        assert_eq!(offsets.watermark(), Some(t2), "watermark = min(t2, t3) = t2");

        // Remove B — watermark becomes A's offset.
        offsets.remove(&Some("B".to_string()));
        assert_eq!(offsets.watermark(), Some(t2), "watermark = t2 (only A left)");

        // Remove A — watermark gone.
        offsets.remove(&Some("A".to_string()));
        assert_eq!(offsets.watermark(), None);
    }

    #[test]
    fn test_partition_offsets_backward_update_ignored() {
        let offsets = PartitionOffsets::new();
        let t1 = datetime!(2025-01-01 0:00 UTC);
        let t2 = datetime!(2025-01-02 0:00 UTC);

        offsets.register(Some("A".to_string()), t2);
        offsets.update(&Some("A".to_string()), t1); // backward — must be ignored
        assert_eq!(
            offsets.watermark(),
            Some(t2),
            "backward update must not change offset"
        );
    }

    // -----------------------------------------------------------------------
    // Deferred HashMap dedup: child discovered from two parents is inserted
    // only once. The old Vec had no dedup — a child would be pushed twice,
    // potentially spawned twice. HashMap::contains_key gives O(1) check.
    // -----------------------------------------------------------------------

    #[test]
    fn test_deferred_dedup_prevents_duplicate_discovery() {
        let ts = datetime!(2025-01-01 0:00 UTC);

        let mut deferred: HashMap<Option<String>, SpannerCdcSplit> = HashMap::new();

        // Simulate child discovery from two parents.
        // In run_reader, the code checks: if deferred.contains_key(&token) { continue; }
        let child_from_p1 = make_child("C1", vec!["P1", "P2"], ts);
        let child_from_p2 = make_child("C1", vec!["P1", "P2"], ts);

        // First discovery — insert.
        let token = child_from_p1.partition_token.clone();
        if !deferred.contains_key(&token) {
            deferred.insert(token.clone(), child_from_p1);
        }
        assert_eq!(deferred.len(), 1);

        // Second discovery — duplicate, skipped.
        let token2 = child_from_p2.partition_token.clone();
        if !deferred.contains_key(&token2) {
            deferred.insert(token2, child_from_p2);
        }
        assert_eq!(deferred.len(), 1, "duplicate child must be skipped");

        // After both parents finish, child is promoted exactly once.
        let mut ready_pool: HashMap<Option<String>, SpannerCdcSplit> = HashMap::new();
        let mut children_by_parent = HashMap::new();
        children_by_parent.insert(Some("P1".to_string()), vec![Some("C1".to_string())]);
        children_by_parent.insert(Some("P2".to_string()), vec![Some("C1".to_string())]);

        let mut finished = HashMap::new();
        finished.insert(Some("P1".to_string()), true);
        finished.insert(Some("P2".to_string()), true);

        promote_children_of(
            &Some("P1".to_string()),
            &mut deferred,
            &mut ready_pool,
            &mut children_by_parent,
            &finished,
        );

        assert_eq!(ready_pool.len(), 1, "child promoted exactly once");
        assert!(deferred.is_empty());
    }

    // -----------------------------------------------------------------------
    // advance_offset: backward or equal timestamps must be silently ignored.
    // The function enforces monotonically increasing offsets.
    // -----------------------------------------------------------------------

    #[test]
    fn test_advance_offset_backward_ignored() {
        let ts = datetime!(2025-01-01 0:00 UTC);
        let mut split = SpannerCdcSplit::new_root("test-stream".to_string(), 0, ts);

        let earlier = ts - time::Duration::seconds(10);
        split.advance_offset(earlier); // backward — must be ignored
        assert_eq!(split.offset, Some(ts), "backward update must not change offset");

        split.advance_offset(ts); // equal — must be ignored
        assert_eq!(split.offset, Some(ts), "equal update must not change offset");

        let later = ts + time::Duration::seconds(10);
        split.advance_offset(later); // forward — must update
        assert_eq!(split.offset, Some(later));
    }

    // -----------------------------------------------------------------------
    // PartitionOffsets: counts tracking when multiple partitions share the same
    // offset. The BTreeMap<OffsetDateTime, usize> counts map must correctly
    // track how many partitions are at each offset, so watermark remains
    // correct after individual partitions are removed.
    // -----------------------------------------------------------------------

    #[test]
    fn test_partition_offsets_counts_tracking() {
        let offsets = PartitionOffsets::new();
        let t1 = datetime!(2025-01-01 0:00 UTC);
        let t2 = datetime!(2025-01-02 0:00 UTC);

        // Two partitions at t1, one at t2.
        offsets.register(Some("A".to_string()), t1);
        offsets.register(Some("B".to_string()), t1);
        offsets.register(Some("C".to_string()), t2);

        // Watermark = t1 (min).
        assert_eq!(offsets.watermark(), Some(t1));

        // Remove A — B still at t1, watermark stays t1.
        offsets.remove(&Some("A".to_string()));
        assert_eq!(offsets.watermark(), Some(t1), "B still at t1");

        // Remove B — no more partitions at t1, watermark becomes t2.
        offsets.remove(&Some("B".to_string()));
        assert_eq!(offsets.watermark(), Some(t2), "only C left at t2");

        // Remove C — watermark gone.
        offsets.remove(&Some("C".to_string()));
        assert_eq!(offsets.watermark(), None);
    }

    // -----------------------------------------------------------------------
    // PartitionOffsets: update moves a partition from one offset bucket to
    // another. The counts map must decrement the old bucket and increment
    // the new one.
    // -----------------------------------------------------------------------

    #[test]
    fn test_partition_offsets_update_moves_between_buckets() {
        let offsets = PartitionOffsets::new();
        let t1 = datetime!(2025-01-01 0:00 UTC);
        let t2 = datetime!(2025-01-02 0:00 UTC);
        let t3 = datetime!(2025-01-03 0:00 UTC);

        // A at t1, B at t3.
        offsets.register(Some("A".to_string()), t1);
        offsets.register(Some("B".to_string()), t3);
        assert_eq!(offsets.watermark(), Some(t1));

        // Update A from t1 to t2 — t1 bucket becomes empty, t2 bucket gets A.
        offsets.update(&Some("A".to_string()), t2);
        assert_eq!(offsets.watermark(), Some(t2), "watermark moves to t2");

        // Update A from t2 to t3 — now both A and B at t3.
        offsets.update(&Some("A".to_string()), t3);
        assert_eq!(offsets.watermark(), Some(t3), "watermark moves to t3");

        // Remove A — B still at t3.
        offsets.remove(&Some("A".to_string()));
        assert_eq!(offsets.watermark(), Some(t3), "B still at t3");

        // Remove B — watermark gone.
        offsets.remove(&Some("B".to_string()));
        assert_eq!(offsets.watermark(), None);
    }

    // -----------------------------------------------------------------------
    // promote_children_of: parent has two children, one ready (all parents done),
    // one not ready (still waiting for another parent). The ready one gets
    // promoted, the not-ready one stays deferred, and children_by_parent
    // is re-inserted with only the unprocessed child.
    // -----------------------------------------------------------------------

    #[test]
    fn test_promote_mixed_ready_and_deferred_children() {
        let t1 = datetime!(2025-01-01 0:00 UTC);
        let t2 = datetime!(2025-01-02 0:00 UTC);

        // C1 has only P1 as parent — ready when P1 finishes.
        // C2 has P1 and P2 — not ready until P2 also finishes.
        let c1 = make_child("C1", vec!["P1"], t1);
        let c2 = make_child("C2", vec!["P1", "P2"], t2);

        let mut deferred = HashMap::new();
        deferred.insert(Some("C1".to_string()), c1);
        deferred.insert(Some("C2".to_string()), c2);

        let mut ready_pool: HashMap<Option<String>, SpannerCdcSplit> = HashMap::new();
        let mut children_by_parent = HashMap::new();
        children_by_parent.insert(
            Some("P1".to_string()),
            vec![Some("C1".to_string()), Some("C2".to_string())],
        );
        children_by_parent.insert(Some("P2".to_string()), vec![Some("C2".to_string())]);

        let mut finished = HashMap::new();
        finished.insert(Some("P1".to_string()), true);
        finished.insert(Some("P2".to_string()), false); // P2 not done

        promote_children_of(
            &Some("P1".to_string()),
            &mut deferred,
            &mut ready_pool,
            &mut children_by_parent,
            &finished,
        );

        // C1 promoted (only parent is P1, which is done).
        assert!(
            ready_pool.contains_key(&Some("C1".to_string())),
            "C1 should be promoted"
        );

        // C2 stays deferred (P2 not done).
        assert!(
            deferred.contains_key(&Some("C2".to_string())),
            "C2 should stay deferred"
        );

        // children_by_parent[P1] re-inserted with only C2 (the unprocessed child).
        assert_eq!(
            children_by_parent[&Some("P1".to_string())],
            vec![Some("C2".to_string())],
            "only unprocessed child should remain in children_by_parent"
        );

        // Now P2 finishes — C2 should be promoted.
        finished.insert(Some("P2".to_string()), true);
        promote_children_of(
            &Some("P2".to_string()),
            &mut deferred,
            &mut ready_pool,
            &mut children_by_parent,
            &finished,
        );

        assert!(deferred.is_empty());
        assert!(ready_pool.contains_key(&Some("C2".to_string())));
    }
}
