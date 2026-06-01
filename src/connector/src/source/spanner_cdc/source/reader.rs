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
//! ## PartitionCoordinator: eager emit + per-partition checkpoint
//!
//! Spanner change-stream partitions are read concurrently with no cross-partition
//! commit-timestamp ordering. Partition tasks send `PartitionRecord`s to a shared
//! record channel. The `run_reader` main loop runs a `PartitionCoordinator` that:
//!
//! 1. Emits records **eagerly** in per-partition order (no global reorder). This
//!    is correct because the pipeline needs only per-key ordering, which Spanner +
//!    the `parents_all_finished` gate guarantee.
//! 2. Tracks a scalar **checkpoint watermark** = monotonic min over non-finished
//!    partitions, stamped on every message for checkpoint compatibility.
//! 3. Tracks a monotonic schema accumulator (superset-only) so a lagging partition
//!    can never shrink the schema (no destructive DROP COLUMN).
//! 4. Exposes a per-partition **frontier** so a restart resumes each partition from
//!    its own offset instead of re-reading the whole tree from one position.

use std::collections::{BTreeMap, HashMap, VecDeque};

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use futures_async_stream::try_stream;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::statement::Statement;
use risingwave_common::ensure;
use risingwave_common::types::DataType;
use risingwave_pb::connector_service::{SourceType, cdc_message};
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

use super::TaggedChangeRecord;
use crate::error::{ConnectorError, ConnectorResult as Result};
use crate::parser::ParserConfig;
use crate::source::cdc::DebeziumCdcMeta;
use crate::source::cdc::external::spanner::PartitionOffset;
use crate::source::spanner_cdc::coordinator::{
    CoordinatorEvent, PartitionCoordinator, PartitionRecord, SeedSchema,
};
use crate::source::spanner_cdc::split::PartitionState;
use crate::source::spanner_cdc::types::{ChangeStreamRecord, SpannerType, TypeCode};
use crate::source::spanner_cdc::{SpannerCdcProperties, SpannerCdcSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceColumnDesc, SourceContextRef, SourceMessage, SourceMeta,
    SplitId, SplitReader, into_chunk_stream,
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

        // Extract the checkpointed offset and per-partition resume frontier from splits.
        let own_split = splits.iter().find(|s| s.index == source_id);
        let checkpointed_offset = own_split
            .map(|s| s.offset_as_micros())
            .filter(|&m| m > 0)
            .and_then(|micros| {
                OffsetDateTime::from_unix_timestamp_nanos((micros as i128) * 1000).ok()
            });
        let mut resume_frontier = own_split.map(|s| s.partitions.clone()).unwrap_or_default();
        if let Some(invalid) = resume_frontier
            .iter()
            .find(|p| micros_to_offset(p.micros).is_none())
        {
            tracing::warn!(
                token = ?invalid.token,
                micros = invalid.micros,
                "invalid Spanner CDC resume frontier; falling back to scalar root resume"
            );
            resume_frontier.clear();
        }
        let seed_schema = make_seed_schema(
            properties.table_name.clone(),
            &parser_config.common.rw_columns,
            &resume_frontier,
            checkpointed_offset,
        );

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
            resume_frontier,
            seed_schema,
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
    /// Per-partition resume frontier from the restored split. Empty => root mode
    /// (fresh start or legacy single-offset split); non-empty => resume each
    /// partition from its own offset.
    resume_frontier: Vec<PartitionOffset>,
    seed_schema: Option<SeedSchema>,
}

/// Result from each partition task.
struct PartitionResult {
    partition_token: Option<String>,
}

/// Tagged wrapper so the coordinator knows which partition a record came from.
struct TaggedPartitionRecord {
    partition_token: Option<String>,
    record: PartitionRecord,
}

/// Key for grouping child partitions by (parent_tokens, start_ts).
/// Children from the same parent split/merge share the same (parents, start_timestamp).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ChildGroupKey {
    parent_tokens: Vec<String>, // sorted
    start_ts: OffsetDateTime,
}

/// A child partition waiting to be started.
struct PendingChild {
    split: SpannerCdcSplit,
}

/// Pending child partition queue ordered by start_ts for BFS scheduling.
///
/// Backed by a `BTreeMap<OffsetDateTime, VecDeque<PendingChild>>` so children
/// are yielded in ascending start_ts order by construction — no explicit sort
/// needed. Within the same start_ts bucket, deferred children (pushed via
/// [`push_deferred`]) are at the front, newly discovered children at the back.
///
/// [`push_deferred`]: PendingQueue::push_deferred
struct PendingQueue {
    children: BTreeMap<OffsetDateTime, VecDeque<PendingChild>>,
}

impl PendingQueue {
    fn new() -> Self {
        Self {
            children: BTreeMap::new(),
        }
    }

    /// Total number of pending children across all start_ts buckets.
    fn len(&self) -> usize {
        self.children.values().map(|b| b.len()).sum()
    }

    /// Add a newly discovered child (back of its start_ts bucket).
    fn push(&mut self, child: PendingChild) {
        let ts = child.split.offset.unwrap_or_else(OffsetDateTime::now_utc);
        self.children.entry(ts).or_default().push_back(child);
    }

    /// Add a deferred child (front of its start_ts bucket).
    /// Deferred children are processed before newly discovered ones at the same start_ts.
    fn push_deferred(&mut self, child: PendingChild) {
        let ts = child.split.offset.unwrap_or_else(OffsetDateTime::now_utc);
        self.children.entry(ts).or_default().push_front(child);
    }

    /// Drain all children whose parents are finished, in BFS order (ascending start_ts).
    /// Not-ready children remain in the queue.
    fn drain_ready(
        &mut self,
        partition_progress: &HashMap<String, PartitionState>,
    ) -> Vec<PendingChild> {
        let mut ready = Vec::new();
        let keys: Vec<OffsetDateTime> = self.children.keys().copied().collect();
        for key in keys {
            let bucket = self.children.get_mut(&key).unwrap();
            let mut still_pending = VecDeque::new();
            for child in bucket.drain(..) {
                if parents_all_finished(&child.split.parent_partition_tokens, partition_progress) {
                    ready.push(child);
                } else {
                    still_pending.push_back(child);
                }
            }
            if still_pending.is_empty() {
                self.children.remove(&key);
            } else {
                // bucket is already drained, write back the not-ready ones
                self.children.insert(key, still_pending);
            }
        }
        ready
    }

    /// Pop the lowest-start_ts child whose parents are finished.
    /// Returns `None` if no ready children exist.
    ///
    /// Scans buckets in key order; O(buckets) in the worst case but bounded by
    /// `max_concurrent` in practice.
    fn pop_ready(
        &mut self,
        partition_progress: &HashMap<String, PartitionState>,
    ) -> Option<PendingChild> {
        let mut found_child: Option<PendingChild> = None;
        for bucket in self.children.values_mut() {
            let mut not_ready = VecDeque::new();
            while let Some(child) = bucket.pop_front() {
                if parents_all_finished(&child.split.parent_partition_tokens, partition_progress) {
                    found_child = Some(child);
                    break;
                } else {
                    not_ready.push_back(child);
                }
            }
            // Rebuild bucket: not_ready (front) + remaining items (back)
            let remaining: VecDeque<_> = bucket.drain(..).collect();
            *bucket = not_ready;
            bucket.extend(remaining);
            if found_child.is_some() {
                break;
            }
        }
        self.children.retain(|_, b| !b.is_empty());
        found_child
    }
}

/// Collect all ready children from `pending_children` and `child_discovery_rx`.
/// Returns children in BFS order (ascending start_ts) by construction.
fn collect_ready_children(
    pending_children: &mut PendingQueue,
    child_discovery_rx: &mut tokio::sync::mpsc::UnboundedReceiver<SpannerCdcSplit>,
    partition_progress: &mut HashMap<String, PartitionState>,
) -> Vec<PendingChild> {
    // Register newly arrived children into the queue (goes to the right bucket by start_ts)
    while let Ok(split) = child_discovery_rx.try_recv() {
        if let Some(ref token) = split.partition_token {
            if !register_child(partition_progress, token.clone()) {
                continue; // duplicate
            }
        }
        pending_children.push(PendingChild { split });
    }

    // Drain all ready children — BFS order by construction
    pending_children.drain_ready(partition_progress)
}

async fn run_reader(mut ctx: ReaderContext, tx: mpsc::Sender<Vec<SourceMessage>>) -> Result<()> {
    let mut partition_streams: FuturesUnordered<tokio::task::JoinHandle<Result<PartitionResult>>> =
        FuturesUnordered::new();

    let max_concurrent = ctx.max_concurrent_partitions;
    let mut active_count: usize = 0;

    // Local HashMap for dedup and parent coordination. Not used for recovery.
    let mut partition_progress: HashMap<String, PartitionState> = HashMap::new();

    let mut pending_children = PendingQueue::new();
    let (child_discovery_tx, mut child_discovery_rx) =
        tokio::sync::mpsc::unbounded_channel::<SpannerCdcSplit>();

    // Shared record channel: partition tasks → coordinator
    let (record_tx, mut record_rx) = mpsc::channel::<TaggedPartitionRecord>(DEFAULT_CHANNEL_SIZE);

    let split_id = SplitId::from(ctx.source_id.to_string());
    let root_offset = ctx
        .checkpointed_offset
        .unwrap_or_else(OffsetDateTime::now_utc);
    let mut coordinator = PartitionCoordinator::new_with_seed_schema(ctx.seed_schema.take());

    if ctx.resume_frontier.is_empty() {
        // Root mode: fresh start or a legacy single-offset split. Start one root
        // partition and re-discover the whole tree from `root_offset`.
        tracing::info!(starting_offset = ?root_offset, "starting Spanner CDC reader with root partition");
        let root_split =
            SpannerCdcSplit::new_root(ctx.change_stream_name.clone(), ctx.source_id, root_offset);
        coordinator.handle(CoordinatorEvent::PartitionStarted {
            token: root_split.partition_token.clone(),
            parent_tokens: vec![],
            start_ts: root_offset,
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
    } else {
        // Resume mode: restore each non-finished partition from its own offset
        // (the Beam PartitionMetadata model) instead of re-reading from the minimum.
        tracing::info!(
            partitions = ctx.resume_frontier.len(),
            "resuming Spanner CDC reader from per-partition frontier"
        );

        // Seed local progress: each frontier partition gets its persisted state;
        // any parent referenced but absent from the frontier must have finished
        // pre-restart (the frontier persists all non-finished partitions).
        partition_progress = seed_resume_progress(&ctx.resume_frontier);

        // Declare every frontier partition as a placeholder so the checkpoint
        // watermark stays pinned at each partition's offset until it advances.
        for p in &ctx.resume_frontier {
            if let Some(start_ts) = micros_to_offset(p.micros) {
                coordinator.handle(CoordinatorEvent::DeclarePartitions {
                    tokens: vec![p.token.clone()],
                    parent_tokens: p.parent_tokens.clone(),
                    start_ts,
                });
            }
        }

        // Start ready partitions (parents finished, slot available); queue the rest.
        // Resumed Running partitions re-discover their children via
        // ChildPartitionsRecord; `register_child` dedups against queued children.
        for p in &ctx.resume_frontier {
            let Some(start_ts) = micros_to_offset(p.micros) else {
                continue;
            };
            let split = match &p.token {
                None => SpannerCdcSplit::new_root(
                    ctx.change_stream_name.clone(),
                    ctx.source_id,
                    start_ts,
                ),
                Some(tok) => SpannerCdcSplit::new_child(
                    tok.clone(),
                    p.parent_tokens.clone(),
                    start_ts,
                    ctx.change_stream_name.clone(),
                    0,
                ),
            };
            let ready =
                p.token.is_none() || parents_all_finished(&p.parent_tokens, &partition_progress);
            if ready && active_count < max_concurrent {
                if let Some(ref tok) = p.token {
                    set_running(&mut partition_progress, tok);
                }
                coordinator.handle(CoordinatorEvent::PartitionStarted {
                    token: p.token.clone(),
                    parent_tokens: p.parent_tokens.clone(),
                    start_ts,
                });
                active_count += 1;
                spawn_partition_task(
                    &ctx,
                    split,
                    &split_id,
                    &mut partition_streams,
                    child_discovery_tx.clone(),
                    &record_tx,
                );
            } else {
                pending_children.push(PendingChild { split });
            }
        }
    }

    // Main event loop
    loop {
        let mut checkpoint_only = false;
        if tx.is_closed() {
            // Graceful shutdown: RisingWave cancelled us (source dropped, rebalance, etc).
            // Drain the coordinator and attempt a final send so the downstream
            // can checkpoint as far as possible before we exit.
            let mut remaining = coordinator.drain();
            stamp_watermark(&mut remaining, &coordinator);
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
                        coordinator.handle(CoordinatorEvent::PartitionFinished {
                            token: pr.partition_token.clone(),
                        });
                        checkpoint_only = true;

                        if let Some(ref token) = pr.partition_token {
                            if let Some(state) = partition_progress.get_mut(token) {
                                *state = PartitionState::Finished;
                            }
                        }

                        // Collect ready children — BFS order by construction (PendingQueue)
                        let ready_children = collect_ready_children(
                            &mut pending_children, &mut child_discovery_rx,
                            &mut partition_progress,
                        );

                        // Group all ready children by (sorted parent tokens, start_ts)
                        let mut groups: HashMap<ChildGroupKey, Vec<PendingChild>> = HashMap::new();
                        for child in ready_children {
                            let mut parents = child.split.parent_partition_tokens.clone();
                            parents.sort();
                            let key = ChildGroupKey {
                                parent_tokens: parents,
                                start_ts: child.split.offset.unwrap_or(root_offset),
                            };
                            groups.entry(key).or_default().push(child);
                        }

                        // Start children in each group
                        for (key, children) in groups {
                            let tokens: Vec<Option<String>> = children
                                .iter()
                                .map(|c| c.split.partition_token.clone())
                                .collect();
                            // All children in this group share the same start_ts
                            let group_start_ts = children
                                .first()
                                .and_then(|c| c.split.offset)
                                .unwrap_or(root_offset);
                            coordinator.handle(CoordinatorEvent::DeclarePartitions {
                                tokens,
                                parent_tokens: key.parent_tokens.clone(),
                                start_ts: group_start_ts,
                            });
                            checkpoint_only = true;

                            for child in children {
                                if active_count >= max_concurrent {
                                    pending_children.push_deferred(child);
                                    continue;
                                }
                                let start_ts =
                                    child.split.offset.unwrap_or(root_offset);
                                if let Some(ref token) = child.split.partition_token {
                                    set_running(&mut partition_progress, token);
                                }
                                coordinator.handle(CoordinatorEvent::PartitionStarted {
                                    token: child.split.partition_token.clone(),
                                    parent_tokens: child.split.parent_partition_tokens.clone(),
                                    start_ts,
                                });
                                checkpoint_only = true;
                                active_count += 1;
                                spawn_partition_task(
                                    &ctx,
                                    child.split,
                                    &split_id,
                                    &mut partition_streams,
                                    child_discovery_tx.clone(),
                                    &record_tx,
                                );
                            }
                        }
                    }
                    Some(Ok(Err(e))) => return Err(e),
                    Some(Err(e)) => {
                        return Err(ConnectorError::from(anyhow::anyhow!("partition task panicked: {}", e)));
                    }
                    None => {
                        tracing::warn!(%split_id, pending = pending_children.len(), "all partitions finished");
                        break;
                    }
                }

                active_count = active_count.saturating_sub(1);

                // Try starting pending children (non-parent-triggered path).
                // These children were already declared via DeclarePartitions
                // when their parent finished — placeholders are in place.
                while active_count < max_concurrent {
                    if let Some(child) = pending_children.pop_ready(&partition_progress) {
                        if let Some(ref token) = child.split.partition_token { set_running(&mut partition_progress, token); }
                        let start_ts = child.split.offset.unwrap_or(root_offset);
                        coordinator.handle(CoordinatorEvent::PartitionStarted {
                            token: child.split.partition_token.clone(),
                            parent_tokens: child.split.parent_partition_tokens.clone(),
                            start_ts,
                        });
                        checkpoint_only = true;
                        active_count += 1;
                        spawn_partition_task(&ctx, child.split, &split_id, &mut partition_streams, child_discovery_tx.clone(), &record_tx);
                    } else { break; }
                }
            }

            Some(child) = child_discovery_rx.recv() => {
                if let Some(ref token) = child.partition_token {
                    if !register_child(&mut partition_progress, token.clone()) {
                        tracing::debug!(token = %token, "duplicate child partition, skipping");
                        continue;
                    }
                }
                pending_children.push(PendingChild {
                    split: child,
                });
                // Children are started by the PartitionFinished arm (Arm 1),
                // which groups children by (parent_tokens, start_ts) and
                // declares partitions with placeholders before starting them.
            }

            Some(tagged) = record_rx.recv() => {
                coordinator.handle(CoordinatorEvent::Record {
                    partition_token: tagged.partition_token,
                    record: tagged.record,
                });
            }
        }

        // Drain and emit after every event — eager emission, no data loss
        let mut batch = coordinator.drain();
        if batch.is_empty() && checkpoint_only {
            batch.push(checkpoint_heartbeat_msg(&split_id, &coordinator));
        }
        stamp_watermark(&mut batch, &coordinator);
        if !batch.is_empty() && tx.send(batch).await.is_err() {
            break;
        }
    }

    // Safety net: drain anything still staged (e.g., after a `break` from
    // a send error). Partition tasks have already exited or will exit when
    // `record_tx` is dropped.
    let mut remaining = coordinator.drain();
    stamp_watermark(&mut remaining, &coordinator);
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
) -> bool {
    parent_tokens.iter().all(|p| {
        partition_progress
            .get(p)
            .map(|state| *state == PartitionState::Finished)
            .unwrap_or(false)
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
            // are occurring. Sent to the coordinator so the offset can progress while idle.
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

/// Convert microseconds since epoch to an `OffsetDateTime`.
fn micros_to_offset(micros: i64) -> Option<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp_nanos((micros as i128) * 1000).ok()
}

/// Build an offset string from the coordinator's scalar checkpoint watermark plus
/// the per-partition resume frontier. The scalar `timestamp` stays as the global
/// min checkpoint floor; `partitions` rides along so a restart resumes each
/// partition from its own offset.
fn make_watermark_offset_string(watermark_micros: i64, frontier: Vec<PartitionOffset>) -> String {
    let spanner_offset =
        crate::source::cdc::external::spanner::SpannerOffset::new(watermark_micros)
            .with_partitions(frontier);
    let cdc_offset = crate::source::cdc::external::CdcOffset::Spanner(spanner_offset);
    serde_json::to_string(&cdc_offset).unwrap_or_else(|_| watermark_micros.to_string())
}

/// Stamp all messages in a batch with the coordinator's scalar checkpoint watermark and
/// the current per-partition frontier. Every message carries the same offset
/// (last-in-chunk wins when the executor persists the split).
fn stamp_watermark(batch: &mut [SourceMessage], coordinator: &PartitionCoordinator) {
    let wm_micros = coordinator
        .checkpoint_watermark()
        .map(|wm| (wm.unix_timestamp_nanos() / 1000) as i64)
        .unwrap_or(0);
    if wm_micros > 0 {
        let offset_str = make_watermark_offset_string(wm_micros, coordinator.checkpoint_frontier());
        for msg in batch.iter_mut() {
            msg.offset = offset_str.clone();
        }
    } else if !batch.is_empty() {
        tracing::warn!("stamp_watermark called with wm_micros=0, messages will carry empty offset");
    }
}

fn checkpoint_heartbeat_msg(
    split_id: &SplitId,
    coordinator: &PartitionCoordinator,
) -> SourceMessage {
    let source_ts_ms = coordinator
        .checkpoint_watermark()
        .map(|wm| (wm.unix_timestamp_nanos() / 1_000_000) as i64)
        .unwrap_or(0);
    SourceMessage {
        key: None,
        payload: None,
        offset: String::new(),
        split_id: split_id.clone(),
        meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
            String::new(),
            source_ts_ms,
            cdc_message::CdcMessageType::Heartbeat,
            SourceType::Unspecified,
        )),
    }
}

fn make_seed_schema(
    table_name: Option<String>,
    columns: &[SourceColumnDesc],
    resume_frontier: &[PartitionOffset],
    checkpointed_offset: Option<OffsetDateTime>,
) -> Option<SeedSchema> {
    let table_name = table_name?;
    let columns = seed_schema_from_source_columns(columns);
    if columns.is_empty() {
        return None;
    }
    let min_type_change_ts = resume_frontier
        .iter()
        .filter_map(|p| micros_to_offset(p.micros))
        .max()
        .or(checkpointed_offset);
    Some(SeedSchema {
        table_name,
        columns,
        min_type_change_ts,
    })
}

fn seed_schema_from_source_columns(
    columns: &[SourceColumnDesc],
) -> Vec<(String, SpannerType, bool)> {
    let columns: Vec<_> = columns
        .iter()
        .filter(|c| c.is_visible())
        .filter_map(|c| {
            rw_type_to_spanner_type(&c.data_type)
                .map(|spanner_type| (c.name.clone(), spanner_type, c.is_pk))
        })
        .collect();

    // Shared Spanner CDC sources expose a generic JSON `payload` column that
    // does not describe any upstream table schema. Seeding from it would install
    // an unrelated baseline before the first real table record arrives.
    if matches!(columns.as_slice(), [(name, _, _)] if name == "payload") {
        return vec![];
    }

    columns
}

fn rw_type_to_spanner_type(data_type: &DataType) -> Option<SpannerType> {
    let code = match data_type {
        DataType::Boolean => TypeCode::Bool,
        DataType::Int64 => TypeCode::Int64,
        DataType::Float64 => TypeCode::Float64,
        DataType::Float32 => TypeCode::Float32,
        DataType::Timestamp | DataType::Timestamptz => TypeCode::Timestamp,
        DataType::Date => TypeCode::Date,
        DataType::Varchar => TypeCode::String,
        DataType::Bytea => TypeCode::Bytes,
        DataType::Decimal => TypeCode::Numeric,
        DataType::Jsonb => TypeCode::Json,
        DataType::List(list_type) => {
            return rw_type_to_spanner_type(list_type.elem()).map(|element_type| SpannerType {
                code: TypeCode::Array,
                array_element_type: Some(Box::new(element_type)),
                struct_type: None,
                type_annotation: None,
                proto_type_fqn: None,
            });
        }
        _ => return None,
    };
    Some(SpannerType::simple(code))
}

/// Seed local partition progress from a restored per-partition frontier.
///
/// Each frontier partition is registered with its persisted state (Running or
/// Pending). Any parent token referenced by a frontier partition but **absent**
/// from the frontier must have finished before the checkpoint (the frontier
/// persists exactly the non-finished partitions), so it is marked `Finished` —
/// which lets the `parents_all_finished` gate release resumed children.
fn seed_resume_progress(frontier: &[PartitionOffset]) -> HashMap<String, PartitionState> {
    let mut progress: HashMap<String, PartitionState> = HashMap::new();
    let mut present: std::collections::HashSet<String> = std::collections::HashSet::new();
    for p in frontier {
        if let Some(token) = &p.token {
            present.insert(token.clone());
            let state = if p.running {
                PartitionState::Running
            } else {
                PartitionState::Pending
            };
            progress.insert(token.clone(), state);
        }
    }
    for p in frontier {
        for parent in &p.parent_tokens {
            if !present.contains(parent) {
                progress
                    .entry(parent.clone())
                    .or_insert(PartitionState::Finished);
            }
        }
    }
    progress
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: make a split with given token, offset, and parent tokens.
    fn make_split(token: &str, offset: OffsetDateTime, parents: Vec<&str>) -> SpannerCdcSplit {
        SpannerCdcSplit {
            partition_token: Some(token.to_string()),
            parent_partition_tokens: parents.iter().map(|p| p.to_string()).collect(),
            offset: Some(offset),
            change_stream_name: String::new(),
            index: 0,
            partitions: vec![],
        }
    }

    /// Helper: mark a parent as Finished in partition_progress.
    fn finish_parent(progress: &mut HashMap<String, PartitionState>, token: &str) {
        progress.insert(token.to_string(), PartitionState::Finished);
    }

    fn ts(sec: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(sec).unwrap()
    }

    // ---------------------------------------------------------------
    // BFS ordering: ascending start_ts by construction
    // ---------------------------------------------------------------

    #[test]
    fn pending_queue_yields_in_start_ts_order() {
        let mut q = PendingQueue::new();
        let mut progress = HashMap::new();
        finish_parent(&mut progress, "P1");

        // Push children at different start_ts, out of order
        q.push(PendingChild {
            split: make_split("C3", ts(300), vec!["P1"]),
        });
        q.push(PendingChild {
            split: make_split("C1", ts(100), vec!["P1"]),
        });
        q.push(PendingChild {
            split: make_split("C2", ts(200), vec!["P1"]),
        });

        let ready = q.drain_ready(&progress);
        let tokens: Vec<&str> = ready
            .iter()
            .map(|c| c.split.partition_token.as_deref().unwrap())
            .collect();
        assert_eq!(
            tokens,
            vec!["C1", "C2", "C3"],
            "should yield in ascending start_ts order"
        );
    }

    // ---------------------------------------------------------------
    // Deferred children go to front of their bucket
    // ---------------------------------------------------------------

    #[test]
    fn pending_queue_deferred_children_have_priority_within_same_ts() {
        let mut q = PendingQueue::new();
        let mut progress = HashMap::new();
        finish_parent(&mut progress, "P1");

        // New child at ts=100
        q.push(PendingChild {
            split: make_split("NEW", ts(100), vec!["P1"]),
        });
        // Deferred child at ts=100 (should come out first)
        q.push_deferred(PendingChild {
            split: make_split("DEFERRED", ts(100), vec!["P1"]),
        });

        let ready = q.drain_ready(&progress);
        let tokens: Vec<&str> = ready
            .iter()
            .map(|c| c.split.partition_token.as_deref().unwrap())
            .collect();
        assert_eq!(
            tokens,
            vec!["DEFERRED", "NEW"],
            "deferred should come before new at same ts"
        );
    }

    // ---------------------------------------------------------------
    // Not-ready children stay in queue
    // ---------------------------------------------------------------

    #[test]
    fn pending_queue_not_ready_children_stay() {
        let mut q = PendingQueue::new();
        let mut progress = HashMap::new();
        // P1 not finished — children won't be ready

        q.push(PendingChild {
            split: make_split("C1", ts(100), vec!["P1"]),
        });
        q.push(PendingChild {
            split: make_split("C2", ts(200), vec!["P1"]),
        });

        let ready = q.drain_ready(&progress);
        assert!(ready.is_empty(), "no parents finished → nothing ready");
        assert_eq!(q.len(), 2, "both children still pending");

        // Now finish P1
        finish_parent(&mut progress, "P1");
        let ready = q.drain_ready(&progress);
        assert_eq!(ready.len(), 2);
        assert_eq!(q.len(), 0);
    }

    // ---------------------------------------------------------------
    // pop_ready: single lowest-ts ready child
    // ---------------------------------------------------------------

    #[test]
    fn pending_queue_pop_ready_returns_lowest_ts() {
        let mut q = PendingQueue::new();
        let mut progress = HashMap::new();
        finish_parent(&mut progress, "P1");

        q.push(PendingChild {
            split: make_split("C2", ts(200), vec!["P1"]),
        });
        q.push(PendingChild {
            split: make_split("C1", ts(100), vec!["P1"]),
        });

        let child = q.pop_ready(&progress).unwrap();
        assert_eq!(child.split.partition_token.as_deref(), Some("C1"));
        assert_eq!(q.len(), 1);
    }

    // ---------------------------------------------------------------
    // pop_ready: skips not-ready, returns next ready
    // ---------------------------------------------------------------

    #[test]
    fn pending_queue_pop_ready_skips_not_ready() {
        let mut q = PendingQueue::new();
        let mut progress = HashMap::new();
        finish_parent(&mut progress, "P1");
        // P2 NOT finished

        q.push(PendingChild {
            split: make_split("C1", ts(100), vec!["P2"]), // not ready
        });
        q.push(PendingChild {
            split: make_split("C2", ts(200), vec!["P1"]), // ready
        });

        let child = q.pop_ready(&progress).unwrap();
        assert_eq!(
            child.split.partition_token.as_deref(),
            Some("C2"),
            "skip C1 (P2 not finished), return C2"
        );
        assert_eq!(q.len(), 1, "C1 still pending");
    }

    // ---------------------------------------------------------------
    // Overflow: all children preserved when pushed via continue
    // ---------------------------------------------------------------

    #[test]
    fn pending_queue_overflow_preserves_all_children() {
        // Simulates the overflow pattern from the reader loop:
        // push_deferred(child) + continue (not break).
        // All overflow children must survive.
        let mut q = PendingQueue::new();
        let mut progress = HashMap::new();
        finish_parent(&mut progress, "P1");

        // Push 4 children at ts=100 as deferred (overflow)
        for i in 0..4 {
            q.push_deferred(PendingChild {
                split: make_split(&format!("OV{i}"), ts(100), vec!["P1"]),
            });
        }

        // Plus 2 new children at ts=200
        q.push(PendingChild {
            split: make_split("NEW1", ts(200), vec!["P1"]),
        });
        q.push(PendingChild {
            split: make_split("NEW2", ts(200), vec!["P1"]),
        });

        assert_eq!(q.len(), 6, "all 6 children must be in queue");

        // Drain all — BFS order: ts=100 first, then ts=200
        let ready = q.drain_ready(&progress);
        assert_eq!(ready.len(), 6, "all 6 children drained");

        // ts=100 bucket: all 4 deferred children must be present (order within
        // bucket is an implementation detail of push_deferred, not a contract).
        let ts100_tokens: Vec<&str> = ready[0..4]
            .iter()
            .map(|c| c.split.partition_token.as_deref().unwrap())
            .collect();
        let mut ts100_sorted = ts100_tokens.clone();
        ts100_sorted.sort();
        assert_eq!(ts100_sorted, vec!["OV0", "OV1", "OV2", "OV3"]);

        // ts=200 bucket (new children, FIFO order):
        let ts200_tokens: Vec<&str> = ready[4..6]
            .iter()
            .map(|c| c.split.partition_token.as_deref().unwrap())
            .collect();
        assert_eq!(ts200_tokens, vec!["NEW1", "NEW2"]);
    }

    #[test]
    fn pending_queue_is_empty_and_len() {
        let mut q = PendingQueue::new();
        assert_eq!(q.len(), 0);

        q.push(PendingChild {
            split: make_split("C1", ts(100), vec!["P1"]),
        });
        assert!(q.len() > 0);
        assert_eq!(q.len(), 1);

        q.push(PendingChild {
            split: make_split("C2", ts(200), vec!["P1"]),
        });
        assert_eq!(q.len(), 2);

        // Drain everything
        let mut progress = HashMap::new();
        finish_parent(&mut progress, "P1");
        let ready = q.drain_ready(&progress);
        assert_eq!(ready.len(), 2);
        assert_eq!(q.len(), 0);
    }

    // ---------------------------------------------------------------
    // Resume seeding: absent parents marked Finished; readiness derived
    // ---------------------------------------------------------------

    #[test]
    fn resume_seed_marks_absent_parents_finished() {
        let frontier = vec![
            PartitionOffset {
                token: Some("R".into()),
                parent_tokens: vec!["G".into()],
                micros: 100,
                running: true,
            },
            PartitionOffset {
                token: Some("C".into()),
                parent_tokens: vec!["G2".into()],
                micros: 100,
                running: false,
            },
            PartitionOffset {
                token: Some("D".into()),
                parent_tokens: vec!["R".into()],
                micros: 100,
                running: false,
            },
        ];
        let progress = seed_resume_progress(&frontier);

        // Frontier partitions keep their persisted state.
        assert_eq!(progress.get("R"), Some(&PartitionState::Running));
        assert_eq!(progress.get("C"), Some(&PartitionState::Pending));
        assert_eq!(progress.get("D"), Some(&PartitionState::Pending));
        // Parents absent from the frontier are marked Finished.
        assert_eq!(progress.get("G"), Some(&PartitionState::Finished));
        assert_eq!(progress.get("G2"), Some(&PartitionState::Finished));

        // R (parent G finished) and C (parent G2 finished) are ready;
        // D (parent R still running) is not.
        assert!(parents_all_finished(&["G".to_string()], &progress));
        assert!(parents_all_finished(&["G2".to_string()], &progress));
        assert!(!parents_all_finished(&["R".to_string()], &progress));
    }

    #[test]
    fn checkpoint_heartbeat_persists_lifecycle_only_frontier_change() {
        let split_id = SplitId::from("test");
        let mut coordinator = PartitionCoordinator::new();
        coordinator.handle(CoordinatorEvent::PartitionStarted {
            token: Some("p1".into()),
            parent_tokens: vec![],
            start_ts: ts(0),
        });
        coordinator.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(10),
                msg: SourceMessage::dummy(),
            },
        });
        assert_eq!(coordinator.drain().len(), 1);

        coordinator.handle(CoordinatorEvent::PartitionFinished {
            token: Some("p1".into()),
        });
        let mut batch = coordinator.drain();
        assert!(batch.is_empty(), "finish alone emits no data record");
        batch.push(checkpoint_heartbeat_msg(&split_id, &coordinator));
        stamp_watermark(&mut batch, &coordinator);

        let offset: crate::source::cdc::external::CdcOffset =
            serde_json::from_str(&batch[0].offset).unwrap();
        let crate::source::cdc::external::CdcOffset::Spanner(spanner_offset) = offset else {
            panic!("expected Spanner offset");
        };
        assert_eq!(spanner_offset.timestamp, 10_000_000);
        assert_eq!(spanner_offset.partitions, Some(vec![]));
    }

    #[test]
    fn seed_schema_is_disabled_without_tracked_table_name() {
        let columns = vec![SourceColumnDesc::simple("id", DataType::Int64, 0.into())];

        assert!(make_seed_schema(None, &columns, &[], Some(ts(10))).is_none());
    }

    #[test]
    fn seed_schema_skips_shared_payload_shape() {
        let columns = vec![SourceColumnDesc::simple(
            "payload",
            DataType::Jsonb,
            0.into(),
        )];

        assert!(make_seed_schema(Some("orders".into()), &columns, &[], Some(ts(10))).is_none());
    }
}
