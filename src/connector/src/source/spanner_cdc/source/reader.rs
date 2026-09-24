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
//! 4. `into_stream()` parses on a dedicated task and yields the chunks
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
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::types;
use risingwave_common::array::StreamChunk;
#[allow(unused_imports)]
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_common::metrics::{LabelGuardedIntCounter, LabelGuardedIntGauge};
use risingwave_common::{bail, ensure};
use risingwave_pb::connector_service::{SourceType, cdc_message};
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

use super::{ChangeRecordContext, build_source_message};
use crate::error::{ConnectorError, ConnectorResult as Result};
use crate::parser::ParserConfig;
use crate::source::cdc::DebeziumCdcMeta;
use crate::source::monitor::SourceMetrics;
use crate::source::spanner_cdc::schema_track::SchemaTracker;
use crate::source::spanner_cdc::{SpannerCdcProperties, SpannerCdcSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SourceMeta, SplitId,
    SplitReader, into_chunk_stream,
};

const DEFAULT_CHANNEL_SIZE: usize = 16;

/// How often the lifecycle loop re-samples the partition gauges.
const METRICS_SAMPLE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Depth of the parsed-chunk channel between the parser task and the source actor.
///
/// Each element is a whole `StreamChunk`, and this bound is what propagates
/// backpressure from the actor back to the Spanner readers.
const PARSED_CHUNK_CHANNEL_SIZE: usize = 8;

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
        let heartbeat_interval_ms = properties.heartbeat_milliseconds;

        let ctx = ReaderContext {
            client,
            database: properties.database.clone(),
            change_stream_name: properties.change_stream_name.clone(),
            heartbeat_interval_ms,
            retry_attempts: properties.get_retry_attempts(),
            retry_backoff: properties.get_retry_backoff(),
            retry_backoff_max_delay_ms: properties.get_retry_backoff_max_delay_ms(),
            retry_backoff_factor: properties.get_retry_backoff_factor(),
            stall_timeout: properties.get_stall_timeout(),
            source_id,
            checkpointed_offset,
            metrics: source_ctx.metrics.clone(),
            source_name: source_ctx.source_name.clone(),
            fragment_id: source_ctx.fragment_id.to_string(),
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
        let queue_depth = source_context
            .metrics
            .spanner_cdc_parsed_chunk_queue_depth
            .with_guarded_label_values(&[
                &source_context.source_id.to_string(),
                &source_context.source_name,
                &source_context.fragment_id.to_string(),
            ]);
        let chunk_stream =
            into_chunk_stream(self.into_data_stream(), parser_config, source_context);

        // Parse on a dedicated task so the actor only forwards and dispatches chunks;
        // the two then occupy separate runtime workers.
        //
        // The channel is bounded, so a slow actor stalls the parser on `send`, which
        // stops it polling the message stream and parks the Spanner partition readers
        // once `DEFAULT_CHANNEL_SIZE` fills.
        let (tx, rx) = mpsc::channel(PARSED_CHUNK_CHANNEL_SIZE);
        tokio::spawn(async move {
            let mut chunk_stream = std::pin::pin!(chunk_stream);
            loop {
                let item = tokio::select! {
                    biased;
                    // The actor dropped the stream. Without this branch the task
                    // would stay parked on `next()` until the partition readers
                    // produce their next record or heartbeat.
                    _ = tx.closed() => break,
                    item = chunk_stream.next() => match item {
                        Some(item) => item,
                        None => break,
                    },
                };
                let is_err = item.is_err();
                if tx.send(item).await.is_err() {
                    break;
                }
                // `into_chunk_stream` terminates after an error.
                if is_err {
                    break;
                }
            }
        });

        Self::forward_parsed_chunks(rx, queue_depth)
    }
}

impl SpannerCdcSplitReader {
    /// Yield chunks parsed by the background parser task.
    ///
    /// The queue depth is sampled on dequeue: a value near
    /// `PARSED_CHUNK_CHANNEL_SIZE` means the actor is the constraint, near zero
    /// means the parser is.
    #[try_stream(boxed, ok = StreamChunk, error = ConnectorError)]
    async fn forward_parsed_chunks(
        mut rx: mpsc::Receiver<Result<StreamChunk>>,
        queue_depth: LabelGuardedIntGauge,
    ) {
        while let Some(chunk) = rx.recv().await {
            queue_depth.set(rx.len() as i64);
            yield chunk?;
        }
    }

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
    client: DatabaseClient,
    database: String,
    change_stream_name: String,
    heartbeat_interval_ms: i64,
    retry_attempts: u32,
    retry_backoff: std::time::Duration,
    retry_backoff_max_delay_ms: u64,
    retry_backoff_factor: u64,
    stall_timeout: std::time::Duration,
    source_id: u32,
    checkpointed_offset: Option<OffsetDateTime>,
    metrics: Arc<SourceMetrics>,
    source_name: String,
    fragment_id: String,
}

/// Guarded metric handles for one Spanner CDC reader.
///
/// Held for the lifetime of the reader task so the label guards keep the series
/// alive. Labels are source-scoped only — partition tokens are unbounded and
/// must never become label values.
struct ReaderMetrics {
    active_partitions: LabelGuardedIntGauge,
    deferred_partitions: LabelGuardedIntGauge,
    watermark_lag_milliseconds: LabelGuardedIntGauge,
    newest_partition_lag_milliseconds: LabelGuardedIntGauge,
    /// One counter per [`ChildKind`], pre-resolved like `query_failures`.
    child_partitions_discovered: [LabelGuardedIntCounter; ChildKind::ALL.len()],
    partitions_finished: LabelGuardedIntCounter,
    queries: LabelGuardedIntCounter,
    /// One counter per [`QueryFailure`], pre-resolved so the hot path never
    /// touches the label map.
    query_failures: [LabelGuardedIntCounter; QueryFailure::ALL.len()],
}

/// How a child partition came to exist. Spanner reports both through
/// `ChildPartitionsRecord`; the parent count tells them apart.
#[derive(Clone, Copy)]
enum ChildKind {
    /// One parent fanned out into several children.
    Split,
    /// Several parents folded into one child.
    Merge,
}

impl ChildKind {
    const ALL: [Self; 2] = [Self::Split, Self::Merge];

    /// Index into the pre-resolved counter array. See [`QueryFailure::index`].
    fn index(self) -> usize {
        match self {
            Self::Split => 0,
            Self::Merge => 1,
        }
    }

    fn of(parent_tokens: &[String]) -> Self {
        if parent_tokens.len() > 1 {
            Self::Merge
        } else {
            Self::Split
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Split => "split",
            Self::Merge => "merge",
        }
    }
}

/// Why a change stream query ended. Kept as a closed set: it becomes a metric
/// label value, so it must never carry a partition token or an error string.
#[derive(Clone, Copy)]
enum QueryFailure {
    /// The query never returned its first record.
    Establish,
    /// The query was established but then went quiet past the stall timeout.
    Stall,
    /// Spanner rejected the query outright.
    Query,
    /// The established stream produced an error mid-flight.
    Row,
    /// A row arrived but its `ChangeRecord` column could not be parsed.
    Decode,
}

impl QueryFailure {
    const ALL: [Self; 5] = [
        Self::Establish,
        Self::Stall,
        Self::Query,
        Self::Row,
        Self::Decode,
    ];

    /// Index into the pre-resolved counter array.
    ///
    /// Written out rather than `self as usize` so that reordering or inserting
    /// a variant cannot silently misroute a label.
    fn index(self) -> usize {
        match self {
            Self::Establish => 0,
            Self::Stall => 1,
            Self::Query => 2,
            Self::Row => 3,
            Self::Decode => 4,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Establish => "establish_timeout",
            Self::Stall => "stall_timeout",
            Self::Query => "query_error",
            Self::Row => "row_error",
            Self::Decode => "decode_error",
        }
    }
}

impl ReaderMetrics {
    fn new(metrics: &SourceMetrics, source_id: &str, source_name: &str, fragment_id: &str) -> Self {
        let labels = [source_id, source_name, fragment_id];
        Self {
            active_partitions: metrics
                .spanner_cdc_active_partitions
                .with_guarded_label_values(&labels),
            deferred_partitions: metrics
                .spanner_cdc_deferred_partitions
                .with_guarded_label_values(&labels),
            watermark_lag_milliseconds: metrics
                .spanner_cdc_watermark_lag_milliseconds
                .with_guarded_label_values(&labels),
            child_partitions_discovered: ChildKind::ALL.map(|kind| {
                metrics
                    .spanner_cdc_child_partition_discovered_count
                    .with_guarded_label_values(&[
                        source_id,
                        source_name,
                        fragment_id,
                        kind.as_str(),
                    ])
            }),
            newest_partition_lag_milliseconds: metrics
                .spanner_cdc_newest_partition_lag_milliseconds
                .with_guarded_label_values(&labels),
            partitions_finished: metrics
                .spanner_cdc_partition_finished_count
                .with_guarded_label_values(&labels),
            queries: metrics
                .spanner_cdc_partition_query_count
                .with_guarded_label_values(&labels),
            query_failures: QueryFailure::ALL.map(|cause| {
                metrics
                    .spanner_cdc_partition_query_failure_count
                    .with_guarded_label_values(&[
                        source_id,
                        source_name,
                        fragment_id,
                        cause.as_str(),
                    ])
            }),
        }
    }

    fn record_child(&self, kind: ChildKind) {
        self.child_partitions_discovered[kind.index()].inc();
    }

    fn record_failure(&self, cause: QueryFailure) {
        self.query_failures[cause.index()].inc();
    }

    /// Publish the partition-lifecycle gauges.
    fn observe(
        &self,
        active: usize,
        deferred: usize,
        watermark: Option<OffsetDateTime>,
        newest: Option<OffsetDateTime>,
    ) {
        self.active_partitions.set(active as i64);
        self.deferred_partitions.set(deferred as i64);
        match watermark {
            Some(wm) => {
                let lag = (OffsetDateTime::now_utc() - wm).whole_milliseconds();
                self.watermark_lag_milliseconds
                    .set(lag.clamp(0, i64::MAX as i128) as i64);
            }
            // No registered partition has an offset, so there is no lag to
            // report. Leaving the previous value would pin a stale reading.
            None => self.watermark_lag_milliseconds.set(0),
        }
        match newest {
            Some(ts) => {
                let lag = (OffsetDateTime::now_utc() - ts).whole_milliseconds();
                self.newest_partition_lag_milliseconds
                    .set(lag.clamp(0, i64::MAX as i128) as i64);
            }
            None => self.newest_partition_lag_milliseconds.set(0),
        }
    }
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
        if let Some(entry) = inner.offsets.get_mut(token)
            && offset > *entry
        {
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

    /// Remove a finished partition.
    fn remove(&self, token: &Option<String>) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(offset) = inner.offsets.remove(token)
            && let Some(count) = inner.counts.get_mut(&offset)
        {
            *count -= 1;
            if *count == 0 {
                inner.counts.remove(&offset);
            }
        }
    }

    /// Watermark = min(offset) across all registered (un-finished) partitions. O(1).
    fn watermark(&self) -> Option<OffsetDateTime> {
        self.inner.lock().unwrap().counts.keys().next().copied()
    }

    /// `(min, max)` offset across all registered (un-finished) partitions, both
    /// O(1) and read under a single lock so the pair is coherent.
    ///
    /// The max is the partition keeping up best; its gap to the min is what
    /// distinguishes one stuck partition from a reader that is uniformly behind.
    fn offset_bounds(&self) -> (Option<OffsetDateTime>, Option<OffsetDateTime>) {
        let inner = self.inner.lock().unwrap();
        let mut keys = inner.counts.keys();
        let min = keys.next().copied();
        // `next_back` after `next` still yields the max unless there is exactly
        // one key, in which case the iterator is already exhausted.
        let max = keys.next_back().copied().or(min);
        (min, max)
    }
}

/// Process a single discovered child: dedup, register offset, route to
/// `ready_pool` or `deferred`.
fn process_child(
    child: SpannerCdcSplit,
    discovered: &mut HashMap<Option<String>, bool>,
    deferred: &mut Vec<SpannerCdcSplit>,
    ready_pool: &mut Vec<SpannerCdcSplit>,
    offsets: &PartitionOffsets,
    split_id: &SplitId,
    reader_metrics: &ReaderMetrics,
) {
    let token = child.partition_token.clone();
    if discovered.contains_key(&token) {
        return;
    }
    reader_metrics.record_child(ChildKind::of(&child.parent_partition_tokens));
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
    discovered.insert(token, false);

    if parents_all_finished(&child.parent_partition_tokens, discovered) {
        ready_pool.push(child);
    } else {
        deferred.push(child);
    }
}

/// Drain `child_discovery_rx` and register discovered children.
///
/// Batch-drains all pending children from the channel, deduplicates via
/// `discovered`, registers offsets, and routes to `ready_pool` (if all
/// parents finished) or `deferred` (otherwise).
fn ingest_children(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<SpannerCdcSplit>,
    discovered: &mut HashMap<Option<String>, bool>,
    deferred: &mut Vec<SpannerCdcSplit>,
    ready_pool: &mut Vec<SpannerCdcSplit>,
    offsets: &PartitionOffsets,
    split_id: &SplitId,
    reader_metrics: &ReaderMetrics,
) {
    while let Ok(child) = rx.try_recv() {
        process_child(
            child,
            discovered,
            deferred,
            ready_pool,
            offsets,
            split_id,
            reader_metrics,
        );
    }
}

/// Main reader loop — equivalent to Debezium's JNI thread that reads from the WAL.
async fn run_reader(ctx: ReaderContext, tx: mpsc::Sender<Vec<SourceMessage>>) -> Result<()> {
    let mut partition_streams: FuturesUnordered<tokio::task::JoinHandle<Result<PartitionResult>>> =
        FuturesUnordered::new();

    // Ready partitions (parents all finished) — spawned in batch.
    let mut ready_pool: Vec<SpannerCdcSplit> = Vec::new();
    let mut active_count: usize = 0;

    // Registered children waiting for parents to finish.
    let mut deferred: Vec<SpannerCdcSplit> = Vec::new();

    // Track which partitions have been discovered (for dedup and parent-before-child coordination).
    let mut discovered: HashMap<Option<String>, bool> = HashMap::new();

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

    let reader_metrics = Arc::new(ReaderMetrics::new(
        &ctx.metrics,
        &ctx.source_id.to_string(),
        &ctx.source_name,
        &ctx.fragment_id,
    ));

    // Spawn root partition.
    let root_split =
        SpannerCdcSplit::new_root(ctx.change_stream_name.clone(), ctx.source_id, root_offset);
    let root_token = root_split.partition_token.clone();
    offsets.register(root_token.clone(), root_offset);
    discovered.insert(root_token, false);
    spawn_partition_task(
        &ctx,
        root_split,
        &split_id,
        &offsets,
        &shared_schema,
        &tx,
        &mut partition_streams,
        child_discovery_tx.clone(),
        &reader_metrics,
    );
    active_count += 1;

    // The loop otherwise only wakes on partition completion or child discovery.
    // A partition that is streaming but falling behind produces neither, so
    // without this tick the gauges would freeze for exactly the incident they
    // are meant to show.
    let mut metrics_tick = tokio::time::interval(METRICS_SAMPLE_INTERVAL);
    metrics_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Main event loop — partition lifecycle management only.
    // Records flow directly from partition tasks → tx.
    loop {
        let (watermark, newest) = offsets.offset_bounds();
        reader_metrics.observe(active_count, deferred.len(), watermark, newest);

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
                        reader_metrics.partitions_finished.inc();
                        offsets.remove(&pr.partition_token);
                        if let Some(ref token) = pr.partition_token {
                            discovered.insert(Some(token.clone()), true);
                        } else {
                            discovered.insert(None, true);
                        }

                        // Check deferred children.
                        promote_deferred(
                            &mut deferred,
                            &mut ready_pool,
                            &discovered,
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
                            &reader_metrics,
                        );
                    }
                    Some(Ok(Err(e))) => {
                        // Fail fast: abort remaining partition tasks so their tx
                        // clones are dropped, the channel closes, and the reader
                        // restarts on the next poll cycle instead of waiting for
                        // orphaned tasks to finish.
                        for handle in &partition_streams {
                            handle.abort();
                        }
                        return Err(e);
                    }
                    Some(Err(e)) => {
                        for handle in &partition_streams {
                            handle.abort();
                        }
                        return Err(ConnectorError::from(anyhow::anyhow!(
                            "partition task panicked: {}", e
                        )));
                    }
                    None => {
                        // All tasks done — drain any children that arrived before
                        // the last task's JoinHandle resolved. Without this,
                        // biased select starves child_discovery_rx when
                        // partition_streams is exhausted (returns Ready(None)
                        // immediately, short-circuiting the channel branch).
                        ingest_children(
                            &mut child_discovery_rx,
                            &mut discovered,
                            &mut deferred,
                            &mut ready_pool,
                            &offsets,
                            &split_id,
                            &reader_metrics,
                        );
                        promote_deferred(
                            &mut deferred,
                            &mut ready_pool,
                            &discovered,
                        );
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
                            &reader_metrics,
                        );
                        // `spawn_from_pool` drains `ready_pool`, so its emptiness
                        // says nothing about in-flight work — tasks may have just
                        // been spawned above. Exit only when nothing is running
                        // and nothing is waiting; otherwise keep managing the
                        // lifecycle of the tasks we just spawned.
                        if active_count == 0 {
                            if deferred.is_empty() {
                                tracing::info!(%split_id, "all partitions finished");
                                break;
                            }
                            return Err(ConnectorError::from(anyhow::anyhow!(
                                "deadlock: no active tasks but {} children still waiting",
                                deferred.len()
                            )));
                        }
                    }
                }
            }

            Some(child) = child_discovery_rx.recv() => {
                // First child already available — process it, then drain siblings.
                process_child(
                    child,
                    &mut discovered,
                    &mut deferred,
                    &mut ready_pool,
                    &offsets,
                    &split_id,
                    &reader_metrics,
                );
                // Drain any remaining siblings that arrived concurrently.
                ingest_children(
                    &mut child_discovery_rx,
                    &mut discovered,
                    &mut deferred,
                    &mut ready_pool,
                    &offsets,
                    &split_id,
                    &reader_metrics,
                );
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
                    &reader_metrics,
                );
            }

            // Re-sample the gauges. Listed last so `biased` still prioritises
            // partition progress.
            _ = metrics_tick.tick() => {}
        }
    }

    // Abort any partition tasks still running when the lifecycle loop exits
    // (e.g. the reader channel closed). Dropping a `JoinHandle` does NOT stop
    // a tokio task — without the abort, orphaned tasks keep querying Spanner
    // and holding `tx` clones, so the channel never closes and the source
    // cannot detect the dead lifecycle loop and restart.
    for handle in &partition_streams {
        handle.abort();
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Partition coordination
// ---------------------------------------------------------------------------

fn parents_all_finished(
    parent_tokens: &[String],
    discovered: &HashMap<Option<String>, bool>,
) -> bool {
    // Root partition has no parent tokens — check if root (token=None) has finished.
    if parent_tokens.is_empty() {
        return discovered.get(&None).copied().unwrap_or(false);
    }
    parent_tokens
        .iter()
        .all(|p| discovered.get(&Some(p.clone())).copied().unwrap_or(false))
}

fn promote_deferred(
    deferred: &mut Vec<SpannerCdcSplit>,
    ready_pool: &mut Vec<SpannerCdcSplit>,
    discovered: &HashMap<Option<String>, bool>,
) {
    deferred.retain(|child| {
        if parents_all_finished(&child.parent_partition_tokens, discovered) {
            ready_pool.push(child.clone());
            false
        } else {
            true
        }
    });
}

fn spawn_from_pool(
    ready_pool: &mut Vec<SpannerCdcSplit>,
    active_count: &mut usize,
    ctx: &ReaderContext,
    split_id: &SplitId,
    offsets: &Arc<PartitionOffsets>,
    shared_schema: &Arc<std::sync::Mutex<SchemaTracker>>,
    tx: &mpsc::Sender<Vec<SourceMessage>>,
    partition_streams: &mut FuturesUnordered<tokio::task::JoinHandle<Result<PartitionResult>>>,
    child_discovery_tx: &tokio::sync::mpsc::UnboundedSender<SpannerCdcSplit>,
    reader_metrics: &Arc<ReaderMetrics>,
) {
    for split in ready_pool.drain(..) {
        spawn_partition_task(
            ctx,
            split,
            split_id,
            offsets,
            shared_schema,
            tx,
            partition_streams,
            child_discovery_tx.clone(),
            reader_metrics,
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
    reader_metrics: &Arc<ReaderMetrics>,
) {
    let client = ctx.client.clone();
    let database = ctx.database.clone();
    let change_stream_name = ctx.change_stream_name.clone();
    let heartbeat_interval_ms = ctx.heartbeat_interval_ms;
    let retry_attempts = ctx.retry_attempts;
    let retry_backoff = ctx.retry_backoff;
    let retry_backoff_max_delay_ms = ctx.retry_backoff_max_delay_ms;
    let retry_backoff_factor = ctx.retry_backoff_factor;
    let stall_timeout = ctx.stall_timeout;
    let offsets = offsets.clone();
    let shared_schema = shared_schema.clone();
    let tx = tx.clone();
    let split_id = split_id.clone();
    let partition_token = split.partition_token.clone();
    let reader_metrics = reader_metrics.clone();

    partition_streams.push(tokio::spawn(async move {
        Box::pin(read_partition(
            client,
            database,
            split,
            change_stream_name,
            heartbeat_interval_ms,
            split_id,
            retry_attempts,
            retry_backoff,
            retry_backoff_max_delay_ms,
            retry_backoff_factor,
            stall_timeout,
            offsets,
            shared_schema,
            tx,
            child_discovery_tx,
            reader_metrics,
        ))
        .await
        .map(|()| PartitionResult { partition_token })
    }));
}

// ---------------------------------------------------------------------------
// Change stream query execution
// ---------------------------------------------------------------------------

#[expect(clippy::too_many_arguments)]
async fn read_partition(
    client: DatabaseClient,
    database: String,
    mut split: SpannerCdcSplit,
    change_stream_name: String,
    heartbeat_interval_ms: i64,
    split_id: SplitId,
    retry_attempts: u32,
    retry_backoff: std::time::Duration,
    retry_backoff_max_delay_ms: u64,
    retry_backoff_factor: u64,
    stall_timeout: std::time::Duration,
    offsets: Arc<PartitionOffsets>,
    shared_schema: Arc<std::sync::Mutex<SchemaTracker>>,
    tx: mpsc::Sender<Vec<SourceMessage>>,
    child_discovery_tx: tokio::sync::mpsc::UnboundedSender<SpannerCdcSplit>,
    reader_metrics: Arc<ReaderMetrics>,
) -> Result<()> {
    let start_ts = split.offset.ok_or_else(|| {
        ConnectorError::from(anyhow::anyhow!(
            "offset is None for split_id={}, change_stream={}",
            split_id,
            change_stream_name
        ))
    })?;

    // Workaround: the googleapis SDK's `OffsetDateTime::to_value()` formats
    // timestamps with 9-digit subsecond precision (nanoseconds, padded with
    // zeros for microsecond-precision values). Spanner's TIMESTAMP literal
    // parser rejects that, so we format the timestamp ourselves with
    // microsecond precision and bind it as a string.
    let sql = format!(
        "SELECT ChangeRecord FROM READ_{}(start_timestamp => @start_timestamp, end_timestamp => @end_timestamp, partition_token => @partition_token, heartbeat_milliseconds => @heartbeat_milliseconds)",
        change_stream_name
    );

    tracing::info!(%split_id, %start_ts, partition_token = ?split.partition_token, "change stream query starting");

    if retry_attempts == 0 {
        let stmt = Statement::builder(&sql)
            .add_typed_param("start_timestamp", start_ts, types::timestamp())
            .add_typed_param(
                "end_timestamp",
                Option::<OffsetDateTime>::None,
                types::timestamp(),
            )
            .add_typed_param("partition_token", &split.partition_token, types::string())
            .add_typed_param(
                "heartbeat_milliseconds",
                heartbeat_interval_ms,
                types::int64(),
            )
            .build();
        return Box::pin(execute_query(
            &client,
            &stmt,
            &mut split,
            &split_id,
            &database,
            &offsets,
            &shared_schema,
            &tx,
            &child_discovery_tx,
            &change_stream_name,
            stall_timeout,
            &reader_metrics,
        ))
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
        let resume_ts = split
            .offset
            .expect("offset validated at entry and only advanced by advance_offset");
        let stmt = Statement::builder(&sql)
            .add_typed_param("start_timestamp", resume_ts, types::timestamp())
            .add_typed_param(
                "end_timestamp",
                Option::<OffsetDateTime>::None,
                types::timestamp(),
            )
            .add_typed_param("partition_token", &split.partition_token, types::string())
            .add_typed_param(
                "heartbeat_milliseconds",
                heartbeat_interval_ms,
                types::int64(),
            )
            .build();
        match Box::pin(execute_query(
            &client,
            &stmt,
            &mut split,
            &split_id,
            &database,
            &offsets,
            &shared_schema,
            &tx,
            &child_discovery_tx,
            &change_stream_name,
            stall_timeout,
            &reader_metrics,
        ))
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

#[expect(clippy::too_many_arguments)]
async fn execute_query(
    client: &DatabaseClient,
    stmt: &Statement,
    split: &mut SpannerCdcSplit,
    split_id: &SplitId,
    database: &str,
    offsets: &PartitionOffsets,
    shared_schema: &std::sync::Mutex<SchemaTracker>,
    tx: &mpsc::Sender<Vec<SourceMessage>>,
    child_discovery_tx: &tokio::sync::mpsc::UnboundedSender<SpannerCdcSplit>,
    change_stream_name: &str,
    stall_timeout: std::time::Duration,
    reader_metrics: &ReaderMetrics,
) -> Result<()> {
    reader_metrics.queries.inc();
    let txn = client.single_use().build();
    let mut result_set =
        match tokio::time::timeout(stall_timeout, txn.execute_query(stmt.clone())).await {
            Ok(Ok(rs)) => rs,
            Ok(Err(e)) => {
                reader_metrics.record_failure(QueryFailure::Query);
                return Err(anyhow::anyhow!("failed to execute query: {}", e).into());
            }
            Err(_) => {
                reader_metrics.record_failure(QueryFailure::Establish);
                return Err(anyhow::anyhow!(
                    "query establishment timed out after {:?} for partition {:?}",
                    stall_timeout,
                    split.partition_token,
                )
                .into());
            }
        };

    let mut offset_cache = OffsetStringCache::new();

    loop {
        let row = match tokio::time::timeout(stall_timeout, result_set.next()).await {
            Ok(Some(Ok(row))) => row,
            Ok(Some(Err(e))) => {
                reader_metrics.record_failure(QueryFailure::Row);
                return Err(anyhow::anyhow!("failed to get next row: {}", e).into());
            }
            Ok(None) => break, // result set exhausted
            Err(_) => {
                reader_metrics.record_failure(QueryFailure::Stall);
                return Err(anyhow::anyhow!(
                    "stream stalled: no data or heartbeat received within {:?} for partition {:?}",
                    stall_timeout,
                    split.partition_token,
                )
                .into());
            }
        };
        if tx.is_closed() {
            return Ok(());
        }

        let change_records = crate::source::spanner_cdc::types::parse_change_record_column(&row, 0)
            .inspect_err(|_| {
                reader_metrics.record_failure(QueryFailure::Decode);
            })?;

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
                let offset_str = offset_cache.get((wm.unix_timestamp_nanos() / 1000) as i64);

                // Schema evolution: emit schema change before data records,
                // mimicking Debezium's Relation messages that precede DML events.
                let schema_payload = shared_schema.lock().unwrap().check_and_evolve(
                    &data_change.table_name,
                    &data_change.column_types,
                    commit_ts,
                );
                if let Some(schema_payload) = schema_payload {
                    if !messages.is_empty() && tx.send(std::mem::take(&mut messages)).await.is_err()
                    {
                        return Ok(());
                    }
                    let schema_msg = make_schema_change_msg(
                        split_id,
                        schema_payload.json,
                        data_change,
                        offset_str,
                    );
                    if tx.send(vec![schema_msg]).await.is_err() {
                        return Ok(());
                    }
                }

                // Column types and the rest of the record header are the same for every
                // mod, so derive them once per record instead of once per row. Skipped
                // entirely for records that carry no mods (e.g. schema-change only).
                if !data_change.mods.is_empty() {
                    let column_types = data_change.column_type_map();
                    let ctx = ChangeRecordContext::new(database, data_change, &column_types);
                    for modification in &data_change.mods {
                        messages.push(build_source_message(
                            split_id,
                            &ctx,
                            modification,
                            offset_str,
                        ));
                    }
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
                let offset_str = offset_cache.get((wm.unix_timestamp_nanos() / 1000) as i64);

                messages.push(SourceMessage {
                    key: None,
                    payload: None,
                    offset: offset_str.to_owned(),
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
                        change_stream_name.to_owned(),
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
    let spanner_offset = crate::source::cdc::external::spanner::SpannerOffset::new(offset_micros);
    let cdc_offset = crate::source::cdc::external::CdcOffset::Spanner(spanner_offset);
    serde_json::to_string(&cdc_offset).unwrap_or_else(|_| offset_micros.to_string())
}

/// Memoises the rendered checkpoint offset for one partition reader.
///
/// The offset carries the *watermark* — the minimum over all un-finished partitions — not this
/// partition's own position, so it is unchanged across most consecutive records and heartbeats.
/// Rendering it means a `serde_json::to_string` of a `CdcOffset`, which the reader otherwise
/// repeats for every record and every heartbeat.
struct OffsetStringCache {
    micros: Option<i64>,
    rendered: String,
}

impl OffsetStringCache {
    fn new() -> Self {
        Self {
            micros: None,
            rendered: String::new(),
        }
    }

    fn get(&mut self, offset_micros: i64) -> &str {
        if self.micros != Some(offset_micros) {
            self.rendered = make_offset_string(offset_micros);
            self.micros = Some(offset_micros);
        }
        &self.rendered
    }
}

fn make_schema_change_msg(
    split_id: &SplitId,
    payload: Vec<u8>,
    data_change: &crate::source::spanner_cdc::types::DataChangeRecord,
    offset_str: &str,
) -> SourceMessage {
    SourceMessage {
        key: None,
        payload: Some(payload),
        offset: offset_str.to_owned(),
        split_id: split_id.clone(),
        meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
            data_change.table_name.clone(),
            (data_change.commit_time().unix_timestamp_nanos() / 1_000_000) as i64,
            cdc_message::CdcMessageType::SchemaChange,
            SourceType::Unspecified,
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use time::macros::datetime;

    use super::*;

    // -----------------------------------------------------------------------
    // forward_parsed_chunks: hands parsed chunks from the parser task to the actor.
    // -----------------------------------------------------------------------

    fn test_reader_metrics() -> ReaderMetrics {
        ReaderMetrics::new(&SourceMetrics::default(), "0", "test", "0")
    }

    fn test_queue_depth_gauge() -> LabelGuardedIntGauge {
        SourceMetrics::default()
            .spanner_cdc_parsed_chunk_queue_depth
            .with_guarded_label_values(&["0", "test", "0"])
    }

    #[tokio::test]
    async fn test_forward_parsed_chunks_yields_in_order() {
        let (tx, rx) = mpsc::channel(PARSED_CHUNK_CHANNEL_SIZE);
        tx.send(Ok(StreamChunk::from_pretty("I\n + 1")))
            .await
            .unwrap();
        tx.send(Ok(StreamChunk::from_pretty("I\n + 2")))
            .await
            .unwrap();
        drop(tx);

        let chunks: Vec<_> =
            SpannerCdcSplitReader::forward_parsed_chunks(rx, test_queue_depth_gauge())
                .collect()
                .await;
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].as_ref().unwrap().cardinality(), 1);
        assert_eq!(chunks[1].as_ref().unwrap().cardinality(), 1);
    }

    /// A parser error surfaces as `Err` rather than a clean end of stream, so the
    /// source fails loudly instead of going quiet.
    #[tokio::test]
    async fn test_forward_parsed_chunks_propagates_error() {
        let (tx, rx) = mpsc::channel(PARSED_CHUNK_CHANNEL_SIZE);
        tx.send(Ok(StreamChunk::from_pretty("I\n + 1")))
            .await
            .unwrap();
        tx.send(Err(anyhow::anyhow!("parser blew up").into()))
            .await
            .unwrap();
        drop(tx);

        let chunks: Vec<_> =
            SpannerCdcSplitReader::forward_parsed_chunks(rx, test_queue_depth_gauge())
                .collect()
                .await;
        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].is_ok());
        let err = chunks[1].as_ref().unwrap_err();
        assert!(
            err.to_string().contains("parser blew up"),
            "unexpected error: {err}"
        );
    }

    /// The stream ends cleanly once the parser task drops its sender.
    #[tokio::test]
    async fn test_forward_parsed_chunks_ends_when_sender_dropped() {
        let (tx, rx) = mpsc::channel::<Result<StreamChunk>>(PARSED_CHUNK_CHANNEL_SIZE);
        drop(tx);

        let chunks: Vec<_> =
            SpannerCdcSplitReader::forward_parsed_chunks(rx, test_queue_depth_gauge())
                .collect()
                .await;
        assert!(chunks.is_empty());
    }

    /// `offset_bounds` reads both ends of one `BTreeMap` iterator, so the
    /// single-key case (where `next_back` is already exhausted) needs covering.
    #[test]
    fn test_offset_bounds() {
        let offsets = PartitionOffsets::new();
        assert_eq!(offsets.offset_bounds(), (None, None));

        let t0 = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        offsets.register(Some("a".to_owned()), t0);
        assert_eq!(
            offsets.offset_bounds(),
            (Some(t0), Some(t0)),
            "a single partition is both the min and the max"
        );

        let t1 = OffsetDateTime::from_unix_timestamp(2_000).unwrap();
        let t2 = OffsetDateTime::from_unix_timestamp(3_000).unwrap();
        offsets.register(Some("b".to_owned()), t1);
        offsets.register(Some("c".to_owned()), t2);
        assert_eq!(offsets.offset_bounds(), (Some(t0), Some(t2)));

        // The straggler finishing lifts the watermark to the next oldest.
        offsets.remove(&Some("a".to_owned()));
        assert_eq!(offsets.offset_bounds(), (Some(t1), Some(t2)));
    }

    fn make_child(token: &str, parents: Vec<&str>, offset: OffsetDateTime) -> SpannerCdcSplit {
        SpannerCdcSplit::new_child(
            token.to_owned(),
            parents.into_iter().map(String::from).collect(),
            offset,
            "test-stream".to_owned(),
            0,
        )
    }

    // -----------------------------------------------------------------------
    // promote_deferred: scans all deferred children, promotes when parents done.
    // -----------------------------------------------------------------------

    #[test]
    fn test_promote_deferred_basic() {
        let ts = datetime!(2025-01-01 0:00 UTC);
        let child = make_child("C1", vec!["P1"], ts);

        let mut deferred = vec![child];
        let mut ready_pool = Vec::new();
        let mut discovered = HashMap::new();
        discovered.insert(Some("P1".to_owned()), true);

        promote_deferred(&mut deferred, &mut ready_pool, &discovered);

        assert!(deferred.is_empty());
        assert_eq!(ready_pool.len(), 1);
    }

    #[test]
    fn test_promote_deferred_multi_parent_one_done() {
        let ts = datetime!(2025-01-01 0:00 UTC);
        let child = make_child("C1", vec!["P1", "P2"], ts);

        let mut deferred = vec![child];
        let mut ready_pool = Vec::new();
        let mut discovered = HashMap::new();
        discovered.insert(Some("P1".to_owned()), true);
        discovered.insert(Some("P2".to_owned()), false);

        promote_deferred(&mut deferred, &mut ready_pool, &discovered);

        // C1 stays deferred — P2 not done.
        assert_eq!(deferred.len(), 1);
        assert!(ready_pool.is_empty());
    }

    #[test]
    fn test_promote_deferred_multi_parent_both_done() {
        let ts = datetime!(2025-01-01 0:00 UTC);
        let child = make_child("C1", vec!["P1", "P2"], ts);

        let mut deferred = vec![child];
        let mut ready_pool = Vec::new();
        let mut discovered = HashMap::new();
        discovered.insert(Some("P1".to_owned()), true);
        discovered.insert(Some("P2".to_owned()), true);

        promote_deferred(&mut deferred, &mut ready_pool, &discovered);

        assert!(deferred.is_empty());
        assert_eq!(ready_pool.len(), 1);
    }

    // -----------------------------------------------------------------------
    // parents_all_finished: root case, multi-parent case.
    // -----------------------------------------------------------------------

    #[test]
    fn test_parents_all_finished_root() {
        let mut discovered = HashMap::new();
        discovered.insert(None, true);

        // Root partition (empty parent_tokens).
        assert!(parents_all_finished(&[], &discovered));

        // Root not discovered/finished.
        discovered.insert(None, false);
        assert!(!parents_all_finished(&[], &discovered));
    }

    #[test]
    fn test_parents_all_finished_multi_parent() {
        let mut discovered = HashMap::new();
        discovered.insert(Some("P1".to_owned()), true);
        discovered.insert(Some("P2".to_owned()), false);

        assert!(!parents_all_finished(
            &["P1".to_owned(), "P2".to_owned()],
            &discovered
        ));

        discovered.insert(Some("P2".to_owned()), true);
        assert!(parents_all_finished(
            &["P1".to_owned(), "P2".to_owned()],
            &discovered
        ));
    }

    #[test]
    fn test_parents_all_finished_absent_parent() {
        let discovered = HashMap::new();

        // Parent "PX" was never discovered — should return false (child waits forever).
        assert!(!parents_all_finished(&["PX".to_owned()], &discovered));
    }

    // -----------------------------------------------------------------------
    // PartitionOffsets: watermark, counts, backward update.
    // -----------------------------------------------------------------------

    #[test]
    fn test_partition_offsets_watermark_is_min() {
        let offsets = PartitionOffsets::new();
        assert_eq!(offsets.watermark(), None);

        let t1 = datetime!(2025-01-01 0:00 UTC);
        let t2 = datetime!(2025-01-02 0:00 UTC);
        let t3 = datetime!(2025-01-03 0:00 UTC);

        offsets.register(Some("A".to_owned()), t1);
        offsets.register(Some("B".to_owned()), t3);
        assert_eq!(offsets.watermark(), Some(t1));

        offsets.update(&Some("A".to_owned()), t2);
        assert_eq!(offsets.watermark(), Some(t2));

        offsets.remove(&Some("B".to_owned()));
        assert_eq!(offsets.watermark(), Some(t2));

        offsets.remove(&Some("A".to_owned()));
        assert_eq!(offsets.watermark(), None);
    }

    #[test]
    fn test_partition_offsets_backward_update_ignored() {
        let offsets = PartitionOffsets::new();
        let t1 = datetime!(2025-01-01 0:00 UTC);
        let t2 = datetime!(2025-01-02 0:00 UTC);

        offsets.register(Some("A".to_owned()), t2);
        offsets.update(&Some("A".to_owned()), t1);
        assert_eq!(offsets.watermark(), Some(t2));
    }

    #[test]
    fn test_partition_offsets_counts_tracking() {
        let offsets = PartitionOffsets::new();
        let t1 = datetime!(2025-01-01 0:00 UTC);
        let t2 = datetime!(2025-01-02 0:00 UTC);

        offsets.register(Some("A".to_owned()), t1);
        offsets.register(Some("B".to_owned()), t1);
        offsets.register(Some("C".to_owned()), t2);

        assert_eq!(offsets.watermark(), Some(t1));

        // Remove A — B still at t1, watermark stays.
        offsets.remove(&Some("A".to_owned()));
        assert_eq!(offsets.watermark(), Some(t1));

        // Remove B — watermark moves to t2.
        offsets.remove(&Some("B".to_owned()));
        assert_eq!(offsets.watermark(), Some(t2));

        offsets.remove(&Some("C".to_owned()));
        assert_eq!(offsets.watermark(), None);
    }

    #[test]
    fn test_partition_offsets_update_moves_between_buckets() {
        let offsets = PartitionOffsets::new();
        let t1 = datetime!(2025-01-01 0:00 UTC);
        let t2 = datetime!(2025-01-02 0:00 UTC);
        let t3 = datetime!(2025-01-03 0:00 UTC);

        offsets.register(Some("A".to_owned()), t1);
        offsets.register(Some("B".to_owned()), t3);
        assert_eq!(offsets.watermark(), Some(t1));

        offsets.update(&Some("A".to_owned()), t2);
        assert_eq!(offsets.watermark(), Some(t2));

        offsets.update(&Some("A".to_owned()), t3);
        assert_eq!(offsets.watermark(), Some(t3));

        offsets.remove(&Some("A".to_owned()));
        assert_eq!(offsets.watermark(), Some(t3));

        offsets.remove(&Some("B".to_owned()));
        assert_eq!(offsets.watermark(), None);
    }

    // -----------------------------------------------------------------------
    // Multiple children with different parent states: some promoted, some not.
    // Tests that promote_deferred correctly handles partial promotion.
    // -----------------------------------------------------------------------

    #[test]
    fn test_promote_deferred_mixed_parent_states() {
        let ts = datetime!(2025-01-01 0:00 UTC);

        // C1: parent P1 done → should be promoted.
        // C2: parents P1 and P2, P2 not done → stays deferred.
        // C3: parent P2 done → should be promoted.
        let mut deferred = vec![
            make_child("C1", vec!["P1"], ts),
            make_child("C2", vec!["P1", "P2"], ts),
            make_child("C3", vec!["P2"], ts),
        ];

        let mut ready_pool = Vec::new();
        let mut discovered = HashMap::new();
        discovered.insert(Some("P1".to_owned()), true);
        discovered.insert(Some("P2".to_owned()), false);

        promote_deferred(&mut deferred, &mut ready_pool, &discovered);

        // C1 promoted (P1 done).
        // C2 stays deferred (P2 not done).
        // C3 stays deferred (P2 not done).
        assert_eq!(ready_pool.len(), 1, "only C1 should be promoted");
        assert_eq!(ready_pool[0].partition_token, Some("C1".to_owned()));
        assert_eq!(deferred.len(), 2, "C2 and C3 should stay deferred");

        // Now P2 finishes.
        discovered.insert(Some("P2".to_owned()), true);
        promote_deferred(&mut deferred, &mut ready_pool, &discovered);

        // C2 and C3 promoted.
        assert_eq!(ready_pool.len(), 3, "all children should be promoted");
        assert!(deferred.is_empty());
    }

    // -----------------------------------------------------------------------
    // Batch-drain via ingest_children: multiple children arriving at once.
    // -----------------------------------------------------------------------

    #[test]
    fn test_ingest_children_multiple_children() {
        let ts = datetime!(2025-01-01 0:00 UTC);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let mut discovered = HashMap::new();
        let mut deferred: Vec<SpannerCdcSplit> = Vec::new();
        let mut ready_pool: Vec<SpannerCdcSplit> = Vec::new();
        let offsets = PartitionOffsets::new();
        let split_id = SplitId::from("test".to_owned());

        discovered.insert(Some("P1".to_owned()), false); // P1 not done

        tx.send(make_child("C1", vec!["P1"], ts)).unwrap();
        tx.send(make_child("C2", vec!["P1"], ts)).unwrap();
        tx.send(make_child("C3", vec!["P1"], ts)).unwrap();

        ingest_children(
            &mut rx,
            &mut discovered,
            &mut deferred,
            &mut ready_pool,
            &offsets,
            &split_id,
            &test_reader_metrics(),
        );

        assert_eq!(deferred.len(), 3, "all 3 children should be in deferred");
    }

    #[test]
    fn test_ingest_children_dedup_within_batch() {
        let ts = datetime!(2025-01-01 0:00 UTC);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let mut discovered = HashMap::new();
        let mut deferred: Vec<SpannerCdcSplit> = Vec::new();
        let mut ready_pool: Vec<SpannerCdcSplit> = Vec::new();
        let offsets = PartitionOffsets::new();
        let split_id = SplitId::from("test".to_owned());

        discovered.insert(Some("P1".to_owned()), false);

        tx.send(make_child("C1", vec!["P1"], ts)).unwrap();
        tx.send(make_child("C2", vec!["P1"], ts)).unwrap();
        tx.send(make_child("C1", vec!["P1"], ts)).unwrap(); // duplicate

        ingest_children(
            &mut rx,
            &mut discovered,
            &mut deferred,
            &mut ready_pool,
            &offsets,
            &split_id,
            &test_reader_metrics(),
        );

        assert_eq!(deferred.len(), 2, "duplicate C1 should be deduped");
    }

    #[test]
    fn test_ingest_children_mixed_routing() {
        let ts = datetime!(2025-01-01 0:00 UTC);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let mut discovered = HashMap::new();
        let mut deferred: Vec<SpannerCdcSplit> = Vec::new();
        let mut ready_pool: Vec<SpannerCdcSplit> = Vec::new();
        let offsets = PartitionOffsets::new();
        let split_id = SplitId::from("test".to_owned());

        // P1 finished, P2 not finished.
        discovered.insert(Some("P1".to_owned()), true);
        discovered.insert(Some("P2".to_owned()), false);

        // C1's parent P1 done → ready_pool.
        // C2's parent P2 not done → deferred.
        tx.send(make_child("C1", vec!["P1"], ts)).unwrap();
        tx.send(make_child("C2", vec!["P2"], ts)).unwrap();

        ingest_children(
            &mut rx,
            &mut discovered,
            &mut deferred,
            &mut ready_pool,
            &offsets,
            &split_id,
            &test_reader_metrics(),
        );

        assert_eq!(ready_pool.len(), 1, "C1 should be in ready_pool");
        assert_eq!(ready_pool[0].partition_token, Some("C1".to_owned()));
        assert_eq!(deferred.len(), 1, "C2 should be in deferred");
        assert_eq!(deferred[0].partition_token, Some("C2".to_owned()));
    }

    #[test]
    fn test_ingest_children_registers_offsets() {
        let ts = datetime!(2025-01-01 0:00 UTC);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let mut discovered = HashMap::new();
        let mut deferred: Vec<SpannerCdcSplit> = Vec::new();
        let mut ready_pool: Vec<SpannerCdcSplit> = Vec::new();
        let offsets = PartitionOffsets::new();
        let split_id = SplitId::from("test".to_owned());

        discovered.insert(Some("P1".to_owned()), true);

        tx.send(make_child("C1", vec!["P1"], ts)).unwrap();

        assert_eq!(offsets.watermark(), None);
        ingest_children(
            &mut rx,
            &mut discovered,
            &mut deferred,
            &mut ready_pool,
            &offsets,
            &split_id,
            &test_reader_metrics(),
        );
        assert_eq!(offsets.watermark(), Some(ts), "offset should be registered");
    }

    // -----------------------------------------------------------------------
    // ingest_children: drains channel, dedup, registers, routes to deferred/ready_pool.
    // -----------------------------------------------------------------------

    #[test]
    fn test_ingest_children_routes_to_deferred_when_parents_not_done() {
        let ts = datetime!(2025-01-01 0:00 UTC);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let mut discovered = HashMap::new();
        let mut deferred: Vec<SpannerCdcSplit> = Vec::new();
        let mut ready_pool: Vec<SpannerCdcSplit> = Vec::new();
        let offsets = PartitionOffsets::new();
        let split_id = SplitId::from("test".to_owned());

        // P1 discovered but not finished.
        discovered.insert(Some("P1".to_owned()), false);

        // C1's parent P1 not finished → should go to deferred.
        tx.send(make_child("C1", vec!["P1"], ts)).unwrap();

        ingest_children(
            &mut rx,
            &mut discovered,
            &mut deferred,
            &mut ready_pool,
            &offsets,
            &split_id,
            &test_reader_metrics(),
        );

        assert_eq!(deferred.len(), 1, "C1 should be in deferred");
        assert!(ready_pool.is_empty());
        assert!(discovered.contains_key(&Some("C1".to_owned())));
    }

    #[test]
    fn test_ingest_children_routes_to_ready_pool_when_parents_done() {
        let ts = datetime!(2025-01-01 0:00 UTC);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let mut discovered = HashMap::new();
        let mut deferred: Vec<SpannerCdcSplit> = Vec::new();
        let mut ready_pool: Vec<SpannerCdcSplit> = Vec::new();
        let offsets = PartitionOffsets::new();
        let split_id = SplitId::from("test".to_owned());

        // P1 discovered and finished.
        discovered.insert(Some("P1".to_owned()), true);

        // C1's parent P1 finished → should go to ready_pool.
        tx.send(make_child("C1", vec!["P1"], ts)).unwrap();

        ingest_children(
            &mut rx,
            &mut discovered,
            &mut deferred,
            &mut ready_pool,
            &offsets,
            &split_id,
            &test_reader_metrics(),
        );

        assert!(deferred.is_empty());
        assert_eq!(ready_pool.len(), 1, "C1 should be in ready_pool");
    }

    #[test]
    fn test_ingest_children_dedup_across_calls() {
        let ts = datetime!(2025-01-01 0:00 UTC);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let mut discovered = HashMap::new();
        let mut deferred: Vec<SpannerCdcSplit> = Vec::new();
        let mut ready_pool: Vec<SpannerCdcSplit> = Vec::new();
        let offsets = PartitionOffsets::new();
        let split_id = SplitId::from("test".to_owned());

        discovered.insert(Some("P1".to_owned()), false);

        // First call: C1 ingested.
        tx.send(make_child("C1", vec!["P1"], ts)).unwrap();
        ingest_children(
            &mut rx,
            &mut discovered,
            &mut deferred,
            &mut ready_pool,
            &offsets,
            &split_id,
            &test_reader_metrics(),
        );
        assert_eq!(deferred.len(), 1);

        // Second call: same C1 again — should be deduped.
        tx.send(make_child("C1", vec!["P1"], ts)).unwrap();
        ingest_children(
            &mut rx,
            &mut discovered,
            &mut deferred,
            &mut ready_pool,
            &offsets,
            &split_id,
            &test_reader_metrics(),
        );
        assert_eq!(deferred.len(), 1, "duplicate C1 should be deduped");
    }

    #[test]
    fn test_ingest_children_empty_channel() {
        let (_, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let mut discovered = HashMap::new();
        let mut deferred: Vec<SpannerCdcSplit> = Vec::new();
        let mut ready_pool: Vec<SpannerCdcSplit> = Vec::new();
        let offsets = PartitionOffsets::new();
        let split_id = SplitId::from("test".to_owned());

        ingest_children(
            &mut rx,
            &mut discovered,
            &mut deferred,
            &mut ready_pool,
            &offsets,
            &split_id,
            &test_reader_metrics(),
        );

        assert!(deferred.is_empty());
        assert!(ready_pool.is_empty());
    }

    // -----------------------------------------------------------------------
    // Abort sibling tasks on failure: run_reader must fail-fast.
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_run_reader_aborts_siblings_on_failure() {
        use std::time::Duration;

        // Verify that when one partition task fails, sibling tasks are aborted
        // promptly rather than left running (holding channel open).
        //
        // Setup: tasks that hold cloned senders — one fails immediately, one
        // sleeps for an hour.  Without the abort fix the test would hang for
        // 3600s; with the fix the sleeping task is aborted and the whole thing
        // completes in <5s.

        let (tx, rx) = mpsc::channel::<Vec<SourceMessage>>(16);

        let test_result = tokio::time::timeout(Duration::from_secs(5), async {
            let mut partition_streams: FuturesUnordered<
                tokio::task::JoinHandle<std::result::Result<PartitionResult, anyhow::Error>>,
            > = FuturesUnordered::new();

            // Task that holds a tx clone and fails immediately.
            let tx_clone = tx.clone();
            partition_streams.push(tokio::spawn(async move {
                let _sender = tx_clone; // held until task is dropped
                Err(anyhow::anyhow!("simulated partition failure"))
            }));

            // Task that holds a tx clone and would take forever.
            let tx_clone = tx.clone();
            partition_streams.push(tokio::spawn(async move {
                let _sender = tx_clone; // held until task is dropped
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok(PartitionResult {
                    partition_token: None,
                })
            }));

            // Simulate the fixed error handling path: abort siblings on error.
            let mut error = None;
            while let Some(result) = partition_streams.next().await {
                match result {
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => {
                        for handle in &partition_streams {
                            handle.abort();
                        }
                        error = Some(e);
                        break;
                    }
                    Err(e) => {
                        for handle in &partition_streams {
                            handle.abort();
                        }
                        error = Some(anyhow::anyhow!("task panicked: {}", e));
                        break;
                    }
                }
            }
            error
        })
        .await;

        // Should complete within timeout (not wait 3600s for the slow task).
        assert!(
            test_result.is_ok(),
            "run_reader should complete within timeout, not wait for orphaned tasks"
        );
        let inner_error = test_result.unwrap();
        assert!(inner_error.is_some(), "should have captured the error");
        assert!(
            inner_error
                .unwrap()
                .to_string()
                .contains("simulated partition failure"),
            "error should be from the failing partition"
        );

        // Channel must close once the original tx is dropped — abort freed
        // the cloned senders held by the tasks.
        drop(tx);
        let closed = tokio::time::timeout(Duration::from_secs(1), async {
            while !rx.is_closed() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            closed.is_ok(),
            "channel should close after sibling tasks are aborted"
        );
    }

    #[tokio::test]
    async fn test_run_reader_aborts_siblings_on_panic() {
        use std::time::Duration;

        let (tx, rx) = mpsc::channel::<Vec<SourceMessage>>(16);

        let test_result = tokio::time::timeout(Duration::from_secs(5), async {
            let mut partition_streams: FuturesUnordered<
                tokio::task::JoinHandle<std::result::Result<PartitionResult, anyhow::Error>>,
            > = FuturesUnordered::new();

            // Task that panics.
            let tx_clone = tx.clone();
            partition_streams.push(tokio::spawn(async move {
                let _sender = tx_clone;
                panic!("simulated partition panic");
            }));

            // Task that holds a tx clone and sleeps forever.
            let tx_clone = tx.clone();
            partition_streams.push(tokio::spawn(async move {
                let _sender = tx_clone;
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok(PartitionResult {
                    partition_token: None,
                })
            }));

            // Simulate the fixed panic handling path.
            let mut error = None;
            while let Some(result) = partition_streams.next().await {
                match result {
                    Ok(Ok(_)) => continue,
                    Ok(Err(e)) => {
                        for handle in &partition_streams {
                            handle.abort();
                        }
                        error = Some(e);
                        break;
                    }
                    Err(e) => {
                        for handle in &partition_streams {
                            handle.abort();
                        }
                        error = Some(anyhow::anyhow!("task panicked: {}", e));
                        break;
                    }
                }
            }
            error
        })
        .await;

        assert!(test_result.is_ok(), "should complete within timeout");
        let inner_error = test_result
            .unwrap()
            .expect("should have captured the panic error");
        assert!(
            inner_error.to_string().contains("task panicked"),
            "error should indicate a panic, got: {}",
            inner_error
        );

        // Channel must close after abort.
        drop(tx);
        let closed = tokio::time::timeout(Duration::from_secs(1), async {
            while !rx.is_closed() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            closed.is_ok(),
            "channel should close after sibling tasks are aborted"
        );
    }
}
