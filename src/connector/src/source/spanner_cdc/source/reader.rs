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
//! Every message carries it as its offset, and it becomes the split's `offset`.
//!
//! Every [`PROGRESS_INTERVAL`] the reader also reports each unfinished partition's
//! offset and parents, in band after the messages it covers, as the split's
//! `partitions`. On restart each saved partition resumes from its own offset. A split
//! without saved partitions restarts the root query from the watermark, and all
//! partitions are re-discovered from scratch.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::types;
use googleapis_gax::error::rpc::Code;
use googleapis_gax::retry_policy::NeverRetry;
#[allow(unused_imports)]
use risingwave_common::metrics::GLOBAL_ERROR_METRICS;
use risingwave_common::metrics::{LabelGuardedIntCounter, LabelGuardedIntGauge};
use risingwave_common::{bail, ensure};
use risingwave_pb::connector_service::{SourceType, cdc_message};
use serde_json::Value as JsonValue;
use thiserror_ext::AsReport;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

use super::{ChangeRecordContext, build_source_message};
use crate::error::{ConnectorError, ConnectorResult as Result};
use crate::parser::ParserConfig;
use crate::source::cdc::DebeziumCdcMeta;
use crate::source::monitor::SourceMetrics;
use crate::source::spanner_cdc::enumerator::SUPPORTED_VALUE_CAPTURE_TYPES;
use crate::source::spanner_cdc::schema_track::SchemaTracker;
use crate::source::spanner_cdc::{PartitionProgress, SpannerCdcProperties, SpannerCdcSplit};
use crate::source::{
    BoxSourceChunkStream, BoxSourceReaderEventStream, Column, SourceContextRef, SourceMessage,
    SourceMessageEvent, SourceMeta, SourceReaderEvent, SplitId, SplitReader,
    into_chunk_event_stream,
};

const DEFAULT_CHANNEL_SIZE: usize = 16;

/// How often the lifecycle loop re-samples the partition gauges and reports each
/// partition's progress. A restart replays at most about this much of every partition,
/// on top of the time since the last checkpoint.
const PROGRESS_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Depth of the parsed-chunk channel between the parser task and the source actor.
///
/// Each element is a whole `StreamChunk`, and this bound is what propagates
/// backpressure from the actor back to the Spanner readers.
const PARSED_CHUNK_CHANNEL_SIZE: usize = 8;

/// Spanner CDC split reader — same pattern as Debezium's `CdcSplitReader`.
pub struct SpannerCdcSplitReader {
    /// Receives message batches and progress reports from the background reader task.
    rx: mpsc::Receiver<SourceMessageEvent>,
    /// The background reader task, awaited for its error once `rx` closes.
    reader_task: JoinHandle<Result<()>>,
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

        let checkpointed = splits.iter().find(|s| s.index == source_id);
        let checkpointed_offset = checkpointed.and_then(|s| s.offset);
        let saved_partitions = checkpointed
            .map(|s| s.partitions.clone())
            .unwrap_or_default();

        let client = properties.create_client().await?;
        let heartbeat_interval_ms = properties.heartbeat_milliseconds;

        let ctx = ReaderContext {
            querier: Arc::new(SpannerChangeStreamQuerier::new(
                client,
                &properties.change_stream_name,
            )),
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
            saved_partitions,
            metrics: source_ctx.metrics.clone(),
            source_name: source_ctx.source_name.clone(),
            fragment_id: source_ctx.fragment_id.to_string(),
        };

        // Spawn background task — like Debezium spawns the JNI thread
        let reader_task = tokio::spawn(run_reader(ctx, tx));

        tracing::info!(source_id, "Spanner CDC reader started");

        Ok(Self {
            rx,
            reader_task,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        self.into_event_stream()
            .try_filter_map(|event| async move {
                Ok(match event {
                    SourceReaderEvent::DataChunk(chunk) => Some(chunk),
                    SourceReaderEvent::SplitProgress(_) => None,
                })
            })
            .boxed()
    }

    fn into_event_stream(self) -> BoxSourceReaderEventStream {
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
        let event_stream =
            into_chunk_event_stream(self.into_data_stream(), parser_config, source_context);

        // Parse on a dedicated task so the actor only forwards and dispatches chunks;
        // the two then occupy separate runtime workers.
        //
        // The channel is bounded, so a slow actor stalls the parser on `send`, which
        // stops it polling the message stream and parks the Spanner partition readers
        // once `DEFAULT_CHANNEL_SIZE` fills.
        let (tx, rx) = mpsc::channel(PARSED_CHUNK_CHANNEL_SIZE);
        tokio::spawn(async move {
            let mut event_stream = std::pin::pin!(event_stream);
            loop {
                let item = tokio::select! {
                    biased;
                    // The actor dropped the stream. Without this branch the task
                    // would stay parked on `next()` until the partition readers
                    // produce their next record or heartbeat.
                    _ = tx.closed() => break,
                    item = event_stream.next() => match item {
                        Some(item) => item,
                        None => break,
                    },
                };
                let is_err = item.is_err();
                if tx.send(item).await.is_err() {
                    break;
                }
                // `into_chunk_event_stream` terminates after an error.
                if is_err {
                    break;
                }
            }
        });

        Self::forward_parsed_events(rx, queue_depth)
    }
}

impl SpannerCdcSplitReader {
    /// Yield chunks and progress reports from the background parser task, in order.
    ///
    /// The queue depth is sampled on dequeue: a value near
    /// `PARSED_CHUNK_CHANNEL_SIZE` means the actor is the constraint, near zero
    /// means the parser is.
    #[try_stream(boxed, ok = SourceReaderEvent, error = ConnectorError)]
    async fn forward_parsed_events(
        mut rx: mpsc::Receiver<Result<SourceReaderEvent>>,
        queue_depth: LabelGuardedIntGauge,
    ) {
        while let Some(event) = rx.recv().await {
            queue_depth.set(rx.len() as i64);
            yield event?;
        }
    }

    /// Receive message batches and progress reports from the reader task, like
    /// `CdcSplitReader::into_data_stream`.
    ///
    /// Partition tasks send one batch per change record, and the parser ends a chunk at
    /// every batch. Batches already queued behind the current one are merged, up to the
    /// chunk size, so a burst becomes full chunks instead of one chunk per record. An
    /// idle stream never waits: only batches that have already arrived are merged.
    /// A progress report is never merged, so it stays after the messages it covers.
    #[try_stream(ok = SourceMessageEvent, error = ConnectorError)]
    async fn into_data_stream(mut self) {
        let source_id = self.source_ctx.source_id.to_string();
        let max_batch_len = self.source_ctx.source_ctrl_opts.chunk_size;

        // An event received while merging that must not be merged, yielded next.
        let mut pending: Option<SourceMessageEvent> = None;
        loop {
            let event = match pending.take() {
                Some(event) => event,
                None => match self.rx.recv().await {
                    Some(event) => event,
                    None => break,
                },
            };
            let SourceMessageEvent::Data(mut messages) = event else {
                yield event;
                continue;
            };
            if is_mergeable_batch(&messages) {
                while messages.len() < max_batch_len {
                    match self.rx.try_recv() {
                        Ok(SourceMessageEvent::Data(next)) if is_mergeable_batch(&next) => {
                            messages.extend(next)
                        }
                        Ok(next) => {
                            pending = Some(next);
                            break;
                        }
                        // Empty, or disconnected: the next `recv` reports the close.
                        Err(_) => break,
                    }
                }
            }
            if !messages.is_empty() {
                yield SourceMessageEvent::Data(messages);
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
        // The channel closes once the task drops its senders, so surface why it ended.
        match self.reader_task.await {
            Ok(Ok(())) => bail!("Spanner CDC reader channel closed"),
            Ok(Err(e)) => return Err(e),
            Err(e) => bail!("Spanner CDC reader task failed: {}", e.as_report()),
        }
    }
}

/// Whether a batch holds only data rows and may be merged with its neighbours.
///
/// Schema changes stay alone in their batch: the parser applies one before parsing the
/// messages after it, and rows before it in the same batch would be held back until the
/// change is applied. Heartbeats stay alone too, since the parser emits only the first
/// heartbeat of a batch, and merging would drop the offset progress of the later ones.
fn is_mergeable_batch(messages: &[SourceMessage]) -> bool {
    messages.iter().all(|msg| {
        !msg.is_cdc_heartbeat()
            && !matches!(
                &msg.meta,
                SourceMeta::DebeziumCdc(meta)
                    if matches!(meta.msg_type, crate::source::cdc::CdcMessageType::SchemaChange)
            )
    })
}

// ---------------------------------------------------------------------------
// Change stream queries
// ---------------------------------------------------------------------------

/// One change stream query for one partition.
#[derive(Clone, Debug)]
struct PartitionQuery {
    /// `None` for the root partition.
    partition_token: Option<String>,
    start_timestamp: OffsetDateTime,
    heartbeat_milliseconds: i64,
}

/// Why reading the next row of a change stream query failed.
enum RowError {
    /// Spanner returned an error mid-stream.
    Spanner(google_cloud_spanner::Error),
    /// The row's `ChangeRecord` column could not be read as JSON.
    Decode(anyhow::Error),
}

/// The rows of one change stream query, each the JSON of its `ChangeRecord` column.
type ChangeRecordRows = BoxStream<'static, std::result::Result<JsonValue, RowError>>;

/// Runs change stream queries: the only part of the reader that talks to Spanner.
///
/// Partition lifecycle, retries, timeouts and record handling all sit above this
/// trait, so tests drive them through a scripted implementation instead of Spanner.
#[async_trait]
trait ChangeStreamQuerier: Send + Sync {
    /// Start `query`, returning its rows once Spanner has accepted it.
    async fn query(
        &self,
        query: PartitionQuery,
    ) -> std::result::Result<ChangeRecordRows, google_cloud_spanner::Error>;
}

/// [`ChangeStreamQuerier`] backed by a Spanner database client.
struct SpannerChangeStreamQuerier {
    client: DatabaseClient,
    sql: String,
}

impl SpannerChangeStreamQuerier {
    fn new(client: DatabaseClient, change_stream_name: &str) -> Self {
        // Workaround: the googleapis SDK's `OffsetDateTime::to_value()` formats
        // timestamps with 9-digit subsecond precision (nanoseconds, padded with
        // zeros for microsecond-precision values). Spanner's TIMESTAMP literal
        // parser rejects that, so we format the timestamp ourselves with
        // microsecond precision and bind it as a string.
        let sql = format!(
            "SELECT ChangeRecord FROM READ_{}(start_timestamp => @start_timestamp, end_timestamp => @end_timestamp, partition_token => @partition_token, heartbeat_milliseconds => @heartbeat_milliseconds)",
            change_stream_name
        );
        Self { client, sql }
    }
}

#[async_trait]
impl ChangeStreamQuerier for SpannerChangeStreamQuerier {
    async fn query(
        &self,
        query: PartitionQuery,
    ) -> std::result::Result<ChangeRecordRows, google_cloud_spanner::Error> {
        // The SDK retries a failed query or stream by default, with up to 10 attempts and
        // backoff of up to a minute, all inside a single `execute_query` or `next` call.
        // That is invisible to the stall timeout around those calls, which then fires and
        // reports a timeout for what was really a retried error. `RetryBudget` is the only
        // retry layer: it counts failures, resets on progress and resumes from the
        // advanced offset.
        let stmt = Statement::builder(&self.sql)
            .with_retry_policy(NeverRetry)
            .add_typed_param("start_timestamp", query.start_timestamp, types::timestamp())
            .add_typed_param(
                "end_timestamp",
                Option::<OffsetDateTime>::None,
                types::timestamp(),
            )
            .add_typed_param("partition_token", &query.partition_token, types::string())
            .add_typed_param(
                "heartbeat_milliseconds",
                query.heartbeat_milliseconds,
                types::int64(),
            )
            .build();
        let result_set = self.client.single_use().build().execute_query(stmt).await?;
        let rows = futures::stream::unfold(result_set, |mut result_set| async move {
            let row = match result_set.next().await? {
                Ok(row) => crate::source::spanner_cdc::types::change_record_column(&row, 0)
                    .map_err(RowError::Decode),
                Err(e) => Err(RowError::Spanner(e)),
            };
            Some((row, result_set))
        });
        Ok(rows.boxed())
    }
}

// ---------------------------------------------------------------------------
// Background reader task (equivalent to Debezium's JNI thread)
// ---------------------------------------------------------------------------

struct ReaderContext {
    querier: Arc<dyn ChangeStreamQuerier>,
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
    /// Unfinished partitions saved in the split; when present they are resumed
    /// instead of the root query from `checkpointed_offset`.
    saved_partitions: Arc<[PartitionProgress]>,
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
    /// A record used a value capture type that yields partial rows.
    UnsupportedValueCaptureType,
    /// The query ended without naming the partitions that take over its key range.
    NoChildPartitions,
}

impl QueryFailure {
    const ALL: [Self; 7] = [
        Self::Establish,
        Self::Stall,
        Self::Query,
        Self::Row,
        Self::Decode,
        Self::UnsupportedValueCaptureType,
        Self::NoChildPartitions,
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
            Self::UnsupportedValueCaptureType => 5,
            Self::NoChildPartitions => 6,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Establish => "establish_timeout",
            Self::Stall => "stall_timeout",
            Self::Query => "query_error",
            Self::Row => "row_error",
            Self::Decode => "decode_error",
            Self::UnsupportedValueCaptureType => "unsupported_value_capture_type",
            Self::NoChildPartitions => "no_child_partitions",
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
/// - `offsets`: token → current offset and parents (O(1) lookup)
/// - `counts`: offset → count of partitions at that offset (O(1) watermark via first key)
///
/// Shared between the main loop and partition tasks via `Arc`.
/// Partition tasks update their offset once they have sent the records up to it.
/// The main loop computes the watermark from this map, and saves it as progress.
///
/// A partition's entry is removed when it finishes, so the watermark
/// only reflects un-finished partitions.
struct PartitionOffsets {
    inner: std::sync::Mutex<PartitionOffsetsInner>,
}

struct PartitionOffsetsInner {
    offsets: HashMap<Option<String>, PartitionEntry>,
    counts: BTreeMap<OffsetDateTime, usize>,
}

struct PartitionEntry {
    offset: OffsetDateTime,
    parents: Vec<String>,
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

    /// Register a partition with its start offset. A no-op if the partition is
    /// already registered: a child is registered first by the parent task that
    /// reported it, then again by the lifecycle loop, and a merged child is
    /// reported by every parent.
    ///
    /// Relies on a partition never being reported again after it finished and was
    /// removed. That holds because a child starts only after all of its parents
    /// finished, and a finished parent reports nothing more.
    fn register(&self, token: Option<String>, start_ts: OffsetDateTime, parents: &[String]) {
        let mut inner = self.inner.lock().unwrap();
        if inner.offsets.contains_key(&token) {
            return;
        }
        inner.offsets.insert(
            token,
            PartitionEntry {
                offset: start_ts,
                parents: parents.to_vec(),
            },
        );
        *inner.counts.entry(start_ts).or_insert(0) += 1;
    }

    /// Update a partition's offset (called by partition tasks).
    fn update(&self, token: &Option<String>, offset: OffsetDateTime) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.offsets.get_mut(token)
            && offset > entry.offset
        {
            let old = entry.offset;
            entry.offset = offset;

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
        if let Some(entry) = inner.offsets.remove(token)
            && let Some(count) = inner.counts.get_mut(&entry.offset)
        {
            *count -= 1;
            if *count == 0 {
                inner.counts.remove(&entry.offset);
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

    /// Every registered partition, read under one lock, sorted by token.
    ///
    /// A valid restore point at any moment: a child is registered before its parent
    /// can finish and be removed, so every partition missing from it has finished and
    /// its children are in it.
    fn snapshot(&self) -> Vec<PartitionProgress> {
        let inner = self.inner.lock().unwrap();
        let mut partitions: Vec<_> = inner
            .offsets
            .iter()
            .map(|(token, entry)| PartitionProgress {
                token: token.clone(),
                parents: entry.parents.clone(),
                offset: entry.offset,
            })
            .collect();
        partitions.sort_unstable_by(|a, b| a.token.cmp(&b.token));
        partitions
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
        &child.parent_partition_tokens,
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
async fn run_reader(ctx: ReaderContext, tx: mpsc::Sender<SourceMessageEvent>) -> Result<()> {
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

    let reader_metrics = Arc::new(ReaderMetrics::new(
        &ctx.metrics,
        &ctx.source_id.to_string(),
        &ctx.source_name,
        &ctx.fragment_id,
    ));

    if ctx.saved_partitions.is_empty() {
        let root_offset = ctx
            .checkpointed_offset
            .unwrap_or_else(OffsetDateTime::now_utc);
        tracing::info!(starting_offset = ?root_offset, "starting Spanner CDC reader");

        // Spawn root partition.
        let root_split =
            SpannerCdcSplit::new_root(ctx.change_stream_name.clone(), ctx.source_id, root_offset);
        let root_token = root_split.partition_token.clone();
        offsets.register(root_token.clone(), root_offset, &[]);
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
    } else {
        tracing::info!(
            partitions = ctx.saved_partitions.len(),
            watermark = ?ctx.saved_partitions.iter().map(|p| p.offset).min(),
            "resuming Spanner CDC reader from saved partitions"
        );
        restore_partitions(
            &ctx,
            &offsets,
            &mut discovered,
            &mut deferred,
            &mut ready_pool,
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

    // The loop otherwise only wakes on partition completion or child discovery.
    // A partition that is streaming but falling behind produces neither, so
    // without this tick the gauges would freeze for exactly the incident they
    // are meant to show, and progress would be reported only when the tree changes.
    let mut progress_tick = tokio::time::interval(PROGRESS_INTERVAL);
    progress_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

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

            // Re-sample the gauges (at the top of the loop) and report progress. Listed
            // last so `biased` still prioritises partition progress.
            _ = progress_tick.tick() => {
                // Sent through the data channel, so it follows every batch the partitions
                // sent before their offsets advanced. Skipped while the channel is full,
                // so backpressure never stalls the lifecycle loop; the next tick retries.
                let progress = SpannerCdcSplit::encode_partition_progress(offsets.snapshot());
                let _ = tx.try_send(SourceMessageEvent::SplitProgress(HashMap::from([(
                    split_id.clone(),
                    progress,
                )])));
            }
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

/// Register the saved partitions and route each to `ready_pool` or `deferred`.
///
/// Only unfinished partitions are saved, so every partition the saved ones name as a
/// parent but that is not saved itself has finished, and so has the root unless it is
/// saved. Marking them finished lets their children start; otherwise
/// `parents_all_finished` would treat them as unknown and hold the children forever.
fn restore_partitions(
    ctx: &ReaderContext,
    offsets: &PartitionOffsets,
    discovered: &mut HashMap<Option<String>, bool>,
    deferred: &mut Vec<SpannerCdcSplit>,
    ready_pool: &mut Vec<SpannerCdcSplit>,
) {
    for p in ctx.saved_partitions.iter() {
        offsets.register(p.token.clone(), p.offset, &p.parents);
        discovered.insert(p.token.clone(), false);
    }
    discovered.entry(None).or_insert(true);
    for parent in ctx.saved_partitions.iter().flat_map(|p| &p.parents) {
        discovered.entry(Some(parent.clone())).or_insert(true);
    }

    for p in ctx.saved_partitions.iter() {
        let split = match &p.token {
            None => {
                SpannerCdcSplit::new_root(ctx.change_stream_name.clone(), ctx.source_id, p.offset)
            }
            // Index 0, as for children discovered while reading; see `execute_query`.
            Some(token) => SpannerCdcSplit::new_child(
                token.clone(),
                p.parents.clone(),
                p.offset,
                ctx.change_stream_name.clone(),
                0,
            ),
        };
        if split.is_root() || parents_all_finished(&split.parent_partition_tokens, discovered) {
            ready_pool.push(split);
        } else {
            deferred.push(split);
        }
    }
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
    tx: &mpsc::Sender<SourceMessageEvent>,
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
    tx: &mpsc::Sender<SourceMessageEvent>,
    partition_streams: &mut FuturesUnordered<tokio::task::JoinHandle<Result<PartitionResult>>>,
    child_discovery_tx: tokio::sync::mpsc::UnboundedSender<SpannerCdcSplit>,
    reader_metrics: &Arc<ReaderMetrics>,
) {
    let querier = ctx.querier.clone();
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
            querier,
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
    querier: Arc<dyn ChangeStreamQuerier>,
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
    tx: mpsc::Sender<SourceMessageEvent>,
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

    tracing::info!(%split_id, %start_ts, partition_token = ?split.partition_token, "change stream query starting");

    let mut retry = RetryBudget::new(
        retry_attempts,
        retry_backoff,
        retry_backoff_max_delay_ms,
        retry_backoff_factor,
    );

    loop {
        let resume_ts = split
            .offset
            .expect("offset validated at entry and only advanced by advance_offset");
        let query = PartitionQuery {
            partition_token: split.partition_token.clone(),
            start_timestamp: resume_ts,
            heartbeat_milliseconds: heartbeat_interval_ms,
        };
        let Err(e) = Box::pin(execute_query(
            &*querier,
            query,
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
        else {
            return Ok(());
        };

        // Retention only moves forward, so an expired start timestamp never recovers.
        let retryable = e.0.downcast_ref::<StartBeforeRetention>().is_none();
        let delay = retry.on_failure(split.offset > Some(resume_ts), retryable);
        let will_retry = delay.is_some();
        tracing::warn!(
            %split_id,
            consecutive_failures = retry.failures,
            max_attempts = retry_attempts,
            ?delay,
            error = %e,
            resume_ts = ?resume_ts,
            will_retry,
            "query failed"
        );
        let Some(delay) = delay else {
            return Err(e);
        };
        tokio::time::sleep(delay).await;
    }
}

#[expect(clippy::too_many_arguments)]
async fn execute_query(
    querier: &dyn ChangeStreamQuerier,
    query: PartitionQuery,
    split: &mut SpannerCdcSplit,
    split_id: &SplitId,
    database: &str,
    offsets: &PartitionOffsets,
    shared_schema: &std::sync::Mutex<SchemaTracker>,
    tx: &mpsc::Sender<SourceMessageEvent>,
    child_discovery_tx: &tokio::sync::mpsc::UnboundedSender<SpannerCdcSplit>,
    change_stream_name: &str,
    stall_timeout: std::time::Duration,
    reader_metrics: &ReaderMetrics,
) -> Result<()> {
    reader_metrics.queries.inc();
    let mut rows = match tokio::time::timeout(stall_timeout, querier.query(query)).await {
        Ok(Ok(rows)) => rows,
        Ok(Err(e)) => {
            reader_metrics.record_failure(QueryFailure::Query);
            if let Some(expired) = StartBeforeRetention::from_spanner(&e, split) {
                return Err(anyhow::Error::new(expired).into());
            }
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
    let mut saw_child_partitions = false;

    loop {
        let json = match tokio::time::timeout(stall_timeout, rows.next()).await {
            Ok(Some(Ok(json))) => json,
            Ok(Some(Err(RowError::Decode(e)))) => {
                reader_metrics.record_failure(QueryFailure::Decode);
                return Err(e.into());
            }
            Ok(Some(Err(RowError::Spanner(e)))) => {
                reader_metrics.record_failure(QueryFailure::Row);
                if let Some(expired) = StartBeforeRetention::from_spanner(&e, split) {
                    return Err(anyhow::Error::new(expired).into());
                }
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

        let change_records = crate::source::spanner_cdc::types::parse_change_record_json(json)
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

                // The enumerator validates the stream at creation, but the capture type
                // can be changed later with ALTER CHANGE STREAM. Fail rather than write
                // rows whose unmodified columns are NULL.
                check_value_capture_type(data_change).inspect_err(|_| {
                    reader_metrics.record_failure(QueryFailure::UnsupportedValueCaptureType);
                })?;

                split.advance_offset(data_change.commit_time());
                // Use watermark (min of all un-finished partitions) for checkpoint offset.
                let wm = offsets
                    .watermark()
                    .expect("partition active, watermark must exist");
                let offset_str = offset_cache.get((wm.unix_timestamp_nanos() / 1000) as i64);

                // Schema evolution: emit schema change before data records,
                // mimicking Debezium's Relation messages that precede DML events.
                if !send_schema_change_if_evolved(
                    shared_schema,
                    tx,
                    split_id,
                    data_change,
                    offset_str,
                    &mut messages,
                )
                .await
                {
                    return Ok(());
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
                if tx.send(SourceMessageEvent::Data(messages)).await.is_err() {
                    return Ok(());
                }
                // Only now, with the records up to it in the channel: saved progress must
                // never claim records that were not sent. The watermark stamped on this
                // batch is therefore at most the previous offset, which is only lower.
                offsets.update(
                    &split.partition_token,
                    split.offset.expect("advanced above"),
                );
            }

            // Child partition discovery
            for cpr in &record.child_partitions_record {
                saw_child_partitions = true;
                let start_time = cpr.start_time();
                for cp in &cpr.child_partitions {
                    // Register the child's offset here, before this task can finish.
                    // The lifecycle loop removes this partition's offset as soon as
                    // the task completes, which may be before it drains the discovery
                    // channel. Without this, the watermark would briefly exclude the
                    // child and could jump past its start, and a checkpoint taken in
                    // that window would skip the child's first records on recovery.
                    offsets.register(
                        Some(cp.token.clone()),
                        start_time,
                        &cp.parent_partition_tokens,
                    );
                    // Index 0 is a placeholder, so `id()` is the same for every child.
                    // Children live only inside this reader and are tracked by token; all
                    // their messages carry the root's `split_id`. Give each child a unique
                    // id before ever persisting or assigning children as separate splits.
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

    // The query has no end timestamp, so it ends only once the partition has named the
    // partitions that take over its key range. Ending without them would leave that range
    // unread for good; fail the query so it is retried, as Debezium's Spanner connector does.
    if !saw_child_partitions {
        reader_metrics.record_failure(QueryFailure::NoChildPartitions);
        return Err(anyhow::anyhow!(
            "change stream query for partition {:?} ended without child partitions",
            split.partition_token,
        )
        .into());
    }
    tracing::info!(%split_id, final_offset = ?split.offset, "change stream result set exhausted");
    Ok(())
}

/// Retry budget for one partition's change stream queries.
///
/// `max_attempts` bounds back-to-back failures, not failures over the partition's
/// lifetime: a query that advanced the offset before failing resets the count and the
/// backoff, so a long-lived partition is not failed by scattered transient errors.
/// Values 0 and 1 both mean a single attempt.
struct RetryBudget {
    max_attempts: u32,
    failures: u32,
    initial_backoff: Backoff,
    backoff: Backoff,
}

type Backoff = std::iter::Map<ExponentialBackoff, fn(std::time::Duration) -> std::time::Duration>;

impl RetryBudget {
    fn new(max_attempts: u32, base: std::time::Duration, max_delay_ms: u64, factor: u64) -> Self {
        let initial_backoff = ExponentialBackoff::from_millis(base.as_millis() as u64)
            .max_delay(std::time::Duration::from_millis(max_delay_ms))
            .factor(factor)
            .map(jitter as fn(_) -> _);
        Self {
            max_attempts,
            failures: 0,
            backoff: initial_backoff.clone(),
            initial_backoff,
        }
    }

    /// Count a failed query; returns the delay before the next attempt, or `None` to
    /// give up.
    fn on_failure(&mut self, made_progress: bool, retryable: bool) -> Option<std::time::Duration> {
        if made_progress {
            self.failures = 0;
            self.backoff = self.initial_backoff.clone();
        }
        self.failures += 1;
        if !retryable || self.failures >= self.max_attempts {
            return None;
        }
        Some(self.backoff.next().expect("backoff is unbounded"))
    }
}

/// Spanner rejected a query because its start timestamp is older than the change
/// stream's retention period. The emulator reports this as `OUT_OF_RANGE` with
/// "Specified `start_timestamp` is too far in the past"; any other wording falls back
/// to the generic, retried query error.
#[derive(Debug, thiserror::Error)]
#[error(
    "partition {partition_token:?} cannot resume from {start_ts}, which is older than the \
     change stream's retention period: changes between that time and the oldest retained \
     change are lost. Recreate the source and its tables to take a new snapshot. \
     Spanner: {message}"
)]
struct StartBeforeRetention {
    partition_token: Option<String>,
    start_ts: OffsetDateTime,
    message: String,
}

impl StartBeforeRetention {
    fn from_spanner(e: &google_cloud_spanner::Error, split: &SpannerCdcSplit) -> Option<Self> {
        let status = e.status()?;
        if status.code != Code::OutOfRange || !status.message.contains("too far in the past") {
            return None;
        }
        Some(Self {
            partition_token: split.partition_token.clone(),
            start_ts: split.offset?,
            message: status.message.clone(),
        })
    }
}

/// Reject a record whose value capture type omits unmodified columns on UPDATE.
fn check_value_capture_type(
    data_change: &crate::source::spanner_cdc::types::DataChangeRecord,
) -> Result<()> {
    if SUPPORTED_VALUE_CAPTURE_TYPES.contains(&data_change.value_capture_type.as_str()) {
        return Ok(());
    }
    Err(anyhow::anyhow!(
        "change stream record for table '{}' uses value_capture_type '{}', which is not \
         supported; set the change stream to one of {:?}",
        data_change.table_name,
        data_change.value_capture_type,
        SUPPORTED_VALUE_CAPTURE_TYPES,
    )
    .into())
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

/// Sends the schema change message for `data_change` if its columns evolve the table's
/// schema, after flushing `messages`, which hold this partition's earlier rows.
///
/// The tracker records the new schema and the message enters the channel under one lock,
/// through a channel slot reserved beforehand. A partition that then sees the new schema
/// and skips emission can only send its rows after this message, so the parser applies
/// the schema change before reading them.
///
/// Returns `false` if the receiver has been dropped.
async fn send_schema_change_if_evolved(
    shared_schema: &std::sync::Mutex<SchemaTracker>,
    tx: &mpsc::Sender<SourceMessageEvent>,
    split_id: &SplitId,
    data_change: &crate::source::spanner_cdc::types::DataChangeRecord,
    offset_str: &str,
    messages: &mut Vec<SourceMessage>,
) -> bool {
    let table_name = &data_change.table_name;
    let column_types = &data_change.column_types;
    let commit_ts = data_change.commit_time();
    if !shared_schema
        .lock()
        .unwrap()
        .needs_emit(table_name, column_types, commit_ts)
    {
        return true;
    }
    if !messages.is_empty()
        && tx
            .send(SourceMessageEvent::Data(std::mem::take(messages)))
            .await
            .is_err()
    {
        return false;
    }
    let Ok(permit) = tx.reserve().await else {
        return false;
    };
    // Re-check under the lock: another partition may have emitted it while we waited.
    let mut tracker = shared_schema.lock().unwrap();
    if let Some(schema_payload) = tracker.check_and_evolve(table_name, column_types, commit_ts) {
        permit.send(SourceMessageEvent::Data(vec![make_schema_change_msg(
            split_id,
            schema_payload.json,
            data_change,
            offset_str,
        )]));
    }
    true
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
    use std::time::Duration;

    use risingwave_common::array::StreamChunk;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use time::macros::datetime;

    use super::*;
    use crate::source::SplitMetaData;

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

    fn chunk_event(pretty: &str) -> Result<SourceReaderEvent> {
        Ok(SourceReaderEvent::DataChunk(StreamChunk::from_pretty(
            pretty,
        )))
    }

    #[tokio::test]
    async fn test_forward_parsed_events_yields_in_order() {
        let (tx, rx) = mpsc::channel(PARSED_CHUNK_CHANNEL_SIZE);
        tx.send(chunk_event("I\n + 1")).await.unwrap();
        tx.send(Ok(SourceReaderEvent::SplitProgress(HashMap::new())))
            .await
            .unwrap();
        tx.send(chunk_event("I\n + 2")).await.unwrap();
        drop(tx);

        let events: Vec<_> =
            SpannerCdcSplitReader::forward_parsed_events(rx, test_queue_depth_gauge())
                .collect()
                .await;
        assert_eq!(events.len(), 3);
        assert!(matches!(&events[0], Ok(SourceReaderEvent::DataChunk(c)) if c.cardinality() == 1));
        assert!(matches!(
            &events[1],
            Ok(SourceReaderEvent::SplitProgress(_))
        ));
        assert!(matches!(&events[2], Ok(SourceReaderEvent::DataChunk(c)) if c.cardinality() == 1));
    }

    /// A parser error surfaces as `Err` rather than a clean end of stream, so the
    /// source fails loudly instead of going quiet.
    #[tokio::test]
    async fn test_forward_parsed_events_propagates_error() {
        let (tx, rx) = mpsc::channel(PARSED_CHUNK_CHANNEL_SIZE);
        tx.send(chunk_event("I\n + 1")).await.unwrap();
        tx.send(Err(anyhow::anyhow!("parser blew up").into()))
            .await
            .unwrap();
        drop(tx);

        let chunks: Vec<_> =
            SpannerCdcSplitReader::forward_parsed_events(rx, test_queue_depth_gauge())
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
    async fn test_forward_parsed_events_ends_when_sender_dropped() {
        let (tx, rx) = mpsc::channel::<Result<SourceReaderEvent>>(PARSED_CHUNK_CHANNEL_SIZE);
        drop(tx);

        let chunks: Vec<_> =
            SpannerCdcSplitReader::forward_parsed_events(rx, test_queue_depth_gauge())
                .collect()
                .await;
        assert!(chunks.is_empty());
    }

    fn test_split_reader(reader_task: JoinHandle<Result<()>>) -> SpannerCdcSplitReader {
        // The sender is dropped at once, as when the reader task has exited.
        let (_, rx) = mpsc::channel(1);
        SpannerCdcSplitReader {
            rx,
            reader_task,
            parser_config: ParserConfig::default(),
            source_ctx: Arc::new(crate::source::SourceContext::dummy()),
        }
    }

    #[tokio::test]
    async fn test_data_stream_returns_reader_task_error() {
        let reader = test_split_reader(tokio::spawn(async {
            Err(anyhow::anyhow!("partition query failed").into())
        }));
        let err = std::pin::pin!(reader.into_data_stream())
            .next()
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            err.to_report_string().contains("partition query failed"),
            "{}",
            err.as_report()
        );
    }

    fn test_message(msg_type: cdc_message::CdcMessageType, offset: &str) -> SourceMessage {
        let is_heartbeat = matches!(msg_type, cdc_message::CdcMessageType::Heartbeat);
        SourceMessage {
            key: None,
            payload: (!is_heartbeat).then(|| b"{}".to_vec()),
            offset: offset.to_owned(),
            split_id: "0".into(),
            meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
                "db.t".to_owned(),
                0,
                msg_type,
                SourceType::Unspecified,
            )),
        }
    }

    fn data(offset: &str) -> SourceMessage {
        test_message(cdc_message::CdcMessageType::Data, offset)
    }

    /// Queue `events` behind a finished reader task and collect the merged batches, as
    /// offsets, until the stream reports the closed channel. A progress report is
    /// collected as `["progress"]`.
    async fn collect_data_stream(
        events: Vec<SourceMessageEvent>,
        chunk_size: usize,
    ) -> Vec<Vec<String>> {
        let (tx, rx) = mpsc::channel(events.len().max(1));
        for event in events {
            tx.send(event).await.unwrap();
        }
        drop(tx);
        let mut source_ctx = crate::source::SourceContext::dummy();
        source_ctx.source_ctrl_opts.chunk_size = chunk_size;
        let reader = SpannerCdcSplitReader {
            rx,
            reader_task: tokio::spawn(async { Ok(()) }),
            parser_config: ParserConfig::default(),
            source_ctx: Arc::new(source_ctx),
        };
        let mut stream = std::pin::pin!(reader.into_data_stream());
        let mut merged = vec![];
        while let Some(Ok(event)) = stream.next().await {
            merged.push(match event {
                SourceMessageEvent::Data(batch) => {
                    batch.into_iter().map(|msg| msg.offset).collect()
                }
                SourceMessageEvent::SplitProgress(_) => vec!["progress".to_owned()],
            });
        }
        merged
    }

    fn batch(messages: Vec<SourceMessage>) -> SourceMessageEvent {
        SourceMessageEvent::Data(messages)
    }

    #[tokio::test]
    async fn test_data_stream_merges_queued_data_batches() {
        let merged = collect_data_stream(
            vec![
                batch(vec![data("1")]),
                batch(vec![data("2"), data("3")]),
                batch(vec![data("4")]),
            ],
            1024,
        )
        .await;
        assert_eq!(merged, vec![vec!["1", "2", "3", "4"]]);
    }

    /// Merging stops once a batch reaches the chunk size; a batch is never split.
    #[tokio::test]
    async fn test_data_stream_merge_stops_at_chunk_size() {
        let merged = collect_data_stream(
            vec![
                batch(vec![data("1")]),
                batch(vec![data("2"), data("3")]),
                batch(vec![data("4")]),
                batch(vec![data("5")]),
            ],
            3,
        )
        .await;
        assert_eq!(merged, vec![vec!["1", "2", "3"], vec!["4", "5"]]);
    }

    /// Schema changes, heartbeats and progress reports keep their own place, and order
    /// is preserved, so a progress report never moves ahead of the batches it covers.
    #[tokio::test]
    async fn test_data_stream_never_merges_schema_change_heartbeat_or_progress() {
        let schema_change = test_message(cdc_message::CdcMessageType::SchemaChange, "s");
        let heartbeat = |offset| test_message(cdc_message::CdcMessageType::Heartbeat, offset);
        let merged = collect_data_stream(
            vec![
                batch(vec![data("1")]),
                batch(vec![data("2")]),
                batch(vec![schema_change]),
                batch(vec![data("3")]),
                batch(vec![heartbeat("h1")]),
                batch(vec![heartbeat("h2")]),
                batch(vec![data("4")]),
                SourceMessageEvent::SplitProgress(HashMap::new()),
                batch(vec![data("5")]),
                batch(vec![data("6")]),
            ],
            1024,
        )
        .await;
        assert_eq!(
            merged,
            vec![
                vec!["1", "2"],
                vec!["s"],
                vec!["3"],
                vec!["h1"],
                vec!["h2"],
                vec!["4"],
                vec!["progress"],
                vec!["5", "6"],
            ]
        );
    }

    #[tokio::test]
    async fn test_data_stream_reports_reader_task_panic() {
        let reader = test_split_reader(tokio::spawn(async { panic!("reader bug") }));
        let err = std::pin::pin!(reader.into_data_stream())
            .next()
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            err.to_report_string().contains("reader bug"),
            "{}",
            err.as_report()
        );
    }

    /// `offset_bounds` reads both ends of one `BTreeMap` iterator, so the
    /// single-key case (where `next_back` is already exhausted) needs covering.
    #[test]
    fn test_offset_bounds() {
        let offsets = PartitionOffsets::new();
        assert_eq!(offsets.offset_bounds(), (None, None));

        let t0 = OffsetDateTime::from_unix_timestamp(1_000).unwrap();
        offsets.register(Some("a".to_owned()), t0, &[]);
        assert_eq!(
            offsets.offset_bounds(),
            (Some(t0), Some(t0)),
            "a single partition is both the min and the max"
        );

        let t1 = OffsetDateTime::from_unix_timestamp(2_000).unwrap();
        let t2 = OffsetDateTime::from_unix_timestamp(3_000).unwrap();
        offsets.register(Some("b".to_owned()), t1, &[]);
        offsets.register(Some("c".to_owned()), t2, &[]);
        assert_eq!(offsets.offset_bounds(), (Some(t0), Some(t2)));

        // The straggler finishing lifts the watermark to the next oldest.
        offsets.remove(&Some("a".to_owned()));
        assert_eq!(offsets.offset_bounds(), (Some(t1), Some(t2)));
    }

    /// Registering a partition twice must not double-count it, or removing it once
    /// would leave a phantom entry pinning the watermark.
    #[test]
    fn test_register_is_idempotent() {
        let offsets = PartitionOffsets::new();
        let t1 = datetime!(2025-01-01 0:00 UTC);
        let t2 = datetime!(2025-01-02 0:00 UTC);

        offsets.register(Some("C".to_owned()), t1, &[]);
        // A later registration neither double-counts nor moves the offset.
        offsets.register(Some("C".to_owned()), t2, &[]);
        assert_eq!(offsets.watermark(), Some(t1));

        offsets.remove(&Some("C".to_owned()));
        assert_eq!(offsets.watermark(), None);
    }

    #[test]
    fn test_retry_budget_counts_consecutive_failures() {
        let new = |max| RetryBudget::new(max, std::time::Duration::from_millis(1), 10, 2);

        let mut retry = new(3);
        assert!(retry.on_failure(false, true).is_some());
        assert!(retry.on_failure(false, true).is_some());
        assert!(retry.on_failure(false, true).is_none());

        // A failure after progress starts a new run of failures.
        let mut retry = new(3);
        assert!(retry.on_failure(false, true).is_some());
        assert!(retry.on_failure(false, true).is_some());
        assert!(retry.on_failure(true, true).is_some());
        assert_eq!(retry.failures, 1);
        assert!(retry.on_failure(false, true).is_some());
        assert!(retry.on_failure(false, true).is_none());

        // A non-retryable error gives up at once.
        assert!(new(3).on_failure(false, false).is_none());
        assert!(new(3).on_failure(true, false).is_none());

        // 0 and 1 both mean a single attempt.
        assert!(new(0).on_failure(false, true).is_none());
        assert!(new(1).on_failure(false, true).is_none());
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

        offsets.register(Some("A".to_owned()), t1, &[]);
        offsets.register(Some("B".to_owned()), t3, &[]);
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

        offsets.register(Some("A".to_owned()), t2, &[]);
        offsets.update(&Some("A".to_owned()), t1);
        assert_eq!(offsets.watermark(), Some(t2));
    }

    #[test]
    fn test_partition_offsets_counts_tracking() {
        let offsets = PartitionOffsets::new();
        let t1 = datetime!(2025-01-01 0:00 UTC);
        let t2 = datetime!(2025-01-02 0:00 UTC);

        offsets.register(Some("A".to_owned()), t1, &[]);
        offsets.register(Some("B".to_owned()), t1, &[]);
        offsets.register(Some("C".to_owned()), t2, &[]);

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

        offsets.register(Some("A".to_owned()), t1, &[]);
        offsets.register(Some("B".to_owned()), t3, &[]);
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
    // run_reader: the partition lifecycle, driven by a scripted querier.
    // -----------------------------------------------------------------------

    /// One scripted query result for [`FakeQuerier`].
    enum FakeQuery {
        /// Spanner rejects the query.
        Fail(google_cloud_spanner::Error),
        /// The query never starts.
        Hang,
        /// The partition task panics.
        Panic,
        /// The query yields `rows`, then ends, or stays open when `then_hang` is set.
        Rows {
            rows: Vec<std::result::Result<JsonValue, google_cloud_spanner::Error>>,
            then_hang: bool,
        },
    }

    /// Answers each partition's queries from a script, in order, and records every
    /// query. A partition with no script left stays open without sending anything.
    #[derive(Default)]
    struct FakeQuerier {
        scripts: std::sync::Mutex<HashMap<Option<String>, Vec<FakeQuery>>>,
        calls: std::sync::Mutex<Vec<PartitionQuery>>,
    }

    impl FakeQuerier {
        fn script(self, token: Option<&str>, mut queries: Vec<FakeQuery>) -> Self {
            // Stored reversed so `pop` returns them in order.
            queries.reverse();
            self.scripts
                .lock()
                .unwrap()
                .insert(token.map(str::to_owned), queries);
            self
        }

        fn calls(&self) -> Vec<PartitionQuery> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ChangeStreamQuerier for FakeQuerier {
        async fn query(
            &self,
            query: PartitionQuery,
        ) -> std::result::Result<ChangeRecordRows, google_cloud_spanner::Error> {
            self.calls.lock().unwrap().push(query.clone());
            let next = self
                .scripts
                .lock()
                .unwrap()
                .get_mut(&query.partition_token)
                .and_then(Vec::pop);
            match next {
                Some(FakeQuery::Fail(e)) => Err(e),
                Some(FakeQuery::Hang) => futures::future::pending().await,
                Some(FakeQuery::Panic) => panic!("simulated partition panic"),
                Some(FakeQuery::Rows { rows, then_hang }) => {
                    let rows = futures::stream::iter(
                        rows.into_iter().map(|row| row.map_err(RowError::Spanner)),
                    );
                    Ok(if then_hang {
                        rows.chain(futures::stream::pending()).boxed()
                    } else {
                        rows.boxed()
                    })
                }
                None => Ok(futures::stream::pending().boxed()),
            }
        }
    }

    const T0: OffsetDateTime = datetime!(2026-01-01 00:00:00 UTC);

    fn rfc3339(ts: OffsetDateTime) -> String {
        ts.format(&time::format_description::well_known::Rfc3339)
            .unwrap()
    }

    fn heartbeat_row(ts: OffsetDateTime) -> JsonValue {
        serde_json::json!([{ "heartbeat_record": [{ "timestamp": rfc3339(ts) }] }])
    }

    fn children_row(start: OffsetDateTime, children: &[(&str, &[&str])]) -> JsonValue {
        let children: Vec<_> = children
            .iter()
            .map(|(token, parents)| {
                serde_json::json!({ "token": token, "parent_partition_tokens": parents })
            })
            .collect();
        serde_json::json!([{ "child_partitions_record": [{
            "start_timestamp": rfc3339(start),
            "record_sequence": "00000001",
            "child_partitions": children,
        }] }])
    }

    fn spanner_error(code: Code, message: &str) -> google_cloud_spanner::Error {
        google_cloud_spanner::Error::service(
            googleapis_gax::error::rpc::Status::default()
                .set_code(code)
                .set_message(message),
        )
    }

    fn rows(rows: Vec<JsonValue>) -> FakeQuery {
        FakeQuery::Rows {
            rows: rows.into_iter().map(Ok).collect(),
            then_hang: false,
        }
    }

    fn test_reader_context(querier: Arc<FakeQuerier>, retry_attempts: u32) -> ReaderContext {
        ReaderContext {
            querier,
            database: "db".to_owned(),
            change_stream_name: "stream".to_owned(),
            heartbeat_interval_ms: 1000,
            retry_attempts,
            retry_backoff: Duration::from_millis(1),
            retry_backoff_max_delay_ms: 10,
            retry_backoff_factor: 2,
            stall_timeout: Duration::from_secs(60),
            source_id: 1,
            checkpointed_offset: Some(T0),
            saved_partitions: Arc::new([]),
            metrics: Arc::new(SourceMetrics::default()),
            source_name: "test".to_owned(),
            fragment_id: "0".to_owned(),
        }
    }

    /// Receive messages until a heartbeat at `ts` arrives.
    async fn recv_heartbeat_at(rx: &mut mpsc::Receiver<SourceMessageEvent>, ts: OffsetDateTime) {
        let ts_ms = (ts.unix_timestamp_nanos() / 1_000_000) as i64;
        while let Some(event) = rx.recv().await {
            let SourceMessageEvent::Data(batch) = event else {
                continue;
            };
            if batch.iter().any(|msg| {
                msg.is_cdc_heartbeat()
                    && matches!(&msg.meta, SourceMeta::DebeziumCdc(meta) if meta.source_ts_ms == ts_ms)
            }) {
                return;
            }
        }
        panic!("channel closed before the heartbeat at {ts}");
    }

    /// A merged child starts once, after every parent finished, from the start
    /// timestamp its parents reported.
    #[tokio::test(start_paused = true)]
    async fn test_run_reader_starts_merged_child_once_after_all_parents() {
        let t1 = T0 + Duration::from_secs(1);
        let t2 = T0 + Duration::from_secs(2);
        let querier = Arc::new(
            FakeQuerier::default()
                .script(
                    None,
                    vec![rows(vec![children_row(t1, &[("a", &[]), ("b", &[])])])],
                )
                .script(
                    Some("a"),
                    vec![rows(vec![children_row(t2, &[("c", &["a", "b"])])])],
                )
                // `b` fails once, so `c` must wait for its retry to finish.
                .script(
                    Some("b"),
                    vec![
                        FakeQuery::Fail(spanner_error(Code::Unavailable, "try again")),
                        rows(vec![children_row(t2, &[("c", &["a", "b"])])]),
                    ],
                )
                .script(
                    Some("c"),
                    vec![FakeQuery::Rows {
                        rows: vec![Ok(heartbeat_row(t2 + Duration::from_secs(1)))],
                        then_hang: true,
                    }],
                ),
        );
        let (tx, mut rx) = mpsc::channel(16);
        let reader = tokio::spawn(run_reader(test_reader_context(querier.clone(), 3), tx));

        recv_heartbeat_at(&mut rx, t2 + Duration::from_secs(1)).await;
        drop(rx);
        reader.await.unwrap().unwrap();

        let calls = querier.calls();
        let tokens: Vec<_> = calls.iter().map(|q| q.partition_token.as_deref()).collect();
        assert_eq!(tokens.iter().filter(|t| **t == Some("c")).count(), 1);
        assert_eq!(tokens.last(), Some(&Some("c")), "{tokens:?}");
        assert_eq!(calls.last().unwrap().start_timestamp, t2);
    }

    /// A query that fails after making progress resumes from the advanced offset.
    #[tokio::test(start_paused = true)]
    async fn test_run_reader_resumes_from_advanced_offset() {
        let t1 = T0 + Duration::from_secs(1);
        let t2 = T0 + Duration::from_secs(2);
        let querier = Arc::new(FakeQuerier::default().script(
            None,
            vec![
                FakeQuery::Rows {
                    rows: vec![
                        Ok(heartbeat_row(t1)),
                        Err(spanner_error(Code::Unavailable, "connection reset")),
                    ],
                    then_hang: false,
                },
                FakeQuery::Rows {
                    rows: vec![Ok(heartbeat_row(t2))],
                    then_hang: true,
                },
            ],
        ));
        let (tx, mut rx) = mpsc::channel(16);
        let reader = tokio::spawn(run_reader(test_reader_context(querier.clone(), 2), tx));

        recv_heartbeat_at(&mut rx, t2).await;
        drop(rx);
        reader.await.unwrap().unwrap();

        let starts: Vec<_> = querier.calls().iter().map(|q| q.start_timestamp).collect();
        assert_eq!(starts, vec![T0, t1]);
    }

    /// Saved partitions resume from their own offsets instead of the root query. A saved
    /// child whose saved parent is unfinished waits for it; a parent that is not saved
    /// counts as finished.
    #[tokio::test(start_paused = true)]
    async fn test_run_reader_resumes_saved_partitions() {
        let t2 = T0 + Duration::from_secs(2);
        let t3 = T0 + Duration::from_secs(3);
        let querier = Arc::new(
            FakeQuerier::default()
                .script(
                    Some("a"),
                    vec![rows(vec![children_row(t2, &[("c", &["a", "x"])])])],
                )
                .script(
                    Some("c"),
                    vec![FakeQuery::Rows {
                        rows: vec![Ok(heartbeat_row(t3))],
                        then_hang: true,
                    }],
                ),
        );
        let mut ctx = test_reader_context(querier.clone(), 1);
        ctx.saved_partitions = vec![
            PartitionProgress {
                token: Some("a".to_owned()),
                parents: vec![],
                offset: T0 + Duration::from_secs(1),
            },
            PartitionProgress {
                token: Some("c".to_owned()),
                parents: vec!["a".to_owned(), "x".to_owned()],
                offset: t2,
            },
        ]
        .into();
        let (tx, mut rx) = mpsc::channel(16);
        let reader = tokio::spawn(run_reader(ctx, tx));

        recv_heartbeat_at(&mut rx, t3).await;
        drop(rx);
        reader.await.unwrap().unwrap();

        let calls: Vec<_> = querier
            .calls()
            .into_iter()
            .map(|q| (q.partition_token, q.start_timestamp))
            .collect();
        assert_eq!(
            calls,
            vec![
                (Some("a".to_owned()), T0 + Duration::from_secs(1)),
                (Some("c".to_owned()), t2),
            ]
        );
    }

    /// Progress reports follow the batches they cover: a partition's reported offset
    /// never passes the last heartbeat already received from the channel.
    #[tokio::test(start_paused = true)]
    async fn test_run_reader_reports_progress_after_sent_batches() {
        let t1 = T0 + Duration::from_secs(1);
        let t2 = T0 + Duration::from_secs(2);
        let querier = Arc::new(FakeQuerier::default().script(
            None,
            vec![FakeQuery::Rows {
                rows: vec![Ok(heartbeat_row(t1)), Ok(heartbeat_row(t2))],
                then_hang: true,
            }],
        ));
        let (tx, mut rx) = mpsc::channel(16);
        let reader = tokio::spawn(run_reader(test_reader_context(querier, 1), tx));

        let mut received = T0;
        loop {
            match rx.recv().await.expect("reader stopped") {
                SourceMessageEvent::Data(batch) => {
                    for msg in batch {
                        if let SourceMeta::DebeziumCdc(meta) = &msg.meta {
                            let ts = OffsetDateTime::from_unix_timestamp_nanos(
                                meta.source_ts_ms as i128 * 1_000_000,
                            )
                            .unwrap();
                            received = received.max(ts);
                        }
                    }
                }
                SourceMessageEvent::SplitProgress(progress) => {
                    let mut split = SpannerCdcSplit::new_root("stream".to_owned(), 1, T0);
                    split.update_offset(progress["1"].clone()).unwrap();
                    let [root] = &*split.partitions else {
                        panic!("expected only the root, got {:?}", split.partitions);
                    };
                    assert_eq!(root.token, None);
                    assert!(root.offset <= received, "{} > {received}", root.offset);
                    if root.offset == t2 {
                        break;
                    }
                }
            }
        }
        drop(rx);
        reader.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_reader_fails_query_ending_without_child_partitions() {
        let querier =
            Arc::new(FakeQuerier::default().script(None, vec![rows(vec![heartbeat_row(T0)])]));
        let (tx, _rx) = mpsc::channel(16);
        let err = run_reader(test_reader_context(querier, 1), tx)
            .await
            .unwrap_err();
        assert!(
            err.to_report_string()
                .contains("ended without child partitions"),
            "{}",
            err.as_report()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_run_reader_does_not_retry_start_before_retention() {
        let querier = Arc::new(FakeQuerier::default().script(
            None,
            vec![FakeQuery::Fail(spanner_error(
                Code::OutOfRange,
                "Specified start_timestamp is too far in the past",
            ))],
        ));
        let (tx, _rx) = mpsc::channel(16);
        let err = run_reader(test_reader_context(querier.clone(), 5), tx)
            .await
            .unwrap_err();
        assert!(
            err.to_report_string().contains("retention period"),
            "{}",
            err.as_report()
        );
        assert_eq!(querier.calls().len(), 1);
    }

    /// Queries that never start, or go quiet, fail after the stall timeout and are retried.
    #[tokio::test(start_paused = true)]
    async fn test_run_reader_times_out_stuck_queries() {
        let stalled = || FakeQuery::Rows {
            rows: vec![],
            then_hang: true,
        };
        for (queries, expected) in [
            (
                vec![FakeQuery::Hang, FakeQuery::Hang],
                "query establishment timed out",
            ),
            (vec![stalled(), stalled()], "stream stalled"),
        ] {
            let querier = Arc::new(FakeQuerier::default().script(None, queries));
            let (tx, _rx) = mpsc::channel(16);
            let err = run_reader(test_reader_context(querier.clone(), 2), tx)
                .await
                .unwrap_err();
            assert!(
                err.to_report_string().contains(expected),
                "{}",
                err.as_report()
            );
            assert_eq!(querier.calls().len(), 2);
        }
    }

    /// One partition failing, or panicking, aborts its siblings, so their senders
    /// are dropped and the channel closes.
    #[tokio::test(start_paused = true)]
    async fn test_run_reader_aborts_siblings_when_a_partition_fails() {
        let t1 = T0 + Duration::from_secs(1);
        for (failure, expected) in [
            (
                FakeQuery::Fail(spanner_error(
                    Code::OutOfRange,
                    "Specified start_timestamp is too far in the past",
                )),
                "retention period",
            ),
            (FakeQuery::Panic, "partition task panicked"),
        ] {
            let querier = Arc::new(
                FakeQuerier::default()
                    .script(
                        None,
                        vec![rows(vec![children_row(t1, &[("a", &[]), ("b", &[])])])],
                    )
                    .script(Some("a"), vec![failure])
                    .script(
                        Some("b"),
                        vec![FakeQuery::Rows {
                            rows: vec![],
                            then_hang: true,
                        }],
                    ),
            );
            let (tx, mut rx) = mpsc::channel(16);
            let err = run_reader(test_reader_context(querier, 1), tx)
                .await
                .unwrap_err();
            assert!(
                err.to_report_string().contains(expected),
                "{}",
                err.as_report()
            );
            // `b` would stay open for the whole stall timeout if it were not aborted.
            tokio::time::timeout(Duration::from_secs(1), async {
                while rx.recv().await.is_some() {}
            })
            .await
            .expect("channel should close once the sibling task is aborted");
        }
    }

    // -----------------------------------------------------------------------
    // send_schema_change_if_evolved: the tracker evolves only once the schema
    // change is in the channel.
    // -----------------------------------------------------------------------

    fn test_data_change(
        column_names: &[&str],
        commit_ts: OffsetDateTime,
    ) -> crate::source::spanner_cdc::types::DataChangeRecord {
        use crate::source::spanner_cdc::types::{ColumnType, SpannerType, TypeCode};
        crate::source::spanner_cdc::types::DataChangeRecord {
            commit_timestamp: commit_ts,
            record_sequence: "00000000".to_owned(),
            server_transaction_id: "txn".to_owned(),
            is_last_record_in_transaction_in_partition: true,
            table_name: "t".to_owned(),
            value_capture_type: "OLD_AND_NEW_VALUES".to_owned(),
            column_types: column_names
                .iter()
                .map(|name| ColumnType {
                    name: (*name).to_owned(),
                    spanner_type: SpannerType::simple(TypeCode::String),
                    is_primary_key: false,
                    ordinal_position: 0,
                })
                .collect(),
            mods: vec![],
            mod_type: "INSERT".to_owned(),
            number_of_records_in_transaction: 1,
            number_of_partitions_in_transaction: 1,
            transaction_tag: String::new(),
            is_system_transaction: false,
        }
    }

    #[tokio::test]
    async fn test_schema_change_in_channel_before_tracker_evolves() {
        let tracker = Arc::new(std::sync::Mutex::new(SchemaTracker::new()));
        let split_id = SplitId::from("0");
        let old = test_data_change(&["id"], datetime!(2026-01-01 00:00:01 UTC));
        let new = test_data_change(&["id", "note"], datetime!(2026-01-01 00:00:02 UTC));
        tracker.lock().unwrap().check_and_evolve(
            &old.table_name,
            &old.column_types,
            old.commit_time(),
        );

        // Fill the channel so partition A has to wait for a slot.
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(SourceMessageEvent::Data(vec![])).await.unwrap();
        let partition_a = tokio::spawn({
            let (tracker, tx, split_id, new) =
                (tracker.clone(), tx.clone(), split_id.clone(), new.clone());
            async move {
                send_schema_change_if_evolved(&tracker, &tx, &split_id, &new, "", &mut vec![]).await
            }
        });
        tokio::task::yield_now().await;
        assert!(!partition_a.is_finished());

        // While A waits, partition B must not skip the schema change, or its rows with
        // the new column would enter the channel first.
        assert!(tracker.lock().unwrap().needs_emit(
            &new.table_name,
            &new.column_types,
            new.commit_time(),
        ));

        assert!(matches!(rx.recv().await, Some(SourceMessageEvent::Data(msgs)) if msgs.is_empty()));
        assert!(partition_a.await.unwrap());
        let Some(SourceMessageEvent::Data(msgs)) = rx.recv().await else {
            panic!("expected the schema change batch");
        };
        assert!(matches!(
            &msgs[0].meta,
            SourceMeta::DebeziumCdc(meta) if matches!(meta.msg_type, crate::source::cdc::CdcMessageType::SchemaChange)
        ));
        assert!(!tracker.lock().unwrap().needs_emit(
            &new.table_name,
            &new.column_types,
            new.commit_time(),
        ));
    }
}
