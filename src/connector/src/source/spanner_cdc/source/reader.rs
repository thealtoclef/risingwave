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
//! ## Spanner Change Stream Query Model
//!
//! The background task handles Spanner-specific concerns:
//! - **Partition management**: Tracks partition tokens and handles splits/merges
//! - **Parent-child coordination**: Child partitions wait until ALL parents finish
//! - **Heartbeat handling**: Updates offset based on heartbeat records
//! - **Schema evolution**: Detects schema changes and emits them as separate messages
//!   (mimicking Debezium's Relation messages that precede DML events)
//!
//! ## Per-partition progress
//!
//! Child partitions are tracked in a local `HashMap` keyed by partition token
//! for dedup and parent coordination. Control messages (registration + finished)
//! go through the mpsc data channel so the executor's `SpannerCdcSplit` stays
//! in sync via `update_in_place`.
//!
//! On restart the reader always starts a fresh root from the lagging-edge
//! offset. Old partition tokens may have expired, so per-partition restart
//! is not safe.

use std::collections::HashMap;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use futures_async_stream::try_stream;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::statement::Statement;
use risingwave_common::ensure;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

use risingwave_pb::connector_service::{SourceType, cdc_message};

use super::TaggedChangeRecord;
use crate::error::{ConnectorError, ConnectorResult as Result};
use crate::parser::ParserConfig;
use crate::source::cdc::DebeziumCdcMeta;
use crate::source::spanner_cdc::schema_track::SchemaTracker;
use crate::source::spanner_cdc::split::{PartitionState, PerPartitionProgress};
use crate::source::spanner_cdc::types::ChangeStreamRecord;
use crate::source::spanner_cdc::{SpannerCdcProperties, SpannerCdcSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId,
    SplitReader, SourceMeta, into_chunk_stream,
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
            change_stream_name: properties.change_stream_name.clone(),
            max_concurrent_partitions: properties.get_change_stream_max_concurrent_partitions(),
            heartbeat_interval_ms,
            retry_attempts: properties.get_retry_attempts(),
            retry_backoff: properties.get_retry_backoff(),
            retry_backoff_max_delay_ms: properties.get_retry_backoff_max_delay_ms(),
            retry_backoff_factor: properties.get_retry_backoff_factor(),
            schema_tracker: std::sync::Arc::new(SchemaTracker::new()),
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
        risingwave_common::metrics::GLOBAL_ERROR_METRICS.user_source_error.report([
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
    change_stream_name: String,
    max_concurrent_partitions: usize,
    heartbeat_interval_ms: i64,
    retry_attempts: u32,
    retry_backoff: std::time::Duration,
    retry_backoff_max_delay_ms: u64,
    retry_backoff_factor: u64,
    schema_tracker: std::sync::Arc<SchemaTracker>,
    source_id: u32,
    checkpointed_offset: Option<OffsetDateTime>,
}

/// Result from each partition task.
struct PartitionResult {
    partition_token: Option<String>,
    final_offset: Option<OffsetDateTime>,
}

/// Main reader loop — reads from Spanner change stream and sends to `tx`.
///
/// Manages partition lifecycle (root → children → grandchildren…) per the
/// Spanner change stream query model. This is the Spanner equivalent of
/// Debezium's JNI thread that reads from the WAL.
async fn run_reader(
    ctx: ReaderContext,
    tx: mpsc::Sender<Vec<SourceMessage>>,
) -> Result<()> {
    let mut partition_streams: FuturesUnordered<
        tokio::task::JoinHandle<Result<PartitionResult>>,
    > = FuturesUnordered::new();

    let max_concurrent = ctx.max_concurrent_partitions;
    let mut active_count: usize = 0;

    // Local HashMap for dedup and parent coordination. Not used for recovery.
    let mut partition_progress: HashMap<String, PerPartitionProgress> = HashMap::new();

    let mut pending_children: std::collections::VecDeque<SpannerCdcSplit> =
        std::collections::VecDeque::new();
    let (child_discovery_tx, mut child_discovery_rx) =
        tokio::sync::mpsc::unbounded_channel::<SpannerCdcSplit>();

    let split_id = SplitId::from(ctx.source_id.to_string());
    let root_offset = ctx.checkpointed_offset.unwrap_or_else(OffsetDateTime::now_utc);

    tracing::info!(
        starting_offset = ?root_offset,
        "starting Spanner CDC reader with root partition"
    );

    let root_split = SpannerCdcSplit::new_root(
        ctx.change_stream_name.clone(),
        ctx.source_id,
        root_offset,
    );
    active_count += 1;
    spawn_partition_task(
        &ctx, root_split, &split_id, &tx,
        &mut partition_streams, child_discovery_tx.clone(),
    );

    // Main event loop
    loop {
        // If the mpsc receiver was dropped (source executor stopped), we stop too.
        if tx.is_closed() {
            tracing::info!("reader channel closed, stopping");
            break;
        }

        tokio::select! {
            result = partition_streams.next() => {
                match result {
                    Some(Ok(Ok(partition_result))) => {
                        if let Some(ref token) = partition_result.partition_token {
                            if let Some(entry) = partition_progress.get_mut(token) {
                                entry.state = PartitionState::Finished;
                                if let Some(off) = partition_result.final_offset {
                                    entry.offset = Some(off);
                                }
                            }
                            let msg = make_partition_control_message(
                                &split_id,
                                token,
                                &partition_progress.get(token)
                                    .map(|e| e.parent_tokens.clone())
                                    .unwrap_or_default(),
                                partition_result.final_offset
                                    .map(|o| (o.unix_timestamp_nanos() / 1000) as i64)
                                    .unwrap_or(0),
                                true,
                            );
                            if tx.send(vec![msg]).await.is_err() {
                                break;
                            }
                        } else {
                            let msg = make_root_finished_message(
                                &split_id,
                                partition_result.final_offset
                                    .map(|o| (o.unix_timestamp_nanos() / 1000) as i64)
                                    .unwrap_or(0),
                            );
                            if tx.send(vec![msg]).await.is_err() {
                                break;
                            }
                        }
                    }
                    Some(Ok(Err(e))) => return Err(e),
                    Some(Err(e)) => {
                        return Err(ConnectorError::from(anyhow::anyhow!(
                            "partition task panicked: {}", e
                        )));
                    }
                    None => {
                        // Try pending children before declaring done
                        let mut started = false;
                        while active_count < max_concurrent {
                            if let Some(child) = pending_children.pop_front() {
                                if parents_all_finished(&child.parent_partition_tokens, &partition_progress) {
                                    if let Some(ref token) = child.partition_token {
                                        set_running(&mut partition_progress, token);
                                    }
                                    active_count += 1;
                                    spawn_partition_task(
                                        &ctx, child, &split_id, &tx,
                                        &mut partition_streams, child_discovery_tx.clone(),
                                    );
                                    started = true;
                                } else {
                                    pending_children.push_front(child);
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                        if started { continue; }
                        // All partition tasks completed and no pending children can
                        // start. This indicates the Spanner change stream has terminated
                        // (e.g., stream deleted or retention expired).
                        tracing::warn!(
                            %split_id,
                            pending = pending_children.len(),
                            "all Spanner change stream partitions finished"
                        );
                        break;
                    }
                }

                active_count = active_count.saturating_sub(1);

                // Start pending children
                while active_count < max_concurrent {
                    if let Some(child) = pending_children.pop_front() {
                        if parents_all_finished(&child.parent_partition_tokens, &partition_progress) {
                            if let Some(ref token) = child.partition_token {
                                set_running(&mut partition_progress, token);
                            }
                            active_count += 1;
                            spawn_partition_task(
                                &ctx, child, &split_id, &tx,
                                &mut partition_streams, child_discovery_tx.clone(),
                            );
                        } else {
                            pending_children.push_front(child);
                            break;
                        }
                    } else {
                        break;
                    }
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

                    let reg_msg = make_partition_control_message(
                        &split_id,
                        token,
                        &parents,
                        (start_offset.unix_timestamp_nanos() / 1000) as i64,
                        false,
                    );
                    if tx.send(vec![reg_msg]).await.is_err() {
                        break;
                    }
                }

                tracing::debug!(
                    token = ?child.partition_token,
                    parents = ?child.parent_partition_tokens,
                    "discovered child partition"
                );

                if parents_all_finished(&child.parent_partition_tokens, &partition_progress)
                    && active_count < max_concurrent
                {
                    if let Some(ref token) = child.partition_token {
                        set_running(&mut partition_progress, token);
                    }
                    active_count += 1;
                    spawn_partition_task(
                        &ctx, child, &split_id, &tx,
                        &mut partition_streams, child_discovery_tx.clone(),
                    );
                } else {
                    pending_children.push_back(child);
                }
            }
        }
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

fn set_running(
    partition_progress: &mut HashMap<String, PerPartitionProgress>,
    token: &str,
) {
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
    tx: &mpsc::Sender<Vec<SourceMessage>>,
    partition_streams: &mut FuturesUnordered<tokio::task::JoinHandle<Result<PartitionResult>>>,
    child_discovery_tx: tokio::sync::mpsc::UnboundedSender<SpannerCdcSplit>,
) {
    let client = ctx.client.clone();
    let change_stream_name = ctx.change_stream_name.clone();
    let heartbeat_interval_ms = ctx.heartbeat_interval_ms;
    let retry_attempts = ctx.retry_attempts;
    let retry_backoff = ctx.retry_backoff;
    let retry_backoff_max_delay_ms = ctx.retry_backoff_max_delay_ms;
    let retry_backoff_factor = ctx.retry_backoff_factor;
    let schema_tracker = ctx.schema_tracker.clone();
    let tx = tx.clone();
    let split_id = split_id.clone();
    let partition_token = split.partition_token.clone();

    partition_streams.push(tokio::spawn(async move {
        let result = read_partition(
            client, split, change_stream_name, heartbeat_interval_ms,
            split_id, retry_attempts, retry_backoff,
            retry_backoff_max_delay_ms, retry_backoff_factor,
            schema_tracker, tx, child_discovery_tx,
        ).await;
        match result {
            Ok(final_offset) => Ok(PartitionResult {
                partition_token: partition_token,
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
    mut split: SpannerCdcSplit,
    change_stream_name: String,
    heartbeat_interval_ms: i64,
    split_id: SplitId,
    retry_attempts: u32,
    retry_backoff: std::time::Duration,
    retry_backoff_max_delay_ms: u64,
    retry_backoff_factor: u64,
    schema_tracker: std::sync::Arc<SchemaTracker>,
    tx: mpsc::Sender<Vec<SourceMessage>>,
    child_discovery_tx: tokio::sync::mpsc::UnboundedSender<SpannerCdcSplit>,
) -> Result<OffsetDateTime> {
    let start_ts = split.offset.ok_or_else(|| {
        ConnectorError::from(anyhow::anyhow!(
            "offset is None for split_id={}, change_stream={}",
            split_id, change_stream_name
        ))
    })?;

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
        %split_id, %start_ts,
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
        if tx.is_closed() {
            return Ok(split.offset.unwrap_or(start_ts));
        }

        match execute_query(
            &client, &stmt, &mut split, &split_id, &schema_tracker,
            &tx, &child_discovery_tx, &change_stream_name,
        ).await {
            Ok(()) => return Ok(split.offset.unwrap_or(start_ts)),
            Err(e) => {
                tracing::warn!(
                    %split_id, attempt = attempt + 1,
                    max_attempts = retry_attempts + 1,
                    ?delay, error = %e,
                    "query failed, retrying"
                );
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
    stmt: &Statement,
    split: &mut SpannerCdcSplit,
    split_id: &SplitId,
    schema_tracker: &std::sync::Arc<SchemaTracker>,
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
                    split_id = %split_id,
                    table_name = %data_change.table_name,
                    commit_time = ?data_change.commit_time(),
                    mod_count = data_change.mods.len(),
                    "received data change from Spanner change stream"
                );
                split.advance_offset(data_change.commit_time());

                // Schema evolution: emit schema change as a SEPARATE message
                // before the data records, mimicking Debezium's Relation messages
                // that naturally precede DML events in Postgres WAL.
                match schema_tracker.check_and_evolve(data_change).await {
                    Ok(Some(json)) => {
                        // Flush accumulated messages first to preserve ordering.
                        if !messages.is_empty() {
                            if tx.send(std::mem::take(&mut messages)).await.is_err() {
                                return Ok(());
                            }
                        }
                        // Send schema change alone so the parser can process it
                        // before the data records that follow.
                        let schema_msg = make_schema_change_msg(split, split_id, json, data_change);
                        if tx.send(vec![schema_msg]).await.is_err() {
                            return Ok(());
                        }
                        // Fall through: data records accumulate in `messages`
                        // and are sent in the next batch.
                    }
                    Ok(None) => {}
                    Err(e) => return Err(e),
                }

                // Data messages for all modifications
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

            // Heartbeats
            for heartbeat in &record.heartbeat_record {
                tracing::debug!(
                    split_id = %split_id,
                    heartbeat_time = ?heartbeat.heartbeat_time(),
                    "received heartbeat from change stream"
                );
                split.advance_offset(heartbeat.heartbeat_time());

                let heartbeat_msg = SourceMessage {
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
                messages.push(heartbeat_msg);
            }

            // Send messages to the mpsc channel
            if !messages.is_empty() {
                tracing::debug!(
                    split_id = %split_id,
                    message_count = messages.len(),
                    "sending CDC messages"
                );
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

    tracing::info!(
        %split_id,
        final_offset = ?split.offset,
        "change stream result set exhausted"
    );
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
    serde_json::to_string(&cdc_offset)
        .unwrap_or_else(|_| split.offset_as_micros().to_string())
}

/// Control message for child partition lifecycle (registration / finished).
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
    let mut spanner_offset =
        crate::source::cdc::external::spanner::SpannerOffset::with_partition(
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

fn make_root_finished_message(
    split_id: &SplitId,
    final_offset_micros: i64,
) -> SourceMessage {
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
        meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta::new(
            data_change.table_name.clone(),
            (data_change.commit_time().unix_timestamp_nanos() / 1_000_000) as i64,
            cdc_message::CdcMessageType::SchemaChange,
            SourceType::Unspecified,
        )),
    }
}
