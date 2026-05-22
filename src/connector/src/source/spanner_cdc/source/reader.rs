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
//! 2. The background task sends data/progress events through an `mpsc` channel
//! 3. `into_data_event_stream()` calls `rx.recv()` and yields events
//! 4. `into_event_stream()` wraps with `into_chunk_event_stream` (parser)
//!
//! ## Spanner Change Stream Query Model
//!
//! The background task handles Spanner-specific concerns:
//! - **Partition management**: Tracks partition tokens and handles splits/merges
//! - **Parent-child coordination**: Child partitions wait until ALL parents finish
//! - **Heartbeat handling**: Updates offset based on heartbeat records
//! - **Schema evolution**: Detects schema changes and emits them as separate messages
//!   (mimicking Debezium's Relation messages that precede DML events)

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
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
use crate::source::spanner_cdc::types::ChangeStreamRecord;
use crate::source::spanner_cdc::split::SpannerPartitionState;
use crate::source::spanner_cdc::{SpannerCdcProperties, SpannerCdcSplit};
use crate::source::{
    BoxSourceChunkStream, BoxSourceReaderEventStream, Column, SourceContextRef, SourceMessage,
    SourceMessageEvent, SourceReaderEvent, SplitId, SplitReader, SourceMeta,
    into_chunk_event_stream,
};

const DEFAULT_CHANNEL_SIZE: usize = 16;

/// Spanner CDC split reader — same pattern as Debezium's `CdcSplitReader`.
///
/// The background task reads from the Spanner change stream and sends messages
/// through an `mpsc` channel. This reader just receives and yields them.
pub struct SpannerCdcSplitReader {
    /// Receives data/progress events from the background reader task.
    rx: mpsc::Receiver<SourceMessageEvent>,
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

        let initial_split = splits
            .iter()
            .find(|s| s.index == source_id)
            .cloned()
            .unwrap_or_else(|| splits[0].clone());

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
            schema_tracker: Arc::new(SchemaTracker::new()),
            source_id,
            initial_split,
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
        into_chunk_event_stream(self.into_data_event_stream(), parser_config, source_context)
    }
}

impl SpannerCdcSplitReader {
    /// Receives source data/progress events from the background reader task.
    #[try_stream(ok = SourceMessageEvent, error = ConnectorError)]
    async fn into_data_event_stream(mut self) {
        let source_id = self.source_ctx.source_id.to_string();

        while let Some(event) = self.rx.recv().await {
            yield event;
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
    database_name: String,
    change_stream_name: String,
    max_concurrent_partitions: usize,
    heartbeat_interval_ms: i64,
    retry_attempts: u32,
    retry_backoff: std::time::Duration,
    retry_backoff_max_delay_ms: u64,
    retry_backoff_factor: u64,
    schema_tracker: Arc<SchemaTracker>,
    source_id: u32,
    initial_split: SpannerCdcSplit,
}

struct PartitionTaskOutput {
    split: SpannerCdcSplit,
    child_partitions: Vec<DiscoveredChildPartition>,
}

struct DiscoveredChildPartition {
    token: String,
    parent_tokens: Vec<String>,
    start_timestamp: OffsetDateTime,
}

/// Main reader loop — reads from Spanner change stream and sends to `tx`.
///
/// Manages partition lifecycle (root → children → grandchildren…) per the
/// Spanner change stream query model. This is the Spanner equivalent of
/// Debezium's JNI thread that reads from the WAL.
async fn run_reader(
    ctx: ReaderContext,
    tx: mpsc::Sender<SourceMessageEvent>,
) -> Result<()> {
    let mut partition_streams: FuturesUnordered<tokio::task::JoinHandle<Result<PartitionTaskOutput>>> =
        FuturesUnordered::new();

    let split_id = SplitId::from(ctx.source_id.to_string());
    let mut source_split = ctx.initial_split.clone();
    source_split.change_stream_name = ctx.change_stream_name.clone();
    source_split.index = ctx.source_id;
    source_split.prepare_for_runtime();

    tracing::info!(
        starting_offset = ?source_split.global_watermark,
        partition_count = source_split.partition_states.len(),
        "starting Spanner CDC reader with partition state"
    );

    emit_split_progress(&tx, &split_id, &source_split).await?;
    spawn_ready_partitions(&ctx, &split_id, &tx, &mut source_split, &mut partition_streams).await?;

    // Main event loop
    loop {
        // If the mpsc receiver was dropped (source executor stopped), we stop too.
        if tx.is_closed() {
            tracing::info!("reader channel closed, stopping");
            break;
        }

        if partition_streams.is_empty() {
            spawn_ready_partitions(
                &ctx,
                &split_id,
                &tx,
                &mut source_split,
                &mut partition_streams,
            )
            .await?;

            if partition_streams.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }
        }

        tokio::select! {
            result = partition_streams.next() => {
                match result {
                    Some(Ok(Ok(output))) => {
                        let key = SpannerPartitionState::key_for_token(output.split.partition_token.as_deref());
                        if let Some(progress) = output.split.offset {
                            source_split.mark_partition_finished(&key, progress);
                        }
                        source_split.messages_processed = source_split
                            .messages_processed
                            .saturating_add(output.split.messages_processed);
                        for child in output.child_partitions {
                            source_split.discover_partition(
                                child.token,
                                child.parent_tokens,
                                child.start_timestamp,
                            );
                        }
                        // After child discovery, so just-finished partitions aren't
                        // evicted before their children register them as parents.
                        source_split.prune_finished_partitions();
                        emit_split_progress(&tx, &split_id, &source_split).await?;
                        spawn_ready_partitions(
                            &ctx,
                            &split_id,
                            &tx,
                            &mut source_split,
                            &mut partition_streams,
                        ).await?;
                    }
                    Some(Ok(Err(e))) => return Err(e),
                    Some(Err(e)) => {
                        return Err(ConnectorError::from(anyhow::anyhow!(
                            "partition task panicked: {}", e
                        )));
                    }
                    None => {
                        spawn_ready_partitions(
                            &ctx,
                            &split_id,
                            &tx,
                            &mut source_split,
                            &mut partition_streams,
                        ).await?;
                        if !partition_streams.is_empty() {
                            continue;
                        }
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Partition task management
// ---------------------------------------------------------------------------

fn spawn_partition_task(
    ctx: &ReaderContext,
    split: SpannerCdcSplit,
    split_id: &SplitId,
    tx: &mpsc::Sender<SourceMessageEvent>,
    partition_streams: &mut FuturesUnordered<tokio::task::JoinHandle<Result<PartitionTaskOutput>>>,
) {
    let client = ctx.client.clone();
    let database_name = ctx.database_name.clone();
    let change_stream_name = ctx.change_stream_name.clone();
    let heartbeat_interval_ms = ctx.heartbeat_interval_ms;
    let retry_attempts = ctx.retry_attempts;
    let retry_backoff = ctx.retry_backoff;
    let retry_backoff_max_delay_ms = ctx.retry_backoff_max_delay_ms;
    let retry_backoff_factor = ctx.retry_backoff_factor;
    let schema_tracker = ctx.schema_tracker.clone();
    let tx = tx.clone();
    let split_id = split_id.clone();

    partition_streams.push(tokio::spawn(async move {
        read_partition(
            client, database_name, split, change_stream_name, heartbeat_interval_ms,
            split_id, retry_attempts, retry_backoff,
            retry_backoff_max_delay_ms, retry_backoff_factor,
            schema_tracker, tx,
        ).await
    }));
}

async fn spawn_ready_partitions(
    ctx: &ReaderContext,
    split_id: &SplitId,
    tx: &mpsc::Sender<SourceMessageEvent>,
    source_split: &mut SpannerCdcSplit,
    partition_streams: &mut FuturesUnordered<tokio::task::JoinHandle<Result<PartitionTaskOutput>>>,
) -> Result<()> {
    while source_split.running_partition_count() < ctx.max_concurrent_partitions {
        let Some(partition) = source_split.next_runnable_partition() else {
            break;
        };
        let key = partition.key();
        source_split.mark_partition_running(&key);
        emit_split_progress(tx, split_id, source_split).await?;

        let start_offset = partition.progress_or_start();
        let split = match partition.token {
            Some(token) => SpannerCdcSplit::new_child(
                token,
                partition.parent_tokens,
                start_offset,
                ctx.change_stream_name.clone(),
                ctx.source_id,
            ),
            None => SpannerCdcSplit::new_root(
                ctx.change_stream_name.clone(),
                ctx.source_id,
                start_offset,
            ),
        };
        spawn_partition_task(ctx, split, split_id, tx, partition_streams);
    }
    Ok(())
}

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
    schema_tracker: Arc<SchemaTracker>,
    tx: mpsc::Sender<SourceMessageEvent>,
) -> Result<PartitionTaskOutput> {
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
            return Ok(PartitionTaskOutput {
                split,
                child_partitions: vec![],
            });
        }

        match execute_query(
            &client, &database_name, &stmt, &mut split, &split_id, &schema_tracker,
            &tx, &change_stream_name,
        ).await {
            Ok(child_partitions) => {
                return Ok(PartitionTaskOutput {
                    split,
                    child_partitions,
                });
            }
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

// ---------------------------------------------------------------------------
// Change stream query execution
// ---------------------------------------------------------------------------

async fn execute_query(
    client: &Client,
    database_name: &str,
    stmt: &Statement,
    split: &mut SpannerCdcSplit,
    split_id: &SplitId,
    schema_tracker: &Arc<SchemaTracker>,
    tx: &mpsc::Sender<SourceMessageEvent>,
    change_stream_name: &str,
) -> Result<Vec<DiscoveredChildPartition>> {
    let mut txn = client
        .single()
        .await
        .map_err(|e| anyhow::anyhow!("failed to create transaction: {}", e))?;
    let mut result_set = txn
        .query(stmt.clone())
        .await
        .map_err(|e| anyhow::anyhow!("failed to execute query: {}", e))?;
    let mut child_partitions = Vec::new();

    while let Some(row) = result_set
        .next()
        .await
        .map_err(|e| anyhow::anyhow!("failed to get next row: {}", e))?
    {
        if tx.is_closed() {
            return Ok(child_partitions);
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
                split.messages_processed += 1;

                // Schema evolution: emit schema change as a SEPARATE message
                // before the data records, mimicking Debezium's Relation messages
                // that naturally precede DML events in Postgres WAL.
                match schema_tracker.check_and_evolve(data_change).await {
                    Ok(Some(json)) => {
                        // Flush accumulated messages first to preserve ordering.
                        if !messages.is_empty() {
                            if tx.send(SourceMessageEvent::Data(std::mem::take(&mut messages))).await.is_err() {
                                return Ok(child_partitions);
                            }
                        }
                        // Send schema change alone so the parser can process it
                        // before the data records that follow.
                        let schema_msg = make_schema_change_msg(
                            split,
                            split_id,
                            json,
                            data_change,
                            database_name,
                        );
                        if tx.send(SourceMessageEvent::Data(vec![schema_msg])).await.is_err() {
                            return Ok(child_partitions);
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
                        database_name: database_name.to_owned(),
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
                if tx.send(SourceMessageEvent::Data(messages)).await.is_err() {
                    return Ok(child_partitions);
                }
            }
            if split.offset.is_some() && !split.is_aggregate_state() {
                emit_partition_progress(tx, split_id, split).await?;
            }

            // Child partition discovery
            for cpr in &record.child_partitions_record {
                let start_time = cpr.start_time();
                for cp in &cpr.child_partitions {
                    tracing::debug!(
                        token = %cp.token,
                        parents = ?cp.parent_partition_tokens,
                        change_stream = change_stream_name,
                        "discovered child partition"
                    );
                    child_partitions.push(DiscoveredChildPartition {
                        token: cp.token.clone(),
                        parent_tokens: cp.parent_partition_tokens.clone(),
                        start_timestamp: start_time,
                    });
                }
            }
        }
    }

    tracing::info!(
        %split_id,
        final_offset = ?split.offset,
        "change stream result set exhausted"
    );
    Ok(child_partitions)
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

async fn emit_partition_progress(
    tx: &mpsc::Sender<SourceMessageEvent>,
    split_id: &SplitId,
    split: &SpannerCdcSplit,
) -> Result<()> {
    let mut progress = HashMap::new();
    progress.insert(split_id.clone(), make_offset_string(split));
    if tx.send(SourceMessageEvent::SplitProgress(progress)).await.is_err() {
        return Err(ConnectorError::from(anyhow::anyhow!(
            "Spanner CDC reader channel closed"
        )));
    }
    Ok(())
}

async fn emit_split_progress(
    tx: &mpsc::Sender<SourceMessageEvent>,
    split_id: &SplitId,
    split: &SpannerCdcSplit,
) -> Result<()> {
    let mut progress = HashMap::new();
    progress.insert(split_id.clone(), split.encode_progress_offset());
    if tx.send(SourceMessageEvent::SplitProgress(progress)).await.is_err() {
        return Err(ConnectorError::from(anyhow::anyhow!(
            "Spanner CDC reader channel closed"
        )));
    }
    Ok(())
}

fn make_schema_change_msg(
    split: &SpannerCdcSplit,
    split_id: &SplitId,
    payload: Vec<u8>,
    data_change: &crate::source::spanner_cdc::types::DataChangeRecord,
    database_name: &str,
) -> SourceMessage {
    SourceMessage {
        key: None,
        payload: Some(payload.into()),
        offset: make_offset_string(split),
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
