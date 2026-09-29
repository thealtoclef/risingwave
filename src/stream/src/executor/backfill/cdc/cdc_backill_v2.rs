// Copyright 2025 RisingWave Labs
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

use std::cmp::Ordering;
use std::collections::BTreeMap;

use either::Either;
use futures::stream;
use futures::stream::select_with_strategy;
use itertools::Itertools;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::catalog::{CdcKeyComparison, ColumnDesc, Field};
use risingwave_common::row::RowDeserializer;
use risingwave_common::types::{DatumRef, JsonbRef, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::parser::DebeziumParser;
use risingwave_connector::source::cdc::CdcScanOptions;
use risingwave_connector::source::cdc::external::{
    CdcOffset, ExternalCdcTableType, ExternalTableConfig, ExternalTableReaderImpl,
};
use risingwave_connector::source::{CdcTableSnapshotSplit, CdcTableSnapshotSplitRaw};
use risingwave_pb::common::ThrottleType;
use rw_futures_util::pausable;
use thiserror_ext::AsReport;
use tracing::Instrument;

use crate::executor::backfill::cdc::cdc_backfill::{
    build_debezium_parser, build_reader_and_poll_upstream,
    get_cdc_json_parse_handling_from_properties, parse_debezium_chunk,
};
use crate::executor::backfill::cdc::state_v2::ParallelizedCdcBackfillState;
use crate::executor::backfill::cdc::upstream_table::external::ExternalStorageTable;
use crate::executor::backfill::cdc::upstream_table::snapshot::{
    SplitSnapshotReadArgs, UpstreamTableRead, UpstreamTableReader,
};
use crate::executor::backfill::utils::{
    cmp_pk_unsigned_aware, get_cdc_chunk_last_offset, mapping_chunk, mapping_message,
};
use crate::executor::prelude::*;
use crate::executor::source::get_infinite_backoff_strategy;
use crate::task::cdc_progress::CdcProgressReporter;
pub struct ParallelizedCdcBackfillExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// The external table to be backfilled
    external_table: ExternalStorageTable,

    /// Upstream changelog stream which may contain metadata columns, e.g. `_rw_offset`
    upstream: Executor,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    /// The schema of output chunk, including additional columns if any
    output_columns: Vec<ColumnDesc>,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,

    options: CdcScanOptions,

    state_table: StateTable<S>,

    properties: BTreeMap<String, String>,

    progress: Option<CdcProgressReporter>,
}

impl<S: StateStore> ParallelizedCdcBackfillExecutor<S> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        external_table: ExternalStorageTable,
        upstream: Executor,
        output_indices: Vec<usize>,
        output_columns: Vec<ColumnDesc>,
        _metrics: Arc<StreamingMetrics>,
        state_table: StateTable<S>,
        rate_limit_rps: Option<u32>,
        options: CdcScanOptions,
        properties: BTreeMap<String, String>,
        progress: Option<CdcProgressReporter>,
    ) -> Self {
        Self {
            actor_ctx,
            external_table,
            upstream,
            output_indices,
            output_columns,
            rate_limit_rps,
            options,
            state_table,
            properties,
            progress,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        assert!(!self.options.disable_backfill);
        // The indices to primary key columns
        let pk_indices = self.external_table.pk_indices().to_vec();
        let table_id = self.external_table.table_id();
        let upstream_table_name = self.external_table.qualified_table_name();
        let schema_table_name = self.external_table.schema_table_name().clone();
        let external_database_name = self.external_table.database_name().to_owned();
        let additional_columns = self
            .output_columns
            .iter()
            .filter(|col| col.additional_column.column_type.is_some())
            .cloned()
            .collect_vec();
        assert!(
            (self.options.backfill_split_pk_column_index as usize) < pk_indices.len(),
            "split pk column index {} out of bound",
            self.options.backfill_split_pk_column_index
        );
        let snapshot_split_column_index =
            pk_indices[self.options.backfill_split_pk_column_index as usize];
        let cdc_table_snapshot_split_column =
            vec![self.external_table.schema().fields[snapshot_split_column_index].clone()];
        // A MySQL `BIGINT UNSIGNED` split key is stored as `i64`, but split bounds follow the
        // upstream unsigned order, so it must be compared as `u64`. Graphs created before PK
        // comparison metadata existed resolve it from the table reader instead.
        let mut split_key_needs_unsigned_i64_compare = self
            .external_table
            .pk_comparisons()
            .and_then(|comparisons| {
                comparisons.get(self.options.backfill_split_pk_column_index as usize)
            })
            .map(|comparison| *comparison == CdcKeyComparison::UnsignedInt64);

        let mut upstream = self.upstream.execute();
        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;

        // If user sets debezium.time.precision.mode to "connect", it means the user can guarantee
        // that the upstream data precision is MilliSecond. In this case, we don't use GuessNumberUnit
        // mode to guess precision, but use Milli mode directly, which can handle extreme timestamps.
        let (timestamp_handling, timestamptz_handling, time_handling, bigint_unsigned_handling) =
            get_cdc_json_parse_handling_from_properties(&self.properties);
        // Only postgres-cdc connector may trigger TOAST.
        let handle_toast_columns: bool =
            self.external_table.table_type() == &ExternalCdcTableType::Postgres;
        // Upstream chunks are parsed here rather than by `transform_upstream`, so that rows outside
        // this actor's splits skip the parse. Make sure to map a chunk only after parsing it.
        let parser = build_debezium_parser(
            &self.output_columns,
            timestamp_handling,
            timestamptz_handling,
            time_handling,
            bigint_unsigned_handling,
            handle_toast_columns,
        )
        .await?;
        let split_key_column =
            &self.output_columns[self.output_indices[snapshot_split_column_index]];
        let mut upstream_parser = UpstreamChunkParser {
            parser,
            split_key_name: split_key_column.name.clone(),
            split_key_type: split_key_column.data_type.clone(),
        };
        let mut next_reset_barrier = Some(first_barrier);
        let mut is_reset = false;
        let mut state_impl = ParallelizedCdcBackfillState::new(self.state_table);
        // The buffered chunks have already been mapped.
        let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];

        // Need reset on CDC table snapshot splits reschedule.
        'with_cdc_table_snapshot_splits: loop {
            assert!(upstream_chunk_buffer.is_empty());
            let reset_barrier = next_reset_barrier.take().unwrap();
            let all_snapshot_splits = match reset_barrier.mutation.as_deref() {
                Some(Mutation::Add(add)) => &add.actor_cdc_table_snapshot_splits.splits,

                Some(Mutation::Update(update)) => &update.actor_cdc_table_snapshot_splits.splits,
                _ => {
                    return Err(anyhow::anyhow!("ParallelizedCdcBackfillExecutor expects either Mutation::Add or Mutation::Update to initialize CDC table snapshot splits.").into());
                }
            };
            let mut actor_snapshot_splits = vec![];
            let mut generation = None;
            // TODO(zw): optimization: remove consumed splits to reduce barrier size for downstream.
            if let Some((splits, snapshot_generation)) = all_snapshot_splits.get(&self.actor_ctx.id)
            {
                actor_snapshot_splits = splits
                    .iter()
                    .map(|s: &CdcTableSnapshotSplitRaw| {
                        let de = RowDeserializer::new(
                            cdc_table_snapshot_split_column
                                .iter()
                                .map(Field::data_type)
                                .collect_vec(),
                        );
                        let left_bound_inclusive =
                            de.deserialize(s.left_bound_inclusive.as_ref()).unwrap();
                        let right_bound_exclusive =
                            de.deserialize(s.right_bound_exclusive.as_ref()).unwrap();
                        CdcTableSnapshotSplit {
                            split_id: s.split_id,
                            left_bound_inclusive,
                            right_bound_exclusive,
                        }
                    })
                    .collect();
                generation = Some(*snapshot_generation);
            }
            tracing::debug!(?actor_snapshot_splits, ?generation, "actor splits");
            assert_consecutive_splits(&actor_snapshot_splits, split_key_needs_unsigned_i64_compare);

            let mut is_snapshot_paused = reset_barrier.is_pause_on_startup();
            let barrier_epoch = reset_barrier.epoch;
            yield Message::Barrier(reset_barrier);
            if !is_reset {
                state_impl.init_epoch(barrier_epoch).await?;
                is_reset = true;
                tracing::info!(%table_id, "Initialize executor.");
            } else {
                tracing::info!(%table_id, "Reset executor.");
            }

            let mut current_actor_bounds = None;
            let mut actor_cdc_offset_high: Option<CdcOffset> = None;
            let mut actor_cdc_offset_low: Option<CdcOffset> = None;
            // Find next split that need backfill.
            let mut next_split_idx = actor_snapshot_splits.len();
            for (idx, split) in actor_snapshot_splits.iter().enumerate() {
                let state = state_impl.restore_state(split.split_id).await?;
                if !state.is_finished {
                    next_split_idx = idx;
                    break;
                }
                extends_current_actor_bound(&mut current_actor_bounds, split);
                if let Some(ref cdc_offset) = state.cdc_offset_low {
                    if let Some(ref cur) = actor_cdc_offset_low {
                        if *cur > *cdc_offset {
                            actor_cdc_offset_low = state.cdc_offset_low.clone();
                        }
                    } else {
                        actor_cdc_offset_low = state.cdc_offset_low.clone();
                    }
                }
                if let Some(ref cdc_offset) = state.cdc_offset_high {
                    if let Some(ref cur) = actor_cdc_offset_high {
                        if *cur < *cdc_offset {
                            actor_cdc_offset_high = state.cdc_offset_high.clone();
                        }
                    } else {
                        actor_cdc_offset_high = state.cdc_offset_high.clone();
                    }
                }
            }
            for split in actor_snapshot_splits.iter().skip(next_split_idx) {
                // Initialize state so that overall progress can be measured.
                state_impl
                    .mutate_state(split.split_id, false, 0, None, None)
                    .await?;
            }
            let mut should_report_actor_backfill_progress = if next_split_idx > 0 {
                Some((
                    actor_snapshot_splits[0].split_id,
                    actor_snapshot_splits[next_split_idx - 1].split_id,
                ))
            } else {
                None
            };

            // After init the state table and forward the initial barrier to downstream,
            // we now try to create the table reader with retry.
            let mut table_reader: Option<ExternalTableReaderImpl> = None;
            let external_table = self.external_table.clone();
            let actor_id = self.actor_ctx.id;
            let fragment_id = self.actor_ctx.fragment_id;
            let mut future = Box::pin(async move {
                let backoff = get_infinite_backoff_strategy();
                tokio_retry::Retry::spawn(backoff, || async {
                    match external_table.create_table_reader().await {
                        Ok(reader) => Ok(reader),
                        Err(e) => {
                            tracing::warn!(error = %e.as_report(), actor_id = %actor_id, fragment_id = %fragment_id, "failed to create cdc table reader, retrying...");
                            Err(e)
                        }
                    }
                })
                    .instrument(tracing::info_span!("create_cdc_table_reader_with_retry"))
                    .await
                    .expect("Retry create cdc table reader until success.")
            });
            if split_key_needs_unsigned_i64_compare.is_none() && current_actor_bounds.is_some() {
                // Upstream events must be filtered against the finished splits, which needs the
                // split key comparison that legacy graphs only get from the reader. Create the
                // reader before polling upstream; unconsumed events stay buffered upstream.
                table_reader = Some(future.as_mut().await);
            }
            loop {
                if let Some(msg) =
                    build_reader_and_poll_upstream(&mut upstream, &mut table_reader, &mut future)
                        .await?
                {
                    let msg = match msg {
                        Message::Chunk(chunk) => {
                            let Some(chunk) = upstream_parser
                                .parse(
                                    chunk,
                                    &current_actor_bounds,
                                    split_key_needs_unsigned_i64_compare.unwrap_or_default(),
                                )
                                .await?
                            else {
                                continue;
                            };
                            Message::Chunk(chunk)
                        }
                        msg => msg,
                    };
                    if let Some(msg) = mapping_message(msg, &self.output_indices) {
                        match msg {
                            Message::Barrier(barrier) => {
                                state_impl.commit_state(barrier.epoch).await?;
                                if is_reset_barrier(&barrier, self.actor_ctx.id) {
                                    next_reset_barrier = Some(barrier);
                                    continue 'with_cdc_table_snapshot_splits;
                                }
                                yield Message::Barrier(barrier);
                            }
                            Message::Chunk(chunk) => {
                                if chunk.cardinality() == 0 {
                                    continue;
                                }
                                if let Some(filtered_chunk) = filter_stream_chunk(
                                    chunk,
                                    &current_actor_bounds,
                                    snapshot_split_column_index,
                                    split_key_needs_unsigned_i64_compare.unwrap_or_default(),
                                ) && filtered_chunk.cardinality() > 0
                                {
                                    yield Message::Chunk(filtered_chunk);
                                }
                            }
                            Message::Watermark(_) => {
                                // Ignore watermark, like the `CdcBackfillExecutor`.
                            }
                        }
                    }
                } else {
                    assert!(table_reader.is_some(), "table reader must created");
                    tracing::info!(
                        %table_id,
                        upstream_table_name,
                        "table reader created successfully"
                    );
                    break;
                }
            }
            let table_reader = table_reader.expect("table reader must created");
            let split_key_unsigned = match split_key_needs_unsigned_i64_compare {
                Some(needs_unsigned) => needs_unsigned,
                None => {
                    let split_column_name = cdc_table_snapshot_split_column[0].name.clone();
                    let comparisons = table_reader.pk_column_comparisons(&[split_column_name])?;
                    assert_eq!(comparisons.len(), 1);
                    let needs_unsigned = comparisons[0] == CdcKeyComparison::UnsignedInt64;
                    split_key_needs_unsigned_i64_compare = Some(needs_unsigned);
                    needs_unsigned
                }
            };
            let upstream_table_reader =
                UpstreamTableReader::new(self.external_table.clone(), table_reader);
            // let mut upstream = upstream.peekable();
            let offset_parse_func = upstream_table_reader.reader.get_cdc_offset_parser();

            // Backfill snapshot splits sequentially.
            for split in actor_snapshot_splits.iter().skip(next_split_idx) {
                tracing::info!(
                    %table_id,
                    upstream_table_name,
                    ?split,
                    is_snapshot_paused,
                    "start cdc backfill split"
                );
                let finished_split_bounds = current_actor_bounds.clone();
                let current_split_bounds = Some((
                    split.left_bound_inclusive.clone(),
                    split.right_bound_exclusive.clone(),
                ));
                extends_current_actor_bound(&mut current_actor_bounds, split);

                let split_cdc_offset_low = {
                    // Limit concurrent CDC connections globally to 10 using a semaphore.
                    static CDC_CONN_SEMAPHORE: tokio::sync::Semaphore =
                        tokio::sync::Semaphore::const_new(10);

                    let _permit = CDC_CONN_SEMAPHORE.acquire().await.unwrap();
                    upstream_table_reader.current_cdc_offset().await?
                };
                if let Some(ref cdc_offset) = split_cdc_offset_low {
                    if let Some(ref cur) = actor_cdc_offset_low {
                        if *cur > *cdc_offset {
                            actor_cdc_offset_low = split_cdc_offset_low.clone();
                        }
                    } else {
                        actor_cdc_offset_low = split_cdc_offset_low.clone();
                    }
                }
                let mut split_cdc_offset_high = None;

                let left_upstream = upstream.by_ref().map(Either::Left);
                let read_args = SplitSnapshotReadArgs::new(
                    split.left_bound_inclusive.clone(),
                    split.right_bound_exclusive.clone(),
                    cdc_table_snapshot_split_column.clone(),
                    self.rate_limit_rps,
                    additional_columns.clone(),
                    schema_table_name.clone(),
                    external_database_name.clone(),
                );
                // Fold the WAL catch-up gate into the snapshot stream so `select_with_strategy`
                // keeps servicing upstream barriers while it waits (it yields nothing until
                // catch-up completes). No-op unless `snapshot.dedicated`.
                let gate_config = self.external_table.config().clone();
                let right_snapshot = pin!(
                    gated_snapshot_read_table_split(
                        &upstream_table_reader,
                        gate_config,
                        read_args,
                    )
                    .map(Either::Right)
                );
                let (right_snapshot, snapshot_valve) = pausable(right_snapshot);
                if is_snapshot_paused {
                    snapshot_valve.pause();
                }
                let mut backfill_stream =
                    select_with_strategy(left_upstream, right_snapshot, |_: &mut ()| {
                        stream::PollNext::Left
                    });
                let mut row_count: u64 = 0;
                #[for_await]
                for either in &mut backfill_stream {
                    match either {
                        // Upstream
                        Either::Left(msg) => {
                            match msg? {
                                Message::Barrier(barrier) => {
                                    state_impl.commit_state(barrier.epoch).await?;
                                    if let Some(mutation) = barrier.mutation.as_deref() {
                                        use crate::executor::Mutation;
                                        match mutation {
                                            Mutation::Pause => {
                                                is_snapshot_paused = true;
                                                snapshot_valve.pause();
                                            }
                                            Mutation::Resume => {
                                                is_snapshot_paused = false;
                                                snapshot_valve.resume();
                                            }
                                            Mutation::Throttle(some) => {
                                                // TODO(zw): optimization: improve throttle.
                                                // 1. Handle rate limit 0. Currently, to resume the process, the actor must be rebuilt.
                                                // 2. Apply new rate limit immediately.
                                                if let Some(entry) =
                                                    some.get(&self.actor_ctx.fragment_id)
                                                    && entry.throttle_type()
                                                        == ThrottleType::Backfill
                                                    && entry.rate_limit != self.rate_limit_rps
                                                {
                                                    // The new rate limit will take effect since next split.
                                                    self.rate_limit_rps = entry.rate_limit;
                                                }
                                            }
                                            mutation if mutation.is_stop(self.actor_ctx.id) => {
                                                tracing::info!(
                                                    %table_id,
                                                    upstream_table_name,
                                                    "CdcBackfill has been dropped due to config change"
                                                );
                                                for chunk in upstream_chunk_buffer.drain(..) {
                                                    yield Message::Chunk(chunk);
                                                }
                                                yield Message::Barrier(barrier);
                                                let () = futures::future::pending().await;
                                                unreachable!();
                                            }
                                            _ => (),
                                        }
                                    }
                                    if is_reset_barrier(&barrier, self.actor_ctx.id) {
                                        next_reset_barrier = Some(barrier);
                                        for chunk in upstream_chunk_buffer.drain(..) {
                                            yield Message::Chunk(chunk);
                                        }
                                        continue 'with_cdc_table_snapshot_splits;
                                    }
                                    if let Some(split_range) =
                                        should_report_actor_backfill_progress.take()
                                        && let Some(ref progress) = self.progress
                                    {
                                        progress.update(
                                            self.actor_ctx.fragment_id,
                                            self.actor_ctx.id,
                                            barrier.epoch,
                                            generation.expect("should have set generation when having progress to report"),
                                            split_range,
                                        );
                                    }
                                    // emit barrier and continue to consume the backfill stream
                                    yield Message::Barrier(barrier);
                                }
                                Message::Chunk(chunk) => {
                                    // skip empty upstream chunk
                                    if chunk.cardinality() == 0 {
                                        continue;
                                    }

                                    // TODO(zw): re-enable
                                    // let chunk_cdc_offset =
                                    //     get_cdc_chunk_last_offset(&offset_parse_func, &chunk)?;
                                    // if *self.external_table.table_type()
                                    //     == ExternalCdcTableType::Postgres
                                    //     && let Some(cur) = actor_cdc_offset_low.as_ref()
                                    //     && let Some(chunk_offset) = chunk_cdc_offset
                                    //     && chunk_offset < *cur
                                    // {
                                    //     continue;
                                    // }

                                    // `current_actor_bounds` already covers the current split.
                                    let Some(chunk) = upstream_parser
                                        .parse(chunk, &current_actor_bounds, split_key_unsigned)
                                        .await?
                                    else {
                                        continue;
                                    };
                                    let chunk = mapping_chunk(chunk, &self.output_indices);
                                    let (finished_chunk, current_chunk) =
                                        split_finished_and_current_chunk(
                                            chunk,
                                            &finished_split_bounds,
                                            &current_split_bounds,
                                            snapshot_split_column_index,
                                            split_key_unsigned,
                                        );
                                    if let Some(finished_chunk) = finished_chunk
                                        && finished_chunk.cardinality() > 0
                                    {
                                        yield Message::Chunk(finished_chunk);
                                    }
                                    if let Some(filtered_chunk) = current_chunk
                                        && filtered_chunk.cardinality() > 0
                                    {
                                        // Buffer only rows that overlap the split currently being backfilled.
                                        upstream_chunk_buffer.push(filtered_chunk);
                                    }
                                }
                                Message::Watermark(_) => {
                                    // Ignore watermark during backfill, like the `CdcBackfillExecutor`.
                                }
                            }
                        }
                        // Snapshot read
                        Either::Right(msg) => {
                            match msg? {
                                None => {
                                    tracing::info!(
                                        %table_id,
                                        split_id = split.split_id,
                                        "snapshot read stream ends"
                                    );
                                    for chunk in upstream_chunk_buffer.drain(..) {
                                        yield Message::Chunk(chunk);
                                    }

                                    split_cdc_offset_high = {
                                        // Limit concurrent CDC connections globally to 10 using a semaphore.
                                        static CDC_CONN_SEMAPHORE: tokio::sync::Semaphore =
                                            tokio::sync::Semaphore::const_new(10);

                                        let _permit = CDC_CONN_SEMAPHORE.acquire().await.unwrap();
                                        upstream_table_reader.current_cdc_offset().await?
                                    };
                                    if let Some(ref cdc_offset) = split_cdc_offset_high {
                                        if let Some(ref cur) = actor_cdc_offset_high {
                                            if *cur < *cdc_offset {
                                                actor_cdc_offset_high =
                                                    split_cdc_offset_high.clone();
                                            }
                                        } else {
                                            actor_cdc_offset_high = split_cdc_offset_high.clone();
                                        }
                                    }
                                    // Next split.
                                    break;
                                }
                                Some(chunk) => {
                                    let chunk_cardinality = chunk.cardinality() as u64;
                                    row_count = row_count.saturating_add(chunk_cardinality);
                                    yield Message::Chunk(mapping_chunk(
                                        chunk,
                                        &self.output_indices,
                                    ));
                                }
                            }
                        }
                    }
                }
                // Mark current split backfill as finished. The state will be persisted by next barrier.
                state_impl
                    .mutate_state(
                        split.split_id,
                        true,
                        row_count,
                        split_cdc_offset_low,
                        split_cdc_offset_high,
                    )
                    .await?;
                if let Some((_, right_split)) = &mut should_report_actor_backfill_progress {
                    assert!(
                        *right_split < split.split_id,
                        "{} {}",
                        *right_split,
                        split.split_id
                    );
                    *right_split = split.split_id;
                } else {
                    should_report_actor_backfill_progress = Some((split.split_id, split.split_id));
                }
            }

            upstream_table_reader.disconnect().await?;
            tracing::info!(
                %table_id,
                upstream_table_name,
                "CdcBackfill has already finished and will forward messages directly to the downstream"
            );

            let mut should_report_actor_backfill_done = false;
            // One-shot latch ensuring completion is reported exactly once.
            let mut backfill_completion_reported = false;
            // After backfill progress finished
            // we can forward messages directly to the downstream,
            // as backfill is finished.
            #[for_await]
            for msg in &mut upstream {
                let msg = msg?;
                match msg {
                    Message::Barrier(barrier) => {
                        state_impl.commit_state(barrier.epoch).await?;
                        if is_reset_barrier(&barrier, self.actor_ctx.id) {
                            next_reset_barrier = Some(barrier);
                            continue 'with_cdc_table_snapshot_splits;
                        }
                        if let Some(split_range) = should_report_actor_backfill_progress.take()
                            && let Some(ref progress) = self.progress
                        {
                            progress.update(
                                self.actor_ctx.fragment_id,
                                self.actor_ctx.id,
                                barrier.epoch,
                                generation.expect(
                                    "should have set generation when having progress to report",
                                ),
                                split_range,
                            );
                        }
                        // Report completion on the first forwarding barrier: all splits are
                        // snapshotted and forwarded by now, so the cut is established. The
                        // `Message::Chunk` offset check alone hangs idle tables, whose only
                        // events are heartbeats (zero-cardinality chunks dropped upstream).
                        if !backfill_completion_reported && !actor_snapshot_splits.is_empty() {
                            should_report_actor_backfill_done = true;
                        }
                        if should_report_actor_backfill_done {
                            should_report_actor_backfill_done = false;
                            backfill_completion_reported = true;
                            actor_cdc_offset_high = None;
                            assert!(!actor_snapshot_splits.is_empty());
                            if let Some(ref progress) = self.progress {
                                progress.finish(
                                    self.actor_ctx.fragment_id,
                                    self.actor_ctx.id,
                                    barrier.epoch,
                                    generation.expect(
                                        "should have set generation when having progress to report",
                                    ),
                                    (
                                        actor_snapshot_splits[0].split_id,
                                        actor_snapshot_splits[actor_snapshot_splits.len() - 1]
                                            .split_id,
                                    ),
                                );
                            }
                        }
                        yield Message::Barrier(barrier);
                    }
                    Message::Chunk(chunk) => {
                        // Invariant: an actor with no splits drops ALL upstream data, live events
                        // included. The upstream dispatcher relies on this to stop sending data to
                        // such actors (`cdc_scan_actor_idleness` in `dispatch.rs`); if this ever
                        // starts consuming data without splits, that suppression loses rows.
                        if actor_snapshot_splits.is_empty() {
                            continue;
                        }
                        let Some(chunk) = upstream_parser
                            .parse(chunk, &current_actor_bounds, split_key_unsigned)
                            .await?
                        else {
                            continue;
                        };
                        if chunk.cardinality() == 0 {
                            continue;
                        }

                        let chunk_cdc_offset =
                            get_cdc_chunk_last_offset(&offset_parse_func, &chunk)?;
                        // // TODO(zw): re-enable
                        // if *self.external_table.table_type() == ExternalCdcTableType::Postgres
                        //     && let Some(cur) = actor_cdc_offset_low.as_ref()
                        //     && let Some(ref chunk_offset) = chunk_cdc_offset
                        //     && *chunk_offset < *cur
                        // {
                        //     continue;
                        // }

                        // should_report_actor_backfill_done is set to true at most once.
                        if let Some(high) = actor_cdc_offset_high.as_ref() {
                            if state_impl.is_legacy_state() {
                                // Since the legacy state does not track CDC offsets, report backfill completion immediately.
                                actor_cdc_offset_high = None;
                                should_report_actor_backfill_done = true;
                            } else if let Some(ref chunk_offset) = chunk_cdc_offset
                                && *chunk_offset >= *high
                            {
                                // Report backfill completion once the latest CDC offset exceeds the highest offset tracked during the backfill.
                                actor_cdc_offset_high = None;
                                should_report_actor_backfill_done = true;
                            }
                        }
                        let chunk = mapping_chunk(chunk, &self.output_indices);
                        if let Some(filtered_chunk) = filter_stream_chunk(
                            chunk,
                            &current_actor_bounds,
                            snapshot_split_column_index,
                            split_key_unsigned,
                        ) && filtered_chunk.cardinality() > 0
                        {
                            yield Message::Chunk(filtered_chunk);
                        }
                    }
                    msg @ Message::Watermark(_) => {
                        if let Some(msg) = mapping_message(msg, &self.output_indices) {
                            yield msg;
                        }
                    }
                }
            }
        }
    }
}

fn split_finished_and_current_chunk(
    chunk: StreamChunk,
    finished_split_bounds: &Option<(OwnedRow, OwnedRow)>,
    current_split_bounds: &Option<(OwnedRow, OwnedRow)>,
    snapshot_split_column_index: usize,
    split_key_unsigned: bool,
) -> (Option<StreamChunk>, Option<StreamChunk>) {
    let finished_chunk = filter_stream_chunk(
        chunk.clone(),
        finished_split_bounds,
        snapshot_split_column_index,
        split_key_unsigned,
    )
    .map(StreamChunk::compact_vis);
    let current_chunk = filter_stream_chunk(
        chunk,
        current_split_bounds,
        snapshot_split_column_index,
        split_key_unsigned,
    )
    .map(StreamChunk::compact_vis);
    (finished_chunk, current_chunk)
}

/// Compare two split keys, reinterpreting `i64` as `u64` for MySQL `BIGINT UNSIGNED`.
fn cmp_split_key(
    lhs: DatumRef<'_>,
    rhs: DatumRef<'_>,
    order: OrderType,
    split_key_unsigned: bool,
) -> Ordering {
    cmp_pk_unsigned_aware(
        std::iter::once(lhs),
        std::iter::once(rhs),
        &[order],
        &[split_key_unsigned],
    )
}

fn filter_stream_chunk(
    chunk: StreamChunk,
    bound: &Option<(OwnedRow, OwnedRow)>,
    snapshot_split_column_index: usize,
    split_key_unsigned: bool,
) -> Option<StreamChunk> {
    // No bound means no splits: drop everything. See the invariant note in the chunk arm of
    // the main loop; `cdc_scan_actor_idleness` in `dispatch.rs` depends on it.
    let Some((left, right)) = bound else {
        return None;
    };
    assert_eq!(left.len(), 1, "multiple split columns is not supported yet");
    assert_eq!(
        right.len(),
        1,
        "multiple split columns is not supported yet"
    );
    if is_leftmost_bound(left) && is_rightmost_bound(right) {
        return Some(chunk);
    }
    let mut new_bitmap = BitmapBuilder::with_capacity(chunk.capacity());
    let (ops, columns, visibility) = chunk.into_inner();
    for (row_split_key, v) in columns[snapshot_split_column_index]
        .iter()
        .zip_eq_fast(visibility.iter())
    {
        if !v {
            new_bitmap.append(false);
            continue;
        }
        let is_in_range = is_in_split_range(row_split_key, left, right, split_key_unsigned);
        if !is_in_range {
            tracing::trace!(?row_split_key, ?left, ?right, snapshot_split_column_index, data_type = ?columns[snapshot_split_column_index].data_type(), "filter out row")
        }
        new_bitmap.append(is_in_range);
    }
    Some(StreamChunk::with_visibility(
        ops,
        columns,
        new_bitmap.finish(),
    ))
}

/// Returns whether `split_key` is in `[left, right)`, where an all-NULL bound is unbounded.
fn is_in_split_range(
    split_key: DatumRef<'_>,
    left: &OwnedRow,
    right: &OwnedRow,
    split_key_unsigned: bool,
) -> bool {
    let order = OrderType::ascending_nulls_first();
    (is_leftmost_bound(left)
        || cmp_split_key(split_key, left.datum_at(0), order, split_key_unsigned).is_ge())
        && (is_rightmost_bound(right)
            || cmp_split_key(split_key, right.datum_at(0), order, split_key_unsigned).is_lt())
}

/// Parses upstream chunks, skipping the Debezium parse of rows outside the actor's splits.
///
/// Upstream rows are broadcast to every backfill actor that has splits, and each actor keeps
/// only the rows in its own splits. Reading the split key straight from the JSON payload lets
/// an actor drop the other rows before the parse, which costs far more than the lookup. The
/// filter after the parse still decides which rows are kept.
struct UpstreamChunkParser {
    parser: DebeziumParser,
    split_key_name: String,
    split_key_type: DataType,
}

impl UpstreamChunkParser {
    /// Returns `None` when no row of `chunk` can be in `bounds`.
    async fn parse(
        &mut self,
        chunk: StreamChunk,
        bounds: &Option<(OwnedRow, OwnedRow)>,
        split_key_unsigned: bool,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        // The parser expects one visible payload per row, and callers drop empty chunks anyway.
        if chunk.cardinality() == 0 {
            return Ok(None);
        }
        let Some(chunk) = self.drop_rows_out_of_bounds(chunk, bounds, split_key_unsigned) else {
            return Ok(None);
        };
        parse_debezium_chunk(&mut self.parser, &chunk)
            .await
            .map(Some)
    }

    fn drop_rows_out_of_bounds(
        &self,
        chunk: StreamChunk,
        bounds: &Option<(OwnedRow, OwnedRow)>,
        split_key_unsigned: bool,
    ) -> Option<StreamChunk> {
        // No bound means no splits, and `filter_stream_chunk` drops everything.
        let (left, right) = bounds.as_ref()?;
        // A `BIGINT UNSIGNED` key is encoded according to `bigint_unsigned_handling`, which
        // this lookup does not replicate.
        if split_key_unsigned || (is_leftmost_bound(left) && is_rightmost_bound(right)) {
            return Some(chunk);
        }
        let mut visibility = BitmapBuilder::with_capacity(chunk.capacity());
        let mut dropped_any = false;
        for (payload, visible) in chunk.columns()[0]
            .iter()
            .zip_eq_fast(chunk.visibility().iter())
        {
            let keep = visible && self.may_be_in_range(payload, left, right).unwrap_or(true);
            dropped_any |= visible && !keep;
            visibility.append(keep);
        }
        if !dropped_any {
            return Some(chunk);
        }
        let chunk = chunk.clone_with_vis(visibility.finish());
        // The parser expects one visible payload per row.
        (chunk.cardinality() > 0).then(|| chunk.compact_vis())
    }

    /// Returns `None` when the split key cannot be read exactly as the parser would read it.
    fn may_be_in_range(
        &self,
        payload: DatumRef<'_>,
        left: &OwnedRow,
        right: &OwnedRow,
    ) -> Option<bool> {
        let Some(ScalarRefImpl::Jsonb(event)) = payload else {
            return None;
        };
        // Like `DebeziumJsonAccessBuilder`, read the event inside an optional `payload` envelope.
        let payload = event.access_object_field("payload").unwrap_or(event);
        // The parser reads `after` for an insert, update or snapshot read, and `before` for a
        // delete.
        let row = match get_json_field(payload, "op")?.as_str().ok()? {
            "r" | "c" | "u" => "after",
            "d" => "before",
            _ => return None,
        };
        let value = get_json_field(get_json_field(payload, row)?, &self.split_key_name)?;
        let split_key = match self.split_key_type {
            DataType::Int16 => ScalarRefImpl::Int16(value.as_i64()?.try_into().ok()?),
            DataType::Int32 => ScalarRefImpl::Int32(value.as_i64()?.try_into().ok()?),
            DataType::Int64 => ScalarRefImpl::Int64(value.as_i64()?),
            DataType::Varchar => ScalarRefImpl::Utf8(value.as_str().ok()?),
            _ => return None,
        };
        Some(is_in_split_range(Some(split_key), left, right, false))
    }
}

/// Gets a field the way the Debezium parser does: exact name first, then ignoring ASCII case.
/// Returns `None` if several fields match ignoring case, since the parser's pick among them
/// depends on its map's iteration order.
fn get_json_field<'a>(object: JsonbRef<'a>, name: &str) -> Option<JsonbRef<'a>> {
    if let Some(value) = object.access_object_field(name) {
        return Some(value);
    }
    let mut matches = object
        .object_key_values()
        .ok()?
        .filter(|(key, _)| key.eq_ignore_ascii_case(name));
    let (_, value) = matches.next()?;
    matches.next().is_none().then_some(value)
}

fn is_leftmost_bound(row: &OwnedRow) -> bool {
    row.iter().all(|d| d.is_none())
}

fn is_rightmost_bound(row: &OwnedRow) -> bool {
    row.iter().all(|d| d.is_none())
}

impl<S: StateStore> Execute for ParallelizedCdcBackfillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

fn extends_current_actor_bound(
    current: &mut Option<(OwnedRow, OwnedRow)>,
    split: &CdcTableSnapshotSplit,
) {
    if current.is_none() {
        *current = Some((
            split.left_bound_inclusive.clone(),
            split.right_bound_exclusive.clone(),
        ));
    } else {
        current.as_mut().unwrap().1 = split.right_bound_exclusive.clone();
    }
}

fn is_reset_barrier(barrier: &Barrier, actor_id: ActorId) -> bool {
    match barrier.mutation.as_deref() {
        Some(Mutation::Update(update)) => update
            .actor_cdc_table_snapshot_splits
            .splits
            .contains_key(&actor_id),
        _ => false,
    }
}

/// Waits for standby WAL catch-up (`prepare_snapshot`, no-op unless `snapshot.dedicated`),
/// then streams the split snapshot. Gating inside the stream keeps the caller's select loop
/// free to service barriers while waiting. Safe here only because V2 has no "consume snapshot
/// once" patch that would force a blocking poll outside the select loop.
#[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
async fn gated_snapshot_read_table_split(
    upstream_table_reader: &UpstreamTableReader<ExternalStorageTable>,
    config: ExternalTableConfig,
    read_args: SplitSnapshotReadArgs,
) {
    upstream_table_reader
        .reader
        .prepare_snapshot(&config)
        .await
        .map_err(StreamExecutorError::connector_error)?;
    #[for_await]
    for msg in upstream_table_reader.snapshot_read_table_split(read_args) {
        yield msg?;
    }
}

/// `split_key_unsigned` is `None` when the split key comparison is not resolved yet; the bound
/// order is then not checked, since a `BIGINT UNSIGNED` key would look unordered as `i64`.
fn assert_consecutive_splits(
    actor_snapshot_splits: &[CdcTableSnapshotSplit],
    split_key_unsigned: Option<bool>,
) {
    for i in 1..actor_snapshot_splits.len() {
        assert_eq!(
            actor_snapshot_splits[i].split_id,
            actor_snapshot_splits[i - 1].split_id + 1,
            "{:?}",
            actor_snapshot_splits
        );
        if let Some(split_key_unsigned) = split_key_unsigned {
            assert!(
                cmp_split_key(
                    actor_snapshot_splits[i - 1]
                        .right_bound_exclusive
                        .datum_at(0),
                    actor_snapshot_splits[i].right_bound_exclusive.datum_at(0),
                    OrderType::ascending_nulls_last(),
                    split_key_unsigned,
                )
                .is_lt()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::types::{DataType, JsonbVal, ScalarImpl};
    use risingwave_connector::source::CdcTableSnapshotSplit;

    use crate::executor::backfill::cdc::cdc_backfill::{
        build_debezium_parser, parse_debezium_chunk,
    };
    use crate::executor::backfill::cdc::cdc_backill_v2::{
        UpstreamChunkParser, assert_consecutive_splits, filter_stream_chunk,
        split_finished_and_current_chunk,
    };

    /// A MySQL `BIGINT UNSIGNED` value as RisingWave stores it in an `Int64` column.
    fn unsigned_i64(v: u64) -> ScalarImpl {
        ScalarImpl::Int64(v as i64)
    }

    /// An upstream chunk of `(payload, _rw_offset)` rows.
    fn upstream_chunk(payloads: &[&str]) -> StreamChunk {
        let rows = payloads
            .iter()
            .enumerate()
            .map(|(i, payload)| {
                (
                    Op::Insert,
                    OwnedRow::new(vec![
                        Some(JsonbVal::from_str(payload).unwrap().into()),
                        Some(ScalarImpl::Utf8(format!("offset {i}").into())),
                    ]),
                )
            })
            .collect::<Vec<_>>();
        StreamChunk::from_rows(&rows, &[DataType::Jsonb, DataType::Varchar])
    }

    /// A parser of `(id bigint, name varchar)` rows, split on `split_key`.
    async fn new_upstream_parser(split_key: &str) -> UpstreamChunkParser {
        let columns = [
            ColumnDesc::named("id", ColumnId::new(1), DataType::Int64),
            ColumnDesc::named("name", ColumnId::new(2), DataType::Varchar),
        ];
        let split_key_type = columns
            .iter()
            .find(|c| c.name == split_key)
            .unwrap()
            .data_type
            .clone();
        UpstreamChunkParser {
            parser: build_debezium_parser(&columns, None, None, None, None, false)
                .await
                .unwrap(),
            split_key_name: split_key.to_owned(),
            split_key_type,
        }
    }

    fn split_bounds(
        left: Option<ScalarImpl>,
        right: Option<ScalarImpl>,
    ) -> Option<(OwnedRow, OwnedRow)> {
        Some((OwnedRow::new(vec![left]), OwnedRow::new(vec![right])))
    }

    #[tokio::test]
    async fn test_upstream_chunk_parser_keeps_the_rows_of_a_full_parse() {
        let chunk = upstream_chunk(&[
            r#"{"op": "c", "before": null, "after": {"id": 1, "name": "a"}}"#,
            r#"{"op": "c", "before": null, "after": {"id": 3, "name": "b"}}"#,
            r#"{"payload": {"op": "r", "before": null, "after": {"id": 7, "name": "c"}}}"#,
            r#"{"op": "d", "before": {"id": 4, "name": "d"}, "after": null}"#,
            r#"{"op": "d", "before": {"id": 9, "name": "e"}, "after": null}"#,
            r#"{"op": "u", "before": {"id": 2, "name": "f"}, "after": {"id": 2, "name": "g"}}"#,
            r#"{"op": "c", "before": null, "after": {"ID": 8, "name": "h"}}"#,
            // The exact name wins over a name that differs only in case.
            r#"{"op": "c", "before": null, "after": {"id": 3, "ID": 9, "name": "i"}}"#,
            // Not readable as the parser reads a `bigint`, so it is left to the parser.
            r#"{"op": "c", "before": null, "after": {"id": "3", "name": "j"}}"#,
        ]);
        let int = |v| Some(ScalarImpl::Int64(v));
        for (bounds, parsed_rows) in [
            (split_bounds(int(2), int(5)), 5),
            (split_bounds(None, int(5)), 6),
            (split_bounds(int(5), None), 4),
        ] {
            let mut upstream_parser = new_upstream_parser("id").await;
            let parsed = upstream_parser
                .parse(chunk.clone(), &bounds, false)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(parsed.capacity(), parsed_rows);

            let fully_parsed = parse_debezium_chunk(&mut upstream_parser.parser, &chunk)
                .await
                .unwrap();
            let filter = |chunk| {
                filter_stream_chunk(chunk, &bounds, 0, false)
                    .unwrap()
                    .compact_vis()
            };
            assert_eq!(filter(parsed), filter(fully_parsed));
        }
    }

    #[tokio::test]
    async fn test_upstream_chunk_parser_drops_rows_by_varchar_split_key() {
        let chunk = upstream_chunk(&[
            r#"{"op": "c", "after": {"id": 1, "name": "a"}}"#,
            r#"{"op": "c", "after": {"id": 2, "name": "c"}}"#,
            r#"{"op": "c", "after": {"id": 3, "name": "e"}}"#,
        ]);
        let upstream_parser = new_upstream_parser("name").await;
        let varchar = |v: &str| Some(ScalarImpl::Utf8(v.into()));

        let kept = upstream_parser
            .drop_rows_out_of_bounds(
                chunk.clone(),
                &split_bounds(varchar("b"), varchar("d")),
                false,
            )
            .unwrap();
        assert_eq!(kept.capacity(), 1);
        assert_eq!(chunk_payload(&kept, 0), chunk_payload(&chunk, 1));

        // An actor without splits keeps nothing.
        assert!(
            upstream_parser
                .drop_rows_out_of_bounds(chunk.clone(), &None, false)
                .is_none()
        );
        // Every row out of bounds.
        assert!(
            upstream_parser
                .drop_rows_out_of_bounds(chunk, &split_bounds(varchar("x"), varchar("y")), false)
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_upstream_chunk_parser_keeps_rows_it_cannot_read() {
        let chunk = upstream_chunk(&[
            // The parser's pick between these depends on its map's iteration order.
            r#"{"op": "c", "after": {"Id": 1, "ID": 9, "name": "a"}}"#,
            r#"{"op": "x", "after": {"id": 9, "name": "a"}}"#,
            r#"{"op": "c", "after": {"id": 9.5, "name": "a"}}"#,
            r#"{"op": "c", "after": {"id": null, "name": "a"}}"#,
            r#"{"op": "c", "after": {"name": "a"}}"#,
            r#"{"op": "c", "after": null}"#,
        ]);
        let upstream_parser = new_upstream_parser("id").await;
        let bounds = split_bounds(Some(ScalarImpl::Int64(0)), Some(ScalarImpl::Int64(5)));
        assert_eq!(
            upstream_parser
                .drop_rows_out_of_bounds(chunk.clone(), &bounds, false)
                .unwrap(),
            chunk
        );

        // `BIGINT UNSIGNED` keys are left to the filter after the parse.
        let chunk = upstream_chunk(&[r#"{"op": "c", "after": {"id": 9, "name": "a"}}"#]);
        assert_eq!(
            upstream_parser
                .drop_rows_out_of_bounds(chunk.clone(), &bounds, true)
                .unwrap(),
            chunk
        );
    }

    fn chunk_payload(chunk: &StreamChunk, row: usize) -> String {
        chunk
            .row_at(row)
            .1
            .datum_at(0)
            .unwrap()
            .into_jsonb()
            .to_string()
    }

    #[test]
    fn test_filter_stream_chunk_unsigned_bigint_split_key() {
        let keys = [50, 200, (1 << 63) + 5, u64::MAX];
        let rows = keys
            .iter()
            .map(|&k| (Op::Insert, OwnedRow::new(vec![Some(unsigned_i64(k))])))
            .collect::<Vec<_>>();
        let chunk = StreamChunk::from_rows(&rows, &[DataType::Int64]);
        // The split crosses 2^63, so its right bound is negative as `i64`.
        let bound = Some((
            OwnedRow::new(vec![Some(unsigned_i64(100))]),
            OwnedRow::new(vec![Some(unsigned_i64((1 << 63) + 10))]),
        ));

        let c = filter_stream_chunk(chunk.clone(), &bound, 0, true).unwrap();
        let expected = StreamChunk::from_rows(&rows[1..3], &[DataType::Int64]);
        assert_eq!(c.compact_vis(), expected);

        // Signed comparison sees an empty range and would drop every event of the split.
        let c = filter_stream_chunk(chunk, &bound, 0, false).unwrap();
        assert_eq!(c.cardinality(), 0);
    }

    #[test]
    fn test_assert_consecutive_splits_unsigned_bigint_split_key() {
        let split = |split_id, left: Option<u64>, right: Option<u64>| CdcTableSnapshotSplit {
            split_id,
            left_bound_inclusive: OwnedRow::new(vec![left.map(unsigned_i64)]),
            right_bound_exclusive: OwnedRow::new(vec![right.map(unsigned_i64)]),
        };
        let splits = vec![
            split(1, None, Some(100)),
            split(2, Some(100), Some((1 << 63) + 10)),
            split(3, Some((1 << 63) + 10), None),
        ];
        assert_consecutive_splits(&splits, Some(true));
        // Unresolved comparison skips the bound order check.
        assert_consecutive_splits(&splits, None);
    }

    #[test]
    #[should_panic]
    fn test_assert_consecutive_splits_signed_rejects_unsigned_order() {
        let splits = vec![
            CdcTableSnapshotSplit {
                split_id: 1,
                left_bound_inclusive: OwnedRow::new(vec![None]),
                right_bound_exclusive: OwnedRow::new(vec![Some(unsigned_i64(100))]),
            },
            CdcTableSnapshotSplit {
                split_id: 2,
                left_bound_inclusive: OwnedRow::new(vec![Some(unsigned_i64(100))]),
                right_bound_exclusive: OwnedRow::new(vec![Some(unsigned_i64((1 << 63) + 10))]),
            },
        ];
        assert_consecutive_splits(&splits, Some(false));
    }

    #[test]
    fn test_filter_stream_chunk() {
        use risingwave_common::array::StreamChunkTestExt;
        let chunk = StreamChunk::from_pretty(
            "  I I
             + 1 6
             - 2 .
            U- 3 7
            U+ 4 .",
        );
        let bound = None;
        let c = filter_stream_chunk(chunk.clone(), &bound, 0, false);
        assert!(c.is_none());

        let bound = Some((OwnedRow::new(vec![None]), OwnedRow::new(vec![None])));
        let c = filter_stream_chunk(chunk.clone(), &bound, 0, false);
        assert_eq!(c.unwrap().compact_vis(), chunk);

        let bound = Some((
            OwnedRow::new(vec![None]),
            OwnedRow::new(vec![Some(ScalarImpl::Int64(3))]),
        ));
        let c = filter_stream_chunk(chunk.clone(), &bound, 0, false);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
             + 1 6
             - 2 .",
            )
        );

        let bound = Some((
            OwnedRow::new(vec![Some(ScalarImpl::Int64(3))]),
            OwnedRow::new(vec![None]),
        ));
        let c = filter_stream_chunk(chunk.clone(), &bound, 0, false);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
            U- 3 7
            U+ 4 .",
            )
        );

        let bound = Some((
            OwnedRow::new(vec![Some(ScalarImpl::Int64(2))]),
            OwnedRow::new(vec![Some(ScalarImpl::Int64(4))]),
        ));
        let c = filter_stream_chunk(chunk.clone(), &bound, 0, false);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
             - 2 .
            U- 3 7",
            )
        );

        // Test NULL value.
        let bound = None;
        let c = filter_stream_chunk(chunk.clone(), &bound, 1, false);
        assert!(c.is_none());

        let bound = Some((OwnedRow::new(vec![None]), OwnedRow::new(vec![None])));
        let c = filter_stream_chunk(chunk.clone(), &bound, 1, false);
        assert_eq!(c.unwrap().compact_vis(), chunk);

        let bound = Some((
            OwnedRow::new(vec![None]),
            OwnedRow::new(vec![Some(ScalarImpl::Int64(7))]),
        ));
        let c = filter_stream_chunk(chunk.clone(), &bound, 1, false);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
             + 1 6
             - 2 .
            U+ 4 .",
            )
        );

        let bound = Some((
            OwnedRow::new(vec![Some(ScalarImpl::Int64(7))]),
            OwnedRow::new(vec![None]),
        ));
        let c = filter_stream_chunk(chunk, &bound, 1, false);
        assert_eq!(
            c.unwrap().compact_vis(),
            StreamChunk::from_pretty(
                "  I I
            U- 3 7",
            )
        );
    }

    #[test]
    fn test_split_finished_and_current_chunk() {
        use risingwave_common::array::StreamChunkTestExt;

        let chunk = StreamChunk::from_pretty(
            "  I I
             + 1 11
             + 6 10
             + 199 40",
        );
        let finished_split_bounds = Some((
            OwnedRow::new(vec![Some(ScalarImpl::Int64(1))]),
            OwnedRow::new(vec![Some(ScalarImpl::Int64(6))]),
        ));
        let current_split_bounds = Some((
            OwnedRow::new(vec![Some(ScalarImpl::Int64(6))]),
            OwnedRow::new(vec![Some(ScalarImpl::Int64(100))]),
        ));

        let (finished_chunk, current_chunk) = split_finished_and_current_chunk(
            chunk,
            &finished_split_bounds,
            &current_split_bounds,
            0,
            false,
        );

        assert_eq!(
            finished_chunk.unwrap(),
            StreamChunk::from_pretty(
                "  I I
                 + 1 11",
            )
        );
        assert_eq!(
            current_chunk.unwrap(),
            StreamChunk::from_pretty(
                "  I I
                 + 6 10",
            )
        );
    }
}
