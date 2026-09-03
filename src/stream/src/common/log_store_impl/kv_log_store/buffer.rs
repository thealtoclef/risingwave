// Copyright 2023 RisingWave Labs
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

use std::collections::VecDeque;
use std::mem::size_of;
use std::ops::DerefMut;
use std::sync::Arc;

use await_tree::InstrumentAwait;
use parking_lot::{Mutex, MutexGuard};
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_connector::sink::log_store::{ChunkId, LogStoreResult, TruncateOffset};
use risingwave_pb::stream_plan::PbSinkSchemaChange;
use tokio::sync::Notify;

use crate::common::log_store_impl::kv_log_store::{
    KvLogStoreMetrics, ReaderTruncationOffsetType, SeqId,
};

#[derive(Clone)]
pub(crate) enum LogStoreBufferItem {
    StreamChunk {
        chunk: StreamChunk,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        flushed: bool,
        chunk_id: ChunkId,
    },

    Flushed {
        vnode_bitmap: Bitmap,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        chunk_id: ChunkId,
    },

    Barrier {
        is_checkpoint: bool,
        next_epoch: u64,
        schema_change: Option<PbSinkSchemaChange>,
        is_stop: bool,
    },
}

impl EstimateSize for LogStoreBufferItem {
    fn estimated_heap_size(&self) -> usize {
        match self {
            LogStoreBufferItem::StreamChunk { chunk, .. } => chunk.estimated_heap_size(),
            LogStoreBufferItem::Flushed { vnode_bitmap, .. } => vnode_bitmap.estimated_heap_size(),
            LogStoreBufferItem::Barrier { .. } => 0,
        }
    }

    fn estimated_size(&self) -> usize {
        size_of::<Self>() + self.estimated_heap_size()
    }
}

/// Number of rows an item contributes to `buffer_unconsumed_row_count`.
fn item_row_count(item: &LogStoreBufferItem) -> usize {
    match item {
        LogStoreBufferItem::StreamChunk { chunk, .. } => chunk.cardinality(),
        LogStoreBufferItem::Flushed {
            start_seq_id,
            end_seq_id,
            ..
        } => (end_seq_id - start_seq_id) as usize,
        LogStoreBufferItem::Barrier { .. } => 0,
    }
}

/// Number of epochs an item contributes to `buffer_unconsumed_epoch_count`.
fn item_epoch_count(item: &LogStoreBufferItem) -> usize {
    match item {
        LogStoreBufferItem::Barrier { .. } => 1,
        _ => 0,
    }
}

struct LogStoreBufferInner {
    /// Items not read by log reader. Newer item at the front
    unconsumed_queue: VecDeque<(u64, LogStoreBufferItem)>,
    /// Items already read by log reader by not truncated. Newer item at the front
    consumed_queue: VecDeque<(u64, LogStoreBufferItem)>,
    row_count: usize,
    max_row_count: usize,
    chunk_size: usize,

    truncation_list: VecDeque<ReaderTruncationOffsetType>,

    next_chunk_id: ChunkId,

    metrics: KvLogStoreMetrics,

    /// Running aggregates over the two queues, maintained incrementally by the
    /// `*_stats` helpers below, which saturate rather than wrap or panic, following
    /// `HeapSizeReporter` in `stream/src/cache/managed_lru.rs`. Recomputing them by walking the queues on every
    /// buffer operation makes the cost quadratic in the buffer length, which becomes
    /// a significant share of the CPU when a sink falls behind and the buffer grows
    /// to millions of items.
    unconsumed_memory_bytes: usize,
    consumed_memory_bytes: usize,
    unconsumed_row_count: usize,
    unconsumed_epoch_count: usize,
}

impl LogStoreBufferInner {
    fn update_buffer_metrics(&self) {
        self.metrics
            .buffer_unconsumed_epoch_count
            .set(self.unconsumed_epoch_count as _);
        self.metrics
            .buffer_unconsumed_row_count
            .set(self.unconsumed_row_count as _);
        self.metrics
            .buffer_unconsumed_item_count
            .set(self.unconsumed_queue.len() as _);
        self.metrics.buffer_unconsumed_min_epoch.set(
            self.unconsumed_queue
                .front()
                .map(|(epoch, _)| *epoch)
                .unwrap_or_default() as _,
        );
        self.metrics.buffer_memory_bytes.set(
            self.unconsumed_memory_bytes
                .saturating_add(self.consumed_memory_bytes) as _,
        );
    }

    /// Add `item`'s contribution to the unconsumed aggregates. Does not touch the queue.
    fn add_unconsumed_stats(&mut self, item: &LogStoreBufferItem) {
        self.unconsumed_memory_bytes = self
            .unconsumed_memory_bytes
            .saturating_add(item.estimated_size());
        self.unconsumed_row_count = self
            .unconsumed_row_count
            .saturating_add(item_row_count(item));
        self.unconsumed_epoch_count = self
            .unconsumed_epoch_count
            .saturating_add(item_epoch_count(item));
    }

    /// Remove `item`'s contribution from the unconsumed aggregates. Does not touch the queue.
    fn sub_unconsumed_stats(&mut self, item: &LogStoreBufferItem) {
        self.unconsumed_memory_bytes = self
            .unconsumed_memory_bytes
            .saturating_sub(item.estimated_size());
        self.unconsumed_row_count = self
            .unconsumed_row_count
            .saturating_sub(item_row_count(item));
        self.unconsumed_epoch_count = self
            .unconsumed_epoch_count
            .saturating_sub(item_epoch_count(item));
    }

    /// Add `item`'s contribution to the consumed aggregates. Does not touch the queue.
    fn add_consumed_stats(&mut self, item: &LogStoreBufferItem) {
        self.consumed_memory_bytes = self
            .consumed_memory_bytes
            .saturating_add(item.estimated_size());
    }

    /// Remove `item`'s contribution from the consumed aggregates. Does not touch the queue.
    fn sub_consumed_stats(&mut self, item: &LogStoreBufferItem) {
        self.consumed_memory_bytes = self
            .consumed_memory_bytes
            .saturating_sub(item.estimated_size());
    }

    /// Pop the oldest consumed item, keeping the aggregates in sync.
    fn pop_consumed_back(&mut self) {
        if let Some((_, item)) = self.consumed_queue.pop_back() {
            self.sub_consumed_stats(&item);
        }
    }

    fn can_add_stream_chunk(&self) -> bool {
        self.row_count < self.max_row_count
    }

    fn add_item(&mut self, epoch: u64, item: LogStoreBufferItem) {
        if let LogStoreBufferItem::StreamChunk { .. } = item {
            unreachable!("StreamChunk should call try_add_item")
        }
        if let LogStoreBufferItem::Barrier { .. } = &item {
            self.next_chunk_id = 0;
        }
        self.add_unconsumed_stats(&item);
        self.unconsumed_queue.push_front((epoch, item));
        self.update_buffer_metrics();
    }

    pub(crate) fn try_add_stream_chunk(
        &mut self,
        epoch: u64,
        chunk: StreamChunk,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
    ) -> Option<StreamChunk> {
        if !self.can_add_stream_chunk() {
            Some(chunk)
        } else {
            let chunk_id = self.next_chunk_id;
            self.next_chunk_id += 1;
            self.row_count += chunk.cardinality();
            let item = LogStoreBufferItem::StreamChunk {
                chunk,
                start_seq_id,
                end_seq_id,
                flushed: false,
                chunk_id,
            };
            self.add_unconsumed_stats(&item);
            self.unconsumed_queue.push_front((epoch, item));
            self.update_buffer_metrics();
            None
        }
    }

    fn pop_item(&mut self) -> Option<(u64, LogStoreBufferItem)> {
        if let Some((epoch, item)) = self.unconsumed_queue.pop_back() {
            self.sub_unconsumed_stats(&item);
            // The item is cloned into the consumed queue, so it is counted in both
            // queues until truncated. This matches the previous behaviour.
            self.add_consumed_stats(&item);
            self.consumed_queue.push_front((epoch, item.clone()));
            self.update_buffer_metrics();
            Some((epoch, item))
        } else {
            None
        }
    }

    fn add_flushed(
        &mut self,
        epoch: u64,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        new_vnode_bitmap: Bitmap,
    ) {
        let curr_chunk_size = (end_seq_id - start_seq_id + 1) as usize;
        // Decide first, without holding a mutable borrow, so that the merge below can
        // read the item's size before and after mutating it.
        let can_merge = if let Some((
            item_epoch,
            LogStoreBufferItem::Flushed {
                start_seq_id: prev_start_seq_id,
                end_seq_id: prev_end_seq_id,
                ..
            },
        )) = self.unconsumed_queue.front()
            && let prev_chunk_size = (*prev_end_seq_id - *prev_start_seq_id + 1) as usize
            && curr_chunk_size + prev_chunk_size <= self.chunk_size
        {
            assert!(
                *prev_end_seq_id < start_seq_id,
                "prev end_seq_id {} should be smaller than current start_seq_id {}",
                end_seq_id,
                start_seq_id
            );
            assert_eq!(
                epoch, *item_epoch,
                "epoch of newly added flushed item must be the same as the last flushed item"
            );
            true
        } else {
            false
        };

        if can_merge {
            // Extending the item changes its row count, and OR-ing the vnode bitmap can
            // change its heap size in place (a compact `Bitmap` reports zero heap size).
            // So take the item out, re-derive its contribution, and put it back, rather
            // than assuming the contribution is unchanged.
            let (item_epoch, mut item) = self
                .unconsumed_queue
                .pop_front()
                .expect("checked to be a mergeable flushed item above");
            self.sub_unconsumed_stats(&item);
            let LogStoreBufferItem::Flushed {
                end_seq_id: prev_end_seq_id,
                vnode_bitmap,
                ..
            } = &mut item
            else {
                unreachable!("checked to be a flushed item above")
            };
            *prev_end_seq_id = end_seq_id;
            *vnode_bitmap |= new_vnode_bitmap;
            self.add_unconsumed_stats(&item);
            self.unconsumed_queue.push_front((item_epoch, item));
            self.update_buffer_metrics();
        } else {
            let chunk_id = self.next_chunk_id;
            self.next_chunk_id += 1;
            // `add_item` refreshes the gauges.
            self.add_item(
                epoch,
                LogStoreBufferItem::Flushed {
                    start_seq_id,
                    end_seq_id,
                    vnode_bitmap: new_vnode_bitmap,
                    chunk_id,
                },
            );
        }
    }

    fn add_truncate_offset(&mut self, (epoch, seq_id): ReaderTruncationOffsetType) {
        if let Some((prev_epoch, prev_seq_id)) = self.truncation_list.back_mut()
            && *prev_epoch == epoch
        {
            *prev_seq_id = seq_id;
        } else {
            self.truncation_list.push_back((epoch, seq_id));
        }
    }

    fn rewind(&mut self, log_store_rewind_start_epoch: Option<u64>) {
        let rewind_start_epoch = log_store_rewind_start_epoch.unwrap_or(0);
        while let Some((epoch, item)) = self.consumed_queue.pop_front() {
            self.sub_consumed_stats(&item);
            if epoch > rewind_start_epoch {
                self.add_unconsumed_stats(&item);
                self.unconsumed_queue.push_back((epoch, item));
            }
        }
        self.update_buffer_metrics();
    }

    fn clear(&mut self) {
        self.consumed_queue.clear();
        self.unconsumed_queue.clear();
        self.next_chunk_id = 0;
        self.truncation_list.clear();
        self.row_count = 0;
        self.unconsumed_memory_bytes = 0;
        self.consumed_memory_bytes = 0;
        self.unconsumed_row_count = 0;
        self.unconsumed_epoch_count = 0;
    }
}

struct SharedMutex<T>(Arc<Mutex<T>>);

impl<T> Clone for SharedMutex<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> SharedMutex<T> {
    fn new(value: T) -> Self {
        Self(Arc::new(Mutex::new(value)))
    }

    fn inner(&self) -> MutexGuard<'_, T> {
        if let Some(guard) = self.0.try_lock() {
            guard
        } else {
            info!("fall back to lock");
            self.0.lock()
        }
    }
}

pub(crate) struct LogStoreBufferSender {
    buffer: SharedMutex<LogStoreBufferInner>,
    update_notify: Arc<Notify>,
    truncate_notify: Arc<Notify>,
}

impl LogStoreBufferSender {
    pub(crate) fn add_flushed(
        &self,
        epoch: u64,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        vnode_bitmap: Bitmap,
    ) {
        self.buffer
            .inner()
            .add_flushed(epoch, start_seq_id, end_seq_id, vnode_bitmap);
        self.update_notify.notify_waiters();
    }

    pub(crate) fn try_add_stream_chunk(
        &self,
        epoch: u64,
        chunk: StreamChunk,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
    ) -> Option<StreamChunk> {
        let ret = self
            .buffer
            .inner()
            .try_add_stream_chunk(epoch, chunk, start_seq_id, end_seq_id);
        if ret.is_none() {
            // notify when successfully add
            self.update_notify.notify_waiters();
        }
        ret
    }

    pub(crate) fn barrier(
        &self,
        epoch: u64,
        is_checkpoint: bool,
        next_epoch: u64,
        schema_change: Option<PbSinkSchemaChange>,
        is_stop: bool,
    ) {
        self.buffer.inner().add_item(
            epoch,
            LogStoreBufferItem::Barrier {
                is_checkpoint,
                next_epoch,
                schema_change,
                is_stop,
            },
        );
        self.update_notify.notify_waiters();
    }

    pub(crate) fn pop_truncation(&self, curr_epoch: u64) -> Option<ReaderTruncationOffsetType> {
        let mut inner = self.buffer.inner();
        let mut ret = None;
        while let Some((epoch, _)) = inner.truncation_list.front()
            && *epoch < curr_epoch
        {
            ret = inner.truncation_list.pop_front();
        }
        ret
    }

    pub(crate) async fn wait_for_barrier_truncation(
        &self,
        curr_epoch: u64,
    ) -> LogStoreResult<ReaderTruncationOffsetType> {
        loop {
            let notified = self.truncate_notify.notified();

            {
                let mut inner = self.buffer.inner();
                while let Some((epoch, seq_id)) = inner.truncation_list.pop_front() {
                    if epoch > curr_epoch {
                        // TODO: should panic, after we confirm the correctness
                        return Err(anyhow::anyhow!(
                            "truncation epoch {} should not be larger than current epoch {}",
                            epoch,
                            curr_epoch
                        ));
                    }
                    if epoch == curr_epoch && seq_id.is_none() {
                        return Ok((epoch, seq_id));
                    }
                }
            }

            notified
                .instrument_await("Wait For Barrier Truncation")
                .await;
        }
    }

    pub(crate) fn flush_all_unflushed(
        &mut self,
        mut flush_fn: impl FnMut(&StreamChunk, u64, SeqId, SeqId) -> LogStoreResult<()>,
    ) -> LogStoreResult<()> {
        let mut inner_guard = self.buffer.inner();
        let inner = inner_guard.deref_mut();
        for (epoch, item) in inner
            .unconsumed_queue
            .iter_mut()
            .chain(inner.consumed_queue.iter_mut())
        {
            if let LogStoreBufferItem::StreamChunk {
                chunk,
                start_seq_id,
                end_seq_id,
                flushed,
                ..
            } = item
            {
                if *flushed {
                    // Since we iterate from new data to old data, when we meet a flushed data, the
                    // rest should have been flushed.
                    break;
                }
                flush_fn(chunk, *epoch, *start_seq_id, *end_seq_id)?;
                *flushed = true;
            }
        }
        Ok(())
    }

    pub(crate) fn clear(&mut self) {
        self.buffer.inner().clear();
    }
}

pub(crate) struct LogStoreBufferReceiver {
    buffer: SharedMutex<LogStoreBufferInner>,
    update_notify: Arc<Notify>,
    truncate_notify: Arc<Notify>,
}

impl LogStoreBufferReceiver {
    pub(crate) async fn next_item(&self) -> (u64, LogStoreBufferItem) {
        let notified = self.update_notify.notified();
        if let Some(item) = { self.buffer.inner().pop_item() } {
            item
        } else {
            notified.instrument_await("Wait For New Buffer Item").await;
            self.buffer
                .inner()
                .pop_item()
                .expect("should get the item because notified")
        }
    }

    pub(crate) fn truncate_buffer(&mut self, offset: TruncateOffset) {
        let mut inner = self.buffer.inner();
        let mut latest_offset: Option<ReaderTruncationOffsetType> = None;
        while let Some((epoch, item)) = inner.consumed_queue.back() {
            let epoch = *epoch;
            match item {
                LogStoreBufferItem::StreamChunk {
                    chunk_id,
                    flushed,
                    end_seq_id,
                    chunk,
                    ..
                } => {
                    let chunk_offset = TruncateOffset::Chunk {
                        epoch,
                        chunk_id: *chunk_id,
                    };
                    let flushed = *flushed;
                    let end_seq_id = *end_seq_id;
                    if chunk_offset <= offset {
                        inner.row_count -= chunk.cardinality();
                        inner.pop_consumed_back();
                        if flushed {
                            latest_offset = Some((epoch, Some(end_seq_id)));
                        }
                    } else {
                        break;
                    }
                }
                LogStoreBufferItem::Flushed {
                    chunk_id,
                    end_seq_id,
                    ..
                } => {
                    let end_seq_id = *end_seq_id;
                    let chunk_offset = TruncateOffset::Chunk {
                        epoch,
                        chunk_id: *chunk_id,
                    };
                    if chunk_offset <= offset {
                        inner.pop_consumed_back();
                        latest_offset = Some((epoch, Some(end_seq_id)));
                    } else {
                        break;
                    }
                }
                LogStoreBufferItem::Barrier { .. } => {
                    let chunk_offset = TruncateOffset::Barrier { epoch };
                    if chunk_offset <= offset {
                        inner.pop_consumed_back();
                        latest_offset = Some((epoch, None));
                    } else {
                        break;
                    }
                }
            }
        }
        inner.update_buffer_metrics();
        if let Some(offset) = latest_offset {
            inner.add_truncate_offset(offset);
            self.truncate_notify.notify_waiters();
        }
    }

    pub(crate) fn truncate_historical(&mut self, epoch: u64) {
        let mut inner = self.buffer.inner();
        inner.add_truncate_offset((epoch, None));
        self.truncate_notify.notify_waiters();
    }

    pub(crate) fn rewind(&self, log_store_rewind_start_epoch: Option<u64>) {
        self.buffer.inner().rewind(log_store_rewind_start_epoch)
    }
}

pub(crate) fn new_log_store_buffer(
    max_row_count: usize,
    chunk_size: usize,
    metrics: KvLogStoreMetrics,
) -> (LogStoreBufferSender, LogStoreBufferReceiver) {
    let buffer = SharedMutex::new(LogStoreBufferInner {
        unconsumed_queue: VecDeque::new(),
        consumed_queue: VecDeque::new(),
        row_count: 0,
        max_row_count,
        chunk_size,
        truncation_list: VecDeque::new(),
        next_chunk_id: 0,
        metrics,
        unconsumed_memory_bytes: 0,
        consumed_memory_bytes: 0,
        unconsumed_row_count: 0,
        unconsumed_epoch_count: 0,
    });
    let update_notify = Arc::new(Notify::new());
    let truncate_notify = Arc::new(Notify::new());
    let tx = LogStoreBufferSender {
        buffer: buffer.clone(),
        update_notify: update_notify.clone(),
        truncate_notify: truncate_notify.clone(),
    };

    let rx = LogStoreBufferReceiver {
        buffer,
        update_notify,
        truncate_notify,
    };

    (tx, rx)
}
