// Copyright 2022 RisingWave Labs
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

//! Spanner CDC split definitions for partition tracking and coordination.

use std::collections::HashMap;

use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::ConnectorResult;
use crate::source::{SplitId, SplitMetaData};

/// Lifecycle state of a Spanner change stream partition.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionState {
    /// Discovered via `ChildPartitionsRecord` but waiting for all parents to finish.
    Pending,
    /// Actively reading from this partition.
    Running,
    /// Result set exhausted — all data consumed.
    Finished,
}

/// Per-partition progress entry for `SpannerCdcSplit::partition_progress`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PerPartitionProgress {
    #[serde(with = "time::serde::rfc3339::option")]
    #[serde(default)]
    pub offset: Option<OffsetDateTime>,

    #[serde(default)]
    pub parent_tokens: Vec<String>,

    pub state: PartitionState,
}

/// Spanner CDC split representing a partition in the change stream.
///
/// Based on Spanner change stream documentation:
/// https://docs.cloud.google.com/spanner/docs/change-streams/details
///
/// Key concepts:
/// - Partitions can split into child partitions
/// - Child partitions must wait for ALL parents to finish
/// - Offset tracking ensures timestamp-ordered processing
///
/// **Offset semantics:**
/// - `offset_as_micros()` returns this partition's own offset as microseconds.
/// - `lagging_edge_micros()` returns `min(root, running children)` — the checkpoint-safe
///   position meaning "everything before this timestamp has been consumed."
/// - `encode_to_json` snapshots the lagging edge into `offset` and clears
///   `partition_progress`/`root_finished` (runtime-only). On recovery the reader
///   starts a fresh root from that offset and Spanner re-discovers children naturally.
///
/// **RisingWave CDC Convention:**
/// - `start_offset()` method - converts to String on-demand (matches other CDC sources)
/// - `is_snapshot_done()` - whether backfill is complete (matches other CDC sources)
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SpannerCdcSplit {
    /// Partition token (None for root partition)
    pub partition_token: Option<String>,

    /// Parent partition tokens (empty for root partition)
    ///
    /// According to Spanner docs:
    /// "To make sure changes for a key is processed in timestamp order,
    /// wait until the records returned from all parents have been processed."
    #[serde(default)]
    pub parent_partition_tokens: Vec<String>,

    /// CDC offset - timestamp-based position for change stream queries
    ///
    /// Following RisingWave CDC convention:
    /// - Set by enumerator (user-provided timestamp or current time)
    /// - Used as `@start_timestamp` parameter for change stream queries
    /// - Child partitions wait for parent offsets before starting (timestamp ordering)
    ///
    /// On the executor's split: root partition offset only.
    /// On partition-task splits: that single partition's progress.
    /// After `encode_to_json`: the lagging-edge offset.
    ///
    /// Type is `OffsetDateTime` (not String like Debezium) for type safety and efficiency.
    /// Spanner CDC uses timestamps directly, not complex Debezium offset structures.
    #[serde(with = "time::serde::rfc3339::option")]
    #[serde(default)]
    pub offset: Option<OffsetDateTime>,

    /// Whether snapshot/backfill is done (RisingWave CDC convention)
    ///
    /// - false: Backfill in progress or not started
    /// - true: Backfill complete, CDC streaming active
    #[serde(default)]
    pub snapshot_done: bool,

    /// Change stream name
    pub change_stream_name: String,

    /// Split index for identification
    pub index: u32,

    /// Whether the root partition has exhausted its result set. Runtime-only —
    /// cleared by `encode_to_json`.
    #[serde(default)]
    pub root_finished: bool,

    /// Per-child-partition progress keyed by partition token. Runtime-only —
    /// cleared by `encode_to_json`. Used for dedup and lagging-edge computation.
    #[serde(default)]
    pub partition_progress: HashMap<String, PerPartitionProgress>,
}

impl SpannerCdcSplit {
    /// Create a new root partition (no parents).
    ///
    /// Offset is REQUIRED - set by enumerator using user-provided timestamp or current time.
    pub fn new_root(change_stream_name: String, index: u32, offset: OffsetDateTime) -> Self {
        Self {
            partition_token: None,
            parent_partition_tokens: vec![],
            offset: Some(offset),
            snapshot_done: false,
            change_stream_name,
            index,
            root_finished: false,
            partition_progress: HashMap::new(),
        }
    }

    /// Create a new child partition with parent tracking.
    pub fn new_child(
        token: String,
        parent_tokens: Vec<String>,
        offset: OffsetDateTime,
        change_stream_name: String,
        index: u32,
    ) -> Self {
        Self {
            partition_token: Some(token),
            parent_partition_tokens: parent_tokens,
            offset: Some(offset),
            snapshot_done: false,
            change_stream_name,
            index,
            root_finished: false,
            partition_progress: HashMap::new(),
        }
    }

    /// Returns true if this is a root partition (no parents)
    pub fn is_root(&self) -> bool {
        self.partition_token.is_none() && self.parent_partition_tokens.is_empty()
    }

    /// Returns true if this partition has parents that must finish before it can start
    pub fn has_parents(&self) -> bool {
        !self.parent_partition_tokens.is_empty()
    }

    /// Get start_offset (RisingWave CDC convention)
    ///
    /// This returns the offset as a String, matching the interface used by other CDC sources.
    /// The offset is the timestamp in microseconds since epoch.
    pub fn start_offset(&self) -> Option<String> {
        let micros = self.offset_as_micros();
        if micros > 0 {
            Some(micros.to_string())
        } else {
            None
        }
    }

    /// Check if snapshot/backfill is done (RisingWave CDC convention)
    pub fn is_snapshot_done(&self) -> bool {
        self.snapshot_done
    }

    /// Mark snapshot as done (called after backfill completes)
    pub fn mark_snapshot_done(&mut self) {
        self.snapshot_done = true;
    }

    /// Advance the offset to a higher timestamp (monotonically increasing)
    pub fn advance_offset(&mut self, new_offset: OffsetDateTime) {
        match &self.offset {
            Some(current) if new_offset > *current => {
                self.offset = Some(new_offset);
            }
            None => {
                self.offset = Some(new_offset);
            }
            _ => {} // new_offset is not higher, don't update
        }
    }

    /// Returns this partition's own offset as microseconds since epoch.
    ///
    /// Simple getter — no cross-partition logic. Used in `start_offset()` and
    /// `make_offset_string()` where each partition reports its own progress.
    pub fn offset_as_micros(&self) -> i64 {
        self.offset
            .map(|ts| (ts.unix_timestamp_nanos() / 1000) as i64)
            .unwrap_or(0)
    }

    /// Returns the lagging-edge offset: `min(root, running children)`.
    /// Means "everything before this timestamp has been consumed."
    ///
    /// Only `Running` children are considered in the lagging edge.
    /// `Pending` children (discovered but not yet started) have `offset = discovery_time`,
    /// which represents the _start_ of the query window for that child, not data consumption.
    /// Including them would artificially hold back the lagging edge.
    /// Falls back to `self.offset` when no running children exist or none tracked.
    ///
    /// Used for checkpointing (`encode_to_json`) and metrics.
    pub fn lagging_edge_micros(&self) -> i64 {
        let mut min_offset: Option<OffsetDateTime> = None;

        if !self.root_finished {
            min_offset = self.offset;
        }

        for progress in self.partition_progress.values() {
            if progress.state == PartitionState::Running
                && let Some(off) = progress.offset
            {
                match min_offset {
                    Some(current) if off < current => min_offset = Some(off),
                    None => min_offset = Some(off),
                    _ => {}
                }
            }
        }

        match min_offset {
            Some(ts) => (ts.unix_timestamp_nanos() / 1000) as i64,
            None => self
                .offset
                .map(|ts| (ts.unix_timestamp_nanos() / 1000) as i64)
                .unwrap_or(0),
        }
    }

    /// Update offset and per-partition progress from a [`SpannerOffset`].
    ///
    /// Child partition messages (those with a token) update only the HashMap entry.
    /// Root partition messages advance `self.offset`.
    pub fn update_from_offset(
        &mut self,
        offset: &crate::source::cdc::external::spanner::SpannerOffset,
    ) {
        if let Some(ref token) = offset.partition_token {
            // Child partition — update HashMap only.
            let entry = self
                .partition_progress
                .entry(token.clone())
                .or_insert_with(|| PerPartitionProgress {
                    offset: None,
                    parent_tokens: offset.parent_partition_tokens.clone(),
                    state: PartitionState::Pending,
                });

            if let Ok(offset_ts) =
                OffsetDateTime::from_unix_timestamp_nanos((offset.offset as i128) * 1000)
            {
                match &entry.offset {
                    Some(current) if offset_ts > *current => {
                        entry.offset = Some(offset_ts);
                    }
                    None => {
                        entry.offset = Some(offset_ts);
                    }
                    _ => {}
                }
            }

            entry.state = if offset.is_finished {
                PartitionState::Finished
            } else {
                PartitionState::Running
            };
        } else {
            // Root partition — advance self.offset.
            if let Ok(offset_ts) =
                OffsetDateTime::from_unix_timestamp_nanos((offset.timestamp as i128) * 1000)
            {
                self.advance_offset(offset_ts);
            } else {
                tracing::error!(timestamp = offset.timestamp, "failed to parse offset");
            }

            if offset.is_finished {
                self.root_finished = true;
                self.snapshot_done = true;
            }
        }
    }

    /// Set the offset from backfill snapshot timestamp.
    ///
    /// This is called after backfill completes to set the initial position for CDC.
    pub fn set_offset_from_backfill(&mut self, snapshot_timestamp_micros: i64) {
        if let Ok(offset_ts) =
            OffsetDateTime::from_unix_timestamp_nanos((snapshot_timestamp_micros as i128) * 1000)
        {
            self.offset = Some(offset_ts);
            // Mark snapshot as done when setting offset from backfill
            self.snapshot_done = true;
        }
    }
}

impl SplitMetaData for SpannerCdcSplit {
    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn encode_to_json(&self) -> JsonbVal {
        // Snapshot the lagging edge into `offset`, clear runtime-only state.
        // On recovery the reader starts a fresh root from this offset.
        let lagging_edge = self.lagging_edge_micros();
        let mut persisted = self.clone();
        if lagging_edge > 0
            && let Ok(ts) = OffsetDateTime::from_unix_timestamp_nanos((lagging_edge as i128) * 1000)
        {
            persisted.offset = Some(ts);
        }
        persisted.partition_progress.clear();
        persisted.root_finished = false;
        serde_json::to_value(persisted)
            .expect("SpannerCdcSplit serialization to JSON should never fail")
            .into()
    }

    fn id(&self) -> SplitId {
        // Return numeric ID (index) to satisfy CDC framework's requirement for parseable u64
        // The partition token and stream name are still tracked internally in the split struct
        self.index.to_string().into()
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        // Parse as plain timestamp (microseconds as string) -- the framework convention.
        // This is what current_cdc_offset() returns: timestamp as microseconds string.
        //
        // Note: this path advances `self.offset` (root) unconditionally, bypassing
        // per-partition tracking. It is only used by `InjectSourceOffsets`
        // (ALTER SOURCE ... SET OFFSET), which is explicitly marked unsafe because
        // it can cause data duplication or loss. Normal CDC messages always go
        // through the CdcOffset JSON path below.
        if let Ok(timestamp_micros) = last_seen_offset.trim().parse::<i64>() {
            if timestamp_micros > 0 {
                if let Ok(offset_ts) =
                    OffsetDateTime::from_unix_timestamp_nanos((timestamp_micros as i128) * 1000)
                {
                    self.advance_offset(offset_ts);
                    return Ok(());
                }
            }
        }

        // Fall back to parsing as CdcOffset enum format: {"Spanner": {...}}
        // This is used for CDC checkpoint updates with full partition state
        let cdc_offset: crate::source::cdc::external::CdcOffset =
            serde_json::from_str(&last_seen_offset)?;
        match cdc_offset {
            crate::source::cdc::external::CdcOffset::Spanner(spanner_offset) => {
                self.update_from_offset(&spanner_offset);
            }
            _ => {
                // Should not happen for Spanner CDC
                tracing::warn!("Received non-Spanner CdcOffset: {:?}", cdc_offset);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_partition() {
        let offset = OffsetDateTime::now_utc();
        let split = SpannerCdcSplit::new_root("test_stream".into(), 0, offset);

        assert!(split.is_root());
        assert!(!split.has_parents());
        assert_eq!(split.offset, Some(offset));
        assert!(split.partition_progress.is_empty());
        assert!(!split.root_finished);
    }

    #[test]
    fn test_child_partition() {
        let start = OffsetDateTime::now_utc();
        let parents = vec!["parent1".into(), "parent2".into()];
        let split = SpannerCdcSplit::new_child(
            "child1".into(),
            parents.clone(),
            start,
            "test_stream".into(),
            1,
        );

        assert!(!split.is_root());
        assert!(split.has_parents());
        assert_eq!(split.parent_partition_tokens, parents);
    }

    #[test]
    fn test_offset_update() {
        let offset = OffsetDateTime::UNIX_EPOCH;
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 0, offset);

        // Offset is set at creation
        assert_eq!(split.offset, Some(offset));

        // Update to a later timestamp
        let later = offset + time::Duration::seconds(100);
        split.advance_offset(later);
        assert_eq!(split.offset, Some(later));

        // Should not go backwards
        let earlier = offset + time::Duration::seconds(50);
        split.advance_offset(earlier);
        assert_eq!(split.offset, Some(later));
    }

    #[test]
    fn test_lagging_edge_min() {
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let t50 = t0 + time::Duration::seconds(50);
        let t100 = t0 + time::Duration::seconds(100);
        let t200 = t0 + time::Duration::seconds(200);

        let mut split = SpannerCdcSplit::new_root("test".into(), 0, t0);
        split.advance_offset(t200);

        split.partition_progress.insert(
            "fast".into(),
            PerPartitionProgress {
                offset: Some(t100),
                parent_tokens: vec![],
                state: PartitionState::Running,
            },
        );
        split.partition_progress.insert(
            "slow".into(),
            PerPartitionProgress {
                offset: Some(t50),
                parent_tokens: vec![],
                state: PartitionState::Running,
            },
        );

        assert_eq!(
            split.lagging_edge_micros(),
            (t50.unix_timestamp_nanos() / 1000) as i64
        );

        split.partition_progress.get_mut("slow").unwrap().state = PartitionState::Finished;
        assert_eq!(
            split.lagging_edge_micros(),
            (t100.unix_timestamp_nanos() / 1000) as i64
        );

        split.partition_progress.get_mut("fast").unwrap().state = PartitionState::Finished;
        assert_eq!(
            split.lagging_edge_micros(),
            (t200.unix_timestamp_nanos() / 1000) as i64
        );

        // All finished — falls back to root's last offset.
        split.root_finished = true;
        assert_eq!(
            split.lagging_edge_micros(),
            (t200.unix_timestamp_nanos() / 1000) as i64
        );
    }

    #[test]
    fn test_pending_children_do_not_affect_lagging_edge() {
        // Pending children have offset = discovery_time, which represents the start of
        // the query window, NOT actual data consumption. They should be excluded from
        // the lagging edge calculation to avoid artificially holding back progress.
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let t100 = t0 + time::Duration::seconds(100);
        let t200 = t0 + time::Duration::seconds(200);

        let mut split = SpannerCdcSplit::new_root("test".into(), 0, t0);
        split.advance_offset(t200);

        // Child is Pending with discovery_time (t0) — should NOT affect lagging edge.
        split.partition_progress.insert(
            "pending_child".into(),
            PerPartitionProgress {
                offset: Some(t0),
                parent_tokens: vec![],
                state: PartitionState::Pending,
            },
        );

        // Lagging edge should be root's offset (t200), not the Pending child's discovery time.
        assert_eq!(
            split.lagging_edge_micros(),
            (t200.unix_timestamp_nanos() / 1000) as i64
        );

        // When child becomes Running with actual data at t100, lagging edge should be t100.
        split
            .partition_progress
            .get_mut("pending_child")
            .unwrap()
            .state = PartitionState::Running;
        split
            .partition_progress
            .get_mut("pending_child")
            .unwrap()
            .offset = Some(t100);
        assert_eq!(
            split.lagging_edge_micros(),
            (t100.unix_timestamp_nanos() / 1000) as i64
        );
    }

    #[test]
    fn test_update_from_offset_child_updates_hashmap_only() {
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let t100 = t0 + time::Duration::seconds(100);
        let mut split = SpannerCdcSplit::new_root("test".into(), 0, t0);

        let child_offset = crate::source::cdc::external::spanner::SpannerOffset::with_partition(
            (t100.unix_timestamp_nanos() / 1000) as i64,
            Some("child1".into()),
            vec!["parent1".into()],
            (t100.unix_timestamp_nanos() / 1000) as i64,
            "test".into(),
            0,
        );
        split.update_from_offset(&child_offset);

        assert_eq!(split.offset, Some(t0));
        assert_eq!(
            split.partition_progress["child1"].state,
            PartitionState::Running
        );
    }

    #[test]
    fn test_update_from_offset_root_advances_self_offset() {
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let t100 = t0 + time::Duration::seconds(100);
        let mut split = SpannerCdcSplit::new_root("test".into(), 0, t0);

        let root_offset = crate::source::cdc::external::spanner::SpannerOffset::new(
            (t100.unix_timestamp_nanos() / 1000) as i64,
        );
        split.update_from_offset(&root_offset);

        assert_eq!(split.offset, Some(t100));
        assert!(!split.root_finished);
    }

    #[test]
    fn test_finished_marks_state() {
        let t100_micros = 1_100_000_000_i64;
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let mut split = SpannerCdcSplit::new_root("test".into(), 0, t0);

        // Child finished — no snapshot_done.
        let mut offset = crate::source::cdc::external::spanner::SpannerOffset::with_partition(
            t100_micros,
            Some("child1".into()),
            vec!["parent1".into()],
            t100_micros,
            "test".into(),
            0,
        );
        offset.mark_finished();
        split.update_from_offset(&offset);
        assert_eq!(
            split.partition_progress["child1"].state,
            PartitionState::Finished
        );
        assert!(!split.snapshot_done);

        // Root finished — sets snapshot_done.
        let mut root_fin = crate::source::cdc::external::spanner::SpannerOffset::new(t100_micros);
        root_fin.mark_finished();
        split.update_from_offset(&root_fin);
        assert!(split.root_finished);
        assert!(split.snapshot_done);
    }

    #[test]
    fn test_partition_progress_not_persisted() {
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let t50 = t0 + time::Duration::seconds(50);
        let t100 = t0 + time::Duration::seconds(100);
        let t200 = t0 + time::Duration::seconds(200);

        let mut split = SpannerCdcSplit::new_root("test_stream".into(), 42, t0);
        split.advance_offset(t200);

        split.partition_progress.insert(
            "child1".into(),
            PerPartitionProgress {
                offset: Some(t50),
                parent_tokens: vec!["p1".into(), "p2".into()],
                state: PartitionState::Running,
            },
        );
        split.partition_progress.insert(
            "p1".into(),
            PerPartitionProgress {
                offset: Some(t100),
                parent_tokens: vec![],
                state: PartitionState::Finished,
            },
        );
        split.root_finished = true;

        // Lagging edge = min(root=200, child1=50) = 50 (p1 Finished, excluded).
        assert_eq!(
            split.lagging_edge_micros(),
            (t50.unix_timestamp_nanos() / 1000) as i64
        );

        let json = split.encode_to_json();
        let restored = SpannerCdcSplit::restore_from_json(json).unwrap();

        assert!(restored.partition_progress.is_empty());
        assert!(!restored.root_finished);
        // offset = lagging edge (50), not root's (200).
        assert_eq!(restored.offset, Some(t50));
    }

    #[test]
    fn test_snapshot_done_root_finishes_while_children_running() {
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let t50 = t0 + time::Duration::seconds(50);
        let t200 = t0 + time::Duration::seconds(200);

        let mut split = SpannerCdcSplit::new_root("test".into(), 0, t0);
        split.advance_offset(t200);

        // Root finishes.
        let mut root_fin = crate::source::cdc::external::spanner::SpannerOffset::new(
            (t200.unix_timestamp_nanos() / 1000) as i64,
        );
        root_fin.mark_finished();
        split.update_from_offset(&root_fin);
        assert!(split.root_finished);
        assert!(split.snapshot_done);

        // Child still running.
        split.partition_progress.insert(
            "child_slow".into(),
            PerPartitionProgress {
                offset: Some(t50),
                parent_tokens: vec![],
                state: PartitionState::Running,
            },
        );

        // Lagging edge anchored to active child at t50.
        assert_eq!(
            split.lagging_edge_micros(),
            (t50.unix_timestamp_nanos() / 1000) as i64
        );

        let json = split.encode_to_json();
        let restored = SpannerCdcSplit::restore_from_json(json).unwrap();
        assert!(restored.snapshot_done);
        assert_eq!(restored.offset, Some(t50));
        assert!(restored.partition_progress.is_empty());
        assert!(!restored.root_finished);
    }

    #[test]
    fn test_update_offset_plain_int_path() {
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let t50 = t0 + time::Duration::seconds(50);
        let t100 = t0 + time::Duration::seconds(100);
        let t200 = t0 + time::Duration::seconds(200);

        let mut split = SpannerCdcSplit::new_root("test".into(), 0, t0);
        split.advance_offset(t100);

        // Child still running at t50.
        split.partition_progress.insert(
            "child_slow".into(),
            PerPartitionProgress {
                offset: Some(t50),
                parent_tokens: vec![],
                state: PartitionState::Running,
            },
        );

        // Lagging edge = min(root=100, child=50) = 50.
        assert_eq!(
            split.lagging_edge_micros(),
            (t50.unix_timestamp_nanos() / 1000) as i64
        );

        // InjectSourceOffsets: plain integer advances root unconditionally.
        split
            .update_offset((t200.unix_timestamp_nanos() / 1000).to_string())
            .unwrap();
        assert_eq!(split.offset, Some(t200));

        // Lagging edge is still protected: min(root=200, child=50) = 50.
        assert_eq!(
            split.lagging_edge_micros(),
            (t50.unix_timestamp_nanos() / 1000) as i64
        );
    }
}
