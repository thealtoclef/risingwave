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

use std::collections::{BTreeMap, HashSet};

use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};

use crate::error::ConnectorResult;
use crate::source::{SplitId, SplitMetaData};

pub const SPANNER_CDC_STATE_VERSION: u32 = 1;
pub const SPANNER_CDC_ROOT_PARTITION_KEY: &str = "__rw_spanner_root__";
const LEGACY_RESUME_REWIND_SECS: i64 = 3600;
/// Soft cap; exceeding it only warns. Force-evicting referenced entries would
/// break parent-dependency checks.
const MAX_PARTITION_STATES: usize = 10_000;

fn normalize_parent_token(token: String) -> String {
    if token.is_empty() {
        SPANNER_CDC_ROOT_PARTITION_KEY.to_owned()
    } else {
        token
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SpannerPartitionStatus {
    Pending,
    Running,
    Finished,
}

impl Default for SpannerPartitionStatus {
    fn default() -> Self {
        Self::Pending
    }
}

/// Runtime and checkpointed state for one Spanner change stream partition.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Hash)]
pub struct SpannerPartitionState {
    pub token: Option<String>,
    #[serde(default)]
    pub parent_tokens: Vec<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub start_timestamp: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    #[serde(default)]
    pub progress_timestamp: Option<OffsetDateTime>,
    #[serde(default)]
    pub status: SpannerPartitionStatus,
}

impl SpannerPartitionState {
    pub fn new(
        token: Option<String>,
        parent_tokens: Vec<String>,
        start_timestamp: OffsetDateTime,
    ) -> Self {
        let mut parent_tokens = parent_tokens
            .into_iter()
            .map(normalize_parent_token)
            .collect::<Vec<_>>();
        parent_tokens.sort();
        parent_tokens.dedup();
        Self {
            token,
            parent_tokens,
            start_timestamp,
            progress_timestamp: None,
            status: SpannerPartitionStatus::Pending,
        }
    }

    pub fn key_for_token(token: Option<&str>) -> String {
        token
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| SPANNER_CDC_ROOT_PARTITION_KEY.to_owned())
    }

    pub fn key(&self) -> String {
        Self::key_for_token(self.token.as_deref())
    }

    pub fn progress_or_start(&self) -> OffsetDateTime {
        self.progress_timestamp.unwrap_or(self.start_timestamp)
    }

    pub fn advance_progress(&mut self, timestamp: OffsetDateTime) {
        if self
            .progress_timestamp
            .is_none_or(|current| timestamp > current)
        {
            self.progress_timestamp = Some(timestamp);
        }
    }
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
/// **RisingWave CDC Convention:**
/// - `offset: Option<OffsetDateTime>` - single source of truth (timestamp)
/// - `start_offset()` method - converts to String on-demand (matches other CDC sources)
/// - `is_snapshot_done()` - whether backfill is complete (matches other CDC sources)
#[derive(Clone, Debug, Serialize, Deserialize)]
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

    /// Number of messages processed (for monitoring)
    #[serde(default)]
    pub messages_processed: u64,

    /// Versioned aggregate partition state. Empty means legacy state and is
    /// upgraded in memory before reading.
    #[serde(default)]
    pub partition_states: BTreeMap<String, SpannerPartitionState>,

    /// Source-level watermark derived from unfinished partitions. This is not
    /// a Spanner partition-local timestamp.
    #[serde(with = "time::serde::rfc3339::option")]
    #[serde(default)]
    pub global_watermark: Option<OffsetDateTime>,

    #[serde(default)]
    pub state_version: u32,

    /// Watermark indexes (count per `progress_or_start`, bucketed by status).
    /// Rebuilt lazily after deserialization; maintained incrementally on the
    /// hot path so `recompute_global_watermark` is O(log N) instead of O(N).
    #[serde(skip)]
    unfinished_ts_counts: BTreeMap<OffsetDateTime, usize>,

    #[serde(skip)]
    finished_ts_counts: BTreeMap<OffsetDateTime, usize>,

    #[serde(skip)]
    indexes_built: bool,
}

// Hand-impl so cache fields don't perturb equality / hashing.
impl PartialEq for SpannerCdcSplit {
    fn eq(&self, other: &Self) -> bool {
        self.partition_token == other.partition_token
            && self.parent_partition_tokens == other.parent_partition_tokens
            && self.offset == other.offset
            && self.snapshot_done == other.snapshot_done
            && self.change_stream_name == other.change_stream_name
            && self.index == other.index
            && self.messages_processed == other.messages_processed
            && self.partition_states == other.partition_states
            && self.global_watermark == other.global_watermark
            && self.state_version == other.state_version
    }
}

impl std::hash::Hash for SpannerCdcSplit {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.partition_token.hash(state);
        self.parent_partition_tokens.hash(state);
        self.offset.hash(state);
        self.snapshot_done.hash(state);
        self.change_stream_name.hash(state);
        self.index.hash(state);
        self.messages_processed.hash(state);
        self.partition_states.hash(state);
        self.global_watermark.hash(state);
        self.state_version.hash(state);
    }
}

impl SpannerCdcSplit {
    /// Create a new root partition (no parents).
    ///
    /// Offset is REQUIRED - set by enumerator using user-provided timestamp or current time.
    pub fn new_root(
        change_stream_name: String,
        index: u32,
        offset: OffsetDateTime,
    ) -> Self {
        Self {
            partition_token: None,
            parent_partition_tokens: vec![],
            offset: Some(offset),
            snapshot_done: false,
            change_stream_name,
            index,
            messages_processed: 0,
            partition_states: BTreeMap::new(),
            global_watermark: Some(offset),
            state_version: SPANNER_CDC_STATE_VERSION,
            unfinished_ts_counts: BTreeMap::new(),
            finished_ts_counts: BTreeMap::new(),
            indexes_built: true,
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
            messages_processed: 0,
            partition_states: BTreeMap::new(),
            global_watermark: Some(offset),
            state_version: SPANNER_CDC_STATE_VERSION,
            unfinished_ts_counts: BTreeMap::new(),
            finished_ts_counts: BTreeMap::new(),
            indexes_built: true,
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
        self.offset
            .map(|ts| (ts.unix_timestamp_nanos() / 1000).to_string())
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

    pub fn is_aggregate_state(&self) -> bool {
        !self.partition_states.is_empty()
    }

    pub fn initialize_partition_state_if_needed(&mut self) {
        if !self.partition_states.is_empty() {
            if self.state_version == 0 {
                self.state_version = SPANNER_CDC_STATE_VERSION;
            }
            self.recompute_global_watermark();
            return;
        }

        let Some(offset) = self.offset else {
            return;
        };
        let is_legacy = self.state_version == 0;
        let start_timestamp = if is_legacy {
            offset
                .checked_sub(Duration::seconds(LEGACY_RESUME_REWIND_SECS))
                .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        } else {
            offset
        };
        if is_legacy {
            // The metric dips by the rewind window once on upgrade; log so it's explainable.
            tracing::info!(
                change_stream = %self.change_stream_name,
                index = self.index,
                old_offset = %offset,
                new_start_timestamp = %start_timestamp,
                rewind_seconds = LEGACY_RESUME_REWIND_SECS,
                "upgrading legacy Spanner CDC split state"
            );
        }
        let state = SpannerPartitionState::new(
            self.partition_token.clone(),
            self.parent_partition_tokens.clone(),
            start_timestamp,
        );
        self.partition_states.insert(state.key(), state);
        self.global_watermark = Some(start_timestamp);
        self.state_version = SPANNER_CDC_STATE_VERSION;
        self.indexes_built = false;
    }

    /// Prepare checkpointed state for a new reader task. Partitions that were
    /// running before a crash/restart are retried from their last checkpointed
    /// progress.
    pub fn prepare_for_runtime(&mut self) {
        self.initialize_partition_state_if_needed();
        // Pending and Running share the unfinished index bucket with the same ts,
        // so flipping Running→Pending leaves indexes intact.
        for state in self.partition_states.values_mut() {
            if state.status == SpannerPartitionStatus::Running {
                state.status = SpannerPartitionStatus::Pending;
            }
        }
        // Also cleans orphaned `Finished` entries from older binaries.
        self.prune_finished_partitions();
    }

    pub fn discover_partition(
        &mut self,
        token: String,
        mut parent_tokens: Vec<String>,
        start_timestamp: OffsetDateTime,
    ) -> bool {
        if !self.indexes_built {
            self.rebuild_indexes();
        }
        parent_tokens = parent_tokens
            .into_iter()
            .map(normalize_parent_token)
            .collect();
        parent_tokens.sort();
        parent_tokens.dedup();

        let key = SpannerPartitionState::key_for_token(Some(&token));
        let (inserted, ts_changed) = match self.partition_states.get_mut(&key) {
            Some(existing) => {
                if existing.status != SpannerPartitionStatus::Pending {
                    return false;
                }
                let old_ts = existing.progress_or_start();
                for parent in parent_tokens {
                    if !existing.parent_tokens.contains(&parent) {
                        existing.parent_tokens.push(parent);
                    }
                }
                existing.parent_tokens.sort();
                existing.parent_tokens.dedup();
                if start_timestamp < existing.start_timestamp {
                    existing.start_timestamp = start_timestamp;
                }
                let new_ts = existing.progress_or_start();
                if new_ts != old_ts {
                    self.index_remove(SpannerPartitionStatus::Pending, old_ts);
                    self.index_add(SpannerPartitionStatus::Pending, new_ts);
                }
                (false, new_ts != old_ts)
            }
            None => {
                let state =
                    SpannerPartitionState::new(Some(token), parent_tokens, start_timestamp);
                let ts = state.progress_or_start();
                self.partition_states.insert(key, state);
                self.index_add(SpannerPartitionStatus::Pending, ts);
                (true, true)
            }
        };
        if inserted || ts_changed {
            self.recompute_global_watermark();
        }
        inserted
    }

    pub fn mark_partition_running(&mut self, key: &str) {
        if !self.indexes_built {
            self.rebuild_indexes();
        }
        if let Some(state) = self.partition_states.get_mut(key) {
            let old_status = state.status;
            let ts = state.progress_or_start();
            state.status = SpannerPartitionStatus::Running;
            // Pending and Running share the unfinished bucket; only Finished→Running swaps.
            if old_status == SpannerPartitionStatus::Finished {
                self.index_remove(SpannerPartitionStatus::Finished, ts);
                self.index_add(SpannerPartitionStatus::Running, ts);
                self.recompute_global_watermark();
            }
        }
    }

    pub fn mark_partition_finished(&mut self, key: &str, progress: OffsetDateTime) {
        if !self.indexes_built {
            self.rebuild_indexes();
        }
        let snapshot = self
            .partition_states
            .get(key)
            .map(|s| (s.status, s.progress_or_start()));
        let Some((old_status, old_ts)) = snapshot else {
            return;
        };
        let state = self.partition_states.get_mut(key).unwrap();
        state.advance_progress(progress);
        state.status = SpannerPartitionStatus::Finished;
        let new_ts = state.progress_or_start();
        self.index_remove(old_status, old_ts);
        self.index_add(SpannerPartitionStatus::Finished, new_ts);
        self.recompute_global_watermark();
    }

    /// Advance progress for an already-discovered partition. Unknown tokens
    /// are dropped — inserting here would let a stale offset resurrect a
    /// finished or pruned partition.
    pub fn apply_partition_progress(
        &mut self,
        token: Option<String>,
        _parent_tokens: Vec<String>,
        progress: OffsetDateTime,
    ) {
        self.initialize_partition_state_if_needed();
        if !self.indexes_built {
            self.rebuild_indexes();
        }
        let key = SpannerPartitionState::key_for_token(token.as_deref());
        let snapshot = self
            .partition_states
            .get(&key)
            .map(|s| (s.status, s.progress_or_start()));
        let Some((status, old_ts)) = snapshot else {
            tracing::warn!(
                partition_key = %key,
                progress = %progress,
                change_stream = %self.change_stream_name,
                index = self.index,
                "dropping progress for unknown partition"
            );
            return;
        };
        let state = self.partition_states.get_mut(&key).unwrap();
        state.advance_progress(progress);
        let new_ts = state.progress_or_start();
        if new_ts != old_ts {
            self.index_remove(status, old_ts);
            self.index_add(status, new_ts);
            self.recompute_global_watermark();
        }
    }

    pub fn next_runnable_partition(&self) -> Option<SpannerPartitionState> {
        self.partition_states
            .values()
            .filter(|state| state.status == SpannerPartitionStatus::Pending)
            .find(|state| {
                state.parent_tokens.iter().all(|parent| {
                    self.partition_states
                        .get(parent)
                        .is_some_and(|parent_state| {
                            parent_state.status == SpannerPartitionStatus::Finished
                        })
                })
            })
            .cloned()
    }

    pub fn running_partition_count(&self) -> usize {
        self.partition_states
            .values()
            .filter(|state| state.status == SpannerPartitionStatus::Running)
            .count()
    }

    /// Merge the orchestrator's authoritative aggregate into `self`.
    ///
    /// Orchestrator wins on status and discovery; `self` wins on
    /// `progress_timestamp` (which it accumulates from per-row offsets the
    /// orchestrator never sees). A blind `*self = other` would discard that
    /// in-flight progress.
    pub fn merge_from(&mut self, other: SpannerCdcSplit) {
        self.initialize_partition_state_if_needed();

        // Trust the orchestrator's pruning for Finished entries; keep
        // local-only Pending/Running (in-flight worker progress).
        self.partition_states.retain(|key, state| {
            other.partition_states.contains_key(key)
                || state.status != SpannerPartitionStatus::Finished
        });

        for (key, incoming) in other.partition_states {
            match self.partition_states.get_mut(&key) {
                Some(existing) => {
                    existing.status = incoming.status;
                    if incoming.start_timestamp < existing.start_timestamp {
                        existing.start_timestamp = incoming.start_timestamp;
                    }
                    if let Some(incoming_progress) = incoming.progress_timestamp {
                        existing.advance_progress(incoming_progress);
                    }
                    for parent in incoming.parent_tokens {
                        if !existing.parent_tokens.contains(&parent) {
                            existing.parent_tokens.push(parent);
                        }
                    }
                    existing.parent_tokens.sort();
                    existing.parent_tokens.dedup();
                }
                None => {
                    self.partition_states.insert(key, incoming);
                }
            }
        }

        self.change_stream_name = other.change_stream_name;
        self.index = other.index;
        self.state_version = other.state_version.max(self.state_version);
        self.snapshot_done = self.snapshot_done || other.snapshot_done;
        self.messages_processed = self.messages_processed.max(other.messages_processed);
        self.partition_token = other.partition_token;
        self.parent_partition_tokens = other.parent_partition_tokens;

        // Bulk mutation; next recompute rebuilds indexes.
        self.indexes_built = false;
        self.recompute_global_watermark();
    }

    /// Evict `Finished` partitions no longer referenced by any unfinished
    /// entry's `parent_tokens`. Keeps the highest-progress `Finished` entry
    /// as a watermark anchor when nothing unfinished remains.
    pub fn prune_finished_partitions(&mut self) {
        if self.partition_states.is_empty() {
            return;
        }

        let referenced: HashSet<&str> = self
            .partition_states
            .values()
            .filter(|state| state.status != SpannerPartitionStatus::Finished)
            .flat_map(|state| state.parent_tokens.iter().map(String::as_str))
            .collect();

        let all_finished = referenced.is_empty()
            && self
                .partition_states
                .values()
                .all(|state| state.status == SpannerPartitionStatus::Finished);

        let anchor_key: Option<String> = if all_finished {
            self.partition_states
                .iter()
                .max_by_key(|(_, state)| state.progress_or_start())
                .map(|(key, _)| key.clone())
        } else {
            None
        };

        let to_evict: Vec<String> = self
            .partition_states
            .iter()
            .filter(|(key, state)| {
                state.status == SpannerPartitionStatus::Finished
                    && !referenced.contains(key.as_str())
                    && Some(key.as_str()) != anchor_key.as_deref()
            })
            .map(|(key, _)| key.clone())
            .collect();

        if !to_evict.is_empty() {
            let evicted = to_evict.len();
            for key in &to_evict {
                self.partition_states.remove(key);
            }
            tracing::debug!(
                change_stream = %self.change_stream_name,
                index = self.index,
                evicted,
                remaining = self.partition_states.len(),
                "pruned finished Spanner CDC partition states"
            );
            self.indexes_built = false;
        }

        if self.partition_states.len() > MAX_PARTITION_STATES {
            tracing::warn!(
                change_stream = %self.change_stream_name,
                index = self.index,
                partition_state_count = self.partition_states.len(),
                cap = MAX_PARTITION_STATES,
                "Spanner CDC partition_states exceeded soft cap"
            );
        }

        self.recompute_global_watermark();
    }

    fn rebuild_indexes(&mut self) {
        self.unfinished_ts_counts.clear();
        self.finished_ts_counts.clear();
        for state in self.partition_states.values() {
            let ts = state.progress_or_start();
            let map = if state.status == SpannerPartitionStatus::Finished {
                &mut self.finished_ts_counts
            } else {
                &mut self.unfinished_ts_counts
            };
            *map.entry(ts).or_insert(0) += 1;
        }
        self.indexes_built = true;
    }

    fn index_add(&mut self, status: SpannerPartitionStatus, ts: OffsetDateTime) {
        let map = if status == SpannerPartitionStatus::Finished {
            &mut self.finished_ts_counts
        } else {
            &mut self.unfinished_ts_counts
        };
        *map.entry(ts).or_insert(0) += 1;
    }

    fn index_remove(&mut self, status: SpannerPartitionStatus, ts: OffsetDateTime) {
        let map = if status == SpannerPartitionStatus::Finished {
            &mut self.finished_ts_counts
        } else {
            &mut self.unfinished_ts_counts
        };
        if let Some(count) = map.get_mut(&ts) {
            *count -= 1;
            if *count == 0 {
                map.remove(&ts);
            }
        }
    }

    pub fn recompute_global_watermark(&mut self) {
        if !self.indexes_built {
            self.rebuild_indexes();
        }
        let watermark = self
            .unfinished_ts_counts
            .keys()
            .next()
            .copied()
            .or_else(|| self.finished_ts_counts.keys().next_back().copied());
        self.global_watermark = watermark;
        if let Some(w) = watermark {
            self.offset = Some(w);
        }
    }

    pub fn state_timestamp_micros(&self) -> i64 {
        self.global_watermark
            .or(self.offset)
            .map(|ts| (ts.unix_timestamp_nanos() / 1000) as i64)
            .unwrap_or(0)
    }

    pub fn encode_progress_offset(&self) -> String {
        serde_json::to_string(self)
            .unwrap_or_else(|_| self.state_timestamp_micros().to_string())
    }

    /// Get the current offset as microseconds since epoch (for offset persistence).
    ///
    /// Returns 0 if offset is not yet set.
    pub fn offset_as_micros(&self) -> i64 {
        self.offset
            .map(|ts| (ts.unix_timestamp_nanos() / 1000) as i64)
            .unwrap_or(0)
    }

    /// Update this split's offset from a SpannerOffset.
    ///
    /// This is called during checkpoint recovery to restore the resume position.
    pub fn update_from_offset(&mut self, offset: &crate::source::cdc::external::spanner::SpannerOffset) {
        // Update offset from the SpannerOffset (stored as microseconds, convert to OffsetDateTime)
        if let Ok(offset_ts) = time::OffsetDateTime::from_unix_timestamp_nanos((offset.timestamp as i128) * 1000) {
            self.apply_partition_progress(
                offset.partition_token.clone(),
                offset.parent_partition_tokens.clone(),
                offset_ts,
            );
        } else {
            tracing::error!(timestamp = offset.timestamp, "failed to parse offset from microseconds");
        }

        // If the offset indicates this partition is finished, mark snapshot as done
        if offset.is_finished {
            self.snapshot_done = true;
        }
    }

}

impl SplitMetaData for SpannerCdcSplit {
    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone())
            .expect("SpannerCdcSplit serialization to JSON should never fail")
            .into()
    }

    fn id(&self) -> SplitId {
        // Return numeric ID (index) to satisfy CDC framework's requirement for parseable u64
        // The partition token and stream name are still tracked internally in the split struct
        self.index.to_string().into()
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        // Full-aggregate from orchestrator: merge (preserves in-flight per-row progress).
        if let Ok(mut split) = serde_json::from_str::<SpannerCdcSplit>(&last_seen_offset) {
            split.initialize_partition_state_if_needed();
            self.merge_from(split);
            return Ok(());
        }

        // Plain microseconds timestamp (legacy framework convention).
        if let Ok(timestamp_micros) = last_seen_offset.trim().parse::<i64>() {
            if timestamp_micros > 0 {
                if let Ok(offset_ts) =
                    time::OffsetDateTime::from_unix_timestamp_nanos((timestamp_micros as i128) * 1000)
                {
                    if self.partition_states.is_empty() {
                        self.offset = Some(offset_ts);
                        self.global_watermark = Some(offset_ts);
                    } else {
                        // Post-upgrade: route to root so the watermark advances.
                        tracing::warn!(
                            timestamp_micros,
                            change_stream = %self.change_stream_name,
                            index = self.index,
                            "legacy plain-timestamp offset on upgraded split"
                        );
                        self.apply_partition_progress(None, vec![], offset_ts);
                    }
                    return Ok(());
                }
            }
        }

        // Per-row CdcOffset::Spanner: merge progress for one partition.
        let cdc_offset: crate::source::cdc::external::CdcOffset = serde_json::from_str(&last_seen_offset)?;
        match cdc_offset {
            crate::source::cdc::external::CdcOffset::Spanner(spanner_offset) => {
                self.update_from_offset(&spanner_offset);
            }
            _ => {
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
        let split = SpannerCdcSplit::new_root("test_stream".to_string(), 0, offset);

        assert!(split.is_root());
        assert!(!split.has_parents());
        assert_eq!(split.offset, Some(offset));
        assert_eq!(split.change_stream_name, "test_stream");
    }

    #[test]
    fn test_child_partition() {
        let start = OffsetDateTime::now_utc();
        let parents = vec!["parent1".to_string(), "parent2".to_string()];
        let split = SpannerCdcSplit::new_child(
            "child1".to_string(),
            parents.clone(),
            start,
            "test_stream".to_string(),
            1,
        );

        assert!(!split.is_root());
        assert!(split.has_parents());
        assert_eq!(split.parent_partition_tokens, parents);
        assert_eq!(split.change_stream_name, "test_stream");
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
    fn test_fresh_split_initializes_without_legacy_rewind() {
        let offset = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, offset);

        split.prepare_for_runtime();

        let root = split
            .partition_states
            .get(SPANNER_CDC_ROOT_PARTITION_KEY)
            .unwrap();
        assert_eq!(root.start_timestamp, offset);
        assert_eq!(split.global_watermark, Some(offset));
    }

    #[test]
    fn test_legacy_state_initializes_pending_root_with_rewind() {
        let offset = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let legacy = SpannerCdcSplit::new_root("test".to_string(), 42, offset);
        let mut legacy_json = serde_json::to_value(legacy).unwrap();
        let legacy_obj = legacy_json.as_object_mut().unwrap();
        legacy_obj.remove("partition_states");
        legacy_obj.remove("global_watermark");
        legacy_obj.remove("state_version");

        let mut split: SpannerCdcSplit = serde_json::from_value(legacy_json).unwrap();
        split.prepare_for_runtime();

        assert_eq!(split.index, 42);
        assert_eq!(split.partition_states.len(), 1);
        let root = split
            .partition_states
            .get(SPANNER_CDC_ROOT_PARTITION_KEY)
            .unwrap();
        assert_eq!(root.status, SpannerPartitionStatus::Pending);
        assert_eq!(
            root.start_timestamp,
            offset - Duration::seconds(LEGACY_RESUME_REWIND_SECS)
        );
        assert_eq!(split.global_watermark, Some(root.start_timestamp));
    }

    #[test]
    fn test_running_partitions_retry_after_restart() {
        let offset = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, offset);
        split.initialize_partition_state_if_needed();
        split.mark_partition_running(SPANNER_CDC_ROOT_PARTITION_KEY);

        split.prepare_for_runtime();

        let root = split
            .partition_states
            .get(SPANNER_CDC_ROOT_PARTITION_KEY)
            .unwrap();
        assert_eq!(root.status, SpannerPartitionStatus::Pending);
    }

    #[test]
    fn test_new_state_roundtrip_preserves_partition_states() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        split.initialize_partition_state_if_needed();
        split.discover_partition("child".to_string(), vec![], base + Duration::seconds(1));
        split.mark_partition_running("child");

        let json = split.encode_to_json();
        let restored = SpannerCdcSplit::restore_from_json(json).unwrap();

        assert_eq!(restored.partition_states.len(), 2);
        assert_eq!(
            restored.partition_states.get("child").unwrap().status,
            SpannerPartitionStatus::Running
        );
        assert_eq!(restored.global_watermark, split.global_watermark);
    }

    #[test]
    fn test_cdc_offset_updates_only_matching_partition_progress() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        split.discover_partition("a".to_string(), vec![], base);
        split.discover_partition("b".to_string(), vec![], base);

        let progress = base + Duration::seconds(30);
        let offset = crate::source::cdc::external::spanner::SpannerOffset::with_partition(
            (progress.unix_timestamp_nanos() / 1000) as i64,
            Some("a".to_string()),
            vec![],
            (progress.unix_timestamp_nanos() / 1000) as i64,
            "test".to_string(),
            42,
        );
        let cdc_offset = crate::source::cdc::external::CdcOffset::Spanner(offset);
        split
            .update_offset(serde_json::to_string(&cdc_offset).unwrap())
            .unwrap();

        assert_eq!(
            split.partition_states.get("a").unwrap().progress_timestamp,
            Some(progress)
        );
        assert_eq!(
            split.partition_states.get("b").unwrap().progress_timestamp,
            None
        );
    }

    #[test]
    fn test_duplicate_child_is_scheduled_once_after_all_parents_finish() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        split.discover_partition("child_token_2".to_string(), vec![], base);
        split.discover_partition("child_token_3".to_string(), vec![], base);
        split.mark_partition_finished("child_token_2", base + Duration::seconds(10));

        assert!(split.discover_partition(
            "child_token_4".to_string(),
            vec!["child_token_2".to_string(), "child_token_3".to_string()],
            base + Duration::seconds(20),
        ));
        assert!(!split.discover_partition(
            "child_token_4".to_string(),
            vec!["child_token_2".to_string(), "child_token_3".to_string()],
            base + Duration::seconds(20),
        ));

        assert_ne!(
            split.next_runnable_partition().map(|state| state.token),
            Some(Some("child_token_4".to_string()))
        );

        split.mark_partition_finished("child_token_3", base + Duration::seconds(11));
        assert_eq!(
            split.next_runnable_partition().and_then(|state| state.token),
            Some("child_token_4".to_string())
        );
        split.mark_partition_running("child_token_4");
        assert_ne!(
            split.next_runnable_partition().and_then(|state| state.token),
            Some("child_token_4".to_string())
        );
    }

    #[test]
    fn test_deduplication_is_scoped_to_one_source_split() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut source_a = SpannerCdcSplit::new_root("stream_a".to_string(), 1, base);
        let mut source_b = SpannerCdcSplit::new_root("stream_b".to_string(), 2, base);

        assert!(source_a.discover_partition("same_token".to_string(), vec![], base));
        assert!(source_b.discover_partition("same_token".to_string(), vec![], base));

        assert_eq!(source_a.partition_states.len(), 1);
        assert_eq!(source_b.partition_states.len(), 1);
        assert_ne!(source_a.index, source_b.index);
    }

    #[test]
    fn test_merge_preserves_in_flight_progress_on_running_partition() {
        // Orchestrator has no progress for the Running partition; per-row path
        // on local has advanced it. Merge must not regress.
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut local = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        local.discover_partition("child".to_string(), vec![], base);
        local.mark_partition_running("child");
        local.apply_partition_progress(
            Some("child".to_string()),
            vec![],
            base + Duration::seconds(120),
        );

        let mut orchestrator = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        orchestrator.discover_partition("child".to_string(), vec![], base);
        orchestrator.mark_partition_running("child");
        let aggregate_json = orchestrator.encode_progress_offset();

        local.update_offset(aggregate_json).unwrap();

        let child = local.partition_states.get("child").unwrap();
        assert_eq!(child.status, SpannerPartitionStatus::Running);
        assert_eq!(child.progress_timestamp, Some(base + Duration::seconds(120)));
        assert_eq!(local.global_watermark, Some(base + Duration::seconds(120)));
    }

    #[test]
    fn test_merge_adopts_authoritative_status_transitions() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut local = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        local.discover_partition("child".to_string(), vec![], base);
        local.mark_partition_running("child");

        let mut orchestrator = local.clone();
        orchestrator.mark_partition_finished("child", base + Duration::seconds(60));

        local
            .update_offset(orchestrator.encode_progress_offset())
            .unwrap();

        let child = local.partition_states.get("child").unwrap();
        assert_eq!(child.status, SpannerPartitionStatus::Finished);
        assert_eq!(
            child.progress_timestamp,
            Some(base + Duration::seconds(60))
        );
    }

    #[test]
    fn test_apply_partition_progress_does_not_resurrect_unknown_token() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        split.initialize_partition_state_if_needed();

        let before = split.partition_states.len();
        split.apply_partition_progress(
            Some("never_discovered".to_string()),
            vec![],
            base + Duration::seconds(10),
        );

        assert_eq!(split.partition_states.len(), before);
        assert!(split.partition_states.get("never_discovered").is_none());
    }

    #[test]
    fn test_plain_timestamp_post_upgrade_advances_root_partition() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        split.initialize_partition_state_if_needed();
        assert!(!split.partition_states.is_empty());

        let later = base + Duration::seconds(75);
        let later_micros = (later.unix_timestamp_nanos() / 1000) as i64;
        split.update_offset(later_micros.to_string()).unwrap();

        let root = split
            .partition_states
            .get(SPANNER_CDC_ROOT_PARTITION_KEY)
            .unwrap();
        assert_eq!(root.progress_timestamp, Some(later));
    }

    #[test]
    fn test_plain_timestamp_pre_upgrade_path_unchanged() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit {
            partition_token: None,
            parent_partition_tokens: vec![],
            offset: Some(base),
            snapshot_done: false,
            change_stream_name: "test".to_string(),
            index: 1,
            messages_processed: 0,
            partition_states: BTreeMap::new(),
            global_watermark: Some(base),
            state_version: SPANNER_CDC_STATE_VERSION,
            unfinished_ts_counts: BTreeMap::new(),
            finished_ts_counts: BTreeMap::new(),
            indexes_built: true,
        };

        let later = base + Duration::seconds(5);
        let later_micros = (later.unix_timestamp_nanos() / 1000) as i64;
        split.update_offset(later_micros.to_string()).unwrap();

        assert_eq!(split.offset, Some(later));
        assert_eq!(split.global_watermark, Some(later));
        assert!(split.partition_states.is_empty());
    }

    #[test]
    fn test_prune_keeps_finished_parent_while_child_pending() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        split.initialize_partition_state_if_needed();
        split.discover_partition("a".to_string(), vec![], base);
        split.discover_partition(
            "b".to_string(),
            vec!["a".to_string()],
            base + Duration::seconds(10),
        );
        split.mark_partition_finished("a", base + Duration::seconds(5));

        split.prune_finished_partitions();

        // `a` is Finished but still referenced by Pending `b`.
        assert!(split.partition_states.contains_key("a"));
        assert!(split.partition_states.contains_key("b"));
    }

    #[test]
    fn test_prune_evicts_finished_chain_once_unreferenced() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        split.initialize_partition_state_if_needed();
        split.discover_partition("a".to_string(), vec![], base);
        split.discover_partition(
            "b".to_string(),
            vec!["a".to_string()],
            base + Duration::seconds(10),
        );
        split.discover_partition(
            "c".to_string(),
            vec!["b".to_string()],
            base + Duration::seconds(20),
        );
        split.mark_partition_finished("a", base + Duration::seconds(5));
        split.mark_partition_finished("b", base + Duration::seconds(15));

        split.prune_finished_partitions();

        // Only Pending/Running entries' parent_tokens anchor; `b` is Finished
        // so its reference to `a` doesn't keep `a` alive.
        assert!(!split.partition_states.contains_key("a"));
        assert!(split.partition_states.contains_key("b"));
        assert!(split.partition_states.contains_key("c"));
    }

    #[test]
    fn test_prune_retains_one_anchor_when_all_finished() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        split.initialize_partition_state_if_needed();
        split.discover_partition("a".to_string(), vec![], base);
        split.discover_partition(
            "b".to_string(),
            vec!["a".to_string()],
            base + Duration::seconds(10),
        );
        split.mark_partition_finished(SPANNER_CDC_ROOT_PARTITION_KEY, base);
        split.mark_partition_finished("a", base + Duration::seconds(5));
        split.mark_partition_finished("b", base + Duration::seconds(15));

        split.prune_finished_partitions();

        // One anchor remains (highest progress) so the watermark fallback holds.
        assert_eq!(split.partition_states.len(), 1);
        let (_, anchor) = split.partition_states.iter().next().unwrap();
        assert_eq!(anchor.progress_timestamp, Some(base + Duration::seconds(15)));
        assert_eq!(split.global_watermark, Some(base + Duration::seconds(15)));
    }

    #[test]
    fn test_prune_is_noop_in_steady_state() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        split.initialize_partition_state_if_needed();
        split.discover_partition("a".to_string(), vec![], base);
        split.mark_partition_running("a");

        let before: Vec<String> = split.partition_states.keys().cloned().collect();
        split.prune_finished_partitions();
        let after: Vec<String> = split.partition_states.keys().cloned().collect();

        assert_eq!(before, after);
    }

    #[test]
    fn test_merge_drops_local_finished_entries_pruned_by_orchestrator() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);

        let mut local = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        local.initialize_partition_state_if_needed();
        local.discover_partition("a".to_string(), vec![], base);
        local.discover_partition(
            "b".to_string(),
            vec!["a".to_string()],
            base + Duration::seconds(10),
        );
        local.mark_partition_finished("a", base + Duration::seconds(5));
        local.mark_partition_finished("b", base + Duration::seconds(15));
        local.discover_partition(
            "c".to_string(),
            vec!["b".to_string()],
            base + Duration::seconds(20),
        );
        local.mark_partition_running("c");

        let mut orchestrator = local.clone();
        orchestrator.prune_finished_partitions();
        assert!(!orchestrator.partition_states.contains_key("a"));

        // Local should adopt the orchestrator's pruned view.
        local
            .update_offset(orchestrator.encode_progress_offset())
            .unwrap();

        assert!(!local.partition_states.contains_key("a"));
        assert!(local.partition_states.contains_key("b"));
        assert!(local.partition_states.contains_key("c"));
    }

    #[test]
    fn test_merge_keeps_local_only_running_entries() {
        // Running entries absent from `other` are in-flight worker progress
        // the orchestrator hasn't observed yet — merge must keep them.
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut local = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        local.initialize_partition_state_if_needed();
        let orchestrator = local.clone();

        local.discover_partition("child".to_string(), vec![], base);
        local.mark_partition_running("child");
        local.apply_partition_progress(
            Some("child".to_string()),
            vec![],
            base + Duration::seconds(30),
        );

        local
            .update_offset(orchestrator.encode_progress_offset())
            .unwrap();

        assert!(local.partition_states.contains_key("child"));
        assert_eq!(
            local.partition_states.get("child").unwrap().status,
            SpannerPartitionStatus::Running
        );
    }

    #[test]
    fn test_prepare_for_runtime_preserves_finished_partitions() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        split.initialize_partition_state_if_needed();
        split.discover_partition("a".to_string(), vec![], base);
        let progress = base + Duration::seconds(50);
        split.mark_partition_finished("a", progress);
        split.discover_partition(
            "b".to_string(),
            vec!["a".to_string()],
            base + Duration::seconds(10),
        );
        split.mark_partition_running("b");

        split.prepare_for_runtime();

        let a = split.partition_states.get("a").unwrap();
        assert_eq!(a.status, SpannerPartitionStatus::Finished);
        assert_eq!(a.progress_timestamp, Some(progress));
        // Running → Pending on restart.
        let b = split.partition_states.get("b").unwrap();
        assert_eq!(b.status, SpannerPartitionStatus::Pending);
    }

    #[test]
    fn test_discover_partition_dedup_on_running_and_finished() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        split.initialize_partition_state_if_needed();
        split.discover_partition("a".to_string(), vec![], base);
        split.mark_partition_running("a");

        // Re-discovery of a Running partition is a no-op.
        assert!(!split.discover_partition("a".to_string(), vec![], base));
        assert_eq!(
            split.partition_states.get("a").unwrap().status,
            SpannerPartitionStatus::Running
        );

        split.mark_partition_finished("a", base + Duration::seconds(10));

        // Re-discovery of a Finished partition is also a no-op.
        assert!(!split.discover_partition("a".to_string(), vec![], base));
        assert_eq!(
            split.partition_states.get("a").unwrap().status,
            SpannerPartitionStatus::Finished
        );
    }

    #[test]
    fn test_watermark_monotonic_after_legacy_rewind() {
        let offset = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let legacy = SpannerCdcSplit::new_root("test".to_string(), 42, offset);
        let mut legacy_json = serde_json::to_value(legacy).unwrap();
        let obj = legacy_json.as_object_mut().unwrap();
        obj.remove("partition_states");
        obj.remove("global_watermark");
        obj.remove("state_version");

        let mut split: SpannerCdcSplit = serde_json::from_value(legacy_json).unwrap();
        split.prepare_for_runtime();

        let rewound = offset - Duration::seconds(LEGACY_RESUME_REWIND_SECS);
        assert_eq!(split.global_watermark, Some(rewound));

        // Advance progress past the original (pre-rewind) offset.
        let advanced = offset + Duration::seconds(30);
        split.apply_partition_progress(None, vec![], advanced);
        assert_eq!(split.global_watermark, Some(advanced));

        // A stale earlier offset must not regress the watermark.
        split.apply_partition_progress(None, vec![], offset);
        assert_eq!(split.global_watermark, Some(advanced));
    }

    #[test]
    fn test_incremental_indexes_match_full_rebuild() {
        // Exercise every mutation path, then assert the incremental indexes
        // produce the same watermark and bucket counts as a fresh full rebuild.
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        split.initialize_partition_state_if_needed();

        split.discover_partition("a".to_string(), vec![], base + Duration::seconds(10));
        split.discover_partition(
            "b".to_string(),
            vec!["a".to_string()],
            base + Duration::seconds(20),
        );
        split.mark_partition_running("a");
        split.apply_partition_progress(
            Some("a".to_string()),
            vec![],
            base + Duration::seconds(15),
        );
        split.mark_partition_finished("a", base + Duration::seconds(30));
        split.discover_partition(
            "c".to_string(),
            vec!["b".to_string()],
            base + Duration::seconds(40),
        );
        split.apply_partition_progress(
            Some("b".to_string()),
            vec!["a".to_string()],
            base + Duration::seconds(25),
        );
        // Lower-progress update must not regress.
        split.apply_partition_progress(
            Some("b".to_string()),
            vec!["a".to_string()],
            base + Duration::seconds(5),
        );
        // Re-discovery of a Running/Finished partition must not touch indexes.
        assert!(!split.discover_partition("a".to_string(), vec![], base));

        let incremental_watermark = split.global_watermark;
        let incremental_unfinished = split.unfinished_ts_counts.clone();
        let incremental_finished = split.finished_ts_counts.clone();

        // Force a full rebuild from scratch.
        split.unfinished_ts_counts.clear();
        split.finished_ts_counts.clear();
        split.indexes_built = false;
        split.recompute_global_watermark();

        assert_eq!(split.global_watermark, incremental_watermark);
        assert_eq!(split.unfinished_ts_counts, incremental_unfinished);
        assert_eq!(split.finished_ts_counts, incremental_finished);
    }

    #[test]
    fn test_global_watermark_uses_unfinished_minimum() {
        let base = OffsetDateTime::UNIX_EPOCH + Duration::hours(1);
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 42, base);
        split.discover_partition("a".to_string(), vec![], base + Duration::seconds(100));
        split.discover_partition("b".to_string(), vec![], base + Duration::seconds(10));
        split.apply_partition_progress(
            Some("a".to_string()),
            vec![],
            base + Duration::seconds(200),
        );
        split.apply_partition_progress(
            Some("b".to_string()),
            vec![],
            base + Duration::seconds(20),
        );

        assert_eq!(split.global_watermark, Some(base + Duration::seconds(20)));

        split.mark_partition_finished("b", base + Duration::seconds(30));
        assert_eq!(split.global_watermark, Some(base + Duration::seconds(200)));
    }
}
