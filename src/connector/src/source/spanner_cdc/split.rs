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

use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::error::ConnectorResult;
use crate::source::{SplitId, SplitMetaData};

/// Partition state following the Spanner change stream state machine
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub enum PartitionState {
    /// Partition discovered but not yet scheduled
    #[default]
    Created,
    /// Partition queued to be processed
    Scheduled,
    /// Partition currently being queried
    Running,
    /// Partition fully processed
    Finished,
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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Hash)]
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

    /// Stream name
    pub stream_name: String,

    /// Split index for identification
    pub index: u32,

    /// Current state of this partition
    #[serde(default)]
    pub state: PartitionState,

    /// Number of messages processed (for monitoring)
    #[serde(default)]
    pub messages_processed: u64,
}

impl SpannerCdcSplit {
    /// Create a new root partition (no parents).
    ///
    /// Offset is REQUIRED - set by enumerator using user-provided timestamp or current time.
    pub fn new_root(
        stream_name: String,
        index: u32,
        offset: OffsetDateTime,
    ) -> Self {
        Self {
            partition_token: None,
            parent_partition_tokens: vec![],
            offset: Some(offset),
            snapshot_done: false,
            stream_name,
            index,
            state: PartitionState::Created,
            messages_processed: 0,
        }
    }

    /// Create a new child partition with parent tracking.
    pub fn new_child(
        token: String,
        parent_tokens: Vec<String>,
        offset: OffsetDateTime,
        stream_name: String,
        index: u32,
    ) -> Self {
        Self {
            partition_token: Some(token),
            parent_partition_tokens: parent_tokens,
            offset: Some(offset),
            snapshot_done: false,
            stream_name,
            index,
            state: PartitionState::Created,
            messages_processed: 0,
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

    /// Returns true if this partition is finished
    pub fn is_finished(&self) -> bool {
        self.state == PartitionState::Finished
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

    /// Mark this partition as running
    pub fn mark_running(&mut self) {
        self.state = PartitionState::Running;
    }

    /// Mark this partition as finished
    pub fn mark_finished(&mut self) {
        self.state = PartitionState::Finished;
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

    /// Get the current offset as microseconds since epoch (for offset persistence).
    ///
    /// Returns 0 if offset is not yet set.
    pub fn offset_as_micros(&self) -> i64 {
        self.offset
            .map(|ts| (ts.unix_timestamp_nanos() / 1000) as i64)
            .unwrap_or(0)
    }

    /// Create a SpannerOffset from this split's current state (for checkpointing)
    pub fn to_spanner_offset(&self) -> crate::source::cdc::external::spanner::SpannerOffset {
        let mut spanner_offset = crate::source::cdc::external::spanner::SpannerOffset::with_partition(
            self.offset_as_micros(),
            self.partition_token.clone(),
            self.parent_partition_tokens.clone(),
            self.offset_as_micros(),
            self.stream_name.clone(),
            self.index,
        );
        if self.is_finished() {
            spanner_offset.mark_finished();
        }
        spanner_offset
    }

    /// Update this split's offset from a SpannerOffset.
    ///
    /// This is called during checkpoint recovery to restore the resume position.
    pub fn update_from_offset(&mut self, offset: &crate::source::cdc::external::spanner::SpannerOffset) {
        // Update offset from the SpannerOffset (stored as microseconds, convert to OffsetDateTime)
        if let Ok(offset_ts) = time::OffsetDateTime::from_unix_timestamp_nanos((offset.timestamp as i128) * 1000) {
            self.offset = Some(offset_ts);
        } else {
            tracing::error!(timestamp = offset.timestamp, "failed to parse offset from microseconds");
        }

        // If the offset indicates this partition is finished, mark it as such
        if offset.is_finished {
            self.state = PartitionState::Finished;
        }
    }

    /// Set the offset from backfill snapshot timestamp.
    ///
    /// This is called after backfill completes to set the initial position for CDC.
    pub fn set_offset_from_backfill(&mut self, snapshot_timestamp_micros: i64) {
        if let Ok(offset_ts) = time::OffsetDateTime::from_unix_timestamp_nanos((snapshot_timestamp_micros as i128) * 1000) {
            self.offset = Some(offset_ts);
            // Mark snapshot as done when setting offset from backfill
            self.snapshot_done = true;
        }
    }

    /// Check if this partition can be scheduled (parents have finished)
    pub fn can_schedule(&self, parents_finished: impl Fn(&str) -> bool) -> bool {
        if self.state != PartitionState::Created {
            return false;
        }

        // Check if all parents have finished
        for parent_token in &self.parent_partition_tokens {
            if !parents_finished(parent_token) {
                return false;
            }
        }

        true
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
        // Parse as plain timestamp (microseconds as string) -- the framework convention.
        // This is what current_cdc_offset() returns: timestamp as microseconds string.
        if let Ok(timestamp_micros) = last_seen_offset.trim().parse::<i64>() {
            if timestamp_micros > 0 {
                if let Ok(offset_ts) = time::OffsetDateTime::from_unix_timestamp_nanos((timestamp_micros as i128) * 1000) {
                    self.offset = Some(offset_ts);
                    return Ok(());
                }
            }
        }

        // Fall back to parsing as CdcOffset enum format: {"Spanner": {...}}
        // This is used for CDC checkpoint updates with full partition state
        let cdc_offset: crate::source::cdc::external::CdcOffset = serde_json::from_str(&last_seen_offset)?;
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
        let split = SpannerCdcSplit::new_root("test_stream".to_string(), 0, offset);

        assert!(split.is_root());
        assert!(!split.has_parents());
        assert_eq!(split.state, PartitionState::Created);
        assert_eq!(split.offset, Some(offset));
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
        assert_eq!(split.state, PartitionState::Created);
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
    fn test_can_schedule() {
        let start = OffsetDateTime::now_utc();
        let mut split = SpannerCdcSplit::new_child(
            "child".to_string(),
            vec!["parent1".to_string(), "parent2".to_string()],
            start,
            "test".to_string(),
            1,
        );

        // Not ready if parents not finished
        assert!(!split.can_schedule(|_| false));

        // Not ready if only one parent finished
        assert!(!split.can_schedule(|p| p == "parent1"));

        // Ready when all parents finished
        assert!(split.can_schedule(|_| true));

        // Not ready if already running
        split.mark_running();
        assert!(!split.can_schedule(|_| true));
    }
}
