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
/// - Watermark tracking ensures timestamp-ordered processing
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

    /// Start timestamp for reading this partition
    #[serde(with = "time::serde::rfc3339")]
    pub start_timestamp: OffsetDateTime,

    /// Watermark: highest timestamp processed by this partition
    ///
    /// Used for coordinating child partition scheduling and ensuring
    /// timestamp-ordered processing across partitions.
    #[serde(with = "time::serde::rfc3339")]
    #[serde(default = "default_watermark")]
    pub watermark: OffsetDateTime,

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

fn default_watermark() -> OffsetDateTime {
    // Default to Unix epoch if not set
    time::OffsetDateTime::UNIX_EPOCH
}

impl SpannerCdcSplit {
    /// Create a new root partition (no parents)
    pub fn new_root(
        start_timestamp: OffsetDateTime,
        stream_name: String,
        index: u32,
    ) -> Self {
        Self {
            partition_token: None,
            parent_partition_tokens: vec![],
            start_timestamp,
            watermark: start_timestamp,
            stream_name,
            index,
            state: PartitionState::Created,
            messages_processed: 0,
        }
    }

    /// Create a new child partition with parent tracking
    pub fn new_child(
        token: String,
        parent_tokens: Vec<String>,
        start_timestamp: OffsetDateTime,
        stream_name: String,
        index: u32,
    ) -> Self {
        Self {
            partition_token: Some(token),
            parent_partition_tokens: parent_tokens,
            start_timestamp,
            watermark: start_timestamp,
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

    /// Update the watermark to a higher timestamp
    pub fn update_watermark(&mut self, new_watermark: OffsetDateTime) {
        if new_watermark > self.watermark {
            self.watermark = new_watermark;
        }
    }

    /// Mark this partition as running
    pub fn mark_running(&mut self) {
        self.state = PartitionState::Running;
    }

    /// Mark this partition as finished
    pub fn mark_finished(&mut self) {
        self.state = PartitionState::Finished;
    }

    /// Get the current watermark as microseconds since epoch (for offset persistence)
    pub fn watermark_as_micros(&self) -> i64 {
        (self.watermark.unix_timestamp_nanos() / 1000) as i64
    }

    /// Get the start timestamp as microseconds since epoch
    pub fn start_timestamp_as_micros(&self) -> i64 {
        (self.start_timestamp.unix_timestamp_nanos() / 1000) as i64
    }

    /// Create a SpannerOffset from this split's current state (for checkpointing)
    pub fn to_spanner_offset(&self) -> crate::source::cdc::external::spanner::SpannerOffset {
        let mut offset = crate::source::cdc::external::spanner::SpannerOffset::with_partition(
            self.watermark_as_micros(),
            self.partition_token.clone(),
            self.parent_partition_tokens.clone(),
            self.watermark_as_micros(),
            self.stream_name.clone(),
            self.index,
        );
        if self.is_finished() {
            offset.mark_finished();
        }
        offset
    }

    /// Update this split's watermark from a SpannerOffset (for state restoration)
    pub fn update_from_offset(&mut self, offset: &crate::source::cdc::external::spanner::SpannerOffset) {
        // Update watermark from the offset
        // Watermark is stored as microseconds, convert to nanoseconds for OffsetDateTime
        if let Ok(watermark_ts) = time::OffsetDateTime::from_unix_timestamp_nanos((offset.watermark as i128) * 1000) {
            self.watermark = watermark_ts;
        }
        // If the offset indicates this partition is finished, mark it as such
        if offset.is_finished {
            self.state = PartitionState::Finished;
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
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn id(&self) -> SplitId {
        match &self.partition_token {
            Some(token) => format!("{}-{}-{}", self.stream_name, self.index, token).into(),
            None => format!("{}-{}-root", self.stream_name, self.index).into(),
        }
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        // Parse the offset as JSON to extract the SpannerOffset state
        // The offset string contains the serialized SpannerOffset for checkpointing
        if let Ok(spanner_offset) = serde_json::from_str::<crate::source::cdc::external::spanner::SpannerOffset>(&last_seen_offset) {
            self.update_from_offset(&spanner_offset);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_partition() {
        let start = OffsetDateTime::now_utc();
        let split = SpannerCdcSplit::new_root(start, "test_stream".to_string(), 0);

        assert!(split.is_root());
        assert!(!split.has_parents());
        assert_eq!(split.state, PartitionState::Created);
        assert_eq!(split.watermark, start);
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
    fn test_watermark_update() {
        let start = OffsetDateTime::UNIX_EPOCH;
        let mut split = SpannerCdcSplit::new_root(start, "test".to_string(), 0);

        assert_eq!(split.watermark, start);

        let later = start + time::Duration::seconds(100);
        split.update_watermark(later);
        assert_eq!(split.watermark, later);

        // Should not go backwards
        let earlier = start + time::Duration::seconds(50);
        split.update_watermark(earlier);
        assert_eq!(split.watermark, later);
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
