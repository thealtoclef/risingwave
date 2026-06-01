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
use crate::source::cdc::external::spanner::PartitionOffset;
use crate::source::{SplitId, SplitMetaData};

/// Lifecycle state of a Spanner change stream partition.
///
/// Used by the reader for local coordination, not by the executor's split.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionState {
    /// Discovered via `ChildPartitionsRecord` but waiting for all parents to finish.
    Pending,
    /// Actively reading from this partition.
    Running,
    /// Result set exhausted — all data consumed.
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
/// **Offset semantics:**
/// The reader's `PartitionCoordinator` emits records eagerly and tracks a scalar
/// **checkpoint watermark** — the monotonic min over non-finished partition
/// offsets. Every SourceMessage carries this as its scalar offset, so the
/// executor's split receives a single checkpoint-compatible offset like other CDC
/// sources. It is **not** the resume point.
///
/// Resume is driven by a per-partition **frontier** carried in every message's
/// `SpannerOffset.partitions` and captured here in [`SpannerCdcSplit::partitions`].
/// A restart resumes each partition from its own offset (Beam `PartitionMetadata`
/// model) rather than re-reading the whole tree from a single offset.
///
/// **RisingWave CDC Convention:**
/// - `start_offset()` method - converts to String on-demand (matches other CDC sources)
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

    /// CDC offset - scalar checkpoint watermark for change stream queries.
    ///
    /// On the executor's split: the coordinator's scalar checkpoint watermark.
    /// Always advances monotonically; resume uses `partitions`, not this scalar.
    ///
    /// Type is `OffsetDateTime` (not String like Debezium) for type safety and efficiency.
    /// Spanner CDC uses timestamps directly, not complex Debezium offset structures.
    #[serde(with = "time::serde::rfc3339::option")]
    #[serde(default)]
    pub offset: Option<OffsetDateTime>,

    /// Change stream name
    pub change_stream_name: String,

    /// Split index for identification
    pub index: u32,

    /// Per-partition resume frontier — every non-finished partition with its own
    /// offset, parents and running flag, captured from the latest stamped
    /// `SpannerOffset.partitions`. Empty for a freshly enumerated split or a split
    /// restored from a legacy (pre-frontier) offset, in which case the reader
    /// falls back to resuming a single root partition from `offset`.
    #[serde(default)]
    pub partitions: Vec<PartitionOffset>,
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
            change_stream_name,
            index,
            partitions: vec![],
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
            change_stream_name,
            index,
            partitions: vec![],
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

    /// Returns the offset as microseconds since epoch.
    ///
    /// On the executor's split this is the coordinator's scalar checkpoint watermark.
    /// Used by `start_offset()` and the source executor metrics.
    pub fn offset_as_micros(&self) -> i64 {
        self.offset
            .map(|ts| (ts.unix_timestamp_nanos() / 1000) as i64)
            .unwrap_or(0)
    }

    /// Update offset from a [`SpannerOffset`].
    ///
    /// Advances the scalar checkpoint watermark and, when the offset carries a
    /// per-partition frontier, **replaces** [`SpannerCdcSplit::partitions`] with
    /// it. Replace (not merge) is correct: each stamped frontier is the complete
    /// set of non-finished partitions at a barrier-consistent point and advances
    /// monotonically, so finished partitions correctly drop out. The replace is
    /// guarded by scalar monotonicity — a frontier whose timestamp would regress
    /// the scalar offset is ignored.
    pub fn update_from_offset(
        &mut self,
        offset: &crate::source::cdc::external::spanner::SpannerOffset,
    ) {
        if let Ok(offset_ts) =
            OffsetDateTime::from_unix_timestamp_nanos((offset.timestamp as i128) * 1000)
        {
            // Determine regression against the current scalar before advancing.
            let regresses = self.offset.is_some_and(|cur| offset_ts < cur);
            self.advance_offset(offset_ts);
            if !regresses {
                match &offset.partitions {
                    Some(parts) => self.partitions = parts.clone(),
                    None => self.partitions.clear(),
                }
            }
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
        // Parse as plain timestamp (microseconds as string) -- the framework convention.
        // This is what current_cdc_offset() returns: timestamp as microseconds string.
        //
        // Note: this path is only used by `InjectSourceOffsets`
        // (ALTER SOURCE ... SET OFFSET), which is explicitly marked unsafe because
        // it can cause data duplication or loss. Normal CDC messages always go
        // through the CdcOffset JSON path below.
        if let Ok(timestamp_micros) = last_seen_offset.trim().parse::<i64>() {
            if timestamp_micros > 0 {
                if let Ok(offset_ts) =
                    OffsetDateTime::from_unix_timestamp_nanos((timestamp_micros as i128) * 1000)
                {
                    self.advance_offset(offset_ts);
                    // A manual SET OFFSET override resets resume to a single root
                    // partition from the given timestamp — drop any stale frontier.
                    self.partitions.clear();
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
    fn test_offset_monotonic_advance() {
        let offset = OffsetDateTime::UNIX_EPOCH;
        let mut split = SpannerCdcSplit::new_root("test".to_string(), 0, offset);

        assert_eq!(split.offset, Some(offset));

        let later = offset + time::Duration::seconds(100);
        split.advance_offset(later);
        assert_eq!(split.offset, Some(later));

        // Should not go backwards
        let earlier = offset + time::Duration::seconds(50);
        split.advance_offset(earlier);
        assert_eq!(split.offset, Some(later));
    }

    #[test]
    fn test_update_from_offset_watermark() {
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let t100 = t0 + time::Duration::seconds(100);
        let t200 = t0 + time::Duration::seconds(200);

        let mut split = SpannerCdcSplit::new_root("test".into(), 0, t0);

        // Watermark advances offset.
        let offset1 = crate::source::cdc::external::spanner::SpannerOffset::new(
            (t100.unix_timestamp_nanos() / 1000) as i64,
        );
        split.update_from_offset(&offset1);
        assert_eq!(split.offset, Some(t100));

        // Second watermark advances further (monotonic).
        let offset2 = crate::source::cdc::external::spanner::SpannerOffset::new(
            (t200.unix_timestamp_nanos() / 1000) as i64,
        );
        split.update_from_offset(&offset2);
        assert_eq!(split.offset, Some(t200));
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let t100 = t0 + time::Duration::seconds(100);

        let mut split = SpannerCdcSplit::new_root("test_stream".into(), 42, t0);
        split.advance_offset(t100);

        let json = split.encode_to_json();
        let restored = SpannerCdcSplit::restore_from_json(json).unwrap();

        assert_eq!(restored.offset, Some(t100));
        assert_eq!(restored.index, 42);
        assert_eq!(restored.change_stream_name, "test_stream");
    }

    #[test]
    fn test_update_offset_plain_int_path() {
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let t200 = t0 + time::Duration::seconds(200);

        let mut split = SpannerCdcSplit::new_root("test".into(), 0, t0);
        // Seed a stale frontier; the plain-int override must clear it.
        split.partitions = vec![PartitionOffset {
            token: Some("stale".into()),
            parent_tokens: vec![],
            micros: 1,
            running: true,
        }];

        split
            .update_offset((t200.unix_timestamp_nanos() / 1000).to_string())
            .unwrap();
        assert_eq!(split.offset, Some(t200));
        assert!(
            split.partitions.is_empty(),
            "SET OFFSET clears the frontier"
        );
    }

    fn poff(token: &str, micros: i64, running: bool) -> PartitionOffset {
        PartitionOffset {
            token: Some(token.into()),
            parent_tokens: vec![],
            micros,
            running,
        }
    }

    #[test]
    fn test_update_from_offset_populates_frontier() {
        use crate::source::cdc::external::spanner::SpannerOffset;
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let mut split = SpannerCdcSplit::new_root("test".into(), 0, t0);

        let t100 = t0 + time::Duration::seconds(100);
        let micros = (t100.unix_timestamp_nanos() / 1000) as i64;
        let offset = SpannerOffset::new(micros)
            .with_partitions(vec![poff("A", micros, true), poff("B", micros, false)]);
        split.update_from_offset(&offset);

        assert_eq!(split.offset, Some(t100));
        assert_eq!(split.partitions.len(), 2);
    }

    #[test]
    fn test_update_from_offset_replaces_frontier() {
        use crate::source::cdc::external::spanner::SpannerOffset;
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let mut split = SpannerCdcSplit::new_root("test".into(), 0, t0);

        let m1 = ((t0 + time::Duration::seconds(100)).unix_timestamp_nanos() / 1000) as i64;
        split.update_from_offset(
            &SpannerOffset::new(m1).with_partitions(vec![poff("A", m1, true), poff("B", m1, true)]),
        );
        assert_eq!(split.partitions.len(), 2);

        // Newer frontier with B finished (dropped out) and only A remaining.
        let m2 = ((t0 + time::Duration::seconds(200)).unix_timestamp_nanos() / 1000) as i64;
        split
            .update_from_offset(&SpannerOffset::new(m2).with_partitions(vec![poff("A", m2, true)]));
        assert_eq!(
            split.partitions.len(),
            1,
            "replace, not merge — B drops out"
        );
        assert_eq!(split.partitions[0].token.as_deref(), Some("A"));
    }

    #[test]
    fn test_update_from_offset_without_frontier_clears_stale_frontier() {
        use crate::source::cdc::external::spanner::SpannerOffset;
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let mut split = SpannerCdcSplit::new_root("test".into(), 0, t0);

        let m1 = ((t0 + time::Duration::seconds(100)).unix_timestamp_nanos() / 1000) as i64;
        split
            .update_from_offset(&SpannerOffset::new(m1).with_partitions(vec![poff("A", m1, true)]));
        assert_eq!(split.partitions.len(), 1);

        let m2 = ((t0 + time::Duration::seconds(200)).unix_timestamp_nanos() / 1000) as i64;
        split.update_from_offset(&SpannerOffset::new(m2));
        assert!(split.partitions.is_empty());
    }

    #[test]
    fn test_update_from_offset_monotonic_guard() {
        use crate::source::cdc::external::spanner::SpannerOffset;
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let mut split = SpannerCdcSplit::new_root("test".into(), 0, t0);

        let m200 = ((t0 + time::Duration::seconds(200)).unix_timestamp_nanos() / 1000) as i64;
        split.update_from_offset(
            &SpannerOffset::new(m200).with_partitions(vec![poff("A", m200, true)]),
        );
        assert_eq!(split.partitions.len(), 1);

        // An older frontier must be ignored (scalar would regress).
        let m100 = ((t0 + time::Duration::seconds(100)).unix_timestamp_nanos() / 1000) as i64;
        split.update_from_offset(
            &SpannerOffset::new(m100)
                .with_partitions(vec![poff("OLD", m100, true), poff("X", m100, true)]),
        );
        assert_eq!(split.partitions.len(), 1, "older frontier ignored");
        assert_eq!(split.partitions[0].token.as_deref(), Some("A"));
    }

    #[test]
    fn test_encode_decode_roundtrip_with_frontier() {
        let t0 = OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000).unwrap();
        let mut split = SpannerCdcSplit::new_root("test_stream".into(), 7, t0);
        split.partitions = vec![poff("A", 1, true), poff("B", 2, false)];

        let json = split.encode_to_json();
        let restored = SpannerCdcSplit::restore_from_json(json).unwrap();
        assert_eq!(restored.partitions.len(), 2);
        assert_eq!(restored, split);
    }

    #[test]
    fn test_restore_legacy_split_without_frontier() {
        // A split persisted before the frontier existed must restore with an
        // empty `partitions` (serde default), so the reader falls back to root mode.
        let legacy = r#"{
            "partition_token": null,
            "parent_partition_tokens": [],
            "offset": "2024-01-15T10:30:45Z",
            "change_stream_name": "s",
            "index": 0
        }"#;
        let value: serde_json::Value = serde_json::from_str(legacy).unwrap();
        let split = SpannerCdcSplit::restore_from_json(JsonbVal::from(value)).unwrap();
        assert!(split.partitions.is_empty());
        assert!(split.offset.is_some());
    }
}
