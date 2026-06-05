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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SpannerCdcSplit {
    /// Partition token (empty string for root partition)
    pub partition_token: String,

    /// Parent partition tokens (empty for root partition)
    ///
    /// According to Spanner docs:
    /// "To make sure changes for a key is processed in timestamp order,
    /// wait until the records returned from all parents have been processed."
    #[serde(default)]
    pub parent_partition_tokens: Vec<String>,

    /// Per-partition CDC offset. Advances monotonically as records are processed.
    /// The *watermark* (safe recovery point) is computed as `min(offset)` across
    /// all un-finished partitions by `PartitionOffsets`.
    #[serde(with = "time::serde::rfc3339::option")]
    #[serde(default)]
    pub offset: Option<OffsetDateTime>,

    /// Change stream name
    pub change_stream_name: String,

    /// Split index for identification
    pub index: u32,
}

impl SpannerCdcSplit {
    /// Create a new root partition (no parents).
    pub fn new_root(change_stream_name: String, index: u32, offset: OffsetDateTime) -> Self {
        Self {
            partition_token: String::new(),
            parent_partition_tokens: vec![],
            offset: Some(offset),
            change_stream_name,
            index,
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
            partition_token: token,
            parent_partition_tokens: parent_tokens,
            offset: Some(offset),
            change_stream_name,
            index,
        }
    }

    /// Returns true if this is a root partition (no parents)
    pub fn is_root(&self) -> bool {
        self.partition_token.is_empty() && self.parent_partition_tokens.is_empty()
    }

    /// Returns true if this partition has parents that must finish before it can start
    pub fn has_parents(&self) -> bool {
        !self.parent_partition_tokens.is_empty()
    }

    /// Get start_offset (RisingWave CDC convention)
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
    pub fn offset_as_micros(&self) -> i64 {
        self.offset
            .map(|ts| (ts.unix_timestamp_nanos() / 1000) as i64)
            .unwrap_or(0)
    }

    /// Update this split's offset from a SpannerOffset.
    pub fn update_from_offset(
        &mut self,
        offset: &crate::source::cdc::external::spanner::SpannerOffset,
    ) {
        if let Ok(offset_ts) =
            OffsetDateTime::from_unix_timestamp_nanos((offset.timestamp as i128) * 1000)
        {
            self.advance_offset(offset_ts);
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
        self.index.to_string().into()
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        // Parse as plain timestamp (microseconds as string)
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

        // Fall back to CdcOffset enum format: {"Spanner": {...}}
        let cdc_offset: crate::source::cdc::external::CdcOffset =
            serde_json::from_str(&last_seen_offset)?;
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

        split.update_offset((t200.unix_timestamp_nanos() / 1000).to_string())
            .unwrap();
        assert_eq!(split.offset, Some(t200));
    }
}
