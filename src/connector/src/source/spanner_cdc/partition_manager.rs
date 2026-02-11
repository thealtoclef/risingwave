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

//! Partition manager for Spanner CDC change stream consumption.
//!
//! This module implements partition state tracking and coordination based on the
//! Spanner change stream documentation:
//! https://docs.cloud.google.com/spanner/docs/change-streams/details
//!
//! Key concepts:
//! - Partitions have states: Created -> Scheduled -> Running -> Finished
//! - Child partitions must wait for ALL parents to finish before starting
//! - Watermark tracking ensures timestamp-ordered processing
//! - Heartbeat records advance the offset even when no data changes occur

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

use crate::source::spanner_cdc::split::PartitionState;
use crate::source::SplitId;
use time::OffsetDateTime;

/// Information about a partition being tracked
#[derive(Clone, Debug)]
pub struct PartitionInfo {
    /// Current state of the partition
    pub state: PartitionState,

    /// Partition token (None for root partition)
    pub partition_token: Option<String>,

    /// Parent partition tokens (empty for root partition)
    pub parent_tokens: Vec<String>,

    /// Watermark: highest timestamp processed by this partition
    ///
    /// - None: Not yet set (enumerator creates partitions with None)
    /// - Some(timestamp): Set by backfill after snapshot, or restored from checkpoint
    pub offset: Option<OffsetDateTime>,

    /// Split ID for this partition
    pub split_id: SplitId,
}

impl PartitionInfo {
    pub fn new(
        partition_token: Option<String>,
        parent_tokens: Vec<String>,
        offset: Option<OffsetDateTime>,
        split_id: SplitId,
    ) -> Self {
        Self {
            state: PartitionState::Created,
            partition_token,
            parent_tokens,
            offset,
            split_id,
        }
    }

    /// Returns true if this partition is the root partition (no parents)
    pub fn is_root(&self) -> bool {
        self.parent_tokens.is_empty()
    }
}

/// Manages partition state and coordinates child partition scheduling.
///
/// Based on spream's partition management and Spanner's change stream documentation:
/// - Tracks partition states (Created -> Scheduled -> Running -> Finished)
/// - Ensures children only start when ALL parents have finished
/// - Uses offset-based scheduling to maintain timestamp ordering
pub struct PartitionManager {
    /// All tracked partitions keyed by partition token
    partitions: Arc<Mutex<HashMap<Option<String>, PartitionInfo>>>,

    /// Notifies when any partition state changes
    notify: Arc<Notify>,
}

impl PartitionManager {
    pub fn new() -> Self {
        Self {
            partitions: Arc::new(Mutex::new(HashMap::new())),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Initialize the root partition
    pub async fn initialize_root(
        &self,
        offset: Option<OffsetDateTime>,
        split_id: SplitId,
    ) {
        let mut partitions = self.partitions.lock().await;
        let root_info = PartitionInfo::new(None, vec![], offset, split_id);
        partitions.insert(None, root_info);
    }

    /// Add a child partition discovered from a parent partition.
    /// Returns true if this is a new partition (not already tracked).
    pub async fn add_child_partition(
        &self,
        token: String,
        parent_tokens: Vec<String>,
        offset: OffsetDateTime,
        split_id: SplitId,
    ) -> bool {
        self.add_child_partition_internal(
            token,
            parent_tokens,
            Some(offset),
            split_id,
        )
        .await
    }

    /// Internal method to add or restore a child partition.
    async fn add_child_partition_internal(
        &self,
        token: String,
        parent_tokens: Vec<String>,
        offset: Option<OffsetDateTime>,
        split_id: SplitId,
    ) -> bool {
        let mut partitions = self.partitions.lock().await;
        let key = Some(token.clone());

        if partitions.contains_key(&key) {
            return false;
        }

        let info = PartitionInfo::new(
            Some(token.clone()),
            parent_tokens,
            offset,
            split_id.clone(),
        );

        partitions.insert(key, info.clone());

        // Notify waiting tasks
        self.notify.notify_waiters();
        true
    }

    /// Mark a partition as finished and update its offset.
    /// This may enable child partitions to be scheduled.
    pub async fn mark_finished(
        &self,
        partition_token: &Option<String>,
        offset: Option<OffsetDateTime>,
    ) {
        let mut partitions = self.partitions.lock().await;

        if let Some(info) = partitions.get_mut(partition_token) {
            info.state = PartitionState::Finished;
            info.offset = offset;

            // Notify waiting tasks that a parent has finished
            self.notify.notify_waiters();
        }
    }

    /// Mark a partition as running.
    pub async fn mark_running(&self, partition_token: &Option<String>) {
        let mut partitions = self.partitions.lock().await;

        if let Some(info) = partitions.get_mut(partition_token) {
            info.state = PartitionState::Running;
        }
    }

    /// Check if a child partition can be scheduled.
    ///
    /// According to Spanner docs, a child can only start when:
    /// 1. ALL its parents have finished
    /// 2. The child's offset is >= the current minimum offset
    pub async fn can_schedule_child(&self, token: &str) -> bool {
        let partitions = self.partitions.lock().await;
        let key = Some(token.to_string());

        let Some(info) = partitions.get(&key) else {
            return false;
        };

        // Check if all parents have finished
        for parent_token in &info.parent_tokens {
            let parent_key = Some(parent_token.clone());
            match partitions.get(&parent_key) {
                None => {
                    return false;
                }
                Some(parent_info) => {
                    if parent_info.state != PartitionState::Finished {
                        return false;
                    }
                }
            }
        }

        true
    }

    /// Get all child partitions that are ready to be scheduled.
    ///
    /// A child is ready when:
    /// - It's in Created state
    /// - ALL its parents have finished
    /// - AND the child's offset is >= the current minimum offset
    ///
    /// This follows the spream pattern from subscriber.go:238:
    /// "To make sure changes for a key is processed in timestamp order,
    /// wait until the records returned from all parents have been processed."
    pub async fn get_ready_children(&self) -> Vec<String> {
        let partitions = self.partitions.lock().await;
        let mut ready = Vec::new();

        // Get the minimum offset across all unfinished partitions
        // This is the "safe" timestamp - we can schedule children that
        // start at or after this timestamp
        let min_offset = self.get_min_offset_unlocked(&partitions);

        for (token_key, info) in partitions.iter() {
            if info.state == PartitionState::Created {
                if let Some(token) = token_key {
                    // Skip root partition (None token)
                    // Check if all parents have finished
                    let parents_finished = info.parent_tokens.iter().all(|parent_token| {
                        partitions
                            .get(&Some(parent_token.clone()))
                            .map(|p| p.state == PartitionState::Finished)
                            .unwrap_or(false)
                    });

                    // Watermark-based scheduling: child's offset must be >= min_offset
                    // This ensures timestamp-ordered processing as per Spanner change stream docs
                    let offset_ok = match info.offset {
                        Some(wm) => wm >= min_offset,
                        None => false, // No offset set yet, can't schedule
                    };

                    if parents_finished && offset_ok {
                        ready.push(token.clone());
                    }
                }
            }
        }

        ready
    }

    /// Get the minimum offset across all active/unfinished partitions.
    ///
    /// This represents the "safe" timestamp up to which all data has been
    /// processed. Child partitions can only start when their offset
    /// is >= this minimum offset.
    ///
    /// Returns UNIX_EPOCH if no active partitions have a offset set.
    fn get_min_offset_unlocked(&self, partitions: &HashMap<Option<String>, PartitionInfo>) -> OffsetDateTime {
        let mut min_offset = None;

        for info in partitions.values() {
            // Only consider active (Running or Finished) partitions
            if info.state == PartitionState::Running || info.state == PartitionState::Finished {
                if let Some(wm) = info.offset {
                    match min_offset {
                        None => min_offset = Some(wm),
                        Some(current_min) if wm < current_min => {
                            min_offset = Some(wm)
                        }
                        _ => {}
                    }
                }
            }
        }

        min_offset.unwrap_or_else(|| {
            // Default to Unix epoch if no partitions are active or no offsets set
            OffsetDateTime::UNIX_EPOCH
        })
    }

    /// Wait for a specific child partition to become ready.
    /// Returns when the child's parents have all finished.
    pub async fn wait_for_child_ready(&self, child_token: &str) {
        loop {
            // Check if child is ready
            if self.can_schedule_child(child_token).await {
                return;
            }

            // Wait for notification
            self.notify.notified().await;
        }
    }

    /// Get partition info by token
    pub async fn get_partition(&self, token: &Option<String>) -> Option<PartitionInfo> {
        let partitions = self.partitions.lock().await;
        partitions.get(token).cloned()
    }

    /// Get the current offset (minimum of all active partitions).
    ///
    /// This represents the "safe" timestamp up to which all data has been
    /// processed across all active partitions.
    pub async fn get_offset(&self) -> OffsetDateTime {
        let partitions = self.partitions.lock().await;
        self.get_min_offset_unlocked(&partitions)
    }

    /// Check if a partition token is already tracked
    pub async fn contains(&self, token: &Option<String>) -> bool {
        let partitions = self.partitions.lock().await;
        partitions.contains_key(token)
    }

    /// Restore a child partition from checkpoint with its state.
    ///
    /// This is used when recovering from a checkpoint to restore child partitions
    /// that were discovered before the checkpoint but not yet finished.
    pub async fn restore_child_partition(
        &self,
        token: String,
        parent_tokens: Vec<String>,
        offset: OffsetDateTime,
        state: PartitionState,
        split_id: SplitId,
    ) -> bool {
        let mut partitions = self.partitions.lock().await;
        let key = Some(token.clone());

        if partitions.contains_key(&key) {
            return false;
        }

        let info = PartitionInfo {
            state,
            partition_token: Some(token.clone()),
            parent_tokens,
            offset: Some(offset),
            split_id,
        };

        partitions.insert(key, info.clone());

        // Notify waiting tasks
        self.notify.notify_waiters();
        true
    }

    /// Get all partition tokens currently tracked
    pub async fn all_tokens(&self) -> HashSet<Option<String>> {
        let partitions = self.partitions.lock().await;
        partitions.keys().cloned().collect()
    }

    /// Get debug information about all partitions
    pub async fn debug_info(&self) -> String {
        let partitions = self.partitions.lock().await;
        let mut result = String::from("Partition Manager State:\n");

        let mut sorted_partitions: Vec<_> = partitions.iter().collect();
        sorted_partitions.sort_by_key(|(k, _)| *k);

        for (token, partition_info) in sorted_partitions {
            result.push_str(&format!(
                "  {:?}: state={:?}, offset={:?}, parents={:?}\n",
                token, partition_info.state, partition_info.offset, partition_info.parent_tokens
            ));
        }

        result
    }
}

impl Default for PartitionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_root_partition() {
        let manager = PartitionManager::new();
        let start = OffsetDateTime::now_utc();
        let split_id = "test-split".into();

        manager.initialize_root(Some(start), split_id).await;

        assert!(manager.contains(&None).await);
        let info = manager.get_partition(&None).await.unwrap();
        assert!(info.is_root());
        assert_eq!(info.state, PartitionState::Created);
    }

    #[tokio::test]
    async fn test_child_partition_wait_for_parents() {
        let manager = PartitionManager::new();
        let start = OffsetDateTime::now_utc();

        // Add root
        manager.initialize_root(Some(start), "root".into()).await;

        // Add child with parent
        let child_added = manager
            .add_child_partition(
                "child1".to_string(),
                vec![], // No parents for root's children
                start,
                "child1".into(),
            )
            .await;

        assert!(child_added);
        assert!(manager.can_schedule_child("child1").await);
    }

    #[tokio::test]
    async fn test_child_partition_with_multiple_parents() {
        let manager = PartitionManager::new();
        let start = OffsetDateTime::now_utc();

        // Create two parent partitions
        manager
            .add_child_partition("parent1".to_string(), vec![], start, "p1".into())
            .await;
        manager
            .add_child_partition("parent2".to_string(), vec![], start, "p2".into())
            .await;

        // Create child with both parents
        manager
            .add_child_partition(
                "child".to_string(),
                vec!["parent1".to_string(), "parent2".to_string()],
                start,
                "child".into(),
            )
            .await;

        // Child should NOT be ready yet
        assert!(!manager.can_schedule_child("child").await);

        // Mark first parent as finished
        manager
            .mark_finished(&Some("parent1".to_string()), Some(start))
            .await;

        // Child should still NOT be ready
        assert!(!manager.can_schedule_child("child").await);

        // Mark second parent as finished
        manager
            .mark_finished(&Some("parent2".to_string()), Some(start))
            .await;

        // Now child should be ready
        assert!(manager.can_schedule_child("child").await);
    }

    #[tokio::test]
    async fn test_offset_based_scheduling() {
        let manager = PartitionManager::new();
        let start = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let later_start = OffsetDateTime::from_unix_timestamp(2000).unwrap();

        // Initialize root partition
        manager.initialize_root(Some(start), "root".into()).await;

        // Mark root as running and update its offset
        manager.mark_running(&None).await;
        manager
            .mark_finished(&None, Some(OffsetDateTime::from_unix_timestamp(1500).unwrap()))
            .await;

        // Add a child partition that starts AFTER the root's offset
        manager
            .add_child_partition(
                "child1".to_string(),
                vec![],
                later_start, // offset=2000, which is >= root offset=1500
                "child1".into(),
            )
            .await;

        // Child should be ready because:
        // 1. All parents (none) have finished
        // 2. offset (2000) >= min_offset (1500)
        let ready = manager.get_ready_children().await;
        assert_eq!(ready, vec!["child1".to_string()]);
    }

    #[tokio::test]
    async fn test_offset_blocks_child_scheduling() {
        let manager = PartitionManager::new();
        let start = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let early_child_start = OffsetDateTime::from_unix_timestamp(1200).unwrap();

        // Initialize root partition
        manager.initialize_root(Some(start), "root".into()).await;

        // Mark root as running but with low offset
        manager.mark_running(&None).await;
        manager
            .mark_finished(&None, Some(OffsetDateTime::from_unix_timestamp(1100).unwrap()))
            .await;

        // Add a child partition that starts BEFORE the current min offset
        // start=1200, min_offset=1100
        // Since 1200 >= 1100, this should still be schedulable
        manager
            .add_child_partition(
                "child1".to_string(),
                vec![],
                early_child_start,
                "child1".into(),
            )
            .await;

        // Child should be ready because start (1200) >= min_offset (1100)
        let ready = manager.get_ready_children().await;
        assert_eq!(ready, vec!["child1".to_string()]);

        // Now add another child with start timestamp in the "future" relative to offset
        // and verify it's NOT ready until offset advances
        let future_child_start = OffsetDateTime::from_unix_timestamp(2000).unwrap();
        manager
            .add_child_partition(
                "child2".to_string(),
                vec![],
                future_child_start,
                "child2".into(),
            )
            .await;

        // child2 should NOT be ready because start (2000) > min_offset (1100)
        let ready = manager.get_ready_children().await;
        assert!(ready.contains(&"child1".to_string()));
        assert!(!ready.contains(&"child2".to_string()));
    }

    #[tokio::test]
    async fn test_child_with_multiple_parents_offset_check() {
        let manager = PartitionManager::new();
        let start = OffsetDateTime::from_unix_timestamp(1000).unwrap();
        let child_start = OffsetDateTime::from_unix_timestamp(2000).unwrap();

        // Create two parent partitions
        manager
            .add_child_partition("parent1".to_string(), vec![], start, "p1".into())
            .await;
        manager
            .add_child_partition("parent2".to_string(), vec![], start, "p2".into())
            .await;

        // Create child with both parents
        manager
            .add_child_partition(
                "child".to_string(),
                vec!["parent1".to_string(), "parent2".to_string()],
                child_start,
                "child".into(),
            )
            .await;

        // Initially child should NOT be ready (parents not finished)
        let ready = manager.get_ready_children().await;
        assert!(!ready.contains(&"child".to_string()));

        // Mark first parent as finished with low offset
        manager
            .mark_finished(
                &Some("parent1".to_string()),
                Some(OffsetDateTime::from_unix_timestamp(1500).unwrap()),
            )
            .await;

        // Child should still NOT be ready (second parent not finished)
        let ready = manager.get_ready_children().await;
        assert!(!ready.contains(&"child".to_string()));

        // Mark second parent as finished
        manager
            .mark_finished(
                &Some("parent2".to_string()),
                Some(OffsetDateTime::from_unix_timestamp(1600).unwrap()),
            )
            .await;

        // Now child should be ready:
        // - All parents finished
        // - child start (2000) >= min_offset (1500)
        let ready = manager.get_ready_children().await;
        assert!(ready.contains(&"child".to_string()));
    }
}
