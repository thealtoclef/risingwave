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

//! Partition coordinator for Spanner change-stream records.
//!
//! Records from concurrent change-stream partitions are emitted **eagerly**, in
//! per-partition commit-timestamp order, with no global reordering. This is
//! correct because RisingWave's CDC pipeline needs only **per-key** ordering,
//! which Spanner guarantees (a key lives in exactly one partition at any commit
//! timestamp) together with the reader's `parents_all_finished` gate across
//! splits/merges. Cross-partition interleaving only reorders *different* keys,
//! which is irrelevant to the downstream materialized state.
//!
//! ## What this type does
//!
//! 1. **Eager emit**: on each [`CoordinatorEvent::Record`] the record is turned
//!    into `SourceMessage`s immediately (a schema-change message prepended when
//!    the table schema grows) and staged in `ready`, drained every loop tick.
//! 2. **Scalar checkpoint watermark**: a monotonic min over non-finished
//!    partition offsets, stamped as the scalar offset on every message. It does
//!    not gate emission and exists for checkpoint compatibility, lag metrics, and
//!    safe root-mode fallback.
//! 3. **Per-partition frontier**: [`checkpoint_frontier`] exposes each
//!    non-finished partition's own offset so a restart resumes each partition
//!    independently from its own position (the Beam `PartitionMetadata` model).
//!    This — not the scalar — is the resume mechanism.
//! 4. **Monotonic schema accumulator**: the tracked schema only ever grows
//!    (ADD COLUMN / type-up), never shrinks. A record carrying *fewer* columns
//!    never emits a schema change, so the meta service never receives a subset
//!    schema and never issues a destructive DROP COLUMN.
//!
//! ## Placeholder entries
//!
//! When a parent partition splits/merges it produces child partitions that all
//! share the same `start_timestamp`. [`CoordinatorEvent::DeclarePartitions`]
//! inserts a `Pending` placeholder for every known-but-unstarted child so it
//! appears in [`checkpoint_frontier`] at its `start_ts`. A restart therefore
//! re-reads each not-yet-started child from its `start_ts`, so no records a child
//! owns are skipped. Emission is *not* affected — faster partitions flush
//! immediately.

use std::collections::HashMap;

use time::OffsetDateTime;

use crate::error::ConnectorResult;
use crate::source::cdc::external::spanner::PartitionOffset;
use crate::source::spanner_cdc::split::PartitionState;
use crate::source::spanner_cdc::types::{ColumnType, SpannerType};
use crate::source::{SourceMessage, SplitId};

/// Log a coordinator-health line every this many records seen.
const HEALTH_LOG_EVERY: u64 = 50_000;

// ---------------------------------------------------------------------------
// Schema types
// ---------------------------------------------------------------------------

/// Schema information derived from Spanner's `column_types`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TableSchema {
    table_name: String,
    columns: Vec<ColumnSchema>,
}

/// Column schema derived from Spanner `ColumnType`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnSchema {
    name: String,
    spanner_type: crate::source::spanner_cdc::types::SpannerType,
    is_primary_key: bool,
    ordinal_position: i64,
}

pub(crate) struct SeedSchema {
    pub table_name: String,
    pub columns: Vec<(String, SpannerType, bool)>,
    pub min_type_change_ts: Option<OffsetDateTime>,
}

impl ColumnSchema {
    fn from_column_type(ct: &ColumnType) -> ConnectorResult<Self> {
        Ok(Self {
            name: ct.name.clone(),
            spanner_type: ct.spanner_type.clone(),
            is_primary_key: ct.is_primary_key,
            ordinal_position: ct.ordinal_position,
        })
    }
}

// ---------------------------------------------------------------------------
// Types sent from partition tasks → coordinator
// ---------------------------------------------------------------------------

/// Record produced by a single partition task.
pub(crate) enum PartitionRecord {
    DataChange {
        commit_ts: OffsetDateTime,
        table_name: String,
        column_types: Vec<ColumnType>,
        split_id: SplitId,
        database_name: String,
        offset: String,
        data_msgs: Vec<SourceMessage>,
    },
    Heartbeat {
        commit_ts: OffsetDateTime,
        msg: SourceMessage,
    },
}

impl PartitionRecord {
    fn commit_ts(&self) -> OffsetDateTime {
        match self {
            Self::DataChange { commit_ts, .. } => *commit_ts,
            Self::Heartbeat { commit_ts, .. } => *commit_ts,
        }
    }
}

// ---------------------------------------------------------------------------
// Events (input to the state machine)
// ---------------------------------------------------------------------------

/// Events that drive the partition coordinator state machine.
pub(crate) enum CoordinatorEvent {
    /// Declare that these partition tokens exist at `start_ts`, sharing
    /// `parent_tokens`. Inserts `Pending` placeholders so the checkpoint
    /// watermark is pinned until each partition starts and advances.
    DeclarePartitions {
        tokens: Vec<Option<String>>,
        parent_tokens: Vec<String>,
        start_ts: OffsetDateTime,
    },

    /// A partition has started reading. Its initial offset is `start_ts`.
    PartitionStarted {
        token: Option<String>,
        parent_tokens: Vec<String>,
        start_ts: OffsetDateTime,
    },

    /// A partition has finished reading. Excluded from future watermark
    /// computation and from the checkpoint frontier.
    PartitionFinished { token: Option<String> },

    /// A record from a partition task — emitted eagerly.
    Record {
        partition_token: Option<String>,
        record: PartitionRecord,
    },
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Per-partition state tracked by the coordinator.
struct PartitionEntry {
    /// Latest commit_ts this partition has reached.
    offset: OffsetDateTime,
    /// Parent partition tokens (empty for the root). Persisted in the frontier
    /// so a restart can rebuild the `parents_all_finished` gate.
    parent_tokens: Vec<String>,
    /// Lifecycle state.
    state: PartitionState,
}

// ---------------------------------------------------------------------------
// PartitionCoordinator
// ---------------------------------------------------------------------------

/// Synchronous coordinator that emits records from concurrent Spanner
/// change-stream partitions eagerly while tracking the scalar checkpoint watermark,
/// the per-partition resume frontier, and a monotonic schema.
///
/// Designed for single-threaded use inside the `run_reader` main loop.
/// Call [`PartitionCoordinator::handle`] with [`CoordinatorEvent`]s, then
/// [`PartitionCoordinator::drain`] to retrieve the eagerly produced messages.
pub(crate) struct PartitionCoordinator {
    /// Partition token → offset + parents + lifecycle state.
    partitions: HashMap<Option<String>, PartitionEntry>,
    /// Messages produced eagerly, awaiting the next `drain`.
    ready: Vec<SourceMessage>,
    /// In-memory schema accumulator (monotonic superset, content-only).
    known_schemas: HashMap<String, TableSchema>,
    /// Per-table commit_ts of the last applied schema change. Gates type
    /// changes so an older out-of-order record cannot flip a column's type back.
    last_schema_commit_ts: HashMap<String, OffsetDateTime>,
    /// Optional live-schema seed derived from the table schema known by RisingWave.
    /// It is installed for a table the first time the table is seen, before
    /// comparing the incoming Spanner schema. This prevents a lagging old-schema
    /// record from becoming the first tracked schema and emitting a destructive
    /// subset schema-change event after restart.
    seed_schema: Option<TableSchema>,
    seed_min_type_change_ts: Option<OffsetDateTime>,
    /// Monotonic scalar checkpoint watermark: max(previous, min offset across
    /// non-finished partitions). It is stamped on every message for lag/backfill
    /// compatibility and root-mode fallback; per-partition resume uses
    /// `checkpoint_frontier`.
    checkpoint_watermark: Option<OffsetDateTime>,
    /// Number of records observed (for periodic health logging).
    record_count: u64,
}

impl PartitionCoordinator {
    pub fn new() -> Self {
        Self {
            partitions: HashMap::new(),
            ready: Vec::new(),
            known_schemas: HashMap::new(),
            last_schema_commit_ts: HashMap::new(),
            seed_schema: None,
            seed_min_type_change_ts: None,
            checkpoint_watermark: None,
            record_count: 0,
        }
    }

    pub fn new_with_seed_schema(seed_schema: Option<SeedSchema>) -> Self {
        let mut this = Self::new();
        if let Some(seed_schema) = seed_schema {
            if !seed_schema.columns.is_empty() {
                this.seed_min_type_change_ts = seed_schema.min_type_change_ts;
                this.seed_schema = Some(TableSchema {
                    table_name: seed_schema.table_name,
                    columns: seed_schema
                        .columns
                        .into_iter()
                        .enumerate()
                        .map(|(idx, (name, spanner_type, is_primary_key))| ColumnSchema {
                            name,
                            spanner_type,
                            is_primary_key,
                            ordinal_position: (idx + 1) as i64,
                        })
                        .collect(),
                });
            }
        }
        this
    }

    /// Process an event. Records are turned into output immediately; call
    /// [`drain`] to retrieve the staged messages.
    pub fn handle(&mut self, event: CoordinatorEvent) {
        match event {
            CoordinatorEvent::DeclarePartitions {
                tokens,
                parent_tokens,
                start_ts,
            } => {
                // `or_insert` so a real PartitionStarted that arrived first is
                // not clobbered back to Pending.
                for token in &tokens {
                    self.partitions
                        .entry(token.clone())
                        .or_insert_with(|| PartitionEntry {
                            offset: start_ts,
                            parent_tokens: parent_tokens.clone(),
                            state: PartitionState::Pending,
                        });
                }
            }
            CoordinatorEvent::PartitionStarted {
                token,
                parent_tokens,
                start_ts,
            } => {
                // If a placeholder already exists, the start offset must not
                // regress below it (safety check).
                if let Some(existing) = self.partitions.get(&token) {
                    assert!(
                        start_ts >= existing.offset,
                        "PartitionStarted offset regression: {} < {}",
                        start_ts,
                        existing.offset
                    );
                }
                self.partitions.insert(
                    token,
                    PartitionEntry {
                        offset: start_ts,
                        parent_tokens,
                        state: PartitionState::Running,
                    },
                );
            }
            CoordinatorEvent::PartitionFinished { token } => {
                if let Some(entry) = self.partitions.get_mut(&token) {
                    entry.state = PartitionState::Finished;
                }
            }
            CoordinatorEvent::Record {
                partition_token,
                record,
            } => {
                let commit_ts = record.commit_ts();
                // Advance this partition's offset (monotonic within a partition).
                if let Some(entry) = self.partitions.get_mut(&partition_token) {
                    if commit_ts > entry.offset {
                        entry.offset = commit_ts;
                    }
                }
                // Emit immediately — no global reorder gate.
                match record {
                    PartitionRecord::DataChange {
                        table_name,
                        column_types,
                        split_id,
                        database_name,
                        offset,
                        data_msgs,
                        ..
                    } => {
                        if let Some(schema_msg) = self.evolve_schema(
                            &table_name,
                            &column_types,
                            &split_id,
                            &database_name,
                            &offset,
                            commit_ts,
                        ) {
                            self.ready.push(schema_msg);
                        }
                        self.ready.extend(data_msgs);
                    }
                    PartitionRecord::Heartbeat { msg, .. } => {
                        self.ready.push(msg);
                    }
                }

                self.record_count += 1;
                if self.record_count % HEALTH_LOG_EVERY == 0 {
                    self.log_health();
                }
            }
        }
    }

    /// Take the eagerly produced messages and advance the scalar checkpoint
    /// watermark. Emission is eager; the scalar watermark is checkpoint-only.
    pub fn drain(&mut self) -> Vec<SourceMessage> {
        if let Some(min_offset) = self
            .min_active_offset()
            .or_else(|| self.max_partition_offset())
        {
            self.checkpoint_watermark = Some(match self.checkpoint_watermark {
                Some(prev) => prev.max(min_offset),
                None => min_offset,
            });
        }
        std::mem::take(&mut self.ready)
    }

    /// The scalar checkpoint watermark. Used to stamp every `SourceMessage`'s
    /// offset; per-partition resume uses `checkpoint_frontier`.
    pub fn checkpoint_watermark(&self) -> Option<OffsetDateTime> {
        self.checkpoint_watermark
    }

    /// The per-partition resume frontier: every non-finished partition with its
    /// own offset, parents and running flag. Stamped alongside the scalar offset
    /// so a restart can resume each partition independently.
    pub fn checkpoint_frontier(&self) -> Vec<PartitionOffset> {
        self.partitions
            .iter()
            .filter(|(_, e)| e.state != PartitionState::Finished)
            .map(|(token, e)| PartitionOffset {
                token: token.clone(),
                parent_tokens: e.parent_tokens.clone(),
                micros: (e.offset.unix_timestamp_nanos() / 1000) as i64,
                running: e.state == PartitionState::Running,
            })
            .collect()
    }

    /// `min(offset)` across non-finished partitions (placeholders included), for
    /// observability only — the slowest partition's position.
    fn min_active_offset(&self) -> Option<OffsetDateTime> {
        self.partitions
            .values()
            .filter(|e| e.state != PartitionState::Finished)
            .map(|e| e.offset)
            .min()
    }

    fn max_partition_offset(&self) -> Option<OffsetDateTime> {
        self.partitions.values().map(|e| e.offset).max()
    }

    fn log_health(&self) {
        let total = self.partitions.len();
        let finished = self
            .partitions
            .values()
            .filter(|e| e.state == PartitionState::Finished)
            .count();
        let active = total - finished;
        let min_offset = self.min_active_offset();
        tracing::info!(
            records = self.record_count,
            active_partitions = active,
            finished_partitions = finished,
            checkpoint_watermark = ?self.checkpoint_watermark,
            min_active_offset = ?min_offset,
            "spanner coordinator progress"
        );
    }

    // -----------------------------------------------------------------------
    // Monotonic superset schema accumulator
    // -----------------------------------------------------------------------

    /// Evolve the tracked schema for `table_name` toward the superset of all
    /// observed columns. Returns a schema-change message only when the schema
    /// grows (first sight, a new column, or an in-order type change). A record
    /// carrying a subset of columns never emits — so the meta service never sees
    /// a subset and never issues a destructive DROP COLUMN.
    fn evolve_schema(
        &mut self,
        table_name: &str,
        column_types: &[ColumnType],
        split_id: &SplitId,
        database_name: &str,
        offset: &str,
        commit_ts: OffsetDateTime,
    ) -> Option<SourceMessage> {
        let incoming = Self::extract_schema(table_name, column_types);

        match self.known_schemas.get(table_name).cloned() {
            None => self.evolve_unknown_table(
                table_name,
                incoming,
                split_id,
                database_name,
                offset,
                commit_ts,
            ),
            Some(accumulated) => {
                let last_ts = self.last_schema_commit_ts.get(table_name).copied();
                match Self::merge_superset(&accumulated, &incoming, commit_ts, last_ts) {
                    Some(merged) => {
                        self.known_schemas
                            .insert(table_name.to_string(), merged.clone());
                        let new_last = last_ts.map_or(commit_ts, |t| t.max(commit_ts));
                        self.last_schema_commit_ts
                            .insert(table_name.to_string(), new_last);
                        Some(Self::make_schema_change_msg(
                            &merged,
                            split_id,
                            database_name,
                            offset,
                            table_name,
                            commit_ts,
                        ))
                    }
                    None => None,
                }
            }
        }
    }

    fn evolve_unknown_table(
        &mut self,
        table_name: &str,
        incoming: TableSchema,
        split_id: &SplitId,
        database_name: &str,
        offset: &str,
        commit_ts: OffsetDateTime,
    ) -> Option<SourceMessage> {
        let Some(seeded) = self
            .seed_schema
            .as_ref()
            .filter(|seed| seed.table_name == table_name)
            .cloned()
        else {
            self.known_schemas
                .insert(table_name.to_string(), incoming.clone());
            self.last_schema_commit_ts
                .insert(table_name.to_string(), commit_ts);
            return Some(Self::make_schema_change_msg(
                &incoming,
                split_id,
                database_name,
                offset,
                table_name,
                commit_ts,
            ));
        };

        let seed_min_type_change_ts = self.seed_min_type_change_ts;
        let change = Self::merge_superset(&seeded, &incoming, commit_ts, seed_min_type_change_ts)
            .map(|merged| {
                self.known_schemas
                    .insert(table_name.to_string(), merged.clone());
                let new_last = seed_min_type_change_ts
                    .map(|ts| ts.max(commit_ts))
                    .unwrap_or(commit_ts);
                self.last_schema_commit_ts
                    .insert(table_name.to_string(), new_last);
                Self::make_schema_change_msg(
                    &merged,
                    split_id,
                    database_name,
                    offset,
                    table_name,
                    commit_ts,
                )
            });

        if change.is_none() {
            self.known_schemas.insert(table_name.to_string(), seeded);
            let baseline_ts = seed_min_type_change_ts
                .map(|ts| ts.max(commit_ts))
                .unwrap_or(commit_ts);
            self.last_schema_commit_ts
                .insert(table_name.to_string(), baseline_ts);
        }
        change
    }

    /// Merge `incoming` into `old`, only ever growing the schema. Returns the
    /// merged superset when something changed, else `None`.
    ///
    /// - New columns (by name) are always added (ADD is monotonic & idempotent).
    /// - A type change for an existing column is applied only if
    ///   `commit_ts >= last_ts`, so an older out-of-order record cannot revert a
    ///   newer type.
    /// - Columns present in `old` but absent in `incoming` are **kept** — never
    ///   dropped.
    fn merge_superset(
        old: &TableSchema,
        incoming: &TableSchema,
        commit_ts: OffsetDateTime,
        last_ts: Option<OffsetDateTime>,
    ) -> Option<TableSchema> {
        let old_by_name: HashMap<&str, &ColumnSchema> =
            old.columns.iter().map(|c| (c.name.as_str(), c)).collect();
        let allow_type_change = last_ts.is_none_or(|t| commit_ts >= t);

        let mut changed = false;
        let mut merged = old.columns.clone();
        let mut idx_by_name: HashMap<String, usize> = merged
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();

        for inc in &incoming.columns {
            match old_by_name.get(inc.name.as_str()) {
                None => {
                    // New column — additive.
                    idx_by_name.insert(inc.name.clone(), merged.len());
                    merged.push(inc.clone());
                    changed = true;
                }
                Some(existing) => {
                    if existing.spanner_type != inc.spanner_type && allow_type_change {
                        if let Some(&i) = idx_by_name.get(inc.name.as_str()) {
                            merged[i] = inc.clone();
                            changed = true;
                        }
                    }
                }
            }
        }

        changed.then(|| TableSchema {
            table_name: old.table_name.clone(),
            columns: merged,
        })
    }

    fn extract_schema(table_name: &str, column_types: &[ColumnType]) -> TableSchema {
        let columns = column_types
            .iter()
            .filter_map(|ct| ColumnSchema::from_column_type(ct).ok())
            .collect();
        TableSchema {
            table_name: table_name.to_string(),
            columns,
        }
    }

    fn make_schema_change_msg(
        schema: &TableSchema,
        split_id: &SplitId,
        database_name: &str,
        offset: &str,
        table_name: &str,
        commit_ts: OffsetDateTime,
    ) -> SourceMessage {
        use risingwave_pb::connector_service::SourceType;
        use risingwave_pb::connector_service::cdc_message::CdcMessageType;

        let columns: Vec<serde_json::Value> = schema
            .columns
            .iter()
            .map(|col| {
                serde_json::json!({
                    "name": col.name,
                    "typeName": col.spanner_type.to_type_string(),
                })
            })
            .collect();

        let payload = serde_json::json!({
            // Spanner's change stream API does not expose the original DDL text,
            // so we use a placeholder. Downstream consumers treat this as metadata-only.
            "ddl": "UNKNOWN_DDL",
            "tableChanges": [{
                "id": table_name,
                "type": "ALTER",
                "table": { "columns": columns },
            }],
        });

        SourceMessage {
            key: None,
            payload: Some(
                serde_json::to_vec(&payload)
                    .expect("schema change JSON serialization is infallible")
                    .into(),
            ),
            offset: offset.to_string(),
            split_id: split_id.clone(),
            meta: crate::source::SourceMeta::DebeziumCdc(
                crate::source::cdc::DebeziumCdcMeta::new_with_database_name(
                    table_name.to_string(),
                    (commit_ts.unix_timestamp_nanos() / 1_000_000) as i64,
                    CdcMessageType::SchemaChange,
                    SourceType::Unspecified,
                    Some(database_name.to_string()),
                ),
            ),
        }
    }
}

impl Default for PartitionCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::spanner_cdc::types::{SpannerType, TypeCode};

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    fn ts(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).unwrap()
    }

    fn heartbeat_msg(ts_val: OffsetDateTime) -> SourceMessage {
        use risingwave_pb::connector_service::SourceType;
        use risingwave_pb::connector_service::cdc_message::CdcMessageType;

        SourceMessage {
            key: None,
            payload: None,
            offset: format!("{}", ts_val.unix_timestamp_nanos()),
            split_id: SplitId::from("test".to_string()),
            meta: crate::source::SourceMeta::DebeziumCdc(crate::source::cdc::DebeziumCdcMeta::new(
                String::new(),
                (ts_val.unix_timestamp_nanos() / 1_000_000) as i64,
                CdcMessageType::Heartbeat,
                SourceType::Unspecified,
            )),
        }
    }

    fn col(name: &str, code: TypeCode, ordinal: i64, pk: bool) -> ColumnType {
        ColumnType {
            name: name.to_string(),
            spanner_type: SpannerType::simple(code),
            is_primary_key: pk,
            ordinal_position: ordinal,
        }
    }

    fn data_record(
        commit_ts: OffsetDateTime,
        table: &str,
        column_types: Vec<ColumnType>,
    ) -> PartitionRecord {
        PartitionRecord::DataChange {
            commit_ts,
            table_name: table.to_string(),
            column_types,
            split_id: SplitId::from("test".to_string()),
            database_name: "db".to_string(),
            offset: format!("{}", commit_ts.unix_timestamp_nanos()),
            data_msgs: vec![],
        }
    }

    fn started(token: &str, start: OffsetDateTime) -> CoordinatorEvent {
        CoordinatorEvent::PartitionStarted {
            token: Some(token.into()),
            parent_tokens: vec![],
            start_ts: start,
        }
    }

    fn heartbeat(token: &str, at: OffsetDateTime) -> CoordinatorEvent {
        CoordinatorEvent::Record {
            partition_token: Some(token.into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: at,
                msg: heartbeat_msg(at),
            },
        }
    }

    fn count_schema_changes(msgs: &[SourceMessage]) -> usize {
        msgs.iter().filter(|m| m.payload.is_some()).count()
    }

    /// Extract column names from a schema-change message payload.
    fn schema_columns(msg: &SourceMessage) -> Vec<String> {
        let payload = msg.payload.as_ref().expect("schema change has payload");
        let json: serde_json::Value = serde_json::from_slice(payload).unwrap();
        json["tableChanges"][0]["table"]["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["name"].as_str().unwrap().to_string())
            .collect()
    }

    // -------------------------------------------------------------------
    // Eager emission
    // -------------------------------------------------------------------

    #[test]
    fn single_partition_emits_immediately() {
        let mut c = PartitionCoordinator::new();
        c.handle(started("only", ts(0)));
        c.handle(heartbeat("only", ts(10)));
        let out = c.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(c.checkpoint_watermark(), Some(ts(10)));
    }

    #[test]
    fn emits_eagerly_regardless_of_other_partitions() {
        // Old design held P1's record until P2 caught up. Now it emits at once.
        let mut c = PartitionCoordinator::new();
        c.handle(started("p1", ts(0)));
        c.handle(started("p2", ts(0)));

        c.handle(heartbeat("p1", ts(10)));
        let out = c.drain();
        assert_eq!(out.len(), 1, "P1's record emits eagerly");
        // Scalar checkpoint watermark stays pinned by P2.
        assert_eq!(c.checkpoint_watermark(), Some(ts(0)));
    }

    // -------------------------------------------------------------------
    // Scalar checkpoint watermark = monotonic min over non-finished partitions
    // -------------------------------------------------------------------

    #[test]
    fn checkpoint_watermark_tracks_min_active_offset() {
        let mut c = PartitionCoordinator::new();
        c.handle(started("p1", ts(0)));
        c.handle(started("p2", ts(0)));

        c.handle(heartbeat("p1", ts(10)));
        c.drain();
        assert_eq!(c.checkpoint_watermark(), Some(ts(0)));

        c.handle(heartbeat("p2", ts(20)));
        c.drain();
        assert_eq!(c.checkpoint_watermark(), Some(ts(10)), "min(10,20)=10");
    }

    #[test]
    fn checkpoint_watermark_advances_when_slowest_partition_finishes() {
        let mut c = PartitionCoordinator::new();
        c.handle(started("p1", ts(0)));
        c.handle(started("p2", ts(0)));

        c.handle(heartbeat("p1", ts(10)));
        c.drain();
        assert_eq!(c.checkpoint_watermark(), Some(ts(0)));

        c.handle(CoordinatorEvent::PartitionFinished {
            token: Some("p2".into()),
        });
        c.drain();
        assert_eq!(c.checkpoint_watermark(), Some(ts(10)));
    }

    #[test]
    fn checkpoint_watermark_monotonic_frontier_keeps_true_offsets() {
        let mut c = PartitionCoordinator::new();
        c.handle(started("p1", ts(0)));
        c.handle(heartbeat("p1", ts(100)));
        c.drain();
        assert_eq!(c.checkpoint_watermark(), Some(ts(100)));

        // P2 joins late at ts=0 and produces ts=10 — emitted eagerly. The scalar
        // checkpoint watermark stays monotonic at 100; the frontier still records
        // P2=10 so a restart resumes P2 from its true position.
        c.handle(started("p2", ts(0)));
        c.handle(heartbeat("p2", ts(10)));
        let out = c.drain();
        assert_eq!(out.len(), 1, "P2's ts=10 emitted eagerly");
        assert_eq!(
            c.checkpoint_watermark(),
            Some(ts(100)),
            "watermark is monotonic"
        );
        let p2 = c
            .checkpoint_frontier()
            .into_iter()
            .find(|p| p.token.as_deref() == Some("p2"))
            .unwrap();
        assert_eq!(p2.micros, (ts(10).unix_timestamp_nanos() / 1000) as i64);
    }

    // -------------------------------------------------------------------
    // Placeholder appears in the frontier (drives resume), not the scalar
    // -------------------------------------------------------------------

    #[test]
    fn placeholder_appears_in_frontier_not_scalar() {
        let mut c = PartitionCoordinator::new();
        c.handle(CoordinatorEvent::DeclarePartitions {
            tokens: vec![
                Some("B1".into()),
                Some("B2".into()),
                Some("B3".into()),
                Some("B4".into()),
            ],
            parent_tokens: vec!["A".into()],
            start_ts: ts(0),
        });
        c.handle(started("B1", ts(0)));
        c.handle(started("B2", ts(0)));
        c.handle(started("B3", ts(0)));

        // B1, B2 produce data at ts=400 — emitted eagerly despite B4 pending.
        c.handle(heartbeat("B1", ts(400)));
        c.handle(heartbeat("B2", ts(400)));
        let out = c.drain();
        assert_eq!(out.len(), 2, "fast partitions flush even with B4 pending");
        // Scalar checkpoint watermark stays pinned by pending/unadvanced children.
        assert_eq!(c.checkpoint_watermark(), Some(ts(0)));
        // B4 stays in the frontier as Pending at its start_ts → re-read on restart.
        let b4 = c
            .checkpoint_frontier()
            .into_iter()
            .find(|p| p.token.as_deref() == Some("B4"))
            .unwrap();
        assert!(!b4.running);
        assert_eq!(b4.micros, (ts(0).unix_timestamp_nanos() / 1000) as i64);
    }

    // -------------------------------------------------------------------
    // checkpoint_frontier contents
    // -------------------------------------------------------------------

    #[test]
    fn checkpoint_frontier_lists_nonfinished_only() {
        let mut c = PartitionCoordinator::new();
        c.handle(CoordinatorEvent::PartitionStarted {
            token: Some("running".into()),
            parent_tokens: vec!["parent".into()],
            start_ts: ts(50),
        });
        c.handle(CoordinatorEvent::DeclarePartitions {
            tokens: vec![Some("pending".into())],
            parent_tokens: vec!["running".into()],
            start_ts: ts(70),
        });
        c.handle(CoordinatorEvent::PartitionStarted {
            token: Some("done".into()),
            parent_tokens: vec![],
            start_ts: ts(0),
        });
        c.handle(CoordinatorEvent::PartitionFinished {
            token: Some("done".into()),
        });

        let mut frontier = c.checkpoint_frontier();
        frontier.sort_by(|a, b| a.token.cmp(&b.token));
        assert_eq!(frontier.len(), 2, "finished partition excluded");

        let pending = frontier
            .iter()
            .find(|p| p.token.as_deref() == Some("pending"))
            .unwrap();
        assert!(!pending.running);
        assert_eq!(pending.parent_tokens, vec!["running".to_string()]);

        let running = frontier
            .iter()
            .find(|p| p.token.as_deref() == Some("running"))
            .unwrap();
        assert!(running.running);
        assert_eq!(
            running.micros,
            (ts(50).unix_timestamp_nanos() / 1000) as i64
        );
    }

    // -------------------------------------------------------------------
    // Checkpoint watermark is the min across active partitions
    // -------------------------------------------------------------------

    #[test]
    fn checkpoint_watermark_is_min_across_partitions() {
        let mut c = PartitionCoordinator::new();
        c.handle(started("p1", ts(0)));
        c.handle(started("p2", ts(0)));
        c.handle(heartbeat("p1", ts(100)));
        c.handle(heartbeat("p2", ts(200)));
        c.drain();
        assert_eq!(c.checkpoint_watermark(), Some(ts(100)), "min(100,200)=100");

        c.handle(CoordinatorEvent::PartitionFinished {
            token: Some("p1".into()),
        });
        c.handle(CoordinatorEvent::PartitionFinished {
            token: Some("p2".into()),
        });
        c.drain();
        assert_eq!(c.checkpoint_watermark(), Some(ts(200)));
    }

    // -------------------------------------------------------------------
    // Monotonic superset schema accumulator
    // -------------------------------------------------------------------

    #[test]
    fn first_data_record_emits_schema_change() {
        let mut c = PartitionCoordinator::new();
        c.handle(started("p1", ts(0)));
        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(10), "t", vec![col("id", TypeCode::Int64, 1, true)]),
        });
        let out = c.drain();
        assert_eq!(count_schema_changes(&out), 1);
    }

    #[test]
    fn same_schema_does_not_reemit() {
        let mut c = PartitionCoordinator::new();
        c.handle(started("p1", ts(0)));
        let cols = vec![col("id", TypeCode::Int64, 1, true)];
        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(10), "t", cols.clone()),
        });
        assert_eq!(count_schema_changes(&c.drain()), 1);

        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(20), "t", cols),
        });
        assert_eq!(
            count_schema_changes(&c.drain()),
            0,
            "same schema → no re-emit"
        );
    }

    #[test]
    fn add_column_emits_superset() {
        let mut c = PartitionCoordinator::new();
        c.handle(started("p1", ts(0)));
        let old_cols = vec![col("id", TypeCode::Int64, 1, true)];
        let new_cols = vec![
            col("id", TypeCode::Int64, 1, true),
            col("name", TypeCode::String, 2, false),
        ];
        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(10), "t", old_cols),
        });
        let out1 = c.drain();
        assert_eq!(schema_columns(&out1[0]), vec!["id"]);

        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(20), "t", new_cols),
        });
        let out2 = c.drain();
        assert_eq!(count_schema_changes(&out2), 1);
        assert_eq!(schema_columns(&out2[0]), vec!["id", "name"]);
    }

    #[test]
    fn subset_does_not_emit_or_drop() {
        // THE critical anti-DROP test: after the schema grows, a lagging
        // partition's old-schema (subset) record must NOT emit a schema change,
        // so the meta service never receives a subset → never drops a column.
        let mut c = PartitionCoordinator::new();
        c.handle(started("p1", ts(0)));
        let old_cols = vec![col("id", TypeCode::Int64, 1, true)];
        let new_cols = vec![
            col("id", TypeCode::Int64, 1, true),
            col("name", TypeCode::String, 2, false),
        ];

        // Establish the superset.
        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(10), "t", new_cols),
        });
        c.drain();

        // Subset record arrives later.
        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(20), "t", old_cols),
        });
        let out = c.drain();
        assert_eq!(count_schema_changes(&out), 0, "subset must not emit a DROP");
    }

    #[test]
    fn seeded_live_schema_prevents_first_seen_subset_drop() {
        let mut c = PartitionCoordinator::new_with_seed_schema(Some(SeedSchema {
            table_name: "t".into(),
            columns: vec![
                ("id".into(), SpannerType::simple(TypeCode::Int64), true),
                ("name".into(), SpannerType::simple(TypeCode::String), false),
            ],
            min_type_change_ts: Some(ts(100)),
        }));
        c.handle(started("p1", ts(0)));

        // A lagging partition can be the first record after restart and carry an
        // old subset schema. The live-schema seed becomes the baseline, so no
        // schema-change message is emitted and meta never sees a DROP candidate.
        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(10), "t", vec![col("id", TypeCode::Int64, 1, true)]),
        });
        assert_eq!(count_schema_changes(&c.drain()), 0);

        // A real additive schema beyond the live seed still emits the full superset.
        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(
                ts(20),
                "t",
                vec![
                    col("id", TypeCode::Int64, 1, true),
                    col("name", TypeCode::String, 2, false),
                    col("city", TypeCode::String, 3, false),
                ],
            ),
        });
        let out = c.drain();
        assert_eq!(count_schema_changes(&out), 1);
        assert_eq!(schema_columns(&out[0]), vec!["id", "name", "city"]);
    }

    #[test]
    fn seeded_live_schema_prevents_first_seen_type_regression() {
        let mut c = PartitionCoordinator::new_with_seed_schema(Some(SeedSchema {
            table_name: "t".into(),
            columns: vec![("id".into(), SpannerType::simple(TypeCode::Int64), true)],
            min_type_change_ts: Some(ts(100)),
        }));
        c.handle(started("p1", ts(0)));

        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(10), "t", vec![col("id", TypeCode::String, 1, true)]),
        });

        assert_eq!(
            count_schema_changes(&c.drain()),
            0,
            "first seen record must not regress a seeded live type"
        );
    }

    #[test]
    fn seeded_live_schema_allows_first_seen_type_change_after_seed_floor() {
        let mut c = PartitionCoordinator::new_with_seed_schema(Some(SeedSchema {
            table_name: "t".into(),
            columns: vec![("id".into(), SpannerType::simple(TypeCode::Int64), true)],
            min_type_change_ts: Some(ts(100)),
        }));
        c.handle(started("p1", ts(0)));

        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(150), "t", vec![col("id", TypeCode::String, 1, true)]),
        });

        assert_eq!(count_schema_changes(&c.drain()), 1);
    }

    #[test]
    fn seeded_live_schema_allows_first_seen_type_change_without_seed_floor() {
        let mut c = PartitionCoordinator::new_with_seed_schema(Some(SeedSchema {
            table_name: "t".into(),
            columns: vec![("id".into(), SpannerType::simple(TypeCode::Int64), true)],
            min_type_change_ts: None,
        }));
        c.handle(started("p1", ts(0)));

        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(20), "t", vec![col("id", TypeCode::String, 1, true)]),
        });

        assert_eq!(
            count_schema_changes(&c.drain()),
            1,
            "the first observed type change after restart must not be dropped"
        );
    }

    #[test]
    fn seeded_live_schema_add_before_floor_does_not_lower_type_floor() {
        let mut c = PartitionCoordinator::new_with_seed_schema(Some(SeedSchema {
            table_name: "t".into(),
            columns: vec![("id".into(), SpannerType::simple(TypeCode::Int64), true)],
            min_type_change_ts: Some(ts(100)),
        }));
        c.handle(started("p1", ts(0)));

        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(
                ts(20),
                "t",
                vec![
                    col("id", TypeCode::Int64, 1, true),
                    col("name", TypeCode::String, 2, false),
                ],
            ),
        });
        assert_eq!(count_schema_changes(&c.drain()), 1);

        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(
                ts(50),
                "t",
                vec![
                    col("id", TypeCode::String, 1, true),
                    col("name", TypeCode::String, 2, false),
                ],
            ),
        });

        assert_eq!(
            count_schema_changes(&c.drain()),
            0,
            "additive changes before the seed floor must not lower the type-change floor"
        );
    }

    #[test]
    fn seed_schema_applies_only_to_matching_table() {
        let mut c = PartitionCoordinator::new_with_seed_schema(Some(SeedSchema {
            table_name: "seeded".into(),
            columns: vec![
                ("id".into(), SpannerType::simple(TypeCode::Int64), true),
                (
                    "seed_only".into(),
                    SpannerType::simple(TypeCode::String),
                    false,
                ),
            ],
            min_type_change_ts: Some(ts(100)),
        }));
        c.handle(started("p1", ts(0)));

        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(10), "other", vec![col("id", TypeCode::Int64, 1, true)]),
        });
        let out = c.drain();

        assert_eq!(count_schema_changes(&out), 1);
        assert_eq!(schema_columns(&out[0]), vec!["id"]);
    }

    #[test]
    fn out_of_order_new_then_old_keeps_superset() {
        // Cross-partition: new-schema record observed first, older old-schema
        // record after. One ADD; superset retained; no DROP.
        let mut c = PartitionCoordinator::new();
        c.handle(started("fast", ts(0)));
        c.handle(started("slow", ts(0)));
        let old_cols = vec![col("id", TypeCode::Int64, 1, true)];
        let new_cols = vec![
            col("id", TypeCode::Int64, 1, true),
            col("name", TypeCode::String, 2, false),
        ];

        c.handle(CoordinatorEvent::Record {
            partition_token: Some("fast".into()),
            record: data_record(ts(400), "t", new_cols),
        });
        c.handle(CoordinatorEvent::Record {
            partition_token: Some("slow".into()),
            record: data_record(ts(150), "t", old_cols),
        });
        let out = c.drain();
        assert_eq!(count_schema_changes(&out), 1, "only the ADD, no DROP");
        assert_eq!(schema_columns(&out[0]), vec!["id", "name"]);
    }

    #[test]
    fn type_change_gated_by_commit_ts() {
        let mut c = PartitionCoordinator::new();
        c.handle(started("p1", ts(0)));
        c.handle(started("p2", ts(0)));

        // Establish id:INT64 at ts=100.
        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(100), "t", vec![col("id", TypeCode::Int64, 1, true)]),
        });
        assert_eq!(count_schema_changes(&c.drain()), 1);

        // Older record (ts=50) with id:STRING must NOT revert the type.
        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p2".into()),
            record: data_record(ts(50), "t", vec![col("id", TypeCode::String, 1, true)]),
        });
        assert_eq!(
            count_schema_changes(&c.drain()),
            0,
            "older type change is gated out"
        );

        // Newer record (ts=200) with id:STRING applies the type change.
        c.handle(CoordinatorEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(200), "t", vec![col("id", TypeCode::String, 1, true)]),
        });
        assert_eq!(
            count_schema_changes(&c.drain()),
            1,
            "newer type change applies"
        );
    }

    // -------------------------------------------------------------------
    // Root partition (no token)
    // -------------------------------------------------------------------

    #[test]
    fn root_partition_emits_and_checkpoint_watermark() {
        let mut c = PartitionCoordinator::new();
        c.handle(CoordinatorEvent::PartitionStarted {
            token: None,
            parent_tokens: vec![],
            start_ts: ts(0),
        });
        c.handle(CoordinatorEvent::Record {
            partition_token: None,
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(50),
                msg: heartbeat_msg(ts(50)),
            },
        });
        let out = c.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(c.checkpoint_watermark(), Some(ts(50)));
        // Root appears in the frontier with token None.
        let frontier = c.checkpoint_frontier();
        assert_eq!(frontier.len(), 1);
        assert_eq!(frontier[0].token, None);
        assert!(frontier[0].running);
    }

    #[test]
    #[should_panic(expected = "offset regression")]
    fn partition_started_panics_on_offset_regression() {
        let mut c = PartitionCoordinator::new();
        c.handle(CoordinatorEvent::DeclarePartitions {
            tokens: vec![Some("A".into())],
            parent_tokens: vec![],
            start_ts: ts(200),
        });
        // Placeholder at 200; starting at 100 would regress.
        c.handle(started("A", ts(100)));
    }
}
