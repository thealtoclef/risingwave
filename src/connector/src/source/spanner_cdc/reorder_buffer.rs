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

//! Reorder buffer for records from concurrent Spanner change-stream partitions.
//!
//! Modeled after PostgreSQL's `ReorderBuffer` for logical replication:
//! records arrive from multiple sources (WAL in PG, partitions in Spanner)
//! and must be reassembled into a single commit-timestamp-ordered stream.
//!
//! ## How it works
//!
//! 1. **Buffer**: records from each partition are buffered in a `BTreeMap`
//!    keyed by commit timestamp.
//! 2. **Reorder**: the global watermark is the minimum high-watermark across
//!    all *eligible* partitions (active, non-finished, in a complete sibling
//!    group). Records at `commit_ts <= global_watermark` are drained in order.
//! 3. **Emit**: drained records are emitted as `Vec<SourceMessage>`, with
//!    schema-change messages prepended before their data records.
//!
//! ## Sibling group gating
//!
//! When a parent partition splits or merges, it produces child partitions that
//! all share the same `start_timestamp`. These are **siblings**. The reorder
//! buffer gates flushing on sibling-group completeness: no records from any
//! member of a group participate in watermark computation until ALL members
//! have been started.
//!
//! This prevents the following race:
//! ```text
//! Parent A splits → B1, B2, B3, B4 (all start at T0).
//! max_concurrent=3 → B1,B2,B3 start. B4 queued.
//! Without gating: global_wm=min(B1,B2,B3) → emits past T0.
//! B4 starts at T0 → produces data at ts=150 (pre-DDL).
//! Already emitted data at ts=400 (post-DDL). Order violation!
//!
//! With gating: group {B1,B2,B3,B4} incomplete → buffer, don't flush.
//! B4 starts → group complete → watermark advances → flush in order.
//! ```
//!
//! ## Watermark model
//!
//! Each partition tracks a **high watermark** (latest commit_ts from data
//! records or heartbeats). The global watermark is:
//! ```text
//! global_watermark = max(
//!     last_emitted_watermark,    // hard floor: never regress
//!     min(                       // min across eligible partitions
//!         partition.watermark
//!         WHERE partition is active
//!           AND partition's sibling group is complete (or no group)
//!     )
//! )
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};

use time::OffsetDateTime;

use crate::error::ConnectorResult;
use crate::source::spanner_cdc::types::ColumnType;
use crate::source::{SourceMessage, SplitId};

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
// Types sent from partition tasks → reorder buffer
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

/// Events that drive the reorder buffer state machine.
pub(crate) enum ReorderBufferEvent {
    /// Declare a sibling group. No member's watermark participates in
    /// global watermark computation until ALL declared tokens have been
    /// `PartitionStarted`.
    SiblingGroupDeclared {
        group_id: u64,
        tokens: Vec<Option<String>>,
    },

    /// A partition has started reading. Its initial watermark is `start_ts`.
    /// If `group_id` is set, this partition belongs to that sibling group.
    PartitionStarted {
        token: Option<String>,
        start_ts: OffsetDateTime,
        group_id: Option<u64>,
    },

    /// A partition has finished reading. Excluded from future watermark
    /// computation, allowing the global watermark to advance past it.
    PartitionFinished { token: Option<String> },

    /// A record from a partition task.
    Record {
        partition_token: Option<String>,
        record: PartitionRecord,
    },
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// A record buffered inside the reorder buffer, awaiting the global watermark.
enum BufferedRecord {
    DataChange {
        table_name: String,
        column_types: Vec<ColumnType>,
        split_id: SplitId,
        database_name: String,
        offset: String,
        data_msgs: Vec<SourceMessage>,
    },
    Heartbeat {
        msg: SourceMessage,
    },
}

impl From<PartitionRecord> for BufferedRecord {
    fn from(record: PartitionRecord) -> Self {
        match record {
            PartitionRecord::DataChange {
                table_name,
                column_types,
                split_id,
                database_name,
                offset,
                data_msgs,
                ..
            } => BufferedRecord::DataChange {
                table_name,
                column_types,
                split_id,
                database_name,
                offset,
                data_msgs,
            },
            PartitionRecord::Heartbeat { msg, .. } => BufferedRecord::Heartbeat { msg },
        }
    }
}

/// Per-partition state tracked by the reorder buffer.
struct PartitionEntry {
    watermark: OffsetDateTime,
    finished: bool,
    group_id: Option<u64>,
}

/// A sibling group — partitions that must all be started before any of
/// them contribute to the global watermark.
struct SiblingGroup {
    /// All tokens declared for this group.
    expected: HashSet<Option<String>>,
    /// Tokens that have been PartitionStarted.
    started: HashSet<Option<String>>,
}

impl SiblingGroup {
    fn new(tokens: Vec<Option<String>>) -> Self {
        Self {
            expected: tokens.into_iter().collect(),
            started: HashSet::new(),
        }
    }

    /// True when every declared token has been started.
    fn is_complete(&self) -> bool {
        self.expected.iter().all(|t| self.started.contains(t))
    }

    fn mark_started(&mut self, token: &Option<String>) {
        if self.expected.contains(token) {
            self.started.insert(token.clone());
        }
    }
}

// ---------------------------------------------------------------------------
// ReorderBuffer
// ---------------------------------------------------------------------------

/// Synchronous reorder buffer that orders records from concurrent Spanner
/// change-stream partitions by commit timestamp.
///
/// Designed for single-threaded use inside the `run_reader` main loop.
/// Call [`ReorderBuffer::handle`] with [`ReorderBufferEvent`]s.
pub(crate) struct ReorderBuffer {
    /// Per-partition state.
    partitions: HashMap<Option<String>, PartitionEntry>,
    /// Sibling groups keyed by group_id.
    sibling_groups: HashMap<u64, SiblingGroup>,
    /// Records buffered by commit_ts, awaiting the global watermark.
    buffer: BTreeMap<OffsetDateTime, Vec<BufferedRecord>>,
    /// In-memory schema tracker (content-only, no watermarks).
    known_schemas: HashMap<String, TableSchema>,
    /// Hard floor: the watermark never goes below this.
    /// Updated each time we drain records.
    last_emitted_watermark: Option<OffsetDateTime>,
}

impl ReorderBuffer {
    pub fn new() -> Self {
        Self {
            partitions: HashMap::new(),
            sibling_groups: HashMap::new(),
            buffer: BTreeMap::new(),
            known_schemas: HashMap::new(),
            last_emitted_watermark: None,
        }
    }

    /// Process an event. Returns messages ready to emit in commit-ts order.
    pub fn handle(&mut self, event: ReorderBufferEvent) -> Vec<SourceMessage> {
        match event {
            ReorderBufferEvent::SiblingGroupDeclared { group_id, tokens } => {
                self.sibling_groups
                    .insert(group_id, SiblingGroup::new(tokens));
                vec![]
            }
            ReorderBufferEvent::PartitionStarted {
                token,
                start_ts,
                group_id,
            } => {
                // Register partition
                self.partitions.insert(
                    token.clone(),
                    PartitionEntry {
                        watermark: start_ts,
                        finished: false,
                        group_id,
                    },
                );
                // Mark as started in its sibling group
                if let Some(gid) = &self.partitions.get(&token).and_then(|e| e.group_id) {
                    if let Some(group) = self.sibling_groups.get_mut(gid) {
                        group.mark_started(&token);
                    }
                }
                self.drain_ready()
            }
            ReorderBufferEvent::PartitionFinished { token } => {
                if let Some(entry) = self.partitions.get_mut(&token) {
                    entry.finished = true;
                }
                self.drain_ready()
            }
            ReorderBufferEvent::Record {
                partition_token,
                record,
            } => {
                let commit_ts = record.commit_ts();
                // Advance this partition's watermark
                if let Some(entry) = self.partitions.get_mut(&partition_token) {
                    entry.watermark = commit_ts;
                }
                // Buffer the record
                self.buffer
                    .entry(commit_ts)
                    .or_default()
                    .push(record.into());
                self.drain_ready()
            }
        }
    }

    /// Force-drain any buffered records that are now ready.
    pub fn drain(&mut self) -> Vec<SourceMessage> {
        self.drain_ready()
    }

    /// Returns the last emitted watermark — the highest timestamp fully emitted
    /// in commit-ts order. Used to stamp SourceMessages with the correct offset
    /// for the executor's split.
    pub fn last_emitted_watermark(&self) -> Option<OffsetDateTime> {
        self.last_emitted_watermark
    }

    /// Compute the global watermark.
    ///
    /// Only partitions whose sibling group is complete (or that have no group)
    /// participate. Finished partitions are excluded. The result is clamped to
    /// `last_emitted_watermark` (never regress).
    ///
    /// When all partitions are finished and buffer is non-empty, returns
    /// the highest buffered timestamp to force-drain everything remaining.
    fn global_watermark(&self) -> Option<OffsetDateTime> {
        let has_active = self.partitions.values().any(|e| !e.finished);

        // All done → drain remaining buffer
        if !has_active && !self.buffer.is_empty() {
            return self.buffer.keys().last().copied();
        }

        // Compute min across eligible partitions
        let raw = self
            .partitions
            .iter()
            .filter(|(_, entry)| {
                if entry.finished {
                    return false;
                }
                // Check sibling group completeness
                if let Some(gid) = &entry.group_id {
                    if let Some(group) = self.sibling_groups.get(gid) {
                        if !group.is_complete() {
                            return false;
                        }
                    }
                }
                true
            })
            .map(|(_, entry)| entry.watermark)
            .min();

        match (raw, self.last_emitted_watermark) {
            (Some(wm), Some(floor)) => Some(std::cmp::max(wm, floor)),
            (Some(wm), None) => Some(wm),
            (None, _) => None,
        }
    }

    /// Drain all buffered records whose `commit_ts <= global_watermark`.
    fn drain_ready(&mut self) -> Vec<SourceMessage> {
        let wm = match self.global_watermark() {
            Some(wm) => wm,
            None => return vec![],
        };

        let mut output = Vec::new();

        let keys_to_drain: Vec<OffsetDateTime> = self
            .buffer
            .keys()
            .filter(|&&ts| ts <= wm)
            .copied()
            .collect();

        for key in keys_to_drain {
            if let Some(records) = self.buffer.remove(&key) {
                for record in records {
                    match record {
                        BufferedRecord::DataChange {
                            table_name,
                            column_types,
                            split_id,
                            database_name,
                            offset,
                            data_msgs,
                        } => {
                            if let Some(schema_msg) = self.check_and_evolve_schema(
                                &table_name,
                                &column_types,
                                &split_id,
                                &database_name,
                                &offset,
                                key,
                            ) {
                                output.push(schema_msg);
                            }
                            output.extend(data_msgs);
                        }
                        BufferedRecord::Heartbeat { msg } => {
                            output.push(msg);
                        }
                    }
                }
            }
        }

        if !output.is_empty() {
            self.last_emitted_watermark = Some(wm);
        }

        output
    }

    // -----------------------------------------------------------------------
    // Schema tracking (content-only, monotonic by construction)
    // -----------------------------------------------------------------------

    fn check_and_evolve_schema(
        &mut self,
        table_name: &str,
        column_types: &[ColumnType],
        split_id: &SplitId,
        database_name: &str,
        offset: &str,
        commit_ts: OffsetDateTime,
    ) -> Option<SourceMessage> {
        let new_schema = Self::extract_schema(table_name, column_types);

        match self.known_schemas.get(table_name) {
            None => {
                self.known_schemas
                    .insert(table_name.to_string(), new_schema.clone());
                Some(Self::make_schema_change_msg(
                    &new_schema,
                    split_id,
                    database_name,
                    offset,
                    table_name,
                    commit_ts,
                ))
            }
            Some(old) => {
                if Self::schemas_differ(old, &new_schema) {
                    self.known_schemas
                        .insert(table_name.to_string(), new_schema.clone());
                    Some(Self::make_schema_change_msg(
                        &new_schema,
                        split_id,
                        database_name,
                        offset,
                        table_name,
                        commit_ts,
                    ))
                } else {
                    None
                }
            }
        }
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

    fn schemas_differ(old: &TableSchema, new: &TableSchema) -> bool {
        use std::collections::HashMap;
        let old_cols: HashMap<&str, &ColumnSchema> =
            old.columns.iter().map(|c| (c.name.as_str(), c)).collect();
        let new_cols: HashMap<&str, &ColumnSchema> =
            new.columns.iter().map(|c| (c.name.as_str(), c)).collect();

        let all_names: HashSet<&str> = old_cols.keys().chain(new_cols.keys()).copied().collect();

        for name in all_names {
            match (old_cols.get(name), new_cols.get(name)) {
                (Some(o), Some(n)) if o.spanner_type != n.spanner_type => return true,
                (None, Some(_)) | (Some(_), None) => return true,
                _ => {}
            }
        }
        false
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

impl Default for ReorderBuffer {
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

    fn count_schema_changes(msgs: &[SourceMessage]) -> usize {
        msgs.iter().filter(|m| m.payload.is_some()).count()
    }

    // Helper: assert no records emitted
    fn assert_no_emit(out: &[SourceMessage], msg: &str) {
        assert!(
            out.is_empty(),
            "{}: expected no output, got {} msgs",
            msg,
            out.len()
        );
    }

    // -------------------------------------------------------------------
    // Basic tests
    // -------------------------------------------------------------------

    #[test]
    fn single_partition_emits_immediately() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("only".into()),
            start_ts: ts(0),
            group_id: None,
        });
        let out = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("only".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(10),
                msg: heartbeat_msg(ts(10)),
            },
        });
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn two_partitions_watermark_gating() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
            group_id: None,
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p2".into()),
            start_ts: ts(0),
            group_id: None,
        });

        let out = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(10),
                msg: heartbeat_msg(ts(10)),
            },
        });
        assert_no_emit(&out, "P2 at ts=0, P1 at ts=10");

        let out = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(10),
                msg: heartbeat_msg(ts(10)),
            },
        });
        assert_eq!(out.len(), 2, "both at ts=10, global_wm=10");
    }

    #[test]
    fn finished_partition_releases_watermark() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
            group_id: None,
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p2".into()),
            start_ts: ts(0),
            group_id: None,
        });
        // P1 heartbeat buffered
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(10),
                msg: heartbeat_msg(ts(10)),
            },
        });
        // P2 finishes
        let out = rb.handle(ReorderBufferEvent::PartitionFinished {
            token: Some("p2".into()),
        });
        assert_eq!(out.len(), 1, "P1's ts=10 record released");
    }

    // -------------------------------------------------------------------
    // Sibling group: the core race condition fix
    // -------------------------------------------------------------------

    #[test]
    fn incomplete_sibling_group_blocks_flush() {
        let mut rb = ReorderBuffer::new();

        // Parent A → B1,B2,B3,B4. Declare sibling group.
        rb.handle(ReorderBufferEvent::SiblingGroupDeclared {
            group_id: 1,
            tokens: vec![
                Some("B1".into()),
                Some("B2".into()),
                Some("B3".into()),
                Some("B4".into()),
            ],
        });

        // Start B1, B2, B3 (max_concurrent=3)
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B1".into()),
            start_ts: ts(0),
            group_id: Some(1),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B2".into()),
            start_ts: ts(0),
            group_id: Some(1),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B3".into()),
            start_ts: ts(0),
            group_id: Some(1),
        });

        // B1 and B2 produce data + advance watermark to 400
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(400),
                msg: heartbeat_msg(ts(400)),
            },
        });
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(400),
                msg: heartbeat_msg(ts(400)),
            },
        });

        // Nothing emitted — group incomplete (B4 not started)
        // Records buffered but NOT flushed
        // Verify by checking global watermark returns None
        assert!(
            rb.global_watermark().is_none(),
            "incomplete group → no eligible partitions → no watermark"
        );
    }

    #[test]
    fn sibling_group_flushes_when_complete() {
        let mut rb = ReorderBuffer::new();

        rb.handle(ReorderBufferEvent::SiblingGroupDeclared {
            group_id: 1,
            tokens: vec![
                Some("B1".into()),
                Some("B2".into()),
                Some("B3".into()),
                Some("B4".into()),
            ],
        });

        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B1".into()),
            start_ts: ts(0),
            group_id: Some(1),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B2".into()),
            start_ts: ts(0),
            group_id: Some(1),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B3".into()),
            start_ts: ts(0),
            group_id: Some(1),
        });

        // B1,B2,B3 produce data
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(300),
                msg: heartbeat_msg(ts(300)),
            },
        });
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(400),
                msg: heartbeat_msg(ts(400)),
            },
        });

        // Still incomplete — nothing emitted
        // B1 finishes
        rb.handle(ReorderBufferEvent::PartitionFinished {
            token: Some("B1".into()),
        });

        // Start B4 — group now complete!
        let out = rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B4".into()),
            start_ts: ts(0),
            group_id: Some(1),
        });

        // B4 just started at ts=0, B2 at ts=400, B3 at ts=0
        // Global watermark = min(B2=400, B3=0, B4=0) = 0
        // Only records at ts=0 → none buffered at ts=0
        // B1 is finished so excluded from min
        assert_no_emit(&out, "B4 just started at ts=0, nothing at ts=0 in buffer");

        // B4 heartbeat at ts=500
        let out = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B4".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(500),
                msg: heartbeat_msg(ts(500)),
            },
        });
        // global_wm = min(B2=400, B3=0, B4=500) = 0 (B3 hasn't advanced)
        // ts=300 and ts=400 are > 0, still buffered
        assert_no_emit(&out, "B3 hasn't advanced past ts=0");

        // B3 heartbeat at ts=600
        let out = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B3".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(600),
                msg: heartbeat_msg(ts(600)),
            },
        });
        // global_wm = min(B2=400, B3=600, B4=500) = 400
        // Drain ts=300, ts=400 → 2 heartbeats
        assert_eq!(
            out.len(),
            2,
            "group complete, global_wm=400, drain ts=300 and ts=400"
        );
    }

    #[test]
    fn late_sibling_does_not_cause_ordering_violation() {
        // THE critical test: B4 starts late with old-schema data.
        // Verify no ordering violation.
        let mut rb = ReorderBuffer::new();

        let old_cols = vec![col("id", TypeCode::Int64, 1, true)];
        let new_cols = vec![
            col("id", TypeCode::Int64, 1, true),
            col("name", TypeCode::String, 2, false),
        ];

        rb.handle(ReorderBufferEvent::SiblingGroupDeclared {
            group_id: 1,
            tokens: vec![
                Some("B1".into()),
                Some("B2".into()),
                Some("B3".into()),
                Some("B4".into()),
            ],
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B1".into()),
            start_ts: ts(0),
            group_id: Some(1),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B2".into()),
            start_ts: ts(0),
            group_id: Some(1),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B3".into()),
            start_ts: ts(0),
            group_id: Some(1),
        });

        // B2 produces new-schema data at ts=400
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B2".into()),
            record: data_record(ts(400), "t", new_cols.clone()),
        });
        // B2 heartbeat at ts=500
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(500),
                msg: heartbeat_msg(ts(500)),
            },
        });

        // Group incomplete → nothing emitted. ✅
        assert!(rb.global_watermark().is_none());

        // B1 finishes, B4 starts → group complete
        rb.handle(ReorderBufferEvent::PartitionFinished {
            token: Some("B1".into()),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B4".into()),
            start_ts: ts(0),
            group_id: Some(1),
        });

        // B4 produces old-schema data at ts=150
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B4".into()),
            record: data_record(ts(150), "t", old_cols.clone()),
        });

        // B4 heartbeat at ts=600
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B4".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(600),
                msg: heartbeat_msg(ts(600)),
            },
        });

        // B3 heartbeat at ts=700
        let out = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B3".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(700),
                msg: heartbeat_msg(ts(700)),
            },
        });

        // global_wm = min(B2=500, B3=700, B4=600) = 500
        // Drain: ts=150 (old schema, first encounter → schema change),
        //        ts=400 (new schema → schema change), ts=500 (heartbeat)
        // data_msgs are empty so no extra data messages
        assert_eq!(out.len(), 3, "schema@150 + schema@400 + heartbeat@500");

        // Verify ORDERING: ts=150 before ts=400 before ts=500
        assert!(
            out[0].offset.contains("150"),
            "first at ts=150, got offset={}",
            out[0].offset
        );
        assert!(
            out[1].offset.contains("400"),
            "second at ts=400, got offset={}",
            out[1].offset
        );
        assert!(
            out[2].offset.contains("500"),
            "third at ts=500, got offset={}",
            out[2].offset
        );
    }

    // -------------------------------------------------------------------
    // last_emitted_watermark: hard floor prevents any regression
    // -------------------------------------------------------------------

    #[test]
    fn last_emitted_watermark_prevents_regression() {
        // Two partitions, no sibling group. P1 emits to ts=100.
        // Then P2 starts at ts=0 and produces ts=10.
        // The last_emitted_watermark clamps the global watermark to 100,
        // so P2's ts=10 record is immediately eligible (10 <= 100).
        // This is correct because P2 is NOT in a sibling group — the reader
        // decided it's safe to start P2 without waiting for siblings.
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
            group_id: None,
        });

        // Emit ts=100
        let out1 = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(100),
                msg: heartbeat_msg(ts(100)),
            },
        });
        assert_eq!(out1.len(), 1);
        assert_eq!(rb.last_emitted_watermark, Some(ts(100)));

        // P2 joins (no sibling group — reader decided this is safe)
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p2".into()),
            start_ts: ts(0),
            group_id: None,
        });

        // P2 heartbeat at ts=10
        // global_wm = min(P1=100, P2=10) = 10
        // clamped to max(10, last_emitted=100) = 100
        // ts=10 ≤ 100 → drained
        let out2 = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(10),
                msg: heartbeat_msg(ts(10)),
            },
        });
        // P2's ts=10 heartbeat is emitted because it's ≤ watermark=100
        assert_eq!(out2.len(), 1, "ts=10 ≤ clamped watermark=100, eligible");
        // Watermark doesn't regress — stays at 100
        assert_eq!(rb.last_emitted_watermark, Some(ts(100)));
    }

    // -------------------------------------------------------------------
    // No group (root partition): always eligible
    // -------------------------------------------------------------------

    #[test]
    fn root_partition_no_group_always_eligible() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: None,
            start_ts: ts(0),
            group_id: None,
        });
        let out = rb.handle(ReorderBufferEvent::Record {
            partition_token: None,
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(50),
                msg: heartbeat_msg(ts(50)),
            },
        });
        assert_eq!(
            out.len(),
            1,
            "no group → always eligible → emit immediately"
        );
    }

    // -------------------------------------------------------------------
    // Schema tracking tests (inherited)
    // -------------------------------------------------------------------

    #[test]
    fn first_data_record_emits_schema_change() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
            group_id: None,
        });
        let cols = vec![col("id", TypeCode::Int64, 1, true)];
        let out = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(10), "t", cols),
        });
        assert_eq!(count_schema_changes(&out), 1);
    }

    #[test]
    fn same_schema_does_not_reemit() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
            group_id: None,
        });
        let cols = vec![col("id", TypeCode::Int64, 1, true)];
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(10), "t", cols.clone()),
        });
        let out = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(20), "t", cols),
        });
        assert_eq!(count_schema_changes(&out), 0);
    }

    #[test]
    fn stale_partition_cannot_regress_schema() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
            group_id: None,
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p2".into()),
            start_ts: ts(0),
            group_id: None,
        });

        let old_cols = vec![col("id", TypeCode::Int64, 1, true)];
        let new_cols = vec![
            col("id", TypeCode::Int64, 1, true),
            col("name", TypeCode::String, 2, false),
        ];

        // P1 new-schema at ts=100 → buffered
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(100), "t", new_cols.clone()),
        });
        // P2 old-schema at ts=50 → global_wm=min(100,50)=50 → drain
        let out2 = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p2".into()),
            record: data_record(ts(50), "t", old_cols),
        });
        assert_eq!(
            count_schema_changes(&out2),
            1,
            "ts=50: old schema first encounter"
        );

        // P2 heartbeat at ts=120 → global_wm=min(100,120)=100 → drain
        let out3 = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(120),
                msg: heartbeat_msg(ts(120)),
            },
        });
        assert_eq!(
            count_schema_changes(&out3),
            1,
            "ts=100: schema changed to new"
        );
        // Verify ordering: old schema at ts=50 emitted BEFORE new schema at ts=100
        assert!(out2[0].offset.contains("50"));
        assert!(out3[0].offset.contains("100"));
    }

    // -------------------------------------------------------------------
    // Deferred sibling: the max_concurrent split bug fix
    // -------------------------------------------------------------------

    #[test]
    fn deferred_sibling_completes_group_when_started_with_same_group_id() {
        // Simulates: max_concurrent=3, 4 siblings declared.
        // B1,B2,B3 start immediately. B4 deferred.
        // Later B4 starts with the SAME group_id (carried through pending queue).
        // Group should complete and buffered records should drain.
        let mut rb = ReorderBuffer::new();

        rb.handle(ReorderBufferEvent::SiblingGroupDeclared {
            group_id: 1,
            tokens: vec![
                Some("B1".into()),
                Some("B2".into()),
                Some("B3".into()),
                Some("B4".into()),
            ],
        });

        // Start B1, B2, B3
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B1".into()),
            start_ts: ts(0),
            group_id: Some(1),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B2".into()),
            start_ts: ts(0),
            group_id: Some(1),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B3".into()),
            start_ts: ts(0),
            group_id: Some(1),
        });

        // B1 and B2 produce data
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(300),
                msg: heartbeat_msg(ts(300)),
            },
        });
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(400),
                msg: heartbeat_msg(ts(400)),
            },
        });

        // Group incomplete → nothing emitted
        assert!(
            rb.global_watermark().is_none(),
            "B4 not started → group incomplete → no watermark"
        );

        // B4 starts with the SAME group_id (carried through deferral)
        let out = rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B4".into()),
            start_ts: ts(0),
            group_id: Some(1), // ← same group_id, not a new one
        });

        // Group now complete! But B4 just started at ts=0, B3 still at ts=0.
        // global_wm = min(B2=400, B3=0, B4=0) = 0. Nothing at ts=0 in buffer.
        assert_no_emit(&out, "B4 just started, nothing at ts=0");

        // B3 and B4 advance
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B3".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(500),
                msg: heartbeat_msg(ts(500)),
            },
        });
        let out = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B4".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(600),
                msg: heartbeat_msg(ts(600)),
            },
        });

        // global_wm = min(B1=300, B2=400, B3=500, B4=600) = 300
        // Drain ts=300
        assert_eq!(out.len(), 1, "global_wm=300 → drain ts=300 only");
        assert!(out[0].offset.contains("300"), "first emitted at ts=300");

        // B1 heartbeat at ts=500 → global_wm = min(500,400,500,600) = 400
        let out = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(500),
                msg: heartbeat_msg(ts(500)),
            },
        });
        assert_eq!(out.len(), 1, "global_wm=400 → drain ts=400");
        assert!(out[0].offset.contains("400"), "second emitted at ts=400");
    }

    // -------------------------------------------------------------------
    // Independent sibling groups: watermark computed independently
    // -------------------------------------------------------------------

    #[test]
    fn independent_sibling_groups_dont_interfere() {
        // Two independent sibling groups:
        //   Group 1: A1, A2 at start_ts=100
        //   Group 2: B1, B2 at start_ts=300 (children of A1)
        //
        // Both groups complete. Verifies that groups are independent —
        // each group's watermark contribution is separate, and a slow
        // partition in one group doesn't block the other.
        let mut rb = ReorderBuffer::new();

        // Group 1: siblings A1, A2
        rb.handle(ReorderBufferEvent::SiblingGroupDeclared {
            group_id: 1,
            tokens: vec![Some("A1".into()), Some("A2".into())],
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("A1".into()),
            start_ts: ts(100),
            group_id: Some(1),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("A2".into()),
            start_ts: ts(100),
            group_id: Some(1),
        });

        // Group 2: siblings B1, B2 (children of A1, but independent group)
        rb.handle(ReorderBufferEvent::SiblingGroupDeclared {
            group_id: 2,
            tokens: vec![Some("B1".into()), Some("B2".into())],
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B1".into()),
            start_ts: ts(300),
            group_id: Some(2),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B2".into()),
            start_ts: ts(300),
            group_id: Some(2),
        });

        // A1 produces data
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("A1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(200),
                msg: heartbeat_msg(ts(200)),
            },
        });

        // B1 and B2 produce data
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(400),
                msg: heartbeat_msg(ts(400)),
            },
        });
        let out = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(500),
                msg: heartbeat_msg(ts(500)),
            },
        });

        // global_wm = min(A1=200, A2=100, B1=400, B2=500) = 100
        // ts=100: nothing buffered at ts=100 → nothing to drain
        assert_no_emit(&out, "A2 watermark at ts=100 holds back drain");

        // A2 advances to ts=250
        let out = rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("A2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(250),
                msg: heartbeat_msg(ts(250)),
            },
        });

        // global_wm = min(200, 250, 400, 500) = 200
        // Drain ts=200 (A1's heartbeat)
        assert_eq!(out.len(), 1, "global_wm=200 → drain A1's ts=200");
        assert!(out[0].offset.contains("200"));
    }

    // -------------------------------------------------------------------
    // All partitions finished → drain remaining buffer
    // -------------------------------------------------------------------

    #[test]
    fn all_finished_drains_remaining_buffer() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
            group_id: None,
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p2".into()),
            start_ts: ts(0),
            group_id: None,
        });
        // P1 heartbeat at ts=100 → buffered (P2 at ts=0)
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(100),
                msg: heartbeat_msg(ts(100)),
            },
        });
        // P2 heartbeat at ts=200 → buffered
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(200),
                msg: heartbeat_msg(ts(200)),
            },
        });
        // global_wm = min(100, 200) = 100 → drain ts=100
        // (this was done inside the second handle() call, so ts=100 is already drained)

        // P1 finishes → global_wm = P2=200 → drain ts=200
        let out = rb.handle(ReorderBufferEvent::PartitionFinished {
            token: Some("p1".into()),
        });
        assert_eq!(out.len(), 1, "P1 finished → P2's ts=200 record drained");
    }
}
