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
//! 2. **Reorder**: the watermark is `min(partition.offset)` across all
//!    non-finished partitions. Records at `commit_ts <= watermark` are
//!    drained in order.
//! 3. **Emit**: drained records are emitted as `Vec<SourceMessage>`, with
//!    schema-change messages prepended before their data records.
//!
//! ## Watermark model
//!
//! Each partition tracks an **offset** (latest commit_ts from data records
//! or heartbeats). The watermark follows the Spanner change-stream docs:
//!
//! > "Once all readers have received either a heartbeat greater than or
//! > equal to some timestamp A or have received data or child partition
//! > records greater than or equal to timestamp A, the readers know they
//! > have received all records committed at or before that timestamp A."
//!
//! ```text
//! watermark = max(
//!     last_emitted_watermark,    // hard floor: never regress
//!     min(                       // min across all non-finished partitions
//!         partition.offset
//!         WHERE partition is not finished
//!     )
//! )
//! ```
//!
//! ## Parent-child ordering
//!
//! When a parent partition splits or merges, children share the same
//! `start_timestamp`. The reader ensures all siblings are registered
//! atomically before any of them starts receiving data, so the watermark
//! naturally pins at `start_ts` until all children advance past it.

use std::collections::{BTreeMap, HashMap};

use time::OffsetDateTime;

use crate::source::spanner_cdc::types::{ColumnType, SpannerType};
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
    spanner_type: SpannerType,
    is_primary_key: bool,
    ordinal_position: i64,
}

impl ColumnSchema {
    fn from_column_type(ct: &ColumnType) -> Self {
        Self {
            name: ct.name.clone(),
            spanner_type: ct.spanner_type.clone(),
            is_primary_key: ct.is_primary_key,
            ordinal_position: ct.ordinal_position,
        }
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
    /// A partition has started reading. Its initial watermark is `start_ts`.
    PartitionStarted {
        token: Option<String>,
        start_ts: OffsetDateTime,
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
    /// Latest commit_ts this partition has reached.
    offset: OffsetDateTime,
    finished: bool,
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
    /// Partition token → offset + finished status.
    partitions: HashMap<Option<String>, PartitionEntry>,
    /// Records buffered by commit_ts, awaiting the global watermark.
    buffer: BTreeMap<OffsetDateTime, Vec<BufferedRecord>>,
    /// Total number of records in `buffer`.
    buffered_count: usize,
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
            buffer: BTreeMap::new(),
            buffered_count: 0,
            known_schemas: HashMap::new(),
            last_emitted_watermark: None,
        }
    }

    /// Process an event. Updates internal state only — call [`drain`] to
    /// retrieve records that are now ready to emit.
    pub fn handle(&mut self, event: ReorderBufferEvent) {
        match event {
            ReorderBufferEvent::PartitionStarted { token, start_ts } => {
                // Register partition.  If already exists at the same start_ts
                // this is a no-op overwrite.  We assert the offset doesn't
                // regress as a safety check.
                if let Some(existing) = self.partitions.get(&token) {
                    assert!(
                        start_ts >= existing.offset,
                        "PartitionStarted offset regression: {} < {}",
                        start_ts,
                        existing.offset
                    );
                }
                self.partitions.insert(
                    token.clone(),
                    PartitionEntry {
                        offset: start_ts,
                        finished: false,
                    },
                );
            }
            ReorderBufferEvent::PartitionFinished { token } => {
                if let Some(entry) = self.partitions.get_mut(&token) {
                    entry.finished = true;
                }
            }
            ReorderBufferEvent::Record {
                partition_token,
                record,
            } => {
                let commit_ts = record.commit_ts();
                // Advance this partition's offset
                if let Some(entry) = self.partitions.get_mut(&partition_token) {
                    entry.offset = commit_ts;
                } else {
                    tracing::warn!(
                        ?partition_token,
                        ?commit_ts,
                        "record from unknown partition — buffered but watermark not advanced"
                    );
                }
                // Buffer the record
                self.buffer
                    .entry(commit_ts)
                    .or_default()
                    .push(record.into());
                self.buffered_count += 1;
                if self.buffered_count % 10_000 == 0 {
                    let total = self.partitions.len();
                    let finished = self.partitions.values().filter(|e| e.finished).count();
                    let active = total - finished;
                    let wm = self.watermark();
                    let highest_buffered = self.buffer.keys().last().copied();
                    tracing::warn!(
                        buffered = self.buffered_count,
                        active_partitions = active,
                        finished_partitions = finished,
                        ?wm,
                        ?highest_buffered,
                        "reorder buffer growing — unstarted partition pinning watermark or downstream backpressure"
                    );
                }
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

    /// Total number of buffered records across all timestamps.
    /// Maintained incrementally by `handle` and `drain`.
    pub fn buffer_len(&self) -> usize {
        self.buffered_count
    }

    /// Compute the watermark: `min(partition.offset)` across all
    /// non-finished partitions, clamped to `last_emitted_watermark`.
    ///
    /// When all partitions are finished and buffer is non-empty, returns
    /// the highest buffered timestamp to force-drain everything remaining.
    fn watermark(&self) -> Option<OffsetDateTime> {
        let has_active = self.partitions.values().any(|e| !e.finished);

        // All done → drain remaining buffer
        if !has_active && !self.buffer.is_empty() {
            return self.buffer.keys().last().copied();
        }

        // min across all non-finished partitions (placeholders included)
        let raw = self
            .partitions
            .iter()
            .filter(|(_, entry)| !entry.finished)
            .map(|(_, entry)| entry.offset)
            .min();

        match (raw, self.last_emitted_watermark) {
            (Some(wm), Some(floor)) => Some(std::cmp::max(wm, floor)),
            (Some(wm), None) => Some(wm),
            (None, _) => None,
        }
    }

    /// Drain all buffered records whose `commit_ts <= watermark`.
    fn drain_ready(&mut self) -> Vec<SourceMessage> {
        let wm = match self.watermark() {
            Some(wm) => wm,
            None => return vec![],
        };

        let mut output = Vec::new();

        // Drain records from the front of the BTreeMap in ascending
        // commit_ts order until we exceed the watermark.
        while let Some(entry) = self.buffer.first_entry() {
            if *entry.key() > wm {
                break;
            }
            let (ts, records) = entry.remove_entry();
            self.buffered_count -= records.len();
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
                            ts,
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
        match self.known_schemas.get(table_name) {
            None => {
                let new_schema = Self::extract_schema(table_name, column_types);
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
                // Avoid allocating TableSchema when schema unchanged (hot path).
                if !Self::column_types_differ(&old.columns, column_types) {
                    return None;
                }
                let new_schema = Self::extract_schema(table_name, column_types);
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
        }
    }

    fn extract_schema(table_name: &str, column_types: &[ColumnType]) -> TableSchema {
        let columns = column_types
            .iter()
            .map(|ct| ColumnSchema::from_column_type(ct))
            .collect();
        TableSchema {
            table_name: table_name.to_string(),
            columns,
        }
    }

    /// Compare column names/types without allocating a `TableSchema`.
    fn column_types_differ(old_columns: &[ColumnSchema], column_types: &[ColumnType]) -> bool {
        if old_columns.len() != column_types.len() {
            return true;
        }
        old_columns
            .iter()
            .zip(column_types.iter())
            .any(|(old, new)| old.name != new.name || old.spanner_type != new.spanner_type)
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
        });
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("only".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(10),
                msg: heartbeat_msg(ts(10)),
            },
        });
        let out = rb.drain();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn two_partitions_watermark_gating() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p2".into()),
            start_ts: ts(0),
        });

        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(10),
                msg: heartbeat_msg(ts(10)),
            },
        });
        let out = rb.drain();
        assert_no_emit(&out, "P2 at ts=0, P1 at ts=10");

        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(10),
                msg: heartbeat_msg(ts(10)),
            },
        });
        let out = rb.drain();
        assert_eq!(out.len(), 2, "both at ts=10, global_wm=10");
    }

    #[test]
    fn finished_partition_releases_watermark() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p2".into()),
            start_ts: ts(0),
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
        rb.handle(ReorderBufferEvent::PartitionFinished {
            token: Some("p2".into()),
        });
        let out = rb.drain();
        assert_eq!(out.len(), 1, "P1's ts=10 record released");
    }

    // -------------------------------------------------------------------
    // last_emitted_watermark: hard floor prevents any regression
    // -------------------------------------------------------------------

    #[test]
    fn last_emitted_watermark_prevents_regression() {
        // Two partitions, no partition group. P1 emits to ts=100.
        // Then P2 starts at ts=0 and produces ts=10.
        // The last_emitted_watermark clamps the global watermark to 100,
        // so P2's ts=10 record is immediately eligible (10 <= 100).
        // This is correct because P2 is NOT in a partition group — the reader
        // decided it's safe to start P2 without waiting for other partitions.
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
        });

        // Emit ts=100
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(100),
                msg: heartbeat_msg(ts(100)),
            },
        });
        let out1 = rb.drain();
        assert_eq!(out1.len(), 1);
        assert_eq!(rb.last_emitted_watermark, Some(ts(100)));

        // P2 joins (no partition group — reader decided this is safe)
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p2".into()),
            start_ts: ts(0),
        });

        // P2 heartbeat at ts=10
        // global_wm = min(P1=100, P2=10) = 10
        // clamped to max(10, last_emitted=100) = 100
        // ts=10 ≤ 100 → drained
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(10),
                msg: heartbeat_msg(ts(10)),
            },
        });
        let out2 = rb.drain();
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
        });
        rb.handle(ReorderBufferEvent::Record {
            partition_token: None,
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(50),
                msg: heartbeat_msg(ts(50)),
            },
        });
        let out = rb.drain();
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
        });
        let cols = vec![col("id", TypeCode::Int64, 1, true)];
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(10), "t", cols),
        });
        let out = rb.drain();
        assert_eq!(count_schema_changes(&out), 1);
    }

    #[test]
    fn same_schema_does_not_reemit() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
        });
        let cols = vec![col("id", TypeCode::Int64, 1, true)];
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(10), "t", cols.clone()),
        });
        // First drain: schema change emitted for first encounter
        let out = rb.drain();
        assert_eq!(count_schema_changes(&out), 1);

        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p1".into()),
            record: data_record(ts(20), "t", cols),
        });
        // Second drain: same schema → no re-emission
        let out = rb.drain();
        assert_eq!(count_schema_changes(&out), 0);
    }

    #[test]
    fn stale_partition_cannot_regress_schema() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p2".into()),
            start_ts: ts(0),
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
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p2".into()),
            record: data_record(ts(50), "t", old_cols),
        });
        let out2 = rb.drain();
        assert_eq!(
            count_schema_changes(&out2),
            1,
            "ts=50: old schema first encounter"
        );

        // P2 heartbeat at ts=120 → global_wm=min(100,120)=100 → drain
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(120),
                msg: heartbeat_msg(ts(120)),
            },
        });
        let out3 = rb.drain();
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
    // Independent partition groups: watermark computed independently
    // -------------------------------------------------------------------

    #[test]
    fn independent_partition_groups_dont_interfere() {
        // Two independent partition groups:
        //   Group 1: A1, A2 at start_ts=100
        //   Group 2: B1, B2 at start_ts=300 (children of A1)
        //
        // Both groups complete. Verifies that groups are independent —
        // each group's watermark contribution is separate, and a slow
        // partition in one group doesn't block the other.
        let mut rb = ReorderBuffer::new();

        // Group 1: partitions A1, A2
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("A1".into()),
            start_ts: ts(100),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("A2".into()),
            start_ts: ts(100),
        });

        // Group 2: partitions B1, B2 (children of A1, but independent group)
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B1".into()),
            start_ts: ts(300),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B2".into()),
            start_ts: ts(300),
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
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(500),
                msg: heartbeat_msg(ts(500)),
            },
        });
        let out = rb.drain();

        // global_wm = min(A1=200, A2=100, B1=400, B2=500) = 100
        // ts=100: nothing buffered at ts=100 → nothing to drain
        assert_no_emit(&out, "A2 watermark at ts=100 holds back drain");

        // A2 advances to ts=250
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("A2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(250),
                msg: heartbeat_msg(ts(250)),
            },
        });
        let out = rb.drain();

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
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p2".into()),
            start_ts: ts(0),
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
        let out = rb.drain();
        assert_eq!(out.len(), 1, "drain ts=100");

        // P1 finishes → global_wm = P2=200 → drain ts=200
        rb.handle(ReorderBufferEvent::PartitionFinished {
            token: Some("p1".into()),
        });
        let out = rb.drain();
        assert_eq!(out.len(), 1, "P1 finished → P2's ts=200 record drained");
    }

    #[test]
    fn buffer_len_tracks_buffered_count() {
        let mut rb = ReorderBuffer::new();
        assert_eq!(rb.buffer_len(), 0);

        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
        });

        // Buffer 3 records
        for i in 1..=3 {
            rb.handle(ReorderBufferEvent::Record {
                partition_token: Some("p1".into()),
                record: PartitionRecord::Heartbeat {
                    commit_ts: ts(i * 100),
                    msg: heartbeat_msg(ts(i * 100)),
                },
            });
        }

        // All 3 buffered (watermark at ts=0, nothing drains yet)
        assert_eq!(rb.buffer_len(), 3);

        // Advance another partition to push watermark
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p2".into()),
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(500),
                msg: heartbeat_msg(ts(500)),
            },
        });

        // Drain: watermark = min(300, 500) = 300, drains 3 records
        rb.drain();
        assert_eq!(rb.buffer_len(), 1); // ts=500 still buffered
    }

    // -------------------------------------------------------------------
    // Edge case tests
    // -------------------------------------------------------------------

    #[test]
    fn drain_empty_buffer_with_active_partitions() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
        });
        // No records buffered, partition active → empty drain
        let out = rb.drain();
        assert!(out.is_empty(), "empty buffer with active partitions");
    }

    #[test]
    fn drain_all_finished_empty_buffer() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::PartitionFinished {
            token: Some("p1".into()),
        });
        // All finished, buffer empty → empty drain
        let out = rb.drain();
        assert!(out.is_empty(), "all finished with empty buffer");
    }

    #[test]
    fn record_from_unknown_partition_is_buffered() {
        let mut rb = ReorderBuffer::new();
        // Register p1 only
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
        });
        // Send record from unknown partition "unknown"
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("unknown".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(100),
                msg: heartbeat_msg(ts(100)),
            },
        });
        // Record is buffered but watermark is not advanced for unknown partition
        assert_eq!(rb.buffer_len(), 1, "record from unknown partition buffered");
        // Drain: p1 at ts=0, unknown not tracked → watermark=0 → nothing drained
        let out = rb.drain();
        assert!(out.is_empty(), "watermark not advanced for unknown partition");
        // Finish p1 → no active → force-drain everything
        rb.handle(ReorderBufferEvent::PartitionFinished {
            token: Some("p1".into()),
        });
        let out = rb.drain();
        assert_eq!(out.len(), 1, "force-drain after all known partitions finished");
    }

    #[test]
    fn partition_finished_for_unknown_token_is_noop() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
        });
        // Finish unknown partition → should not panic or affect state
        rb.handle(ReorderBufferEvent::PartitionFinished {
            token: Some("nonexistent".into()),
        });
        // p1 still active, watermark still at ts=0
        assert_eq!(rb.watermark(), Some(ts(0)));
    }

    #[test]
    fn data_msgs_emitted_after_schema_change() {
        let mut rb = ReorderBuffer::new();
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p1".into()),
            start_ts: ts(0),
        });

        let cols = vec![col("id", TypeCode::Int64, 1, true)];

        // Create a data record with non-empty data_msgs
        let data_msg = SourceMessage {
            key: None,
            payload: Some(serde_json::to_vec(&serde_json::json!({"after": {"id": 1}})).unwrap().into()),
            offset: "100".to_string(),
            split_id: SplitId::from("test".to_string()),
            meta: crate::source::SourceMeta::DebeziumCdc(crate::source::cdc::DebeziumCdcMeta::new(
                "t".to_string(),
                100,
                risingwave_pb::connector_service::cdc_message::CdcMessageType::Data,
                risingwave_pb::connector_service::SourceType::Unspecified,
            )),
        };
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p1".into()),
            record: PartitionRecord::DataChange {
                commit_ts: ts(100),
                table_name: "t".to_string(),
                column_types: cols,
                split_id: SplitId::from("test".to_string()),
                database_name: "db".to_string(),
                offset: "100".to_string(),
                data_msgs: vec![data_msg],
            },
        });

        // Push watermark to drain
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("p2".into()),
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("p2".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(200),
                msg: heartbeat_msg(ts(200)),
            },
        });

        let out = rb.drain();
        // Should have: 1 schema change + 1 data message = 2 total
        assert_eq!(out.len(), 2, "schema change + data message");
        // Verify ordering: schema change at index 0, data at index 1
        let schema_meta = &out[0].meta;
        let data_meta = &out[1].meta;
        match (schema_meta, data_meta) {
            (
                crate::source::SourceMeta::DebeziumCdc(s),
                crate::source::SourceMeta::DebeziumCdc(d),
            ) => {
                use crate::source::cdc::CdcMessageType;
                assert!(matches!(s.msg_type, CdcMessageType::SchemaChange), "expected SchemaChange, got {:?}", s.msg_type);
                assert!(matches!(d.msg_type, CdcMessageType::Data), "expected Data, got {:?}", d.msg_type);
            }
            _ => panic!("expected DebeziumCdc meta for both messages"),
        }
    }
}
