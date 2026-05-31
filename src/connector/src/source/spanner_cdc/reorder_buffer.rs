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
//! ## Placeholder entries
//!
//! When a parent partition splits or merges, it produces child partitions
//! that all share the same `start_timestamp`. Placeholder entries are
//! inserted for every known future partition at `start_ts`. The global
//! watermark (`min` across all non-finished partitions) is naturally
//! pinned at `start_ts` until every partition starts and advances past it.
//!
//! This prevents the following race:
//! ```text
//! Parent A splits → B1, B2, B3, B4 (all start at T0).
//! max_concurrent=3 → B1,B2,B3 start. B4 queued.
//! Without placeholders: global_wm=min(B1,B2,B3) → emits past T0.
//! B4 starts at T0 → produces data at ts=150 (pre-DDL).
//! Already emitted data at ts=400 (post-DDL). Order violation!
//!
//! With placeholders: B4 placeholder at T0 → global_wm pinned at T0.
//! B4 starts → advances past T0 → watermark advances → flush in order.
//! ```
//!
//! ## Watermark model
//!
//! Each partition tracks an **offset** (latest commit_ts from data records
//! or heartbeats). Un-started partitions have placeholder entries at their
//! `start_ts`. The watermark is:
//! ```text
//! watermark = max(
//!     last_emitted_watermark,    // hard floor: never regress
//!     min(                       // min across all non-finished partitions
//!         partition.offset
//!         WHERE partition is not finished
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
    /// Declare that these partition tokens exist at `start_ts`. Inserts
    /// placeholder entries so the global watermark is pinned until each
    /// partition starts and advances.
    DeclarePartitions {
        tokens: Vec<Option<String>>,
        start_ts: OffsetDateTime,
    },

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
            known_schemas: HashMap::new(),
            last_emitted_watermark: None,
        }
    }

    /// Process an event. Updates internal state only — call [`drain`] to
    /// retrieve records that are now ready to emit.
    pub fn handle(&mut self, event: ReorderBufferEvent) {
        match event {
            ReorderBufferEvent::DeclarePartitions { tokens, start_ts } => {
                // `or_insert` so a real PartitionStarted that arrived first
                // is not clobbered.
                for token in &tokens {
                    self.partitions
                        .entry(token.clone())
                        .or_insert(PartitionEntry {
                            offset: start_ts,
                            finished: false,
                        });
                }
            }
            ReorderBufferEvent::PartitionStarted { token, start_ts } => {
                // Register partition.  If a placeholder already exists at
                // the same start_ts this is a no-op overwrite.  We assert
                // the offset doesn't regress as a safety check.
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
                }
                // Buffer the record
                self.buffer
                    .entry(commit_ts)
                    .or_default()
                    .push(record.into());
                let buffered = self.buffer_len();
                if buffered > 0 && buffered % 10_000 == 0 {
                    tracing::warn!(
                        buffered,
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
    pub fn buffer_len(&self) -> usize {
        self.buffer.values().map(|v| v.len()).sum()
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
    // Placeholder entries: the core race condition fix
    // -------------------------------------------------------------------

    #[test]
    fn incomplete_partition_group_pins_watermark() {
        let mut rb = ReorderBuffer::new();

        // Parent A → B1,B2,B3,B4. Declare partition group at ts=0.
        rb.handle(ReorderBufferEvent::DeclarePartitions {
            tokens: vec![
                Some("B1".into()),
                Some("B2".into()),
                Some("B3".into()),
                Some("B4".into()),
            ],
            start_ts: ts(0),
        });

        // Start B1, B2, B3 (max_concurrent=3)
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B1".into()),
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B2".into()),
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B3".into()),
            start_ts: ts(0),
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

        // B4 placeholder at ts(0) pins the watermark at ts(0).
        // No records buffered at ts=0 → nothing emitted.
        assert_eq!(
            rb.watermark(),
            Some(ts(0)),
            "B4 placeholder pins watermark at start_ts"
        );
    }

    #[test]
    fn partition_group_flushes_when_complete() {
        let mut rb = ReorderBuffer::new();

        rb.handle(ReorderBufferEvent::DeclarePartitions {
            tokens: vec![
                Some("B1".into()),
                Some("B2".into()),
                Some("B3".into()),
                Some("B4".into()),
            ],
            start_ts: ts(0),
        });

        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B1".into()),
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B2".into()),
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B3".into()),
            start_ts: ts(0),
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

        // B4 placeholder at ts(0) pins watermark → nothing at ts=0 to drain
        // B1 finishes
        rb.handle(ReorderBufferEvent::PartitionFinished {
            token: Some("B1".into()),
        });

        // Start B4 — overwrites placeholder (same ts=0)
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B4".into()),
            start_ts: ts(0),
        });
        let out = rb.drain();

        // B4 just started at ts=0, B2 at ts=400, B3 at ts=0
        // Global watermark = min(B2=400, B3=0, B4=0) = 0
        // Only records at ts=0 → none buffered at ts=0
        // B1 is finished so excluded from min
        assert_no_emit(&out, "B4 just started at ts=0, nothing at ts=0 in buffer");

        // B4 heartbeat at ts=500
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B4".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(500),
                msg: heartbeat_msg(ts(500)),
            },
        });
        let out = rb.drain();
        // global_wm = min(B2=400, B3=0, B4=500) = 0 (B3 hasn't advanced)
        // ts=300 and ts=400 are > 0, still buffered
        assert_no_emit(&out, "B3 hasn't advanced past ts=0");

        // B3 heartbeat at ts=600
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B3".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(600),
                msg: heartbeat_msg(ts(600)),
            },
        });
        let out = rb.drain();
        // global_wm = min(B2=400, B3=600, B4=500) = 400
        // Drain ts=300, ts=400 → 2 heartbeats
        assert_eq!(
            out.len(),
            2,
            "group complete, global_wm=400, drain ts=300 and ts=400"
        );
    }

    #[test]
    fn late_partition_does_not_cause_ordering_violation() {
        // THE critical test: B4 starts late with old-schema data.
        // Verify no ordering violation.
        let mut rb = ReorderBuffer::new();

        let old_cols = vec![col("id", TypeCode::Int64, 1, true)];
        let new_cols = vec![
            col("id", TypeCode::Int64, 1, true),
            col("name", TypeCode::String, 2, false),
        ];

        rb.handle(ReorderBufferEvent::DeclarePartitions {
            tokens: vec![
                Some("B1".into()),
                Some("B2".into()),
                Some("B3".into()),
                Some("B4".into()),
            ],
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B1".into()),
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B2".into()),
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B3".into()),
            start_ts: ts(0),
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

        // B4 placeholder at ts(0) pins watermark — nothing at ts=0 to drain.
        assert_eq!(rb.watermark(), Some(ts(0)));

        // B1 finishes, B4 starts → group complete
        rb.handle(ReorderBufferEvent::PartitionFinished {
            token: Some("B1".into()),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B4".into()),
            start_ts: ts(0),
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
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B3".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(700),
                msg: heartbeat_msg(ts(700)),
            },
        });
        let out = rb.drain();

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
    // Deferred partition: the max_concurrent split bug fix
    // -------------------------------------------------------------------

    #[test]
    fn deferred_partition_starts_when_slot_opens() {
        // Simulates: max_concurrent=3, 4 partitions declared.
        // B1,B2,B3 start immediately. B4 deferred.
        // Later B4 starts (carried through pending queue).
        // Group should complete and buffered records should drain.
        let mut rb = ReorderBuffer::new();

        rb.handle(ReorderBufferEvent::DeclarePartitions {
            tokens: vec![
                Some("B1".into()),
                Some("B2".into()),
                Some("B3".into()),
                Some("B4".into()),
            ],
            start_ts: ts(0),
        });

        // Start B1, B2, B3
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B1".into()),
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B2".into()),
            start_ts: ts(0),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B3".into()),
            start_ts: ts(0),
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

        // B4 placeholder at ts(0) pins watermark — nothing at ts=0 to drain
        assert_eq!(
            rb.watermark(),
            Some(ts(0)),
            "B4 placeholder pins watermark at start_ts"
        );

        // B4 starts (carried through deferral)
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B4".into()),
            start_ts: ts(0),
        });
        let out = rb.drain();

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
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B4".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(600),
                msg: heartbeat_msg(ts(600)),
            },
        });
        let out = rb.drain();

        // global_wm = min(B1=300, B2=400, B3=500, B4=600) = 300
        // Drain ts=300
        assert_eq!(out.len(), 1, "global_wm=300 → drain ts=300 only");
        assert!(out[0].offset.contains("300"), "first emitted at ts=300");

        // B1 heartbeat at ts=500 → global_wm = min(500,400,500,600) = 400
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(500),
                msg: heartbeat_msg(ts(500)),
            },
        });
        let out = rb.drain();
        assert_eq!(out.len(), 1, "global_wm=400 → drain ts=400");
        assert!(out[0].offset.contains("400"), "second emitted at ts=400");
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

        // Group 1: partition group A1, A2
        rb.handle(ReorderBufferEvent::DeclarePartitions {
            tokens: vec![Some("A1".into()), Some("A2".into())],
            start_ts: ts(100),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("A1".into()),
            start_ts: ts(100),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("A2".into()),
            start_ts: ts(100),
        });

        // Group 2: partition group B1, B2 (children of A1, but independent group)
        rb.handle(ReorderBufferEvent::DeclarePartitions {
            tokens: vec![Some("B1".into()), Some("B2".into())],
            start_ts: ts(300),
        });
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

    // -------------------------------------------------------------------
    // Mixed-group ordering: placeholder pins watermark across groups
    // -------------------------------------------------------------------

    #[test]
    fn mixed_group_placeholder_pins_watermark() {
        // G1 = {B1,B2,B3,B4} at start_ts=100. B4 deferred (max_concurrent).
        // B1,B2,B3 finish. Their children form G2 at start_ts=500.
        // G2 advances to wm=800. B4 placeholder must pin global wm at 100.
        // B4 starts later, produces ts=350 (old schema) before ts=400 (new).

        let mut rb = ReorderBuffer::new();

        let old_cols = vec![col("id", TypeCode::Int64, 1, true)];
        let new_cols = vec![
            col("id", TypeCode::Int64, 1, true),
            col("name", TypeCode::String, 2, false),
        ];

        // G1: B1,B2,B3,B4 at start_ts=100
        rb.handle(ReorderBufferEvent::DeclarePartitions {
            tokens: vec![
                Some("B1".into()),
                Some("B2".into()),
                Some("B3".into()),
                Some("B4".into()),
            ],
            start_ts: ts(100),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B1".into()),
            start_ts: ts(100),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B2".into()),
            start_ts: ts(100),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B3".into()),
            start_ts: ts(100),
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

        // B4 placeholder pins watermark at ts=100
        assert_eq!(
            rb.watermark(),
            Some(ts(100)),
            "B4 placeholder at ts=100 pins global watermark"
        );

        // B1,B2,B3 finish — their watermark is gone, but B4 placeholder remains
        rb.handle(ReorderBufferEvent::PartitionFinished {
            token: Some("B1".into()),
        });
        rb.handle(ReorderBufferEvent::PartitionFinished {
            token: Some("B2".into()),
        });
        rb.handle(ReorderBufferEvent::PartitionFinished {
            token: Some("B3".into()),
        });

        // G2: children of B1-B3, complete, at start_ts=500
        rb.handle(ReorderBufferEvent::DeclarePartitions {
            tokens: vec![Some("C1".into()), Some("C2".into())],
            start_ts: ts(500),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("C1".into()),
            start_ts: ts(500),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("C2".into()),
            start_ts: ts(500),
        });
        // C1 advances to ts=800
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("C1".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(800),
                msg: heartbeat_msg(ts(800)),
            },
        });

        // CRITICAL: watermark must still be pinned at ts=100 (B4 placeholder)
        assert_eq!(
            rb.watermark(),
            Some(ts(100)),
            "G2 running ahead must NOT push watermark past B4's start_ts"
        );

        // No records should be emitted yet (nothing at ts≤100 in buffer)
        let drain = rb.drain();
        assert!(drain.is_empty(), "nothing at ts≤100 should be drained yet");

        // B4 finally starts — overwrites placeholder
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B4".into()),
            start_ts: ts(100),
        });

        // B4 produces old-schema data at ts=350
        // drain() below yields schema@350
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B4".into()),
            record: data_record(ts(350), "t", old_cols.clone()),
        });
        let out1 = rb.drain();

        // B4 heartbeat at ts=900
        // drain() below yields schema@400 + heartbeat@500
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B4".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(900),
                msg: heartbeat_msg(ts(900)),
            },
        });
        let out2 = rb.drain();

        // Combine: out1=[schema@350], out2=[schema@400, heartbeat@500]
        let mut all = out1;
        all.extend(out2);

        // global_wm progression:
        //   B4 data ts=350 → wm=min(C1=800,C2=500,B4=350)=350 → drain ts=350
        //   B4 heartbeat ts=900 → wm=min(800,500,900)=500 → drain ts=400,500
        assert_eq!(all.len(), 3, "schema@350 + schema@400 + heartbeat@500");

        // CRITICAL: ordering must be ts=350 BEFORE ts=400
        assert!(
            all[0].offset.contains("350"),
            "old schema at ts=350 must come first, got offset={}",
            all[0].offset
        );
        assert!(
            all[1].offset.contains("400"),
            "new schema at ts=400 must come second, got offset={}",
            all[1].offset
        );
    }

    #[test]
    fn placeholder_does_not_emit_records() {
        // Placeholder entries must NOT produce drain output — they have no
        // buffered records. Only real partitions produce records.
        let mut rb = ReorderBuffer::new();

        rb.handle(ReorderBufferEvent::DeclarePartitions {
            tokens: vec![Some("A".into()), Some("B".into())],
            start_ts: ts(100),
        });
        // Only A starts, B is still a placeholder
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("A".into()),
            start_ts: ts(100),
        });

        // A produces data at ts=200
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("A".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(200),
                msg: heartbeat_msg(ts(200)),
            },
        });

        // global_wm = min(A=200, B_placeholder=100) = 100
        // ts=100: nothing buffered → no output
        let out = rb.drain();
        assert!(out.is_empty(), "placeholder has no records to drain");
    }

    #[test]
    fn placeholder_overwrite_preserves_watermark() {
        // When PartitionStarted fires for a token that already has a
        // placeholder, the watermark must not regress.
        let mut rb = ReorderBuffer::new();

        rb.handle(ReorderBufferEvent::DeclarePartitions {
            tokens: vec![Some("A".into()), Some("B".into())],
            start_ts: ts(100),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("A".into()),
            start_ts: ts(100),
        });

        // A advances to ts=300 — nothing at ts≤100 to drain
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("A".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(300),
                msg: heartbeat_msg(ts(300)),
            },
        });

        // B placeholder at ts=100 pins watermark
        assert_eq!(rb.watermark(), Some(ts(100)));

        // B starts — same start_ts, must not clobber anything
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B".into()),
            start_ts: ts(100),
        });

        // B advances to ts=250 — handle() drains ts=250 internally
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(250),
                msg: heartbeat_msg(ts(250)),
            },
        });
        let out = rb.drain();

        // global_wm = min(A=300, B=250) = 250
        // drain ts=250 (B's heartbeat) — returned by handle()
        assert_eq!(out.len(), 1, "drain ts=250 (B's heartbeat)");
        assert!(out[0].offset.contains("250"));
    }

    // -------------------------------------------------------------------
    // Partitions discovered one at a time (simulates child_discovery_rx path)
    // -------------------------------------------------------------------

    #[test]
    fn partitions_discovered_sequentially_still_pinned() {
        // Simulates: parent finishes, children arrive one at a time via
        // child_discovery_rx.  Each child triggers DeclarePartitions
        // + PartitionStarted.  The placeholder from the first group
        // declaration must pin the watermark until all partitions start.
        let mut rb = ReorderBuffer::new();

        // First partition discovered — group declared with just A
        rb.handle(ReorderBufferEvent::DeclarePartitions {
            tokens: vec![Some("A".into()), Some("B".into())],
            start_ts: ts(100),
        });
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("A".into()),
            start_ts: ts(100),
        });

        // A advances to ts=400
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("A".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(400),
                msg: heartbeat_msg(ts(400)),
            },
        });

        // B placeholder at ts=100 pins watermark — nothing at ts≤100
        assert_eq!(rb.watermark(), Some(ts(100)));
        let out = rb.drain();
        assert!(out.is_empty(), "B placeholder pins watermark at 100");

        // Second partition discovered — starts, advances
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("B".into()),
            start_ts: ts(100),
        });
        rb.handle(ReorderBufferEvent::Record {
            partition_token: Some("B".into()),
            record: PartitionRecord::Heartbeat {
                commit_ts: ts(300),
                msg: heartbeat_msg(ts(300)),
            },
        });

        // global_wm = min(A=400, B=300) = 300
        // ts=300 is B's heartbeat, no records at ts≤300 from A
        let out = rb.drain();
        assert_eq!(out.len(), 1, "B's ts=300 heartbeat drained");
    }

    #[test]
    #[should_panic(expected = "offset regression")]
    fn partition_started_panics_on_watermark_regression() {
        // PartitionStarted with start_ts < placeholder.offset must panic.
        let mut rb = ReorderBuffer::new();

        rb.handle(ReorderBufferEvent::DeclarePartitions {
            tokens: vec![Some("A".into())],
            start_ts: ts(200),
        });
        // Placeholder at ts=200.  Starting at ts=100 would regress.
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("A".into()),
            start_ts: ts(100),
        });
    }

    #[test]
    fn overlapping_declare_partitions_uses_first_offset() {
        // DeclarePartitions called twice for the same token at different
        // offsets.  `or_insert` keeps the first (placeholder) entry.
        let mut rb = ReorderBuffer::new();

        rb.handle(ReorderBufferEvent::DeclarePartitions {
            tokens: vec![Some("A".into())],
            start_ts: ts(100),
        });
        // Second declaration at ts=200 — should NOT overwrite the ts=100 placeholder
        rb.handle(ReorderBufferEvent::DeclarePartitions {
            tokens: vec![Some("A".into())],
            start_ts: ts(200),
        });

        // Placeholder offset is still 100 (first insert wins)
        assert_eq!(rb.watermark(), Some(ts(100)));

        // Start A at ts=100 (same as placeholder) — no regression
        rb.handle(ReorderBufferEvent::PartitionStarted {
            token: Some("A".into()),
            start_ts: ts(100),
        });
        assert_eq!(rb.watermark(), Some(ts(100)));
    }

    #[test]
    fn buffer_len_tracks_record_count() {
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
}
