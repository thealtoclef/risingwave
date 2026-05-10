// Copyright 2023 RisingWave Labs
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

//! Centralized alignment-time decision for the sink coordinator.
//!
//! Each writer either commits at a checkpoint (forced by interval, vnode update, stop, schema
//! change, or a piggybacked signal from the coordinator's previous response) or sends a barrier
//! report carrying its current uncommitted byte count. When every vnode has reported for an
//! epoch, the coordinator combines those messages into one [`AlignmentOutcome`]: commit the
//! whole epoch, or accumulate (and tell the writers whether to commit at the next barrier).
//!
//! Keeping this logic out of `coordinator_worker.rs` reduces the diff in that file (which is
//! high-churn upstream).

use std::collections::HashSet;

use anyhow::anyhow;
use risingwave_connector::sink::iceberg::{
    COMMIT_CHECKPOINT_SIZE_THRESHOLD_MB, ICEBERG_DEFAULT_COMMIT_CHECKPOINT_SIZE_THRESHOLD_MB,
    ICEBERG_SINK,
};
use risingwave_connector::sink::{CONNECTOR_TYPE_KEY, SinkParam};
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::stream_plan::PbSinkSchemaChange;

use crate::manager::sink_coordination::coordinator_worker::{AligningRequests, HandleId};

/// Per-writer message accumulated for an epoch's alignment.
///
/// Each writer at each epoch is either a `Commit` (forced trigger, carries metadata) or a
/// `Report` (writer kept files open, carries current buffered bytes).
#[derive(Debug)]
pub(super) enum WriterReport {
    Report {
        uncommitted_bytes: u64,
    },
    Commit {
        metadata: SinkMetadata,
        schema_change: Option<PbSinkSchemaChange>,
    },
}

impl WriterReport {
    fn is_commit(&self) -> bool {
        matches!(self, WriterReport::Commit { .. })
    }

    fn report_bytes(&self) -> u64 {
        match self {
            WriterReport::Report { uncommitted_bytes } => *uncommitted_bytes,
            WriterReport::Commit { .. } => 0,
        }
    }
}

/// Result of [`decide_alignment`].
pub(super) enum AlignmentOutcome {
    /// No writer was forced to commit; the coordinator either tells everyone to keep
    /// accumulating (`commit_next_barrier=false`) or to commit at the next barrier
    /// (`commit_next_barrier=true`, threshold crossed).
    Accumulate {
        handle_ids: HashSet<HandleId>,
        commit_next_barrier: bool,
    },
    /// At least one writer was forced to commit. Run iceberg's pre_commit/commit_data on the
    /// gathered metadatas; ack writers that committed via `ack_commit`; tell report-only
    /// writers (if any) to commit at their next barrier so they rejoin the alignment cycle.
    Commit {
        metadatas: Vec<SinkMetadata>,
        schema_change: Option<PbSinkSchemaChange>,
        commit_handle_ids: HashSet<HandleId>,
        report_handle_ids: HashSet<HandleId>,
    },
}

/// Read the iceberg-specific snapshot threshold (per-snapshot total, in bytes) from a sink
/// param. Returns `None` for non-iceberg sinks or when the user disabled the threshold (set
/// to 0). For iceberg sinks without an explicit setting, the upstream iceberg config default
/// applies.
pub(super) fn commit_checkpoint_size_threshold_bytes(param: &SinkParam) -> Option<u64> {
    let connector = param.properties.get(CONNECTOR_TYPE_KEY)?;
    if !connector.eq_ignore_ascii_case(ICEBERG_SINK) {
        return None;
    }
    let mb = match param.properties.get(COMMIT_CHECKPOINT_SIZE_THRESHOLD_MB) {
        Some(s) => s.parse::<u64>().ok()?,
        None => ICEBERG_DEFAULT_COMMIT_CHECKPOINT_SIZE_THRESHOLD_MB,
    };
    if mb == 0 {
        return None;
    }
    Some(mb.saturating_mul(1024 * 1024))
}

/// Decide what to do with one fully-aligned epoch's worth of writer messages.
///
/// Two-branch state machine (T2 collapsed): if no writer was forced, every writer gets the
/// same `commit_next_barrier` flag and the epoch is dropped (accumulate). Otherwise we run
/// the existing commit path with the gathered metadata; report-only writers in the same
/// epoch (rare — only after post-rescale interval drift) are told to commit next barrier.
pub(super) fn decide_alignment(
    aligned_requests: AligningRequests<WriterReport>,
    threshold_bytes: Option<u64>,
) -> anyhow::Result<AlignmentOutcome> {
    let force_commit = aligned_requests.requests.iter().any(WriterReport::is_commit);
    let total_bytes: u64 = aligned_requests
        .requests
        .iter()
        .map(WriterReport::report_bytes)
        .sum();
    let threshold_crossed = threshold_bytes
        .map(|t| total_bytes >= t)
        .unwrap_or(false);

    if !force_commit {
        return Ok(AlignmentOutcome::Accumulate {
            handle_ids: aligned_requests.handle_ids,
            commit_next_barrier: threshold_crossed,
        });
    }

    let AligningRequests {
        requests,
        handle_ids_in_order,
        ..
    } = aligned_requests;
    let mut metadatas = Vec::new();
    let mut first_schema_change: Option<Option<PbSinkSchemaChange>> = None;
    let mut commit_handle_ids = HashSet::new();
    let mut report_handle_ids = HashSet::new();
    for (req, handle_id) in requests.into_iter().zip(handle_ids_in_order) {
        match req {
            WriterReport::Commit {
                metadata,
                schema_change,
            } => {
                match &first_schema_change {
                    None => first_schema_change = Some(schema_change.clone()),
                    Some(prev) => {
                        if prev != &schema_change {
                            return Err(anyhow!(
                                "got different schema change {:?} to prev schema change {:?}",
                                schema_change,
                                prev
                            ));
                        }
                    }
                }
                metadatas.push(metadata);
                commit_handle_ids.insert(handle_id);
            }
            WriterReport::Report { .. } => {
                report_handle_ids.insert(handle_id);
            }
        }
    }
    Ok(AlignmentOutcome::Commit {
        metadatas,
        schema_change: first_schema_change.unwrap_or(None),
        commit_handle_ids,
        report_handle_ids,
    })
}

/// Aggregate sum of report bytes for tracing — used by the main loop for observability.
pub(super) fn report_total_bytes(aligned: &AligningRequests<WriterReport>) -> u64 {
    aligned.requests.iter().map(WriterReport::report_bytes).sum()
}

/// Whether any writer in the aligned set was forced — used by the main loop for observability.
pub(super) fn report_force_commit(aligned: &AligningRequests<WriterReport>) -> bool {
    aligned.requests.iter().any(WriterReport::is_commit)
}

#[cfg(test)]
mod tests {
    use risingwave_connector::sink::SinkParam;
    use risingwave_connector::sink::catalog::{SinkId, SinkType};

    use super::commit_checkpoint_size_threshold_bytes;

    fn make_param(properties: &[(&str, &str)]) -> SinkParam {
        SinkParam {
            sink_id: SinkId::from(1),
            sink_name: "test".into(),
            properties: properties
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            columns: vec![],
            downstream_pk: None,
            sink_type: SinkType::AppendOnly,
            ignore_delete: false,
            format_desc: None,
            db_name: "test".into(),
            sink_from_name: "test".into(),
        }
    }

    #[test]
    fn non_iceberg_sink_returns_none() {
        let param = make_param(&[("connector", "kafka")]);
        assert_eq!(commit_checkpoint_size_threshold_bytes(&param), None);
    }

    #[test]
    fn missing_connector_returns_none() {
        let param = make_param(&[]);
        assert_eq!(commit_checkpoint_size_threshold_bytes(&param), None);
    }

    #[test]
    fn iceberg_default_threshold_is_128mb() {
        let param = make_param(&[("connector", "iceberg")]);
        assert_eq!(
            commit_checkpoint_size_threshold_bytes(&param),
            Some(128 * 1024 * 1024)
        );
    }

    #[test]
    fn iceberg_explicit_threshold_is_used() {
        let param = make_param(&[
            ("connector", "iceberg"),
            ("commit_checkpoint_size_threshold_mb", "64"),
        ]);
        assert_eq!(
            commit_checkpoint_size_threshold_bytes(&param),
            Some(64 * 1024 * 1024)
        );
    }

    #[test]
    fn iceberg_zero_threshold_disables() {
        let param = make_param(&[
            ("connector", "iceberg"),
            ("commit_checkpoint_size_threshold_mb", "0"),
        ]);
        assert_eq!(commit_checkpoint_size_threshold_bytes(&param), None);
    }

    #[test]
    fn iceberg_unparseable_threshold_returns_none() {
        let param = make_param(&[
            ("connector", "iceberg"),
            ("commit_checkpoint_size_threshold_mb", "notanumber"),
        ]);
        assert_eq!(commit_checkpoint_size_threshold_bytes(&param), None);
    }

    #[test]
    fn connector_match_is_case_insensitive() {
        let param = make_param(&[("connector", "ICEBERG")]);
        assert_eq!(
            commit_checkpoint_size_threshold_bytes(&param),
            Some(128 * 1024 * 1024)
        );
    }

    #[test]
    fn connector_value_with_whitespace_does_not_match() {
        // Documents current behavior: eq_ignore_ascii_case does not trim. SQL property
        // values are trimmed by the parser before reaching SinkParam, so this case is
        // unreachable in practice, but the test fixes the contract.
        let param = make_param(&[("connector", " iceberg ")]);
        assert_eq!(commit_checkpoint_size_threshold_bytes(&param), None);
    }
}
