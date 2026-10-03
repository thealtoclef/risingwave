// Copyright 2026 RisingWave Labs
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

//! Compares CDC key values in the upstream database, for key columns whose order RisingWave
//! cannot reproduce ([`CdcKeyComparison::Upstream`]).

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::iter;
use std::time::Duration;

use anyhow::anyhow;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::CdcKeyComparison;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DatumRef, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::error::ConnectorResult;
use risingwave_connector::source::CdcTableSnapshotSplit;
use risingwave_connector::source::cdc::external::ExternalCdcTableType;
use risingwave_connector::source::cdc::external::mysql::MySqlKeyComparator;
use risingwave_connector::source::cdc::external::postgres::PostgresKeyComparator;
use thiserror_ext::AsReport;

use crate::executor::StreamExecutorResult;
use crate::executor::backfill::cdc::upstream_table::external::ExternalStorageTable;
use crate::executor::backfill::utils::cmp_pk_unsigned_aware;

// A comparison blocks the actor and its barriers, so retries and timeouts stay short.
const MAX_COMPARISON_ATTEMPTS: u32 = 5;
const INITIAL_RETRY_BACKOFF: Duration = Duration::from_millis(200);
const COMPARISON_TIMEOUT: Duration = Duration::from_secs(10);

// Every scan actor receives every change event, so it caches the whole table's split keys.
const MAX_CACHED_SPLIT_KEY_RANKS: usize = 65_536;
const MAX_CACHED_PK_POSITION_ORDERINGS: usize = 65_536;

pub(crate) struct UpstreamKeyOrder {
    external_table: ExternalStorageTable,
    column_names: Vec<String>,
    comparator: Option<KeyComparatorImpl>,
}

enum KeyComparatorImpl {
    Postgres(PostgresKeyComparator),
    MySql(MySqlKeyComparator),
    Mock,
}

impl KeyComparatorImpl {
    async fn rank(
        &self,
        column: usize,
        values: &[&str],
        bounds: &[&str],
    ) -> ConnectorResult<Vec<usize>> {
        match self {
            Self::Postgres(comparator) => comparator.rank(column, values, bounds).await,
            Self::MySql(comparator) => comparator.rank(column, values, bounds).await,
            Self::Mock => Ok(values
                .iter()
                .map(|value| {
                    bounds
                        .iter()
                        .filter(|bound| mock_collation_cmp(bound, value).is_le())
                        .count()
                })
                .collect()),
        }
    }

    async fn compare(
        &self,
        column: usize,
        values: &[&str],
        pivot: &str,
    ) -> ConnectorResult<Vec<Ordering>> {
        match self {
            Self::Postgres(comparator) => comparator.compare(column, values, pivot).await,
            Self::MySql(comparator) => comparator.compare(column, values, pivot).await,
            Self::Mock => Ok(values
                .iter()
                .map(|value| mock_collation_cmp(value, pivot))
                .collect()),
        }
    }
}

/// Like a locale collation: alphanumerics case-insensitively first, then bytes.
fn mock_collation_cmp(lhs: &str, rhs: &str) -> Ordering {
    let collation_key = |s: &str| {
        s.chars()
            .filter(|c| c.is_alphanumeric())
            .flat_map(char::to_lowercase)
            .collect::<String>()
    };
    collation_key(lhs)
        .cmp(&collation_key(rhs))
        .then_with(|| lhs.cmp(rhs))
}

impl UpstreamKeyOrder {
    pub(crate) fn new(external_table: &ExternalStorageTable, column_names: Vec<String>) -> Self {
        Self {
            external_table: external_table.clone(),
            column_names,
            comparator: None,
        }
    }

    /// For each value, the number of `bounds` less than or equal to it.
    pub(crate) async fn rank(
        &mut self,
        column: usize,
        values: &[&str],
        bounds: &[&str],
    ) -> StreamExecutorResult<Vec<usize>> {
        self.with_retry(async |comparator| comparator.rank(column, values, bounds).await)
            .await
    }

    pub(crate) async fn compare(
        &mut self,
        column: usize,
        values: &[&str],
        pivot: &str,
    ) -> StreamExecutorResult<Vec<Ordering>> {
        self.with_retry(async |comparator| comparator.compare(column, values, pivot).await)
            .await
    }

    async fn with_retry<T>(
        &mut self,
        op: impl AsyncFn(&KeyComparatorImpl) -> ConnectorResult<T>,
    ) -> StreamExecutorResult<T> {
        // Bound the whole operation, including reconnects and backoff. Giving each attempt
        // its own timeout multiplies the time the actor cannot process barriers.
        let retries = async {
            let mut backoff = INITIAL_RETRY_BACKOFF;
            let mut attempt = 1;
            loop {
                match self.try_once(&op).await {
                    Ok(value) => return Ok(value),
                    Err(error) if attempt < MAX_COMPARISON_ATTEMPTS => {
                        tracing::warn!(
                            error = %error.as_report(),
                            attempt,
                            table = self.external_table.qualified_table_name(),
                            "failed to compare CDC keys upstream; retrying"
                        );
                        self.comparator = None;
                        tokio::time::sleep(backoff).await;
                        backoff *= 2;
                        attempt += 1;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        };
        match tokio::time::timeout(COMPARISON_TIMEOUT, retries).await {
            Ok(result) => result,
            Err(_) => {
                self.comparator = None;
                Err(anyhow!(
                    "CDC key comparison timed out after {COMPARISON_TIMEOUT:?}, including retries"
                )
                .into())
            }
        }
    }

    async fn try_once<T>(
        &mut self,
        op: &impl AsyncFn(&KeyComparatorImpl) -> ConnectorResult<T>,
    ) -> ConnectorResult<T> {
        if self.comparator.is_none() {
            self.comparator = Some(self.connect().await?);
        }
        let comparator = self.comparator.as_ref().expect("connected above");
        op(comparator).await
    }

    async fn connect(&self) -> ConnectorResult<KeyComparatorImpl> {
        match self.external_table.table_type() {
            ExternalCdcTableType::Postgres => Ok(KeyComparatorImpl::Postgres(
                PostgresKeyComparator::connect(
                    self.external_table.config(),
                    &self.external_table.schema_table_name(),
                    &self.column_names,
                )
                .await?,
            )),
            ExternalCdcTableType::MySql => Ok(KeyComparatorImpl::MySql(
                MySqlKeyComparator::connect(self.external_table.config(), &self.column_names)
                    .await?,
            )),
            ExternalCdcTableType::Mock => Ok(KeyComparatorImpl::Mock),
            table_type => Err(anyhow!(
                "comparing CDC keys upstream is not supported for {table_type:?} tables"
            )
            .into()),
        }
    }
}

fn visible_column_values(
    chunk: &StreamChunk,
    column_index: usize,
) -> impl Iterator<Item = DatumRef<'_>> {
    chunk
        .column_at(column_index)
        .iter()
        .zip_eq_debug(chunk.visibility().iter())
        .filter_map(|(value, visible)| visible.then_some(value))
}

fn upstream_key_str(datum: ScalarRefImpl<'_>) -> StreamExecutorResult<&str> {
    match datum {
        ScalarRefImpl::Utf8(value) => Ok(value),
        other => Err(anyhow!("CDC key compared upstream must be text, got {other:?}").into()),
    }
}

/// Locates split keys among an actor's consecutive splits by their rank among the split bounds.
pub(crate) struct UpstreamSplitKeyRanks {
    /// The first split's left bound if bounded, then every bounded right bound.
    bounds: Vec<Box<str>>,
    has_left_bound: bool,
    bounds_verified: bool,
    split_count: usize,
    ranks: HashMap<Box<str>, usize>,
}

impl UpstreamSplitKeyRanks {
    pub(crate) fn new(splits: &[CdcTableSnapshotSplit]) -> StreamExecutorResult<Self> {
        let mut bounds = vec![];
        let left_bound = splits
            .first()
            .and_then(|split| split.left_bound_inclusive.datum_at(0));
        if let Some(left_bound) = left_bound {
            bounds.push(upstream_key_str(left_bound)?.into());
        }
        for split in splits {
            if let Some(right_bound) = split.right_bound_exclusive.datum_at(0) {
                bounds.push(upstream_key_str(right_bound)?.into());
            }
        }
        Ok(Self {
            bounds,
            has_left_bound: left_bound.is_some(),
            bounds_verified: false,
            split_count: splits.len(),
            ranks: HashMap::new(),
        })
    }

    /// Must be called on a chunk before [`Self::locate`] on its rows.
    pub(crate) async fn resolve(
        &mut self,
        chunk: &StreamChunk,
        split_key_index: usize,
        order: &mut UpstreamKeyOrder,
    ) -> StreamExecutorResult<()> {
        if self.bounds.is_empty() {
            return Ok(());
        }
        if !self.bounds_verified {
            self.verify_bounds(order).await?;
        }
        // Evict before looking up, so every key of this chunk stays cached until it is located.
        if self.ranks.len() + chunk.cardinality() > MAX_CACHED_SPLIT_KEY_RANKS {
            self.ranks.clear();
        }
        let mut missing = HashSet::new();
        for key in visible_column_values(chunk, split_key_index).flatten() {
            let key = upstream_key_str(key)?;
            if !self.ranks.contains_key(key) {
                missing.insert(key);
            }
        }
        if missing.is_empty() {
            return Ok(());
        }

        let values = missing.into_iter().collect::<Vec<_>>();
        let bounds = self.bounds.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        let ranks = order.rank(0, &values, &bounds).await?;
        self.ranks
            .extend(values.into_iter().map(Box::<str>::from).zip_eq_debug(ranks));
        Ok(())
    }

    /// Bounds were generated in upstream order. Disorder means it changed since, e.g. a new
    /// collation version, and routing by them would lose rows.
    async fn verify_bounds(&mut self, order: &mut UpstreamKeyOrder) -> StreamExecutorResult<()> {
        let bounds = self.bounds.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        let ranks = order.rank(0, &bounds, &bounds).await?;
        if !ranks.iter().copied().eq(1..=bounds.len()) {
            return Err(anyhow!(
                "snapshot split bounds {bounds:?} are not in upstream order, which may have \
                 changed since the splits were generated"
            )
            .into());
        }
        self.bounds_verified = true;
        Ok(())
    }

    /// Returns the index of the split containing `key`, or `None` if it is outside all splits.
    pub(crate) fn locate(&self, key: DatumRef<'_>) -> Option<usize> {
        let rank = if self.bounds.is_empty() {
            0
        } else {
            match key {
                None => 0,
                Some(key) => *self
                    .ranks
                    .get(key.into_utf8())
                    .expect("split key rank must be resolved before locating the key"),
            }
        };
        let split_idx = if self.has_left_bound {
            rank.checked_sub(1)?
        } else {
            rank
        };
        (split_idx < self.split_count).then_some(split_idx)
    }
}

/// Orderings of key values against a serial backfill's scan position, for PK columns compared
/// upstream.
pub(crate) struct UpstreamPkPositionOrder {
    /// PK indices of the upstream-compared columns, in [`UpstreamKeyOrder`] column order.
    upstream_columns: Vec<usize>,
    position: Option<OwnedRow>,
    orderings: Vec<HashMap<Box<str>, Ordering>>,
}

impl UpstreamPkPositionOrder {
    pub(crate) fn new(pk_comparisons: &[CdcKeyComparison]) -> Option<Self> {
        let upstream_columns = pk_comparisons
            .iter()
            .enumerate()
            .filter(|(_, comparison)| **comparison == CdcKeyComparison::Upstream)
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();
        (!upstream_columns.is_empty()).then(|| Self {
            orderings: vec![HashMap::new(); upstream_columns.len()],
            upstream_columns,
            position: None,
        })
    }

    pub(crate) fn upstream_columns(&self) -> &[usize] {
        &self.upstream_columns
    }

    /// Must be called before comparing rows of `chunks` with `position`.
    pub(crate) async fn resolve<'a>(
        &mut self,
        chunks: impl IntoIterator<Item = &'a StreamChunk>,
        pk_indices: &[usize],
        position: &OwnedRow,
        order: &mut UpstreamKeyOrder,
    ) -> StreamExecutorResult<()> {
        if self.position.as_ref() != Some(position) {
            self.orderings.iter_mut().for_each(HashMap::clear);
            self.position = Some(position.clone());
        }

        let chunks = chunks.into_iter().collect::<Vec<_>>();
        let row_count = chunks
            .iter()
            .map(|chunk| chunk.cardinality())
            .sum::<usize>();
        for (column, &pk_idx) in self.upstream_columns.iter().enumerate() {
            let Some(pivot) = position.datum_at(pk_idx) else {
                continue;
            };
            let pivot = upstream_key_str(pivot)?;
            // Evict before resolving the entire batch, so comparisons for every buffered
            // chunk stay available until it is consumed. A single large batch may exceed the
            // cache limit, but later batches cannot accumulate additional unbounded history.
            if self.orderings[column].len().saturating_add(row_count)
                > MAX_CACHED_PK_POSITION_ORDERINGS
            {
                self.orderings[column].clear();
            }
            let mut missing = HashSet::new();
            for chunk in &chunks {
                for value in visible_column_values(chunk, pk_indices[pk_idx]).flatten() {
                    let value = upstream_key_str(value)?;
                    if value != pivot && !self.orderings[column].contains_key(value) {
                        missing.insert(value);
                    }
                }
            }
            if missing.is_empty() {
                continue;
            }
            let values = missing.into_iter().collect::<Vec<_>>();
            let orderings = order.compare(column, &values, pivot).await?;
            self.orderings[column].extend(
                values
                    .into_iter()
                    .map(Box::<str>::from)
                    .zip_eq_debug(orderings),
            );
        }
        Ok(())
    }

    /// Returns `None` if column `pk_idx` is not compared upstream.
    fn cmp_with_position(&self, pk_idx: usize, value: DatumRef<'_>) -> Option<Ordering> {
        let column = self
            .upstream_columns
            .iter()
            .position(|&idx| idx == pk_idx)?;
        let position = self
            .position
            .as_ref()
            .expect("upstream orderings must be resolved before comparing with the position");
        let ordering = match (value, position.datum_at(pk_idx)) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(value), Some(pivot)) => {
                let value = value.into_utf8();
                // Identical values are equal under any upstream order.
                if value == pivot.into_utf8() {
                    Ordering::Equal
                } else {
                    *self.orderings[column]
                        .get(value)
                        .expect("upstream ordering must be resolved before comparing the key")
                }
            }
        };
        Some(ordering)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CdcPkOrder<'a> {
    pub order: &'a [OrderType],
    pub needs_unsigned_i64_compare: &'a [bool],
    pub upstream: Option<&'a UpstreamPkPositionOrder>,
}

impl CdcPkOrder<'_> {
    pub(crate) fn cmp_with_position(&self, pk: impl Row, position: &OwnedRow) -> Ordering {
        for (idx, (lhs, rhs)) in pk.iter().zip_eq_debug(position.iter()).enumerate() {
            let upstream_ordering = self
                .upstream
                .and_then(|upstream| upstream.cmp_with_position(idx, lhs));
            let ordering = match upstream_ordering {
                Some(ordering) if self.order[idx].is_descending() => ordering.reverse(),
                Some(ordering) => ordering,
                None => cmp_pk_unsigned_aware(
                    iter::once(lhs),
                    iter::once(rhs),
                    &self.order[idx..=idx],
                    &self.needs_unsigned_i64_compare[idx..=idx],
                ),
            };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

    use risingwave_common::array::{Op, StreamChunk, StreamChunkTestExt};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::ScalarImpl;

    use super::*;

    fn split(split_id: i64, left: Option<&str>, right: Option<&str>) -> CdcTableSnapshotSplit {
        let bound = |value: Option<&str>| OwnedRow::new(vec![value.map(ScalarImpl::from)]);
        CdcTableSnapshotSplit {
            split_id,
            left_bound_inclusive: bound(left),
            right_bound_exclusive: bound(right),
        }
    }

    fn mock_order() -> UpstreamKeyOrder {
        let table =
            ExternalStorageTable::for_test_undefined().with_table_type(ExternalCdcTableType::Mock);
        UpstreamKeyOrder::new(&table, vec!["k".to_owned(), "v".to_owned()])
    }

    fn text(value: &str) -> Option<ScalarImpl> {
        Some(ScalarImpl::from(value))
    }

    #[tokio::test(start_paused = true)]
    async fn test_comparison_retry_succeeds_after_transient_failure() {
        let mut order = mock_order();
        let attempts = AtomicU32::new(0);
        let value = order
            .with_retry(async |_| {
                if attempts.fetch_add(1, AtomicOrdering::Relaxed) == 0 {
                    Err(anyhow!("transient failure").into())
                } else {
                    Ok(42)
                }
            })
            .await
            .unwrap();
        assert_eq!(value, 42);
        assert_eq!(attempts.load(AtomicOrdering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn test_comparison_retry_preserves_terminal_error() {
        let mut order = mock_order();
        let attempts = AtomicU32::new(0);
        let error = order
            .with_retry(async |_| {
                attempts.fetch_add(1, AtomicOrdering::Relaxed);
                Err::<(), _>(anyhow!("permanent failure").into())
            })
            .await
            .unwrap_err();
        assert_eq!(
            attempts.load(AtomicOrdering::Relaxed),
            MAX_COMPARISON_ATTEMPTS
        );
        assert!(error.as_report().to_string().contains("permanent failure"));
    }

    #[tokio::test(start_paused = true)]
    async fn test_comparison_timeout_includes_all_retries() {
        let mut order = mock_order();
        let attempts = AtomicU32::new(0);
        let start = tokio::time::Instant::now();
        let error = order
            .with_retry(async |_| {
                attempts.fetch_add(1, AtomicOrdering::Relaxed);
                tokio::time::sleep(Duration::from_secs(4)).await;
                Err::<(), _>(anyhow!("slow failure").into())
            })
            .await
            .unwrap_err();
        assert_eq!(start.elapsed(), COMPARISON_TIMEOUT);
        assert_eq!(attempts.load(AtomicOrdering::Relaxed), 3);
        assert!(error.as_report().to_string().contains("including retries"));
        assert!(order.comparator.is_none());
        // Cancellation must leave the comparator able to reconnect on the next operation.
        assert_eq!(
            order.compare(0, &["a"], "B").await.unwrap(),
            vec![Ordering::Less]
        );
    }

    #[test]
    fn test_mock_collation_disagrees_with_bytes() {
        assert!("a_z" < "ab");
        assert!(mock_collation_cmp("a_z", "ab").is_gt());
        assert!("B" < "a");
        assert!(mock_collation_cmp("B", "a").is_gt());
        assert!(mock_collation_cmp("a-b", "a_b").is_lt());
        assert!(mock_collation_cmp("ab", "ab").is_eq());
    }

    #[tokio::test]
    async fn test_split_key_ranks_follow_upstream_order() {
        // Upstream order: a < ab < a_z < B < c. Bytes: B < a < a_z < ab < c.
        let splits = [
            split(1, None, Some("ab")),
            split(2, Some("ab"), Some("B")),
            split(3, Some("B"), None),
        ];
        let mut ranks = UpstreamSplitKeyRanks::new(&splits).unwrap();
        let chunk = StreamChunk::from_pretty(
            " T
            + a
            + ab
            + a_z
            + B
            + c",
        );
        ranks.resolve(&chunk, 0, &mut mock_order()).await.unwrap();
        let locate = |key: &str| ranks.locate(Some(ScalarRefImpl::Utf8(key)));
        assert_eq!(locate("a"), Some(0));
        assert_eq!(locate("ab"), Some(1));
        // Byte order would put `a_z` before `ab`, in the first split.
        assert_eq!(locate("a_z"), Some(1));
        // Byte order would put `B` before `a`, in the first split.
        assert_eq!(locate("B"), Some(2));
        assert_eq!(locate("c"), Some(2));
    }

    #[tokio::test]
    async fn test_split_key_ranks_exclude_keys_outside_actor_splits() {
        let splits = [
            split(2, Some("ab"), Some("B")),
            split(3, Some("B"), Some("d")),
        ];
        let mut ranks = UpstreamSplitKeyRanks::new(&splits).unwrap();
        let chunk = StreamChunk::from_pretty(
            " T
            + a
            + a_z
            + c
            + d
            + e",
        );
        ranks.resolve(&chunk, 0, &mut mock_order()).await.unwrap();
        let locate = |key: &str| ranks.locate(Some(ScalarRefImpl::Utf8(key)));
        assert_eq!(locate("a"), None);
        assert_eq!(locate("a_z"), Some(0));
        assert_eq!(locate("c"), Some(1));
        // Right bounds are exclusive.
        assert_eq!(locate("d"), None);
        assert_eq!(locate("e"), None);
    }

    #[tokio::test]
    async fn test_split_key_rank_cache_keeps_keys_of_the_resolved_chunk() {
        let splits = [split(1, None, Some("b")), split(2, Some("b"), None)];
        let mut ranks = UpstreamSplitKeyRanks::new(&splits).unwrap();
        let mut order = mock_order();
        let cached = StreamChunk::from_pretty(
            " T
            + a",
        );
        ranks.resolve(&cached, 0, &mut order).await.unwrap();
        ranks.ranks.extend(
            (0..MAX_CACHED_SPLIT_KEY_RANKS).map(|i| (format!("filler{i}").into_boxed_str(), 0)),
        );
        let chunk = StreamChunk::from_pretty(
            " T
            + a
            + c",
        );
        ranks.resolve(&chunk, 0, &mut order).await.unwrap();
        assert_eq!(ranks.locate(Some(ScalarRefImpl::Utf8("a"))), Some(0));
        assert_eq!(ranks.locate(Some(ScalarRefImpl::Utf8("c"))), Some(1));
        assert!(ranks.ranks.len() <= MAX_CACHED_SPLIT_KEY_RANKS);
    }

    #[tokio::test]
    async fn test_single_unbounded_split_needs_no_upstream() {
        let splits = [split(1, None, None)];
        let mut ranks = UpstreamSplitKeyRanks::new(&splits).unwrap();
        let chunk = StreamChunk::from_pretty(
            " T
            + a",
        );
        // An undefined table fails any upstream query.
        let mut order = UpstreamKeyOrder::new(
            &ExternalStorageTable::for_test_undefined(),
            vec!["k".to_owned()],
        );
        ranks.resolve(&chunk, 0, &mut order).await.unwrap();
        assert_eq!(ranks.locate(Some(ScalarRefImpl::Utf8("a"))), Some(0));
    }

    #[tokio::test]
    async fn test_pk_position_order_compares_upstream_columns_upstream() {
        let comparisons = [CdcKeyComparison::Native, CdcKeyComparison::Upstream];
        let mut upstream = UpstreamPkPositionOrder::new(&comparisons).unwrap();
        assert_eq!(upstream.upstream_columns(), &[1]);
        let chunk = StreamChunk::from_pretty(
            " I T
            + 1 a_z
            + 1 B
            + 1 a
            + 1 ab
            + 0 zz
            + 2 a",
        );
        let position = OwnedRow::new(vec![Some(ScalarImpl::Int64(1)), text("ab")]);
        upstream
            .resolve([&chunk], &[0, 1], &position, &mut mock_order())
            .await
            .unwrap();

        let order = CdcPkOrder {
            order: &[OrderType::ascending(), OrderType::ascending()],
            needs_unsigned_i64_compare: &[false, false],
            upstream: Some(&upstream),
        };
        let cmp = |id: i64, value: &str| {
            order.cmp_with_position(
                OwnedRow::new(vec![Some(ScalarImpl::Int64(id)), text(value)]),
                &position,
            )
        };
        assert_eq!(cmp(1, "a"), Ordering::Less);
        assert_eq!(cmp(1, "ab"), Ordering::Equal);
        // Byte order would say `a_z < ab` and `B < ab`.
        assert_eq!(cmp(1, "a_z"), Ordering::Greater);
        assert_eq!(cmp(1, "B"), Ordering::Greater);
        assert_eq!(cmp(0, "zz"), Ordering::Less);
        assert_eq!(cmp(2, "a"), Ordering::Greater);
    }

    #[tokio::test]
    async fn test_pk_position_order_resolves_again_for_a_new_position() {
        let comparisons = [CdcKeyComparison::Upstream];
        let mut upstream = UpstreamPkPositionOrder::new(&comparisons).unwrap();
        let chunk = StreamChunk::from_pretty(
            " T
            + a_z",
        );
        let mut order = mock_order();
        let pk_order = [OrderType::ascending()];
        let cmp = |upstream: &UpstreamPkPositionOrder, key: &OwnedRow, position: &OwnedRow| {
            CdcPkOrder {
                order: &pk_order,
                needs_unsigned_i64_compare: &[false],
                upstream: Some(upstream),
            }
            .cmp_with_position(key, position)
        };
        let key = OwnedRow::new(vec![text("a_z")]);

        let position = OwnedRow::new(vec![text("ab")]);
        upstream
            .resolve([&chunk], &[0], &position, &mut order)
            .await
            .unwrap();
        assert_eq!(cmp(&upstream, &key, &position), Ordering::Greater);

        let position = OwnedRow::new(vec![text("b")]);
        upstream
            .resolve([&chunk], &[0], &position, &mut order)
            .await
            .unwrap();
        assert_eq!(cmp(&upstream, &key, &position), Ordering::Less);
    }

    #[tokio::test]
    async fn test_pk_position_cache_eviction_preserves_all_buffered_chunks() {
        let mut upstream =
            UpstreamPkPositionOrder::new(&[CdcKeyComparison::Upstream, CdcKeyComparison::Upstream])
                .unwrap();
        let position = OwnedRow::new(vec![text("B"), text("B")]);
        let first = StreamChunk::from_pretty("T T\n+ a a_z");
        let second = StreamChunk::from_pretty("T T\n+ c a");
        let mut order = mock_order();
        upstream
            .resolve([&first], &[0, 1], &position, &mut order)
            .await
            .unwrap();
        for cache in &mut upstream.orderings {
            cache.extend(
                (0..MAX_CACHED_PK_POSITION_ORDERINGS)
                    .map(|i| (format!("filler{i}").into_boxed_str(), Ordering::Less)),
            );
        }
        upstream
            .resolve([&first, &second], &[0, 1], &position, &mut order)
            .await
            .unwrap();
        assert!(upstream.orderings.iter().all(|cache| cache.len() == 2));
        assert_eq!(
            upstream.cmp_with_position(0, Some(ScalarRefImpl::Utf8("a"))),
            Some(Ordering::Less)
        );
        assert_eq!(
            upstream.cmp_with_position(0, Some(ScalarRefImpl::Utf8("c"))),
            Some(Ordering::Greater)
        );
        assert_eq!(
            upstream.cmp_with_position(1, Some(ScalarRefImpl::Utf8("a_z"))),
            Some(Ordering::Less)
        );
        assert_eq!(
            upstream.cmp_with_position(1, Some(ScalarRefImpl::Utf8("a"))),
            Some(Ordering::Less)
        );
    }

    #[tokio::test]
    async fn test_pk_position_cache_accepts_batch_larger_than_limit() {
        let mut upstream = UpstreamPkPositionOrder::new(&[CdcKeyComparison::Upstream]).unwrap();
        let position = OwnedRow::new(vec![text("B")]);
        let rows = (0..=MAX_CACHED_PK_POSITION_ORDERINGS)
            .map(|i| (Op::Insert, OwnedRow::new(vec![text(&format!("a{i}"))])))
            .collect::<Vec<_>>();
        let chunk = StreamChunk::from_rows(&rows, &[risingwave_common::types::DataType::Varchar]);
        let mut order = mock_order();
        upstream
            .resolve([&chunk], &[0], &position, &mut order)
            .await
            .unwrap();
        for (_, row) in &rows {
            assert_eq!(
                upstream.cmp_with_position(0, row.datum_at(0)),
                Some(Ordering::Less)
            );
        }
        // A large batch is retained until consumed, then evicted on the next resolve.
        let next = StreamChunk::from_pretty("T\n+ c");
        upstream
            .resolve([&next], &[0], &position, &mut order)
            .await
            .unwrap();
        assert_eq!(upstream.orderings[0].len(), 1);
        assert_eq!(
            upstream.cmp_with_position(0, Some(ScalarRefImpl::Utf8("c"))),
            Some(Ordering::Greater)
        );
    }

    #[tokio::test]
    async fn test_split_key_ranks_reject_bounds_out_of_upstream_order() {
        // Ordered as bytes, not upstream.
        let splits = [split(1, None, Some("B")), split(2, Some("B"), Some("a"))];
        let mut ranks = UpstreamSplitKeyRanks::new(&splits).unwrap();
        let chunk = StreamChunk::from_pretty(
            " T
            + a",
        );
        let error = ranks
            .resolve(&chunk, 0, &mut mock_order())
            .await
            .unwrap_err();
        let error = error.as_report().to_string();
        assert!(error.contains("not in upstream order"), "{error}");
    }

    #[test]
    fn test_no_upstream_columns() {
        assert!(UpstreamPkPositionOrder::new(&[CdcKeyComparison::Native]).is_none());
    }
}
