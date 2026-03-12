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

//! Spanner external table reader for CDC backfill.
//!
//! Provides snapshot reads using Spanner's [`BatchReadOnlyTransaction`] to ensure
//! all reads observe the same consistent snapshot. The snapshot timestamp is
//! obtained from Spanner at CREATE TABLE time (not at CREATE SOURCE time),
//! following the same pattern as Postgres CDC (`pg_current_wal_lsn()`) and
//! MySQL CDC (`SHOW MASTER STATUS`).

use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use base64::Engine;
use futures::stream::BoxStream;
use futures_async_stream::try_stream;
use google_cloud_spanner::client::{Client, ReadOnlyTransactionOption};
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::value::TimestampBound;

use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, F32, F64, ListType, ScalarImpl};

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CdcOffset, CdcTableSnapshotSplitOption, ExternalTableConfig, ExternalTableReader,
    SchemaTableName,
};

/// Spanner offset representing a position in the change stream.
///
/// Uses the commit timestamp (microseconds since epoch) as the ordering key.
/// Includes partition metadata for checkpoint / restoration.
#[derive(Debug, Clone, Default, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct SpannerOffset {
    /// Commit timestamp of the change stream position (microseconds since epoch).
    pub timestamp: i64,
    #[serde(default)]
    pub partition_token: Option<String>,
    #[serde(default)]
    pub parent_partition_tokens: Vec<String>,
    /// Highest timestamp processed by this partition (microseconds).
    pub offset: i64,
    #[serde(default)]
    pub stream_name: String,
    #[serde(default)]
    pub index: u32,
    #[serde(default)]
    pub is_finished: bool,
}

impl SpannerOffset {
    pub fn new(timestamp: i64) -> Self {
        Self {
            timestamp,
            offset: timestamp,
            ..Default::default()
        }
    }

    pub fn with_partition(
        offset: i64,
        partition_token: Option<String>,
        parent_partition_tokens: Vec<String>,
        timestamp: i64,
        stream_name: String,
        index: u32,
    ) -> Self {
        Self {
            timestamp,
            partition_token,
            parent_partition_tokens,
            offset,
            stream_name,
            index,
            is_finished: false,
        }
    }

    pub fn mark_finished(&mut self) {
        self.is_finished = true;
    }
}

// ---------------------------------------------------------------------------
// Schema discovery
// ---------------------------------------------------------------------------

/// Discovers table schema and primary keys from Spanner INFORMATION_SCHEMA.
pub struct SpannerExternalTable {
    column_descs: Vec<ColumnDesc>,
    pk_names: Vec<String>,
    table_name: String,
}

impl SpannerExternalTable {
    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        let table_name = config.table.clone();
        let client = create_spanner_client(
            &config.spanner_project,
            &config.spanner_instance,
            &config.database,
            config.emulator_host.as_deref(),
            config.credentials.as_deref(),
            config.credentials_path.as_deref(),
        )
        .await?;

        let column_descs = Self::discover_columns(&client, &table_name).await?;
        let pk_names = Self::discover_primary_keys(&client, &table_name).await?;

        if pk_names.is_empty() {
            bail!("table '{}' has no primary key (required for backfill)", table_name);
        }

        Ok(Self { column_descs, pk_names, table_name })
    }

    async fn discover_columns(
        client: &Client,
        table_name: &str,
    ) -> ConnectorResult<Vec<ColumnDesc>> {
        let mut stmt = Statement::new(
            "SELECT COLUMN_NAME, SPANNER_TYPE \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @p1 \
             ORDER BY ORDINAL_POSITION",
        );
        stmt.add_param("p1", &table_name);

        let mut txn = client.single().await.context("snapshot txn")?;
        let mut rows = txn.query(stmt).await.context("column query")?;

        let mut descs = Vec::new();
        while let Some(row) = rows.next().await.context("column row")? {
            let name: String = row.column_by_name("COLUMN_NAME").context("COLUMN_NAME")?;
            let spanner_type: String = row.column_by_name("SPANNER_TYPE").context("SPANNER_TYPE")?;
            let dt = spanner_type_to_rw_type(&spanner_type)?;
            descs.push(ColumnDesc::named(name, ColumnId::placeholder(), dt));
        }

        if descs.is_empty() {
            bail!("table '{}' not found", table_name);
        }
        Ok(descs)
    }

    async fn discover_primary_keys(
        client: &Client,
        table_name: &str,
    ) -> ConnectorResult<Vec<String>> {
        let mut stmt = Statement::new(
            "SELECT COLUMN_NAME \
             FROM INFORMATION_SCHEMA.INDEX_COLUMNS \
             WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @p1 AND INDEX_TYPE = 'PRIMARY_KEY' \
             ORDER BY ORDINAL_POSITION",
        );
        stmt.add_param("p1", &table_name);

        let mut txn = client.single().await.context("snapshot txn")?;
        let mut rows = txn.query(stmt).await.context("pk query")?;

        let mut names = Vec::new();
        while let Some(row) = rows.next().await.context("pk row")? {
            let name: String = row.column_by_name("COLUMN_NAME").context("COLUMN_NAME")?;
            names.push(name);
        }
        Ok(names)
    }

    pub fn column_descs(&self) -> &Vec<ColumnDesc> {
        &self.column_descs
    }

    pub fn pk_names(&self) -> &Vec<String> {
        &self.pk_names
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

// ---------------------------------------------------------------------------
// Snapshot reader
// ---------------------------------------------------------------------------

/// Reads snapshot data from Spanner for CDC backfill.
///
/// Implements two-level parallelism:
/// 1. **Inter-actor**: PK range splits distribute work across compute nodes
/// 2. **Intra-actor**: Within each PK range, uses Spanner's partition_query to
///    further parallelize reads using BatchReadOnlyTransaction
///
/// The snapshot timestamp is generated at table creation time (in the frontend)
/// and stored in `connect_properties` as `spanner.snapshot_ts`. This ensures
/// the same timestamp is used across all splits and all recoveries.
///
/// The snapshot timestamp serves as the CDC offset for filtering changes:
/// - Backfill reads at snapshot timestamp
/// - CDC streaming phase filters: commit_ts > snapshot_ts
pub struct SpannerExternalTableReader {
    rw_schema: Schema,
    field_names: Vec<String>,
    pk_names: Vec<String>,
    pk_types: Vec<DataType>,
    table_name: String,
    enable_databoost: bool,
    /// Parallelism for partition query execution within a PK range split
    partition_query_parallelism: u32,
    /// Snapshot timestamp from config (set by frontend at table creation).
    /// Used for consistent snapshot reads and CDC filtering.
    snapshot_ts: Option<i64>,
    /// Client for creating transactions. Transactions are created lazily per-operation.
    client: Client,
}

impl SpannerExternalTableReader {
    pub async fn new(
        config: ExternalTableConfig,
        schema: Schema,
    ) -> ConnectorResult<Self> {
        let enable_databoost = config.spanner_databoost_enabled;
        let partition_query_parallelism = config.spanner_partition_query_parallelism;
        let snapshot_ts = config.spanner_snapshot_ts;
        let client = create_spanner_client(
            &config.spanner_project,
            &config.spanner_instance,
            &config.database,
            config.emulator_host.as_deref(),
            config.credentials.as_deref(),
            config.credentials_path.as_deref(),
        )
        .await?;
        let external_table = SpannerExternalTable::connect(config).await?;

        let pk_names = external_table.pk_names().clone();
        let pk_types: Vec<DataType> = pk_names
            .iter()
            .map(|pk| {
                external_table
                    .column_descs()
                    .iter()
                    .find(|c| c.name.as_str() == pk.as_str())
                    .map(|c| c.data_type.clone())
                    .ok_or_else(|| anyhow!("pk column '{}' not in schema", pk))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let field_names = schema.fields().iter().map(|f| f.name.clone()).collect();

        Ok(Self {
            rw_schema: schema,
            field_names,
            pk_names,
            pk_types,
            table_name: external_table.table_name().to_string(),
            enable_databoost,
            partition_query_parallelism,
            snapshot_ts,
            client,
        })
    }
}

impl ExternalTableReader for SpannerExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        // Return the snapshot timestamp from config (set by frontend at table creation).
        // This serves as the CDC offset for filtering changes (commit_ts > snapshot_ts).
        let timestamp = self.snapshot_ts
            .ok_or_else(|| anyhow!("spanner.snapshot_ts not found in connect_properties"))?;
        Ok(CdcOffset::Spanner(SpannerOffset::new(timestamp)))
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner(table_name, start_pk, primary_keys, limit)
    }

    async fn disconnect(self) -> ConnectorResult<()> {
        Ok(())
    }

    fn get_parallel_cdc_splits(
        &self,
        options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>> {
        self.get_parallel_cdc_splits_inner(options)
    }

    fn split_snapshot_read(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.split_snapshot_read_inner(table_name, left, right, split_columns)
    }
}

impl SpannerExternalTableReader {
    /// Returns a function that parses CDC offset strings.
    ///
    /// Spanner CDC uses JSON-serialized `CdcOffset::Spanner` format.
    /// This is consistent with the offset format produced by both
    /// the backfill phase (`current_cdc_offset()`) and the CDC phase
    /// (`make_offset_string()` in reader.rs).
    pub fn get_cdc_offset_parser() -> crate::source::cdc::external::CdcOffsetParseFunc {
        Box::new(move |offset_str| {
            serde_json::from_str::<CdcOffset>(offset_str)
                .context("failed to parse Spanner CDC offset")
                .map_err(crate::error::ConnectorError::from)
        })
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        _table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        _primary_keys: Vec<String>,
        limit: u32,
    ) {
        let fields = self.rw_schema.fields();
        let columns_str = self.field_names.join(", ");

        let mut sql = format!("SELECT {} FROM {}", columns_str, self.table_name);
        if let Some(ref pk) = start_pk {
            let filter = build_pk_filter(&self.pk_names, pk, &self.pk_types, true)?;
            sql = format!("{} WHERE {}", sql, filter);
        }
        sql = format!("{} LIMIT {}", sql, limit);

        // Create a read-only transaction at the snapshot timestamp for consistency.
        // Note: We use regular read_only_transaction (not batch) because the query
        // has LIMIT clause which cannot be partitioned.
        let snapshot_timestamp = self.snapshot_ts
            .ok_or_else(|| anyhow!("spanner.snapshot_ts not found in connect_properties"))?;

        // Convert i64 (microseconds) to Timestamp
        let timestamp = google_cloud_spanner::value::Timestamp {
            seconds: snapshot_timestamp / 1_000_000,
            nanos: ((snapshot_timestamp % 1_000_000) * 1000) as i32,
        };
        let tb = TimestampBound::read_timestamp(timestamp);
        let options = ReadOnlyTransactionOption {
            timestamp_bound: tb,
            call_options: Default::default(),
        };
        let mut txn = self.client
            .read_only_transaction_with_option(options)
            .await
            .context("failed to create read-only transaction at snapshot timestamp")?;

        // Execute the query directly (no partition, since LIMIT queries can't be partitioned)
        let mut rows = txn
            .query(Statement::new(&sql))
            .await
            .context("snapshot query failed")?;

        while let Some(row) = rows.next().await.context("row read failed")? {
            yield spanner_row_to_owned_row(&row, &fields)?;
        }
    }

    /// Generate parallel splits based on primary key ranges.
    ///
    /// Follows Postgres CDC's split strategy exactly:
    /// - Uses `backfill_split_pk_column_index` to select split column (not always first PK)
    /// - For numeric types (Int16/32/64): uses even splits with computed numeric boundaries
    /// - For other types (Varchar, etc.): uses uneven splits with data-driven sampling
    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn get_parallel_cdc_splits_inner(&self, cdc_split_options: CdcTableSnapshotSplitOption) {
        // Create a read-only transaction at the snapshot timestamp for consistency.
        // Note: We use regular read_only_transaction (not batch) because the split
        // enumeration queries (MIN/MAX, LIMIT) cannot be partitioned anyway.
        let snapshot_timestamp = self.snapshot_ts
            .ok_or_else(|| anyhow!("spanner.snapshot_ts not found in connect_properties - table must be recreated"))?;

        tracing::info!(
            snapshot_timestamp_us = snapshot_timestamp,
            "PK range enumeration started"
        );

        // Convert i64 (microseconds) to Timestamp
        let timestamp = google_cloud_spanner::value::Timestamp {
            seconds: snapshot_timestamp / 1_000_000,
            nanos: ((snapshot_timestamp % 1_000_000) * 1000) as i32,
        };
        let tb = TimestampBound::read_timestamp(timestamp);
        let txn_options = ReadOnlyTransactionOption {
            timestamp_bound: tb,
            call_options: Default::default(),
        };
        let mut txn = self.client
            .read_only_transaction_with_option(txn_options)
            .await
            .context("failed to create read-only transaction at snapshot timestamp")?;

        // Get the split column (follows Postgres CDC's split_column pattern)
        let split_column_idx = cdc_split_options.backfill_split_pk_column_index as usize;
        if split_column_idx >= self.pk_names.len() {
            bail!(
                "invalid backfill_split_pk_column_index {}, out of bound ({} pk columns)",
                split_column_idx,
                self.pk_names.len()
            );
        }
        let split_column_name = &self.pk_names[split_column_idx];
        let split_column_type = &self.pk_types[split_column_idx];

        // Query MIN/MAX split column values.
        // Note: MIN/MAX queries cannot use partition_query (not root-partitionable),
        // so they don't benefit from DataBoost. But they DO use the snapshot timestamp
        // for read consistency.
        let minmax_query = format!(
            "SELECT MIN(`{}`) as min_val, MAX(`{}`) as max_val FROM `{}`",
            split_column_name, split_column_name, self.table_name
        );

        let stmt = Statement::new(&minmax_query);
        let mut rows = txn.query(stmt).await.context("PK range query failed")?;

        let (min_val, max_val) = if let Some(row) = rows.next().await.context("PK range row failed")? {
            let min_val: Option<String> = row.column_by_name("min_val").ok();
            let max_val: Option<String> = row.column_by_name("max_val").ok();
            (min_val, max_val)
        } else {
            // Table is empty, return a single empty split
            yield CdcTableSnapshotSplit {
                split_id: 1,
                left_bound_inclusive: OwnedRow::new(vec![None]),
                right_bound_exclusive: OwnedRow::new(vec![None]),
            };
            return Ok(());
        };

        let (min_val, max_val) = match (min_val, max_val) {
            (Some(min), Some(max)) => (min, max),
            _ => {
                // Table is empty or NULL PKs, return a single empty split
                yield CdcTableSnapshotSplit {
                    split_id: 1,
                    left_bound_inclusive: OwnedRow::new(vec![None]),
                    right_bound_exclusive: OwnedRow::new(vec![None]),
                };
                return Ok(());
            }
        };

        tracing::info!(
            "PK range: min={}, max={}, type={:?}",
            min_val, max_val, split_column_type
        );

        // Follow Postgres CDC's split strategy
        if cdc_split_options.backfill_as_even_splits
            && is_supported_even_split_data_type(split_column_type)
        {
            // For integer types, use evenly-sized partitions (follows Postgres as_even_splits)
            tracing::info!(table_name = %self.table_name, ?split_column_name, "Using even splits");
            let min_i64 = min_val.parse::<i64>()
                .context(format!("failed to parse min_val '{}' as i64", min_val))?;
            let max_i64 = max_val.parse::<i64>()
                .context(format!("failed to parse max_val '{}' as i64", max_val))?;

            let saturated_split_max_size = cdc_split_options.backfill_num_rows_per_split.try_into().unwrap_or(i64::MAX);
            let mut left: Option<i64> = None;
            let mut right: Option<i64> = Some(min_i64.saturating_add(saturated_split_max_size));
            let mut split_id = 1i64;

            loop {
                let mut is_completed = false;
                if right.as_ref().map(|r| *r >= max_i64).unwrap_or(true) {
                    right = None;
                    is_completed = true;
                }

                let split = CdcTableSnapshotSplit {
                    split_id,
                    left_bound_inclusive: OwnedRow::new(vec![
                        left.map(|l| to_int_scalar(l, split_column_type)),
                    ]),
                    right_bound_exclusive: OwnedRow::new(vec![
                        right.map(|r| to_int_scalar(r, split_column_type)),
                    ]),
                };

                split_id += 1;
                yield split;

                if is_completed {
                    break;
                }

                left = right;
                right = left.map(|l| l.saturating_add(saturated_split_max_size));
            }
            return Ok(());
        }

        // For other types, use data-driven sampling (follows Postgres as_uneven_splits)
        tracing::info!(table_name = %self.table_name, ?split_column_name, "Using uneven splits");

        let mut split_id = 1i64;
        let mut next_left_bound_inclusive = min_val.clone();
        let table_name = &self.table_name;

        loop {
            let left_bound_inclusive: Option<String> = if next_left_bound_inclusive == min_val {
                None
            } else {
                Some(next_left_bound_inclusive.clone())
            };

            // Get right bound by fetching backfill_num_rows_per_split rows
            // Follows Postgres CDC's next_split_right_bound_exclusive
            let sql = if left_bound_inclusive.is_none() {
                // First split: no left bound
                format!(
                    "SELECT `{}` FROM `{}` ORDER BY `{}` ASC LIMIT {}",
                    split_column_name, table_name, split_column_name, cdc_split_options.backfill_num_rows_per_split
                )
            } else {
                // Subsequent splits: start from left_value
                format!(
                    "SELECT `{}` FROM `{}` WHERE `{}` > '{}' ORDER BY `{}` ASC LIMIT {}",
                    split_column_name,
                    table_name,
                    split_column_name,
                    escape_string(&next_left_bound_inclusive),
                    split_column_name,
                    cdc_split_options.backfill_num_rows_per_split
                )
            };

            let stmt = Statement::new(&sql);
            // Note: LIMIT queries cannot use partition_query (not root-partitionable),
            // so they don't benefit from DataBoost. But they DO use the snapshot timestamp
            // for read consistency.
            let mut rows = txn.query(stmt).await.context("boundary query failed")?;

            // Get the last row's PK value as the split boundary
            let mut last_val: Option<String> = None;
            while let Some(row) = rows.next().await.context("boundary row fetch failed")? {
                last_val = row.column_by_name(split_column_name).ok().flatten();
            }

            // Safeguard: if boundary equals left bound (all rows have same PK value),
            // find next distinct greater value (follows Postgres CDC's next_greater_bound)
            let mut next_right = Some(last_val);
            if let Some(ref inner) = next_right {
                let left_val = left_bound_inclusive.as_ref().unwrap_or(&min_val);
                if inner.as_ref() == Some(left_val) {
                    // All rows_per_split rows have the same PK value - find next distinct
                    let greater_sql = format!(
                        "SELECT MIN(`{}`) FROM `{}` WHERE `{}` > '{}' AND `{}` < '{}'",
                        split_column_name,
                        table_name,
                        split_column_name,
                        escape_string(left_val),
                        split_column_name,
                        escape_string(&max_val)
                    );
                    let greater_stmt = Statement::new(&greater_sql);
                    // Note: MIN() aggregate query cannot use partition_query
                    let mut greater_rows = txn.query(greater_stmt).await.context("next_greater_bound query failed")?;
                    if let Some(row) = greater_rows.next().await.context("next_greater_bound row fetch failed")? {
                        next_right = row.column_by_name(split_column_name).ok().flatten();
                    } else {
                        next_right = None;
                    }
                }
            }

            let right_bound_exclusive = next_right.flatten();
            let is_completed = right_bound_exclusive.is_none();

            if is_completed && left_bound_inclusive.is_none() {
                assert_eq!(split_id, 1);
            }

            tracing::info!(
                split_id,
                ?left_bound_inclusive,
                ?right_bound_exclusive,
                "New CDC table snapshot split"
            );

            let left_bound_row = OwnedRow::new(vec![
                left_bound_inclusive.clone().map(|s| ScalarImpl::Utf8(s.into()))
            ]);
            let right_bound_row = OwnedRow::new(vec![
                right_bound_exclusive.clone().map(|s| ScalarImpl::Utf8(s.into()))
            ]);

            let split = CdcTableSnapshotSplit {
                split_id,
                left_bound_inclusive: left_bound_row,
                right_bound_exclusive: right_bound_row,
            };

            split_id += 1;
            yield split;

            if let Some(ref right) = right_bound_exclusive {
                next_left_bound_inclusive = right.clone();
            }

            if is_completed {
                break;
            }
        }
    }

    /// Read a split using PK range-based filtering with intra-actor parallelism.
    ///
    /// Two-level parallelism:
    /// 1. **Inter-actor**: This split is one PK range assigned to this actor
    /// 2. **Intra-actor**: Within this PK range, uses Spanner's partition_query
    ///    to further parallelize reads
    ///
    /// Creates a NEW BatchReadOnlyTransaction at the snapshot timestamp from config
    /// (set by frontend at table creation), then uses partition_query with WHERE
    /// clause filtering for intra-actor parallelism.
    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn split_snapshot_read_inner(
        &self,
        _table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) {
        let fields = self.rw_schema.fields();

        // Use the split column from parameters (follows Postgres CDC pattern)
        // The split column is determined by backfill_split_pk_column_index
        assert_eq!(
            split_columns.len(),
            1,
            "multiple split columns is not supported yet"
        );
        let split_column_name = &split_columns[0].name;
        let split_column_type = &split_columns[0].data_type;

        // Extract PK range from split boundary (PK-only bounds, no timestamp)
        // Format: [pk_start, ...] or [None] for unbounded
        let (pk_start_str, pk_end_str) = match (&left[0], &right[0]) {
            // Int64 PK boundaries
            (Some(ScalarImpl::Int64(start)), Some(ScalarImpl::Int64(end))) => {
                (start.to_string(), end.to_string())
            }
            (Some(ScalarImpl::Int64(start)), None) => {
                (start.to_string(), String::new())
            }
            (None, Some(ScalarImpl::Int64(end))) => {
                (String::new(), end.to_string())
            }
            (None, None) => {
                (String::new(), String::new())
            }
            // Varchar/String PK boundaries
            (Some(ScalarImpl::Utf8(start)), Some(ScalarImpl::Utf8(end))) => {
                (start.as_ref().to_string(), end.as_ref().to_string())
            }
            (Some(ScalarImpl::Utf8(start)), None) => {
                (start.as_ref().to_string(), String::new())
            }
            (None, Some(ScalarImpl::Utf8(end))) => {
                (String::new(), end.as_ref().to_string())
            }
            _ => {
                bail!("invalid split boundary: expected [pk_start (Int64 or Utf8), pk_end (Int64 or Utf8)], got [{:?}, {:?}]", left[0], right[0]);
            }
        };

        // Get snapshot timestamp from reader state (set from config.spanner_snapshot_ts)
        let split_snapshot_ts = self.snapshot_ts
            .ok_or_else(|| anyhow!("spanner.snapshot_ts not found in connect_properties"))?;

        tracing::info!(
            "split_snapshot_read: PK range=[{}, {}), snapshot_ts={}, pk_type={:?}",
            pk_start_str, pk_end_str, split_snapshot_ts, split_column_type
        );

        // Create a NEW BatchReadOnlyTransaction at the exact same snapshot timestamp
        // This is the key to snapshot consistency across all actors
        use google_cloud_spanner::value::Timestamp;
        let timestamp = Timestamp {
            seconds: split_snapshot_ts / 1_000_000,
            nanos: ((split_snapshot_ts % 1_000_000) * 1000) as i32,
        };
        let tb = TimestampBound::read_timestamp(timestamp);
        let options = ReadOnlyTransactionOption {
            timestamp_bound: tb,
            call_options: Default::default(),
        };

        let mut txn = self.client
            .batch_read_only_transaction_with_option(options)
            .await
            .context("failed to create batch read-only transaction at snapshot timestamp")?;

        // Verify the transaction's read timestamp matches
        let actual_ts = (txn
            .rts
            .context("read timestamp not available")?
            .unix_timestamp_nanos()
            / 1000) as i64;

        if actual_ts != split_snapshot_ts {
            bail!(
                "snapshot timestamp mismatch: expected {}, got {}",
                split_snapshot_ts,
                actual_ts
            );
        }

        // Build query with PK range WHERE clause (single-column comparison)
        // Follows Postgres CDC pattern: split on single column specified by backfill_split_pk_column_index
        let columns_str = self.field_names.join(", ");
        let where_clause = if pk_start_str.is_empty() && pk_end_str.is_empty() {
            // No bounds - full table scan
            String::new()
        } else if pk_start_str.is_empty() {
            // Unbounded left side: WHERE pk < end
            if *split_column_type == DataType::Varchar {
                format!("WHERE `{}` < '{}'", split_column_name, pk_end_str.replace('\'', "''"))
            } else {
                format!("WHERE `{}` < {}", split_column_name, pk_end_str)
            }
        } else if pk_end_str.is_empty() {
            // Unbounded right side: WHERE pk >= start
            if *split_column_type == DataType::Varchar {
                format!("WHERE `{}` >= '{}'", split_column_name, pk_start_str.replace('\'', "''"))
            } else {
                format!("WHERE `{}` >= {}", split_column_name, pk_start_str)
            }
        } else {
            // Bounded range: WHERE pk >= start AND pk < end
            if *split_column_type == DataType::Varchar {
                format!("WHERE `{}` >= '{}' AND `{}` < '{}'",
                    split_column_name, pk_start_str.replace('\'', "''"),
                    split_column_name, pk_end_str.replace('\'', "''"))
            } else {
                format!("WHERE `{}` >= {} AND `{}` < {}", split_column_name, pk_start_str, split_column_name, pk_end_str)
            }
        };

        tracing::info!(
            "split_snapshot_read: executing partition_query with databoost={}, sql={}",
            self.enable_databoost, where_clause
        );

        let sql = if where_clause.is_empty() {
            format!("SELECT {} FROM {}", columns_str, self.table_name)
        } else {
            format!("SELECT {} FROM {} {}", columns_str, self.table_name, where_clause)
        };

        tracing::info!(
            "split_snapshot_read: executing partition_query with databoost={}, sql={}",
            self.enable_databoost, sql
        );

        // Use partition_query for intra-actor parallelism
        // Spanner will create multiple partitions within this PK range
        let stmt = Statement::new(&sql);
        let partitions = txn
            .partition_query_with_option(
                stmt,
                None,
                google_cloud_spanner::transaction::QueryOptions::default(),
                self.enable_databoost,
                None,
            )
            .await
            .context("failed to get partitions for PK range")?;

        tracing::info!(
            "split_snapshot_read: got {} intra-partitions for PK range [{}, {}), executing with parallelism={}",
            partitions.len(), pk_start_str, pk_end_str, self.partition_query_parallelism
        );

        // Execute partitions concurrently using Arc<Mutex<>> pattern
        // Same as Python's ThreadPoolExecutor with shared snapshot
        use futures::stream::{StreamExt, TryStreamExt};
        use tokio::sync::Mutex;

        let txn = Arc::new(Mutex::new(txn));

        // Stream partitions and execute them concurrently
        let results = futures::stream::iter(partitions)
            .map(|partition| {
                let txn = Arc::clone(&txn);
                async move {
                    let mut txn_guard = txn.lock().await;
                    let mut rows = txn_guard
                        .execute(partition, None)
                        .await
                        .context("failed to execute partition")?;

                    let mut partition_rows = Vec::new();
                    while let Some(row) = rows.next().await.context("row read failed")? {
                        partition_rows.push(row);
                    }
                    Ok::<_, ConnectorError>(partition_rows)
                }
            })
            .buffered(self.partition_query_parallelism as usize)
            .try_collect::<Vec<_>>()
            .await?;

        // Yield all rows from all partitions
        for partition_rows in results {
            for row in partition_rows {
                yield spanner_row_to_owned_row(&row, &fields)?;
            }
        }
    }
}

/// Check if the data type supports even-split (numeric range-based) splitting.
///
/// This follows Postgres CDC's approach: only integer types can use computed
/// numeric boundaries. All other types (Varchar, text, UUID, etc.) require
/// data-driven sampling to find split points.
fn is_supported_even_split_data_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Int16 | DataType::Int32 | DataType::Int64)
}

/// Convert i64 to ScalarImpl based on data type (follows Postgres CDC's to_int_scalar)
fn to_int_scalar(i: i64, data_type: &DataType) -> ScalarImpl {
    match data_type {
        DataType::Int16 => ScalarImpl::Int16(i.try_into().unwrap()),
        DataType::Int32 => ScalarImpl::Int32(i.try_into().unwrap()),
        DataType::Int64 => ScalarImpl::Int64(i),
        _ => {
            panic!("Can't convert int {} to ScalarImpl::{}", i, data_type)
        }
    }
}

/// Escape single quotes in a string for SQL
fn escape_string(s: &str) -> String {
    s.replace('\'', "''")
}

// ---------------------------------------------------------------------------
// Spanner client factory
// ---------------------------------------------------------------------------

/// Create a Spanner client from connection parameters.
///
/// Shared by both `SpannerExternalTable` (backfill) and `SpannerCdcProperties` (CDC reader).
pub(crate) async fn create_spanner_client(
    project: &str,
    instance: &str,
    database: &str,
    emulator_host: Option<&str>,
    credentials: Option<&str>,
    credentials_path: Option<&str>,
) -> ConnectorResult<Client> {
    use google_cloud_spanner::client::ClientConfig;

    if project.is_empty() || instance.is_empty() || database.is_empty() {
        bail!("spanner.project, spanner.instance, and database.name are required");
    }

    if let Some(host) = emulator_host {
        unsafe { std::env::set_var("SPANNER_EMULATOR_HOST", host) };
    }
    if let Some(path) = credentials_path {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read credentials file: {}", path))?;
        unsafe { std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS_JSON", content) };
    } else if let Some(json) = credentials {
        unsafe { std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS_JSON", json) };
    }

    let client_config = ClientConfig::default()
        .with_auth()
        .await
        .context("Spanner auth failed")?;

    let dsn = format!("projects/{}/instances/{}/databases/{}", project, instance, database);

    Ok(Client::new(&dsn, client_config)
        .await
        .context("failed to create Spanner client")?)
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

/// Map a Spanner SQL type string to a RisingWave [`DataType`].
///
/// Reference: <https://cloud.google.com/spanner/docs/reference/standard-sql/data-types>
///
/// ## Type Mapping Strategy
///
/// We map all Spanner types to RisingWave types with NO data loss:
/// - Primitive types: Direct mapping
/// - PROTO/ENUM: Stored as BYTEA (raw bytes preserved)
/// - STRUCT: Stored as JSONB (serialized structure preserved)
/// - ARRAY: Stored as List (element-wise mapping)
/// - INTERVAL/TIME: Stored as VARCHAR (text representation preserved)
pub(crate) fn spanner_type_to_rw_type(spanner_type: &str) -> ConnectorResult<DataType> {
    if let Some(rest) = spanner_type.strip_prefix("ARRAY<") {
        let inner = rest
            .strip_suffix('>')
            .ok_or_else(|| anyhow!("invalid ARRAY type: {}", spanner_type))?;
        return Ok(DataType::List(ListType::new(spanner_type_to_rw_type(inner)?)));
    }

    // Handle STRUCT type - map to JSONB for serialization
    if spanner_type.starts_with("STRUCT") {
        return Ok(DataType::Jsonb);
    }

    let base = spanner_type.split('(').next().unwrap_or(spanner_type);
    match base {
        "BOOL" | "BOOLEAN" => Ok(DataType::Boolean),
        "INT64" => Ok(DataType::Int64),
        "FLOAT64" => Ok(DataType::Float64),
        "FLOAT32" => Ok(DataType::Float32),
        "STRING" => Ok(DataType::Varchar),
        "BYTES" => Ok(DataType::Bytea),
        "TIMESTAMP" => Ok(DataType::Timestamptz),
        "DATE" => Ok(DataType::Date),
        "NUMERIC" => Ok(DataType::Decimal),
        "JSON" => Ok(DataType::Jsonb),
        // PROTO types - store as raw bytes (data preserved, can be deserialized later)
        t if t.starts_with("PROTO") => {
            tracing::info!("mapping PROTO type '{}' to BYTEA (raw bytes preserved)", spanner_type);
            Ok(DataType::Bytea)
        }
        // ENUM types - store as VARCHAR (enum name preserved)
        t if t.starts_with("ENUM") => {
            tracing::info!("mapping ENUM type '{}' to VARCHAR (enum name preserved)", spanner_type);
            Ok(DataType::Varchar)
        }
        // INTERVAL - store as VARCHAR (text representation)
        "INTERVAL" => {
            tracing::info!("mapping INTERVAL type to VARCHAR (text representation)");
            Ok(DataType::Varchar)
        }
        // TIME - store as VARCHAR (text representation)
        "TIME" => {
            tracing::info!("mapping TIME type to VARCHAR (text representation)");
            Ok(DataType::Varchar)
        }
        // Unknown types - log and map to VARCHAR as safe fallback
        _ => {
            tracing::warn!("unknown Spanner type '{}' mapped to VARCHAR as fallback", spanner_type);
            Ok(DataType::Varchar)
        }
    }
}

// ---------------------------------------------------------------------------
// Row / value helpers
// ---------------------------------------------------------------------------

fn build_pk_filter(
    pk_names: &[String],
    pk_row: &OwnedRow,
    pk_types: &[DataType],
    is_lower_bound: bool,
) -> ConnectorResult<String> {
    let op = if is_lower_bound { ">=" } else { "<" };
    let mut parts = Vec::with_capacity(pk_names.len());
    for (i, name) in pk_names.iter().enumerate() {
        let val = datum_to_sql_literal(pk_row[i].clone(), &pk_types[i])?;
        parts.push(format!("{} {} {}", name, op, val));
    }
    Ok(parts.join(" AND "))
}

fn datum_to_sql_literal(datum: Datum, data_type: &DataType) -> ConnectorResult<String> {
    let Some(scalar) = datum else {
        return Ok("NULL".to_string());
    };
    let s = match (scalar, data_type) {
        (ScalarImpl::Int64(v), _) => v.to_string(),
        (ScalarImpl::Int32(v), _) => v.to_string(),
        (ScalarImpl::Int16(v), _) => v.to_string(),
        (ScalarImpl::Float64(v), _) => v.to_string(),
        (ScalarImpl::Float32(v), _) => v.to_string(),
        (ScalarImpl::Bool(v), _) => v.to_string(),
        (ScalarImpl::Utf8(v), _) => format!("'{}'", v.replace('\'', "''")),
        (ScalarImpl::Bytea(v), _) => {
            format!(
                "CASTFROMBYTES('{}')",
                base64::engine::general_purpose::STANDARD.encode(v.as_ref())
            )
        }
        (ScalarImpl::Jsonb(v), _) => format!("'{}'", v.to_string().replace('\'', "''")),
        (ScalarImpl::Timestamptz(v), _) => format!("'{}'", v),
        (ScalarImpl::Date(v), _) => format!("'{}'", v),
        _ => bail!("unsupported datum type for SQL literal: {:?}", data_type),
    };
    Ok(s)
}

fn spanner_row_to_owned_row(
    row: &google_cloud_spanner::row::Row,
    fields: &[Field],
) -> ConnectorResult<OwnedRow> {
    let mut values = Vec::with_capacity(fields.len());
    for f in fields {
        // Read value based on expected RisingWave type
        // The google_cloud_spanner library's TryFromValue trait handles the conversion
        // from Spanner's internal representation to Rust types
        let datum = match f.data_type {
            DataType::Boolean => row
                .column_by_name::<bool>(&f.name)
                .ok()
                .map(ScalarImpl::Bool),

            DataType::Int64 => row
                .column_by_name::<i64>(&f.name)
                .ok()
                .map(ScalarImpl::Int64),

            DataType::Int32 => row
                .column_by_name::<i64>(&f.name)
                .ok()
                .map(|v| ScalarImpl::Int32(v as i32)),

            DataType::Int16 => row
                .column_by_name::<i64>(&f.name)
                .ok()
                .map(|v| ScalarImpl::Int16(v as i16)),

            DataType::Float64 => row
                .column_by_name::<f64>(&f.name)
                .ok()
                .map(|v| ScalarImpl::Float64(F64::from(v))),

            DataType::Float32 => row
                .column_by_name::<f64>(&f.name)
                .ok()
                .map(|v| ScalarImpl::Float32(F32::from(v as f32))),

            DataType::Varchar => {
                // Handle VARCHAR, ENUM (stored as string), TIME, INTERVAL
                row.column_by_name::<String>(&f.name)
                    .ok()
                    .map(|s| ScalarImpl::Utf8(s.into()))
            }

            DataType::Bytea => {
                // Handle BYTEA and PROTO types (stored as bytes)
                // Try reading as Vec<u8> first (for PROTO)
                if let Ok(v) = row.column_by_name::<Vec<u8>>(&f.name) {
                    Some(ScalarImpl::Bytea(v.into()))
                } else {
                    // Fallback: read as string (for BYTES stored as base64 string)
                    row.column_by_name::<String>(&f.name)
                        .ok()
                        .map(|s| ScalarImpl::Bytea(s.into_bytes().into()))
                }
            }

            DataType::Timestamptz => {
                use risingwave_common::types::Timestamptz;
                row.column_by_name::<time::OffsetDateTime>(&f.name)
                    .ok()
                    .map(|v| {
                        let micros = (v.unix_timestamp_nanos() / 1000) as i64;
                        ScalarImpl::Timestamptz(Timestamptz::from_micros(micros))
                    })
            }

            DataType::Timestamp => {
                use risingwave_common::types::Timestamp;
                row.column_by_name::<time::OffsetDateTime>(&f.name)
                    .ok()
                    .map(|v| {
                        let micros = (v.unix_timestamp_nanos() / 1000) as i64;
                        ScalarImpl::Timestamp(Timestamp::with_micros(micros).unwrap())
                    })
            }

            DataType::Date => {
                use risingwave_common::types::Date;
                row.column_by_name::<time::Date>(&f.name)
                    .ok()
                    .and_then(|v| {
                        let s = format!("{:04}-{:02}-{:02}", v.year(), v.month(), v.day());
                        Date::from_str(&s).ok()
                    })
                    .map(ScalarImpl::Date)
            }

            DataType::Decimal => {
                use risingwave_common::types::Decimal;
                // Numeric is returned as BigDecimal by the spanner library
                // Read as string first (Spanner stores NUMERIC as string)
                row.column_by_name::<String>(&f.name)
                    .ok()
                    .and_then(|s| Decimal::from_str(&s).ok())
                    .map(ScalarImpl::Decimal)
            }

            DataType::Jsonb => {
                // Handle JSON and STRUCT types (stored as JSON string)
                row.column_by_name::<String>(&f.name)
                    .ok()
                    .and_then(|s| {
                        // Try to parse as JSON first
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&s) {
                            Some(ScalarImpl::Jsonb(json.into()))
                        } else {
                            // If not valid JSON, wrap as string
                            Some(ScalarImpl::Jsonb(
                                serde_json::json!(s).into()
                            ))
                        }
                    })
            }

            DataType::List(_) => {
                // Handle ARRAY types - read as JSON array and convert
                // The spanner library returns arrays as JSON strings
                row.column_by_name::<String>(&f.name)
                    .ok()
                    .and_then(|s| {
                        // Parse the JSON array string
                        if let Ok(json_arr) = serde_json::from_str::<serde_json::Value>(&s) {
                            // Return as JSONB for now (preserves all data)
                            Some(ScalarImpl::Jsonb(json_arr.into()))
                        } else {
                            None
                        }
                    })
            }

            // Unknown or unsupported types - try to read as string as fallback
            _ => {
                tracing::warn!(
                    "column '{}' has unsupported type {:?} - reading as string fallback",
                    f.name,
                    f.data_type
                );
                row.column_by_name::<String>(&f.name)
                    .ok()
                    .map(|s| ScalarImpl::Utf8(s.into()))
            }
        };
        values.push(datum);
    }
    Ok(OwnedRow::new(values))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spanner_offset() {
        let offset = SpannerOffset::new(1234567890);
        assert_eq!(offset.timestamp, 1234567890);
    }

    #[test]
    fn test_spanner_type_to_rw_type() {
        assert!(matches!(spanner_type_to_rw_type("BOOL").unwrap(), DataType::Boolean));
        assert!(matches!(spanner_type_to_rw_type("INT64").unwrap(), DataType::Int64));
        assert!(matches!(spanner_type_to_rw_type("STRING").unwrap(), DataType::Varchar));
        assert!(matches!(spanner_type_to_rw_type("ARRAY<INT64>").unwrap(), DataType::List(_)));
    }
}
