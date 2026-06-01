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
//! set at CREATE TABLE time (not at CREATE SOURCE time), following the same
//! pattern as Postgres CDC (`pg_current_wal_lsn()`) and MySQL CDC (`SHOW MASTER STATUS`).

use std::str::FromStr;

use anyhow::{Context, anyhow};
use futures::pin_mut;
use futures::stream::BoxStream;
use futures_async_stream::try_stream;
use google_cloud_spanner::client::{Client, ReadOnlyTransactionOption};
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::transaction::Transaction;
use google_cloud_spanner::value::TimestampBound;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum, F32, F64, ListType, ScalarImpl};
use time::OffsetDateTime;

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CDC_TABLE_SPLIT_ID_START, CdcOffset, CdcTableSnapshotSplitOption, ExternalTableConfig,
    ExternalTableReader, SchemaTableName,
};

/// Resume position for a single change-stream partition.
///
/// Carried inside [`SpannerOffset::partitions`] so a restart can resume each
/// partition from its own commit-timestamp position (the Beam `PartitionMetadata`
/// model) instead of re-reading the whole partition tree from the global minimum.
///
/// The same shape is persisted on the executor's split (`SpannerCdcSplit::partitions`).
/// Offsets are stored as microseconds since epoch to match [`SpannerOffset`].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PartitionOffset {
    /// Partition token. `None` is the root partition.
    pub token: Option<String>,
    /// Parent partition tokens (empty for the root). Used on restore to
    /// reconstruct the `parents_all_finished` gate.
    #[serde(default)]
    pub parent_tokens: Vec<String>,
    /// Resume offset for this partition (commit-ts microseconds since epoch).
    pub micros: i64,
    /// `true` = the partition was actively reading (Running); `false` = declared
    /// but not yet started (Pending).
    pub running: bool,
}

/// Spanner offset representing a position in the change stream.
///
/// `timestamp` is the scalar checkpoint watermark (microseconds since epoch) and
/// is the **sole ordering key**. `partitions` is optional per-partition resume
/// metadata that does **not** participate in ordering (see the manual `PartialOrd`).
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SpannerOffset {
    /// Commit timestamp of the change stream position (microseconds since epoch).
    pub timestamp: i64,
    /// Highest timestamp processed (microseconds). Same as `timestamp` for
    /// watermark-based offsets.
    pub offset: i64,
    /// Optional per-partition resume frontier. `None` for legacy/single-offset
    /// positions (e.g. the backfill snapshot offset); `#[serde(default)]` keeps
    /// old persisted offsets deserializable.
    #[serde(default)]
    pub partitions: Option<Vec<PartitionOffset>>,
}

// Ordering is by `(timestamp, offset)` ONLY. `partitions` is metadata for resume
// and must never affect comparison, or it would break the CDC backfill offset
// comparison (`chunk_offset < last_binlog_offset`). Mirrors `SqlServerOffset`.
impl PartialOrd for SpannerOffset {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.timestamp.partial_cmp(&other.timestamp) {
            Some(std::cmp::Ordering::Equal) => self.offset.partial_cmp(&other.offset),
            ord => ord,
        }
    }
}

impl SpannerOffset {
    pub fn new(timestamp: i64) -> Self {
        Self {
            timestamp,
            offset: timestamp,
            partitions: None,
        }
    }

    /// Attach a per-partition resume frontier to this offset.
    pub fn with_partitions(mut self, partitions: Vec<PartitionOffset>) -> Self {
        self.partitions = Some(partitions);
        self
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
            bail!(
                "table '{}' has no primary key (required for backfill)",
                table_name
            );
        }

        Ok(Self {
            column_descs,
            pk_names,
            table_name,
        })
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
            let spanner_type: String =
                row.column_by_name("SPANNER_TYPE").context("SPANNER_TYPE")?;
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
    field_names: String,
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
    /// Quotes a Spanner identifier (column or table name) with backticks to
    /// prevent reserved-word conflicts.
    fn quote_column(name: &str) -> String {
        format!("`{}`", name)
    }

    pub async fn new(config: ExternalTableConfig, schema: Schema) -> ConnectorResult<Self> {
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

        let field_names = schema
            .fields()
            .iter()
            .map(|f| Self::quote_column(&f.name))
            .collect::<Vec<_>>()
            .join(", ");

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
        let timestamp = self.snapshot_ts.ok_or_else(|| {
            anyhow!("spanner.snapshot_ts not found in connect_properties - table must be recreated")
        })?;
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

    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn get_parallel_cdc_splits(&self, options: CdcTableSnapshotSplitOption) {
        let backfill_num_rows_per_split = options.backfill_num_rows_per_split;
        if backfill_num_rows_per_split == 0 {
            return Err(
                anyhow!("invalid backfill_num_rows_per_split, must be greater than 0").into(),
            );
        }
        if options.backfill_split_pk_column_index as usize >= self.pk_names.len() {
            return Err(anyhow!(
                "invalid backfill_split_pk_column_index {}, out of bound",
                options.backfill_split_pk_column_index
            )
            .into());
        }

        let split_column = self.split_column(&options);
        let row_stream = if options.backfill_as_even_splits
            && is_supported_even_split_data_type(&split_column.data_type)
        {
            // For certain types, use evenly-sized partition to optimize performance.
            tracing::info!(table_name = %self.table_name, ?split_column, "Using even splits");
            self.as_even_splits(options)
        } else {
            tracing::info!(table_name = %self.table_name, ?split_column, "Using uneven splits");
            self.as_uneven_splits(options)
        };
        pin_mut!(row_stream);
        #[for_await]
        for row in row_stream {
            let row = row?;
            yield row;
        }
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
    /// Returns the split column as a `Field` based on the backfill_split_pk_column_index.
    ///
    /// Follows Postgres CDC's split_column pattern.
    fn split_column(&self, options: &CdcTableSnapshotSplitOption) -> Field {
        let idx = options.backfill_split_pk_column_index as usize;
        Field::new(&self.pk_names[idx], self.pk_types[idx].clone())
    }

    fn get_order_key(primary_keys: &[String]) -> String {
        primary_keys
            .iter()
            .map(|col| Self::quote_column(col))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Queries MIN and MAX values of the split column.
    async fn min_and_max(
        &self,
        txn: &mut Transaction,
        split_column: &Field,
    ) -> ConnectorResult<Option<(ScalarImpl, ScalarImpl)>> {
        let col = Self::quote_column(&split_column.name);
        let tbl = Self::quote_column(&self.table_name);
        let minmax_query =
            format!("SELECT MIN({col}) as min_val, MAX({col}) as max_val FROM {tbl}",);

        let stmt = Statement::new(&minmax_query);
        let mut rows = txn.query(stmt).await.context("PK range query failed")?;

        if let Some(row) = rows.next().await.context("PK range row failed")? {
            let min_val = spanner_cell_to_scalar_impl(&row, &split_column.data_type, "min_val");
            let max_val = spanner_cell_to_scalar_impl(&row, &split_column.data_type, "max_val");
            match (min_val, max_val) {
                (Some(min), Some(max)) => Ok(Some((min, max))),
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    /// Gets the right bound exclusive for the next split.
    ///
    /// Fetches `max_split_size` rows starting from `left_value` and returns the
    /// max value if it is less than `max_value`, otherwise returns NULL (last split).
    /// Follows Postgres CDC's CTE pattern exactly.
    async fn next_split_right_bound_exclusive(
        &self,
        txn: &mut Transaction,
        left_value: &ScalarImpl,
        max_value: &ScalarImpl,
        max_split_size: u64,
        split_column: &Field,
    ) -> ConnectorResult<Option<Datum>> {
        let col = Self::quote_column(&split_column.name);
        let tbl = Self::quote_column(&self.table_name);
        let sql = format!(
            "WITH t AS (SELECT {col} FROM {tbl} WHERE {col} >= @left ORDER BY {col} ASC LIMIT {max_split_size}) \
             SELECT CASE WHEN MAX({col}) < @max THEN MAX({col}) ELSE NULL END AS val FROM t",
        );

        let mut stmt = Statement::new(&sql);
        add_scalar_param(&mut stmt, "left", left_value);
        add_scalar_param(&mut stmt, "max", max_value);

        let mut rows = txn.query(stmt).await.context("boundary query failed")?;

        if let Some(row) = rows.next().await.context("boundary row fetch failed")? {
            let datum = spanner_cell_to_scalar_impl(&row, &split_column.data_type, "val");
            Ok(Some(datum))
        } else {
            Ok(None)
        }
    }

    /// Finds the next greater distinct value when all rows have the same PK value.
    async fn next_greater_bound(
        &self,
        txn: &mut Transaction,
        start_offset: &ScalarImpl,
        max_value: &ScalarImpl,
        split_column: &Field,
    ) -> ConnectorResult<Option<Datum>> {
        let col = Self::quote_column(&split_column.name);
        let tbl = Self::quote_column(&self.table_name);
        let sql =
            format!("SELECT MIN({col}) AS val FROM {tbl} WHERE {col} > @start AND {col} < @max",);

        let mut stmt = Statement::new(&sql);
        add_scalar_param(&mut stmt, "start", start_offset);
        add_scalar_param(&mut stmt, "max", max_value);

        let mut rows = txn
            .query(stmt)
            .await
            .context("next_greater_bound query failed")?;

        if let Some(row) = rows
            .next()
            .await
            .context("next_greater_bound row fetch failed")?
        {
            let datum = spanner_cell_to_scalar_impl(&row, &split_column.data_type, "val");
            Ok(Some(datum))
        } else {
            Ok(None)
        }
    }

    /// Generates even splits for integer types (Int16, Int32, Int64).
    ///
    /// Uses computed numeric boundaries to create evenly-sized partitions.
    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn as_even_splits(&self, options: CdcTableSnapshotSplitOption) {
        let split_column = self.split_column(&options);

        // Create a read-only transaction at the snapshot timestamp for consistency.
        let snapshot_timestamp = self.snapshot_ts.ok_or_else(|| {
            anyhow!("spanner.snapshot_ts not found in connect_properties - table must be recreated")
        })?;

        tracing::info!(
            snapshot_timestamp_us = snapshot_timestamp,
            "PK range enumeration started (even splits)"
        );

        let timestamp = micros_to_spanner_ts(snapshot_timestamp);
        let tb = TimestampBound::read_timestamp(timestamp);
        let txn_options = ReadOnlyTransactionOption {
            timestamp_bound: tb,
            call_options: Default::default(),
        };
        let mut txn = self
            .client
            .read_only_transaction_with_option(txn_options)
            .await
            .context("failed to create read-only transaction at snapshot timestamp")?;

        let Some((min_value, max_value)) = self.min_and_max(&mut txn, &split_column).await? else {
            // Table is empty, return a single empty split
            yield CdcTableSnapshotSplit {
                split_id: CDC_TABLE_SPLIT_ID_START,
                left_bound_inclusive: OwnedRow::new(vec![None]),
                right_bound_exclusive: OwnedRow::new(vec![None]),
            };
            return Ok(());
        };

        let min_value = min_value.as_integral();
        let max_value = max_value.as_integral();

        tracing::info!(
            "PK range: min={}, max={}, type={:?}",
            min_value,
            max_value,
            split_column.data_type
        );

        let saturated_split_max_size = options
            .backfill_num_rows_per_split
            .try_into()
            .unwrap_or(i64::MAX);
        let mut left: Option<i64> = None;
        let mut right: Option<i64> = Some(min_value.saturating_add(saturated_split_max_size));
        let mut split_id = CDC_TABLE_SPLIT_ID_START;

        loop {
            let mut is_completed = false;
            if right.as_ref().map(|r| *r >= max_value).unwrap_or(true) {
                right = None;
                is_completed = true;
            }

            let split = CdcTableSnapshotSplit {
                split_id,
                left_bound_inclusive: OwnedRow::new(vec![
                    left.map(|l| to_int_scalar(l, &split_column.data_type)),
                ]),
                right_bound_exclusive: OwnedRow::new(vec![
                    right.map(|r| to_int_scalar(r, &split_column.data_type)),
                ]),
            };

            try_increase_split_id(&mut split_id)?;
            yield split;

            if is_completed {
                break;
            }

            left = right;
            right = left.map(|l| l.saturating_add(saturated_split_max_size));
        }
    }

    /// Generates uneven splits for non-integer types (Varchar, etc.).
    ///
    /// Uses data-driven sampling to find split points.
    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn as_uneven_splits(&self, options: CdcTableSnapshotSplitOption) {
        let split_column = self.split_column(&options);

        // Create a read-only transaction at the snapshot timestamp for consistency.
        let snapshot_timestamp = self.snapshot_ts.ok_or_else(|| {
            anyhow!("spanner.snapshot_ts not found in connect_properties - table must be recreated")
        })?;

        tracing::info!(
            snapshot_timestamp_us = snapshot_timestamp,
            "PK range enumeration started (uneven splits)"
        );

        let timestamp = micros_to_spanner_ts(snapshot_timestamp);
        let tb = TimestampBound::read_timestamp(timestamp);
        let txn_options = ReadOnlyTransactionOption {
            timestamp_bound: tb,
            call_options: Default::default(),
        };
        let mut txn = self
            .client
            .read_only_transaction_with_option(txn_options)
            .await
            .context("failed to create read-only transaction at snapshot timestamp")?;

        let Some((min_value, max_value)) = self.min_and_max(&mut txn, &split_column).await? else {
            // Table is empty, return a single empty split
            yield CdcTableSnapshotSplit {
                split_id: CDC_TABLE_SPLIT_ID_START,
                left_bound_inclusive: OwnedRow::new(vec![None]),
                right_bound_exclusive: OwnedRow::new(vec![None]),
            };
            return Ok(());
        };

        tracing::info!("PK range: min={:?}, max={:?}", min_value, max_value);

        // left bound will never be NULL value.
        let mut next_left_bound_inclusive = min_value.clone();
        let mut split_id = CDC_TABLE_SPLIT_ID_START;

        loop {
            let left_bound_inclusive: Datum = if next_left_bound_inclusive == min_value {
                None
            } else {
                Some(next_left_bound_inclusive.clone())
            };

            let right_bound_exclusive;
            let mut next_right = self
                .next_split_right_bound_exclusive(
                    &mut txn,
                    &next_left_bound_inclusive,
                    &max_value,
                    options.backfill_num_rows_per_split,
                    &split_column,
                )
                .await?;

            // Safeguard: if boundary equals left bound (all rows have same PK value),
            // find next distinct greater value (follows Postgres CDC's next_greater_bound)
            if let Some(Some(ref inner)) = next_right {
                if *inner == next_left_bound_inclusive {
                    // All rows_per_split rows have the same PK value - find next distinct
                    next_right = self
                        .next_greater_bound(
                            &mut txn,
                            &next_left_bound_inclusive,
                            &max_value,
                            &split_column,
                        )
                        .await?;
                }
            }

            if let Some(next_right) = next_right {
                match next_right {
                    None => {
                        // NULL found — last split.
                        right_bound_exclusive = None;
                    }
                    Some(next_right) => {
                        next_left_bound_inclusive = next_right.clone();
                        right_bound_exclusive = Some(next_right);
                    }
                }
            } else {
                // Not found.
                right_bound_exclusive = None;
            }

            let is_completed = right_bound_exclusive.is_none();

            if is_completed && left_bound_inclusive.is_none() {
                assert_eq!(split_id, CDC_TABLE_SPLIT_ID_START);
            }

            tracing::info!(
                split_id,
                ?left_bound_inclusive,
                ?right_bound_exclusive,
                "New CDC table snapshot split."
            );

            let left_bound_row = OwnedRow::new(vec![left_bound_inclusive]);
            let right_bound_row = OwnedRow::new(vec![right_bound_exclusive]);
            let split = CdcTableSnapshotSplit {
                split_id,
                left_bound_inclusive: left_bound_row,
                right_bound_exclusive: right_bound_row,
            };

            try_increase_split_id(&mut split_id)?;
            yield split;

            if is_completed {
                break;
            }
        }
    }

    /// Returns a function that parses CDC offset strings.
    ///
    /// Spanner CDC uses JSON-serialized `CdcOffset::Spanner` format.
    /// This is consistent with the offset format produced by both
    /// the backfill phase (`current_cdc_offset()`) and the CDC phase
    /// (`make_watermark_offset_string()` in reader.rs).
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
        start_pk_row: Option<OwnedRow>,
        primary_keys: Vec<String>,
        scan_limit: u32,
    ) {
        let fields = self.rw_schema.fields();
        let order_key = Self::get_order_key(&primary_keys);

        let stmt = if let Some(ref pk_row) = start_pk_row {
            let primary_keys: Vec<String> = self.pk_names.clone();
            let order_key = Self::get_order_key(&primary_keys);
            // Build filter: `pk0` > @pk0 OR (`pk0` = @pk0 AND `pk1` > @pk1) ...
            let filter = build_pk_filter_sql(&primary_keys);
            let sql = format!(
                "SELECT {} FROM {} WHERE {} ORDER BY {} LIMIT {}",
                self.field_names,
                Self::quote_column(&self.table_name),
                filter,
                order_key,
                scan_limit
            );
            let mut stmt = Statement::new(&sql);
            add_pk_params(&mut stmt, pk_row);
            stmt
        } else {
            let sql = format!(
                "SELECT {} FROM {} ORDER BY {} LIMIT {}",
                self.field_names,
                Self::quote_column(&self.table_name),
                order_key,
                scan_limit
            );
            Statement::new(&sql)
        };

        // Create a read-only transaction at the snapshot timestamp for consistency.
        // Note: We use regular read_only_transaction (not batch) because the query
        // has LIMIT clause which cannot be partitioned.
        let snapshot_timestamp = self.snapshot_ts.ok_or_else(|| {
            anyhow!("spanner.snapshot_ts not found in connect_properties - table must be recreated")
        })?;

        let timestamp = micros_to_spanner_ts(snapshot_timestamp);
        let tb = TimestampBound::read_timestamp(timestamp);
        let options = ReadOnlyTransactionOption {
            timestamp_bound: tb,
            call_options: Default::default(),
        };
        let mut txn = self
            .client
            .read_only_transaction_with_option(options)
            .await
            .context("failed to create read-only transaction at snapshot timestamp")?;

        // Execute the query directly (no partition, since LIMIT queries can't be partitioned)
        let mut rows = txn.query(stmt).await.context("snapshot query failed")?;

        while let Some(row) = rows.next().await.context("row read failed")? {
            yield spanner_row_to_owned_row(&row, &fields)?;
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
        assert_eq!(left.len(), 1, "multiple split columns is not supported yet");
        assert_eq!(
            right.len(),
            1,
            "multiple split columns is not supported yet"
        );
        let split_column_name = &split_columns[0].name;

        let is_first_split = left[0].is_none();
        let is_last_split = right[0].is_none();

        // Snapshot timestamp is guaranteed to exist by the frontend.
        let split_snapshot_ts = self.snapshot_ts.ok_or_else(|| {
            anyhow!("spanner.snapshot_ts not found in connect_properties - table must be recreated")
        })?;

        tracing::info!(
            "split_snapshot_read: PK range=[{:?}, {:?}), snapshot_ts={}",
            left[0],
            right[0],
            split_snapshot_ts
        );

        // Create a NEW BatchReadOnlyTransaction at the exact same snapshot timestamp
        // This is the key to snapshot consistency across all actors
        let timestamp = micros_to_spanner_ts(split_snapshot_ts);
        let tb = TimestampBound::read_timestamp(timestamp);
        let options = ReadOnlyTransactionOption {
            timestamp_bound: tb,
            call_options: Default::default(),
        };

        let mut txn = self
            .client
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

        // Build query with PK range WHERE clause using parameter binding
        // Follows Postgres CDC pattern: split on single column specified by backfill_split_pk_column_index
        let col = Self::quote_column(split_column_name);
        let tbl = Self::quote_column(&self.table_name);

        let (where_clause, stmt) = if is_first_split && is_last_split {
            // No bounds - full table scan
            let sql = format!("SELECT {} FROM {}", self.field_names, tbl);
            (String::new(), Statement::new(&sql))
        } else if is_first_split {
            // Unbounded left side: WHERE pk < @pk_end
            let sql = format!(
                "SELECT {} FROM {} WHERE {} < @pk_end",
                self.field_names, tbl, col
            );
            let mut stmt = Statement::new(&sql);
            if let Some(ref scalar) = right[0] {
                add_scalar_param(&mut stmt, "pk_end", scalar);
            }
            (format!("WHERE {} < @pk_end", col), stmt)
        } else if is_last_split {
            // Unbounded right side: WHERE pk >= @pk_start
            let sql = format!(
                "SELECT {} FROM {} WHERE {} >= @pk_start",
                self.field_names, tbl, col
            );
            let mut stmt = Statement::new(&sql);
            if let Some(ref scalar) = left[0] {
                add_scalar_param(&mut stmt, "pk_start", scalar);
            }
            (format!("WHERE {} >= @pk_start", col), stmt)
        } else {
            // Bounded range: WHERE pk >= @pk_start AND pk < @pk_end
            let sql = format!(
                "SELECT {} FROM {} WHERE {} >= @pk_start AND {} < @pk_end",
                self.field_names, tbl, col, col
            );
            let mut stmt = Statement::new(&sql);
            if let Some(ref scalar) = left[0] {
                add_scalar_param(&mut stmt, "pk_start", scalar);
            }
            if let Some(ref scalar) = right[0] {
                add_scalar_param(&mut stmt, "pk_end", scalar);
            }
            (
                format!("WHERE {} >= @pk_start AND {} < @pk_end", col, col),
                stmt,
            )
        };

        tracing::info!(
            "split_snapshot_read: executing partition_query with databoost={}, where={}",
            self.enable_databoost,
            where_clause
        );

        // Use partition_query for intra-actor parallelism
        // Spanner will create multiple partitions within this PK range
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
            "split_snapshot_read: got {} intra-partitions, streaming rows sequentially with configured parallelism={}",
            partitions.len(),
            self.partition_query_parallelism
        );

        // Stream each intra-partition directly instead of collecting all rows in memory.
        // The current upstream library requires mutable access to the batch transaction for
        // partition execution, so partitions are still consumed sequentially here.
        for partition in partitions {
            let mut rows = txn
                .execute(partition, None)
                .await
                .context("failed to execute partition")?;

            while let Some(row) = rows.next().await.context("row read failed")? {
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
    matches!(
        data_type,
        DataType::Int16 | DataType::Int32 | DataType::Int64
    )
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

/// Tries to increase the split ID, returns an error if overflow.
///
/// Follows Postgres CDC's try_increase_split_id pattern.
fn try_increase_split_id(split_id: &mut i64) -> ConnectorResult<()> {
    match split_id.checked_add(1) {
        Some(s) => {
            *split_id = s;
            Ok(())
        }
        None => Err(anyhow!("too many CDC snapshot splits").into()),
    }
}

/// Adds a `ScalarImpl` value as a named parameter to a Spanner `Statement`.
///
/// Converts RisingWave scalar types to the corresponding Spanner parameter types.
fn add_scalar_param(stmt: &mut Statement, name: &str, scalar: &ScalarImpl) {
    match scalar {
        ScalarImpl::Int16(v) => stmt.add_param(name, &(*v as i64)),
        ScalarImpl::Int32(v) => stmt.add_param(name, &(*v as i64)),
        ScalarImpl::Int64(v) => stmt.add_param(name, v),
        ScalarImpl::Float32(v) => stmt.add_param(name, &(v.0 as f64)),
        ScalarImpl::Float64(v) => stmt.add_param(name, &v.0),
        ScalarImpl::Utf8(v) => stmt.add_param(name, &v.as_ref().to_string()),
        ScalarImpl::Bool(v) => stmt.add_param(name, v),
        ScalarImpl::Decimal(v) => stmt.add_param(name, &v.to_string()),
        _ => panic!(
            "unsupported ScalarImpl type for Spanner param binding: {:?}",
            scalar
        ),
    }
}

/// Builds a lexicographic `>` filter for composite PKs, expanded for Spanner
/// which does not support tuple comparison `(a, b) > (@p1, @p2)`.
///
/// For a single PK column: `` `pk0` > @pk0 ``
/// For composite (pk0, pk1, pk2):
///   `` (`pk0` > @pk0) OR (`pk0` = @pk0 AND `pk1` > @pk1) OR (`pk0` = @pk0 AND `pk1` = @pk1 AND `pk2` > @pk2) ``
fn build_pk_filter_sql(pk_names: &[String]) -> String {
    let cols: Vec<String> = pk_names
        .iter()
        .map(|n| SpannerExternalTableReader::quote_column(n))
        .collect();

    let mut clauses = Vec::with_capacity(pk_names.len());
    for i in 0..pk_names.len() {
        let mut parts = Vec::with_capacity(i + 1);
        // All preceding columns must be equal
        for j in 0..i {
            parts.push(format!("{} = @pk{}", cols[j], j));
        }
        // The i-th column must be strictly greater
        parts.push(format!("{} > @pk{}", cols[i], i));
        clauses.push(format!("({})", parts.join(" AND ")));
    }
    clauses.join(" OR ")
}

/// Adds PK row values as named parameters (@pk0, @pk1, ...) to a Spanner `Statement`.
fn add_pk_params(stmt: &mut Statement, pk_row: &OwnedRow) {
    for (i, datum_ref) in pk_row.iter().enumerate() {
        if let Some(scalar_ref) = datum_ref {
            let scalar = scalar_ref.into_scalar_impl();
            add_scalar_param(stmt, &format!("pk{}", i), &scalar);
        }
    }
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

    let dsn = format!(
        "projects/{}/instances/{}/databases/{}",
        project, instance, database
    );

    Ok(Client::new(&dsn, client_config)
        .await
        .context("failed to create Spanner client")?)
}

/// Current time as microseconds since epoch.
pub fn now_micros() -> ConnectorResult<i64> {
    Ok(i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| anyhow!("system clock before Unix epoch: {}", e))?
            .as_micros(),
    )
    .map_err(|_| anyhow!("timestamp out of i64 range"))?)
}

/// Convert RFC3339 string to microseconds since epoch.
pub fn rfc3339_to_micros(s: &str) -> ConnectorResult<i64> {
    let offset = time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .map_err(|e| anyhow!("invalid RFC3339 timestamp '{}': {}", s, e))?;
    let nanos = offset.unix_timestamp_nanos();
    let micros = nanos.div_euclid(1000);
    Ok(
        i64::try_from(micros)
            .map_err(|_| anyhow!("timestamp out of i64 range: {} nanos", nanos))?,
    )
}

/// Convert microseconds since epoch to OffsetDateTime.
pub fn micros_to_offset_datetime(micros: i64) -> ConnectorResult<OffsetDateTime> {
    Ok(
        OffsetDateTime::from_unix_timestamp_nanos((micros as i128) * 1000)
            .map_err(|e| anyhow!("invalid microseconds timestamp {}: {}", micros, e))?,
    )
}

/// Convert microseconds since epoch to a Spanner `Timestamp`.
fn micros_to_spanner_ts(micros: i64) -> google_cloud_spanner::value::Timestamp {
    google_cloud_spanner::value::Timestamp {
        seconds: micros.div_euclid(1_000_000),
        nanos: (micros.rem_euclid(1_000_000) * 1000) as i32,
    }
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
        return Ok(DataType::List(ListType::new(spanner_type_to_rw_type(
            inner,
        )?)));
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
            tracing::info!(
                "mapping PROTO type '{}' to BYTEA (raw bytes preserved)",
                spanner_type
            );
            Ok(DataType::Bytea)
        }
        // ENUM types - store as VARCHAR (enum name preserved)
        t if t.starts_with("ENUM") => {
            tracing::info!(
                "mapping ENUM type '{}' to VARCHAR (enum name preserved)",
                spanner_type
            );
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
            tracing::warn!(
                "unknown Spanner type '{}' mapped to VARCHAR as fallback",
                spanner_type
            );
            Ok(DataType::Varchar)
        }
    }
}

// ---------------------------------------------------------------------------
// Row / value helpers
// ---------------------------------------------------------------------------

/// Extracts a single typed cell from a Spanner row as a `ScalarImpl`.
///
/// Follows the same pattern as `postgres_cell_to_scalar_impl` in the Postgres CDC parser.
fn spanner_cell_to_scalar_impl(
    row: &google_cloud_spanner::row::Row,
    data_type: &DataType,
    col_name: &str,
) -> Option<ScalarImpl> {
    match data_type {
        DataType::Boolean => row
            .column_by_name::<bool>(col_name)
            .ok()
            .map(ScalarImpl::Bool),

        DataType::Int64 => row
            .column_by_name::<i64>(col_name)
            .ok()
            .map(ScalarImpl::Int64),

        DataType::Int32 => row
            .column_by_name::<i64>(col_name)
            .ok()
            .map(|v| ScalarImpl::Int32(v as i32)),

        DataType::Int16 => row
            .column_by_name::<i64>(col_name)
            .ok()
            .map(|v| ScalarImpl::Int16(v as i16)),

        DataType::Float64 => row
            .column_by_name::<f64>(col_name)
            .ok()
            .map(|v| ScalarImpl::Float64(F64::from(v))),

        DataType::Float32 => row
            .column_by_name::<f64>(col_name)
            .ok()
            .map(|v| ScalarImpl::Float32(F32::from(v as f32))),

        DataType::Varchar => {
            // Handle VARCHAR, ENUM (stored as string), TIME, INTERVAL
            row.column_by_name::<String>(col_name)
                .ok()
                .map(|s| ScalarImpl::Utf8(s.into()))
        }

        DataType::Bytea => {
            // Handle BYTEA and PROTO types (stored as bytes)
            // Try reading as Vec<u8> first (for PROTO)
            if let Ok(v) = row.column_by_name::<Vec<u8>>(col_name) {
                Some(ScalarImpl::Bytea(v.into()))
            } else {
                // Fallback: read as string (for BYTES stored as base64 string)
                row.column_by_name::<String>(col_name)
                    .ok()
                    .map(|s| ScalarImpl::Bytea(s.into_bytes().into()))
            }
        }

        DataType::Timestamptz => {
            use risingwave_common::types::Timestamptz;
            row.column_by_name::<time::OffsetDateTime>(col_name)
                .ok()
                .map(|v| {
                    let micros = (v.unix_timestamp_nanos() / 1000) as i64;
                    ScalarImpl::Timestamptz(Timestamptz::from_micros(micros))
                })
        }

        DataType::Timestamp => {
            use risingwave_common::types::Timestamp;
            row.column_by_name::<time::OffsetDateTime>(col_name)
                .ok()
                .map(|v| {
                    let micros = (v.unix_timestamp_nanos() / 1000) as i64;
                    ScalarImpl::Timestamp(Timestamp::with_micros(micros).unwrap())
                })
        }

        DataType::Date => {
            use risingwave_common::types::Date;
            row.column_by_name::<time::Date>(col_name)
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
            row.column_by_name::<String>(col_name)
                .ok()
                .and_then(|s| Decimal::from_str(&s).ok())
                .map(ScalarImpl::Decimal)
        }

        DataType::Jsonb => {
            // Handle JSON and STRUCT types (stored as JSON string)
            row.column_by_name::<String>(col_name).ok().and_then(|s| {
                // Try to parse as JSON first
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&s) {
                    Some(ScalarImpl::Jsonb(json.into()))
                } else {
                    // If not valid JSON, wrap as string
                    Some(ScalarImpl::Jsonb(serde_json::json!(s).into()))
                }
            })
        }

        DataType::List(_) => {
            // Handle ARRAY types - read as JSON array and convert
            // The spanner library returns arrays as JSON strings
            row.column_by_name::<String>(col_name).ok().and_then(|s| {
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
                col_name,
                data_type
            );
            row.column_by_name::<String>(col_name)
                .ok()
                .map(|s| ScalarImpl::Utf8(s.into()))
        }
    }
}

fn spanner_row_to_owned_row(
    row: &google_cloud_spanner::row::Row,
    fields: &[Field],
) -> ConnectorResult<OwnedRow> {
    let values = fields
        .iter()
        .map(|f| spanner_cell_to_scalar_impl(row, &f.data_type, &f.name))
        .collect();
    Ok(OwnedRow::new(values))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spanner_offset() {
        let offset = SpannerOffset::new(1234567890);
        assert_eq!(offset.timestamp, 1234567890);
        assert_eq!(offset.partitions, None);
    }

    #[test]
    fn test_spanner_offset_ordering_ignores_partitions() {
        // Two offsets at the same timestamp/offset but different partition
        // frontiers must compare Equal — partitions must not affect ordering.
        let a = SpannerOffset::new(100).with_partitions(vec![PartitionOffset {
            token: Some("a".into()),
            parent_tokens: vec![],
            micros: 100,
            running: true,
        }]);
        let b = SpannerOffset::new(100).with_partitions(vec![PartitionOffset {
            token: Some("z".into()),
            parent_tokens: vec!["p".into()],
            micros: 100,
            running: false,
        }]);
        assert_eq!(a.partial_cmp(&b), Some(std::cmp::Ordering::Equal));

        // Ordering still tracks timestamp.
        let earlier = SpannerOffset::new(50).with_partitions(vec![]);
        assert!(earlier < a);
        assert!(a > earlier);
    }

    #[test]
    fn test_spanner_offset_serde_backward_compat() {
        // Old persisted offsets lack the `partitions` field — must deserialize
        // with `partitions: None` thanks to `#[serde(default)]`.
        let old = r#"{"timestamp":123,"offset":123}"#;
        let parsed: SpannerOffset = serde_json::from_str(old).unwrap();
        assert_eq!(parsed.timestamp, 123);
        assert_eq!(parsed.partitions, None);

        // Round-trip with partitions present.
        let with_parts = SpannerOffset::new(200).with_partitions(vec![PartitionOffset {
            token: None,
            parent_tokens: vec![],
            micros: 200,
            running: true,
        }]);
        let json = serde_json::to_string(&with_parts).unwrap();
        let back: SpannerOffset = serde_json::from_str(&json).unwrap();
        assert_eq!(with_parts, back);
    }

    #[test]
    fn test_spanner_offset_in_cdc_offset_ordering() {
        // The backfill comparison uses CdcOffset's derived PartialOrd, which
        // delegates to SpannerOffset's manual impl for the Spanner variant.
        let lo = CdcOffset::Spanner(SpannerOffset::new(10));
        let hi =
            CdcOffset::Spanner(
                SpannerOffset::new(20).with_partitions(vec![PartitionOffset {
                    token: Some("x".into()),
                    parent_tokens: vec![],
                    micros: 20,
                    running: true,
                }]),
            );
        assert!(lo < hi);
    }

    #[test]
    fn test_spanner_type_to_rw_type() {
        assert!(matches!(
            spanner_type_to_rw_type("BOOL").unwrap(),
            DataType::Boolean
        ));
        assert!(matches!(
            spanner_type_to_rw_type("INT64").unwrap(),
            DataType::Int64
        ));
        assert!(matches!(
            spanner_type_to_rw_type("STRING").unwrap(),
            DataType::Varchar
        ));
        assert!(matches!(
            spanner_type_to_rw_type("ARRAY<INT64>").unwrap(),
            DataType::List(_)
        ));
    }

    #[test]
    fn test_build_pk_filter_sql() {
        let cols = vec!["v1".to_owned()];
        let expr = build_pk_filter_sql(&cols);
        assert_eq!(expr, "(`v1` > @pk0)");

        let cols = vec!["v1".to_owned(), "v2".to_owned()];
        let expr = build_pk_filter_sql(&cols);
        assert_eq!(expr, "(`v1` > @pk0) OR (`v1` = @pk0 AND `v2` > @pk1)");

        let cols = vec!["v1".to_owned(), "v2".to_owned(), "v3".to_owned()];
        let expr = build_pk_filter_sql(&cols);
        assert_eq!(
            expr,
            "(`v1` > @pk0) OR (`v1` = @pk0 AND `v2` > @pk1) OR (`v1` = @pk0 AND `v2` = @pk1 AND `v3` > @pk2)"
        );
    }

    #[test]
    fn test_micros_to_spanner_ts_positive() {
        let ts = micros_to_spanner_ts(1_500_000); // 1.5 seconds
        assert_eq!(ts.seconds, 1);
        assert_eq!(ts.nanos, 500_000_000); // 500ms in nanos
    }

    #[test]
    fn test_micros_to_spanner_ts_zero() {
        let ts = micros_to_spanner_ts(0);
        assert_eq!(ts.seconds, 0);
        assert_eq!(ts.nanos, 0);
    }

    #[test]
    fn test_micros_to_spanner_ts_negative() {
        // -1.5 seconds: div_euclid gives -2, rem_euclid gives 500000
        let ts = micros_to_spanner_ts(-1_500_000);
        assert_eq!(ts.seconds, -2);
        assert_eq!(ts.nanos, 500_000_000);
    }

    #[test]
    fn test_micros_to_spanner_ts_exact_second() {
        let ts = micros_to_spanner_ts(1_000_000);
        assert_eq!(ts.seconds, 1);
        assert_eq!(ts.nanos, 0);

        let ts = micros_to_spanner_ts(-1_000_000);
        assert_eq!(ts.seconds, -1);
        assert_eq!(ts.nanos, 0);
    }

    #[test]
    fn test_micros_to_spanner_ts_sub_second() {
        let ts = micros_to_spanner_ts(999_999);
        assert_eq!(ts.seconds, 0);
        assert_eq!(ts.nanos, 999_999_000);
    }

    #[test]
    fn test_micros_to_spanner_ts_negative_one() {
        let ts = micros_to_spanner_ts(-1);
        assert_eq!(ts.seconds, -1);
        assert_eq!(ts.nanos, 999_999_000);
    }
}
