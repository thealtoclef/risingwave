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
use anyhow::{Context, anyhow};
use futures::stream::BoxStream;
use futures::pin_mut;
use futures_async_stream::try_stream;
use google_cloud_spanner::client::{Client, ReadOnlyTransactionOption};
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::transaction::Transaction;
use google_cloud_spanner::value::TimestampBound;

use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum, F32, F64, ListType, ScalarImpl};

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CDC_TABLE_SPLIT_ID_START, CdcOffset, CdcTableSnapshotSplitOption, ExternalTableConfig,
    ExternalTableReader, SchemaTableName,
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

    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn get_parallel_cdc_splits(&self, options: CdcTableSnapshotSplitOption) {
        let backfill_num_rows_per_split = options.backfill_num_rows_per_split;
        if backfill_num_rows_per_split == 0 {
            return Err(
                anyhow::anyhow!("invalid backfill_num_rows_per_split, must be greater than 0")
                    .into(),
            );
        }
        if options.backfill_split_pk_column_index as usize >= self.pk_names.len() {
            return Err(anyhow::anyhow!(format!(
                "invalid backfill_split_pk_column_index {}, out of bound",
                options.backfill_split_pk_column_index
            ))
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
        let minmax_query = format!(
            "SELECT MIN({col}) as min_val, MAX({col}) as max_val FROM {tbl}",
        );

        let stmt = Statement::new(&minmax_query);
        let mut rows = txn
            .query(stmt)
            .await
            .context("PK range query failed")?;

        if let Some(row) = rows.next().await.context("PK range row failed")? {
            let min_val = spanner_cell_to_scalar_impl(&row, &split_column.data_type, "min_val")?;
            let max_val = spanner_cell_to_scalar_impl(&row, &split_column.data_type, "max_val")?;
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
        add_scalar_param(&mut stmt, "left", left_value)?;
        add_scalar_param(&mut stmt, "max", max_value)?;

        let mut rows = txn
            .query(stmt)
            .await
            .context("boundary query failed")?;

        if let Some(row) = rows.next().await.context("boundary row fetch failed")? {
            let datum = spanner_cell_to_scalar_impl(&row, &split_column.data_type, "val")?;
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
        let sql = format!(
            "SELECT MIN({col}) AS val FROM {tbl} WHERE {col} > @start AND {col} < @max",
        );

        let mut stmt = Statement::new(&sql);
        add_scalar_param(&mut stmt, "start", start_offset)?;
        add_scalar_param(&mut stmt, "max", max_value)?;

        let mut rows = txn
            .query(stmt)
            .await
            .context("next_greater_bound query failed")?;

        if let Some(row) = rows.next().await.context("next_greater_bound row fetch failed")?
        {
            let datum = spanner_cell_to_scalar_impl(&row, &split_column.data_type, "val")?;
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
        let snapshot_timestamp = self.snapshot_ts
            .ok_or_else(|| anyhow!("spanner.snapshot_ts not found in connect_properties - table must be recreated"))?;

        tracing::info!(
            snapshot_timestamp_us = snapshot_timestamp,
            "PK range enumeration started (even splits)"
        );

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
            min_value, max_value, split_column.data_type
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
        let snapshot_timestamp = self.snapshot_ts
            .ok_or_else(|| anyhow!("spanner.snapshot_ts not found in connect_properties - table must be recreated"))?;

        tracing::info!(
            snapshot_timestamp_us = snapshot_timestamp,
            "PK range enumeration started (uneven splits)"
        );

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

        let Some((min_value, max_value)) = self.min_and_max(&mut txn, &split_column).await?
        else {
            // Table is empty, return a single empty split
            yield CdcTableSnapshotSplit {
                split_id: CDC_TABLE_SPLIT_ID_START,
                left_bound_inclusive: OwnedRow::new(vec![None]),
                right_bound_exclusive: OwnedRow::new(vec![None]),
            };
            return Ok(());
        };

        tracing::info!(
            "PK range: min={:?}, max={:?}",
            min_value, max_value
        );

        // left bound will never be NULL value.
        let mut next_left_bound_inclusive = min_value.clone();
        let mut split_id = CDC_TABLE_SPLIT_ID_START;

        loop {
            let left_bound_inclusive: Datum =
                if next_left_bound_inclusive == min_value {
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
                self.field_names, Self::quote_column(&self.table_name), filter, order_key, scan_limit
            );
            let mut stmt = Statement::new(&sql);
            add_pk_params(&mut stmt, pk_row)?;
            stmt
        } else {
            let sql = format!(
                "SELECT {} FROM {} ORDER BY {} LIMIT {}",
                self.field_names, Self::quote_column(&self.table_name), order_key, scan_limit
            );
            Statement::new(&sql)
        };

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
            .query(stmt)
            .await
            .context("snapshot query failed")?;

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
        assert_eq!(right.len(), 1, "multiple split columns is not supported yet");
        let split_column_name = &split_columns[0].name;

        let is_first_split = left[0].is_none();
        let is_last_split = right[0].is_none();

        // Get snapshot timestamp from reader state (set from config.spanner_snapshot_ts)
        let split_snapshot_ts = self.snapshot_ts
            .ok_or_else(|| anyhow!("spanner.snapshot_ts not found in connect_properties"))?;

        tracing::info!(
            "split_snapshot_read: PK range=[{:?}, {:?}), snapshot_ts={}",
            left[0], right[0], split_snapshot_ts
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
            let sql = format!("SELECT {} FROM {} WHERE {} < @pk_end", self.field_names, tbl, col);
            let mut stmt = Statement::new(&sql);
            if let Some(ref scalar) = right[0] {
                add_scalar_param(&mut stmt, "pk_end", scalar)?;
            }
            (format!("WHERE {} < @pk_end", col), stmt)
        } else if is_last_split {
            // Unbounded right side: WHERE pk >= @pk_start
            let sql = format!("SELECT {} FROM {} WHERE {} >= @pk_start", self.field_names, tbl, col);
            let mut stmt = Statement::new(&sql);
            if let Some(ref scalar) = left[0] {
                add_scalar_param(&mut stmt, "pk_start", scalar)?;
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
                add_scalar_param(&mut stmt, "pk_start", scalar)?;
            }
            if let Some(ref scalar) = right[0] {
                add_scalar_param(&mut stmt, "pk_end", scalar)?;
            }
            (format!("WHERE {} >= @pk_start AND {} < @pk_end", col, col), stmt)
        };

        tracing::info!(
            "split_snapshot_read: executing partition_query with databoost={}, where={}",
            self.enable_databoost, where_clause
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
            partitions.len(), self.partition_query_parallelism
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

/// Tries to increase the split ID, returns an error if overflow.
///
/// Follows Postgres CDC's try_increase_split_id pattern.
fn try_increase_split_id(split_id: &mut i64) -> ConnectorResult<()> {
    match split_id.checked_add(1) {
        Some(s) => {
            *split_id = s;
            Ok(())
        }
        None => Err(anyhow::anyhow!("too many CDC snapshot splits").into()),
    }
}

/// Binds a `ScalarImpl` as a named parameter on a Spanner `Statement`.
///
/// Handles every type Spanner allows as a PK column part (all except
/// `FLOAT32`, `ARRAY`, `JSON`, `STRUCT`).
fn add_scalar_param(
    stmt: &mut Statement,
    name: &str,
    scalar: &ScalarImpl,
) -> ConnectorResult<()> {
    match scalar {
        ScalarImpl::Int16(v) => stmt.add_param(name, &(*v as i64)),
        ScalarImpl::Int32(v) => stmt.add_param(name, &(*v as i64)),
        ScalarImpl::Int64(v) => stmt.add_param(name, v),
        ScalarImpl::Float32(v) => stmt.add_param(name, &(v.0 as f64)),
        ScalarImpl::Float64(v) => stmt.add_param(name, &v.0),
        ScalarImpl::Utf8(v) => stmt.add_param(name, &v.as_ref().to_string()),
        ScalarImpl::Bool(v) => stmt.add_param(name, v),
        ScalarImpl::Decimal(d) => {
            // Must bind as BigDecimal so the emitted TypeCode is Numeric;
            // a String bind would be rejected against a NUMERIC column.
            // Bound type isn't unit-testable (`Statement::param_types` is
            // `pub(crate)`) — covered by the Spanner CDC e2e suite.
            let bd = decimal_to_spanner_numeric(d)?;
            stmt.add_param(name, &bd);
        }
        ScalarImpl::Date(v) => {
            use chrono::Datelike;
            let nd = v.0;
            let month = time::Month::try_from(nd.month() as u8)
                .map_err(|e| anyhow!("invalid month in Date: {}", e))?;
            let td = time::Date::from_calendar_date(nd.year(), month, nd.day() as u8)
                .map_err(|e| anyhow!("invalid Date for Spanner bind: {}", e))?;
            stmt.add_param(name, &td);
        }
        ScalarImpl::Timestamp(v) => {
            // Spanner TIMESTAMP is a UTC instant; treat RW's naive Timestamp
            // as UTC to match the read path in `spanner_cell_to_scalar_impl`.
            let micros = v.0.and_utc().timestamp_micros();
            let od = time::OffsetDateTime::from_unix_timestamp_nanos((micros as i128) * 1_000)
                .map_err(|e| anyhow!("invalid Timestamp for Spanner bind: {}", e))?;
            stmt.add_param(name, &od);
        }
        ScalarImpl::Timestamptz(v) => {
            let micros = v.timestamp_micros();
            let od = time::OffsetDateTime::from_unix_timestamp_nanos((micros as i128) * 1_000)
                .map_err(|e| anyhow!("invalid Timestamptz for Spanner bind: {}", e))?;
            stmt.add_param(name, &od);
        }
        ScalarImpl::Bytea(v) => {
            let bytes: &[u8] = v.as_ref();
            stmt.add_param(name, &bytes);
        }
        other => bail!(
            "unsupported ScalarImpl variant for Spanner param binding: {:?}",
            other
        ),
    }
    Ok(())
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
fn add_pk_params(stmt: &mut Statement, pk_row: &OwnedRow) -> ConnectorResult<()> {
    for (i, datum_ref) in pk_row.iter().enumerate() {
        if let Some(scalar_ref) = datum_ref {
            let scalar = scalar_ref.into_scalar_impl();
            add_scalar_param(stmt, &format!("pk{}", i), &scalar)?;
        }
    }
    Ok(())
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

/// Decodes a Spanner column into `serde_json::Value` using the declared
/// `Type` to disambiguate `Kind::StringValue`, which Spanner overloads for
/// `STRING`/`INT64`/`NUMERIC`/`BYTES`/`TIMESTAMP`/`JSON`/etc.
///
/// A kind-only walker would coerce a `STRING` field with value `"1"` /
/// `"null"` into a JSON number/null and corrupt the data.
struct SpannerJson(serde_json::Value);

impl google_cloud_spanner::row::TryFromValue for SpannerJson {
    fn try_from(
        value: &prost_types::Value,
        field: &google_cloud_googleapis::spanner::v1::struct_type::Field,
    ) -> Result<Self, google_cloud_spanner::row::Error> {
        Ok(SpannerJson(prost_value_to_json(value, field.r#type.as_ref())))
    }
}

fn prost_value_to_json(
    v: &prost_types::Value,
    ty: Option<&google_cloud_googleapis::spanner::v1::Type>,
) -> serde_json::Value {
    use google_cloud_googleapis::spanner::v1::TypeCode;
    use prost_types::value::Kind;

    let code = ty.map(|t| t.code).unwrap_or(TypeCode::Unspecified as i32);

    match v.kind.as_ref() {
        None | Some(Kind::NullValue(_)) => serde_json::Value::Null,
        Some(Kind::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(Kind::NumberValue(n)) => serde_json::Number::from_f64(*n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Some(Kind::StringValue(s)) => {
            if code == TypeCode::Json as i32 {
                serde_json::from_str(s).unwrap_or_else(|_| serde_json::Value::String(s.clone()))
            } else if code == TypeCode::Int64 as i32 {
                s.parse::<i64>()
                    .ok()
                    .map(|n| serde_json::Value::Number(n.into()))
                    .unwrap_or_else(|| serde_json::Value::String(s.clone()))
            } else {
                // All other TypeCodes are opaque text in JSON terms; keep
                // verbatim so STRING values like "1" / "null" don't retype.
                serde_json::Value::String(s.clone())
            }
        }
        Some(Kind::ListValue(l)) => {
            if code == TypeCode::Struct as i32 {
                // STRUCT in result rows is a positional ListValue; pair each
                // child with its declared field type.
                let struct_ty = ty.and_then(|t| t.struct_type.as_ref());
                let fields: Vec<_> = struct_ty
                    .map(|st| st.fields.as_slice())
                    .unwrap_or_default()
                    .iter()
                    .zip(l.values.iter())
                    .map(|(f, child)| {
                        (f.name.clone(), prost_value_to_json(child, f.r#type.as_ref()))
                    })
                    .collect();
                serde_json::Value::Object(fields.into_iter().collect())
            } else {
                let elem_ty = ty.and_then(|t| t.array_element_type.as_deref());
                serde_json::Value::Array(
                    l.values
                        .iter()
                        .map(|child| prost_value_to_json(child, elem_ty))
                        .collect(),
                )
            }
        }
        Some(Kind::StructValue(s)) => {
            // STRUCT-keyed-by-name form: look up each field's declared type.
            let struct_ty = ty.and_then(|t| t.struct_type.as_ref());
            let lookup: std::collections::HashMap<&str, Option<&google_cloud_googleapis::spanner::v1::Type>> =
                struct_ty
                    .map(|st| {
                        st.fields
                            .iter()
                            .map(|f| (f.name.as_str(), f.r#type.as_ref()))
                            .collect()
                    })
                    .unwrap_or_default();
            serde_json::Value::Object(
                s.fields
                    .iter()
                    .map(|(k, child)| {
                        let field_ty = lookup.get(k.as_str()).copied().flatten();
                        (k.clone(), prost_value_to_json(child, field_ty))
                    })
                    .collect(),
            )
        }
    }
}

/// Sole chokepoint for `Decimal` → Spanner `NUMERIC` parameter conversion.
/// Returning `BigDecimal` here is what makes `add_scalar_param` bind as
/// `TypeCode::Numeric`; see the comment in that function.
fn decimal_to_spanner_numeric(
    d: &risingwave_common::types::Decimal,
) -> ConnectorResult<google_cloud_spanner::bigdecimal::BigDecimal> {
    use google_cloud_spanner::bigdecimal::BigDecimal;
    use risingwave_common::types::Decimal;

    match d {
        Decimal::Normalized(n) => BigDecimal::from_str(&n.to_string())
            .map_err(|e| anyhow!("invalid Decimal for Spanner NUMERIC bind: {}", e).into()),
        other => Err(anyhow!("Decimal value {:?} cannot be bound to Spanner NUMERIC", other).into()),
    }
}

/// Extracts a single typed cell from a Spanner row as a `ScalarImpl`.
///
/// SQL NULL is returned as `Ok(None)`; wire-format / type mismatches
/// propagate as `Err` rather than silently degrading to NULL.
fn spanner_cell_to_scalar_impl(
    row: &google_cloud_spanner::row::Row,
    data_type: &DataType,
    col_name: &str,
) -> ConnectorResult<Option<ScalarImpl>> {
    let read_err = |e: google_cloud_spanner::row::Error| -> ConnectorError {
        anyhow!(
            "spanner cell read failed for column '{}' as {:?}: {}",
            col_name,
            data_type,
            e
        )
        .into()
    };

    let scalar = match data_type {
        DataType::Boolean => row
            .column_by_name::<Option<bool>>(col_name)
            .map_err(read_err)?
            .map(ScalarImpl::Bool),

        DataType::Int64 => row
            .column_by_name::<Option<i64>>(col_name)
            .map_err(read_err)?
            .map(ScalarImpl::Int64),

        DataType::Int32 => row
            .column_by_name::<Option<i64>>(col_name)
            .map_err(read_err)?
            .map(|v| ScalarImpl::Int32(v as i32)),

        DataType::Int16 => row
            .column_by_name::<Option<i64>>(col_name)
            .map_err(read_err)?
            .map(|v| ScalarImpl::Int16(v as i16)),

        DataType::Float64 => row
            .column_by_name::<Option<f64>>(col_name)
            .map_err(read_err)?
            .map(|v| ScalarImpl::Float64(F64::from(v))),

        DataType::Float32 => row
            .column_by_name::<Option<f64>>(col_name)
            .map_err(read_err)?
            .map(|v| ScalarImpl::Float32(F32::from(v as f32))),

        DataType::Varchar => row
            .column_by_name::<Option<String>>(col_name)
            .map_err(read_err)?
            .map(|s| ScalarImpl::Utf8(s.into())),

        DataType::Bytea => {
            // PROTO arrives as `Vec<u8>`; BYTES arrives as a base64 `String`.
            match row.column_by_name::<Option<Vec<u8>>>(col_name) {
                Ok(v) => v.map(|b| ScalarImpl::Bytea(b.into())),
                Err(_) => row
                    .column_by_name::<Option<String>>(col_name)
                    .map_err(read_err)?
                    .map(|s| ScalarImpl::Bytea(s.into_bytes().into())),
            }
        }

        DataType::Timestamptz => {
            use risingwave_common::types::Timestamptz;
            row.column_by_name::<Option<time::OffsetDateTime>>(col_name)
                .map_err(read_err)?
                .map(|v| {
                    let micros = (v.unix_timestamp_nanos() / 1000) as i64;
                    ScalarImpl::Timestamptz(Timestamptz::from_micros(micros))
                })
        }

        DataType::Timestamp => row
            .column_by_name::<Option<time::OffsetDateTime>>(col_name)
            .map_err(read_err)?
            .map(|v| {
                let micros = (v.unix_timestamp_nanos() / 1000) as i64;
                risingwave_common::types::Timestamp::with_micros(micros).map(ScalarImpl::Timestamp)
            })
            .transpose()
            .map_err(|e| anyhow!("spanner Timestamp out of range for '{}': {}", col_name, e))?,

        DataType::Date => row
            .column_by_name::<Option<time::Date>>(col_name)
            .map_err(read_err)?
            .map(|v| {
                let s = format!("{:04}-{:02}-{:02}", v.year(), v.month(), v.day());
                risingwave_common::types::Date::from_str(&s).map(ScalarImpl::Date)
            })
            .transpose()
            .map_err(|e| anyhow!("spanner Date parse failed for '{}': {}", col_name, e))?,

        DataType::Decimal => row
            .column_by_name::<Option<String>>(col_name)
            .map_err(read_err)?
            .map(|s| risingwave_common::types::Decimal::from_str(&s).map(ScalarImpl::Decimal))
            .transpose()
            .map_err(|e| anyhow!("spanner Decimal parse failed for '{}': {}", col_name, e))?,

        DataType::Jsonb => {
            // Covers Spanner JSON and STRUCT uniformly via the type-aware walker.
            row.column_by_name::<Option<SpannerJson>>(col_name)
                .map_err(read_err)?
                .map(|j| ScalarImpl::Jsonb(j.0.into()))
        }

        DataType::List(list_type) => {
            // Dispatched on the declared inner type; Spanner forbids nested arrays.
            use risingwave_common::array::ListValue;
            use risingwave_common::types::{Date, Decimal, Timestamp, Timestamptz};

            let inner = list_type.elem();
            let datums: Option<Vec<Datum>> = match inner {
                DataType::Boolean => row
                    .column_by_name::<Option<Vec<Option<bool>>>>(col_name)
                    .map_err(read_err)?
                    .map(|vs| vs.into_iter().map(|o| o.map(ScalarImpl::Bool)).collect()),
                DataType::Int64 => row
                    .column_by_name::<Option<Vec<Option<i64>>>>(col_name)
                    .map_err(read_err)?
                    .map(|vs| vs.into_iter().map(|o| o.map(ScalarImpl::Int64)).collect()),
                DataType::Int32 => row
                    .column_by_name::<Option<Vec<Option<i64>>>>(col_name)
                    .map_err(read_err)?
                    .map(|vs| {
                        vs.into_iter()
                            .map(|o| o.map(|v| ScalarImpl::Int32(v as i32)))
                            .collect()
                    }),
                DataType::Int16 => row
                    .column_by_name::<Option<Vec<Option<i64>>>>(col_name)
                    .map_err(read_err)?
                    .map(|vs| {
                        vs.into_iter()
                            .map(|o| o.map(|v| ScalarImpl::Int16(v as i16)))
                            .collect()
                    }),
                DataType::Float64 => row
                    .column_by_name::<Option<Vec<Option<f64>>>>(col_name)
                    .map_err(read_err)?
                    .map(|vs| {
                        vs.into_iter()
                            .map(|o| o.map(|v| ScalarImpl::Float64(F64::from(v))))
                            .collect()
                    }),
                DataType::Float32 => row
                    .column_by_name::<Option<Vec<Option<f64>>>>(col_name)
                    .map_err(read_err)?
                    .map(|vs| {
                        vs.into_iter()
                            .map(|o| o.map(|v| ScalarImpl::Float32(F32::from(v as f32))))
                            .collect()
                    }),
                DataType::Varchar => row
                    .column_by_name::<Option<Vec<Option<String>>>>(col_name)
                    .map_err(read_err)?
                    .map(|vs| {
                        vs.into_iter()
                            .map(|o| o.map(|s| ScalarImpl::Utf8(s.into())))
                            .collect()
                    }),
                DataType::Bytea => row
                    .column_by_name::<Option<Vec<Option<Vec<u8>>>>>(col_name)
                    .map_err(read_err)?
                    .map(|vs| {
                        vs.into_iter()
                            .map(|o| o.map(|b| ScalarImpl::Bytea(b.into())))
                            .collect()
                    }),
                DataType::Date => {
                    let vs = row
                        .column_by_name::<Option<Vec<Option<time::Date>>>>(col_name)
                        .map_err(read_err)?;
                    match vs {
                        None => None,
                        Some(vs) => Some(
                            vs.into_iter()
                                .map(|o| match o {
                                    None => Ok(None),
                                    Some(v) => {
                                        let s = format!(
                                            "{:04}-{:02}-{:02}",
                                            v.year(),
                                            v.month() as u8,
                                            v.day()
                                        );
                                        Date::from_str(&s)
                                            .map(|d| Some(ScalarImpl::Date(d)))
                                            .map_err(|e| anyhow!(
                                                "spanner Date parse failed in ARRAY '{}': {}",
                                                col_name, e
                                            ))
                                    }
                                })
                                .collect::<Result<Vec<Datum>, _>>()?,
                        ),
                    }
                }
                DataType::Timestamptz => row
                    .column_by_name::<Option<Vec<Option<time::OffsetDateTime>>>>(col_name)
                    .map_err(read_err)?
                    .map(|vs| {
                        vs.into_iter()
                            .map(|o| {
                                o.map(|v| {
                                    let micros = (v.unix_timestamp_nanos() / 1000) as i64;
                                    ScalarImpl::Timestamptz(Timestamptz::from_micros(micros))
                                })
                            })
                            .collect()
                    }),
                DataType::Timestamp => {
                    let vs = row
                        .column_by_name::<Option<Vec<Option<time::OffsetDateTime>>>>(col_name)
                        .map_err(read_err)?;
                    match vs {
                        None => None,
                        Some(vs) => Some(
                            vs.into_iter()
                                .map(|o| match o {
                                    None => Ok(None),
                                    Some(v) => {
                                        let micros = (v.unix_timestamp_nanos() / 1000) as i64;
                                        Timestamp::with_micros(micros)
                                            .map(|t| Some(ScalarImpl::Timestamp(t)))
                                            .map_err(|e| anyhow!(
                                                "spanner Timestamp out of range in ARRAY '{}': {}",
                                                col_name, e
                                            ))
                                    }
                                })
                                .collect::<Result<Vec<Datum>, _>>()?,
                        ),
                    }
                }
                DataType::Decimal => {
                    let vs = row
                        .column_by_name::<Option<Vec<Option<String>>>>(col_name)
                        .map_err(read_err)?;
                    match vs {
                        None => None,
                        Some(vs) => Some(
                            vs.into_iter()
                                .map(|o| match o {
                                    None => Ok(None),
                                    Some(s) => Decimal::from_str(&s)
                                        .map(|d| Some(ScalarImpl::Decimal(d)))
                                        .map_err(|e| anyhow!(
                                            "spanner Decimal parse failed in ARRAY '{}': {}",
                                            col_name, e
                                        )),
                                })
                                .collect::<Result<Vec<Datum>, _>>()?,
                        ),
                    }
                }
                DataType::Jsonb => {
                    // Read the whole ARRAY in one shot so the walker keeps
                    // per-element Type context (a per-element read would
                    // reuse the outer ARRAY field via `Vec<T>: TryFromValue`).
                    let j = row
                        .column_by_name::<Option<SpannerJson>>(col_name)
                        .map_err(read_err)?;
                    match j.map(|j| j.0) {
                        None => None,
                        Some(serde_json::Value::Array(items)) => Some(
                            items
                                .into_iter()
                                .map(|v| {
                                    if v.is_null() {
                                        None
                                    } else {
                                        Some(ScalarImpl::Jsonb(v.into()))
                                    }
                                })
                                .collect(),
                        ),
                        Some(other) => bail!(
                            "expected JSON array for ARRAY column '{}', got {:?}",
                            col_name, other
                        ),
                    }
                }
                other => bail!(
                    "unsupported ARRAY inner type for column '{}': {:?}",
                    col_name, other
                ),
            };

            datums.map(|datums| ScalarImpl::List(ListValue::from_datum_iter(inner, datums)))
        }

        other => bail!(
            "unsupported RisingWave type for Spanner cell '{}': {:?}",
            col_name, other
        ),
    };
    Ok(scalar)
}

fn spanner_row_to_owned_row(
    row: &google_cloud_spanner::row::Row,
    fields: &[Field],
) -> ConnectorResult<OwnedRow> {
    let values: Vec<Datum> = fields
        .iter()
        .map(|f| spanner_cell_to_scalar_impl(row, &f.data_type, &f.name))
        .collect::<ConnectorResult<_>>()?;
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

    #[test]
    fn test_add_scalar_param_covers_all_valid_pk_types() {
        // Spanner allows every type except FLOAT32/ARRAY/JSON/STRUCT in a PK;
        // all of these must bind without panicking.
        use risingwave_common::types::{Date, Decimal, Interval, Timestamp, Timestamptz};

        let cases: Vec<(&str, ScalarImpl)> = vec![
            ("i16", ScalarImpl::Int16(1)),
            ("i32", ScalarImpl::Int32(2)),
            ("i64", ScalarImpl::Int64(3)),
            ("f32", ScalarImpl::Float32(F32::from(1.5_f32))),
            ("f64", ScalarImpl::Float64(F64::from(2.5_f64))),
            ("bool", ScalarImpl::Bool(true)),
            ("utf8", ScalarImpl::Utf8("hello".into())),
            (
                "decimal",
                ScalarImpl::Decimal(Decimal::from_str("123.456").unwrap()),
            ),
            (
                "date",
                ScalarImpl::Date(Date::from_str("2025-01-02").unwrap()),
            ),
            (
                "timestamp",
                ScalarImpl::Timestamp(Timestamp::with_micros(1_700_000_000_000_000).unwrap()),
            ),
            (
                "timestamptz",
                ScalarImpl::Timestamptz(Timestamptz::from_micros(1_700_000_000_000_000)),
            ),
            (
                "bytea",
                ScalarImpl::Bytea(b"\x00\x01\x02".to_vec().into_boxed_slice()),
            ),
        ];
        for (name, scalar) in cases {
            let mut stmt = Statement::new("SELECT 1");
            add_scalar_param(&mut stmt, name, &scalar)
                .unwrap_or_else(|e| panic!("binding {} failed: {}", name, e));
        }

        // Non-representable Decimals and PK-invalid types must error, not panic.
        let mut stmt = Statement::new("SELECT 1");
        assert!(add_scalar_param(&mut stmt, "nan", &ScalarImpl::Decimal(Decimal::NaN)).is_err());

        let mut stmt = Statement::new("SELECT 1");
        let interval = ScalarImpl::Interval(Interval::from_month_day_usec(0, 1, 0));
        assert!(add_scalar_param(&mut stmt, "interval", &interval).is_err());
    }

    #[test]
    fn test_decimal_to_spanner_numeric_rejects_non_normalized() {
        // Only the conversion helper's error semantics are unit-testable;
        // the bound TypeCode requires e2e coverage.
        use risingwave_common::types::Decimal;

        assert!(decimal_to_spanner_numeric(&Decimal::from_str("3.14").unwrap()).is_ok());
        assert!(decimal_to_spanner_numeric(&Decimal::NaN).is_err());
        assert!(decimal_to_spanner_numeric(&Decimal::PositiveInf).is_err());
        assert!(decimal_to_spanner_numeric(&Decimal::NegativeInf).is_err());
    }

    #[test]
    fn test_list_decoder_int64_with_nulls() {
        use std::collections::HashMap;
        use std::sync::Arc;

        use google_cloud_googleapis::spanner::v1::struct_type::Field as SpField;
        use google_cloud_googleapis::spanner::v1::{Type, TypeCode};
        use google_cloud_spanner::row::Row;
        use prost_types::value::Kind;
        use prost_types::Value as ProstValue;
        use risingwave_common::types::ToOwnedDatum;

        // ARRAY<INT64> [10, NULL, 20]; Spanner encodes INT64 as StringValue.
        let elem_kind = |s: &str| ProstValue {
            kind: Some(Kind::StringValue(s.to_owned())),
        };
        let null_kind = || ProstValue {
            kind: Some(Kind::NullValue(0)),
        };
        let list = ProstValue {
            kind: Some(Kind::ListValue(prost_types::ListValue {
                values: vec![elem_kind("10"), null_kind(), elem_kind("20")],
            })),
        };

        let array_type = Type {
            code: TypeCode::Array as i32,
            array_element_type: Some(Box::new(Type {
                code: TypeCode::Int64 as i32,
                ..Default::default()
            })),
            struct_type: None,
            type_annotation: 0,
            proto_type_fqn: String::new(),
        };
        let field = SpField {
            name: "arr".to_owned(),
            r#type: Some(array_type),
        };
        let mut index = HashMap::new();
        index.insert("arr".to_owned(), 0);
        let row = Row::new(Arc::new(index), Arc::new(vec![field]), vec![list]);

        let dt = DataType::List(ListType::new(DataType::Int64));
        let scalar = spanner_cell_to_scalar_impl(&row, &dt, "arr")
            .expect("list cell read")
            .expect("list cell not null");
        let ScalarImpl::List(list_value) = scalar else {
            panic!("expected ScalarImpl::List, got {:?}", scalar);
        };
        let datums: Vec<Datum> = list_value.iter().map(|d| d.to_owned_datum()).collect();
        assert_eq!(
            datums,
            vec![
                Some(ScalarImpl::Int64(10)),
                None,
                Some(ScalarImpl::Int64(20)),
            ]
        );
    }

    #[test]
    fn test_list_decoder_array_of_struct_preserves_field_types() {
        // Regression: INT64 fields become JSON numbers; STRING fields with
        // values like "1" or "null" must stay JSON strings, not be retyped.
        use std::collections::HashMap;
        use std::sync::Arc;

        use google_cloud_googleapis::spanner::v1::struct_type::Field as SpField;
        use google_cloud_googleapis::spanner::v1::{StructType, Type, TypeCode};
        use google_cloud_spanner::row::Row;
        use prost_types::value::Kind;
        use prost_types::{ListValue as ProstList, Value as ProstValue};
        use risingwave_common::types::ToOwnedDatum;

        // Spanner encodes STRUCT result values as positional ListValues.
        let positional_struct = |a: &str, b: &str| ProstValue {
            kind: Some(Kind::ListValue(ProstList {
                values: vec![
                    ProstValue {
                        kind: Some(Kind::StringValue(a.to_owned())),
                    },
                    ProstValue {
                        kind: Some(Kind::StringValue(b.to_owned())),
                    },
                ],
            })),
        };
        let array = ProstValue {
            kind: Some(Kind::ListValue(ProstList {
                // Second element exercises the STRING-"null" regression case.
                values: vec![positional_struct("1", "hello"), positional_struct("2", "null")],
            })),
        };

        let struct_type = StructType {
            fields: vec![
                SpField {
                    name: "a".to_owned(),
                    r#type: Some(Type {
                        code: TypeCode::Int64 as i32,
                        ..Default::default()
                    }),
                },
                SpField {
                    name: "b".to_owned(),
                    r#type: Some(Type {
                        code: TypeCode::String as i32,
                        ..Default::default()
                    }),
                },
            ],
        };
        let array_type = Type {
            code: TypeCode::Array as i32,
            array_element_type: Some(Box::new(Type {
                code: TypeCode::Struct as i32,
                struct_type: Some(struct_type),
                ..Default::default()
            })),
            struct_type: None,
            type_annotation: 0,
            proto_type_fqn: String::new(),
        };
        let field = SpField {
            name: "arr".to_owned(),
            r#type: Some(array_type),
        };
        let mut index = HashMap::new();
        index.insert("arr".to_owned(), 0);
        let row = Row::new(Arc::new(index), Arc::new(vec![field]), vec![array]);

        // `ARRAY<STRUCT<...>>` is mapped to `List(Jsonb)` by `spanner_type_to_rw_type`.
        let dt = DataType::List(ListType::new(DataType::Jsonb));
        let scalar = spanner_cell_to_scalar_impl(&row, &dt, "arr")
            .expect("ARRAY<STRUCT> read")
            .expect("ARRAY<STRUCT> must not silently become NULL");
        let ScalarImpl::List(list_value) = scalar else {
            panic!("expected ScalarImpl::List, got {:?}", scalar);
        };

        let datums: Vec<Datum> = list_value.iter().map(|d| d.to_owned_datum()).collect();
        assert_eq!(datums.len(), 2);

        let elem_json = |d: &Datum| match d.as_ref().expect("element is not null") {
            ScalarImpl::Jsonb(j) => j.clone().take(),
            other => panic!("expected Jsonb element, got {:?}", other),
        };
        let first = elem_json(&datums[0]);
        let second = elem_json(&datums[1]);

        assert_eq!(
            first,
            serde_json::json!({"a": 1, "b": "hello"}),
            "INT64 → number, STRING → string"
        );
        assert_eq!(
            second,
            serde_json::json!({"a": 2, "b": "null"}),
            "STRING value 'null' must remain the string \"null\", not JSON null"
        );
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
}
