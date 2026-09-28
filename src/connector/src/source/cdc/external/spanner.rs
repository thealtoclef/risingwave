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
//! Snapshot reads use **strong** (latest) reads, so every read observes the
//! current committed state at read time — exactly like Postgres/MySQL CDC. This
//! is what lets the CDC backfill executor safely drop change-log events ahead of
//! the backfill cursor: the snapshot that eventually covers that key is
//! guaranteed to contain its latest state.
//!
//! `current_cdc_offset()` returns the read timestamp resolved by a strong
//! read-only transaction — the latest commit position visible "now". This is the
//! Spanner analogue of Postgres CDC's `pg_current_wal_lsn()` / MySQL's
//! `SHOW MASTER STATUS`, and is used to bracket the change-log against each
//! snapshot read.
//!
//! NOTE: we deliberately do *not* pin reads to a fixed snapshot timestamp. A
//! fixed past timestamp would (1) hide rows written after it that sort ahead of
//! the cursor — those change-log events get dropped and never re-supplied,
//! losing data — and (2) risk exceeding Spanner's `version_retention_period` on
//! long backfills.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::LazyLock;

use anyhow::{Context, anyhow};
use chrono::Datelike;
use futures::stream::BoxStream;
use futures::{StreamExt, pin_mut};
use futures_async_stream::try_stream;
use google_cloud_auth::credentials::{Credentials, anonymous, service_account};
use google_cloud_spanner::client::{DatabaseClient, Spanner};
use google_cloud_spanner::model::PartitionOptions;
use google_cloud_spanner::result::Row as SpannerRow;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::transaction::{
    BeginTransactionOption, MultiUseReadOnlyTransaction, TimestampBound,
};
use google_cloud_spanner::value::{FromValue, Value};
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::log::LogSuppressor;
use risingwave_common::row::{OwnedRow, Row as OwnedRowTrait};
use risingwave_common::types::{DataType, Datum, F32, F64, ListType, ListValue, ScalarImpl};
use risingwave_common::util::env_var::env_var_is_true;
use thiserror_ext::AsReport;
use time::OffsetDateTime;

use crate::connector_common::DISABLE_DEFAULT_CREDENTIAL;
use crate::error::{ConnectorError, ConnectorResult};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CDC_TABLE_SPLIT_ID_START, CdcOffset, CdcTableSnapshotSplitOption, ExternalTableConfig,
    ExternalTableReader, SchemaTableName,
};

/// Cap on concurrent partition executions per split during CDC backfill.
///
/// Each partition runs as an independent read and buffers its full result set
/// before yielding, so this bounds both in-flight reads and peak memory per
/// split. Balanced to keep the split read well-parallelized without
/// over-fanning out.
const DEFAULT_MAX_CONCURRENT_PARTITIONS: usize = 16;

const DEFAULT_SPANNER_ENDPOINT: &str = "https://spanner.googleapis.com";

/// Upper bound for a `spanner.credentials_path` file. A service account key is
/// about 2.4 KB.
const MAX_CREDENTIALS_FILE_BYTES: u64 = 64 * 1024;

/// A position in the Spanner change stream, used as the CDC offset.
///
/// Ordered by the commit timestamp (microseconds since epoch), which is what the
/// backfill merge compares change-log events against. Also carries per-partition
/// change-stream metadata (partition token, stream name, progress) for checkpoint
/// and restoration.
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

/// Discovers table schema and primary keys from Spanner `INFORMATION_SCHEMA`.
pub struct SpannerExternalTable {
    column_descs: Vec<ColumnDesc>,
    pk_names: Vec<String>,
    table_name: String,
}

impl SpannerExternalTable {
    pub async fn connect(
        db_client: &DatabaseClient,
        config: ExternalTableConfig,
    ) -> ConnectorResult<Self> {
        let table_name = config.table.clone();

        let column_descs = Box::pin(Self::discover_columns(db_client, &table_name)).await?;
        let pk_names = Box::pin(Self::discover_primary_keys(db_client, &table_name)).await?;

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
        db_client: &DatabaseClient,
        table_name: &str,
    ) -> ConnectorResult<Vec<ColumnDesc>> {
        let (schema, table) = split_table_name(table_name);
        let stmt = Statement::builder(
            "SELECT COLUMN_NAME, SPANNER_TYPE \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table \
             ORDER BY ORDINAL_POSITION",
        )
        .add_param("schema", schema)
        .add_param("table", table)
        .build();

        let mut rows = db_client
            .single_use()
            .build()
            .execute_query(stmt)
            .await
            .context("column query")?;

        let mut descs = Vec::new();
        while let Some(row) = rows.next().await.transpose().context("column row")? {
            let name: String = row.try_get(0).context("COLUMN_NAME")?;
            let spanner_type: String = row.try_get(1).context("SPANNER_TYPE")?;
            let dt = spanner_type_to_rw_type(&spanner_type)?;
            descs.push(ColumnDesc::named(name, ColumnId::placeholder(), dt));
        }

        if descs.is_empty() {
            bail!("table '{}' not found", table_name);
        }
        Ok(descs)
    }

    async fn discover_primary_keys(
        db_client: &DatabaseClient,
        table_name: &str,
    ) -> ConnectorResult<Vec<String>> {
        let (schema, table) = split_table_name(table_name);
        let stmt = Statement::builder(
            "SELECT COLUMN_NAME \
             FROM INFORMATION_SCHEMA.INDEX_COLUMNS \
             WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND INDEX_TYPE = 'PRIMARY_KEY' \
             ORDER BY ORDINAL_POSITION",
        )
        .add_param("schema", schema)
        .add_param("table", table)
        .build();

        let mut rows = db_client
            .single_use()
            .build()
            .execute_query(stmt)
            .await
            .context("pk query")?;

        let mut names = Vec::new();
        while let Some(row) = rows.next().await.transpose().context("pk row")? {
            let name: String = row.try_get(0).context("COLUMN_NAME")?;
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
// Change stream coverage
// ---------------------------------------------------------------------------

/// The columns of one table a change stream watches. Key columns are always watched.
enum WatchedColumns {
    /// The stream watches the whole table, including columns added later.
    All,
    /// The stream watches only these non-key columns, compared case-insensitively
    /// because Spanner identifiers are case-insensitive.
    Only(HashSet<String>),
}

/// Check that the source's change stream delivers every change the CDC table needs.
///
/// A change stream can watch a subset of tables and columns. A table the stream does not
/// watch never receives a change after backfill. For a column the stream does not watch, a
/// `NEW_ROW` record carries no value, so each update overwrites the column with NULL. Both
/// are rejected. Filter options that drop some changes by design are returned as notices.
pub(crate) async fn check_change_stream_capture(
    config: &ExternalTableConfig,
    column_names: &[String],
    pk_names: &[String],
) -> ConnectorResult<Vec<String>> {
    let Some(stream) = config.spanner_change_stream_name.as_deref() else {
        return Ok(vec![]);
    };
    let client = create_spanner_client(
        &config.spanner_project,
        &config.spanner_instance,
        &config.database,
        config.emulator_host.as_deref(),
        config.credentials.as_deref(),
        config.credentials_path.as_deref(),
    )
    .await?;

    let watched = Box::pin(fetch_watched_columns(&client, stream, &config.table)).await?;
    check_watched_columns(
        stream,
        &config.table,
        watched.as_ref(),
        column_names,
        pk_names,
    )?;

    let options =
        crate::source::spanner_cdc::enumerator::fetch_change_stream_options(&client, stream)
            .await?;
    Ok(change_stream_filter_notices(stream, &options))
}

/// Read what `stream` watches of `table`, or `None` if it does not watch the table.
async fn fetch_watched_columns(
    client: &DatabaseClient,
    stream: &str,
    table: &str,
) -> ConnectorResult<Option<WatchedColumns>> {
    let (schema, table) = split_table_name(table);
    let stmt = Statement::builder(
        "SELECT `ALL` FROM INFORMATION_SCHEMA.CHANGE_STREAMS WHERE CHANGE_STREAM_NAME = @s",
    )
    .add_param("s", stream)
    .build();
    let mut rows = client
        .single_use()
        .build()
        .execute_query(stmt)
        .await
        .context("change stream query")?;
    let Some(row) = rows.next().await.transpose().context("change stream row")? else {
        bail!("change stream '{}' does not exist", stream);
    };
    let watches_all: bool = row.try_get(0).context("ALL")?;
    if watches_all {
        return Ok(Some(WatchedColumns::All));
    }

    let stmt = Statement::builder(
        "SELECT ALL_COLUMNS FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES \
         WHERE CHANGE_STREAM_NAME = @s AND TABLE_SCHEMA = @schema AND TABLE_NAME = @t",
    )
    .add_param("s", stream)
    .add_param("schema", schema)
    .add_param("t", table)
    .build();
    let mut rows = client
        .single_use()
        .build()
        .execute_query(stmt)
        .await
        .context("change stream table query")?;
    let Some(row) = rows
        .next()
        .await
        .transpose()
        .context("change stream table row")?
    else {
        return Ok(None);
    };
    let all_columns: bool = row.try_get(0).context("ALL_COLUMNS")?;
    if all_columns {
        return Ok(Some(WatchedColumns::All));
    }

    let stmt = Statement::builder(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_COLUMNS \
         WHERE CHANGE_STREAM_NAME = @s AND TABLE_SCHEMA = @schema AND TABLE_NAME = @t",
    )
    .add_param("s", stream)
    .add_param("schema", schema)
    .add_param("t", table)
    .build();
    let mut rows = client
        .single_use()
        .build()
        .execute_query(stmt)
        .await
        .context("change stream column query")?;
    let mut columns = HashSet::new();
    while let Some(row) = rows
        .next()
        .await
        .transpose()
        .context("change stream column row")?
    {
        let name: String = row.try_get(0).context("COLUMN_NAME")?;
        columns.insert(name.to_lowercase());
    }
    Ok(Some(WatchedColumns::Only(columns)))
}

fn check_watched_columns(
    stream: &str,
    table: &str,
    watched: Option<&WatchedColumns>,
    column_names: &[String],
    pk_names: &[String],
) -> ConnectorResult<()> {
    let Some(watched) = watched else {
        bail!(
            "change stream '{}' does not watch table '{}', so the table would never receive \
             changes after backfill. Add the table to the change stream",
            stream,
            table,
        );
    };
    let WatchedColumns::Only(watched) = watched else {
        return Ok(());
    };
    let unwatched: Vec<&str> = column_names
        .iter()
        .filter(|c| !pk_names.iter().any(|pk| pk.eq_ignore_ascii_case(c)))
        .filter(|c| !watched.contains(&c.to_lowercase()))
        .map(String::as_str)
        .collect();
    if !unwatched.is_empty() {
        bail!(
            "change stream '{}' does not watch columns {:?} of table '{}': every update would \
             overwrite them with NULL. Watch the whole table in the change stream, or leave \
             these columns out of the table definition",
            stream,
            unwatched,
            table,
        );
    }
    Ok(())
}

/// Notices for change stream options that leave some upstream changes out of the table.
fn change_stream_filter_notices(stream: &str, options: &HashMap<String, String>) -> Vec<String> {
    const FILTERS: [(&str, &str); 5] = [
        ("exclude_insert", "inserts are not applied to the table"),
        ("exclude_update", "updates are not applied to the table"),
        ("exclude_delete", "deletes are not applied to the table"),
        (
            "exclude_ttl_deletes",
            "rows deleted by a TTL policy stay in the table",
        ),
        (
            "allow_txn_exclusion",
            "transactions written with exclude_txn_from_change_streams are not applied to the table",
        ),
    ];
    FILTERS
        .iter()
        .filter(|(option, _)| {
            options
                .get(*option)
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"))
        })
        .map(|(option, effect)| format!("change stream '{stream}' sets {option}: {effect}"))
        .collect()
}

// ---------------------------------------------------------------------------
// Snapshot reader
// ---------------------------------------------------------------------------

/// Reads snapshot data from Spanner for CDC backfill.
///
/// Implements two-level parallelism:
/// 1. **Inter-actor**: PK range splits distribute work across compute nodes
/// 2. **Intra-actor**: Within each PK range, uses Spanner's `partition_query` to
///    further parallelize reads using `BatchReadOnlyTransaction`
///
/// All snapshot reads use **strong** (latest) reads. The CDC offset is the read
/// timestamp resolved by a strong read-only transaction, obtained on demand in
/// [`ExternalTableReader::current_cdc_offset`]. There is no pinned snapshot
/// timestamp.
pub struct SpannerExternalTableReader {
    rw_schema: Schema,
    field_names: String,
    pk_names: Vec<String>,
    pk_types: Vec<DataType>,
    table_name: String,
    enable_databoost: bool,
    /// Database client. Shared across transactions and partition executions.
    db_client: DatabaseClient,
}

impl SpannerExternalTableReader {
    /// Quotes a Spanner identifier (column or table name) with backticks to
    /// prevent reserved-word conflicts.
    fn quote_column(name: &str) -> String {
        format!("`{}`", name)
    }

    /// Quotes a table name, quoting the schema and table separately for a table in a
    /// named schema (`` `sch`.`t` ``): `` `sch.t` `` would name a table called `sch.t`.
    fn quote_table(name: &str) -> String {
        match split_table_name(name) {
            ("", table) => Self::quote_column(table),
            (schema, table) => format!(
                "{}.{}",
                Self::quote_column(schema),
                Self::quote_column(table)
            ),
        }
    }

    pub async fn new(config: ExternalTableConfig, schema: Schema) -> ConnectorResult<Self> {
        let enable_databoost = config.spanner_databoost_enabled;
        let db_client = create_spanner_client(
            &config.spanner_project,
            &config.spanner_instance,
            &config.database,
            config.emulator_host.as_deref(),
            config.credentials.as_deref(),
            config.credentials_path.as_deref(),
        )
        .await?;
        let external_table = SpannerExternalTable::connect(&db_client, config).await?;

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
            table_name: external_table.table_name().to_owned(),
            enable_databoost,
            db_client,
        })
    }
}

impl ExternalTableReader for SpannerExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        // The current CDC offset is the latest commit position visible "now" — the
        // Spanner analogue of Postgres's `pg_current_wal_lsn()`.
        //
        // We obtain it from the read timestamp that Spanner resolves for a strong
        // read-only transaction. Using `ExplicitBegin` forces a `BeginTransaction`
        // RPC at build time, so the read timestamp is available immediately without
        // executing any query (no `SELECT CURRENT_TIMESTAMP` round-trip needed).
        let txn = self
            .db_client
            .read_only_transaction()
            .with_begin_transaction_option(BeginTransactionOption::ExplicitBegin)
            // No `set_timestamp_bound`: the transaction defaults to a strong (latest) read.
            .build()
            .await
            .context("failed to begin strong read-only transaction for current CDC offset")?;
        let read_ts = txn.read_timestamp().ok_or_else(|| {
            anyhow!("Spanner did not return a read timestamp for the strong read-only transaction")
        })?;
        Ok(CdcOffset::Spanner(SpannerOffset::new(
            spanner_ts_to_micros(&read_ts),
        )))
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
    /// Returns the split column as a `Field` based on the `backfill_split_pk_column_index`.
    ///
    /// Follows Postgres CDC's `split_column` pattern.
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
        txn: &mut MultiUseReadOnlyTransaction,
        split_column: &Field,
    ) -> ConnectorResult<Option<(ScalarImpl, ScalarImpl)>> {
        let col = Self::quote_column(&split_column.name);
        let tbl = Self::quote_table(&self.table_name);
        let minmax_query =
            format!("SELECT MIN({col}) as min_val, MAX({col}) as max_val FROM {tbl}",);

        let stmt = Statement::builder(&minmax_query).build();
        let mut rows = txn
            .execute_query(stmt)
            .await
            .context("min/max query failed")?;

        if let Some(row) = rows
            .next()
            .await
            .transpose()
            .context("PK range row failed")?
        {
            let min_val = spanner_cell_to_datum(&row, &split_column.data_type, 0)?;
            let max_val = spanner_cell_to_datum(&row, &split_column.data_type, 1)?;
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
        txn: &mut MultiUseReadOnlyTransaction,
        left_value: &ScalarImpl,
        max_value: &ScalarImpl,
        max_split_size: u64,
        split_column: &Field,
    ) -> ConnectorResult<Option<Datum>> {
        let col = Self::quote_column(&split_column.name);
        let tbl = Self::quote_table(&self.table_name);
        let sql = format!(
            "WITH t AS (SELECT {col} FROM {tbl} WHERE {col} >= @left ORDER BY {col} ASC LIMIT {max_split_size}) \
             SELECT CASE WHEN MAX({col}) < @max THEN MAX({col}) ELSE NULL END AS val FROM t",
        );

        let stmt = Statement::builder(&sql);
        let stmt = add_scalar_param(stmt, "left", left_value)?;
        let stmt = add_scalar_param(stmt, "max", max_value)?;
        let stmt = stmt.build();

        let mut rows = txn
            .execute_query(stmt)
            .await
            .context("boundary query failed")?;

        if let Some(row) = rows
            .next()
            .await
            .transpose()
            .context("boundary row fetch failed")?
        {
            let datum = spanner_cell_to_datum(&row, &split_column.data_type, 0)?;
            Ok(Some(datum))
        } else {
            Ok(None)
        }
    }

    /// Finds the next greater distinct value when all rows have the same PK value.
    async fn next_greater_bound(
        &self,
        txn: &mut MultiUseReadOnlyTransaction,
        start_offset: &ScalarImpl,
        max_value: &ScalarImpl,
        split_column: &Field,
    ) -> ConnectorResult<Option<Datum>> {
        let col = Self::quote_column(&split_column.name);
        let tbl = Self::quote_table(&self.table_name);
        let sql =
            format!("SELECT MIN({col}) AS val FROM {tbl} WHERE {col} > @start AND {col} < @max",);

        let stmt = Statement::builder(&sql);
        let stmt = add_scalar_param(stmt, "start", start_offset)?;
        let stmt = add_scalar_param(stmt, "max", max_value)?;
        let stmt = stmt.build();

        let mut rows = txn
            .execute_query(stmt)
            .await
            .context("next_greater_bound query failed")?;

        if let Some(row) = rows
            .next()
            .await
            .transpose()
            .context("next_greater_bound row fetch failed")?
        {
            let datum = spanner_cell_to_datum(&row, &split_column.data_type, 0)?;
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

        tracing::info!("PK range enumeration started (even splits)");

        // Use a strong (latest) read. Splits always cover (-inf, +inf) via the
        // unbounded first/last splits, so generating boundaries from the current
        // data distribution is safe even as the table changes.
        let mut txn = self
            .db_client
            .read_only_transaction()
            .set_timestamp_bound(TimestampBound::strong())
            .build()
            .await
            .context("failed to create strong read-only transaction")?;

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

        tracing::info!("PK range enumeration started (uneven splits)");

        // Use a strong (latest) read. Splits always cover (-inf, +inf) via the
        // unbounded first/last splits, so generating boundaries from the current
        // data distribution is safe even as the table changes.
        let mut txn = self
            .db_client
            .read_only_transaction()
            .set_timestamp_bound(TimestampBound::strong())
            .build()
            .await
            .context("failed to create strong read-only transaction")?;

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
            if let Some(Some(ref inner)) = next_right
                && *inner == next_left_bound_inclusive
            {
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
        let pk_names = &self.pk_names;
        let order_key = Self::get_order_key(&primary_keys);

        let stmt = if let Some(ref pk_row) = start_pk_row {
            let primary_keys: Vec<String> = self.pk_names.clone();
            let order_key = Self::get_order_key(&primary_keys);
            // Build filter: `pk0` > @pk0 OR (`pk0` = @pk0 AND `pk1` > @pk1) ...
            let filter = build_pk_filter_sql(&primary_keys);
            let sql = format!(
                "SELECT {} FROM {} WHERE {} ORDER BY {} LIMIT {}",
                self.field_names,
                Self::quote_table(&self.table_name),
                filter,
                order_key,
                scan_limit
            );
            let stmt = Statement::builder(&sql);
            add_pk_params(stmt, pk_row)?.build()
        } else {
            let sql = format!(
                "SELECT {} FROM {} ORDER BY {} LIMIT {}",
                self.field_names,
                Self::quote_table(&self.table_name),
                order_key,
                scan_limit
            );
            Statement::builder(&sql).build()
        };

        // Use a strong (latest) read so the snapshot reflects the current committed
        // state at read time. Note: we use a regular read_only_transaction (not batch)
        // because the query has a LIMIT clause, which cannot be partitioned.
        let txn = self
            .db_client
            .read_only_transaction()
            .set_timestamp_bound(TimestampBound::strong())
            .build()
            .await
            .context("failed to create strong read-only transaction")?;

        // Execute the query directly (no partition, since LIMIT queries can't be partitioned)
        let mut rows = txn
            .execute_query(stmt)
            .await
            .context("snapshot query failed")?;

        while let Some(row) = rows.next().await.transpose().context("row read failed")? {
            yield spanner_row_to_owned_row(&row, fields, pk_names)?;
        }
    }

    /// Read a split using PK range-based filtering with intra-actor parallelism.
    ///
    /// Two-level parallelism:
    /// 1. **Inter-actor**: This split is one PK range assigned to this actor
    /// 2. **Intra-actor**: Within this PK range, uses Spanner's `partition_query`
    ///    to further parallelize reads
    ///
    /// Creates a strong (latest) `BatchReadOnlyTransaction`, then uses `partition_query`
    /// with WHERE clause filtering for intra-actor parallelism. The change-log is
    /// reconciled against this read via the offsets captured by
    /// [`ExternalTableReader::current_cdc_offset`] before and after the read.
    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn split_snapshot_read_inner(
        &self,
        _table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) {
        let fields = self.rw_schema.fields();
        let pk_names = &self.pk_names;

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

        let is_first_split = left[0].is_none();
        let is_last_split = right[0].is_none();

        tracing::info!(
            "split_snapshot_read: PK range=[{:?}, {:?}), strong read",
            left[0],
            right[0],
        );

        // Create a strong (latest) BatchReadOnlyTransaction so the read reflects the
        // current committed state. Each split/actor reads at its own resolved
        // timestamp; cross-split consistency is provided by change-log reconciliation,
        // not by a shared pinned timestamp (matches Postgres/MySQL CDC).
        let txn = self
            .db_client
            .batch_read_only_transaction()
            .set_timestamp_bound(TimestampBound::strong())
            .build()
            .await
            .context("failed to create strong batch read-only transaction")?;

        // Build query with PK range WHERE clause using parameter binding
        // Follows Postgres CDC pattern: split on single column specified by backfill_split_pk_column_index
        let where_clause = build_split_filter_sql(&split_columns[0], is_first_split, is_last_split);
        let sql = format!(
            "SELECT {} FROM {} {}",
            self.field_names,
            Self::quote_table(&self.table_name),
            where_clause
        );
        let mut stmt = Statement::builder(&sql);
        if let Some(ref scalar) = left[0] {
            stmt = add_scalar_param(stmt, "pk_start", scalar)?;
        }
        if let Some(ref scalar) = right[0] {
            stmt = add_scalar_param(stmt, "pk_end", scalar)?;
        }
        let stmt = stmt.build();

        tracing::info!(
            "split_snapshot_read: executing partition_query with databoost={}, where={}",
            self.enable_databoost,
            where_clause
        );

        // partition_query returns Partition handles that can be cloned and executed
        // concurrently against the same DatabaseClient.
        let partitions = txn
            .partition_query(stmt, PartitionOptions::default())
            .await
            .context("failed to get partitions for PK range")?;

        let data_boost = self.enable_databoost;
        let db_client = self.db_client.clone();
        let partition_count = partitions.len();
        tracing::info!(partition_count, "split_snapshot_read: fanning out");

        // Fan out partitions and stream completed batches. Each partition
        // future materializes its full result set into a `Vec<OwnedRow>` before
        // yielding, so `buffer_unordered` holds completed batches rather than
        // row-yielding streams. In-flight reads and peak memory per split are
        // bounded by `DEFAULT_MAX_CONCURRENT_PARTITIONS`.
        let mut stream = futures::stream::iter(partitions.into_iter().map(|p| {
            let p = p.set_data_boost(data_boost);
            let db = db_client.clone();
            let fields = &fields;
            async move {
                let mut rs = p.execute(&db).await?;
                let mut out: Vec<OwnedRow> = Vec::new();
                while let Some(row) = rs.next().await.transpose()? {
                    out.push(spanner_row_to_owned_row(&row, fields, pk_names)?);
                }
                Ok::<_, anyhow::Error>(out)
            }
        }))
        .buffer_unordered(DEFAULT_MAX_CONCURRENT_PARTITIONS);

        while let Some(batch) = stream.next().await {
            for row in batch? {
                yield row;
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

/// Convert i64 to `ScalarImpl` based on data type (follows Postgres CDC's `to_int_scalar`)
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
/// Follows Postgres CDC's `try_increase_split_id` pattern.
fn try_increase_split_id(split_id: &mut i64) -> ConnectorResult<()> {
    match split_id.checked_add(1) {
        Some(s) => {
            *split_id = s;
            Ok(())
        }
        None => Err(anyhow!("too many CDC snapshot splits").into()),
    }
}

/// Adds a `ScalarImpl` value as a named parameter to a Spanner `StatementBuilder`.
fn add_scalar_param(
    stmt: google_cloud_spanner::statement::StatementBuilder,
    name: &str,
    scalar: &ScalarImpl,
) -> ConnectorResult<google_cloud_spanner::statement::StatementBuilder> {
    Ok(stmt.add_param(name, scalar_to_spanner_value(scalar)?))
}

/// Converts a RisingWave scalar to a Spanner parameter value, covering every type a
/// Spanner primary-key column maps to.
///
/// The value is bound untyped, so Spanner infers its type from the SQL: a NUMERIC is sent
/// as its decimal string, like the SDK encodes one.
fn scalar_to_spanner_value(scalar: &ScalarImpl) -> ConnectorResult<Value> {
    let value = match scalar {
        ScalarImpl::Int16(v) => Value::from(*v as i64),
        ScalarImpl::Int32(v) => Value::from(*v as i64),
        ScalarImpl::Int64(v) => Value::from(*v),
        ScalarImpl::Float32(v) => Value::from(v.0 as f64),
        ScalarImpl::Float64(v) => Value::from(v.0),
        ScalarImpl::Utf8(v) => Value::from(v.as_ref().to_owned()),
        ScalarImpl::Bool(v) => Value::from(*v),
        ScalarImpl::Decimal(v) => Value::from(v.to_string()),
        ScalarImpl::Bytea(v) => Value::from(v.to_vec()),
        ScalarImpl::Timestamptz(v) => Value::from(micros_to_offset_datetime(v.timestamp_micros())?),
        ScalarImpl::Date(v) => {
            let month = time::Month::try_from(v.0.month() as u8)
                .map_err(|e| anyhow!("invalid date {}: {}", v, e))?;
            let date = time::Date::from_calendar_date(v.0.year(), month, v.0.day() as u8)
                .map_err(|e| anyhow!("invalid date {}: {}", v, e))?;
            Value::from(date)
        }
        _ => bail!(
            "unsupported ScalarImpl type for Spanner param binding: {:?}",
            scalar
        ),
    };
    Ok(value)
}

/// Splits a table name into its schema and table, the way change records name a table:
/// `sch.t` for table `t` in the named schema `sch`, and `t` for a table in the default
/// schema, whose `INFORMATION_SCHEMA.TABLE_SCHEMA` is `''`. Spanner identifiers cannot
/// contain `.`, so the first `.` separates the two.
fn split_table_name(name: &str) -> (&str, &str) {
    name.split_once('.').unwrap_or(("", name))
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
        for (j, col) in cols.iter().enumerate().take(i) {
            parts.push(format!("{} = @pk{}", col, j));
        }
        // The i-th column must be strictly greater
        parts.push(format!("{} > @pk{}", cols[i], i));
        clauses.push(format!("({})", parts.join(" AND ")));
    }
    clauses.join(" OR ")
}

/// Builds the `WHERE` clause that reads one snapshot split, `[@pk_start, @pk_end)` on the
/// split column. The first split has no lower bound and the last no upper bound.
///
/// The edge splits also read the keys that fail every range comparison in Spanner, so that
/// each row is read by the split that `filter_stream_chunk` routes its changes to:
/// - NULL sorts first there, so the first split reads NULL keys.
/// - `NaN` sorts last there, so the last split reads `NaN` keys of a FLOAT64 column (Spanner
///   does not allow FLOAT32 key columns).
fn build_split_filter_sql(
    split_column: &Field,
    is_first_split: bool,
    is_last_split: bool,
) -> String {
    let col = SpannerExternalTableReader::quote_column(&split_column.name);
    match (is_first_split, is_last_split) {
        (true, true) => String::new(),
        (true, false) => format!("WHERE ({col} < @pk_end OR {col} IS NULL)"),
        (false, true) if split_column.data_type == DataType::Float64 => {
            format!("WHERE ({col} >= @pk_start OR IS_NAN({col}))")
        }
        (false, true) => format!("WHERE {col} >= @pk_start"),
        (false, false) => format!("WHERE {col} >= @pk_start AND {col} < @pk_end"),
    }
}

/// Adds PK row values as named parameters (@pk0, @pk1, ...) to a Spanner `StatementBuilder`.
fn add_pk_params(
    mut stmt: google_cloud_spanner::statement::StatementBuilder,
    pk_row: &OwnedRow,
) -> ConnectorResult<google_cloud_spanner::statement::StatementBuilder> {
    for (i, datum_ref) in pk_row.iter().enumerate() {
        if let Some(scalar_ref) = datum_ref {
            let scalar = scalar_ref.into_scalar_impl();
            stmt = add_scalar_param(stmt, &format!("pk{}", i), &scalar)?;
        }
    }
    Ok(stmt)
}

// ---------------------------------------------------------------------------
// Spanner client factory
// ---------------------------------------------------------------------------

/// Resolve endpoint and authentication together so emulator clients never use real credentials.
///
/// Without explicit credentials a production client falls back to Application Default
/// Credentials, which authenticate as the RisingWave node. `default_credentials_allowed`
/// is false when `DISABLE_DEFAULT_CREDENTIAL` forbids that fallback.
fn spanner_client_connection_config(
    emulator_host: Option<&str>,
    credentials: Option<&str>,
    credentials_path: Option<&str>,
    default_credentials_allowed: bool,
) -> ConnectorResult<(String, Option<Credentials>)> {
    if let Some(host) = emulator_host {
        if credentials.is_some() || credentials_path.is_some() {
            bail!(
                "spanner.emulator_host cannot be combined with spanner.credentials or spanner.credentials_path"
            );
        }
        if host.trim().is_empty() {
            bail!("spanner.emulator_host must not be empty");
        }
        let endpoint = if url::Url::parse(host).is_ok_and(|url| url.has_host()) {
            host.to_owned()
        } else {
            format!("http://{host}")
        };
        // Set anonymous credentials explicitly, including for HTTPS emulators where
        // the SDK would otherwise fall back to Application Default Credentials.
        return Ok((endpoint, Some(anonymous::Builder::new().build())));
    }

    let credentials = if let Some(json) = credentials {
        Some(build_service_account_credentials(json)?)
    } else if let Some(path) = credentials_path {
        let content = read_credentials_file(path)
            .with_context(|| format!("failed to read credentials file: {}", path))?;
        Some(build_service_account_credentials(&content)?)
    } else if default_credentials_allowed {
        None
    } else {
        bail!(
            "Google Application Default Credentials are disabled; configure `spanner.credentials` or `spanner.credentials_path`"
        );
    };

    // An explicit endpoint prevents the SDK from redirecting production clients
    // through SPANNER_EMULATOR_HOST inherited from the process environment.
    Ok((DEFAULT_SPANNER_ENDPOINT.to_owned(), credentials))
}

/// Create a Spanner `DatabaseClient` from connection parameters.
///
/// Shared by both `SpannerExternalTable` (backfill) and `SpannerCdcProperties` (CDC reader).
pub(crate) async fn create_spanner_client(
    project: &str,
    instance: &str,
    database: &str,
    emulator_host: Option<&str>,
    credentials: Option<&str>,
    credentials_path: Option<&str>,
) -> ConnectorResult<DatabaseClient> {
    if project.is_empty() || instance.is_empty() || database.is_empty() {
        bail!("spanner.project, spanner.instance, and database.name are required");
    }

    let (endpoint, credentials) = spanner_client_connection_config(
        emulator_host,
        credentials,
        credentials_path,
        !env_var_is_true(DISABLE_DEFAULT_CREDENTIAL),
    )?;

    let dsn = format!(
        "projects/{}/instances/{}/databases/{}",
        project, instance, database
    );

    let mut builder = Spanner::builder().with_endpoint(endpoint);
    if let Some(creds) = credentials {
        builder = builder.with_credentials(creds);
    }

    let spanner = builder
        .build()
        .await
        .context("failed to create Spanner client")?;
    let db_client = spanner
        .database_client(&dsn)
        .build()
        .await
        .context("failed to create Spanner database client")?;
    Ok(db_client)
}

/// Read a service account key file from a path given in the source options.
///
/// The path comes from the user, so only regular files up to
/// `MAX_CREDENTIALS_FILE_BYTES` are read: a path such as `/dev/zero` must not
/// exhaust the memory of the frontend, meta or compute node that reads it.
fn read_credentials_file(path: &str) -> std::io::Result<String> {
    use std::io::Read;

    let file = std::fs::File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(std::io::Error::other("not a regular file"));
    }
    if metadata.len() > MAX_CREDENTIALS_FILE_BYTES {
        return Err(std::io::Error::other(format!(
            "file is larger than {MAX_CREDENTIALS_FILE_BYTES} bytes"
        )));
    }
    // The size can still change after the check, so bound the read itself too.
    let mut content = String::new();
    file.take(MAX_CREDENTIALS_FILE_BYTES + 1)
        .read_to_string(&mut content)?;
    if content.len() as u64 > MAX_CREDENTIALS_FILE_BYTES {
        return Err(std::io::Error::other(format!(
            "file is larger than {MAX_CREDENTIALS_FILE_BYTES} bytes"
        )));
    }
    Ok(content)
}

fn build_service_account_credentials(json: &str) -> ConnectorResult<Credentials> {
    let key: serde_json::Value =
        serde_json::from_str(json).with_context(|| "credentials JSON is not valid JSON")?;
    let creds = service_account::Builder::new(key)
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build service account credentials: {:?}", e))?;
    Ok(creds)
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

/// Convert microseconds since epoch to `OffsetDateTime`.
pub fn micros_to_offset_datetime(micros: i64) -> ConnectorResult<OffsetDateTime> {
    Ok(
        OffsetDateTime::from_unix_timestamp_nanos((micros as i128) * 1000)
            .map_err(|e| anyhow!("invalid microseconds timestamp {}: {}", micros, e))?,
    )
}

/// Convert a `google_cloud_wkt::Timestamp` to microseconds since epoch.
///
/// Sub-microsecond nanos are truncated; `SpannerOffset` tracks microsecond
/// precision, matching how change-stream commit timestamps are stored.
fn spanner_ts_to_micros(ts: &google_cloud_wkt::Timestamp) -> i64 {
    ts.seconds()
        .saturating_mul(1_000_000)
        .saturating_add((ts.nanos() as i64) / 1000)
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

/// Decodes a single typed cell from a Spanner row.
///
/// Returns `Ok(None)` for a SQL NULL and an error for a value that does not decode or
/// convert, so a bad value is never mistaken for NULL.
fn spanner_cell_to_datum(
    row: &SpannerRow,
    data_type: &DataType,
    idx: usize,
) -> anyhow::Result<Datum> {
    let datum = match data_type {
        DataType::Boolean => row.try_get::<Option<bool>, _>(idx)?.map(ScalarImpl::Bool),
        DataType::Int64 => row.try_get::<Option<i64>, _>(idx)?.map(ScalarImpl::Int64),
        DataType::Int32 => row
            .try_get::<Option<i64>, _>(idx)?
            .map(|v| i32::try_from(v).map(ScalarImpl::Int32))
            .transpose()?,
        DataType::Int16 => row
            .try_get::<Option<i64>, _>(idx)?
            .map(|v| i16::try_from(v).map(ScalarImpl::Int16))
            .transpose()?,
        DataType::Float64 => row
            .try_get::<Option<f64>, _>(idx)?
            .map(|v| ScalarImpl::Float64(F64::from(v))),
        DataType::Float32 => row
            .try_get::<Option<f32>, _>(idx)?
            .map(|v| ScalarImpl::Float32(F32::from(v))),
        // STRING, and ENUM, which is sent as a string.
        DataType::Varchar => row
            .try_get::<Option<String>, _>(idx)?
            .map(|s| ScalarImpl::Utf8(s.into())),
        // BYTES and PROTO, both base64 on the wire and decoded by the SDK.
        DataType::Bytea => row
            .try_get::<Option<Vec<u8>>, _>(idx)?
            .map(|v| ScalarImpl::Bytea(v.into())),
        DataType::Timestamptz => row
            .try_get::<Option<OffsetDateTime>, _>(idx)?
            .map(offset_datetime_to_timestamptz)
            .transpose()?,
        DataType::Timestamp => row
            .try_get::<Option<OffsetDateTime>, _>(idx)?
            .map(|v| {
                let micros = (v.unix_timestamp_nanos() / 1000) as i64;
                risingwave_common::types::Timestamp::with_micros(micros).map(ScalarImpl::Timestamp)
            })
            .transpose()?,
        DataType::Date => row
            .try_get::<Option<time::Date>, _>(idx)?
            .map(spanner_date_to_scalar)
            .transpose()?,
        // Spanner sends NUMERIC as a decimal string.
        DataType::Decimal => row
            .try_get::<Option<String>, _>(idx)?
            .map(|s| parse_spanner_numeric(&s))
            .transpose()?,
        DataType::Jsonb => row.try_get::<Option<String>, _>(idx)?.map(|s| {
            // Try to parse as JSON first
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&s) {
                ScalarImpl::Jsonb(json.into())
            } else {
                // If not valid JSON, wrap as string
                ScalarImpl::Jsonb(serde_json::json!(s).into())
            }
        }),
        DataType::List(list_type) => {
            spanner_array_to_list(row, list_type.elem(), idx)?.map(ScalarImpl::List)
        }

        // Unknown or unsupported types - try to read as string as fallback
        _ => {
            tracing::warn!(
                "column at index {} has unsupported type {:?} - reading as string fallback",
                idx,
                data_type
            );
            row.try_get::<Option<String>, _>(idx)?
                .map(|s| ScalarImpl::Utf8(s.into()))
        }
    };
    Ok(datum)
}

fn offset_datetime_to_timestamptz(v: OffsetDateTime) -> anyhow::Result<ScalarImpl> {
    let micros = (v.unix_timestamp_nanos() / 1000) as i64;
    risingwave_common::types::Timestamptz::from_micros(micros)
        .map(ScalarImpl::Timestamptz)
        .ok_or_else(|| anyhow!("timestamp {v} is out of range"))
}

/// Converts a Spanner DATE to a RisingWave `Date`.
///
/// Built from the numeric year, month and day: `time::Month` displays as its name, so
/// formatting it into a `YYYY-MM-DD` string does not round-trip.
fn spanner_date_to_scalar(v: time::Date) -> anyhow::Result<ScalarImpl> {
    chrono::NaiveDate::from_ymd_opt(v.year(), v.month() as u32, v.day() as u32)
        .map(|d| ScalarImpl::Date(risingwave_common::types::Date::new(d)))
        .ok_or_else(|| anyhow!("date {v} is out of range"))
}

fn parse_spanner_numeric(s: &str) -> anyhow::Result<ScalarImpl> {
    risingwave_common::types::Decimal::from_str(s)
        .map(ScalarImpl::Decimal)
        .map_err(|e| anyhow!("NUMERIC {s} does not fit DECIMAL: {e}"))
}

/// Reads an ARRAY cell as a `ListValue` of `elem_type`.
///
/// Spanner sends an ARRAY as a list value, not a string, so each element is decoded by the
/// SDK with the same `FromValue` impls the scalar branches of `spanner_cell_to_datum`
/// use. Returns `Ok(None)` for a NULL array.
fn spanner_array_to_list(
    row: &SpannerRow,
    elem_type: &DataType,
    idx: usize,
) -> anyhow::Result<Option<ListValue>> {
    fn collect<T: FromValue>(
        row: &SpannerRow,
        idx: usize,
        elem_type: &DataType,
        convert: impl Fn(T) -> anyhow::Result<ScalarImpl>,
    ) -> anyhow::Result<Option<ListValue>> {
        let Some(items) = row.try_get::<Option<Vec<Option<T>>>, _>(idx)? else {
            return Ok(None);
        };
        let datums = items
            .into_iter()
            .map(|item| item.map(&convert).transpose())
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Some(ListValue::from_datum_iter(elem_type, datums)))
    }

    match elem_type {
        DataType::Boolean => collect(row, idx, elem_type, |v: bool| Ok(ScalarImpl::Bool(v))),
        DataType::Int64 => collect(row, idx, elem_type, |v: i64| Ok(ScalarImpl::Int64(v))),
        DataType::Float64 => collect(row, idx, elem_type, |v: f64| {
            Ok(ScalarImpl::Float64(F64::from(v)))
        }),
        DataType::Float32 => collect(row, idx, elem_type, |v: f32| {
            Ok(ScalarImpl::Float32(F32::from(v)))
        }),
        DataType::Varchar => collect(row, idx, elem_type, |v: String| {
            Ok(ScalarImpl::Utf8(v.into()))
        }),
        DataType::Bytea => collect(row, idx, elem_type, |v: Vec<u8>| {
            Ok(ScalarImpl::Bytea(v.into()))
        }),
        DataType::Timestamptz => collect(row, idx, elem_type, offset_datetime_to_timestamptz),
        DataType::Date => collect(row, idx, elem_type, spanner_date_to_scalar),
        // NUMERIC is a decimal string on the wire, as in the scalar branch.
        DataType::Decimal => collect(row, idx, elem_type, |v: String| parse_spanner_numeric(&v)),
        DataType::Jsonb => collect(row, idx, elem_type, |v: serde_json::Value| {
            Ok(ScalarImpl::Jsonb(v.into()))
        }),
        _ => bail!("array of unsupported element type {elem_type:?}"),
    }
}

/// Decodes a snapshot row. A primary-key value that does not decode fails the read, as in
/// the other CDC snapshot readers; any other column is logged and read as NULL. Unlike
/// those readers, a NULL key is valid, since Spanner allows NULL key columns.
fn spanner_row_to_owned_row(
    row: &SpannerRow,
    fields: &[Field],
    pk_names: &[String],
) -> ConnectorResult<OwnedRow> {
    static LOG_SUPPRESSOR: LazyLock<LogSuppressor> = LazyLock::new(LogSuppressor::default);

    let mut datums = Vec::with_capacity(fields.len());
    for (idx, field) in fields.iter().enumerate() {
        let datum = match spanner_cell_to_datum(row, &field.data_type, idx) {
            Ok(datum) => datum,
            Err(err) if pk_names.contains(&field.name) => {
                return Err(err
                    .context(format!(
                        "failed to decode Spanner snapshot primary key `{}`",
                        field.name
                    ))
                    .into());
            }
            Err(err) => {
                if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                    tracing::error!(
                        column = %field.name,
                        error = %err.as_report(),
                        suppressed_count,
                        "parse column failed"
                    );
                }
                None
            }
        };
        datums.push(datum);
    }
    Ok(OwnedRow::new(datums))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_spanner_emulator_endpoint_and_anonymous_credentials() {
        use google_cloud_auth::credentials::CacheableResource;

        let previous_host = std::env::var_os("SPANNER_EMULATOR_HOST");
        for (host, expected) in [
            ("localhost:9010", "http://localhost:9010"),
            ("http://localhost:9010", "http://localhost:9010"),
            ("https://emulator.example", "https://emulator.example"),
        ] {
            let (endpoint, credentials) =
                spanner_client_connection_config(Some(host), None, None, true).unwrap();
            assert_eq!(endpoint, expected);
            let headers = credentials
                .unwrap()
                .headers(Default::default())
                .await
                .unwrap();
            match headers {
                CacheableResource::New { data, .. } => assert!(data.is_empty()),
                CacheableResource::NotModified => panic!("expected fresh anonymous headers"),
            }
        }
        assert_eq!(std::env::var_os("SPANNER_EMULATOR_HOST"), previous_host);

        // Configuring an emulator must not change a subsequent production client.
        let (endpoint, credentials) =
            spanner_client_connection_config(None, None, None, true).unwrap();
        assert_eq!(endpoint, DEFAULT_SPANNER_ENDPOINT);
        assert!(credentials.is_none());
    }

    #[tokio::test]
    async fn test_spanner_client_passes_endpoint_to_sdk_builder() {
        // An invalid endpoint must fail while building the SDK client, before
        // creating a database client. This fails if `with_endpoint` is removed.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            create_spanner_client(
                "project",
                "instance",
                "database",
                Some(":::invalid-uri"),
                None,
                None,
            ),
        )
        .await
        .expect("SDK client build should reject the invalid endpoint promptly");
        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("failed to create Spanner client"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_spanner_emulator_rejects_explicit_credentials_before_loading() {
        for (credentials, path) in [
            (Some("invalid JSON"), None),
            (None, Some("/nonexistent/spanner-service-account.json")),
            (Some("invalid JSON"), Some("/nonexistent/key.json")),
        ] {
            let error =
                spanner_client_connection_config(Some("localhost:9010"), credentials, path, true)
                    .unwrap_err();
            assert!(error.to_string().contains("cannot be combined"));
        }
    }

    #[test]
    fn test_spanner_emulator_rejects_empty_host() {
        for host in ["", "   "] {
            let error = spanner_client_connection_config(Some(host), None, None, true).unwrap_err();
            assert!(error.to_string().contains("must not be empty"));
        }
    }

    /// With `DISABLE_DEFAULT_CREDENTIAL` set, a source without credentials must not
    /// authenticate as the RisingWave node. Emulator clients are unaffected.
    #[test]
    fn test_spanner_default_credentials_can_be_disabled() {
        let error = spanner_client_connection_config(None, None, None, false).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Default Credentials are disabled"),
            "unexpected error: {error}"
        );
        assert!(
            spanner_client_connection_config(Some("localhost:9010"), None, None, false).is_ok()
        );
    }

    #[test]
    fn test_read_credentials_file_rejects_non_regular_and_oversized_files() {
        let dir = tempfile::tempdir().unwrap();
        let error = read_credentials_file(dir.path().to_str().unwrap()).unwrap_err();
        assert!(error.to_string().contains("not a regular file"), "{error}");

        let oversized = dir.path().join("oversized.json");
        std::fs::write(
            &oversized,
            vec![b' '; MAX_CREDENTIALS_FILE_BYTES as usize + 1],
        )
        .unwrap();
        let error = read_credentials_file(oversized.to_str().unwrap()).unwrap_err();
        assert!(error.to_string().contains("larger than"), "{error}");

        let key = dir.path().join("key.json");
        std::fs::write(&key, "{}").unwrap();
        assert_eq!(read_credentials_file(key.to_str().unwrap()).unwrap(), "{}");
    }

    #[test]
    fn test_spanner_offset() {
        let offset = SpannerOffset::new(1234567890);
        assert_eq!(offset.timestamp, 1234567890);
    }

    #[test]
    fn test_table_type_offset_parser_supports_spanner() {
        // The legacy (non-parallelized) CDC backfill resolves the offset parser from the
        // table type before the reader exists.
        let parser = crate::source::cdc::external::ExternalCdcTableType::Spanner
            .get_cdc_offset_parser()
            .unwrap();
        let offset = CdcOffset::Spanner(SpannerOffset::new(1234567890));
        let parsed = parser(&serde_json::to_string(&offset).unwrap()).unwrap();
        assert_eq!(parsed, offset);
    }

    #[test]
    fn test_spanner_ts_to_micros() {
        // Whole seconds.
        let ts = google_cloud_wkt::Timestamp::clamp(1_700_000_000, 0);
        assert_eq!(spanner_ts_to_micros(&ts), 1_700_000_000_000_000);
        // Sub-microsecond nanos are truncated.
        let ts = google_cloud_wkt::Timestamp::clamp(5, 123_456);
        assert_eq!(spanner_ts_to_micros(&ts), 5_000_123);
        // Epoch.
        let ts = google_cloud_wkt::Timestamp::clamp(0, 0);
        assert_eq!(spanner_ts_to_micros(&ts), 0);
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
    fn test_check_watched_columns() {
        let names = |cols: &[&str]| cols.iter().map(|c| (*c).to_owned()).collect::<Vec<_>>();
        let columns = names(&["id", "name", "Email"]);
        let pk = names(&["id"]);
        let check = |watched: Option<&WatchedColumns>, columns: &[String]| {
            check_watched_columns("s", "users", watched, columns, &pk)
        };

        check(Some(&WatchedColumns::All), &columns).unwrap();
        // Key columns are always watched and never listed; names match case-insensitively.
        let both = WatchedColumns::Only(["name".to_owned(), "email".to_owned()].into());
        check(Some(&both), &columns).unwrap();

        let err = check(None, &columns).unwrap_err();
        assert!(
            err.to_string().contains("does not watch table 'users'"),
            "{err}"
        );

        let name_only = WatchedColumns::Only(["name".to_owned()].into());
        let err = check(Some(&name_only), &columns).unwrap_err();
        assert!(err.to_string().contains(r#"["Email"]"#), "{err}");
        // A table defined with only the watched columns is fine.
        check(Some(&name_only), &names(&["id", "name"])).unwrap();
    }

    #[test]
    fn test_change_stream_filter_notices() {
        let options = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect::<HashMap<_, _>>()
        };
        assert!(
            change_stream_filter_notices("s", &options(&[("value_capture_type", "NEW_ROW")]))
                .is_empty()
        );
        assert!(
            change_stream_filter_notices("s", &options(&[("exclude_delete", "false")])).is_empty()
        );

        let notices = change_stream_filter_notices(
            "s",
            &options(&[("exclude_delete", "TRUE"), ("exclude_ttl_deletes", "true")]),
        );
        assert_eq!(notices.len(), 2, "{notices:?}");
        assert!(notices[0].contains("exclude_delete"), "{notices:?}");
        assert!(notices[1].contains("exclude_ttl_deletes"), "{notices:?}");
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
    fn test_named_schema_table() {
        assert_eq!(split_table_name("t"), ("", "t"));
        assert_eq!(split_table_name("sch.t"), ("sch", "t"));
        assert_eq!(SpannerExternalTableReader::quote_table("t"), "`t`");
        assert_eq!(
            SpannerExternalTableReader::quote_table("sch.t"),
            "`sch`.`t`"
        );
    }

    #[test]
    fn test_build_split_filter_sql() {
        let key = Field::new("k", DataType::Int64);
        assert_eq!(build_split_filter_sql(&key, true, true), "");
        assert_eq!(
            build_split_filter_sql(&key, true, false),
            "WHERE (`k` < @pk_end OR `k` IS NULL)"
        );
        assert_eq!(
            build_split_filter_sql(&key, false, false),
            "WHERE `k` >= @pk_start AND `k` < @pk_end"
        );
        assert_eq!(
            build_split_filter_sql(&key, false, true),
            "WHERE `k` >= @pk_start"
        );

        let key = Field::new("k", DataType::Float64);
        assert_eq!(
            build_split_filter_sql(&key, false, true),
            "WHERE (`k` >= @pk_start OR IS_NAN(`k`))"
        );
    }

    #[test]
    fn test_scalar_to_spanner_value() {
        use risingwave_common::types::{Date, Decimal, Timestamptz};

        let to_string = |scalar: ScalarImpl| {
            scalar_to_spanner_value(&scalar)
                .unwrap()
                .as_string()
                .to_owned()
        };
        assert_eq!(to_string(ScalarImpl::Bytea(vec![1, 2, 255].into())), "AQL/");
        assert_eq!(
            to_string(ScalarImpl::Timestamptz(
                Timestamptz::from_micros(1_704_067_200_123_456).unwrap()
            )),
            "2024-01-01T00:00:00.123456000Z"
        );
        assert_eq!(
            to_string(ScalarImpl::Date(Date::from_ymd_uncheck(2024, 2, 29))),
            "2024-02-29"
        );
        assert_eq!(
            to_string(ScalarImpl::Decimal(
                Decimal::from_str("12345678901234567.8").unwrap()
            )),
            "12345678901234567.8"
        );
        assert!(scalar_to_spanner_value(&ScalarImpl::Jsonb(serde_json::json!(1).into())).is_err());
    }
}
