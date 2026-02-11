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

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use base64::Engine;
use futures::stream::BoxStream;
use futures_async_stream::try_stream;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::transaction_ro::{BatchReadOnlyTransaction, Partition};

use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, F32, F64, ListType, ScalarImpl};
use tokio::sync::Mutex;

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
/// Created once per CREATE TABLE FROM SOURCE. Calls `batch_read_only_transaction()`
/// to obtain a single consistent snapshot. The transaction's read timestamp (`.rts`)
/// is returned by [`current_cdc_offset()`] so the [`CdcBackfillExecutor`] can filter
/// out CDC events that predate the snapshot -- the same pattern as Postgres CDC
/// (`pg_current_wal_lsn()`) and MySQL CDC (`SHOW MASTER STATUS`).
///
/// All reads (regular and partitioned) go through the same transaction to guarantee
/// they observe the same snapshot.
pub struct SpannerExternalTableReader {
    rw_schema: Schema,
    field_names: Vec<String>,
    pk_names: Vec<String>,
    pk_types: Vec<DataType>,
    table_name: String,
    enable_databoost: bool,
    /// Snapshot timestamp in microseconds since epoch, from the transaction's `.rts`.
    snapshot_timestamp: i64,
    /// Single `BatchReadOnlyTransaction` used for all reads.
    /// Spanner requires the same transaction for `partition_query` and `execute`.
    txn: Arc<Mutex<BatchReadOnlyTransaction>>,
    /// Partitions produced by [`get_parallel_cdc_splits_inner`], consumed by
    /// [`split_snapshot_read_inner`].
    partitions: Arc<Mutex<HashMap<String, Partition<google_cloud_spanner::reader::StatementReader>>>>,
}

impl SpannerExternalTableReader {
    pub async fn new(
        config: ExternalTableConfig,
        schema: Schema,
    ) -> ConnectorResult<Self> {
        let enable_databoost = config.enable_databoost;
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

        // One transaction for the entire backfill. Its read timestamp (.rts) becomes
        // the snapshot offset that coordinates with the CDC phase.
        let txn = client
            .batch_read_only_transaction()
            .await
            .context("failed to create batch read-only transaction")?;
        let snapshot_timestamp = (txn
            .rts
            .context("read timestamp not available")?
            .unix_timestamp_nanos()
            / 1000) as i64;

        tracing::info!(snapshot_timestamp_us = snapshot_timestamp, "snapshot created");

        Ok(Self {
            rw_schema: schema,
            field_names,
            pk_names,
            pk_types,
            table_name: external_table.table_name().to_string(),
            enable_databoost,
            snapshot_timestamp,
            txn: Arc::new(Mutex::new(txn)),
            partitions: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}

impl ExternalTableReader for SpannerExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        Ok(CdcOffset::Spanner(SpannerOffset::new(self.snapshot_timestamp)))
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

        let mut txn = self.txn.lock().await;
        let mut rows = txn
            .query(Statement::new(&sql))
            .await
            .context("snapshot query failed")?;

        while let Some(row) = rows.next().await.context("row read failed")? {
            yield spanner_row_to_owned_row(&row, &fields)?;
        }
    }

    /// Generate parallel splits via Spanner partitioned reads.
    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn get_parallel_cdc_splits_inner(&self, _options: CdcTableSnapshotSplitOption) {
        let mut txn = self.txn.lock().await;

        let columns_str = self.field_names.join(", ");
        let sql = format!("SELECT {} FROM {}", columns_str, self.table_name);

        let partitions = txn
            .partition_query_with_option(
                Statement::new(sql),
                None,
                google_cloud_spanner::transaction::QueryOptions::default(),
                self.enable_databoost,
                None,
            )
            .await
            .context("failed to get partitions")?;

        let mut store = self.partitions.lock().await;
        let mut split_id = 1i64;

        for partition in partitions {
            let token_bytes = partition.reader.request.partition_token.clone();
            let token_str = base64::engine::general_purpose::STANDARD.encode(&token_bytes);
            store.insert(token_str.clone(), partition);

            let mut left_values: Vec<Datum> = vec![Some(ScalarImpl::Utf8(token_str.into()))];
            for _ in 1..self.pk_names.len() {
                left_values.push(None);
            }

            yield CdcTableSnapshotSplit {
                split_id,
                left_bound_inclusive: OwnedRow::new(left_values),
                right_bound_exclusive: OwnedRow::new(vec![None; self.pk_names.len()]),
            };
            split_id += 1;
        }
    }

    /// Read one partition created by [`get_parallel_cdc_splits_inner`].
    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn split_snapshot_read_inner(
        &self,
        _table_name: SchemaTableName,
        left: OwnedRow,
        _right: OwnedRow,
        _split_columns: Vec<Field>,
    ) {
        let fields = self.rw_schema.fields();

        let Some(ScalarImpl::Utf8(encoded_token)) = &left[0] else {
            bail!("expected partition token in left_bound[0], got {:?}", left[0]);
        };

        let partition = {
            let mut store = self.partitions.lock().await;
            store
                .remove(&**encoded_token)
                .context(format!("partition token '{}' not found", encoded_token))?
        };

        let mut txn = self.txn.lock().await;
        let mut rows = txn
            .execute(partition, None)
            .await
            .context("failed to execute partition")?;

        while let Some(row) = rows.next().await.context("partition row read failed")? {
            yield spanner_row_to_owned_row(&row, &fields)?;
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
pub(crate) fn spanner_type_to_rw_type(spanner_type: &str) -> ConnectorResult<DataType> {
    if let Some(rest) = spanner_type.strip_prefix("ARRAY<") {
        let inner = rest
            .strip_suffix('>')
            .ok_or_else(|| anyhow!("invalid ARRAY type: {}", spanner_type))?;
        return Ok(DataType::List(ListType::new(spanner_type_to_rw_type(inner)?)));
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
        _ => bail!("unsupported Spanner type: {}", spanner_type),
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
        let datum = match f.data_type {
            DataType::Boolean => {
                let v: bool = row.column_by_name(&f.name).context("bool")?;
                Some(ScalarImpl::Bool(v))
            }
            DataType::Int64 => {
                let v: i64 = row.column_by_name(&f.name).context("i64")?;
                Some(ScalarImpl::Int64(v))
            }
            DataType::Int32 => {
                let v: i64 = row.column_by_name(&f.name).context("i32")?;
                Some(ScalarImpl::Int32(v as i32))
            }
            DataType::Int16 => {
                let v: i64 = row.column_by_name(&f.name).context("i16")?;
                Some(ScalarImpl::Int16(v as i16))
            }
            DataType::Float64 => {
                let v: f64 = row.column_by_name(&f.name).context("f64")?;
                Some(ScalarImpl::Float64(F64::from(v)))
            }
            DataType::Float32 => {
                let v: f64 = row.column_by_name(&f.name).context("f32")?;
                Some(ScalarImpl::Float32(F32::from(v as f32)))
            }
            DataType::Varchar => {
                let v: String = row.column_by_name(&f.name).context("string")?;
                Some(ScalarImpl::Utf8(v.into()))
            }
            DataType::Bytea => {
                let v: Vec<u8> = row.column_by_name(&f.name).context("bytes")?;
                Some(ScalarImpl::Bytea(v.into()))
            }
            DataType::Jsonb => {
                let v: String = row.column_by_name(&f.name).context("json")?;
                let json: serde_json::Value =
                    serde_json::from_str(&v).context("invalid json")?;
                Some(ScalarImpl::Jsonb(json.into()))
            }
            DataType::Timestamptz => {
                let v: time::OffsetDateTime =
                    row.column_by_name(&f.name).context("timestamptz")?;
                let micros = (v.unix_timestamp_nanos() / 1000) as i64;
                use risingwave_common::types::Timestamptz;
                Some(ScalarImpl::Timestamptz(Timestamptz::from_micros(micros)))
            }
            DataType::Date => {
                let v: time::Date = row.column_by_name(&f.name).context("date")?;
                use risingwave_common::types::Date;
                let s = format!("{:04}-{:02}-{:02}", v.year(), v.month(), v.day());
                let d = Date::from_str(&s).context("date conversion")?;
                Some(ScalarImpl::Date(d))
            }
            _ => bail!("unsupported type for column '{}': {:?}", f.name, f.data_type),
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
