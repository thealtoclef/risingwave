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
//! This module implements snapshot backfill for Spanner CDC using databoost
//! (partitioned reads) as described in:
//! https://cloud.google.com/spanner/docs/reads#read_data_in_parallel
//!
//! The implementation uses partitioned reads to scan the table in parallel,
//! splitting the data based on the primary key ranges.

use std::str::FromStr;

use anyhow::{Context, anyhow};
use base64::Engine;
use futures::stream::BoxStream;
use futures_async_stream::try_stream;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::statement::Statement;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum, F32, F64, ListType, ScalarImpl};

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CdcOffset, CdcTableSnapshotSplitOption, ExternalTableConfig, ExternalTableReader,
    SchemaTableName,
};

/// Spanner offset representing a position in the change stream.
///
/// Spanner change streams use timestamps as offsets, representing the
/// commit timestamp of the transaction.
///
/// This offset also includes partition state for proper checkpoint/restoration,
/// following the pattern used by spream where partition metadata is tracked
/// for coordinating child partition scheduling.
#[derive(Debug, Clone, Default, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct SpannerOffset {
    /// The timestamp of the change stream position (microseconds since epoch)
    pub timestamp: i64,

    /// Partition token (None for root partition)
    #[serde(default)]
    pub partition_token: Option<String>,

    /// Parent partition tokens (empty for root partition)
    #[serde(default)]
    pub parent_partition_tokens: Vec<String>,

    /// Watermark: highest timestamp processed by this partition
    pub watermark: i64,

    /// Stream name for this partition
    #[serde(default)]
    pub stream_name: String,

    /// Split index for identification
    #[serde(default)]
    pub index: u32,

    /// Whether this partition is finished
    #[serde(default)]
    pub is_finished: bool,
}

impl SpannerOffset {
    pub fn new(timestamp: i64) -> Self {
        Self {
            timestamp,
            partition_token: None,
            parent_partition_tokens: Vec::new(),
            watermark: timestamp,
            stream_name: String::new(),
            index: 0,
            is_finished: false,
        }
    }

    /// Create a SpannerOffset with full partition state for checkpointing.
    ///
    /// This follows the spream pattern where partition state is persisted
    /// to enable proper restoration after failure/restart.
    pub fn with_partition(
        watermark: i64,
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
            watermark,
            stream_name,
            index,
            is_finished: false,
        }
    }

    /// Mark this partition as finished
    pub fn mark_finished(&mut self) {
        self.is_finished = true;
    }

    /// Create a SpannerOffset from a timestamp string (microseconds since epoch)
    pub fn from_timestamp_str(ts: &str) -> ConnectorResult<Self> {
        let timestamp = ts
            .parse::<i64>()
            .context("failed to parse Spanner timestamp")?;
        Ok(Self::new(timestamp))
    }

    /// Convert to timestamp string (microseconds since epoch)
    pub fn to_timestamp_str(&self) -> String {
        self.timestamp.to_string()
    }
}

/// Spanner external table for schema discovery.
pub struct SpannerExternalTable {
    column_descs: Vec<ColumnDesc>,
    pk_names: Vec<String>,
    table_name: String,
}

impl SpannerExternalTable {
    /// Connect to Spanner and discover table schema.
    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        let table_name = config.table.clone();

        // Create Spanner client using the same pattern as CDC
        let client = create_spanner_client(&config).await?;

        // Get table schema from Spanner INFORMATION_SCHEMA
        let column_descs = Self::discover_table_schema(&client, &table_name).await?;

        // Get primary key columns
        let pk_names = Self::discover_primary_keys(&client, &table_name).await?;

        if pk_names.is_empty() {
            bail!(
                "Spanner table '{}' does not have a primary key. Backfill requires a primary key.",
                table_name
            );
        }

        Ok(Self {
            column_descs,
            pk_names,
            table_name,
        })
    }

    /// Discover table schema from Spanner INFORMATION_SCHEMA.
    async fn discover_table_schema(
        client: &Client,
        table_name: &str,
    ) -> ConnectorResult<Vec<ColumnDesc>> {
        let sql = r#"
            SELECT
                COLUMN_NAME,
                SPANNER_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ''
            AND TABLE_NAME = @p1
            ORDER BY ORDINAL_POSITION
        "#;

        let mut statement = Statement::new(sql);
        statement.add_param("p1", &table_name);

        // Execute query using snapshot transaction
        let mut txn = client
            .single()
            .await
            .context("failed to create snapshot transaction")?;

        let mut results = txn.query(statement).await.context("failed to execute schema query")?;

        let mut column_descs = Vec::new();

        while let Some(row) = results.next().await.context("failed to get next row")? {
            let column_name: String = row
                .column_by_name("COLUMN_NAME")
                .context("failed to get column name")?;

            let spanner_type: String = row
                .column_by_name("SPANNER_TYPE")
                .context("failed to get spanner type")?;

            let data_type = spanner_type_to_rw_type(&spanner_type)?;
            let column_desc = ColumnDesc::named(column_name.clone(), ColumnId::placeholder(), data_type);
            column_descs.push(column_desc);
        }

        if column_descs.is_empty() {
            bail!("table '{}' not found", table_name);
        }

        Ok(column_descs)
    }

    /// Discover primary key columns from Spanner INFORMATION_SCHEMA.
    async fn discover_primary_keys(
        client: &Client,
        table_name: &str,
    ) -> ConnectorResult<Vec<String>> {
        let sql = r#"
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.INDEX_COLUMNS
            WHERE TABLE_SCHEMA = ''
            AND TABLE_NAME = @p1
            AND INDEX_TYPE = 'PRIMARY_KEY'
            ORDER BY ORDINAL_POSITION
        "#;

        let mut statement = Statement::new(sql);
        statement.add_param("p1", &table_name);

        let mut txn = client
            .single()
            .await
            .context("failed to create snapshot transaction")?;

        let mut results = txn.query(statement).await.context("failed to execute primary key query")?;

        let mut pk_names = Vec::new();

        while let Some(row) = results.next().await.context("failed to get next row")? {
            let column_name: String = row
                .column_by_name("COLUMN_NAME")
                .context("failed to get column name")?;
            pk_names.push(column_name);
        }

        Ok(pk_names)
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

/// Spanner external table reader for snapshot backfill.
///
/// Uses databoost (partitioned reads) to scan the table in parallel.
pub struct SpannerExternalTableReader {
    rw_schema: Schema,
    field_names: Vec<String>,
    pk_names: Vec<String>,
    pk_types: Vec<DataType>,
    client: Client,
    table_name: String,
    start_timestamp: Option<i64>,
    enable_databoost: bool,
}

impl SpannerExternalTableReader {
    /// Create a new Spanner external table reader.
    pub async fn new(
        config: ExternalTableConfig,
        schema: Schema,
        enable_databoost: bool,
    ) -> ConnectorResult<Self> {
        // Create Spanner client
        let client = create_spanner_client(&config).await?;

        // Get primary key information from table schema
        let external_table = SpannerExternalTable::connect(config).await?;
        let pk_names = external_table.pk_names().clone();
        let pk_types: Vec<DataType> = pk_names
            .iter()
            .map(|pk_name| {
                external_table
                    .column_descs()
                    .iter()
                    .find(|col| col.name.as_str() == pk_name.as_str())
                    .map(|col| col.data_type.clone())
                    .ok_or_else(|| anyhow!("primary key column '{}' not found in schema", pk_name))
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Build field names from schema
        let field_names = schema
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();

        Ok(Self {
            rw_schema: schema,
            field_names,
            pk_names,
            pk_types,
            client,
            table_name: external_table.table_name().to_string(),
            start_timestamp: None,
            enable_databoost,
        })
    }

    /// Get the current timestamp from Spanner to use as the start timestamp for CDC.
    async fn get_current_timestamp(&self) -> ConnectorResult<i64> {
        let statement = Statement::new("SELECT CURRENT_TIMESTAMP AS TS");

        let mut txn = self
            .client
            .single()
            .await
            .context("failed to create snapshot transaction")?;

        let mut results = txn
            .query(statement)
            .await
            .context("failed to get current timestamp")?;

        if let Some(row) = results.next().await.context("failed to get next row")? {
            // The timestamp is returned as a timestamp type
            let ts: time::OffsetDateTime = row
                .column_by_name("TS")
                .context("failed to get timestamp")?;

            // Convert to microseconds since epoch
            let timestamp = ts.unix_timestamp_nanos() / 1000;
            Ok(timestamp as i64)
        } else {
            bail!("no rows returned from timestamp query");
        }
    }
}

impl ExternalTableReader for SpannerExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        // For Spanner, the offset is the current timestamp
        let timestamp = self.get_current_timestamp().await?;
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
        // Spanner client doesn't need explicit disconnection
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
    /// Read snapshot data from the table.
    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        _table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        _primary_keys: Vec<String>,
        limit: u32,
    ) {
        let fields = self.rw_schema.fields().clone();
        let column_names = self.field_names.clone();
        let table_name = &self.table_name;

        // Build SELECT query
        let columns_str = column_names.join(", ");
        let sql = format!("SELECT {} FROM {}", columns_str, table_name);

        // Add WHERE clause for start PK if provided
        let sql_with_filter = if let Some(ref start_pk) = start_pk {
            let pk_filter = build_pk_filter(&self.pk_names, start_pk, &self.pk_types, true)?;
            format!("{} WHERE {}", sql, pk_filter)
        } else {
            sql
        };

        // Add LIMIT
        let sql_with_limit = format!("{} LIMIT {}", sql_with_filter, limit);

        let statement = Statement::new(&sql_with_limit);

        // Create snapshot transaction
        let mut txn = self
            .client
            .single()
            .await
            .context("failed to create snapshot transaction for backfill")?;

        let mut results = txn
            .query(statement)
            .await
            .context("failed to execute snapshot read")?;

        while let Some(row) = results.next().await.context("failed to get next row")? {
            let owned_row = spanner_row_to_owned_row(&row, &fields)?;
            yield owned_row;
        }
    }

    /// Generate parallel splits for the table using databoost (partitioned reads).
    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn get_parallel_cdc_splits_inner(&self, _options: CdcTableSnapshotSplitOption) {
        if self.enable_databoost {
            // Create batch read-only transaction for partitioned queries
            let mut batch_txn = self
                .client
                .batch_read_only_transaction()
                .await
                .context("failed to create batch transaction for partitioned query")?;

            // Build SELECT query for the table
            let columns_str = self.field_names.join(", ");
            let sql = format!("SELECT {} FROM {}", columns_str, self.table_name);
            let statement = Statement::new(sql);

            // Use partition_query to get partitions for the table with databoost enabled
            let partitions = batch_txn
                .partition_query_with_option(
                    statement,
                    None, // partition_options - use default partition sizing
                    google_cloud_spanner::transaction::QueryOptions::default(),
                    true, // data_boost_enabled - enable databoost for faster reads
                    None, // directed_read_options
                )
                .await
                .context("failed to get partitions for databoost query")?;

            // Convert each partition to a CdcTableSnapshotSplit
            // Store the partition_token in the left_bound for later use
            let mut split_id = 1i64;
            for partition in partitions {
                // Each partition has a partition_token that identifies it
                let partition_token = partition.reader.request.partition_token.clone();

                // Create left_bound with the partition token encoded as base64
                let token_str = base64::engine::general_purpose::STANDARD.encode(&partition_token);
                let mut left_values: Vec<Datum> = vec
![Some(ScalarImpl::Utf8(token_str.into()))];
                // Add placeholder None values for remaining PK columns
                for _ in 1..self.pk_names.len() {
                    left_values.push(None);
                }
                let left_bound = OwnedRow::new(left_values);

                // Right bound is empty to indicate this partition's end
                let right_bound = OwnedRow::new(vec
![None; self.pk_names.len()]);

                let split = CdcTableSnapshotSplit {
                    split_id,
                    left_bound_inclusive: left_bound,
                    right_bound_exclusive: right_bound,
                };

                tracing::debug!(
                    "created split_id={} with partition_token size={} bytes",
                    split_id,
                    partition_token.len()
                );

                yield split;
                split_id += 1;
            }
        } else {
            // databoost disabled - return a single split covering the entire table

            let split_id = 1;
            let left_bound = OwnedRow::new(vec
![None; self.pk_names.len()]);
            let right_bound = OwnedRow::new(vec
![None; self.pk_names.len()]);
            let split = CdcTableSnapshotSplit {
                split_id,
                left_bound_inclusive: left_bound,
                right_bound_exclusive: right_bound,
            };

            yield split;
        }
    }

    /// Read a specific split of the snapshot data.
    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn split_snapshot_read_inner(
        &self,
        _table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        _split_columns: Vec<Field>,
    ) {
        let fields = self.rw_schema.fields().clone();
        let column_names = self.field_names.clone();

        // Check if left_bound contains a partition token (for databoost partitioned reads)
        // The partition token is stored in the first column of left_bound when databoost is enabled
        if let Some(ScalarImpl::Utf8(encoded_token)) = &left[0] {
            // This is a databoost partition - decode the token and use it to read
            let partition_token = base64::engine::general_purpose::STANDARD
                .decode(encoded_token.as_bytes())
                .context("failed to decode partition token")?;

            // Build SELECT query for the table
            let columns_str = column_names.join(", ");
            let sql = format!("SELECT {} FROM {}", columns_str, self.table_name);
            let statement = Statement::new(sql);

            // Create batch read-only transaction
            let mut batch_txn = self
                .client
                .batch_read_only_transaction()
                .await
                .context("failed to create batch transaction for partition read")?;

            // Create a new partition query to get partitions with the same SQL
            let all_partitions = batch_txn
                .partition_query_with_option(
                    statement,
                    None,
                    google_cloud_spanner::transaction::QueryOptions::default(),
                    true, // data_boost_enabled
                    None,
                )
                .await
                .context("failed to get partitions for comparison")?;

            // Find the partition that matches our token and execute it
            for partition in all_partitions {
                if partition.reader.request.partition_token == partition_token {
                    // Found matching partition - execute it
                    let mut results = batch_txn
                        .execute(partition, None)
                        .await
                        .context("failed to execute partition")?;

                    while let Some(row) = results
                        .next()
                        .await
                        .context("failed to get next row from partition")?
                    {
                        let owned_row = spanner_row_to_owned_row(&row, &fields)?;
                        yield owned_row;
                    }
                    break; // Found and executed our partition
                }
            }
        } else {
            // This is a regular (non-databoost) split - use PK range filtering
            // Build SELECT query with PK range filter
            let columns_str = column_names.join(", ");
            let sql = format!("SELECT {} FROM {}", columns_str, self.table_name);

            // Build WHERE clause for the PK range
            if !left.is_empty() && !right.is_empty() {
                let left_filter = build_pk_filter(&self.pk_names, &left, &self.pk_types, true)?;
                let right_filter = build_pk_filter(&self.pk_names, &right, &self.pk_types, false)?;
                let sql_with_filter = format!("{} WHERE {} AND {}", sql, left_filter, right_filter);
                let statement = Statement::new(&sql_with_filter);

                // Create snapshot transaction
                let mut txn = self
                    .client
                    .single()
                    .await
                    .context("failed to create snapshot transaction for split read")?;

                let mut results = txn
                    .query(statement)
                    .await
                    .context("failed to execute split snapshot read")?;

                while let Some(row) = results.next().await.context("failed to get next row")? {
                    let owned_row = spanner_row_to_owned_row(&row, &fields)?;
                    yield owned_row;
                }
            } else {
                // No range specified - read all data
                let sql = format!("SELECT {} FROM {}", column_names.join(", "), self.table_name);
                let statement = Statement::new(&sql);

                let mut txn = self
                    .client
                    .single()
                    .await
                    .context("failed to create snapshot transaction")?;

                let mut results = txn
                    .query(statement)
                    .await
                    .context("failed to execute snapshot read")?;

                while let Some(row) = results.next().await.context("failed to get next row")? {
                    let owned_row = spanner_row_to_owned_row(&row, &fields)?;
                    yield owned_row;
                }
            }
        }
    }
}

/// Create a Spanner client from the external table config.
async fn create_spanner_client(config: &ExternalTableConfig) -> ConnectorResult<Client> {
    use google_cloud_spanner::client::ClientConfig;

    tracing::info!("create_spanner_client: project={}, instance={}, database={}, emulator_host={:?}",
        config.spanner_project, config.spanner_instance, config.spanner_database, config.emulator_host);

    // Validate required fields
    if config.spanner_project.is_empty() || config.spanner_instance.is_empty() || config.spanner_database.is_empty() {
        bail!("Spanner configuration requires spanner.project, spanner.instance, and spanner.database");
    }

    // Set environment variables for authentication
    // Priority: credentials_path > credentials > ADC (none set)
    if let Some(emulator_host) = &config.emulator_host {
        unsafe { std::env::set_var("SPANNER_EMULATOR_HOST", emulator_host) };
    }
    if let Some(credentials_path) = &config.credentials_path {
        // Read credentials from file and set as JSON
        let credentials_content = std::fs::read_to_string(credentials_path)
            .with_context(|| format!("failed to read credentials file: {}", credentials_path))?;
        unsafe { std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS_JSON", credentials_content) };
    } else if let Some(credentials) = &config.credentials {
        unsafe { std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS_JSON", credentials) };
    }
    // If neither credentials nor credentials_path is set, ADC will be used automatically

    // Create client configuration using the existing CDC pattern
    let client_config = ClientConfig::default()
        .with_auth()
        .await
        .context("failed to create Spanner client config")?;

    // Construct DSN from separate properties
    let dsn = format!(
        "projects/{}/instances/{}/databases/{}",
        config.spanner_project, config.spanner_instance, config.spanner_database
    );

    let client = Client::new(&dsn, client_config)
        .await
        .context("failed to create Spanner client")?;

    Ok(client)
}

/// Convert Spanner type to RisingWave data type.
///
/// Reference: https://docs.cloud.google.com/spanner/docs/reference/standard-sql/data-types
///
/// Supported Spanner types:
/// - BOOL (alias BOOLEAN)
/// - INT64
/// - NUMERIC
/// - FLOAT32
/// - FLOAT64
/// - STRING
/// - BYTES
/// - TIMESTAMP
/// - DATE
/// - JSON
/// - ARRAY<T>
/// - STRUCT<T>
/// - ENUM
/// - PROTO
/// - INTERVAL
pub(crate) fn spanner_type_to_rw_type(spanner_type: &str) -> ConnectorResult<DataType> {
    // Handle array types: ARRAY<T>
    if let Some(rest) = spanner_type.strip_prefix("ARRAY<") {
        let inner_type = rest
            .strip_suffix('>')
            .ok_or_else(|| anyhow!("invalid ARRAY type: {}", spanner_type))?;
        let inner = spanner_type_to_rw_type(inner_type)?;
        return Ok(DataType::List(ListType::new(inner)));
    }

    // Normalize the type - handle STRING(MAX) as STRING, BOOL as BOOL
    let normalized_type = spanner_type.split('(').next().unwrap_or(spanner_type);

    let dtype = match normalized_type {
        // Boolean type
        "BOOL" | "BOOLEAN" => DataType::Boolean,

        // Integer types (only INT64 exists in Spanner)
        "INT64" => DataType::Int64,

        // Floating point types
        "FLOAT64" => DataType::Float64,
        "FLOAT32" => DataType::Float32,

        // String type
        "STRING" => DataType::Varchar,

        // Binary data type
        "BYTES" => DataType::Bytea,

        // Timestamp type (absolute point in time, UTC)
        "TIMESTAMP" => DataType::Timestamptz,

        // Date type
        "DATE" => DataType::Date,

        // Decimal type (precision 38, scale 9)
        "NUMERIC" => DataType::Decimal,

        // JSON type
        "JSON" => DataType::Jsonb,

        // Struct type (not supported as column type, but can appear in change streams)
        "STRUCT" => {
            bail!(
                "STRUCT type is not supported as a column type in RisingWave. Spanner type: {}",
                spanner_type
            );
        }

        // Enum type (requires protocol buffer definition)
        "ENUM" => {
            bail!(
                "ENUM type requires protocol buffer definition. Spanner type: {}",
                spanner_type
            );
        }

        // Protocol buffer type
        "PROTO" => {
            bail!(
                "PROTO type requires protocol buffer definition. Spanner type: {}",
                spanner_type
            );
        }

        // Interval type
        "INTERVAL" => {
            bail!(
                "INTERVAL type is not supported as a column type in RisingWave. Spanner type: {}",
                spanner_type
            );
        }

        _ => {
            bail!("unsupported Spanner type: {}", spanner_type);
        }
    };

    Ok(dtype)
}

/// Build a PK filter condition for WHERE clause.
fn build_pk_filter(
    pk_names: &[String],
    pk_row: &OwnedRow,
    pk_types: &[DataType],
    is_lower_bound: bool,
) -> ConnectorResult<String> {
    let mut conditions = Vec::new();
    for (i, pk_name) in pk_names.iter().enumerate() {
        let datum = pk_row[i].clone(); // Clone to move into the function
        let value = datum_to_sql_value(datum, &pk_types[i])?;

        if is_lower_bound {
            conditions.push(format!("{} >= {}", pk_name, value));
        } else {
            conditions.push(format!("{} < {}", pk_name, value));
        }
    }

    Ok(conditions.join(" AND "))
}

/// Convert a datum to SQL value string.
fn datum_to_sql_value(datum: Datum, data_type: &DataType) -> ConnectorResult<String> {
    let Some(scalar) = datum else {
        return Ok("NULL".to_string());
    };

    let value = match (scalar, data_type) {
        (ScalarImpl::Int64(v), _) => v.to_string(),
        (ScalarImpl::Int32(v), _) => v.to_string(),
        (ScalarImpl::Int16(v), _) => v.to_string(),
        (ScalarImpl::Float64(v), _) => v.to_string(),
        (ScalarImpl::Float32(v), _) => v.to_string(),
        (ScalarImpl::Bool(v), _) => v.to_string(),
        (ScalarImpl::Utf8(v), _) => format!("'{}", v.replace('\'', "''")),
        (ScalarImpl::Bytea(v), _) => format!("CASTFROMBYTES('{}')", base64::engine::general_purpose::STANDARD.encode(v.as_ref())),
        (ScalarImpl::Jsonb(v), _) => {
            // Convert JSONB to string for SQL - use the Display impl
            let json_str = v.to_string();
            format!("'{}'", json_str.replace('\'', "''"))
        }
        (ScalarImpl::Timestamptz(v), _) => {
            // Convert timestamp to string for SQL
            // Use the Display impl which formats in ISO 8601 format
            let ts_str = v.to_string();
            format!("'{}'", ts_str)
        }
        (ScalarImpl::Date(v), _) => {
            // Format date as string for SQL - use the Display impl
            format!("'{}'", v)
        }
        _ => {
            bail!("unsupported data type for SQL value: {:?}", data_type);
        }
    };

    Ok(value)
}

/// Convert a Spanner row to an OwnedRow.
fn spanner_row_to_owned_row(
    row: &google_cloud_spanner::row::Row,
    fields: &[Field],
) -> ConnectorResult<OwnedRow> {
    let mut values = Vec::with_capacity(fields.len());

    for field in fields.iter() {
        let datum = match field.data_type {
            DataType::Boolean => {
                let v: bool = row
                    .column_by_name(&field.name)
                    .context("failed to get bool")?;
                Some(ScalarImpl::Bool(v))
            }
            DataType::Int64 => {
                let v: i64 = row
                    .column_by_name(&field.name)
                    .context("failed to get i64")?;
                Some(ScalarImpl::Int64(v))
            }
            DataType::Int32 => {
                let v: i64 = row
                    .column_by_name(&field.name)
                    .context("failed to get i32")?;
                Some(ScalarImpl::Int32(v as i32))
            }
            DataType::Int16 => {
                let v: i64 = row
                    .column_by_name(&field.name)
                    .context("failed to get i16")?;
                Some(ScalarImpl::Int16(v as i16))
            }
            DataType::Float64 => {
                let v: f64 = row
                    .column_by_name(&field.name)
                    .context("failed to get f64")?;
                Some(ScalarImpl::Float64(F64::from(v)))
            }
            DataType::Float32 => {
                let v: f64 = row
                    .column_by_name(&field.name)
                    .context("failed to get f32")?;
                Some(ScalarImpl::Float32(F32::from(v as f32)))
            }
            DataType::Varchar => {
                let v: String = row
                    .column_by_name(&field.name)
                    .context("failed to get string")?;
                Some(ScalarImpl::Utf8(v.into()))
            }
            DataType::Bytea => {
                let v: Vec<u8> = row
                    .column_by_name(&field.name)
                    .context("failed to get bytes")?;
                Some(ScalarImpl::Bytea(v.into()))
            }
            DataType::Jsonb => {
                let v: String = row
                    .column_by_name(&field.name)
                    .context("failed to get json")?;
                // The JSON is already stored as a JSON string in Spanner
                // Parse it to get JsonbVal
                let json_val: serde_json::Value = serde_json::from_str(&v)
                    .context("failed to parse JSON")?;
                Some(ScalarImpl::Jsonb(json_val.into()))
            }
            DataType::Timestamptz => {
                let v: time::OffsetDateTime = row
                    .column_by_name(&field.name)
                    .context("failed to get timestamp")?;
                // Convert to risingwave_common::types::Timestamptz
                // Use microseconds since epoch
                let micros = (v.unix_timestamp_nanos() / 1000) as i64;
                use risingwave_common::types::Timestamptz;
                Some(ScalarImpl::Timestamptz(Timestamptz::from_micros(micros)))
            }
            DataType::Date => {
                let v: time::Date = row
                    .column_by_name(&field.name)
                    .context("failed to get date")?;
                // Convert time::Date to risingwave_common::types::Date
                // Use the Display format to create a string and parse it back
                use risingwave_common::types::Date;
                let date_str = format!("{:04}-{:02}-{:02}", v.year(), v.month(), v.day());
                let rw_date = Date::from_str(&date_str)
                    .context("failed to convert date")?;
                Some(ScalarImpl::Date(rw_date))
            }
            _ => {
                bail!(
                    "unsupported data type for column '{}': {:?}",
                    field.name,
                    field.data_type
                );
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
        // Test basic types
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

        // Test array type
        let array_result = spanner_type_to_rw_type("ARRAY<INT64>");
        assert!(matches!(
            array_result.unwrap(),
            DataType::List(box => DataType::Int64)
        ));
    }
}
