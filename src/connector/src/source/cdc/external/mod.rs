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

pub mod mock_external_table;
pub mod postgres;
pub mod sql_server;

pub mod mysql;
pub mod spanner;

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, anyhow};
use futures::pin_mut;
use futures::stream::BoxStream;
use futures_async_stream::try_stream;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::secret::LocalSecretManager;
use risingwave_pb::catalog::table::CdcTableType as PbCdcTableType;
use risingwave_pb::secret::PbSecretRef;
use serde::{Deserialize, Serialize};

use crate::WithPropertiesExt;
use crate::connector_common::{PostgresExternalTable, SslMode};
use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::mysql_row_to_owned_row;
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::CdcSourceType;
use crate::source::cdc::external::mock_external_table::MockExternalTableReader;
use crate::source::cdc::external::mysql::{
    MySqlExternalTable, MySqlExternalTableReader, MySqlOffset,
};
use crate::source::cdc::external::postgres::{PostgresExternalTableReader, PostgresOffset};
use crate::source::cdc::external::spanner::{
    SpannerExternalTable, SpannerExternalTableReader, SpannerOffset,
};
use crate::source::cdc::external::sql_server::{
    SqlServerExternalTable, SqlServerExternalTableReader, SqlServerOffset,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExternalCdcTableType {
    Undefined,
    Mock,
    MySql,
    Postgres,
    SqlServer,
    Citus,
    Mongo,
    Spanner,
}

impl ExternalCdcTableType {
    pub fn from_properties(with_properties: &impl WithPropertiesExt) -> Self {
        let connector = with_properties.get_connector().unwrap_or_default();
        Self::from_connector(&connector)
    }

    pub fn from_connector(connector: &str) -> Self {
        match connector {
            "mysql-cdc" => Self::MySql,
            "postgres-cdc" => Self::Postgres,
            "citus-cdc" => Self::Citus,
            "sqlserver-cdc" => Self::SqlServer,
            "mongodb-cdc" => Self::Mongo,
            "spanner-cdc" => Self::Spanner,
            _ => Self::Undefined,
        }
    }

    pub fn can_backfill(&self) -> bool {
        matches!(self, Self::MySql | Self::Postgres | Self::SqlServer | Self::Spanner)
    }

    pub fn enable_transaction_metadata(&self) -> bool {
        // In Debezium, transactional metadata cause delay of the newest events, as the `END` message is never sent unless a new transaction starts.
        // So we only allow transactional metadata for MySQL and Postgres.
        // See more in https://debezium.io/documentation/reference/2.6/connectors/sqlserver.html#sqlserver-transaction-metadata
        matches!(self, Self::MySql | Self::Postgres)
    }

    pub fn shareable_only(&self) -> bool {
        matches!(self, Self::SqlServer | Self::Spanner)
    }

    pub async fn create_table_reader(
        &self,
        config: ExternalTableConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        schema_table_name: SchemaTableName,
    ) -> ConnectorResult<ExternalTableReaderImpl> {
        match self {
            Self::MySql => Ok(ExternalTableReaderImpl::MySql(
                MySqlExternalTableReader::new(config, schema).await?,
            )),
            Self::Postgres => Ok(ExternalTableReaderImpl::Postgres(
                PostgresExternalTableReader::new(config, schema, pk_indices, schema_table_name)
                    .await?,
            )),
            Self::SqlServer => Ok(ExternalTableReaderImpl::SqlServer(
                SqlServerExternalTableReader::new(config, schema, pk_indices).await?,
            )),
            Self::Spanner => Ok(ExternalTableReaderImpl::Spanner(
                SpannerExternalTableReader::new(config, schema).await?,
            )),
            // citus is never supported for cdc backfill (create source + create table).
            Self::Mock => Ok(ExternalTableReaderImpl::Mock(MockExternalTableReader::new())),
            _ => bail!("invalid external table type: {:?}", *self),
        }
    }
}

impl From<ExternalCdcTableType> for PbCdcTableType {
    fn from(cdc_table_type: ExternalCdcTableType) -> Self {
        match cdc_table_type {
            ExternalCdcTableType::Postgres => Self::Postgres,
            ExternalCdcTableType::MySql => Self::Mysql,
            ExternalCdcTableType::SqlServer => Self::Sqlserver,

            ExternalCdcTableType::Citus => Self::Citus,
            ExternalCdcTableType::Mongo => Self::Mongo,
            ExternalCdcTableType::Spanner => Self::Unspecified, // Spanner CDC doesn't use Debezium format
            ExternalCdcTableType::Undefined | ExternalCdcTableType::Mock => Self::Unspecified,
        }
    }
}

impl From<PbCdcTableType> for ExternalCdcTableType {
    fn from(cdc_table_type: PbCdcTableType) -> Self {
        match cdc_table_type {
            PbCdcTableType::Postgres => Self::Postgres,
            PbCdcTableType::Mysql => Self::MySql,
            PbCdcTableType::Sqlserver => Self::SqlServer,
            PbCdcTableType::Mongo => Self::Mongo,
            PbCdcTableType::Citus => Self::Citus,
            PbCdcTableType::Unspecified => Self::Undefined,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaTableName {
    // namespace of the table, e.g. database in mysql, schema in postgres
    pub schema_name: String,
    pub table_name: String,
}

pub const TABLE_NAME_KEY: &str = "table.name";
pub const SCHEMA_NAME_KEY: &str = "schema.name";
pub const DATABASE_NAME_KEY: &str = "database.name";

impl SchemaTableName {
    pub fn from_properties(properties: &BTreeMap<String, String>) -> Self {
        let table_type = ExternalCdcTableType::from_properties(properties);
        let table_name = properties.get(TABLE_NAME_KEY).cloned().unwrap_or_default();

        let schema_name = match table_type {
            ExternalCdcTableType::MySql => properties
                .get(DATABASE_NAME_KEY)
                .cloned()
                .unwrap_or_default(),
            ExternalCdcTableType::Postgres | ExternalCdcTableType::Citus => {
                properties.get(SCHEMA_NAME_KEY).cloned().unwrap_or_default()
            }
            ExternalCdcTableType::SqlServer => {
                properties.get(SCHEMA_NAME_KEY).cloned().unwrap_or_default()
            }
            ExternalCdcTableType::Spanner => {
                // Spanner doesn't use schema names
                String::new()
            }
            _ => {
                unreachable!("invalid external table type: {:?}", table_type);
            }
        };

        Self {
            schema_name,
            table_name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum CdcOffset {
    MySql(MySqlOffset),
    Postgres(PostgresOffset),
    SqlServer(SqlServerOffset),
    Spanner(SpannerOffset),
}

// Example debezium offset for Postgres:
// {
//     "sourcePartition":
//     {
//         "server": "RW_CDC_1004"
//     },
//     "sourceOffset":
//     {
//         "last_snapshot_record": false,
//         "lsn": 29973552,
//         "txId": 1046,
//         "ts_usec": 1670826189008456,
//         "snapshot": true
//     }
// }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebeziumOffset {
    #[serde(rename = "sourcePartition")]
    pub source_partition: HashMap<String, String>,
    #[serde(rename = "sourceOffset")]
    pub source_offset: DebeziumSourceOffset,
    #[serde(rename = "isHeartbeat")]
    pub is_heartbeat: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DebeziumSourceOffset {
    // postgres snapshot progress
    pub last_snapshot_record: Option<bool>,
    // mysql snapshot progress
    pub snapshot: Option<bool>,

    // mysql binlog offset
    pub file: Option<String>,
    pub pos: Option<u64>,

    // postgres offset
    pub lsn: Option<u64>,
    #[serde(rename = "txId")]
    pub txid: Option<i64>,
    pub tx_usec: Option<u64>,
    pub lsn_commit: Option<u64>,
    pub lsn_proc: Option<u64>,

    // sql server offset
    pub commit_lsn: Option<String>,
    pub change_lsn: Option<String>,
}

pub type CdcOffsetParseFunc = Box<dyn Fn(&str) -> ConnectorResult<CdcOffset> + Send>;

pub trait ExternalTableReader: Sized {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset>;

    // Currently, MySQL cdc uses a connection pool to manage connections to MySQL, and other CDC processes do not require the disconnect step for now.
    #[allow(clippy::unused_async)]
    async fn disconnect(self) -> ConnectorResult<()> {
        Ok(())
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>>;

    fn get_parallel_cdc_splits(
        &self,
        options: CdcTableSnapshotSplitOption,
    ) -> BoxStream<'_, ConnectorResult<CdcTableSnapshotSplit>>;

    fn split_snapshot_read(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>>;
}

pub struct CdcTableSnapshotSplitOption {
    pub backfill_num_rows_per_split: u64,
    pub backfill_as_even_splits: bool,
    pub backfill_split_pk_column_index: u32,
}

pub enum ExternalTableReaderImpl {
    MySql(MySqlExternalTableReader),
    Postgres(PostgresExternalTableReader),
    SqlServer(SqlServerExternalTableReader),
    Spanner(SpannerExternalTableReader),
    Mock(MockExternalTableReader),
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
pub struct ExternalTableConfig {
    pub connector: String,

    #[serde(rename = "hostname")]
    pub host: String,
    pub port: String,
    pub username: String,
    pub password: String,
    #[serde(rename = "database.name")]
    pub database: String,
    #[serde(rename = "schema.name")]
    pub schema: String,
    #[serde(rename = "table.name")]
    pub table: String,
    /// `ssl.mode` specifies the SSL/TLS encryption level for secure communication with Postgres.
    /// Choices include `disabled`, `preferred`, and `required`.
    /// This field is optional.
    #[serde(rename = "ssl.mode", default = "postgres_ssl_mode_default")]
    #[serde(alias = "debezium.database.sslmode")]
    pub ssl_mode: SslMode,

    #[serde(rename = "ssl.root.cert")]
    #[serde(alias = "debezium.database.sslrootcert")]
    pub ssl_root_cert: Option<String>,

    /// `encrypt` specifies whether connect to SQL Server using SSL.
    /// Only "true" means using SSL. All other values are treated as "false".
    #[serde(rename = "database.encrypt")]
    pub encrypt: String,

    /// Spanner Google Cloud project ID
    #[serde(rename = "spanner.project")]
    pub spanner_project: String,

    /// Spanner instance ID
    #[serde(rename = "spanner.instance")]
    pub spanner_instance: String,

    /// Spanner emulator host for testing
    #[serde(rename = "spanner.emulator_host")]
    pub emulator_host: Option<String>,

    /// GCP credentials JSON string
    #[serde(rename = "spanner.credentials")]
    pub credentials: Option<String>,

    /// Path to GCP service account credentials file
    /// Alternative to `spanner.credentials` for file-based credentials
    #[serde(rename = "spanner.credentials_path")]
    pub credentials_path: Option<String>,

    /// Enable databoost for parallel reads
    #[serde(rename = "spanner.enable_databoost")]
    #[serde(deserialize_with = "crate::deserialize_bool_from_string")]
    pub enable_databoost: bool,
}

fn postgres_ssl_mode_default() -> SslMode {
    // NOTE(StrikeW): Default to `disabled` for backward compatibility
    SslMode::Disabled
}

impl ExternalTableConfig {
    pub fn try_from_btreemap(
        connect_properties: BTreeMap<String, String>,
        secret_refs: BTreeMap<String, PbSecretRef>,
    ) -> ConnectorResult<Self> {
        let options_with_secret =
            LocalSecretManager::global().fill_secrets(connect_properties, secret_refs)?;
        let json_value = serde_json::to_value(options_with_secret)?;
        let config = serde_json::from_value::<ExternalTableConfig>(json_value)?;
        Ok(config)
    }
}

impl ExternalTableReader for ExternalTableReaderImpl {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        match self {
            ExternalTableReaderImpl::MySql(mysql) => mysql.current_cdc_offset().await,
            ExternalTableReaderImpl::Postgres(postgres) => postgres.current_cdc_offset().await,
            ExternalTableReaderImpl::SqlServer(sql_server) => sql_server.current_cdc_offset().await,
            ExternalTableReaderImpl::Spanner(spanner) => spanner.current_cdc_offset().await,
            ExternalTableReaderImpl::Mock(mock) => mock.current_cdc_offset().await,
        }
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

impl ExternalTableReaderImpl {
    pub fn get_cdc_offset_parser(&self) -> CdcOffsetParseFunc {
        match self {
            ExternalTableReaderImpl::MySql(_) => MySqlExternalTableReader::get_cdc_offset_parser(),
            ExternalTableReaderImpl::Postgres(_) => {
                PostgresExternalTableReader::get_cdc_offset_parser()
            }
            ExternalTableReaderImpl::SqlServer(_) => {
                SqlServerExternalTableReader::get_cdc_offset_parser()
            }
            ExternalTableReaderImpl::Spanner(_) => {
                // Spanner uses timestamp-based offsets
                Box::new(|offset_str: &str| {
                    let timestamp = offset_str.parse::<i64>()
                        .context("failed to parse Spanner timestamp")?;
                    Ok(CdcOffset::Spanner(SpannerOffset::new(timestamp)))
                })
            }
            ExternalTableReaderImpl::Mock(_) => MockExternalTableReader::get_cdc_offset_parser(),
        }
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) {
        let stream = match self {
            ExternalTableReaderImpl::MySql(mysql) => {
                mysql.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
            ExternalTableReaderImpl::Postgres(postgres) => {
                postgres.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
            ExternalTableReaderImpl::SqlServer(sql_server) => {
                sql_server.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
            ExternalTableReaderImpl::Spanner(spanner) => {
                spanner.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
            ExternalTableReaderImpl::Mock(mock) => {
                mock.snapshot_read(table_name, start_pk, primary_keys, limit)
            }
        };

        pin_mut!(stream);
        #[for_await]
        for row in stream {
            let row = row?;
            yield row;
        }
    }

    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn get_parallel_cdc_splits_inner(&self, options: CdcTableSnapshotSplitOption) {
        let stream = match self {
            ExternalTableReaderImpl::MySql(e) => e.get_parallel_cdc_splits(options),
            ExternalTableReaderImpl::Postgres(e) => e.get_parallel_cdc_splits(options),
            ExternalTableReaderImpl::SqlServer(e) => e.get_parallel_cdc_splits(options),
            ExternalTableReaderImpl::Spanner(e) => e.get_parallel_cdc_splits(options),
            ExternalTableReaderImpl::Mock(e) => e.get_parallel_cdc_splits(options),
        };
        pin_mut!(stream);
        #[for_await]
        for row in stream {
            let row = row?;
            yield row;
        }
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn split_snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) {
        let stream = match self {
            ExternalTableReaderImpl::MySql(mysql) => {
                mysql.split_snapshot_read(table_name, left, right, split_columns)
            }
            ExternalTableReaderImpl::Postgres(postgres) => {
                postgres.split_snapshot_read(table_name, left, right, split_columns)
            }
            ExternalTableReaderImpl::SqlServer(sql_server) => {
                sql_server.split_snapshot_read(table_name, left, right, split_columns)
            }
            ExternalTableReaderImpl::Spanner(spanner) => {
                spanner.split_snapshot_read(table_name, left, right, split_columns)
            }
            ExternalTableReaderImpl::Mock(mock) => {
                mock.split_snapshot_read(table_name, left, right, split_columns)
            }
        };

        pin_mut!(stream);
        #[for_await]
        for row in stream {
            let row = row?;
            yield row;
        }
    }
}

pub enum ExternalTableImpl {
    MySql(MySqlExternalTable),
    Postgres(PostgresExternalTable),
    SqlServer(SqlServerExternalTable),
    Spanner(SpannerExternalTable),
}

impl ExternalTableImpl {
    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        // Spanner CDC is not a Debezium-based connector, check it first.
        let table_type = ExternalCdcTableType::from_connector(&config.connector);
        if matches!(table_type, ExternalCdcTableType::Spanner) {
            return Ok(ExternalTableImpl::Spanner(
                SpannerExternalTable::connect(config).await?,
            ));
        }

        // For Debezium-based CDC connectors, use the standard CdcSourceType dispatch.
        let cdc_source_type = CdcSourceType::from(config.connector.as_str());
        match cdc_source_type {
            CdcSourceType::Mysql => Ok(ExternalTableImpl::MySql(
                MySqlExternalTable::connect(config).await?,
            )),
            CdcSourceType::Postgres => Ok(ExternalTableImpl::Postgres(
                PostgresExternalTable::connect(
                    &config.username,
                    &config.password,
                    &config.host,
                    config.port.parse::<u16>().unwrap(),
                    &config.database,
                    &config.schema,
                    &config.table,
                    &config.ssl_mode,
                    &config.ssl_root_cert,
                    false,
                )
                .await?,
            )),
            CdcSourceType::SqlServer => Ok(ExternalTableImpl::SqlServer(
                SqlServerExternalTable::connect(config).await?,
            )),
            _ => Err(anyhow!("Unsupported cdc connector type: {}", config.connector).into()),
        }
    }

    pub fn column_descs(&self) -> &Vec<ColumnDesc> {
        match self {
            ExternalTableImpl::MySql(mysql) => mysql.column_descs(),
            ExternalTableImpl::Postgres(postgres) => postgres.column_descs(),
            ExternalTableImpl::SqlServer(sql_server) => sql_server.column_descs(),
            ExternalTableImpl::Spanner(spanner) => spanner.column_descs(),
        }
    }

    pub fn pk_names(&self) -> &Vec<String> {
        match self {
            ExternalTableImpl::MySql(mysql) => mysql.pk_names(),
            ExternalTableImpl::Postgres(postgres) => postgres.pk_names(),
            ExternalTableImpl::SqlServer(sql_server) => sql_server.pk_names(),
            ExternalTableImpl::Spanner(spanner) => spanner.pk_names(),
        }
    }
}

pub const CDC_TABLE_SPLIT_ID_START: i64 = 1;
