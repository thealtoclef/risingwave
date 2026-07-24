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

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose;
use bytes::{BufMut, Bytes, BytesMut};
use mysql_async::Opts;
use mysql_async::prelude::Queryable;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use url::{Url, form_urlencoded};
use with_options::WithOptions;

use super::doris_starrocks_connector::{
    DORIS_DELETE_SIGN, DORIS_SUCCESS_STATUS, HeaderBuilder, InserterInner, InserterInnerBuilder,
    POOL_IDLE_TIMEOUT,
};
use super::{
    Result, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinkError, SinkWriterMetrics,
};
use crate::enforce_secret::EnforceSecret;
use crate::sink::encoder::{DorisJsonConfig, JsonEncoder, RowEncoder};
use crate::sink::writer::{LogSinkerOf, SinkWriter, SinkWriterExt};
use crate::sink::{Sink, SinkParam, SinkWriterParam};

pub const DORIS_SINK: &str = "doris";

// Connection parameters for the MySQL-protocol query port of Doris FE, only used for DDL
// (e.g. auto-create). Mirrors the values used by the StarRocks sink.
const DORIS_MYSQL_PREFER_SOCKET: &str = "false";
// Unlike the StarRocks schema client (which only issues short `SELECT`s), this client sends
// `CREATE TABLE` DDL, which can exceed the StarRocks default of 1024 bytes for wide tables.
// `mysql_async` enforces this as a client-side cap on the outbound packet, so a larger value is
// needed here or wide-table auto-create fails with `PacketTooLarge`.
const DORIS_MYSQL_MAX_ALLOWED_PACKET: usize = 1024 * 1024;
const DORIS_MYSQL_WAIT_TIMEOUT: usize = 28800;

const fn default_stream_load_http_timeout_ms() -> u64 {
    30 * 1000
}

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct DorisCommon {
    #[serde(rename = "doris.url")]
    pub url: String,
    #[serde(rename = "doris.user")]
    pub user: String,
    #[serde(rename = "doris.password")]
    pub password: String,
    #[serde(rename = "doris.database")]
    pub database: String,
    #[serde(rename = "doris.table")]
    pub table: String,
    #[serde(rename = "doris.partial_update")]
    pub partial_update: Option<String>,
    /// The query port of Doris FE (default `9030`), only used when `auto_create` is enabled, since
    /// DDL is issued over this `MySQL`-compatible protocol port.
    #[serde(rename = "doris.query_port")]
    pub query_port: Option<String>,
    /// Automatically create the target database and table if they don't exist. Defaults to false.
    #[serde(default)]
    #[serde_as(as = "DisplayFromStr")]
    pub auto_create: bool,
    /// Number of replicas for the auto-created table. Only used when `auto_create` is enabled.
    /// When unset, the Doris cluster default is used.
    #[serde(rename = "doris.replication_num")]
    pub replication_num: Option<String>,
}

impl EnforceSecret for DorisCommon {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf::phf_set! {
        "doris.password", "doris.user"
    };
}

impl DorisCommon {
    pub(crate) fn build_get_client(&self) -> DorisSchemaClient {
        DorisSchemaClient::new(
            self.url.clone(),
            self.table.clone(),
            self.database.clone(),
            self.user.clone(),
            self.password.clone(),
        )
    }

    /// Extract the FE host from `doris.url` (e.g. `http://fe:8030` -> `fe`), used to build the
    /// `MySQL`-protocol connection for DDL.
    fn fe_host(&self) -> Result<String> {
        Ok(Url::parse(&self.url)
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?
            .host_str()
            .ok_or_else(|| SinkError::DorisStarrocksConnect(anyhow!("Can't get fe host from url")))?
            .to_owned())
    }

    /// Build a `MySQL`-protocol client for issuing DDL against Doris FE. Falls back to the
    /// default Doris FE query port `9030` when `doris.query_port` is not set.
    pub(crate) async fn build_ddl_client(&self) -> Result<DorisDdlClient> {
        let port = self.query_port.clone().unwrap_or_else(|| "9030".to_owned());
        DorisDdlClient::new(self.fe_host()?, port, self.user.clone(), self.password.clone()).await
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct DorisConfig {
    #[serde(flatten)]
    pub common: DorisCommon,

    pub r#type: String, // accept "append-only" or "upsert"

    /// The timeout in milliseconds for stream load http request, defaults to 30 seconds.
    #[serde(
        rename = "doris.stream_load.http.timeout.ms",
        default = "default_stream_load_http_timeout_ms"
    )]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub stream_load_http_timeout_ms: u64,

    #[serde(flatten)]
    pub unknown_fields: std::collections::HashMap<String, String>,
}

crate::impl_sink_unknown_fields!(DorisConfig);

impl EnforceSecret for DorisConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        DorisCommon::enforce_one(prop)
    }
}

impl DorisConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<DorisConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

#[derive(Debug)]
pub struct DorisSink {
    pub config: DorisConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl EnforceSecret for DorisSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            DorisConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl DorisSink {
    pub fn new(
        config: DorisConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl DorisSink {
    fn check_column_name_and_type(&self, doris_column_fields: Vec<DorisField>) -> Result<()> {
        let doris_columns_desc: HashMap<String, String> = doris_column_fields
            .iter()
            .map(|s| (s.name.clone(), s.r#type.clone()))
            .collect();

        let rw_fields_name = self.schema.fields();
        if rw_fields_name.len() > doris_columns_desc.len() {
            return Err(SinkError::Doris(
                "The columns of the sink must be equal to or a superset of the target table's columns.".to_owned(),
            ));
        }

        for i in rw_fields_name {
            let value = doris_columns_desc.get(&i.name).ok_or_else(|| {
                SinkError::Doris(format!(
                    "Column name don't find in doris, risingwave is {:?} ",
                    i.name
                ))
            })?;
            if !Self::check_and_correct_column_type(&i.data_type, value.clone())? {
                return Err(SinkError::Doris(format!(
                    "Column type don't match, column name is {:?}. doris type is {:?} risingwave type is {:?} ",
                    i.name, value, i.data_type
                )));
            }
        }
        Ok(())
    }

    fn check_and_correct_column_type(
        rw_data_type: &DataType,
        doris_data_type: String,
    ) -> Result<bool> {
        let doris_data_type = doris_data_type.to_ascii_uppercase();
        let is_variant = doris_data_type.contains("VARIANT");
        match rw_data_type {
            risingwave_common::types::DataType::Boolean => Ok(doris_data_type.contains("BOOLEAN")),
            risingwave_common::types::DataType::Int16 => Ok(doris_data_type.contains("SMALLINT")),
            risingwave_common::types::DataType::Int32 => Ok(doris_data_type.contains("INT")),
            risingwave_common::types::DataType::Int64 => Ok(doris_data_type.contains("BIGINT")),
            risingwave_common::types::DataType::Float32 => Ok(doris_data_type.contains("FLOAT")),
            risingwave_common::types::DataType::Float64 => Ok(doris_data_type.contains("DOUBLE")),
            risingwave_common::types::DataType::Decimal => Ok(doris_data_type.contains("DECIMAL")),
            risingwave_common::types::DataType::Date => Ok(doris_data_type.contains("DATE")),
            risingwave_common::types::DataType::Varchar => {
                Ok(
                    doris_data_type.contains("STRING")
                        || doris_data_type.contains("VARCHAR")
                        || is_variant,
                )
            }
            risingwave_common::types::DataType::Time => {
                Err(SinkError::Doris("TIME is not supported for Doris sink. Please convert to VARCHAR or other supported types.".to_owned()))
            }
            risingwave_common::types::DataType::Timestamp => {
                Ok(doris_data_type.contains("DATETIME"))
            }
            risingwave_common::types::DataType::Timestamptz => Err(SinkError::Doris(
                "TIMESTAMP WITH TIMEZONE is not supported for Doris sink as Doris doesn't store time values with timezone information. Please convert to TIMESTAMP first.".to_owned(),
            )),
            risingwave_common::types::DataType::Interval => Err(SinkError::Doris(
                "INTERVAL is not supported for Doris sink. Please convert to VARCHAR or other supported types.".to_owned(),
            )),
            risingwave_common::types::DataType::Struct(_) => Ok(doris_data_type.contains("STRUCT")),
            risingwave_common::types::DataType::List(_) => Ok(doris_data_type.contains("ARRAY")),
            risingwave_common::types::DataType::Bytea => {
                Err(SinkError::Doris("BYTEA is not supported for Doris sink. Please convert to VARCHAR or other supported types.".to_owned()))
            }
            risingwave_common::types::DataType::Jsonb => {
                Ok(doris_data_type.contains("JSON") || is_variant)
            }
            risingwave_common::types::DataType::Serial => Ok(doris_data_type.contains("BIGINT")),
            risingwave_common::types::DataType::Int256 => {
                Err(SinkError::Doris("INT256 is not supported for Doris sink.".to_owned()))
            }
            risingwave_common::types::DataType::Map(_) => {
                Err(SinkError::Doris("MAP is not supported for Doris sink.".to_owned()))
            }
            DataType::Vector(_) => {
                Err(SinkError::Doris("VECTOR is not supported for Doris sink.".to_owned()))
            },
        }
    }

    /// Map a `RisingWave` data type to the Doris column type used for auto-created tables.
    fn get_doris_type_string(data_type: &DataType) -> Result<String> {
        match data_type {
            DataType::Boolean => Ok("BOOLEAN".to_owned()),
            DataType::Int16 => Ok("SMALLINT".to_owned()),
            DataType::Int32 => Ok("INT".to_owned()),
            DataType::Int64 | DataType::Serial => Ok("BIGINT".to_owned()),
            DataType::Float32 => Ok("FLOAT".to_owned()),
            DataType::Float64 => Ok("DOUBLE".to_owned()),
            DataType::Decimal => Ok("DECIMAL(38, 9)".to_owned()),
            DataType::Date => Ok("DATE".to_owned()),
            // Use microsecond precision to match RisingWave's timestamp resolution. Bare `DATETIME`
            // (scale 0) would silently drop sub-second digits when loading auto-created tables.
            DataType::Timestamp => Ok("DATETIME(6)".to_owned()),
            DataType::Varchar => Ok("VARCHAR(65533)".to_owned()),
            DataType::Jsonb => Ok("JSON".to_owned()),
            DataType::List(inner) => {
                Ok(format!("ARRAY<{}>", Self::get_doris_type_string(inner.elem())?))
            }
            DataType::Time => Err(SinkError::Doris(
                "TIME is not supported for Doris sink. Please convert to VARCHAR or other supported types.".to_owned(),
            )),
            DataType::Timestamptz => Err(SinkError::Doris(
                "TIMESTAMP WITH TIMEZONE is not supported for Doris sink as Doris doesn't store time values with timezone information. Please convert to TIMESTAMP first.".to_owned(),
            )),
            DataType::Interval => Err(SinkError::Doris(
                "INTERVAL is not supported for Doris sink. Please convert to VARCHAR or other supported types.".to_owned(),
            )),
            DataType::Struct(_) => Err(SinkError::Doris(
                "STRUCT is not supported for auto-creating Doris tables. Please create the table manually.".to_owned(),
            )),
            DataType::Bytea => Err(SinkError::Doris(
                "BYTEA is not supported for Doris sink. Please convert to VARCHAR or other supported types.".to_owned(),
            )),
            DataType::Int256 => Err(SinkError::Doris(
                "INT256 is not supported for Doris sink.".to_owned(),
            )),
            DataType::Map(_) => Err(SinkError::Doris(
                "MAP is not supported for Doris sink.".to_owned(),
            )),
            DataType::Vector(_) => Err(SinkError::Doris(
                "VECTOR is not supported for Doris sink.".to_owned(),
            )),
        }
    }

    /// Whether a `RisingWave` type maps to a Doris type that is allowed as a key column. Doris
    /// forbids `FLOAT`/`DOUBLE` and complex types (`JSON`, `ARRAY`, ...) as key columns.
    fn is_doris_key_type(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Serial
                | DataType::Decimal
                | DataType::Date
                | DataType::Timestamp
                | DataType::Varchar
        )
    }

    /// Quote an identifier for Doris DDL: wrap in backticks and escape embedded backticks by
    /// doubling them, so a column/database/table name containing a backtick can't produce
    /// malformed DDL.
    fn quote_ident(name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    /// Build a `CREATE TABLE` statement for the sink schema. Doris requires key columns to be the
    /// first columns in the table, so key columns are emitted first.
    fn build_create_table_sql(&self) -> Result<String> {
        let fields = self.schema.fields();

        // Determine the key columns. Upsert sinks key on the primary key. Append-only sinks use
        // the primary key when present; otherwise Doris still needs a (duplicate) key, and it must
        // consist of key-able types, so pick the first such column.
        let key_indices: Vec<usize> = if !self.pk_indices.is_empty() {
            self.pk_indices.clone()
        } else {
            let first_key_able = fields
                .iter()
                .position(|f| Self::is_doris_key_type(&f.data_type))
                .ok_or_else(|| {
                    SinkError::Doris(
                        "Cannot auto-create an append-only Doris table: no column has a type \
                         usable as a Doris key (e.g. all columns are FLOAT/DOUBLE/JSON). Please \
                         create the table manually or define a `primary_key`."
                            .to_owned(),
                    )
                })?;
            vec![first_key_able]
        };

        // Doris forbids FLOAT/DOUBLE/JSON/complex types as key columns. The append-only fallback
        // above already picks a key-able column, but the primary-key path must be guarded too, so
        // an unsupported key type fails with a clear message instead of a raw Doris DDL error.
        if let Some(&bad) = key_indices
            .iter()
            .find(|&&i| !Self::is_doris_key_type(&fields[i].data_type))
        {
            return Err(SinkError::Doris(format!(
                "Cannot auto-create Doris table: column `{}` of type {:?} cannot be used as a Doris \
                 key column (Doris forbids FLOAT/DOUBLE/JSON/complex types as keys). Please create \
                 the table manually or choose a different primary key.",
                fields[bad].name, fields[bad].data_type
            )));
        }

        // Order key columns first (in key order), followed by the remaining columns. Loads match
        // columns by name, so this reordering does not affect ingestion.
        let mut ordered_indices: Vec<usize> = key_indices.clone();
        for i in 0..fields.len() {
            if !key_indices.contains(&i) {
                ordered_indices.push(i);
            }
        }

        let mut columns = Vec::with_capacity(fields.len());
        for &i in &ordered_indices {
            let field = &fields[i];
            columns.push(format!(
                "{} {}",
                Self::quote_ident(&field.name),
                Self::get_doris_type_string(&field.data_type)?
            ));
        }

        let key_columns: Vec<String> = key_indices
            .iter()
            .map(|&i| Self::quote_ident(&fields[i].name))
            .collect();

        let mut sql = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} (\n  {}\n) ENGINE=OLAP\n",
            Self::quote_ident(&self.config.common.database),
            Self::quote_ident(&self.config.common.table),
            columns.join(",\n  ")
        );

        let key_clause = if self.is_append_only {
            "DUPLICATE KEY"
        } else {
            "UNIQUE KEY"
        };
        let key_list = key_columns.join(", ");
        sql.push_str(&format!("{}({})\n", key_clause, key_list));

        // Choose the distribution, always with AUTO bucketing so Doris sizes the bucket count.
        // Hashing on the key co-locates rows and is required for UNIQUE KEY (upsert) tables and
        // sensible when the user gave a primary key. But for an append-only table with no primary
        // key we picked an arbitrary key-able column above; hashing on it would risk severe bucket
        // skew if that column has low cardinality (e.g. a boolean), so we distribute rows randomly
        // instead to spread them evenly.
        if self.is_append_only && self.pk_indices.is_empty() {
            sql.push_str("DISTRIBUTED BY RANDOM BUCKETS AUTO\n");
        } else {
            sql.push_str(&format!("DISTRIBUTED BY HASH({}) BUCKETS AUTO\n", key_list));
        }

        let mut properties: Vec<String> = Vec::new();
        if let Some(replication_num) = &self.config.common.replication_num {
            properties.push(format!("\"replication_num\" = \"{}\"", replication_num));
        }
        if !self.is_append_only {
            // Required so the target UNIQUE KEY table honors the `__DORIS_DELETE_SIGN__` column
            // used by upsert deletes.
            properties.push("\"enable_unique_key_merge_on_write\" = \"true\"".to_owned());
        }
        if !properties.is_empty() {
            sql.push_str(&format!("PROPERTIES (\n  {}\n)", properties.join(",\n  ")));
        }

        Ok(sql)
    }

    /// Create the target database and table if they don't already exist. Uses the Doris FE
    /// `MySQL`-protocol port for DDL.
    async fn auto_create_database_and_table(&self) -> Result<()> {
        let mut client = self.config.common.build_ddl_client().await?;

        if !client.database_exists(&self.config.common.database).await? {
            let create_db_sql = format!(
                "CREATE DATABASE IF NOT EXISTS {}",
                Self::quote_ident(&self.config.common.database)
            );
            tracing::info!(sql = %create_db_sql, "auto-creating Doris database");
            client.execute_sql(&create_db_sql).await?;
        }

        if !client
            .table_exists(&self.config.common.database, &self.config.common.table)
            .await?
        {
            let create_table_sql = self.build_create_table_sql()?;
            tracing::info!(sql = %create_table_sql, "auto-creating Doris table");
            client.execute_sql(&create_table_sql).await?;
        }

        Ok(())
    }
}

impl Sink for DorisSink {
    type LogSinker = LogSinkerOf<DorisSinkWriter>;

    const SINK_NAME: &'static str = DORIS_SINK;

    crate::impl_validate_sink_unknown_fields!();

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(DorisSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?
        .into_log_sinker(SinkWriterMetrics::new(&writer_param)))
    }

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert doris sink (please define in `primary_key` field)"
            )));
        }
        // Auto-create the database and table if requested, before validating the schema below.
        if self.config.common.auto_create {
            self.auto_create_database_and_table().await?;
        }
        // check reachability
        let client = self.config.common.build_get_client();
        let doris_schema = client.get_schema_from_doris().await?;

        if !self.is_append_only && doris_schema.keys_type.ne("UNIQUE_KEYS") {
            return Err(SinkError::Config(anyhow!(
                "If you want to use upsert, please set the keysType of doris to UNIQUE_KEYS"
            )));
        }
        self.check_column_name_and_type(doris_schema.properties)?;
        Ok(())
    }
}

pub struct DorisSinkWriter {
    pub config: DorisConfig,
    #[expect(dead_code)]
    schema: Schema,
    #[expect(dead_code)]
    pk_indices: Vec<usize>,
    inserter_inner_builder: InserterInnerBuilder,
    is_append_only: bool,
    client: Option<DorisClient>,
    row_encoder: JsonEncoder,
}

impl TryFrom<SinkParam> for DorisSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = DorisConfig::from_btreemap(param.properties)?;
        DorisSink::new(config, schema, pk_indices, param.sink_type.is_append_only())
    }
}

impl DorisSinkWriter {
    pub async fn new(
        config: DorisConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let mut decimal_map = HashMap::default();
        let mut variant_columns = HashSet::default();
        let doris_schema = config
            .common
            .build_get_client()
            .get_schema_from_doris()
            .await?;
        for s in &doris_schema.properties {
            if let Some(v) = s.get_decimal_pre_scale()? {
                decimal_map.insert(s.name.clone(), v);
            }
            if s.is_variant() {
                variant_columns.insert(s.name.clone());
            }
        }

        let header_builder = HeaderBuilder::new()
            .add_common_header()
            .set_user_password(config.common.user.clone(), config.common.password.clone())
            .add_json_format()
            .set_partial_columns(config.common.partial_update.clone())
            .add_read_json_by_line();
        let header = if !is_append_only {
            header_builder.add_hidden_column().build()
        } else {
            header_builder.build()
        };

        let doris_insert_builder = InserterInnerBuilder::new(
            config.common.url.clone(),
            config.common.database.clone(),
            config.common.table.clone(),
            header,
            config.stream_load_http_timeout_ms,
        )?;
        Ok(Self {
            config,
            schema: schema.clone(),
            pk_indices,
            inserter_inner_builder: doris_insert_builder,
            is_append_only,
            client: None,
            row_encoder: JsonEncoder::new_with_doris(
                schema,
                None,
                DorisJsonConfig {
                    decimal_scale: decimal_map,
                    variant_columns,
                },
            ),
        })
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }
            let row_json_string = Value::Object(self.row_encoder.encode(row)?).to_string();
            self.client
                .as_mut()
                .ok_or_else(|| SinkError::Doris("Can't find doris sink insert".to_owned()))?
                .write(row_json_string.into())
                .await?;
        }
        Ok(())
    }

    async fn upsert(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            match op {
                Op::Insert => {
                    let mut row_json_value = self.row_encoder.encode(row)?;
                    row_json_value
                        .insert(DORIS_DELETE_SIGN.to_owned(), Value::String("0".to_owned()));
                    let row_json_string = serde_json::to_string(&row_json_value).map_err(|e| {
                        SinkError::Doris(format!("Json derialize error: {}", e.as_report()))
                    })?;
                    self.client
                        .as_mut()
                        .ok_or_else(|| SinkError::Doris("Can't find doris sink insert".to_owned()))?
                        .write(row_json_string.into())
                        .await?;
                }
                Op::Delete => {
                    let mut row_json_value = self.row_encoder.encode(row)?;
                    row_json_value
                        .insert(DORIS_DELETE_SIGN.to_owned(), Value::String("1".to_owned()));
                    let row_json_string = serde_json::to_string(&row_json_value).map_err(|e| {
                        SinkError::Doris(format!("Json derialize error: {}", e.as_report()))
                    })?;
                    self.client
                        .as_mut()
                        .ok_or_else(|| SinkError::Doris("Can't find doris sink insert".to_owned()))?
                        .write(row_json_string.into())
                        .await?;
                }
                Op::UpdateDelete => {}
                Op::UpdateInsert => {
                    let mut row_json_value = self.row_encoder.encode(row)?;
                    row_json_value
                        .insert(DORIS_DELETE_SIGN.to_owned(), Value::String("0".to_owned()));
                    let row_json_string = serde_json::to_string(&row_json_value).map_err(|e| {
                        SinkError::Doris(format!("Json derialize error: {}", e.as_report()))
                    })?;
                    self.client
                        .as_mut()
                        .ok_or_else(|| SinkError::Doris("Can't find doris sink insert".to_owned()))?
                        .write(row_json_string.into())
                        .await?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for DorisSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.client.is_none() {
            self.client = Some(DorisClient::new(self.inserter_inner_builder.build().await?));
        }
        if self.is_append_only {
            self.append_only(chunk).await
        } else {
            self.upsert(chunk).await
        }
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<()> {
        if self.client.is_some() {
            let client = self
                .client
                .take()
                .ok_or_else(|| SinkError::Doris("Can't find doris inserter".to_owned()))?;
            client.finish().await?;
        }
        Ok(())
    }
}

pub struct DorisSchemaClient {
    url: String,
    table: String,
    db: String,
    user: String,
    password: String,
}
impl DorisSchemaClient {
    pub fn new(url: String, table: String, db: String, user: String, password: String) -> Self {
        Self {
            url,
            table,
            db,
            user,
            password,
        }
    }

    pub async fn get_schema_from_doris(&self) -> Result<DorisSchema> {
        let uri = format!("{}/api/{}/{}/_schema", self.url, self.db, self.table);

        let client = reqwest::Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build()
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;

        let response = client
            .get(uri)
            .header(
                "Authorization",
                format!(
                    "Basic {}",
                    general_purpose::STANDARD.encode(format!("{}:{}", self.user, self.password))
                ),
            )
            .send()
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;

        let json: Value = response
            .json()
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;
        let json_data = if json.get("code").is_some() && json.get("msg").is_some() {
            json.get("data")
                .ok_or_else(|| {
                    SinkError::DorisStarrocksConnect(anyhow::anyhow!("Can't find data"))
                })?
                .clone()
        } else {
            json
        };
        let schema: DorisSchema = serde_json::from_value(json_data)
            .context("Can't get schema from json")
            .map_err(SinkError::DorisStarrocksConnect)?;
        Ok(schema)
    }
}

/// A `MySQL`-protocol client against the Doris FE query port, used to issue DDL (auto-create).
/// Doris FE is `MySQL`-compatible, so this mirrors the StarRocks sink's schema client.
pub struct DorisDdlClient {
    conn: mysql_async::Conn,
}

impl DorisDdlClient {
    pub async fn new(host: String, port: String, user: String, password: String) -> Result<Self> {
        // username & password may contain special chars, so we need to URL-encode them,
        // otherwise `Opts::from_url` may report a `Parse error`.
        let user = form_urlencoded::byte_serialize(user.as_bytes()).collect::<String>();
        let password = form_urlencoded::byte_serialize(password.as_bytes()).collect::<String>();

        // Connect without selecting a database, so we can create it if it doesn't exist yet.
        let conn_uri = format!(
            "mysql://{}:{}@{}:{}/?prefer_socket={}&max_allowed_packet={}&wait_timeout={}",
            user,
            password,
            host,
            port,
            DORIS_MYSQL_PREFER_SOCKET,
            DORIS_MYSQL_MAX_ALLOWED_PACKET,
            DORIS_MYSQL_WAIT_TIMEOUT
        );
        let pool = mysql_async::Pool::new(
            Opts::from_url(&conn_uri).map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?,
        );
        let conn = pool
            .get_conn()
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;
        Ok(Self { conn })
    }

    pub async fn database_exists(&mut self, db: &str) -> Result<bool> {
        let query = format!(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = {:?}",
            db
        );
        let count: u64 = self
            .conn
            .query_first(query)
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?
            .unwrap_or(0);
        Ok(count > 0)
    }

    pub async fn table_exists(&mut self, db: &str, table: &str) -> Result<bool> {
        let query = format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = {:?} AND table_schema = {:?}",
            table, db
        );
        let count: u64 = self
            .conn
            .query_first(query)
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?
            .unwrap_or(0);
        Ok(count > 0)
    }

    pub async fn execute_sql(&mut self, sql: &str) -> Result<()> {
        self.conn
            .query_drop(sql)
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DorisSchema {
    status: i32,
    #[serde(rename = "keysType")]
    pub keys_type: String,
    pub properties: Vec<DorisField>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct DorisField {
    pub name: String,
    pub r#type: String,
    comment: String,
    pub precision: Option<String>,
    pub scale: Option<String>,
    aggregation_type: String,
}
impl DorisField {
    pub fn get_decimal_pre_scale(&self) -> Result<Option<u8>> {
        if self.r#type.contains("DECIMAL") {
            let scale = self
                .scale
                .as_ref()
                .ok_or_else(|| {
                    SinkError::Doris(format!(
                        "In doris, the type of {} is DECIMAL, but `scale` is not found",
                        self.name
                    ))
                })?
                .parse::<u8>()
                .map_err(|err| {
                    SinkError::Doris(format!(
                        "Unable to convert decimal's scale to u8. error: {:?}",
                        err.kind()
                    ))
                })?;
            Ok(Some(scale))
        } else {
            Ok(None)
        }
    }

    pub fn is_variant(&self) -> bool {
        self.r#type.to_ascii_uppercase().contains("VARIANT")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::{DorisConfig, DorisSink};
    use crate::sink::SINK_TYPE_APPEND_ONLY;

    #[test]
    fn test_jsonb_can_write_to_variant() {
        assert!(
            DorisSink::check_and_correct_column_type(&DataType::Jsonb, "VARIANT".into()).unwrap()
        );
    }

    #[test]
    fn test_varchar_can_write_to_variant() {
        assert!(
            DorisSink::check_and_correct_column_type(&DataType::Varchar, "VARIANT".into()).unwrap()
        );
    }

    fn base_properties(r#type: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("doris.url".to_owned(), "http://127.0.0.1:8030".to_owned()),
            ("doris.user".to_owned(), "root".to_owned()),
            ("doris.password".to_owned(), "".to_owned()),
            ("doris.database".to_owned(), "demo".to_owned()),
            ("doris.table".to_owned(), "sink_table".to_owned()),
            ("type".to_owned(), r#type.to_owned()),
        ])
    }

    fn build_sink(r#type: &str, is_append_only: bool) -> DorisSink {
        let config = DorisConfig::from_btreemap(base_properties(r#type)).unwrap();
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int64, "id"),
            Field::with_name(DataType::Varchar, "name"),
            Field::with_name(DataType::Int32, "age"),
        ]);
        DorisSink::new(config, schema, vec![0], is_append_only).unwrap()
    }

    #[test]
    fn test_build_create_table_sql_upsert_puts_key_first_and_merge_on_write() {
        let sink = build_sink("upsert", false);
        let sql = sink.build_create_table_sql().unwrap();
        assert!(sql.contains("UNIQUE KEY(`id`)"), "sql: {sql}");
        assert!(
            sql.contains("DISTRIBUTED BY HASH(`id`) BUCKETS AUTO"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("\"enable_unique_key_merge_on_write\" = \"true\""),
            "sql: {sql}"
        );
    }

    #[test]
    fn test_build_create_table_sql_append_only_uses_duplicate_key() {
        let sink = build_sink(SINK_TYPE_APPEND_ONLY, true);
        let sql = sink.build_create_table_sql().unwrap();
        assert!(sql.contains("DUPLICATE KEY(`id`)"), "sql: {sql}");
        assert!(
            !sql.contains("enable_unique_key_merge_on_write"),
            "sql: {sql}"
        );
    }

    #[test]
    fn test_build_create_table_sql_append_only_no_pk_picks_key_able_column() {
        // Append-only sink with no primary key whose first column (`score`) is a non-key-able
        // type. The DDL must still declare a valid duplicate key over a key-able column (`id`),
        // reordered to the front, rather than emitting a keyless table that Doris rejects.
        let config = DorisConfig::from_btreemap(base_properties(SINK_TYPE_APPEND_ONLY)).unwrap();
        let schema = Schema::new(vec![
            Field::with_name(DataType::Float64, "score"),
            Field::with_name(DataType::Int64, "id"),
        ]);
        let sink = DorisSink::new(config, schema, vec![], true).unwrap();
        let sql = sink.build_create_table_sql().unwrap();
        assert!(sql.contains("DUPLICATE KEY(`id`)"), "sql: {sql}");
        // No user-defined key, so distribute randomly rather than hashing on the arbitrarily
        // picked key column (which could skew badly for a low-cardinality column).
        assert!(sql.contains("DISTRIBUTED BY RANDOM"), "sql: {sql}");
    }

    #[test]
    fn test_build_create_table_sql_append_only_no_key_able_column_errors() {
        // No primary key and no key-able column: auto-create cannot pick a valid key, so it must
        // fail with a clear error instead of producing invalid DDL.
        let config = DorisConfig::from_btreemap(base_properties(SINK_TYPE_APPEND_ONLY)).unwrap();
        let schema = Schema::new(vec![Field::with_name(DataType::Float64, "score")]);
        let sink = DorisSink::new(config, schema, vec![], true).unwrap();
        assert!(sink.build_create_table_sql().is_err());
    }

    #[test]
    fn test_build_create_table_sql_upsert_non_key_able_pk_errors() {
        // Upsert primary key on a non-key-able type (DOUBLE): auto-create must reject it with a
        // clear error instead of emitting DDL that Doris rejects with a raw low-level error.
        let config = DorisConfig::from_btreemap(base_properties("upsert")).unwrap();
        let schema = Schema::new(vec![
            Field::with_name(DataType::Float64, "score"),
            Field::with_name(DataType::Int64, "id"),
        ]);
        let sink = DorisSink::new(config, schema, vec![0], false).unwrap();
        assert!(sink.build_create_table_sql().is_err());
    }

    #[test]
    fn test_build_create_table_sql_escapes_backtick_in_identifier() {
        // A column name containing a backtick must be escaped (doubled) so the generated DDL stays
        // well-formed rather than breaking out of the backtick-quoted identifier.
        let config = DorisConfig::from_btreemap(base_properties(SINK_TYPE_APPEND_ONLY)).unwrap();
        let schema = Schema::new(vec![Field::with_name(DataType::Int64, "we`ird")]);
        let sink = DorisSink::new(config, schema, vec![0], true).unwrap();
        let sql = sink.build_create_table_sql().unwrap();
        assert!(sql.contains("`we``ird`"), "sql: {sql}");
    }

    #[test]
    fn test_get_doris_type_string() {
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Int64).unwrap(),
            "BIGINT"
        );
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Int32.list()).unwrap(),
            "ARRAY<INT>"
        );
        assert!(DorisSink::get_doris_type_string(&DataType::Timestamptz).is_err());
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DorisInsertResultResponse {
    #[serde(rename = "TxnId")]
    txn_id: i64,
    #[serde(rename = "Label")]
    label: String,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "TwoPhaseCommit")]
    two_phase_commit: String,
    #[serde(rename = "Message")]
    message: String,
    #[serde(rename = "NumberTotalRows")]
    number_total_rows: i64,
    #[serde(rename = "NumberLoadedRows")]
    number_loaded_rows: i64,
    #[serde(rename = "NumberFilteredRows")]
    number_filtered_rows: i32,
    #[serde(rename = "NumberUnselectedRows")]
    number_unselected_rows: i32,
    #[serde(rename = "LoadBytes")]
    load_bytes: i64,
    #[serde(rename = "LoadTimeMs")]
    load_time_ms: i32,
    #[serde(rename = "BeginTxnTimeMs")]
    begin_txn_time_ms: i32,
    #[serde(rename = "StreamLoadPutTimeMs")]
    stream_load_put_time_ms: i32,
    #[serde(rename = "ReadDataTimeMs")]
    read_data_time_ms: i32,
    #[serde(rename = "WriteDataTimeMs")]
    write_data_time_ms: i32,
    #[serde(rename = "CommitAndPublishTimeMs")]
    commit_and_publish_time_ms: i32,
    #[serde(rename = "ErrorURL")]
    err_url: Option<String>,
}

pub struct DorisClient {
    insert: InserterInner,
    is_first_record: bool,
}
impl DorisClient {
    pub fn new(insert: InserterInner) -> Self {
        Self {
            insert,
            is_first_record: true,
        }
    }

    pub async fn write(&mut self, data: Bytes) -> Result<()> {
        let mut data_build = BytesMut::new();
        if self.is_first_record {
            self.is_first_record = false;
        } else {
            data_build.put_slice("\n".as_bytes());
        }
        data_build.put_slice(&data);
        self.insert.write(data_build.into()).await?;
        Ok(())
    }

    pub async fn finish(self) -> Result<DorisInsertResultResponse> {
        let raw = self.insert.finish().await?;
        let res: DorisInsertResultResponse = serde_json::from_slice(&raw)
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;

        if !DORIS_SUCCESS_STATUS.contains(&res.status.as_str()) {
            return Err(SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                "Insert error: {:?}, error url: {:?}",
                res.message,
                res.err_url
            )));
        };
        Ok(res)
    }
}
