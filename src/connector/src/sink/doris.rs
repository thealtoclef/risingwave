// Copyright 2025 RisingWave Labs
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

use std::collections::{BTreeMap, HashMap};

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose;
use bytes::{BufMut, Bytes, BytesMut};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use serde::Deserialize;
use serde_derive::Serialize;
use serde_json::Value;
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use with_options::WithOptions;

use super::doris_starrocks_connector::{
    DORIS_DELETE_SIGN, DORIS_SUCCESS_STATUS, HeaderBuilder, InserterInner, InserterInnerBuilder,
    POOL_IDLE_TIMEOUT,
};
use super::{
    Result, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinkError, SinkWriterMetrics,
};
use crate::enforce_secret::EnforceSecret;
use crate::sink::encoder::{JsonEncoder, RowEncoder};
use crate::sink::writer::{LogSinkerOf, SinkWriterExt};
use crate::sink::{DummySinkCommitCoordinator, Sink, SinkParam, SinkWriter, SinkWriterParam};

pub const DORIS_SINK: &str = "doris";

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
    #[serde(default)] // default false
    #[serde_as(as = "DisplayFromStr")]
    pub auto_create: bool,
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
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct DorisConfig {
    #[serde(flatten)]
    pub common: DorisCommon,

    pub r#type: String, // accept "append-only" or "upsert"
}

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
            if !Self::check_and_correct_column_type(&i.data_type, value.to_string())? {
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
                Ok(doris_data_type.contains("STRING") | doris_data_type.contains("VARCHAR"))
            }
            risingwave_common::types::DataType::Time => {
                Err(SinkError::Doris("doris can not support Time".to_owned()))
            }
            risingwave_common::types::DataType::Timestamp => {
                Ok(doris_data_type.contains("DATETIME"))
            }
            risingwave_common::types::DataType::Timestamptz => Err(SinkError::Doris(
                "TIMESTAMP WITH TIMEZONE is not supported for Doris sink as Doris doesn't store time values with timezone information. Please convert to TIMESTAMP first.".to_owned(),
            )),
            risingwave_common::types::DataType::Interval => Err(SinkError::Doris(
                "doris can not support Interval".to_owned(),
            )),
            risingwave_common::types::DataType::Struct(_) => Ok(doris_data_type.contains("STRUCT")),
            risingwave_common::types::DataType::List(_) => Ok(doris_data_type.contains("ARRAY")),
            risingwave_common::types::DataType::Bytea => {
                Err(SinkError::Doris("doris can not support Bytea".to_owned()))
            }
            risingwave_common::types::DataType::Jsonb => Ok(doris_data_type.contains("JSON")),
            risingwave_common::types::DataType::Serial => Ok(doris_data_type.contains("BIGINT")),
            risingwave_common::types::DataType::Int256 => {
                Err(SinkError::Doris("doris can not support Int256".to_owned()))
            }
            risingwave_common::types::DataType::Map(_) => {
                Err(SinkError::Doris("doris can not support Map".to_owned()))
            }
            DataType::Vector(_) => todo!("VECTOR_PLACEHOLDER"),
        }
    }

    fn get_doris_type_string(data_type: &DataType) -> Result<String> {
        match data_type {
            // Numeric types
            DataType::Boolean => Ok("BOOLEAN".to_string()),
            DataType::Int16 => Ok("SMALLINT".to_string()),
            DataType::Int32 => Ok("INT".to_string()),
            DataType::Int64 | DataType::Serial => Ok("BIGINT".to_string()),
            DataType::Float32 => Ok("FLOAT".to_string()),
            DataType::Float64 => Ok("DOUBLE".to_string()),
            DataType::Decimal => Ok("DECIMAL(38, 9)".to_string()), 
            
            // Date/Time types
            DataType::Date => Ok("DATE".to_string()),
            DataType::Timestamp => Ok("DATETIME".to_string()),
            
            // String types
            DataType::Varchar => Ok("VARCHAR(65533)".to_string()), 
            
            // Semi-structured types
            DataType::Jsonb => Ok("JSON".to_string()),
            DataType::List(list) => {
                let inner_type = Self::get_doris_type_string(list.as_ref())?;
                Ok(format!("ARRAY<{}>", inner_type))
            },
            DataType::Struct(_) => Ok("STRUCT".to_string()),
            DataType::Map(_) => Ok("MAP".to_string()),  
            
            // Unsupported types with clear error messages
            DataType::Time => Err(SinkError::Doris(
                "TIME type is not supported in Doris. Please convert to VARCHAR or TIMESTAMP".to_owned(),
            )),
            DataType::Timestamptz => Err(SinkError::Doris(
                "TIMESTAMP WITH TIMEZONE is not supported in Doris as it doesn't store timezone information. Please convert to DATETIME first.".to_owned(),
            )),
            DataType::Interval => Err(SinkError::Doris(
                "INTERVAL type is not supported in Doris. Please convert to VARCHAR or TIMESTAMP".to_owned(),
            )),
            DataType::Bytea => Err(SinkError::Doris(
                "BYTEA type is not supported in Doris. Please convert to VARCHAR or use a different storage format".to_owned(),
            )),
            DataType::Int256 => Err(SinkError::Doris(
                "INT256 type is not supported in Doris. Please use a different integer type".to_owned(),
            )),
            DataType::Vector(_) => Err(SinkError::Doris(
                "VECTOR type is not currently supported in Doris".to_owned(),
            )),
        }
    }

    async fn auto_create_database(&self, client: &mut DorisSchemaClient) -> Result<()> {
        let create_database_sql = format!(
            "CREATE DATABASE IF NOT EXISTS `{}`",
            self.config.common.database
        );

        tracing::info!("Creating Doris database with SQL: {}", create_database_sql);
        client.statement_execute(&create_database_sql).await?;
        Ok(())
    }

    async fn auto_create_table(&self, client: &mut DorisSchemaClient) -> Result<()> {
        // Build column definitions
        let mut columns = Vec::new();
        let mut primary_keys = Vec::new();

        for (index, field) in self.schema.fields().iter().enumerate() {
            // Get the Doris type string for the column
            let doris_type = Self::get_doris_type_string(&field.data_type)?;
            columns.push(format!("`{}` {}", field.name, doris_type));

            // If the column is a primary key, add it to the primary keys list
            if self.pk_indices.contains(&index) {
                primary_keys.push(format!("`{}`", field.name));
            }
        }

        // Start building the CREATE TABLE statement
        let mut create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS `{}`.`{}` ({}) ",
            self.config.common.database,
            self.config.common.table,
            columns.join(", ")
        );

        // Add engine type (OLAP is the default in Doris)
        create_table_sql.push_str("ENGINE=OLAP ");

        if !primary_keys.is_empty() {
            // Add key definition based on mode
            create_table_sql.push_str(&format!(
                "{} ({}) ",
                if !self.is_append_only { "UNIQUE KEY" } else { "DUPLICATE KEY" },
                primary_keys.join(", ")
            ));

            // Add distribution strategy
            create_table_sql.push_str(&format!(
                "DISTRIBUTED BY HASH({}) BUCKETS AUTO ",
                primary_keys.join(", ")
            ));
        }

        // Add table properties
        let mut properties = Vec::new();

        // Add bloom filter support for eligible columns
        let bloom_filter_columns: Vec<String> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                // Check if column type supports bloom filter
                let supports_bloom = match &field.data_type {
                    // These types don't support bloom filter according to Doris docs
                    DataType::Float32 | DataType::Float64 | DataType::Decimal => false,

                    // All other types can have bloom filters
                    _ => true,
                };

                // Add column to bloom filter list if it supports bloom filters and is a primary key
                if supports_bloom && self.pk_indices.contains(&index) {
                    Some(field.name.clone())
                } else {
                    None
                }
            })
            .collect();

        // Add bloom filter property if we have eligible columns
        if !bloom_filter_columns.is_empty() {
            properties.push(format!(
                "\"bloom_filter_columns\" = \"{}\"",
                bloom_filter_columns.join(",")
            ));
        }

        if !properties.is_empty() {
            create_table_sql.push_str(&format!("PROPERTIES ({})", properties.join(", ")));
        }

        tracing::info!("Creating Doris table with SQL: {}", create_table_sql);
        client.statement_execute(&create_table_sql).await?;
        Ok(())
    }
}

impl Sink for DorisSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<DorisSinkWriter>;

    const SINK_NAME: &'static str = DORIS_SINK;

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
        // create database and table if auto_create is true
        if self.config.common.auto_create {
            let mut client = self.config.common.build_get_client();
            self.auto_create_database(&mut client).await?;
            self.auto_create_table(&mut client).await?;

            tracing::info!(
                "Database {} and table {} are created successfully if they don't exist.",
                self.config.common.database,
                self.config.common.table
            );
            return Ok(());
        }

        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert doris sink (please define in `primary_key` field)"
            )));
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
        let config = DorisConfig::from_btreemap(param.properties)?;
        DorisSink::new(
            config,
            schema,
            param.downstream_pk,
            param.sink_type.is_append_only(),
        )
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
        let doris_schema = config
            .common
            .build_get_client()
            .get_schema_from_doris()
            .await?;
        for s in &doris_schema.properties {
            if let Some(v) = s.get_decimal_pre_scale()? {
                decimal_map.insert(s.name.clone(), v);
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
        )?;
        Ok(Self {
            config,
            schema: schema.clone(),
            pk_indices,
            inserter_inner_builder: doris_insert_builder,
            is_append_only,
            client: None,
            row_encoder: JsonEncoder::new_with_doris(schema, None, decimal_map),
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

    pub async fn statement_execute(&mut self, sql: &str) -> Result<()> {
        let uri = format!("{}/api/query/internal/information_schema", self.url);

        let client = reqwest::Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build()
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;

        let request_body = serde_json::json!({ "stmt": sql });

        let response = client
            .post(uri)
            .header(
                "Authorization",
                format!(
                    "Basic {}",
                    general_purpose::STANDARD.encode(format!("{}:{}", self.user, self.password))
                ),
            )
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;

        // Check if the request was successful (status code 2xx)
        if !response.status().is_success() {
            return Err(SinkError::DorisStarrocksConnect(anyhow!(
                "Request failed with status: {}",
                response.status()
            )));
        }

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
