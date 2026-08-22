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
use std::num::NonZeroU64;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose;
use bytes::{BufMut, Bytes, BytesMut};
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder};
use arrow_58_ipc::writer::StreamWriter;
use risingwave_common::array::arrow::{
    Arrow58ToArrow, arrow_array_58,
    arrow_array_58::{Int32Array, RecordBatch},
    arrow_schema_58,
};
use risingwave_common::array::{Array, ArrayError, ArrayImpl, Op, StreamChunk, StreamChunkBuilder};
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use risingwave_common_estimate_size::EstimateSize;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedSender;
use url::Url;
use with_options::WithOptions;

use crate::connector_common::IcebergSinkCompactionUpdate;

use super::coordinate::CoordinatedLogSinker;
use super::decouple_checkpoint_log_sink::default_commit_checkpoint_interval;
use super::doris_starrocks_connector::{
    DORIS_DELETE_SIGN, DORIS_SUCCESS_STATUS, HeaderBuilder, InserterInner, InserterInnerBuilder,
    POOL_IDLE_TIMEOUT,
};
use super::{
    Result, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinglePhaseCommitCoordinator,
    SinkCommitCoordinator, SinkError,
};
use crate::enforce_secret::EnforceSecret;
use crate::sink::encoder::{DorisJsonConfig, JsonEncoder, RowEncoder};
use crate::sink::writer::SinkWriter;
use crate::sink::{Sink, SinkParam, SinkWriterParam};
use risingwave_pb::connector_service::{SinkMetadata, sink_metadata};

pub const DORIS_SINK: &str = "doris";

/// The stream-load body format. `"json"` (default) and `"arrow"` are supported. Arrow is faster
/// to ingest than JSON because Doris parses the typed columns directly instead of re-parsing a
/// JSON text body, and it supports every RisingWave type that JSON does.
pub const DORIS_FORMAT_JSON: &str = "json";
pub const DORIS_FORMAT_ARROW: &str = "arrow";

// Connection parameters for the MySQL-protocol query endpoint of Doris FE, only used for DDL
// (e.g. auto-create). `mysql_async` applies `max_allowed_packet` as a client-side cap on the
// outbound packet, and this client sends `CREATE TABLE` statements, so it needs enough room for
// the DDL of a wide table or auto-create fails with `PacketTooLarge`.
const DORIS_MYSQL_MAX_ALLOWED_PACKET: usize = 1024 * 1024;
const DORIS_MYSQL_WAIT_TIMEOUT: usize = 28800;

const fn default_stream_load_http_timeout_ms() -> u64 {
    30 * 1000
}

fn default_format() -> String {
    DORIS_FORMAT_JSON.to_owned()
}

/// Default cap on the payload of a single stream load. See
/// [`DorisConfig::max_batch_size_bytes`] for why this defaults to a finite value.
const fn default_max_batch_size_bytes() -> u64 {
    100 * 1024 * 1024
}

const fn default_strict_mode() -> bool {
    true
}

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct DorisCommon {
    #[serde(rename = "doris.url")]
    pub url: String,
    /// The full MySQL-protocol Doris FE URL with an explicit port, used for DDL when `auto_create`
    /// is enabled or schema change is used, e.g. `mysql://query-fe:9030`.
    #[serde(rename = "doris.query_url")]
    pub query_url: Option<String>,
    /// The HTTP endpoint used for stream-load (`_stream_load`) requests. This must be a Doris BE
    /// HTTP address (e.g. `http://be:8040`), or something that forwards to one; setting it makes
    /// the sink stop probing for the FE → BE redirect, so an FE address here is rejected with an
    /// error on the first load. When unset, stream loads go to the FE on `doris.url` and follow the
    /// FE → BE 307 redirect.
    ///
    /// Set this when the network topology makes the FE-issued redirect unusable: the FE
    /// advertises BE addresses that aren't routable from RisingWave (a common Kubernetes case),
    /// or the BEs are only reachable through a Service / load balancer. The FE on `doris.url` is
    /// still used for schema (`/_schema`) lookups during sink creation, so it must remain
    /// reachable for that purpose.
    ///
    /// What you give up: the FE's load balancing across BEs and its failover away from a dead BE.
    /// A single bare BE address is a single point of failure. Only one address is accepted; point
    /// it at a Service or load balancer if you need more than one BE behind it.
    #[serde(rename = "doris.stream_load_url")]
    pub stream_load_url: Option<String>,
    #[serde(rename = "doris.user")]
    pub user: String,
    #[serde(rename = "doris.password")]
    pub password: String,
    #[serde(rename = "doris.database")]
    pub database: String,
    #[serde(rename = "doris.table")]
    pub table: String,
    /// Enable partial update, so a sink whose schema covers only some of the target table's
    /// columns updates just those columns instead of replacing the whole row. Accepts exactly
    /// `'true'` or `'false'`.
    #[serde(rename = "doris.partial_update")]
    pub partial_update: Option<String>,
    /// Automatically create the target database and table if they don't exist. Defaults to false.
    /// Accepts exactly `'true'` or `'false'`; other spellings are rejected.
    ///
    /// The DDL runs during sink validation, so if `CREATE SINK` fails afterwards for an unrelated
    /// reason the Doris table is left behind. This is deliberate: dropping a table because sink
    /// creation failed would be a far worse failure mode.
    #[serde(default)]
    #[serde_as(as = "DisplayFromStr")]
    pub auto_create: bool,
    /// Number of replicas for the auto-created table. Only used when `auto_create` is
    /// enabled.
    /// When unset, the Doris cluster default is used.
    #[serde(rename = "doris.replication_num")]
    pub replication_num: Option<String>,
    /// Partition the auto-created table by the given columns, materialized by Doris's
    /// auto-partitioning. Only used when `auto_create` is enabled.
    ///
    /// The value selects the partition kind by its shape, mirroring the Iceberg sink's
    /// `partition_by` option:
    /// - `month(ts)` (a date-trunc granularity applied to one column) partitions by RANGE:
    ///   `AUTO PARTITION BY RANGE(date_trunc(`ts`, 'month'))()`. The granularity is one of
    ///   `year`, `month`, `week`, `day`, `hour`, and the column must map to a Doris
    ///   `DATE`/`DATETIME` type (a RisingWave `Date` or `Timestamp`).
    /// - bare column(s) partition by LIST: `a` becomes
    ///   `AUTO PARTITION BY LIST(`a`)()`, and `a, b` becomes a multi-column
    ///   `AUTO PARTITION BY LIST(`a`, `b`)()`. Each column must be a key column with a
    ///   Doris-allowable LIST partition type.
    ///
    /// The function form and the bare form cannot be mixed: `month(ts), id` is rejected.
    /// Because Doris auto-RANGE only accepts a `date_trunc` expression, a bare column
    /// can only mean LIST, so the shape alone determines the partition kind.
    #[serde(rename = "doris.partition_by")]
    pub partition_by: Option<String>,
    /// Store RisingWave `timestamptz` values in a Doris `DATETIME` column, accepting that the
    /// timezone is lost. Defaults to false.
    ///
    /// Doris 4 added a native `TIMESTAMPTZ` column (a UTC microsecond instant, rendered in the
    /// session timezone on read), which is what the sink uses by default. Doris 3 has no such
    /// type, so a `timestamptz` can only be written there as `DATETIME`, where Doris interprets
    /// the value in the session timezone — the stored instant then depends on the FE/BE
    /// `time_zone` setting rather than on the value. Set this option to accept that loss for a
    /// Doris 3 target.
    #[serde(default)]
    #[serde_as(as = "DisplayFromStr")]
    pub timestamptz_as_datetime: bool,
}

impl EnforceSecret for DorisCommon {
    const ENFORCE_SECRET_PROPERTIES: phf::Set<&'static str> = phf::phf_set! {
        "doris.password", "doris.user"
    };
}

impl DorisCommon {
    /// Whether partial update is enabled. `partial_update` is a free-form string for backwards
    /// compatibility (it is passed through to the `partial_columns` header verbatim), so treat
    /// anything other than `true` as off.
    fn is_partial_update(&self) -> bool {
        self.partial_update
            .as_deref()
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    }

    pub(crate) fn build_get_client(&self) -> DorisSchemaClient {
        DorisSchemaClient::new(
            self.url.clone(),
            self.table.clone(),
            self.database.clone(),
            self.user.clone(),
            self.password.clone(),
        )
    }

    /// Return the `doris.query_url` value, erroring with a clear message when absent (since it is
    /// required when `auto_create` is enabled or schema change is used, both of which issue DDL via
    /// the FE MySQL port).
    fn get_query_url(&self) -> Result<&str> {
        self.query_url.as_deref().ok_or_else(|| {
            SinkError::DorisStarrocksConnect(anyhow!(
                "doris.query_url must be set when auto_create or schema change is used"
            ))
        })
    }

    /// Build a `MySQL`-protocol client for issuing DDL against Doris FE. Requires
    /// `doris.query_url` to be set.
    pub(crate) async fn build_ddl_client(&self) -> Result<DorisDdlClient> {
        let query_url = self.get_query_url()?;
        let opts = build_ddl_opts(query_url, &self.user, &self.password)?;
        DorisDdlClient::new(opts).await
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

    /// The stream-load body format, `"json"` (default) or `"arrow"`. Arrow is faster to ingest
    /// because Doris reads the typed columns directly instead of parsing a JSON text body, and it
    /// supports every type JSON does, including complex types.
    #[serde(rename = "doris.format", default = "default_format")]
    #[with_option(allow_alter_on_fly)]
    pub format: String,

    /// Set this option to a positive integer n and RisingWave will commit data to Doris every n
    /// checkpoints. A value greater than 1 requires the `sink_decouple` session config to be
    /// enabled. Defaults to 10.
    ///
    /// Doris has no multi-load transaction API, so every commit is one stream load and therefore
    /// one new table version. Committing on every checkpoint is the main cause of the Doris
    /// `-235 TOO_MANY_VERSIONS` error and of severe write amplification, which is why this
    /// defaults to 10 rather than 1.
    ///
    /// The cost is latency and a wider replay window: a failure mid-interval replays up to n
    /// checkpoints. On a UNIQUE KEY table that is idempotent, but on an append-only DUPLICATE KEY
    /// table the replayed rows become real duplicate rows.
    #[serde(default = "default_commit_checkpoint_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub commit_checkpoint_interval: u64,

    /// The maximum payload size in bytes of a single Doris stream load, defaults to 100MB. Once
    /// the current load reaches the cap it is committed and a new one is started, so this also
    /// bounds the memory a writer holds for an in-flight load.
    ///
    /// The cap defaults to a finite value because closing a load is what commits it in Doris, so
    /// with `commit_checkpoint_interval` above 1 one request stays open for the whole interval.
    /// This is what keeps a long interval from growing an unbounded in-flight load.
    ///
    /// Note that a split here is *not* atomic with the rest of the epoch: each split is an
    /// independently committed Doris load, so a mid-epoch failure leaves earlier splits committed
    /// and they are re-sent on replay.
    ///
    /// A single row whose payload exceeds the cap is still sent, as a load of its own, since a row
    /// cannot be split across loads.
    #[serde(
        rename = "doris.max_batch_size_bytes",
        default = "default_max_batch_size_bytes"
    )]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub max_batch_size_bytes: u64,

    /// Enable Doris strict mode, defaults to true. Accepts exactly `'true'` or `'false'`.
    ///
    /// With strict mode on, a value Doris cannot convert to the target column type is counted as
    /// a filtered row instead of being silently stored as NULL. Since the sink never raises
    /// Doris's `max_filter_ratio` above 0, a filtered row fails the load, which surfaces the bad
    /// value instead of corrupting the column.
    ///
    /// The cost is that a single unconvertible row stalls the sink, retrying in place. To get
    /// moving again at the price of the old silent-NULL behaviour, run
    /// `ALTER SINK <name> CONNECTOR WITH (doris.strict_mode = 'false')` — this takes effect
    /// immediately, even while the sink is stuck retrying.
    ///
    /// Strict mode does **not** cover range restrictions: a value of the right type but outside
    /// the column's declared range (for example a decimal exceeding its declared precision) is
    /// still stored as NULL regardless of this setting. Nor does it cover precision truncation —
    /// the Doris column's declared precision governs, so a `DATETIME(0)` target silently drops the
    /// sub-second digits of a RisingWave `timestamp`, which Doris considers a successful
    /// conversion. Declare the target column with the precision you want (`DATETIME(6)`).
    #[serde(rename = "doris.strict_mode", default = "default_strict_mode")]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub strict_mode: bool,

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
        if config.commit_checkpoint_interval == 0 {
            return Err(SinkError::Config(anyhow!(
                "`commit_checkpoint_interval` must be greater than 0"
            )));
        }
        if config.max_batch_size_bytes == 0 {
            return Err(SinkError::Config(anyhow!(
                "`doris.max_batch_size_bytes` must be greater than 0"
            )));
        }
        if config.format != DORIS_FORMAT_JSON && config.format != DORIS_FORMAT_ARROW {
            return Err(SinkError::Config(anyhow!(
                "`doris.format` must be \"{}\" or \"{}\", got {:?}",
                DORIS_FORMAT_JSON,
                DORIS_FORMAT_ARROW,
                config.format
            )));
        }
        // The value is interpolated into the `PROPERTIES` clause of the auto-create DDL, so reject
        // anything that isn't a plain positive integer instead of letting it inject arbitrary
        // properties (or produce an opaque Doris parse error).
        if let Some(replication_num) = &config.common.replication_num
            && !replication_num.parse::<u32>().is_ok_and(|num| num > 0)
        {
            return Err(SinkError::Config(anyhow!(
                "`doris.replication_num` must be a positive integer, got {:?}",
                replication_num
            )));
        }
        // `doris.partition_by` only shapes the auto-created table, so it is meaningless without
        // `auto_create`. Parsing it here also surfaces grammar errors at config time (before any
        // Doris connection is opened) rather than only when the DDL is built during validation.
        if let Some(partition_by) = &config.common.partition_by {
            if !config.common.auto_create {
                return Err(SinkError::Config(anyhow!(
                    "`doris.partition_by` only takes effect when `auto_create` is enabled"
                )));
            }
            parse_partition_by(partition_by)?;
        }
        Ok(config)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct LoadRequestSizeDecision {
    finish_current_load: bool,
    next_batch_size_bytes: u64,
}

/// Decide whether the current stream load must be finished before writing a row of `row_size`
/// bytes, and what the running batch size becomes afterwards.
///
/// A row larger than the cap on its own is not an error. It gets a load to itself, because a row
/// cannot be split across loads and the cap is a budget for batching rather than a Doris limit:
/// rejecting the row would stall the sink on something it can never make smaller.
fn decide_load_request_size(
    current_batch_size_bytes: u64,
    row_size: u64,
    max_batch_size_bytes: u64,
) -> LoadRequestSizeDecision {
    if current_batch_size_bytes > 0
        && current_batch_size_bytes
            .checked_add(row_size)
            .is_none_or(|next_batch_size_bytes| next_batch_size_bytes > max_batch_size_bytes)
    {
        return LoadRequestSizeDecision {
            finish_current_load: true,
            next_batch_size_bytes: row_size,
        };
    }

    LoadRequestSizeDecision {
        finish_current_load: false,
        next_batch_size_bytes: current_batch_size_bytes.saturating_add(row_size),
    }
}

#[derive(Debug)]
pub struct DorisSink {
    pub config: DorisConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    param: SinkParam,
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
        param: SinkParam,
    ) -> Result<Self> {
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
            param,
        })
    }
}

/// Doris integer types, narrowest first.
const DORIS_INT_WIDTHS: [&str; 5] = ["TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT"];

/// Normalize a Doris column type string for matching: upper-case it and drop any trailing
/// `(p)` / `(p,s)` precision suffix.
///
/// [`DorisField`] reports precision and scale as separate JSON fields, so `type` is normally a
/// bare name. The exception is `TIMESTAMPTZ`, which has been observed carrying an inline
/// precision, so strip it defensively for every type rather than special-casing one.
fn normalize_doris_type(doris_data_type: &str) -> String {
    let upper = doris_data_type.to_ascii_uppercase();
    match upper.split_once('(') {
        Some((base, _)) => base.trim_end().to_owned(),
        None => upper,
    }
}

/// Whether a normalized Doris type is acceptable for a `RisingWave` integer that needs at least
/// the width of `min_width`.
///
/// Doris stores an out-of-range integer as NULL rather than rejecting it, so narrowing must be
/// caught here: `Int32` may target `INT`/`BIGINT`/`LARGEINT` but not `TINYINT`/`SMALLINT`.
/// A spelling we don't recognize falls back to the historical `contains` check instead of being
/// rejected, so an unanticipated Doris type name cannot break a sink that works today. A name with
/// a modifier suffix (e.g. `BIGINT UNSIGNED`) is matched on its leading token, because the
/// `contains` fallback would otherwise accept a *narrower* type: `SMALLINT UNSIGNED` contains
/// `INT`, and letting it through is exactly the silent-NULL narrowing this check exists to stop.
fn check_doris_int_width(doris_data_type: &str, min_width: &str) -> bool {
    let min_rank = DORIS_INT_WIDTHS
        .iter()
        .position(|t| *t == min_width)
        .expect("min_width must be a known Doris integer type");
    let base = doris_data_type
        .split_once(char::is_whitespace)
        .map_or(doris_data_type, |(base, _)| base);
    match DORIS_INT_WIDTHS.iter().position(|t| *t == base) {
        Some(rank) => rank >= min_rank,
        None => doris_data_type.contains(min_width),
    }
}

/// The `date_trunc` granularities Doris accepts for auto-RANGE partitioning, in the spelling
/// used by `doris.partition_by`.
const DORIS_AUTO_PARTITION_GRANULARITIES: [&str; 5] = ["year", "month", "week", "day", "hour"];

/// The parsed shape of a `doris.partition_by` value.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DorisPartitionSpec {
    /// `granularity(column)`: a single `date_trunc` partition column, rendered as
    /// `AUTO PARTITION BY RANGE(date_trunc(`column`, 'granularity'))()`.
    Range { column: String, granularity: &'static str },
    /// Bare column(s): rendered as `AUTO PARTITION BY LIST(`col`, ...)()`.
    List { columns: Vec<String> },
}

/// Whether `name` is a plain Doris identifier (letter or `_` first, then letters, digits,
/// `_`). Partition columns are emitted into the DDL and looked up in the sink schema, so
/// anything else is rejected rather than risking malformed DDL or a confusing Doris error.
fn is_valid_partition_column(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Parse a `doris.partition_by` value into a [`DorisPartitionSpec`].
///
/// The grammar is self-describing, mirroring the Iceberg sink's `partition_by` option: a
/// function form `granularity(column)` means RANGE — the only RANGE spelling Doris accepts,
/// since auto-RANGE partitions on a `date_trunc` expression — while bare column(s) mean
/// LIST. The two forms cannot be mixed.
fn parse_partition_by(partition_by: &str) -> Result<DorisPartitionSpec> {
    let partition_by = partition_by.trim();
    if partition_by.is_empty() {
        return Err(SinkError::Config(anyhow!(
            "`doris.partition_by` must not be empty"
        )));
    }

    // RANGE form: a single `granularity(column)`.
    if partition_by.contains('(') {
        let (granularity, column) = partition_by
            .strip_suffix(')')
            .and_then(|s| s.split_once('('))
            .map(|(granularity, column)| (granularity.trim(), column.trim()))
            .ok_or_else(|| {
                SinkError::Config(anyhow!(
                    "Invalid `doris.partition_by`: {partition_by:?}\n\
                     HINT: use `granularity(column)` for RANGE partitioning (e.g. `month(ts)`), \
                     or bare column(s) for LIST partitioning (e.g. `id` or `id, city`)"
                ))
            })?;
        let granularity = DORIS_AUTO_PARTITION_GRANULARITIES
            .iter()
            .find(|g| **g == granularity)
            .ok_or_else(|| {
                SinkError::Config(anyhow!(
                    "`doris.partition_by` granularity must be one of {}, got {:?}",
                    DORIS_AUTO_PARTITION_GRANULARITIES.join(", "),
                    granularity
                ))
            })?;
        if !is_valid_partition_column(column) {
            return Err(SinkError::Config(anyhow!(
                "Invalid `doris.partition_by` partition column {column:?}: only plain \
                 identifiers (letters, digits, `_`) are allowed"
            )));
        }
        return Ok(DorisPartitionSpec::Range {
            column: column.to_owned(),
            granularity: *granularity,
        });
    }

    // LIST form: one or more bare columns.
    let columns: Vec<String> = partition_by
        .split(',')
        .map(str::trim)
        .map(str::to_owned)
        .collect();
    for column in &columns {
        if column.is_empty() || !is_valid_partition_column(column) {
            return Err(SinkError::Config(anyhow!(
                "Invalid `doris.partition_by` partition column {column:?}: expected bare \
                 identifiers (letters, digits, `_`) separated by commas, or a single \
                 `granularity(column)` RANGE expression"
            )));
        }
    }
    Ok(DorisPartitionSpec::List { columns })
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
                "The columns of the sink must be equal to or a subset of the target table's columns.".to_owned(),
            ));
        }

        for i in rw_fields_name {
            let value = doris_columns_desc.get(&i.name).ok_or_else(|| {
                SinkError::Doris(format!(
                    "Column name don't find in doris, risingwave is {:?} ",
                    i.name
                ))
            })?;
            if !Self::check_and_correct_column_type(
                &i.data_type,
                value.clone(),
                self.config.common.timestamptz_as_datetime,
            )? {
                return Err(SinkError::Doris(format!(
                    "Column type don't match, column name is {:?}. doris type is {:?} risingwave type is {:?} ",
                    i.name, value, i.data_type
                )));
            }
        }

        self.check_no_silent_column_blanking(&doris_columns_desc)?;
        Ok(())
    }

    /// Reject an upsert sink whose schema covers only part of the target table while partial
    /// update is off.
    ///
    /// A full-row upsert against a UNIQUE KEY merge-on-write table replaces the whole row, so the
    /// Doris columns the sink never mentions are silently reset to their default (or NULL) on every
    /// update. Append-only sinks are unaffected: writing a subset into a DUPLICATE KEY table just
    /// inserts rows whose remaining columns take their defaults, which is what the user asked for.
    fn check_no_silent_column_blanking(
        &self,
        doris_columns_desc: &HashMap<String, String>,
    ) -> Result<()> {
        if self.is_append_only || self.config.common.is_partial_update() {
            return Ok(());
        }

        let sink_column_names: HashSet<&str> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        let mut omitted: Vec<&str> = doris_columns_desc
            .keys()
            .map(|name| name.as_str())
            // Doris hidden columns (e.g. `__DORIS_DELETE_SIGN__`) are maintained by Doris itself.
            .filter(|name| !name.starts_with("__DORIS_") && !sink_column_names.contains(name))
            .collect();
        if omitted.is_empty() {
            return Ok(());
        }
        omitted.sort_unstable();

        Err(SinkError::Doris(format!(
            "This upsert sink does not cover the Doris columns {:?}, and `doris.partial_update` is \
             not enabled. Each update would replace the whole Doris row, silently resetting those \
             columns to their default or NULL. Either add them to the sink's schema, or set \
             `doris.partial_update = 'true'` to update only the columns the sink writes.",
            omitted
        )))
    }

    fn check_and_correct_column_type(
        rw_data_type: &DataType,
        doris_data_type: String,
        timestamptz_as_datetime: bool,
    ) -> Result<bool> {
        let doris_data_type = normalize_doris_type(&doris_data_type);
        let is_variant = doris_data_type.contains("VARIANT");
        match rw_data_type {
            risingwave_common::types::DataType::Boolean => Ok(doris_data_type.contains("BOOLEAN")),
            risingwave_common::types::DataType::Int16 => {
                Ok(check_doris_int_width(&doris_data_type, "SMALLINT"))
            }
            risingwave_common::types::DataType::Int32 => {
                Ok(check_doris_int_width(&doris_data_type, "INT"))
            }
            risingwave_common::types::DataType::Int64 => {
                Ok(check_doris_int_width(&doris_data_type, "BIGINT"))
            }
            risingwave_common::types::DataType::Float32 => Ok(doris_data_type.contains("FLOAT")),
            risingwave_common::types::DataType::Float64 => Ok(doris_data_type.contains("DOUBLE")),
            risingwave_common::types::DataType::Decimal => Ok(doris_data_type.contains("DECIMAL")),
            // `DATETIME`/`DATETIMEV2` contain `DATE`, but writing a date into one is a narrowing
            // in the other direction (the time part is invented), so reject it explicitly.
            risingwave_common::types::DataType::Date => Ok(doris_data_type.contains("DATE")
                && !doris_data_type.contains("DATETIME")),
            risingwave_common::types::DataType::Varchar => {
                Ok(
                    doris_data_type.contains("STRING")
                        || doris_data_type.contains("VARCHAR")
                        || is_variant,
                )
            }
            // Types with no natural Doris column are stored as text in a `STRING`/`VARCHAR`
            // column, mirroring `get_doris_type_string`'s fallback. The JSON encoder emits each as
            // a plain string, so a text column round-trips it losslessly; only a Doris `STRING` or
            // `VARCHAR` column is accepted.
            risingwave_common::types::DataType::Time
            | risingwave_common::types::DataType::Interval
            | risingwave_common::types::DataType::Bytea
            | risingwave_common::types::DataType::Int256 => {
                Ok(doris_data_type.contains("STRING") || doris_data_type.contains("VARCHAR"))
            }
            // `Serial` is an auto-incrementing `i64` and maps to Doris `BIGINT` (see
            // `get_doris_type_string`). Doris's stream-load converter parses a numeric string into
            // an integer column, which is what the encoder emits for `Serial`.
            risingwave_common::types::DataType::Serial => {
                Ok(check_doris_int_width(&doris_data_type, "BIGINT"))
            }
            risingwave_common::types::DataType::Timestamp => {
                Ok(doris_data_type.contains("DATETIME"))
            }
            risingwave_common::types::DataType::Timestamptz => {
                // (kept separate from the fallback: `Timestamptz` is written natively)
                // Doris 4 supports a native `TIMESTAMPTZ` (microsecond precision, UTC stored,
                // re-rendered in session TZ on read). Accept writes against `TIMESTAMPTZ` columns
                // and let the encoder emit tz-bearing strings; reject writes against `DATETIME`
                // because Doris interprets a tz-naive string in the session's timezone, which
                // makes the stored value depend on the Doris FE/BE `time_zone` setting rather
                // than the actual UTC instant.
                //
                // `timestamptz_as_datetime` opts into the `DATETIME` target for Doris 3 (which
                // has no `TIMESTAMPTZ` type), accepting that the timezone is lost.
                if doris_data_type.contains("TIMESTAMPTZ") {
                    Ok(true)
                } else if timestamptz_as_datetime && doris_data_type.contains("DATETIME") {
                    Ok(true)
                } else {
                    Err(SinkError::Doris(format!(
                        "TIMESTAMP WITH TIMEZONE can only be written to a Doris `TIMESTAMPTZ` \
                         column (the Doris column type is `{}`); either declare the target \
                         column as `TIMESTAMPTZ`, cast the source value with \
                         `... AT TIME ZONE '<offset>'` to a plain `TIMESTAMP` first, or set \
                         `timestamptz_as_datetime = 'true'` to accept storing it in a \
                         `DATETIME` column (losing the timezone).",
                        doris_data_type
                    )))
                }
            }
            risingwave_common::types::DataType::Struct(_) => Ok(doris_data_type.contains("STRUCT")),
            risingwave_common::types::DataType::List(_) => Ok(doris_data_type.contains("ARRAY")),
            risingwave_common::types::DataType::Jsonb => {
                Ok(doris_data_type.contains("JSON") || is_variant)
            }
            risingwave_common::types::DataType::Map(_) => Ok(doris_data_type.contains("MAP")),
            // A `Vector` is stored in Doris as a fixed-length `ARRAY<FLOAT>` column (Doris has no
            // dedicated `VECTOR` type; vector search is an ANN secondary index over an
            // `ARRAY<FLOAT>` column). Both encoders already emit it as a float array — the Arrow
            // path converts it to `List<Float32>` and the JSON encoder to `[f32, ...]` — so a
            // `Vector` value loads directly into an `ARRAY<FLOAT>` column.
            DataType::Vector(_) => Ok(doris_data_type.contains("ARRAY")),
        }
    }

    /// Map a `RisingWave` data type to the Doris column type used for auto-created tables.
    ///
    /// `is_key` selects between the two string types Doris offers, which differ in what they can
    /// hold: `STRING` is effectively unbounded but is rejected in key columns, while `VARCHAR(n)`
    /// is capped at 65533 *bytes*. Key columns therefore have no alternative to `VARCHAR`, and a
    /// key value longer than that stalls the sink; see [`Self::build_create_table_sql`].
    fn get_doris_type_string(
        data_type: &DataType,
        is_key: bool,
        timestamptz_as_datetime: bool,
    ) -> Result<String> {
        match data_type {
            DataType::Boolean => Ok("BOOLEAN".to_owned()),
            DataType::Int16 => Ok("SMALLINT".to_owned()),
            DataType::Int32 => Ok("INT".to_owned()),
            DataType::Int64 => Ok("BIGINT".to_owned()),
            DataType::Float32 => Ok("FLOAT".to_owned()),
            DataType::Float64 => Ok("DOUBLE".to_owned()),
            DataType::Decimal => Ok("DECIMAL(38, 9)".to_owned()),
            DataType::Date => Ok("DATE".to_owned()),
            // Use microsecond precision to match RisingWave's timestamp resolution. Bare `DATETIME`
            // (scale 0) would silently drop sub-second digits when loading auto-created tables.
            DataType::Timestamp => Ok("DATETIME(6)".to_owned()),
            // `RisingWave` `VARCHAR` is unbounded, so a non-key column becomes Doris `STRING`.
            // `VARCHAR(65533)` would reject any longer value, and because the sink loads with
            // `max_filter_ratio` at 0 one such row fails the whole load, which then retries
            // forever. Key columns cannot use `STRING`, so they keep `VARCHAR` at its maximum.
            DataType::Varchar => Ok(if is_key {
                "VARCHAR(65533)".to_owned()
            } else {
                "STRING".to_owned()
            }),
            DataType::Jsonb => Ok("JSON".to_owned()),
            // An `ARRAY` can never be a key column, so its elements are never key types either.
            DataType::List(inner) => Ok(format!(
                "ARRAY<{}>",
                Self::get_doris_type_string(inner.elem(), false, timestamptz_as_datetime)?
            )),
            // Doris DDL accepts `STRUCT<name:type, ...>` (verified against Doris 4 FE); the
            // parser requires a `:` between each field name and type, unlike the `ARRAY<T>`
            // sugar. Sub-field `VARCHAR` is emitted as `STRING`, which Doris normalizes to
            // `text` inside a struct (a bare `VARCHAR(n)` there would cap the value and fail
            // the load). A struct can never be a key column, so sub-fields are never keys.
            DataType::Struct(st) => Ok(format!(
                "STRUCT<{}>",
                st.iter()
                    .map(|(name, ty)| {
                        Self::get_doris_type_string(ty, false, timestamptz_as_datetime)
                            .map(|doris_type| format!("{}:{}", name, doris_type))
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?
                    .join(",")
            )),
            // Types with no natural Doris column are stored as text in a `STRING`/`VARCHAR`
            // column. The JSON encoder emits each as a plain string (see
            // `datum_to_json_object`), so a text column round-trips it losslessly. Reuse the
            // existing `Varchar` mapping (non-key `STRING`, key `VARCHAR(65533)`) rather than
            // duplicating it.
            DataType::Time
            | DataType::Interval
            | DataType::Bytea
            | DataType::Int256 => {
                Self::get_doris_type_string(&DataType::Varchar, is_key, timestamptz_as_datetime)
            }
            // `Serial` is an auto-incrementing `i64`, so it maps to Doris `BIGINT` rather than
            // text. The encoder emits it as a decimal string, which Doris's stream-load converter
            // parses into the integer column.
            DataType::Serial => Ok("BIGINT".to_owned()),
            // Doris 4 `TIMESTAMPTZ(p)` stores a UTC microsecond instant and renders it in
            // the session timezone at query time. Use precision 6 to match RisingWave's
            // `Timestamptz` resolution. With `timestamptz_as_datetime` set (a Doris 3 target,
            // which has no `TIMESTAMPTZ`), emit `DATETIME(6)` instead and accept tz loss.
            DataType::Timestamptz => Ok(if timestamptz_as_datetime {
                "DATETIME(6)".to_owned()
            } else {
                "TIMESTAMPTZ(6)".to_owned()
            }),
            // A `MAP` becomes Doris `MAP<K,V>` (verified against Doris 4 FE; the parser requires
            // a `,` between key and value type). A map can never be a key column, so key and value
            // types are never keys. Doris also rejects `STRING` as the key type of a `MAP` — the
            // docs list `STRING`/`VARIANT`/complex types as invalid map keys — so a `VARCHAR` key
            // maps to `VARCHAR(65533)` (as in key columns) rather than the unbounded `STRING`.
            DataType::Map(map_type) => Ok(format!(
                "MAP<{},{}>",
                Self::get_doris_type_string(map_type.key(), true, timestamptz_as_datetime)?,
                Self::get_doris_type_string(map_type.value(), false, timestamptz_as_datetime)?
            )),
            // A `Vector` becomes Doris `ARRAY<FLOAT>`: RisingWave's `Vector` is a fixed-length
            // `Float32` array, which is exactly what Doris's `ARRAY<FLOAT>` holds (Doris has no
            // separate `VECTOR` column type). Doris's ANN vector-search index is a secondary index
            // on such an `ARRAY<FLOAT>` column and its `dim` is data-dependent, so it is left to
            // manual DDL rather than auto-created here.
            DataType::Vector(_) => Ok("ARRAY<FLOAT>".to_owned()),
        }
    }

    /// Whether a `RisingWave` type maps to a Doris type that is allowed as a key column. Doris
    /// forbids `FLOAT`/`DOUBLE` and complex types (`JSON`, `ARRAY`, ...) as key columns, and
    /// `STRING`, which is why a `Varchar` key is emitted as `VARCHAR`. The fallback types
    /// (`Time`, `Interval`, `Bytea`, `Int256`) map to `Varchar` via the fallback in
    /// [`Self::get_doris_type_string`], so they are key-able too (their text form is short enough
    /// for the `VARCHAR(65533)` cap). `Serial` maps to `BIGINT`, so it is key-able like any integer.
    fn is_doris_key_type(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                // `Serial` maps to Doris `BIGINT`, so it is key-able like any integer.
                | DataType::Serial
                | DataType::Decimal
                | DataType::Date
                | DataType::Timestamp
                | DataType::Varchar
                // Fallback-to-text types. Their auto-created columns are `VARCHAR`, which Doris
                // allows as keys (unlike `STRING`).
                | DataType::Time
                | DataType::Interval
                | DataType::Bytea
                | DataType::Int256
        )
    }

    /// Whether a `RisingWave` type maps to a Doris column type that auto-RANGE partitioning
    /// accepts. Doris auto-RANGE partitions on a `date_trunc` of a `DATE` or `DATETIME` column;
    /// `TIMESTAMPTZ` is not accepted even though manual RANGE allows it. A `Timestamptz` only
    /// auto-creates as `DATETIME` (under `timestamptz_as_datetime`, a Doris 3 target), so it is
    /// range-partitionable exactly then.
    fn is_range_partitionable(data_type: &DataType, timestamptz_as_datetime: bool) -> bool {
        matches!(data_type, DataType::Date | DataType::Timestamp)
            || (*data_type == DataType::Timestamptz && timestamptz_as_datetime)
    }

    /// Whether a `RisingWave` type maps to a Doris column type that auto-LIST partitioning
    /// accepts. Doris auto-LIST accepts `BOOLEAN`, the integer types up to `LARGEINT`,
    /// `DATE`/`DATETIME`/`TIMESTAMPTZ`, and `CHAR`/`VARCHAR`.
    fn is_list_partitionable(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Date
                | DataType::Timestamp
                | DataType::Timestamptz
                | DataType::Varchar
        )
    }

    /// Quote an identifier for Doris DDL: wrap in backticks and escape embedded backticks by
    /// doubling them, so a column/database/table name containing a backtick can't produce
    /// malformed DDL.
    fn quote_ident(name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    /// Render the auto-partitioning clause for `doris.partition_by`, if set, validating that
    /// each partition column exists in the sink schema, is a Doris key column (Doris requires
    /// partition columns to be key columns), and has a type the chosen partition kind accepts.
    ///
    /// Returns `None` when `doris.partition_by` is unset. The tuple is
    /// `(is_list, range_column, clause)`:
    /// - `is_list` is `true` when the spec is a LIST partition: Doris rejects `BUCKETS AUTO` on
    ///   a table with an auto-LIST partition (`Cannot use auto bucket with auto list partition`),
    ///   so the caller must emit a fixed bucket count in that case. Auto-RANGE accepts
    ///   `BUCKETS AUTO`.
    /// - `range_column` is the partition column name for the RANGE form, `None` for LIST. Doris
    ///   rejects an auto-RANGE partition on a nullable column (`AUTO RANGE PARTITION doesn't
    ///   support NULL column`), so the caller must emit that column `NOT NULL`.
    fn build_partition_clause(&self, key_indices: &[usize]) -> Result<Option<(bool, Option<String>, String)>> {
        let Some(partition_by) = &self.config.common.partition_by else {
            return Ok(None);
        };
        let spec = parse_partition_by(partition_by)?;
        let fields = self.schema.fields();

        let columns: Vec<&str> = match &spec {
            DorisPartitionSpec::Range { column, .. } => vec![column.as_str()],
            DorisPartitionSpec::List { columns } => columns.iter().map(String::as_str).collect(),
        };
        let is_range = matches!(&spec, DorisPartitionSpec::Range { .. });

        for column in &columns {
            let idx = fields
                .iter()
                .position(|f| &f.name == column)
                .ok_or_else(|| {
                    SinkError::Doris(format!(
                        "`doris.partition_by` names column `{column}`, which does not exist in \
                         the sink schema"
                    ))
                })?;
            if !key_indices.contains(&idx) {
                return Err(SinkError::Doris(format!(
                    "`doris.partition_by` column `{column}` must be a Doris key column: Doris \
                     requires partition columns to be key columns, so add `{column}` to the \
                     sink's `primary_key`"
                )));
            }
            let data_type = &fields[idx].data_type;
            let type_ok = if is_range {
                Self::is_range_partitionable(data_type, self.config.common.timestamptz_as_datetime)
            } else {
                Self::is_list_partitionable(data_type)
            };
            if !type_ok {
                let allowed = if is_range {
                    "Doris auto-RANGE partitions on a `DATE` or `DATETIME` column (a RisingWave \
                     `Date` or `Timestamp`)"
                } else {
                    "Doris auto-LIST accepts boolean, integer, date/time, or varchar columns"
                };
                return Err(SinkError::Doris(format!(
                    "`doris.partition_by` column `{column}` of type {data_type:?} cannot be a \
                     Doris partition column: {allowed}. Cast the column or choose a different one."
                )));
            }
        }

        let (range_column, clause) = match spec {
            DorisPartitionSpec::Range { column, granularity } => (
                Some(column.clone()),
                format!(
                    "AUTO PARTITION BY RANGE(date_trunc({}, '{}'))()\n",
                    Self::quote_ident(&column),
                    granularity
                ),
            ),
            DorisPartitionSpec::List { columns } => {
                let cols = columns
                    .iter()
                    .map(|c| Self::quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                (None, format!("AUTO PARTITION BY LIST({})()\n", cols))
            }
        };
        Ok(Some((!is_range, range_column, clause)))
    }

    /// Build a `CREATE TABLE` statement for the sink schema. Doris requires key columns to be the
    /// first columns in the table, so key columns are emitted first.
    ///
    /// Note that a `VARCHAR` key column is limited to 65533 bytes by Doris and there is no wider
    /// key type available, so a sink keyed on a string longer than that cannot load into the table
    /// created here. Such a schema needs a manually created table with a different key.
    /// Build the `CREATE TABLE` statement for auto-creation. `alive_be_count` is the number of
    /// alive Doris backends, used to size the bucket count for an auto-LIST partition table (see
    /// the distribution logic below).
    fn build_create_table_sql(&self, alive_be_count: u32) -> Result<String> {
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

        // The auto-RANGE partition column (if any) must be `NOT NULL`: Doris rejects an
        // auto-RANGE partition on a nullable column (`AUTO RANGE PARTITION doesn't support NULL
        // column`). A nullable key column on a UNIQUE KEY table is not a meaningful primary key
        // anyway, so this aligns with how a user would define the table by hand. Auto-LIST
        // partitions have no such requirement.
        let range_partition_column = self
            .build_partition_clause(&key_indices)?
            .and_then(|(_, range_column, _)| range_column);

        let mut columns = Vec::with_capacity(fields.len());
        for &i in &ordered_indices {
            let field = &fields[i];
            let not_null = range_partition_column
                .as_ref()
                .is_some_and(|col| col == &field.name);
            columns.push(format!(
                "{} {}{}",
                Self::quote_ident(&field.name),
                Self::get_doris_type_string(
                    &field.data_type,
                    key_indices.contains(&i),
                    self.config.common.timestamptz_as_datetime,
                )?,
                if not_null { " NOT NULL" } else { "" }
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

        // Auto-partitioning clause, between the key clause and the distribution clause. Doris
        // requires partition columns to be key columns, which `build_partition_clause` enforces.
        // The first tuple element tells us whether the partition is LIST.
        let is_auto_list_partition = match self.build_partition_clause(&key_indices)? {
            Some((is_list, _, partition_clause)) => {
                sql.push_str(&partition_clause);
                is_list
            }
            None => false,
        };

        // Choose the distribution:
        //
        // - An append-only (DUPLICATE KEY) table with no user primary key, or any append-only
        //   auto-LIST table, omits `DISTRIBUTED BY` so Doris auto-generates `RANDOM BUCKETS AUTO`
        //   and auto-sizes the buckets. RANDOM is what the docs recommend when there is no fixed
        //   filter/join column (the no-pk case; the sink picked an arbitrary key-able column purely
        //   to satisfy Doris's key requirement, and hashing on it risks skew). For auto-LIST,
        //   omitting is the only way to get auto-scaled buckets at all: explicit `BUCKETS AUTO`
        //   is rejected (`Cannot use auto bucket with auto list partition` — the docs limit it to
        //   AUTO RANGE), so the append-only LIST case omits too even when a pk is declared.
        // - Every other case declares `DISTRIBUTED BY HASH(...)`. A UNIQUE KEY (upsert) table
        //   requires it (`Create unique keys table should not contain random distribution desc`)
        //   and needs hash co-location for merge-on-write. An append-only table *with* a primary
        //   key (not auto-LIST) keeps `HASH` so the user's key enables bucket pruning for point
        //   queries (the docs recommend HASH when filtering on a specific field). The bucket count
        //   is `AUTO`, except for an upsert auto-LIST table, which rejects `BUCKETS AUTO` and so
        //   uses the alive BE count — matching the Doris sizing rule that the bucket count be an
        //   integer multiple of the BE count, and scaling with the cluster instead of hardcoding.
        let omit_distribution = self.is_append_only
            && (self.pk_indices.is_empty() || is_auto_list_partition);
        if omit_distribution {
            // Omit `DISTRIBUTED BY`; Doris emits `RANDOM BUCKETS AUTO` and auto-sizes the buckets.
        } else {
            let buckets = if is_auto_list_partition {
                alive_be_count.to_string()
            } else {
                "AUTO".to_owned()
            };
            sql.push_str(&format!("DISTRIBUTED BY HASH({}) BUCKETS {buckets}\n", key_list));
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
        let result = self.run_auto_create_ddl(&mut client).await;
        // Close the connection with a proper `COM_QUIT` even when the DDL failed, so a failed
        // `CREATE SINK` doesn't leave a connection sitting on the FE until `wait_timeout` expires.
        let disconnect_result = client.disconnect().await;
        result.and(disconnect_result)
    }

    async fn run_auto_create_ddl(&self, client: &mut DorisDdlClient) -> Result<()> {
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
            // An auto-LIST upsert table sizes its bucket count from the alive BE count.
            let alive_be_count = client.alive_be_count().await?;
            let create_table_sql = self.build_create_table_sql(alive_be_count)?;
            tracing::info!(sql = %create_table_sql, "auto-creating Doris table");
            client.execute_sql(&create_table_sql).await?;
        }

        Ok(())
    }

}

/// Build an `ALTER TABLE ... ADD COLUMN` statement adding the given new columns to the target
/// table. Each `(name, doris_type)` pair becomes one column definition; names are quoted with
/// backticks so a name containing a backtick can't break out of the DDL.
///
/// Doris has no `ADD COLUMN IF NOT EXISTS`. The Doris sink is coordinated, so the coordinator
/// issues exactly one `ALTER` per schema change; if it fails the meta retries, and since an `ALTER`
/// that failed did not add the column, the retried statement is identical and does not duplicate.
fn build_alter_add_column_sql(
    database: &str,
    table: &str,
    columns: &[(String, String)],
) -> String {
    let column_definitions = columns
        .iter()
        .map(|(name, typ)| format!("{} {}", DorisSink::quote_ident(name), typ))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "ALTER TABLE {}.{} ADD COLUMN ({})",
        DorisSink::quote_ident(database),
        DorisSink::quote_ident(table),
        column_definitions
    )
}

/// Build an `ALTER TABLE ... DROP COLUMN` statement dropping the given columns from the target
/// table. Names are quoted with backticks so a name containing a backtick can't break out of the
/// DDL.
fn build_alter_drop_column_sql(database: &str, table: &str, column_names: &[String]) -> String {
    let drop_clauses = column_names
        .iter()
        .map(|name| format!("DROP COLUMN {}", DorisSink::quote_ident(name)))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "ALTER TABLE {}.{} {}",
        DorisSink::quote_ident(database),
        DorisSink::quote_ident(table),
        drop_clauses
    )
}

/// The commit coordinator for the Doris sink. Doris stream loads are per-writer and atomic, so
/// `commit_data` is a no-op (the writer already closed its own load at the barrier); the
/// coordinator exists to apply schema changes (`ALTER TABLE ... ADD/DROP COLUMN`) exactly once, and
/// to give the frontend the `auto.schema.change` gate via [`DorisSink::support_schema_change`].
///
/// A `DropColumns` change can only ever contain non-key columns: RisingWave rejects dropping a
/// primary-key column at the frontend, so a dropped key column never reaches the sink. Doris
/// supports dropping value columns, so the DDL is issued directly.
pub struct DorisSinkCommitCoordinator {
    config: DorisConfig,
}

impl DorisSinkCommitCoordinator {
    pub fn new(config: DorisConfig) -> Self {
        Self { config }
    }

    /// Map an `AddColumns` schema change to `(name, doris_type)` pairs, reusing the same type
    /// mapping as auto-create so a column added here exactly matches what `CREATE TABLE` would
    /// have produced. Any other operation (`DropColumns`, ...) is rejected, matching the
    /// Snowflake/Redshift sinks.
    fn add_columns_from_schema_change(
        schema_change: &risingwave_pb::stream_plan::PbSinkSchemaChange,
        timestamptz_as_datetime: bool,
    ) -> Result<Vec<(String, String)>> {
        use risingwave_pb::stream_plan::sink_schema_change::Op as SinkSchemaChangeOp;
        let schema_change_op = schema_change
            .op
            .as_ref()
            .ok_or_else(|| SinkError::Coordinator(anyhow!("Invalid schema change operation")))?;
        let SinkSchemaChangeOp::AddColumns(add_columns) = schema_change_op else {
            return Err(SinkError::Coordinator(anyhow!(
                "Only AddColumns schema change is supported for Doris sink"
            )));
        };

        let mut columns = Vec::with_capacity(add_columns.fields.len());
        for f in &add_columns.fields {
            let data_type = f.data_type.as_ref().ok_or_else(|| {
                SinkError::Coordinator(anyhow!("Missing data type for column '{}'", f.name))
            })?;
            let rw_type = risingwave_common::types::DataType::from(data_type.clone());
            let doris_type = DorisSink::get_doris_type_string(
                &rw_type,
                false, // new columns are never keys
                timestamptz_as_datetime,
            )?;
            columns.push((f.name.clone(), doris_type));
        }
        Ok(columns)
    }

    /// Execute a DDL statement via the Doris FE `MySQL`-protocol port, closing the connection with
    /// a proper `COM_QUIT` even on failure so a failed schema change doesn't leak a connection.
    ///
    /// The ALTER's own outcome takes precedence: a failed `COM_QUIT` after a successful ALTER must
    /// not report failure, or the coordinator would retry the identical ALTER and Doris would
    /// reject it as a duplicate column, wedging the sink in a permanent retry loop.
    async fn execute_schema_change_ddl(&self, sql: &str) -> Result<()> {
        tracing::info!(sql = %sql, "applying Doris schema change");
        let mut client = self.config.common.build_ddl_client().await?;
        let result = client.execute_sql(sql).await;
        let _ = client.disconnect().await;
        result
    }

    /// Whether a schema-change DDL error means the change was in fact applied by a previous
    /// attempt. The coordinator retries a failed epoch (including its schema change) verbatim, so
    /// a transient error after the ALTER committed would otherwise loop on this error forever.
    fn is_idempotent_schema_change_error(err: &SinkError, is_drop: bool) -> bool {
        let msg = err.to_report_string().to_lowercase();
        if is_drop {
            msg.contains("unknown column")
        } else {
            msg.contains("duplicate column")
        }
    }
}

#[async_trait]
impl SinglePhaseCommitCoordinator for DorisSinkCommitCoordinator {
    async fn init(&mut self) -> Result<()> {
        // Auto-create happens during `validate` (when `auto_create` is set); there is nothing to
        // set up here.
        Ok(())
    }

    async fn commit_data(&mut self, _epoch: u64, _metadata: Vec<SinkMetadata>) -> Result<()> {
        // Each writer commits its own stream load at the barrier; the coordinator has no
        // cross-writer transaction to finalize.
        Ok(())
    }

    async fn commit_schema_change(
        &mut self,
        _epoch: u64,
        schema_change: risingwave_pb::stream_plan::PbSinkSchemaChange,
    ) -> Result<()> {
        use risingwave_pb::stream_plan::sink_schema_change::Op as SinkSchemaChangeOp;
        let schema_change_op = schema_change
            .op
            .as_ref()
            .ok_or_else(|| SinkError::Coordinator(anyhow!("Invalid schema change operation")))?;
        match schema_change_op {
            SinkSchemaChangeOp::AddColumns(_) => {
                let columns = Self::add_columns_from_schema_change(
                    &schema_change,
                    self.config.common.timestamptz_as_datetime,
                )?;
                let sql = build_alter_add_column_sql(
                    &self.config.common.database,
                    &self.config.common.table,
                    &columns,
                );
                match self.execute_schema_change_ddl(&sql).await {
                    // A retry of an already-applied ALTER is not an error: the columns are there.
                    Err(e) if Self::is_idempotent_schema_change_error(&e, false) => Ok(()),
                    other => other,
                }
            }
            SinkSchemaChangeOp::DropColumns(drop_columns) => {
                // RisingWave rejects dropping a primary-key column at the frontend, so the dropped
                // names here are never key columns and Doris supports dropping them. If one ever
                // slipped through, Doris itself rejects dropping a key column, which surfaces as a
                // coordinator error (loud, and the sink retries).
                let sql = build_alter_drop_column_sql(
                    &self.config.common.database,
                    &self.config.common.table,
                    &drop_columns.column_names,
                );
                match self.execute_schema_change_ddl(&sql).await {
                    // A retry of an already-applied drop is not an error: the columns are gone.
                    Err(e) if Self::is_idempotent_schema_change_error(&e, true) => Ok(()),
                    other => other,
                }
            }
        }
    }
}

impl Sink for DorisSink {
    type LogSinker = CoordinatedLogSinker<DorisSinkWriter>;

    const SINK_NAME: &'static str = DORIS_SINK;

    crate::impl_validate_sink_unknown_fields!();

    fn support_schema_change() -> bool {
        true
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        DorisConfig::from_btreemap(config.clone())?;
        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let commit_checkpoint_interval =
            NonZeroU64::new(self.config.commit_checkpoint_interval).expect(
                "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
            );

        let writer = DorisSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?;

        CoordinatedLogSinker::new(
            &writer_param,
            self.param.clone(),
            writer,
            commit_checkpoint_interval,
        )
        .await
    }

    fn is_coordinated_sink(&self) -> bool {
        true
    }

    async fn new_coordinator(
        &self,
        _iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
    ) -> Result<SinkCommitCoordinator> {
        let coordinator = DorisSinkCommitCoordinator::new(self.config.clone());
        Ok(SinkCommitCoordinator::SinglePhase(Box::new(coordinator)))
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

/// Convert RisingWave arrays to Arrow using the default scalar conversions.
struct DorisArrowConvert;

impl Arrow58ToArrow for DorisArrowConvert {}

/// Whether a RisingWave type is stored as text in Doris via the fallback.
fn is_fallback_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        // `Serial` is excluded: it maps to Doris `BIGINT` natively, not to text.
        DataType::Time | DataType::Interval | DataType::Bytea | DataType::Int256
    )
}

/// Stringify a whole column of a fallback type into a `StringArray`, using the same text form the
/// JSON encoder emits (`ToText` for `Time`/`Interval`/`Serial`/`Int256`, base64 for `Bytea`).
fn stringify_fallback_array(
    array: &ArrayImpl,
) -> std::result::Result<arrow_array_58::ArrayRef, ArrayError> {
    use risingwave_common::types::ToText;
    let len = array.len();
    let mut buf = Vec::with_capacity(len);
    for idx in 0..len {
        match array {
            ArrayImpl::Time(a) => match a.value_at(idx) {
                // Same `%H:%M:%S%.6f` form the JSON encoder emits for Doris (it is the string
                // mode now set in `new_with_doris`), so json and arrow loads agree.
                Some(v) => buf.push(Some(v.0.format("%H:%M:%S%.6f").to_string())),
                None => buf.push(None),
            },
            ArrayImpl::Interval(a) => match a.value_at(idx) {
                // Same ISO-8601 form the JSON encoder emits (`as_iso_8601`), so json and arrow
                // loads of the same value agree.
                Some(v) => buf.push(Some(v.as_iso_8601())),
                None => buf.push(None),
            },
            ArrayImpl::Int256(a) => match a.value_at(idx) {
                Some(v) => buf.push(Some(v.to_text())),
                None => buf.push(None),
            },
            ArrayImpl::Bytea(a) => match a.value_at(idx) {
                Some(v) => {
                    use base64::Engine;
                    buf.push(Some(general_purpose::STANDARD.encode(v)))
                }
                None => buf.push(None),
            },
            _ => {
                return Err(ArrayError::internal(
                    "stringify_fallback_array called on a non-fallback array",
                ))
            }
        }
    }
    Ok(Arc::new(arrow_array_58::StringArray::from(buf)))
}

/// Build the Arrow schema for the Arrow load path. Field names must match the Doris column names
/// exactly, because Doris's Arrow reader matches columns by name.
///
/// Fallback types (`Time`, `Interval`, `Bytea`, `Serial`, `Int256`) are stored as text in Doris,
/// so their Arrow field is `Utf8` rather than the native Time64/Interval/Binary/Decimal256.
fn doris_arrow_schema(
    schema: &Schema,
    timestamptz_as_datetime: bool,
) -> arrow_schema_58::Schema {
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|f| {
            if is_fallback_type(&f.data_type) {
                arrow_schema_58::Field::new(&f.name, arrow_schema_58::DataType::Utf8, true)
            } else if timestamptz_as_datetime && f.data_type == DataType::Timestamptz {
                // The column targets a Doris `DATETIME` (tz-naive), so the Arrow field must match
                // the JSON encoder's tz-naive string form, not carry a timezone.
                arrow_schema_58::Field::new(
                    &f.name,
                    arrow_schema_58::DataType::Timestamp(
                        arrow_schema_58::TimeUnit::Microsecond,
                        None,
                    ),
                    true,
                )
            } else {
                DorisArrowConvert
                    .to_arrow_field(&f.name, &f.data_type)
                    .expect("arrow field conversion should not fail")
            }
        })
        .collect();
    arrow_schema_58::Schema::new(fields)
}

/// Append the `__DORIS_DELETE_SIGN__` Int32 field to the arrow schema (upsert path only).
fn arrow_schema_with_delete_sign(schema: &arrow_schema_58::Schema) -> arrow_schema_58::Schema {
    let mut fields: Vec<_> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(arrow_schema_58::Field::new(
        DORIS_DELETE_SIGN,
        arrow_schema_58::DataType::Int32,
        true,
    )));
    arrow_schema_58::Schema::new(fields)
}

pub struct DorisSinkWriter {
    pub config: DorisConfig,
    schema: Schema,
    #[expect(dead_code)]
    pk_indices: Vec<usize>,
    inserter_inner_builder: InserterInnerBuilder,
    is_append_only: bool,
    client: Option<DorisClient>,
    row_encoder: JsonEncoder,
    max_batch_size_bytes: u64,
    current_batch_size_bytes: u64,
    /// The stream-load body format: `"json"` or `"arrow"`.
    format: String,
    /// Arrow schema for the Arrow path, `None` when `format` is `json`.
    arrow_schema: Option<Arc<arrow_schema_58::Schema>>,
    /// Rows accumulated for the next Arrow load.
    arrow_pending: Option<StreamChunkBuilder>,
    /// Estimated heap size of `arrow_pending`, the trigger for splitting a load.
    arrow_pending_size: u64,
}

impl TryFrom<SinkParam> for DorisSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let is_append_only = param.sink_type.is_append_only();
        let config = DorisConfig::from_btreemap(param.properties.clone())?;
        DorisSink::new(config, schema, pk_indices, is_append_only, param)
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
        let mut tstz_target_columns: HashSet<String> = HashSet::default();
        for s in &doris_schema.properties {
            if let Some(v) = s.get_decimal_pre_scale()? {
                decimal_map.insert(s.name.clone(), v);
            }
            if s.is_variant() {
                variant_columns.insert(s.name.clone());
            }
            // Use the same normalization as `check_and_correct_column_type`, so validation and
            // encoding can never disagree about which columns are `TIMESTAMPTZ`.
            if normalize_doris_type(&s.r#type).contains("TIMESTAMPTZ") {
                tstz_target_columns.insert(s.name.clone());
            }
        }

        let header = Self::build_stream_load_header(&config, &schema, is_append_only);

        // Pick the stream-load base. `doris.stream_load_url` points straight at a BE, so there is
        // no FE → BE 307 to chase and the redirect probe is skipped: a BE answers a bodyless probe
        // by running an empty stream load, which would make every commit cost two Doris
        // transactions instead of one. Without the option we go to the FE on `doris.url` and let
        // `InserterInnerBuilder::build()` probe for the redirect, which costs the FE nothing.
        let stream_load_base = config
            .common
            .stream_load_url
            .clone()
            .unwrap_or_else(|| config.common.url.clone());
        let doris_insert_builder = InserterInnerBuilder::new(
            stream_load_base,
            config.common.database.clone(),
            config.common.table.clone(),
            header,
            config.stream_load_http_timeout_ms,
            config.common.stream_load_url.is_some(),
        )?;
        let max_batch_size_bytes = config.max_batch_size_bytes;
        let format = config.format.clone();
        let is_arrow = format == DORIS_FORMAT_ARROW;
        let arrow_schema = is_arrow.then(|| {
            Arc::new(doris_arrow_schema(
                &schema,
                config.common.timestamptz_as_datetime,
            ))
        });
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
                    tstz_target_columns,
                },
            ),
            max_batch_size_bytes,
            current_batch_size_bytes: 0,
            format,
            arrow_schema,
            arrow_pending: None,
            arrow_pending_size: 0,
        })
    }

    /// Build the stream load request headers.
    fn build_stream_load_header(
        config: &DorisConfig,
        schema: &Schema,
        is_append_only: bool,
    ) -> HashMap<String, String> {
        // The `columns` header tells Doris which columns this load supplies. Partial updates
        // require it, and the list must name every key column. Names are quoted `MySQL`-style so a
        // column named after a reserved word (`order`, `from`, ...) is accepted, and embedded
        // backticks are doubled so a name like ``we`ird`` can't break out of the quoting and make
        // Doris reject every load.
        let mut field_names = schema.names_str();
        if !is_append_only {
            field_names.push(DORIS_DELETE_SIGN);
        }
        let field_names = field_names
            .into_iter()
            .map(DorisSink::quote_ident)
            .collect::<Vec<String>>();
        let field_names_str = field_names
            .iter()
            .map(|name| name.as_str())
            .collect::<Vec<&str>>();

        let mut header_builder = HeaderBuilder::new()
            .add_common_header()
            .set_user_password(config.common.user.clone(), config.common.password.clone())
            .set_partial_columns(config.common.partial_update.clone())
            .set_strict_mode(config.strict_mode)
            .set_columns_name(field_names_str);
        if config.format == DORIS_FORMAT_ARROW {
            header_builder = header_builder.add_arrow_format();
        } else {
            header_builder = header_builder.add_json_format().add_read_json_by_line();
        }
        if !is_append_only {
            // Upsert declares the delete sign in `hidden_columns` as well as in `columns`. Doris
            // only reads `hidden_columns` when `columns` is absent, so this is usually redundant.
            header_builder.add_hidden_column().build()
        } else {
            header_builder.build()
        }
    }

    /// Commit the in-flight stream load, if there is one.
    async fn finish_load_request(&mut self) -> Result<()> {
        if let Some(client) = self.client.take() {
            client.finish().await?;
            self.current_batch_size_bytes = 0;
        }
        Ok(())
    }

    /// Open a stream load if none is in flight. Called per row, so a chunk that yields no written
    /// rows never opens (and immediately commits) an empty load.
    async fn ensure_load_request(&mut self) -> Result<()> {
        if self.client.is_none() {
            self.client = Some(DorisClient::new(self.inserter_inner_builder.build().await?));
            self.current_batch_size_bytes = 0;
        }
        Ok(())
    }

    async fn write_row_json(&mut self, row_json_string: String) -> Result<()> {
        // Add the newline separator `DorisClient::write` puts between rows. Counting it for the
        // first row of a load too overestimates by one byte, which can only close a load one row
        // early and never lets the payload exceed the cap.
        let row_size = row_json_string.len() as u64 + 1;
        let size_decision = decide_load_request_size(
            self.current_batch_size_bytes,
            row_size,
            self.max_batch_size_bytes,
        );
        if size_decision.finish_current_load {
            self.finish_load_request().await?;
        }
        self.ensure_load_request().await?;
        self.client
            .as_mut()
            .ok_or_else(|| SinkError::Doris("Can't find doris sink insert".to_owned()))?
            .write(row_json_string.into())
            .await?;
        self.current_batch_size_bytes = size_decision.next_batch_size_bytes;
        Ok(())
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.format == DORIS_FORMAT_ARROW {
            return self.accumulate_arrow(chunk, |op| op == Op::Insert).await;
        }
        for (op, row) in chunk.rows() {
            // `force_append_only` is applied upstream in `src/stream/src/executor/sink.rs`, so an
            // append-only sink should only ever see inserts. Assert that in debug builds; a
            // release-build panic here would kill the actor and turn a stray op into a crash loop.
            debug_assert!(op == Op::Insert, "append-only doris sink got op {:?}", op);
            if op != Op::Insert {
                continue;
            }
            let row_json_string = Value::Object(self.row_encoder.encode(row)?).to_string();
            self.write_row_json(row_json_string).await?;
        }
        Ok(())
    }

    async fn upsert(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.format == DORIS_FORMAT_ARROW {
            return self
                .accumulate_arrow(chunk, |op| {
                    matches!(op, Op::Insert | Op::UpdateInsert | Op::Delete)
                })
                .await;
        }
        for (op, row) in chunk.rows() {
            match op {
                Op::Insert => {
                    let mut row_json_value = self.row_encoder.encode(row)?;
                    row_json_value
                        .insert(DORIS_DELETE_SIGN.to_owned(), Value::String("0".to_owned()));
                    let row_json_string = serde_json::to_string(&row_json_value).map_err(|e| {
                        SinkError::Doris(format!("Json derialize error: {}", e.as_report()))
                    })?;
                    self.write_row_json(row_json_string).await?;
                }
                Op::Delete => {
                    let mut row_json_value = self.row_encoder.encode(row)?;
                    row_json_value
                        .insert(DORIS_DELETE_SIGN.to_owned(), Value::String("1".to_owned()));
                    let row_json_string = serde_json::to_string(&row_json_value).map_err(|e| {
                        SinkError::Doris(format!("Json derialize error: {}", e.as_report()))
                    })?;
                    self.write_row_json(row_json_string).await?;
                }
                Op::UpdateDelete => {}
                Op::UpdateInsert => {
                    let mut row_json_value = self.row_encoder.encode(row)?;
                    row_json_value
                        .insert(DORIS_DELETE_SIGN.to_owned(), Value::String("0".to_owned()));
                    let row_json_string = serde_json::to_string(&row_json_value).map_err(|e| {
                        SinkError::Doris(format!("Json derialize error: {}", e.as_report()))
                    })?;
                    self.write_row_json(row_json_string).await?;
                }
            }
        }
        Ok(())
    }

    /// Arrow write path. Rows whose op passes `accept` accumulate in a chunk builder; when the
    /// builder reaches `max_batch_size_bytes` they become one `RecordBatch` and one committed
    /// stream load. Upsert appends a `__DORIS_DELETE_SIGN__` column derived from the ops.
    async fn accumulate_arrow(
        &mut self,
        chunk: StreamChunk,
        accept: impl Fn(Op) -> bool,
    ) -> Result<()> {
        let data_types = self.schema.data_types();
        let mut pending = self.arrow_pending.take().unwrap_or_else(|| {
            StreamChunkBuilder::unlimited(data_types, Some(chunk.cardinality()))
        });
        self.arrow_pending_size += chunk.estimated_heap_size() as u64;

        for (op, row) in chunk.rows() {
            if !accept(op) {
                continue;
            }
            let full = pending.append_row(op, row);
            debug_assert!(full.is_none(), "unlimited chunk builder should not fill");
        }

        if self.arrow_pending_size >= self.max_batch_size_bytes {
            if let Some(accumulated) = pending.take() {
                self.write_arrow_load(accumulated).await?;
            }
            self.arrow_pending = None;
            self.arrow_pending_size = 0;
        } else {
            self.arrow_pending = Some(pending);
        }
        Ok(())
    }

    /// Encode one accumulated batch as an Arrow IPC stream and send it as one stream load with a
    /// known `Content-Length`. `chunk` holds only the rows `accumulate_arrow` accepted; for upsert
    /// the per-row `__DORIS_DELETE_SIGN__` column is derived from the ops and appended. The batch
    /// size is already capped by the caller.
    async fn write_arrow_load(&mut self, chunk: StreamChunk) -> Result<()> {
        let schema = self
            .arrow_schema
            .clone()
            .ok_or_else(|| SinkError::Doris("arrow schema is None".to_owned()))?;
        let chunk = chunk.compact_vis();
        if chunk.cardinality() == 0 {
            return Ok(());
        }
        // Build the Arrow columns manually. `to_record_batch` would try to `arrow_cast` a
        // fallback column to `Utf8` (its schema type), which Arrow cannot do for
        // Time64/Interval/Binary/Decimal256, so stringify fallback columns up front instead.
        // Route by the *array* type, not the field type: `Decimal`/`Jsonb` also have a `Utf8`
        // arrow field (an `ARROW:extension:name` extension), but `to_array` stringifies them
        // correctly, whereas `stringify_fallback_array` rejects them.
        let mut columns = Vec::with_capacity(chunk.columns().len());
        for (col, field) in chunk.columns().iter().zip(schema.fields().iter()) {
            let is_fallback_array = matches!(
                col.as_ref(),
                ArrayImpl::Time(_)
                    | ArrayImpl::Interval(_)
                    | ArrayImpl::Bytea(_)
                    | ArrayImpl::Int256(_)
            );
            if is_fallback_array {
                columns.push(
                    stringify_fallback_array(col.as_ref())
                        .map_err(|e| SinkError::Doris(format!("arrow encode error: {}", e.as_report())))?,
                );
            } else if let (
                ArrayImpl::Timestamptz(arr),
                arrow_schema_58::DataType::Timestamp(arrow_schema_58::TimeUnit::Microsecond, None),
            ) = (col.as_ref(), field.data_type())
            {
                // `timestamptz_as_datetime` targets a tz-naive Doris `DATETIME`: the Arrow field
                // (from `doris_arrow_schema`) is naive, so the array must be naive too, matching
                // the JSON encoder's tz-naive string form.
                columns.push(Arc::new(arrow_array_58::TimestampMicrosecondArray::from(arr)));
            } else {
                columns.push(
                    DorisArrowConvert
                        .to_array(field.data_type(), col.as_ref())
                        .map_err(|e| SinkError::Doris(format!("arrow encode error: {}", e.as_report())))?,
                );
            }
        }
        let mut batch =
            RecordBatch::try_new(schema.clone(), columns)
                .map_err(|e| SinkError::Doris(format!("arrow schema error: {}", e.as_report())))?;
        if !self.is_append_only {
            // `__DORIS_DELETE_SIGN__` drives the upsert delete in Doris; 1 deletes the row, 0
            // inserts/updates it. The sink schema never contains it, so it is appended here and the
            // schema gains a matching trailing field (the `columns` header already names it).
            let delete_signs: Int32Array = chunk
                .ops()
                .iter()
                .map(|op| match op {
                    Op::Insert | Op::UpdateInsert => 0,
                    Op::Delete => 1,
                    Op::UpdateDelete => 0,
                })
                .collect();
            let mut columns = batch.columns().to_vec();
            columns.push(Arc::new(delete_signs));
            let full_schema = arrow_schema_with_delete_sign(schema.as_ref());
            batch = RecordBatch::try_new(Arc::new(full_schema), columns).map_err(|e| {
                SinkError::Doris(format!("arrow schema error: {}", e.as_report()))
            })?;
        }
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref()).map_err(|e| {
                SinkError::Doris(format!("arrow writer error: {}", e.as_report()))
            })?;
            writer
                .write(&batch)
                .map_err(|e| SinkError::Doris(format!("arrow write error: {}", e.as_report())))?;
            writer
                .finish()
                .map_err(|e| SinkError::Doris(format!("arrow writer error: {}", e.as_report())))?;
        }
        self.inserter_inner_builder.send_body(buf.into()).await?;
        Ok(())
    }
}

impl Drop for DorisSinkWriter {
    fn drop(&mut self) {
        // There is no transaction to roll back — a Doris stream load commits when its body closes.
        // Dropping the writer drops the inserter, which aborts the request mid-body so Doris fails
        // the load instead of committing it (see `impl Drop for InserterInner`), but a load already
        // committed on the Doris side still lands. The epoch is replayed either way. With
        // `commit_checkpoint_interval` above 1 the abandoned load can span several checkpoints, so
        // log it: this line is what lets an operator explain a burst of duplicate rows instead of
        // having to infer it.
        if self.client.is_some() {
            tracing::warn!(
                current_batch_size_bytes = self.current_batch_size_bytes,
                "doris sink writer dropped with a stream load still in flight; the load is \
                 abandoned and the affected epochs will be replayed, which may duplicate rows on \
                 an append-only table"
            );
        }
    }
}

#[async_trait]
impl SinkWriter for DorisSinkWriter {
    type CommitMetadata = Option<SinkMetadata>;

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        // The stream load is opened lazily per row by `ensure_load_request`, so a chunk with no
        // rows to write doesn't open one.
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

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        // Only commit on the checkpoint barriers the log sinker selects, i.e. one in every
        // `commit_checkpoint_interval`. Committing on every barrier would defeat the interval
        // entirely, because for Doris each committed load is a new table version.
        //
        // Holding one request open across barriers is safe: `begin_epoch`/`abort` are no-ops and
        // the newline-separator state lives on `DorisClient`, which survives alongside the request.
        if is_checkpoint {
            if let Some(accumulated) = self.arrow_pending.take().and_then(|mut p| p.take()) {
                self.write_arrow_load(accumulated).await?;
            }
            self.arrow_pending = None;
            self.arrow_pending_size = 0;
            self.finish_load_request().await?;
        } else {
            // Non-checkpoint barriers carry no commit; reporting metadata for them only makes
            // `CoordinatedLogSinker` log a spurious warning on every such barrier.
            return Ok(None);
        }
        // Doris commits are per-writer atomic stream loads, so there is no global metadata to
        // report to the coordinator; it only needs to know the writer committed.
        Ok(Some(SinkMetadata {
            metadata: Some(sink_metadata::Metadata::Serialized(
                risingwave_pb::connector_service::sink_metadata::SerializedMetadata {
                    metadata: vec![],
                },
            )),
        }))
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

/// A `MySQL`-protocol client against the Doris FE query endpoint, used to issue DDL (auto-create).
/// Doris FE is `MySQL`-compatible. The connection `Opts` are built via [`build_ddl_opts`]
/// from the full `mysql://` query URL with credentials overridden from sink properties.
pub struct DorisDdlClient {
    conn: mysql_async::Conn,
}

/// Build `mysql_async::Opts` from a complete `mysql://` query URL, overriding credentials and
/// enforcing the connection parameters needed for Doris DDL.
fn build_ddl_opts(query_url: &str, user: &str, password: &str) -> Result<Opts> {
    // Validate the scheme first for a clear error message.
    let parsed = Url::parse(query_url).map_err(|e| {
        SinkError::DorisStarrocksConnect(anyhow!("Invalid doris.query_url '{}': {}", query_url, e))
    })?;
    if parsed.scheme() != "mysql" {
        return Err(SinkError::DorisStarrocksConnect(anyhow!(
            "doris.query_url scheme must be 'mysql', got '{}'",
            parsed.scheme()
        )));
    }
    if parsed.port().is_none() {
        return Err(SinkError::DorisStarrocksConnect(anyhow!(
            "doris.query_url must include an explicit port"
        )));
    }
    // Parse into Opts (preserves host/port and any URL parameters from the user).
    let base = Opts::from_url(query_url).map_err(|e| {
        SinkError::DorisStarrocksConnect(anyhow!("Invalid doris.query_url '{}': {}", query_url, e))
    })?;
    // Convert to builder, override credentials with sink config, clear database,
    // and enforce the connection parameters needed for DDL.
    let builder = OptsBuilder::from_opts(base)
        .user(Some(user.to_owned()))
        .pass(Some(password.to_owned()))
        .db_name(None::<&str>)
        .prefer_socket(false)
        .max_allowed_packet(Some(DORIS_MYSQL_MAX_ALLOWED_PACKET))
        .wait_timeout(Some(DORIS_MYSQL_WAIT_TIMEOUT));
    Ok(Opts::from(builder))
}

impl DorisDdlClient {
    /// Create a new DDL client from pre-built `Opts`. Auto-create issues a handful of statements
    /// once, so this opens a single connection directly rather than standing up a pool.
    pub async fn new(opts: Opts) -> Result<Self> {
        let conn = mysql_async::Conn::new(opts)
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;
        Ok(Self { conn })
    }

    /// Close the connection with a proper `COM_QUIT`. Without this the FE keeps the connection
    /// until it hits the `wait_timeout` this client sets ([`DORIS_MYSQL_WAIT_TIMEOUT`], 8 hours).
    pub async fn disconnect(self) -> Result<()> {
        self.conn
            .disconnect()
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;
        Ok(())
    }

    /// The names are bound as parameters rather than formatted into the statement, so a name
    /// containing a quote cannot terminate the literal and run as SQL.
    pub async fn database_exists(&mut self, db: &str) -> Result<bool> {
        let count: u64 = self
            .conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?",
                (db,),
            )
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?
            .unwrap_or(0);
        Ok(count > 0)
    }

    pub async fn table_exists(&mut self, db: &str, table: &str) -> Result<bool> {
        let count: u64 = self
            .conn
            .exec_first(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ? AND table_schema = ?",
                (table, db),
            )
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

    /// Count the number of alive Doris backends via `SHOW BACKENDS`. Used to size the bucket
    /// count of an auto-created auto-LIST partition table: `BUCKETS AUTO` is rejected for
    /// auto-LIST (`Cannot use auto bucket with auto list partition`), and the Doris sizing rule
    /// is that the bucket count should be an integer multiple of the BE count. Querying the
    /// actual cluster makes the created table scale with the cluster instead of hardcoding.
    ///
    /// Returns at least 1: a `BUCKETS 0` DDL would be rejected by Doris, and auto-create only
    /// runs when a table is missing, so a cluster with zero alive BEs cannot create the table
    /// anyway.
    pub async fn alive_be_count(&mut self) -> Result<u32> {
        let rows: Vec<mysql_async::Row> = self
            .conn
            .query("SHOW BACKENDS")
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(anyhow!(err)))?;
        // `SHOW BACKENDS` lists every registered backend, dead ones included, with the `Alive`
        // column distinguishing them, so count the rows whose `Alive` column is true instead of
        // trusting the row count.
        let alive_col = rows
            .first()
            .and_then(|row| {
                row.columns()
                    .iter()
                    .position(|col| col.name_str().eq_ignore_ascii_case("alive"))
            })
            .unwrap_or(0);
        let alive = rows
            .iter()
            .filter(|row| {
                matches!(
                    row.get::<mysql_async::Value, usize>(alive_col),
                    Some(mysql_async::Value::Bytes(b)) if b.eq_ignore_ascii_case(b"true")
                )
            })
            .count() as u32;
        Ok(alive.max(1))
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

    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::id::SinkId;
    use risingwave_common::types::{DataType, ListType, MapType, StructType};

    use super::{
        DorisArrowConvert, DorisConfig, DorisField, DorisInsertResultResponse, DorisPartitionSpec,
        DorisSink, DorisSinkCommitCoordinator, DorisSinkWriter, LoadRequestSizeDecision,
        build_alter_add_column_sql, build_alter_drop_column_sql, decide_load_request_size,
        doris_arrow_schema, is_fallback_type, normalize_doris_type, parse_partition_by,
        stringify_fallback_array,
    };
    use risingwave_common::array::arrow::{Arrow58ToArrow, arrow_schema_58};
    use risingwave_common::array::{Op, StreamChunkBuilder};
    use std::sync::Arc;
    use crate::sink::catalog::SinkType;
    use crate::sink::{SINK_TYPE_APPEND_ONLY, SINK_TYPE_UPSERT, Sink, SinkParam};

    #[test]
    fn test_jsonb_can_write_to_variant() {
        assert!(
            DorisSink::check_and_correct_column_type(&DataType::Jsonb, "VARIANT".into(), false)
                .unwrap()
        );
    }

    #[test]
    fn test_varchar_can_write_to_variant() {
        assert!(
            DorisSink::check_and_correct_column_type(&DataType::Varchar, "VARIANT".into(), false)
                .unwrap()
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

    /// A `SinkParam` for tests, with the same schema as `upsert_schema()`.
    fn test_param(r#type: &str, is_append_only: bool) -> SinkParam {
        let schema = upsert_schema();
        SinkParam {
            sink_id: SinkId::from(1u32),
            sink_name: "test_sink".to_owned(),
            properties: base_properties(r#type),
            columns: schema
                .fields
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    ColumnDesc::named(&f.name, ColumnId::new(i as i32 + 1), f.data_type.clone())
                })
                .collect(),
            downstream_pk: Some(vec![0]),
            sink_type: if is_append_only {
                SinkType::AppendOnly
            } else {
                SinkType::Upsert
            },
            ignore_delete: false,
            format_desc: None,
            db_name: "demo_db".to_owned(),
            sink_from_name: "test_sink".to_owned(),
        }
    }

    fn build_sink(r#type: &str, is_append_only: bool) -> DorisSink {
        let schema = upsert_schema();
        let config = DorisConfig::from_btreemap(base_properties(r#type)).unwrap();
        DorisSink::new(
            config,
            schema,
            vec![0],
            is_append_only,
            test_param(r#type, is_append_only),
        )
        .unwrap()
    }

    #[test]
    fn test_build_create_table_sql_upsert_puts_key_first_and_merge_on_write() {
        let sink = build_sink("upsert", false);
        let sql = sink.build_create_table_sql(1).unwrap();
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
        let sql = sink.build_create_table_sql(1).unwrap();
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
        let sink = DorisSink::new(
            config,
            schema,
            vec![],
            true,
            test_param(SINK_TYPE_APPEND_ONLY, true),
        )
        .unwrap();
        let sql = sink.build_create_table_sql(1).unwrap();
        assert!(sql.contains("DUPLICATE KEY(`id`)"), "sql: {sql}");
        // No user-defined key: omit `DISTRIBUTED BY` so Doris auto-generates `RANDOM BUCKETS AUTO`
        // rather than hashing on the arbitrarily picked key column (which could skew badly for a
        // low-cardinality column).
        assert!(
            !sql.contains("DISTRIBUTED BY"),
            "append-only table without a pk omits DISTRIBUTED BY, sql: {sql}"
        );
    }

    #[test]
    fn test_build_create_table_sql_append_only_no_key_able_column_errors() {
        // No primary key and no key-able column: auto-create cannot pick a valid key, so it must
        // fail with a clear error instead of producing invalid DDL.
        let config = DorisConfig::from_btreemap(base_properties(SINK_TYPE_APPEND_ONLY)).unwrap();
        let schema = Schema::new(vec![Field::with_name(DataType::Float64, "score")]);
        let sink = DorisSink::new(
            config,
            schema,
            vec![],
            true,
            test_param(SINK_TYPE_APPEND_ONLY, true),
        )
        .unwrap();
        assert!(sink.build_create_table_sql(1).is_err());
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
        let sink = DorisSink::new(config, schema, vec![0], false, test_param("upsert", false)).unwrap();
        assert!(sink.build_create_table_sql(1).is_err());
    }

    #[test]
    fn test_build_create_table_sql_includes_struct_columns() {
        // A `STRUCT` column must be emitted as Doris `STRUCT<name:type, ...>` so auto-create can
        // build a table the sink can then write to. This was previously rejected outright.
        let config = DorisConfig::from_btreemap(base_properties(SINK_TYPE_APPEND_ONLY)).unwrap();
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int64, "id"),
            Field::with_name(
                DataType::Struct(StructType::new([
                    ("x", DataType::Int32),
                    ("y", DataType::Varchar),
                ])),
                "s",
            ),
        ]);
        let sink = DorisSink::new(
            config,
            schema,
            vec![0],
            true,
            test_param(SINK_TYPE_APPEND_ONLY, true),
        )
        .unwrap();
        let sql = sink.build_create_table_sql(1).unwrap();
        assert!(sql.contains("`s` STRUCT<x:INT,y:STRING>"), "sql: {sql}");
    }

    #[test]
    fn test_build_create_table_sql_includes_map_columns() {
        // A `MAP` column must be emitted as Doris `MAP<K,V>` so auto-create can build a table the
        // sink can then write to. This was previously rejected outright.
        let config = DorisConfig::from_btreemap(base_properties(SINK_TYPE_APPEND_ONLY)).unwrap();
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int64, "id"),
            Field::with_name(
                DataType::Map(MapType::from_kv(DataType::Int32, DataType::Varchar)),
                "m",
            ),
        ]);
        let sink = DorisSink::new(
            config,
            schema,
            vec![0],
            true,
            test_param(SINK_TYPE_APPEND_ONLY, true),
        )
        .unwrap();
        let sql = sink.build_create_table_sql(1).unwrap();
        assert!(sql.contains("`m` MAP<INT,STRING>"), "sql: {sql}");
    }

    #[test]
    fn test_build_create_table_sql_escapes_backtick_in_identifier() {
        // A column name containing a backtick must be escaped (doubled) so the generated DDL stays
        // well-formed rather than breaking out of the backtick-quoted identifier.
        let config = DorisConfig::from_btreemap(base_properties(SINK_TYPE_APPEND_ONLY)).unwrap();
        let schema = Schema::new(vec![Field::with_name(DataType::Int64, "we`ird")]);
        let sink = DorisSink::new(
            config,
            schema,
            vec![0],
            true,
            test_param(SINK_TYPE_APPEND_ONLY, true),
        )
        .unwrap();
        let sql = sink.build_create_table_sql(1).unwrap();
        assert!(sql.contains("`we``ird`"), "sql: {sql}");
    }

    /// Build a sink whose config has `doris.partition_by` set (and `auto_create` on), with `id`
    /// as the first column, the given extra fields appended, and the given primary-key indices.
    fn partition_sink(
        r#type: &str,
        is_append_only: bool,
        partition_by: &str,
        pk_indices: Vec<usize>,
        extra_fields: Vec<Field>,
    ) -> DorisSink {
        let mut properties = base_properties(r#type);
        properties.insert("auto_create".to_owned(), "true".to_owned());
        properties.insert("doris.partition_by".to_owned(), partition_by.to_owned());
        let config = DorisConfig::from_btreemap(properties).unwrap();
        let mut fields = vec![Field::with_name(DataType::Int64, "id")];
        fields.extend(extra_fields);
        DorisSink::new(
            config,
            Schema::new(fields),
            pk_indices,
            is_append_only,
            test_param(r#type, is_append_only),
        )
        .unwrap()
    }

    #[test]
    fn test_parse_partition_by_fn_form_is_range() {
        assert_eq!(
            parse_partition_by("month(ts)").unwrap(),
            DorisPartitionSpec::Range {
                column: "ts".to_owned(),
                granularity: "month",
            }
        );
        // Whitespace inside the parens is tolerated.
        assert_eq!(
            parse_partition_by("day( ts )").unwrap(),
            DorisPartitionSpec::Range {
                column: "ts".to_owned(),
                granularity: "day",
            }
        );
    }

    #[test]
    fn test_parse_partition_by_bare_form_is_list() {
        assert_eq!(
            parse_partition_by("id").unwrap(),
            DorisPartitionSpec::List {
                columns: vec!["id".to_owned()]
            }
        );
        // Comma-separated columns are a multi-column LIST partition.
        assert_eq!(
            parse_partition_by("id, city").unwrap(),
            DorisPartitionSpec::List {
                columns: vec!["id".to_owned(), "city".to_owned()]
            }
        );
        // Surrounding whitespace is tolerated.
        assert_eq!(
            parse_partition_by(" id ,  city ").unwrap(),
            DorisPartitionSpec::List {
                columns: vec!["id".to_owned(), "city".to_owned()]
            }
        );
    }

    #[test]
    fn test_parse_partition_by_rejects_invalid_forms() {
        for (value, needle) in [
            ("", "must not be empty"),
            ("montsh(ts)", "granularity must be one of"),
            // Mixed function and bare forms, or more than one function: Doris auto-RANGE is a
            // single `date_trunc` expression, so these have no valid reading.
            ("month(a), b", "Invalid `doris.partition_by`"),
            ("month(a), day(b)", "Invalid `doris.partition_by`"),
            // Nested argument and non-identifier characters are rejected rather than emitted
            // into the DDL.
            ("month(a, b)", "only plain"),
            ("month(`ts`)", "only plain"),
            ("a,b,", "expected bare"),
        ] {
            let err = parse_partition_by(value).unwrap_err();
            let msg = format!("{}", err);
            assert!(msg.contains(needle), "value {value:?}: expected error containing {needle:?}, got: {msg}");
        }
    }

    #[test]
    fn test_partition_by_requires_auto_create() {
        let mut properties = base_properties(SINK_TYPE_APPEND_ONLY);
        properties.insert("doris.partition_by".to_owned(), "id".to_owned());
        let err = DorisConfig::from_btreemap(properties).unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("only takes effect when `auto_create` is enabled"),
            "got: {msg}"
        );
    }

    #[test]
    fn test_partition_by_parsed_at_config_time() {
        // A grammar error surfaces at config time (before any Doris connection), so a bad value
        // fails `from_btreemap` even with `auto_create` on.
        let mut properties = base_properties(SINK_TYPE_APPEND_ONLY);
        properties.insert("auto_create".to_owned(), "true".to_owned());
        properties.insert("doris.partition_by".to_owned(), "nope(a)".to_owned());
        assert!(DorisConfig::from_btreemap(properties).is_err());
    }

    #[test]
    fn test_create_table_sql_range_partition() {
        // Both `id` and `ts` are keys so the DDL is valid; the partition clause references `ts`.
        let sink = partition_sink(
            SINK_TYPE_APPEND_ONLY,
            true,
            "month(ts)",
            vec![0, 1],
            vec![Field::with_name(DataType::Timestamp, "ts")],
        );
        let sql = sink.build_create_table_sql(1).unwrap();
        assert!(
            sql.contains("AUTO PARTITION BY RANGE(date_trunc(`ts`, 'month'))()"),
            "sql: {sql}"
        );
    }

    #[test]
    fn test_create_table_sql_list_partition_multi_column() {
        let sink = partition_sink(
            SINK_TYPE_APPEND_ONLY,
            true,
            "id, city",
            vec![0, 1],
            vec![Field::with_name(DataType::Varchar, "city")],
        );
        let sql = sink.build_create_table_sql(1).unwrap();
        assert!(
            sql.contains("AUTO PARTITION BY LIST(`id`, `city`)()"),
            "sql: {sql}"
        );
        // An append-only (DUPLICATE KEY) auto-LIST table omits `DISTRIBUTED BY` entirely so
        // Doris auto-generates `RANDOM BUCKETS AUTO` and auto-sizes the buckets (the docs
        // restrict explicit `BUCKETS AUTO` to AUTO RANGE only).
        assert!(
            !sql.contains("DISTRIBUTED BY"),
            "append-only auto-LIST partition omits DISTRIBUTED BY, sql: {sql}"
        );
    }

    #[test]
    fn test_create_table_sql_upsert_list_partition_uses_be_count_buckets() {
        let sink = partition_sink(
            SINK_TYPE_UPSERT,
            false,
            "id, city",
            vec![0, 1],
            vec![Field::with_name(DataType::Varchar, "city")],
        );
        // A UNIQUE KEY (upsert) table cannot use `RANDOM` distribution, so it declares
        // `HASH ... BUCKETS <alive BE count>`, sized from the cluster so it scales with it.
        let sql = sink.build_create_table_sql(3).unwrap();
        assert!(
            sql.contains("DISTRIBUTED BY HASH(`id`, `city`) BUCKETS 3"),
            "upsert auto-LIST partition sizes buckets from the BE count, sql: {sql}"
        );
        assert!(
            !sql.contains("BUCKETS AUTO"),
            "upsert auto-LIST partition must not use BUCKETS AUTO, sql: {sql}"
        );
    }

    #[test]
    fn test_create_table_sql_range_partition_keeps_auto_buckets() {
        let sink = partition_sink(
            SINK_TYPE_APPEND_ONLY,
            true,
            "month(ts)",
            vec![0, 1],
            vec![Field::with_name(DataType::Timestamp, "ts")],
        );
        let sql = sink.build_create_table_sql(1).unwrap();
        // Auto-RANGE accepts `BUCKETS AUTO`; only auto-LIST forbids it.
        assert!(
            sql.contains("BUCKETS AUTO"),
            "auto-RANGE partition keeps BUCKETS AUTO, sql: {sql}"
        );
    }

    #[test]
    fn test_create_table_sql_range_partition_column_is_not_null() {
        let sink = partition_sink(
            SINK_TYPE_APPEND_ONLY,
            true,
            "month(ts)",
            vec![0, 1],
            vec![Field::with_name(DataType::Timestamp, "ts")],
        );
        let sql = sink.build_create_table_sql(1).unwrap();
        // Doris rejects an auto-RANGE partition on a nullable column, so the partition column
        // (`ts`, the second column after the key `id`) must be emitted `NOT NULL`.
        assert!(
            sql.contains("`ts` DATETIME(6) NOT NULL"),
            "auto-RANGE partition column must be NOT NULL, sql: {sql}"
        );
        // The LIST form has no such requirement, so `id`/`city` stay nullable.
        let list_sink = partition_sink(
            SINK_TYPE_APPEND_ONLY,
            true,
            "id, city",
            vec![0, 1],
            vec![Field::with_name(DataType::Varchar, "city")],
        );
        let list_sql = list_sink.build_create_table_sql(1).unwrap();
        assert!(
            !list_sql.contains("NOT NULL"),
            "auto-LIST partition columns need not be NOT NULL, sql: {list_sql}"
        );
    }

    #[test]
    fn test_create_table_sql_partition_clause_comes_after_key_and_before_distributed() {
        let sink = partition_sink(
            SINK_TYPE_APPEND_ONLY,
            true,
            "month(ts)",
            vec![0, 1],
            vec![Field::with_name(DataType::Timestamp, "ts")],
        );
        let sql = sink.build_create_table_sql(1).unwrap();
        let key_pos = sql.find("DUPLICATE KEY").expect("key clause");
        let part_pos = sql
            .find("AUTO PARTITION BY RANGE")
            .expect("partition clause");
        let dist_pos = sql.find("DISTRIBUTED BY").expect("distribution clause");
        assert!(
            key_pos < part_pos && part_pos < dist_pos,
            "expected key < partition < distributed, sql: {sql}"
        );
    }

    #[test]
    fn test_create_table_sql_partition_column_not_in_schema_errors() {
        let sink = partition_sink(
            SINK_TYPE_APPEND_ONLY,
            true,
            "month(created_at)",
            vec![0, 1],
            vec![Field::with_name(DataType::Timestamp, "ts")],
        );
        let err = sink.build_create_table_sql(1).unwrap_err();
        assert!(
            format!("{}", err).contains("does not exist in the sink schema"),
            "got: {err}"
        );
    }

    #[test]
    fn test_create_table_sql_partition_column_must_be_key_errors() {
        // The partition column is not in the primary key, so Doris would reject the DDL. The
        // sink must reject it up front with a hint to extend the key.
        let sink = partition_sink(
            SINK_TYPE_UPSERT,
            false,
            "month(ts)",
            vec![0],
            vec![Field::with_name(DataType::Timestamp, "ts")],
        );
        let err = sink.build_create_table_sql(1).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("must be a Doris key column"), "got: {msg}");
        assert!(msg.contains("primary_key"), "got: {msg}");
    }

    #[test]
    fn test_create_table_sql_range_partition_rejects_non_date_type() {
        // Auto-RANGE partitions on a `date_trunc` of a `DATE`/`DATETIME` column; an integer
        // partition column has no valid RANGE reading.
        let sink = partition_sink(SINK_TYPE_APPEND_ONLY, true, "month(id)", vec![0], vec![]);
        let err = sink.build_create_table_sql(1).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("cannot be a Doris partition column"), "got: {msg}");
        assert!(msg.contains("DATE"), "got: {msg}");
    }

    #[test]
    fn test_create_table_sql_list_partition_rejects_decimal_type() {
        // `score` is a key column (DECIMAL is key-able) but DECIMAL is not an allowed auto-LIST
        // partition type, so the partition check must reject it.
        let sink = partition_sink(
            SINK_TYPE_APPEND_ONLY,
            true,
            "id, score",
            vec![0, 1],
            vec![Field::with_name(DataType::Decimal, "score")],
        );
        let err = sink.build_create_table_sql(1).unwrap_err();
        assert!(
            format!("{}", err).contains("cannot be a Doris partition column"),
            "got: {err}"
        );
    }

    #[test]
    fn test_doris_sink_supports_schema_change() {
        // The frontend gates `auto.schema.change` on this; Doris must claim support so a CDC table
        // sink can be created with the option.
        assert!(DorisSink::support_schema_change());
        let sink = build_sink(SINK_TYPE_UPSERT, false);
        assert!(sink.is_coordinated_sink());
    }

    #[test]
    fn test_build_alter_add_column_sql_joins_columns_and_quotes_names() {
        let sql = build_alter_add_column_sql(
            "demo",
            "sink_table",
            &[
                ("nickname".to_owned(), "STRING".to_owned()),
                ("score".to_owned(), "FLOAT".to_owned()),
            ],
        );
        assert_eq!(
            sql,
            "ALTER TABLE `demo`.`sink_table` ADD COLUMN (`nickname` STRING, `score` FLOAT)"
        );
    }

    #[test]
    fn test_build_alter_add_column_sql_escapes_backtick() {
        let sql = build_alter_add_column_sql(
            "demo",
            "sink_table",
            &[("we`ird".to_owned(), "INT".to_owned())],
        );
        assert_eq!(
            sql,
            "ALTER TABLE `demo`.`sink_table` ADD COLUMN (`we``ird` INT)"
        );
    }

    #[test]
    fn test_add_columns_from_schema_change_maps_rw_types_to_doris() {
        use risingwave_pb::stream_plan::sink_schema_change::Op as SinkSchemaChangeOp;
        use risingwave_pb::stream_plan::SinkAddColumnsOp;

        let add_columns = SinkAddColumnsOp {
            fields: vec![
                Field::with_name(DataType::Int64, "id").to_prost(),
                Field::with_name(DataType::Varchar, "name").to_prost(),
                Field::with_name(DataType::Timestamptz, "ts").to_prost(),
            ],
        };
        let change = risingwave_pb::stream_plan::PbSinkSchemaChange {
            original_schema: vec![],
            op: Some(SinkSchemaChangeOp::AddColumns(add_columns)),
        };

        // `timestamptz_as_datetime = false`: `timestamptz` maps to native `TIMESTAMPTZ`.
        let cols = DorisSinkCommitCoordinator::add_columns_from_schema_change(&change, false).unwrap();
        assert_eq!(
            cols,
            vec![
                ("id".to_owned(), "BIGINT".to_owned()),
                ("name".to_owned(), "STRING".to_owned()),
                ("ts".to_owned(), "TIMESTAMPTZ(6)".to_owned()),
            ]
        );

        // `timestamptz_as_datetime = true` (a Doris 3 target): `timestamptz` maps to `DATETIME`.
        let cols = DorisSinkCommitCoordinator::add_columns_from_schema_change(&change, true).unwrap();
        assert_eq!(cols[2], ("ts".to_owned(), "DATETIME(6)".to_owned()));
    }

    #[test]
    fn test_build_alter_drop_column_sql_joins_columns_and_quotes_names() {
        let sql = build_alter_drop_column_sql(
            "demo",
            "sink_table",
            &["old1".to_owned(), "we`ird".to_owned()],
        );
        assert_eq!(
            sql,
            "ALTER TABLE `demo`.`sink_table` DROP COLUMN `old1`, DROP COLUMN `we``ird`"
        );
    }

    #[test]
    fn test_commit_schema_change_maps_add_columns_via_doris_type_string() {
        use risingwave_pb::stream_plan::sink_schema_change::Op as SinkSchemaChangeOp;
        use risingwave_pb::stream_plan::SinkAddColumnsOp;

        let add_columns = SinkAddColumnsOp {
            fields: vec![
                Field::with_name(DataType::Int64, "id").to_prost(),
                Field::with_name(DataType::Varchar, "name").to_prost(),
            ],
        };
        let change = risingwave_pb::stream_plan::PbSinkSchemaChange {
            original_schema: vec![],
            op: Some(SinkSchemaChangeOp::AddColumns(add_columns)),
        };
        let cols = DorisSinkCommitCoordinator::add_columns_from_schema_change(&change, false).unwrap();
        assert_eq!(
            cols,
            vec![
                ("id".to_owned(), "BIGINT".to_owned()),
                ("name".to_owned(), "STRING".to_owned()),
            ]
        );
    }

    #[test]
    fn test_get_doris_type_string() {
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Int64, false, false).unwrap(),
            "BIGINT"
        );
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Int32.list(), false, false).unwrap(),
            "ARRAY<INT>"
        );
        // RisingWave `Timestamptz` maps to Doris `TIMESTAMPTZ(6)` so the value is stored as a
        // UTC instant and rendered in the Doris session timezone on read.
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Timestamptz, false, false).unwrap(),
            "TIMESTAMPTZ(6)"
        );
        // With `timestamptz_as_datetime` set (a Doris 3 target), auto-create emits `DATETIME(6)`.
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Timestamptz, false, true).unwrap(),
            "DATETIME(6)"
        );
        // `Struct` maps to Doris `STRUCT<name:type, ...>` with `STRING` for sub-field `VARCHAR`
        // (Doris normalizes `STRING` to `text` inside a struct).
        assert_eq!(
            DorisSink::get_doris_type_string(
                &DataType::Struct(StructType::new([
                    ("x", DataType::Int32),
                    ("y", DataType::Varchar)
                ])),
                false,
                false
            )
            .unwrap(),
            "STRUCT<x:INT,y:STRING>"
        );
        // Nested structs recurse.
        assert_eq!(
            DorisSink::get_doris_type_string(
                &DataType::Struct(StructType::new([(
                    "a",
                    DataType::Struct(StructType::new([
                        ("c", DataType::Int32),
                        ("d", DataType::Varchar)
                    ]))
                )])),
                false,
                false
            )
            .unwrap(),
            "STRUCT<a:STRUCT<c:INT,d:STRING>>"
        );
        // `ARRAY<STRUCT<...>>` combines the two recursive forms.
        assert_eq!(
            DorisSink::get_doris_type_string(
                &DataType::List(ListType::new(DataType::Struct(StructType::new([(
                    "x",
                    DataType::Int32
                )])))),
                false,
                false
            )
            .unwrap(),
            "ARRAY<STRUCT<x:INT>>"
        );
        // A `MAP` becomes Doris `MAP<K,V>`. The key uses `is_key` semantics because Doris rejects
        // `STRING` as a map key, so a `VARCHAR` key maps to `VARCHAR(65533)`; the value does not.
        assert_eq!(
            DorisSink::get_doris_type_string(
                &DataType::Map(MapType::from_kv(DataType::Varchar, DataType::Int32)),
                false,
                false
            )
            .unwrap(),
            "MAP<VARCHAR(65533),INT>"
        );
        assert_eq!(
            DorisSink::get_doris_type_string(
                &DataType::Map(MapType::from_kv(DataType::Int32, DataType::Varchar)),
                false,
                false
            )
            .unwrap(),
            "MAP<INT,STRING>"
        );
    }

    #[test]
    fn test_vector_maps_to_array_float() {
        // A `Vector` becomes Doris `ARRAY<FLOAT>`: RisingWave's `Vector` is a fixed-length
        // `Float32` array, which is exactly what Doris's `ARRAY<FLOAT>` holds. There is no
        // dedicated `VECTOR` column type in Doris.
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Vector(3), false, false).unwrap(),
            "ARRAY<FLOAT>"
        );
        assert!(accepts(&DataType::Vector(3), "ARRAY<FLOAT>"));
        assert!(!accepts(&DataType::Vector(3), "FLOAT"));
        assert!(!accepts(&DataType::Vector(3), "STRING"));
    }

    #[test]
    fn test_varchar_maps_to_string_unless_it_is_a_key() {
        // A non-key `VARCHAR` must become `STRING`: `VARCHAR(65533)` caps at 65533 bytes, and one
        // longer value fails the entire load (`max_filter_ratio` is 0), which the sink then retries
        // forever. Doris rejects `STRING` in key columns, so keys keep `VARCHAR` at its maximum.
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Varchar, false, false).unwrap(),
            "STRING"
        );
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Varchar, true, false).unwrap(),
            "VARCHAR(65533)"
        );
        // Array elements are never key columns, so they take the unbounded type too.
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Varchar.list(), true, false).unwrap(),
            "ARRAY<STRING>"
        );
    }

    #[test]
    fn test_fallback_types_map_to_varchar_or_string() {
        // Types with no natural Doris column fall back to a text column, reusing the `Varchar`
        // mapping: `STRING` for non-key, `VARCHAR(65533)` for key. The encoder stringifies them.
        // (`Serial` is excluded: it maps to `BIGINT` natively, see `test_serial_maps_to_bigint`.)
        for ty in [
            DataType::Time,
            DataType::Interval,
            DataType::Bytea,
            DataType::Int256,
        ] {
            assert_eq!(
                DorisSink::get_doris_type_string(&ty, false, false).unwrap(),
                "STRING",
                "fallback {ty:?} non-key"
            );
            assert_eq!(
                DorisSink::get_doris_type_string(&ty, true, false).unwrap(),
                "VARCHAR(65533)",
                "fallback {ty:?} key"
            );
            // Validation accepts only a text Doris column.
            assert!(
                accepts(&ty, "STRING"),
                "fallback {ty:?} must accept STRING"
            );
            assert!(
                accepts(&ty, "VARCHAR(100)"),
                "fallback {ty:?} must accept VARCHAR"
            );
            assert!(
                !accepts(&ty, "BIGINT"),
                "fallback {ty:?} must not accept BIGINT"
            );
        }
    }

    #[test]
    fn test_create_table_sql_uses_string_for_non_key_varchar() {
        let config = DorisConfig::from_btreemap(base_properties(SINK_TYPE_UPSERT)).unwrap();
        let schema = Schema::new(vec![
            Field::with_name(DataType::Varchar, "k"),
            Field::with_name(DataType::Varchar, "payload"),
        ]);
        let sink = DorisSink::new(
            config,
            schema,
            vec![0],
            false,
            test_param(SINK_TYPE_UPSERT, false),
        )
        .unwrap();
        let sql = sink.build_create_table_sql(1).unwrap();
        assert!(sql.contains("`k` VARCHAR(65533)"), "sql: {sql}");
        assert!(sql.contains("`payload` STRING"), "sql: {sql}");
    }

    // -- query_url tests (no network required) --

    #[test]
    fn test_get_query_url_preserved_exactly() {
        // The full mysql:// URL value set in sink properties is returned verbatim.
        let common = super::DorisCommon {
            url: "http://fe:8030".to_owned(),
            query_url: Some("mysql://doris-server:9030".to_owned()),
            stream_load_url: None,
            user: "u".to_owned(),
            password: "p".to_owned(),
            database: "d".to_owned(),
            table: "t".to_owned(),
            partial_update: None,
            auto_create: true,
            replication_num: None,
            partition_by: None,
            timestamptz_as_datetime: false,
        };
        assert_eq!(common.get_query_url().unwrap(), "mysql://doris-server:9030");
    }

    #[test]
    fn test_get_query_url_errors_when_absent() {
        // When query_url is unset and auto_create is enabled, the helper used by
        // build_ddl_client must error with a clear message.
        let common = super::DorisCommon {
            url: "http://fe:8030".to_owned(),
            query_url: None,
            stream_load_url: None,
            user: "u".to_owned(),
            password: "p".to_owned(),
            database: "d".to_owned(),
            table: "t".to_owned(),
            partial_update: None,
            auto_create: true,
            replication_num: None,
            partition_by: None,
            timestamptz_as_datetime: false,
        };
        let err = common.get_query_url().unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("doris.query_url"),
            "expected error mentioning 'doris.query_url', got: {msg}"
        );
    }

    #[test]
    fn test_build_ddl_opts_preserves_host_and_port() {
        // build_ddl_opts accepts a full mysql:// URL and produces Opts with the correct
        // host and port extracted from it.
        let opts = super::build_ddl_opts("mysql://query-fe:9030", "user", "pass").unwrap();
        assert_eq!(opts.ip_or_hostname(), "query-fe");
        assert_eq!(opts.tcp_port(), 9030);
    }

    #[test]
    fn test_build_ddl_opts_rejects_non_mysql_scheme() {
        // Non-mysql schemes must fail with a clear error.
        let err = super::build_ddl_opts("http://fe:9030", "user", "pass").unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("scheme must be 'mysql'"),
            "expected scheme error, got: {msg}"
        );
    }

    #[test]
    fn test_build_ddl_opts_rejects_missing_port() {
        let err = super::build_ddl_opts("mysql://query-fe", "user", "pass").unwrap_err();
        assert!(format!("{}", err).contains("explicit port"));
    }

    #[test]
    fn test_build_ddl_opts_rejects_invalid_url() {
        // Completely invalid URL must fail.
        assert!(super::build_ddl_opts("not a url", "user", "pass").is_err());
    }

    // -- W6: type matching --

    #[test]
    fn test_normalize_doris_type_strips_precision() {
        assert_eq!(normalize_doris_type("decimal(38, 9)"), "DECIMAL");
        assert_eq!(normalize_doris_type("TIMESTAMPTZ(6)"), "TIMESTAMPTZ");
        assert_eq!(normalize_doris_type("bigint"), "BIGINT");
        assert_eq!(normalize_doris_type("ARRAY<INT>"), "ARRAY<INT>");
    }

    fn accepts(rw_data_type: &DataType, doris_data_type: &str) -> bool {
        DorisSink::check_and_correct_column_type(rw_data_type, doris_data_type.to_owned(), false)
            .unwrap()
    }

    fn accepts_with_tstz_as_datetime(rw_data_type: &DataType, doris_data_type: &str) -> bool {
        DorisSink::check_and_correct_column_type(rw_data_type, doris_data_type.to_owned(), true)
            .unwrap()
    }

    #[test]
    fn test_integer_widening_is_allowed_but_narrowing_is_rejected() {
        // Narrowing silently stores an out-of-range value as NULL, so it must be rejected.
        for narrower in ["TINYINT", "SMALLINT"] {
            assert!(
                !accepts(&DataType::Int32, narrower),
                "Int32 must not accept {narrower}"
            );
        }
        for wide_enough in ["INT", "BIGINT", "LARGEINT"] {
            assert!(
                accepts(&DataType::Int32, wide_enough),
                "Int32 must accept {wide_enough}"
            );
        }

        assert!(!accepts(&DataType::Int16, "TINYINT"));
        assert!(accepts(&DataType::Int16, "SMALLINT"));
        assert!(accepts(&DataType::Int16, "BIGINT"));

        for narrower in ["TINYINT", "SMALLINT", "INT"] {
            assert!(
                !accepts(&DataType::Int64, narrower),
                "Int64 must not accept {narrower}"
            );
        }
        assert!(accepts(&DataType::Int64, "BIGINT"));
        assert!(accepts(&DataType::Int64, "LARGEINT"));
    }

    #[test]
    fn test_serial_maps_to_bigint() {
        // `Serial` is an auto-incrementing `i64` and maps to Doris `BIGINT` (an integer column,
        // not text). Doris's stream-load converter parses the encoder's decimal string into it.
        assert!(accepts(&DataType::Serial, "BIGINT"));
        assert!(accepts(&DataType::Serial, "LARGEINT"));
        // Narrowing would silently store an out-of-range value as NULL, so it is rejected.
        assert!(!accepts(&DataType::Serial, "INT"));
        // A text column is not what `Serial` maps to.
        assert!(!accepts(&DataType::Serial, "STRING"));
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Serial, false, false).unwrap(),
            "BIGINT"
        );
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Serial, true, false).unwrap(),
            "BIGINT"
        );
    }

    #[test]
    fn test_unrecognized_integer_spelling_falls_back_to_substring_match() {
        // A Doris type name we didn't anticipate must not break a sink that works today.
        assert!(accepts(&DataType::Int32, "INTEGER"));
        assert!(accepts(&DataType::Int64, "BIGINT UNSIGNED"));
        // ...but a modifier suffix must not let a narrower type through: `SMALLINT UNSIGNED`
        // contains `INT`, and accepting it would silently store an out-of-range `int` as NULL.
        assert!(!accepts(&DataType::Int32, "SMALLINT UNSIGNED"));
        assert!(!accepts(&DataType::Int64, "INT UNSIGNED"));
    }

    #[test]
    fn test_date_rejects_datetime() {
        assert!(accepts(&DataType::Date, "DATE"));
        assert!(accepts(&DataType::Date, "DATEV2"));
        assert!(!accepts(&DataType::Date, "DATETIME"));
        assert!(!accepts(&DataType::Date, "DATETIMEV2"));
    }

    #[test]
    fn test_timestamp_and_timestamptz_matching_unchanged() {
        assert!(accepts(&DataType::Timestamp, "DATETIME"));
        assert!(accepts(&DataType::Timestamp, "DATETIMEV2"));
        assert!(accepts(&DataType::Timestamptz, "TIMESTAMPTZ"));
        assert!(accepts(&DataType::Timestamptz, "TIMESTAMPTZ(6)"));
        // A tz-naive Doris column would make the stored value depend on the Doris session
        // timezone, so it stays an error rather than a plain `false`.
        assert!(
            DorisSink::check_and_correct_column_type(
                &DataType::Timestamptz,
                "DATETIME".to_owned(),
                false
            )
            .is_err()
        );
        // With `timestamptz_as_datetime` set (a Doris 3 target, which has no `TIMESTAMPTZ`),
        // a `DATETIME` column is accepted and the tz loss is the user's choice.
        assert!(accepts_with_tstz_as_datetime(&DataType::Timestamptz, "DATETIME"));
        assert!(accepts_with_tstz_as_datetime(&DataType::Timestamptz, "DATETIME(6)"));
        // A `TIMESTAMPTZ` target is still accepted with the option on.
        assert!(accepts_with_tstz_as_datetime(&DataType::Timestamptz, "TIMESTAMPTZ"));
        // But a non-date target is still rejected (an error, not a plain `false`).
        assert!(
            DorisSink::check_and_correct_column_type(
                &DataType::Timestamptz,
                "STRING".to_owned(),
                true
            )
            .is_err()
        );
        // Nested `timestamptz` is accepted: for the JSON path the nested value is encoded as a
        // tz-naive string that Doris reinterprets in the session timezone, so the stored instant
        // can shift if the server `time_zone` is not UTC. That is the user's responsibility to
        // accept (the JSON encoder has no way to mark a nested value's timezone), and the Arrow
        // path stores it correctly as a UTC instant regardless. Blocking it outright was too
        // strict, so a schema with nested `timestamptz` is no longer rejected.
        for doris_type in ["ARRAY<TIMESTAMPTZ(6)>", "ARRAY<DATETIME(6)>"] {
            assert!(
                accepts(&DataType::Timestamptz.list(), doris_type),
                "{doris_type} should be accepted"
            );
        }
        assert!(accepts(
            &DataType::Timestamptz.list().list(),
            "ARRAY<ARRAY<TIMESTAMPTZ(6)>>"
        ));
        assert!(accepts(
            &DataType::Struct(StructType::new([
                ("a", DataType::Int32),
                ("ts", DataType::Timestamptz)
            ])),
            "STRUCT<a:INT,ts:TIMESTAMPTZ(6)>"
        ));
        // Arrays and structs of other element types are unaffected.
        assert!(accepts(&DataType::Int32.list(), "ARRAY<INT>"));
        assert!(accepts(
            &DataType::Struct(StructType::new([("a", DataType::Int32)])),
            "STRUCT<a:INT>"
        ));
    }

    #[test]
    fn test_decimal_matching_unchanged() {
        for decimal_type in ["DECIMAL", "DECIMALV3", "DECIMAL128I", "DECIMAL(38, 9)"] {
            assert!(
                accepts(&DataType::Decimal, decimal_type),
                "Decimal must accept {decimal_type}"
            );
        }
    }

    // -- W6: subset schemas that would silently blank columns --

    fn doris_field(name: &str, r#type: &str) -> DorisField {
        DorisField {
            name: name.to_owned(),
            r#type: r#type.to_owned(),
            comment: String::new(),
            precision: None,
            scale: None,
            aggregation_type: String::new(),
        }
    }

    /// The Doris table the sinks built by `build_sink` are a strict subset of.
    fn doris_fields_with_extra_column() -> Vec<DorisField> {
        vec![
            doris_field("id", "BIGINT"),
            doris_field("name", "VARCHAR(100)"),
            doris_field("age", "INT"),
            doris_field("nickname", "VARCHAR(100)"),
        ]
    }

    #[test]
    fn test_upsert_subset_schema_is_rejected_without_partial_update() {
        let sink = build_sink(SINK_TYPE_UPSERT, false);
        let err = sink
            .check_column_name_and_type(doris_fields_with_extra_column())
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("nickname"), "got: {msg}");
        assert!(msg.contains("doris.partial_update"), "got: {msg}");
    }

    #[test]
    fn test_upsert_subset_schema_is_accepted_with_partial_update() {
        let mut properties = base_properties(SINK_TYPE_UPSERT);
        properties.insert("doris.partial_update".to_owned(), "true".to_owned());
        let config = DorisConfig::from_btreemap(properties).unwrap();
        let sink = DorisSink::new(
            config,
            upsert_schema(),
            vec![0],
            false,
            test_param(SINK_TYPE_UPSERT, false),
        )
        .unwrap();
        sink.check_column_name_and_type(doris_fields_with_extra_column())
            .unwrap();
    }

    #[test]
    fn test_append_only_subset_schema_is_accepted() {
        // A subset load into a DUPLICATE KEY table just inserts rows whose remaining columns take
        // their defaults. Nothing is overwritten, so there is nothing to reject.
        let sink = build_sink(SINK_TYPE_APPEND_ONLY, true);
        sink.check_column_name_and_type(doris_fields_with_extra_column())
            .unwrap();
    }

    #[test]
    fn test_doris_hidden_columns_do_not_count_as_omitted() {
        let sink = build_sink(SINK_TYPE_UPSERT, false);
        let mut fields = vec![
            doris_field("id", "BIGINT"),
            doris_field("name", "VARCHAR(100)"),
            doris_field("age", "INT"),
        ];
        fields.push(doris_field("__DORIS_DELETE_SIGN__", "TINYINT"));
        sink.check_column_name_and_type(fields).unwrap();
    }

    #[test]
    fn test_sink_with_more_columns_than_target_is_rejected() {
        let sink = build_sink(SINK_TYPE_UPSERT, false);
        let err = sink
            .check_column_name_and_type(vec![doris_field("id", "BIGINT")])
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("subset of the target table's columns"),
            "got: {msg}"
        );
    }

    // -- W6: request headers --

    fn upsert_schema() -> Schema {
        Schema::new(vec![
            Field::with_name(DataType::Int64, "id"),
            Field::with_name(DataType::Varchar, "name"),
            Field::with_name(DataType::Int32, "age"),
        ])
    }

    #[test]
    fn test_header_sets_strict_mode_and_backtick_quoted_columns() {
        let config = DorisConfig::from_btreemap(base_properties(SINK_TYPE_UPSERT)).unwrap();
        let header = DorisSinkWriter::build_stream_load_header(&config, &upsert_schema(), false);
        assert_eq!(header.get("strict_mode").map(String::as_str), Some("true"));
        assert_eq!(
            header.get("columns").map(String::as_str),
            Some("`id`,`name`,`age`,`__DORIS_DELETE_SIGN__`")
        );
        assert_eq!(
            header.get("hidden_columns").map(String::as_str),
            Some("__DORIS_DELETE_SIGN__")
        );
    }

    #[test]
    fn test_header_escapes_backtick_in_column_name() {
        // A backtick inside a column name must be doubled, otherwise the `columns` header is
        // malformed and Doris rejects every stream load of this sink.
        let config = DorisConfig::from_btreemap(base_properties(SINK_TYPE_APPEND_ONLY)).unwrap();
        let schema = Schema::new(vec![Field::with_name(DataType::Int64, "we`ird")]);
        let header = DorisSinkWriter::build_stream_load_header(&config, &schema, true);
        assert_eq!(header.get("columns").map(String::as_str), Some("`we``ird`"));
    }

    #[test]
    fn test_append_only_header_omits_the_delete_sign() {
        let config = DorisConfig::from_btreemap(base_properties(SINK_TYPE_APPEND_ONLY)).unwrap();
        let header = DorisSinkWriter::build_stream_load_header(&config, &upsert_schema(), true);
        assert_eq!(
            header.get("columns").map(String::as_str),
            Some("`id`,`name`,`age`")
        );
        assert!(!header.contains_key("hidden_columns"));
    }

    #[test]
    fn test_header_reflects_strict_mode_disabled() {
        let mut properties = base_properties(SINK_TYPE_APPEND_ONLY);
        properties.insert("doris.strict_mode".to_owned(), "false".to_owned());
        let config = DorisConfig::from_btreemap(properties).unwrap();
        let header = DorisSinkWriter::build_stream_load_header(&config, &upsert_schema(), true);
        assert_eq!(header.get("strict_mode").map(String::as_str), Some("false"));
    }

    // -- W2 / W3 / W4 / W6 / W8: config defaults and validation --

    #[test]
    fn test_config_defaults() {
        let config = DorisConfig::from_btreemap(base_properties(SINK_TYPE_APPEND_ONLY)).unwrap();
        assert_eq!(config.commit_checkpoint_interval, 10);
        assert_eq!(config.max_batch_size_bytes, 100 * 1024 * 1024);
        assert_eq!(config.format, "json");
        assert!(config.strict_mode);
        assert_eq!(config.stream_load_http_timeout_ms, 30 * 1000);
        assert!(!config.common.auto_create);
        assert!(!config.common.timestamptz_as_datetime);
    }

    #[test]
    fn test_config_parses_timestamptz_as_datetime() {
        // Default is off.
        let config = DorisConfig::from_btreemap(base_properties(SINK_TYPE_APPEND_ONLY)).unwrap();
        assert!(!config.common.timestamptz_as_datetime);

        // Exact 'true' turns it on; other spellings are rejected by `DisplayFromStr`.
        let mut on = base_properties(SINK_TYPE_APPEND_ONLY);
        on.insert("timestamptz_as_datetime".to_owned(), "true".to_owned());
        let config = DorisConfig::from_btreemap(on).unwrap();
        assert!(config.common.timestamptz_as_datetime);

        let mut bad = base_properties(SINK_TYPE_APPEND_ONLY);
        bad.insert("timestamptz_as_datetime".to_owned(), "yes".to_owned());
        assert!(DorisConfig::from_btreemap(bad).is_err());
    }

    #[test]
    fn test_config_accepts_arrow_format() {
        let mut properties = base_properties(SINK_TYPE_APPEND_ONLY);
        properties.insert("doris.format".to_owned(), "arrow".to_owned());
        let config = DorisConfig::from_btreemap(properties).unwrap();
        assert_eq!(config.format, "arrow");
    }

    #[test]
    fn test_config_rejects_unknown_format() {
        let mut properties = base_properties(SINK_TYPE_APPEND_ONLY);
        properties.insert("doris.format".to_owned(), "csv".to_owned());
        let err = DorisConfig::from_btreemap(properties).unwrap_err();
        assert!(err.to_string().contains("doris.format"), "got: {err}");
    }

    #[test]
    fn test_arrow_header_uses_arrow_format_and_omits_json_by_line() {
        let mut properties = base_properties(SINK_TYPE_APPEND_ONLY);
        properties.insert("doris.format".to_owned(), "arrow".to_owned());
        let config = DorisConfig::from_btreemap(properties).unwrap();
        let header = DorisSinkWriter::build_stream_load_header(&config, &upsert_schema(), true);
        assert_eq!(header.get("format").map(String::as_str), Some("arrow"));
        assert!(!header.contains_key("read_json_by_line"));
        assert_eq!(
            header.get("columns").map(String::as_str),
            Some("`id`,`name`,`age`")
        );
    }

    #[test]
    fn test_arrow_header_includes_delete_sign_for_upsert() {
        let mut properties = base_properties(SINK_TYPE_UPSERT);
        properties.insert("doris.format".to_owned(), "arrow".to_owned());
        let config = DorisConfig::from_btreemap(properties).unwrap();
        let header = DorisSinkWriter::build_stream_load_header(&config, &upsert_schema(), false);
        assert_eq!(header.get("format").map(String::as_str), Some("arrow"));
        assert_eq!(
            header.get("columns").map(String::as_str),
            Some("`id`,`name`,`age`,`__DORIS_DELETE_SIGN__`")
        );
        assert_eq!(
            header.get("hidden_columns").map(String::as_str),
            Some("__DORIS_DELETE_SIGN__")
        );
    }

    #[test]
    fn test_arrow_schema_covers_all_rw_types() {
        // Arrow supports every RisingWave type, including the complex ones parquet gated out.
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int64, "id"),
            Field::with_name(DataType::Varchar, "name"),
            Field::with_name(DataType::Timestamp, "ts"),
            Field::with_name(DataType::List(ListType::new(DataType::Int32)), "arr"),
            Field::with_name(
                DataType::Struct(StructType::new([("x", DataType::Int32)])),
                "st",
            ),
            Field::with_name(DataType::Jsonb, "js"),
        ]);
        let arrow = doris_arrow_schema(&schema, false);
        assert_eq!(arrow.fields().len(), 6);
        use risingwave_common::array::arrow::arrow_schema_58::DataType as ArrowDt;
        assert_eq!(arrow.field(3).data_type(), &ArrowDt::List(Arc::new(arrow_schema_58::Field::new("item", ArrowDt::Int32, true))));
        assert_eq!(arrow.field(5).data_type(), &ArrowDt::Utf8);
    }

    #[test]
    fn test_arrow_schema_stringifies_fallback_types() {
        // Fallback types are stored as text in Doris, so their Arrow field is `Utf8` rather than
        // the native Time64/Interval/Binary/Decimal256. (`Serial` maps to `BIGINT`, so it is not
        // stringified.)
        use risingwave_common::array::arrow::arrow_schema_58::DataType as ArrowDt;
        let schema = Schema::new(vec![
            Field::with_name(DataType::Time, "t"),
            Field::with_name(DataType::Interval, "i"),
            Field::with_name(DataType::Bytea, "b"),
            Field::with_name(DataType::Int256, "n"),
        ]);
        let arrow = doris_arrow_schema(&schema, false);
        for f in arrow.fields() {
            assert_eq!(
                f.data_type(),
                &ArrowDt::Utf8,
                "fallback field {} must be Utf8",
                f.name()
            );
        }
    }

    #[test]
    fn test_arrow_schema_tstz_naive_with_timestamptz_as_datetime() {
        // `timestamptz_as_datetime` targets a tz-naive Doris `DATETIME`, so the Arrow field must
        // carry no timezone; otherwise the array (tz-aware UTC) would not match the target column.
        use risingwave_common::array::arrow::arrow_schema_58::DataType as ArrowDt;
        let schema = Schema::new(vec![Field::with_name(DataType::Timestamptz, "ts")]);
        let arrow = doris_arrow_schema(&schema, true);
        assert_eq!(
            arrow.field(0).data_type(),
            &ArrowDt::Timestamp(arrow_schema_58::TimeUnit::Microsecond, None),
            "timestamptz_as_datetime arrow field must be tz-naive"
        );
        let arrow = doris_arrow_schema(&schema, false);
        assert_eq!(
            arrow.field(0).data_type(),
            &ArrowDt::Timestamp(
                arrow_schema_58::TimeUnit::Microsecond,
                Some("+00:00".to_owned().into())
            ),
            "default arrow field must carry the UTC timezone"
        );
    }

    #[test]
    fn test_stringify_fallback_array() {
        use risingwave_common::array::arrow::arrow_array_58::Array as ArrowArray;
        use risingwave_common::array::arrow::arrow_array_58::StringArray;
        use risingwave_common::types::{Interval, Time};

        // Time -> "HH:MM:SS.ffffff", Interval -> ISO-8601, Serial/Int256 -> decimal.
        let time = Time::from_num_seconds_from_midnight_uncheck(1000, 0);
        let arr = risingwave_common::array::TimeArray::from_iter([Some(time)]);
        let out = stringify_fallback_array(&arr.into()).unwrap();
        assert_eq!(out.as_any().downcast_ref::<StringArray>().unwrap().value(0), "00:16:40.000000");

        let interval = Interval::from_month_day_usec(13, 2, 1000000);
        let arr = risingwave_common::array::IntervalArray::from_iter([Some(interval)]);
        let out = stringify_fallback_array(&arr.into()).unwrap();
        assert_eq!(out.as_any().downcast_ref::<StringArray>().unwrap().value(0), "P1Y1M2DT0H0M1S");

        let int256 = risingwave_common::types::Int256::from(42_i64);
        let arr = risingwave_common::array::Int256Array::from_iter([int256]);
        let out = stringify_fallback_array(&arr.into()).unwrap();
        assert_eq!(out.as_any().downcast_ref::<StringArray>().unwrap().value(0), "42");

        // Bytea -> base64.
        let bytes: &[u8] = b"abc\xff";
        let arr = risingwave_common::array::BytesArray::from_iter([Some(bytes)]);
        let out = stringify_fallback_array(&arr.into()).unwrap();
        assert_eq!(out.as_any().downcast_ref::<StringArray>().unwrap().value(0), "YWJj/w==");
    }

    #[test]
    fn test_config_rejects_zero_commit_checkpoint_interval() {
        let mut properties = base_properties(SINK_TYPE_APPEND_ONLY);
        properties.insert("commit_checkpoint_interval".to_owned(), "0".to_owned());
        let err = DorisConfig::from_btreemap(properties).unwrap_err();
        assert!(
            err.to_string()
                .contains("`commit_checkpoint_interval` must be greater than 0"),
            "got: {err}"
        );
    }

    #[test]
    fn test_config_rejects_zero_max_batch_size_bytes() {
        let mut properties = base_properties(SINK_TYPE_APPEND_ONLY);
        properties.insert("doris.max_batch_size_bytes".to_owned(), "0".to_owned());
        let err = DorisConfig::from_btreemap(properties).unwrap_err();
        assert!(
            err.to_string()
                .contains("`doris.max_batch_size_bytes` must be greater than 0"),
            "got: {err}"
        );
    }

    #[test]
    fn test_config_rejects_non_numeric_replication_num() {
        // The value goes straight into the auto-create DDL's `PROPERTIES`, so anything that isn't a
        // plain positive integer must be rejected rather than injected.
        for value in ["0", "one", "1\", \"foo\" = \"bar"] {
            let mut properties = base_properties(SINK_TYPE_APPEND_ONLY);
            properties.insert("doris.replication_num".to_owned(), value.to_owned());
            let err = DorisConfig::from_btreemap(properties).unwrap_err();
            assert!(
                err.to_string()
                    .contains("`doris.replication_num` must be a positive integer"),
                "got: {err}"
            );
        }
        let mut properties = base_properties(SINK_TYPE_APPEND_ONLY);
        properties.insert("doris.replication_num".to_owned(), "3".to_owned());
        assert_eq!(
            DorisConfig::from_btreemap(properties)
                .unwrap()
                .common
                .replication_num,
            Some("3".to_owned())
        );
    }

    /// `auto_create` is deliberately *not* prefixed with the connector name, unlike every other
    /// Doris-specific option. It is a cross-connector option name: `bigquery` prefixes all of its
    /// own options yet keeps `auto_create` bare, and Iceberg/Snowflake/Redshift spell the same
    /// concept as the equally unprefixed `create_table_if_not_exists`.
    #[test]
    fn test_auto_create_is_not_connector_prefixed() {
        let mut properties = base_properties(SINK_TYPE_APPEND_ONLY);
        properties.insert("auto_create".to_owned(), "true".to_owned());
        let config = DorisConfig::from_btreemap(properties).unwrap();
        assert!(config.common.auto_create);

        // A prefixed spelling is not silently accepted — it lands in `unknown_fields` and is
        // rejected by the unknown-fields check, rather than being ignored.
        let mut prefixed = base_properties(SINK_TYPE_APPEND_ONLY);
        prefixed.insert("doris.auto_create".to_owned(), "true".to_owned());
        let config = DorisConfig::from_btreemap(prefixed).unwrap();
        assert!(!config.common.auto_create);
        assert!(config.unknown_fields.contains_key("doris.auto_create"));
    }

    #[test]
    fn test_bool_options_reject_non_exact_spellings() {
        // `DisplayFromStr` on a bool accepts exactly "true"/"false".
        for (key, value) in [
            ("auto_create", "TRUE"),
            ("auto_create", "1"),
            ("doris.strict_mode", "yes"),
        ] {
            let mut properties = base_properties(SINK_TYPE_APPEND_ONLY);
            properties.insert(key.to_owned(), value.to_owned());
            assert!(
                DorisConfig::from_btreemap(properties).is_err(),
                "{key} = {value} should be rejected"
            );
        }
    }

    // -- W3: payload cap --

    #[test]
    fn test_batch_size_allows_exact_limit() {
        assert_eq!(
            decide_load_request_size(3, 2, 5),
            LoadRequestSizeDecision {
                finish_current_load: false,
                next_batch_size_bytes: 5,
            }
        );
    }

    #[test]
    fn test_batch_size_rolls_over_before_exceeding_limit() {
        assert_eq!(
            decide_load_request_size(4, 2, 5),
            LoadRequestSizeDecision {
                finish_current_load: true,
                next_batch_size_bytes: 2,
            }
        );
    }

    #[test]
    fn test_batch_size_gives_a_single_oversized_row_its_own_load() {
        // A row bigger than the whole cap must still go out, or the sink would stall forever on a
        // row it cannot shrink. On an empty load it is written as-is...
        assert_eq!(
            decide_load_request_size(0, 6, 5),
            LoadRequestSizeDecision {
                finish_current_load: false,
                next_batch_size_bytes: 6,
            }
        );
        // ...and with rows already buffered, that load is closed first so the oversized row still
        // ends up alone. The next row then closes its load in turn, since 6 is already over 5.
        assert_eq!(
            decide_load_request_size(1, 6, 5),
            LoadRequestSizeDecision {
                finish_current_load: true,
                next_batch_size_bytes: 6,
            }
        );
    }

    #[test]
    fn test_batch_size_rolls_over_on_u64_overflow() {
        assert_eq!(
            decide_load_request_size(u64::MAX, 1, u64::MAX),
            LoadRequestSizeDecision {
                finish_current_load: true,
                next_batch_size_bytes: 1,
            }
        );
    }

    // -- W1: stream load result verification --

    fn load_result(
        loaded: i64,
        filtered: i32,
        unselected: i32,
        status: &str,
    ) -> DorisInsertResultResponse {
        DorisInsertResultResponse {
            txn_id: 42,
            label: "label-1".to_owned(),
            status: status.to_owned(),
            two_phase_commit: "false".to_owned(),
            message: "OK".to_owned(),
            number_total_rows: loaded + i64::from(filtered) + i64::from(unselected),
            number_loaded_rows: loaded,
            number_filtered_rows: filtered,
            number_unselected_rows: unselected,
            load_bytes: 100,
            load_time_ms: 5,
            begin_txn_time_ms: 1,
            stream_load_put_time_ms: 1,
            read_data_time_ms: 1,
            write_data_time_ms: 1,
            commit_and_publish_time_ms: 1,
            err_url: Some("http://be:8040/api/_load_error_log?file=x".to_owned()),
        }
    }

    #[test]
    fn test_clean_load_passes_verification() {
        load_result(3, 0, 0, "Success")
            .check_all_rows_loaded()
            .unwrap();
    }

    #[test]
    fn test_filtered_rows_fail_the_load() {
        let err = load_result(2, 1, 0, "Success")
            .check_all_rows_loaded()
            .unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("filtered 1 of the 3 rows it read"),
            "got: {msg}"
        );
        // The error URL is where Doris records the offending rows, so it must be surfaced.
        assert!(msg.contains("_load_error_log"), "got: {msg}");
    }

    #[test]
    fn test_unselected_rows_fail_the_load() {
        let err = load_result(2, 0, 1, "Success")
            .check_all_rows_loaded()
            .unwrap_err();
        assert!(
            format!("{}", err)
                .contains("left 1 of the 3 rows it read in this stream load unselected"),
            "got: {err}"
        );
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

impl DorisInsertResultResponse {
    /// Verify that Doris did not silently drop rows.
    ///
    /// A `Status` of `Success` on its own does not mean the data arrived: Doris reports dropped
    /// rows in `NumberFilteredRows` / `NumberUnselectedRows` and still calls the load a success as
    /// long as the filtered ratio stays within `max_filter_ratio`. The sink never raises
    /// `max_filter_ratio` above its default of 0, so any nonzero count here is a bug or a
    /// conversion failure that must not pass silently — this check is also what makes
    /// `doris.strict_mode` meaningful, since strict mode's whole effect is to turn a bad value
    /// into a filtered row.
    fn check_all_rows_loaded(&self) -> Result<()> {
        if self.number_filtered_rows != 0 {
            return Err(SinkError::DorisStarrocksConnect(anyhow!(
                "Doris filtered {} of the {} rows it read in this stream load. message: {:?}, \
                 error url: {:?}, txn id: {}, label: {:?}",
                self.number_filtered_rows,
                self.number_total_rows,
                self.message,
                self.err_url,
                self.txn_id,
                self.label
            )));
        }
        if self.number_unselected_rows != 0 {
            return Err(SinkError::DorisStarrocksConnect(anyhow!(
                "Doris left {} of the {} rows it read in this stream load unselected. \
                 message: {:?}, error url: {:?}, txn id: {}, label: {:?}",
                self.number_unselected_rows,
                self.number_total_rows,
                self.message,
                self.err_url,
                self.txn_id,
                self.label
            )));
        }
        Ok(())
    }
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
        // The response model is strict (every field is required); if Doris ever returns a
        // 200 response that lacks some field (e.g. an early txn failure body), a bare serde
        // error would mask what Doris actually said. Include the raw body in that error.
        let res: DorisInsertResultResponse = serde_json::from_slice(&raw).map_err(|err| {
            SinkError::DorisStarrocksConnect(anyhow!(
                "failed to parse stream load response: {:?}, raw response: {}",
                err,
                String::from_utf8_lossy(&raw)
            ))
        })?;

        if !DORIS_SUCCESS_STATUS.contains(&res.status.as_str()) {
            return Err(SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                "Insert error: {:?}, error url: {:?}",
                res.message,
                res.err_url
            )));
        };
        res.check_all_rows_loaded()?;
        tracing::info!(
            txn_id = res.txn_id,
            label = %res.label,
            status = %res.status,
            number_loaded_rows = res.number_loaded_rows,
            load_bytes = res.load_bytes,
            load_time_ms = res.load_time_ms,
            "doris stream load committed"
        );
        Ok(res)
    }
}

