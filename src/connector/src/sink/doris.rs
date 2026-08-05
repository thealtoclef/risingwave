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

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose;
use bytes::{BufMut, Bytes, BytesMut};
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use url::Url;
use with_options::WithOptions;

use super::decouple_checkpoint_log_sink::{
    DecoupleCheckpointLogSinkerOf, default_commit_checkpoint_interval,
};
use super::doris_starrocks_connector::{
    DORIS_DELETE_SIGN, DORIS_SUCCESS_STATUS, HeaderBuilder, InserterInner, InserterInnerBuilder,
    POOL_IDLE_TIMEOUT,
};
use super::{
    Result, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, SinkError, SinkWriterMetrics,
};
use crate::enforce_secret::EnforceSecret;
use crate::sink::encoder::{DorisJsonConfig, JsonEncoder, RowEncoder};
use crate::sink::writer::SinkWriter;
use crate::sink::{Sink, SinkParam, SinkWriterParam};

pub const DORIS_SINK: &str = "doris";

// Connection parameters for the MySQL-protocol query endpoint of Doris FE, only used for DDL
// (e.g. auto-create). `mysql_async` applies `max_allowed_packet` as a client-side cap on the
// outbound packet, and this client sends `CREATE TABLE` statements, so it needs enough room for
// the DDL of a wide table or auto-create fails with `PacketTooLarge`.
const DORIS_MYSQL_MAX_ALLOWED_PACKET: usize = 1024 * 1024;
const DORIS_MYSQL_WAIT_TIMEOUT: usize = 28800;

const fn default_stream_load_http_timeout_ms() -> u64 {
    60 * 1000
}

/// Default cap on the payload of a single stream load. See
/// [`DorisConfig::max_batch_size_bytes`] for why this defaults to a finite value.
const fn default_max_batch_size_bytes() -> u64 {
    32 * 1024 * 1024
}

const fn default_strict_mode() -> bool {
    true
}

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct DorisCommon {
    #[serde(rename = "doris.url")]
    pub url: String,
    /// The full MySQL-protocol Doris FE URL with an explicit port, used only when `auto_create`
    /// is enabled, e.g. `mysql://query-fe:9030`.
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
    /// required when `auto_create` is enabled and `build_ddl_client` is called).
    fn get_query_url(&self) -> Result<&str> {
        self.query_url.as_deref().ok_or_else(|| {
            SinkError::DorisStarrocksConnect(anyhow!(
                "doris.query_url must be set when auto_create is enabled"
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

    /// The budget in milliseconds for Doris to commit and publish one stream load of up to
    /// `doris.max_batch_size_bytes`, defaults to 60 seconds.
    ///
    /// The timer starts when RisingWave closes the request body, so it covers only the final
    /// commit-and-publish, not the time spent streaming rows. It is therefore independent of
    /// `commit_checkpoint_interval`.
    #[serde(
        rename = "doris.stream_load.http.timeout.ms",
        default = "default_stream_load_http_timeout_ms"
    )]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub stream_load_http_timeout_ms: u64,

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

    /// The maximum payload size in bytes of a single Doris stream load, defaults to 32MB. Once
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

/// Whether `data_type` is, or nests, a `Timestamptz`.
///
/// The `TIMESTAMPTZ` format override in the JSON encoder is keyed on the *top-level* column name,
/// so a `Timestamptz` nested inside an `ARRAY` or a `STRUCT` never gets it: it is encoded as a
/// tz-naive string that Doris reinterprets in the session timezone, storing a different instant.
/// That is the same silent shifting the scalar `Timestamptz` arm rejects, and it happens whatever
/// the nested Doris type is, so nested `timestamptz` has to be rejected outright.
fn contains_timestamptz(data_type: &DataType) -> bool {
    match data_type {
        DataType::Timestamptz => true,
        DataType::List(list) => contains_timestamptz(list.elem()),
        DataType::Struct(st) => st.types().any(contains_timestamptz),
        _ => false,
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
            if !Self::check_and_correct_column_type(&i.data_type, value.clone())? {
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
            risingwave_common::types::DataType::Time => {
                Err(SinkError::Doris("TIME is not supported for Doris sink. Please convert to VARCHAR or other supported types.".to_owned()))
            }
            risingwave_common::types::DataType::Timestamp => {
                Ok(doris_data_type.contains("DATETIME"))
            }
            risingwave_common::types::DataType::Timestamptz => {
                // Doris 4.x supports a native `TIMESTAMPTZ` (microsecond precision, UTC stored,
                // re-rendered in session TZ on read). Accept writes against `TIMESTAMPTZ` columns
                // and let the encoder emit tz-bearing strings; reject writes against `DATETIME`
                // because Doris interprets a tz-naive string in the session's timezone, which
                // makes the stored value depend on the Doris FE/BE `time_zone` setting rather
                // than the actual UTC instant.
                if doris_data_type.contains("TIMESTAMPTZ") {
                    Ok(true)
                } else {
                    Err(SinkError::Doris(format!(
                        "TIMESTAMP WITH TIMEZONE can only be written to a Doris `TIMESTAMPTZ` \
                         column (the Doris column type is `{}`); either declare the target \
                         column as `TIMESTAMPTZ` or cast the source value with \
                         `... AT TIME ZONE '<offset>'` to a plain `TIMESTAMP` first.",
                        doris_data_type
                    )))
                }
            }
            risingwave_common::types::DataType::Interval => Err(SinkError::Doris(
                "INTERVAL is not supported for Doris sink. Please convert to VARCHAR or other supported types.".to_owned(),
            )),
            risingwave_common::types::DataType::Struct(st) => {
                if st.types().any(contains_timestamptz) {
                    return Err(SinkError::Doris(
                        "TIMESTAMP WITH TIMEZONE nested in a STRUCT is not supported for Doris sink. Please cast the field to TIMESTAMP or VARCHAR first.".to_owned(),
                    ));
                }
                Ok(doris_data_type.contains("STRUCT"))
            }
            risingwave_common::types::DataType::List(list) => {
                if contains_timestamptz(list.elem()) {
                    return Err(SinkError::Doris(
                        "TIMESTAMP WITH TIMEZONE nested in an ARRAY is not supported for Doris sink. Please cast the elements to TIMESTAMP or VARCHAR first.".to_owned(),
                    ));
                }
                Ok(doris_data_type.contains("ARRAY"))
            }
            risingwave_common::types::DataType::Bytea => {
                Err(SinkError::Doris("BYTEA is not supported for Doris sink. Please convert to VARCHAR or other supported types.".to_owned()))
            }
            risingwave_common::types::DataType::Jsonb => {
                Ok(doris_data_type.contains("JSON") || is_variant)
            }
            risingwave_common::types::DataType::Serial => {
                // The JSON encoder emits `Serial` as a hex string (`"0x0000000000000001"`) to keep
                // large values away from JSON number precision. Doris cannot convert that to any
                // integer type: with `doris.strict_mode` off it stores NULL, and with strict mode
                // on the row is filtered and the load fails, which stalls the sink on a retry that
                // can never succeed. Neither outcome is usable, so reject it up front.
                Err(SinkError::Doris(
                    "SERIAL is not supported for Doris sink. Please cast to BIGINT or VARCHAR first.".to_owned(),
                ))
            }
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
    ///
    /// `is_key` selects between the two string types Doris offers, which differ in what they can
    /// hold: `STRING` is effectively unbounded but is rejected in key columns, while `VARCHAR(n)`
    /// is capped at 65533 *bytes*. Key columns therefore have no alternative to `VARCHAR`, and a
    /// key value longer than that stalls the sink; see [`Self::build_create_table_sql`].
    fn get_doris_type_string(data_type: &DataType, is_key: bool) -> Result<String> {
        match data_type {
            DataType::Boolean => Ok("BOOLEAN".to_owned()),
            DataType::Int16 => Ok("SMALLINT".to_owned()),
            DataType::Int32 => Ok("INT".to_owned()),
            DataType::Int64 => Ok("BIGINT".to_owned()),
            // Rejected here as well as in `check_and_correct_column_type`, because auto-create runs
            // first: mapping it to `BIGINT` would leave a table behind that no load can ever fill.
            DataType::Serial => Err(SinkError::Doris(
                "SERIAL is not supported for Doris sink. Please cast to BIGINT or VARCHAR first."
                    .to_owned(),
            )),
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
                Self::get_doris_type_string(inner.elem(), false)?
            )),
            DataType::Time => Err(SinkError::Doris(
                "TIME is not supported for Doris sink. Please convert to VARCHAR or other supported types.".to_owned(),
            )),
            // Doris 4.x `TIMESTAMPTZ(p)` stores a UTC microsecond instant and renders it in
            // the session timezone at query time. Use precision 6 to match RisingWave's
            // `Timestamptz` resolution.
            DataType::Timestamptz => Ok("TIMESTAMPTZ(6)".to_owned()),
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
    /// forbids `FLOAT`/`DOUBLE` and complex types (`JSON`, `ARRAY`, ...) as key columns, and
    /// `STRING`, which is why a `Varchar` key is emitted as `VARCHAR`. Types this sink rejects
    /// outright are not listed, since they can never be emitted at all.
    fn is_doris_key_type(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
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
    ///
    /// Note that a `VARCHAR` key column is limited to 65533 bytes by Doris and there is no wider
    /// key type available, so a sink keyed on a string longer than that cannot load into the table
    /// created here. Such a schema needs a manually created table with a different key.
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
                Self::get_doris_type_string(&field.data_type, key_indices.contains(&i))?
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
            let create_table_sql = self.build_create_table_sql()?;
            tracing::info!(sql = %create_table_sql, "auto-creating Doris table");
            client.execute_sql(&create_table_sql).await?;
        }

        Ok(())
    }
}

impl Sink for DorisSink {
    type LogSinker = DecoupleCheckpointLogSinkerOf<DorisSinkWriter>;

    const SINK_NAME: &'static str = DORIS_SINK;

    crate::impl_validate_sink_unknown_fields!();

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

        Ok(DecoupleCheckpointLogSinkerOf::new(
            writer,
            SinkWriterMetrics::new(&writer_param),
            commit_checkpoint_interval,
        ))
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
    max_batch_size_bytes: u64,
    current_batch_size_bytes: u64,
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

        let header_builder = HeaderBuilder::new()
            .add_common_header()
            .set_user_password(config.common.user.clone(), config.common.password.clone())
            .add_json_format()
            .set_partial_columns(config.common.partial_update.clone())
            .set_strict_mode(config.strict_mode)
            .set_columns_name(field_names_str)
            .add_read_json_by_line();
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

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<()> {
        // Only commit on the checkpoint barriers `DecoupleCheckpointLogSinkerOf` selects, i.e. one
        // in every `commit_checkpoint_interval`. Committing on every barrier would defeat the
        // interval entirely, because for Doris each committed load is a new table version.
        //
        // Holding one request open across barriers is safe: `begin_epoch`/`abort` are no-ops and
        // the newline-separator state lives on `DorisClient`, which survives alongside the request.
        if is_checkpoint {
            self.finish_load_request().await?;
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
    use risingwave_common::types::{DataType, StructType};

    use super::{
        DorisConfig, DorisField, DorisInsertResultResponse, DorisSink, DorisSinkWriter,
        LoadRequestSizeDecision, decide_load_request_size, normalize_doris_type,
    };
    use crate::sink::{SINK_TYPE_APPEND_ONLY, SINK_TYPE_UPSERT};

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
        DorisSink::new(config, upsert_schema(), vec![0], is_append_only).unwrap()
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
            DorisSink::get_doris_type_string(&DataType::Int64, false).unwrap(),
            "BIGINT"
        );
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Int32.list(), false).unwrap(),
            "ARRAY<INT>"
        );
        // RisingWave `Timestamptz` maps to Doris `TIMESTAMPTZ(6)` so the value is stored as a
        // UTC instant and rendered in the Doris session timezone on read.
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Timestamptz, false).unwrap(),
            "TIMESTAMPTZ(6)"
        );
    }

    #[test]
    fn test_varchar_maps_to_string_unless_it_is_a_key() {
        // A non-key `VARCHAR` must become `STRING`: `VARCHAR(65533)` caps at 65533 bytes, and one
        // longer value fails the entire load (`max_filter_ratio` is 0), which the sink then retries
        // forever. Doris rejects `STRING` in key columns, so keys keep `VARCHAR` at its maximum.
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Varchar, false).unwrap(),
            "STRING"
        );
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Varchar, true).unwrap(),
            "VARCHAR(65533)"
        );
        // Array elements are never key columns, so they take the unbounded type too.
        assert_eq!(
            DorisSink::get_doris_type_string(&DataType::Varchar.list(), true).unwrap(),
            "ARRAY<STRING>"
        );
    }

    #[test]
    fn test_create_table_sql_uses_string_for_non_key_varchar() {
        let config = DorisConfig::from_btreemap(base_properties(SINK_TYPE_UPSERT)).unwrap();
        let schema = Schema::new(vec![
            Field::with_name(DataType::Varchar, "k"),
            Field::with_name(DataType::Varchar, "payload"),
        ]);
        let sink = DorisSink::new(config, schema, vec![0], false).unwrap();
        let sql = sink.build_create_table_sql().unwrap();
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
        DorisSink::check_and_correct_column_type(rw_data_type, doris_data_type.to_owned()).unwrap()
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
    fn test_serial_is_rejected() {
        // The JSON encoder emits `Serial` as a hex string, which Doris turns into NULL (strict mode
        // off) or a filtered row that fails the load (strict mode on, the default). Both are
        // rejected at validation instead, including for auto-create.
        assert!(
            DorisSink::check_and_correct_column_type(&DataType::Serial, "BIGINT".to_owned())
                .is_err()
        );
        assert!(DorisSink::get_doris_type_string(&DataType::Serial, true).is_err());
        assert!(DorisSink::get_doris_type_string(&DataType::Serial, false).is_err());
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
            DorisSink::check_and_correct_column_type(&DataType::Timestamptz, "DATETIME".to_owned())
                .is_err()
        );
        // The per-column `TIMESTAMPTZ` format override can't reach list elements, so any array of
        // `timestamptz` is rejected instead of silently storing shifted instants.
        for doris_type in ["ARRAY<TIMESTAMPTZ(6)>", "ARRAY<DATETIME(6)>"] {
            assert!(
                DorisSink::check_and_correct_column_type(
                    &DataType::Timestamptz.list(),
                    doris_type.to_owned()
                )
                .is_err(),
                "{doris_type} should be rejected"
            );
        }
        // Nesting one level deeper must be rejected too.
        assert!(
            DorisSink::check_and_correct_column_type(
                &DataType::Timestamptz.list().list(),
                "ARRAY<ARRAY<TIMESTAMPTZ(6)>>".to_owned()
            )
            .is_err()
        );
        // A `STRUCT` field is encoded through its own (nested) name, which is never in
        // `tstz_target_columns`, so it shifts silently in exactly the same way.
        let struct_with_tstz = DataType::Struct(StructType::new([
            ("a", DataType::Int32),
            ("ts", DataType::Timestamptz),
        ]));
        assert!(
            DorisSink::check_and_correct_column_type(
                &struct_with_tstz,
                "STRUCT<a:INT,ts:TIMESTAMPTZ(6)>".to_owned()
            )
            .is_err()
        );
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
        let sink = DorisSink::new(config, upsert_schema(), vec![0], false).unwrap();
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
        assert_eq!(config.max_batch_size_bytes, 32 * 1024 * 1024);
        assert!(config.strict_mode);
        assert_eq!(config.stream_load_http_timeout_ms, 60 * 1000);
        assert!(!config.common.auto_create);
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
        let res: DorisInsertResultResponse = serde_json::from_slice(&raw)
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;

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
