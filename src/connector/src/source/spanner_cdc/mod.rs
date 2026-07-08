// Copyright 2022 RisingWave Labs
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

use std::collections::HashMap;

use google_cloud_spanner::client::DatabaseClient;
use phf::{Set, phf_set};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::source::SourceProperties;

pub mod enumerator;
pub mod schema_track;
pub mod source;
pub mod split;
pub mod types;

#[cfg(test)]
mod tests;

pub use enumerator::*;
pub use source::*;
pub use split::*;

pub const SPANNER_CDC_CONNECTOR: &str = "spanner-cdc";

/// Spanner CDC source properties
///
/// # Implementation Notes
/// Spanner CDC reads from change streams which track all changes to tables.
/// The source manages partition tokens internally to handle partition splits/merges.
/// Change streams can watch multiple tables, so table filtering is done at the reader level.
#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct SpannerCdcProperties {
    /// Google Cloud project ID
    #[serde(rename = "spanner.project")]
    pub project: String,

    /// Spanner instance ID
    #[serde(rename = "spanner.instance")]
    pub instance: String,

    /// Spanner database name (uses standard `database.name` key for consistency with other CDC sources)
    #[serde(rename = "database.name")]
    pub database: String,

    /// Name of the change stream to read from
    #[serde(rename = "spanner.change_stream.name")]
    pub change_stream_name: String,

    /// Table name to filter change records (optional for shared source)
    /// When creating a table FROM a source, this specifies which upstream table to track
    #[serde(rename = "table.name")]
    pub table_name: Option<String>,

    /// Heartbeat interval for partition health monitoring, in milliseconds (default: 2000).
    /// Maps directly to the `heartbeat_milliseconds` argument of the
    /// `READ_<change_stream_name>` TVF. Valid range per Spanner: 1,000–300,000.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(rename = "spanner.heartbeat_milliseconds")]
    #[serde(default = "default_heartbeat_milliseconds")]
    #[with_option(allow_alter_on_fly)]
    pub heartbeat_milliseconds: i64,

    /// GCP credentials JSON string
    /// See: https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account
    #[serde(rename = "spanner.credentials")]
    pub credentials: Option<String>,

    /// Path to GCP service account credentials file
    /// Alternative to `spanner.credentials` for file-based credentials
    /// See: https://cloud.google.com/docs/authentication/provide-credentials-adc
    #[serde(rename = "spanner.credentials_path")]
    pub credentials_path: Option<String>,

    /// Use Spanner emulator for testing
    #[serde(rename = "spanner.emulator_host")]
    pub emulator_host: Option<String>,

    /// Retry attempts for transient failures (default: 5)
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "spanner.retry_attempts")]
    #[with_option(allow_alter_on_fly)]
    pub retry_attempts: Option<u32>,

    /// Retry backoff in milliseconds (default: 1000)
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "spanner.retry_backoff_ms")]
    #[with_option(allow_alter_on_fly)]
    pub retry_backoff_ms: Option<u64>,

    /// Retry backoff max delay in milliseconds (default: 10000)
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "spanner.retry_backoff_max_delay_ms")]
    #[with_option(allow_alter_on_fly)]
    pub retry_backoff_max_delay_ms: Option<u64>,

    /// Retry backoff factor (default: 2, meaning double each time)
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "spanner.retry_backoff_factor")]
    #[with_option(allow_alter_on_fly)]
    pub retry_backoff_factor: Option<u64>,

    /// Maximum consecutive missed heartbeats before a partition stream is
    /// considered stalled and restarted (default: 10, matching the Spanner
    /// Kafka connector's `connector.spanner.max.missed.heartbeats`).
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "spanner.max_missed_heartbeats")]
    #[with_option(allow_alter_on_fly)]
    pub max_missed_heartbeats: Option<u32>,

    /// Start timestamp for the change stream query (RFC3339 format)
    #[serde(rename = "spanner.start_timestamp", default)]
    #[serde(deserialize_with = "crate::deserialize_i64_from_string_opt")]
    pub start_ts: Option<i64>,

    // ---------------------------------------------------------------------------
    // Fields below are NOT read by the Spanner CDC connector itself.
    //
    // They exist because the meta service deserialises the combined
    // source + table properties (built by `derive_with_options_for_cdc_table`
    // in the frontend) into this struct via `ConnectorProperties::extract`.
    // If a key present in the combined map has no matching serde field here,
    // it lands in `unknown_fields` and gets rejected when
    // `deny_unknown_fields = true`.
    //
    // Table-level options are set at CREATE TABLE time and injected into
    // `CdcTableDesc.connect_properties` by `derive_with_options_for_cdc_table`.
    //
    // DO NOT REMOVE — removing any of these will break CREATE SOURCE or
    // CREATE TABLE for Spanner CDC.
    // ---------------------------------------------------------------------------
    /// Auto schema change flag
    #[serde(rename = "auto.schema.change", default)]
    #[serde(skip_serializing)]
    _auto_schema_change: Option<String>,

    /// Enable databoost for Spanner CDC backfill
    #[serde(rename = "spanner.databoost.enabled", default)]
    #[serde(skip_serializing)]
    _databoost_enabled: Option<String>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

fn default_heartbeat_milliseconds() -> i64 {
    2000
}

impl EnforceSecret for SpannerCdcProperties {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "spanner.credentials",
        "spanner.credentials_path",
    };
}

impl SourceProperties for SpannerCdcProperties {
    type Split = SpannerCdcSplit;
    type SplitEnumerator = SpannerCdcSplitEnumerator;
    type SplitReader = SpannerCdcSplitReader;

    const SOURCE_NAME: &'static str = SPANNER_CDC_CONNECTOR;
}

impl crate::source::UnknownFields for SpannerCdcProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl SpannerCdcProperties {
    /// Get retry attempts (default: 5)
    pub fn get_retry_attempts(&self) -> u32 {
        self.retry_attempts.unwrap_or(5)
    }

    /// Get retry backoff duration
    pub fn get_retry_backoff(&self) -> std::time::Duration {
        let ms = self.retry_backoff_ms.unwrap_or(1000);
        std::time::Duration::from_millis(ms)
    }

    /// Get retry backoff max delay in milliseconds (default: 10000)
    pub fn get_retry_backoff_max_delay_ms(&self) -> u64 {
        self.retry_backoff_max_delay_ms.unwrap_or(10000)
    }

    /// Get retry backoff factor (default: 2, meaning double each time)
    pub fn get_retry_backoff_factor(&self) -> u64 {
        self.retry_backoff_factor.unwrap_or(2)
    }

    /// Stall timeout = heartbeat_interval * max_missed_heartbeats.
    /// Matches the Spanner Kafka connector's default (10 missed heartbeats).
    pub fn get_stall_timeout(&self) -> std::time::Duration {
        let heartbeat_ms = self.heartbeat_milliseconds.max(1000) as u64;
        let max_missed = self.max_missed_heartbeats.unwrap_or(10).max(1) as u64;
        std::time::Duration::from_millis(heartbeat_ms * max_missed)
    }

    /// Create a Spanner `DatabaseClient` using the shared factory.
    pub(crate) async fn create_client(&self) -> ConnectorResult<DatabaseClient> {
        crate::source::cdc::external::spanner::create_spanner_client(
            &self.project,
            &self.instance,
            &self.database,
            self.emulator_host.as_deref(),
            self.credentials.as_deref(),
            self.credentials_path.as_deref(),
        )
        .await
    }
}
