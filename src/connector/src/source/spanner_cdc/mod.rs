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

use anyhow::Context;
use google_cloud_spanner::client::{Client, ClientConfig};
use phf::{Set, phf_set};
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use time::OffsetDateTime;
use with_options::WithOptions;

use crate::enforce_secret::EnforceSecret;
use crate::error::ConnectorResult;
use crate::source::SourceProperties;

pub mod enumerator;
pub mod partition_manager;
pub mod schema_track;
pub mod source;
pub mod split;
pub mod types;

#[cfg(test)]
mod tests;


pub use enumerator::*;
pub use partition_manager::*;
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

    /// Spanner database name
    #[serde(rename = "spanner.database")]
    pub database: String,

    /// Name of the change stream to read from
    #[serde(rename = "spanner.stream_name")]
    pub stream_name: String,

    /// Table name to filter change records (optional for shared source)
    /// When creating a table FROM a source, this specifies which upstream table to track
    #[serde(rename = "table.name")]
    pub table_name: Option<String>,

    /// Heartbeat interval for partition health monitoring (default: 3s)
    #[serde(rename = "spanner.heartbeat_interval")]
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: String,

    /// Start time for reading change stream (RFC3339 format)
    /// If not specified, starts from current time
    #[serde(rename = "spanner.start_time")]
    pub start_time: Option<String>,

    /// End time for reading change stream (RFC3339 format)
    /// If not specified, reads indefinitely
    #[serde(rename = "spanner.end_time")]
    pub end_time: Option<String>,

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

    /// Number of parallel readers (default: 1)
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "spanner.parallelism")]
    pub parallelism: Option<u32>,

    /// Maximum number of concurrent partitions to read (default: 100)
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "spanner.max_concurrent_partitions")]
    pub max_concurrent_partitions: Option<u32>,

    /// Buffer size for prefetching messages (default: 1024)
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "spanner.buffer_size")]
    pub buffer_size: Option<usize>,

    /// Enable automatic partition discovery and spawning (default: true)
    #[serde(rename = "spanner.enable_partition_discovery")]
    #[serde(default = "default_enable_partition_discovery")]
    pub enable_partition_discovery: bool,

    /// Retry attempts for transient failures (default: 3)
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "spanner.retry_attempts")]
    pub retry_attempts: Option<u32>,

    /// Retry backoff in milliseconds (default: 1000)
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "spanner.retry_backoff_ms")]
    pub retry_backoff_ms: Option<u64>,

    /// Retry backoff max delay in milliseconds (default: 10000)
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "spanner.retry_backoff_max_delay_ms")]
    pub retry_backoff_max_delay_ms: Option<u64>,

    /// Retry backoff factor (default: 2, meaning double each time)
    #[serde_as(as = "Option<DisplayFromStr>")]
    #[serde(rename = "spanner.retry_backoff_factor")]
    pub retry_backoff_factor: Option<u64>,

    /// Enable automatic schema change (for CDC sources)
    /// When enabled, schema changes detected at the source level will be propagated to the table
    #[serde(rename = "auto.schema.change")]
    #[serde(default, deserialize_with = "crate::deserialize_bool_from_string")]
    pub auto_schema_change: bool,

    /// Enable databoost for snapshot backfill (default: false)
    /// When enabled, uses Spanner's databoost feature to avoid throttling during snapshot reads
    #[serde(rename = "spanner.enable_databoost")]
    #[serde(default, deserialize_with = "crate::deserialize_bool_from_string")]
    pub enable_databoost: bool,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

fn default_enable_partition_discovery() -> bool {
    true
}

fn default_heartbeat_interval() -> String {
    "3s".to_string()
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
    /// Get max concurrent partitions (default: 100)
    pub fn get_max_concurrent_partitions(&self) -> usize {
        self.max_concurrent_partitions.unwrap_or(100) as usize
    }

    /// Get buffer size (default: 1024)
    pub fn get_buffer_size(&self) -> usize {
        self.buffer_size.unwrap_or(1024)
    }

    /// Get retry attempts (default: 3)
    pub fn get_retry_attempts(&self) -> u32 {
        self.retry_attempts.unwrap_or(3)
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

    /// Parse heartbeat interval as duration in milliseconds
    pub fn heartbeat_interval_ms(&self) -> ConnectorResult<i64> {
        let duration = duration_str::parse(&self.heartbeat_interval)
            .map_err(|e| anyhow::anyhow!("failed to parse heartbeat_interval: {}", e))?;
        Ok(duration.as_millis() as i64)
    }

    /// Parse start time as OffsetDateTime
    pub fn parse_start_time(&self) -> ConnectorResult<Option<OffsetDateTime>> {
        if let Some(ref start_time) = self.start_time {
            let dt =
                OffsetDateTime::parse(start_time, &time::format_description::well_known::Rfc3339)
                    .context("failed to parse start_time")?;
            Ok(Some(dt))
        } else {
            Ok(None)
        }
    }

    /// Parse end time as OffsetDateTime
    pub fn parse_end_time(&self) -> ConnectorResult<Option<OffsetDateTime>> {
        if let Some(ref end_time) = self.end_time {
            let dt =
                OffsetDateTime::parse(end_time, &time::format_description::well_known::Rfc3339)
                    .context("failed to parse end_time")?;
            Ok(Some(dt))
        } else {
            Ok(None)
        }
    }

    /// Create a Spanner client
    pub(crate) async fn create_client(&self) -> ConnectorResult<Client> {
        // Construct DSN from project, instance, database
        let dsn = format!(
            "projects/{}/instances/{}/databases/{}",
            self.project, self.instance, self.database
        );

        // Set environment variables for authentication
        // Priority: credentials_path > credentials > ADC (none set)
        if let Some(ref emulator_host) = self.emulator_host {
            // Safety: only read in the same thread below in with_auth
            unsafe { std::env::set_var("SPANNER_EMULATOR_HOST", emulator_host) };
        }
        if let Some(ref credentials_path) = self.credentials_path {
            // Read credentials from file and set as JSON
            let credentials_content = std::fs::read_to_string(credentials_path)
                .with_context(|| format!("failed to read credentials file: {}", credentials_path))?;
            // Safety: only read in the same thread below in with_auth
            unsafe { std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS_JSON", credentials_content) };
        } else if let Some(ref credentials) = self.credentials {
            // Safety: only read in the same thread below in with_auth
            unsafe { std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS_JSON", credentials) };
        }
        // If neither credentials nor credentials_path is set, ADC will be used automatically

        // Create client configuration
        let config = ClientConfig::default()
            .with_auth()
            .await
            .context("failed to create Spanner client config")?;

        let client = Client::new(&dsn, config)
            .await
            .context("failed to create Spanner client")?;

        Ok(client)
    }

    /// Get the DSN string for this connection (used for external table config)
    pub(crate) fn get_dsn(&self) -> String {
        format!(
            "projects/{}/instances/{}/databases/{}",
            self.project, self.instance, self.database
        )
    }
}
