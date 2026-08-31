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

use anyhow::{Context, anyhow};
use opendal::Operator;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::Gcs;
use url::Url;

use crate::error::ConnectorResult;

/// Split a `gs://bucket/path/to/file` url into its bucket and object key.
fn parse_gcs_url(location: &Url) -> ConnectorResult<(&str, &str)> {
    let bucket = location
        .host_str()
        .with_context(|| format!("illegal file path {}", location))?;
    let key = location
        .path()
        .strip_prefix('/')
        .ok_or_else(|| anyhow!("gcs url {location} should have a '/' at the start of path."))?;
    Ok((bucket, key))
}

/// Load a schema file from Google Cloud Storage.
///
/// Location format: `gs://bucket_name/path/to/file` (`gcs://` is also accepted).
///
/// Authentication uses Application Default Credentials (ADC). No credential is
/// configured explicitly: opendal's `GoogleCredentialLoader` resolves it from
/// `GOOGLE_APPLICATION_CREDENTIALS`, the well-known gcloud config file
/// (`~/.config/gcloud/application_default_credentials.json`), or the GCE/GKE
/// metadata server (i.e. Workload Identity). No HMAC key is required.
pub async fn load_file_descriptor_from_gcs(location: &Url) -> ConnectorResult<Vec<u8>> {
    let (bucket, key) = parse_gcs_url(location)?;

    let builder = Gcs::default().bucket(bucket);

    let op: Operator = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::default())
        .finish();

    let bytes = op
        .read(key)
        .await
        .with_context(|| format!("failed to get file from gcs at `{}`", location))?;

    Ok(bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_gcs_url() {
        let url = Url::parse("gs://my-bucket/path/to/schema.avsc").unwrap();
        assert_eq!(
            parse_gcs_url(&url).unwrap(),
            ("my-bucket", "path/to/schema.avsc")
        );

        // `gcs://` is accepted as well.
        let url = Url::parse("gcs://my-bucket/schema.proto").unwrap();
        assert_eq!(parse_gcs_url(&url).unwrap(), ("my-bucket", "schema.proto"));

        // Bucket names may contain dots.
        let url = Url::parse("gs://my.bucket.example/a/b.avsc").unwrap();
        assert_eq!(
            parse_gcs_url(&url).unwrap(),
            ("my.bucket.example", "a/b.avsc")
        );
    }

    /// Fetch a real object from GCS using Application Default Credentials.
    ///
    /// Set `RW_TEST_GCS_URL` to a readable `gs://bucket/object` and run with:
    /// `cargo test -p risingwave_connector --lib gcs_utils -- --ignored`
    ///
    /// Note that ADC here must resolve to a service account, an impersonated
    /// service account, an external account, or a metadata server. Plain user
    /// credentials from `gcloud auth application-default login` are of type
    /// `authorized_user`, which opendal's credential loader does not support.
    #[ignore]
    #[tokio::test]
    async fn test_load_file_descriptor_from_gcs_with_adc() {
        let Ok(raw) = std::env::var("RW_TEST_GCS_URL") else {
            panic!("RW_TEST_GCS_URL must be set to a readable gs:// object");
        };
        let url = Url::parse(&raw).unwrap();

        let bytes = load_file_descriptor_from_gcs(&url).await.unwrap();

        assert!(!bytes.is_empty());
    }
}
