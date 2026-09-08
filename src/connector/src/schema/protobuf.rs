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

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Context as _;
use google_cloud_googleapis::pubsub::v1::schema_service_client::SchemaServiceClient;
use google_cloud_googleapis::pubsub::v1::{GetSchemaRequest, SchemaView};
use google_cloud_pubsub::client::google_cloud_auth::credentials::CredentialsFile;
use google_cloud_pubsub::client::google_cloud_auth::project::Config as AuthConfig;
use google_cloud_pubsub::client::google_cloud_auth::token::DefaultTokenSourceProvider;
use prost_reflect::{DescriptorPool, FileDescriptor, MessageDescriptor};
use prost_types::FileDescriptorSet;
use risingwave_connector_codec::common::protobuf::compile_pb;
use token_source::TokenSourceProvider;

use super::loader::{LoadedSchema, SchemaLoader};
use super::schema_registry::Subject;
use super::{
    InvalidOptionError, MESSAGE_NAME_KEY, SCHEMA_LOCATION_KEY, SCHEMA_REGISTRY_KEY,
    SchemaFetchError, invalid_option_error,
};
use crate::connector_common::AwsAuthProps;
use crate::parser::{EncodingProperties, ProtobufParserConfig, ProtobufProperties};

/// `aws_auth_props` is only required when reading `s3://` URL.
pub async fn fetch_descriptor(
    format_options: &BTreeMap<String, String>,
    topic: &str,
    aws_auth_props: Option<&AwsAuthProps>,
) -> Result<(MessageDescriptor, Option<i32>), SchemaFetchError> {
    let message_name = format_options
        .get(MESSAGE_NAME_KEY)
        .ok_or_else(|| invalid_option_error!("{MESSAGE_NAME_KEY} required"))?
        .clone();
    let schema_location = format_options.get(SCHEMA_LOCATION_KEY);
    let schema_registry = format_options.get(SCHEMA_REGISTRY_KEY);
    let row_schema_location = match (schema_location, schema_registry) {
        (Some(_), Some(_)) => {
            return Err(invalid_option_error!(
                "cannot use {SCHEMA_LOCATION_KEY} and {SCHEMA_REGISTRY_KEY} together"
            )
            .into());
        }
        (None, None) => {
            return Err(invalid_option_error!(
                "requires one of {SCHEMA_LOCATION_KEY} or {SCHEMA_REGISTRY_KEY}"
            )
            .into());
        }
        (None, Some(_)) => {
            let (md, sid) = fetch_from_registry(&message_name, format_options, topic).await?;
            return Ok((md, Some(sid)));
        }
        (Some(url), None) => url.clone(),
    };

    if row_schema_location.starts_with("s3") && aws_auth_props.is_none() {
        return Err(invalid_option_error!("s3 URL not supported yet").into());
    }

    let enc = EncodingProperties::Protobuf(ProtobufProperties {
        schema_location: crate::parser::SchemaLocation::File {
            url: row_schema_location,
            aws_auth_props: aws_auth_props.cloned(),
        },
        message_name,
        // name_strategy, topic, key_message_name, enable_upsert, client_config
        ..Default::default()
    });
    // Ideally, we should extract the schema loading logic from source parser to this place,
    // and call this in both source and sink.
    // But right now this function calls into source parser for its schema loading functionality.
    // This reversed dependency will be fixed when we support schema registry.
    let conf = ProtobufParserConfig::new(enc)
        .await
        .map_err(SchemaFetchError::YetToMigrate)?;
    Ok((conf.message_descriptor, None))
}

pub async fn fetch_from_registry(
    message_name: &str,
    format_options: &BTreeMap<String, String>,
    topic: &str,
) -> Result<(MessageDescriptor, i32), SchemaFetchError> {
    let loader = SchemaLoader::from_format_options(topic, format_options).await?;

    let (vid, vpb) = loader.load_val_schema::<FileDescriptor>().await?;
    let vid = match vid {
        super::SchemaVersion::Confluent(vid) => vid,
        super::SchemaVersion::Glue(_) => {
            return Err(
                invalid_option_error!("Protobuf with Glue Schema Registry unsupported").into(),
            );
        }
    };
    let message_descriptor = vpb
        .parent_pool()
        .get_message_by_name(message_name)
        .ok_or_else(|| invalid_option_error!("message {message_name} not defined in proto"))?;

    Ok((message_descriptor, vid))
}

/// Fetches a schema's raw `.proto` definition directly from GCP Pub/Sub's own Schema resource
/// (`projects/{project}/schemas/{schema}`), and compiles it the same way the schema-registry
/// path does. `credentials` is a JSON string containing service account credentials (see
/// `PubsubProperties::credentials`); `emulator_host` overrides the endpoint for local testing.
/// Note: this deliberately builds its own gRPC channel instead of using
/// `google_cloud_pubsub::apiv1::conn_pool::ConnectionManager` (whose `SchemaClient` wrapper is
/// `pub(crate)` and thus unreachable from outside that crate). TLS uses the standard
/// `with_webpki_roots()` bundle, matching `ConnectionManager`'s own default — correct for a real
/// deployment, where the base image installs `ca-certificates`. For local development behind a
/// TLS-inspecting corporate proxy, set `RW_PUBSUB_DEV_EXTRA_CA_CERT` to a PEM file containing the
/// proxy's CA (layered on top of the standard bundle, not replacing it) — analogous to pointing
/// `SSL_CERT_FILE`/`REQUESTS_CA_BUNDLE`/`GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` at a combined bundle
/// for Python clients hitting the same proxy. Never read in a way that could affect production:
/// unset, this is a no-op and behavior is identical to the plain webpki bundle.
/// Matches `sink/big_query.rs`'s `CONNECT_TIMEOUT` for a comparable GCP client.
const PUBSUB_SCHEMA_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn fetch_from_pubsub_native(
    schema_name: &str,
    emulator_host: Option<&str>,
    credentials: Option<&str>,
) -> Result<FileDescriptor, SchemaFetchError> {
    let to_err = |e: anyhow::Error| SchemaFetchError::PubsubApi(e.into());

    let (channel, token) = if let Some(emulator_host) = emulator_host {
        let channel = tonic_014::transport::Channel::from_shared(format!("http://{emulator_host}"))
            .map_err(|e| to_err(anyhow::anyhow!(e)))?
            .connect_timeout(PUBSUB_SCHEMA_TIMEOUT)
            .timeout(PUBSUB_SCHEMA_TIMEOUT)
            .connect()
            .await
            .map_err(|e| to_err(anyhow::anyhow!(e)))?;
        (channel, None)
    } else {
        let mut tls_config = tonic_014::transport::ClientTlsConfig::new().with_webpki_roots();
        if let Ok(extra_ca_path) = std::env::var("RW_PUBSUB_DEV_EXTRA_CA_CERT") {
            let pem = std::fs::read(&extra_ca_path).map_err(|e| to_err(anyhow::anyhow!(e)))?;
            tls_config = tls_config.ca_certificate(tonic_014::transport::Certificate::from_pem(pem));
        }
        let channel = tonic_014::transport::Endpoint::from_static("https://pubsub.googleapis.com")
            .tls_config(tls_config)
            .map_err(|e| to_err(anyhow::anyhow!(e)))?
            .connect_timeout(PUBSUB_SCHEMA_TIMEOUT)
            .timeout(PUBSUB_SCHEMA_TIMEOUT)
            .connect()
            .await
            .map_err(|e| to_err(anyhow::anyhow!(e)))?;

        let auth_config = AuthConfig::default()
            .with_audience("https://pubsub.googleapis.com/")
            .with_scopes(&["https://www.googleapis.com/auth/cloud-platform"]);
        let provider = match credentials {
            Some(json) => {
                let file = CredentialsFile::new_from_str(json)
                    .await
                    .map_err(|e| to_err(anyhow::anyhow!(e)))?;
                DefaultTokenSourceProvider::new_with_credentials(auth_config, Box::new(file))
                    .await
                    .map_err(|e| to_err(anyhow::anyhow!(e)))?
            }
            None => DefaultTokenSourceProvider::new(auth_config)
                .await
                .map_err(|e| to_err(anyhow::anyhow!(e)))?,
        };
        let token = provider
            .token_source()
            .token()
            .await
            .map_err(|e| to_err(anyhow::anyhow!("{e}")))?;
        (channel, Some(token))
    };

    let interceptor = move |mut req: tonic_014::Request<()>| {
        if let Some(token) = &token {
            let value = token
                .parse()
                .map_err(|e| tonic_014::Status::internal(format!("invalid auth token: {e}")))?;
            req.metadata_mut().insert("authorization", value);
        }
        Ok(req)
    };
    let schema = SchemaServiceClient::with_interceptor(channel, interceptor)
        .get_schema(GetSchemaRequest {
            name: schema_name.to_owned(),
            view: SchemaView::Full as i32,
        })
        .await
        .map_err(|e| to_err(anyhow::anyhow!(e)))?
        .into_inner();

    let fd_set = compile_pb((schema_name.to_owned(), schema.definition), std::iter::empty())
        .map_err(|e| SchemaFetchError::SchemaCompile(e.into()))?;
    DescriptorPool::from_file_descriptor_set(fd_set)
        .context("failed to convert fd set to descriptor pool")
        .and_then(|pool| {
            pool.get_file_by_name(schema_name)
                .context("file lost after compilation")
        })
        .map_err(|e| SchemaFetchError::SchemaCompile(e.into()))
}

impl LoadedSchema for FileDescriptor {
    fn compile(primary: Subject, references: Vec<Subject>) -> Result<Self, SchemaFetchError> {
        let primary_name = primary.name.clone();

        match compile_pb_subject(primary, references)
            .context("failed to compile protobuf schema into fd set")
        {
            Err(e) => Err(SchemaFetchError::SchemaCompile(e.into())),
            Ok(fd_set) => DescriptorPool::from_file_descriptor_set(fd_set)
                .context("failed to convert fd set to descriptor pool")
                .and_then(|pool| {
                    pool.get_file_by_name(&primary_name)
                        .context("file lost after compilation")
                })
                .map_err(|e| SchemaFetchError::SchemaCompile(e.into())),
        }
    }
}

fn compile_pb_subject(
    primary_subject: Subject,
    dependency_subjects: Vec<Subject>,
) -> Result<FileDescriptorSet, SchemaFetchError> {
    compile_pb(
        (primary_subject.name.clone(), primary_subject.schema.content),
        dependency_subjects
            .into_iter()
            .map(|s| (s.name.clone(), s.schema.content)),
    )
    .map_err(|e| SchemaFetchError::SchemaCompile(e.into()))
}
