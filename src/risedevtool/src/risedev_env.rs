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

#![allow(clippy::doc_markdown)] // RiseDev

use std::fmt::Write;
use std::process::Command;

use crate::{Application, HummockInMemoryStrategy, ServiceConfig, add_hummock_backend};

/// Generate environment variables (put in file `.risingwave/config/risedev-env`)
/// from the given service configurations to be used by future
/// RiseDev commands, like `risedev ctl` or `risedev psql` ().
pub fn generate_risedev_env(services: &Vec<ServiceConfig>) -> String {
    generate_risedev_env_with_profile(services, &Vec::new())
}

/// Generate environment variables with profile-level env vars included.
/// Profile env vars are written first, followed by service-specific env vars.
pub fn generate_risedev_env_with_profile(services: &Vec<ServiceConfig>, profile_env: &Vec<String>) -> String {
    let mut env = String::new();

    // Track Spanner-related env vars from profile
    let mut spanner_project: Option<String> = None;
    let mut spanner_instance: Option<String> = None;
    let mut spanner_database: Option<String> = None;
    let mut spanner_emulator_host: Option<String> = None;

    // Write profile-level env vars first (these can be overridden by services)
    // Also extract Spanner-related values for generating RISEDEV_SPANNER_WITH_OPTIONS_COMMON
    for e in profile_env {
        writeln!(env, "{e}").unwrap();
        // Extract SPANNER_* values
        if let Some((key, value)) = e.split_once('=') {
            match key {
                "SPANNER_PROJECT" => spanner_project = Some(value.to_string()),
                "SPANNER_INSTANCE" => spanner_instance = Some(value.to_string()),
                "SPANNER_DATABASE" => spanner_database = Some(value.to_string()),
                "SPANNER_EMULATOR_HOST" => spanner_emulator_host = Some(value.to_string()),
                _ => {}
            }
        }
    }

    // If profile has Spanner env vars but no Spanner service, generate RISEDEV_SPANNER_WITH_OPTIONS_COMMON
    let has_spanner_service = services.iter().any(|s| matches!(s, ServiceConfig::Spanner(_)));
    if spanner_project.is_some() && !has_spanner_service {
        let project = spanner_project.unwrap_or_else(|| "test-project".to_string());
        let instance = spanner_instance.unwrap_or_else(|| "test-instance".to_string());
        let database = spanner_database.unwrap_or_else(|| "test-database".to_string());

        if let Some(emulator_host) = spanner_emulator_host {
            // Emulator mode
            writeln!(env, r#"SPANNER_EMULATOR_HOST="{}""#, emulator_host).unwrap();
            writeln!(env, r#"RISEDEV_SPANNER_WITH_OPTIONS_COMMON="connector='spanner-cdc',spanner.emulator_host='{}',spanner.project='{}',spanner.instance='{}',database.name='{}'""#,
                emulator_host, project, instance, database).unwrap();
        } else {
            // Real Spanner mode - no emulator_host
            writeln!(env, r#"RISEDEV_SPANNER_WITH_OPTIONS_COMMON="connector='spanner-cdc',spanner.project='{}',spanner.instance='{}',database.name='{}'""#,
                project, instance, database).unwrap();
        }
    }
    for item in services {
        match item {
            ServiceConfig::ComputeNode(c) => {
                // RW_HUMMOCK_URL
                // If the cluster is launched without a shared storage, we will skip this.
                {
                    let mut cmd = Command::new("compute-node");
                    if add_hummock_backend(
                        "dummy",
                        c.provide_opendal.as_ref().unwrap(),
                        c.provide_minio.as_ref().unwrap(),
                        c.provide_aws_s3.as_ref().unwrap(),
                        c.provide_moat.as_ref().unwrap(),
                        HummockInMemoryStrategy::Disallowed,
                        &mut cmd,
                    )
                    .is_ok()
                    {
                        writeln!(
                            env,
                            "RW_HUMMOCK_URL=\"{}\"",
                            cmd.get_args().nth(1).unwrap().to_str().unwrap()
                        )
                        .unwrap();
                    }
                }

                // RW_META_ADDR
                {
                    let meta_node = &c.provide_meta_node.as_ref().unwrap()[0];
                    writeln!(
                        env,
                        "RW_META_ADDR=\"http://{}:{}\"",
                        meta_node.address, meta_node.port
                    )
                    .unwrap();
                }
            }
            ServiceConfig::Frontend(c) => {
                let listen_address = &c.listen_address;
                writeln!(
                    env,
                    "RISEDEV_RW_FRONTEND_LISTEN_ADDRESS=\"{listen_address}\"",
                )
                .unwrap();
                let port = &c.port;
                writeln!(env, "RISEDEV_RW_FRONTEND_PORT=\"{port}\"",).unwrap();
            }
            ServiceConfig::Kafka(c) => {
                let brokers = format!("{}:{}", c.address, c.port);
                writeln!(env, r#"RISEDEV_KAFKA_BOOTSTRAP_SERVERS="{brokers}""#,).unwrap();
                writeln!(env, r#"RISEDEV_KAFKA_WITH_OPTIONS_COMMON="connector='kafka',properties.bootstrap.server='{brokers}'""#).unwrap();
                writeln!(env, r#"RPK_BROKERS="{brokers}""#).unwrap();
            }
            ServiceConfig::SchemaRegistry(c) => {
                let url = format!("http://{}:{}", c.address, c.port);
                writeln!(env, r#"RISEDEV_SCHEMA_REGISTRY_URL="{url}""#,).unwrap();
                writeln!(env, r#"RPK_REGISTRY_HOSTS="{url}""#).unwrap();
            }
            ServiceConfig::Pulsar(c) => {
                // These 2 names are NOT defined by Pulsar, but by us.
                // The `pulsar-admin` CLI uses a `PULSAR_CLIENT_CONF` file with `brokerServiceUrl` and `webServiceUrl`
                // It may be used by our upcoming `PulsarCat` #21401
                writeln!(
                    env,
                    r#"PULSAR_BROKER_URL="pulsar://{}:{}""#,
                    c.address, c.broker_port
                )
                .unwrap();
                writeln!(
                    env,
                    r#"PULSAR_HTTP_URL="http://{}:{}""#,
                    c.address, c.http_port
                )
                .unwrap();
            }
            ServiceConfig::MySql(c) if c.application != Application::Metastore => {
                let host = &c.address;
                let port = &c.port;
                let user = &c.user;
                let password = &c.password;
                // These envs are used by `mysql` cli.
                writeln!(env, r#"MYSQL_HOST="{host}""#,).unwrap();
                writeln!(env, r#"MYSQL_TCP_PORT="{port}""#,).unwrap();
                // Note: There's no env var for the username read by `mysql` cli. Here we set
                // `RISEDEV_MYSQL_USER`, which will be read by `e2e_test/commands/mysql` when
                // running `risedev slt`, as a wrapper of `mysql` cli.
                writeln!(env, r#"RISEDEV_MYSQL_USER="{user}""#,).unwrap();
                writeln!(env, r#"MYSQL_PWD="{password}""#,).unwrap();
                // Note: user and password are not included in the common WITH options.
                // It's expected to create another dedicated user for the source.
                writeln!(env, r#"RISEDEV_MYSQL_WITH_OPTIONS_COMMON="connector='mysql-cdc',hostname='{host}',port='{port}'""#,).unwrap();
            }
            ServiceConfig::Pubsub(c) => {
                let address = &c.address;
                let port = &c.port;
                writeln!(env, r#"PUBSUB_EMULATOR_HOST="{address}:{port}""#,).unwrap();
                writeln!(env, r#"RISEDEV_PUBSUB_WITH_OPTIONS_COMMON="connector='google_pubsub',pubsub.emulator_host='{address}:{port}'""#,).unwrap();
            }
            ServiceConfig::Spanner(c) => {
                let address = &c.address;
                let port = &c.port;
                writeln!(env, r#"SPANNER_EMULATOR_HOST="{address}:{port}""#,).unwrap();
                // Default values used by tests - can be overridden with real Spanner values
                writeln!(env, r#"SPANNER_PROJECT="test-project""#,).unwrap();
                writeln!(env, r#"SPANNER_INSTANCE="test-instance""#,).unwrap();
                writeln!(env, r#"SPANNER_DATABASE="test-database""#,).unwrap();
                writeln!(env, r#"RISEDEV_SPANNER_WITH_OPTIONS_COMMON="connector='spanner-cdc',spanner.emulator_host='{address}:{port}',spanner.project='test-project',spanner.instance='test-instance',database.name='test-database'""#,).unwrap();
            }
            ServiceConfig::Postgres(c) => {
                let host = &c.address;
                let port = &c.port;
                let user = &c.user;
                let password = &c.password;
                let database = &c.database;
                // These envs are used by `postgres` cli.
                writeln!(env, r#"PGHOST="{host}""#,).unwrap();
                writeln!(env, r#"PGPORT="{port}""#,).unwrap();
                writeln!(env, r#"PGUSER="{user}""#,).unwrap();
                writeln!(env, r#"PGPASSWORD="{password}""#,).unwrap();
                writeln!(env, r#"PGDATABASE="{database}""#,).unwrap();
                writeln!(
                    env,
                    r#"RISEDEV_POSTGRES_WITH_OPTIONS_COMMON="connector='postgres-cdc',hostname='{host}',port='{port}'""#,
                )
                .unwrap();
            }
            ServiceConfig::SqlServer(c) => {
                let host = &c.address;
                let port = &c.port;
                let user = &c.user;
                let password = &c.password;
                let database = &c.database;
                // These envs are used by `sqlcmd`.
                writeln!(env, r#"SQLCMDSERVER="{host}""#,).unwrap();
                writeln!(env, r#"SQLCMDPORT="{port}""#,).unwrap();
                writeln!(env, r#"SQLCMDUSER="{user}""#,).unwrap();
                writeln!(env, r#"SQLCMDPASSWORD="{password}""#,).unwrap();
                writeln!(env, r#"SQLCMDDBNAME="{database}""#,).unwrap();
                writeln!(
                    env,
                    r#"RISEDEV_SQLSERVER_WITH_OPTIONS_COMMON="connector='sqlserver-cdc',hostname='{host}',port='{port}',username='{user}',password='{password}',database.name='{database}'""#,
                )
                .unwrap();
            }
            ServiceConfig::MetaNode(meta_node_config) => {
                writeln!(
                    env,
                    r#"RISEDEV_RW_META_DASHBOARD_ADDR="http://{}:{}""#,
                    meta_node_config.address, meta_node_config.dashboard_port
                )
                .unwrap();
            }
            ServiceConfig::Lakekeeper(c) => {
                let base_url = format!("http://{}:{}", c.address, c.port);
                let catalog_url = format!("{}/catalog", base_url);
                writeln!(env, r#"RISEDEV_LAKEKEEPER_URL="{base_url}""#,).unwrap();
                writeln!(env, r#"LAKEKEEPER_CATALOG_URL="{catalog_url}""#,).unwrap();
                writeln!(env, r#"RISEDEV_LAKEKEEPER_WITH_OPTIONS_COMMON="connector='iceberg',catalog.type='rest',catalog.uri='{catalog_url}'""#,).unwrap();
            }
            _ => {}
        }
    }
    env
}
