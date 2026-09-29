#!/usr/bin/env -S cargo -Zscript
---cargo
[package]
edition = "2024"

[dependencies]
anyhow = "1"
tokio = { version = "1", features = ["rt", "rt-multi-thread", "macros"] }
google-cloud-spanner = { git = "https://github.com/googleapis/google-cloud-rust", rev = "4855fdd8be0e9c6063d3eac9f7d80bcb71782732" }
google-cloud-spanner-admin-database-v1 = { git = "https://github.com/googleapis/google-cloud-rust", rev = "4855fdd8be0e9c6063d3eac9f7d80bcb71782732" }
google-cloud-lro = { git = "https://github.com/googleapis/google-cloud-rust", rev = "4855fdd8be0e9c6063d3eac9f7d80bcb71782732" }
---

//! Spanner emulator fixtures for `spanner_cdc.slt.serial`.
//!
//! - `setup`: create the instance if needed, and recreate the test database empty.
//! - `create-postgresql-database`: recreate a PostgreSQL-dialect database.
//! - `ddl <SQL>` / `dml <SQL>`: run one statement.
//!
//! The Spanner client finds the emulator through `SPANNER_EMULATOR_HOST`, which
//! `risedev` sets along with `SPANNER_PROJECT`, `SPANNER_INSTANCE` and `SPANNER_DATABASE`.

use std::env;
use std::process::Command;

use anyhow::{Context, bail};
use google_cloud_lro::Poller;
use google_cloud_spanner::client::Spanner;
use google_cloud_spanner::statement::Statement;

/// A PostgreSQL-dialect database, for checking that CREATE SOURCE rejects it.
const POSTGRESQL_DIALECT_DATABASE: &str = "test-postgresql-database";

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_owned())
}

fn project() -> String {
    env_or("SPANNER_PROJECT", "test-project")
}

fn instance() -> String {
    env_or("SPANNER_INSTANCE", "test-instance")
}

fn database() -> String {
    env_or("SPANNER_DATABASE", "test-database")
}

/// Runs `gcloud spanner <args>` against the emulator's REST endpoint, which listens
/// 10 ports above its gRPC port, without touching the global gcloud configuration.
/// A failure whose message contains `tolerated` is treated as success.
fn gcloud_spanner(args: &[&str], tolerated: Option<&str>) -> anyhow::Result<()> {
    let emulator_host = env::var("SPANNER_EMULATOR_HOST")
        .context("SPANNER_EMULATOR_HOST must be set: these fixtures run on the emulator")?;
    let (host, grpc_port) = emulator_host
        .rsplit_once(':')
        .context("SPANNER_EMULATOR_HOST must be host:port")?;
    let rest_port = grpc_port.parse::<u16>()? + 10;

    let output = Command::new("gcloud")
        .arg("spanner")
        .args(args)
        .env("CLOUDSDK_AUTH_DISABLE_CREDENTIALS", "true")
        .env(
            "CLOUDSDK_API_ENDPOINT_OVERRIDES_SPANNER",
            format!("http://{host}:{rest_port}/"),
        )
        .env("CLOUDSDK_CORE_PROJECT", project())
        .output()
        .context("failed to run gcloud")?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success()
        && !tolerated.is_some_and(|message| stderr.to_lowercase().contains(message))
    {
        bail!("gcloud spanner {} failed: {stderr}", args.join(" "));
    }
    Ok(())
}

/// Drops `name` if it exists and creates it again, empty.
fn recreate_database(name: &str, extra_args: &[&str]) -> anyhow::Result<()> {
    let instance = format!("--instance={}", instance());
    gcloud_spanner(
        &["databases", "delete", name, &instance, "--quiet"],
        Some("not found"),
    )?;
    let mut args = vec!["databases", "create", name, &instance];
    args.extend_from_slice(extra_args);
    gcloud_spanner(&args, None)
}

fn database_path() -> String {
    format!(
        "projects/{}/instances/{}/databases/{}",
        project(),
        instance(),
        database()
    )
}

async fn execute_ddl(ddl: String) -> anyhow::Result<()> {
    let admin = Spanner::builder()
        .build()
        .await?
        .database_admin_builder()
        .build()
        .await?;
    admin
        .update_database_ddl()
        .set_database(database_path())
        .set_statements([ddl])
        .poller()
        .until_done()
        .await
        .context("DDL failed")?;
    Ok(())
}

async fn execute_dml(dml: String) -> anyhow::Result<()> {
    let client = Spanner::builder()
        .build()
        .await?
        .database_client(database_path())
        .build()
        .await?;
    client
        .read_write_transaction()
        .build()
        .await?
        .run(async |tx| {
            tx.execute_update(Statement::builder(&dml).build()).await?;
            Ok(())
        })
        .await
        .context("DML failed")?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = env::args().skip(1);
    let command = args.next().context("missing command")?;

    match command.as_str() {
        "setup" => {
            gcloud_spanner(
                &[
                    "instances",
                    "create",
                    &instance(),
                    "--config=emulator-config",
                    "--description=Test Instance",
                    "--nodes=1",
                ],
                Some("already exists"),
            )?;
            recreate_database(&database(), &[])?;
        }
        "create-postgresql-database" => {
            recreate_database(POSTGRESQL_DIALECT_DATABASE, &["--database-dialect=POSTGRESQL"])?;
        }
        "ddl" => execute_ddl(args.next().context("missing SQL")?).await?,
        "dml" => execute_dml(args.next().context("missing SQL")?).await?,
        other => panic!("unknown command {other}"),
    }
    Ok(())
}
