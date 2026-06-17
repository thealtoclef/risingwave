#!/usr/bin/env -S cargo -Zscript
---cargo
edition = "2021"

[dependencies]
anyhow = "1"
tokio = { version = "1", features = ["rt", "rt-multi-thread", "macros", "time"] }
google-cloud-spanner = { git = "https://github.com/googleapis/google-cloud-rust", rev = "d0d2f50f099248d4795e4f4efa32fbac67e28024" }
google-cloud-spanner-admin-database-v1 = { git = "https://github.com/googleapis/google-cloud-rust", rev = "d0d2f50f099248d4795e4f4efa32fbac67e28024" }
google-cloud-auth = { git = "https://github.com/googleapis/google-cloud-rust", rev = "d0d2f50f099248d4795e4f4efa32fbac67e28024" }
google-cloud-lro = { git = "https://github.com/googleapis/google-cloud-rust", rev = "d0d2f50f099248d4795e4f4efa32fbac67e28024" }
---
use std::env;
use std::io::{self, Read};

use anyhow::Context;
use google_cloud_lro::Poller;
use google_cloud_spanner::client::Spanner;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;

fn get_env_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn get_project() -> String {
    get_env_or_default("SPANNER_PROJECT", "test-project")
}
fn get_instance() -> String {
    get_env_or_default("SPANNER_INSTANCE", "test-instance")
}
fn get_database() -> String {
    get_env_or_default("SPANNER_DATABASE", "test-database")
}
const STREAM: &str = "test_stream";

/// Check if we're using emulator (SPANNER_EMULATOR_HOST is set)
fn is_using_emulator() -> bool {
    env::var("SPANNER_EMULATOR_HOST").is_ok()
}

fn database_name() -> String {
    format!(
        "projects/{}/instances/{}/databases/{}",
        get_project(),
        get_instance(),
        get_database()
    )
}

/// Build a `gcloud` command pre-configured for the emulator.
/// Sets env vars so gcloud talks to the emulator's REST endpoint
/// without touching any global gcloud configuration.
fn emulator_gcloud_command() -> std::process::Command {
    let emulator_host = env::var("SPANNER_EMULATOR_HOST")
        .expect("SPANNER_EMULATOR_HOST must be set for emulator mode");
    let (host, grpc_port_str) = emulator_host
        .rsplit_once(':')
        .expect("invalid SPANNER_EMULATOR_HOST format");
    let grpc_port: u16 = grpc_port_str
        .parse()
        .expect("invalid port in SPANNER_EMULATOR_HOST");
    let rest_port = grpc_port + 10;

    let mut cmd = std::process::Command::new("gcloud");
    cmd.env("CLOUDSDK_AUTH_DISABLE_CREDENTIALS", "true")
        .env(
            "CLOUDSDK_API_ENDPOINT_OVERRIDES_SPANNER",
            format!("http://{}:{}/", host, rest_port),
        )
        .env("CLOUDSDK_CORE_PROJECT", get_project());
    cmd
}

/// Create a Spanner client, auto-detecting emulator via SPANNER_EMULATOR_HOST.
async fn create_spanner() -> anyhow::Result<Spanner> {
    Spanner::builder()
        .build()
        .await
        .context("failed to build Spanner client")
}

/// Create a DatabaseAdmin client for DDL operations.
async fn create_admin_client() -> anyhow::Result<DatabaseAdmin> {
    let spanner = create_spanner().await?;
    let admin = spanner
        .database_admin_builder()
        .build()
        .await
        .context("failed to build DatabaseAdmin client")?;
    Ok(admin)
}

/// Execute a DDL statement using DatabaseAdminClient.
async fn execute_ddl(ddl: &str) -> anyhow::Result<()> {
    let admin = create_admin_client().await?;

    let poller = admin
        .update_database_ddl()
        .set_database(database_name())
        .set_statements([ddl.to_string()])
        .poller();

    poller
        .until_done()
        .await
        .context("DDL operation failed")?;

    println!("  DDL operation completed");
    Ok(())
}

/// Execute DML (INSERT, UPDATE, DELETE) inside a read-write transaction.
async fn execute_sql(sql: &str) -> anyhow::Result<()> {
    let spanner = create_spanner().await?;
    let db_client = spanner
        .database_client(database_name())
        .build()
        .await
        .context("failed to build DatabaseClient")?;

    let runner = db_client
        .read_write_transaction()
        .build()
        .await
        .context("failed to build read-write transaction")?;

    runner
        .run(async |tx| {
            tx.execute_update(Statement::builder(sql).build()).await?;
            Ok(())
        })
        .await
        .context("read-write transaction failed")?;

    Ok(())
}

/// Read SQL from stdin or from argument
fn read_sql() -> String {
    let args: Vec<String> = env::args().collect();

    // If SQL is provided as argument (2nd arg), use it
    if let Some(sql) = args.get(2) {
        return sql.clone();
    }

    // Otherwise, read from stdin
    let mut buffer = String::new();
    io::stdin()
        .read_to_string(&mut buffer)
        .expect("Failed to read SQL from stdin");
    buffer.trim().to_string()
}

// ============================================================================
// Commands
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    let command = args.get(1).map(|s| s.as_str()).unwrap_or("help");

    let project = get_project();
    let instance = get_instance();
    let database = get_database();
    let using_emulator = is_using_emulator();

    match command {
        "cleanup" => {
            println!("Cleaning up Spanner resources...");
            println!("  Dropping change stream if exists...");
            let _ = execute_ddl(&format!("DROP CHANGE STREAM IF EXISTS {}", STREAM)).await;
            println!("  Dropping tables if exist...");
            for table in &["orders", "inventory", "products", "users"] {
                let _ = execute_ddl(&format!("DROP TABLE IF EXISTS {}", table)).await;
            }
            println!("Spanner resources cleaned up");
        }
        "setup" => {
            if using_emulator {
                println!("Setting up Spanner emulator resources...");
                println!("  Creating instance...");
                let output = emulator_gcloud_command()
                    .args([
                        "spanner",
                        "instances",
                        "create",
                        &instance,
                        "--config=emulator-config",
                        "--description=Test Instance",
                        "--nodes=1",
                    ])
                    .output()?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    if !stderr.contains("already exists") {
                        anyhow::bail!("Failed to create instance: {}", stderr);
                    }
                    println!("  Instance already exists");
                }
                println!("  Creating database...");
                let output = emulator_gcloud_command()
                    .args([
                        "spanner",
                        "databases",
                        "create",
                        &database,
                        &format!("--instance={}", instance),
                    ])
                    .output()?;
                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    if !stderr.contains("already exists") {
                        anyhow::bail!("Failed to create database: {}", stderr);
                    }
                    println!("  Database already exists");
                }
                println!("  Emulator resources created");
            } else {
                println!("Setting up real Spanner resources...");
                println!("  Using existing Spanner resources:");
                println!("    Project: {}", project);
                println!("    Instance: {}", instance);
                println!("    Database: {}", database);
                println!("  Note: For real Spanner, instance and database must already exist");
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            println!("Spanner resources setup complete!");
        }
        "create-table" => {
            println!("Creating table users...");
            let ddl = "CREATE TABLE IF NOT EXISTS users (id STRING(MAX) NOT NULL, name STRING(MAX), email STRING(MAX), age INT64) PRIMARY KEY (id)";
            execute_ddl(ddl).await?;
            println!("Table created");
        }
        "create-change-stream" => {
            println!("Creating change stream {}...", STREAM);
            let ddl = format!(
                "CREATE CHANGE STREAM {} FOR ALL OPTIONS (retention_period='7d', value_capture_type='NEW_ROW')",
                STREAM
            );
            execute_ddl(&ddl).await?;
            println!("  Change stream created");
        }
        "insert-data" => {
            println!("Inserting test data...");
            // Idempotent: delete existing rows first, then re-insert. Re-runs
            // of the SLT refresh the seed data without duplicate-key errors.
            let delete_sql = "DELETE FROM users WHERE true";
            execute_sql(delete_sql).await?;
            let sql = "INSERT INTO users (id, name, email, age) VALUES ('1', 'Alice', 'alice@example.com', 30), ('2', 'Bob', 'bob@example.com', 25)";
            execute_sql(sql).await?;
            println!("Test data inserted (Alice, Bob)");
        }
        "dml" => {
            let sql = read_sql();
            if sql.is_empty() {
                eprintln!(
                    "Error: No SQL provided. Use: prepare-data.rs dml \"<SQL>\" or echo \"<SQL>\" | prepare-data.rs dml"
                );
                std::process::exit(1);
            }
            execute_sql(&sql).await?;
            println!("  DML executed successfully");
        }
        "ddl" => {
            let ddl = read_sql();
            if ddl.is_empty() {
                eprintln!(
                    "Error: No DDL provided. Use: prepare-data.rs ddl \"<DDL>\" or echo \"<DDL>\" | prepare-data.rs ddl"
                );
                std::process::exit(1);
            }
            execute_ddl(&ddl).await?;
        }
        "help" | _ => {
            eprintln!("Spanner test data preparation script");
            eprintln!();
            eprintln!("Usage:");
            eprintln!("  prepare-data.rs <command> [args]");
            eprintln!();
            eprintln!("Setup commands:");
            eprintln!("  cleanup              Drop table and change stream");
            eprintln!("  setup                Create instance and database (emulator only)");
            eprintln!("  create-table         Create the users table");
            eprintln!("  create-change-stream Create the test_stream change stream");
            eprintln!("  insert-data          Insert initial test data");
            eprintln!();
            eprintln!("Generic commands (accept SQL via argument or stdin):");
            eprintln!("  dml \"<SQL>\"          Execute DML (INSERT, UPDATE, DELETE)");
            eprintln!("  ddl \"<DDL>\"          Execute DDL (CREATE, ALTER, DROP)");
            eprintln!();
            eprintln!("Examples:");
            eprintln!(
                "  prepare-data.rs dml \"INSERT INTO users (id, name) VALUES ('1', 'Alice')\""
            );
            eprintln!("  prepare-data.rs ddl \"ALTER TABLE users ADD COLUMN city STRING(MAX)\"");
            eprintln!("  echo \"UPDATE users SET age = 30 WHERE id = '1'\" | prepare-data.rs dml");
            eprintln!();
            eprintln!("For emulator (via risedev): All env vars are set automatically");
            eprintln!("For real Spanner: Set SPANNER_PROJECT, SPANNER_INSTANCE, SPANNER_DATABASE");
            std::process::exit(1);
        }
    }

    Ok(())
}
