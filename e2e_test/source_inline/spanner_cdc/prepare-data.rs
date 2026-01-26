#!/usr/bin/env -S cargo -Zscript
---cargo
[dependencies]
anyhow = "1"
tokio = { version = "0.2", package = "madsim-tokio", features = [
    "rt",
    "rt-multi-thread",
    "sync",
    "macros",
    "time",
    "signal",
    "fs",
] }
---
use std::process::Command;

const PROJECT: &str = "test-project";
const INSTANCE: &str = "test-instance";
const DATABASE: &str = "test-database";
const STREAM: &str = "test_stream";

/// Get the full path to gcloud, including PREFIX_BIN if needed
fn gcloud_cmd() -> String {
    // Try to use gcloud from PATH first
    if let Ok(_) = Command::new("gcloud").arg("--version").output() {
        "gcloud".to_string()
    } else {
        // Fall back to downloaded gcloud
        let prefix_bin = std::env::var("PREFIX_BIN").unwrap_or_else(|_| ".".to_string());
        format!("{}/gcloud/bin/gcloud", prefix_bin)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).map(|s| s.as_str()).unwrap_or("setup");

    let emulator_host = std::env::var("SPANNER_EMULATOR_HOST")
        .expect("SPANNER_EMULATOR_HOST must be set for emulator testing");

    match command {
        "cleanup" => cleanup_resources(&emulator_host).await?,
        "setup" => setup_resources(&emulator_host).await?,
        "create-table" => create_table(&emulator_host).await?,
        "create-change-stream" => create_change_stream(&emulator_host).await?,
        "insert-data" => insert_test_data(&emulator_host).await?,
        _ => {
            eprintln!("Unknown command: {}", command);
            eprintln!("Usage: prepare-data.rs [cleanup|setup|create-table|create-change-stream|insert-data]");
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Cleanup resources: drop table, drop change stream, delete all data
/// This is robust and will not error if resources don't exist
async fn cleanup_resources(_emulator_host: &str) -> anyhow::Result<()> {
    println!("Cleaning up Spanner resources...");

    let gcloud = gcloud_cmd();

    // Drop change stream if it exists (ignore errors)
    println!("  Dropping change stream if exists...");
    let drop_stream_ddl = format!("DROP CHANGE STREAM IF EXISTS {}", STREAM);
    let _ = Command::new(&gcloud)
        .args(&[
            "spanner",
            "databases",
            "ddl",
            "update",
            DATABASE,
            &format!("--instance={}", INSTANCE),
            &format!("--ddl={}", drop_stream_ddl),
        ])
        .output();

    // Drop table if it exists (ignore errors)
    println!("  Dropping table if exists...");
    let drop_table_ddl = format!("DROP TABLE IF EXISTS users");
    let _ = Command::new(&gcloud)
        .args(&[
            "spanner",
            "databases",
            "ddl",
            "update",
            DATABASE,
            &format!("--instance={}", INSTANCE),
            &format!("--ddl={}", drop_table_ddl),
        ])
        .output();

    // Delete any remaining data (ignore errors)
    println!("  Deleting any remaining data...");
    let delete_sql = "DELETE FROM users WHERE true";
    let _ = Command::new(&gcloud)
        .args(&[
            "spanner",
            "databases",
            "execute-sql",
            DATABASE,
            &format!("--instance={}", INSTANCE),
            &format!("--sql={}", delete_sql),
        ])
        .output();

    println!("Spanner resources cleaned up");
    Ok(())
}

/// Setup resources: create instance and database only
async fn setup_resources(emulator_host: &str) -> anyhow::Result<()> {
    println!("Setting up Spanner resources (instance and database)...");
    println!("   Emulator: {}", emulator_host);

    let gcloud = gcloud_cmd();

    let grpc_port = emulator_host.split(':').nth(1).unwrap_or("9010");
    let rest_port: u16 = grpc_port.parse::<u16>().unwrap_or(9010) + 10;
    let rest_endpoint = format!("http://127.0.0.1:{}/", rest_port);

    // Configure gcloud for emulator
    println!("   Configuring gcloud...");

    Command::new(&gcloud)
        .args(&["config", "configurations", "create", "emulator"])
        .output()?;

    Command::new(&gcloud)
        .args(&["config", "configurations", "activate", "emulator"])
        .output()?;

    Command::new(&gcloud)
        .args(&["config", "set", "auth/disable_credentials", "true"])
        .output()?;

    Command::new(&gcloud)
        .args(&["config", "set", "project", PROJECT])
        .output()?;

    Command::new(&gcloud)
        .args(&["config", "set", "api_endpoint_overrides/spanner", &rest_endpoint])
        .output()?;

    println!("   gcloud configured");

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Create instance
    println!("   Creating instance...");
    let create_instance_result = Command::new(&gcloud)
        .args(&[
            "spanner",
            "instances",
            "create",
            INSTANCE,
            "--config=emulator-config",
            "--description=Test Instance",
            "--nodes=1",
        ])
        .output()?;

    if !create_instance_result.status.success() {
        let stderr = String::from_utf8_lossy(&create_instance_result.stderr);
        if stderr.contains("already exists") || stderr.contains("AlreadyExists") {
            println!("   Instance already exists");
        } else {
            eprintln!("   Warning: Failed to create instance");
        }
    } else {
        println!("   Instance created");
    }

    // Create database
    println!("   Creating database...");
    let create_db_result = Command::new(&gcloud)
        .args(&[
            "spanner",
            "databases",
            "create",
            DATABASE,
            &format!("--instance={}", INSTANCE),
        ])
        .output()?;

    if !create_db_result.status.success() {
        let stderr = String::from_utf8_lossy(&create_db_result.stderr);
        if stderr.contains("already exists") || stderr.contains("AlreadyExists") {
            println!("   Database already exists");
        } else {
            eprintln!("   Warning: Failed to create database");
        }
    } else {
        println!("   Database created");
    }

    println!("Spanner resources setup complete (instance and database)!");
    Ok(())
}

/// Create the users table
async fn create_table(_emulator_host: &str) -> anyhow::Result<()> {
    println!("Creating table users...");

    let gcloud = gcloud_cmd();

    let create_table_ddl = "CREATE TABLE IF NOT EXISTS users (id STRING(MAX) NOT NULL, name STRING(MAX), email STRING(MAX), age INT64) PRIMARY KEY (id)";

    let _ = Command::new(&gcloud)
        .args(&[
            "spanner",
            "databases",
            "ddl",
            "update",
            DATABASE,
            &format!("--instance={}", INSTANCE),
            &format!("--ddl={}", create_table_ddl),
        ])
        .output()?;

    println!("Table created");
    Ok(())
}

/// Create the change stream
async fn create_change_stream(_emulator_host: &str) -> anyhow::Result<()> {
    println!("Creating change stream {}...", STREAM);

    let gcloud = gcloud_cmd();

    let create_stream_ddl = format!(
        "CREATE CHANGE STREAM {} FOR users OPTIONS (retention_period='7d', value_capture_type='NEW_ROW')",
        STREAM
    );

    let create_stream_result = Command::new(&gcloud)
        .args(&[
            "spanner",
            "databases",
            "ddl",
            "update",
            DATABASE,
            &format!("--instance={}", INSTANCE),
            &format!("--ddl={}", create_stream_ddl),
        ])
        .output()?;

    if !create_stream_result.status.success() {
        let stderr = String::from_utf8_lossy(&create_stream_result.stderr);
        if stderr.contains("already exists") || stderr.contains("Duplicate name") {
            println!("  Change stream already exists");
        } else {
            eprintln!("  Warning: Failed to create change stream: {}", stderr);
        }
    } else {
        println!("  Change stream created");
    }

    Ok(())
}

/// Insert test data
async fn insert_test_data(_emulator_host: &str) -> anyhow::Result<()> {
    println!("Inserting test data...");

    let gcloud = gcloud_cmd();

    let insert_sql = "INSERT INTO users (id, name, email, age) VALUES ('1', 'Alice', 'alice@example.com', 30), ('2', 'Bob', 'bob@example.com', 25)";

    let _ = Command::new(&gcloud)
        .args(&[
            "spanner",
            "databases",
            "execute-sql",
            DATABASE,
            &format!("--instance={}", INSTANCE),
            &format!("--sql={}", insert_sql),
        ])
        .output()?;

    println!("Test data inserted (Alice, Bob)");
    Ok(())
}
