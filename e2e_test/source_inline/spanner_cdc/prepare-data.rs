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

fn get_env_or_default(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn get_project() -> String { get_env_or_default("SPANNER_PROJECT", "test-project") }
fn get_instance() -> String { get_env_or_default("SPANNER_INSTANCE", "test-instance") }
fn get_database() -> String { get_env_or_default("SPANNER_DATABASE", "test-database") }
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

/// Check if we're using emulator (SPANNER_EMULATOR_HOST is set)
fn is_using_emulator() -> bool {
    std::env::var("SPANNER_EMULATOR_HOST").is_ok()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).map(|s| s.as_str()).unwrap_or("setup");

    let project = get_project();
    let instance = get_instance();
    let database = get_database();
    let using_emulator = is_using_emulator();

    println!("Spanner config: project={}, instance={}, database={}, emulator={}",
        project, instance, database, using_emulator);

    match command {
        "cleanup" => cleanup_resources().await?,
        "setup" => setup_resources().await?,
        "create-table" => create_table().await?,
        "create-change-stream" => create_change_stream().await?,
        "insert-data" => insert_test_data().await?,
        _ => {
            eprintln!("Unknown command: {}", command);
            eprintln!("Usage: prepare-data.rs [cleanup|setup|create-table|create-change-stream|insert-data]");
            eprintln!();
            eprintln!("For emulator (via risedev): All env vars are set automatically");
            eprintln!("For real Spanner: Set SPANNER_PROJECT, SPANNER_INSTANCE, SPANNER_DATABASE");
            eprintln!("                 Ensure gcloud is authenticated: gcloud auth login");
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Cleanup resources: drop table, drop change stream, delete all data
/// This is robust and will not error if resources don't exist
async fn cleanup_resources() -> anyhow::Result<()> {
    println!("Cleaning up Spanner resources...");

    let gcloud = gcloud_cmd();
    let instance = get_instance();
    let database = get_database();

    // Drop change stream if it exists (ignore errors)
    println!("  Dropping change stream if exists...");
    let drop_stream_ddl = format!("DROP CHANGE STREAM IF EXISTS {}", STREAM);
    let _ = Command::new(&gcloud)
        .args(&[
            "spanner",
            "databases",
            "ddl",
            "update",
            &database,
            &format!("--instance={}", instance),
            &format!("--ddl={}", drop_stream_ddl),
        ])
        .output();

    // Drop table if it exists (ignore errors)
    println!("  Dropping table if exists...");
    let drop_table_ddl = "DROP TABLE IF EXISTS users";
    let _ = Command::new(&gcloud)
        .args(&[
            "spanner",
            "databases",
            "ddl",
            "update",
            &database,
            &format!("--instance={}", instance),
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
            &database,
            &format!("--instance={}", instance),
            &format!("--sql={}", delete_sql),
        ])
        .output();

    println!("Spanner resources cleaned up");
    Ok(())
}

/// Setup resources: create instance and database only
async fn setup_resources() -> anyhow::Result<()> {
    let using_emulator = is_using_emulator();
    let project = get_project();
    let instance = get_instance();
    let database = get_database();
    let gcloud = gcloud_cmd();

    if using_emulator {
        println!("Setting up Spanner emulator resources...");
        configure_gcloud_for_emulator(&gcloud, &project)?;
    } else {
        println!("Setting up real Spanner resources...");
        println!("  Using existing project: {}", project);
        println!("  Ensure gcloud is authenticated: gcloud auth login");
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // For emulator: create instance
    // For real Spanner: instance should already exist
    if using_emulator {
        println!("  Creating instance...");
        let create_instance_result = Command::new(&gcloud)
            .args(&[
                "spanner",
                "instances",
                "create",
                &instance,
                "--config=emulator-config",
                "--description=Test Instance",
                "--nodes=1",
            ])
            .output()?;

        if !create_instance_result.status.success() {
            let stderr = String::from_utf8_lossy(&create_instance_result.stderr);
            if stderr.contains("already exists") || stderr.contains("AlreadyExists") {
                println!("  Instance already exists");
            } else {
                eprintln!("  Warning: Failed to create instance");
            }
        } else {
            println!("  Instance created");
        }
    } else {
        println!("  Verifying instance exists...");
        let list_result = Command::new(&gcloud)
            .args(&[
                "spanner",
                "instances",
                "list",
                &format!("--project={}", project),
                &format!("--filter=name:{}", instance),
            ])
            .output()?;

        if !list_result.status.success() || String::from_utf8_lossy(&list_result.stdout).is_empty() {
            eprintln!("  ERROR: Instance '{}' not found in project '{}'", instance, project);
            eprintln!("  Please create the instance first:");
            eprintln!("    gcloud spanner instances create {} --project={} --description=Test --nodes=1", instance, project);
            std::process::exit(1);
        }
        println!("  Instance verified");
    }

    // Create database
    println!("  Creating database...");
    let create_db_result = Command::new(&gcloud)
        .args(&[
            "spanner",
            "databases",
            "create",
            &database,
            &format!("--instance={}", instance),
        ])
        .output()?;

    if !create_db_result.status.success() {
        let stderr = String::from_utf8_lossy(&create_db_result.stderr);
        if stderr.contains("already exists") || stderr.contains("AlreadyExists") {
            println!("  Database already exists");
        } else {
            eprintln!("  Warning: Failed to create database: {}", stderr);
        }
    } else {
        println!("  Database created");
    }

    println!("Spanner resources setup complete!");
    Ok(())
}

/// Configure gcloud for emulator use
fn configure_gcloud_for_emulator(gcloud: &str, project: &str) -> anyhow::Result<()> {
    println!("  Configuring gcloud for emulator...");

    Command::new(gcloud)
        .args(&["config", "configurations", "activate", "emulator"])
        .output()?;

    Command::new(gcloud)
        .args(&["config", "set", "auth/disable_credentials", "true"])
        .output()?;

    Command::new(gcloud)
        .args(&["config", "set", "project", project])
        .output()?;

    println!("  gcloud configured for emulator");
    Ok(())
}

/// Create the users table
async fn create_table() -> anyhow::Result<()> {
    println!("Creating table users...");

    let gcloud = gcloud_cmd();
    let instance = get_instance();
    let database = get_database();

    let create_table_ddl = "CREATE TABLE IF NOT EXISTS users (id STRING(MAX) NOT NULL, name STRING(MAX), email STRING(MAX), age INT64) PRIMARY KEY (id)";

    let _ = Command::new(&gcloud)
        .args(&[
            "spanner",
            "databases",
            "ddl",
            "update",
            &database,
            &format!("--instance={}", instance),
            &format!("--ddl={}", create_table_ddl),
        ])
        .output()?;

    println!("Table created");
    Ok(())
}

/// Create the change stream
async fn create_change_stream() -> anyhow::Result<()> {
    println!("Creating change stream {}...", STREAM);

    let gcloud = gcloud_cmd();
    let instance = get_instance();
    let database = get_database();

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
            &database,
            &format!("--instance={}", instance),
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
async fn insert_test_data() -> anyhow::Result<()> {
    println!("Inserting test data...");

    let gcloud = gcloud_cmd();
    let instance = get_instance();
    let database = get_database();

    let insert_sql = "INSERT INTO users (id, name, email, age) VALUES ('1', 'Alice', 'alice@example.com', 30), ('2', 'Bob', 'bob@example.com', 25)";

    let _ = Command::new(&gcloud)
        .args(&[
            "spanner",
            "databases",
            "execute-sql",
            &database,
            &format!("--instance={}", instance),
            &format!("--sql={}", insert_sql),
        ])
        .output()?;

    println!("Test data inserted (Alice, Bob)");
    Ok(())
}
