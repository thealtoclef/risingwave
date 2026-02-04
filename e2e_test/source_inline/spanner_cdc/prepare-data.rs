#!/usr/bin/env -S cargo -Zscript
---cargo
[dependencies]
anyhow = "1"
tokio = { version = "1", features = ["rt", "rt-multi-thread", "macros", "time"] }
google-cloud-spanner = { package = "gcloud-spanner", version = "1", features = ["auth"] }
google-cloud-gax = { package = "gcloud-gax", version = "1.3" }
google-cloud-googleapis = { package = "gcloud-googleapis", version = "1", features = ["spanner"] }
google-cloud-longrunning = { package = "gcloud-longrunning", version = "1.3" }
google-cloud-auth = { package = "gcloud-auth", version = "1.2", default-features = false }
rustls = { version = "0.23", features = ["ring"] }
---
use std::env;

use google_cloud_gax::conn::{ConnectionManager, ConnectionOptions, Environment};
use google_cloud_googleapis::spanner::admin::database::v1::UpdateDatabaseDdlRequest;
use google_cloud_googleapis::spanner::v1::{ExecuteSqlRequest, CreateSessionRequest, DeleteSessionRequest, BeginTransactionRequest, CommitRequest, TransactionSelector};
use google_cloud_googleapis::spanner::v1::{transaction_options, TransactionOptions};
use google_cloud_googleapis::spanner::v1::commit_request;

use google_cloud_spanner::admin::database::database_admin_client::DatabaseAdminClient;
use google_cloud_googleapis::spanner::v1::spanner_client::SpannerClient;
use google_cloud_longrunning::autogen::operations_client::OperationsClient;

// Type alias for the authenticated channel type returned by ConnectionManager
type Channel = google_cloud_gax::conn::Channel;

const AUDIENCE: &str = "https://spanner.googleapis.com/";
const SPANNER_DOMAIN: &str = "spanner.googleapis.com";

fn get_env_or_default(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn get_project() -> String { get_env_or_default("SPANNER_PROJECT", "test-project") }
fn get_instance() -> String { get_env_or_default("SPANNER_INSTANCE", "test-instance") }
fn get_database() -> String { get_env_or_default("SPANNER_DATABASE", "test-database") }
const STREAM: &str = "test_stream";

/// Check if we're using emulator (SPANNER_EMULATOR_HOST is set)
fn is_using_emulator() -> bool {
    env::var("SPANNER_EMULATOR_HOST").is_ok()
}

/// Create environment for Spanner connection (emulator or real)
async fn create_environment() -> anyhow::Result<Environment> {
    if let Ok(host) = env::var("SPANNER_EMULATOR_HOST") {
        // Emulator mode
        Ok(Environment::Emulator(host))
    } else {
        // Real Spanner mode - use default authentication
        // This will read GOOGLE_APPLICATION_CREDENTIALS_JSON or GOOGLE_APPLICATION_CREDENTIALS
        use google_cloud_auth::token::DefaultTokenSourceProvider;
        use google_cloud_auth::project::Config;

        // Scopes required for Spanner
        const SCOPES: &[&str] = &[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/spanner.data",
        ];

        let config = Config::default().with_scopes(SCOPES);
        let token_source_provider = DefaultTokenSourceProvider::new(config).await?;

        // Debug: Print the SA email to confirm auth is working
        if let Some(ref sa) = token_source_provider.source_credentials {
            eprintln!("  Using Spanner SA: {}", sa.client_email.as_ref().unwrap_or(&"(unknown)".to_string()));
        }

        Ok(Environment::GoogleCloud(Box::new(token_source_provider)))
    }
}

/// Create Spanner admin client for DDL operations
async fn create_admin_client() -> anyhow::Result<DatabaseAdminClient> {
    let environment = create_environment().await?;

    let conn_pool = ConnectionManager::new(
        1,
        SPANNER_DOMAIN,
        AUDIENCE,
        &environment,
        &ConnectionOptions::default(),
    ).await?;

    let lro_client = OperationsClient::new(conn_pool.conn()).await?;
    Ok(DatabaseAdminClient::new(conn_pool.conn(), lro_client))
}

/// Create Spanner client for DML operations
async fn create_spanner_client() -> anyhow::Result<SpannerClient<Channel>> {
    let environment = create_environment().await?;

    let conn_pool = ConnectionManager::new(
        1,
        SPANNER_DOMAIN,
        AUDIENCE,
        &environment,
        &ConnectionOptions::default(),
    ).await?;

    Ok(SpannerClient::new(conn_pool.conn()))
}

/// Execute DDL statement using DatabaseAdminClient
async fn execute_ddl(ddl: &str) -> anyhow::Result<()> {
    let db_client = create_admin_client().await?;

    let database_name = format!(
        "projects/{}/instances/{}/databases/{}",
        get_project(),
        get_instance(),
        get_database()
    );

    // operation_id should be empty string for most cases
    // (only needed for retrying operations)
    let request = UpdateDatabaseDdlRequest {
        database: database_name,
        operation_id: "".to_string(),
        statements: vec![ddl.to_string()],
        proto_descriptors: vec![],
        throughput_mode: false,
    };

    let mut operation = db_client.update_database_ddl(request, None).await?;

    // Wait for the DDL operation to complete
    let _ = operation.wait(None).await;

    println!("  DDL operation completed");
    Ok(())
}

/// Execute SQL using SpannerClient
/// For DML statements, this uses a read-write transaction.
/// For DDL statements, use execute_ddl() instead.
async fn execute_sql(sql: &str) -> anyhow::Result<()> {
    let mut spanner_client = create_spanner_client().await?;

    let database_name = format!(
        "projects/{}/instances/{}/databases/{}",
        get_project(),
        get_instance(),
        get_database()
    );

    let session = spanner_client
        .create_session(CreateSessionRequest {
            database: database_name,
            session: None,
        })
        .await?
        .into_inner();

    let session_name = session.name.clone();

    // Begin a read-write transaction for DML
    let begin_txn_response = spanner_client
        .begin_transaction(BeginTransactionRequest {
            session: session_name.clone(),
            options: Some(TransactionOptions {
                mode: Some(transaction_options::Mode::ReadWrite(transaction_options::ReadWrite::default())),
                exclude_txn_from_change_streams: false,
                isolation_level: 0,
            }),
            mutation_key: None,
            request_options: None,
        })
        .await?;

    let transaction_id = begin_txn_response.into_inner().id;
    let transaction_selector = Some(TransactionSelector {
        selector: Some(google_cloud_googleapis::spanner::v1::transaction_selector::Selector::Id(
            transaction_id.clone(),
        )),
    });

    // Execute the SQL with the transaction
    let request = ExecuteSqlRequest {
        session: session_name.clone(),
        sql: sql.to_string(),
        transaction: transaction_selector,
        params: None,
        param_types: Default::default(),
        resume_token: vec![],
        query_mode: 0,
        partition_token: vec![],
        seqno: 0,
        query_options: None,
        request_options: None,
        directed_read_options: None,
        data_boost_enabled: false,
        last_statement: false,
    };

    let _ = spanner_client.execute_sql(request).await?;

    // Commit the transaction
    spanner_client
        .commit(CommitRequest {
            session: session_name.clone(),
            transaction: Some(commit_request::Transaction::TransactionId(transaction_id)),
            mutations: vec![],
            max_commit_delay: None,
            precommit_token: None,
            return_commit_stats: false,
            request_options: None,
        })
        .await?;

    // Delete the session
    let _ = spanner_client
        .delete_session(DeleteSessionRequest {
            name: session_name,
        })
        .await;

    Ok(())
}

// ============================================================================
// Commands
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Install default CryptoProvider for rustls TLS
    rustls::crypto::ring::default_provider().install_default().unwrap();

    let args: Vec<String> = env::args().collect();
    let command = args.get(1).map(|s| s.as_str()).unwrap_or("setup");

    let project = get_project();
    let instance = get_instance();
    let database = get_database();
    let using_emulator = is_using_emulator();

    println!(
        "Spanner config: project={}, instance={}, database={}, emulator={}",
        project, instance, database, using_emulator
    );

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
            std::process::exit(1);
        }
    }

    Ok(())
}

/// Cleanup resources: drop table, drop change stream
async fn cleanup_resources() -> anyhow::Result<()> {
    println!("Cleaning up Spanner resources...");

    // Drop change stream
    println!("  Dropping change stream if exists...");
    let _ = execute_ddl(&format!("DROP CHANGE STREAM IF EXISTS {}", STREAM)).await;

    // Drop table
    println!("  Dropping table if exists...");
    let _ = execute_ddl("DROP TABLE IF EXISTS users").await;

    println!("Spanner resources cleaned up");
    Ok(())
}

/// Setup resources: create instance and database only
async fn setup_resources() -> anyhow::Result<()> {
    let using_emulator = is_using_emulator();
    let project = get_project();
    let instance = get_instance();
    let database = get_database();

    if using_emulator {
        println!("Setting up Spanner emulator resources...");
        println!("  Creating instance...");

        // For emulator, we still need gcloud to create the instance
        // This is a limitation - Spanner API doesn't provide instance creation
        let _ = std::process::Command::new("gcloud")
            .args([
                "spanner",
                "instances",
                "create",
                &instance,
                "--config=emulator-config",
                "--description=Test Instance",
                "--nodes=1",
            ])
            .output();

        println!("  Creating database...");
        let _ = std::process::Command::new("gcloud")
            .args([
                "spanner",
                "databases",
                "create",
                &database,
                &format!("--instance={}", instance),
            ])
            .output();

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
    Ok(())
}

/// Create the users table
async fn create_table() -> anyhow::Result<()> {
    println!("Creating table users...");

    let ddl = "CREATE TABLE IF NOT EXISTS users (id STRING(MAX) NOT NULL, name STRING(MAX), email STRING(MAX), age INT64) PRIMARY KEY (id)";
    execute_ddl(ddl).await?;

    println!("Table created");
    Ok(())
}

/// Create the change stream
async fn create_change_stream() -> anyhow::Result<()> {
    println!("Creating change stream {}...", STREAM);

    let ddl = format!(
        "CREATE CHANGE STREAM {} FOR users OPTIONS (retention_period='7d', value_capture_type='NEW_ROW')",
        STREAM
    );
    execute_ddl(&ddl).await?;

    println!("  Change stream created");
    Ok(())
}

/// Insert test data
async fn insert_test_data() -> anyhow::Result<()> {
    println!("Inserting test data...");

    let sql = "INSERT INTO users (id, name, email, age) VALUES ('1', 'Alice', 'alice@example.com', 30), ('2', 'Bob', 'bob@example.com', 25)";
    execute_sql(sql).await?;

    println!("Test data inserted (Alice, Bob)");
    Ok(())
}
