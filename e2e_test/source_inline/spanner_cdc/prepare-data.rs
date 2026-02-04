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
use std::io::{self, Read};

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

/// Read SQL from stdin or from argument
fn read_sql() -> String {
    let args: Vec<String> = env::args().collect();

    // If SQL is provided as argument (2nd arg), use it
    if let Some(sql) = args.get(2) {
        return sql.clone();
    }

    // Otherwise, read from stdin
    let mut buffer = String::new();
    io::stdin().read_to_string(&mut buffer).expect("Failed to read SQL from stdin");
    buffer.trim().to_string()
}

// ============================================================================
// Commands
// ============================================================================

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Install default CryptoProvider for rustls TLS
    rustls::crypto::ring::default_provider().install_default().unwrap();

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
            println!("  Dropping table if exists...");
            let _ = execute_ddl("DROP TABLE IF EXISTS users").await;
            println!("Spanner resources cleaned up");
        }
        "setup" => {
            if using_emulator {
                println!("Setting up Spanner emulator resources...");
                println!("  Creating instance...");
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
                "CREATE CHANGE STREAM {} FOR users OPTIONS (retention_period='7d', value_capture_type='NEW_ROW')",
                STREAM
            );
            execute_ddl(&ddl).await?;
            println!("  Change stream created");
        }
        "insert-data" => {
            println!("Inserting test data...");
            let sql = "INSERT INTO users (id, name, email, age) VALUES ('1', 'Alice', 'alice@example.com', 30), ('2', 'Bob', 'bob@example.com', 25)";
            execute_sql(sql).await?;
            println!("Test data inserted (Alice, Bob)");
        }
        "dml" => {
            let sql = read_sql();
            if sql.is_empty() {
                eprintln!("Error: No SQL provided. Use: prepare-data.rs dml \"<SQL>\" or echo \"<SQL>\" | prepare-data.rs dml");
                std::process::exit(1);
            }
            execute_sql(&sql).await?;
            println!("  DML executed successfully");
        }
        "ddl" => {
            let ddl = read_sql();
            if ddl.is_empty() {
                eprintln!("Error: No DDL provided. Use: prepare-data.rs ddl \"<DDL>\" or echo \"<DDL>\" | prepare-data.rs ddl");
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
            eprintln!("  prepare-data.rs dml \"INSERT INTO users (id, name) VALUES ('1', 'Alice')\"");
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
