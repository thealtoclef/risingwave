#!/usr/bin/env python3
"""
Standalone Spanner setup script for the RisingWave Spanner CDC e2e test.

This does the setup portion of spanner_cdc.slt.serial:
  1. Cleanup any existing resources
  2. Create tables (users, products)
  3. Create change stream (FOR ALL)
  4. Insert initial data

Prerequisites:
  pip install google-cloud-spanner

Environment variables:
  SPANNER_PROJECT   - GCP project ID        (default: test-project)
  SPANNER_INSTANCE  - Spanner instance name  (default: test-instance)
  SPANNER_DATABASE  - Spanner database name  (default: test-database)

For real Spanner, ensure you have authenticated via:
  gcloud auth application-default login
  or set GOOGLE_APPLICATION_CREDENTIALS to a service account key file.
"""

import json
import os

from google.cloud import spanner

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SPANNER_PROJECT = "project-7c6332ba-6520-4c94-848"
SPANNER_INSTANCE = "rainbow-cursor02-spanner"
SPANNER_DATABASE = "cymbal"
STREAM_NAME = "test_stream"


def log_credentials():
    """Log which credentials are being used."""
    cred_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_file:
        print(f"  GOOGLE_APPLICATION_CREDENTIALS = {cred_file}")
        try:
            with open(cred_file) as f:
                cred = json.load(f)
            print(f"  Credential type: {cred.get('type', '(unknown)')}")
            print(f"  Client email:    {cred.get('client_email', '(not present)')}")
            print(f"  Project ID:      {cred.get('project_id', '(not present)')}")
        except Exception as e:
            print(f"  Could not read credential file: {e}")
    else:
        print(
            "  GOOGLE_APPLICATION_CREDENTIALS is not set — using ADC (gcloud auth application-default login)"
        )


def get_database():
    client = spanner.Client(project=SPANNER_PROJECT)
    instance = client.instance(SPANNER_INSTANCE)
    return instance.database(SPANNER_DATABASE)


def execute_ddl(database, statements, description="DDL"):
    """Execute DDL statements and wait for completion."""
    print(f"  {description} ...")
    operation = database.update_ddl(statements)
    operation.result()  # blocks until done
    print(f"  {description} — done")


def execute_dml(database, sql, description="DML"):
    """Execute a DML statement inside a read-write transaction."""

    def _run(transaction):
        transaction.execute_update(sql)

    print(f"  {description} ...")
    database.run_in_transaction(_run)
    print(f"  {description} — done")


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


def step_cleanup(database):
    """Drop change stream and tables from previous runs (idempotent)."""
    print("\n=== Step 1: Cleanup ===")
    ddl = [
        f"DROP CHANGE STREAM IF EXISTS {STREAM_NAME}",
    ]
    try:
        execute_ddl(database, ddl, "Drop change stream")
    except Exception as e:
        print(f"  (ignored: {e})")

    for table in ["orders", "products", "users"]:
        try:
            execute_ddl(
                database, [f"DROP TABLE IF EXISTS {table}"], f"Drop table {table}"
            )
        except Exception as e:
            print(f"  (ignored: {e})")


def step_create_tables(database):
    """Create users and products tables."""
    print("\n=== Step 2: Create tables ===")
    execute_ddl(
        database,
        [
            "CREATE TABLE IF NOT EXISTS users ("
            "  id STRING(MAX) NOT NULL,"
            "  name STRING(MAX),"
            "  email STRING(MAX),"
            "  age INT64"
            ") PRIMARY KEY (id)",
        ],
        "Create users table",
    )

    execute_ddl(
        database,
        [
            "CREATE TABLE IF NOT EXISTS products ("
            "  id STRING(MAX) NOT NULL,"
            "  name STRING(MAX),"
            "  price FLOAT64,"
            "  category STRING(MAX)"
            ") PRIMARY KEY (id)",
        ],
        "Create products table",
    )


def step_insert_users(database):
    """Insert initial user rows BEFORE the change stream exists (for backfill test)."""
    print("\n=== Step 3: Insert initial users (pre-change-stream) ===")
    execute_dml(
        database,
        "INSERT INTO users (id, name, email, age) VALUES "
        "('1', 'Alice', 'alice@example.com', 30), "
        "('2', 'Bob', 'bob@example.com', 25)",
        "Insert Alice & Bob",
    )


def step_create_change_stream(database):
    """Create a change stream FOR ALL with 7-day retention."""
    print("\n=== Step 4: Create change stream ===")
    execute_ddl(
        database,
        [
            f"CREATE CHANGE STREAM {STREAM_NAME} FOR ALL "
            "OPTIONS (retention_period='7d', value_capture_type='NEW_ROW')",
        ],
        f"Create change stream '{STREAM_NAME}'",
    )


def step_insert_products(database):
    """Insert products AFTER the change stream (captured by both snapshot and CDC)."""
    print("\n=== Step 5: Insert products (post-change-stream) ===")
    execute_dml(
        database,
        "INSERT INTO products (id, name, price, category) VALUES "
        "('p1', 'Widget', 9.99, 'gadgets'), "
        "('p2', 'Gizmo', 24.50, 'gadgets'), "
        "('p3', 'Thingamajig', 4.99, 'misc')",
        "Insert 3 products",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    print("Spanner setup for RisingWave CDC e2e test")
    print(f"  Project:  {SPANNER_PROJECT}")
    print(f"  Instance: {SPANNER_INSTANCE}")
    print(f"  Database: {SPANNER_DATABASE}")
    log_credentials()

    database = get_database()

    step_cleanup(database)
    step_create_tables(database)
    step_insert_users(database)
    step_create_change_stream(database)
    step_insert_products(database)

    print("\n=== Setup complete! ===")
    print("You can now create the RisingWave source and tables.")
    print(f"""
Example RisingWave SQL:

  CREATE SOURCE spanner_source WITH (
      connector = 'spanner-cdc',
      spanner.project = '{SPANNER_PROJECT}',
      spanner.instance = '{SPANNER_INSTANCE}',
      database.name = '{SPANNER_DATABASE}',
      spanner.change_stream.name = '{STREAM_NAME}',
      spanner.change_stream.max_concurrent_partitions = 10,
      spanner.heartbeat_interval = '5s',
      auto.schema.change = 'true'
  );

  CREATE TABLE t_users (*) WITH (
      backfill.parallelism = 3,
      spanner.databoost.enabled = 'true',
      spanner.partition_query.parallelism = 4,
  ) FROM spanner_source TABLE 'users';

  CREATE TABLE t_products (*) WITH (
      backfill.parallelism = 3,
      spanner.databoost.enabled = 'true',
      spanner.partition_query.parallelism = 4,
  ) FROM spanner_source TABLE 'products';
""")


if __name__ == "__main__":
    main()
