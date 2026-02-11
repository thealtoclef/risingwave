#!/bin/bash
# Setup Spanner emulator with required instance, database, table, and change stream

set -e

export SPANNER_EMULATOR_HOST=localhost:9010

PROJECT="test-project"
INSTANCE="test-instance"
DATABASE="test-database"
STREAM="test_stream"

echo "🔧 Setting up Spanner emulator..."

# Create instance
echo "   Creating instance $INSTANCE..."
gcloud spanner instances create $INSTANCE \
  --config=emulator-config \
  --description="Test instance" \
  --nodes=1 \
  --project=$PROJECT \
  2>/dev/null || echo "   Instance already exists"

# Create database
echo "   Creating database $DATABASE..."
gcloud spanner databases create $DATABASE \
  --instance=$INSTANCE \
  --project=$PROJECT \
  2>/dev/null || echo "   Database already exists"

# Create table
echo "   Creating table users..."
gcloud spanner databases ddl update $DATABASE \
  --instance=$INSTANCE \
  --project=$PROJECT \
  --ddl="CREATE TABLE users (
    id STRING(MAX) NOT NULL,
    name STRING(MAX),
    email STRING(MAX),
    age INT64
  ) PRIMARY KEY (id)" \
  2>/dev/null || echo "   Table already exists"

# Create change stream
echo "   Creating change stream $STREAM..."
gcloud spanner databases ddl update $DATABASE \
  --instance=$INSTANCE \
  --project=$PROJECT \
  --ddl="CREATE CHANGE STREAM $STREAM FOR users OPTIONS (retention_period='7d', value_capture_type='NEW_ROW')" \
  2>/dev/null || echo "   Change stream already exists"

echo "✅ Emulator setup complete!"
echo "   Instance: $INSTANCE"
echo "   Database: $DATABASE"
echo "   Table: users"
echo "   Change Stream: $STREAM"
