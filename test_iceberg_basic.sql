-- BigQuery Iceberg Integration Test
-- Usage: ./risedev psql -f test_iceberg_basic.sql
-- Verification: ./check_iceberg_status.sh

-- Cleanup
DROP TABLE IF EXISTS risingwave_iceberg.test_basic CASCADE;

-- Create schema
CREATE SCHEMA IF NOT EXISTS risingwave_iceberg;

-- Create connection
DROP CONNECTION IF EXISTS risingwave_iceberg.my_iceberg_conn;
CREATE CONNECTION risingwave_iceberg.my_iceberg_conn WITH (
    type = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
    catalog.rest.auth_type = 'google',
    catalog.rest.metrics_reporting_enabled = 'false',
    catalog.io_impl = 'org.apache.iceberg.gcp.gcs.GCSFileIO',
    catalog.header = 'x-goog-user-project=cake-data-non-production',
    warehouse.path = 'bq://projects/cake-data-non-production/locations/asia-southeast1',
    namespace.properties = 'location=gs://cake-data-non-production-iceberg/risingwave_iceberg;gcp-region=asia-southeast1',
);

SET iceberg_engine_connection = 'risingwave_iceberg.my_iceberg_conn';

-- Create table
CREATE TABLE risingwave_iceberg.test_basic (
    id INT PRIMARY KEY,
    name VARCHAR
) with(commit_checkpoint_interval = 1) engine = iceberg;

-- Insert data
INSERT INTO risingwave_iceberg.test_basic VALUES (1, 'Alice'), (2, 'Bob');

-- Force checkpoint
FLUSH;