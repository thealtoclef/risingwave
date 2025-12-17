#!/bin/bash
# BigQuery Iceberg Troubleshooting Script
# Usage: ./check_iceberg_status.sh
# 
# This script checks the status of BigQuery Iceberg integration
# and displays diagnostic information for troubleshooting.

set -e

echo "=========================================="
echo "BigQuery Iceberg Status Check"
echo "=========================================="
echo ""

# Check if RisingWave is running
echo "[1] RisingWave Process Status"
echo "----------------------------------------"
if pgrep -f "risingwave" > /dev/null; then
    echo "Status: Running"
else
    echo "Status: NOT running"
fi
echo ""

# Check compute log for errors
echo "[2] Recent Iceberg/GCS Errors"
echo "----------------------------------------"
COMPUTE_LOG=".risingwave/log/compute-node-"*.log
if ls $COMPUTE_LOG 1> /dev/null 2>&1; then
    tail -200 $COMPUTE_LOG | grep -i "error.*iceberg\|error.*gcs\|error.*opendal\|failed.*parquet\|no valid credential\|metadata.google.internal" | tail -20 || echo "No errors found"
else
    echo "Log file not found"
fi
echo ""

# Check sink status
echo "[3] Iceberg Sink Status"
echo "----------------------------------------"
./risedev psql -t -c "
SELECT 
    'Sink: ' || s.name || 
    ' | Actors: ' || COALESCE(COUNT(a.actor_id)::text, '0') ||
    ' | Connector: ' || s.connector
FROM rw_catalog.rw_sinks s
LEFT JOIN rw_catalog.rw_fragments f ON s.id = f.table_id
LEFT JOIN rw_catalog.rw_actors a ON f.fragment_id = a.fragment_id
WHERE s.name LIKE '%iceberg%' OR s.name LIKE '%test_basic%'
GROUP BY s.id, s.name, s.connector;
" 2>/dev/null || echo "Could not query sink status"
echo ""

# Check tables
echo "[4] Iceberg Tables"
echo "----------------------------------------"
./risedev psql -t -c "
SELECT 
    'Schema: ' || schemaname || ' | Table: ' || tablename
FROM pg_tables 
WHERE schemaname = 'risingwave_iceberg';
" 2>/dev/null || echo "Could not query tables"
echo ""

# Check table data
echo "[4b] Table Data Verification"
echo "----------------------------------------"
./risedev psql -t -c "
SELECT 
    'Table: risingwave_iceberg.test_basic | Rows: ' || COUNT(*)::text
FROM risingwave_iceberg.test_basic;
" 2>/dev/null || echo "Table does not exist or query failed"

./risedev psql -c "
SELECT * FROM risingwave_iceberg.test_basic LIMIT 10;
" 2>/dev/null || echo "Could not query table data"
echo ""

# Check connections
echo "[5] Iceberg Connections"
echo "----------------------------------------"
./risedev psql -t -c "
SELECT 
    'Connection: ' || name
FROM rw_catalog.rw_connections 
WHERE name LIKE '%iceberg%';
" 2>/dev/null || echo "Could not query connections"
echo ""

# Check catalog auth logs
echo "[6] Catalog Authentication Status"
echo "----------------------------------------"
if ls $COMPUTE_LOG 1> /dev/null 2>&1; then
    tail -100 $COMPUTE_LOG | grep -i "Application Default Credentials\|GoogleAuthManager" | tail -10 || echo "No catalog auth logs found"
else
    echo "Log file not found"
fi
echo ""

# Check sink errors
echo "[7] Sink Errors"
echo "----------------------------------------"
if ls $COMPUTE_LOG 1> /dev/null 2>&1; then
    echo "Recent sink errors (if empty, sink is working):"
    grep "ERROR.*sink\|reset log reader stream" $COMPUTE_LOG | tail -5 || echo "✓ No sink errors found"
    echo ""
    echo "If you see 'reset log reader stream', the sink is FAILING and auto-retrying."
else
    echo "Log file not found"
fi
echo ""

# Check OpenDAL errors
echo "[8] OpenDAL Errors"
echo "----------------------------------------"
if ls $COMPUTE_LOG 1> /dev/null 2>&1; then
    tail -100 $COMPUTE_LOG | grep -i "opendal.*error\|opendal.*warn\|ConfigInvalid\|credential.*gcs" | tail -10 || echo "No OpenDAL errors found"
else
    echo "Log file not found"
fi
echo ""

# Check specific error patterns
echo "[9] Known Error Patterns"
echo "----------------------------------------"
if ls $COMPUTE_LOG 1> /dev/null 2>&1; then
    echo -n "no valid credential found: "
    if tail -200 $COMPUTE_LOG | grep -q "no valid credential found"; then
        echo "FOUND"
    else
        echo "Not found"
    fi
    
    echo -n "metadata.google.internal: "
    if tail -200 $COMPUTE_LOG | grep -q "metadata.google.internal"; then
        echo "FOUND"
    else
        echo "Not found"
    fi
    
    echo -n "Failed to finish parquet writer: "
    if tail -200 $COMPUTE_LOG | grep -q "Failed to finish parquet writer"; then
        echo "FOUND"
    else
        echo "Not found"
    fi
    
    echo -n "Malformed request (location): "
    if tail -200 $COMPUTE_LOG | grep -q "Malformed request.*location"; then
        echo "FOUND"
    else
        echo "Not found"
    fi
else
    echo "Log file not found"
fi
echo ""

echo "=========================================="
echo "Status Summary"
echo "=========================================="

# Provide a quick summary
if ls $COMPUTE_LOG 1> /dev/null 2>&1; then
    ERROR_COUNT=$(tail -200 $COMPUTE_LOG | grep -c "error.*iceberg\|error.*gcs\|no valid credential\|Failed to finish parquet" || echo "0")
    echo "Recent error count: $ERROR_COUNT"
    
    # Check if data is visible
    ROW_COUNT=$(./risedev psql -t -c "SELECT COUNT(*) FROM risingwave_iceberg.test_basic;" 2>/dev/null | xargs || echo "N/A")
    if [ "$ROW_COUNT" = "N/A" ]; then
        echo "Data status: Table not found or query failed"
    elif [ "$ROW_COUNT" = "0" ]; then
        echo "Data status: No data committed yet (this is normal for async sinks)"
    else
        echo "Data status: $ROW_COUNT rows visible"
    fi
fi

echo ""
echo "To view live logs:"
echo "  tail -f $COMPUTE_LOG | grep -i iceberg"

