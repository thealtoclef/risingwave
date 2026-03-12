# Spanner CDC Source Connector

Native Rust implementation of Google Cloud Spanner Change Data Capture (CDC) source for RisingWave.

## Overview

This connector reads data from Google Cloud Spanner change streams and delivers it to RisingWave tables. It supports both **snapshot backfill** (initial data load) and **CDC streaming** (real-time change capture).

### Key Features

- **Native Rust Implementation**: Uses `gcloud-spanner` crate (v1.x), not Debezium
- **Shared Reader Architecture**: Multiple tables from the same source share a single reader instance
- **Full Type Support**: All Spanner types supported with data-preserving fallbacks
- **Automatic Partition Management**: Handles parent-child partition splits and merges
- **Watermark-Based Scheduling**: Ensures timestamp-ordered processing across partitions
- **Schema Evolution**: Automatic detection and propagation of schema changes (ADD COLUMN)
- **Production Ready**: Retry logic, checkpointing, graceful shutdown

---

## Quick Start

### Basic Usage

```sql
-- Create a source that connects to Spanner change stream
CREATE SOURCE spanner_source WITH (
    connector = 'spanner-cdc',
    spanner.project = 'my-project',
    spanner.instance = 'my-instance',
    database.name = 'my-database',
    spanner.change_stream.name = 'my_stream',
    spanner.credentials_path = '/path/to/service-account.json'
) FORMAT PLAIN ENCODE JSON;

-- Create tables from the source (specify upstream table name)
CREATE TABLE users FROM spanner_source TABLE 'users';
CREATE TABLE orders FROM spanner_source TABLE 'orders';
```

### Testing with Emulator

```sql
CREATE SOURCE spanner_test WITH (
    connector = 'spanner-cdc',
    spanner.project = 'test-project',
    spanner.instance = 'test-instance',
    database.name = 'test-database',
    spanner.change_stream.name = 'test_stream',
    spanner.emulator_host = 'http://localhost:9010'
) FORMAT PLAIN ENCODE JSON;

CREATE TABLE test_table FROM spanner_test TABLE 'test_table';
```

---

## Architecture

### Shared Reader Pattern

**Problem**: When creating multiple tables FROM the same Spanner source, RisingWave spawns multiple table actors. Without sharing, each actor would create its own Spanner reader, causing:
- N concurrent Spanner queries (one per table × partitions)
- Duplicate data reads across tables
- Potential Spanner query limit exhaustion (default: 20 queries)

**Solution**: Single shared reader instance per source, with broadcast to all table actors.

```
┌──────────────────────────────────────────────────────────────────┐
│                    RisingWave Streaming Graph                     │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ Table Actor │  │ Table Actor │  │ Table Actor │  (N actors)  │
│  │  (users)    │  │ (products)  │  │  (orders)   │              │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘              │
│         │                │                │                      │
│         └────────────────┼────────────────┘                      │
│                          │                                       │
│                          ▼                                       │
│         ┌────────────────────────────────┐                       │
│         │   SharedReaderState (Arc)      │                       │
│         │  ┌──────────────────────────┐  │                       │
│         │  │ broadcast::Sender        │  │  (single sender)      │
│         │  │ partition tasks          │  │  (spans partitions)   │
│         │  │ shutdown_token           │  │                       │
│         │  │ subscriber_count: 3      │  │                       │
│         │  └──────────────────────────┘  │                       │
│         └────────────────────────────────┘                       │
│                          │                                       │
│         ┌────────────────┼────────────────┐                      │
│         ▼                ▼                ▼                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ broadcast:: │  │ broadcast:: │  │ broadcast:: │  (receivers) │
│  │ Receiver    │  │ Receiver    │  │ Receiver    │              │
│  │ (filter:    │  │ (filter:    │  │ (filter:    │              │
│  │  "users")   │  │  "products")│  │  "orders")  │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
                            │
                            ▼
              ┌─────────────────────────────┐
              │   Google Cloud Spanner      │
              │  (single change stream      │
              │   query per partition)      │
              └─────────────────────────────┘
```

### Implementation Details

**Global Registry** (`SHARED_READERS`):
```rust
static SHARED_READERS: Lazy<DashMap<u32, Arc<Mutex<SharedReaderState>>>> = ...;
```
- Keyed by `source_id` (u32)
- `DashMap` enables concurrent access without global lock

**SharedReaderState**:
```rust
struct SharedReaderState {
    source_id: u32,
    tx: broadcast::Sender<Vec<SourceMessage>>,  // Broadcasts to all subscribers
    shutdown_token: CancellationToken,           // Graceful shutdown
    subscriber_count: usize,                     // Lifecycle management
}
```

**Lifecycle**:
1. **First subscriber**: Creates `SharedReaderState`, spawns partition query tasks
2. **Subsequent subscribers**: Subscribe to existing broadcast channel, increment `subscriber_count`
3. **Drop**: Decrement `subscriber_count`, when 0 → cancel tasks, remove from registry

**Benefits**:
| Metric | Before (N readers) | After (Shared) |
|--------|-------------------|----------------|
| Concurrent Spanner queries | N tables × partitions | 1 × partitions |
| Data duplication | Each table reads all changes | Read once, filter per table |
| Resource efficiency | Poor (N× overhead) | Optimal |

---

### Three-Layer State Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                RisingWave State Table (Persisted)               │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐                                           │
│  │ SpannerCdcSplit  │ ← Persisted state, restored on restart    │
│  │ - partition_token│                                           │
│  │ - parent_tokens │                                           │
│  │ - watermark      │ ← Resume position                         │
│  │ - state: Enum    │ ← Created|Running|Finished                 │
│  └────────┬────────┘                                           │
└───────────┼─────────────────────────────────────────────────────┘
            │ update_from_offset()
            ▼
┌─────────────────────────────────────────────────────────────────┐
│              SourceMessage.offset (Checkpoint)                  │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐                                           │
│  │  SpannerOffset   │ ← Lightweight checkpoint format           │
│  │ - timestamp      │                                           │
│  │ - watermark      │                                           │
│  │ - partition_token│                                           │
│  │ - is_finished    │ ← Quick check without full state         │
│  └─────────────────┘                                           │
└───────────┼─────────────────────────────────────────────────────┘
            │ restored from checkpoint
            ▼
┌─────────────────────────────────────────────────────────────────┐
│           PartitionManager (In-memory, NOT persisted)           │
├─────────────────────────────────────────────────────────────────┤
│  • Coordinates parent-child partition relationships             │
│  • Watermark-based scheduling (start_timestamp >= min_watermark)│
│  • Ensures children wait for ALL parents to finish             │
│  • Recreated on restart from persisted splits                  │
└─────────────────────────────────────────────────────────────────┘
```

| Layer | Purpose | Persisted |
|-------|---------|-----------|
| `SpannerCdcSplit` | Full partition state | Yes (state table) |
| `SpannerOffset` | Lightweight checkpoint | Yes (message offset) |
| `PartitionManager` | Runtime coordinator | No (recreated on restart) |

---

### Watermark-Based Scheduling

From Spanner's partition coordination design:
> "To make sure changes for a key is processed in timestamp order, wait until
> the records returned from all parents have been processed."

A child partition is scheduled only when:
1. **ALL** parent partitions have finished (`state == Finished`)
2. AND the child's `start_timestamp >= current_min_watermark`

This ensures timestamp-ordered processing across the partition tree.

**Partition State Machine**:
```
Created → Scheduled → Running → Finished
```

- **Created**: Partition discovered but not yet scheduled
- **Scheduled**: Partition queued for processing
- **Running**: Partition actively being queried
- **Finished**: Partition fully processed (will not be restarted)

---

## Configuration

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `spanner.project` | GCP project ID | `my-project` |
| `spanner.instance` | Spanner instance ID | `my-instance` |
| `database.name` | Spanner database ID | `my-database` |
| `spanner.change_stream.name` | Change stream name | `my_stream` |

### Optional Parameters

#### Connection & Authentication

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spanner.credentials` | - | GCP service account JSON (required for production) |
| `spanner.credentials_path` | - | Path to service account credentials file |
| `spanner.emulator_host` | - | Emulator host for testing (e.g., `http://localhost:9010`) |

**Note**: If neither `credentials` nor `credentials_path` is specified, uses Application Default Credentials (ADC).

#### Change Stream Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spanner.change_stream.max_concurrent_partitions` | `5` | Maximum concurrent change stream partitions to query |
| `spanner.heartbeat_interval` | `3s` | Heartbeat interval for partition health monitoring |
| `spanner.table_name` | - | Filter by upstream table (set via `TABLE 'name'` in CREATE TABLE) |

#### Retry Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spanner.retry_attempts` | `3` | Number of retry attempts |
| `spanner.retry_backoff_ms` | `1000` | Base backoff interval in milliseconds |
| `spanner.retry_backoff_max_delay_ms` | `10000` | Maximum backoff delay in milliseconds |
| `spanner.retry_backoff_factor` | `2` | Multiplier for each retry (doubles each time) |

#### Advanced Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spanner.enable_databoost` | `false` | Enable partitioned reads for faster snapshot backfill |
| `spanner.buffer_size` | `1024` | Buffer size for prefetching messages |
| `auto.schema.change` | `true` | Enable automatic schema change propagation |

---

## Production Configuration Examples

### Basic Production Setup

```sql
CREATE SOURCE spanner_cdc_source WITH (
    connector = 'spanner-cdc',
    spanner.project = 'my-project',
    spanner.instance = 'my-instance',
    database.name = 'my-database',
    spanner.change_stream.name = 'my_stream',
    spanner.credentials_path = '/secrets/spanner-sa.json',
    spanner.heartbeat_interval = '5s',
    spanner.change_stream.max_concurrent_partitions = '10'
) FORMAT PLAIN ENCODE JSON;

CREATE TABLE users FROM spanner_cdc_source TABLE 'users';
CREATE TABLE orders FROM spanner_cdc_source TABLE 'orders';
```

### With Time Range

```sql
CREATE SOURCE spanner_cdc_source WITH (
    connector = 'spanner-cdc',
    spanner.project = 'my-project',
    spanner.instance = 'my-instance',
    database.name = 'my-database',
    spanner.change_stream.name = 'my_stream',
    spanner.credentials = '{"type": "service_account", ...}',
    spanner.start_time = '2024-01-01T00:00:00Z',
    spanner.end_time = '2024-12-31T23:59:59Z'
) FORMAT PLAIN ENCODE JSON;
```

### Using Secrets Manager (Recommended)

```sql
-- Store credentials in RisingWave secrets manager
CREATE SECRET spanner_credentials WITH (
    backend = 'meta'
) AS '{"type": "service_account", "project_id": "...", ...}';

-- Reference the secret in source creation
CREATE SOURCE spanner_cdc_source WITH (
    connector = 'spanner-cdc',
    spanner.project = 'my-project',
    spanner.instance = 'my-instance',
    database.name = 'my-database',
    spanner.change_stream.name = 'my_stream',
    spanner.credentials = SECRET spanner_credentials
) FORMAT PLAIN ENCODE JSON;
```

### With Databoost for Large Tables

```sql
CREATE SOURCE spanner_cdc_source WITH (
    connector = 'spanner-cdc',
    spanner.project = 'my-project',
    spanner.instance = 'my-instance',
    database.name = 'my-database',
    spanner.change_stream.name = 'my_stream',
    spanner.credentials_path = '/secrets/spanner-sa.json',
    spanner.enable_databoost = 'true'  -- Enable faster backfill
) FORMAT PLAIN ENCODE JSON;

-- Rate limit backfill to avoid overwhelming downstream
CREATE TABLE large_table FROM spanner_cdc_source TABLE 'large_table'
WITH (backfill_rate_limit = '5000');
```

---

## Spanner Type Support

### Type Mapping Table

All Spanner types are mapped to RisingWave types with **NO data loss**.

| Spanner Type | RisingWave Type | Notes |
|--------------|-----------------|-------|
| **BOOL** | `BOOLEAN` | Direct mapping |
| **INT64** | `BIGINT` | Direct mapping |
| **FLOAT64** | `DOUBLE PRECISION` | Direct mapping |
| **FLOAT32** | `REAL` | Direct mapping |
| **STRING** | `VARCHAR` | Direct mapping |
| **BYTES** | `BYTEA` | Direct mapping |
| **TIMESTAMP** | `TIMESTAMPTZ` | Direct mapping |
| **DATE** | `DATE` | Direct mapping |
| **NUMERIC** | `DECIMAL` | Direct mapping (read as string, parsed to Decimal) |
| **JSON** | `JSONB` | Direct mapping |
| **ARRAY\<T\>** | `LIST` or `JSONB` | Element-wise mapping; complex arrays as JSONB |
| **STRUCT** | `JSONB` | Serialized structure preserved |
| **PROTO** | `BYTEA` | Raw bytes preserved (can deserialize later) |
| **ENUM** | `VARCHAR` | Enum name preserved |
| **INTERVAL** | `VARCHAR` | Text representation preserved |
| **TIME** | `VARCHAR` | Text representation preserved |

### Type Mapping Strategy

**Primitive Types**: Direct 1:1 mapping with native RisingWave types.

**PROTO Types**: Mapped to `BYTEA` to preserve raw bytes. The data is fully preserved and can be deserialized by the application later.

```rust
// PROTO types logged for observability
tracing::info!("mapping PROTO type 'PROTO.my_proto.Message' to BYTEA (raw bytes preserved)");
```

**ENUM Types**: Mapped to `VARCHAR` preserving the enum name as a string.

```rust
tracing::info!("mapping ENUM type 'ENUM.my_enum' to VARCHAR (enum name preserved)");
```

**STRUCT Types**: Serialized to `JSONB` for full structure preservation.

**ARRAY Types**:
- Simple arrays (`ARRAY<INT64>`, `ARRAY<STRING>`) → `LIST` type
- Complex arrays (STRUCT, PROTO) → `JSONB` for preservation

**Fallback Strategy**: Unknown types are mapped to `VARCHAR` with a warning log, ensuring no data is lost.

```rust
tracing::warn!("unknown Spanner type '{}' mapped to VARCHAR as fallback", spanner_type);
```

---

### Change Stream CDC Type Support

The change stream reader **already supports all Spanner types** because:

1. **Change stream returns data as JSON strings**: The `Mod` struct reads `keys`, `new_values`, and `old_values` as `Option<String>` - these are JSON-encoded strings from Spanner.

2. **Type information is preserved**: The `ColumnType` struct includes `column_types: Vec<ColumnType>` which contains the actual Spanner type for each column.

3. **`Mod::to_json_map()` handles type conversion**:
   - Parses JSON string values
   - Uses `column_types` to determine the correct type
   - Converts numeric strings to proper numbers based on actual column type
   - Outputs Debezium-compatible JSON format (`before`, `after`, `op`)

**No changes needed for change stream CDC** - it handles all Spanner types correctly via JSON interchange format.

---

## Data Format

### Change Event Record (Debezium-Compatible Envelope)

The connector outputs Debezium-compatible JSON format for compatibility with RisingWave's CDC parser:

```json
{
  "before": {"id": 123, "name": "Jane", "email": "jane@example.com"},
  "after": {"id": 123, "name": "John", "email": "john@example.com"},
  "op": "u"
}
```

Operation types:
- `"c"` = Create (INSERT)
- `"u"` = Update
- `"d"` = Delete

**Note**: For `UPDATE` operations with `NEW_ROW` value capture type (no old values), the `before` field is `null` and `op` is `"c"` to ensure correct INSERT semantics.

### Internal Spanner Record Format

Internally, Spanner change streams return records in this format:

```json
{
  "keys": {"id": "123"},
  "new_values": {"name": "John", "email": "john@example.com"},
  "old_values": {"name": "Jane"},
  "mod_type": "UPDATE",
  "value_capture_type": "NEW_ROW_AND_OLD_VALUES",
  "number_of_records_in_transaction": 1,
  "number_of_partitions_in_transaction": 1,
  "transaction_tag": "",
  "is_system_transaction": false
}
```

### Schema Change Event

```json
{
  "change_type": "ALTER",
  "cdc_table_id": "spanner_cdc_my_stream_users",
  "columns": [
    {"name": "id", "type": "int64"},
    {"name": "name", "type": "varchar"},
    {"name": "city", "type": "varchar"}
  ],
  "upstream_ddl": "ALTER TABLE users ADD COLUMN city STRING(MAX)"
}
```

---

## Backfill and CDC Streaming

### Snapshot Backfill

When you create a table FROM a Spanner CDC source, RisingWave performs:

1. **Snapshot Backfill**: Reads existing data from the table using `BatchReadOnlyTransaction`
2. **CDC Streaming**: Starts reading change events from the change stream

**Timestamp Coordination**:
- Backfill uses its own snapshot timestamp from Spanner's TrueTime
- CDC streaming starts from `CURRENT_TIMESTAMP()` at source creation time
- `CdcBackfillExecutor` coordinates the two phases automatically

### Databoost (Opt-in for Large Tables)

Enable faster backfill using Spanner's partitioned reads:

```sql
CREATE SOURCE spanner_source WITH (
    connector = 'spanner-cdc',
    ...,
    spanner.enable_databoost = 'true'  -- Enable partitioned reads
);
```

**Benefits**:
- Faster snapshot backfill (parallel vs sequential)
- Avoids Spanner throttling during large table scans
- Creates multiple partitions for parallel backfill

### Rate Limiting

Control backfill throughput to avoid overwhelming downstream systems:

```sql
-- Session-level (affects all tables in session)
SET backfill_rate_limit = 1000;  -- 1000 rows/second

-- Per-table (when creating table)
CREATE TABLE my_table FROM spanner_source TABLE 'users'
WITH (backfill_rate_limit = '1000');

-- Dynamic adjustment (no restart required)
ALTER TABLE my_table SET BACKFILL RATE LIMIT 1000;

-- Pause backfill
SET backfill_rate_limit = 0;
```

**Scope**:
- ✅ Applies to **snapshot backfill** (reading initial data)
- ❌ Does NOT apply to **CDC streaming** (real-time changes flow at natural rate)

---

## Testing

### Test Coverage

The e2e test suite (`e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial`) covers:

- ✅ Backfill captures initial rows
- ✅ CDC INSERT captures new rows
- ✅ CDC UPDATE captures updates
- ✅ CDC DELETE removes rows
- ✅ Schema evolution: DDL ADD COLUMN + INSERT
- ✅ Shared reader: Multiple tables from same source
- ✅ Databoost: Backfill uses partitioned reads

### Running Tests

Tests require Spanner emulator or production Spanner. Use the remote testing framework:

```bash
# Full workflow (recommended)
make -C scripts/remote run

# With real Spanner (not emulator)
make -C scripts/remote run profile=spanner-real

# Individual steps
make -C scripts/remote sync    # Sync code to remote builder
make -C scripts/remote deploy  # Deploy RisingWave cluster
make -C scripts/remote test    # Run e2e tests
```

See `scripts/remote/README.md` for detailed instructions.

---

## Production Readiness

### Checkpointing & Recovery

- **Full checkpoint support** via `SplitMetaData` trait
- State persisted in RisingWave state table
- Automatic recovery on restart from last committed offset
- Watermark preservation across restarts

### Graceful Shutdown

- Uses `CancellationToken` for task coordination
- Cancellation checks before query attempts and during record processing
- Proper cleanup of broadcast channel subscribers
- Automatic removal from global registry when all subscribers drop

### Retry with Exponential Backoff

Configurable retry logic using `tokio_retry`:

```rust
// Default configuration
retry_attempts: 3
retry_backoff_ms: 1000         // 1 second base
retry_backoff_max_delay_ms: 10000  // 10 seconds max
retry_backoff_factor: 2        // Doubles each retry
```

### Metrics (Auto-Tracked)

All metrics are automatically exposed via Prometheus:

| Metric | Description |
|--------|-------------|
| `partition_input_count` | Number of rows processed |
| `partition_input_bytes` | Number of bytes processed |
| `direct_cdc_event_lag_latency` | Event lag latency in milliseconds |
| `user_source_error` | Parse errors (triggers retry) |

---

## Limitations

### Schema Evolution

**Supported**:
- ✅ Adding columns (ADD COLUMN)

**Not Yet Implemented**:
- ❌ Column type changes
- ❌ Column deletion
- ❌ Table rename
- ❌ Primary key changes
- ❌ Schema rollback

### Other Considerations

- Requires Spanner change streams to be created beforehand
- At-least-once delivery (may have duplicates on failures)
- Emulator has limited functionality compared to production Spanner

---

## Troubleshooting

### "credentials must be set"

Set `spanner.credentials` or `spanner.credentials_path`, or set `spanner.emulator_host` for testing.

### "change stream does not exist"

Create the change stream in Spanner:

```sql
CREATE CHANGE STREAM my_stream FOR ALL OPTIONS (
    retention_period='7d',
    value_capture_type='NEW_ROW_AND_OLD_VALUES'
);
```

### "partition not found"

The partition may have split or merged. The source automatically handles partition splits via the `PartitionManager`.

### Schema changes not appearing

Ensure `auto.schema.change` is enabled (default: `true`).

### PROTO/ENUM columns showing as NULL

This was fixed in the latest version. PROTO types now map to `BYTEA` and ENUM types map to `VARCHAR`. Check that your RisingWave version includes the type mapping updates.

---

## Key Files

### Connector Core

| File | Purpose |
|------|---------|
| `src/connector/src/source/spanner_cdc/mod.rs` | Properties and connector constants |
| `src/connector/src/source/spanner_cdc/enumerator/mod.rs` | Split enumeration |
| `src/connector/src/source/spanner_cdc/partition_manager.rs` | Partition state tracking and coordination |
| `src/connector/src/source/spanner_cdc/source/reader.rs` | CDC streaming with shared reader architecture |
| `src/connector/src/source/spanner_cdc/source/message.rs` | Message conversion to `SourceMessage` |
| `src/connector/src/source/spanner_cdc/split.rs` | Split definition with state and offset |
| `src/connector/src/source/spanner_cdc/schema_track.rs` | Schema tracking for auto evolution |
| `src/connector/src/source/spanner_cdc/types.rs` | Data type definitions and JSON formatting |

### Backfill (External Table Reader)

| File | Purpose |
|------|---------|
| `src/connector/src/source/cdc/external/spanner.rs` | External table reader for snapshot backfill |
| `src/connector/src/source/cdc/external/spanner.rs::spanner_type_to_rw_type()` | Spanner → RisingWave type mapping |
| `src/connector/src/source/cdc/external/spanner.rs::spanner_row_to_owned_row()` | Row value conversion |

### Frontend Integration

| File | Purpose |
|------|---------|
| `src/frontend/src/handler/create_source.rs` | Source creation |
| `src/frontend/src/handler/create_table.rs` | Table creation (skips Debezium-specific logic) |
| `src/connector/src/source/cdc/external/mod.rs` | CDC classification (`ExternalCdcTableType::Spanner`) |

---

## References

- [Spanner Change Streams Documentation](https://cloud.google.com/spanner/docs/change-streams/details)
- [Spanner Type System](https://cloud.google.com/spanner/docs/reference/rest/v1/Type)
- [Spanner Standard SQL Data Types](https://cloud.google.com/spanner/docs/reference/standard-sql/data-types)
- `spream` - Go reference implementation for Spanner change streams
