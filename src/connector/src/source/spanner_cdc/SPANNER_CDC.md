# Spanner CDC Source Connector

Native Rust implementation of Google Cloud Spanner Change Data Capture (CDC) source for RisingWave.

## Overview

This connector reads data from Google Cloud Spanner change streams and delivers it to RisingWave tables. It supports both **snapshot backfill** (initial data load) and **CDC streaming** (real-time change capture).

### Key Features

- **Native Rust Implementation**: Uses the official `google-cloud-spanner` crate (googleapis/google-cloud-rust), not Debezium
- **Debezium-Pattern Reader**: Same architecture as Postgres CDC — background task, mpsc channel, simple rx.recv()
- **Full Type Support**: All Spanner types supported with data-preserving fallbacks
- **Automatic Partition Management**: Handles parent-child partition splits and merges
- **Per-Key Ordering**: Relies on Spanner's non-overlapping key ranges + parent-before-child spawning (no global reorder needed)
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

### Reader Architecture (Debezium Pattern)

The Spanner CDC reader follows the **exact same pattern** as RisingWave's Debezium CDC reader (`CdcSplitReader`):

1. `SplitReader::new()` spawns a **background task** that reads from the Spanner change stream
2. The background task sends `Vec<SourceMessage>` through an **`mpsc` channel** (buffer size 16, same as Debezium)
3. `into_data_stream()` calls `rx.recv()` and yields messages
4. `into_stream()` wraps with `into_chunk_stream` (parser)

**Key Design Decision**: Each CDC source has exactly one split with `split_id = source_id.as_raw_id()`. The source executor reads from one `SpannerCdcSplitReader` via a single `mpsc` channel — identical to how it reads from Debezium's JNI channel.

```
                    Debezium (Postgres CDC)           Spanner CDC
                    ──────────────────────           ───────────
Background task:    JNI thread (std::thread)         tokio::spawn(run_reader)
Channel:            mpsc::channel(16)                mpsc::channel(16)
Send:               tx.blocking_send(events)         tx.send(messages).await
Reader struct:      { rx, parser_config, source_ctx } { rx, parser_config, source_ctx }
into_data_stream:   rx.recv() → yield msgs           rx.recv() → yield msgs
into_stream:        into_chunk_stream(...)            into_chunk_stream(...)
```

```
┌──────────────────────────────────────────────────────────────────┐
│                    RisingWave Streaming Graph                     │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐    │
│  │ Source Executor (Actor 0)                                 │    │
│  │  ┌────────────────────────────────────────────────────┐  │    │
│  │  │ SpannerCdcSplitReader                              │  │    │
│  │  │  rx: mpsc::Receiver ──── rx.recv() ── yield msgs   │  │    │
│  │  └────────────────────────────────────────────────────┘  │    │
│  └──────────────────────────────────────────────────────────┘    │
│         │                                                        │
│         ▼  (dispatcher routes by table)                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ CdcBackfill │  │ CdcBackfill │  │ CdcBackfill │              │
│  │  (users)    │  │ (products)  │  │  (orders)   │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘
                            ▲
                            │ mpsc::Sender
              ┌─────────────────────────────┐
               │   Background Reader Task     │
               │  (partition management,      │
               │   shared schema registry,    │
               │   retry logic)               │
              └──────────────┬──────────────┘
                             │
                             ▼
              ┌─────────────────────────────┐
              │   Google Cloud Spanner      │
              │  (change stream queries)    │
              └─────────────────────────────┘
```

### Message Format

Each `SourceMessage` produced by the reader carries `SourceMeta::DebeziumCdc`, the same meta type used by all other CDC sources (Postgres, MySQL, SQL Server). This means Spanner CDC messages flow through the standard `PlainParser` Debezium CDC path with no special handling required.

The `mpsc` channel carries messages for **all tables** in the change stream. The downstream dispatcher routes messages to the appropriate CdcBackfill actor based on table name — same as how Debezium's shared source routes to multiple CDC tables.

### Backpressure

The `mpsc` channel provides natural **backpressure**: when the source executor is busy (e.g., blocked during schema change processing), the channel fills up and the background reader task waits on `tx.send().await`. This prevents data from being produced faster than it can be consumed — the same semantics as Debezium's `tx.blocking_send()`.

---

### Partition Model

```
┌─────────────────────────────────────────────────────────────────┐
│                RisingWave State Table (Persisted)               │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐                                           │
│  │ SpannerCdcSplit  │ ← Persisted state, restored on restart    │
│  │ - partition_token│                                           │
│  │ - parent_tokens │                                           │
│  │ - offset         │ ← Resume position (commit timestamp)      │
│  │ - index          │ ← source_id.as_raw_id() (unique per source)│
│  └────────┬────────┘                                           │
└───────────┼─────────────────────────────────────────────────────┘
            │ update_split_offset()
            ▼
┌─────────────────────────────────────────────────────────────────┐
│              SourceMessage.offset (Checkpoint)                  │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐                                           │
│  │  SpannerOffset   │ ← Lightweight checkpoint format           │
│  │ - timestamp      │ ← min(offset) across un-finished partitions│
│  └─────────────────┘                                           │
└───────────┼─────────────────────────────────────────────────────┘
            │ restored from checkpoint
            ▼
┌─────────────────────────────────────────────────────────────────┐
│           Runtime Coordination (In-memory, NOT persisted)       │
├─────────────────────────────────────────────────────────────────┤
│  • PartitionOffsets: HashMap<Option<String>, OffsetDateTime>    │
│  • finished: HashMap<Option<String>, bool>                      │
│  • ready_pool: Vec<Split> (parents all finished, spawn all)     │
│  • deferred_children: Vec<Split> (registered, waiting for parents)│
│  • child_discovery_tx: unbounded mpsc channel                   │
│  • Recreated on restart from checkpointed watermark             │
└─────────────────────────────────────────────────────────────────┘
```

| Layer | Purpose | Persisted |
|-------|---------|-----------|
| `SpannerCdcSplit` | Partition identity + offset | Yes (state table) |
| `SpannerOffset` | Checkpoint watermark | Yes (message offset) |
| Runtime coordination | Partition lifecycle (spawn, finish, discover) | No (recreated on restart) |

---

### Partition Coordination

From Spanner's documentation:
> "Due to the parent-child partition lineage, in order to process changes for a particular key in commit timestamp order, records returned from child partitions should be processed only after records from all parent partitions have been processed."

The reader implements this via:

1. **Parent-before-child spawning**: A child partition is only spawned after ALL its parent partitions have finished (returned `PartitionResult`)
2. **Ready pool**: Children whose parents have finished are moved to `ready_pool` and spawned in batch
3. **Deferred children**: Children whose parents haven't finished yet wait in `deferred_children`

**Watermark & Checkpoint**:

The watermark = `min(offset)` across all registered, un-finished partitions. It is the safe recovery point and used for metrics. On restart, the root query restarts from the checkpointed watermark — all partitions are re-discovered from scratch.

```
PartitionOffsets (shared via Arc<Mutex<HashMap>>):
  root:     offset=100  (finished → removed)
  child_a:  offset=150  (active)
  child_b:  offset=120  (active)
  watermark = min(150, 120) = 120
```

**Child Partition Discovery**:
- Child partitions are discovered at runtime via `ChildPartitionsRecord` from Spanner
- An unbounded mpsc channel (`child_discovery_tx`) sends discovered child partitions to the main loop
- `HashMap<Option<String>, bool` tracks which partitions have finished
- Children whose parents haven't finished wait in `deferred_children` until promoted

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
| `spanner.heartbeat_milliseconds` | `2000` | Heartbeat interval in milliseconds for partition health monitoring. Maps to the `heartbeat_milliseconds` TVF argument. Valid range: 1,000–300,000. |
| `spanner.max_missed_heartbeats` | `100` | Maximum consecutive missed heartbeats before a partition stream is considered stalled and restarted. Stall timeout = `spanner.heartbeat_milliseconds` × `spanner.max_missed_heartbeats`. |
| `spanner.start_timestamp` | current time | Start timestamp for the change stream query (RFC3339 format) |
| `table.name` | - | Filter by upstream table (set via `TABLE 'name'` in CREATE TABLE) |

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
| `spanner.databoost.enabled` | `false` | Enable DataBoost for partitioned snapshot backfill (requires `spanner.databases.useDataBoost` IAM permission) |
| `auto.schema.change` | `false` | Enable automatic schema change propagation |

**Note**: `spanner.databoost.enabled` is a table-level property set automatically by the frontend during `CREATE TABLE FROM source`. It is passed internally and should not be set manually in `CREATE SOURCE`.

---

## Live Configuration Updates (`ALTER SOURCE`)

The following properties can be changed on a running source without dropping and recreating it:

| Parameter | Alterable on the fly |
|-----------|-----------------------|
| `spanner.heartbeat_milliseconds` | Yes |
| `spanner.retry_attempts` | Yes |
| `spanner.retry_backoff_ms` | Yes |
| `spanner.retry_backoff_max_delay_ms` | Yes |
| `spanner.retry_backoff_factor` | Yes |
| `spanner.max_missed_heartbeats` | Yes |
| `spanner.databoost.enabled` | No — table-level, set once at `CREATE TABLE FROM ... TABLE '...'` time; consumed only by the one-shot snapshot backfill reader, which has no live-reload path |

```sql
ALTER SOURCE spanner_cdc_source SET (
    spanner.heartbeat_milliseconds = 5000,
    spanner.max_missed_heartbeats = 200
);
```

Under the hood, `ALTER SOURCE ... SET (...)` updates the source catalog and issues a `ConnectorPropsChange` barrier mutation; the running source executor rebuilds its `SpannerCdcSplitReader` with the new properties in place — no restart or backfill re-run required. Only properties registered as `#[with_option(allow_alter_on_fly)]` on `SpannerCdcProperties` (see `mod.rs`) are accepted; anything else is rejected by `check_source_allow_alter_on_fly_fields`.

`spanner.databoost.enabled` cannot be altered this way: it's injected into `CdcTableDesc.connect_properties` at `CREATE TABLE` time and read once by the backfill's external table reader, which doesn't subscribe to `ConnectorPropsChange`.

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
    spanner.heartbeat_milliseconds = 5000
) FORMAT PLAIN ENCODE JSON;

CREATE TABLE users FROM spanner_cdc_source TABLE 'users';
CREATE TABLE orders FROM spanner_cdc_source TABLE 'orders';
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
    spanner.credentials_path = '/secrets/spanner-sa.json'
) FORMAT PLAIN ENCODE JSON;

-- Enable databoost at table level
CREATE TABLE large_table FROM spanner_cdc_source TABLE 'large_table' WITH (
    spanner.databoost.enabled = 'true'         -- Enable DataBoost for backfill
);
```

**Important**:
- `spanner.databoost.enabled` is a table-level property, not source-level
- DataBoost requires IAM permission `spanner.databases.useDataBoost` on the service account
- If DataBoost permission is not available, set `spanner.databoost.enabled = 'false'` to use regular Spanner resources

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
| **NUMERIC** | `DECIMAL` | Direct mapping |
| **JSON** | `JSONB` | Direct mapping |
| **ARRAY\<T\>** | `LIST` | Element-wise mapping; e.g., `ARRAY<INT64>` → `LIST<BIGINT>` |
| **STRUCT\<...\>** | `JSONB` | Serialized structure preserved |
| **PROTO\<...\>** | `BYTEA` | Raw bytes preserved (can deserialize later) |
| **ENUM\<...\>** | `VARCHAR` | Enum name preserved as string |
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

**ARRAY Types**: Element-wise mapping to `LIST` type. For example:
- `ARRAY<INT64>` → `LIST<BIGINT>`
- `ARRAY<STRING>` → `LIST<VARCHAR>`

**Fallback Strategy**: Unknown types are mapped to `VARCHAR` with a warning log, ensuring no data is lost.

```rust
tracing::warn!("unknown Spanner type '{}' mapped to VARCHAR as fallback", spanner_type);
```

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

Schema change messages use the same Debezium JSON format as Postgres CDC, so they are processed by the shared `parse_schema_change` path in `debezium.rs`:

```json
{
  "ddl": "UNKNOWN_DDL",
  "tableChanges": [{
    "id": "users",
    "type": "ALTER",
    "table": {
      "columns": [
        {"name": "id",   "typeName": "INT64"},
        {"name": "name", "typeName": "STRING"},
        {"name": "city", "typeName": "STRING"}
      ]
    }
  }]
}
```

`ddl` is always `"UNKNOWN_DDL"` because Spanner change streams do not carry DDL text, mirroring how Postgres CDC (via Debezium) emits `"UNKNOWN_DDL"` for RELATION messages.

Type names use the Spanner type string (e.g., `"INT64"`, `"STRING"`) and are resolved to RisingWave `DataType` by `spanner_type_name_to_rw_type` inside `parse_schema_change`.

---

## Schema Evolution

### How It Works

Spanner embeds `column_types` metadata in every `DataChangeRecord`. A shared `SchemaTracker` (`schema_track.rs`) acts as a schema registry — one per source, shared across all partition tasks via `Arc<Mutex<>>`:

```
DataChangeRecord arrives (contains column_types + commit_timestamp)
  │
  ├── Table not yet in registry (first encounter)?
  │     → Emit schema change event (type: "CREATE")
  │     → Register schema + commit timestamp
  │
  └── Table already in registry?
        ├── Same schema? → Skip (no allocation on hot path)
        └── Different schema?
              ├── commit_timestamp > stored → Real DDL → Emit (type: "ALTER") + update
              └── commit_timestamp <= stored → Stale partition → Skip, adopt stored schema
```

**Debezium-Pattern Schema Change Emission**

When a schema change is detected, the reader emits the schema change message **as a separate `tx.send()` before the data messages** — mimicking how Debezium emits a Relation message before DML events in the Postgres WAL. This ensures the parser processes the schema change first and triggers `ReplaceStreamJob` before the data records arrive.

```
Schema detected in DataChangeRecord
  │
  ├── 1. Flush any accumulated messages (tx.send)
  ├── 2. Send schema change message alone (tx.send)
  └── 3. Data messages from same record sent in next batch (tx.send)
```

The `mpsc` channel's backpressure ensures the data messages wait until the parser has finished processing the schema change.

**First Encounter After Restart**

The `SchemaTracker` is in-memory and resets on every restart. This means the first `DataChangeRecord` for each table after any startup or recovery triggers a schema change event (type: `"CREATE"`). The downstream parser skips `"CREATE"` events — the initial schema is established by the user's `CREATE TABLE` DDL. The registry is then populated, and subsequent records with the same schema are skipped without emission.

This design closes the **recovery gap**: if a schema change happened while the reader was down, the first record after restart will carry the new `column_types`, the registry will see it as a new schema, and the table will be updated automatically.

### Enabling Schema Evolution

```sql
CREATE SOURCE spanner_source WITH (
    ...,
    auto.schema.change = 'true'
) FORMAT PLAIN ENCODE JSON;
```

### Supported Operations

| Operation | Supported |
|-----------|-----------|
| ADD COLUMN | Yes |
| DROP COLUMN | No |
| Column type change | No |
| Table rename | No |
| Primary key change | No |

---

## Backfill and CDC Streaming

### Snapshot Backfill

When you create a table FROM a Spanner CDC source, RisingWave performs:

1. **Snapshot Backfill**: Reads existing data from the table using `BatchReadOnlyTransaction`
2. **CDC Streaming**: Starts reading change events from the change stream

**Backfill Implementation**:
- Uses `BatchReadOnlyTransaction.partition_query_with_option()` API
- Automatically discovers table schema via INFORMATION_SCHEMA
- DataBoost can be enabled via `spanner.databoost.enabled` for large tables

**Timestamp Coordination**:
- Backfill uses **strong (latest) reads**, so each snapshot reflects the current
  committed state at read time — there is no pinned snapshot timestamp.
- The CDC offset is the read timestamp resolved by a strong read-only transaction
  (`current_cdc_offset()`), the Spanner analogue of Postgres's current WAL LSN.
- CDC streaming starts from `spanner.start_timestamp` (user-provided or auto-generated at source creation time)
- `CdcBackfillExecutor` coordinates the two phases automatically, bracketing the
  change-log against each snapshot read via the CDC offset (matches Postgres/MySQL).

### Rate Limiting

Control backfill throughput to avoid overwhelming downstream systems:

```sql
-- Rate limit is applied per-actor during snapshot backfill
-- Default: 1000 rows/second per actor
SET backfill_rate_limit = 1000;

-- Per-table (when creating table)
CREATE TABLE my_table FROM spanner_source TABLE 'users'
WITH (backfill_rate_limit = '1000');

-- Dynamic adjustment (no restart required)
ALTER TABLE my_table SET BACKFILL RATE LIMIT 1000;

-- Pause backfill
SET backfill_rate_limit = 0;
```

**Scope**:
- Applies to **snapshot backfill** (reading initial data)
- Does NOT apply to **CDC streaming** (real-time changes flow at natural rate)

---

## Testing

### E2E Tests

The e2e test suite (`e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial`) covers:

- Backfill captures initial rows
- CDC INSERT captures new rows
- CDC UPDATE captures updates
- CDC DELETE removes rows
- Shared reader: multiple tables from same source with correct per-table routing
- Schema evolution: ADD COLUMN propagated automatically
- Cluster recovery: CDC resumes from checkpointed offset, schema-evolved tables included
- Cross-table isolation: changes to one table do not affect others

### Running Tests

Tests require a real Spanner instance. Set the Spanner coordinates
via env vars before launching RisingWave; the connector and the e2e
setup script (`prepare-data.rs`) read them via ADC and standard env
discovery:

```bash
export SPANNER_PROJECT="<your-gcp-project>"
export SPANNER_INSTANCE="<your-spanner-instance>"
export SPANNER_DATABASE="<your-database>"
export GOOGLE_APPLICATION_CREDENTIALS="<path-to-service-account.json>"
# or rely on `gcloud auth application-default login` and skip the export above

./risedev k
./risedev d
./risedev slt 'e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial'
```

---

## Production Readiness

### Checkpointing & Recovery

- **Full checkpoint support** via `SplitMetaData` trait
- State persisted in RisingWave state table
- Automatic recovery on restart from last committed watermark
- On restart: root query restarts from watermark, all partitions re-discovered from scratch

### Retry with Exponential Backoff

Configurable retry logic for transient failures:

```rust
// Default configuration
retry_attempts: 3
retry_backoff_ms: 1000         // 1 second base
retry_backoff_max_delay_ms: 10000  // 10 seconds max
retry_backoff_factor: 2        // Doubles each retry
```

### Log Levels

The connector uses appropriate log levels for production:

- **INFO**: Lifecycle events (source creation, partition enumeration, schema changes)
- **DEBUG**: High-frequency operational details (per-record processing, message batches)
- **WARN**: Non-fatal issues (unknown types, fallback conversions)
- **ERROR**: Actual errors requiring attention

### Metrics

RisingWave exposes operational metrics via Prometheus. Spanner CDC has full metric parity with Postgres CDC and MySQL CDC.

#### Spanner-Specific Metrics

| Metric | Labels | Description | Equivalent to |
|--------|--------|-------------|---------------|
| `spanner_cdc_change_stream_timestamp` | `source_id` | Current change stream position (microseconds since epoch) | `pg_cdc_confirmed_flush_lsn` |
| `stream_spanner_cdc_state_timestamp` | `source_id` | Checkpointed timestamp in state table (microseconds since epoch) | `stream_pg_cdc_state_table_lsn` |

#### General CDC Metrics (framework-level, shared with all CDC sources)

| Metric | Description |
|--------|-------------|
| `source_cdc_event_lag_duration_milliseconds` | CDC event lag latency (histogram, labels: `table_name`) |
| `stream_source_output_rows_counts` | Total rows output from source |
| `stream_source_split_change_event_count` | Split change events |
| `stream_cdc_backfill_snapshot_read_row_count` | Rows read during snapshot backfill |
| `stream_cdc_backfill_upstream_output_row_count` | Rows forwarded from upstream CDC |
| `user_source_error_cnt` | Source errors (parse failures, channel errors) |

#### Schema Change Metrics (meta-level)

| Metric | Description |
|--------|-------------|
| `auto_schema_change_success_cnt` | Successful auto schema changes (labels: `table_id`, `table_name`) |
| `auto_schema_change_failure_cnt` | Failed auto schema changes (labels: `table_id`, `table_name`) |
| `auto_schema_change_latency` | Schema change processing latency (histogram) |

---

## Limitations

### Schema Evolution

**Supported**:
- Adding columns (ADD COLUMN) - requires `auto.schema.change = 'true'`

**Not Yet Implemented**:
- Column type changes
- Column deletion
- Table rename
- Primary key changes
- Schema rollback

### Other Considerations

- Requires Spanner change streams to be created beforehand
- At-least-once delivery (may have duplicates on failures)
- Emulator has limited functionality compared to production Spanner
- DataBoost requires IAM permission `spanner.databases.useDataBoost`

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

The partition may have split or merged. The source automatically handles partition splits via runtime child partition discovery. Child partitions are discovered via `ChildPartitionsRecord` from Spanner and coordinated using an mpsc channel.

### Schema changes not appearing

Ensure `auto.schema.change = 'true'` is set in the source properties (default: `false`).

### PROTO/ENUM columns showing as NULL

PROTO types are mapped to `BYTEA` and ENUM types map to `VARCHAR`. If you see NULL values, verify:
1. The change stream has `value_capture_type='NEW_ROW_AND_OLD_VALUES'`
2. The column type in RisingWave matches the expected type (BYTEA for PROTO, VARCHAR for ENUM)

---

## Key Files

### Connector Core

| File | Purpose |
|------|---------|
| `mod.rs` | Source properties (`SpannerCdcProperties`) and connector constants |
| `enumerator/mod.rs` | Split enumeration (`SpannerCdcSplitEnumerator`) — creates single split with `split_id = source_id.as_raw_id()` |
| `source/reader.rs` | CDC streaming reader — follows Debezium pattern (background task → mpsc channel → rx.recv) |
| `source/message.rs` | `TaggedChangeRecord → SourceMessage` conversion; uses `SourceMeta::DebeziumCdc` so messages flow through the standard Debezium CDC path in `PlainParser` |
| `split.rs` | Split definition (`SpannerCdcSplit`) — partition token, parent tokens, offset, index |
| `schema_track.rs` | Shared schema registry for automatic schema evolution; deduplicates schema change events across partitions; emits Debezium-format JSON schema change messages |
| `types.rs` | Spanner data type definitions, JSON serialization, and `spanner_type_name_to_rw_type` mapping used during schema change parsing |

### Backfill (Snapshot Read)

| File | Purpose |
|------|---------|
| `src/connector/src/source/cdc/external/spanner.rs` | External table reader for snapshot backfill using `BatchReadOnlyTransaction` |
| `src/connector/src/source/cdc/external/spanner.rs::spanner_type_to_rw_type()` | Spanner → RisingWave type mapping |
| `src/connector/src/source/cdc/external/spanner.rs::spanner_row_to_owned_row()` | Row value conversion with type handling |

### Shared Parser Path

| File | Purpose |
|------|---------|
| `src/connector/src/parser/plain_parser.rs` | Parses Spanner messages via the standard `SourceMeta::DebeziumCdc` branch — no separate Spanner block needed |
| `src/connector/src/parser/unified/debezium.rs` | `parse_schema_change` — handles schema change JSON for all CDC sources including Spanner; uses `spanner_type_name_to_rw_type` for type resolution |

### Frontend Integration

| File | Purpose |
|------|---------|
| `src/frontend/src/handler/create_source.rs` | Source creation and validation |
| `src/frontend/src/handler/create_table.rs` | Table creation from CDC source (injects table-level properties) |
| `src/connector/src/source/cdc/external/mod.rs` | CDC classification (`ExternalCdcTableType::Spanner`) |

---

## References

- [Spanner Change Streams Documentation](https://cloud.google.com/spanner/docs/change-streams/details)
- [Spanner Type System](https://cloud.google.com/spanner/docs/reference/rest/v1/Type)
- [Spanner Standard SQL Data Types](https://cloud.google.com/spanner/docs/reference/standard-sql/data-types)
- [google-cloud-spanner](https://github.com/googleapis/google-cloud-rust) - Official Rust SDK for Google Cloud Spanner
