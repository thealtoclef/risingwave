# Spanner CDC Source Connector

Native Rust implementation of Google Cloud Spanner Change Data Capture (CDC) source for RisingWave.

## Overview

This connector reads data from Google Cloud Spanner change streams and delivers it to RisingWave tables. It supports both **snapshot backfill** (initial data load) and **CDC streaming** (real-time change capture).

### Key Features

- **Native Rust Implementation**: Uses `gcloud-spanner` crate (v1.x), not Debezium
- **Debezium-Pattern Reader**: Same architecture as Postgres CDC — background task, mpsc channel, simple rx.recv()
- **Full Type Support**: All Spanner types supported with data-preserving fallbacks
- **Automatic Partition Management**: Handles parent-child partition splits and merges
- **ReorderBuffer Cross-Partition Ordering**: Records from concurrent partitions are buffered and emitted in commit-timestamp order, with sibling-group gating to prevent ordering violations during partition splits
- **Schema Evolution**: Automatic detection and propagation of schema changes (ADD COLUMN), with correct ordering guaranteed by the ReorderBuffer
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

### Reader Architecture (Debezium Pattern + ReorderBuffer)

The Spanner CDC reader follows the **same pattern** as RisingWave's Debezium CDC reader (`CdcSplitReader`), with one critical addition: a **ReorderBuffer** that re-establishes commit-timestamp ordering across concurrent partition readers.

**Why we need it**: Spanner change-stream partitions are read concurrently with no cross-partition ordering guarantee. A partition that splits into children may produce records that arrive out of order at the reader. The ReorderBuffer buffers, reorders, and emits them correctly.

```
                    Debezium (Postgres CDC)           Spanner CDC
                    ──────────────────────           ───────────
Background task:    JNI thread (std::thread)         tokio::spawn(run_reader)
Channel:            mpsc::channel(16)                mpsc::channel(16)
Send:               tx.blocking_send(events)         tx.send(messages).await
Reader struct:      { rx, parser_config, source_ctx } { rx, parser_config, source_ctx }
into_data_stream:   rx.recv() → yield msgs           rx.recv() → yield msgs
into_stream:        into_chunk_stream(...)            into_chunk_stream(...)
Cross-partition ordering: N/A (Kafka per-topic)      ReorderBuffer (BTreeMap + watermark)
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
                            │ mpsc::Sender (ordered batches, each
                            │ message carries the ReorderBuffer's
                            │ last_emitted_watermark as offset)
              ┌─────────────────────────────────────┐
              │     Background Reader Task           │
              │                                      │
              │  ┌──────────────────────────────┐   │
              │  │      ReorderBuffer            │   │
              │  │  • BTreeMap<ts, records>      │   │
              │  │  • Per-partition watermarks   │   │
              │  │  • Sibling group tracking     │   │
              │  │  • Schema tracking (content)  │   │
              │  │  • last_emitted_watermark     │   │
              │  └──────────┬───────────────────┘   │
              │             │ drain in commit-ts order│
              │  ┌──────────┴───────────────────┐   │
              │  │  Partition Tasks (tokio::spawn)│   │
              │  │  P1 ──→ record_tx ──┐         │   │
              │  │  P2 ──→ record_tx ──┤         │   │
              │  │  P3 ──→ record_tx ──┘         │   │
              │  └──────────┬───────────────────┘   │
              └─────────────┼─────────────────────────┘
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

### ReorderBuffer Design (reorder_buffer.rs)

Named after PostgreSQL's `ReorderBuffer` for logical replication. The concept is identical: records arrive from multiple sources (WAL transactions in PG, partition readers in Spanner) and must be reassembled into a single commit-timestamp-ordered stream.

#### How it works

1. **Buffer**: records from each partition are buffered in a `BTreeMap` keyed by commit timestamp
2. **Reorder**: the global watermark is the minimum high-watermark across all *eligible* partitions (active, non-finished, in a complete sibling group). Records at `commit_ts ≤ global_watermark` are drained in order
3. **Emit**: drained records are emitted as `Vec<SourceMessage>`, with schema-change messages prepended before their data records

#### Global watermark

```
global_watermark = max(
    last_emitted_watermark,    // hard floor: never regress
    min(                       // min across eligible partitions
        partition.watermark
        WHERE partition is active
          AND partition's sibling group is complete (or no group)
    )
)
```

- **Active** = not finished (still reading from Spanner)
- **Eligible** = active AND sibling group is complete (or no group — root partition)
- **`last_emitted_watermark`** = hard floor, prevents any regression. Updated each time records are drained.

When all partitions are finished and buffer is non-empty, returns the highest buffered timestamp to force-drain everything remaining.

#### Sibling group gating

When a parent partition splits or merges, it produces child partitions that all share the same `start_timestamp`. These are **siblings**. The ReorderBuffer gates flushing on sibling-group completeness: no records from any member of a group participate in watermark computation until ALL members have been started.

This prevents the following race:
```
Parent A splits → B1, B2, B3, B4 (all start at T0).
max_concurrent=3 → B1,B2,B3 start. B4 queued.

Without gating: global_wm = min(B1,B2,B3) → emits past T0.
B4 starts at T0 → produces data@150 (pre-DDL schema).
Already emitted data@400 (post-DDL schema). ORDER VIOLATION!

With gating: group {B1,B2,B3,B4} incomplete → buffer, don't flush.
B4 starts → group complete → watermark advances → flush in commit-ts order.
data@150 emitted BEFORE data@400. Correct!
```

**Spanner guarantee that enables this**: "All child partition records returned by a partition have the same start_timestamp" — sibling groups can be identified at discovery time. The parent returns ALL child partition records before its query terminates, so we know the full sibling set immediately.

**Design choice: start immediately, buffer, flush when group complete** (vs. spream's approach of blocking partition starts until `minWatermark` passes). Starting immediately allows parallel I/O — partition tasks read from Spanner concurrently. The BTreeMap buffer is pure in-memory computation, essentially free compared to network latency. This gives better throughput at the cost of slightly higher memory usage during the buffering window.

#### Graceful shutdown

When RisingWave cancels the source (drop, rebalance, scale), `tx.is_closed()` triggers. The reader:

1. Drains the ReorderBuffer (best-effort send)
2. Drops `tx` → `rx.recv()` returns `None` in `into_data_stream`
3. Partition tasks detect `record_tx.is_closed()` and exit their query loops
4. Tokio RAII cleans up `JoinHandle`s (no goroutine leaks)

Identical to Debezium's pattern: in-flight records lost on cancellation are re-delivered from the last checkpoint on restart (at-least-once semantics).

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
│  │ - offset         │ ← ReorderBuffer watermark (monotonic)    │
│  │ - index          │ ← source_id.as_raw_id() (unique per source)│
│  └────────┬────────┘                                           │
└───────────┼─────────────────────────────────────────────────────┘
            │ update_from_offset()
            ▼
┌─────────────────────────────────────────────────────────────────┐
│              SourceMessage.offset (Checkpoint)                  │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐                                           │
│  │  SpannerOffset   │ ← Lightweight checkpoint format           │
│  │ - timestamp      │ ← ReorderBuffer watermark (microseconds)  │
│  └─────────────────┘                                           │
└───────────┼─────────────────────────────────────────────────────┘
            │ restored from checkpoint
            ▼
┌─────────────────────────────────────────────────────────────────┐
│     Runtime Coordination (In-memory, NOT persisted)             │
├─────────────────────────────────────────────────────────────────┤
│  • ReorderBuffer: BTreeMap buffer + global watermark            │
│  • Sibling group tracking: groups declared on parent finish,    │
│    gates emission until all members started                     │
│  • PendingQueue: BTreeMap<start_ts, children> — BFS ordering   │
│    by construction (siblings before children, no runtime sort)  │
│    Deferred children (carrying group_id) at front of bucket so  │
│    sibling groups complete faster                                │
│  • Schema tracking: content-only HashMap, no timestamps/locks   │
│  • HashMap tracks per-partition progress (Pending/Running/Fin)  │
│    for reader-local coordination only (not on executor split)  │
│  • Children wait for ALL parents to finish before starting      │
│  • Recreated on restart from persisted splits                  │
└─────────────────────────────────────────────────────────────────┘
```

| Layer | Purpose | Persisted |
|-------|---------|-----------|
| `SpannerCdcSplit` | Full partition state | Yes (state table) |
| `SpannerOffset` | Lightweight checkpoint | Yes (message offset) |
| Runtime coordination | ReorderBuffer + sibling groups + PendingQueue + schema | No (recreated on restart) |

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
| `spanner.change_stream.max_concurrent_partitions` | `5` | Maximum concurrent change stream partitions to query per reader |
| `spanner.heartbeat_interval` | `3s` | Heartbeat interval for partition health monitoring |
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
| `spanner.partition_query.parallelism` | `1` | Number of concurrent partition queries during snapshot backfill |
| `auto.schema.change` | `false` | Enable automatic schema change propagation |

**Note**: `spanner.databoost.enabled` and `spanner.partition_query.parallelism` are table-level properties set automatically by the frontend during `CREATE TABLE FROM source`. They are passed internally and should not be set manually in `CREATE SOURCE`.

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

-- Enable databoost and set partition parallelism at table level
CREATE TABLE large_table FROM spanner_cdc_source TABLE 'large_table' WITH (
    spanner.databoost.enabled = 'true',        -- Enable DataBoost for backfill
    spanner.partition_query.parallelism = '10' -- 10 concurrent partition queries
);
```

**Important**:
- `spanner.databoost.enabled` and `spanner.partition_query.parallelism` are table-level properties, not source-level
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

Spanner embeds `column_types` metadata in every `DataChangeRecord`. The ReorderBuffer maintains an in-memory `HashMap<String, TableSchema>` — a simple content-based comparison with no timestamps, no locks, and no watermarks.

Schema correctness is guaranteed **by construction**: the ReorderBuffer drains records in commit-timestamp order (ascending BTreeMap scan). Schema changes are checked during drain, so they are always emitted in the correct order relative to data records.

```
DataChangeRecord arrives → PartitionRecord::DataChange
  │
  ├── Buffered in BTreeMap keyed by commit_ts
  │
  └── On drain (when commit_ts ≤ global_watermark):
        ├── Table not yet seen?
        │     → Insert schema; emit schema change message
        │
        ├── Table seen, schemas differ?
        │     → Update schema; emit schema change message
        │
        └── Table seen, schemas same?
              → No emit
```

**Why this works when the old `SchemaTracker` didn't:**

The old `SchemaTracker` used per-table `(schema, last_commit_ts)` with `Arc<RwLock<SchemaTracker>>`. Partition tasks independently emitted schema changes to the shared `tx` channel. No cross-partition ordering → a slow partition's old-schema record could arrive after a fast partition had advanced the schema, causing regression.

The new design eliminates this because:
1. **All records go through the ReorderBuffer** — single-threaded, no locks
2. **BTreeMap drain is ordered by commit_ts** — schema changes are naturally sequenced
3. **Sibling group gating** — prevents late-joining partitions from pulling the watermark backward

**Example — schema change across a DDL boundary:**
```
Partition B2 produces data@400 with NEW schema (column "city" added)
Partition B4 produces data@150 with OLD schema (no "city")

Without ReorderBuffer: B2's data emitted first, schema set to new.
  B4's data arrives later → old schema → REGRESSION or DROP.

With ReorderBuffer: Both buffered. When group completes:
  Drain: data@150 (old schema, first seen → emit schema change)
         data@400 (new schema → emit schema change)
  ORDERING CORRECT: old schema before new schema.
```

### Schema Change Message Format

Schema change messages use the same Debezium JSON format as Postgres CDC, processed by the shared `parse_schema_change` path in `debezium.rs`:

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

### False-Positive on First Encounter

The schema tracker is in-memory and resets on every restart. This means the first `DataChangeRecord` for each table after any startup or recovery always triggers a schema change event, even if the schema hasn't changed.

This mirrors how Postgres CDC (Debezium) works: a RELATION message is sent unconditionally at the start of each session. RisingWave's meta service deduplicates by comparing the incoming column set against the current table columns — if identical, the replace job is skipped.

This design closes the **recovery gap**: if a schema change happened while the reader was down, the first record after restart will carry the new `column_types`, meta will detect the difference, and the table will be updated automatically.

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
- Supports parallel execution via `spanner.partition_query.parallelism`
- DataBoost can be enabled via `spanner.databoost.enabled` for large tables

**Timestamp Coordination**:
- Backfill uses its own snapshot timestamp from Spanner's TrueTime
- CDC streaming starts from `CURRENT_TIMESTAMP()` at source creation time
- `CdcBackfillExecutor` coordinates the two phases automatically

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

Tests require a real Spanner instance. Use the `spanner-real` risedev profile:

```bash
./risedev k
./risedev d spanner-real
./risedev slt 'e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial'
```

---

## Production Readiness

### Checkpointing & Recovery

- **Full checkpoint support** via `SplitMetaData` trait
- State persisted in RisingWave state table
- Automatic recovery on restart from last committed offset (ReorderBuffer watermark)
- Partition state machine: `Pending` → `Running` → `Finished`

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
| `source/reader.rs` | CDC streaming reader — partition tasks → shared channel → ReorderBuffer → ordered emission. `PendingQueue` (BTreeMap by start_ts) provides BFS scheduling by construction |
| `source/message.rs` | `TaggedChangeRecord → SourceMessage` conversion; uses `SourceMeta::DebeziumCdc` so messages flow through the standard Debezium CDC path in `PlainParser` |
| `reorder_buffer.rs` | ReorderBuffer: BTreeMap buffer, global watermark, sibling-group gating, content-based schema tracking. Modeled after PostgreSQL's `ReorderBuffer` for logical replication |
| `split.rs` | Split definition (`SpannerCdcSplit`) — simple single-offset split like other CDC sources. `PartitionState` enum defined here for reader-local use |
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
- [gcloud-spanner](https://github.com/yoshidan/google-cloud-rust) - Rust SDK for Google Cloud Spanner
