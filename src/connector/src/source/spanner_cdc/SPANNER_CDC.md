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
CREATE TABLE users (*) FROM spanner_source TABLE 'users';
CREATE TABLE orders (*) FROM spanner_source TABLE 'orders';

-- A table in a named schema is referenced as 'schema.table'
CREATE TABLE sales_orders (*) FROM spanner_source TABLE 'sales.orders';
```

Only GoogleSQL-dialect databases are supported; `CREATE SOURCE` rejects a
PostgreSQL-dialect database.

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

CREATE TABLE test_table (*) FROM spanner_test TABLE 'test_table';
```

---

## Architecture

### Reader Architecture (Debezium Pattern)

The Spanner CDC reader follows the **exact same pattern** as RisingWave's Debezium CDC reader (`CdcSplitReader`):

1. `SplitReader::new()` spawns a **background task** that reads from the Spanner change stream
2. The background task sends `Vec<SourceMessage>` through an **`mpsc` channel** (buffer size 16, same as Debezium)
3. `into_data_stream()` calls `rx.recv()` and yields messages. Partition tasks send one batch per change record, so batches already queued behind the current one are merged, up to the source chunk size, before the parser sees them. Schema-change and heartbeat batches are never merged
4. `into_stream()` wraps with `into_chunk_stream` (parser)

**Key Design Decision**: Each CDC source has exactly one split with `split_id = source_id.as_raw_id()`. The source executor reads from one `SpannerCdcSplitReader` via a single `mpsc` channel — identical to how it reads from Debezium's JNI channel.

```
                    Debezium (Postgres CDC)           Spanner CDC
                    ──────────────────────           ───────────
Background task:    JNI thread (std::thread)         tokio::spawn(run_reader)
Channel:            mpsc::channel(16)                mpsc::channel(16)
Send:               tx.blocking_send(events)         tx.send(messages).await
Reader struct:      { rx, parser_config, source_ctx } { rx, parser_config, source_ctx }
into_data_stream:   rx.recv() → yield msgs           rx.recv() + merge queued → yield
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
│  │ - offset         │ ← Watermark; resume point without partitions│
│  │ - index          │ ← source_id.as_raw_id() (unique per source)│
│  │ - partitions     │ ← Each unfinished partition: token,       │
│  │                  │   parents, offset. Resumed one by one     │
│  └────────┬────────┘                                           │
└───────────┼─────────────────────────────────────────────────────┘
            │ update_offset(): message offsets advance `offset`,
            │ progress reports replace `partitions`
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
│  • Rebuilt on restart from `partitions`, or from the watermark  │
│    by re-discovering the tree when there are none               │
└─────────────────────────────────────────────────────────────────┘
```

| Layer | Purpose | Persisted |
|-------|---------|-----------|
| `SpannerCdcSplit` | Watermark + every unfinished partition's progress | Yes (state table) |
| `SpannerOffset` | Checkpoint watermark | Yes (message offset) |
| Runtime coordination | Partition lifecycle (spawn, finish, discover) | Rebuilt from `partitions` on restart |

---

### Partition Coordination

From Spanner's documentation:
> "Due to the parent-child partition lineage, in order to process changes for a particular key in commit timestamp order, records returned from child partitions should be processed only after records from all parent partitions have been processed."

The reader implements this via:

1. **Parent-before-child spawning**: A child partition is only spawned after ALL its parent partitions have finished (returned `PartitionResult`)
2. **Ready pool**: Children whose parents have finished are moved to `ready_pool` and spawned in batch
3. **Deferred children**: Children whose parents haven't finished yet wait in `deferred_children`

**Watermark & Checkpoint**:

The watermark = `min(offset)` across all registered, un-finished partitions. Every message carries it as its offset, and it becomes the split's `offset`: a lower bound that is always safe to resume from, and the value v1 CDC backfill compares against.

Every 5 seconds the reader also reports each unfinished partition's token, parents and offset,
through the same channel as the data and after the batches it covers, as the split's
`partitions`. A partition's offset advances only once its batch is in the channel, so the
report never claims records that were not sent. On restart:

- **With `partitions`**: each saved partition resumes from its own offset. A saved child still
  waits for its saved parents; a parent that is not saved has finished. A restart therefore
  replays about 5 seconds plus the time since the last checkpoint of each partition, not
  everything since the slowest one.
- **Without** (a split saved by an older version, or after `RESET SOURCE`): the root query
  restarts from the watermark and all partitions are re-discovered from scratch.

Queries resume from an offset inclusively, since other records can share its commit timestamp;
the records at that timestamp are read again. A version that predates `partitions` ignores the
field and resumes from `offset`, so a rollback is safe.

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
- The parent task registers each child's offset (its `start_timestamp`) as soon as it
  reads the `ChildPartitionsRecord`, before the parent can finish. Otherwise the
  watermark could jump past the child's start between the parent's removal and the
  main loop draining the discovery channel, and a checkpoint taken then would skip the
  child's first records on recovery.

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

**Note**: Production connections use Application Default Credentials (ADC) if neither
`credentials` nor `credentials_path` is specified. Emulator connections always use
anonymous authentication, including HTTPS endpoints, and reject either credential
option. `spanner.emulator_host` applies only to that client's connection and does
not modify the process environment. Without this option, the connector explicitly
connects to `https://spanner.googleapis.com`; `SPANNER_EMULATOR_HOST` does not select
the endpoint. Configure emulator connections with `spanner.emulator_host`.

ADC authenticates as the RisingWave node, so any user who can create a source can read
every Spanner database the node's service account can reach. Set the
`DISABLE_DEFAULT_CREDENTIAL=true` environment variable on the frontend, meta and compute
nodes to reject sources without explicit credentials, as the Pub/Sub, S3 and Iceberg
connectors do. `spanner.credentials_path` is read by the frontend, meta and compute nodes
from their own file systems and must name a regular file of at most 64 KiB. Prefer
`spanner.credentials = secret ...` over an inline key or a path.

#### Change Stream Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spanner.heartbeat_milliseconds` | `2000` | Heartbeat interval in milliseconds for partition health monitoring. Maps to the `heartbeat_milliseconds` TVF argument. Valid range: 1,000–300,000. |
| `spanner.max_missed_heartbeats` | `10` | Maximum consecutive missed heartbeats before a partition stream is considered stalled and restarted. Stall timeout = `spanner.heartbeat_milliseconds` × `spanner.max_missed_heartbeats`. |
| `spanner.start_timestamp` | current time | Start timestamp for the change stream query (RFC3339 format) |
| `table.name` | - | Filter by upstream table (set via `TABLE 'name'` in CREATE TABLE; `'schema.name'` for a table in a named schema) |

#### Retry Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spanner.retry_attempts` | `5` | Attempts per run of back-to-back query failures; a query that made progress before failing resets the count. `0` and `1` both mean a single attempt |
| `spanner.retry_backoff_ms` | `1000` | Base backoff interval in milliseconds |
| `spanner.retry_backoff_max_delay_ms` | `10000` | Maximum backoff delay in milliseconds |
| `spanner.retry_backoff_factor` | `2` | Multiplier applied to every delay; delays grow by powers of `spanner.retry_backoff_ms` (see "Retry with Exponential Backoff") |

`spanner.max_missed_heartbeats` and the three backoff options must be greater than 0, and
`spanner.heartbeat_milliseconds` must be in its valid range. `CREATE SOURCE` rejects other
values; an `ALTER` that sets one makes the reader fail to start with the same error.

#### Advanced Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spanner.databoost.enabled` | `false` | Enable DataBoost for partitioned snapshot backfill (requires `spanner.databases.useDataBoost` IAM permission) |
| `auto.schema.change` | `false` | Enable automatic schema change propagation |

**Note**: `spanner.databoost.enabled` is a table-level property set automatically by the frontend during `CREATE TABLE FROM source`. It is passed internally and should not be set manually in `CREATE SOURCE`.

#### gRPC Channels (compute node environment)

Every change-stream partition holds one never-ending streaming query, and all partitions of a
reader share one Spanner client. The client opens `SPANNER_NUM_CHANNELS` gRPC channels (default
4, or 1 against the emulator), and each channel carries about 100 concurrent streams. Queries
beyond that wait for a free stream and surface as `establish_timeout` in
`spanner_cdc_partition_query_failure_count`. When a source can have more concurrent partitions
than the channels allow, raise it in the compute node environment, e.g. `SPANNER_NUM_CHANNELS=16`.
The value is process-wide, must be between 1 and 256, and an invalid value makes client creation
fail.

The Spanner client's built-in Cloud Monitoring metrics are compiled out; use the metrics below.

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
ALTER SOURCE spanner_cdc_source CONNECTOR WITH (
    spanner.heartbeat_milliseconds = 5000,
    spanner.max_missed_heartbeats = 200
);
```

Under the hood, `ALTER SOURCE ... CONNECTOR WITH (...)` updates the source catalog and issues a `ConnectorPropsChange` barrier mutation; the running source executor then rebuilds its `SpannerCdcSplitReader` with the new properties. No backfill re-runs, but the new reader restarts every partition query from that partition's last reported progress (see Watermark & Checkpoint), so a few seconds of each partition are read again (at-least-once). Changing `SOURCE_RATE_LIMIT` rebuilds the reader the same way. Only properties registered as `#[with_option(allow_alter_on_fly)]` on `SpannerCdcProperties` (see `mod.rs`) are accepted; anything else is rejected by `check_source_allow_alter_on_fly_fields`.

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

CREATE TABLE users (*) FROM spanner_cdc_source TABLE 'users';
CREATE TABLE orders (*) FROM spanner_cdc_source TABLE 'orders';
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
CREATE TABLE large_table (*) FROM spanner_cdc_source TABLE 'large_table' WITH (
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

All Spanner types are mapped to RisingWave types without data loss, except the non-finite
float values described under [Limitations](#other-considerations).

| Spanner Type | RisingWave Type | Notes |
|--------------|-----------------|-------|
| **BOOL** | `BOOLEAN` | Direct mapping |
| **INT64** | `BIGINT` | Direct mapping |
| **FLOAT64** | `DOUBLE PRECISION` | Direct mapping, including NaN and ±Infinity |
| **FLOAT32** | `REAL` | Direct mapping, including NaN and ±Infinity |
| **STRING** | `VARCHAR` | Direct mapping |
| **BYTES** | `BYTEA` | Direct mapping |
| **TIMESTAMP** | `TIMESTAMPTZ` | Direct mapping |
| **DATE** | `DATE` | Direct mapping |
| **NUMERIC** | `DECIMAL` | Passed to the parser as the exact decimal string (never through `f64`) |
| **JSON** | `JSONB` | Direct mapping |
| **ARRAY\<T\>** | `LIST` | Element-wise mapping; e.g., `ARRAY<INT64>` → `LIST<BIGINT>` |
| **STRUCT\<...\>** | `JSONB` | Serialized structure preserved |
| **PROTO\<...\>** | `BYTEA` | Raw bytes preserved (can deserialize later) |
| **ENUM\<...\>** | `VARCHAR` | Enum name preserved as string |
| **INTERVAL** | `VARCHAR` | Text representation preserved |
| **UUID** | `VARCHAR` | Text representation preserved |
| **TIME** | `VARCHAR` | Text representation preserved |
| *any other type* | `VARCHAR` | Unknown type codes decode as `VARCHAR` instead of failing the record |

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
  │     → Emit schema change event (type: "ALTER")
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

The `SchemaTracker` is in-memory and resets on every restart, so the first `DataChangeRecord` for each table after any startup or recovery triggers a schema change event. It is emitted as `"ALTER"`, never `"CREATE"`: the downstream parser drops `"CREATE"` events, and the first record may already carry a column the RW table lacks — added while the reader was down, or while it was running but before the table's next write. Meta compares the full column list with the table, adds any new columns, and skips the change when there are none. The registry is then populated, and subsequent records with the same schema are skipped without emission.

The cost is one `auto_schema_change` round trip to meta per table after each restart, during which parsing pauses.

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
- Spanner CDC tables always use the parallelized backfill (`backfill.parallelism`
  defaults to 1 instead of 0). The non-parallel `CdcBackfillExecutor` drops change-log
  events whose offset is below a low offset that it advances to each consumed event,
  which assumes a totally ordered log. A change stream interleaves partitions out of
  commit order and each event carries the cross-partition watermark, so that filter
  would drop events the snapshot never saw. The parallelized backfill routes events by
  PK range instead of filtering by offset.

### Rate Limiting

Control backfill throughput to avoid overwhelming downstream systems:

```sql
-- Rate limit is applied per-actor during snapshot backfill
-- Default: 1000 rows/second per actor
SET backfill_rate_limit = 1000;

-- Per-table (when creating table)
CREATE TABLE my_table (*) FROM spanner_source TABLE 'users'
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
- On restart: each partition saved in `partitions` resumes from its own offset; without
  saved partitions the root query restarts from the watermark (see Watermark & Checkpoint)
- `risectl meta inject-source-offsets` only moves `offset` forward. It has no effect while
  `partitions` is saved, since those are resumed first

### Retry with Exponential Backoff

Each partition retries failed change stream queries from its own offset:

```rust
// Default configuration
retry_attempts: 5
retry_backoff_ms: 1000         // 1 second base
retry_backoff_max_delay_ms: 10000  // 10 seconds max
retry_backoff_factor: 2
```

- The budget counts back-to-back failures only. When a failed query had advanced the
  partition's offset (a record or heartbeat arrived), the count and backoff start over, so
  a long-lived partition is not failed by occasional transient errors spread over days.
- A start timestamp older than the change stream's retention period is not retried (see
  "start timestamp older than retention" below).
- Once the budget runs out the reader fails and the source restarts every partition from
  its last reported progress.
- This budget is the only retry layer. The Spanner SDK's own retry is turned off for change
  stream queries: it would retry inside a single call, hidden from the stall timeout and
  from the `spanner_cdc_partition_query_failure_count` metric.
- `tokio-retry`'s `ExponentialBackoff` grows by powers of `retry_backoff_ms`, not of
  `retry_backoff_factor`: with the defaults the delays are 2 s, then the 10 s cap. Each delay
  is jittered to a random value below it.

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
| `spanner_cdc_change_stream_timestamp` | `source_id` | Spanner's current time (`CURRENT_TIMESTAMP()`, microseconds since epoch), sampled on each source-worker tick. This is the upstream head, not how far the source has read: subtract `stream_spanner_cdc_state_timestamp` for the checkpoint lag | `pg_cdc_upstream_max_lsn` |
| `stream_spanner_cdc_state_timestamp` | `source_id` | Checkpointed timestamp in state table (microseconds since epoch) | `stream_pg_cdc_state_table_lsn` |

#### Partition-Lifecycle Metrics

All labelled `source_id`, `source_name`, `fragment_id`. The partition gauges are
re-sampled by the reader's lifecycle loop every 5 seconds; the counters advance
on the corresponding event, and the queue depth is sampled by the source actor
on each dequeue. Partition tokens are never used as label values — they are
unbounded and churn as Spanner splits and merges.

| Metric | Type | Description |
|--------|------|-------------|
| `spanner_cdc_active_partitions` | gauge | Partition tasks currently reading the change stream |
| `spanner_cdc_deferred_partitions` | gauge | Children waiting for their parents to finish |
| `spanner_cdc_watermark_lag_milliseconds` | gauge | Age of the checkpoint watermark, i.e. `now - min(offset)` over unfinished partitions. Floors at `spanner.heartbeat_milliseconds`, since an idle partition only advances on heartbeats |
| `spanner_cdc_newest_partition_lag_milliseconds` | gauge | Age of the *newest* offset, i.e. `now - max(offset)` over unfinished partitions — the partition keeping up best |
| `spanner_cdc_child_partition_discovered_count` | counter | Child partitions discovered through `ChildPartitionsRecord`, excluding the root. Extra label `kind`: `split` (one parent) or `merge` (several) |
| `spanner_cdc_partition_finished_count` | counter | Partition tasks that ran to completion |
| `spanner_cdc_partition_query_count` | counter | Change stream queries issued, including retries — the load this source puts on the Spanner instance |
| `spanner_cdc_partition_query_failure_count` | counter | Failed change stream queries. Extra label `cause`: `establish_timeout`, `stall_timeout`, `query_error`, `row_error`, `decode_error`, `unsupported_value_capture_type`, `no_child_partitions` |
| `spanner_cdc_parsed_chunk_queue_depth` | gauge | Chunks buffered between the parser task and the source actor, sampled on dequeue. Near `8` means the actor is the constraint, near `0` means the parser is |

##### Reading the lag pair

`spanner_cdc_watermark_lag_milliseconds` is the *worst* partition and is what gets stamped as the
message offset, so it is the number that matters for freshness. Comparing it
with `spanner_cdc_newest_partition_lag_milliseconds` tells you which failure you have:

| `..._watermark_lag_milliseconds` | `..._newest_partition_lag_milliseconds` | Interpretation |
|---|---|---|
| high | low | One partition is stuck; it pins the watermark, though a restart resumes the others from their own offsets. Check `spanner_cdc_partition_query_failure_count` by `cause` |
| high | high | The reader is uniformly behind — a genuine throughput limit |
| low | low | Healthy; both floor at `spanner.heartbeat_milliseconds` |

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
- The emulator sends NaN, Infinity and -Infinity in FLOAT64/FLOAT32 change events as JSON
  `null`, so they become NULL there. Production Spanner sends the strings `"NaN"`,
  `"Infinity"` and `"-Infinity"`, which are read as the float values.
- A change record whose `mod_type` is not `INSERT`, `UPDATE` or `DELETE` fails the reader
  instead of being written as an insert.
- DataBoost requires IAM permission `spanner.databases.useDataBoost`

---

## Troubleshooting

### "Google Application Default Credentials are disabled"

`DISABLE_DEFAULT_CREDENTIAL` is set on the node, so a source must name its own credentials.
Set `spanner.credentials` or `spanner.credentials_path`, or set `spanner.emulator_host` for
testing.

### Supported change stream options

`CREATE SOURCE` validates the change stream against `INFORMATION_SCHEMA.CHANGE_STREAM_OPTIONS`:

- `value_capture_type` must be `NEW_ROW` or `NEW_ROW_AND_OLD_VALUES`. Spanner's default,
  `OLD_AND_NEW_VALUES`, and `NEW_VALUES` carry only the modified columns on UPDATE, which
  would write every unmodified column as NULL. The reader also fails on any record with an
  unsupported capture type, in case the stream is altered after the source is created.
- `partition_mode` must be unset or `IMMUTABLE_KEY_RANGE`. `MUTABLE_KEY_RANGE` streams use
  a different record model (partition start/end/event records) that is not implemented.

### "cannot resume from ..., which is older than the change stream's retention period"

The source was stopped (or stuck) for longer than the change stream's `retention_period`, so
its checkpoint is older than the oldest change Spanner still keeps. Spanner rejects the query
with `OUT_OF_RANGE` ("Specified start_timestamp is too far in the past"), which the reader
fails on without retrying. Changes between the checkpoint and the oldest retained change are
lost; recreate the source and its tables to take a new snapshot, and consider a longer
`retention_period`. To keep the tables and accept the gap instead, run
`RESET SOURCE <source>` on the shared source, then trigger a recovery (for example `RECOVER;`
as a superuser, which restarts every streaming job). The reset clears the saved offset, but the
running reader keeps its own copy and does not use the cleared one; after the recovery the
reader starts from the current time. `RESET SOURCE` is not
available for a table created directly with the connector. This wording was observed on the emulator; if production Spanner words it
differently, the reader falls back to retrying and failing with Spanner's message.

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
| `source/message.rs` | `Mod → SourceMessage` conversion (via `ChangeRecordContext`, built once per `DataChangeRecord`); uses `SourceMeta::DebeziumCdc` so messages flow through the standard Debezium CDC path in `PlainParser` |
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
