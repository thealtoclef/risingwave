# Spanner CDC Source Connector

Native Rust implementation of Google Cloud Spanner Change Data Capture (CDC) source for RisingWave.

## Overview

This connector reads data from Google Cloud Spanner change streams and delivers it to RisingWave tables. It supports both **snapshot backfill** (initial data load) and **CDC streaming** (real-time change capture).

### Key Features

- **Native Rust Implementation**: Uses `gcloud-spanner` crate (v1.x), not Debezium
- **Debezium-Pattern Reader**: Same architecture as Postgres CDC — background task, mpsc channel, simple rx.recv()
- **Full Type Support**: All Spanner types supported with data-preserving fallbacks
- **Automatic Partition Management**: Handles parent-child partition splits and merges
- **Eager Per-Partition Emission**: Records are emitted immediately in per-partition commit-ts order (no global reorder buffer). Per-key correctness is guaranteed by Spanner's one-partition-per-key rule plus the parents-before-children gate, so memory stays bounded and backpressure flows naturally to Spanner
- **Per-Partition Checkpoint**: Each non-finished partition's offset is persisted, so a restart resumes each partition from its own position (Beam `PartitionMetadata` model)
- **Monotonic Schema Evolution**: Schema changes (ADD COLUMN, type-up) are accumulated into a superset that only ever grows — a lagging partition can never shrink the schema, so no destructive DROP COLUMN is ever propagated
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

### Reader Architecture (Debezium Pattern + PartitionCoordinator)

The Spanner CDC reader follows the **same pattern** as RisingWave's Debezium CDC reader (`CdcSplitReader`), with a **PartitionCoordinator** that emits records eagerly, tracks the scalar checkpoint watermark, accumulates a monotonic schema, and exposes a per-partition resume frontier.

**Why no global reorder?** Spanner change-stream partitions are read concurrently with no cross-partition ordering guarantee — but the downstream pipeline needs only **per-key** ordering. A given key lives in exactly one partition at any commit timestamp, and across a split/merge it moves parent→child at a clean `start_timestamp` boundary that the `parents_all_finished` gate enforces. So per-key order holds **without** a global commit-ts buffer. Dropping the global reorder removes the head-of-line blocking and unbounded buffering that the old design suffered, while per-key correctness is unchanged.

```
                    Debezium (Postgres CDC)           Spanner CDC
                    ──────────────────────           ───────────
Background task:    JNI thread (std::thread)         tokio::spawn(run_reader)
Channel:            mpsc::channel(16)                mpsc::channel(16)
Send:               tx.blocking_send(events)         tx.send(messages).await
Reader struct:      { rx, parser_config, source_ctx } { rx, parser_config, source_ctx }
into_data_stream:   rx.recv() → yield msgs           rx.recv() → yield msgs
into_stream:        into_chunk_stream(...)            into_chunk_stream(...)
Ordering model:     N/A (Kafka per-topic)            eager per-partition + per-key gate
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
                            │ mpsc::Sender (eager batches; each message
                            │ carries the scalar checkpoint watermark + the
                            │ per-partition frontier in SpannerOffset)
              ┌─────────────────────────────────────┐
              │     Background Reader Task           │
              │                                      │
              │  ┌──────────────────────────────┐   │
              │  │     PartitionCoordinator      │   │
              │  │  • eager emit (no buffer)     │   │
              │  │  • per-partition offsets      │   │
              │  │  • scalar checkpoint watermark│   │
              │  │  • monotonic schema (superset)│   │
              │  │  • checkpoint_frontier()      │   │
              │  └──────────┬───────────────────┘   │
              │             │ emit eagerly, per-partition│
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

### PartitionCoordinator Design (coordinator.rs)

The coordinator turns records from concurrent partition tasks into a single output
stream **eagerly**, while tracking everything needed to checkpoint and resume.

#### How it works

1. **Eager emit**: on each record, the coordinator produces its `SourceMessage`s
   immediately (a schema-change message prepended when the schema grows) and stages
   them for the next drain. There is no global commit-ts buffer and no emission gate.
2. **Scalar checkpoint watermark**: the monotonic min offset over non-finished
   partitions, with placeholders included. Stamped as each message's scalar
   offset; used by CDC backfill coordination, lag metrics, offset comparison and
   safe root-mode fallback. It is **not** the resume point.
3. **Per-partition frontier** (`checkpoint_frontier()`): every non-finished
   partition with its own offset, parents and running flag. Stamped alongside the
   scalar offset; this is what a restart resumes from.

#### Why eager emission is correct

The pipeline needs only **per-key** ordering, and per-key order is preserved
without a global buffer:

- A key lives in exactly one partition at any commit timestamp, and that partition
  emits its records in commit-ts order (Spanner guarantee).
- Across a split/merge a key moves parent→child at a clean `start_timestamp`
  boundary; the reader's `parents_all_finished` gate drains the parent fully before
  the child starts. So two updates to the same key are never emitted out of order.
- Eager cross-partition interleaving only reorders records of **different** keys,
  which has no effect on the downstream materialized state.

#### Why the scalar offset remains the min checkpoint watermark

The scalar offset is the conservative checkpoint floor: `max(previous,
min(non-finished partition offsets))`. It can lag fast partitions, but that is
intentional. If the per-partition frontier is absent or invalid, root-mode fallback
can safely re-read from this scalar floor without skipping records from a slower or
not-yet-started child partition. Eager emission no longer depends on this value;
per-partition resume is carried by the frontier.

#### Per-partition checkpoint and resume

The frontier is persisted (via `SpannerOffset.partitions` → `SpannerCdcSplit.partitions`).
On restart the reader seeds each non-finished partition from its own offset; any
parent referenced but absent from the frontier must have finished pre-restart and is
marked `Finished` so the gate releases resumed children. Placeholders
(`DeclarePartitions`) ensure not-yet-started split children appear in the frontier at
their `start_ts`, so a restart re-reads them and never skips records they own. This is
consistent under RisingWave's aligned checkpoints: the frontier persisted at barrier
`E` matches exactly the records applied downstream by `E`; everything after is
re-read (idempotent).

Partition lifecycle transitions can change the frontier without producing data rows
(for example, a partition exhausts quietly and drops out of the frontier). The reader
emits a checkpoint-only heartbeat for these transitions so the normal
`SourceMessage.offset` → split-state path still persists the updated frontier.

If the restored split has **no** frontier (a fresh enumerator split or a legacy
single-offset split), the reader falls back to **root mode** — start one root
partition from the scalar offset and re-discover the tree. This is the always-correct
migration/bootstrap path.

#### Monotonic schema accumulator

Spanner embeds `column_types` in every `DataChangeRecord`. The coordinator keeps a
per-table accumulated **superset** that only ever grows:

- New column → merge in and emit an ALTER (ADD is monotonic and idempotent in meta).
- Type change → applied only after the table has an observed schema-change timestamp
  and `commit_ts >= last_schema_commit_ts`, so an older out-of-order record cannot
  flip a column's type back. When the reader has a table-scoped live-schema seed
  from `table.name`, first-seen type changes are accepted only at or after the seed
  floor (`max` persisted partition offset); older records cannot regress the live
  type. Shared all-table sources do not use a single global seed.
- A record carrying **fewer** columns → no schema change, no shrink. The meta service
  therefore never receives a subset schema and never issues a destructive DROP COLUMN.
  Missing non-PK columns are NULL-filled downstream; convergence to the superset holds
  under reordering.

#### Graceful shutdown

When RisingWave cancels the source (drop, rebalance, scale), `tx.is_closed()` triggers. The reader:

1. Drains the coordinator (best-effort send)
2. Drops `tx` → `rx.recv()` returns `None` in `into_data_stream`
3. Partition tasks detect `record_tx.is_closed()` and exit their query loops
4. Tokio RAII cleans up `JoinHandle`s (no task leaks)

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
│  │ - offset         │ ← scalar checkpoint watermark (monotonic)│
│  │ - partitions     │ ← per-partition resume frontier           │
│  │ - index          │ ← source_id.as_raw_id() (unique per source)│
│  └────────┬────────┘                                           │
└───────────┼─────────────────────────────────────────────────────┘
            │ update_from_offset()
            ▼
┌─────────────────────────────────────────────────────────────────┐
│              SourceMessage.offset (Checkpoint)                  │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐                                           │
│  │  SpannerOffset   │ ← Checkpoint format                       │
│  │ - timestamp      │ ← scalar checkpoint watermark (micros)    │
│  │ - partitions     │ ← Vec<PartitionOffset> resume frontier    │
│  └─────────────────┘                                           │
└───────────┼─────────────────────────────────────────────────────┘
            │ restored from checkpoint
            ▼
┌─────────────────────────────────────────────────────────────────┐
│     Runtime Coordination (In-memory, NOT persisted)             │
├─────────────────────────────────────────────────────────────────┤
│  • PartitionCoordinator: eager emit + per-partition offsets +   │
│    scalar checkpoint watermark + monotonic schema accumulator   │
│  • PendingQueue: BTreeMap<start_ts, children> — BFS ordering   │
│    by construction (parents before children, no runtime sort)  │
│  • HashMap tracks per-partition progress (Pending/Running/Fin)  │
│    for reader-local coordination (seeded from the frontier on   │
│    resume; absent parents marked Finished)                      │
│  • Children wait for ALL parents to finish before starting      │
│  • Recreated on restart from the persisted frontier (or root    │
│    mode when the frontier is empty)                             │
└─────────────────────────────────────────────────────────────────┘
```

| Layer | Purpose | Persisted |
|-------|---------|-----------|
| `SpannerCdcSplit` | Scalar offset + per-partition frontier | Yes (state table) |
| `SpannerOffset` | Checkpoint watermark + frontier | Yes (message offset) |
| Runtime coordination | PartitionCoordinator + PendingQueue + progress | No (recreated on restart) |

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

Spanner embeds `column_types` metadata in every `DataChangeRecord`. The coordinator
maintains an in-memory per-table accumulated **superset** (`HashMap<String, TableSchema>`)
plus a per-table `last_schema_commit_ts`. The schema only ever grows.

```
DataChangeRecord arrives → PartitionRecord::DataChange
  │
  └── evolve_schema (eager, on arrival):
        ├── Table not yet seen?
        │     → store schema; emit full schema change
        │
        ├── New column(s) present?
        │     → merge into the superset; emit ALTER with the superset
        │
        ├── Type change AND commit_ts ≥ type-change floor?
        │     → apply the new type; emit ALTER
        │
        └── Subset / no change?
              → no emit, no shrink  (never a DROP)
```

**Why this is robust without a global reorder:**

1. **ADD COLUMN is monotonic and order-insensitive** — whichever partition first
   observes a new column triggers the ALTER; the meta service applies it
   idempotently. A later record missing that column is a no-op (subset → no emit).
2. **Never a subset → never a DROP.** The meta service treats a schema with fewer
   columns as a DROP COLUMN (and that path is not idempotent on replay). By never
   emitting a subset, the coordinator closes that data-loss hazard entirely.
3. **Type changes are commit-ts gated** so an older out-of-order record cannot revert
   a newer type. After restart, a table-scoped live-schema seed uses the max
   persisted partition offset as the minimum safe timestamp for first-seen type
   changes; shared sources skip seeding to avoid applying one table's schema to another.

**Example — schema change across a DDL boundary (eager + monotonic):**
```
Partition (fast) produces data@400 with NEW schema (column "city" added)
Partition (slow) produces data@150 with OLD schema (no "city")

Fast record observed first → ALTER ADD "city"; superset = {id, name, city}.
Slow record observed later → subset of the superset → NO schema change, NO drop.
  Its missing "city" is NULL-filled downstream.
Result: superset retained, no column ever lost, regardless of arrival order.
```

> **Design note**: not propagating a real Spanner DROP COLUMN is the deliberate,
> safe choice under "don't lose data" — a dropped source column is retained in the
> materialized view (future upserts NULL it) rather than destructively removed.

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

### First Encounter After Restart

The schema tracker is in-memory and resets on every restart. For unseeded tables,
the first `DataChangeRecord` for each table after startup or recovery triggers a
schema change event even if the schema has not changed.

This mirrors how Postgres CDC (Debezium) works: a RELATION message is sent unconditionally at the start of each session. RisingWave's meta service deduplicates by comparing the incoming column set against the current table columns — if identical, the replace job is skipped.

For a single tracked table (`table.name` is present), the reader seeds the
coordinator from RisingWave's restored table schema. This suppresses false DROP
signals from lagging subset records and allows first-seen type changes only when the
record timestamp is at or after the seed floor. For shared all-table sources, the
seed is skipped because the parser column list belongs to one output schema, not a
per-table schema map.

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
| Column type change | Yes (commit-ts gated; seeded restart requires record timestamp at/after seed floor) |
| DROP COLUMN | Not propagated — column is retained, never dropped (safe under "don't lose data") |
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
- Backfill uses `spanner.snapshot_ts` (auto-generated at table creation time)
- CDC streaming starts from `spanner.start_timestamp` (user-provided or auto-generated at source creation time)
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
- State persisted in RisingWave state table (scalar checkpoint watermark + per-partition frontier)
- Automatic recovery on restart: each non-finished partition resumes from its own offset
  (frontier); falls back to root mode from the scalar offset when no frontier exists
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

**Supported** (requires `auto.schema.change = 'true'`):
- Adding columns (ADD COLUMN) — accumulated into a monotonic superset
- Column type changes — applied in commit-ts order; seeded restart accepts first-seen type changes only at or after the seed floor

**By design (not propagated)**:
- Column deletion — a dropped source column is **retained** in the materialized view rather than destructively dropped (the schema only ever grows). This is the safe choice under "don't lose data".

**Not Yet Implemented**:
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
| `source/reader.rs` | CDC streaming reader — partition tasks → shared channel → PartitionCoordinator → eager emission; stamps the checkpoint watermark + frontier; resume-mode seeding + root-mode fallback. `PendingQueue` (BTreeMap by start_ts) provides BFS scheduling by construction |
| `source/message.rs` | `TaggedChangeRecord → SourceMessage` conversion; uses `SourceMeta::DebeziumCdc` so messages flow through the standard Debezium CDC path in `PlainParser` |
| `coordinator.rs` | PartitionCoordinator: eager per-partition emission, scalar checkpoint watermark, per-partition checkpoint frontier, monotonic superset schema accumulator |
| `split.rs` | Split definition (`SpannerCdcSplit`) — scalar offset + per-partition `partitions` frontier. `PartitionState` enum defined here for reader-local use |
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
