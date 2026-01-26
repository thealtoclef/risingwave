# Spanner CDC Source

Native Rust implementation of Google Cloud Spanner Change Data Capture (CDC) source for RisingWave.

## Features

- **Direct Change Stream Access**: Reads directly from Spanner change streams
- **Automatic Partition Management**: Handles parent-child partition splits and merges
- **Watermark-Based Scheduling**: Ensures timestamp-ordered processing across partitions
- **Checkpointing & State Persistence**: Tracks partition state for crash recovery
- **Event-Based Schema Evolution**: Detects and handles schema changes automatically
- **Parallel Reading**: Configurable parallelism for multiple readers
- **Production Ready**: Retry logic, error recovery, and metrics

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    RisingWave State Table (Persisted)               │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐                                            │
│  │ SpannerCdcSplit  │ ← Persisted state, restored on restart       │
│  │ - partition_token│                                            │
│  │ - parent_tokens │                                            │
│  │ - watermark      │ ← Resume position                           │
│  │ - state: Enum    │ ← Created|Running|Finished                   │
│  └────────┬────────┘                                            │
└───────────┼──────────────────────────────────────────────────────┘
            │ update_from_offset()
            ▼
┌─────────────────────────────────────────────────────────────────────┐
│              SourceMessage.offset (Checkpoint)                    │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐                                            │
│  │  SpannerOffset   │ ← Lightweight checkpoint format             │
│  │ - timestamp      │                                            │
│  │ - watermark      │                                            │
│  │ - partition_token│                                            │
│  │ - is_finished    │ ← Quick check without full state          │
│  └─────────────────┘                                            │
└───────────┼──────────────────────────────────────────────────────┘
            │ restored from checkpoint
            ▼
┌─────────────────────────────────────────────────────────────────────┐
│           PartitionManager (In-memory, NOT persisted)            │
├─────────────────────────────────────────────────────────────────────┤
│  • Coordinates parent-child partition relationships                │
│  • Watermark-based scheduling (start_timestamp >= min_watermark)   │
│  • Ensures children wait for ALL parents to finish                │
│  • Recreated on restart from persisted splits                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Design Principles

**Watermark-Based Scheduling** (from spream/subscriber.go:238):
> "To make sure changes for a key is processed in timestamp order, wait until
> the records returned from all parents have been processed."

A child partition is scheduled only when:
1. **ALL** parent partitions have finished (`state == Finished`)
2. AND the child's `start_timestamp >= current_min_watermark`

This ensures timestamp-ordered processing across the partition tree.

**Three-Layer Architecture**:
| Layer | Purpose | Persisted |
|-------|---------|------------|
| `SpannerCdcSplit` | Full partition state | Yes (state table) |
| `SpannerOffset` | Lightweight checkpoint | Yes (message offset) |
| `PartitionManager` | Runtime coordinator | No (recreated on restart) |

## Quick Start

### Basic Usage

```sql
CREATE SOURCE spanner_cdc_source WITH (
    connector = 'spanner_cdc',
    spanner.project = 'my-project',
    spanner.instance = 'my-instance',
    spanner.database = 'my-database',
    spanner.stream_name = 'my_change_stream',
    spanner.credentials = '{"type": "service_account", ...}'
) FORMAT PLAIN ENCODE JSON;
```

### With Emulator (Testing)

```sql
CREATE SOURCE spanner_test WITH (
    connector = 'spanner_cdc',
    spanner.project = 'test-project',
    spanner.instance = 'test-instance',
    spanner.database = 'test-database',
    spanner.stream_name = 'test_stream',
    spanner.emulator_host = 'localhost:9010'
) FORMAT PLAIN ENCODE JSON;
```

## Configuration

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `spanner.project` | GCP project ID | `my-project` |
| `spanner.instance` | Spanner instance ID | `my-instance` |
| `spanner.database` | Spanner database ID | `my-database` |
| `spanner.stream_name` | Change stream name | `my_change_stream` |

### Optional Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spanner.credentials` | - | GCP service account JSON (required for production) |
| `spanner.emulator_host` | - | Emulator host for testing (e.g., `localhost:9010`) |
| `spanner.heartbeat_interval` | `3s` | Heartbeat interval for partition monitoring |
| `spanner.start_time` | now | Start time in RFC3339 format |
| `spanner.end_time` | - | End time in RFC3339 format |
| `spanner.table_name` | all tables | Optional table name filter |
| `spanner.parallelism` | `1` | Number of parallel readers |
| `spanner.max_concurrent_partitions` | `10` | Max concurrent partitions |
| `spanner.auto_schema_change` | `true` | Enable automatic schema evolution |

## Production Configuration

### With Time Range

```sql
CREATE SOURCE spanner_cdc_source WITH (
    connector = 'spanner_cdc',
    spanner.project = 'my-project',
    spanner.instance = 'my-instance',
    spanner.database = 'my-database',
    spanner.stream_name = 'my_change_stream',
    spanner.credentials = '{"type": "service_account", ...}',
    spanner.start_time = '2024-01-01T00:00:00Z',
    spanner.end_time = '2024-12-31T23:59:59Z',
    spanner.heartbeat_interval = '5s',
    spanner.parallelism = '4'
) FORMAT PLAIN ENCODE JSON;
```

### Credentials Setup

**Option 1: Inline JSON**
```sql
spanner.credentials = '{"type": "service_account", "project_id": "my-project", ...}'
```

**Option 2: Secret (Recommended)**
```sql
CREATE SECRET spanner_credentials WITH (backend = 'meta') AS '{"type": "service_account", ...}';

CREATE SOURCE spanner_cdc_source WITH (
    connector = 'spanner_cdc',
    spanner.project = 'my-project',
    spanner.instance = 'my-instance',
    spanner.database = 'my-database',
    spanner.stream_name = 'my_change_stream',
    spanner.credentials = SECRET spanner_credentials
) FORMAT PLAIN ENCODE JSON;
```

## Data Format

### Change Event Record

```json
{
  "keys": {"id": "123"},
  "new_values": {"name": "John", "email": "john@example.com"},
  "old_values": {"name": "Jane"},
  "mod_type": "UPDATE",
  "value_capture_type": "NEW_ROW",
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

### Accessing Fields

```sql
CREATE MATERIALIZED VIEW user_changes AS
SELECT
    (payload->>'keys'->>'id')::int64 as user_id,
    (payload->>'new_values'->>'name') as name,
    (payload->>'new_values'->>'email') as email,
    (payload->>'mod_type') as operation,
    spanner_commit_timestamp,
    spanner_table_name
FROM spanner_cdc_source;
```

## Partition State Machine

Each partition transitions through these states:

```
Created → Scheduled → Running → Finished
```

- **Created**: Partition discovered but not yet scheduled
- **Scheduled**: Partition queued for processing
- **Running**: Partition actively being queried
- **Finished**: Partition fully processed (will not be restarted)

### Parent-Child Coordination

When a partition splits into child partitions:
1. Parent discovers child partitions via `child_partitions_record`
2. Children are added to `PartitionManager` in `Created` state
3. Children wait for **ALL** parents to finish
4. Children wait until `start_timestamp >= min_watermark`
5. Child transitions to `Running` and begins processing

## Checkpointing & State Persistence

### What Gets Persisted

1. **SpannerCdcSplit** (in state table):
   - Full partition state including watermark
   - Used to restore after restart
   - Updated via `update_offset()` from `SpannerOffset`

2. **SpannerOffset** (in message offset):
   - Lightweight checkpoint format
   - Included in every `SourceMessage`
   - Used for fine-grained resume position

### Recovery Process

1. On restart, `SpannerCdcSplit` restored from state table
2. `PartitionManager` recreated from splits
3. Unfinished partitions resume from saved watermark
4. Child partitions discovered after restart wait for parents

## Production Features

### Error Recovery
- Configurable retry attempts (default: 3)
- Exponential backoff between retries
- Graceful handling of transient failures

### Metrics
- Records read counter
- Bytes processed tracking
- Error and retry counters
- Partition statistics

### Schema Evolution
- Event-based detection (no polling required)
- Supports: `ADD COLUMN`, `DROP COLUMN`, `ALTER COLUMN TYPE`
- Schema changes emitted as special CDC messages
- Zero data loss from race conditions

## Testing

See `e2e_test/source_inline/spanner_cdc/README.md` for testing instructions.

## Limitations

- Requires Spanner change streams to be created beforehand
- At-least-once delivery (may have duplicates on failures)
- Emulator has limited functionality compared to production Spanner
- Schema evolution is one-way (adding columns supported, removing may cause issues)

## Troubleshooting

### "credentials must be set"
Set `spanner.credentials` or `spanner.emulator_host` for testing.

### "change stream does not exist"
Create the change stream in Spanner:
```sql
CREATE CHANGE STREAM my_stream FOR ALL OPTIONS (
    retention_period='7d',
    value_capture_type='NEW_ROW_AND_OLD_VALUES'
);
```

### "partition not found"
The partition may have split or merged. The source will automatically handle this.

### Schema changes not appearing
Ensure `spanner.auto_schema_change` is enabled (default: true).

## Development Notes

### Key Files

- `split.rs` - `SpannerCdcSplit` and `PartitionState` definitions
- `partition_manager.rs` - In-memory partition coordination
- `source/reader.rs` - Main reader logic with partition management
- `schema_track.rs` - Event-based schema evolution
- `types.rs` - Change stream record types

### Comparison with Other CDC Sources

Unlike Debezium-based sources (Postgres, MySQL), Spanner CDC requires:
- **PartitionManager** - For parent-child coordination
- **Watermark-based scheduling** - For timestamp-ordered processing
- **SpannerOffset** - Custom offset format with partition state

This is because Spanner change streams expose the partition tree structure directly, while Debezium handles partition complexity internally.

## References

- [Spanner Change Streams Details](https://docs.cloud.google.com/spanner/docs/change-streams/details)
- [Spanner Change Streams Data Model](https://docs.cloud.google.com/spanner/docs/change-streams/data-model)
- [spream](https://github.com/toga4/spream) - Go reference implementation
