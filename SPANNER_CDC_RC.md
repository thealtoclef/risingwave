# Spanner CDC Source - Release Candidate

## Overview

Spanner CDC source connector for RisingWave that follows the shared CDC source pattern (like MySQL/Postgres CDC).

**Key Design Decision**: Non-Debezium connector using native `gcloud-spanner` Rust crate (version 1.x). Uses Debezium-style JSON envelope for compatibility with RisingWave's CDC parser.

## Quick Start

```sql
CREATE SOURCE spanner_source WITH (
    connector = 'spanner-cdc',
    spanner.project = 'my-project',
    spanner.instance = 'my-instance',
    database.name = 'my-database',
    spanner.stream_name = 'my-stream',
    spanner.emulator_host = 'http://localhost:9010'  -- For emulator
);

CREATE TABLE my_table () FROM spanner_source TABLE 'users';
```

## Configuration

### Required Properties
- `spanner.project` - GCP project ID
- `spanner.instance` - Spanner instance ID
- `database.name` - Spanner database ID
- `spanner.stream_name` - Change stream name

### Optional Properties
| Property | Default | Description |
|----------|---------|-------------|
| `spanner.heartbeat_interval` | 3s | Heartbeat interval for partition health monitoring |
| `table.name` | - | Filter by upstream table (set via `CREATE TABLE ... FROM SOURCE TABLE 'name'`) |
| `spanner.emulator_host` | - | Emulator host (e.g., `http://localhost:9010`) |
| `spanner.enable_databoost` | false | Enable partitioned reads for faster snapshot backfill |
| `spanner.credentials` | - | GCP service account credentials as JSON string |
| `spanner.credentials_path` | - | Path to GCP service account credentials file |
| `spanner.max_concurrent_partitions` | 100 | Maximum concurrent partitions |
| `spanner.buffer_size` | 1024 | Buffer size for prefetching messages |
| `spanner.enable_partition_discovery` | true | Enable automatic partition discovery |
| `auto.schema.change` | false | Enable automatic schema change propagation |

### Retry Configuration
| Property | Default | Description |
|----------|---------|-------------|
| `spanner.retry_attempts` | 3 | Number of retry attempts |
| `spanner.retry_backoff_ms` | 1000 | Base backoff interval in milliseconds |
| `spanner.retry_backoff_max_delay_ms` | 10000 | Maximum backoff delay in milliseconds |
| `spanner.retry_backoff_factor` | 2 | Multiplier for each retry (doubles each time) |

### Authentication
If neither `credentials` nor `credentials_path` is specified, uses Application Default Credentials (ADC).

## Features

### Core CDC Features
- **Shared Source Pattern**: `CREATE SOURCE` + `CREATE TABLE ... FROM SOURCE TABLE 'name'`
- **Change Stream Support**: Reads from Spanner change streams with partition coordination
- **Schema Evolution**: Automatic column addition detection and propagation
- **Checkpoint/Persistence**: Full checkpoint support via `SplitMetaData` trait
- **Graceful Shutdown**: Using `CancellationToken` for task coordination
- **Exponential Backoff**: Configurable retry with jitter

### Backfill Features
- **Snapshot Backfill**: Consistent snapshot reads via `BatchReadOnlyTransaction`
- **Timestamp Coordination**: Snapshot timestamp from Spanner's TrueTime serves as CDC offset for `CdcBackfillExecutor` filtering (same pattern as Postgres `pg_current_wal_lsn()`)
- **Databoost**: Opt-in partitioned reads (`spanner.enable_databoost = 'true'`)
- **Rate Limiting**: Inherited from RisingWave's CDC infrastructure
  - `SET backfill_rate_limit = 1000;`
  - `WITH (backfill_rate_limit = '1000')`
  - `ALTER TABLE ... SET BACKFILL RATE LIMIT 1000;`

### Timestamp Design (RC2)
- **CDC phase**: Starts from `CURRENT_TIMESTAMP()` queried from Spanner at CREATE SOURCE time.
- **Backfill phase**: Creates its own `BatchReadOnlyTransaction` at CREATE TABLE time. The transaction's read timestamp (`.rts`) is the snapshot offset.
- **No user-configurable start timestamp**: The CDC always starts from "now" on Spanner's clock. The backfill reader independently acquires its snapshot timestamp. The `CdcBackfillExecutor` handles the gap via its standard filtering logic.

## Architecture

### Non-Regression Design Principle

Spanner CDC extends RisingWave without modifying shared code behavior:

- **`ExternalTableImpl::connect()`**: Spanner is checked first via `ExternalCdcTableType::from_connector()`, then falls through to the existing `CdcSourceType::from()` dispatch for Debezium connectors.
- **`debezium_cdc_source_cols()`**: Not modified. Spanner uses the same column layout.
- **`create_source.rs`**: Debezium-specific properties (snapshot mode, sharing mode, transactional CDC) are gated behind `is_debezium()`.
- **`create_table.rs`**: Non-Debezium connectors skip `database.name` validation and `ExternalTableConfig` creation for Debezium-specific default values.
- **Parser**: Spanner CDC messages are handled in `PlainParser` via `SourceMeta::SpannerCdc` pattern matching, separate from Debezium flow.

### Key Files
**Connector Core**:
- `src/connector/src/source/spanner_cdc/mod.rs` - Properties and connector constants
- `src/connector/src/source/spanner_cdc/enumerator/mod.rs` - Split enumeration
- `src/connector/src/source/spanner_cdc/partition_manager.rs` - Partition state tracking
- `src/connector/src/source/spanner_cdc/source/reader.rs` - CDC streaming with mpsc channel delivery
- `src/connector/src/source/spanner_cdc/source/message.rs` - Message conversion to SourceMessage
- `src/connector/src/source/spanner_cdc/split.rs` - Split definition with state and offset
- `src/connector/src/source/spanner_cdc/schema_track.rs` - Schema tracking for auto evolution
- `src/connector/src/source/spanner_cdc/types.rs` - Data type definitions and JSON formatting

**Frontend Integration**:
- `src/frontend/src/handler/create_source.rs` - Source creation
- `src/frontend/src/handler/create_table.rs` - Table creation (skips Debezium logic)
- `src/connector/src/source/cdc/external/mod.rs` - CDC classification
- `src/connector/src/source/cdc/external/spanner.rs` - External table reader for backfill

**Test Infrastructure**:
- `e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial` - Main test file
- `e2e_test/source_inline/spanner_cdc/prepare-data.rs` - Resource setup
- `scripts/remote/` - Remote builder for testing

## Testing

**IMPORTANT**: Remote-only testing required.

```bash
# Full workflow (recommended)
make -C scripts/remote run

# With real Spanner (not emulator)
make -C scripts/remote run profile=spanner-real

# Individual steps
make -C scripts/remote sync
make -C scripts/remote deploy
make -C scripts/remote test
```

See `scripts/remote/README.md` for details.

## Test Coverage

- Backfill captures initial rows (id 1, 2)
- CDC INSERT captures new rows (id 3, 4)
- CDC UPDATE captures updates (id 1 age 30->31)
- CDC DELETE removes rows (id 2 Bob)
- Schema evolution: DDL ADD COLUMN `city` + INSERT (id 5, 6)
- Schema evolution: DDL ADD COLUMN `zipcode`, `state` + INSERT (id 7)
- Databoost: Backfill uses partitioned reads

## Known Limitations

### Schema Evolution (Edge Cases)
- Adding columns (supported)
- Column type changes, deletion, table rename, PK changes (not yet implemented)

### Other
- Partition split handling at scale (100+ partitions)
- Large value handling
- `PbCdcTableType` mapping uses `Unspecified` for Spanner (no proto enum value yet)

## RC2 Changes from RC1

### Regression Fixes
1. **Restored `ExternalTableImpl::connect()`** - RC1 replaced the `CdcSourceType` dispatch with custom string matching, breaking Postgres/MySQL/SQL Server CDC. RC2 restores the original dispatch and adds Spanner as a separate check.
2. **Restored `debezium_cdc_source_cols()`** - RC1 modified the payload column for ALL CDC sources. RC2 reverts this.
3. **Removed development debug logs from shared code** - `actor.rs`, `cdc_backfill.rs`, `source_executor.rs`, `create_table.rs`, `parser/mod.rs` had development logs scattered through shared paths.

### Simplifications
4. **Removed `spanner.start_timestamp` property** - CDC always starts from Spanner's `CURRENT_TIMESTAMP()`.
5. **Simplified backfill reader** - Single `batch_read_only_transaction()` for all reads. No double-transaction creation or `TimestampBound` recreation.
6. **Fixed unit tests** - Updated `tests.rs` and `partition_manager.rs` tests to match current API.
7. **Cleaned up `plain_parser.rs`** - Removed verbose Spanner-specific debug logs.
8. **Fixed `last_error.unwrap()`** in reader retry logic - replaced with `unwrap_or_else`.
9. **Fixed `message.rs`** - replaced `unwrap()` after error logging with `expect()`.

## References

- Spanner Change Streams: https://cloud.google.com/spanner/docs/change-streams/details
- Development Notes: `SPANNER_CDC_DEVELOPMENT_NOTES.md`
