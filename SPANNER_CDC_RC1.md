# Spanner CDC Source - Release Candidate 1 (RC1)

## Overview

Spanner CDC source connector for RisingWave that follows the shared CDC source pattern (like MySQL/Postgres CDC).

**Key Design Decision**: Non-Debezium connector using native `gcloud-spanner` Rust crate (version 1.x). Uses Debezium-style JSON envelope for compatibility with RisingWave's CDC parser.

## Quick Start

```sql
CREATE SOURCE spanner_source WITH (
    connector = 'spanner-cdc',
    spanner.project = 'my-project',
    spanner.instance = 'my-instance',
    spanner.database = 'my-database',
    spanner.stream_name = 'my-stream',
    spanner.endpoint = 'https://127.0.0.1:9010'  -- For emulator
);

CREATE TABLE my_table () FROM spanner_source TABLE 'users';
```

## Configuration

### Required Properties
- `spanner.project` - GCP project ID
- `spanner.instance` - Spanner instance ID
- `spanner.database` - Spanner database ID
- `spanner.stream_name` - Change stream name

### Optional Properties
| Property | Default | Description |
|----------|---------|-------------|
| `spanner.heartbeat_interval` | 3s | Heartbeat interval for partition health monitoring |
| `spanner.table_name` | - | Filter by upstream table name (used in `CREATE TABLE ... FROM SOURCE TABLE 'table_name'`) |
| `spanner.endpoint` | - | Custom endpoint (for emulator: `https://127.0.0.1:9010`) |
| `spanner.enable_databoost` | false | Enable databoost (partitioned reads) for faster snapshot backfill |
| `spanner.credentials` | - | GCP service account credentials as JSON string |
| `spanner.credentials_path` | - | Path to GCP service account credentials file |
| `spanner.parallelism` | 1 | Number of parallel readers |
| `spanner.max_concurrent_partitions` | 100 | Maximum concurrent partitions |
| `spanner.buffer_size` | 1024 | Buffer size for prefetching messages |
| `spanner.enable_partition_discovery` | true | Enable automatic partition discovery |
| `auto.schema.change` | - | Enable automatic schema change (for CDC sources) |

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
- **Snapshot Backfill**: Reads initial data from Spanner tables
- **Databoost**: Opt-in feature for faster backfill using partitioned reads (`spanner.enable_databoost = 'true'`)
- **Rate Limiting**: Inherited from RisingWave's CDC infrastructure
  - `SET backfill_rate_limit = 1000;` - Limit to 1000 rows/second
  - `WITH (backfill_rate_limit = '1000')` - Set per-table limit during creation
  - `ALTER TABLE ... SET BACKFILL RATE LIMIT 1000;` - Dynamically adjust limit

## Architecture

### Key Files
**Connector Core**:
- `src/connector/src/source/spanner_cdc/mod.rs` - Properties and connector constants
- `src/connector/src/source/spanner_cdc/enumerator/mod.rs` - Split enumeration
- `src/connector/src/source/spanner_cdc/partition_manager.rs` - Partition state tracking
- `src/connector/src/source/spanner_cdc/source/reader.rs` - Data reading and partition coordination
- `src/connector/src/source/spanner_cdc/split.rs` - Split definition with state and watermark
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
- `scripts/remote/` - Remote builder for testing (required - local testing not supported)

## Testing

**IMPORTANT**: Remote-only testing required. Local testing is NOT supported.

```bash
# Full workflow (recommended)
make -C scripts/remote run

# Individual steps
make -C scripts/remote sync      # Sync code
make -C scripts/remote deploy    # Deploy cluster
make -C scripts/remote test      # Run tests
```

See `scripts/remote/README.md` for details.

## Test Coverage

The current tests cover:
- Backfill captures initial rows (id 1, 2)
- CDC INSERT captures new rows (id 3, 4)
- CDC UPDATE captures updates (id 1 age 30→31)
- CDC DELETE removes rows (id 2 Bob)
- Schema evolution: DDL ADD COLUMN `city` + INSERT (id 5, 6)
- Schema evolution: DDL ADD COLUMN `zipcode`, `state` + INSERT (id 7)
- Databoost: Backfill uses partitioned reads for performance

## Known Limitations

### Schema Evolution (Edge Cases)
Current implementation handles:
- ✅ Adding columns

Not yet implemented:
- ❌ Column type changes (e.g., INT64 → FLOAT64)
- ❌ Column deletion
- ❌ Table rename
- ❌ Concurrent schema changes (race condition)
- ❌ Schema rollback (upstream reverts DDL)
- ❌ Primary key changes

### Other Known Limitations
- Partition split handling at scale (100+ partitions)
- Large value handling (rows exceeding Spanner's size limits)
- Comprehensive test coverage (chaos testing, network partitions, etc.)

## References

**Spanner CDC Connector**:
- `src/connector/src/source/spanner_cdc/` - Main connector implementation

**Parser and Metrics**:
- `src/connector/src/parser/mod.rs` - Central parser with lag tracking
- `src/connector/src/source/common.rs` - Auto-tracks partition metrics
- `src/common/metrics/src/error_metrics.rs` - Error tracking

**Spanner Change Streams Documentation**:
- https://cloud.google.com/spanner/docs/change-streams/details

## Development Notes Archive

For detailed development history and design discussions, see `SPANNER_CDC_DEVELOPMENT_NOTES.md`.
