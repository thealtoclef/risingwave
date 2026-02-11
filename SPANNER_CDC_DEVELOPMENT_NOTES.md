# Spanner CDC Source - Development Notes

> **Release Candidate**: See [SPANNER_CDC_RC.md](SPANNER_CDC_RC.md) for the release candidate document.

## RC2 Changelog (2025-02-11)

### Regression Fixes (Critical)
- **`ExternalTableImpl::connect()`**: RC1 replaced the `CdcSourceType::from()` dispatch with custom string matching (`contains("mysql")`, etc.), which broke Postgres/MySQL/SQL Server CDC. RC2 restores the original dispatch and adds Spanner as a separate check via `ExternalCdcTableType::from_connector()`.
- **`debezium_cdc_source_cols()`**: RC1 added `AdditionalColumnType::Payload` to the payload column descriptor for ALL CDC sources. RC2 reverts this -- the original `ColumnDesc::named()` is correct.
- **Development debug logs in shared code**: Removed all `tracing::debug!`/`tracing::info!` logs that were added for Spanner debugging to shared files: `actor.rs`, `cdc_backfill.rs`, `source_executor.rs`, `create_table.rs`, `parser/mod.rs`.

### Timestamp Design Simplification
- **Removed `spanner.start_timestamp` property**: The CDC phase always starts from `CURRENT_TIMESTAMP()` queried from Spanner's clock. No user override.
- **Simplified enumerator**: Always queries `SELECT CURRENT_TIMESTAMP()` from Spanner. No conditional branch.
- **Simplified `SpannerExternalTableReader`**: Just calls `batch_read_only_transaction()` once. The transaction's `.rts` is the snapshot offset. No double-transaction creation, no `TimestampBound` recreation.
- **Timestamp coordination**: CDC and backfill readers acquire their timestamps independently (same pattern as Postgres CDC). `CdcBackfillExecutor` handles the gap via filtering.

### Code Quality
- **Fixed `last_error.unwrap()`** in reader retry logic -- replaced with `unwrap_or_else`.
- **Fixed `message.rs`** -- replaced panicking `unwrap()` with `expect()`.
- **Removed debug logs with `===` markers** from `split.rs`.
- **Fixed unit tests** -- `tests.rs` rewrote broken tests, `partition_manager.rs` fixed `Option<OffsetDateTime>` mismatches.
- **Fixed existing test compilation** -- added `..Default::default()` to `ExternalTableConfig` struct literals in `postgres.rs` and `mysql.rs` tests.
- **Cleaned `plain_parser.rs`** -- removed verbose `target: "spanner_cdc_schema_evolution"` debug logs.

### Fail-Fast Parser (Separate Commit)
The fail-fast error handling in `parser/mod.rs` (propagate parse errors instead of skipping) is kept but committed separately as a general RisingWave behavior change, not Spanner-specific.

---

## Feature Overview

**Goal**: Implement a production-ready Google Cloud Spanner CDC (Change Data Capture) source connector for RisingWave that follows the shared CDC source pattern (like MySQL/Postgres CDC).

## Key Requirements

1. **Shared Source Pattern**:
   - `CREATE SOURCE spanner_source WITH (...)` - defines connection and change stream
   - `CREATE TABLE table_name (...) FROM spanner_source TABLE 'upstream_table'` - creates table from source with table name filtering

2. **Non-Debezium Connector**:
   - Spanner CDC is NOT a Debezium-based connector
   - Uses native `gcloud-spanner` Rust crate (version 1.x)
   - Must be classified separately from Debezium connectors
   - Uses Debezium-style JSON envelope for compatibility with RisingWave's CDC parser

3. **Emulator-Only Testing**:
   - All tests use Spanner emulator (no production Spanner support in tests)
   - Emulator runs via Docker on ports 9010 (gRPC) and 9020 (REST)
4. **Remote-Only Testing (IMPORTANT)**:
   - **NEVER test locally** - Always use remote builder for testing
   - Use `make -C scripts/remote run` for full workflow
   - Full workflow: sync → clean → deploy → test
   - Local testing is NOT supported due to Docker/Spanner emulator requirements
   - See `scripts/remote/README.md` for details

## Core Parser Changes: Fail-Fast Error Handling (2025-01-30)

### Background: Data Consistency vs Stream Continuity

**Historical Design**: RisingWave's parser used a "skip and continue" approach for parse errors:
- When a message failed to parse, it was logged, reported to metrics, and **skipped**
- The stream continued processing subsequent messages
- This prioritized **stream continuity** over **data consistency**

**Problem**: This approach causes **data loss** - failed messages are permanently lost and cannot be recovered.

**User Decision**: For Spanner CDC and data consistency use cases, **fail-fast behavior is required**. If a message cannot be parsed, the entire stream must fail to alert operators immediately rather than silently losing data.

### Implementation: Core-Level Change

**File Modified**: `src/connector/src/parser/mod.rs`

**Change**: Modified the message parse error handling to propagate errors instead of skipping:

```rust
// OLD BEHAVIOR (skip and continue):
res @ (Ok(ParseResult::Rows) | Err(_)) => {
    if let Err(error) = res {
        tracing::error!("failed to parse message, skipping");
        GLOBAL_ERROR_METRICS.user_source_error.report([...]);
        // Stream continues - message is lost
    }
    for chunk in chunk_builder.consume_ready_chunks() {
        yield chunk;
    }
}

// NEW BEHAVIOR (fail fast):
res @ (Ok(ParseResult::Rows) | Err(_)) => {
    if let Err(error) = res {
        tracing::error!("failed to parse message, failing stream");
        GLOBAL_ERROR_METRICS.user_source_error.report([...]);
        // PROPAGATE ERROR - stream fails immediately
        return Err(error);
    }
    for chunk in chunk_builder.consume_ready_chunks() {
        yield chunk;
    }
}
```

### Impact on ALL Sources

This is a **core-level change** that affects ALL sources using the parser:

**Affected Sources**:
- Kafka (JSON, Avro, Protobuf, CSV)
- Debezium CDC (MySQL, Postgres, SQL Server, MongoDB, Oracle)
- Maxwell, Canal
- Plain/JSON
- **Spanner CDC**

**Behavior Change**:
- **Before**: Parse error → log → skip message → continue
- **After**: Parse error → log → **fail entire stream** → retry from checkpoint

### Trade-offs

**Pros (Data Consistency)**:
- ✅ No silent data loss
- ✅ Immediate alerting when data cannot be parsed
- ✅ Stream fails fast, operators can investigate the root cause
- ✅ After fix, stream resumes from checkpoint with correct data

**Cons (Availability)**:
- ⚠️ Stream stops on parse error (requires manual intervention or automatic retry)
- ⚠️ May require more robust upstream data quality
- ⚠️ Temporary schema mismatches cause stream failure

### Rationale for Core-Level (vs Per-Source)

**Why Core-Level**:
1. **Consistent Data Guarantees**: All sources now have the same data consistency behavior
2. **No Hidden Data Loss**: No source silently loses data
3. **Simplicity**: Single implementation path, easier to maintain
4. **Aligns with User Requirements**: Data consistency > stream availability

**Why NOT Per-Source**:
- Would create inconsistent behavior across sources
- Users might not be aware their source is "losing data silently"
- More complex implementation and testing

### Mitigation: Stream Retry Mechanism

RisingWave's source executor has built-in retry logic (`reader_stream.rs`):

```rust
Err(e) => {
    tracing::error!("stream source reader error");
    GLOBAL_ERROR_METRICS.user_source_error.report([...]);
    is_error = true;
    break 'consume;  // Stop stream
}

// After error, retry after 1 second:
tokio::time::sleep(Duration::from_secs(1)).await;
// Stream rebuilds and retries from last checkpoint
```

**This means**:
- Temporary errors (network glitches, transient schema issues) will recover
- Stream resumes from the last committed offset (checkpoint)
- No data is permanently lost for recoverable errors

### Testing Considerations

**Tests That May Need Updates**:
- Tests that rely on "skip invalid message" behavior
- Tests with intentionally malformed data
- `e2e_test/source_inline/kafka/protobuf/presence_handling.slt`

**New Test Pattern**:
```sql
-- Old: Expected invalid message to be skipped
-- New: Expect stream to fail on invalid message

-- For tests with invalid data, either:
-- 1. Fix the test data to be valid
-- 2. Test that the stream fails as expected
```

## Current Status

**Test Status**: ✅ PASSING - All CDC operations including schema evolution work correctly!

### ✅ Completed Features

**Core Implementation**:
- Core connector implementation (`src/connector/src/source/spanner_cdc/`)
- Shared source classification (`ExternalCdcTableType::Spanner`)
- Frontend integration (skip Debezium-specific logic for Spanner)
- Test infrastructure (`e2e_test/source_inline/spanner_cdc/`)
- Emulator setup via `risedev` (`gcloud-spanner.toml`, `spanner_service.rs`)
- Resource creation script (`prepare-data.rs` using `gcloud` CLI)

**Configuration & Properties**:
- **Configuration Format**: Changed from single `spanner.dsn` to separate `spanner.project`, `spanner.instance`, `database.name` properties
- **Backfill Support**: Implemented `SpannerExternalTableReader` with snapshot read capability
- **Databoost Support**: Opt-in feature for faster snapshot backfill using Spanner's partitioned reads
  - Property: `spanner.enable_databoost = 'true'` (default: `false`)
  - Uses `partition_query_with_option()` with `data_boost_enabled = true`
  - Creates multiple partitions for parallel backfill instead of single split

**CDC Streaming**:
- **CDC JSON Format**: Fixed to produce Debezium envelope (`before`, `after`, `op` fields)
- **Partition Split Deduplication**: Thread-safe deduplication using `Arc<Mutex<HashSet<Option<String>>>>`
- **Stream Keep-Alive**: Periodic empty chunk yielding (1 second interval)

**Schema Evolution**:
- **Schema Evolution Working**: Automatic column addition via DDL detected and applied correctly
- Schema tracker in `schema_track.rs` properly detects new columns from `column_types` metadata

**Production Readiness Features**:
- **Checkpoint/Persistence**: Full checkpoint support via `SplitMetaData` trait - prevents data loss on restart
- **Graceful Shutdown**: Using `CancellationToken` for task coordination
  - Cancellation checks before query attempts and during record processing
  - Watermark preservation via RisingWave's checkpoint system
- **Exponential Backoff on Retry**: Using `tokio_retry` with jitter
  - `spanner.retry_backoff_ms`: Base interval (default: 1000ms = 1 second)
  - `spanner.retry_backoff_max_delay_ms`: Max delay cap (default: 10000ms = 10 seconds)
  - `spanner.retry_backoff_factor`: Multiplier (default: 2, doubles each retry)
- **Production Credentials**: Support for ADC and service account files
  - `spanner.credentials_path`: Path to service account file
  - `spanner.credentials`: JSON string credentials
  - Falls back to Application Default Credentials (ADC)
- **Fail-Fast Error Handling**: Follows RisingWave's CDC design
  - Errors cause stream to stop immediately (no silent data loss)
  - Stream rebuilds and retries from last checkpoint
  - JSON serialization errors fail with detailed error message (table name, mod_type)
- **Rate Limiting**: Inherited from RisingWave's CDC backfill infrastructure
  - `SET backfill_rate_limit = 1000;` - Limit snapshot backfill to 1000 rows/second
  - `WITH (backfill_rate_limit = '1000')` - Set per-table limit during creation
  - `ALTER TABLE ... SET BACKFILL RATE LIMIT 1000;` - Dynamically adjust limit
  - Applies to snapshot backfill only (streaming flows at Spanner's natural change rate)

### Databoost Feature

**Property**: `spanner.enable_databoost = 'true'` (opt-in, defaults to `false`)

**User configuration**:
```sql
CREATE SOURCE spanner_source WITH (
    connector = 'spanner-cdc',
    spanner.project = 'my-project',
    spanner.instance = 'my-instance',
    database.name = 'my-database',
    spanner.stream_name = 'my-stream',
    spanner.enable_databoost = 'true',  -- Enable faster backfill
    ...
);

CREATE TABLE my_table FROM spanner_source TABLE 'users';
```

**Benefits**:
- Faster snapshot backfill (parallel vs sequential)
- Avoids Spanner throttling during large table scans
- Reduces backfill time for large tables

## Architecture

### Key Files

**Connector Core**:
- `src/connector/src/source/spanner_cdc/mod.rs` - Properties and connector constants
- `src/connector/src/source/spanner_cdc/enumerator/mod.rs` - Split enumeration (creates root split)
- `src/connector/src/source/spanner_cdc/partition_manager.rs` - Partition state tracking and coordination
- `src/connector/src/source/spanner_cdc/source/reader.rs` - Data reading, partition coordination, heartbeat handling
- `src/connector/src/source/spanner_cdc/source/message.rs` - Message conversion to SourceMessage
- `src/connector/src/source/spanner_cdc/types.rs` - Data type definitions and JSON formatting
- `src/connector/src/source/spanner_cdc/split.rs` - Split definition with state, watermark, parent tracking
- `src/connector/src/source/spanner_cdc/schema_track.rs` - Schema tracking for auto evolution

**Frontend Integration**:
- `src/frontend/src/handler/create_source.rs` - Source creation (imports `SPANNER_CDC_CONNECTOR`)
- `src/frontend/src/handler/create_table.rs` - Table creation (skips `ExternalTableConfig` for non-Debezium)
- `src/connector/src/source/cdc/external/mod.rs` - CDC classification (`ExternalCdcTableType::Spanner`)
- `src/connector/src/source/cdc/external/spanner.rs` - External table reader for backfill
- `src/stream/src/from_proto/stream_cdc_scan.rs` - CDC scan executor (backfill coordination)

**Test Infrastructure**:
- `e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial` - Main test file
- `e2e_test/source_inline/spanner_cdc/prepare-data.rs` - Resource setup (uses `gcloud` CLI)
- `src/risedevtool/gcloud-spanner.toml` - Emulator download/setup
- `src/risedevtool/src/task/spanner_service.rs` - Emulator service management

### Configuration

**Connector Name**: `spanner-cdc` (hyphenated, required for SQL parser)

**Required Properties**:
- `spanner.project` - GCP project ID
- `spanner.instance` - Spanner instance ID
- `database.name` - Spanner database ID
- `spanner.stream_name` - Change stream name

**Optional Properties**:
- `spanner.parallelism` - Number of parallel readers
- `spanner.heartbeat_interval` - Heartbeat interval (e.g., `5s`, `10s`, default: `3s`)
- `spanner.table_name` - Filter by upstream table name (used in `CREATE TABLE ... FROM SOURCE TABLE 'table_name'`)
- `spanner.endpoint` - Custom endpoint (for emulator: `https://127.0.0.1:9010`)
- `spanner.enable_databoost` - Enable databoost (partitioned reads) for faster snapshot backfill (default: `false`)
- `spanner.credentials` - GCP service account credentials as JSON string
- `spanner.credentials_path` - Path to GCP service account credentials file
- **Authentication**: If neither `credentials` nor `credentials_path` is specified, uses Application Default Credentials (ADC)

**Retry Configuration**:
- `spanner.retry_attempts` - Number of retry attempts (default: 3)
- `spanner.retry_backoff_ms` - Base backoff interval in milliseconds (default: 1000)
- `spanner.retry_backoff_max_delay_ms` - Maximum backoff delay in milliseconds (default: 10000)
- `spanner.retry_backoff_factor` - Multiplier for each retry (default: 2)

### Example Configuration

```sql
CREATE SOURCE spanner_source WITH (
  connector = 'spanner-cdc',
  spanner.project = 'test-project',
  spanner.instance = 'test-instance',
  database.name = 'test-database',
  spanner.stream_name = 'test_stream',
  spanner.endpoint = 'https://127.0.0.1:9010',
  spanner.enable_databoost = 'true'  -- Enable faster backfill with partitioned reads
);

CREATE TABLE spanner_cdc_test () FROM spanner_source TABLE 'users';
```

## Testing

### ⚠️ IMPORTANT: Remote-Only Testing

**NEVER test locally** - Always use remote builder for testing.

**Reasons:**
- Spanner emulator requires Docker (not available in local dev environment)
- Remote builder has proper Docker daemon and network setup
- Consistent test environment across all developers

### Run Tests (Remote Only)

**Full Workflow (recommended):**
```bash
make -C scripts/remote run
```
This runs: sync → clean → deploy → test

**Individual Steps:**
```bash
# Sync code to remote builder
make -C scripts/remote sync

# Deploy RisingWave cluster on remote
make -C scripts/remote deploy

# Run tests
make -C scripts/remote test
```

**See `scripts/remote/README.md` for details.**

### Test Flow
1. `prepare-data.rs` configures `gcloud` CLI and creates:
   - Instance: `test-instance`
   - Database: `test-database`
   - Table: `users` (id, name, email, age)
   - Change stream: `test_stream` (watches `users` table)
   - Test data: 2 users (Alice, Bob)

2. Test creates shared source `spanner_source`
3. Test creates table `spanner_cdc_test` from source with `TABLE 'users'` filter
4. Test verifies:
   - Backfill captures initial rows (id 1, 2) ✅
   - CDC INSERT captures new rows (id 3, 4) ✅
   - CDC UPDATE captures updates (id 1 age 30→31) ✅
   - CDC DELETE removes rows (id 2 Bob) ✅
   - Schema evolution: DDL ADD COLUMN `city` + INSERT (id 5, 6) ✅
   - Schema evolution: DDL ADD COLUMN `zipcode`, `state` + INSERT (id 7) ✅
   - Databoost: Backfill uses partitioned reads for performance ✅

## Work In Progress / Future Tasks

### 🔴 P1: High Priority - Stability & Data Quality

#### 5. Schema Evolution Edge Cases (🔴 HIGH)
**Risk**: Data corruption on unsupported schema changes
**Estimate**: 3-5 days

**Current**: Only handles adding columns

**Missing Edge Cases**:
1. Column type changes (e.g., INT64 → FLOAT64)
2. Column deletion
3. Table rename
4. Concurrent schema changes (race condition)
5. Schema rollback (upstream reverts DDL)
6. Primary key changes

**What's Needed**:
- Schema compatibility validation
- Graceful failure on unsupported changes with clear error
- Schema versioning with conflict detection
- Configuration for schema evolution mode (strict vs lenient)

---

### 🟡 P2: Medium Priority - Operability & Performance

#### 6. Metrics and Observability (✅ COMPLETED)
**Risk**: Cannot debug production issues
**Estimate**: 2-3 days → **COMPLETED**

**Status**: ✅ **COMPLETED** - Follows RisingWave's standard metrics pattern

**Implemented Metrics** (all auto-tracked via RisingWave's infrastructure):
- `partition_input_count` - Number of rows processed (auto-tracked by parser in `common.rs`)
- `partition_input_bytes` - Number of bytes processed (auto-tracked by parser in `common.rs`)
- `direct_cdc_event_lag_latency` - Event lag latency in milliseconds (tracked in `parser/mod.rs`)
- `user_source_error` - Parse errors (tracked in `parser/mod.rs` via `GLOBAL_ERROR_METRICS`)

**Design Decision**:
- Spanner CDC follows the exact same pattern as other CDC sources (PostgreSQL, MySQL)
- No custom Spanner-specific metrics module - uses global RisingWave metrics infrastructure
- Schema evolution is NOT tracked via metrics - uses control-plane `schema_change_tx` channel instead
- All metrics are automatically exposed via Prometheus without any Spanner-specific code

---

#### 7. Rate Limiting/Backpressure (✅ ALREADY IMPLEMENTED)
**Risk**: Overwhelms downstream or Spanner
**Estimate**: N/A - **Already available via RisingWave infrastructure**

**Status**: ✅ **ALREADY IMPLEMENTED** - Inherited from RisingWave's CDC backfill executor

**Configuration Options**:

```sql
-- Method 1: Session configuration (affects all tables in session)
SET backfill_rate_limit = 1000;  -- 1000 rows/second

-- Method 2: WITH option (when creating table)
CREATE TABLE my_table FROM spanner_source TABLE 'users'
WITH (backfill_rate_limit = '1000');

-- Method 3: ALTER statement (dynamic adjustment)
ALTER TABLE my_table SET BACKFILL RATE LIMIT 1000;

-- Method 4: Set to 0 to pause backfill
SET backfill_rate_limit = 0;
```

**Implementation Details**:
- ✅ Backfill rate limiting handled by `CdcBackfillExecutor` (shared by all CDC sources)
- ✅ Uses `risingwave_common_rate_limit::RateLimiter` for throttling
- ✅ Dynamically adjustable via ALTER statement (no restart required)
- ✅ Per-table rate limits supported

**Scope**:
- ✅ **Applies to snapshot backfill** (reading initial data from Spanner)
- ❌ **Does NOT apply to CDC streaming** (real-time change events - these flow at natural change rate)

**Streaming Flow Control**:
- Streaming phase is naturally rate-limited by Spanner's change stream emission rate
- Chunk size controlled by `source_ctrl_opts.chunk_size` configuration

---

#### 8. Partition Split Handling (🟡 MEDIUM)
**Risk**: Memory explosion at scale
**Estimate**: 2-3 days

**Missing**:
- Test with actual partition splits (emulator may not split)
- Handling of rapid partition splits (100+ partitions)
- Partition cleanup after completion
- Max concurrent partition limits

**What's Needed**:
- Configurable max concurrent partitions
- Partition completion cleanup
- Testing with forced partition splits
- Memory usage monitoring and limits

---

### 🟢 P3: Low Priority - Edge Cases

#### 9. Large Value Handling (🟢 LOW)
**Estimate**: 1-2 days

**Missing**:
- Rows exceeding Spanner's size limits
- JSON columns with large objects
- ARRAY columns with many elements

---

#### 10. Timezone/Timestamp Handling (🟢 LOW)
**Estimate**: 1 day

**Potential Issues**:
- Spanner timestamps are nanoseconds since epoch
- RisingWave timestamps may differ
- Timezone handling for TIMESTAMP columns

**What's Needed**:
- Explicit timezone conversion tests
- Leap second handling
- Monotonicity guarantees

---

#### 11. Comprehensive Test Coverage (🟢 LOW)
**Estimate**: 5-7 days

**Current Tests**:
- Only emulator tests
- Only happy path
- No failure injection
- No performance tests

**Missing**:
- Chaos testing (kill RisingWave mid-stream)
- Network partition simulation
- Spanner outage simulation
- Large-scale tests (100K+ rows)
- Multi-table tests
- Stress tests (high throughput)

---

## Priority Summary

| Priority | Item | Risk | Status | Estimate |
|----------|------|------|--------|----------|
| 🔴 P0 | Checkpoint/Persistence | Data loss | ✅ COMPLETED | Done |
| 🔴 P0 | Production credentials | Can't deploy | ✅ COMPLETED | Done |
| 🔴 P0 | Graceful shutdown | Data loss | ✅ COMPLETED | Done |
| 🔴 P1 | Exponential backoff | Stability | ✅ COMPLETED | Done |
| 🔴 P1 | Schema evolution edge cases | Corruption | ❌ TODO | 3-5 days |
| 🟡 P2 | Metrics/observability | Operability | ✅ COMPLETED | Done |
| 🟡 P2 | Rate limiting/backpressure | Stability | ✅ ALREADY IMPLEMENTED | N/A |
| 🟡 P2 | Partition split handling | Performance | ❌ TODO | 2-3 days |
| 🟢 P3 | Large values | Edge case | ❌ TODO | 1-2 days |
| 🟢 P3 | Timezone handling | Edge case | ❌ TODO | 1 day |
| 🟢 P3 | Test coverage | Quality | ❌ TODO | 5-7 days |

**Note**: Dead Letter Queue is NOT implemented. Spanner CDC follows RisingWave's "fail fast" design - when an error occurs, the entire stream stops and retries from the last checkpoint. This ensures no silent data loss.

**🎉 ALL P0 CRITICAL ITEMS COMPLETED!**
**✅ P2 Metrics and Observability COMPLETED!**
**✅ P2 Rate Limiting/Backpressure ALREADY IMPLEMENTED (inherited from CDC infrastructure)!**

**Total Estimate**: ~15-19 development days for full production readiness (all P0 + P2 items completed or already implemented)

---

## References

**Spanner CDC Connector**:
- `src/connector/src/source/spanner_cdc/` - Main connector implementation
- `src/connector/src/source/spanner_cdc/mod.rs` - Properties and connector constants
- `src/connector/src/source/spanner_cdc/source/reader.rs` - Data reading and partition coordination
- `src/connector/src/source/spanner_cdc/split.rs` - Split definition with state, watermark, parent tracking
- `src/connector/src/source/spanner_cdc/schema_track.rs` - Schema tracking for auto evolution

**Parser and Metrics**:
- `src/connector/src/parser/mod.rs` - Central parser with lag tracking for CDC sources (lines 299-312 for Spanner CDC)
- `src/connector/src/source/common.rs` - Auto-tracks `partition_input_count` and `partition_input_bytes` (lines 38-90)
- `src/connector/src/source/monitor/metrics.rs` - Global metrics infrastructure (`SourceMetrics`, `EnumeratorMetrics`)
- `src/common/metrics/src/error_metrics.rs` - Error tracking via `GLOBAL_ERROR_METRICS`

**CDC Integration and Rate Limiting**:
- `src/stream/src/executor/backfill/cdc/cdc_backfill.rs` - CDC backfill executor with rate limit support
- `src/stream/src/executor/backfill/cdc/upstream_table/snapshot.rs` - Snapshot read with `RateLimiter` (lines 198-257)
- `src/common/rate_limit/src/lib.rs` - Rate limiter implementation
- `src/frontend/src/utils/overwrite_options.rs` - Extracts `backfill_rate_limit` from WITH options
- `src/stream/src/from_proto/stream_cdc_scan.rs` - Passes `node.rate_limit` to CDC backfill executor
- Debezium parser: `src/connector/src/parser/debezium/`
- CDC parser (MessageMeta): `src/connector/src/parser/mod.rs` - handles `_rw_table_name` extraction
- CDC external classification: `src/connector/src/source/cdc/external/mod.rs`
- CdcFilter executor: `src/stream/src/from_proto/cdc_filter.rs` - filters rows by `_rw_table_name`
