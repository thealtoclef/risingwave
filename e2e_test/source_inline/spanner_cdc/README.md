# Spanner CDC E2E Tests

End-to-end tests for the Spanner CDC source connector.

The test can run with **either** the Spanner emulator or real Spanner.

## Running Tests

### Option 1: With Emulator (via risedev)

This is the easiest way for local development - risedev handles all setup.

```bash
# Start RisingWave with Spanner emulator
./risedev d spanner-only

# Run Spanner CDC test
./risedev slt 'e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial'

# Stop when done
./risedev k
```

When using risedev, the following environment variables are set automatically:
- `SPANNER_EMULATOR_HOST` - Points to emulator
- `SPANNER_PROJECT`, `SPANNER_INSTANCE`, `SPANNER_DATABASE` - Set to test values
- `RISEDEV_SPANNER_WITH_OPTIONS_COMMON` - Pre-configured connection options

### Option 2: With Real Spanner

Run tests against your real Spanner instance for production validation.

```bash
# 1. Set environment variables for your Spanner resources
export SPANNER_PROJECT="your-project-id"
export SPANNER_INSTANCE="your-instance-name"
export SPANNER_DATABASE="your-database-name"

# 2. Ensure gcloud is authenticated
gcloud auth login

# 3. Start RisingWave (default profile, no emulator)
./risedev d

# 4. In another terminal, run the test
./risedev slt 'e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial'
```

**Important for real Spanner:**
- The instance must exist before running the test
- `prepare-data.rs` will verify the instance exists and create the database/table/change stream
- No `SPANNER_EMULATOR_HOST` env var should be set

### Option 3: Remote Build (K8s)

For building and testing on a remote K8s cluster:

```bash
# With emulator
make -C scripts/remote run profile=spanner-only

# With real Spanner (default, no profile)
make -C scripts/remote run

# Individual commands
make -C scripts/remote sync      # Sync code
make -C scripts/remote deploy profile=spanner-only  # Deploy with emulator
make -C scripts/remote deploy                       # Deploy with real Spanner
make -C scripts/remote test       # Run tests
```

### CI (Automatic)

Tests run automatically in CI with the emulator.

## Test Structure

### Files
- `spanner_cdc.slt.serial` - Main test file
- `prepare-data.rs` - Resource setup script (works with both emulator and real Spanner)

### How It Works

The test script (`prepare-data.rs`) automatically detects the environment:

1. **If `SPANNER_EMULATOR_HOST` is set** → Uses emulator (risedev mode)
   - Configures gcloud with no auth
   - Creates instance, database, table, change stream

2. **If `SPANNER_EMULATOR_HOST` is NOT set** → Uses real Spanner
   - Verifies instance exists (you must create it first)
   - Creates database, table, change stream
   - Uses your existing gcloud authentication

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SPANNER_EMULATOR_HOST` | No* | - | Set by risedev when using emulator |
| `SPANNER_PROJECT` | No | `test-project` | Your GCP project (for real Spanner) |
| `SPANNER_INSTANCE` | No | `test-instance` | Spanner instance name |
| `SPANNER_DATABASE` | No | `test-database` | Database name |

*Required when using emulator

## Expected Output

### Successful Test
```
e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial    .. [PASSED]
```

### Failed Test
```
e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial    .. [FAILED]
error: ...
  --> e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial:XX:X
```

## Troubleshooting

### Emulator Issues

**Emulator not running:**
```bash
./risedev k
./risedev d spanner-only
```

**Clean start:**
```bash
./risedev k
./risedev clean-data
./risedev d spanner-only
```

**Test hangs:**
The test may take 10-20 seconds to create Spanner resources. If it hangs longer:
```bash
# Check if emulator is running
docker ps | grep spanner

# Check RisingWave logs
./risedev l
```

### Real Spanner Issues

**Instance not found:**
```bash
# List your instances
gcloud spanner instances list --project=your-project

# Create if needed
gcloud spanner instances create test-instance \
  --project=your-project \
  --description="Test" \
  --nodes=1
```

**Authentication issues:**
```bash
# Re-authenticate
gcloud auth login

# Check current account
gcloud auth list
```

**Test fails with connection error:**
- Ensure `SPANNER_EMULATOR_HOST` is NOT set
- Verify environment variables are correct
- Check gcloud can access your Spanner: `gcloud spanner databases list --instance=your-instance`
