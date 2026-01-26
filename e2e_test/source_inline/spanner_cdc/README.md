# Spanner CDC E2E Tests

End-to-end tests for the Spanner CDC source connector.

## Running Tests

### In CI (Automatic)
Tests run automatically with all inline source tests:
```bash
risedev slt './e2e_test/source_inline/**/*.slt.serial'
```

### Locally - Spanner Only (Recommended)
```bash
# Start RisingWave with Spanner emulator only
./risedev d spanner-only

# Run Spanner CDC test
./risedev slt 'e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial'

# Stop when done
./risedev k
```

### Locally - All Inline Sources
```bash
# Start RisingWave with all inline source emulators
./risedev d ci-inline-source-test

# Run all inline source tests
./risedev slt './e2e_test/source_inline/**/*.slt.serial'

# Stop when done
./risedev k
```

## Test Structure

### Files
- `spanner_cdc.slt.serial` - Test file
- `prepare-data.rs` - Resource setup (called from SLT)
- `setup-emulator.sh` - Alternative manual setup

### How It Works
The SLT test automatically:
1. Configures gcloud for emulator
2. Creates Spanner resources (instance, database, table, change stream)
3. Tests error handling and data flow
4. Cleans up

### Resource Creation
`prepare-data.rs` uses `gcloud --configuration=emulator` to redirect gcloud commands to the emulator:

```rust
// Configure gcloud for emulator
Command::new("gcloud")
    .args(&["config", "set", "api_endpoint_overrides/spanner", 
            "http://localhost:9020/", "--configuration=emulator"])
    .output()?;

// Create resources
Command::new("gcloud")
    .args(&["--configuration=emulator"])
    .arg("spanner").arg("instances").arg("create")
    .arg("test-instance")
    .output()?;
```

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

### Emulator not running
```bash
./risedev k
./risedev d spanner-only
```

### Clean start
```bash
./risedev k
./risedev clean-data
./risedev d spanner-only
```

### Test hangs
The test may take 10-20 seconds to create Spanner resources. If it hangs longer, check:
```bash
# Check if emulator is running
docker ps | grep spanner

# Check RisingWave logs
./risedev l
```
