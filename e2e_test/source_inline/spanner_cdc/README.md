# Spanner CDC E2E Tests

End-to-end tests for the Spanner CDC source connector, run against the Spanner emulator.

## Prerequisites

- The `gcloud` CLI: `prepare-data.rs` uses it to create the instance and databases.
- The Spanner emulator, either:
  - the gcloud emulator component (`gcloud components install cloud-spanner-emulator`, Linux
    only), started by the `spanner-emulator` profile, or
  - the emulator Docker image, started by you, for the `spanner-emulator-docker` profile (macOS).
- A nightly Rust toolchain: `prepare-data.rs` is a `cargo -Zscript` script.

## Running

On Linux, with the gcloud emulator component:

```bash
./risedev d spanner-emulator
./risedev slt 'e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial'
./risedev k
```

On macOS, or anywhere without the component, run the emulator in Docker:

```bash
docker run -d --name spanner-emulator -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator
./risedev d spanner-emulator-docker
./risedev slt 'e2e_test/source_inline/spanner_cdc/spanner_cdc.slt.serial'
./risedev k
docker rm -f spanner-emulator
```

Both profiles set:
- `SPANNER_EMULATOR_HOST`: the emulator's gRPC address
- `SPANNER_PROJECT`, `SPANNER_INSTANCE`, `SPANNER_DATABASE`: the test resources
- `RISEDEV_SPANNER_WITH_OPTIONS_COMMON`: the connection options for `CREATE SOURCE`

This test is not run in CI. Run it locally before changing the Spanner CDC connector.

## Files

- `spanner_cdc.slt.serial`: the test. One shared source reads a `FOR ALL` change stream,
  and each part checks one behaviour:
  1. Spanner fixtures
  2. `CREATE SOURCE` / `CREATE TABLE` validation (options, value capture type, dialect, watched columns)
  3. Backfill with default, even and uneven splits, an empty table, and backfill completion
  4. Insert, update and delete through the change stream, including successive updates to one row
  5. A table created after the source
  6. Column types (scalars read by snapshot vs stream, arrays) and a named schema
  7. Parallel backfill with changes issued while it runs
  8. Schema evolution
  9. `ALTER SOURCE`
  10. Changes committed during recovery
  11. Cleanup
- `prepare-data.rs`: emulator fixtures, used as `system` commands by the test:
  - `setup`: create the instance if needed and recreate the test database empty, so every
    run starts clean
  - `create-postgresql-database`: recreate a PostgreSQL-dialect database
  - `ddl <SQL>` / `dml <SQL>`: run one statement

## Troubleshooting

**Emulator not running, or a clean start:**
```bash
./risedev k
./risedev clean-data
./risedev d spanner-emulator
```

**Test hangs:** each `prepare-data.rs` call compiles and runs a cargo script, and the checks
retry for up to two minutes while data arrives. If it hangs longer, check the RisingWave logs
with `./risedev l`.
