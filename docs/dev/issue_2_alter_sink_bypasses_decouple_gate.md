# Issue 2 — `ALTER SINK ... CONNECTOR WITH` bypasses the sink-decouple gate

> Draft for risingwavelabs/risingwave bug report, following `bug_report.yml`. To file, paste the body
> below into a new issue with title + labels `["type/bug"]`.

**Title:** `ALTER SINK ... CONNECTOR WITH (commit_checkpoint_interval = 'N>1')` bypasses the sink-decouple gate on a non-decoupled sink

**Labels:** `type/bug`

**Body:**

### Describe the bug

A user can `ALTER SINK s CONNECTOR WITH (commit_checkpoint_interval = '2')` on a sink that was created with `sink_decouple = false` (in-memory log store) — a state the frontend explicitly forbids at CREATE time (it rejects `commit_checkpoint_interval > 1` when `sink_decouple` is disabled). The ALTER is accepted, leaving the sink running a commit protocol its in-memory log store cannot satisfy: the in-memory store is not durable and cannot rewind, and the ALTER's apply path races the recovery barrier.

This affects every sink that supports `commit_checkpoint_interval`: **Iceberg, Doris, StarRocks, ClickHouse, Delta Lake, Snowflake**. The CREATE-time gate is shared, and the ALTER-time validation is shared (`validate_alter_config`), so all are equally exposed.

### Error message/log

No error is raised — the ALTER silently succeeds. The failure manifests later: the sink's log reader cannot rewind, so the ALTER triggers a recovery path, and/or the sink stalls. There is no validation error at ALTER time.

### To Reproduce

First set the session and create a sink with a decoupled log store disabled:

```sql
SET sink_decouple = false;

CREATE TABLE t (v1 int primary key, v2 varchar);
CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;

CREATE SINK s
FROM mv
WITH (
    connector = 'iceberg',
    type = 'append-only',
    catalog.type = 'storage',
    database.name = 'demo',
    table.name = 'demo_sink',
    warehouse.path = 's3a://bucket/warehouse',
    commit_checkpoint_interval = '1'
);
```

Note the CREATE succeeds only because `commit_checkpoint_interval = '1'` (the gate at CREATE time rejects values > 1 when `sink_decouple` is disabled). Then the bug is triggered by:

```sql
ALTER SINK s CONNECTOR WITH (
    commit_checkpoint_interval = '2'
);
```

This is **accepted**, although the same `commit_checkpoint_interval = '2'` would have been **rejected at CREATE time** with `sink_decouple` disabled.

### Expected behavior

I expected the ALTER to be **rejected** with a clear error, matching the CREATE-time rule: `commit_checkpoint_interval` larger than 1 requires sink decoupling to be enabled.

Instead, this happened: the ALTER is silently applied, putting a non-decoupled (in-memory, non-rewindable) sink into a commit protocol it cannot satisfy.

### How did you deploy RisingWave?

via RiseDev (or any local deployment). This is reproducible with `./risedev d` and a default profile.

### The version of RisingWave

Reproduced on current `main` (verified against `src/meta/src/controller/streaming_job.rs`), latest release `v3.0.3`.

### Additional context

Root cause: the decouple gate lives **only at CREATE time**. The frontend's `set_default_commit_checkpoint_interval` (`src/connector/src/sink/mod.rs`) rejects `commit_checkpoint_interval > 1` when `sink_decouple == Disable`, but `ALTER SINK ... CONNECTOR WITH` re-parses the sink config in the meta ALTER path (`src/meta/src/controller/streaming_job.rs`, `validate_alter_config`) with **no knowledge of the decouple state**. There is no persisted decouple state on the sink catalog: `rw_catalog.rw_sinks` derives `is_decouple` by inspecting internal tables (`rw_catalog.rw_sink_decouple`), not from stored config — so the meta ALTER path has nothing to check against.

Supporting references (all on `main`):

- `src/connector/src/sink/mod.rs` — `set_default_commit_checkpoint_interval` (CREATE-time gate) and `validate_alter_config` (no gate)
- `src/meta/src/controller/streaming_job.rs` — `validate_sink_props` / `update_sink_props_by_sink_id` (ALTER path, no decouple check)
- `src/frontend/src/optimizer/plan_node/stream_sink.rs` — where the CREATE-time gate is invoked
- `src/frontend/src/catalog/system_catalog/rw_catalog/rw_sinks.rs` — how `is_decouple` is derived (internal-table watermark vnodes, not persisted config)
- `commit_checkpoint_interval` is in the allow-alter-on-fly lists for Iceberg/Doris/ClickHouse/Delta Lake (`src/connector/src/allow_alter_on_fly_fields.rs`), so the ALTER is not rejected as a non-alterable field

`commit_checkpoint_interval` is also allow-alter-on-fly for the same sinks, which is why this slips through both the allowlist check and the config re-parse.
