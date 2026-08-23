# Issue 1 — Coordinated sink fails when its upstream is single-distributed (singleton input)

> Draft for risingwavelabs/risingwave bug report, following `bug_report.yml`. To file, paste the body
> below into a new issue with title + labels `["type/bug"]`.

**Title:** Coordinated sinks (Iceberg/Doris/Snowflake/Deltalake/Remote/Redshift) fail at actor start when the upstream is single-distributed (e.g. a global aggregate)

**Labels:** `type/bug`

**Body:**

### Describe the bug

Any coordinated sink whose upstream is **single-distributed** — e.g. `CREATE SINK ... FROM (SELECT count(*), sum(x) FROM t)`, i.e. a global aggregate, or any query the planner renders as a single fragment — fails at actor start with:

```
sink needs coordination and should not have singleton input
```

The sink executor retries this forever, so no data is ever written. It affects every sink that uses `CoordinatedLogSinker`: Iceberg, Doris, Snowflake, Delta Lake, Remote, and Redshift. There is no user-facing workaround except rewriting the query so the planner distributes it.

### Error message/log

```
ERROR: failed to build streaming job: sink needs coordination and should not have singleton input
```

(the exact wording is the `anyhow!` at `src/connector/src/sink/coordinate.rs`, surfaced through the sink executor's retry loop)

### To Reproduce

First create a table and insert data with

```sql
CREATE TABLE t (id int, v int);
INSERT INTO t VALUES (1, 10), (2, 20);
FLUSH;
```

Then the bug is triggered by creating a sink over a global aggregate (any `CoordinatedLogSinker`-backed connector works; Iceberg is shown here):

```sql
CREATE SINK s FROM (SELECT count(*) AS cnt, sum(v) AS total FROM t)
WITH (
    connector = 'iceberg',
    type = 'append-only',
    catalog.type = 'storage',
    database.name = 'demo',
    table.name = 'demo_sink',
    warehouse.path = 's3a://bucket/warehouse'
);
```

Instead of starting and streaming rows, the sink fails immediately and retries forever. The same failure occurs with `connector = 'doris'`, `'snowflake'`, `'deltalake'`, `'redshift'`, and the remote sink, as well as with any upstream the planner collapses to a single fragment (e.g. a join that reduces to one parallel unit).

### Expected behavior

I expected a single-distributed sink to start normally and stream data. A single-distributed upstream has exactly one actor and needs no coordinator sharding, so coordination should either be skipped entirely for a singleton input or initialized with the default/full vnode bitmap.

Instead, this happened: actor start hard-fails with `sink needs coordination and should not have singleton input` and retries forever.

### How did you deploy RisingWave?

via RiseDev (or any local deployment). This is reproducible with `./risedev d` and a default profile.

### The version of RisingWave

Reproduced on current `main` (verified against `src/connector/src/sink/coordinate.rs`), latest release `v3.0.3`.

### Additional context

Root cause: `CoordinatedLogSinker::new` requires `SinkWriterParam.vnode_bitmap` to be `Some`. Meta renders a single-distribution fragment (`DistributionType::Single`) with `vnode_bitmap = None` (see `src/meta/src/controller/scale.rs`, `DistributionType::Single => None`), so a singleton-distributed upstream of any coordinated sink hits this.

Supporting references (all on `main`):

- `src/connector/src/sink/coordinate.rs` — the hard error and the `vnode_bitmap` usage (`new_stream_handle(&self.param, self.vnode_bitmap)`)
- `src/connector/src/sink/redshift.rs`, `snowflake.rs`, `deltalake.rs`, `remote.rs`, `doris.rs` — all call `CoordinatedLogSinker::new`
- `src/meta/src/controller/scale.rs` — `DistributionType::Single => None`
- `src/frontend/src/optimizer/plan_node/stream_sink.rs` — `Distribution::Single => RequiredDist::single()` keeps the input single (no repartition)

Note that the coordinated sink path also assumes the vnode bitmap in the coordinator (`src/meta/src/manager/sink_coordination/coordinator_worker.rs` tracks per-handle committed vnodes via the bitmap), so a fix must supply a valid bitmap for the single actor rather than simply dropping it.
