# Table-routed dispatch for the shared CDC source fanout

**Status:** feasibility research complete — **recommendation: build it.**
**Date:** 2026-09-04 (revised with burst evidence 2026-09-04 17:00-17:30 UTC)
**Scope:** the `Source -> CdcFilter` exchange of a shared CDC source, one edge per dependent CDC table.

## Summary

A shared CDC source sends **every row to every dependent CDC table job**, each of which discards all
rows that are not its own table. Measured in `platform-non-prod`: source `src_ice` emits ~1,580
rows/s into **25 downstream fragments, each receiving the full 1,580 rows/s** — 39,500 deliveries/s
to yield 1,580 useful ones.

Routing each row to the single table it belongs to would cut that ~25x. Unlike the range dispatcher
rejected in [cdc-range-dispatcher.md](./cdc-range-dispatcher.md), every precondition here holds:
the routing key is a **plain varchar column at a fixed index**, the predicate is a **compile-time
constant**, and the payload never has to be parsed.

**Feasibility: high.** The main risks are a legacy SQL Server compatibility rewrite and the general
hazard of under-delivery, both addressed below.

## Why this hop is tractable where the scan hop was not

| | Scan hop (rejected) | Source hop (this doc) |
| --- | --- | --- |
| Routing key | PK range, `OwnedRow` bounds | `_rw_table_name`, a varchar |
| Key location | needs decode with PK data types | fixed column index 2, no decode |
| Predicate | grows as splits *finish* (progress-dependent) | constant string, fixed at plan time |
| Steady state | collapses to 1 split on 1 actor, nothing to partition | 25 distinct tables, always |
| Payload parse | n/a | not required, payload stays opaque |

## Established facts

**The edge is `NoShuffle`, one per CDC table job.** `meta/src/stream/stream_graph/fragment.rs:1780`:

```rust
dispatch_strategy: DispatchStrategy {
    r#type: DispatcherType::NoShuffle as _,
    dist_key_indices: vec![],
    output_mapping: DispatchOutputMapping::identical(CDC_SOURCE_COLUMN_NUM as _),
}
```

`NoShuffle` maps to `SimpleDispatcher` with exactly one output (`dispatch.rs:720`). So the source
actor holds **N independent single-output dispatchers**, one per dependent table — the cleanest
possible place to attach a per-edge predicate.

**The routing key is a fixed column.** `CDC_SOURCE_COLUMN_NUM = 3`
(`common/src/catalog/mod.rs:141`); the CDC source chunk is `(payload, _rw_offset, _rw_table_name)`,
and `stream/src/from_proto/cdc_filter.rs` pins `RW_TABLE_NAME_COLUMN_IDX = 2`.

**The predicate is a constant equality.** From the same file: *"`CdcFilter` search condition is
expected to be a single equality on `_rw_table_name`."* `extract_cdc_filter_eq_condition` already
parses exactly `InputRef(2) = <varchar literal>`, and that helper can be reused.

**No update-pair hazard.** Each CDC message becomes one row. Transaction-control and schema-change
messages are separate `ParseResult` variants (`parser/mod.rs:155-163`) and never become chunk rows,
so there is no `U-`/`U+` pair that table routing could split — the failure mode that complicates
`HashDataDispatcher`.

**Heartbeats are already dropped here.** `chunk_builder.heartbeat()`
(`parser/chunk_builder.rs:146`) writes a single **invisible** row and forces it into its **own
chunk**. `FilterExecutor` calls `chunk.compact_vis()` before evaluating
(`stream/src/executor/filter.rs:182`), so an all-invisible chunk compacts to empty and is dropped
(`None => continue`). Dispatcher-level filtering therefore preserves today's behaviour exactly.

## Design

1. **Carry the predicate on the edge.** Add an optional table-name field to `DispatchStrategy` /
   `PbDispatcher` (the split-key column index is a constant, so it need not be plumbed).
2. **Populate it in meta**, where the edge is already built (`fragment.rs:1780`). Meta holds the
   downstream fragment's node tree and can locate the `CdcFilter` node and extract the literal with
   the existing `extract_cdc_filter_eq_condition` logic, which would move to a shared crate.
3. **Apply it in `SimpleDispatcher::dispatch_data`**: compare column 2 against the constant, build a
   visibility bitmap, and skip the send entirely when nothing matches. Columns are shared via
   `StreamChunk::with_visibility`, so the cost is a bitmap, not a copy.
4. **Keep the `CdcFilter` executor in place.** This is what makes the change safe — see below.

### Rules that must hold

- **Forward zero-cardinality chunks unchanged to every output.** Heartbeats are already dropped
  downstream, so this changes nothing today, but it keeps the optimization decoupled from the known
  heartbeat/idle-table bug. If that bug is ever fixed by letting heartbeats through `CdcFilter`,
  this rule means the dispatcher does not silently re-break it.
- **Fail open.** Absent, unparseable, or unexpected predicate shape - dispatch everything, exactly
  as today.
- **Never enable on an edge whose predicate is not a bare equality.**

### Why keeping `CdcFilter` makes this safe

The two failure directions are not symmetric:

- **Over-delivery** (dispatcher sends a row the table does not want) is harmless: `CdcFilter`
  rejects it, exactly as it does for all 24 other tables' rows today.
- **Under-delivery** (dispatcher withholds a row the table needs) is silent data loss.

Because `CdcFilter` remains in the plan enforcing correctness, the dispatcher predicate is a pure
optimization and any conservative over-approximation is safe. All mitigations should therefore be
biased toward sending more.

## Complications

**1. Legacy SQL Server compat rewrite.** `with_legacy_sqlserver_table_name_compat`
(`from_proto/cdc_filter.rs`) rewrites the predicate at *executor build time* into
`OR(original, normalized)` to bridge `db.schema.table` against a runtime `_rw_table_name` of
`schema.table`. A dispatcher predicate built from the raw literal would **under-deliver** for those
tables — the dangerous direction. Two options:

- apply the same normalization and match either form (over-approximate, safe); or
- skip the optimization entirely when `normalize_legacy_sqlserver_table_name` returns `Some`.

The second is simpler and costs nothing for Spanner/Postgres/MySQL sources.

**2. This does not raise parallelism.** `NoShuffle` requires downstream parallelism to equal
upstream, and the Spanner source is architecturally single-split (`list_splits` returns one
`SpannerCdcSplit::new_root`, `spanner_cdc/enumerator/mod.rs:84`). So every `CdcFilter` is pinned to
one actor. This change cuts **bytes and sends**, not actor count. Any plan to parallelize CDC
ingestion is a separate, larger piece of work.

**3. Versioning.** Existing jobs have no predicate on their edges; the field must default to "no
filtering" so a rolling upgrade is a no-op until jobs are recreated or the field is backfilled.

## Expected gain

### The controlled measurement (2026-09-04 burst)

Earlier drafts compared `src_ice` and `src_ic_gl` at the same *moment*, which confounds fanout with
volume. During the 2026-09-04 burst both sources ramped through the same throughput range, which
allows comparing them at the same *rate*:

| | throughput | fanout | blocking sum | **per channel** |
| --- | --- | --- | --- | --- |
| `src_ice` @ 17:02 | 1,056 rows/s | 25 | 1.777 s/s | **0.071** |
| `src_ic_gl` @ 17:22 | 942 rows/s | 4 | 0.310 s/s | **0.078** |

**Per-channel cost is the same within noise.** Total cost differs by 5.7x against a fanout ratio of
6.25x. Dispatch cost is therefore *linear in fanout with per-row cost held constant* — the fanout is
not merely correlated with the problem, it is the multiplier.

### `src_ice` is pinned against a ceiling

Total blocking on the source fragment across the burst:

| throughput (rows/s) | blocking sum (s/s) |
| --- | --- |
| 1,056 | 1.78 |
| 1,983 | 4.16 |
| 2,739 | 8.88 |
| 2,019 | 8.96 |
| 1,764 | 8.91 |
| 1,573 | 9.13 |
| 1,316 | 9.20 |
| 1,821 | 9.05 |

It rises with load, then **pins at ~9.05 s/s and stays flat while throughput swings over a 2x range**
(1,316 -> 2,739). Flat output against varying input is a saturation signature. `src_ic_gl` sits at
0.25-0.31 s/s against an equivalent ceiling near 4 x 0.36 = 1.44 s/s, i.e. ~20% of the wall
`src_ice` is already against; it would need ~4x more load to feel the same effect.

Await-tree sampling of the source actor during the burst (120 samples) puts it in `dispatch_chunk`
**80% of the time** and in the Spanner read path 19%. The source is not read-bound.

### Why this matters more now: the CPU limit cannot be raised

Compute pods run against a **3-core cgroup limit** and hit 3.6 / 3.1 / 2.8 cores during the burst.
Raising the limit is not available, which removes the cheapest alternative fix and makes CPU a hard
budget.

CPU profile of two pods during the burst (45s, `risectl profile cpu`):

| thread | streaming-1 | streaming-0 |
| --- | --- | --- |
| `rw-streaming` | 39.9% | 38.1% |
| `foyer.meta-unif` (SSTable meta decode) | 39.1% | 38.5% |
| `rw-main` | 19.7% | 22.4% |
| `foyer.data-unif` | 0.3% | 0.4% |

A **separate and larger** problem exists: 39% of CPU is `Box<Sstable> as Code>::decode` via bincode,
of which 26.7% of *all* CPU is a single `VecVisitor<u8>::visit_seq` — byte-at-a-time `Vec<u8>`
deserialization that should be using `serde_bytes`. That is not caused by the fanout and is not
fixed by this design; it is tracked separately. Its relevance here is that it leaves streaming with
only ~40% of an already-capped budget, so the redundant fanout work competes for a much scarcer
resource than the earlier profile suggested.

Within that squeezed budget, the fanout costs:

| path | cumulative CPU (burst) |
| --- | --- |
| `FilterExecutor` (the 25 CdcFilters) | 7.13% |
| dispatch (`await_with_metrics`) | 2.66% |

Removing 24/25 of both frees roughly **9% of total node CPU (~0.28 of 3 cores)** — about a **24%
relative increase in the streaming thread pool's budget**.

### Deliveries removed

| | Today | With table routing |
| --- | --- | --- |
| Source output | ~1,580 rows/s | ~1,580 rows/s |
| Downstream fragments | 25 | 25 |
| Deliveries | **39,500 rows/s** | **~1,580 rows/s** |

### The mechanism that should lift the ceiling

`DispatchExecutor::dispatch` runs `for_each_concurrent(limit, ...).await` over all 25 dispatchers and
**does not return until every one has accepted the chunk** (`dispatch.rs:237-245`). The 25 futures
are concurrent but not parallel — one actor task polls them in turn — so the source's wall-clock cost
per chunk is the serial poll of all 25 plus the wait on the slowest.

With table routing, 24 of the 25 skip the send entirely: no permit acquisition, no `output_mapping`,
no mpsc push. The `for_each_concurrent` collapses to one meaningful future per chunk.

**Estimated effect: the source's 80% wall-clock share in `dispatch_chunk` should fall
substantially.** This is a reasoned estimate from the measured mechanism, **not a measurement**.
Anyone relying on it should treat the delivery reduction and the ~9% CPU saving as the defensible
claims, and the ceiling lift as the hypothesis being tested.

### What was ruled out

- **Exchange permits are not the constraint.** `exchange_initial_permits` was raised 2048 -> 12288 on
  2026-09-04 (restoring 8 chunks of slack against `chunk_size = 1536`). Peak throughput improved
  2,047 -> 2,739 rows/s (**1.34x**), then per-channel blocking returned to **0.365 s/s** — the same
  value measured on the old setting. Credits were a secondary limit; the dispatch path saturates at
  the same point regardless of pool size.
- **Spanner is not the constraint.** Spanner client CPU is 5.85%; the source actor spends 19% of its
  time in the read path.
- **This hop carries no network traffic.** `NoShuffle` forces 1:1 colocation; all 25 filter fragments
  run on the source's node and `stream_exchange_frag_send_size{up_fragment_id=<source>}` has no
  series. The saving is CPU and allocator pressure on one node, not bytes on the wire.
- **An earlier claim that the fanout cost "~70% of demonstrated ingest capacity" was wrong** and has
  been withdrawn. The 14,617 rows/s 30-day peak is not established as a like-for-like baseline; if
  dependent tables were added over time, that peak was measured at a lower fanout.

## Validation

- Unit: dispatcher routes matching rows, withholds non-matching, forwards zero-cardinality chunks to
  all outputs, and falls open with no predicate.
- SLT: a shared CDC source with two tables; assert both receive exactly their own rows.
- Metric: `stream_actor_in_record_cnt` summed over the dependent fragments should fall from
  ~25x the source's `stream_actor_out_record_cnt` to ~1x, and the source's
  `stream_actor_output_buffer_blocking_duration_ns` should drop sharply from 7.9 s/s.

## Implementation plan

Verified against the tree on 2026-09-04; every anchor below still exists.

1. **Proto** — add one optional field to `DispatchStrategy` (`proto/stream_plan.proto:1275`) and the
   matching `Dispatcher` message. Absent means "no filtering", so a rolling upgrade is a no-op.
2. **Meta** — populate it at `stream_graph/fragment.rs:1780`, the single site that builds this edge.
   The downstream `CdcFilter` fragment is in scope there (`fragment.inner.get_node()` is already read
   for the adjacent `tracing::debug!`), so the node tree can be walked for the `search_condition`.
   Move `extract_cdc_filter_eq_condition` (`from_proto/cdc_filter.rs:124`) into a shared crate and
   reuse it verbatim; do not re-implement the parse.
3. **Dispatcher** — `SimpleDispatcher::dispatch_data` is four lines
   (`dispatch.rs`, `impl Dispatcher for SimpleDispatcher`). Compare column
   `RW_TABLE_NAME_COLUMN_IDX = 2` against the constant, build a `BitmapBuilder`, attach via
   `StreamChunk::with_visibility` (shares columns, no copy), and return early without sending when
   nothing matches.
4. **Skip the SQL Server case.** `normalize_legacy_sqlserver_table_name`
   (`from_proto/cdc_filter.rs:162`) rewrites the predicate at executor-build time into
   `OR(original, normalized)`. A dispatcher predicate built from the raw literal would
   **under-deliver** — the unsafe direction. When that function returns `Some`, leave the edge
   unfiltered. Costs nothing for Spanner/Postgres/MySQL.

Scope: one proto field, one meta call site, one dispatcher branch. `CdcFilter` stays in the plan, so
the predicate remains a pure optimization and every failure mode is over-delivery, which is already
the normal case for 24 of 25 tables.

## Recommendation

**Build it.**

The case is stronger than in the first draft and rests on different evidence. The matched-throughput
comparison shows dispatch cost is linear in fanout with per-row cost constant; `src_ice` is pinned
against a ~9.05 s/s ceiling that `src_ic_gl` will not reach until ~4x more load; the CPU limit cannot
be raised, so redundant work can no longer be absorbed; and the permit change has already
demonstrated that credits were not the binding constraint.

Sequence the work behind the `foyer.meta-unif` decode fix, which is ~4x larger (39% vs ~9% of CPU)
and is a config change plus an upstream bug report rather than a code change. But that fix helps
every fragment equally — it does **not** address why `src_ice` specifically lags roughly 2x more than
`src_ic_gl`. Only this design does.

**Measure before and after:** `stream_actor_in_record_cnt` summed over the 25 dependent fragments
should fall from ~25x the source's `stream_actor_out_record_cnt` to ~1x, and the source's
`stream_actor_output_buffer_blocking_duration_ns` should fall from its ~9.05 s/s pin. If it does not
move off that pin, the ceiling is CPU-bound rather than structural and the remaining lever is the
meta-decode fix.
