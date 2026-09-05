# Range-partitioned dispatcher for parallelized CDC backfill

**Status:** research complete — **recommendation: do not build it for the steady state.**
**Date:** 2026-09-03
**Scope:** the `CdcFilter -> Broadcast -> CDC scan actors` exchange in parallelized CDC backfill (V2).

## Summary

The question was whether the dispatcher can filter rows by PK range and send each row only to
the actor that owns it, instead of broadcasting every row to all `A` scan actors and letting each
one discard what is not its own.

Research says: **the steady state does not need it.** Once backfill finishes, meta collapses the
assignment to a *single* merged split held by *one* actor, so the optimal dispatch is "send to that
one actor" — which is exactly what suppressing the empty-split actors already achieves. A range
dispatcher would add a new dispatcher type, a plan change and a silent-data-loss failure mode to
buy nothing.

Range partitioning would only pay during the *backfill* phase, and that phase is precisely where
the required bounds are not derivable from the barrier.

## The decisive finding

`CdcTableBackfillTracker::reassign_splits` (`src/meta/src/barrier/cdc_progress.rs:161`) branches on
backfill status:

```rust
CdcBackfillStatus::Backfilling(progress) => { ...; progress.splits.as_slice() }
CdcBackfillStatus::PreCompleted | CdcBackfillStatus::Completed => {
    static SINGLE_SPLIT: LazyLock<CdcTableSnapshotSplitRaw> = LazyLock::new(single_merged_split);
    core::slice::from_ref(&*SINGLE_SPLIT)
}
```

`single_merged_split()` (`src/meta/src/stream/cdc.rs:318`) builds one split whose bounds are both
`OwnedRow::new(vec![None])`.

Those all-NULL rows are exactly the sentinels `is_leftmost_bound` / `is_rightmost_bound`
(`cdc_backill_v2.rs:773,777`, both literally `row.iter().all(|d| d.is_none())`) test for, and
`filter_stream_chunk` short-circuits on them:

```rust
if is_leftmost_bound && is_rightmost_bound { return Some(chunk); }
```

Feeding `S = 1` through `assign_cdc_table_snapshot_splits` (`src/meta/src/stream/cdc.rs:188`) with
`A = 16`:

- `splits_per_actor = 1.div_ceil(16) = 1`
- `.chunks(1)` yields exactly one non-empty chunk
- `.chain(iter::repeat(Vec::default())).take(16)` pads the remaining fifteen

**Post-backfill steady state, therefore:** one actor holds a whole-key-range split and forwards
every row unfiltered; the other fifteen hold empty vectors and drop every chunk at the
`actor_snapshot_splits.is_empty()` guard (`cdc_backill_v2.rs:641`), in the forwarding path, forever.

There is nothing to range-partition. `1` split cannot be split across `16` actors.

### What this means for the suppression change

`perf/cdc-suppress-idle-scan-actors` (commit `75e3a751cb`) suppresses exactly those fifteen actors.
In the post-backfill steady state it eliminates **15/16 = 93.75%** of this exchange's traffic, and
the residual is genuinely needed. That is the optimum for this topology — a range dispatcher cannot
beat it.

The commit message currently claims ~69%, derived from a metrics inference. **That figure is wrong
and should be corrected to 15/16**, now that the mechanism is known from code rather than inferred.

It also explains the production observation that only one compute pod emits per CDC fragment: not
five colocated actors, but *one* actor — the single holder of the merged split. The "colocation
puzzle" was an artifact of pod-granularity metrics and is now closed.

## Supporting findings

| Question | Answer | Source |
| --- | --- | --- |
| Are split bounds available to the dispatcher? | Yes — `left_bound_inclusive` / `right_bound_exclusive` ride on the same `actor_cdc_table_snapshot_splits` mutation the suppression change already reads | `proto/source.proto:31` |
| Bound encoding | Value encoding via `RowDeserializer`, **not** memcomparable — byte comparison is invalid, the PK data types are required to decode | `cdc_backill_v2.rs:178-188` |
| Split key arity | Single column (`assert_eq!(left.len(), 1, "multiple split columns is not supported yet")`) | `cdc_backill_v2.rs` `filter_stream_chunk` |
| Split key column | `pk_indices[options.backfill_split_pk_column_index]`, an index into the upstream table schema | `cdc_backill_v2.rs:124` |
| Are an actor's splits contiguous? | Yes — `assert_consecutive_splits` asserts consecutive `split_id` and strictly increasing right bounds | `cdc_backill_v2.rs:832` |
| Is an actor's live-filter bound one range? | Yes — `extends_current_actor_bound` keeps the first left bound and only advances the right | `cdc_backill_v2.rs:787` |
| Does the machinery exist? | Yes — `HashDataDispatcher` already computes a per-row target, builds one visibility bitmap per output, and shares columns via `StreamChunk::with_visibility` (no data copy) | `dispatch.rs` |
| Is the column index workable? | Yes — `output_mapping.apply()` runs *after* target computation, so `dist_key_indices` index the pre-mapping chunk, as a range key would | `dispatch.rs` |
| Where would a variant be added? | `DispatcherType` enum + `DispatcherImpl::new` match + `impl_dispatcher!` | `proto/stream_plan.proto:1229`, `dispatch.rs:690` |

Two inconsistencies noticed in passing, neither blocking:

- `assert_consecutive_splits` compares with `OrderType::ascending_nulls_last()` while
  `filter_stream_chunk` compares with `ascending_nulls_first()`.
- `is_leftmost_bound` and `is_rightmost_bound` are identical implementations, so an all-NULL row is
  simultaneously both sentinels; a genuine NULL split key would be indistinguishable from
  "unbounded". Benign today because the split column is a PK column.

## Why the backfill phase resists range dispatch

During backfill the filter bound is **progress-dependent**, not assignment-dependent.
`extends_current_actor_bound` is called again each time a split finishes
(`cdc_backill_v2.rs:340`), so an actor's live-CDC filter range grows as its backfill advances. The
barrier tells the dispatcher which splits an actor *owns*; it does not tell it which splits that
actor has *finished*, and only the latter defines the filter.

Closing that gap requires per-actor backfill progress to reach the upstream dispatcher — a new
feedback channel against the direction of dataflow, which is a substantially larger change than the
dispatcher itself.

The value is also bounded: backfill is transient, whereas the steady-state waste this would not
improve on is permanent and already solved.

## If it were built anyway — design sketch

Recorded for completeness; not recommended.

1. **New `DispatcherType::CDC_RANGE`**, carrying the split-key column index (reusing the
   `dist_key_indices` field) and the PK data types needed to decode bounds.
2. **Bounds from the barrier.** Extend the existing `cdc_scan_actor_idleness` hook to decode each
   actor's `[left, right)` via `RowDeserializer`, producing an ordered, disjoint range table.
   Because ranges are contiguous and ordered, routing is a **binary search per row** —
   `O(rows · log A)` versus today's `O(rows · A)` comparisons spread across actors.
3. **Chunk construction** exactly as `HashDataDispatcher`: one `BitmapBuilder` per output, then
   `StreamChunk::with_visibility` sharing columns.
4. **`U-` / `U+` pairs must be rewritten to `Delete` / `Insert` when the split key changes between
   them**, or an update pair splits across two actors and the downstream sees an orphan `U+`.
   `HashDataDispatcher` already does this for the distribution key and the same hazard applies.
5. **Mode switch.** Broadcast during backfill; switch to range dispatch on the barrier where the
   tracker reaches `PreCompleted`/`Completed` — except that at that point there is one split and one
   actor, so the "range dispatcher" degenerates to a simple dispatcher. This is the step at which
   the whole idea collapses.
6. **Fail open.** Any uncertainty — recovery, absent assignment, undecodable bound — must fall back
   to broadcast.

### Risk that decides it

Suppression and range routing have asymmetric failure modes. A stale suppression view can only
over-deliver to an actor that discards anyway: harmless. A stale range view **mis-routes**, and a
mis-routed row is silently dropped by every actor — data loss with no error. For a permanent 0%
additional gain, that is not a trade worth making.

## Recommendation

1. **Do not build the range dispatcher.** The steady state is already optimal after suppression.
2. **Correct the `75e3a751cb` commit message** from ~69% to 15/16 (93.75%), and cite the
   `single_merged_split` mechanism instead of the metrics inference.
3. **Revisit only if** the post-backfill merge is ever removed — i.e. if steady state keeps `S > 1`
   splits spread across actors. Then range dispatch becomes worthwhile and this sketch applies.
4. **Consider instead**, as separate work: the fifteen stranded actors still hold state tables,
   consume barrier alignment and occupy actor slots for the life of the job. Rescaling the CDC scan
   fragment to 1 actor after backfill would reclaim that, and is a meta-side change with no
   data-loss surface. Not investigated here.

## Production confirmation

The fragments observed at 16x fanout in `platform-prod` are confirmed CDC scan fragments.
`rw_fragments` maps them to table ids, and the `table_info` metric resolves those to names:

| fragment | table_id | table_name |
| --- | --- | --- |
| 995 | 2679 | `account_schedules` |
| 1053 | 2748 | `book_entries` |
| 1063 | 2768 | `postings` |
| 1077 | 2795 | `eod_events` |
| 1128 | 2898 | `entry_line` |

All are `table_type = TABLE` in compaction group 911, and are the Spanner CDC tables. Combined
with the measured 16x fanout and the fact that exactly one compute pod emits per fragment — one
actor, the holder of the merged split — the deployment matches the mechanism described above.

### Confirmed

`SELECT * FROM rw_catalog.rw_cdc_progress` returns **1 for every column of every CDC table** in
`platform-prod` (checked 2026-09-03).

`CdcTableBackfillTracker::gen_cdc_progress` (`cdc_progress.rs:183`) emits the literal
`split_total_count: 1, split_backfilled_count: 1, split_completed_count: 1` *only* in the
`PreCompleted`/`Completed` arm; while backfilling it reports the true split counts. All-ones is
therefore a direct read of the merged state, not a coincidence of small tables.

Every CDC table in this cluster is post-backfill, holding one merged whole-range split assigned to
a single actor, with the remaining fifteen actors of each scan fragment stranded and discarding the
full broadcast. No inference remains in the chain:

1. Fragments are CDC scan fragments — `rw_fragments` + `table_info`.
2. Fanout is 16x — measured.
3. Post-backfill assignment collapses to one split on one actor — code, and now `rw_cdc_progress`.
4. Stranded actors discard in the forwarding path — code (`cdc_backill_v2.rs:641`).
5. Exactly one pod emits per fragment — measured.

**The suppression change removes 15 of every 16 deliveries on these exchanges: 93.75%, or roughly
1.18 MB/s of the measured 1.26 MB/s, almost entirely cross-node.**
