# Event lag for created CDC tables

Shared CDC sources can capture an entire upstream database. The source parser observes events
before downstream table routing, so `source_cdc_event_lag_duration_milliseconds` alone does not
identify the CDC tables created in RisingWave. Do not filter it by matching upstream names to
RisingWave names or by removing schema prefixes.

## Metrics and identity

The event-lag histogram retains its upstream `table_name` label and adds `cdc_table_id`.
The identity is the source ID followed by the upstream routing name, using the same
`build_cdc_table_id` helper as table creation. PostgreSQL/Citus/SQL Server routing names omit
the database prefix; MySQL names retain it. Different sources with identically named tables
therefore have different identities. The observation remains source-event timestamp to
reader-processing delay, in milliseconds; heartbeats do not produce table lag observations.

Meta exports `cdc_table_info` with labels:

| Label | Meaning |
|---|---|
| `cdc_table_id` | Source-scoped upstream identity used to join event-lag observations |
| `table_id` | RisingWave table ID |
| `database` | RisingWave database name |
| `schema` | RisingWave schema name |
| `table_name` | RisingWave table name, including renames |

Each value is 1. Membership requires a catalog table with a CDC identity and a streaming job
in `Created` state. It excludes ordinary tables, internal tables and jobs still being created.
The catalog mapping covers shared-source CDC tables created with `FROM <source> TABLE ...`;
legacy connector-backed tables without a catalog `cdc_table_id` are outside this mapping.
Legacy SQL Server identities are normalized using the recorded connector type and the same
three-part-name compatibility rule as the CDC filter.

The existing meta information monitor refreshes membership every 60 seconds, including tables
that are idle or have never emitted an event. It resets previous labels before publishing the
new snapshot, so a drop or rename is reflected after refresh and scrape. A failed catalog query
clears membership rather than continuing to export stale membership. Source observations may
still include captured upstream-only tables for diagnostics; the catalog join excludes those
from the created-table view without changing ingestion or routing behavior.

## Queries and dashboard migration

Aggregate histogram buckets **by `le, cdc_table_id`**, calculate a percentile, then join the
result to `cdc_table_info` with `on(cdc_table_id) group_right()`. The right-hand metric carries
the RisingWave table identity and permits multiple RisingWave tables consuming one upstream
table. Deduplicate meta series by all catalog labels before joining; retain cluster/namespace/
RisingWave name scope on both metrics.

The developer dashboard's CDC event-lag panel uses this join. Updated generated panels are
included in the Grafana and Docker dashboard files.

[The operator panel definition](../../grafana/created-cdc-table-panel.json) is prepared for the
custom platform dashboard. It lists only catalog members, shows database/schema/table/ID,
and separates three cases:

- A finite p99: the measured event delay in seconds.
- Zero observed events during the selected interval: **No recent events**.
- Missing or insufficient lag observations for a catalog member: **Lag unavailable**.

Internal values -1 and -2 support numeric sorting and have display mappings; they are not
negative lag measurements. The list includes every catalog mapping even when no histogram
series exists. An idle status alone does not prove reader or destination health.

Deploy the updated meta and compute binaries before applying the operator panel definition.
During a mixed-version rollout, old histogram series lack `cdc_table_id`; they are intentionally
excluded from this exact join, and catalog members without new observations show unavailable.
Do not silently fall back to upstream-name matching. Production Grafana has not been changed
as part of this code change because these metrics are not deployed there yet.

This is observed event lag, not current unread backlog age or end-to-end destination freshness.
The histogram's largest finite bucket is approximately 1048 seconds; very large delays are
bounded by that bucket resolution and should be diagnosed using connector progress/backlog
and retention metrics too.
