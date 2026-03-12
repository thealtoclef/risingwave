CREATE SOURCE "dev"."public"."src_ice" WITH (
  connector = 'spanner-cdc',
  spanner.project = 'ice-as-se1-stg',
  spanner.instance = 'ic-spanner-stg-master',
  database.name = 'ice',
  spanner.change_stream.name = 'ice_change_streams',
  spanner.change_stream.max_concurrent_partitions = 10,
  spanner.heartbeat_interval = '5s',
  auto.schema.change = 'true',
);

CREATE TABLE "dev"."public"."accounts" (*) WITH (
  backfill.parallelism = 3,
  spanner.databoost.enabled = 'true',
  spanner.partition_query.parallelism = 4,
)
FROM "dev"."public"."src_ice"
TABLE 'accounts';
