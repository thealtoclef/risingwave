#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

sink_test_env_setup "$profile" --sleep-duration 0

export DORIS_HOST="doris-server"
export DORIS_HTTP_PORT="8030"
export DORIS_QUERY_PORT="9030"
export DORIS_CONTAINER="doris-server"
export DORIS_USER="users"
export DORIS_PASSWORD="123456"
export DORIS_DATABASE="demo"
export RISEDEV_DORIS_WITH_OPTIONS_COMMON="connector='doris',doris.url='http://doris-server:8030',doris.user='users',doris.password='123456',doris.database='demo'"

create_doris_table() {
  local ddl

  ddl="CREATE DATABASE IF NOT EXISTS demo;
USE demo;
DROP TABLE IF EXISTS demo_bhv_table;
CREATE TABLE demo_bhv_table(v1 int,v2 smallint,v3 bigint,v4 float,v5 double,v6 string,v7 datev2,v8 datetime,v9 boolean,v10 json) UNIQUE KEY(\`v1\`)
DISTRIBUTED BY HASH(\`v1\`) BUCKETS 1
PROPERTIES (
    \"replication_allocation\" = \"tag.location.default: 1\"
);
DROP TABLE IF EXISTS demo_variant_table;
CREATE TABLE demo_variant_table(id int, v variant) UNIQUE KEY(\`id\`)
DISTRIBUTED BY HASH(\`id\`) BUCKETS 1
PROPERTIES (
    \"replication_allocation\" = \"tag.location.default: 1\"
);
DROP TABLE IF EXISTS demo_stream_load_url_table;
CREATE TABLE demo_stream_load_url_table(v1 int,v2 smallint,v3 bigint,v4 float,v5 double,v6 string,v7 datev2,v8 datetime,v9 boolean,v10 json) UNIQUE KEY(\`v1\`)
DISTRIBUTED BY HASH(\`v1\`) BUCKETS 1
PROPERTIES (
    \"replication_allocation\" = \"tag.location.default: 1\"
);
DROP TABLE IF EXISTS demo_tstz_table;
CREATE TABLE demo_tstz_table(v1 int, v2 timestamptz(6)) UNIQUE KEY(\`v1\`)
DISTRIBUTED BY HASH(\`v1\`) BUCKETS 1
PROPERTIES (
    \"replication_allocation\" = \"tag.location.default: 1\"
);
-- Target for the \`commit_checkpoint_interval\` case: several checkpoints are batched into one
-- stream load, and repeated primary keys land inside a single load.
DROP TABLE IF EXISTS demo_decoupled_table;
CREATE TABLE demo_decoupled_table(v1 int, v2 varchar(50)) UNIQUE KEY(\`v1\`)
DISTRIBUTED BY HASH(\`v1\`) BUCKETS 1
PROPERTIES (
    \"replication_allocation\" = \"tag.location.default: 1\",
    \"enable_unique_key_merge_on_write\" = \"true\"
);
-- Target for the partial-update case. The sink's schema covers only (v1, v2), so v3 must keep the
-- value seeded below after the sink writes.
DROP TABLE IF EXISTS demo_partial_update_table;
CREATE TABLE demo_partial_update_table(v1 int, v2 varchar(50), v3 varchar(50)) UNIQUE KEY(\`v1\`)
DISTRIBUTED BY HASH(\`v1\`) BUCKETS 1
PROPERTIES (
    \"replication_allocation\" = \"tag.location.default: 1\",
    \"enable_unique_key_merge_on_write\" = \"true\"
);
INSERT INTO demo_partial_update_table VALUES (1, 'orig2', 'orig3');
-- Target for the payload-cap case: a small \`doris.max_batch_size_bytes\` splits one chunk across
-- several stream loads, and every row must still land exactly once.
DROP TABLE IF EXISTS demo_split_table;
CREATE TABLE demo_split_table(v1 int, v2 varchar(50)) UNIQUE KEY(\`v1\`)
DISTRIBUTED BY HASH(\`v1\`) BUCKETS 1
PROPERTIES (
    \"replication_allocation\" = \"tag.location.default: 1\"
);
-- Target for the type-narrowing rejection case: \`v2 smallint\` is narrower than RisingWave's
-- \`int\`, so creating the sink must fail instead of silently NULLing out-of-range values.
DROP TABLE IF EXISTS demo_narrow_table;
CREATE TABLE demo_narrow_table(v1 int, v2 smallint) UNIQUE KEY(\`v1\`)
DISTRIBUTED BY HASH(\`v1\`) BUCKETS 1
PROPERTIES (
    \"replication_allocation\" = \"tag.location.default: 1\"
);
-- Intentionally NOT created here: this is auto-created by RisingWave via the sink's
-- auto_create option.
DROP TABLE IF EXISTS demo_auto_create_table;"

  echo "--- create doris table"
  for _ in $(seq 1 60); do
    if mysql -uroot -P 9030 -h doris-server -e "$ddl"; then
      return
    fi
    mysql -uroot -P 9030 -h doris-server -e "SHOW BACKENDS;" || true
    sleep 2
  done

  echo "Doris backend did not become ready for table creation in time"
  mysql -uroot -P 9030 -h doris-server -e "SHOW BACKENDS;" || true
  exit 1
}

create_doris_table
mysql -uroot -P 9030 -h doris-server -e "CREATE USER 'users'@'%' IDENTIFIED BY '123456';
GRANT ALL ON *.* TO 'users'@'%';"
sleep 2

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/doris_sink.slt'
sleep 1
mysql -uroot -P 9030 -h doris-server -e "select * from demo.demo_bhv_table" > ./query_result.csv
mysql -uroot -P 9030 -h doris-server -N -B -e "select id, cast(v as string) from demo.demo_variant_table order by id" > ./variant_result.tsv
mysql -uroot -P 9030 -h doris-server -e "select * from demo.demo_stream_load_url_table" > ./stream_load_url_result.csv
# Query in UTC so the assertion compares against the UTC instant RisingWave produced, not the
# Doris session timezone.
mysql -uroot -P 9030 -h doris-server -N -B -e "SET time_zone = '+00:00'; select v1, v2 from demo.demo_tstz_table order by v1" > ./tstz_result.tsv
# The auto-created table only exists if RisingWave created it via `auto_create`. Fail loudly if it
# is missing. Note: the upsert sink's validation already rejects a non-UNIQUE-KEY target at
# CREATE SINK time, so a passing SLT proves the auto-created table is a UNIQUE KEY table.
mysql -uroot -P 9030 -h doris-server -N -B -e "select v1, v2, v3 from demo.demo_auto_create_table order by v1" > ./auto_create_result.tsv


if cat ./query_result.csv | sed '1d; s/\t/,/g' | awk -F "," '{
    exit !($1 == 1 && $2 == 1 && $3 == 1 && $4 == 1.1 && $5 == 1.2 && $6 == "test" && $7 == "2013-01-01" && $8 == "2013-01-01 01:01:01" && $9 == 0 && $10 == "{\"a\":1}"); }'; then
  echo "Doris sink check passed"
else
  cat ./query_result.csv
  echo "The output is not as expected."
  exit 1
fi

if cat ./variant_result.tsv | awk -F "\t" '
{
    seen++;
    gsub(/[[:space:]]/, "", $2);
    if ($1 == 1 && $2 == "{\"nested\":[1,2]}") {
        matched++;
    }
}
END {
    exit !(seen == 1 && matched == 1);
}'; then
  echo "Doris variant sink check passed"
else
  cat ./variant_result.tsv
  echo "The variant output is not as expected."
  exit 1
fi

# Verify the auto-created table: it must have been created by RisingWave and contain the row.
if cat ./auto_create_result.tsv | awk -F "\t" '
{
    seen++;
    gsub(/[[:space:]]/, "", $2);
    if ($1 == 1 && $2 == "auto" && $3 == 100) {
        matched++;
    }
}
END {
    exit !(seen == 1 && matched == 1);
}'; then
  echo "Doris auto-create sink check passed"
else
  cat ./auto_create_result.tsv
  echo "The auto-create output is not as expected."
  exit 1
fi

# Verify the row inserted via `doris.stream_load_url` (direct BE write): the sink routed the
# _stream_load PUT to doris-server:8040, so Doris should hold the row we inserted into t6_direct.
if cat ./stream_load_url_result.csv | sed '1d; s/\t/,/g' | awk -F "," '{
    exit !($1 == 2 && $2 == 2 && $3 == 2 && $4 == 2.1 && $5 == 2.2 && $6 == "direct" && $7 == "2014-02-02" && $8 == "2014-02-02 02:02:02" && $9 == 1 && $10 == "{\"b\":2}"); }'; then
  echo "Doris doris.stream_load_url check passed"
else
  cat ./stream_load_url_result.csv
  echo "The doris.stream_load_url output is not as expected."
  exit 1
fi

# Verify the TIMESTAMPTZ round-trip: RisingWave inserts `2024-01-02 03:04:05.123456+00:00`,
# which the encoder must convert into the UTC microsecond instant `2024-01-02 03:04:05.123456`.
# We queried with `SET time_zone = '+00:00'`, so the value printed must match that instant
# exactly. Any session-local re-rendering (a tz-naive string landed in DATETIME) would show up
# here as a different microsecond count, failing the test.
if cat ./tstz_result.tsv | awk -F "\t" '
BEGIN { matched = 0; bad = 0 }
{
    ts = $2;
    # Acceptable shapes: `YYYY-MM-DD HH:MM:SS.uuuuuu` or `YYYY-MM-DD HH:MM:SS.uuuuuu +00:00`.
    if (ts == "2024-01-02 03:04:05.123456" || ts == "2024-01-02 03:04:05.123456 +00:00") {
        matched = 1;
    } else {
        bad = 1;
    }
}
END {
    exit !(matched == 1 && bad == 0);
}'; then
  echo "Doris TIMESTAMPTZ sink check passed"
else
  cat ./tstz_result.tsv
  echo "The Doris TIMESTAMPTZ sink output is not as expected."
  exit 1
fi

# Verify partial update: the sink covers only (v1, v2), so v2 must be patched while v3 keeps the
# value the fixture seeded. Without the `columns` request header Doris cannot do a partial update,
# so a regression there shows up here as v3 being reset to NULL.
mysql -uroot -P 9030 -h doris-server -N -B -e "select v1, v2, v3 from demo.demo_partial_update_table order by v1" > ./partial_update_result.tsv
if cat ./partial_update_result.tsv | awk -F "\t" '
{
    seen++;
    if ($1 == 1 && $2 == "patched" && $3 == "orig3") {
        matched++;
    }
}
END {
    exit !(seen == 1 && matched == 1);
}'; then
  echo "Doris partial update check passed"
else
  cat ./partial_update_result.tsv
  echo "The Doris partial update output is not as expected."
  exit 1
fi

# Verify the payload-cap case: the 64-byte `doris.max_batch_size_bytes` split the three rows across
# separate stream loads, and all three must be present exactly once.
mysql -uroot -P 9030 -h doris-server -N -B -e "select v1, v2 from demo.demo_split_table order by v1" > ./split_result.tsv
if cat ./split_result.tsv | awk -F "\t" '
{
    seen++;
    if (($1 == 1 && $2 == "aaaaaaaaaaaaaaaaaaaa") || ($1 == 2 && $2 == "bbbbbbbbbbbbbbbbbbbb") || ($1 == 3 && $2 == "cccccccccccccccccccc")) {
        matched++;
    }
}
END {
    exit !(seen == 3 && matched == 3);
}'; then
  echo "Doris max_batch_size_bytes split check passed"
else
  cat ./split_result.tsv
  echo "The Doris max_batch_size_bytes output is not as expected."
  exit 1
fi

# Verify the `commit_checkpoint_interval = '2'` case. The sink for this table is deliberately still
# running: dropping a decoupled sink does not flush it, so the rows land through the sink's own
# interval commits rather than at teardown. That makes the timing asynchronous but the final
# contents deterministic, so poll instead of sleeping a fixed amount.
echo "--- verifying commit_checkpoint_interval sink"
decoupled_ok=""
for _ in $(seq 1 60); do
  mysql -uroot -P 9030 -h doris-server -N -B -e "select v1, v2 from demo.demo_decoupled_table order by v1" > ./decoupled_result.tsv || true
  if cat ./decoupled_result.tsv | awk -F "\t" '
{
    seen++;
    if (($1 == 1 && $2 == "r2") || ($1 == 2 && $2 == "r4") || ($1 == 3 && $2 == "r3")) {
        matched++;
    }
}
END {
    exit !(seen == 3 && matched == 3);
}'; then
    decoupled_ok="yes"
    break
  fi
  sleep 2
done

if [ -n "$decoupled_ok" ]; then
  echo "Doris commit_checkpoint_interval sink check passed"
else
  cat ./decoupled_result.tsv
  echo "The Doris commit_checkpoint_interval output is not as expected."
  exit 1
fi

echo "--- Kill cluster"
risedev ci-kill
