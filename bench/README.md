# Comparison benchmark harness

The tooling used to produce [`../docs/BENCHMARKS.md`](../docs/BENCHMARKS.md) —
the comparison of skaidb against MongoDB 7/8, PostgreSQL 15, and MariaDB 11.4 on
identical small nodes (1 vCPU / 512 MB / 4 GB), with matched write durability.

skaidb's own load generator lives in-tree as a driver example
(`cargo run --release --example bench -p skaidb-driver -- ...`); this directory
holds the equivalents for the other databases plus the cluster setup scripts, so
the comparison is reproducible.

```
bench/
  clients/         load generators (one persistent conn per thread)
    mongo_bench.py     # pymongo
    pg_bench.py        # psycopg2
    maria_bench.py     # pymysql
  setup/           per-node cluster provisioning (run inside each Debian node)
    mongo_node.sh
    postgres_primary.sh / postgres_standby.sh
    mariadb_primary.sh / mariadb_replica.sh
  run_suite.sh     runs the standard workload set and appends CSV rows
```

> **Credentials & hosts are parameters, not defaults.** Every script reads
> addresses and passwords from arguments/env vars (placeholder defaults like
> `changeme`). Nothing here is wired to a specific host. Set strong passwords
> when you run them.

## Client dependencies

```sh
python3 -m venv venv && . venv/bin/activate
pip install pymongo psycopg2-binary pymysql
```

## Workloads

`run_suite.sh` runs four cases per database: `write` @1 conn, `write` @16,
`read` @16 (point read by primary key over 1,000 rows), `mixed` @16 (50/50).
Throughput is ops/sec; it also records p50/p99 latency.

## Durability configs

A write is acked only after the configured number of nodes have it durable
(see `docs/BENCHMARKS.md` for the full table):

| Config | nodes | acked after | skaidb | MongoDB | PostgreSQL | MariaDB |
|--------|:-----:|-------------|--------|---------|------------|---------|
| C1 | 2 | both | `QUORUM` | `w:majority` | sync standby | semi-sync ON |
| C2 | 2 | primary only | `ONE` | `w:1` | `synchronous_standby_names=''` | semi-sync OFF |
| C3 | 3 | all 3 | `ALL` | `w:3` | `FIRST 2` standbys | (n/a, see note) |
| C4 | 3 | 2 of 3 | `QUORUM` | `w:majority` | `ANY 1` standby | semi-sync ON |

skaidb consistency is a node config (`cluster.default_write_consistency`);
MongoDB's is the client write concern (`MONGO_W`); PostgreSQL's is
`synchronous_standby_names` on the primary; MariaDB's is
`rpl_semi_sync_master_enabled`.

## Running

```sh
# skaidb (in-tree example as the client prefix)
CSV=results.csv ./run_suite.sh skaidb C4 \
  ../target/release/examples/bench NODE1:7000 skaidb "$PASS"

# skaidb fanned across all nodes (leaderless: any node coordinates)
CSV=results.csv ./run_suite.sh skaidb C4-fanout \
  ../target/release/examples/bench NODE1:7000,NODE2:7000,NODE3:7000 skaidb "$PASS"

# MongoDB (write concern via env)
CSV=results.csv MONGO_W=majority ./run_suite.sh mongo C4 \
  python3 clients/mongo_bench.py NODE1:27017,NODE2:27017

# PostgreSQL (server-side synchronous_standby_names sets durability)
CSV=results.csv PG_PASS="$PASS" ./run_suite.sh postgres C4 \
  python3 clients/pg_bench.py PRIMARY_IP

# MariaDB
CSV=results.csv MARIA_PASS="$PASS" ./run_suite.sh mariadb C4 \
  python3 clients/maria_bench.py PRIMARY_IP
```

Results accumulate in the CSV as `db,config,workload,conns,throughput,p50,p99`.
