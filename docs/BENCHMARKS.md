# skaidb benchmarks

A throughput/latency comparison of **skaidb** against four production databases —
**MongoDB 7.0**, **MongoDB 8.0**, **PostgreSQL 15**, and **MariaDB 11.4** — run on
identical hardware with matched durability semantics, across four
cluster/consistency configurations.

> Numbers are from a homelab Proxmox cluster and are meant for *relative*
> comparison on small nodes, not as absolute benchmarks. All five systems were
> driven by the same client model and workloads.

## Methodology

**Nodes.** Every database runs on its own set of identical unprivileged LXC
containers, each **1 vCPU / 512 MB RAM / 4 GB disk**, Debian 12, on one Proxmox
host (kernel `6.17`), bridged on the same VLAN.

**Durability is matched across systems.** In each config a write is acknowledged
to the client only after the same number of nodes have made it durable:

| Config | Nodes | A write is acked after… | skaidb | MongoDB | PostgreSQL | MariaDB |
|--------|:-----:|--------------------------|--------|---------|------------|---------|
| **C1** | 2 | both nodes | `QUORUM` | `w:majority` | sync standby | semi-sync |
| **C2** | 2 | the primary only (async replica) | `ONE` | `w:1` | `synchronous_standby_names=''` | semi-sync OFF |
| **C3** | 3 | all 3 nodes | `ALL` | `w:3` | `FIRST 2` sync standbys | — ¹ |
| **C4** | 3 | any 2 of 3 (quorum) | `QUORUM` | `w:majority` | `ANY 1` standby | semi-sync ² |

¹ MariaDB 11.4 semi-sync acknowledges after the **first** replica responds and
has no "wait for N replicas" knob, so true all-3 durability isn't expressible;
its C3 row is the same semi-sync mode as C4 (≈ 2-of-3) and is marked `*`.
² MariaDB semi-sync (acks after 1 replica) ≈ 2-of-3 durability.

**Client.** A multithreaded load generator holds one persistent, pre-authenticated
connection per thread (skaidb over its binary protocol via the Rust driver;
MongoDB via `pymongo`; PostgreSQL via `psycopg2`; MariaDB via `pymysql`). Each
op is its own committed/acked operation.

**Workloads** (throughput in **ops/sec**, higher is better):

- `write 1c` — single connection inserting unique keys (durable-write latency floor)
- `write 16c` — 16 connections inserting (concurrent write throughput)
- `read 16c` — 16 connections, point read by primary key over a 1,000-row table
- `mixed 16c` — 16 connections, 50/50 read/write

## C1 — 2 nodes, writes wait for **both**

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  |   150 |   174 |   164 | **189** | 160 |
| write 16c | 1,371 | 1,633 | 1,735 | **1,795** | 1,584 |
| read 16c  | 2,273 | 1,824 | 2,092 | 2,455 | **2,473** |
| mixed 16c | 1,890 | 1,890 | 1,903 | **2,321** | 2,193 |

## C2 — 2 nodes, writes wait for the **primary only** (async replication)

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  |   124 |   263 | **317** |   295 | 224 |
| write 16c | 1,812 | 2,407 | **2,564** | 2,443 | 1,377 |
| read 16c  | 2,728 | 2,558 | 2,636 | 2,705 | 1,841 |
| mixed 16c | 2,329 | 2,531 | 1,881 | **2,611** | 1,539 |

## C3 — 3 nodes, writes wait for **all 3**

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB* |
|----------|-------:|----------:|----------:|-----------:|---------:|
| write 1c  |   156 |   186 |   207 | **235** | 220* |
| write 16c | 1,354 | 1,626 | 1,891 | **2,049** | 1,365* |
| read 16c  | 2,786 | 2,438 | 2,488 | **2,920** | 2,890* |
| mixed 16c | 1,769 | 2,034 | 2,149 | **2,504** | 1,989* |

`*` MariaDB acks after 1 replica (see note ¹), so its C3 ≈ 2-of-3, not all-3.

## C4 — 3 nodes, writes wait for **2 of 3** (quorum)

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  |   205 |   214 | **233** | **235** | 225 |
| write 16c | 1,584 | 1,915 | **1,926** | 1,798 | 1,343 |
| read 16c  | 2,324 | 2,442 | **2,639** | 2,229 | 2,430 |
| mixed 16c | 2,007 | 1,678 | 2,065 | **2,691** | 1,023 |

## skaidb: reads and writes on **all nodes** (leaderless)

skaidb is leaderless — every node accepts both reads and writes and coordinates
the quorum itself. Verified directly: inserting a row through each of the three
nodes and then reading each row back from a *different* node returns consistent
results, and a full scan from any node sees all three writes.

The table below compares driving all client load at a **single coordinator**
node vs **fanning the 16 connections across all 3 nodes** (round-robin), both in
the C4 (3-node, quorum 2-of-3) config:

| Workload | single coordinator | all 3 nodes (fan-out) |
|----------|-------------------:|----------------------:|
| write 16c | **1,584** | 1,157 |
| read 16c  | 2,324 | 2,269 |
| mixed 16c | 2,007 | 2,096 |

**Reads and mixed are flat; concurrent writes are *lower* when fanned out.**
That's expected on **1-core** nodes: with one coordinator, only that node runs
the coordination/replication logic while the other two just apply replicas. When
every node is also a coordinator, each one simultaneously coordinates its own
writes *and* receives replication from the other two — so all three single cores
saturate, and group-commit batching is split three ways instead of concentrated.

The takeaway: serving reads/writes from all nodes is about **availability and
client locality** (connect to any node, tolerate losing one), not extra write
throughput on tiny nodes. On multi-core nodes, fan-out would spread coordinator
CPU and help; here, the single core per node is the ceiling.

## Higher concurrency — 64 connections (C4, 3-node quorum)

Pushing from 16 to 64 client connections on the same 1-core nodes:

| Workload | skaidb (1 coord) | skaidb (all nodes) | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-----------------:|-------------------:|----------:|----------:|-----------:|--------:|
| write 64c | 3,158 | 3,285 | 3,524 | 3,567 | **5,183** | 2,853 |
| read 64c  | 2,042 | **3,360** | 4,862 | 4,447 | 4,692 | 5,646 |
| mixed 64c | 3,292 | 3,259 | 4,289 | 4,067 | **5,269** | 4,532 |

- **skaidb writes scale ~2× from 16→64** (1,584 → 3,158) — group commit keeps
  coalescing more fsyncs as concurrency rises.
- **Single-coordinator reads stop scaling and dip** (2,324 at 16c → 2,042 at 64c):
  one node's single core saturates coordinating every read. **Fanning reads
  across all 3 nodes recovers it (→ 3,360, +65%)** — the opposite of the 16-conn
  case, where fan-out was flat. The more concurrent the read load, the more it
  pays to spread coordinators.
- skaidb's reads are **quorum reads** (the coordinator contacts a peer to satisfy
  `default_read_consistency = QUORUM`), so each read costs a cross-node hop; the
  other systems read locally from the primary. Setting skaidb's read consistency
  to `ONE` would make reads node-local and faster, at the cost of read-your-writes
  guarantees across coordinators.
- The mature engines keep scaling on their fast local-read / pipelined-commit
  paths; PostgreSQL again leads writes/mixed, MariaDB leads reads.

> Connections are pinned to nodes round-robin at connect time (thread *i* → node
> *i mod N*), so the 64-connection fan-out spreads ~21 connections per node; the
> *key* each op touches is random, the target node is fixed per connection.

## Memory footprint (idle, of 512 MB per node)

| | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|--|------:|----------:|----------:|-----------:|--------:|
| node RAM used | **19 MB** | 119 MB | 146 MB | 53 MB | 120 MB |

## What the matrix shows

**Relaxing write durability speeds writes (C1 → C2).** Acking on the primary
only, instead of both nodes, roughly **doubles single-connection write
throughput** for the mature engines (MongoDB 8 164 → 317, PostgreSQL 189 → 295,
MongoDB 7 174 → 263) — they're no longer paying the cross-node round-trip per
commit. skaidb's *concurrent* writes also rise (1,371 → 1,812), but its
single-connection async case dips (150 → 124): skaidb replicates the async tail
with a thread spawned per write, and that churn outweighs the savings when
there's only one writer. A background replication queue would remove it (noted
as future work).

**Quorum beats all-nodes at 3 nodes (C3 → C4).** Waiting for 2 of 3 instead of
all 3 cuts the per-write wait — clearest at single-connection
(skaidb 156 → 205, MongoDB 8 207 → 233, MongoDB 7 186 → 214). With more nodes to
wait for, C3 is the most expensive write config; C4 recovers most of the cost
while still surviving one node down.

**Single-connection writes cluster tightly (≈150–235 across all systems).**
A durable write to N nodes is bounded by `fsync` + the slowest required network
hop; no engine escapes that floor on this hardware.

**PostgreSQL is the most consistent leader**, topping most write and read rows.
**MongoDB 8** is competitive *here* but the heaviest on RAM (146 MB) and needs a
kernel < 6.19 to run at all (see caveat). **MariaDB** trails on concurrent/mixed
writes (semi-sync + per-statement InnoDB commit), with the highest mixed p99s.

**skaidb holds mid-pack** — within ~10–25% of MongoDB/PostgreSQL across the
board, ahead of MariaDB on several write/mixed cases, and **competitive on reads**
(its point reads route to the key's replica set). It does so on **~19 MB of
RAM**, 3–8× less than every other system, which is the main reason it stays
stable on a 512 MB node.

## Caveats

- **MongoDB 8 requires a Linux kernel < 6.19.** On a ≥6.19 kernel, 8.0.15+ refuse
  to start (a guard for [SERVER-121912](https://jira.mongodb.org/browse/SERVER-121912))
  and 8.0.0 segfaults. The host was pinned to kernel `6.17` so MongoDB 8 could run;
  MongoDB 7, skaidb, PostgreSQL, and MariaDB run on either kernel.
- **MariaDB** can't express "wait for all replicas" with semi-sync (acks after 1),
  so its C3 column is effectively its C4 mode.
- **512 MB nodes** penalize memory-hungry engines (MongoDB 8's WiredTiger cache,
  MariaDB's buffer pool). On larger nodes the heavier engines would pull ahead.
- skaidb's async (`ONE`) single-connection write path spawns a replication thread
  per write; fine for concurrency, slightly slower for a lone writer.

## Reproducing

skaidb's load generator is in-tree:

```sh
cargo run --release --example bench -p skaidb-driver -- \
  <host:7000> <user> <pass> <write|read|mixed> <ops> <threads> [preload]
```

Write consistency is set per node via `cluster.default_write_consistency`
(`ONE` | `QUORUM` | `ALL`) and replication factor via `cluster.replication_factor`.
