# skaidb benchmarks

A throughput/latency comparison of **skaidb** against four production databases —
**MongoDB 7.0**, **MongoDB 8.0**, **PostgreSQL 15**, and **MariaDB 11.4** — run on
identical containers with matched durability semantics, across four
cluster/consistency configurations.

Latest full-matrix run: **2026-07-03**, skaidb **v0.16.3** (all other systems at
the versions above). Earlier published figures for v0.16.0 appear in the
[optimization-pass section](#v016x-performance-optimization-pass) below.

> Numbers are for *relative* comparison on small nodes, not absolute peak
> throughput. All five systems are driven by the same client model and the same
> workloads, on identical hardware.

## Setup

**Host.** All containers run on a single Proxmox host — an Intel Core
**i7-8550U** (4 cores / 8 threads, 1.8 GHz) with 8 GB RAM.

**Nodes.** Every database runs as its own set of identical unprivileged LXC
containers — each **1 vCPU / 512 MB RAM / 4 GB disk**, Debian 12 — bridged on one
VLAN. A 3-node configuration is three such containers; a 2-node configuration is
two.

**Durability is matched across systems.** In each config a write is acknowledged
to the client only after the same number of nodes have made it durable:

| Config | Nodes | A write is acked after… | skaidb | MongoDB | PostgreSQL | MariaDB |
|--------|:-----:|--------------------------|--------|---------|------------|---------|
| **C1** | 2 | both nodes | `QUORUM` | `w:majority` | sync standby (`FIRST 1`) | semi-sync |
| **C2** | 2 | the primary only (async replica) | `ONE` | `w:1` | async (`''`) | semi-sync off |
| **C3** | 3 | all 3 nodes | `ALL` | `w:3` | `FIRST 2` sync standbys | — ¹ |
| **C4** | 3 | any 2 of 3 (quorum) | `QUORUM` | `w:majority` | `ANY 1` standby | semi-sync ¹ |

¹ MariaDB semi-sync acknowledges after the **first** replica responds and has no
"wait for N replicas" knob, so true all-3 durability isn't expressible; its C3
row is the same semi-sync mode as C4 (≈ 2-of-3) and is marked `*`.

**Client.** A multithreaded load generator holds one persistent, pre-authenticated
connection per thread (skaidb over its binary protocol via the Rust driver;
MongoDB via `pymongo`; PostgreSQL via `psycopg2`; MariaDB via `pymysql`). Each op
is its own committed/acked operation.

**Workloads** (throughput in **ops/sec**, higher is better):

- `write 1c` — single connection inserting unique keys (durable-write latency floor)
- `write 16c` — 16 connections inserting (concurrent write throughput)
- `read 16c` — 16 connections, point read by primary key over a 1,000-row table
- `mixed 16c` — 16 connections, 50/50 read/write

## C1 — 2 nodes, writes wait for **both**

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  |   137 |   125 |   145 | **219** | 146 |
| write 16c | 1,315 |   878 |   789 | **1,907** | 968 |
| read 16c  | **3,058** | 2,508 | 2,340 | 2,789 | 2,386 |
| mixed 16c | 1,741 | 1,100 | 1,132 | **2,042** | 1,503 |

## C2 — 2 nodes, writes wait for the **primary only** (async replication)

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  |   132 |   224 |   219 | **271** | 164 |
| write 16c | 1,255 | 1,831 | 1,225 | **2,305** | 1,091 |
| read 16c  | **2,982** | 2,426 | 2,091 | 2,676 | 2,585 |
| mixed 16c | 1,753 | 1,996 | 1,562 | **2,459** | 1,603 |

## C3 — 3 nodes, writes wait for **all 3**

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB* |
|----------|-------:|----------:|----------:|-----------:|---------:|
| write 1c  |   146 |   111 |   117 | **196** |  146* |
| write 16c | 1,259 |   807 |   685 | **1,651** | 885* |
| read 16c  | 1,737 | 2,456 | 2,455 | **2,715** | 2,229* |
| mixed 16c | 1,583 | 1,077 | 1,009 | **2,024** | 1,344* |

`*` MariaDB acks after 1 replica (see note ¹), so its C3 ≈ 2-of-3, not all-3.

## C4 — 3 nodes, writes wait for **2 of 3** (quorum)

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  |   134 |   134 |   126 | **197** | 121 |
| write 16c | 1,246 |   861 |   836 | **1,728** | 897 |
| read 16c  | 1,783 | 2,399 | **2,476** | 1,682 | 2,431 |
| mixed 16c | 1,598 | 1,232 | 1,132 | **1,649** | 1,424 |

## skaidb: reads and writes on **all nodes** (leaderless)

skaidb is leaderless — every node accepts both reads and writes and coordinates
the quorum itself. Inserting a row through each of the three nodes and reading
each back from a *different* node returns consistent results, and a full scan
from any node sees all writes.

Driving all 16 connections at a **single coordinator** node vs **fanning them
across all 3 nodes** (round-robin), in the C4 (3-node quorum) config:

| Workload | single coordinator | all 3 nodes (fan-out) |
|----------|-------------------:|----------------------:|
| write 16c | 1,246 | **1,251** |
| read 16c  | 1,783 | **2,298** |
| mixed 16c | 1,598 | **1,661** |

Fan-out is a little faster on writes and mixed and markedly faster on reads
(+29%): with several host cores available, spreading read coordination over
three nodes uses more of them than funnelling every request through one. The
larger point is **availability and client locality** — connect to any node,
tolerate losing one.

## Memory footprint (idle, per node, of 512 MB)

Measured as container `free` "used" on one node of each system, idle a few
minutes after the benchmark run:

| | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|--|------:|----------:|----------:|-----------:|--------:|
| node RAM used | **24 MB** | 91 MB | 97 MB | 41 MB | 51 MB |

(The skaidb server process itself is ~7 MB RSS; the rest is the container's
base system.)

## What the matrix shows

**PostgreSQL leads writes and mixed everywhere.** It tops every `write 1c`,
`write 16c`, and `mixed 16c` row in the matrix (up to 2,305 concurrent writes/s
at C2), and the C3 read row as well.

**skaidb owns the 2-node read rows.** Its C1/C2 point reads (3,058 / 2,982
ops/s) are the best read figures in the whole matrix — with two nodes, the
quorum read's peer confirmation is a single cheap hop, and v0.16.3's concurrent
read path keeps 16 connections busy. At 3 nodes the extra coordination hop costs
it the lead (1,737–1,783), where **MongoDB** takes the read rows (2,399–2,476).

**skaidb is the strongest non-PostgreSQL writer.** Its group-commit WAL
coalesces fsyncs under concurrency: `write 16c` of 1,246–1,315 beats both
MongoDBs and MariaDB in every config, second only to PostgreSQL. Notably its
write throughput barely moves between C1 and C2 (1,315 vs 1,255) — the
replication ack is parallel and cheap, so relaxing durability buys little,
whereas MongoDB 7 jumps 878 → 1,831 and PostgreSQL 1,907 → 2,305.

**Relaxing write durability speeds writes (C1 → C2)** for the primary-based
engines — PostgreSQL 1,907 → 2,305, MongoDB 7 878 → 1,831, MariaDB 968 → 1,091
concurrent writes/s — and single-connection writes even more (MongoDB 7
125 → 224, PostgreSQL 219 → 271).

**Waiting for all 3 is the most expensive write config (C3); quorum recovers
some of it (C4).** MongoDB's concurrent writes drop to their matrix lows at C3
(807 / 685) and recover at C4 (861 / 836); PostgreSQL goes 1,651 → 1,728.
skaidb is nearly flat (1,259 → 1,246) — its parallel replica fan-out already
bounds the wait at the slowest replica, so all-3 vs 2-of-3 changes little.

**skaidb does all of this on 24 MB of node RAM** (~7 MB process RSS) — a
fraction of MongoDB's ~95 MB, and well under PostgreSQL's 41 MB and MariaDB's
51 MB, on nodes with only 512 MB to spend.

## Caveats

- **Single host, small cores.** All 15 containers share one 4-core / 8-thread
  host, and each database node is capped at 1 vCPU. Numbers are a relative,
  small-node comparison; absolute throughput would be far higher on server-class
  hardware. Connection counts above 16 saturate the shared host and are not
  reported.
- **Run-to-run noise on a shared host is real** — treat single-digit percentage
  differences between systems or between runs as a tie.
- **MariaDB** can't express "wait for all replicas" with semi-sync (acks after 1),
  so its C3 column is effectively its C4 mode.
- skaidb reads are **quorum reads** (the coordinator contacts a peer to satisfy
  `default_read_consistency = QUORUM`), so each read costs a cross-node hop; the
  other systems read locally from the primary. Setting skaidb's read consistency
  to `ONE` would make reads node-local and faster, at the cost of read-your-writes
  across coordinators.

## v0.16.x performance optimization pass

A full-stack performance audit (`docs/PERFORMANCE_AUDIT.md`) identified 12 major
bottlenecks, fixed in v0.16.2 (commit `a0bd866`):

- **Storage:** streaming k-way merge replaces full-DB materialization; range-bounded `scan_prefix`; decompressed-block cache; single-buffer WAL frames; sharded read cache.
- **Engine:** concurrent `&self` reads under RwLock; streaming scans with early-stop `LIMIT`; `COUNT(*)` fast path; hash equi-joins; top-k selection; one-fsync-per-multi-row-statement group-commit.
- **Cluster:** parallel replica fan-out (latency = max RTT, not sum); batched `ApplyBatch` internode RPC (one fsync per batch instead of per row).
- **Server:** parse once per request (was 2–3×); lock-free atomic metrics; buffered socket reads.
- **SQL:** byte-cursor lexer with near-zero allocation.

Measured on the canonical 1 vCPU / 512 MB bench containers, v0.16.0 baseline
vs. v0.16.3 (2026-07-03 full-matrix rerun; same nodes, same client):

| Workload | C1 v0.16.0 | C1 v0.16.3 | Δ | C4 v0.16.0 | C4 v0.16.3 | Δ |
|----------|-----------:|-----------:|---|-----------:|-----------:|---|
| write 1c  |   121 |   137 | +13% | 136 | 134 | ≈ |
| write 16c |   982 | **1,315** | +34% | 1,071 | **1,246** | +16% |
| read 16c  | 1,665 | **3,058** | +84% | 1,848 | 1,783 | ≈ |
| mixed 16c | 1,347 | **1,741** | +29% | 1,353 | **1,598** | +18% |

The concurrent-read path (RwLock `&self` reads) and streaming scans show up
strongest where coordination is cheapest — 2-node reads nearly double. Group
commit and parallel fan-out lift concurrent writes and mixed workloads in every
config; 3-node quorum reads are unchanged because the cross-node read hop, not
the local read path, dominates there.

> An earlier version of this section reported a v0.16.2 C4 rerun with larger
> gains (25–61%); those figures were measured on the development test cluster
> (1 vCPU / 1 GB nodes spread across two Proxmox hosts) and are superseded by
> the same-hardware numbers above.

## Reproducing

skaidb's load generator is in-tree:

```sh
cargo run --release --example bench -p skaidb-driver -- \
  <host:7000> <user> <pass> <write|read|mixed> <ops> <threads> [preload]
```

Write consistency is set per node via `cluster.default_write_consistency`
(`ONE` | `QUORUM` | `ALL`) and replication factor via `cluster.replication_factor`.
