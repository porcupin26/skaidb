# skaidb benchmarks

A throughput/latency comparison of **skaidb** against four production databases —
**MongoDB 7.0**, **MongoDB 8.0**, **PostgreSQL 15**, and **MariaDB 11.4** — run on
identical containers with matched durability semantics, across four
cluster/consistency configurations.

Latest full-matrix run: **2026-07-03**, skaidb **v0.16.5** (the coordination-path
optimization pass described [below](#v016x-performance-optimization-passes); all
other systems at the versions above).

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
| write 1c  |   149 |   105 |   139 | **216** | 153 |
| write 16c | 1,369 |   897 |   849 | **1,924** | 979 |
| read 16c  | **3,064** | 2,503 | 2,231 | 2,605 | 2,449 |
| mixed 16c | 1,848 | 1,196 | 1,145 | **2,175** | 1,464 |

## C2 — 2 nodes, writes wait for the **primary only** (async replication)

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  |   141 |   239 |   216 | **261** | 150 |
| write 16c | 1,348 | 1,725 | 1,275 | **2,270** | 1,071 |
| read 16c  | **3,234** | 2,418 | 2,131 | 2,714 | 2,058 |
| mixed 16c | 1,809 | 1,965 | 1,554 | **2,249** | 1,514 |

## C3 — 3 nodes, writes wait for **all 3**

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB* |
|----------|-------:|----------:|----------:|-----------:|---------:|
| write 1c  |   149 |   120 |   120 | **184** | 147* |
| write 16c | 1,290 |   802 |   696 | **1,357** | 915* |
| read 16c  | 2,486 | 2,408 | 2,393 | **2,664** | 2,601* |
| mixed 16c | 1,734 | 1,136 |   904 | **1,770** | 1,410* |

`*` MariaDB acks after 1 replica (see note ¹), so its C3 ≈ 2-of-3, not all-3.

## C4 — 3 nodes, writes wait for **2 of 3** (quorum)

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  |   149 |   127 |   138 | **183** | 134 |
| write 16c | 1,328 |   886 |   791 | **1,684** | 886 |
| read 16c  | **3,000** | 2,256 | 2,255 | 1,958 | 2,066 |
| mixed 16c | 1,856 | 1,201 | 1,098 | **1,999** | 1,364 |

## skaidb: reads and writes on **all nodes** (leaderless)

skaidb is leaderless — every node accepts both reads and writes and coordinates
the quorum itself. Inserting a row through each of the three nodes and reading
each back from a *different* node returns consistent results, and a full scan
from any node sees all writes.

Driving all 16 connections at a **single coordinator** node vs **fanning them
across all 3 nodes** (round-robin), in the C4 (3-node quorum) config:

| Workload | single coordinator | all 3 nodes (fan-out) |
|----------|-------------------:|----------------------:|
| write 16c | **1,328** | 1,174 |
| read 16c  | 3,000 | **3,156** |
| mixed 16c | **1,856** | 1,748 |

Since the v0.16.5 coordination-path rework a single coordinator no longer
bottlenecks on threading, so fan-out only wins on pure reads (spreading the
local-read work); on writes the extra cross-node coordination slightly
outweighs it. The larger point of fan-out remains **availability and client
locality** — connect to any node, tolerate losing one.

## Memory footprint (idle, per node, of 512 MB)

Measured as container `free` "used" on one node of each system, idle a few
minutes after the benchmark run:

| | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|--|------:|----------:|----------:|-----------:|--------:|
| node RAM used | **25 MB** | 91 MB | 97 MB | 41 MB | 51 MB |

(The skaidb server process itself is ~6 MB RSS; the rest is the container's
base system.)

## What the matrix shows

**skaidb owns the read rows.** Its point reads lead every config except C3
(where PostgreSQL edges it by 7%): 3,064 ops/s at C1, 3,234 at C2, and 3,000
at C4 — 53% ahead of PostgreSQL's 1,958 in the headline 3-node-quorum config,
despite skaidb doing a cross-node quorum confirmation per read while the
others read purely locally from the primary.

**PostgreSQL still leads writes and mixed, but the gap has closed sharply.**
It tops every `write` and `mixed` row. At C3 the margins are now 5% on
concurrent writes (1,357 vs 1,290) and 2% on mixed (1,770 vs 1,734) —
effectively ties on this hardware; at C4 they are 27% and 8%. PostgreSQL's
remaining edge is a commit path that is at the network-latency floor
(single-digit-connection writes) and decades of group-commit tuning under
concurrency (C2's async 2,270 writes/s).

**skaidb is the strongest non-PostgreSQL writer in every durable config.** Its
group-commit WAL and pipelined replication put `write 16c` at 1,290–1,369
across C1/C3/C4 — ahead of both MongoDBs and MariaDB everywhere. Its write
throughput barely moves with the consistency level (C2's `ONE` 1,348 ≈ C1's
both-nodes 1,369): the replica round-trip is fully overlapped with the local
fsync, so relaxing durability buys almost nothing — whereas MongoDB 7 jumps
897 → 1,725 and PostgreSQL 1,924 → 2,270 when freed from the sync ack.

**Waiting for all 3 (C3) vs quorum (C4)** now costs skaidb almost nothing
(1,290 vs 1,328 concurrent writes): the second peer's append+fsync happens in
parallel with the first's. MongoDB pays the most for C3 (mongo8: 696 vs 791).

**skaidb does all of this on 25 MB of node RAM** (~6 MB process RSS) — a
fraction of MongoDB's ~95 MB and half of PostgreSQL's 41 MB, on nodes with
only 512 MB to spend.

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
- skaidb reads are **quorum reads** (the coordinator confirms with a peer to
  satisfy `default_read_consistency = QUORUM`), so each read still costs a
  cross-node round-trip; the other systems read locally from the primary.
  Setting skaidb's read consistency to `ONE` would make reads node-local and
  faster still, at the cost of read-your-writes across coordinators.

## v0.16.x performance optimization passes

**v0.16.2** (commit `a0bd866`): a full-stack audit (`docs/PERFORMANCE_AUDIT.md`)
fixed 12 bottlenecks — streaming k-way merge scans, decompressed-block cache,
sharded read cache, concurrent `&self` reads under RwLock, `COUNT(*)` fast
path, hash equi-joins, group-commit WAL, parallel replica fan-out, batched
internode `ApplyBatch` RPC, lock-free metrics, byte-cursor lexer.

**v0.16.5**: a coordination-path pass driven by syscall profiling on the bench
nodes (an `strace -c` of the coordinator showed the per-write cost was thread
machinery — `clone3` + stack setup + futex parks — not I/O; the fdatasync
itself is ~50–170 µs on these containers):

- **Thread-free pipelined replication.** The coordinator no longer spawns a
  thread per peer per write. It puts each quorum peer's RPC on the wire, runs
  its local WAL fsync while the peers append+fsync, then collects the acks —
  all on the request thread (`Pool::call_begin`/`Pending::finish`).
- **Quorum-count read fan-out.** A quorum read now consults exactly the quorum
  (local replica + `needed-1` peers, pipelined the same way) instead of every
  replica — at RF=3 that halves cluster-wide read work. Unconsulted replicas
  converge via read-repair on ALL reads and anti-entropy, as before.
- **Statement-level replication batching.** A multi-row `INSERT` is now one
  `ApplyBatch` round-trip per peer and one fsync per node for the whole
  statement, instead of one replication round per row.
- **Parse-once request path** now covers cluster mode (the coordinator reuses
  the statement parsed for the privilege check).
- **`[storage]` config actually applies.** `memtable_size_mb` (and the new
  `read_cache_entries`) now reach the storage engine; previously they were
  parsed but never wired, so every node silently ran hard-coded defaults.

Measured on the canonical 1 vCPU / 512 MB bench containers, all on 2026-07-03
(same nodes, same client, C4 = 3-node quorum):

| Workload | v0.16.0 | v0.16.3 | v0.16.5 | v0.16.5 vs v0.16.0 |
|----------|--------:|--------:|--------:|-------------------:|
| write 1c  |   136 |   134 |   149 | +10% |
| write 16c | 1,071 | 1,246 | **1,328** | +24% |
| read 16c  | 1,848 | 1,783 | **3,000** | +62% |
| mixed 16c | 1,353 | 1,598 | **1,856** | +37% |

> An earlier version of this section reported a v0.16.2 C4 rerun with 25–61%
> gains; those figures were measured on the development test cluster
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
