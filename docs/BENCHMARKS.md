# skaidb benchmarks

A throughput/latency comparison of **skaidb** against four production databases —
**MongoDB 7.0**, **MongoDB 8.0**, **PostgreSQL 15**, and **MariaDB 11.4** — run on
identical containers with matched durability semantics, across four
cluster/consistency configurations.

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
| write 1c  |   121 |   126 |   131 | **176** |  99 |
| write 16c |   982 |   891 | 1,066 | **1,124** | 892 |
| read 16c  | 1,665 | 1,534 | 2,081 | **2,170** | 1,156 |
| mixed 16c | 1,347 | 1,247 | 1,493 | **1,870** | 912 |

## C2 — 2 nodes, writes wait for the **primary only** (async replication)

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  |   162 | **245** |   239 |   227 | 152 |
| write 16c | 1,132 | 1,847 | 1,880 | **2,130** | 1,033 |
| read 16c  | 2,223 | 2,141 | 2,200 | **2,627** | 2,350 |
| mixed 16c | 1,535 | 2,095 | 1,695 | **2,260** | 1,442 |

## C3 — 3 nodes, writes wait for **all 3**

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB* |
|----------|-------:|----------:|----------:|-----------:|---------:|
| write 1c  |   113 |   121 |   108 | **144** |  92* |
| write 16c |   842 |   729 |   643 | **1,236** | 737* |
| read 16c  | 1,397 | **2,284** | 1,875 | 2,014 | 1,481* |
| mixed 16c | 1,186 |   885 |   848 | **1,433** | 1,119* |

`*` MariaDB acks after 1 replica (see note ¹), so its C3 ≈ 2-of-3, not all-3.

## C4 — 3 nodes, writes wait for **2 of 3** (quorum)

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  |   136 |   140 |   113 | **180** | 139 |
| write 16c | 1,071 |   836 |   717 | **1,250** | 842 |
| read 16c  | 1,848 | **2,346** | 2,157 | 2,026 | 2,138 |
| mixed 16c | 1,353 | 1,000 | 1,078 | **2,089** | 1,207 |

## skaidb: reads and writes on **all nodes** (leaderless)

skaidb is leaderless — every node accepts both reads and writes and coordinates
the quorum itself. Inserting a row through each of the three nodes and reading
each back from a *different* node returns consistent results, and a full scan
from any node sees all writes.

Driving all 16 connections at a **single coordinator** node vs **fanning them
across all 3 nodes** (round-robin), in the C4 (3-node quorum) config:

| Workload | single coordinator | all 3 nodes (fan-out) |
|----------|-------------------:|----------------------:|
| write 16c | 1,011 | **1,071** |
| read 16c  | 1,997 | **2,310** |
| mixed 16c | 1,417 | **1,481** |

Fan-out is a little faster across the board (read +16%): with several host cores
available, spreading coordination over three nodes uses more of them than
funnelling every request through one. The larger point is **availability and
client locality** — connect to any node, tolerate losing one.

## Memory footprint (idle, per node, of 512 MB)

| | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|--|------:|----------:|----------:|-----------:|--------:|
| node RAM used | **16 MB** | 145 MB | 150 MB | 45 MB | 136 MB |

## What the matrix shows

**Relaxing write durability speeds writes (C1 → C2).** Acking on the primary only,
instead of both nodes, lifts concurrent write throughput sharply for the mature
engines — PostgreSQL 1,124 → 2,130, MongoDB 7 891 → 1,847, MongoDB 8 1,066 →
1,880 — and single-connection writes too (MongoDB 7 126 → 245). skaidb rises as
well (write 16c 982 → 1,132; write 1c 121 → 162) but less dramatically.

**Waiting for all 3 is the most expensive write config (C3); quorum recovers it
(C4).** Single-connection writes drop at C3 and climb back at C4 (skaidb 113 →
136, MongoDB 8 108 → 113, PostgreSQL 144 → 180) — waiting for 2 of 3 instead of
all 3 cuts the per-write wait while still surviving one node down.

**PostgreSQL is the most consistent leader,** topping most write and mixed rows.
**MongoDB** leads several read rows (2,284–2,346 at 3 nodes). **MariaDB** trails on
concurrent/mixed writes (semi-sync + per-statement InnoDB commit).

**skaidb holds mid-pack** — close to MongoDB/PostgreSQL across the board and
strong on **concurrent writes** (its group-commit WAL coalesces fsyncs: write 16c
of 1,071 at C4 is second only to PostgreSQL), competitive on reads (point reads
route to the key's replica set). It does so on **16 MB of RAM per node**, 3–9×
less than every other system.

## Caveats

- **Single host, small cores.** All 15 containers share one 4-core / 8-thread
  host, and each database node is capped at 1 vCPU. Numbers are a relative,
  small-node comparison; absolute throughput would be far higher on server-class
  hardware. Connection counts above 16 saturate the shared host and are not
  reported.
- **MongoDB 8's WiredTiger is the heaviest on RAM** (150 MB idle) and the most
  variable under load on a 512 MB node.
- **MariaDB** can't express "wait for all replicas" with semi-sync (acks after 1),
  so its C3 column is effectively its C4 mode.
- skaidb reads are **quorum reads** (the coordinator contacts a peer to satisfy
  `default_read_consistency = QUORUM`), so each read costs a cross-node hop; the
  other systems read locally from the primary. Setting skaidb's read consistency
  to `ONE` would make reads node-local and faster, at the cost of read-your-writes
  across coordinators.

## Reproducing

skaidb's load generator is in-tree:

```sh
cargo run --release --example bench -p skaidb-driver -- \
  <host:7000> <user> <pass> <write|read|mixed> <ops> <threads> [preload]
```

Write consistency is set per node via `cluster.default_write_consistency`
(`ONE` | `QUORUM` | `ALL`) and replication factor via `cluster.replication_factor`.
