# skaidb benchmarks

A throughput/latency comparison of **skaidb** against four production databases —
**MongoDB 7.0**, **MongoDB 8.0**, **PostgreSQL 15**, and **MariaDB 11.4** — run on
identical containers with matched durability semantics, across four
cluster/consistency configurations.

Latest full-matrix run: **2026-07-03**, skaidb **v0.16.5** (the coordination-path
optimization pass described [below](#v016x-performance-optimization-passes); all
other systems at the versions above). The **v0.16.6** async-tail replication
batching was A/B-measured against v0.16.5 on the same fleet on **2026-07-04**
(same-day, alternating binaries); its deltas are listed in the optimization-pass
section rather than re-rolled into the matrix.

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
across all 3 nodes** (round-robin), in the C4 (3-node quorum) config
(v0.16.6, measured 2026-07-04):

| Workload | single coordinator | all 3 nodes (fan-out) |
|----------|-------------------:|----------------------:|
| write 16c | 1,337 | 1,338 |
| read 16c  | 3,181 | 3,178 |
| mixed 16c | 1,891 | 1,879 |

Since the v0.16.5/v0.16.6 coordination-path reworks a single coordinator is no
longer the bottleneck at this connection count — fan-out and single-coordinator
are a statistical tie on every workload. The point of fan-out is
**availability and client locality** — connect to any node, tolerate losing
one — not throughput.

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

**v0.16.6**: replication batching on the **async tail**, plus a measured dead
end on the sync path:

- **Batched async-tail replication.** The coordinator's background worker now
  drains its whole task queue per wakeup and regroups the rows by peer and
  table, so a burst of asynchronous tail replication — the beyond-quorum
  replica at C4, *every* replica in C2's `ONE` mode — reaches each peer as a
  few `ApplyBatch` frames (one frame parse, one lock acquisition, one WAL
  fsync per batch on the peer) instead of one blocking round-trip per write.
- **Tried and reverted: sync-path replication group commit.** Coalescing
  concurrent sessions' quorum-path writes into shared per-peer `ApplyBatch`
  flushes (PostgreSQL-style cross-session group commit, including a bounded
  4-deep flush pipeline per peer) **cost ~9%** concurrent-write throughput
  (C4 write 16c: 1,325 → ~1,200 in same-day A/B). The coordinator is
  CPU-bound on these 1-vCPU nodes — C2, which never waits on a peer, hits the
  same ~1,350 writes/s ceiling as C1/C4 — so batching the internode frames
  saved CPU on the *peer* (which had headroom) while the queue/wake machinery
  added CPU on the *coordinator* (which had none). Reverted; the analysis is
  recorded next to the scatter path in `node.rs`.

Same-day A/B, v0.16.5 vs v0.16.6 binaries alternated on the same fleet
(2026-07-04; two runs each, averaged; unlisted workloads were unchanged
within noise):

| Config / workload | v0.16.5 | v0.16.6 | Δ |
|-------------------|--------:|--------:|--:|
| C2 write 16c      | 1,274 | **1,365** | +7% |
| C2 mixed 16c      | 1,844 | **1,934** | +5% |
| C4 mixed 16c      | 1,742 | **1,881** | +8% |
| C4 write 16c      | 1,310 | 1,330 | +2% |
| fan-out write 16c | 1,305 | 1,337 | +2% |

The C2 gains are the direct effect (in `ONE` mode all replication is tail
replication); the C4 mixed gain comes from the freed background-worker CPU —
each C4 write ships one beyond-quorum tail copy, and batching those frees
coordinator cycles that the interleaved reads then use.

**v0.16.7**: an allocation/copy pass on the framing layers, driven by a CPU
profile of the coordinator under 16-connection write load (allocator calls
were ~20% of on-CPU cycles; every internode send built three buffers and
every received ack allocated twice):

- **Reused frame buffers everywhere.** Client connections and pooled
  internode connections now read each frame into a per-connection buffer and
  build outbound frames (length prefix + payload, encoded in place) in
  another — steady-state, a request/response pair allocates nothing in the
  framing layer on either side.
- **Fused internode message writes.** A replicated write used to be encoded
  into one buffer, wrapped into a compression envelope in a second, and
  coalesced with the length prefix into a third; it is now encoded once,
  directly behind the frame header, and written with one call.
- **Borrowing decode.** Uncompressed internode payloads (acks, point ops) are
  decoded straight out of the connection's read buffer instead of being
  copied out of the envelope first.

Profile after the pass: internode ack handling fell from 16% to 12% of
coordinator CPU, send-side from 4.5% to 3.3%, and the proto-decode and
response-encode allocations disappeared. **Same-day A/B on the fleet measured
no throughput change** (C4 write 16c: 1,283 vs 1,291 avg; C2: 1,359 vs 1,367)
— the saved cycles are a few percent of per-op service time, under this
setup's ±5% noise floor. The pass ships anyway: it is strictly less CPU and
allocator pressure per operation, which matters more on TLS internode links
and busier cores.

The remaining concurrent-write gap to PostgreSQL on this hardware (~25% at
C4) is per-statement service time spread across SQL parsing (~8% of
coordinator CPU), WAL append + memtable (~24%), and executor setup — there is
no single plumbing lever left. The structural next step was prepared
statements at the protocol level — measured below (v0.17.0).

**v0.17.0**: **prepared statements**, end to end — `?` placeholders in the
grammar, `Prepare`/`Execute`/`Close` opcodes in the binary protocol
(per-connection statement cache, parse once / bind per call), and
`prepare()`/`execute_prepared()` in the Rust driver. Statements are bound
server-side (typed values, no SQL-injection surface) and the per-request SQL
parse disappears.

Both skaidb and PostgreSQL were benchmarked text vs prepared **same-day, same
C4 config, alternating runs** (PostgreSQL via `PREPARE`/`EXECUTE` in the same
client — both systems get the same fairness):

| C4 workload | skaidb text | skaidb prepared | postgres text | postgres prepared |
|-------------|------------:|----------------:|--------------:|------------------:|
| write 1c    |   148 |   148 |   216 |   214 |
| write 16c   | 1,294 | 1,309 | 1,686 | **1,771** |
| read 16c    | 2,951 | **3,214** | 2,790 | 2,808 |
| mixed 16c   | 1,784 | **1,853** | 2,000 | **2,110** |

What it shows, honestly:

- **skaidb reads gain ~9%** (3,214 ops/s — extending the same-day read lead
  over PostgreSQL to +14% when both use prepared statements) and **mixed
  gains ~4%** with visibly tighter p99s. Point reads are the workload where
  the parse was the largest share of service time.
- **skaidb writes are flat.** A durable quorum write's service time is
  fsync + replication + WAL/memtable work; the ~40µs parse was never a
  measurable part of it on this hardware (the same lesson as the v0.16.7
  pass, now confirmed from the other direction).
- **PostgreSQL gains ~5% on writes and mixed** from its own prepared path, so
  fully-prepared-vs-fully-prepared slightly *widens* its concurrent-write
  lead (1,771 vs 1,309). Its per-statement planner overhead is larger than
  skaidb's parser, so it has more to save.
- Beyond throughput, prepared statements are a correctness/safety feature:
  typed server-side bindings replace client-side string interpolation as the
  injection boundary, and clients spend less CPU formatting SQL.

Methodology note: an earlier read of this experiment showed prepared mixed
*losing* 12% — an artifact of leg ordering (the prepared leg always ran last,
behind extra table-churn legs). Alternating text/prepared runs back-to-back
reversed the sign. On this fleet, position-in-sequence effects are the same
size as single-digit deltas; only alternating same-day A/Bs are trusted.

**v0.18.0**: **`memory_target`** and **`NEAREST`** (SQL vector search) — an
opt-in storage memory budget
(`[storage] memory_target = "auto"` or an explicit `"512MB"`-style size) that
sizes the memtable and read cache together instead of via two independent
knobs. `"auto"` detects the node's own memory limit — the **cgroup** limit
when one applies, so a container gets its own budget, not the host's — and
spends half of it on storage. This landed alongside two correctness fixes
anti-entropy testing on a 1M-row table surfaced:

- **Bounded background-replication queue.** The async-tail queue
  ([[v0.16.6]]'s batching target) was unbounded; a sustained large preload
  that outran the tail could queue hundreds of MB of cloned rows and get the
  node OOM-killed on a 512 MB node. It's now a bounded channel (1024 tasks);
  full means the write becomes a hint instead, reconciled by the mechanism
  below. Merged background sends are also chunked (2,000 rows/frame) so one
  drain can't build one multi-megabyte frame with a multi-second lock hold on
  the receiving replica.
- **Paged anti-entropy.** `repair()` used to pull whole tables into memory on
  both ends to diff them — fine at thousand-row scale, but it also OOM-killed
  512 MB nodes at a million rows. It's now a merge-join over two paged,
  key-ordered scans (2,000 rows/page each side), so repair memory is
  O(page), not O(table), regardless of dataset size.

Both were caught by *scaling the benchmark itself* to 1M rows for this
release — the existing 1,000-row suite could never have found them.

**What the numbers show, honestly:** at this fleet's benchmark shape (16
client connections, quorum reads crossing 1-vCPU LXCs on one oversubscribed
Proxmox host), `memory_target` produced **no measurable throughput change** —
and neither did the cache-size gap it's meant to control:

| 1M rows × 100B, C4 | skaidb default | skaidb `memory_target=auto` |
|---------------------|---------------:|------------------------------:|
| read 16c, 100k hot set | 3,175 | 3,206 |
| read 16c, uniform      | 2,939 | 2,867 |

That's a tie in both directions — and it turns out *every* database tested
lands in the same band on this leg, regardless of engine or cache config:
PostgreSQL 2,980–3,102, MongoDB 7 ~2,540, MongoDB 8 ~2,435, MariaDB
2,764–2,863. A ping from the **Proxmox host itself** (not the WiFi'd
workstation) to a bench node still shows ~5ms RTT — this fleet's per-op floor
is the shared, oversubscribed host and the cross-node quorum hop, not client
network or storage-engine caching. 16 connections × ~5ms/op ≈ 3,200 ops/s is
almost exactly the ceiling every system hits here; raising to 64 connections
on skaidb lifts it to 4,347 ops/s (confirming it's a concurrency/queuing
ceiling, not a hard cap) — the fleet just isn't shaped to isolate what a
storage memory budget does.

Isolated from that confound — one node, loopback (no quorum hop, no VLAN),
1M rows, comparing a deliberately **undersized** 48 MB budget (94% of rows
forced out of the 16 MB memtable onto SSTables) against everything fitting in
a 256 MB memtable — the mechanism is real, and its cost is small on fast
local storage:

| Config (single node, loopback) | read 16c throughput |
|---------------------------------|---------------------:|
| 256 MB memtable (all rows resident) | 321,038 ops/s |
| 48 MB budget (94% of rows on SSTables) | 294,887 ops/s |

An ~8% cost for correctly shrinking a node's RAM commitment to a fraction of
its dataset — the block cache and bloom filters keep SSTable point-reads
nearly memtable-speed. `memory_target` is a genuine, verified capacity
control; it just isn't the lever that moves *this* cluster benchmark, because
this cluster benchmark is bound by something else entirely. Worth knowing
before reaching for it to fix a throughput number — check whether the
bottleneck is actually memory-bound first.

## Reproducing

skaidb's load generator is in-tree:

```sh
cargo run --release --example bench -p skaidb-driver -- \
  <host:7000> <user> <pass> <write|read|mixed> <ops> <threads> [preload]
```

Write consistency is set per node via `cluster.default_write_consistency`
(`ONE` | `QUORUM` | `ALL`) and replication factor via `cluster.replication_factor`.
