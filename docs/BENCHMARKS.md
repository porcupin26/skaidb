# skaidb benchmarks

A throughput/latency comparison of **skaidb** against four production databases —
**MongoDB 7.0**, **MongoDB 8.0**, **PostgreSQL 15**, and **MariaDB 11.4** — run on
identical containers with matched durability semantics, across four
cluster/consistency configurations.

Full-matrix run: **2026-07-03**, skaidb **v0.16.5**. Releases since (through
v0.19.0) were A/B-measured against their predecessors on the same fleet;
everything landed within this setup's noise on these workloads **except
prepared-statement reads (+9%)** — see
[Current performance notes](#current-performance-notes-v0190) for what changed
and what it means in practice.

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
| write 16c | 1,337 | 1,338 |
| read 16c  | 3,181 | 3,178 |
| mixed 16c | 1,891 | 1,879 |

A single coordinator is not the bottleneck at this connection count — fan-out
and single-coordinator are a statistical tie on every workload. The point of
fan-out is **availability and client locality** — connect to any node, tolerate
losing one — not throughput.

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
others read purely locally from the primary. With prepared statements on both
sides the read lead widens further (below).

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

**Waiting for all 3 (C3) vs quorum (C4)** costs skaidb almost nothing
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
  differences between systems or between runs as a tie. Only alternating
  same-day A/B runs are trusted for release-to-release deltas
  (position-in-sequence effects are the same size as single-digit deltas).
- **This fleet has a shared per-op floor** (~5 ms from the oversubscribed host
  plus cross-node quorum RTT): on read-heavy legs *every* database lands in the
  same ~2,400–3,200 ops/s band regardless of engine or cache configuration. If
  a change doesn't move this fleet's numbers, isolate it on a single node over
  loopback before concluding it does nothing.
- **MariaDB** can't express "wait for all replicas" with semi-sync (acks after 1),
  so its C3 column is effectively its C4 mode.
- skaidb reads are **quorum reads** (the coordinator confirms with a peer to
  satisfy `default_read_consistency = QUORUM`), so each read still costs a
  cross-node round-trip; the other systems read locally from the primary.
  Setting skaidb's read consistency to `ONE` would make reads node-local and
  faster still, at the cost of read-your-writes across coordinators.

## Current performance notes (v0.19.0)

The durable findings from the optimization work since the matrix run — what to
use and what to expect. (Per-release histories and the record of measured dead
ends live in [Performance engineering notes](#performance-engineering-notes) below and git history.)

**Use prepared statements for read-heavy work.** `?` placeholders +
`Prepare`/`Execute` (v0.17.0) make point reads ~9% faster (C4 read 16c:
2,951 → 3,214, extending the same-day lead over PostgreSQL-prepared to +14%)
with tighter p99s; mixed gains ~4%. Writes are flat — a durable quorum write's
service time is fsync + replication, and the ~40 µs parse was never a
measurable part of it. Beyond speed, server-side typed bindings replace
client-side string interpolation as the injection boundary.

**Use streamed queries for large results.** A buffered `SELECT` holds the whole
result on both sides and cannot return a result set past the 64 MiB frame limit
at all. `query_stream()` (v0.19.0) sends ~256 KB chunks: on a 55 MB result it
was measurably *faster* end-to-end (274 → 213 ms, transfer overlaps decode) and
cut client peak RSS from ~140 MB to <9 MB; a 66 MB result that the buffered
path refuses streams through in ~270 ms. Single node, loopback, release build.

**`memory_target` is a capacity control, not a throughput lever.**
`[storage] memory_target = "auto"` (v0.18.0) budgets the memtable + read cache
from the node's cgroup/host memory limit. Isolated on loopback with 1M rows, a
deliberately undersized 48 MB budget cost only ~8% read throughput vs
everything fitting in a 256 MB memtable — block cache + bloom filters keep
SSTable point-reads nearly memtable-speed. On the fleet it changed nothing,
because the fleet is network-bound (see Caveats). Check whether your bottleneck
is actually memory before reaching for it.

**Scale exposed real bugs the small suite never hit.** Loading 1M rows into
512 MB nodes OOM-killed them twice: an unbounded background-replication queue
and an unpaged anti-entropy pass (both fixed in v0.18.0 — bounded queue,
paged merge-join repair), and v0.19.0 paged the distributed full-table scan the
same way. Benchmarks are now expected to scale to the feature's target size,
not the suite's historical 1,000 rows.

## Full-text search vs Elasticsearch (v0.38, 2026-07-08)

The FTS performance exit benchmark: skaidb `SEARCH INDEX` against
Elasticsearch 8.14.3 on **identical fresh containers** (p225: 2 vCPU /
2 GB / 25 GB Debian 12 LXC each), one system running at a time with the
rest of the bench fleet stopped. skaidb v0.38 + the background NRT
refresher (found by this bench, see below), `memory_target = "1GB"`; ES
with a 1 GB heap, 1 shard, 0 replicas, security off. Corpus: **280,595
Simple English Wikipedia articles** (lead prose, ≤ 2,000 chars — Wikimedia
discontinued the abstract dumps), identical bytes to both engines, both on
their `standard` analyzer and 1 s refresh, per-batch durability (skaidb:
WAL fsync per statement; ES: translog fsync per bulk request). Client: one
connection on each system's canonical protocol (skaidb binary / ES HTTP
keep-alive) from a container on the same bridge (0.1 ms RTT). 1,000-doc
batches; queries are the same 400 generated term/AND/OR/phrase inputs,
top-10 ranked. Two alternating runs per system; run-2 (warm) shown for ES,
skaidb was stable across both.

|                       | skaidb 0.38 | Elasticsearch 8.14.3 |
|-----------------------|------------:|---------------------:|
| ingest (docs/s)       |  **10,600** |   7,000 (5,200 cold) |
| term p50 / p95 (ms)   | **0.5 / 0.7** |           5.8 / 10.4 |
| AND p50 / p95 (ms)    | **0.5 / 0.6** |            5.0 / 8.2 |
| OR p50 / p95 (ms)     | **0.7 / 0.9** |            5.1 / 8.5 |
| phrase p50 / p95 (ms) | **0.7 / 5.4** |           4.9 / 11.2 |
| NRT visibility (ms)   |    43–1,197 |            414–2,594 |
| RSS after ingest (MB) |    **~650** |               ~1,490 |
| disk, all data (MB)   |     **336** |                  529 |

Both §4 single-node targets hold on this hardware: query latency ≤ ES on
every class, ingest ≥ ES bulk.

**Cluster leg** (3-node test cluster on v0.39, RF=3 QUORUM, 1 GB nodes,
60 k docs): ingest 3,053 docs/s through one coordinator (every row
replicated ×3), ranked top-10 scatter queries p50 2–3 ms / p99
2.9–7.6 ms (term/AND/OR) — the scatter adds well under the ≤ 10 ms p99
budget over single-node. NRT visibility 149 ms cluster-wide. Kill -9 on a
member left searches complete, a quorum write during the outage landed,
and the rejoined node served the converged result set.

Caveats, honestly: part of the per-query gap is protocol — ES answers
JSON-over-HTTP (its only surface), skaidb its binary protocol; both are
each system's canonical path, but they are not equal-cost framing. A 1 GB
heap is small for ES (it is also half the container, matching skaidb's
budget); hit counts agreed within tokenizer-level differences (AND 850 vs
845, phrase 551 vs 495). Single query stream; the host carries unrelated
(identical for both) background load. Cluster scatter-gather overhead
(§4's ≤ 10 ms p99 target) is a separate leg on the 3-node test cluster,
pending its upgrade to ≥ v0.38.

**The bench found a real bug**: skaidb's NRT probe initially hung forever —
index refresh checks ran only on the write path, so an idle table's *last*
writes never became visible to read-only searches. Fixed with a background
refresher tick in the server (v0.39); the probe then measured 43–1,197 ms,
inside the refresh_ms + tick bound.

**Result-set parity** (the FTS query-DSL exit, same corpus and
queries): mean top-10 overlap per query against ES initially measured
89.2% — score traces put nearly all of the divergence in tokenization
(skaidb's `standard` split on every non-alphanumeric; ES uses Unicode word
segmentation, so postings, phrase adjacency, and length norms differed).
Replacing the simple tokenizer with a UAX §29 word tokenizer (v0.39)
brought it to **98.5% strict top-10 overlap / 99.8% with tie tolerance**
(each engine's top-10 within the other's top-15) across
term/AND/OR/phrase, with per-query hit counts matching ES exactly and no
measurable query-latency cost. The remaining ~1.5% is BM25 fieldnorm
quantization flipping near-tied docs at the cutoff. Run it:
`fts_bench.py parity <skaidb:7080> <es:9200> <data_dir>`.

Reproduce: `bench/clients/fts_corpus.py` (corpus + query generation from a
MediaWiki dump) and `bench/clients/fts_bench.py`
(`fts_bench.py <skaidb|es> <addr> <setup|ingest|query|nrt> <data_dir>`).

### Aggregations (logs track, v0.41)

The phase-6 exit: a 500 k-doc synthetic http_logs-shape corpus
(`bench/clients/fts_logs_corpus.py` — request line text, keyword
method/status, long bytes, date ts), same containers and procedure as
above, 20 MATCH-filtered aggregation queries per class, warm runs:

|                                     | skaidb 0.41 | Elasticsearch 8.14.3 |
|-------------------------------------|------------:|---------------------:|
| ingest (docs/s)                     |  **34,400** |  20,500 (15,400 cold) |
| terms buckets p50 (ms)              |    **0.6** |                  6.8 |
| date_histogram p50 (ms)             |    **1.5** |                 10.4 |
| global stats p50 (ms)               |    **2.1** |                  6.3 |
| COUNT(DISTINCT) p50 (ms)            |    **0.5** (exact) |     4.4 (approx.) |
| grouped multi-metric p50 (ms)       |        276 |             **10.4** |

**Result parity: 80/80 aggregation queries identical** (bucket sets,
counts, sums, averages; skaidb's `COUNT(DISTINCT)` is exact where ES's
cardinality is HLL-approximate).

The grouped multi-metric row is the interesting one — and it's the row
that made this benchmark worth running. The first pass pushed grouped
`SUM`/`AVG` into Tantivy sub-aggregations and the parity check caught
**silently wrong sums on minority buckets** (counts exact, sums ~40% low):
an upstream tantivy 0.26.1 bug where `CachedSubAggs::flush_local` drops
small buckets' cached doc ids uncollected on periodic flushes (triggered
by merged segments > 2048 docs). No count-based exactness check can see
it. skaidb now declines that pushdown shape — grouped queries push down
only when every metric reads the bucket's doc count, and per-bucket
metrics take the exact row-materialization fallback (the 276 ms above,
~29 k rows per query). The gap closes when the upstream fix lands.

## Reproducing

skaidb's load generator is in-tree:

```sh
cargo run --release --example bench -p skaidb-driver -- \
  <host:7000> <user> <pass> <write|read|mixed> <ops> <threads> [preload]
```

Modes `writep`/`readp`/`mixedp` use prepared statements. `preload` accepts
`NxS` (e.g. `1000000x100`: rows × payload bytes) for large-dataset legs, and
`READ_SPAN` limits the read key range (hot-set reads). Secondary-index numbers
come from `cargo run --release --example index_bench -p skaidb-engine`
([INDEX_BENCH.md](INDEX_BENCH.md)).

Write consistency is set per node via `cluster.default_write_consistency`
(`ONE` | `QUORUM` | `ALL`) and replication factor via `cluster.replication_factor`.

## Performance engineering notes

*Absorbed from the standalone performance audit (originally 2026-07-03,
v0.16.0; everything actionable was implemented and measured across
v0.16.2 – v0.19.0). The remaining open items live in [TODO.md](TODO.md)
under `[perf]`; what follows is the record that keeps dead ends dead.*

### What already holds

- Scans stream (k-way merge over memtable + SSTables, O(pages) memory) with
  early-stop for plain `LIMIT`; `scan_prefix` is range-bounded.
- Reads run under a shared lock (`&self`); writes group-commit one fsync per
  multi-row statement; replica fan-out is pipelined and batched
  (`ApplyBatch`), with the async tail drained and regrouped per peer.
- Equi-joins hash-join; unfiltered `COUNT(*)` answers from key stats; `ORDER
  BY … LIMIT k` is a selection, not a full sort; per-table block cache +
  sharded read cache + bloom filters on the point-read path.
- SQL parses once per request; prepared statements skip the parse entirely;
  hot metrics are pre-registered atomics; framing layers reuse per-connection
  buffers and allocate nothing steady-state.
- Row-returning results can stream in ~256 KB chunks (`QueryStream`), and
  distributed full-table gathers + anti-entropy are paged (2,000 rows/page) —
  coordinator memory is O(pages in flight), not O(table).
- Client connections **pipeline**: id-tagged requests (`OP_TAGGED`) let a
  client keep any number of requests in flight per connection; the server
  executes serially in order (session semantics unchanged) and echoes the id
  on every frame, so a batch pays one round-trip of link latency
  (`Client::pipeline`). Old servers reject the opcode cleanly.
- Topology changes are paged: rebalance, drain, and reclaim scan shards
  2,000 rows at a time (like repair/gathers) — a join or decommission
  against a multi-GB table holds one page + one in-flight batch, not the
  shard.
- `[profile.release]`: `lto = "thin"`, `codegen-units = 1`.
- Checked and fine: TCP_NODELAY everywhere, pooled internode connections,
  no locks held across network calls, no per-row re-parsing, compact binary
  wire protocol with LZ4 above 256 B.
- The pre-`ScanPage` repair fallback (used only against peers too old to
  answer `ScanPage`) necessarily remains O(table) — the old peer's wire
  protocol has no paging. It never fires between current versions.

### Deliberately skipped (documented reasons)

- **Atomic HLC.** `HlcClock` stays a `Mutex` — it is only taken under the
  write lock, which already serializes writers, and repacking the 96-bit
  state risks the on-disk stamp format.
- **Boxing `Statement::Select`.** `clippy::large_enum_variant` is allowed
  with a comment instead; boxing would touch every match site in the engine
  for no runtime benefit.

### Measured dead ends — do not re-attempt without new evidence

- **Sync-path replication group commit** (tried v0.16.6): coalescing
  concurrent sessions' quorum writes into shared per-peer `ApplyBatch`
  flushes cost **~9%** concurrent-write throughput. The coordinator is
  CPU-bound on small nodes; the batching saved CPU on the peer (which had
  headroom) and spent it on coordinator queue/wake machinery (which had
  none). Analysis recorded next to the scatter path in `node.rs`. Async-tail
  batching (kept) is the part that paid.
- **Borrowed (`Cow`) predicate evaluation** (tried v0.19.0): removing the
  per-row column-value clones from `eval` measured **3–6% slower** on
  AND-chain predicates over 200k-row scans, reproducibly (alternating A/B,
  three rounds). Per-row document *decode* dominates scan cost — even
  deleting a 60-byte string clone per row was unmeasurable — while the `Cow`
  wrapper added real per-node overhead. Any future attempt must first make
  row decode itself borrowed/lazy; predicate-eval tweaks alone are
  optimizing a rounding error.
- **Allocation cleanups on fsync/RTT-dominated paths** (v0.16.7 framing
  pass): real CPU/allocator reduction, zero throughput change on this
  hardware. Fine to ship for CPU headroom, but don't expect ops/s.

### Methodology (hard-won, follow it)

- Scale the benchmark to the feature's target size — the 1,000-row suite hid
  two OOM bugs that 1M rows exposed immediately.
- Only trust alternating same-day A/B runs; leg ordering alone produces
  double-digit artifacts on a shared host.
- If every system lands in the same band on a fleet leg, suspect a shared
  environmental floor and isolate on a single node over loopback before
  concluding a change does nothing (or something).
