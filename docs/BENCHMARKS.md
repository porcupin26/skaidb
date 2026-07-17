# skaidb benchmarks

The latest measured results for every benchmark scenario, the environment
they ran in, and how to reproduce them. Each scenario header carries its
run date and the exact versions measured; when a scenario is re-run, its
section is replaced wholesale (superseded results live in git history).
Durable conclusions — root causes, measured dead ends, methodology — are
kept separately in [Performance engineering notes](#performance-engineering-notes)
at the bottom so they survive result refreshes.

> Numbers are for *relative* comparison on small nodes, not absolute peak
> throughput. Within one scenario all systems are driven by the same
> client model and workloads on identical hardware, so relative standings
> are trustworthy even when absolute figures move between rounds. Never
> compare absolute numbers across scenarios or rounds.

## Environment

**Host.** One Proxmox host — Intel Core **i7-8550U** (4 cores / 8
threads, 1.8 GHz), 8 GB RAM. Every node below is an unprivileged LXC
container on this host, bridged on one VLAN.

**Nodes.** Identical containers: **1 vCPU / 512 MB RAM / 4 GB disk**,
Debian 12. A 3-node configuration is three such containers. The only
exception is the full-text-search pair (skaidb-FTS vs Elasticsearch),
which uses two **2 vCPU / 2 GB** containers.

**Client.** Always VLAN-local: a dedicated client container for the
multi-node scenarios (workstation→VLAN RTT measured 17–40 ms — it would
dominate every latency under test), colocated on the server container for
the single-node C0 scenario (true loopback, zero network hop).

**Conditions.** One system benchmarked at a time; no other load on the
host during a leg (the 2026-07-17 round ran with no background soak and
idle non-participant containers). Run-to-run spread on this host is
±10–15% — treat differences inside that band as noise.

**Versions measured** (per scenario date):

| Scenario | Date | skaidb | PostgreSQL | MongoDB 7 / 8 | MariaDB | Elasticsearch |
|---|---|---|---|---|---|---|
| C1–C4 | 2026-07-17 | 0.94.3 | 17.10 | 7.0.37 / 8.0.26 | 11.4 | — |
| C0 | 2026-07-16 | 0.94.x-dev¹ | 15.18 | 7.0.34 / — | 11.4 | 8.14.3 |
| FTS vs ES | 2026-07-16 | 0.92.1 | — | — | — | 8.14.3 |
| Global-index A/B | 2026-07-16 | 0.92.1 | — | — | — | — |
| Sharded scatter | 2026-07-16 | 0.92.1 | — | — | — | — |

¹ C0's skaidb column was measured with the exact code released as
v0.94.x (both write-path fixes in), before the tag existed.

**Durability is matched across systems.** In each config a write is
acknowledged only after the same number of nodes have made it durable:

| Config | Nodes | A write is acked after… | skaidb | MongoDB | PostgreSQL | MariaDB |
|--------|:-----:|--------------------------|--------|---------|------------|---------|
| **C0** | 1 | local WAL/journal fsync | RF=1, `ONE` | single-member rs | no standbys | binlog only |
| **C1** | 2 | both nodes | `QUORUM` | `w:majority` | sync standby (`FIRST 1`) | semi-sync |
| **C2** | 2 | the primary only (async replica) | `ONE` | `w:1` | async (`''`) | semi-sync off |
| **C3** | 3 | all 3 nodes | `ALL` | `w:3` | `FIRST 2` sync standbys | — ¹ |
| **C4** | 3 | any 2 of 3 (quorum) | `QUORUM` | `w:majority` | `ANY 1` standby | semi-sync ¹ |

¹ MariaDB semi-sync acknowledges after the **first** replica responds and
has no "wait for N replicas" knob, so true all-3 durability isn't
expressible; its C3 row is the same semi-sync mode as C4 (≈ 2-of-3), a
single measurement marked `*`.

**Workloads** (throughput in **ops/sec**, higher is better):

- `write 1c` — single connection inserting unique keys (durable-write latency floor)
- `write 16c` — 16 connections inserting (concurrent write throughput)
- `read 16c` — 16 connections, point read by primary key over a 1,000-row table
- `mixed 16c` — 16 connections, 50/50 read/write

## C0 — 1 node, no replication (2026-07-16)

The floor underneath the replicated configs: one node, no peers, client
colocated (loopback). skaidb: `replication_factor = 1`, no seeds, both
read and write consistency `ONE`. PostgreSQL: `synchronous_standby_names`
cleared with standbys stopped (otherwise every write blocks forever on an
ack that can never come). MongoDB: reconfigured to a genuine
single-member replica set (`rs.reconfig {force:true}` — a 3-member set
cannot elect a primary once 2 members are stopped). MariaDB: semi-sync
off. **Elasticsearch ran on the same 1 vCPU / 512 MB spec** — its
default 1 GB heap doesn't fit, so it ran a **256 MB heap**, which idles
at ~94% of container RAM before any workload; a real data point about
ES's footprint at this hardware class, and the reason its numbers
reflect a system at its memory floor.

| Workload | skaidb | MongoDB 7 | PostgreSQL | MariaDB | Elasticsearch ¹ |
|----------|-------:|----------:|-----------:|--------:|-----------------:|
| write 1c  |  1,887 |    571 | **1,895** |    386 |    136 |
| write 16c |  3,427 |  1,123 | **3,828** |  2,127 |    278 |
| read 16c  | **10,643** |  1,684 |  4,696 |  4,796 |  1,709 |
| mixed 16c | **4,561** |    999 |  4,158 |  2,843 |    467 |

`¹` non-default 256 MB heap (see above).

skaidb leads reads (2.3× over PostgreSQL) and mixed, and is at
PostgreSQL parity on writes (−0.4% at 1c, −10% at 16c). Single-thread
point-read latency: skaidb p50 ≈ 110 µs vs PostgreSQL ≈ 198 µs, MariaDB
≈ 301 µs, MongoDB ≈ 689 µs on this loopback setup. Both of skaidb's
2026-07-16 write-path fixes are in these numbers — the root-cause record
for each (WAL extension fsync cost; standalone lock serialization) is in
the [notes](#root-caused-findings-2026-07-1617).

## C1–C4 — replicated configs (2026-07-17)

All five systems measured in one pass after upgrading everything (see the
versions table above), with the **corrected harness**: every client
connects and authenticates *before* the timed window opens (an earlier
same-day pass timed connection setup inside the window, which depressed
skaidb's 16-connection numbers 4-11× — the full root-cause record,
including the driver-side SCRAM fix it prompted, is in the
[notes](#root-caused-findings-2026-07-1617)). Reading the standings:

- **skaidb leads every cell of every config** — writes, reads, and mixed,
  at 1 and 16 connections. Largest margins on reads (2.2× over
  PostgreSQL at C4: 13,629 vs 6,153) and quorum writes (1.4×: 3,873 vs
  2,768); narrowest on async-replication writes (C2 write 1c: 967 vs
  846, inside the noise band).
- **skaidb's reads scale with members**: 9,384-9,529 at 2 nodes →
  13,108-13,629 at 3 (every node coordinates its share of clients
  against its full local copy). The other systems read from a single
  primary regardless of cluster size, and it shows.
- **Durability level barely moves skaidb's throughput** (C1 ≈ C2 ≈ C3 ≈
  C4 within noise at 16c) — the fsync is group-committed and the peer
  round-trip is pipelined, so stricter acks cost latency headroom, not
  throughput. MongoDB pays heavily for w:3 (write 1c drops ~4× from C2
  to C3); PostgreSQL is comparatively flat like skaidb.

### C1 — 2 nodes, writes wait for **both**

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  | **1,008** |    225 |    296 |    609 |    312 |
| write 16c | **3,903** |  1,092 |  1,428 |  2,870 |  1,450 |
| read 16c  | **9,384** |  2,264 |  2,230 |  5,487 |  2,952 |
| mixed 16c | **5,740** |  1,425 |  1,581 |  4,146 |  2,848 |

### C2 — 2 nodes, writes wait for the **primary only** (async replication)

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  | **967** |    749 |    735 |    846 |    332 |
| write 16c | **3,730** |  2,010 |  1,750 |  3,565 |  1,682 |
| read 16c  | **9,529** |  2,298 |  2,333 |  6,056 |  3,033 |
| mixed 16c | **5,524** |  1,641 |  1,996 |  4,463 |  2,506 |

### C3 — 3 nodes, writes wait for **all 3**

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB* |
|----------|-------:|----------:|----------:|-----------:|---------:|
| write 1c  | **717** |    156 |    161 |    577 |    269* |
| write 16c | **3,999** |    864 |    926 |  2,815 |  1,310* |
| read 16c  | **13,108** |  2,189 |  2,338 |  6,034 |  2,644* |
| mixed 16c | **5,564** |  1,255 |  1,222 |  3,919 |  2,271* |

`*` identical physical config as C4 (see note ¹ under Environment) — a
single measurement, not an independent second run.

### C4 — 3 nodes, writes wait for **2 of 3** (quorum)

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  | **734** |    221 |    242 |    592 |    257 |
| write 16c | **3,873** |  1,065 |  1,267 |  2,768 |  1,272 |
| read 16c  | **13,629** |  2,186 |  2,286 |  6,153 |  2,691 |
| mixed 16c | **6,500** |  1,380 |  1,452 |  4,148 |  2,176 |

### Memory footprint (process RSS, 2026-07-16 round)

Single-process RSS snapshots taken during/shortly after runs — a rough
order-of-magnitude comparison, not a precise idle-state measurement:

| | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|--|------:|----------:|----------:|-----------:|--------:|
| process RSS | ~36 MB | ~61 MB | ~70 MB | n/m¹ | ~41 MB |

¹ PostgreSQL's per-backend-process model makes single-process RSS
misleading (shared_buffers is shared memory); not measured rather than
publish a number known to understate it.

## Global-index routed probe — phase-4 A/B (v0.92.1, 2026-07-16)

RF=1 (genuinely sharded), 250k rows / 5,000 distinct indexed values, one
LOCAL and one GLOBAL index on identical twin tables, 100 equality probes
per round with the same seeded value sequence, interleaved so host noise
hits both arms equally.

**Correctness: exact in both rounds** — identical row counts from the
local-index scatter and the global-index routed probe at both
topologies. (This bench caught two real backfill bugs before any number
was trusted — batched drives and retry-or-abort readiness — both fixed
in v0.91.1/.2; a third operational finding, backfills stalling below
quorum during ring joins until the next repair pass re-drives them, is
recorded in GLOBAL_INDEXES.md.)

| Members | RF | local-index scatter (median / p95) | global-index routed probe (median / p95) |
|--------:|---:|----------------------------------:|------------------------------------------:|
| 2 | 1 | 11.5 / 28.2 ms | 11.3 / 32.3 ms |
| 3 | 1 | 3.3 / 4.5 ms | 2.9 / 4.0 ms |

Parity at 2 members (scatter costs exactly one extra peer RPC there); the
routed probe pulls ahead ~12% at 3 members — scatter fan-out cost grows
with member count, the probe's does not. Expected to widen at 5+ members
(untested; this fleet tops out at 3). The two rows' absolute latencies
aren't comparable to each other (different host-contention conditions).
At RF = full a global index buys nothing by design.

## Sharded scatter partials — fleet verification (v0.92.1, 2026-07-16)

3-node fleet, **RF = 2 over 3 members**, 100,000 deterministic synthetic
log documents (text `msg`, keyword `level`, long `bytes`).

- **Parity**: grouped per-level counts summing to exactly 100,000;
  global `COUNT`/`SUM`/`MIN`/`MAX`/`AVG` all exact.
- **Latency** (grouped count over `MATCH`, p50/p95 of 15 warm runs):
  partials **184.4 / 229.6 ms** vs forced row-fallback **8,166.3 /
  8,754.4 ms** — a **44.3×** p50 speedup, and the gap itself proves which
  path served each query.
- **Sorted top-k**: exact 10-row result in 203.6 ms.

Kill/rejoin and live-reshard resilience demos were not re-run this
round — re-run them before citing resilience claims from this document.

## Full-text search vs Elasticsearch (v0.92.1, 2026-07-16)

skaidb `SEARCH INDEX` vs Elasticsearch, single node each, both on
dedicated **2 vCPU / 2 GB** LXCs, driven from a third VLAN-local client.
**100,000-document synthetic corpus** (short prose, deterministic
generation, `id`/`title`/`body`) — not the original 280,595-doc Simple
English Wikipedia corpus (regenerating it is tracked in TODO.md). Both
engines: `standard` analyzer, 1 s refresh, per-batch durability (skaidb:
WAL fsync per statement; ES: translog fsync per bulk), 1 GB ES heap /
matching skaidb memtable budget.

|                       | skaidb 0.92.1 | Elasticsearch 8.14.3 |
|-----------------------|--------------:|----------------------:|
| ingest (docs/s)       |     **7,952** |                  4,733 |
| term p50 / p95 (ms)   |    **1.3 / 1.7** |            14.2 / 19.9 |
| AND p50 / p95 (ms)    |    **4.1 / 6.3** |            26.6 / 53.6 |
| OR p50 / p95 (ms)     |    **4.4 / 5.2** |            29.3 / 32.5 |
| phrase p50 / p95 (ms) |         26.2 / 26.8 |        **27.1 / 37.9** |
| NRT visibility (ms)   |         **43** |                    684 |
| process RSS (post-ingest) | **~291 MB** |               ~1,412 MB |

skaidb leads ingest (1.7×) and every query class except phrase, where the
two are within noise — a synthetic-corpus artifact (heavy phrase
repetition stresses phrase adjacency for both engines).

**Result-set parity** (top-10 id-set overlap): term 94.7% / and 99.0% /
or 97.0% / phrase 86.0% strict; **94.2% overall strict, 99.6%
tie-tolerant** (@10-in-15) — in line with the original natural-prose
corpus round (98.5%/99.8%); the synthetic corpus stresses BM25
tie-breaking harder.

The original round's 3-node cluster leg (ingest + scatter latency +
kill/rejoin) was not re-verified — re-run before citing cluster-scale FTS
claims.

## Reproducing

All load generators are in-tree. Client binaries/scripts get staged into
a VLAN-local container with `pct push` (direct SSH into containers is not
set up on this fleet); DB drivers come from Debian packages
(`python3-psycopg2 python3-pymysql python3-pymongo` — the containers have
no pip).

**The harness** (used verbatim for C0–C4):

```sh
# one suite = write 1c/16c, read 16c, mixed 16c, appended to $CSV
CSV=results.csv bench/run_suite.sh <label> <config> <client-prefix...>

# client prefixes:
#   skaidb   target/release/examples/bench <addr[,addr…]:7000> <user> <pass>
#            (musl build for the fleet: --target x86_64-unknown-linux-musl)
#   mongo    MONGO_RS=rs0 MONGO_W=majority python3 bench/clients/mongo_bench.py <seeds>
#   pg       PG_PASS=… python3 bench/clients/pg_bench.py <host>
#   maria    MARIA_PASS=… python3 bench/clients/maria_bench.py <host>
```

The skaidb client also runs standalone:
`bench <addr> <user> <pass> <write|read|mixed>[p] <ops> <threads> [preload]`
— `*p` modes use prepared statements, `preload` accepts `NxS` (rows ×
payload bytes), `READ_SPAN` env caps the read key range (hot-set reads).

**C0** — configure each system genuinely single-node, client on the same
container:

- skaidb: `seeds = []`, `replication_factor = 1`, both consistencies
  `ONE`; fresh `data_dir`.
- PostgreSQL: stop standbys AND `ALTER SYSTEM SET
  synchronous_standby_names = ''` + reload — with it set and standbys
  down, every write blocks indefinitely (not a timeout).
- MongoDB: `rs.reconfig(cfg, {force:true})` down to only the surviving
  member — a majority-less replica set has no primary and rejects writes.
- Elasticsearch on a 512 MB node: set
  `/etc/elasticsearch/jvm.options.d/heap.options` to `-Xms256m -Xmx256m`
  (the default 1 GB heap cannot start).

**C1–C4** — switch configs per the durability table, one system at a
time, and **verify the engaged state before trusting a number**:

- skaidb: rewrite `/etc/skaidb.toml` (seeds list, `replication_factor`,
  `default_write_consistency`), wipe the data dir, restart all members;
  verify `/status` shows the expected `members` and `write_consistency`.
  Keep `anti_entropy_interval_secs = 3600` in every config (the 60 s
  default reproduces a documented repair-storm failure mode).
- MongoDB: `rs.remove`/`rs.add` the third member for 2↔3 nodes; the ack
  level is client-side (`MONGO_W`). Verify `rs.status()` member count.
- PostgreSQL: `ALTER SYSTEM SET synchronous_standby_names = …` (its own
  `psql -c`, cannot share one with `pg_reload_conf()`); verify
  `pg_stat_replication.sync_state` shows `sync`/`quorum` as intended.
- MariaDB: `rpl_semi_sync_master_enabled` on the primary AND
  `rpl_semi_sync_slave_enabled` + IO-thread restart on replicas; verify
  `SHOW STATUS LIKE 'Rpl_semi_sync_master_clients'` > 0 — the master-side
  flag alone reports nothing when replicas never connected semi-sync
  (this silently degraded to async for this fleet's entire early
  history). Stop the third replica's container for 2-node configs.

**FTS vs ES**: `bench/clients/fts_bench.py` (corpus generation in
`fts_corpus.py` / `fts_logs_corpus.py`), identical queries against both
engines, parity checked by top-10 id-set overlap.

**Microbenchmarks** (single binaries, no fleet needed):

```sh
# layered point-read cost: parse-only vs full in-process execute
cargo run --release --example read_path_breakdown -p skaidb-engine -- [ops]
# layered durable-write cost (run on the REAL data disk, not tmpfs)
cargo run --release --example write_path_breakdown -p skaidb-engine -- [dir] [ops]
# raw fsync floor of a disk: growing file vs pre-allocated in-place
cargo run --release --example raw_fsync -p skaidb-storage -- [dir] [ops] [prealloc]
# full-Engine concurrent point-read throughput (cache contention)
cargo run --release --example cache_contention -p skaidb-storage -- <rows> <hot> <threads> <ops>
# secondary-index costs (see INDEX_BENCH.md)
cargo run --release --example index_bench -p skaidb-engine
```

## Performance engineering notes

*The durable audit record: what holds, root causes, measured dead ends,
methodology. Result tables above get replaced on re-runs; this section
only grows. Open `[perf]` work lives in [TODO.md](TODO.md).*

### What already holds

- Scans stream (k-way merge over memtable + SSTables, O(pages) memory) with
  early-stop for plain `LIMIT`; `scan_prefix` is range-bounded.
- Reads run under a shared lock (`&self`); writes group-commit one fsync per
  multi-row statement; replica fan-out is pipelined and batched
  (`ApplyBatch`), with the async tail drained and regrouped per peer.
- WAL segments are pre-allocated in 1 MiB grow-ahead chunks (`wal.rs`)
  instead of extending per append, so a durable single-row write's fsync
  is a data-only flush — measured 3× on write throughput on
  extension-costly storage (see root-caused findings below).
- Standalone (`Backend::Local`) statements fsync **after** releasing the
  exclusive Database lock, so concurrent sessions' commits coalesce in
  `WalSync::sync_through` (group commit) — measured 2.1× on concurrent
  write throughput (see below). The clustered path always did this.
- A plain `GROUP BY`/aggregate query (no `TOP k BY`, wildcard, join, or
  set op) decodes only the columns its filter, grouping, aggregates,
  `HAVING`, and `ORDER BY` can actually read — not every column of every
  matching row — across every gather path, local and clustered
  (`decode_document_projected`, `wal.rs`'s sibling in `codec.rs`; wired
  through `matching_rows_projected` on both `Database` and `Node`).
  Measured on a 488 MB synthetic table: RSS growth for an unfiltered
  `GROUP BY ... COUNT(*)` went from 239 MB to 1 MB (see root-caused
  findings below — this is the fix for agencik wishlist E-7, a real
  production OOM crash).
- Equi-joins hash-join; unfiltered `COUNT(*)` answers from key stats; `ORDER
  BY … LIMIT k` is a selection, not a full sort; per-table block cache +
  sharded read cache + bloom filters on the point-read path.
- SQL parses once per request; prepared statements skip the parse entirely;
  hot metrics are pre-registered atomics; framing layers reuse per-connection
  buffers and allocate nothing steady-state.
- Row-returning results can stream in ~256 KB chunks (`QueryStream`), and
  distributed full-table gathers + anti-entropy are paged (2,000 rows/page) —
  coordinator memory is O(pages in flight), not O(table). Raw time-series
  dumps are scan-metered (v0.91) so an unbounded range fails cleanly instead
  of growing until OOM; forward index-ordered walks stream (v0.91) instead
  of materializing the whole entry range up front.
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
- Repair digests skip a stamp scan entirely for unchanged tables (v0.88.1's
  versioned digest cache) and, when a scan is needed, never decompress
  value bytes (the `.stamps` sidecar) — a converged prod pass dropped from
  >240s to ~60s (measured post-v0.89.0 roll).

### Root-caused findings (2026-07-16/17)

The C0/C1-C4 work, plus a separate agencik-reported production crash,
produced six measured, code-verified findings:

- **WAL file extension dominated durable-write latency** (fixed,
  v0.93.0). On the bench fleet's storage an fsync after extending a file
  costs ~1.5–1.7 ms (filesystem journal metadata) vs ~500 µs overwriting
  already-allocated space in place — measured with a raw fsync probe
  containing no skaidb code (`raw_fsync.rs`), and matching PostgreSQL's
  write latency exactly (its 16 MiB WAL segments are recycled, never
  grown). skaidb's own SQL/dispatch cost on the write path is ~15 µs.
  Fix: 1 MiB grow-ahead chunks — chunked rather than PG-style whole
  segments because skaidb keeps one WAL per table/index, so 16 MiB each
  would idle-reserve gigabytes on many-table deployments. Required a
  replay correctness fix: `crc32(&[]) == 0`, so a zero-filled
  pre-allocated tail previously passed the checksum and then hard-failed
  decoding; a zero-length payload now ends replay cleanly. Measured:
  write 1c 565→1,859 (3.3×) standalone; write 1c 234→684-888 clustered.
- **Standalone server serialized the fsync under its exclusive lock**
  (fixed, v0.94.x). `Backend::Local` held `db.write()` across the
  statement's group-commit fsync, so 16 connections measured no faster
  than 1 (serial ~0.51 ms/op caps at ~1,950 ops/s; measured 1,608 flat) —
  cross-session group commit never fired because no two sessions could
  be in the sync section at once. Fix:
  `execute_session_statement_deferred` hands `(WalSync, WalCommit)` pairs
  out; the server syncs after dropping the lock (on error paths too).
  Measured: write 16c 1,608→3,427 (2.1×), mixed 16c 2,380→4,561; 1c and
  reads unchanged. The clustered path (`Node::replicate`) always fsynced
  outside the lock — only standalone deployments were affected.
- **Quorum reads cost ~8%, not the read gap.** `read_consistency = ONE`
  vs `QUORUM` measured ~8% throughput difference on the live 3-node
  fleet (properly isolated A/B, first-run-after-restart outlier
  discarded). The quorum read path (`Node::point_get`) pipelines the
  peer confirmation concurrently with the local read — not sequential.
  One flagged risk for future work: **read-repair is synchronous** — a
  stale replica blocks the client response on its repair instead of
  repairing in the background.
- **The published "16c coordinator bottleneck" was a harness artifact
  plus a real driver inefficiency** (both fixed, 2026-07-17). The
  original C1-C4 pass showed skaidb's 16-connection throughput stuck at
  its 1-connection level (~950-1,210) and this document attributed it to
  coordinator CPU — wrongly. Decomposition on a live 2-node cluster:
  Node layer alone (single member) 10,269 reads/s; peers at ONE/ONE
  6,765; QUORUM/QUORUM 7,293; client spread across both nodes 6,257 —
  none of it reproduced the collapse. The suite's exact shape (4,000
  ops / 16 threads) did: **the harness started its clock before the
  worker threads connected**, and each skaidb connection paid ~190 ms of
  setup — SCRAM PBKDF2 (15,000 scalar HMAC iterations) run **twice** per
  handshake (once for the client proof, once for the server-signature
  check), with nothing shared across connections. 16 handshakes ≈ 2.9 s
  of a 3.7 s window; identical per-op latency in fast and slow runs was
  the tell. Fixes: (1) harness — all clients (Rust and Python) now
  connect before a barrier and the clock starts at the barrier, with
  setup time reported separately; (2) driver — one PBKDF2 per handshake
  (`client_proof_salted`/`server_signature_salted` share the derived
  key) and a process-global `(password, salt, iterations)` →
  SaltedPassword cache, making reconnects/pool-refills ~free. Re-measured
  C1: read 16c 1,131 → 9,384; write 16c 924 → 3,903. The corrected
  matrix flipped every 16c standing. Residual truth in the old claim:
  quorum coordination does cost ~30-40% vs the bare Node layer
  (10,269 → 6,257-7,293 reads) — real, but nowhere near the artifact's
  magnitude.
- **skaidb's per-op code cost is microseconds; coordination and I/O are
  everything at this scale.** In-process, no network: SQL parse ≈ 0.54 µs,
  full point-read execute ≈ 1.19 µs (`read_path_breakdown.rs`) — 50-100×
  below even loopback wire numbers. And on a true 1-vCPU node, 16 client
  threads buy *nothing* over 1 for every system tested (pure
  context-switch tax); many-core scaling headroom (~720K reads/s at 16
  threads on a 32-core box) must never be quoted for the 1-vCPU
  deployment shape.
- **A plain `GROUP BY` over a wide-row table OOM-killed the node**
  (fixed; agencik wishlist E-7 — two-part fix, see below). Reported
  crash: `SELECT account, COUNT(*) FROM gmail_emails GROUP BY account`
  (183k rows, 1.9 GB — large `body_*` fields) ramped allocated memory
  1.02 GB → 4.03 GB in under 4 seconds and blew the 4 GB cgroup ceiling.
  **Part 1 — column-projected decode.** `matching_rows_ordered`/
  `matching_rows` decoded every matching row into a **full** `Document`
  before any grouping happened — `GROUP BY` explicitly disables the
  fetch-limit push-down other queries get, and the only existing guard
  (`scan_row_budget`, default 250k) counts rows, not bytes: 183k rows
  sails under budget while the gather still allocates every column of
  every row. Fix: `group_by_projection_columns` (exec.rs) statically
  determines every column a `GROUP BY`/aggregate query can possibly
  read — filter, `group_by`, select items (including inside aggregates:
  `SUM(amount)` needs `amount`), `HAVING`, `ORDER BY`, and any
  non-aggregated select item (read from the group's representative row
  under MySQL-style "any value" semantics) — and a new storage
  primitive, `Value::decode_document_projected` (`codec.rs`), decodes
  only those top-level fields, walking past everything else via a
  `skip_value` companion to `decode_value` (O(1) past a large unwanted
  `String`/`Bytes` field: read its length, skip the bytes, no
  allocation). Standalone-path measurement (488 MB synthetic table):
  RSS growth for the crash query went from 239 MB to 1 MB.
  **This alone did not fix the reported crash.** The first release
  (v0.95.1) validated only against the standalone `Session`/`Database`
  API, which never touches the clustered gather path production
  actually uses; a live retest on the real 3-node cluster (same
  methodology as the C0/C1-C4 work — real binary, real service RSS, not
  a synthetic harness) showed the crash still reproduced. Root cause:
  the clustered "no predicate: gather the whole table" path ran through
  `Node::cluster_scan`, a function that accumulated **every** row's raw
  bytes for the **entire table** into one `BTreeMap` before any decode
  step ran at all — column-projecting the final decode does nothing
  when the thing being column-pruned was never the bottleneck. This is
  the same failure mode already root-caused once before and tracked in
  `cluster_scan_collect`'s own doc comment ("the old whole-table
  `cluster_scan` here materialized every row on the coordinator — 4.6
  GB allocated, OOM-killed two production nodes, 2026-07-13").
  **Part 2 — retire `cluster_scan`, shrink the page.** `cluster_scan`
  is deleted outright; its two callers now go through
  `cluster_scan_collect`, the already-bounded sibling used by every
  other broad gather (LIMIT pushdown, candidate resolution, anti-entropy)
  — a page-at-a-time pull per source with a sliding "seal" frontier that
  evicts finalized rows every round, so the coordinator never holds more
  than roughly one page window per source. But a page is a fixed **row**
  count (`SCAN_PAGE_ROWS`, 2,000) with no byte cap, so a wide-row table
  still fully materializes a 2,000-row raw-byte page per source, per
  round, before any pruning runs — pruning can't shrink a buffer that's
  already allocated. Fix: when a projection is active, both the local
  scan (`local_scan_versioned_page`) and the peer `ScanPage` RPC request
  a much smaller page (`PROJECTED_SCAN_PAGE_ROWS`, 100 rows) — no
  wire-protocol change, `Request::ScanPage.limit` already existed as a
  runtime value. More, smaller round trips instead of fewer, larger
  ones. **Measured on the live 3-node p225 bench fleet** (real
  `skaidb` service, `VmRSS` from `/proc/<pid>/status`, not the
  standalone API): an 8,000-row/62 MB wide table went from ~59-74 MB
  growth (with `cluster_scan` retired but page size unchanged) to
  **~11.2 MB growth**, stable across repeated trials and settling back
  to baseline within seconds. Tripling the table to 24,000 rows/187 MB
  grew RSS only ~26.6 MB (sub-linear vs. table size), the expected
  signature of page-bounded rather than table-bounded memory. Correct
  results verified at both scales. Deliberately does **not** apply to
  `TOP k BY` (returns whole rows via `select_group_topk`, a different
  consumer with unrestricted column needs), wildcards, joins, or set
  operations — none of those gather shapes were audited for it. 197
  engine tests + 86 cluster tests pass, including new targeted coverage:
  an exhaustive (2⁹ field-subset) property test that the projected
  decode always matches a full decode on every possible wanted-set, and
  end-to-end `GROUP BY`/`HAVING`/`ORDER BY`/filtered/nested-path/
  no-group-by queries against a wide table compared byte-for-byte
  against the same queries against a narrow table with no pruning to
  do. **Lesson recorded for future OOM-shaped fixes:** validate against
  the actual clustered `Coordinator`/`cluster_scan*` path on a live
  cluster before declaring a memory fix complete — the standalone
  `Session`/`Database` API does not exercise it and can show a
  misleadingly clean result.

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
- **`ReadCache`/`BlockCache` shard widening** (tried 2026-07-16): widened
  `ReadCache` 16→64 shards and gave `BlockCache` the same sharded design
  (previously one mutex). Measured null on both a direct lock-contention
  probe and a full-Engine A/B — the fleet's bottleneck is per-node
  coordination CPU, not cache lock contention. Kept as a cleanliness
  change (PG uses the same per-partition rationale), not a perf one.

### Methodology (hard-won, follow it)

- Scale the benchmark to the feature's target size — the 1,000-row suite hid
  two OOM bugs that 1M rows exposed immediately.
- Only trust alternating same-day A/B runs; leg ordering alone produces
  double-digit artifacts on a shared host.
- If every system lands in the same band on a fleet leg, suspect a shared
  environmental floor and isolate on a single node over loopback before
  concluding a change does nothing (or something).
- **Run the client inside the VLAN, never from the workstation** (2026-07-16):
  workstation→VLAN RTT measured 17–40 ms on this network, enough to
  dominate every sub-10ms latency this fleet measures. Stage the client
  binary/venv on a spare container via `pct push`.
- **A fresh bench-fleet config is not automatically a *safe* config** —
  it must carry every production hardening lesson explicitly (a round's
  `anti_entropy_interval_secs` omission reproduced a documented repair-storm
  failure mode). Diff a new fleet config against the current production
  template before trusting numbers from it.
- **A silently-wrong config produces a silently-wrong number, not an error**
  — one round caught PostgreSQL running the previous sync mode (a
  transaction-block error was visible, but easy to miss in scrollback) and
  MariaDB "semi-sync" actually running fully async for the fleet's entire
  history (no error at all — the slave-side flag being off degrades
  silently). Verify the actual engaged state
  (`SHOW STATUS LIKE 'Rpl_semi_sync_master_clients'`, `SHOW
  synchronous_standby_names`, `SHOW INDEXES` for skaidb) before trusting a
  config-switch, not just the command that was supposed to set it.
- The first run after a fleet restart is often an outlier — discard and
  re-run it before recording anything.
- **Never time connection setup inside a throughput window.** Clients
  must connect + authenticate before a barrier and the clock starts at
  the barrier — N expensive handshakes (SCRAM PBKDF2) inside a short
  window depressed published 16-connection numbers 4-11× while per-op
  latency looked perfectly normal. Identical p50s between a fast and a
  slow run are the tell that wall-clock, not ops, is being measured.
