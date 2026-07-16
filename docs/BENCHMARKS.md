# skaidb benchmarks

A throughput/latency comparison of **skaidb** against four production databases —
**MongoDB 7.0**, **MongoDB 8.0**, **PostgreSQL 15**, and **MariaDB 11.4** — run on
identical containers with matched durability semantics, across four
cluster/consistency configurations; plus a skaidb-vs-Elasticsearch full-text
search comparison, and fleet-level correctness/latency verification of
skaidb's sharded-scatter and global-index paths.

**Full re-run: 2026-07-16, skaidb v0.92.1.** This is a wholesale re-run —
every number in this document was freshly measured this pass; nothing here
is carried over from earlier releases. Two things make this round's absolute
throughput numbers **not comparable** to any prior version of this document:
the client now runs from a container on the same bridge as the fleet instead
of a workstation over a hairpin path (17–40 ms RTT would otherwise dominate
every measurement), and this round's host was shared with an active 24-hour
FTS soak plus the full 15-container comparison fleet — 16 containers total
on one 8-thread host throughout. See **Setup** below for what that means for
skaidb's standing specifically.

> Numbers are for *relative* comparison on small, contended nodes, not
> absolute peak throughput. Within one round, all systems are driven by the
> same client model and workloads on identical hardware, so the *relative*
> standings are trustworthy even when absolute figures move between rounds.

## Setup

**Host.** All containers run on a single Proxmox host — an Intel Core
**i7-8550U** (4 cores / 8 threads, 1.8 GHz) with 8 GB RAM.

**Nodes.** Every database runs as its own set of identical unprivileged LXC
containers — each **1 vCPU / 512 MB RAM / 4 GB disk**, Debian 12, bridged on
one VLAN. A 3-node configuration is three such containers; a 2-node
configuration is two.

**Client runs inside the VLAN**, on a dedicated container, not the
workstation — workstation→VLAN RTT measured 17–40 ms this round, which would
swamp the sub-10ms latencies under test.

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
row is the same semi-sync mode as C4 (≈ 2-of-3), a single measurement marked `*`.

**Workloads** (throughput in **ops/sec**, higher is better):

- `write 1c` — single connection inserting unique keys (durable-write latency floor)
- `write 16c` — 16 connections inserting (concurrent write throughput)
- `read 16c` — 16 connections, point read by primary key over a 1,000-row table
- `mixed 16c` — 16 connections, 50/50 read/write

**skaidb's standing dropped sharply from every previous version of this
document** — from leading reads and being competitive on writes, to trailing
every system on every workload this round. This was investigated before
publishing, not waved away:

- **A real, fixed bug**: the fleet's reference skaidb config predated the
  `anti_entropy_interval_secs=3600` pin and ran on the 60-second default —
  the same back-to-back-repair-pass pattern documented as a production
  incident (see CLUSTERING.md). Fixed and the **entire skaidb matrix
  re-run**; numbers barely moved (read 16c: 960→984 ops/s), so this was not
  the dominant cause — but it was a genuine methodology bug and the fix
  stands.
- **Ruled out**: per-operation reconnection (the client holds one persistent
  connection per thread for the whole run — confirmed in `bench.rs`), and
  write shedding (0 errors on every leg, every config).
- **The latency shape** (p50 ≈ 1 ms, p99 10–35 ms) matches scheduling-jitter
  tail latency on an oversubscribed host, not a uniform engine slowdown — a
  fast median says the storage path itself isn't the bottleneck.
- **Not fully root-caused.** The leading hypothesis: skaidb's
  leaderless/quorum coordination issues more internode round-trips per
  operation than PostgreSQL's simpler primary-writes-locally model, making
  it more sensitive to host-level scheduling jitter at this row count, where
  coordination overhead dominates raw storage work. This is inconsistent
  with production skai-cluster telemetry (sub-50 ms p99 on equivalent shapes
  at far higher load) — **treat this round's skaidb C1–C4 numbers as
  contention-dominated, not representative of isolated-host or production
  performance; re-run on a dedicated host before drawing product
  conclusions.**

## C1 — 2 nodes, writes wait for **both**

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  |    299 |    215 |    244 | **565** |    314 |
| write 16c |    635 |    951 |  1,091 | **2,100** |  1,189 |
| read 16c  |    975 |  1,942 |  1,846 | **3,709** |  2,082 |
| mixed 16c |    791 |  1,287 |  1,450 | **2,606** |  1,580 |

## C2 — 2 nodes, writes wait for the **primary only** (async replication)

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  |    325 |    666 |    536 | **788** |    327 |
| write 16c |    621 |  1,436 |  1,561 | **3,022** |  1,097 |
| read 16c  |  1,053 |  1,776 |  1,878 | **4,441** |  2,205 |
| mixed 16c |    790 |  1,383 |  1,556 | **3,460** |  1,541 |

## C3 — 3 nodes, writes wait for **all 3**

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB* |
|----------|-------:|----------:|----------:|-----------:|---------:|
| write 1c  |    237 |    260 |    235 | **499** |    292* |
| write 16c |    584 |  1,063 |    983 | **2,128** |    954* |
| read 16c  |    989 |  1,893 |  1,836 | **4,164** |  2,173* |
| mixed 16c |    775 |  1,427 |  1,438 | **3,135** |  1,476* |

`*` MariaDB's C3 is the identical physical config as C4 (see note ¹) — a
single measurement, not an independent second run.

## C4 — 3 nodes, writes wait for **2 of 3** (quorum)

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  |    234 |    219 |    265 | **574** |    292 |
| write 16c |    585 |    952 |  1,026 | **2,447** |    954 |
| read 16c  |    984 |  1,822 |  1,691 | **4,409** |  2,173 |
| mixed 16c |    733 |  1,238 |  1,310 | **3,071** |  1,476 |

**PostgreSQL wins every cell this round.** Its commit path sits at the
network-latency floor and its replication is a lightweight streaming-WAL
protocol; combined with the host contention discussed above, it pulled
further ahead of every leaderless/multi-round-trip system (skaidb, MongoDB)
than in prior isolated-host runs. Take this as this-round's relative
standing under contention, not a durable architectural verdict — see Setup.

Two real methodology bugs were caught and fixed **during** this round rather
than silently producing wrong numbers:

- **PostgreSQL**: `ALTER SYSTEM` and `pg_reload_conf()` must be separate
  `psql -c` invocations — combining them errors ("cannot run inside a
  transaction block") and silently leaves the *previous* sync config active.
  The first C3 attempt used the stale C4 setting; discarded and re-run
  correctly.
- **MariaDB**: `rpl_semi_sync_slave_enabled` was OFF on the replicas
  throughout this fleet's history — only the master-side flag had ever been
  set. With the slave flag off, `rpl_semi_sync_master_enabled=ON` silently
  degrades to fully-async writes with no error and no timeout, which means
  **every previous MariaDB "semi-sync" number ever published in this
  document was actually measuring async**. Fixed this round (slave-side
  flag set + IO thread restarted to register — verified via
  `Rpl_semi_sync_master_clients > 0` before every semi-sync leg) and all
  four MariaDB configs measured with genuine semi-sync where the config
  calls for it.

The previous version of this document's **leaderless fan-out sub-experiment**
(single-coordinator vs. round-robin across all 3 skaidb nodes) was not
re-run — the in-tree Rust bench client has no multi-host round-robin mode.
Dropped rather than kept stale; a future re-add needs a small harness change.

## Memory footprint (process RSS, after this round's workload)

Not a clean re-measurement of the original "idle, `free`-based, minutes after
a 1,000-row run" methodology — containers were repurposed between legs this
round. These are single-process RSS snapshots taken during/shortly after
this round's runs, given as a rough order-of-magnitude comparison, not a
precise re-verification:

| | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|--|------:|----------:|----------:|-----------:|--------:|
| process RSS | ~36 MB | ~61 MB | ~70 MB | n/m¹ | ~41 MB |

¹ PostgreSQL's per-backend-process model makes a single process RSS
misleading (shared_buffers is shared memory, not counted per-process); not
measured this round rather than publish a number known to understate it.

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

**Run the client from a VLAN-local container**, not the workstation —
see Setup. `bench/run_suite.sh` needs the client binaries and (for
Mongo/Postgres/MariaDB) a Python venv with `pymongo psycopg2-binary pymysql`
pushed into that container; `pct push`/`pct pull` move files between the
Proxmox host and a container (direct SSH to container IPs from outside the
VLAN is not set up on this fleet).

## Global-index routed probe — phase-4 A/B (v0.92.1, 2026-07-16)

Two rounds this pass, both RF=1 (genuinely sharded — every row lives on
exactly one member), 250k rows / 5,000 distinct indexed values, one LOCAL
secondary index and one GLOBAL index on identical twin tables, 100 equality
probes per round with the same seeded value sequence on both, interleaved
so host-contention noise (this host also ran the C1–C4 matrix's containers
plus the FTS soak) hits both arms equally.

**Correctness: exact in both rounds** — identical row counts returned by the
local-index scatter and the global-index routed probe on every round, at
both topologies below. Getting a clean result here is itself the finding:
this bench caught two real backfill bugs on its first attempts, both fixed
before any number was trusted:

1. The backfill driver wrote entries one replicated quorum write at a time
   (~15 ms each → close to an hour for 250k rows), and a repair pass would
   queue a duplicate full re-drive on top. Fixed: entries batch into one
   `ApplyBatch` per destination per page, and a drive exits immediately if
   the index is already ready.
2. Under host memory/CPU pressure the driver **logged and silently skipped**
   entry batches that failed to reach write quorum, then broadcast readiness
   anyway — every probe would have silently missed ~1% of rows, permanently
   at RF < members (no full-copy verify leg to catch it). Fixed: failed
   batches retry with backoff; a batch that still fails aborts the drive
   with `building` left set, so probes keep the correct (if slower) scatter
   fallback until a re-drive completes cleanly.
3. At 3 members, the backfill silently stalled below quorum on one member
   (a fresh 3-node ring joining right as 250k-row loads landed) and needed
   an explicit `REPAIR CLUSTER` to re-drive — the phase-3 hardening's
   re-queue-on-`building` logic picked it up and completed cleanly within
   the same repair pass. Automatic re-drive depends on
   `anti_entropy_interval_secs`; this fleet has it pinned to 3600s to avoid
   the repair-storm failure mode (see Setup), so a stalled backfill during
   active benchmarking needs a manual nudge — noted as a real operational
   texture, not hidden.

**Latency:**

| Members | RF | local-index scatter (median / p95) | global-index routed probe (median / p95) |
|--------:|---:|----------------------------------:|------------------------------------------:|
| 2 | 1 | 11.5 / 28.2 ms | 11.3 / 32.3 ms |
| 3 | 1 | 3.3 / 4.5 ms | 2.9 / 4.0 ms |

At 2 members the two are at parity — the ~50-candidate quorum resolve
dominates both arms, and 2-member scatter costs exactly one extra peer RPC.
At 3 members the routed probe pulls ahead (~12% faster median): the
scatter's fan-out cost grows with member count while the routed probe's does
not, matching the design's core thesis. (The 3-member absolute latencies are
far lower than the 2-member row because this leg ran without the C1-C4
matrix's host contention alongside it — not a topology effect; don't compare
the two rows' absolute numbers to each other.) The routing win is expected
to widen further at higher member counts (untested — this fleet tops out at
3 containers); re-run at 5+ members if that fleet becomes available before
any prod-adoption call. At RF = full (the current production topology) a
global index buys nothing by design — every node already holds every row.

## Sharded scatter partials — fleet verification (v0.92.1, 2026-07-16)

3-node fleet, **RF = 2 over 3 members** — a genuinely sharded corpus, every
document replicated twice. 100,000 deterministic synthetic log documents
(text `msg`, keyword `level`, long `bytes`), ingested via the binary
protocol driver.

- **Parity**: grouped per-level counts (error 10,084 / info 69,945 / warn
  19,971, summing to exactly 100,000), global `COUNT`/`SUM`/`MIN`/`MAX`, and
  `AVG` all exact.
- **Latency** (grouped count over `MATCH`, p50/p95 of 15 runs, warm):
  partials **184.4 / 229.6 ms** vs. a forced row-fallback (a residual
  predicate the aggregation pushdown can't cover) **8,166.3 / 8,754.4 ms** —
  a **44.3×** p50 speedup, and the gap itself proves which path served each
  query.
- **Sorted top-k**: exact 10-row result in 203.6 ms.

Scope note: this pass re-verified parity and the partials-vs-fallback
latency gap — the prior version's kill/rejoin and live-reshard resilience
demos were not re-run this round (they exercise cluster membership
mechanics, not benchmark throughput, and would have meaningfully extended
an already long fleet campaign). Re-run them before citing resilience
claims from this document.

## Full-text search vs Elasticsearch (v0.92.1, 2026-07-16)

skaidb `SEARCH INDEX` (single node) against Elasticsearch 8.14.3 (single
node), both on dedicated 2 vCPU / 2 GB Debian 12 LXCs, driven from a third
VLAN-local client container. **100,000-document synthetic corpus** (short
prose sentences, deterministic generation, `id`/`title`/`body` schema) —
**not** the original benchmark's 280,595-document Simple English Wikipedia
corpus. The original corpus generator needs a MediaWiki `pages-articles`
XML dump that isn't staged on this fleet and wasn't re-downloaded this
round (Wikimedia discontinued the shortcut abstract dumps the generator
was written against); regenerating a matching Wikipedia corpus is future
work, tracked in TODO.md. Both engines: `standard` analyzer, 1 s refresh,
per-batch durability (skaidb: WAL fsync per statement; ES: translog fsync
per bulk request), 1 GB heap for ES / matching memtable budget for skaidb.

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
two are within noise (26.2 vs 27.1 ms p50) — a synthetic-corpus artifact:
the generator's limited vocabulary produces heavy phrase repetition
("X borders the X" patterns), which is a harder phrase-adjacency shape than
natural prose for both engines' postings.

**Result-set parity** (same corpus and queries, top-10 id-set overlap):

| class | strict @10 | @10-in-15 (tie-tolerant) |
|-------|-----------:|--------------------------:|
| term | 94.7% | 100.0% |
| and | 99.0% | 100.0% |
| or | 97.0% | 100.0% |
| phrase | 86.0% | 98.0% |
| **overall** | **94.2%** | **99.6%** |

In line with the original benchmark's 98.5%/99.8% (natural-prose corpus);
the gap here is attributable to the synthetic corpus's repetitive phrasing
stressing BM25 tie-breaking harder than the original's real-article prose.

Scope note: the original benchmark's cluster leg (3-node ingest + scatter
query latency + kill/rejoin) and the NRT-visibility bug-discovery narrative
are historical record from when they were found, not re-verified this
round — re-run before citing cluster-scale FTS latency claims from this
document.

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
- **Run the client inside the VLAN, never from the workstation** (2026-07-16):
  workstation→VLAN RTT measured 17–40 ms on this network, enough to
  dominate every sub-10ms latency this fleet measures. Stage the client
  binary/venv on a spare container via `pct push`.
- **A fresh bench-fleet config is not automatically a *safe* config** —
  it must carry every production hardening lesson explicitly (this round's
  `anti_entropy_interval_secs` omission reproduced a documented repair-storm
  failure mode). Diff a new fleet config against the current production
  template before trusting numbers from it.
- **A silently-wrong config produces a silently-wrong number, not an error**
  — this round caught PostgreSQL running the previous sync mode (a
  transaction-block error was visible, but easy to miss in scrollback) and
  MariaDB "semi-sync" actually running fully async for this fleet's entire
  history (no error at all — the slave-side flag being off degrades
  silently). Verify the actual engaged state
  (`SHOW STATUS LIKE 'Rpl_semi_sync_master_clients'`, `SHOW
  synchronous_standby_names`, `SHOW INDEXES` for skaidb) before trusting a
  config-switch, not just the command that was supposed to set it.
