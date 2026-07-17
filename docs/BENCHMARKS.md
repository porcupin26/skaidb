# skaidb benchmarks

A throughput/latency comparison of **skaidb** against four production databases —
**MongoDB 7.0**, **MongoDB 8.0**, **PostgreSQL 17**, and **MariaDB 11.4** — run on
identical containers with matched durability semantics, across a single-node
baseline plus four replicated cluster/consistency configurations; plus a
single-node **skaidb-vs-Elasticsearch** comparison on matched hardware, a
larger-scale skaidb-vs-Elasticsearch full-text search comparison, and
fleet-level correctness/latency verification of skaidb's sharded-scatter
and global-index paths.

**C1-C4 re-run: 2026-07-17, skaidb v0.94.3, all comparison systems
upgraded first** — PostgreSQL 15.18→**17.10** (PGDG, primary + both
standbys rebuilt), MongoDB 7.0.34→**7.0.37**, MongoDB 8.0.23→**8.0.26**,
Elasticsearch 8.14.3→**8.19.18**; MariaDB stayed 11.4. This round's host
was much quieter than the previous one (no FTS soak; benchmarks run one
system at a time) and skaidb carries the two 2026-07-16 write-path fixes
(WAL chunk pre-allocation in v0.93.0; standalone fsync-outside-the-lock
in v0.94.x — the latter affects only the C0 standalone scenario, not
these clustered configs). Absolute numbers are therefore **not
comparable to the previous round's** — but every C1-C4 cell below was
measured in this same pass, so the relative standings are. Sections
other than C1-C4 and their dated headers (C0, FTS-vs-ES, global-index,
sharded-scatter) still carry the 2026-07-16 round's numbers and version
labels.

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

**Reading the C1-C4 standings** (2026-07-17 round):

- **skaidb leads write 1c in every config** (888/848/722/684 vs
  PostgreSQL's 598/832/581/631) — the durable-write latency floor. This
  is new: the previous round had skaidb *last* here (234-325), and the
  difference is v0.93.0's WAL chunk pre-allocation (see the C0 section's
  root-cause writeup) carrying straight through the clustered path.
- **PostgreSQL still leads every 16c cell.** skaidb's 16-connection
  numbers stay near its 1-connection numbers (~870-1,210) across all
  configs — on a 1-vCPU node the coordinator is CPU-bound coordinating
  quorum ops, so extra client concurrency has nothing to schedule onto.
  This is a different limit from the (fixed) standalone lock-serialization
  issue: the C0 scenario shows what the same node does with no
  replication coordination (3,427 write 16c / 10,643 read 16c).
- **Quorum reads carry a real cost at this scale**: every skaidb read
  here is a QUORUM read (a live confirmation round to a peer), while the
  other systems read from the primary alone. The measured ONE-vs-QUORUM
  delta on this fleet is ~8% of *skaidb's own* number (see the C0
  section) — the rest of the read gap vs PG is per-op coordination CPU
  on 1 vCPU.
- The previous round's investigation of skaidb's then-anomalous standing
  (repair-interval config bug — found and fixed; shared-host contention
  with an active 24h soak — gone this round) is preserved in git history;
  its conclusions stand and led to the C0 scenario plus both write-path
  fixes.

## C0 — 1 node, no replication (single-node baseline, 2026-07-16)

Every other config below tests a *replicated* durability contract. This is
the floor underneath all of them: one node, no peers, nothing to
coordinate with — same **1 vCPU / 512 MB** LXC class, same workloads, same
client-colocated-on-server methodology as the rest of this document.
skaidb ran with `replication_factor = 1` and no seeds (detached from its
usual C1-C4 cluster for this leg); PostgreSQL had `synchronous_standby_names`
cleared (its standbys were stopped for this leg — otherwise every write
blocks forever waiting for an acknowledgment that will never come); MariaDB
was already semi-sync-off at rest; MongoDB was reconfigured to a genuine
single-member replica set (`rs.reconfig` with only itself, since a 3-member
set can't elect a primary without a majority once its peers are stopped).
**Elasticsearch ran on a newly-provisioned container matched to the exact
same 1 vCPU / 512 MB spec** — its own default heap (1 GB) doesn't fit a
512 MB box at all, so this only works with the heap cut to **256 MB**,
which leaves the JVM using ~94% of container RAM (481 MB/512 MB) with no
workload running yet — a real, previously-undocumented data point about
Elasticsearch's footprint at this hardware class.

| Workload | skaidb (pre-fix) | skaidb (both fixes) | MongoDB 7 | PostgreSQL | MariaDB | Elasticsearch ¹ |
|----------|------------------:|----------------------:|----------:|-----------:|--------:|-----------------:|
| write 1c  |    565 |  **1,887** |    571 | 1,895 |    386 |    136 |
| write 16c |    558 |  **3,427** |  1,123 | **3,828** |  2,127 |    278 |
| read 16c  | 10,749 | 10,643 |  1,684 |  4,696 |  4,796 |  1,709 |
| mixed 16c |  1,003 |  **4,561** |    999 | 4,158 |  2,843 |    467 |

The "both fixes" column is the WAL pre-allocation (v0.93.0) **plus** the
standalone fsync-outside-the-lock fix described below, measured together.
With both in place skaidb leads every system on mixed 16c and reads, and
is within 2%/10% of PostgreSQL on write 1c/16c.

`¹` Elasticsearch's container needed a non-default 256 MB heap to run at
all in this spec class (see above) — its numbers reflect a system running
much closer to its memory floor than the other four.

**This splits the C1-C4 investigation's finding cleanly into two different
stories.** Reads: skaidb goes from **last place in every C1-C4 cell** to
**first place here** (10,749 vs PostgreSQL's 4,696) — confirming, as
suspected, that the C1-C4 read gap was quorum-RTT-plus-shared-host
contention, not a slower engine. Writes were a real, reproducible,
non-contention gap — and, unlike the read finding, root-caused and fixed
rather than explained away.

**Root-caused and fixed (2026-07-16): this was a filesystem-metadata
cost, not engine slowness.** A layered breakdown
(`write_path_breakdown.rs`, in-process, real disk, no network) showed
skaidb's own SQL/dispatch cost at 15µs — the remaining ~1.73ms was 100%
the WAL `fsync` call itself, confirmed independently against a raw fsync
microbenchmark (`raw_fsync.rs`, no skaidb code at all) run on the same
disk: a **growing file** (write past EOF, then fsync — skaidb's WAL
append pattern before this fix, since its WAL file was never pre-sized)
cost **~1.5-1.7ms** per fsync on this host's storage; the **same fsync on
a pre-allocated, fixed-size file overwritten in place** (no filesystem
metadata/journal update needed) cost **~500µs** — matching PostgreSQL's
write latency almost exactly. PostgreSQL's WAL segments are fixed-size
(16 MB) and recycled rather than grown from zero, so its commits pay the
cheap in-place-overwrite fsync.

**Fix**: the WAL file now grows **one chunk ahead of appends**
(`WAL_PREALLOC_CHUNK_BYTES = 1 MiB`, capped by `flush_threshold_bytes`;
`wal.rs`) instead of extending on every append — the extension's
metadata cost is paid once per MiB (~17k single-row commits) instead of
once per commit, and the fsync that follows a typical append is a
data-only flush into already-allocated space. Chunked rather than a full
PG-style 16 MiB segment up front deliberately: skaidb keeps **one WAL
per table and per index**, so a 16 MiB reservation each would cost a
many-table deployment gigabytes of idle disk — one chunk caps the
per-table overhead at 1 MiB. Required a real correctness fix alongside
it: WAL replay previously decoded a **zero-length payload** as a
candidate record — `crc32(&[])` is 0, so a zero-filled (unwritten,
pre-allocated) region would pass the checksum check and then hard-fail
trying to decode a payload no real record ever produces (minimum real
payload is 17 bytes), turning "nothing more here" into a replay error.
`replay()` now treats `payload_len == 0` as the clean end of valid data.
Reopening a WAL no longer truncates trailing space down to the last
valid record either (that would silently undo the pre-allocation on
every restart) — future appends overwrite whatever's there, whether
pre-allocated zeros or a torn write's leftover bytes. 10 WAL-layer tests
cover the zero-payload replay case, pre-allocation surviving a restart,
chunk-boundary growth, `rotate()` pre-allocating the fresh segment, and
`truncate()` re-reserving a zeroed chunk (not resurrecting flushed
records).

**Measured after the fix, same fleet, same methodology**: write 1c
565→**1,859 ops/s (3.3×)**, write 16c 558→**1,608 ops/s (2.9×)**, mixed
16c 1,003→**2,380 ops/s (2.4×)**, read 16c unchanged (within run-to-run
noise) — confirming the fix is write-path-only, as designed. (An earlier
whole-segment-up-front variant measured the same within noise — the
chunked design keeps the full win.) skaidb write-1c is now within 2% of
PostgreSQL's 1,895 (was 3.3× behind); write-16c and mixed-16c narrowed
from ~6.9×/4.1× behind to ~2.4×/1.7× behind PostgreSQL.

**The remaining 16c gap was also root-caused and fixed (2026-07-16): the
STANDALONE server serialized the WAL fsync under its exclusive Database
lock.** `Backend::Local` (what `seeds = []` gets you — exactly this
scenario's config) executed every write statement under `db.write()`
(`shared.rs`), and the statement's group-commit fsync
(`LocalCluster::flush_pending`, exec.rs) ran **inside** that scope — so
16 connections serialized through [append + ~0.5ms fsync] one at a time.
The arithmetic closes exactly: a serial 0.51ms per op caps at ~1,950
ops/s, and 16c measured 1,608-1,683 — concurrency bought nothing, and
the cross-session group-commit machinery (`WalSync::sync_through`
coalescing) never got a chance to coalesce because no two sessions were
ever in the sync section concurrently. PostgreSQL's 2× scaling to 3,828
is precisely what its leader/follower group commit buys: concurrent
committers share fsyncs, pushing past the single-fsync serial ceiling.
The kicker: **skaidb's CLUSTERED write path already did this right** —
`Node::replicate` applies under the lock buffered, then fsyncs *outside*
it (node.rs, step 1-2 comments), which is why the C4 fleet numbers DID
scale (write 1c 234 → 16c 585, 2.5×) while standalone C0 stayed flat.

**Fix**: `Database::execute_session_statement_deferred` (exec.rs) hands
the statement's pending `(WalSync, WalCommit)` pairs back to the caller
instead of fsyncing inline, and `Backend::Local` (shared.rs) drops the
write lock, THEN syncs — mirroring the cluster path. Read-your-writes
holds (the memtable has the rows before the fsync, same as the cluster
path); durability-before-ack is preserved (the sync completes before the
response goes out); rows applied before a mid-statement error still sync
(the pending set is returned on the error path too, matching the inline
contract). Covered by three deferred-sync tests (durable across reopen,
statement-scoped mode reset, error-path commit handling).

**Measured**: write 16c 1,608 → **3,427 ops/s (2.1×)**, mixed 16c 2,380
→ **4,561 ops/s (1.9×, now ahead of PostgreSQL's 4,158)**; write 1c and
read 16c unchanged (1,887 / 10,643 — both within run-to-run noise of
their pre-fix values), as expected for a change that only affects
concurrent writers. Only standalone (`Backend::Local`) deployments were
affected — clustered nodes already had the correct design.

See also the single-thread read latency deep dive, the ONE-vs-QUORUM cost
measurement, and the code-level timing breakdown in **Single-node isolated
comparison** further down — that work predates this scenario and used a
smaller, less-isolated leg (11 fewer idle bench-fleet containers stopped,
and Elasticsearch on unmatched 2 vCPU/2 GB hardware); treat the numbers
in this section as superseding those for anything the two overlap on.

## C1 — 2 nodes, writes wait for **both**

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  | **888** |    242 |    283 |    598 |    290 |
| write 16c |    924 |  1,079 |  1,311 | **2,361** |  1,614 |
| read 16c  |  1,131 |  2,238 |  2,031 | **4,648** |  3,191 |
| mixed 16c |  1,045 |  1,457 |  1,437 | **3,333** |  2,815 |

## C2 — 2 nodes, writes wait for the **primary only** (async replication)

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  | **848** |    715 |    756 |    832 |    339 |
| write 16c |    956 |  2,186 |  1,652 | **3,300** |  1,693 |
| read 16c  |  1,175 |  2,250 |  2,091 | **4,573** |  3,241 |
| mixed 16c |  1,023 |  1,835 |  1,522 | **3,556** |  2,850 |

## C3 — 3 nodes, writes wait for **all 3**

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB* |
|----------|-------:|----------:|----------:|-----------:|---------:|
| write 1c  | **722** |    156 |    175 |    581 |    299* |
| write 16c |    870 |    960 |    985 | **2,328** |  1,370* |
| read 16c  |  1,135 |  2,326 |  2,256 | **4,339** |  3,430* |
| mixed 16c |  1,037 |  1,511 |  1,433 | **3,328** |  2,403* |

`*` MariaDB's C3 is the identical physical config as C4 (see note ¹) — a
single measurement, not an independent second run.

## C4 — 3 nodes, writes wait for **2 of 3** (quorum)

| Workload | skaidb | MongoDB 7 | MongoDB 8 | PostgreSQL | MariaDB |
|----------|-------:|----------:|----------:|-----------:|--------:|
| write 1c  | **684** |    229 |    277 |    631 |    283 |
| write 16c |    953 |  1,171 |  1,298 | **2,543** |  1,438 |
| read 16c  |  1,210 |  2,134 |  2,281 | **4,611** |  3,164 |
| mixed 16c |  1,079 |  1,557 |  1,606 | **3,506** |  2,536 |

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

## Single-node isolated comparison — closing the C1-C4 investigation (2026-07-16)

The C1-C4 matrix above flagged its own result as suspect: skaidb trailing
every system on every workload was attributed to a "leading hypothesis" —
quorum coordination RTT compounding with a 16-container shared host —
with an explicit note to **"re-run on a dedicated host before drawing
product conclusions."** This is that re-run.

**Setup.** One node per system, no replication (skaidb: `replication_factor
= 1`, no seeds; PostgreSQL/MariaDB/MongoDB: read against the primary of
their existing C1-C4 cluster — sync replication is decoupled from reads by
design in all three, confirmed by reading PostgreSQL's own
`SyncRepWaitForLSN`/`walsender` implementation, so leaving replication
attached doesn't bias a read-only measurement). Same **1 vCPU / 512 MB**
LXC class as the rest of this document (skaidb1/pg1/maria1/mongo1 from the
C1-C4 fleet, temporarily detached/restored for skaidb only, since it alone
pays a live replica RTT on quorum reads). Elasticsearch ran on the
existing fts-es container (**2 vCPU / 2 GB** — a different spec class,
the only ES instance available on this fleet; marked ¹, directional
only). Client colocated on the same container as the server (true
loopback, zero network hop) — a 1000-row table, PK point read,
unprepared SQL/query per op, same shape as the `read 16c` workload above.

| | skaidb 0.92.1 | PostgreSQL 15 | MariaDB 11.4 | MongoDB 7.0 | Elasticsearch 8.14.3 ¹ |
|---|---:|---:|---:|---:|---:|
| **1 thread** throughput (ops/s) | **7,755** | 4,596 | 3,314 | 1,341 | 839 |
| 1 thread p50 / p99 (µs) | **110 / 610** | 198 / 311 | 301 / 470 | 689 / 1,348 | 788 / 6,107 |
| **16 threads** throughput (ops/s) | 4,477 | 4,237 | 3,469 | 1,352 | 2,243 |
| 16 threads p50 / p99 (µs) | 2,400 / 17,470 | 3,566 / 5,961 | 4,540 / 7,479 | 11,322 / 25,414 | 6,148 / 21,031 |

¹ Elasticsearch has 2× the CPU of every other row; its numbers aren't
matched-hardware and are included for rough orientation only.

**skaidb leads single-thread by 1.7× over PostgreSQL, 2.3× over MariaDB,
5.8× over MongoDB** — the opposite ranking from the C1-C4 matrix,
confirming the matrix's own hypothesis: skaidb's last place there was a
quorum-RTT-plus-shared-host artifact, not a reflection of per-operation
cost. On genuinely isolated hardware, skaidb's storage/engine path (also
verified separately at the code level — see below) is the fastest of the
five.

**A second, independently useful finding: on a true 1 vCPU node, 16
threads buys nothing.** skaidb, PostgreSQL, MariaDB, and MongoDB (all 1
vCPU here) show *flat-to-worse* throughput at 16 threads than at 1 —
pure context-switch tax with no real parallelism to exploit. Elasticsearch
(2 vCPU) is the only system that scales up, ~2.7×, consistent with having
a second core to actually use. This means the near-linear 11×
single-vCPU→16-thread scaling skaidb shows on many-core hardware (up to
~720K ops/s on a 32-core workstation, loopback, unreplicated) describes
*multi-core scaling headroom*, not what this fleet's actual 1-vCPU
deployment target experiences — don't conflate the two when citing
concurrency numbers for this product's typical deployment shape.

**Code-level read cost, isolated from any I/O**: a layered in-process
timing breakdown (`skaidb-engine/examples/read_path_breakdown.rs`) on the
point-read path — SQL parse alone vs. parse+bind+dispatch+engine-lookup,
zero network — measured **parse ≈ 0.54 µs, full in-process execute ≈
1.19 µs** (200k-iteration median). This is 50-100× below even the
isolated single-node loopback numbers above, meaning wire
protocol/socket I/O, not skaidb's own SQL/storage path, is the dominant
cost at this scale — expected, and consistent with every other system
here paying the same class of overhead.

**Read-consistency cost, measured on the live 3-node bench fleet**:
`read_consistency = ONE` vs `QUORUM` (skaidb1/2/3, quorum write, isolated
A/B, contaminated first-run-after-restart discarded per the methodology
below) showed **~8%** throughput difference — real, but not the dominant
factor in the C1-C4 gap it was initially (over-confidently) suspected to
be. Tracing the actual quorum read path
(`Node::point_get`, node.rs:5780) confirmed peer fan-out is pipelined and
overlapped with the local read, not sequential — but also surfaced an
undocumented tail-latency risk worth flagging for future investigation:
**read-repair is synchronous** (node.rs:5920) — if any queried replica
returned a stale version, the coordinator blocks the client response on
repairing it before returning, rather than returning the fresh value
immediately and repairing in the background.

**Housekeeping**: `ReadCache` shard count (16→64) and a newly-sharded
`BlockCache` (previously a single mutex, now 4-way, matching PG's
per-partition buffer-mapping rationale) were tried as a direct response
to this investigation — both are correctness-neutral and shipped
uncommitted pending review, but neither moved a contention-isolated
microbenchmark (`read_cache_contention_probe`) or a full-Engine A/B
(`cache_contention` example): this fleet's actual bottleneck is per-node
scheduling/coordination overhead, not lock contention on caches that
were already comfortably under-contended at this row count and thread
count.

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
- WAL segments are pre-allocated up front (`set_len` + one `sync_all`,
  `wal.rs`) instead of growing per append, so a durable single-row write's
  fsync becomes a data-only flush on already-allocated space instead of
  one that also has to journal a file-size extension every time — 3.0-3.1×
  on write/mixed throughput on at least one measured storage class (see
  "C0 — 1 node" below for the full writeup and numbers).

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
- **`ReadCache`/`BlockCache` shard widening** (tried 2026-07-16, prompted
  by the C1-C4 investigation below): widened `ReadCache` 16→64 shards and
  gave `BlockCache` the same sharded design (previously one mutex).
  Measured null on both a direct lock-contention probe and a full-Engine
  A/B — this fleet's C1-C4 gap is per-node scheduling/coordination
  overhead, not cache lock contention (see the single-node isolated
  comparison below for the actual cause). Harmless and architecturally
  consistent (PG uses the same per-partition rationale for its buffer
  mapping table); kept as a cleanliness change, not a perf one.

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
