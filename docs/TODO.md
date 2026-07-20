# skaidb — to do

**The single consolidated list of all pending work**, roughly in priority
order within each area. Shipped feature state lives in
[SEARCH.md](SEARCH.md) / [TIMESERIES.md](TIMESERIES.md) /
[VECTOR.md](VECTOR.md) / [UI.md](UI.md) / [GRAFANA.md](GRAFANA.md) / the
README; measured results, performance dead ends, and benchmark
methodology in [BENCHMARKS.md](BENCHMARKS.md); everything completed, in
git history.

## Full-text search

- [ ] **[fts] Sharded scatter, residue** — aggregations (incl. AVG via
  the sum+count rewrite), sorted top-k, and key-routed per-hit explain
  all shipped (see SEARCH.md). Remaining niceties: residual SQL filters
  on the sorted scatter (needs a serializable predicate subset on the
  wire), grouped per-bucket metrics (blocked on the tantivy#2992 guard),
  and distinct counts (need term-set/sketch partials).
- [ ] **[fts] Lift the grouped-metrics pushdown guard** when
  [quickwit-oss/tantivy#2992](https://github.com/quickwit-oss/tantivy/issues/2992)
  is fixed upstream (per-bucket metrics currently take the exact row
  fallback; doc-count-only groupings still push down).
- [ ] **[fts] Phase-9 residue**: the 24 h mixed ingest+query soak
  (deferred on request); disk-full-mid-merge injection (needs a
  fault-injecting filesystem — torn-dir injection shipped and found a
  real bug).
- [ ] **[fts] Restore the Wikipedia-corpus ES A/B** — the 2026-07-16
  BENCHMARKS.md re-run used a 100k-doc synthetic corpus (`fts_corpus.py`
  needs a `simplewiki-*-pages-articles.xml.bz2` dump not staged on the
  fleet and not re-downloaded this pass) instead of the original
  280,595-doc Simple English Wikipedia corpus. Numbers are directionally
  consistent (skaidb still leads ingest + 3/4 query classes, parity
  94.2%/99.6%) but not a clean apples-to-apples re-verification. Stage
  the dump on the fleet (or find a stable download mirror) and re-run at
  the original scale; also re-run the cluster leg (3-node ingest/scatter
  latency/kill-rejoin) and the sharded-scatter kill/reshard resilience
  demos, neither re-verified this pass either.
- [ ] **[fts] Global BM25 statistics mode** — per-shard stats today (each
  shard scores against its own stats); an optional global-stats mode if
  result-set parity tests ever care.
- [ ] **[fts] ES-REST extras on demand**: `minimum_should_match` > 1,
  multi_match per-field `^boosts` (declined today with a pointer to
  `<col>.boost`), `top_hits` explicit sort, `_mget`, index templates —
  add when a real client needs them.
- [ ] **[fts] Merge-policy tuning on LXC-class disks**: conditional —
  revisit only if an ingest-heavy workload surfaces merge stalls (the
  ingest win over ES leaves no urgency).

## Time-series

- [ ] **[ts] Streamed TS results** — raw range dumps now charge the
  statement **scan budget** (v0.91): an unbounded `SELECT *` over a big
  range fails with the budget error instead of materializing until OOM
  (aggregations take the bounded partials path and are unaffected), and
  both wire surfaces already chunk-stream row results. TRUE end-to-end
  streaming remains an architecture item: `QueryOutput::Rows` and the
  wire `Response::Rows` are materialized enums (~50 consumers) — needs a
  lazy-rows variant threaded engine → node → server, plus a streaming
  gather (per-series k-way field merge over owned samples). Do it when a
  real >250k-sample raw-dump consumer exists; the budget error names the
  workaround (narrow the range / aggregate).

- [ ] **[ts] PromQL partial gather** — investigated 2026-07-08 and
  **declined as inexact**: the raw evaluator's range window is
  `[t-w, t]` (both boundaries inclusive) while partials bucket
  `[b, b+step)`, so samples landing exactly on step boundaries — the
  common case for scraped data — would silently differ. Needs a
  windowed-partial variant (per-window `(t-w, t]` partials over the
  wire). Groundwork shipped: `Backend::ts_partials` exposes the cluster
  partial scatter to the server layer.
- [ ] **[ts] PromQL round 3** — subqueries, `group_left`/`group_right`
  many-to-one matching, `topk`/`bottomk`; then the node-exporter
  dashboard panel-by-panel diff against a real Prometheus (the original
  phase-7 exit criterion).
- [ ] **[ts] Rollup opportunistic serving, clustered** — single-node
  shipped; clustered needs a min-over-replicas head-boundary exchange
  (pairs naturally with the sharded-partials wire work).
- [ ] **[ts] Exemplars / native histograms** — chunk-format headroom is
  reserved.
- [ ] **[ts] Validation soak** — 24 h Prometheus remote_write
  side-by-side with its own TSDB, zero-loss comparison (the TS phase-4
  exit criterion; deferred with the other soaks).

## SQL surface gaps (capabilities with no native SQL form)

Everything below works today through another surface (HTTP admin,
`skaidbsh` backslash commands, config file, or the ES subset) but cannot
be spoken in SQL. Each row carries the suggested SQL extension.

| Capability | Today | Suggested SQL extension |
|---|---|---|
| Batch scripts | one statement per call | multi-statement bodies are a protocol decision, not grammar — probably keep as-is |

Everything from the original gap list with real pull has shipped
(`EXPLAIN`, admin statements, `BACKUP`/`RESTORE`, TTL, `GROUP BY ... TOP
k BY` per-group top-k). General window functions (`ROW_NUMBER() OVER
(...)`) remain unimplemented; `TOP k BY` covers the common case.

## agencik integration wishlist (open items)

Gaps from migrating **agencik** onto native skaidb
(`~/projects/agencik/docs/skaidb-wishlist.md`). Shipped work (v0.83.0
through v0.87.x — see the wishlist ledger + git history; latest round:
index-def convergence on schema sync + SHOW INDEXES `local` health,
chunked-streaming REST rows, CAST syntax, batched executemany wire op,
distributed sorted top-k for QUORUM ordered reads, humanized EXPLAIN
names) lives in git history. Still open:

- [ ] **[cluster] Global (value-sharded) secondary indexes** (see
  [GLOBAL_INDEXES.md](GLOBAL_INDEXES.md)) — **phases 1+2+3 shipped**
  (v0.89 entry plumbing; v0.90 routed probe + backfill; v0.91
  hardening: full-copy repair verify leg [heal missing entries / GC
  orphans], ready-stamped `building` convergence, backfill re-drive on
  repair, IN-list multi-tuple probes). **Phase 4 first A/B done
  (2026-07-16, BENCHMARKS.md)**: correctness exact after two
  bench-caught backfill fixes (v0.91.1 batched drives, v0.91.2
  retry-or-abort readiness); latency parity at 2 members as expected —
  the fan-out delta needs a **3+ member re-run** (fleet was
  soak-constrained) before any prod call. The RF<members verify leg
  shipped in v0.92 (batched `KeysPresent`/`GidxProduced` exchanges driven
  by each shard's primary owner) — every crash-window direction now heals
  at every topology.
- [ ] **[cluster] Distributed / multi-key transactions** (deferred, large)
  — cluster mode autocommits per statement (`BEGIN/COMMIT/ROLLBACK`
  rejected); no 2PC/coordinator exists. agencik designs around it with
  idempotent PK overwrites.

## Performance

(The audit record — what already holds, measured dead ends, methodology —
lives in [BENCHMARKS.md](BENCHMARKS.md#performance-engineering-notes).)

- [ ] **[perf] Remaining O(table) coordinator materializations on the
  read path** — what's left after the 2026-07-17 ordered-read OOM fix
  (see BENCHMARKS.md for the incident + fix): (a) an unfiltered
  `SELECT * FROM <large table>` (no ORDER BY, no LIMIT) is page-bounded
  through `cluster_scan_collect` but still accumulates the full result
  Vec (`CollectSink::Rows(None)`) before the response is written —
  inherent to the response size, but streaming it chunk-by-chunk from
  the walk (QueryStream all the way down) would cap the coordinator;
  (b) at QUORUM, an exact single-key ordered read with LIMIT >
  `SORTED_TOPK_MAX` (1000) or a multi-key ORDER BY still falls back to
  the full gather-then-sort; (c) `EXPLAIN` doesn't yet report the
  PK-ordered walk or the bounded top-k (shows "full table scan"), the
  same under-reporting E-8 notes for PK-prefix plans.
- [ ] **[perf] Standalone deployments have NO memory-pressure release
  loop** — surfaced by the witness's first sync (2026-07-18, five OOM
  iterations): the engine's bulk-apply path (`apply_batch_buffered` and
  kin) leaves flushing to its caller, and only cluster mode has the
  Node-level shedding/release tier (v0.70.15) that provides it. A
  standalone node bulk-ingesting therefore accumulates memtables (and,
  if the caller forgets, unsynced WAL buffers) until the cgroup kill.
  The witness now self-manages (per-page WAL sync, flush every 32
  pages), but the underlying asymmetry remains for any standalone bulk
  writer. Consider: `apply_*_buffered` self-triggering flush at the
  memtable cap, or porting a minimal release tier to `Backend::Local`.
- [ ] **[perf] The point-read cache is entry-capped, byte-blind** —
  `read_cache_entries` counts entries, so a workload of multi-KB rows
  (the 181k × ~6 KB vector rows) occupies an order of magnitude more
  RAM than the `memory_target` budget assumed; mass point reads of
  large rows ramped the witness to its ceiling even with memtables
  flat (fixed on the witness by switching its staleness guard to the
  value-free stamps walk — but any client doing bulk point reads of
  large rows can still blow the cache budget). Consider a byte-cap
  (weight-based eviction) alongside the entry cap.
- [ ] **[perf] Active memory release at the cgroup ceiling (skai2 #8)** —
  three production wedges in five days (2026-07-11/13/15): RSS ratchets to
  the 4 GB cgroup limit and stays there, starving file cache into an IO
  storm; with swap now off the failure mode is an OOM-kill. v0.83.1's REST
  guardrails + v0.87.0's chunked row streaming removed the known multi-GB
  live-buffer sources, but "at the ceiling, shed writes only" remains the
  policy — there is still no active reclaim (flush + FTS commit exist;
  jemalloc purge does not). **Instrumentation shipped in v0.89**: anon/file
  + jemalloc allocated/resident/retained now flow as `node_stats` columns,
  `/metrics` gauges (`skaidb_memory_anon_bytes`, `skaidb_alloc_*_bytes`),
  and a 15-min "memory ramp" log line whenever usage exceeds half the
  limit — the next creep arrives with its live-vs-allocator history
  attached. NEXT: watch skai2's ramp for a few days; if
  `resident − allocated` dominates, the fix is allocator purge — an
  explicit `arena.<all>.purge` needs `tikv_jemalloc_ctl::raw` which is
  `unsafe`, and the workspace FORBIDS unsafe, so that is a deliberate
  lint-carve-out decision. If `allocated` itself ratchets, hunt the holder
  (FTS writer heaps, hint buffers, vector graphs are the candidates).
  Watch `oom_kills` in `node_stats` after every deploy day.

- [ ] **[perf] Repair digests: incremental digests (endgame)** —
  speedups (1) value-free stamp scan (stamps sidecar `<sst>.stamps`, old
  files fall back) and (2) versioned digest cache (keyed on
  `(schema stamp, write_seq)`, full-copy clusters) landed in v0.88.1: a
  converged pass now costs one stamp scan per *changed* table, zero for
  idle ones. Remaining endgame: **incremental digests** — maintain the
  bucket XOR at write time so even changed tables skip the scan.
  Design constraint: a digest that misses even one write site produces
  FALSE CONVERGENCE (repair skips real divergence forever) — needs a
  complete write-site audit plus a periodic scan-rebuild self-check.
  Note: pre-v0.88.1 SSTables have no sidecar until compaction rewrites
  them (fallback decompresses values); the digest cache hides this for
  idle tables. **Measured on prod after the v0.89.0 roll (2026-07-16):
  62 s cold / 60 s warm (was >240 s on v0.88.0).** The warm floor is the
  live-ingest tables (every write bumps write_seq → rescan) still on
  sidecar-less pre-v0.89 SSTables, plus inter-pair pacing sleeps — expect
  it to drop as compaction rewrites files. Re-measure in a few days, then
  re-tighten `anti_entropy_interval_secs` (3600 → 900 is safe at a 60 s
  pass; the 2026-07-10 lesson only forbids interval < pass time).
- [ ] **[perf] Unfiltered `GROUP BY`/aggregate over a large replicated
  table hits `scan_row_budget` (agencik E-7 residual, was: times out)** —
  the OOM crash itself is fixed and verified on prod (v0.95.3: retired
  the whole-table `cluster_scan` for the already page-bounded
  `cluster_scan_collect`). The query then still didn't *finish* within
  `storage.statement_timeout_secs` (120 s) on the real `gmail_emails`
  table (183k rows, 1.9 GB) — safe, but not useful. Root-caused via live
  profiling (2026-07-17, `strace -c -f` + per-thread `ps` sampling on
  skai1, idle vs. active-run comparison): lock contention on
  `Node::local` (`RwLock<Database>`, node.rs), not disk I/O or decode
  cost — `pread64` stayed fast under load, but the busiest thread in the
  65+-thread process sat at ~3% CPU blocked in `futex_do_wait`.
  `cluster_scan_collect` took `self.local.read()` fresh every round
  (~92 times for this table at `SCAN_PAGE_ROWS`), contending with the
  same lock every write on the node takes; Linux's writer-preferring
  `pthread_rwlock` let live write traffic repeatedly queue the reader.
  **Fixed**: `LOCAL_SCAN_BATCH_ROWS` (20,000, node.rs) decouples the
  *local* page size from `SCAN_PAGE_ROWS` (still 2,000 for peers and the
  round-based memory bound) — a bigger local page means `local_done`
  flips true after far fewer rounds, so the lock stops being touched at
  all for the rest of a large gather. Verified on prod, reproduced
  twice: the query's behavior changed from silently timing out at 120 s
  with negligible progress to completing 74.7 s of real work.
  **New residual**: that 74.7 s of progress now hits
  `storage.scan_row_budget` (default 250,000) instead — reproduced
  twice at exactly 251,766 rows examined. This cluster is RF=3
  full-replica, and `scan_meter::tick` counts each source's (local +
  every peer's) contribution separately, so a full unfiltered gather
  over a table this size inherently examines well over 250k "rows"
  before completing, independent of the lock fix. Not obviously a bug —
  the budget is tracking real per-source I/O/decode work — but worth
  a decision: raise `storage.scan_row_budget` (cluster-wide, affects the
  safety margin for every query, not just this one), or reconsider
  whether replica-source contributions should count once instead of
  per-source toward this specific budget.
- [ ] **[perf] Vector index memory: quantization + mmap** — persistence
  itself already exists (snapshot + watermark-delta replay at open; saved
  at create/rebuild/graceful shutdown, and since v0.88 checkpointed every
  10 min so a crash replays minutes, not everything since the last clean
  stop). What remains from the original idea: full-precision f32 vectors
  live in RAM and in the snapshot (182k × dim × 4 B for the gmail set) —
  quantize the in-RAM copy, and mmap the (possibly per-segment) snapshot
  instead of a full deserialize, when vector memory becomes the constraint.
- [ ] **[perf] Per-statement replica/peer snapshot** — `replicas_for`
  builds a fresh `Vec` per row in batch replication and peer addresses
  clone per fan-out site. Measured class: a few small allocations next
  to an fsync + RTT — cleaner CPU profile, no expected throughput
  change.
- [ ] **[perf] Clustered gather has no PK-prefix-range narrowing (agencik
  E-8, P2)** — `SELECT COUNT(*) FROM slack_messages WHERE channel = ? AND
  thread_ts = ?` (PK `(channel, ts)`) genuinely full-table-scans in the
  clustered path: 8.6 s at QUORUM / 0.85 s at ONE on a 130k-row table,
  confirmed 2026-07-17. Root cause confirmed by reading both gather
  paths: `Database::gather_rows_planned` (exec.rs, the **local/standalone**
  path) already narrows to a leftmost equality-pinned PK-column run —
  `channel = ?` alone bounds the scan to that channel's key slice, with
  `thread_ts` applied as a residual filter — but
  `Coordinator::matching_rows`/`matching_rows_ordered` (node.rs, the
  **clustered** path every production query actually uses) has no
  equivalent check at all: it goes full-PK-equality → PK IN-list →
  secondary index → global index → straight to `filtered_lookup`, an
  unbounded per-node scatter-scan, for anything in between (a partial PK
  prefix with no covering index, exactly this shape). EXPLAIN's "full
  table scan" label is accurate for this case, not a mislabel — but note
  EXPLAIN's own reporting logic (exec.rs) doesn't check
  `gather_rows_planned`'s PK-prefix branch either, so it would
  *under-report* a standalone-mode query that actually does take the
  narrower local path. **Moot for agencik today** — a dedicated index
  (`i_slack_messages_channel_thread_ts`) now serves their real query
  index-only — so this is about making every future "PK-prefix +
  uncovered extra filter" shape safe by default, not an active
  production issue. Fix scope: port PK-prefix-range narrowing into the
  clustered path, adapted to the distributed candidate-scatter-then-
  quorum-resolve pattern `index_lookup` already uses (a real, moderate
  change — not a one-line fix). Not started.
