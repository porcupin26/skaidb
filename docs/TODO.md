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
  real bug); re-run the ES A/B campaign to publish post-guard,
  post-`MATCH_CROSS`/`BOOSTED` numbers in BENCHMARKS.md.
- [ ] **[fts] Global BM25 statistics mode** — per-shard stats today (like
  ES across shards); an optional global-stats mode if result-set parity
  tests ever care.
- [ ] **[fts] ES-REST extras on demand**: `minimum_should_match` > 1,
  multi_match per-field `^boosts` (declined today with a pointer to
  `<col>.boost`), `top_hits` explicit sort, `_mget`, index templates —
  add when a real client needs them.
- [ ] **[fts] Merge-policy tuning on LXC-class disks**: conditional —
  revisit only if an ingest-heavy workload surfaces merge stalls (the
  ingest win over ES leaves no urgency).

## Time-series

- [ ] **[ts] Streamed TS results** — TS SELECT materializes before the
  (streamable) wire; large raw range dumps should stream end-to-end like
  row-table `QueryStream`.
- [ ] **[ts] Label postings index** — regex matchers shipped
  (scan-based, fine at moderate cardinality); a postings index remains
  the perf unlock for high-cardinality matching.
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

- [ ] **[cluster] Global (value-sharded) secondary indexes** (large —
  see [GLOBAL_INDEXES.md](GLOBAL_INDEXES.md)) — **phase 1 (entry
  plumbing) shipped in v0.90**: `WITH (global = true)` DDL, `__gidx__`
  entry table, coordinator companion writes at the row's consistency,
  single-node parity, planner exclusion. Next: **phase 2** — routed
  equality-probe read path + planner pick + EXPLAIN
  `global-index probe (routed)` + backfill of pre-existing rows; then
  phase 3 (repair leg / orphan GC) and phase 4 (bench at RF<members,
  prod rollout behind the flag).
- [ ] **[cluster] Distributed / multi-key transactions** (deferred, large)
  — cluster mode autocommits per statement (`BEGIN/COMMIT/ROLLBACK`
  rejected); no 2PC/coordinator exists. agencik designs around it with
  idempotent PK overwrites.

## Performance

(The audit record — what already holds, measured dead ends, methodology —
lives in [BENCHMARKS.md](BENCHMARKS.md#performance-engineering-notes).)

- [ ] **[perf] Active memory release at the cgroup ceiling (skai2 #8)** —
  three production wedges in five days (2026-07-11/13/15): RSS ratchets to
  the 4 GB cgroup limit and stays there, starving file cache into an IO
  storm; with swap now off the failure mode is an OOM-kill. v0.83.1's REST
  guardrails + v0.87.0's chunked row streaming removed the known multi-GB
  live-buffer sources, but "at the ceiling, shed writes only" remains the
  policy — there is still no active reclaim (flush + FTS commit exist;
  jemalloc purge does not). **Instrumentation shipped in v0.90**: anon/file
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
  `(schema stamp, write_seq)`, full-copy clusters) landed in v0.89: a
  converged pass now costs one stamp scan per *changed* table, zero for
  idle ones. Remaining endgame: **incremental digests** — maintain the
  bucket XOR at write time so even changed tables skip the scan.
  Design constraint: a digest that misses even one write site produces
  FALSE CONVERGENCE (repair skips real divergence forever) — needs a
  complete write-site audit plus a periodic scan-rebuild self-check.
  Note: pre-v0.89 SSTables have no sidecar until compaction rewrites
  them (fallback decompresses values); the digest cache hides this for
  idle tables. **Measure a prod pass post-v0.89, then consider
  re-tightening `anti_entropy_interval_secs` from 3600.**
- [ ] **[perf] Vector index memory: quantization + mmap** — persistence
  itself already exists (snapshot + watermark-delta replay at open; saved
  at create/rebuild/graceful shutdown, and since v0.88 checkpointed every
  10 min so a crash replays minutes, not everything since the last clean
  stop). What remains from the original idea: full-precision f32 vectors
  live in RAM and in the snapshot (182k × dim × 4 B for the gmail set) —
  quantize the in-RAM copy, and mmap the (possibly per-segment) snapshot
  instead of a full deserialize, when vector memory becomes the constraint.
- [ ] **[perf] Lazy index-order merge for unbounded `ORDER BY`** —
  `ORDER BY <indexed>` without `LIMIT` still materializes the index in
  order first; a lazy merge would stream it. (With `LIMIT` the top-k
  path already avoids the sort.)
- [ ] **[perf] Per-statement replica/peer snapshot** — `replicas_for`
  builds a fresh `Vec` per row in batch replication and peer addresses
  clone per fan-out site. Measured class: a few small allocations next
  to an fsync + RTT — cleaner CPU profile, no expected throughput
  change.
