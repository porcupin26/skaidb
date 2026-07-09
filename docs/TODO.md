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
- [ ] **[fts] top_hits SQL surface** — shipped on the ES subset
  (per-bucket top documents); a native SQL spelling (window functions or
  a dedicated per-group-top-k clause) is an open language-design
  question.
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
| Query-plan inspection | nothing (only `EXPLAIN SCORE`) | `EXPLAIN <statement>` — index selection, pushdown/scatter/fallback decisions, per-node fan-out |
| Cluster membership | `/admin/add-node`, `\node add` | `ALTER CLUSTER ADD NODE 'host:7100'` / `ALTER CLUSTER REMOVE NODE 'id'` |
| Cluster health | `/status`, `/admin/status`, `\cluster` | `SHOW CLUSTER` (members, epoch, ring, hints, liveness) |
| Anti-entropy / space | `/admin/repair`, `/admin/reclaim` | `REPAIR CLUSTER` / `RECLAIM` (async, returns a job row) |
| Runtime configuration | `/admin/config*`, `\config` | `SHOW CONFIG [LIKE 'observability.%']` / `SET CONFIG key = 'value'` (ALTER SYSTEM-style) |
| Slow-query log | `/admin/slow` | `SHOW SLOW QUERIES [LIMIT n]` |
| Session consistency | driver/shell (`\consistency`) | `SET CONSISTENCY { ONE \| QUORUM \| ALL }` as a statement |
| Per-group top documents | ES `top_hits` sub-agg only | window functions (`ROW_NUMBER() OVER (PARTITION BY g ORDER BY score() DESC)`), or a dedicated `TOP k BY <expr>` group clause |
| Field-subset dis-max match | internal `match_best` (ES multi_match best_fields) | document/expose `MATCH_BEST(col, col, ..., 'text')` |
| Vector index tuning | fixed ef/M | `ALTER VECTOR INDEX v SET (ef = 128, m = 16)` |
| Row TTL on regular tables | only TS `RETENTION` | `CREATE TABLE t (PRIMARY KEY (id)) WITH (ttl = 30d)` |
| Backup / snapshot | nothing anywhere | `BACKUP [DATABASE d] TO '<path>'` / `RESTORE FROM '<path>'` |
| Batch scripts | one statement per call | multi-statement bodies are a protocol decision, not grammar — probably keep as-is |

Ordered by expected value: `EXPLAIN`, `SHOW CONFIG`/`SET CONFIG`, and
`SHOW CLUSTER` close the biggest day-to-day gaps (they make skaidbsh and
any SQL-only client fully self-sufficient); `BACKUP`/`RESTORE` is the
biggest missing capability outright; TTL and window functions are the
biggest language features.

## Performance

(The audit record — what already holds, measured dead ends, methodology —
lives in [BENCHMARKS.md](BENCHMARKS.md#performance-engineering-notes).)

- [ ] **[perf] Merkle-tree anti-entropy** — paged repair compares every
  key each pass; a Merkle tree per table would make repair cost
  proportional to divergence, not table size.
- [ ] **[perf] Vector index persistence** — HNSW graphs are in-memory
  and rebuilt from the table on open (slow for large sets). Persist
  per-segment graphs alongside the LSM (snapshot + mmap), quantized
  vectors in RAM.
- [ ] **[perf] Lazy index-order merge for unbounded `ORDER BY`** —
  `ORDER BY <indexed>` without `LIMIT` still materializes the index in
  order first; a lazy merge would stream it. (With `LIMIT` the top-k
  path already avoids the sort.)
- [ ] **[perf] Memtable flush without clones** — flush streams entries
  but still clones each key/value even though the memtable is dropped
  right after; a consuming iterator would halve the transient flush
  spike. Background path only.
- [ ] **[perf] Per-statement replica/peer snapshot** — `replicas_for`
  builds a fresh `Vec` per row in batch replication and peer addresses
  clone per fan-out site. Measured class: a few small allocations next
  to an fsync + RTT — cleaner CPU profile, no expected throughput
  change.
