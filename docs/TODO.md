# skaidb — to do

**One consolidated list of all pending work**, roughly in priority order,
tagged by area. Shipped feature state lives in
[TIMESERIES.md](TIMESERIES.md) / [VECTOR.md](VECTOR.md) /
[SEARCH.md](SEARCH.md) / [UI.md](UI.md) / [GRAFANA.md](GRAFANA.md) / the
README; the FTS phase history and exit benchmarks in
[FTS_TODO.md](FTS_TODO.md) and [BENCHMARKS.md](BENCHMARKS.md); the UI plan
history in [UI_TODO.md](UI_TODO.md); performance-specific items in
[PERFORMANCE_AUDIT.md](PERFORMANCE_AUDIT.md); everything completed, in git.

FTS status: **phases 0–8 complete with every exit criterion met** (perf +
parity + aggregation A/Bs vs Elasticsearch, cluster fleet smoke with
kill/rejoin, ES-REST subset + follow-ups). Web UI: all five phases shipped
and cluster-verified. What remains is phase-9 hardening and the tracked
extras below.

## Full-text search

- [ ] **[fts] Phase 9 — hardening & the honesty pass**: 24 h soak under
  mixed ingest+query; failure injection (disk-full mid-merge, torn index
  dir → rebuild); explain-output audit; final SEARCH.md pass; benchmark
  publication tidy-up in BENCHMARKS.md.
- [ ] **[fts] Lift the grouped-metrics pushdown guard** when
  [quickwit-oss/tantivy#2992](https://github.com/quickwit-oss/tantivy/issues/2992)
  is fixed upstream (per-bucket metrics currently take the exact row
  fallback; doc-count-only groupings still push down).
- [ ] **[fts] Sharded scatter for aggregations and fast-field sort**:
  per-shard partials / per-shard sorted top-k need per-key ownership
  filters (e.g. a ring-hash fast field kept consistent through
  resharding) so RF < members doesn't double-count replicas, plus wire
  additions. Correctness-critical — wants its own design + fleet-bench
  cycle. Today sharded corpora take the correct-but-slower coordinator
  fallback (this also gates per-hit explain on sharded clusters).
- [ ] **[fts] top_hits** (per-group top documents): wants a SQL surface —
  window functions or a dedicated per-group-top-k clause.
- [ ] **[fts] Multi-word synonyms + phrase expansion** (single-word groups
  ship today; multi-word needs query-time phrase rewriting).
- [ ] **[fts] Approximate cardinality opt-in** (e.g.
  `APPROX_COUNT_DISTINCT()`) if `COUNT(DISTINCT)`'s exact terms-bucket
  bail ever hurts on very high cardinality. Conditional — no demand yet.
- [ ] **[fts] Merge-policy tuning on LXC-class disks**: conditional —
  revisit only if an ingest-heavy workload surfaces merge stalls (the
  ingest win over ES leaves no urgency).
- [ ] **[fts] ES-REST extras on demand**: `minimum_should_match` > 1,
  multi_match per-field `^boosts` (declined today with a pointer to
  `<col>.boost`), `_mget`, index templates — add when a real client needs
  them.

## Time-series

- [x] **[ts] Rollup backfill** — shipped: any write path (flush **or**
  repair merge) now recomputes every touched rollup bucket from the
  authoritative store and rewrites it (rollup head flushed first so the
  newer block wins the dedupe) — rollups stay exact under gap-filling
  repairs and out-of-order flushes.
- [x] **[ts] Opportunistic rollup serving within retention** — shipped
  (automatic, single-node): with backfill keeping rollups exact, group
  buckets wholly below the head's oldest sample serve from the rollup —
  same numbers, less raw IO; the straddling bucket rounds **down** to
  stay on the source (the retention tier keeps rounding up). Clustered
  deployments keep retention-only routing until a min-over-replicas
  boundary exchange exists (the sharded-partials work below).
- [x] **[ts] TS reclaim** — shipped: `reclaim` now also drops whole
  unowned series (head flushed so the WAL can't resurrect them; blocks
  rewritten without the series) once a current owner confirms an
  **identical** copy (count + checksum); a diverged owner gets the copy
  pushed instead and the next pass reclaims. Rollup series co-place with
  their source (placement ignores `__field__`), so they reclaim by the
  same rule.
- [ ] **[ts] Label postings index** — regex matchers shipped (scan-based,
  fine at moderate cardinality); a postings index remains the perf
  unlock for high-cardinality matching.
- [ ] **[ts] PromQL extras, round 2** — shipped: regex matchers
  (anchored `=~`/`!~`; coordinator-applied on clusters), `offset`,
  vector arithmetic (`+ - * /`, scalar and 1:1 vector matching),
  `histogram_quantile`. Remaining: subqueries, `group_left`/right,
  `topk`; then the node-exporter dashboard panel-by-panel diff against
  a real Prometheus (the original phase-7 exit criterion).
- [ ] **[ts] PromQL partial gather** — the `/api/v1` evaluator still
  ships raw samples cluster-wide (per-step lookback windows don't align
  with fixed buckets); teach `query_range` to reuse the v0.31.0 partial
  gather for step-aligned windows.
- [ ] **[ts] Self-scrape** — `observability.self_scrape` ingesting the
  node's own `/metrics` into the TS store.
- [ ] **[ts] `memory_target` integration** — TS head memory isn't part of
  the storage budget yet (FTS writer heap joined in v0.38).
- [ ] **[ts] Streamed TS results** — TS SELECT materializes before the
  (streamable) wire; large raw range dumps should stream end-to-end.
- [ ] **[ts] Exemplars / native histograms** — chunk-format headroom is
  reserved.
- [ ] **[ts] Validation soak** — 24 h Prometheus remote_write
  side-by-side with its own TSDB, zero-loss comparison (the TS phase-4
  exit criterion).

## Web UI

- [x] **[ui] Phase 5+ extras** — shipped: search tab with the FTS
  playground (predicate builder → runs in the query console with
  score/HIGHLIGHT), SUGGEST tester, and an ES-subset request tester
  (method/path/body → pretty JSON); the query console draws a line
  chart when a result looks like a time series (a ts/time/bucket
  column + numeric columns); plus the RBAC-filtered schema browser.

## Other

- [ ] **[perf]** See PERFORMANCE_AUDIT.md for the perf backlog
  (pipelining and paged migration shipped in v0.34.0).
