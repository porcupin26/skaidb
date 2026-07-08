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
  mixed ingest+query (deferred on request); ~~failure injection~~ —
  **done 2026-07-08**: torn-dir injections (truncated meta.json, deleted
  segment file, wiped dir) all rebuild from the table on reopen, and the
  deleted-segment case exposed a real gap (torn-but-openable index
  served corruption errors forever) now fixed with checksum validation
  at open. ~~Explain-output audit~~ — done: the ES `"explain": true`
  breakdown carries the full BM25 chain (per-term K1 / idf(n, N) /
  tf-norm), verified live. ~~SEARCH.md pass~~ — done. Still open: the
  24 h soak (deferred on request), disk-full-mid-merge (needs a
  fault-injecting filesystem), benchmark re-publication.
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
- [x] **[fts] top_hits** — shipped on the ES subset: a `top_hits`
  sub-agg under terms/date_histogram buckets returns each retained
  bucket's top documents (relevance-ordered when the query searches;
  one bounded per-bucket query each; `size`/`_source` honored, explicit
  `sort` declines). A native SQL surface (window functions or a
  per-group-top-k clause) remains an open language-design question.
- [x] **[fts] Multi-word synonyms + phrase expansion** — shipped:
  multi-word group entries match as consecutive query-token sequences
  and expand as phrase alternatives (both directions), analyzed with the
  field's own pipeline; still `MATCH`-only, hot-reloadable.
- [x] **[fts] Approximate cardinality opt-in** — shipped:
  `APPROX_COUNT_DISTINCT(col)` pushes down as tantivy's HLL cardinality
  sketch (never bails on wide term sets); grouped requests and every
  non-pushdown path answer exactly (an exact answer is a valid
  approximation). `COUNT(DISTINCT)` stays exact-or-fallback.
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
- [ ] **[ts] PromQL partial gather** — investigated 2026-07-08 and
  **declined as inexact**: the raw evaluator's range window is
  `[t-w, t]` (both boundaries inclusive) while partials bucket
  `[b, b+step)`, so samples landing exactly on step boundaries — the
  common case for scraped data — would silently differ. Needs a
  windowed-partial variant (per-window `(t-w, t]` partials over the
  wire) rather than the fixed-bucket ts_partialize. Groundwork shipped:
  `Backend::ts_partials` now exposes the cluster partial scatter to the
  server layer.
- [x] **[ts] Self-scrape** — shipped: `observability.self_scrape`
  (+ `self_scrape_interval_secs`, both live-mutable) ingests the node's
  own `/metrics` into the `metrics` TS table every interval — the node
  dashboards itself with no external Prometheus.
- [x] **[ts] `memory_target` integration** — shipped: the budget carves
  a per-TS-table head cap (budget/8, clamped 4–256 MB); a head past its
  cap flushes wholesale mid-window (partial blocks compact together),
  bounding ingest RSS on budgeted nodes.
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
