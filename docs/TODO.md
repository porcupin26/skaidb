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

- [ ] **[ts] Rollup backfill** — repair-merged (gap-filled) samples don't
  retroactively update rollups (flush-path maintenance only).
- [ ] **[ts] Opportunistic rollup serving within retention** — the
  v0.32.0 rewrite reads rollups only beyond the retention horizon;
  serving big in-retention windows from rollups would cut IO but can
  silently miss repair-backfilled samples until rollup backfill lands.
  Revisit (opt-in hint or automatic) after backfill.
- [ ] **[ts] TS reclaim** — after a reshard, former owners keep stale
  series copies (harmless under union-merge reads); add a reclaim pass
  like row tables.
- [ ] **[ts] Label postings index + regex matchers** — matchers scan the
  per-block series list (fine at moderate cardinality); postings unlock
  regex and high-cardinality matching.
- [ ] **[ts] PromQL extras** — regex matchers (`=~`/`!~`), `offset`,
  vector arithmetic, `histogram_quantile`; then the node-exporter
  dashboard panel-by-panel diff against a real Prometheus (the original
  phase-7 exit criterion).
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

- [ ] **[ui] Phase 5+ extras (demand-driven)**: FTS playground
  (query + highlight + SUGGEST tester), TS mini graphs from the query
  console, ES-subset request tester.

## Other

- [ ] **[perf]** See PERFORMANCE_AUDIT.md for the perf backlog
  (pipelining and paged migration shipped in v0.34.0).
