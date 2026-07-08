# skaidb — to do

**One consolidated list of all pending work**, roughly in priority order,
tagged by area. Shipped feature state lives in
[TIMESERIES.md](TIMESERIES.md) / [VECTOR.md](VECTOR.md) /
[SEARCH.md](SEARCH.md) / the README; the FTS phase history and exit
benchmarks in [FTS_TODO.md](FTS_TODO.md) and
[BENCHMARKS.md](BENCHMARKS.md); performance-specific items in
[PERFORMANCE_AUDIT.md](PERFORMANCE_AUDIT.md); history in git.

FTS status: **phases 0–7 are complete with every exit criterion met**
(perf + parity + aggregation A/Bs vs Elasticsearch, cluster fleet smoke
with kill/rejoin). What follows FTS-wise is the phase-8 decision, phase-9
hardening, and tracked extras.

## Decisions needed (not code yet)

- [x] **[fts] Phase 8 checkpoint** — decided **build**, core shipped
  2026-07-08 (`_bulk`/`_search`/`_count`/`_mapping`; see FTS_TODO.md
  phase 8 and SEARCH.md).
- [x] **[fts] Tantivy sub-aggregation bug filed upstream** —
  [quickwit-oss/tantivy#2992](https://github.com/quickwit-oss/tantivy/issues/2992).
- [ ] **[fts] Lift the grouped-metrics pushdown guard** when
  tantivy#2992 is fixed upstream (restores the one aggregation class ES
  currently wins, 276 ms → ~2 ms).

## Operational

- [x] **[ops] Publish the latest release** — done 2026-07-08: v0.43.1
  (ORDER BY, SUGGEST, MORE_LIKE_THIS, synonyms/ALTER, plus the two
  cluster-path fixes its own release smoke caught) on repo.zapolski.nyc,
  test cluster upgraded and smoke-verified.

## Web UI

- [ ] **[ui] Built-in admin UI** — all five phases shipped (status,
  query console, stats dashboards, config editor, admin ops, hardening
  pass); feature doc: [UI.md](UI.md), plan history:
  [UI_TODO.md](UI_TODO.md). Remaining: verify the UI on the 3-node test
  cluster after the next release rollout (login, status vs `/status`,
  a node join driven from the admin tab, read-only role sees denials).

## Full-text search

- [ ] **[fts] ES-REST subset follow-ups**: `bool.should` beside
  must/filter (needs optional-scoring composition), multi-key sort,
  `_source` include/exclude lists, GET-by-id (`/{index}/_doc/{id}`),
  auto-create-on-bulk if shipper demand shows up.
- [ ] **[fts] Phase 9 — hardening & the honesty pass**: 24 h soak under
  mixed ingest+query; failure injection (disk-full mid-merge, torn index
  dir → rebuild); explain-output audit; final SEARCH.md pass; benchmark
  publication tidy-up in BENCHMARKS.md.
- [ ] **[fts] Sharded scatter for aggregations and fast-field sort**:
  per-shard partials / per-shard sorted top-k need per-key ownership
  filters (e.g. a ring-hash fast field) so RF < members doesn't
  double-count replicas, plus wire additions. Today sharded corpora take
  the correct-but-slower coordinator fallback.
- [ ] **[fts] Per-hit score explain** (phase-3 nicety): tantivy has
  `Query::explain`; needs a SQL surface (an `EXPLAIN SCORE`-style
  statement or `score_explain()` projection).
- [ ] **[fts] `multi_match` `cross_fields` mode** (phase-3 nicety;
  dis-max `best_fields` is the shipped default).
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

## Other

- [ ] **[perf]** See PERFORMANCE_AUDIT.md for the perf backlog
  (pipelining and paged migration shipped in v0.34.0).
- [ ] **[docs] Grafana route documentation** — a short docs/GRAFANA.md:
  pointing the built-in Prometheus datasource at skaidb (works today),
  the Infinity/SQL recipe as the fallback.
