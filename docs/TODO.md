# skaidb — to do

Pending work only, roughly in priority order. Shipped feature state lives in
[TIMESERIES.md](TIMESERIES.md) / [VECTOR.md](VECTOR.md) / the README;
performance-specific items in [PERFORMANCE_AUDIT.md](PERFORMANCE_AUDIT.md);
history in git.

## 1. Time-series follow-ups

- **Rollup backfill** — repair-merged (gap-filled) samples don't
  retroactively update rollups (flush-path maintenance only).
- **Opportunistic rollup serving within retention** — the v0.32.0 rewrite
  reads rollups only beyond the retention horizon (where it strictly adds
  data); serving big in-retention windows from rollups would cut IO but
  can silently miss repair-backfilled samples until rollup backfill lands.
  Revisit (opt-in hint or automatic) after backfill.
- **TS reclaim** — after a reshard, former owners keep stale series copies
  (harmless under union-merge reads); add a reclaim pass like row tables.
- **Label postings index + regex matchers** — matchers scan the per-block
  series list (fine at moderate cardinality); postings unlock regex and
  high-cardinality matching.
- **PromQL extras** — regex matchers (`=~`/`!~`), `offset`, vector
  arithmetic, `histogram_quantile`; then the node-exporter dashboard
  panel-by-panel diff against a real Prometheus (the original phase-7 exit
  criterion).
- **PromQL partial gather** — the `/api/v1` evaluator still ships raw
  samples cluster-wide (its per-step lookback windows don't align with
  fixed buckets); teach `query_range` to reuse the v0.31.0 partial
  gather for step-aligned windows.
- **Self-scrape** — `observability.self_scrape` ingesting the node's own
  `/metrics` into the TS store.
- **`memory_target` integration** — TS head memory isn't part of the
  storage budget yet.
- **Streamed TS results** — TS SELECT materializes before the (streamable)
  wire; large raw range dumps should stream end-to-end.
- **Exemplars / native histograms** — chunk-format headroom is reserved.
- **Validation soak** — 24 h Prometheus remote_write side-by-side with its
  own TSDB, zero-loss comparison (phase-4 exit criterion).

## 2. Full-text search

Elasticsearch-class search, SQL-first, Tantivy-cored (Rust Lucene — a JVM
is a non-starter in a single-binary DB). Full plan, feature matrix, phased
roadmap and benchmarks: [FTS_TODO.md](FTS_TODO.md). Phase 0 (Tantivy
go/no-go spike), phase 1 (single-node core: `skaidb-fts` crate,
`CREATE SEARCH INDEX` DDL, `MATCH`/`SEARCH`/`score()`, top-k pushdown,
watermark crash recovery), and phase 2 (analysis & mappings parity:
analyzer registry, per-column options, typed fast fields, `.keyword`
twins, `copy_to`) are done — shipped state in [SEARCH.md](SEARCH.md).
Next action: **phase 3** (query DSL parity).

## 3. Other

- See PERFORMANCE_AUDIT.md for the perf backlog (pipelining and paged
  migration shipped in v0.34.0).
- **Grafana route documentation** — a short docs/GRAFANA.md: pointing the
  built-in Prometheus datasource at skaidb (works today), the Infinity/SQL
  recipe as the fallback.
