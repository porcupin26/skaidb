# skaidb — to do

Pending work only, roughly in priority order. Shipped feature state lives in
[TIMESERIES.md](TIMESERIES.md) / [VECTOR.md](VECTOR.md) / the README;
performance-specific items in [PERFORMANCE_AUDIT.md](PERFORMANCE_AUDIT.md);
history in git.

## 1. Multi-user auth & RBAC management

The enforcement plumbing exists (SCRAM/Basic authn; privileges on
Global/Table objects; role inheritance; per-statement checks incl. prepared
statements; `/admin/*` gated on `Admin`) — but the only runtime principal is
the config superuser. Add the management layer:

- SQL surface: `CREATE/ALTER/DROP USER`, `CREATE/DROP ROLE`,
  `GRANT/REVOKE <priv> ON <table|*> TO <role>`, `GRANT ROLE r TO u`,
  `SHOW GRANTS` — gated on the (currently unused) `Grant` privilege;
  `Admin` implies `Grant`.
- Persistence: users (SCRAM verifiers, never plaintext) + roles/grants in
  the catalog (serde-default), loaded at open; RoleStore/AuthState become
  views over catalog state.
- Cluster replication: user/role DDL broadcasts + schema bootstrap/repair
  replay with LWW stamps, like table DDL.
- Close the Prometheus endpoint gaps: `remote_write` checks `Insert` on the
  metrics table; `/api/v1/query*` checks `Select` (today both only require
  authentication).
- Config superuser stays as the bootstrap principal. Docs: README +
  QUERY_SYNTAX sections.

## 2. Time-series follow-ups

- **TS hinted handoff** — buffer failed replica `TsAppend` batches per peer
  (bounded, like row hints) and replay via `TsMerge` on reachability;
  repair already converges but hints recover brief outages in seconds.
- **Partial-aggregate pushdown** — cluster TS queries ship raw matching
  samples to the coordinator; push per-series per-bucket partials
  (count/sum/min/max/first/last/increase) to the nodes and pick one
  replica's answer per series. Cuts transfer ~RF× and more for wide
  aggregations feeding Grafana.
- **Rollup query rewrite** — pick the coarsest rollup satisfying a
  `time_bucket` query automatically; today queries target the rollup table
  explicitly.
- **Rollup backfill** — repair-merged (gap-filled) samples don't
  retroactively update rollups (flush-path maintenance only).
- **TS reclaim** — after a reshard, former owners keep stale series copies
  (harmless under union-merge reads); add a reclaim pass like row tables.
- **Label postings index + regex matchers** — matchers scan the per-block
  series list (fine at moderate cardinality); postings unlock regex and
  high-cardinality matching.
- **PromQL extras** — regex matchers (`=~`/`!~`), `offset`, vector
  arithmetic, `histogram_quantile`; then the node-exporter dashboard
  panel-by-panel diff against a real Prometheus (the original phase-7 exit
  criterion).
- **Self-scrape** — `observability.self_scrape` ingesting the node's own
  `/metrics` into the TS store.
- **`memory_target` integration** — TS head memory isn't part of the
  storage budget yet.
- **Streamed TS results** — TS SELECT materializes before the (streamable)
  wire; large raw range dumps should stream end-to-end.
- **Exemplars / native histograms** — chunk-format headroom is reserved.
- **Validation soak** — 24 h Prometheus remote_write side-by-side with its
  own TSDB, zero-loss comparison (phase-4 exit criterion).

## 3. Other

- **Request pipelining** on client connections (id-tagged concurrent
  requests; streaming shipped, pipelining didn't) — see
  PERFORMANCE_AUDIT.md for the rest of the perf backlog.
- **Grafana route documentation** — a short docs/GRAFANA.md: pointing the
  built-in Prometheus datasource at skaidb (works today), the Infinity/SQL
  recipe as the fallback.
