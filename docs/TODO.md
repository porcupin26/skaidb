# skaidb — planned work

The project's to-do document: substantial planned features with enough design
detail to start work. Performance-specific outstanding items live in
[PERFORMANCE_AUDIT.md](PERFORMANCE_AUDIT.md); this file holds feature plans.

---

# Time-series tables — implementation plan

Make skaidb a competitive home for metrics/telemetry: a **time-series table
type** whose storage, indexing, query surface, and ingestion match what
Prometheus, VictoriaMetrics, TimescaleDB, and InfluxDB deliver — while staying
one database (same nodes, same SQL, same replication, same ops story).

## 1. What "match the top TSDBs" means (feature bar)

| Capability | Prometheus | VictoriaMetrics | TimescaleDB | skaidb target |
|---|---|---|---|---|
| Compressed columnar samples | ✅ (~1.3 B/sample) | ✅ (<1 B) | ✅ (segments) | **≤2 B/sample** |
| Series/label inverted index | ✅ | ✅ | (B-tree) | ✅ |
| Time-bucketed aggregation | ✅ (PromQL) | ✅ | ✅ `time_bucket` | ✅ (SQL) |
| Counter `rate`/`increase` | ✅ | ✅ | ✅ (hyperfunctions) | ✅ |
| Retention (O(1) block drop) | ✅ | ✅ | ✅ (chunk drop) | ✅ |
| Downsampling / rollups | ⚠️ (rec. rules) | ✅ | ✅ (cont. aggregates) | ✅ (phase 6) |
| Out-of-order ingest window | ✅ (bounded) | ✅ | ✅ | ✅ (bounded) |
| Horizontal scale + replication | ❌ (federation) | ✅ (cluster) | ⚠️ | ✅ (native ring) |
| Prometheus `remote_write` ingest | n/a | ✅ | ⚠️ (adapter) | ✅ |
| PromQL | ✅ | ✅ | ❌ | ⚠️ subset, stretch |
| Also a general OLTP/document store | ❌ | ❌ | ✅ | ✅ (already) |

Non-goals (v1): full PromQL, alerting/recording-rule engine, Grafana-native
datasource (remote_write + SQL covers ingestion and dashboards via the
PostgreSQL-style path or JSON REST), exemplars/native histograms (schema
reserved, implementation later).

## 2. Why the existing engine can't just do this

A sample stored as an LSM row costs ~100+ bytes (encoded key + document +
WAL record + memtable node) and one ring lookup per insert; Prometheus stores
the same sample in ~1.3 bytes by exploiting what makes time series special:
timestamps are near-regular (delta-of-delta encodes to ~0 bits) and successive
float values barely change (XOR/Gorilla encodes to a few bits). Matching TSDB
performance requires a **second storage layout** — per-series compressed
chunks in time-partitioned blocks — not a tuning of the document LSM.
Everything *around* storage (ring placement, quorum replication, scatter-
gather with partial-aggregate pushdown, WAL group commit, streaming results,
`memory_target`) is already built and gets reused.

## 3. Data model & SQL surface

A time-series table declares **series-key columns** (labels), a timestamp, and
numeric **fields**; rows are samples.

```sql
CREATE TIMESERIES TABLE cpu (
  SERIES KEY (host, region, core),   -- string label columns; placement key
  RETENTION 30d                      -- optional; 0/absent = keep forever
);

-- Ingest: ordinary INSERT (multi-row & prepared work as usual).
INSERT INTO cpu (host, region, core, ts, value)
  VALUES ('web1', 'nyc', '0', 1712000000000, 0.63), (...);

-- Range read, bucketed aggregation:
SELECT time_bucket('1m', ts) AS t, host, avg(value), max(value)
FROM cpu
WHERE ts BETWEEN now() - '1h' AND now() AND region = 'nyc'
GROUP BY t, host ORDER BY t;

-- Counters:
SELECT time_bucket('5m', ts) AS t, rate(value) FROM http_requests_total
WHERE ts > now() - '6h' AND handler = '/api' GROUP BY t;
```

Rules and semantics:
- Series key columns are strings (label semantics); `ts` is the existing
  millisecond `timestamp` type and is **implicitly the last PK component** —
  `(series key, ts)` uniquely identifies a sample. All other inserted fields
  must be numeric (`int`/`float`); each named field is stored as its own
  chunk stream (multi-field like Influx; a single `value` field reproduces the
  Prometheus model).
- Samples are **immutable facts**: duplicate `(series, ts)` resolves
  last-write-wins by arrival, `UPDATE`/`DELETE` are rejected except
  `DELETE ... WHERE ts < X` (range tombstone) — no per-sample HLC needed
  (the timestamp *is* the version), which is what keeps replication cheap.
- New SQL (parser work — the grammar currently has aggregates but **no scalar
  function-call syntax**, so this adds one): `time_bucket(interval, ts)`,
  `now()`, duration literals (`'5m'`, `'1h'`, `30d` in DDL), and the
  TS aggregates `rate(f)`, `increase(f)`, `delta(f)`, `first(f)`, `last(f)`.
  `rate` is counter-aware (reset detection), computed **per series** then
  aggregated — the PromQL semantics people expect.
- Introspection: `SHOW TIMESERIES TABLES`, per-table series-count/cardinality
  in `SHOW STATUS` and `/metrics`.
- `docs/QUERY_SYNTAX.md` is updated in the same change as every grammar step
  (standing rule).

## 4. Storage engine (new `skaidb-tsdb` crate)

Prometheus-style head/blocks layout, one instance per TS table per node:

```
<data_dir>/tsdb/<table>/
  wal/                    # sample WAL segments (reuses group-commit machinery)
  blocks/<minT-maxT>/     # immutable: chunks + series index + postings + meta
  head/                   # (in memory) open chunks; rebuilt from WAL on start
```

- **Head**: `series id → open chunk` map. A chunk is delta-of-delta timestamps
  + Gorilla XOR values, appended in place, sealed at ~120 samples or 2 h span.
  Sealed chunks accumulate; every 2 h boundary the head range is flushed as an
  immutable **block** and the covered WAL segments are truncated. Head memory
  participates in `memory_target` (a third share alongside memtable/read
  cache); pressure forces an early flush.
- **Blocks**: chunks file (all series' chunks for the window, sequentially),
  series file (series id → labels + chunk refs), postings (label=value →
  sorted series ids), meta.json (time range, stats). Background compaction
  merges 2 h → 8 h → 32 h blocks (fewer index lookups per query, better
  compression). Retention = delete whole expired block directories — O(1),
  no tombstone churn.
- **Out-of-order**: samples older than the head window go to a bounded OOO
  head (default window 1 h, configurable), merged at flush — Prometheus'
  current approach. Older than the window → rejected with a counted error.
- **Crash recovery**: WAL replay rebuilds the head; blocks are immutable and
  fsynced at creation (same tmp-file + dir-fsync pattern the manifest uses).
- **Compression at rest**: block chunk files already benefit from the
  existing `bottom_compression` (brotli) option applied at compaction.

## 5. Series & label index

- Series id: u64 assigned at first sight of a label set (head), persisted per
  block. Label values dictionary-encoded.
- Postings lists per `label=value`, intersected/unioned for matchers
  (`=`, `!=`, and — behind the new function syntax — `label_match(host,
  're.*ex')` for regex, kept out of the WHERE fast path).
- **Cardinality protection** (the failure mode that kills TSDBs): per-table
  `max_series` (default 1M/node) rejecting new series past the cap with a
  clear error; `SHOW STATUS` exposes active series, label cardinality top-N,
  and churn rate so the operator sees an explosion before it OOMs a node.

## 6. Query engine

- Planner recognizes a TS table + `ts` range + label predicates and routes to
  the tsdb store: matchers → postings → series set → chunk ranges → decode.
- Aggregation executes **per series first** (rate/increase need it), then
  across series into time buckets — streaming, never materializing raw
  samples for aggregated queries.
- Raw-sample range reads stream through the existing `QueryStream` chunked
  protocol (already shipped) — a day of raw samples never buffers.
- `LIMIT`/`ORDER BY t` come from chunk order (time-sorted by construction).

## 7. Distribution (reuse the ring)

- **Placement**: a series is a unit — placed by `hash(series key)` on the
  existing consistent-hash ring, replicated to RF nodes. This is exactly
  today's PK routing; no new placement machinery.
- **Ingest path**: the coordinator groups a multi-row INSERT / remote_write
  batch by replica set and ships one `TsAppendBatch { table, samples }`
  internode op per peer — one WAL append + head insert per node per batch.
  Default consistency `ONE` with async tail + hinted handoff (metrics favor
  availability; `QUORUM`/`ALL` still selectable per statement). Duplicate
  delivery is idempotent (same `(series, ts, value)` overwrites equal data).
- **Query path**: matchers can hit any shard → broadcast (like vector search
  and `cluster_scan` today), with **partial-aggregate pushdown**: each node
  returns per-bucket partials (`sum+count` for avg, min/max, per-series rate
  segments), the coordinator merges. Raw reads stream node pages through the
  paged-gather machinery.
- **Convergence**: hinted handoff for down replicas; anti-entropy for TS
  tables compares **block/chunk checksums per time window** (cheap — data is
  immutable after flush) instead of per-key merge-join.
- **Resharding**: joins/decommissions migrate whole series (their chunks) for
  reassigned series ids — a new chunk-level path in the existing
  `Rebalance`/`Drain` flow. Until it lands, document that TS tables pin the
  ring (refuse topology changes with TS tables present) — shipped honestly as
  a phase-3 limitation, removed in phase 5.

## 8. Ecosystem compatibility

- **Prometheus `remote_write`** (the adoption lever): `POST /api/v1/write`
  (snappy + protobuf) on the REST listener. Metric name becomes the
  `__name__` label; samples land in one configured TS table (default
  `metrics`, `SERIES KEY` = full label set). Any Prometheus, Grafana Agent,
  or OpenTelemetry collector can then ship to skaidb unchanged.
- **Self-monitoring**: optionally scrape the node's own `/metrics` into a TS
  table (`observability.self_scrape = true`) — dogfooding that doubles as an
  always-on integration test.
- **PromQL subset** (stretch, phase 7): `/api/v1/query_range` evaluating
  instant/range selectors + `rate`/`sum by` over the native query API — enough
  for Grafana's Prometheus datasource. Explicitly time-boxed; SQL is the
  primary interface and remote_write covers ingestion without it.
- Not planned: Influx line protocol, Graphite (adapters exist upstream that
  speak remote_write).

### Grafana integration

Four candidate routes, in recommended order:

1. **Prometheus datasource emulation** (the prize; phase 7): implement the
   Prometheus HTTP query API — `/api/v1/query_range`, `/api/v1/query`, plus
   the cheap metadata endpoints `/api/v1/labels`, `/api/v1/label/<n>/values`,
   `/api/v1/series` (direct reads of the postings index, and what powers
   Grafana's autocomplete). Every existing Prometheus dashboard then works
   unchanged — the VictoriaMetrics adoption path. The PromQL subset needed is
   measurable, not speculative: the top community dashboards
   (node-exporter-full, kubernetes-mixin) are dominated by selectors,
   `rate`/`irate`/`increase`, `sum/avg/min/max by()`, `topk`, and
   `histogram_quantile` (which waits on histograms anyway). Exit criterion:
   load the node-exporter dashboard against skaidb and diff panel-by-panel
   against a real Prometheus scraping the same targets. Grafana **alerting**
   rides on the datasource, so this also delivers alerts without skaidb
   building an alerting engine.
2. **Infinity/JSON datasource recipe** (free, day one of phase 2): skaidb's
   REST `POST /query` already returns `{columns, rows}` JSON; the Infinity
   plugin can POST SQL with `$__from`/`$__to` interpolated. Clunky but real —
   document the recipe for dogfooding and demos.
3. **Native skaidb datasource plugin** (only if SQL becomes the primary query
   story): TypeScript/Go plugin with `$__timeFilter(ts)` macros and label
   pickers. Separate codebase + catalog signing cycle; reaches only users who
   install it, vs route 1's built-in datasource.
4. **PostgreSQL wire emulation** — ruled out: Grafana's pg datasource
   generates PostgreSQL-dialect SQL (casts, `date_trunc`) that skaidb would
   have to chase forever.

Sequencing: metadata endpoints ship early with `remote_write` (phase 4, they
share the ingest mapping); the evaluator is phase 7.

## 9. Retention & downsampling

- `RETENTION <dur>` per table: a background pass drops expired blocks (and
  bumps a "retention watermark" so queries don't see partial windows).
- **Continuous aggregates** (phase 6): `CREATE ROLLUP ON cpu BUCKET '5m'
  AGG avg, max RETENTION 1y` — maintained incrementally at block flush (the
  Thanos/VictoriaMetrics downsampling model: 5m/1h tiers). Queries pick the
  coarsest rollup that satisfies the requested bucket automatically.

## 10. Performance targets & benchmark plan

Targets (workstation NVMe / 1-vCPU fleet node, measured before each phase
ships — same discipline as [BENCHMARKS.md](BENCHMARKS.md)):

| Metric | Target | Reference point |
|---|---|---|
| Storage cost | ≤2 B/sample end-to-end | Prometheus ~1.3 B |
| Ingest, single node | ≥500k samples/s workstation; ≥50k/s fleet node | Prometheus ~1M/s on server cores |
| Range query, 1k series × 1h × 1m buckets | <100 ms single node | |
| Active series per 512 MB node | ≥1M within `memory_target` | Prometheus ~1–3 kB/series head |
| Retention drop | O(1), no ingest stall | |

Benchmarks:
- **TSBS** (Time Series Benchmark Suite, `cpu-only` + `devops`) against
  Prometheus, VictoriaMetrics, and TimescaleDB on the existing 15-LXC fleet —
  the same matched-hardware methodology as the OLTP matrix.
- A `tsbench` mode in the in-tree bench driver for ingest/query
  micro-benchmarks (per-phase A/B without the fleet).
- Follow the methodology rules already recorded in
  [PERFORMANCE_AUDIT.md](PERFORMANCE_AUDIT.md) — scale to target size (≥100M
  samples, ≥1M series), alternating same-day A/Bs, loopback isolation when
  fleet numbers are flat.

## 11. Phased delivery (each phase ships, tested + documented)

1. **Storage core** — `skaidb-tsdb` crate: chunk codec (delta-of-delta + XOR;
   property-tested against a naive store, fuzzed on edge floats), head + WAL
   + replay, block flush/compaction/retention. Exit: 500k samples/s + ≤2 B/s
   sample on workstation, crash-recovery tests green. *(Largest phase.)*
   **✅ Done (v0.20.0).** Measured (workstation NVMe, 10M samples, 10k
   series, one WAL fsync per scrape round, `ts_bench` example): ingest
   2.0–2.7M samples/s; 1.54 B/sample counters, 1.02 B/sample mostly-idle
   gauges, 6.67 B/sample worst-case random-walk gauges (full mantissa
   churn — inherent to Gorilla), 3.08 B/sample on a ⅓/⅓/⅓ mix; 8k-sample
   query over 2 blocks + head in 1.1 ms; reopen replay 0.01 s; 24 unit
   tests including torn-WAL, crash-recovery, no-duplication-after-flush,
   retention, and compaction invariants.
2. **SQL surface, single node** — `CREATE TIMESERIES TABLE`, INSERT routing,
   scalar-function grammar (`time_bucket`, `now`, durations), range SELECT +
   bucketed aggregates + `rate`/`increase`/`first`/`last`, label postings +
   matchers, cardinality caps, SHOW/metrics. Exit: the §3 examples all run;
   QUERY_SYNTAX.md updated.
3. **Cluster** — series placement on the ring, `TsAppendBatch` replication +
   hints, broadcast queries with partial-aggregate pushdown, checksum-based
   anti-entropy. Limitation shipped: topology changes refused with TS tables.
   Exit: 3-node fleet ingest/query correct under node kill + recovery.
4. **remote_write + OOO** — `/api/v1/write` endpoint, `__name__` mapping,
   out-of-order window, staleness markers, self-scrape option. Exit: a real
   Prometheus remote-writing to skaidb for 24 h with zero data loss vs its
   own TSDB.
5. **Resharding for TS tables** — chunk-level migrate/drain, lifting the
   phase-3 limitation. Exit: join + decommission under sustained ingest, all
   samples accounted for.
6. **Downsampling** — `CREATE ROLLUP`, tiered retention, query-time rollup
   selection. Exit: year-long queries hit rollups, not raw.
7. **Stretch: PromQL subset** — `/api/v1/query_range` for Grafana. Re-scope
   or drop based on demand once 1–6 are real.

Each phase ends with the TSBS/fleet comparison for what exists so far,
documented in BENCHMARKS.md (current-notes section, per the docs policy).

## 12. Risks & open questions

- **Second storage engine = permanent maintenance surface.** Mitigation:
  strict crate boundary (`skaidb-tsdb` exposes append/query/flush; the engine
  crate routes), immutable blocks keep the format simple, and the chunk codec
  is the only novel on-disk format (blocks reuse manifest/fsync patterns).
- **Cardinality explosions** are the #1 operational failure of every TSDB —
  caps + observability are in phase 2, not bolted on later.
- **PromQL scope creep** — fenced into phase 7 with an explicit
  re-scope/drop gate.
- **Schema-less tension**: TS tables are the first skaidb tables with
  enforced column roles (labels/ts/numeric fields). Decision: enforce at
  insert with clear errors — correctness over uniformity.
- Open: multi-field vs single-value default table for remote_write
  (proposed: one `metrics` table, single `value` field); whether `DELETE`
  range tombstones are needed in v1 or retention suffices (proposed: defer);
  exemplars/native-histogram encoding (schema bit reserved in the chunk
  header now, implemented later).
