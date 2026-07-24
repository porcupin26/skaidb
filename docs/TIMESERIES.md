# Time-series tables

skaidb can store metrics/telemetry natively: a `TIMESERIES` table keeps
samples in a purpose-built storage engine (Prometheus-style compressed
chunks), not the document LSM — with retention, counter-aware SQL
aggregates, and time-bucketed queries.

> Status: **distributed** — series place on the consistent-hash ring and
> replicate at the configured write consistency; queries union-merge across
> members at the read consistency; joins/decommissions migrate series like
> any other data. Shipped in v0.20.0 (storage), v0.21.0 (SQL), v0.22.0
> (cluster), v0.23.0 (remote_write), v0.24.0 (resharding), v0.25.0 (OOO DDL + stats), v0.26.0 (anti-entropy), v0.27.0 (rollups), v0.28.0 (PromQL API), v0.30.0 (hinted handoff), v0.31.0 (partial-aggregate pushdown), v0.32.0 (rollup query rewrite).

## Usage

```sql
CREATE TIMESERIES TABLE cpu (SERIES KEY (host, core), RETENTION 30d);

INSERT INTO cpu (host, core, ts, value)
  VALUES ('web1', '0', 1712000000000, 0.63), ('web1', '1', 1712000000000, 0.41);

-- Time-bucketed aggregation over the last hour:
SELECT time_bucket(1m, ts) AS t, host, avg(value), max(value)
FROM cpu WHERE ts >= now() - 1h AND host = 'web1'
GROUP BY t, host ORDER BY t;

-- Counter rate (reset-aware, per series, summed — sum(rate(...)) semantics):
SELECT time_bucket(5m, ts) AS t, rate(value)
FROM http_requests_total WHERE ts >= now() - 6h GROUP BY t;

-- Raw samples:
SELECT ts, value FROM cpu WHERE host = 'web1' AND ts >= now() - 5m ORDER BY ts;
```

Column roles: `SERIES KEY` columns are string **labels** (required on every
insert); `ts` is the sample timestamp (required, ms); every other inserted
column is a numeric **field** — multiple fields per row are fine, each is
stored as its own compressed stream. Full grammar and semantics:
[`QUERY_SYNTAX.md`](QUERY_SYNTAX.md#time-series-tables).

## What's implemented

**Storage** (`skaidb-tsdb`, measured on workstation NVMe):

- Gorilla compression (delta-of-delta timestamps + XOR floats):
  **1.0–1.5 bytes/sample** on typical fleet patterns (counters, mostly-idle
  gauges); ~6.7 worst-case on full-entropy random walks.
- Ingest ≥**2M samples/s** single node with a WAL fsync per batch.
- Crash recovery: CRC-framed WAL, torn-tail tolerant, checkpointed on flush
  (WAL size tracks the unflushed window, not history).
- Immutable 2 h blocks, 4× tiered compaction, `RETENTION` as O(1)
  whole-block drops.
- Cardinality cap (default 1M series/node) with per-batch accounting of
  out-of-order and over-limit rejections; per-table
  `timeseries.<name>.{series,blocks,samples_appended,samples_rejected,disk_bytes}`
  in `SHOW STATUS`.

**SQL surface:**

- `CREATE TIMESERIES TABLE (SERIES KEY (...) [, RETENTION <dur>]
  [, OOO <dur>])` — `OOO` sets a bounded out-of-order ingest window
  (buffered per series, merged in time order; the remote_write `metrics`
  table auto-creates with `OOO 1h` for HA Prometheus pairs); plain
  `DROP TABLE`; listed by `SHOW TABLES` with the implicit `(series key, ts)`
  key; survives restart (catalog + WAL replay).
- **`ALTER TABLE <ts> SET (retention = <dur> | ooo = <dur>)`** — both are
  live-tunable, no create-new/backfill/swap: retention changes apply at the
  next flush (widening cannot resurrect already-dropped blocks; `0` clears
  retention), the OOO window applies to subsequent inserts. Temporarily
  widening `ooo` is the supported way to backfill history into a table
  that already takes live writes.
- **INSERT reports dropped points.** Samples older than a series' OOO
  window are discarded per sample, and the INSERT's `affected` count
  reflects only what landed — `{"affected": 0}` when every point was late
  (one count per numeric field when rows carry several; full-success
  inserts keep reporting the row count). Compare `affected` with what you
  sent to detect loss; `timeseries.<t>.samples_rejected` in `SHOW STATUS`
  tracks the cumulative total per node.
- Duration literals (`250ms`, `15s`, `5m`, `2h`, `30d`, `1w`),
  `time_bucket(step, ts)`, `now()` (one instant per statement).
- Time-series aggregates: `rate(f)` / `increase(f)` (counter-reset-aware,
  per series then summed), `delta(f)`, `first(f)` / `last(f)` — alongside
  the ordinary `COUNT/SUM/AVG/MIN/MAX`.
- Storage pushdown of `AND`-combined `ts` ranges and label `=` / `!=`
  predicates; everything else applies afterward with full SQL semantics.
- **Label-DISTINCT serves from series metadata**: `SELECT DISTINCT
  <series-key columns> FROM <ts>` (optionally label-filtered/ordered)
  answers from the store's series label sets — no sample materialization,
  no scan-budget exposure, regardless of point count. A time (`ts`)
  constraint forces the sample path (label sets are all-time).
- **Prometheus `remote_write`** (v0.23.0): `POST /api/v1/write` on the REST
  listener (HTTP Basic auth like `/query`) accepts snappy-compressed
  protobuf WriteRequests from any Prometheus / Grafana Agent / OTel
  collector. Samples land in a `metrics` TS table (auto-created on first
  write, `SERIES KEY (name)`); `__name__` maps to the `name` label, other
  labels pass through — a series' OWN `name` label renames to
  `exported_name` (the Prometheus collision convention) so it cannot
  clobber the metric name — and **any** label equality in SQL pushes down to the
  store, so `WHERE name = '...' AND instance = '...'` is efficient without
  declaring every label. In a cluster, ingested samples replicate through
  the same series-placement path as SQL INSERTs.
- **Prometheus query API / Grafana** (v0.28.0): point Grafana's built-in
  Prometheus datasource at skaidb's REST listener. `/api/v1/query` and
  `/api/v1/query_range` evaluate a PromQL subset — instant selectors with
  `=`/`!=` matchers, `rate`/`increase`/`delta` and
  `avg/min/max/sum/count/last_over_time` over range selectors
  (counter-reset-aware where applicable, matching the SQL aggregates), and
  `sum/avg/min/max/count [by|without (...)]` — plus **regex matchers**
  (`=~`/`!~`, anchored like Prometheus), **`offset`**, **vector
  arithmetic** (`+ - * /`; scalar∘vector and one-to-one vector∘vector on
  identical label sets), and **`histogram_quantile`** — over the
  remote_write `metrics` table. `/api/v1/labels`,
  `/api/v1/label/<n>/values`, `/api/v1/series`, buildinfo and metadata
  stubs power Grafana's autocomplete. On a cluster, regex matchers are
  applied by the coordinator (peers answer the equality-matched
  superset). A fresh datasource with no ingest sees empty
  results, not errors. Datasource setup recipes: [GRAFANA.md](GRAFANA.md).
- **Raw dumps are scan-metered** (v0.91; metered at the source since
  v0.139): a raw `SELECT` over a time-series table charges each gathered
  sample against the statement's scan budget, exactly like row-table
  gathers — an unbounded dump over a huge range fails with the budget
  error instead of materializing until the coordinator OOMs. The charge
  now lands *inside* the store walk (and per peer response on a
  cluster), so an over-budget gather aborts as it reads instead of after
  the whole result sits resident at the coordinator. Narrow the time
  range or aggregate (aggregations push down as bounded per-bucket
  partials and are unaffected). Note `ts` bounds are epoch
  **milliseconds** — a bound accidentally supplied in epoch *seconds*
  reads as ~1970, unbounding the walk (the classic symptom: a
  narrow-window query dying on the scan budget).
- **`COUNT(*)` over an empty selection returns 0** (v0.139): the
  partials plan folds COUNT into a SUM of per-bucket counts, and SQL SUM
  over zero rows is NULL — the fold now coalesces to 0, so an empty time
  window or non-matching filter counts as 0 like every SQL COUNT.
- **LIMIT'd raw reads page efficiently**: `WHERE ts > <cursor> ORDER BY
  ts LIMIT n` (or `DESC` with an upper bound) walks the range in time
  slices and stops as soon as `n` rows survive the filter — each page
  costs ~its own rows, so exporting a table of any size is a linear
  keyset-pagination loop instead of a quadratic re-scan (and pages never
  trip the budget on their own). `COUNT(*)` on single-field tables (all
  remote_write tables) serves from per-bucket partials the same way
  aggregates do.
- **Rollups / downsampling** (v0.27.0): `CREATE ROLLUP r30m ON cpu BUCKET
  30m RETENTION 90d` — per-bucket partials (`<field>_{count,sum,min,max,
  first,last}`) maintained automatically at window flush and queryable as a
  normal TS table with the same labels. Each replica maintains its rollups
  locally: a rollup series has the same labels as its source, so it places
  on the same replica set by construction. Long retention on the rollup +
  short on the source = classic tiered downsampling.
- **Rollup query rewrite** (v0.32.0): aggregate queries on the **source**
  table keep answering after raw samples age out — buckets older than the
  source's `RETENTION` horizon are served from the coarsest rollup whose
  bucket divides the group's `time_bucket` step, stitched seamlessly with
  exact source partials for the within-retention part of the window.
  Covers `count/sum/avg/min/max/first/last`; `rate`-family aggregates need
  raw samples and never read rollups. **Within retention** (single-node):
  group buckets wholly below the head's oldest sample also serve from the
  rollup — the backfill above keeps them exact, so this is the same
  numbers with less raw IO; the bucket straddling the head boundary stays
  on the source. Clustered deployments keep retention-only routing (a
  peer's head may lag; extending needs a min-over-replicas boundary
  exchange).
- **Partial-aggregate pushdown** (v0.31.0): an aggregation whose `WHERE`
  is fully served by the pushdown (a `ts` range plus label `=`/`!=`),
  grouping by labels and/or one `time_bucket`, ships **per-series
  per-bucket partials** (count/sum/min/max/first/last/increase) from each
  member instead of raw samples, and answers each `(series, bucket)` from
  the responder that saw the most samples for it. All the supported
  aggregates — `count/sum/avg/min/max/first/last/rate/increase/delta`,
  `HAVING`, `ORDER BY`, `LIMIT` included — fold from the partials with
  identical semantics (equivalence-tested against the raw path). Cuts
  coordinator transfer ~RF× and more for wide Grafana-style aggregations;
  anything ineligible (residual predicates, `COUNT(*)`, computed aggregate
  arguments) transparently uses the raw union-merge path. The PromQL
  endpoint still gathers raw samples (its lookback windows aren't
  bucket-aligned).
- **Hinted handoff** (v0.30.0): a replica unreachable during an append
  gets its batch buffered on the coordinator (bounded per peer) and
  replayed via the gap-filling merge as soon as it's reachable — brief
  outages recover in seconds; anti-entropy repair remains the durable
  backstop for anything the bounded buffer dropped.
- **Anti-entropy** (v0.26.0): `repair()` (the periodic pass and
  `cluster repair`) converges TS replicas — per-series `(count, checksum)`
  summaries are compared per peer, and the series' elected sender pushes
  divergent series via a merge path that accepts samples of any age (fills
  mid-series gaps a normal append would reject). Duplicate chunks the merge
  creates fold away at the next compaction. A long-down replica now
  converges durably, not just at read time. Merge ingest folds its own
  backlog: each merge call cuts a level-0 block, and once a store holds
  more than a small backlog of blocks a bounded compaction round runs
  inline (capped group size, wall-clock budget), so hint-replay storms
  can't grow the block count without bound. Hinted TS writes coalesce
  per table into one merge per drain pass. Retention/compaction are
  best-effort maintenance: a maintenance failure surfaces in stats
  (`maintenance_errors`) and retries next flush — it never fails the
  client append that triggered it. Block sequence numbers skip any
  directory already on disk, so residue from an interrupted block write
  can't wedge later flushes.
- **Cluster distribution**: TS DDL broadcasts like other DDL; a series (its
  labels) is the placement unit on the ring, replicated to RF nodes — all of
  a series' field streams co-locate. Appends group per replica set (one
  internode batch per replica, per-sample idempotent on replay) and ack at
  the write consistency. Queries broadcast matchers + range to all members
  and **union-merge** per series (samples are immutable facts keyed by
  timestamp, so any responder holding a sample covers a replica that missed
  it), requiring the read-consistency responder count.
- The whole ordinary SELECT surface on top: `GROUP BY` (including by output
  alias, `GROUP BY t`), `HAVING`, `ORDER BY`, `DISTINCT`, `LIMIT/OFFSET`,
  multi-field queries, `SELECT *`.
- Append-only semantics enforced: `UPDATE`/`DELETE`/transactions rejected
  with clear errors; reserved `__`-prefixed names blocked.

## Compatibility note

The shipped PromQL subset covers selectors, `rate`/`increase`/`delta`, and
`sum`/`avg`/`min`/`max`/`count` `by`/`without`; label matchers push down `=`/`!=`
(regex matchers re-check against the full population). Label-postings selection
shipped in v0.92.
