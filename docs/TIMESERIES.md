# Time-series tables

skaidb can store metrics/telemetry natively: a `TIMESERIES` table keeps
samples in a purpose-built storage engine (Prometheus-style compressed
chunks), not the document LSM — with retention, counter-aware SQL
aggregates, and time-bucketed queries.

> Status: **single-node** (cluster mode rejects the DDL until the
> distribution phase lands — see the roadmap at the end). Shipped in
> v0.20.0 (storage engine) and v0.21.0 (SQL surface).

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

**Storage** (`skaidb-tsdb`, measured on workstation NVMe — see
`docs/TODO.md` phase 1 for the full numbers):

- Gorilla compression (delta-of-delta timestamps + XOR floats):
  **1.0–1.5 bytes/sample** on typical fleet patterns (counters, mostly-idle
  gauges); ~6.7 worst-case on full-entropy random walks.
- Ingest ≥**2M samples/s** single node with a WAL fsync per batch.
- Crash recovery: CRC-framed WAL, torn-tail tolerant, checkpointed on flush
  (WAL size tracks the unflushed window, not history).
- Immutable 2 h blocks, 4× tiered compaction, `RETENTION` as O(1)
  whole-block drops.
- Cardinality cap (default 1M series/node) with per-batch accounting of
  out-of-order and over-limit rejections.

**SQL surface:**

- `CREATE TIMESERIES TABLE (SERIES KEY (...) [, RETENTION <dur>])`; plain
  `DROP TABLE`; listed by `SHOW TABLES` with the implicit `(series key, ts)`
  key; survives restart (catalog + WAL replay).
- Duration literals (`250ms`, `15s`, `5m`, `2h`, `30d`, `1w`),
  `time_bucket(step, ts)`, `now()` (one instant per statement).
- Time-series aggregates: `rate(f)` / `increase(f)` (counter-reset-aware,
  per series then summed), `delta(f)`, `first(f)` / `last(f)` — alongside
  the ordinary `COUNT/SUM/AVG/MIN/MAX`.
- Storage pushdown of `AND`-combined `ts` ranges and label `=` / `!=`
  predicates; everything else applies afterward with full SQL semantics.
- The whole ordinary SELECT surface on top: `GROUP BY` (including by output
  alias, `GROUP BY t`), `HAVING`, `ORDER BY`, `DISTINCT`, `LIMIT/OFFSET`,
  multi-field queries, `SELECT *`.
- Append-only semantics enforced: `UPDATE`/`DELETE`/transactions rejected
  with clear errors; reserved `__`-prefixed names blocked.

## What's missing (and where it's planned)

Roadmap phases refer to the implementation plan in [`TODO.md`](TODO.md).

| Gap | Notes | Planned |
|---|---|---|
| **Cluster mode** | DDL rejected on clustered nodes today; series placement on the ring, replicated appends, scatter-gather with partial-aggregate pushdown | phase 3 |
| **Label postings index** | matchers scan the per-block series list (fine at moderate cardinality) | phase 3 |
| **Prometheus `remote_write`** | ingest endpoint + `__name__` mapping; self-scrape option | phase 4 |
| **Out-of-order ingest window** | samples older than a series' last ts are rejected today | phase 4 |
| **Resharding with TS tables** | chunk-level migrate/drain on join/decommission | phase 5 |
| **Downsampling / rollups** | `CREATE ROLLUP`, tiered retention, query-time rollup selection | phase 6 |
| **PromQL subset / Grafana datasource** | `/api/v1/query_range` + metadata endpoints | phase 7 (stretch) |
| Regex label matchers | only `=` / `!=` push down | with phase 3 postings |
| Per-store stats in `SHOW STATUS` / `/metrics` | series counts, disk bytes, rejection counters exist internally | with phase 3 |
| `memory_target` integration | head memory isn't yet part of the storage budget | with phase 3 |
| Streamed TS results | TS SELECT materializes its result before the (streamable) wire; raw range dumps of very large windows should stream end-to-end | later |
| Exemplars / native histograms | schema headroom reserved in the chunk format | later |
