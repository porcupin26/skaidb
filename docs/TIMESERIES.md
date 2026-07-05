# Time-series tables

skaidb can store metrics/telemetry natively: a `TIMESERIES` table keeps
samples in a purpose-built storage engine (Prometheus-style compressed
chunks), not the document LSM — with retention, counter-aware SQL
aggregates, and time-bucketed queries.

> Status: **distributed** — series place on the consistent-hash ring and
> replicate at the configured write consistency; queries union-merge across
> members at the read consistency; joins/decommissions migrate series like
> any other data. Shipped in v0.20.0 (storage), v0.21.0 (SQL), v0.22.0
> (cluster), v0.23.0 (remote_write), v0.24.0 (resharding), v0.25.0 (OOO DDL + stats).

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
- Duration literals (`250ms`, `15s`, `5m`, `2h`, `30d`, `1w`),
  `time_bucket(step, ts)`, `now()` (one instant per statement).
- Time-series aggregates: `rate(f)` / `increase(f)` (counter-reset-aware,
  per series then summed), `delta(f)`, `first(f)` / `last(f)` — alongside
  the ordinary `COUNT/SUM/AVG/MIN/MAX`.
- Storage pushdown of `AND`-combined `ts` ranges and label `=` / `!=`
  predicates; everything else applies afterward with full SQL semantics.
- **Prometheus `remote_write`** (v0.23.0): `POST /api/v1/write` on the REST
  listener (HTTP Basic auth like `/query`) accepts snappy-compressed
  protobuf WriteRequests from any Prometheus / Grafana Agent / OTel
  collector. Samples land in a `metrics` TS table (auto-created on first
  write, `SERIES KEY (name)`); `__name__` maps to the `name` label, other
  labels pass through — and **any** label equality in SQL pushes down to the
  store, so `WHERE name = '...' AND instance = '...'` is efficient without
  declaring every label. In a cluster, ingested samples replicate through
  the same series-placement path as SQL INSERTs.
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

## What's missing (and where it's planned)

Roadmap phases refer to the implementation plan in [`TODO.md`](TODO.md).

| Gap | Notes | Planned |
|---|---|---|
| **TS anti-entropy / hints** | a replica down during a write stays missing those samples until re-written; reads stay correct via union-merge at quorum, but there is no background repair or hinted handoff for TS data yet (block-checksum repair planned) | phase 3 follow-up |
| **Partial-aggregate pushdown** | cluster queries ship matching raw samples to the coordinator; per-node partial aggregation (sum/count per bucket, per-series rate segments) would cut transfer for wide aggregations | phase 3 follow-up |
| Self-scrape (`/metrics` → TS table) | remote_write covers external scrapers | later |
| TS reclaim | after a reshard, former owners keep stale series copies (harmless under union-merge; no `reclaim` pass for TS yet) | with TS anti-entropy |
| **Downsampling / rollups** | `CREATE ROLLUP`, tiered retention, query-time rollup selection | phase 6 |
| **PromQL subset / Grafana datasource** | `/api/v1/query_range` + metadata endpoints | phase 7 (stretch) |
| Label postings index | matchers scan the per-block series list (fine at moderate cardinality) | with pushdown work |
| Regex label matchers | only `=` / `!=` push down | with postings |
| TS gauges on `/metrics` | per-store stats are in `SHOW STATUS`; Prometheus-endpoint gauges pending | soon |
| `memory_target` integration | head memory isn't yet part of the storage budget | soon |
| Streamed TS results | TS SELECT materializes its result before the (streamable) wire; raw range dumps of very large windows should stream end-to-end | later |
| Exemplars / native histograms | schema headroom reserved in the chunk format | later |
