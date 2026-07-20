# Grafana with skaidb

skaidb speaks enough of the Prometheus wire protocol that **Grafana's
built-in Prometheus datasource works against a node directly** — no
exporter, no sidecar. Ingestion comes in over `remote_write`, queries go
out over the Prometheus HTTP query API, both on the REST port (default
7080).

## 1. Point a Prometheus datasource at skaidb (recommended)

In Grafana: *Connections → Data sources → Add data source → Prometheus*:

- **URL**: `http://<node>:7080`
- **Auth**: enable *Basic auth* and use a skaidb account when the node
  runs with authentication (the query API requires `Select` on the
  `metrics` table; a database-level grant satisfies it). Without auth
  enabled, leave it off.

### Scoping a datasource to a database — or to any time-series table

The bare URL serves the `metrics` table in the **default** database. A
path prefix on the datasource URL scopes the whole API (Grafana just
appends `/api/v1/…` to the base URL, so this works with the stock
Prometheus datasource):

- `http://<node>:7080/db/<database>` — that database's `metrics` table
  (same remote_write semantics; `remote_write` to
  `/db/<database>/api/v1/write` ingests there too).
- `http://<node>:7080/db/<database>/table/<table>` — **any time-series
  table**: the table's *fields* become the metric names. E.g. a table
  `air_quality (SERIES KEY (sensor))` with fields `pm25`/`co2` serves
  PromQL like `pm25{sensor="pi1"}` and `rate(co2[5m])`.

The permission check follows the scope: `Select` on that table (a grant
on its database satisfies it) — so a database-scoped account works
against its own data without any grant in the default database. A 403
from the API names the exact table and database it checked.

Grafana health-checks the datasource via `/api/v1/status/buildinfo` and
`/api/v1/metadata` — both answered. Queries hit:

| Endpoint | Purpose |
|---|---|
| `GET/POST /api/v1/query` | instant queries |
| `GET/POST /api/v1/query_range` | dashboard panels |
| `GET /api/v1/labels`, `/api/v1/label/<name>/values` | template variables |
| `GET /api/v1/series` | series lookup |

All of them evaluate over the `metrics` time-series table that
`remote_write` ingests into (the metric name is the `name` label).

### Getting data in

Point any Prometheus-compatible shipper (Prometheus itself, the Grafana
Agent/Alloy, vmagent, …) at the node:

```yaml
# prometheus.yml
remote_write:
  - url: http://<node>:7080/api/v1/write
    basic_auth:            # when the node requires auth
      username: admin
      password: ...
```

Samples land in the auto-created `metrics` table (label columns intact)
and are queryable from SQL too:

```sql
SELECT rate(value) FROM metrics WHERE name = 'http_requests_total' AND job = 'api';
```

### Supported PromQL (v1 subset)

Instant selectors with `=`/`!=`/`=~`/`!~` label matchers (regex forms
anchored, Prometheus-style), `offset`, `rate` / `increase` / `delta` and
`avg/min/max/sum/count/last_over_time` over range selectors (`[5m]` —
the `*_over_time` family is what Grafana's Metrics Drilldown tiles use),
`sum/avg/min/max/count/stddev [by|without (...)]` and `quantile(φ, v)`
(the drilldown's "Standard deviation" / "Percentiles" previews), vector
arithmetic (`+ - * /`, one-to-one matching), number-only expressions (the
`1+1` datasource health check), and `histogram_quantile`. Trailing commas
in matcher blocks are accepted, Prometheus-style. The drilldown's full
query catalog is pinned by the `grafana_promql_compatibility` test. Typical dashboard panels —
`sum by (job) (rate(http_requests_total[5m]))`,
`histogram_quantile(0.9, sum by (le) (rate(req_bucket[5m])))` — work
as-is.

**Not supported (yet)**: subqueries, `group_left`/`group_right`
many-to-one matching, `topk`/`bottomk` and friends. Panels using those
need the fallback below.

### Monitoring skaidb itself

The node's own operational metrics are a Prometheus scrape at
`GET /metrics` (unauthenticated, gauge names under `skaidb_*`) — including
per-node **host system stats** (`skaidb_host_*`: CPU%, cgroup-aware memory,
process RSS, disk IO counters, data-dir disk space; see
[METRICS.md](METRICS.md)), so basic host dashboards need no separate
node_exporter. Scrape it with your regular Prometheus and dashboard it
like any other target — or `remote_write` that Prometheus back into skaidb
and dashboard skaidb from skaidb. There is also a built-in web UI with
live stats (incl. a per-node CPU/RAM/disk table) at
`http://<node>:7080/ui` ([UI.md](UI.md)).

## 2. Fallback: SQL over REST (Infinity / JSON API datasource)

For queries outside the PromQL subset — or any non-timeseries table — use
a JSON-over-HTTP datasource (e.g. the *Infinity* plugin) against the SQL
gateway:

- **Method**: POST, **URL**: `http://<node>:7080/query`
- **Body**: the SQL, either raw text or `{"sql": "SELECT ...", "db": "mydb"}`
- **Auth**: HTTP Basic, same accounts as everything else
- **Response shape**: `{"columns": [...], "rows": [[...], ...]}` — in
  Infinity set *Format: table*, *Rows selector*: `rows`, and map columns
  by index.

Time-series SQL (`docs/TIMESERIES.md`) gives you windowed aggregates the
PromQL subset lacks:

```sql
SELECT time_bucket(1m, ts) AS t, avg(value)
FROM metrics
WHERE name = 'http_requests_total' AND ts >= now() - 1h
GROUP BY t ORDER BY t;
```

## Notes

- One request per connection (`Connection: close`): fine at dashboard
  refresh rates.
- TLS: terminate at a proxy in front of the REST port; Basic auth wants
  TLS on untrusted networks.
- Timestamps follow the Prometheus HTTP API conventions (float seconds;
  sample values as strings) — Grafana handles this natively.
