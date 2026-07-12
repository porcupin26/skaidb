# skaidb — complete reference for LLM agents

This single file contains everything an LLM needs to manage a skaidb
deployment and to write software that fully uses it. It is self-contained;
the per-topic docs (SEARCH.md, TIMESERIES.md, VECTOR.md, CLUSTERING.md,
GRAFANA.md, UI.md, QUERY_SYNTAX.md) go deeper but are not required.

**What skaidb is:** a distributed, schema-less, SQL-speaking database in a
single static binary. One engine serves relational rows (LSM storage),
full-text search (embedded Tantivy, BM25), time-series (Gorilla-compressed
samples with PromQL), and vector search (HNSW). Leaderless replication on a
consistent-hash ring; quorum reads/writes, hinted handoff, read repair,
anti-entropy, online resharding. Two binaries ship: `skaidb` (server) and
`skaidbsh` (network SQL shell + admin client).

---

## 1. Connecting

| Surface | Default port | Auth | Use for |
|---|---|---|---|
| Binary protocol (drivers, `skaidbsh`) | 7000 | SCRAM | sessions, prepared statements, pipelining, streaming |
| REST `POST /query` | 7080 | HTTP Basic | one-shot SQL from anything that speaks HTTP |
| ES-compatible REST subset | 7080 | HTTP Basic | existing ES clients / log shippers |
| Prometheus `remote_write` + `/api/v1/*` | 7080 | HTTP Basic | metrics ingest + Grafana |
| Web UI `/ui` | 7080 | HTTP Basic (login form) | humans: status, SQL console, stats, config, admin |
| `GET /metrics`, `/health`, `/ready`, `/status` | 7080 | none | probes/scrapers (read-only, secret-free) |
| Internode | 7100 | configurable | cluster traffic (not for clients) |

```bash
# SQL over REST — body is plain SQL, or JSON with an optional session db:
curl -u user:pass -X POST http://node:7080/query -d "SELECT * FROM t LIMIT 5"
curl -u user:pass -X POST http://node:7080/query \
     -d '{"sql":"SELECT * FROM t","db":"mydb"}'
# Response: {"columns":[...],"rows":[[...],...]} | {"affected":n} | {"ok":true}
# | {"error":"..."} (HTTP 400)

# Bulk JSON document upsert (overwrites on primary key). Optional
# "consistency": "one" | "quorum" | "all" overrides the write default for
# this request — bulk loaders use "one" so the ack never waits on the
# slowest replica (replication still reaches every replica via the async
# tail; hints + anti-entropy backstop).
curl -u user:pass -X POST http://node:7080/insert \
     -d '{"db":"mydb","table":"t","rows":[{"id":"a","x":1}],"consistency":"one"}'
# Response: {"inserted":n} | {"error":"..."}

# Shell (nearest-node selection, failover, discovers peers via /status):
skaidbsh --host node --port 7000 --rest-port 7080 [--user u --password p]
```

Key facts an agent must know:
- **One statement per call.** No multi-statement bodies. `;` is allowed as a
  terminator but does not chain statements.
- **The REST gateway is stateless**: `USE db` does not persist between
  calls. Pass `{"sql": ..., "db": "mydb"}` per request, or qualify names
  (`mydb.orders`). Binary-protocol sessions do keep `USE` state.
- **String literals use single quotes** (`'ada'`, escape by doubling:
  `'O''Brien'`). **Double quotes are identifiers** (`"weird name"`). Sending
  `"text"` where a string is expected is a common LLM error.
- Rows are schema-less documents: any field not present reads as `NULL`;
  there is no column DDL to manage.

---

## 2. Data model, types, expressions

- **Table** = documents keyed by a declared primary key (single or
  composite). `CREATE TABLE t (PRIMARY KEY (id))` is the whole schema.
- **Types** (dynamic): `null`, `bool`, `int64`, `float64`, `decimal`,
  `string`, `bytes`, `uuid`, `timestamp` (Unix ms), `array`, `document`
  (nested). SQL literals exist for int, float, string, bool, null, and
  constant arrays (`[0.1, -0.2]`); the other types arrive via stored data.
- **Paths**: dotted paths reach nested fields everywhere —
  `address.city`, in projections, WHERE, GROUP BY, ORDER BY, UPDATE SET,
  and index declarations.
- **Duration literals**: `250ms 15s 5m 2h 30d 1w` — integers in ms, usable
  wherever an integer is (`WHERE ts >= now() - 1h`).
- **Operators** (rising precedence): `OR`; `AND`; `NOT`; comparisons
  `= != <> < <= > >=`; `IS [NOT] NULL`; `+ -`; `* /`; unary `-`; parens.
  Three-valued logic: `NULL` comparisons are unknown.
- **Scalar functions**: `now()` (statement start, timestamp),
  `time_bucket(step, ts)` (floor to bucket: `time_bucket(5m, ts)`).
- **Aggregates**: `COUNT(*)`, `COUNT(expr)`, `COUNT(DISTINCT expr)` (exact),
  `APPROX_COUNT_DISTINCT(expr)` (opt-in HLL on the search pushdown, exact
  everywhere else), `SUM`, `AVG`, `MIN`, `MAX`; time-series only: `RATE`,
  `INCREASE`, `DELTA`, `FIRST`, `LAST`.
- **Bind parameters**: `?` in prepared `SELECT/INSERT/UPDATE/DELETE`
  (binary protocol / drivers). Not on the one-shot REST path.

**Not in the language**: subqueries, CTEs, window functions, `FULL OUTER
JOIN`, `INTERSECT`/`EXCEPT`, `ADD/DROP COLUMN` (schema-less), `ORDER BY
embedding <-> [..]` (use `NEAREST`).

---

## 3. Statement reference

```sql
-- DDL
CREATE TABLE [IF NOT EXISTS] t (PRIMARY KEY (col [, col ...])) [WITH (ttl = dur)]
--   ttl: rows expire <dur> after their last write — immediately invisible to
--   every read; space reclaimed lazily by compaction. Converges at any RF.
DROP TABLE [IF EXISTS] t                    -- cascades to the table's
--   secondary/search/vector indexes
ALTER TABLE t RENAME TO t2
ALTER TABLE t RENAME COLUMN a TO b          -- rewrites rows, rebuilds indexes
CREATE INDEX [IF NOT EXISTS] i ON t (path [, path ...])   -- composite = leftmost-prefix
DROP INDEX [IF EXISTS] i
CREATE VECTOR INDEX [IF NOT EXISTS] v ON t (path) DIM n [USING cosine|l2|dot]
DROP VECTOR INDEX [IF EXISTS] v
ALTER VECTOR INDEX v SET (ef = n)           -- live recall/latency tuning (persisted);
--   build-time knobs (m, ef_construction, dim, metric) need a rebuild
CREATE SEARCH INDEX [IF NOT EXISTS] s ON t (path [, ...]) [WITH (opts)]
DROP SEARCH INDEX [IF EXISTS] s
REBUILD SEARCH INDEX s
ALTER SEARCH INDEX s SET (opts)             -- query-time opts only, live
CREATE TIMESERIES TABLE [IF NOT EXISTS] t
       (SERIES KEY (label [, ...]) [, RETENTION dur] [, OOO dur])
CREATE ROLLUP [IF NOT EXISTS] r ON ts_table BUCKET dur [RETENTION dur]

-- DML (UPDATE/DELETE rejected on time-series tables — append-only)
INSERT INTO t (col, ...) VALUES (expr, ...) [, (...), ...]
UPDATE t SET path = expr [, ...] [WHERE expr]
DELETE FROM t [WHERE expr]

-- Query
SELECT [DISTINCT] item [, ...] FROM t [[AS] a]
  [JOIN ...] [NEAREST (path, [vector], k)] [WHERE expr]
  [GROUP BY expr [, ...] [TOP k BY expr [ASC|DESC]]] [HAVING expr]
--   GROUP BY ... TOP k BY e: per-group top-k ROWS (not aggregates); with
--   MATCH + TOP k BY score() it is ES top_hits in SQL
  [{UNION | UNION ALL} SELECT ...]
  [ORDER BY expr [ASC|DESC] [, ...]] [LIMIT n] [OFFSET n]
-- joins: [INNER|LEFT [OUTER]|RIGHT [OUTER]|CROSS] JOIN t [AS a] [ON expr]
--   equi-joins hash-join; qualify columns by alias (u.id). No cluster
--   join pushdown: joins pull both tables to the coordinator — fine for
--   lookups, wrong for large fact-to-fact joins.

-- Search-specific statements
SUGGEST 'text' ON index [COLUMN col] [LIMIT n]   -- "did you mean" terms
EXPLAIN SCORE <select> FOR <pk-literal>          -- per-row BM25 breakdown
EXPLAIN <statement>                              -- plan inspection: access path,
--   pushdown/fallback decisions, cluster fan-out — advisory, never executes

-- Databases (namespaces; `default` always exists and cannot be dropped)
CREATE DATABASE [IF NOT EXISTS] d
DROP DATABASE [IF EXISTS] d
USE d                                            -- session-state (binary protocol)
SHOW DATABASES

-- Transactions — EMBEDDED ENGINE ONLY (cluster autocommits, rejects these)
BEGIN | COMMIT | ROLLBACK

-- Users, roles, grants
CREATE USER [IF NOT EXISTS] u PASSWORD 'pw'
ALTER USER u PASSWORD 'pw'
DROP USER [IF EXISTS] u
CREATE ROLE [IF NOT EXISTS] r  |  DROP ROLE [IF EXISTS] r
GRANT  privilege ON { table | DATABASE db | * } TO r
REVOKE privilege ON { table | DATABASE db | * } FROM r
GRANT ROLE r TO u  |  REVOKE ROLE r FROM u
SHOW GRANTS [FOR r]

-- Introspection (no privilege needed; names only, no data)
SHOW TABLES        -- (table, primary_key)
SHOW INDEXES       -- (index, table, kind, columns)
SHOW STATUS        -- (metric, value): disk/memtable/wal/cache/compactions,
                   -- per-table table.<db>.<table>.*, per-index search.<name>.*
SHOW DATABASES

-- Admin statements (SQL spellings of the HTTP admin surface; need ADMIN
-- on * and share its RBAC + audit path — a SQL-only client is fully
-- self-sufficient)
SHOW CLUSTER                        -- ring/peers/liveness as (key, value) rows
SHOW CONFIG [LIKE 'pat%']           -- full config flattened to dotted keys, masked
SET CONFIG section.key = literal    -- live-mutable keys apply instantly
SHOW SLOW QUERIES [LIMIT n]         -- slow-query log (masked SQL)
REPAIR CLUSTER                      -- anti-entropy pass
RECLAIM                             -- drop keys/series this node no longer owns
ALTER CLUSTER ADD NODE 'host:7100'  -- online resharding
ALTER CLUSTER REMOVE NODE 'id'

-- Backup / restore (paths are server-side, on the answering node)
BACKUP TO 'path'      -- crash-consistent copy of this node's data dir
                      -- (per-shard on a cluster); refuses to overwrite
RESTORE FROM 'path'   -- embedded/standalone only — on a cluster stop the
                      -- node, restore its dir offline, let repair converge

-- Session consistency (binary protocol only; overrides the per-request
-- value until changed. REST is stateless and rejects it with guidance.)
SET CONSISTENCY ONE | QUORUM | ALL
```

**RBAC**: privileges are `SELECT INSERT UPDATE DELETE CREATE DROP GRANT
ADMIN`. `ADMIN ON *` = superuser; a database grant covers its tables; a
user acts as its own-named role and inherits granted roles. Management
statements need `GRANT`; `SHOW GRANTS FOR <own role>` is always allowed.
`remote_write` needs `INSERT` and `/api/v1/query*` need `SELECT` on the
`metrics` table. Admin HTTP endpoints need `ADMIN` on `*`.

**Indexing**: predicates on indexed columns (`=`, ranges, AND-combined) and
matching `ORDER BY` accelerate; everything else scans with identical
results. The primary key routes in a cluster: `WHERE pk = v` is a point
read to the key's replica set.

---

## 4. Full-text search

```sql
CREATE SEARCH INDEX articles_fts ON articles (title, body, year, published)
  WITH (analyzer = 'english', refresh_ms = 1000,
        title.boost = 2.0, title.keyword = true,
        title.copy_to = 'everything', body.copy_to = 'everything',
        year.type = 'long', published.type = 'bool');

SELECT id, title, score(), HIGHLIGHT(body, 120) AS snippet FROM articles
WHERE MATCH(body, 'quick brown fox') AND published = true
ORDER BY score() DESC LIMIT 10;
```

**Index options** — global: `analyzer` (default `'standard'` = UAX§29 +
lowercase; `'folding'`, `'whitespace'`, `'keyword'`, `'ngram(min,max)'`,
`'edge_ngram(min,max)'`, languages `'english'`, `'german'`, … with
stopwords+stemming), `refresh_ms` (NRT visibility, default 1000; `0` =
commit every write), `synonyms` (`'quick,fast; new york,nyc'` — multi-word
entries match as phrases, both directions, hot-reloadable via `ALTER`).
Per-column: `<col>.type` (`text` default, `keyword`, `long`, `double`,
`bool`, `date` — typed columns become fast fields queryable in `SEARCH()`
ranges and usable for sorting/facets), `<col>.analyzer`,
`<col>.search_analyzer`, `<col>.boost`, `<col>.keyword = true` (adds an
exact-match `.keyword` twin), `<col>.copy_to` ("search everything" field).

**Predicates** (compose with AND/OR/NOT among themselves; ordinary
conditions join at top level with AND and filter afterward; mixing under
OR/NOT is rejected):
- Analyzed: `MATCH(col,'text')`, `MATCH_PHRASE(col,'text'[,slop])`,
  `FUZZY(col,'text'[,dist])` (≤2), `SEARCH('query-string')` (mini-language:
  `term "phrase" col:term +must -must_not AND OR year:[2020 TO 2024]`),
  `MATCH_CROSS(col, col, ..., 'text')` (term-centric multi-field,
  ES cross_fields), `MATCH_BEST(col, col, ..., 'text')` (field-centric
  dis-max over an explicit column subset, ES best_fields — a row scores
  as its best single field).
- Pattern (NOT analyzed — lowercase them under lowercasing analyzers):
  `MATCH_PREFIX(col,'pre')`, `WILDCARD(col,'qu*ck')`, `REGEXP(col,'...')`.
- `MORE_LIKE_THIS(col, 'like text')` — similar rows.
- `BOOSTED(required, optional, ...)` — `required` decides matches, each
  `optional` only raises scores (ES must+should).
- `score()` projects BM25 (also injected as `_score`); `ORDER BY score()
  DESC LIMIT k` is the pushed top-k (LIMIT required, DESC only).
  `ORDER BY <fast column> LIMIT k` also pushes down.
- `HIGHLIGHT(col [, max_chars])` — snippet with `<b>…</b>` marks.
- Per-group top documents: `SELECT region, title, score() FROM t WHERE
  MATCH(title, 'q') GROUP BY region TOP 3 BY score()` — each group's 3
  best-scoring rows (ES `top_hits` equivalent).
- Aggregations work over search (`GROUP BY region`, `COUNT/SUM/AVG/MIN/
  MAX`, `time_bucket` date histograms, `COUNT(DISTINCT)` exact,
  `APPROX_COUNT_DISTINCT` sketch) — exact fast-field pushdown or exact
  row fallback, never approximated silently.
- Diagnostics: `EXPLAIN <statement>` (plan rows: access path, pushdown
  vs. fallback, cluster fan-out — advisory, never executes);
  `EXPLAIN SCORE SELECT ... WHERE MATCH(...) FOR <pk>`
  (BM25 breakdown JSON; works at any RF — routed to a replica of the key);
  `SUGGEST 'levensthein' ON idx` (typo suggestions).

**Semantics to remember**: the table is the source of truth — indexes are
derived and rebuild automatically if lost/torn/mismatched. Writes become
searchable within `refresh_ms` (+ a 200 ms server tick); the writing
session sees its own writes immediately. Multi-field scoring is dis-max.
Distributed: relevance top-k scatters and merges at any RF; aggregations
and fast-field sorted top-k use sharded partials on RF < members clusters
(each node aggregates only key-space it primarily owns), falling back to
an exact row gather when anything wobbles.

---

## 5. Time-series

```sql
CREATE TIMESERIES TABLE cpu (SERIES KEY (host, core), RETENTION 30d, OOO 10m);
INSERT INTO cpu (host, core, ts, value) VALUES ('web1', '0', 1712000000000, 0.63);

SELECT time_bucket(1m, ts) AS t, host, avg(value), max(value)
FROM cpu WHERE ts >= now() - 1h AND host = 'web1' GROUP BY t, host ORDER BY t;

SELECT time_bucket(5m, ts) AS t, rate(value) FROM cpu
WHERE ts >= now() - 6h GROUP BY t;
```

- `SERIES KEY` columns are string labels (all required per insert); `ts` is
  required (timestamp or int ms, increasing per series unless within the
  `OOO` window; equal ts = last-write-wins); other columns are numeric
  fields. Append-only: UPDATE/DELETE rejected; `RETENTION` expires blocks.
- `rate/increase/delta` are counter-reset-aware, computed per series then
  summed across the group (PromQL `sum(rate(...))` semantics); `first/last`
  take the earliest/latest value. GROUP BY/ORDER BY may reference output
  aliases (`GROUP BY t`).
- **Rollups**: `CREATE ROLLUP r30m ON cpu BUCKET 30m RETENTION 90d` stores
  `f_count/f_sum/f_min/f_max/f_first/f_last` per bucket, auto-maintained on
  flush AND on repair backfill. Aggregate queries on the source
  automatically answer from rollups past the retention horizon (and, on a
  single node, for any fully-flushed window) — `rate`-family always needs
  raw samples. Query rollups directly like any TS table.
- **Prometheus**: `remote_write` at `POST /api/v1/write` ingests into the
  auto-created `metrics` table (metric name = `name` label).
  `/api/v1/query`, `/query_range`, `/labels`, `/label/<n>/values`,
  `/series`, buildinfo/metadata serve Grafana's built-in Prometheus
  datasource. PromQL subset: selectors with `= != =~ !~` (regex anchored),
  bare `{name=~"..."}` selectors, `offset`, `rate/increase/delta[5m]`,
  `sum/avg/min/max/count [by|without]`, vector arithmetic `+ - * /`,
  `histogram_quantile`. Not yet: subqueries, `group_left/right`, `topk`.
- **Self-scrape**: `config set observability.self_scrape true` (live) makes
  the node ingest its own `/metrics` every
  `observability.self_scrape_interval_secs` — self-dashboarding without an
  external Prometheus.
- **Node stats table**: every node INSERTs its host stats (cpu, mem, disk,
  uptime, restarts, oom_kills) into the replicated `node_stats` table every
  `observability.node_stats_interval_secs` (default 1s; on by default, live
  keys `observability.node_stats*`). One timestamped row per node, PK=node.
  The UI NODES view reads it and shows per-row age (no probe flapping);
  query it: `SELECT node, restarts, oom_kills FROM node_stats`.

---

## 6. Vector search

```sql
CREATE VECTOR INDEX docs_emb ON docs (embedding) DIM 768 USING cosine;
INSERT INTO docs (id, embedding, cat) VALUES (1, [/* 768 floats */], 'news');
SELECT id, _distance FROM docs NEAREST (embedding, [/* query */], 5)
WHERE cat = 'news';
```

HNSW, metrics `cosine` (default) / `l2` / `dot`; `_distance` injected;
`ALTER VECTOR INDEX v SET (ef = n)` retunes search-time recall/latency
live (persisted; build-time knobs need a rebuild);
`WHERE` filters candidates (over-fetch + filter); `LIMIT/OFFSET` apply
after. No JOIN/UNION/aggregates/ORDER BY with NEAREST. Vectors are float
arrays of one consistent dimension. The index is in-memory, rebuilt from
the table on open. Distributed: scatter, merge by distance.

---

## 7. Cluster semantics

- **Topology**: static seed list in config (`cluster.seeds`), or runtime
  `add-node`/`remove-node` (online resharding: dual-ring placement during
  the change, data migrates, epoch bumps). `vnodes_per_node` (256) balances
  the ring.
- **Replication**: `replication_factor` copies per key (row key or TS
  series). Consistency per operation: `ONE`/`QUORUM`/`ALL` (defaults from
  config; `\consistency` in the shell per session). Writes: quorum acks,
  hinted handoff for down replicas. Reads: quorum + read repair.
  Anti-entropy (`repair`) converges replicas both directions; `reclaim`
  drops data a node no longer owns (rows and TS series) once an owner
  confirms an identical copy.
- **Read paths for search/aggregation** ("exact-or-decline" everywhere):
  RF ≥ members → any node's local index holds everything, serves locally.
  RF < members (sharded) → aggregations, AVG, and fast-field sorted top-k
  scatter per-shard partials filtered to each node's primary-owned
  key-space (`_ring` placement-hash fast field; epoch-gated;
  all-members-or-fallback); relevance top-k scatters and dedups by key;
  per-hit explain routes to a replica of the key. Anything unmergeable
  (distinct counts, grouped per-bucket metrics, residual filters on sorted
  scatters) falls back to an exact row gather.
- **Transactions do not work on clusters** (each statement autocommits).
- Every acked write is durable (WAL) and searchable cluster-wide within the
  refresh interval.
- **Graceful shutdown**: SIGTERM flushes memtables + commits search
  writers (fast restart, no index rebuild). **Full-copy counts**: at
  RF >= members, unfiltered `COUNT(*)` answers from local key stats (no
  gather). Compaction deletes retired SSTables only after the manifest
  commit (crash-safe).
- **Memory pressure** (limits from cgroup/system RAM, non-reclaimable
  usage): above 75% a node actively releases (flushes memtables, commits
  search writers); above 85% it also sheds writes with a retryable
  "memory pressure" error (clears at 70%). Shedding logs loudly (anon/file
  + jemalloc allocated/resident/retained; a distress line every 60 s while
  stuck). Anti-entropy passes log duration when they reconcile rows or run
  ≥60 s. The systemd unit sets `MemoryHigh=85%` as a kernel-side backstop.

---

## 8. Admin & configuration

**HTTP admin** (POST, Basic auth, `ADMIN` on `*`):

```
/admin/status        cluster detail (ring, peers, liveness)
                     (GET /status also carries peers[] w/ hints_pending + lag_ms;
                      UI members panel shows per-node backlog + lag)
/admin/repair        anti-entropy pass          {"ok":true,"repaired":n}
/admin/reclaim       drop unowned keys/series   {"ok":true,"reclaimed":n}
/admin/add-node      {"addr":"host:7100"}
/admin/remove-node   {"id":"host:7100"}
/admin/config        full config, secrets masked
/admin/config/get    {"key":"section.field"}
/admin/config/set    {"key":"...","value":"..."}  → {applied, persisted,
                     restart_required} — live-mutable keys apply instantly
/admin/slow          slow-query log (masked SQL)
```

**Config** (TOML at `/etc/skaidb/skaidb.toml` on packaged installs; every
key also reachable via `config set`): `[server]` bind_addr, quic_port,
rest_port, data_dir, node_role; `[cluster]` seeds, internode_port,
replication_factor, vnodes_per_node, default_read/write_consistency,
anti_entropy_interval_secs; `[auth]` scram_enabled, superuser,
superuser_password, internode_auth (none/token/mtls); `[storage]`
memory_target (`"auto"`, `"1GB"` — budgets memtable + read cache + FTS
writer heaps + TS heads; set explicitly in containers), memtable_size_mb,
read_cache_entries; `[observability]` slow_query_ms, query_log_*,
log_format/log_file, per_table_metrics, prometheus_port, self_scrape,
self_scrape_interval_secs, node_stats, node_stats_interval_secs; `[ui]` enabled.
**Live-mutable** (no restart): all `observability.*` log/slow-query keys,
`observability.self_scrape*`, `observability.node_stats*`, `ui.enabled`.

Every admin endpoint above also has a SQL spelling (section 3: `SHOW
CLUSTER`, `SHOW CONFIG`/`SET CONFIG`, `SHOW SLOW QUERIES`, `REPAIR
CLUSTER`, `RECLAIM`, `ALTER CLUSTER`) with identical RBAC and audit.

**Docker**: `docker/` ships a Dockerfile + compose files (single node and
3-node cluster); every config key is settable as a `SKAIDB_*` env var
(env > config file > defaults), `SKAIDB_MEMORY_TARGET=auto` reads the
container's cgroup limit. See DOCKER.md.

**skaidbsh commands**: SQL plus `\status \metrics \cluster [raw] \repair
\reclaim \node add <addr> | remove <id> \config [get k | set k v]
\consistency one|quorum|all \ui [on|off]` and `USE db`.

**System requirements**: min 1 core / 512 MB / 1 GB disk (set
`storage.memory_target` on small boxes); recommended 2+ cores, 2 GB+, SSD
at 2–3× data (LSM compaction + WAL + FTS indexes); ×RF across the cluster.

**Web UI** at `/ui` (embed-in-binary, RBAC-aware): status, SQL console
with schema browser + result charts + CSV/JSON export, stats dashboards,
FTS playground + ES tester, config editor, admin ops. Disable with
`config set ui.enabled false` (live, 404s).

---

## 9. Elasticsearch-compatible subset

An ES "index" = a skaidb table; its search index = the mapping; `_id` = the
single-column primary key (string on the wire; auto-generated if omitted).
Auto-creates unknown indexes on `_bulk` (pk `id`, dynamic mapping:
string→text, int→long, float→double, bool→bool).

```
POST /{index}/_bulk      index/create/delete NDJSON
POST /{index}/_search    query DSL: match, match_phrase, prefix, wildcard,
                         regexp, fuzzy, term, terms, range, exists, bool
                         (must/filter/must_not/should — should beside
                         must boosts via BOOSTED; minimum_should_match 0|1),
                         multi_match (best_fields/most_fields/cross_fields),
                         query_string, more_like_this; from/size, multi-key
                         sort, _source include/exclude (trailing-* globs),
                         highlight, "explain": true, exact totals;
                         aggs: terms, date_histogram + sum/avg/min/max/
                         value_count/cardinality (EXACT distinct)/top_hits
POST /{index}/_count
GET  /{index}/_doc/{id}
GET  /{index}/_mapping
```

Everything translates to SQL statements internally — RBAC, replication,
and all pushdowns apply unchanged. Not Kibana-compatible; clients that
hard-check `X-elastic-product` need that check off.

---

## 10. Recipes & pitfalls checklist (for agents)

1. **Quotes**: strings `'single'`, identifiers `"double"`. A double-quoted
   "string" becomes an identifier lookup and usually a type error.
2. **One statement per request**; no `;`-chaining.
3. **REST is stateless** — use `{"sql":..., "db":...}` or `db.table`; `USE`
   only helps on binary-protocol sessions.
4. **Search visibility**: after INSERT, a search from *another* connection
   may lag up to `refresh_ms` (+200 ms). Same-session searches see their
   own writes. For tests, create the index `WITH (refresh_ms = 0)`.
5. **`ORDER BY score()` requires `LIMIT`** and is DESC-only.
6. **Search + ordinary predicates**: combine with top-level `AND` only;
   `OR`/`NOT` across the search/ordinary boundary is rejected by design.
7. **TS tables**: every insert needs all SERIES KEY labels + `ts`; UPDATE/
   DELETE are rejected; use RETENTION/rollups for lifecycle.
8. **Transactions**: embedded only. On a cluster, design idempotent
   statements instead.
9. **Joins**: no cluster pushdown — keep one side small.
10. **Schema evolution**: just write new fields; missing fields read NULL.
    `ALTER TABLE ... RENAME COLUMN` exists when a rename must be physical.
11. **Counting**: `COUNT(DISTINCT x)` is always exact;
    `APPROX_COUNT_DISTINCT(x)` opts into a sketch on the search pushdown.
12. **Diagnosing relevance**: `EXPLAIN SCORE ... FOR <pk>` (SQL) or
    `"explain": true` (ES) → full BM25 breakdown; `HIGHLIGHT()` shows what
    matched; `SUGGEST` catches typos.
13. **Monitoring**: `SHOW STATUS` (SQL) == `GET /metrics` (Prometheus);
    `/status` for topology; enable `observability.self_scrape` to dashboard
    the node from itself; Grafana points its Prometheus datasource at
    `http://node:7080`.
14. **Backups/repair**: `BACKUP TO '/path'` takes a crash-consistent
    node-local backup; `RESTORE FROM` restores it (standalone — on a
    cluster restore the node offline and let repair converge it). The
    table is the source of truth for every derived index; `REBUILD
    SEARCH INDEX` and automatic rebuild-on-open cover index damage.
    `REPAIR CLUSTER` converges replicas; `RECLAIM` frees space after
    topology changes.
15. **Upgrades**: packaged installs via apt/dnf; every node restarts into
    the new version; search indexes rebuild automatically when their
    on-disk schema version changes (one-time cost proportional to table
    size).
