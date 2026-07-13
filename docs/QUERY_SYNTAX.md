# skaidb query syntax

The SQL surface skaidb accepts. It's a subset of SQL — one statement per call,
no subqueries or CTEs (joins, `UNION`, aggregates, prepared statements, and
embedded-engine transactions are supported; see below). Rows are schema-less
documents keyed by a declared primary key; any field not present reads as
`NULL`.

> **Maintenance:** this document is the source of truth for the query language.
> Whenever the parser/grammar changes — a new statement, clause, operator,
> literal form, function, or type — update this file in the same change.

## Statements

A `<table>` reference may be qualified by a database: `<database> . <table>`
(e.g. `shop.orders`). An unqualified table resolves against the connection's
current database (see *Databases* below).

```sql
-- DDL
CREATE TABLE [IF NOT EXISTS] <table> (PRIMARY KEY (<col> [, <col> ...]))
       [WITH (ttl = <duration>)]              -- rows expire after this age
CREATE TIMESERIES TABLE [IF NOT EXISTS] <table>
       (SERIES KEY (<label> [, <label> ...])
        [, RETENTION <duration>] [, OOO <duration>])
CREATE ROLLUP [IF NOT EXISTS] <name> ON <ts-table> BUCKET <duration>
       [RETENTION <duration>]
DROP   TABLE [IF EXISTS] <table>
       -- cascades to every derived index on the table (secondary, search, vector)
CREATE INDEX [IF NOT EXISTS] <name> ON <table> (<path> [, <path> ...])
DROP   INDEX [IF EXISTS] <name>
CREATE VECTOR INDEX [IF NOT EXISTS] <name> ON <table> (<path>) DIM <n> [USING <metric>]
DROP   VECTOR INDEX [IF EXISTS] <name>
ALTER  VECTOR INDEX <name> SET (ef = <n>)   -- live recall/latency tuning
CREATE SEARCH INDEX [IF NOT EXISTS] <name> ON <table> (<path> [, <path> ...])
       [WITH (<option> = <literal> [, <option> = <literal> ...])]
       -- options are global (analyzer, refresh_ms) or per-column (<path>.<option>)
DROP    SEARCH INDEX [IF EXISTS] <name>
REBUILD SEARCH INDEX <name>
ALTER   SEARCH INDEX <name> SET (<option> = <literal> [, ...])
        -- query-time options only: synonyms, refresh_ms,
        -- <col>.search_analyzer, <col>.boost (applied live, no reindex)
SUGGEST '<text>' ON <index> [COLUMN <col>] [LIMIT <n>]
EXPLAIN SCORE <select> FOR <pk-literal>
EXPLAIN <statement>
ALTER  TABLE <table> RENAME TO <new_table>
ALTER  TABLE <table> RENAME COLUMN <from> TO <to>

-- DML
INSERT INTO <table> (<col> [, <col> ...]) VALUES (<expr>, ...) [, (<expr>, ...) ...]
UPDATE <table> SET <path> = <expr> [, <path> = <expr> ...] [WHERE <expr>]
DELETE FROM <table> [WHERE <expr>]

-- Query
SELECT [DISTINCT] <select-item> [, <select-item> ...]
FROM <table> [[AS] <alias>]
[ <join> ... ]
[NEAREST (<path>, <query-vector>, <k>)]
[WHERE <expr>]
[GROUP BY <expr> [, <expr> ...] [TOP <k> BY <expr> [ASC|DESC]]]
[HAVING <expr>]
[ { UNION | UNION ALL } <select> ... ]
[ORDER BY <expr> [ASC|DESC] [, <expr> [ASC|DESC] ...]]
[LIMIT <n>] [OFFSET <n>]

<join> := [INNER | LEFT [OUTER] | RIGHT [OUTER] | CROSS] JOIN <table> [[AS] <alias>] [ON <expr>]

-- Transactions (embedded engine only — see note)
BEGIN [TRANSACTION]
COMMIT [TRANSACTION]
ROLLBACK [TRANSACTION]

-- Databases (see note)
CREATE DATABASE [IF NOT EXISTS] <name>
DROP   DATABASE [IF EXISTS] <name>
USE    [DATABASE] <name>

-- Users, roles, grants (see Access control note)
CREATE USER [IF NOT EXISTS] <name> PASSWORD '<password>'
ALTER  USER <name> PASSWORD '<password>'
DROP   USER [IF EXISTS] <name>
CREATE ROLE [IF NOT EXISTS] <name>
DROP   ROLE [IF EXISTS] <name>
GRANT  <privilege> ON { <table> | DATABASE <db> | * } TO <role>
REVOKE <privilege> ON { <table> | DATABASE <db> | * } FROM <role>
GRANT  ROLE <role> TO <user>
REVOKE ROLE <role> FROM <user>
SHOW GRANTS [FOR <role>]

-- Introspection (read-only catalog)
SHOW TABLES
SHOW INDEXES
SHOW STATUS
SHOW DATABASES

-- Admin control plane (network server only; needs ADMIN on *)
SHOW CLUSTER                                  -- ring, members, epoch, liveness
SHOW CONFIG [LIKE '<pattern>']                -- flattened keys, secrets masked
SET  CONFIG <section.field> = <literal>       -- live-mutable keys apply instantly
SHOW SLOW QUERIES [LIMIT <n>]                 -- masked slow-query sample
REPAIR CLUSTER                                -- one anti-entropy pass
RECLAIM                                       -- drop unowned keys/series
ALTER CLUSTER ADD    NODE '<host:port>'
ALTER CLUSTER REMOVE NODE '<id>'

-- Session (binary-protocol connections)
SET CONSISTENCY { ONE | QUORUM | ALL }        -- per-connection override

-- Backup & restore (ADMIN)
BACKUP TO '<path>'        -- crash-consistent copy of this node's data dir
RESTORE FROM '<path>'     -- embedded / single node only; old data kept aside
```

- `CREATE TABLE` declares **only the primary key** — there is no column list;
  documents are schema-less. A composite PK lists several columns.
  `WITH (ttl = <duration>)` makes rows **expire**: a row older than the TTL
  (measured from its write's HLC timestamp) becomes invisible to every read
  and is physically dropped at the next compaction. TTL is a read-visibility
  rule applied uniformly on every replica (the stamped data still
  replicates, so expiry converges regardless of which node serves the
  read). Useful for caches, sessions, and rolling event windows.
- `CREATE INDEX` with one path is a single-column index; with several it is a
  **composite** index (ordered left-to-right). See indexing notes below.
- `CREATE VECTOR INDEX` builds an HNSW index for nearest-neighbor search over the
  float array at `<path>`. `DIM <n>` (the vector dimension) is **required**;
  `USING <metric>` is `cosine` (default), `l2`, or `dot`. It broadcasts across
  the cluster so every node indexes its shard. Query it with the `NEAREST`
  clause (see *Vector search* below and [VECTOR.md](VECTOR.md)).
- `CREATE SEARCH INDEX` builds a **full-text (BM25) index** over the listed
  document paths. Global options: `analyzer` (`'standard'` default,
  `'folding'`, `'whitespace'`, `'keyword'`, `'ngram(min,max)'`,
  `'edge_ngram(min,max)'`, or a language like `'english'` — full list in
  [SEARCH.md](SEARCH.md)) and `refresh_ms` (integer, default `1000` — how
  quickly writes become searchable). Per-column options use the path as
  prefix: `<path>.type` (`'text'` default, `'keyword'`, `'long'`,
  `'double'`, `'bool'`, `'date'`), `<path>.analyzer`,
  `<path>.search_analyzer`, `<path>.boost` (number), `<path>.keyword`
  (boolean — adds a `<path>.keyword` exact-match twin), and
  `<path>.copy_to` (composite target field). Query it with
  `MATCH()`/`SEARCH()` predicates and `score()` (see *Full-text search*
  below and [SEARCH.md](SEARCH.md)); `REBUILD SEARCH INDEX` re-indexes the
  table from scratch (recovery escape hatch).
- `<select-item>` is `*` (all fields seen in the result rows) or
  `<expr> [[AS] <alias>]`.
- `ALTER TABLE … RENAME TO` renames a table (moving its on-disk data and
  repointing its indexes); `RENAME COLUMN from TO to` rewrites that field in
  every row (recomputing the primary key if it is a key column) and rebuilds any
  index that referenced it. The store is schema-less, so there is no
  `ADD`/`DROP COLUMN` — a field simply exists in the rows that set it.
- **`SHOW TABLES`** lists catalog tables as `(table, primary_key)` rows;
  **`SHOW INDEXES`** lists secondary, vector, and search indexes as
  `(index, table, kind, columns)`. Both are read-only and require no special
  privilege, so a monitoring/tooling agent can enumerate the schema without
  `/query` data access. In cluster mode they answer from the local catalog (the
  schema is identical on every node).
- **Admin statements** (`SHOW CLUSTER`/`SHOW CONFIG`/`SET CONFIG`/
  `SHOW SLOW QUERIES`/`REPAIR CLUSTER`/`RECLAIM`/`ALTER CLUSTER`) are the
  SQL spellings of the HTTP `/admin/*` control plane — identical handler,
  RBAC (`ADMIN` on `*`), and audit; results come back as `(key, value)`
  rows (LIKE uses `%`/`_`). They execute on the **network server** — the
  embedded `--local` engine rejects them. `SET CONSISTENCY` is
  per-connection session state on binary-protocol sessions (it overrides
  the wire consistency until changed); the stateless REST gateway rejects
  it with guidance.
- **`BACKUP TO '<path>'`** copies the whole data directory (tables, WALs,
  catalog, search and time-series stores) under the exclusive lock —
  crash-consistent by construction (opening the copy replays WALs like a
  crash recovery); vector indexes load their persisted snapshot on open and
  replay only rows newer than its watermark (full rebuild if absent). The
  target must not exist. On a cluster each node backs up **its own
  shard**. **`RESTORE FROM '<path>'`** swaps the backup in and reopens,
  moving the previous data aside to `<dir>.pre-restore-<n>` (never
  deleted); it refuses to run on a cluster — stop the node and restore
  offline, then let repair converge it.
- **`SHOW STATUS`** returns storage and runtime statistics for the current
  database as `(metric, value)` rows — table/index counts, on-disk and memtable
  bytes, SSTable count, WAL bytes/fsyncs, compactions, cache hit/miss/hit-rate,
  and a per-table `table.<name>.{live_keys,tombstones,disk_bytes}` breakdown. It
  is the same data the server publishes at `GET /metrics`, surfaced as SQL.
- **`CREATE`/`DROP DATABASE`**, **`USE`**, and **`SHOW DATABASES`** manage
  databases — each is an isolated set of tables and indexes. A database is a
  **namespace**: internally a table is stored under a per-database name, with the
  implicit `default` database using unprefixed names (so an existing
  single-database directory keeps working unchanged — its tables are the
  `default` database). `USE` sets the **current database** for the connection
  (the `skaidbsh` shell, or a binary-protocol connection); unqualified table
  names resolve against it, and `db.table` reaches another database without
  switching. `SHOW DATABASES` lists them as `(database, current)` rows with `*`
  marking the current one; `SHOW TABLES`/`SHOW INDEXES` are scoped to the current
  database. The `default` database cannot be dropped; dropping the current
  database (or its cascade) reverts the connection to `default`.
  - **Replication:** in cluster mode, `CREATE`/`DROP DATABASE` broadcast to every
    node (like other DDL), and writes inside any database replicate by the same
    quorum/hinted-handoff/read-repair path as the `default` database. The REST
    `/query` gateway is stateless — it always starts at `default`, so reach other
    databases there with `db.table` qualifiers rather than `USE`.
- **`DISTINCT`** removes duplicate output rows. **`HAVING`** filters groups after
  aggregation (it may reference aggregates and the `GROUP BY` columns).
- **`GROUP BY ... TOP <k> BY <expr> [ASC|DESC]`** — per-group top-k **rows**:
  instead of one aggregated row per group, each group contributes its `k`
  best rows ranked by the expression (`DESC` — best-first — is the
  default; NULLs rank last either way). The select items are then ordinary
  per-row expressions (`*` works; aggregates cannot mix with `TOP`), and
  `HAVING` (which may aggregate) still filters whole groups first.
  `ORDER BY`/`LIMIT`/`OFFSET` apply to the flattened output; without
  `ORDER BY`, groups keep first-seen order with rows best-first inside
  each. Under a search predicate `TOP k BY score()` ranks by BM25 — the
  SQL spelling of ES `top_hits` (per-group best documents), and
  `HIGHLIGHT()` works in the projection.
- **`JOIN`** combines tables — equi-joins (`ON a = b`) run as a hash join,
  other predicates and `RIGHT` joins fall back to a nested loop.
  `INNER`/`LEFT`/`RIGHT`/`CROSS` are supported (`JOIN` alone means `INNER`;
  `CROSS JOIN` takes no `ON`). Reference columns **qualified** by table alias
  (`u.id`, `o.amt`); an unqualified field resolves against whichever joined
  table defines it (first table wins on a name clash). `SELECT *` over a join
  expands to the underlying fields.
- **`UNION`** / **`UNION ALL`** concatenate the rows of several `SELECT`s that
  share a column count (`UNION` removes duplicates, `UNION ALL` keeps them). A
  trailing `ORDER BY`/`LIMIT`/`OFFSET` after the last branch applies to the whole
  combined result and references the **output column** names.
- **`BEGIN`/`COMMIT`/`ROLLBACK`** wrap several statements in a transaction with
  read-your-writes: buffered writes are invisible to other readers until
  `COMMIT`, and `ROLLBACK` discards them. **Embedded engine only** — in cluster
  mode each statement autocommits and transaction control returns an error (a
  distributed transaction coordinator is future work). DDL is not transactional.

- **Access control.** `<privilege>` is one of `SELECT`, `INSERT`, `UPDATE`,
  `DELETE`, `CREATE`, `DROP`, `GRANT`, `ADMIN` (`ADMIN` on `*` = superuser;
  `ADMIN` on a table implies every privilege on it). A **user** authenticates
  (SCRAM on the binary protocol, HTTP Basic on REST) and acts as its
  own-named role; `GRANT ROLE r TO u` adds inherited roles. A grant
  `ON DATABASE db` covers every table in that database (checked against the
  session's current database; shown by `SHOW GRANTS` as `db:<name>`). All
  management statements (and `SHOW GRANTS`) require the `GRANT` privilege
  cluster-wide — except `SHOW GRANTS FOR <your own role>`, which any
  authenticated role may run to inspect itself. Auth DDL (user/role/grant
  changes) is recorded in the identity audit log (the login-log category)
  with the acting role and a secret-free statement summary.
  Users/roles persist in the catalog, replicate like other DDL (passwords
  travel between nodes only as salted SCRAM verifiers, never plaintext), and
  converge via schema repair. The config-file superuser remains the
  bootstrap principal. The Prometheus endpoints are covered too:
  `remote_write` requires `INSERT` and `/api/v1/query*` require `SELECT` on
  the `metrics` table.

## Expressions

- **Literals:** integer (`42`), float (`3.14`), string (`'ada'`), `TRUE`,
  `FALSE`, `NULL`, and **array** literals `[<lit> [, <lit> ...]]` (constant
  elements only — a literal or a negated number, e.g. `[0.1, -0.2, 0.3]`).
  There is no object/nested-document literal.
- **Column / field paths:** `name`, or a dotted path into a nested document:
  `address.city`, `a.b.c`. Usable in projections, `WHERE`, `GROUP BY`,
  `ORDER BY`, and `UPDATE … SET` targets.
- **Operators**, by increasing precedence:
  1. `OR`
  2. `AND`
  3. `NOT <expr>`
  4. comparison: `=`, `!=` (or `<>`), `<`, `<=`, `>`, `>=`
  5. `<expr> IS [NOT] NULL`
  6. additive: `+`, `-`
  7. multiplicative: `*`, `/`
  8. unary: `-<expr>`, `NOT <expr>`
  9. parentheses `( … )`
- **Aggregate functions:** `COUNT(*)` / `COUNT(<expr>)` /
  `COUNT(DISTINCT <expr>)` (exact distinct non-null values),
  `APPROX_COUNT_DISTINCT(<expr>)` (opt-in approximate distinct: the
  search-index pushdown may answer from an HLL sketch — never truncates,
  ~±1–2% at high cardinality; every other path answers exactly), `SUM`,
  `AVG`,
  `MIN`, `MAX`, and the time-series aggregates `RATE`, `INCREASE`, `DELTA`,
  `FIRST`, `LAST` (see *Time-series tables* below). Using an aggregate (or
  `GROUP BY`) puts the query in aggregate mode.
- **Duration literals:** an integer immediately followed by a unit — `250ms`,
  `15s`, `5m`, `2h`, `30d`, `1w` — is a duration, valued as integer
  milliseconds (`5m` = `300000`). Usable anywhere an integer is.
- **Scalar functions:** `now()` (the query's start time, as a timestamp —
  one instant per statement) and `time_bucket(<step>, <ts>)` (floors `<ts>`
  to a `<step>`-wide bucket: `time_bucket(5m, ts)`). A bare identifier
  directly followed by `(` parses as a function call; unknown functions are
  execution errors. The full-text search functions `MATCH`, `MATCH_PHRASE`,
  `FUZZY`, `SEARCH`, and `score` parse the same way and are only valid in a
  search query (see *Full-text search* below).
- **Bind parameters:** `?` marks a positional parameter in any expression
  position of a **prepared** `SELECT`/`INSERT`/`UPDATE`/`DELETE` (e.g.
  `INSERT INTO t (id, v) VALUES (?, ?)`). Parameters are numbered by order of
  appearance and bound to values at execute time via the binary protocol's
  prepared-statement messages (or a driver's `prepare`/`execute` API). DDL
  and session-control statements cannot be prepared, and a `?` submitted
  through the plain one-shot query path is an error at execution.
- Comparisons follow three-valued logic (`NULL` compares as unknown).

## Identifiers & literals (lexical)

- **Bare identifiers** start with a letter or `_` and continue with letters,
  digits, or `_`. Keywords are case-insensitive.
- **Quoted identifiers** use double quotes: `"select"`, `"weird name"`.
- **String literals** use single quotes; embed a quote by doubling it:
  `'O''Brien'`.
- `;` may terminate a statement. Only one statement is executed per call.

## Types

Values are dynamically typed: `null`, `bool`, `int64`, `float64`, `decimal`,
`string`, `bytes`, `uuid`, `timestamp` (Unix time, milliseconds), `array`,
`document`. Only `int`, `float`, `string`, `bool`, `null`, and `array` literals
can be written directly in SQL; `decimal`/`uuid`/`bytes`/`timestamp`/`document`
values arrive via stored data or the value codec, not as SQL literals.

## Vector search (`NEAREST`)

`NEAREST (<path>, <query-vector>, <k>)` runs an approximate nearest-neighbor
search over a [vector index](VECTOR.md) on `(<table>, <path>)`, returning the
`<k>` closest rows ordered nearest-first, with the match distance exposed as a
`_distance` field:

```sql
CREATE VECTOR INDEX docs_emb ON docs (embedding) DIM 3 USING cosine;
SELECT id, _distance FROM docs NEAREST (embedding, [1.0, 0.0, 0.0], 5);
SELECT id FROM docs NEAREST (embedding, [1.0, 0.0, 0.0], 5) WHERE cat = 'news';
```

`<query-vector>` and `<k>` may be literals or bind parameters (`?`) in a
prepared statement. Requires a vector index on the path; errors if none
exists. Cannot combine with `JOIN`, `UNION`, aggregates/`GROUP BY`, or
`ORDER BY` (results are already ordered by distance) — `WHERE`, `LIMIT`, and
`OFFSET` apply normally, post-search.

## Full-text search (`MATCH` / `SEARCH`)

Full-text predicates run against a [search index](SEARCH.md) on the table
(BM25-ranked, Tantivy-backed). They are ordinary `WHERE` conditions: search
predicates **compose with `AND`/`OR`/`NOT` among themselves** and join
ordinary conditions at the top level with `AND` (those filter the matches
afterward). Mixing a search predicate with an ordinary condition under
`OR`/`NOT` is rejected — the index cannot serve it.

```sql
CREATE SEARCH INDEX articles_fts ON articles (title, body)
  WITH (analyzer = 'english', refresh_ms = 1000);

-- Ranked retrieval: top-k pushed into the index, best first. The BM25
-- score is exposed as score() (and a _score field on the row);
-- HIGHLIGHT() projects a snippet with the matches marked.
SELECT id, title, score(), HIGHLIGHT(body, 120) AS snippet FROM articles
WHERE MATCH(body, 'quick brown fox') AND published = true
ORDER BY score() DESC LIMIT 10;

-- Query-string mini-language over all indexed columns:
SELECT id, score() FROM articles
WHERE SEARCH('title:"rust database" +body:performance -draft')
ORDER BY score() DESC LIMIT 20;

-- Bool composition of search predicates (must / should / must_not):
SELECT id FROM articles
WHERE (MATCH(body, 'rust') OR MATCH(title, 'rust'))
  AND NOT MATCH_PHRASE(body, 'rust belt') AND year >= 2024;
```

- **Analyzed predicates** (query text goes through the field's query-time
  analyzer — `search_analyzer` if set, else the index-time one):
  - `MATCH(<col>, '<text>')` — true if the column matches any term (OR).
  - `MATCH_PHRASE(<col>, '<text>' [, <slop>])` — terms in order, within
    `<slop>` transpositions (default 0).
  - `FUZZY(<col>, '<text>' [, <distance>])` — Levenshtein-fuzzy terms,
    distance ≤ 2 (default 1).
  - `SEARCH('<query-string>')` — mini-language, bare terms over the text
    columns: `term`, `"phrase"`, `col:term`, `+must`, `-must_not`,
    `AND`/`OR`, and ranges over typed columns (`year:[2020 TO 2024]`,
    `price:[30 TO *]`, `published:true`).
  - `MATCH_CROSS(<col>, <col> [, ...], '<text>')` — term-centric
    multi-field match (ES `multi_match` `cross_fields`): the listed
    fields behave like one big field — each term scores by its best
    field, terms OR together. At least two columns.
  - `MATCH_BEST(<col>, <col> [, ...], '<text>')` — field-centric twin
    (ES `best_fields` over an explicit subset): each listed field scores
    the whole query and the best field wins (dis-max). `MATCH` with all
    text fields already dis-maxes; `MATCH_BEST` picks the subset.
- **Pattern predicates** (not analyzed — they run against the indexed
  terms, so with a lowercasing analyzer write patterns lowercase):
  - `MATCH_PREFIX(<col>, '<prefix>')` — term prefix.
  - `WILDCARD(<col>, '<pattern>')` — `*` any run, `?` any one char.
  - `REGEXP(<col>, '<pattern>')` — regular expression.
- **`MORE_LIKE_THIS(<col>, '<like text>')`** — rows textually similar to
  the given text: its most distinctive terms (by in-index document
  frequency; terms in fewer than 2 docs are ignored, at most 25 terms)
  OR-ed together. Composes like the other predicates.
- **`BOOSTED(<required>, <optional> [, <optional> ...])`** — optional
  scoring composition (ES bool `must` + `should`): rows match iff
  `<required>` matches; each `<optional>` predicate only **raises the
  score** of rows that already match. Every argument must itself be a
  search predicate (possibly AND/OR/NOT-composed).
- **`EXPLAIN <statement>`** — the plan the executor would choose for any
  SELECT/DML statement, as `(aspect, decision)` rows: the access path
  (primary-key point read, secondary-index scan with its bounds, full
  table scan, BM25 top-k / index-ordered / unranked search pushdown,
  search-aggregation pushdown vs. row-gather fallback, HNSW vector
  search), residual-filter and join-strategy notes, and — on a cluster —
  appended `cluster.*` rows (members, replication factor, fan-out:
  point-routed / served locally / scatter-gather). Advisory: it mirrors
  the planner's decision logic without executing anything, so `EXPLAIN
  DELETE ...` is safe. Gated by the wrapped statement's own privilege.
  `EXPLAIN EXPLAIN` is rejected.
- **`EXPLAIN SCORE <select> FOR <pk literal>`** — a standalone statement
  returning the BM25 breakdown (`explanation` column, tantivy's JSON —
  per-term K1 / idf(n, N) / tf-normalization) of how the row with that
  primary-key value scored against the SELECT's search predicates. One
  row when the key matches, zero rows when it does not; an error without
  a search predicate. On a cluster the explain routes to a replica of
  the key, so it works at any replication factor.
- **`SUGGEST '<text>' ON <index> [COLUMN <col>] [LIMIT n]`** — a
  standalone statement returning "did you mean" term suggestions
  (`input`, `suggestion`, `distance`, `doc_freq` — closest first, most
  frequent within a distance, at most `n` per input token, default 5).
  `COLUMN` is required when the index covers several text columns. For
  search-as-you-type completion, use an `edge_ngram` analyzer with
  `MATCH_PREFIX` instead.
- `<col>` may also be a declared `.keyword` twin (`title.keyword`, exact
  original string) or a `copy_to` composite field. Text predicates on a
  numeric/date/bool column error.
- `NOT` returns only rows the index contains: a row with none of the
  indexed columns present is never returned by a search query.
- **`score()`** projects the BM25 relevance of the row against the search
  predicates; it is also injected as a `_score` field (like `_distance` for
  vector search). Only valid together with a search predicate — else an
  error. Multi-field matches score dis-max (best field wins, ES
  `best_fields`).
- **`HIGHLIGHT(<col> [, <max_chars>])`** projects the best-scoring snippet
  of the column's text (default 150 chars), matches wrapped in `<b>…</b>`
  (HTML-escaped otherwise; empty string if the column didn't match). Only
  valid together with a search predicate.
- **`ORDER BY score() DESC LIMIT k`** pushes BM25 top-k retrieval into the
  index (no full scan); it requires `LIMIT`, and `score()` orders only
  descending. Without `ORDER BY`, matches return in unspecified order.
- **`ORDER BY <col> [ASC|DESC]`** also works with search predicates: a
  single declared fast-field column with `LIMIT` retrieves index-ordered
  top-k directly; any other ordering (multi-key, expressions, non-fast
  columns) gathers the matches and sorts through the ordinary executor.
  Keyset pagination: `ORDER BY <col> LIMIT k` plus a residual range
  predicate (`AND col > <last>`).
- The named column(s) must be covered by a search index on the table —
  errors if none exists. Query text may be a bind parameter (`?`).
- **Aggregates / `GROUP BY` work over search queries**
  (`SELECT region, COUNT(*) … WHERE MATCH(…) GROUP BY region`) — keyword
  and `time_bucket(step, date-col)` groupings with
  `COUNT`/`COUNT(DISTINCT col)`/`SUM`/`AVG`/`MIN`/`MAX` push down as exact
  fast-field facets; every other shape (HAVING, text-column grouping,
  residual predicates) aggregates the gathered matching rows. See
  [SEARCH.md](SEARCH.md#aggregations).
- **`COUNT(DISTINCT <expr>)`** counts distinct non-null values, exactly —
  in any aggregate query, search or not. `DISTINCT` arguments to other
  aggregate functions are not supported.
- Cannot combine with `JOIN`, `UNION`, `DISTINCT`, or `NEAREST`. Residual
  `WHERE` conditions, `LIMIT`, and `OFFSET` apply normally, post-search.
- **Visibility:** writes become searchable within `refresh_ms` (default
  1 s, Elasticsearch-style near-real-time); on the single-node write path a
  search after a write sees it immediately. The index is derived data — the
  table is the source of truth, and a lost or stale index rebuilds from it
  (automatically on restart, or via `REBUILD SEARCH INDEX`).
- Cluster mode: the DDL broadcasts like other DDL, every node indexes its
  shard from replicated writes, and a search scatters to all members —
  per-shard top-k merged by score at the coordinator, survivors re-read at
  read consistency. Every acked write is searchable cluster-wide (replicas
  commit pending index writes before answering). Unreachable members are
  skipped; their rows surface through reachable replicas.

## Time-series tables

`CREATE TIMESERIES TABLE` declares a table whose rows are **samples**, stored
in the time-series engine (Gorilla-compressed chunks; feature status and
internals in [TIMESERIES.md](TIMESERIES.md), pending work in
[TODO.md](TODO.md)). Distributed: the DDL broadcasts, series place on the
ring and replicate at the write consistency, and queries union-merge across
members. Joins and
decommissions migrate series like any other data.

```sql
CREATE TIMESERIES TABLE cpu (SERIES KEY (host, core), RETENTION 30d);
INSERT INTO cpu (host, core, ts, value) VALUES ('web1', '0', 1712000000000, 0.63);

SELECT time_bucket(1m, ts) AS t, host, avg(value), max(value)
FROM cpu WHERE ts >= now() - 1h AND host = 'web1'
GROUP BY t, host ORDER BY t;

SELECT time_bucket(5m, ts) AS t, rate(value) FROM cpu
WHERE ts >= now() - 6h GROUP BY t;
```

- **Columns have roles.** `SERIES KEY` columns are string **labels** (every
  insert must set all of them); `ts` is the sample timestamp (a `timestamp`
  or integer milliseconds, required, strictly increasing per series); every
  other inserted column is a numeric **field**. Names starting with `__` are
  reserved.
- **Append-only.** `UPDATE`/`DELETE` are rejected; old data expires via
  `RETENTION <duration>` (whole storage blocks drop as they age out).
  Older timestamps for a series are rejected per sample unless the table
  declares an out-of-order window (`OOO 10m`): samples within the window of
  the series' newest are buffered and merged in time order; an equal
  timestamp overwrites (last write wins).
- **Pushdown.** `AND`-combined `ts` comparisons and label `=`/`!=`
  predicates narrow the storage read; any other predicate still applies with
  full SQL semantics afterward.
- **Time-series aggregates**, valid wherever other aggregates are:
  `rate(f)` (per-second counter rate, reset-aware), `increase(f)` (total
  counter increase, reset-aware), `delta(f)` (`last - first`), `first(f)` /
  `last(f)` (value at the earliest/latest `ts` in the group).
  `rate`/`increase`/`delta` are computed **per series** over its
  time-ordered samples, then **summed** across the series in the group —
  PromQL `sum(rate(...))` semantics. Group by label columns to keep series
  separate.
- **`GROUP BY`/`ORDER BY` may reference output aliases** on time-series
  tables (`GROUP BY t` with `time_bucket(1m, ts) AS t`).
- **Rollups** (`CREATE ROLLUP r30m ON cpu BUCKET 30m RETENTION 90d`): a
  derived time-series table holding per-bucket partials of its source,
  maintained automatically when source windows flush. For each source field
  `f` it stores `f_count`, `f_sum`, `f_min`, `f_max`, `f_first`, `f_last`
  (so `avg = f_sum / f_count`), keyed by the same series labels at the
  bucket-start timestamp. Query it like any TS table
  (`SELECT ts, value_sum / value_count FROM r30m WHERE host = '...'`).
  `BUCKET` must evenly divide the 2 h storage window. Dropped with
  `DROP TABLE` (dropping the source cascades). Repair-merged samples do not
  retroactively update rollups.
- **Rollup query rewrite** (v0.32.0): an aggregate query on the **source**
  table whose window reaches past the source's `RETENTION` horizon answers
  the aged buckets from the coarsest rollup whose `BUCKET` divides the
  group's `time_bucket` step, stitched with exact source data for the
  within-retention part — so long-range dashboards keep working after raw
  samples age out, without naming the rollup. Applies to
  `count/sum/avg/min/max/first/last`; `rate`/`increase`/`delta` need raw
  samples and never read rollups. In the rollup-served region,
  `first()`/`last()` order series at bucket granularity, and the window
  edge trims to whole rollup buckets.
- Not supported on time-series tables: `JOIN`, `UNION`, `NEAREST`,
  transactions.

## Indexing notes (query-relevant)

- A `WHERE` predicate is **index-accelerated** when it constrains an indexed
  column by equality or range (`=`, `<`, `<=`, `>`, `>=`, and `AND`-combined
  ranges / `BETWEEN`-style), or when `ORDER BY` follows an index. Composite
  indexes use a leftmost prefix of equalities plus an optional trailing range,
  and serve `ORDER BY` along that prefix. Everything else falls back to a scan;
  results are identical either way.
- The **primary key** is the routing key in a cluster: `WHERE pk = <v>` is a
  point read to the key's replica set; other indexed predicates are pushed to
  each node's local index.

## Not part of the SQL surface

- **No** subqueries, CTEs (`WITH`), `DISTINCT ON`, `FULL OUTER JOIN`,
  `INTERSECT`/`EXCEPT`, window functions, or set operators other than `UNION` /
  `UNION ALL`.
- **No** `ALTER` beyond `RENAME TABLE` / `RENAME COLUMN` (the store is
  schema-less, so there is nothing to `ADD`/`DROP COLUMN`).
- **Transactions are embedded-only.** `BEGIN`/`COMMIT`/`ROLLBACK` work against an
  embedded `Database`; the cluster coordinator autocommits each statement and
  rejects transaction control (no distributed 2PC yet).
- **Joins have no pushdown in a cluster:** a single-table `WHERE` *is* pushed to
  the shards (each node returns only matching keys, re-read at quorum), but a
  *join* pulls each table to the coordinator and joins there — so joins suit
  modest tables / lookups, not large fact-to-fact joins.
- **Vector search uses `NEAREST`**, not an `ORDER BY embedding <-> [...]`
  operator (see *Vector search* above).
