# skaidb query syntax

The SQL surface skaidb accepts. It's a subset of SQL: one statement per call,
no transactions, joins, or subqueries. Rows are schema-less documents keyed by a
declared primary key; any field not present reads as `NULL`.

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
DROP   TABLE [IF EXISTS] <table>
CREATE INDEX [IF NOT EXISTS] <name> ON <table> (<path> [, <path> ...])
DROP   INDEX [IF EXISTS] <name>
CREATE VECTOR INDEX [IF NOT EXISTS] <name> ON <table> (<path>) DIM <n> [USING <metric>]
DROP   VECTOR INDEX [IF EXISTS] <name>
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
[WHERE <expr>]
[GROUP BY <expr> [, <expr> ...]]
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

-- Introspection (read-only catalog)
SHOW TABLES
SHOW INDEXES
SHOW STATUS
SHOW DATABASES
```

- `CREATE TABLE` declares **only the primary key** — there is no column list;
  documents are schema-less. A composite PK lists several columns.
- `CREATE INDEX` with one path is a single-column index; with several it is a
  **composite** index (ordered left-to-right). See indexing notes below.
- `CREATE VECTOR INDEX` builds an HNSW index for nearest-neighbor search over the
  float array at `<path>`. `DIM <n>` (the vector dimension) is **required**;
  `USING <metric>` is `cosine` (default), `l2`, or `dot`. It broadcasts across
  the cluster so every node indexes its shard. **Querying** it is via the
  `vector_search` API, not SQL yet (see below and [VECTOR.md](VECTOR.md)).
- `<select-item>` is `*` (all fields seen in the result rows) or
  `<expr> [[AS] <alias>]`.
- `ALTER TABLE … RENAME TO` renames a table (moving its on-disk data and
  repointing its indexes); `RENAME COLUMN from TO to` rewrites that field in
  every row (recomputing the primary key if it is a key column) and rebuilds any
  index that referenced it. The store is schema-less, so there is no
  `ADD`/`DROP COLUMN` — a field simply exists in the rows that set it.
- **`SHOW TABLES`** lists catalog tables as `(table, primary_key)` rows;
  **`SHOW INDEXES`** lists secondary and vector indexes as
  `(index, table, kind, columns)`. Both are read-only and require no special
  privilege, so a monitoring/tooling agent can enumerate the schema without
  `/query` data access. In cluster mode they answer from the local catalog (the
  schema is identical on every node).
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
- **`JOIN`** combines tables by nested-loop. `INNER`/`LEFT`/`RIGHT`/`CROSS` are
  supported (`JOIN` alone means `INNER`; `CROSS JOIN` takes no `ON`). Reference
  columns **qualified** by table alias (`u.id`, `o.amt`); an unqualified field
  resolves against whichever joined table defines it (first table wins on a name
  clash). `SELECT *` over a join expands to the underlying fields.
- **`UNION`** / **`UNION ALL`** concatenate the rows of several `SELECT`s that
  share a column count (`UNION` removes duplicates, `UNION ALL` keeps them). A
  trailing `ORDER BY`/`LIMIT`/`OFFSET` after the last branch applies to the whole
  combined result and references the **output column** names.
- **`BEGIN`/`COMMIT`/`ROLLBACK`** wrap several statements in a transaction with
  read-your-writes: buffered writes are invisible to other readers until
  `COMMIT`, and `ROLLBACK` discards them. **Embedded engine only** — in cluster
  mode each statement autocommits and transaction control returns an error (a
  distributed transaction coordinator is future work). DDL is not transactional.

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
- **Aggregate functions:** `COUNT(*)` / `COUNT(<expr>)`, `SUM`, `AVG`, `MIN`,
  `MAX`. Using an aggregate (or `GROUP BY`) puts the query in aggregate mode.
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
  *join* pulls each table to the coordinator and nested-loops there — so joins
  suit modest tables / lookups, not large fact-to-fact joins.
- **Vector index *creation* is SQL** (`CREATE VECTOR INDEX …`), but the
  **nearest-neighbor *query* has no SQL syntax yet** — searches go through the
  `Database::vector_search` / `Node::vector_search` API (see
  [VECTOR.md](VECTOR.md)), not an `ORDER BY embedding <-> [...]` operator.
