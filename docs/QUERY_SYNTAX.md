# skaidb query syntax

The SQL surface skaidb accepts. It's a subset of SQL: one statement per call,
no transactions, joins, or subqueries. Rows are schema-less documents keyed by a
declared primary key; any field not present reads as `NULL`.

> **Maintenance:** this document is the source of truth for the query language.
> Whenever the parser/grammar changes — a new statement, clause, operator,
> literal form, function, or type — update this file in the same change.

## Statements

```sql
-- DDL
CREATE TABLE [IF NOT EXISTS] <table> (PRIMARY KEY (<col> [, <col> ...]))
DROP   TABLE [IF EXISTS] <table>
CREATE INDEX [IF NOT EXISTS] <name> ON <table> (<path> [, <path> ...])
DROP   INDEX [IF EXISTS] <name>
CREATE VECTOR INDEX [IF NOT EXISTS] <name> ON <table> (<path>) DIM <n> [USING <metric>]
DROP   VECTOR INDEX [IF EXISTS] <name>

-- DML
INSERT INTO <table> (<col> [, <col> ...]) VALUES (<expr>, ...) [, (<expr>, ...) ...]
UPDATE <table> SET <path> = <expr> [, <path> = <expr> ...] [WHERE <expr>]
DELETE FROM <table> [WHERE <expr>]

-- Query
SELECT <select-item> [, <select-item> ...]
FROM <table>
[WHERE <expr>]
[GROUP BY <expr> [, <expr> ...]]
[ORDER BY <expr> [ASC|DESC] [, <expr> [ASC|DESC] ...]]
[LIMIT <n>] [OFFSET <n>]
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

- **No** `JOIN`, subqueries, `DISTINCT`, `HAVING`, `UNION`, CTEs, window
  functions, or multi-statement transactions (`BEGIN`/`COMMIT`/`ROLLBACK`).
- **No** `ALTER`. Schema changes are limited to create/drop of tables/indexes.
- **Vector index *creation* is SQL** (`CREATE VECTOR INDEX …`), but the
  **nearest-neighbor *query* has no SQL syntax yet** — searches go through the
  `Database::vector_search` / `Node::vector_search` API (see
  [VECTOR.md](VECTOR.md)), not an `ORDER BY embedding <-> [...]` operator.
