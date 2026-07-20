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
       [WITH (ttl = <duration>,               -- rows expire after this age
              witness = <bool>,               -- mirror to witness nodes (default true)
              replication = <n>,              -- per-table RF override
              nodes = ['<alias-or-id>', ...])] -- pin the whole table to these members
CREATE TIMESERIES TABLE [IF NOT EXISTS] <table>
       (SERIES KEY (<label> [, <label> ...])
        [, RETENTION <duration>] [, OOO <duration>])
CREATE ROLLUP [IF NOT EXISTS] <name> ON <ts-table> BUCKET <duration>
       [RETENTION <duration>]
DROP   TABLE [IF EXISTS] <table>
       -- cascades to every derived index on the table (secondary, search, vector)
CREATE INDEX [IF NOT EXISTS] <name> ON <table> (<path>[[]] [, <path>[[]] ...]) [WITH (global = true)]
       -- WITH (global = true): value-sharded index — equality probes route to the value's
       -- replica set instead of scattering; ranges keep the scatter path (GLOBAL_INDEXES.md)
DROP   INDEX [IF EXISTS] <name>
CREATE VECTOR INDEX [IF NOT EXISTS] <name> ON <table> (<path>) DIM <n> [USING <metric>] [QUANTIZED] [EMBED]
DROP   VECTOR INDEX [IF EXISTS] <name>
ALTER  VECTOR INDEX <name> SET (ef = <n>)   -- live recall/latency tuning
CREATE GEO INDEX [IF NOT EXISTS] <name> ON <table> (<point-path>)
       -- Morton/Z-order index: geo_distance / geo_bbox prune to a neighborhood
DROP   GEO INDEX [IF EXISTS] <name>
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
SELECT <expr> [[AS] <alias>] [, ...]   -- no FROM: constant projection, one row
                                       -- (`SELECT 1` liveness probe; no other
                                       --  clause may follow; `*` needs a table)
SELECT [DISTINCT] <select-item> [, <select-item> ...]
FROM <table> [[AS] <alias>]
[ <join> ... ]
[NEAREST (<path>, <query-vector>, <k>)]
[WHERE <expr>]
[RANK BY RRF [(<constant>)]]            -- hybrid: fuse NEAREST + WHERE-search by RRF
[RERANK [ON <col>] [WITH '<model>'] [QUERY '<text>'] [TOP <n>]]
                                        -- second-stage cross-encoder reranking
[GROUP BY <expr> [, <expr> ...] [TOP <k> BY <expr> [ASC|DESC]]]
[HAVING <expr>]
[ { UNION | UNION ALL } <select> ... ]
[ORDER BY <expr> [ASC|DESC] [, <expr> [ASC|DESC] ...]]
[LIMIT <n>] [OFFSET <n>]
[AFTER (<sort-value>, <pk-value>)]      -- deep pagination: keyset cursor (search queries)

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
CREATE USER [IF NOT EXISTS] <name> GSSAPI      -- external Kerberos principal,
                                               -- no local secret; <name> is the
                                               -- principal, e.g. "user@REALM"
                                               -- (double-quote it — it contains
                                               -- '@' and '.')
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

-- Admin control plane (network server only; reads need MONITOR on *,
-- mutations need ADMIN on *; ADMIN implies MONITOR)
SHOW CLUSTER                                  -- ring, members, epoch, liveness
SHOW CONFIG [LIKE '<pattern>']                -- flattened keys, secrets masked
SET  CONFIG <section.field> = <literal>       -- live-mutable keys apply instantly (ADMIN)
SHOW SLOW QUERIES [LIMIT <n>]                 -- masked slow-query sample
REPAIR CLUSTER                                -- one anti-entropy pass
RECLAIM                                       -- drop unowned keys/series
ALTER CLUSTER ADD    NODE '<host:port>'
ALTER CLUSTER REMOVE NODE '<id>'
ALTER CLUSTER SET NAME '<name>'               -- rename the cluster (ADMIN)
ALTER NODE '<alias|dotted|id>' SET NAME '<n>' -- rename a member/witness alias (ADMIN)
ALTER TABLE <table> SET (witness = <bool>     -- toggle witness mirroring
                       | replication = <n>    -- online placement transition
                       | nodes = ['<ref>',..] -- online pin change
                       | placement_finalized = true)  -- operator escape hatch

-- Session (binary-protocol connections)
SET CONSISTENCY { ONE | QUORUM | ALL }        -- per-connection override

-- Backup & restore (ADMIN)
BACKUP TO '<path>'        -- crash-consistent copy of this node's data dir
RESTORE FROM '<path>'     -- embedded / single node only; old data kept aside
```

- `CREATE TABLE` declares **only the primary key** — there is no column list;
  documents are schema-less. A composite PK lists several columns.
  `WITH (memory = true)` makes the table **RAM-only**: no write-ahead log
  fsyncs, never flushed to disk, empty after a restart, and skipped by
  repair/reshard data motion — for short-lived bounded data (node stats,
  caches); pair with a `ttl`. Indexes on memory tables are not supported.
  `WITH (ttl = <duration>)` makes rows **expire**: a row older than the TTL
  (measured from its write's HLC timestamp) becomes invisible to every read
  and is physically dropped at the next compaction. TTL is a read-visibility
  rule applied uniformly on every replica (the stamped data still
  replicates, so expiry converges regardless of which node serves the
  read). Useful for caches, sessions, and rolling event windows.
- `CREATE INDEX` with one path is a single-column index; with several it is a
  **composite** index (ordered left-to-right). See indexing notes below.
- `WITH (global = true)` makes it a **global (value-sharded) index**: entries
  live in an internal replicated table placed on the ring by indexed value,
  so a full-tuple equality/`IN` probe routes to one replica set instead of
  scattering to every member — the RF < members win; local stays the default.
  Ranges and partial prefixes keep the scatter path. The DDL acks at
  schema-apply and backfills in the background (`SHOW INDEXES` says
  `global (building)`; probes fall back to scatter until ready). Guide:
  [INDEXING.md](INDEXING.md#global-value-sharded-indexes).
- A `[]` suffix on one path (`CREATE INDEX i ON t (account, labels[])`) marks
  a **multikey** component: the value there is an array and each element gets
  its own index entry, so `labels = 'x'` (element containment) becomes an
  index probe — including exact index-only counts. At most one `[]` per
  index. The planner uses a multikey index only when every column through
  the `[]` component is equality-constrained; other shapes (ranges or sorts
  on the array column) fall back to a scan.
- `CREATE VECTOR INDEX` builds an HNSW index for nearest-neighbor search over the
  float array at `<path>`. `DIM <n>` (the vector dimension) is **required**;
  `USING <metric>` is `cosine` (default), `l2`, or `dot`. `QUANTIZED` stores
  int8 scalar-quantized vectors in the in-RAM graph (4× less vector RAM);
  queries over-fetch and rescore the top-k against the exact row vectors, so
  returned distances stay exact (build-time choice — rebuild to change; not
  combinable with `EMBED`). It broadcasts across the cluster so every node
  indexes its shard. Query it with the `NEAREST` clause (see *Vector search*
  below and [VECTOR.md](VECTOR.md)).
- `CREATE GEO INDEX` builds a **Morton (Z-order) spatial index** over the
  `{lat, lon}` point column at `<point-path>`, so `geo_distance` / `geo_bbox`
  predicates in a `WHERE` scan a small set of code ranges instead of the whole
  table — no query change, the index is used transparently when present. It
  broadcasts across the cluster (each node indexes its shard), maintains itself
  on write, backfills existing rows in the background, and persists on disk (no
  rebuild on restart). There is nothing to configure; a row whose column is not
  a readable point is simply not indexed. See [GEO.md](GEO.md).
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
- `ALTER TABLE <t> SET (witness = <bool>)` toggles witness mirroring for an
  existing table. `WITH (witness = false)` at CREATE (or the ALTER form)
  excludes the table from witness pulls: witnesses skip it entirely (rows
  already mirrored are kept but stop updating), and the table stops holding
  back tombstone GC for lagging witnesses. Toggling back to `true` re-includes it on the next full sweep.
  System/registry tables (`witnesses`, `drivers`, `node_aliases`, ...) refuse
  placement/witness options — every node consults them locally.
- **Per-table placement**: `replication = <n>` overrides the cluster
  replication factor for one table (n above the member count = full copy,
  the same semantics cluster-wide RF has); `nodes = ['<alias-or-id>', ...]`
  pins the WHOLE table to an explicit member set (entries accept node
  aliases as sugar, resolved to stable ids at DDL time; a reference that
  is not a current member is refused, and witnesses can never be pinned) — every pin holds every row,
  quorum math counts the pins, and a non-pin coordinator routes reads and
  writes to them. The two are mutually exclusive. Pins are a deliberate
  durability trade: a pinned node down means quorum errors for that table
  until it returns, and `ALTER CLUSTER REMOVE NODE` refuses to remove a
  pinned member (re-pin first with `ALTER TABLE t SET (nodes = [...])`).
  GLOBAL-index entry tables follow their base table's placement.
- **Placement transitions**: `ALTER TABLE t SET (replication = n)` /
  `SET (nodes = [...])` change placement ONLINE. The old placement is
  kept alongside the new one and every read/write addresses the UNION
  of both (the per-table twin of the membership change's dual-ring
  window), so quorum reads stay correct while new owners are still
  empty. A background driver (the sorted union's first member) runs
  repair until every member has completed a full anti-entropy pass that
  started after the change, then finalizes automatically; `SHOW TABLES`
  shows `transition = true` while the window is open. One transition
  per table at a time. If the driver is down the window just stays open
  (safe, wider than needed) — the operator escape is `REPAIR CLUSTER`
  followed by `ALTER TABLE t SET (placement_finalized = true)`. After
  finalize, `RECLAIM` trims copies the new placement no longer owns.
  Shrinking a pin set needs no window (remaining pins already hold full
  copies) and applies immediately.
- **`SHOW TABLES`** lists catalog tables as
  `(table, primary_key, replication, nodes, witness, transition)` rows —
  `replication`/`nodes` per-table placement, `witness` the mirroring
  flag, `transition` whether a placement change's dual-placement window
  is currently open;
  **`SHOW INDEXES`** lists secondary, vector, and search indexes as
  `(index, table, kind, columns, local)` — `local` is **this node's** live
  state for the index: `ok` (open and serving), `building` (backfill or
  catch-up running), or `missing` (in the catalog but no live index — the
  divergence that used to be invisible and made a per-node search outage a
  multi-day mystery). Both are read-only and require no special privilege,
  so a monitoring/tooling agent can enumerate the schema without `/query`
  data access. In cluster mode they answer from the local catalog — ask each
  node to compare `local` states across the ring.
- **`DESCRIBE <table>`** (alias **`DESC <table>`**) is one table's structure as
  `(column, key, indexes)` rows — one per column that is part of the primary key
  or an index. `key` is `primary key` (with a `(n/m)` position for a composite
  key); `indexes` lists the covering indexes as `name (kind)` (`secondary`,
  `secondary, building`, `vector`, `search`), and a MULTIKEY `[]` suffix is
  stripped from the column name. Rows come out primary-key columns first (in key
  order), then the remaining indexed columns alphabetically. Like the `SHOW`
  pair it is read-only, needs no privilege, and answers from the local catalog —
  no data is read. **The store is schema-less, so a column that is neither part
  of the key nor indexed is not in the catalog and does not appear here.** An
  unknown table is an error. Accepts a `db.table` qualifier.
  - **`DESCRIBE <table> FULL [SAMPLE n | EXACT]`** additionally reads the data
    to surface **every** field — the schema-less fields the catalog can't know.
    It returns `(column, type, key, indexes)`: the added `type` is the set of
    value types seen for that field, joined by ` | ` (a field can hold several
    types). Because it reads rows, `FULL` requires the `SELECT` privilege (plain
    `DESCRIBE` does not) and, on a cluster, reads the local shard (complete when
    RF ≥ members). Time-series tables report catalog columns only (blank types).
    - Default/**`SAMPLE n`**: reads the first `n` rows in primary-key order
      (default `1000`), streamed so it reads at most `n` rows and never
      materializes the table — a field that appears only in rows outside the
      sample is not seen; widen `SAMPLE` to trade cost for completeness.
    - **`EXACT`**: scans all rows (streamed — memory stays bounded regardless
      of table size) and caches the field map in a RAM **field registry**,
      stamped with the table's write sequence. Repeated `EXACT`s on an
      unchanged table answer from RAM in O(fields); any write, delete, or
      reclaim to the table invalidates the stamp so the next call rescans —
      results are always exact, including fields *disappearing* when their last
      row is deleted. The registry is process-local (a restart clears it; the
      first `EXACT` after rebuilds) and is skipped for TTL tables, whose row
      visibility decays without writes — `EXACT` on those rescans every call.
- **Admin statements** (`SHOW CLUSTER`/`SHOW CONFIG`/`SET CONFIG`/
  `SHOW SLOW QUERIES`/`REPAIR CLUSTER`/`RECLAIM`/`ALTER CLUSTER`) are the
  SQL spellings of the HTTP `/admin/*` control plane — identical handler,
  RBAC (reads: `MONITOR` on `*`; mutations: `ADMIN` on `*`), and audit;
  results come back as `(key, value)`
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
- **`DISTINCT`** removes duplicate output rows. `SELECT DISTINCT <one
  plain column>` (no ORDER BY/GROUP BY/JOIN) streams the value set without
  materializing rows — safe on tables of any size; an array-valued column
  dedupes whole arrays. **`HAVING`** filters groups after
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
  mode each statement autocommits and transaction control returns an error
  (there is no distributed transaction coordinator). DDL is not transactional.

- **Access control.** `<privilege>` is one of `SELECT`, `INSERT`, `UPDATE`,
  `DELETE`, `CREATE`, `DROP`, `GRANT`, `MONITOR`, `ADMIN` (`ADMIN` on `*` =
  superuser; `ADMIN` on a table implies every privilege on it). `MONITOR`
  (granted `ON *`) opens the **read-only** control plane — `SHOW CLUSTER`,
  `SHOW CONFIG` (secrets stay masked), `SHOW SLOW QUERIES`, and the matching
  read-only HTTP admin endpoints — so an application role can report cluster
  health and its effective config without an admin credential; it never
  authorizes a mutation (`SET CONFIG`, repair, membership stay `ADMIN`).
  **Index DDL is table-scoped:** `CREATE INDEX` needs `CREATE` on the target
  table, and `DROP INDEX` / `REBUILD`/`ALTER` of an index need that same
  `CREATE` on the index's **owning table** — an index is derived data, so a
  role that can create its own indexes can also drop or retune them (no
  `DROP on Global` needed; `DROP TABLE` still requires `DROP`). Dropping a
  nonexistent index needs no privilege (`IF EXISTS` stays an idempotent
  no-op; index existence is already free via `SHOW INDEXES`). A **user** authenticates
  (SCRAM on the binary protocol, HTTP Basic on REST) and acts as its
  own-named role; `GRANT ROLE r TO u` adds inherited roles. A user created
  `… GSSAPI` is **external**: it has no local password — a Kerberos KDC
  vouches for the principal, and skaidb only maps the authenticated principal
  to its own-named role (grants and role inheritance work identically). An
  external user cannot authenticate by password/SCRAM, and a password user is
  never reachable through the external path. A grant
  `ON DATABASE db` covers every table in that database (checked against the
  session's current database; shown by `SHOW GRANTS` as `db:<name>`).
  **A table grant is scoped to the table's canonical `<database>.<table>`
  identity, not the raw name.** A bare `GRANT … ON t` issued while the
  session is in database `d` grants `d.t` — never a wildcard that would
  match a same-named `t` in another database — and a qualified
  `GRANT … ON d.t` authorizes the natural `USE d; … t` query just as it
  authorizes `… FROM d.t` (both resolve to the same table). `SHOW GRANTS`
  renders the object as `<database>.<table>` (bare for the default
  database). All
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

- **Array membership:** comparing an array-valued column to a non-array
  scalar tests containment: `labels = 'work'` matches rows
  whose array holds `'work'`; `!=` is not-contains. Array-to-array
  comparison remains whole-value equality.
- **Literals:** integer (`42`), float (`3.14`, scientific `1.2e-5`/`3E8`), string (`'ada'`), `TRUE`,
  `FALSE`, `NULL`, **array** literals `[<lit> [, <lit> ...]]` (constant
  elements only — a literal or a negated number, e.g. `[0.1, -0.2, 0.3]`),
  and **object/document** literals `{<key>: <lit> [, ...]}` — the way to write
  a nested document in `SET`/`VALUES`. Keys are bare identifiers or string
  literals (quote reserved words: `{'from': 1}`); values must be constant and
  may nest arrays and objects: `{name: 'ada', tags: ['x'], addr: {city: 'paris'}}`.
  A duplicate key keeps the last value; `{}` is the empty document. Assigning
  an object literal to a path (`SET meta.addr = {…}`) **replaces** that whole
  sub-document (sibling fields outside the path survive); dotted-path `SET`
  (`SET meta.addr.city = 'x'`) remains the idiom for updating a single scalar
  leaf in place.
- **Column / field paths:** `name`, or a dotted path into a nested document:
  `address.city`, `a.b.c`. Usable in projections, `WHERE`, `GROUP BY`,
  `ORDER BY`, and `UPDATE … SET` targets.
- **Operators**, by increasing precedence:
  1. `OR`
  2. `AND`
  3. `NOT <expr>`
  4. comparison: `=`, `!=` (or `<>`), `<`, `<=`, `>`, `>=`
  5. `<expr> IS [NOT] NULL`
  6. postfix predicates: `<expr> [NOT] IN (<expr> [, ...])`,
     `<expr> [NOT] BETWEEN <expr> AND <expr>`,
     `<expr> [NOT] { LIKE | ILIKE } <pattern-expr>`
  7. additive: `+`, `-`
  8. multiplicative: `*`, `/`
  9. unary: `-<expr>`, `NOT <expr>`
  10. parentheses `( … )`

  `in`, `between`, `like`, and `ilike` are contextual keywords — still usable
  as column names outside the operator position.
- **`IN` / `NOT IN`** — set membership: `x IN (a, b, c)` is true when `x`
  equals any listed element (three-valued: unknown if `x` is `NULL`, or if
  no element matches but some element is `NULL`); `NOT IN` negates it. The
  list needs at least one element (`IN ()` is a parse error). A list element
  that is an **array** is flattened — each of its elements becomes a
  candidate — so a bound array parameter works directly: `WHERE id IN (?)`
  with `?` = `[1, 2, 3]` tests membership in that set (the "fetch these N
  ids" pattern). When the left side is an array column, `IN` matches if the
  array holds any listed value, mirroring the `=` containment rule above.
  > **Performance:** when **every primary-key column** is pinned by `=` or a
  > literal `IN` list (bound array parameters included), the query resolves
  > as a **point-read set** — one bloom-gated point read per candidate key
  > (cross product on composite keys, capped at 1000 keys), routed to each
  > key's replica set on a cluster; `EXPLAIN` shows `point-read set`. Other
  > `IN` shapes (non-PK columns, `NOT IN`, non-literal elements) evaluate as
  > a residual row filter over the scanned range and can trip
  > `scan budget exceeded` on large unindexed scans — pair those with a
  > narrowing indexed predicate.
- **`BETWEEN`** — `x BETWEEN lo AND hi` is the inclusive range
  `x >= lo AND x <= hi` (three-valued: a `NULL` operand or bound makes the
  undecided side unknown); `NOT BETWEEN` negates it. A `col BETWEEN lo AND hi`
  with literal bounds contributes both bounds to index/PK **range pushdown**,
  exactly like writing the two comparisons.
- **`LIKE` / `ILIKE`** — SQL pattern match on strings: `%` matches any run of
  characters (including none), `_` matches exactly one; every other character
  matches itself, and there is **no escape sequence** (a literal `%`/`_` in the
  data cannot be targeted — use equality or a search index). `ILIKE` is
  case-insensitive (Unicode-aware lowercasing). The pattern is any expression
  (usually a literal or a `?` parameter). Non-string operands — including
  `NULL` — compare as unknown, so a mixed-type column never errors the query.
  This is **exact substring/prefix matching**, complementing the analyzed
  full-text `MATCH()` (which tokenizes words): use `LIKE '%needle%'` for
  verbatim fragments, `MATCH` for word search.
  > **Performance note:** `LIKE`/`ILIKE` evaluate as a residual row filter
  > (no index acceleration, including `'prefix%'` for now) — the same
  > scan-budget caveat as `IN` applies on large unindexed scans.
- **Aggregate functions:** `COUNT(*)` / `COUNT(<expr>)` /
  `COUNT(DISTINCT <expr>)` (exact distinct non-null values),
  `APPROX_COUNT_DISTINCT(<expr>)` (opt-in approximate distinct: the
  search-index pushdown may answer from an HLL sketch — never truncates,
  ~±1–2% at high cardinality; every other path answers exactly), `SUM`,
  `AVG`,
  `MIN`, `MAX`, `PERCENTILE(<expr>, <p>)` (the linear-interpolated
  `percentile_cont` quantile of the group's numeric values; `<p>` is a
  literal fraction in `(0, 1]`, e.g. `PERCENTILE(latency_ms, 0.95)` —
  computed exactly over the gathered rows), and the time-series aggregates
  `RATE`, `INCREASE`, `DELTA`,
  `FIRST`, `LAST` (see *Time-series tables* below). Using an aggregate (or
  `GROUP BY`) puts the query in aggregate mode.
- **Duration literals:** an integer immediately followed by a unit — `250ms`,
  `15s`, `5m`, `2h`, `30d`, `1w` — is a duration, valued as integer
  milliseconds (`5m` = `300000`). Usable anywhere an integer is.
- **Scalar functions:** `distance('<n><unit>')` (a distance literal in
  metres — `'500m'`, `'5km'`, `'1mi'`, `'3NM'`; the natural radius for
  `geo_distance` comparisons, and constant, so the geo index still prunes:
  `WHERE geo_distance(loc, 40.7, -74.0) <= distance('5km')`),
  `now()` (the query's start time, as a timestamp —
  one instant per statement), `time_bucket(<step>, <ts>)` (floors `<ts>`
  to a `<step>`-wide bucket: `time_bucket(5m, ts)`), and
  `to_timestamp(<value>)` — coerce to a timestamp: numeric epoch-ms passes
  through and an ISO-8601 string is parsed (`YYYY-MM-DD`,
  `YYYY-MM-DD[T ]HH:MM[:SS[.fff]]`, optional `Z`/`±HH[:MM]` offset; no offset
  = UTC). Unparseable or mistyped input yields `NULL`, never an error — so
  string timestamps (e.g. from a document-store migration) range-filter in-query:
  `WHERE to_timestamp(created_at) >= now() - 30d`. The standard spelling
  **`CAST(<expr> AS <type>)`** (types: `INT`, `FLOAT`, `STRING`, `BOOL`,
  `TIMESTAMP`) desugars to the matching coercion (`to_int`, `to_float`,
  `to_string` — timestamps render as ISO-8601 —, `to_bool`,
  `to_timestamp`), all with the same NULL-on-unconvertible policy. `cast`
  stays usable as a column name. A bare identifier
  directly followed by `(` parses as a function call; unknown functions are
  execution errors. The full-text search functions `MATCH`, `MATCH_PHRASE`,
  `FUZZY`, `SEARCH`, and `score` parse the same way and are only valid in a
  search query (see *Full-text search* below).
- **Geospatial:** `geo_distance(<point>, <lat>, <lon>)` returns the great-circle
  (haversine) distance in **metres** from a point column to `(<lat>, <lon>)`;
  `geo_bbox(<point>, <min_lat>, <min_lon>, <max_lat>, <max_lon>)` tests whether
  a point lies inside a bounding box (`min_lon > max_lon` crosses the
  antimeridian). A point is a `{lat, lon}` object (`lng` also accepted) or a
  `[lat, lon]` array; a non-point / NULL value yields `NULL` (so it drops from a
  filter) rather than erroring. Use them anywhere:
  `WHERE geo_distance(loc, 40.71, -74.0) <= 5000`,
  `ORDER BY geo_distance(loc, 40.71, -74.0) LIMIT 10` (nearest-first), or
  `WHERE geo_bbox(loc, 40.4, -74.3, 40.9, -73.7)`. A **`CREATE GEO INDEX`**
  (below) makes these predicates prune to a neighborhood instead of scanning the
  whole table; without one they still work over a (scan-budget-bounded) scan.
- **Bind parameters:** `?` marks a positional parameter in any expression
  position of a **prepared** `SELECT`/`INSERT`/`UPDATE`/`DELETE` (e.g.
  `INSERT INTO t (id, v) VALUES (?, ?)`), and additionally in `LIMIT ?` /
  `OFFSET ?` (the bound value must be a non-negative integer). Parameters
  are numbered by order of appearance and bound to values at execute time
  via the binary protocol's prepared-statement messages (or a driver's
  `prepare`/`execute` API). `EXPLAIN` of a preparable statement is itself
  preparable, so the exact bound query can be explained. DDL and
  session-control statements cannot be prepared, and a `?` submitted
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
`document`. Only `int`, `float`, `string`, `bool`, `null`, `array`, and
`document` (object) literals can be written directly in SQL;
`decimal`/`uuid`/`bytes`/`timestamp` values arrive via stored data or the
value codec (e.g. bound parameters), not as SQL literals.

## Vector search (`NEAREST`)

`NEAREST (<path>, <query-vector>, <k>)` runs an approximate nearest-neighbor
search over a [vector index](VECTOR.md) on `(<table>, <path>)`, returning the
`<k>` closest rows ordered nearest-first, with the match distance exposed as a
`_distance` field:

```sql
CREATE VECTOR INDEX docs_emb ON docs (embedding) DIM 3 USING cosine;
SELECT id, _distance FROM docs NEAREST (embedding, [1.0, 0.0, 0.0], 5);
SELECT id FROM docs NEAREST (embedding, [1.0, 0.0, 0.0], 5) WHERE cat = 'news';

-- Managed (semantic_text) — embed a TEXT column via the [inference] provider:
CREATE VECTOR INDEX docs_sem ON docs (body) EMBED DIM 768;
SELECT id FROM docs NEAREST (body, 'natural language query', 10);  -- query auto-embedded
```

`EMBED` makes the index **managed**: `path` names a TEXT column that skaidb
embeds via the configured `[inference]` endpoint (rather than reading a
pre-computed vector array), and a **string** `NEAREST` query on it is
auto-embedded. Embedding is out of band — a write commits with the raw text and
a background worker embeds it, so the model server being down delays
searchability but never blocks or fails a write. Needs `[inference]` enabled and
`DIM` matching the model; a managed index errors at create otherwise.

`<query-vector>` and `<k>` may be literals or bind parameters (`?`) in a
prepared statement. Requires a vector index on the path; errors if none
exists. Cannot combine with `JOIN`, `UNION`, aggregates/`GROUP BY`, or
`ORDER BY` (results are already ordered by distance) — `WHERE`, `LIMIT`, and
`OFFSET` apply normally, post-search.

## Hybrid search (`RANK BY RRF`)

Fuse a full-text leg and a vector leg in one query by **Reciprocal Rank
Fusion**. The `NEAREST` clause supplies the vector leg, the `WHERE` search
predicate supplies the text leg, and `RANK BY RRF` fuses them:

```sql
SELECT id, rrf_score() FROM docs
NEAREST (embedding, [1.0, 0.0, 0.0], 100)   -- vector leg (100 candidates)
WHERE MATCH(body, 'quick brown fox')         -- text leg
RANK BY RRF                                  -- fuse (default constant 60)
LIMIT 10;
```

Each leg fetches the `NEAREST` `<k>` candidates, then a hit at 1-based rank
`r` in a leg contributes `1 / (c + r)` to its fused score; `rrf_score()`
exposes the sum. Because fusion is rank-based, BM25 scores and vector
distances need no normalization. Results are ordered by `rrf_score()`
descending; `LIMIT` takes the final top-k.

- `RANK BY RRF (<c>)` overrides the rank constant (default 60; must be ≥ 1).
- The **residual** (non-search) part of `WHERE` filters **both** legs, e.g.
  `WHERE MATCH(body, 'fox') AND published = true`.
- Requires both a `NEAREST` clause and a search predicate in `WHERE`. Cannot
  combine with `JOIN`, `UNION`, `DISTINCT`, `GROUP BY`, or `ORDER BY`
  (ordering is `rrf_score()` desc). Works cluster-wide (both legs
  scatter-gather; the coordinator fuses).

## Reranking (`RERANK`)

Second-stage relevance: retrieve the top candidates by BM25 (or hybrid RRF),
send `(query, document)` pairs to an external **cross-encoder rerank
endpoint** (`[inference] rerank_url` — see [VECTOR.md](VECTOR.md)), and
return the page in the reranker's order:

```sql
SELECT id, title, score() FROM docs
WHERE MATCH(body, 'how do I tune compaction')
RERANK TOP 100                               -- rerank the top 100 BM25 hits
LIMIT 10;
```

`RERANK [ON <col>] [WITH '<model>'] [QUERY '<text>'] [TOP <n>]` — every part
is optional:

- `ON <col>` — the column whose text is sent as each candidate's document.
  Default: the columns the search predicate targets (all string fields for a
  field-less `SEARCH('…')`). Document text is capped at 4000 chars.
- `WITH '<model>'` — the model name sent to the endpoint. Default:
  `inference.rerank_model`.
- `QUERY '<text>'` — the query the reranker scores against. Default: the
  search predicate's text (e.g. the `MATCH` string).
- `TOP <n>` — the candidate window (default 100, capped at 1000). `LIMIT`/
  `OFFSET` then page the reranked list.

`score()` reads the rerank score. Composes with hybrid retrieval — `… RANK BY
RRF RERANK TOP 50 LIMIT 10` reranks the fused top-50 (`rrf_score()` keeps the
fusion score). Requires a search predicate in `WHERE`; cannot combine with
`ORDER BY` (the reranker orders results), `GROUP BY`, or aggregates. Runs
**coordinator-side** over the already-gathered candidates, cluster-wide; it
is opt-in per query, so a down rerank endpoint fails only queries that ask
for it. ES clients reach the same path via the `text_similarity_reranker`
retriever ([SEARCH.md](SEARCH.md)).

## Deep pagination (`AFTER`)

Stable keyset pagination for search queries — the SQL analogue of
Elasticsearch's `search_after`. Instead of `OFFSET` (which re-fetches and
discards every earlier row, and shifts under concurrent writes), pass the
previous page's **last sort value and last primary-key value**:

```sql
-- page 1
SELECT id, title, score() FROM docs
WHERE MATCH(body, 'compaction') ORDER BY score() DESC LIMIT 10;
-- page 2: strictly after (last score, last id) of page 1
SELECT id, title, score() FROM docs
WHERE MATCH(body, 'compaction') ORDER BY score() DESC LIMIT 10
AFTER (0.7311, 'doc-141');
```

Works with `ORDER BY score() DESC` (cursor = last score) or `ORDER BY <col>
[ASC|DESC]` (cursor = last column value). The **primary key is the implicit
tie-break, always ascending** — every sorted search page is ordered
`(sort value, pk)` deterministically, so ties never straddle a page boundary
inconsistently. Rules:

- Requires a search predicate in `WHERE`, exactly one `ORDER BY` key, and
  `LIMIT`; a **single-column** primary key; mutually exclusive with `OFFSET`,
  `GROUP BY`/`DISTINCT`, `RANK BY RRF`, and `RERANK`. Filter-only (non-search)
  queries should keyset-paginate with `WHERE <col> > <last>` instead.
- The cursor is resolved by re-ranking: the fetch depth doubles until the page
  fills or the match set is exhausted (capped at 65,536 ranked hits), so a
  page costs about the same as the equivalent `OFFSET` fetch — the win is
  **stability** (concurrent writes never shift or duplicate pages; a row
  inserted before the cursor simply never appears) plus no offset ceiling.
- Score-sorted deep paging inherits the usual distributed-search caveat
  (per-shard BM25 statistics; scores move if the index changes between
  pages) — the same caveat ES documents for `search_after` without a PIT.

ES clients use the `_search` `search_after` parameter, echoing each hit's
`sort` array ([SEARCH.md](SEARCH.md)).

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
  SELECT/DML statement, as `(aspect, decision)` rows. The `access` row
  names the path: `point read` / `point-read set (primary-key =/IN, N
  keys)` / `index scan via '<name>' (<bounds>)` / `index-ordered walk via
  '<name>' (… early-stop at LIMIT)` / `global-index probe via '<name>'
  (routed to the value's replica set)` / BM25 top-k, index-ordered or
  unranked search pushdown / search-aggregation pushdown vs. row-gather
  fallback / HNSW vector search / `full table scan (streaming k-way
  merge)`. An `order` row notes ORDER BY strategy (index-served vs top-k
  selection); residual-filter and join-strategy rows follow, and — on a
  cluster — `cluster.*` rows (members, replication factor, fan-out:
  point-routed / global-probe-routed / served locally on full copy /
  scatter-gather). Advisory: it mirrors the planner's decision logic
  without executing anything, so `EXPLAIN DELETE ...` is safe. Bindable
  (`EXPLAIN SELECT … WHERE id = ?` prepares like the statement it wraps).
  Gated by the wrapped statement's own privilege. `EXPLAIN EXPLAIN` is
  rejected. Worked example with output:
  [INDEXING.md](INDEXING.md#global-value-sharded-indexes).
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
- **`HIGHLIGHT(<col> [, <max_chars> [, <pre_tag>, <post_tag> [, <no_match_size>]]])`**
  projects the best-scoring snippet of the column's text (default fragment
  size 150 chars), matches wrapped in tags (`<b>…</b>` by default, HTML-escaped
  otherwise; empty string if the column didn't match). A `pre_tag`/`post_tag`
  string pair overrides the markers (ES `pre_tags`/`post_tags`); a trailing
  `no_match_size` returns that many leading characters when nothing matched
  (ES `no_match_size`). Only valid together with a search predicate.
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
  1 s, near-real-time); on the single-node write path a
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
internals in [TIMESERIES.md](TIMESERIES.md)). Distributed: the DDL broadcasts, series place on the
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
