# Full-text search

Native full-text search over skaidb tables: BM25-ranked retrieval with
`MATCH()` / `SEARCH()` SQL predicates, backed by an embedded
[Tantivy](https://github.com/quickwit-oss/tantivy) index per search index.
This documents the **shipped** state; the plan and pending phases live in
[FTS_TODO.md](FTS_TODO.md), and the SQL grammar in
[QUERY_SYNTAX.md](QUERY_SYNTAX.md#full-text-search-match--search).

Status: **phase 1 (single-node core)** — DDL, index maintenance on
put/delete, `MATCH`/`MATCH_PHRASE`/`FUZZY`/`SEARCH` predicates, `score()`,
top-k pushdown, crash recovery, rebuild.

## Using it

```sql
CREATE SEARCH INDEX articles_fts ON articles (title, body)
  WITH (analyzer = 'english', refresh_ms = 1000);

SELECT id, title, score() FROM articles
WHERE MATCH(body, 'quick brown fox') AND published = true
ORDER BY score() DESC LIMIT 10;

SELECT id FROM articles
WHERE SEARCH('title:"rust database" +body:performance -draft')
ORDER BY score() DESC LIMIT 20;

REBUILD SEARCH INDEX articles_fts;   -- re-index from the table
DROP SEARCH INDEX articles_fts;
```

- An index covers one or more **document paths** (dotted paths into nested
  documents work: `meta.title`). String values are indexed; arrays of
  strings index every element. Non-string values are skipped (numeric/date
  fast fields are a later phase).
- **Analyzers** (`analyzer = '...'`): `standard` (word split + lowercase,
  the default), `english` (standard + stopwords + Snowball stemming),
  `whitespace` (split only, case kept), `keyword` (whole value as one
  term). Query text is analyzed with the same pipeline at query time.
- `refresh_ms` (default 1000) controls how quickly writes become
  searchable — Elasticsearch-style near-real-time. On the single-node
  write path, a search after a write commits the index first, so you read
  your own writes immediately.

## Architecture

- **`skaidb-fts` crate** wraps Tantivy behind an engine-agnostic API
  (skaidb `Document`s in, `(key, score)` hits out); no Tantivy types cross
  the crate boundary, so the engine and SQL layers stay independent of the
  search core.
- **Derived data over the LSM table**, like the vector indexes: the index
  registers in the catalog (schema-version prefix `s:<name>`, replicated
  like other DDL), and every `put`/`delete` maintains it alongside
  secondary and vector indexes. The table remains the source of truth — a
  lost, stale, or mis-configured index **rebuilds from the table**.
- **Durability — the row WAL is the translog.** Index writes apply
  immediately but commit lazily (on the `refresh_ms` cadence). Each commit
  atomically persists the max row HLC it contains (the *watermark*) as the
  Tantivy commit payload. On open, the engine replays table rows (and
  tombstones) newer than the watermark into the index — so a crash, or a
  clean shutdown with uncommitted index writes, loses nothing. There is
  deliberately no commit-on-shutdown: the replay path runs on every open,
  keeping recovery constantly exercised.
- **Storage layout**: Tantivy segments live under `<data_dir>/fts/<index>/`
  (mmap'd for search; the writer heap is bounded, 64 MB in phase 1 —
  `memory_target` integration is phase 5).
- **Query pushdown**: `ORDER BY score() DESC LIMIT k` retrieves top-k
  directly from the index (early-terminated, no scan). Residual `WHERE`
  conditions are applied after the authoritative row re-read, with
  over-fetch to keep k results (the vector-search discipline). A `MATCH`
  used as a plain predicate (no ranking) retrieves the matching key set
  from the index.
- **Cluster**: DDL broadcasts (every node indexes its shard from replicated
  writes); scatter-gather top-k merge across members is phase 4 — today a
  multi-node coordinator rejects search queries, single-node serves them.

## Observability

- `SHOW INDEXES` lists search indexes with their analyzer and columns.
- `SHOW STATUS` rows: `search_indexes`, `search_docs`, `search_rebuild_ms`,
  and per-index `search.<name>.{docs,disk_bytes,uncommitted}`.
- `/metrics` gauges: `skaidb_search_indexes`, `skaidb_search_docs_total`,
  `skaidb_search_disk_bytes`, `skaidb_search_rebuild_seconds`.

## Limits (phase 1)

- Search predicates must be top-level `AND` conditions; `OR`/`NOT` around
  them is rejected (full bool composition is phase 3).
- `ORDER BY score() DESC` is the only ordering usable with search
  predicates and requires `LIMIT`.
- No `JOIN`, `UNION`, aggregates/`GROUP BY`, `DISTINCT`, or `NEAREST` in
  the same query.
- Per-shard BM25 statistics (like Elasticsearch's default); a global-stats
  mode is a later phase.
- Highlighting (`HIGHLIGHT()`), multi-field boosts, per-field analyzers,
  numeric/date fast fields, aggregations, and suggesters land in phases
  2–7 (see [FTS_TODO.md](FTS_TODO.md)).
