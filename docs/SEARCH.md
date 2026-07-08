# Full-text search

Native full-text search over skaidb tables: BM25-ranked retrieval with
`MATCH()` / `SEARCH()` SQL predicates, backed by an embedded
[Tantivy](https://github.com/quickwit-oss/tantivy) index per search index.
This documents the **shipped** state; the plan and pending phases live in
[FTS_TODO.md](FTS_TODO.md), and the SQL grammar in
[QUERY_SYNTAX.md](QUERY_SYNTAX.md#full-text-search-match--search).

Status: **phase 4 (cluster)** — phases 1–3 (single-node core: DDL, index
maintenance on put/delete, `score()`, top-k pushdown, crash recovery,
rebuild; analysis & mappings: analyzer registry, per-column configuration,
typed fast fields, `.keyword` twins, `copy_to`; query DSL: the full
predicate family `MATCH`/`MATCH_PHRASE`/`MATCH_PREFIX`/`FUZZY`/`WILDCARD`/
`REGEXP`/`SEARCH`, AND/OR/NOT composition, dis-max multi-field scoring,
`HIGHLIGHT()` snippets) plus **distributed search**: scatter-gather top-k
across the cluster, per-replica local indexes over replicated writes, and
index continuity through join/decommission/rebalance/repair.

## Using it

```sql
CREATE SEARCH INDEX articles_fts ON articles (title, body, year, published)
  WITH (analyzer = 'english', refresh_ms = 1000,
        title.boost = 2.0, title.keyword = true,
        title.copy_to = 'everything', body.copy_to = 'everything',
        year.type = 'long', published.type = 'bool');

SELECT id, title, score(), HIGHLIGHT(body, 120) AS snippet FROM articles
WHERE MATCH(body, 'quick brown fox') AND published = true
ORDER BY score() DESC LIMIT 10;

SELECT id FROM articles
WHERE SEARCH('title:"rust database" +body:performance year:[2020 TO 2024]')
ORDER BY score() DESC LIMIT 20;

-- Search predicates compose with AND/OR/NOT:
SELECT id FROM articles
WHERE (MATCH(body, 'rust') OR MATCH(title, 'rust')) AND NOT MATCH(body, 'draft');

REBUILD SEARCH INDEX articles_fts;   -- re-index from the table
DROP SEARCH INDEX articles_fts;
```

- An index covers one or more **document paths** (dotted paths into nested
  documents work: `meta.title`). Arrays index every element (multi-valued
  fields). Rows are schema-less — **the declaration is the mapping**: a
  value that doesn't fit its column's declared type is simply not indexed
  for that column.
- `refresh_ms` (default 1000) controls how quickly writes become
  searchable — Elasticsearch-style near-real-time. On the single-node
  write path, a search after a write commits the index first, so you read
  your own writes immediately.

## Analyzers

Set the index default with `analyzer = '...'`, or per column with
`<column>.analyzer = '...'`:

- `standard` — word split + lowercase (the default). One documented
  divergence from Elasticsearch's `standard`: ES uses Unicode word
  segmentation and keeps `dog's` whole; our simple tokenizer splits on the
  apostrophe (`dog`, `s`).
- `folding` — `standard` + ASCII folding (`café` → `cafe`) for
  accent-insensitive matching without stemming.
- **Languages** (standard + stopwords where a list exists + Snowball
  stemmer): `arabic`, `danish`, `dutch`, `english`, `finnish`, `french`,
  `german`, `greek`, `hungarian`, `italian`, `norwegian`, `portuguese`,
  `romanian`, `russian`, `spanish`, `swedish`, `tamil`, `turkish`.
- `whitespace` — split only, case kept.
- `keyword` — the whole value as one term.
- `ngram(min,max)` — lowercased character ngrams (substring matching).
- `edge_ngram(min,max)` — lowercased prefix ngrams (search-as-you-type);
  pair with `search_analyzer = 'standard'` so queries aren't ngrammed too.

Query text is analyzed with the field's **query-time** analyzer:
`<column>.search_analyzer` if set, else the index-time analyzer.

## Per-column options

`WITH (...)` takes global options (`analyzer`, `refresh_ms`) and
`<column>.<option>` per-column options:

| option | meaning |
|---|---|
| `<col>.type` | `text` (default), `keyword`, `long`, `double`, `bool`, `date` |
| `<col>.analyzer` | index-time analyzer for this text column |
| `<col>.search_analyzer` | query-time analyzer override |
| `<col>.boost` | score multiplier in multi-field queries (positive number) |
| `<col>.keyword` | `true` adds a `<col>.keyword` exact-match twin |
| `<col>.copy_to` | also index this text into a named composite field |

- **Typed columns** (`long`, `double`, `date`, `bool`) become fast fields,
  addressable from the `SEARCH()` query-string language (`year:1999`,
  `price:[30 TO *]`, `published:true`). `double` accepts integer values;
  `date` accepts `timestamp` and millisecond-integer values. `MATCH()` on a
  non-text column is an error.
- **`.keyword` twins** index the raw string alongside the analyzed text:
  `MATCH(title, 'rust handbook')` matches analyzed terms while
  `MATCH(title.keyword, 'Rust Handbook')` matches only the exact original
  string.
- **`copy_to`** aggregates several columns into one searchable composite
  field (analyzed with the index default) — the ES `copy_to` pattern for
  "search everything" fields. Several columns may share one target.
- Options are validated at `CREATE` time; unknown options, unknown
  analyzers, or analyzer/keyword/copy_to options on non-text columns error.
- Changing columns, types, or index-time analyzers requires a rebuild (the
  engine rebuilds automatically on open if the definition changed);
  `search_analyzer` is query-time-only and needs none.

## Predicates

Analyzed predicates (query text goes through the field's query-time
analyzer):

- `MATCH(col, 'text')` — any analyzed term matches (ES `match`).
- `MATCH_PHRASE(col, 'text' [, slop])` — terms in order within `slop`
  transpositions (ES `match_phrase`).
- `FUZZY(col, 'text' [, distance])` — Levenshtein ≤ 2 per term (ES `fuzzy`).
- `SEARCH('query-string')` — the mini-language: bare terms over text
  columns, `"phrase"`, `col:term`, `+must`, `-must_not`, `AND`/`OR`, and
  ranges over typed columns (`year:[2020 TO 2024]`, `published:true`).

Term-level pattern predicates (**not analyzed** — they run against the
indexed terms, so with a lowercasing analyzer write patterns lowercase):

- `MATCH_PREFIX(col, 'qu')` — term prefix (ES `prefix`).
- `WILDCARD(col, 'qu*ck')` — `*` any run, `?` any one char (ES `wildcard`).
- `REGEXP(col, 'qu.[ck]+')` — regular expression (ES `regexp`).

**Composition**: search predicates combine freely with `AND`/`OR`/`NOT`
among themselves (ES bool `must`/`should`/`must_not`); ordinary SQL
conditions join at the top level with `AND` and filter the hits afterward.
Mixing a search predicate with an ordinary condition under `OR`/`NOT` is
rejected — the index cannot serve it. A `NOT` search returns only rows the
index knows: a row with none of the indexed columns present is never
returned.

**Multi-field scoring** is dis-max (a row scores as its best field, ES
`best_fields`), with per-column boosts applied.

**Highlighting**: `HIGHLIGHT(col [, max_chars])` in the projection returns
the best-scoring snippet of the column's text (default 150 chars) with
matching terms wrapped in `<b>…</b>` (HTML-escaped otherwise, empty string
when the column didn't match). Stemming is respected — a query for
`jumping` highlights `jumps`. Only valid together with a search predicate.

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
- **Cluster** (the vector-search pattern): DDL broadcasts, and every member
  indexes its shard locally from replicated writes — the replicated apply
  paths (`apply_put`/`apply_delete`, batched) maintain search indexes, so
  replication, rebalance, drain, hinted replay, and anti-entropy repair all
  keep the index in step with the table for free. A query scatters to all
  members; each answers with its local `(key, score)` top-k after
  committing pending index writes (writes replicate synchronously at the
  write consistency, so **every acked write is searchable cluster-wide**,
  not just NRT). The coordinator merges by score (keeping a replicated
  row's best per-shard score), re-reads survivors at read consistency,
  applies the residual filter, and generates highlight snippets from its
  own index. An unreachable member is skipped — its rows still surface
  through reachable replicas. Scoring uses **per-shard BM25 statistics**
  (Elasticsearch's default across shards); a two-phase global-stats mode is
  a later phase. Post-resharding `Reclaim` leaves stale postings for
  moved-away keys — harmless (the authoritative re-read resolves them) and
  reclaimed by `REBUILD SEARCH INDEX`.

## Observability

- `SHOW INDEXES` lists search indexes with their analyzer and columns.
- `SHOW STATUS` rows: `search_indexes`, `search_docs`, `search_rebuild_ms`,
  and per-index `search.<name>.{docs,disk_bytes,uncommitted}`.
- `/metrics` gauges: `skaidb_search_indexes`, `skaidb_search_docs_total`,
  `skaidb_search_disk_bytes`, `skaidb_search_rebuild_seconds`.

## Limits (phase 4)

- Search predicates compose with `AND`/`OR`/`NOT` among themselves; mixing
  them with ordinary conditions under `OR`/`NOT` is rejected (top-level
  `AND` with ordinary conditions works — they filter the hits).
- `ORDER BY score() DESC` is the only ordering usable with search
  predicates and requires `LIMIT`.
- No `JOIN`, `UNION`, aggregates/`GROUP BY`, `DISTINCT`, or `NEAREST` in
  the same query.
- Per-shard BM25 statistics (like Elasticsearch's default); a global-stats
  mode is a later phase.
- Per-hit score explain, the ES side-by-side parity suite, ingest/NRT
  performance work, aggregations, and suggesters land in phases 3–7 (see
  [FTS_TODO.md](FTS_TODO.md)).
