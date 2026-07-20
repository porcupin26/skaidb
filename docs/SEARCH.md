# Full-text search

Native full-text search over skaidb tables: BM25-ranked retrieval with
`MATCH()` / `SEARCH()` SQL predicates, backed by an embedded
[Tantivy](https://github.com/quickwit-oss/tantivy) index per search index.
This documents the **shipped** state; pending work lives in
[TODO.md](TODO.md), the SQL grammar in
[QUERY_SYNTAX.md](QUERY_SYNTAX.md#full-text-search-match--search), and the
phase history in git.

Status: **phases 0–8 complete** — phases 1–5 (single-node core: DDL,
index maintenance on put/delete, `score()`, top-k pushdown, crash
recovery, rebuild; analysis & mappings: analyzer registry, per-column
configuration, typed fast fields, `.keyword` twins, `copy_to`; query DSL:
the full predicate family `MATCH`/`MATCH_PHRASE`/`MATCH_PREFIX`/`FUZZY`/
`WILDCARD`/`REGEXP`/`SEARCH`/`MATCH_CROSS`, AND/OR/NOT composition plus
`BOOSTED()` optional scoring, dis-max multi-field scoring, `HIGHLIGHT()`
snippets, per-hit BM25 explain over the ES subset, multi-word synonyms; cluster: scatter-gather top-k,
per-replica indexes, topology continuity; performance: bulk ingest path,
writer heap under `memory_target` — benchmarked vs Elasticsearch, see
[BENCHMARKS.md](BENCHMARKS.md)) plus **aggregations**: GROUP BY and
aggregate functions over search queries, with an exact fast-field facet
pushdown.

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
  your own writes immediately; the server also runs a background refresher
  tick (200 ms), so even a table receiving no further traffic becomes
  searchable on the shared/read-only path within `refresh_ms` + one tick.
- Measured against Elasticsearch on identical hardware: ~1.5× ES's bulk
  ingest and single-digit-fraction query latencies (see
  [BENCHMARKS.md](BENCHMARKS.md#full-text-search-vs-elasticsearch-v038-2026-07-08)).

## Analyzers

Set the index default with `analyzer = '...'`, or per column with
`<column>.analyzer = '...'`:

- `standard` — Unicode word split (UAX §29, the same segmentation ES's
  `standard` uses: `dog's` stays one token) + lowercase (the default).
  Measured at 98.5% strict top-10 result-set overlap with ES on a 280 k
  article corpus (see [BENCHMARKS.md](BENCHMARKS.md)). Indexes built
  before v0.39 (simple tokenizer) rebuild from the table automatically on
  first open.
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

**Synonyms** (`synonyms = 'quick,fast,speedy; new york,nyc,big apple'`)
expand at query time in `MATCH` — each group entry is analyzed with the
field's own pipeline so stemming lines up. **Multi-word entries** work in
both directions: an entry that occurs in the query as a consecutive token
sequence expands to its peers, and multi-word peers expand as **phrase**
alternatives (a query for `nyc` matches "new york" only where the words
are adjacent). `MATCH_PHRASE` and the query-string language do not
expand. Because expansion is query-time, synonyms **hot-reload**:

```sql
ALTER SEARCH INDEX articles_fts SET (synonyms = 'quick,fast; car,auto');
```

`ALTER SEARCH INDEX … SET` changes query-time-safe options in place
(`synonyms`, `refresh_ms`, `<col>.search_analyzer`, `<col>.boost`) with no
reindex; index-time options (analyzers, types, twins, `copy_to`) error —
those change the stored postings and need `DROP` + `CREATE`.

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
- `MATCH_CROSS(col, col, …, 'text')` — term-centric multi-field match
  (ES `multi_match` `cross_fields`): the fields behave like one big field —
  each term scores by its best field and the terms OR together, so a query
  whose terms are spread across columns (`'bob smith'` against
  `first_name`/`last_name`) still matches and ranks sensibly. Per-field
  `MATCH` composed with OR is field-centric instead (each field scores the
  whole query).
- `MATCH_BEST(col, col, …, 'text')` — field-centric dis-max over an
  explicit column subset (ES `multi_match` `best_fields`): a row matches
  if any listed column matches and scores as its best single field. The
  same match set as OR-ing per-field `MATCH`es, spelled in one predicate.

Term-level pattern predicates (**not analyzed** — they run against the
indexed terms, so with a lowercasing analyzer write patterns lowercase):

- `MATCH_PREFIX(col, 'qu')` — term prefix (ES `prefix`).
- `WILDCARD(col, 'qu*ck')` — `*` any run, `?` any one char (ES `wildcard`).
- `REGEXP(col, 'qu.[ck]+')` — regular expression (ES `regexp`).

**Composition**: search predicates combine freely with `AND`/`OR`/`NOT`
among themselves (ES bool `must`/`should`/`must_not`); ordinary SQL
conditions join at the top level with `AND` and filter the hits afterward.
`BOOSTED(required, optional…)` adds ES's optional-scoring shape: the
`required` predicate decides which rows match, and each `optional`
predicate only raises the score of rows that already match (tantivy
Must + Should — ES bool `must` + `should` under the default
`minimum_should_match: 0`). Every argument must itself be a search
predicate.
Mixing a search predicate with an ordinary condition under `OR`/`NOT` is
rejected — the index cannot serve it. A `NOT` search returns only rows the
index knows: a row with none of the indexed columns present is never
returned.

**Multi-field scoring** is dis-max (a row scores as its best field, ES
`best_fields`), with per-column boosts applied.

**Similarity & suggestions**: `MORE_LIKE_THIS(col, 'like text')` finds
textually similar rows (the like-text's most distinctive terms by
in-index IDF, OR-ed — permissive defaults so short like-texts work).
`SUGGEST '<text>' ON <index>` returns per-token "did you mean" terms from
the index dictionary (Levenshtein ≤ 2, doc-frequency ranked); completion
/search-as-you-type is the `edge_ngram` + `MATCH_PREFIX` pattern.

**Highlighting**: `HIGHLIGHT(col [, max_chars [, pre_tag, post_tag [, no_match_size]]])`
in the projection returns the best-scoring snippet of the column's text
(default fragment size 150 chars) with matching terms wrapped in tags
(`<b>…</b>` by default), other text HTML-escaped. Stemming is respected —
a query for `jumping` highlights `jumps`. Only valid together with a
search predicate; highlight multiple columns by calling `HIGHLIGHT()`
once per column. The snippet re-reads the row's live text (not stored
offsets), so it always reflects the current document.

- **Custom tags** (ES `pre_tags`/`post_tags`): pass a string pair after the
  fragment size, e.g. `HIGHLIGHT(body, 40, '<em>', '</em>')` →
  `slow roasted <em>vegetables</em>`.
- **`no_match_size`** (ES): a trailing integer returns that many leading
  characters (HTML-escaped) when the column had no match, instead of an
  empty string — `HIGHLIGHT(body, 40, '<b>', '</b>', 80)`.

Scope vs Elasticsearch: skaidb returns **one** best passage per column
(the ES `unified`-style default) and now covers custom tags and
`no_match_size`. Still not supported: multiple fragments
(`number_of_fragments`), a separate `highlight_query`, sentence/word
boundary scanners, and the `plain`/`fvh` highlighter types (`fvh` would
need stored term-vector offsets). On the RF<members sorted-scan scatter,
remote shards use default tags (the wire carries only the fragment size);
full-replica clusters and the primary search path use the full options.

## Aggregations

Search queries combine with GROUP BY and aggregates like any other SQL,
and `GROUP BY g TOP k BY score()` returns each group's k best-scoring
**rows** instead of aggregates — the SQL spelling of ES `top_hits`
(per-group top documents), with `HIGHLIGHT()` available in the
projection:

```sql
SELECT region, title, score() FROM products
WHERE MATCH(title, 'widget') GROUP BY region TOP 3 BY score();
```


```sql
SELECT region, COUNT(*), SUM(units), AVG(price) FROM sales
WHERE MATCH(product, 'widget') GROUP BY region;

SELECT COUNT(*), MAX(price) FROM sales
WHERE SEARCH('+widget -clearance');   -- one global row
```

Two serving paths produce identical results:

- **Fast-field pushdown** (no row materialization): global aggregates
  (`COUNT(*)`/`COUNT(col)`/`COUNT(DISTINCT col)`/`SUM`/`AVG`/`MIN`/`MAX`
  over declared columns, no GROUP BY), and grouped **counts** — a single
  declared `keyword` column or a `time_bucket(step, col)` over a declared
  `date` column (fixed-interval date histogram) with `COUNT(*)` — compute
  inside the index over fast fields (Tantivy aggregations).
  `COUNT(DISTINCT)` is **exact** — a terms-bucket count, never an HLL
  (the opt-in `APPROX_COUNT_DISTINCT()` is the HLL: it pushes down as a
  cardinality sketch and never bails on wide term sets) —
  approximation. Grouped **per-bucket metrics** deliberately do not push
  down: tantivy 0.26.1 has a sub-aggregation data-loss bug (small buckets
  lose metric input on periodic flushes while their doc counts stay
  exact — found by our parity benchmark, guarded until fixed upstream).
- **Row fallback**: everything else — grouped metrics, text-column
  grouping, residual predicates, HAVING, ORDER BY — gathers the matching
  rows (deduped by key at the coordinator, so correct at any replication
  factor) and runs the ordinary grouped executor. The gather is **bounded
  by the scan budget** (past it, the error names the fix: declare the
  group column a keyword fast field), and a `GROUP BY` on a column **not
  on the index at all** fails fast instead of gathering — it can never be
  answered index-side, and on a large match set the silent gather tied a
  production coordinator up for the full statement timeout. Group without
  a search predicate to aggregate off-index columns row-side.

SQL semantics hold on both paths: rows missing the group column form the
NULL group, `SUM` over no values is NULL (ES-style aggregations would say
0), and metric types follow the column declarations (`SUM` of a `long` is
an integer). The pushdown is **exact or declined** — on a truncated bucket
list or any count mismatch (e.g. a date histogram that would lose rows
missing the date column) it silently falls back rather than approximate.
Cluster mode pushes down when one index holds every row (single node, or
RF ≥ member count). **Sharded corpora** (RF < members) scatter partials:
every document carries its placement hash in a `_ring` fast field, each
member aggregates only the hash arcs it primarily owns (the arcs tile the
key-space, so every key counts exactly once regardless of replication),
and the coordinator merges. Exact-or-decline throughout: the scatter runs
only for losslessly mergeable metrics (`COUNT(*)`/`COUNT(col)`/`SUM`/
`MIN`/`MAX` globally; doc-count-only groupings while the tantivy#2992
guard stands), requires a stable membership epoch across the whole gather
and **every member answering** — a silent peer, an epoch change, or a
membership change in flight (dual-ring placement) falls back to the
deduped row gather. AVG scatters as SUM+COUNT pairs and the coordinator
divides after the merge; the distinct counts keep the fallback (their
partials don't merge losslessly). **Sorted top-k** (`ORDER BY <fast
column> LIMIT k`) scatters the same way — each member resolves its
primary-owned top k (highlights included) and the coordinator k-way
merges; a residual SQL filter declines the scatter (filters don't
travel). **Per-hit explain** routes to a replica of the key (ring
order), so both `"explain": true` and the SQL spelling —
`EXPLAIN SCORE SELECT ... WHERE MATCH(...) FOR <pk>` — work at any RF. Upgrade note: `_ring` was a schema
change, so each search index rebuilds from its table once on first open
after upgrading. One typing nuance:
`time_bucket` pushdown keys are timestamps (a `date` column's semantics),
while the fallback preserves each row's stored type — store timestamp
values (not bare integers) in date columns for consistent typing.

## Hybrid search (`RANK BY RRF`)

Fuse a full-text leg and a [vector](VECTOR.md) leg in one query by **Reciprocal
Rank Fusion** — the SQL analogue of Elasticsearch's `rrf` retriever. The
`NEAREST` clause is the vector leg, the `WHERE` search predicate is the text
leg, and `RANK BY RRF` fuses them:

```sql
SELECT id, rrf_score() FROM docs
NEAREST (embedding, [1.0, 0.0, 0.0], 100)   -- vector leg (100 candidates)
WHERE MATCH(body, 'quick brown fox')         -- text leg
RANK BY RRF                                  -- default constant 60
LIMIT 10;
```

Each leg fetches the `NEAREST` `k` candidates; a hit at 1-based rank `r` in a
leg contributes `1 / (c + r)` to its fused score (`rrf_score()`), so a doc that
ranks well in **both** legs beats one strong in only a single leg. Fusion is
rank-based, so BM25 scores and vector distances need no normalization.
`RANK BY RRF (c)` overrides the constant. The residual (non-search) part of
`WHERE` filters **both** legs. Cluster-wide: each leg scatter-gathers to a
coordinator-merged ranked list, then the coordinator fuses. Requires both a
`NEAREST` clause and a search predicate; no `JOIN`/`UNION`/`DISTINCT`/`GROUP
BY`/`ORDER BY` (ordering is `rrf_score()` desc).

## ES-compatible REST subset

The REST endpoint speaks enough Elasticsearch for existing ES client
libraries and log shippers (not Kibana). An ES "index" is a skaidb
**table**; its `SEARCH INDEX` is the mapping; `_id` maps to the table's
single-column primary key (stored as a string, auto-generated when a bulk
action omits it). Pre-create the table + search index for full control —
or let `_bulk` **auto-create** an unknown index ES-style: primary key
`id` plus a dynamic mapping from the first document (strings → `text`,
integers → `long`, floats → `double`, bools → `bool`; null/array/object
fields are stored but not indexed).

```
POST /{index}/_bulk      index / create / delete NDJSON actions
                         (auto-creates an unknown index, see above)
POST /{index}/_search    query DSL: match, match_phrase, prefix, wildcard,
                         regexp, fuzzy, term, terms, range, exists, bool
                         (must/filter/must_not/should — should beside
                         must/filter boosts scores via BOOSTED(), or is
                         required with minimum_should_match: 1),
                         query_string, more_like_this, multi_match
                         (best_fields / most_fields / cross_fields);
                         "explain": true per-hit BM25 breakdowns; from/size,
                         multi-key sort (incl. _score), _source with
                         include/exclude lists (trailing-* globs),
                         highlight, exact totals; aggs: terms,
                         date_histogram (+ sum/avg/min/max/value_count/
                         cardinality/top_hits sub-aggs — top_hits runs
                         one relevance-ordered query per retained
                         bucket);
                         vector retrieval: a top-level knn block
                         {field, query_vector | query_vector_builder,
                         k, filter} → NEAREST (a query_vector_builder
                         text searches a managed EMBED index, auto-
                         embedded); a retriever {rrf {retrievers:
                         [standard, knn]}} block → NEAREST + WHERE-search
                         RANK BY RRF (rank_constant → the RRF constant)
POST /{index}/_count     exact match count
GET  /{index}/_doc/{id}  fetch one document by _id
GET  /{index}/_mapping   the search-index declaration as ES properties
```

Everything translates to the same SQL statements documented above and
runs through the ordinary session path — HTTP Basic auth, RBAC, cluster
routing, and all pushdowns apply unchanged. Limits:
`minimum_should_match` above 1 is rejected, and `bool.should` beside a
must/filter with **no search clause** cannot be scored (set
`minimum_should_match: 1` to make the shoulds required); clients that
hard-check the `X-elastic-product` header need that check disabled.
`cardinality` is skaidb's **exact** `COUNT(DISTINCT)`, not an HLL
approximation. For `knn`/`retriever` queries `num_candidates` is ignored
(HNSW breadth is an index property — tune it with `ALTER VECTOR INDEX …
SET (ef = n)`), the hit `_score` is the fused `rrf_score()` for a
retriever or a `1/(1+distance)` similarity for a plain knn, and the
`total` is the number of ranked hits (≤ `k`), not a full match count.

## Architecture

- **Why Tantivy** (decision record): Lucene is a JVM library — embedding a
  JVM contradicts the single static Rust binary, and out-of-process Lucene
  is just running Elasticsearch. Tantivy is Lucene's architecture re-done
  in Rust (MIT, a plain dependency, `forbid(unsafe)` on our crates
  unaffected): immutable segments, skip-list postings, positional indexes,
  FST term dictionaries, columnar fast fields, BM25 — and public
  benchmarks have it matching or beating Lucene per core. A native engine
  stayed on the table (skaidb built its own TSDB and HNSW), but FTS parity
  is 10–20× the surface of either; hence the thin crate boundary below,
  which keeps a native replacement possible without touching engine/SQL.
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
- **Storage layout**: Tantivy segments live under `<data_dir>/fts/<index>/`,
  mmap'd for search (evictable pages — reads cost no memory budget). The
  writer heap is bounded per index: 64 MB by default, or `memory_target`/8
  clamped to [16 MB, 64 MB] when a budget is set (peak RSS during a bulk
  build ≈ 1.5× the heap).
- **Bulk ingest**: a multi-row statement (and a replicated batch, including
  the async replication frames) feeds every search index in one pass with a
  single NRT refresh check at the end — an index commit never fires
  mid-batch. Dev-box reference (100 k-row synthetic corpus,
  `skaidb-engine/examples/fts_bench`): ≈ 126 k rows/s ingest with the index
  live (batched) vs ≈ 81 k rows/s per-row; ranked top-10 ≈ 1.2 ms p50.
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
- `ORDER BY score()` orders descending only (and requires `LIMIT`).
  Column orderings work: declared fast-field columns with `LIMIT` retrieve
  index-ordered top-k (declining to an exact gather-and-sort when matching
  rows lack the sort column — SQL NULL placement differs from the
  index's); anything else gathers and sorts through the ordinary
  executor.
- No `JOIN`, `UNION`, `DISTINCT` in the same query. A search predicate
  combines with `NEAREST` (vector) only through **hybrid `RANK BY RRF`** (see
  below), not as a boolean sibling.
- Per-shard BM25 statistics (like Elasticsearch's default across shards);
  a global-stats mode remains future work. "Parity" with ES is defined as
  result-set parity, not identical score floats — BM25 constants and
  length normalization differ subtly between engines.
