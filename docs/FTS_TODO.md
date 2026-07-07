# Full-text search — implementation plan

Goal: **Elasticsearch-class full-text search native to skaidb** — feature
parity on the search surface people actually use, with single-node and
cluster query performance that matches or beats Elasticsearch on the same
hardware. Search stays SQL-first (like time-series did), with an optional
ES-compatible REST subset later.

This is the working plan + pending-items list for the effort. Shipped state
will move to a `docs/SEARCH.md` feature doc as phases land (the TIMESERIES.md
pattern); this file tracks what's left.

---

## 0. Engine decision: Lucene vs Tantivy vs native

**Lucene itself is out.** Lucene is a JVM library; skaidb is a single static
Rust binary (`unsafe_code = "forbid"`, no runtime deps). Embedding a JVM
(JNI, GC pauses, heap sizing, deployment weight) contradicts everything the
project optimizes for, and out-of-process Lucene (an ES/Solr sidecar) is just
running Elasticsearch.

**Recommended core: [Tantivy](https://github.com/quickwit-oss/tantivy)** —
Lucene's architecture re-done in Rust (MIT; a workspace *dependency*, so the
`forbid(unsafe)` lint on our own crates is unaffected):

- Same fundamental design Lucene proved: immutable segments, skip-list
  postings, positional indexes, FST term dictionaries, columnar "fast
  fields", BM25, segment merging.
- Public benchmarks (search-benchmark-game, Quickwit's comparisons) have it
  matching or beating Lucene per core on most query classes — the
  "match/outperform ES" target is credible on day one instead of after two
  years of native postings-format work.
- Battle-tested at scale as Quickwit's engine; active upstream.

**Native engine stays on the table** (we built our own TSDB and HNSW), but
FTS parity is 10–20× the surface of either: analyzers × languages, positional
scoring, fuzzy automata, facets, highlighters. Building that from scratch
*and* winning benchmarks is a multi-year detour. Decision: wrap Tantivy
behind a thin `skaidb-fts` crate boundary (our types in, our types out) so a
native replacement — or a fork with skaidb-specific formats — remains
possible without touching the engine/SQL layers.

Phase 0 below validates the risky integration points before we commit.

---

## 1. Architecture (how it fits skaidb)

- **New crate `skaidb-fts`**: wraps Tantivy; owns schema mapping, analyzer
  registry, document conversion, query building, and the searcher/writer
  lifecycle. No Tantivy types leak past it.
- **Index = derived data over an LSM table**, exactly like the vector
  indexes: `CREATE SEARCH INDEX idx ON t (title, body) WITH (...)` registers
  in the catalog; `put`/`delete` maintain the index alongside secondary and
  vector indexes (`maintain_vectors_put` pattern). The LSM table remains the
  source of truth — a lost/corrupt index **rebuilds from the table**, which
  is also the anti-entropy and resharding story.
- **Durability**: the row WAL is the translog. The index itself commits
  lazily (near-real-time refresh + periodic commit); on restart, replay rows
  newer than the index's last-committed opstamp from the table (keyed by
  HLC), or rebuild if the index is behind by more than a threshold.
- **Distribution**: doc-partitioned by construction — rows already place on
  the ring, every replica indexes its shard locally from replicated writes
  (the vector-index pattern). Queries scatter to all members, each returns
  its local top-k `(key, score)` (+ requested fields), the coordinator
  merges by score and re-reads survivors at read consistency. Scoring is
  per-shard IDF by default (Elasticsearch's default too); an optional
  two-phase global-stats mode (ES `dfs_query_then_fetch`) is a later phase.
- **SQL surface first** (details §3): `MATCH()` predicates, `score()`,
  `HIGHLIGHT()`, ranked `ORDER BY score() DESC LIMIT k` pushdown. The
  ES-compatible REST subset is a separate, optional phase — same playbook as
  the Prometheus API for time-series.
- **RBAC**: search rides the SELECT path — `Select` on the table is enforced
  already; index DDL rides Create/Drop. Nothing new.
- **Memory**: Tantivy searches over mmap'd segments; writer heap is bounded
  and configurable. Ties into `memory_target` alongside the TS head budget.

---

## 2. Feature parity matrix (what "Elasticsearch-class" means here)

Legend: ✱ = core parity (phases 1–6), ▷ = later phase, ✗ = explicit non-goal.

**Analysis** ✱ standard/whitespace/keyword/ngram/edge-ngram tokenizers;
lowercase, stopword, ASCII-folding filters; Snowball stemmers (EN + the
usual European set); per-field analyzers; index-time vs query-time analyzer
split; multi-fields (`text` + `keyword` twin). ▷ synonym filters with
hot-reload; ICU normalization; language detection. ✗ plugins-as-code.

**Mappings** ✱ text, keyword, long/double, date, bool; explicit index
config per column (skaidb is schema-lite — the SEARCH INDEX declaration is
the mapping); `copy_to`-style composite field. ▷ nested/object fields
(skaidb docs are nested already — index dotted paths ✱, nested *queries*
with per-object scoping ▷). ✗ dynamic mapping guessing (declared columns
only), join fields, percolator.

**Queries** ✱ match, match_phrase (+slop), term/terms, prefix, wildcard,
regexp, fuzzy (Levenshtein ≤2 via FST automata), range, exists, bool
composition (SQL AND/OR/NOT ↔ must/should/must_not/filter), multi_match
(best_fields, cross_fields), query_string mini-language, boosts (field^n,
term^n), constant_score. ▷ span/interval queries, more_like_this,
function_score beyond boosts, rescoring. ✗ scripted scoring (no script
engine — deliberate).

**Scoring & retrieval** ✱ BM25 (tunable k1/b), top-k with early
termination, sort by score or fast field, from/size + search_after
pagination, source filtering, explain (per-hit score breakdown),
highlighting (unified-style, fragment control). ▷ point-in-time readers.

**Aggregations** (facets over fast fields — skaidb's SQL GROUP BY covers
the relational cases already) ✱ terms, range, histogram, date_histogram,
min/max/sum/avg/count, cardinality (HyperLogLog++), top_hits. ▷ percentiles
(t-digest), significant_terms, composite/pipeline aggs.

**Suggest** ▷ term suggester (fuzzy), completion suggester (FST prefix).

**Indexing & ops** ✱ NRT refresh (configurable interval, default 1 s),
updates/deletes (delete + reinsert on the same PK), bulk ingest path,
segment merge policy tuning, per-index stats in `SHOW STATUS` + `/metrics`
gauges, crash-safe restart, rebuild command. ▷ index warming, forcemerge
admin verb, snapshot/restore beyond table-level export. ✗ ILM (retention
lives on tables), cross-cluster search, security realms (RBAC exists).

---

## 3. SQL surface (draft — final grammar decided in phase 1, then locked
into QUERY_SYNTAX.md)

```sql
CREATE SEARCH INDEX articles_fts ON articles (title, body)
  WITH (analyzer = 'english', title_boost = 2.0, refresh_ms = 1000);

-- Ranked retrieval: MATCH is a predicate, score() the BM25 score of the
-- row against the query. ORDER BY score() DESC LIMIT k pushes top-k into
-- the index (no full scan, cluster scatter returns k per member).
SELECT id, title, score() FROM articles
WHERE MATCH(body, 'quick brown fox') AND published = true
ORDER BY score() DESC LIMIT 10;

-- Query-string mini-language (field:term, phrases, +must -not, fuzz~1,
-- wild*, [ranges]) over the index's default fields:
SELECT id, score() FROM articles
WHERE SEARCH('title:"rust database" +body:performance -draft')
ORDER BY score() DESC LIMIT 20;

-- Phrase / fuzzy / prefix forms as explicit predicates:
WHERE MATCH_PHRASE(body, 'exactly this phrase', 2)   -- slop 2
WHERE FUZZY(title, 'databsae', 1)
-- Highlighting projects snippets:
SELECT id, HIGHLIGHT(body, 30) AS snippet FROM ... WHERE MATCH(...)
```

Composition rule: `MATCH`/`SEARCH` predicates are index-served; residual SQL
predicates filter afterward (same authoritative-residual discipline as the
TS pushdown). `score()` is only meaningful with a search predicate — else
error.

---

## 4. Performance plan (the "match or beat ES" part)

- **Benchmarks are the exit criteria, not an afterthought.** Corpora:
  Wikipedia EN abstracts (search-benchmark-game queries: term, intersection,
  union, phrase — the per-core honesty test) and an ES Rally track
  (http_logs or so) for ingest + aggregations at scale.
- **Hardware**: the p225 bench fleet (see memory: 15-LXC comparison fleet,
  C1–C4 config switching) — ES gets identical containers, page cache warmed
  both sides, and the shared-network confounds checked before declaring
  wins (see benchmark-methodology memory).
- **Targets** (same hardware, measured p50/p99):
  - Single node: query latency ≤ Lucene/ES on term/bool/phrase/agg classes;
    ingest ≥ ES bulk (ES pays JSON + JVM tax; we should win clearly).
  - Cluster (3-node RF=3): scatter top-k adds ≤ 10 ms p99 over single-node
    on the wiki corpus; ingest scales with the existing replicated-write
    path (batched, one internode round per replica per statement).
  - NRT: default refresh visibility ≤ 1 s under sustained ingest.
- **Known perf work items**: top-k early termination (WAND/block-max via
  Tantivy), fast-field agg pushdown per shard with partial merge at the
  coordinator (the TS partial-aggregate pattern applies verbatim), query
  result + filter caching (later, measured first), segment merge tuning on
  LXC-class disks.

---

## 5. Phases (each ends tested, clippy-clean, docs updated, released,
fleet-verified — the TS cadence)

- [ ] **Phase 0 — spike & decision checkpoint** (~branchless, throwaway ok):
  embed Tantivy in a scratch binary; validate (a) index-from-LSM-table
  rebuild speed, (b) writer memory bounds + commit/refresh semantics against
  our WAL-replay recovery idea, (c) mmap behavior inside LXC memory limits,
  (d) binary-size and compile-time cost. Exit: written go/no-go on Tantivy
  (fallback: native mini-engine scoped to match/phrase/BM25 only — accept
  the smaller surface, then grow).
- [ ] **Phase 1 — single-node core**: `skaidb-fts` crate; catalog +
  `CREATE/DROP SEARCH INDEX` DDL (broadcast like other DDL); maintenance on
  put/delete; `MATCH()`, `SEARCH()`, `score()`; top-k pushdown for
  `ORDER BY score() DESC LIMIT k`; restart recovery (opstamp replay) +
  `REBUILD SEARCH INDEX`. Exit: wiki-subset indexed; top-k parity with raw
  Tantivy; kill -9 recovery test; QUERY_SYNTAX.md grammar locked.
- [ ] **Phase 2 — analysis & mappings parity**: analyzer registry (WITH
  options), per-field config, multi-field keyword twin, numeric/date/bool
  fast fields, dotted-path indexing. Exit: analyzer conformance fixtures
  (same token streams ES produces for the standard cases); mixed-type
  corpus round-trips.
- [ ] **Phase 3 — query DSL parity**: phrase+slop, fuzzy, prefix, wildcard,
  regexp, ranges, boosts, multi_match modes, full query_string syntax, bool
  composition from SQL, explain, highlighting. Exit: curated 100-query
  suite side-by-side vs ES — same result *sets* (top-k overlap ≥ 95%,
  scoring-order differences documented).
- [ ] **Phase 4 — cluster**: per-replica local indexes over replicated
  writes; scatter-gather top-k merge at read consistency (vector-search
  pattern); rebuild on join/decommission/rebalance; anti-entropy = detect
  index-behind-table (opstamp/checksum) and rebuild; hinted writes already
  replay through the table path so the index follows for free. Exit: 3-node
  tests incl. kill/rejoin convergence; fleet smoke.
- [ ] **Phase 5 — NRT + ingest performance**: refresh-interval config, bulk
  ingest path (index once per statement batch, not per row), writer memory
  budget under `memory_target`, merge-policy tuning; first full ingest+query
  bench vs ES on the bench fleet. Exit: targets in §4 met or gaps
  root-caused with a plan.
- [ ] **Phase 6 — aggregations/facets**: terms/range/histogram/
  date_histogram/cardinality/top_hits over fast fields, exposed through SQL
  (GROUP BY over indexed columns pushes to per-shard facet partials, merged
  at the coordinator — reuse the TS partial-merge shape). Exit: agg parity +
  perf vs ES on the logs track.
- [ ] **Phase 7 — search UX extras**: search_after, fast-field sort,
  term + completion suggesters, synonyms with hot-reload, more_like_this.
- [ ] **Phase 8 — ES-compatible REST subset (decision checkpoint)**:
  `_search` (query DSL JSON: the §2 ✱ queries + aggs), `_bulk`, `_mapping`
  read-only — enough for existing ES client libraries and log shippers, not
  Kibana. Weigh maintenance cost vs adoption pull before building (the
  Prometheus-API precedent says it's worth it; validate demand first).
- [ ] **Phase 9 — hardening & the honesty pass**: 24 h soak under mixed
  ingest+query; full benchmark publication in docs/BENCHMARKS.md; explain
  output audit; failure-injection (disk-full mid-merge, torn index dir →
  rebuild); docs/SEARCH.md finalized.

---

## 6. Risks / open questions

- **Tantivy NRT model**: reader reloads are cheap but commits fsync — our
  WAL-as-translog recovery must make index commits *optional* for
  durability. Phase 0 proves it.
- **Distributed IDF**: per-shard scoring differs slightly from a
  single-node ES with one shard; ES has the same property across shards.
  Document it; optional global-stats phase if parity tests care.
- **Updates-heavy tables**: delete+reinsert churns segments; merge pressure
  on LXC disks needs the phase-5 tuning pass.
- **Index size**: Tantivy is compact, but positions+fast fields can exceed
  the LSM table size on text-heavy corpora; per-index stats + disk gauges
  land in phase 1 so growth is visible from day one.
- **Scoring "parity"** is defined as result-set parity, not identical
  floats — BM25 constants and length normalization differ subtly between
  engines; the phase-3 suite pins expectations.
- **Licensing**: Tantivy is MIT — compatible with embedding in SSPL skaidb.
