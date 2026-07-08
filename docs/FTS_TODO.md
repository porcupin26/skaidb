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

- [x] **Phase 0 — spike & decision checkpoint** — **GO on Tantivy** (0.26).
  Spike lives at `crates/skaidb-fts/examples/phase0_spike.rs`; measured on
  the dev box (synthetic corpus, 47-word vocab — deliberately worst-case
  posting lengths since every term matches ~20% of docs):
  - (a) Rebuild speed: 434–576 k docs/s (≈280–370 MB/s of text) with a
    128 MB writer heap; 1 M docs (640 MB text) indexed + committed in 2.3 s.
    Index on disk ≈ 29–30 % of raw text (positions + stored title/pk).
    Full rebuild-from-table is clearly viable as the recovery/anti-entropy
    story.
  - (b) Commit/refresh semantics match the WAL-as-translog plan: uncommitted
    docs are invisible to readers and lost on crash (verified via
    rollback + reopen); tantivy commits return a monotonic opstamp and
    accept a payload — we'll store the max-indexed HLC as the commit
    payload, and on restart replay table rows with `hlc > payload`.
    Refresh tick under ingest (2 k-doc batch): commit ≈ 13 ms, reader
    reload ≈ 1 ms — a 1 s NRT refresh interval costs ~1.5 % of a core.
    Delete+reinsert updates (1 k) commit in ~20 ms.
  - (c) Memory: peak RSS ≈ 1.5× the configured writer heap during a bulk
    build (arena + stored-field buffers), independent of corpus size
    (196 MB peak at 1 M docs / 128 MB heap). Search-side RSS is mmap'd
    segment pages — evictable under LXC memory pressure, so `memory_target`
    only needs to budget the writer heap. LXC-limit behavior re-checked on
    the fleet in phase 5.
  - (d) Build cost: tantivy dep tree adds ~40 s wall (~130 s CPU) one-time
    release compile; incremental `skaidb-fts` rebuild ≈ 11 s; spike binary
    6.7 MB release — acceptable for the static-binary story. ~110 crates in
    the fts dep tree (compile-time only; `forbid(unsafe)` on our crates is
    unaffected).
  - (d) Query sanity at 1 M docs, worst-case postings: term p50 3 ms,
    bool-AND 16 ms, bool-OR 8 ms, phrase 47 ms (top-10). Realistic-corpus
    numbers (wiki abstracts) come with the phase-1 exit benchmark.
- [x] **Phase 1 — single-node core**: `skaidb-fts` crate; catalog +
  `CREATE/DROP SEARCH INDEX` DDL (broadcast like other DDL); maintenance on
  put/delete; `MATCH()`, `SEARCH()`, `score()`; top-k pushdown for
  `ORDER BY score() DESC LIMIT k`; restart recovery (watermark replay) +
  `REBUILD SEARCH INDEX`. Grammar locked into QUERY_SYNTAX.md; shipped
  state in docs/SEARCH.md. (Wiki-subset benchmark rolls into the phase-5
  ingest/query bench.)
- [x] **Phase 2 — analysis & mappings parity**: analyzer registry resolved
  from WITH-option spec strings (`standard`, `folding`, `whitespace`,
  `keyword`, `ngram(min,max)`, `edge_ngram(min,max)`, 18 Snowball
  languages); per-column config (`<path>.analyzer` / `search_analyzer` /
  `type` / `boost` / `keyword` / `copy_to`); `.keyword` exact-match twins;
  `copy_to` composites; `long`/`double`/`bool`/`date` fast fields
  addressable from the query-string language; dotted-path indexing.
  Catalog stores the raw declaration (phase-1 defs auto-migrate on load);
  index-time config changes trigger rebuild-on-open, `search_analyzer` is
  query-time-only. Analyzer conformance fixtures vs ES token streams
  (documented divergence: simple tokenizer splits `dog's`; ES keeps it
  whole); mixed-type corpus round-trips in engine tests.
- [ ] **Phase 3 — query DSL parity** (core shipped, exit pending):
  - [x] Predicates: `MATCH_PREFIX` (prefix), `WILDCARD`, `REGEXP` — term-
    level via FST regex automata, not analyzed (documented); phrase+slop and
    fuzzy shipped in phase 1, ranges/boosts via the query-string in phase 2.
  - [x] Bool composition from SQL: search predicates compose with
    AND/OR/NOT (`SearchQuery::All/Any/Not` ↔ must/should/must_not); mixing
    with ordinary predicates under OR/NOT is rejected; NOT is documented as
    index-only (rows with no indexed columns are absent from the index).
  - [x] Multi-field scoring is dis-max (ES `best_fields` default) with
    per-field boosts.
  - [x] Highlighting: `HIGHLIGHT(col [, max_chars])` projection —
    `SnippetGenerator` per (query, column) applied to the authoritative row
    text at hit-resolve time, `_highlight_<col>` injected like `_score`
    (cluster-ready: travels with the hit doc).
  - [x] **Exit met** (2026-07-08): 400-query suite (term/AND/OR/phrase ×
    100) side-by-side vs ES 8.14.3 on the 280 k-article corpus — **98.5%
    strict top-10 overlap, 99.8% tie-tolerant**, per-query hit counts
    equal. Getting there required replacing the simple tokenizer with a
    **UAX §29 Unicode word tokenizer** in the standard-based pipelines
    (the first run scored 89.2%; tokenization was nearly all of the gap).
    The tokenizer registers under a versioned name (`.u1`), so existing
    indexes schema-mismatch on open and rebuild from the table
    automatically. Residual ~1.5% is fieldnorm quantization on near-ties;
    documented in BENCHMARKS.md.
  - [ ] `multi_match` `cross_fields` mode (best_fields is the shipped
    default).
  - [ ] Per-hit score explain.
- [ ] **Phase 4 — cluster** (core shipped, fleet smoke pending):
  - [x] Per-replica local indexes over replicated writes: the replicated
    apply paths (`apply_put`/`apply_delete` + batched variants) maintain
    search indexes, so replication, rebalance, drain, hinted replay, and
    repair all keep the index in step with the table — verified, no extra
    machinery needed.
  - [x] Scatter-gather: `Request::Search` (query ships as serde_json —
    self-describing, grows without wire changes) → per-shard `(key, score)`
    top-k; coordinator merges best-score-per-key, re-reads survivors at
    read consistency, filters, snippets from its own index. Peers commit
    pending index writes before answering, so acked writes are searchable
    cluster-wide (stronger than NRT). Unreachable members are skipped.
  - [x] Join/decommission/rebalance: schema sync delivers the regenerated
    `CREATE SEARCH INDEX` (backfills from whatever rows already arrived);
    migrated rows index via the apply path. 3-node tests: rf=1 scatter
    completeness, rf=1 join migration, dead-member tolerance; engine-level
    kill -9 watermark-replay recovery. `Reclaim` leaves stale postings for
    moved-away keys (harmless — authoritative re-read resolves; `REBUILD`
    reclaims); revisit in phase 9 if it bothers anyone.
  - [x] Anti-entropy: open-time watermark replay covers restart divergence;
    repair copies flow through the apply path so the index follows.
  - [x] **Fleet smoke passed** (2026-07-08, test cluster on v0.39.0,
    upgraded live from 0.34.0 — catalog migration included): 60 k wiki
    docs at RF=3/QUORUM (3,053 docs/s through one coordinator), identical
    hit counts from every coordinator, NRT 149 ms cluster-wide;
    kill -9 one node → searches stay complete, a quorum write during the
    outage lands, and the rejoined node serves the converged result.
- [ ] **Phase 5 — NRT + ingest performance** (code core shipped, fleet
  bench pending):
  - [x] Bulk ingest path: one search-index pass and one NRT refresh check
    per statement batch instead of per row, on the single-node `put_batch`,
    the replicated `apply_batch` (engine-side now), and the async
    replication frames — an index commit can no longer fire mid-batch.
    Dev-box sanity (100 k-row synthetic corpus, `fts_bench`): batched
    ingest ≈ 126 k rows/s with the index live vs ≈ 81 k rows/s per-row;
    backfill ≈ 154 k rows/s; ranked top-10 ≈ 1.2 ms p50.
  - [x] Writer memory under `memory_target`: per-index Tantivy writer heap
    = budget/8 clamped [16 MB, 64 MB] (peak RSS ≈ 1.5× heap, phase-0
    spike), wired through `EngineOptions::search_writer_heap_bytes`;
    default stays 64 MB when no target is set. Search reads cost no
    budget — segments are mmap'd and evictable.
  - [x] Refresh-interval config shipped in phases 1–2 (`refresh_ms`); the
    write path checks it per statement, peers commit-if-dirty on scatter.
  - [x] `skaidb-engine/examples/fts_bench` — ingest + query latency bench
    over the real SQL path (the ES A/B uses this shape on the fleet).
  - [x] Background NRT refresher (server tick every 200 ms →
    `search_refresh_tick`): refresh checks previously ran only on the
    write path, so an idle table's last index writes never became visible
    to read-only searches. **Found by the exit bench** (the NRT probe hung
    forever); regression-tested.
  - [x] **Exit met** (2026-07-08): ingest+query A/B vs Elasticsearch
    8.14.3 on identical 2 vCPU / 2 GB containers, 280 k Simple-English-
    Wikipedia articles — skaidb 10.6 k docs/s ingest vs ES 7.0 k warm;
    term/AND/OR/phrase p50 0.5–0.7 ms vs ES 4.9–5.8 ms; RSS 650 MB vs
    1.49 GB; disk 336 MB vs 529 MB. Both §4 single-node targets hold
    (query ≤ ES every class, ingest ≥ ES bulk). Full table + caveats
    (protocol framing, 1 GB ES heap) in docs/BENCHMARKS.md; harness in
    `bench/clients/fts_{corpus,bench}.py`.
  - [x] Cluster scatter leg (2026-07-08, 3-node test cluster, RF=3,
    60 k docs): ranked top-10 p50 2–3 ms / p99 2.9–7.6 ms on
    term/AND/OR from both coordinators tried, vs ~1.2–1.9 ms p99
    single-node — scatter adds well under the §4 ≤ 10 ms p99 budget
    (phrase p99 hit 11 ms once, within noise of its single-node 13 ms
    worst case).
  - [ ] Merge-policy tuning on LXC-class disks: the win over ES leaves no
    urgency; revisit if an ingest-heavy workload surfaces merge stalls.
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
