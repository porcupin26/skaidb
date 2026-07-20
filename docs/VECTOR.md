# Vector search (HNSW)

skaidb can store embeddings and run **approximate nearest-neighbor (ANN)**
search over them with an in-memory **HNSW** index, including **filtered** search
("nearest neighbors *where* …"). This is the index family behind semantic
search / RAG / recommendations.

> Status: **distributed** (sharded scatter-gather), in-memory, persisted as a
> snapshot and reloaded on open (full rebuild only when no usable snapshot
> exists). Both index creation and the kNN query have SQL syntax (`NEAREST`,
> below). See limitations at the end.

## Storing vectors

Vectors are ordinary document fields holding an array of numbers — SQL now
supports array literals:

```sql
CREATE TABLE docs (PRIMARY KEY (id));
INSERT INTO docs (id, cat, embedding) VALUES (1, 'news', [0.12, -0.04, 0.91, ...]);
```

## Creating an index (SQL — works cluster-wide)

```sql
CREATE VECTOR INDEX docs_emb ON docs (embedding) DIM 768 USING cosine
DROP   VECTOR INDEX docs_emb
```

`DIM` (the vector dimension) is required; `USING` is `cosine` (default), `l2`, or
`dot`. This is **broadcast DDL**: every node builds and maintains an HNSW over
its own shard. The index is maintained automatically on `INSERT`/`UPDATE`/
`DELETE` (a replace soft-deletes the old vector and inserts the new one).

The DDL acks at schema-apply; each node backfills existing rows in paged
background work (like secondary indexes). While a node is backfilling,
`SHOW INDEXES` reports `local = building` there, and searches against the
index answer **"vector index is rebuilding — retry shortly"** rather than
silently serving a partial graph. On a single-node/embedded database the
backfill completes before the DDL returns.

## Managed embeddings (`EMBED`, semantic_text)

A **managed** vector index embeds a TEXT column for you — the ES `semantic_text`
workflow. Configure an embeddings endpoint in `[inference]`, then:

```sql
CREATE VECTOR INDEX docs_sem ON docs (body) EMBED DIM 768;
SELECT id FROM docs NEAREST (body, 'natural language query', 10);  -- query auto-embedded
```

`EMBED` makes `path` a text column: on write skaidb embeds it via the provider
(rather than reading a pre-computed vector array), and a **string** `NEAREST`
query is auto-embedded. `DIM` must match the model; the index errors at create
if `[inference]` is off or the dimension disagrees.

### How the embedding model is used

skaidb never runs an ML model in-process — the single static binary stays. An
"embedding model" is always an **external HTTP model server** that skaidb calls
as a client. This keeps the database free of Python/CUDA/model weights and lets
you point at whatever provider you run.

**The wire contract (OpenAI embeddings API).** For a batch of texts skaidb sends:

```
POST <inference.url>
Authorization: Bearer <inference.api_key>     # only when api_key is set
Content-Type: application/json

{ "model": "<inference.model>", "input": ["text one", "text two", ...] }
```

and expects the OpenAI-shaped response (order preserved, one entry per input):

```json
{ "data": [ {"embedding": [0.01, -0.02, ...]}, {"embedding": [...]} ] }
```

Any server that speaks this shape works — **OpenAI**, **Azure OpenAI**, a local
**text-embeddings-inference (TEI)** server, **Ollama** (`/v1/embeddings`),
**Cohere**/others behind an OpenAI-compatible proxy, or your own endpoint. The
returned vector length must equal the index `DIM` (skaidb validates and rejects a
mismatch).

**Configuration (`[inference]` block, see `config/skaidb.toml`):**

| key | meaning |
|---|---|
| `enabled` | master switch; `EMBED` DDL errors if this is off |
| `url` | full embeddings endpoint, e.g. `https://api.openai.com/v1/embeddings` or `http://tei-host:8080/embed`-style OpenAI route |
| `model` | model name sent in the request body (e.g. `text-embedding-3-small`) |
| `dim` | the model's output dimension — must equal every `EMBED` index's `DIM` |
| `api_key` | bearer token; sent as `Authorization: Bearer …` only when non-empty (leave empty for a local unauthenticated server) |
| `batch_size` | max texts per request (default 32); the background worker batches queued rows up to this |
| `timeout_secs` | per-request timeout (default 30) |
| `tls_verify` | `"ca"` (verify against `tls_ca`), `"system"` (public CAs — not built in yet), or `"insecure"` (skip — dev only). An HTTPS endpoint today needs `"ca"` + `tls_ca`, or `"insecure"` |
| `tls_ca` | CA certificate (PEM) when `tls_verify = "ca"` |
| `rerank_url` | cross-encoder rerank endpoint for the `RERANK` query clause (see below); empty = reranking off. `url` and `rerank_url` are independent — either may be set alone |
| `rerank_model` | default rerank model sent in the request body; a query's `RERANK WITH '<model>'` overrides it |

Example — OpenAI:

```toml
[inference]
enabled = true
url = "https://api.openai.com/v1/embeddings"
model = "text-embedding-3-small"
dim = 1536
api_key = "sk-..."
tls_verify = "system"   # or "ca" + a tls_ca bundle until system roots land
```

Example — a local TEI/Ollama server (no auth, plaintext):

```toml
[inference]
enabled = true
url = "http://127.0.0.1:8080/v1/embeddings"
model = "BAAI/bge-base-en-v1.5"
dim = 768
```

### The rerank endpoint (`RERANK`)

The same `[inference]` block also configures the **cross-encoder reranker**
behind the SQL `RERANK` clause and the ES `text_similarity_reranker`
retriever (see [SEARCH.md](SEARCH.md#reranking-rerank)). It is a separate
endpoint (`rerank_url`) sharing `api_key`, `timeout_secs`, and the TLS
settings. The wire contract (Cohere/Jina rerank API; the candidate texts are
sent under both `documents` and `texts` so a TEI `/rerank` route works too):

```
POST <inference.rerank_url>

{ "model": "<model>", "query": "<query text>",
  "documents": ["candidate one", ...], "texts": ["candidate one", ...] }
```

expecting either shape (order-independent, `index` refers to the request
order; `relevance_score`/`score`, higher = more relevant):

```json
{ "results": [ {"index": 0, "relevance_score": 0.98}, ... ] }   // Cohere/Jina
[ {"index": 0, "score": 0.98}, ... ]                            // TEI
```

Reranking is invoked **only** by queries that say `RERANK` — never at ingest
and never by ordinary searches — so the endpoint being down fails exactly
those queries.

**Model is pinned per index.** The `DIM` you declare fixes the vector geometry;
the `[inference].model` is the model actually called. Changing the model (or its
dimension) means the stored vectors no longer match — `DROP` and re-`CREATE` the
index (a fresh backfill re-embeds every row with the new model). Keep `dim`
consistent across the config and every `EMBED` index.

**When it runs (two moments):**
- **Ingest** — on `INSERT`/`UPDATE` of the text column, the row commits with the
  raw text and the vector is produced later (see below). Each node embeds its own
  shard's rows.
- **Query** — a **string** `NEAREST(text_col, 'some query', k)` embeds the query
  string once (a single-input call to the same endpoint) and searches with the
  returned vector. A numeric-array `NEAREST` skips inference entirely.

**Never blocks a write (out-of-band embedding).** The write path never calls the
model server. A write commits with the raw text (the source of truth); a
background worker then drains a queue: it gathers a batch of pending texts under
a brief lock, POSTs them to the endpoint **off** the engine lock, and inserts the
returned vectors under a brief lock afterward. So if the model server is slow or
**down**, rows simply stay queued and searchability lags — **no write is ever
blocked or failed**, and the queue drains when the server returns. Each node
embeds its own shard; a crash-window delta (rows written but not yet in the HNSW
snapshot) is re-queued on restart, and a freshly created index backfills its
existing rows the same way.

## Searching (SQL)

```sql
SELECT id, _distance FROM docs NEAREST (embedding, [0.1, -0.2, 0.9], 10);
SELECT id FROM docs NEAREST (embedding, [0.1, -0.2, 0.9], 10) WHERE cat = 'news';
```

`NEAREST (<path>, <query>, <k>)` returns the `k` nearest rows ordered
nearest-first with their distance exposed as `_distance`; `<query>` and `<k>`
may be bind parameters. Full grammar in
[`QUERY_SYNTAX.md`](QUERY_SYNTAX.md#vector-search-nearest).

## Searching (API)

The SQL path above calls into the same embedded/cluster methods directly
usable from Rust:

```rust
// Embedded single-node:
let hits = db.vector_search("docs_emb", &query_vec, 10, &None)?;        // (key, doc, distance)
let hits = db.vector_search("docs_emb", &query_vec, 10, &filter)?;      // filtered ANN

// Cluster coordinator (distributed): scatters to every node's local HNSW,
// merges the per-shard top-k by distance, then re-reads survivors at quorum.
let hits = node.vector_search("docs_emb", &query_vec, 10, &filter)?;
```

The embedded `create_vector_index(name, table, path, metric, dim)` also exists
(pass `dim = None` to infer from existing rows — single-node only).

## Distributed search

Similarity can't be routed to one shard, so distributed ANN **broadcasts** to all
nodes and merges — the same scatter-gather skaidb uses for secondary-index
pushdown. Each node runs its local HNSW top-k; the coordinator merges by
distance, then re-reads the survivors at the read quorum (authoritative
last-writer-wins vector) and applies the filter. The index is implicitly
replicated/fault-tolerant because each replica derives its graph from the rows
it already holds. Note: per-shard top-k merge means global recall depends on each
shard's recall, so the coordinator over-fetches per shard (more so when a filter
is present, since filtering happens after the re-read).

## How it works

- **HNSW** (Hierarchical Navigable Small World): a layered proximity graph. A
  search descends with a small beam from a sparse top layer to the dense base
  layer, following edges toward the query, giving high recall at a fraction of
  a brute-force scan. Neighbor edges are chosen with the diversity heuristic
  (Malkov & Yashunin Algorithm 4), which preserves long-range links on
  clustered data — closest-only selection lets dense near-duplicate islands
  wire exclusively to each other and leaves whole regions unreachable.
  Metrics: cosine (vectors normalized on insert), squared L2, negative dot
  product. Verified at **>90% recall vs. brute force** on random data across
  all three metrics, plus dedicated self-recall tests on tightly clustered
  near-duplicate data.
- **Filtered search** evaluates the predicate against candidates surfaced by the
  graph; the graph is still traversed through filtered-out nodes for
  connectivity (the basic filtered-HNSW approach).

## How vector DBs compare

ANN is a distinct index family from the B-tree / inverted indexes the OLTP and
search engines use. Where vector search sits across the systems compared in
[`docs/BENCHMARKS.md`](BENCHMARKS.md) plus dedicated vector stores:

| Capability | PostgreSQL | MongoDB | Elasticsearch | Qdrant | Milvus | Weaviate | skaidb |
|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| Vector ANN (kNN) | ⚠️ `pgvector` | ⚠️ Atlas | ✅ `dense_vector` | ✅ | ✅ | ✅ | ✅ (HNSW, embedded) |
| Filtered ANN (`WHERE` + vector) | ✅ | ✅ | ✅ | ✅ (core) | ✅ | ✅ | ✅ |
| ANN index types | HNSW/IVFFlat | HNSW | HNSW | HNSW (+quantization) | HNSW/IVF/PQ/DiskANN/GPU | HNSW | HNSW |
| Distributed vector search | single-primary | sharded | sharded | sharded | sharded | sharded | ✅ (sharded scatter-gather) |
| Primary durable store + tunable consistency | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ |

The dedicated vector DBs (Qdrant, Milvus, Weaviate; plus managed Pinecone) are
specialists — superb at ANN and filtered ANN, but not transactional systems of
record, so they usually run beside a primary DB. The general engines add vector
search as a *feature* (pgvector, Mongo Atlas, ES `dense_vector`). skaidb sits in
that second group: a durable, tunably-consistent store that can also do
filtered, distributed ANN — for moderate vector sets (each node's graph is
in-memory; see limitations).

## Limitations

- **In-memory** — the whole graph (vectors included) lives in RAM
  (~1 GB resident for 182k×768). The RAM-lean evolution is quantized vectors
  in RAM with exact vectors re-read from the table.
- **Snapshots** — each HNSW persists to `<data>/vector/<name>.hnsw` (written
  on build and graceful shutdown). A restart loads the snapshot and replays
  only rows stamped after its watermark — seconds, where the from-scratch
  build of a 182k×768 graph took 10–40 minutes per restart. A
  construction-parameter change or corrupt file falls back to a full
  rebuild.
- `ALTER VECTOR INDEX <name> SET (ef = <n>)` retunes the search-time
  candidate-list size live (higher = better recall, slower queries;
  persisted, applies immediately). `m`/`ef_construction` shape the graph
  and need DROP + CREATE.
- **Recall/latency beyond `ef`** aren't tuned to production ANN libraries;
  large/high-dimensional workloads want a specialist.
- Vectors must be arrays of `int`/`float` of a single, consistent dimension.
