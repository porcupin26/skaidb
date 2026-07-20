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

**Never blocks a write.** Embedding is out of band: a write commits with the
raw text (the source of truth) and a background worker embeds it — a batch of
texts POSTed to the endpoint OFF the engine lock, the returned vectors inserted
after. If the model server is down, rows stay queued and searchability lags, but
no write is ever blocked or failed. Each node embeds its own shard; a crash-
window delta (rows not yet in the HNSW snapshot) is re-queued on restart. Config
(`[inference]`): `url` (OpenAI/TEI-compatible `{"model","input":[…]}` →
`{"data":[{"embedding":[…]}]}`), `model`, `dim`, `api_key`, `batch_size`,
`timeout_secs`, `tls_verify`/`tls_ca`.

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
