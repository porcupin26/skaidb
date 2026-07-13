# Vector search (HNSW)

skaidb can store embeddings and run **approximate nearest-neighbor (ANN)**
search over them with an in-memory **HNSW** index, including **filtered** search
("nearest neighbors *where* …"). This is the index family behind semantic
search / RAG / recommendations.

> Status: **distributed** (sharded scatter-gather) but still in-memory and
> rebuilt from the table on open. Both index creation and the kNN query have
> SQL syntax (`NEAREST`, below). See limitations at the end.

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
(pass `dim = None` to infer from existing rows — single-node only). On reopen the
in-memory graph is rebuilt from the table's rows.

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

- **In-memory, rebuilt on open** — the HNSW lives in RAM and is reconstructed by
  scanning the table at startup (slow for very large sets). The performant fix is
  to persist per-segment graphs that ride the LSM (snapshot + mmap), with
  quantized vectors in RAM and exact vectors re-read from the table.
- `ALTER VECTOR INDEX <name> SET (ef = <n>)` retunes the search-time
  candidate-list size live (higher = better recall, slower queries;
  persisted, applies immediately). `m`/`ef_construction` shape the graph
  and need DROP + CREATE.
- **Recall/latency beyond `ef`** aren't tuned to production ANN libraries;
  large/high-dimensional workloads want a specialist.
- Vectors must be arrays of `int`/`float` of a single, consistent dimension.
