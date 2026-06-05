# Vector search (HNSW) — prototype

skaidb can store embeddings and run **approximate nearest-neighbor (ANN)**
search over them with an in-memory **HNSW** index, including **filtered** search
("nearest neighbors *where* …"). This is the index family behind semantic
search / RAG / recommendations.

> Status: an embedded-engine prototype. There is no SQL syntax for kNN yet (use
> the `Database` API below) and the index is single-node, in-memory, rebuilt
> from the table on open. See limitations at the end.

## Storing vectors

Vectors are ordinary document fields holding an array of numbers — SQL now
supports array literals:

```sql
CREATE TABLE docs (PRIMARY KEY (id));
INSERT INTO docs (id, cat, embedding) VALUES (1, 'news', [0.12, -0.04, 0.91, ...]);
```

## Building an index and searching (embedded API)

```rust
use skaidb_engine::Database;

let mut db = Database::open("data")?;
// metric: "cosine" | "l2" | "dot"; dimension is inferred from existing rows.
db.create_vector_index("docs_emb", "docs", "embedding", "cosine")?;

// k nearest to a query embedding:
let hits = db.vector_search("docs_emb", &query_vec, 10, &None)?;
for (key, doc, distance) in hits { /* ... */ }

// filtered ANN — only rows matching the predicate are returned, while the
// graph is still traversed for connectivity:
let filter = Some(/* Expr: cat = 'news' */);
let hits = db.vector_search("docs_emb", &query_vec, 10, &filter)?;
```

The index is maintained automatically on `INSERT`/`UPDATE`/`DELETE` (a replace
soft-deletes the old vector and inserts the new one), and is persisted as a
catalog definition — on reopen it is rebuilt from the table's rows.

## How it works

- **HNSW** (Hierarchical Navigable Small World): a layered proximity graph. A
  search greedily descends from a sparse top layer to the dense base layer,
  following edges toward the query, giving high recall at a fraction of a
  brute-force scan. Metrics: cosine (vectors normalized on insert), squared L2,
  negative dot product. Verified at **>90% recall vs. brute force** on random
  data across all three metrics.
- **Filtered search** evaluates the predicate against candidates surfaced by the
  graph; the graph is still traversed through filtered-out nodes for
  connectivity (the basic filtered-HNSW approach).

## How vector DBs compare

ANN is a distinct index family from the B-tree / inverted indexes the OLTP and
search engines use. Where vector search sits across the systems compared in
[`docs/INDEX_BENCH.md`](INDEX_BENCH.md) plus dedicated vector stores:

| Capability | PostgreSQL | MongoDB | Elasticsearch | Qdrant | Milvus | Weaviate | skaidb |
|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| Vector ANN (kNN) | ⚠️ `pgvector` | ⚠️ Atlas | ✅ `dense_vector` | ✅ | ✅ | ✅ | ✅ (HNSW, embedded) |
| Filtered ANN (`WHERE` + vector) | ✅ | ✅ | ✅ | ✅ (core) | ✅ | ✅ | ✅ |
| ANN index types | HNSW/IVFFlat | HNSW | HNSW | HNSW (+quantization) | HNSW/IVF/PQ/DiskANN/GPU | HNSW | HNSW |
| Distributed vector search | single-primary | sharded | sharded | sharded | sharded | sharded | ❌ (single-node) |
| Primary durable store + tunable consistency | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ |

The dedicated vector DBs (Qdrant, Milvus, Weaviate; plus managed Pinecone) are
specialists — superb at ANN and filtered ANN, but not transactional systems of
record, so they usually run beside a primary DB. The general engines add vector
search as a *feature* (pgvector, Mongo Atlas, ES `dense_vector`). skaidb now sits
in that second group: a durable, tunably-consistent store that can also do
filtered ANN — for moderate, single-node vector sets.

## Limitations (prototype)

- **No SQL kNN syntax** yet — searches go through the `Database::vector_search`
  API, not `ORDER BY embedding <-> [..] LIMIT k`.
- **Single-node, in-memory** — the HNSW lives in RAM and is rebuilt from the
  table on open (slow startup for very large sets); it is not sharded across the
  cluster, so there is no distributed vector search.
- **Simple neighbor selection** and a fixed `ef` — recall/latency aren't tuned to
  production ANN libraries; large/high-dimensional workloads want a specialist.
- Vectors must be arrays of `int`/`float` of a single, consistent dimension.
