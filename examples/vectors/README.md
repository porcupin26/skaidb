# Vector search examples

An end-to-end walkthrough of skaidb's vector search: turn text into an
embedding, store it alongside the document, index it, and run
nearest-neighbor search — with and without a metadata filter.

| Language | Directory | Status |
|----------|-----------|--------|
| Python | [`python/`](python/) | ✅ verified |
| Rust | [`rust/`](rust/) | ✅ verified |

Run:

```sh
python3 python/vector_search.py [host] [port] [user] [password]
cd rust && cargo run --bin vector_search -- [host:port] [user] [password]
```

Both produce the same distances (up to f32/f64 rounding) — they implement
the identical toy vectorizer independently, in each language, as a
correctness cross-check.

## The workflow

```sql
CREATE TABLE docs (PRIMARY KEY (id));
CREATE VECTOR INDEX docs_emb ON docs (embedding) DIM 32 USING cosine;

INSERT INTO docs (id, category, text, embedding)
  VALUES (1, 'tech', 'The new GPU doubles inference throughput...', [0.02, -0.11, ...]);

SELECT id, category, text, _distance FROM docs
  NEAREST (embedding, [0.03, -0.09, ...], 3);

SELECT id, text, _distance FROM docs
  NEAREST (embedding, [0.03, -0.09, ...], 3) WHERE category = 'cooking';
```

1. **Vectorize.** Turn a document's text into a fixed-length numeric vector
   (an "embedding") — some representation where semantically similar text
   produces vectors that are close together by some distance metric (cosine,
   here). This step is *outside* skaidb: these examples use a small,
   dependency-free deterministic "hashing trick" vectorizer (`vectorize()`
   in each example) so the code runs with zero external dependencies and
   produces identical output every run — good for learning the SQL, not
   production semantic search. Swap it for a call to a real embedding
   model/API (OpenAI, Sentence-Transformers, Cohere, a local ONNX model,
   ...) and everything below is unchanged.
2. **Store.** The vector is just another document field — an array of
   numbers — alongside whatever other fields the row has. No separate
   vector-store service, no dual-write.
3. **Index.** `CREATE VECTOR INDEX ... DIM <n> USING <metric>` builds an HNSW
   graph over the field. `DIM` must match the vectorizer's output length;
   `USING` is `cosine` (default), `l2`, or `dot`.
4. **Extract (search).** `NEAREST (<path>, <query-vector>, <k>)` returns the
   `k` closest rows, nearest first, with the match distance exposed as
   `_distance`. Combine with `WHERE` to narrow the candidate set by ordinary
   fields (category, date, tenant, ...) — the classic "nearest neighbors
   *where* ..." filtered-search pattern behind RAG and recommendation
   systems.

Full grammar and constraints (what `NEAREST` can't combine with) are in
[`../../docs/QUERY_SYNTAX.md`](../../docs/QUERY_SYNTAX.md#vector-search-nearest);
how the index and distributed search work internally are in
[`../../docs/VECTOR.md`](../../docs/VECTOR.md).

## A note on binding the vector

The client-side drivers in [`../../drivers/`](../../drivers/) bind scalar
parameter types (`?`/`$1`) today, not arrays — so the Python example formats
the vector as a SQL array literal (`[0.1, -0.2, ...]`) directly into the
query text. The **native wire protocol** (used by the Rust driver and, since
prepared statements shipped, its `Prepare`/`Execute` opcodes) already
supports array-valued parameters end-to-end, so the Rust example binds the
query vector as a typed `Value::Array` through a prepared statement instead
— no string formatting, no injection surface. If you're writing a new
client-side driver, wiring array binding through is the natural next step to
get the same ergonomics as Rust's.
