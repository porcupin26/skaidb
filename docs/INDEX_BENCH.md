# Secondary-index performance

Synthetic measurements of index acceleration on the **embedded engine** (no
network), comparing each query shape with a full table scan vs. the matching
index, plus write throughput with and without index maintenance.

Reproduce:

```sh
cargo run --release --example index_bench -p skaidb-engine -- <data_dir> [rows] [pad_bytes]
```

Rows are `{ id (PK), v, g, pad }`: `v` is high-cardinality (equality / range /
`ORDER BY`), `g = id % 10` is low-cardinality (composite leading column), `pad`
inflates row size. Indexes: `idx_v ON t(v)` and `idx_gv ON t(g, v)`. Each query
is best-of-3; the range/composite windows select ~1% of `v`.

## Reads — 1,000,000 rows (workstation, NVMe)

| Query | full scan | with index | speedup | rows |
|-------|----------:|-----------:|--------:|-----:|
| equality `v = X` | 1,105 ms | ~0 ms | point lookup | 1 |
| range `v` in ~1% | 1,101 ms | 34 ms | **~32×** | 10,055 |
| composite `g = .. AND v` in ~1% | 899 ms | 2 ms | **~430×** | 1,006 |
| top-N `ORDER BY v LIMIT 10` | 2,557 ms | 758 ms | ~3.4× | 10 |

A full scan is O(rows); an indexed equality/range/composite scan touches only the
matching slice, so the speedup grows with the dataset. `ORDER BY … LIMIT` avoids
the sort (≈3.4×) but an *unbounded* order-by still materializes the index in
order before the limit applies — a lazy merge would make it a true top-N read.

## Reads — 100,000 rows (homelab LXC, 1 vCPU i7-8550U @ 1.8 GHz)

| Query | full scan | with index | speedup | rows |
|-------|----------:|-----------:|--------:|-----:|
| equality `v = X` | 529 ms | 138 ms | 3.8× | 0 |
| range `v` in ~1% | 540 ms | 146 ms | 3.7× | 979 |
| composite `g = .. AND v` in ~1% | 523 ms | 172 ms | 3.0× | 109 |
| top-N `ORDER BY v LIMIT 10` | 1,053 ms | 143 ms | 7.4× | 10 |

Smaller wins than at 1M: fewer rows (less full-scan to beat) and a much slower
core where the per-row cost of individual indexed point-fetches weighs more
heavily against a sequential scan. The index still helps every shape.

## Write cost of maintaining indexes

| | workstation, 1M rows | LXC, 100k rows |
|--|--------------------:|---------------:|
| insert, no index | 275,000 rows/s | 399 rows/s |
| insert, 2 indexes | 59,000 rows/s (**21%**) | 199 rows/s (**50%**) |

Each extra index is a second order-preserving write per row, so throughput drops
roughly in proportion to the number of indexes — the usual read/write trade-off.

## Notes

- **The embedded write path fsyncs per row** (group-commit batching lives in the
  cluster coordinator, not the single-node engine). On the LXC's slow disk that
  caps bulk load and index builds (≈400 rows/s; a 100k index build ≈ 170 s) — the
  limit is fsync latency, not CPU or disk space (100k rows = 45 MB, 1M = 241 MB,
  both far under the 4 GB volume, so no resize was needed).
- These are single-node/embedded numbers that isolate the planner. In a cluster,
  a non-PK indexed read additionally pays the index-pushdown fan-out (scatter to
  each member's local index, then a quorum re-read per candidate key).
