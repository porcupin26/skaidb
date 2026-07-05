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
inflates row size (~120 B default). Indexes: `idx_v ON t(v)` and
`idx_gv ON t(g, v)`. Each query is best-of-3; the range/composite windows
select ~1% of `v`. Rows load as 500-row multi-row `INSERT`s, which group-commit
(one fsync per statement, not per row).

## Reads — 1,000,000 rows (workstation, NVMe, v0.19.0)

| Query | full scan | with index | speedup | rows |
|-------|----------:|-----------:|--------:|-----:|
| equality `v = X` | 262 ms | ~0 ms | point lookup | 1 |
| range `v` in ~1% | 292 ms | 27 ms | **~11×** | 10,055 |
| composite `g = .. AND v` in ~1% | 279 ms | 1.4 ms | **~198×** | 1,006 |
| top-N `ORDER BY v LIMIT 10` | 809 ms | 387 ms | ~2.1× | 10 |

A full scan is O(rows); an indexed equality/range/composite scan touches only
the matching slice, so the speedup grows with the dataset. `ORDER BY … LIMIT`
avoids the sort (≈2×) but an *unbounded* order-by still materializes the index
in order before the limit applies — a lazy merge would make it a true top-N
read. On small slow-core boxes (1-vCPU containers) the wins are smaller: less
full-scan cost to beat, and per-row indexed point-fetches weigh more against a
sequential scan — the index still helps every shape.

## Write cost of maintaining indexes

| workstation, 1M rows | rows/s |
|--|--------------------:|
| insert, no index | 368,000 |
| insert, 2 indexes | 91,000 (**25%**) |

Each extra index is a second order-preserving write per row, so throughput
drops roughly in proportion to the number of indexes — the usual read/write
trade-off.

## Notes

- These are single-node/embedded numbers that isolate the planner. In a
  cluster, a non-PK indexed read additionally pays the index-pushdown fan-out
  (scatter to each member's local index, then a quorum re-read per candidate
  key).
- 1M rows ≈ 241 MB on disk at this row shape.
