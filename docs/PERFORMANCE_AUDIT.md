# Performance Audit

*Audited: 2026-07-03 (v0.16.0). Full-codebase review of the storage, engine, sql, cluster, server, and proto crates. Findings ranked by expected impact. Line numbers refer to the tree at commit `c28d257`.*

## Implementation status (2026-07-03)

Everything below was implemented in the same pass, except three deliberate skips:

- **1.1/1.2** — `merged()` replaced by a streaming k-way merge (`KWayMerge` in storage `engine.rs`); `scan_prefix` delegates to `scan_range`; flush/compaction/retain stream block-in/block-out via `SsTable::write_stream`.
- **1.3** — `Cluster` trait reads and SELECT execution take `&self`; the server holds `RwLock<Database>` and serves reads under the shared lock (`execute_session_read_statement`).
- **1.4/1.5** — replica fan-out is concurrent (scoped-thread `scatter()` in `node.rs`); `Request::ApplyBatch` (wire tag 18) applies N rows under one lock + one fsync, used by migration/drain/repair/hint paths, with per-row fallback for rolling upgrades.
- **1.6/2.6** — unindexed gathers stream `scan_iter()` with early-stop for plain `LIMIT`; unfiltered `COUNT(*)` served from `key_stats().live_keys` (disabled inside transactions).
- **2.1** — equi-joins use a hash build/probe with residual `ON` re-verification; nested loop kept for RIGHT joins and non-equi `ON`s.
- **2.2** — per-table SSTable block cache (32 decompressed blocks, point-read path only). Batch re-sorting of index hits was **skipped**: the block cache removes the repeated decompression, which was the actual cost.
- **2.3** — SQL parsed once per request; `required_privilege` takes `&Statement`; engine exposes `execute_session[_read]_statement`.
- **2.4** — hot metrics are pre-registered atomics (enum-dispatched labels); no lock or allocation per op; `/metrics` output unchanged.
- **2.5** — transaction reads run the index planner and merge the overlay per key.
- **2.7** — filtered vector search decodes each candidate once (shared decode cache for traversal + results).
- **2.8** — `ORDER BY … LIMIT k` uses `select_nth_unstable` selection (O(n) + O(k log k)) with stable tie-breaks; sorts reorder by moves, not clones.
- **Tier 3 storage** — WAL frames encode in one pre-sized buffer (`append_op`); multi-row statements group-commit one fsync per touched engine (`put_deferred` + `flush_pending`); bloom built during block encode; `BufWriter` on table writes; sharded read cache (≤16 shards); point reads compare keys borrowed from the block; SSTable sizes cached; manifest tmp-file + directory fsync. **Skipped:** atomic HLC (96-bit state, already serialized by the write lock; repacking risks the on-disk stamp format).
- **Tier 3 network** — single-write/vectored framing; buffered frame reads on internode and client connections (handshake-safe); per-write thread spawns replaced by one persistent background worker; REST serializes once and drops the per-request `dup()`; `Value::encode_value_into` for per-cell wire encode; per-statement index-list memo.
- **Tier 3 SQL** — byte-cursor lexer (no `Vec<char>`, sliced tokens), allocation-free keyword match, tokens moved out of the buffer instead of cloned, array literals moved. **Skipped:** compiled predicates / borrowed `eval` values — a large `eval.rs` rewrite with regression risk; revisit if profiling still shows predicate overhead now that scans stream and early-stop.
- **Tier 4** — `[profile.release]`: `lto = "thin"`, `codegen-units = 1`.

Verified: 261 workspace tests green, clippy clean (`--all-targets`), plus an end-to-end smoke run (DDL, multi-row insert, indexed range + top-k, hash join, txn overlay count, REST).

The codebase has good bones — bloom filters, block-index seeking, WAL group-commit machinery, TCP_NODELAY everywhere, pooled mTLS internode connections, a compact binary wire protocol, and locks that are snapshotted before network I/O. The findings below are the places where design choices or hot-path allocations cap throughput and latency below what the hardware can do.

---

## Tier 1 — design bottlenecks (biggest wins)

### 1.1 Every scan materializes the entire database in RAM (storage)
`merged()` (`crates/skaidb-storage/src/engine.rs:436`) calls `entries()` on every SSTable at every level — decompressing every block, cloning every key and value — and collects it all into a `BTreeMap`. It backs `scan`, `scan_versioned`, `scan_versioned_with_tombstones`, `scan_prefix`, `key_stats`, and `retain`, plus the cluster migration scans. Cost is O(total live+dead rows) in time and memory per call; tables larger than RAM cannot be scanned at all.

**Fix:** streaming k-way merge iterator over per-SSTable block iterators (heap-ordered by key). SSTables are already sorted; the merge is O(rows) time and O(#tables × block) memory.

### 1.2 `scan_prefix` does a full-DB merge then filters (storage)
`scan_prefix` (`engine.rs:385`) calls `merged()` and filters `starts_with` afterward, so secondary-index prefix lookups pay O(whole database).

**Fix:** delegate to `scan_range(prefix, prefix_upper_bound)` — `scan_range` already seeks memtable BTree ranges and SSTable block indexes. Turns O(DB) into O(matches).

### 1.3 Reads take `&mut self` — engine is effectively single-threaded (engine)
`execute_statement`/`execute` (`crates/skaidb-engine/src/exec.rs:396`, `:389`) and the whole `Cluster` trait (`exec.rs:1752`) require `&mut` even for read-only SELECTs, so the server's `RwLock<Database>` can never run readers concurrently.

**Fix:** split the API so SELECT/gather paths take `&self`; only `put`/`delete` need `&mut`.

### 1.4 Replica fan-out is sequential (cluster)
`point_get` (`crates/skaidb-cluster/src/node.rs:1806`), `cluster_scan` (`:1966`), `index_lookup` (`:2049`), `filtered_lookup` (`:2096`), `vector_search` (`:2164`), `broadcast_ddl` (`:1616`) all loop over peers and block on each RTT in turn. Latency = sum of peer RTTs instead of max.

**Fix:** concurrent scatter-gather; return as soon as quorum is met.

### 1.5 Per-row coordination + per-row fsync (cluster)
Non-PK reads re-read each candidate key with its own sequential quorum `point_get` (`node.rs:2059`, `:2105`, `:2176`); writes call `replicate` once per row (`node.rs:2439`); there is no multi-get/multi-put RPC (`internode.rs:23-47`); the receiving replica fsyncs once per RPC (`node.rs:1763`). Bulk load throughput is capped at ~1/fsync-latency rows/sec/replica.

**Fix:** `MultiGet { keys }` / `MultiPut { writes }` internode RPCs; apply a batch under one lock with a single fsync; pipeline per-key gets.

### 1.6 Unindexed `LIMIT` decodes the whole table (engine)
The no-index branch of `gather_rows_planned` (`exec.rs:1078`) materializes and decodes every row via `scan_docs` before `apply_offset_limit` discards them. `SELECT * FROM t LIMIT 10` is O(table).

**Fix:** streaming scan with early-stop when there is no ORDER BY/DISTINCT/aggregate.

---

## Tier 2 — hot-path algorithmic fixes

### 2.1 Nested-loop joins, O(n·m) with per-pair clones (engine)
`gather_join_docs` (`exec.rs:1960`) pulls the whole right table unfiltered, then double-loops cloning the left tuple and right doc per candidate pair; `merge_tuple` (`exec.rs:1931`) clones every sub-document twice per surviving row. The `ON a = b` equality is never exploited.

**Fix:** hash join for equi-predicates (build on smaller side, probe); push equality filters into the right-side gather; drop the redundant unqualified copy.

### 2.2 Index range scan = one random point-get per row; no block cache (engine + storage)
After `scan_range` on the index, the engine does `table_engine.get(&row_key)` per entry (`exec.rs:1097`). Each get walks memtable → cache → every SSTable. `read_block` (`sstable.rs:283`) decompresses from disk on every access with no block cache, so rows sharing a block re-decompress it once per row (brotli at the bottom level).

**Fix:** batch-resolve index hits in key order; add a bounded LRU of decompressed blocks keyed by `(table_id, block_offset)`.

### 2.3 Same SQL parsed 2–3× per request; no plan cache (server + engine + cluster)
`required_privilege(sql)` parses for RBAC (`crates/skaidb-server/src/shared.rs:341` → `:477`); the engine re-parses (`exec.rs:451`); cluster DDL parses a third time (`node.rs:1535`, `:1611`). No prepared-statement or AST cache exists anywhere.

**Fix:** parse once per request, thread the `Statement` through (`required_privilege(&Statement)`, `execute_statement(stmt)` already exists); optional LRU keyed on SQL text.

### 2.4 Global metrics mutex + per-call `format!` (server)
Every query takes a single process-wide `Mutex` ~6–10 times, each preceded by a heap-allocated key string (`crates/skaidb-server/src/metrics.rs:76-159`, `shared.rs:393`). This serializes all connection threads on the hottest path.

**Fix:** pre-registered series with `AtomicU64` counters; no lock and no allocation on the hot path.

### 2.5 Transaction reads full-scan and ignore indexes (engine)
`gather_with_overlay` (`exec.rs:1022`) materializes the whole table into a `BTreeMap` per read while a txn is open; `matching_rows_ordered` bypasses the planner in a txn (`exec.rs:2139`).

**Fix:** run the index planner and merge only the overlay entries for the scanned range.

### 2.6 `COUNT(*)` decodes every row (engine)
Aggregates route through full `scan_docs` (`exec.rs:1871`, `:2584`); unfiltered `COUNT(*)` is `docs.len()` after decoding N documents, while storage already tracks `live_keys` (`engine.key_stats()`).

**Fix:** special-case unfiltered `COUNT(*)` to `key_stats().live_keys`.

### 2.7 Filtered vector search reads each candidate twice, mid-traversal (engine)
`vector_search` (`exec.rs:300`) does a storage get + full decode per HNSW candidate inside the graph walk, then re-gets and re-decodes each survivor.

**Fix:** cache decoded docs from the keep phase; longer-term keep filterable attributes alongside HNSW nodes.

### 2.8 `ORDER BY … LIMIT k` sorts everything (engine)
`select_rows` (`exec.rs:2524`) full-sorts then truncates; `sort_docs`/`sort_result_rows` also clone the entire row set to reorder (`exec.rs:2783`, `:2064`).

**Fix:** bounded binary-heap top-k (O(n log k)); reorder via permutation instead of cloning.

---

## Tier 3 — mechanical copy/allocation cleanups

### Storage write path
- **3× value copy, 3× key alloc per put** (`engine.rs:218-236`, `wal.rs:62-76`, `:204-219`): WAL op clone, payload encode copy, frame re-copy; key `to_vec()` for WAL record and memtable. Fix: encode the frame in one pre-sized buffer directly from the borrowed value; share the key.
- **Per-write fsync on the normal path** (`engine.rs:226`): group-commit machinery (`WalSync`) exists but only the buffered replication path uses it. Fix: batch-write API / deferred `sync_through`.
- **SSTable flush holds ~2× table in RAM and clones all keys for the bloom filter** (`sstable.rs:66-129`): stream blocks through a `BufWriter` (none exists in the crate), feed the bloom filter during encoding.
- **`merged()`/`merge_tables` clone the value on every overwrite** (`engine.rs:411`, `:732`): the `and_modify` closure forces a borrow; restructure with `get_mut`/`insert` to move.
- **Read cache: one global `Mutex`, locked twice per point read, deep-copies values** (`cache.rs:38-130`, `engine.rs:296-315`): shard by key hash, store `Arc<[u8]>`.
- **Point read decodes every entry it walks past in a block** (`sstable.rs:205-227`): compare keys borrowed in place; `to_vec()` only the match.
- **Memtable flush clones all entries** (`memtable.rs:84-95`): `mem::take` the old memtable and move entries out.
- **`stats()` stat()s every SSTable file per scrape** (`engine.rs:567`, `sstable.rs:200`): cache file length at write/open.
- **`HlcClock::now()` takes a `Mutex` per timestamp** (`hlc.rs:76`): atomic CAS over a packed u64.
- **Manifest tmp file not fsync'd before rename** (`engine.rs:694`): durability gap, flagged while in the area.

### Network / proto
- **Unbuffered socket I/O** (`crates/skaidb-proto/src/frame.rs:20-37`): ≥2 syscalls per frame each way; with NODELAY the 4-byte length prefix ships as its own TCP segment. Fix: single-buffer frame write, `BufReader` on reads.
- **No pipelining, no result streaming** (`binary.rs:56`, `message.rs:96`): whole result set buffered twice (row Vec + encoded Vec); peers' scan results fully buffered too (`node.rs:1943`). Fix: id-tagged in-flight requests, chunked row frames.
- **Per-cell allocation in row encode/decode** (`message.rs:109`, `internode.rs:789`): add `Value::encode_into(&mut Vec)`; decode borrowed.
- **1–3 OS threads spawned per write** for fsync overlap / hint flush / tail replication (`node.rs:1663`, `:1677`, `:1719`); unbounded thread-per-connection (`binary.rs:30`, `rest.rs:29`, `node.rs:1217`). Fix: small persistent worker pools; bounded accept pool.
- **REST gateway serializes response JSON twice and `dup()`s the socket per request** (`rest.rs:123`, `:309`, `:138`).
- **Per-fan-out peer-address clones; `replicas_for` allocates per key** (`node.rs:544-569`, `ring.rs:73-90`): snapshot once per statement.

### SQL parser (all crate-local)
- **Keyword match allocates via `to_ascii_uppercase()` per word token** (`token.rs:113`): match with `eq_ignore_ascii_case` / length-bucketed dispatch.
- **Whole input collected into `Vec<char>`, each token re-collected into a `String`** (`token.rs:195`, `:313-374`): lex over bytes, slice the input for the common case.
- **Parser clones every consumed token; identifier operands cloned twice** (`parser.rs:47`, `:745`): `mem::replace` the token out of the vec; match by reference before taking ownership.
- **`format!` for qualified names / eager default aliases** (`parser.rs:623`, `:491`).
- **Array-literal values cloned instead of moved** (`parser.rs:839`) — matters for large embedding inserts.
- **Predicate paths re-split per row** (`value.rs:113`, via `eval.rs:16`): pre-split path segments once per statement; evaluate against borrowed values.

---

## Tier 4 — build-level

- **No `[profile.release]` in the workspace `Cargo.toml`**: release binaries ship with LTO off and 16 codegen units. Add `lto = "thin"` (or `"fat"`) and `codegen-units = 1` — a typical 5–15% across the board. (`panic = "abort"` is deliberately *not* recommended: the thread-per-connection server relies on per-thread panic isolation.)

---

## Non-issues (checked, already good)
- TCP_NODELAY set on all sockets; TLS configs built once and Arc-shared; internode connections pooled and authenticated once, not per write.
- No locks held across network calls (snapshot-then-release pattern used throughout the cluster crate).
- No hot polling loops in production paths; wire protocol is compact binary with LZ4 above 256 B, not JSON.
- Index planner handles equality prefixes + trailing range and satisfies ORDER BY from index order with early-stop.
- No regex, no per-row re-parsing; `statement_type`/`tx_kind` fast paths already avoid full parses.

## Suggested attack order
1. Release profile (free).
2. Storage streaming merge + range-bounded `scan_prefix` (1.1, 1.2).
3. `&self` reads (1.3).
4. Parallel fan-out + batched replication RPCs + batched fsync (1.4, 1.5).
5. Parse-once threading, hash join, block cache, metrics rewrite (2.x).
6. Allocation cleanups (tier 3), re-running `bench/run_suite.sh` after each stage against `docs/BENCHMARKS.md`.
