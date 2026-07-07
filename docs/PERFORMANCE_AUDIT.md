# Performance status & outstanding work

*Originally a full-codebase audit (2026-07-03, v0.16.0) of the storage, engine,
sql, cluster, server, and proto crates. Everything actionable from that audit
was implemented and measured across v0.16.2 – v0.19.0 — the release-by-release
record is in git history and [BENCHMARKS.md](BENCHMARKS.md). This document now
tracks what is deliberately **not** done: open items, nice-to-haves, and
measured dead ends kept on record so they aren't re-attempted without new
evidence. Last reviewed: 2026-07-07 (v0.34.0).*

## Current state (what already holds)

- Scans stream (k-way merge over memtable + SSTables, O(pages) memory) with
  early-stop for plain `LIMIT`; `scan_prefix` is range-bounded.
- Reads run under a shared lock (`&self`); writes group-commit one fsync per
  multi-row statement; replica fan-out is pipelined and batched
  (`ApplyBatch`), with the async tail drained and regrouped per peer.
- Equi-joins hash-join; unfiltered `COUNT(*)` answers from key stats; `ORDER
  BY … LIMIT k` is a selection, not a full sort; per-table block cache +
  sharded read cache + bloom filters on the point-read path.
- SQL parses once per request; prepared statements skip the parse entirely;
  hot metrics are pre-registered atomics; framing layers reuse per-connection
  buffers and allocate nothing steady-state.
- Row-returning results can stream in ~256 KB chunks (`QueryStream`), and
  distributed full-table gathers + anti-entropy are paged (2,000 rows/page) —
  coordinator memory is O(pages in flight), not O(table).
- Client connections **pipeline**: id-tagged requests (`OP_TAGGED`) let a
  client keep any number of requests in flight per connection; the server
  executes serially in order (session semantics unchanged) and echoes the id
  on every frame, so a batch pays one round-trip of link latency
  (`Client::pipeline`). Old servers reject the opcode cleanly.
- Topology changes are paged: rebalance, drain, and reclaim scan shards
  2,000 rows at a time (like repair/gathers) — a join or decommission
  against a multi-GB table holds one page + one in-flight batch, not the
  shard.
- `[profile.release]`: `lto = "thin"`, `codegen-units = 1`.
- Checked and fine: TCP_NODELAY everywhere, pooled internode connections,
  no locks held across network calls, no per-row re-parsing, compact binary
  wire protocol with LZ4 above 256 B.

## Outstanding — worth doing when the workload demands it

1. **Merkle-tree anti-entropy.** Paged repair compares every key each pass;
   a Merkle tree per table would skip identical ranges and make repair cost
   proportional to divergence, not table size.
2. **Lazy index-order merge for unbounded `ORDER BY`.** `ORDER BY <indexed>`
   without `LIMIT` still materializes the index in order first; a lazy merge
   would stream it. (With `LIMIT` the top-k path already avoids the sort.)
3. **Per-statement replica/peer snapshot.** `replicas_for` builds a fresh
   `Vec` per row in batch replication and peer addresses are cloned per
   fan-out site. Measured class: a few small allocations next to an
   fsync + RTT — cleaner CPU profile, no expected throughput change.
4. **Memtable flush without clones.** Flush streams entries but still clones
   each key/value even though the memtable is dropped right after; a
   consuming iterator would halve the transient flush spike. Background path
   only.
5. **Vector index persistence.** HNSW graphs are in-memory and rebuilt from
   the table on open (slow for large sets). Persist per-segment graphs
   alongside the LSM (snapshot + mmap), quantized vectors in RAM.

Note: the pre-`ScanPage` repair fallback (used only against peers too old
to answer `ScanPage` at all) necessarily remains O(table) — the old peer's
wire protocol has no paging. It never fires between current versions.

## Deliberately skipped (documented reasons)

- **Atomic HLC.** `HlcClock` stays a `Mutex` — it is only taken under the
  write lock, which already serializes writers, and repacking the 96-bit
  state risks the on-disk stamp format.
- **Boxing `Statement::Select`.** `clippy::large_enum_variant` is allowed
  with a comment instead; boxing would touch every match site in the engine
  for no runtime benefit.

## Measured dead ends — do not re-attempt without new evidence

- **Sync-path replication group commit** (tried v0.16.6): coalescing
  concurrent sessions' quorum writes into shared per-peer `ApplyBatch`
  flushes cost **~9%** concurrent-write throughput. The coordinator is
  CPU-bound on small nodes; the batching saved CPU on the peer (which had
  headroom) and spent it on coordinator queue/wake machinery (which had
  none). Analysis recorded next to the scatter path in `node.rs`. Async-tail
  batching (kept) is the part that paid.
- **Borrowed (`Cow`) predicate evaluation** (tried v0.19.0): removing the
  per-row column-value clones from `eval` measured **3–6% slower** on
  AND-chain predicates over 200k-row scans, reproducibly (alternating A/B,
  three rounds). Per-row document *decode* dominates scan cost — even
  deleting a 60-byte string clone per row was unmeasurable — while the `Cow`
  wrapper added real per-node overhead. Any future attempt must first make
  row decode itself borrowed/lazy; predicate-eval tweaks alone are
  optimizing a rounding error.
- **Allocation cleanups on fsync/RTT-dominated paths** (v0.16.7 framing
  pass): real CPU/allocator reduction, zero throughput change on this
  hardware. Fine to ship for CPU headroom, but don't expect ops/s.

## Methodology (hard-won, follow it)

- Scale the benchmark to the feature's target size — the 1,000-row suite hid
  two OOM bugs that 1M rows exposed immediately.
- Only trust alternating same-day A/B runs; leg ordering alone produces
  double-digit artifacts on a shared host.
- If every system lands in the same band on a fleet leg, suspect a shared
  environmental floor and isolate on a single node over loopback before
  concluding a change does nothing (or something).
