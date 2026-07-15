# skaidb — to do

**The single consolidated list of all pending work**, roughly in priority
order within each area. Shipped feature state lives in
[SEARCH.md](SEARCH.md) / [TIMESERIES.md](TIMESERIES.md) /
[VECTOR.md](VECTOR.md) / [UI.md](UI.md) / [GRAFANA.md](GRAFANA.md) / the
README; measured results, performance dead ends, and benchmark
methodology in [BENCHMARKS.md](BENCHMARKS.md); everything completed, in
git history.

## Full-text search

- [ ] **[fts] Sharded scatter, residue** — aggregations (incl. AVG via
  the sum+count rewrite), sorted top-k, and key-routed per-hit explain
  all shipped (see SEARCH.md). Remaining niceties: residual SQL filters
  on the sorted scatter (needs a serializable predicate subset on the
  wire), grouped per-bucket metrics (blocked on the tantivy#2992 guard),
  and distinct counts (need term-set/sketch partials).
- [ ] **[fts] Lift the grouped-metrics pushdown guard** when
  [quickwit-oss/tantivy#2992](https://github.com/quickwit-oss/tantivy/issues/2992)
  is fixed upstream (per-bucket metrics currently take the exact row
  fallback; doc-count-only groupings still push down).
- [ ] **[fts] Phase-9 residue**: the 24 h mixed ingest+query soak
  (deferred on request); disk-full-mid-merge injection (needs a
  fault-injecting filesystem — torn-dir injection shipped and found a
  real bug); re-run the ES A/B campaign to publish post-guard,
  post-`MATCH_CROSS`/`BOOSTED` numbers in BENCHMARKS.md.
- [ ] **[fts] Global BM25 statistics mode** — per-shard stats today (like
  ES across shards); an optional global-stats mode if result-set parity
  tests ever care.
- [ ] **[fts] ES-REST extras on demand**: `minimum_should_match` > 1,
  multi_match per-field `^boosts` (declined today with a pointer to
  `<col>.boost`), `top_hits` explicit sort, `_mget`, index templates —
  add when a real client needs them.
- [ ] **[fts] Merge-policy tuning on LXC-class disks**: conditional —
  revisit only if an ingest-heavy workload surfaces merge stalls (the
  ingest win over ES leaves no urgency).

## Time-series

- [ ] **[ts] Streamed TS results** — TS SELECT materializes before the
  (streamable) wire; large raw range dumps should stream end-to-end like
  row-table `QueryStream`.
- [ ] **[ts] Label postings index** — regex matchers shipped
  (scan-based, fine at moderate cardinality); a postings index remains
  the perf unlock for high-cardinality matching.
- [ ] **[ts] PromQL partial gather** — investigated 2026-07-08 and
  **declined as inexact**: the raw evaluator's range window is
  `[t-w, t]` (both boundaries inclusive) while partials bucket
  `[b, b+step)`, so samples landing exactly on step boundaries — the
  common case for scraped data — would silently differ. Needs a
  windowed-partial variant (per-window `(t-w, t]` partials over the
  wire). Groundwork shipped: `Backend::ts_partials` exposes the cluster
  partial scatter to the server layer.
- [ ] **[ts] PromQL round 3** — subqueries, `group_left`/`group_right`
  many-to-one matching, `topk`/`bottomk`; then the node-exporter
  dashboard panel-by-panel diff against a real Prometheus (the original
  phase-7 exit criterion).
- [ ] **[ts] Rollup opportunistic serving, clustered** — single-node
  shipped; clustered needs a min-over-replicas head-boundary exchange
  (pairs naturally with the sharded-partials wire work).
- [ ] **[ts] Exemplars / native histograms** — chunk-format headroom is
  reserved.
- [ ] **[ts] Validation soak** — 24 h Prometheus remote_write
  side-by-side with its own TSDB, zero-loss comparison (the TS phase-4
  exit criterion; deferred with the other soaks).

## SQL surface gaps (capabilities with no native SQL form)

Everything below works today through another surface (HTTP admin,
`skaidbsh` backslash commands, config file, or the ES subset) but cannot
be spoken in SQL. Each row carries the suggested SQL extension.

| Capability | Today | Suggested SQL extension |
|---|---|---|
| Batch scripts | one statement per call | multi-statement bodies are a protocol decision, not grammar — probably keep as-is |

Everything from the original gap list with real pull has shipped
(`EXPLAIN`, admin statements, `BACKUP`/`RESTORE`, TTL, `GROUP BY ... TOP
k BY` per-group top-k). General window functions (`ROW_NUMBER() OVER
(...)`) remain unimplemented; `TOP k BY` covers the common case.

## agencik integration wishlist (phased)

Gaps found migrating **agencik** off its MongoDB-emulation façade onto
native skaidb (`~/projects/agencik/docs/skaidb-wishlist.md`). The wishlist
was probed against a *deployed* build; each item below was re-checked
against `main`. Sequenced by priority × dependency. Phases 2/3/5 are
independent tracks; only batched `executemany` (P2) depends on the Python
prepared-statement work in phase 1.

**Phase 1 — P0 unblockers (native adoption)** ✅ shipped

- [x] **[driver] Python prepared statements** — typed value encoder
  (list→Array, dict→Document), `OP_PREPARE`/`OP_EXECUTE`/`OP_CLOSE`, per-conn
  prepared cache, `RESP_PREPARED` handling, unpreparable-kind fallback to text
  binding; `Cursor.execute` routes params through it. Removes the REST-vs-binary
  split transport.
- [x] **[sql] `IN` / `NOT IN`** — `Expr::InList`, contextual-ident parse,
  three-valued eval with array-element flattening (bound array param) and
  multikey containment, cluster filter-pushdown codec. Still a residual filter
  (no index/PK pushdown yet — see the IN/OR pushdown follow-up below).

**Phase 2 — P1 Python driver ergonomics** (all in `drivers/python`) ✅ shipped

- [x] **[driver] `connect(database=…)`** — issues `USE <db>` as part of
  connecting, so the session starts in the app database.
- [x] **[driver] Multi-seed + failover + pool** — `seeds=[…]` tried in
  randomized order until one connects; `reconnect()` fails over; thread-safe
  `ConnectionPool` / `skaidb.pool(…)` discards broken connections on checkin.
- [x] **[driver] Split connect/read timeout; wrap mid-query errors** —
  `connect_timeout`/`read_timeout` split; all mid-query transport errors wrap
  as `OperationalError`; `is_usable()` (cheap) + `ping()` (round-trip) for pools.
- [x] **[driver] Batched `executemany`** — the per-connection prepared cache
  makes `executemany` prepare once and reuse the id across rows (no re-parse).
  A single-round-trip batched wire path remains an optional future optimization.

**Phase 3 — P1 SQL grammar** ✅ shipped

- [x] **[sql] `BETWEEN`** — `Expr::Between`, contextual-ident parse (bounds at
  additive precedence so the separator `AND` survives), three-valued eval;
  literal bounds feed `collect_comparisons` → real index/PK **range pushdown**.
- [x] **[sql] `LIKE` / `ILIKE`** — `Expr::Like` + a `%`/`_` two-pointer glob
  matcher (no escape sequence); `ILIKE` lowercases both sides; non-string
  operands are unknown (never a query error on mixed-type columns). Residual
  filter only — a `'prefix%'` → prefix-range scan remains a future optimization.
- [x] **[sql] Object/document literals** — `{key: lit, ...}` builds
  `Value::Document` in any expression position (keys: bare idents or strings,
  reserved words need quoting; values constant, nesting allowed). `SET
  meta.addr = {…}` replaces the sub-document; dotted-path `SET` documented as
  the scalar-leaf idiom. All three predicates travel the internode filter
  codec, so clustered filtered scans serve them.

**Phase 4 — P2 SQL niceties**

- [ ] **[sql] `to_timestamp` / `CAST`** — unblocks Mongo-migrated ISO-8601
  string timestamps. Ship `to_timestamp`/`parse_iso` as scalar functions
  first (one `eval_func` arm each, no grammar change); `CAST(x AS t)`
  syntax later if a client needs the standard spelling.
- [ ] **[sql] `SELECT <expr>` without `FROM`** — lowest ROI (app falls back
  to `SHOW TABLES`); `from` is non-optional engine-wide, so needs an
  `Option`/sentinel + synthetic single-row source. Do only if justified.

**Phase 5 — P1/P2 localized ops/RBAC tweaks** (`crates/skaidb-server`,
`skaidb-auth`, engine)

- [ ] **[ops] `DROP INDEX` table-scoped like `CREATE`** (P1) — `CreateIndex`
  is `(Create, Table)` but `DropIndex` is `(Drop, Global)`, so an app role
  can create indexes it can never drop. `DROP INDEX` carries only the index
  name, so scoping needs an index→table catalog resolution in
  `required_privilege` (or a narrower per-`Index` privilege).
- [ ] **[ops] Read-only introspection grant** (P2) — `SHOW CLUSTER` /
  `SHOW CONFIG` are `(Admin, Global)`; split the read-only members to a
  lesser gate (relax the `admin::handle` re-check in lockstep). Precedent:
  the redacted low-privilege topology read in `rest.rs`.
- [ ] **[ops] Scan-budget error names the column** (P2) — enrich the
  `scan budget exceeded (N rows examined)` message at the catch site (where
  the parsed statement + catalog are in scope), not inside the
  context-free thread-local meter.

**Phase 6 — deferred, large architecture** (already on the lists below)

- [ ] **[cluster] Global value-sharded secondary indexes** (C-1) — indexes
  are local-per-node today, so a non-PK indexed read scatters to every
  peer; a value-sharded global index would route it to the value's owner.
  Rearchitects index maintenance + routing + ring + internode protocol.
- [ ] **[cluster] Distributed / multi-key transactions** (C-2) — cluster is
  single-node-only (`BEGIN/COMMIT/ROLLBACK` rejected; autocommit per
  statement); no 2PC/coordinator exists. If the ask is only to *confirm +
  document*, that is already the shipped behavior.

## Performance

(The audit record — what already holds, measured dead ends, methodology —
lives in [BENCHMARKS.md](BENCHMARKS.md#performance-engineering-notes).)

- [ ] **[perf] Merkle-tree anti-entropy** — paged repair compares every
  key each pass; a Merkle tree per table would make repair cost
  proportional to divergence, not table size.
- [ ] **[perf] Vector index persistence** — HNSW graphs are in-memory
  and rebuilt from the table on open (slow for large sets). Persist
  per-segment graphs alongside the LSM (snapshot + mmap), quantized
  vectors in RAM.
- [ ] **[perf] Lazy index-order merge for unbounded `ORDER BY`** —
  `ORDER BY <indexed>` without `LIMIT` still materializes the index in
  order first; a lazy merge would stream it. (With `LIMIT` the top-k
  path already avoids the sort.)
- [ ] **[perf] Memtable flush without clones** — flush streams entries
  but still clones each key/value even though the memtable is dropped
  right after; a consuming iterator would halve the transient flush
  spike. Background path only.
- [ ] **[perf] Per-statement replica/peer snapshot** — `replicas_for`
  builds a fresh `Vec` per row in batch replication and peer addresses
  clone per fan-out site. Measured class: a few small allocations next
  to an fsync + RTT — cleaner CPU profile, no expected throughput
  change.
