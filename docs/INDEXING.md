# Indexing guide

Everything about making skaidb queries fast: what index types exist, how to
create them, how the planner picks one, and — most importantly — how to look
at a slow workload and decide which index it needs.

Related: [QUERY_SYNTAX.md](QUERY_SYNTAX.md) (grammar), [SEARCH.md](SEARCH.md)
(full-text), [VECTOR.md](VECTOR.md) (HNSW), [LLM.md](LLM.md) (the condensed
LLM-facing reference).

## Index types at a glance

| Type | Create | Answers | Notes |
|------|--------|---------|-------|
| Primary key | `CREATE TABLE t (PRIMARY KEY (a [, b ...]))` | point reads, **prefix-equality slices** (+ one trailing range) | the table IS the index; `WHERE a = ?` on PK `(a, b)` scans only that slice |
| Secondary | `CREATE INDEX i ON t (a, b, ...)` | equality/range filters, index-served `ORDER BY`, index-only counts | composite = leftmost-prefix; see planner section |
| Multikey | `CREATE INDEX i ON t (a, tags[])` | array **element** equality (`tags = 'x'` containment), exact index-only counts | one `[]` component per index; entry per element |
| Vector | `CREATE VECTOR INDEX v ON t (emb) DIM n [USING cosine\|l2\|dot]` | `NEAREST` k-NN | HNSW; snapshotted for fast restarts |
| Search | `CREATE SEARCH INDEX s ON t (body, title) [WITH (...)]` | `MATCH()`, `SEARCH()`, BM25 ranking, fast-field aggregations | see SEARCH.md; fast fields answer `GROUP BY`/counts index-side |

Memory tables (`WITH (memory = true)`) reject indexes — they are ephemeral
by contract.

## Managing indexes

```sql
CREATE INDEX IF NOT EXISTS i_mail_star ON mail (account, _tombstone, is_starred);
SHOW INDEXES;          -- name, table, type, paths (multikey keep their []),
                       -- and `local`: THIS node's live state (ok/building/missing)
DROP INDEX IF EXISTS i_mail_star;
```

- DDL broadcasts cluster-wide and **acks at schema-apply**; each node then
  pages its backfill in the background (brief locks, memory-flat — a
  183k-row backfill takes a few minutes per node). `SHOW INDEXES` shows
  `secondary (building)` until that node completes; the planner never uses
  a building index, so queries fall back to their pre-index plans until
  then.
- Every statement, DDL included, lands in the query log with its real
  duration; slow ones also land in the slow-query log (see *Spotting the
  missing index* below).
- The UI's **Inventory** tab lists every index with type, definition,
  approximate entries, and disk size.
- `ALTER TABLE ... RENAME COLUMN` rewrites index definitions (including
  multikey markers) automatically.

## How the planner chooses (what your index must look like)

- **Leftmost prefix.** A composite index `(a, b, c)` serves filters that pin
  `a` (then `b`, then `c`) by equality, plus one trailing range on the first
  unpinned column. `WHERE b = 1` alone does not use `(a, b)`.
- **Selectivity ranking.** Among usable indexes, the one consuming the most
  equality columns (then a range) wins — a two-equality probe beats a
  sibling index that pins one column and spans half the table.
- **A fully pinned primary key is a point-read set.** Every PK column
  pinned by `=` or a literal `IN` list (bound array parameters included)
  resolves to exact candidate keys — one bloom-gated point read each
  (cross product on composite keys, ≤ 1000), never a scan; `EXPLAIN`
  shows `point-read set`. On a cluster the keys route to their replica
  sets, and `ORDER BY <indexed> LIMIT k` at QUORUM takes a distributed
  sorted top-k (see CLUSTERING.md) instead of gathering the match set.
- **ORDER BY + LIMIT prefers a sorted walk.** An index whose next-unpinned
  column matches the `ORDER BY` column serves the sort directly and stops at
  the limit (`(account, date)` for `WHERE account = ? ORDER BY date DESC
  LIMIT 50`). Multi-key `ORDER BY` works when the leading sort key is the
  indexed one (tie groups re-sort exactly). **But**: when a strictly more
  selective equality index also covers the filter, the planner first probes
  its range (a capped, O(cap) peek) — if the filter matches almost nothing,
  it answers through the equality index instead of walking the whole sorted
  range finding nothing.
- **Index-only counts.** A filtered `COUNT(*)` whose conjunctive
  equality/range filter is fully consumed by one index is answered from
  index-range cardinality — no row reads, safe at any table size. One
  NULL-safe negated equality (`col != v OR col IS NULL`, the Mongo-`$ne`
  shape) beside a covering conjunction counts by complement (two range
  cardinalities).
- **Multikey gate.** A multikey index is used only when every column through
  the `[]` component is equality-pinned (the element probe). Under that
  gate, counts are exact — the entry key embeds the row key, so duplicate
  elements in one array collapse. Ranges or sorts *on the array column* fall
  back to scans by design.
- **Consistency matters.** On a full-copy cluster, reads at consistency
  `"one"` answer non-covering counts, DISTINCT, and sorted+limited gathers
  with a single local pass; quorum reads pay a cross-replica page merge.
  Interactive/UI reads that tolerate a beat of staleness should send
  `"one"`.

## Spotting the missing index

The workflow that has caught every production shape so far:

1. **Read the slow-query log** (`slow.log`; every statement is also in the
   query log with its duration). Group by statement *shape* — the masked
   literals make identical shapes easy to aggregate.
2. **Classify each recurring shape:**
   - *Point read?* (`WHERE pk = ?`) — should be milliseconds at any size. If
     slow, the row itself is fat (oversized documents; move blobs out of
     rows) — no index will help.
   - *Equality filter, no order?* — needs a secondary index consuming every
     equality column: `(a, b)` for `WHERE a = ? AND b = ?`. Watch for the
     scan-budget error (`scan budget exceeded`) — it is the engine telling
     you a filter had no usable index.
   - *Filter + `ORDER BY x LIMIT n`?* — needs the equality columns followed
     by the sort column: `(account, date)`; add trailing sort tiebreakers as
     further index columns.
   - *Filtered `COUNT(*)`?* — same index as the filter, and make it consume
     the **whole** filter so the count is index-only. `!= `/`IS NULL`
     complements need the positive-equality form indexed.
   - *Array containment* (`tags = 'x'` where `tags` is an array)? — a
     multikey index `( ..., tags[])`. Without it the filter cannot be served
     by any scalar index and always scans.
   - *Substring/text search?* — there is no `LIKE '%x%'` fast path; use a
     search index and `MATCH`/`SEARCH()`. Counts and `GROUP BY` over search
     predicates can be answered by fast fields.
   - *k-NN / semantic?* — vector index + `NEAREST`.
3. **Verify with the application's real literals at its real consistency.**
   A hand-written probe with a different type (string where the app sends an
   int) cross-type-compares to NULL, matches nothing, and tells you nothing.
4. **After creating the index, re-check the same log.** The shape should
   either vanish or drop to the point-read/limit-bounded cost class.

### Reading the query log

Every executed statement — DML, SELECT, and DDL alike — is recorded:

```
[query] 33ms SELECT * FROM accounts WHERE "email" = ? LIMIT ?
[query] 376769ms CREATE INDEX IF NOT EXISTS i_star ON mail (account, _tombstone, is_starred)
[slow-query] 6615ms SELECT count(*) FROM mail WHERE "_tombstone" = false AND "labels" = ?
```

Config keys (see INSTALL.md): `query_log_enabled`, `query_log_masked`
(literals → `?`), `query_log_file` / `slow_query_log_file` (separate sinks),
`slow_query_ms` threshold. Errors are recorded on the statement line, so a
timeout or budget kill is visible next to its SQL.

## Costs and caveats

- Indexes are write-amplification: every row write updates every index on
  the table (multikey: once per array element). Index only what queries
  need.
- Backfills stream but still read the whole table once per node — schedule
  large ones accordingly; `IF NOT EXISTS` makes retries idempotent.
- An index whose leading column is wrong for your filters is worse than
  useless — it can win selectivity ranking with a huge range. Prefer the
  most selective equality columns first, then the sort column.
- Index-only counts are exact but reflect the local replica when read at
  consistency `"one"` — a count lagging a write by a beat.
