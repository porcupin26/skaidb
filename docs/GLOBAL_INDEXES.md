# Global (value-sharded) secondary indexes — design

Status: **phases 1+2+3 shipped, phase 4 partially done** (entry plumbing
v0.89; routed read path + backfill v0.90; hardening v0.91; first A/B on
the bench fleet 2026-07-16 — see BENCHMARKS.md "Global-index routed
probe"). Phase-4 findings so far: **correctness exact** after two
bench-caught backfill fixes (batched drives v0.91.1; retry-or-abort
readiness v0.91.2), **latency parity at 2 members** (candidate resolve
dominates; scatter is one extra RPC there). Remaining: the 3+ member
run where the fan-out delta actually surfaces, then the prod-adoption
call. Phase-3 notes:

- **Repair verify leg** (`gidx_repair`, part of every repair pass): paged
  two-direction verification of entries against rows — *heals missing
  entries* (a missing entry silently hides its row from probes; the
  correctness direction) and *GCs orphans* (harmless, pure waste). On
  full-copy clusters everything is local point reads. At RF < members
  (v0.92) it is a batched cross-node exchange driven by each shard's
  PRIMARY owner: row-primaries derive their rows' entries and ask entry
  owners which exist (`KeysPresent`, absentees re-put to the entry's
  replica set); entry-primaries ask row-owners which entries are still
  produced (`GidxProduced` — recomputed on the node that HAS the row) and
  tombstone the rest. An unreachable owner skips the batch: silence is
  never treated as absence.
- **`building` convergence**: readiness advances the index's schema
  stamp, and schema replay emits `WITH (global = true, ready = true)` —
  a node that missed the `GidxReady` broadcast (down at the time, or
  freshly bootstrapped) converges on its next schema sync instead of
  never routing probes. `ready` is an internal DDL option.
- **Backfill resumability**: a repair pass that finds a global index
  still `building` re-queues the drive on that node; duplicate drives
  are idempotent LWW upserts and readiness is stamped, so whichever
  drive finishes first wins.
- **`IN`-list probes**: every index column pinned by `=`/literal `IN`
  expands to one probe range per value tuple (cross product, capped at
  100 ranges — past that the scatter paths win), each routed to its own
  value's replica set, candidates unioned into one resolve. A pin to an
  empty set answers empty without touching the ring.

Phase-2 notes:

- **Entry keys are self-describing for placement**:
  `u16 BE prefix_len ‖ values_prefix ‖ row_key`. Every ring lookup with
  table context (writes, repair ownership, reshard/joiner motion, reads)
  places `__gidx__` keys by the embedded VALUES prefix, so one value's
  entries live on one replica set — the routed-probe contract. ⚠ This
  supersedes v0.89's full-key-hash placement and array-encoded entry keys:
  an index created ON v0.89 must be dropped and recreated (none existed).
- **Probe**: full-tuple equality only (`plan_global_probe`), consulted by
  the coordinator after PK and local-index plans decline. Reads the entry
  range from the value's replica set at the statement's consistency
  (`Request::EntryRange`), LWW-merges per entry key, resolves row keys via
  the standard candidate quorum re-read + residual filter (orphan entries
  drop out there). Falls back to the scatter paths on any shortfall:
  entry-set quorum miss, a pre-v0.90 peer, or a hot value past
  `GIDX_PROBE_MAX` (10 000 candidates). Ranges/partial prefixes never
  route (hash placement) and keep the scatter paths.
- **Backfill**: the DDL-coordinating node drives it in the background —
  pages every member's shard (`ScanPage`), writes entries for rows the
  member primarily owns (exactly-once across members) through the normal
  routed write path at QUORUM, then broadcasts `GidxReady`; every node
  flips `building` off and starts routing probes. Single-node databases
  backfill inline before the DDL returns. A node down during the ready
  broadcast keeps `building` (routes no probes — safe, slow) until
  phase-3 flag convergence.
- EXPLAIN: access `global-index probe via '<name>' (routed …)`;
  cluster.fan_out `global-index probe routed to the value's replica set`.

Phase-1 notes:

- Syntax landed: `CREATE INDEX i ON t (cols) WITH (global = true)`;
  `IndexDef.global` (serde-default false). SHOW INDEXES reports kind
  `global`, local health `ok` (entries are replicated rows — no per-node
  state to be missing).
- Entry table name is `<db>␟__gidx__<bare>` — a plain `__gidx__` prefix
  rather than the `␟`-separated segment drafted below, because default-db
  names are unprefixed and a leading `␟` segment would parse as a database
  name. Hidden from SHOW TABLES and schema replay (the index DDL implies
  it; replay emits the `WITH (global = true)` clause).
- Coordinator companion writes ship at the row write's consistency, after
  the row write, before the ack. The old row is fetched with one quorum
  point read (only on tables that declared a global index); multi-row
  INSERTs on such tables take the per-row path. `DELETE` reuses the
  already-matched row — no extra read. Single-node (Session) writes
  maintain entries directly in the local entry table (the local shard IS
  the whole ring there); the replica APPLY path never touches entries.
- Unchanged entries produce no writes (old/new entry-key set difference,
  `global_entry_delta`).
- The planner **excludes** global indexes (no reader until phase 2), and
  backfill of pre-existing rows is also phase 2 — a phase-1 global index
  covers writes from creation onward.

## Problem

Secondary indexes are **local per node**: each node indexes only its own
shard. A non-PK indexed read therefore scatters `IndexScan` to every member,
unions candidate keys, and quorum re-reads them — every indexed equality
probe pays a full-cluster fan-out (`node.rs::index_candidate_keys`). On the
production 3-node/RF=3 cluster the fan-out is masked (every node has all
data), but on any RF < members deployment the cost scales with cluster size,
and even at RF=full each probe touches every member.

## Target

An index whose **entries are placed on the ring by indexed value**, so an
equality probe routes to the value's replica set only — one replica-set
round-trip, like a PK point read.

## Data model: the index is a table

A global index is an **internal row table** `__gidx␟<index-name>` whose rows
are:

```
key   = encode_key([indexed value(s)..., primary-key values...])
value = (empty)
```

The PK tail makes entries unique per row and lets a probe enumerate
candidate PKs by scanning the `[values...]` prefix range. Because it *is* a
table, it inherits every existing mechanism unchanged: ring placement +
replication + hints (`replicas_for` on the entry key), LWW via HLC,
anti-entropy repair (including the new digest gate), backup, resharding.
No new storage or repair machinery.

## Write path

On a row put/delete the coordinator computes old→new entry deltas (the old
version is already read on the apply path for idempotency — same cost
class) and issues companion writes to the index table:

- delete of old entry (tombstone), put of new entry — routed by the *entry*
  keys, i.e. potentially to different nodes than the row.
- **Consistency choice (recommended):** companion writes ship at the same
  consistency as the row write, *before* the ack. Cost: one extra
  replicated write per indexed column touched. Benefit: read-your-writes
  parity with today's local indexes.
- There is no cross-key atomicity (no distributed txn). A crash between row
  ack and entry write leaves a **missing or orphan entry**; both are
  self-healing:
  - orphan entry → the probe's candidate re-read finds the row absent or
    non-matching and drops it (the existing residual re-check);
  - missing entry → invisible to index reads until repair; bounded by the
    anti-entropy interval. Phase 3 adds an entry-table repair leg that
    regenerates entries from rows (the row table is the source of truth).

## Read path

Equality probe `WHERE col = v` on a global index:

1. `start/end = prefix range of encode_key([v])` in the entry table.
2. Route to `replicas_for(prefix)` — **the one replica set owning that
   value** (hash placement keeps a single value's entries on one set).
3. Read the entry range at the statement's consistency, extract PKs.
4. `resolve_candidates` (existing): quorum point-reads of the candidate
   rows, residual filter re-check.

**Scope limit:** value *ranges* (`col > v`) do not route under hash
placement — they stay on the local-index scatter path (or a future
order-preserving placement mode). v1 targets equality/`IN` probes — the
dedup and candidate-fetch shapes that dominate agencik's workload.

Multikey (`[]`) columns: one entry per array element (same expansion the
local multikey index does), equality-pinned probes only.

## DDL, backfill, compat

- Syntax: `CREATE INDEX i ON t (cols) WITH (global = true)` — local stays
  the default. Catalog: `IndexDef.global: bool` (serde-default false, so
  old catalogs deserialize).
- Backfill: scan the table paged (existing backfill pattern, `building`
  flag, SHOW INDEXES `local` column applies) writing entries through the
  normal replicated write path.
- Mixed versions: entries replicate as ordinary table rows, so old peers
  store them fine; only the *planner* use is version-gated. Feature-flag
  the read path until the fleet is upgraded.

## Phases

1. **Entry plumbing** — internal-table naming, entry key codec,
   write-path companion writes behind `global = true`, no reader yet.
   Tests: entry create/delete parity with row mutations, crash-window
   orphan tolerated by re-check.
2. **Read path** — prefix-range probe + `resolve_candidates` integration,
   planner picks a global index for equality probes, EXPLAIN says
   `global-index probe (routed)`. 3-node harness test: probe touches one
   replica set (assert via internode call counts).
3. **Hardening** — repair leg regenerating entries from rows (scan rows,
   diff against entry table, fix), orphan GC, delete-tombstone compaction
   interplay, backfill resumability.
4. **Prove it** — bench the 250k-row `slack_messages` `"user"` probe on the
   bench fleet at RF<members (where the win is real), then prod rollout
   behind the flag.

## Open questions (resolve during phase 1)

- Composite global indexes: probe requires the full value tuple (leftmost
  prefixes do not route under hashing) — accept, or hash only the first
  column and range-scan the rest within the set?
- Index-only answers (covered counts) — entries carry no row data; a count
  over entries double-counts unrepaired divergence. Defer; counts keep the
  candidate-resolve path.
- Entry-table TTL interplay if the base table has `WITH (ttl)` — entries
  must expire with their rows (stamp entries with the row HLC).
