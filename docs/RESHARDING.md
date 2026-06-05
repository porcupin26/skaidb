# Online resharding — adding a node at runtime

skaidb places keys on a **consistent-hash ring** (vnodes; see SPEC §4 and
[ring.rs](../crates/skaidb-cluster/src/ring.rs)). The ring is held behind a lock
so membership can change while the cluster serves traffic: a new node can **join
online** and receive its share of the keyspace without a restart or a full
reload.

> Status: single-node **join** is implemented and tested (`Node::add_member`).
> Node **removal/decommission** and active **anti-entropy** (read-repair, hinted
> handoff, space reclamation on the former owner) are deferred — see Limitations.

## Why a single join moves so little

Consistent hashing is the whole point: when one node joins, the only keys that
change owner are the ones whose hash now lands on the joiner's vnodes. Every
other key keeps its current placement. So a join moves roughly `1/(N+1)` of the
keyspace **onto** the new node and disturbs nothing else — no global reshuffle.
(Contrast hash-modulo-N sharding, where bumping `N` remaps almost every key.)

## How a join works

`Node::add_member(id, addr)` can be driven from **any** existing member. It runs
three broadcast steps over the internode RPC
([internode.rs](../crates/skaidb-cluster/src/internode.rs)):

1. **Re-ring everyone.** The coordinator computes the new membership (current
   members + the joiner) and broadcasts `SetMembership { members }`. Each
   recipient — and the joiner — rebuilds the identical ring + peer table from
   the list. Placement is deterministic from the member set, so every node now
   agrees on who owns what.
2. **Bootstrap the joiner's schema.** The coordinator replays its catalog as
   `CREATE` DDL (`Database::schema_ddl()` — tables, then secondary indexes, then
   vector indexes) to the joiner via `ApplyDdl`, so the joiner can accept rows
   and build the same local + vector indexes over its shard.
3. **Migrate the keys.** The coordinator broadcasts `Rebalance { joiner }`. Each
   existing member scans every table and, for each row whose key the joiner now
   owns (recomputed against the new ring), **pushes** it to the joiner with the
   row's original `(value | tombstone, HLC)` preserved. Tombstones migrate too,
   so a delete that hasn't compacted away still wins after the move.

After step 3 the joiner holds (and indexes) every key the ring assigns it, and
reads route to it normally. Because pushed rows keep their **original HLC**, any
write that happened before the move stays correctly ordered under
last-writer-wins.

```rust
// On a running cluster member:
node.add_member("c", "10.0.0.3:7100")?;   // c joins; its share is migrated to it
```

## Correctness model

- **LWW is preserved.** Migrated rows carry their original HLC, so they neither
  shadow nor are shadowed by concurrent writes incorrectly — the newest stamp
  wins as always.
- **Stale copies are harmless.** The former owner keeps its physical copy (see
  Limitations); but with rf=1 a point read routes only to the new owner via the
  ring, and with rf>1 a cluster/index read merges by HLC, so the migrated copy
  (equal or newer) wins or ties. No read returns a lost or resurrected row.
- **Idempotent.** `Rebalance` can be re-sent; re-pushing a row at the same HLC is
  a no-op under LWW. A join that half-completes can be retried.

## Assumptions & limitations

- **Quiescent migration.** The join assumes no concurrent writes to the specific
  keys being migrated. A write that races the push can be ordered correctly by
  HLC, but the design target is "add capacity during a calm window," not a
  guaranteed-consistent live cutover under peak write load.
- **No space reclamation yet.** The former owner does **not** delete keys it
  handed off. Reclaiming that space needs a *local physical delete that bypasses
  LWW* (a normal tombstone would carry a newer HLC and could mask the migrated
  copy elsewhere). That belongs with active anti-entropy, which isn't built yet —
  so a long-lived cluster that reshards repeatedly will accumulate stale copies
  until compaction + a future GC pass removes them.
- **No catch-up log.** `SetMembership`/`Rebalance` are best-effort broadcasts.
  A member that is unreachable during the join keeps the old ring until it is
  re-broadcast to; there is no schema/topology log it replays on reconnect yet.
  Bring such a node back by re-running `add_member` (idempotent) once it is up.
- **Join only.** Graceful **decommission** (drain a node's keys to its successors
  before removing it) reuses the same machinery but is not wired up yet.
- **rf > 1.** Every replica that holds a migrating key independently pushes it,
  which is correct (idempotent) but does redundant work; a future version can
  elect one sender per key.

## Tested

`online_resharding_migrates_keys_to_a_joining_node` (rf=1, CL=ONE) fills a table
on a two-node cluster, joins a third node online, and asserts every row is still
readable from every coordinator (so the rows the new node now owns were migrated
to it), the secondary index bootstrapped onto the joiner serves a distributed
lookup, and a write after the join routes under the new ring.
