# Online resharding — adding and removing nodes at runtime

skaidb places keys on a **consistent-hash ring** (vnodes; see SPEC §4 and
[ring.rs](../crates/skaidb-cluster/src/ring.rs)). The ring is held behind a lock
so membership can change while the cluster serves traffic: a node can **join** or
**leave** online and the keyspace rebalances without a restart or a full reload.

> Status: single-node **join** (`Node::add_member`), graceful **decommission**
> (`Node::remove_member`), and post-move **space reclamation** (`Node::reclaim`)
> are implemented and tested. Active **anti-entropy** (read-repair, hinted
> handoff) is deferred — see Limitations.

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

## How a graceful leave works

`Node::remove_member(id)` is the inverse of a join and can be driven from any
member — including the leaving node itself (self-decommission):

1. **Drain.** The orchestrator sends the leaving node a `Drain` carrying the
   *post-removal* membership. The leaving node walks every local row and, for
   each key, computes its owners under the smaller ring; for any owner that is
   not already a replica (i.e. a node that must pick up this key now), it pushes
   the row — HLC and tombstone preserved. So every key keeps its full replica set
   before the node disappears.
2. **Shrink the ring.** The orchestrator broadcasts `SetMembership` with the node
   removed, so the survivors stop routing to it.

After this the leaving node still has its (now unowned) data on disk but serves
no key, so it is safe to shut down. Because consistent hashing only reassigns the
departing node's keys, a single leave moves about `1/N` of the keyspace and
touches nothing else.

```rust
node.remove_member("c")?;   // c drains its keys to their new owners, then leaves
```

## Reclaiming space after a move

A join or leave leaves the *former* owner holding copies of keys it no longer
owns. `Node::reclaim` frees that space: it walks each local key, and for any key
this node is no longer a replica of, it **physically purges** the key — but only
after an actual current owner confirms it holds that key at a version at least as
new (the *ack-gate*, so a key whose migration never completed is never dropped
from its last copy). `reclaim_cluster` fans the cleanup out to every node.

The purge is a real physical drop ([`Engine::retain`](../crates/skaidb-storage/src/engine.rs)),
**not a tombstone**: it rewrites the table keeping only retained keys and discards
the rest, so dropped keys vanish from every scan and never resurrect via
compaction. That matters because a tombstone would carry a newer HLC than the
migrated copy and could (a) re-enter a later migration and delete the key on its
new owner, or (b) win an LWW merge and mask the live value elsewhere — a physical
purge does neither. Secondary-index entries for purged rows are left dangling;
reads already skip an index entry whose row is absent, and they compact away.

```rust
node.reclaim()?;          // this node drops keys it no longer owns
node.reclaim_cluster()?;  // …and tells every peer to do the same
```

## Correctness model

- **LWW is preserved.** Migrated rows carry their original HLC, so they neither
  shadow nor are shadowed by concurrent writes incorrectly — the newest stamp
  wins as always.
- **Stale copies are harmless, then reclaimed.** Until reclamation runs the
  former owner keeps its physical copy, but with rf=1 a point read routes only to
  the new owner via the ring, and with rf>1 a cluster/index read merges by HLC,
  so the migrated copy (equal or newer) wins or ties — no read returns a lost or
  resurrected row. `Node::reclaim` (below) then frees the space.
- **Idempotent.** `Rebalance` can be re-sent; re-pushing a row at the same HLC is
  a no-op under LWW. A join that half-completes can be retried.

## Assumptions & limitations

- **Quiescent migration.** The join assumes no concurrent writes to the specific
  keys being migrated. A write that races the push can be ordered correctly by
  HLC, but the design target is "add capacity during a calm window," not a
  guaranteed-consistent live cutover under peak write load.
- **Reclamation is a manual pass.** `reclaim`/`reclaim_cluster` are explicit
  calls run after a move, not automatic — until then the former owner keeps the
  (harmless) stale copies. It is also currently a full-table scan with a
  point-read ack-gate per key; fine after a single reshard, heavier on a very
  large dataset.
- **No catch-up log.** `SetMembership`/`Rebalance`/`Drain` are best-effort
  broadcasts. A member that is unreachable during the change keeps the old ring
  until it is re-broadcast to; there is no schema/topology log it replays on
  reconnect yet. Bring such a node back by re-running `add_member` (idempotent)
  once it is up.
- **Single sender per key.** Only the key's primary under the pre-join ring
  pushes it during a join, so `rf > 1` no longer re-sends each key from every
  replica. The trade-off: if that one sender is unreachable mid-join the key
  isn't migrated until the join is retried (the old all-replicas-push behavior
  was more redundant but more resilient).

## Tested

- `online_resharding_migrates_keys_to_a_joining_node` (rf=1, CL=ONE) fills a
  table on a two-node cluster, joins a third node online, and asserts every row
  is still readable from every coordinator (so the rows the new node now owns
  were migrated to it), the secondary index bootstrapped onto the joiner serves a
  distributed lookup, and a write after the join routes under the new ring.
- `graceful_decommission_drains_keys_before_leaving` (rf=1, CL=ONE) removes a
  node from a three-node cluster and asserts every key it owned was drained to
  its new owner (all rows stay readable from the survivors) and that writes after
  the leave route under the smaller ring.
- `reclaim_drops_unowned_keys_after_join` (rf=1) joins a node, then has the
  former owners `reclaim`; it asserts space is actually freed (rows dropped > 0),
  no row is lost, and a second pass is a no-op (idempotent). The storage-level
  `retain_physically_drops_keys_without_resurrection` checks the purge leaves no
  tombstone and survives reopen.
