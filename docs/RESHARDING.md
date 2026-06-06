# Online resharding — adding and removing nodes at runtime

skaidb places keys on a **consistent-hash ring** (vnodes; see SPEC §4 and
[ring.rs](../crates/skaidb-cluster/src/ring.rs)). The ring is held behind a lock
so membership can change while the cluster serves traffic: a node can **join** or
**leave** online and the keyspace rebalances without a restart or a full reload.

> Status: single-node **join** (`Node::add_member`), graceful **decommission**
> (`Node::remove_member`), post-move **space reclamation** (`Node::reclaim`),
> **versioned + persisted membership** (epoch'd, survives restart), and active
> **anti-entropy** — read-repair, [`Node::repair`], and hinted handoff — are all
> implemented and tested. See Limitations for what remains.

## Why a single join moves so little

Consistent hashing is the whole point: when one node joins, the only keys that
change owner are the ones whose hash now lands on the joiner's vnodes. Every
other key keeps its current placement. So a join moves roughly `1/(N+1)` of the
keyspace **onto** the new node and disturbs nothing else — no global reshuffle.
(Contrast hash-modulo-N sharding, where bumping `N` remaps almost every key.)

## Versioned, persisted membership

The ring carries a monotonically increasing **epoch**. Every `SetMembership`
broadcast carries the new epoch, and a node applies it **only if it is newer**
than the one it holds — so a stale or out-of-order broadcast can't move a node's
ring backward, and two concurrent topology changes can't both win (the higher
epoch supersedes; a losing orchestrator must retry at the next epoch). The
membership + epoch are **persisted** to a small `topology` file in the data
directory, so a node that restarts rejoins with the **live** ring (the one in
effect when it went down) instead of its original bootstrap config. See
`Node::membership_epoch` / `member_ids`.

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
   owns (recomputed against the new ring) **and that it is the elected sender
   for**, **pushes** it to the joiner with the row's original
   `(value | tombstone, HLC)` preserved. Tombstones migrate too, so a delete that
   hasn't compacted away still wins after the move. The push is **throttled and
   resumable**: rows are sent in batches (`set_migration_throttle(batch,
   pause_ms)` rate-limits so a large move doesn't saturate the cluster), and a
   per-joiner checkpoint records progress so an interrupted migration resumes
   where it left off instead of re-sending from the start (it's idempotent either
   way).

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

## Anti-entropy (keeping replicas converged)

Three mechanisms drive replicas toward agreement, so a write that reached only a
minority — or a replica that was briefly down — doesn't leave a permanent
divergence:

- **Read-repair.** A quorum point read compares the version each replica
  returned; the winning (highest-HLC) version is written back to any replica that
  answered with an older or missing one. Reads themselves heal the data.
- **Hinted handoff.** When a replicated write can't reach a replica (it's down),
  the coordinator buffers the write as a *hint* (per replica, bounded) and
  replays it once that replica is reachable again — opportunistically on the next
  write, or via `flush_hints`. Faster recovery than waiting for a full repair.
- **Active anti-entropy** (`Node::repair` / `repair_cluster`). A background pass
  reconciles each pair of co-replicas: they exchange per-key version stamps and
  copy the newer side in both directions (tombstones included). This is the
  durable backstop that converges replicas even with no reads and even if hints
  were lost. It's a full-table comparison today; a Merkle tree would let it skip
  identical key ranges instead of streaming the whole shard (future work).

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

- **Live migration (pending ranges).** A join runs as a two-phase transition:
  it first broadcasts the new ring with the **old ring unioned in**, so while
  data is migrating every coordinator treats a migrating key's owners as *both*
  its old and new node — writes dual-write to both and reads consult both — then
  finalizes to drop the old ring. So concurrent writes during a join stay
  consistent, not just on a quiescent cluster. (A node that *restarts* mid-
  transition reloads only the finalized ring, since the pending state isn't
  persisted; re-run `add_member` if a migration was interrupted. `remove_member`
  drains before changing the ring, so it needs no transition.)
- **Reclamation is a manual pass.** `reclaim`/`reclaim_cluster` are explicit
  calls run after a move, not automatic — until then the former owner keeps the
  (harmless) stale copies. It is also currently a full-table scan with a
  point-read ack-gate per key; fine after a single reshard, heavier on a very
  large dataset.
- **Migration materializes a table at a time.** The push is throttled, batched,
  and resumable, but each table's rows are still scanned into memory once per
  pass (then streamed out in batches). A bounded-memory disk cursor (lazy,
  range-limited SSTable iteration) would remove that last in-memory step — future
  work.
- **Membership has no gossip/consensus.** Data converges via anti-entropy, but
  *membership* changes (`SetMembership`/`Rebalance`/`Drain`) are still best-effort
  broadcasts. The epoch stops a node from regressing to an older ring and
  persistence reloads the live ring on restart, but a member unreachable for the
  *whole* change lags until re-broadcast to — there is no membership gossip it
  pulls on reconnect. Concurrent topology changes converge to the highest epoch,
  but the losing change is dropped (a membership coordinator or Raft-for-the-ring
  would make this linearizable). Bring a lagging node up to date by re-running
  `add_member`/`remove_member` (idempotent) once it is reachable.
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
- `membership_persists_across_restart_and_rejects_stale_epoch` joins a node, then
  restarts a peer from its data dir with the *stale* bootstrap config and asserts
  it reloads the live ring at the right epoch, and that a stale (lower-epoch)
  `SetMembership` is ignored.
- `rf2_join_migrates_via_single_sender` (rf=2) joins a third node and verifies
  every row is readable, exercising migration under replication.
- `read_repair_and_anti_entropy_converge_replicas` (rf=3) injects divergence on
  specific replicas and checks that a quorum read repairs the missing one, and
  that `repair` reconciles both push and pull directions.
- `hinted_handoff_replays_to_a_recovered_replica` (rf=3, CL=ALL) writes while a
  replica is down (buffering a hint), then recovers it and checks `flush_hints`
  hands the write off.
- `pending_ranges_dual_write_to_old_and_new_owner` (rf=1, CL=ALL) imposes a
  ring transition and checks a write to a migrating key lands on **both** its old
  and new owner.
- `throttled_migration_completes_and_clears_checkpoint` joins a node with a tiny
  batch size + pause and checks everything migrates and the resume checkpoint is
  removed; `migrate_checkpoint_roundtrips` covers the checkpoint codec.
