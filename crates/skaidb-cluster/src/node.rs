//! A cluster member node: local storage plus the coordinator that replicates
//! writes and gathers distributed reads (SPEC §4–6).
//!
//! Writes go to a key's replica set (the ring) and wait for a write quorum;
//! reads scatter to members and merge by HLC last-writer-wins. DDL is broadcast
//! to a member quorum. The same `run()` executor the embedded engine uses drives
//! everything — only the [`Cluster`] impl changes. (Active read-repair and
//! hinted handoff are noted for a later phase; convergence currently relies on
//! every write reaching its replica quorum.)
//!
//! Consistency note: per-key write quorums are enforced exactly; a table read
//! gathers from all reachable members (strongest read), and requires at least a
//! cluster quorum of members to respond.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use std::thread;

use skaidb_engine::{
    filter_rows, namespace, run, Cluster, Database, DbStats, EngineError, IndexScanRange,
    QueryOutput, SessionEffect, DEFAULT_DATABASE,
};
use skaidb_proto::{read_frame, write_frame};
use skaidb_sql::ast::{BinaryOp, Expr, Statement};
use skaidb_sql::parse;
use skaidb_storage::{Hlc, HlcClock, WalCommit, WalSync};
use skaidb_types::{Document, Value};

use crate::internode::{self, Request, Response};
use crate::quorum::Consistency;
use crate::ring::{NodeId, Ring};

type EngineResult<T> = std::result::Result<T, EngineError>;

/// Configuration for a node's place in the cluster.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: NodeId,
    /// Address this node serves internode RPC on.
    pub internode_addr: String,
    /// All members: id → internode address (including this node).
    pub members: Vec<(NodeId, String)>,
    pub replication_factor: usize,
    pub vnodes_per_node: u32,
    pub read_consistency: Consistency,
    pub write_consistency: Consistency,
}

/// The cluster's placement view: the hash ring plus peer addresses. Held behind
/// a lock so membership can change at runtime (resharding).
#[derive(Debug)]
struct Topology {
    /// Membership version: a higher epoch supersedes a lower one, so stale or
    /// concurrent topology updates can't move the ring backward.
    epoch: u64,
    ring: Ring,
    /// During a membership change, the ring as it was *before* the change. While
    /// set, a key's replica set is the **union** of its owners on both rings, so
    /// writes dual-write and reads consult both the old and new owner — keeping
    /// resharding correct under concurrent writes. Cleared when the change is
    /// finalized.
    prev: Option<Ring>,
    /// Peer id → internode address (excludes self).
    peers: HashMap<NodeId, String>,
    /// Full membership (including self) — kept so it can be persisted/rebroadcast.
    members: Vec<(NodeId, String)>,
}

impl Topology {
    /// Build a topology at `epoch` from the full member list, excluding `self_id`
    /// from peers. `prev_members` (when non-empty) marks an in-progress change:
    /// the ring they describe is unioned in for placement until finalized.
    fn build(
        members: &[(NodeId, String)],
        prev_members: &[(NodeId, String)],
        self_id: &NodeId,
        vnodes: u32,
        epoch: u64,
    ) -> Topology {
        let mut ring = Ring::new(vnodes);
        for (id, _) in members {
            ring.add_node(id.clone());
        }
        let prev = if prev_members.is_empty() {
            None
        } else {
            let mut r = Ring::new(vnodes);
            for (id, _) in prev_members {
                r.add_node(id.clone());
            }
            Some(r)
        };
        let peers = members
            .iter()
            .filter(|(id, _)| id != self_id)
            .map(|(id, addr)| (id.clone(), addr.clone()))
            .collect();
        Topology {
            epoch,
            ring,
            prev,
            peers,
            members: members.to_vec(),
        }
    }

    /// Build a settled topology (no in-progress change).
    fn from_members(
        members: &[(NodeId, String)],
        self_id: &NodeId,
        vnodes: u32,
        epoch: u64,
    ) -> Topology {
        Topology::build(members, &[], self_id, vnodes, epoch)
    }
}

/// A cluster member: owns local storage and coordinates cluster operations.
#[derive(Debug)]
pub struct Node {
    id: NodeId,
    /// Local storage behind an `RwLock` so concurrent reads share a read lock
    /// while writes are exclusive.
    local: RwLock<Database>,
    /// Cluster placement, mutable so nodes can join/leave at runtime.
    topo: RwLock<Topology>,
    clock: HlcClock,
    /// Pooled persistent connections to peers.
    pool: internode::Pool,
    /// Writes that couldn't reach a replica (it was down), buffered per replica
    /// for replay when it comes back — *hinted handoff*. In-memory and bounded;
    /// anti-entropy ([`Node::repair`]) is the durable backstop if hints are lost.
    hints: Mutex<HashMap<NodeId, Vec<HintedWrite>>>,
    /// Rows pushed per migration batch before a checkpoint + throttle pause.
    migration_batch: AtomicUsize,
    /// Pause (ms) between migration batches — throttles a reshard so it doesn't
    /// saturate the cluster. `0` = no throttle.
    migration_pause_ms: AtomicU64,
    /// Cumulative coordinator/replication counters, surfaced as metrics.
    counters: Counters,
    cfg: NodeConfig,
}

/// Cumulative cluster counters (correctness-critical replication/anti-entropy
/// activity that is otherwise invisible). All monotonic; read via [`Node::stats`].
#[derive(Debug, Default)]
struct Counters {
    writes_total: AtomicU64,
    write_quorum_failures: AtomicU64,
    reads_total: AtomicU64,
    read_quorum_failures: AtomicU64,
    read_repairs: AtomicU64,
    hints_stored: AtomicU64,
    hints_replayed: AtomicU64,
    peer_requests: AtomicU64,
    peer_errors: AtomicU64,
}

/// A point-in-time snapshot of cluster state and counters for metrics/`/status`.
#[derive(Debug, Clone)]
pub struct ClusterStats {
    pub node_id: String,
    pub epoch: u64,
    pub members: usize,
    pub replication_factor: usize,
    /// True while a join/decommission is in flight (dual-write window open).
    pub resharding_active: bool,
    /// Hinted writes currently buffered for unreachable replicas.
    pub hints_pending: usize,
    pub write_consistency: &'static str,
    pub read_consistency: &'static str,
    pub writes_total: u64,
    pub write_quorum_failures: u64,
    pub reads_total: u64,
    pub read_quorum_failures: u64,
    pub read_repairs: u64,
    pub hints_stored: u64,
    pub hints_replayed: u64,
    pub peer_requests: u64,
    pub peer_errors: u64,
}

/// A buffered write awaiting handoff to a recovered replica:
/// `(table, key, op, hlc)`.
type HintedWrite = (String, Vec<u8>, WriteOp, Hlc);

/// Cap on buffered hints per replica, so a long outage can't grow unboundedly
/// (anti-entropy reconciles whatever overflows).
const MAX_HINTS_PER_REPLICA: usize = 4096;

impl Node {
    /// Create a node with the given local database and cluster config. If a
    /// persisted membership from a prior run exists (a node that joined/left
    /// while this one was up), it is loaded so the node rejoins with the **live**
    /// ring rather than its bootstrap `cfg.members`.
    pub fn new(local: Database, cfg: NodeConfig) -> Arc<Node> {
        let path = membership_path(local.dir());
        let (members, epoch) = match load_membership(&path) {
            Some((members, epoch)) => (members, epoch),
            None => (cfg.members.clone(), 0),
        };
        let topo = Topology::from_members(&members, &cfg.id, cfg.vnodes_per_node, epoch);
        Arc::new(Node {
            id: cfg.id.clone(),
            local: RwLock::new(local),
            topo: RwLock::new(topo),
            clock: HlcClock::new(),
            pool: internode::Pool::new(),
            hints: Mutex::new(HashMap::new()),
            migration_batch: AtomicUsize::new(1024),
            migration_pause_ms: AtomicU64::new(0),
            counters: Counters::default(),
            cfg,
        })
    }

    /// Tune migration throttling: push at most `batch` rows between checkpoints,
    /// pausing `pause_ms` between batches (0 = no throttle). Lets a reshard of a
    /// large shard proceed without saturating the cluster.
    pub fn set_migration_throttle(&self, batch: usize, pause_ms: u64) {
        self.migration_batch.store(batch.max(1), Ordering::Relaxed);
        self.migration_pause_ms.store(pause_ms, Ordering::Relaxed);
    }

    /// The current membership epoch.
    fn current_epoch(&self) -> u64 {
        self.topo.read().expect("topo lock").epoch
    }

    /// The current membership version (for diagnostics).
    pub fn membership_epoch(&self) -> u64 {
        self.current_epoch()
    }

    /// This node's id (its `host:internode_port` on the ring).
    pub fn id(&self) -> String {
        self.id.0.clone()
    }

    /// The configured replication factor.
    pub fn replication_factor(&self) -> usize {
        self.cfg.replication_factor
    }

    /// A snapshot of cluster state and replication counters for metrics and the
    /// read-only `/status` endpoint.
    pub fn stats(&self) -> ClusterStats {
        let c = &self.counters;
        let (epoch, members, resharding) = {
            let topo = self.topo.read().expect("topo lock");
            (topo.epoch, topo.peers.len() + 1, topo.prev.is_some())
        };
        let hints_pending = self
            .hints
            .lock()
            .expect("hints lock")
            .values()
            .map(|v| v.len())
            .sum();
        ClusterStats {
            node_id: self.id.0.clone(),
            epoch,
            members,
            replication_factor: self.cfg.replication_factor,
            resharding_active: resharding,
            hints_pending,
            write_consistency: consistency_label(self.cfg.write_consistency),
            read_consistency: consistency_label(self.cfg.read_consistency),
            writes_total: c.writes_total.load(Ordering::Relaxed),
            write_quorum_failures: c.write_quorum_failures.load(Ordering::Relaxed),
            reads_total: c.reads_total.load(Ordering::Relaxed),
            read_quorum_failures: c.read_quorum_failures.load(Ordering::Relaxed),
            read_repairs: c.read_repairs.load(Ordering::Relaxed),
            hints_stored: c.hints_stored.load(Ordering::Relaxed),
            hints_replayed: c.hints_replayed.load(Ordering::Relaxed),
            peer_requests: c.peer_requests.load(Ordering::Relaxed),
            peer_errors: c.peer_errors.load(Ordering::Relaxed),
        }
    }

    /// Storage/runtime statistics for this node's local engine (for metrics).
    pub fn db_stats(&self, per_table: bool) -> Option<DbStats> {
        self.local.read().ok().map(|db| db.stats(per_table))
    }

    /// The current membership as node ids (for diagnostics).
    pub fn member_ids(&self) -> Vec<String> {
        self.topo
            .read()
            .expect("topo lock")
            .members
            .iter()
            .map(|(id, _)| id.0.clone())
            .collect()
    }

    /// Persist the current membership + epoch so it survives a restart.
    fn persist_membership(&self) {
        let topo = self.topo.read().expect("topo lock");
        let path = {
            let db = self.local.read().expect("local lock");
            membership_path(db.dir())
        };
        save_membership(&path, topo.epoch, &topo.members);
    }

    /// Total member count (peers + self).
    fn member_count(&self) -> usize {
        self.topo.read().expect("topo lock").peers.len() + 1
    }

    /// Replica set for `key` at the configured replication factor (snapshot).
    /// During a membership change this is the **union** of the key's owners on
    /// the new and previous rings, so a migrating key is written to (and read
    /// from) both its old and new owner until the change is finalized.
    fn replicas_for(&self, key: &[u8]) -> Vec<NodeId> {
        let rf = self.cfg.replication_factor;
        let topo = self.topo.read().expect("topo lock");
        let mut reps = topo.ring.replicas_for(key, rf);
        if let Some(prev) = &topo.prev {
            for n in prev.replicas_for(key, rf) {
                if !reps.contains(&n) {
                    reps.push(n);
                }
            }
        }
        reps
    }

    /// Address of peer `id`, if it is a current peer (snapshot, cloned).
    fn peer_addr(&self, id: &NodeId) -> Option<String> {
        self.topo.read().expect("topo lock").peers.get(id).cloned()
    }

    /// All current peers as `(id, addr)` pairs (snapshot, cloned).
    fn peers_with_ids(&self) -> Vec<(NodeId, String)> {
        self.topo
            .read()
            .expect("topo lock")
            .peers
            .iter()
            .map(|(id, addr)| (id.clone(), addr.clone()))
            .collect()
    }

    /// Addresses of all current peers (snapshot, cloned) — never held across I/O.
    fn peer_addrs(&self) -> Vec<String> {
        self.topo
            .read()
            .expect("topo lock")
            .peers
            .values()
            .cloned()
            .collect()
    }

    /// The full current membership (`(id, addr)` pairs), including this node.
    fn members_snapshot(&self) -> Vec<(NodeId, String)> {
        self.topo.read().expect("topo lock").members.clone()
    }

    /// Adopt `members` at version `epoch`, but only if `epoch` is newer than the
    /// one currently held (so a stale or concurrent update can't move the ring
    /// backward). A non-empty `prev_members` marks an in-progress change whose old
    /// ring is unioned in for placement (dual-write/read) until a later
    /// finalizing update clears it. Persists on success. Returns whether applied.
    fn set_membership(
        &self,
        members: &[(NodeId, String)],
        prev_members: &[(NodeId, String)],
        epoch: u64,
    ) -> bool {
        {
            let mut topo = self.topo.write().expect("topo lock");
            if epoch <= topo.epoch && topo.epoch != 0 {
                return false; // stale / superseded
            }
            *topo = Topology::build(
                members,
                prev_members,
                &self.id,
                self.cfg.vnodes_per_node,
                epoch,
            );
        }
        self.persist_membership();
        true
    }

    /// `(database, CREATE-sql)` pairs reconstructing the local schema — databases,
    /// tables, and indexes — for joiner bootstrap.
    fn schema_ddl(&self) -> EngineResult<Vec<(String, String)>> {
        Ok(self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .schema_ddl())
    }

    /// The hash ring as it was **before** `exclude` joined — the current
    /// membership minus that node. Used to elect a single migration sender per
    /// key (the key's primary under the pre-join ring).
    fn ring_excluding(&self, exclude: &NodeId) -> Ring {
        let mut ring = Ring::new(self.cfg.vnodes_per_node);
        for (id, _) in self.members_snapshot() {
            if &id != exclude {
                ring.add_node(id);
            }
        }
        ring
    }

    /// Push the rows `joiner` now owns to it, preserving each row's HLC and
    /// tombstone state. To avoid every replica re-sending the same key, **only
    /// the key's primary under the pre-join ring** sends it — a single,
    /// deterministic sender per key (it is also a current holder, since it was a
    /// replica before the join). Rows are snapshotted under a read lock per
    /// table, then sent without the lock held. Stale copies are left in place on
    /// the former owner until [`Node::reclaim`] purges them. Idempotent.
    fn rebalance_to(&self, joiner: &NodeId) -> EngineResult<()> {
        if joiner == &self.id {
            return Ok(());
        }
        let addr = self
            .peer_addr(joiner)
            .ok_or_else(|| EngineError::Cluster(format!("joiner {joiner} not in topology")))?;
        let old_ring = self.ring_excluding(joiner);
        let batch = self.migration_batch.load(Ordering::Relaxed).max(1);
        let pause = self.migration_pause_ms.load(Ordering::Relaxed);

        // Resume from a checkpoint left by an interrupted migration, if any.
        let dir = {
            let db = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
            db.dir().to_path_buf()
        };
        let ckpt = migrate_ckpt_path(&dir, joiner);
        let resume = load_migrate_ckpt(&ckpt);

        // Tables in deterministic (sorted) order, so the checkpoint advances
        // monotonically and a resume can skip whole tables already migrated.
        let mut tables = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .table_names();
        tables.sort();

        for table in tables {
            // Resume handling: a table sorting before the checkpoint's table is
            // already done; within the checkpoint's table, skip keys <= last sent.
            let skip_until: Option<Vec<u8>> = match &resume {
                Some((ct, _)) if &table < ct => continue,
                Some((ct, lk)) if ct == &table => Some(lk.clone()),
                _ => None,
            };

            let rows = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .local_scan_versioned_with_tombstones(&table)?;
            // Keys this node should send for the joiner, in key order.
            let pending: Vec<(Vec<u8>, Vec<u8>, Hlc, bool)> = rows
                .into_iter()
                .filter(|(key, _, _, _)| {
                    skip_until.as_ref().is_none_or(|lk| key > lk)
                        && self.replicas_for(key).contains(joiner)
                        && old_ring.primary_for(key) == Some(self.id.clone())
                })
                .collect();

            // Stream in throttled batches, checkpointing after each.
            for chunk in pending.chunks(batch) {
                for (key, value, hlc, is_put) in chunk {
                    let op = if *is_put {
                        WriteOp::Put(value.clone())
                    } else {
                        WriteOp::Delete
                    };
                    match self.send_write(&addr, &table, key, &op, *hlc) {
                        Ok(true) => {}
                        _ => {
                            return Err(EngineError::Cluster(format!(
                                "rebalance to {joiner}: write not acked"
                            )))
                        }
                    }
                }
                if let Some((last_key, _, _, _)) = chunk.last() {
                    save_migrate_ckpt(&ckpt, &table, last_key);
                }
                if pause > 0 {
                    thread::sleep(Duration::from_millis(pause));
                }
            }
        }
        // Done — clear the checkpoint.
        let _ = std::fs::remove_file(&ckpt);
        Ok(())
    }

    /// Drain this (leaving) node: push every locally-held row to the owners it
    /// will have under `new_members` (the post-removal ring, excluding this
    /// node), so every key keeps its full replica set after the node departs.
    /// Only rows destined for a *new* owner — one that isn't already a replica
    /// under the current ring — are sent; existing replicas already hold them.
    /// HLC/tombstone state is preserved. Idempotent and safe to retry.
    fn drain_to(&self, new_members: &[(NodeId, String)]) -> EngineResult<()> {
        let mut new_ring = Ring::new(self.cfg.vnodes_per_node);
        let mut addr_of: HashMap<NodeId, String> = HashMap::new();
        for (id, addr) in new_members {
            new_ring.add_node(id.clone());
            addr_of.insert(id.clone(), addr.clone());
        }
        let rf = self.cfg.replication_factor;

        let tables = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .table_names();
        for table in tables {
            let rows = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .local_scan_versioned_with_tombstones(&table)?;
            for (key, value, hlc, is_put) in rows {
                let old = self.replicas_for(&key); // current ring (includes self)
                let op = if is_put {
                    WriteOp::Put(value)
                } else {
                    WriteOp::Delete
                };
                for replica in new_ring.replicas_for(&key, rf) {
                    if old.contains(&replica) {
                        continue; // that node already holds this row
                    }
                    let Some(addr) = addr_of.get(&replica) else {
                        continue;
                    };
                    match self.send_write(addr, &table, &key, &op, hlc) {
                        Ok(true) => {}
                        _ => {
                            return Err(EngineError::Cluster(format!(
                                "drain: write to {replica} not acked"
                            )))
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Add a node to the cluster at runtime and migrate it its share of the
    /// keyspace (online resharding). Orchestrated from any existing member:
    ///
    /// 1. compute the new membership (current members + the joiner) and broadcast
    ///    [`Request::SetMembership`] so every node (and the joiner) recomputes the
    ///    same ring;
    /// 2. bootstrap the joiner's schema with the local catalog's `CREATE` DDL;
    /// 3. broadcast [`Request::Rebalance`] so every existing member pushes the
    ///    keys the joiner now owns.
    ///
    /// Consistent hashing means a single join only moves keys *onto* the joiner,
    /// so existing placements are otherwise undisturbed. The join runs as a
    /// two-phase **pending-ranges** transition: while migrating, every node treats
    /// the migrating keys' owners as the **union** of the old and new rings, so
    /// concurrent writes dual-write to both and reads find not-yet-migrated data
    /// on the old owner — correct even under live writes, not just a quiescent
    /// cluster. The joiner and a member quorum must be reachable; a member that
    /// missed the broadcast needs it re-sent (see [docs/RESHARDING.md]).
    pub fn add_member(&self, id: &str, addr: &str) -> EngineResult<()> {
        let joiner = NodeId::new(id);
        let old_members = self.members_snapshot();
        if old_members.iter().any(|(mid, _)| *mid == joiner) {
            return Ok(()); // already a member
        }
        let mut new_members = old_members.clone();
        new_members.push((joiner.clone(), addr.to_string()));
        let new_wire = wire_of(&new_members);
        let old_wire = wire_of(&old_members);

        // 1) Begin the transition: adopt the new ring with the old ring unioned
        //    in (dual-write/read), so writes during migration reach both owners.
        let epoch_begin = self.current_epoch() + 1;
        self.set_membership(&new_members, &old_members, epoch_begin);
        for (mid, maddr) in &new_members {
            if *mid == self.id {
                continue;
            }
            match self.pool.call(
                maddr,
                &Request::SetMembership {
                    epoch: epoch_begin,
                    members: new_wire.clone(),
                    prev_members: old_wire.clone(),
                },
            ) {
                Ok(Response::Ack) => {}
                _ if *mid == joiner => {
                    return Err(EngineError::Cluster("joiner unreachable".into()))
                }
                _ => {} // existing member lagging: best-effort (no catch-up log yet)
            }
        }

        // 2) Bootstrap the joiner's schema (databases + tables + indexes) so it
        //    can accept migrated rows.
        for (db, ddl) in self.schema_ddl()? {
            match self.pool.call(addr, &Request::ApplyDdl { db, sql: ddl }) {
                Ok(Response::Ack) => {}
                Ok(Response::Err(e)) => {
                    return Err(EngineError::Cluster(format!("joiner DDL failed: {e}")))
                }
                _ => return Err(EngineError::Cluster("joiner unreachable during bootstrap".into())),
            }
        }

        // 3) Every existing member pushes the keys the joiner now owns.
        self.rebalance_to(&joiner)?;
        for (mid, maddr) in &new_members {
            if *mid == self.id || *mid == joiner {
                continue;
            }
            match self.pool.call(maddr, &Request::Rebalance { joiner: id.to_string() }) {
                Ok(Response::Ack) | Ok(Response::Err(_)) => {}
                _ => {} // unreachable member: its keys migrate when it rejoins
            }
        }

        // 4) Finalize: drop the old ring so placement is the joiner-inclusive ring
        //    only. (Left set on a failed migration above, keeping dual-read safe.)
        let epoch_final = self.current_epoch() + 1;
        self.set_membership(&new_members, &[], epoch_final);
        for (mid, maddr) in &new_members {
            if *mid == self.id {
                continue;
            }
            let _ = self.pool.call(
                maddr,
                &Request::SetMembership {
                    epoch: epoch_final,
                    members: new_wire.clone(),
                    prev_members: Vec::new(),
                },
            );
        }
        Ok(())
    }

    /// Gracefully remove a node from the cluster at runtime (the inverse of
    /// [`Node::add_member`]). Orchestrated from any member — including the
    /// leaving node itself (self-decommission):
    ///
    /// 1. ask the leaving node to [`Request::Drain`] — push each of its keys to
    ///    the owners it will have once it is gone (the post-removal ring), so no
    ///    key loses a replica;
    /// 2. broadcast [`Request::SetMembership`] with the node removed so the
    ///    survivors recompute the smaller ring and stop routing to it.
    ///
    /// The leaving node keeps its now-unowned data on disk (reclaiming it is the
    /// separate space-reclamation step) but no longer serves any key, so it is
    /// safe to shut down. Draining requires the leaving node and the affected new
    /// owners to be reachable, and assumes a quiescent cluster — as `add_member`
    /// does. See [docs/RESHARDING.md].
    pub fn remove_member(&self, id: &str) -> EngineResult<()> {
        let leaving = NodeId::new(id);
        let members = self.members_snapshot();
        if !members.iter().any(|(m, _)| *m == leaving) {
            return Ok(()); // not a member
        }
        if members.len() <= 1 {
            return Err(EngineError::Cluster(
                "cannot remove the last node in the cluster".into(),
            ));
        }
        let new_members: Vec<(NodeId, String)> =
            members.into_iter().filter(|(m, _)| *m != leaving).collect();
        let wire: Vec<(String, String)> = new_members
            .iter()
            .map(|(id, a)| (id.0.clone(), a.clone()))
            .collect();

        // 1) Drain the leaving node's keys to their new owners.
        if leaving == self.id {
            self.drain_to(&new_members)?;
        } else {
            let addr = self.peer_addr(&leaving).ok_or_else(|| {
                EngineError::Cluster(format!("leaving node {leaving} not in topology"))
            })?;
            match self.pool.call(&addr, &Request::Drain { members: wire.clone() }) {
                Ok(Response::Ack) => {}
                Ok(Response::Err(e)) => {
                    return Err(EngineError::Cluster(format!("drain failed: {e}")))
                }
                _ => {
                    return Err(EngineError::Cluster(
                        "leaving node unreachable; cannot drain its keyspace safely".into(),
                    ))
                }
            }
        }

        // 2) Survivors adopt the smaller ring at a new epoch; the leaving node is
        //    dropped from it. (Drain already moved the data, so no transition is
        //    needed — the leaving node served reads up to this point.)
        let epoch = self.current_epoch() + 1;
        if leaving != self.id {
            self.set_membership(&new_members, &[], epoch);
        }
        for (mid, maddr) in &new_members {
            if *mid == self.id {
                continue;
            }
            // Best-effort: a lagging survivor catches up when re-broadcast to
            // (no membership catch-up log yet).
            let _ = self.pool.call(
                maddr,
                &Request::SetMembership {
                    epoch,
                    members: wire.clone(),
                    prev_members: Vec::new(),
                },
            );
        }
        Ok(())
    }

    /// Reclaim local disk space after resharding: physically drop every
    /// locally-held key this node **no longer owns** under the current ring, but
    /// only once an actual owner is confirmed to hold that key at a version at
    /// least as new (the *ack-gate*, so a key whose migration never completed is
    /// never dropped from its last copy). The drop is a physical purge — no
    /// tombstone — so it neither re-enters migration nor poisons an LWW merge.
    /// Returns the number of rows dropped. Idempotent.
    pub fn reclaim(&self) -> EngineResult<usize> {
        let tables = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .table_names();

        let mut total = 0;
        for table in tables {
            let rows = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .local_scan_versioned_with_tombstones(&table)?;

            // Collect unowned keys whose owners confirm a copy at >= our version.
            let mut confirmed: HashSet<Vec<u8>> = HashSet::new();
            for (key, _value, hlc, _is_put) in rows {
                let owners = self.replicas_for(&key);
                if owners.contains(&self.id) {
                    continue; // we still own it
                }
                if self.owners_hold(&table, &key, hlc, &owners) {
                    confirmed.insert(key);
                }
            }
            if confirmed.is_empty() {
                continue;
            }
            let dropped = self
                .local
                .write()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .retain_rows(&table, |k| !confirmed.contains(k))?;
            total += dropped;
        }
        Ok(total)
    }

    /// Whether at least one current owner of `key` (other than self) holds a
    /// version stamped at or after `our_hlc` — the ack-gate for [`Node::reclaim`].
    fn owners_hold(&self, table: &str, key: &[u8], our_hlc: Hlc, owners: &[NodeId]) -> bool {
        for owner in owners {
            if *owner == self.id {
                continue;
            }
            let Some(addr) = self.peer_addr(owner) else {
                continue;
            };
            if let Ok(Response::Get {
                entry: Some((_, hlc, _)),
            }) = self.pool.call(
                &addr,
                &Request::LocalGet {
                    table: table.to_string(),
                    key: key.to_vec(),
                },
            ) {
                if hlc >= our_hlc {
                    return true;
                }
            }
        }
        false
    }

    /// Trigger [`Node::reclaim`] on this node and every peer (a cluster-wide
    /// post-resharding cleanup). Returns the number of rows this node dropped;
    /// peers reclaim best-effort (their counts are not returned over the wire).
    pub fn reclaim_cluster(&self) -> EngineResult<usize> {
        let local = self.reclaim()?;
        for addr in &self.peer_addrs() {
            let _ = self.pool.call(addr, &Request::Reclaim);
        }
        Ok(local)
    }

    /// Active **anti-entropy**: reconcile this node's data with each co-replica
    /// peer by exchanging per-key version stamps and copying the newer side in
    /// **both** directions, so replicas converge even without read traffic (the
    /// gap that read-repair alone leaves — e.g. a write that reached only a
    /// minority, or a replica that was down). Reconciliation is restricted to
    /// keys both nodes replicate, and tombstones take part (a newer delete wins).
    /// This is a full-table comparison; a Merkle tree would let it skip identical
    /// key ranges instead of streaming the whole shard (future work). Returns the
    /// number of rows repaired (pulled + pushed).
    pub fn repair(&self) -> EngineResult<usize> {
        let mut repaired = 0usize;
        let tables = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .table_names();

        for table in tables {
            let local_rows = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .local_scan_versioned_with_tombstones(&table)?;
            let local_map: HashMap<Vec<u8>, (Hlc, bool, Vec<u8>)> = local_rows
                .into_iter()
                .map(|(k, v, h, is_put)| (k, (h, is_put, v)))
                .collect();

            for (pid, addr) in self.peers_with_ids() {
                let remote = match self.pool.call(
                    &addr,
                    &Request::LocalScan {
                        table: table.clone(),
                    },
                ) {
                    Ok(Response::Scan { rows }) => rows,
                    _ => continue, // unreachable peer: skip this round
                };

                // Pull: a remote version newer than ours (for a key we replicate)
                // is applied locally.
                let mut remote_hlc: HashMap<Vec<u8>, Hlc> = HashMap::new();
                for (key, value, hlc, is_put) in &remote {
                    remote_hlc.insert(key.clone(), *hlc);
                    if !self.replicas_for(key).contains(&self.id) {
                        continue;
                    }
                    if local_map.get(key).is_some_and(|(lh, _, _)| *lh >= *hlc) {
                        continue;
                    }
                    let op = if *is_put {
                        WriteOp::Put(value.clone())
                    } else {
                        WriteOp::Delete
                    };
                    if self.apply_write_local(&table, key, &op, *hlc).is_ok() {
                        repaired += 1;
                    }
                }

                // Push: a local version newer than the peer's (for a key the peer
                // replicates) is sent to it.
                for (key, (lh, is_put, value)) in &local_map {
                    if !self.replicas_for(key).contains(&pid) {
                        continue;
                    }
                    if remote_hlc.get(key).is_some_and(|rh| rh >= lh) {
                        continue;
                    }
                    let op = if *is_put {
                        WriteOp::Put(value.clone())
                    } else {
                        WriteOp::Delete
                    };
                    if matches!(self.send_write(&addr, &table, key, &op, *lh), Ok(true)) {
                        repaired += 1;
                    }
                }
            }
        }
        Ok(repaired)
    }

    /// Trigger [`Node::repair`] on this node and every peer (a cluster-wide
    /// anti-entropy pass). Returns the rows this node repaired.
    pub fn repair_cluster(&self) -> EngineResult<usize> {
        let local = self.repair()?;
        for addr in &self.peer_addrs() {
            let _ = self.pool.call(addr, &Request::Repair);
        }
        Ok(local)
    }

    /// Buffer a write that couldn't reach `replica` (for hinted handoff).
    fn store_hint(&self, replica: &NodeId, table: &str, key: &[u8], op: &WriteOp, hlc: Hlc) {
        let mut hints = self.hints.lock().expect("hints lock");
        let bucket = hints.entry(replica.clone()).or_default();
        if bucket.len() < MAX_HINTS_PER_REPLICA {
            bucket.push((table.to_string(), key.to_vec(), op.clone(), hlc));
            self.counters.hints_stored.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Whether any hints are buffered (cheap check before spawning a flush).
    fn hints_pending(&self) -> bool {
        !self.hints.lock().expect("hints lock").is_empty()
    }

    /// Replay buffered hints to replicas that are reachable again — *hinted
    /// handoff*. A hint that still can't be delivered is kept for the next
    /// attempt. Returns the number of hinted writes handed off.
    pub fn flush_hints(&self) -> usize {
        // Snapshot + clear so the lock isn't held across network I/O.
        let pending: Vec<(NodeId, Vec<HintedWrite>)> = {
            let mut hints = self.hints.lock().expect("hints lock");
            hints.drain().collect()
        };
        let mut delivered = 0usize;
        for (replica, writes) in pending {
            let Some(addr) = self.peer_addr(&replica) else {
                continue; // no longer a peer: drop its hints
            };
            let mut remaining = Vec::new();
            for (table, key, op, hlc) in writes {
                if matches!(self.send_write(&addr, &table, &key, &op, hlc), Ok(true)) {
                    delivered += 1;
                } else {
                    remaining.push((table, key, op, hlc));
                }
            }
            if !remaining.is_empty() {
                self.hints
                    .lock()
                    .expect("hints lock")
                    .entry(replica)
                    .or_default()
                    .extend(remaining);
            }
        }
        self.counters
            .hints_replayed
            .fetch_add(delivered as u64, Ordering::Relaxed);
        delivered
    }

    /// Start serving internode RPC on this node's address (background thread).
    pub fn serve_internode(self: &Arc<Self>) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.cfg.internode_addr)?;
        let node = Arc::clone(self);
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let node = Arc::clone(&node);
                thread::spawn(move || node.handle_internode(stream));
            }
        });
        Ok(())
    }

    fn handle_internode(&self, mut stream: TcpStream) {
        // Disable Nagle: connections are pooled and reused for many small
        // request/response frames, so Nagle + delayed-ACK would add ~40 ms.
        stream.set_nodelay(true).ok();
        while let Ok(framed) = read_frame(&mut stream) {
            let response = match internode::frame_decode(&framed).and_then(|p| Request::decode(&p)) {
                Ok(req) => self.apply_local(req),
                Err(e) => Response::Err(e.to_string()),
            };
            if write_frame(&mut stream, &internode::frame_encode(&response.encode())).is_err() {
                return;
            }
        }
    }

    /// Apply an internode request to local storage. Reads take a shared lock;
    /// writes take an exclusive lock.
    fn apply_local(&self, req: Request) -> Response {
        match req {
            Request::Ping => Response::Pong,
            Request::LocalScan { table } => match self.local.read() {
                Ok(db) => match db.local_scan_versioned_with_tombstones(&table) {
                    Ok(rows) => Response::Scan { rows },
                    Err(e) => Response::Err(e.to_string()),
                },
                Err(_) => Response::Err("local lock poisoned".into()),
            },
            Request::LocalGet { table, key } => match self.local.read() {
                Ok(db) => match db.local_get_versioned(&table, &key) {
                    Ok(entry) => Response::Get { entry },
                    Err(e) => Response::Err(e.to_string()),
                },
                Err(_) => Response::Err("local lock poisoned".into()),
            },
            Request::FilteredScan { table, filter } => match self.local.read() {
                Ok(db) => match db.local_scan_filtered_keys(&table, &Some(filter)) {
                    Ok(keys) => Response::Keys { keys },
                    Err(e) => Response::Err(e.to_string()),
                },
                Err(_) => Response::Err("local lock poisoned".into()),
            },
            Request::IndexScan { index, start, end } => match self.local.read() {
                Ok(db) => match db.index_scan_keys(&index, start.as_deref(), end.as_deref()) {
                    Ok(keys) => Response::Keys { keys },
                    Err(e) => Response::Err(e.to_string()),
                },
                Err(_) => Response::Err("local lock poisoned".into()),
            },
            Request::VectorSearch { index, query, k } => match self.local.read() {
                Ok(db) => match db.vector_search_local(&index, &query, k as usize) {
                    Ok(hits) => Response::VectorHits { hits },
                    Err(e) => Response::Err(e.to_string()),
                },
                Err(_) => Response::Err("local lock poisoned".into()),
            },
            Request::ApplyPut {
                table,
                key,
                value,
                hlc,
            } => write_response(self.apply_write_local(&table, &key, &WriteOp::Put(value), hlc)),
            Request::ApplyDelete { table, key, hlc } => {
                write_response(self.apply_write_local(&table, &key, &WriteOp::Delete, hlc))
            }
            Request::ApplyDdl { db, sql } => {
                self.with_write(|d| d.execute_session(&db, &sql).map(|_| ()))
            }
            Request::SetMembership {
                epoch,
                members,
                prev_members,
            } => {
                let to_ids = |v: Vec<(String, String)>| -> Vec<(NodeId, String)> {
                    v.into_iter().map(|(id, addr)| (NodeId::new(id), addr)).collect()
                };
                self.set_membership(&to_ids(members), &to_ids(prev_members), epoch);
                Response::Ack
            }
            Request::Rebalance { joiner } => match self.rebalance_to(&NodeId::new(joiner)) {
                Ok(()) => Response::Ack,
                Err(e) => Response::Err(e.to_string()),
            },
            Request::Drain { members } => {
                let members: Vec<(NodeId, String)> = members
                    .into_iter()
                    .map(|(id, addr)| (NodeId::new(id), addr))
                    .collect();
                match self.drain_to(&members) {
                    Ok(()) => Response::Ack,
                    Err(e) => Response::Err(e.to_string()),
                }
            }
            Request::Reclaim => match self.reclaim() {
                Ok(_) => Response::Ack,
                Err(e) => Response::Err(e.to_string()),
            },
            Request::Repair => match self.repair() {
                Ok(_) => Response::Ack,
                Err(e) => Response::Err(e.to_string()),
            },
        }
    }

    /// Run a write closure under the exclusive lock, mapping the result to an
    /// `Ack`/`Err` response.
    fn with_write(&self, f: impl FnOnce(&mut Database) -> EngineResult<()>) -> Response {
        match self.local.write() {
            Ok(mut db) => match f(&mut db) {
                Ok(()) => Response::Ack,
                Err(e) => Response::Err(e.to_string()),
            },
            Err(_) => Response::Err("local lock poisoned".into()),
        }
    }

    /// Execute a SQL statement as the cluster coordinator against the `default`
    /// database. Convenience wrapper over [`Node::execute_session`].
    pub fn execute(self: &Arc<Self>, sql: &str) -> EngineResult<QueryOutput> {
        match self.execute_session(DEFAULT_DATABASE, sql)? {
            SessionEffect::Output(out) => Ok(out),
            // `USE` outside a stateful session is a no-op acknowledgement.
            SessionEffect::UseDatabase(_) => Ok(QueryOutput::Ddl),
        }
    }

    /// Execute a SQL statement as the cluster coordinator within a session whose
    /// current database is `current_db`. Table/index names resolve against it
    /// (and `db.table` overrides it); `CREATE`/`DROP DATABASE` and table DDL are
    /// broadcast to every member; `USE` is returned for the caller to apply.
    pub fn execute_session(
        self: &Arc<Self>,
        current_db: &str,
        sql: &str,
    ) -> EngineResult<SessionEffect> {
        self.execute_session_with(current_db, sql, None)
    }

    /// Like [`Node::execute_session`], but overriding the cluster's configured
    /// read/write consistency with `consistency` when it is `Some` (a per-request
    /// level carried from the client); `None` uses the node defaults.
    pub fn execute_session_with(
        self: &Arc<Self>,
        current_db: &str,
        sql: &str,
        consistency: Option<Consistency>,
    ) -> EngineResult<SessionEffect> {
        let mut stmt = parse(sql)?;
        if matches!(
            stmt,
            Statement::Begin | Statement::Commit | Statement::Rollback
        ) {
            return Err(EngineError::Unsupported(
                "multi-statement transactions are not supported in cluster mode \
                 (writes are autocommitted per statement)"
                    .into(),
            ));
        }
        // `USE` validates against the (replicated) database registry and asks the
        // caller to switch; it never touches storage.
        if let Statement::UseDatabase { name } = &stmt {
            let exists = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .has_database(name);
            if !exists {
                return Err(EngineError::DatabaseNotFound(name.clone()));
            }
            return Ok(SessionEffect::UseDatabase(name.clone()));
        }
        // DDL — including CREATE/DROP DATABASE — broadcasts to every member so the
        // schema and database registry stay identical cluster-wide.
        if is_ddl(&stmt) {
            self.broadcast_ddl(current_db, sql)?;
            return Ok(SessionEffect::Output(QueryOutput::Ddl));
        }
        // Read-only catalog/stat introspection: the catalog is identical on every
        // node (DDL is broadcast), so answer from the local engine, filtered to
        // the current database, without fan-out.
        if matches!(
            stmt,
            Statement::ShowTables
                | Statement::ShowIndexes
                | Statement::ShowStatus
                | Statement::ShowDatabases
        ) {
            return self
                .local
                .write()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .execute_session(current_db, sql);
        }
        // DML/SELECT: resolve names to internal (database-qualified) form, then
        // coordinate. The resolved table name is opaque to replication, so quorum
        // writes, hinted handoff, read-repair, and scatter/gather all work as-is.
        namespace::resolve_statement(&mut stmt, current_db);
        let mut coord = Coordinator {
            node: Arc::clone(self),
            oc: consistency,
        };
        run(stmt, &mut coord)
            .map(SessionEffect::Output)
            .map_err(|e| namespace::humanize_error(e, current_db))
    }

    /// Broadcast DDL to all members; require a member quorum to apply it (so a
    /// single node being down does not block schema changes). Each node applies
    /// it within `current_db` so table/index names resolve identically. A node
    /// that missed the broadcast would need to catch up via a schema log — not
    /// yet built, so phase 1 relies on the broadcast reaching each node.
    fn broadcast_ddl(&self, current_db: &str, sql: &str) -> EngineResult<()> {
        let mut acks = 0usize;
        // Local first.
        match self.local.write() {
            Ok(mut db) => {
                db.execute_session(current_db, sql)?;
                acks += 1;
            }
            Err(_) => return Err(EngineError::Cluster("local lock poisoned".into())),
        }
        for addr in &self.peer_addrs() {
            if let Ok(Response::Ack) = self.pool.call(
                addr,
                &Request::ApplyDdl {
                    db: current_db.to_string(),
                    sql: sql.to_string(),
                },
            ) {
                acks += 1;
            }
        }
        let needed = Consistency::Quorum.required(self.member_count());
        if acks >= needed {
            Ok(())
        } else {
            Err(EngineError::Cluster(format!(
                "DDL quorum not met: {acks}/{needed} members applied"
            )))
        }
    }

    /// Replicate a single write to the key's replica set, blocking only until
    /// the configured write consistency is satisfied. The local replica is
    /// always written synchronously (so the coordinator can read its own
    /// writes); peers are written synchronously up to the quorum, and any
    /// remaining peers are replicated in the background (so e.g. CL=ONE returns
    /// after the local fsync without waiting for the peer round-trip).
    fn replicate(
        self: &Arc<Self>,
        table: &str,
        key: &[u8],
        op: WriteOp,
        hlc: Hlc,
        oc: Option<Consistency>,
    ) -> EngineResult<()> {
        self.counters.writes_total.fetch_add(1, Ordering::Relaxed);
        let replicas = self.replicas_for(key);
        let needed = oc
            .unwrap_or(self.cfg.write_consistency)
            .required(replicas.len().max(1));

        // 0) Opportunistically hand off any buffered hints to recovered replicas
        //    (cheap when there are none; non-blocking).
        if self.hints_pending() {
            let node = Arc::clone(self);
            thread::spawn(move || {
                node.flush_hints();
            });
        }

        // 1) Apply locally to the memtable + WAL buffer under the write lock (fast),
        //    then run the local fsync on a separate thread so it overlaps the peer
        //    network round-trips below — instead of fsync-then-send serially. Read-
        //    your-writes holds immediately (the memtable has the row before the
        //    fsync lands); the local replica only *counts* toward the quorum once
        //    its fsync completes, so durability is unchanged — just overlapped.
        let local_owns = replicas.contains(&self.id);
        let fsync = if local_owns {
            let (commit, handle) = self.apply_write_buffered(table, key, &op, hlc)?;
            Some(thread::spawn(move || handle.sync_through(commit).is_ok()))
        } else {
            None
        };

        // 2) Send to peers inline (concurrent with the local fsync), synchronously
        //    up to the quorum; defer the rest to the background.
        let mut acks = 0usize;
        let local_will_ack = fsync.is_some();
        let mut async_peers: Vec<(NodeId, String)> = Vec::new();
        for replica in &replicas {
            if *replica == self.id {
                continue;
            }
            let Some(addr) = self.peer_addr(replica) else {
                continue;
            };
            // Acks we will have once the in-flight local fsync lands.
            let projected = acks + usize::from(local_will_ack);
            if projected >= needed {
                async_peers.push((replica.clone(), addr));
            } else if matches!(self.send_write(&addr, table, key, &op, hlc), Ok(true)) {
                acks += 1;
            } else {
                // Replica down: buffer the write for hinted handoff.
                self.store_hint(replica, table, key, &op, hlc);
            }
        }

        // 3) Fold in the local replica's durable ack (joins the overlapped fsync).
        if let Some(handle) = fsync {
            if handle.join().unwrap_or(false) {
                acks += 1;
            }
        }

        // 4) Fire-and-forget the remaining replicas (eventual consistency),
        //    buffering a hint for any that are unreachable.
        if !async_peers.is_empty() {
            let node = Arc::clone(self);
            let (table, key, op) = (table.to_string(), key.to_vec(), op.clone());
            thread::spawn(move || {
                for (replica, addr) in async_peers {
                    if !matches!(node.send_write(&addr, &table, &key, &op, hlc), Ok(true)) {
                        node.store_hint(&replica, &table, &key, &op, hlc);
                    }
                }
            });
        }

        if acks >= needed {
            Ok(())
        } else {
            self.counters
                .write_quorum_failures
                .fetch_add(1, Ordering::Relaxed);
            Err(EngineError::Cluster(format!(
                "write quorum not met: {acks}/{needed} acks"
            )))
        }
    }

    /// Apply a write to the local memtable + WAL buffer (no fsync) under the
    /// write lock, returning the commit point and durability handle so the caller
    /// can fsync after releasing the lock — and, in [`Node::replicate`],
    /// concurrently with peer replication.
    fn apply_write_buffered(
        &self,
        table: &str,
        key: &[u8],
        op: &WriteOp,
        hlc: Hlc,
    ) -> EngineResult<(WalCommit, Arc<WalSync>)> {
        let mut db = self
            .local
            .write()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
        match op {
            WriteOp::Put(bytes) => db.apply_put_buffered(table, key, bytes.clone(), hlc),
            WriteOp::Delete => db.apply_delete_buffered(table, key, hlc),
        }
    }

    fn apply_write_local(
        &self,
        table: &str,
        key: &[u8],
        op: &WriteOp,
        hlc: Hlc,
    ) -> EngineResult<()> {
        // Append + apply under the write lock (fast), then fsync outside the lock
        // so concurrent writers' fsyncs coalesce (group commit).
        let (commit, handle) = {
            let mut db = self
                .local
                .write()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
            match op {
                WriteOp::Put(bytes) => db.apply_put_buffered(table, key, bytes.clone(), hlc)?,
                WriteOp::Delete => db.apply_delete_buffered(table, key, hlc)?,
            }
        };
        handle.sync_through(commit)?;
        Ok(())
    }

    /// Point-read `key` from its replica set, resolving by last-writer-wins,
    /// requiring a read quorum of replicas to respond.
    fn point_get(
        &self,
        table: &str,
        key: &[u8],
        oc: Option<Consistency>,
    ) -> EngineResult<Vec<(Vec<u8>, Document)>> {
        self.counters.reads_total.fetch_add(1, Ordering::Relaxed);
        let replicas = self.replicas_for(key);
        let needed = oc
            .unwrap_or(self.cfg.read_consistency)
            .required(replicas.len().max(1));
        let mut responders = 0usize;
        // Best (highest-stamped) version seen: (hlc, Some(value) | None tombstone).
        let mut best: Option<(Hlc, Option<Vec<u8>>)> = None;
        // Per-responding-replica version stamp (`None` = it had no entry), with
        // its address (`None` = the local replica), for read-repair.
        let mut seen: Vec<(Option<String>, Option<Hlc>)> = Vec::new();

        for replica in &replicas {
            let (addr_opt, entry) = if *replica == self.id {
                let e = match self.local.read() {
                    Ok(db) => db.local_get_versioned(table, key)?,
                    Err(_) => return Err(EngineError::Cluster("local lock poisoned".into())),
                };
                (None, e)
            } else if let Some(addr) = self.peer_addr(replica) {
                self.counters.peer_requests.fetch_add(1, Ordering::Relaxed);
                match self.pool.call(
                    &addr,
                    &Request::LocalGet {
                        table: table.to_string(),
                        key: key.to_vec(),
                    },
                ) {
                    Ok(Response::Get { entry }) => (Some(addr), entry),
                    _ => {
                        self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                        continue; // unreachable peer: not a responder
                    }
                }
            } else {
                continue;
            };
            responders += 1;
            let entry_hlc = entry.as_ref().map(|(_, h, _)| *h);
            if let Some((value, hlc, is_put)) = entry {
                if best.as_ref().is_none_or(|(h, _)| hlc > *h) {
                    best = Some((hlc, is_put.then_some(value)));
                }
            }
            seen.push((addr_opt, entry_hlc));
        }

        if responders < needed {
            self.counters
                .read_quorum_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(EngineError::Cluster(format!(
                "read quorum not met: {responders}/{needed} replicas responded"
            )));
        }

        // Advance our clock past what we read so a read-then-write on this
        // coordinator (e.g. `DELETE WHERE pk = …`) mints a strictly newer stamp
        // and wins last-writer-wins — independent of wall-clock resolution.
        if let Some((hlc, _)) = best {
            self.clock.observe(hlc);
        }

        // Read-repair: push the winning version to any replica that responded
        // with an older or missing version, so reads drive convergence.
        if let Some((hbest, valopt)) = &best {
            self.read_repair(table, key, *hbest, valopt, &seen);
        }

        match best {
            Some((_, Some(value))) => {
                let doc = match Value::decode(&value)
                    .map_err(|e| EngineError::Cluster(format!("corrupt row: {e}")))?
                {
                    Value::Document(d) => d,
                    _ => return Ok(Vec::new()),
                };
                Ok(vec![(key.to_vec(), doc)])
            }
            // Absent everywhere, or newest version is a tombstone.
            _ => Ok(Vec::new()),
        }
    }

    /// Push the winning version (`hbest`, `valopt`) to every replica in `seen`
    /// that returned an older or missing version. Best-effort and idempotent (a
    /// write at `hbest` is a no-op on a replica already at `hbest`).
    fn read_repair(
        &self,
        table: &str,
        key: &[u8],
        hbest: Hlc,
        valopt: &Option<Vec<u8>>,
        seen: &[(Option<String>, Option<Hlc>)],
    ) {
        let op = match valopt {
            Some(v) => WriteOp::Put(v.clone()),
            None => WriteOp::Delete,
        };
        for (addr_opt, entry_hlc) in seen {
            if entry_hlc.is_some_and(|h| h >= hbest) {
                continue; // already up to date
            }
            self.counters.read_repairs.fetch_add(1, Ordering::Relaxed);
            match addr_opt {
                None => {
                    let _ = self.apply_write_local(table, key, &op, hbest);
                }
                Some(addr) => {
                    let _ = self.send_write(addr, table, key, &op, hbest);
                }
            }
        }
    }

    fn send_write(
        &self,
        addr: &str,
        table: &str,
        key: &[u8],
        op: &WriteOp,
        hlc: Hlc,
    ) -> std::io::Result<bool> {
        let req = match op {
            WriteOp::Put(bytes) => Request::ApplyPut {
                table: table.to_string(),
                key: key.to_vec(),
                value: bytes.clone(),
                hlc,
            },
            WriteOp::Delete => Request::ApplyDelete {
                table: table.to_string(),
                key: key.to_vec(),
                hlc,
            },
        };
        self.counters.peer_requests.fetch_add(1, Ordering::Relaxed);
        match self.pool.call(addr, &req) {
            Ok(resp) => Ok(matches!(resp, Response::Ack)),
            Err(e) => {
                self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Gather a table from all reachable members, merged by last-writer-wins.
    /// Tombstones participate in the merge so a delete on one replica correctly
    /// masks a stale `Put` gathered from another (quorum read ∩ quorum write).
    fn cluster_scan(
        &self,
        table: &str,
        oc: Option<Consistency>,
    ) -> EngineResult<Vec<(Vec<u8>, Document)>> {
        self.counters.reads_total.fetch_add(1, Ordering::Relaxed);
        // key -> (hlc, Some(encoded value) | None tombstone)
        let mut merged: BTreeMap<Vec<u8>, (Hlc, Option<Vec<u8>>)> = BTreeMap::new();
        let mut responders = 0usize;

        // Local shard (with tombstones).
        {
            let db = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
            for (key, value, hlc, is_put) in db.local_scan_versioned_with_tombstones(table)? {
                merge_row(&mut merged, key, is_put.then_some(value), hlc);
            }
            responders += 1;
        }

        // Peers.
        for addr in &self.peer_addrs() {
            self.counters.peer_requests.fetch_add(1, Ordering::Relaxed);
            if let Ok(Response::Scan { rows }) = self.pool.call(
                addr,
                &Request::LocalScan {
                    table: table.to_string(),
                },
            ) {
                for (key, value, hlc, is_put) in rows {
                    merge_row(&mut merged, key, is_put.then_some(value), hlc);
                }
                responders += 1;
            } else {
                self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
            }
        }

        let needed = oc
            .unwrap_or(self.cfg.read_consistency)
            .required(self.member_count());
        if responders < needed {
            self.counters
                .read_quorum_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(EngineError::Cluster(format!(
                "read quorum not met: {responders}/{needed} members responded"
            )));
        }

        // Advance our clock past the newest row seen, so a read-then-write on
        // this coordinator (e.g. a non-PK `UPDATE`/`DELETE`) is causally ordered
        // after it under last-writer-wins.
        if let Some(max) = merged.values().map(|(hlc, _)| *hlc).max() {
            self.clock.observe(max);
        }

        // Decode surviving rows into documents, dropping tombstoned keys.
        let mut out = Vec::with_capacity(merged.len());
        for (key, (_hlc, value)) in merged {
            let Some(bytes) = value else { continue };
            if let Value::Document(doc) = Value::decode(&bytes)
                .map_err(|e| EngineError::Cluster(format!("corrupt row: {e}")))?
            {
                out.push((key, doc));
            }
        }
        Ok(out)
    }

    /// Distributed secondary-index lookup: gather candidate row keys from every
    /// member's local index over `[start, end)`, then re-read each key at the
    /// configured read quorum so the authoritative last-writer-wins version is
    /// returned. Candidate keys are a superset (a node's local index may reflect
    /// a stale value); the quorum re-read + the coordinator's post-filter make
    /// the result exact. Unreachable members are skipped — their rows are still
    /// found via other replicas' indexes (for `replication_factor > 1`).
    fn index_lookup(
        &self,
        table: &str,
        index: &str,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        oc: Option<Consistency>,
    ) -> EngineResult<Vec<(Vec<u8>, Document)>> {
        let mut keys: BTreeMap<Vec<u8>, ()> = BTreeMap::new();

        // Local index shard.
        {
            let db = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
            for k in db.index_scan_keys(index, start.as_deref(), end.as_deref())? {
                keys.insert(k, ());
            }
        }

        // Peer index shards.
        let req = Request::IndexScan {
            index: index.to_string(),
            start,
            end,
        };
        for addr in &self.peer_addrs() {
            if let Ok(Response::Keys { keys: ks }) = self.pool.call(addr, &req) {
                for k in ks {
                    keys.insert(k, ());
                }
            }
        }

        // Re-read each candidate key at quorum for its authoritative version.
        let mut out = Vec::new();
        for key in keys.into_keys() {
            out.extend(self.point_get(table, &key, oc)?);
        }
        Ok(out)
    }

    /// Distributed **filter pushdown** for a non-indexed `WHERE`: scatter the
    /// predicate to every member so each filters its own shard and returns only
    /// the matching candidate keys, then re-read each at quorum (last-writer-wins
    /// authoritative version). Like [`Node::index_lookup`] but the candidates come
    /// from a filtered scan rather than an index — far less data than shipping
    /// every member's whole shard and filtering at the coordinator. Sound under
    /// LWW because a stale or since-changed candidate is corrected by the re-read.
    fn filtered_lookup(
        &self,
        table: &str,
        filter: &Expr,
        oc: Option<Consistency>,
    ) -> EngineResult<Vec<(Vec<u8>, Document)>> {
        let mut keys: BTreeMap<Vec<u8>, ()> = BTreeMap::new();

        // Local shard.
        {
            let db = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
            for k in db.local_scan_filtered_keys(table, &Some(filter.clone()))? {
                keys.insert(k, ());
            }
        }

        // Peer shards.
        let req = Request::FilteredScan {
            table: table.to_string(),
            filter: filter.clone(),
        };
        for addr in &self.peer_addrs() {
            if let Ok(Response::Keys { keys: ks }) = self.pool.call(addr, &req) {
                for k in ks {
                    keys.insert(k, ());
                }
            }
        }

        let mut out = Vec::new();
        for key in keys.into_keys() {
            out.extend(self.point_get(table, &key, oc)?);
        }
        Ok(out)
    }

    /// Distributed approximate nearest-neighbor search: scatter the query to
    /// every member's local vector index, merge their per-shard top-k by
    /// distance, then re-read the survivors at quorum and apply `filter`.
    /// Returns up to `k` rows as `(key, doc, distance)`, nearest first. Distances
    /// are from the (per-shard) HNSW; `filter` is applied after the authoritative
    /// re-read, so very selective filters want more over-fetch.
    pub fn vector_search(
        self: &Arc<Self>,
        index: &str,
        query: &[f32],
        k: usize,
        filter: &Option<Expr>,
    ) -> EngineResult<Vec<(Vec<u8>, Document, f32)>> {
        let table = {
            let db = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
            db.vector_index_table(index)
                .ok_or_else(|| EngineError::IndexNotFound(index.to_string()))?
        };
        // Over-fetch per shard so the merge (and any filtering) still yields k.
        let fetch = if filter.is_some() {
            k.saturating_mul(4).max(k + 16)
        } else {
            k.max(1)
        };

        // Best (smallest) distance seen per key across all shards.
        let mut best: HashMap<Vec<u8>, f32> = HashMap::new();
        let consider = |key: Vec<u8>, dist: f32, best: &mut HashMap<Vec<u8>, f32>| {
            best.entry(key)
                .and_modify(|d| {
                    if dist < *d {
                        *d = dist;
                    }
                })
                .or_insert(dist);
        };
        {
            let db = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
            for (key, dist) in db.vector_search_local(index, query, fetch)? {
                consider(key, dist, &mut best);
            }
        }
        let req = Request::VectorSearch {
            index: index.to_string(),
            query: query.to_vec(),
            k: fetch as u32,
        };
        for addr in &self.peer_addrs() {
            if let Ok(Response::VectorHits { hits }) = self.pool.call(addr, &req) {
                for (key, dist) in hits {
                    consider(key, dist, &mut best);
                }
            }
        }

        // Rank globally by distance, then re-read + filter until we have k.
        let mut ranked: Vec<(Vec<u8>, f32)> = best.into_iter().collect();
        ranked.sort_by(|a, b| a.1.total_cmp(&b.1));
        let mut out = Vec::new();
        for (key, dist) in ranked {
            let rows = filter_rows(filter, self.point_get(table.as_str(), &key, None)?)?;
            if let Some((_, doc)) = rows.into_iter().next() {
                out.push((key, doc, dist));
                if out.len() >= k {
                    break;
                }
            }
        }
        Ok(out)
    }
}

/// A pending replicated mutation.
#[derive(Clone, Debug)]
enum WriteOp {
    Put(Vec<u8>),
    Delete,
}

fn merge_row(
    merged: &mut BTreeMap<Vec<u8>, (Hlc, Option<Vec<u8>>)>,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
    hlc: Hlc,
) {
    merged
        .entry(key)
        .and_modify(|cur| {
            if hlc > cur.0 {
                *cur = (hlc, value.clone());
            }
        })
        .or_insert((hlc, value));
}

/// If `filter` is a single-column primary-key equality (`pk = literal`), return
/// the storage key for that row so the read can be a point get. The key must be
/// built exactly as the engine builds it for inserts: the order-preserving
/// encoding of a one-element array holding the value.
fn pk_point_key(pk: &[String], filter: &Option<Expr>) -> Option<Vec<u8>> {
    if pk.len() != 1 {
        return None;
    }
    let Some(Expr::Binary {
        op: BinaryOp::Eq,
        left,
        right,
    }) = filter
    else {
        return None;
    };
    let col = &pk[0];
    let value = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(c), Expr::Literal(v)) | (Expr::Literal(v), Expr::Column(c)) if c == col => v,
        _ => return None,
    };
    if value.is_null() {
        return None;
    }
    Some(Value::Array(vec![value.clone()]).encode_key())
}

/// Map a local write result to an internode `Ack`/`Err` response.
fn write_response(result: EngineResult<()>) -> Response {
    match result {
        Ok(()) => Response::Ack,
        Err(e) => Response::Err(e.to_string()),
    }
}

/// Path of the per-joiner migration checkpoint file in a node's data directory.
fn migrate_ckpt_path(dir: &Path, joiner: &NodeId) -> PathBuf {
    let safe: String = joiner
        .0
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    dir.join(format!("migrate-{safe}"))
}

/// Persist migration progress as `<table>\n<hex(last_key)>` (atomic). Best-effort:
/// a failed write just means a re-run does a little extra (idempotent) work.
fn save_migrate_ckpt(path: &Path, table: &str, last_key: &[u8]) {
    let body = format!("{table}\n{}", to_hex(last_key));
    let tmp = path.with_extension("tmp");
    if std::fs::write(&tmp, &body).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

/// Load a migration checkpoint written by [`save_migrate_ckpt`], if present.
fn load_migrate_ckpt(path: &Path) -> Option<(String, Vec<u8>)> {
    let text = std::fs::read_to_string(path).ok()?;
    let (table, hex) = text.split_once('\n')?;
    Some((table.to_string(), from_hex(hex.trim())?))
}

fn to_hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push_str(&format!("{x:02x}"));
    }
    s
}

fn from_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Encode a member list as `(id, addr)` string pairs for the wire.
fn wire_of(members: &[(NodeId, String)]) -> Vec<(String, String)> {
    members
        .iter()
        .map(|(id, a)| (id.0.clone(), a.clone()))
        .collect()
}

/// Path of the persisted membership file inside a node's data directory.
fn membership_path(dir: &Path) -> PathBuf {
    dir.join("topology")
}

/// Persist `epoch` + `members` as a small text file (first line the epoch, then
/// one `id<space>addr` per line), written atomically. Best-effort: a failed
/// write is non-fatal (the in-memory ring stays authoritative for this run).
fn save_membership(path: &Path, epoch: u64, members: &[(NodeId, String)]) {
    let mut body = format!("{epoch}\n");
    for (id, addr) in members {
        body.push_str(&format!("{} {}\n", id.0, addr));
    }
    let tmp = path.with_extension("tmp");
    if std::fs::write(&tmp, &body).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

/// Load a persisted membership written by [`save_membership`], if present.
fn load_membership(path: &Path) -> Option<(Vec<(NodeId, String)>, u64)> {
    let text = std::fs::read_to_string(path).ok()?;
    let mut lines = text.lines();
    let epoch: u64 = lines.next()?.trim().parse().ok()?;
    let mut members = Vec::new();
    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let (id, addr) = line.split_once(' ')?;
        members.push((NodeId::new(id), addr.to_string()));
    }
    if members.is_empty() {
        return None;
    }
    Some((members, epoch))
}

/// A stable Prometheus label for a consistency level.
fn consistency_label(c: Consistency) -> &'static str {
    match c {
        Consistency::One => "one",
        Consistency::Quorum => "quorum",
        Consistency::All => "all",
    }
}

fn is_ddl(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateTable(_)
            | Statement::DropTable { .. }
            | Statement::CreateIndex(_)
            | Statement::DropIndex { .. }
            | Statement::CreateVectorIndex(_)
            | Statement::DropVectorIndex { .. }
            | Statement::AlterTable(_)
            | Statement::CreateDatabase { .. }
            | Statement::DropDatabase { .. }
    )
}

/// The networked [`Cluster`] implementation driving `run()` on a coordinator.
struct Coordinator {
    node: Arc<Node>,
    /// Per-request consistency override; `None` uses the node's configured level.
    oc: Option<Consistency>,
}

impl Coordinator {
    /// Ask the local catalog whether a secondary index can serve `filter`, and
    /// the byte range to scan. The encoding is catalog-deterministic, so the
    /// same range applies to every member's local index.
    fn plan_index_scan(
        &self,
        table: &str,
        filter: &Option<Expr>,
    ) -> EngineResult<Option<IndexScanRange>> {
        Ok(self
            .node
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .plan_index_scan(table, filter))
    }
}

impl Cluster for Coordinator {
    fn primary_key(&self, table: &str) -> EngineResult<Vec<String>> {
        self.node
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .table_primary_key(table)
    }

    fn matching_rows(
        &mut self,
        table: &str,
        filter: &Option<Expr>,
    ) -> EngineResult<Vec<(Vec<u8>, Document)>> {
        // Fast path: a primary-key equality is a point read to the key's
        // replica set, not a full cluster scan.
        let pk = self.primary_key(table)?;
        if let Some(key) = pk_point_key(&pk, filter) {
            let rows = self.node.point_get(table, &key, self.oc)?;
            return filter_rows(filter, rows);
        }
        // Indexed non-PK predicate: push the index scan to every node to gather
        // candidate keys, then re-read each at quorum — far less data than
        // shipping every node's whole shard.
        if let Some((index, start, end)) = self.plan_index_scan(table, filter)? {
            let rows = self.node.index_lookup(table, &index, start, end, self.oc)?;
            return filter_rows(filter, rows);
        }
        // Non-indexed predicate: push the filter to each node and gather only the
        // matching candidate keys (then re-read at quorum), instead of shipping
        // every node's whole shard to the coordinator.
        if let Some(f) = filter {
            let rows = self.node.filtered_lookup(table, f, self.oc)?;
            return filter_rows(filter, rows);
        }
        // No predicate at all: gather the whole table, LWW-merged.
        let rows = self.node.cluster_scan(table, self.oc)?;
        filter_rows(filter, rows)
    }

    fn put(&mut self, table: &str, key: &[u8], doc: &Document) -> EngineResult<()> {
        let hlc = self.node.clock.now();
        let bytes = Value::Document(doc.clone()).encode();
        self.node.replicate(table, key, WriteOp::Put(bytes), hlc, self.oc)
    }

    fn delete(&mut self, table: &str, key: &[u8], _doc: &Document) -> EngineResult<()> {
        let hlc = self.node.clock.now();
        self.node.replicate(table, key, WriteOp::Delete, hlc, self.oc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skaidb_engine::{QueryOutput, ResultSet};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        static C: AtomicU64 = AtomicU64::new(0);
        let n = C.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("skaidb-node-{tag}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    /// Grab a free localhost address (small TOCTOU window, fine for tests).
    fn free_addr() -> String {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        format!("127.0.0.1:{}", addr.port())
    }

    fn member(id: &str, addr: &str) -> (NodeId, String) {
        (NodeId::new(id), addr.to_string())
    }

    fn rows(out: QueryOutput) -> ResultSet {
        match out {
            QueryOutput::Rows(rs) => rs,
            other => panic!("expected rows, got {other:?}"),
        }
    }

    fn cfg(
        id: &str,
        addr: &str,
        members: &[(NodeId, String)],
        r: Consistency,
        w: Consistency,
    ) -> NodeConfig {
        NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: 3,
            vnodes_per_node: 64,
            read_consistency: r,
            write_consistency: w,
        }
    }

    #[test]
    fn three_node_replication_and_distributed_reads() {
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b), member("c", &c)];

        let na = Node::new(
            Database::open(temp_dir("a")).unwrap(),
            cfg("a", &a, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nb = Node::new(
            Database::open(temp_dir("b")).unwrap(),
            cfg("b", &b, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nc = Node::new(
            Database::open(temp_dir("c")).unwrap(),
            cfg("c", &c, &members, Consistency::Quorum, Consistency::Quorum),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        nc.serve_internode().unwrap();

        // DDL via A propagates to all.
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        // Writes via A replicate to the ring.
        na.execute("INSERT INTO t (id, name) VALUES (1, 'ada'), (2, 'bob'), (3, 'cleo')")
            .unwrap();

        // Reads via B and C see all rows (gathered from replicas, LWW-merged).
        for coord in [&nb, &nc] {
            let rs = rows(coord.execute("SELECT id, name FROM t ORDER BY id").unwrap());
            assert_eq!(rs.rows.len(), 3, "every coordinator sees all rows");
            assert_eq!(rs.rows[0], vec![Value::Int(1), Value::String("ada".into())]);
            assert_eq!(
                rs.rows[2],
                vec![Value::Int(3), Value::String("cleo".into())]
            );
        }

        // Update via B, read via C reflects it (last-writer-wins by HLC).
        nb.execute("UPDATE t SET name = 'ADA' WHERE id = 1")
            .unwrap();
        let rs = rows(nc.execute("SELECT name FROM t WHERE id = 1").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::String("ADA".into())]]);

        // Delete via C, read via A reflects it.
        nc.execute("DELETE FROM t WHERE id = 2").unwrap();
        let rs = rows(na.execute("SELECT id FROM t ORDER BY id").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
    }

    #[test]
    fn distributed_secondary_index_query() {
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(
            Database::open(temp_dir("ixa")).unwrap(),
            cfg("a", &a, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nb = Node::new(
            Database::open(temp_dir("ixb")).unwrap(),
            cfg("b", &b, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nc = Node::new(
            Database::open(temp_dir("ixc")).unwrap(),
            cfg("c", &c, &members, Consistency::Quorum, Consistency::Quorum),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        nc.serve_internode().unwrap();

        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        na.execute("CREATE INDEX t_region ON t(region)").unwrap();
        na.execute("CREATE INDEX t_age ON t(age)").unwrap();
        na.execute(
            "INSERT INTO t (id, region, age) VALUES \
             (1,'eu',30),(2,'us',20),(3,'eu',40),(4,'us',50),(5,'eu',25)",
        )
        .unwrap();

        // Equality on a non-PK indexed column, coordinated by a *different* node:
        // each member uses its local index; the coordinator re-reads at quorum.
        let rs = rows(
            nb.execute("SELECT id FROM t WHERE region = 'eu' ORDER BY id")
                .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![vec![Value::Int(1)], vec![Value::Int(3)], vec![Value::Int(5)]]
        );

        // Range on a non-PK indexed column, coordinated by a third node.
        let rs = rows(
            nc.execute("SELECT id FROM t WHERE age > 30 ORDER BY id").unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(3)], vec![Value::Int(4)]]);

        // Update the indexed value (PK update); the index query reflects it
        // cluster-wide because candidates are re-read at quorum (LWW).
        nb.execute("UPDATE t SET region = 'us' WHERE id = 1").unwrap();
        let rs = rows(
            na.execute("SELECT id FROM t WHERE region = 'eu' ORDER BY id")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(3)], vec![Value::Int(5)]]);
    }

    #[test]
    fn distributed_non_indexed_filter_pushdown() {
        // A WHERE on a non-indexed column is pushed to each node (filtered scan →
        // candidate keys → quorum re-read), and stays correct when a row is
        // updated to no longer match (the re-read sees the authoritative version).
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(
            Database::open(temp_dir("fpa")).unwrap(),
            cfg("a", &a, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nb = Node::new(
            Database::open(temp_dir("fpb")).unwrap(),
            cfg("b", &b, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nc = Node::new(
            Database::open(temp_dir("fpc")).unwrap(),
            cfg("c", &c, &members, Consistency::Quorum, Consistency::Quorum),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        nc.serve_internode().unwrap();

        // No index on `status` or `age` — these queries take the pushdown path.
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        na.execute(
            "INSERT INTO t (id, status, age) VALUES \
             (1,'active',20),(2,'inactive',35),(3,'active',40),(4,'active',25),(5,'inactive',50)",
        )
        .unwrap();

        // Equality filter, coordinated by a different node.
        let rs = rows(
            nb.execute("SELECT id FROM t WHERE status = 'active' ORDER BY id")
                .unwrap(),
        );
        assert_eq!(
            rs.rows,
            vec![vec![Value::Int(1)], vec![Value::Int(3)], vec![Value::Int(4)]]
        );

        // Range filter on another non-indexed column.
        let rs = rows(nc.execute("SELECT id FROM t WHERE age > 30 ORDER BY id").unwrap());
        assert_eq!(
            rs.rows,
            vec![vec![Value::Int(2)], vec![Value::Int(3)], vec![Value::Int(5)]]
        );

        // Update id=4 to no longer match; the quorum re-read drops it (sound LWW).
        nc.execute("UPDATE t SET status = 'inactive' WHERE id = 4")
            .unwrap();
        let rs = rows(
            na.execute("SELECT id FROM t WHERE status = 'active' ORDER BY id")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);
    }

    #[test]
    fn distributed_databases_replicate() {
        use skaidb_engine::SessionEffect;
        // `CREATE DATABASE`, DDL, and DML inside a non-default database all
        // replicate across the cluster, and databases are isolated.
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(
            Database::open(temp_dir("dba")).unwrap(),
            cfg("a", &a, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nb = Node::new(
            Database::open(temp_dir("dbb")).unwrap(),
            cfg("b", &b, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nc = Node::new(
            Database::open(temp_dir("dbc")).unwrap(),
            cfg("c", &c, &members, Consistency::Quorum, Consistency::Quorum),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        nc.serve_internode().unwrap();

        // Create a database + table + rows in it, coordinated by node A.
        na.execute("CREATE DATABASE shop").unwrap();
        na.execute_session("shop", "CREATE TABLE orders (PRIMARY KEY (id))")
            .unwrap();
        na.execute_session("shop", "INSERT INTO orders (id, total) VALUES (1, 10), (2, 20)")
            .unwrap();

        // The database is visible cluster-wide (registry replicated via DDL).
        assert!(nb
            .local
            .read()
            .unwrap()
            .has_database("shop"));

        // Rows are readable from another node within `shop`, and via an explicit
        // qualifier from the default database.
        let got = |node: &std::sync::Arc<Node>, db: &str, sql: &str| match node
            .execute_session(db, sql)
            .unwrap()
        {
            SessionEffect::Output(QueryOutput::Rows(r)) => r.rows,
            other => panic!("expected rows, got {other:?}"),
        };
        assert_eq!(
            got(&nb, "shop", "SELECT id FROM orders ORDER BY id"),
            vec![vec![Value::Int(1)], vec![Value::Int(2)]]
        );
        assert_eq!(
            got(&nc, "default", "SELECT total FROM shop.orders WHERE id = 2"),
            vec![vec![Value::Int(20)]]
        );

        // Isolation: `orders` does not exist in the default database.
        assert!(nb
            .execute_session("default", "SELECT id FROM orders")
            .is_err());

        // A delete inside `shop` from node C replicates back to A.
        nc.execute_session("shop", "DELETE FROM orders WHERE id = 1")
            .unwrap();
        assert_eq!(
            got(&na, "shop", "SELECT id FROM orders ORDER BY id"),
            vec![vec![Value::Int(2)]]
        );
    }

    #[test]
    fn distributed_vector_search() {
        use skaidb_sql::ast::{BinaryOp, Expr};
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(
            Database::open(temp_dir("vva")).unwrap(),
            cfg("a", &a, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nb = Node::new(
            Database::open(temp_dir("vvb")).unwrap(),
            cfg("b", &b, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nc = Node::new(
            Database::open(temp_dir("vvc")).unwrap(),
            cfg("c", &c, &members, Consistency::Quorum, Consistency::Quorum),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        nc.serve_internode().unwrap();

        na.execute("CREATE TABLE docs (PRIMARY KEY (id))").unwrap();
        // Broadcast DDL: every node builds its own (initially empty) HNSW.
        na.execute("CREATE VECTOR INDEX docs_emb ON docs (embedding) DIM 3 USING cosine")
            .unwrap();
        na.execute(
            "INSERT INTO docs (id, cat, embedding) VALUES \
             (1,'a',[1.0,0.0,0.0]),(2,'b',[0.0,1.0,0.0]),(3,'a',[0.0,0.0,1.0]),(4,'b',[0.9,0.1,0.0])",
        )
        .unwrap();

        let ids = |hits: Vec<(Vec<u8>, Document, f32)>| -> Vec<i64> {
            hits.iter()
                .map(|(_, doc, _)| match doc.get("id") {
                    Some(Value::Int(i)) => *i,
                    other => panic!("expected int id, got {other:?}"),
                })
                .collect()
        };

        // Distributed kNN coordinated by a different node: scatter to all
        // members' local HNSW, merge, re-read at quorum.
        assert_eq!(ids(nb.vector_search("docs_emb", &[1.0, 0.0, 0.0], 2, &None).unwrap()), vec![1, 4]);

        // Filtered distributed kNN: WHERE cat = 'a' excludes id 4.
        let filter = Some(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column("cat".into())),
            right: Box::new(Expr::Literal(Value::String("a".into()))),
        });
        assert_eq!(
            ids(nc.vector_search("docs_emb", &[1.0, 0.0, 0.0], 2, &filter).unwrap()),
            vec![1, 3]
        );
    }

    #[test]
    fn online_resharding_migrates_keys_to_a_joining_node() {
        // rf=1, CL=ONE: every key has exactly one owner, so a read that succeeds
        // proves the row physically lives on whichever node the *current* ring
        // routes to. Start with {a, b}, fill the table, then join c online and
        // confirm every row is still readable (the ones c now owns were migrated
        // to it — otherwise their point reads would route to c and come back
        // empty).
        let one = Consistency::One;
        let rf1 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: 1,
            vnodes_per_node: 64,
            read_consistency: one,
            write_consistency: one,
        };

        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let ab = vec![member("a", &a), member("b", &b)];
        let abc = vec![member("a", &a), member("b", &b), member("c", &c)];

        let na = Node::new(Database::open(temp_dir("rsa")).unwrap(), rf1("a", &a, &ab));
        let nb = Node::new(Database::open(temp_dir("rsb")).unwrap(), rf1("b", &b, &ab));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        na.execute("CREATE INDEX t_g ON t(g)").unwrap();
        let n = 60;
        for i in 1..=n {
            na.execute(&format!("INSERT INTO t (id, g) VALUES ({i}, {})", i % 5))
                .unwrap();
        }

        // c starts up knowing the eventual membership; add_member rebuilds every
        // node's ring anyway via SetMembership.
        let nc = Node::new(Database::open(temp_dir("rsc")).unwrap(), rf1("c", &c, &abc));
        nc.serve_internode().unwrap();

        // Bring c into the cluster online, orchestrated from a.
        na.add_member("c", &c).unwrap();

        // Every row is still readable from every coordinator (PK point reads now
        // route under the 3-node ring; c serves the share migrated to it).
        for coord in [&na, &nb, &nc] {
            for i in 1..=n {
                let rs = rows(
                    coord
                        .execute(&format!("SELECT id FROM t WHERE id = {i}"))
                        .unwrap(),
                );
                assert_eq!(rs.rows, vec![vec![Value::Int(i)]], "id {i} via some coord");
            }
            // Full table is intact (no key lost or duplicated in the merge).
            let rs = rows(coord.execute("SELECT id FROM t").unwrap());
            assert_eq!(rs.rows.len(), n as usize);
        }

        // The secondary index, bootstrapped onto c, serves a distributed lookup.
        let rs = rows(nc.execute("SELECT id FROM t WHERE g = 0 ORDER BY id").unwrap());
        assert_eq!(
            rs.rows,
            vec![
                vec![Value::Int(5)],
                vec![Value::Int(10)],
                vec![Value::Int(15)],
                vec![Value::Int(20)],
                vec![Value::Int(25)],
                vec![Value::Int(30)],
                vec![Value::Int(35)],
                vec![Value::Int(40)],
                vec![Value::Int(45)],
                vec![Value::Int(50)],
                vec![Value::Int(55)],
                vec![Value::Int(60)],
            ]
        );

        // A write after the join routes under the new ring and is read back.
        nc.execute("INSERT INTO t (id, g) VALUES (61, 1)").unwrap();
        let rs = rows(na.execute("SELECT id FROM t WHERE id = 61").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(61)]]);
    }

    #[test]
    fn pending_ranges_dual_write_to_old_and_new_owner() {
        // rf=1, CL=ALL. We impose a transition (current ring {a,b,c}, previous
        // ring {a,b}); a key that moved onto c must, while the transition is
        // active, be written to BOTH its new owner (c) and its old owner (a/b),
        // so concurrent reads still find it on the old owner until migration
        // finishes.
        let all = Consistency::All;
        let cfg1 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: 1,
            vnodes_per_node: 64,
            read_consistency: all,
            write_consistency: all,
        };
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let abc = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(Database::open(temp_dir("pra")).unwrap(), cfg1("a", &a, &abc));
        let nb = Node::new(Database::open(temp_dir("prb")).unwrap(), cfg1("b", &b, &abc));
        let nc = Node::new(Database::open(temp_dir("prc")).unwrap(), cfg1("c", &c, &abc));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        nc.serve_internode().unwrap();
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();

        // Impose the transition on every node: ring {a,b,c}, prev {a,b}.
        let abc_w = vec![
            ("a".into(), a.clone()),
            ("b".into(), b.clone()),
            ("c".into(), c.clone()),
        ];
        let ab_w = vec![("a".into(), a.clone()), ("b".into(), b.clone())];
        for addr in [&a, &b, &c] {
            let r = internode::call(
                addr,
                &Request::SetMembership {
                    epoch: 1,
                    members: abc_w.clone(),
                    prev_members: ab_w.clone(),
                },
            )
            .unwrap();
            assert!(matches!(r, Response::Ack));
        }

        let addr_of = |n: &NodeId| -> String {
            match n.0.as_str() {
                "a" => a.clone(),
                "b" => b.clone(),
                _ => c.clone(),
            }
        };
        let mut new_ring = Ring::new(64);
        let mut old_ring = Ring::new(64);
        for n in ["a", "b", "c"] {
            new_ring.add_node(NodeId::new(n));
        }
        for n in ["a", "b"] {
            old_ring.add_node(NodeId::new(n));
        }
        let has = |addr: &str, key: &[u8]| -> bool {
            matches!(
                internode::call(
                    addr,
                    &Request::LocalGet {
                        table: "t".into(),
                        key: key.to_vec(),
                    },
                ),
                Ok(Response::Get { entry: Some(_) })
            )
        };

        let mut moved = 0;
        for id in 1..=40i64 {
            na.execute(&format!("INSERT INTO t (id, v) VALUES ({id}, {})", id * 10))
                .unwrap();
            let key = Value::Array(vec![Value::Int(id)]).encode_key();
            let new_owner = new_ring.primary_for(&key).unwrap();
            let old_owner = old_ring.primary_for(&key).unwrap();
            if new_owner != old_owner {
                moved += 1;
                assert!(has(&addr_of(&new_owner), &key), "new owner has id {id}");
                assert!(
                    has(&addr_of(&old_owner), &key),
                    "old owner also has id {id} (dual-write)"
                );
            }
        }
        assert!(moved > 0, "some keys moved to c under the new ring");
    }

    #[test]
    fn hinted_handoff_replays_to_a_recovered_replica() {
        // rf=3, CL=ALL so a write must reach all three replicas. c is created but
        // not served (down), so the write to it fails synchronously and is
        // buffered as a hint. After c recovers, flush_hints replays it.
        let all = Consistency::All;
        let cfg3 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: 3,
            vnodes_per_node: 64,
            read_consistency: all,
            write_consistency: all,
        };
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let m = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(Database::open(temp_dir("hha")).unwrap(), cfg3("a", &a, &m));
        let nb = Node::new(Database::open(temp_dir("hhb")).unwrap(), cfg3("b", &b, &m));
        let nc = Node::new(Database::open(temp_dir("hhc")).unwrap(), cfg3("c", &c, &m));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        // c is intentionally NOT served yet.

        // DDL reaches the a+b quorum; the insert can't reach c (down) so it errors
        // at CL=ALL, but the write to c is buffered as a hint.
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        let _ = na.execute("INSERT INTO t (id, v) VALUES (1, 100)");

        let k1 = Value::Array(vec![Value::Int(1)]).encode_key();
        let has = |addr: &str, key: &[u8]| -> bool {
            matches!(
                internode::call(
                    addr,
                    &Request::LocalGet {
                        table: "t".into(),
                        key: key.to_vec(),
                    },
                ),
                Ok(Response::Get { entry: Some(_) })
            )
        };

        // Bring c up and bootstrap its schema, then hand off the buffered hint.
        nc.serve_internode().unwrap();
        let _ = internode::call(
            &c,
            &Request::ApplyDdl {
                db: "default".into(),
                sql: "CREATE TABLE t (PRIMARY KEY (id))".into(),
            },
        )
        .unwrap();
        assert!(!has(&c, &k1), "c has no row before handoff");

        let delivered = na.flush_hints();
        assert!(delivered >= 1, "the buffered write was handed off to c");
        assert!(has(&c, &k1), "c received id=1 via hinted handoff");
    }

    #[test]
    fn read_repair_and_anti_entropy_converge_replicas() {
        use skaidb_types::Document;
        // rf=3 so every node replicates every key. We create controlled
        // divergence by writing a row to only some replicas (bypassing the
        // coordinator via a direct internode ApplyPut), then check that (a) a
        // quorum read repairs the missing replica and (b) repair() reconciles
        // both directions.
        let q = Consistency::Quorum;
        let cfg3 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: 3,
            vnodes_per_node: 64,
            read_consistency: q,
            write_consistency: q,
        };
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let m = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(Database::open(temp_dir("aea")).unwrap(), cfg3("a", &a, &m));
        let nb = Node::new(Database::open(temp_dir("aeb")).unwrap(), cfg3("b", &b, &m));
        let nc = Node::new(Database::open(temp_dir("aec")).unwrap(), cfg3("c", &c, &m));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        nc.serve_internode().unwrap();
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();

        let row = |id: i64, v: i64| -> (Vec<u8>, Vec<u8>) {
            let mut doc = Document::new();
            doc.insert("id", Value::Int(id));
            doc.insert("v", Value::Int(v));
            (
                Value::Array(vec![Value::Int(id)]).encode_key(),
                Value::Document(doc).encode(),
            )
        };
        let inject = |addr: &str, key: &[u8], val: &[u8], hlc: Hlc| {
            let r = internode::call(
                addr,
                &Request::ApplyPut {
                    table: "t".into(),
                    key: key.to_vec(),
                    value: val.to_vec(),
                    hlc,
                },
            )
            .unwrap();
            assert!(matches!(r, Response::Ack));
        };
        let has = |addr: &str, key: &[u8]| -> bool {
            matches!(
                internode::call(
                    addr,
                    &Request::LocalGet {
                        table: "t".into(),
                        key: key.to_vec(),
                    },
                ),
                Ok(Response::Get { entry: Some(_) })
            )
        };

        // (a) read-repair: write id=1 to a and b only; c is missing it.
        let (k1, v1) = row(1, 100);
        inject(&a, &k1, &v1, Hlc::new(1000, 0));
        inject(&b, &k1, &v1, Hlc::new(1000, 0));
        assert!(!has(&c, &k1), "c starts without id=1");
        // A quorum read through a touches all replicas and repairs c.
        let rs = rows(na.execute("SELECT v FROM t WHERE id = 1").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(100)]]);
        assert!(has(&c, &k1), "read-repair pushed id=1 to c");

        // (b) anti-entropy push: write id=2 to a only; repair fans it out.
        let (k2, v2) = row(2, 200);
        inject(&a, &k2, &v2, Hlc::new(2000, 0));
        assert!(!has(&b, &k2) && !has(&c, &k2));
        let fixed = na.repair().unwrap();
        assert!(fixed > 0);
        assert!(has(&b, &k2) && has(&c, &k2), "repair pushed id=2 to b and c");

        // (c) anti-entropy pull: write id=3 to c only; a's repair pulls it.
        let (k3, v3) = row(3, 300);
        inject(&c, &k3, &v3, Hlc::new(3000, 0));
        assert!(!has(&a, &k3));
        na.repair().unwrap();
        assert!(has(&a, &k3), "repair pulled id=3 onto a");
    }

    #[test]
    fn membership_persists_across_restart_and_rejects_stale_epoch() {
        let one = Consistency::One;
        let rf1 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: 1,
            vnodes_per_node: 64,
            read_consistency: one,
            write_consistency: one,
        };

        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let ab = vec![member("a", &a), member("b", &b)];
        let abc = vec![member("a", &a), member("b", &b), member("c", &c)];
        let adir = temp_dir("pm-a");
        let bdir = temp_dir("pm-b");

        let na = Node::new(Database::open(&adir).unwrap(), rf1("a", &a, &ab));
        let nb = Node::new(Database::open(&bdir).unwrap(), rf1("b", &b, &ab));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        let nc = Node::new(Database::open(temp_dir("pm-c")).unwrap(), rf1("c", &c, &abc));
        nc.serve_internode().unwrap();
        na.add_member("c", &c).unwrap();
        // A join is a two-phase transition (begin + finalize), so two epoch bumps.
        assert_eq!(na.membership_epoch(), 2);

        // Restart b from the same data dir but with the *stale* bootstrap config
        // [a, b]. It must load the persisted live ring [a, b, c] at epoch 2.
        let nb2 = Node::new(Database::open(&bdir).unwrap(), rf1("b", &b, &ab));
        assert_eq!(nb2.membership_epoch(), 2, "loaded persisted epoch");
        let mut ids = nb2.member_ids();
        ids.sort();
        assert_eq!(ids, vec!["a", "b", "c"], "loaded live ring, not stale cfg");

        // A stale SetMembership (epoch 0) is rejected — a's ring doesn't regress.
        let _ = internode::call(
            &a,
            &Request::SetMembership {
                epoch: 0,
                members: vec![("a".into(), a.clone())],
                prev_members: Vec::new(),
            },
        );
        assert_eq!(na.membership_epoch(), 2);
        assert_eq!(na.member_ids().len(), 3, "stale update ignored");
    }

    #[test]
    fn rf2_join_migrates_via_single_sender() {
        // rf=2: every key starts on both a and b. When c joins, each key c now
        // owns is sent by exactly one node (the key's primary under the {a,b}
        // ring), not both. Correctness check: every row is readable after the
        // join from every coordinator.
        let one = Consistency::One;
        let rf2 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: 2,
            vnodes_per_node: 64,
            read_consistency: one,
            write_consistency: one,
        };

        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let ab = vec![member("a", &a), member("b", &b)];
        let abc = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(Database::open(temp_dir("r2a")).unwrap(), rf2("a", &a, &ab));
        let nb = Node::new(Database::open(temp_dir("r2b")).unwrap(), rf2("b", &b, &ab));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        let n = 40;
        for i in 1..=n {
            na.execute(&format!("INSERT INTO t (id, v) VALUES ({i}, {})", i * 10))
                .unwrap();
        }

        let nc = Node::new(Database::open(temp_dir("r2c")).unwrap(), rf2("c", &c, &abc));
        nc.serve_internode().unwrap();
        na.add_member("c", &c).unwrap();

        for coord in [&na, &nb, &nc] {
            for i in 1..=n {
                let rs = rows(
                    coord
                        .execute(&format!("SELECT v FROM t WHERE id = {i}"))
                        .unwrap(),
                );
                assert_eq!(rs.rows, vec![vec![Value::Int(i * 10)]], "rf2 id {i}");
            }
        }
    }

    #[test]
    fn migrate_checkpoint_roundtrips() {
        let dir = temp_dir("ckpt");
        std::fs::create_dir_all(&dir).unwrap();
        let path = migrate_ckpt_path(&dir, &NodeId::new("node:7000"));
        save_migrate_ckpt(&path, "orders", &[0, 255, 16, 7]);
        let (table, key) = load_migrate_ckpt(&path).unwrap();
        assert_eq!(table, "orders");
        assert_eq!(key, vec![0, 255, 16, 7]);
        assert_eq!(from_hex(&to_hex(&[1, 2, 250])).unwrap(), vec![1, 2, 250]);
    }

    #[test]
    fn throttled_migration_completes_and_clears_checkpoint() {
        // A join with a tiny batch size + pause still migrates everything, and
        // the resume checkpoint is removed once migration finishes.
        let one = Consistency::One;
        let rf1 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: 1,
            vnodes_per_node: 64,
            read_consistency: one,
            write_consistency: one,
        };
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let ab = vec![member("a", &a), member("b", &b)];
        let abc = vec![member("a", &a), member("b", &b), member("c", &c)];
        let adir = temp_dir("tma");
        let bdir = temp_dir("tmb");
        let na = Node::new(Database::open(&adir).unwrap(), rf1("a", &a, &ab));
        let nb = Node::new(Database::open(&bdir).unwrap(), rf1("b", &b, &ab));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        let n = 60;
        for i in 1..=n {
            na.execute(&format!("INSERT INTO t (id, v) VALUES ({i}, {})", i * 10))
                .unwrap();
        }
        // Tiny batches with a small throttle pause on both potential senders.
        na.set_migration_throttle(7, 1);
        nb.set_migration_throttle(7, 1);

        let nc = Node::new(Database::open(temp_dir("tmc")).unwrap(), rf1("c", &c, &abc));
        nc.serve_internode().unwrap();
        na.add_member("c", &c).unwrap();

        for i in 1..=n {
            let rs = rows(nc.execute(&format!("SELECT v FROM t WHERE id = {i}")).unwrap());
            assert_eq!(rs.rows, vec![vec![Value::Int(i * 10)]], "id {i} migrated");
        }
        // The checkpoint is cleared on the former owners after a clean finish.
        let cid = NodeId::new("c");
        assert!(!migrate_ckpt_path(&adir, &cid).exists());
        assert!(!migrate_ckpt_path(&bdir, &cid).exists());
    }

    #[test]
    fn reclaim_drops_unowned_keys_after_join() {
        // rf=1: after a node joins and takes its share, the former owners still
        // hold stale copies. reclaim() physically drops the keys they no longer
        // own (once an owner confirms it holds them) without losing any data.
        let one = Consistency::One;
        let rf1 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: 1,
            vnodes_per_node: 64,
            read_consistency: one,
            write_consistency: one,
        };

        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let ab = vec![member("a", &a), member("b", &b)];
        let abc = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(Database::open(temp_dir("rca")).unwrap(), rf1("a", &a, &ab));
        let nb = Node::new(Database::open(temp_dir("rcb")).unwrap(), rf1("b", &b, &ab));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        let n = 60;
        for i in 1..=n {
            na.execute(&format!("INSERT INTO t (id, v) VALUES ({i}, {})", i * 10))
                .unwrap();
        }

        let nc = Node::new(Database::open(temp_dir("rcc")).unwrap(), rf1("c", &c, &abc));
        nc.serve_internode().unwrap();
        na.add_member("c", &c).unwrap();

        // Former owners reclaim the keys that moved onto c.
        let dropped = na.reclaim().unwrap() + nb.reclaim().unwrap() + nc.reclaim().unwrap();
        assert!(dropped > 0, "some keys moved to c and were reclaimed by a/b");

        // No data lost — every row still readable from every coordinator.
        for coord in [&na, &nb, &nc] {
            for i in 1..=n {
                let rs = rows(
                    coord
                        .execute(&format!("SELECT v FROM t WHERE id = {i}"))
                        .unwrap(),
                );
                assert_eq!(rs.rows, vec![vec![Value::Int(i * 10)]], "id {i} after reclaim");
            }
        }

        // Idempotent: a second pass drops nothing (everyone owns what they hold).
        assert_eq!(
            na.reclaim().unwrap() + nb.reclaim().unwrap() + nc.reclaim().unwrap(),
            0
        );
    }

    #[test]
    fn graceful_decommission_drains_keys_before_leaving() {
        // rf=1, CL=ONE: remove c from a 3-node cluster and confirm every key it
        // owned was drained to its new owner under the 2-node ring — every row
        // stays readable, and c is no longer routed to.
        let one = Consistency::One;
        let rf1 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: 1,
            vnodes_per_node: 64,
            read_consistency: one,
            write_consistency: one,
        };

        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let abc = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(Database::open(temp_dir("dca")).unwrap(), rf1("a", &a, &abc));
        let nb = Node::new(Database::open(temp_dir("dcb")).unwrap(), rf1("b", &b, &abc));
        let nc = Node::new(Database::open(temp_dir("dcc")).unwrap(), rf1("c", &c, &abc));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        nc.serve_internode().unwrap();

        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        let n = 60;
        for i in 1..=n {
            na.execute(&format!("INSERT INTO t (id, v) VALUES ({i}, {})", i * 10))
                .unwrap();
        }

        // Removing a non-member is a no-op; removing the last node is rejected.
        na.remove_member("ghost").unwrap();

        // Gracefully decommission c (orchestrated from a).
        na.remove_member("c").unwrap();

        // Every row is still readable from the survivors — the keys c owned were
        // drained to their new owners under the {a, b} ring before c left.
        for coord in [&na, &nb] {
            for i in 1..=n {
                let rs = rows(
                    coord
                        .execute(&format!("SELECT v FROM t WHERE id = {i}"))
                        .unwrap(),
                );
                assert_eq!(rs.rows, vec![vec![Value::Int(i * 10)]], "id {i} after drain");
            }
            let rs = rows(coord.execute("SELECT id FROM t").unwrap());
            assert_eq!(rs.rows.len(), n as usize, "no rows lost or duplicated");
        }

        // Writes after the decommission route under the 2-node ring.
        nb.execute("INSERT INTO t (id, v) VALUES (61, 610)").unwrap();
        let rs = rows(na.execute("SELECT v FROM t WHERE id = 61").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(610)]]);
    }

    #[test]
    fn cluster_tolerates_one_node_down_at_quorum() {
        let (a, b, dead) = (free_addr(), free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b), member("dead", &dead)];

        // Only A and B are served; "dead" is never started.
        let na = Node::new(
            Database::open(temp_dir("qa")).unwrap(),
            cfg("a", &a, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nb = Node::new(
            Database::open(temp_dir("qb")).unwrap(),
            cfg("b", &b, &members, Consistency::Quorum, Consistency::Quorum),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        // DDL reaches a quorum (a + b = 2 of 3).
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        // Writes meet write quorum (2 of 3 replicas ack) despite the dead node.
        na.execute("INSERT INTO t (id, v) VALUES (1, 'x'), (2, 'y')")
            .unwrap();
        // Reads meet read quorum (a + b respond).
        let rs = rows(nb.execute("SELECT id FROM t ORDER BY id").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
    }
}
