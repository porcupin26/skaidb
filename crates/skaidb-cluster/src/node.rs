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
use std::sync::{Arc, RwLock};
use std::thread;

use skaidb_engine::{filter_rows, run, Cluster, Database, EngineError, IndexScanRange, QueryOutput};
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
    /// Peer id → internode address (excludes self).
    peers: HashMap<NodeId, String>,
    /// Full membership (including self) — kept so it can be persisted/rebroadcast.
    members: Vec<(NodeId, String)>,
}

impl Topology {
    /// Build a topology at `epoch` from the full member list, excluding `self_id`
    /// from peers.
    fn from_members(
        members: &[(NodeId, String)],
        self_id: &NodeId,
        vnodes: u32,
        epoch: u64,
    ) -> Topology {
        let mut ring = Ring::new(vnodes);
        for (id, _) in members {
            ring.add_node(id.clone());
        }
        let peers = members
            .iter()
            .filter(|(id, _)| id != self_id)
            .map(|(id, addr)| (id.clone(), addr.clone()))
            .collect();
        Topology {
            epoch,
            ring,
            peers,
            members: members.to_vec(),
        }
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
    cfg: NodeConfig,
}

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
            cfg,
        })
    }

    /// The current membership epoch.
    fn current_epoch(&self) -> u64 {
        self.topo.read().expect("topo lock").epoch
    }

    /// The current membership version (for diagnostics).
    pub fn membership_epoch(&self) -> u64 {
        self.current_epoch()
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
    fn replicas_for(&self, key: &[u8]) -> Vec<NodeId> {
        self.topo
            .read()
            .expect("topo lock")
            .ring
            .replicas_for(key, self.cfg.replication_factor)
    }

    /// Address of peer `id`, if it is a current peer (snapshot, cloned).
    fn peer_addr(&self, id: &NodeId) -> Option<String> {
        self.topo.read().expect("topo lock").peers.get(id).cloned()
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
    /// backward). Persists on success. Returns whether it was applied.
    fn set_membership(&self, members: &[(NodeId, String)], epoch: u64) -> bool {
        {
            let mut topo = self.topo.write().expect("topo lock");
            if epoch <= topo.epoch && topo.epoch != 0 {
                return false; // stale / superseded
            }
            *topo = Topology::from_members(members, &self.id, self.cfg.vnodes_per_node, epoch);
        }
        self.persist_membership();
        true
    }

    /// `CREATE` statements reconstructing the local schema (for joiner bootstrap).
    fn schema_ddl(&self) -> EngineResult<Vec<String>> {
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
                if !self.replicas_for(&key).contains(joiner) {
                    continue; // the joiner does not own this key
                }
                if old_ring.primary_for(&key) != Some(self.id.clone()) {
                    continue; // not the elected sender for this key
                }
                let op = if is_put {
                    WriteOp::Put(value)
                } else {
                    WriteOp::Delete
                };
                match self.send_write(&addr, &table, &key, &op, hlc) {
                    Ok(true) => {}
                    _ => {
                        return Err(EngineError::Cluster(format!(
                            "rebalance to {joiner}: write not acked"
                        )))
                    }
                }
            }
        }
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
    /// so existing placements are otherwise undisturbed. This assumes a quiescent
    /// cluster (no concurrent writes to migrating keys) and that the joiner and a
    /// member quorum are reachable — there is no schema-log catch-up for a member
    /// that missed the broadcast yet (see [docs/RESHARDING.md]).
    pub fn add_member(&self, id: &str, addr: &str) -> EngineResult<()> {
        let joiner = NodeId::new(id);
        let mut members = self.members_snapshot();
        if members.iter().any(|(mid, _)| *mid == joiner) {
            return Ok(()); // already a member
        }
        members.push((joiner.clone(), addr.to_string()));
        let wire: Vec<(String, String)> =
            members.iter().map(|(id, a)| (id.0.clone(), a.clone())).collect();
        let epoch = self.current_epoch() + 1;

        // 1) Everyone (including the joiner) adopts the new ring at the new epoch.
        self.set_membership(&members, epoch);
        for (mid, maddr) in &members {
            if *mid == self.id {
                continue;
            }
            match self.pool.call(
                maddr,
                &Request::SetMembership {
                    epoch,
                    members: wire.clone(),
                },
            ) {
                Ok(Response::Ack) => {}
                _ if *mid == joiner => {
                    return Err(EngineError::Cluster("joiner unreachable".into()))
                }
                _ => {} // existing member lagging: best-effort (no catch-up log yet)
            }
        }

        // 2) Bootstrap the joiner's schema so it can accept migrated rows.
        for ddl in self.schema_ddl()? {
            match self.pool.call(addr, &Request::ApplyDdl { sql: ddl }) {
                Ok(Response::Ack) => {}
                Ok(Response::Err(e)) => {
                    return Err(EngineError::Cluster(format!("joiner DDL failed: {e}")))
                }
                _ => return Err(EngineError::Cluster("joiner unreachable during bootstrap".into())),
            }
        }

        // 3) Every existing member pushes the keys the joiner now owns.
        self.rebalance_to(&joiner)?;
        for (mid, maddr) in &members {
            if *mid == self.id || *mid == joiner {
                continue;
            }
            match self.pool.call(maddr, &Request::Rebalance { joiner: id.to_string() }) {
                Ok(Response::Ack) | Ok(Response::Err(_)) => {}
                _ => {} // unreachable member: its keys migrate when it rejoins
            }
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
        //    dropped from it.
        let epoch = self.current_epoch() + 1;
        if leaving != self.id {
            self.set_membership(&new_members, epoch);
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
            Request::ApplyDdl { sql } => self.with_write(|db| db.execute(&sql).map(|_| ())),
            Request::SetMembership { epoch, members } => {
                let members: Vec<(NodeId, String)> = members
                    .into_iter()
                    .map(|(id, addr)| (NodeId::new(id), addr))
                    .collect();
                self.set_membership(&members, epoch);
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

    /// Execute a SQL statement as the cluster coordinator.
    pub fn execute(self: &Arc<Self>, sql: &str) -> EngineResult<QueryOutput> {
        let stmt = parse(sql)?;
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
        if is_ddl(&stmt) {
            self.broadcast_ddl(sql)?;
            return Ok(QueryOutput::Ddl);
        }
        let mut coord = Coordinator {
            node: Arc::clone(self),
        };
        run(stmt, &mut coord)
    }

    /// Broadcast DDL to all members; require a member quorum to apply it (so a
    /// single node being down does not block schema changes). A node that missed
    /// the broadcast would need to catch up via a schema log — not yet built, so
    /// phase 1 relies on the broadcast reaching each node.
    fn broadcast_ddl(&self, sql: &str) -> EngineResult<()> {
        let mut acks = 0usize;
        // Local first.
        match self.local.write() {
            Ok(mut db) => {
                db.execute(sql)?;
                acks += 1;
            }
            Err(_) => return Err(EngineError::Cluster("local lock poisoned".into())),
        }
        for addr in &self.peer_addrs() {
            if let Ok(Response::Ack) = self.pool.call(
                addr,
                &Request::ApplyDdl {
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
    ) -> EngineResult<()> {
        let replicas = self.replicas_for(key);
        let needed = self.cfg.write_consistency.required(replicas.len().max(1));

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
        let mut async_peers: Vec<String> = Vec::new();
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
                async_peers.push(addr);
            } else if matches!(self.send_write(&addr, table, key, &op, hlc), Ok(true)) {
                acks += 1;
            }
        }

        // 3) Fold in the local replica's durable ack (joins the overlapped fsync).
        if let Some(handle) = fsync {
            if handle.join().unwrap_or(false) {
                acks += 1;
            }
        }

        // 4) Fire-and-forget the remaining replicas (eventual consistency).
        if !async_peers.is_empty() {
            let node = Arc::clone(self);
            let (table, key, op) = (table.to_string(), key.to_vec(), op.clone());
            thread::spawn(move || {
                for addr in async_peers {
                    let _ = node.send_write(&addr, &table, &key, &op, hlc);
                }
            });
        }

        if acks >= needed {
            Ok(())
        } else {
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
    fn point_get(&self, table: &str, key: &[u8]) -> EngineResult<Vec<(Vec<u8>, Document)>> {
        let replicas = self.replicas_for(key);
        let needed = self.cfg.read_consistency.required(replicas.len().max(1));
        let mut responders = 0usize;
        // Best (highest-stamped) version seen: (hlc, Some(value) | None tombstone).
        let mut best: Option<(Hlc, Option<Vec<u8>>)> = None;

        for replica in &replicas {
            let entry = if *replica == self.id {
                match self.local.read() {
                    Ok(db) => db.local_get_versioned(table, key)?,
                    Err(_) => return Err(EngineError::Cluster("local lock poisoned".into())),
                }
            } else if let Some(addr) = self.peer_addr(replica) {
                match self.pool.call(
                    &addr,
                    &Request::LocalGet {
                        table: table.to_string(),
                        key: key.to_vec(),
                    },
                ) {
                    Ok(Response::Get { entry }) => entry,
                    _ => continue, // unreachable peer: not a responder
                }
            } else {
                continue;
            };
            responders += 1;
            if let Some((value, hlc, is_put)) = entry {
                if best.as_ref().is_none_or(|(h, _)| hlc > *h) {
                    best = Some((hlc, is_put.then_some(value)));
                }
            }
        }

        if responders < needed {
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
        Ok(matches!(self.pool.call(addr, &req)?, Response::Ack))
    }

    /// Gather a table from all reachable members, merged by last-writer-wins.
    /// Tombstones participate in the merge so a delete on one replica correctly
    /// masks a stale `Put` gathered from another (quorum read ∩ quorum write).
    fn cluster_scan(&self, table: &str) -> EngineResult<Vec<(Vec<u8>, Document)>> {
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
            }
        }

        let needed = self.cfg.read_consistency.required(self.member_count());
        if responders < needed {
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
            out.extend(self.point_get(table, &key)?);
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
            let rows = filter_rows(filter, self.point_get(table.as_str(), &key)?)?;
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
#[derive(Clone)]
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
    )
}

/// The networked [`Cluster`] implementation driving `run()` on a coordinator.
struct Coordinator {
    node: Arc<Node>,
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
            let rows = self.node.point_get(table, &key)?;
            return filter_rows(filter, rows);
        }
        // Indexed non-PK predicate: push the index scan to every node to gather
        // candidate keys, then re-read each at quorum — far less data than
        // shipping every node's whole shard.
        if let Some((index, start, end)) = self.plan_index_scan(table, filter)? {
            let rows = self.node.index_lookup(table, &index, start, end)?;
            return filter_rows(filter, rows);
        }
        let rows = self.node.cluster_scan(table)?;
        filter_rows(filter, rows)
    }

    fn put(&mut self, table: &str, key: &[u8], doc: &Document) -> EngineResult<()> {
        let hlc = self.node.clock.now();
        let bytes = Value::Document(doc.clone()).encode();
        self.node.replicate(table, key, WriteOp::Put(bytes), hlc)
    }

    fn delete(&mut self, table: &str, key: &[u8], _doc: &Document) -> EngineResult<()> {
        let hlc = self.node.clock.now();
        self.node.replicate(table, key, WriteOp::Delete, hlc)
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
        assert_eq!(na.membership_epoch(), 1);

        // Restart b from the same data dir but with the *stale* bootstrap config
        // [a, b]. It must load the persisted live ring [a, b, c] at epoch 1.
        let nb2 = Node::new(Database::open(&bdir).unwrap(), rf1("b", &b, &ab));
        assert_eq!(nb2.membership_epoch(), 1, "loaded persisted epoch");
        let mut ids = nb2.member_ids();
        ids.sort();
        assert_eq!(ids, vec!["a", "b", "c"], "loaded live ring, not stale cfg");

        // A stale SetMembership (epoch 0) is rejected — a's ring doesn't regress.
        let _ = internode::call(
            &a,
            &Request::SetMembership {
                epoch: 0,
                members: vec![("a".into(), a.clone())],
            },
        );
        assert_eq!(na.membership_epoch(), 1);
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
