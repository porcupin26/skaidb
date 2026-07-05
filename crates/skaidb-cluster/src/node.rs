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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::time::Duration;
use std::thread;

use skaidb_engine::{
    filter_rows, namespace, pk_point_key, run, Cluster, Database, DbStats, EngineError,
    IndexScanRange, QueryOutput, SessionEffect, DEFAULT_DATABASE,
};
use skaidb_sql::ast::{Expr, Statement};
use skaidb_sql::parse;
use skaidb_storage::{Hlc, HlcClock, WalCommit, WalSync};
use skaidb_types::{Document, Value};

use crate::internode::{self, Pending, Request, Response};
use crate::quorum::Consistency;
use crate::ring::{NodeId, Ring};
use crate::transport::Authenticator;

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
    /// How internode connections authenticate (none / token / cert).
    pub auth: Arc<Authenticator>,
    /// Announce this node to a seed on startup so the cluster admits it
    /// automatically (auto-join). On in production; tests disable it to keep
    /// unrelated nodes from announcing.
    pub auto_join: bool,
    /// Interval (seconds) for the background anti-entropy repair loop, so missed
    /// DDL/writes converge without operator action. `0` disables it.
    pub anti_entropy_interval_secs: u64,
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
    /// Authenticates every internode connection (none / token / mTLS). Shared
    /// with the pool; also used to wrap inbound connections in the accept loop.
    auth: Arc<Authenticator>,
    /// Pooled persistent connections to peers.
    pool: internode::Pool,
    /// Writes that couldn't reach a replica (it was down), buffered per replica
    /// for replay when it comes back — *hinted handoff*. In-memory and bounded;
    /// anti-entropy ([`Node::repair`]) is the durable backstop if hints are lost.
    hints: Mutex<HashMap<NodeId, Vec<HintedWrite>>>,
    /// Highest HLC each peer has confirmed it applied (a successful replicated
    /// write or hint replay). Compared against [`Node::write_watermark`] to
    /// estimate how far behind a peer is — see [`Node::note_acked`] and the
    /// per-peer replication-lag metric. Peers absent from the map have no
    /// confirmed write yet (their lag is reported as unknown).
    acked: Mutex<HashMap<NodeId, Hlc>>,
    /// Physical-time (ms) high-water mark of data writes this node has
    /// *coordinated*. Replication lag for a peer is `watermark − acked[peer]`
    /// (both track data writes). Unlike the HLC frontier (`clock.peek()`), this advances
    /// *only* on real writes — never on reads, status probes, or observing a
    /// peer's clock — so an idle cluster reports ~0 lag instead of counting up
    /// wall-clock time since the last write.
    write_watermark: AtomicU64,
    /// Serializes membership changes coordinated by this node (`add_member` /
    /// `remove_member`), so a runtime add-node and a peer's auto-join announce
    /// can't interleave their multi-step broadcasts on the same coordinator.
    membership_lock: Mutex<()>,
    /// Queue feeding the node's background worker (hint flushes and async tail
    /// replication): one long-lived thread instead of a spawned thread per
    /// write. The worker holds only a `Weak<Node>`, so dropping the node closes
    /// this sender and the worker exits — the same lifetime the detached
    /// per-write threads had.
    bg: mpsc::SyncSender<BgTask>,
    /// Whether a [`BgTask::FlushHints`] is already queued — coalesces the
    /// per-write flush trigger into at most one outstanding task.
    hint_flush_queued: AtomicBool,
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

/// Per-peer replication state, surfaced for diagnostics and per-peer metrics.
/// One entry per node this coordinator knows of (from the live ring and/or the
/// configured seeds), excluding itself.
#[derive(Debug, Clone)]
pub struct PeerStat {
    /// Peer node id (its `host:internode_port`).
    pub id: String,
    /// Internode address (from the live ring, falling back to the configured seed).
    pub addr: String,
    /// Present in this node's configured `seeds`.
    pub in_config: bool,
    /// Present in the live membership ring (actually a routing/replication target).
    pub in_ring: bool,
    /// Hinted writes currently buffered for this peer (exact backlog).
    pub hints_pending: usize,
    /// Approximate staleness: ms between this node's HLC frontier and the latest
    /// write it has confirmed the peer applied. `None` if nothing confirmed yet.
    pub lag_ms: Option<u64>,
    /// Liveness from a probe, when one was requested. `None` if not probed.
    pub reachable: Option<bool>,
    /// The peer's own membership epoch (from a probe). `None` if not probed/reachable.
    pub reported_epoch: Option<u64>,
    /// How many members the peer believes are in the ring (from a probe).
    pub reported_members: Option<usize>,
    /// Whether the peer's own member list includes *this* node — `Some(false)`
    /// means a one-sided view (we route to it, but it doesn't know us).
    pub lists_self: Option<bool>,
    /// The peer's row count (data status, from a probe).
    pub rows: Option<u64>,
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
    /// Configured seed node ids (what membership *should* be), sorted.
    pub configured: Vec<String>,
    /// Whether this node's own id is in the live ring. `false` flags a node that
    /// is coordinating/catching-up but was never admitted (a "half-join": it
    /// pulls data via anti-entropy yet owns no ring tokens, so no one routes
    /// writes to it).
    pub self_in_ring: bool,
    /// Per-peer replication detail (ring ∪ configured, excluding self), sorted by id.
    pub peers: Vec<PeerStat>,
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

/// One batched row write `(key, value, hlc, is_put)` — the [`Response::Scan`]
/// row shape carried by [`Request::ApplyBatch`]; `is_put == false` marks a
/// tombstone (delete, empty value).
type BatchRow = (Vec<u8>, Vec<u8>, Hlc, bool);

/// Rows per anti-entropy scan page (both the local and the remote side), and
/// per flushed repair batch: bounds repair memory and each repair RPC/fsync,
/// independent of table size.
#[cfg(not(test))]
const REPAIR_PAGE_ROWS: usize = 2_000;
#[cfg(test)]
const REPAIR_PAGE_ROWS: usize = 8;

/// Rows per `ScanPage` pulled from each member during a distributed
/// full-table gather (`cluster_scan`): bounds the coordinator's transient
/// buffering to a few in-flight pages regardless of table size, instead of
/// every peer's whole shard at once.
#[cfg(not(test))]
const SCAN_PAGE_ROWS: usize = 2_000;
#[cfg(test)]
const SCAN_PAGE_ROWS: usize = 8;

/// Why reconciling one table with one peer stopped.
enum RepairPeerError {
    /// Peer predates the paged scan RPC (rolling upgrade).
    Unpaged,
    /// Peer unreachable / bad response: skip it this round.
    Unreachable,
    /// Local engine error: abort the repair pass.
    Engine(EngineError),
}

/// A paged, key-ordered stream of one side's versioned rows during repair.
struct PageCursor {
    buf: std::collections::VecDeque<BatchRow>,
    /// Key to resume after; `None` before the first page.
    last: Option<Vec<u8>>,
    /// The stream is exhausted (a page came back short).
    done: bool,
}

impl PageCursor {
    fn new() -> PageCursor {
        PageCursor {
            buf: std::collections::VecDeque::new(),
            last: None,
            done: false,
        }
    }
    fn needs_fill(&self) -> bool {
        self.buf.is_empty() && !self.done
    }
    fn after(&self) -> Option<&[u8]> {
        self.last.as_deref()
    }
    fn fill(&mut self, rows: Vec<BatchRow>) {
        if rows.len() < REPAIR_PAGE_ROWS {
            self.done = true; // short page: final one
        }
        if let Some((k, _, _, _)) = rows.last() {
            self.last = Some(k.clone());
        }
        self.buf = rows.into();
    }
    fn head(&self) -> Option<&BatchRow> {
        self.buf.front()
    }
    fn pop(&mut self) -> BatchRow {
        self.buf.pop_front().expect("popped an empty repair cursor")
    }
}

/// Work items for the node's background worker thread: deferred, best-effort
/// replication work that used to spawn a fresh thread per write.
#[derive(Debug)]
enum BgTask {
    /// Replay buffered hints ([`Node::flush_hints`]). Coalesced via
    /// `Node::hint_flush_queued` so at most one is queued at a time.
    FlushHints,
    /// Replicate one write to the replicas beyond the quorum (the async tail),
    /// hinting any that are unreachable.
    Replicate {
        peers: Vec<(NodeId, String)>,
        table: String,
        key: Vec<u8>,
        op: WriteOp,
        hlc: Hlc,
    },
    /// Replicate a statement's batch to the replicas beyond the quorum (the
    /// async tail), hinting every row for any replica that is unreachable.
    ReplicateBatch {
        peers: Vec<(NodeId, String)>,
        table: String,
        rows: Vec<BatchRow>,
    },
}

/// Cap on buffered hints per replica, so a long outage can't grow unboundedly
/// (anti-entropy reconciles whatever overflows).
const MAX_HINTS_PER_REPLICA: usize = 4096;

/// Max deferred tasks the background worker drains per wakeup: bounds the
/// size of one batched send while still collapsing a burst of async-tail
/// replication into few round-trips.
const BG_BATCH_MAX: usize = 256;

/// Cap on queued background tasks. The queue holds cloned rows, so an
/// unbounded queue turns a sustained ingest that outruns the async tail into
/// unbounded coordinator memory (observed: hundreds of MB on a 512 MB node).
/// When the queue is full the tail rows become hints instead — bounded
/// memory, reconciled by hint replay and anti-entropy.
const BG_QUEUE_MAX: usize = 1024;

/// Max rows shipped per background `ApplyBatch` frame. The worker merges its
/// whole drain per peer; unchunked, that reached ~128k rows (~17 MB) in one
/// frame — one multi-second lock hold and fsync on the receiving replica, and
/// transient multi-MB buffers on both ends. Chunking keeps the tail pipeline
/// in small, steady round-trips.
const BG_SEND_MAX_ROWS: usize = 2_000;

/// Startup catch-up / self-announce: how many times to wait for a peer to become
/// reachable before giving up, and how long between attempts. The interval is
/// shortened under test so background threads reach their exit condition (and
/// release their node) quickly instead of lingering across other tests.
const CATCH_UP_ATTEMPTS: usize = 15;
#[cfg(not(test))]
const CATCH_UP_DELAY: Duration = Duration::from_secs(2);
#[cfg(test)]
const CATCH_UP_DELAY: Duration = Duration::from_millis(120);

/// Per-peer connect+round-trip budget for liveness probes in `/admin/status`.
const PROBE_TIMEOUT: Duration = Duration::from_millis(800);

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
        let auth = cfg.auth.clone();
        let (bg, bg_rx) = mpsc::sync_channel(BG_QUEUE_MAX);
        let node = Arc::new(Node {
            id: cfg.id.clone(),
            local: RwLock::new(local),
            topo: RwLock::new(topo),
            clock: HlcClock::new(),
            auth: auth.clone(),
            pool: internode::Pool::new(auth),
            hints: Mutex::new(HashMap::new()),
            acked: Mutex::new(HashMap::new()),
            write_watermark: AtomicU64::new(0),
            membership_lock: Mutex::new(()),
            bg,
            hint_flush_queued: AtomicBool::new(false),
            migration_batch: AtomicUsize::new(1024),
            migration_pause_ms: AtomicU64::new(0),
            counters: Counters::default(),
            cfg,
        });
        // The background worker holds only a `Weak`, so it can't keep the node
        // alive; it exits when the node is dropped (the sender closes).
        let weak = Arc::downgrade(&node);
        thread::spawn(move || {
            while let Ok(task) = bg_rx.recv() {
                let Some(node) = weak.upgrade() else { return };
                // Drain whatever else queued up while the worker was busy, so
                // a burst of async-tail replication collapses into batched
                // sends (one `ApplyBatch` per peer) instead of one blocking
                // round-trip per write.
                let mut tasks = vec![task];
                while tasks.len() < BG_BATCH_MAX {
                    match bg_rx.try_recv() {
                        Ok(t) => tasks.push(t),
                        Err(_) => break,
                    }
                }
                node.run_bg_tasks(tasks);
            }
        });
        node
    }

    /// Execute a drained batch of deferred tasks on the background worker
    /// thread. Replication tasks are regrouped by peer and table so a burst
    /// of async-tail writes reaches each replica as a few `ApplyBatch`
    /// round-trips (one lock + one fsync per batch on the peer) instead of
    /// one blocking round-trip per write; hint flushes run once at the end,
    /// after any freshly-failed sends have buffered their hints.
    fn run_bg_tasks(&self, tasks: Vec<BgTask>) {
        if tasks.len() == 1 && !matches!(tasks[0], BgTask::FlushHints) {
            let mut tasks = tasks;
            return self.run_bg_task(tasks.pop().expect("non-empty"));
        }
        let mut flush = false;
        // Rows per (replica, addr), grouped per table in arrival order.
        type PeerBatches = Vec<(String, Vec<BatchRow>)>;
        let mut by_peer: Vec<((NodeId, String), PeerBatches)> = Vec::new();
        let mut add = |peer: &(NodeId, String), table: &str, rows: &[BatchRow]| {
            let idx = match by_peer.iter().position(|(p, _)| p == peer) {
                Some(i) => i,
                None => {
                    by_peer.push((peer.clone(), Vec::new()));
                    by_peer.len() - 1
                }
            };
            let tables = &mut by_peer[idx].1;
            match tables.iter_mut().find(|(t, _)| t == table) {
                Some((_, batch)) => batch.extend_from_slice(rows),
                None => tables.push((table.to_string(), rows.to_vec())),
            }
        };
        for task in &tasks {
            match task {
                BgTask::FlushHints => flush = true,
                BgTask::Replicate {
                    peers,
                    table,
                    key,
                    op,
                    hlc,
                } => {
                    let row: BatchRow = match op {
                        WriteOp::Put(bytes) => (key.clone(), bytes.clone(), *hlc, true),
                        WriteOp::Delete => (key.clone(), Vec::new(), *hlc, false),
                    };
                    for peer in peers {
                        add(peer, table, std::slice::from_ref(&row));
                    }
                }
                BgTask::ReplicateBatch { peers, table, rows } => {
                    for peer in peers {
                        add(peer, table, rows);
                    }
                }
            }
        }
        for ((replica, addr), tables) in by_peer {
            for (table, rows) in tables {
                // Chunked sends: bounded frames, bounded lock holds and fsyncs
                // on the receiving replica (see BG_SEND_MAX_ROWS).
                for chunk in rows.chunks(BG_SEND_MAX_ROWS) {
                    if self.send_batch(&addr, &table, chunk) {
                        if let Some((_, _, hlc, _)) = chunk.last() {
                            self.note_acked(&replica, *hlc);
                        }
                    } else {
                        self.hint_batch(&replica, &table, chunk);
                    }
                }
            }
        }
        if flush {
            self.hint_flush_queued.store(false, Ordering::Release);
            self.flush_hints();
        }
    }

    /// Execute one deferred task on the node's background worker thread.
    fn run_bg_task(&self, task: BgTask) {
        match task {
            BgTask::FlushHints => {
                // Clear the coalescing flag *before* flushing, so a write that
                // lands mid-flush can queue a fresh pass.
                self.hint_flush_queued.store(false, Ordering::Release);
                self.flush_hints();
            }
            BgTask::Replicate {
                peers,
                table,
                key,
                op,
                hlc,
            } => {
                for (replica, addr) in peers {
                    if matches!(self.send_write(&addr, &table, &key, &op, hlc), Ok(true)) {
                        self.note_acked(&replica, hlc);
                    } else {
                        self.store_hint(&replica, &table, &key, &op, hlc);
                    }
                }
            }
            BgTask::ReplicateBatch { peers, table, rows } => {
                for (replica, addr) in peers {
                    if self.send_batch(&addr, &table, &rows) {
                        if let Some((_, _, hlc, _)) = rows.last() {
                            self.note_acked(&replica, *hlc);
                        }
                    } else {
                        self.hint_batch(&replica, &table, &rows);
                    }
                }
            }
        }
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
        let (epoch, members, resharding, self_in_ring) = {
            let topo = self.topo.read().expect("topo lock");
            let self_in_ring = topo.members.iter().any(|(id, _)| *id == self.id);
            (
                topo.epoch,
                topo.peers.len() + 1,
                topo.prev.is_some(),
                self_in_ring,
            )
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
            configured: self.configured_ids(),
            self_in_ring,
            peers: self.peer_stats(false),
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

    /// Record that `peer` has confirmed a write stamped `hlc` (a successful
    /// replicated write or hint replay). Monotonic — only advances. Feeds the
    /// per-peer replication-lag estimate.
    fn note_acked(&self, peer: &NodeId, hlc: Hlc) {
        let mut acked = self.acked.lock().expect("acked lock");
        let slot = acked.entry(peer.clone()).or_insert(Hlc::MIN);
        if hlc > *slot {
            *slot = hlc;
        }
    }

    /// Advance the coordinated-write high-water mark (monotonic). Called once per
    /// write this node coordinates, so per-peer lag measures replication backlog
    /// rather than idle wall-clock time. See [`Node::write_watermark`].
    fn note_local_write(&self, hlc: Hlc) {
        self.write_watermark.fetch_max(hlc.physical, Ordering::Relaxed);
    }

    /// Configured seed node ids (what membership is *supposed* to be), sorted.
    fn configured_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.cfg.members.iter().map(|(id, _)| id.0.clone()).collect();
        ids.sort();
        ids.dedup();
        ids
    }

    /// Per-peer replication detail: the union of the live ring's peers and the
    /// configured seeds (excluding self), each flagged with whether it is in the
    /// config, in the live ring, its hint backlog, and an estimated lag. When
    /// `probe` is set, each peer is pinged (short timeout) for liveness — used by
    /// the operator-facing `/admin/status`, skipped on the cheap metrics path.
    fn peer_stats(&self, probe: bool) -> Vec<PeerStat> {
        // Candidate set: ring peers (id -> addr) plus configured seeds, minus self.
        let mut addrs: BTreeMap<NodeId, (String, bool, bool)> = BTreeMap::new();
        {
            let topo = self.topo.read().expect("topo lock");
            for (id, addr) in &topo.peers {
                addrs.insert(id.clone(), (addr.clone(), false, true));
            }
        }
        for (id, addr) in &self.cfg.members {
            if *id == self.id {
                continue;
            }
            let entry = addrs.entry(id.clone()).or_insert((addr.clone(), false, false));
            entry.1 = true; // in_config
        }

        // Measure lag against the coordinated-write watermark, not the HLC
        // frontier: the frontier also advances on reads, probes and observed
        // peer clocks, so on an idle cluster it would race ahead of `acked` and
        // report ever-growing phantom lag. The watermark only moves on real
        // writes, so a fully-replicated idle cluster reports ~0.
        let watermark = self.write_watermark.load(Ordering::Relaxed);
        let acked = self.acked.lock().expect("acked lock");
        let hints = self.hints.lock().expect("hints lock");

        let self_id = self.id.0.clone();
        addrs
            .into_iter()
            .map(|(id, (addr, in_config, in_ring))| {
                let hints_pending = hints.get(&id).map(|v| v.len()).unwrap_or(0);
                let lag_ms = acked
                    .get(&id)
                    .map(|h| watermark.saturating_sub(h.physical));
                // One probe per peer fetches liveness *and* the peer's own
                // membership view + data status, so we can flag cross-node
                // disagreement (a peer that doesn't list us, or a different
                // member count) — the failure mode static seeds can't reveal.
                let mut reachable = None;
                let (mut reported_epoch, mut reported_members, mut lists_self, mut rows) =
                    (None, None, None, None);
                if probe {
                    match self.pool.call_timeout(&addr, &Request::NodeStatus, PROBE_TIMEOUT) {
                        Ok(Response::NodeStatus {
                            epoch,
                            members,
                            rows: r,
                            ..
                        }) => {
                            reachable = Some(true);
                            reported_epoch = Some(epoch);
                            reported_members = Some(members.len());
                            lists_self = Some(members.contains(&self_id));
                            rows = Some(r);
                        }
                        _ => reachable = Some(false),
                    }
                }
                PeerStat {
                    id: id.0,
                    addr,
                    in_config,
                    in_ring,
                    hints_pending,
                    lag_ms,
                    reachable,
                    reported_epoch,
                    reported_members,
                    lists_self,
                    rows,
                }
            })
            .collect()
    }

    /// An operator-facing snapshot that probes each peer for liveness. Heavier
    /// than [`Node::stats`] (one ping per peer), so it backs the explicit
    /// `/admin/status` call rather than the metrics scrape.
    pub fn peer_stats_probed(&self) -> Vec<PeerStat> {
        self.peer_stats(true)
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
    /// Chunk-level migration of time-series stores is not built yet
    /// (docs/TODO.md phase 5) — joins/decommissions would strand or lose
    /// series, so they are refused while any TS table exists.
    fn reject_if_timeseries(&self, what: &str) -> EngineResult<()> {
        let has = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .has_timeseries_tables();
        if has {
            return Err(EngineError::Unsupported(format!(
                "{what} are not yet supported while time-series tables exist (drop them first)"
            )));
        }
        Ok(())
    }

    /// Gather matching time-series samples from every member and union-merge
    /// per series (samples are immutable facts keyed by timestamp, so the
    /// union is the authoritative view — a replica that missed a write is
    /// covered by any responder that has it). Requires the read-consistency
    /// number of responders.
    fn ts_scatter(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
        t0: i64,
        t1: i64,
        oc: Option<Consistency>,
    ) -> EngineResult<Vec<(skaidb_tsdb::Labels, Vec<skaidb_tsdb::Sample>)>> {
        let mut merged: BTreeMap<skaidb_tsdb::Labels, BTreeMap<i64, f64>> = BTreeMap::new();
        let mut responders = 0usize;
        {
            let db = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
            for (labels, samples) in db.ts_query(table, matchers, t0, t1)? {
                let entry = merged.entry(labels).or_default();
                for s in samples {
                    entry.insert(s.ts, s.value);
                }
            }
            responders += 1;
        }
        let wire_matchers: Vec<(bool, String, String)> = matchers
            .iter()
            .map(|m| match m {
                skaidb_tsdb::Matcher::Eq(k, v) => (false, k.clone(), v.clone()),
                skaidb_tsdb::Matcher::Ne(k, v) => (true, k.clone(), v.clone()),
            })
            .collect();
        let addrs = self.peer_addrs();
        let shards = scatter(&addrs, |addr| {
            self.counters.peer_requests.fetch_add(1, Ordering::Relaxed);
            match self.pool.call(
                addr,
                &Request::TsQuery {
                    table: table.to_string(),
                    matchers: wire_matchers.clone(),
                    t0,
                    t1,
                },
            ) {
                Ok(Response::TsSeries { series }) => Some(series),
                _ => {
                    self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                    None
                }
            }
        });
        for shard in shards.into_iter().flatten() {
            for (labels, samples) in shard {
                let entry = merged.entry(labels).or_default();
                for (ts, value) in samples {
                    entry.insert(ts, value);
                }
            }
            responders += 1;
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
        Ok(merged
            .into_iter()
            .map(|(labels, samples)| {
                (
                    labels,
                    samples
                        .into_iter()
                        .map(|(ts, value)| skaidb_tsdb::Sample { ts, value })
                        .collect(),
                )
            })
            .collect())
    }

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

    /// Versioned schema — live objects as CREATEs, dropped ones as DROPs, each
    /// with its HLC — for last-writer-wins reconciliation and joiner bootstrap.
    fn schema_sync(&self) -> EngineResult<Vec<(String, String, Hlc)>> {
        Ok(self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .schema_sync())
    }

    /// Bidirectionally converge the catalog with the peer at `addr`: pull the
    /// peer's versioned schema and apply it locally, then push the local schema so
    /// the peer converges too. Each statement carries its HLC and is applied under
    /// last-writer-wins, so creates *and drops* propagate without a stale node
    /// resurrecting a dropped object. Best-effort: an unreachable peer or an
    /// individual statement error is skipped rather than failing the pass.
    fn sync_schema_with(&self, addr: &str) {
        // Pull: apply the peer's schema (creates and drops) locally, LWW.
        if let Ok(Response::Schema { entries }) = self.pool.call(addr, &Request::SchemaDdl) {
            for (db, sql, hlc) in entries {
                if let Ok(mut d) = self.local.write() {
                    let _ = d.execute_session_with_hlc(&db, &sql, hlc);
                }
            }
        }
        // Push: send the local schema (with versions) so the peer converges too.
        if let Ok(mine) = self.schema_sync() {
            for (db, sql, hlc) in mine {
                let _ = self.pool.call(addr, &Request::ApplyDdl { db, sql, hlc });
            }
        }
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

            // Stream in throttled batches — each chunk is one ApplyBatch RPC
            // (one round-trip + one fsync on the joiner, not one per row) —
            // checkpointing after each.
            for chunk in pending.chunks(batch) {
                if !self.send_batch(&addr, &table, chunk) {
                    return Err(EngineError::Cluster(format!(
                        "rebalance to {joiner}: batch not acked"
                    )));
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
        let batch = self.migration_batch.load(Ordering::Relaxed).max(1);

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
            // Group rows by their *new* owner so each destination receives
            // chunked ApplyBatch RPCs (one round-trip + one fsync per chunk)
            // instead of a round-trip per row.
            let mut per_dest: BTreeMap<NodeId, Vec<BatchRow>> = BTreeMap::new();
            for (key, value, hlc, is_put) in rows {
                let old = self.replicas_for(&key); // current ring (includes self)
                for replica in new_ring.replicas_for(&key, rf) {
                    if old.contains(&replica) {
                        continue; // that node already holds this row
                    }
                    if !addr_of.contains_key(&replica) {
                        continue;
                    }
                    per_dest
                        .entry(replica)
                        .or_default()
                        .push((key.clone(), value.clone(), hlc, is_put));
                }
            }
            for (replica, dest_rows) in per_dest {
                let addr = &addr_of[&replica];
                for chunk in dest_rows.chunks(batch) {
                    if !self.send_batch(addr, &table, chunk) {
                        return Err(EngineError::Cluster(format!(
                            "drain: write to {replica} not acked"
                        )));
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
        self.reject_if_timeseries("topology changes")?;
        let _guard = self.membership_lock.lock().expect("membership lock");
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

        // 2) Bootstrap the joiner's schema (databases + tables + indexes, with
        //    versions) so it can accept migrated rows and converge under LWW.
        for (db, ddl, hlc) in self.schema_sync()? {
            match self.pool.call(addr, &Request::ApplyDdl { db, sql: ddl, hlc }) {
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
        self.reject_if_timeseries("topology changes")?;
        let _guard = self.membership_lock.lock().expect("membership lock");
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
        // Converge the catalog first (databases/tables/indexes), both directions,
        // so a node that missed a DDL broadcast learns it — and so the data pass
        // below sees any newly-created tables. Schema DDL is idempotent
        // (`CREATE ... IF NOT EXISTS`), so this is safe to run repeatedly.
        for (_pid, addr) in self.peers_with_ids() {
            self.sync_schema_with(&addr);
        }
        let tables = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .table_names();

        // Reconcile each (table, peer) pair as a merge-join over two paged,
        // key-ordered streams — memory stays O(page) regardless of table size
        // (an unpaged pass once pulled a whole 1M-row table into RAM on both
        // ends and OOM-killed 512 MB nodes). Writes that land between pages
        // are simply seen by the next anti-entropy pass.
        for table in tables {
            for (pid, addr) in self.peers_with_ids() {
                match self.repair_table_with(&table, &pid, &addr) {
                    Ok(n) => repaired += n,
                    Err(RepairPeerError::Unpaged) => {
                        // Rolling upgrade: the peer predates ScanPage. Fall
                        // back to the old whole-table pull for it.
                        repaired += self.repair_table_unpaged(&table, &pid, &addr);
                    }
                    Err(RepairPeerError::Unreachable) => continue,
                    Err(RepairPeerError::Engine(e)) => return Err(e),
                }
            }
        }
        Ok(repaired)
    }

    /// Reconcile one table with one peer by merge-joining paged key-ordered
    /// scans of both sides: pull remote-newer rows (for keys we replicate),
    /// push local-newer rows (for keys the peer replicates), page by page.
    fn repair_table_with(
        &self,
        table: &str,
        pid: &NodeId,
        addr: &str,
    ) -> Result<usize, RepairPeerError> {
        let mut repaired = 0usize;
        let mut local: PageCursor = PageCursor::new();
        let mut remote: PageCursor = PageCursor::new();
        let mut pull: Vec<BatchRow> = Vec::new();
        let mut push: Vec<BatchRow> = Vec::new();
        loop {
            // Refill whichever side ran dry (both start empty).
            if local.needs_fill() {
                let rows = self
                    .local
                    .read()
                    .map_err(|_| {
                        RepairPeerError::Engine(EngineError::Cluster("local lock poisoned".into()))
                    })?
                    .local_scan_versioned_page(table, local.after(), REPAIR_PAGE_ROWS)
                    .map_err(RepairPeerError::Engine)?;
                local.fill(rows);
            }
            if remote.needs_fill() {
                let resp = self.pool.call(
                    addr,
                    &Request::ScanPage {
                        table: table.to_string(),
                        after: remote.after().map(<[u8]>::to_vec),
                        limit: REPAIR_PAGE_ROWS as u32,
                    },
                );
                match resp {
                    Ok(Response::Scan { rows }) => remote.fill(rows),
                    Ok(Response::Err(e)) if e.contains("unknown request op") => {
                        return Err(RepairPeerError::Unpaged)
                    }
                    _ => return Err(RepairPeerError::Unreachable),
                }
            }

            // Merge step: advance the smaller key; equal keys resolve by LWW.
            match (local.head(), remote.head()) {
                (None, None) => break,
                (Some(_), None) => {
                    let (key, value, hlc, is_put) = local.pop();
                    if self.replicas_for(&key).contains(pid) {
                        push.push((key, value, hlc, is_put));
                    }
                }
                (None, Some(_)) => {
                    let (key, value, hlc, is_put) = remote.pop();
                    if self.replicas_for(&key).contains(&self.id) {
                        pull.push((key, value, hlc, is_put));
                    }
                }
                (Some(l), Some(r)) => match l.0.cmp(&r.0) {
                    std::cmp::Ordering::Less => {
                        let (key, value, hlc, is_put) = local.pop();
                        if self.replicas_for(&key).contains(pid) {
                            push.push((key, value, hlc, is_put));
                        }
                    }
                    std::cmp::Ordering::Greater => {
                        let (key, value, hlc, is_put) = remote.pop();
                        if self.replicas_for(&key).contains(&self.id) {
                            pull.push((key, value, hlc, is_put));
                        }
                    }
                    std::cmp::Ordering::Equal => {
                        let lh = l.2;
                        let rh = r.2;
                        let lrow = local.pop();
                        let rrow = remote.pop();
                        match lh.cmp(&rh) {
                            std::cmp::Ordering::Greater
                                if self.replicas_for(&lrow.0).contains(pid) =>
                            {
                                push.push(lrow);
                            }
                            std::cmp::Ordering::Less
                                if self.replicas_for(&rrow.0).contains(&self.id) =>
                            {
                                pull.push(rrow);
                            }
                            _ => {}
                        }
                    }
                },
            }

            // Flush per page so the repair batches stay bounded too.
            if pull.len() >= REPAIR_PAGE_ROWS {
                repaired += self.repair_apply_pull(table, &mut pull);
            }
            if push.len() >= REPAIR_PAGE_ROWS {
                repaired += self.repair_send_push(table, addr, &mut push);
            }
        }
        repaired += self.repair_apply_pull(table, &mut pull);
        repaired += self.repair_send_push(table, addr, &mut push);
        Ok(repaired)
    }

    /// Apply a pulled repair batch locally (one lock + one fsync).
    fn repair_apply_pull(&self, table: &str, pull: &mut Vec<BatchRow>) -> usize {
        if pull.is_empty() {
            return 0;
        }
        let n = pull.len();
        let ok = self.apply_batch_local(table, pull).is_ok();
        pull.clear();
        if ok {
            n
        } else {
            0
        }
    }

    /// Ship a pushed repair batch to the peer (one round-trip + one fsync there).
    fn repair_send_push(&self, table: &str, addr: &str, push: &mut Vec<BatchRow>) -> usize {
        if push.is_empty() {
            return 0;
        }
        let n = push.len();
        let ok = self.send_batch(addr, table, push);
        push.clear();
        if ok {
            n
        } else {
            0
        }
    }

    /// Pre-`ScanPage` fallback: reconcile one table with one old peer by
    /// pulling its whole shard in one response — O(table) memory, kept only
    /// for rolling upgrades against peers that don't know the paged scan.
    fn repair_table_unpaged(&self, table: &str, pid: &NodeId, addr: &str) -> usize {
        let mut repaired = 0usize;
        let Ok(local_rows) = self
            .local
            .read()
            .map_err(|_| ())
            .and_then(|db| db.local_scan_versioned_with_tombstones(table).map_err(|_| ()))
        else {
            return 0;
        };
        let local_map: HashMap<Vec<u8>, (Hlc, bool, Vec<u8>)> = local_rows
            .into_iter()
            .map(|(k, v, h, is_put)| (k, (h, is_put, v)))
            .collect();
        let remote = match self.pool.call(
            addr,
            &Request::LocalScan {
                table: table.to_string(),
            },
        ) {
            Ok(Response::Scan { rows }) => rows,
            _ => return 0, // unreachable peer: skip this round
        };
        let mut remote_hlc: HashMap<Vec<u8>, Hlc> = HashMap::new();
        let mut pull: Vec<BatchRow> = Vec::new();
        for (key, value, hlc, is_put) in &remote {
            remote_hlc.insert(key.clone(), *hlc);
            if !self.replicas_for(key).contains(&self.id) {
                continue;
            }
            if local_map.get(key).is_some_and(|(lh, _, _)| *lh >= *hlc) {
                continue;
            }
            pull.push((key.clone(), value.clone(), *hlc, *is_put));
        }
        if !pull.is_empty() && self.apply_batch_local(table, &pull).is_ok() {
            repaired += pull.len();
        }
        let mut push: Vec<BatchRow> = Vec::new();
        for (key, (lh, is_put, value)) in &local_map {
            if !self.replicas_for(key).contains(pid) {
                continue;
            }
            if remote_hlc.get(key).is_some_and(|rh| rh >= lh) {
                continue;
            }
            push.push((key.clone(), value.clone(), *lh, *is_put));
        }
        if !push.is_empty() && self.send_batch(addr, table, &push) {
            repaired += push.len();
        }
        repaired
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
            // Group this replica's hints by table so each group replays as one
            // ApplyBatch RPC (one round-trip + one fsync there). LWW makes the
            // reordering across tables/keys safe.
            let mut by_table: BTreeMap<String, Vec<HintedWrite>> = BTreeMap::new();
            for hint in writes {
                by_table.entry(hint.0.clone()).or_default().push(hint);
            }
            let mut remaining = Vec::new();
            for (table, hints) in by_table {
                let rows: Vec<BatchRow> = hints
                    .iter()
                    .map(|(_, key, op, hlc)| match op {
                        WriteOp::Put(v) => (key.clone(), v.clone(), *hlc, true),
                        WriteOp::Delete => (key.clone(), Vec::new(), *hlc, false),
                    })
                    .collect();
                if self.send_batch(&addr, &table, &rows) {
                    delivered += hints.len();
                    if let Some(max) = hints.iter().map(|(_, _, _, hlc)| *hlc).max() {
                        self.note_acked(&replica, max);
                    }
                } else {
                    remaining.extend(hints);
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
        // Announce ourselves so the cluster admits us into the live ring (so
        // peers route writes to us), then catch up on data. Both run in the
        // background so startup isn't blocked; both are no-ops when standalone.
        if self.cfg.auto_join {
            let node = Arc::clone(self);
            thread::spawn(move || node.announce_self());
        }
        let node = Arc::clone(self);
        thread::spawn(move || node.startup_catch_up());
        // Continuous anti-entropy: periodically reconcile so a missed broadcast
        // (e.g. a DDL that committed at quorum while this node was momentarily
        // behind) converges on its own, without an operator running repair.
        if self.cfg.anti_entropy_interval_secs > 0 {
            let node = Arc::clone(self);
            thread::spawn(move || node.anti_entropy_loop());
        }
        Ok(())
    }

    /// Background anti-entropy: every `anti_entropy_interval_secs` run a full
    /// repair pass. Staggered per-node so the cluster doesn't repair in lockstep.
    fn anti_entropy_loop(self: Arc<Self>) {
        let interval = Duration::from_secs(self.cfg.anti_entropy_interval_secs);
        // Stagger the first pass by a node-derived fraction of the interval.
        let stagger = fnv1a(self.id.0.as_bytes()) % self.cfg.anti_entropy_interval_secs.max(1);
        thread::sleep(Duration::from_secs(stagger));
        loop {
            thread::sleep(interval);
            if self.member_count() <= 1 {
                continue; // standalone: nothing to reconcile
            }
            match self.repair() {
                Ok(n) if n > 0 => skaidb_types::slog!("skaidb: anti-entropy reconciled {n} rows"),
                Ok(_) => {}                  // already converged: stay quiet
                Err(e) => skaidb_types::slog!("skaidb: anti-entropy pass failed: {e}"),
            }
        }
    }

    /// Announce this node to a seed so the cluster admits it into the live ring
    /// (auto-join). A node that has never been admitted (epoch 0) and has other
    /// seeds to reach asks one of them to [`Node::add_member`] it; the seed
    /// broadcasts the new membership to every node. Idempotent: if this node is
    /// already in the seed's ring (symmetric seeds), the seed's `add_member` is a
    /// no-op. Skipped for a standalone / sole bootstrap node (no other seeds).
    fn announce_self(self: Arc<Self>) {
        if self.current_epoch() > 0 {
            return; // already admitted via a prior membership broadcast
        }
        let self_id = self.id.clone();
        let seeds: Vec<String> = self
            .cfg
            .members
            .iter()
            .filter(|(id, _)| *id != self_id)
            .map(|(_, addr)| addr.clone())
            .collect();
        if seeds.is_empty() {
            return; // standalone, or the sole bootstrap node
        }
        let rf = self.cfg.replication_factor as u32;
        for _ in 0..CATCH_UP_ATTEMPTS {
            thread::sleep(CATCH_UP_DELAY);
            if self.current_epoch() > 0 {
                return; // someone admitted us in the meantime
            }
            for addr in &seeds {
                // Ask the seed for its view first: if it already lists us
                // (symmetric seeds — the common case), we're known and there's
                // nothing to do. Only announce when a reachable seed doesn't
                // know us, so a well-formed cluster never mutates membership.
                match self.pool.call(addr, &Request::NodeStatus) {
                    Ok(Response::NodeStatus { members, .. }) if members.contains(&self_id.0) => {
                        return; // already a member of this peer's ring
                    }
                    Ok(Response::NodeStatus { .. }) => {} // reachable but doesn't know us → announce
                    _ => continue,                        // unreachable: try the next seed
                }
                let req = Request::Announce {
                    id: self_id.0.clone(),
                    addr: self.cfg.internode_addr.clone(),
                    rf,
                };
                match self.pool.call(addr, &req) {
                    Ok(Response::Ack) => {
                        skaidb_types::slog!("skaidb: announced to {addr}; admitted to the cluster");
                        return;
                    }
                    Ok(Response::Err(e)) => {
                        // Terminal (e.g. replication-factor mismatch) — retrying
                        // won't help; the operator must fix the config.
                        skaidb_types::slog!("skaidb: announce rejected by {addr}: {e}");
                        return;
                    }
                    _ => continue, // unreachable peer: try the next seed
                }
            }
        }
    }

    /// Background catch-up after a (re)join: wait for a peer to come up, then run
    /// a full anti-entropy pass (schema + data) so a node that missed DDL or
    /// writes while it was down converges automatically.
    fn startup_catch_up(self: Arc<Self>) {
        if self.member_count() <= 1 {
            return; // standalone: nothing to catch up from
        }
        for attempt in 1..=CATCH_UP_ATTEMPTS {
            thread::sleep(CATCH_UP_DELAY);
            let peer_up = self
                .peer_addrs()
                .iter()
                .any(|a| matches!(self.pool.call(a, &Request::Ping), Ok(Response::Pong)));
            if !peer_up {
                continue; // no peer yet — keep waiting
            }
            match self.repair() {
                Ok(n) => {
                    skaidb_types::slog!("skaidb: startup catch-up complete ({n} rows reconciled)");
                    return;
                }
                Err(e) => skaidb_types::slog!("skaidb: startup catch-up attempt {attempt} failed: {e}"),
            }
        }
    }

    fn handle_internode(&self, stream: TcpStream) {
        // Authenticate the connection (token challenge / mTLS handshake / none)
        // before serving any RPC. A peer that can't satisfy the configured mode
        // is dropped here. (`accept` also disables Nagle on the socket.)
        let mut conn = match self.auth.accept(stream) {
            Ok(c) => c,
            Err(_) => return, // failed auth: drop silently
        };
        loop {
            // Frame buffers live on the Conn and are reused across requests,
            // so steady-state a request/response pair allocates nothing in
            // the framing layer.
            let response = match internode::conn_recv_request(&mut conn) {
                Ok(Ok(req)) => self.apply_local(req),
                // Undecodable but fully-read frame: answer and keep serving
                // (a newer peer's unknown op lands here — rolling upgrades).
                Ok(Err(e)) => Response::Err(e.to_string()),
                Err(_) => return, // disconnect or framing error
            };
            if internode::conn_send(&mut conn, |o| response.encode_into(o)).is_err() {
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
            Request::ScanPage { table, after, limit } => match self.local.read() {
                Ok(db) => {
                    match db.local_scan_versioned_page(&table, after.as_deref(), limit as usize) {
                        Ok(rows) => Response::Scan { rows },
                        Err(e) => Response::Err(e.to_string()),
                    }
                }
                Err(_) => Response::Err("local lock poisoned".into()),
            },
            Request::TsAppend { table, rows } => match self.local.read() {
                Ok(db) => match db.ts_append(&table, &rows) {
                    Ok(_) => Response::Ack,
                    Err(e) => Response::Err(e.to_string()),
                },
                Err(_) => Response::Err("local lock poisoned".into()),
            },
            Request::TsQuery {
                table,
                matchers,
                t0,
                t1,
            } => match self.local.read() {
                Ok(db) => {
                    let matchers: Vec<skaidb_tsdb::Matcher> = matchers
                        .into_iter()
                        .map(|(negated, k, v)| {
                            if negated {
                                skaidb_tsdb::Matcher::Ne(k, v)
                            } else {
                                skaidb_tsdb::Matcher::Eq(k, v)
                            }
                        })
                        .collect();
                    match db.ts_query(&table, &matchers, t0, t1) {
                        Ok(series) => Response::TsSeries {
                            series: series
                                .into_iter()
                                .map(|(labels, samples)| {
                                    (
                                        labels,
                                        samples.into_iter().map(|s| (s.ts, s.value)).collect(),
                                    )
                                })
                                .collect(),
                        },
                        Err(e) => Response::Err(e.to_string()),
                    }
                }
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
            Request::ApplyBatch { table, rows } => {
                write_response(self.apply_batch_local(&table, &rows))
            }
            Request::ApplyDdl { db, sql, hlc } => {
                self.with_write(|d| d.execute_session_with_hlc(&db, &sql, hlc).map(|_| ()))
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
            Request::SchemaDdl => match self.schema_sync() {
                Ok(entries) => Response::Schema { entries },
                Err(e) => Response::Err(e.to_string()),
            },
            Request::Announce { id, addr, rf } => self.handle_announce(&id, &addr, rf),
            Request::NodeStatus => {
                let (epoch, members) = {
                    let t = self.topo.read().expect("topo lock");
                    (t.epoch, t.members.iter().map(|(id, _)| id.0.clone()).collect())
                };
                let rows = self
                    .db_stats(true)
                    .map(|s| s.per_table.iter().map(|t| t.live_keys).sum::<u64>())
                    .unwrap_or(0);
                Response::NodeStatus {
                    epoch,
                    members,
                    rows,
                    hlc_ms: self.clock.peek().physical,
                }
            }
        }
    }

    /// Handle a peer's [`Request::Announce`] (auto-join): admit the announcing
    /// node into the ring and broadcast the new membership to everyone. Refuses a
    /// replication-factor mismatch, which would otherwise create a cluster whose
    /// coordinators disagree on each key's replica set.
    fn handle_announce(&self, id: &str, addr: &str, rf: u32) -> Response {
        if rf as usize != self.cfg.replication_factor {
            return Response::Err(format!(
                "replication-factor mismatch: joiner rf={rf}, cluster rf={}; \
                 make them equal before joining",
                self.cfg.replication_factor
            ));
        }
        match self.add_member(id, addr) {
            Ok(()) => Response::Ack,
            Err(e) => Response::Err(e.to_string()),
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
        self.execute_session_parsed(current_db, sql, parse(sql)?, consistency)
    }

    /// Like [`Node::execute_session_with`], but taking the statement already
    /// parsed by the caller (which parsed it for its privilege check), so the
    /// request path parses exactly once. `sql` is still needed verbatim for
    /// DDL broadcast and catalog reads.
    pub fn execute_session_parsed(
        self: &Arc<Self>,
        current_db: &str,
        sql: &str,
        stmt: Statement,
        consistency: Option<Consistency>,
    ) -> EngineResult<SessionEffect> {
        let mut stmt = stmt;
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
        // the current database, without fan-out — under a shared lock, so it
        // never queues behind (or blocks) writers.
        if matches!(
            stmt,
            Statement::ShowTables
                | Statement::ShowIndexes
                | Statement::ShowStatus
                | Statement::ShowDatabases
        ) {
            return self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .execute_session_read(current_db, sql)
                .map(SessionEffect::Output);
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
    /// that missed the broadcast (down at the time) converges later: schema is
    /// reconciled by [`Node::sync_schema_with`] during [`Node::repair`], which
    /// also runs automatically on (re)join via [`Node::startup_catch_up`].
    fn broadcast_ddl(&self, current_db: &str, sql: &str) -> EngineResult<()> {
        // Stamp the DDL once so every node records the same schema version.
        // (DDL deliberately does not move the replication-lag watermark: DDL acks
        // aren't tracked in `acked`, so counting DDL here would show phantom lag
        // against the last data write. DDL is broadcast at quorum and reconciled
        // by schema sync regardless.)
        let hlc = self.clock.now();
        let mut acks = 0usize;
        // Local first.
        match self.local.write() {
            Ok(mut db) => {
                db.execute_session_with_hlc(current_db, sql, hlc)?;
                acks += 1;
            }
            Err(_) => return Err(EngineError::Cluster("local lock poisoned".into())),
        }
        // Broadcast to all peers concurrently (scatter/gather), so the DDL
        // waits ~one round-trip instead of the sum of every peer's RTT.
        let addrs = self.peer_addrs();
        let req = Request::ApplyDdl {
            db: current_db.to_string(),
            sql: sql.to_string(),
            hlc,
        };
        acks += scatter(&addrs, |addr| {
            matches!(self.pool.call(addr, &req), Ok(Response::Ack))
        })
        .into_iter()
        .filter(|ok| *ok)
        .count();
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
    /// writes); enough peers to satisfy the quorum are written concurrently
    /// (scatter/gather: the wait is ~the slowest quorum peer's RTT, not the
    /// sum), with the local fsync overlapped on this thread; any remaining
    /// peers are replicated in the background (so e.g. CL=ONE returns after
    /// the local fsync without waiting for the peer round-trip).
    fn replicate(
        self: &Arc<Self>,
        table: &str,
        key: &[u8],
        op: WriteOp,
        hlc: Hlc,
        oc: Option<Consistency>,
    ) -> EngineResult<()> {
        self.counters.writes_total.fetch_add(1, Ordering::Relaxed);
        self.note_local_write(hlc);
        let replicas = self.replicas_for(key);
        let needed = oc
            .unwrap_or(self.cfg.write_consistency)
            .required(replicas.len().max(1));

        // 0) Opportunistically hand off any buffered hints to recovered replicas
        //    via the background worker (cheap when there are none; non-blocking;
        //    coalesced to at most one queued flush).
        if self.hints_pending()
            && !self.hint_flush_queued.swap(true, Ordering::AcqRel)
            && self.bg.try_send(BgTask::FlushHints).is_err()
        {
            // Queue full: clear the coalescing flag so a later write
            // re-queues the flush once there is room.
            self.hint_flush_queued.store(false, Ordering::Release);
        }

        // 1) Apply locally to the memtable + WAL buffer under the write lock
        //    (fast); the fsync runs below on this thread while the peer sends
        //    are in flight — instead of fsync-then-send serially. Read-your-
        //    writes holds immediately (the memtable has the row before the
        //    fsync lands); the local replica only *counts* toward the quorum
        //    once its fsync completes, so durability is unchanged — just
        //    overlapped.
        let buffered = if replicas.contains(&self.id) {
            Some(self.apply_write_buffered(table, key, &op, hlc)?)
        } else {
            None
        };

        // 2) Pipelined scatter: put each quorum peer's write on the wire (a
        //    small frame into the socket buffer — doesn't block on the peer),
        //    run the local fsync on this thread while the peers append+fsync,
        //    then collect the acks. No thread is spawned: on small nodes the
        //    per-write spawn/park/join cycle (clone3 + stack setup + futex
        //    wakeups) used to cost more than the fsync itself. A failed peer
        //    is hinted and the next untried replica takes its place (retry
        //    waves below), so the set contacted synchronously matches the old
        //    sequential loop.
        //
        //    (Measured dead end, kept for the record: coalescing concurrent
        //    writers' frames into shared per-peer ApplyBatch flushes —
        //    replication group commit — cost ~9% write throughput at 16
        //    clients on 1-vCPU nodes. The coordinator is CPU-bound there, so
        //    the saved peer frames bought nothing while the queue/wake
        //    machinery added coordinator CPU; only the async tail benefits
        //    from batching, which `run_bg_tasks` does off the client path.)
        let peers: Vec<(NodeId, String)> = replicas
            .iter()
            .filter(|r| **r != self.id)
            .filter_map(|r| self.peer_addr(r).map(|addr| (r.clone(), addr)))
            .collect();
        let sync_target = needed.saturating_sub(usize::from(buffered.is_some()));
        let first = sync_target.min(peers.len());
        let (oks, local_ok) = {
            let pending: Vec<Option<Pending<'_>>> = peers[..first]
                .iter()
                .map(|(_, addr)| self.send_write_begin(addr, table, key, &op, hlc))
                .collect();
            // Overlap: fsync the local buffered write while the sends fly.
            let local_ok = match &buffered {
                Some((commit, handle)) => handle.sync_through(*commit).is_ok(),
                None => false,
            };
            let oks: Vec<bool> = pending.into_iter().map(|p| self.finish_ack(p)).collect();
            (oks, local_ok)
        };
        let mut acks = usize::from(local_ok);
        for ((replica, _), ok) in peers[..first].iter().zip(oks) {
            if ok {
                acks += 1;
                self.note_acked(replica, hlc);
            } else {
                // Replica down: buffer the write for hinted handoff.
                self.store_hint(replica, table, key, &op, hlc);
            }
        }

        // Retry waves: while short of the quorum, try the next untried
        // replicas (concurrently) in place of the ones that failed.
        let mut next = first;
        while acks < needed && next < peers.len() {
            let take = (needed - acks).min(peers.len() - next);
            let wave = &peers[next..next + take];
            next += take;
            let op_ref = &op;
            let oks = scatter(wave, |(_, addr)| {
                matches!(self.send_write(addr, table, key, op_ref, hlc), Ok(true))
            });
            for ((replica, _), ok) in wave.iter().zip(oks) {
                if ok {
                    acks += 1;
                    self.note_acked(replica, hlc);
                } else {
                    self.store_hint(replica, table, key, &op, hlc);
                }
            }
        }

        // 3) Hand the remaining replicas to the background worker (eventual
        //    consistency), which buffers a hint for any that are unreachable.
        if next < peers.len() {
            let task = BgTask::Replicate {
                peers: peers[next..].to_vec(),
                table: table.to_string(),
                key: key.to_vec(),
                op: op.clone(),
                hlc,
            };
            // Queue full (tail replication can't keep up with ingest) or
            // worker gone (shutdown): keep the writes as bounded hints
            // instead of growing the queue without limit.
            if let Err(
                mpsc::TrySendError::Full(BgTask::Replicate { peers, table, key, op, hlc })
                | mpsc::TrySendError::Disconnected(BgTask::Replicate { peers, table, key, op, hlc }),
            ) = self.bg.try_send(task)
            {
                for (replica, _) in peers {
                    self.store_hint(&replica, &table, &key, &op, hlc);
                }
            }
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

    /// Replicate a whole statement's puts. Rows are grouped by replica set;
    /// each group is applied locally under one write-lock acquisition with
    /// **one** fsync, overlapped with a single [`Request::ApplyBatch`]
    /// round-trip per quorum peer — instead of one replication round (and one
    /// fsync on every replica) per row. Per-group consistency semantics match
    /// [`Node::replicate`]: the statement acks once every group reaches its
    /// quorum; replicas beyond the quorum get the batch in the background.
    fn replicate_batch(
        self: &Arc<Self>,
        table: &str,
        rows: Vec<(Vec<u8>, Vec<u8>, Hlc)>,
        oc: Option<Consistency>,
    ) -> EngineResult<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.counters
            .writes_total
            .fetch_add(rows.len() as u64, Ordering::Relaxed);
        if let Some((_, _, hlc)) = rows.last() {
            self.note_local_write(*hlc);
        }
        if self.hints_pending()
            && !self.hint_flush_queued.swap(true, Ordering::AcqRel)
            && self.bg.try_send(BgTask::FlushHints).is_err()
        {
            // Queue full: clear the coalescing flag so a later write
            // re-queues the flush once there is room.
            self.hint_flush_queued.store(false, Ordering::Release);
        }
        // Group rows by replica set. With members == RF every key maps to the
        // same set (one group, one batch); larger clusters get one batch per
        // distinct set.
        let mut groups: Vec<(Vec<NodeId>, Vec<BatchRow>)> = Vec::new();
        for (key, value, hlc) in rows {
            let replicas = self.replicas_for(&key);
            let row: BatchRow = (key, value, hlc, true);
            match groups.iter_mut().find(|(r, _)| *r == replicas) {
                Some((_, group)) => group.push(row),
                None => groups.push((replicas, vec![row])),
            }
        }
        for (replicas, group) in groups {
            self.replicate_group(table, &replicas, group, oc)?;
        }
        Ok(())
    }

    /// Replicate one replica-set group of a statement batch (see
    /// [`Node::replicate_batch`]).
    fn replicate_group(
        self: &Arc<Self>,
        table: &str,
        replicas: &[NodeId],
        rows: Vec<BatchRow>,
        oc: Option<Consistency>,
    ) -> EngineResult<()> {
        let needed = oc
            .unwrap_or(self.cfg.write_consistency)
            .required(replicas.len().max(1));
        let last_hlc = rows.last().map(|(_, _, h, _)| *h).expect("group is non-empty");
        let buffered = if replicas.contains(&self.id) {
            self.apply_batch_buffered(table, &rows)?
        } else {
            None
        };
        let peers: Vec<(NodeId, String)> = replicas
            .iter()
            .filter(|r| **r != self.id)
            .filter_map(|r| self.peer_addr(r).map(|addr| (r.clone(), addr)))
            .collect();
        let sync_target = needed.saturating_sub(usize::from(buffered.is_some()));
        let first = sync_target.min(peers.len());
        // Pipelined, thread-free scatter as in `replicate`: sends on the
        // wire, local fsync overlapped, then collect.
        let pending: Vec<Option<Pending<'_>>> = peers[..first]
            .iter()
            .map(|(_, addr)| self.send_batch_begin(addr, table, &rows))
            .collect();
        let local_ok = match &buffered {
            Some((commit, handle)) => handle.sync_through(*commit).is_ok(),
            None => false,
        };
        let mut acks = usize::from(local_ok);
        for ((replica, addr), p) in peers[..first].iter().zip(pending) {
            if self.finish_batch_ack(p, addr, table, &rows) {
                acks += 1;
                self.note_acked(replica, last_hlc);
            } else {
                self.hint_batch(replica, table, &rows);
            }
        }
        // Retry waves: while short of the quorum, try the next untried
        // replicas in place of the ones that failed.
        let mut next = first;
        while acks < needed && next < peers.len() {
            let take = (needed - acks).min(peers.len() - next);
            let wave = &peers[next..next + take];
            next += take;
            let rows_ref = &rows;
            let oks = scatter(wave, |(_, addr)| self.send_batch(addr, table, rows_ref));
            for ((replica, _), ok) in wave.iter().zip(oks) {
                if ok {
                    acks += 1;
                    self.note_acked(replica, last_hlc);
                } else {
                    self.hint_batch(replica, table, &rows);
                }
            }
        }
        // Replicas beyond the quorum get the whole batch in the background.
        if next < peers.len() {
            let task = BgTask::ReplicateBatch {
                peers: peers[next..].to_vec(),
                table: table.to_string(),
                rows: rows.clone(),
            };
            // Full queue or shutdown: bounded hints instead of unbounded queue.
            if let Err(
                mpsc::TrySendError::Full(BgTask::ReplicateBatch { peers, table, rows })
                | mpsc::TrySendError::Disconnected(BgTask::ReplicateBatch { peers, table, rows }),
            ) = self.bg.try_send(task)
            {
                for (replica, _) in peers {
                    self.hint_batch(&replica, &table, &rows);
                }
            }
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

    /// Apply a batch of writes to one table under a single write-lock
    /// acquisition, with buffered WAL appends and **one** fsync for the whole
    /// batch (the per-row path pays one fsync per RPC). All rows go to the same
    /// table — one WAL — and commit points are monotonic, so syncing through
    /// the last row's commit makes every row durable.
    fn apply_batch_local(&self, table: &str, rows: &[BatchRow]) -> EngineResult<()> {
        if let Some((commit, handle)) = self.apply_batch_buffered(table, rows)? {
            handle.sync_through(commit)?;
        }
        Ok(())
    }

    /// The buffered half of [`Node::apply_batch_local`]: append + apply every
    /// row under one write-lock acquisition, returning the last row's commit
    /// point so the caller can overlap the single fsync with peer round-trips.
    fn apply_batch_buffered(
        &self,
        table: &str,
        rows: &[BatchRow],
    ) -> EngineResult<Option<(WalCommit, Arc<WalSync>)>> {
        let mut db = self
            .local
            .write()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
        let mut last = None;
        for (key, value, hlc, is_put) in rows {
            last = Some(if *is_put {
                db.apply_put_buffered(table, key, value.clone(), *hlc)?
            } else {
                db.apply_delete_buffered(table, key, *hlc)?
            });
        }
        Ok(last)
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

        // Quorum-count fan-out: the local replica counts as one responder, so
        // only `needed - 1` peers are consulted (not every replica) — at RF=3
        // that's one peer read per key instead of two, halving cluster-wide
        // read work. The peer sends are pipelined: put each LocalGet on the
        // wire, do the local read while the peers work, then collect — all on
        // this thread (no spawn/join). If a contacted peer is unreachable,
        // the next untried replicas take its place (shortfall waves below).
        let mut merge = |addr_opt: Option<String>, entry: Option<(Vec<u8>, Hlc, bool)>| {
            let entry_hlc = entry.as_ref().map(|(_, h, _)| *h);
            if let Some((value, hlc, is_put)) = entry {
                if best.as_ref().is_none_or(|(h, _)| hlc > *h) {
                    best = Some((hlc, is_put.then_some(value)));
                }
            }
            seen.push((addr_opt, entry_hlc));
        };
        let is_local_replica = replicas.contains(&self.id);
        let peers: Vec<(NodeId, String)> = replicas
            .iter()
            .filter(|r| **r != self.id)
            .filter_map(|r| self.peer_addr(r).map(|addr| (r.clone(), addr)))
            .collect();
        let req = Request::LocalGet {
            table: table.to_string(),
            key: key.to_vec(),
        };
        let first = needed
            .saturating_sub(usize::from(is_local_replica))
            .min(peers.len());
        let pending: Vec<Option<Pending<'_>>> = peers[..first]
            .iter()
            .map(|(_, addr)| {
                self.counters.peer_requests.fetch_add(1, Ordering::Relaxed);
                match self.pool.call_begin(addr, &req) {
                    Ok(p) => Some(p),
                    Err(_) => {
                        self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                }
            })
            .collect();
        // Overlap: read the local replica while the peer reads fly.
        if is_local_replica {
            let entry = match self.local.read() {
                Ok(db) => db.local_get_versioned(table, key)?,
                Err(_) => return Err(EngineError::Cluster("local lock poisoned".into())),
            };
            responders += 1;
            merge(None, entry);
        }
        for ((_, addr), p) in peers[..first].iter().zip(pending) {
            match p.map(Pending::finish) {
                Some(Ok(Response::Get { entry })) => {
                    responders += 1;
                    merge(Some(addr.clone()), entry);
                }
                Some(Ok(_)) | None => {}
                Some(Err(_)) => {
                    self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        // Shortfall waves: while short of the quorum, consult the next
        // untried replicas in place of the unreachable ones.
        let mut next = first;
        while responders < needed && next < peers.len() {
            let (_, addr) = &peers[next];
            next += 1;
            self.counters.peer_requests.fetch_add(1, Ordering::Relaxed);
            match self.pool.call(addr, &req) {
                Ok(Response::Get { entry }) => {
                    responders += 1;
                    merge(Some(addr.clone()), entry);
                }
                Ok(_) => {}
                Err(_) => {
                    self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
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

    /// First half of [`Node::send_write`]: put the replicated write on the
    /// wire and return the in-flight call, so the caller can overlap its own
    /// fsync with the peer's append+fsync. `None` means the send itself
    /// failed (peer unreachable) — the caller treats it like a nack.
    fn send_write_begin(
        &self,
        addr: &str,
        table: &str,
        key: &[u8],
        op: &WriteOp,
        hlc: Hlc,
    ) -> Option<Pending<'_>> {
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
        match self.pool.call_begin(addr, &req) {
            Ok(pending) => Some(pending),
            Err(_) => {
                self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Second half: collect a peer's response; `true` iff it acked.
    fn finish_ack(&self, pending: Option<Pending<'_>>) -> bool {
        match pending.map(Pending::finish) {
            Some(Ok(resp)) => matches!(resp, Response::Ack),
            Some(Err(_)) => {
                self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                false
            }
            None => false,
        }
    }

    /// First half of [`Node::send_batch`]: put the whole batch on the wire
    /// and return the in-flight call so the caller can overlap its own fsync
    /// with the peer's batch apply. `None` means the send itself failed.
    fn send_batch_begin(&self, addr: &str, table: &str, rows: &[BatchRow]) -> Option<Pending<'_>> {
        self.counters.peer_requests.fetch_add(1, Ordering::Relaxed);
        match self.pool.call_begin(
            addr,
            &Request::ApplyBatch {
                table: table.to_string(),
                rows: rows.to_vec(),
            },
        ) {
            Ok(pending) => Some(pending),
            Err(_) => {
                self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Second half: collect a peer's batch response; `true` iff every row is
    /// durable on it. A peer that predates the batch RPC (rolling upgrade)
    /// rejects the unknown op, in which case the rows are re-sent per-row.
    fn finish_batch_ack(
        &self,
        pending: Option<Pending<'_>>,
        addr: &str,
        table: &str,
        rows: &[BatchRow],
    ) -> bool {
        match pending.map(Pending::finish) {
            Some(Ok(Response::Ack)) => true,
            Some(Ok(Response::Err(e))) if e.contains("unknown request op") => {
                rows.iter().all(|(key, value, hlc, is_put)| {
                    let op = if *is_put {
                        WriteOp::Put(value.clone())
                    } else {
                        WriteOp::Delete
                    };
                    matches!(self.send_write(addr, table, key, &op, *hlc), Ok(true))
                })
            }
            Some(Ok(_)) => false,
            Some(Err(_)) => {
                self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                false
            }
            None => false,
        }
    }

    /// Buffer every row of a batch as a hint for `replica` (it was down or
    /// short of the quorum), for later hinted handoff.
    fn hint_batch(&self, replica: &NodeId, table: &str, rows: &[BatchRow]) {
        for (key, value, hlc, is_put) in rows {
            let op = if *is_put {
                WriteOp::Put(value.clone())
            } else {
                WriteOp::Delete
            };
            self.store_hint(replica, table, key, &op, *hlc);
        }
    }

    /// Send a one-table batch of writes to `addr` as a single
    /// [`Request::ApplyBatch`] round-trip (one lock acquisition and one fsync
    /// on the peer). Returns whether the peer acked every row. A peer that
    /// predates the batch RPC (rolling upgrade) rejects the unknown op, in
    /// which case the rows are re-sent per-row so migration/handoff still
    /// completes against it.
    fn send_batch(&self, addr: &str, table: &str, rows: &[BatchRow]) -> bool {
        if rows.is_empty() {
            return true;
        }
        self.counters.peer_requests.fetch_add(1, Ordering::Relaxed);
        match self.pool.call(
            addr,
            &Request::ApplyBatch {
                table: table.to_string(),
                rows: rows.to_vec(),
            },
        ) {
            Ok(Response::Ack) => return true,
            Ok(Response::Err(e)) if e.contains("unknown request op") => {} // old peer: fall back
            Ok(_) => return false,
            Err(_) => {
                self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }
        rows.iter().all(|(key, value, hlc, is_put)| {
            let op = if *is_put {
                WriteOp::Put(value.clone())
            } else {
                WriteOp::Delete
            };
            matches!(self.send_write(addr, table, key, &op, *hlc), Ok(true))
        })
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

        // Local shard (with tombstones), paged: the read lock is held one
        // page at a time and no whole-shard `Vec` is built beside the merge
        // map. Writes landing between pages resolve by last-writer-wins,
        // like writes landing between two peers' scans always have.
        {
            let mut after: Option<Vec<u8>> = None;
            loop {
                let rows = self
                    .local
                    .read()
                    .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                    .local_scan_versioned_page(table, after.as_deref(), SCAN_PAGE_ROWS)?;
                let full = rows.len() == SCAN_PAGE_ROWS;
                after = rows.last().map(|(k, ..)| k.clone());
                for (key, value, hlc, is_put) in rows {
                    merge_row(&mut merged, key, is_put.then_some(value), hlc);
                }
                if !full {
                    break;
                }
            }
            responders += 1;
        }

        // Peers: one worker per peer pages its shard concurrently, feeding
        // pages into the merge as they arrive. Peak memory is the merge map
        // plus a few in-flight pages — never every peer's whole shard at
        // once (an unpaged gather held full shards and OOM-killed 512 MB
        // nodes at 1M rows). A peer failing mid-scan may leave some of its
        // rows merged; that is harmless (they are real replica data and LWW
        // applies) and it is not counted as a responder.
        let addrs = self.peer_addrs();
        let peer_ok: Vec<bool> = thread::scope(|s| {
            let (tx, rx) = mpsc::sync_channel::<Vec<BatchRow>>(addrs.len().max(1));
            let handles: Vec<_> = addrs
                .iter()
                .map(|addr| {
                    let tx = tx.clone();
                    s.spawn(move || self.scan_peer_paged(addr, table, &tx))
                })
                .collect();
            drop(tx);
            for rows in rx {
                for (key, value, hlc, is_put) in rows {
                    merge_row(&mut merged, key, is_put.then_some(value), hlc);
                }
            }
            handles
                .into_iter()
                .map(|h| h.join().expect("scan worker panicked"))
                .collect()
        });
        responders += peer_ok.into_iter().filter(|ok| *ok).count();

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

    /// Stream one peer's shard of `table` into `tx`, one `ScanPage` at a
    /// time. Returns whether the peer supplied its complete shard (only then
    /// does it count toward the read quorum). Peers that predate `ScanPage`
    /// fall back to a single whole-table pull.
    fn scan_peer_paged(&self, addr: &str, table: &str, tx: &mpsc::SyncSender<Vec<BatchRow>>) -> bool {
        let mut after: Option<Vec<u8>> = None;
        let mut first = true;
        loop {
            self.counters.peer_requests.fetch_add(1, Ordering::Relaxed);
            match self.pool.call(
                addr,
                &Request::ScanPage {
                    table: table.to_string(),
                    after: after.clone(),
                    limit: SCAN_PAGE_ROWS as u32,
                },
            ) {
                Ok(Response::Scan { rows }) => {
                    first = false;
                    let full = rows.len() == SCAN_PAGE_ROWS;
                    after = rows.last().map(|(k, ..)| k.clone());
                    if tx.send(rows).is_err() {
                        return false; // merge side gone; scan abandoned
                    }
                    if !full {
                        return true;
                    }
                }
                // Rolling upgrade: the peer predates ScanPage. Fall back to
                // the old whole-shard pull for it.
                Ok(Response::Err(e)) if first && e.contains("unknown request op") => {
                    self.counters.peer_requests.fetch_add(1, Ordering::Relaxed);
                    return match self.pool.call(
                        addr,
                        &Request::LocalScan {
                            table: table.to_string(),
                        },
                    ) {
                        Ok(Response::Scan { rows }) => tx.send(rows).is_ok(),
                        _ => {
                            self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                            false
                        }
                    };
                }
                _ => {
                    self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                    return false;
                }
            }
        }
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

        // Peer index shards, scattered concurrently (unreachable peers are
        // skipped, exactly as before).
        let req = Request::IndexScan {
            index: index.to_string(),
            start,
            end,
        };
        let addrs = self.peer_addrs();
        for ks in scatter(&addrs, |addr| match self.pool.call(addr, &req) {
            Ok(Response::Keys { keys }) => keys,
            _ => Vec::new(),
        }) {
            for k in ks {
                keys.insert(k, ());
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

        // Peer shards, scattered concurrently (unreachable peers are skipped,
        // exactly as before).
        let req = Request::FilteredScan {
            table: table.to_string(),
            filter: filter.clone(),
        };
        let addrs = self.peer_addrs();
        for ks in scatter(&addrs, |addr| match self.pool.call(addr, &req) {
            Ok(Response::Keys { keys }) => keys,
            _ => Vec::new(),
        }) {
            for k in ks {
                keys.insert(k, ());
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
        // Peer shards, scattered concurrently (unreachable peers are skipped,
        // exactly as before); merged in deterministic peer order.
        let req = Request::VectorSearch {
            index: index.to_string(),
            query: query.to_vec(),
            k: fetch as u32,
        };
        let addrs = self.peer_addrs();
        for hits in scatter(&addrs, |addr| match self.pool.call(addr, &req) {
            Ok(Response::VectorHits { hits }) => hits,
            _ => Vec::new(),
        }) {
            for (key, dist) in hits {
                consider(key, dist, &mut best);
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

/// Scatter `f` over `items` concurrently — one scoped thread per item, which is
/// fine because item counts are small (at most the replication factor or the
/// cluster size) — and gather the results in item order. Scoped threads let `f`
/// borrow from the caller; a single item runs inline (no spawn).
fn scatter<T, R, F>(items: &[T], f: F) -> Vec<R>
where
    T: Sync,
    R: Send,
    F: Fn(&T) -> R + Sync,
{
    if items.len() <= 1 {
        return items.iter().map(f).collect();
    }
    thread::scope(|s| {
        let f = &f;
        let handles: Vec<_> = items
            .iter()
            .map(|item| s.spawn(move || f(item)))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("scatter worker panicked"))
            .collect()
    })
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

/// FNV-1a 64-bit hash — used to derive a stable per-node stagger for the
/// anti-entropy loop (not security-sensitive).
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Ring-placement key for a series: its labels minus the reserved
/// `__field__` discriminator, so every field stream of a logical series
/// lands on the same replica set.
fn ts_placement_key(labels: &[(String, String)]) -> Vec<u8> {
    let mut key = Vec::new();
    for (k, v) in labels {
        if k == "__field__" {
            continue;
        }
        key.extend_from_slice(k.as_bytes());
        key.push(0);
        key.extend_from_slice(v.as_bytes());
        key.push(0);
    }
    key
}

fn is_ddl(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateTable(_)
            | Statement::CreateTimeseriesTable(_)
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

    fn ts_series_key(&self, table: &str) -> EngineResult<Option<Vec<String>>> {
        Ok(self
            .node
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .ts_series_key(table))
    }

    fn ts_append(
        &mut self,
        table: &str,
        rows: &[(skaidb_tsdb::Labels, i64, f64)],
    ) -> EngineResult<usize> {
        // A series is the placement unit: group samples by the replica set
        // of their series (labels minus the per-field discriminator, so all
        // of a series' field streams co-locate), then ship one batch per
        // replica. Replays of a duplicate batch are harmless (per-sample
        // rejection), so retries can't double-count.
        let mut groups: HashMap<Vec<NodeId>, Vec<(skaidb_tsdb::Labels, i64, f64)>> =
            HashMap::new();
        for row in rows {
            let key = ts_placement_key(&row.0);
            groups
                .entry(self.node.replicas_for(&key))
                .or_default()
                .push(row.clone());
        }
        let needed = self
            .oc
            .unwrap_or(self.node.cfg.write_consistency)
            .required(self.node.member_count());
        let mut appended = 0usize;
        for (replicas, batch) in groups {
            let mut acks = 0usize;
            for replica in &replicas {
                if *replica == self.node.cfg.id {
                    let db = self
                        .node
                        .local
                        .read()
                        .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
                    db.ts_append(table, &batch)?;
                    acks += 1;
                } else if let Some(addr) = self.node.peer_addr(replica) {
                    self.node
                        .counters
                        .peer_requests
                        .fetch_add(1, Ordering::Relaxed);
                    match self.node.pool.call(
                        &addr,
                        &Request::TsAppend {
                            table: table.to_string(),
                            rows: batch.clone(),
                        },
                    ) {
                        Ok(Response::Ack) => acks += 1,
                        _ => {
                            self.node
                                .counters
                                .peer_errors
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            if acks < needed {
                self.node
                    .counters
                    .write_quorum_failures
                    .fetch_add(1, Ordering::Relaxed);
                return Err(EngineError::Cluster(format!(
                    "write quorum not met: {acks}/{needed} replicas acked"
                )));
            }
            appended += batch.len();
        }
        Ok(appended)
    }

    fn ts_query(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
        t0: i64,
        t1: i64,
    ) -> EngineResult<Vec<(skaidb_tsdb::Labels, Vec<skaidb_tsdb::Sample>)>> {
        self.node.ts_scatter(table, matchers, t0, t1, self.oc)
    }

    fn vector_search(
        &self,
        table: &str,
        path: &str,
        query: &[f32],
        k: usize,
        filter: &Option<Expr>,
    ) -> EngineResult<Vec<(Vec<u8>, Document, f32)>> {
        let index = self
            .node
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .vector_index_for(table, path)
            .ok_or_else(|| {
                EngineError::Unsupported(format!("no vector index on {table} ({path})"))
            })?;
        // Distributed ANN: scatter to every node's local HNSW, merge by
        // distance, re-read survivors at the read quorum.
        self.node.vector_search(&index, query, k, filter)
    }

    fn matching_rows(
        &self,
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

    fn put_batch(&mut self, table: &str, rows: &[(Vec<u8>, Document)]) -> EngineResult<()> {
        // A single row gets the per-row path (same cost, no grouping pass).
        if let [(key, doc)] = rows {
            return self.put(table, key, doc);
        }
        let batch: Vec<(Vec<u8>, Vec<u8>, Hlc)> = rows
            .iter()
            .map(|(key, doc)| {
                (
                    key.clone(),
                    Value::Document(doc.clone()).encode(),
                    self.node.clock.now(),
                )
            })
            .collect();
        self.node.replicate_batch(table, batch, self.oc)
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

    /// Grab a free localhost address. Dedups within this test binary so the
    /// many parallel tests can't be handed the same just-freed ephemeral port
    /// (a TOCTOU the OS otherwise hits under load, causing cross-test crosstalk).
    fn free_addr() -> String {
        use std::collections::HashSet;
        static USED: Mutex<Option<HashSet<u16>>> = Mutex::new(None);
        loop {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            let port = l.local_addr().unwrap().port();
            drop(l);
            let mut guard = USED.lock().expect("free_addr lock");
            if guard.get_or_insert_with(HashSet::new).insert(port) {
                return format!("127.0.0.1:{port}");
            }
        }
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
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
        }
    }

    /// Like [`cfg`] but with an explicit replication factor and internode auth.
    /// Auto-join stays off: the announce tests drive the `Announce` RPC
    /// synchronously (deterministic), and the background announce path is covered
    /// by the end-to-end smoke test.
    fn cfg_auth(
        id: &str,
        addr: &str,
        members: &[(NodeId, String)],
        rf: usize,
        auth: Authenticator,
    ) -> NodeConfig {
        NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: rf,
            vnodes_per_node: 64,
            read_consistency: Consistency::Quorum,
            write_consistency: Consistency::Quorum,
            auth: Arc::new(auth),
            auto_join: false,
            anti_entropy_interval_secs: 0,
        }
    }

    #[test]
    fn token_auth_allows_replication_with_matching_secret() {
        let (a, b) = (free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b)];
        let tok = || Authenticator::token(b"shared-cluster-secret".to_vec());
        let na = Node::new(
            Database::open(temp_dir("tok-a")).unwrap(),
            cfg_auth("a", &a, &members, 2, tok()),
        );
        let nb = Node::new(
            Database::open(temp_dir("tok-b")).unwrap(),
            cfg_auth("b", &b, &members, 2, tok()),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        // DDL + writes require reaching b across the authenticated channel.
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        na.execute("INSERT INTO t (id, v) VALUES (1, 'x')").unwrap();
        let rs = rows(nb.execute("SELECT v FROM t WHERE id = 1").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::String("x".into())]]);
    }

    #[test]
    fn concurrent_writers_replicate_and_converge() {
        // 2 nodes, rf=2, QUORUM writes: every write must sync-replicate to
        // the peer, so concurrent writers exercise the pipelined scatter
        // (overlapped send/fsync/collect) under contention.
        let (a, b) = (free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b)];
        let na = Node::new(
            Database::open(temp_dir("lane-a")).unwrap(),
            cfg_auth("a", &a, &members, 2, Authenticator::None),
        );
        let nb = Node::new(
            Database::open(temp_dir("lane-b")).unwrap(),
            cfg_auth("b", &b, &members, 2, Authenticator::None),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();

        const WRITERS: usize = 8;
        const PER_WRITER: usize = 25;
        thread::scope(|s| {
            for w in 0..WRITERS {
                let na = &na;
                s.spawn(move || {
                    for i in 0..PER_WRITER {
                        let id = w * PER_WRITER + i;
                        na.execute(&format!("INSERT INTO t (id, v) VALUES ({id}, 'v{id}')"))
                            .unwrap();
                    }
                });
            }
        });

        // Quorum reads on a 2-of-2 cluster touch both replicas, so a write
        // acked but not applied on either replica would surface here.
        for node in [&na, &nb] {
            let rs = rows(node.execute("SELECT id FROM t").unwrap());
            assert_eq!(rs.rows.len(), WRITERS * PER_WRITER);
        }
    }

    #[test]
    fn token_mismatch_blocks_internode_rpc() {
        let (a, b) = (free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b)];
        let na = Node::new(
            Database::open(temp_dir("tokx-a")).unwrap(),
            cfg_auth("a", &a, &members, 2, Authenticator::token(b"secret-A".to_vec())),
        );
        let nb = Node::new(
            Database::open(temp_dir("tokx-b")).unwrap(),
            cfg_auth("b", &b, &members, 2, Authenticator::token(b"secret-B".to_vec())),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        // b rejects a's handshake, so the DDL can't reach a member quorum (2/2).
        let err = na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap_err();
        assert!(
            err.to_string().contains("quorum"),
            "expected a quorum failure, got: {err}"
        );
    }

    #[test]
    fn announce_admits_a_node_the_cluster_was_not_configured_with() {
        // a + b form a 2-node cluster; their seeds do NOT list c. c announces
        // itself (the auto-join RPC, driven synchronously here so the test is
        // deterministic) and must be admitted on every node.
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let ab = vec![member(&a, &a), member(&b, &b)];
        let abc = vec![member(&a, &a), member(&b, &b), member(&c, &c)];
        let none = || Authenticator::None;
        let na = Node::new(
            Database::open(temp_dir("aj-a")).unwrap(),
            cfg_auth(&a, &a, &ab, 2, none()),
        );
        let nb = Node::new(
            Database::open(temp_dir("aj-b")).unwrap(),
            cfg_auth(&b, &b, &ab, 2, none()),
        );
        let nc = Node::new(
            Database::open(temp_dir("aj-c")).unwrap(),
            cfg_auth(&c, &c, &abc, 2, none()),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        nc.serve_internode().unwrap();

        // `add_member` completes (broadcast + rebalance) before the Ack returns.
        let resp = internode::call(
            &a,
            &Request::Announce {
                id: c.clone(),
                addr: c.clone(),
                rf: 2,
            },
        )
        .unwrap();
        assert_eq!(resp, Response::Ack);
        assert!(na.member_ids().contains(&c), "a admitted c");
        assert!(nb.member_ids().contains(&c), "b learned c via the broadcast");
        assert!(nc.current_epoch() > 0, "c received the live ring");
    }

    #[test]
    fn announce_rejected_on_replication_factor_mismatch() {
        let a = free_addr();
        let na = Node::new(
            Database::open(temp_dir("rfx-a")).unwrap(),
            cfg_auth(&a, &a, &[member(&a, &a)], 2, Authenticator::None),
        );
        na.serve_internode().unwrap();

        // A node announcing rf=3 against an rf=2 cluster must be refused.
        let resp = internode::call(
            &a,
            &Request::Announce {
                id: "127.0.0.1:1".into(),
                addr: "127.0.0.1:1".into(),
                rf: 3,
            },
        )
        .unwrap();
        match resp {
            Response::Err(e) => assert!(
                e.contains("replication-factor"),
                "expected an RF-mismatch error, got: {e}"
            ),
            other => panic!("expected rejection, got {other:?}"),
        }
        assert!(
            !na.member_ids().contains(&"127.0.0.1:1".to_string()),
            "a must not admit a node with a mismatched replication factor"
        );
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
    fn peer_stats_report_config_ring_reachability_and_lag() {
        let (a, b) = (free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b)];
        let na = Node::new(
            Database::open(temp_dir("psa")).unwrap(),
            cfg("a", &a, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nb = Node::new(
            Database::open(temp_dir("psb")).unwrap(),
            cfg("b", &b, &members, Consistency::Quorum, Consistency::Quorum),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        na.execute("INSERT INTO t (id, v) VALUES (1, 'x')").unwrap();

        let peers = na.peer_stats(true);
        assert_eq!(peers.len(), 1, "exactly one peer (b), self excluded");
        let p = &peers[0];
        assert_eq!(p.id, "b");
        assert!(p.in_config, "b is a configured seed");
        assert!(p.in_ring, "b is in the live ring");
        assert_eq!(p.reachable, Some(true), "b is serving, probe succeeds");
        assert_eq!(p.hints_pending, 0, "all writes reached b");
        assert!(
            p.lag_ms.is_some(),
            "a quorum write was confirmed to b, so its lag is known"
        );
        assert!(na.stats().self_in_ring, "a is a normal ring member");
    }

    #[test]
    fn idle_cluster_reports_zero_lag_despite_clock_advancing() {
        // Regression: lag is measured against the coordinated-write watermark,
        // not the HLC frontier. After a write is fully acked, an idle cluster
        // whose clock keeps advancing (reads, probes, observed peer clocks) must
        // still report ~0 lag — not an ever-growing "time since last write".
        let (a, b) = (free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b)];
        let na = Node::new(
            Database::open(temp_dir("idlelag_a")).unwrap(),
            cfg("a", &a, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nb = Node::new(
            Database::open(temp_dir("idlelag_b")).unwrap(),
            cfg("b", &b, &members, Consistency::Quorum, Consistency::Quorum),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        na.execute("INSERT INTO t (id, v) VALUES (1, 'x')").unwrap();
        // Quorum across 2 members means b acked synchronously, so it is fully
        // caught up: lag is exactly zero.
        assert_eq!(na.peer_stats(true)[0].lag_ms, Some(0), "b is caught up");

        // Simulate an idle cluster where time passes and the local clock keeps
        // moving (a read, a status probe, observing a peer's clock all call
        // `clock.now()`), but no new write is coordinated.
        std::thread::sleep(std::time::Duration::from_millis(30));
        for _ in 0..5 {
            na.clock.now();
        }
        assert!(
            na.clock.peek().physical > na.write_watermark.load(Ordering::Relaxed),
            "clock frontier has advanced past the write watermark (the old bug's input)"
        );
        assert_eq!(
            na.peer_stats(true)[0].lag_ms,
            Some(0),
            "no new writes: idle lag stays zero, not 'time since last write'"
        );
    }

    #[test]
    fn self_in_ring_false_for_half_joined_node() {
        // A node whose own id is absent from its membership (it points only at
        // peers) — it would catch up data but was never admitted to the ring.
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let peers_only = vec![member("b", &b), member("c", &c)];
        let na = Node::new(
            Database::open(temp_dir("halfjoin")).unwrap(),
            cfg("a", &a, &peers_only, Consistency::Quorum, Consistency::Quorum),
        );
        assert!(
            !na.stats().self_in_ring,
            "a is coordinating but not in its own ring (half-join)"
        );
    }

    #[test]
    fn peer_stats_flag_unreachable_peer_and_hint_backlog() {
        // Three configured members; a coordinates, c is up, b is never served.
        // DDL still reaches quorum (a+c of 3), but replica writes to b fail.
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(
            Database::open(temp_dir("psda")).unwrap(),
            cfg("a", &a, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nc = Node::new(
            Database::open(temp_dir("psdc")).unwrap(),
            cfg("c", &c, &members, Consistency::Quorum, Consistency::Quorum),
        );
        na.serve_internode().unwrap();
        nc.serve_internode().unwrap();

        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        na.execute("INSERT INTO t (id, v) VALUES (1, 'x')").unwrap();
        // An undeliverable replica write may be buffered on a background thread.
        thread::sleep(Duration::from_millis(200));

        let peers = na.peer_stats(true);
        let pb = peers.iter().find(|p| p.id == "b").expect("b listed");
        assert_eq!(pb.reachable, Some(false), "b is down, probe fails fast");
        assert!(pb.hints_pending >= 1, "the write to b was buffered as a hint");
        assert_eq!(pb.lag_ms, None, "no write ever confirmed to b => lag unknown");

        let pc = peers.iter().find(|p| p.id == "c").expect("c listed");
        assert_eq!(pc.reachable, Some(true), "c is serving");
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

        // The same searches through SQL (`NEAREST`), on yet another node:
        // distance-ordered ids, `_distance` exposed as a projected field.
        let rs = rows(
            nb.execute("SELECT id, _distance FROM docs NEAREST (embedding, [1.0, 0.0, 0.0], 2)")
                .unwrap(),
        );
        assert_eq!(rs.columns, vec!["id", "_distance"]);
        assert_eq!(rs.rows.len(), 2);
        assert_eq!(rs.rows[0][0], Value::Int(1));
        assert_eq!(rs.rows[1][0], Value::Int(4));
        let d0 = match rs.rows[0][1] {
            Value::Float(f) => f,
            ref other => panic!("expected float distance, got {other:?}"),
        };
        let d1 = match rs.rows[1][1] {
            Value::Float(f) => f,
            ref other => panic!("expected float distance, got {other:?}"),
        };
        assert!(d0 <= d1, "results must be nearest-first ({d0} > {d1})");

        let rs = rows(
            nc.execute(
                "SELECT id FROM docs NEAREST (embedding, [1.0, 0.0, 0.0], 2) WHERE cat = 'a'",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Int(1)], vec![Value::Int(3)]]);

        // Unindexed path and disallowed combinations error cleanly.
        let err = na
            .execute("SELECT id FROM docs NEAREST (missing, [1.0, 0.0, 0.0], 2)")
            .unwrap_err();
        assert!(err.to_string().contains("no vector index"), "got: {err}");
        let err = na
            .execute("SELECT id FROM docs NEAREST (embedding, [1.0, 0.0, 0.0], 2) ORDER BY id")
            .unwrap_err();
        assert!(err.to_string().contains("ORDER BY"), "got: {err}");
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
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
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
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
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
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
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
                hlc: Hlc::new(1, 0),
            },
        )
        .unwrap();
        assert!(!has(&c, &k1), "c has no row before handoff");

        let delivered = na.flush_hints();
        assert!(delivered >= 1, "the buffered write was handed off to c");
        assert!(has(&c, &k1), "c received id=1 via hinted handoff");
    }

    #[test]
    fn paged_repair_converges_tables_larger_than_one_page() {
        use skaidb_types::Document;
        // Divergence spanning many repair pages (REPAIR_PAGE_ROWS is 8 under
        // test): node a holds 40 rows node b lacks, node b holds 10 newer
        // versions node a lacks. One repair() from a must converge both
        // directions with paged scans.
        let q = Consistency::Quorum;
        let cfg2 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: 2,
            vnodes_per_node: 64,
            read_consistency: q,
            write_consistency: q,
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
        };
        let (a, b) = (free_addr(), free_addr());
        let m = vec![member("a", &a), member("b", &b)];
        let na = Node::new(Database::open(temp_dir("pra")).unwrap(), cfg2("a", &a, &m));
        let nb = Node::new(Database::open(temp_dir("prb")).unwrap(), cfg2("b", &b, &m));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();

        let enc = |id: i64, v: i64| -> (Vec<u8>, Vec<u8>) {
            let mut doc = Document::new();
            doc.insert("id", Value::Int(id));
            doc.insert("v", Value::Int(v));
            (
                Value::Array(vec![Value::Int(id)]).encode_key(),
                Value::Document(doc).encode(),
            )
        };
        let inject = |addr: &str, id: i64, v: i64, hlc: Hlc| {
            let (key, val) = enc(id, v);
            let r = internode::call(
                addr,
                &Request::ApplyPut {
                    table: "t".into(),
                    key,
                    value: val,
                    hlc,
                },
            )
            .unwrap();
            assert!(matches!(r, Response::Ack));
        };

        // 40 rows only on a (b missing them entirely)…
        for id in 0..40 {
            inject(&a, id, id, Hlc::new(100 + id as u64, 0));
        }
        // …and 10 of those ids with NEWER versions only on b.
        for id in 0..10 {
            inject(&b, id, 1000 + id, Hlc::new(500 + id as u64, 0));
        }

        let repaired = na.repair().unwrap();
        assert!(repaired >= 40, "expected ≥40 rows repaired, got {repaired}");

        // Both nodes now agree: 40 rows, ids 0..9 at the newer b version.
        for node in [&na, &nb] {
            let rs = rows(node.execute("SELECT id, v FROM t ORDER BY id").unwrap());
            assert_eq!(rs.rows.len(), 40);
            for id in 0..40i64 {
                let expect = if id < 10 { 1000 + id } else { id };
                assert_eq!(
                    rs.rows[id as usize],
                    vec![Value::Int(id), Value::Int(expect)],
                    "row {id} mismatch"
                );
            }
        }
    }

    #[test]
    fn paged_cluster_scan_merges_divergent_multi_page_shards() {
        use skaidb_types::Document;
        // A full-table SELECT gathers every member's shard in ScanPage-sized
        // pages (8 under test) and merges by last-writer-wins. Shards are
        // left deliberately divergent — no repair — so the merge itself must
        // reconcile them across many pages: a holds 40 rows b lacks, b holds
        // 10 newer versions a lacks.
        let q = Consistency::Quorum;
        let cfg2 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: 2,
            vnodes_per_node: 64,
            read_consistency: q,
            write_consistency: q,
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
        };
        let (a, b) = (free_addr(), free_addr());
        let m = vec![member("a", &a), member("b", &b)];
        let na = Node::new(Database::open(temp_dir("psa")).unwrap(), cfg2("a", &a, &m));
        let nb = Node::new(Database::open(temp_dir("psb")).unwrap(), cfg2("b", &b, &m));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();

        let inject = |addr: &str, id: i64, v: i64, hlc: Hlc| {
            let mut doc = Document::new();
            doc.insert("id", Value::Int(id));
            doc.insert("v", Value::Int(v));
            let r = internode::call(
                addr,
                &Request::ApplyPut {
                    table: "t".into(),
                    key: Value::Array(vec![Value::Int(id)]).encode_key(),
                    value: Value::Document(doc).encode(),
                    hlc,
                },
            )
            .unwrap();
            assert!(matches!(r, Response::Ack));
        };
        for id in 0..40 {
            inject(&a, id, id, Hlc::new(100 + id as u64, 0));
        }
        for id in 0..10 {
            inject(&b, id, 1000 + id, Hlc::new(500 + id as u64, 0));
        }

        // Scanned from either coordinator, the merged view is identical:
        // all 40 rows, ids 0..9 at b's newer version.
        for node in [&na, &nb] {
            let rs = rows(node.execute("SELECT id, v FROM t ORDER BY id").unwrap());
            assert_eq!(rs.rows.len(), 40);
            for id in 0..40i64 {
                let expect = if id < 10 { 1000 + id } else { id };
                assert_eq!(
                    rs.rows[id as usize],
                    vec![Value::Int(id), Value::Int(expect)],
                    "row {id} mismatch"
                );
            }
        }
    }

    #[test]
    fn timeseries_cluster_end_to_end() {
        // 3 nodes, rf=3, QUORUM: TS DDL broadcasts, samples replicate by
        // series placement, queries union-merge across members.
        let q = Consistency::Quorum;
        let cfg3 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: 3,
            vnodes_per_node: 64,
            read_consistency: q,
            write_consistency: q,
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
        };
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let m = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(Database::open(temp_dir("tsa")).unwrap(), cfg3("a", &a, &m));
        let nb = Node::new(Database::open(temp_dir("tsb")).unwrap(), cfg3("b", &b, &m));
        let nc = Node::new(Database::open(temp_dir("tsc")).unwrap(), cfg3("c", &c, &m));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        nc.serve_internode().unwrap();

        // DDL broadcast from one node; visible on another.
        na.execute("CREATE TIMESERIES TABLE cpu (SERIES KEY (host), RETENTION 30d)")
            .unwrap();
        // Inserts via different coordinators.
        na.execute("INSERT INTO cpu (host, ts, value) VALUES ('x', 0, 0), ('x', 15000, 15)")
            .unwrap();
        nb.execute("INSERT INTO cpu (host, ts, value) VALUES ('x', 30000, 30), ('y', 0, 100)")
            .unwrap();

        // Query via the third coordinator: full view, correct math.
        let rs = rows(
            nc.execute("SELECT ts, value FROM cpu WHERE host = 'x' ORDER BY ts")
                .unwrap(),
        );
        assert_eq!(rs.rows.len(), 3);
        assert_eq!(rs.rows[2][1], Value::Float(30.0));
        let rs = rows(nc.execute("SELECT rate(value) FROM cpu WHERE host = 'x'").unwrap());
        assert_eq!(rs.rows[0][0], Value::Float(1.0));
        for node in [&na, &nb, &nc] {
            let rs = rows(node.execute("SELECT last(value) FROM cpu WHERE host = 'y'").unwrap());
            assert_eq!(rs.rows[0][0], Value::Float(100.0));
        }

        // Union-merge covers a replica that has data the others miss: inject
        // a sample directly into one node's local store only.
        let r = internode::call(
            &a,
            &Request::TsAppend {
                table: "cpu".into(),
                rows: vec![(
                    vec![
                        ("__field__".into(), "value".into()),
                        ("host".into(), "z".into()),
                    ],
                    5_000,
                    7.0,
                )],
            },
        )
        .unwrap();
        assert!(matches!(r, Response::Ack));
        let rs = rows(nb.execute("SELECT value FROM cpu WHERE host = 'z'").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Float(7.0)]]);

        // Append-only holds in cluster mode too.
        let err = nb.execute("UPDATE cpu SET value = 1 WHERE host = 'x'").unwrap_err();
        assert!(err.to_string().contains("append-only"), "{err}");

        // Topology changes are refused while TS tables exist.
        let err = na.add_member("d", "127.0.0.1:1").unwrap_err();
        assert!(err.to_string().contains("time-series"), "{err}");

        // Dropping the TS table lifts the guard (the join still fails to
        // reach the bogus address, but past the TS check).
        na.execute("DROP TABLE cpu").unwrap();
        let err = na.add_member("d", "127.0.0.1:1").unwrap_err();
        assert!(!err.to_string().contains("time-series"), "{err}");
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
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
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

        // (a) read-repair: write id=1 to a and b only; c is missing it. A
        // quorum read consults only a quorum of replicas (not all of them),
        // so it may not touch c; a read at ALL consults — and repairs —
        // every replica.
        let (k1, v1) = row(1, 100);
        inject(&a, &k1, &v1, Hlc::new(1000, 0));
        inject(&b, &k1, &v1, Hlc::new(1000, 0));
        assert!(!has(&c, &k1), "c starts without id=1");
        let rs = rows(na.execute("SELECT v FROM t WHERE id = 1").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(100)]]);
        let out = na
            .execute_session_with(
                DEFAULT_DATABASE,
                "SELECT v FROM t WHERE id = 1",
                Some(Consistency::All),
            )
            .unwrap();
        let rs = match out {
            SessionEffect::Output(o) => rows(o),
            SessionEffect::UseDatabase(_) => unreachable!(),
        };
        assert_eq!(rs.rows, vec![vec![Value::Int(100)]]);
        assert!(has(&c, &k1), "ALL read repaired c");

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
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
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
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
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
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
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
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
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
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
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
