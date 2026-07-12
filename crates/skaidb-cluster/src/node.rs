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
use std::time::{Duration, Instant};
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
use crate::memguard::MemoryGuard;
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
/// Resolved rows of a sorted search shard: `(key, row document)`.
type SortedRows = Vec<(Vec<u8>, Document)>;

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
    /// Serializes appends to the per-replica on-disk hint logs (the overflow
    /// past the in-memory cap), so a persistently-behind replica loses no
    /// writes to a bounded memory buffer.
    hint_spill: Mutex<()>,
    /// Approx. count of hints spilled to disk and not yet delivered (for the
    /// `hints_pending` accounting / metric).
    disk_hints: AtomicU64,
    /// Buffered time-series batches for unreachable replicas, replayed via
    /// the gap-filling `TsMerge` on recovery (per peer: `(table, rows)`).
    ts_hints: Mutex<HashMap<NodeId, Vec<TsHint>>>,
    /// Highest HLC each peer has confirmed it applied (a successful replicated
    /// write or hint replay). Compared against [`Node::write_watermark`] to
    /// estimate how far behind a peer is — see [`Node::note_acked`] and the
    /// per-peer replication-lag metric. Peers absent from the map have no
    /// confirmed write yet (their lag is reported as unknown).
    acked: Mutex<HashMap<NodeId, Hlc>>,
    /// Last host-stats snapshot per peer `(stats, taken_at, was_reachable)` —
    /// smooths dashboard liveness over missed probes (see
    /// [`Node::cluster_host_stats`]).
    host_cache: Mutex<HashMap<NodeId, (crate::host::HostStats, Instant, bool)>>,
    /// QoS: bounds concurrent inbound bulk appliers (see [`BulkGate`]).
    bulk_gate: BulkGate,
    /// True while an anti-entropy pass runs here (any trigger: the hourly
    /// loop or an admin REPAIR). Served to peers in HostStats so they defer
    /// their own pass — concurrent passes dent write quorum.
    repairing: AtomicBool,
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
    /// Memory-pressure load shedding: under memory pressure the write path
    /// rejects new writes so the node can drain and survive instead of being
    /// OOM-killed. Sampled by a background thread ([`MemoryGuard`]).
    mem: MemoryGuard,
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
    /// Memory-pressure load shedding: whether the node is currently rejecting
    /// writes, and its last sampled usage / limit (bytes; limit 0 = no limit).
    pub shedding_writes: bool,
    pub memory_used_bytes: u64,
    pub memory_limit_bytes: u64,
}

/// A buffered write awaiting handoff to a recovered replica:
/// `(table, key, op, hlc)`.
type HintedWrite = (String, Vec<u8>, WriteOp, Hlc);

/// One batched row write `(key, value, hlc, is_put)` — the [`Response::Scan`]
/// row shape carried by [`Request::ApplyBatch`]; `is_put == false` marks a
/// tombstone (delete, empty value).
type BatchRow = (Vec<u8>, Vec<u8>, Hlc, bool);
/// One buffered time-series hint: the table and its undelivered samples.
type TsHint = (String, Vec<(skaidb_tsdb::Labels, i64, f64)>);

/// Rows per anti-entropy scan page (both the local and the remote side), and
/// per flushed repair batch: bounds repair memory and each repair RPC/fsync,
/// independent of table size.
#[cfg(not(test))]
const REPAIR_PAGE_ROWS: usize = 2_000;

/// Pause between repair page fills / (table, peer) pairs: repair is a
/// background op and must never saturate a node. ~5% duty impact on pass
/// wall-time per page, whole cores handed back to foreground queries.
const REPAIR_PAGE_PAUSE: Duration = Duration::from_millis(25);
/// Pause between (table, peer) reconciliations in a pass.
const REPAIR_PAIR_PAUSE: Duration = Duration::from_millis(250);
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

/// Rows per local scan page in the topology-change passes (rebalance,
/// drain, reclaim): bounds the sender's transient memory to one page plus
/// the in-flight batch, independent of shard size — the same fix that
/// paged repair and distributed gathers.
#[cfg(not(test))]
const MIGRATE_PAGE_ROWS: usize = 2_000;
#[cfg(test)]
const MIGRATE_PAGE_ROWS: usize = 8;

/// QoS admission control for inbound bulk work: at most `max` concurrent
/// `ApplyBatch` appliers run at once (excess connection threads wait).
///
/// Decode + memtable + WAL + index updates per batch are CPU-heavy, and the
/// internode server runs one thread per connection — during a decommission
/// drain an unbounded flood of appliers monopolized every core and starved
/// foreground queries (measured: cpu PSI ~80%, io PSI ~0, on nodes that were
/// answering probes fine). Capping appliers keeps cores free for queries;
/// senders that queue too long hit their I/O timeout and degrade to hints —
/// backpressure with a durable fallback, not silent loss.
#[derive(Debug)]
struct BulkGate {
    max: usize,
    active: std::sync::Mutex<usize>,
    cv: std::sync::Condvar,
}

impl BulkGate {
    fn new() -> Self {
        // Half the cores, clamped: bulk work can use real parallelism on big
        // hosts but never the whole machine.
        let max = std::thread::available_parallelism()
            .map(|n| (n.get() / 2).clamp(1, 4))
            .unwrap_or(2);
        BulkGate {
            max,
            active: std::sync::Mutex::new(0),
            cv: std::sync::Condvar::new(),
        }
    }

    /// Try to admit an applier, waiting at most `wait`. `None` = the gate is
    /// saturated: respond "busy" fast so the sender degrades to a hint.
    ///
    /// The wait MUST be bounded and short. The first unbounded version parked
    /// each excess connection thread on the condvar forever; under a
    /// catch-up flood (a rejoining node facing hint drains + repair pushes
    /// from every peer) senders timed out and retried on fresh connections
    /// while their abandoned server threads stayed queued — observed in
    /// production as 2 800+ threads (~23 GB of stack VIRT) on a node that made
    /// no progress. Bounding the wait lets an abandoned thread notice the
    /// dead socket (its response write fails) and exit; hinted handoff and
    /// anti-entropy already guarantee no write is lost to a `Busy` reply.
    fn acquire(&self, wait: Duration) -> Option<BulkPermit<'_>> {
        let deadline = Instant::now() + wait;
        let mut n = self.active.lock().expect("bulk gate lock");
        while *n >= self.max {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            let (guard, timeout) = self.cv.wait_timeout(n, left).expect("bulk gate wait");
            n = guard;
            if timeout.timed_out() && *n >= self.max {
                return None;
            }
        }
        *n += 1;
        Some(BulkPermit(self))
    }
}

/// How long an inbound bulk applier may wait for gate admission before the
/// node answers "busy". Short by design: the queue exists to smooth bursts,
/// not to buffer a flood — senders' 10s I/O timeout means anything parked
/// longer is likely already abandoned, and durability comes from the hint
/// path, not from queueing.
const BULK_ADMISSION_WAIT: Duration = Duration::from_secs(2);

/// How long an inbound repair scan may wait for the engine read lock before
/// answering "busy". During a rejoin catch-up the admitted bulk appliers keep
/// the engine write lock near-continuously; reader threads parked on the
/// lock outlive their callers (senders time out at 10s and hang up) and were
/// observed piling into the thousands — each a zombie serving a dead socket
/// whenever the lock finally frees. Repair treats a busy peer as unreachable
/// for that (table, peer) pass and retries next interval, so failing fast
/// loses nothing.
const SCAN_LOCK_WAIT: Duration = Duration::from_millis(1500);

/// Result of pre-filtering a replica-apply chunk against local versions
/// (see [`Node::filter_newer_rows`]); the all-fresh common case avoids
/// cloning row payloads.
enum RowFilter {
    AllFresh,
    AllStale,
    Some(Vec<BatchRow>),
}

/// RAII permit from [`BulkGate::acquire`].
struct BulkPermit<'a>(&'a BulkGate);

impl Drop for BulkPermit<'_> {
    fn drop(&mut self) {
        *self.0.active.lock().expect("bulk gate lock") -= 1;
        self.0.cv.notify_one();
    }
}

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

/// Cap on **in-memory** buffered hints per replica; overflow spills to a
/// per-replica on-disk hint log (durable, bounded memory) rather than being
/// dropped, so a persistently-behind replica loses no writes.
#[cfg(not(test))]
const MAX_HINTS_PER_REPLICA: usize = 4096;
/// Tiny in tests so a handful of writes exercises the disk-spill path.
#[cfg(test)]
const MAX_HINTS_PER_REPLICA: usize = 4;

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
            hint_spill: Mutex::new(()),
            disk_hints: AtomicU64::new(0),
            ts_hints: Mutex::new(HashMap::new()),
            acked: Mutex::new(HashMap::new()),
            host_cache: Mutex::new(HashMap::new()),
            bulk_gate: BulkGate::new(),
            repairing: AtomicBool::new(false),
            write_watermark: AtomicU64::new(0),
            membership_lock: Mutex::new(()),
            bg,
            mem: MemoryGuard::new(),
            hint_flush_queued: AtomicBool::new(false),
            migration_batch: AtomicUsize::new(1024),
            migration_pause_ms: AtomicU64::new(0),
            counters: Counters::default(),
            cfg,
        });
        // Seed the disk-hint counter from the directory: it is process-local,
        // and `hints_pending()` — the gate that queues a hint flush at all —
        // trusts it. A restarted node with zero new failures otherwise never
        // notices the hint logs it inherited (observed: a 1.8 GB log for a
        // recovered peer sat untouched forever because the gate never fired;
        // the directory check inside `flush_hints` was unreachable). The
        // sentinel self-corrects on the first drain.
        if node.has_disk_hints() {
            node.disk_hints.store(1, Ordering::Relaxed);
            skaidb_types::slog!("skaidb: inherited on-disk hint logs found — queueing drain");
        }
        // Memory-pressure sampler: holds a `Weak`, so it exits when the node is
        // dropped. Updates the shedding flag every `SAMPLE_INTERVAL`.
        let mem_weak = Arc::downgrade(&node);
        thread::spawn(move || {
            use crate::memguard::Pressure;
            // Release actions are paced (not every 1s tick — a flush + purge
            // storm is its own denial of service) and shedding is loudly
            // logged: both production wedges crept to the ceiling in silence.
            const RELEASE_EVERY: Duration = Duration::from_secs(10);
            const DISTRESS_EVERY: Duration = Duration::from_secs(60);
            let mut last_release: Option<Instant> = None;
            let mut shed_since: Option<Instant> = None;
            let mut last_distress: Option<Instant> = None;
            while let Some(node) = mem_weak.upgrade() {
                let pressure = node.mem.sample_pressure();
                if pressure >= Pressure::Release
                    && last_release.is_none_or(|t| t.elapsed() >= RELEASE_EVERY)
                {
                    last_release = Some(Instant::now());
                    // Free what we can ourselves: a shedding node rejects the
                    // client writes that would otherwise trigger a flush, so
                    // without this it deadlocks (sheds → no write → no flush →
                    // stays shedding), and the per-engine flush threshold
                    // misses pressure spread thin across many tables. Then
                    // hand freed pages back to the OS — under a cgroup limit,
                    // allocator-retained pages are what strangle the file
                    // cache and start the major-fault storm.
                    let reclaimed = match node.local.write() {
                        Ok(mut db) => {
                            db.release_memory_under_pressure(pressure == Pressure::Shed)
                        }
                        Err(_) => 0,
                    };
                    if reclaimed > 0 {
                        skaidb_types::slog!(
                            "skaidb: memory pressure — flushed {} MB of memtables, committed search writers",
                            reclaimed / (1024 * 1024)
                        );
                    }
                }
                let (used, limit) = node.mem.snapshot();
                let breakdown = || {
                    let mut s = crate::memguard::anon_file_breakdown()
                        .map_or(String::new(), |(a, f)| {
                            format!(
                                " (anon {} MB, file {} MB)",
                                a / (1024 * 1024),
                                f / (1024 * 1024)
                            )
                        });
                    if let Some(alloc) = crate::memguard::alloc_stats() {
                        s.push_str(&format!(" [{alloc}]"));
                    }
                    s
                };
                match (pressure == Pressure::Shed, shed_since) {
                    (true, None) => {
                        shed_since = Some(Instant::now());
                        last_distress = Some(Instant::now());
                        skaidb_types::slog!(
                            "skaidb: memory pressure — SHEDDING writes at {}/{} MB{}",
                            used / (1024 * 1024),
                            limit / (1024 * 1024),
                            breakdown()
                        );
                    }
                    (true, Some(since)) => {
                        if last_distress.is_none_or(|t| t.elapsed() >= DISTRESS_EVERY) {
                            last_distress = Some(Instant::now());
                            skaidb_types::slog!(
                                "skaidb: memory pressure — still shedding after {}s at {}/{} MB{} — releases are not freeing enough; OOM risk",
                                since.elapsed().as_secs(),
                                used / (1024 * 1024),
                                limit / (1024 * 1024),
                                breakdown()
                            );
                        }
                    }
                    (false, Some(since)) => {
                        shed_since = None;
                        skaidb_types::slog!(
                            "skaidb: memory pressure — recovered after {}s, now {}/{} MB",
                            since.elapsed().as_secs(),
                            used / (1024 * 1024),
                            limit / (1024 * 1024)
                        );
                    }
                    (false, None) => {}
                }
                drop(node); // don't hold the node alive across the sleep
                thread::sleep(crate::memguard::SAMPLE_INTERVAL);
            }
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
            .sum::<usize>()
            + self.disk_hints.load(Ordering::Relaxed) as usize;
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
            shedding_writes: self.mem.shedding(),
            memory_used_bytes: self.mem.snapshot().0,
            memory_limit_bytes: self.mem.snapshot().1,
        }
    }

    /// Force the memory-shedding flag (tests only).
    #[cfg(test)]
    pub(crate) fn set_shedding_for_test(&self, on: bool) {
        self.mem.force(on);
    }

    /// Host system statistics for every member: this node sampled locally,
    /// peers over internode. Ordered by node id.
    ///
    /// A peer that misses one probe is **not** immediately reported down —
    /// that made the dashboard flap between "live" and "unreachable" on any
    /// transient network blip or busy scrape. Instead its last snapshot is
    /// served with `stale_secs` set (the UI dims it and shows "last seen"),
    /// and only past [`HOST_STALE_HORIZON`] does it become `None`
    /// (unreachable). Transitions are logged once each way.
    pub fn cluster_host_stats(self: &Arc<Self>) -> Vec<(String, Option<crate::host::HostStats>)> {
        /// Serve cached stats for a silent peer this long before calling it down.
        const HOST_STALE_HORIZON: Duration = Duration::from_secs(120);
        let dir = self
            .local
            .read()
            .ok()
            .map(|db| db.dir().to_path_buf())
            .unwrap_or_default();
        let mut out = vec![(self.id.0.clone(), Some(crate::host::sample(&dir)))];
        let peers: Vec<(NodeId, String)> = self
            .members_snapshot()
            .into_iter()
            .filter(|(id, _)| *id != self.id)
            .collect();
        let addrs: Vec<String> = peers.iter().map(|(_, a)| a.clone()).collect();
        let stats = scatter(&addrs, |addr| {
            match self.pool.call_timeout(addr, &Request::HostStats, PROBE_TIMEOUT) {
                Ok(Response::HostStats { json }) => {
                    serde_json::from_str::<crate::host::HostStats>(&json).ok()
                }
                _ => None,
            }
        });
        let mut cache = self.host_cache.lock().expect("host cache lock");
        for ((id, _), stat) in peers.into_iter().zip(stats) {
            let entry = match stat {
                Some(s) => {
                    if matches!(cache.get(&id), Some((_, _, false))) {
                        skaidb_types::slog!("skaidb: peer {} answering probes again", id.0);
                    }
                    cache.insert(id.clone(), (s.clone(), Instant::now(), true));
                    Some(s)
                }
                None => match cache.get_mut(&id) {
                    Some((cached, last_ok, was_ok)) if last_ok.elapsed() < HOST_STALE_HORIZON => {
                        if *was_ok {
                            skaidb_types::slog!(
                                "skaidb: peer {} missed a stats probe — serving cached stats \
                                 until it answers or {}s pass",
                                id.0,
                                HOST_STALE_HORIZON.as_secs()
                            );
                            *was_ok = false;
                        }
                        let mut s = cached.clone();
                        s.stale_secs = last_ok.elapsed().as_secs().max(1);
                        Some(s)
                    }
                    _ => None,
                },
            };
            out.push((id.0, entry));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// This node's data directory (for host-stats disk sampling).
    pub fn data_dir(&self) -> Option<std::path::PathBuf> {
        self.local.read().ok().map(|db| db.dir().to_path_buf())
    }

    /// Storage/runtime statistics for this node's local engine (for metrics).
    /// Bounded lock wait: stats are periodic and best-effort, and every
    /// scrape/probe thread that parks unboundedly behind a long write-lock
    /// tenure (index rebuild, bulk apply) is a leak — the production pile of
    /// stats threads stacked up at ~90/min behind one rebuild. `None` under
    /// contention; callers already treat that as "stats unavailable".
    pub fn db_stats(&self, per_table: bool) -> Option<DbStats> {
        self.local_read_bounded().map(|db| db.stats(per_table))
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
    /// One node's shard of a sharded aggregation: the aggregation over the
    /// LOCAL index restricted to this node's **primary-owned** placement
    /// arcs at `epoch`. Declines (`None`) when the epoch differs, a
    /// membership change is in flight (dual-ring placement — a key may be
    /// mid-move), this node is not in the ring, or the index cannot serve
    /// the shape exactly.
    pub(crate) fn search_agg_shard(
        &self,
        table: &str,
        query: &skaidb_fts::SearchQuery,
        agg: &skaidb_fts::AggRequest,
        epoch: u64,
    ) -> EngineResult<Option<Vec<skaidb_fts::AggRow>>> {
        let arcs = {
            let topo = self.topo.read().expect("topo lock");
            if topo.epoch != epoch || topo.prev.is_some() {
                return Ok(None);
            }
            topo.ring.primary_arcs(&self.id)
        };
        if arcs.is_empty() {
            return Ok(None);
        }
        self.local
            .write()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .search_aggregate(table, query, agg, Some(&arcs))
    }

    /// Sharded scatter-gather aggregation (RF < members): every member
    /// aggregates its primary-owned key-space from its local index and the
    /// coordinator merges the partials — each key counted by exactly one
    /// replica (the ring arcs tile the hash space). Exact-or-decline:
    /// requires a stable epoch across the whole gather, no membership
    /// change in flight, EVERY member answering, and metrics whose
    /// partials merge losslessly (count/value_count/sum/min/max; grouped
    /// requests are doc-count-only via the index-level guard). Anything
    /// else returns `Ok(None)` and the caller falls back to the deduped
    /// row gather.
    pub(crate) fn search_aggregate_sharded(
        &self,
        table: &str,
        query: &skaidb_fts::SearchQuery,
        agg: &skaidb_fts::AggRequest,
    ) -> EngineResult<Option<Vec<skaidb_fts::AggRow>>> {
        use skaidb_fts::AggMetricFunc as F;
        // Mergeable shapes only. COUNT(DISTINCT)/APPROX_COUNT_DISTINCT
        // would need term sets or sketches on the wire — decline. AVG
        // merges via a rewrite: each AVG(col) scatters as SUM(col) +
        // COUNT(col) and the coordinator divides after the merge.
        if agg
            .metrics
            .iter()
            .any(|m| matches!(m.func, F::CountDistinct | F::ApproxCountDistinct))
        {
            return Ok(None);
        }
        let has_avg = agg.metrics.iter().any(|m| m.func == F::Avg);
        let original_funcs: Vec<F> = agg.metrics.iter().map(|m| m.func).collect();
        let scatter_agg = if has_avg {
            let mut metrics = Vec::with_capacity(agg.metrics.len() + 1);
            for m in &agg.metrics {
                if m.func == F::Avg {
                    metrics.push(skaidb_fts::AggMetric {
                        func: F::Sum,
                        column: m.column.clone(),
                    });
                    metrics.push(skaidb_fts::AggMetric {
                        func: F::ValueCount,
                        column: m.column.clone(),
                    });
                } else {
                    metrics.push(m.clone());
                }
            }
            std::borrow::Cow::Owned(skaidb_fts::AggRequest {
                group_by: agg.group_by.clone(),
                metrics,
            })
        } else {
            std::borrow::Cow::Borrowed(agg)
        };
        let agg = scatter_agg.as_ref();
        let (epoch, resharding) = {
            let topo = self.topo.read().expect("topo lock");
            (topo.epoch, topo.prev.is_some())
        };
        if resharding {
            return Ok(None);
        }
        let Some(local) = self.search_agg_shard(table, query, agg, epoch)? else {
            return Ok(None);
        };
        let peers = self.peers_with_ids();
        let query_json = serde_json::to_string(query)
            .map_err(|e| EngineError::Cluster(format!("encode query: {e}")))?;
        let agg_json = serde_json::to_string(agg)
            .map_err(|e| EngineError::Cluster(format!("encode agg: {e}")))?;
        let shards = scatter(&peers, |(_, addr)| {
            self.counters.peer_requests.fetch_add(1, Ordering::Relaxed);
            match self.pool.call(
                addr,
                &Request::SearchAgg {
                    table: table.to_string(),
                    query: query_json.clone(),
                    agg: agg_json.clone(),
                    epoch,
                },
            ) {
                Ok(Response::SearchAggRows { rows: Some(rows) }) => {
                    crate::internode::decode_agg_rows(&rows).ok()
                }
                _ => None,
            }
        });
        let mut parts = vec![local];
        for shard in shards {
            match shard {
                // A declining, unreachable, or too-old peer means some
                // key-space would go uncounted — fall back.
                Some(rows) => parts.push(rows),
                None => return Ok(None),
            }
        }
        // The ring must not have moved during the gather: every responder
        // checked `epoch` before answering, and this re-check closes the
        // window where the coordinator itself learns of a change late.
        {
            let topo = self.topo.read().expect("topo lock");
            if topo.epoch != epoch || topo.prev.is_some() {
                return Ok(None);
            }
        }
        let mut rows = merge_agg_shards(agg, parts);
        if has_avg {
            // Collapse each scattered SUM+COUNT pair back into the
            // requested AVG (Float; NULL over an empty value set — SQL
            // semantics, matching the unsharded pushdown).
            for row in &mut rows {
                let mut merged = row.metrics.drain(..);
                let mut out = Vec::with_capacity(original_funcs.len());
                for func in &original_funcs {
                    if *func == F::Avg {
                        let sum = merged.next().unwrap_or(skaidb_types::Value::Null);
                        let count = merged.next().unwrap_or(skaidb_types::Value::Null);
                        let n = match count {
                            skaidb_types::Value::Int(n) => n,
                            _ => 0,
                        };
                        let total = match sum {
                            skaidb_types::Value::Int(v) => Some(v as f64),
                            skaidb_types::Value::Float(v) => Some(v),
                            _ => None,
                        };
                        out.push(match total {
                            Some(t) if n > 0 => skaidb_types::Value::Float(t / n as f64),
                            _ => skaidb_types::Value::Null,
                        });
                    } else {
                        out.push(merged.next().unwrap_or(skaidb_types::Value::Null));
                    }
                }
                drop(merged);
                row.metrics = out;
            }
        }
        Ok(Some(rows))
    }

    /// One node's shard of a sharded sorted-top-k: fast-field-ordered top
    /// `k` of its primary-owned key-space, rows fully resolved (with any
    /// requested highlights). Declines like [`Node::search_agg_shard`].
    pub(crate) fn search_sorted_shard(
        &self,
        table: &str,
        query: &skaidb_fts::SearchQuery,
        sort: &skaidb_fts::SortSpec,
        k: usize,
        highlights: &[(String, usize)],
        epoch: u64,
    ) -> EngineResult<Option<SortedRows>> {
        let arcs = {
            let topo = self.topo.read().expect("topo lock");
            if topo.epoch != epoch || topo.prev.is_some() {
                return Ok(None);
            }
            topo.ring.primary_arcs(&self.id)
        };
        if arcs.is_empty() {
            return Ok(None);
        }
        self.local
            .write()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .search_sorted(table, query, sort, k, &None, highlights, Some(&arcs))
    }

    /// Sharded sorted top-k (RF < members): every member resolves its own
    /// primary-owned top `k` and the coordinator k-way merges by the sort
    /// column — the global top-k rows each live in exactly one member's
    /// owned set, so the union of per-shard top-k lists contains them all.
    /// Exact-or-decline like the aggregation scatter; additionally
    /// declines when a residual SQL filter is present (filters do not
    /// travel; the fallback applies them at the coordinator).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn search_sorted_sharded(
        &self,
        table: &str,
        query: &skaidb_fts::SearchQuery,
        sort: &skaidb_fts::SortSpec,
        k: usize,
        filter: &Option<Expr>,
        highlights: &[(String, usize)],
    ) -> EngineResult<Option<SortedRows>> {
        if filter.is_some() {
            return Ok(None);
        }
        let (epoch, resharding) = {
            let topo = self.topo.read().expect("topo lock");
            (topo.epoch, topo.prev.is_some())
        };
        if resharding {
            return Ok(None);
        }
        let Some(local) = self.search_sorted_shard(table, query, sort, k, highlights, epoch)?
        else {
            return Ok(None);
        };
        let peers = self.peers_with_ids();
        let query_json = serde_json::to_string(query)
            .map_err(|e| EngineError::Cluster(format!("encode query: {e}")))?;
        let sort_json = serde_json::to_string(sort)
            .map_err(|e| EngineError::Cluster(format!("encode sort: {e}")))?;
        let wire_highlights: Vec<(String, u32)> = highlights
            .iter()
            .map(|(c, m)| (c.clone(), *m as u32))
            .collect();
        let shards = scatter(&peers, |(_, addr)| {
            self.counters.peer_requests.fetch_add(1, Ordering::Relaxed);
            match self.pool.call(
                addr,
                &Request::SearchSorted {
                    table: table.to_string(),
                    query: query_json.clone(),
                    sort: sort_json.clone(),
                    k: k as u32,
                    highlights: wire_highlights.clone(),
                    epoch,
                },
            ) {
                Ok(Response::SortedRows { rows: Some(rows) }) => {
                    crate::internode::decode_sorted_rows(&rows).ok()
                }
                _ => None,
            }
        });
        let mut parts = vec![local];
        for shard in shards {
            match shard {
                Some(rows) => parts.push(rows),
                None => return Ok(None),
            }
        }
        {
            let topo = self.topo.read().expect("topo lock");
            if topo.epoch != epoch || topo.prev.is_some() {
                return Ok(None);
            }
        }
        // Merge by the sort column's key-encoded order (order-preserving
        // for a single fast-field type, exactly what each shard sorted by);
        // rows missing the column cannot occur — every shard declined if
        // any of its matching rows lacked it.
        let mut all: Vec<(Vec<u8>, Document)> = parts.into_iter().flatten().collect();
        all.sort_by(|(_, a), (_, b)| {
            let ka = a.get_path(&sort.column).map(|v| v.encode_key());
            let kb = b.get_path(&sort.column).map(|v| v.encode_key());
            let ord = ka.cmp(&kb);
            if sort.descending {
                ord.reverse()
            } else {
                ord
            }
        });
        all.truncate(k);
        Ok(Some(all))
    }

    /// Per-hit score explain, routed to a **replica of the key**: the
    /// local index when this node replicates the row, else a forward to
    /// its owners in ring order. Works at any RF — the answering index
    /// only needs to hold that one row.
    pub fn search_explain(
        &self,
        table: &str,
        filter: &Option<skaidb_sql::ast::Expr>,
        pk_value: &skaidb_types::Value,
    ) -> EngineResult<Option<String>> {
        let Some(query) = skaidb_engine::filter_search_query(filter)? else {
            return Err(EngineError::Type(
                "score explain needs a MATCH()/SEARCH() predicate".into(),
            ));
        };
        let key = skaidb_types::Value::Array(vec![pk_value.clone()]).encode_key();
        let replicas = self.replicas_for(&key);
        if replicas.contains(&self.id) {
            return self
                .local
                .write()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .search_explain_query(table, &query, pk_value);
        }
        let query_json = serde_json::to_string(&query)
            .map_err(|e| EngineError::Cluster(format!("encode query: {e}")))?;
        let pk = pk_value.encode();
        for owner in &replicas {
            let Some(addr) = self.peer_addr(owner) else {
                continue;
            };
            if let Ok(Response::Explanation { text }) = self.pool.call(
                &addr,
                &Request::SearchExplain {
                    table: table.to_string(),
                    query: query_json.clone(),
                    pk: pk.clone(),
                },
            ) {
                return Ok(text);
            }
        }
        Err(EngineError::Cluster(
            "no replica of the row is reachable for explain".into(),
        ))
    }

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
    /// RBAC check against the replicated catalog (see engine docs).
    pub fn has_privilege(
        &self,
        role: &str,
        privilege: skaidb_auth::Privilege,
        object: &skaidb_auth::Object,
    ) -> bool {
        self.local
            .read()
            .map(|db| db.has_privilege(role, privilege, object))
            .unwrap_or(false)
    }

    /// Stored SCRAM credential for a catalog user.
    pub fn auth_user(&self, name: &str) -> Option<skaidb_auth::ScramCredential> {
        self.local.read().ok()?.auth_user(name)
    }

    /// Cluster-wide per-bucket partials at the configured read
    /// consistency — the PromQL `query_range` fast path (per-bucket
    /// partials ship instead of raw samples).
    pub fn ts_partials_replicated(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
        t0: i64,
        t1: i64,
        bucket_ms: i64,
    ) -> EngineResult<Vec<(skaidb_tsdb::Labels, Vec<skaidb_engine::TsPartial>)>> {
        self.ts_partials_scatter(table, matchers, t0, t1, bucket_ms, None)
    }

    /// Cluster-wide time-series query at the configured read consistency
    /// (union-merged across members) — the PromQL/HTTP query path.
    pub fn ts_query_replicated(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
        t0: i64,
        t1: i64,
    ) -> EngineResult<Vec<(skaidb_tsdb::Labels, Vec<skaidb_tsdb::Sample>)>> {
        self.ts_scatter(table, matchers, t0, t1, None)
    }

    /// Replicated time-series append at the configured write consistency
    /// (the remote_write ingest path; SQL INSERT uses the same logic via
    /// the per-statement Coordinator).
    pub fn ts_append_replicated(
        self: &Arc<Self>,
        table: &str,
        rows: &[(skaidb_tsdb::Labels, i64, f64)],
    ) -> EngineResult<usize> {
        let mut coordinator = Coordinator {
            node: Arc::clone(self),
            oc: None,
        };
        Cluster::ts_append(&mut coordinator, table, rows)
    }

    /// Time-series anti-entropy: for each TS table and peer, compare
    /// per-series `(count, checksum)` summaries and push whole series that
    /// differ (or that the peer lacks) via `TsMerge`, which accepts
    /// any-aged samples. Only the series' elected sender (its primary under
    /// the current ring) pushes, and only to peers that replicate it.
    /// Duplicate chunks the merge creates fold away at compaction.
    fn ts_repair(&self) -> EngineResult<usize> {
        let mut repaired = 0usize;
        let tables = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .ts_table_names();
        for table in tables {
            let local: HashMap<skaidb_tsdb::Labels, (u64, u64)> = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .ts_summaries(&table)?
                .into_iter()
                .map(|(labels, count, sum)| (labels, (count, sum)))
                .collect();
            for (pid, addr) in self.peers_with_ids() {
                let theirs: HashMap<skaidb_tsdb::Labels, (u64, u64)> = match self
                    .pool
                    .call(&addr, &Request::TsSummary { table: table.clone() })
                {
                    Ok(Response::TsSummaries { series }) => series
                        .into_iter()
                        .map(|(labels, count, sum)| (labels, (count, sum)))
                        .collect(),
                    // Old peer or table missing there: schema sync/DDL will
                    // converge it; skip this pass.
                    _ => continue,
                };
                for (labels, mine) in &local {
                    let key = ts_placement_key(labels);
                    let replicas = self.replicas_for(&key);
                    if replicas.first() != Some(&self.id) || !replicas.contains(&pid) {
                        continue; // not this series' sender, or peer not a replica
                    }
                    if theirs.get(labels) == Some(mine) {
                        continue; // converged
                    }
                    self.ts_push_series_merge(&addr, &table, labels)?;
                    repaired += 1;
                }
            }
        }
        Ok(repaired)
    }

    /// Push one series' full history via `TsMerge` (repair: fills gaps of
    /// any age on the receiver).
    fn ts_push_series_merge(
        &self,
        addr: &str,
        table: &str,
        labels: &skaidb_tsdb::Labels,
    ) -> EngineResult<()> {
        let matchers: Vec<skaidb_tsdb::Matcher> = labels
            .iter()
            .map(|(k, v)| skaidb_tsdb::Matcher::Eq(k.clone(), v.clone()))
            .collect();
        let series = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .ts_query(table, &matchers, i64::MIN, i64::MAX)?;
        for (slabels, samples) in series {
            for chunk in samples.chunks(10_000) {
                let rows: Vec<(skaidb_tsdb::Labels, i64, f64)> = chunk
                    .iter()
                    .map(|s| (slabels.clone(), s.ts, s.value))
                    .collect();
                match self.pool.call(
                    addr,
                    &Request::TsMerge {
                        table: table.to_string(),
                        rows,
                    },
                ) {
                    Ok(Response::Ack) => {}
                    other => {
                        return Err(EngineError::Cluster(format!(
                            "ts repair: merge to {addr} not acked: {other:?}"
                        )))
                    }
                }
            }
        }
        Ok(())
    }

    /// Push one series' full sample history to `addr` in bounded TsAppend
    /// batches. Idempotent: replays reject per sample on the receiver.
    fn ts_push_series(
        &self,
        addr: &str,
        table: &str,
        labels: &skaidb_tsdb::Labels,
    ) -> EngineResult<()> {
        let matchers: Vec<skaidb_tsdb::Matcher> = labels
            .iter()
            .map(|(k, v)| skaidb_tsdb::Matcher::Eq(k.clone(), v.clone()))
            .collect();
        let series = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .ts_query(table, &matchers, i64::MIN, i64::MAX)?;
        for (slabels, samples) in series {
            for chunk in samples.chunks(10_000) {
                let rows: Vec<(skaidb_tsdb::Labels, i64, f64)> = chunk
                    .iter()
                    .map(|s| (slabels.clone(), s.ts, s.value))
                    .collect();
                match self.pool.call(
                    addr,
                    &Request::TsAppend {
                        table: table.to_string(),
                        rows,
                    },
                ) {
                    Ok(Response::Ack) => {}
                    other => {
                        return Err(EngineError::Cluster(format!(
                            "ts migration: append to {addr} not acked: {other:?}"
                        )))
                    }
                }
            }
        }
        Ok(())
    }

    /// Time-series share of a rebalance: push every local series the joiner
    /// now replicates, elected single-sender style (the series' primary
    /// under the pre-join ring pushes).
    fn ts_rebalance_to(&self, joiner: &NodeId, addr: &str, old_ring: &Ring) -> EngineResult<()> {
        let rf = self.cfg.replication_factor;
        let tables = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .ts_table_names();
        for table in tables {
            let all_series = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .ts_series_labels(&table)?;
            for labels in all_series {
                let key = ts_placement_key(&labels);
                if !self.replicas_for(&key).contains(joiner) {
                    continue; // joiner doesn't own this series under the new ring
                }
                let elected = old_ring.replicas_for(&key, rf).first().cloned();
                if elected.as_ref() != Some(&self.id) {
                    continue; // another member is this series' sender
                }
                self.ts_push_series(addr, &table, &labels)?;
            }
        }
        Ok(())
    }

    /// Time-series share of a drain: push every local series to any owner
    /// under the post-removal ring that isn't already a replica.
    fn ts_drain_to(
        &self,
        new_ring: &Ring,
        addr_of: &HashMap<NodeId, String>,
    ) -> EngineResult<()> {
        let rf = self.cfg.replication_factor;
        let tables = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .ts_table_names();
        for table in tables {
            let all_series = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .ts_series_labels(&table)?;
            for labels in all_series {
                let key = ts_placement_key(&labels);
                let old = self.replicas_for(&key);
                for replica in new_ring.replicas_for(&key, rf) {
                    if old.contains(&replica) {
                        continue;
                    }
                    let Some(addr) = addr_of.get(&replica) else { continue };
                    // Retry past a transient failure window (e.g. the peer's
                    // circuit breaker cooling down mid-drain), then log and
                    // continue: samples are immutable facts union-merged on
                    // read, so a missed series converges via ts repair —
                    // aborting the whole decommission over one push doesn't.
                    let mut pushed = false;
                    for wait_ms in [0u64, 2_000, 12_000] {
                        if wait_ms > 0 {
                            thread::sleep(Duration::from_millis(wait_ms));
                        }
                        if self.ts_push_series(addr, &table, &labels).is_ok() {
                            pushed = true;
                            break;
                        }
                    }
                    if !pushed {
                        skaidb_types::slog!(
                            "skaidb: ts drain: series in {table} not pushed to {} — \
                             ts repair will converge it",
                            replica.0
                        );
                    }
                }
            }
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
        let (wire_matchers, post_matchers) = split_wire_matchers(matchers);
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
                if !post_matchers.iter().all(|m| m.accepts(&labels)) {
                    continue; // peers only saw the equality forms
                }
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

    /// Partial-aggregate pushdown gather: every member computes per-series
    /// per-bucket partials locally and each `(series, bucket)` is answered by
    /// the responder that saw the most samples for it (replicas converge via
    /// quorum writes, handoff and repair, so the fullest view is the series'
    /// answer — raw-sample queries keep the union-merge). Requires the
    /// read-consistency number of responders.
    pub(crate) fn ts_partials_scatter(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
        t0: i64,
        t1: i64,
        bucket_ms: i64,
        oc: Option<Consistency>,
    ) -> EngineResult<Vec<(skaidb_tsdb::Labels, Vec<skaidb_engine::TsPartial>)>> {
        let mut merged: BTreeMap<skaidb_tsdb::Labels, BTreeMap<i64, skaidb_engine::TsPartial>> =
            BTreeMap::new();
        let mut absorb = |series: Vec<(skaidb_tsdb::Labels, Vec<skaidb_engine::TsPartial>)>| {
            for (labels, partials) in series {
                let entry = merged.entry(labels).or_default();
                for p in partials {
                    match entry.get(&p.bucket_ts) {
                        Some(have) if have.count >= p.count => {}
                        _ => {
                            entry.insert(p.bucket_ts, p);
                        }
                    }
                }
            }
        };
        let mut responders = 0usize;
        {
            let db = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
            absorb(db.ts_partials(table, matchers, t0, t1, bucket_ms)?);
            responders += 1;
        }
        let (wire_matchers, post_matchers) = split_wire_matchers(matchers);
        let addrs = self.peer_addrs();
        let shards = scatter(&addrs, |addr| {
            self.counters.peer_requests.fetch_add(1, Ordering::Relaxed);
            match self.pool.call(
                addr,
                &Request::TsPartials {
                    table: table.to_string(),
                    matchers: wire_matchers.clone(),
                    t0,
                    t1,
                    bucket: bucket_ms,
                },
            ) {
                Ok(Response::TsPartials { series }) => Some(series),
                _ => {
                    self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                    None
                }
            }
        });
        for shard in shards.into_iter().flatten() {
            absorb(
                shard
                    .into_iter()
                    .filter(|(labels, _)| post_matchers.iter().all(|m| m.accepts(labels)))
                    .map(|(labels, partials)| {
                        (labels, partials.iter().map(partial_from_wire).collect())
                    })
                    .collect(),
            );
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
            .map(|(labels, partials)| (labels, partials.into_values().collect()))
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
            // Resume handling: a table sorting before the checkpoint's table
            // is already done; within the checkpoint's table, the page cursor
            // starts after the last key sent.
            let mut cursor: Option<Vec<u8>> = match &resume {
                Some((ct, _)) if &table < ct => continue,
                Some((ct, lk)) if ct == &table => Some(lk.clone()),
                _ => None,
            };

            // Page through the shard (bounded memory regardless of table
            // size), filter each page to the joiner's keys, and stream those
            // in throttled batches — each chunk is one ApplyBatch RPC (one
            // round-trip + one fsync on the joiner, not one per row) —
            // checkpointing after each.
            loop {
                let page = self
                    .local
                    .read()
                    .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                    .local_scan_versioned_page(&table, cursor.as_deref(), MIGRATE_PAGE_ROWS)?;
                let done = page.len() < MIGRATE_PAGE_ROWS;
                cursor = page.last().map(|(k, _, _, _)| k.clone());
                let pending: Vec<BatchRow> = page
                    .into_iter()
                    .filter(|(key, _, _, _)| {
                        self.replicas_for(key).contains(joiner)
                            && old_ring.primary_for(key) == Some(self.id.clone())
                    })
                    .collect();
                for chunk in pending.chunks(batch) {
                    // One retry before erroring: transient slowness on the
                    // joiner shouldn't fail the join — and if it does, the
                    // checkpoint below makes a re-triggered join resume here.
                    if !self.send_batch(&addr, &table, chunk)
                        && !self.send_batch(&addr, &table, chunk)
                    {
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
                if done {
                    break;
                }
            }
        }
        // Time-series stores migrate too (series-level, idempotent — no
        // checkpoint needed; a retried join re-pushes harmlessly).
        self.ts_rebalance_to(joiner, &addr, &old_ring)?;

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
        // QoS: breathe between chunks (configured pause, floor 10ms) so the
        // receivers' cores aren't monopolized by back-to-back batch applies —
        // the drain is a background op; foreground queries are not.
        let pause = self.migration_pause_ms.load(Ordering::Relaxed).max(10);

        let tables = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .table_names();
        for table in tables {
            // Page through the shard so drain memory is one page + the
            // per-destination groups of that page, independent of shard size.
            let mut cursor: Option<Vec<u8>> = None;
            loop {
                let page = self
                    .local
                    .read()
                    .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                    .local_scan_versioned_page(&table, cursor.as_deref(), MIGRATE_PAGE_ROWS)?;
                let done = page.len() < MIGRATE_PAGE_ROWS;
                cursor = page.last().map(|(k, _, _, _)| k.clone());
                // Group the page's rows by their *new* owner so each
                // destination receives chunked ApplyBatch RPCs (one
                // round-trip + one fsync per chunk) instead of one per row.
                let mut per_dest: BTreeMap<NodeId, Vec<BatchRow>> = BTreeMap::new();
                for (key, value, hlc, is_put) in page {
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
                        // Retry once, then degrade the chunk to hints and keep
                        // draining: a receiver too slow to ack inside the I/O
                        // timeout (say, FTS-indexing a large chunk) used to
                        // abort the whole decommission here. Hinted handoff +
                        // anti-entropy repair backstop missed writes on every
                        // other path — the drain is no different, and the
                        // remaining replicas still hold every drained key.
                        let sent = self.send_batch(addr, &table, chunk)
                            || self.send_batch(addr, &table, chunk);
                        if !sent {
                            skaidb_types::slog!(
                                "skaidb: drain: {} rows for {} not acked — hinted for replay/repair",
                                chunk.len(),
                                replica.0
                            );
                            for (k, v, hlc, is_put) in chunk {
                                let op = if *is_put {
                                    WriteOp::Put(v.clone())
                                } else {
                                    WriteOp::Delete
                                };
                                self.store_hint(&replica, &table, k, &op, *hlc);
                            }
                        }
                        // Per-chunk breather on every path, so back-to-back
                        // applies never monopolize the receivers' cores.
                        thread::sleep(Duration::from_millis(pause));
                    }
                }
                if done {
                    break;
                }
            }
        }
        // Time-series stores drain the same way, series-level.
        self.ts_drain_to(&new_ring, &addr_of)?;
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
        let _guard = self.membership_lock.lock().expect("membership lock");
        let joiner = NodeId::new(id);
        let old_members = self.members_snapshot();
        if old_members.iter().any(|(mid, _)| *mid == joiner) {
            // Already in the ring. If a previous join attempt died between
            // its begin and finalize broadcasts (e.g. the joiner bootstrap
            // failed), the dual-placement window is still open on every
            // member and nothing would ever close it — a re-announce landed
            // here and returned Ok without healing. Finalize it now.
            let pending = self.topo.read().expect("topo lock").prev.is_some();
            if pending {
                let wire = wire_of(&old_members);
                let epoch_final = self.current_epoch() + 1;
                self.set_membership(&old_members, &[], epoch_final);
                for (mid, maddr) in &old_members {
                    if *mid == self.id {
                        continue;
                    }
                    let _ = self.pool.call(
                        maddr,
                        &Request::SetMembership {
                            epoch: epoch_final,
                            members: wire.clone(),
                            prev_members: Vec::new(),
                        },
                    );
                }
            }
            return Ok(());
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
            // Draining a whole keyspace takes minutes on a large/slow node —
            // one synchronous RPC under the ordinary I/O timeout read as
            // "unreachable" long before the drain finished. Rare admin op:
            // give it a deadline sized to the work.
            const DRAIN_TIMEOUT: Duration = Duration::from_secs(3600);
            match self
                .pool
                .call_timeout(&addr, &Request::Drain { members: wire.clone() }, DRAIN_TIMEOUT)
            {
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
            // Collect unowned keys whose owners confirm a copy at >= our
            // version, paging the scan so only keys under consideration (not
            // whole rows) accumulate.
            let mut confirmed: HashSet<Vec<u8>> = HashSet::new();
            let mut cursor: Option<Vec<u8>> = None;
            loop {
                let page = self
                    .local
                    .read()
                    .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                    .local_scan_versioned_page(&table, cursor.as_deref(), MIGRATE_PAGE_ROWS)?;
                let done = page.len() < MIGRATE_PAGE_ROWS;
                cursor = page.last().map(|(k, _, _, _)| k.clone());
                for (key, _value, hlc, _is_put) in page {
                    let owners = self.replicas_for(&key);
                    if owners.contains(&self.id) {
                        continue; // we still own it
                    }
                    if self.owners_hold(&table, &key, hlc, &owners) {
                        confirmed.insert(key);
                    }
                }
                if done {
                    break;
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
        // Time-series tables reclaim by whole series (their migration unit).
        total += self.ts_reclaim()?;
        Ok(total)
    }

    /// Time-series reclaim: drop whole series this node no longer owns,
    /// but only once a current owner provably holds an **identical** copy
    /// (same sample count and checksum). A diverged owner gets our copy
    /// pushed instead — the next reclaim pass (post-convergence) drops it.
    /// Rollup tables reclaim by the same rule: a rollup series carries its
    /// source's labels (placement ignores `__field__`), so it is owned by
    /// the same replica set.
    fn ts_reclaim(&self) -> EngineResult<usize> {
        let tables = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .ts_table_names();
        let mut total = 0usize;
        for table in tables {
            let mine: Vec<(skaidb_tsdb::Labels, u64, u64)> = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .ts_summaries(&table)?;
            // Unowned series, grouped so each owner is asked once per table.
            let unowned: Vec<&(skaidb_tsdb::Labels, u64, u64)> = mine
                .iter()
                .filter(|(labels, _, _)| {
                    !self.replicas_for(&ts_placement_key(labels)).contains(&self.id)
                })
                .collect();
            if unowned.is_empty() {
                continue;
            }
            type OwnerSummary = Option<HashMap<skaidb_tsdb::Labels, (u64, u64)>>;
            let mut owner_summaries: HashMap<NodeId, OwnerSummary> = HashMap::new();
            let mut confirmed: std::collections::HashSet<skaidb_tsdb::Labels> =
                std::collections::HashSet::new();
            for (labels, count, checksum) in unowned {
                let owners = self.replicas_for(&ts_placement_key(labels));
                let mut held = false;
                for owner in &owners {
                    let summary = owner_summaries.entry(owner.clone()).or_insert_with(|| {
                        let addr = self.peer_addr(owner)?;
                        match self
                            .pool
                            .call(&addr, &Request::TsSummary { table: table.clone() })
                        {
                            Ok(Response::TsSummaries { series }) => Some(
                                series
                                    .into_iter()
                                    .map(|(l, c, x)| (l, (c, x)))
                                    .collect(),
                            ),
                            _ => None,
                        }
                    });
                    if let Some(theirs) = summary {
                        if theirs.get(labels) == Some(&(*count, *checksum)) {
                            held = true;
                            break;
                        }
                    }
                }
                if held {
                    confirmed.insert(labels.clone());
                } else if let Some(owner) = owners.first() {
                    // Converge first, reclaim on a later pass.
                    if let Some(addr) = self.peer_addr(owner) {
                        let _ = self.ts_push_series_merge(&addr, &table, labels);
                    }
                }
            }
            if !confirmed.is_empty() {
                total += self
                    .local
                    .write()
                    .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                    .ts_drop_series(&table, &confirmed)?;
            }
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
        self.repairing.store(true, Ordering::Relaxed);
        let result = self.repair_inner();
        self.repairing.store(false, Ordering::Relaxed);
        result
    }

    fn repair_inner(&self) -> EngineResult<usize> {
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
                // QoS: repair is a background op. Back-to-back (table, peer)
                // reconciliations kept 1-2 cores busy for the whole pass
                // (~100-120s measured) and degraded write quorum on the
                // passing node — foreground traffic saw timeouts every hour,
                // per node. Breathe between pairs; a slower pass is free,
                // a degraded quorum is not.
                thread::sleep(REPAIR_PAIR_PAUSE);
            }
        }
        repaired += self.ts_repair()?;
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
                // QoS: local fills decompress whole pages (the CPU-heavy
                // part of a pass, measured as the multi-core brotli burn) —
                // yield between pages so queries interleave.
                thread::sleep(REPAIR_PAGE_PAUSE);
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

    /// The engine read lock with a bounded wait ([`SCAN_LOCK_WAIT`]); `None`
    /// when it stayed write-locked (or poisoned) — callers answer "busy"
    /// instead of parking the connection thread indefinitely.
    fn local_read_bounded(&self) -> Option<std::sync::RwLockReadGuard<'_, Database>> {
        let deadline = Instant::now() + SCAN_LOCK_WAIT;
        loop {
            match self.local.try_read() {
                Ok(guard) => return Some(guard),
                Err(std::sync::TryLockError::Poisoned(_)) => return None,
                Err(std::sync::TryLockError::WouldBlock) => {
                    if Instant::now() >= deadline {
                        return None;
                    }
                    thread::sleep(Duration::from_millis(25));
                }
            }
        }
    }

    /// Write-lock twin of [`Node::local_read_bounded`], for inbound handlers
    /// that need the exclusive lock (search read-your-writes commit, applied
    /// DDL). Same rationale: an abandoned connection thread parked on the
    /// lock is a leak; the callers' failure paths (search scatter fallback,
    /// DDL schema-sync backstop) already tolerate a busy reply.
    fn local_write_bounded(&self) -> Option<std::sync::RwLockWriteGuard<'_, Database>> {
        let deadline = Instant::now() + SCAN_LOCK_WAIT;
        loop {
            match self.local.try_write() {
                Ok(guard) => return Some(guard),
                Err(std::sync::TryLockError::Poisoned(_)) => return None,
                Err(std::sync::TryLockError::WouldBlock) => {
                    if Instant::now() >= deadline {
                        return None;
                    }
                    thread::sleep(Duration::from_millis(25));
                }
            }
        }
    }

    /// Graceful-shutdown hook: flush memtables and commit search writers so
    /// the next start replays (almost) nothing — an unclean kill used to cost
    /// a full search-index rebuild (~15 min) because uncommitted writer state
    /// forced replay from a stale watermark. Bounded lock wait: a wedged
    /// engine must not stall exit past systemd's kill window.
    pub fn prepare_shutdown(&self) {
        for _ in 0..40 {
            match self.local.try_write() {
                Ok(mut db) => {
                    let _ = db.release_memory_under_pressure(true);
                    return;
                }
                Err(std::sync::TryLockError::Poisoned(_)) => return,
                Err(std::sync::TryLockError::WouldBlock) => {
                    thread::sleep(Duration::from_millis(250));
                }
            }
        }
        skaidb_types::slog!("skaidb: shutdown flush skipped — engine lock busy for 10s");
    }

    /// Buffer a write that couldn't reach `replica` (for hinted handoff).
    fn store_hint(&self, replica: &NodeId, table: &str, key: &[u8], op: &WriteOp, hlc: Hlc) {
        {
            let mut hints = self.hints.lock().expect("hints lock");
            let bucket = hints.entry(replica.clone()).or_default();
            if bucket.len() < MAX_HINTS_PER_REPLICA {
                bucket.push((table.to_string(), key.to_vec(), op.clone(), hlc));
                self.counters.hints_stored.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        // In-memory bucket full: spill to the replica's on-disk hint log so a
        // persistently-behind replica loses no writes (the old behavior
        // silently dropped past the cap, leaving a full repair pass as the
        // only recovery). Bounded memory, durable across restarts.
        if self.spill_hint_to_disk(replica, table, key, op, hlc) {
            self.disk_hints.fetch_add(1, Ordering::Relaxed);
            self.counters.hints_stored.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Directory holding the per-replica on-disk hint logs.
    fn hint_dir(&self) -> Option<std::path::PathBuf> {
        let dir = self.data_dir()?.join("hints");
        std::fs::create_dir_all(&dir).ok()?;
        Some(dir)
    }

    /// A replica's hint-log path (id sanitized to a safe filename).
    fn hint_log_path(&self, replica: &NodeId) -> Option<std::path::PathBuf> {
        let safe: String = replica
            .0
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        Some(self.hint_dir()?.join(format!("{safe}.hintlog")))
    }

    /// Append one hint record to the replica's on-disk log. Serialized by
    /// `hint_spill` so concurrent writers don't interleave records.
    fn spill_hint_to_disk(
        &self,
        replica: &NodeId,
        table: &str,
        key: &[u8],
        op: &WriteOp,
        hlc: Hlc,
    ) -> bool {
        use std::io::Write;
        let Some(path) = self.hint_log_path(replica) else {
            return false;
        };
        let rec = encode_hint_record(table, key, op, hlc);
        let _guard = self.hint_spill.lock().expect("hint spill lock");
        match std::fs::OpenOptions::new().create(true).append(true).open(&path) {
            Ok(mut f) => f.write_all(&rec).is_ok(),
            Err(_) => false,
        }
    }

    /// Whether any disk-spilled hint log exists (cheap readdir). The
    /// `disk_hints` counter is process-local, so a restarted node must still
    /// discover and drain logs it inherited from the previous process.
    fn has_disk_hints(&self) -> bool {
        let Some(dir) = self.hint_dir() else {
            return false;
        };
        std::fs::read_dir(dir).ok().is_some_and(|entries| {
            entries
                .flatten()
                .any(|e| e.path().extension().is_some_and(|x| x == "hintlog"))
        })
    }

    /// Replay the disk-spilled hints for `replica` to `addr`, **streamed in
    /// pages**. Returns how many records were delivered.
    ///
    /// The previous implementation read and decoded the whole log into memory
    /// and, when the peer was still down, re-encoded and rewrote every record
    /// — after every write batch. A 147 MB log ballooned a restarting node to
    /// its cgroup limit and turned each flush cycle into a full-log rewrite.
    /// Now: probe the peer first (down ⇒ leave the log untouched), then
    /// rename-claim the log and decode/deliver it a bounded page at a time; if
    /// the peer fails mid-drain, the undelivered remainder is appended back as
    /// raw bytes (no decode, no per-record rewrite).
    fn drain_disk_hints(&self, replica: &NodeId, addr: &str) -> usize {
        use std::io::{Read, Write};
        let Some(path) = self.hint_log_path(replica) else {
            return 0;
        };
        if !path.exists() {
            return 0;
        }
        // Reachability gate: draining at a dead peer is pure churn.
        if !matches!(
            self.pool.call_timeout(addr, &Request::NodeStatus, PROBE_TIMEOUT),
            Ok(Response::NodeStatus { .. })
        ) {
            return 0;
        }
        // Claim the log by renaming; concurrent spills append to a fresh file.
        let draining = path.with_extension("hintlog.draining");
        {
            let _guard = self.hint_spill.lock().expect("hint spill lock");
            if std::fs::rename(&path, &draining).is_err() {
                return 0;
            }
        }
        let Ok(f) = std::fs::File::open(&draining) else {
            let _ = std::fs::remove_file(&draining);
            return 0;
        };
        let mut reader = std::io::BufReader::new(f);
        let mut buf: Vec<u8> = Vec::new();
        let mut pos = 0usize;
        let mut eof = false;
        let mut delivered = 0usize;
        let mut aborted = false;
        let mut max_hlc: Option<Hlc> = None;
        const PAGE_RECORDS: usize = 1024;
        const READ_CHUNK: u64 = 1 << 20;
        loop {
            // Decode one page, refilling the carry buffer from disk as needed.
            let mut page: Vec<(String, Vec<u8>, WriteOp, Hlc)> = Vec::new();
            while page.len() < PAGE_RECORDS {
                if let Some(rec) = decode_hint_record(&buf, &mut pos) {
                    page.push(rec);
                    continue;
                }
                if eof {
                    break; // truncated tail (torn write) — repair backstops it
                }
                buf.drain(..pos);
                pos = 0;
                let n = (&mut reader)
                    .take(READ_CHUNK)
                    .read_to_end(&mut buf)
                    .unwrap_or(0);
                if n == 0 {
                    eof = true;
                }
            }
            if page.is_empty() {
                break;
            }
            // Group the page by table so each group replays as one ApplyBatch.
            let mut by_table: BTreeMap<String, Vec<BatchRow>> = BTreeMap::new();
            for (table, key, op, hlc) in page {
                let row: BatchRow = match op {
                    WriteOp::Put(v) => (key, v, hlc, true),
                    WriteOp::Delete => (key, Vec::new(), hlc, false),
                };
                by_table.entry(table).or_default().push(row);
            }
            for (table, rows) in by_table {
                if aborted {
                    // Peer failed earlier in this page: re-spill without retrying.
                    for (k, v, h, is_put) in rows {
                        let op = if is_put { WriteOp::Put(v) } else { WriteOp::Delete };
                        self.spill_hint_to_disk(replica, &table, &k, &op, h);
                    }
                    continue;
                }
                let batch_max = rows.iter().map(|(_, _, h, _)| *h).max();
                if self.send_batch(addr, &table, &rows) {
                    delivered += rows.len();
                    if let Some(h) = batch_max {
                        max_hlc = Some(max_hlc.map_or(h, |m| m.max(h)));
                    }
                } else {
                    aborted = true;
                    for (k, v, h, is_put) in rows {
                        let op = if is_put { WriteOp::Put(v) } else { WriteOp::Delete };
                        self.spill_hint_to_disk(replica, &table, &k, &op, h);
                    }
                }
            }
            if aborted || (eof && pos >= buf.len()) {
                break;
            }
        }
        if let Some(h) = max_hlc {
            self.note_acked(replica, h);
        }
        // Peer went away mid-drain: append the undecoded remainder back to the
        // live log as raw bytes — no decode, no per-record rewrite.
        if aborted {
            let _guard = self.hint_spill.lock().expect("hint spill lock");
            if let Ok(mut out) = std::fs::OpenOptions::new().create(true).append(true).open(&path)
            {
                let _ = out.write_all(&buf[pos..]);
                let _ = std::io::copy(&mut reader, &mut out);
            }
        }
        let _ = std::fs::remove_file(&draining);
        // Saturating: the counter is approximate (process-local; resets on
        // restart while logs persist).
        let _ = self
            .disk_hints
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(delivered as u64))
            });
        delivered
    }

    /// Buffer a time-series batch that couldn't reach `replica`. Bounded by
    /// total buffered samples per peer; overflow is dropped (anti-entropy
    /// repair remains the durable backstop).
    fn store_ts_hint(
        &self,
        replica: &NodeId,
        table: &str,
        rows: Vec<(skaidb_tsdb::Labels, i64, f64)>,
    ) {
        const MAX_TS_HINT_SAMPLES_PER_REPLICA: usize = 100_000;
        let n = rows.len();
        let mut hints = self.ts_hints.lock().expect("ts hints lock");
        let bucket = hints.entry(replica.clone()).or_default();
        let buffered: usize = bucket.iter().map(|(_, r)| r.len()).sum();
        if buffered + n <= MAX_TS_HINT_SAMPLES_PER_REPLICA {
            bucket.push((table.to_string(), rows));
            self.counters.hints_stored.fetch_add(n as u64, Ordering::Relaxed);
        }
    }

    /// Whether any hints are buffered (cheap check before spawning a flush).
    fn hints_pending(&self) -> bool {
        !self.hints.lock().expect("hints lock").is_empty()
            || !self.ts_hints.lock().expect("ts hints lock").is_empty()
            || self.disk_hints.load(Ordering::Relaxed) > 0
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
        // Time-series hints replay via TsMerge, which accepts samples of any
        // age (the receiver may have moved past them while it was down).
        let ts_pending: Vec<(NodeId, Vec<TsHint>)> = {
            let mut hints = self.ts_hints.lock().expect("ts hints lock");
            hints.drain().collect()
        };
        for (replica, batches) in ts_pending {
            let Some(addr) = self.peer_addr(&replica) else {
                continue;
            };
            let mut remaining = Vec::new();
            for (table, rows) in batches {
                match self.pool.call(
                    &addr,
                    &Request::TsMerge {
                        table: table.clone(),
                        rows: rows.clone(),
                    },
                ) {
                    Ok(Response::Ack) => delivered += rows.len(),
                    _ => remaining.push((table, rows)),
                }
            }
            if !remaining.is_empty() {
                self.ts_hints
                    .lock()
                    .expect("ts hints lock")
                    .entry(replica)
                    .or_default()
                    .extend(remaining);
            }
        }
        // Drain any disk-spilled hints for currently-reachable peers (a
        // replica that stayed full has hints only on disk, not in `pending`).
        // Checked via the directory, not just the in-memory counter: the
        // counter is process-local, so a restarted node must still drain the
        // logs it inherited. drain_disk_hints itself probes each peer and
        // leaves the log untouched while the peer is down.
        if self.disk_hints.load(Ordering::Relaxed) > 0 || self.has_disk_hints() {
            for (replica, addr) in self.members_snapshot() {
                if replica == self.id {
                    continue;
                }
                delivered += self.drain_disk_hints(&replica, &addr);
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
            // Defer while any peer is mid-pass: two concurrent passes (even
            // paced ones) dent write quorum — the hourly stagger only offsets
            // start times, and minutes-long passes still overlapped. Bounded
            // deferral (10 x 90s) so mutual deference can't live-lock.
            for _ in 0..10 {
                let peer_repairing = self.peers_with_ids().into_iter().any(|(_, addr)| {
                    matches!(
                        self.pool.call_timeout(&addr, &Request::HostStats, PROBE_TIMEOUT),
                        Ok(Response::HostStats { json })
                            if serde_json::from_str::<crate::host::HostStats>(&json)
                                .map(|h| h.repairing)
                                .unwrap_or(false)
                    )
                });
                if !peer_repairing {
                    break;
                }
                skaidb_types::slog!("skaidb: anti-entropy deferred — a peer is mid-pass");
                thread::sleep(Duration::from_secs(90));
            }
            // Start line: a pass that hangs (or crawls) is otherwise
            // invisible until it ends — the multi-hour silent burns of
            // 2026-07-11 were only attributable after this class of logging.
            skaidb_types::slog!("skaidb: anti-entropy pass starting");
            let started = Instant::now();
            let result = self.repair();
            let secs = started.elapsed().as_secs();
            // A pass streams every table page by page — a large transient
            // allocation churn, and the prime suspect for footprint ratchet
            // between passes. Log the allocator's live/resident/retained split
            // after any pass big enough to matter so growth is attributable.
            let alloc = || {
                crate::memguard::alloc_stats().map_or(String::new(), |s| format!(" [{s}]"))
            };
            match result {
                Ok(n) if n > 0 => {
                    skaidb_types::slog!(
                        "skaidb: anti-entropy reconciled {n} rows in {secs}s{}",
                        alloc()
                    );
                }
                // Converged: stay quiet unless the no-op pass itself is slow
                // enough to matter (it competes with foreground queries).
                Ok(_) if secs >= 60 => {
                    skaidb_types::slog!(
                        "skaidb: anti-entropy pass converged (took {secs}s){}",
                        alloc()
                    );
                }
                Ok(_) => {}
                Err(e) => skaidb_types::slog!("skaidb: anti-entropy pass failed after {secs}s: {e}"),
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
            Request::HostStats => {
                let dir = match self.local_read_bounded() {
                    Some(db) => db.dir().to_path_buf(),
                    None => return Response::Err("busy: engine write-locked, retry".into()),
                };
                let mut stats = crate::host::sample(&dir);
                stats.repairing = self.repairing.load(Ordering::Relaxed);
                match serde_json::to_string(&stats) {
                    Ok(json) => Response::HostStats { json },
                    Err(e) => Response::Err(format!("encode host stats: {e}")),
                }
            }
            Request::LocalScan { table } => match self.local_read_bounded() {
                Some(db) => match db.local_scan_versioned_with_tombstones(&table) {
                    Ok(rows) => Response::Scan { rows },
                    Err(e) => Response::Err(e.to_string()),
                },
                None => Response::Err("busy: engine write-locked, retry".into()),
            },
            Request::ScanPage { table, after, limit } => match self.local_read_bounded() {
                Some(db) => {
                    match db.local_scan_versioned_page(&table, after.as_deref(), limit as usize) {
                        Ok(rows) => Response::Scan { rows },
                        Err(e) => Response::Err(e.to_string()),
                    }
                }
                None => Response::Err("busy: engine write-locked, retry".into()),
            },
            Request::TsAppend { table, rows } => match self.local_read_bounded() {
                Some(db) => match db.ts_append(&table, &rows) {
                    Ok(_) => Response::Ack,
                    Err(e) => Response::Err(e.to_string()),
                },
                None => Response::Err("busy: engine write-locked, retry".into()),
            },
            Request::TsQuery {
                table,
                matchers,
                t0,
                t1,
            } => match self.local_read_bounded() {
                Some(db) => {
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
                None => Response::Err("busy: engine write-locked, retry".into()),
            },
            Request::TsMerge { table, rows } => match self.local_read_bounded() {
                Some(db) => match db.ts_merge(&table, &rows) {
                    Ok(_) => Response::Ack,
                    Err(e) => Response::Err(e.to_string()),
                },
                None => Response::Err("busy: engine write-locked, retry".into()),
            },
            Request::TsSummary { table } => match self.local_read_bounded() {
                Some(db) => match db.ts_summaries(&table) {
                    Ok(series) => Response::TsSummaries { series },
                    Err(e) => Response::Err(e.to_string()),
                },
                None => Response::Err("busy: engine write-locked, retry".into()),
            },
            Request::TsPartials {
                table,
                matchers,
                t0,
                t1,
                bucket,
            } => match self.local_read_bounded() {
                Some(db) => {
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
                    match db.ts_partials(&table, &matchers, t0, t1, bucket) {
                        Ok(series) => Response::TsPartials {
                            series: series
                                .into_iter()
                                .map(|(labels, partials)| {
                                    (labels, partials.iter().map(partial_to_wire).collect())
                                })
                                .collect(),
                        },
                        Err(e) => Response::Err(e.to_string()),
                    }
                }
                None => Response::Err("busy: engine write-locked, retry".into()),
            },
            Request::LocalGet { table, key } => match self.local_read_bounded() {
                Some(db) => match db.local_get_versioned(&table, &key) {
                    Ok(entry) => Response::Get { entry },
                    Err(e) => Response::Err(e.to_string()),
                },
                None => Response::Err("busy: engine write-locked, retry".into()),
            },
            Request::FilteredScan { table, filter } => match self.local_read_bounded() {
                Some(db) => match db.local_scan_filtered_keys(&table, &Some(filter)) {
                    Ok(keys) => Response::Keys { keys },
                    Err(e) => Response::Err(e.to_string()),
                },
                None => Response::Err("busy: engine write-locked, retry".into()),
            },
            Request::IndexScan { index, start, end } => match self.local_read_bounded() {
                Some(db) => match db.index_scan_keys(&index, start.as_deref(), end.as_deref()) {
                    Ok(keys) => Response::Keys { keys },
                    Err(e) => Response::Err(e.to_string()),
                },
                None => Response::Err("busy: engine write-locked, retry".into()),
            },
            Request::VectorSearch { index, query, k } => match self.local_read_bounded() {
                Some(db) => match db.vector_search_local(&index, &query, k as usize) {
                    Ok(hits) => Response::VectorHits { hits },
                    Err(e) => Response::Err(e.to_string()),
                },
                None => Response::Err("busy: engine write-locked, retry".into()),
            },
            // Serve under the write lock so pending index writes commit
            // first: replicated writes reach replicas synchronously at the
            // write consistency, so committing here makes every acked write
            // searchable cluster-wide (read-your-writes, not just NRT). A
            // clean index is a no-op.
            Request::Search { table, query, k } => match self.local_write_bounded() {
                Some(mut db) => match serde_json::from_str::<skaidb_fts::SearchQuery>(&query) {
                    Ok(parsed) => {
                        let k = (k > 0).then_some(k as usize);
                        match db.search_local_commit_if_dirty(&table, &parsed, k) {
                            Ok(hits) => Response::SearchHits { hits },
                            Err(e) => Response::Err(e.to_string()),
                        }
                    }
                    Err(e) => Response::Err(format!("bad search query: {e}")),
                },
                None => Response::Err("busy: engine write-locked, retry".into()),
            },
            Request::SearchSorted {
                table,
                query,
                sort,
                k,
                highlights,
                epoch,
            } => {
                let parsed = (
                    serde_json::from_str::<skaidb_fts::SearchQuery>(&query),
                    serde_json::from_str::<skaidb_fts::SortSpec>(&sort),
                );
                match parsed {
                    (Ok(query), Ok(sort)) => {
                        let highlights: Vec<(String, usize)> = highlights
                            .into_iter()
                            .map(|(c, m)| (c, m as usize))
                            .collect();
                        match self.search_sorted_shard(
                            &table,
                            &query,
                            &sort,
                            k as usize,
                            &highlights,
                            epoch,
                        ) {
                            Ok(rows) => Response::SortedRows {
                                rows: rows.map(|r| crate::internode::encode_sorted_rows(&r)),
                            },
                            Err(e) => Response::Err(e.to_string()),
                        }
                    }
                    _ => Response::Err("bad search-sorted request".into()),
                }
            }
            Request::SearchExplain { table, query, pk } => {
                let parsed = (
                    serde_json::from_str::<skaidb_fts::SearchQuery>(&query),
                    skaidb_types::Value::decode(&pk),
                );
                match parsed {
                    (Ok(query), Ok(pk_value)) => match self.local_write_bounded() {
                        Some(mut db) => {
                            match db.search_explain_query(&table, &query, &pk_value) {
                                Ok(text) => Response::Explanation { text },
                                Err(e) => Response::Err(e.to_string()),
                            }
                        }
                        None => Response::Err("busy: engine write-locked, retry".into()),
                    },
                    _ => Response::Err("bad search-explain request".into()),
                }
            }
            Request::SearchAgg {
                table,
                query,
                agg,
                epoch,
            } => {
                let parsed = (
                    serde_json::from_str::<skaidb_fts::SearchQuery>(&query),
                    serde_json::from_str::<skaidb_fts::AggRequest>(&agg),
                );
                match parsed {
                    (Ok(query), Ok(agg)) => {
                        match self.search_agg_shard(&table, &query, &agg, epoch) {
                            Ok(rows) => Response::SearchAggRows {
                                rows: rows
                                    .map(|r| crate::internode::encode_agg_rows(&r)),
                            },
                            Err(e) => Response::Err(e.to_string()),
                        }
                    }
                    _ => Response::Err("bad search-agg request".into()),
                }
            }
            // Inbound replica writes are shed under memory pressure too, so a
            // pressured node stops accepting the coordinator's replication
            // (which then hints it for handoff once it recovers) — not just
            // its own client writes. Reads and DDL are never shed.
            Request::ApplyPut { .. } | Request::ApplyDelete { .. } | Request::ApplyBatch { .. }
                if self.mem.shedding() =>
            {
                Response::Err(shed_error().to_string())
            }
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
                // QoS: bounded concurrency for bulk appliers — queries keep
                // cores even under a drain/rebalance/repair flood. A bounded
                // admission wait, then "busy": the sender's failure path
                // (hint + retry via handoff/anti-entropy) is durable, and an
                // unbounded queue melts a rejoining node (see BulkGate).
                match self.bulk_gate.acquire(BULK_ADMISSION_WAIT) {
                    Some(_permit) => write_response(self.apply_batch_local(&table, &rows)),
                    None => Response::Err("busy: bulk appliers saturated, retry".into()),
                }
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
        match self.local_write_bounded() {
            Some(mut db) => match f(&mut db) {
                Ok(()) => Response::Ack,
                Err(e) => Response::Err(e.to_string()),
            },
            None => Response::Err("busy: engine write-locked, retry".into()),
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
        // schema and database registry stay identical cluster-wide. User
        // statements carrying a plaintext password are rewritten to the
        // encoded-verifier form first, so plaintext never crosses internode
        // links (which may be token-authenticated but unencrypted).
        if is_ddl(&stmt) {
            let sql_owned;
            let sql = match &stmt {
                Statement::CreateUser(cu) if cu.password.is_some() => {
                    sql_owned = render_user_verifier_ddl(&cu.name, cu.password.as_deref().unwrap());
                    &sql_owned
                }
                Statement::AlterUser { name, password } => {
                    sql_owned = render_user_verifier_ddl(name, password);
                    &sql_owned
                }
                _ => sql,
            };
            self.broadcast_ddl(current_db, sql)?;
            return Ok(SessionEffect::Output(QueryOutput::Ddl));
        }
        // BACKUP copies THIS node's shard (each node backs up its own
        // data); RESTORE into a live ring is an operator action, not a
        // statement — swapping one node's data underneath quorum reads
        // would silently diverge replicas.
        if let Statement::Backup { path } = &stmt {
            return self
                .local
                .write()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .backup_to(path)
                .map(|rs| SessionEffect::Output(QueryOutput::Rows(rs)));
        }
        if let Statement::Restore { .. } = &stmt {
            return Err(EngineError::Unsupported(
                "RESTORE is not available on a cluster — stop the node and restore its \
                 data directory offline, then let repair converge it"
                    .into(),
            ));
        }
        // Per-row score explain routes to a replica of the key (any RF);
        // the row and its index entry may live only on other nodes.
        if let Statement::ExplainScore { .. } = &stmt {
            let mut stmt = stmt;
            namespace::resolve_statement(&mut stmt, current_db);
            let Statement::ExplainScore { select, key } = stmt else {
                unreachable!()
            };
            let text = self.search_explain(&select.from, &select.filter, &key)?;
            return Ok(SessionEffect::Output(QueryOutput::Rows(
                skaidb_engine::ResultSet {
                    columns: vec!["explanation".into()],
                    rows: text
                        .map(|t| vec![vec![skaidb_types::Value::String(t)]])
                        .unwrap_or_default(),
                },
            )));
        }
        // Plan inspection: the engine's plan describer answers from the local
        // catalog (identical on every node), then cluster fan-out rows are
        // appended so the answer covers routing too.
        if let Statement::Explain { .. } = &stmt {
            let mut stmt = stmt;
            namespace::resolve_statement(&mut stmt, current_db);
            let Statement::Explain { statement } = stmt else {
                unreachable!()
            };
            let db = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
            let mut rs = db.explain_statement(&statement)?;
            let members = self.member_count();
            let rf = self.cfg.replication_factor;
            let mut push = |aspect: &str, decision: String| {
                rs.rows
                    .push(vec![Value::String(aspect.into()), Value::String(decision)]);
            };
            push("cluster.members", members.to_string());
            push("cluster.replication_factor", rf.to_string());
            let fan_out = match &*statement {
                Statement::Select(sel) => {
                    let pk = db.table_primary_key(&sel.from).unwrap_or_default();
                    if pk_point_key(&pk, &sel.filter).is_some() {
                        "point read routed to the key's replica set".to_string()
                    } else if rf >= members {
                        "every member holds all data — served without fan-out".to_string()
                    } else {
                        "scatter to all members, gather + LWW-merge at the coordinator"
                            .to_string()
                    }
                }
                Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
                    "each key routes to its replica set, acked at the write consistency"
                        .to_string()
                }
                _ => "executes per its statement type (local or broadcast)".to_string(),
            };
            push("cluster.fan_out", fan_out);
            drop(db);
            return Ok(SessionEffect::Output(QueryOutput::Rows(rs)));
        }
        // Read-only catalog/stat introspection: the catalog is identical on every
        // node (DDL is broadcast), so answer from the local engine, filtered to
        // the current database, without fan-out — under a shared lock, so it
        // never queues behind (or blocks) writers. SUGGEST reads the local
        // shard's term dictionary the same way (complete when RF >= members;
        // per-shard otherwise).
        if matches!(
            stmt,
            Statement::ShowTables
                | Statement::ShowIndexes
                | Statement::ShowStatus
                | Statement::ShowDatabases
                | Statement::ShowGrants { .. }
                | Statement::Suggest { .. }
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
        if self.mem.shedding() {
            return Err(shed_error());
        }
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
        if self.mem.shedding() {
            return Err(shed_error());
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
        // QoS: bounded lock tenure. One write-lock acquisition for a whole
        // 250-row FTS-indexed batch, arriving back-to-back on every replica
        // during a bulk load, kept the engine write-locked essentially
        // continuously — every query starved behind it (health answered,
        // queries timed out). Apply in sub-chunks, one lock acquisition each
        // with a breather between, so queued readers interleave. The WAL
        // commit point is monotonic, so syncing through the last chunk's
        // commit still covers the whole batch — one fsync, same durability
        // at the ack as before.
        const APPLY_CHUNK_ROWS: usize = 64;
        let mut last: Option<(WalCommit, Arc<WalSync>)> = None;
        for chunk in rows.chunks(APPLY_CHUNK_ROWS) {
            // Idempotent redelivery: drop rows already held at an equal-or-
            // newer stamp BEFORE paying the write + search-index cost. A
            // catch-up sender whose ack times out re-spills and redelivers
            // pages the replica in fact applied — observed as a rejoining
            // node re-indexing the same rows at 4 cores for hours while the
            // sender's hint log never shrank, plus duplicate LSM versions
            // bloating its tables. A bloom-gated point read per row is cheap
            // next to indexing; blind LWW appends are not idempotent-cheap.
            let fresh = self.filter_newer_rows(table, chunk)?;
            let applied = match &fresh {
                RowFilter::AllFresh => self.apply_batch_buffered(table, chunk)?,
                RowFilter::Some(rows) => self.apply_batch_buffered(table, rows)?,
                RowFilter::AllStale => None,
            };
            if let Some(c) = applied {
                last = Some(c);
            }
            thread::sleep(Duration::from_millis(1));
        }
        if let Some((commit, handle)) = last {
            handle.sync_through(commit)?;
        }
        Ok(())
    }

    /// Partition a replica-apply chunk into rows strictly newer than what we
    /// hold (apply) vs redelivered/stale rows (skip). Absent keys and unknown
    /// tables count as fresh — the apply path owns those errors.
    fn filter_newer_rows(&self, table: &str, chunk: &[BatchRow]) -> EngineResult<RowFilter> {
        let db = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
        let is_fresh = |(key, _, hlc, _): &BatchRow| match db.local_get_versioned(table, key) {
            Ok(Some((_, cur, _))) => cur < *hlc,
            _ => true,
        };
        let stale = chunk.iter().filter(|r| !is_fresh(r)).count();
        Ok(if stale == 0 {
            RowFilter::AllFresh
        } else if stale == chunk.len() {
            RowFilter::AllStale
        } else {
            RowFilter::Some(chunk.iter().filter(|r| is_fresh(r)).cloned().collect())
        })
    }

    /// Catalog lookups for stateless gateways (the catalog is identical
    /// cluster-wide, so the local copy answers).
    pub fn table_primary_key(&self, table: &str) -> Option<Vec<String>> {
        self.local.read().ok()?.table_primary_key(table).ok()
    }

    /// See [`Node::table_primary_key`].
    pub fn search_index_fields(&self, table: &str) -> Option<Vec<(String, String)>> {
        self.local.read().ok()?.search_index_fields(table)
    }

    /// One background NRT tick over the local shard's search indexes (the
    /// engine's `search_refresh_tick`): read-lock gate first so the common
    /// no-index case never takes the write lock.
    pub fn search_refresh_tick(&self) -> EngineResult<()> {
        let has = self
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .has_search_indexes();
        if has {
            self.local
                .write()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                .search_refresh_tick()?;
        }
        Ok(())
    }

    /// The buffered half of [`Node::apply_batch_local`]: append + apply every
    /// row under one write-lock acquisition (and one search-index refresh
    /// check for the whole batch), returning the last row's commit point so
    /// the caller can overlap the single fsync with peer round-trips.
    fn apply_batch_buffered(
        &self,
        table: &str,
        rows: &[BatchRow],
    ) -> EngineResult<Option<(WalCommit, Arc<WalSync>)>> {
        self.local
            .write()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .apply_batch_buffered(table, rows)
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

    /// Like [`Node::cluster_scan`] but stops once `limit` surviving rows have
    /// been produced — the push-down for an unfiltered, unordered `LIMIT n`.
    /// Without it a `SELECT … LIMIT 2` on a million-row table gathered and
    /// merged every shard in full before the executor threw all but two rows
    /// away (slow, and it re-materialised the whole table on the coordinator).
    ///
    /// Every source (local shard + each peer) is paged in key order in
    /// lockstep and merged last-writer-wins. A key is only emitted once every
    /// still-active source has scanned past it — the "seal" frontier, the
    /// minimum of the active sources' latest keys — so a tombstone or a
    /// higher-HLC version arriving from a slower replica still wins, exactly as
    /// in a full scan, but only over the short key prefix a `LIMIT` needs.
    /// Rows come back in key order (an unordered `LIMIT` promises no order).
    fn cluster_scan_limited(
        &self,
        table: &str,
        oc: Option<Consistency>,
        limit: usize,
    ) -> EngineResult<Vec<(Vec<u8>, Document)>> {
        self.counters.reads_total.fetch_add(1, Ordering::Relaxed);
        let needed = oc
            .unwrap_or(self.cfg.read_consistency)
            .required(self.member_count());
        if limit == 0 {
            return Ok(Vec::new());
        }

        let addrs = self.peer_addrs();
        // Per-source paging state. Index 0 is the local shard; the rest track
        // `addrs`. `done` = this source has delivered its whole shard; `ok` = it
        // has never errored (only `ok` sources count toward the read quorum).
        let mut local_cursor: Option<Vec<u8>> = None;
        let mut local_done = false;
        let mut peer_cursor: Vec<Option<Vec<u8>>> = vec![None; addrs.len()];
        let mut peer_done: Vec<bool> = vec![false; addrs.len()];
        let mut peer_ok: Vec<bool> = vec![true; addrs.len()];

        let mut merged: BTreeMap<Vec<u8>, (Hlc, Option<Vec<u8>>)> = BTreeMap::new();
        let mut out: Vec<(Vec<u8>, Document)> = Vec::new();
        let mut max_hlc: Option<Hlc> = None;

        loop {
            // 1. Pull the next page from every still-active source and merge it.
            if !local_done {
                let rows = self
                    .local
                    .read()
                    .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
                    .local_scan_versioned_page(table, local_cursor.as_deref(), SCAN_PAGE_ROWS)?;
                local_done = rows.len() < SCAN_PAGE_ROWS;
                if let Some((k, ..)) = rows.last() {
                    local_cursor = Some(k.clone());
                }
                for (key, value, hlc, is_put) in rows {
                    merge_row(&mut merged, key, is_put.then_some(value), hlc);
                }
            }
            for (i, addr) in addrs.iter().enumerate() {
                if peer_done[i] || !peer_ok[i] {
                    continue;
                }
                match self.scan_peer_page(addr, table, peer_cursor[i].as_deref()) {
                    Some((rows, exhausted)) => {
                        peer_done[i] = exhausted;
                        if let Some((k, ..)) = rows.last() {
                            peer_cursor[i] = Some(k.clone());
                        }
                        for (key, value, hlc, is_put) in rows {
                            merge_row(&mut merged, key, is_put.then_some(value), hlc);
                        }
                    }
                    // Peer failed mid-scan: stop reading it and drop it from the
                    // responder count (its rows still reach us via other replicas).
                    None => {
                        peer_ok[i] = false;
                        peer_done[i] = true;
                    }
                }
            }

            // 2. Seal = the smallest latest-key among sources that still have
            //    more to deliver. Everything <= seal is final: each active
            //    source has returned all its keys <= its own latest key, hence
            //    all its keys <= seal. Done sources cap nothing (fully seen); if
            //    none remain active, every buffered key is final.
            let mut frontiers: Vec<&Vec<u8>> = Vec::new();
            if !local_done {
                if let Some(c) = local_cursor.as_ref() {
                    frontiers.push(c);
                }
            }
            for i in 0..addrs.len() {
                if !peer_done[i] && peer_ok[i] {
                    if let Some(c) = peer_cursor[i].as_ref() {
                        frontiers.push(c);
                    }
                }
            }
            let all_done = frontiers.is_empty();
            let seal = frontiers.into_iter().min().cloned();

            // 3. Emit finalised survivors in key order, up to `limit`.
            let finalized: Vec<Vec<u8>> = match &seal {
                Some(s) => merged.range(..=s.clone()).map(|(k, _)| k.clone()).collect(),
                None => merged.keys().cloned().collect(),
            };
            for k in finalized {
                let (hlc, val) = merged.remove(&k).expect("finalised key present");
                max_hlc = Some(max_hlc.map_or(hlc, |m| m.max(hlc)));
                if let Some(bytes) = val {
                    if let Value::Document(doc) = Value::decode(&bytes)
                        .map_err(|e| EngineError::Cluster(format!("corrupt row: {e}")))?
                    {
                        out.push((k, doc));
                        if out.len() >= limit {
                            break;
                        }
                    }
                }
            }

            // 4. Stop once we have enough rows or every source is drained.
            if out.len() >= limit || all_done {
                break;
            }
        }

        // Read quorum over the scanned prefix: the local shard always responds;
        // require a quorum of members to have contributed without error.
        let responders = 1 + peer_ok.iter().filter(|ok| **ok).count();
        if responders < needed {
            self.counters
                .read_quorum_failures
                .fetch_add(1, Ordering::Relaxed);
            return Err(EngineError::Cluster(format!(
                "read quorum not met: {responders}/{needed} members responded"
            )));
        }
        if let Some(max) = max_hlc {
            self.clock.observe(max);
        }
        out.truncate(limit);
        Ok(out)
    }

    /// Fetch one `ScanPage`-sized page of `table` from `addr`, in key order
    /// after `after`. `(rows, exhausted)` where `exhausted` marks the peer's
    /// last (short) page; `None` if the RPC failed. Used by the on-demand,
    /// early-terminating [`Node::cluster_scan_limited`] (vs. the drain-to-a-
    /// channel [`Node::scan_peer_paged`] a full gather uses).
    fn scan_peer_page(
        &self,
        addr: &str,
        table: &str,
        after: Option<&[u8]>,
    ) -> Option<(Vec<BatchRow>, bool)> {
        self.counters.peer_requests.fetch_add(1, Ordering::Relaxed);
        match self.pool.call(
            addr,
            &Request::ScanPage {
                table: table.to_string(),
                after: after.map(<[u8]>::to_vec),
                limit: SCAN_PAGE_ROWS as u32,
            },
        ) {
            Ok(Response::Scan { rows }) => {
                let exhausted = rows.len() < SCAN_PAGE_ROWS;
                Some((rows, exhausted))
            }
            _ => {
                self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
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

        self.resolve_candidates(table, keys, oc)
    }

    /// Resolve a gathered candidate-key set to authoritative rows.
    ///
    /// Few candidates: one quorum point read each — cheap and exact. Many
    /// candidates: **one** paged, LWW-merged pass over the table intersected
    /// with the set. A broad predicate (`WHERE channel = 'X'` matching 100k
    /// rows, or the count(*) behind it) previously issued one sequential
    /// quorum RPC fan-out *per key* — minutes of round-trips for what a
    /// single merged scan answers with the same read-quorum guarantee.
    fn resolve_candidates(
        &self,
        table: &str,
        keys: BTreeMap<Vec<u8>, ()>,
        oc: Option<Consistency>,
    ) -> EngineResult<Vec<(Vec<u8>, Document)>> {
        const POINT_READ_MAX: usize = 256;
        if keys.len() <= POINT_READ_MAX {
            let mut out = Vec::new();
            for key in keys.into_keys() {
                out.extend(self.point_get(table, &key, oc)?);
            }
            return Ok(out);
        }
        let rows = self.cluster_scan(table, oc)?;
        Ok(rows
            .into_iter()
            .filter(|(k, _)| keys.contains_key(k))
            .collect())
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

        self.resolve_candidates(table, keys, oc)
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

    /// Distributed full-text search (the vector-search pattern): every
    /// member searches its local shard, the coordinator merges hits by
    /// score, re-reads survivors at read consistency, applies the residual
    /// `filter`, and snippets `highlights` from its own index (every node
    /// holds the same index definition). `k = None` is the unranked path
    /// (every matching row, scores 0.0).
    ///
    /// Scoring is per-shard BM25 (Elasticsearch's default across shards): a
    /// row replicated on several members keeps its best per-shard score.
    pub fn search(
        self: &Arc<Self>,
        table: &str,
        query: &skaidb_fts::SearchQuery,
        k: Option<usize>,
        filter: &Option<Expr>,
        highlights: &[(String, usize)],
    ) -> EngineResult<Vec<(Vec<u8>, Document, f32)>> {
        // Over-fetch per shard so the merge (and any filtering) still yields k.
        let fetch = k.map(|k| {
            if filter.is_some() {
                k.saturating_mul(4).max(k.saturating_add(16))
            } else {
                k.max(1)
            }
        });

        // Best score seen per key across all shards.
        let mut best: HashMap<Vec<u8>, f32> = HashMap::new();
        let consider = |key: Vec<u8>, score: f32, best: &mut HashMap<Vec<u8>, f32>| {
            best.entry(key)
                .and_modify(|s| {
                    if score > *s {
                        *s = score;
                    }
                })
                .or_insert(score);
        };
        // Local shard under the write lock: pending index writes commit
        // first, so the coordinator reads its own writes.
        {
            let mut db = self
                .local
                .write()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
            for (key, score) in db.search_local_commit_if_dirty(table, query, fetch)? {
                consider(key, score, &mut best);
            }
        }
        // Peer shards, scattered concurrently (unreachable peers are
        // skipped — their rows still surface through the replicas that
        // remain reachable).
        let req = Request::Search {
            table: table.to_string(),
            query: serde_json::to_string(query)
                .map_err(|e| EngineError::Cluster(format!("encode search query: {e}")))?,
            k: fetch.map_or(0, |f| f as u32),
        };
        let addrs = self.peer_addrs();
        for hits in scatter(&addrs, |addr| match self.pool.call(addr, &req) {
            Ok(Response::SearchHits { hits }) => hits,
            _ => Vec::new(),
        }) {
            for (key, score) in hits {
                consider(key, score, &mut best);
            }
        }

        // Coordinator-side snippet generators, one per requested column.
        let highlighters = {
            let db = self
                .local
                .read()
                .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?;
            highlights
                .iter()
                .map(|(col, max_chars)| {
                    db.search_highlighter(table, query, col, *max_chars)
                        .map(|h| (col.clone(), h))
                })
                .collect::<EngineResult<Vec<_>>>()?
        };

        // Rank globally by score, then re-read + filter until we have k.
        let mut ranked: Vec<(Vec<u8>, f32)> = best.into_iter().collect();
        ranked.sort_by(|a, b| b.1.total_cmp(&a.1));
        let mut out = Vec::new();
        for (key, score) in ranked {
            let rows = filter_rows(filter, self.point_get(table, &key, None)?)?;
            if let Some((_, mut doc)) = rows.into_iter().next() {
                for (col, h) in &highlighters {
                    let snippet = h.snippet_doc(&doc, col);
                    doc.insert(format!("_highlight_{col}"), Value::String(snippet));
                }
                out.push((key, doc, score));
                if k.is_some_and(|k| out.len() >= k) {
                    break;
                }
            }
        }
        Ok(out)
    }
}

/// A pending replicated mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
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

/// The error returned for a write rejected by memory-pressure load shedding.
/// Retryable — the client (or coordinator, which then hints the replica)
/// should back off and retry once the node has drained and recovered.
fn shed_error() -> EngineError {
    EngineError::Cluster("memory pressure: node is shedding writes, retry".into())
}

/// Serialize one on-disk hint record: `table`, `key`, op (`1`+value / `0`),
/// then the 12-byte HLC. Length-prefixed so records can be streamed back.
fn encode_hint_record(table: &str, key: &[u8], op: &WriteOp, hlc: Hlc) -> Vec<u8> {
    let mut o = Vec::with_capacity(table.len() + key.len() + 24);
    o.extend_from_slice(&(table.len() as u32).to_le_bytes());
    o.extend_from_slice(table.as_bytes());
    o.extend_from_slice(&(key.len() as u32).to_le_bytes());
    o.extend_from_slice(key);
    match op {
        WriteOp::Put(v) => {
            o.push(1);
            o.extend_from_slice(&(v.len() as u32).to_le_bytes());
            o.extend_from_slice(v);
        }
        WriteOp::Delete => o.push(0),
    }
    o.extend_from_slice(&hlc.to_bytes());
    o
}

/// Decode one hint record from `bytes[*i..]`, advancing `i` past it. `None`
/// on an incomplete record (`i` is left unchanged so the caller can refill
/// its buffer and retry, or treat a truncated tail as torn and stop).
fn decode_hint_record(bytes: &[u8], i: &mut usize) -> Option<(String, Vec<u8>, WriteOp, Hlc)> {
    fn rd_u32(b: &[u8], i: &mut usize) -> Option<usize> {
        let end = i.checked_add(4)?;
        if end > b.len() {
            return None;
        }
        let v = u32::from_le_bytes(b[*i..end].try_into().ok()?) as usize;
        *i = end;
        Some(v)
    }
    let start = *i;
    let mut j = *i;
    let parsed = (|| {
        let tl = rd_u32(bytes, &mut j)?;
        if j + tl > bytes.len() {
            return None;
        }
        let table = String::from_utf8_lossy(&bytes[j..j + tl]).into_owned();
        j += tl;
        let kl = rd_u32(bytes, &mut j)?;
        if j + kl > bytes.len() {
            return None;
        }
        let key = bytes[j..j + kl].to_vec();
        j += kl;
        if j >= bytes.len() {
            return None;
        }
        let tag = bytes[j];
        j += 1;
        let op = if tag == 1 {
            let vl = rd_u32(bytes, &mut j)?;
            if j + vl > bytes.len() {
                return None;
            }
            let v = bytes[j..j + vl].to_vec();
            j += vl;
            WriteOp::Put(v)
        } else {
            WriteOp::Delete
        };
        if j + 12 > bytes.len() {
            return None;
        }
        let mut hb = [0u8; 12];
        hb.copy_from_slice(&bytes[j..j + 12]);
        j += 12;
        Some((table, key, op, Hlc::from_bytes(hb)))
    })();
    match parsed {
        Some(rec) => {
            *i = j;
            Some(rec)
        }
        None => {
            *i = start;
            None
        }
    }
}

/// Decode a whole hint log written by [`encode_hint_record`] (tests; the
/// production drain streams via [`decode_hint_record`] a page at a time).
/// A truncated trailing record (torn write) is ignored.
#[cfg(test)]
fn decode_hint_records(bytes: &[u8]) -> Vec<(String, Vec<u8>, WriteOp, Hlc)> {
    let mut out = Vec::new();
    let mut i = 0;
    while let Some(rec) = decode_hint_record(bytes, &mut i) {
        out.push(rec);
    }
    out
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
/// Merge per-shard aggregation partials. Shards partition the key-space
/// (ring arcs tile it exactly once), so doc counts and value counts add,
/// sums add (SQL semantics: all-NULL stays NULL, otherwise NULL is the
/// identity), and min/max fold — each preserving the typed Value the
/// winning shard produced. Group keys union across shards; output is
/// key-ordered for determinism.
fn merge_agg_shards(
    agg: &skaidb_fts::AggRequest,
    parts: Vec<Vec<skaidb_fts::AggRow>>,
) -> Vec<skaidb_fts::AggRow> {
    use skaidb_fts::AggMetricFunc as F;
    use skaidb_types::Value;

    fn add(a: Value, b: &Value) -> Value {
        match (a, b) {
            (Value::Null, b) => b.clone(),
            (a, Value::Null) => a,
            (Value::Int(x), Value::Int(y)) => Value::Int(x + y),
            (Value::Float(x), Value::Float(y)) => Value::Float(x + y),
            (Value::Int(x), Value::Float(y)) => Value::Float(x as f64 + y),
            (Value::Float(x), Value::Int(y)) => Value::Float(x + *y as f64),
            (a, _) => a, // unreachable for column-typed metrics
        }
    }
    fn extreme(a: Value, b: &Value, want_greater: bool) -> Value {
        let num = |v: &Value| match v {
            Value::Int(x) => Some(*x as f64),
            Value::Float(x) => Some(*x),
            Value::Timestamp(x) => Some(*x as f64),
            _ => None,
        };
        match (num(&a), num(b)) {
            (None, _) => b.clone(),
            (_, None) => a,
            (Some(x), Some(y)) => {
                if (y > x) == want_greater && y != x {
                    b.clone()
                } else {
                    a
                }
            }
        }
    }

    let mut merged: std::collections::BTreeMap<Vec<u8>, skaidb_fts::AggRow> =
        std::collections::BTreeMap::new();
    for part in parts {
        for row in part {
            let slot = merged.entry(row.key.encode_key());
            match slot {
                std::collections::btree_map::Entry::Vacant(v) => {
                    v.insert(row);
                }
                std::collections::btree_map::Entry::Occupied(mut o) => {
                    let acc = o.get_mut();
                    acc.count += row.count;
                    for (i, metric) in agg.metrics.iter().enumerate() {
                        let cur = std::mem::replace(&mut acc.metrics[i], Value::Null);
                        acc.metrics[i] = match metric.func {
                            F::Count | F::ValueCount | F::Sum => add(cur, &row.metrics[i]),
                            F::Min => extreme(cur, &row.metrics[i], false),
                            F::Max => extreme(cur, &row.metrics[i], true),
                            // Filtered out before the scatter.
                            F::Avg | F::CountDistinct | F::ApproxCountDistinct => cur,
                        };
                    }
                }
            }
        }
    }
    merged.into_values().collect()
}

/// Split matchers for a scatter: the internode requests carry equality
/// forms only, so regex matchers stay behind — peers answer with the
/// equality-matched superset and the coordinator re-applies the regex
/// forms on the gathered series labels.
fn split_wire_matchers(
    matchers: &[skaidb_tsdb::Matcher],
) -> (Vec<(bool, String, String)>, Vec<skaidb_tsdb::Matcher>) {
    let mut wire = Vec::new();
    let mut post = Vec::new();
    for m in matchers {
        match m {
            skaidb_tsdb::Matcher::Eq(k, v) => wire.push((false, k.clone(), v.clone())),
            skaidb_tsdb::Matcher::Ne(k, v) => wire.push((true, k.clone(), v.clone())),
            other => post.push(other.clone()),
        }
    }
    (wire, post)
}

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

/// Engine partial → wire tuple (see [`internode::TsPartialRow`]).
fn partial_to_wire(p: &skaidb_engine::TsPartial) -> internode::TsPartialRow {
    (
        p.bucket_ts,
        p.count,
        p.sum,
        p.min,
        p.max,
        p.first_ts,
        p.first_val,
        p.last_ts,
        p.last_val,
        p.increase,
    )
}

/// Wire tuple → engine partial (see [`internode::TsPartialRow`]).
fn partial_from_wire(r: &internode::TsPartialRow) -> skaidb_engine::TsPartial {
    let (bucket_ts, count, sum, min, max, first_ts, first_val, last_ts, last_val, increase) = *r;
    skaidb_engine::TsPartial {
        bucket_ts,
        count,
        sum,
        min,
        max,
        first_ts,
        first_val,
        last_ts,
        last_val,
        increase,
    }
}

/// Render a user's DDL in verifier form (derives the same credential the
/// engine would from the plaintext, so every member stores the same bytes).
fn render_user_verifier_ddl(name: &str, password: &str) -> String {
    let salt = skaidb_auth::crypto::sha256(format!("skaidb-user:{name}").as_bytes())[..16].to_vec();
    let credential =
        skaidb_auth::ScramCredential::new(password, &salt, skaidb_auth::DEFAULT_ITERATIONS);
    format!("CREATE USER {name} VERIFIER '{}'", credential.encode())
}

fn is_ddl(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateTable(_)
            | Statement::CreateTimeseriesTable(_)
            | Statement::CreateRollup(_)
            | Statement::DropTable { .. }
            | Statement::CreateIndex(_)
            | Statement::DropIndex { .. }
            | Statement::CreateVectorIndex(_)
            | Statement::DropVectorIndex { .. }
            | Statement::CreateSearchIndex(_)
            | Statement::DropSearchIndex { .. }
            | Statement::RebuildSearchIndex { .. }
            | Statement::AlterSearchIndex { .. }
            | Statement::AlterVectorIndex { .. }
            | Statement::AlterTable(_)
            | Statement::CreateDatabase { .. }
            | Statement::DropDatabase { .. }
            | Statement::CreateUser(_)
            | Statement::AlterUser { .. }
            | Statement::DropUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::Grant { .. }
            | Statement::Revoke { .. }
            | Statement::GrantRole { .. }
            | Statement::RevokeRole { .. }
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
                            // Hinted handoff: replay to this replica when it
                            // is reachable again (repair is the backstop).
                            self.node.store_ts_hint(replica, table, batch.clone());
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

    fn ts_partials(
        &self,
        table: &str,
        matchers: &[skaidb_tsdb::Matcher],
        t0: i64,
        t1: i64,
        bucket_ms: i64,
    ) -> EngineResult<Vec<(skaidb_tsdb::Labels, Vec<skaidb_engine::TsPartial>)>> {
        self.node
            .ts_partials_scatter(table, matchers, t0, t1, bucket_ms, self.oc)
    }

    fn ts_rollup_info(&self, table: &str) -> EngineResult<skaidb_engine::TsRollupInfo> {
        // Rollup registrations replicate with the schema, and the local data
        // frontier is a sound horizon: it can only lag the cluster's, which
        // only widens the exactly-served source range.
        let (horizon, complete_below, rollups) = self
            .node
            .local
            .read()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .ts_rollup_info(table)?;
        // The opportunistic boundary is a *local* head watermark; a peer's
        // head may still hold older samples its rollup hasn't seen, so on a
        // multi-member cluster only the retention tier routes to rollups.
        // Extending this needs a min-over-replicas boundary exchange (the
        // sharded-partials work in docs/TODO.md).
        let complete_below = (self.node.member_count() <= 1).then_some(complete_below).flatten();
        Ok((horizon, complete_below, rollups))
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

    fn search(
        &mut self,
        table: &str,
        query: &skaidb_fts::SearchQuery,
        k: Option<usize>,
        filter: &Option<Expr>,
        highlights: &[(String, usize)],
    ) -> EngineResult<Vec<(Vec<u8>, Document, f32)>> {
        // Multi-node: scatter to every member's local shard and merge
        // (per-shard BM25, re-read at read consistency).
        if self.node.member_count() > 1 {
            return self.node.search(table, query, k, filter, highlights);
        }
        // Sole member: every row is local. Serve under the write lock so
        // pending index writes commit first (read-your-writes).
        self.node
            .local
            .write()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .search_commit_if_dirty(table, query, k, filter, highlights)
    }

    fn search_sorted(
        &mut self,
        table: &str,
        query: &skaidb_fts::SearchQuery,
        sort: &skaidb_fts::SortSpec,
        k: usize,
        filter: &Option<Expr>,
        highlights: &[(String, usize)],
    ) -> EngineResult<Option<Vec<(Vec<u8>, Document)>>> {
        // One index holding every row serves locally; a sharded corpus
        // scatters per-shard sorted top-k over ownership arcs and merges —
        // declining (to the coordinator's gather-and-sort) when a residual
        // filter is present or any member cannot answer.
        let members = self.node.member_count();
        if members > 1 && self.node.cfg.replication_factor < members {
            return self
                .node
                .search_sorted_sharded(table, query, sort, k, filter, highlights);
        }
        self.node
            .local
            .write()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .search_sorted(table, query, sort, k, filter, highlights, None)
    }

    fn search_aggregate(
        &mut self,
        table: &str,
        query: &skaidb_fts::SearchQuery,
        agg: &skaidb_fts::AggRequest,
    ) -> EngineResult<Option<Vec<skaidb_fts::AggRow>>> {
        // One index holding every row (a sole member, or RF ≥ members)
        // serves locally. A sharded corpus (RF < members) scatters: each
        // member aggregates its primary-owned key-space (an ownership
        // filter over the `_ring` fast field) and the partials merge —
        // exact-or-decline throughout, so any wobble (epoch change, silent
        // peer, unmergeable metric) falls back to the deduped row gather.
        let members = self.node.member_count();
        if members > 1 && self.node.cfg.replication_factor < members {
            return self.node.search_aggregate_sharded(table, query, agg);
        }
        self.node
            .local
            .write()
            .map_err(|_| EngineError::Cluster("local lock poisoned".into()))?
            .search_aggregate(table, query, agg, None)
    }

    fn count_rows(&self, table: &str) -> EngineResult<Option<usize>> {
        // Full-copy cluster (RF >= members): every node holds every row, so
        // the local engine's key stats answer without a cluster gather — the
        // gather materialized the whole merged table on the coordinator, and
        // a plain count(*) OOM-killed a production node (2026-07-11). Same
        // freshness trade the search-index paths already make at RF >=
        // members: the count may lag an in-flight write by a beat.
        if self.node.cfg.replication_factor >= self.node.member_count() {
            if let Some(db) = self.node.local_read_bounded() {
                return db.local_count_rows(table);
            }
        }
        Ok(None)
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

    fn matching_rows_ordered(
        &self,
        table: &str,
        filter: &Option<Expr>,
        order: Option<&str>,
        fetch_limit: Option<usize>,
    ) -> EngineResult<(Vec<(Vec<u8>, Document)>, bool)> {
        // Push a plain `LIMIT n` into the gather so an unfiltered, unordered
        // scan reads only the first n rows' worth across the ring instead of
        // every shard in full. The filtered/indexed paths already gather
        // bounded candidate sets, and an ordered scan needs a different sort
        // key than the on-disk key order — both keep the default full gather.
        if let (None, None, Some(lim)) = (filter, order, fetch_limit) {
            let rows = self.node.cluster_scan_limited(table, self.oc, lim)?;
            return Ok((rows, false));
        }
        Ok((self.matching_rows(table, filter)?, false))
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
    fn bulk_gate_bounded_wait_rejects_when_saturated() {
        let gate = BulkGate {
            max: 1,
            active: std::sync::Mutex::new(0),
            cv: std::sync::Condvar::new(),
        };
        let held = gate.acquire(Duration::from_millis(10)).expect("first permit");
        // Saturated: a bounded wait must give up rather than park forever.
        let t = Instant::now();
        assert!(gate.acquire(Duration::from_millis(50)).is_none());
        assert!(t.elapsed() >= Duration::from_millis(50));
        drop(held);
        assert!(gate.acquire(Duration::from_millis(10)).is_some());
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

    /// rf=1 NodeConfig: every key lives on exactly one node, so a full-text
    /// result set that is complete from any coordinator proves the scatter
    /// actually gathered the other members' shards.
    #[cfg(test)]
    fn rf1(id: &str, addr: &str, members: &[(NodeId, String)]) -> NodeConfig {
        NodeConfig {
            id: NodeId::new(id),
            internode_addr: addr.to_string(),
            members: members.to_vec(),
            replication_factor: 1,
            vnodes_per_node: 64,
            read_consistency: Consistency::One,
            write_consistency: Consistency::One,
            auth: Arc::new(Authenticator::None),
            auto_join: false,
            anti_entropy_interval_secs: 0,
        }
    }

    /// The sorted `id` column of a search result set.
    #[cfg(test)]
    fn sorted_ids(rs: skaidb_engine::ResultSet) -> Vec<i64> {
        let mut out: Vec<i64> = rs
            .rows
            .iter()
            .map(|r| match &r[0] {
                Value::Int(i) => *i,
                other => panic!("expected int id, got {other:?}"),
            })
            .collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn distributed_full_text_search() {
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(Database::open(temp_dir("ftsa")).unwrap(), rf1("a", &a, &members));
        let nb = Node::new(Database::open(temp_dir("ftsb")).unwrap(), rf1("b", &b, &members));
        let nc = Node::new(Database::open(temp_dir("ftsc")).unwrap(), rf1("c", &c, &members));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        nc.serve_internode().unwrap();

        na.execute("CREATE TABLE articles (PRIMARY KEY (id))").unwrap();
        // Broadcast DDL: every node builds its own (initially empty) index.
        na.execute("CREATE SEARCH INDEX articles_fts ON articles (body)").unwrap();
        // Enough rows that (with rf=1 over 64 vnodes) every node owns some.
        for i in 1..=30 {
            let text = if i % 3 == 0 {
                "quick brown fox jumps"
            } else if i % 3 == 1 {
                "slow roasted vegetables"
            } else {
                "unrelated filler words"
            };
            na.execute(&format!("INSERT INTO articles (id, body, flag) VALUES ({i}, '{text}', {})", i % 2 == 0))
                .unwrap();
        }

        // Predicate-only search from every coordinator returns the complete
        // cross-shard match set (ids 3, 6, ..., 30).
        let expect: Vec<i64> = (1..=30).filter(|i| i % 3 == 0).collect();
        for coord in [&na, &nb, &nc] {
            let rs = rows(coord.execute("SELECT id FROM articles WHERE MATCH(body, 'fox')").unwrap());
            assert_eq!(sorted_ids(rs), expect);
        }

        // Ranked top-k from another node: scatter, merge by score, k rows
        // best-first with score() projected.
        let rs = rows(
            nb.execute(
                "SELECT id, score() FROM articles WHERE MATCH(body, 'quick fox') \
                 ORDER BY score() DESC LIMIT 5",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows.len(), 5);
        for row in &rs.rows {
            assert!(matches!(row[1], Value::Float(s) if s > 0.0), "scored hit: {row:?}");
        }

        // Residual filter applies after the authoritative re-read.
        let rs = rows(
            nc.execute("SELECT id FROM articles WHERE MATCH(body, 'vegetables') AND flag = true")
                .unwrap(),
        );
        assert_eq!(sorted_ids(rs), vec![4, 10, 16, 22, 28]);

        // Bool composition pushes the whole tree to every shard.
        let rs = rows(
            nb.execute(
                "SELECT id FROM articles \
                 WHERE (MATCH(body, 'fox') OR MATCH(body, 'vegetables')) \
                   AND NOT MATCH(body, 'quick')",
            )
            .unwrap(),
        );
        assert_eq!(sorted_ids(rs), (1..=30).filter(|i| i % 3 == 1).collect::<Vec<_>>());

        // Highlighting is generated coordinator-side after the re-read.
        let rs = rows(
            nc.execute(
                "SELECT id, HIGHLIGHT(body, 40) AS s FROM articles \
                 WHERE MATCH(body, 'roasted') ORDER BY score() DESC LIMIT 3",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows.len(), 3);
        for row in &rs.rows {
            assert!(
                matches!(&row[1], Value::String(s) if s.contains("<b>roasted</b>")),
                "snippet: {row:?}"
            );
        }

        // SUGGEST routes to the coordinator's local term dictionary on the
        // cluster path (the v0.43 fleet smoke found it mis-routed to the
        // data-plane executor).
        let rs = rows(nb.execute("SUGGEST 'vegetbles' ON articles_fts LIMIT 1").unwrap());
        assert_eq!(rs.rows.len(), 1, "{:?}", rs.rows);
        assert_eq!(rs.rows[0][1], Value::String("vegetables".into()));

        // Per-group top-k over the sharded corpus: every match gathers
        // *scored* from all shards, then each group keeps its best rows.
        let rs = rows(
            nc.execute(
                "SELECT flag, id, score() FROM articles WHERE MATCH(body, 'fox') \
                 GROUP BY flag TOP 2 BY score()",
            )
            .unwrap(),
        );
        assert_eq!(rs.rows.len(), 4, "2 rows per flag group: {:?}", rs.rows);
        let flags: std::collections::HashSet<bool> = rs
            .rows
            .iter()
            .map(|r| match r[0] {
                Value::Bool(b) => b,
                ref other => panic!("expected bool flag, got {other:?}"),
            })
            .collect();
        assert_eq!(flags.len(), 2, "both groups represented");
        for row in &rs.rows {
            assert!(matches!(row[2], Value::Float(s) if s > 0.0), "scored: {row:?}");
        }
    }

    #[test]
    fn distributed_search_aggregates() {
        // rf=1 over three members: the exact-pushdown gate declines
        // (per-shard partials would need ownership filters), so grouped
        // search SELECTs take the coordinator fallback — matching rows are
        // gathered (deduped by key) and aggregated there. Completeness of
        // the groups proves the cross-shard gather.
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(Database::open(temp_dir("fagg-a")).unwrap(), rf1("a", &a, &members));
        let nb = Node::new(Database::open(temp_dir("fagg-b")).unwrap(), rf1("b", &b, &members));
        let nc = Node::new(Database::open(temp_dir("fagg-c")).unwrap(), rf1("c", &c, &members));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        nc.serve_internode().unwrap();

        na.execute("CREATE TABLE s (PRIMARY KEY (id))").unwrap();
        na.execute(
            "CREATE SEARCH INDEX s_fts ON s (product, region, units) WITH (\
             region.type = 'keyword', units.type = 'long')",
        )
        .unwrap();
        for i in 1..=30 {
            let region = if i % 2 == 0 { "east" } else { "west" };
            na.execute(&format!(
                "INSERT INTO s (id, product, region, units) VALUES ({i}, 'widget number {i}', '{region}', {i})"
            ))
            .unwrap();
        }

        for coord in [&na, &nb, &nc] {
            let rs = rows(
                coord
                    .execute(
                        "SELECT region, COUNT(*), SUM(units) FROM s \
                         WHERE MATCH(product, 'widget') GROUP BY region",
                    )
                    .unwrap(),
            );
            let mut got = rs.rows;
            got.sort_by_key(|r| r[0].encode_key());
            assert_eq!(
                got,
                vec![
                    // east: 15 even ids summing to 240; west: 15 odd = 225.
                    vec![Value::String("east".into()), Value::Int(15), Value::Int(240)],
                    vec![Value::String("west".into()), Value::Int(15), Value::Int(225)],
                ]
            );
        }
    }

    /// The sharded partials path (RF < members): every member aggregates
    /// its primary-owned key-space and the coordinator merges — proven
    /// directly (the sharded call answers rather than declines, and its
    /// numbers equal ground truth), end-to-end over SQL from every
    /// coordinator, with unmergeable metrics declining, and with a dead
    /// member forcing the exact fallback (rf=2 keeps every key readable).
    #[test]
    fn sharded_search_aggregate_partials_merge_exactly() {
        use skaidb_fts::{AggGroupBy, AggMetric, AggMetricFunc, AggRequest};
        let rf2 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
            replication_factor: 2,
            ..rf1(id, addr, members)
        };
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(Database::open(temp_dir("sagg-a")).unwrap(), rf2("a", &a, &members));
        let nb = Node::new(Database::open(temp_dir("sagg-b")).unwrap(), rf2("b", &b, &members));
        let nc = Node::new(Database::open(temp_dir("sagg-c")).unwrap(), rf2("c", &c, &members));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        nc.serve_internode().unwrap();

        na.execute("CREATE TABLE s (PRIMARY KEY (id))").unwrap();
        na.execute(
            "CREATE SEARCH INDEX s_fts ON s (product, region, units) WITH (\
             region.type = 'keyword', units.type = 'long')",
        )
        .unwrap();
        let n = 60i64;
        for i in 1..=n {
            let region = if i % 2 == 0 { "east" } else { "west" };
            na.execute(&format!(
                "INSERT INTO s (id, product, region, units) VALUES ({i}, 'widget number {i}', '{region}', {i})"
            ))
            .unwrap();
        }

        let query = skaidb_fts::SearchQuery::Match {
            field: Some("product".into()),
            text: "widget".into(),
        };
        // Direct proof the sharded path SERVES (rather than declining):
        // grouped doc counts...
        let grouped = AggRequest {
            group_by: Some(AggGroupBy::Keyword("region".into())),
            metrics: vec![AggMetric {
                func: AggMetricFunc::Count,
                column: None,
            }],
        };
        let mut agg_rows = na
            .search_aggregate_sharded("s", &query, &grouped)
            .unwrap()
            .expect("sharded partials must serve grouped doc counts");
        agg_rows.sort_by_key(|r| r.key.encode_key());
        assert_eq!(agg_rows.len(), 2);
        assert_eq!(
            (agg_rows[0].key.clone(), agg_rows[0].count),
            (Value::String("east".into()), 30)
        );
        assert_eq!(
            (agg_rows[1].key.clone(), agg_rows[1].count),
            (Value::String("west".into()), 30)
        );

        // ...and global count/sum/min/max, merged across shards. Every key
        // is replicated on TWO nodes; the ownership arcs count each exactly
        // once.
        let units = |f| AggMetric {
            func: f,
            column: Some("units".into()),
        };
        let global = AggRequest {
            group_by: None,
            metrics: vec![
                AggMetric {
                    func: AggMetricFunc::Count,
                    column: None,
                },
                units(AggMetricFunc::Sum),
                units(AggMetricFunc::Min),
                units(AggMetricFunc::Max),
            ],
        };
        let agg_rows = na
            .search_aggregate_sharded("s", &query, &global)
            .unwrap()
            .expect("sharded partials must serve global metrics");
        assert_eq!(agg_rows.len(), 1);
        assert_eq!(agg_rows[0].count, n as u64);
        assert_eq!(
            agg_rows[0].metrics,
            vec![
                Value::Int(n),
                Value::Int(n * (n + 1) / 2), // 1830
                Value::Int(1),
                Value::Int(n),
            ]
        );

        // AVG merges via the SUM+COUNT rewrite (Float, exact).
        let avg = AggRequest {
            group_by: None,
            metrics: vec![units(AggMetricFunc::Avg), units(AggMetricFunc::Max)],
        };
        let avg_rows = na
            .search_aggregate_sharded("s", &query, &avg)
            .unwrap()
            .expect("sharded AVG must serve via the sum+count rewrite");
        assert_eq!(
            avg_rows[0].metrics,
            vec![Value::Float(30.5), Value::Int(n)],
            "avg collapses, neighbours keep their slots"
        );
        // Distinct counts still decline (no mergeable partial).
        let distinct = AggRequest {
            group_by: None,
            metrics: vec![AggMetric {
                func: AggMetricFunc::CountDistinct,
                column: Some("region".into()),
            }],
        };
        assert!(na
            .search_aggregate_sharded("s", &query, &distinct)
            .unwrap()
            .is_none());

        // Sharded sorted top-k: per-shard fast-field top-k merged by the
        // sort column — the ids with the largest `units` in exact order.
        let sort = skaidb_fts::SortSpec {
            column: "units".into(),
            descending: true,
        };
        let sorted = na
            .search_sorted_sharded("s", &query, &sort, 5, &None, &[])
            .unwrap()
            .expect("sharded sorted top-k must serve");
        let got_units: Vec<i64> = sorted
            .iter()
            .map(|(_, doc)| match doc.get_path("units") {
                Some(Value::Int(u)) => *u,
                other => panic!("bad units {other:?}"),
            })
            .collect();
        assert_eq!(got_units, vec![60, 59, 58, 57, 56]);
        // A residual filter declines (filters do not travel).
        let residual = Some(skaidb_sql::ast::Expr::Literal(Value::Bool(true)));
        assert!(na
            .search_sorted_sharded("s", &query, &sort, 5, &residual, &[])
            .unwrap()
            .is_none());

        // Per-hit explain routes to a replica of the key — every row
        // explains from every coordinator, wherever it lives.
        let filter = Some(skaidb_sql::ast::Expr::Func {
            name: "match".into(),
            args: vec![
                skaidb_sql::ast::Expr::Column("product".into()),
                skaidb_sql::ast::Expr::Literal(Value::String("widget".into())),
            ],
        });
        for coord in [&na, &nb, &nc] {
            for id in [1i64, 17, 42, 60] {
                let text = coord
                    .search_explain("s", &filter, &Value::Int(id))
                    .unwrap()
                    .unwrap_or_else(|| panic!("row {id} must explain"));
                assert!(text.contains("TermQuery"), "row {id}: {text}");
            }
            // And the SQL spelling routes the same way.
            let rs = rows(
                coord
                    .execute(
                        "EXPLAIN SCORE SELECT id FROM s WHERE MATCH(product, 'widget') FOR 42",
                    )
                    .unwrap(),
            );
            assert_eq!(rs.columns, vec!["explanation"]);
            assert_eq!(rs.rows.len(), 1, "row 42 must explain over SQL");
        }

        // End-to-end over SQL from every coordinator: the sharded path and
        // the fallback must be indistinguishable.
        for coord in [&na, &nb, &nc] {
            let rs = rows(
                coord
                    .execute(
                        "SELECT region, COUNT(*) FROM s \
                         WHERE MATCH(product, 'widget') GROUP BY region",
                    )
                    .unwrap(),
            );
            let mut got = rs.rows;
            got.sort_by_key(|r| r[0].encode_key());
            assert_eq!(
                got,
                vec![
                    vec![Value::String("east".into()), Value::Int(30)],
                    vec![Value::String("west".into()), Value::Int(30)],
                ]
            );
            let rs = rows(
                coord
                    .execute(
                        "SELECT COUNT(*), SUM(units), MIN(units), MAX(units), AVG(units) FROM s \
                         WHERE MATCH(product, 'widget')",
                    )
                    .unwrap(),
            );
            assert_eq!(
                rs.rows,
                vec![vec![
                    Value::Int(60),
                    Value::Int(1830),
                    Value::Int(1),
                    Value::Int(60),
                    Value::Float(30.5),
                ]]
            );
        }

        let _keep = nc; // all three stayed live for the assertions above
    }

    /// A dead member forces the sharded path to decline — its key-space
    /// would go uncounted — while the SQL answer stays exact through the
    /// row fallback (rf=2 keeps every key on a surviving replica).
    #[test]
    fn sharded_search_aggregate_declines_with_dead_member() {
        use skaidb_fts::{AggGroupBy, AggMetric, AggMetricFunc, AggRequest};
        let rf2 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
            replication_factor: 2,
            ..rf1(id, addr, members)
        };
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b), member("c", &c)];
        // c is in the ring but its listener never starts (kill -9 style).
        let na = Node::new(Database::open(temp_dir("saggd-a")).unwrap(), rf2("a", &a, &members));
        let nb = Node::new(Database::open(temp_dir("saggd-b")).unwrap(), rf2("b", &b, &members));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        na.execute("CREATE TABLE s (PRIMARY KEY (id))").unwrap();
        na.execute(
            "CREATE SEARCH INDEX s_fts ON s (product, region) WITH (region.type = 'keyword')",
        )
        .unwrap();
        for i in 1..=30 {
            let region = if i % 2 == 0 { "east" } else { "west" };
            na.execute(&format!(
                "INSERT INTO s (id, product, region) VALUES ({i}, 'widget {i}', '{region}')"
            ))
            .unwrap();
        }

        let query = skaidb_fts::SearchQuery::Match {
            field: Some("product".into()),
            text: "widget".into(),
        };
        let grouped = AggRequest {
            group_by: Some(AggGroupBy::Keyword("region".into())),
            metrics: vec![AggMetric {
                func: AggMetricFunc::Count,
                column: None,
            }],
        };
        assert!(
            na.search_aggregate_sharded("s", &query, &grouped)
                .unwrap()
                .is_none(),
            "a silent member must force the fallback"
        );
        // The fallback still answers exactly: every key has a live replica.
        let rs = rows(
            na.execute(
                "SELECT region, COUNT(*) FROM s WHERE MATCH(product, 'widget') GROUP BY region",
            )
            .unwrap(),
        );
        let mut got = rs.rows;
        got.sort_by_key(|r| r[0].encode_key());
        assert_eq!(
            got,
            vec![
                vec![Value::String("east".into()), Value::Int(15)],
                vec![Value::String("west".into()), Value::Int(15)],
            ]
        );
    }

    #[test]
    fn search_index_follows_resharding_join() {
        // rf=1: after c joins, the keys it now owns exist ONLY on c — a
        // complete search result proves c's index was populated by the
        // migration apply path (schema sync + rebalance push).
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let ab = vec![member("a", &a), member("b", &b)];
        let abc = vec![member("a", &a), member("b", &b), member("c", &c)];

        let na = Node::new(Database::open(temp_dir("fjr-a")).unwrap(), rf1("a", &a, &ab));
        let nb = Node::new(Database::open(temp_dir("fjr-b")).unwrap(), rf1("b", &b, &ab));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        na.execute("CREATE SEARCH INDEX t_fts ON t (body)").unwrap();
        let n = 40;
        for i in 1..=n {
            na.execute(&format!("INSERT INTO t (id, body) VALUES ({i}, 'searchable words')"))
                .unwrap();
        }

        let nc = Node::new(Database::open(temp_dir("fjr-c")).unwrap(), rf1("c", &c, &abc));
        nc.serve_internode().unwrap();
        na.add_member("c", &c).unwrap();

        // Every row is still found through the index, from every coordinator
        // (the keys c now owns migrated to it, and its index followed).
        for coord in [&na, &nb, &nc] {
            let rs = rows(coord.execute("SELECT id FROM t WHERE MATCH(body, 'searchable')").unwrap());
            assert_eq!(rs.rows.len(), n, "complete result set after the join");
        }

        // A write after the join indexes on the new owner and is searchable
        // cluster-wide.
        nc.execute(&format!("INSERT INTO t (id, body) VALUES ({}, 'fresh searchable entry')", n + 1))
            .unwrap();
        let rs = rows(na.execute("SELECT id FROM t WHERE MATCH(body, 'fresh')").unwrap());
        assert_eq!(sorted_ids(rs), vec![n as i64 + 1]);
    }

    #[test]
    fn search_skips_unreachable_member() {
        // rf=3 over three members: every node holds every row. c is in the
        // ring but its listener never starts (a dead peer, kill -9 style);
        // the scatter must skip it and still return the full result set from
        // the reachable replicas.
        let (a, b, c) = (free_addr(), free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b), member("c", &c)];
        let na = Node::new(
            Database::open(temp_dir("fdark-a")).unwrap(),
            cfg("a", &a, &members, Consistency::One, Consistency::One),
        );
        let nb = Node::new(
            Database::open(temp_dir("fdark-b")).unwrap(),
            cfg("b", &b, &members, Consistency::One, Consistency::One),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        na.execute("CREATE SEARCH INDEX t_fts ON t (body)").unwrap();
        for i in 1..=10 {
            na.execute(&format!("INSERT INTO t (id, body) VALUES ({i}, 'resilient text')"))
                .unwrap();
        }
        for coord in [&na, &nb] {
            let rs = rows(
                coord
                    .execute(
                        "SELECT id FROM t WHERE MATCH(body, 'resilient') \
                         ORDER BY score() DESC LIMIT 20",
                    )
                    .unwrap(),
            );
            assert_eq!(rs.rows.len(), 10, "dead peer skipped, replicas answer");
        }
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
    fn limit_pushdown_scan_returns_merged_tombstone_skipping_prefix() {
        use skaidb_types::Document;
        // An unfiltered `LIMIT k` must return the k smallest-key surviving
        // rows — LWW-merged across divergent shards and skipping tombstones,
        // the same prefix a full `ORDER BY` scan yields — instead of gathering
        // and merging every shard in full (cluster_scan_limited push-down).
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
        let na = Node::new(Database::open(temp_dir("lpa")).unwrap(), cfg2("a", &a, &m));
        let nb = Node::new(Database::open(temp_dir("lpb")).unwrap(), cfg2("b", &b, &m));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();

        let key = |id: i64| Value::Array(vec![Value::Int(id)]).encode_key();
        let put = |addr: &str, id: i64, v: i64, hlc: Hlc| {
            let mut doc = Document::new();
            doc.insert("id", Value::Int(id));
            doc.insert("v", Value::Int(v));
            let r = internode::call(
                addr,
                &Request::ApplyPut {
                    table: "t".into(),
                    key: key(id),
                    value: Value::Document(doc).encode(),
                    hlc,
                },
            )
            .unwrap();
            assert!(matches!(r, Response::Ack));
        };
        // a holds 20 rows; b holds newer versions of ids 0..4 (they win under
        // LWW) plus a tombstone on id 1 (its newest write) — so id 1 must be
        // skipped, forcing the scan to seal past it to collect enough rows.
        for id in 0..20 {
            put(&a, id, id, Hlc::new(100 + id as u64, 0));
        }
        for id in 0..5 {
            put(&b, id, 1000 + id, Hlc::new(500 + id as u64, 0));
        }
        let r = internode::call(
            &b,
            &Request::ApplyDelete {
                table: "t".into(),
                key: key(1),
                hlc: Hlc::new(600, 0),
            },
        )
        .unwrap();
        assert!(matches!(r, Response::Ack));

        // Identical from either coordinator: the pushed-down LIMIT 3 equals the
        // first three rows of the authoritative full ordered scan.
        for node in [&na, &nb] {
            let full = rows(node.execute("SELECT id, v FROM t ORDER BY id").unwrap());
            assert_eq!(full.rows.len(), 19, "20 rows minus the id=1 tombstone");
            let limited = rows(node.execute("SELECT id, v FROM t LIMIT 3").unwrap());
            assert_eq!(limited.rows.len(), 3);
            assert_eq!(
                limited.rows,
                full.rows[..3].to_vec(),
                "LIMIT 3 must be the merged, tombstone-skipping prefix"
            );
            // Concretely: b's newer versions win and id 1 is gone.
            assert_eq!(limited.rows[0], vec![Value::Int(0), Value::Int(1000)]);
            assert_eq!(limited.rows[1], vec![Value::Int(2), Value::Int(1002)]);
            assert_eq!(limited.rows[2], vec![Value::Int(3), Value::Int(1003)]);
            // A limit past the end returns every survivor (all sources drained).
            let all = rows(node.execute("SELECT id FROM t LIMIT 100").unwrap());
            assert_eq!(all.rows.len(), 19);
        }
    }

    #[test]
    fn ts_hinted_handoff_replays_to_a_recovered_replica() {
        // rf=3, QUORUM: with c down the append succeeds 2/3 and c's batch
        // buffers as a hint; when c recovers, flush_hints replays it via
        // the gap-filling TsMerge.
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
        let na = Node::new(Database::open(temp_dir("tsha")).unwrap(), cfg3("a", &a, &m));
        let nb = Node::new(Database::open(temp_dir("tshb")).unwrap(), cfg3("b", &b, &m));
        let nc = Node::new(Database::open(temp_dir("tshc")).unwrap(), cfg3("c", &c, &m));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        // c is intentionally NOT served yet.

        let _ = na.execute("CREATE TIMESERIES TABLE m (SERIES KEY (h))");
        na.execute("INSERT INTO m (h, ts, value) VALUES ('x', 1000, 1), ('x', 2000, 2)")
            .unwrap();

        // c recovers; DDL converges via schema sync, samples via the hint.
        nc.serve_internode().unwrap();
        na.repair_cluster().ok(); // schema for the TS table reaches c
        let replayed = na.flush_hints();
        assert!(replayed >= 2, "expected the buffered batch to replay, got {replayed}");

        let r = internode::call(
            &c,
            &Request::TsQuery {
                table: "m".into(),
                matchers: vec![],
                t0: i64::MIN,
                t1: i64::MAX,
            },
        )
        .unwrap();
        let Response::TsSeries { series } = r else { panic!("expected TsSeries") };
        assert_eq!(series[0].1.len(), 2, "hinted samples landed on c locally");
    }

    #[test]
    fn timeseries_repair_converges_lagging_replica() {
        // rf=3: every node replicates every series. Inject a window of
        // samples into only two of the three nodes (as if the third was
        // down), then repair() from the sender and verify the lagging
        // node's LOCAL store has the gap filled — mid-series, which
        // TsAppend alone could never fix.
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
        let na = Node::new(Database::open(temp_dir("tra")).unwrap(), cfg3("a", &a, &m));
        let nb = Node::new(Database::open(temp_dir("trb")).unwrap(), cfg3("b", &b, &m));
        let nc = Node::new(Database::open(temp_dir("trc")).unwrap(), cfg3("c", &c, &m));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        nc.serve_internode().unwrap();
        na.execute("CREATE TIMESERIES TABLE m (SERIES KEY (h))").unwrap();

        let labels = vec![
            ("__field__".to_string(), "value".to_string()),
            ("h".to_string(), "x".to_string()),
        ];
        // ts=1000 everywhere; ts=2000 only on a and b; ts=3000 everywhere.
        for (addr, tss) in [
            (&a, vec![1000i64, 2000, 3000]),
            (&b, vec![1000, 2000, 3000]),
            (&c, vec![1000, 3000]),
        ] {
            let rows: Vec<_> = tss.iter().map(|&ts| (labels.clone(), ts, ts as f64)).collect();
            let r = internode::call(addr, &Request::TsAppend { table: "m".into(), rows }).unwrap();
            assert!(matches!(r, Response::Ack));
        }

        // Repair from every node (only the series' elected sender pushes).
        for node in [&na, &nb, &nc] {
            node.repair().unwrap();
        }

        // The lagging node now holds the mid-series gap locally.
        let r = internode::call(
            &c,
            &Request::TsQuery {
                table: "m".into(),
                matchers: vec![],
                t0: i64::MIN,
                t1: i64::MAX,
            },
        )
        .unwrap();
        let Response::TsSeries { series } = r else { panic!("expected TsSeries") };
        let samples = &series[0].1;
        assert_eq!(
            samples.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
            vec![1000, 2000, 3000],
            "gap filled on the lagging replica"
        );
    }

    #[test]
    fn timeseries_resharding_join_and_decommission() {
        // rf=1 so placement moves are observable: every series lives on
        // exactly one node; a join must migrate the joiner's share and a
        // decommission must drain the leaver's share.
        let one = Consistency::One;
        let cfg1 = |id: &str, addr: &str, members: &[(NodeId, String)]| NodeConfig {
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
        let m2 = vec![member("a", &a), member("b", &b)];
        let na = Node::new(Database::open(temp_dir("rsa")).unwrap(), cfg1("a", &a, &m2));
        let nb = Node::new(Database::open(temp_dir("rsb")).unwrap(), cfg1("b", &b, &m2));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        na.execute("CREATE TIMESERIES TABLE m (SERIES KEY (host))").unwrap();
        for i in 0..20 {
            na.execute(&format!(
                "INSERT INTO m (host, ts, value) VALUES ('h{i}', 1000, 1.0), ('h{i}', 2000, 2.0)"
            ))
            .unwrap();
        }

        // Join a third node (previously refused with TS tables present).
        let m3 = vec![member("a", &a), member("b", &b), member("c", &c)];
        let nc = Node::new(Database::open(temp_dir("rsc")).unwrap(), cfg1("c", &c, &m3));
        nc.serve_internode().unwrap();
        na.add_member("c", &c).unwrap();

        // All samples remain readable from every coordinator...
        for node in [&na, &nb, &nc] {
            let rs = rows(node.execute("SELECT count(value) FROM m").unwrap());
            assert_eq!(rs.rows[0][0], Value::Int(40), "full view after join");
        }
        // ...and the joiner actually received its share (local-only check).
        let r = internode::call(
            &c,
            &Request::TsQuery {
                table: "m".into(),
                matchers: vec![],
                t0: i64::MIN,
                t1: i64::MAX,
            },
        )
        .unwrap();
        let Response::TsSeries { series } = r else { panic!("expected TsSeries") };
        assert!(!series.is_empty(), "joiner should own some series");

        // Decommission b: its series drain to their new owners first.
        na.remove_member("b").unwrap();
        for node in [&na, &nc] {
            let rs = rows(node.execute("SELECT count(value) FROM m").unwrap());
            assert_eq!(rs.rows[0][0], Value::Int(40), "full view after drain");
        }
    }

    #[test]
    fn users_and_grants_replicate_across_members() {
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
        let na = Node::new(Database::open(temp_dir("aua")).unwrap(), cfg2("a", &a, &m));
        let nb = Node::new(Database::open(temp_dir("aub")).unwrap(), cfg2("b", &b, &m));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        // User DDL broadcasts (password rewritten to a verifier first).
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        na.execute("CREATE USER bob PASSWORD 'hunter2'").unwrap();
        na.execute("GRANT SELECT ON t TO bob").unwrap();

        // The peer can authenticate bob and sees his grant.
        let cred = nb.auth_user("bob").expect("user replicated");
        let candidate =
            skaidb_auth::ScramCredential::new("hunter2", &cred.salt, cred.iterations);
        assert_eq!(candidate.stored_key, cred.stored_key);
        assert!(nb.has_privilege(
            "bob",
            skaidb_auth::Privilege::Select,
            &skaidb_auth::Object::Table("t".into())
        ));
        assert!(!nb.has_privilege(
            "bob",
            skaidb_auth::Privilege::Insert,
            &skaidb_auth::Object::Table("t".into())
        ));

        // SHOW GRANTS answers from the replicated catalog through the
        // cluster session path, and per-database grants replicate.
        na.execute("GRANT INSERT ON DATABASE default TO bob").unwrap();
        let rs = rows(nb.execute("SHOW GRANTS FOR bob").unwrap());
        assert!(
            rs.rows.iter().any(|r| r[2] == Value::String("db:default".into())),
            "got: {:?}",
            rs.rows
        );
        assert!(nb.has_privilege(
            "bob",
            skaidb_auth::Privilege::Insert,
            &skaidb_auth::Object::Database("default".into())
        ));

        // Revocation and drops replicate too.
        nb.execute("REVOKE SELECT ON t FROM bob").unwrap();
        assert!(!na.has_privilege(
            "bob",
            skaidb_auth::Privilege::Select,
            &skaidb_auth::Object::Table("t".into())
        ));
        na.execute("DROP USER bob").unwrap();
        assert!(nb.auth_user("bob").is_none());
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

        // Partial-aggregate pushdown answers each series from its fullest
        // responder: the 'z' sample lives on one member only, yet grouped
        // aggregates via any coordinator see it.
        let rs = rows(
            nb.execute("SELECT sum(value), count(value) FROM cpu WHERE host = 'z'")
                .unwrap(),
        );
        assert_eq!(rs.rows, vec![vec![Value::Float(7.0), Value::Int(1)]]);
        let rs = rows(
            nc.execute(
                "SELECT time_bucket(1m, ts) AS t, host, max(value) FROM cpu \
                 GROUP BY t, host ORDER BY t, host",
            )
            .unwrap(),
        );
        assert_eq!(
            rs.rows[0],
            vec![
                Value::Timestamp(0),
                Value::String("x".into()),
                Value::Float(30.0)
            ]
        );

        // Append-only holds in cluster mode too.
        let err = nb.execute("UPDATE cpu SET value = 1 WHERE host = 'x'").unwrap_err();
        assert!(err.to_string().contains("append-only"), "{err}");

        // Topology changes work with TS tables (an unreachable joiner is
        // still an error, but not a TS refusal — migration is covered by
        // timeseries_resharding_join_and_decommission).
        let err = na.add_member("d", "127.0.0.1:1").unwrap_err();
        assert!(err.to_string().contains("unreachable"), "{err}");
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
    fn ts_reclaim_drops_unowned_series_after_join() {
        // The TS twin of reclaim_drops_unowned_keys_after_join: rf=1, series
        // move to a joining node; former owners drop whole series only after
        // the new owner confirms an identical copy; nothing is lost.
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
        let na = Node::new(Database::open(temp_dir("tsrca")).unwrap(), rf1("a", &a, &ab));
        let nb = Node::new(Database::open(temp_dir("tsrcb")).unwrap(), rf1("b", &b, &ab));
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        na.execute("CREATE TIMESERIES TABLE cpu (SERIES KEY (host))")
            .unwrap();
        let n = 40;
        for i in 1..=n {
            for t in 0..3i64 {
                na.execute(&format!(
                    "INSERT INTO cpu (host, ts, value) VALUES ('h{i}', {}, {})",
                    t * 60_000,
                    i * 10 + t
                ))
                .unwrap();
            }
        }

        let nc = Node::new(Database::open(temp_dir("tsrcc")).unwrap(), rf1("c", &c, &abc));
        nc.serve_internode().unwrap();
        na.add_member("c", &c).unwrap();

        // Former owners reclaim series that moved onto c.
        let dropped = na.reclaim().unwrap() + nb.reclaim().unwrap() + nc.reclaim().unwrap();
        assert!(dropped > 0, "some series moved to c and were reclaimed");

        // No data lost — every series still fully readable from every node.
        for coord in [&na, &nb, &nc] {
            for i in 1..=n {
                let rs = rows(
                    coord
                        .execute(&format!(
                            "SELECT value FROM cpu WHERE host = 'h{i}' ORDER BY ts"
                        ))
                        .unwrap(),
                );
                assert_eq!(rs.rows.len(), 3, "host h{i} after ts reclaim");
                assert_eq!(rs.rows[0], vec![Value::Float((i * 10) as f64)]);
            }
        }

        // Idempotent.
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

    /// A join that died between its begin and finalize broadcasts leaves
    /// the dual-placement window open on every member with nothing to
    /// close it. The joiner's re-announce (`add_member` on an existing
    /// member) must finalize the pending transition, not no-op.
    #[test]
    fn reannounce_finalizes_stuck_dual_ring() {
        let (a, b) = (free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b)];
        let na = Node::new(
            Database::open(temp_dir("dual_a")).unwrap(),
            cfg("a", &a, &members, Consistency::One, Consistency::One),
        );
        let nb = Node::new(
            Database::open(temp_dir("dual_b")).unwrap(),
            cfg("b", &b, &members, Consistency::One, Consistency::One),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        // Re-open the dual window everywhere, as a join of `b` that failed
        // after its begin broadcast would have left it.
        let prev = vec![member("a", &a)];
        let epoch = na.membership_epoch() + 1;
        assert!(na.set_membership(&members, &prev, epoch));
        assert!(nb.set_membership(&members, &prev, epoch));
        assert!(na.stats().resharding_active, "window open");

        // b re-announces; a's add_member sees an existing member with a
        // pending transition and finalizes it cluster-wide.
        na.add_member("b", &b).unwrap();
        assert!(!na.stats().resharding_active, "a finalized");
        assert!(!nb.stats().resharding_active, "b finalized");
        assert_eq!(na.stats().members, 2);
        assert!(na.membership_epoch() > epoch, "finalize bumped the epoch");
    }

    /// EXPLAIN on a cluster: engine plan rows plus appended cluster
    /// fan-out rows; nothing executes.
    #[test]
    fn cluster_explain_statement() {
        let (a, b) = (free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b)];
        // RF 1 < members 2 so non-point reads scatter.
        let na = Node::new(
            Database::open(temp_dir("expl_a")).unwrap(),
            cfg_auth("a", &a, &members, 1, Authenticator::None),
        );
        let nb = Node::new(
            Database::open(temp_dir("expl_b")).unwrap(),
            cfg_auth("b", &b, &members, 1, Authenticator::None),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        na.execute("INSERT INTO t (id, v) VALUES (1, 'x')").unwrap();

        let explain = |sql: &str| -> Vec<(String, String)> {
            let out = na
                .execute_session_with(DEFAULT_DATABASE, sql, None)
                .unwrap();
            let rs = match out {
                SessionEffect::Output(o) => rows(o),
                SessionEffect::UseDatabase(_) => unreachable!(),
            };
            assert_eq!(rs.columns, vec!["aspect", "decision"]);
            rs.rows
                .into_iter()
                .map(|r| {
                    let s = |v: &Value| match v {
                        Value::String(s) => s.clone(),
                        other => format!("{other:?}"),
                    };
                    (s(&r[0]), s(&r[1]))
                })
                .collect()
        };
        let find = |rows: &[(String, String)], aspect: &str| -> String {
            rows.iter()
                .find(|(a, _)| a == aspect)
                .map(|(_, d)| d.clone())
                .unwrap_or_default()
        };

        // Point read: engine access row + cluster routing rows.
        let r = explain("EXPLAIN SELECT * FROM t WHERE id = 1");
        assert!(find(&r, "access").contains("point read"));
        assert_eq!(find(&r, "cluster.members"), "2");
        assert!(find(&r, "cluster.fan_out").contains("replica set"));
        // Scatter (RF 1 < members 2, no PK equality).
        let r = explain("EXPLAIN SELECT * FROM t WHERE v = 'x'");
        assert!(find(&r, "cluster.fan_out").contains("scatter"));
        // DML explains without executing.
        let r = explain("EXPLAIN DELETE FROM t WHERE id = 1");
        assert!(find(&r, "cluster.fan_out").contains("write consistency"));
        let rs = rows(na.execute("SELECT id FROM t").unwrap());
        assert_eq!(rs.rows.len(), 1, "EXPLAIN DELETE must not delete");
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

    /// Hint records survive an encode/decode round-trip, including a
    /// truncated trailing record (torn write) which is skipped.
    #[test]
    fn hint_record_round_trip() {
        let recs = vec![
            ("t1".to_string(), b"k1".to_vec(), WriteOp::Put(b"val1".to_vec()), Hlc::new(10, 1)),
            ("t2".to_string(), b"key-2".to_vec(), WriteOp::Delete, Hlc::new(20, 0)),
        ];
        let mut buf = Vec::new();
        for (t, k, op, h) in &recs {
            buf.extend_from_slice(&encode_hint_record(t, k, op, *h));
        }
        assert_eq!(decode_hint_records(&buf), recs);
        // A torn trailing record is ignored; the whole records survive.
        buf.extend_from_slice(&[9, 0, 0, 0, b'x']); // partial next record
        assert_eq!(decode_hint_records(&buf), recs);
    }

    /// Hints past the in-memory cap spill to a per-replica on-disk log
    /// (durable, bounded memory) instead of being dropped — so a
    /// persistently-behind replica loses no writes.
    #[test]
    fn hints_spill_to_disk_beyond_cap() {
        let (a, b) = (free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b)];
        let na = Node::new(
            Database::open(temp_dir("spill_a")).unwrap(),
            cfg("a", &a, &members, Consistency::One, Consistency::One),
        );
        let bid = NodeId::new("b");
        // MAX_HINTS_PER_REPLICA is 4 in tests; store 10 → 4 in memory, 6 spilled.
        for i in 0..10u32 {
            na.store_hint(&bid, "t", &i.to_le_bytes(), &WriteOp::Put(vec![i as u8]), Hlc::new(100 + i as u64, 0));
        }
        assert_eq!(na.disk_hints.load(Ordering::Relaxed), 6, "6 spilled to disk");
        // The on-disk log decodes to exactly the 6 overflow records.
        let path = na.hint_log_path(&bid).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let recs = decode_hint_records(&bytes);
        assert_eq!(recs.len(), 6);
        assert_eq!(recs[0].1, 4u32.to_le_bytes().to_vec()); // 5th write (index 4)
        assert_eq!(recs[5].1, 9u32.to_le_bytes().to_vec());
        // hints_pending() sees the disk hints (so a flush gets queued).
        assert!(na.hints_pending());
        assert_eq!(na.stats().hints_pending, 10); // 4 memory + 6 disk

        // Restart-inheritance: a NEW process over the same data dir must
        // still see the spilled log — the counter is process-local, and a
        // restarted node whose writes all succeed otherwise never queues a
        // flush (a 1.8 GB inherited log sat undrained in production because
        // this gate never fired).
        let dir = na.data_dir().unwrap();
        drop(na);
        let nb = Node::new(
            Database::open(dir).unwrap(),
            cfg("a", &a, &members, Consistency::One, Consistency::One),
        );
        assert!(
            nb.hints_pending(),
            "restarted node must notice inherited disk hint logs"
        );
    }

    /// The disk-hint drain must not touch the log while the peer is down
    /// (the old drain re-read + rewrote the whole log after every write
    /// batch), and must deliver a large log in bounded pages once the peer
    /// is reachable.
    #[test]
    fn disk_hint_drain_skips_down_peer_and_streams_to_live_peer() {
        let (a, b) = (free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b)];
        let na = Node::new(
            Database::open(temp_dir("drain_a")).unwrap(),
            cfg("a", &a, &members, Consistency::One, Consistency::One),
        );
        let nb = Node::new(
            Database::open(temp_dir("drain_b")).unwrap(),
            cfg("b", &b, &members, Consistency::One, Consistency::One),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();

        // Down peer: spill hints for an unreachable node, then drain — the
        // probe fails and the log must be left byte-identical (no churn).
        let cid = NodeId::new("c");
        let dead_addr = free_addr(); // nothing listening there
        for i in 0..8u32 {
            na.store_hint(
                &cid,
                "t",
                &i.to_le_bytes(),
                &WriteOp::Put(vec![i as u8]),
                Hlc::new(50 + u64::from(i), 0),
            );
        }
        let c_log = na.hint_log_path(&cid).unwrap();
        let before = std::fs::read(&c_log).unwrap();
        assert!(!before.is_empty());
        assert_eq!(na.drain_disk_hints(&cid, &dead_addr), 0);
        assert_eq!(std::fs::read(&c_log).unwrap(), before, "log untouched");

        // Live peer: spill far more than one drain page (1024 records) so the
        // streamed path pages more than once, then drain — everything lands
        // on b and the log is gone.
        let bid = NodeId::new("b");
        let n = 2500u32;
        for i in 0..n {
            let mut doc = skaidb_types::Document::new();
            doc.insert("id", Value::Int(i64::from(i)));
            na.store_hint(
                &bid,
                "t",
                &Value::Array(vec![Value::Int(i64::from(i))]).encode_key(),
                &WriteOp::Put(Value::Document(doc).encode()),
                Hlc::new(100 + u64::from(i), 0),
            );
        }
        let b_log = na.hint_log_path(&bid).unwrap();
        assert!(b_log.exists());
        // In-memory cap (4 under test) holds the first few; the rest hit disk.
        let delivered = na.drain_disk_hints(&bid, &b);
        assert_eq!(delivered as u32, n - 4, "all spilled records delivered");
        assert!(!b_log.exists(), "drained log removed");
        let rs = rows(nb.execute("SELECT count(*) FROM t").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(i64::from(n - 4))]]);
    }

    /// A broad non-indexed filter (many matching rows) resolves through one
    /// merged scan instead of a quorum point read per candidate key — and
    /// stays exact: count and rows match, non-matching rows are excluded.
    #[test]
    fn broad_filter_resolves_via_merged_scan_exactly() {
        let q = Consistency::Quorum;
        let (a, b) = (free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b)];
        let na = Node::new(
            Database::open(temp_dir("bfa")).unwrap(),
            cfg("a", &a, &members, q, q),
        );
        let nb = Node::new(
            Database::open(temp_dir("bfb")).unwrap(),
            cfg("b", &b, &members, q, q),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        // 300 matching rows (> the 256 point-read cutoff) + 40 non-matching.
        for i in 0..340 {
            let grp = if i < 300 { "hot" } else { "cold" };
            na.execute(&format!("INSERT INTO t (id, grp) VALUES ({i}, '{grp}')"))
                .unwrap();
        }
        for node in [&na, &nb] {
            let rs = rows(
                node.execute("SELECT count(*) FROM t WHERE grp = 'hot'")
                    .unwrap(),
            );
            assert_eq!(rs.rows, vec![vec![Value::Int(300)]]);
            let rs = rows(
                node.execute("SELECT id FROM t WHERE grp = 'cold' ORDER BY id LIMIT 5")
                    .unwrap(),
            );
            assert_eq!(rs.rows.len(), 5);
            assert_eq!(rs.rows[0], vec![Value::Int(300)]);
        }
    }

    /// Under memory pressure the node sheds *writes* (with a retryable
    /// error) so it can drain and survive instead of being OOM-killed —
    /// while reads and DDL keep working. Recovery re-enables writes.
    #[test]
    fn memory_pressure_sheds_writes_not_reads() {
        let (a, b) = (free_addr(), free_addr());
        let members = vec![member("a", &a), member("b", &b)];
        let na = Node::new(
            Database::open(temp_dir("shed_a")).unwrap(),
            cfg("a", &a, &members, Consistency::One, Consistency::One),
        );
        let nb = Node::new(
            Database::open(temp_dir("shed_b")).unwrap(),
            cfg("b", &b, &members, Consistency::One, Consistency::One),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();
        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        na.execute("INSERT INTO t (id) VALUES (1)").unwrap();

        // Enter memory pressure: writes are rejected with a retryable error.
        na.set_shedding_for_test(true);
        let err = na.execute("INSERT INTO t (id) VALUES (2)").unwrap_err();
        assert!(err.to_string().contains("memory pressure"), "got: {err}");
        assert!(na.stats().shedding_writes);
        // Reads are never shed.
        let rs = rows(na.execute("SELECT count(*) FROM t").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(1)]]);

        // Recovered: writes resume.
        na.set_shedding_for_test(false);
        na.execute("INSERT INTO t (id) VALUES (2)").unwrap();
        let rs = rows(na.execute("SELECT count(*) FROM t").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::Int(2)]]);
    }

    /// A replica that is *up but unresponsive* — it accepts the TCP
    /// connection but never answers (a node thrashing under memory pressure,
    /// a kernel that accepted the socket while the process is stalled) — must
    /// not hang a quorum write. Distinct from a *refused* connection (the
    /// test above), which fails fast on connect; this one connects, so
    /// without a socket read timeout the coordinator's `recv` would block on
    /// it forever. The write must still succeed via the two live replicas and
    /// hint the black hole. Bounded by `transport::IO_TIMEOUT` (1s in tests).
    #[test]
    fn quorum_write_survives_unresponsive_replica() {
        let (a, b, blackhole) = (free_addr(), free_addr(), free_addr());
        // Black hole: accept connections and hold them open, never replying.
        let listener = TcpListener::bind(&blackhole).unwrap();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            for stream in listener.incoming().flatten() {
                held.push(stream); // keep the socket open; never read/respond
            }
        });

        let members = vec![member("a", &a), member("b", &b), member("bh", &blackhole)];
        let na = Node::new(
            Database::open(temp_dir("bh_a")).unwrap(),
            cfg("a", &a, &members, Consistency::Quorum, Consistency::Quorum),
        );
        let nb = Node::new(
            Database::open(temp_dir("bh_b")).unwrap(),
            cfg("b", &b, &members, Consistency::Quorum, Consistency::Quorum),
        );
        na.serve_internode().unwrap();
        nb.serve_internode().unwrap();

        na.execute("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        // The write must return Ok (quorum: a + b), and must not hang on the
        // black hole — bounded by the socket read timeout.
        let start = std::time::Instant::now();
        na.execute("INSERT INTO t (id, v) VALUES (1, 'x')").unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(8),
            "write hung on the unresponsive replica: {elapsed:?}"
        );
        // The two live replicas hold the data.
        let rs = rows(nb.execute("SELECT v FROM t WHERE id = 1").unwrap());
        assert_eq!(rs.rows, vec![vec![Value::String("x".into())]]);
    }
}
