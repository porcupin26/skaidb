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

use std::collections::{BTreeMap, HashMap};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;

use skaidb_engine::{filter_rows, run, Cluster, Database, EngineError, QueryOutput};
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

/// A cluster member: owns local storage and coordinates cluster operations.
#[derive(Debug)]
pub struct Node {
    id: NodeId,
    /// Local storage behind an `RwLock` so concurrent reads share a read lock
    /// while writes are exclusive.
    local: RwLock<Database>,
    ring: Ring,
    /// Peer id → internode address (excludes self).
    peers: HashMap<NodeId, String>,
    clock: HlcClock,
    /// Pooled persistent connections to peers.
    pool: internode::Pool,
    cfg: NodeConfig,
}

impl Node {
    /// Create a node with the given local database and cluster config.
    pub fn new(local: Database, cfg: NodeConfig) -> Arc<Node> {
        let mut ring = Ring::new(cfg.vnodes_per_node);
        for (id, _) in &cfg.members {
            ring.add_node(id.clone());
        }
        let peers = cfg
            .members
            .iter()
            .filter(|(id, _)| *id != cfg.id)
            .map(|(id, addr)| (id.clone(), addr.clone()))
            .collect();

        Arc::new(Node {
            id: cfg.id.clone(),
            local: RwLock::new(local),
            ring,
            peers,
            clock: HlcClock::new(),
            pool: internode::Pool::new(),
            cfg,
        })
    }

    /// Total member count (peers + self).
    fn member_count(&self) -> usize {
        self.peers.len() + 1
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
        for addr in self.peers.values() {
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
        let replicas = self.ring.replicas_for(key, self.cfg.replication_factor);
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
            let Some(addr) = self.peers.get(replica) else {
                continue;
            };
            // Acks we will have once the in-flight local fsync lands.
            let projected = acks + usize::from(local_will_ack);
            if projected >= needed {
                async_peers.push(addr.clone());
            } else if matches!(self.send_write(addr, table, key, &op, hlc), Ok(true)) {
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
        let replicas = self.ring.replicas_for(key, self.cfg.replication_factor);
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
            } else if let Some(addr) = self.peers.get(replica) {
                match self.pool.call(
                    addr,
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
        for addr in self.peers.values() {
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

fn is_ddl(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateTable(_)
            | Statement::DropTable { .. }
            | Statement::CreateIndex(_)
            | Statement::DropIndex { .. }
    )
}

/// The networked [`Cluster`] implementation driving `run()` on a coordinator.
struct Coordinator {
    node: Arc<Node>,
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
