//! Internode RPC for replication and distributed reads (SPEC §4–6).
//!
//! Members talk to each other over the same length-prefixed framing as the
//! client protocol. A coordinator replicates row writes (`ApplyPut`/
//! `ApplyDelete`, or `ApplyBatch` for many rows at once) to a key's replica
//! set, scatters `LocalScan` to gather a table for a read, and broadcasts
//! `ApplyDdl`.

use std::io::{self, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use crate::transport::{Authenticator, Conn};
use skaidb_proto::{read_frame, write_frame};
use skaidb_sql::ast::{BinaryOp, Expr, UnaryOp};
use skaidb_storage::compress::{compress, decompress, Codec};
use skaidb_storage::Hlc;
use skaidb_types::Value;

/// A request from a coordinator to a peer member.
#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    ApplyPut {
        table: String,
        key: Vec<u8>,
        value: Vec<u8>,
        hlc: Hlc,
    },
    ApplyDelete {
        table: String,
        key: Vec<u8>,
        hlc: Hlc,
    },
    /// Apply a batch of row writes to one table in a single round-trip: the
    /// receiver applies every row under one lock acquisition and issues one
    /// WAL fsync for the whole batch (versus one per `ApplyPut`/`ApplyDelete`).
    /// Rows are `(key, value, hlc, is_put)` — the [`Response::Scan`] row shape;
    /// `is_put == false` marks a tombstone (delete, empty value). Used by
    /// migration, drain, hint replay and anti-entropy. Acked with
    /// [`Response::Ack`] only if every row applied.
    ApplyBatch {
        table: String,
        rows: Vec<(Vec<u8>, Vec<u8>, Hlc, bool)>,
    },
    LocalScan {
        table: String,
    },
    /// One page of a node's versioned shard: up to `limit` rows (tombstones
    /// included) with key strictly greater than `after`, in key order.
    /// Answered with [`Response::Scan`]; fewer than `limit` rows marks the
    /// final page. Anti-entropy pages through these instead of pulling a
    /// whole table in one [`Request::LocalScan`], so repairing a large table
    /// costs bounded memory on both ends.
    ScanPage {
        table: String,
        after: Option<Vec<u8>>,
        limit: u32,
    },
    /// Append time-series samples to the peer's local store (TS replication).
    /// Rows are `(sorted labels, ts ms, value)`; replays are harmless (an
    /// already-present timestamp is rejected per sample, not per batch).
    TsAppend {
        table: String,
        rows: Vec<TsRow>,
    },
    /// The peer's local samples for series matching every matcher in
    /// `[t0, t1]`. Matchers are `(negated, label, value)` equality forms.
    /// Answered with [`Response::TsSeries`].
    TsQuery {
        table: String,
        matchers: Vec<(bool, String, String)>,
        t0: i64,
        t1: i64,
    },
    /// Repair-path merge: apply samples of any age to the peer's local
    /// store (fills mid-series gaps that `TsAppend` would reject).
    TsMerge {
        table: String,
        rows: Vec<TsRow>,
    },
    /// The peer's per-series anti-entropy summaries for `table`. Answered
    /// with [`Response::TsSummaries`].
    TsSummary {
        table: String,
    },
    /// The peer's per-series per-bucket partial aggregates for series
    /// matching every matcher in `[t0, t1]` (`bucket <= 0` = one whole-range
    /// bucket) — the partial-aggregate pushdown that ships bucket summaries
    /// instead of raw samples. Answered with [`Response::TsPartials`].
    TsPartials {
        table: String,
        matchers: Vec<(bool, String, String)>,
        t0: i64,
        t1: i64,
        bucket: i64,
    },
    /// Like [`Request::LocalScan`] but pushes a `WHERE` filter to the node: it
    /// returns only rows matching `filter` (plus all tombstones, for LWW), so a
    /// non-indexed scan ships far less than the whole shard.
    FilteredScan {
        table: String,
        filter: Expr,
    },
    LocalGet {
        table: String,
        key: Vec<u8>,
    },
    /// Scan the named local secondary index over a byte range and return the
    /// candidate row keys (a superset; the coordinator re-reads each).
    IndexScan {
        index: String,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    },
    /// Approximate `k` nearest keys to `query` from the node's local vector
    /// index (one shard). The coordinator merges these across nodes.
    VectorSearch {
        index: String,
        query: Vec<f32>,
        k: u32,
    },
    /// Top-k full-text hits from the node's local search index (one shard).
    /// `query` is the serde_json-encoded `skaidb_fts::SearchQuery`
    /// (self-describing, so the predicate model can grow without a wire
    /// change); `k == 0` is the unranked path (every matching key). The
    /// coordinator merges per-shard hits and re-reads survivors.
    Search {
        table: String,
        query: String,
        k: u32,
    },
    /// Sharded-aggregation shard: run `agg` (serde_json `AggRequest`) over
    /// the responder's local index restricted to **its own primary-owned
    /// key-space** derived from its ring at `epoch`. The responder declines
    /// (`SearchAggRows { rows: None }`) when its epoch differs, a reshard
    /// is active, or its index cannot serve the shape exactly — the
    /// coordinator then falls back to the row gather.
    SearchAgg {
        table: String,
        query: String,
        agg: String,
        epoch: u64,
    },
    /// Sharded sorted-top-k shard: fast-field-ordered top `k` over the
    /// responder's primary-owned key-space at `epoch`, rows fully resolved
    /// (residual filters do not travel — the coordinator declines the
    /// scatter when one is present). `sort` is a serde_json `SortSpec`;
    /// `highlights` are `(column, max_chars)` pairs resolved per shard.
    SearchSorted {
        table: String,
        query: String,
        sort: String,
        k: u32,
        highlights: Vec<(String, u32)>,
        epoch: u64,
    },
    /// Per-hit score explain routed to a replica of the key: `query` is a
    /// serde_json `SearchQuery`, `pk` a `Value::encode`d primary-key value.
    SearchExplain {
        table: String,
        query: String,
        pk: Vec<u8>,
    },
    ApplyDdl {
        /// The coordinator's current database, so table/index names in `sql`
        /// resolve to the same internal namespace on every node.
        db: String,
        sql: String,
        /// DDL version stamp, so every node records the same schema version and
        /// drops/creates converge under last-writer-wins.
        hlc: Hlc,
    },
    /// Replace the recipient's cluster membership/ring with `members`
    /// (`(id, addr)` pairs, including the recipient) at version `epoch`. Broadcast
    /// when a node joins or leaves. The recipient applies it only if `epoch` is
    /// newer than the one it holds, so stale updates and concurrent topology
    /// changes can't move a node's ring backward.
    SetMembership {
        epoch: u64,
        members: Vec<(String, String)>,
        /// The pre-change ring during an in-progress membership change (empty for
        /// a settled/finalizing update). While set, recipients union it in for
        /// placement so migrating keys dual-write/read.
        prev_members: Vec<(String, String)>,
    },
    /// Push every locally-held row whose key the named `joiner` now owns (under
    /// the current ring) to that joiner, preserving each row's HLC. Sent to
    /// existing members after a [`Request::SetMembership`] so the new node
    /// receives its share of the keyspace.
    Rebalance {
        joiner: String,
    },
    /// Drain this (leaving) node: push every locally-held row to its new owners
    /// under the post-removal ring described by `members` (which excludes the
    /// leaving node), so no key loses a replica when the node departs. Sent to a
    /// node being gracefully decommissioned, before it is removed from the ring.
    Drain {
        members: Vec<(String, String)>,
    },
    /// Reclaim local disk space: physically drop every locally-held key this node
    /// no longer owns under the current ring, after confirming an owner holds it.
    /// A post-resharding "cleanup" trigger.
    Reclaim,
    /// Run an anti-entropy pass: reconcile this node's replicas with its peers,
    /// copying the newer version of each key in both directions.
    Repair,
    /// Ask the peer for its full schema as idempotent `CREATE ... IF NOT EXISTS`
    /// statements, so a (re)joining node can converge its catalog.
    SchemaDdl,
    Ping,
    /// A node announcing itself to a seed so the cluster admits it (auto-join).
    /// The receiver runs `add_member` and broadcasts the new membership to every
    /// node, then rebalances. `rf` lets the seed reject a replication-factor
    /// mismatch rather than form a split-replica cluster.
    Announce {
        id: String,
        addr: String,
        rf: u32,
    },
    /// Ask a peer for a snapshot of its own membership view and data status, for
    /// cross-node diagnostics (so `\cluster` can flag nodes that disagree).
    NodeStatus,
    /// Ask a peer for its host system statistics (CPU, memory, disk IO,
    /// disk space) for the UI's per-node view.
    HostStats,
}

/// A response from a peer member.
#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    Ack,
    /// A versioned table shard: `(key, value, hlc, is_put)`. `is_put == false`
    /// marks a tombstone (empty value), so the coordinator can resolve deletes
    /// across replicas by last-writer-wins.
    Scan {
        rows: Vec<(Vec<u8>, Vec<u8>, Hlc, bool)>,
    },
    /// Point-read result: `(value, stamp, is_put)`, or `None` if absent here.
    Get {
        entry: Option<(Vec<u8>, Hlc, bool)>,
    },
    /// Candidate row keys from an [`Request::IndexScan`].
    Keys {
        keys: Vec<Vec<u8>>,
    },
    /// `(key, distance)` hits from a [`Request::VectorSearch`], nearest-first.
    VectorHits {
        hits: Vec<(Vec<u8>, f32)>,
    },
    /// Per-shard aggregation rows from a [`Request::SearchAgg`], encoded
    /// with [`encode_agg_rows`]; `None` = declined, fall back.
    SearchAggRows { rows: Option<Vec<u8>> },
    /// Per-shard resolved rows from a [`Request::SearchSorted`], encoded
    /// with [`encode_sorted_rows`]; `None` = declined, fall back.
    SortedRows { rows: Option<Vec<u8>> },
    /// The BM25 breakdown from a [`Request::SearchExplain`]; `None` = the
    /// key does not match the query (or is absent) on the answering
    /// replica.
    Explanation { text: Option<String> },
    /// `(key, BM25 score)` hits from a [`Request::Search`], best-first
    /// (scores 0.0 on the unranked path).
    SearchHits {
        hits: Vec<(Vec<u8>, f32)>,
    },
    /// A node's versioned schema as `(database, ddl, hlc)` triples (reply to
    /// `SchemaDdl`): live objects as CREATEs and dropped ones as DROPs, each
    /// with its version stamp for last-writer-wins merge.
    Schema {
        entries: Vec<(String, String, Hlc)>,
    },
    Err(String),
    Pong,
    /// Reply to [`Request::NodeStatus`]: the peer's membership epoch, its member
    /// ids (its own view of the ring), a row count, and its HLC frontier (ms).
    NodeStatus {
        epoch: u64,
        members: Vec<String>,
        rows: u64,
        hlc_ms: u64,
    },
    /// Reply to [`Request::HostStats`]: the peer's host statistics as
    /// serde_json-encoded [`crate::host::HostStats`] (self-describing, so
    /// fields can grow without a wire change).
    HostStats { json: String },
    /// Reply to [`Request::TsQuery`]: this node's matching series and samples.
    TsSeries {
        series: TsSeriesData,
    },
    /// Reply to [`Request::TsSummary`]: `(labels, deduped count, checksum)`.
    TsSummaries {
        series: Vec<TsSummaryRow>,
    },
    /// Reply to [`Request::TsPartials`]: this node's matching series with
    /// their per-bucket partial aggregates.
    TsPartials {
        series: TsPartialsData,
    },
}

/// A versioned row on the wire: `(key, value, hlc, is_put)` — the shape shared
/// by [`Response::Scan`] and [`Request::ApplyBatch`].
type WireRow = (Vec<u8>, Vec<u8>, Hlc, bool);

/// A time-series sample on the wire: `(sorted labels, ts ms, value)`.
pub type TsRow = (Vec<(String, String)>, i64, f64);
/// Per-series samples on the wire: `(labels, [(ts, value)])`.
pub type TsSeriesData = Vec<(Vec<(String, String)>, Vec<(i64, f64)>)>;
/// One anti-entropy summary row: `(labels, deduped count, checksum)`.
pub type TsSummaryRow = (Vec<(String, String)>, u64, u64);
/// One bucket partial on the wire: `(bucket_ts, count, sum, min, max,
/// first_ts, first_val, last_ts, last_val, increase)` — the
/// `skaidb_engine::TsPartial` fields in order.
pub type TsPartialRow = (i64, u64, f64, f64, f64, i64, f64, i64, f64, f64);
/// Per-series partials on the wire: `(labels, [partial])`.
pub type TsPartialsData = Vec<(Vec<(String, String)>, Vec<TsPartialRow>)>;

/// Errors decoding an internode message.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WireError {
    #[error("malformed internode message: {0}")]
    Malformed(&'static str),
}

const REQ_PUT: u8 = 1;
const REQ_DEL: u8 = 2;
const REQ_SCAN: u8 = 3;
const REQ_DDL: u8 = 4;
const REQ_PING: u8 = 5;
const REQ_GET: u8 = 6;
const REQ_INDEX: u8 = 7;
const REQ_VECTOR: u8 = 8;
const REQ_MEMBERS: u8 = 9;
const REQ_REBAL: u8 = 10;
const REQ_DRAIN: u8 = 11;
const REQ_RECLAIM: u8 = 12;
const REQ_REPAIR: u8 = 13;
const REQ_FSCAN: u8 = 14;
const REQ_SCHEMA: u8 = 15;
const REQ_ANNOUNCE: u8 = 16;
const REQ_NODESTATUS: u8 = 17;
const REQ_BATCH: u8 = 18;
const REQ_SCANPAGE: u8 = 19;
const REQ_TSAPPEND: u8 = 20;
const REQ_TSQUERY: u8 = 21;
const REQ_TSMERGE: u8 = 22;
const REQ_TSSUMMARY: u8 = 23;
const REQ_TSPARTIALS: u8 = 24;
const REQ_SEARCH: u8 = 25;
const REQ_SEARCHAGG: u8 = 26;
const REQ_SEARCHSORTED: u8 = 27;
const REQ_SEARCHEXPLAIN: u8 = 28;
const REQ_HOSTSTATS: u8 = 29;

const RES_ACK: u8 = 0;
const RES_SCAN: u8 = 1;
const RES_ERR: u8 = 2;
const RES_PONG: u8 = 3;
const RES_GET: u8 = 4;
const RES_KEYS: u8 = 5;
const RES_VHITS: u8 = 6;
const RES_SCHEMA: u8 = 7;
const RES_NODESTATUS: u8 = 8;
const RES_TSSERIES: u8 = 9;
const RES_TSSUMMARIES: u8 = 10;
const RES_TSPARTIALS: u8 = 11;
const RES_SHITS: u8 = 12;
const RES_SAGG: u8 = 13;
const RES_SORTEDROWS: u8 = 14;
const RES_EXPLAIN: u8 = 15;
const RES_HOSTSTATS: u8 = 16;

impl Request {
    pub fn encode(&self) -> Vec<u8> {
        let mut o = Vec::new();
        self.encode_into(&mut o);
        o
    }

    /// Encode appending to `o`, so the RPC layer can build messages in a
    /// reused per-connection buffer instead of allocating one per call.
    pub fn encode_into(&self, o: &mut Vec<u8>) {
        match self {
            Request::ApplyPut {
                table,
                key,
                value,
                hlc,
            } => {
                o.push(REQ_PUT);
                put_str(o, table);
                put_bytes(o, key);
                put_bytes(o, value);
                o.extend_from_slice(&hlc.to_bytes());
            }
            Request::ApplyDelete { table, key, hlc } => {
                o.push(REQ_DEL);
                put_str(o, table);
                put_bytes(o, key);
                o.extend_from_slice(&hlc.to_bytes());
            }
            Request::ApplyBatch { table, rows } => {
                o.push(REQ_BATCH);
                put_str(o, table);
                put_rows(o, rows);
            }
            Request::LocalScan { table } => {
                o.push(REQ_SCAN);
                put_str(o, table);
            }
            Request::ScanPage { table, after, limit } => {
                o.push(REQ_SCANPAGE);
                put_str(o, table);
                put_opt_bytes(o, after.as_deref());
                o.extend_from_slice(&limit.to_le_bytes());
            }
            Request::TsAppend { table, rows } => {
                o.push(REQ_TSAPPEND);
                put_str(o, table);
                o.extend_from_slice(&(rows.len() as u32).to_le_bytes());
                for (labels, ts, value) in rows {
                    put_labels(o, labels);
                    o.extend_from_slice(&ts.to_le_bytes());
                    o.extend_from_slice(&value.to_bits().to_le_bytes());
                }
            }
            Request::TsQuery {
                table,
                matchers,
                t0,
                t1,
            } => {
                o.push(REQ_TSQUERY);
                put_str(o, table);
                o.extend_from_slice(&(matchers.len() as u32).to_le_bytes());
                for (negated, k, v) in matchers {
                    o.push(u8::from(*negated));
                    put_str(o, k);
                    put_str(o, v);
                }
                o.extend_from_slice(&t0.to_le_bytes());
                o.extend_from_slice(&t1.to_le_bytes());
            }
            Request::TsMerge { table, rows } => {
                o.push(REQ_TSMERGE);
                put_str(o, table);
                o.extend_from_slice(&(rows.len() as u32).to_le_bytes());
                for (labels, ts, value) in rows {
                    put_labels(o, labels);
                    o.extend_from_slice(&ts.to_le_bytes());
                    o.extend_from_slice(&value.to_bits().to_le_bytes());
                }
            }
            Request::TsSummary { table } => {
                o.push(REQ_TSSUMMARY);
                put_str(o, table);
            }
            Request::TsPartials {
                table,
                matchers,
                t0,
                t1,
                bucket,
            } => {
                o.push(REQ_TSPARTIALS);
                put_str(o, table);
                o.extend_from_slice(&(matchers.len() as u32).to_le_bytes());
                for (negated, k, v) in matchers {
                    o.push(u8::from(*negated));
                    put_str(o, k);
                    put_str(o, v);
                }
                o.extend_from_slice(&t0.to_le_bytes());
                o.extend_from_slice(&t1.to_le_bytes());
                o.extend_from_slice(&bucket.to_le_bytes());
            }
            Request::FilteredScan { table, filter } => {
                o.push(REQ_FSCAN);
                put_str(o, table);
                put_expr(o, filter);
            }
            Request::LocalGet { table, key } => {
                o.push(REQ_GET);
                put_str(o, table);
                put_bytes(o, key);
            }
            Request::IndexScan { index, start, end } => {
                o.push(REQ_INDEX);
                put_str(o, index);
                put_opt_bytes(o, start.as_deref());
                put_opt_bytes(o, end.as_deref());
            }
            Request::VectorSearch { index, query, k } => {
                o.push(REQ_VECTOR);
                put_str(o, index);
                o.extend_from_slice(&(query.len() as u32).to_le_bytes());
                for x in query {
                    o.extend_from_slice(&x.to_le_bytes());
                }
                o.extend_from_slice(&k.to_le_bytes());
            }
            Request::Search { table, query, k } => {
                o.push(REQ_SEARCH);
                put_str(o, table);
                put_str(o, query);
                o.extend_from_slice(&k.to_le_bytes());
            }
            Request::SearchAgg {
                table,
                query,
                agg,
                epoch,
            } => {
                o.push(REQ_SEARCHAGG);
                put_str(o, table);
                put_str(o, query);
                put_str(o, agg);
                o.extend_from_slice(&epoch.to_le_bytes());
            }
            Request::SearchSorted {
                table,
                query,
                sort,
                k,
                highlights,
                epoch,
            } => {
                o.push(REQ_SEARCHSORTED);
                put_str(o, table);
                put_str(o, query);
                put_str(o, sort);
                o.extend_from_slice(&k.to_le_bytes());
                o.extend_from_slice(&(highlights.len() as u32).to_le_bytes());
                for (col, max_chars) in highlights {
                    put_str(o, col);
                    o.extend_from_slice(&max_chars.to_le_bytes());
                }
                o.extend_from_slice(&epoch.to_le_bytes());
            }
            Request::SearchExplain { table, query, pk } => {
                o.push(REQ_SEARCHEXPLAIN);
                put_str(o, table);
                put_str(o, query);
                put_bytes(o, pk);
            }
            Request::ApplyDdl { db, sql, hlc } => {
                o.push(REQ_DDL);
                put_str(o, db);
                put_str(o, sql);
                o.extend_from_slice(&hlc.to_bytes());
            }
            Request::SetMembership {
                epoch,
                members,
                prev_members,
            } => {
                o.push(REQ_MEMBERS);
                o.extend_from_slice(&epoch.to_le_bytes());
                put_members(o, members);
                put_members(o, prev_members);
            }
            Request::Rebalance { joiner } => {
                o.push(REQ_REBAL);
                put_str(o, joiner);
            }
            Request::Drain { members } => {
                o.push(REQ_DRAIN);
                o.extend_from_slice(&(members.len() as u32).to_le_bytes());
                for (id, addr) in members {
                    put_str(o, id);
                    put_str(o, addr);
                }
            }
            Request::Reclaim => o.push(REQ_RECLAIM),
            Request::Repair => o.push(REQ_REPAIR),
            Request::SchemaDdl => o.push(REQ_SCHEMA),
            Request::Ping => o.push(REQ_PING),
            Request::Announce { id, addr, rf } => {
                o.push(REQ_ANNOUNCE);
                put_str(o, id);
                put_str(o, addr);
                o.extend_from_slice(&rf.to_le_bytes());
            }
            Request::NodeStatus => o.push(REQ_NODESTATUS),
            Request::HostStats => o.push(REQ_HOSTSTATS),
        }
    }

    pub fn decode(buf: &[u8]) -> Result<Request, WireError> {
        let mut c = Cur::new(buf);
        Ok(match c.u8()? {
            REQ_PUT => Request::ApplyPut {
                table: c.string()?,
                key: c.bytes()?,
                value: c.bytes()?,
                hlc: c.hlc()?,
            },
            REQ_DEL => Request::ApplyDelete {
                table: c.string()?,
                key: c.bytes()?,
                hlc: c.hlc()?,
            },
            REQ_BATCH => Request::ApplyBatch {
                table: c.string()?,
                rows: c.rows()?,
            },
            REQ_SCAN => Request::LocalScan { table: c.string()? },
            REQ_SCANPAGE => Request::ScanPage {
                table: c.string()?,
                after: c.opt_bytes()?,
                limit: c.u32()?,
            },
            REQ_TSAPPEND => {
                let table = c.string()?;
                let n = c.u32()? as usize;
                let mut rows = Vec::with_capacity(n);
                for _ in 0..n {
                    let labels = c.labels()?;
                    let ts = c.u64()? as i64;
                    let value = f64::from_bits(c.u64()?);
                    rows.push((labels, ts, value));
                }
                Request::TsAppend { table, rows }
            }
            REQ_TSQUERY => {
                let table = c.string()?;
                let n = c.u32()? as usize;
                let mut matchers = Vec::with_capacity(n);
                for _ in 0..n {
                    let negated = c.u8()? != 0;
                    let k = c.string()?;
                    let v = c.string()?;
                    matchers.push((negated, k, v));
                }
                Request::TsQuery {
                    table,
                    matchers,
                    t0: c.u64()? as i64,
                    t1: c.u64()? as i64,
                }
            }
            REQ_TSMERGE => {
                let table = c.string()?;
                let n = c.u32()? as usize;
                let mut rows = Vec::with_capacity(n);
                for _ in 0..n {
                    let labels = c.labels()?;
                    let ts = c.u64()? as i64;
                    let value = f64::from_bits(c.u64()?);
                    rows.push((labels, ts, value));
                }
                Request::TsMerge { table, rows }
            }
            REQ_TSSUMMARY => Request::TsSummary { table: c.string()? },
            REQ_TSPARTIALS => {
                let table = c.string()?;
                let n = c.u32()? as usize;
                let mut matchers = Vec::with_capacity(n);
                for _ in 0..n {
                    let negated = c.u8()? != 0;
                    let k = c.string()?;
                    let v = c.string()?;
                    matchers.push((negated, k, v));
                }
                Request::TsPartials {
                    table,
                    matchers,
                    t0: c.u64()? as i64,
                    t1: c.u64()? as i64,
                    bucket: c.u64()? as i64,
                }
            }
            REQ_FSCAN => Request::FilteredScan {
                table: c.string()?,
                filter: c.expr()?,
            },
            REQ_GET => Request::LocalGet {
                table: c.string()?,
                key: c.bytes()?,
            },
            REQ_INDEX => Request::IndexScan {
                index: c.string()?,
                start: c.opt_bytes()?,
                end: c.opt_bytes()?,
            },
            REQ_VECTOR => {
                let index = c.string()?;
                let n = c.u32()? as usize;
                let mut query = Vec::with_capacity(n);
                for _ in 0..n {
                    query.push(c.f32()?);
                }
                let k = c.u32()?;
                Request::VectorSearch { index, query, k }
            }
            REQ_SEARCH => Request::Search {
                table: c.string()?,
                query: c.string()?,
                k: c.u32()?,
            },
            REQ_SEARCHAGG => Request::SearchAgg {
                table: c.string()?,
                query: c.string()?,
                agg: c.string()?,
                epoch: c.u64()?,
            },
            REQ_SEARCHSORTED => {
                let table = c.string()?;
                let query = c.string()?;
                let sort = c.string()?;
                let k = c.u32()?;
                let n = c.u32()? as usize;
                let mut highlights = Vec::with_capacity(n.min(64));
                for _ in 0..n {
                    let col = c.string()?;
                    highlights.push((col, c.u32()?));
                }
                Request::SearchSorted {
                    table,
                    query,
                    sort,
                    k,
                    highlights,
                    epoch: c.u64()?,
                }
            }
            REQ_SEARCHEXPLAIN => Request::SearchExplain {
                table: c.string()?,
                query: c.string()?,
                pk: c.bytes()?,
            },
            REQ_DDL => Request::ApplyDdl {
                db: c.string()?,
                sql: c.string()?,
                hlc: c.hlc()?,
            },
            REQ_MEMBERS => {
                let epoch = c.u64()?;
                let members = c.members()?;
                let prev_members = c.members()?;
                Request::SetMembership {
                    epoch,
                    members,
                    prev_members,
                }
            }
            REQ_REBAL => Request::Rebalance {
                joiner: c.string()?,
            },
            REQ_DRAIN => {
                let n = c.u32()? as usize;
                let mut members = Vec::with_capacity(n);
                for _ in 0..n {
                    let id = c.string()?;
                    let addr = c.string()?;
                    members.push((id, addr));
                }
                Request::Drain { members }
            }
            REQ_RECLAIM => Request::Reclaim,
            REQ_REPAIR => Request::Repair,
            REQ_SCHEMA => Request::SchemaDdl,
            REQ_PING => Request::Ping,
            REQ_ANNOUNCE => Request::Announce {
                id: c.string()?,
                addr: c.string()?,
                rf: c.u32()?,
            },
            REQ_NODESTATUS => Request::NodeStatus,
            REQ_HOSTSTATS => Request::HostStats,
            _ => return Err(WireError::Malformed("unknown request op")),
        })
    }
}

impl Response {
    pub fn encode(&self) -> Vec<u8> {
        let mut o = Vec::new();
        self.encode_into(&mut o);
        o
    }

    /// Encode appending to `o` (see [`Request::encode_into`]).
    pub fn encode_into(&self, o: &mut Vec<u8>) {
        match self {
            Response::Ack => o.push(RES_ACK),
            Response::Scan { rows } => {
                o.push(RES_SCAN);
                put_rows(o, rows);
            }
            Response::Get { entry } => {
                o.push(RES_GET);
                match entry {
                    Some((value, hlc, is_put)) => {
                        o.push(1);
                        put_bytes(o, value);
                        o.extend_from_slice(&hlc.to_bytes());
                        o.push(u8::from(*is_put));
                    }
                    None => o.push(0),
                }
            }
            Response::Keys { keys } => {
                o.push(RES_KEYS);
                o.extend_from_slice(&(keys.len() as u32).to_le_bytes());
                for k in keys {
                    put_bytes(o, k);
                }
            }
            Response::VectorHits { hits } => {
                o.push(RES_VHITS);
                o.extend_from_slice(&(hits.len() as u32).to_le_bytes());
                for (key, dist) in hits {
                    put_bytes(o, key);
                    o.extend_from_slice(&dist.to_le_bytes());
                }
            }
            Response::SearchAggRows { rows } => {
                o.push(RES_SAGG);
                match rows {
                    Some(rows) => {
                        o.push(1);
                        put_bytes(o, rows);
                    }
                    None => o.push(0),
                }
            }
            Response::SortedRows { rows } => {
                o.push(RES_SORTEDROWS);
                match rows {
                    Some(rows) => {
                        o.push(1);
                        put_bytes(o, rows);
                    }
                    None => o.push(0),
                }
            }
            Response::Explanation { text } => {
                o.push(RES_EXPLAIN);
                match text {
                    Some(text) => {
                        o.push(1);
                        put_str(o, text);
                    }
                    None => o.push(0),
                }
            }
            Response::SearchHits { hits } => {
                o.push(RES_SHITS);
                o.extend_from_slice(&(hits.len() as u32).to_le_bytes());
                for (key, score) in hits {
                    put_bytes(o, key);
                    o.extend_from_slice(&score.to_le_bytes());
                }
            }
            Response::Schema { entries } => {
                o.push(RES_SCHEMA);
                o.extend_from_slice(&(entries.len() as u32).to_le_bytes());
                for (db, ddl, hlc) in entries {
                    put_str(o, db);
                    put_str(o, ddl);
                    o.extend_from_slice(&hlc.to_bytes());
                }
            }
            Response::Err(msg) => {
                o.push(RES_ERR);
                put_str(o, msg);
            }
            Response::Pong => o.push(RES_PONG),
            Response::HostStats { json } => {
                o.push(RES_HOSTSTATS);
                put_str(o, json);
            }
            Response::NodeStatus {
                epoch,
                members,
                rows,
                hlc_ms,
            } => {
                o.push(RES_NODESTATUS);
                o.extend_from_slice(&epoch.to_le_bytes());
                o.extend_from_slice(&(members.len() as u32).to_le_bytes());
                for m in members {
                    put_str(o, m);
                }
                o.extend_from_slice(&rows.to_le_bytes());
                o.extend_from_slice(&hlc_ms.to_le_bytes());
            }
            Response::TsSummaries { series } => {
                o.push(RES_TSSUMMARIES);
                o.extend_from_slice(&(series.len() as u32).to_le_bytes());
                for (labels, count, checksum) in series {
                    put_labels(o, labels);
                    o.extend_from_slice(&count.to_le_bytes());
                    o.extend_from_slice(&checksum.to_le_bytes());
                }
            }
            Response::TsSeries { series } => {
                o.push(RES_TSSERIES);
                o.extend_from_slice(&(series.len() as u32).to_le_bytes());
                for (labels, samples) in series {
                    put_labels(o, labels);
                    o.extend_from_slice(&(samples.len() as u32).to_le_bytes());
                    for (ts, value) in samples {
                        o.extend_from_slice(&ts.to_le_bytes());
                        o.extend_from_slice(&value.to_bits().to_le_bytes());
                    }
                }
            }
            Response::TsPartials { series } => {
                o.push(RES_TSPARTIALS);
                o.extend_from_slice(&(series.len() as u32).to_le_bytes());
                for (labels, partials) in series {
                    put_labels(o, labels);
                    o.extend_from_slice(&(partials.len() as u32).to_le_bytes());
                    for (bucket_ts, count, sum, min, max, fts, fval, lts, lval, inc) in partials {
                        o.extend_from_slice(&bucket_ts.to_le_bytes());
                        o.extend_from_slice(&count.to_le_bytes());
                        o.extend_from_slice(&sum.to_bits().to_le_bytes());
                        o.extend_from_slice(&min.to_bits().to_le_bytes());
                        o.extend_from_slice(&max.to_bits().to_le_bytes());
                        o.extend_from_slice(&fts.to_le_bytes());
                        o.extend_from_slice(&fval.to_bits().to_le_bytes());
                        o.extend_from_slice(&lts.to_le_bytes());
                        o.extend_from_slice(&lval.to_bits().to_le_bytes());
                        o.extend_from_slice(&inc.to_bits().to_le_bytes());
                    }
                }
            }
        }
    }

    pub fn decode(buf: &[u8]) -> Result<Response, WireError> {
        let mut c = Cur::new(buf);
        Ok(match c.u8()? {
            RES_ACK => Response::Ack,
            RES_SCAN => Response::Scan { rows: c.rows()? },
            RES_GET => {
                let entry = if c.u8()? == 1 {
                    let value = c.bytes()?;
                    let hlc = c.hlc()?;
                    let is_put = c.u8()? == 1;
                    Some((value, hlc, is_put))
                } else {
                    None
                };
                Response::Get { entry }
            }
            RES_KEYS => {
                let n = c.u32()? as usize;
                let mut keys = Vec::with_capacity(n);
                for _ in 0..n {
                    keys.push(c.bytes()?);
                }
                Response::Keys { keys }
            }
            RES_VHITS => {
                let n = c.u32()? as usize;
                let mut hits = Vec::with_capacity(n);
                for _ in 0..n {
                    let key = c.bytes()?;
                    hits.push((key, c.f32()?));
                }
                Response::VectorHits { hits }
            }
            RES_SHITS => {
                let n = c.u32()? as usize;
                let mut hits = Vec::with_capacity(n);
                for _ in 0..n {
                    let key = c.bytes()?;
                    hits.push((key, c.f32()?));
                }
                Response::SearchHits { hits }
            }
            RES_SAGG => {
                let rows = if c.u8()? == 1 { Some(c.bytes()?) } else { None };
                Response::SearchAggRows { rows }
            }
            RES_SORTEDROWS => {
                let rows = if c.u8()? == 1 { Some(c.bytes()?) } else { None };
                Response::SortedRows { rows }
            }
            RES_EXPLAIN => {
                let text = if c.u8()? == 1 { Some(c.string()?) } else { None };
                Response::Explanation { text }
            }
            RES_SCHEMA => {
                let n = c.u32()? as usize;
                let mut entries = Vec::with_capacity(n);
                for _ in 0..n {
                    let db = c.string()?;
                    let ddl = c.string()?;
                    let hlc = c.hlc()?;
                    entries.push((db, ddl, hlc));
                }
                Response::Schema { entries }
            }
            RES_ERR => Response::Err(c.string()?),
            RES_PONG => Response::Pong,
            RES_HOSTSTATS => Response::HostStats { json: c.string()? },
            RES_NODESTATUS => {
                let epoch = c.u64()?;
                let n = c.u32()? as usize;
                let mut members = Vec::with_capacity(n);
                for _ in 0..n {
                    members.push(c.string()?);
                }
                let rows = c.u64()?;
                let hlc_ms = c.u64()?;
                Response::NodeStatus {
                    epoch,
                    members,
                    rows,
                    hlc_ms,
                }
            }
            RES_TSSERIES => {
                let n = c.u32()? as usize;
                let mut series = Vec::with_capacity(n);
                for _ in 0..n {
                    let labels = c.labels()?;
                    let m = c.u32()? as usize;
                    let mut samples = Vec::with_capacity(m);
                    for _ in 0..m {
                        let ts = c.u64()? as i64;
                        samples.push((ts, f64::from_bits(c.u64()?)));
                    }
                    series.push((labels, samples));
                }
                Response::TsSeries { series }
            }
            RES_TSSUMMARIES => {
                let n = c.u32()? as usize;
                let mut series = Vec::with_capacity(n);
                for _ in 0..n {
                    let labels = c.labels()?;
                    let count = c.u64()?;
                    let checksum = c.u64()?;
                    series.push((labels, count, checksum));
                }
                Response::TsSummaries { series }
            }
            RES_TSPARTIALS => {
                let n = c.u32()? as usize;
                let mut series = Vec::with_capacity(n);
                for _ in 0..n {
                    let labels = c.labels()?;
                    let m = c.u32()? as usize;
                    let mut partials = Vec::with_capacity(m);
                    for _ in 0..m {
                        let bucket_ts = c.u64()? as i64;
                        let count = c.u64()?;
                        let sum = f64::from_bits(c.u64()?);
                        let min = f64::from_bits(c.u64()?);
                        let max = f64::from_bits(c.u64()?);
                        let fts = c.u64()? as i64;
                        let fval = f64::from_bits(c.u64()?);
                        let lts = c.u64()? as i64;
                        let lval = f64::from_bits(c.u64()?);
                        let inc = f64::from_bits(c.u64()?);
                        partials.push((bucket_ts, count, sum, min, max, fts, fval, lts, lval, inc));
                    }
                    series.push((labels, partials));
                }
                Response::TsPartials { series }
            }
            _ => return Err(WireError::Malformed("unknown response op")),
        })
    }
}

/// Payloads at or above this size are LZ4-compressed on the wire. Small frames
/// (acks, point writes/reads) stay raw — compression would only add overhead.
const COMPRESS_THRESHOLD: usize = 256;

/// Wrap a raw message payload in a compression envelope so the peer can tell
/// whether to decompress: `[codec u8] [u32 uncompressed_len if codec!=None] [body]`.
///
/// LZ4 is used (fast, cheap on the small cores nodes run on); a payload that
/// doesn't shrink, or is below [`COMPRESS_THRESHOLD`], is sent uncompressed.
pub(crate) fn frame_encode(payload: &[u8]) -> Vec<u8> {
    if payload.len() >= COMPRESS_THRESHOLD {
        let comp = compress(Codec::Lz4, payload);
        if comp.len() + 5 < payload.len() {
            let mut out = Vec::with_capacity(comp.len() + 5);
            out.push(Codec::Lz4.to_u8());
            out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            out.extend_from_slice(&comp);
            return out;
        }
    }
    let mut out = Vec::with_capacity(payload.len() + 1);
    out.push(Codec::None.to_u8());
    out.extend_from_slice(payload);
    out
}

/// Reverse of [`frame_encode`]: recover the raw message payload.
pub(crate) fn frame_decode(framed: &[u8]) -> Result<Vec<u8>, WireError> {
    let (&tag, rest) = framed
        .split_first()
        .ok_or(WireError::Malformed("empty frame"))?;
    match Codec::from_u8(tag) {
        Some(Codec::None) => Ok(rest.to_vec()),
        Some(codec) => {
            let len_bytes = rest
                .get(..4)
                .ok_or(WireError::Malformed("short compressed frame"))?;
            let ulen = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
            decompress(codec, &rest[4..], ulen).map_err(|_| WireError::Malformed("decompress"))
        }
        None => Err(WireError::Malformed("unknown wire codec")),
    }
}

/// Send one request to `addr` over a fresh, unauthenticated plain-TCP
/// connection. Used by tests and tooling against `auth = none` nodes; the
/// production path goes through [`Pool`], which authenticates.
pub fn call(addr: &str, req: &Request) -> io::Result<Response> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true).ok();
    write_frame(&mut stream, &frame_encode(&req.encode()))?;
    let framed = read_frame(&mut stream)?;
    let payload =
        frame_decode(&framed).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    Response::decode(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}

fn wire_io_err(e: WireError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

/// Serialize one message and write it as a single length-prefixed,
/// envelope-tagged frame, building it in the connection's reusable write
/// buffer: steady-state a message costs no allocation, no payload copy, and
/// one write. The payload is encoded directly after the frame header; only
/// when it turns out large enough to compress (and actually shrinks) is the
/// envelope rebuilt around the compressed bytes.
pub(crate) fn conn_send(conn: &mut Conn, encode: impl FnOnce(&mut Vec<u8>)) -> io::Result<()> {
    let (stream, wbuf) = conn.write_half();
    wbuf.clear();
    // 4-byte length prefix + 1-byte codec tag, patched below.
    wbuf.extend_from_slice(&[0u8; 5]);
    encode(wbuf);
    let payload_len = wbuf.len() - 5;
    let mut codec = Codec::None;
    if payload_len >= COMPRESS_THRESHOLD {
        let comp = compress(Codec::Lz4, &wbuf[5..]);
        if comp.len() + 5 < payload_len {
            wbuf.truncate(5);
            wbuf.extend_from_slice(&(payload_len as u32).to_le_bytes());
            wbuf.extend_from_slice(&comp);
            codec = Codec::Lz4;
        }
    }
    if (wbuf.len() - 4) as u64 > skaidb_proto::MAX_FRAME_LEN as u64 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "frame too large"));
    }
    let framed_len = ((wbuf.len() - 4) as u32).to_be_bytes();
    wbuf[..4].copy_from_slice(&framed_len);
    wbuf[4] = codec.to_u8();
    stream.write_all(wbuf)?;
    stream.flush()
}

/// Read one framed message into the connection's reusable read buffer and
/// decode it, borrowing the payload in place when it is uncompressed (the
/// common case for acks and point ops — no copy, no allocation).
///
/// The outer error is an I/O failure (disconnect, oversized frame): the
/// stream is unusable. The inner error is a decode failure on a fully-read
/// frame: the stream is still framed correctly, so a server can answer with
/// an error and keep serving.
fn conn_recv<T>(
    conn: &mut Conn,
    decode: impl FnOnce(&[u8]) -> Result<T, WireError>,
) -> io::Result<Result<T, WireError>> {
    let (stream, rbuf) = conn.read_half();
    skaidb_proto::read_frame_into(stream, rbuf)?;
    let Some((&tag, rest)) = rbuf.split_first() else {
        return Ok(Err(WireError::Malformed("empty frame")));
    };
    Ok(match Codec::from_u8(tag) {
        Some(Codec::None) => decode(rest),
        Some(codec) => match rest.get(..4) {
            Some(len_bytes) => {
                let ulen = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
                match decompress(codec, &rest[4..], ulen) {
                    Ok(payload) => decode(&payload),
                    Err(_) => Err(WireError::Malformed("decompress")),
                }
            }
            None => Err(WireError::Malformed("short compressed frame")),
        },
        None => Err(WireError::Malformed("unknown wire codec")),
    })
}

/// Read and decode one [`Response`] from a pooled/authenticated connection.
fn conn_recv_response(conn: &mut Conn) -> io::Result<Response> {
    conn_recv(conn, Response::decode)?.map_err(wire_io_err)
}

/// Read and decode one [`Request`] (the internode server side). See
/// [`conn_recv`] for the two error layers.
pub(crate) fn conn_recv_request(conn: &mut Conn) -> io::Result<Result<Request, WireError>> {
    conn_recv(conn, Request::decode)
}

/// Max idle connections kept per peer.
const MAX_IDLE_PER_PEER: usize = 32;

/// A pool of persistent, authenticated internode connections, keyed by peer
/// address. Reuses an idle connection when available (the server keeps
/// connections alive across requests), avoiding a TCP+auth handshake per
/// replicated write. All connections go through the shared [`Authenticator`], so
/// a peer that can't satisfy the configured auth mode is rejected.
#[derive(Debug)]
pub struct Pool {
    auth: Arc<Authenticator>,
    idle: std::sync::Mutex<std::collections::HashMap<String, Vec<Conn>>>,
}

impl Pool {
    pub fn new(auth: Arc<Authenticator>) -> Self {
        Pool {
            auth,
            idle: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Send `req` to `addr`, reusing a pooled connection if possible. On any I/O
    /// error the connection is dropped (not returned to the pool). An error on
    /// a **reused** connection retries once on a fresh one: after a peer
    /// restarts, every idle connection to it is silently dead, and without the
    /// retry each stale socket fails one real request (visible as spurious
    /// quorum failures for a while after every rolling upgrade).
    pub fn call(&self, addr: &str, req: &Request) -> io::Result<Response> {
        if let Some(mut conn) = self.take_idle(addr) {
            if let Ok(resp) = Self::roundtrip(&mut conn, req) {
                self.put(addr, conn);
                return Ok(resp);
            }
            // Stale pooled connection: fall through to a fresh dial. Also
            // drop the rest of this peer's idle bucket — they were opened
            // to the same dead process.
            self.idle.lock().expect("pool lock").remove(addr);
        }
        let mut conn = self.auth.connect(addr, None)?;
        let resp = Self::roundtrip(&mut conn, req)?;
        self.put(addr, conn);
        Ok(resp)
    }

    fn roundtrip(conn: &mut Conn, req: &Request) -> io::Result<Response> {
        conn_send(conn, |o| req.encode_into(o))?;
        conn_recv_response(conn)
    }

    /// First half of [`Pool::call`]: send `req` on a pooled connection and
    /// return the in-flight call, so the caller can overlap its own work
    /// (a WAL fsync, a local read) with the peer's processing — without
    /// spawning a thread. Internode frames are small, so the send lands in
    /// the socket buffer without blocking on the peer.
    pub fn call_begin(&self, addr: &str, req: &Request) -> io::Result<Pending<'_>> {
        let mut conn = self.take(addr)?;
        conn_send(&mut conn, |o| req.encode_into(o))?;
        Ok(Pending {
            pool: self,
            addr: addr.to_string(),
            conn: Some(conn),
        })
    }

    /// Like [`Pool::call`], but on a fresh, time-bounded connection that is not
    /// returned to the pool — for liveness probing, where an unreachable peer
    /// must fail fast rather than block on the OS connect timeout.
    pub fn call_timeout(&self, addr: &str, req: &Request, timeout: Duration) -> io::Result<Response> {
        let mut conn = self.auth.connect(addr, Some(timeout))?;
        conn_send(&mut conn, |o| req.encode_into(o))?;
        conn_recv_response(&mut conn)
    }

    fn take(&self, addr: &str) -> io::Result<Conn> {
        match self.take_idle(addr) {
            Some(conn) => Ok(conn),
            None => self.auth.connect(addr, None),
        }
    }

    fn take_idle(&self, addr: &str) -> Option<Conn> {
        self.idle
            .lock()
            .expect("pool lock")
            .get_mut(addr)
            .and_then(|v| v.pop())
    }

    fn put(&self, addr: &str, conn: Conn) {
        let mut idle = self.idle.lock().expect("pool lock");
        // Look up before inserting so the hot path (bucket exists) doesn't
        // allocate a key `String` per returned connection.
        match idle.get_mut(addr) {
            Some(bucket) => {
                if bucket.len() < MAX_IDLE_PER_PEER {
                    bucket.push(conn);
                }
            }
            None => {
                idle.insert(addr.to_string(), vec![conn]);
            }
        }
    }
}

/// A request sent via [`Pool::call_begin`] whose response hasn't been read
/// yet. Dropping it without [`Pending::finish`] discards the connection (a
/// response is still in flight on it, so it can't be pooled for reuse).
#[derive(Debug)]
pub struct Pending<'p> {
    pool: &'p Pool,
    addr: String,
    conn: Option<Conn>,
}

impl Pending<'_> {
    /// Second half of the round-trip: block on the peer's response and return
    /// the connection to the pool.
    pub fn finish(mut self) -> io::Result<Response> {
        let mut conn = self.conn.take().expect("Pending::finish called twice");
        let resp = conn_recv_response(&mut conn)?;
        self.pool.put(&self.addr, conn);
        Ok(resp)
    }
}

fn put_labels(o: &mut Vec<u8>, labels: &[(String, String)]) {
    o.extend_from_slice(&(labels.len() as u32).to_le_bytes());
    for (k, v) in labels {
        put_str(o, k);
        put_str(o, v);
    }
}

fn put_str(o: &mut Vec<u8>, s: &str) {
    put_bytes(o, s.as_bytes());
}
/// Encode a filter [`Expr`] for pushdown. Aggregates can't appear in a `WHERE`
/// filter, so they're encoded as a sentinel that fails to decode.
fn put_expr(o: &mut Vec<u8>, e: &Expr) {
    match e {
        Expr::Literal(v) => {
            o.push(0);
            put_bytes(o, &v.encode());
        }
        Expr::Column(p) => {
            o.push(1);
            put_str(o, p);
        }
        Expr::Unary { op, expr } => {
            o.push(2);
            o.push(unary_code(*op));
            put_expr(o, expr);
        }
        Expr::Binary { op, left, right } => {
            o.push(3);
            o.push(binary_code(*op));
            put_expr(o, left);
            put_expr(o, right);
        }
        Expr::IsNull { expr, negated } => {
            o.push(4);
            put_expr(o, expr);
            o.push(u8::from(*negated));
        }
        // Not valid in a pushed filter: aggregates never are, parameters are
        // substituted by `bind`, and scalar functions (`now()` resolves to a
        // literal; `time_bucket` never lands in an index-pushdown filter) are
        // evaluated coordinator-side before pushdown.
        Expr::Aggregate { .. } | Expr::Parameter(_) | Expr::Func { .. } => o.push(255),
    }
}

fn unary_code(op: UnaryOp) -> u8 {
    match op {
        UnaryOp::Not => 0,
        UnaryOp::Neg => 1,
    }
}
fn unary_from(b: u8) -> Result<UnaryOp, WireError> {
    match b {
        0 => Ok(UnaryOp::Not),
        1 => Ok(UnaryOp::Neg),
        _ => Err(WireError::Malformed("bad unary op")),
    }
}
fn binary_code(op: BinaryOp) -> u8 {
    use BinaryOp::*;
    match op {
        Eq => 0,
        NotEq => 1,
        Lt => 2,
        LtEq => 3,
        Gt => 4,
        GtEq => 5,
        And => 6,
        Or => 7,
        Add => 8,
        Sub => 9,
        Mul => 10,
        Div => 11,
    }
}
fn binary_from(b: u8) -> Result<BinaryOp, WireError> {
    use BinaryOp::*;
    Ok(match b {
        0 => Eq,
        1 => NotEq,
        2 => Lt,
        3 => LtEq,
        4 => Gt,
        5 => GtEq,
        6 => And,
        7 => Or,
        8 => Add,
        9 => Sub,
        10 => Mul,
        11 => Div,
        _ => return Err(WireError::Malformed("bad binary op")),
    })
}

/// A length-prefixed list of versioned [`WireRow`]s.
fn put_rows(o: &mut Vec<u8>, rows: &[WireRow]) {
    o.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for (k, v, hlc, is_put) in rows {
        put_bytes(o, k);
        put_bytes(o, v);
        o.extend_from_slice(&hlc.to_bytes());
        o.push(u8::from(*is_put));
    }
}

/// A length-prefixed list of `(id, addr)` member pairs.
fn put_members(o: &mut Vec<u8>, members: &[(String, String)]) {
    o.extend_from_slice(&(members.len() as u32).to_le_bytes());
    for (id, addr) in members {
        put_str(o, id);
        put_str(o, addr);
    }
}
/// Binary encoding for shard aggregation rows: `u32` row count, then per
/// row a length-prefixed `Value::encode` key, `u64` doc count, `u32`
/// metric count, and length-prefixed `Value::encode` metrics. Uses the
/// same Value codec as row data, so every Value variant round-trips.
pub fn encode_agg_rows(rows: &[skaidb_fts::AggRow]) -> Vec<u8> {
    let mut o = Vec::new();
    o.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for row in rows {
        put_bytes(&mut o, &row.key.encode());
        o.extend_from_slice(&row.count.to_le_bytes());
        o.extend_from_slice(&(row.metrics.len() as u32).to_le_bytes());
        for m in &row.metrics {
            put_bytes(&mut o, &m.encode());
        }
    }
    o
}

/// Inverse of [`encode_agg_rows`].
pub fn decode_agg_rows(buf: &[u8]) -> Result<Vec<skaidb_fts::AggRow>, WireError> {
    let mut c = Cur::new(buf);
    let n = c.u32()? as usize;
    let mut rows = Vec::with_capacity(n.min(1 << 20));
    for _ in 0..n {
        let key = skaidb_types::Value::decode(&c.bytes()?)
            .map_err(|_| WireError::Malformed("bad agg key value"))?;
        let count = c.u64()?;
        let mlen = c.u32()? as usize;
        let mut metrics = Vec::with_capacity(mlen.min(1 << 16));
        for _ in 0..mlen {
            metrics.push(
                skaidb_types::Value::decode(&c.bytes()?)
                    .map_err(|_| WireError::Malformed("bad agg metric value"))?,
            );
        }
        rows.push(skaidb_fts::AggRow { key, count, metrics });
    }
    Ok(rows)
}

/// Binary encoding for shard sorted rows: `u32` count, then per row a
/// length-prefixed key and a length-prefixed `Value::encode`d document.
pub fn encode_sorted_rows(rows: &[(Vec<u8>, skaidb_types::Document)]) -> Vec<u8> {
    let mut o = Vec::new();
    o.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for (key, doc) in rows {
        put_bytes(&mut o, key);
        put_bytes(&mut o, &skaidb_types::Value::Document(doc.clone()).encode());
    }
    o
}

/// Inverse of [`encode_sorted_rows`].
pub fn decode_sorted_rows(
    buf: &[u8],
) -> Result<Vec<(Vec<u8>, skaidb_types::Document)>, WireError> {
    let mut c = Cur::new(buf);
    let n = c.u32()? as usize;
    let mut rows = Vec::with_capacity(n.min(1 << 20));
    for _ in 0..n {
        let key = c.bytes()?;
        let doc = match skaidb_types::Value::decode(&c.bytes()?) {
            Ok(skaidb_types::Value::Document(doc)) => doc,
            _ => return Err(WireError::Malformed("bad sorted-row document")),
        };
        rows.push((key, doc));
    }
    Ok(rows)
}

fn put_bytes(o: &mut Vec<u8>, b: &[u8]) {
    o.extend_from_slice(&(b.len() as u32).to_le_bytes());
    o.extend_from_slice(b);
}
/// An optional byte string: a presence flag followed by the bytes when present.
fn put_opt_bytes(o: &mut Vec<u8>, b: Option<&[u8]>) {
    match b {
        Some(bytes) => {
            o.push(1);
            put_bytes(o, bytes);
        }
        None => o.push(0),
    }
}

struct Cur<'a> {
    buf: &'a [u8],
    pos: usize,
}
impl<'a> Cur<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cur { buf, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], WireError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(WireError::Malformed("overflow"))?;
        let s = self
            .buf
            .get(self.pos..end)
            .ok_or(WireError::Malformed("short"))?;
        self.pos = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, WireError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, WireError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn f32(&mut self) -> Result<f32, WireError> {
        Ok(f32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn bytes(&mut self) -> Result<Vec<u8>, WireError> {
        let n = self.u32()? as usize;
        Ok(self.take(n)?.to_vec())
    }
    fn opt_bytes(&mut self) -> Result<Option<Vec<u8>>, WireError> {
        Ok(if self.u8()? == 1 {
            Some(self.bytes()?)
        } else {
            None
        })
    }
    fn string(&mut self) -> Result<String, WireError> {
        String::from_utf8(self.bytes()?).map_err(|_| WireError::Malformed("bad utf-8"))
    }
    fn labels(&mut self) -> Result<Vec<(String, String)>, WireError> {
        let n = self.u32()? as usize;
        let mut labels = Vec::with_capacity(n);
        for _ in 0..n {
            let k = self.string()?;
            let v = self.string()?;
            labels.push((k, v));
        }
        Ok(labels)
    }
    fn expr(&mut self) -> Result<Expr, WireError> {
        Ok(match self.u8()? {
            0 => Expr::Literal(
                Value::decode(&self.bytes()?).map_err(|_| WireError::Malformed("bad value"))?,
            ),
            1 => Expr::Column(self.string()?),
            2 => {
                let op = unary_from(self.u8()?)?;
                Expr::Unary {
                    op,
                    expr: Box::new(self.expr()?),
                }
            }
            3 => {
                let op = binary_from(self.u8()?)?;
                let left = Box::new(self.expr()?);
                let right = Box::new(self.expr()?);
                Expr::Binary { op, left, right }
            }
            4 => {
                let expr = Box::new(self.expr()?);
                let negated = self.u8()? != 0;
                Expr::IsNull { expr, negated }
            }
            _ => return Err(WireError::Malformed("unsupported filter expr")),
        })
    }
    /// Reverse of [`put_rows`].
    fn rows(&mut self) -> Result<Vec<WireRow>, WireError> {
        let n = self.u32()? as usize;
        let mut rows = Vec::with_capacity(n);
        for _ in 0..n {
            let key = self.bytes()?;
            let value = self.bytes()?;
            let hlc = self.hlc()?;
            let is_put = self.u8()? != 0;
            rows.push((key, value, hlc, is_put));
        }
        Ok(rows)
    }
    fn members(&mut self) -> Result<Vec<(String, String)>, WireError> {
        let n = self.u32()? as usize;
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let id = self.string()?;
            let addr = self.string()?;
            out.push((id, addr));
        }
        Ok(out)
    }
    fn hlc(&mut self) -> Result<Hlc, WireError> {
        let b: [u8; 12] = self.take(12)?.try_into().unwrap();
        Ok(Hlc::from_bytes(b))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrips() {
        for req in [
            Request::ApplyPut {
                table: "t".into(),
                key: vec![1, 2],
                value: vec![3, 4, 5],
                hlc: Hlc::new(10, 1),
            },
            Request::ApplyDelete {
                table: "t".into(),
                key: vec![9],
                hlc: Hlc::new(11, 0),
            },
            Request::ApplyBatch {
                table: "t".into(),
                rows: vec![
                    (vec![1], vec![2, 3], Hlc::new(12, 0), true),
                    (vec![4], vec![], Hlc::new(13, 1), false), // tombstone
                ],
            },
            Request::ApplyBatch {
                table: "t".into(),
                rows: vec![],
            },
            Request::LocalScan { table: "t".into() },
            Request::ScanPage {
                table: "t".into(),
                after: Some(vec![3, 4]),
                limit: 512,
            },
            Request::ScanPage {
                table: "t".into(),
                after: None,
                limit: 1,
            },
            Request::TsAppend {
                table: "cpu".into(),
                rows: vec![
                    (
                        vec![("__field__".into(), "value".into()), ("host".into(), "a".into())],
                        1_700_000_000_000,
                        0.5,
                    ),
                    (vec![("host".into(), "b".into())], 1, f64::NEG_INFINITY),
                ],
            },
            Request::TsQuery {
                table: "cpu".into(),
                matchers: vec![(false, "host".into(), "a".into()), (true, "core".into(), "0".into())],
                t0: i64::MIN,
                t1: i64::MAX,
            },
            Request::TsPartials {
                table: "cpu".into(),
                matchers: vec![(false, "__field__".into(), "value".into())],
                t0: 0,
                t1: 3_600_000,
                bucket: 60_000,
            },
            Request::TsPartials {
                table: "cpu".into(),
                matchers: vec![],
                t0: i64::MIN,
                t1: i64::MAX,
                bucket: 0,
            },
            Request::FilteredScan {
                table: "t".into(),
                filter: Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(Expr::Column("region".into())),
                    right: Box::new(Expr::Literal(Value::String("eu".into()))),
                },
            },
            Request::LocalGet {
                table: "t".into(),
                key: vec![7, 8, 9],
            },
            Request::ApplyDdl {
                db: "default".into(),
                sql: "CREATE TABLE t (PRIMARY KEY (id))".into(),
                hlc: Hlc::new(7, 1),
            },
            Request::IndexScan {
                index: "t_age".into(),
                start: Some(vec![1, 2, 3]),
                end: None,
            },
            Request::IndexScan {
                index: "t_age".into(),
                start: None,
                end: Some(vec![9]),
            },
            Request::VectorSearch {
                index: "t_emb".into(),
                query: vec![0.1, -0.2, 0.3, 0.0],
                k: 10,
            },
            Request::Search {
                table: "articles".into(),
                query: r#"{"match":{"field":"body","text":"quick fox"}}"#.into(),
                k: 10,
            },
            Request::Search {
                table: "articles".into(),
                query: "{}".into(),
                k: 0,
            },
            Request::SetMembership {
                epoch: 7,
                members: vec![
                    ("a".into(), "127.0.0.1:1".into()),
                    ("b".into(), "127.0.0.1:2".into()),
                ],
                prev_members: vec![("a".into(), "127.0.0.1:1".into())],
            },
            Request::Rebalance {
                joiner: "c".into(),
            },
            Request::Drain {
                members: vec![
                    ("a".into(), "127.0.0.1:1".into()),
                    ("b".into(), "127.0.0.1:2".into()),
                ],
            },
            Request::Reclaim,
            Request::Repair,
            Request::SchemaDdl,
            Request::Ping,
        ] {
            assert_eq!(Request::decode(&req.encode()).unwrap(), req);
        }
    }

    #[test]
    fn response_roundtrips() {
        let scan = Response::Scan {
            rows: vec![
                (vec![1], vec![2, 3], Hlc::new(5, 0), true),
                (vec![4], vec![], Hlc::new(6, 2), false),
            ],
        };
        for res in [
            Response::Ack,
            scan,
            Response::Keys {
                keys: vec![vec![1], vec![2, 3], vec![]],
            },
            Response::VectorHits {
                hits: vec![(vec![1], 0.0), (vec![2, 3], 1.5)],
            },
            Response::SearchHits {
                hits: vec![(vec![1], 7.25), (vec![2, 3], 0.0)],
            },
            Response::Get {
                entry: Some((vec![1, 2, 3], Hlc::new(9, 1), true)),
            },
            Response::Get {
                entry: Some((vec![], Hlc::new(9, 2), false)),
            },
            Response::Get { entry: None },
            Response::Schema {
                entries: vec![
                    ("default".into(), "CREATE DATABASE IF NOT EXISTS foo".into(), Hlc::new(1, 0)),
                    (
                        "foo".into(),
                        "CREATE TABLE IF NOT EXISTS t (PRIMARY KEY (id))".into(),
                        Hlc::new(2, 3),
                    ),
                ],
            },
            Response::Err("x".into()),
            Response::Pong,
            Response::HostStats {
                json: r#"{"cpu_percent":12.5}"#.into(),
            },
            Response::TsSeries {
                series: vec![
                    (
                        vec![("host".into(), "a".into())],
                        vec![(1000, 0.5), (2000, f64::MAX)],
                    ),
                    (vec![], vec![]),
                ],
            },
            Response::TsPartials {
                series: vec![
                    (
                        vec![("__field__".into(), "value".into()), ("host".into(), "a".into())],
                        vec![
                            (0, 4, 90.0, 0.0, 45.0, 0, 0.0, 45_000, 45.0, 45.0),
                            (60_000, 1, 60.0, 60.0, 60.0, 60_000, 60.0, 60_000, 60.0, 0.0),
                        ],
                    ),
                    (vec![], vec![]),
                ],
            },
        ] {
            assert_eq!(Response::decode(&res.encode()).unwrap(), res);
        }
    }

    #[test]
    fn frame_envelope_roundtrips_small_and_large() {
        // Small payload stays raw; large compressible payload is LZ4'd.
        let small = vec![1u8, 2, 3];
        let large = b"row-row-row-your-boat ".repeat(64);

        let small_framed = frame_encode(&small);
        assert_eq!(small_framed[0], Codec::None.to_u8());
        assert_eq!(frame_decode(&small_framed).unwrap(), small);

        let large_framed = frame_encode(&large);
        assert_eq!(large_framed[0], Codec::Lz4.to_u8());
        assert!(large_framed.len() < large.len(), "large frame should shrink");
        assert_eq!(frame_decode(&large_framed).unwrap(), large);
    }

    #[test]
    fn frame_decode_rejects_garbage() {
        assert!(frame_decode(&[]).is_err());
        assert!(frame_decode(&[99]).is_err()); // unknown codec tag
    }

    #[test]
    fn large_scan_response_survives_frame_envelope() {
        let rows: Vec<(Vec<u8>, Vec<u8>, Hlc, bool)> = (0..200)
            .map(|i| {
                (
                    format!("key{i:04}").into_bytes(),
                    format!("a fairly repetitive value number {i}").into_bytes(),
                    Hlc::new(i as u64, 0),
                    i % 7 != 0, // sprinkle in some tombstones
                )
            })
            .collect();
        let res = Response::Scan { rows };
        let framed = frame_encode(&res.encode());
        let payload = frame_decode(&framed).unwrap();
        assert_eq!(Response::decode(&payload).unwrap(), res);
    }
}
