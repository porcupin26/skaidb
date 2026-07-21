//! Prometheus-style metrics (SPEC §10).
//!
//! A small thread-safe registry that renders the Prometheus text exposition
//! format. It supports the three core metric types:
//!
//! * **counters** — monotonically increasing (`incr`/`add`),
//! * **gauges** — set to an absolute value (`set`) or moved by a delta
//!   (`gauge_add`/`gauge_inc`/`gauge_dec`), and
//! * **histograms** — bucketed observations (`observe`), rendered as the
//!   standard `_bucket{le=…}` + `_sum` + `_count` family.
//!
//! Series keys carry their labels inline, e.g.
//! `skaidb_queries_total{type="select"}`. Each *base* metric name (the part
//! before any `{labels}`) is rendered with the correct `# TYPE` and a `# HELP`
//! line. Rendering a gauge as a counter (the old behaviour for `skaidb_up`)
//! made `rate()`/`increase()` semantically wrong and tripped strict scrapers;
//! the type is now tracked per base metric.
//!
//! # Hot path vs. cold path
//!
//! The per-query/per-connection series are a small closed set, so they live in
//! pre-allocated [`AtomicU64`] cells addressed by enum ([`QueryType`],
//! [`ErrorClass`], [`TxKind`], [`Endpoint`]). Recording one of them is a
//! handful of relaxed atomic ops — **no mutex, no heap allocation** — so
//! concurrent connection threads never serialize on the registry. The
//! string-keyed API (`incr`/`add`/`set`/`observe`) remains for the cold paths:
//! startup info gauges, scrape-time storage/cluster snapshots (including
//! open-ended per-table/per-peer labels), and rare admin ops. Rendering merges
//! both worlds and may allocate freely — scrapes are cold.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;

/// The Prometheus metric type of a base metric name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

impl MetricType {
    fn as_str(self) -> &'static str {
        match self {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
            MetricType::Histogram => "histogram",
        }
    }
}

/// Type + help text for one base metric name.
#[derive(Debug, Clone)]
struct Meta {
    kind: MetricType,
    help: String,
}

/// Default latency histogram bucket upper bounds, in seconds. Covers sub-ms
/// point reads through multi-second scans. `+Inf` is implied by `_count`.
const BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// One histogram series' accumulated observations. `buckets[i]` is the
/// cumulative count of observations `<= BUCKETS[i]`.
#[derive(Debug, Clone)]
struct Hist {
    buckets: Vec<u64>,
    sum: f64,
    count: u64,
}

impl Default for Hist {
    fn default() -> Self {
        Hist {
            buckets: vec![0; BUCKETS.len()],
            sum: 0.0,
            count: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Hot-path label enums
// ---------------------------------------------------------------------------

/// REST request class, for `skaidb_rest_requests_total{path=…}` and the
/// status tab's REST-activity table: every request the REST listener
/// serves lands in exactly one class, timed end to end (read → response
/// written), so count + duration-sum give the average response time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestPath {
    /// `POST /query` (+ streaming variant).
    Query,
    /// `POST /insert` — the JSON document upsert.
    Insert,
    /// The ES-compatible subset (`/{index}/_search`, `_bulk`, …).
    Es,
    /// Prometheus `remote_write` + `/api/v1/*` reads.
    Prom,
    /// The web UI: static shell + `/ui/*` JSON.
    Ui,
    /// Unauthenticated ops probes: `/metrics`, `/health`, `/ready`, `/status`.
    Ops,
    /// `POST /admin/*` control plane.
    Admin,
    /// Everything else (404s included — they cost a request too).
    Other,
}

impl RestPath {
    pub const ALL: [RestPath; 8] = [
        RestPath::Query,
        RestPath::Insert,
        RestPath::Es,
        RestPath::Prom,
        RestPath::Ui,
        RestPath::Ops,
        RestPath::Admin,
        RestPath::Other,
    ];

    /// The UI-facing label.
    pub fn label(self) -> &'static str {
        match self {
            RestPath::Query => "query",
            RestPath::Insert => "insert",
            RestPath::Es => "es",
            RestPath::Prom => "prom",
            RestPath::Ui => "ui",
            RestPath::Ops => "ops",
            RestPath::Admin => "admin",
            RestPath::Other => "other",
        }
    }

    fn count_series(self) -> &'static str {
        match self {
            RestPath::Query => "skaidb_rest_requests_total{path=\"query\"}",
            RestPath::Insert => "skaidb_rest_requests_total{path=\"insert\"}",
            RestPath::Es => "skaidb_rest_requests_total{path=\"es\"}",
            RestPath::Prom => "skaidb_rest_requests_total{path=\"prom\"}",
            RestPath::Ui => "skaidb_rest_requests_total{path=\"ui\"}",
            RestPath::Ops => "skaidb_rest_requests_total{path=\"ops\"}",
            RestPath::Admin => "skaidb_rest_requests_total{path=\"admin\"}",
            RestPath::Other => "skaidb_rest_requests_total{path=\"other\"}",
        }
    }

    fn duration_series(self) -> &'static str {
        match self {
            RestPath::Query => "skaidb_rest_request_duration_us_total{path=\"query\"}",
            RestPath::Insert => "skaidb_rest_request_duration_us_total{path=\"insert\"}",
            RestPath::Es => "skaidb_rest_request_duration_us_total{path=\"es\"}",
            RestPath::Prom => "skaidb_rest_request_duration_us_total{path=\"prom\"}",
            RestPath::Ui => "skaidb_rest_request_duration_us_total{path=\"ui\"}",
            RestPath::Ops => "skaidb_rest_request_duration_us_total{path=\"ops\"}",
            RestPath::Admin => "skaidb_rest_request_duration_us_total{path=\"admin\"}",
            RestPath::Other => "skaidb_rest_request_duration_us_total{path=\"other\"}",
        }
    }
}

/// The `type` label of `skaidb_queries_total`/`skaidb_query_duration_seconds`,
/// classifying a statement by its leading keyword.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    Ddl,
    Tx,
    Other,
}

impl QueryType {
    /// Every variant, in cell-array order (`variant as usize` indexes it).
    const ALL: [QueryType; 7] = [
        QueryType::Select,
        QueryType::Insert,
        QueryType::Update,
        QueryType::Delete,
        QueryType::Ddl,
        QueryType::Tx,
        QueryType::Other,
    ];

    /// Full counter series key, e.g. `skaidb_queries_total{type="select"}`.
    fn counter_series(self) -> &'static str {
        match self {
            QueryType::Select => "skaidb_queries_total{type=\"select\"}",
            QueryType::Insert => "skaidb_queries_total{type=\"insert\"}",
            QueryType::Update => "skaidb_queries_total{type=\"update\"}",
            QueryType::Delete => "skaidb_queries_total{type=\"delete\"}",
            QueryType::Ddl => "skaidb_queries_total{type=\"ddl\"}",
            QueryType::Tx => "skaidb_queries_total{type=\"tx\"}",
            QueryType::Other => "skaidb_queries_total{type=\"other\"}",
        }
    }

    /// Full histogram series key, e.g. `skaidb_query_duration_seconds{type="select"}`.
    fn duration_series(self) -> &'static str {
        match self {
            QueryType::Select => "skaidb_query_duration_seconds{type=\"select\"}",
            QueryType::Insert => "skaidb_query_duration_seconds{type=\"insert\"}",
            QueryType::Update => "skaidb_query_duration_seconds{type=\"update\"}",
            QueryType::Delete => "skaidb_query_duration_seconds{type=\"delete\"}",
            QueryType::Ddl => "skaidb_query_duration_seconds{type=\"ddl\"}",
            QueryType::Tx => "skaidb_query_duration_seconds{type=\"tx\"}",
            QueryType::Other => "skaidb_query_duration_seconds{type=\"other\"}",
        }
    }
}

/// The `class` label of `skaidb_query_errors_total` — a small, bounded set so
/// the metric is actionable without unbounded label values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    Permission,
    Timeout,
    Parse,
    Constraint,
    Storage,
    Other,
}

impl ErrorClass {
    /// Every variant, in cell-array order (`variant as usize` indexes it).
    const ALL: [ErrorClass; 6] = [
        ErrorClass::Permission,
        ErrorClass::Timeout,
        ErrorClass::Parse,
        ErrorClass::Constraint,
        ErrorClass::Storage,
        ErrorClass::Other,
    ];

    /// Full series key, e.g. `skaidb_query_errors_total{class="parse"}`.
    fn series(self) -> &'static str {
        match self {
            ErrorClass::Permission => "skaidb_query_errors_total{class=\"permission\"}",
            ErrorClass::Timeout => "skaidb_query_errors_total{class=\"timeout\"}",
            ErrorClass::Parse => "skaidb_query_errors_total{class=\"parse\"}",
            ErrorClass::Constraint => "skaidb_query_errors_total{class=\"constraint\"}",
            ErrorClass::Storage => "skaidb_query_errors_total{class=\"storage\"}",
            ErrorClass::Other => "skaidb_query_errors_total{class=\"other\"}",
        }
    }
}

/// The `kind` label of `skaidb_transactions_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxKind {
    Begin,
    Commit,
    Rollback,
}

impl TxKind {
    /// Every variant, in cell-array order (`variant as usize` indexes it).
    const ALL: [TxKind; 3] = [TxKind::Begin, TxKind::Commit, TxKind::Rollback];

    /// Full series key, e.g. `skaidb_transactions_total{kind="begin"}`.
    fn series(self) -> &'static str {
        match self {
            TxKind::Begin => "skaidb_transactions_total{kind=\"begin\"}",
            TxKind::Commit => "skaidb_transactions_total{kind=\"commit\"}",
            TxKind::Rollback => "skaidb_transactions_total{kind=\"rollback\"}",
        }
    }
}

/// The `endpoint` label of the connection/bytes series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    Binary,
    Rest,
}

impl Endpoint {
    /// Every variant, in cell-array order (`variant as usize` indexes it).
    const ALL: [Endpoint; 2] = [Endpoint::Binary, Endpoint::Rest];

    fn connections_total_series(self) -> &'static str {
        match self {
            Endpoint::Binary => "skaidb_connections_total{endpoint=\"binary\"}",
            Endpoint::Rest => "skaidb_connections_total{endpoint=\"rest\"}",
        }
    }

    fn connections_active_series(self) -> &'static str {
        match self {
            Endpoint::Binary => "skaidb_connections_active{endpoint=\"binary\"}",
            Endpoint::Rest => "skaidb_connections_active{endpoint=\"rest\"}",
        }
    }

    fn bytes_returned_series(self) -> &'static str {
        match self {
            Endpoint::Binary => "skaidb_bytes_returned_total{endpoint=\"binary\"}",
            Endpoint::Rest => "skaidb_bytes_returned_total{endpoint=\"rest\"}",
        }
    }
}

// ---------------------------------------------------------------------------
// Hot-path storage
// ---------------------------------------------------------------------------

/// One lock-free counter/gauge cell. `touched` mirrors the map semantics of
/// the string-keyed API: a series appears in the exposition only after its
/// first operation (even a zero-delta `add`).
#[derive(Debug, Default)]
struct Cell {
    value: AtomicU64,
    touched: AtomicBool,
}

impl Cell {
    /// Add `n` (counters, and positive gauge deltas).
    fn add(&self, n: u64) {
        self.value.fetch_add(n, Relaxed);
        self.touched.store(true, Relaxed);
    }

    /// Subtract `n`, saturating at zero (gauge decrements never wrap).
    fn sub_saturating(&self, n: u64) {
        let _ = self
            .value
            .fetch_update(Relaxed, Relaxed, |cur| Some(cur.saturating_sub(n)));
        self.touched.store(true, Relaxed);
    }

    /// Current value, or `None` if the series was never touched.
    fn read(&self) -> Option<u64> {
        self.touched
            .load(Relaxed)
            .then(|| self.value.load(Relaxed))
    }
}

/// One lock-free histogram cell. Buckets store *per-bucket* counts (one
/// `fetch_add` per observation); the cumulative `le` form is computed at
/// render time. The sum is kept as `f64` bits in an `AtomicU64` since std has
/// no atomic float.
#[derive(Debug, Default)]
struct HistCell {
    buckets: [AtomicU64; BUCKETS.len()],
    sum_bits: AtomicU64,
    count: AtomicU64,
}

impl HistCell {
    fn observe(&self, value: f64) {
        if let Some(i) = BUCKETS.iter().position(|&bound| value <= bound) {
            self.buckets[i].fetch_add(1, Relaxed);
        }
        let _ = self.sum_bits.fetch_update(Relaxed, Relaxed, |bits| {
            Some((f64::from_bits(bits) + value).to_bits())
        });
        self.count.fetch_add(1, Relaxed);
    }

    /// Cumulative snapshot, or `None` if nothing was ever observed.
    fn snapshot(&self) -> Option<Hist> {
        let count = self.count.load(Relaxed);
        if count == 0 {
            return None;
        }
        let mut cumulative = 0u64;
        let buckets = self
            .buckets
            .iter()
            .map(|b| {
                cumulative += b.load(Relaxed);
                cumulative
            })
            .collect();
        Some(Hist {
            buckets,
            sum: f64::from_bits(self.sum_bits.load(Relaxed)),
            count,
        })
    }
}

/// Pre-allocated cells for every hot series (the fixed set written on the
/// query/connection paths). Indexed by the label enums' discriminants.
#[derive(Debug, Default)]
struct HotMetrics {
    queries_total: [Cell; QueryType::ALL.len()],
    query_duration: [HistCell; QueryType::ALL.len()],
    query_errors_total: [Cell; ErrorClass::ALL.len()],
    transactions_total: [Cell; TxKind::ALL.len()],
    rows_returned_total: Cell,
    rows_written_total: Cell,
    rows_scanned_total: Cell,
    slow_queries_total: Cell,
    authz_denied_total: Cell,
    logins_total: Cell,
    login_failures_total: Cell,
    bytes_returned_total: [Cell; Endpoint::ALL.len()],
    connections_total: [Cell; Endpoint::ALL.len()],
    connections_active: [Cell; Endpoint::ALL.len()],
    queries_in_flight: Cell,
    rest_requests_total: [Cell; RestPath::ALL.len()],
    rest_request_duration_us: [Cell; RestPath::ALL.len()],
}

impl HotMetrics {
    /// Every hot counter cell with its full series key (render/`get` helper).
    fn counters(&self) -> Vec<(&'static str, &Cell)> {
        let mut out = Vec::new();
        for t in QueryType::ALL {
            out.push((t.counter_series(), &self.queries_total[t as usize]));
        }
        for c in ErrorClass::ALL {
            out.push((c.series(), &self.query_errors_total[c as usize]));
        }
        for k in TxKind::ALL {
            out.push((k.series(), &self.transactions_total[k as usize]));
        }
        out.push(("skaidb_rows_returned_total", &self.rows_returned_total));
        out.push(("skaidb_rows_written_total", &self.rows_written_total));
        out.push(("skaidb_rows_scanned_total", &self.rows_scanned_total));
        out.push(("skaidb_slow_queries_total", &self.slow_queries_total));
        out.push(("skaidb_authz_denied_total", &self.authz_denied_total));
        out.push(("skaidb_logins_total", &self.logins_total));
        out.push(("skaidb_login_failures_total", &self.login_failures_total));
        for p in RestPath::ALL {
            out.push((p.count_series(), &self.rest_requests_total[p as usize]));
            out.push((p.duration_series(), &self.rest_request_duration_us[p as usize]));
        }
        for e in Endpoint::ALL {
            out.push((
                e.bytes_returned_series(),
                &self.bytes_returned_total[e as usize],
            ));
            out.push((
                e.connections_total_series(),
                &self.connections_total[e as usize],
            ));
        }
        out
    }

    /// Every hot gauge cell with its full series key (render/`get` helper).
    fn gauges(&self) -> Vec<(&'static str, &Cell)> {
        let mut out = Vec::new();
        for e in Endpoint::ALL {
            out.push((
                e.connections_active_series(),
                &self.connections_active[e as usize],
            ));
        }
        out.push(("skaidb_queries_in_flight", &self.queries_in_flight));
        out
    }

    /// Fold the touched hot scalar cells into a string-keyed snapshot.
    /// Counters accumulate on top of any same-named dynamic series; gauges
    /// overwrite (set semantics).
    fn merge_scalars(&self, out: &mut BTreeMap<String, u64>) {
        for (series, cell) in self.counters() {
            if let Some(v) = cell.read() {
                *out.entry(series.to_string()).or_insert(0) += v;
            }
        }
        for (series, cell) in self.gauges() {
            if let Some(v) = cell.read() {
                out.insert(series.to_string(), v);
            }
        }
    }

    /// Fold the touched hot histogram cells into a string-keyed snapshot.
    fn merge_hists(&self, out: &mut BTreeMap<String, Hist>) {
        for t in QueryType::ALL {
            if let Some(snap) = self.query_duration[t as usize].snapshot() {
                let h = out.entry(t.duration_series().to_string()).or_default();
                for (b, add) in h.buckets.iter_mut().zip(&snap.buckets) {
                    *b += add;
                }
                h.sum += snap.sum;
                h.count += snap.count;
            }
        }
    }
}

/// A registry of counters, gauges, and histograms.
#[derive(Debug)]
pub struct Metrics {
    /// Lock-free cells for the fixed hot-path series.
    hot: HotMetrics,
    /// Counter and gauge values, keyed by full series (name + labels).
    /// Cold-path only: startup info, scrape-time snapshots, admin ops.
    series: Mutex<BTreeMap<String, u64>>,
    /// Histogram accumulators, keyed by full series (name + labels, no `le`).
    hist: Mutex<BTreeMap<String, Hist>>,
    /// Type + help, keyed by base metric name.
    meta: Mutex<BTreeMap<String, Meta>>,
}

impl Metrics {
    pub fn new() -> Self {
        let m = Metrics {
            hot: HotMetrics::default(),
            series: Mutex::default(),
            hist: Mutex::default(),
            meta: Mutex::default(),
        };
        // Register the well-known metrics with their correct type + help so the
        // exposition is meaningful even before the first observation.
        for (base, kind, help) in KNOWN_METRICS {
            m.register(base, *kind, help);
        }
        m
    }

    /// Declare a base metric's type and help text (idempotent; last write wins).
    pub fn register(&self, base: &str, kind: MetricType, help: &str) {
        self.meta.lock().expect("metrics meta").insert(
            base.to_string(),
            Meta {
                kind,
                help: help.to_string(),
            },
        );
    }

    /// Ensure `base` has metadata; default to `kind` with no help if unset.
    fn ensure_meta(&self, base: &str, kind: MetricType) {
        let mut meta = self.meta.lock().expect("metrics meta");
        meta.entry(base.to_string()).or_insert_with(|| Meta {
            kind,
            help: String::new(),
        });
    }

    // -- hot-path API (lock-free, allocation-free) ---------------------------

    /// Count one executed statement (`skaidb_queries_total{type=…}`).
    pub fn incr_query(&self, kind: QueryType) {
        self.hot.queries_total[kind as usize].add(1);
    }

    /// Record one statement latency (`skaidb_query_duration_seconds{type=…}`).
    pub fn observe_query_duration(&self, kind: QueryType, seconds: f64) {
        self.hot.query_duration[kind as usize].observe(seconds);
    }

    /// Count one failed statement (`skaidb_query_errors_total{class=…}`).
    pub fn incr_query_error(&self, class: ErrorClass) {
        self.hot.query_errors_total[class as usize].add(1);
    }

    /// Count one transaction-control statement (`skaidb_transactions_total{kind=…}`).
    pub fn incr_transaction(&self, kind: TxKind) {
        self.hot.transactions_total[kind as usize].add(1);
    }

    /// Add to `skaidb_rows_returned_total`.
    pub fn add_rows_returned(&self, n: u64) {
        self.hot.rows_returned_total.add(n);
    }

    /// Add to `skaidb_rows_scanned_total`.
    pub fn add_rows_scanned(&self, n: u64) {
        self.hot.rows_scanned_total.add(n);
    }

    /// Add to `skaidb_rows_written_total` (rows affected by a write).
    pub fn add_rows_written(&self, n: u64) {
        self.hot.rows_written_total.add(n);
    }

    /// Add to `skaidb_bytes_returned_total{endpoint=…}`.
    pub fn add_bytes_returned(&self, endpoint: Endpoint, n: u64) {
        self.hot.bytes_returned_total[endpoint as usize].add(n);
    }

    /// Count one statement slower than `slow_query_ms` (`skaidb_slow_queries_total`).
    pub fn incr_slow_query(&self) {
        self.hot.slow_queries_total.add(1);
    }

    /// Record one served REST request: count + duration (µs), per path class.
    pub fn observe_rest(&self, path: RestPath, seconds: f64) {
        self.hot.rest_requests_total[path as usize].add(1);
        self.hot.rest_request_duration_us[path as usize]
            .add((seconds * 1_000_000.0) as u64);
    }

    /// Per-class REST activity for the status tab:
    /// `(label, requests, avg_ms)` for every class that has served at
    /// least one request.
    pub fn rest_stats(&self) -> Vec<(&'static str, u64, f64)> {
        RestPath::ALL
            .iter()
            .filter_map(|p| {
                let count = self.hot.rest_requests_total[*p as usize].read()?;
                if count == 0 {
                    return None;
                }
                let us = self.hot.rest_request_duration_us[*p as usize]
                    .read()
                    .unwrap_or(0);
                Some((p.label(), count, us as f64 / count as f64 / 1_000.0))
            })
            .collect()
    }

    /// Count one RBAC denial (`skaidb_authz_denied_total`).
    pub fn incr_authz_denied(&self) {
        self.hot.authz_denied_total.add(1);
    }

    /// Count one successful authentication (`skaidb_logins_total`).
    pub fn incr_login(&self) {
        self.hot.logins_total.add(1);
    }

    /// Count one rejected authentication (`skaidb_login_failures_total`).
    pub fn incr_login_failure(&self) {
        self.hot.login_failures_total.add(1);
    }

    /// Account an accepted connection: bumps `skaidb_connections_total` and
    /// `skaidb_connections_active` for `endpoint`.
    pub fn connection_opened(&self, endpoint: Endpoint) {
        self.hot.connections_total[endpoint as usize].add(1);
        self.hot.connections_active[endpoint as usize].add(1);
    }

    /// Account a closed connection: drops `skaidb_connections_active`.
    pub fn connection_closed(&self, endpoint: Endpoint) {
        self.hot.connections_active[endpoint as usize].sub_saturating(1);
    }

    /// A statement started executing (`skaidb_queries_in_flight` +1).
    pub fn inc_queries_in_flight(&self) {
        self.hot.queries_in_flight.add(1);
    }

    /// A statement finished executing (`skaidb_queries_in_flight` -1).
    pub fn dec_queries_in_flight(&self) {
        self.hot.queries_in_flight.sub_saturating(1);
    }

    // -- string-keyed API (cold paths: startup, scrape snapshots, admin) -----

    /// Increment a counter series by one (creating it at zero first).
    pub fn incr(&self, series: &str) {
        self.add(series, 1);
    }

    /// Add `n` to a counter series.
    pub fn add(&self, series: &str, n: u64) {
        self.ensure_meta(base_of(series), MetricType::Counter);
        let mut map = self.series.lock().expect("metrics lock");
        *map.entry(series.to_string()).or_insert(0) += n;
    }

    /// Set a gauge series to an absolute value.
    pub fn set(&self, series: &str, value: u64) {
        self.ensure_meta(base_of(series), MetricType::Gauge);
        let mut map = self.series.lock().expect("metrics lock");
        map.insert(series.to_string(), value);
    }

    /// Move a gauge series by a signed delta, saturating at zero.
    pub fn gauge_add(&self, series: &str, delta: i64) {
        self.ensure_meta(base_of(series), MetricType::Gauge);
        let mut map = self.series.lock().expect("metrics lock");
        let cur = map.entry(series.to_string()).or_insert(0);
        *cur = if delta >= 0 {
            cur.saturating_add(delta as u64)
        } else {
            cur.saturating_sub((-delta) as u64)
        };
    }

    /// Increment a gauge series by one (e.g. an opened connection).
    pub fn gauge_inc(&self, series: &str) {
        self.gauge_add(series, 1);
    }

    /// Decrement a gauge series by one (e.g. a closed connection).
    pub fn gauge_dec(&self, series: &str) {
        self.gauge_add(series, -1);
    }

    /// Record one observation (e.g. a query latency in seconds) into a histogram.
    pub fn observe(&self, series: &str, value: f64) {
        self.ensure_meta(base_of(series), MetricType::Histogram);
        let mut map = self.hist.lock().expect("metrics hist");
        let h = map.entry(series.to_string()).or_default();
        for (i, &bound) in BUCKETS.iter().enumerate() {
            if value <= bound {
                h.buckets[i] += 1;
            }
        }
        h.sum += value;
        h.count += 1;
    }

    /// Current value of a counter/gauge series (0 if unset), merging the hot
    /// cells with the string-keyed map. Mainly for tests.
    pub fn get(&self, series: &str) -> u64 {
        let mut value = *self
            .series
            .lock()
            .expect("metrics lock")
            .get(series)
            .unwrap_or(&0);
        for (name, cell) in self.hot.counters() {
            if name == series {
                if let Some(v) = cell.read() {
                    value += v;
                }
            }
        }
        for (name, cell) in self.hot.gauges() {
            if name == series {
                if let Some(v) = cell.read() {
                    value = v;
                }
            }
        }
        value
    }

    /// Render the Prometheus text exposition format. Each base metric emits a
    /// `# HELP` and `# TYPE` line (with the correct type) followed by its series.
    pub fn render(&self) -> String {
        let mut series = self.series.lock().expect("metrics lock").clone();
        self.hot.merge_scalars(&mut series);
        let mut hist = self.hist.lock().expect("metrics hist").clone();
        self.hot.merge_hists(&mut hist);
        let meta = self.meta.lock().expect("metrics meta");

        let mut out = String::new();
        for (base, m) in meta.iter() {
            // Only emit a family that actually has at least one series.
            let cg: Vec<(&String, &u64)> = series
                .iter()
                .filter(|(s, _)| base_of(s) == base)
                .collect();
            let hs: Vec<(&String, &Hist)> =
                hist.iter().filter(|(s, _)| base_of(s) == base).collect();
            if cg.is_empty() && hs.is_empty() {
                continue;
            }

            if !m.help.is_empty() {
                out.push_str(&format!("# HELP {base} {}\n", m.help));
            }
            out.push_str(&format!("# TYPE {base} {}\n", m.kind.as_str()));

            match m.kind {
                MetricType::Counter | MetricType::Gauge => {
                    for (s, v) in cg {
                        out.push_str(&format!("{s} {v}\n"));
                    }
                }
                MetricType::Histogram => {
                    for (s, h) in hs {
                        let (name, labels) = split_series(s);
                        for (i, &bound) in BUCKETS.iter().enumerate() {
                            out.push_str(&format!(
                                "{name}_bucket{} {}\n",
                                with_label(labels, &format!("le=\"{}\"", fmt_f64(bound))),
                                h.buckets[i]
                            ));
                        }
                        out.push_str(&format!(
                            "{name}_bucket{} {}\n",
                            with_label(labels, "le=\"+Inf\""),
                            h.count
                        ));
                        out.push_str(&format!(
                            "{name}_sum{} {}\n",
                            label_block(labels),
                            fmt_f64(h.sum)
                        ));
                        out.push_str(&format!(
                            "{name}_count{} {}\n",
                            label_block(labels),
                            h.count
                        ));
                    }
                }
            }
        }
        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics::new()
    }
}

/// The base metric name: everything before the first `{`.
fn base_of(series: &str) -> &str {
    series.split('{').next().unwrap_or(series)
}

/// Split a series into `(name, labels_inner)` where `labels_inner` is the label
/// list without the surrounding braces (empty when there are no labels).
fn split_series(series: &str) -> (&str, &str) {
    match series.split_once('{') {
        Some((name, rest)) => (name, rest.strip_suffix('}').unwrap_or(rest)),
        None => (series, ""),
    }
}

/// Render a label block `{...}` for `inner` (empty string when there are none).
fn label_block(inner: &str) -> String {
    if inner.is_empty() {
        String::new()
    } else {
        format!("{{{inner}}}")
    }
}

/// Render a label block adding `extra` to any existing labels in `inner`.
fn with_label(inner: &str, extra: &str) -> String {
    if inner.is_empty() {
        format!("{{{extra}}}")
    } else {
        format!("{{{inner},{extra}}}")
    }
}

/// Format an f64 without scientific notation, trimming a trailing `.0`.
fn fmt_f64(v: f64) -> String {
    if v == v.trunc() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        let s = format!("{v:.6}");
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    }
}

/// Well-known metrics: `(base_name, type, help)`. Registered up front so the
/// exposition carries correct types and documentation. The full contract lives
/// in `docs/METRICS.md`.
const KNOWN_METRICS: &[(&str, MetricType, &str)] = &[
    (
        "skaidb_rest_requests_total",
        MetricType::Counter,
        "REST requests served, by path class (query/insert/es/prom/ui/ops/admin/other).",
    ),
    (
        "skaidb_rest_request_duration_us_total",
        MetricType::Counter,
        "Total time serving REST requests, microseconds, by path class (divide by \
         skaidb_rest_requests_total for the average).",
    ),
    ("skaidb_up", MetricType::Gauge, "1 if the server is up."),
    (
        "skaidb_build_info",
        MetricType::Gauge,
        "Build metadata; always 1, labelled with version/git_sha/rustc.",
    ),
    (
        "skaidb_node_info",
        MetricType::Gauge,
        "Node identity; always 1, labelled with node_id and role.",
    ),
    (
        "skaidb_start_time_seconds",
        MetricType::Gauge,
        "Unix time the process started.",
    ),
    (
        "skaidb_uptime_seconds",
        MetricType::Gauge,
        "Seconds since the process started.",
    ),
    (
        "skaidb_queries_total",
        MetricType::Counter,
        "Statements executed, by type.",
    ),
    (
        "skaidb_query_duration_seconds",
        MetricType::Histogram,
        "Statement execution latency in seconds, by type.",
    ),
    (
        "skaidb_queries_in_flight",
        MetricType::Gauge,
        "Statements currently executing.",
    ),
    (
        "skaidb_query_errors_total",
        MetricType::Counter,
        "Failed statements, by error class.",
    ),
    (
        "skaidb_rows_scanned_total",
        MetricType::Counter,
        "Rows examined while serving queries.",
    ),
    (
        "skaidb_rows_returned_total",
        MetricType::Counter,
        "Rows returned to clients.",
    ),
    (
        "skaidb_rows_written_total",
        MetricType::Counter,
        "Rows written (inserted/updated/deleted) by mutations.",
    ),
    (
        "skaidb_bytes_returned_total",
        MetricType::Counter,
        "Bytes of result data returned to clients.",
    ),
    (
        "skaidb_slow_queries_total",
        MetricType::Counter,
        "Statements slower than slow_query_ms.",
    ),
    (
        "skaidb_authz_denied_total",
        MetricType::Counter,
        "Statements denied by RBAC.",
    ),
    (
        "skaidb_logins_total",
        MetricType::Counter,
        "Successful authentications.",
    ),
    (
        "skaidb_login_failures_total",
        MetricType::Counter,
        "Rejected authentications.",
    ),
    (
        "skaidb_admin_total",
        MetricType::Counter,
        "Admin control-plane operations, by op.",
    ),
    (
        "skaidb_transactions_total",
        MetricType::Counter,
        "Transaction control statements, by kind (begin/commit/rollback).",
    ),
    (
        "skaidb_connections_active",
        MetricType::Gauge,
        "Open client connections, by endpoint (binary/rest).",
    ),
    (
        "skaidb_connections_total",
        MetricType::Counter,
        "Client connections accepted, by endpoint.",
    ),
    // ---- storage (populated at scrape time from the engine snapshot) ----
    (
        "skaidb_storage_tables",
        MetricType::Gauge,
        "Number of tables in the catalog.",
    ),
    (
        "skaidb_storage_indexes",
        MetricType::Gauge,
        "Number of secondary indexes.",
    ),
    (
        "skaidb_storage_memtable_bytes",
        MetricType::Gauge,
        "Approximate live memtable footprint across all engines.",
    ),
    (
        "skaidb_storage_sstables",
        MetricType::Gauge,
        "On-disk SSTable count across all engines.",
    ),
    (
        "skaidb_storage_disk_bytes",
        MetricType::Gauge,
        "On-disk bytes across all SSTables.",
    ),
    (
        "skaidb_storage_compactions_total",
        MetricType::Counter,
        "Compaction passes completed.",
    ),
    (
        "skaidb_storage_compaction_bytes_total",
        MetricType::Counter,
        "Bytes written by compaction.",
    ),
    (
        "skaidb_wal_bytes",
        MetricType::Gauge,
        "Live write-ahead log size in bytes.",
    ),
    (
        "skaidb_wal_fsyncs_total",
        MetricType::Counter,
        "WAL fsyncs issued.",
    ),
    (
        "skaidb_cache_hits_total",
        MetricType::Counter,
        "Read-cache hits.",
    ),
    (
        "skaidb_cache_misses_total",
        MetricType::Counter,
        "Read-cache misses.",
    ),
    (
        "skaidb_cache_evictions_total",
        MetricType::Counter,
        "Read-cache evictions.",
    ),
    (
        "skaidb_cache_entries",
        MetricType::Gauge,
        "Live read-cache entries.",
    ),
    (
        "skaidb_bloom_negative_lookups_total",
        MetricType::Counter,
        "Point reads resolved absent by the Bloom/SSTable layer.",
    ),
    (
        "skaidb_table_live_keys",
        MetricType::Gauge,
        "Live keys per table (opt-in per-table metric).",
    ),
    (
        "skaidb_table_tombstones",
        MetricType::Gauge,
        "Tombstones per table (opt-in per-table metric).",
    ),
    (
        "skaidb_table_disk_bytes",
        MetricType::Gauge,
        "On-disk bytes per table, all table kinds (opt-in per-table metric).",
    ),
    (
        "skaidb_ts_table_series",
        MetricType::Gauge,
        "Series per time-series table (opt-in per-table metric).",
    ),
    (
        "skaidb_ts_table_samples_appended_total",
        MetricType::Counter,
        "Samples appended per time-series table (opt-in per-table metric).",
    ),
    (
        "skaidb_ts_table_samples_rejected_total",
        MetricType::Counter,
        "Samples rejected (OOO/series-limit) per time-series table (opt-in per-table metric).",
    ),
    // ---- vector index ----
    (
        "skaidb_vector_indexes",
        MetricType::Gauge,
        "Number of HNSW vector indexes.",
    ),
    (
        "skaidb_vector_indexed_total",
        MetricType::Gauge,
        "Total vectors held across HNSW indexes.",
    ),
    (
        "skaidb_vector_rebuild_seconds",
        MetricType::Gauge,
        "Time to rebuild vector indexes on the last open.",
    ),
    // ---- cluster ----
    (
        "skaidb_membership_epoch",
        MetricType::Gauge,
        "Cluster membership epoch (bumps on every ring change).",
    ),
    (
        "skaidb_cluster_members",
        MetricType::Gauge,
        "Cluster members visible from this node.",
    ),
    (
        "skaidb_cluster_resharding",
        MetricType::Gauge,
        "1 while a join/decommission dual-write window is open.",
    ),
    (
        "skaidb_cluster_writes_total",
        MetricType::Counter,
        "Coordinated writes, by consistency level.",
    ),
    (
        "skaidb_cluster_reads_total",
        MetricType::Counter,
        "Coordinated reads, by consistency level.",
    ),
    (
        "skaidb_cluster_quorum_failures_total",
        MetricType::Counter,
        "Operations that failed to reach quorum, by kind (read/write).",
    ),
    (
        "skaidb_cluster_read_repairs_total",
        MetricType::Counter,
        "Read-repair writes pushed to lagging replicas.",
    ),
    (
        "skaidb_cluster_hints_stored_total",
        MetricType::Counter,
        "Hinted-handoff writes buffered for unreachable replicas.",
    ),
    (
        "skaidb_cluster_hints_replayed_total",
        MetricType::Counter,
        "Hinted-handoff writes successfully replayed.",
    ),
    (
        "skaidb_cluster_hints_pending",
        MetricType::Gauge,
        "Hinted-handoff writes currently buffered.",
    ),
    (
        "skaidb_cluster_peer_requests_total",
        MetricType::Counter,
        "Internode RPCs issued by the coordinator.",
    ),
    (
        "skaidb_cluster_peer_errors_total",
        MetricType::Counter,
        "Internode RPCs that errored or timed out.",
    ),
    (
        "skaidb_cluster_hints_pending_peer",
        MetricType::Gauge,
        "Hinted-handoff writes currently buffered, per peer (exact backlog).",
    ),
    (
        "skaidb_cluster_replication_lag_ms",
        MetricType::Gauge,
        "Approx. ms between this node's HLC frontier and the latest write it has \
         confirmed a peer applied (0/absent until a write is confirmed).",
    ),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_renders() {
        let m = Metrics::new();
        m.incr("skaidb_queries_total{type=\"select\"}");
        m.incr("skaidb_queries_total{type=\"select\"}");
        m.incr("skaidb_queries_total{type=\"insert\"}");
        m.set("skaidb_up", 1);

        assert_eq!(m.get("skaidb_queries_total{type=\"select\"}"), 2);
        let text = m.render();
        assert!(text.contains("# TYPE skaidb_queries_total counter"));
        assert!(text.contains("# HELP skaidb_queries_total Statements executed, by type."));
        assert!(text.contains("skaidb_queries_total{type=\"select\"} 2"));
        assert!(text.contains("skaidb_queries_total{type=\"insert\"} 1"));
        assert!(text.contains("skaidb_up 1"));
    }

    #[test]
    fn hot_counters_render_like_string_counters() {
        let m = Metrics::new();
        m.incr_query(QueryType::Select);
        m.incr_query(QueryType::Select);
        m.incr_query(QueryType::Insert);
        m.incr_query_error(ErrorClass::Parse);
        m.incr_transaction(TxKind::Begin);
        m.add_rows_returned(3);
        m.add_bytes_returned(Endpoint::Rest, 128);

        assert_eq!(m.get("skaidb_queries_total{type=\"select\"}"), 2);
        let text = m.render();
        assert!(text.contains("skaidb_queries_total{type=\"select\"} 2"));
        assert!(text.contains("skaidb_queries_total{type=\"insert\"} 1"));
        assert!(text.contains("skaidb_query_errors_total{class=\"parse\"} 1"));
        assert!(text.contains("skaidb_transactions_total{kind=\"begin\"} 1"));
        assert!(text.contains("skaidb_rows_returned_total 3"));
        assert!(text.contains("skaidb_bytes_returned_total{endpoint=\"rest\"} 128"));
        // Untouched hot series stay out of the exposition.
        assert!(!text.contains("skaidb_queries_total{type=\"delete\"}"));
        assert!(!text.contains("skaidb_logins_total"));
    }

    #[test]
    fn up_is_a_gauge_not_a_counter() {
        let m = Metrics::new();
        m.set("skaidb_up", 1);
        let text = m.render();
        assert!(text.contains("# TYPE skaidb_up gauge"), "got: {text}");
        assert!(!text.contains("# TYPE skaidb_up counter"));
    }

    #[test]
    fn gauge_inc_dec_saturates() {
        let m = Metrics::new();
        m.gauge_inc("skaidb_connections_active{endpoint=\"rest\"}");
        m.gauge_inc("skaidb_connections_active{endpoint=\"rest\"}");
        m.gauge_dec("skaidb_connections_active{endpoint=\"rest\"}");
        assert_eq!(m.get("skaidb_connections_active{endpoint=\"rest\"}"), 1);
        // Saturates at zero, never wraps.
        m.gauge_dec("skaidb_connections_active{endpoint=\"rest\"}");
        m.gauge_dec("skaidb_connections_active{endpoint=\"rest\"}");
        assert_eq!(m.get("skaidb_connections_active{endpoint=\"rest\"}"), 0);
    }

    #[test]
    fn hot_gauges_saturate_and_render() {
        let m = Metrics::new();
        m.connection_opened(Endpoint::Rest);
        m.connection_opened(Endpoint::Rest);
        m.connection_closed(Endpoint::Rest);
        assert_eq!(m.get("skaidb_connections_active{endpoint=\"rest\"}"), 1);
        assert_eq!(m.get("skaidb_connections_total{endpoint=\"rest\"}"), 2);
        // Saturates at zero, never wraps.
        m.connection_closed(Endpoint::Rest);
        m.connection_closed(Endpoint::Rest);
        assert_eq!(m.get("skaidb_connections_active{endpoint=\"rest\"}"), 0);
        let text = m.render();
        assert!(text.contains("skaidb_connections_active{endpoint=\"rest\"} 0"));
        assert!(text.contains("skaidb_connections_total{endpoint=\"rest\"} 2"));

        m.inc_queries_in_flight();
        m.dec_queries_in_flight();
        m.dec_queries_in_flight();
        assert_eq!(m.get("skaidb_queries_in_flight"), 0);
    }

    #[test]
    fn type_line_emitted_once_per_base() {
        let m = Metrics::new();
        m.incr("skaidb_queries_total{type=\"a\"}");
        m.incr("skaidb_queries_total{type=\"b\"}");
        let text = m.render();
        assert_eq!(
            text.matches("# TYPE skaidb_queries_total counter").count(),
            1
        );
    }

    #[test]
    fn histogram_renders_buckets_sum_and_count() {
        let m = Metrics::new();
        m.observe("skaidb_query_duration_seconds{type=\"select\"}", 0.003);
        m.observe("skaidb_query_duration_seconds{type=\"select\"}", 0.2);
        let text = m.render();
        assert!(text.contains("# TYPE skaidb_query_duration_seconds histogram"));
        // 0.003 <= 0.005 bucket, both <= 0.25 bucket.
        assert!(
            text.contains("skaidb_query_duration_seconds_bucket{type=\"select\",le=\"0.005\"} 1"),
            "got: {text}"
        );
        assert!(text
            .contains("skaidb_query_duration_seconds_bucket{type=\"select\",le=\"+Inf\"} 2"));
        assert!(text.contains("skaidb_query_duration_seconds_count{type=\"select\"} 2"));
        assert!(text.contains("skaidb_query_duration_seconds_sum{type=\"select\"}"));
    }

    #[test]
    fn hot_histogram_renders_identically() {
        let m = Metrics::new();
        m.observe_query_duration(QueryType::Select, 0.003);
        m.observe_query_duration(QueryType::Select, 0.2);
        let text = m.render();
        assert!(text.contains("# TYPE skaidb_query_duration_seconds histogram"));
        // 0.003 <= 0.005 bucket, both <= 0.25 bucket (cumulative form).
        assert!(
            text.contains("skaidb_query_duration_seconds_bucket{type=\"select\",le=\"0.005\"} 1"),
            "got: {text}"
        );
        assert!(
            text.contains("skaidb_query_duration_seconds_bucket{type=\"select\",le=\"0.25\"} 2"),
            "got: {text}"
        );
        assert!(text
            .contains("skaidb_query_duration_seconds_bucket{type=\"select\",le=\"+Inf\"} 2"));
        assert!(text.contains("skaidb_query_duration_seconds_count{type=\"select\"} 2"));
        assert!(text.contains("skaidb_query_duration_seconds_sum{type=\"select\"} 0.203"));
        // An observation past the last bound lands only in +Inf/_count.
        m.observe_query_duration(QueryType::Select, 60.0);
        let text = m.render();
        assert!(text.contains("skaidb_query_duration_seconds_bucket{type=\"select\",le=\"10\"} 2"));
        assert!(text
            .contains("skaidb_query_duration_seconds_bucket{type=\"select\",le=\"+Inf\"} 3"));
    }
}
