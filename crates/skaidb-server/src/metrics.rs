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

use std::collections::BTreeMap;
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

/// A registry of counters, gauges, and histograms.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Counter and gauge values, keyed by full series (name + labels).
    series: Mutex<BTreeMap<String, u64>>,
    /// Histogram accumulators, keyed by full series (name + labels, no `le`).
    hist: Mutex<BTreeMap<String, Hist>>,
    /// Type + help, keyed by base metric name.
    meta: Mutex<BTreeMap<String, Meta>>,
}

impl Metrics {
    pub fn new() -> Self {
        let m = Metrics::default();
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

    /// Current value of a counter/gauge series (0 if unset). Mainly for tests.
    pub fn get(&self, series: &str) -> u64 {
        *self
            .series
            .lock()
            .expect("metrics lock")
            .get(series)
            .unwrap_or(&0)
    }

    /// Render the Prometheus text exposition format. Each base metric emits a
    /// `# HELP` and `# TYPE` line (with the correct type) followed by its series.
    pub fn render(&self) -> String {
        let series = self.series.lock().expect("metrics lock");
        let hist = self.hist.lock().expect("metrics hist");
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
        "On-disk bytes per table (opt-in per-table metric).",
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
}
