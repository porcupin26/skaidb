//! Prometheus-style metrics (SPEC §10).
//!
//! A small thread-safe counter registry that renders the Prometheus text
//! exposition format. Series keys carry their labels inline, e.g.
//! `skaidb_queries_total{type="select"}`.

use std::collections::BTreeMap;
use std::sync::Mutex;

/// A registry of monotonic counters and simple gauges.
#[derive(Debug, Default)]
pub struct Metrics {
    series: Mutex<BTreeMap<String, u64>>,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics::default()
    }

    /// Increment a counter series by one (creating it at zero first).
    pub fn incr(&self, series: &str) {
        self.add(series, 1);
    }

    /// Add `n` to a counter series.
    pub fn add(&self, series: &str, n: u64) {
        let mut map = self.series.lock().expect("metrics lock");
        *map.entry(series.to_string()).or_insert(0) += n;
    }

    /// Set a gauge series to an absolute value.
    pub fn set(&self, series: &str, value: u64) {
        let mut map = self.series.lock().expect("metrics lock");
        map.insert(series.to_string(), value);
    }

    /// Current value of a series (0 if unset). Mainly for tests.
    pub fn get(&self, series: &str) -> u64 {
        *self
            .series
            .lock()
            .expect("metrics lock")
            .get(series)
            .unwrap_or(&0)
    }

    /// Render the Prometheus text exposition format, with one `# TYPE` line per
    /// base metric name (the part before any `{labels}`).
    pub fn render(&self) -> String {
        let map = self.series.lock().expect("metrics lock");
        let mut out = String::new();
        let mut last_base: Option<String> = None;
        for (series, value) in map.iter() {
            let base = series.split('{').next().unwrap_or(series).to_string();
            if last_base.as_deref() != Some(base.as_str()) {
                out.push_str(&format!("# TYPE {base} counter\n"));
                last_base = Some(base);
            }
            out.push_str(&format!("{series} {value}\n"));
        }
        out
    }
}

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
        assert!(text.contains("skaidb_queries_total{type=\"select\"} 2"));
        assert!(text.contains("skaidb_queries_total{type=\"insert\"} 1"));
        assert!(text.contains("skaidb_up 1"));
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
}
