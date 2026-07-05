//! PromQL subset + Prometheus HTTP query API (docs/TODO.md phase 7).
//!
//! Enough of PromQL for typical Grafana dashboards: instant selectors with
//! label matchers, `rate`/`increase`/`delta` over range selectors, and
//! `sum/avg/min/max/count [by|without (...)]` aggregation — evaluated over
//! the `metrics` time-series table remote_write ingests into (the metric
//! name is the `name` label). Not supported (v1): regex matchers, `offset`,
//! subqueries, arithmetic between vectors, `histogram_quantile`.
//!
//! Responses follow the Prometheus HTTP API: timestamps in (float) seconds,
//! sample values as strings, `resultType` of `vector` (instant) or `matrix`
//! (range).

use std::collections::BTreeMap;

use serde_json::{json, Value as Json};
use skaidb_tsdb::{Labels, Matcher, Sample};

use crate::shared::Shared;

/// The table PromQL evaluates over (the remote_write ingest table).
const TABLE: &str = "metrics";
/// Instant selectors look back this far for a series' latest sample.
const LOOKBACK_MS: i64 = 5 * 60 * 1000;

// ---- expression AST ----

#[derive(Debug, Clone, PartialEq)]
enum PExpr {
    /// `metric{l="v", l2!="w"}` with an optional `[range]`.
    Selector {
        matchers: Vec<Matcher>,
        range_ms: Option<i64>,
    },
    /// `rate(sel[5m])` / `increase(...)` / `delta(...)`.
    RangeFn { func: RangeFn, arg: Box<PExpr> },
    /// `sum by (a, b) (expr)` and friends.
    Agg {
        op: AggOp,
        by: Option<Vec<String>>,
        without: Option<Vec<String>>,
        arg: Box<PExpr>,
    },
    /// A bare number.
    Number(f64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum RangeFn {
    Rate,
    Increase,
    Delta,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum AggOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

// ---- parser ----

struct P<'a> {
    s: &'a [u8],
    i: usize,
}

impl<'a> P<'a> {
    fn new(s: &'a str) -> P<'a> {
        P { s: s.as_bytes(), i: 0 }
    }

    fn ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.ws();
        self.s.get(self.i).copied()
    }

    fn eat(&mut self, b: u8) -> bool {
        if self.peek() == Some(b) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, b: u8) -> Result<(), String> {
        if self.eat(b) {
            Ok(())
        } else {
            Err(format!("expected '{}' at position {}", b as char, self.i))
        }
    }

    fn ident(&mut self) -> Option<String> {
        self.ws();
        let start = self.i;
        while self.i < self.s.len()
            && (self.s[self.i].is_ascii_alphanumeric()
                || self.s[self.i] == b'_'
                || self.s[self.i] == b':')
        {
            self.i += 1;
        }
        if self.i == start {
            None
        } else {
            Some(String::from_utf8_lossy(&self.s[start..self.i]).into_owned())
        }
    }

    fn string(&mut self) -> Result<String, String> {
        let quote = self.peek().ok_or("unexpected end in string")?;
        if quote != b'"' && quote != b'\'' {
            return Err("expected quoted string".into());
        }
        self.i += 1;
        let mut out = String::new();
        while let Some(&b) = self.s.get(self.i) {
            self.i += 1;
            if b == quote {
                return Ok(out);
            }
            if b == b'\\' {
                if let Some(&esc) = self.s.get(self.i) {
                    self.i += 1;
                    out.push(match esc {
                        b'n' => '\n',
                        b't' => '\t',
                        other => other as char,
                    });
                    continue;
                }
            }
            out.push(b as char);
        }
        Err("unterminated string".into())
    }

    /// `[5m]` → milliseconds.
    fn duration(&mut self) -> Result<i64, String> {
        self.ws();
        let start = self.i;
        while self.i < self.s.len() && self.s[self.i].is_ascii_digit() {
            self.i += 1;
        }
        let n: i64 = std::str::from_utf8(&self.s[start..self.i])
            .unwrap_or("")
            .parse()
            .map_err(|_| "bad duration".to_string())?;
        let unit_start = self.i;
        while self.i < self.s.len() && self.s[self.i].is_ascii_alphabetic() {
            self.i += 1;
        }
        let per = match &self.s[unit_start..self.i] {
            b"ms" => 1,
            b"s" => 1000,
            b"m" => 60_000,
            b"h" => 3_600_000,
            b"d" => 86_400_000,
            b"w" => 7 * 86_400_000,
            _ => return Err("bad duration unit".into()),
        };
        Ok(n * per)
    }

    fn parse(&mut self) -> Result<PExpr, String> {
        let expr = self.expr()?;
        self.ws();
        if self.i < self.s.len() {
            return Err(format!(
                "unsupported trailing input: {:?}",
                String::from_utf8_lossy(&self.s[self.i..])
            ));
        }
        Ok(expr)
    }

    fn expr(&mut self) -> Result<PExpr, String> {
        self.ws();
        // Number literal.
        if self
            .peek()
            .is_some_and(|b| b.is_ascii_digit() || b == b'-' || b == b'.')
        {
            let start = self.i;
            if self.s[self.i] == b'-' {
                self.i += 1;
            }
            while self
                .s
                .get(self.i)
                .is_some_and(|b| b.is_ascii_digit() || *b == b'.' || *b == b'e' || *b == b'E')
            {
                self.i += 1;
            }
            let text = std::str::from_utf8(&self.s[start..self.i]).unwrap_or("");
            return text
                .parse()
                .map(PExpr::Number)
                .map_err(|_| format!("bad number {text:?}"));
        }
        let name = self.ident().ok_or("expected expression")?;
        // Aggregations.
        let agg = match name.as_str() {
            "sum" => Some(AggOp::Sum),
            "avg" => Some(AggOp::Avg),
            "min" => Some(AggOp::Min),
            "max" => Some(AggOp::Max),
            "count" => Some(AggOp::Count),
            _ => None,
        };
        if let Some(op) = agg {
            let (mut by, mut without) = (None, None);
            self.ws();
            if self.peek() != Some(b'(') {
                let modifier = self.ident().ok_or("expected 'by', 'without' or '('")?;
                let list = {
                    self.expect(b'(')?;
                    let mut ls = Vec::new();
                    if !self.eat(b')') {
                        loop {
                            ls.push(self.ident().ok_or("expected label")?);
                            if !self.eat(b',') {
                                break;
                            }
                        }
                        self.expect(b')')?;
                    }
                    ls
                };
                match modifier.as_str() {
                    "by" => by = Some(list),
                    "without" => without = Some(list),
                    other => return Err(format!("unsupported modifier {other}")),
                }
            }
            self.expect(b'(')?;
            let arg = self.expr()?;
            self.expect(b')')?;
            return Ok(PExpr::Agg {
                op,
                by,
                without,
                arg: Box::new(arg),
            });
        }
        // Range functions.
        let func = match name.as_str() {
            "rate" => Some(RangeFn::Rate),
            "increase" => Some(RangeFn::Increase),
            "delta" => Some(RangeFn::Delta),
            _ => None,
        };
        if let Some(func) = func {
            self.expect(b'(')?;
            let arg = self.expr()?;
            self.expect(b')')?;
            return Ok(PExpr::RangeFn {
                func,
                arg: Box::new(arg),
            });
        }
        // Selector: name is the metric.
        let mut matchers = vec![Matcher::Eq("name".into(), name)];
        if self.eat(b'{') && !self.eat(b'}') {
            {
                loop {
                    let label = self.ident().ok_or("expected label name")?;
                    self.ws();
                    let negated = if self.eat(b'=') {
                        // `=~` / `!~` are regex forms — unsupported.
                        if self.peek() == Some(b'~') {
                            return Err("regex matchers are not supported yet".into());
                        }
                        false
                    } else if self.eat(b'!') {
                        self.expect(b'=')?;
                        true
                    } else {
                        return Err("expected '=' or '!='".into());
                    };
                    let value = self.string()?;
                    matchers.push(if negated {
                        Matcher::Ne(label, value)
                    } else {
                        Matcher::Eq(label, value)
                    });
                    if !self.eat(b',') {
                        break;
                    }
                }
                self.expect(b'}')?;
            }
        }
        let mut range_ms = None;
        if self.eat(b'[') {
            range_ms = Some(self.duration()?);
            self.expect(b']')?;
        }
        Ok(PExpr::Selector { matchers, range_ms })
    }
}

// ---- evaluation ----

/// One output series: its labels and per-step values (NaN = absent).
type Vector = Vec<(Labels, f64)>;

/// Pre-fetched data for the expression's selector.
struct Fetched {
    series: Vec<(Labels, Vec<Sample>)>,
}

/// The single selector inside `expr` (v1 supports one data source per query).
fn selector_of(expr: &PExpr) -> Result<(&Vec<Matcher>, Option<i64>), String> {
    match expr {
        PExpr::Selector { matchers, range_ms } => Ok((matchers, *range_ms)),
        PExpr::RangeFn { arg, .. } | PExpr::Agg { arg, .. } => selector_of(arg),
        PExpr::Number(_) => Err("number-only expressions are not supported".into()),
    }
}

/// Evaluate `expr` at each step in `steps` (ms). Returns per-step vectors.
fn eval_steps(expr: &PExpr, fetched: &Fetched, steps: &[i64]) -> Result<Vec<Vector>, String> {
    match expr {
        PExpr::Number(_) => Err("number-only expressions are not supported".into()),
        PExpr::Selector { range_ms, .. } => {
            if range_ms.is_some() {
                return Err("range selectors need rate()/increase()/delta()".into());
            }
            Ok(steps
                .iter()
                .map(|&t| {
                    let mut v = Vec::new();
                    for (labels, samples) in &fetched.series {
                        // Latest sample at or before t, within the lookback.
                        let idx = samples.partition_point(|s| s.ts <= t);
                        if idx > 0 {
                            let s = &samples[idx - 1];
                            if t - s.ts <= LOOKBACK_MS {
                                v.push((clean_labels(labels, true), s.value));
                            }
                        }
                    }
                    v
                })
                .collect())
        }
        PExpr::RangeFn { func, arg } => {
            let PExpr::Selector {
                range_ms: Some(window),
                ..
            } = arg.as_ref()
            else {
                return Err("rate()/increase()/delta() need a range selector like m[5m]".into());
            };
            let window = *window;
            Ok(steps
                .iter()
                .map(|&t| {
                    let mut v = Vec::new();
                    for (labels, samples) in &fetched.series {
                        let lo = samples.partition_point(|s| s.ts < t - window);
                        let hi = samples.partition_point(|s| s.ts <= t);
                        let win = &samples[lo..hi];
                        if win.len() < 2 {
                            continue;
                        }
                        let change = match func {
                            RangeFn::Delta => win[win.len() - 1].value - win[0].value,
                            _ => {
                                // Counter-reset-aware increase.
                                let mut inc = 0.0;
                                let mut prev = win[0].value;
                                for s in &win[1..] {
                                    inc += if s.value >= prev {
                                        s.value - prev
                                    } else {
                                        s.value
                                    };
                                    prev = s.value;
                                }
                                inc
                            }
                        };
                        let value = if *func == RangeFn::Rate {
                            change / (window as f64 / 1000.0)
                        } else {
                            change
                        };
                        // rate() drops the metric name, PromQL-style.
                        v.push((clean_labels(labels, false), value));
                    }
                    v
                })
                .collect())
        }
        PExpr::Agg {
            op,
            by,
            without,
            arg,
        } => {
            let inner = eval_steps(arg, fetched, steps)?;
            Ok(inner
                .into_iter()
                .map(|vector| {
                    let mut groups: BTreeMap<Labels, Vec<f64>> = BTreeMap::new();
                    for (labels, value) in vector {
                        let key: Labels = match (by, without) {
                            (Some(by), _) => labels
                                .iter()
                                .filter(|(k, _)| by.contains(k))
                                .cloned()
                                .collect(),
                            (None, Some(wo)) => labels
                                .iter()
                                .filter(|(k, _)| !wo.contains(k))
                                .cloned()
                                .collect(),
                            (None, None) => Vec::new(),
                        };
                        groups.entry(key).or_default().push(value);
                    }
                    groups
                        .into_iter()
                        .map(|(labels, values)| {
                            let value = match op {
                                AggOp::Sum => values.iter().sum(),
                                AggOp::Avg => values.iter().sum::<f64>() / values.len() as f64,
                                AggOp::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
                                AggOp::Max => {
                                    values.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
                                }
                                AggOp::Count => values.len() as f64,
                            };
                            (labels, value)
                        })
                        .collect()
                })
                .collect())
        }
    }
}

/// Output labels: drop skaidb-internal pairs; `name` renders as `__name__`
/// (kept for plain selectors, dropped after rate(), PromQL-style).
fn clean_labels(labels: &Labels, keep_name: bool) -> Labels {
    let mut out = Vec::with_capacity(labels.len());
    for (k, v) in labels {
        if k.starts_with("__") {
            continue;
        }
        if k == "name" {
            if keep_name {
                out.push(("__name__".to_string(), v.clone()));
            }
            continue;
        }
        out.push((k.clone(), v.clone()));
    }
    out.sort();
    out
}

fn labels_json(labels: &Labels) -> Json {
    Json::Object(
        labels
            .iter()
            .map(|(k, v)| (k.clone(), Json::String(v.clone())))
            .collect(),
    )
}

// ---- HTTP entry points ----

fn fetch(ctx: &Shared, expr: &PExpr, t0: i64, t1: i64) -> Result<Fetched, String> {
    let (matchers, _) = selector_of(expr)?;
    // The evaluator needs history behind the first step: the range window
    // (or instant lookback), whichever the expression uses.
    let (_, range) = selector_of(expr)?;
    let back = range.unwrap_or(LOOKBACK_MS);
    let series = ctx
        .backend
        .ts_query(TABLE, matchers, t0.saturating_sub(back), t1)
        .map_err(|e| e.to_string())?;
    Ok(Fetched { series })
}

/// `/api/v1/query`: evaluate at one instant.
pub fn query(ctx: &Shared, params: &BTreeMap<String, String>) -> (u16, Json) {
    let Some(q) = params.get("query") else {
        return err_json("missing query parameter");
    };
    let t = params
        .get("time")
        .and_then(|t| parse_prom_time(t))
        .unwrap_or_else(wall_ms);
    let expr = match P::new(q).parse() {
        Ok(e) => e,
        Err(e) => return err_json(&e),
    };
    let fetched = match fetch(ctx, &expr, t, t) {
        Ok(f) => f,
        Err(e) => return err_json(&e),
    };
    match eval_steps(&expr, &fetched, &[t]) {
        Ok(mut vectors) => {
            let vector = vectors.pop().unwrap_or_default();
            let result: Vec<Json> = vector
                .into_iter()
                .filter(|(_, v)| v.is_finite())
                .map(|(labels, value)| {
                    json!({
                        "metric": labels_json(&labels),
                        "value": [t as f64 / 1000.0, format!("{value}")],
                    })
                })
                .collect();
            (
                200,
                json!({"status": "success",
                       "data": {"resultType": "vector", "result": result}}),
            )
        }
        Err(e) => err_json(&e),
    }
}

/// `/api/v1/query_range`: evaluate over `start..=end` at `step`.
pub fn query_range(ctx: &Shared, params: &BTreeMap<String, String>) -> (u16, Json) {
    let Some(q) = params.get("query") else {
        return err_json("missing query parameter");
    };
    let (Some(start), Some(end)) = (
        params.get("start").and_then(|t| parse_prom_time(t)),
        params.get("end").and_then(|t| parse_prom_time(t)),
    ) else {
        return err_json("missing start/end");
    };
    let step_ms = params
        .get("step")
        .and_then(|s| parse_prom_duration(s))
        .unwrap_or(60_000)
        .max(1);
    if end < start || (end - start) / step_ms > 50_000 {
        return err_json("bad or too-wide range");
    }
    let expr = match P::new(q).parse() {
        Ok(e) => e,
        Err(e) => return err_json(&e),
    };
    let steps: Vec<i64> = (0..).map(|i| start + i * step_ms).take_while(|t| *t <= end).collect();
    let fetched = match fetch(ctx, &expr, start, end) {
        Ok(f) => f,
        Err(e) => return err_json(&e),
    };
    match eval_steps(&expr, &fetched, &steps) {
        Ok(vectors) => {
            // Pivot per-step vectors into per-series time series.
            let mut by_series: BTreeMap<Labels, Vec<(i64, f64)>> = BTreeMap::new();
            for (t, vector) in steps.iter().zip(vectors) {
                for (labels, value) in vector {
                    if value.is_finite() {
                        by_series.entry(labels).or_default().push((*t, value));
                    }
                }
            }
            let result: Vec<Json> = by_series
                .into_iter()
                .map(|(labels, points)| {
                    let values: Vec<Json> = points
                        .into_iter()
                        .map(|(t, v)| json!([t as f64 / 1000.0, format!("{v}")]))
                        .collect();
                    json!({"metric": labels_json(&labels), "values": values})
                })
                .collect();
            (
                200,
                json!({"status": "success",
                       "data": {"resultType": "matrix", "result": result}}),
            )
        }
        Err(e) => err_json(&e),
    }
}

/// `/api/v1/labels` and `/api/v1/label/<name>/values`.
pub fn labels(ctx: &Shared, value_of: Option<&str>) -> (u16, Json) {
    let series = match ctx.backend.ts_query(TABLE, &[], i64::MIN, i64::MAX) {
        Ok(s) => s,
        Err(e) if e.to_string().contains("does not exist") => Vec::new(),
        Err(e) => return err_json(&e.to_string()),
    };
    let mut out: Vec<String> = Vec::new();
    for (labels, _) in &series {
        for (k, v) in labels {
            if k.starts_with("__") {
                continue;
            }
            match value_of {
                None => {
                    let name = if k == "name" { "__name__" } else { k.as_str() };
                    if !out.iter().any(|o| o == name) {
                        out.push(name.to_string());
                    }
                }
                Some(want) => {
                    let k = if k == "name" { "__name__" } else { k.as_str() };
                    if k == want && !out.contains(v) {
                        out.push(v.clone());
                    }
                }
            }
        }
    }
    out.sort();
    (200, json!({"status": "success", "data": out}))
}

/// `/api/v1/series`: label sets matching `match[]` selectors.
pub fn series(ctx: &Shared, params: &BTreeMap<String, String>) -> (u16, Json) {
    let matchers = match params.get("match[]") {
        Some(sel) => match P::new(sel).parse() {
            Ok(expr) => match selector_of(&expr) {
                Ok((m, _)) => m.clone(),
                Err(e) => return err_json(&e),
            },
            Err(e) => return err_json(&e),
        },
        None => Vec::new(),
    };
    let series = match ctx.backend.ts_query(TABLE, &matchers, i64::MIN, i64::MAX) {
        Ok(s) => s,
        Err(e) if e.to_string().contains("does not exist") => Vec::new(),
        Err(e) => return err_json(&e.to_string()),
    };
    let mut seen: Vec<Labels> = Vec::new();
    for (labels, _) in &series {
        let cleaned = clean_labels(labels, true);
        if !seen.contains(&cleaned) {
            seen.push(cleaned);
        }
    }
    let result: Vec<Json> = seen.iter().map(labels_json).collect();
    (200, json!({"status": "success", "data": result}))
}

fn wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn err_json(msg: &str) -> (u16, Json) {
    (400, json!({"status": "error", "errorType": "bad_data", "error": msg}))
}

/// Prometheus time params: unix seconds (possibly fractional) or RFC3339
/// (unsupported v1 — Grafana sends unix). Returns ms.
fn parse_prom_time(s: &str) -> Option<i64> {
    s.parse::<f64>().ok().map(|secs| (secs * 1000.0) as i64)
}

/// Step: seconds (possibly fractional) or a duration like `30s`.
fn parse_prom_duration(s: &str) -> Option<i64> {
    if let Ok(secs) = s.parse::<f64>() {
        return Some((secs * 1000.0) as i64);
    }
    let mut p = P::new(s);
    p.duration().ok()
}

/// Parse `a=1&b=2` (query string or form body) with percent-decoding.
pub fn parse_params(s: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for pair in s.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        out.insert(percent_decode(k), percent_decode(v));
    }
    out
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() + 1 && i + 3 <= bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b);
                    i += 3;
                } else {
                    out.push(b'%');
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_selectors_and_functions() {
        let e = P::new(r#"sum by (job) (rate(http_requests_total{job="api",env!="dev"}[5m]))"#)
            .parse()
            .unwrap();
        let PExpr::Agg { op, by, arg, .. } = e else { panic!() };
        assert_eq!(op, AggOp::Sum);
        assert_eq!(by, Some(vec!["job".into()]));
        let PExpr::RangeFn { func, arg } = *arg else { panic!() };
        assert_eq!(func, RangeFn::Rate);
        let PExpr::Selector { matchers, range_ms } = *arg else { panic!() };
        assert_eq!(range_ms, Some(300_000));
        assert_eq!(matchers.len(), 3); // name + 2 label matchers
    }

    #[test]
    fn rejects_regex_and_trailing() {
        assert!(P::new(r#"m{l=~"x.*"}"#).parse().is_err());
        assert!(P::new("m offset 5m").parse().is_err());
    }

    #[test]
    fn percent_decoding() {
        let p = parse_params("query=rate%28m%5B5m%5D%29&time=100.5");
        assert_eq!(p["query"], "rate(m[5m])");
        assert_eq!(p["time"], "100.5");
        assert_eq!(percent_decode("a+b%20c"), "a b c");
    }
}
