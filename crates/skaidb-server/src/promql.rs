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

/// Which time-series table a Prometheus-API request evaluates over. The
/// default scope is the classic remote_write `metrics` table in the default
/// database; a path prefix (`/db/<database>[/table/<table>]/api/v1/*`)
/// scopes to another database — and, with `table`, to any TS table, whose
/// **fields** then become the metric names (`pm25{...}` selects the `pm25`
/// field; the remote_write `name`-label convention only applies to tables
/// named `metrics`).
pub struct Scope {
    /// Backend table name, already database-qualified.
    pub table: String,
    /// The bare table name (for permission checks / messages).
    pub bare: String,
    /// The database the scope resolves in.
    pub db: String,
    /// Generic-table mode: metric names are the table's fields (the
    /// `__field__` series label) instead of the `name` label.
    pub field_metrics: bool,
}

impl Default for Scope {
    fn default() -> Scope {
        Scope::new(skaidb_engine::DEFAULT_DATABASE, None)
    }
}

impl Scope {
    pub fn new(db: &str, table: Option<&str>) -> Scope {
        let bare = table.unwrap_or(TABLE);
        Scope {
            table: skaidb_engine::namespace::qualify(db, bare),
            bare: bare.to_string(),
            db: db.to_string(),
            field_metrics: bare != TABLE,
        }
    }

    /// The storage label a metric-name matcher targets in this scope.
    fn metric_label(&self) -> &'static str {
        if self.field_metrics {
            "__field__"
        } else {
            "name"
        }
    }
}

/// Rewrite parsed metric-name matchers (the parser emits label `name`) onto
/// the scope's metric label.
fn scope_matchers(matchers: &mut [Matcher], scope: &Scope) {
    if !scope.field_metrics {
        return;
    }
    for m in matchers {
        let label = match m {
            Matcher::Eq(l, _) | Matcher::Ne(l, _) | Matcher::Re(l, _) | Matcher::NotRe(l, _) => l,
        };
        if label == "name" {
            "__field__".clone_into(label);
        }
    }
}

/// In field-metrics mode, rename each fetched series' `__field__` label to
/// `name` — downstream (the evaluator, `clean_labels`' `name` → `__name__`
/// rendering, `rate()`'s name-dropping) then behaves exactly as it does for
/// the remote_write table.
fn normalize_series(series: &mut [(Labels, Vec<Sample>)], scope: &Scope) {
    if !scope.field_metrics {
        return;
    }
    for (labels, _) in series {
        for (k, _) in labels.iter_mut() {
            if k == "__field__" {
                "name".clone_into(k);
            }
        }
        labels.sort();
    }
}
/// Instant selectors look back this far for a series' latest sample.
const LOOKBACK_MS: i64 = 5 * 60 * 1000;

// ---- expression AST ----

#[derive(Debug, Clone, PartialEq)]
enum PExpr {
    /// `metric{l="v", l2=~"a.*"} [range] [offset d]`. `slot` indexes the
    /// pre-fetched data for this selector (assigned after parsing).
    Selector {
        matchers: Vec<Matcher>,
        range_ms: Option<i64>,
        offset_ms: i64,
        slot: usize,
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
    /// `lhs + rhs` and friends: scalar∘scalar, scalar∘vector, and
    /// vector∘vector matched one-to-one on identical label sets
    /// (ignoring `__name__`, which the result drops — PromQL semantics).
    Binary {
        op: BinOp,
        lhs: Box<PExpr>,
        rhs: Box<PExpr>,
    },
    /// `histogram_quantile(φ, expr)` over `_bucket` series with `le`.
    HistogramQuantile { phi: f64, arg: Box<PExpr> },
    /// A bare number.
    Number(f64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
}

impl BinOp {
    fn apply(self, l: f64, r: f64) -> f64 {
        match self {
            BinOp::Add => l + r,
            BinOp::Sub => l - r,
            BinOp::Mul => l * r,
            BinOp::Div => l / r,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum RangeFn {
    Rate,
    Increase,
    Delta,
    /// The `<agg>_over_time` window aggregations. Grafana's Metrics
    /// Drilldown tiles wrap gauges in `avg_over_time`, so these are load-
    /// bearing for stock dashboards, not a completeness nicety.
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    SumOverTime,
    CountOverTime,
    /// Keeps the metric name (returns a raw sample), Prometheus-style.
    LastOverTime,
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

    /// Peek-and-eat a keyword (backtracks when it does not match).
    fn keyword(&mut self, want: &str) -> bool {
        self.ws();
        let save = self.i;
        match self.ident() {
            Some(w) if w == want => true,
            _ => {
                self.i = save;
                false
            }
        }
    }

    /// `expr := term (('+'|'-') term)*`
    fn expr(&mut self) -> Result<PExpr, String> {
        let mut lhs = self.term()?;
        loop {
            let op = match self.peek() {
                Some(b'+') => BinOp::Add,
                Some(b'-') => BinOp::Sub,
                _ => return Ok(lhs),
            };
            self.i += 1;
            let rhs = self.term()?;
            lhs = PExpr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
    }

    /// `term := factor (('*'|'/') factor)*`
    fn term(&mut self) -> Result<PExpr, String> {
        let mut lhs = self.factor()?;
        loop {
            let op = match self.peek() {
                Some(b'*') => BinOp::Mul,
                Some(b'/') => BinOp::Div,
                _ => return Ok(lhs),
            };
            self.i += 1;
            let rhs = self.factor()?;
            lhs = PExpr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
    }

    fn factor(&mut self) -> Result<PExpr, String> {
        self.ws();
        // Parenthesized sub-expression.
        if self.eat(b'(') {
            let inner = self.expr()?;
            self.expect(b')')?;
            return Ok(inner);
        }
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
        // Bare label-matcher selector: `{name=~"skaidb.*"}` (no metric).
        if self.peek() == Some(b'{') {
            let mut matchers = Vec::new();
            self.matcher_block(&mut matchers)?;
            if matchers.is_empty() {
                return Err("a bare {} selector needs at least one matcher".into());
            }
            return self.selector_tail(matchers);
        }
        let name = self.ident().ok_or("expected expression")?;
        // histogram_quantile(φ, expr).
        if name == "histogram_quantile" {
            self.expect(b'(')?;
            let PExpr::Number(phi) = self.factor()? else {
                return Err("histogram_quantile needs a number as its first argument".into());
            };
            self.expect(b',')?;
            let arg = self.expr()?;
            self.expect(b')')?;
            return Ok(PExpr::HistogramQuantile {
                phi,
                arg: Box::new(arg),
            });
        }
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
            "avg_over_time" => Some(RangeFn::AvgOverTime),
            "min_over_time" => Some(RangeFn::MinOverTime),
            "max_over_time" => Some(RangeFn::MaxOverTime),
            "sum_over_time" => Some(RangeFn::SumOverTime),
            "count_over_time" => Some(RangeFn::CountOverTime),
            "last_over_time" => Some(RangeFn::LastOverTime),
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
        if self.peek() == Some(b'{') {
            self.matcher_block(&mut matchers)?;
        }
        self.selector_tail(matchers)
    }

    /// `{label op "value", ...}` — the braces and every matcher inside.
    /// `__name__` maps to the `name` label (the metric name's storage
    /// label), so `{__name__=~"skaidb.*"}` and `{name=~"skaidb.*"}` both
    /// work.
    fn matcher_block(&mut self, matchers: &mut Vec<Matcher>) -> Result<(), String> {
        if self.eat(b'{') && !self.eat(b'}') {
            {
                loop {
                    let mut label = self.ident().ok_or("expected label name")?;
                    if label == "__name__" {
                        label = "name".into();
                    }
                    self.ws();
                    enum Op {
                        Eq,
                        Ne,
                        Re,
                        NotRe,
                    }
                    let op = if self.eat(b'=') {
                        if self.eat(b'~') {
                            Op::Re
                        } else {
                            Op::Eq
                        }
                    } else if self.eat(b'!') {
                        if self.eat(b'~') {
                            Op::NotRe
                        } else {
                            self.expect(b'=')?;
                            Op::Ne
                        }
                    } else {
                        return Err("expected '=', '!=', '=~' or '!~'".into());
                    };
                    let value = self.string()?;
                    matchers.push(match op {
                        Op::Eq => Matcher::Eq(label, value),
                        Op::Ne => Matcher::Ne(label, value),
                        Op::Re => Matcher::re(label, &value).map_err(|e| e.to_string())?,
                        Op::NotRe => Matcher::not_re(label, &value).map_err(|e| e.to_string())?,
                    });
                    if !self.eat(b',') {
                        break;
                    }
                }
                self.expect(b'}')?;
            }
        }
        Ok(())
    }

    /// The optional `[range]` / `offset` tail after a selector's matchers.
    fn selector_tail(&mut self, matchers: Vec<Matcher>) -> Result<PExpr, String> {
        let mut range_ms = None;
        if self.eat(b'[') {
            range_ms = Some(self.duration()?);
            self.expect(b']')?;
        }
        let offset_ms = if self.keyword("offset") {
            self.duration()?
        } else {
            0
        };
        Ok(PExpr::Selector {
            matchers,
            range_ms,
            offset_ms,
            slot: 0,
        })
    }
}

// ---- evaluation ----

/// One output series: its labels and per-step values (NaN = absent).
type Vector = Vec<(Labels, f64)>;

/// One step's evaluation result: an instant vector, or a scalar (from a
/// number literal or scalar arithmetic).
enum StepVal {
    Scalar(f64),
    Vector(Vector),
}

/// Pre-fetched data for one selector.
struct Fetched {
    series: Vec<(Labels, Vec<Sample>)>,
}

/// Assign each selector a fetch slot (pre-order) and return the fetch
/// specs `(matchers, range, offset)` in slot order.
fn assign_slots(expr: &mut PExpr, specs: &mut Vec<(Vec<Matcher>, Option<i64>, i64)>) {
    match expr {
        PExpr::Selector {
            matchers,
            range_ms,
            offset_ms,
            slot,
        } => {
            *slot = specs.len();
            specs.push((matchers.clone(), *range_ms, *offset_ms));
        }
        PExpr::RangeFn { arg, .. }
        | PExpr::Agg { arg, .. }
        | PExpr::HistogramQuantile { arg, .. } => assign_slots(arg, specs),
        PExpr::Binary { lhs, rhs, .. } => {
            assign_slots(lhs, specs);
            assign_slots(rhs, specs);
        }
        PExpr::Number(_) => {}
    }
}

/// The first selector's matchers (the `/api/v1/series` entry point).
fn selector_of(expr: &PExpr) -> Result<&Vec<Matcher>, String> {
    match expr {
        PExpr::Selector { matchers, .. } => Ok(matchers),
        PExpr::RangeFn { arg, .. }
        | PExpr::Agg { arg, .. }
        | PExpr::HistogramQuantile { arg, .. } => selector_of(arg),
        PExpr::Binary { lhs, .. } => selector_of(lhs),
        PExpr::Number(_) => Err("number-only expressions are not supported".into()),
    }
}

/// Match-key for binary operations: labels sans `__name__` (PromQL drops
/// the metric name from arithmetic results and matches ignoring it).
fn match_key(labels: &Labels) -> Labels {
    labels
        .iter()
        .filter(|(k, _)| k != "__name__")
        .cloned()
        .collect()
}

/// Evaluate `expr` at each step in `steps` (ms). Returns per-step values.
fn eval_steps(expr: &PExpr, fetched: &[Fetched], steps: &[i64]) -> Result<Vec<StepVal>, String> {
    match expr {
        PExpr::Number(n) => Ok(steps.iter().map(|_| StepVal::Scalar(*n)).collect()),
        PExpr::Binary { op, lhs, rhs } => {
            let l = eval_steps(lhs, fetched, steps)?;
            let r = eval_steps(rhs, fetched, steps)?;
            Ok(l.into_iter()
                .zip(r)
                .map(|(lv, rv)| match (lv, rv) {
                    (StepVal::Scalar(a), StepVal::Scalar(b)) => StepVal::Scalar(op.apply(a, b)),
                    (StepVal::Scalar(a), StepVal::Vector(v)) => StepVal::Vector(
                        v.into_iter()
                            .map(|(labels, b)| (match_key(&labels), op.apply(a, b)))
                            .collect(),
                    ),
                    (StepVal::Vector(v), StepVal::Scalar(b)) => StepVal::Vector(
                        v.into_iter()
                            .map(|(labels, a)| (match_key(&labels), op.apply(a, b)))
                            .collect(),
                    ),
                    (StepVal::Vector(lv), StepVal::Vector(rv)) => {
                        // One-to-one on identical label sets sans __name__.
                        let rhs_by: BTreeMap<Labels, f64> = rv
                            .into_iter()
                            .map(|(labels, v)| (match_key(&labels), v))
                            .collect();
                        StepVal::Vector(
                            lv.into_iter()
                                .filter_map(|(labels, a)| {
                                    let key = match_key(&labels);
                                    rhs_by.get(&key).map(|b| (key, op.apply(a, *b)))
                                })
                                .collect(),
                        )
                    }
                })
                .collect())
        }
        PExpr::HistogramQuantile { phi, arg } => {
            let inner = eval_steps(arg, fetched, steps)?;
            Ok(inner
                .into_iter()
                .map(|val| {
                    let StepVal::Vector(vector) = val else {
                        return StepVal::Vector(Vec::new());
                    };
                    StepVal::Vector(histogram_quantile(*phi, vector))
                })
                .collect())
        }
        PExpr::Selector { range_ms, .. } => {
            if range_ms.is_some() {
                return Err("range selectors need rate()/increase()/delta()".into());
            }
            let PExpr::Selector { slot, .. } = expr else {
                unreachable!()
            };
            let data = &fetched[*slot];
            Ok(steps
                .iter()
                .map(|&t| {
                    let mut v = Vec::new();
                    for (labels, samples) in &data.series {
                        // Latest sample at or before t, within the lookback.
                        let idx = samples.partition_point(|s| s.ts <= t);
                        if idx > 0 {
                            let s = &samples[idx - 1];
                            if t - s.ts <= LOOKBACK_MS {
                                v.push((clean_labels(labels, true), s.value));
                            }
                        }
                    }
                    StepVal::Vector(v)
                })
                .collect())
        }
        PExpr::RangeFn { func, arg } => {
            let PExpr::Selector {
                range_ms: Some(window),
                slot,
                ..
            } = arg.as_ref()
            else {
                return Err(
                    "rate()/increase()/delta()/*_over_time() need a range selector like m[5m]"
                        .into(),
                );
            };
            let window = *window;
            let data = &fetched[*slot];
            Ok(steps
                .iter()
                .map(|&t| {
                    let mut v = Vec::new();
                    for (labels, samples) in &data.series {
                        let lo = samples.partition_point(|s| s.ts < t - window);
                        let hi = samples.partition_point(|s| s.ts <= t);
                        let win = &samples[lo..hi];
                        let value = match func {
                            // Change-over-window family: needs two samples.
                            RangeFn::Rate | RangeFn::Increase | RangeFn::Delta => {
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
                                if *func == RangeFn::Rate {
                                    change / (window as f64 / 1000.0)
                                } else {
                                    change
                                }
                            }
                            // Window aggregations: any sample counts.
                            _ => {
                                if win.is_empty() {
                                    continue;
                                }
                                match func {
                                    RangeFn::AvgOverTime => {
                                        win.iter().map(|s| s.value).sum::<f64>()
                                            / win.len() as f64
                                    }
                                    RangeFn::MinOverTime => win
                                        .iter()
                                        .map(|s| s.value)
                                        .fold(f64::INFINITY, f64::min),
                                    RangeFn::MaxOverTime => win
                                        .iter()
                                        .map(|s| s.value)
                                        .fold(f64::NEG_INFINITY, f64::max),
                                    RangeFn::SumOverTime => {
                                        win.iter().map(|s| s.value).sum()
                                    }
                                    RangeFn::CountOverTime => win.len() as f64,
                                    RangeFn::LastOverTime => win[win.len() - 1].value,
                                    _ => unreachable!("change family handled above"),
                                }
                            }
                        };
                        // Range functions drop the metric name, PromQL-style —
                        // except last_over_time, which returns a raw sample.
                        let keep_name = *func == RangeFn::LastOverTime;
                        v.push((clean_labels(labels, keep_name), value));
                    }
                    StepVal::Vector(v)
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
                .map(|val| {
                    let StepVal::Vector(vector) = val else {
                        return Vec::new();
                    };
                    fold_agg(*op, by, without, vector)
                })
                .map(StepVal::Vector)
                .collect())
        }
    }
}

/// Prometheus `histogram_quantile`: group `_bucket` series by their labels
/// sans `le`, order the cumulative buckets, and interpolate linearly
/// inside the bucket containing the φ-rank. Needs a `+Inf` bucket; counts
/// are made monotone (double-counted resets clamp) like Prometheus does.
fn histogram_quantile(phi: f64, vector: Vector) -> Vector {
    let mut groups: BTreeMap<Labels, Vec<(f64, f64)>> = BTreeMap::new();
    for (labels, value) in vector {
        let Some(le) = labels.iter().find(|(k, _)| k == "le").map(|(_, v)| v) else {
            continue;
        };
        let Ok(le) = le.trim_start_matches('+').parse::<f64>() else {
            continue;
        };
        let base: Labels = labels.iter().filter(|(k, _)| k != "le").cloned().collect();
        groups.entry(base).or_default().push((le, value));
    }
    let mut out = Vec::new();
    for (labels, mut buckets) in groups {
        buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
        if buckets.len() < 2 || buckets.last().map(|(le, _)| *le) != Some(f64::INFINITY) {
            continue;
        }
        // Monotone counts (merged/raced buckets can dip).
        let mut max_so_far = 0.0f64;
        for (_, c) in &mut buckets {
            max_so_far = max_so_far.max(*c);
            *c = max_so_far;
        }
        let total = buckets.last().expect("len checked").1;
        if total.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
            continue; // empty histogram (or NaN counts)
        }
        let value = if phi < 0.0 {
            f64::NEG_INFINITY
        } else if phi > 1.0 {
            f64::INFINITY
        } else {
            let rank = phi * total;
            let idx = buckets.partition_point(|(_, c)| *c < rank).min(buckets.len() - 1);
            let (end, count) = buckets[idx];
            if end.is_infinite() {
                buckets[idx - 1].0
            } else {
                let (start, count_start) = if idx == 0 { (0.0, 0.0) } else { buckets[idx - 1] };
                if count <= count_start {
                    end
                } else {
                    start + (end - start) * (rank - count_start) / (count - count_start)
                }
            }
        };
        out.push((labels, value));
    }
    out
}

/// One aggregation fold over an instant vector (`sum by (...)` etc.).
fn fold_agg(
    op: AggOp,
    by: &Option<Vec<String>>,
    without: &Option<Vec<String>>,
    vector: Vector,
) -> Vector {
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
                AggOp::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                AggOp::Count => values.len() as f64,
            };
            (labels, value)
        })
        .collect()
}

/// Pivot per-step values into the Prometheus `matrix` response.
fn render_matrix(steps: &[i64], vals: Vec<StepVal>) -> (u16, Json) {
    let mut by_series: BTreeMap<Labels, Vec<(i64, f64)>> = BTreeMap::new();
    for (t, val) in steps.iter().zip(vals) {
        let vector = match val {
            StepVal::Vector(v) => v,
            StepVal::Scalar(v) => vec![(Vec::new(), v)],
        };
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

/// Fetch every selector's data. `offset` shifts the fetched window into
/// the past and then shifts the sample timestamps forward by the same
/// amount, so the evaluator's step arithmetic needs no offset awareness.
fn fetch_all(
    ctx: &Shared,
    scope: &Scope,
    specs: &[(Vec<Matcher>, Option<i64>, i64)],
    t0: i64,
    t1: i64,
) -> Result<Vec<Fetched>, String> {
    let mut out = Vec::with_capacity(specs.len());
    for (matchers, range, offset) in specs {
        let mut matchers = matchers.clone();
        scope_matchers(&mut matchers, scope);
        // The evaluator needs history behind the first step: the range
        // window (or instant lookback), whichever the selector uses.
        let back = range.unwrap_or(LOOKBACK_MS) + offset;
        let mut series = match ctx.backend.ts_query(
            &scope.table,
            &matchers,
            t0.saturating_sub(back),
            t1.saturating_sub(*offset),
        ) {
            Ok(series) => series,
            // No ingest yet: an empty result, not an error — a fresh
            // Grafana datasource should see empty panels, matching
            // Prometheus.
            Err(e) if e.to_string().contains("does not exist") => Vec::new(),
            Err(e) => return Err(e.to_string()),
        };
        if *offset != 0 {
            for (_, samples) in &mut series {
                for sample in samples {
                    sample.ts += offset;
                }
            }
        }
        normalize_series(&mut series, scope);
        out.push(Fetched { series });
    }
    Ok(out)
}

/// `/api/v1/query`: evaluate at one instant.
pub fn query(ctx: &Shared, scope: &Scope, params: &BTreeMap<String, String>) -> (u16, Json) {
    let Some(q) = params.get("query") else {
        return err_json("missing query parameter");
    };
    let t = params
        .get("time")
        .and_then(|t| parse_prom_time(t))
        .unwrap_or_else(wall_ms);
    let mut expr = match P::new(q).parse() {
        Ok(e) => e,
        Err(e) => return err_json(&e),
    };
    // Number-only expressions (Grafana's datasource health check probes
    // `1+1`) fetch nothing and evaluate to a scalar.
    let mut specs = Vec::new();
    assign_slots(&mut expr, &mut specs);
    let fetched = match fetch_all(ctx, scope, &specs, t, t) {
        Ok(f) => f,
        Err(e) => return err_json(&e),
    };
    match eval_steps(&expr, &fetched, &[t]) {
        Ok(mut vals) => {
            let vector = match vals.pop() {
                Some(StepVal::Vector(v)) => v,
                Some(StepVal::Scalar(v)) => {
                    return (
                        200,
                        json!({"status": "success",
                               "data": {"resultType": "scalar",
                                        "result": [t as f64 / 1000.0, format!("{v}")]}}),
                    )
                }
                None => Vec::new(),
            };
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
pub fn query_range(ctx: &Shared, scope: &Scope, params: &BTreeMap<String, String>) -> (u16, Json) {
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
    let mut expr = match P::new(q).parse() {
        Ok(e) => e,
        Err(e) => return err_json(&e),
    };
    // Number-only expressions fetch nothing; `render_matrix` turns the
    // per-step scalars into one `{}`-labeled series, matching Prometheus.
    let mut specs = Vec::new();
    assign_slots(&mut expr, &mut specs);
    let steps: Vec<i64> = (0..).map(|i| start + i * step_ms).take_while(|t| *t <= end).collect();
    let fetched = match fetch_all(ctx, scope, &specs, start, end) {
        Ok(f) => f,
        Err(e) => return err_json(&e),
    };
    match eval_steps(&expr, &fetched, &steps) {
        Ok(vals) => render_matrix(&steps, vals),
        Err(e) => err_json(&e),
    }
}

/// `/api/v1/labels` and `/api/v1/label/<name>/values`.
pub fn labels(ctx: &Shared, scope: &Scope, value_of: Option<&str>) -> (u16, Json) {
    let series = match ctx.backend.ts_query(&scope.table, &[], i64::MIN, i64::MAX) {
        Ok(s) => s,
        Err(e) if e.to_string().contains("does not exist") => Vec::new(),
        Err(e) => return err_json(&e.to_string()),
    };
    let metric = scope.metric_label();
    let mut out: Vec<String> = Vec::new();
    for (labels, _) in &series {
        for (k, v) in labels {
            // The scope's metric label surfaces as `__name__`; other internal
            // (`__`-prefixed) labels stay hidden.
            let k = if k == metric {
                "__name__"
            } else if k.starts_with("__") {
                continue;
            } else {
                k.as_str()
            };
            match value_of {
                None => {
                    if !out.iter().any(|o| o == k) {
                        out.push(k.to_string());
                    }
                }
                Some(want) => {
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
pub fn series(ctx: &Shared, scope: &Scope, params: &BTreeMap<String, String>) -> (u16, Json) {
    let mut matchers = match params.get("match[]") {
        Some(sel) => match P::new(sel).parse() {
            Ok(expr) => match selector_of(&expr) {
                Ok(m) => m.clone(),
                Err(e) => return err_json(&e),
            },
            Err(e) => return err_json(&e),
        },
        None => Vec::new(),
    };
    scope_matchers(&mut matchers, scope);
    let series = match ctx.backend.ts_query(&scope.table, &matchers, i64::MIN, i64::MAX) {
        Ok(s) => s,
        Err(e) if e.to_string().contains("does not exist") => Vec::new(),
        Err(e) => return err_json(&e.to_string()),
    };
    let mut series = series;
    normalize_series(&mut series, scope);
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

    /// The `*_over_time` window aggregations — Grafana's Metrics Drilldown
    /// tiles wrap gauges in `avg_over_time`, so an unsupported family showed
    /// every tile as "No data".
    #[test]
    fn over_time_functions_evaluate() {
        let series = |values: &[(i64, f64)]| Fetched {
            series: vec![(
                vec![("name".to_string(), "m".to_string())],
                values.iter().map(|&(ts, value)| Sample { ts, value }).collect(),
            )],
        };
        let run = |q: &str, fetched: &Fetched| -> Vec<(Labels, f64)> {
            let mut e = P::new(q).parse().unwrap();
            let mut specs = Vec::new();
            assign_slots(&mut e, &mut specs);
            let f = vec![Fetched { series: fetched.series.clone() }];
            let vals = eval_steps(&e, &f, &[10_000]).unwrap();
            let StepVal::Vector(v) = &vals[0] else { panic!("expected vector") };
            v.clone()
        };
        let data = series(&[(1_000, 2.0), (5_000, 6.0), (9_000, 4.0)]);
        assert_eq!(run("avg_over_time(m[10s])", &data)[0].1, 4.0);
        assert_eq!(run("min_over_time(m[10s])", &data)[0].1, 2.0);
        assert_eq!(run("max_over_time(m[10s])", &data)[0].1, 6.0);
        assert_eq!(run("sum_over_time(m[10s])", &data)[0].1, 12.0);
        assert_eq!(run("count_over_time(m[10s])", &data)[0].1, 3.0);
        let last = run("last_over_time(m[10s])", &data);
        assert_eq!(last[0].1, 4.0);
        // last_over_time keeps the metric name; the aggregations drop it.
        assert_eq!(last[0].0, vec![("__name__".to_string(), "m".to_string())]);
        assert!(run("avg_over_time(m[10s])", &data)[0].0.is_empty());
        // A single sample is enough (unlike rate's two-sample floor).
        let one = series(&[(9_000, 7.0)]);
        assert_eq!(run("avg_over_time(m[10s])", &one)[0].1, 7.0);
        // The drilldown's exact tile shape parses and evaluates.
        assert_eq!(run("avg(avg_over_time(m[10s]))", &one)[0].1, 7.0);
    }

    /// Number-only expressions (Grafana's `1+1` datasource health check)
    /// evaluate to scalars with nothing fetched.
    #[test]
    fn number_only_expressions_evaluate() {
        let mut e = P::new("1+1").parse().unwrap();
        let mut specs = Vec::new();
        assign_slots(&mut e, &mut specs);
        assert!(specs.is_empty());
        let vals = eval_steps(&e, &[], &[1000, 2000]).unwrap();
        assert_eq!(vals.len(), 2);
        assert!(matches!(vals[0], StepVal::Scalar(v) if v == 2.0));
    }

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
        let PExpr::Selector { matchers, range_ms, .. } = *arg else { panic!() };
        assert_eq!(range_ms, Some(300_000));
        assert_eq!(matchers.len(), 3); // name + 2 label matchers
    }

    #[test]
    fn parses_regex_offset_arithmetic_and_quantile() {
        // Regex matchers compile anchored.
        let e = P::new(r#"m{job=~"api.*",env!~"dev|test"}"#).parse().unwrap();
        let PExpr::Selector { matchers, .. } = e else { panic!() };
        assert!(matches!(&matchers[1], Matcher::Re(k, r) if k == "job" && r.is_match("api-2")));
        assert!(matches!(&matchers[1], Matcher::Re(_, r) if !r.is_match("xapi")), "anchored");
        assert!(matches!(&matchers[2], Matcher::NotRe(k, _) if k == "env"));
        // A bad pattern is a parse error, not a panic.
        assert!(P::new(r#"m{l=~"["}"#).parse().is_err());

        // offset.
        let e = P::new("rate(m[5m] offset 1h)").parse().unwrap();
        let PExpr::RangeFn { arg, .. } = e else { panic!() };
        let PExpr::Selector { offset_ms, range_ms, .. } = *arg else { panic!() };
        assert_eq!(offset_ms, 3_600_000);
        assert_eq!(range_ms, Some(300_000));

        // Arithmetic precedence: a + b * c parses as a + (b * c).
        let e = P::new("a + b * c").parse().unwrap();
        let PExpr::Binary { op: BinOp::Add, rhs, .. } = e else { panic!() };
        assert!(matches!(*rhs, PExpr::Binary { op: BinOp::Mul, .. }));
        // Parens override.
        let e = P::new("(a + b) / c").parse().unwrap();
        assert!(matches!(e, PExpr::Binary { op: BinOp::Div, .. }));

        // histogram_quantile.
        let e = P::new(r#"histogram_quantile(0.9, rate(req_bucket[5m]))"#).parse().unwrap();
        let PExpr::HistogramQuantile { phi, .. } = e else { panic!() };
        assert_eq!(phi, 0.9);
    }

    #[test]
    fn evaluates_arithmetic_and_quantile() {
        // Two selectors' worth of fetched data, one sample each at t=1000.
        let series = |name: &str, extra: &[(&str, &str)], value: f64| {
            let mut labels: Labels = vec![("name".into(), name.into())];
            labels.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
            labels.sort();
            (labels, vec![Sample { ts: 1000, value }])
        };
        let mut a = P::new("a / b").parse().unwrap();
        let mut specs = Vec::new();
        assign_slots(&mut a, &mut specs);
        assert_eq!(specs.len(), 2);
        let fetched = vec![
            Fetched { series: vec![series("a", &[("job", "x")], 10.0)] },
            Fetched { series: vec![series("b", &[("job", "x")], 4.0)] },
        ];
        let vals = eval_steps(&a, &fetched, &[1000]).unwrap();
        let StepVal::Vector(v) = &vals[0] else { panic!() };
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].1, 2.5);
        assert_eq!(v[0].0, vec![("job".to_string(), "x".to_string())]);

        // scalar * vector.
        let mut e = P::new("2 * a").parse().unwrap();
        let mut specs = Vec::new();
        assign_slots(&mut e, &mut specs);
        let fetched = vec![Fetched { series: vec![series("a", &[], 21.0)] }];
        let vals = eval_steps(&e, &fetched, &[1000]).unwrap();
        let StepVal::Vector(v) = &vals[0] else { panic!() };
        assert_eq!(v[0].1, 42.0);

        // histogram_quantile: uniform 0..100 over two buckets + Inf.
        let buckets: Vector = vec![
            (vec![("le".to_string(), "50".to_string())], 5.0),
            (vec![("le".to_string(), "100".to_string())], 10.0),
            (vec![("le".to_string(), "+Inf".to_string())], 10.0),
        ];
        let q = histogram_quantile(0.5, buckets.clone());
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].1, 50.0);
        let q = histogram_quantile(0.75, buckets);
        assert_eq!(q[0].1, 75.0);
    }

    #[test]
    fn parses_bare_and_name_mapped_selectors() {
        // Bare {matcher} selector, regex form.
        let e = P::new(r#"{name=~"skaidb.*"}"#).parse().unwrap();
        let PExpr::Selector { matchers, .. } = e else { panic!() };
        assert_eq!(matchers.len(), 1);
        assert!(matches!(&matchers[0], Matcher::Re(k, r) if k == "name" && r.is_match("skaidb_up")));
        // __name__ maps to the storage label `name`.
        let e = P::new(r#"{__name__="up", job="api"}"#).parse().unwrap();
        let PExpr::Selector { matchers, .. } = e else { panic!() };
        assert!(matches!(&matchers[0], Matcher::Eq(k, v) if k == "name" && v == "up"));
        // Works inside functions too.
        assert!(P::new(r#"rate({name=~"http.*"}[5m])"#).parse().is_ok());
        // An empty bare selector is rejected.
        assert!(P::new("{}").parse().is_err());
    }

    #[test]
    fn rejects_trailing() {
        assert!(P::new("m }").parse().is_err());
        assert!(P::new("1 + 2").parse().is_ok());
    }

    #[test]
    fn percent_decoding() {
        let p = parse_params("query=rate%28m%5B5m%5D%29&time=100.5");
        assert_eq!(p["query"], "rate(m[5m])");
        assert_eq!(p["time"], "100.5");
        assert_eq!(percent_decode("a+b%20c"), "a b c");
    }
}
