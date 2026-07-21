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
    /// (ignoring `__name__`, which arithmetic drops — PromQL semantics).
    /// `bool_mod` is the comparison `bool` modifier (0/1 values instead of
    /// filtering).
    Binary {
        op: BinOp,
        bool_mod: bool,
        lhs: Box<PExpr>,
        rhs: Box<PExpr>,
    },
    /// `histogram_quantile(φ, expr)` over `_bucket` series with `le`.
    HistogramQuantile { phi: f64, arg: Box<PExpr> },
    /// `timestamp(sel)` — each series' selected sample TIME (unix seconds)
    /// as the value; `max(timestamp(m)) * 1000` is the "last reading"
    /// dashboard pattern. The argument must be an instant selector.
    Timestamp { arg: Box<PExpr> },
    /// `time()` — the evaluation timestamp as a scalar (unix seconds);
    /// `time() - max(timestamp(m))` is data staleness.
    Time,
    /// `label_replace(v, dst, replacement, src, regex)`. The pattern is kept
    /// as source text (PExpr is PartialEq for tests); it is validated at
    /// parse and compiled once per evaluation.
    LabelReplace {
        arg: Box<PExpr>,
        dst: String,
        repl: String,
        src: String,
        pattern: String,
    },
    /// `label_join(v, dst, sep, src1, src2, ...)`.
    LabelJoin {
        arg: Box<PExpr>,
        dst: String,
        sep: String,
        srcs: Vec<String>,
    },
    /// `sort(v)` / `sort_desc(v)` — order series by value within each step.
    Sort { desc: bool, arg: Box<PExpr> },
    /// `absent(v)` — empty vector when v has samples; otherwise a single
    /// 1-valued sample carrying the labels derivable from v's `=` matchers.
    Absent { arg: Box<PExpr>, labels: Labels },
    /// `absent_over_time(m[w])` — the window form of `absent`: 1 (with the
    /// derived labels) when NO series has a sample in the window.
    AbsentOverTime { arg: Box<PExpr>, labels: Labels },
    /// `count_values("dst", v) [by|without (...)]` — one output series per
    /// distinct sample value per group, the value carried in label `dst`,
    /// the output value its count.
    CountValues {
        dst: String,
        by: Option<Vec<String>>,
        without: Option<Vec<String>>,
        arg: Box<PExpr>,
    },
    /// A bare number.
    Number(f64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    /// Comparisons: filters without `bool` (keep the sample when true),
    /// 0/1-valued with it. Grafana's drilldown emits `<expr> > -Inf`
    /// (extreme-values filtering); alert rules live on `== 0` etc.
    Gt,
    Lt,
    Ge,
    Le,
    CmpEq,
    CmpNe,
    /// Set operators, one-to-one on the label set sans `__name__`.
    And,
    Or,
    Unless,
}

impl BinOp {
    fn apply(self, l: f64, r: f64) -> f64 {
        match self {
            BinOp::Add => l + r,
            BinOp::Sub => l - r,
            BinOp::Mul => l * r,
            BinOp::Div => l / r,
            _ => unreachable!("comparison/set ops route through apply_binary"),
        }
    }

    fn is_arithmetic(self) -> bool {
        matches!(self, BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div)
    }

    fn is_comparison(self) -> bool {
        matches!(
            self,
            BinOp::Gt | BinOp::Lt | BinOp::Ge | BinOp::Le | BinOp::CmpEq | BinOp::CmpNe
        )
    }

    fn cmp(self, l: f64, r: f64) -> bool {
        match self {
            BinOp::Gt => l > r,
            BinOp::Lt => l < r,
            BinOp::Ge => l >= r,
            BinOp::Le => l <= r,
            BinOp::CmpEq => l == r,
            BinOp::CmpNe => l != r,
            _ => unreachable!("cmp() is only called for comparison ops"),
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
    /// `irate` / `idelta` — instantaneous forms over the LAST TWO samples
    /// in the window (irate is counter-reset-aware and per-second).
    IRate,
    IDelta,
    /// Tier-2 window analytics (alerting/analytics parity).
    /// `present_over_time` — 1 when the window has any sample.
    PresentOverTime,
    /// `changes` — value changes between consecutive samples.
    Changes,
    /// `resets` — counter decreases between consecutive samples.
    Resets,
    /// `deriv` — per-second least-squares slope over the window.
    Deriv,
    /// `predict_linear(m[w], t)` — regression value at eval time + t secs.
    PredictLinear(f64),
    /// Population stddev / variance over the window.
    StddevOverTime,
    StdvarOverTime,
    /// Median absolute deviation over the window.
    MadOverTime,
    /// `quantile_over_time(φ, m[w])` — φ-quantile of the window's values.
    QuantileOverTime(f64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum AggOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    /// Population standard deviation (Prometheus semantics). Grafana's
    /// drilldown "Standard deviation" preview queries `stddev(m{…})`.
    Stddev,
    /// `quantile(φ, v)` — φ-quantile across the group, linear interpolation;
    /// the drilldown's "Percentiles" preview (P50/P90/P99 …).
    Quantile(f64),
    /// `topk(k, v)` / `bottomk(k, v)` — SELECT the k best/worst samples per
    /// group (original labels and values, `__name__` kept), unlike the
    /// folding aggregations above.
    TopK(f64),
    BottomK(f64),
    /// Population variance (stddev²), Prometheus semantics.
    Stdvar,
    /// `group(v)` — 1 for every group (existence aggregation).
    Group,
}

impl AggOp {
    /// Aggregations taking a leading scalar parameter (`op(param, v)`).
    fn takes_param(self) -> bool {
        matches!(self, AggOp::Quantile(_) | AggOp::TopK(_) | AggOp::BottomK(_))
    }

    fn with_param(self, p: f64) -> AggOp {
        match self {
            AggOp::Quantile(_) => AggOp::Quantile(p),
            AggOp::TopK(_) => AggOp::TopK(p),
            AggOp::BottomK(_) => AggOp::BottomK(p),
            other => other,
        }
    }
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

    /// PromQL precedence, loosest first: `or` → `and`/`unless` →
    /// comparisons (`== != <= < >= >`, optional `bool`) → `+ -` → `* /`.
    fn expr(&mut self) -> Result<PExpr, String> {
        let mut lhs = self.and_expr()?;
        while self.keyword("or") {
            let rhs = self.and_expr()?;
            lhs = PExpr::Binary {
                op: BinOp::Or,
                bool_mod: false,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
        Ok(lhs)
    }

    fn and_expr(&mut self) -> Result<PExpr, String> {
        let mut lhs = self.cmp_expr()?;
        loop {
            let op = if self.keyword("and") {
                BinOp::And
            } else if self.keyword("unless") {
                BinOp::Unless
            } else {
                return Ok(lhs);
            };
            let rhs = self.cmp_expr()?;
            lhs = PExpr::Binary {
                op,
                bool_mod: false,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
    }

    fn cmp_expr(&mut self) -> Result<PExpr, String> {
        let mut lhs = self.add_expr()?;
        loop {
            let op = match self.peek() {
                Some(b'=') if self.s.get(self.i + 1) == Some(&b'=') => {
                    self.i += 2;
                    BinOp::CmpEq
                }
                Some(b'!') if self.s.get(self.i + 1) == Some(&b'=') => {
                    self.i += 2;
                    BinOp::CmpNe
                }
                Some(b'<') => {
                    self.i += 1;
                    if self.eat(b'=') {
                        BinOp::Le
                    } else {
                        BinOp::Lt
                    }
                }
                Some(b'>') => {
                    self.i += 1;
                    if self.eat(b'=') {
                        BinOp::Ge
                    } else {
                        BinOp::Gt
                    }
                }
                _ => return Ok(lhs),
            };
            let bool_mod = self.keyword("bool");
            let rhs = self.add_expr()?;
            lhs = PExpr::Binary {
                op,
                bool_mod,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            };
        }
    }

    /// `add_expr := term (('+'|'-') term)*`
    fn add_expr(&mut self) -> Result<PExpr, String> {
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
                bool_mod: false,
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
                bool_mod: false,
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
            // `Inf`/`NaN` are number literals in PromQL (case-insensitive);
            // Grafana's drilldown filters extreme values with `> -Inf`.
            for (word, value) in [("inf", f64::INFINITY), ("nan", f64::NAN)] {
                let end = self.i + word.len();
                if self.s.get(self.i..end).is_some_and(|s| s.eq_ignore_ascii_case(word.as_bytes()))
                    && !self.s.get(end).is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
                {
                    self.i = end;
                    let neg = self.s[start] == b'-';
                    return Ok(PExpr::Number(if neg { -value } else { value }));
                }
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
        if name.eq_ignore_ascii_case("inf") {
            return Ok(PExpr::Number(f64::INFINITY));
        }
        if name.eq_ignore_ascii_case("nan") {
            return Ok(PExpr::Number(f64::NAN));
        }
        // histogram_quantile(φ, expr).
        if name == "timestamp" {
            self.expect(b'(')?;
            let arg = self.expr()?;
            self.expect(b')')?;
            if !matches!(arg, PExpr::Selector { range_ms: None, .. }) {
                return Err(
                    "timestamp() takes an instant selector, e.g. timestamp(m{})".into(),
                );
            }
            return Ok(PExpr::Timestamp { arg: Box::new(arg) });
        }
        if name == "time" {
            self.expect(b'(')?;
            self.expect(b')')?;
            return Ok(PExpr::Time);
        }
        if name == "label_replace" || name == "label_join" {
            // A label-name argument; `__name__` maps to the storage label.
            let label_arg = |p: &mut Self| -> Result<String, String> {
                p.ws();
                let s = p.string()?;
                Ok(if s == "__name__" { "name".into() } else { s })
            };
            self.expect(b'(')?;
            let arg = Box::new(self.expr()?);
            self.expect(b',')?;
            let dst = label_arg(self)?;
            if !valid_label_name(&dst) {
                return Err(format!("{name}: invalid destination label {dst:?}"));
            }
            self.expect(b',')?;
            let out = if name == "label_replace" {
                self.ws();
                let repl = self.string()?;
                self.expect(b',')?;
                let src = label_arg(self)?;
                self.expect(b',')?;
                self.ws();
                let pattern = self.string()?;
                compile_anchored(&pattern)?;
                PExpr::LabelReplace {
                    arg,
                    dst,
                    repl,
                    src,
                    pattern,
                }
            } else {
                self.ws();
                let sep = self.string()?;
                let mut srcs = Vec::new();
                while self.eat(b',') {
                    srcs.push(label_arg(self)?);
                }
                PExpr::LabelJoin {
                    arg,
                    dst,
                    sep,
                    srcs,
                }
            };
            self.expect(b')')?;
            return Ok(out);
        }
        if name == "sort" || name == "sort_desc" {
            self.expect(b'(')?;
            let arg = self.expr()?;
            self.expect(b')')?;
            return Ok(PExpr::Sort {
                desc: name == "sort_desc",
                arg: Box::new(arg),
            });
        }
        if name == "absent" || name == "absent_over_time" {
            self.expect(b'(')?;
            let arg = self.expr()?;
            self.expect(b')')?;
            let labels = match &arg {
                PExpr::Selector { matchers, .. } => absent_labels(matchers),
                _ => Vec::new(),
            };
            return Ok(if name == "absent" {
                PExpr::Absent {
                    arg: Box::new(arg),
                    labels,
                }
            } else {
                if !matches!(arg, PExpr::Selector { range_ms: Some(_), .. }) {
                    return Err(
                        "absent_over_time takes a range selector, e.g. absent_over_time(m[5m])"
                            .into(),
                    );
                }
                PExpr::AbsentOverTime {
                    arg: Box::new(arg),
                    labels,
                }
            });
        }
        if name == "quantile_over_time" {
            self.expect(b'(')?;
            let PExpr::Number(phi) = self.factor()? else {
                return Err("quantile_over_time needs a number as its first argument".into());
            };
            self.expect(b',')?;
            let arg = self.expr()?;
            self.expect(b')')?;
            return Ok(PExpr::RangeFn {
                func: RangeFn::QuantileOverTime(phi),
                arg: Box::new(arg),
            });
        }
        if name == "predict_linear" {
            self.expect(b'(')?;
            let arg = self.expr()?;
            self.expect(b',')?;
            let PExpr::Number(t) = self.factor()? else {
                return Err(
                    "predict_linear needs a number of seconds as its second argument".into(),
                );
            };
            self.expect(b')')?;
            return Ok(PExpr::RangeFn {
                func: RangeFn::PredictLinear(t),
                arg: Box::new(arg),
            });
        }
        if name == "count_values" {
            // Optional by/without BEFORE the parens, like other aggs.
            let (mut by, mut without) = (None, None);
            self.ws();
            if self.peek() != Some(b'(') {
                let modifier = self.ident().ok_or("expected 'by', 'without' or '('")?;
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
                match modifier.as_str() {
                    "by" => by = Some(ls),
                    "without" => without = Some(ls),
                    other => return Err(format!("unsupported modifier {other}")),
                }
            }
            self.expect(b'(')?;
            self.ws();
            let dst = self.string()?;
            if !valid_label_name(&dst) {
                return Err(format!("count_values: invalid label name {dst:?}"));
            }
            self.expect(b',')?;
            let arg = self.expr()?;
            self.expect(b')')?;
            return Ok(PExpr::CountValues {
                dst,
                by,
                without,
                arg: Box::new(arg),
            });
        }
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
            "stddev" => Some(AggOp::Stddev),
            "stdvar" => Some(AggOp::Stdvar),
            "group" => Some(AggOp::Group),
            // Scalar params parsed below, after the optional by/without.
            "quantile" => Some(AggOp::Quantile(f64::NAN)),
            "topk" => Some(AggOp::TopK(f64::NAN)),
            "bottomk" => Some(AggOp::BottomK(f64::NAN)),
            _ => None,
        };
        if let Some(mut op) = agg {
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
            // `quantile(φ, v)` / `topk(k, v)` / `bottomk(k, v)`: the scalar
            // parameter leads the argument list.
            if op.takes_param() {
                let PExpr::Number(p) = self.factor()? else {
                    return Err(format!(
                        "{name} needs a number as its first argument, e.g. {name}(5, m)"
                    ));
                };
                self.expect(b',')?;
                op = op.with_param(p);
            }
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
            "irate" => Some(RangeFn::IRate),
            "idelta" => Some(RangeFn::IDelta),
            "present_over_time" => Some(RangeFn::PresentOverTime),
            "changes" => Some(RangeFn::Changes),
            "resets" => Some(RangeFn::Resets),
            "deriv" => Some(RangeFn::Deriv),
            "stddev_over_time" => Some(RangeFn::StddevOverTime),
            "stdvar_over_time" => Some(RangeFn::StdvarOverTime),
            "mad_over_time" => Some(RangeFn::MadOverTime),
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
        if !self.eat(b'{') {
            return Ok(());
        }
        loop {
            // Trailing commas are legal PromQL (`{a="b",}`) — Grafana's
            // Metrics Drilldown emits `{__ignore_usage__="", }` when its
            // filters variable interpolates empty, so this closes-after-comma
            // check is load-bearing, not pedantry.
            if self.eat(b'}') {
                return Ok(());
            }
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
                self.expect(b'}')?;
                return Ok(());
            }
        }
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
        | PExpr::HistogramQuantile { arg, .. }
        | PExpr::Timestamp { arg }
        | PExpr::LabelReplace { arg, .. }
        | PExpr::LabelJoin { arg, .. }
        | PExpr::Sort { arg, .. }
        | PExpr::Absent { arg, .. }
        | PExpr::AbsentOverTime { arg, .. }
        | PExpr::CountValues { arg, .. } => assign_slots(arg, specs),
        PExpr::Binary { lhs, rhs, .. } => {
            assign_slots(lhs, specs);
            assign_slots(rhs, specs);
        }
        PExpr::Time | PExpr::Number(_) => {}
    }
}

/// The first selector's matchers (the `/api/v1/series` entry point).
fn selector_of(expr: &PExpr) -> Result<&Vec<Matcher>, String> {
    match expr {
        PExpr::Selector { matchers, .. } => Ok(matchers),
        PExpr::RangeFn { arg, .. }
        | PExpr::Agg { arg, .. }
        | PExpr::HistogramQuantile { arg, .. }
        | PExpr::Timestamp { arg }
        | PExpr::LabelReplace { arg, .. }
        | PExpr::LabelJoin { arg, .. }
        | PExpr::Sort { arg, .. }
        | PExpr::Absent { arg, .. }
        | PExpr::AbsentOverTime { arg, .. }
        | PExpr::CountValues { arg, .. } => selector_of(arg),
        PExpr::Binary { lhs, .. } => selector_of(lhs),
        PExpr::Time | PExpr::Number(_) => {
            Err("number-only expressions are not supported".into())
        }
    }
}

/// Anchored regex, PromQL-style: `label_replace`'s pattern must match the
/// WHOLE source value.
fn compile_anchored(pattern: &str) -> Result<regex::Regex, String> {
    regex::Regex::new(&format!("^(?:{pattern})$")).map_err(|e| format!("bad regex: {e}"))
}

/// Labels Prometheus derives for an `absent`/`absent_over_time` sample:
/// every unambiguous `=` matcher (name excluded; a label matched against
/// two different values is ambiguous and omitted).
fn absent_labels(matchers: &[Matcher]) -> Labels {
    let mut labels: Labels = Vec::new();
    for m in matchers {
        if let Matcher::Eq(k, v) = m {
            if k != "name" && !v.is_empty() {
                labels.push((k.clone(), v.clone()));
            }
        }
    }
    labels.sort();
    labels.dedup();
    let mut i = 0;
    while i < labels.len() {
        let dups = labels[i..]
            .iter()
            .take_while(|(k, _)| *k == labels[i].0)
            .count();
        if dups > 1 {
            labels.drain(i..i + dups);
        } else {
            i += 1;
        }
    }
    labels
}

fn valid_label_name(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .enumerate()
            .all(|(i, b)| b.is_ascii_alphabetic() || b == b'_' || (i > 0 && b.is_ascii_digit()))
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

/// One step of a binary operation. Arithmetic maps values (dropping
/// `__name__`, PromQL-style); comparisons FILTER (keep the sample, labels
/// intact, when true) unless `bool` makes them 0/1-valued (name dropped);
/// `and`/`or`/`unless` are label-set operations over vectors.
fn apply_binary(op: BinOp, bool_mod: bool, lv: StepVal, rv: StepVal) -> Result<StepVal, String> {
    if op.is_arithmetic() {
        return Ok(match (lv, rv) {
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
        });
    }
    if op.is_comparison() {
        let as01 = |b: bool| if b { 1.0 } else { 0.0 };
        return Ok(match (lv, rv) {
            (StepVal::Scalar(a), StepVal::Scalar(b)) => {
                if !bool_mod {
                    return Err(
                        "comparisons between scalars must use BOOL modifier".into(),
                    );
                }
                StepVal::Scalar(as01(op.cmp(a, b)))
            }
            (StepVal::Vector(v), StepVal::Scalar(b)) => StepVal::Vector(
                v.into_iter()
                    .filter_map(|(labels, a)| {
                        if bool_mod {
                            Some((match_key(&labels), as01(op.cmp(a, b))))
                        } else {
                            op.cmp(a, b).then_some((labels, a))
                        }
                    })
                    .collect(),
            ),
            (StepVal::Scalar(a), StepVal::Vector(v)) => StepVal::Vector(
                v.into_iter()
                    .filter_map(|(labels, b)| {
                        if bool_mod {
                            Some((match_key(&labels), as01(op.cmp(a, b))))
                        } else {
                            op.cmp(a, b).then_some((labels, b))
                        }
                    })
                    .collect(),
            ),
            (StepVal::Vector(lv), StepVal::Vector(rv)) => {
                let rhs_by: BTreeMap<Labels, f64> = rv
                    .into_iter()
                    .map(|(labels, v)| (match_key(&labels), v))
                    .collect();
                StepVal::Vector(
                    lv.into_iter()
                        .filter_map(|(labels, a)| {
                            let b = *rhs_by.get(&match_key(&labels))?;
                            if bool_mod {
                                Some((match_key(&labels), as01(op.cmp(a, b))))
                            } else {
                                op.cmp(a, b).then_some((labels, a))
                            }
                        })
                        .collect(),
                )
            }
        });
    }
    // Set operators: vectors only.
    let (StepVal::Vector(lv), StepVal::Vector(rv)) = (lv, rv) else {
        return Err("and/or/unless require vector operands on both sides".into());
    };
    let rhs_keys: std::collections::BTreeSet<Labels> =
        rv.iter().map(|(labels, _)| match_key(labels)).collect();
    Ok(StepVal::Vector(match op {
        BinOp::And => lv
            .into_iter()
            .filter(|(labels, _)| rhs_keys.contains(&match_key(labels)))
            .collect(),
        BinOp::Unless => lv
            .into_iter()
            .filter(|(labels, _)| !rhs_keys.contains(&match_key(labels)))
            .collect(),
        BinOp::Or => {
            let lhs_keys: std::collections::BTreeSet<Labels> =
                lv.iter().map(|(labels, _)| match_key(labels)).collect();
            let mut out = lv;
            out.extend(
                rv.into_iter()
                    .filter(|(labels, _)| !lhs_keys.contains(&match_key(labels))),
            );
            out
        }
        _ => unreachable!("arithmetic/comparison handled above"),
    }))
}

/// Evaluate `expr` at each step in `steps` (ms). Returns per-step values.
fn eval_steps(expr: &PExpr, fetched: &[Fetched], steps: &[i64]) -> Result<Vec<StepVal>, String> {
    match expr {
        PExpr::Number(n) => Ok(steps.iter().map(|_| StepVal::Scalar(*n)).collect()),
        PExpr::Time => Ok(steps
            .iter()
            .map(|&t| StepVal::Scalar(t as f64 / 1000.0))
            .collect()),
        // Each series' selected sample TIME (unix seconds) as the value —
        // instant-vector selection like a plain selector, but surfacing
        // `s.ts` instead of `s.value`. Functions drop the metric name.
        PExpr::Timestamp { arg } => {
            let PExpr::Selector { slot, .. } = arg.as_ref() else {
                unreachable!("parser enforces an instant selector");
            };
            let data = &fetched[*slot];
            Ok(steps
                .iter()
                .map(|&t| {
                    let mut v = Vec::new();
                    for (labels, samples) in &data.series {
                        let idx = samples.partition_point(|s| s.ts <= t);
                        if idx > 0 {
                            let s = &samples[idx - 1];
                            if t - s.ts <= LOOKBACK_MS {
                                v.push((clean_labels(labels, false), s.ts as f64 / 1000.0));
                            }
                        }
                    }
                    StepVal::Vector(v)
                })
                .collect())
        }
        PExpr::Binary { op, bool_mod, lhs, rhs } => {
            let l = eval_steps(lhs, fetched, steps)?;
            let r = eval_steps(rhs, fetched, steps)?;
            l.into_iter()
                .zip(r)
                .map(|(lv, rv)| apply_binary(*op, *bool_mod, lv, rv))
                .collect()
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
                            // Instantaneous forms: the last two samples only.
                            RangeFn::IRate | RangeFn::IDelta => {
                                if win.len() < 2 {
                                    continue;
                                }
                                let (a, b) = (&win[win.len() - 2], &win[win.len() - 1]);
                                if *func == RangeFn::IDelta {
                                    b.value - a.value
                                } else {
                                    // Counter-reset-aware, per-second.
                                    let inc = if b.value >= a.value {
                                        b.value - a.value
                                    } else {
                                        b.value
                                    };
                                    let dt = (b.ts - a.ts) as f64 / 1000.0;
                                    if dt <= 0.0 {
                                        continue;
                                    }
                                    inc / dt
                                }
                            }
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
                            // Regression family: least-squares over the
                            // window, x = seconds relative to the eval time
                            // (so the intercept IS the regressed value now).
                            RangeFn::Deriv | RangeFn::PredictLinear(_) => {
                                if win.len() < 2 {
                                    continue;
                                }
                                let n = win.len() as f64;
                                let (mut sx, mut sy) = (0.0f64, 0.0f64);
                                for s in win {
                                    sx += (s.ts - t) as f64 / 1000.0;
                                    sy += s.value;
                                }
                                let (mx, my) = (sx / n, sy / n);
                                let (mut cov, mut var) = (0.0f64, 0.0f64);
                                for s in win {
                                    let dx = (s.ts - t) as f64 / 1000.0 - mx;
                                    cov += dx * (s.value - my);
                                    var += dx * dx;
                                }
                                if var == 0.0 {
                                    continue;
                                }
                                let slope = cov / var;
                                match func {
                                    RangeFn::Deriv => slope,
                                    RangeFn::PredictLinear(secs) => {
                                        (my - slope * mx) + slope * secs
                                    }
                                    _ => unreachable!(),
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
                                    RangeFn::PresentOverTime => 1.0,
                                    RangeFn::Changes => win
                                        .windows(2)
                                        .filter(|p| p[1].value != p[0].value)
                                        .count()
                                        as f64,
                                    RangeFn::Resets => win
                                        .windows(2)
                                        .filter(|p| p[1].value < p[0].value)
                                        .count()
                                        as f64,
                                    RangeFn::StddevOverTime | RangeFn::StdvarOverTime => {
                                        let mean =
                                            win.iter().map(|s| s.value).sum::<f64>()
                                                / win.len() as f64;
                                        let var = win
                                            .iter()
                                            .map(|s| (s.value - mean) * (s.value - mean))
                                            .sum::<f64>()
                                            / win.len() as f64;
                                        if *func == RangeFn::StddevOverTime {
                                            var.sqrt()
                                        } else {
                                            var
                                        }
                                    }
                                    RangeFn::MadOverTime => {
                                        let med = |mut v: Vec<f64>| -> f64 {
                                            v.sort_by(f64::total_cmp);
                                            let n = v.len();
                                            if n % 2 == 1 {
                                                v[n / 2]
                                            } else {
                                                (v[n / 2 - 1] + v[n / 2]) / 2.0
                                            }
                                        };
                                        let m = med(win.iter().map(|s| s.value).collect());
                                        med(win.iter().map(|s| (s.value - m).abs()).collect())
                                    }
                                    RangeFn::QuantileOverTime(phi) => {
                                        if *phi < 0.0 {
                                            f64::NEG_INFINITY
                                        } else if *phi > 1.0 {
                                            f64::INFINITY
                                        } else {
                                            let mut sorted: Vec<f64> =
                                                win.iter().map(|s| s.value).collect();
                                            sorted.sort_by(f64::total_cmp);
                                            let rank = phi * (sorted.len() - 1) as f64;
                                            let (lo, hi) =
                                                (rank.floor() as usize, rank.ceil() as usize);
                                            sorted[lo]
                                                + (sorted[hi] - sorted[lo])
                                                    * (rank - lo as f64)
                                        }
                                    }
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
        PExpr::LabelReplace {
            arg,
            dst,
            repl,
            src,
            pattern,
        } => {
            let re = compile_anchored(pattern)?;
            let inner = eval_steps(arg, fetched, steps)?;
            inner
                .into_iter()
                .map(|val| {
                    let StepVal::Vector(v) = val else {
                        return Err("label_replace() requires an instant vector".into());
                    };
                    Ok(StepVal::Vector(
                        v.into_iter()
                            .map(|(mut labels, value)| {
                                let srcval = label_value(&labels, src).to_string();
                                if let Some(caps) = re.captures(&srcval) {
                                    let mut expanded = String::new();
                                    caps.expand(repl, &mut expanded);
                                    set_label(&mut labels, dst, expanded);
                                }
                                (labels, value)
                            })
                            .collect(),
                    ))
                })
                .collect()
        }
        PExpr::LabelJoin {
            arg,
            dst,
            sep,
            srcs,
        } => {
            let inner = eval_steps(arg, fetched, steps)?;
            inner
                .into_iter()
                .map(|val| {
                    let StepVal::Vector(v) = val else {
                        return Err("label_join() requires an instant vector".into());
                    };
                    Ok(StepVal::Vector(
                        v.into_iter()
                            .map(|(mut labels, value)| {
                                let joined = srcs
                                    .iter()
                                    .map(|s| label_value(&labels, s))
                                    .collect::<Vec<_>>()
                                    .join(sep);
                                set_label(&mut labels, dst, joined);
                                (labels, value)
                            })
                            .collect(),
                    ))
                })
                .collect()
        }
        PExpr::Sort { desc, arg } => {
            let inner = eval_steps(arg, fetched, steps)?;
            Ok(inner
                .into_iter()
                .map(|val| match val {
                    StepVal::Vector(mut v) => {
                        v.sort_by(|a, b| {
                            if *desc {
                                b.1.total_cmp(&a.1)
                            } else {
                                a.1.total_cmp(&b.1)
                            }
                        });
                        StepVal::Vector(v)
                    }
                    scalar => scalar,
                })
                .collect())
        }
        PExpr::Absent { arg, labels } => {
            let inner = eval_steps(arg, fetched, steps)?;
            Ok(inner
                .into_iter()
                .map(|val| {
                    // A scalar always "exists"; only an empty vector is absent.
                    let absent = matches!(&val, StepVal::Vector(v) if v.is_empty());
                    StepVal::Vector(if absent {
                        vec![(labels.clone(), 1.0)]
                    } else {
                        Vec::new()
                    })
                })
                .collect())
        }
        PExpr::AbsentOverTime { arg, labels } => {
            let PExpr::Selector {
                range_ms: Some(window),
                slot,
                ..
            } = arg.as_ref()
            else {
                unreachable!("parser enforces a range selector");
            };
            let data = &fetched[*slot];
            Ok(steps
                .iter()
                .map(|&t| {
                    let any = data.series.iter().any(|(_, samples)| {
                        let lo = samples.partition_point(|s| s.ts < t - *window);
                        let hi = samples.partition_point(|s| s.ts <= t);
                        hi > lo
                    });
                    StepVal::Vector(if any {
                        Vec::new()
                    } else {
                        vec![(labels.clone(), 1.0)]
                    })
                })
                .collect())
        }
        PExpr::CountValues {
            dst,
            by,
            without,
            arg,
        } => {
            let inner = eval_steps(arg, fetched, steps)?;
            Ok(inner
                .into_iter()
                .map(|val| {
                    let StepVal::Vector(vector) = val else {
                        return StepVal::Vector(Vec::new());
                    };
                    // Group by (group key + the formatted value as `dst`),
                    // count members. The value renders exactly like sample
                    // output does, so `count_values("v", m)` labels align
                    // with what clients see elsewhere.
                    let mut groups: BTreeMap<Labels, f64> = BTreeMap::new();
                    for (labels, value) in vector {
                        let mut key: Labels = match (by, without) {
                            (Some(by), _) => labels
                                .iter()
                                .filter(|(k, _)| by.contains(k))
                                .cloned()
                                .collect(),
                            (None, Some(wo)) => labels
                                .iter()
                                .filter(|(k, _)| !wo.contains(k) && k != "__name__")
                                .cloned()
                                .collect(),
                            (None, None) => Vec::new(),
                        };
                        key.retain(|(k, _)| k != dst);
                        key.push((dst.clone(), format!("{value}")));
                        key.sort();
                        *groups.entry(key).or_insert(0.0) += 1.0;
                    }
                    StepVal::Vector(groups.into_iter().collect())
                })
                .collect())
        }
    }
}

/// A label's value, missing = "" (PromQL treats absent labels as empty).
fn label_value<'a>(labels: &'a Labels, key: &str) -> &'a str {
    labels
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .unwrap_or("")
}

/// Set `dst` to `val` on a sorted label set; the empty string REMOVES the
/// label (empty value == absent, PromQL-style).
fn set_label(labels: &mut Labels, dst: &str, val: String) {
    labels.retain(|(k, _)| k != dst);
    if !val.is_empty() {
        labels.push((dst.to_string(), val));
        labels.sort();
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
    let group_key = |labels: &Labels| -> Labels {
        match (by, without) {
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
        }
    };
    // topk/bottomk SELECT samples (original labels, `__name__` intact)
    // instead of folding each group to one value.
    if let AggOp::TopK(k) | AggOp::BottomK(k) = op {
        let desc = matches!(op, AggOp::TopK(_));
        let k = if k.is_finite() && k >= 1.0 { k as usize } else { 0 };
        let mut groups: BTreeMap<Labels, Vector> = BTreeMap::new();
        for (labels, value) in vector {
            groups.entry(group_key(&labels)).or_default().push((labels, value));
        }
        let mut out = Vec::new();
        for (_, mut members) in groups {
            members.sort_by(|a, b| {
                if desc {
                    b.1.total_cmp(&a.1)
                } else {
                    a.1.total_cmp(&b.1)
                }
            });
            members.truncate(k);
            out.append(&mut members);
        }
        return out;
    }
    let mut groups: BTreeMap<Labels, Vec<f64>> = BTreeMap::new();
    for (labels, value) in vector {
        let key = group_key(&labels);
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
                AggOp::Stddev | AggOp::Stdvar => {
                    // Population stddev/variance, Prometheus semantics.
                    let mean = values.iter().sum::<f64>() / values.len() as f64;
                    let var = values.iter().map(|v| (v - mean) * (v - mean)).sum::<f64>()
                        / values.len() as f64;
                    if op == AggOp::Stddev {
                        var.sqrt()
                    } else {
                        var
                    }
                }
                AggOp::Group => 1.0,
                AggOp::Quantile(phi) => {
                    // φ outside [0, 1] → ±Inf, like Prometheus.
                    if phi < 0.0 {
                        f64::NEG_INFINITY
                    } else if phi > 1.0 {
                        f64::INFINITY
                    } else {
                        let mut sorted = values.clone();
                        sorted.sort_by(|a, b| a.total_cmp(b));
                        let rank = phi * (sorted.len() - 1) as f64;
                        let (lo, hi) = (rank.floor() as usize, rank.ceil() as usize);
                        sorted[lo] + (sorted[hi] - sorted[lo]) * (rank - lo as f64)
                    }
                }
                AggOp::TopK(_) | AggOp::BottomK(_) => {
                    unreachable!("selected, not folded — handled above")
                }
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

    /// Grafana / Metrics Drilldown compatibility: the catalog of PromQL
    /// shapes Grafana's stock dashboards and the drilldown app actually emit
    /// (health check, tiles, function-picker previews, breakdowns, counter
    /// and histogram panels), parsed and evaluated end-to-end against a
    /// seeded series universe with matcher semantics applied — every shape
    /// here has bitten in production at least once (the 2026-07-20 chain:
    /// `1+1`, `*_over_time`, trailing commas, `stddev`/`quantile`).
    #[test]
    fn grafana_promql_compatibility() {
        // The universe: a gauge with two label values, a counter, buckets.
        let mk = |pairs: &[(&str, &str)], samples: &[(i64, f64)]| {
            let mut labels: Labels =
                pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
            labels.sort();
            let samples: Vec<Sample> =
                samples.iter().map(|&(ts, value)| Sample { ts, value }).collect();
            (labels, samples)
        };
        let universe: Vec<(Labels, Vec<Sample>)> = vec![
            mk(&[("name", "aqi"), ("device_id", "a")], &[(1_000, 10.0), (5_000, 14.0), (9_000, 12.0)]),
            mk(&[("name", "aqi"), ("device_id", "b")], &[(1_000, 20.0), (5_000, 24.0), (9_000, 22.0)]),
            mk(&[("name", "reqs_total"), ("job", "api")], &[(1_000, 100.0), (5_000, 140.0), (9_000, 200.0)]),
            mk(&[("name", "lat_bucket"), ("le", "50")], &[(1_000, 0.0), (9_000, 5.0)]),
            mk(&[("name", "lat_bucket"), ("le", "100")], &[(1_000, 0.0), (9_000, 10.0)]),
            mk(&[("name", "lat_bucket"), ("le", "+Inf")], &[(1_000, 0.0), (9_000, 10.0)]),
        ];
        // Apply matcher semantics like the store does (missing label = "").
        let matches = |ms: &[Matcher], labels: &Labels| {
            let get = |k: &str| {
                labels.iter().find(|(lk, _)| lk == k).map_or("", |(_, v)| v.as_str())
            };
            ms.iter().all(|m| match m {
                Matcher::Eq(k, v) => get(k) == v,
                Matcher::Ne(k, v) => get(k) != v,
                Matcher::Re(k, r) => r.is_match(get(k)),
                Matcher::NotRe(k, r) => !r.is_match(get(k)),
            })
        };
        // Parse, fetch (per spec, matcher-filtered), evaluate at t = 10s.
        let eval = |q: &str| -> Result<Vec<(Labels, f64)>, String> {
            let mut e = P::new(q).parse()?;
            let mut specs = Vec::new();
            assign_slots(&mut e, &mut specs);
            let fetched: Vec<Fetched> = specs
                .iter()
                .map(|(ms, _, _)| Fetched {
                    series: universe
                        .iter()
                        .filter(|(labels, _)| matches(ms, labels))
                        .cloned()
                        .collect(),
                })
                .collect();
            match eval_steps(&e, &fetched, &[10_000])?.pop() {
                Some(StepVal::Vector(v)) => Ok(v),
                Some(StepVal::Scalar(v)) => Ok(vec![(Vec::new(), v)]),
                None => Ok(Vec::new()),
            }
        };
        let one = |q: &str| -> f64 {
            let v = eval(q).unwrap_or_else(|e| panic!("{q}: {e}"));
            assert_eq!(v.len(), 1, "{q}: expected one series, got {v:?}");
            v[0].1
        };

        // Datasource health check.
        assert_eq!(one("1+1"), 2.0);
        // Plain and filtered selectors, bare regex selectors.
        assert_eq!(eval("aqi").unwrap().len(), 2);
        assert_eq!(eval(r#"aqi{device_id="a"}"#).unwrap().len(), 1);
        assert_eq!(eval(r#"{__name__=~"aqi|reqs.*"}"#).unwrap().len(), 3);
        // Drilldown gauge tile (the verbatim production shape).
        assert_eq!(one(r#"avg(avg_over_time(aqi{__ignore_usage__="", }[10s]))"#), 17.0);
        // Function-picker previews: average / sum / min-max / stddev /
        // percentiles.
        assert_eq!(one(r#"avg(aqi{__ignore_usage__="", })"#), 17.0);
        assert_eq!(one(r#"sum(aqi{__ignore_usage__="", })"#), 34.0);
        assert_eq!(one(r#"min(aqi{__ignore_usage__="", })"#), 12.0);
        assert_eq!(one(r#"max(aqi{__ignore_usage__="", })"#), 22.0);
        assert_eq!(one(r#"stddev(aqi{__ignore_usage__="", })"#), 5.0);
        assert_eq!(one(r#"quantile(0.5, aqi{__ignore_usage__="", })"#), 17.0);
        assert_eq!(one(r#"quantile(0.99, aqi{__ignore_usage__="", })"#), 21.9);
        assert_eq!(one(r#"quantile(1, aqi{})"#), 22.0);
        // Breakdown tab: per-label grouping of the tile query.
        let by = eval(r#"avg by (device_id) (avg_over_time(aqi{__ignore_usage__="", }[10s]))"#)
            .unwrap();
        assert_eq!(by.len(), 2, "{by:?}");
        // Counter tile.
        let rate = one(r#"sum(rate(reqs_total{__ignore_usage__="", }[10s]))"#);
        assert!(rate > 0.0, "{rate}");
        // Other *_over_time picker options.
        assert_eq!(one(r#"max(max_over_time(aqi{}[10s]))"#), 24.0);
        assert_eq!(one(r#"sum(count_over_time(aqi{}[10s]))"#), 6.0);
        assert_eq!(eval(r#"last_over_time(aqi{}[10s])"#).unwrap().len(), 2);
        // Histogram panels.
        let hq = one(r#"histogram_quantile(0.9, sum by (le) (rate(lat_bucket{}[10s])))"#);
        assert!(hq > 50.0 && hq <= 100.0, "{hq}");
        // Classic dashboard staples (rate = increase / window: 100/10s → ×60).
        assert_eq!(one(r#"sum by (job) (rate(reqs_total{job="api"}[10s])) * 60"#).round(), 600.0);
        // "Last reading" stat panels: timestamp() surfaces the sample time
        // (unix seconds; the dashboard multiplies by 1000 for a Time field),
        // and time() - timestamp() is staleness.
        assert_eq!(one(r#"max(timestamp(aqi{device_id=~"a"})) * 1000"#), 9_000.0);
        assert_eq!(one("time()"), 10.0);
        assert_eq!(one(r#"time() - max(timestamp(aqi{device_id="a"}))"#), 1.0);
        // timestamp() over a computed vector is a clear error, not a wrong
        // answer; quantile without a scalar first argument likewise.
        assert!(eval("timestamp(avg(aqi))").is_err());
        assert!(eval("quantile(aqi)").is_err());

        // ---- comparisons (filter vs bool) ----
        // aqi at t=10s: device a = 12, device b = 22.
        let gt = eval("aqi > 15").unwrap();
        assert_eq!(gt.len(), 1, "{gt:?}");
        assert_eq!(gt[0].1, 22.0);
        // Filtering keeps the sample untouched, metric name included.
        assert!(gt[0].0.iter().any(|(k, v)| k == "__name__" && v == "aqi"));
        // `bool` turns the comparison 0/1-valued (and drops the name).
        let gtb = eval("aqi > bool 15").unwrap();
        assert_eq!(gtb.len(), 2);
        let mut bools: Vec<f64> = gtb.iter().map(|(_, v)| *v).collect();
        bools.sort_by(f64::total_cmp);
        assert_eq!(bools, vec![0.0, 1.0]);
        assert!(gtb[0].0.iter().all(|(k, _)| k != "__name__"));
        // Scalar comparisons demand `bool`, like Prometheus.
        assert_eq!(one("2 > bool 1"), 1.0);
        assert!(eval("2 > 1").is_err());
        // The drilldown's extreme-values filter — its exact production shape.
        assert_eq!(
            eval(r#"aqi{__ignore_usage__="", } and aqi{__ignore_usage__="", } > -Inf"#)
                .map(|v| v.len()),
            Ok(2)
        );

        // ---- set operators ----
        assert_eq!(eval("aqi or reqs_total").unwrap().len(), 3);
        assert_eq!(eval(r#"aqi and aqi{device_id="a"}"#).unwrap().len(), 1);
        let unless = eval(r#"aqi unless aqi{device_id="a"}"#).unwrap();
        assert_eq!(unless.len(), 1);
        assert_eq!(unless[0].1, 22.0);
        // `or` keeps lhs on key collisions.
        assert_eq!(eval("aqi or aqi").unwrap().len(), 2);

        // ---- topk / bottomk (select, don't fold) ----
        let top = eval("topk(1, aqi)").unwrap();
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].1, 22.0);
        assert!(top[0].0.iter().any(|(k, v)| k == "__name__" && v == "aqi"));
        assert!(top[0].0.iter().any(|(k, v)| k == "device_id" && v == "b"));
        assert_eq!(one("bottomk(1, aqi)"), 12.0);
        assert_eq!(eval("topk(5, aqi)").unwrap().len(), 2, "k > n keeps all");

        // ---- irate / idelta (last two samples in the window) ----
        // reqs_total: … (5s, 140), (9s, 200) → (200-140)/4s and a raw diff.
        assert_eq!(one("irate(reqs_total[10s])"), 15.0);
        assert_eq!(one("idelta(reqs_total[10s])"), 60.0);

        // ---- label_replace / label_join ----
        // (${1}, not $1X — `$1X` reads as the group named "1X", like Go.)
        let lr = eval(r#"label_replace(aqi{device_id="a"}, "dev", "${1}X", "device_id", "(.*)")"#)
            .unwrap();
        assert!(lr[0].0.iter().any(|(k, v)| k == "dev" && v == "aX"), "{lr:?}");
        // A non-matching regex leaves the series untouched.
        let lr = eval(r#"label_replace(aqi{device_id="a"}, "dev", "X", "device_id", "zzz")"#)
            .unwrap();
        assert!(lr[0].0.iter().all(|(k, _)| k != "dev"));
        // An invalid regex or destination label is a parse error.
        assert!(eval(r#"label_replace(aqi, "dev", "X", "device_id", "[")"#).is_err());
        assert!(eval(r#"label_replace(aqi, "not-a-label!", "X", "device_id", ".*")"#).is_err());
        let lj = eval(r#"label_join(aqi{device_id="a"}, "combo", "-", "device_id", "device_id")"#)
            .unwrap();
        assert!(lj[0].0.iter().any(|(k, v)| k == "combo" && v == "a-a"), "{lj:?}");

        // ---- sort / sort_desc (instant-query ordering) ----
        let sorted = eval("sort_desc(aqi)").unwrap();
        assert_eq!(sorted.iter().map(|(_, v)| *v).collect::<Vec<_>>(), vec![22.0, 12.0]);
        let sorted = eval("sort(aqi)").unwrap();
        assert_eq!(sorted.iter().map(|(_, v)| *v).collect::<Vec<_>>(), vec![12.0, 22.0]);

        // ---- absent() (alerting on missing data) ----
        assert_eq!(eval("absent(aqi)").unwrap().len(), 0);
        let ab = eval(r#"absent(nope{job="x"})"#).unwrap();
        assert_eq!(ab, vec![(vec![("job".to_string(), "x".to_string())], 1.0)]);
        assert_eq!(eval("absent(nope)").unwrap(), vec![(Vec::new(), 1.0)]);
        // Ambiguous `=` constraints drop out of the derived labels.
        let ab = eval(r#"absent(nope{job="x", job="y", dc="ny"})"#).unwrap();
        assert_eq!(ab, vec![(vec![("dc".to_string(), "ny".to_string())], 1.0)]);

        // ---- Tier 2: window analytics ----
        // aqi device a in [0, 10s]: (1s, 10), (5s, 14), (9s, 12).
        assert_eq!(eval("present_over_time(aqi[10s])").unwrap().len(), 2);
        assert_eq!(eval("absent_over_time(aqi[10s])").unwrap().len(), 0);
        assert_eq!(
            eval(r#"absent_over_time(nope{job="x"}[10s])"#).unwrap(),
            vec![(vec![("job".to_string(), "x".to_string())], 1.0)]
        );
        assert_eq!(one(r#"changes(aqi{device_id="a"}[10s])"#), 2.0);
        assert_eq!(one(r#"resets(aqi{device_id="a"}[10s])"#), 1.0, "14 -> 12 dips once");
        assert_eq!(one("resets(reqs_total[10s])"), 0.0, "monotone counter");
        // Least squares over x = seconds before eval (-9, -5, -1),
        // y = (10, 14, 12): slope 0.25; regressed value now = 13.25.
        assert_eq!(one(r#"deriv(aqi{device_id="a"}[10s])"#), 0.25);
        assert_eq!(one(r#"predict_linear(aqi{device_id="a"}[10s], 4)"#), 14.25);
        let sv = one(r#"stdvar_over_time(aqi{device_id="a"}[10s])"#);
        assert!((sv - 8.0 / 3.0).abs() < 1e-9, "{sv}");
        let sd = one(r#"stddev_over_time(aqi{device_id="a"}[10s])"#);
        assert!((sd - (8.0f64 / 3.0).sqrt()).abs() < 1e-9, "{sd}");
        assert_eq!(one(r#"mad_over_time(aqi{device_id="a"}[10s])"#), 2.0);
        assert_eq!(one(r#"quantile_over_time(0.5, aqi{device_id="a"}[10s])"#), 12.0);

        // ---- Tier 2: aggregations ----
        assert_eq!(one("stdvar(aqi)"), 25.0, "values 12 and 22");
        assert_eq!(one("group(aqi)"), 1.0);
        let cv = eval(r#"count_values("v", aqi)"#).unwrap();
        assert_eq!(cv.len(), 2, "{cv:?}");
        assert!(cv.contains(&(vec![("v".to_string(), "12".to_string())], 1.0)), "{cv:?}");
        assert!(cv.contains(&(vec![("v".to_string(), "22".to_string())], 1.0)), "{cv:?}");
        let cv = eval(r#"count_values by (device_id) ("v", aqi)"#).unwrap();
        assert_eq!(cv.len(), 2);
        assert!(cv.iter().all(|(labels, n)| labels.len() == 2 && *n == 1.0), "{cv:?}");
    }

    /// Grafana's Metrics Drilldown emits `{__ignore_usage__="", }` — a
    /// no-op empty-value matcher AND a trailing comma (its filters variable
    /// interpolated empty). Both must parse; the empty-value Eq matches
    /// series lacking the label.
    #[test]
    fn matcher_block_accepts_drilldown_shapes() {
        // The exact tile expression that failed with "expected label name".
        let mut e = P::new(r#"avg(avg_over_time(aqi{__ignore_usage__="", }[4m]))"#)
            .parse()
            .unwrap();
        let mut specs = Vec::new();
        assign_slots(&mut e, &mut specs);
        assert_eq!(specs.len(), 1);
        assert!(specs[0].0.iter().any(
            |m| matches!(m, Matcher::Eq(k, v) if k == "__ignore_usage__" && v.is_empty())
        ));
        let fetched = vec![Fetched {
            series: vec![(
                vec![("name".to_string(), "aqi".to_string())],
                vec![Sample { ts: 9_000, value: 14.0 }],
            )],
        }];
        let vals = eval_steps(&e, &fetched, &[10_000]).unwrap();
        let StepVal::Vector(v) = &vals[0] else { panic!() };
        assert_eq!(v[0].1, 14.0);
        // Trailing-comma / whitespace variants all parse.
        for q in [
            r#"m{a="b",}"#,
            r#"m{ a="b" , c!="d" , }"#,
            "m{ }",
            "m{}",
        ] {
            P::new(q).parse().unwrap_or_else(|e| panic!("{q}: {e}"));
        }
        // A lone comma is still malformed, like Prometheus.
        assert!(P::new("m{,}").parse().is_err());
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
