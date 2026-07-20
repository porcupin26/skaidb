//! Time-series INSERT/SELECT execution (docs/TODO.md phase 2).
//!
//! INSERT maps rows to `(labels, ts, value)` samples — one per numeric field,
//! the field name carried as a reserved `__field__` label. SELECT extracts a
//! time range + label matchers from `WHERE` for pushdown, gathers samples,
//! rebuilds one document per `(series, ts)`, and hands the docs to the
//! ordinary projection/aggregation machinery — so `GROUP BY time_bucket(...)`,
//! `HAVING`, `ORDER BY`, `DISTINCT`, and `LIMIT` behave exactly as on any
//! table, and `rate()`/`increase()`/`first()`/`last()` see the hidden
//! `__series__`/`ts` fields they need.

use std::collections::{BTreeMap, HashSet};

use skaidb_sql::ast::{AggArg, AggFunc, BinaryOp, Expr, Insert, Select, SelectItem};
use skaidb_tsdb::{Labels, Matcher, Sample};
use skaidb_types::{Document, Value};

use crate::error::{EngineError, Result};
use crate::eval::{as_int_ms, eval, eval_predicate};
use crate::exec::{expr_name, is_grouped, project, Cluster};
use crate::result::{QueryOutput, ResultSet};

/// The reserved label carrying a sample's field name.
const FIELD_LABEL: &str = "__field__";
/// The hidden per-row field identifying a sample's series (used by
/// `rate()`-family aggregates; hidden from `SELECT *`).
const SERIES_FIELD: &str = "__series__";

pub(crate) fn run_ts_insert(
    ins: &Insert,
    series_key: &[String],
    cluster: &mut dyn Cluster,
) -> Result<QueryOutput> {
    let empty = Document::new();
    let mut rows: Vec<(Labels, i64, f64)> = Vec::new();
    for row in &ins.rows {
        let mut doc = Document::new();
        for (col, expr) in ins.columns.iter().zip(row) {
            doc.insert(col.clone(), eval(expr, &empty)?);
        }
        let mut labels: Labels = Vec::with_capacity(series_key.len() + 1);
        for col in series_key {
            match doc.get(col) {
                Some(Value::String(s)) => labels.push((col.clone(), s.clone())),
                Some(other) => {
                    return Err(EngineError::Type(format!(
                        "series key column {col} must be a string, found {:?}",
                        other.type_of()
                    )))
                }
                None => {
                    return Err(EngineError::Constraint(format!(
                        "series key column {col} is required on every insert"
                    )))
                }
            }
        }
        let ts = match doc.get("ts") {
            Some(v) => as_int_ms(v).ok_or_else(|| {
                EngineError::Type("ts must be a timestamp or integer milliseconds".into())
            })?,
            None => {
                return Err(EngineError::Constraint(
                    "column ts is required on every insert".into(),
                ))
            }
        };
        let mut fields = 0usize;
        for (col, val) in &doc.0 {
            if col == "ts" || series_key.contains(col) {
                continue;
            }
            if col.starts_with("__") {
                return Err(EngineError::Constraint(format!(
                    "column names starting with __ are reserved ({col})"
                )));
            }
            let v = match val {
                Value::Int(i) => *i as f64,
                Value::Float(f) => *f,
                other => {
                    return Err(EngineError::Type(format!(
                        "field {col} must be numeric, found {:?}",
                        other.type_of()
                    )))
                }
            };
            let mut l = labels.clone();
            l.push((FIELD_LABEL.to_string(), col.clone()));
            l.sort();
            rows.push((l, ts, v));
            fields += 1;
        }
        if fields == 0 {
            return Err(EngineError::Constraint(
                "insert must set at least one numeric field besides the series key and ts".into(),
            ));
        }
    }
    let affected = ins.rows.len();
    cluster.ts_append(&ins.table, &rows)?;
    Ok(QueryOutput::Mutation { affected })
}

pub(crate) fn run_ts_select(
    sel: &Select,
    series_key: &[String],
    cluster: &dyn Cluster,
) -> Result<ResultSet> {
    if sel.nearest.is_some() {
        return Err(EngineError::Unsupported(
            "NEAREST is not supported on time-series tables".into(),
        ));
    }
    if !sel.joins.is_empty() || !sel.set_ops.is_empty() {
        return Err(EngineError::Unsupported(
            "JOIN/UNION are not supported on time-series tables".into(),
        ));
    }

    // `GROUP BY t` / `ORDER BY t` where `t` is an output alias
    // (`time_bucket(1m, ts) AS t`) is the canonical time-series shape;
    // resolve such aliases to their expressions before executing.
    let sel = &resolve_output_aliases(sel);

    // Pushdown: a time range and label matchers from AND-reachable
    // comparisons. Everything else stays in the residual filter, which is
    // re-applied in full over the rebuilt documents below.
    let mut t0 = i64::MIN;
    let mut t1 = i64::MAX;
    let mut matchers: Vec<Matcher> = Vec::new();
    if let Some(filter) = &sel.filter {
        extract_pushdown(filter, series_key, &mut t0, &mut t1, &mut matchers);
    }

    // Partial-aggregate pushdown: an aggregation whose WHERE is entirely
    // served by the pushdown gathers per-series per-bucket partials from the
    // cluster (one replica's answer per series) instead of raw samples. Any
    // failure or dynamically detected ineligibility falls back to the raw
    // path below, which is authoritative.
    if let Some(plan) = partials_plan(sel) {
        if let Ok(Some(rs)) = run_partials(sel, &plan, &matchers, t0, t1, cluster) {
            return Ok(rs);
        }
    }

    // Which value fields does the query touch? `None` = all (wildcard, or
    // nothing referenced — e.g. `COUNT(*)`).
    let fields = referenced_fields(sel, series_key);
    let mut gathered = Vec::new();
    match &fields {
        Some(names) => {
            for f in names {
                let mut m = matchers.clone();
                m.push(Matcher::Eq(FIELD_LABEL.into(), f.clone()));
                gathered.extend(cluster.ts_query(&sel.from, &m, t0, t1)?);
            }
        }
        None => gathered = cluster.ts_query(&sel.from, &matchers, t0, t1)?,
    }

    // Rebuild documents: one per (series, ts), fields merged across streams,
    // ordered series-then-time so `rate()` sees per-series runs in order.
    // Every sample charges the statement's scan budget — the raw gather
    // materializes at the coordinator (like any row gather), and an
    // unbounded `SELECT * FROM <ts>` over a large range must fail with the
    // budget error, not grow until the OOM killer arrives. Aggregations
    // took the bounded partials path above and never get here.
    let mut merged: BTreeMap<(Vec<u8>, i64), Document> = BTreeMap::new();
    for (labels, samples) in gathered {
        crate::scan_meter::tick(samples.len())?;
        let field = labels
            .iter()
            .find(|(k, _)| k == FIELD_LABEL)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| "value".into());
        let series: Labels = labels
            .iter()
            .filter(|(k, _)| k != FIELD_LABEL)
            .cloned()
            .collect();
        let sid = series_id(&series);
        let tag = series
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        for s in samples {
            let doc = merged.entry((sid.clone(), s.ts)).or_insert_with(|| {
                let mut d = Document::new();
                for (k, v) in &series {
                    d.insert(k.clone(), Value::String(v.clone()));
                }
                d.insert("ts", Value::Timestamp(s.ts));
                d.insert(SERIES_FIELD, Value::String(tag.clone()));
                d
            });
            doc.insert(field.clone(), Value::Float(s.value));
        }
    }

    // Residual filtering with full SQL semantics (pushdown only narrowed the
    // gather; re-applying the whole predicate keeps NULL handling exact).
    let mut docs = Vec::with_capacity(merged.len());
    for (_, doc) in merged {
        let keep = match &sel.filter {
            Some(f) => eval_predicate(f, &doc)?,
            None => true,
        };
        if keep {
            docs.push(doc);
        }
    }

    let mut hide = HashSet::new();
    hide.insert(SERIES_FIELD.to_string());
    project(sel, docs, &hide, true)
}

/// Replace top-level `Column(name)` references in GROUP BY / ORDER BY that
/// name a select-item alias with that item's expression.
fn resolve_output_aliases(sel: &Select) -> Select {
    let aliases: Vec<(&String, &Expr)> = sel
        .items
        .iter()
        .filter_map(|i| match i {
            SelectItem::Expr {
                expr,
                alias: Some(a),
            } => Some((a, expr)),
            _ => None,
        })
        .collect();
    if aliases.is_empty() {
        return sel.clone();
    }
    let mut out = sel.clone();
    let fix = |e: &mut Expr| {
        if let Expr::Column(name) = e {
            if let Some((_, target)) = aliases.iter().find(|(a, _)| *a == name) {
                *e = (*target).clone();
            }
        }
    };
    for g in &mut out.group_by {
        fix(g);
    }
    for o in &mut out.order_by {
        fix(&mut o.expr);
    }
    out
}

/// Order-preserving series identity for grouping samples across field
/// streams.
fn series_id(series: &Labels) -> Vec<u8> {
    let mut id = Vec::new();
    for (k, v) in series {
        id.extend_from_slice(k.as_bytes());
        id.push(0);
        id.extend_from_slice(v.as_bytes());
        id.push(0);
    }
    id
}

/// Fold a constant (column-free) expression to a millisecond value.
fn const_ms(e: &Expr) -> Option<i64> {
    if expr_has_columns(e) {
        return None;
    }
    let v = eval(e, &Document::new()).ok()?;
    as_int_ms(&v)
}

fn expr_has_columns(e: &Expr) -> bool {
    let mut found = false;
    walk(e, &mut |x| {
        if matches!(x, Expr::Column(_)) {
            found = true;
        }
    });
    found
}

fn walk(e: &Expr, f: &mut impl FnMut(&Expr)) {
    f(e);
    match e {
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => walk(expr, f),
        Expr::Binary { left, right, .. } => {
            walk(left, f);
            walk(right, f);
        }
        Expr::Aggregate { arg, .. } => {
            if let skaidb_sql::ast::AggArg::Expr(expr) = arg {
                walk(expr, f);
            }
        }
        Expr::Func { args, .. } => {
            for a in args {
                walk(a, f);
            }
        }
        Expr::InList { expr, list, .. } => {
            walk(expr, f);
            for a in list {
                walk(a, f);
            }
        }
        Expr::Between { expr, lo, hi, .. } => {
            walk(expr, f);
            walk(lo, f);
            walk(hi, f);
        }
        Expr::Like { expr, pattern, .. } => {
            walk(expr, f);
            walk(pattern, f);
        }
        Expr::Literal(_) | Expr::Column(_) | Expr::Parameter(_) => {}
    }
}

fn extract_pushdown(
    e: &Expr,
    series_key: &[String],
    t0: &mut i64,
    t1: &mut i64,
    matchers: &mut Vec<Matcher>,
) {
    match e {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            extract_pushdown(left, series_key, t0, t1, matchers);
            extract_pushdown(right, series_key, t0, t1, matchers);
        }
        Expr::Binary { op, left, right } => {
            let (col, op, rhs) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), rhs) if !expr_has_columns(rhs) => (c, *op, rhs),
                (lhs, Expr::Column(c)) if !expr_has_columns(lhs) => (c, flip(*op), lhs),
                _ => return,
            };
            if col == "ts" {
                let Some(v) = const_ms(rhs) else { return };
                match op {
                    BinaryOp::GtEq => *t0 = (*t0).max(v),
                    BinaryOp::Gt => *t0 = (*t0).max(v.saturating_add(1)),
                    BinaryOp::LtEq => *t1 = (*t1).min(v),
                    BinaryOp::Lt => *t1 = (*t1).min(v.saturating_sub(1)),
                    BinaryOp::Eq => {
                        *t0 = (*t0).max(v);
                        *t1 = (*t1).min(v);
                    }
                    _ => {}
                }
            } else if !col.contains('.') && !col.starts_with("__") {
                // Any string equality pushes down as a label matcher — not
                // just declared series-key columns, so dynamically-labeled
                // series (e.g. remote_write ingest) filter at the store.
                // Safe: the full WHERE re-applies afterward, a matcher can
                // only widen relative to SQL NULL semantics, and a *field*
                // column compared to a string matches nothing either way.
                let _ = series_key; // placement declares labels; matching is dynamic
                let Ok(v) = eval(rhs, &Document::new()) else {
                    return;
                };
                if let Value::String(s) = v {
                    match op {
                        BinaryOp::Eq => matchers.push(Matcher::Eq(col.clone(), s)),
                        BinaryOp::NotEq => matchers.push(Matcher::Ne(col.clone(), s)),
                        _ => {}
                    }
                }
            }
        }
        _ => {}
    }
}

fn flip(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}

/// One bucket's pre-aggregated summary of one series (docs/TODO.md
/// partial-aggregate pushdown): everything the supported SQL aggregates need,
/// so a cluster query ships one row per (series, bucket) instead of raw
/// samples. `increase` is the counter-reset-aware within-bucket increase and
/// is meaningful only when `count >= 2` (`rate`/`delta` derive from it and
/// the first/last pairs at fold time).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TsPartial {
    pub bucket_ts: i64,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub first_ts: i64,
    pub first_val: f64,
    pub last_ts: i64,
    pub last_val: f64,
    pub increase: f64,
}

/// Fold time-ordered per-series samples into per-bucket partials.
/// `bucket_ms <= 0` means one whole-range bucket (`bucket_ts` 0). Buckets
/// floor like SQL `time_bucket` (`div_euclid`).
pub fn ts_partialize(
    series: Vec<(Labels, Vec<Sample>)>,
    bucket_ms: i64,
) -> Vec<(Labels, Vec<TsPartial>)> {
    let mut out = Vec::with_capacity(series.len());
    for (labels, samples) in series {
        let mut partials: Vec<TsPartial> = Vec::new();
        for s in samples {
            let bucket_ts = if bucket_ms > 0 {
                s.ts.div_euclid(bucket_ms) * bucket_ms
            } else {
                0
            };
            match partials.last_mut() {
                Some(p) if p.bucket_ts == bucket_ts => {
                    p.count += 1;
                    p.sum += s.value;
                    p.min = p.min.min(s.value);
                    p.max = p.max.max(s.value);
                    // Counter increase: a drop is a reset — the counter
                    // restarted, so the new value is its own contribution.
                    p.increase += if s.value >= p.last_val {
                        s.value - p.last_val
                    } else {
                        s.value
                    };
                    p.last_ts = s.ts;
                    p.last_val = s.value;
                }
                _ => partials.push(TsPartial {
                    bucket_ts,
                    count: 1,
                    sum: s.value,
                    min: s.value,
                    max: s.value,
                    first_ts: s.ts,
                    first_val: s.value,
                    last_ts: s.ts,
                    last_val: s.value,
                    increase: 0.0,
                }),
            }
        }
        if !partials.is_empty() {
            out.push((labels, partials));
        }
    }
    out
}

/// What an eligible aggregation pushes down (see [`partials_plan`]).
struct PartialsPlan {
    /// Group `time_bucket` step; 0 = no bucketing (whole range per group).
    bucket_ms: i64,
    /// The group-by `time_bucket` expression, verbatim.
    bucket_expr: Option<Expr>,
    /// Label columns grouped by.
    group_cols: Vec<String>,
    /// Distinct fields the aggregates read (empty = pure label grouping).
    agg_fields: Vec<String>,
    /// Whether any `last()` aggregate needs the extra per-partial doc.
    needs_last: bool,
    /// Whether `rate()`/`increase()`/`delta()` appear — those need raw
    /// samples (rollups don't store within-bucket increase), so they never
    /// route to a rollup.
    has_change_aggs: bool,
}

/// Decide whether `sel` (aliases already resolved) can be answered from
/// per-series per-bucket partials with semantics identical to the raw path:
/// an aggregate/grouped query whose WHERE is entirely consumed by the
/// pushdown, grouping by labels and at most one `time_bucket(step, ts)`, with
/// every aggregate a supported function over a plain field and every bare
/// column reference a group key. Anything else returns `None` (raw path).
fn partials_plan(sel: &Select) -> Option<PartialsPlan> {
    if !is_grouped(sel) || sel.items.iter().any(|i| matches!(i, SelectItem::Wildcard)) {
        return None;
    }
    if let Some(f) = &sel.filter {
        if !fully_pushable(f) {
            return None;
        }
    }
    let mut bucket_ms = 0i64;
    let mut bucket_expr: Option<Expr> = None;
    let mut group_cols: Vec<String> = Vec::new();
    for g in &sel.group_by {
        match g {
            Expr::Column(c) if c != "ts" && !c.contains('.') && !c.starts_with("__") => {
                group_cols.push(c.clone())
            }
            e => {
                let step = bucket_step(e)?;
                if bucket_expr.is_some() {
                    return None;
                }
                bucket_ms = step;
                bucket_expr = Some(e.clone());
            }
        }
    }
    let mut plan = PartialsPlan {
        bucket_ms,
        bucket_expr,
        group_cols,
        agg_fields: Vec::new(),
        needs_last: false,
        has_change_aggs: false,
    };
    for item in &sel.items {
        let SelectItem::Expr { expr, .. } = item else { return None };
        if !plan_expr_ok(expr, &mut plan) {
            return None;
        }
    }
    if let Some(h) = &sel.having {
        if !plan_expr_ok(h, &mut plan) {
            return None;
        }
    }
    for o in &sel.order_by {
        if !plan_expr_ok(&o.expr, &mut plan) {
            return None;
        }
    }
    Some(plan)
}

/// `time_bucket(<positive const step>, ts)` → the step in ms.
fn bucket_step(e: &Expr) -> Option<i64> {
    let Expr::Func { name, args } = e else { return None };
    if name != "time_bucket" || args.len() != 2 {
        return None;
    }
    if !matches!(&args[1], Expr::Column(c) if c == "ts") {
        return None;
    }
    let step = const_ms(&args[0])?;
    (step > 0).then_some(step)
}

/// Validate one output/HAVING/ORDER BY expression for the partials plan,
/// collecting aggregate fields. Bare columns must be group keys; `ts` may
/// appear only as the group's `time_bucket` expression.
fn plan_expr_ok(e: &Expr, plan: &mut PartialsPlan) -> bool {
    if plan.bucket_expr.as_ref() == Some(e) {
        return true;
    }
    match e {
        Expr::Aggregate { func, arg } => {
            let AggArg::Expr(inner) = arg else {
                return false; // COUNT(*) falls back
            };
            let Expr::Column(f) = inner.as_ref() else {
                return false; // computed aggregate args fall back
            };
            if f == "ts" || f.contains('.') || f.starts_with("__") {
                return false;
            }
            // No partial exists for a percentile — exact row path only.
            if matches!(func, AggFunc::Percentile(_)) {
                return false;
            }
            if !plan.agg_fields.contains(f) {
                plan.agg_fields.push(f.clone());
            }
            if *func == AggFunc::Last {
                plan.needs_last = true;
            }
            if matches!(func, AggFunc::Rate | AggFunc::Increase | AggFunc::Delta) {
                plan.has_change_aggs = true;
            }
            true
        }
        Expr::Column(c) => c != "ts" && plan.group_cols.contains(c),
        Expr::Literal(_) => true,
        Expr::Parameter(_) => false,
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => plan_expr_ok(expr, plan),
        Expr::Binary { left, right, .. } => {
            plan_expr_ok(left, plan) && plan_expr_ok(right, plan)
        }
        Expr::Func { args, .. } => args.iter().all(|a| plan_expr_ok(a, plan)),
        // `IN`/`BETWEEN`/`LIKE` predicates aren't part of the partials fast
        // path; fall back to the exact row path.
        Expr::InList { .. } | Expr::Between { .. } | Expr::Like { .. } => false,
    }
}

/// Whether the whole WHERE is consumed by [`extract_pushdown`] — an AND-tree
/// of `ts` range comparisons against integral constants and label `=`/`!=`
/// string constants — so skipping the residual re-application is sound
/// (given the series-level label-presence filter applied at fold time).
fn fully_pushable(e: &Expr) -> bool {
    match e {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => fully_pushable(left) && fully_pushable(right),
        Expr::Binary { op, left, right } => {
            let (col, op, rhs) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), rhs) if !expr_has_columns(rhs) => (c, *op, rhs),
                (lhs, Expr::Column(c)) if !expr_has_columns(lhs) => (c, flip(*op), lhs),
                _ => return false,
            };
            if col == "ts" {
                // A fractional bound truncates in the pushdown (the raw
                // path's residual would fix it up) — not exactly servable.
                let Ok(v) = eval(rhs, &Document::new()) else {
                    return false;
                };
                if let Value::Float(f) = &v {
                    if f.fract() != 0.0 {
                        return false;
                    }
                }
                as_int_ms(&v).is_some()
                    && matches!(
                        op,
                        BinaryOp::GtEq
                            | BinaryOp::Gt
                            | BinaryOp::LtEq
                            | BinaryOp::Lt
                            | BinaryOp::Eq
                    )
            } else if !col.contains('.') && !col.starts_with("__") {
                matches!(op, BinaryOp::Eq | BinaryOp::NotEq)
                    && matches!(eval(rhs, &Document::new()), Ok(Value::String(_)))
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Execute the partials plan: gather per-series per-bucket partials at the
/// read consistency, verify the dynamic eligibility conditions, synthesize
/// partial documents, and run the aggregate-rewritten query over them.
/// `Ok(None)` = dynamically ineligible (caller falls back to raw samples).
fn run_partials(
    sel: &Select,
    plan: &PartialsPlan,
    matchers: &[Matcher],
    t0: i64,
    t1: i64,
    cluster: &dyn Cluster,
) -> Result<Option<ResultSet>> {
    let mut gathered: Vec<(Labels, Vec<TsPartial>)> = Vec::new();
    // Tiered read: group buckets older than the source's retention horizon
    // (where blocks may already be dropped) are answered from the coarsest
    // rollup whose bucket divides the group step; everything newer keeps
    // exact source partials. The straddling bucket merges both halves
    // through the ordinary group fold (partials are additive).
    let mut src_t0 = t0;
    if !plan.has_change_aggs && !plan.agg_fields.is_empty() {
        if let Some((rollup, bucket, split)) = rollup_route(&sel.from, plan, t0, cluster)? {
            gathered.extend(rollup_partials(
                &rollup, bucket, plan, matchers, t0, t1, split, cluster,
            )?);
            src_t0 = split;
        }
    }
    if plan.agg_fields.is_empty() {
        gathered = cluster.ts_partials(&sel.from, matchers, src_t0, t1, plan.bucket_ms)?;
    } else if src_t0 <= t1 {
        for f in &plan.agg_fields {
            let mut m = matchers.to_vec();
            m.push(Matcher::Eq(FIELD_LABEL.into(), f.clone()));
            gathered.extend(cluster.ts_partials(&sel.from, &m, src_t0, t1, plan.bucket_ms)?);
        }
    }

    // Dynamic eligibility: a grouped-by column that is actually a field, or
    // an aggregated field that is actually a label, has per-sample semantics
    // partials cannot reproduce — fall back to the raw path.
    for (labels, _) in &gathered {
        for (k, v) in labels {
            if k == FIELD_LABEL {
                if plan.group_cols.contains(v) {
                    return Ok(None);
                }
            } else if plan.agg_fields.contains(k) {
                return Ok(None);
            }
        }
    }

    // The raw path's residual WHERE drops rows whose series lacks a label the
    // filter compares (`NULL = 'x'` / `NULL != 'x'` are not true), while the
    // store matches a missing label as `""`. Reproduce the residual: every
    // matcher-referenced label must exist on the series.
    let matcher_keys: Vec<&String> = matchers
        .iter()
        .map(|m| match m {
            Matcher::Eq(k, _) | Matcher::Ne(k, _) => k,
            // The SQL surface never builds regex matchers; keys still count.
            Matcher::Re(k, _) | Matcher::NotRe(k, _) => k,
        })
        .filter(|k| *k != FIELD_LABEL)
        .collect();
    gathered.retain(|(labels, _)| {
        matcher_keys
            .iter()
            .all(|k| labels.iter().any(|(lk, _)| lk == *k))
    });

    let docs = partial_docs(gathered, &plan.agg_fields, plan.needs_last);
    let rewritten = rewrite_partials(sel);
    let mut hide = HashSet::new();
    hide.insert(SERIES_FIELD.to_string());
    project(&rewritten, docs, &hide, true).map(Some)
}

/// Decide whether (and how) part of `[t0, ..]` routes to a rollup, and
/// where the source takes over. Two tiers, both requiring a rollup whose
/// bucket divides the group step (any rollup for whole-range groups):
///
/// - **Required** (below the retention horizon): raw blocks may already be
///   dropped — rounded **up** to the group step so the straddling bucket
///   comes wholly from the rollup (everything below the true horizon was
///   flushed, so its rollup buckets are complete).
/// - **Opportunistic** (below the rollup-complete boundary, i.e. the head's
///   oldest sample): the raw data still exists, but the rollup answers with
///   the same numbers and less IO — rounded **down** so the straddling
///   bucket (which may have head samples the rollup hasn't seen) stays on
///   the source.
///
/// Returns `(rollup table, its bucket, split)` — group buckets starting
/// at/after `split` stay on the source.
fn rollup_route(
    table: &str,
    plan: &PartialsPlan,
    t0: i64,
    cluster: &dyn Cluster,
) -> Result<Option<(String, i64, i64)>> {
    let (horizon, complete_below, rollups) = cluster.ts_rollup_info(table)?;
    let s = plan.bucket_ms;
    let best = rollups
        .into_iter()
        .filter(|(_, b)| *b > 0 && (s == 0 || s % *b == 0))
        .max_by_key(|(_, b)| *b);
    let Some((name, b)) = best else { return Ok(None) };
    let step = if s > 0 { s } else { b };
    let required = horizon.map(|h| round_up(h, step));
    let opportunistic = complete_below.map(|c| round_down(c, step));
    let split = match (required, opportunistic) {
        (Some(r), Some(o)) => r.max(o),
        (Some(r), None) => r,
        (None, Some(o)) => o,
        (None, None) => return Ok(None),
    };
    if t0 >= split {
        return Ok(None); // nothing the rollup should serve
    }
    Ok(Some((name, b, split)))
}

/// Read a rollup's per-bucket `<field>_{count,sum,min,max,first,last}` rows
/// over the aged range (whole rollup buckets inside `[t0, min(t1, split-1)]`)
/// and fold them into group-step partials, `__field__`-labeled like source
/// partials. Bucket starts stand in for first/last timestamps — exact
/// ordering within a series, bucket-granular across series.
#[allow(clippy::too_many_arguments)]
fn rollup_partials(
    name: &str,
    b: i64,
    plan: &PartialsPlan,
    matchers: &[Matcher],
    t0: i64,
    t1: i64,
    split: i64,
    cluster: &dyn Cluster,
) -> Result<Vec<(Labels, Vec<TsPartial>)>> {
    let r0 = if t0 == i64::MIN { i64::MIN } else { round_up(t0, b) };
    let r1 = if t1 >= split - 1 {
        split - 1
    } else {
        round_down(t1 + 1, b) - 1
    };
    if r1 < r0 {
        return Ok(Vec::new());
    }
    let s = plan.bucket_ms;
    const SUFFIXES: [&str; 6] = ["count", "sum", "min", "max", "first", "last"];
    let mut out = Vec::new();
    for f in &plan.agg_fields {
        // series (sans __field__) → rollup-bucket ts → per-suffix values.
        let mut per: BTreeMap<Labels, BTreeMap<i64, [f64; 6]>> = BTreeMap::new();
        for (i, suffix) in SUFFIXES.iter().enumerate() {
            let mut m = matchers.to_vec();
            m.push(Matcher::Eq(FIELD_LABEL.into(), format!("{f}_{suffix}")));
            for (labels, samples) in cluster.ts_query(name, &m, r0, r1)? {
                let base: Labels = labels
                    .iter()
                    .filter(|(k, _)| k != FIELD_LABEL)
                    .cloned()
                    .collect();
                let buckets = per.entry(base).or_default();
                for smp in samples {
                    buckets.entry(smp.ts).or_insert([f64::NAN; 6])[i] = smp.value;
                }
            }
        }
        for (base, buckets) in per {
            let mut partials: Vec<TsPartial> = Vec::new();
            for (bts, vals) in buckets {
                if !(vals[0].is_finite() && vals[0] >= 1.0) {
                    continue; // incomplete row set (no count stream)
                }
                let bucket_ts = if s > 0 { bts.div_euclid(s) * s } else { 0 };
                match partials.last_mut() {
                    Some(p) if p.bucket_ts == bucket_ts => {
                        p.count += vals[0] as u64;
                        p.sum += vals[1];
                        p.min = p.min.min(vals[2]);
                        p.max = p.max.max(vals[3]);
                        // Buckets iterate in time order: first stays put,
                        // last advances with every row.
                        p.last_ts = bts;
                        p.last_val = vals[5];
                    }
                    _ => partials.push(TsPartial {
                        bucket_ts,
                        count: vals[0] as u64,
                        sum: vals[1],
                        min: vals[2],
                        max: vals[3],
                        first_ts: bts,
                        first_val: vals[4],
                        last_ts: bts,
                        last_val: vals[5],
                        // Change aggregates never route to rollups.
                        increase: 0.0,
                    }),
                }
            }
            if partials.is_empty() {
                continue;
            }
            let mut labels = base;
            labels.push((FIELD_LABEL.to_string(), f.clone()));
            labels.sort();
            out.push((labels, partials));
        }
    }
    Ok(out)
}

fn round_up(v: i64, step: i64) -> i64 {
    let f = v.div_euclid(step) * step;
    if f == v {
        v
    } else {
        f + step
    }
}

fn round_down(v: i64, step: i64) -> i64 {
    v.div_euclid(step) * step
}

/// Synthesize the documents the rewritten aggregates fold over: per partial,
/// one doc at `first_ts` carrying the summed/extremal fields (with explicit
/// zero counts for the query's other fields, so `COUNT` of an absent field
/// stays 0, not NULL), plus — when `last()` is used — one doc at `last_ts`
/// carrying the last value, so `last()` stays an argmax over `ts`. Docs are
/// ordered like the raw path (series id, then time) so first/last
/// tie-breaking matches.
fn partial_docs(
    gathered: Vec<(Labels, Vec<TsPartial>)>,
    agg_fields: &[String],
    needs_last: bool,
) -> Vec<Document> {
    let mut streams: Vec<(Vec<u8>, Labels, String, Vec<TsPartial>)> = gathered
        .into_iter()
        .map(|(labels, partials)| {
            let field = labels
                .iter()
                .find(|(k, _)| k == FIELD_LABEL)
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| "value".into());
            let series: Labels = labels
                .iter()
                .filter(|(k, _)| k != FIELD_LABEL)
                .cloned()
                .collect();
            (series_id(&series), series, field, partials)
        })
        .collect();
    streams.sort_by(|a, b| (&a.0, &a.3.first().map(|p| p.bucket_ts)).cmp(&(&b.0, &b.3.first().map(|p| p.bucket_ts))));

    let mut docs = Vec::new();
    for (_, series, field, partials) in &streams {
        for p in partials {
            let mut d = Document::new();
            for (k, v) in series {
                d.insert(k.clone(), Value::String(v.clone()));
            }
            d.insert("ts", Value::Timestamp(p.first_ts));
            for g in agg_fields {
                let n = if g == field { p.count as i64 } else { 0 };
                d.insert(format!("__pcnt_{g}"), Value::Int(n));
            }
            if agg_fields.contains(field) {
                d.insert(format!("__psum_{field}"), Value::Float(p.sum));
                d.insert(format!("__pmin_{field}"), Value::Float(p.min));
                d.insert(format!("__pmax_{field}"), Value::Float(p.max));
                d.insert(format!("__pfirst_{field}"), Value::Float(p.first_val));
                if p.count >= 2 {
                    d.insert(format!("__pinc_{field}"), Value::Float(p.increase));
                    d.insert(
                        format!("__pdel_{field}"),
                        Value::Float(p.last_val - p.first_val),
                    );
                    let span_secs = (p.last_ts - p.first_ts) as f64 / 1000.0;
                    if span_secs > 0.0 {
                        d.insert(
                            format!("__prate_{field}"),
                            Value::Float(p.increase / span_secs),
                        );
                    }
                }
            }
            docs.push(d);
            if needs_last && agg_fields.contains(field) {
                let mut d2 = Document::new();
                for (k, v) in series {
                    d2.insert(k.clone(), Value::String(v.clone()));
                }
                d2.insert("ts", Value::Timestamp(p.last_ts));
                d2.insert(format!("__plast_{field}"), Value::Float(p.last_val));
                docs.push(d2);
            }
        }
    }
    docs
}

/// Rewrite the select to fold partial docs: the WHERE is gone (fully served
/// by the gather), unaliased items keep their raw-path column names, and
/// every aggregate becomes its partial-field equivalent.
fn rewrite_partials(sel: &Select) -> Select {
    let mut out = sel.clone();
    out.filter = None;
    for item in &mut out.items {
        if let SelectItem::Expr { expr, alias } = item {
            if alias.is_none() {
                *alias = Some(expr_name(expr));
            }
            rewrite_aggs(expr);
        }
    }
    if let Some(h) = &mut out.having {
        rewrite_aggs(h);
    }
    for o in &mut out.order_by {
        rewrite_aggs(&mut o.expr);
    }
    out
}

fn rewrite_aggs(e: &mut Expr) {
    match e {
        Expr::Aggregate { func, arg } => {
            let AggArg::Expr(inner) = arg else { return };
            let Expr::Column(f) = inner.as_ref() else { return };
            let (func, f) = (*func, f.clone());
            let agg = |func: AggFunc, prefix: &str| Expr::Aggregate {
                func,
                arg: AggArg::Expr(Box::new(Expr::Column(format!("{prefix}{f}")))),
            };
            *e = match func {
                AggFunc::Count => agg(AggFunc::Sum, "__pcnt_"),
                AggFunc::Sum => agg(AggFunc::Sum, "__psum_"),
                // avg = Σsum / Σcount (all field values are non-null floats,
                // so the raw path's denominator is exactly Σcount).
                AggFunc::Avg => Expr::Binary {
                    op: BinaryOp::Div,
                    left: Box::new(agg(AggFunc::Sum, "__psum_")),
                    right: Box::new(agg(AggFunc::Sum, "__pcnt_")),
                },
                AggFunc::Min => agg(AggFunc::Min, "__pmin_"),
                AggFunc::Max => agg(AggFunc::Max, "__pmax_"),
                AggFunc::First => agg(AggFunc::First, "__pfirst_"),
                AggFunc::Last => agg(AggFunc::Last, "__plast_"),
                AggFunc::Rate => agg(AggFunc::Sum, "__prate_"),
                AggFunc::Increase => agg(AggFunc::Sum, "__pinc_"),
                AggFunc::Delta => agg(AggFunc::Sum, "__pdel_"),
                // Excluded from the partials plan (`plan_expr_ok`).
                AggFunc::Percentile(_) => unreachable!("percentile never plans partials"),
            };
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => rewrite_aggs(expr),
        Expr::Binary { left, right, .. } => {
            rewrite_aggs(left);
            rewrite_aggs(right);
        }
        Expr::Func { args, .. } => {
            for a in args {
                rewrite_aggs(a);
            }
        }
        Expr::InList { expr, list, .. } => {
            rewrite_aggs(expr);
            for a in list {
                rewrite_aggs(a);
            }
        }
        Expr::Between { expr, lo, hi, .. } => {
            rewrite_aggs(expr);
            rewrite_aggs(lo);
            rewrite_aggs(hi);
        }
        Expr::Like { expr, pattern, .. } => {
            rewrite_aggs(expr);
            rewrite_aggs(pattern);
        }
        Expr::Literal(_) | Expr::Column(_) | Expr::Parameter(_) => {}
    }
}

/// Value-field names the query references (columns that are not labels, not
/// `ts`, not dotted). `None` = query all fields (wildcard or none named).
fn referenced_fields(sel: &Select, series_key: &[String]) -> Option<Vec<String>> {
    let mut fields: Vec<String> = Vec::new();
    let mut collect = |e: &Expr| {
        walk(e, &mut |x| {
            if let Expr::Column(name) = x {
                if name != "ts"
                    && !name.contains('.')
                    && !name.starts_with("__")
                    && !series_key.contains(name)
                    && !fields.contains(name)
                {
                    fields.push(name.clone());
                }
            }
        });
    };
    for item in &sel.items {
        match item {
            SelectItem::Wildcard => return None,
            SelectItem::Expr { expr, .. } => collect(expr),
        }
    }
    if let Some(f) = &sel.filter {
        collect(f);
    }
    for g in &sel.group_by {
        collect(g);
    }
    if let Some(h) = &sel.having {
        collect(h);
    }
    for o in &sel.order_by {
        collect(&o.expr);
    }
    if fields.is_empty() {
        return None;
    }
    Some(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(ts: i64, value: f64) -> Sample {
        Sample { ts, value }
    }

    fn labels() -> Labels {
        vec![
            ("__field__".into(), "value".into()),
            ("host".into(), "a".into()),
        ]
    }

    #[test]
    fn partialize_buckets_and_summaries() {
        let series = vec![(
            labels(),
            vec![
                sample(0, 10.0),
                sample(15_000, 25.0),
                sample(45_000, 5.0), // counter reset: contributes 5, not -20
                sample(60_000, 7.0), // next bucket
            ],
        )];
        let out = ts_partialize(series, 60_000);
        assert_eq!(out.len(), 1);
        let partials = &out[0].1;
        assert_eq!(partials.len(), 2);
        let p = &partials[0];
        assert_eq!((p.bucket_ts, p.count), (0, 3));
        assert_eq!((p.sum, p.min, p.max), (40.0, 5.0, 25.0));
        assert_eq!((p.first_ts, p.first_val), (0, 10.0));
        assert_eq!((p.last_ts, p.last_val), (45_000, 5.0));
        assert_eq!(p.increase, 20.0); // +15, then reset → +5
        let p = &partials[1];
        assert_eq!((p.bucket_ts, p.count, p.sum), (60_000, 1, 7.0));
        assert_eq!(p.increase, 0.0);
    }

    #[test]
    fn partialize_whole_range_and_negative_ts() {
        let series = vec![(labels(), vec![sample(-70_000, 1.0), sample(10_000, 3.0)])];
        // bucket 0 → one whole-range partial.
        let out = ts_partialize(series.clone(), 0);
        assert_eq!(out[0].1.len(), 1);
        assert_eq!(out[0].1[0].count, 2);
        // Negative timestamps floor like time_bucket (div_euclid).
        let out = ts_partialize(series, 60_000);
        assert_eq!(out[0].1[0].bucket_ts, -120_000);
        assert_eq!(out[0].1[1].bucket_ts, 0);
    }

    fn parsed(sql: &str) -> Select {
        match skaidb_sql::parse(sql).unwrap() {
            skaidb_sql::ast::Statement::Select(sel) => resolve_output_aliases(&sel),
            other => panic!("expected SELECT, got {other:?}"),
        }
    }

    #[test]
    fn partials_plan_accepts_the_canonical_shapes() {
        let plan = partials_plan(&parsed(
            "SELECT time_bucket(1m, ts) AS t, host, avg(value), max(value) \
             FROM cpu WHERE ts >= 0 AND host = 'a' GROUP BY t, host ORDER BY t",
        ))
        .expect("eligible");
        assert_eq!(plan.bucket_ms, 60_000);
        assert_eq!(plan.group_cols, vec!["host".to_string()]);
        assert_eq!(plan.agg_fields, vec!["value".to_string()]);
        assert!(!plan.needs_last);

        // No bucket, label-only grouping, HAVING over aggregates, last().
        let plan = partials_plan(&parsed(
            "SELECT host, sum(value), last(value) FROM cpu WHERE host != 'b' \
             GROUP BY host HAVING count(value) > 1",
        ))
        .expect("eligible");
        assert_eq!(plan.bucket_ms, 0);
        assert!(plan.needs_last);

        // Ungrouped aggregate over the whole range.
        assert!(partials_plan(&parsed("SELECT max(value) FROM cpu")).is_some());
    }

    #[test]
    fn partials_plan_rejects_what_needs_raw_samples() {
        for sql in [
            // COUNT(*) counts merged (series, ts) rows across fields.
            "SELECT count(*) FROM cpu GROUP BY host",
            // Residual WHERE (field comparison / OR) must see raw rows.
            "SELECT max(value) FROM cpu WHERE value > 5",
            "SELECT max(value) FROM cpu WHERE host = 'a' OR host = 'b'",
            // Grouping by raw ts or a computed aggregate argument.
            "SELECT count(value) FROM cpu GROUP BY ts",
            "SELECT sum(value * 2) FROM cpu GROUP BY host",
            // A bare column that is not a group key.
            "SELECT core, max(value) FROM cpu GROUP BY host",
            // ts outside the group's time_bucket.
            "SELECT time_bucket(2m, ts), max(value) FROM cpu GROUP BY time_bucket(1m, ts)",
            // Not an aggregate query at all.
            "SELECT ts, value FROM cpu WHERE host = 'a'",
        ] {
            assert!(partials_plan(&parsed(sql)).is_none(), "{sql}");
        }
    }
}
