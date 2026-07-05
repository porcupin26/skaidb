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

use skaidb_sql::ast::{BinaryOp, Expr, Insert, Select, SelectItem};
use skaidb_tsdb::{Labels, Matcher};
use skaidb_types::{Document, Value};

use crate::error::{EngineError, Result};
use crate::eval::{as_int_ms, eval, eval_predicate};
use crate::exec::{project, Cluster};
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
    let mut merged: BTreeMap<(Vec<u8>, i64), Document> = BTreeMap::new();
    for (labels, samples) in gathered {
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
            } else if series_key.contains(col) {
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
