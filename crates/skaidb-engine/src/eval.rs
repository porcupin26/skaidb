//! Expression evaluation with SQL three-valued logic (SPEC §2/§3).
//!
//! Scalar evaluation turns an [`Expr`] and a row [`Document`] into a [`Value`].
//! `NULL` propagates through comparisons and arithmetic; boolean connectives
//! follow [`Ternary`] rules. Aggregates are not scalar and are rejected here —
//! the executor handles them separately.

use std::cmp::Ordering;

use skaidb_sql::ast::{BinaryOp, Expr, UnaryOp};
use skaidb_types::{Document, Ternary, Value};

use crate::error::{EngineError, Result};

/// Evaluate `expr` against `row`.
pub fn eval(expr: &Expr, row: &Document) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Column(path) => Ok(row.get_path(path).cloned().unwrap_or(Value::Null)),
        Expr::IsNull { expr, negated } => {
            let v = eval(expr, row)?;
            Ok(Value::Bool(v.is_null() != *negated))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => eval_in_list(expr, list, *negated, row),
        Expr::Between {
            expr,
            lo,
            hi,
            negated,
        } => {
            // Sugar for `expr >= lo AND expr <= hi` under three-valued logic.
            let v = eval(expr, row)?;
            let l = eval(lo, row)?;
            let h = eval(hi, row)?;
            let ge = Ternary::from_option(compare(&v, &l).map(|o| o != Ordering::Less));
            let le = Ternary::from_option(compare(&v, &h).map(|o| o != Ordering::Greater));
            let t = ge.and(le);
            Ok(ternary_to_value(if *negated { !t } else { t }))
        }
        Expr::Like {
            expr,
            pattern,
            case_insensitive,
            negated,
        } => {
            let v = eval(expr, row)?;
            let p = eval(pattern, row)?;
            // Non-string operands (including NULL) compare as unknown, the
            // same as incomparable types in ordinary comparisons.
            let (Value::String(s), Value::String(pat)) = (&v, &p) else {
                return Ok(Value::Null);
            };
            let matched = if *case_insensitive {
                like_match(&s.to_lowercase(), &pat.to_lowercase())
            } else {
                like_match(s, pat)
            };
            Ok(Value::Bool(matched != *negated))
        }
        Expr::Unary { op, expr } => eval_unary(*op, eval(expr, row)?),
        Expr::Binary { op, left, right } => eval_binary(*op, left, right, row),
        // Parameters are substituted by `bind` before execution; one
        // surviving to evaluation means the statement was never bound.
        Expr::Parameter(_) => Err(EngineError::Type(
            "unbound parameter (`?`) in expression".into(),
        )),
        Expr::Aggregate { .. } => Err(EngineError::Type(
            "aggregate function not allowed here".into(),
        )),
        Expr::Func { name, args } => eval_func(name, args, row),
    }
}

/// Scalar function evaluation. `now()` never reaches here — it is replaced
/// with a literal once per execution (`skaidb_sql::resolve_now`), so a whole
/// query shares one instant.
fn eval_func(name: &str, args: &[Expr], row: &Document) -> Result<Value> {
    match name {
        "time_bucket" => {
            if args.len() != 2 {
                return Err(EngineError::Type(
                    "time_bucket(step, ts) takes exactly two arguments".into(),
                ));
            }
            let step = eval(&args[0], row)?;
            let ts = eval(&args[1], row)?;
            if ts.is_null() {
                return Ok(Value::Null);
            }
            let step_ms = as_int_ms(&step).ok_or_else(|| {
                EngineError::Type("time_bucket step must be a duration or integer ms".into())
            })?;
            if step_ms <= 0 {
                return Err(EngineError::Type("time_bucket step must be positive".into()));
            }
            let t = as_int_ms(&ts).ok_or_else(|| {
                EngineError::Type("time_bucket timestamp must be numeric".into())
            })?;
            let bucket = t.div_euclid(step_ms) * step_ms;
            Ok(match ts {
                Value::Timestamp(_) => Value::Timestamp(bucket),
                _ => Value::Int(bucket),
            })
        }
        "now" => Ok(Value::Timestamp(
            crate::exec::now_ms(),
        )),
        // Coerce a value into a timestamp: numeric epoch-ms passes through,
        // and an ISO-8601 string is parsed. The escape hatch for data whose
        // timestamps landed as strings (e.g. a Mongo migration): the typed
        // decode never triggers, so `to_timestamp(created_at) >= ?` converts
        // in-query without a data rewrite. Unparseable or mistyped input is
        // `NULL`, not an error — one bad row in a schema-less column must not
        // kill the whole query (same policy as LIKE on non-strings).
        "to_timestamp" => {
            if args.len() != 1 {
                return Err(EngineError::Type(
                    "to_timestamp(value) takes exactly one argument".into(),
                ));
            }
            let v = eval(&args[0], row)?;
            Ok(match v {
                Value::Timestamp(t) => Value::Timestamp(t),
                Value::Int(i) => Value::Timestamp(i),
                Value::Float(f) => Value::Timestamp(f as i64),
                Value::String(s) => parse_iso8601_ms(s.trim())
                    .map(Value::Timestamp)
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            })
        }
        // `score()` reads the BM25 score the search gather injects into each
        // hit; outside a search query there is nothing to read.
        "score" => {
            if !args.is_empty() {
                return Err(EngineError::Type("score() takes no arguments".into()));
            }
            row.get("_score").cloned().ok_or_else(|| {
                EngineError::Type(
                    "score() is only valid in a query with a MATCH()/SEARCH() predicate".into(),
                )
            })
        }
        // `HIGHLIGHT(col [, max_chars])` reads the snippet the search gather
        // injects for the column; outside a search query there is nothing to
        // read.
        "highlight" => {
            let Some(Expr::Column(path)) = args.first() else {
                return Err(EngineError::Type(
                    "HIGHLIGHT(column [, max_chars]) takes a column as its first argument".into(),
                ));
            };
            row.get(&format!("_highlight_{path}")).cloned().ok_or_else(|| {
                EngineError::Type(
                    "HIGHLIGHT() is only valid in a query with a MATCH()/SEARCH() predicate"
                        .into(),
                )
            })
        }
        // Search predicates are consumed by the search planner; one reaching
        // scalar evaluation sits in a position the index cannot serve.
        "match" | "match_phrase" | "match_prefix" | "fuzzy" | "wildcard" | "regexp"
        | "more_like_this" | "search" => {
            Err(EngineError::Type(format!(
                "{name}() must appear in the WHERE clause of a search query, composed only \
                 with AND/OR/NOT"
            )))
        }
        other => Err(EngineError::Type(format!("unknown function {other}()"))),
    }
}

/// Millisecond projection for time arguments (`Int`, `Timestamp`, `Float`).
pub(crate) fn as_int_ms(v: &Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(*i),
        Value::Timestamp(t) => Some(*t),
        Value::Float(f) => Some(*f as i64),
        _ => None,
    }
}

/// Evaluate `expr` as a `WHERE`/`HAVING` predicate (kept only when `True`).
pub fn eval_predicate(expr: &Expr, row: &Document) -> Result<bool> {
    Ok(to_ternary(&eval(expr, row)?)?.is_true())
}

fn eval_unary(op: UnaryOp, v: Value) -> Result<Value> {
    match op {
        UnaryOp::Not => match to_ternary(&v)? {
            Ternary::Unknown => Ok(Value::Null),
            t => Ok(Value::Bool((!t).is_true())),
        },
        UnaryOp::Neg => match v {
            Value::Null => Ok(Value::Null),
            Value::Int(i) => Ok(Value::Int(-i)),
            Value::Float(f) => Ok(Value::Float(-f)),
            other => Err(EngineError::Type(format!(
                "cannot negate {:?}",
                other.type_of()
            ))),
        },
    }
}

fn eval_binary(op: BinaryOp, left: &Expr, right: &Expr, row: &Document) -> Result<Value> {
    // Logical connectives use three-valued logic and short-circuit on dominance.
    match op {
        BinaryOp::And => {
            let l = to_ternary(&eval(left, row)?)?;
            if l == Ternary::False {
                return Ok(Value::Bool(false));
            }
            let r = to_ternary(&eval(right, row)?)?;
            return Ok(ternary_to_value(l.and(r)));
        }
        BinaryOp::Or => {
            let l = to_ternary(&eval(left, row)?)?;
            if l == Ternary::True {
                return Ok(Value::Bool(true));
            }
            let r = to_ternary(&eval(right, row)?)?;
            return Ok(ternary_to_value(l.or(r)));
        }
        _ => {}
    }

    let l = eval(left, row)?;
    let r = eval(right, row)?;

    match op {
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => Ok(eval_comparison(op, &l, &r)),
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div => eval_arith(op, &l, &r),
        BinaryOp::And | BinaryOp::Or => unreachable!("handled above"),
    }
}

fn eval_comparison(op: BinaryOp, l: &Value, r: &Value) -> Value {
    // Mongo-style array membership: comparing an array-valued column to a
    // non-array scalar tests CONTAINMENT for =/!= (`labels = 'work'` matches
    // rows whose labels array holds 'work'). SQL's own semantics made this
    // NULL/never-true — useless, and it silently emptied every tag-filtered
    // view of a Mongo-shaped app (2026-07-14). Array-to-array comparison
    // keeps whole-value equality.
    match (op, l, r) {
        (BinaryOp::Eq, Value::Array(items), other) if !matches!(other, Value::Array(_)) => {
            if other.is_null() {
                return Value::Null;
            }
            return Value::Bool(items.iter().any(|it| compare(it, other) == Some(Ordering::Equal)));
        }
        (BinaryOp::NotEq, Value::Array(items), other) if !matches!(other, Value::Array(_)) => {
            if other.is_null() {
                return Value::Null;
            }
            return Value::Bool(!items.iter().any(|it| compare(it, other) == Some(Ordering::Equal)));
        }
        _ => {}
    }
    match compare(l, r) {
        None => Value::Null, // NULL or incomparable types
        Some(ord) => {
            let result = match op {
                BinaryOp::Eq => ord == Ordering::Equal,
                BinaryOp::NotEq => ord != Ordering::Equal,
                BinaryOp::Lt => ord == Ordering::Less,
                BinaryOp::LtEq => ord != Ordering::Greater,
                BinaryOp::Gt => ord == Ordering::Greater,
                BinaryOp::GtEq => ord != Ordering::Less,
                _ => unreachable!(),
            };
            Value::Bool(result)
        }
    }
}

/// `expr [NOT] IN (list)` under SQL three-valued logic. `x IN (...)` is a
/// disjunction of equalities: TRUE if any element equals `x`; else NULL if
/// any comparison was unknown (`x` NULL, or a NULL element); else FALSE.
/// `NOT IN` negates the three-valued result. An element that evaluates to an
/// array is flattened — each of its elements becomes a candidate — so a bound
/// array parameter (`col IN (?)` with `?` = `['a','b']`) tests membership in
/// that array. When `expr` is itself an array column, equality falls through
/// to the Mongo-style containment in [`eval_comparison`].
fn eval_in_list(expr: &Expr, list: &[Expr], negated: bool, row: &Document) -> Result<Value> {
    let left = eval(expr, row)?;
    let mut any_true = false;
    let mut any_null = false;
    'outer: for item in list {
        let v = eval(item, row)?;
        let candidates: &[Value] = match &v {
            Value::Array(elems) => elems,
            _ => std::slice::from_ref(&v),
        };
        for cand in candidates {
            match eval_comparison(BinaryOp::Eq, &left, cand) {
                Value::Bool(true) => {
                    any_true = true;
                    break 'outer;
                }
                Value::Null => any_null = true,
                _ => {}
            }
        }
    }
    let result = if any_true {
        Ternary::True
    } else if any_null {
        Ternary::Unknown
    } else {
        Ternary::False
    };
    Ok(ternary_to_value(if negated { !result } else { result }))
}

/// SQL `LIKE` pattern match: `%` matches any run of characters (including the
/// empty run), `_` matches exactly one character, anything else matches
/// itself. No escape sequence — a literal `%`/`_` cannot be matched (use
/// `MATCH`/equality for such data). Iterative two-pointer scan over chars,
/// backtracking to the most recent `%` on mismatch: O(len(s)·len(pattern))
/// worst case, no allocation beyond the two char vectors.
fn like_match(s: &str, pattern: &str) -> bool {
    let s: Vec<char> = s.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let (mut si, mut pi) = (0usize, 0usize);
    // Position of the last `%` seen, and where its match run started in `s`.
    let mut star: Option<usize> = None;
    let mut mark = 0usize;
    while si < s.len() {
        if pi < p.len() && p[pi] == '%' {
            star = Some(pi);
            mark = si;
            pi += 1;
        } else if pi < p.len() && (p[pi] == '_' || p[pi] == s[si]) {
            si += 1;
            pi += 1;
        } else if let Some(sp) = star {
            // Mismatch after a `%`: widen the `%`'s run by one and retry.
            mark += 1;
            si = mark;
            pi = sp + 1;
        } else {
            return false;
        }
    }
    // Only trailing `%`s may remain unconsumed.
    p[pi..].iter().all(|&c| c == '%')
}

/// Parse an ISO-8601 date/datetime into Unix epoch milliseconds. Accepts
/// `YYYY-MM-DD`, `YYYY-MM-DD[T ]HH:MM[:SS[.fff]]`, with an optional trailing
/// `Z` or `±HH[:MM]` offset (no offset = UTC). Sub-millisecond digits are
/// truncated. Returns `None` on anything malformed.
fn parse_iso8601_ms(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let digits = |i: usize, n: usize| -> Option<i64> {
        let end = i.checked_add(n)?;
        if end > b.len() || !b[i..end].iter().all(u8::is_ascii_digit) {
            return None;
        }
        std::str::from_utf8(&b[i..end]).ok()?.parse().ok()
    };

    // Date part: YYYY-MM-DD.
    let year = digits(0, 4)?;
    if b.get(4) != Some(&b'-') {
        return None;
    }
    let month = digits(5, 2)?;
    if b.get(7) != Some(&b'-') {
        return None;
    }
    let day = digits(8, 2)?;
    if !(1..=12).contains(&month) {
        return None;
    }
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let dim = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ][month as usize - 1];
    if !(1..=dim).contains(&day) {
        return None;
    }
    // Days since the epoch for a civil date (Howard Hinnant's algorithm).
    let (y, m, d) = (if month <= 2 { year - 1 } else { year }, month, day);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let epoch_days = era * 146097 + doe - 719_468;

    let mut ms = epoch_days.checked_mul(86_400_000)?;
    let mut i = 10;
    if i == b.len() {
        return Some(ms); // date only
    }

    // Time part: [T ]HH:MM[:SS[.fff]].
    if b[i] != b'T' && b[i] != b' ' {
        return None;
    }
    i += 1;
    let hh = digits(i, 2)?;
    if b.get(i + 2) != Some(&b':') {
        return None;
    }
    let mm = digits(i + 3, 2)?;
    if hh > 23 || mm > 59 {
        return None;
    }
    ms += (hh * 3600 + mm * 60) * 1000;
    i += 5;
    if b.get(i) == Some(&b':') {
        let ss = digits(i + 1, 2)?;
        if ss > 60 {
            return None; // allow a leap-second's `60`
        }
        ms += ss * 1000;
        i += 3;
        if b.get(i) == Some(&b'.') {
            i += 1;
            let start = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            if i == start {
                return None;
            }
            // First three fractional digits are milliseconds; rest truncate.
            let frac = &s[start..(start + 3).min(i)];
            let mut v: i64 = frac.parse().ok()?;
            for _ in frac.len()..3 {
                v *= 10;
            }
            ms += v;
        }
    }

    // Offset: Z | ±HH[:MM] (absent = UTC).
    match b.get(i) {
        None => Some(ms),
        Some(b'Z' | b'z') if i + 1 == b.len() => Some(ms),
        Some(sign @ (b'+' | b'-')) => {
            let oh = digits(i + 1, 2)?;
            let om = match b.get(i + 3) {
                None => 0,
                Some(b':') => {
                    let v = digits(i + 4, 2)?;
                    if i + 6 != b.len() {
                        return None;
                    }
                    v
                }
                _ => return None,
            };
            if oh > 14 || om > 59 {
                return None;
            }
            let offset_ms = (oh * 3600 + om * 60) * 1000;
            Some(if *sign == b'+' { ms - offset_ms } else { ms + offset_ms })
        }
        _ => None,
    }
}

/// SQL comparison: `None` when either side is `NULL` or the types do not
/// compare. Numeric types compare across each other.
pub fn compare(l: &Value, r: &Value) -> Option<Ordering> {
    if l.is_null() || r.is_null() {
        return None;
    }
    if let (Some(a), Some(b)) = (as_number(l), as_number(r)) {
        return a.partial_cmp(&b);
    }
    match (l, r) {
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
        (Value::Bytes(a), Value::Bytes(b)) => Some(a.cmp(b)),
        (Value::Uuid(a), Value::Uuid(b)) => Some(a.cmp(b)),
        // Same-typed composite values fall back to the canonical total order.
        (Value::Array(_), Value::Array(_)) | (Value::Document(_), Value::Document(_)) => {
            Some(l.total_cmp(r))
        }
        _ => None,
    }
}

/// Numeric projection for cross-type numeric comparison/arithmetic.
pub(crate) fn as_number(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Decimal(d) => Some(d.to_f64()),
        Value::Timestamp(t) => Some(*t as f64),
        _ => None,
    }
}

fn eval_arith(op: BinaryOp, l: &Value, r: &Value) -> Result<Value> {
    if l.is_null() || r.is_null() {
        return Ok(Value::Null);
    }
    // Integer arithmetic stays integral; anything else promotes to float.
    if let (Value::Int(a), Value::Int(b)) = (l, r) {
        return Ok(match op {
            BinaryOp::Add => Value::Int(a.wrapping_add(*b)),
            BinaryOp::Sub => Value::Int(a.wrapping_sub(*b)),
            BinaryOp::Mul => Value::Int(a.wrapping_mul(*b)),
            BinaryOp::Div => {
                if *b == 0 {
                    Value::Null
                } else {
                    Value::Int(a / b)
                }
            }
            _ => unreachable!(),
        });
    }
    let (a, b) = match (as_number(l), as_number(r)) {
        (Some(a), Some(b)) => (a, b),
        _ => {
            return Err(EngineError::Type(format!(
                "cannot apply arithmetic to {:?} and {:?}",
                l.type_of(),
                r.type_of()
            )))
        }
    };
    Ok(match op {
        BinaryOp::Add => Value::Float(a + b),
        BinaryOp::Sub => Value::Float(a - b),
        BinaryOp::Mul => Value::Float(a * b),
        BinaryOp::Div => {
            if b == 0.0 {
                Value::Null
            } else {
                Value::Float(a / b)
            }
        }
        _ => unreachable!(),
    })
}

/// Interpret a value as a ternary truth value (`Bool`/`NULL` only).
fn to_ternary(v: &Value) -> Result<Ternary> {
    match v {
        Value::Bool(b) => Ok(Ternary::from_bool(*b)),
        Value::Null => Ok(Ternary::Unknown),
        other => Err(EngineError::Type(format!(
            "expected boolean, found {:?}",
            other.type_of()
        ))),
    }
}

fn ternary_to_value(t: Ternary) -> Value {
    match t {
        Ternary::True => Value::Bool(true),
        Ternary::False => Value::Bool(false),
        Ternary::Unknown => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skaidb_sql::ast::{SelectItem, Statement};
    use skaidb_sql::parse;

    fn doc(pairs: &[(&str, Value)]) -> Document {
        let mut d = Document::new();
        for (k, v) in pairs {
            d.insert(*k, v.clone());
        }
        d
    }

    /// Parse `SELECT <expr> FROM t` and pull out the single projected expression.
    fn expr(src: &str) -> Expr {
        let sql = format!("SELECT {src} FROM t");
        let Statement::Select(sel) = parse(&sql).unwrap() else {
            panic!("expected select")
        };
        match sel.items.into_iter().next().unwrap() {
            SelectItem::Expr { expr, .. } => expr,
            SelectItem::Wildcard => panic!("unexpected wildcard"),
        }
    }

    #[test]
    fn arithmetic_and_precedence() {
        let row = doc(&[("a", Value::Int(2)), ("b", Value::Int(3))]);
        assert_eq!(eval(&expr("a + b * 4"), &row).unwrap(), Value::Int(14));
    }

    #[test]
    fn null_propagates_in_comparison() {
        let row = doc(&[("a", Value::Null)]);
        assert_eq!(eval(&expr("a = 1"), &row).unwrap(), Value::Null);
    }

    #[test]
    fn three_valued_and_or() {
        let row = doc(&[("a", Value::Null), ("b", Value::Bool(false))]);
        // NULL AND false = false; NULL OR false = NULL
        assert_eq!(
            eval(&expr("a = 1 AND b"), &row).unwrap(),
            Value::Bool(false)
        );
        assert_eq!(eval(&expr("a = 1 OR b"), &row).unwrap(), Value::Null);
    }

    #[test]
    fn is_null_predicate() {
        let row = doc(&[("a", Value::Null), ("b", Value::Int(1))]);
        assert!(eval_predicate(&expr("a IS NULL"), &row).unwrap());
        assert!(eval_predicate(&expr("b IS NOT NULL"), &row).unwrap());
        // A NULL predicate (Unknown) does not keep the row.
        assert!(!eval_predicate(&expr("a = 1"), &row).unwrap());
    }

    #[test]
    fn cross_type_numeric_comparison() {
        let row = doc(&[("a", Value::Int(3))]);
        assert_eq!(eval(&expr("a < 3.5"), &row).unwrap(), Value::Bool(true));
    }

    #[test]
    fn nested_path_access() {
        let mut inner = Document::new();
        inner.insert("c", Value::Int(9));
        let row = doc(&[("b", Value::Document(inner))]);
        assert_eq!(eval(&expr("b.c"), &row).unwrap(), Value::Int(9));
        assert_eq!(eval(&expr("b.missing"), &row).unwrap(), Value::Null);
    }

    #[test]
    fn division_by_zero_is_null() {
        let row = doc(&[("a", Value::Int(1))]);
        assert_eq!(eval(&expr("a / 0"), &row).unwrap(), Value::Null);
    }

    #[test]
    fn in_list_membership() {
        let row = doc(&[("a", Value::Int(2))]);
        assert!(eval_predicate(&expr("a IN (1, 2, 3)"), &row).unwrap());
        assert!(!eval_predicate(&expr("a IN (5, 6)"), &row).unwrap());
        assert!(!eval_predicate(&expr("a NOT IN (1, 2, 3)"), &row).unwrap());
        assert!(eval_predicate(&expr("a NOT IN (5, 6)"), &row).unwrap());
    }

    #[test]
    fn in_list_three_valued_logic() {
        // Left is NULL → IN is Unknown → neither IN nor NOT IN keeps the row.
        let row = doc(&[("a", Value::Null)]);
        assert_eq!(eval(&expr("a IN (1, 2)"), &row).unwrap(), Value::Null);
        assert_eq!(eval(&expr("a NOT IN (1, 2)"), &row).unwrap(), Value::Null);

        // No match but a NULL element present → Unknown, not False.
        let row = doc(&[("a", Value::Int(9))]);
        assert_eq!(eval(&expr("a IN (1, NULL)"), &row).unwrap(), Value::Null);
        // A concrete match wins over the NULL element → True.
        let row = doc(&[("a", Value::Int(1))]);
        assert_eq!(
            eval(&expr("a IN (1, NULL)"), &row).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn in_list_against_array_column_is_containment() {
        // Multikey column: `labels IN ('work', 'x')` matches if the array holds
        // any listed value.
        let row = doc(&[(
            "labels",
            Value::Array(vec![Value::String("home".into()), Value::String("work".into())]),
        )]);
        assert!(eval_predicate(&expr("labels IN ('work', 'x')"), &row).unwrap());
        assert!(!eval_predicate(&expr("labels IN ('y', 'z')"), &row).unwrap());
    }

    #[test]
    fn in_list_flattens_array_elements() {
        // An array-valued list element expands to its members — the shape a
        // bound array parameter takes (`col IN (?)` with `?` = ['a','b']).
        let row = doc(&[("a", Value::Int(2))]);
        assert!(eval_predicate(&expr("a IN ([1, 2, 3])"), &row).unwrap());
        assert!(!eval_predicate(&expr("a IN ([4, 5])"), &row).unwrap());
    }

    #[test]
    fn like_matcher() {
        // (%: any run, _: exactly one char)
        assert!(like_match("invoice 42", "%invoice%"));
        assert!(like_match("invoice", "invoice"));
        assert!(like_match("invoice", "inv%"));
        assert!(like_match("invoice", "%ice"));
        assert!(like_match("invoice", "in_oice"));
        assert!(like_match("", "%"));
        assert!(like_match("", ""));
        assert!(like_match("abc", "%%%"));
        assert!(like_match("aXbXc", "a%b%c"));
        assert!(!like_match("invoice", "voice")); // no implicit anchors
        assert!(!like_match("invoice", "in_ice")); // _ is exactly one
        assert!(!like_match("abc", ""));
        assert!(!like_match("ab", "a_c"));
        // Backtracking: the first % try must not starve the second.
        assert!(like_match("a-b-c-b-d", "a%b%d"));
        // Unicode chars count as one `_`.
        assert!(like_match("naïve", "na_ve"));
    }

    #[test]
    fn like_and_ilike_predicates() {
        let row = doc(&[("subject", Value::String("Invoice #42 overdue".into()))]);
        assert!(eval_predicate(&expr("subject LIKE '%#42%'"), &row).unwrap());
        assert!(!eval_predicate(&expr("subject LIKE '%invoice%'"), &row).unwrap());
        assert!(eval_predicate(&expr("subject ILIKE '%invoice%'"), &row).unwrap());
        assert!(eval_predicate(&expr("subject NOT LIKE 'x%'"), &row).unwrap());
        // NULL / non-string operands are unknown, not errors.
        let row = doc(&[("subject", Value::Null), ("n", Value::Int(3))]);
        assert_eq!(eval(&expr("subject LIKE '%a%'"), &row).unwrap(), Value::Null);
        assert_eq!(eval(&expr("n LIKE '3'"), &row).unwrap(), Value::Null);
    }

    #[test]
    fn between_predicate() {
        let row = doc(&[("ts", Value::Int(5))]);
        assert!(eval_predicate(&expr("ts BETWEEN 1 AND 10"), &row).unwrap());
        assert!(eval_predicate(&expr("ts BETWEEN 5 AND 5"), &row).unwrap()); // inclusive
        assert!(!eval_predicate(&expr("ts BETWEEN 6 AND 10"), &row).unwrap());
        assert!(eval_predicate(&expr("ts NOT BETWEEN 6 AND 10"), &row).unwrap());
        // NULL operand or bound -> unknown (kept out by predicates, and NOT
        // BETWEEN stays unknown too).
        let row = doc(&[("ts", Value::Null)]);
        assert_eq!(eval(&expr("ts BETWEEN 1 AND 10"), &row).unwrap(), Value::Null);
        assert_eq!(
            eval(&expr("ts NOT BETWEEN 1 AND 10"), &row).unwrap(),
            Value::Null
        );
        let row = doc(&[("ts", Value::Int(5))]);
        assert_eq!(
            eval(&expr("ts BETWEEN NULL AND 10"), &row).unwrap(),
            Value::Null
        );
        // ...but a definite False short-circuits an unknown bound:
        // 5 <= NULL is unknown, 5 >= 20 is false -> false.
        assert_eq!(
            eval(&expr("ts BETWEEN 20 AND NULL"), &row).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn iso8601_parser() {
        // Date-only, midnight UTC. 2026-07-15 is 20649 days after the epoch.
        assert_eq!(parse_iso8601_ms("1970-01-01"), Some(0));
        assert_eq!(parse_iso8601_ms("2026-07-15"), Some(20_649 * 86_400_000));
        // Datetime with T or space, seconds optional.
        assert_eq!(
            parse_iso8601_ms("1970-01-01T01:02:03"),
            Some(3_723_000)
        );
        assert_eq!(parse_iso8601_ms("1970-01-01 01:02"), Some(3_720_000));
        // Fractional seconds: ms kept, extra digits truncate.
        assert_eq!(
            parse_iso8601_ms("1970-01-01T00:00:00.5"),
            Some(500)
        );
        assert_eq!(
            parse_iso8601_ms("1970-01-01T00:00:00.123456Z"),
            Some(123)
        );
        // Offsets shift back to UTC.
        assert_eq!(
            parse_iso8601_ms("1970-01-01T02:00:00+02:00"),
            Some(0)
        );
        assert_eq!(
            parse_iso8601_ms("1969-12-31T22:00:00-02:00"),
            Some(0)
        );
        // Leap day valid on leap years only.
        assert!(parse_iso8601_ms("2024-02-29").is_some());
        assert!(parse_iso8601_ms("2023-02-29").is_none());
        // Malformed input.
        for bad in [
            "", "2026", "2026-13-01", "2026-00-10", "2026-07-32",
            "2026-07-15X10:00", "2026-07-15T25:00", "2026-07-15T10:0",
            "not a date", "2026-07-15T10:00:00+2", "2026-07-15T10:00:00Zx",
        ] {
            assert_eq!(parse_iso8601_ms(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn to_timestamp_function() {
        let row = doc(&[
            ("iso", Value::String("1970-01-02T00:00:00Z".into())),
            ("n", Value::Int(1000)),
            ("bad", Value::String("yesterday-ish".into())),
            ("b", Value::Bool(true)),
        ]);
        assert_eq!(
            eval(&expr("to_timestamp(iso)"), &row).unwrap(),
            Value::Timestamp(86_400_000)
        );
        assert_eq!(
            eval(&expr("to_timestamp(n)"), &row).unwrap(),
            Value::Timestamp(1000)
        );
        // Unparseable / mistyped / NULL -> NULL, never an error.
        assert_eq!(eval(&expr("to_timestamp(bad)"), &row).unwrap(), Value::Null);
        assert_eq!(eval(&expr("to_timestamp(b)"), &row).unwrap(), Value::Null);
        assert_eq!(
            eval(&expr("to_timestamp(missing)"), &row).unwrap(),
            Value::Null
        );
        // The point of it all: range-filter string timestamps in-query.
        assert!(eval_predicate(
            &expr("to_timestamp(iso) BETWEEN 0 AND 100000000"),
            &row
        )
        .unwrap());
    }

    #[test]
    fn in_list_bound_array_parameter() {
        // End-to-end: `WHERE a IN (?)` bound to an array value tests membership
        // in that array — the agencik "fetch these N ids" pattern.
        use skaidb_sql::{bind, parse, Statement};
        let stmt = parse("SELECT a FROM t WHERE a IN (?)").unwrap();
        let bound = bind(
            &stmt,
            &[Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])],
        )
        .unwrap();
        let Statement::Select(sel) = bound else {
            panic!("expected select")
        };
        let filter = sel.filter.unwrap();
        assert!(eval_predicate(&filter, &doc(&[("a", Value::Int(2))])).unwrap());
        assert!(!eval_predicate(&filter, &doc(&[("a", Value::Int(9))])).unwrap());
    }
}
