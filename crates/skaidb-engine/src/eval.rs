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
        // First non-NULL argument (SQL COALESCE). Also used internally by
        // the TS partials rewrite: COUNT folds to SUM(partial counts), and
        // SUM over zero rows is NULL where COUNT must be 0.
        "coalesce" => {
            for a in args {
                let v = eval(a, row)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            Ok(Value::Null)
        }
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
        // The remaining coercions (the `CAST(x AS t)` desugar targets). Same
        // policy as `to_timestamp`: unconvertible input is `NULL`, never an
        // error — one odd row in a schema-less column must not kill a query.
        "to_int" => {
            let v = one_arg("to_int", args, row)?;
            Ok(match v {
                Value::Int(i) => Value::Int(i),
                Value::Float(f) if f.is_finite() => Value::Int(f as i64),
                Value::Bool(b) => Value::Int(i64::from(b)),
                Value::Timestamp(t) => Value::Int(t),
                Value::Decimal(d) => 10i128
                    .checked_pow(d.scale)
                    .map(|div| Value::Int((d.mantissa / div) as i64))
                    .unwrap_or(Value::Null),
                Value::String(s) => {
                    let s = s.trim();
                    s.parse::<i64>()
                        .ok()
                        .or_else(|| s.parse::<f64>().ok().filter(|f| f.is_finite()).map(|f| f as i64))
                        .map(Value::Int)
                        .unwrap_or(Value::Null)
                }
                _ => Value::Null,
            })
        }
        "to_float" => {
            let v = one_arg("to_float", args, row)?;
            Ok(match v {
                Value::Float(f) => Value::Float(f),
                Value::Int(i) => Value::Float(i as f64),
                Value::Bool(b) => Value::Float(if b { 1.0 } else { 0.0 }),
                Value::Timestamp(t) => Value::Float(t as f64),
                Value::Decimal(d) => {
                    Value::Float(d.mantissa as f64 / 10f64.powi(d.scale as i32))
                }
                Value::String(s) => s
                    .trim()
                    .parse::<f64>()
                    .ok()
                    .filter(|f| f.is_finite())
                    .map(Value::Float)
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            })
        }
        "to_string" => {
            let v = one_arg("to_string", args, row)?;
            Ok(match v {
                Value::String(s) => Value::String(s),
                Value::Int(i) => Value::String(i.to_string()),
                Value::Float(f) => Value::String(f.to_string()),
                Value::Bool(b) => Value::String(b.to_string()),
                Value::Timestamp(t) => Value::String(format_iso8601_ms(t)),
                Value::Uuid(u) => Value::String(u.to_string()),
                Value::Decimal(d) => Value::String(d.to_string()),
                _ => Value::Null,
            })
        }
        "to_bool" => {
            let v = one_arg("to_bool", args, row)?;
            Ok(match v {
                Value::Bool(b) => Value::Bool(b),
                Value::Int(i) => Value::Bool(i != 0),
                Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                    "true" | "t" | "1" => Value::Bool(true),
                    "false" | "f" | "0" => Value::Bool(false),
                    _ => Value::Null,
                },
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
        // `rrf_score()` reads the fused Reciprocal-Rank-Fusion score the hybrid
        // (`RANK BY RRF`) gather injects into each hit.
        "rrf_score" => {
            if !args.is_empty() {
                return Err(EngineError::Type("rrf_score() takes no arguments".into()));
            }
            row.get("_rrf_score").cloned().ok_or_else(|| {
                EngineError::Type("rrf_score() is only valid in a RANK BY RRF query".into())
            })
        }
        // `distance('<n><unit>')` — a distance literal with an ES-style unit
        // suffix ("5km", "1mi", "3 NM", …; bare number = metres) evaluated to
        // METRES, for use as a `geo_distance` radius:
        // `WHERE geo_distance(loc, ..) <= distance('5km')`. Constant when its
        // argument is, so the geo-index planner sees it as a plain radius. A
        // bad unit is an ERROR (a typo'd radius must not silently match
        // nothing); a numeric argument passes through as metres.
        "distance" => {
            let [arg] = args else {
                return Err(EngineError::Type(
                    "distance('<n><unit>') takes exactly one argument".into(),
                ));
            };
            match eval(arg, row)? {
                Value::String(s) => match crate::geo::parse_distance_m(&s) {
                    Some(m) => Ok(Value::Float(m)),
                    None => Err(EngineError::Type(format!(
                        "distance(): cannot parse '{s}' (use e.g. '500m', '5km', '1mi', '3NM')"
                    ))),
                },
                Value::Int(i) => Ok(Value::Float(i as f64)),
                Value::Float(f) => Ok(Value::Float(f)),
                Value::Null => Ok(Value::Null),
                other => Err(EngineError::Type(format!(
                    "distance() takes a string like '5km' or a number of metres, got {:?}",
                    other.type_of()
                ))),
            }
        }
        // `geo_distance(point, lat, lon)` — great-circle (haversine) distance in
        // METERS from the row's `point` field to `(lat, lon)`. `point` is a
        // `{lat, lon}` object or a `[lat, lon]` array; a non-point value (or a
        // NULL/absent field or coordinate) yields NULL, never an error — one
        // bad row in a schema-less column must not kill the query (the LIKE /
        // to_* policy). Use in `WHERE geo_distance(loc, ..) <= <meters>` and in
        // `ORDER BY geo_distance(loc, ..) LIMIT k` (nearest-first).
        "geo_distance" => {
            if args.len() != 3 {
                return Err(EngineError::Type(
                    "geo_distance(point, lat, lon) takes exactly three arguments".into(),
                ));
            }
            let Some((plat, plon)) = read_point(&eval(&args[0], row)?) else {
                return Ok(Value::Null);
            };
            match (
                as_f64(&eval(&args[1], row)?),
                as_f64(&eval(&args[2], row)?),
            ) {
                (Some(lat), Some(lon)) => Ok(Value::Float(haversine_m(plat, plon, lat, lon))),
                _ => Ok(Value::Null),
            }
        }
        // `geo_bbox(point, min_lat, min_lon, max_lat, max_lon)` — whether the
        // row's `point` lies inside the bounding box. `min_lon > max_lon` is a
        // box that crosses the antimeridian (±180°). Non-point/NULL → NULL.
        "geo_bbox" => {
            if args.len() != 5 {
                return Err(EngineError::Type(
                    "geo_bbox(point, min_lat, min_lon, max_lat, max_lon) takes exactly five \
                     arguments"
                        .into(),
                ));
            }
            let Some((plat, plon)) = read_point(&eval(&args[0], row)?) else {
                return Ok(Value::Null);
            };
            let mut b = [0.0f64; 4];
            for (i, arg) in args[1..].iter().enumerate() {
                match as_f64(&eval(arg, row)?) {
                    Some(v) => b[i] = v,
                    None => return Ok(Value::Null),
                }
            }
            let [min_lat, min_lon, max_lat, max_lon] = b;
            let lon_ok = if min_lon <= max_lon {
                plon >= min_lon && plon <= max_lon
            } else {
                // Box crosses the antimeridian.
                plon >= min_lon || plon <= max_lon
            };
            Ok(Value::Bool(plat >= min_lat && plat <= max_lat && lon_ok))
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

/// Evaluate the single argument of a one-arg scalar function.
fn one_arg(name: &str, args: &[Expr], row: &Document) -> Result<Value> {
    if args.len() != 1 {
        return Err(EngineError::Type(format!(
            "{name}(value) takes exactly one argument"
        )));
    }
    eval(&args[0], row)
}

/// Format epoch milliseconds as UTC ISO-8601 (`YYYY-MM-DDTHH:MM:SS[.mmm]Z`),
/// the inverse of [`parse_iso8601_ms`] (Howard Hinnant's civil-from-days).
fn format_iso8601_ms(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let (secs, ms_part) = (rem / 1000, rem % 1000);
    let (hh, mm, ss) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    if ms_part == 0 {
        format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
    } else {
        format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{ms_part:03}Z")
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

/// A numeric value as `f64` (`Int`/`Float`/`Decimal`), else `None`.
pub(crate) fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Decimal(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

/// Read a geo point as `(lat, lon)` from a `{lat, lon}` object (`lng` also
/// accepted) or a `[lat, lon]` array. `None` for any other shape. `pub(crate)`
/// so the geo index maintenance/planner reads points the same way the
/// `geo_distance` / `geo_bbox` predicates do.
pub(crate) fn read_point(v: &Value) -> Option<(f64, f64)> {
    match v {
        Value::Document(d) => {
            let lat = as_f64(d.get("lat")?)?;
            let lon = as_f64(d.get("lon").or_else(|| d.get("lng"))?)?;
            Some((lat, lon))
        }
        Value::Array(a) if a.len() == 2 => Some((as_f64(&a[0])?, as_f64(&a[1])?)),
        _ => None,
    }
}

/// Great-circle distance in metres between two `(lat, lon)` points (haversine,
/// mean-Earth-radius sphere — the model Elasticsearch's `arc` distance uses).
fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_M: f64 = 6_371_000.0;
    let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_M * a.sqrt().atan2((1.0 - a).sqrt())
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
    fn cast_desugars_to_coercions() {
        let row = doc(&[
            ("s", Value::String("42".into())),
            ("f", Value::Float(3.9)),
            ("iso", Value::String("1970-01-02T00:00:00Z".into())),
            ("junk", Value::String("not a number".into())),
        ]);
        assert_eq!(eval(&expr("CAST(s AS INT)"), &row).unwrap(), Value::Int(42));
        assert_eq!(eval(&expr("CAST(f AS INT)"), &row).unwrap(), Value::Int(3));
        assert_eq!(
            eval(&expr("CAST(s AS FLOAT)"), &row).unwrap(),
            Value::Float(42.0)
        );
        assert_eq!(
            eval(&expr("CAST(f AS STRING)"), &row).unwrap(),
            Value::String("3.9".into())
        );
        assert_eq!(
            eval(&expr("CAST(iso AS TIMESTAMP)"), &row).unwrap(),
            Value::Timestamp(86_400_000)
        );
        assert_eq!(
            eval(&expr("CAST('true' AS BOOL)"), &row).unwrap(),
            Value::Bool(true)
        );
        // Unconvertible -> NULL, never an error.
        assert_eq!(eval(&expr("CAST(junk AS INT)"), &row).unwrap(), Value::Null);
        // Timestamp -> ISO string round-trips through the parser.
        assert_eq!(
            eval(&expr("CAST(CAST(iso AS TIMESTAMP) AS STRING)"), &row).unwrap(),
            Value::String("1970-01-02T00:00:00Z".into())
        );
        // `cast` is contextual: still a valid column name.
        let row2 = doc(&[("cast", Value::Int(7))]);
        assert_eq!(eval(&expr("cast + 1"), &row2).unwrap(), Value::Int(8));
    }

    #[test]
    fn iso8601_formatter_roundtrips() {
        for ms in [0i64, 86_400_000, 1_752_595_200_123, 253_402_300_799_000] {
            let s = format_iso8601_ms(ms);
            assert_eq!(parse_iso8601_ms(&s), Some(ms), "{s}");
        }
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
