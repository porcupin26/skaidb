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
}
