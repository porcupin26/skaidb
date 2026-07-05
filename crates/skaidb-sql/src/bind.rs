//! Bind-parameter substitution for prepared statements.
//!
//! A prepared statement is parsed once with `?` placeholders
//! ([`Expr::Parameter`]); each execution [`bind`]s a value list, producing a
//! plain statement (every parameter replaced by a literal) that runs through
//! the normal executor. Binding is by position: the first `?` in the text is
//! parameter 0.

use crate::ast::{AggArg, Delete, Expr, Insert, Select, SelectItem, Statement, Update};
use skaidb_types::Value;

/// Errors binding parameters to a prepared statement.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum BindError {
    #[error("statement expects {expected} parameters, got {got}")]
    Arity { expected: usize, got: usize },
    #[error("statement kind cannot be prepared")]
    Unpreparable,
}

/// The number of bind parameters (`?`) in a statement. `None` marks a
/// statement kind that cannot be prepared (DDL, session control): those may
/// embed SQL text that is re-broadcast verbatim, so placeholder substitution
/// cannot reach them.
pub fn param_count(stmt: &Statement) -> Option<usize> {
    let mut n = 0usize;
    let counted = visit_statement(stmt, &mut |e| {
        if let Expr::Parameter(i) = e {
            n = n.max(*i as usize + 1);
        }
    });
    counted.then_some(n)
}

/// Replace every [`Expr::Parameter`] with the corresponding literal from
/// `params`, returning the executable statement. The statement must be a
/// preparable kind and `params` must match its parameter count exactly.
pub fn bind(stmt: &Statement, params: &[Value]) -> Result<Statement, BindError> {
    let expected = param_count(stmt).ok_or(BindError::Unpreparable)?;
    if expected != params.len() {
        return Err(BindError::Arity {
            expected,
            got: params.len(),
        });
    }
    let mut bound = stmt.clone();
    mutate_statement(&mut bound, &mut |e| {
        if let Expr::Parameter(i) = e {
            *e = Expr::Literal(params[*i as usize].clone());
        }
    });
    Ok(bound)
}

/// Visit every expression of a statement. Returns `false` for statement kinds
/// that binding does not support (their expressions, if any, are not visited).
fn visit_statement(stmt: &Statement, f: &mut impl FnMut(&Expr)) -> bool {
    match stmt {
        Statement::Select(s) => {
            visit_select(s, f);
            true
        }
        Statement::Insert(i) => {
            visit_insert(i, f);
            true
        }
        Statement::Update(u) => {
            visit_update(u, f);
            true
        }
        Statement::Delete(d) => {
            visit_delete(d, f);
            true
        }
        _ => false,
    }
}

fn visit_select(s: &Select, f: &mut impl FnMut(&Expr)) {
    if let Some(n) = &s.nearest {
        visit_expr(&n.query, f);
        visit_expr(&n.k, f);
    }
    for item in &s.items {
        if let SelectItem::Expr { expr, .. } = item {
            visit_expr(expr, f);
        }
    }
    for j in &s.joins {
        if let Some(e) = &j.on {
            visit_expr(e, f);
        }
    }
    if let Some(e) = &s.filter {
        visit_expr(e, f);
    }
    for e in &s.group_by {
        visit_expr(e, f);
    }
    if let Some(e) = &s.having {
        visit_expr(e, f);
    }
    for op in &s.set_ops {
        visit_select(&op.select, f);
    }
    for k in &s.order_by {
        visit_expr(&k.expr, f);
    }
}

fn visit_insert(i: &Insert, f: &mut impl FnMut(&Expr)) {
    for row in &i.rows {
        for e in row {
            visit_expr(e, f);
        }
    }
}

fn visit_update(u: &Update, f: &mut impl FnMut(&Expr)) {
    for (_, e) in &u.assignments {
        visit_expr(e, f);
    }
    if let Some(e) = &u.filter {
        visit_expr(e, f);
    }
}

fn visit_delete(d: &Delete, f: &mut impl FnMut(&Expr)) {
    if let Some(e) = &d.filter {
        visit_expr(e, f);
    }
}

fn visit_expr(e: &Expr, f: &mut impl FnMut(&Expr)) {
    f(e);
    match e {
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => visit_expr(expr, f),
        Expr::Binary { left, right, .. } => {
            visit_expr(left, f);
            visit_expr(right, f);
        }
        Expr::Aggregate { arg, .. } => {
            if let AggArg::Expr(expr) = arg {
                visit_expr(expr, f);
            }
        }
        Expr::Func { args, .. } => {
            for arg in args {
                visit_expr(arg, f);
            }
        }
        Expr::Literal(_) | Expr::Column(_) | Expr::Parameter(_) => {}
    }
}

fn mutate_select(s: &mut Select, f: &mut impl FnMut(&mut Expr)) {
    if let Some(n) = &mut s.nearest {
        mutate_expr(&mut n.query, f);
        mutate_expr(&mut n.k, f);
    }
    for item in &mut s.items {
        if let SelectItem::Expr { expr, .. } = item {
            mutate_expr(expr, f);
        }
    }
    for j in &mut s.joins {
        if let Some(e) = &mut j.on {
            mutate_expr(e, f);
        }
    }
    if let Some(e) = &mut s.filter {
        mutate_expr(e, f);
    }
    for e in &mut s.group_by {
        mutate_expr(e, f);
    }
    if let Some(e) = &mut s.having {
        mutate_expr(e, f);
    }
    for op in &mut s.set_ops {
        mutate_select(&mut op.select, f);
    }
    for k in &mut s.order_by {
        mutate_expr(&mut k.expr, f);
    }
}

/// Mutable mirror of [`visit_statement`] over the statement kinds it accepts.
fn mutate_statement(stmt: &mut Statement, f: &mut impl FnMut(&mut Expr)) {
    match stmt {
        Statement::Select(s) => mutate_select(s, f),
        Statement::Insert(i) => {
            for row in &mut i.rows {
                for e in row {
                    mutate_expr(e, f);
                }
            }
        }
        Statement::Update(u) => {
            for (_, e) in &mut u.assignments {
                mutate_expr(e, f);
            }
            if let Some(e) = &mut u.filter {
                mutate_expr(e, f);
            }
        }
        Statement::Delete(d) => {
            if let Some(e) = &mut d.filter {
                mutate_expr(e, f);
            }
        }
        _ => {}
    }
}

fn mutate_expr(e: &mut Expr, f: &mut impl FnMut(&mut Expr)) {
    f(e);
    match e {
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => mutate_expr(expr, f),
        Expr::Binary { left, right, .. } => {
            mutate_expr(left, f);
            mutate_expr(right, f);
        }
        Expr::Aggregate { arg, .. } => {
            if let AggArg::Expr(expr) = arg {
                mutate_expr(expr, f);
            }
        }
        Expr::Func { args, .. } => {
            for arg in args {
                mutate_expr(arg, f);
            }
        }
        Expr::Literal(_) | Expr::Column(_) | Expr::Parameter(_) => {}
    }
}

/// Replace every `now()` call with the given timestamp literal, so one
/// query-wide instant drives range predicates and bucketing (and pushdown
/// sees plain literals). Called once per execution by the engine.
pub fn resolve_now(stmt: &mut Statement, now_ms: i64) {
    mutate_statement(stmt, &mut |e| {
        if let Expr::Func { name, args } = e {
            if name == "now" && args.is_empty() {
                *e = Expr::Literal(Value::Timestamp(now_ms));
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;

    #[test]
    fn bind_insert_and_where() {
        let stmt = parse("INSERT INTO t (id, v) VALUES (?, ?)").unwrap();
        assert_eq!(param_count(&stmt), Some(2));
        let bound = bind(&stmt, &[Value::Int(7), Value::String("x".into())]).unwrap();
        let expect = parse("INSERT INTO t (id, v) VALUES (7, 'x')").unwrap();
        assert_eq!(bound, expect);

        let stmt = parse("SELECT v FROM t WHERE id = ?").unwrap();
        assert_eq!(param_count(&stmt), Some(1));
        let bound = bind(&stmt, &[Value::Int(3)]).unwrap();
        assert_eq!(bound, parse("SELECT v FROM t WHERE id = 3").unwrap());
    }

    #[test]
    fn bind_nearest_clause() {
        let stmt = parse("SELECT id FROM docs NEAREST (embedding, ?, ?) WHERE cat = ?").unwrap();
        assert_eq!(param_count(&stmt), Some(3));
        let bound = bind(
            &stmt,
            &[
                Value::Array(vec![Value::Float(1.0), Value::Float(0.0)]),
                Value::Int(5),
                Value::String("a".into()),
            ],
        )
        .unwrap();
        let expect =
            parse("SELECT id FROM docs NEAREST (embedding, [1.0, 0.0], 5) WHERE cat = 'a'")
                .unwrap();
        assert_eq!(bound, expect);
    }

    #[test]
    fn bind_update_delete() {
        let stmt = parse("UPDATE t SET v = ? WHERE id = ?").unwrap();
        let bound = bind(&stmt, &[Value::String("y".into()), Value::Int(1)]).unwrap();
        assert_eq!(bound, parse("UPDATE t SET v = 'y' WHERE id = 1").unwrap());

        let stmt = parse("DELETE FROM t WHERE id = ?").unwrap();
        let bound = bind(&stmt, &[Value::Int(9)]).unwrap();
        assert_eq!(bound, parse("DELETE FROM t WHERE id = 9").unwrap());
    }

    #[test]
    fn arity_mismatch_errors() {
        let stmt = parse("SELECT v FROM t WHERE id = ?").unwrap();
        assert_eq!(
            bind(&stmt, &[]),
            Err(BindError::Arity {
                expected: 1,
                got: 0
            })
        );
        assert_eq!(
            bind(&stmt, &[Value::Int(1), Value::Int(2)]),
            Err(BindError::Arity {
                expected: 1,
                got: 2
            })
        );
    }

    #[test]
    fn ddl_is_unpreparable() {
        let stmt = parse("CREATE TABLE t (PRIMARY KEY (id))").unwrap();
        assert_eq!(param_count(&stmt), None);
        assert_eq!(bind(&stmt, &[]), Err(BindError::Unpreparable));
    }

    #[test]
    fn zero_param_dml_binds() {
        let stmt = parse("SELECT v FROM t WHERE id = 1").unwrap();
        assert_eq!(param_count(&stmt), Some(0));
        assert_eq!(bind(&stmt, &[]).unwrap(), stmt);
    }
}
