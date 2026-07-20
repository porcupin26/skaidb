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
    #[error("{0}")]
    Value(String),
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
    // `LIMIT ?` / `OFFSET ?` positions live outside the expression tree.
    fn count_limit_params(stmt: &Statement, n: &mut usize) {
        match stmt {
            Statement::Select(s) => {
                for p in [s.limit_param, s.offset_param].into_iter().flatten() {
                    *n = (*n).max(p as usize + 1);
                }
            }
            Statement::Explain { statement } => count_limit_params(statement, n),
            _ => {}
        }
    }
    count_limit_params(stmt, &mut n);
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
    // `LIMIT ?` / `OFFSET ?`: the bound value must be a non-negative integer;
    // it lands in the plain `limit`/`offset` count, and the param slot clears.
    fn bind_counts(stmt: &mut Statement, params: &[Value]) -> Result<(), BindError> {
        match stmt {
            Statement::Explain { statement } => bind_counts(statement, params),
            Statement::Select(s) => {
                let as_count = |what: &str, v: &Value| -> Result<u64, BindError> {
                    match v {
                        Value::Int(n) if *n >= 0 => Ok(*n as u64),
                        other => Err(BindError::Value(format!(
                            "{what} parameter must be a non-negative integer, got {:?}",
                            other.type_of()
                        ))),
                    }
                };
                if let Some(i) = s.limit_param.take() {
                    s.limit = Some(as_count("LIMIT", &params[i as usize])?);
                }
                if let Some(i) = s.offset_param.take() {
                    s.offset = Some(as_count("OFFSET", &params[i as usize])?);
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    bind_counts(&mut bound, params)?;
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
        // EXPLAIN of a preparable statement is itself preparable — you must
        // be able to EXPLAIN exactly the bound query you run (a typed array
        // param has no SQL text form, so client-side interpolation cannot
        // render it for a one-shot EXPLAIN).
        Statement::Explain { statement } => visit_statement(statement, f),
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
    for e in s.after.iter().flatten() {
        visit_expr(e, f);
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
        Expr::InList { expr, list, .. } => {
            visit_expr(expr, f);
            for item in list {
                visit_expr(item, f);
            }
        }
        Expr::Between { expr, lo, hi, .. } => {
            visit_expr(expr, f);
            visit_expr(lo, f);
            visit_expr(hi, f);
        }
        Expr::Like { expr, pattern, .. } => {
            visit_expr(expr, f);
            visit_expr(pattern, f);
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
    for e in s.after.iter_mut().flatten() {
        mutate_expr(e, f);
    }
}

/// Mutable mirror of [`visit_statement`] over the statement kinds it accepts.
fn mutate_statement(stmt: &mut Statement, f: &mut impl FnMut(&mut Expr)) {
    match stmt {
        Statement::Select(s) => mutate_select(s, f),
        Statement::Explain { statement } => mutate_statement(statement, f),
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
        Expr::InList { expr, list, .. } => {
            mutate_expr(expr, f);
            for item in list {
                mutate_expr(item, f);
            }
        }
        Expr::Between { expr, lo, hi, .. } => {
            mutate_expr(expr, f);
            mutate_expr(lo, f);
            mutate_expr(hi, f);
        }
        Expr::Like { expr, pattern, .. } => {
            mutate_expr(expr, f);
            mutate_expr(pattern, f);
        }
        Expr::Literal(_) | Expr::Column(_) | Expr::Parameter(_) => {}
    }
}

/// Resolve select-list output aliases referenced from ORDER BY, GROUP BY,
/// and HAVING (`SELECT count(*) AS c ... HAVING c > 1 ORDER BY c`).
/// Without this, an alias reference evaluates against source documents,
/// reads NULL, and the query SUCCEEDS with wrongly-ordered or empty
/// results — the silent-failure mode this rewrite exists to kill.
///
/// Semantics (Postgres-informed, adapted to schemaless tables where
/// "does a source column with this name exist?" is undecidable):
/// - ORDER BY / GROUP BY resolve bare top-level names only (`ORDER BY c`,
///   not `ORDER BY c + 1`), matching how SQL treats output-column names.
/// - GROUP BY and HAVING skip self-referential aliases
///   (`upper(name) AS name` — the bare name keeps its source-column
///   meaning, mirroring SQL's input-column preference there); ORDER BY
///   prefers the output column, so it always resolves.
/// - HAVING resolves references anywhere in the predicate tree
///   (`HAVING c > 1` nests the reference), without re-entering
///   replacements — alias cycles cannot loop.
pub fn resolve_select_aliases(sel: &mut Select) {
    let aliases: Vec<(String, Expr)> = sel
        .items
        .iter()
        .filter_map(|i| match i {
            SelectItem::Expr {
                expr,
                alias: Some(a),
            } => Some((a.clone(), expr.clone())),
            _ => None,
        })
        .collect();
    if aliases.is_empty() {
        return;
    }
    let target = |name: &str, skip_self_ref: bool| -> Option<&Expr> {
        let (_, t) = aliases.iter().find(|(a, _)| a == name)?;
        (!(skip_self_ref && contains_column(t, name))).then_some(t)
    };
    for o in &mut sel.order_by {
        if let Expr::Column(name) = &o.expr {
            if let Some(t) = target(name, false) {
                o.expr = t.clone();
            }
        }
    }
    for g in &mut sel.group_by {
        if let Expr::Column(name) = &*g {
            if let Some(t) = target(name, true) {
                *g = t.clone();
            }
        }
    }
    if let Some(h) = &mut sel.having {
        substitute_aliases(h, &|name| target(name, true).cloned());
    }
}

/// Whether `e` references the column `name` anywhere.
fn contains_column(e: &Expr, name: &str) -> bool {
    let mut found = false;
    mutate_expr(&mut e.clone(), &mut |x| {
        if matches!(x, Expr::Column(c) if c == name) {
            found = true;
        }
    });
    found
}

/// Replace `Column` nodes for which `target` returns a replacement,
/// recursing into ORIGINAL children only — replacements are final.
fn substitute_aliases(e: &mut Expr, target: &impl Fn(&str) -> Option<Expr>) {
    if let Expr::Column(name) = e {
        if let Some(t) = target(name) {
            *e = t;
        }
        return;
    }
    match e {
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => substitute_aliases(expr, target),
        Expr::Binary { left, right, .. } => {
            substitute_aliases(left, target);
            substitute_aliases(right, target);
        }
        Expr::Aggregate { arg, .. } => {
            if let AggArg::Expr(expr) = arg {
                substitute_aliases(expr, target);
            }
        }
        Expr::Func { args, .. } => {
            for arg in args {
                substitute_aliases(arg, target);
            }
        }
        Expr::InList { expr, list, .. } => {
            substitute_aliases(expr, target);
            for item in list {
                substitute_aliases(item, target);
            }
        }
        Expr::Between { expr, lo, hi, .. } => {
            substitute_aliases(expr, target);
            substitute_aliases(lo, target);
            substitute_aliases(hi, target);
        }
        Expr::Like { expr, pattern, .. } => {
            substitute_aliases(expr, target);
            substitute_aliases(pattern, target);
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

    /// EXPLAIN of a preparable statement binds like the statement itself —
    /// you can EXPLAIN exactly the bound query you run (incl. an array
    /// param, which has no SQL text form, and `LIMIT ?`).
    #[test]
    fn explain_of_preparable_binds() {
        let stmt = parse("EXPLAIN SELECT v FROM t WHERE id IN (?) LIMIT ?").unwrap();
        assert_eq!(param_count(&stmt), Some(2));
        let bound = bind(
            &stmt,
            &[
                Value::Array(vec![Value::Int(1), Value::Int(2)]),
                Value::Int(5),
            ],
        )
        .unwrap();
        let Statement::Explain { statement } = &bound else { panic!() };
        let Statement::Select(s) = statement.as_ref() else { panic!() };
        assert_eq!(s.limit, Some(5));
        assert_eq!(s.limit_param, None);
        // EXPLAIN of DDL stays unpreparable.
        let stmt = parse("EXPLAIN CREATE TABLE t (PRIMARY KEY (id))");
        if let Ok(stmt) = stmt {
            assert_eq!(param_count(&stmt), None);
        }
    }

    #[test]
    fn zero_param_dml_binds() {
        let stmt = parse("SELECT v FROM t WHERE id = 1").unwrap();
        assert_eq!(param_count(&stmt), Some(0));
        assert_eq!(bind(&stmt, &[]).unwrap(), stmt);
    }

    /// `LIMIT ?` / `OFFSET ?` are bindable positions: counted, substituted
    /// into plain counts, and type-checked.
    #[test]
    fn bind_after_cursor_params() {
        let stmt =
            parse("SELECT v FROM t WHERE MATCH(b, 'x') ORDER BY v LIMIT 5 AFTER (?, ?)").unwrap();
        assert_eq!(param_count(&stmt), Some(2));
        let bound = bind(&stmt, &[Value::Float(0.5), Value::Int(42)]).unwrap();
        let Statement::Select(s) = &bound else { panic!() };
        assert_eq!(
            s.after,
            Some(vec![
                Expr::Literal(Value::Float(0.5)),
                Expr::Literal(Value::Int(42)),
            ])
        );
    }

    #[test]
    fn bind_limit_and_offset_params() {
        let stmt = parse("SELECT v FROM t WHERE a = ? ORDER BY v LIMIT ? OFFSET ?").unwrap();
        assert_eq!(param_count(&stmt), Some(3));
        let bound = bind(
            &stmt,
            &[Value::String("x".into()), Value::Int(10), Value::Int(20)],
        )
        .unwrap();
        let Statement::Select(s) = &bound else { panic!() };
        assert_eq!(s.limit, Some(10));
        assert_eq!(s.offset, Some(20));
        assert_eq!(s.limit_param, None);
        assert_eq!(s.offset_param, None);

        // Wrong type / negative → a bind error, not silent nonsense.
        let stmt = parse("SELECT v FROM t LIMIT ?").unwrap();
        assert_eq!(param_count(&stmt), Some(1));
        assert!(matches!(
            bind(&stmt, &[Value::String("10".into())]),
            Err(BindError::Value(_))
        ));
        assert!(matches!(
            bind(&stmt, &[Value::Int(-1)]),
            Err(BindError::Value(_))
        ));
        // A literal LIMIT still parses as a plain count.
        let stmt = parse("SELECT v FROM t LIMIT 5").unwrap();
        let Statement::Select(s) = &stmt else { panic!() };
        assert_eq!((s.limit, s.limit_param), (Some(5), None));
    }
}
