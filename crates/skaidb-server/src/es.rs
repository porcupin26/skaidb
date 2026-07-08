//! Elasticsearch-compatible REST subset (docs/FTS_TODO.md phase 8):
//! `_bulk`, `_search`, `_count`, and read-only `_mapping` — enough for
//! existing ES client libraries and log shippers, not Kibana.
//!
//! An ES "index" maps to a skaidb **table** (whose `SEARCH INDEX` supplies
//! the mapping). Every request translates to a SQL statement AST and runs
//! through the ordinary session path, so RBAC, cluster routing, and the
//! search pushdowns all apply unchanged. `_id` maps to the table's
//! single-column primary key (written as a string; auto-generated when a
//! bulk action omits it).

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{json, Map, Value as Json};
use skaidb_sql::ast::{
    AggArg, AggFunc, BinaryOp, Expr, OrderKey, Select, SelectItem, Statement, UnaryOp,
};
use skaidb_types::Value;

use crate::shared::{execute_session_statement_as, Shared};
use skaidb_proto::Response;

/// Auto-generated `_id` uniqueness within a process.
static BULK_SEQ: AtomicU64 = AtomicU64::new(0);

/// Cheap path filter so non-ES requests skip the ES handler entirely.
pub fn path_is_es(path: &str) -> bool {
    let path = path.split('?').next().unwrap_or(path);
    matches!(
        path.trim_matches('/').rsplit('/').next(),
        Some("_bulk" | "_search" | "_count" | "_mapping")
    )
}

/// Route an ES-style request; `None` when the path is not ours. Returns
/// `(http_status, body)`.
pub fn try_route(
    ctx: &Shared,
    role: &str,
    method: &str,
    path: &str,
    body: &[u8],
) -> Option<(u16, Json)> {
    let path = path.split('?').next().unwrap_or(path);
    let segs: Vec<&str> = path.trim_matches('/').split('/').collect();
    let out = match (method, segs.as_slice()) {
        ("POST" | "PUT", ["_bulk"]) => bulk(ctx, role, None, body),
        ("POST" | "PUT", [index, "_bulk"]) => bulk(ctx, role, Some(index), body),
        ("POST" | "GET", [index, "_search"]) => search(ctx, role, index, body),
        ("POST" | "GET", [index, "_count"]) => count(ctx, role, index, body),
        ("GET", [index, "_mapping"]) => mapping(ctx, index),
        _ => return None,
    };
    Some(match out {
        Ok(ok) => ok,
        Err(msg) => es_error(400, &msg),
    })
}

fn es_error(status: u16, reason: &str) -> (u16, Json) {
    (
        status,
        json!({"error": {"type": "illegal_argument_exception", "reason": reason}, "status": status}),
    )
}

/// A bare single-table SELECT skeleton.
fn select_from(table: &str) -> Select {
    Select {
        distinct: false,
        nearest: None,
        items: Vec::new(),
        from: table.to_string(),
        from_alias: table.to_string(),
        joins: Vec::new(),
        filter: None,
        group_by: Vec::new(),
        having: None,
        set_ops: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
}

/// Run one statement through the session path (RBAC, cluster, metrics).
fn run(ctx: &Shared, role: &str, stmt: Statement, audit: &str) -> Result<Response, String> {
    let mut current_db = skaidb_engine::DEFAULT_DATABASE.to_string();
    Ok(execute_session_statement_as(
        ctx,
        role,
        &mut current_db,
        audit,
        Ok(stmt),
        None,
    ))
}

fn rows_of(resp: Response) -> Result<(Vec<String>, Vec<Vec<Value>>), String> {
    match resp {
        Response::Rows { columns, rows } => Ok((columns, rows)),
        Response::Error(e) => Err(e),
        other => Err(format!("unexpected engine response: {other:?}")),
    }
}

// ---- query DSL translation ----

fn func(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Func {
        name: name.to_string(),
        args,
    }
}

fn str_lit(s: &str) -> Expr {
    Expr::Literal(Value::String(s.to_string()))
}

fn field_and_text(name: &str, body: &Json) -> Result<(String, String), String> {
    let obj = body
        .as_object()
        .ok_or_else(|| format!("{name} expects an object"))?;
    let (col, spec) = obj
        .iter()
        .next()
        .ok_or_else(|| format!("{name} needs a field"))?;
    let text = match spec {
        Json::String(s) => s.clone(),
        Json::Object(o) => o
            .get("query")
            .or_else(|| o.get("value"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("{name}.{col} needs a query/value string"))?
            .to_string(),
        other => other.to_string().trim_matches('"').to_string(),
    };
    Ok((col.clone(), text))
}

fn and(exprs: Vec<Expr>) -> Option<Expr> {
    exprs.into_iter().reduce(|l, r| Expr::Binary {
        op: BinaryOp::And,
        left: Box::new(l),
        right: Box::new(r),
    })
}

fn or(exprs: Vec<Expr>) -> Option<Expr> {
    exprs.into_iter().reduce(|l, r| Expr::Binary {
        op: BinaryOp::Or,
        left: Box::new(l),
        right: Box::new(r),
    })
}

/// Translate an ES query DSL object into a WHERE expression (`Ok(None)` =
/// `match_all`).
fn query_expr(q: &Json) -> Result<Option<Expr>, String> {
    let obj = q.as_object().ok_or("query must be an object")?;
    let (kind, body) = match obj.iter().next() {
        Some(kv) => kv,
        None => return Ok(None),
    };
    match kind.as_str() {
        "match_all" => Ok(None),
        "match" => {
            let (col, text) = field_and_text("match", body)?;
            Ok(Some(func("match", vec![Expr::Column(col), str_lit(&text)])))
        }
        "match_phrase" => {
            let (col, text) = field_and_text("match_phrase", body)?;
            let mut args = vec![Expr::Column(col.clone()), str_lit(&text)];
            if let Some(slop) = body[&col]["slop"].as_i64() {
                args.push(Expr::Literal(Value::Int(slop)));
            }
            Ok(Some(func("match_phrase", args)))
        }
        "match_phrase_prefix" | "prefix" => {
            let (col, text) = field_and_text(kind, body)?;
            Ok(Some(func(
                "match_prefix",
                vec![Expr::Column(col), str_lit(&text)],
            )))
        }
        "wildcard" | "regexp" => {
            let (col, text) = field_and_text(kind, body)?;
            Ok(Some(func(kind, vec![Expr::Column(col), str_lit(&text)])))
        }
        "fuzzy" => {
            let (col, text) = field_and_text("fuzzy", body)?;
            let mut args = vec![Expr::Column(col.clone()), str_lit(&text)];
            if let Some(d) = body[&col]["fuzziness"].as_i64() {
                args.push(Expr::Literal(Value::Int(d)));
            }
            Ok(Some(func("fuzzy", args)))
        }
        "more_like_this" => {
            let fields = body["fields"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|f| f.as_str())
                .ok_or("more_like_this needs fields: [column]")?;
            let like = body["like"]
                .as_str()
                .ok_or("more_like_this needs like: '<text>'")?;
            Ok(Some(func(
                "more_like_this",
                vec![Expr::Column(fields.to_string()), str_lit(like)],
            )))
        }
        "query_string" => {
            let text = body["query"]
                .as_str()
                .ok_or("query_string needs query: '<text>'")?;
            Ok(Some(func("search", vec![str_lit(text)])))
        }
        // Exact comparisons run as residual predicates over the row —
        // right for keyword/numeric/bool/date fields (ES best practice for
        // `term` anyway; on analyzed text use `match`).
        "term" => {
            let obj = body.as_object().ok_or("term expects an object")?;
            let (col, spec) = obj.iter().next().ok_or("term needs a field")?;
            let v = spec.get("value").unwrap_or(spec);
            Ok(Some(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Column(col.clone())),
                right: Box::new(Expr::Literal(Value::from_json(v.clone()))),
            }))
        }
        "terms" => {
            let obj = body.as_object().ok_or("terms expects an object")?;
            let (col, list) = obj.iter().next().ok_or("terms needs a field")?;
            let vals = list.as_array().ok_or("terms expects an array")?;
            let eqs: Vec<Expr> = vals
                .iter()
                .map(|v| Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(Expr::Column(col.clone())),
                    right: Box::new(Expr::Literal(Value::from_json(v.clone()))),
                })
                .collect();
            Ok(or(eqs))
        }
        "range" => {
            let obj = body.as_object().ok_or("range expects an object")?;
            let (col, bounds) = obj.iter().next().ok_or("range needs a field")?;
            let bounds = bounds.as_object().ok_or("range bounds must be an object")?;
            let mut parts = Vec::new();
            for (bound, op) in [
                ("gt", BinaryOp::Gt),
                ("gte", BinaryOp::GtEq),
                ("lt", BinaryOp::Lt),
                ("lte", BinaryOp::LtEq),
            ] {
                if let Some(v) = bounds.get(bound) {
                    parts.push(Expr::Binary {
                        op,
                        left: Box::new(Expr::Column(col.clone())),
                        right: Box::new(Expr::Literal(Value::from_json(v.clone()))),
                    });
                }
            }
            Ok(and(parts))
        }
        "exists" => {
            let col = body["field"].as_str().ok_or("exists needs field")?;
            Ok(Some(Expr::IsNull {
                expr: Box::new(Expr::Column(col.to_string())),
                negated: true,
            }))
        }
        "bool" => {
            let mut parts = Vec::new();
            for clause in ["must", "filter"] {
                for sub in body[clause].as_array().into_iter().flatten() {
                    if let Some(e) = query_expr(sub)? {
                        parts.push(e);
                    }
                }
                // A single object (not wrapped in an array) is legal ES.
                if body[clause].is_object() {
                    if let Some(e) = query_expr(&body[clause])? {
                        parts.push(e);
                    }
                }
            }
            for sub in body["must_not"].as_array().into_iter().flatten() {
                if let Some(e) = query_expr(sub)? {
                    parts.push(Expr::Unary {
                        op: UnaryOp::Not,
                        expr: Box::new(e),
                    });
                }
            }
            let shoulds: Vec<Expr> = body["should"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|sub| query_expr(sub).transpose())
                .collect::<Result<_, _>>()?;
            if !shoulds.is_empty() {
                if parts.is_empty() {
                    return Ok(or(shoulds));
                }
                // ES `should` beside must/filter only boosts scores; our
                // subset cannot express optional-scoring clauses.
                return Err("bool.should combined with must/filter is not supported".into());
            }
            Ok(and(parts))
        }
        other => Err(format!("unsupported query type '{other}'")),
    }
}

// ---- _search ----

fn search(ctx: &Shared, role: &str, index: &str, body: &[u8]) -> Result<(u16, Json), String> {
    let body: Json = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(body).map_err(|e| format!("bad JSON body: {e}"))?
    };
    let filter = match body.get("query") {
        Some(q) => query_expr(q)?,
        None => None,
    };
    let size = body["size"].as_u64().unwrap_or(10);
    let from = body["from"].as_u64().unwrap_or(0);
    let uses_search = filter
        .as_ref()
        .map(expr_uses_search)
        .unwrap_or(false);

    // Exact total, ES `track_total_hits: true` semantics (a COUNT(*) is a
    // cheap pushdown either way).
    let mut count_sel = select_from(index);
    count_sel.items = vec![SelectItem::Expr {
        expr: Expr::Aggregate {
            func: AggFunc::Count,
            arg: AggArg::Star,
        },
        alias: None,
    }];
    count_sel.filter = filter.clone();
    let (_, count_rows) = rows_of(run(ctx, role, Statement::Select(count_sel), "es:_search#count")?)?;
    let total = count_rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .unwrap_or(0);

    // Hits (skipped for size 0, the aggregations-only shape).
    let mut hits = Vec::new();
    let mut max_score = Json::Null;
    if size > 0 {
        let mut sel = select_from(index);
        sel.items = vec![SelectItem::Wildcard];
        // highlight: {fields: {col: {}}} → HIGHLIGHT(col) triggers; the
        // snippets come back as injected `_highlight_<col>` fields.
        let mut hl_cols = Vec::new();
        if let Some(fields) = body["highlight"]["fields"].as_object() {
            for col in fields.keys() {
                hl_cols.push(col.clone());
                sel.items.push(SelectItem::Expr {
                    expr: func("highlight", vec![Expr::Column(col.clone())]),
                    alias: Some(format!("_es_hl_{col}")),
                });
            }
        }
        sel.filter = filter.clone();
        sel.limit = Some(size);
        sel.offset = (from > 0).then_some(from);
        match body.get("sort") {
            // Default: relevance order for search queries, unspecified
            // otherwise (like SQL without ORDER BY).
            None if uses_search => {
                sel.order_by = vec![OrderKey {
                    expr: func("score", vec![]),
                    descending: true,
                }];
            }
            None => {}
            Some(sort) => {
                let (col, desc) = parse_sort(sort)?;
                sel.order_by = vec![OrderKey {
                    expr: if col == "_score" {
                        func("score", vec![])
                    } else {
                        Expr::Column(col)
                    },
                    descending: desc,
                }];
            }
        }
        let pk = ctx
            .backend
            .table_primary_key(index)
            .ok_or_else(|| format!("no such index (table) '{index}'"))?;
        let pk = pk.first().cloned().unwrap_or_default();
        let (columns, rows) = rows_of(run(ctx, role, Statement::Select(sel), "es:_search")?)?;
        for row in rows {
            let mut source = Map::new();
            let mut id = Json::Null;
            let mut score = Json::Null;
            let mut highlight = Map::new();
            for (col, val) in columns.iter().zip(row) {
                if col == "_score" {
                    score = val.to_json();
                } else if let Some(hl) = col.strip_prefix("_es_hl_") {
                    if !matches!(val, Value::Null) {
                        highlight.insert(hl.to_string(), json!([val.to_json()]));
                    }
                } else if let Some(hl) = col.strip_prefix("_highlight_") {
                    if !matches!(val, Value::Null) && hl_cols.iter().any(|c| c == hl) {
                        highlight.insert(hl.to_string(), json!([val.to_json()]));
                    }
                } else {
                    if col == &pk {
                        id = match &val {
                            Value::String(s) => Json::String(s.clone()),
                            other => Json::String(other.to_json().to_string()),
                        };
                    }
                    source.insert(col.clone(), val.to_json());
                }
            }
            if max_score.is_null() && !score.is_null() {
                max_score = score.clone();
            }
            let include_source = body.get("_source").map(|s| s != &json!(false)).unwrap_or(true);
            let mut hit = json!({"_index": index, "_id": id, "_score": score});
            if include_source {
                hit["_source"] = Json::Object(source);
            }
            if !highlight.is_empty() {
                hit["highlight"] = Json::Object(highlight);
            }
            hits.push(hit);
        }
    }

    let mut out = json!({
        "took": 0,
        "timed_out": false,
        "hits": {
            "total": {"value": total, "relation": "eq"},
            "max_score": max_score,
            "hits": hits,
        }
    });
    if let Some(aggs) = body.get("aggs").or_else(|| body.get("aggregations")) {
        out["aggregations"] = run_aggs(ctx, role, index, &filter, aggs)?;
    }
    Ok((200, out))
}

fn expr_uses_search(e: &Expr) -> bool {
    match e {
        Expr::Func { name, .. } => matches!(
            name.as_str(),
            "match"
                | "match_phrase"
                | "match_prefix"
                | "fuzzy"
                | "wildcard"
                | "regexp"
                | "more_like_this"
                | "search"
        ),
        Expr::Unary { expr, .. } => expr_uses_search(expr),
        Expr::Binary { left, right, .. } => expr_uses_search(left) || expr_uses_search(right),
        _ => false,
    }
}

fn parse_sort(sort: &Json) -> Result<(String, bool), String> {
    let entry = match sort {
        Json::Array(items) if items.len() == 1 => &items[0],
        Json::Array(_) => return Err("only a single sort key is supported".into()),
        one => one,
    };
    match entry {
        Json::String(col) => Ok((col.clone(), col == "_score")),
        Json::Object(o) => {
            let (col, spec) = o.iter().next().ok_or("empty sort entry")?;
            let desc = spec["order"].as_str().unwrap_or("asc") == "desc";
            Ok((col.clone(), desc))
        }
        _ => Err("unsupported sort specification".into()),
    }
}

// ---- aggregations ----

/// One top-level agg: terms / date_histogram buckets with optional metric
/// sub-aggs, or a bare metric.
fn run_aggs(
    ctx: &Shared,
    role: &str,
    index: &str,
    filter: &Option<Expr>,
    aggs: &Json,
) -> Result<Json, String> {
    let aggs = aggs.as_object().ok_or("aggs must be an object")?;
    let mut out = Map::new();
    for (name, spec) in aggs {
        out.insert(name.clone(), run_one_agg(ctx, role, index, filter, spec)?);
    }
    Ok(Json::Object(out))
}

fn metric_item(func_name: AggFunc, field: &str) -> SelectItem {
    SelectItem::Expr {
        expr: Expr::Aggregate {
            func: func_name,
            arg: AggArg::Expr(Box::new(Expr::Column(field.to_string()))),
        },
        alias: None,
    }
}

fn parse_metrics(spec: &Json) -> Result<Vec<(String, SelectItem)>, String> {
    let mut out = Vec::new();
    if let Some(subs) = spec["aggs"].as_object().or_else(|| spec["aggregations"].as_object()) {
        for (mname, mspec) in subs {
            let mobj = mspec.as_object().ok_or("sub-agg must be an object")?;
            let (kind, body) = mobj.iter().next().ok_or("empty sub-agg")?;
            let field = body["field"].as_str().ok_or("metric agg needs field")?;
            let f = match kind.as_str() {
                "sum" => AggFunc::Sum,
                "avg" => AggFunc::Avg,
                "min" => AggFunc::Min,
                "max" => AggFunc::Max,
                "value_count" => AggFunc::Count,
                "cardinality" => {
                    out.push((
                        mname.clone(),
                        SelectItem::Expr {
                            expr: Expr::Aggregate {
                                func: AggFunc::Count,
                                arg: AggArg::Distinct(Box::new(Expr::Column(field.to_string()))),
                            },
                            alias: None,
                        },
                    ));
                    continue;
                }
                other => return Err(format!("unsupported sub-aggregation '{other}'")),
            };
            out.push((mname.clone(), metric_item(f, field)));
        }
    }
    Ok(out)
}

fn run_one_agg(
    ctx: &Shared,
    role: &str,
    index: &str,
    filter: &Option<Expr>,
    spec: &Json,
) -> Result<Json, String> {
    let obj = spec.as_object().ok_or("aggregation must be an object")?;
    let (kind, body) = obj
        .iter()
        .find(|(k, _)| *k != "aggs" && *k != "aggregations")
        .ok_or("empty aggregation")?;
    let mut sel = select_from(index);
    sel.filter = filter.clone();
    match kind.as_str() {
        "terms" | "date_histogram" => {
            let field = body["field"].as_str().ok_or("bucket agg needs field")?;
            let group = if kind == "terms" {
                Expr::Column(field.to_string())
            } else {
                let interval = body["fixed_interval"]
                    .as_str()
                    .ok_or("date_histogram needs fixed_interval")?;
                let ms = parse_interval_ms(interval)?;
                func(
                    "time_bucket",
                    vec![Expr::Literal(Value::Int(ms)), Expr::Column(field.to_string())],
                )
            };
            sel.group_by = vec![group.clone()];
            sel.items = vec![
                SelectItem::Expr {
                    expr: group,
                    alias: None,
                },
                SelectItem::Expr {
                    expr: Expr::Aggregate {
                        func: AggFunc::Count,
                        arg: AggArg::Star,
                    },
                    alias: None,
                },
            ];
            let metrics = parse_metrics(spec)?;
            for (_, item) in &metrics {
                sel.items.push(item.clone());
            }
            let (_, rows) = rows_of(run(ctx, role, Statement::Select(sel), "es:aggs")?)?;
            let mut buckets: Vec<Json> = rows
                .into_iter()
                .map(|row| {
                    let mut bucket = json!({
                        "key": row[0].to_json(),
                        "doc_count": row[1].to_json(),
                    });
                    for (i, (mname, _)) in metrics.iter().enumerate() {
                        bucket[mname.as_str()] = json!({"value": row[2 + i].to_json()});
                    }
                    bucket
                })
                .collect();
            // ES orders terms buckets by doc_count descending and caps at
            // `size` (default 10); histograms stay key-ordered.
            if kind == "terms" {
                buckets.sort_by_key(|b| std::cmp::Reverse(b["doc_count"].as_i64().unwrap_or(0)));
                let size = body["size"].as_u64().unwrap_or(10) as usize;
                buckets.truncate(size);
            } else {
                buckets.sort_by_key(|b| b["key"].as_i64().unwrap_or(0));
            }
            Ok(json!({"buckets": buckets}))
        }
        "sum" | "avg" | "min" | "max" | "value_count" | "cardinality" => {
            let field = body["field"].as_str().ok_or("metric agg needs field")?;
            let item = match kind.as_str() {
                "sum" => metric_item(AggFunc::Sum, field),
                "avg" => metric_item(AggFunc::Avg, field),
                "min" => metric_item(AggFunc::Min, field),
                "max" => metric_item(AggFunc::Max, field),
                "value_count" => metric_item(AggFunc::Count, field),
                _ => SelectItem::Expr {
                    expr: Expr::Aggregate {
                        func: AggFunc::Count,
                        arg: AggArg::Distinct(Box::new(Expr::Column(field.to_string()))),
                    },
                    alias: None,
                },
            };
            sel.items = vec![item];
            let (_, rows) = rows_of(run(ctx, role, Statement::Select(sel), "es:aggs")?)?;
            let value = rows
                .first()
                .and_then(|r| r.first())
                .map(|v| v.to_json())
                .unwrap_or(Json::Null);
            Ok(json!({"value": value}))
        }
        other => Err(format!("unsupported aggregation '{other}'")),
    }
}

fn parse_interval_ms(spec: &str) -> Result<i64, String> {
    let (num, unit) = spec.split_at(spec.find(|c: char| c.is_alphabetic()).unwrap_or(spec.len()));
    let n: i64 = num.parse().map_err(|_| format!("bad interval '{spec}'"))?;
    let mult = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return Err(format!("bad interval unit '{unit}'")),
    };
    Ok(n * mult)
}

// ---- _count ----

fn count(ctx: &Shared, role: &str, index: &str, body: &[u8]) -> Result<(u16, Json), String> {
    let body: Json = if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(body).map_err(|e| format!("bad JSON body: {e}"))?
    };
    let mut sel = select_from(index);
    sel.items = vec![SelectItem::Expr {
        expr: Expr::Aggregate {
            func: AggFunc::Count,
            arg: AggArg::Star,
        },
        alias: None,
    }];
    sel.filter = match body.get("query") {
        Some(q) => query_expr(q)?,
        None => None,
    };
    let (_, rows) = rows_of(run(ctx, role, Statement::Select(sel), "es:_count")?)?;
    let n = rows
        .first()
        .and_then(|r| r.first())
        .map(|v| v.to_json())
        .unwrap_or(json!(0));
    Ok((200, json!({"count": n})))
}

// ---- _mapping ----

fn mapping(ctx: &Shared, index: &str) -> Result<(u16, Json), String> {
    let Some(fields) = ctx.backend.search_index_fields(index) else {
        return Ok(es_error(
            404,
            &format!("no search index on table '{index}'"),
        ));
    };
    let mut properties = Map::new();
    for (path, ftype) in fields {
        properties.insert(path, json!({"type": ftype}));
    }
    Ok((
        200,
        json!({index: {"mappings": {"properties": properties}}}),
    ))
}

// ---- _bulk ----

fn bulk(
    ctx: &Shared,
    role: &str,
    default_index: Option<&str>,
    body: &[u8],
) -> Result<(u16, Json), String> {
    let text = std::str::from_utf8(body).map_err(|_| "bulk body must be UTF-8 NDJSON")?;
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());

    // Parse actions first: (verb, index, id, source?) in order.
    struct Action {
        verb: &'static str,
        index: String,
        id: String,
        source: Option<Json>,
    }
    let mut actions = Vec::new();
    while let Some(line) = lines.next() {
        let action: Json =
            serde_json::from_str(line).map_err(|e| format!("bad action line: {e}"))?;
        let obj = action.as_object().ok_or("action must be an object")?;
        let (verb_s, meta) = obj.iter().next().ok_or("empty action")?;
        let verb = match verb_s.as_str() {
            "index" => "index",
            "create" => "create",
            "delete" => "delete",
            other => return Err(format!("unsupported bulk action '{other}'")),
        };
        let index = meta["_index"]
            .as_str()
            .or(default_index)
            .ok_or("bulk action needs _index (or use /<index>/_bulk)")?
            .to_string();
        let id = match meta["_id"].as_str() {
            Some(id) => id.to_string(),
            None => format!(
                "{:x}-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0),
                BULK_SEQ.fetch_add(1, Ordering::Relaxed)
            ),
        };
        let source = if verb == "delete" {
            None
        } else {
            let line = lines.next().ok_or("action missing its source line")?;
            Some(serde_json::from_str(line).map_err(|e| format!("bad source line: {e}"))?)
        };
        actions.push(Action {
            verb,
            index,
            id,
            source,
        });
    }

    let mut items = Vec::with_capacity(actions.len());
    let mut errors = false;
    for action in actions {
        let pk = match ctx.backend.table_primary_key(&action.index) {
            Some(pk) if pk.len() == 1 => pk[0].clone(),
            Some(_) => {
                errors = true;
                items.push(bulk_item(action.verb, &action.index, &action.id, 400,
                    Some("composite primary keys cannot map to _id")));
                continue;
            }
            None => {
                errors = true;
                items.push(bulk_item(action.verb, &action.index, &action.id, 404,
                    Some("no such index (table)")));
                continue;
            }
        };
        let stmt = match action.source {
            Some(source) => {
                let Json::Object(fields) = source else {
                    errors = true;
                    items.push(bulk_item(action.verb, &action.index, &action.id, 400,
                        Some("source must be a JSON object")));
                    continue;
                };
                let mut columns = vec![pk.clone()];
                let mut row = vec![Expr::Literal(Value::String(action.id.clone()))];
                for (k, v) in fields {
                    if k == pk {
                        continue; // _id wins over a source field of the same name
                    }
                    columns.push(k);
                    row.push(Expr::Literal(Value::from_json(v)));
                }
                Statement::Insert(skaidb_sql::ast::Insert {
                    table: action.index.clone(),
                    columns,
                    rows: vec![row],
                })
            }
            None => Statement::Delete(skaidb_sql::ast::Delete {
                table: action.index.clone(),
                filter: Some(Expr::Binary {
                    op: BinaryOp::Eq,
                    left: Box::new(Expr::Column(pk)),
                    right: Box::new(Expr::Literal(Value::String(action.id.clone()))),
                }),
            }),
        };
        match run(ctx, role, stmt, "es:_bulk")? {
            Response::Error(e) => {
                errors = true;
                items.push(bulk_item(action.verb, &action.index, &action.id, 400, Some(&e)));
            }
            _ => {
                let status = if action.verb == "delete" { 200 } else { 201 };
                items.push(bulk_item(action.verb, &action.index, &action.id, status, None));
            }
        }
    }
    Ok((200, json!({"took": 0, "errors": errors, "items": items})))
}

fn bulk_item(verb: &str, index: &str, id: &str, status: u16, error: Option<&str>) -> Json {
    let mut body = json!({"_index": index, "_id": id, "status": status});
    if let Some(e) = error {
        body["error"] = json!({"type": "illegal_argument_exception", "reason": e});
    }
    json!({ verb: body })
}
