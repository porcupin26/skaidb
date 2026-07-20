//! Elasticsearch-compatible REST subset (docs/SEARCH.md "ES-compatible REST subset"):
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
    AggArg, AggFunc, BinaryOp, Expr, Nearest, OrderKey, Rrf, Select, SelectItem, Statement,
    UnaryOp, DEFAULT_RRF_CONSTANT,
};
use skaidb_types::Value;

use crate::shared::{execute_session_statement_as, Shared};
use skaidb_proto::Response;

/// Auto-generated `_id` uniqueness within a process.
static BULK_SEQ: AtomicU64 = AtomicU64::new(0);

/// Cheap path filter so non-ES requests skip the ES handler entirely.
pub fn path_is_es(path: &str) -> bool {
    let path = path.split('?').next().unwrap_or(path);
    let segs: Vec<&str> = path.trim_matches('/').split('/').collect();
    matches!(
        segs.last(),
        Some(&("_bulk" | "_search" | "_count" | "_mapping"))
    ) || (segs.len() == 3 && segs[1] == "_doc")
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
        ("GET", [index, "_doc", id]) => get_doc(ctx, role, index, id),
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
        rrf: None,
        items: Vec::new(),
        from: table.to_string(),
        from_alias: table.to_string(),
        joins: Vec::new(),
        filter: None,
        group_by: Vec::new(),
        group_top: None,
        having: None,
        set_ops: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        limit_param: None,
        offset_param: None,
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
        "multi_match" => {
            let text = body["query"]
                .as_str()
                .ok_or("multi_match needs query: '<text>'")?;
            let fields: Vec<&str> = body["fields"]
                .as_array()
                .ok_or("multi_match needs fields: [...]")?
                .iter()
                .filter_map(|f| f.as_str())
                .collect();
            if fields.is_empty() {
                return Err("multi_match needs at least one field".into());
            }
            if fields.iter().any(|f| f.contains('^')) {
                return Err("multi_match per-field ^boosts are not supported — declare \
                     <col>.boost on the search index instead"
                    .into());
            }
            let mm_type = body["type"].as_str().unwrap_or("best_fields");
            if fields.len() == 1 {
                // One field: every type degenerates to a plain match.
                return Ok(Some(func(
                    "match",
                    vec![Expr::Column(fields[0].to_string()), str_lit(text)],
                )));
            }
            match mm_type {
                // Field-centric (whole-query score per field, best wins)
                // vs term-centric (fields act as one big field).
                "best_fields" | "cross_fields" => {
                    let name = if mm_type == "cross_fields" {
                        "match_cross"
                    } else {
                        "match_best"
                    };
                    let mut args: Vec<Expr> = fields
                        .into_iter()
                        .map(|f| Expr::Column(f.to_string()))
                        .collect();
                    args.push(str_lit(text));
                    Ok(Some(func(name, args)))
                }
                // most_fields sums the per-field scores — exactly what an
                // OR of matches scores.
                "most_fields" => Ok(or(fields
                    .into_iter()
                    .map(|f| func("match", vec![Expr::Column(f.to_string()), str_lit(text)]))
                    .collect())),
                other => Err(format!("multi_match type '{other}' is not supported")),
            }
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
                // minimum_should_match (number or numeric string).
                let msm = match &body["minimum_should_match"] {
                    Json::Null => 0,
                    v => v
                        .as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                        .ok_or("minimum_should_match must be a number")?,
                };
                match msm {
                    1 => {
                        // At least one should is required: plain AND/OR.
                        parts.push(or(shoulds).expect("shoulds is non-empty"));
                    }
                    0 => {
                        // ES default beside must/filter: `should` only
                        // boosts scores. BOOSTED(required, optional...)
                        // expresses exactly that in the index; ordinary
                        // (term/range) must-parts stay AND-ed on top —
                        // being required, they shift every hit equally in
                        // ES too, so the ranking is unaffected.
                        let (search_parts, ordinary): (Vec<Expr>, Vec<Expr>) =
                            parts.into_iter().partition(expr_uses_search);
                        if search_parts.is_empty() {
                            return Err("bool.should beside a non-search must/filter cannot \
                                 boost scores; set minimum_should_match: 1 to make the should \
                                 clauses required instead"
                                .into());
                        }
                        let mut args = vec![and(search_parts).expect("non-empty")];
                        args.extend(shoulds);
                        parts = ordinary;
                        parts.push(func("boosted", args));
                    }
                    n => {
                        return Err(format!(
                            "minimum_should_match: {n} is not supported (only 0 or 1)"
                        ))
                    }
                }
            }
            Ok(and(parts))
        }
        other => Err(format!("unsupported query type '{other}'")),
    }
}

/// A parsed vector-retrieval request: the `NEAREST` clause, an optional WHERE
/// filter, and (for `retriever { rrf }`) the fusion constant.
struct VectorSpec {
    nearest: Nearest,
    filter: Option<Expr>,
    rrf: Option<Rrf>,
}

/// Translate an ES `knn` block into a `NEAREST` clause + optional filter.
/// `query_vector` (a float array) searches a plain vector index; a
/// `query_vector_builder.text_embedding.model_text` (or a convenience `text`)
/// string searches a **managed (`EMBED`) index** and is auto-embedded by the
/// engine. `num_candidates` has no per-query knob (HNSW breadth is set on the
/// index via `ALTER VECTOR INDEX`), so it is accepted and ignored.
fn knn_nearest(knn: &Json) -> Result<(Nearest, Option<Expr>), String> {
    let obj = knn.as_object().ok_or("knn must be an object")?;
    let field = obj
        .get("field")
        .and_then(|v| v.as_str())
        .ok_or("knn.field is required")?
        .to_string();
    let k = obj.get("k").and_then(|v| v.as_u64()).unwrap_or(10);
    let query = if let Some(qv) = obj.get("query_vector") {
        if !qv.is_array() {
            return Err("knn.query_vector must be an array of numbers".into());
        }
        Expr::Literal(Value::from_json(qv.clone()))
    } else if let Some(text) = knn_query_text(obj) {
        Expr::Literal(Value::String(text))
    } else {
        return Err(
            "knn needs a query_vector array or a query_vector_builder text (managed EMBED index)"
                .into(),
        );
    };
    let filter = match obj.get("filter") {
        Some(f) => query_expr(f)?,
        None => None,
    };
    Ok((
        Nearest {
            path: field,
            query,
            k: Expr::Literal(Value::Int(k as i64)),
        },
        filter,
    ))
}

/// The text of a semantic `knn` (ES `query_vector_builder.text_embedding.
/// model_text`, or a convenience bare `text` key).
fn knn_query_text(obj: &Map<String, Json>) -> Option<String> {
    obj.get("query_vector_builder")
        .and_then(|b| b.get("text_embedding"))
        .and_then(|t| t.get("model_text"))
        .and_then(|v| v.as_str())
        .or_else(|| obj.get("text").and_then(|v| v.as_str()))
        .map(|s| s.to_string())
}

/// Parse the vector-retrieval part of a `_search` body: a top-level `knn`
/// block (pure/semantic kNN), or a `retriever { rrf { retrievers: [standard,
/// knn] } }` block (hybrid → `NEAREST … WHERE <search> RANK BY RRF`). `None`
/// when the body has neither.
fn parse_vector(body: &Json) -> Result<Option<VectorSpec>, String> {
    if let Some(knn) = body.get("knn") {
        let (nearest, filter) = knn_nearest(knn)?;
        return Ok(Some(VectorSpec { nearest, filter, rrf: None }));
    }
    let Some(retriever) = body.get("retriever") else {
        return Ok(None);
    };
    let rrf = retriever
        .get("rrf")
        .ok_or("only the `rrf` retriever is supported")?;
    let constant = rrf
        .get("rank_constant")
        .and_then(|v| v.as_u64())
        .map(|c| c as u32)
        .unwrap_or(DEFAULT_RRF_CONSTANT);
    let legs = rrf
        .get("retrievers")
        .and_then(|v| v.as_array())
        .ok_or("rrf.retrievers must be an array")?;
    let mut nearest: Option<Nearest> = None;
    let mut filter: Option<Expr> = None;
    for leg in legs {
        if let Some(standard) = leg.get("standard") {
            let q = match standard.get("query") {
                Some(q) => query_expr(q)?,
                None => None,
            };
            filter = merge_and(filter, q);
        } else if let Some(knn) = leg.get("knn") {
            let (n, f) = knn_nearest(knn)?;
            nearest = Some(n);
            filter = merge_and(filter, f);
        } else {
            return Err("rrf.retrievers entries must be `standard` or `knn`".into());
        }
    }
    let nearest = nearest.ok_or("rrf needs a `knn` retriever leg")?;
    Ok(Some(VectorSpec {
        nearest,
        filter,
        rrf: Some(Rrf { constant }),
    }))
}

/// AND two optional WHERE expressions (either may be absent).
fn merge_and(a: Option<Expr>, b: Option<Expr>) -> Option<Expr> {
    match (a, b) {
        (Some(a), Some(b)) => and(vec![a, b]),
        (a, b) => a.or(b),
    }
}

/// Positional args for `HIGHLIGHT(col, …)` from an ES highlight field spec
/// (`field_opts`) with block-level fallback (`block`). Maps `fragment_size` →
/// the size arg, `pre_tags`/`post_tags` (arrays — first element used) → the tag
/// pair, and `no_match_size` → the trailing arg. Emits the minimal form so a
/// bare `{}` stays `HIGHLIGHT(col)`.
fn highlight_args(col: &str, field_opts: &Json, block: &Json) -> Vec<Expr> {
    let num = |key: &str| -> Option<u64> {
        field_opts
            .get(key)
            .and_then(|v| v.as_u64())
            .or_else(|| block.get(key).and_then(|v| v.as_u64()))
    };
    // pre_tags/post_tags are arrays in ES; accept a bare string too.
    fn first_tag(v: &Json) -> Option<String> {
        v.as_array()
            .and_then(|a| a.first())
            .and_then(|x| x.as_str())
            .or_else(|| v.as_str())
            .map(|s| s.to_string())
    }
    let tag = |key: &str| -> Option<String> {
        field_opts
            .get(key)
            .and_then(first_tag)
            .or_else(|| block.get(key).and_then(first_tag))
    };

    let mut args = vec![Expr::Column(col.to_string())];
    let size = num("fragment_size").unwrap_or(150);
    let pre = tag("pre_tags");
    let post = tag("post_tags");
    let no_match = num("no_match_size");
    let want_tags = pre.is_some() || post.is_some();
    let want_no_match = no_match.is_some();
    if size != 150 || want_tags || want_no_match {
        args.push(Expr::Literal(Value::Int(size as i64)));
    }
    if want_tags || want_no_match {
        args.push(str_lit(&pre.unwrap_or_else(|| "<b>".to_string())));
        args.push(str_lit(&post.unwrap_or_else(|| "</b>".to_string())));
    }
    if let Some(n) = no_match {
        args.push(Expr::Literal(Value::Int(n as i64)));
    }
    args
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
    // Vector / hybrid retrieval: a top-level `knn` block or a `retriever { rrf }`
    // block maps to a `NEAREST` clause (+ optional `RANK BY RRF`), which takes a
    // different execution path than a WHERE-only search.
    let vector = parse_vector(&body)?;
    let is_vector = vector.is_some();
    let uses_search = filter
        .as_ref()
        .map(expr_uses_search)
        .unwrap_or(false);

    // Exact total for the plain path, ES `track_total_hits: true` semantics (a
    // COUNT(*) is a cheap pushdown). A kNN/hybrid query returns at most `k`
    // ranked hits, so its total is the retrieved-hit count, set after the fetch.
    let mut total = 0i64;
    if !is_vector {
        let mut count_sel = select_from(index);
        count_sel.items = vec![SelectItem::Expr {
            expr: Expr::Aggregate {
                func: AggFunc::Count,
                arg: AggArg::Star,
            },
            alias: None,
        }];
        count_sel.filter = filter.clone();
        let (_, count_rows) =
            rows_of(run(ctx, role, Statement::Select(count_sel), "es:_search#count")?)?;
        total = count_rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| match v {
                Value::Int(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0);
    }

    // Hits (skipped for size 0, the aggregations-only shape).
    let mut hits = Vec::new();
    let mut max_score = Json::Null;
    if size > 0 {
        let mut sel = select_from(index);
        sel.items = vec![SelectItem::Wildcard];
        // highlight: {fields: {col: {…}}} → HIGHLIGHT(col, …); the snippets
        // come back as injected `_highlight_<col>` fields. Per-field options
        // (fragment_size, pre_tags, post_tags, no_match_size) fall back to the
        // block-level ones, then to skaidb's defaults.
        let mut hl_cols = Vec::new();
        if let Some(hl) = body.get("highlight") {
            if let Some(fields) = hl.get("fields").and_then(|f| f.as_object()) {
                for (col, field_opts) in fields {
                    hl_cols.push(col.clone());
                    sel.items.push(SelectItem::Expr {
                        expr: func("highlight", highlight_args(col, field_opts, hl)),
                        alias: Some(format!("_es_hl_{col}")),
                    });
                }
            }
        }
        if let Some(v) = vector {
            // kNN / hybrid: NEAREST (+ RANK BY RRF) already returns rows ranked
            // (nearest-first, or fused rrf_score desc), so no ORDER BY — the
            // engine rejects ORDER BY alongside NEAREST. The knn/standard-leg
            // filter is the WHERE.
            sel.nearest = Some(Box::new(v.nearest));
            sel.rrf = v.rrf;
            sel.filter = v.filter;
        } else {
            sel.filter = filter.clone();
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
                    sel.order_by = parse_sort(sort)?
                        .into_iter()
                        .map(|(col, desc)| OrderKey {
                            expr: if col == "_score" {
                                func("score", vec![])
                            } else {
                                Expr::Column(col)
                            },
                            descending: desc,
                        })
                        .collect();
                }
            }
        }
        sel.limit = Some(size);
        sel.offset = (from > 0).then_some(from);
        let pk = ctx
            .backend
            .table_primary_key(index)
            .ok_or_else(|| format!("no such index (table) '{index}'"))?;
        let pk = pk.first().cloned().unwrap_or_default();
        let (include_source, includes, excludes) = source_spec(&body);
        let (columns, rows) = rows_of(run(ctx, role, Statement::Select(sel), "es:_search")?)?;
        for row in rows {
            let mut source = Map::new();
            let mut id = Json::Null;
            let mut score = Json::Null;
            let mut highlight = Map::new();
            for (col, val) in columns.iter().zip(row) {
                if col == "_score" {
                    score = val.to_json();
                } else if col == "_rrf_score" {
                    // Hybrid fused score (higher = better, like ES rrf).
                    score = val.to_json();
                } else if col == "_distance" {
                    // Pure kNN: expose an ES-style similarity (higher = better)
                    // derived from the distance, but let a fused score win.
                    if score.is_null() {
                        if let Value::Float(d) = &val {
                            score = json!(1.0 / (1.0 + d.max(0.0)));
                        }
                    }
                    if source_allows(col, &includes, &excludes) {
                        source.insert(col.clone(), val.to_json());
                    }
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
                    if source_allows(col, &includes, &excludes) {
                        source.insert(col.clone(), val.to_json());
                    }
                }
            }
            if max_score.is_null() && !score.is_null() {
                max_score = score.clone();
            }
            let mut hit = json!({"_index": index, "_id": id, "_score": score});
            if include_source {
                hit["_source"] = Json::Object(source);
            }
            if !highlight.is_empty() {
                hit["highlight"] = Json::Object(highlight);
            }
            // "explain": true — per-hit BM25 breakdown from the index (a
            // full-text notion; skipped for kNN/hybrid retrieval).
            if !is_vector && body["explain"].as_bool() == Some(true) {
                if let Some(id_str) = id.as_str() {
                    let mut keys = vec![Value::String(id_str.to_string())];
                    if let Ok(n) = id_str.parse::<i64>() {
                        keys.push(Value::Int(n));
                    }
                    for pk_value in keys {
                        if let Some(explanation) = ctx
                            .backend
                            .search_explain(index, &filter, &pk_value)
                            .map_err(|e| format!("explain: {e}"))?
                        {
                            hit["_explanation"] = serde_json::from_str(&explanation)
                                .unwrap_or_else(|_| json!(explanation));
                            break;
                        }
                    }
                }
            }
            hits.push(hit);
        }
        // kNN/hybrid total = retrieved ranked hits (≤ k).
        if is_vector {
            total = hits.len() as i64;
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

/// `_source`: `false` | `true` | `"field"` | `["f1", "f2*"]` |
/// `{"includes": [...], "excludes": [...]}` → (include at all, includes,
/// excludes). Patterns support a trailing `*` (ES-style prefix glob).
fn source_spec(body: &Json) -> (bool, Vec<String>, Vec<String>) {
    fn list(v: &Json) -> Vec<String> {
        match v {
            Json::String(s) => vec![s.clone()],
            Json::Array(a) => a
                .iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect(),
            _ => Vec::new(),
        }
    }
    match body.get("_source") {
        None => (true, Vec::new(), Vec::new()),
        Some(Json::Bool(b)) => (*b, Vec::new(), Vec::new()),
        Some(Json::Object(o)) => (
            true,
            o.get("includes").map(list).unwrap_or_default(),
            o.get("excludes").map(list).unwrap_or_default(),
        ),
        Some(v) => (true, list(v), Vec::new()),
    }
}

fn source_allows(name: &str, includes: &[String], excludes: &[String]) -> bool {
    let matches = |pat: &String| match pat.strip_suffix('*') {
        Some(prefix) => name.starts_with(prefix),
        None => pat == name,
    };
    if excludes.iter().any(matches) {
        return false;
    }
    includes.is_empty() || includes.iter().any(matches)
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
                | "boosted"
                | "match_cross"
                | "match_best"
        ),
        Expr::Unary { expr, .. } => expr_uses_search(expr),
        Expr::Binary { left, right, .. } => expr_uses_search(left) || expr_uses_search(right),
        _ => false,
    }
}

fn parse_sort(sort: &Json) -> Result<Vec<(String, bool)>, String> {
    let entries: Vec<&Json> = match sort {
        Json::Array(items) => items.iter().collect(),
        one => vec![one],
    };
    entries
        .into_iter()
        .map(|entry| match entry {
            Json::String(col) => Ok((col.clone(), col == "_score")),
            Json::Object(o) => {
                let (col, spec) = o.iter().next().ok_or("empty sort entry")?;
                let desc = spec["order"].as_str().unwrap_or("asc") == "desc";
                Ok((col.clone(), desc))
            }
            _ => Err("unsupported sort specification".to_string()),
        })
        .collect()
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

/// A `top_hits` sub-aggregation request: `(name, size, include_source)`.
type TopHitsSpec = (String, u64, bool);

type ParsedSubAggs = (Vec<(String, SelectItem)>, Vec<TopHitsSpec>);

fn parse_metrics(spec: &Json) -> Result<ParsedSubAggs, String> {
    let mut out = Vec::new();
    let mut top_hits = Vec::new();
    if let Some(subs) = spec["aggs"].as_object().or_else(|| spec["aggregations"].as_object()) {
        for (mname, mspec) in subs {
            let mobj = mspec.as_object().ok_or("sub-agg must be an object")?;
            let (kind, body) = mobj.iter().next().ok_or("empty sub-agg")?;
            if kind == "top_hits" {
                if body.get("sort").is_some() {
                    return Err(
                        "top_hits sort is not supported (hits come back relevance-ordered)"
                            .into(),
                    );
                }
                let size = body["size"].as_u64().unwrap_or(3);
                let include_source =
                    body.get("_source").map(|s| s != &json!(false)).unwrap_or(true);
                top_hits.push((mname.clone(), size, include_source));
                continue;
            }
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
    Ok((out, top_hits))
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
            let (metrics, top_specs) = parse_metrics(spec)?;
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
            // top_hits: one relevance-ordered per-bucket query each (runs
            // only for the retained buckets).
            if !top_specs.is_empty() {
                let interval_ms = if kind == "date_histogram" {
                    Some(parse_interval_ms(
                        body["fixed_interval"].as_str().unwrap_or(""),
                    )?)
                } else {
                    None
                };
                for bucket in &mut buckets {
                    let key = bucket["key"].clone();
                    for (name, size, include_source) in &top_specs {
                        bucket[name.as_str()] = bucket_top_hits(
                            ctx,
                            role,
                            index,
                            filter,
                            field,
                            &key,
                            interval_ms,
                            *size,
                            *include_source,
                        )?;
                    }
                }
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

/// The top documents of one bucket (ES `top_hits`): the base filter ANDed
/// with the bucket's group predicate, relevance-ordered when the query
/// searches, capped at `size`.
#[allow(clippy::too_many_arguments)]
fn bucket_top_hits(
    ctx: &Shared,
    role: &str,
    index: &str,
    filter: &Option<Expr>,
    group_field: &str,
    key: &Json,
    interval_ms: Option<i64>,
    size: u64,
    include_source: bool,
) -> Result<Json, String> {
    // The bucket predicate: field = key (terms), or the histogram's
    // half-open interval [key, key + interval).
    let bucket_pred = match interval_ms {
        None => Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column(group_field.to_string())),
            right: Box::new(Expr::Literal(Value::from_json(key.clone()))),
        },
        Some(ms) => {
            let k = key.as_i64().ok_or("histogram bucket key must be numeric")?;
            Expr::Binary {
                op: BinaryOp::And,
                left: Box::new(Expr::Binary {
                    op: BinaryOp::GtEq,
                    left: Box::new(Expr::Column(group_field.to_string())),
                    right: Box::new(Expr::Literal(Value::Int(k))),
                }),
                right: Box::new(Expr::Binary {
                    op: BinaryOp::Lt,
                    left: Box::new(Expr::Column(group_field.to_string())),
                    right: Box::new(Expr::Literal(Value::Int(k + ms))),
                }),
            }
        }
    };
    let mut sel = select_from(index);
    sel.items = vec![SelectItem::Wildcard];
    sel.filter = Some(match filter {
        Some(f) => Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(f.clone()),
            right: Box::new(bucket_pred),
        },
        None => bucket_pred,
    });
    sel.limit = Some(size);
    let scored = filter.as_ref().map(expr_uses_search).unwrap_or(false);
    if scored {
        sel.order_by = vec![OrderKey {
            expr: func("score", vec![]),
            descending: true,
        }];
    }
    let pk = ctx
        .backend
        .table_primary_key(index)
        .and_then(|pk| pk.first().cloned())
        .unwrap_or_default();
    let (columns, rows) = rows_of(run(ctx, role, Statement::Select(sel), "es:top_hits")?)?;
    let mut hits = Vec::with_capacity(rows.len());
    for row in rows {
        let mut source = Map::new();
        let mut id = Json::Null;
        let mut score = Json::Null;
        for (col, val) in columns.iter().zip(row) {
            if col == "_score" {
                score = val.to_json();
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
        let mut hit = json!({"_index": index, "_id": id, "_score": score});
        if include_source {
            hit["_source"] = Json::Object(source);
        }
        hits.push(hit);
    }
    Ok(json!({"hits": {"total": {"value": hits.len(), "relation": "eq"}, "hits": hits}}))
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

// ---- GET /{index}/_doc/{id} ----

fn get_doc(ctx: &Shared, role: &str, index: &str, id: &str) -> Result<(u16, Json), String> {
    let pk = match ctx.backend.table_primary_key(index) {
        Some(pk) if pk.len() == 1 => pk[0].clone(),
        Some(_) => return Err("composite primary keys cannot map to _id".into()),
        None => return Ok(es_error(404, &format!("no such index (table) '{index}'"))),
    };
    // `_id` is a string on the wire; a table with a numeric key stores it
    // as a number — try the string form first, then the numeric.
    let mut candidates = vec![Value::String(id.to_string())];
    if let Ok(n) = id.parse::<i64>() {
        candidates.push(Value::Int(n));
    }
    for key in candidates {
        let mut sel = select_from(index);
        sel.items = vec![SelectItem::Wildcard];
        sel.filter = Some(Expr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(Expr::Column(pk.clone())),
            right: Box::new(Expr::Literal(key)),
        });
        sel.limit = Some(1);
        let (columns, rows) = rows_of(run(ctx, role, Statement::Select(sel), "es:_doc")?)?;
        if let Some(row) = rows.into_iter().next() {
            let mut source = Map::new();
            for (col, val) in columns.iter().zip(row) {
                source.insert(col.clone(), val.to_json());
            }
            return Ok((
                200,
                json!({
                    "_index": index,
                    "_id": id,
                    "_version": 1,
                    "found": true,
                    "_source": Json::Object(source),
                }),
            ));
        }
    }
    Ok((404, json!({"_index": index, "_id": id, "found": false})))
}

// ---- _bulk ----

/// ES-style dynamic mapping: the first `_bulk` write to an unknown index
/// creates the table (primary key `id`) and a search index derived from the
/// first document — strings → `text`, integer numbers → `long`, floats →
/// `double`, bools → `bool`; null/array/object fields are stored but not
/// search-indexed. Field names that are not plain identifiers are skipped
/// (stored, not indexed). Idempotent (`IF NOT EXISTS`) so concurrent bulks
/// cannot race each other into an error.
fn auto_create(
    ctx: &Shared,
    role: &str,
    index: &str,
    doc: &Map<String, Json>,
) -> Result<(), String> {
    let ident_ok =
        |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !ident_ok(index) {
        return Err(format!(
            "cannot auto-create index '{index}': the name must be alphanumeric/underscore"
        ));
    }
    let ddl = format!("CREATE TABLE IF NOT EXISTS {index} (PRIMARY KEY (id))");
    let stmt = skaidb_sql::parse(&ddl).map_err(|e| format!("auto-create: {e}"))?;
    if let Response::Error(e) = run(ctx, role, stmt, "es:_bulk#auto-create")? {
        return Err(format!("auto-create table: {e}"));
    }
    let mut cols = Vec::new();
    let mut opts = Vec::new();
    for (field, value) in doc {
        if field == "id" || !ident_ok(field) {
            continue;
        }
        // Quoted, so field names that collide with SQL keywords still work.
        match value {
            Json::String(_) => cols.push(format!("\"{field}\"")),
            Json::Number(n) if n.is_i64() || n.is_u64() => {
                cols.push(format!("\"{field}\""));
                opts.push(format!("\"{field}\".type = 'long'"));
            }
            Json::Number(_) => {
                cols.push(format!("\"{field}\""));
                opts.push(format!("\"{field}\".type = 'double'"));
            }
            Json::Bool(_) => {
                cols.push(format!("\"{field}\""));
                opts.push(format!("\"{field}\".type = 'bool'"));
            }
            _ => {}
        }
    }
    if cols.is_empty() {
        return Ok(()); // nothing searchable in the first doc; table alone
    }
    let with = if opts.is_empty() {
        String::new()
    } else {
        format!(" WITH ({})", opts.join(", "))
    };
    let ddl = format!(
        "CREATE SEARCH INDEX IF NOT EXISTS {index}_fts ON {index} ({}){with}",
        cols.join(", ")
    );
    let stmt = skaidb_sql::parse(&ddl).map_err(|e| format!("auto-create: {e}"))?;
    if let Response::Error(e) = run(ctx, role, stmt, "es:_bulk#auto-create")? {
        return Err(format!("auto-create search index: {e}"));
    }
    Ok(())
}

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
            // Unknown index + a document to write → dynamic mapping.
            None => match action.source.as_ref().and_then(|s| s.as_object()) {
                Some(doc) => match auto_create(ctx, role, &action.index, doc) {
                    Ok(()) => "id".to_string(),
                    Err(e) => {
                        errors = true;
                        items.push(bulk_item(action.verb, &action.index, &action.id, 400,
                            Some(&e)));
                        continue;
                    }
                },
                None => {
                    errors = true;
                    items.push(bulk_item(action.verb, &action.index, &action.id, 404,
                        Some("no such index (table)")));
                    continue;
                }
            },
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // A top-level `knn` block with a raw query vector maps to a NEAREST clause
    // over an array literal, with the knn filter as the WHERE and no RRF.
    #[test]
    fn knn_vector_maps_to_nearest() {
        let body = json!({
            "knn": {
                "field": "embedding",
                "query_vector": [0.1, 0.2, 0.3],
                "k": 5,
                "num_candidates": 100,
                "filter": { "term": { "cat": "news" } }
            }
        });
        let spec = parse_vector(&body).unwrap().expect("vector spec");
        assert!(spec.rrf.is_none());
        assert_eq!(spec.nearest.path, "embedding");
        assert!(matches!(spec.nearest.k, Expr::Literal(Value::Int(5))));
        match &spec.nearest.query {
            Expr::Literal(Value::Array(v)) => assert_eq!(v.len(), 3),
            other => panic!("expected array literal vector, got {other:?}"),
        }
        // knn.filter → WHERE cat = 'news'
        match spec.filter {
            Some(Expr::Binary { op: BinaryOp::Eq, .. }) => {}
            other => panic!("expected an equality filter, got {other:?}"),
        }
    }

    // A semantic knn (query_vector_builder text) maps to a STRING NEAREST query,
    // which the engine auto-embeds against a managed EMBED index.
    #[test]
    fn knn_semantic_text_maps_to_string_nearest() {
        let body = json!({
            "knn": {
                "field": "body",
                "k": 3,
                "query_vector_builder": {
                    "text_embedding": { "model_id": "m", "model_text": "natural language query" }
                }
            }
        });
        let spec = parse_vector(&body).unwrap().expect("vector spec");
        match &spec.nearest.query {
            Expr::Literal(Value::String(s)) => assert_eq!(s, "natural language query"),
            other => panic!("expected string query, got {other:?}"),
        }
        assert!(spec.filter.is_none());
    }

    // A `retriever { rrf { retrievers: [standard, knn] } }` block maps to
    // NEAREST + a WHERE search predicate + RANK BY RRF with the given constant.
    #[test]
    fn retriever_rrf_maps_to_hybrid() {
        let body = json!({
            "retriever": {
                "rrf": {
                    "rank_constant": 20,
                    "retrievers": [
                        { "standard": { "query": { "match": { "body": "quick fox" } } } },
                        { "knn": { "field": "embedding", "query_vector": [0.1, 0.2], "k": 50 } }
                    ]
                }
            }
        });
        let spec = parse_vector(&body).unwrap().expect("vector spec");
        assert_eq!(spec.rrf, Some(Rrf { constant: 20 }));
        assert_eq!(spec.nearest.path, "embedding");
        // standard leg → a MATCH() search predicate as the WHERE
        match spec.filter {
            Some(Expr::Func { ref name, .. }) if name == "match" => {}
            other => panic!("expected a match() predicate, got {other:?}"),
        }
    }

    // Default RRF constant when rank_constant is omitted.
    #[test]
    fn retriever_rrf_default_constant() {
        let body = json!({
            "retriever": { "rrf": { "retrievers": [
                { "standard": { "query": { "match": { "body": "x" } } } },
                { "knn": { "field": "e", "query_vector": [1.0], "k": 10 } }
            ] } }
        });
        let spec = parse_vector(&body).unwrap().unwrap();
        assert_eq!(spec.rrf, Some(Rrf { constant: DEFAULT_RRF_CONSTANT }));
    }

    // ES highlight options map to HIGHLIGHT() positional args, with per-field
    // values overriding block-level ones and the minimal form for a bare {}.
    #[test]
    fn highlight_args_map_es_options() {
        let block = json!({});
        // Bare `{}` → HIGHLIGHT(col) only.
        assert_eq!(
            highlight_args("body", &json!({}), &block),
            vec![Expr::Column("body".into())]
        );
        // fragment_size → HIGHLIGHT(col, size).
        assert_eq!(
            highlight_args("body", &json!({ "fragment_size": 80 }), &block),
            vec![Expr::Column("body".into()), Expr::Literal(Value::Int(80))]
        );
        // pre/post tags (arrays, first element) → HIGHLIGHT(col, size, pre, post).
        let args = highlight_args(
            "body",
            &json!({ "pre_tags": ["<em>"], "post_tags": ["</em>"] }),
            &block,
        );
        assert_eq!(
            args,
            vec![
                Expr::Column("body".into()),
                Expr::Literal(Value::Int(150)),
                Expr::Literal(Value::String("<em>".into())),
                Expr::Literal(Value::String("</em>".into())),
            ]
        );
        // no_match_size → the trailing arg (tags default to <b>/</b>).
        let args = highlight_args("body", &json!({ "no_match_size": 20 }), &block);
        assert_eq!(args.len(), 5);
        assert_eq!(args[4], Expr::Literal(Value::Int(20)));
        assert_eq!(args[2], Expr::Literal(Value::String("<b>".into())));
        // Block-level fallback when the field spec omits an option.
        let args = highlight_args(
            "body",
            &json!({}),
            &json!({ "pre_tags": ["<x>"], "post_tags": ["</x>"] }),
        );
        assert_eq!(args[2], Expr::Literal(Value::String("<x>".into())));
    }

    // Error shapes: missing field, no query, an rrf with no knn leg, and a
    // body with neither knn nor retriever.
    #[test]
    fn vector_parse_errors_and_absence() {
        assert!(parse_vector(&json!({ "knn": { "query_vector": [1.0] } })).is_err()); // no field
        assert!(parse_vector(&json!({ "knn": { "field": "e" } })).is_err()); // no vector/text
        assert!(parse_vector(&json!({
            "retriever": { "rrf": { "retrievers": [
                { "standard": { "query": { "match": { "b": "x" } } } }
            ] } }
        }))
        .is_err()); // rrf without a knn leg
        assert!(parse_vector(&json!({ "retriever": { "unknown": {} } })).is_err());
        // No knn / retriever at all → not a vector query.
        assert!(parse_vector(&json!({ "query": { "match_all": {} } })).unwrap().is_none());
    }
}
