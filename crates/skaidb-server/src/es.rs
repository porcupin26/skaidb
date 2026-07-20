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
    AggArg, AggFunc, BinaryOp, Expr, Nearest, OrderKey, Rerank, Rrf, Select, SelectItem,
    Statement, UnaryOp, DEFAULT_RRF_CONSTANT,
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
        rerank: None,
        after: None,
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
        // Geo queries map onto the SQL geo predicates (docs/GEO.md), which the
        // geo-index pruner recognizes when they land as top-level conjuncts —
        // exactly where a `bool.filter`/`must` geo clause ends up.
        "geo_distance" => {
            let obj = body.as_object().ok_or("geo_distance expects an object")?;
            let meters = geo_distance_meters(
                obj.get("distance").ok_or("geo_distance needs `distance`")?,
            )?;
            let (col, point) = geo_field(obj, "geo_distance")?;
            let (lat, lon) = geo_point(point)
                .map_err(|e| format!("geo_distance.{col}: {e}"))?;
            Ok(Some(Expr::Binary {
                op: BinaryOp::LtEq,
                left: Box::new(func(
                    "geo_distance",
                    vec![
                        Expr::Column(col),
                        Expr::Literal(Value::Float(lat)),
                        Expr::Literal(Value::Float(lon)),
                    ],
                )),
                right: Box::new(Expr::Literal(Value::Float(meters))),
            }))
        }
        "geo_bounding_box" => {
            let obj = body
                .as_object()
                .ok_or("geo_bounding_box expects an object")?;
            let (col, spec) = geo_field(obj, "geo_bounding_box")?;
            let bo = spec
                .as_object()
                .ok_or("geo_bounding_box field spec must be an object")?;
            let corner = |name: &str| -> Result<Option<(f64, f64)>, String> {
                bo.get(name)
                    .map(|v| geo_point(v).map_err(|e| format!("geo_bounding_box.{name}: {e}")))
                    .transpose()
            };
            // ES gives (top_left, bottom_right) or (top_right, bottom_left)
            // corners, or flat top/left/bottom/right edges; skaidb's
            // `geo_bbox` takes (min_lat, min_lon, max_lat, max_lon). A
            // left > right box crosses the antimeridian — passed through:
            // the exact predicate handles the wrap (the geo index leaves it
            // to the scan).
            let edges = if let (Some(tl), Some(br)) = (corner("top_left")?, corner("bottom_right")?)
            {
                (br.0, tl.1, tl.0, br.1)
            } else if let (Some(tr), Some(bl)) = (corner("top_right")?, corner("bottom_left")?) {
                (bl.0, bl.1, tr.0, tr.1)
            } else {
                let edge = |name: &str| -> Result<f64, String> {
                    bo.get(name)
                        .and_then(|v| v.as_f64())
                        .ok_or_else(|| format!("geo_bounding_box needs corners or `{name}`"))
                };
                (edge("bottom")?, edge("left")?, edge("top")?, edge("right")?)
            };
            Ok(Some(func(
                "geo_bbox",
                vec![
                    Expr::Column(col),
                    Expr::Literal(Value::Float(edges.0)),
                    Expr::Literal(Value::Float(edges.1)),
                    Expr::Literal(Value::Float(edges.2)),
                    Expr::Literal(Value::Float(edges.3)),
                ],
            )))
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

/// The `(field, spec)` entry of a geo query body — the one key that is not a
/// per-query option.
fn geo_field<'j>(
    obj: &'j Map<String, Json>,
    what: &str,
) -> Result<(String, &'j Json), String> {
    const OPTS: [&str; 6] = [
        "distance",
        "distance_type",
        "type",
        "validation_method",
        "ignore_unmapped",
        "_name",
    ];
    obj.iter()
        .find(|(k, _)| !OPTS.contains(&k.as_str()) && k.as_str() != "boost")
        .map(|(k, v)| (k.clone(), v))
        .ok_or_else(|| format!("{what} needs a field entry"))
}

/// An ES distance value in metres: a bare number, or a string with an
/// optional unit suffix (`"5km"`, `"500m"`, `"1mi"`; no unit = metres).
fn geo_distance_meters(v: &Json) -> Result<f64, String> {
    let meters = if let Some(n) = v.as_f64() {
        n
    } else {
        let s = v
            .as_str()
            .ok_or("`distance` must be a number (metres) or a string like \"5km\"")?
            .trim();
        let unit_at = s
            .find(|c: char| c.is_ascii_alphabetic())
            .unwrap_or(s.len());
        let (num, unit) = s.split_at(unit_at);
        let n: f64 = num
            .trim()
            .parse()
            .map_err(|_| format!("bad distance '{s}'"))?;
        let factor = match unit.trim().to_ascii_lowercase().as_str() {
            "" | "m" | "meters" | "meter" => 1.0,
            "km" | "kilometers" | "kilometer" => 1000.0,
            "mi" | "miles" | "mile" => 1609.344,
            "yd" | "yards" | "yard" => 0.9144,
            "ft" | "feet" | "foot" => 0.3048,
            "in" | "inch" | "inches" => 0.0254,
            "cm" | "centimeters" | "centimeter" => 0.01,
            "mm" | "millimeters" | "millimeter" => 0.001,
            // ES `NM` (nautical miles) arrives lowercased here.
            "nm" | "nmi" | "nauticalmiles" | "nauticalmile" => 1852.0,
            other => return Err(format!("unsupported distance unit '{other}'")),
        };
        n * factor
    };
    if !(meters.is_finite() && meters >= 0.0) {
        return Err("`distance` must be a non-negative finite number".into());
    }
    Ok(meters)
}

/// An ES point as `(lat, lon)`: a `{lat, lon}` object, a **[lon, lat]** array
/// (GeoJSON order), a `"lat,lon"` string, or WKT `POINT (lon lat)`. Geohash
/// strings are not supported.
fn geo_point(v: &Json) -> Result<(f64, f64), String> {
    match v {
        Json::Object(o) => {
            let coord = |k: &str| -> Result<f64, String> {
                o.get(k)
                    .and_then(|x| x.as_f64())
                    .ok_or_else(|| format!("point object needs numeric `{k}`"))
            };
            Ok((coord("lat")?, coord("lon")?))
        }
        Json::Array(a) => match a.as_slice() {
            [lon, lat] => {
                let lon = lon.as_f64().ok_or("point array must be [lon, lat] numbers")?;
                let lat = lat.as_f64().ok_or("point array must be [lon, lat] numbers")?;
                Ok((lat, lon))
            }
            _ => Err("point array must be [lon, lat]".into()),
        },
        Json::String(s) => {
            let s = s.trim();
            // WKT: `POINT (lon lat)`.
            if let Some(rest) = s
                .strip_prefix("POINT")
                .or_else(|| s.strip_prefix("point"))
            {
                let inner = rest
                    .trim()
                    .strip_prefix('(')
                    .and_then(|r| r.strip_suffix(')'))
                    .ok_or("bad WKT point")?;
                let mut it = inner.split_whitespace();
                let lon: f64 = it
                    .next()
                    .and_then(|x| x.parse().ok())
                    .ok_or("bad WKT point")?;
                let lat: f64 = it
                    .next()
                    .and_then(|x| x.parse().ok())
                    .ok_or("bad WKT point")?;
                return Ok((lat, lon));
            }
            // `"lat,lon"`.
            let (lat, lon) = s.split_once(',').ok_or(
                "point string must be \"lat,lon\" or WKT POINT (geohash is not supported)",
            )?;
            let lat: f64 = lat.trim().parse().map_err(|_| "bad point latitude")?;
            let lon: f64 = lon.trim().parse().map_err(|_| "bad point longitude")?;
            Ok((lat, lon))
        }
        _ => Err("unsupported point shape".into()),
    }
}

/// A parsed retriever request: an optional `NEAREST` clause, an optional
/// WHERE filter, (for `retriever { rrf }`) the fusion constant, and (for
/// `retriever { text_similarity_reranker }`) the `RERANK` clause. `nearest`
/// is `None` only for a reranked `standard` retriever (search-only).
struct VectorSpec {
    nearest: Option<Nearest>,
    filter: Option<Expr>,
    rrf: Option<Rrf>,
    rerank: Option<Rerank>,
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

/// Parse the vector/retriever part of a `_search` body: a top-level `knn`
/// block (pure/semantic kNN), or a `retriever` block — `rrf { retrievers:
/// [standard, knn] }` (hybrid → `NEAREST … WHERE <search> RANK BY RRF`) or
/// `text_similarity_reranker { retriever, … }` (→ `RERANK`). `None` when the
/// body has neither.
fn parse_vector(body: &Json) -> Result<Option<VectorSpec>, String> {
    if let Some(knn) = body.get("knn") {
        let (nearest, filter) = knn_nearest(knn)?;
        return Ok(Some(VectorSpec {
            nearest: Some(nearest),
            filter,
            rrf: None,
            rerank: None,
        }));
    }
    let Some(retriever) = body.get("retriever") else {
        return Ok(None);
    };
    retriever_spec(retriever).map(Some)
}

/// One `retriever` object: `rrf`, `text_similarity_reranker`, `knn`, or (only
/// meaningful nested under a reranker) `standard`.
fn retriever_spec(retriever: &Json) -> Result<VectorSpec, String> {
    if let Some(rrf) = retriever.get("rrf") {
        return rrf_spec(rrf);
    }
    if let Some(tsr) = retriever.get("text_similarity_reranker") {
        let inner = tsr
            .get("retriever")
            .ok_or("text_similarity_reranker needs a `retriever`")?;
        let mut spec = if let Some(standard) = inner.get("standard") {
            let q = match standard.get("query") {
                Some(q) => query_expr(q)?,
                None => None,
            };
            VectorSpec {
                nearest: None,
                filter: q,
                rrf: None,
                rerank: None,
            }
        } else {
            retriever_spec(inner)?
        };
        if spec.rerank.is_some() {
            return Err("text_similarity_reranker cannot nest another reranker".into());
        }
        let top = match tsr.get("rank_window_size") {
            // ES's rank_window_size default.
            None => 10,
            Some(v) => match v.as_u64() {
                Some(n) if n >= 1 => n,
                _ => return Err("rank_window_size must be a positive integer".into()),
            },
        };
        spec.rerank = Some(Rerank {
            column: tsr.get("field").and_then(|v| v.as_str()).map(String::from),
            model: tsr
                .get("inference_id")
                .and_then(|v| v.as_str())
                .map(String::from),
            query: tsr
                .get("inference_text")
                .and_then(|v| v.as_str())
                .map(String::from),
            top,
        });
        return Ok(spec);
    }
    if let Some(knn) = retriever.get("knn") {
        let (nearest, filter) = knn_nearest(knn)?;
        return Ok(VectorSpec {
            nearest: Some(nearest),
            filter,
            rrf: None,
            rerank: None,
        });
    }
    Err("only the `rrf`, `text_similarity_reranker`, and `knn` retrievers are supported".into())
}

/// `retriever { rrf }` → hybrid `NEAREST … RANK BY RRF`.
fn rrf_spec(rrf: &Json) -> Result<VectorSpec, String> {
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
    Ok(VectorSpec {
        nearest: Some(nearest),
        filter,
        rrf: Some(Rrf { constant }),
        rerank: None,
    })
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

    let pk = ctx
        .backend
        .table_primary_key(index)
        .ok_or_else(|| format!("no such index (table) '{index}'"))?;
    let pk = pk.first().cloned().unwrap_or_default();

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
        let mut sort_pairs: Option<Vec<(String, bool)>> = None;
        if let Some(v) = vector {
            // kNN / hybrid / reranked: NEAREST (+ RANK BY RRF) and RERANK
            // already return rows ranked (nearest-first, fused rrf_score
            // desc, or reranker order), so no ORDER BY — the engine rejects
            // ORDER BY alongside them. The knn/standard-leg filter is the
            // WHERE.
            sel.nearest = v.nearest.map(Box::new);
            sel.rrf = v.rrf;
            sel.rerank = v.rerank;
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
                    let pairs = parse_sort(sort)?;
                    // Ranked/sorted search pages already tie-break by primary
                    // key ascending, so a trailing `_id asc` sort entry is
                    // implicit — drop it (it also keeps `_score` sorts legal:
                    // the engine wants score() as the only sort key). `_id`
                    // elsewhere maps to the primary-key column.
                    let mut effective: &[(String, bool)] = &pairs;
                    if uses_search {
                        if let [head @ .., (last, false)] = effective {
                            if last == "_id" || last == &pk {
                                effective = head;
                            }
                        }
                    }
                    sel.order_by = effective
                        .iter()
                        .map(|(col, desc)| OrderKey {
                            expr: if col == "_score" {
                                func("score", vec![])
                            } else if col == "_id" {
                                Expr::Column(pk.clone())
                            } else {
                                Expr::Column(col.clone())
                            },
                            descending: *desc,
                        })
                        .collect();
                    sort_pairs = Some(pairs);
                }
            }
        }
        sel.limit = Some(size);
        sel.offset = (from > 0).then_some(from);
        if let Some(sa) = body.get("search_after") {
            if is_vector {
                return Err("search_after is not supported with knn/retriever queries".into());
            }
            if from > 0 {
                return Err("search_after cannot be combined with from".into());
            }
            apply_search_after(&mut sel, sort_pairs.as_deref().unwrap_or(&[]), sa, &pk)?;
        }
        let (include_source, includes, excludes) = source_spec(&body);
        let (columns, rows) = rows_of(run(ctx, role, Statement::Select(sel), "es:_search")?)?;
        for row in rows {
            let mut source = Map::new();
            let mut id = Json::Null;
            let mut score = Json::Null;
            let mut highlight = Map::new();
            let mut pk_raw = Json::Null;
            let mut sort_vals = Map::new();
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
                        pk_raw = val.to_json();
                        id = match &val {
                            Value::String(s) => Json::String(s.clone()),
                            other => Json::String(other.to_json().to_string()),
                        };
                    }
                    if sort_pairs
                        .as_ref()
                        .is_some_and(|ps| ps.iter().any(|(c, _)| c == col))
                    {
                        sort_vals.insert(col.clone(), val.to_json());
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
            // ES parity for sorted queries: each hit carries its sort values,
            // so clients can echo the last hit's `sort` array back as the next
            // page's `search_after` (JSON types round-trip exactly).
            if let Some(pairs) = &sort_pairs {
                let vals: Vec<Json> = pairs
                    .iter()
                    .map(|(c, _)| {
                        if c == "_score" {
                            score.clone()
                        } else if c == "_id" || c == &pk {
                            pk_raw.clone()
                        } else {
                            sort_vals.get(c).cloned().unwrap_or(Json::Null)
                        }
                    })
                    .collect();
                hit["sort"] = json!(vals);
            }
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

/// Map ES `search_after` onto the SQL `AFTER (<sort value>, <pk value>)`
/// keyset cursor. Accepted sort shapes (the `_id`/primary-key tie-break must
/// be last and ascending, exactly as ES best practice prescribes):
/// `[<key>, {"_id": "asc"}]` with two cursor values, or `[{"_id": "asc"}]`
/// alone (pages by primary key). Values should be echoed from the previous
/// page's last hit `sort` array, so their JSON types round-trip.
fn apply_search_after(
    sel: &mut Select,
    pairs: &[(String, bool)],
    vals: &Json,
    pk: &str,
) -> Result<(), String> {
    let vals = vals.as_array().ok_or("search_after must be an array")?;
    if pairs.is_empty() {
        return Err("search_after requires an explicit sort".into());
    }
    if vals.len() != pairs.len() {
        return Err("search_after must carry one value per sort key".into());
    }
    let lit = |v: &Json| Expr::Literal(Value::from_json(v.clone()));
    let is_id = |c: &str| c == "_id" || c == pk;
    match pairs {
        [(_, _), (id, false)] if is_id(id) => {
            // The SQL cursor's primary-key tie-break is implicit — drop the
            // _id sort key and pass its value as the cursor's second half.
            sel.order_by.truncate(1);
            sel.after = Some(vec![lit(&vals[0]), lit(&vals[1])]);
            Ok(())
        }
        [(id, false)] if is_id(id) => {
            sel.order_by = vec![OrderKey {
                expr: Expr::Column(pk.to_string()),
                descending: false,
            }];
            sel.after = Some(vec![lit(&vals[0]), lit(&vals[0])]);
            Ok(())
        }
        _ => Err(
            "search_after needs sort [<key>, {\"_id\": \"asc\"}] — the _id (primary key) \
             tie-break must be the last sort key and ascending"
                .into(),
        ),
    }
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

/// One named metric sub-agg lowered to SQL select items: a single-value
/// metric (`{"value": …}`) or a `percentiles` fan-out (`{"values": {…}}`,
/// one item per requested percent).
enum MetricSql {
    Single(SelectItem),
    Percentiles(Vec<(f64, SelectItem)>),
}

impl MetricSql {
    fn items(&self) -> Vec<&SelectItem> {
        match self {
            MetricSql::Single(it) => vec![it],
            MetricSql::Percentiles(ps) => ps.iter().map(|(_, it)| it).collect(),
        }
    }

    /// Shape this metric's JSON from `row` starting at column `col`,
    /// returning the value and how many columns were consumed.
    fn shape(&self, row: &[Value], col: usize) -> (Json, usize) {
        match self {
            MetricSql::Single(_) => (json!({"value": row[col].to_json()}), 1),
            MetricSql::Percentiles(ps) => {
                let mut values = Map::new();
                for (i, (p, _)) in ps.iter().enumerate() {
                    values.insert(percent_key(*p), row[col + i].to_json());
                }
                (json!({"values": values}), ps.len())
            }
        }
    }
}

/// ES formats percentile keys as `"50.0"` / `"99.9"`.
fn percent_key(p: f64) -> String {
    if p.fract() == 0.0 {
        format!("{p:.1}")
    } else {
        format!("{p}")
    }
}

/// `percentiles` percents (default = ES's) → one `PERCENTILE` item each.
fn percentile_items(field: &str, body: &Json) -> Result<Vec<(f64, SelectItem)>, String> {
    let percents: Vec<f64> = match body.get("percents") {
        None => vec![1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0],
        Some(v) => v
            .as_array()
            .ok_or("percentiles.percents must be an array")?
            .iter()
            .map(|p| p.as_f64().ok_or("percents must be numbers"))
            .collect::<Result<_, _>>()?,
    };
    percents
        .into_iter()
        .map(|p| {
            if !(p > 0.0 && p <= 100.0) {
                return Err(format!("percentile {p} out of range (0, 100]"));
            }
            Ok((
                p,
                SelectItem::Expr {
                    expr: Expr::Aggregate {
                        // The SQL fraction is stored in basis points.
                        func: AggFunc::Percentile((p * 100.0).round() as u16),
                        arg: AggArg::Expr(Box::new(Expr::Column(field.to_string()))),
                    },
                    alias: None,
                },
            ))
        })
        .collect()
}

type ParsedSubAggs = (Vec<(String, MetricSql)>, Vec<TopHitsSpec>);

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
                "percentiles" => {
                    out.push((
                        mname.clone(),
                        MetricSql::Percentiles(percentile_items(field, body)?),
                    ));
                    continue;
                }
                "cardinality" => {
                    out.push((
                        mname.clone(),
                        MetricSql::Single(SelectItem::Expr {
                            expr: Expr::Aggregate {
                                func: AggFunc::Count,
                                arg: AggArg::Distinct(Box::new(Expr::Column(field.to_string()))),
                            },
                            alias: None,
                        }),
                    ));
                    continue;
                }
                other => return Err(format!("unsupported sub-aggregation '{other}'")),
            };
            out.push((mname.clone(), MetricSql::Single(metric_item(f, field))));
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
            for (_, m) in &metrics {
                for item in m.items() {
                    sel.items.push(item.clone());
                }
            }
            let (_, rows) = rows_of(run(ctx, role, Statement::Select(sel), "es:aggs")?)?;
            let mut buckets: Vec<Json> = rows
                .into_iter()
                .map(|row| {
                    let mut bucket = json!({
                        "key": row[0].to_json(),
                        "doc_count": row[1].to_json(),
                    });
                    let mut col = 2;
                    for (mname, m) in &metrics {
                        let (value, used) = m.shape(&row, col);
                        bucket[mname.as_str()] = value;
                        col += used;
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
        // Paginated multi-source buckets (ES `composite`): sources become one
        // multi-column GROUP BY; buckets sort ascending by the composite key
        // tuple (the order-preserving key encoding compares tuples exactly)
        // and page via `after` — echo the returned `after_key` to continue.
        "composite" => {
            let sources = body["sources"]
                .as_array()
                .ok_or("composite needs sources: [...]")?;
            let mut names: Vec<String> = Vec::new();
            let mut exprs: Vec<Expr> = Vec::new();
            for src in sources {
                let (sname, sspec) = src
                    .as_object()
                    .and_then(|o| o.iter().next())
                    .ok_or("empty composite source")?;
                let (skind, sbody) = sspec
                    .as_object()
                    .and_then(|o| o.iter().next())
                    .ok_or("empty composite source")?;
                if sbody["order"].as_str().unwrap_or("asc") != "asc" {
                    return Err("composite sources support ascending order only".into());
                }
                let field = sbody["field"].as_str().ok_or("composite source needs field")?;
                exprs.push(match skind.as_str() {
                    "terms" => Expr::Column(field.to_string()),
                    "date_histogram" => {
                        let interval = sbody["fixed_interval"]
                            .as_str()
                            .ok_or("composite date_histogram needs fixed_interval")?;
                        let ms = parse_interval_ms(interval)?;
                        func(
                            "time_bucket",
                            vec![Expr::Literal(Value::Int(ms)), Expr::Column(field.to_string())],
                        )
                    }
                    other => {
                        return Err(format!(
                            "composite source '{other}' is not supported (terms, date_histogram)"
                        ))
                    }
                });
                names.push(sname.clone());
            }
            if names.is_empty() {
                return Err("composite needs at least one source".into());
            }
            let (metrics, top_specs) = parse_metrics(spec)?;
            if !top_specs.is_empty() {
                return Err("top_hits under composite is not supported".into());
            }
            sel.group_by = exprs.clone();
            sel.items = exprs
                .into_iter()
                .map(|expr| SelectItem::Expr { expr, alias: None })
                .collect();
            sel.items.push(SelectItem::Expr {
                expr: Expr::Aggregate { func: AggFunc::Count, arg: AggArg::Star },
                alias: None,
            });
            for (_, m) in &metrics {
                for item in m.items() {
                    sel.items.push(item.clone());
                }
            }
            let (_, mut rows) = rows_of(run(ctx, role, Statement::Select(sel), "es:aggs")?)?;
            let n = names.len();
            let tuple_key = |row: &[Value]| Value::Array(row[..n].to_vec()).encode_key();
            rows.sort_by_key(|row| tuple_key(row));
            if let Some(after) = body.get("after") {
                let ao = after.as_object().ok_or("composite.after must be an object")?;
                // Values should be echoed from the previous page's after_key,
                // so JSON types round-trip exactly.
                let vals: Vec<Value> = names
                    .iter()
                    .map(|nm| ao.get(nm).cloned().map(Value::from_json).unwrap_or(Value::Null))
                    .collect();
                let after_key = Value::Array(vals).encode_key();
                rows.retain(|row| tuple_key(row) > after_key);
            }
            let size = body["size"].as_u64().unwrap_or(10) as usize;
            rows.truncate(size);
            let buckets: Vec<Json> = rows
                .into_iter()
                .map(|row| {
                    let mut key = Map::new();
                    for (nm, v) in names.iter().zip(&row) {
                        key.insert(nm.clone(), v.to_json());
                    }
                    let mut bucket = json!({
                        "key": Json::Object(key),
                        "doc_count": row[n].to_json(),
                    });
                    let mut col = n + 1;
                    for (mname, m) in &metrics {
                        let (value, used) = m.shape(&row, col);
                        bucket[mname.as_str()] = value;
                        col += used;
                    }
                    bucket
                })
                .collect();
            let mut out = json!({"buckets": buckets});
            if let Some(last) = out["buckets"].as_array().and_then(|b| b.last()) {
                out["after_key"] = last["key"].clone();
            }
            Ok(out)
        }
        "percentiles" => {
            let field = body["field"].as_str().ok_or("metric agg needs field")?;
            let items = percentile_items(field, body)?;
            sel.items = items.iter().map(|(_, it)| it.clone()).collect();
            let (_, rows) = rows_of(run(ctx, role, Statement::Select(sel), "es:aggs")?)?;
            let mut values = Map::new();
            if let Some(row) = rows.first() {
                for (i, (p, _)) in items.iter().enumerate() {
                    values.insert(percent_key(*p), row[i].to_json());
                }
            }
            Ok(json!({"values": values}))
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
        let nearest = spec.nearest.expect("nearest");
        assert_eq!(nearest.path, "embedding");
        assert!(matches!(nearest.k, Expr::Literal(Value::Int(5))));
        match &nearest.query {
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
        match &spec.nearest.expect("nearest").query {
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
        assert_eq!(spec.nearest.expect("nearest").path, "embedding");
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

    #[test]
    fn geo_distance_maps_to_sql_predicate() {
        // Object point + unit string → geo_distance(col, lat, lon) <= metres.
        let e = query_expr(&json!({
            "geo_distance": { "distance": "5km", "loc": { "lat": 40.7, "lon": -74.0 } }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(
            e,
            Expr::Binary {
                op: BinaryOp::LtEq,
                left: Box::new(func(
                    "geo_distance",
                    vec![
                        Expr::Column("loc".into()),
                        Expr::Literal(Value::Float(40.7)),
                        Expr::Literal(Value::Float(-74.0)),
                    ]
                )),
                right: Box::new(Expr::Literal(Value::Float(5000.0))),
            }
        );
        // Array points are GeoJSON [lon, lat]; a bare number is metres.
        let e = query_expr(&json!({
            "geo_distance": { "distance": 250, "loc": [-74.0, 40.7] }
        }))
        .unwrap()
        .unwrap();
        let Expr::Binary { left, right, .. } = e else { panic!() };
        assert_eq!(
            *left,
            func(
                "geo_distance",
                vec![
                    Expr::Column("loc".into()),
                    Expr::Literal(Value::Float(40.7)),
                    Expr::Literal(Value::Float(-74.0)),
                ]
            )
        );
        assert_eq!(*right, Expr::Literal(Value::Float(250.0)));
        // Option keys are skipped when finding the field entry.
        assert!(query_expr(&json!({
            "geo_distance": {
                "distance": "1mi", "distance_type": "arc",
                "loc": "40.7,-74.0"
            }
        }))
        .unwrap()
        .is_some());
        // Errors: missing distance, bad unit, geohash points.
        assert!(query_expr(&json!({ "geo_distance": { "loc": [0.0, 0.0] } })).is_err());
        assert!(query_expr(&json!({
            "geo_distance": { "distance": "3 parsecs", "loc": [0.0, 0.0] }
        }))
        .is_err());
        assert!(query_expr(&json!({
            "geo_distance": { "distance": "1km", "loc": "drm3btev3e86" }
        }))
        .is_err());
    }

    #[test]
    fn geo_bounding_box_maps_to_geo_bbox() {
        // top_left/bottom_right corners → geo_bbox(col, min_lat, min_lon,
        // max_lat, max_lon).
        let e = query_expr(&json!({
            "geo_bounding_box": { "loc": {
                "top_left":     { "lat": 40.9, "lon": -74.3 },
                "bottom_right": { "lat": 40.4, "lon": -73.7 }
            } }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(
            e,
            func(
                "geo_bbox",
                vec![
                    Expr::Column("loc".into()),
                    Expr::Literal(Value::Float(40.4)),
                    Expr::Literal(Value::Float(-74.3)),
                    Expr::Literal(Value::Float(40.9)),
                    Expr::Literal(Value::Float(-73.7)),
                ]
            )
        );
        // top_right/bottom_left and flat edges express the same box; WKT
        // points work for corners.
        for body in [
            json!({ "geo_bounding_box": { "loc": {
                "top_right":   { "lat": 40.9, "lon": -73.7 },
                "bottom_left": { "lat": 40.4, "lon": -74.3 }
            } } }),
            json!({ "geo_bounding_box": { "loc": {
                "top": 40.9, "left": -74.3, "bottom": 40.4, "right": -73.7
            } } }),
            json!({ "geo_bounding_box": { "loc": {
                "top_left":     "POINT (-74.3 40.9)",
                "bottom_right": "POINT (-73.7 40.4)"
            } } }),
        ] {
            assert_eq!(query_expr(&body).unwrap().unwrap(), e, "{body}");
        }
        // Incomplete boxes error.
        assert!(query_expr(&json!({
            "geo_bounding_box": { "loc": { "top_left": { "lat": 1.0, "lon": 2.0 } } }
        }))
        .is_err());
    }

    #[test]
    fn text_similarity_reranker_maps_to_rerank() {
        // Over a standard (search) retriever: no NEAREST, RERANK carries the
        // field/model/query/window.
        let spec = parse_vector(&json!({ "retriever": { "text_similarity_reranker": {
            "retriever": { "standard": { "query": { "match": { "body": "rust db" } } } },
            "field": "body",
            "inference_id": "rerank-v3",
            "inference_text": "rust database engines",
            "rank_window_size": 50
        } } }))
        .unwrap()
        .unwrap();
        assert!(spec.nearest.is_none() && spec.rrf.is_none());
        assert!(spec.filter.is_some());
        let rr = spec.rerank.unwrap();
        assert_eq!(rr.column.as_deref(), Some("body"));
        assert_eq!(rr.model.as_deref(), Some("rerank-v3"));
        assert_eq!(rr.query.as_deref(), Some("rust database engines"));
        assert_eq!(rr.top, 50);
        // Over an rrf retriever: hybrid + rerank compose; the window defaults
        // to ES's 10.
        let spec = parse_vector(&json!({ "retriever": { "text_similarity_reranker": {
            "retriever": { "rrf": { "retrievers": [
                { "standard": { "query": { "match": { "body": "x" } } } },
                { "knn": { "field": "emb", "query_vector": [1.0], "k": 20 } }
            ] } },
            "inference_text": "q"
        } } }))
        .unwrap()
        .unwrap();
        assert!(spec.nearest.is_some() && spec.rrf.is_some());
        assert_eq!(spec.rerank.unwrap().top, 10);
        // A reranker cannot nest another reranker.
        assert!(parse_vector(&json!({ "retriever": { "text_similarity_reranker": {
            "retriever": { "text_similarity_reranker": {
                "retriever": { "standard": {} }
            } },
        } } }))
        .is_err());
    }

    #[test]
    fn search_after_maps_to_after_cursor() {
        let mut sel = select_from("docs");
        // sort [created desc, _id asc] + two cursor values → ORDER BY created
        // (the _id tie-break folds into the implicit AFTER pk half).
        let pairs = vec![("created".to_string(), true), ("_id".to_string(), false)];
        sel.order_by = vec![
            OrderKey { expr: Expr::Column("created".into()), descending: true },
            OrderKey { expr: Expr::Column("_id".into()), descending: false },
        ];
        apply_search_after(&mut sel, &pairs, &json!([1710000000, 42]), "id").unwrap();
        assert_eq!(sel.order_by.len(), 1);
        assert_eq!(
            sel.after,
            Some(vec![
                Expr::Literal(Value::Int(1710000000)),
                Expr::Literal(Value::Int(42)),
            ])
        );
        // The pk column name works as the tie-break too, and _id alone pages
        // by primary key.
        let mut sel = select_from("docs");
        let pairs = vec![("_id".to_string(), false)];
        apply_search_after(&mut sel, &pairs, &json!(["a41"]), "id").unwrap();
        assert!(matches!(&sel.order_by[..], [OrderKey { expr: Expr::Column(c), descending: false }] if c == "id"));
        assert_eq!(
            sel.after,
            Some(vec![
                Expr::Literal(Value::String("a41".into())),
                Expr::Literal(Value::String("a41".into())),
            ])
        );
        // Errors: no sort, arity mismatch, missing/descending _id tie-break.
        let mut sel = select_from("docs");
        assert!(apply_search_after(&mut sel, &[], &json!([1]), "id").is_err());
        let pairs = vec![("created".to_string(), true), ("_id".to_string(), false)];
        assert!(apply_search_after(&mut sel, &pairs, &json!([1]), "id").is_err());
        let no_id = vec![("created".to_string(), true), ("title".to_string(), false)];
        assert!(apply_search_after(&mut sel, &no_id, &json!([1, 2]), "id").is_err());
        let desc_id = vec![("created".to_string(), true), ("_id".to_string(), true)];
        assert!(apply_search_after(&mut sel, &desc_id, &json!([1, 2]), "id").is_err());
    }
}
