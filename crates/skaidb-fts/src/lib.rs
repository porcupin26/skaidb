//! Full-text search for skaidb.
//!
//! Wraps [Tantivy](https://github.com/quickwit-oss/tantivy) behind an
//! engine-agnostic API: skaidb types in, skaidb types out. No Tantivy types
//! leak past this crate, so the engine and SQL layers stay independent of
//! the search core (see docs/FTS_TODO.md §0–1).
//!
//! The index is derived data over an LSM table. The table's WAL is the
//! translog: puts/deletes are applied here immediately (visible to searches
//! after the next commit) but only made durable by [`SearchIndex::commit`],
//! which persists the max row HLC seen (the [`Watermark`]) atomically with
//! the segment data. After a crash the engine replays table rows newer than
//! the watermark, or rebuilds from scratch if the index is missing/corrupt.

mod analyzer;
mod index;
mod query;

pub use analyzer::Analyzer;
pub use index::{Highlighter, SearchIndex, SearchIndexStats, SortedHits, Suggestion};
pub use query::SearchQuery;

/// Errors surfaced by the search crate. Engine code maps these onto
/// `EngineError`; none of them wrap Tantivy types directly.
#[derive(Debug, thiserror::Error)]
pub enum FtsError {
    /// Bad index configuration or query (user error).
    #[error("{0}")]
    Config(String),
    /// The on-disk index does not match the catalog definition (or is
    /// damaged) and must be rebuilt from the table.
    #[error("search index needs rebuild: {0}")]
    NeedsRebuild(String),
    /// Internal engine failure (I/O, corrupt segment, ...).
    #[error("search engine error: {0}")]
    Engine(String),
}

impl From<tantivy::TantivyError> for FtsError {
    fn from(e: tantivy::TantivyError) -> Self {
        FtsError::Engine(e.to_string())
    }
}

/// Durability watermark: the max row HLC included in the last index commit.
/// Mirrors the engine's `Hlc` without depending on the storage crate.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct Watermark {
    pub physical: u64,
    pub logical: u32,
}

/// A grouped-facet request the engine pushes into the index (phase 6):
/// terms buckets over one **keyword fast-field** column — or one global
/// row — with metrics over numeric fast fields. Serializable so the
/// cluster layer can ship it to peers.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggRequest {
    /// How to bucket; `None` = a single global row.
    pub group_by: Option<AggGroupBy>,
    pub metrics: Vec<AggMetric>,
}

/// The bucketing of an [`AggRequest`].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum AggGroupBy {
    /// Terms buckets over a declared keyword column (SQL `GROUP BY col`).
    Keyword(String),
    /// Fixed-interval buckets over a declared date column (SQL
    /// `GROUP BY time_bucket(step, col)`); keys are floored millisecond
    /// timestamps, exactly like `time_bucket`.
    DateHistogram { column: String, interval_ms: i64 },
}

/// One metric within an [`AggRequest`].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggMetric {
    pub func: AggMetricFunc,
    /// The numeric column the metric reads; `None` only for `Count`
    /// (`COUNT(*)` reads the bucket's doc count).
    pub column: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggMetricFunc {
    /// `COUNT(*)` — bucket doc count.
    Count,
    /// `COUNT(col)` — number of present values.
    ValueCount,
    /// `COUNT(DISTINCT col)` — **exact** distinct count, computed as a
    /// nested terms bucket count (bails to the row fallback rather than
    /// approximate à la HLL when the term set would truncate).
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
}

/// One aggregation result bucket. Values come back as engine [`Value`]s
/// typed by the column declarations (a `SUM` over a `long` column is an
/// `Int`, matching what the row-materialization path computes), `Null`
/// where SQL says NULL — no values in the bucket, including `SUM`, which
/// SQL nulls but ES-style aggregations report as 0.
#[derive(Debug, Clone, PartialEq)]
pub struct AggRow {
    /// Bucket key (`String` for a keyword bucket); `Null` for the global
    /// row and for rows missing the group column (SQL's NULL group).
    pub key: skaidb_types::Value,
    /// Docs in the bucket.
    pub count: u64,
    /// One entry per requested metric, in request order.
    pub metrics: Vec<skaidb_types::Value>,
}

/// A fast-field sort for top-k retrieval (SQL
/// `ORDER BY <col> [DESC] LIMIT k` over a search query).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SortSpec {
    /// A declared fast-field column (keyword/long/double/bool/date).
    pub column: String,
    pub descending: bool,
}

/// One search result: the row's primary-key bytes and its BM25 score.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub key: Vec<u8>,
    pub score: f32,
}

/// How a column is indexed (ES mapping types; the `SEARCH INDEX`
/// declaration is the mapping — skaidb rows are schema-less, so values
/// that don't fit the declared type are simply not indexed for that
/// column).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    /// Analyzed full text (the default).
    Text,
    /// The whole string as one exact-match term.
    Keyword,
    /// Signed 64-bit integer fast field.
    Long,
    /// 64-bit float fast field (accepts ints too).
    Double,
    /// Boolean fast field.
    Bool,
    /// Millisecond-timestamp fast field (accepts `timestamp` and integer
    /// values).
    Date,
}

impl FieldType {
    pub fn parse(name: &str) -> Result<FieldType, FtsError> {
        match name.to_ascii_lowercase().as_str() {
            "text" => Ok(FieldType::Text),
            "keyword" => Ok(FieldType::Keyword),
            "long" => Ok(FieldType::Long),
            "double" => Ok(FieldType::Double),
            "bool" => Ok(FieldType::Bool),
            "date" => Ok(FieldType::Date),
            other => Err(FtsError::Config(format!(
                "unknown field type '{other}' (expected text, keyword, long, double, bool, or date)"
            ))),
        }
    }

    /// Whether MATCH()-family text predicates can target the field.
    pub(crate) fn is_texty(&self) -> bool {
        matches!(self, FieldType::Text | FieldType::Keyword)
    }
}

/// Per-column index configuration.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FieldConfig {
    /// Dotted path into the row document.
    pub path: String,
    pub ftype: FieldType,
    /// Index-time analyzer; `None` uses the index default. Text fields only.
    pub analyzer: Option<Analyzer>,
    /// Query-time analyzer; `None` uses the index-time one. The classic use
    /// is `edge_ngram` at index time with `standard` at query time.
    pub search_analyzer: Option<Analyzer>,
    /// Score multiplier when the field participates in a multi-field query.
    pub boost: f32,
    /// Also index the raw string as `<path>.keyword` (exact-match twin).
    pub keyword_twin: bool,
    /// Additionally copy this field's text into a named composite field
    /// (ES `copy_to`); the target is searchable like any text column.
    pub copy_to: Option<String>,
}

impl FieldConfig {
    pub fn text(path: &str) -> FieldConfig {
        FieldConfig {
            path: path.to_string(),
            ftype: FieldType::Text,
            analyzer: None,
            search_analyzer: None,
            boost: 1.0,
            keyword_twin: false,
            copy_to: None,
        }
    }
}

/// Configuration for a search index, derived from the
/// `CREATE SEARCH INDEX ... WITH (...)` declaration.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SearchIndexConfig {
    pub fields: Vec<FieldConfig>,
    /// Analyzer for text fields that don't override it.
    pub default_analyzer: Analyzer,
    /// Synonym groups (`synonyms = 'quick,fast,speedy; car,auto'`),
    /// expanded at **query time** — hot-swappable via
    /// `ALTER SEARCH INDEX … SET`, no reindex. Single-word entries only.
    #[serde(default)]
    pub synonyms: Vec<Vec<String>>,
}

impl SearchIndexConfig {
    /// Build a config from the SQL declaration: the indexed paths plus the
    /// raw `WITH (...)` options. Returns the config and `refresh_ms`.
    ///
    /// Global options: `analyzer`, `refresh_ms`. Per-column options use the
    /// column path as a prefix: `<path>.analyzer`, `<path>.search_analyzer`,
    /// `<path>.type`, `<path>.boost`, `<path>.keyword` (true/false),
    /// `<path>.copy_to`. Unknown options or option prefixes error.
    pub fn from_declaration(
        paths: &[String],
        options: &[(String, String)],
    ) -> Result<(SearchIndexConfig, u64), FtsError> {
        if paths.is_empty() {
            return Err(FtsError::Config(
                "a search index needs at least one column".into(),
            ));
        }
        let mut fields: Vec<FieldConfig> = paths.iter().map(|p| FieldConfig::text(p)).collect();
        let mut default_analyzer = Analyzer::Standard;
        let mut refresh_ms = 1000u64;
        let mut synonyms: Vec<Vec<String>> = Vec::new();

        for (key, value) in options {
            match key.as_str() {
                "analyzer" => default_analyzer = Analyzer::parse(value)?,
                "synonyms" => synonyms = parse_synonyms(value)?,
                "refresh_ms" => {
                    refresh_ms = value.parse().map_err(|_| {
                        FtsError::Config(format!(
                            "refresh_ms must be a non-negative integer, got '{value}'"
                        ))
                    })?;
                }
                _ => {
                    // `<path>.<option>` — longest declared path wins, so a
                    // nested column like `meta.title` parses correctly.
                    let field = fields
                        .iter_mut()
                        .filter(|f| {
                            key.strip_prefix(&f.path)
                                .is_some_and(|rest| rest.starts_with('.'))
                        })
                        .max_by_key(|f| f.path.len())
                        .ok_or_else(|| {
                            FtsError::Config(format!(
                                "unknown search index option '{key}' \
                                 (not a global option or a '<column>.<option>' of an indexed column)"
                            ))
                        })?;
                    let opt = &key[field.path.len() + 1..];
                    match opt {
                        "analyzer" => field.analyzer = Some(Analyzer::parse(value)?),
                        "search_analyzer" => {
                            field.search_analyzer = Some(Analyzer::parse(value)?)
                        }
                        "type" => field.ftype = FieldType::parse(value)?,
                        "boost" => {
                            let boost: f32 = value.parse().map_err(|_| {
                                FtsError::Config(format!(
                                    "{key} must be a number, got '{value}'"
                                ))
                            })?;
                            if !(boost.is_finite() && boost > 0.0) {
                                return Err(FtsError::Config(format!(
                                    "{key} must be a positive number, got '{value}'"
                                )));
                            }
                            field.boost = boost;
                        }
                        "keyword" => {
                            field.keyword_twin = match value.as_str() {
                                "true" => true,
                                "false" => false,
                                other => {
                                    return Err(FtsError::Config(format!(
                                        "{key} must be true or false, got '{other}'"
                                    )))
                                }
                            };
                        }
                        "copy_to" => field.copy_to = Some(value.clone()),
                        other => {
                            return Err(FtsError::Config(format!(
                                "unknown per-column option '{other}' for column '{}' \
                                 (expected analyzer, search_analyzer, type, boost, keyword, \
                                 or copy_to)",
                                field.path
                            )))
                        }
                    }
                }
            }
        }

        // Analyzer / twin options only make sense on text-searchable fields.
        for f in &fields {
            if !f.ftype.is_texty()
                && (f.analyzer.is_some()
                    || f.search_analyzer.is_some()
                    || f.keyword_twin
                    || f.copy_to.is_some())
            {
                return Err(FtsError::Config(format!(
                    "column '{}' is declared {:?} and cannot take analyzer/keyword/copy_to options",
                    f.path, f.ftype
                )));
            }
            if let Some(target) = &f.copy_to {
                if fields.iter().any(|other| &other.path == target) {
                    return Err(FtsError::Config(format!(
                        "copy_to target '{target}' collides with an indexed column"
                    )));
                }
            }
        }
        Ok((
            SearchIndexConfig {
                fields,
                default_analyzer,
                synonyms,
            },
            refresh_ms,
        ))
    }
}

/// Parse the `synonyms` option: groups separated by `;`, terms separated
/// by `,` within a group. Multi-word entries are allowed — at query time
/// they match as consecutive token sequences and expand as phrases.
fn parse_synonyms(spec: &str) -> Result<Vec<Vec<String>>, FtsError> {
    let mut groups = Vec::new();
    for group in spec.split(';') {
        let terms: Vec<String> = group
            .split(',')
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(str::to_string)
            .collect();
        if terms.is_empty() {
            continue;
        }
        if terms.len() < 2 {
            return Err(FtsError::Config(format!(
                "synonym group '{group}' needs at least two comma-separated terms"
            )));
        }
        groups.push(terms);
    }
    Ok(groups)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn declaration_defaults_and_per_field_options() {
        let paths = vec!["title".to_string(), "body".to_string(), "year".to_string()];
        let (cfg, refresh) = SearchIndexConfig::from_declaration(
            &paths,
            &opts(&[
                ("analyzer", "english"),
                ("refresh_ms", "250"),
                ("title.boost", "2.5"),
                ("title.keyword", "true"),
                ("title.search_analyzer", "standard"),
                ("body.copy_to", "everything"),
                ("year.type", "long"),
            ]),
        )
        .unwrap();
        assert_eq!(refresh, 250);
        assert_eq!(cfg.default_analyzer.to_string(), "english");
        let title = &cfg.fields[0];
        assert_eq!(title.boost, 2.5);
        assert!(title.keyword_twin);
        assert_eq!(title.search_analyzer.as_ref().unwrap().to_string(), "standard");
        assert_eq!(cfg.fields[1].copy_to.as_deref(), Some("everything"));
        assert_eq!(cfg.fields[2].ftype, FieldType::Long);
    }

    #[test]
    fn declaration_rejects_bad_options() {
        let paths = vec!["meta.title".to_string(), "n".to_string()];
        let bad: &[&[(&str, &str)]] = &[
            &[("nope", "x")],                       // unknown global
            &[("meta.title.wat", "x")],             // unknown per-field option
            &[("other.analyzer", "english")],       // not an indexed column
            &[("meta.title.boost", "-1")],          // non-positive boost
            &[("n.type", "long"), ("n.keyword", "true")], // twin on a numeric
            &[("refresh_ms", "soon")],
        ];
        for case in bad {
            assert!(
                SearchIndexConfig::from_declaration(&paths, &opts(case)).is_err(),
                "expected error for {case:?}"
            );
        }
        // Dotted columns resolve their options (longest-prefix match).
        let (cfg, _) = SearchIndexConfig::from_declaration(
            &paths,
            &opts(&[("meta.title.analyzer", "french")]),
        )
        .unwrap();
        assert_eq!(cfg.fields[0].analyzer.as_ref().unwrap().to_string(), "french");
    }
}
