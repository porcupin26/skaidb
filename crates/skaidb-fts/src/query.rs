//! Engine-agnostic query model and its translation to Tantivy queries.
//!
//! The SQL layer builds a [`SearchQuery`] from the `MATCH()` predicate
//! family; this module analyzes the text with each field's **query-time**
//! analyzer (`search_analyzer`, falling back to the index-time one) and
//! assembles the corresponding Tantivy query, applying per-field boosts.
//! Multi-field queries combine per-field scores with dis-max, matching
//! Elasticsearch's `multi_match` `best_fields` default.

use tantivy::query::{
    AllQuery, BooleanQuery, BoostQuery, DisjunctionMaxQuery, EmptyQuery, FuzzyTermQuery, Occur,
    PhraseQuery, Query, QueryParser, RegexQuery, TermQuery,
};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::{Index, Term};

use crate::{Analyzer, FieldType, FtsError};

/// Maximum Levenshtein distance Tantivy's FST automata support.
const MAX_FUZZY_DISTANCE: u8 = 2;

/// A search predicate, serializable so the cluster layer can ship it to
/// peers in phase 4.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SearchQuery {
    /// Analyzed terms OR-ed together (ES `match`). `field: None` searches
    /// every text-searchable field.
    Match { field: Option<String>, text: String },
    /// Terms in order within `slop` transpositions (ES `match_phrase`).
    Phrase {
        field: Option<String>,
        text: String,
        slop: u32,
    },
    /// Levenshtein-fuzzy terms, distance ≤ 2 (ES `fuzzy`).
    Fuzzy {
        field: Option<String>,
        text: String,
        distance: u8,
    },
    /// Term prefix match (ES `prefix`). The prefix is **not analyzed**: it
    /// runs against the indexed terms, so with a lowercasing analyzer write
    /// it lowercase.
    Prefix { field: Option<String>, text: String },
    /// `*` (any run) / `?` (any one char) glob over indexed terms (ES
    /// `wildcard`). Not analyzed, like `Prefix`.
    Wildcard {
        field: Option<String>,
        pattern: String,
    },
    /// Regular expression over indexed terms (ES `regexp`). Not analyzed.
    Regexp {
        field: Option<String>,
        pattern: String,
    },
    /// Rows textually similar to `text` (ES `more_like_this`): the like-
    /// text's most distinctive terms (by in-index IDF) OR-ed together.
    MoreLikeThis {
        field: Option<String>,
        text: String,
    },
    /// Query-string mini-language over the default fields
    /// (`title:"rust db" +performance -draft`).
    QueryString(String),
    /// All sub-queries must match (SQL AND).
    All(Vec<SearchQuery>),
    /// At least one sub-query must match (SQL OR).
    Any(Vec<SearchQuery>),
    /// Rows matching the sub-query are excluded (SQL NOT). Rows absent from
    /// the index (none of the indexed columns present) are never returned —
    /// the index cannot speak for rows it does not contain.
    Not(Box<SearchQuery>),
}

/// What the query builder needs to know about one indexed field.
#[derive(Debug, Clone)]
pub(crate) struct FieldRuntime {
    /// Dotted path (and tantivy field name). Synthetic `copy_to` targets and
    /// `.keyword` twins appear here too, so they are directly queryable.
    pub path: String,
    pub field: Field,
    pub ftype: FieldType,
    /// Query-time analyzer (already resolved: search_analyzer, else the
    /// index-time analyzer, else the index default).
    pub query_analyzer: Analyzer,
    pub boost: f32,
}

pub(crate) struct QueryFields<'a> {
    pub fields: &'a [FieldRuntime],
}

impl QueryFields<'_> {
    /// Resolve a field name for a text predicate: a named field must exist
    /// and be text-searchable; `None` means every text-searchable field.
    fn resolve(&self, name: &Option<String>) -> Result<Vec<&FieldRuntime>, FtsError> {
        match name {
            None => Ok(self.fields.iter().filter(|f| f.ftype.is_texty()).collect()),
            Some(name) => {
                let field = self
                    .fields
                    .iter()
                    .find(|f| &f.path == name)
                    .ok_or_else(|| {
                        FtsError::Config(format!(
                            "column '{name}' is not covered by the search index"
                        ))
                    })?;
                if !field.ftype.is_texty() {
                    return Err(FtsError::Config(format!(
                        "column '{name}' is declared {:?} — text predicates need a text or \
                         keyword column",
                        field.ftype
                    )));
                }
                Ok(vec![field])
            }
        }
    }
}

/// Analyze `text` with the field's query-time analyzer, returning
/// `(position, term)` pairs.
fn analyze(rt: &FieldRuntime, text: &str) -> Vec<(usize, Term)> {
    let mut analyzer = rt.query_analyzer.build();
    let mut stream = analyzer.token_stream(text);
    let mut terms = Vec::new();
    while let Some(token) = stream.next() {
        terms.push((token.position, Term::from_field_text(rt.field, &token.text)));
    }
    terms
}

/// Apply the field boost to a built query.
fn boosted(q: Box<dyn Query>, boost: f32) -> Box<dyn Query> {
    if (boost - 1.0).abs() < f32::EPSILON {
        q
    } else {
        Box::new(BoostQuery::new(q, boost))
    }
}

/// Combine one query per field, dropping fields where analysis yields no
/// terms. Multiple fields dis-max (a row scores as its best field, ES
/// `best_fields`) — the match *set* is still the union.
fn per_field_union(
    fields: Vec<&FieldRuntime>,
    mut build: impl FnMut(&FieldRuntime) -> Result<Option<Box<dyn Query>>, FtsError>,
) -> Result<Box<dyn Query>, FtsError> {
    let mut clauses: Vec<Box<dyn Query>> = Vec::new();
    for rt in fields {
        if let Some(q) = build(rt)? {
            clauses.push(boosted(q, rt.boost));
        }
    }
    Ok(match clauses.len() {
        0 => Box::new(EmptyQuery),
        1 => clauses.pop().expect("len checked"),
        _ => Box::new(DisjunctionMaxQuery::new(clauses)),
    })
}

/// A [`RegexQuery`] over a field's indexed terms; pattern errors are user
/// errors.
fn regex_query(field: Field, pattern: &str) -> Result<Box<dyn Query>, FtsError> {
    RegexQuery::from_pattern(pattern, field)
        .map(|q| Box::new(q) as Box<dyn Query>)
        .map_err(|e| FtsError::Config(format!("invalid pattern '{pattern}': {e}")))
}

/// Escape one char into `out` if it is a regex metacharacter.
fn push_regex_literal(out: &mut String, c: char) {
    if "\\.+*?()|[]{}^$#&-~<>".contains(c) {
        out.push('\\');
    }
    out.push(c);
}

/// Translate an ES-style wildcard pattern (`*` any run, `?` any one char)
/// into a regex.
fn wildcard_to_regex(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 4);
    for c in pattern.chars() {
        match c {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            c => push_regex_literal(&mut out, c),
        }
    }
    out
}

pub(crate) fn build_query(
    index: &Index,
    fields: &QueryFields<'_>,
    query: &SearchQuery,
) -> Result<Box<dyn Query>, FtsError> {
    match query {
        SearchQuery::Match { field, text } => per_field_union(fields.resolve(field)?, |rt| {
            let terms = analyze(rt, text);
            if terms.is_empty() {
                return Ok(None);
            }
            let clauses: Vec<(Occur, Box<dyn Query>)> = terms
                .into_iter()
                .map(|(_, term)| {
                    let q: Box<dyn Query> =
                        Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                    (Occur::Should, q)
                })
                .collect();
            Ok(Some(Box::new(BooleanQuery::new(clauses))))
        }),
        SearchQuery::Phrase { field, text, slop } => {
            per_field_union(fields.resolve(field)?, |rt| {
                let terms = analyze(rt, text);
                Ok(match terms.len() {
                    0 => None,
                    // A one-term "phrase" is just a term query.
                    1 => Some(Box::new(TermQuery::new(
                        terms.into_iter().next().expect("len checked").1,
                        IndexRecordOption::WithFreqs,
                    ))),
                    _ => Some(Box::new(PhraseQuery::new_with_offset_and_slop(
                        terms, *slop,
                    ))),
                })
            })
        }
        SearchQuery::Fuzzy {
            field,
            text,
            distance,
        } => {
            if *distance > MAX_FUZZY_DISTANCE {
                return Err(FtsError::Config(format!(
                    "fuzzy distance {distance} exceeds the maximum of {MAX_FUZZY_DISTANCE}"
                )));
            }
            per_field_union(fields.resolve(field)?, |rt| {
                let terms = analyze(rt, text);
                if terms.is_empty() {
                    return Ok(None);
                }
                let clauses: Vec<(Occur, Box<dyn Query>)> = terms
                    .into_iter()
                    .map(|(_, term)| {
                        let q: Box<dyn Query> =
                            Box::new(FuzzyTermQuery::new(term, *distance, true));
                        (Occur::Should, q)
                    })
                    .collect();
                Ok(Some(Box::new(BooleanQuery::new(clauses))))
            })
        }
        SearchQuery::Prefix { field, text } => {
            let pattern = {
                let mut out = String::with_capacity(text.len() + 2);
                text.chars().for_each(|c| push_regex_literal(&mut out, c));
                out.push_str(".*");
                out
            };
            per_field_union(fields.resolve(field)?, |rt| {
                Some(regex_query(rt.field, &pattern)).transpose()
            })
        }
        SearchQuery::Wildcard { field, pattern } => {
            let pattern = wildcard_to_regex(pattern);
            per_field_union(fields.resolve(field)?, |rt| {
                Some(regex_query(rt.field, &pattern)).transpose()
            })
        }
        SearchQuery::Regexp { field, pattern } => {
            per_field_union(fields.resolve(field)?, |rt| {
                Some(regex_query(rt.field, pattern)).transpose()
            })
        }
        SearchQuery::MoreLikeThis { field, text } => {
            // Tantivy's MLT picks the like-text's most distinctive terms
            // by in-index document frequency. Defaults lean permissive
            // (ES's min_term_freq=2 silently empties short like-texts):
            // every term of the text counts, terms in < 2 docs or > 25
            // total are dropped.
            let field_values: Vec<(Field, Vec<tantivy::schema::OwnedValue>)> = fields
                .resolve(field)?
                .into_iter()
                .map(|rt| {
                    (
                        rt.field,
                        vec![tantivy::schema::OwnedValue::Str(text.clone())],
                    )
                })
                .collect();
            Ok(Box::new(
                tantivy::query::MoreLikeThisQuery::builder()
                    .with_min_term_frequency(1)
                    .with_min_doc_frequency(2)
                    .with_max_query_terms(25)
                    .with_document_fields(field_values),
            ))
        }
        SearchQuery::QueryString(text) => {
            // Text fields are the defaults for bare terms; typed fields
            // remain addressable as `field:value` / `field:[a TO b]`.
            let default_fields: Vec<Field> = fields
                .fields
                .iter()
                .filter(|f| f.ftype.is_texty())
                .map(|f| f.field)
                .collect();
            let mut parser = QueryParser::for_index(index, default_fields);
            for rt in fields.fields {
                if (rt.boost - 1.0).abs() >= f32::EPSILON {
                    parser.set_field_boost(rt.field, rt.boost);
                }
            }
            parser
                .parse_query(text)
                .map_err(|e| FtsError::Config(format!("invalid search query: {e}")))
        }
        SearchQuery::All(subs) => {
            let mut clauses = Vec::with_capacity(subs.len());
            for sub in subs {
                clauses.push((Occur::Must, build_query(index, fields, sub)?));
            }
            Ok(Box::new(BooleanQuery::new(clauses)))
        }
        SearchQuery::Any(subs) => {
            let mut clauses = Vec::with_capacity(subs.len());
            for sub in subs {
                clauses.push((Occur::Should, build_query(index, fields, sub)?));
            }
            Ok(Box::new(BooleanQuery::new(clauses)))
        }
        SearchQuery::Not(inner) => {
            // must_not needs a positive base; AllQuery spans every indexed
            // row (rows with none of the indexed columns are not in the
            // index and can never be returned — see the variant docs).
            let sub = build_query(index, fields, inner)?;
            Ok(Box::new(BooleanQuery::new(vec![
                (Occur::Must, Box::new(AllQuery) as Box<dyn Query>),
                (Occur::MustNot, sub),
            ])))
        }
    }
}
