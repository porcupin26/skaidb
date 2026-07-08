//! Engine-agnostic query model and its translation to Tantivy queries.
//!
//! The SQL layer builds a [`SearchQuery`] from `MATCH()` / `MATCH_PHRASE()`
//! / `FUZZY()` / `SEARCH()` predicates; this module analyzes the text with
//! the index's own tokenizer and assembles the corresponding Tantivy query.

use tantivy::query::{
    BooleanQuery, EmptyQuery, FuzzyTermQuery, Occur, PhraseQuery, Query, QueryParser,
};
use tantivy::schema::Field;
use tantivy::{Index, Term};

use crate::FtsError;

/// Maximum Levenshtein distance Tantivy's FST automata support.
const MAX_FUZZY_DISTANCE: u8 = 2;

/// A search predicate, serializable so the cluster layer can ship it to
/// peers in phase 4.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SearchQuery {
    /// Analyzed terms OR-ed together (ES `match`). `field: None` searches
    /// every indexed field.
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
    /// Query-string mini-language over the default fields
    /// (`title:"rust db" +performance -draft`).
    QueryString(String),
    /// All sub-queries must match (SQL AND).
    All(Vec<SearchQuery>),
    /// At least one sub-query must match (SQL OR).
    Any(Vec<SearchQuery>),
}

/// Field lookup the builder needs from the index.
pub(crate) struct QueryFields<'a> {
    pub fields: &'a [(String, Field)],
}

impl QueryFields<'_> {
    /// Resolve a field name to the tantivy fields to search: a named field
    /// must exist in the index; `None` means all indexed fields.
    fn resolve(&self, name: &Option<String>) -> Result<Vec<Field>, FtsError> {
        match name {
            None => Ok(self.fields.iter().map(|(_, f)| *f).collect()),
            Some(name) => self
                .fields
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, f)| vec![*f])
                .ok_or_else(|| {
                    FtsError::Config(format!("column '{name}' is not covered by the search index"))
                }),
        }
    }
}

/// Analyze `text` with the field's tokenizer, returning `(position, term)`
/// pairs.
fn analyze(index: &Index, field: Field, text: &str) -> Result<Vec<(usize, Term)>, FtsError> {
    let mut analyzer = index.tokenizer_for_field(field)?;
    let mut stream = analyzer.token_stream(text);
    let mut terms = Vec::new();
    while let Some(token) = stream.next() {
        terms.push((token.position, Term::from_field_text(field, &token.text)));
    }
    Ok(terms)
}

/// OR together one query per field, dropping fields where analysis yields
/// no terms.
fn per_field_union(
    fields: Vec<Field>,
    mut build: impl FnMut(Field) -> Result<Option<Box<dyn Query>>, FtsError>,
) -> Result<Box<dyn Query>, FtsError> {
    let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
    for field in fields {
        if let Some(q) = build(field)? {
            clauses.push((Occur::Should, q));
        }
    }
    Ok(match clauses.len() {
        0 => Box::new(EmptyQuery),
        1 => clauses.pop().expect("len checked").1,
        _ => Box::new(BooleanQuery::new(clauses)),
    })
}

pub(crate) fn build_query(
    index: &Index,
    fields: &QueryFields<'_>,
    query: &SearchQuery,
) -> Result<Box<dyn Query>, FtsError> {
    match query {
        SearchQuery::Match { field, text } => {
            per_field_union(fields.resolve(field)?, |f| {
                let terms = analyze(index, f, text)?;
                if terms.is_empty() {
                    return Ok(None);
                }
                let clauses: Vec<(Occur, Box<dyn Query>)> = terms
                    .into_iter()
                    .map(|(_, term)| {
                        let q: Box<dyn Query> = Box::new(tantivy::query::TermQuery::new(
                            term,
                            tantivy::schema::IndexRecordOption::WithFreqs,
                        ));
                        (Occur::Should, q)
                    })
                    .collect();
                Ok(Some(Box::new(BooleanQuery::new(clauses))))
            })
        }
        SearchQuery::Phrase { field, text, slop } => {
            per_field_union(fields.resolve(field)?, |f| {
                let terms = analyze(index, f, text)?;
                Ok(match terms.len() {
                    0 => None,
                    // A one-term "phrase" is just a term query.
                    1 => Some(Box::new(tantivy::query::TermQuery::new(
                        terms.into_iter().next().expect("len checked").1,
                        tantivy::schema::IndexRecordOption::WithFreqs,
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
            per_field_union(fields.resolve(field)?, |f| {
                let terms = analyze(index, f, text)?;
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
        SearchQuery::QueryString(text) => {
            let default_fields = fields.fields.iter().map(|(_, f)| *f).collect();
            let parser = QueryParser::for_index(index, default_fields);
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
    }
}
