//! Search index lifecycle: create/open, writes, commits, and queries.
//!
//! One `SearchIndex` wraps one Tantivy index directory (`<root>/fts/<name>`
//! from the engine's point of view). Writes apply immediately to the writer
//! but only become durable — and visible to searches — at [`commit`], which
//! atomically persists the max row HLC as the commit payload. The engine
//! drives commit cadence (NRT refresh) and crash recovery from that
//! watermark.
//!
//! [`commit`]: SearchIndex::commit

use std::fmt;
use std::path::{Path, PathBuf};

use skaidb_types::{Document, Value};
use tantivy::collector::{DocSetCollector, TopDocs};
use tantivy::schema::{
    BytesOptions, DateOptions, Field, IndexRecordOption, NumericOptions, Schema,
    TextFieldIndexing, TextOptions, Value as _,
};
use tantivy::snippet::SnippetGenerator;
use tantivy::{DateTime, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term};

use crate::query::{build_query, FieldRuntime, QueryFields};
use crate::{
    analyzer, Analyzer, FieldType, FtsError, SearchHit, SearchIndexConfig, SearchQuery, Watermark,
};

/// Reserved field holding the row's primary-key bytes.
const KEY_FIELD: &str = "_key";

/// Tantivy rejects writer heaps below ~15 MB; clamp so tiny `memory_target`
/// configurations still work.
const MIN_WRITER_HEAP: usize = 16 * 1024 * 1024;

/// Point-in-time counters for `SHOW STATUS` / metrics.
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchIndexStats {
    /// Searchable (committed) documents.
    pub docs: u64,
    /// Bytes on disk across all segment files.
    pub disk_bytes: u64,
    /// Writes applied since the last commit (lost on crash until committed;
    /// recovered by watermark replay).
    pub uncommitted: u64,
}

/// How one declared column feeds the tantivy document at write time.
struct IndexedColumn {
    path: String,
    field: Field,
    ftype: FieldType,
    /// `<path>.keyword` exact-match twin, if declared.
    twin: Option<Field>,
    /// `copy_to` composite target, if declared.
    copy_to: Option<Field>,
}

/// A live full-text index over one table.
pub struct SearchIndex {
    dir: PathBuf,
    config: SearchIndexConfig,
    index: Index,
    writer: IndexWriter,
    reader: IndexReader,
    key_field: Field,
    /// Write-side view: one entry per declared column.
    columns: Vec<IndexedColumn>,
    /// Query-side view: declared columns plus `.keyword` twins and
    /// `copy_to` targets, each with its resolved query-time analyzer and
    /// boost.
    runtimes: Vec<FieldRuntime>,
    /// Max HLC applied to the writer but not yet committed.
    pending_watermark: Option<Watermark>,
    /// Max HLC durable in the last commit (from the commit payload).
    committed_watermark: Option<Watermark>,
    uncommitted: u64,
}

impl fmt::Debug for SearchIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SearchIndex")
            .field("dir", &self.dir)
            .field("config", &self.config)
            .field("committed_watermark", &self.committed_watermark)
            .field("uncommitted", &self.uncommitted)
            .finish_non_exhaustive()
    }
}

/// The text options for an analyzed field using `analyzer`.
fn text_options(analyzer: &Analyzer) -> TextOptions {
    TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(&analyzer.tokenizer_name())
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    )
}

/// The options for an exact-match (keyword) field: raw single term, indexed
/// with freqs, raw fast column for later sorting/facets.
fn keyword_options() -> TextOptions {
    TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(&Analyzer::Keyword.tokenizer_name())
                .set_index_option(IndexRecordOption::WithFreqs),
        )
        .set_fast(None)
}

fn build_schema(config: &SearchIndexConfig) -> Schema {
    let mut builder = Schema::builder();
    builder.add_bytes_field(
        KEY_FIELD,
        BytesOptions::default().set_indexed().set_stored(),
    );
    let numeric = || NumericOptions::default().set_indexed().set_fast();
    for fc in &config.fields {
        match fc.ftype {
            FieldType::Text => {
                let analyzer = fc.analyzer.as_ref().unwrap_or(&config.default_analyzer);
                builder.add_text_field(&fc.path, text_options(analyzer));
            }
            FieldType::Keyword => {
                builder.add_text_field(&fc.path, keyword_options());
            }
            FieldType::Long => {
                builder.add_i64_field(&fc.path, numeric());
            }
            FieldType::Double => {
                builder.add_f64_field(&fc.path, numeric());
            }
            FieldType::Bool => {
                builder.add_bool_field(&fc.path, numeric());
            }
            FieldType::Date => {
                builder.add_date_field(
                    &fc.path,
                    DateOptions::default().set_indexed().set_fast(),
                );
            }
        }
        if fc.keyword_twin {
            builder.add_text_field(&format!("{}.keyword", fc.path), keyword_options());
        }
    }
    // `copy_to` composite targets: analyzed with the index default. Several
    // columns may share one target; declare it once.
    let mut targets: Vec<&str> = config
        .fields
        .iter()
        .filter_map(|f| f.copy_to.as_deref())
        .collect();
    targets.sort_unstable();
    targets.dedup();
    for target in targets {
        builder.add_text_field(target, text_options(&config.default_analyzer));
    }
    builder.build()
}

impl SearchIndex {
    /// Open the index at `dir`, creating it if the directory is empty. If an
    /// existing index does not match `config` (columns, types, or analyzers
    /// changed), returns [`FtsError::NeedsRebuild`] — the caller wipes the
    /// directory and rebuilds from the table.
    pub fn open(
        dir: &Path,
        config: &SearchIndexConfig,
        writer_heap_bytes: usize,
    ) -> Result<SearchIndex, FtsError> {
        if config.fields.is_empty() {
            return Err(FtsError::Config(
                "a search index needs at least one column".into(),
            ));
        }
        std::fs::create_dir_all(dir)
            .map_err(|e| FtsError::Engine(format!("create {}: {e}", dir.display())))?;
        let schema = build_schema(config);
        let index = match Index::open_in_dir(dir) {
            Ok(existing) => {
                if existing.schema() != schema {
                    return Err(FtsError::NeedsRebuild(
                        "index schema does not match the catalog definition".into(),
                    ));
                }
                existing
            }
            Err(tantivy::TantivyError::IndexAlreadyExists) => unreachable!("open, not create"),
            Err(_) => {
                // Not an index (empty dir, or torn beyond recognition):
                // start fresh. A torn-but-openable index surfaces above as
                // NeedsRebuild or on first search.
                Index::create_in_dir(dir, schema.clone())?
            }
        };
        // Register every index-time analyzer the schema references.
        let mut used: Vec<Analyzer> = vec![config.default_analyzer.clone(), Analyzer::Keyword];
        used.extend(config.fields.iter().filter_map(|f| f.analyzer.clone()));
        used.dedup();
        analyzer::register(&index, used.into_iter());

        let key_field = schema.get_field(KEY_FIELD).expect("schema owns _key");
        let field_of = |path: &str| schema.get_field(path).expect("schema owns declared field");

        let mut columns = Vec::with_capacity(config.fields.len());
        let mut runtimes = Vec::new();
        for fc in &config.fields {
            let field = field_of(&fc.path);
            let twin = fc
                .keyword_twin
                .then(|| field_of(&format!("{}.keyword", fc.path)));
            let copy_to = fc.copy_to.as_deref().map(field_of);
            columns.push(IndexedColumn {
                path: fc.path.clone(),
                field,
                ftype: fc.ftype,
                twin,
                copy_to,
            });
            let index_analyzer = match fc.ftype {
                FieldType::Keyword => Analyzer::Keyword,
                _ => fc
                    .analyzer
                    .clone()
                    .unwrap_or_else(|| config.default_analyzer.clone()),
            };
            runtimes.push(FieldRuntime {
                path: fc.path.clone(),
                field,
                ftype: fc.ftype,
                query_analyzer: fc.search_analyzer.clone().unwrap_or(index_analyzer),
                boost: fc.boost,
            });
            if let Some(twin) = twin {
                runtimes.push(FieldRuntime {
                    path: format!("{}.keyword", fc.path),
                    field: twin,
                    ftype: FieldType::Keyword,
                    query_analyzer: Analyzer::Keyword,
                    boost: 1.0,
                });
            }
        }
        // Deduplicated copy_to targets, queryable like declared text columns.
        let mut targets: Vec<&str> = config
            .fields
            .iter()
            .filter_map(|f| f.copy_to.as_deref())
            .collect();
        targets.sort_unstable();
        targets.dedup();
        for target in targets {
            runtimes.push(FieldRuntime {
                path: target.to_string(),
                field: field_of(target),
                ftype: FieldType::Text,
                query_analyzer: config.default_analyzer.clone(),
                boost: 1.0,
            });
        }

        let writer = index.writer(writer_heap_bytes.max(MIN_WRITER_HEAP))?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let committed_watermark = match index.load_metas()?.payload {
            Some(payload) => Some(
                serde_json::from_str(&payload)
                    .map_err(|e| FtsError::NeedsRebuild(format!("bad commit payload: {e}")))?,
            ),
            None => None,
        };

        Ok(SearchIndex {
            dir: dir.to_path_buf(),
            config: config.clone(),
            index,
            writer,
            reader,
            key_field,
            columns,
            runtimes,
            pending_watermark: None,
            committed_watermark,
            uncommitted: 0,
        })
    }

    pub fn config(&self) -> &SearchIndexConfig {
        &self.config
    }

    /// Max row HLC durable in the index. Rows with a newer stamp must be
    /// replayed from the table after a restart.
    pub fn committed_watermark(&self) -> Option<Watermark> {
        self.committed_watermark
    }

    /// Index (or re-index) one row. Any previous posting for `key` is
    /// removed; a row with no indexable values is simply removed.
    pub fn put(&mut self, key: &[u8], doc: &Document, stamp: Watermark) -> Result<(), FtsError> {
        self.writer
            .delete_term(Term::from_field_bytes(self.key_field, key));
        let mut tdoc = TantivyDocument::default();
        tdoc.add_bytes(self.key_field, key);
        let mut any = false;
        for col in &self.columns {
            if let Some(value) = doc.get_path(&col.path) {
                any |= add_typed_values(&mut tdoc, col, value);
            }
        }
        if any {
            self.writer.add_document(tdoc)?;
        }
        self.note_write(stamp);
        Ok(())
    }

    /// Remove a row from the index.
    pub fn delete(&mut self, key: &[u8], stamp: Watermark) {
        self.writer
            .delete_term(Term::from_field_bytes(self.key_field, key));
        self.note_write(stamp);
    }

    fn note_write(&mut self, stamp: Watermark) {
        self.pending_watermark = Some(match self.pending_watermark {
            Some(w) => w.max(stamp),
            None => stamp,
        });
        self.uncommitted += 1;
    }

    /// Remove every document (start of a rebuild).
    pub fn clear(&mut self) -> Result<(), FtsError> {
        self.writer.delete_all_documents()?;
        self.uncommitted += 1;
        Ok(())
    }

    /// True if there are writes a commit would make durable/visible.
    pub fn dirty(&self) -> bool {
        self.uncommitted > 0
    }

    /// Make all applied writes durable and visible to searches, persisting
    /// the watermark atomically with the segments.
    pub fn commit(&mut self) -> Result<(), FtsError> {
        let watermark = match (self.pending_watermark, self.committed_watermark) {
            (Some(p), Some(c)) => Some(p.max(c)),
            (p, c) => p.or(c),
        };
        let mut prepared = self.writer.prepare_commit()?;
        if let Some(w) = watermark {
            let payload =
                serde_json::to_string(&w).map_err(|e| FtsError::Engine(e.to_string()))?;
            prepared.set_payload(&payload);
        }
        prepared.commit()?;
        self.reader.reload()?;
        self.committed_watermark = watermark;
        self.pending_watermark = None;
        self.uncommitted = 0;
        Ok(())
    }

    /// Top-k search: the `k` best-scoring rows, best first.
    pub fn search_top(&self, query: &SearchQuery, k: usize) -> Result<Vec<SearchHit>, FtsError> {
        let q = build_query(
            &self.index,
            &QueryFields {
                fields: &self.runtimes,
            },
            query,
        )?;
        let searcher = self.reader.searcher();
        let top = searcher.search(&q, &TopDocs::with_limit(k.max(1)).order_by_score())?;
        let mut hits = Vec::with_capacity(top.len());
        for (score, addr) in top {
            let doc: TantivyDocument = searcher.doc(addr)?;
            if let Some(key) = doc.get_first(self.key_field).and_then(|v| v.as_bytes()) {
                hits.push(SearchHit {
                    key: key.to_vec(),
                    score,
                });
            }
        }
        Ok(hits)
    }

    /// Unranked search: the keys of every matching row (for `MATCH` used as
    /// a pure predicate, no `ORDER BY score()`).
    pub fn search_keys(&self, query: &SearchQuery) -> Result<Vec<Vec<u8>>, FtsError> {
        let q = build_query(
            &self.index,
            &QueryFields {
                fields: &self.runtimes,
            },
            query,
        )?;
        let searcher = self.reader.searcher();
        let docs = searcher.search(&q, &DocSetCollector)?;
        let mut keys = Vec::with_capacity(docs.len());
        for addr in docs {
            let doc: TantivyDocument = searcher.doc(addr)?;
            if let Some(key) = doc.get_first(self.key_field).and_then(|v| v.as_bytes()) {
                keys.push(key.to_vec());
            }
        }
        Ok(keys)
    }

    /// Build a [`Highlighter`] for `query` over one text column: snippets of
    /// row text with the query's matching terms marked. Built once per query
    /// per column, then applied to each hit's authoritative row text.
    pub fn highlighter(
        &self,
        query: &SearchQuery,
        field: &str,
        max_chars: usize,
    ) -> Result<Highlighter, FtsError> {
        let rt = self
            .runtimes
            .iter()
            .find(|r| r.path == field)
            .ok_or_else(|| {
                FtsError::Config(format!(
                    "column '{field}' is not covered by the search index"
                ))
            })?;
        if !rt.ftype.is_texty() {
            return Err(FtsError::Config(format!(
                "HIGHLIGHT needs a text or keyword column, '{field}' is declared {:?}",
                rt.ftype
            )));
        }
        let q = build_query(
            &self.index,
            &QueryFields {
                fields: &self.runtimes,
            },
            query,
        )?;
        let searcher = self.reader.searcher();
        let mut generator = SnippetGenerator::create(&searcher, &*q, rt.field)?;
        generator.set_max_num_chars(max_chars.max(1));
        Ok(Highlighter { generator })
    }

    pub fn stats(&self) -> SearchIndexStats {
        let disk_bytes = std::fs::read_dir(&self.dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok()?.metadata().ok().map(|m| m.len()))
                    .sum()
            })
            .unwrap_or(0);
        SearchIndexStats {
            docs: self.reader.searcher().num_docs(),
            disk_bytes,
            uncommitted: self.uncommitted,
        }
    }
}

/// Generates highlighted snippets of row text for one (query, column) pair
/// (from [`SearchIndex::highlighter`]).
pub struct Highlighter {
    generator: SnippetGenerator,
}

impl fmt::Debug for Highlighter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Highlighter").finish_non_exhaustive()
    }
}

impl Highlighter {
    /// The best-scoring fragment of `text` with matching terms wrapped in
    /// `<b>…</b>` (HTML-escaped otherwise); empty string when nothing in
    /// `text` matches.
    pub fn snippet(&self, text: &str) -> String {
        let snippet = self.generator.snippet(text);
        if snippet.is_empty() {
            String::new()
        } else {
            snippet.to_html()
        }
    }

    /// [`Highlighter::snippet`] over every string reachable at `path` in
    /// the row (arrays are multi-valued fields), space-joined — the same
    /// text the index saw at write time.
    pub fn snippet_doc(&self, doc: &Document, path: &str) -> String {
        fn collect(v: &Value, out: &mut String) {
            match v {
                Value::String(s) => {
                    if !out.is_empty() {
                        out.push(' ');
                    }
                    out.push_str(s);
                }
                Value::Array(items) => items.iter().for_each(|item| collect(item, out)),
                _ => {}
            }
        }
        let mut text = String::new();
        if let Some(v) = doc.get_path(path) {
            collect(v, &mut text);
        }
        self.snippet(&text)
    }
}

/// Add `value` to the document according to the column's declared type,
/// recursing into arrays (multi-valued fields). Values that don't fit the
/// type are skipped — skaidb rows are schema-less, the declaration is the
/// mapping. Returns true if anything was added.
fn add_typed_values(tdoc: &mut TantivyDocument, col: &IndexedColumn, value: &Value) -> bool {
    if let Value::Array(items) = value {
        let mut added = false;
        for item in items {
            added |= add_typed_values(tdoc, col, item);
        }
        return added;
    }
    match (col.ftype, value) {
        (FieldType::Text | FieldType::Keyword, Value::String(s)) => {
            tdoc.add_text(col.field, s);
            if let Some(twin) = col.twin {
                tdoc.add_text(twin, s);
            }
            if let Some(target) = col.copy_to {
                tdoc.add_text(target, s);
            }
            true
        }
        (FieldType::Long, Value::Int(i)) => {
            tdoc.add_i64(col.field, *i);
            true
        }
        (FieldType::Double, Value::Float(x)) => {
            tdoc.add_f64(col.field, *x);
            true
        }
        (FieldType::Double, Value::Int(i)) => {
            tdoc.add_f64(col.field, *i as f64);
            true
        }
        (FieldType::Bool, Value::Bool(b)) => {
            tdoc.add_bool(col.field, *b);
            true
        }
        (FieldType::Date, Value::Timestamp(ms)) | (FieldType::Date, Value::Int(ms)) => {
            tdoc.add_date(col.field, DateTime::from_timestamp_millis(*ms));
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a config through the same path the engine uses.
    fn config(paths: &[&str], options: &[(&str, &str)]) -> SearchIndexConfig {
        let paths: Vec<String> = paths.iter().map(|s| s.to_string()).collect();
        let options: Vec<(String, String)> = options
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        SearchIndexConfig::from_declaration(&paths, &options)
            .expect("valid declaration")
            .0
    }

    fn doc(pairs: &[(&str, Value)]) -> Document {
        let mut d = Document::default();
        for (k, v) in pairs {
            d.0.insert(k.to_string(), v.clone());
        }
        d
    }

    fn text_doc(pairs: &[(&str, &str)]) -> Document {
        let mut d = Document::default();
        for (k, v) in pairs {
            d.0.insert(k.to_string(), Value::String(v.to_string()));
        }
        d
    }

    fn wm(n: u64) -> Watermark {
        Watermark {
            physical: n,
            logical: 0,
        }
    }

    fn open(dir: &Path, cfg: &SearchIndexConfig) -> SearchIndex {
        SearchIndex::open(dir, cfg, 0).expect("open index")
    }

    fn matches(field: &str, text: &str) -> SearchQuery {
        SearchQuery::Match {
            field: Some(field.into()),
            text: text.into(),
        }
    }

    #[test]
    fn put_commit_search_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["title", "body"], &[]);
        let mut idx = open(tmp.path(), &cfg);
        idx.put(
            b"k1",
            &text_doc(&[("title", "Rust database"), ("body", "fast full text search")]),
            wm(1),
        )
        .unwrap();
        idx.put(
            b"k2",
            &text_doc(&[("title", "Cooking"), ("body", "slow roasted vegetables")]),
            wm(2),
        )
        .unwrap();
        // Invisible before commit.
        assert!(idx
            .search_top(
                &SearchQuery::Match {
                    field: None,
                    text: "rust".into()
                },
                10
            )
            .unwrap()
            .is_empty());
        idx.commit().unwrap();

        let hits = idx
            .search_top(
                &SearchQuery::Match {
                    field: None,
                    text: "RUST search".into(),
                },
                10,
            )
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key, b"k1");
        assert_eq!(idx.committed_watermark(), Some(wm(2)));
        assert_eq!(idx.stats().docs, 2);
    }

    #[test]
    fn update_and_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["body"], &[]);
        let mut idx = open(tmp.path(), &cfg);
        idx.put(b"k1", &text_doc(&[("body", "old words")]), wm(1)).unwrap();
        idx.commit().unwrap();
        idx.put(b"k1", &text_doc(&[("body", "new words")]), wm(2)).unwrap();
        idx.commit().unwrap();
        assert!(idx.search_keys(&matches("body", "old")).unwrap().is_empty());
        assert_eq!(
            idx.search_keys(&matches("body", "new")).unwrap(),
            vec![b"k1".to_vec()]
        );

        idx.delete(b"k1", wm(3));
        idx.commit().unwrap();
        assert!(idx.search_keys(&matches("body", "new")).unwrap().is_empty());
        assert_eq!(idx.stats().docs, 0);
    }

    #[test]
    fn watermark_survives_reopen_uncommitted_writes_do_not() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["body"], &[]);
        {
            let mut idx = open(tmp.path(), &cfg);
            idx.put(b"k1", &text_doc(&[("body", "committed row")]), wm(10))
                .unwrap();
            idx.commit().unwrap();
            // Applied but never committed: a crash loses it.
            idx.put(b"k2", &text_doc(&[("body", "uncommitted row")]), wm(20))
                .unwrap();
            // Dropped without commit == kill -9 for durability purposes.
        }
        let idx = open(tmp.path(), &cfg);
        assert_eq!(idx.committed_watermark(), Some(wm(10)));
        assert_eq!(idx.stats().docs, 1);
        // The engine now replays table rows with hlc > wm(10).
    }

    #[test]
    fn schema_change_needs_rebuild() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut idx = open(tmp.path(), &config(&["body"], &[]));
            idx.put(b"k1", &text_doc(&[("body", "text")]), wm(1)).unwrap();
            idx.commit().unwrap();
        }
        // Different columns → rebuild.
        let err = SearchIndex::open(tmp.path(), &config(&["title"], &[]), 0)
            .expect_err("schema mismatch");
        assert!(matches!(err, FtsError::NeedsRebuild(_)));
        // Different analyzer → rebuild.
        let err =
            SearchIndex::open(tmp.path(), &config(&["body"], &[("analyzer", "english")]), 0)
                .expect_err("analyzer mismatch");
        assert!(matches!(err, FtsError::NeedsRebuild(_)));
        // Different type → rebuild.
        let err =
            SearchIndex::open(tmp.path(), &config(&["body"], &[("body.type", "keyword")]), 0)
                .expect_err("type mismatch");
        assert!(matches!(err, FtsError::NeedsRebuild(_)));
        // A query-time-only change (search_analyzer) is NOT a rebuild.
        let cfg = config(&["body"], &[("body.search_analyzer", "whitespace")]);
        assert!(SearchIndex::open(tmp.path(), &cfg, 0).is_ok());
    }

    #[test]
    fn english_analyzer_stems_and_drops_stopwords() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["body"], &[("analyzer", "english")]);
        let mut idx = open(tmp.path(), &cfg);
        idx.put(
            b"k1",
            &text_doc(&[("body", "the runner was running quickly")]),
            wm(1),
        )
        .unwrap();
        idx.commit().unwrap();
        // Query analyzed with the same pipeline: "runs" stems to "run".
        assert_eq!(idx.search_keys(&matches("body", "runs")).unwrap().len(), 1);
        // Stopword-only query matches nothing instead of everything.
        assert!(idx.search_keys(&matches("body", "the was")).unwrap().is_empty());
    }

    #[test]
    fn phrase_fuzzy_and_query_string() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["title", "body"], &[]);
        let mut idx = open(tmp.path(), &cfg);
        idx.put(
            b"k1",
            &text_doc(&[("title", "quick brown fox"), ("body", "jumps over the lazy dog")]),
            wm(1),
        )
        .unwrap();
        idx.put(
            b"k2",
            &text_doc(&[("title", "brown quick fox"), ("body", "unrelated")]),
            wm(2),
        )
        .unwrap();
        idx.commit().unwrap();

        // Exact phrase matches only k1; slop 2 lets the transposed k2 in.
        let phrase = |slop| SearchQuery::Phrase {
            field: Some("title".into()),
            text: "quick brown".into(),
            slop,
        };
        assert_eq!(idx.search_keys(&phrase(0)).unwrap(), vec![b"k1".to_vec()]);
        assert_eq!(idx.search_keys(&phrase(2)).unwrap().len(), 2);

        // Typo within distance 1.
        let hits = idx
            .search_top(
                &SearchQuery::Fuzzy {
                    field: None,
                    text: "quikc".into(),
                    distance: 1,
                },
                10,
            )
            .unwrap();
        assert_eq!(hits.len(), 2);

        // Query-string mini-language: required and excluded terms.
        let hits = idx
            .search_keys(&SearchQuery::QueryString("+fox -unrelated".into()))
            .unwrap();
        assert_eq!(hits, vec![b"k1".to_vec()]);

        // Field not in the index is a config error.
        let err = idx
            .search_keys(&matches("nope", "x"))
            .expect_err("unknown field");
        assert!(matches!(err, FtsError::Config(_)));
    }

    #[test]
    fn clear_supports_rebuild() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["body"], &[]);
        let mut idx = open(tmp.path(), &cfg);
        idx.put(b"k1", &text_doc(&[("body", "stale")]), wm(1)).unwrap();
        idx.commit().unwrap();
        idx.clear().unwrap();
        idx.put(b"k2", &text_doc(&[("body", "fresh")]), wm(2)).unwrap();
        idx.commit().unwrap();
        let all = idx
            .search_keys(&SearchQuery::QueryString("stale fresh".into()))
            .unwrap();
        assert_eq!(all, vec![b"k2".to_vec()]);
    }

    #[test]
    fn nested_paths_and_arrays() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["meta.tags"], &[]);
        let mut idx = open(tmp.path(), &cfg);
        let mut inner = Document::default();
        inner.0.insert(
            "tags".into(),
            Value::Array(vec![
                Value::String("alpha".into()),
                Value::String("beta".into()),
            ]),
        );
        let mut row = Document::default();
        row.0.insert("meta".into(), Value::Document(inner));
        idx.put(b"k1", &row, wm(1)).unwrap();
        idx.commit().unwrap();
        assert_eq!(
            idx.search_keys(&matches("meta.tags", "beta")).unwrap(),
            vec![b"k1".to_vec()]
        );
    }

    // ---- phase 2: typed fields, twins, copy_to, analyzer splits, boosts ----

    #[test]
    fn typed_fields_index_and_query_string_ranges() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(
            &["title", "year", "price", "published", "created"],
            &[
                ("year.type", "long"),
                ("price.type", "double"),
                ("published.type", "bool"),
                ("created.type", "date"),
            ],
        );
        let mut idx = open(tmp.path(), &cfg);
        idx.put(
            b"k1",
            &doc(&[
                ("title", Value::String("old book".into())),
                ("year", Value::Int(1999)),
                ("price", Value::Int(20)), // int coerces into a double field
                ("published", Value::Bool(true)),
                ("created", Value::Timestamp(946_684_800_000)),
            ]),
            wm(1),
        )
        .unwrap();
        idx.put(
            b"k2",
            &doc(&[
                ("title", Value::String("new book".into())),
                ("year", Value::Int(2024)),
                ("price", Value::Float(49.5)),
                ("published", Value::Bool(false)),
                ("created", Value::Timestamp(1_700_000_000_000)),
            ]),
            wm(2),
        )
        .unwrap();
        // A row whose value doesn't fit the declared type still indexes the
        // fields that do fit.
        idx.put(
            b"k3",
            &doc(&[
                ("title", Value::String("odd row".into())),
                ("year", Value::String("not a year".into())),
            ]),
            wm(3),
        )
        .unwrap();
        idx.commit().unwrap();

        // Typed fields are addressable from the query-string language.
        let q = |s: &str| SearchQuery::QueryString(s.into());
        assert_eq!(idx.search_keys(&q("year:[2000 TO 2030]")).unwrap(), vec![b"k2".to_vec()]);
        assert_eq!(idx.search_keys(&q("published:true")).unwrap(), vec![b"k1".to_vec()]);
        assert_eq!(idx.search_keys(&q("price:[30 TO *]")).unwrap(), vec![b"k2".to_vec()]);
        assert_eq!(idx.search_keys(&q("year:1999")).unwrap(), vec![b"k1".to_vec()]);
        assert_eq!(idx.search_keys(&matches("title", "odd")).unwrap(), vec![b"k3".to_vec()]);

        // MATCH on a numeric column is a clear error.
        let err = idx.search_keys(&matches("year", "1999")).expect_err("not texty");
        assert!(matches!(err, FtsError::Config(_)));
    }

    #[test]
    fn keyword_twin_exact_match() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["title"], &[("title.keyword", "true")]);
        let mut idx = open(tmp.path(), &cfg);
        idx.put(b"k1", &text_doc(&[("title", "Quick Brown Fox")]), wm(1)).unwrap();
        idx.put(b"k2", &text_doc(&[("title", "quick")]), wm(2)).unwrap();
        idx.commit().unwrap();
        // Analyzed field matches both rows on a term...
        assert_eq!(idx.search_keys(&matches("title", "quick")).unwrap().len(), 2);
        // ...the twin only on the exact original string.
        assert_eq!(
            idx.search_keys(&matches("title.keyword", "Quick Brown Fox")).unwrap(),
            vec![b"k1".to_vec()]
        );
        assert!(idx
            .search_keys(&matches("title.keyword", "quick brown fox"))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn copy_to_composite_field() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(
            &["title", "body"],
            &[("title.copy_to", "everything"), ("body.copy_to", "everything")],
        );
        let mut idx = open(tmp.path(), &cfg);
        idx.put(
            b"k1",
            &text_doc(&[("title", "alpha"), ("body", "beta")]),
            wm(1),
        )
        .unwrap();
        idx.commit().unwrap();
        // One composite field sees text from both columns.
        assert_eq!(idx.search_keys(&matches("everything", "alpha")).unwrap().len(), 1);
        assert_eq!(idx.search_keys(&matches("everything", "beta")).unwrap().len(), 1);
    }

    #[test]
    fn edge_ngram_with_search_analyzer_split() {
        let tmp = tempfile::tempdir().unwrap();
        // Search-as-you-type: prefixes at index time, whole terms at query
        // time.
        let cfg = config(
            &["name"],
            &[
                ("name.analyzer", "edge_ngram(2,10)"),
                ("name.search_analyzer", "standard"),
            ],
        );
        let mut idx = open(tmp.path(), &cfg);
        idx.put(b"k1", &text_doc(&[("name", "Elasticsearch")]), wm(1)).unwrap();
        idx.put(b"k2", &text_doc(&[("name", "Postgres")]), wm(2)).unwrap();
        idx.commit().unwrap();
        for prefix in ["el", "elas", "elastic"] {
            assert_eq!(
                idx.search_keys(&matches("name", prefix)).unwrap(),
                vec![b"k1".to_vec()],
                "prefix {prefix}"
            );
        }
        assert!(idx.search_keys(&matches("name", "search")).unwrap().is_empty());
    }

    #[test]
    fn per_field_boost_orders_multi_field_matches() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["title", "body"], &[("title.boost", "5.0")]);
        let mut idx = open(tmp.path(), &cfg);
        idx.put(
            b"body_hit",
            &text_doc(&[("title", "unrelated words"), ("body", "rust rust rust rust")]),
            wm(1),
        )
        .unwrap();
        idx.put(
            b"title_hit",
            &text_doc(&[("title", "rust handbook"), ("body", "nothing relevant")]),
            wm(2),
        )
        .unwrap();
        idx.commit().unwrap();
        let hits = idx
            .search_top(
                &SearchQuery::Match {
                    field: None,
                    text: "rust".into(),
                },
                10,
            )
            .unwrap();
        assert_eq!(hits.len(), 2);
        // The boosted title match outranks the repeated body match.
        assert_eq!(hits[0].key, b"title_hit");
    }

    // ---- phase 3: prefix/wildcard/regexp, NOT composition, highlighting ----

    /// `body` over three rows for the term-level pattern queries.
    fn pattern_index(dir: &std::path::Path) -> SearchIndex {
        let cfg = config(&["body"], &[]);
        let mut idx = open(dir, &cfg);
        idx.put(b"k1", &text_doc(&[("body", "quick brown fox")]), wm(1)).unwrap();
        idx.put(b"k2", &text_doc(&[("body", "quiet quality street")]), wm(2)).unwrap();
        idx.put(b"k3", &text_doc(&[("body", "slow red panda")]), wm(3)).unwrap();
        idx.commit().unwrap();
        idx
    }

    #[test]
    fn prefix_wildcard_and_regexp() {
        let tmp = tempfile::tempdir().unwrap();
        let idx = pattern_index(tmp.path());
        let sorted = |mut keys: Vec<Vec<u8>>| {
            keys.sort();
            keys
        };
        // Prefix runs against indexed (lowercased) terms.
        let hits = idx
            .search_keys(&SearchQuery::Prefix {
                field: Some("body".into()),
                text: "qui".into(),
            })
            .unwrap();
        assert_eq!(sorted(hits), vec![b"k1".to_vec(), b"k2".to_vec()]);
        // The prefix itself is a literal: regex metacharacters must not leak.
        assert!(idx
            .search_keys(&SearchQuery::Prefix {
                field: Some("body".into()),
                text: "qu.".into(),
            })
            .unwrap()
            .is_empty());
        // Wildcards: `*` any run, `?` exactly one char.
        let hits = idx
            .search_keys(&SearchQuery::Wildcard {
                field: Some("body".into()),
                pattern: "qu*k".into(),
            })
            .unwrap();
        assert_eq!(hits, vec![b"k1".to_vec()]);
        let hits = idx
            .search_keys(&SearchQuery::Wildcard {
                field: Some("body".into()),
                pattern: "qui?t".into(),
            })
            .unwrap();
        assert_eq!(hits, vec![b"k2".to_vec()]);
        // Full regex over indexed terms.
        let hits = idx
            .search_keys(&SearchQuery::Regexp {
                field: Some("body".into()),
                pattern: "qu(ick|iet)".into(),
            })
            .unwrap();
        assert_eq!(sorted(hits), vec![b"k1".to_vec(), b"k2".to_vec()]);
        // A broken regex is a config (user) error.
        let err = idx
            .search_keys(&SearchQuery::Regexp {
                field: Some("body".into()),
                pattern: "qu(".into(),
            })
            .expect_err("bad regex");
        assert!(matches!(err, FtsError::Config(_)));
    }

    #[test]
    fn not_and_bool_composition() {
        let tmp = tempfile::tempdir().unwrap();
        let idx = pattern_index(tmp.path());
        // NOT excludes matching rows; the rest of the index comes back.
        let mut hits = idx
            .search_keys(&SearchQuery::Not(Box::new(matches("body", "quick"))))
            .unwrap();
        hits.sort();
        assert_eq!(hits, vec![b"k2".to_vec(), b"k3".to_vec()]);
        // AND of a positive and a negative: quick-or-quiet rows minus fox rows.
        let hits = idx
            .search_keys(&SearchQuery::All(vec![
                SearchQuery::Any(vec![matches("body", "quick"), matches("body", "quiet")]),
                SearchQuery::Not(Box::new(matches("body", "fox"))),
            ]))
            .unwrap();
        assert_eq!(hits, vec![b"k2".to_vec()]);
    }

    #[test]
    fn highlighter_marks_matches() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["body"], &[("analyzer", "english")]);
        let mut idx = open(tmp.path(), &cfg);
        idx.put(
            b"k1",
            &text_doc(&[("body", "the quick brown fox jumps over the lazy dog")]),
            wm(1),
        )
        .unwrap();
        idx.commit().unwrap();
        let query = matches("body", "jumping foxes");
        let h = idx.highlighter(&query, "body", 60).unwrap();
        // Stemmed query terms highlight the row's original inflections.
        let snippet = h.snippet("the quick brown fox jumps over the lazy dog");
        assert!(snippet.contains("<b>fox</b>"), "{snippet}");
        assert!(snippet.contains("<b>jumps</b>"), "{snippet}");
        // Non-matching text yields no snippet.
        assert_eq!(h.snippet("completely unrelated words"), "");
        // Unknown or non-text columns are config errors.
        assert!(matches!(
            idx.highlighter(&query, "nope", 60),
            Err(FtsError::Config(_))
        ));
    }
}
