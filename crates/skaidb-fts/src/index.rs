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
    BytesOptions, Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions,
    Value as _,
};
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term};

use crate::query::{build_query, QueryFields};
use crate::{analyzer, FtsError, SearchHit, SearchIndexConfig, SearchQuery, Watermark};

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

/// A live full-text index over one table.
pub struct SearchIndex {
    dir: PathBuf,
    config: SearchIndexConfig,
    index: Index,
    writer: IndexWriter,
    reader: IndexReader,
    key_field: Field,
    /// `(dotted path, tantivy field)` for each indexed column.
    fields: Vec<(String, Field)>,
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

fn build_schema(config: &SearchIndexConfig) -> Schema {
    let mut builder = Schema::builder();
    builder.add_bytes_field(
        KEY_FIELD,
        BytesOptions::default().set_indexed().set_stored(),
    );
    let text = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(config.analyzer.tokenizer_name())
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    for path in &config.fields {
        builder.add_text_field(path, text.clone());
    }
    builder.build()
}

impl SearchIndex {
    /// Open the index at `dir`, creating it if the directory is empty. If an
    /// existing index does not match `config` (columns or analyzer changed),
    /// returns [`FtsError::NeedsRebuild`] — the caller wipes the directory
    /// and rebuilds from the table.
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
        analyzer::register_all(&index);

        let key_field = schema.get_field(KEY_FIELD).expect("schema owns _key");
        let fields = config
            .fields
            .iter()
            .map(|p| (p.clone(), schema.get_field(p).expect("schema owns field")))
            .collect();

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
            fields,
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
    /// removed; a row with no indexed text is simply removed.
    pub fn put(&mut self, key: &[u8], doc: &Document, stamp: Watermark) -> Result<(), FtsError> {
        self.writer
            .delete_term(Term::from_field_bytes(self.key_field, key));
        let mut tdoc = TantivyDocument::default();
        tdoc.add_bytes(self.key_field, key);
        let mut has_text = false;
        for (path, field) in &self.fields {
            if let Some(value) = doc.get_path(path) {
                has_text |= add_text_values(&mut tdoc, *field, value);
            }
        }
        if has_text {
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
                fields: &self.fields,
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
                fields: &self.fields,
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

/// Add every string reachable in `value` to the document (a string, an array
/// of strings, arrays of arrays, ...). Non-string scalars are not indexed in
/// phase 1 (numeric/date/bool fast fields are phase 2). Returns true if
/// anything was added.
fn add_text_values(tdoc: &mut TantivyDocument, field: Field, value: &Value) -> bool {
    match value {
        Value::String(s) => {
            tdoc.add_text(field, s);
            true
        }
        Value::Array(items) => {
            let mut added = false;
            for item in items {
                added |= add_text_values(tdoc, field, item);
            }
            added
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Analyzer;

    fn config(fields: &[&str], analyzer: Analyzer) -> SearchIndexConfig {
        SearchIndexConfig {
            fields: fields.iter().map(|s| s.to_string()).collect(),
            analyzer,
        }
    }

    fn doc(pairs: &[(&str, &str)]) -> Document {
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

    #[test]
    fn put_commit_search_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["title", "body"], Analyzer::Standard);
        let mut idx = open(tmp.path(), &cfg);
        idx.put(
            b"k1",
            &doc(&[("title", "Rust database"), ("body", "fast full text search")]),
            wm(1),
        )
        .unwrap();
        idx.put(
            b"k2",
            &doc(&[("title", "Cooking"), ("body", "slow roasted vegetables")]),
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
        let cfg = config(&["body"], Analyzer::Standard);
        let mut idx = open(tmp.path(), &cfg);
        idx.put(b"k1", &doc(&[("body", "old words")]), wm(1)).unwrap();
        idx.commit().unwrap();
        idx.put(b"k1", &doc(&[("body", "new words")]), wm(2)).unwrap();
        idx.commit().unwrap();
        let q = |t: &str| SearchQuery::Match {
            field: None,
            text: t.into(),
        };
        assert!(idx.search_keys(&q("old")).unwrap().is_empty());
        assert_eq!(idx.search_keys(&q("new")).unwrap(), vec![b"k1".to_vec()]);

        idx.delete(b"k1", wm(3));
        idx.commit().unwrap();
        assert!(idx.search_keys(&q("new")).unwrap().is_empty());
        assert_eq!(idx.stats().docs, 0);
    }

    #[test]
    fn watermark_survives_reopen_uncommitted_writes_do_not() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["body"], Analyzer::Standard);
        {
            let mut idx = open(tmp.path(), &cfg);
            idx.put(b"k1", &doc(&[("body", "committed row")]), wm(10))
                .unwrap();
            idx.commit().unwrap();
            // Applied but never committed: a crash loses it.
            idx.put(b"k2", &doc(&[("body", "uncommitted row")]), wm(20))
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
            let mut idx = open(tmp.path(), &config(&["body"], Analyzer::Standard));
            idx.put(b"k1", &doc(&[("body", "text")]), wm(1)).unwrap();
            idx.commit().unwrap();
        }
        // Different columns → rebuild.
        let err = SearchIndex::open(tmp.path(), &config(&["title"], Analyzer::Standard), 0)
            .expect_err("schema mismatch");
        assert!(matches!(err, FtsError::NeedsRebuild(_)));
        // Different analyzer → rebuild.
        let err = SearchIndex::open(tmp.path(), &config(&["body"], Analyzer::English), 0)
            .expect_err("analyzer mismatch");
        assert!(matches!(err, FtsError::NeedsRebuild(_)));
    }

    #[test]
    fn english_analyzer_stems_and_drops_stopwords() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["body"], Analyzer::English);
        let mut idx = open(tmp.path(), &cfg);
        idx.put(b"k1", &doc(&[("body", "the runner was running quickly")]), wm(1))
            .unwrap();
        idx.commit().unwrap();
        let q = |t: &str| SearchQuery::Match {
            field: None,
            text: t.into(),
        };
        // Query analyzed with the same pipeline: "runs" stems to "run".
        assert_eq!(idx.search_keys(&q("runs")).unwrap().len(), 1);
        // Stopword-only query matches nothing instead of everything.
        assert!(idx.search_keys(&q("the was")).unwrap().is_empty());
    }

    #[test]
    fn phrase_fuzzy_and_query_string() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["title", "body"], Analyzer::Standard);
        let mut idx = open(tmp.path(), &cfg);
        idx.put(
            b"k1",
            &doc(&[("title", "quick brown fox"), ("body", "jumps over the lazy dog")]),
            wm(1),
        )
        .unwrap();
        idx.put(
            b"k2",
            &doc(&[("title", "brown quick fox"), ("body", "unrelated")]),
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
            .search_keys(&SearchQuery::Match {
                field: Some("nope".into()),
                text: "x".into(),
            })
            .expect_err("unknown field");
        assert!(matches!(err, FtsError::Config(_)));
    }

    #[test]
    fn clear_supports_rebuild() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["body"], Analyzer::Standard);
        let mut idx = open(tmp.path(), &cfg);
        idx.put(b"k1", &doc(&[("body", "stale")]), wm(1)).unwrap();
        idx.commit().unwrap();
        idx.clear().unwrap();
        idx.put(b"k2", &doc(&[("body", "fresh")]), wm(2)).unwrap();
        idx.commit().unwrap();
        let all = idx.search_keys(&SearchQuery::QueryString("stale fresh".into())).unwrap();
        assert_eq!(all, vec![b"k2".to_vec()]);
    }

    #[test]
    fn nested_paths_and_arrays() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = config(&["meta.tags"], Analyzer::Standard);
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
            idx.search_keys(&SearchQuery::Match {
                field: Some("meta.tags".into()),
                text: "beta".into()
            })
            .unwrap(),
            vec![b"k1".to_vec()]
        );
    }
}
