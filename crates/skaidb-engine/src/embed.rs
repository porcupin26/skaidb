//! Text-embedding provider abstraction for **managed vector indexes**
//! (`CREATE VECTOR INDEX … ON t(text_col) EMBED`).
//!
//! An [`Embedder`] turns text into fixed-dimension vectors. The engine holds
//! one behind `Arc<dyn Embedder>`; the real HTTP client (an OpenAI/TEI-style
//! endpoint) lives in the server so the engine stays transport-free and
//! testable. Embedders call an external model server and must **never** run on
//! the write/read hot path — only the background embedding worker and
//! query-time auto-embed invoke them, so an endpoint being down delays
//! searchability but never blocks or fails a write.

use std::fmt::Debug;

use crate::error::{EngineError, Result};

/// Embeds text into `dim()`-length vectors.
pub trait Embedder: Send + Sync + Debug {
    /// Embed a batch of texts, one vector each, in order. Errors (endpoint
    /// down, bad response, dimension mismatch) propagate so the caller can
    /// retry later without losing data.
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;
    /// The dimension of every returned vector.
    fn dim(&self) -> usize;
}

/// Second-stage relevance scoring for `RERANK` (docs/SEARCH.md "Reranking").
///
/// A [`Reranker`] scores each candidate document's relevance to a query with a
/// cross-encoder model. Like [`Embedder`], the engine holds one behind
/// `Arc<dyn Reranker>` and the real HTTP client (a Cohere/Jina/TEI-style
/// rerank endpoint) lives in the server. Only the opt-in `RERANK` query
/// clause invokes it — never the write path, and never a query that didn't
/// ask for it — so a rerank endpoint being down fails only rerank queries.
pub trait Reranker: Send + Sync + Debug {
    /// Score `documents` against `query`, one score per document in order
    /// (higher = more relevant). `model` overrides the endpoint's configured
    /// default when non-empty.
    fn rerank(&self, model: &str, query: &str, documents: &[String]) -> Result<Vec<f32>>;
}

/// A deterministic, dependency-free reranker: each document scores the
/// fraction of the query's distinct (lowercased, alphanumeric) tokens it
/// contains. **Not semantic**, but deterministic and offline — the reranker
/// for tests, demos, and air-gapped setups, like [`HashEmbedder`].
#[derive(Debug, Clone, Default)]
pub struct OverlapReranker;

impl Reranker for OverlapReranker {
    fn rerank(&self, _model: &str, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        let tokens = |text: &str| -> Vec<String> {
            let mut v: Vec<String> = text
                .split(|c: char| !c.is_alphanumeric())
                .filter(|t| !t.is_empty())
                .map(|t| t.to_ascii_lowercase())
                .collect();
            v.sort();
            v.dedup();
            v
        };
        let q = tokens(query);
        Ok(documents
            .iter()
            .map(|d| {
                if q.is_empty() {
                    return 0.0;
                }
                let dt = tokens(d);
                let hits = q.iter().filter(|t| dt.binary_search(t).is_ok()).count();
                hits as f32 / q.len() as f32
            })
            .collect())
    }
}

/// A deterministic, dependency-free embedder: a token-hashing bag-of-words
/// projected onto `dim` axes and L2-normalized. **Not semantic** — texts that
/// share words get similar vectors, texts that don't are near-orthogonal — but
/// deterministic and offline, which makes it the embedder for tests, demos, and
/// air-gapped setups. Same text always yields the same vector.
#[derive(Debug, Clone)]
pub struct HashEmbedder {
    dim: usize,
}

impl HashEmbedder {
    pub fn new(dim: usize) -> Self {
        HashEmbedder { dim: dim.max(1) }
    }

    /// The vector for one text (public so callers can seed comparisons).
    pub fn embed_one(&self, text: &str) -> Vec<f32> {
        let mut v = vec![0.0f32; self.dim];
        for tok in text.split(|c: char| !c.is_alphanumeric()).filter(|t| !t.is_empty()) {
            // FNV-1a over the lowercased token.
            let mut h: u64 = 0xcbf2_9ce4_8422_2325;
            for b in tok.to_ascii_lowercase().bytes() {
                h ^= u64::from(b);
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
            let idx = (h % self.dim as u64) as usize;
            v[idx] += 1.0;
        }
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in &mut v {
                *x /= norm;
            }
        }
        v
    }
}

impl Embedder for HashEmbedder {
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        Ok(texts.iter().map(|t| self.embed_one(t)).collect())
    }
    fn dim(&self) -> usize {
        self.dim
    }
}

/// Validate that an embedder's output matches an index's declared dimension —
/// a clear error beats a silent dimension mismatch deep in the HNSW.
pub(crate) fn check_dim(vectors: &[Vec<f32>], expected: usize) -> Result<()> {
    for v in vectors {
        if v.len() != expected {
            return Err(EngineError::Type(format!(
                "embedder returned a {}-dim vector; index expects {expected} \
                 (check inference.dim vs the index DIM)",
                v.len()
            )));
        }
    }
    Ok(())
}
