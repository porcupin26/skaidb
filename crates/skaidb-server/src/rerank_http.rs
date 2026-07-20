//! HTTP rerank client — the server-side [`skaidb_engine::Reranker`] behind
//! the `RERANK` query clause. Calls a Cohere/Jina/TEI-compatible cross-encoder
//! endpoint:
//!
//! ```text
//! POST <rerank_url>  {"model": <model>, "query": <q>,
//!                     "documents": [<texts>], "texts": [<texts>]}
//!   -> {"results": [{"index": i, "relevance_score": f}, ...]}   (Cohere/Jina)
//!   -> [{"index": i, "score": f}, ...]                          (TEI)
//! ```
//!
//! The candidate texts are sent under both `documents` (Cohere/Jina) and
//! `texts` (TEI) so one wire shape serves every common endpoint; both response
//! shapes are accepted. Shares the transport (TLS, auth, timeout) with the
//! embeddings client. Invoked only by opt-in `RERANK` queries,
//! coordinator-side — never on the write path.

use skaidb_config::InferenceConfig;
use skaidb_engine::{EngineError, Reranker};

use crate::embed_http::Endpoint;

fn err(msg: impl Into<String>) -> EngineError {
    EngineError::Unsupported(format!("inference: {}", msg.into()))
}

/// A rerank endpoint reachable over HTTP(S).
#[derive(Debug)]
pub struct HttpReranker {
    endpoint: Endpoint,
    /// Default model when the query has no `RERANK WITH '<model>'`.
    model: String,
}

impl HttpReranker {
    /// Build from `[inference] rerank_url`. Errors on a malformed URL or TLS
    /// material.
    pub fn from_config(c: &InferenceConfig) -> Result<Self, String> {
        Ok(HttpReranker {
            endpoint: Endpoint::from_config(&c.rerank_url, c)
                .map_err(|e| format!("rerank_url: {e}"))?,
            model: c.rerank_model.clone(),
        })
    }
}

impl Reranker for HttpReranker {
    fn rerank(&self, model: &str, query: &str, documents: &[String]) -> Result<Vec<f32>, EngineError> {
        if documents.is_empty() {
            return Ok(Vec::new());
        }
        let model = if model.is_empty() { &self.model } else { model };
        let body = serde_json::json!({
            "model": model,
            "query": query,
            "documents": documents,
            "texts": documents,
        })
        .to_string();
        let json = self.endpoint.post_json(&body, "rerank")?;
        // Cohere/Jina wrap the ranked list in `results`; TEI returns it bare.
        let results = json
            .get("results")
            .and_then(|r| r.as_array())
            .or_else(|| json.as_array())
            .ok_or_else(|| err("rerank response has no `results` array"))?;
        let mut scores = vec![0.0f32; documents.len()];
        for item in results {
            let idx = item
                .get("index")
                .and_then(|i| i.as_u64())
                .ok_or_else(|| err("a rerank result has no `index`"))?;
            let score = item
                .get("relevance_score")
                .or_else(|| item.get("score"))
                .and_then(|s| s.as_f64())
                .ok_or_else(|| err("a rerank result has no `relevance_score`/`score`"))?;
            let slot = scores
                .get_mut(idx as usize)
                .ok_or_else(|| err(format!("rerank result index {idx} out of range")))?;
            *slot = score as f32;
        }
        Ok(scores)
    }
}
