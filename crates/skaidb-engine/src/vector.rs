//! A minimal HNSW index for approximate nearest-neighbor (ANN) search over
//! float vectors — the index family used for embeddings / semantic search.
//!
//! HNSW (Hierarchical Navigable Small World) is a layered proximity graph: a
//! search greedily descends from a sparse top layer to the dense base layer,
//! following edges toward the query. It gives high recall at a fraction of a
//! brute-force scan's cost. This implementation supports cosine / L2 / dot
//! metrics, soft deletes, and **filtered** search (a predicate prunes which
//! nodes may appear in the result while the graph is still traversed for
//! connectivity), which is what makes "nearest neighbors WHERE …" possible.
//!
//! It is intentionally compact (simple neighbor selection, in-memory) — a
//! prototype, not a tuned production index.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

/// Distance metric. Smaller is always "closer" in the internal ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    /// `1 - cosine_similarity` (vectors are normalized on insert).
    Cosine,
    /// Squared Euclidean distance.
    L2,
    /// Negative dot product (larger dot = closer).
    Dot,
}

impl Metric {
    pub fn parse(s: &str) -> Option<Metric> {
        match s.to_ascii_lowercase().as_str() {
            "cosine" | "cos" => Some(Metric::Cosine),
            "l2" | "euclidean" => Some(Metric::L2),
            "dot" | "ip" | "inner" => Some(Metric::Dot),
            _ => None,
        }
    }
}

/// A float wrapper with a total order, so distances can live in a heap.
#[derive(Clone, Copy, PartialEq)]
struct Dist(f32);
impl Eq for Dist {}
impl PartialOrd for Dist {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Dist {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Debug)]
struct Node {
    vector: Vec<f32>,
    key: Vec<u8>,
    /// Neighbor internal ids per layer; `neighbors[l]` is this node's layer-`l` adjacency.
    neighbors: Vec<Vec<u32>>,
    deleted: bool,
}

/// An in-memory HNSW index mapping external row keys to vectors.
#[derive(Debug)]
pub struct Hnsw {
    metric: Metric,
    dim: usize,
    m: usize,               // target neighbors per layer (>0)
    m0: usize,              // target neighbors at layer 0
    ef_construction: usize,
    ef_search: usize,
    ml: f64,                // level-generation factor
    nodes: Vec<Node>,
    by_key: HashMap<Vec<u8>, u32>,
    entry: Option<u32>,
    rng: u64,
    /// Mutated since the last snapshot save (never persisted itself).
    dirty: bool,
}

impl Hnsw {
    pub fn new(metric: Metric, dim: usize) -> Hnsw {
        Hnsw::with_params(metric, dim, 16, 200, 64)
    }

    /// Retune the search-time candidate-list size (`ef`) live — a pure
    /// query-time knob: higher = better recall, slower queries. Build-time
    /// parameters (`m`, `ef_construction`) shape the graph and need a
    /// rebuild.
    pub fn set_ef_search(&mut self, ef: usize) {
        self.ef_search = ef.max(1);
    }

    pub fn with_params(
        metric: Metric,
        dim: usize,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> Hnsw {
        Hnsw {
            metric,
            dim,
            m: m.max(2),
            m0: m.max(2) * 2,
            ef_construction: ef_construction.max(m),
            ef_search: ef_search.max(1),
            ml: 1.0 / (m.max(2) as f64).ln(),
            nodes: Vec::new(),
            by_key: HashMap::new(),
            entry: None,
            rng: 0x2545_f491_4f6c_dd1d,
            dirty: false,
        }
    }

    pub fn len(&self) -> usize {
        self.by_key.len()
    }
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Insert or replace the vector for `key`. A replace soft-deletes the old
    /// node and inserts a fresh one (HNSW can't cheaply move a node).
    pub fn insert(&mut self, key: Vec<u8>, vector: Vec<f32>) {
        if vector.len() != self.dim {
            return; // dimension mismatch: ignore (caller validates)
        }
        if self.by_key.contains_key(&key) {
            self.remove(&key);
        }
        self.dirty = true;
        let vector = self.prepared(vector);
        let level = self.random_level();
        let id = self.nodes.len() as u32;
        self.nodes.push(Node {
            vector,
            key: key.clone(),
            neighbors: vec![Vec::new(); level + 1],
            deleted: false,
        });
        self.by_key.insert(key, id);

        let Some(entry) = self.entry else {
            self.entry = Some(id);
            return;
        };

        // Descend from the top layer to just above the new node's top layer.
        let mut cur = entry;
        let entry_level = self.nodes[entry as usize].neighbors.len() - 1;
        for l in (level + 1..=entry_level).rev() {
            // Same small-beam descent as search(): pure greedy can land the
            // connect phase in the wrong region on clustered data.
            let v = self.nodes[id as usize].vector.clone();
            let cand = self.search_layer(&v, cur, 8, l);
            if let Some(&(_, nearest)) = cand.first() {
                cur = nearest;
            }
        }

        // Connect at each layer from the new node's top down to 0.
        let top = level.min(entry_level);
        for l in (0..=top).rev() {
            let cand = self.search_layer(&self.nodes[id as usize].vector.clone(), cur, self.ef_construction, l);
            let max = if l == 0 { self.m0 } else { self.m };
            let chosen = self.select_diverse(&cand, max);
            if let Some(&(_, nearest)) = cand.first() {
                cur = nearest;
            }
            for &nb in &chosen {
                self.nodes[id as usize].neighbors[l].push(nb);
                self.nodes[nb as usize].neighbors[l].push(id);
                self.prune(nb, l, max);
            }
        }
        if level > entry_level {
            self.entry = Some(id);
        }
    }

    /// Soft-delete `key` (it stays in the graph for connectivity but is excluded
    /// from results).
    pub fn remove(&mut self, key: &[u8]) {
        if let Some(&id) = self.by_key.get(key) {
            self.nodes[id as usize].deleted = true;
            self.by_key.remove(key);
            self.dirty = true;
        }
    }

    /// The `k` nearest live keys to `query` that satisfy `keep`, nearest first,
    /// as `(key, distance)`. `keep` is evaluated on candidates surfaced by the
    /// graph search; the graph is still traversed through filtered-out nodes.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        keep: impl Fn(&[u8]) -> bool,
    ) -> Vec<(Vec<u8>, f32)> {
        if query.len() != self.dim || k == 0 {
            return Vec::new();
        }
        let Some(entry) = self.entry else {
            return Vec::new();
        };
        let q = self.prepared(query.to_vec());

        let mut cur = entry;
        let entry_level = self.nodes[entry as usize].neighbors.len() - 1;
        for l in (1..=entry_level).rev() {
            // A small beam (not pure greedy) at the upper layers: greedy
            // descent gets trapped in local minima on clustered data and
            // hands layer 0 an entry point in the wrong region.
            let cand = self.search_layer(&q, cur, 8, l);
            if let Some(&(_, nearest)) = cand.first() {
                cur = nearest;
            }
        }
        let ef = self.ef_search.max(k);
        let cand = self.search_layer(&q, cur, ef, 0);

        let mut out = Vec::with_capacity(k);
        for (Dist(d), id) in cand {
            let node = &self.nodes[id as usize];
            if node.deleted {
                continue;
            }
            if keep(&node.key) {
                out.push((node.key.clone(), d));
                if out.len() == k {
                    break;
                }
            }
        }
        out
    }

    // ---- internals ----

    /// Normalize for cosine; pass through otherwise.
    fn prepared(&self, mut v: Vec<f32>) -> Vec<f32> {
        if self.metric == Metric::Cosine {
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                for x in &mut v {
                    *x /= norm;
                }
            }
        }
        v
    }

    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.metric {
            Metric::L2 => a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum(),
            Metric::Dot => -a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>(),
            Metric::Cosine => 1.0 - a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>(),
        }
    }

    fn random_level(&mut self) -> usize {
        // xorshift64* -> uniform in (0,1], then exponential level distribution.
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        let u = ((self.rng.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 11) as f64)
            / ((1u64 << 53) as f64);
        let u = u.max(f64::MIN_POSITIVE);
        (-u.ln() * self.ml).floor() as usize
    }

    fn search_layer(&self, query: &[f32], entry: u32, ef: usize, layer: usize) -> Vec<(Dist, u32)> {
        let mut visited: HashMap<u32, ()> = HashMap::new();
        // `candidates` is a min-heap (closest first) via Reverse; `result` is a
        // max-heap (farthest first) capped at `ef`.
        let mut candidates: BinaryHeap<std::cmp::Reverse<(Dist, u32)>> = BinaryHeap::new();
        let mut result: BinaryHeap<(Dist, u32)> = BinaryHeap::new();

        let d0 = Dist(self.distance(query, &self.nodes[entry as usize].vector));
        candidates.push(std::cmp::Reverse((d0, entry)));
        result.push((d0, entry));
        visited.insert(entry, ());

        while let Some(std::cmp::Reverse((cd, cur))) = candidates.pop() {
            // If the closest candidate is farther than the current worst result
            // and the result set is full, we're done.
            if let Some(&(worst, _)) = result.peek() {
                if cd > worst && result.len() >= ef {
                    break;
                }
            }
            for &nb in &self.nodes[cur as usize].neighbors[layer] {
                if visited.insert(nb, ()).is_some() {
                    continue;
                }
                let d = Dist(self.distance(query, &self.nodes[nb as usize].vector));
                let worst = result.peek().map(|&(w, _)| w);
                if result.len() < ef || worst.is_none_or(|w| d < w) {
                    candidates.push(std::cmp::Reverse((d, nb)));
                    result.push((d, nb));
                    if result.len() > ef {
                        result.pop();
                    }
                }
            }
        }
        let mut out: Vec<(Dist, u32)> = result.into_vec();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Trim node `id`'s layer-`layer` adjacency to its `max` nearest neighbors.
    fn prune(&mut self, id: u32, layer: usize, max: usize) {
        if self.nodes[id as usize].neighbors[layer].len() <= max {
            return;
        }
        let v = self.nodes[id as usize].vector.clone();
        let mut scored: Vec<(Dist, u32)> = self.nodes[id as usize].neighbors[layer]
            .iter()
            .map(|&nb| (Dist(self.distance(&v, &self.nodes[nb as usize].vector)), nb))
            .collect();
        scored.sort_by(|a, b| a.0.cmp(&b.0));
        scored.dedup_by_key(|(_, nb)| *nb);
        self.nodes[id as usize].neighbors[layer] = self.select_diverse(&scored, max);
    }

    /// Neighbor selection with the diversity heuristic (Malkov & Yashunin,
    /// Algorithm 4): from `cand` (sorted nearest-first relative to the base
    /// node), keep a candidate only if it is closer to the base than to every
    /// neighbor already kept. Closest-only selection collapses on clustered
    /// data — dense islands wire exclusively to each other, long-range links
    /// vanish, and whole regions become unreachable. Discarded candidates
    /// backfill remaining slots so degree stays at `max` when possible.
    fn select_diverse(&self, cand: &[(Dist, u32)], max: usize) -> Vec<u32> {
        let mut kept: Vec<(Dist, u32)> = Vec::with_capacity(max);
        let mut spilled: Vec<u32> = Vec::new();
        for &(d, c) in cand {
            if kept.len() >= max {
                break;
            }
            let cv = &self.nodes[c as usize].vector;
            let diverse = kept
                .iter()
                .all(|&(_, k)| Dist(self.distance(cv, &self.nodes[k as usize].vector)) >= d);
            if diverse {
                kept.push((d, c));
            } else {
                spilled.push(c);
            }
        }
        let mut out: Vec<u32> = kept.into_iter().map(|(_, c)| c).collect();
        for c in spilled {
            if out.len() >= max {
                break;
            }
            out.push(c);
        }
        out
    }
}

impl Hnsw {
    /// Whether the graph changed since the last [`Hnsw::mark_clean`].
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Clear the dirty flag (call after a successful snapshot save).
    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }

    /// Snapshot the whole graph to a byte stream: format `SKHNSW01`, all
    /// parameters, every node (key, tombstone, adjacency, raw f32 vector) —
    /// enough to resume exactly where construction left off, `rng` included
    /// (levels of future inserts stay deterministic across a reload, so
    /// replicas that build vs. load converge on identical graphs).
    pub fn write_to(&self, w: &mut impl std::io::Write) -> std::io::Result<()> {
        w.write_all(b"SKHNSW01")?;
        w.write_all(&[match self.metric {
            Metric::Cosine => 0u8,
            Metric::L2 => 1,
            Metric::Dot => 2,
        }])?;
        for v in [
            self.dim as u64,
            self.m as u64,
            self.m0 as u64,
            self.ef_construction as u64,
            self.ef_search as u64,
        ] {
            w.write_all(&v.to_le_bytes())?;
        }
        w.write_all(&self.ml.to_le_bytes())?;
        w.write_all(&self.rng.to_le_bytes())?;
        w.write_all(&self.entry.map_or(u32::MAX, |e| e).to_le_bytes())?;
        w.write_all(&(self.nodes.len() as u32).to_le_bytes())?;
        for n in &self.nodes {
            w.write_all(&(n.key.len() as u32).to_le_bytes())?;
            w.write_all(&n.key)?;
            w.write_all(&[u8::from(n.deleted)])?;
            w.write_all(&(n.neighbors.len() as u32).to_le_bytes())?;
            for level in &n.neighbors {
                w.write_all(&(level.len() as u32).to_le_bytes())?;
                for &nb in level {
                    w.write_all(&nb.to_le_bytes())?;
                }
            }
            // Vector length is `dim` by construction; write raw f32 LE.
            for &x in &n.vector {
                w.write_all(&x.to_le_bytes())?;
            }
        }
        Ok(())
    }

    /// Rebuild a graph from a [`Hnsw::write_to`] stream. `by_key` is derived;
    /// any structural mismatch (bad magic, truncation) is an error — the
    /// caller falls back to a full rebuild from the table.
    pub fn read_from(r: &mut impl std::io::Read) -> std::io::Result<Hnsw> {
        use std::io::{Error, ErrorKind, Read};
        fn u32_of(r: &mut impl Read) -> std::io::Result<u32> {
            let mut b = [0u8; 4];
            r.read_exact(&mut b)?;
            Ok(u32::from_le_bytes(b))
        }
        fn u64_of(r: &mut impl Read) -> std::io::Result<u64> {
            let mut b = [0u8; 8];
            r.read_exact(&mut b)?;
            Ok(u64::from_le_bytes(b))
        }
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        if &magic != b"SKHNSW01" {
            return Err(Error::new(ErrorKind::InvalidData, "bad HNSW snapshot magic"));
        }
        let mut mb = [0u8; 1];
        r.read_exact(&mut mb)?;
        let metric = match mb[0] {
            0 => Metric::Cosine,
            1 => Metric::L2,
            2 => Metric::Dot,
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("bad metric tag {other}"),
                ))
            }
        };
        let dim = u64_of(r)? as usize;
        let m = u64_of(r)? as usize;
        let m0 = u64_of(r)? as usize;
        let ef_construction = u64_of(r)? as usize;
        let ef_search = u64_of(r)? as usize;
        let mut fb = [0u8; 8];
        r.read_exact(&mut fb)?;
        let ml = f64::from_le_bytes(fb);
        let rng = u64_of(r)?;
        let entry_raw = u32_of(r)?;
        let count = u32_of(r)? as usize;
        let mut nodes = Vec::with_capacity(count);
        let mut by_key = HashMap::with_capacity(count);
        for id in 0..count {
            let key_len = u32_of(r)? as usize;
            let mut key = vec![0u8; key_len];
            r.read_exact(&mut key)?;
            let mut del = [0u8; 1];
            r.read_exact(&mut del)?;
            let levels = u32_of(r)? as usize;
            let mut neighbors = Vec::with_capacity(levels);
            for _ in 0..levels {
                let n = u32_of(r)? as usize;
                let mut level = Vec::with_capacity(n);
                for _ in 0..n {
                    level.push(u32_of(r)?);
                }
                neighbors.push(level);
            }
            let mut raw = vec![0u8; dim * 4];
            r.read_exact(&mut raw)?;
            let vector: Vec<f32> = raw
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            if del[0] == 0 {
                by_key.insert(key.clone(), id as u32);
            }
            nodes.push(Node {
                vector,
                key,
                neighbors,
                deleted: del[0] != 0,
            });
        }
        Ok(Hnsw {
            metric,
            dim,
            m,
            m0,
            ef_construction,
            ef_search,
            ml,
            nodes,
            by_key,
            entry: (entry_raw != u32::MAX).then_some(entry_raw),
            rng,
            dirty: false,
        })
    }

    /// Construction parameters `(metric, dim, m, ef_construction)` — a loaded
    /// snapshot must match the live index definition or be discarded.
    pub fn params(&self) -> (Metric, usize, usize, usize) {
        (self.metric, self.dim, self.m, self.ef_construction)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Snapshot round-trip: a reloaded graph answers identically and future
    /// inserts (rng included) keep it deterministic vs. never-saved twins.
    #[test]
    fn snapshot_roundtrip_preserves_graph_and_determinism() {
        let dim = 24;
        let mut a = Hnsw::new(Metric::Cosine, dim);
        let data = vecs(1500, dim, 11);
        for (i, v) in data.iter().enumerate().take(1000) {
            a.insert(format!("k{i}").into_bytes(), v.clone());
        }
        a.remove(b"k17".as_slice());
        let mut buf = Vec::new();
        a.write_to(&mut buf).unwrap();
        let mut b = Hnsw::read_from(&mut buf.as_slice()).unwrap();
        assert!(!b.is_dirty());
        assert_eq!(a.len(), b.len());
        // Continue building BOTH from the same state; they must stay twins.
        for (i, v) in data.iter().enumerate().skip(1000) {
            a.insert(format!("k{i}").into_bytes(), v.clone());
            b.insert(format!("k{i}").into_bytes(), v.clone());
        }
        assert!(b.is_dirty());
        for i in (0..1500).step_by(113) {
            let qa = a.search(&data[i], 5, |_| true);
            let qb = b.search(&data[i], 5, |_| true);
            assert_eq!(qa, qb, "diverged at probe {i}");
        }
        // The removed key stays gone.
        assert!(b.search(&data[17], 3, |_| true).iter().all(|(k, _)| k != b"k17"));
    }

    /// Clustered data — the production failure shape: dense near-duplicate
    /// islands (e.g. newsletter embeddings). Closest-only neighbor selection
    /// severed inter-cluster links and made whole regions unreachable (top
    /// hits ~0.38 away from an exact-match query). With diversity selection,
    /// every stored vector must find itself or an effectively-identical
    /// near-duplicate; misses to a *different region* are the bug and get
    /// zero tolerance.
    #[test]
    fn self_recall_survives_clustered_data() {
        let dim = 32;
        let mut h = Hnsw::new(Metric::Cosine, dim);
        let centers = vecs(60, dim, 7);
        let jitter = vecs(60 * 80, dim, 99);
        let mut all: Vec<Vec<f32>> = Vec::new();
        for (ci, c) in centers.iter().enumerate() {
            for j in 0..80 {
                let mut v = c.clone();
                for (d, x) in v.iter_mut().enumerate() {
                    *x += jitter[ci * 80 + j][d] * 0.01; // tight cluster
                }
                all.push(v);
            }
        }
        for (i, v) in all.iter().enumerate() {
            h.insert(format!("k{i}").into_bytes(), v.clone());
        }
        let mut catastrophic = 0;
        let mut near_dupe_misses = 0;
        let mut probes = 0;
        for i in (0..all.len()).step_by(97) {
            probes += 1;
            let hits = h.search(&all[i], 3, |_| true);
            let me = format!("k{i}").into_bytes();
            if hits.iter().any(|(k, _)| *k == me) {
                continue;
            }
            match hits.first() {
                Some(&(_, d)) if d < 1e-3 => near_dupe_misses += 1,
                other => {
                    println!("catastrophic miss idx={i}: {other:?}");
                    catastrophic += 1;
                }
            }
        }
        assert_eq!(
            catastrophic, 0,
            "{catastrophic} exact-match queries landed in the wrong region"
        );
        assert!(
            near_dupe_misses * 20 <= probes,
            "{near_dupe_misses}/{probes} near-dupe self-misses (>5%)"
        );
    }

    /// Non-degenerate data: exact self-recall must be perfect.
    #[test]
    fn self_recall_perfect_on_uniform_data() {
        let dim = 32;
        let mut h = Hnsw::new(Metric::Cosine, dim);
        let all = vecs(3000, dim, 42);
        for (i, v) in all.iter().enumerate() {
            h.insert(format!("k{i}").into_bytes(), v.clone());
        }
        for i in (0..all.len()).step_by(37) {
            let hits = h.search(&all[i], 1, |_| true);
            let me = format!("k{i}").into_bytes();
            assert_eq!(hits[0].0, me, "k{i} did not find itself");
        }
    }

    fn vecs(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut r = seed | 1;
        let mut next = || {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            (r >> 11) as f32 / (1u64 << 53) as f32
        };
        (0..n).map(|_| (0..dim).map(|_| next()).collect()).collect()
    }

    fn brute(metric: Metric, data: &[Vec<f32>], q: &[f32], k: usize) -> Vec<usize> {
        let dist = |a: &[f32], b: &[f32]| match metric {
            Metric::L2 => a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum::<f32>(),
            Metric::Dot => -a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>(),
            Metric::Cosine => {
                let na = a.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-12);
                let nb = b.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-12);
                1.0 - a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>() / (na * nb)
            }
        };
        let mut scored: Vec<(f32, usize)> =
            data.iter().enumerate().map(|(i, v)| (dist(v, q), i)).collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));
        scored.into_iter().take(k).map(|(_, i)| i).collect()
    }

    #[test]
    fn high_recall_vs_brute_force() {
        for metric in [Metric::Cosine, Metric::L2, Metric::Dot] {
            let dim = 16;
            let data = vecs(2000, dim, 42);
            let mut h = Hnsw::new(metric, dim);
            for (i, v) in data.iter().enumerate() {
                h.insert((i as u32).to_le_bytes().to_vec(), v.clone());
            }
            let queries = vecs(50, dim, 7);
            let k = 10;
            let mut hits = 0usize;
            let mut total = 0usize;
            for q in &queries {
                let truth: std::collections::HashSet<usize> =
                    brute(metric, &data, q, k).into_iter().collect();
                let got = h.search(q, k, |_| true);
                for (key, _) in got {
                    let id = u32::from_le_bytes(key.try_into().unwrap()) as usize;
                    if truth.contains(&id) {
                        hits += 1;
                    }
                }
                total += k;
            }
            let recall = hits as f64 / total as f64;
            assert!(recall > 0.90, "{metric:?} recall {recall:.3} too low");
        }
    }

    #[test]
    fn filtered_search_only_returns_passing() {
        let dim = 8;
        let data = vecs(1000, dim, 5);
        let mut h = Hnsw::new(Metric::L2, dim);
        for (i, v) in data.iter().enumerate() {
            h.insert((i as u32).to_le_bytes().to_vec(), v.clone());
        }
        // Keep only even ids.
        let got = h.search(&data[0], 20, |key| {
            u32::from_le_bytes(key.try_into().unwrap()) % 2 == 0
        });
        assert_eq!(got.len(), 20);
        for (key, _) in &got {
            assert_eq!(u32::from_le_bytes(key.clone().try_into().unwrap()) % 2, 0);
        }
    }

    #[test]
    fn delete_and_replace() {
        let dim = 4;
        let mut h = Hnsw::new(Metric::L2, dim);
        h.insert(b"a".to_vec(), vec![1.0, 0.0, 0.0, 0.0]);
        h.insert(b"b".to_vec(), vec![0.0, 1.0, 0.0, 0.0]);
        h.insert(b"c".to_vec(), vec![0.0, 0.0, 1.0, 0.0]);
        assert_eq!(h.len(), 3);

        // Nearest to [1,0,0,0] is "a".
        let got = h.search(&[1.0, 0.0, 0.0, 0.0], 1, |_| true);
        assert_eq!(got[0].0, b"a");

        // Replace "a" with a vector near "c"'s direction; delete "b".
        h.insert(b"a".to_vec(), vec![0.0, 0.0, 0.9, 0.0]);
        h.remove(b"b");
        assert_eq!(h.len(), 2);
        let keys: Vec<_> = h.search(&[0.0, 1.0, 0.0, 0.0], 3, |_| true).into_iter().map(|(k, _)| k).collect();
        assert!(!keys.contains(&b"b".to_vec()), "deleted key must not appear");
    }
}
