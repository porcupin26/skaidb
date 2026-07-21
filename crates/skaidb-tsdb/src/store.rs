//! The store facade: append → head + WAL; completed windows flush to
//! blocks; queries merge blocks + head.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::block::{write_block, Block};
use crate::chunk::Sample;
use crate::compact;
use crate::head::Head;
use crate::wal::{Record, Wal};
use crate::{Labels, Result, TsdbError};

/// What one window flush wrote to blocks: `(labels, sealed chunks)` per
/// series — the input to rollup maintenance.
pub type FlushedSeries = Vec<(Labels, Vec<crate::head::SealedChunk>)>;

/// Store configuration.
#[derive(Debug, Clone)]
pub struct TsdbOptions {
    /// Byte budget for the in-memory head (0 = unbounded, the default).
    /// When an append pushes the head's approximate size past this, the
    /// whole head flushes to blocks — bounding ingest RSS on budgeted
    /// nodes (`[storage] memory_target`). Partial-window blocks from these
    /// flushes fold together at compaction.
    pub head_max_bytes: u64,
    /// Width of one block window (default 2 h).
    pub block_span_ms: i64,
    /// Drop data older than this (measured against the newest sample);
    /// `None` keeps everything.
    pub retention_ms: Option<i64>,
    /// Hard cap on live series (cardinality protection).
    pub max_series: usize,
    /// Accept samples up to this far behind a series' newest timestamp
    /// (out-of-order window, e.g. HA Prometheus pairs interleaving). `0`
    /// rejects anything non-monotonic per series.
    pub ooo_window_ms: i64,
    /// fsync the WAL on every `append_batch` (callers batch, so this is one
    /// fsync per batch, not per sample).
    pub sync_on_append: bool,
}

impl Default for TsdbOptions {
    fn default() -> Self {
        TsdbOptions {
            head_max_bytes: 0,
            block_span_ms: 2 * 3600 * 1000,
            retention_ms: None,
            max_series: 1_000_000,
            ooo_window_ms: 0,
            sync_on_append: true,
        }
    }
}

/// A label matcher. A series matches when every matcher accepts it; a
/// missing label reads as the empty string, Prometheus-style. The regex
/// forms are **anchored** (`=~ "api.*"` must match the whole value, like
/// Prometheus) and carry their compiled regex, so matching per series is
/// cheap — build them with [`Matcher::re`] / [`Matcher::not_re`].
#[derive(Debug, Clone)]
pub enum Matcher {
    Eq(String, String),
    Ne(String, String),
    Re(String, regex::Regex),
    NotRe(String, regex::Regex),
}

impl PartialEq for Matcher {
    fn eq(&self, other: &Matcher) -> bool {
        match (self, other) {
            (Matcher::Eq(k, v), Matcher::Eq(k2, v2))
            | (Matcher::Ne(k, v), Matcher::Ne(k2, v2)) => k == k2 && v == v2,
            (Matcher::Re(k, r), Matcher::Re(k2, r2))
            | (Matcher::NotRe(k, r), Matcher::NotRe(k2, r2)) => {
                k == k2 && r.as_str() == r2.as_str()
            }
            _ => false,
        }
    }
}
impl Eq for Matcher {}

impl Matcher {
    /// An anchored regex matcher (`label =~ "pattern"`).
    pub fn re(key: impl Into<String>, pattern: &str) -> Result<Matcher> {
        Ok(Matcher::Re(key.into(), compile_anchored(pattern)?))
    }

    /// An anchored negated regex matcher (`label !~ "pattern"`).
    pub fn not_re(key: impl Into<String>, pattern: &str) -> Result<Matcher> {
        Ok(Matcher::NotRe(key.into(), compile_anchored(pattern)?))
    }

    /// Whether this matcher accepts a series with `labels` (missing label
    /// = empty string). Public so a scatter coordinator can re-apply the
    /// regex forms that could not ship over the wire.
    pub fn accepts(&self, labels: &Labels) -> bool {
        self.matches(labels)
    }

    fn matches(&self, labels: &Labels) -> bool {
        let get = |k: &str| {
            labels
                .iter()
                .find(|(lk, _)| lk == k)
                .map_or("", |(_, v)| v.as_str())
        };
        match self {
            Matcher::Eq(k, v) => get(k) == v,
            Matcher::Ne(k, v) => get(k) != v,
            Matcher::Re(k, r) => r.is_match(get(k)),
            Matcher::NotRe(k, r) => !r.is_match(get(k)),
        }
    }
}

fn compile_anchored(pattern: &str) -> Result<regex::Regex> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
        .map_err(|e| TsdbError::Corrupt(format!("bad regex matcher: {e}")))
}

fn matches_all(matchers: &[Matcher], labels: &Labels) -> bool {
    matchers.iter().all(|m| m.matches(labels))
}

/// Candidate series from a postings index: the smallest usable `Eq`
/// posting, else the first usable regex's value-dictionary union. `None` =
/// no matcher can restrict and the caller scans everything. Candidates are
/// a SUPERSET selected by one matcher — the caller re-checks the full set
/// (`matches_all`), so this only ever narrows, never decides.
///
/// Usable: `Eq` with a non-empty value (an empty value also matches series
/// LACKING the label, which postings cannot enumerate), and `Re` whose
/// regex rejects `""` for the same reason. `Ne`/`NotRe` exclude rather
/// than select and never restrict.
pub(crate) fn postings_candidates<Id: Copy + Ord>(
    matchers: &[Matcher],
    posting: impl Fn(&str, &str) -> Vec<Id>,
    posting_regex: impl Fn(&str, &regex::Regex) -> Vec<Id>,
) -> Option<Vec<Id>> {
    let mut best: Option<Vec<Id>> = None;
    for m in matchers {
        if let Matcher::Eq(k, v) = m {
            if v.is_empty() {
                continue;
            }
            let ids = posting(k, v);
            if best.as_ref().is_none_or(|b| ids.len() < b.len()) {
                best = Some(ids);
            }
        }
    }
    let mut out = best.or_else(|| {
        matchers.iter().find_map(|m| match m {
            Matcher::Re(k, r) if !r.is_match("") => Some(posting_regex(k, r)),
            _ => None,
        })
    })?;
    out.sort_unstable();
    out.dedup();
    Some(out)
}

/// Counters for one `append_batch` call.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    pub appended: usize,
    /// Samples rejected as out-of-order (older than the series' last ts).
    pub rejected_out_of_order: usize,
    /// Samples rejected by the series cap.
    pub rejected_series_limit: usize,
}

/// Store-level statistics.
#[derive(Debug, Default, Clone, Copy)]
pub struct TsdbStats {
    pub series: usize,
    pub blocks: usize,
    pub samples_appended: u64,
    pub samples_rejected: u64,
    pub disk_bytes: u64,
}

#[derive(Debug)]
struct Inner {
    head: Head,
    wal: Wal,
    blocks: Vec<Block>,
    next_block_seq: u64,
    /// Everything below this boundary is durable in blocks; WAL replay
    /// skips older samples.
    flushed_through: i64,
    samples_appended: u64,
    samples_rejected: u64,
}

/// One time-series store (one table's worth of series).
#[derive(Debug)]
pub struct Tsdb {
    dir: PathBuf,
    opts: TsdbOptions,
    inner: Mutex<Inner>,
}

impl Tsdb {
    /// Open (or create) a store at `dir`, replaying the WAL into the head.
    pub fn open(dir: &Path, opts: TsdbOptions) -> Result<Tsdb> {
        if opts.block_span_ms <= 0 {
            return Err(TsdbError::Invalid("block_span_ms must be positive".into()));
        }
        std::fs::create_dir_all(dir)?;
        let blocks = Block::open_all(&dir.join("blocks"))?;
        let next_block_seq = blocks.iter().map(|b| b.meta.seq + 1).max().unwrap_or(1);
        let flushed_through = blocks
            .iter()
            .map(|b| {
                // Blocks end at window boundaries; recover the boundary.
                (b.meta.max_ts.div_euclid(opts.block_span_ms) + 1) * opts.block_span_ms
            })
            .max()
            .unwrap_or(i64::MIN);

        // Rebuild the head. WAL series ids may not be dense after
        // checkpoints, so map them to fresh head ids on the way in.
        let mut head = Head::new();
        let mut id_map: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
        let wal_dir = dir.join("wal");
        Wal::replay(&wal_dir, |record| match record {
            Record::Series { id, labels } => {
                if let Ok((head_id, _)) = head.get_or_create(&labels, usize::MAX) {
                    id_map.insert(id, head_id);
                }
            }
            Record::Samples(samples) => {
                for (id, ts, value) in samples {
                    if ts < flushed_through.saturating_sub(opts.ooo_window_ms) {
                        continue; // already durable in a block
                    }
                    if let Some(&head_id) = id_map.get(&id) {
                        // Out-of-order here means duplicated WAL tail; skip.
                        let _ = head.append(head_id, ts, value, opts.block_span_ms, opts.ooo_window_ms);
                    }
                }
            }
        })?;
        let wal = Wal::open(&wal_dir)?;

        Ok(Tsdb {
            dir: dir.to_path_buf(),
            opts,
            inner: Mutex::new(Inner {
                head,
                wal,
                blocks,
                next_block_seq,
                flushed_through,
                samples_appended: 0,
                samples_rejected: 0,
            }),
        })
    }

    /// Append a batch of samples: one WAL record + one fsync for the whole
    /// batch. Individually bad samples are counted, not fatal. Completed
    /// block windows flush automatically.
    pub fn append_batch(&self, rows: &[(Labels, i64, f64)]) -> Result<AppendResult> {
        self.append_batch_with_flush(rows).map(|(r, _)| r)
    }

    /// [`Tsdb::append_batch`], additionally returning whatever a triggered
    /// window flush wrote to blocks (`(labels, sealed chunks)` per series) —
    /// the hook rollup maintenance aggregates from.
    pub fn append_batch_with_flush(
        &self,
        rows: &[(Labels, i64, f64)],
    ) -> Result<(AppendResult, FlushedSeries)> {
        let mut inner = self.inner.lock().expect("tsdb lock");
        let mut result = AppendResult::default();
        let mut new_series: Vec<Record> = Vec::new();
        let mut accepted: Vec<(u64, i64, f64)> = Vec::new();

        for (labels, ts, value) in rows {
            let (id, created) = match inner.head.get_or_create(labels, self.opts.max_series) {
                Ok(x) => x,
                Err(TsdbError::SeriesLimit(_)) => {
                    result.rejected_series_limit += 1;
                    continue;
                }
                Err(e) => return Err(e),
            };
            if created {
                new_series.push(Record::Series {
                    id,
                    labels: labels.clone(),
                });
            }
            match inner.head.append(id, *ts, *value, self.opts.block_span_ms, self.opts.ooo_window_ms) {
                Ok(()) => {
                    accepted.push((id, *ts, *value));
                    result.appended += 1;
                }
                Err(TsdbError::OutOfOrder { .. }) => result.rejected_out_of_order += 1,
                Err(e) => return Err(e),
            }
        }

        for record in &new_series {
            inner.wal.append(record)?;
        }
        if !accepted.is_empty() {
            inner.wal.append(&Record::Samples(accepted))?;
        }
        if self.opts.sync_on_append {
            inner.wal.sync()?;
        }
        inner.samples_appended += result.appended as u64;
        inner.samples_rejected +=
            (result.rejected_out_of_order + result.rejected_series_limit) as u64;

        // Flush any window that is now complete.
        let boundary = inner.head.max_ts.div_euclid(self.opts.block_span_ms)
            * self.opts.block_span_ms;
        let mut flushed = Vec::new();
        if boundary > inner.flushed_through {
            flushed = self.flush_before(&mut inner, boundary)?;
        }
        // Memory budget: a head past its byte cap flushes wholesale, even
        // mid-window (the partial blocks compact together later).
        if self.opts.head_max_bytes > 0
            && inner.head.max_ts != i64::MIN
            && inner.head.approx_bytes() > self.opts.head_max_bytes
        {
            let boundary = inner.head.max_ts + 1;
            flushed.extend(self.flush_before(&mut inner, boundary)?);
        }
        Ok((result, flushed))
    }

    /// Force-flush everything currently in the head (shutdown, tests).
    /// Approximate resident bytes of the in-memory head (the
    /// memory-pressure release tier's reclaim unit).
    pub fn head_bytes(&self) -> usize {
        self.inner.lock().expect("tsdb lock").head.approx_bytes() as usize
    }

    pub fn flush(&self) -> Result<()> {
        let mut inner = self.inner.lock().expect("tsdb lock");
        if inner.head.max_ts == i64::MIN {
            return Ok(());
        }
        let boundary = inner.head.max_ts + 1;
        self.flush_before(&mut inner, boundary)?;
        Ok(())
    }

    fn flush_before(
        &self,
        inner: &mut Inner,
        boundary: i64,
    ) -> Result<FlushedSeries> {
        let flushed = inner.head.take_before(boundary, self.opts.block_span_ms)?;
        if !flushed.is_empty() {
            let seq = inner.next_block_seq;
            let dir = write_block(&self.dir.join("blocks"), seq, 0, flushed.clone())?;
            inner.next_block_seq += 1;
            inner.blocks.push(Block::open(&dir)?);
            inner
                .blocks
                .sort_by_key(|b| (b.meta.min_ts, b.meta.seq));
        }
        inner.flushed_through = inner.flushed_through.max(boundary);

        // Checkpoint the WAL: re-record live series + still-unflushed
        // samples in a fresh segment, then drop older segments.
        let keep = inner.wal.begin_checkpoint()?;
        let live: Vec<Record> = inner
            .head
            .live_series()
            .map(|(id, labels)| Record::Series {
                id,
                labels: labels.clone(),
            })
            .collect();
        let mut open_samples: Vec<(u64, i64, f64)> = Vec::new();
        let ids: Vec<u64> = inner.head.live_series().map(|(id, _)| id).collect();
        for id in ids {
            for s in inner.head.samples(id, boundary, i64::MAX)? {
                open_samples.push((id, s.ts, s.value));
            }
        }
        for record in &live {
            inner.wal.append(record)?;
        }
        if !open_samples.is_empty() {
            inner.wal.append(&Record::Samples(open_samples))?;
        }
        inner.wal.sync()?;
        inner.wal.truncate_before(keep)?;

        // Retention + compaction ride the flush cadence.
        if let Some(retention) = self.opts.retention_ms {
            let cutoff = inner.head.max_ts.saturating_sub(retention);
            compact::drop_expired(&mut inner.blocks, cutoff)?;
        }
        while let Some(group) = compact::plan(&inner.blocks, self.opts.block_span_ms) {
            let inputs: Vec<&Block> = group.iter().map(|&i| &inner.blocks[i]).collect();
            let seq = inner.next_block_seq;
            compact::merge(&self.dir.join("blocks"), seq, &inputs)?;
            inner.next_block_seq += 1;
            inner.blocks = Block::open_all(&self.dir.join("blocks"))?;
        }
        Ok(flushed)
    }

    /// All samples in `[t0, t1]` for series matching every matcher, grouped
    /// per series, time-ordered.
    pub fn query(
        &self,
        matchers: &[Matcher],
        t0: i64,
        t1: i64,
    ) -> Result<Vec<(Labels, Vec<Sample>)>> {
        let inner = self.inner.lock().expect("tsdb lock");
        let mut merged: BTreeMap<Labels, Vec<Sample>> = BTreeMap::new();
        // Blocks are time-ordered and per-series ranges are disjoint, so
        // appending block-by-block then head keeps samples sorted. Matchers
        // select candidates through each structure's label postings (falling
        // back to the full walk when none can restrict).
        for block in &inner.blocks {
            for (labels, samples) in block.query_matchers(matchers, t0, t1)? {
                merged.entry(labels).or_default().extend(samples);
            }
        }
        let head_hits: Vec<(u64, Labels)> = match postings_candidates(
            matchers,
            |k, v| inner.head.posting(k, v),
            |k, re| inner.head.posting_regex(k, re),
        ) {
            Some(candidates) => candidates
                .into_iter()
                .filter_map(|id| {
                    inner
                        .head
                        .labels_of(id)
                        .filter(|l| matches_all(matchers, l))
                        .map(|l| (id, l.clone()))
                })
                .collect(),
            None => inner
                .head
                .series_matching(|l| matches_all(matchers, l))
                .map(|(id, l)| (id, l.clone()))
                .collect(),
        };
        for (id, labels) in head_hits {
            let samples = inner.head.samples(id, t0, t1)?;
            if !samples.is_empty() {
                merged.entry(labels).or_default().extend(samples);
            }
        }
        Ok(merged
            .into_iter()
            .map(|(labels, mut samples)| {
                // OOO flushes can produce time-overlapping blocks; normalize.
                samples.sort_by_key(|s| s.ts);
                samples.dedup_by(|later, earlier| {
                    if later.ts == earlier.ts {
                        earlier.value = later.value;
                        true
                    } else {
                        false
                    }
                });
                (labels, samples)
            })
            .collect())
    }

    /// Repair-path ingest: accept samples of ANY age for their series by
    /// writing them directly as a new level-0 block (durable via the block
    /// commit protocol; no WAL involvement). Overlaps with existing data are
    /// resolved by the query-time sort+dedupe and folded together by
    /// compaction.
    pub fn merge_samples(&self, rows: &[(Labels, i64, f64)]) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut per: BTreeMap<Labels, Vec<Sample>> = BTreeMap::new();
        for (labels, ts, value) in rows {
            per.entry(labels.clone()).or_default().push(Sample {
                ts: *ts,
                value: *value,
            });
        }
        let mut series = Vec::with_capacity(per.len());
        let mut n = 0usize;
        for (labels, mut samples) in per {
            samples.sort_by_key(|s| s.ts);
            samples.dedup_by_key(|s| s.ts);
            n += samples.len();
            let chunks = crate::head::rechunk(&samples, self.opts.block_span_ms)?;
            series.push((labels, chunks));
        }
        let mut inner = self.inner.lock().expect("tsdb lock");
        let seq = inner.next_block_seq;
        let dir = write_block(&self.dir.join("blocks"), seq, 0, series)?;
        inner.next_block_seq += 1;
        inner.blocks.push(Block::open(&dir)?);
        inner.blocks.sort_by_key(|b| (b.meta.min_ts, b.meta.seq));
        Ok(n)
    }

    /// Per-series `(labels, deduped sample count, order-independent
    /// checksum)` — the anti-entropy comparison unit. Costs a full decode;
    /// meant for background repair, not hot paths.
    pub fn series_summaries(&self) -> Result<Vec<(Labels, u64, u64)>> {
        let all = self.query(&[], i64::MIN, i64::MAX)?;
        Ok(all
            .into_iter()
            .map(|(labels, samples)| {
                let mut checksum = 0u64;
                for s in &samples {
                    let mut h = (s.ts as u64).wrapping_mul(0x9E3779B97F4A7C15);
                    h ^= s.value.to_bits().wrapping_mul(0xC2B2AE3D27D4EB4F);
                    h ^= h >> 29;
                    checksum ^= h.wrapping_mul(0x165667B19E3779F9);
                }
                (labels, samples.len() as u64, checksum)
            })
            .collect())
    }

    /// Every series label set in the store (head + blocks, deduplicated) —
    /// the migration unit for resharding.
    pub fn series_labels(&self) -> Vec<Labels> {
        let inner = self.inner.lock().expect("tsdb lock");
        let mut set: std::collections::BTreeSet<Labels> = std::collections::BTreeSet::new();
        for (_, labels) in inner.head.live_series() {
            set.insert(labels.clone());
        }
        for block in &inner.blocks {
            for labels in block.series_labels() {
                set.insert(labels);
            }
        }
        set.into_iter().collect()
    }

    pub fn stats(&self) -> TsdbStats {
        let inner = self.inner.lock().expect("tsdb lock");
        let disk_bytes = dir_size(&self.dir).unwrap_or(0);
        TsdbStats {
            series: inner.head.series_count(),
            blocks: inner.blocks.len(),
            samples_appended: inner.samples_appended,
            samples_rejected: inner.samples_rejected,
            disk_bytes,
        }
    }

    /// The newest appended timestamp (`i64::MIN` when empty). Retention
    /// drops are relative to this data frontier, so `max_ts - retention` is
    /// the horizon below which blocks may already be gone.
    pub fn max_ts(&self) -> i64 {
        self.inner.lock().expect("tsdb lock").head.max_ts
    }

    /// Live retention update (`ALTER TABLE ... SET (retention = ...)`).
    /// Takes effect at the next flush; a widened retention cannot resurrect
    /// already-dropped blocks.
    pub fn set_retention_ms(&mut self, ms: Option<i64>) {
        self.opts.retention_ms = ms;
    }

    /// Live out-of-order-window update (`ALTER TABLE ... SET (ooo = ...)`).
    /// Applies to subsequent appends; points already rejected are gone.
    pub fn set_ooo_window_ms(&mut self, ms: i64) {
        self.opts.ooo_window_ms = ms;
    }

    /// Remove whole series (a post-resharding reclaim): flush the head so
    /// the WAL cannot resurrect them, then rewrite every block that holds a
    /// target series without it. Returns how many of `targets` were
    /// actually present. Rare admin-path operation — the block rewrites are
    /// deliberate, not incremental.
    pub fn drop_series(
        &self,
        targets: &std::collections::HashSet<Labels>,
    ) -> Result<usize> {
        if targets.is_empty() {
            return Ok(0);
        }
        self.flush()?;
        let mut inner = self.inner.lock().expect("tsdb lock");
        let blocks_dir = self.dir.join("blocks");
        let mut found: std::collections::HashSet<&Labels> = std::collections::HashSet::new();
        let mut rebuilt = Vec::with_capacity(inner.blocks.len());
        type KeptSeries = Vec<(Labels, Vec<crate::head::SealedChunk>)>;
        let mut pending: Vec<(std::path::PathBuf, KeptSeries)> = Vec::new();
        for block in inner.blocks.drain(..) {
            let hit = block.series_labels().iter().any(|l| targets.contains(l));
            if !hit {
                rebuilt.push(block);
                continue;
            }
            let mut kept = Vec::new();
            for (labels, chunks) in block.raw_series()? {
                if let Some(t) = targets.get(&labels) {
                    found.insert(t);
                } else {
                    kept.push((labels, chunks));
                }
            }
            pending.push((block.dir.clone(), kept));
        }
        for (old_dir, kept) in pending {
            if !kept.is_empty() {
                let seq = inner.next_block_seq;
                inner.next_block_seq += 1;
                let dir = write_block(&blocks_dir, seq, 0, kept)?;
                rebuilt.push(Block::open(&dir)?);
            }
            let _ = std::fs::remove_dir_all(&old_dir);
        }
        rebuilt.sort_by_key(|b| (b.meta.min_ts, b.meta.seq));
        inner.blocks = rebuilt;
        Ok(found.len())
    }

    /// The newest timestamp anywhere in the store (head **and** blocks —
    /// merged blocks can outrun the head), `i64::MIN` when empty.
    pub fn max_ts_all(&self) -> i64 {
        let inner = self.inner.lock().expect("tsdb lock");
        let blocks_max = inner
            .blocks
            .iter()
            .map(|b| b.meta.max_ts)
            .max()
            .unwrap_or(i64::MIN);
        inner.head.max_ts.max(blocks_max)
    }

    /// The oldest timestamp still in the head, or `None` when everything
    /// is flushed. Rollups are complete strictly below this boundary (any
    /// non-head sample has been through flush-path or repair-path rollup
    /// maintenance).
    pub fn head_min_ts(&self) -> Option<i64> {
        self.inner.lock().expect("tsdb lock").head.min_ts()
    }

    /// Everything below this boundary is durable in immutable blocks
    /// (`i64::MIN` before the first flush). Data above it lives in the head
    /// and is never touched by retention; data below it is what rollups have
    /// aggregated and what retention may drop.
    pub fn flushed_through(&self) -> i64 {
        self.inner.lock().expect("tsdb lock").flushed_through
    }
}

fn dir_size(dir: &Path) -> std::io::Result<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        total += if meta.is_dir() {
            dir_size(&entry.path())?
        } else {
            meta.len()
        };
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tsdb-store-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn labels(host: &str) -> Labels {
        vec![("host".into(), host.into()), ("job".into(), "node".into())]
    }

    fn opts(span: i64) -> TsdbOptions {
        TsdbOptions {
            block_span_ms: span,
            sync_on_append: false,
            ..TsdbOptions::default()
        }
    }

    /// The postings index is invisible to results: every matcher form
    /// (Eq, Ne, Re, NotRe — including the empty-matching regex and
    /// missing-label shapes that postings CANNOT serve) returns exactly the
    /// brute-force answer, across head + flushed blocks + a GC'd-and-
    /// recreated series.
    #[test]
    fn postings_match_brute_force_for_every_matcher_form() {
        let dir = temp_dir("postings");
        let db = Tsdb::open(&dir, opts(10_000)).unwrap();
        // 60 series over three label dimensions; `extra` exists only on
        // some series (the missing-label semantics trap).
        let mut rows = Vec::new();
        for i in 0..60i64 {
            let mut ls: Labels = vec![
                ("host".into(), format!("web{}", i % 12)),
                ("job".into(), if i % 3 == 0 { "node" } else { "app" }.into()),
            ];
            if i % 4 == 0 {
                ls.push(("extra".into(), format!("e{}", i % 5)));
            }
            ls.sort();
            for w in 0..2i64 {
                rows.push((ls.clone(), w * 10_000 + (i % 7) * 100, i as f64));
            }
        }
        db.append_batch(&rows).unwrap();
        assert!(db.stats().blocks >= 1, "first window flushed to a block");

        let cases: Vec<Vec<Matcher>> = vec![
            vec![Matcher::Eq("host".into(), "web3".into())],
            vec![Matcher::Eq("host".into(), "nosuch".into())],
            vec![Matcher::Eq("extra".into(), "e0".into())],
            vec![Matcher::Eq("extra".into(), "".into())], // matches label-absent series
            vec![Matcher::Ne("job".into(), "node".into())],
            vec![Matcher::re("host", "web[12]").unwrap()],
            vec![Matcher::re("extra", "e.*|").unwrap()], // matches "" → no postings
            vec![Matcher::not_re("host", "web[0-5]").unwrap()],
            vec![
                Matcher::Eq("job".into(), "node".into()),
                Matcher::re("host", "web1?[02]").unwrap(),
            ],
            vec![
                Matcher::re("host", "web.*").unwrap(),
                Matcher::Ne("extra".into(), "e0".into()),
            ],
        ];
        let all = db.query(&[], i64::MIN, i64::MAX).unwrap();
        for matchers in &cases {
            let got = db.query(matchers, i64::MIN, i64::MAX).unwrap();
            let expect: Vec<_> = all
                .iter()
                .filter(|(l, _)| matchers.iter().all(|m| m.accepts(l)))
                .cloned()
                .collect();
            assert_eq!(got, expect, "matchers {matchers:?}");
        }

        // GC a series (flush leaves it idle, slot cleared), recreate it, and
        // confirm postings survived the churn.
        db.append_batch(&[(
            vec![("host".into(), "web3".into()), ("job".into(), "app".into())],
            50_000,
            1.0,
        )])
        .unwrap();
        let got = db
            .query(&[Matcher::Eq("host".into(), "web3".into())], i64::MIN, i64::MAX)
            .unwrap();
        assert!(
            got.iter().any(|(_, s)| s.iter().any(|x| x.ts == 50_000)),
            "recreated series found via postings"
        );
    }

    #[test]
    fn append_flush_query_across_head_and_blocks() {
        let dir = temp_dir("afq");
        let db = Tsdb::open(&dir, opts(10_000)).unwrap();
        // 3 windows × 2 series; the third window stays in the head.
        let mut rows = Vec::new();
        for w in 0..3i64 {
            for i in 0..10i64 {
                let ts = w * 10_000 + i * 1000;
                rows.push((labels("a"), ts, (w * 10 + i) as f64));
                rows.push((labels("b"), ts, -(w * 10 + i) as f64));
            }
        }
        let res = db.append_batch(&rows).unwrap();
        assert_eq!(res.appended, 60);
        assert_eq!(res.rejected_out_of_order, 0);
        assert!(db.stats().blocks >= 1, "completed windows should flush");

        let all = db.query(&[], 0, i64::MAX).unwrap();
        assert_eq!(all.len(), 2);
        for (_, samples) in &all {
            assert_eq!(samples.len(), 30);
            assert!(samples.windows(2).all(|w| w[0].ts < w[1].ts));
        }

        // Matcher + time range crossing the block/head boundary.
        let hits = db
            .query(&[Matcher::Eq("host".into(), "a".into())], 15_000, 25_000)
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].1.len(), 11);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn crash_recovery_replays_unflushed_head() {
        let dir = temp_dir("crash");
        {
            let db = Tsdb::open(&dir, opts(1_000_000)).unwrap();
            let rows: Vec<_> = (0..50i64)
                .map(|i| (labels("a"), i * 1000, i as f64))
                .collect();
            db.append_batch(&rows).unwrap();
            // Dropped without flush: everything lives in WAL + head only.
        }
        let db = Tsdb::open(&dir, opts(1_000_000)).unwrap();
        let all = db.query(&[], 0, i64::MAX).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].1.len(), 50);
        assert_eq!(all[0].1[49].value, 49.0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A byte-capped head flushes wholesale mid-window instead of growing
    /// unbounded; the data stays fully queryable across head and blocks.
    #[test]
    fn head_byte_budget_triggers_flush() {
        let dir = temp_dir("hbb");
        let db = Tsdb::open(
            &dir,
            TsdbOptions {
                block_span_ms: i64::MAX / 4, // the window never completes
                head_max_bytes: 8 * 1024,
                sync_on_append: false,
                ..TsdbOptions::default()
            },
        )
        .unwrap();
        let labels: Labels = vec![("host".into(), "a".into())];
        let mut rows = Vec::new();
        for i in 0..5_000i64 {
            rows.push((labels.clone(), i * 1000, i as f64));
        }
        db.append_batch(&rows).unwrap();
        assert!(
            db.flushed_through() > i64::MIN,
            "budget flush never triggered"
        );
        let head_bytes = {
            // Whatever remains in the head is under (or near) the cap.
            let all = db.query(&[], 0, i64::MAX).unwrap();
            assert_eq!(all.len(), 1);
            assert_eq!(all[0].1.len(), 5_000, "no samples lost");
            db.head_min_ts()
        };
        let _ = head_bytes;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn crash_after_flush_does_not_duplicate() {
        let dir = temp_dir("dup");
        {
            let db = Tsdb::open(&dir, opts(10_000)).unwrap();
            let rows: Vec<_> = (0..30i64)
                .map(|i| (labels("a"), i * 1000, i as f64))
                .collect();
            db.append_batch(&rows).unwrap(); // flushes windows 0 and 1
        }
        let db = Tsdb::open(&dir, opts(10_000)).unwrap();
        let all = db.query(&[], 0, i64::MAX).unwrap();
        assert_eq!(all[0].1.len(), 30, "no duplicates, no loss");
        // And the store still accepts appends after recovery.
        db.append_batch(&[(labels("a"), 30_000, 30.0)]).unwrap();
        let all = db.query(&[], 0, i64::MAX).unwrap();
        assert_eq!(all[0].1.len(), 31);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn out_of_order_and_series_limit_are_counted() {
        let dir = temp_dir("rej");
        let db = Tsdb::open(
            &dir,
            TsdbOptions {
                max_series: 1,
                sync_on_append: false,
                ..TsdbOptions::default()
            },
        )
        .unwrap();
        let res = db
            .append_batch(&[
                (labels("a"), 1000, 1.0),
                (labels("a"), 500, 2.0),  // out of order
                (labels("b"), 1000, 3.0), // over the cap
            ])
            .unwrap();
        assert_eq!(res.appended, 1);
        assert_eq!(res.rejected_out_of_order, 1);
        assert_eq!(res.rejected_series_limit, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retention_drops_old_blocks() {
        let dir = temp_dir("ret");
        let db = Tsdb::open(
            &dir,
            TsdbOptions {
                block_span_ms: 10_000,
                retention_ms: Some(25_000),
                sync_on_append: false,
                ..TsdbOptions::default()
            },
        )
        .unwrap();
        // Spread appends over 10 windows; old ones must expire as we go.
        for w in 0..10i64 {
            let rows: Vec<_> = (0..5i64)
                .map(|i| (labels("a"), w * 10_000 + i * 2000, i as f64))
                .collect();
            db.append_batch(&rows).unwrap();
        }
        let all = db.query(&[], 0, i64::MAX).unwrap();
        let earliest = all[0].1.first().unwrap().ts;
        assert!(
            earliest >= 90_000 - 25_000 - 10_000,
            "old windows should be gone, earliest {earliest}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_preserves_all_samples() {
        let dir = temp_dir("cmp");
        let db = Tsdb::open(&dir, opts(1_000)).unwrap();
        // Many small windows so several level-0 blocks exist and tier up.
        for w in 0..16i64 {
            let rows: Vec<_> = (0..4i64)
                .map(|i| (labels("a"), w * 1_000 + i * 250, (w * 4 + i) as f64))
                .collect();
            db.append_batch(&rows).unwrap();
        }
        db.flush().unwrap();
        let stats = db.stats();
        assert!(
            stats.blocks < 16,
            "compaction should have merged blocks, got {}",
            stats.blocks
        );
        let all = db.query(&[], 0, i64::MAX).unwrap();
        assert_eq!(all[0].1.len(), 64);
        assert!(all[0].1.windows(2).all(|w| w[0].ts < w[1].ts));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
