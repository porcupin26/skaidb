//! The in-memory head: per-series open chunks plus sealed chunks awaiting
//! block flush. Rebuilt from the WAL on open.

use std::collections::HashMap;

use crate::chunk::{ChunkBuilder, Sample};
use crate::{Labels, Result, TsdbError};

/// Seal an open chunk once it holds this many samples (Gorilla sweet spot).
const CHUNK_MAX_SAMPLES: u16 = 120;

/// A sealed, immutable in-head chunk (same bytes a block stores).
#[derive(Debug, Clone)]
pub struct SealedChunk {
    pub min_ts: i64,
    pub max_ts: i64,
    pub count: u16,
    pub data: Vec<u8>,
}

#[derive(Debug)]
struct SeriesEntry {
    labels: Labels,
    sealed: Vec<SealedChunk>,
    /// `(window_start, builder)` — the chunk being appended to. Chunks never
    /// span a block window, so flush boundaries cut cleanly.
    open: Option<(i64, ChunkBuilder)>,
    /// Out-of-order samples within the configured window, kept ts-sorted;
    /// merged into chunks at flush and into reads before then.
    ooo: Vec<Sample>,
    last_ts: i64,
}

/// Cap on buffered out-of-order samples per series.
const OOO_MAX_PER_SERIES: usize = 512;

/// In-memory head for one store.
#[derive(Debug, Default)]
pub struct Head {
    map: HashMap<Labels, u64>,
    entries: Vec<Option<SeriesEntry>>,
    live: usize,
    /// Label postings: key → value → live series ids carrying that pair.
    /// Maintained at series create/GC; lets an equality or regex matcher
    /// select candidate ids without walking every live series (the
    /// high-cardinality unlock). Candidates are always re-checked against
    /// the full matcher set by the caller, so the index only ever narrows.
    postings: HashMap<String, HashMap<String, Vec<u64>>>,
    /// Highest timestamp ever appended.
    pub max_ts: i64,
}

impl Head {
    pub fn new() -> Head {
        Head {
            max_ts: i64::MIN,
            ..Head::default()
        }
    }

    pub fn series_count(&self) -> usize {
        self.live
    }

    /// Approximate bytes held by the head: sealed chunk bytes, open
    /// builders (compressed samples run ~2 bytes; count × 4 is a safe
    /// over-estimate with struct overhead), and OOO buffers at raw sample
    /// size. Serves the `head_max_bytes` budget trigger — an estimate, not
    /// an accounting.
    pub fn approx_bytes(&self) -> u64 {
        let mut total = 0u64;
        for entry in self.entries.iter().flatten() {
            for c in &entry.sealed {
                total += c.data.len() as u64;
            }
            if let Some((_, builder)) = &entry.open {
                total += builder.count() as u64 * 4 + 64;
            }
            total += entry.ooo.len() as u64 * 24;
            total += 64; // per-series map/labels overhead
        }
        total
    }

    /// The oldest timestamp currently held in the head (sealed chunks, the
    /// open builders, and OOO buffers), or `None` when the head is empty.
    /// Everything strictly below it is in immutable blocks — the boundary
    /// below which rollups are complete.
    pub fn min_ts(&self) -> Option<i64> {
        let mut min = i64::MAX;
        for entry in self.entries.iter().flatten() {
            for c in &entry.sealed {
                min = min.min(c.min_ts);
            }
            if let Some((_, builder)) = &entry.open {
                min = min.min(builder.first_ts());
            }
            if let Some(s) = entry.ooo.first() {
                min = min.min(s.ts);
            }
        }
        (min != i64::MAX).then_some(min)
    }

    /// Look up or create the series id for `labels`.
    pub fn get_or_create(
        &mut self,
        labels: &Labels,
        max_series: usize,
    ) -> Result<(u64, bool)> {
        debug_assert!(labels.windows(2).all(|w| w[0].0 <= w[1].0), "labels sorted");
        if let Some(&id) = self.map.get(labels) {
            return Ok((id, false));
        }
        if self.live >= max_series {
            return Err(TsdbError::SeriesLimit(max_series));
        }
        let id = self.entries.len() as u64;
        self.entries.push(Some(SeriesEntry {
            labels: labels.clone(),
            sealed: Vec::new(),
            open: None,
            ooo: Vec::new(),
            last_ts: i64::MIN,
        }));
        self.map.insert(labels.clone(), id);
        for (k, v) in labels {
            self.postings
                .entry(k.clone())
                .or_default()
                .entry(v.clone())
                .or_default()
                .push(id);
        }
        self.live += 1;
        Ok((id, true))
    }

    fn entry_mut(&mut self, id: u64) -> Result<&mut SeriesEntry> {
        self.entries
            .get_mut(id as usize)
            .and_then(Option::as_mut)
            .ok_or_else(|| TsdbError::Invalid(format!("unknown series id {id}")))
    }

    /// Append one sample. `block_span` cuts chunks at window boundaries;
    /// samples older than a series' newest land in its out-of-order buffer
    /// when within `ooo_window` (0 = strict monotonic).
    pub fn append(
        &mut self,
        id: u64,
        ts: i64,
        value: f64,
        block_span: i64,
        ooo_window: i64,
    ) -> Result<()> {
        let entry = self.entry_mut(id)?;
        if ts <= entry.last_ts {
            if ooo_window > 0
                && ts > entry.last_ts.saturating_sub(ooo_window)
                && ts < entry.last_ts
                && entry.ooo.len() < OOO_MAX_PER_SERIES
            {
                // Sorted insert; an equal timestamp overwrites (last wins).
                match entry.ooo.binary_search_by_key(&ts, |s| s.ts) {
                    Ok(i) => entry.ooo[i].value = value,
                    Err(i) => entry.ooo.insert(i, Sample { ts, value }),
                }
                return Ok(());
            }
            return Err(TsdbError::OutOfOrder {
                ts,
                last: entry.last_ts,
            });
        }
        let window = ts.div_euclid(block_span) * block_span;
        let needs_new = match &entry.open {
            None => true,
            Some((w, b)) => *w != window || b.count() >= CHUNK_MAX_SAMPLES,
        };
        if needs_new {
            Self::seal_open(entry);
            entry.open = Some((window, ChunkBuilder::new()));
        }
        let (_, builder) = entry.open.as_mut().expect("open chunk");
        builder.append(ts, value)?;
        entry.last_ts = ts;
        if ts > self.max_ts {
            self.max_ts = ts;
        }
        Ok(())
    }

    fn seal_open(entry: &mut SeriesEntry) {
        if let Some((_, builder)) = entry.open.take() {
            if builder.count() > 0 {
                entry.sealed.push(SealedChunk {
                    min_ts: builder.first_ts(),
                    max_ts: builder.last_ts(),
                    count: builder.count(),
                    data: builder.seal(),
                });
            }
        }
    }

    /// Extract everything strictly before `boundary` for a block flush,
    /// sealing open chunks from completed windows and merging any buffered
    /// out-of-order samples into the flushed chunks (a re-encode, only paid
    /// when OOO data exists). Series left with no data and no recent appends
    /// are dropped (their ids are not reused).
    pub fn take_before(
        &mut self,
        boundary: i64,
        block_span: i64,
    ) -> Result<Vec<(Labels, Vec<SealedChunk>)>> {
        let mut out = Vec::new();
        let postings = &mut self.postings;
        for (id, slot) in self.entries.iter_mut().enumerate() {
            let id = id as u64;
            let Some(entry) = slot else { continue };
            if entry
                .open
                .as_ref()
                .is_some_and(|(window, _)| *window < boundary)
            {
                Self::seal_open(entry);
            }
            let mut flushed: Vec<SealedChunk> = entry
                .sealed
                .iter()
                .filter(|c| c.max_ts < boundary)
                .cloned()
                .collect();
            entry.sealed.retain(|c| c.max_ts >= boundary);

            // Fold flushable out-of-order samples in by decoding, merging
            // (last wins on ties), and re-chunking.
            let ooo_cut = entry.ooo.partition_point(|s| s.ts < boundary);
            if ooo_cut > 0 {
                let ooo: Vec<Sample> = entry.ooo.drain(..ooo_cut).collect();
                let mut samples: Vec<Sample> = Vec::new();
                for chunk in &flushed {
                    samples.extend(crate::chunk::decode(&chunk.data)?);
                }
                samples.extend(ooo);
                samples.sort_by_key(|s| s.ts);
                samples.dedup_by(|later, earlier| {
                    if later.ts == earlier.ts {
                        earlier.value = later.value; // later write wins
                        true
                    } else {
                        false
                    }
                });
                flushed = rechunk(&samples, block_span)?;
            }

            if !flushed.is_empty() {
                out.push((entry.labels.clone(), flushed));
            }
            if entry.sealed.is_empty() && entry.open.is_none() && entry.ooo.is_empty() {
                // Idle series: garbage-collect (recreated on next append).
                self.map.remove(&entry.labels);
                for (k, v) in &entry.labels {
                    if let Some(values) = postings.get_mut(k) {
                        if let Some(ids) = values.get_mut(v) {
                            ids.retain(|i| *i != id);
                        }
                    }
                }
                *slot = None;
                self.live -= 1;
            }
        }
        Ok(out)
    }

    /// All live series as `(id, labels)` (checkpointing).
    pub fn live_series(&self) -> impl Iterator<Item = (u64, &Labels)> {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(id, e)| e.as_ref().map(|e| (id as u64, &e.labels)))
    }

    /// Samples still held in the head for `id` (checkpointing / queries).
    pub fn samples(&self, id: u64, t0: i64, t1: i64) -> Result<Vec<Sample>> {
        let entry = self
            .entries
            .get(id as usize)
            .and_then(Option::as_ref)
            .ok_or_else(|| TsdbError::Invalid(format!("unknown series id {id}")))?;
        let mut out = Vec::new();
        for chunk in &entry.sealed {
            if chunk.max_ts < t0 || chunk.min_ts > t1 {
                continue;
            }
            for s in crate::chunk::decode(&chunk.data)? {
                if s.ts >= t0 && s.ts <= t1 {
                    out.push(s);
                }
            }
        }
        if let Some((_, builder)) = &entry.open {
            for s in builder.snapshot()? {
                if s.ts >= t0 && s.ts <= t1 {
                    out.push(s);
                }
            }
        }
        if !entry.ooo.is_empty() {
            out.extend(entry.ooo.iter().filter(|s| s.ts >= t0 && s.ts <= t1));
            out.sort_by_key(|s| s.ts);
            out.dedup_by(|later, earlier| {
                if later.ts == earlier.ts {
                    earlier.value = later.value;
                    true
                } else {
                    false
                }
            });
        }
        Ok(out)
    }

    /// Ids and labels of series matching `keep` (head-side query planning).
    /// The live ids carrying label pair `(k, v)` (cloned posting).
    pub fn posting(&self, k: &str, v: &str) -> Vec<u64> {
        self.postings
            .get(k)
            .and_then(|values| values.get(v))
            .cloned()
            .unwrap_or_default()
    }

    /// The union of postings for every value of `k` accepted by `re` — a
    /// regex matcher walks the key's value dictionary (distinct values),
    /// not the series population.
    pub fn posting_regex(&self, k: &str, re: &regex::Regex) -> Vec<u64> {
        let Some(values) = self.postings.get(k) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for (v, ids) in values {
            if re.is_match(v) {
                out.extend_from_slice(ids);
            }
        }
        out
    }

    /// The labels of live series `id`, if it exists.
    pub fn labels_of(&self, id: u64) -> Option<&Labels> {
        self.entries.get(id as usize)?.as_ref().map(|e| &e.labels)
    }

    pub fn series_matching<'a>(
        &'a self,
        keep: impl Fn(&Labels) -> bool + 'a,
    ) -> impl Iterator<Item = (u64, &'a Labels)> {
        self.entries
            .iter()
            .enumerate()
            .filter_map(move |(id, e)| match e {
                Some(e) if keep(&e.labels) => Some((id as u64, &e.labels)),
                _ => None,
            })
    }
}

/// Rebuild sealed chunks from sorted, deduplicated samples, cutting at
/// block-window boundaries and the per-chunk sample cap.
pub(crate) fn rechunk(samples: &[Sample], block_span: i64) -> Result<Vec<SealedChunk>> {
    let mut out = Vec::new();
    let mut builder = ChunkBuilder::new();
    let mut window = i64::MIN;
    for s in samples {
        let w = s.ts.div_euclid(block_span) * block_span;
        if builder.count() > 0 && (w != window || builder.count() >= CHUNK_MAX_SAMPLES) {
            let done = std::mem::take(&mut builder);
            out.push(SealedChunk {
                min_ts: done.first_ts(),
                max_ts: done.last_ts(),
                count: done.count(),
                data: done.seal(),
            });
        }
        window = w;
        builder.append(s.ts, s.value)?;
    }
    if builder.count() > 0 {
        out.push(SealedChunk {
            min_ts: builder.first_ts(),
            max_ts: builder.last_ts(),
            count: builder.count(),
            data: builder.seal(),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(host: &str) -> Labels {
        vec![("host".into(), host.into())]
    }

    #[test]
    fn seal_at_capacity_and_window() {
        let mut head = Head::new();
        let (id, created) = head.get_or_create(&labels("a"), 10).unwrap();
        assert!(created);
        // 130 samples at 1ms apart: one seal at 120.
        for i in 0..130i64 {
            head.append(id, i, i as f64, 1_000_000, 0).unwrap();
        }
        let s = head.samples(id, 0, i64::MAX).unwrap();
        assert_eq!(s.len(), 130);
        // Crossing a window boundary seals too.
        head.append(id, 1_000_001, 1.0, 1_000_000, 0).unwrap();
        let s = head.samples(id, 0, i64::MAX).unwrap();
        assert_eq!(s.len(), 131);
    }

    #[test]
    fn take_before_extracts_and_gcs() {
        let mut head = Head::new();
        let (a, _) = head.get_or_create(&labels("a"), 10).unwrap();
        let (b, _) = head.get_or_create(&labels("b"), 10).unwrap();
        head.append(a, 100, 1.0, 1000, 0).unwrap();
        head.append(a, 1500, 2.0, 1000, 0).unwrap(); // second window
        head.append(b, 200, 3.0, 1000, 0).unwrap(); // only first window
        let flushed = head.take_before(1000, 1000).unwrap();
        assert_eq!(flushed.len(), 2);
        // a keeps its open second-window chunk; b is fully flushed + GC'd.
        assert_eq!(head.series_count(), 1);
        assert_eq!(head.samples(a, 0, i64::MAX).unwrap().len(), 1);
        // b's next append recreates it.
        let (b2, created) = head.get_or_create(&labels("b"), 10).unwrap();
        assert!(created);
        assert_ne!(b2, b);
    }

    #[test]
    fn series_limit_enforced() {
        let mut head = Head::new();
        head.get_or_create(&labels("a"), 1).unwrap();
        assert!(matches!(
            head.get_or_create(&labels("b"), 1),
            Err(TsdbError::SeriesLimit(1))
        ));
    }
}
