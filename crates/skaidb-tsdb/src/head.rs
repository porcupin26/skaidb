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
    last_ts: i64,
}

/// In-memory head for one store.
#[derive(Debug, Default)]
pub struct Head {
    map: HashMap<Labels, u64>,
    entries: Vec<Option<SeriesEntry>>,
    live: usize,
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
            last_ts: i64::MIN,
        }));
        self.map.insert(labels.clone(), id);
        self.live += 1;
        Ok((id, true))
    }

    fn entry_mut(&mut self, id: u64) -> Result<&mut SeriesEntry> {
        self.entries
            .get_mut(id as usize)
            .and_then(Option::as_mut)
            .ok_or_else(|| TsdbError::Invalid(format!("unknown series id {id}")))
    }

    /// Append one sample. `block_span` cuts chunks at window boundaries.
    pub fn append(&mut self, id: u64, ts: i64, value: f64, block_span: i64) -> Result<()> {
        let entry = self.entry_mut(id)?;
        if ts <= entry.last_ts {
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
    /// sealing open chunks from completed windows. Series left with no data
    /// and no recent appends are dropped (their ids are not reused).
    pub fn take_before(&mut self, boundary: i64) -> Vec<(Labels, Vec<SealedChunk>)> {
        let mut out = Vec::new();
        for slot in &mut self.entries {
            let Some(entry) = slot else { continue };
            if entry
                .open
                .as_ref()
                .is_some_and(|(window, _)| *window < boundary)
            {
                Self::seal_open(entry);
            }
            let flushed: Vec<SealedChunk> = entry
                .sealed
                .iter()
                .filter(|c| c.max_ts < boundary)
                .cloned()
                .collect();
            if !flushed.is_empty() {
                entry.sealed.retain(|c| c.max_ts >= boundary);
                out.push((entry.labels.clone(), flushed));
            }
            if entry.sealed.is_empty() && entry.open.is_none() {
                // Idle series: garbage-collect (recreated on next append).
                self.map.remove(&entry.labels);
                *slot = None;
                self.live -= 1;
            }
        }
        out
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
        Ok(out)
    }

    /// Ids and labels of series matching `keep` (head-side query planning).
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
            head.append(id, i, i as f64, 1_000_000).unwrap();
        }
        let s = head.samples(id, 0, i64::MAX).unwrap();
        assert_eq!(s.len(), 130);
        // Crossing a window boundary seals too.
        head.append(id, 1_000_001, 1.0, 1_000_000).unwrap();
        let s = head.samples(id, 0, i64::MAX).unwrap();
        assert_eq!(s.len(), 131);
    }

    #[test]
    fn take_before_extracts_and_gcs() {
        let mut head = Head::new();
        let (a, _) = head.get_or_create(&labels("a"), 10).unwrap();
        let (b, _) = head.get_or_create(&labels("b"), 10).unwrap();
        head.append(a, 100, 1.0, 1000).unwrap();
        head.append(a, 1500, 2.0, 1000).unwrap(); // second window
        head.append(b, 200, 3.0, 1000).unwrap(); // only first window
        let flushed = head.take_before(1000);
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
