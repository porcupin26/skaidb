//! Immutable on-disk blocks: one directory per flushed time window.
//!
//! ```text
//! blocks/b-<seq>/
//!   chunks.dat   — concatenated sealed chunks
//!   series.dat   — sorted (labels → chunk refs) index, loaded in memory
//!   meta.json    — window, counts, compaction level; written last (commit)
//! ```
//!
//! A directory without `meta.json` is an aborted flush and is removed on
//! open. Blocks are never modified — compaction writes a replacement and
//! deletes the inputs; retention deletes whole directories.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::chunk::Sample;
use crate::head::SealedChunk;
use crate::varenc::{put_bytes, put_uvarint, put_varint, Dec};
use crate::{Labels, Result, TsdbError};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BlockMeta {
    pub seq: u64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub series: u64,
    pub samples: u64,
    pub level: u32,
}

#[derive(Debug)]
struct ChunkRef {
    offset: u64,
    len: u32,
    min_ts: i64,
    max_ts: i64,
    count: u16,
}

#[derive(Debug)]
struct BlockSeries {
    labels: Labels,
    chunks: Vec<ChunkRef>,
}

/// An open, immutable block.
#[derive(Debug)]
pub struct Block {
    pub dir: PathBuf,
    pub meta: BlockMeta,
    series: Vec<BlockSeries>,
    /// Label postings over `series` (key → value → indexes), built once at
    /// open — blocks are immutable — so matcher evaluation selects candidate
    /// series instead of testing every series' label set per query.
    postings: HashMap<String, HashMap<String, Vec<u32>>>,
    chunks: File,
}

/// Build the postings map over a block's decoded series.
fn build_postings(series: &[BlockSeries]) -> HashMap<String, HashMap<String, Vec<u32>>> {
    let mut postings: HashMap<String, HashMap<String, Vec<u32>>> = HashMap::new();
    for (i, s) in series.iter().enumerate() {
        for (k, v) in &s.labels {
            postings
                .entry(k.clone())
                .or_default()
                .entry(v.clone())
                .or_default()
                .push(i as u32);
        }
    }
    postings
}

/// Write a block from flushed head data. `series` need not be sorted.
pub fn write_block(
    blocks_dir: &Path,
    seq: u64,
    level: u32,
    mut series: Vec<(Labels, Vec<SealedChunk>)>,
) -> Result<PathBuf> {
    series.sort_by(|a, b| a.0.cmp(&b.0));

    let final_dir = blocks_dir.join(format!("b-{seq:08}"));
    let tmp_dir = blocks_dir.join(format!("tmp-b-{seq:08}"));
    let _ = fs::remove_dir_all(&tmp_dir);
    fs::create_dir_all(&tmp_dir)?;

    // chunks.dat: concatenated chunk bytes, offsets recorded for the index.
    let mut chunks_file = File::create(tmp_dir.join("chunks.dat"))?;
    let mut index = Vec::new();
    let mut offset = 0u64;
    let (mut min_ts, mut max_ts) = (i64::MAX, i64::MIN);
    let mut total_samples = 0u64;
    put_uvarint(&mut index, series.len() as u64);
    for (labels, chunks) in &series {
        put_uvarint(&mut index, labels.len() as u64);
        for (k, v) in labels {
            put_bytes(&mut index, k.as_bytes());
            put_bytes(&mut index, v.as_bytes());
        }
        put_uvarint(&mut index, chunks.len() as u64);
        for c in chunks {
            chunks_file.write_all(&c.data)?;
            put_uvarint(&mut index, offset);
            put_uvarint(&mut index, c.data.len() as u64);
            put_varint(&mut index, c.min_ts);
            put_varint(&mut index, c.max_ts);
            put_uvarint(&mut index, c.count as u64);
            offset += c.data.len() as u64;
            min_ts = min_ts.min(c.min_ts);
            max_ts = max_ts.max(c.max_ts);
            total_samples += c.count as u64;
        }
    }
    chunks_file.sync_all()?;

    let mut series_file = File::create(tmp_dir.join("series.dat"))?;
    series_file.write_all(&index)?;
    series_file.sync_all()?;

    let meta = BlockMeta {
        seq,
        min_ts,
        max_ts,
        series: series.len() as u64,
        samples: total_samples,
        level,
    };
    // meta.json last: its presence commits the block.
    let mut meta_file = File::create(tmp_dir.join("meta.json"))?;
    meta_file.write_all(serde_json::to_string(&meta).expect("meta json").as_bytes())?;
    meta_file.sync_all()?;

    fs::rename(&tmp_dir, &final_dir)?;
    File::open(blocks_dir)?.sync_all()?;
    Ok(final_dir)
}

impl Block {
    /// Open one block directory (must contain `meta.json`).
    pub fn open(dir: &Path) -> Result<Block> {
        let meta_raw = fs::read_to_string(dir.join("meta.json"))?;
        let meta: BlockMeta = serde_json::from_str(&meta_raw)
            .map_err(|e| TsdbError::Corrupt(format!("block meta: {e}")))?;

        let mut index_raw = Vec::new();
        File::open(dir.join("series.dat"))?.read_to_end(&mut index_raw)?;
        let mut d = Dec::new(&index_raw);
        let nseries = d.uvarint()? as usize;
        let mut series = Vec::with_capacity(nseries);
        for _ in 0..nseries {
            let nlabels = d.uvarint()? as usize;
            let mut labels = Vec::with_capacity(nlabels);
            for _ in 0..nlabels {
                let k = d.string()?;
                let v = d.string()?;
                labels.push((k, v));
            }
            let nchunks = d.uvarint()? as usize;
            let mut chunks = Vec::with_capacity(nchunks);
            for _ in 0..nchunks {
                chunks.push(ChunkRef {
                    offset: d.uvarint()?,
                    len: d.uvarint()? as u32,
                    min_ts: d.varint()?,
                    max_ts: d.varint()?,
                    count: d.uvarint()? as u16,
                });
            }
            series.push(BlockSeries { labels, chunks });
        }

        let postings = build_postings(&series);
        Ok(Block {
            dir: dir.to_path_buf(),
            meta,
            series,
            postings,
            chunks: File::open(dir.join("chunks.dat"))?,
        })
    }

    /// Scan the blocks directory, removing aborted temp dirs, returning
    /// blocks sorted by `(min_ts, seq)`.
    pub fn open_all(blocks_dir: &Path) -> Result<Vec<Block>> {
        fs::create_dir_all(blocks_dir)?;
        let mut blocks = Vec::new();
        for entry in fs::read_dir(blocks_dir)? {
            let path = entry?.path();
            if !path.is_dir() {
                continue;
            }
            let name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
            if name.starts_with("tmp-") {
                let _ = fs::remove_dir_all(&path);
                continue;
            }
            if name.starts_with("b-") {
                blocks.push(Block::open(&path)?);
            }
        }
        blocks.sort_by_key(|b| (b.meta.min_ts, b.meta.seq));
        Ok(blocks)
    }

    fn read_chunk(&self, r: &ChunkRef) -> Result<Vec<Sample>> {
        let mut file = &self.chunks;
        file.seek(SeekFrom::Start(r.offset))?;
        let mut buf = vec![0u8; r.len as usize];
        file.read_exact(&mut buf)?;
        crate::chunk::decode(&buf)
    }

    /// Sealed chunks per matching series (compaction reads them verbatim).
    pub fn raw_series(&self) -> Result<Vec<(Labels, Vec<SealedChunk>)>> {
        let mut out = Vec::with_capacity(self.series.len());
        for s in &self.series {
            let mut chunks = Vec::with_capacity(s.chunks.len());
            for r in &s.chunks {
                let mut file = &self.chunks;
                file.seek(SeekFrom::Start(r.offset))?;
                let mut data = vec![0u8; r.len as usize];
                file.read_exact(&mut data)?;
                chunks.push(SealedChunk {
                    min_ts: r.min_ts,
                    max_ts: r.max_ts,
                    count: r.count,
                    data,
                });
            }
            out.push((s.labels.clone(), chunks));
        }
        Ok(out)
    }

    /// The label sets of every series in this block.
    pub fn series_labels(&self) -> Vec<Labels> {
        self.series.iter().map(|s| s.labels.clone()).collect()
    }

    /// Samples in `[t0, t1]` for series accepted by `keep`.
    /// [`Block::query`] driven by matchers: candidate series come from the
    /// block's postings (smallest Eq posting, else a regex over the label's
    /// value dictionary) instead of testing every series, then re-check the
    /// full matcher set. Falls back to the full walk when no matcher can
    /// restrict.
    pub fn query_matchers(
        &self,
        matchers: &[crate::store::Matcher],
        t0: i64,
        t1: i64,
    ) -> Result<Vec<(Labels, Vec<Sample>)>> {
        if t1 < self.meta.min_ts || t0 > self.meta.max_ts {
            return Ok(Vec::new());
        }
        let keep = |l: &Labels| matchers.iter().all(|m| m.accepts(l));
        match crate::store::postings_candidates(
            matchers,
            |k, v| {
                self.postings
                    .get(k)
                    .and_then(|values| values.get(v))
                    .cloned()
                    .unwrap_or_default()
            },
            |k, re| {
                let mut out = Vec::new();
                if let Some(values) = self.postings.get(k) {
                    for (v, ids) in values {
                        if re.is_match(v) {
                            out.extend_from_slice(ids);
                        }
                    }
                }
                out
            },
        ) {
            Some(candidates) => {
                let mut out = Vec::new();
                for i in candidates {
                    let Some(s) = self.series.get(i as usize) else { continue };
                    if keep(&s.labels) {
                        if let Some(hit) = self.read_series(s, t0, t1)? {
                            out.push(hit);
                        }
                    }
                }
                Ok(out)
            }
            None => self.query(keep, t0, t1),
        }
    }

    /// Read one series' samples in `[t0, t1]`; `None` when empty.
    fn read_series(
        &self,
        s: &BlockSeries,
        t0: i64,
        t1: i64,
    ) -> Result<Option<(Labels, Vec<Sample>)>> {
        let mut samples = Vec::new();
        for r in &s.chunks {
            if r.max_ts < t0 || r.min_ts > t1 {
                continue;
            }
            for sample in self.read_chunk(r)? {
                if sample.ts >= t0 && sample.ts <= t1 {
                    samples.push(sample);
                }
            }
        }
        Ok((!samples.is_empty()).then(|| (s.labels.clone(), samples)))
    }

    pub fn query(
        &self,
        keep: impl Fn(&Labels) -> bool,
        t0: i64,
        t1: i64,
    ) -> Result<Vec<(Labels, Vec<Sample>)>> {
        if t1 < self.meta.min_ts || t0 > self.meta.max_ts {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for s in &self.series {
            if !keep(&s.labels) {
                continue;
            }
            let mut samples = Vec::new();
            for r in &s.chunks {
                if r.max_ts < t0 || r.min_ts > t1 {
                    continue;
                }
                for sample in self.read_chunk(r)? {
                    if sample.ts >= t0 && sample.ts <= t1 {
                        samples.push(sample);
                    }
                }
            }
            if !samples.is_empty() {
                out.push((s.labels.clone(), samples));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::ChunkBuilder;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tsdb-block-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn sealed(ts0: i64, n: i64) -> SealedChunk {
        let mut b = ChunkBuilder::new();
        for i in 0..n {
            b.append(ts0 + i * 1000, i as f64).unwrap();
        }
        SealedChunk {
            min_ts: ts0,
            max_ts: ts0 + (n - 1) * 1000,
            count: n as u16,
            data: b.seal(),
        }
    }

    #[test]
    fn write_open_query_roundtrip() {
        let dir = temp_dir("rt");
        let series = vec![
            (
                vec![("host".into(), "b".into())],
                vec![sealed(0, 10), sealed(100_000, 5)],
            ),
            (vec![("host".into(), "a".into())], vec![sealed(0, 3)]),
        ];
        write_block(&dir, 1, 0, series).unwrap();
        let blocks = Block::open_all(&dir).unwrap();
        assert_eq!(blocks.len(), 1);
        let b = &blocks[0];
        assert_eq!(b.meta.series, 2);
        assert_eq!(b.meta.samples, 18);

        // All series, full range.
        let all = b.query(|_| true, 0, i64::MAX).unwrap();
        assert_eq!(all.len(), 2);
        // Sorted by labels: a first.
        assert_eq!(all[0].0[0].1, "a");
        assert_eq!(all[1].1.len(), 15);

        // Time filter hits only the second chunk.
        let late = b
            .query(|l| l[0].1 == "b", 100_000, i64::MAX)
            .unwrap();
        assert_eq!(late.len(), 1);
        assert_eq!(late[0].1.len(), 5);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn aborted_tmp_dir_is_cleaned() {
        let dir = temp_dir("abort");
        fs::create_dir_all(dir.join("tmp-b-00000001")).unwrap();
        let blocks = Block::open_all(&dir).unwrap();
        assert!(blocks.is_empty());
        assert!(!dir.join("tmp-b-00000001").exists());
        let _ = fs::remove_dir_all(&dir);
    }
}
