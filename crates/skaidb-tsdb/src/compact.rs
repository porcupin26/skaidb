//! Block compaction and retention.
//!
//! Blocks tier up 4× per level (2 h → 8 h → 32 h with the default span):
//! adjacent same-level blocks whose union fits the next tier merge into one,
//! cutting per-query index lookups. Chunks are carried over verbatim — no
//! re-encode — so compaction is I/O-bound and cheap. Retention deletes whole
//! expired directories: O(1), no tombstones.

use std::fs;
use std::path::Path;

use crate::block::{write_block, Block};
use crate::head::SealedChunk;
use crate::{Labels, Result};

/// Level-`l` blocks tier up when their union spans at most `span * 4^(l+1)`.
fn tier_span(block_span: i64, level: u32) -> i64 {
    block_span.saturating_mul(4i64.saturating_pow(level))
}

/// Pick one group of same-level blocks (lowest level first) whose union
/// fits one aligned next-tier window. `blocks` must be sorted by
/// `(min_ts, seq)`. Other-level blocks interleaved between group members
/// are SKIPPED, not group-resetting: compaction products of different
/// generations share windows (a wide-fold's level-0 outputs sit next to
/// the window's existing level-1 block), and resetting on them left each
/// product stranded as a run of one — the group members may then overlap
/// in time, which `merge`'s decode-dedupe-rechunk path folds correctly.
pub fn plan(blocks: &[Block], block_span: i64) -> Option<Vec<usize>> {
    for level in 0..8u32 {
        let target = tier_span(block_span, level + 1);
        let mut group: Vec<usize> = Vec::new();
        for (i, b) in blocks.iter().enumerate() {
            if b.meta.level != level {
                continue;
            }
            // Would adding this block keep the group within one aligned
            // next-tier window?
            let start = group
                .first()
                .map_or(b.meta.min_ts, |&f| blocks[f].meta.min_ts);
            let window = start.div_euclid(target) * target;
            if b.meta.max_ts < window + target {
                group.push(i);
            } else {
                if group.len() >= 2 {
                    return Some(group);
                }
                group.clear();
                let window = b.meta.min_ts.div_euclid(target) * target;
                if b.meta.max_ts < window + target {
                    group.push(i);
                }
            }
        }
        if group.len() >= 2 {
            return Some(group);
        }
    }
    None
}

/// Merge the given blocks into one at `level + 1`, writing the new block
/// under `blocks_dir` with `seq`, then deleting the inputs. Chunks for the
/// same series concatenate in time order (block windows are disjoint).
/// Returns the new block's directory so the caller can update its block
/// list incrementally — re-listing the whole blocks directory per merge is
/// O(blocks) and was what kept a 390k-block backlog from ever draining
/// (2026-07-22 incident). Input-directory removal is best-effort: a
/// leftover input holds duplicate data that query-time dedupe and the next
/// compaction pass fold away, while an ERROR here used to abort the flush
/// with the store's in-memory state out of sync with disk.
pub fn merge(blocks_dir: &Path, seq: u64, inputs: &[&Block]) -> Result<std::path::PathBuf> {
    let level = inputs.iter().map(|b| b.meta.level).max().unwrap_or(0) + 1;
    // Inputs are time-ordered, so appending per series preserves chunk order.
    let mut merged: Vec<(Labels, Vec<SealedChunk>)> = Vec::new();
    let mut index: std::collections::HashMap<Labels, usize> = std::collections::HashMap::new();
    for block in inputs {
        for (labels, chunks) in block.raw_series()? {
            match index.get(&labels) {
                Some(&i) => merged[i].1.extend(chunks),
                None => {
                    index.insert(labels.clone(), merged.len());
                    merged.push((labels, chunks));
                }
            }
        }
    }
    // A series whose concatenated chunks overlap in time (repair-merged
    // blocks) is decoded, deduplicated, and re-chunked so duplicates don't
    // accumulate across compactions.
    for (_, chunks) in &mut merged {
        let overlapping = chunks
            .windows(2)
            .any(|w| w[1].min_ts <= w[0].max_ts);
        if overlapping {
            let mut samples = Vec::new();
            for c in chunks.iter() {
                samples.extend(crate::chunk::decode(&c.data)?);
            }
            samples.sort_by_key(|s| s.ts);
            samples.dedup_by(|later, earlier| {
                if later.ts == earlier.ts {
                    earlier.value = later.value;
                    true
                } else {
                    false
                }
            });
            // Re-chunk with a huge span: the merged block already owns the
            // whole window, so only the 120-sample cut applies.
            *chunks = crate::head::rechunk(&samples, i64::MAX)?;
        }
    }
    let new_dir = write_block(blocks_dir, seq, level, merged)?;
    for block in inputs {
        let _ = fs::remove_dir_all(&block.dir);
    }
    Ok(new_dir)
}

/// Pick level-0 blocks that can never join a normal [`plan`] group: a
/// group's union must fit one ALIGNED next-tier window, so a block that
/// is wider than the window — or narrower but straddling an aligned
/// boundary — is permanently stranded. Merge-of-scattered-samples ingest
/// (repair/hint merges of samples spread across time) produces both
/// shapes; append-path flushes never do (they cut at window boundaries).
/// Capped by input count AND total samples (the fold decodes everything).
pub fn plan_wide(
    blocks: &[Block],
    block_span: i64,
    max_inputs: usize,
    max_samples: u64,
) -> Option<Vec<usize>> {
    let t1 = tier_span(block_span, 1);
    let mut group = Vec::new();
    let mut samples = 0u64;
    for (i, b) in blocks.iter().enumerate() {
        let window = b.meta.min_ts.div_euclid(t1) * t1;
        let unalignable = b.meta.max_ts >= window + t1;
        if b.meta.level == 0 && unalignable {
            group.push(i);
            samples += b.meta.samples;
            if group.len() >= max_inputs || samples >= max_samples {
                break;
            }
        }
    }
    // A single stranded block still folds (into its aligned windows) —
    // unlike a normal merge, one input is not a no-op here.
    if group.is_empty() {
        None
    } else {
        Some(group)
    }
}

/// One aligned window's rebuilt series set from a wide fold.
pub type FoldedWindow = (i64, Vec<(Labels, Vec<SealedChunk>)>);

/// Fold wide blocks by decoding every sample and regrouping into
/// span-ALIGNED windows: per window, per series, sorted + deduped +
/// re-chunked. The caller writes one narrow level-0 block per window —
/// narrow products join normal tiering (and expire via retention) instead
/// of re-forming wide blocks, so repeated folds converge. Pure read; the
/// caller owns block writing and input removal (seq allocation needs the
/// store's mutable state).
pub fn wide_fold_collect(
    inputs: &[&Block],
    block_span: i64,
) -> Result<Vec<FoldedWindow>> {
    use std::collections::BTreeMap;
    let mut per: BTreeMap<i64, BTreeMap<Labels, Vec<crate::Sample>>> = BTreeMap::new();
    for block in inputs {
        for (labels, chunks) in block.raw_series()? {
            for c in &chunks {
                for s in crate::chunk::decode(&c.data)? {
                    let window = s.ts.div_euclid(block_span) * block_span;
                    per.entry(window)
                        .or_default()
                        .entry(labels.clone())
                        .or_default()
                        .push(s);
                }
            }
        }
    }
    let mut out = Vec::with_capacity(per.len());
    for (window, series) in per {
        let mut rebuilt = Vec::with_capacity(series.len());
        for (labels, mut samples) in series {
            samples.sort_by_key(|s| s.ts);
            samples.dedup_by(|later, earlier| {
                if later.ts == earlier.ts {
                    earlier.value = later.value;
                    true
                } else {
                    false
                }
            });
            rebuilt.push((labels, crate::head::rechunk(&samples, block_span)?));
        }
        out.push((window, rebuilt));
    }
    Ok(out)
}

/// Delete blocks whose entire window is older than `cutoff_ts`. Returns how
/// many were dropped. Removal is best-effort per block: a directory that
/// fails to delete stays in the list and is retried on the next pass —
/// erroring out used to LOSE the drained-but-unremoved blocks from the
/// in-memory list while their directories stayed on disk.
pub fn drop_expired(blocks: &mut Vec<Block>, cutoff_ts: i64) -> usize {
    let mut dropped = 0;
    let mut kept = Vec::with_capacity(blocks.len());
    for block in blocks.drain(..) {
        if block.meta.max_ts < cutoff_ts && fs::remove_dir_all(&block.dir).is_ok() {
            dropped += 1;
        } else {
            kept.push(block);
        }
    }
    *blocks = kept;
    dropped
}
