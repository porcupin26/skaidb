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

/// Pick one group of adjacent, same-level blocks to merge (lowest level
/// first). `blocks` must be sorted by `(min_ts, seq)`. Returns indices.
pub fn plan(blocks: &[Block], block_span: i64) -> Option<Vec<usize>> {
    for level in 0..8u32 {
        let target = tier_span(block_span, level + 1);
        let mut group: Vec<usize> = Vec::new();
        for (i, b) in blocks.iter().enumerate() {
            if b.meta.level != level {
                if group.len() >= 2 {
                    return Some(group);
                }
                group.clear();
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
pub fn merge(blocks_dir: &Path, seq: u64, inputs: &[&Block]) -> Result<()> {
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
    write_block(blocks_dir, seq, level, merged)?;
    for block in inputs {
        fs::remove_dir_all(&block.dir)?;
    }
    Ok(())
}

/// Delete blocks whose entire window is older than `cutoff_ts`. Returns how
/// many were dropped.
pub fn drop_expired(blocks: &mut Vec<Block>, cutoff_ts: i64) -> Result<usize> {
    let mut dropped = 0;
    let mut kept = Vec::with_capacity(blocks.len());
    for block in blocks.drain(..) {
        if block.meta.max_ts < cutoff_ts {
            fs::remove_dir_all(&block.dir)?;
            dropped += 1;
        } else {
            kept.push(block);
        }
    }
    *blocks = kept;
    Ok(dropped)
}
