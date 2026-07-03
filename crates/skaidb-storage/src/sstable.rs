//! Immutable sorted-string tables with block compression (SPEC §12).
//!
//! A memtable flush writes its latest version per key to an SSTable: key-sorted
//! entries grouped into fixed-ish **blocks**, each compressed independently with
//! the table's [`Codec`]. A block index (first key → file offset + sizes) plus a
//! Bloom filter make a point read decompress just one block; a scan decompresses
//! all of them. Files are never mutated; compaction writes new files.
//!
//! Layout: `[block0][block1]...[index][bloom][footer(40)]`, integers little-endian.
//! - block:  the codec-compressed bytes of a run of entries
//! - entry (uncompressed): `u32 keylen | key | hlc[12] | u8 op | (Put) u32 vlen | val`
//! - index:  `u64 nblocks | (u32 keylen | first_key | u64 offset | u32 comp | u32 uncomp)*`
//! - bloom:  the bytes of [`Bloom::encode`]
//! - footer: `u64 index_off | u64 bloom_off | u64 entry_count | u64 codec | u64 magic`

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::posfile::read_exact_at;

use crate::bloom::Bloom;
use crate::compress::{compress, decompress, Codec};
use crate::error::{Result, StorageError};
use crate::hlc::Hlc;
use crate::memtable::VersionValue;

const MAGIC: u64 = 0x736b_6169_6462_5354; // "skaidbST"
const FOOTER_LEN: u64 = 40;
const OP_PUT: u8 = 0;
const OP_DELETE: u8 = 1;
const BLOOM_FP_RATE: f64 = 0.01;
/// Target uncompressed size of a data block before it is sealed.
const BLOCK_TARGET: usize = 4096;

/// One entry as stored in (or read from) an SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstEntry {
    pub key: Vec<u8>,
    pub hlc: Hlc,
    pub value: VersionValue,
}

/// In-memory description of one on-disk compressed block.
#[derive(Debug, Clone)]
struct BlockMeta {
    first_key: Vec<u8>,
    offset: u64,
    comp_len: u32,
    uncomp_len: u32,
}

/// A handle to an immutable on-disk SSTable.
#[derive(Debug)]
pub struct SsTable {
    file: File,
    path: PathBuf,
    codec: Codec,
    blocks: Vec<BlockMeta>,
    bloom: Bloom,
    entry_count: u64,
    /// On-disk file size, captured at write/open time (files are immutable) so
    /// metrics scrapes don't stat every table.
    disk_len: u64,
    /// Bounded FIFO cache of decompressed data blocks, keyed by file offset.
    /// Only the point-read path populates it — multi-row index resolution hits
    /// the same block repeatedly, while scans stream each block exactly once
    /// and would only churn it. Populated lazily; immutable files can't go
    /// stale.
    block_cache: BlockCache,
}

/// A small per-table cache of decompressed blocks (see [`SsTable::block_cache`]).
#[derive(Debug, Default)]
struct BlockCache {
    inner: Mutex<BlockCacheInner>,
}

#[derive(Debug, Default)]
struct BlockCacheInner {
    map: HashMap<u64, Arc<Vec<u8>>>,
    fifo: VecDeque<u64>,
}

/// Blocks retained per table: 32 × ~4 KiB ≈ 128 KiB, allocated only for
/// tables that actually serve point reads.
const BLOCK_CACHE_BLOCKS: usize = 32;

impl BlockCache {
    fn get(&self, offset: u64) -> Option<Arc<Vec<u8>>> {
        self.inner
            .lock()
            .expect("block cache")
            .map
            .get(&offset)
            .cloned()
    }

    fn insert(&self, offset: u64, block: Arc<Vec<u8>>) {
        let mut guard = self.inner.lock().expect("block cache");
        let inner = &mut *guard;
        if inner.map.insert(offset, block).is_none() {
            inner.fifo.push_back(offset);
        }
        while inner.map.len() > BLOCK_CACHE_BLOCKS {
            match inner.fifo.pop_front() {
                Some(old) => {
                    inner.map.remove(&old);
                }
                None => break,
            }
        }
    }
}

impl SsTable {
    /// Write `entries` (sorted by key, unique) to a new SSTable using `codec`.
    pub fn write(path: impl AsRef<Path>, entries: &[SstEntry], codec: Codec) -> Result<SsTable> {
        SsTable::write_stream(
            path,
            entries.iter().cloned().map(Ok),
            entries.len(),
            codec,
        )
    }

    /// Write a stream of entries (sorted by key, unique) to a new SSTable,
    /// compressing and writing each block as it fills — peak memory is one
    /// block plus the index, not the whole table. `expected_entries` sizes the
    /// Bloom filter; overestimating is safe (it only lowers the FP rate).
    pub fn write_stream(
        path: impl AsRef<Path>,
        entries: impl Iterator<Item = Result<SstEntry>>,
        expected_entries: usize,
        codec: Codec,
    ) -> Result<SsTable> {
        use std::io::BufWriter;
        let path = path.as_ref().to_path_buf();
        let mut writer = BufWriter::new(File::create(&path)?);
        let mut blocks: Vec<BlockMeta> = Vec::new();
        let mut bloom = Bloom::with_capacity(expected_entries, BLOOM_FP_RATE);
        let mut offset: u64 = 0;
        let mut entry_count: u64 = 0;

        // Group entries into uncompressed blocks of ~BLOCK_TARGET bytes; seal,
        // compress and write each block as soon as it fills.
        let mut buf = Vec::with_capacity(BLOCK_TARGET + BLOCK_TARGET / 4);
        let mut first: Option<Vec<u8>> = None;
        let mut seal =
            |buf: &mut Vec<u8>, first: &mut Option<Vec<u8>>, writer: &mut BufWriter<File>|
             -> Result<BlockMeta> {
                let comp = compress(codec, buf);
                writer.write_all(&comp)?;
                let meta = BlockMeta {
                    first_key: first.take().unwrap(),
                    offset,
                    comp_len: comp.len() as u32,
                    uncomp_len: buf.len() as u32,
                };
                offset += comp.len() as u64;
                buf.clear();
                Ok(meta)
            };
        for entry in entries {
            let e = entry?;
            if first.is_none() {
                first = Some(e.key.clone());
            }
            bloom.add(&e.key);
            entry_count += 1;
            encode_entry(&mut buf, &e);
            if buf.len() >= BLOCK_TARGET {
                blocks.push(seal(&mut buf, &mut first, &mut writer)?);
            }
        }
        if !buf.is_empty() {
            blocks.push(seal(&mut buf, &mut first, &mut writer)?);
        }

        // Index block.
        let index_off = offset;
        let mut tail = Vec::new();
        tail.extend_from_slice(&(blocks.len() as u64).to_le_bytes());
        for b in &blocks {
            tail.extend_from_slice(&(b.first_key.len() as u32).to_le_bytes());
            tail.extend_from_slice(&b.first_key);
            tail.extend_from_slice(&b.offset.to_le_bytes());
            tail.extend_from_slice(&b.comp_len.to_le_bytes());
            tail.extend_from_slice(&b.uncomp_len.to_le_bytes());
        }

        // Bloom block.
        let bloom_off = index_off + tail.len() as u64;
        tail.extend_from_slice(&bloom.encode());

        // Footer.
        tail.extend_from_slice(&index_off.to_le_bytes());
        tail.extend_from_slice(&bloom_off.to_le_bytes());
        tail.extend_from_slice(&entry_count.to_le_bytes());
        tail.extend_from_slice(&(codec.to_u8() as u64).to_le_bytes());
        tail.extend_from_slice(&MAGIC.to_le_bytes());
        writer.write_all(&tail)?;
        let disk_len = offset + tail.len() as u64;

        let file = writer
            .into_inner()
            .map_err(|e| std::io::Error::from(e.error().kind()))?;
        file.sync_all()?;
        drop(file);
        let file = File::open(&path)?;

        Ok(SsTable {
            file,
            path,
            codec,
            blocks,
            bloom,
            entry_count,
            disk_len,
            block_cache: BlockCache::default(),
        })
    }

    /// Open an existing SSTable, loading its block index and Bloom filter.
    pub fn open(path: impl AsRef<Path>) -> Result<SsTable> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let file_len = file.metadata()?.len();
        if file_len < FOOTER_LEN {
            return Err(corrupt("file shorter than footer"));
        }

        let mut footer = [0u8; FOOTER_LEN as usize];
        read_exact_at(&file, &mut footer, file_len - FOOTER_LEN)?;
        let index_off = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let bloom_off = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let entry_count = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let codec = Codec::from_u8(footer[24]).ok_or_else(|| corrupt("unknown codec"))?;
        let magic = u64::from_le_bytes(footer[32..40].try_into().unwrap());
        if magic != MAGIC {
            return Err(corrupt("bad magic"));
        }
        if index_off > bloom_off || bloom_off > file_len - FOOTER_LEN {
            return Err(corrupt("inconsistent footer offsets"));
        }

        let index_len = (bloom_off - index_off) as usize;
        let mut index_buf = vec![0u8; index_len];
        read_exact_at(&file, &mut index_buf, index_off)?;
        let blocks = parse_block_index(&index_buf)?;

        let bloom_len = (file_len - FOOTER_LEN - bloom_off) as usize;
        let mut bloom_buf = vec![0u8; bloom_len];
        read_exact_at(&file, &mut bloom_buf, bloom_off)?;
        let bloom = Bloom::decode(&bloom_buf).ok_or_else(|| corrupt("bad bloom block"))?;

        Ok(SsTable {
            file,
            path,
            codec,
            blocks,
            bloom,
            entry_count,
            disk_len: file_len,
            block_cache: BlockCache::default(),
        })
    }

    /// Number of entries in this table.
    pub fn len(&self) -> u64 {
        self.entry_count
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Path backing this table.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// On-disk size of this table in bytes (captured at write/open time).
    pub fn disk_len(&self) -> u64 {
        self.disk_len
    }

    /// Point lookup: returns the stored version for `key` if present.
    pub fn get(&self, key: &[u8]) -> Result<Option<(Hlc, VersionValue)>> {
        if !self.bloom.contains(key) {
            return Ok(None);
        }
        // The block that may contain `key` is the last one whose first key <= key.
        let i = self.blocks.partition_point(|b| b.first_key.as_slice() <= key);
        if i == 0 {
            return Ok(None);
        }
        let block = self.read_block_cached(&self.blocks[i - 1])?;
        let mut pos = 0;
        // Compare keys borrowed from the block; only the matching entry's
        // key/value bytes are ever copied out.
        while pos < block.len() {
            let (entry_key, rest) = peek_entry_key(&block, pos)?;
            if entry_key == key {
                let (entry, _) = decode_entry(&block, pos)?;
                return Ok(Some((entry.hlc, entry.value)));
            }
            if entry_key > key {
                break; // entries are sorted; past the key
            }
            pos = rest;
        }
        Ok(None)
    }

    /// Entries whose key is in `[start, end)`, in key order. Seeks to the first
    /// relevant block via the block index and stops once past `end`, so only the
    /// covering blocks are read and decompressed — not the whole table.
    pub fn range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Result<Vec<SstEntry>> {
        // First block that may hold `start`: the last block whose first key <= start.
        let first = match start {
            Some(s) => self
                .blocks
                .partition_point(|b| b.first_key.as_slice() <= s)
                .saturating_sub(1),
            None => 0,
        };
        let mut out = Vec::new();
        for meta in &self.blocks[first.min(self.blocks.len())..] {
            // Every key in this block is >= its first key; if that already
            // reaches `end`, no later block can contribute.
            if let Some(e) = end {
                if meta.first_key.as_slice() >= e {
                    break;
                }
            }
            let block = self.read_block(meta)?;
            let mut pos = 0;
            while pos < block.len() {
                let (entry, next) = decode_entry(&block, pos)?;
                pos = next;
                if start.is_some_and(|s| entry.key.as_slice() < s) {
                    continue;
                }
                if end.is_some_and(|e| entry.key.as_slice() >= e) {
                    return Ok(out); // sorted: nothing further can be in range
                }
                out.push(entry);
            }
        }
        Ok(out)
    }

    /// Read every entry in key order (used by scans and compaction).
    pub fn entries(&self) -> Result<Vec<SstEntry>> {
        let mut out = Vec::with_capacity(self.entry_count as usize);
        for entry in self.iter() {
            out.push(entry?);
        }
        Ok(out)
    }

    /// Stream every entry in key order, reading and decompressing one block at
    /// a time — O(block) memory instead of materializing the table.
    pub fn iter(&self) -> SsTableIter<'_> {
        SsTableIter {
            table: self,
            next_block: 0,
            block: Vec::new(),
            pos: 0,
        }
    }

    /// Read and decompress one block from disk.
    fn read_block(&self, meta: &BlockMeta) -> Result<Vec<u8>> {
        let mut comp = vec![0u8; meta.comp_len as usize];
        read_exact_at(&self.file, &mut comp, meta.offset)?;
        decompress(self.codec, &comp, meta.uncomp_len as usize)
    }

    /// Point-read variant of [`SsTable::read_block`]: serve the decompressed
    /// block from the table's block cache, reading and caching it on a miss —
    /// N rows resolved out of the same block decompress it once, not N times.
    fn read_block_cached(&self, meta: &BlockMeta) -> Result<Arc<Vec<u8>>> {
        if let Some(block) = self.block_cache.get(meta.offset) {
            return Ok(block);
        }
        let block = Arc::new(self.read_block(meta)?);
        self.block_cache.insert(meta.offset, Arc::clone(&block));
        Ok(block)
    }
}

/// Streaming iterator over one SSTable's entries in key order. Holds at most
/// one decompressed block at a time.
#[derive(Debug)]
pub struct SsTableIter<'a> {
    table: &'a SsTable,
    /// Index of the next block to load.
    next_block: usize,
    /// Currently loaded (decompressed) block, empty until first load.
    block: Vec<u8>,
    /// Decode position within `block`.
    pos: usize,
}

impl Iterator for SsTableIter<'_> {
    type Item = Result<SstEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos >= self.block.len() {
            let meta = self.table.blocks.get(self.next_block)?;
            self.next_block += 1;
            self.pos = 0;
            match self.table.read_block(meta) {
                Ok(block) => self.block = block,
                Err(e) => {
                    // Poison the iterator so a caller that keeps polling stops.
                    self.next_block = self.table.blocks.len();
                    self.block = Vec::new();
                    return Some(Err(e));
                }
            }
        }
        match decode_entry(&self.block, self.pos) {
            Ok((entry, next)) => {
                self.pos = next;
                Some(Ok(entry))
            }
            Err(e) => {
                self.next_block = self.table.blocks.len();
                self.block = Vec::new();
                self.pos = 0;
                Some(Err(e))
            }
        }
    }
}

/// Borrow an entry's key from `buf` at `start` without copying, returning the
/// offset of the following entry.
fn peek_entry_key(buf: &[u8], start: usize) -> Result<(&[u8], usize)> {
    let mut pos = start;
    let key_len = read_u32(buf, &mut pos)? as usize;
    let key = take(buf, &mut pos, key_len)?;
    pos += 12; // hlc
    let op = *buf.get(pos).ok_or_else(|| corrupt("missing op"))?;
    pos += 1;
    match op {
        OP_PUT => {
            let val_len = read_u32(buf, &mut pos)? as usize;
            pos = pos
                .checked_add(val_len)
                .filter(|end| *end <= buf.len())
                .ok_or_else(|| corrupt("unexpected end"))?;
        }
        OP_DELETE => {}
        _ => return Err(corrupt("unknown op")),
    }
    Ok((key, pos))
}

fn encode_entry(out: &mut Vec<u8>, e: &SstEntry) {
    out.extend_from_slice(&(e.key.len() as u32).to_le_bytes());
    out.extend_from_slice(&e.key);
    out.extend_from_slice(&e.hlc.to_bytes());
    match &e.value {
        VersionValue::Put(val) => {
            out.push(OP_PUT);
            out.extend_from_slice(&(val.len() as u32).to_le_bytes());
            out.extend_from_slice(val);
        }
        VersionValue::Delete => out.push(OP_DELETE),
    }
}

fn decode_entry(buf: &[u8], start: usize) -> Result<(SstEntry, usize)> {
    let mut pos = start;
    let key_len = read_u32(buf, &mut pos)? as usize;
    let key = take(buf, &mut pos, key_len)?.to_vec();
    let hlc_bytes: [u8; 12] = take(buf, &mut pos, 12)?
        .try_into()
        .map_err(|_| corrupt("bad hlc"))?;
    let hlc = Hlc::from_bytes(hlc_bytes);
    let op = *take(buf, &mut pos, 1)?
        .first()
        .ok_or_else(|| corrupt("missing op"))?;
    let value = match op {
        OP_PUT => {
            let val_len = read_u32(buf, &mut pos)? as usize;
            VersionValue::Put(take(buf, &mut pos, val_len)?.to_vec())
        }
        OP_DELETE => VersionValue::Delete,
        _ => return Err(corrupt("unknown op")),
    };
    Ok((SstEntry { key, hlc, value }, pos))
}

fn parse_block_index(buf: &[u8]) -> Result<Vec<BlockMeta>> {
    let mut pos = 0;
    let n = read_u64(buf, &mut pos)? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let key_len = read_u32(buf, &mut pos)? as usize;
        let first_key = take(buf, &mut pos, key_len)?.to_vec();
        let offset = read_u64(buf, &mut pos)?;
        let comp_len = read_u32(buf, &mut pos)?;
        let uncomp_len = read_u32(buf, &mut pos)?;
        out.push(BlockMeta {
            first_key,
            offset,
            comp_len,
            uncomp_len,
        });
    }
    Ok(out)
}

fn take<'a>(buf: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8]> {
    let end = pos
        .checked_add(n)
        .ok_or_else(|| corrupt("length overflow"))?;
    let slice = buf
        .get(*pos..end)
        .ok_or_else(|| corrupt("unexpected end"))?;
    *pos = end;
    Ok(slice)
}

fn read_u32(buf: &[u8], pos: &mut usize) -> Result<u32> {
    Ok(u32::from_le_bytes(take(buf, pos, 4)?.try_into().unwrap()))
}

fn read_u64(buf: &[u8], pos: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(take(buf, pos, 8)?.try_into().unwrap()))
}

fn corrupt(detail: &'static str) -> StorageError {
    StorageError::Corruption { offset: 0, detail }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmp() -> PathBuf {
        static C: AtomicU64 = AtomicU64::new(0);
        let n = C.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!("skaidb-sst-{}-{n}.sst", std::process::id()));
        p
    }

    fn put(key: &str, hlc: u64, val: &str) -> SstEntry {
        SstEntry {
            key: key.as_bytes().to_vec(),
            hlc: Hlc::new(hlc, 0),
            value: VersionValue::Put(val.as_bytes().to_vec()),
        }
    }

    #[test]
    fn write_get_roundtrip() {
        for codec in [Codec::None, Codec::Lz4, Codec::Brotli] {
            let path = tmp();
            let entries = vec![put("a", 1, "1"), put("b", 2, "2"), put("c", 3, "3")];
            let sst = SsTable::write(&path, &entries, codec).unwrap();
            assert_eq!(
                sst.get(b"b").unwrap(),
                Some((Hlc::new(2, 0), VersionValue::Put(b"2".to_vec()))),
                "codec {codec:?}"
            );
            assert_eq!(sst.get(b"z").unwrap(), None);
            let _ = std::fs::remove_file(&path);
        }
    }

    #[test]
    fn many_entries_span_blocks_and_roundtrip() {
        // Enough entries to span multiple 4 KiB blocks.
        let path = tmp();
        let entries: Vec<SstEntry> = (0..2000)
            .map(|i| put(&format!("key{i:05}"), i as u64 + 1, &format!("value-{i}")))
            .collect();
        let sst = SsTable::write(&path, &entries, Codec::Lz4).unwrap();
        assert!(sst.blocks.len() > 1, "should span multiple blocks");
        // Spot-check point reads and a full scan.
        assert_eq!(
            sst.get(b"key01234").unwrap(),
            Some((Hlc::new(1235, 0), VersionValue::Put(b"value-1234".to_vec())))
        );
        assert_eq!(sst.entries().unwrap(), entries);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reopen_reads_same() {
        let path = tmp();
        let entries = vec![
            put("alpha", 1, "x"),
            SstEntry {
                key: b"beta".to_vec(),
                hlc: Hlc::new(5, 0),
                value: VersionValue::Delete,
            },
            put("gamma", 2, "y"),
        ];
        SsTable::write(&path, &entries, Codec::Brotli).unwrap();
        let sst = SsTable::open(&path).unwrap();
        assert_eq!(sst.len(), 3);
        assert_eq!(sst.codec, Codec::Brotli);
        assert_eq!(
            sst.get(b"beta").unwrap(),
            Some((Hlc::new(5, 0), VersionValue::Delete))
        );
        assert_eq!(sst.entries().unwrap(), entries);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn detects_bad_magic() {
        let path = tmp();
        SsTable::write(&path, &[put("a", 1, "1")], Codec::Lz4).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        let len = bytes.len();
        bytes[len - 1] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(SsTable::open(&path).is_err());
        let _ = std::fs::remove_file(&path);
    }
}
