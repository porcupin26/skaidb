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
//!
//! Each table also gets a best-effort **stamps sidecar** (`<file>.stamps`):
//! the same entries minus their values (`u32 keylen | key | hlc[12] | u8 op`),
//! in stamp blocks compressed with the table's codec and aligned 1:1 with the
//! data blocks so the shared block index seeks both. Anti-entropy digests scan
//! `(key, hlc, op)` for whole tables; the sidecar answers that without
//! decompressing a single value byte. Sidecars are optional — a missing or
//! invalid one (old files, torn write) falls back to decoding data blocks.
//! Layout: `[sblock0][sblock1]...[(u32 comp | u32 uncomp)*][u64 nblocks][u64 magic]`.

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::posfile::read_exact_at;

use crate::bloom::Bloom;
use crate::compress::{compress, decompress, Codec};
use crate::crypto::{Dek, Kek};
use crate::error::{Result, StorageError};
use crate::hlc::Hlc;
use crate::memtable::VersionValue;

const MAGIC: u64 = 0x736b_6169_6462_5354; // "skaidbST"
/// Magic for an ENCRYPTED SSTable — a distinct trailing value so an encrypted
/// file is told apart from a plaintext one (old files read unchanged).
const MAGIC_ENC: u64 = 0x736b_6169_6462_45ed; // "skaidbE." (byte-distinct)
/// Encrypted footer length: `index_off | bloom_off | entry_count | codec |
/// dek_off | MAGIC_ENC` (6 × u64).
const ENC_FOOTER_LEN: u64 = 48;
/// Wrapped-DEK region length in an encrypted file (nonce + key + tag).
const WRAPPED_DEK_LEN: u64 = 12 + 32 + 16;
const STAMPS_MAGIC: u64 = 0x736b_6169_6462_5350; // "skaidbSP"
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

/// A borrowed view of an entry to write. [`SsTable::write_stream`] is generic
/// over this so a memtable flush streams its (Arc-shared, still readable)
/// entries straight into the block buffer without cloning every key and
/// value first — the writer only ever reads bytes.
pub trait EntryRef {
    fn key(&self) -> &[u8];
    fn hlc(&self) -> Hlc;
    fn value(&self) -> &VersionValue;
}

impl EntryRef for SstEntry {
    fn key(&self) -> &[u8] {
        &self.key
    }
    fn hlc(&self) -> Hlc {
        self.hlc
    }
    fn value(&self) -> &VersionValue {
        &self.value
    }
}

impl EntryRef for &SstEntry {
    fn key(&self) -> &[u8] {
        &self.key
    }
    fn hlc(&self) -> Hlc {
        self.hlc
    }
    fn value(&self) -> &VersionValue {
        &self.value
    }
}

impl EntryRef for (&[u8], Hlc, &VersionValue) {
    fn key(&self) -> &[u8] {
        self.0
    }
    fn hlc(&self) -> Hlc {
        self.1
    }
    fn value(&self) -> &VersionValue {
        self.2
    }
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
    /// Per-file data key when this table is encrypted (`None` = plaintext).
    dek: Option<Dek>,
    blocks: Vec<BlockMeta>,
    bloom: Bloom,
    entry_count: u64,
    /// `(put_versions, delete_versions)` in this file, computed on first use
    /// and cached forever (files are immutable). Version counts, not merged
    /// uniques: a key rewritten across files counts once per file until
    /// compaction collapses it.
    version_counts: std::sync::OnceLock<(u64, u64)>,
    /// On-disk file size, captured at write/open time (files are immutable) so
    /// metrics scrapes don't stat every table.
    disk_len: u64,
    /// Bounded FIFO cache of decompressed data blocks, keyed by file offset.
    /// Only the point-read path populates it — multi-row index resolution hits
    /// the same block repeatedly, while scans stream each block exactly once
    /// and would only churn it. Populated lazily; immutable files can't go
    /// stale.
    block_cache: BlockCache,
    /// Value-free stamps sidecar, when present and consistent with `blocks`.
    /// `None` (old files, failed sidecar write) is always safe — stamp scans
    /// fall back to decoding data blocks.
    stamps: Option<StampsIndex>,
}

/// The sidecar path for an SSTable file: `<file>.stamps` (extension appended,
/// so it can never collide with another table's data file).
pub fn stamps_path(path: &Path) -> PathBuf {
    let mut os = path.as_os_str().to_os_string();
    os.push(".stamps");
    PathBuf::from(os)
}

/// An opened, validated stamps sidecar (see the module docs for the layout).
#[derive(Debug)]
struct StampsIndex {
    file: File,
    /// `(offset, comp_len, uncomp_len)` per stamp block, aligned 1:1 with the
    /// table's data-block index.
    blocks: Vec<(u64, u32, u32)>,
}

/// Open and validate `<main_path>.stamps` against the table's block index.
/// Any inconsistency (missing file, torn write, block-count mismatch) yields
/// `None` — callers fall back to data blocks.
fn load_stamps(main_path: &Path, data_blocks: &[BlockMeta]) -> Option<StampsIndex> {
    let file = File::open(stamps_path(main_path)).ok()?;
    let len = file.metadata().ok()?.len();
    if len < 16 {
        return None;
    }
    let mut foot = [0u8; 16];
    read_exact_at(&file, &mut foot, len - 16).ok()?;
    let n = u64::from_le_bytes(foot[0..8].try_into().unwrap()) as usize;
    let magic = u64::from_le_bytes(foot[8..16].try_into().unwrap());
    if magic != STAMPS_MAGIC || n != data_blocks.len() {
        return None;
    }
    let table_len = (n as u64).checked_mul(8)?;
    let table_off = len.checked_sub(16 + table_len)?;
    let mut table = vec![0u8; table_len as usize];
    read_exact_at(&file, &mut table, table_off).ok()?;
    let mut blocks = Vec::with_capacity(n);
    let mut offset = 0u64;
    for i in 0..n {
        let comp = u32::from_le_bytes(table[i * 8..i * 8 + 4].try_into().unwrap());
        let uncomp = u32::from_le_bytes(table[i * 8 + 4..i * 8 + 8].try_into().unwrap());
        blocks.push((offset, comp, uncomp));
        offset += comp as u64;
    }
    // The concatenated stamp blocks must exactly fill the space before the
    // length table — anything else is a torn or foreign file.
    if offset != table_off {
        return None;
    }
    Some(StampsIndex { file, blocks })
}

/// A small per-table cache of decompressed blocks (see [`SsTable::block_cache`]).
/// Sharded like [`crate::cache::ReadCache`] so concurrent point reads against
/// one hot table don't serialize on a single mutex — a table with many
/// concurrent readers used to funnel every hit *and* every miss-then-insert
/// through one lock regardless of which (unrelated) blocks they touched.
#[derive(Debug)]
struct BlockCache {
    shards: [Mutex<BlockCacheInner>; BLOCK_CACHE_SHARDS],
}

impl Default for BlockCache {
    fn default() -> Self {
        BlockCache {
            shards: std::array::from_fn(|_| Mutex::new(BlockCacheInner::default())),
        }
    }
}

#[derive(Debug, Default)]
struct BlockCacheInner {
    map: HashMap<u64, Arc<Vec<u8>>>,
    fifo: VecDeque<u64>,
}

/// Blocks retained per table, split across [`BLOCK_CACHE_SHARDS`]: 64 total ×
/// ~4 KiB ≈ 256 KiB, allocated only for tables that actually serve point
/// reads.
const BLOCK_CACHE_BLOCKS: usize = 64;
/// Shard count for the block cache. Smaller than `ReadCache`'s 64 — this
/// cache's total budget is far smaller (blocks, not point-read results), so
/// shards need enough capacity each (16 blocks here) to hold a useful working
/// set rather than trading hit rate for lock parallelism that a single small
/// table rarely has enough concurrent-but-independent readers to need.
const BLOCK_CACHE_SHARDS: usize = 4;

impl BlockCache {
    /// A block's shard: offsets from a sequential scan land in adjacent file
    /// positions, so a plain modulo would put a scan's cached blocks all in
    /// one shard — multiply-shift first to decorrelate, same idea as
    /// `ReadCache`'s FNV hash over the key bytes.
    fn shard(&self, offset: u64) -> &Mutex<BlockCacheInner> {
        let h = offset.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        &self.shards[(h % self.shards.len() as u64) as usize]
    }

    fn get(&self, offset: u64) -> Option<Arc<Vec<u8>>> {
        self.shard(offset)
            .lock()
            .expect("block cache")
            .map
            .get(&offset)
            .cloned()
    }

    fn insert(&self, offset: u64, block: Arc<Vec<u8>>) {
        let mut guard = self.shard(offset).lock().expect("block cache");
        let inner = &mut *guard;
        if inner.map.insert(offset, block).is_none() {
            inner.fifo.push_back(offset);
        }
        let per_shard = BLOCK_CACHE_BLOCKS / BLOCK_CACHE_SHARDS;
        while inner.map.len() > per_shard {
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
        SsTable::write_stream(path, entries.iter().map(Ok), entries.len(), codec, None)
    }

    /// Write a stream of entries (sorted by key, unique) to a new SSTable,
    /// compressing and writing each block as it fills — peak memory is one
    /// block plus the index, not the whole table. `expected_entries` sizes the
    /// Bloom filter; overestimating is safe (it only lowers the FP rate).
    pub fn write_stream<E: EntryRef>(
        path: impl AsRef<Path>,
        entries: impl Iterator<Item = Result<E>>,
        expected_entries: usize,
        codec: Codec,
        enc: Option<&Kek>,
    ) -> Result<SsTable> {
        use std::io::BufWriter;
        let path = path.as_ref().to_path_buf();
        // Encrypting: mint a fresh per-file DEK; its KEK-wrapped bytes go in
        // the footer region. Each region is sealed by its byte offset (unique
        // within this immutable file's single DEK).
        let (dek, wrapped_dek) = match enc {
            Some(kek) => {
                let (d, w) = kek.wrap_new_dek()?;
                (Some(d), Some(w))
            }
            None => (None, None),
        };
        let dek_ref = dek.as_ref();
        let mut writer = BufWriter::new(File::create(&path)?);
        let mut blocks: Vec<BlockMeta> = Vec::new();
        let mut bloom = Bloom::with_capacity(expected_entries, BLOOM_FP_RATE);
        let mut offset: u64 = 0;
        let mut entry_count: u64 = 0;
        let mut delete_count: u64 = 0;

        // Best-effort stamps sidecar, streamed in lockstep with the data
        // blocks (see the module docs). Skipped for encrypted tables — its
        // blocks would need sealing too; encrypted tables fall back to
        // decoding data blocks for stamp scans (correct, just no fast path).
        let mut stamps_writer = if dek_ref.is_none() {
            File::create(stamps_path(&path)).map(BufWriter::new).ok()
        } else {
            let _ = std::fs::remove_file(stamps_path(&path));
            None
        };
        let mut stamps_table: Vec<u8> = Vec::new();

        // Group entries into uncompressed blocks of ~BLOCK_TARGET bytes; seal,
        // compress and write each block as soon as it fills.
        let mut buf = Vec::with_capacity(BLOCK_TARGET + BLOCK_TARGET / 4);
        let mut stamp_buf = Vec::new();
        let mut first: Option<Vec<u8>> = None;
        let mut seal = |buf: &mut Vec<u8>,
                        stamp_buf: &mut Vec<u8>,
                        first: &mut Option<Vec<u8>>,
                        writer: &mut BufWriter<File>,
                        stamps_writer: &mut Option<BufWriter<File>>,
                        stamps_table: &mut Vec<u8>|
         -> Result<BlockMeta> {
            let comp = compress(codec, buf);
            // Encrypted: seal the COMPRESSED block, nonce = its file offset.
            // `comp_len` stores the SEALED length so `read_block` reads the
            // right span; `uncomp_len` stays the plaintext size for decompress.
            let on_disk = match dek_ref {
                Some(dek) => dek.seal(offset, &comp)?,
                None => comp,
            };
            writer.write_all(&on_disk)?;
            let meta = BlockMeta {
                first_key: first.take().unwrap(),
                offset,
                comp_len: on_disk.len() as u32,
                uncomp_len: buf.len() as u32,
            };
            offset += on_disk.len() as u64;
            buf.clear();
            if let Some(sw) = stamps_writer.as_mut() {
                let scomp = compress(codec, stamp_buf);
                if sw.write_all(&scomp).is_ok() {
                    stamps_table.extend_from_slice(&(scomp.len() as u32).to_le_bytes());
                    stamps_table.extend_from_slice(&(stamp_buf.len() as u32).to_le_bytes());
                } else {
                    *stamps_writer = None;
                }
            }
            stamp_buf.clear();
            Ok(meta)
        };
        for entry in entries {
            let e = entry?;
            if first.is_none() {
                first = Some(e.key().to_vec());
            }
            bloom.add(e.key());
            entry_count += 1;
            if matches!(e.value(), VersionValue::Delete) {
                delete_count += 1;
            }
            encode_entry(&mut buf, &e);
            encode_stamp(&mut stamp_buf, &e);
            if buf.len() >= BLOCK_TARGET {
                blocks.push(seal(
                    &mut buf,
                    &mut stamp_buf,
                    &mut first,
                    &mut writer,
                    &mut stamps_writer,
                    &mut stamps_table,
                )?);
            }
        }
        if !buf.is_empty() {
            blocks.push(seal(
                &mut buf,
                &mut stamp_buf,
                &mut first,
                &mut writer,
                &mut stamps_writer,
                &mut stamps_table,
            )?);
        }

        // Index bytes (block index: first-keys + offsets + sizes).
        let index_off = offset;
        let mut index_bytes = Vec::new();
        index_bytes.extend_from_slice(&(blocks.len() as u64).to_le_bytes());
        for b in &blocks {
            index_bytes.extend_from_slice(&(b.first_key.len() as u32).to_le_bytes());
            index_bytes.extend_from_slice(&b.first_key);
            index_bytes.extend_from_slice(&b.offset.to_le_bytes());
            index_bytes.extend_from_slice(&b.comp_len.to_le_bytes());
            index_bytes.extend_from_slice(&b.uncomp_len.to_le_bytes());
        }
        let bloom_bytes = bloom.encode();

        let disk_len = if let Some(dek) = dek_ref {
            // Encrypted layout:
            //   [sealed blocks][sealed index][sealed bloom][wrapped_dek][footer]
            // The index (first-keys) and bloom (hashed keys) are sensitive, so
            // both are sealed; only the footer (offsets/codec/magic) is clear.
            let sealed_index = dek.seal(index_off, &index_bytes)?;
            writer.write_all(&sealed_index)?;
            let bloom_off = index_off + sealed_index.len() as u64;
            let sealed_bloom = dek.seal(bloom_off, &bloom_bytes)?;
            writer.write_all(&sealed_bloom)?;
            let dek_off = bloom_off + sealed_bloom.len() as u64;
            let wrapped = wrapped_dek.as_ref().expect("wrapped dek when encrypting");
            writer.write_all(wrapped)?;
            let mut footer = Vec::with_capacity(ENC_FOOTER_LEN as usize);
            footer.extend_from_slice(&index_off.to_le_bytes());
            footer.extend_from_slice(&bloom_off.to_le_bytes());
            footer.extend_from_slice(&entry_count.to_le_bytes());
            footer.extend_from_slice(&(codec.to_u8() as u64).to_le_bytes());
            footer.extend_from_slice(&dek_off.to_le_bytes());
            footer.extend_from_slice(&MAGIC_ENC.to_le_bytes());
            writer.write_all(&footer)?;
            dek_off + wrapped.len() as u64 + ENC_FOOTER_LEN
        } else {
            // Plaintext layout (unchanged): [blocks][index][bloom][footer(40)].
            let bloom_off = index_off + index_bytes.len() as u64;
            let mut tail = index_bytes;
            tail.extend_from_slice(&bloom_bytes);
            tail.extend_from_slice(&index_off.to_le_bytes());
            tail.extend_from_slice(&bloom_off.to_le_bytes());
            tail.extend_from_slice(&entry_count.to_le_bytes());
            tail.extend_from_slice(&(codec.to_u8() as u64).to_le_bytes());
            tail.extend_from_slice(&MAGIC.to_le_bytes());
            writer.write_all(&tail)?;
            offset + tail.len() as u64
        };

        let file = writer
            .into_inner()
            .map_err(|e| std::io::Error::from(e.error().kind()))?;
        file.sync_all()?;
        drop(file);
        let file = File::open(&path)?;

        // Seal the stamps sidecar: length table + footer, then re-open it
        // through the same validating loader `open` uses. Best-effort — a
        // failure here only loses the fast path.
        let stamps = match stamps_writer {
            Some(mut sw) => {
                let sealed = (|| -> std::io::Result<()> {
                    sw.write_all(&stamps_table)?;
                    sw.write_all(&(blocks.len() as u64).to_le_bytes())?;
                    sw.write_all(&STAMPS_MAGIC.to_le_bytes())?;
                    let f = sw.into_inner().map_err(|e| {
                        std::io::Error::from(e.error().kind())
                    })?;
                    f.sync_all()
                })();
                match sealed {
                    Ok(()) => load_stamps(&path, &blocks),
                    Err(_) => {
                        let _ = std::fs::remove_file(stamps_path(&path));
                        None
                    }
                }
            }
            None => {
                let _ = std::fs::remove_file(stamps_path(&path));
                None
            }
        };

        let version_counts = std::sync::OnceLock::new();
        let _ = version_counts.set((entry_count - delete_count, delete_count));
        Ok(SsTable {
            file,
            path,
            codec,
            dek,
            blocks,
            bloom,
            entry_count,
            disk_len,
            version_counts,
            block_cache: BlockCache::default(),
            stamps,
        })
    }

    /// Open an existing SSTable, loading its block index and Bloom filter.
    pub fn open(path: impl AsRef<Path>, kek: Option<&Kek>) -> Result<SsTable> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let file_len = file.metadata()?.len();
        if file_len < FOOTER_LEN {
            return Err(corrupt("file shorter than footer"));
        }

        // Discriminate by the trailing magic (last 8 bytes): MAGIC_ENC =>
        // encrypted (extended 48-byte footer + wrapped DEK); MAGIC => plaintext.
        let mut tail_magic = [0u8; 8];
        read_exact_at(&file, &mut tail_magic, file_len - 8)?;
        let trailing = u64::from_le_bytes(tail_magic);

        let (index_off, bloom_off, index_end, entry_count, codec, dek) = if trailing == MAGIC_ENC {
            if file_len < ENC_FOOTER_LEN {
                return Err(corrupt("file shorter than encrypted footer"));
            }
            let mut footer = [0u8; ENC_FOOTER_LEN as usize];
            read_exact_at(&file, &mut footer, file_len - ENC_FOOTER_LEN)?;
            let index_off = u64::from_le_bytes(footer[0..8].try_into().unwrap());
            let bloom_off = u64::from_le_bytes(footer[8..16].try_into().unwrap());
            let entry_count = u64::from_le_bytes(footer[16..24].try_into().unwrap());
            let codec = Codec::from_u8(footer[24]).ok_or_else(|| corrupt("unknown codec"))?;
            let dek_off = u64::from_le_bytes(footer[32..40].try_into().unwrap());
            let kek = kek.ok_or_else(|| {
                StorageError::Crypto("SSTable is encrypted but no keyfile is configured".into())
            })?;
            let mut wrapped = vec![0u8; WRAPPED_DEK_LEN as usize];
            read_exact_at(&file, &mut wrapped, dek_off)?;
            let dek = kek.unwrap_dek(&wrapped)?;
            (index_off, bloom_off, dek_off, entry_count, codec, Some(dek))
        } else if trailing == MAGIC {
            let mut footer = [0u8; FOOTER_LEN as usize];
            read_exact_at(&file, &mut footer, file_len - FOOTER_LEN)?;
            let index_off = u64::from_le_bytes(footer[0..8].try_into().unwrap());
            let bloom_off = u64::from_le_bytes(footer[8..16].try_into().unwrap());
            let entry_count = u64::from_le_bytes(footer[16..24].try_into().unwrap());
            let codec = Codec::from_u8(footer[24]).ok_or_else(|| corrupt("unknown codec"))?;
            (index_off, bloom_off, file_len - FOOTER_LEN, entry_count, codec, None)
        } else {
            return Err(corrupt("bad magic"));
        };
        if index_off > bloom_off || bloom_off > index_end {
            return Err(corrupt("inconsistent footer offsets"));
        }

        // Index region [index_off, bloom_off); bloom region [bloom_off,
        // index_end). Both are sealed when encrypted — open before parsing.
        let mut index_buf = vec![0u8; (bloom_off - index_off) as usize];
        read_exact_at(&file, &mut index_buf, index_off)?;
        let mut bloom_buf = vec![0u8; (index_end - bloom_off) as usize];
        read_exact_at(&file, &mut bloom_buf, bloom_off)?;
        if let Some(dek) = &dek {
            index_buf = dek.open(index_off, &index_buf)?;
            bloom_buf = dek.open(bloom_off, &bloom_buf)?;
        }
        let blocks = parse_block_index(&index_buf)?;
        let bloom = Bloom::decode(&bloom_buf).ok_or_else(|| corrupt("bad bloom block"))?;

        // Encrypted tables carry no stamps sidecar (see write_stream); load
        // returns None and stamp scans fall back to decoding data blocks.
        let stamps = load_stamps(&path, &blocks);
        Ok(SsTable {
            file,
            path,
            codec,
            dek,
            blocks,
            bloom,
            entry_count,
            version_counts: std::sync::OnceLock::new(),
            disk_len: file_len,
            block_cache: BlockCache::default(),
            stamps,
        })
    }

    /// Number of entries in this table.
    pub fn len(&self) -> u64 {
        self.entry_count
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// `(puts, deletes)` version counts for this immutable file — computed on
    /// first call by one streaming pass, cached forever. Fresh files (just
    /// written) already carry the counts for free.
    pub fn version_counts(&self) -> (u64, u64) {
        *self.version_counts.get_or_init(|| {
            let mut puts = 0u64;
            let mut dels = 0u64;
            for row in self.iter() {
                match row {
                    Ok(e) if matches!(e.value, VersionValue::Delete) => dels += 1,
                    Ok(_) => puts += 1,
                    Err(_) => {}
                }
            }
            (puts, dels)
        })
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

    /// Like [`SsTable::iter`], but starting at the first block that can hold
    /// `start` (per the block index) — entries before `start` within that
    /// block are still yielded, so callers filter to their exact bound. Cost
    /// is proportional to what is consumed, not the table.
    pub fn iter_from(&self, start: &[u8]) -> SsTableIter<'_> {
        let first = self
            .blocks
            .partition_point(|b| b.first_key.as_slice() <= start)
            .saturating_sub(1);
        SsTableIter {
            table: self,
            next_block: first,
            block: Vec::new(),
            pos: 0,
        }
    }

    /// Read and decompress one block from disk.
    fn read_block(&self, meta: &BlockMeta) -> Result<Vec<u8>> {
        let mut on_disk = vec![0u8; meta.comp_len as usize];
        read_exact_at(&self.file, &mut on_disk, meta.offset)?;
        // Encrypted: open the block (nonce = its offset) before decompressing.
        let comp = match &self.dek {
            Some(dek) => dek.open(meta.offset, &on_disk)?,
            None => on_disk,
        };
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

    /// Stream `(key, hlc, is_put)` in key order without decoding values,
    /// starting at the first block that can hold `start` (like
    /// [`SsTable::iter_from`], stamps before `start` within that block are
    /// still yielded — callers filter to their exact bound). Served from the
    /// stamps sidecar when present (no value byte is even decompressed);
    /// tables without one decode data blocks but skip the value copies.
    pub fn stamps_iter_from(&self, start: Option<&[u8]>) -> StampsIter<'_> {
        let first = match start {
            Some(s) => self
                .blocks
                .partition_point(|b| b.first_key.as_slice() <= s)
                .saturating_sub(1),
            None => 0,
        };
        StampsIter {
            table: self,
            next_block: first,
            block: Vec::new(),
            pos: 0,
        }
    }

    /// Read and decompress stamp block `i` from the sidecar.
    fn read_stamp_block(&self, i: usize) -> Result<Vec<u8>> {
        let stamps = self.stamps.as_ref().expect("sidecar present");
        let (offset, comp_len, uncomp_len) = stamps.blocks[i];
        let mut comp = vec![0u8; comp_len as usize];
        read_exact_at(&stamps.file, &mut comp, offset)?;
        decompress(self.codec, &comp, uncomp_len as usize)
    }
}

/// Streaming iterator over one SSTable's `(key, hlc, is_put)` stamps in key
/// order — the value-free counterpart of [`SsTableIter`]. Holds at most one
/// decompressed block at a time; reads the stamps sidecar when the table has
/// one, else falls back to data blocks (decompressing values it then skips).
#[derive(Debug)]
pub struct StampsIter<'a> {
    table: &'a SsTable,
    /// Index of the next block to load.
    next_block: usize,
    /// Currently loaded (decompressed) block, empty until first load.
    block: Vec<u8>,
    /// Decode position within `block`.
    pos: usize,
}

impl Iterator for StampsIter<'_> {
    type Item = Result<SstStamp>;

    fn next(&mut self) -> Option<Self::Item> {
        let sidecar = self.table.stamps.is_some();
        while self.pos >= self.block.len() {
            let i = self.next_block;
            if i >= self.table.blocks.len() {
                return None;
            }
            self.next_block += 1;
            self.pos = 0;
            let block = if sidecar {
                self.table.read_stamp_block(i)
            } else {
                self.table.read_block(&self.table.blocks[i])
            };
            match block {
                Ok(block) => self.block = block,
                Err(e) => {
                    // Poison the iterator so a caller that keeps polling stops.
                    self.next_block = self.table.blocks.len();
                    self.block = Vec::new();
                    return Some(Err(e));
                }
            }
        }
        let decoded = if sidecar {
            decode_stamp(&self.block, self.pos)
        } else {
            decode_entry_stamp(&self.block, self.pos)
        };
        match decoded {
            Ok((stamp, next)) => {
                self.pos = next;
                Some(Ok(stamp))
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

fn encode_entry<E: EntryRef>(out: &mut Vec<u8>, e: &E) {
    let key = e.key();
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&e.hlc().to_bytes());
    match e.value() {
        VersionValue::Put(val) => {
            out.push(OP_PUT);
            out.extend_from_slice(&(val.len() as u32).to_le_bytes());
            out.extend_from_slice(val);
        }
        VersionValue::Delete => out.push(OP_DELETE),
    }
}

/// A value-free entry: `(key, hlc, is_put)` — everything an anti-entropy
/// digest needs.
pub type SstStamp = (Vec<u8>, Hlc, bool);

/// Encode one entry's stamp for the sidecar: `u32 keylen | key | hlc[12] | u8 op`.
fn encode_stamp<E: EntryRef>(out: &mut Vec<u8>, e: &E) {
    let key = e.key();
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&e.hlc().to_bytes());
    out.push(match e.value() {
        VersionValue::Put(_) => OP_PUT,
        VersionValue::Delete => OP_DELETE,
    });
}

/// Decode one stamp from a sidecar stamp block.
fn decode_stamp(buf: &[u8], start: usize) -> Result<(SstStamp, usize)> {
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
    let is_put = match op {
        OP_PUT => true,
        OP_DELETE => false,
        _ => return Err(corrupt("unknown op")),
    };
    Ok(((key, hlc, is_put), pos))
}

/// Decode one *data-block* entry as a stamp, skipping the value bytes without
/// copying them — the fallback path for tables with no sidecar.
fn decode_entry_stamp(buf: &[u8], start: usize) -> Result<(SstStamp, usize)> {
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
    let is_put = match op {
        OP_PUT => {
            let val_len = read_u32(buf, &mut pos)? as usize;
            pos = pos
                .checked_add(val_len)
                .filter(|end| *end <= buf.len())
                .ok_or_else(|| corrupt("unexpected end"))?;
            true
        }
        OP_DELETE => false,
        _ => return Err(corrupt("unknown op")),
    };
    Ok(((key, hlc, is_put), pos))
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

    /// The sharded block cache behaves like one logical cache: a miss then
    /// insert is visible on the next `get` regardless of which shard the
    /// offset lands in, distinct offsets don't collide, and the total live
    /// entry count across all shards never exceeds `BLOCK_CACHE_BLOCKS`
    /// (proving eviction is per-shard-capacity, not a single global cap that
    /// sharding would have silently multiplied).
    #[test]
    fn block_cache_shards_are_one_logical_cache() {
        let cache = BlockCache::default();
        for off in 0..500u64 {
            assert!(cache.get(off).is_none(), "unpopulated offset must miss");
            let block = Arc::new(vec![off as u8; 4]);
            cache.insert(off, block.clone());
            assert_eq!(cache.get(off), Some(block), "insert then get must hit");
        }
        let live: usize = cache
            .shards
            .iter()
            .map(|s| s.lock().unwrap().map.len())
            .sum();
        assert!(
            live <= BLOCK_CACHE_BLOCKS,
            "total live entries ({live}) must stay within the shared budget"
        );
        // Every shard actually received traffic — offsets 0..500 with the
        // multiply-shift hash must not collapse onto one shard (that would
        // defeat the whole point of sharding).
        let nonempty = cache
            .shards
            .iter()
            .filter(|s| !s.lock().unwrap().map.is_empty())
            .count();
        assert_eq!(nonempty, BLOCK_CACHE_SHARDS, "every shard should see traffic");
    }

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

    // --- at-rest encryption ---

    fn test_kek() -> Kek {
        Kek::from_bytes(&[9u8; 32]).unwrap()
    }

    /// Encrypted SSTable: multi-block round trip through the block-read path,
    /// on-disk bytes are ciphertext (keys/values absent), reopen recovers all.
    #[test]
    fn encrypted_sstable_round_trip_and_ciphertext() {
        for codec in [Codec::None, Codec::Lz4, Codec::Brotli] {
            let path = tmp();
            let kek = test_kek();
            // Enough distinct entries to span several blocks.
            let entries: Vec<SstEntry> = (0..500)
                .map(|i| put(&format!("topsecretkey{i:04}"), i as u64 + 1, &format!("topsecretval{i:04}")))
                .collect();
            let expected = entries.len();
            let sst = SsTable::write_stream(&path, entries.iter().map(Ok), expected, codec, Some(&kek)).unwrap();
            // Point read through the (decrypting) block path.
            assert_eq!(
                sst.get(b"topsecretkey0042").unwrap(),
                Some((Hlc::new(43, 0), VersionValue::Put(b"topsecretval0042".to_vec())))
            );
            // No stamps sidecar for encrypted tables.
            assert!(!stamps_path(&path).exists(), "encrypted tables write no sidecar");
            // On-disk: keys/values are not in the clear; trailing magic is ENC.
            let raw = std::fs::read(&path).unwrap();
            let hay = String::from_utf8_lossy(&raw);
            assert!(!hay.contains("topsecretkey0042"), "key must not be on disk in the clear ({codec:?})");
            assert!(!hay.contains("topsecretval0042"), "value must not be on disk in the clear");
            let trailing = u64::from_le_bytes(raw[raw.len()-8..].try_into().unwrap());
            assert_eq!(trailing, MAGIC_ENC);
            // Reopen with the key: full scan recovers every entry.
            let re = SsTable::open(&path, Some(&kek)).unwrap();
            let all = re.entries().unwrap();
            assert_eq!(all.len(), 500);
            assert_eq!(re.get(b"topsecretkey0499").unwrap(),
                Some((Hlc::new(500, 0), VersionValue::Put(b"topsecretval0499".to_vec()))));
            let _ = std::fs::remove_file(&path);
        }
    }

    /// Wrong KEK / no KEK cannot open an encrypted SSTable.
    #[test]
    fn encrypted_sstable_wrong_key_refused() {
        let path = tmp();
        let entries = [put("a", 1, "1"), put("b", 2, "2")];
        SsTable::write_stream(&path, entries.iter().map(Ok), 2, Codec::Lz4, Some(&test_kek())).unwrap();
        assert!(SsTable::open(&path, None).is_err(), "encrypted file needs a key");
        let wrong = Kek::from_bytes(&[1u8; 32]).unwrap();
        assert!(SsTable::open(&path, Some(&wrong)).is_err(), "wrong KEK must fail");
        let _ = std::fs::remove_file(&path);
    }

    /// Tampering a sealed data block fails the AEAD tag on read.
    #[test]
    fn encrypted_sstable_tamper_detected() {
        let path = tmp();
        let kek = test_kek();
        let entries: Vec<SstEntry> = (0..200).map(|i| put(&format!("k{i:03}"), i as u64+1, "v")).collect();
        SsTable::write_stream(&path, entries.iter().map(Ok), 200, Codec::None, Some(&kek)).unwrap();
        // Flip a byte in the first data block (offset 0).
        let mut raw = std::fs::read(&path).unwrap();
        raw[10] ^= 0x80;
        std::fs::write(&path, &raw).unwrap();
        let re = SsTable::open(&path, Some(&kek)).unwrap(); // footer/index still ok
        // Reading the tampered block fails.
        let scan: Result<Vec<_>> = re.entries();
        assert!(scan.is_err(), "tampered data block must fail the tag");
        let _ = std::fs::remove_file(&path);
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
        let sst = SsTable::open(&path, None).unwrap();
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
    fn stamps_sidecar_matches_entries_and_falls_back() {
        for codec in [Codec::None, Codec::Lz4, Codec::Brotli] {
            let path = tmp();
            let mut entries: Vec<SstEntry> = (0..2000)
                .map(|i| put(&format!("key{i:05}"), i as u64 + 1, &format!("value-{i}")))
                .collect();
            entries[7].value = VersionValue::Delete;
            entries[1500].value = VersionValue::Delete;
            let sst = SsTable::write(&path, &entries, codec).unwrap();
            assert!(sst.stamps.is_some(), "sidecar written ({codec:?})");
            assert!(stamps_path(&path).exists());
            let expect: Vec<SstStamp> = entries
                .iter()
                .map(|e| {
                    (
                        e.key.clone(),
                        e.hlc,
                        matches!(e.value, VersionValue::Put(_)),
                    )
                })
                .collect();
            let via_sidecar: Vec<SstStamp> = sst
                .stamps_iter_from(None)
                .collect::<Result<_>>()
                .unwrap();
            assert_eq!(via_sidecar, expect, "sidecar scan ({codec:?})");
            // Seeked start behaves like iter_from: begins in the right block.
            let from: Vec<SstStamp> = sst
                .stamps_iter_from(Some(b"key01500"))
                .filter(|r| r.as_ref().is_ok_and(|(k, _, _)| k.as_slice() > b"key01500".as_slice()))
                .collect::<Result<_>>()
                .unwrap();
            assert_eq!(from, expect[1501..], "seeked sidecar scan ({codec:?})");

            // Old-file fallback: delete the sidecar, reopen — identical scan
            // straight off the data blocks.
            std::fs::remove_file(stamps_path(&path)).unwrap();
            let sst = SsTable::open(&path, None).unwrap();
            assert!(sst.stamps.is_none());
            let via_fallback: Vec<SstStamp> = sst
                .stamps_iter_from(None)
                .collect::<Result<_>>()
                .unwrap();
            assert_eq!(via_fallback, expect, "fallback scan ({codec:?})");
            let _ = std::fs::remove_file(&path);
        }
    }

    #[test]
    fn torn_stamps_sidecar_is_ignored() {
        let path = tmp();
        let entries: Vec<SstEntry> = (0..500)
            .map(|i| put(&format!("k{i:04}"), i as u64 + 1, "v"))
            .collect();
        SsTable::write(&path, &entries, Codec::Lz4).unwrap();
        // Truncate the sidecar mid-file (simulates a crash between the data
        // fsync and the sidecar fsync).
        let sp = stamps_path(&path);
        let len = std::fs::metadata(&sp).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&sp).unwrap();
        f.set_len(len / 2).unwrap();
        drop(f);
        let sst = SsTable::open(&path, None).unwrap();
        assert!(sst.stamps.is_none(), "torn sidecar rejected");
        let stamps: Vec<SstStamp> = sst
            .stamps_iter_from(None)
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(stamps.len(), 500, "fallback still serves stamps");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&sp);
    }

    #[test]
    fn detects_bad_magic() {
        let path = tmp();
        SsTable::write(&path, &[put("a", 1, "1")], Codec::Lz4).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        let len = bytes.len();
        bytes[len - 1] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(SsTable::open(&path, None).is_err());
        let _ = std::fs::remove_file(&path);
    }
}
