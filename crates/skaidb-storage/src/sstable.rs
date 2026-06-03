//! Immutable sorted-string tables (SPEC §12).
//!
//! A memtable flush writes its latest version per key to an SSTable: an
//! append-only file of key-sorted entries, followed by a full key→offset index
//! and a Bloom filter, then a fixed footer. Files are never mutated; compaction
//! produces new files and removes old ones.
//!
//! Layout: `[data][index][bloom][footer(32)]`, all integers little-endian.
//! - data entry: `u32 keylen | key | hlc[12] | u8 op | (op==Put) u32 vlen | val`
//! - index:      `u64 count | (u32 keylen | key | u64 offset)*`
//! - bloom:      the bytes of [`Bloom::encode`]
//! - footer:     `u64 index_off | u64 bloom_off | u64 count | u64 magic`

use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::bloom::Bloom;
use crate::error::{Result, StorageError};
use crate::hlc::Hlc;
use crate::memtable::VersionValue;

const MAGIC: u64 = 0x736b_6169_6462_5354; // "skaidbST"
const FOOTER_LEN: u64 = 32;
const OP_PUT: u8 = 0;
const OP_DELETE: u8 = 1;
const BLOOM_FP_RATE: f64 = 0.01;

/// One entry as stored in (or read from) an SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstEntry {
    pub key: Vec<u8>,
    pub hlc: Hlc,
    pub value: VersionValue,
}

/// A handle to an immutable on-disk SSTable.
#[derive(Debug)]
pub struct SsTable {
    file: File,
    path: PathBuf,
    /// Full, in-memory key→offset index (sorted by key).
    index: Vec<(Vec<u8>, u64)>,
    bloom: Bloom,
    data_end: u64,
    entry_count: u64,
}

impl SsTable {
    /// Write `entries` (which must be sorted by key, unique) to a new SSTable.
    pub fn write(path: impl AsRef<Path>, entries: &[SstEntry]) -> Result<SsTable> {
        let path = path.as_ref().to_path_buf();
        let mut buf = Vec::new();
        let mut index: Vec<(Vec<u8>, u64)> = Vec::with_capacity(entries.len());
        let keys: Vec<Vec<u8>> = entries.iter().map(|e| e.key.clone()).collect();

        for e in entries {
            let offset = buf.len() as u64;
            encode_entry(&mut buf, e);
            index.push((e.key.clone(), offset));
        }
        let data_end = buf.len() as u64;

        // Index block.
        let index_off = buf.len() as u64;
        buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for (key, offset) in &index {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&offset.to_le_bytes());
        }

        // Bloom block.
        let bloom = Bloom::build(&keys, BLOOM_FP_RATE);
        let bloom_off = buf.len() as u64;
        buf.extend_from_slice(&bloom.encode());

        // Footer.
        buf.extend_from_slice(&index_off.to_le_bytes());
        buf.extend_from_slice(&bloom_off.to_le_bytes());
        buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        buf.extend_from_slice(&MAGIC.to_le_bytes());

        {
            let mut file = File::create(&path)?;
            file.write_all(&buf)?;
            file.sync_all()?;
        }
        // Reopen read-only: the write handle above cannot serve `read_at`.
        let file = File::open(&path)?;

        Ok(SsTable {
            file,
            path,
            index,
            bloom,
            data_end,
            entry_count: entries.len() as u64,
        })
    }

    /// Open an existing SSTable, loading its index and Bloom filter into memory.
    pub fn open(path: impl AsRef<Path>) -> Result<SsTable> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let file_len = file.metadata()?.len();
        if file_len < FOOTER_LEN {
            return Err(corrupt("file shorter than footer"));
        }

        let mut footer = [0u8; FOOTER_LEN as usize];
        file.read_exact_at(&mut footer, file_len - FOOTER_LEN)?;
        let index_off = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let bloom_off = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let entry_count = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let magic = u64::from_le_bytes(footer[24..32].try_into().unwrap());
        if magic != MAGIC {
            return Err(corrupt("bad magic"));
        }
        if index_off > bloom_off || bloom_off > file_len - FOOTER_LEN {
            return Err(corrupt("inconsistent footer offsets"));
        }

        // Read and parse the index block.
        let index_len = (bloom_off - index_off) as usize;
        let mut index_buf = vec![0u8; index_len];
        file.read_exact_at(&mut index_buf, index_off)?;
        let index = parse_index(&index_buf)?;

        // Read and decode the Bloom block.
        let bloom_len = (file_len - FOOTER_LEN - bloom_off) as usize;
        let mut bloom_buf = vec![0u8; bloom_len];
        file.read_exact_at(&mut bloom_buf, bloom_off)?;
        let bloom = Bloom::decode(&bloom_buf).ok_or_else(|| corrupt("bad bloom block"))?;

        Ok(SsTable {
            file,
            path,
            index,
            bloom,
            data_end: index_off,
            entry_count,
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

    /// Point lookup: returns the stored version for `key` if present.
    pub fn get(&self, key: &[u8]) -> Result<Option<(Hlc, VersionValue)>> {
        if !self.bloom.contains(key) {
            return Ok(None);
        }
        let idx = match self.index.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
            Ok(i) => i,
            Err(_) => return Ok(None),
        };
        let (_, offset) = &self.index[idx];
        let (entry, _) = self.read_entry_at(*offset)?;
        Ok(Some((entry.hlc, entry.value)))
    }

    /// Read every entry in key order (used by scans and compaction).
    pub fn entries(&self) -> Result<Vec<SstEntry>> {
        let mut buf = vec![0u8; self.data_end as usize];
        self.file.read_exact_at(&mut buf, 0)?;
        let mut out = Vec::with_capacity(self.entry_count as usize);
        let mut pos = 0usize;
        while pos < buf.len() {
            let (entry, next) = decode_entry(&buf, pos)?;
            out.push(entry);
            pos = next;
        }
        Ok(out)
    }

    fn read_entry_at(&self, offset: u64) -> Result<(SstEntry, u64)> {
        // Read a generous window covering the entry; entries are small.
        let remaining = self.data_end - offset;
        let mut buf = vec![0u8; remaining as usize];
        self.file.read_exact_at(&mut buf, offset)?;
        let (entry, next) = decode_entry(&buf, 0)?;
        Ok((entry, offset + next as u64))
    }
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

fn parse_index(buf: &[u8]) -> Result<Vec<(Vec<u8>, u64)>> {
    let mut pos = 0;
    let count = read_u64(buf, &mut pos)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let key_len = read_u32(buf, &mut pos)? as usize;
        let key = take(buf, &mut pos, key_len)?.to_vec();
        let offset = read_u64(buf, &mut pos)?;
        out.push((key, offset));
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
        let path = tmp();
        let entries = vec![put("a", 1, "1"), put("b", 2, "2"), put("c", 3, "3")];
        let sst = SsTable::write(&path, &entries).unwrap();
        assert_eq!(
            sst.get(b"b").unwrap(),
            Some((Hlc::new(2, 0), VersionValue::Put(b"2".to_vec())))
        );
        assert_eq!(sst.get(b"z").unwrap(), None);
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
        SsTable::write(&path, &entries).unwrap();
        let sst = SsTable::open(&path).unwrap();
        assert_eq!(sst.len(), 3);
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
        SsTable::write(&path, &[put("a", 1, "1")]).unwrap();
        // Corrupt the magic in the footer.
        let mut bytes = std::fs::read(&path).unwrap();
        let len = bytes.len();
        bytes[len - 1] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(SsTable::open(&path).is_err());
        let _ = std::fs::remove_file(&path);
    }
}
