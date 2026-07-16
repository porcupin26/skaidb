//! Write-ahead log with group commit (SPEC §12).
//!
//! Every mutation is appended before it is applied to the memtable, so a crash
//! recovers by replaying the log. Records are length-prefixed and CRC-checked; a
//! torn trailing record (from a crash mid-append) is detected and truncated on
//! open rather than treated as fatal.
//!
//! **Group commit.** Appending and fsyncing are separated. [`Wal::append`]
//! writes the record (positionally, no fsync) and returns a [`WalCommit`]; the
//! caller later calls [`WalSync::sync_through`] to make it durable. Many
//! concurrent writers therefore share a single fsync: the first into the sync
//! critical section flushes everything appended so far, and the rest observe
//! their commit point is already durable and skip their own fsync. Appends must
//! be serialized by the caller (the engine holds its write lock for the append),
//! but the slow fsync happens outside that lock.
//!
//! On-disk record layout (all integers little-endian):
//! ```text
//! u32 payload_len | payload[payload_len] | u32 crc32(payload)
//! payload = u8 op | hlc[12] | u32 key_len | key | (op==Put) u32 val_len | val
//! ```

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::posfile::write_all_at;
use std::sync::{Arc, Mutex};

use crate::crc::crc32;
use crate::error::{Result, StorageError};
use crate::hlc::Hlc;

const OP_PUT: u8 = 0;
const OP_DELETE: u8 = 1;

/// A logical mutation as recorded in the WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalOp {
    Put(Vec<u8>),
    Delete,
}

/// One durable WAL entry: a versioned mutation of a single key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub hlc: Hlc,
    pub key: Vec<u8>,
    pub op: WalOp,
}

/// A commit point: the WAL generation and the byte offset just past a record.
/// Pass it to [`WalSync::sync_through`] to make that record durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalCommit {
    generation: u64,
    offset: u64,
}

impl WalRecord {
    fn decode_payload(p: &[u8], offset: u64) -> Result<WalRecord> {
        let corrupt = |detail| StorageError::Corruption { offset, detail };
        let mut cur = 0usize;
        let take = |cur: &mut usize, n: usize| -> Result<&[u8]> {
            let end = cur
                .checked_add(n)
                .ok_or_else(|| corrupt("length overflow"))?;
            let slice = p
                .get(*cur..end)
                .ok_or_else(|| corrupt("payload too short"))?;
            *cur = end;
            Ok(slice)
        };

        let op_byte = take(&mut cur, 1)?[0];
        let mut hlc_bytes = [0u8; 12];
        hlc_bytes.copy_from_slice(take(&mut cur, 12)?);
        let hlc = Hlc::from_bytes(hlc_bytes);

        let key_len = read_u32(take(&mut cur, 4)?) as usize;
        let key = take(&mut cur, key_len)?.to_vec();

        let op = match op_byte {
            OP_PUT => {
                let val_len = read_u32(take(&mut cur, 4)?) as usize;
                WalOp::Put(take(&mut cur, val_len)?.to_vec())
            }
            OP_DELETE => WalOp::Delete,
            _ => return Err(corrupt("unknown op byte")),
        };
        Ok(WalRecord { hlc, key, op })
    }
}

/// The shared durability coordinator for one WAL file: the backing file plus the
/// append/synced offsets and a generation that bumps on truncation.
#[derive(Debug)]
pub struct WalSync {
    file: File,
    /// Ephemeral WAL (memory tables): the file is unlinked at open — writes
    /// land in page cache of a deleted inode and durability is explicitly
    /// not promised, so fsync is skipped entirely.
    ephemeral: bool,
    /// Next free append offset (advanced by serialized appends).
    write_offset: AtomicU64,
    /// Bytes known durable on disk.
    synced: AtomicU64,
    /// Bumped on truncate; commits from an older generation are already durable
    /// (their data was flushed to an SSTable), so their fsync is a no-op.
    generation: AtomicU64,
    /// Count of fsync (`sync_data`) calls actually issued — surfaced as a metric
    /// so operators can see group-commit coalescing (commits ≫ fsyncs is healthy).
    fsyncs: AtomicU64,
    /// Serializes the fsync critical section.
    sync_lock: Mutex<()>,
}

impl WalSync {
    /// Ensure everything up to `commit` is durable, coalescing with concurrent
    /// callers (group commit). A commit from a superseded generation is already
    /// durable elsewhere and returns immediately.
    pub fn sync_through(&self, commit: WalCommit) -> Result<()> {
        if self.ephemeral {
            return Ok(());
        }
        if self.generation.load(Ordering::SeqCst) != commit.generation
            || self.synced.load(Ordering::SeqCst) >= commit.offset
        {
            return Ok(());
        }
        let _guard = self.sync_lock.lock().expect("wal sync lock");
        // Re-check under the lock: another writer may have synced past us.
        if self.generation.load(Ordering::SeqCst) != commit.generation
            || self.synced.load(Ordering::SeqCst) >= commit.offset
        {
            return Ok(());
        }
        let durable = self.write_offset.load(Ordering::SeqCst);
        self.file.sync_data()?;
        self.fsyncs.fetch_add(1, Ordering::SeqCst);
        self.synced.store(durable, Ordering::SeqCst);
        Ok(())
    }

    /// Number of fsyncs issued against this WAL since open.
    pub fn fsync_count(&self) -> u64 {
        self.fsyncs.load(Ordering::Relaxed)
    }

    /// Current append offset (≈ live WAL size in bytes).
    pub fn size_bytes(&self) -> u64 {
        self.write_offset.load(Ordering::Relaxed)
    }
}

/// Path of sealed segment `seq` for the WAL at `base`.
fn segment_path(base: &Path, seq: u64) -> PathBuf {
    let name = base.file_name().and_then(|n| n.to_str()).unwrap_or("wal");
    base.with_file_name(format!("{name}.seg.{seq:06}"))
}

/// Sealed segment numbers next to `base`, ascending.
fn list_segments(base: &Path) -> Result<Vec<u64>> {
    let dir = base.parent().unwrap_or_else(|| Path::new("."));
    let name = base.file_name().and_then(|n| n.to_str()).unwrap_or("wal");
    let prefix = format!("{name}.seg.");
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            if let Some(fname) = e.file_name().to_str() {
                if let Some(seq) = fname.strip_prefix(&prefix) {
                    if let Ok(n) = seq.parse::<u64>() {
                        out.push(n);
                    }
                }
            }
        }
    }
    out.sort_unstable();
    Ok(out)
}

/// Chunk size the WAL file grows by ahead of appends (bytes). **Why this
/// exists**: on at least one measured storage class (the p225 bench fleet),
/// a single-row durable write cost ~1.7ms — ~100% filesystem-metadata fsync
/// cost from extending the WAL file on every append, vs. ~500µs to fsync an
/// in-place overwrite of already-allocated space (measured directly, see
/// BENCHMARKS.md's "C0 — 1 node" scenario). Growing the file one chunk at a
/// time converts all but one-in-thousands of appends from "extend + journal
/// metadata + flush" to "flush data only", the same trick as PostgreSQL's
/// fixed-size WAL segments — but chunked rather than a full segment up
/// front because skaidb keeps **one WAL per table and per index**: a whole
/// PG-style 16 MiB reservation per WAL would cost a many-table deployment
/// gigabytes of idle disk, while one chunk caps the per-table overhead at
/// 1 MiB.
pub const WAL_PREALLOC_CHUNK_BYTES: u64 = 1024 * 1024;

/// An append-only write-ahead log file.
#[derive(Debug)]
pub struct Wal {
    sync: Arc<WalSync>,
    path: PathBuf,
    /// Granularity the file is grown by ahead of appends (see
    /// [`WAL_PREALLOC_CHUNK_BYTES`]); `0` disables pre-allocation entirely
    /// (ephemeral WALs, which never fsync and gain nothing from it).
    prealloc_chunk: u64,
    /// Bytes currently allocated to the file (its physical length, which
    /// runs ahead of `write_offset` by up to one chunk). Only touched under
    /// the engine's serialized append path, like `write_offset`.
    allocated: AtomicU64,
}

/// Round `len` up to the next multiple of `chunk` (`chunk > 0`).
fn round_up(len: u64, chunk: u64) -> u64 {
    len.div_ceil(chunk) * chunk
}

impl Wal {
    /// Open (creating if needed) the WAL at `path` and replay it, growing the
    /// file to the next `prealloc_chunk` boundary (`0` disables). Appends
    /// resume from the last valid record; a torn or corrupt trailing record —
    /// including the zero-filled unwritten tail of pre-allocated space — is
    /// left in place (not truncated away) so it can absorb future appends
    /// without re-paying the extension cost after a restart.
    pub fn open(path: impl AsRef<Path>, prealloc_chunk: u64) -> Result<(Wal, Vec<WalRecord>)> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        // Sealed segments (frozen memtables whose background flush never
        // completed) replay first, ascending, then the active log — the same
        // order the writes happened in.
        let mut records = Vec::new();
        for seq in list_segments(&path)? {
            let seg = OpenOptions::new().read(true).open(segment_path(&path, seq))?;
            let (mut seg_records, _) = replay(&seg)?;
            records.append(&mut seg_records);
        }
        let (mut active_records, good_len) = replay(&file)?;
        records.append(&mut active_records);
        let mut allocated = file.metadata()?.len();
        if prealloc_chunk > 0 && allocated < round_up(good_len.max(1), prealloc_chunk) {
            allocated = round_up(good_len.max(1), prealloc_chunk);
            file.set_len(allocated)?;
            file.sync_all()?; // commit the new size once, up front
        }

        let sync = Arc::new(WalSync {
            file,
            ephemeral: false,
            write_offset: AtomicU64::new(good_len),
            synced: AtomicU64::new(good_len),
            generation: AtomicU64::new(0),
            fsyncs: AtomicU64::new(0),
            sync_lock: Mutex::new(()),
        });
        Ok((
            Wal {
                sync,
                path,
                prealloc_chunk,
                allocated: AtomicU64::new(allocated),
            },
            records,
        ))
    }

    /// A WAL for a memory table: created then immediately unlinked, so writes
    /// land in the page cache of a deleted inode, nothing survives a restart,
    /// and fsync is skipped ([`WalSync::sync_through`] no-ops). The engine's
    /// write paths stay identical; only durability is (deliberately) absent.
    pub fn open_ephemeral(path: impl AsRef<Path>) -> Result<Wal> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        let _ = std::fs::remove_file(&path); // unlink: page-cache only
        let sync = Arc::new(WalSync {
            file,
            ephemeral: true,
            write_offset: AtomicU64::new(0),
            synced: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            fsyncs: AtomicU64::new(0),
            sync_lock: Mutex::new(()),
        });
        // Ephemeral WALs never fsync (sync_through no-ops), so pre-allocation
        // buys nothing — 0 disables it.
        Ok(Wal {
            sync,
            path,
            prealloc_chunk: 0,
            allocated: AtomicU64::new(0),
        })
    }

    /// Append a record (positional write, no fsync) and return its commit point.
    ///
    /// Appends must be serialized by the caller (the engine's write lock); the
    /// returned [`WalCommit`] is made durable later via [`WalSync::sync_through`].
    pub fn append(&self, record: &WalRecord) -> Result<WalCommit> {
        let value = match &record.op {
            WalOp::Put(val) => Some(val.as_slice()),
            WalOp::Delete => None,
        };
        self.append_op(record.hlc, &record.key, value)
    }

    /// Append a mutation encoded directly from its borrowed parts (`Some` value
    /// = put, `None` = delete), building the frame in one pre-sized buffer —
    /// key and value bytes are copied exactly once.
    pub fn append_op(&self, hlc: Hlc, key: &[u8], value: Option<&[u8]>) -> Result<WalCommit> {
        let payload_len = 1 + 12 + 4 + key.len() + value.map_or(0, |v| 4 + v.len());
        let mut frame = Vec::with_capacity(4 + payload_len + 4);
        frame.extend_from_slice(&(payload_len as u32).to_le_bytes());
        frame.push(if value.is_some() { OP_PUT } else { OP_DELETE });
        frame.extend_from_slice(&hlc.to_bytes());
        frame.extend_from_slice(&(key.len() as u32).to_le_bytes());
        frame.extend_from_slice(key);
        if let Some(val) = value {
            frame.extend_from_slice(&(val.len() as u32).to_le_bytes());
            frame.extend_from_slice(val);
        }
        frame.extend_from_slice(&crc32(&frame[4..]).to_le_bytes());

        let offset = self.sync.write_offset.load(Ordering::SeqCst);
        let end = offset + frame.len() as u64;
        // Keep the physical file a chunk ahead of the append point, so the
        // fsync that follows most appends is a data-only flush into
        // already-allocated space instead of also journaling a file-size
        // extension (~3x cheaper on some storage — see WAL_PREALLOC_CHUNK_BYTES).
        // The size change committed here is deliberately NOT fsynced: the
        // next sync_through absorbs it (fdatasync flushes size changes),
        // paying the metadata cost once per chunk instead of per append.
        // A crash in between leaves a longer zero-filled file — replay
        // treats the zero tail as clean end-of-data.
        if self.prealloc_chunk > 0 && end > self.allocated.load(Ordering::SeqCst) {
            let new_alloc = round_up(end, self.prealloc_chunk);
            self.sync.file.set_len(new_alloc)?;
            self.allocated.store(new_alloc, Ordering::SeqCst);
        }
        write_all_at(&self.sync.file, &frame, offset)?;
        self.sync.write_offset.store(end, Ordering::SeqCst);
        Ok(WalCommit {
            generation: self.sync.generation.load(Ordering::SeqCst),
            offset: end,
        })
    }

    /// A handle to the durability coordinator, for syncing outside the lock.
    pub fn sync_handle(&self) -> Arc<WalSync> {
        Arc::clone(&self.sync)
    }

    /// Convenience: make `commit` durable immediately (append-then-sync callers).
    pub fn commit_sync(&self, commit: WalCommit) -> Result<()> {
        self.sync.sync_through(commit)
    }

    /// Seal the active log as numbered segment `seq` and start a fresh one.
    /// The sealed file holds exactly the records of a memtable being frozen
    /// for background flush; [`Wal::drop_segments_through`] deletes it once
    /// that memtable is durably an SSTable. Pre-rotation commit handles keep
    /// referring to the sealed file's inode — their fsyncs still work — and
    /// the fsync here makes everything sealed durable anyway.
    pub fn rotate(&mut self, seq: u64) -> Result<()> {
        {
            let _guard = self.sync.sync_lock.lock().expect("wal sync lock");
            self.sync.file.sync_data()?;
        }
        let sealed = segment_path(&self.path, seq);
        std::fs::rename(&self.path, &sealed)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;
        let mut allocated = 0;
        if self.prealloc_chunk > 0 {
            allocated = self.prealloc_chunk;
            file.set_len(allocated)?;
            file.sync_all()?;
        }
        self.allocated.store(allocated, Ordering::SeqCst);
        self.sync = Arc::new(WalSync {
            file,
            ephemeral: false,
            write_offset: AtomicU64::new(0),
            synced: AtomicU64::new(0),
            generation: AtomicU64::new(self.sync.generation.load(Ordering::SeqCst) + 1),
            fsyncs: AtomicU64::new(0),
            sync_lock: Mutex::new(()),
        });
        Ok(())
    }

    /// Delete sealed segments numbered `<= seq` (their contents are durable
    /// in SSTables). Missing files are fine — deletion is idempotent.
    pub fn drop_segments_through(&self, seq: u64) -> Result<()> {
        for s in list_segments(&self.path)? {
            if s <= seq {
                let _ = std::fs::remove_file(segment_path(&self.path, s));
            }
        }
        Ok(())
    }

    /// Truncate the log to empty (re-reserving one pre-allocation chunk) and
    /// bump the generation. Called after a flush makes the logged mutations
    /// durable in an SSTable.
    pub fn truncate(&self) -> Result<()> {
        let _guard = self.sync.sync_lock.lock().expect("wal sync lock");
        // set_len(0) first so the old contents are actually released and the
        // re-reserved chunk reads as zeros (a bare shrink to chunk size would
        // leave stale record bytes in place, which replay would then replay).
        self.sync.file.set_len(0)?;
        self.sync.file.set_len(self.prealloc_chunk)?;
        self.allocated.store(self.prealloc_chunk, Ordering::SeqCst);
        self.sync.file.sync_data()?;
        self.sync.fsyncs.fetch_add(1, Ordering::SeqCst);
        self.sync.write_offset.store(0, Ordering::SeqCst);
        self.sync.synced.store(0, Ordering::SeqCst);
        self.sync.generation.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Path backing this WAL.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of fsyncs issued against this WAL since open.
    pub fn fsync_count(&self) -> u64 {
        self.sync.fsync_count()
    }

    /// Current append offset (≈ live WAL size in bytes).
    pub fn size_bytes(&self) -> u64 {
        self.sync.size_bytes()
    }
}

fn read_u32(bytes: &[u8]) -> u32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(bytes);
    u32::from_le_bytes(b)
}

/// Replay a WAL file, returning the recovered records and the byte length up to
/// the last fully-valid record (so callers can truncate a torn tail).
fn replay(file: &File) -> Result<(Vec<WalRecord>, u64)> {
    let mut reader = BufReader::new(file);
    let mut records = Vec::new();
    let mut good_len: u64 = 0;

    loop {
        // Read the 4-byte length prefix; a short read means a clean or torn EOF.
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let payload_len = u32::from_le_bytes(len_buf) as usize;
        if payload_len == 0 {
            // No real record ever encodes a zero-length payload (the
            // smallest is op+hlc+key_len = 17 bytes) — this is the
            // zero-filled unwritten tail of a pre-allocated segment.
            // `crc32(&[]) == 0` too, so without this check a zero length
            // prefix would pass the checksum below and then fail to decode,
            // turning a clean "nothing more here" into a hard replay error.
            break;
        }

        let mut payload = vec![0u8; payload_len];
        if reader.read_exact(&mut payload).is_err() {
            break; // torn payload
        }
        let mut crc_buf = [0u8; 4];
        if reader.read_exact(&mut crc_buf).is_err() {
            break; // torn checksum
        }
        if u32::from_le_bytes(crc_buf) != crc32(&payload) {
            break; // torn / corrupt trailing record
        }

        let record = WalRecord::decode_payload(&payload, good_len)?;
        records.push(record);
        good_len += (4 + payload_len + 4) as u64;
    }

    Ok((records, good_len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn rec(hlc: u64, key: &str, op: WalOp) -> WalRecord {
        WalRecord {
            hlc: Hlc::new(hlc, 0),
            key: key.as_bytes().to_vec(),
            op,
        }
    }

    /// Append and immediately make durable (test convenience).
    fn append_synced(wal: &Wal, record: &WalRecord) {
        let commit = wal.append(record).unwrap();
        wal.commit_sync(commit).unwrap();
    }

    #[test]
    fn append_and_replay_roundtrip() {
        let dir = tempdir();
        let path = dir.join("wal.log");

        let (wal, recovered) = Wal::open(&path, 0).unwrap();
        assert!(recovered.is_empty());
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        append_synced(&wal, &rec(2, "b", WalOp::Put(b"2".to_vec())));
        append_synced(&wal, &rec(3, "a", WalOp::Delete));
        drop(wal);

        let (_wal, recovered) = Wal::open(&path, 0).unwrap();
        assert_eq!(recovered.len(), 3);
        assert_eq!(recovered[0], rec(1, "a", WalOp::Put(b"1".to_vec())));
        assert_eq!(recovered[2], rec(3, "a", WalOp::Delete));
    }

    #[test]
    fn group_commit_coalesces_syncs() {
        // Two appends, then one sync_through of the first commit makes both
        // durable (the second commit is already <= synced).
        let dir = tempdir();
        let path = dir.join("wal.log");
        let (wal, _) = Wal::open(&path, 0).unwrap();
        let c1 = wal.append(&rec(1, "a", WalOp::Put(b"1".to_vec()))).unwrap();
        let c2 = wal.append(&rec(2, "b", WalOp::Put(b"2".to_vec()))).unwrap();
        wal.commit_sync(c2).unwrap();
        // c1 is already durable: its offset is below the synced point.
        wal.commit_sync(c1).unwrap();
        drop(wal);
        let (_wal, recovered) = Wal::open(&path, 0).unwrap();
        assert_eq!(recovered.len(), 2);
    }

    #[test]
    fn truncate_supersedes_old_commits() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        let (wal, _) = Wal::open(&path, 0).unwrap();
        let old = wal.append(&rec(1, "a", WalOp::Put(b"1".to_vec()))).unwrap();
        wal.truncate().unwrap();
        // Syncing a pre-truncation commit is a no-op (data is durable elsewhere).
        wal.commit_sync(old).unwrap();
        append_synced(&wal, &rec(2, "b", WalOp::Put(b"2".to_vec())));
        drop(wal);
        let (_wal, recovered) = Wal::open(&path, 0).unwrap();
        assert_eq!(recovered, vec![rec(2, "b", WalOp::Put(b"2".to_vec()))]);
    }

    #[test]
    fn torn_trailing_record_is_ignored() {
        let dir = tempdir();
        let path = dir.join("wal.log");

        let (wal, _) = Wal::open(&path, 0).unwrap();
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        drop(wal);

        // Simulate a crash mid-append by writing a bogus partial frame.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[99u8, 0, 0, 0, 1, 2]).unwrap(); // claims len=99, only 2 bytes
            f.flush().unwrap();
        }

        let (wal, recovered) = Wal::open(&path, 0).unwrap();
        assert_eq!(recovered.len(), 1, "torn tail should be ignored");
        drop(wal);
        // No longer truncated away (see zero-payload / preallocation tests
        // below) — replaying again must still land on the same 1 record,
        // and a fresh append must overwrite the torn bytes rather than
        // appending after them (correctness matters here, not exact size).
        let (again, recovered2) = Wal::open(&path, 0).unwrap();
        assert_eq!(recovered2.len(), 1);
        append_synced(&again, &rec(2, "b", WalOp::Put(b"2".to_vec())));
        drop(again);
        let (_wal3, recovered3) = Wal::open(&path, 0).unwrap();
        assert_eq!(
            recovered3,
            vec![
                rec(1, "a", WalOp::Put(b"1".to_vec())),
                rec(2, "b", WalOp::Put(b"2".to_vec())),
            ]
        );
    }

    #[test]
    fn zero_length_payload_is_treated_as_clean_end() {
        // A record can never legitimately have a zero-length payload (the
        // smallest real one is 17 bytes), and crc32(&[]) == 0 — so a
        // zero-filled region (pre-allocated tail, or a hole from any other
        // source) must stop replay cleanly, not be decoded and error out.
        let dir = tempdir();
        let path = dir.join("wal.log");
        let (wal, _) = Wal::open(&path, 0).unwrap();
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        drop(wal);
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0u8; 64]).unwrap(); // zero-filled "unwritten tail"
            f.flush().unwrap();
        }
        let (_wal, recovered) = Wal::open(&path, 0).unwrap();
        assert_eq!(recovered, vec![rec(1, "a", WalOp::Put(b"1".to_vec()))]);
    }

    #[test]
    fn fresh_wal_is_preallocated_one_chunk() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        let (wal, _) = Wal::open(&path, 4096).unwrap();
        assert_eq!(std::fs::metadata(wal.path()).unwrap().len(), 4096);
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        // A small append fits inside the reserved chunk — no further growth.
        assert_eq!(std::fs::metadata(wal.path()).unwrap().len(), 4096);
    }

    #[test]
    fn preallocation_survives_restart() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        let (wal, _) = Wal::open(&path, 4096).unwrap();
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        drop(wal);
        // Reopening must not shrink the file back down to just the valid
        // record's length — that would defeat pre-allocation on every
        // restart, re-paying the extension cost on the next append.
        let (wal2, recovered) = Wal::open(&path, 4096).unwrap();
        assert_eq!(recovered, vec![rec(1, "a", WalOp::Put(b"1".to_vec()))]);
        assert_eq!(std::fs::metadata(wal2.path()).unwrap().len(), 4096);
        append_synced(&wal2, &rec(2, "b", WalOp::Put(b"2".to_vec())));
        drop(wal2);
        let (_wal3, recovered3) = Wal::open(&path, 4096).unwrap();
        assert_eq!(
            recovered3,
            vec![
                rec(1, "a", WalOp::Put(b"1".to_vec())),
                rec(2, "b", WalOp::Put(b"2".to_vec())),
            ]
        );
    }

    #[test]
    fn growth_past_the_first_chunk_reserves_further_chunks() {
        // Appends that cross a chunk boundary must extend the file by whole
        // chunks ahead of the data and keep every record intact.
        let dir = tempdir();
        let path = dir.join("wal.log");
        let (wal, _) = Wal::open(&path, 64).unwrap(); // tiny chunk, easy to cross
        let mut expected = Vec::new();
        for i in 0..50u64 {
            let r = rec(i, &format!("key{i}"), WalOp::Put(format!("val{i}").into_bytes()));
            append_synced(&wal, &r);
            expected.push(r);
        }
        let len = std::fs::metadata(wal.path()).unwrap().len();
        assert_eq!(len % 64, 0, "file grows in whole chunks");
        assert!(len >= wal.size_bytes(), "allocation stays ahead of appends");
        drop(wal);
        let (_wal, recovered) = Wal::open(&path, 64).unwrap();
        assert_eq!(recovered, expected);
    }

    #[test]
    fn rotate_preallocates_fresh_segment() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        let (mut wal, _) = Wal::open(&path, 4096).unwrap();
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        wal.rotate(1).unwrap();
        // rotate() renamed the old active file to a sealed segment and
        // opened a fresh active file in its place — that fresh file is what
        // must be preallocated (the sealed segment keeps whatever size it
        // already had, zero tail and all).
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 4096);
        append_synced(&wal, &rec(2, "b", WalOp::Put(b"2".to_vec())));
        drop(wal);
        // Sealed segment 1 (record 1) plus the active log (record 2) both
        // replay — rotate() doesn't discard data, only a later
        // drop_segments_through (post-flush) would.
        let (_wal, recovered) = Wal::open(&path, 4096).unwrap();
        assert_eq!(
            recovered,
            vec![
                rec(1, "a", WalOp::Put(b"1".to_vec())),
                rec(2, "b", WalOp::Put(b"2".to_vec())),
            ]
        );
    }

    #[test]
    fn truncate_re_reserves_a_zeroed_chunk() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        let (wal, _) = Wal::open(&path, 4096).unwrap();
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        wal.truncate().unwrap();
        // Still one chunk on disk, but the old record must NOT survive in it
        // (truncate releases contents before re-reserving, so the chunk reads
        // as zeros — otherwise replay would resurrect flushed data).
        assert_eq!(std::fs::metadata(wal.path()).unwrap().len(), 4096);
        drop(wal);
        let (_wal, recovered) = Wal::open(&path, 4096).unwrap();
        assert!(recovered.is_empty(), "truncated records must not replay");
    }

    fn tempdir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut dir = std::env::temp_dir();
        dir.push(format!("skaidb-wal-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
