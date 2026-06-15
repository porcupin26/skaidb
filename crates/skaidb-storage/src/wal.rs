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
    fn encode_payload(&self) -> Vec<u8> {
        let mut p = Vec::new();
        match &self.op {
            WalOp::Put(_) => p.push(OP_PUT),
            WalOp::Delete => p.push(OP_DELETE),
        }
        p.extend_from_slice(&self.hlc.to_bytes());
        p.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        p.extend_from_slice(&self.key);
        if let WalOp::Put(val) = &self.op {
            p.extend_from_slice(&(val.len() as u32).to_le_bytes());
            p.extend_from_slice(val);
        }
        p
    }

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

/// An append-only write-ahead log file.
#[derive(Debug)]
pub struct Wal {
    sync: Arc<WalSync>,
    path: PathBuf,
}

impl Wal {
    /// Open (creating if needed) the WAL at `path` and replay it. A torn
    /// trailing record is truncated so future appends start from a clean
    /// boundary.
    pub fn open(path: impl AsRef<Path>) -> Result<(Wal, Vec<WalRecord>)> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let (records, good_len) = replay(&file)?;
        file.set_len(good_len)?; // drop any torn trailing bytes

        let sync = Arc::new(WalSync {
            file,
            write_offset: AtomicU64::new(good_len),
            synced: AtomicU64::new(good_len),
            generation: AtomicU64::new(0),
            fsyncs: AtomicU64::new(0),
            sync_lock: Mutex::new(()),
        });
        Ok((Wal { sync, path }, records))
    }

    /// Append a record (positional write, no fsync) and return its commit point.
    ///
    /// Appends must be serialized by the caller (the engine's write lock); the
    /// returned [`WalCommit`] is made durable later via [`WalSync::sync_through`].
    pub fn append(&self, record: &WalRecord) -> Result<WalCommit> {
        let payload = record.encode_payload();
        let mut frame = Vec::with_capacity(payload.len() + 8);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(&crc32(&payload).to_le_bytes());

        let offset = self.sync.write_offset.load(Ordering::SeqCst);
        write_all_at(&self.sync.file, &frame, offset)?;
        let end = offset + frame.len() as u64;
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

    /// Truncate the log to empty and bump the generation. Called after a flush
    /// makes the logged mutations durable in an SSTable.
    pub fn truncate(&self) -> Result<()> {
        let _guard = self.sync.sync_lock.lock().expect("wal sync lock");
        self.sync.file.set_len(0)?;
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

        let (wal, recovered) = Wal::open(&path).unwrap();
        assert!(recovered.is_empty());
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        append_synced(&wal, &rec(2, "b", WalOp::Put(b"2".to_vec())));
        append_synced(&wal, &rec(3, "a", WalOp::Delete));
        drop(wal);

        let (_wal, recovered) = Wal::open(&path).unwrap();
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
        let (wal, _) = Wal::open(&path).unwrap();
        let c1 = wal.append(&rec(1, "a", WalOp::Put(b"1".to_vec()))).unwrap();
        let c2 = wal.append(&rec(2, "b", WalOp::Put(b"2".to_vec()))).unwrap();
        wal.commit_sync(c2).unwrap();
        // c1 is already durable: its offset is below the synced point.
        wal.commit_sync(c1).unwrap();
        drop(wal);
        let (_wal, recovered) = Wal::open(&path).unwrap();
        assert_eq!(recovered.len(), 2);
    }

    #[test]
    fn truncate_supersedes_old_commits() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        let (wal, _) = Wal::open(&path).unwrap();
        let old = wal.append(&rec(1, "a", WalOp::Put(b"1".to_vec()))).unwrap();
        wal.truncate().unwrap();
        // Syncing a pre-truncation commit is a no-op (data is durable elsewhere).
        wal.commit_sync(old).unwrap();
        append_synced(&wal, &rec(2, "b", WalOp::Put(b"2".to_vec())));
        drop(wal);
        let (_wal, recovered) = Wal::open(&path).unwrap();
        assert_eq!(recovered, vec![rec(2, "b", WalOp::Put(b"2".to_vec()))]);
    }

    #[test]
    fn torn_trailing_record_is_truncated() {
        let dir = tempdir();
        let path = dir.join("wal.log");

        let (wal, _) = Wal::open(&path).unwrap();
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        drop(wal);

        // Simulate a crash mid-append by writing a bogus partial frame.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[99u8, 0, 0, 0, 1, 2]).unwrap(); // claims len=99, only 2 bytes
            f.flush().unwrap();
        }

        let (wal, recovered) = Wal::open(&path).unwrap();
        assert_eq!(recovered.len(), 1, "torn tail should be ignored");
        let clean_len = std::fs::metadata(wal.path()).unwrap().len();
        let (_again, recovered2) = Wal::open(wal.path()).unwrap();
        assert_eq!(recovered2.len(), 1);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), clean_len);
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
