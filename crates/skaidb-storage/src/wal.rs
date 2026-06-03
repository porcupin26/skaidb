//! Write-ahead log (SPEC §12).
//!
//! Every mutation is appended to the WAL before it is applied to the in-memory
//! memtable, so a crash can be recovered by replaying the log. Records are
//! length-prefixed and CRC-checked; a torn trailing record (from a crash
//! mid-append) is detected and truncated on open rather than treated as fatal.
//!
//! On-disk record layout (all integers little-endian):
//! ```text
//! u32 payload_len | payload[payload_len] | u32 crc32(payload)
//! payload = u8 op | hlc[12] | u32 key_len | key | (op==Put) u32 val_len | val
//! ```

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

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

/// An append-only write-ahead log file.
#[derive(Debug)]
pub struct Wal {
    file: File,
    path: PathBuf,
}

impl Wal {
    /// Open (creating if needed) the WAL at `path`, replay it, and position the
    /// file at the end for subsequent appends.
    ///
    /// Returns the recovered records in append order. A torn trailing record is
    /// truncated so future appends start from a clean boundary.
    pub fn open(path: impl AsRef<Path>) -> Result<(Wal, Vec<WalRecord>)> {
        let path = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let (records, good_len) = replay(&file)?;

        // Drop any torn trailing bytes, then seek to the end.
        file.set_len(good_len)?;
        file.seek(SeekFrom::End(0))?;

        Ok((Wal { file, path }, records))
    }

    /// Append a record and flush it durably to disk.
    pub fn append(&mut self, record: &WalRecord) -> Result<()> {
        let payload = record.encode_payload();
        let mut frame = Vec::with_capacity(payload.len() + 8);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(&crc32(&payload).to_le_bytes());
        self.file.write_all(&frame)?;
        self.file.flush()?;
        self.file.sync_data()?;
        Ok(())
    }

    /// Path backing this WAL.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Truncate the log to empty. Called after a flush makes the logged
    /// mutations durable in an SSTable, so they no longer need replay.
    pub fn truncate(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.sync_all()?;
        Ok(())
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

    fn rec(hlc: u64, key: &str, op: WalOp) -> WalRecord {
        WalRecord {
            hlc: Hlc::new(hlc, 0),
            key: key.as_bytes().to_vec(),
            op,
        }
    }

    #[test]
    fn append_and_replay_roundtrip() {
        let dir = tempdir();
        let path = dir.join("wal.log");

        let (mut wal, recovered) = Wal::open(&path).unwrap();
        assert!(recovered.is_empty());
        wal.append(&rec(1, "a", WalOp::Put(b"1".to_vec()))).unwrap();
        wal.append(&rec(2, "b", WalOp::Put(b"2".to_vec()))).unwrap();
        wal.append(&rec(3, "a", WalOp::Delete)).unwrap();
        drop(wal);

        let (_wal, recovered) = Wal::open(&path).unwrap();
        assert_eq!(recovered.len(), 3);
        assert_eq!(recovered[0], rec(1, "a", WalOp::Put(b"1".to_vec())));
        assert_eq!(recovered[2], rec(3, "a", WalOp::Delete));
    }

    #[test]
    fn torn_trailing_record_is_truncated() {
        let dir = tempdir();
        let path = dir.join("wal.log");

        let (mut wal, _) = Wal::open(&path).unwrap();
        wal.append(&rec(1, "a", WalOp::Put(b"1".to_vec()))).unwrap();
        drop(wal);

        // Simulate a crash mid-append by writing a bogus partial frame.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[99u8, 0, 0, 0, 1, 2]).unwrap(); // claims len=99, only 2 bytes
            f.flush().unwrap();
        }

        let (wal, recovered) = Wal::open(&path).unwrap();
        assert_eq!(recovered.len(), 1, "torn tail should be ignored");
        // The torn bytes were truncated, so the file is back to one clean record.
        let clean_len = std::fs::metadata(wal.path()).unwrap().len();
        let (_again, recovered2) = Wal::open(wal.path()).unwrap();
        assert_eq!(recovered2.len(), 1);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), clean_len);
    }

    /// Minimal unique temp dir helper (avoids a dev-dependency).
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
