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
use crate::crypto::{Dek, Kek};
use crate::error::{Result, StorageError};
use crate::hlc::Hlc;

const OP_PUT: u8 = 0;
const OP_DELETE: u8 = 1;

/// Magic marking an ENCRYPTED WAL file (first 8 bytes). Byte index 4 is `0xEE`,
/// which a plaintext WAL can never have there — a plaintext record's byte 4 is
/// its `op` (0 or 1) — so plaintext vs encrypted files are told apart with zero
/// ambiguity (essential for mixed-file migration).
const WAL_ENC_MAGIC: [u8; 8] = [0x73, 0x6b, 0x61, 0x77, 0xEE, 0x45, 0x57, 0x4c]; // "ska\xEEEWL"
/// Encrypted-WAL header = magic(8) | u16 wrapped_dek_len | wrapped_dek(60).
const WAL_WRAPPED_DEK_LEN: usize = 12 + 32 + 16; // nonce + key + tag
const WAL_ENC_HEADER_LEN: u64 = 8 + 2 + WAL_WRAPPED_DEK_LEN as u64;

/// Per-file at-rest encryption state for a WAL. The DEK seals appends
/// (nonce = record byte offset, unique within the file); a FRESH DEK is minted
/// on every `truncate`/`rotate` (which reset offsets to the header end), so an
/// offset is never a nonce twice under one key.
struct WalCrypto {
    kek: Kek,
    dek: Dek,
    /// The serialized header bytes (magic | len | wrapped_dek), re-written at
    /// offset 0 whenever the file is reset.
    header: Vec<u8>,
}

impl std::fmt::Debug for WalCrypto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WalCrypto(<redacted>)")
    }
}

impl WalCrypto {
    fn kek(&self) -> &Kek {
        &self.kek
    }

    /// Mint a fresh DEK under `kek` and build the header bytes.
    fn fresh(kek: &Kek) -> Result<WalCrypto> {
        let (dek, wrapped) = kek.wrap_new_dek()?;
        let mut header = Vec::with_capacity(WAL_ENC_HEADER_LEN as usize);
        header.extend_from_slice(&WAL_ENC_MAGIC);
        header.extend_from_slice(&(wrapped.len() as u16).to_le_bytes());
        header.extend_from_slice(&wrapped);
        Ok(WalCrypto {
            kek: kek.clone(),
            dek,
            header,
        })
    }
}

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
    /// At-rest encryption state (`None` = plaintext WAL). Holds the DEK that
    /// seals appends and is replaced on truncate/rotate.
    crypto: Option<WalCrypto>,
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
    pub fn open(
        path: impl AsRef<Path>,
        prealloc_chunk: u64,
        kek: Option<&Kek>,
        encrypt_new: bool,
    ) -> Result<(Wal, Vec<WalRecord>)> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        // Sealed segments (frozen memtables whose background flush never
        // completed) replay first, ascending, then the active log — the same
        // order the writes happened in. Each file self-describes as plaintext
        // or encrypted via its header, so a mixed set replays uniformly.
        let mut records = Vec::new();
        for seq in list_segments(&path)? {
            let seg = OpenOptions::new().read(true).open(segment_path(&path, seq))?;
            let (mut seg_records, _, _) = recover_file(&seg, kek)?;
            records.append(&mut seg_records);
        }
        let (mut active_records, good_len_existing, existing_dek) = recover_file(&file, kek)?;
        records.append(&mut active_records);

        // Decide this file's encryption state:
        // - it already had an encrypted header  → keep that DEK, resume;
        // - it is empty and encryption is on    → mint a DEK, write a header;
        // - otherwise                           → plaintext.
        let (crypto, good_len) = if let Some(dek) = existing_dek {
            // Reuse the on-disk header bytes (re-derive from the wrapped DEK is
            // unnecessary; truncate/rotate will mint fresh ones as needed).
            let (wrapped, _) = read_enc_header(&file)?.expect("header present");
            let mut header = Vec::with_capacity(WAL_ENC_HEADER_LEN as usize);
            header.extend_from_slice(&WAL_ENC_MAGIC);
            header.extend_from_slice(&(wrapped.len() as u16).to_le_bytes());
            header.extend_from_slice(&wrapped);
            let kek = kek.expect("encrypted file implies a key").clone();
            (Some(WalCrypto { kek, dek, header }), good_len_existing)
        } else if good_len_existing == 0 && encrypt_new {
            let kek = kek.ok_or_else(|| {
                StorageError::Crypto("at-rest is on but no keyfile is configured".into())
            })?;
            let c = WalCrypto::fresh(kek)?;
            write_all_at(&file, &c.header, 0)?;
            file.sync_all()?; // the header (wrapped DEK) must be durable first
            (Some(c), WAL_ENC_HEADER_LEN)
        } else {
            (None, good_len_existing)
        };

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
                crypto,
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
        // buys nothing — 0 disables it. Left plaintext: the file is an
        // unlinked, page-cache-only inode holding RAM-table data.
        Ok(Wal {
            sync,
            path,
            prealloc_chunk: 0,
            allocated: AtomicU64::new(0),
            crypto: None,
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
        // Build the plaintext payload.
        let payload_len = 1 + 12 + 4 + key.len() + value.map_or(0, |v| 4 + v.len());
        let mut payload = Vec::with_capacity(payload_len);
        payload.push(if value.is_some() { OP_PUT } else { OP_DELETE });
        payload.extend_from_slice(&hlc.to_bytes());
        payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
        payload.extend_from_slice(key);
        if let Some(val) = value {
            payload.extend_from_slice(&(val.len() as u32).to_le_bytes());
            payload.extend_from_slice(val);
        }

        let offset = self.sync.write_offset.load(Ordering::SeqCst);
        // Encrypted: the frame body is the SEALED payload, keyed by this
        // record's byte offset (unique within the file's DEK lifetime — a
        // fresh DEK is minted on every truncate/rotate that resets offsets).
        let body = match &self.crypto {
            Some(c) => c.dek.seal(offset, &payload)?,
            None => payload,
        };
        let mut frame = Vec::with_capacity(4 + body.len() + 4);
        frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
        frame.extend_from_slice(&body);
        frame.extend_from_slice(&crc32(&frame[4..]).to_le_bytes());

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
        // The fresh active file gets a FRESH DEK + header when encrypting, so
        // its offsets (reused from the sealed segment's) never reuse a nonce
        // under the old key. Its byte-0 header must be durable before records.
        let start = if self.crypto.is_some() {
            let c = WalCrypto::fresh(self.crypto.as_ref().unwrap().kek())?;
            write_all_at(&file, &c.header, 0)?;
            file.sync_all()?;
            self.crypto = Some(c);
            WAL_ENC_HEADER_LEN
        } else {
            0
        };
        let mut allocated = 0;
        if self.prealloc_chunk > 0 {
            allocated = round_up(start.max(1), self.prealloc_chunk);
            file.set_len(allocated)?;
            file.sync_all()?;
        }
        self.allocated.store(allocated, Ordering::SeqCst);
        self.sync = Arc::new(WalSync {
            file,
            ephemeral: false,
            write_offset: AtomicU64::new(start),
            synced: AtomicU64::new(start),
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
    pub fn truncate(&mut self) -> Result<()> {
        let _guard = self.sync.sync_lock.lock().expect("wal sync lock");
        // set_len(0) first so the old contents are actually released and the
        // re-reserved chunk reads as zeros (a bare shrink to chunk size would
        // leave stale record bytes in place, which replay would then replay).
        self.sync.file.set_len(0)?;
        // Encrypting: the truncate zeroed the header AND resets offsets, so
        // mint a FRESH DEK and write its header at byte 0 — offsets restart at
        // the header end, never reusing a nonce under the old key.
        let start = if self.crypto.is_some() {
            let c = WalCrypto::fresh(self.crypto.as_ref().unwrap().kek())?;
            write_all_at(&self.sync.file, &c.header, 0)?;
            self.crypto = Some(c);
            WAL_ENC_HEADER_LEN
        } else {
            0
        };
        let alloc = round_up(start.max(1), self.prealloc_chunk.max(1));
        self.sync.file.set_len(alloc.max(self.prealloc_chunk))?;
        self.allocated
            .store(alloc.max(self.prealloc_chunk), Ordering::SeqCst);
        self.sync.file.sync_data()?;
        self.sync.fsyncs.fetch_add(1, Ordering::SeqCst);
        self.sync.write_offset.store(start, Ordering::SeqCst);
        self.sync.synced.store(start, Ordering::SeqCst);
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

/// Read the encrypted-WAL header at offset 0, if present. Returns the wrapped
/// DEK bytes and the header length, or `None` for a plaintext/empty file.
fn read_enc_header(file: &File) -> Result<Option<(Vec<u8>, u64)>> {
    let len = file.metadata()?.len();
    if len < WAL_ENC_HEADER_LEN {
        return Ok(None);
    }
    let mut magic = [0u8; 8];
    crate::posfile::read_exact_at(file, &mut magic, 0)?;
    if magic != WAL_ENC_MAGIC {
        return Ok(None);
    }
    let mut lenbuf = [0u8; 2];
    crate::posfile::read_exact_at(file, &mut lenbuf, 8)?;
    let wlen = u16::from_le_bytes(lenbuf) as usize;
    let mut wrapped = vec![0u8; wlen];
    crate::posfile::read_exact_at(file, &mut wrapped, 10)?;
    Ok(Some((wrapped, 10 + wlen as u64)))
}

/// Recover records from one WAL file, transparently handling plaintext or
/// encrypted content. Returns `(records, good_len, dek)` — `good_len` is the
/// absolute byte length up to the last valid record (past the header for an
/// encrypted file), `dek` is the file's key when encrypted (so the active file
/// can keep appending under it).
fn recover_file(file: &File, kek: Option<&Kek>) -> Result<(Vec<WalRecord>, u64, Option<Dek>)> {
    match read_enc_header(file)? {
        Some((wrapped, header_len)) => {
            let kek = kek.ok_or_else(|| {
                StorageError::Crypto("WAL is encrypted but no keyfile is configured".into())
            })?;
            let dek = kek.unwrap_dek(&wrapped)?;
            let (records, good_len) = replay(file, Some(&dek), header_len)?;
            Ok((records, good_len, Some(dek)))
        }
        None => {
            let (records, good_len) = replay(file, None, 0)?;
            Ok((records, good_len, None))
        }
    }
}

fn read_u32(bytes: &[u8]) -> u32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(bytes);
    u32::from_le_bytes(b)
}

/// Replay a WAL file, returning the recovered records and the byte length up to
/// the last fully-valid record (so callers can truncate a torn tail).
fn replay(file: &File, dek: Option<&Dek>, start_offset: u64) -> Result<(Vec<WalRecord>, u64)> {
    use std::io::Seek;
    let mut reader = BufReader::new(file);
    reader.seek(std::io::SeekFrom::Start(start_offset))?;
    let mut records = Vec::new();
    // `good_len` is the absolute file offset of the next record (the record
    // start also serves as its AEAD nonce when encrypted).
    let mut good_len: u64 = start_offset;

    loop {
        // Read the 4-byte length prefix; a short read means a clean or torn EOF.
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let frame_len = u32::from_le_bytes(len_buf) as usize;
        if frame_len == 0 {
            // No real record ever encodes a zero-length frame (the smallest
            // plaintext payload is 17 bytes; a sealed one adds the 16-byte
            // tag) — this is the zero-filled unwritten tail of a
            // pre-allocated segment. `crc32(&[]) == 0` too, so without this
            // check a zero length prefix would pass the checksum below and
            // then fail to decode, turning a clean "nothing more here" into a
            // hard replay error.
            break;
        }

        let mut frame = vec![0u8; frame_len];
        if reader.read_exact(&mut frame).is_err() {
            break; // torn payload
        }
        let mut crc_buf = [0u8; 4];
        if reader.read_exact(&mut crc_buf).is_err() {
            break; // torn checksum
        }
        if u32::from_le_bytes(crc_buf) != crc32(&frame) {
            break; // torn / corrupt trailing record
        }

        // Encrypted: the frame is the sealed payload; open it (nonce = the
        // record's start offset). A tag failure here is corruption/tamper,
        // NOT a torn tail, so it is a hard error — the CRC already passed.
        let payload = match dek {
            Some(dek) => dek.open(good_len, &frame)?,
            None => frame,
        };

        let record = WalRecord::decode_payload(&payload, good_len)?;
        records.push(record);
        good_len += (4 + frame_len + 4) as u64;
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

    // --- at-rest encryption ---

    fn test_kek() -> Kek {
        Kek::from_bytes(&[42u8; 32]).unwrap()
    }

    /// Encrypted round trip: records survive a close/reopen, the on-disk bytes
    /// are ciphertext (the key/value never appear in the clear), and the file
    /// carries the encryption magic.
    #[test]
    fn encrypted_round_trip_and_ciphertext_on_disk() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        let kek = test_kek();
        {
            let (wal, _) = Wal::open(&path, 4096, Some(&kek), true).unwrap();
            append_synced(&wal, &rec(1, "topsecret_key", WalOp::Put(b"topsecret_value".to_vec())));
            append_synced(&wal, &rec(2, "k2", WalOp::Delete));
        }
        // Raw bytes: magic present, plaintext key/value absent.
        let raw = std::fs::read(&path).unwrap();
        assert_eq!(&raw[..8], &WAL_ENC_MAGIC, "encrypted file starts with the magic");
        let hay = String::from_utf8_lossy(&raw);
        assert!(!hay.contains("topsecret_key"), "key must not be on disk in the clear");
        assert!(!hay.contains("topsecret_value"), "value must not be on disk in the clear");
        // Reopen with the key: records recover intact.
        let (_wal, recovered) = Wal::open(&path, 4096, Some(&kek), true).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].key, b"topsecret_key");
        assert_eq!(recovered[0].op, WalOp::Put(b"topsecret_value".to_vec()));
        assert_eq!(recovered[1].op, WalOp::Delete);
    }

    /// The wrong KEK cannot open an encrypted WAL (fails, does not return junk).
    #[test]
    fn wrong_kek_refuses_to_open() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        {
            let (wal, _) = Wal::open(&path, 0, Some(&test_kek()), true).unwrap();
            append_synced(&wal, &rec(1, "a", WalOp::Put(b"v".to_vec())));
        }
        let wrong = Kek::from_bytes(&[7u8; 32]).unwrap();
        assert!(Wal::open(&path, 0, Some(&wrong), true).is_err(), "wrong KEK must fail");
        // And with NO key at all.
        assert!(Wal::open(&path, 0, None, false).is_err(), "encrypted file needs a key");
    }

    /// A tampered ciphertext record fails the AEAD tag — a hard error, not a
    /// silent torn-tail truncation (the CRC still passes, so this proves the
    /// tag is what catches it).
    #[test]
    fn tampered_encrypted_record_is_rejected() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        let kek = test_kek();
        {
            let (wal, _) = Wal::open(&path, 0, Some(&kek), true).unwrap();
            append_synced(&wal, &rec(1, "k", WalOp::Put(b"secret".to_vec())));
        }
        // Flip a byte inside the sealed body (just past the header + len prefix)
        // and fix the CRC so only the AEAD tag can catch it.
        let mut raw = std::fs::read(&path).unwrap();
        let body_start = WAL_ENC_HEADER_LEN as usize + 4;
        raw[body_start] ^= 0x40;
        // recompute crc over the (tampered) frame body
        let len = u32::from_le_bytes(raw[WAL_ENC_HEADER_LEN as usize..WAL_ENC_HEADER_LEN as usize + 4].try_into().unwrap()) as usize;
        let crc = crc32(&raw[body_start..body_start + len]);
        raw[body_start + len..body_start + len + 4].copy_from_slice(&crc.to_le_bytes());
        std::fs::write(&path, &raw).unwrap();
        assert!(Wal::open(&path, 0, Some(&kek), true).is_err(), "tampered record must fail the tag");
    }

    /// Mixed migration: a legacy PLAINTEXT segment plus an ENCRYPTED active log
    /// both replay under one open (each file self-describes).
    #[test]
    fn mixed_plaintext_segment_and_encrypted_active_replay() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        // A plaintext sealed segment (as an old build would have left it):
        // write it directly at the segment path so it self-describes as
        // plaintext (no header/magic).
        let seg = segment_path(&path, 1);
        {
            let (wal, _) = Wal::open(&seg, 0, None, false).unwrap();
            append_synced(&wal, &rec(1, "old_plain", WalOp::Put(b"p".to_vec())));
        }
        // Encrypted active log alongside it.
        let kek = test_kek();
        {
            let (wal, _) = Wal::open(&path, 0, Some(&kek), true).unwrap();
            append_synced(&wal, &rec(2, "new_enc", WalOp::Put(b"e".to_vec())));
        }
        let (_wal, recovered) = Wal::open(&path, 0, Some(&kek), true).unwrap();
        let keys: Vec<_> = recovered.iter().map(|r| String::from_utf8_lossy(&r.key).to_string()).collect();
        assert!(keys.contains(&"old_plain".to_string()), "plaintext segment replayed: {keys:?}");
        assert!(keys.contains(&"new_enc".to_string()), "encrypted active replayed: {keys:?}");
    }

    /// truncate mints a fresh DEK, so reused offsets never reuse a nonce; the
    /// WAL keeps working (encrypted) across a truncate.
    #[test]
    fn encrypted_survives_truncate_with_fresh_key() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        let kek = test_kek();
        let (mut wal, _) = Wal::open(&path, 4096, Some(&kek), true).unwrap();
        append_synced(&wal, &rec(1, "before", WalOp::Put(b"x".to_vec())));
        let hdr1 = std::fs::read(&path).unwrap()[..WAL_ENC_HEADER_LEN as usize].to_vec();
        wal.truncate().unwrap();
        append_synced(&wal, &rec(2, "after", WalOp::Put(b"y".to_vec())));
        let hdr2 = std::fs::read(&path).unwrap()[..WAL_ENC_HEADER_LEN as usize].to_vec();
        assert_ne!(hdr1, hdr2, "truncate must mint a fresh DEK (header changes)");
        drop(wal);
        let (_wal, recovered) = Wal::open(&path, 4096, Some(&kek), true).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].key, b"after");
    }

    #[test]
    fn append_and_replay_roundtrip() {
        let dir = tempdir();
        let path = dir.join("wal.log");

        let (wal, recovered) = Wal::open(&path, 0, None, false).unwrap();
        assert!(recovered.is_empty());
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        append_synced(&wal, &rec(2, "b", WalOp::Put(b"2".to_vec())));
        append_synced(&wal, &rec(3, "a", WalOp::Delete));
        drop(wal);

        let (_wal, recovered) = Wal::open(&path, 0, None, false).unwrap();
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
        let (wal, _) = Wal::open(&path, 0, None, false).unwrap();
        let c1 = wal.append(&rec(1, "a", WalOp::Put(b"1".to_vec()))).unwrap();
        let c2 = wal.append(&rec(2, "b", WalOp::Put(b"2".to_vec()))).unwrap();
        wal.commit_sync(c2).unwrap();
        // c1 is already durable: its offset is below the synced point.
        wal.commit_sync(c1).unwrap();
        drop(wal);
        let (_wal, recovered) = Wal::open(&path, 0, None, false).unwrap();
        assert_eq!(recovered.len(), 2);
    }

    #[test]
    fn truncate_supersedes_old_commits() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        let (mut wal, _) = Wal::open(&path, 0, None, false).unwrap();
        let old = wal.append(&rec(1, "a", WalOp::Put(b"1".to_vec()))).unwrap();
        wal.truncate().unwrap();
        // Syncing a pre-truncation commit is a no-op (data is durable elsewhere).
        wal.commit_sync(old).unwrap();
        append_synced(&wal, &rec(2, "b", WalOp::Put(b"2".to_vec())));
        drop(wal);
        let (_wal, recovered) = Wal::open(&path, 0, None, false).unwrap();
        assert_eq!(recovered, vec![rec(2, "b", WalOp::Put(b"2".to_vec()))]);
    }

    #[test]
    fn torn_trailing_record_is_ignored() {
        let dir = tempdir();
        let path = dir.join("wal.log");

        let (wal, _) = Wal::open(&path, 0, None, false).unwrap();
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        drop(wal);

        // Simulate a crash mid-append by writing a bogus partial frame.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[99u8, 0, 0, 0, 1, 2]).unwrap(); // claims len=99, only 2 bytes
            f.flush().unwrap();
        }

        let (wal, recovered) = Wal::open(&path, 0, None, false).unwrap();
        assert_eq!(recovered.len(), 1, "torn tail should be ignored");
        drop(wal);
        // No longer truncated away (see zero-payload / preallocation tests
        // below) — replaying again must still land on the same 1 record,
        // and a fresh append must overwrite the torn bytes rather than
        // appending after them (correctness matters here, not exact size).
        let (again, recovered2) = Wal::open(&path, 0, None, false).unwrap();
        assert_eq!(recovered2.len(), 1);
        append_synced(&again, &rec(2, "b", WalOp::Put(b"2".to_vec())));
        drop(again);
        let (_wal3, recovered3) = Wal::open(&path, 0, None, false).unwrap();
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
        let (wal, _) = Wal::open(&path, 0, None, false).unwrap();
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        drop(wal);
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0u8; 64]).unwrap(); // zero-filled "unwritten tail"
            f.flush().unwrap();
        }
        let (_wal, recovered) = Wal::open(&path, 0, None, false).unwrap();
        assert_eq!(recovered, vec![rec(1, "a", WalOp::Put(b"1".to_vec()))]);
    }

    #[test]
    fn fresh_wal_is_preallocated_one_chunk() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        let (wal, _) = Wal::open(&path, 4096, None, false).unwrap();
        assert_eq!(std::fs::metadata(wal.path()).unwrap().len(), 4096);
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        // A small append fits inside the reserved chunk — no further growth.
        assert_eq!(std::fs::metadata(wal.path()).unwrap().len(), 4096);
    }

    #[test]
    fn preallocation_survives_restart() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        let (wal, _) = Wal::open(&path, 4096, None, false).unwrap();
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        drop(wal);
        // Reopening must not shrink the file back down to just the valid
        // record's length — that would defeat pre-allocation on every
        // restart, re-paying the extension cost on the next append.
        let (wal2, recovered) = Wal::open(&path, 4096, None, false).unwrap();
        assert_eq!(recovered, vec![rec(1, "a", WalOp::Put(b"1".to_vec()))]);
        assert_eq!(std::fs::metadata(wal2.path()).unwrap().len(), 4096);
        append_synced(&wal2, &rec(2, "b", WalOp::Put(b"2".to_vec())));
        drop(wal2);
        let (_wal3, recovered3) = Wal::open(&path, 4096, None, false).unwrap();
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
        let (wal, _) = Wal::open(&path, 64, None, false).unwrap(); // tiny chunk, easy to cross
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
        let (_wal, recovered) = Wal::open(&path, 64, None, false).unwrap();
        assert_eq!(recovered, expected);
    }

    #[test]
    fn rotate_preallocates_fresh_segment() {
        let dir = tempdir();
        let path = dir.join("wal.log");
        let (mut wal, _) = Wal::open(&path, 4096, None, false).unwrap();
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
        let (_wal, recovered) = Wal::open(&path, 4096, None, false).unwrap();
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
        let (mut wal, _) = Wal::open(&path, 4096, None, false).unwrap();
        append_synced(&wal, &rec(1, "a", WalOp::Put(b"1".to_vec())));
        wal.truncate().unwrap();
        // Still one chunk on disk, but the old record must NOT survive in it
        // (truncate releases contents before re-reserving, so the chunk reads
        // as zeros — otherwise replay would resurrect flushed data).
        assert_eq!(std::fs::metadata(wal.path()).unwrap().len(), 4096);
        drop(wal);
        let (_wal, recovered) = Wal::open(&path, 4096, None, false).unwrap();
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
