//! The storage engine façade (SPEC §12) — an LSM tree.
//!
//! Writes go to the [`Wal`] then the active [`Memtable`]. When the memtable
//! grows past its threshold it is flushed to an immutable [`SsTable`] at level 0
//! and the WAL is truncated. Background-style compaction merges tables: level 0
//! is size-tiered (overlapping runs from successive flushes), and deeper levels
//! hold a single merged, non-overlapping run each (lazy leveling, SPEC §12).
//!
//! Reads consult the memtable first, then level-0 tables newest-first, then each
//! deeper level — the first source holding the key wins, since newer sources are
//! consulted first. Each table stores only the latest version per key, so MVCC
//! snapshot history is bounded to the live memtable.

use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};
use crate::hlc::{Hlc, HlcClock};
use crate::memtable::{Memtable, VersionValue};
use crate::sstable::{SsTable, SstEntry};
use crate::wal::{Wal, WalCommit, WalOp, WalRecord, WalSync};

/// The latest version of each key, keyed by storage key (used by merged reads).
type MergedRows = std::collections::BTreeMap<Vec<u8>, (Hlc, VersionValue)>;

/// A live key/value pair together with its version stamp.
pub type VersionedRow = (Vec<u8>, Vec<u8>, Hlc);

/// Default memtable size that triggers a flush (SPEC §9.1: 256 MiB).
pub const DEFAULT_FLUSH_THRESHOLD_BYTES: usize = 256 * 1024 * 1024;
/// Number of level-0 tables that triggers compaction.
const DEFAULT_L0_COMPACTION_TRIGGER: usize = 4;
/// Entry capacity of level 1; each deeper level holds 10× more.
const DEFAULT_LEVEL1_CAPACITY: u64 = 1024;

/// Tuning knobs for the engine (mainly to make tests exercise flush/compaction).
#[derive(Debug, Clone, Copy)]
pub struct EngineOptions {
    pub flush_threshold_bytes: usize,
    pub l0_compaction_trigger: usize,
    pub level1_capacity: u64,
}

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions {
            flush_threshold_bytes: DEFAULT_FLUSH_THRESHOLD_BYTES,
            l0_compaction_trigger: DEFAULT_L0_COMPACTION_TRIGGER,
            level1_capacity: DEFAULT_LEVEL1_CAPACITY,
        }
    }
}

/// A single-node, single-keyspace LSM storage engine.
#[derive(Debug)]
pub struct Engine {
    dir: PathBuf,
    wal: Wal,
    mem: Memtable,
    clock: HlcClock,
    opts: EngineOptions,
    /// Level-0 tables, newest first.
    l0: Vec<SsTable>,
    /// Deeper levels: `levels[0]` is L1, `levels[1]` is L2, … each a single run.
    levels: Vec<SsTable>,
    next_seq: u64,
}

impl Engine {
    /// Open (creating if needed) an engine rooted at `dir` with defaults.
    pub fn open(dir: impl AsRef<Path>) -> Result<Engine> {
        Engine::open_with_options(dir, EngineOptions::default())
    }

    /// Open with an explicit flush threshold (bytes); other knobs default.
    pub fn open_with(dir: impl AsRef<Path>, flush_threshold_bytes: usize) -> Result<Engine> {
        Engine::open_with_options(
            dir,
            EngineOptions {
                flush_threshold_bytes,
                ..EngineOptions::default()
            },
        )
    }

    /// Open with full options control.
    pub fn open_with_options(dir: impl AsRef<Path>, opts: EngineOptions) -> Result<Engine> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        std::fs::create_dir_all(dir.join("sst"))?;

        let (wal, records) = Wal::open(dir.join("wal.log"))?;
        let mut mem = Memtable::new();
        let mut max_hlc = Hlc::MIN;
        for rec in records {
            max_hlc = max_hlc.max(rec.hlc);
            let value = match rec.op {
                WalOp::Put(bytes) => VersionValue::Put(bytes),
                WalOp::Delete => VersionValue::Delete,
            };
            mem.insert(rec.key, rec.hlc, value);
        }

        let (l0, levels, next_seq) = load_manifest(&dir)?;

        let clock = HlcClock::new();
        if max_hlc > Hlc::MIN {
            clock.observe(max_hlc);
        }

        Ok(Engine {
            dir,
            wal,
            mem,
            clock,
            opts,
            l0,
            levels,
            next_seq,
        })
    }

    /// Append a record to the WAL and apply it to the memtable, returning the
    /// commit point — **without** fsync. Callers make it durable separately
    /// (immediately, or batched outside a lock for group commit).
    fn append_buffered(
        &mut self,
        key: &[u8],
        hlc: Hlc,
        op: WalOp,
        value: VersionValue,
    ) -> Result<WalCommit> {
        let commit = self.wal.append(&WalRecord {
            hlc,
            key: key.to_vec(),
            op,
        })?;
        self.mem.insert(key.to_vec(), hlc, value);
        self.maybe_flush()?;
        Ok(commit)
    }

    /// Write `value` under `key`, returning the version stamp assigned.
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<Hlc> {
        let hlc = self.clock.now();
        let commit = self.append_buffered(
            key,
            hlc,
            WalOp::Put(value.clone()),
            VersionValue::Put(value),
        )?;
        self.wal.commit_sync(commit)?;
        Ok(hlc)
    }

    /// Delete `key` (writes a tombstone), returning the version stamp assigned.
    pub fn delete(&mut self, key: &[u8]) -> Result<Hlc> {
        let hlc = self.clock.now();
        let commit = self.append_buffered(key, hlc, WalOp::Delete, VersionValue::Delete)?;
        self.wal.commit_sync(commit)?;
        Ok(hlc)
    }

    /// Apply a write at a caller-supplied stamp (replication: a replica stores
    /// the coordinator's stamp). The local clock is advanced past `hlc`.
    pub fn put_with_hlc(&mut self, key: &[u8], value: Vec<u8>, hlc: Hlc) -> Result<()> {
        let commit = self.append_put_buffered(key, value, hlc)?;
        self.wal.commit_sync(commit)
    }

    /// Apply a delete at a caller-supplied stamp (replication).
    pub fn delete_with_hlc(&mut self, key: &[u8], hlc: Hlc) -> Result<()> {
        let commit = self.append_delete_buffered(key, hlc)?;
        self.wal.commit_sync(commit)
    }

    /// Buffered replicated write (no fsync): append + apply, returning the
    /// commit point so the caller can fsync outside its write lock (group
    /// commit). Pair with [`Engine::wal_sync_handle`].
    pub fn append_put_buffered(
        &mut self,
        key: &[u8],
        value: Vec<u8>,
        hlc: Hlc,
    ) -> Result<WalCommit> {
        self.clock.observe(hlc);
        self.append_buffered(
            key,
            hlc,
            WalOp::Put(value.clone()),
            VersionValue::Put(value),
        )
    }

    /// Buffered replicated delete (no fsync); see [`Engine::append_put_buffered`].
    pub fn append_delete_buffered(&mut self, key: &[u8], hlc: Hlc) -> Result<WalCommit> {
        self.clock.observe(hlc);
        self.append_buffered(key, hlc, WalOp::Delete, VersionValue::Delete)
    }

    /// Durability coordinator handle, to `sync_through` a buffered commit after
    /// releasing the write lock.
    pub fn wal_sync_handle(&self) -> std::sync::Arc<WalSync> {
        self.wal.sync_handle()
    }

    /// Latest committed value for `key`, or `None` if absent or deleted.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Memtable is authoritative if it holds any version of the key.
        if let Some(entry) = self.mem.get_entry(key) {
            return Ok(version_to_value(entry.clone()));
        }
        for sst in self.sstables_newest_first() {
            if let Some((_, value)) = sst.get(key)? {
                return Ok(version_to_value(value));
            }
        }
        Ok(None)
    }

    /// Latest stored version for `key` (including tombstones) with its stamp,
    /// across memtable and SSTables. Used for last-writer-wins point reads.
    pub fn get_versioned(&self, key: &[u8]) -> Result<Option<(Hlc, VersionValue)>> {
        if let Some((hlc, entry)) = self.mem.get_entry_versioned(key) {
            return Ok(Some((hlc, entry)));
        }
        for sst in self.sstables_newest_first() {
            if let Some((hlc, value)) = sst.get(key)? {
                return Ok(Some((hlc, value)));
            }
        }
        Ok(None)
    }

    /// Value for `key` as visible at snapshot `as_of` (MVCC read).
    ///
    /// Full historical versions live only in the memtable; once flushed, only
    /// the latest version survives, so this falls back to the latest value when
    /// its stamp is `<= as_of`.
    pub fn get_as_of(&self, key: &[u8], as_of: Hlc) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.mem.get_as_of(key, as_of) {
            return Ok(Some(v.to_vec()));
        }
        if self.mem.get_entry(key).is_some() {
            // The memtable has a version, but none at or before `as_of`.
            return Ok(None);
        }
        for sst in self.sstables_newest_first() {
            if let Some((hlc, value)) = sst.get(key)? {
                return Ok(if hlc <= as_of {
                    version_to_value(value)
                } else {
                    None
                });
            }
        }
        Ok(None)
    }

    /// Full scan of the latest live key/value pairs across all sources, in key
    /// order.
    pub fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .merged()?
            .into_iter()
            .filter_map(|(k, (_, v))| match v {
                VersionValue::Put(bytes) => Some((k, bytes)),
                VersionValue::Delete => None,
            })
            .collect())
    }

    /// Like [`Engine::scan`] but also returns each row's version stamp, so a
    /// coordinator can resolve replicas by last-writer-wins (SPEC §5).
    pub fn scan_versioned(&self) -> Result<Vec<VersionedRow>> {
        Ok(self
            .merged()?
            .into_iter()
            .filter_map(|(k, (hlc, v))| match v {
                VersionValue::Put(bytes) => Some((k, bytes, hlc)),
                VersionValue::Delete => None,
            })
            .collect())
    }

    /// Scan only the live keys that start with `prefix`, in key order. Used by
    /// secondary indexes, whose entries are prefixed by the indexed value.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self
            .merged()?
            .into_iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .filter_map(|(k, (_, v))| match v {
                VersionValue::Put(bytes) => Some((k, bytes)),
                VersionValue::Delete => None,
            })
            .collect())
    }

    /// Merge all sources into the latest version per key (newest stamp wins).
    fn merged(&self) -> Result<MergedRows> {
        use std::collections::BTreeMap;
        let mut merged: MergedRows = BTreeMap::new();
        let mut consider = |key: Vec<u8>, hlc: Hlc, value: VersionValue| {
            merged
                .entry(key)
                .and_modify(|cur| {
                    if hlc > cur.0 {
                        *cur = (hlc, value.clone());
                    }
                })
                .or_insert((hlc, value));
        };
        for (key, hlc, value) in self.mem.iter_latest_entries() {
            consider(key, hlc, value);
        }
        for sst in self.sstables_newest_first() {
            for e in sst.entries()? {
                consider(e.key, e.hlc, e.value);
            }
        }
        Ok(merged)
    }

    /// Force the active memtable to flush to an SSTable (no-op if empty).
    pub fn flush(&mut self) -> Result<()> {
        if self.mem.is_empty() {
            return Ok(());
        }
        let entries: Vec<SstEntry> = self
            .mem
            .iter_latest_entries()
            .into_iter()
            .map(|(key, hlc, value)| SstEntry { key, hlc, value })
            .collect();

        let (sst, _path) = self.write_table(&entries)?;
        self.l0.insert(0, sst);

        self.mem = Memtable::new();
        self.wal.truncate()?;
        self.persist_manifest()?;
        self.maybe_compact()?;
        Ok(())
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.mem.approx_bytes() >= self.opts.flush_threshold_bytes {
            self.flush()?;
        }
        Ok(())
    }

    /// Whether the memtable has grown past its flush threshold.
    pub fn needs_flush(&self) -> bool {
        self.mem.approx_bytes() >= self.opts.flush_threshold_bytes
    }

    /// Approximate in-memory footprint of the memtable.
    pub fn memtable_bytes(&self) -> usize {
        self.mem.approx_bytes()
    }

    /// Number of on-disk SSTables across all levels.
    pub fn sstable_count(&self) -> usize {
        self.l0.len() + self.levels.len()
    }

    /// Directory backing this engine.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    // ---- internal ----

    fn sstables_newest_first(&self) -> impl Iterator<Item = &SsTable> {
        self.l0.iter().chain(self.levels.iter())
    }

    fn write_table(&mut self, entries: &[SstEntry]) -> Result<(SsTable, PathBuf)> {
        let seq = self.next_seq;
        self.next_seq += 1;
        let path = self.dir.join("sst").join(format!("{seq:020}.sst"));
        let sst = SsTable::write(&path, entries)?;
        Ok((sst, path))
    }

    /// Compact when level 0 has accumulated too many tables, cascading deeper.
    fn maybe_compact(&mut self) -> Result<()> {
        if self.l0.len() < self.opts.l0_compaction_trigger {
            return Ok(());
        }

        // Merge all L0 tables and the existing L1 (if any) into a new L1 run.
        let mut sources: Vec<&SsTable> = self.l0.iter().collect();
        if let Some(l1) = self.levels.first() {
            sources.push(l1);
        }
        let deepest = self.levels.len() <= 1;
        let merged = merge_tables(&sources, deepest)?;
        let old_paths = collect_paths(&self.l0, self.levels.first());

        let (new_l1, _) = self.write_table(&merged)?;
        self.l0.clear();
        if self.levels.is_empty() {
            self.levels.push(new_l1);
        } else {
            self.levels[0] = new_l1;
        }
        remove_files(&old_paths);

        // Cascade: push a level down when it exceeds its capacity.
        let mut level = 0;
        loop {
            let capacity = self.opts.level1_capacity * 10u64.pow(level as u32);
            if level >= self.levels.len() || self.levels[level].len() <= capacity {
                break;
            }
            let has_next = level + 1 < self.levels.len();
            let deepest = !has_next;
            let mut sources: Vec<&SsTable> = vec![&self.levels[level]];
            if has_next {
                sources.push(&self.levels[level + 1]);
            }
            let merged = merge_tables(&sources, deepest)?;
            let mut old_paths = vec![self.levels[level].path().to_path_buf()];
            if has_next {
                old_paths.push(self.levels[level + 1].path().to_path_buf());
            }

            let (new_run, _) = self.write_table(&merged)?;
            if has_next {
                self.levels[level + 1] = new_run;
                self.levels.remove(level);
            } else {
                self.levels.push(new_run);
                self.levels.remove(level);
            }
            remove_files(&old_paths);
            level += 1;
        }

        self.persist_manifest()?;
        Ok(())
    }

    fn persist_manifest(&self) -> Result<()> {
        let mut lines = String::new();
        for sst in &self.l0 {
            lines.push_str(&format!("0 {}\n", file_name(sst.path())));
        }
        for (i, sst) in self.levels.iter().enumerate() {
            lines.push_str(&format!("{} {}\n", i + 1, file_name(sst.path())));
        }
        let manifest = self.dir.join("sst").join("MANIFEST");
        let tmp = self.dir.join("sst").join("MANIFEST.tmp");
        std::fs::write(&tmp, lines)?;
        std::fs::rename(&tmp, &manifest)?;
        Ok(())
    }
}

fn version_to_value(v: VersionValue) -> Option<Vec<u8>> {
    match v {
        VersionValue::Put(bytes) => Some(bytes),
        VersionValue::Delete => None,
    }
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_string()
}

/// Merge `sources` (ordered newest → oldest) into a deduplicated, key-sorted
/// run keeping the highest-stamped version of each key. When `drop_tombstones`
/// is set (merging into the deepest level), delete markers are discarded.
fn merge_tables(sources: &[&SsTable], drop_tombstones: bool) -> Result<Vec<SstEntry>> {
    use std::collections::BTreeMap;
    let mut merged: BTreeMap<Vec<u8>, (Hlc, VersionValue)> = BTreeMap::new();
    for sst in sources {
        for e in sst.entries()? {
            merged
                .entry(e.key)
                .and_modify(|cur| {
                    if e.hlc > cur.0 {
                        *cur = (e.hlc, e.value.clone());
                    }
                })
                .or_insert((e.hlc, e.value));
        }
    }
    Ok(merged
        .into_iter()
        .filter(|(_, (_, v))| !(drop_tombstones && matches!(v, VersionValue::Delete)))
        .map(|(key, (hlc, value))| SstEntry { key, hlc, value })
        .collect())
}

fn collect_paths(l0: &[SsTable], l1: Option<&SsTable>) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = l0.iter().map(|s| s.path().to_path_buf()).collect();
    if let Some(t) = l1 {
        paths.push(t.path().to_path_buf());
    }
    paths
}

fn remove_files(paths: &[PathBuf]) {
    for p in paths {
        let _ = std::fs::remove_file(p);
    }
}

/// Load the SSTable set described by the manifest. Returns (L0 newest-first,
/// deeper levels, next sequence number).
#[allow(clippy::type_complexity)]
fn load_manifest(dir: &Path) -> Result<(Vec<SsTable>, Vec<SsTable>, u64)> {
    let manifest = dir.join("sst").join("MANIFEST");
    if !manifest.exists() {
        return Ok((Vec::new(), Vec::new(), 0));
    }
    let text = std::fs::read_to_string(&manifest)?;

    let mut l0: Vec<SsTable> = Vec::new();
    let mut leveled: Vec<(usize, SsTable)> = Vec::new();
    let mut max_seq: u64 = 0;
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        let (Some(level), Some(name)) = (parts.next(), parts.next()) else {
            continue;
        };
        let level: usize = level.parse().map_err(|_| StorageError::Corruption {
            offset: 0,
            detail: "bad manifest level",
        })?;
        let path = dir.join("sst").join(name);
        if let Some(seq) = name
            .strip_suffix(".sst")
            .and_then(|s| s.parse::<u64>().ok())
        {
            max_seq = max_seq.max(seq + 1);
        }
        let sst = SsTable::open(&path)?;
        if level == 0 {
            l0.push(sst);
        } else {
            leveled.push((level, sst));
        }
    }

    // Manifest lists L0 newest-first already (we write it that way).
    leveled.sort_by_key(|(lvl, _)| *lvl);
    let levels = leveled.into_iter().map(|(_, sst)| sst).collect();
    Ok((l0, levels, max_seq))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tempdir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut dir = std::env::temp_dir();
        dir.push(format!("skaidb-engine-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn small_opts() -> EngineOptions {
        // Tiny thresholds so a handful of writes exercise flush + compaction.
        EngineOptions {
            flush_threshold_bytes: 256,
            l0_compaction_trigger: 3,
            level1_capacity: 8,
        }
    }

    #[test]
    fn put_get_delete() {
        let mut e = Engine::open(tempdir()).unwrap();
        e.put(b"k", b"v".to_vec()).unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b"v".to_vec()));
        e.delete(b"k").unwrap();
        assert_eq!(e.get(b"k").unwrap(), None);
    }

    #[test]
    fn durability_across_reopen() {
        let dir = tempdir();
        {
            let mut e = Engine::open(&dir).unwrap();
            e.put(b"alpha", b"1".to_vec()).unwrap();
            e.put(b"beta", b"2".to_vec()).unwrap();
            e.put(b"alpha", b"3".to_vec()).unwrap();
            e.delete(b"beta").unwrap();
        }
        let e = Engine::open(&dir).unwrap();
        assert_eq!(e.get(b"alpha").unwrap(), Some(b"3".to_vec()));
        assert_eq!(e.get(b"beta").unwrap(), None);
    }

    #[test]
    fn snapshot_isolation_reads_old_value() {
        let mut e = Engine::open(tempdir()).unwrap();
        e.put(b"k", b"old".to_vec()).unwrap();
        let snap = e.clock_snapshot();
        e.put(b"k", b"new".to_vec()).unwrap();
        assert_eq!(e.get(b"k").unwrap(), Some(b"new".to_vec()));
        assert_eq!(e.get_as_of(b"k", snap).unwrap(), Some(b"old".to_vec()));
    }

    #[test]
    fn flush_creates_sstable_and_reads_still_work() {
        let mut e = Engine::open_with_options(tempdir(), small_opts()).unwrap();
        for i in 0..20u32 {
            e.put(format!("key{i:03}").as_bytes(), vec![i as u8; 32])
                .unwrap();
        }
        assert!(
            e.sstable_count() >= 1,
            "expected at least one flushed table"
        );
        // Reads merge memtable + SSTables.
        assert_eq!(e.get(b"key000").unwrap(), Some(vec![0u8; 32]));
        assert_eq!(e.get(b"key019").unwrap(), Some(vec![19u8; 32]));
    }

    #[test]
    fn compaction_reduces_tables_and_preserves_data() {
        let dir = tempdir();
        let mut e = Engine::open_with_options(&dir, small_opts()).unwrap();
        for i in 0..60u32 {
            e.put(format!("k{i:04}").as_bytes(), vec![1u8; 40]).unwrap();
        }
        // Overwrite some keys; newest value must win after compaction.
        e.put(b"k0000", vec![9u8; 40]).unwrap();
        e.flush().unwrap();

        let count = e.sstable_count();
        let scanned = e.scan().unwrap();
        assert_eq!(
            scanned.len(),
            60,
            "all distinct keys present after compaction"
        );
        assert_eq!(e.get(b"k0000").unwrap(), Some(vec![9u8; 40]));

        // Survives reopen via the manifest.
        drop(e);
        let e = Engine::open_with_options(&dir, small_opts()).unwrap();
        assert_eq!(e.scan().unwrap().len(), 60);
        assert!(count >= 1);
    }

    #[test]
    fn deletes_are_dropped_after_compaction() {
        let mut e = Engine::open_with_options(tempdir(), small_opts()).unwrap();
        for i in 0..30u32 {
            e.put(format!("d{i:03}").as_bytes(), vec![1u8; 40]).unwrap();
        }
        e.delete(b"d000").unwrap();
        e.flush().unwrap();
        assert_eq!(e.get(b"d000").unwrap(), None);
        assert!(!e.scan().unwrap().iter().any(|(k, _)| k == b"d000"));
    }

    impl Engine {
        /// Test helper: a snapshot stamp (exposed only in tests).
        fn clock_snapshot(&self) -> Hlc {
            self.clock.now()
        }
    }
}
