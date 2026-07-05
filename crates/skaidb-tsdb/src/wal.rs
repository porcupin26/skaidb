//! Sample write-ahead log: sequential segments of CRC-framed records.
//!
//! Two record kinds: a **series** record (id → labels), written the first
//! time a series is seen, and a **samples** record (a batch of `(id, ts,
//! value)`). On flush the store writes a *checkpoint*: a fresh segment
//! re-recording every live series and the still-unflushed samples, after
//! which all older segments are deleted — so WAL size tracks the unflushed
//! window, not history.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::varenc::{put_bytes, put_uvarint, put_varint, Dec};
use crate::{Labels, Result, TsdbError};

const REC_SERIES: u8 = 1;
const REC_SAMPLES: u8 = 2;
/// Roll to a new segment past this size.
const SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// A decoded WAL record.
#[derive(Debug, PartialEq)]
pub enum Record {
    Series { id: u64, labels: Labels },
    Samples(Vec<(u64, i64, f64)>),
}

impl Record {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Record::Series { id, labels } => {
                buf.push(REC_SERIES);
                put_uvarint(&mut buf, *id);
                put_uvarint(&mut buf, labels.len() as u64);
                for (k, v) in labels {
                    put_bytes(&mut buf, k.as_bytes());
                    put_bytes(&mut buf, v.as_bytes());
                }
            }
            Record::Samples(samples) => {
                buf.push(REC_SAMPLES);
                put_uvarint(&mut buf, samples.len() as u64);
                for (id, ts, value) in samples {
                    put_uvarint(&mut buf, *id);
                    put_varint(&mut buf, *ts);
                    buf.extend_from_slice(&value.to_bits().to_le_bytes());
                }
            }
        }
        buf
    }

    pub fn decode(payload: &[u8]) -> Result<Record> {
        let mut d = Dec::new(payload);
        match d.u8()? {
            REC_SERIES => {
                let id = d.uvarint()?;
                let n = d.uvarint()? as usize;
                let mut labels = Vec::with_capacity(n);
                for _ in 0..n {
                    let k = d.string()?;
                    let v = d.string()?;
                    labels.push((k, v));
                }
                Ok(Record::Series { id, labels })
            }
            REC_SAMPLES => {
                let n = d.uvarint()? as usize;
                let mut samples = Vec::with_capacity(n);
                for _ in 0..n {
                    let id = d.uvarint()?;
                    let ts = d.varint()?;
                    let value = f64::from_bits(d.u64_le()?);
                    samples.push((id, ts, value));
                }
                Ok(Record::Samples(samples))
            }
            other => Err(TsdbError::Corrupt(format!("unknown wal record {other}"))),
        }
    }
}

/// The active WAL writer.
#[derive(Debug)]
pub struct Wal {
    dir: PathBuf,
    file: BufWriter<File>,
    seq: u64,
    seg_bytes: u64,
}

fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:08}.wal"))
}

fn segment_seqs(dir: &Path) -> Result<Vec<u64>> {
    let mut seqs = Vec::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if let Some(stem) = name.strip_suffix(".wal") {
            if let Ok(seq) = stem.parse::<u64>() {
                seqs.push(seq);
            }
        }
    }
    seqs.sort_unstable();
    Ok(seqs)
}

impl Wal {
    /// Open the WAL for appending, continuing after the highest existing
    /// segment. (Replay existing segments first via [`Wal::replay`].)
    pub fn open(dir: &Path) -> Result<Wal> {
        fs::create_dir_all(dir)?;
        let next = segment_seqs(dir)?.last().map_or(1, |s| s + 1);
        Ok(Wal {
            dir: dir.to_path_buf(),
            file: BufWriter::new(Self::create_segment(dir, next)?),
            seq: next,
            seg_bytes: 0,
        })
    }

    fn create_segment(dir: &Path, seq: u64) -> Result<File> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(segment_path(dir, seq))?;
        // Make the new segment durable in the directory.
        File::open(dir)?.sync_all()?;
        Ok(file)
    }

    pub fn append(&mut self, record: &Record) -> Result<()> {
        let payload = record.encode();
        self.file
            .write_all(&(payload.len() as u32).to_le_bytes())?;
        self.file.write_all(&crc32(&payload).to_le_bytes())?;
        self.file.write_all(&payload)?;
        self.seg_bytes += 8 + payload.len() as u64;
        if self.seg_bytes >= SEGMENT_MAX_BYTES {
            self.roll()?;
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        Ok(())
    }

    fn roll(&mut self) -> Result<()> {
        self.sync()?;
        self.seq += 1;
        self.file = BufWriter::new(Self::create_segment(&self.dir, self.seq)?);
        self.seg_bytes = 0;
        Ok(())
    }

    /// Start a checkpoint segment; records appended after this land in it.
    /// Returns the new segment's sequence for [`Wal::truncate_before`].
    pub fn begin_checkpoint(&mut self) -> Result<u64> {
        self.roll()?;
        Ok(self.seq)
    }

    /// Delete every segment older than `seq` (call after the checkpoint's
    /// contents are synced).
    pub fn truncate_before(&mut self, seq: u64) -> Result<()> {
        for old in segment_seqs(&self.dir)? {
            if old < seq {
                fs::remove_file(segment_path(&self.dir, old))?;
            }
        }
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }

    /// Replay all segments in order. A torn/corrupt record ends replay (it
    /// is the crash tail); everything before it is delivered to `apply`.
    /// Returns the number of records applied.
    pub fn replay(dir: &Path, mut apply: impl FnMut(Record)) -> Result<usize> {
        fs::create_dir_all(dir)?;
        let mut applied = 0usize;
        'segments: for seq in segment_seqs(dir)? {
            let mut data = Vec::new();
            File::open(segment_path(dir, seq))?.read_to_end(&mut data)?;
            let mut pos = 0usize;
            while pos + 8 <= data.len() {
                let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                let crc = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
                let start = pos + 8;
                let end = match start.checked_add(len) {
                    Some(end) if end <= data.len() => end,
                    _ => break 'segments, // torn tail
                };
                let payload = &data[start..end];
                if crc32(payload) != crc {
                    break 'segments;
                }
                match Record::decode(payload) {
                    Ok(rec) => apply(rec),
                    Err(_) => break 'segments,
                }
                applied += 1;
                pos = end;
            }
        }
        Ok(applied)
    }
}

/// CRC-32 (IEEE 802.3), table-driven.
pub fn crc32(data: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut table = [0u32; 256];
        for (i, slot) in table.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 == 1 { 0xEDB88320 ^ (c >> 1) } else { c >> 1 };
            }
            *slot = c;
        }
        table
    });
    let mut crc = 0xFFFF_FFFFu32;
    for &b in data {
        crc = table[((crc ^ b as u32) & 0xff) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tsdb-wal-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn series(id: u64) -> Record {
        Record::Series {
            id,
            labels: vec![("host".into(), format!("h{id}"))],
        }
    }

    #[test]
    fn append_replay_roundtrip() {
        let dir = temp_dir("rt");
        let mut wal = Wal::open(&dir).unwrap();
        wal.append(&series(0)).unwrap();
        wal.append(&Record::Samples(vec![(0, 1000, 1.5), (0, 2000, f64::NAN)]))
            .unwrap();
        wal.sync().unwrap();

        let mut got = Vec::new();
        let n = Wal::replay(&dir, |r| got.push(r)).unwrap();
        assert_eq!(n, 2);
        assert_eq!(got[0], series(0));
        match &got[1] {
            Record::Samples(s) => {
                assert_eq!(s[0], (0, 1000, 1.5));
                assert_eq!(s[1].1, 2000);
                assert!(s[1].2.is_nan());
            }
            other => panic!("unexpected {other:?}"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn torn_tail_is_tolerated() {
        let dir = temp_dir("torn");
        let mut wal = Wal::open(&dir).unwrap();
        wal.append(&series(0)).unwrap();
        wal.append(&series(1)).unwrap();
        wal.sync().unwrap();
        // Corrupt the last few bytes (simulated torn write).
        let seg = segment_path(&dir, 1);
        let mut data = fs::read(&seg).unwrap();
        let n = data.len();
        data[n - 2] ^= 0xff;
        fs::write(&seg, &data).unwrap();

        let mut got = Vec::new();
        let n = Wal::replay(&dir, |r| got.push(r)).unwrap();
        assert_eq!(n, 1); // first record intact, second dropped
        assert_eq!(got[0], series(0));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn checkpoint_truncates_history() {
        let dir = temp_dir("ckpt");
        let mut wal = Wal::open(&dir).unwrap();
        wal.append(&series(0)).unwrap();
        wal.sync().unwrap();
        let keep = wal.begin_checkpoint().unwrap();
        wal.append(&series(1)).unwrap();
        wal.sync().unwrap();
        wal.truncate_before(keep).unwrap();

        let mut got = Vec::new();
        Wal::replay(&dir, |r| got.push(r)).unwrap();
        assert_eq!(got, vec![series(1)]);
        let _ = fs::remove_dir_all(&dir);
    }
}
