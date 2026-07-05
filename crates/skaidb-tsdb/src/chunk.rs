//! Gorilla chunk codec: delta-of-delta timestamps + XOR-compressed floats
//! (Facebook's Gorilla paper, with the Prometheus timestamp bucket sizes).
//!
//! A chunk holds one series' samples for one block window, time-ordered.
//! Regular scrape intervals encode to ~1 bit per timestamp and slowly-moving
//! values to a few bits each — the ~1–2 bytes/sample that makes a TSDB a
//! TSDB.

use crate::bitstream::{BitReader, BitWriter};
use crate::{Result, TsdbError};

/// One observation: millisecond timestamp and value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    pub ts: i64,
    pub value: f64,
}

/// Delta-of-delta bucket boundaries (Prometheus `xor.go`): most scrapes are
/// perfectly regular (dod = 0, one bit); jitter fits 14 bits.
const DOD_BITS: [(u8, u8, i64); 3] = [
    // (prefix bits value, payload bits, max magnitude)
    (0b10, 14, 8191),
    (0b110, 17, 65535),
    (0b1110, 20, 524_287),
];

/// Incremental encoder for one open chunk. Sealed by [`ChunkBuilder::seal`];
/// never reopened (appends past a seal start a new chunk).
#[derive(Debug, Clone)]
pub struct ChunkBuilder {
    w: BitWriter,
    count: u16,
    first_ts: i64,
    last_ts: i64,
    last_delta: i64,
    last_bits: u64,
    leading: u8,
    trailing: u8,
}

impl ChunkBuilder {
    pub fn new() -> ChunkBuilder {
        ChunkBuilder {
            w: BitWriter::new(),
            count: 0,
            first_ts: 0,
            last_ts: 0,
            last_delta: 0,
            last_bits: 0,
            leading: 0xff, // sentinel: no window yet
            trailing: 0,
        }
    }

    pub fn count(&self) -> u16 {
        self.count
    }

    pub fn first_ts(&self) -> i64 {
        self.first_ts
    }

    pub fn last_ts(&self) -> i64 {
        self.last_ts
    }

    /// Append a sample. Timestamps must be strictly increasing.
    pub fn append(&mut self, ts: i64, value: f64) -> Result<()> {
        if self.count > 0 && ts <= self.last_ts {
            return Err(TsdbError::OutOfOrder {
                ts,
                last: self.last_ts,
            });
        }
        let bits = value.to_bits();
        match self.count {
            0 => {
                self.w.write_varint(ts);
                self.w.write_bits(bits, 64);
                self.first_ts = ts;
            }
            1 => {
                let delta = ts - self.last_ts;
                self.w.write_uvarint(delta as u64);
                self.last_delta = delta;
                self.write_xor(bits);
            }
            _ => {
                let delta = ts - self.last_ts;
                let dod = delta - self.last_delta;
                self.write_dod(dod);
                self.last_delta = delta;
                self.write_xor(bits);
            }
        }
        self.last_ts = ts;
        self.last_bits = bits;
        self.count += 1;
        Ok(())
    }

    fn write_dod(&mut self, dod: i64) {
        if dod == 0 {
            self.w.write_bit(false);
            return;
        }
        for &(prefix, bits, max) in &DOD_BITS {
            if dod >= -max && dod <= max + 1 {
                let nprefix = match prefix {
                    0b10 => 2,
                    0b110 => 3,
                    _ => 4,
                };
                self.w.write_bits(prefix as u64, nprefix);
                // Store biased so the payload is unsigned.
                self.w.write_bits((dod + max) as u64, bits);
                return;
            }
        }
        self.w.write_bits(0b1111, 4);
        self.w.write_bits(dod as u64, 64);
    }

    fn write_xor(&mut self, bits: u64) {
        let xor = bits ^ self.last_bits;
        if xor == 0 {
            self.w.write_bit(false);
            return;
        }
        self.w.write_bit(true);
        let mut leading = xor.leading_zeros() as u8;
        let trailing = xor.trailing_zeros() as u8;
        // Cap leading at 31 so it fits 5 bits.
        if leading > 31 {
            leading = 31;
        }
        if self.leading != 0xff && leading >= self.leading && trailing >= self.trailing {
            // Reuse the previous meaningful-bit window.
            self.w.write_bit(false);
            let sig = 64 - self.leading - self.trailing;
            self.w.write_bits(xor >> self.trailing, sig);
        } else {
            self.w.write_bit(true);
            let sig = 64 - leading - trailing;
            self.w.write_bits(leading as u64, 5);
            // sig is 1..=64; 64 is stored as 0 (6 bits).
            self.w.write_bits((sig & 0x3f) as u64, 6);
            self.w.write_bits(xor >> trailing, sig);
            self.leading = leading;
            self.trailing = trailing;
        }
    }

    /// Finish the chunk: `[u16 LE count][bitstream]`.
    pub fn seal(self) -> Vec<u8> {
        let stream = self.w.into_bytes();
        let mut out = Vec::with_capacity(2 + stream.len());
        out.extend_from_slice(&self.count.to_le_bytes());
        out.extend_from_slice(&stream);
        out
    }

    /// Decode the samples appended so far without sealing (used for WAL
    /// checkpointing of the open window).
    pub fn snapshot(&self) -> Result<Vec<Sample>> {
        decode_stream(self.w.as_bytes(), self.count)
    }
}

impl Default for ChunkBuilder {
    fn default() -> Self {
        ChunkBuilder::new()
    }
}

/// Decode a sealed chunk produced by [`ChunkBuilder::seal`].
pub fn decode(data: &[u8]) -> Result<Vec<Sample>> {
    if data.len() < 2 {
        return Err(TsdbError::Corrupt("chunk shorter than header".into()));
    }
    let count = u16::from_le_bytes([data[0], data[1]]);
    decode_stream(&data[2..], count)
}

fn decode_stream(stream: &[u8], count: u16) -> Result<Vec<Sample>> {
    let mut r = BitReader::new(stream);
    let mut out = Vec::with_capacity(count as usize);
    let corrupt = || TsdbError::Corrupt("chunk bitstream truncated".into());

    let mut ts = 0i64;
    let mut delta = 0i64;
    let mut bits = 0u64;
    let mut leading = 0u8;
    let mut trailing = 0u8;

    for i in 0..count {
        match i {
            0 => {
                ts = r.read_varint().ok_or_else(corrupt)?;
                bits = r.read_bits(64).ok_or_else(corrupt)?;
            }
            1 => {
                delta = r.read_uvarint().ok_or_else(corrupt)? as i64;
                ts += delta;
                bits = read_xor(&mut r, bits, &mut leading, &mut trailing).ok_or_else(corrupt)?;
            }
            _ => {
                let dod = read_dod(&mut r).ok_or_else(corrupt)?;
                delta += dod;
                ts += delta;
                bits = read_xor(&mut r, bits, &mut leading, &mut trailing).ok_or_else(corrupt)?;
            }
        }
        out.push(Sample {
            ts,
            value: f64::from_bits(bits),
        });
    }
    Ok(out)
}

fn read_dod(r: &mut BitReader) -> Option<i64> {
    if !r.read_bit()? {
        return Some(0);
    }
    for &(_, bits, max) in &DOD_BITS {
        if !r.read_bit()? {
            return Some(r.read_bits(bits)? as i64 - max);
        }
    }
    Some(r.read_bits(64)? as i64)
}

fn read_xor(r: &mut BitReader, prev: u64, leading: &mut u8, trailing: &mut u8) -> Option<u64> {
    if !r.read_bit()? {
        return Some(prev);
    }
    if r.read_bit()? {
        let lead = r.read_bits(5)? as u8;
        let mut sig = r.read_bits(6)? as u8;
        if sig == 0 {
            sig = 64;
        }
        // Corrupt streams could otherwise underflow the trailing width.
        if lead + sig > 64 {
            return None;
        }
        *leading = lead;
        *trailing = 64 - lead - sig;
    }
    let sig = 64 - *leading - *trailing;
    let xor = r.read_bits(sig)? << *trailing;
    Some(prev ^ xor)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bit-exact comparison (`PartialEq` on f64 fails NaN == NaN).
    fn assert_samples_eq(a: &[Sample], b: &[Sample]) {
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b) {
            assert_eq!(x.ts, y.ts);
            assert_eq!(
                x.value.to_bits(),
                y.value.to_bits(),
                "{} != {}",
                x.value,
                y.value
            );
        }
    }

    fn roundtrip(samples: &[Sample]) {
        let mut b = ChunkBuilder::new();
        for s in samples {
            b.append(s.ts, s.value).unwrap();
        }
        // Snapshot (open) and seal (closed) must both reproduce the input.
        let snap = b.snapshot().unwrap();
        let sealed = b.seal();
        let dec = decode(&sealed).unwrap();
        assert_samples_eq(samples, &dec);
        assert_samples_eq(&snap, &dec);
    }

    #[test]
    fn regular_counter() {
        let samples: Vec<Sample> = (0..120)
            .map(|i| Sample {
                ts: 1_700_000_000_000 + i * 15_000,
                value: (i * 37) as f64,
            })
            .collect();
        roundtrip(&samples);
    }

    #[test]
    fn jittered_gauge() {
        // Deterministic LCG jitter, gauge with small movements.
        let mut state = 0x2545F4914F6CDD1Du64;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            state >> 33
        };
        let mut ts = 1_700_000_000_000i64;
        let mut v = 250.0f64;
        let samples: Vec<Sample> = (0..500)
            .map(|_| {
                ts += 15_000 + (next() % 200) as i64 - 100;
                v += (next() % 100) as f64 / 50.0 - 1.0;
                Sample { ts, value: v }
            })
            .collect();
        roundtrip(&samples);
    }

    #[test]
    fn edge_values() {
        let vals = [
            0.0,
            -0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN,
            f64::MAX,
            f64::MIN_POSITIVE,
            f64::EPSILON,
            1.0,
            -1.0,
            std::f64::consts::PI,
        ];
        let samples: Vec<Sample> = vals
            .iter()
            .enumerate()
            .map(|(i, &value)| Sample {
                ts: 1000 + i as i64,
                value,
            })
            .collect();
        roundtrip(&samples);
    }

    #[test]
    fn extreme_time_gaps() {
        // dods spanning every bucket, including the 64-bit fallback.
        let ts = [
            0i64,
            1,
            2,                 // dod 0
            8000,              // 14-bit
            80_000,            // 17-bit
            800_000,           // 20-bit
            9_000_000_000,     // 64-bit
            9_000_000_001,
        ];
        let samples: Vec<Sample> = ts
            .iter()
            .enumerate()
            .map(|(i, &ts)| Sample {
                ts,
                value: i as f64 * 1e10,
            })
            .collect();
        roundtrip(&samples);
    }

    #[test]
    fn single_and_two_samples() {
        roundtrip(&[Sample { ts: 5, value: 1.5 }]);
        roundtrip(&[
            Sample { ts: 5, value: 1.5 },
            Sample { ts: 20, value: 1.5 },
        ]);
    }

    #[test]
    fn rejects_out_of_order() {
        let mut b = ChunkBuilder::new();
        b.append(100, 1.0).unwrap();
        assert!(matches!(
            b.append(100, 2.0),
            Err(TsdbError::OutOfOrder { .. })
        ));
        assert!(matches!(
            b.append(99, 2.0),
            Err(TsdbError::OutOfOrder { .. })
        ));
    }

    #[test]
    fn random_series_match_naive_store() {
        // Property-style: many random series, decoded output must equal input.
        let mut state = 12345u64;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            state >> 33
        };
        for _ in 0..200 {
            let n = 1 + (next() % 300) as usize;
            let mut ts = next() as i64;
            let mut samples = Vec::with_capacity(n);
            for _ in 0..n {
                ts += 1 + (next() % 100_000) as i64;
                let value = f64::from_bits(next() << 32 | next());
                samples.push(Sample { ts, value });
            }
            roundtrip(&samples);
        }
    }

    #[test]
    fn compression_ratio_on_regular_data() {
        // The reason this codec exists: a regular 15s scrape of a stable
        // gauge (values mostly repeat) must land near 1 bit/sample for ts
        // and value alike.
        let mut b = ChunkBuilder::new();
        for i in 0..120i64 {
            b.append(1_700_000_000_000 + i * 15_000, 42.0 + (i / 40) as f64)
                .unwrap();
        }
        let sealed = b.seal();
        let per_sample = sealed.len() as f64 / 120.0;
        assert!(per_sample < 1.0, "got {per_sample} bytes/sample");
    }
}
