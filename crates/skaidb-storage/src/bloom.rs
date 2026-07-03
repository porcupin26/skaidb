//! A classic Bloom filter for SSTable membership tests (SPEC §12).
//!
//! Each SSTable carries a filter so a point lookup can skip the file entirely
//! when a key is definitely absent. Per-level bit tuning (the Monkey result) is
//! a later refinement; this builds a filter sized for a target false-positive
//! rate from the key count.

/// An immutable Bloom filter built from a set of keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bloom {
    bits: Vec<u8>,
    /// Number of bits (== `bits.len() * 8`).
    m: u64,
    /// Number of hash probes.
    k: u32,
}

impl Bloom {
    /// An empty filter sized for `expected_keys` insertions at `fp_rate`. Keys
    /// are added incrementally with [`Bloom::add`]; inserting fewer keys than
    /// expected only lowers the false-positive rate.
    pub fn with_capacity(expected_keys: usize, fp_rate: f64) -> Bloom {
        let n = expected_keys.max(1) as f64;
        let ln2 = std::f64::consts::LN_2;
        let m = (-(n * fp_rate.ln()) / (ln2 * ln2)).ceil().max(8.0) as u64;
        let m = m.div_ceil(8) * 8; // round up to whole bytes
        let k = (((m as f64 / n) * ln2).round() as u32).clamp(1, 30);

        Bloom {
            bits: vec![0u8; (m / 8) as usize],
            m,
            k,
        }
    }

    /// Record `key` in the filter.
    pub fn add(&mut self, key: &[u8]) {
        let (h1, h2) = hashes(key);
        for i in 0..self.k {
            let bit = combined(h1, h2, i) % self.m;
            self.bits[(bit / 8) as usize] |= 1 << (bit % 8);
        }
    }

    /// Whether `key` *might* be present. `false` is definitive; `true` may be a
    /// false positive.
    pub fn contains(&self, key: &[u8]) -> bool {
        let (h1, h2) = hashes(key);
        for i in 0..self.k {
            let bit = combined(h1, h2, i) % self.m;
            if self.bits[(bit / 8) as usize] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize as `m(u64) | k(u32) | bits`.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(12 + self.bits.len());
        out.extend_from_slice(&self.m.to_le_bytes());
        out.extend_from_slice(&self.k.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    /// Deserialize a filter produced by [`Bloom::encode`].
    pub fn decode(bytes: &[u8]) -> Option<Bloom> {
        if bytes.len() < 12 {
            return None;
        }
        let mut m = [0u8; 8];
        m.copy_from_slice(&bytes[..8]);
        let m = u64::from_le_bytes(m);
        let mut k = [0u8; 4];
        k.copy_from_slice(&bytes[8..12]);
        let k = u32::from_le_bytes(k);
        let bits = bytes[12..].to_vec();
        if bits.len() as u64 * 8 != m {
            return None;
        }
        Some(Bloom { bits, m, k })
    }
}

/// Two independent 64-bit hashes via FNV-1a with differing seeds.
fn hashes(key: &[u8]) -> (u64, u64) {
    (
        fnv1a(key, 0xcbf2_9ce4_8422_2325),
        fnv1a(key, 0x1000_0000_0000_01b3),
    )
}

fn fnv1a(key: &[u8], seed: u64) -> u64 {
    let mut h = seed;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// `i`-th probe via double hashing.
fn combined(h1: u64, h2: u64, i: u32) -> u64 {
    h1.wrapping_add((i as u64).wrapping_mul(h2))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(n: usize) -> Vec<Vec<u8>> {
        (0..n).map(|i| format!("key-{i}").into_bytes()).collect()
    }

    /// Build a filter over `keys` targeting roughly `fp_rate` false positives.
    fn build(keys: &[Vec<u8>], fp_rate: f64) -> Bloom {
        let mut bloom = Bloom::with_capacity(keys.len(), fp_rate);
        for key in keys {
            bloom.add(key);
        }
        bloom
    }

    #[test]
    fn no_false_negatives() {
        let ks = keys(1000);
        let bloom = build(&ks, 0.01);
        for k in &ks {
            assert!(bloom.contains(k), "must contain inserted key");
        }
    }

    #[test]
    fn false_positive_rate_is_reasonable() {
        let ks = keys(1000);
        let bloom = build(&ks, 0.01);
        let mut fp = 0;
        let trials = 10_000;
        for i in 0..trials {
            let probe = format!("absent-{i}").into_bytes();
            if bloom.contains(&probe) {
                fp += 1;
            }
        }
        let rate = fp as f64 / trials as f64;
        assert!(rate < 0.05, "fp rate {rate} too high");
    }

    #[test]
    fn encode_decode_roundtrip() {
        let bloom = build(&keys(100), 0.01);
        let decoded = Bloom::decode(&bloom.encode()).unwrap();
        assert_eq!(bloom, decoded);
    }
}
