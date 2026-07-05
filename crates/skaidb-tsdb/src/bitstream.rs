//! Bit-granular append/read buffers for the Gorilla chunk codec.
//!
//! Bits are written MSB-first within each byte, so the encoded stream is a
//! deterministic function of the write sequence and readable byte-by-byte.

/// Append-only bit writer over a growable byte buffer.
#[derive(Debug, Clone, Default)]
pub struct BitWriter {
    buf: Vec<u8>,
    /// Bits used in the last byte of `buf` (0 = byte boundary).
    used: u8,
}

impl BitWriter {
    pub fn new() -> BitWriter {
        BitWriter::default()
    }

    pub fn write_bit(&mut self, bit: bool) {
        if self.used == 0 {
            self.buf.push(0);
            self.used = 8;
        }
        if bit {
            let last = self.buf.len() - 1;
            self.buf[last] |= 1 << (self.used - 1);
        }
        self.used -= 1;
    }

    /// Write the low `n` bits of `v`, most-significant first (`n` ≤ 64).
    pub fn write_bits(&mut self, v: u64, n: u8) {
        for i in (0..n).rev() {
            self.write_bit((v >> i) & 1 == 1);
        }
    }

    /// Write an LEB128 unsigned varint (byte-aligned values inside the
    /// bitstream; the stream itself need not be byte-aligned here).
    pub fn write_uvarint(&mut self, mut v: u64) {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                self.write_bits(byte as u64, 8);
                return;
            }
            self.write_bits((byte | 0x80) as u64, 8);
        }
    }

    /// Zig-zag signed varint.
    pub fn write_varint(&mut self, v: i64) {
        self.write_uvarint(((v << 1) ^ (v >> 63)) as u64);
    }

    /// The bytes written so far (final partial byte zero-padded).
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }
}

/// Sequential bit reader over a byte slice.
#[derive(Debug)]
pub struct BitReader<'a> {
    buf: &'a [u8],
    /// Absolute bit position.
    pos: usize,
}

impl<'a> BitReader<'a> {
    pub fn new(buf: &'a [u8]) -> BitReader<'a> {
        BitReader { buf, pos: 0 }
    }

    pub fn read_bit(&mut self) -> Option<bool> {
        let byte = self.buf.get(self.pos / 8)?;
        let bit = (byte >> (7 - (self.pos % 8))) & 1 == 1;
        self.pos += 1;
        Some(bit)
    }

    pub fn read_bits(&mut self, n: u8) -> Option<u64> {
        let mut v = 0u64;
        for _ in 0..n {
            v = (v << 1) | self.read_bit()? as u64;
        }
        Some(v)
    }

    pub fn read_uvarint(&mut self) -> Option<u64> {
        let mut v = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = self.read_bits(8)? as u8;
            v |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Some(v);
            }
            shift += 7;
            if shift >= 64 {
                return None;
            }
        }
    }

    pub fn read_varint(&mut self) -> Option<i64> {
        let u = self.read_uvarint()?;
        Some(((u >> 1) as i64) ^ -((u & 1) as i64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bits_roundtrip() {
        let mut w = BitWriter::new();
        w.write_bit(true);
        w.write_bits(0b1011, 4);
        w.write_bits(u64::MAX, 64);
        w.write_bit(false);
        w.write_bits(42, 7);
        let bytes = w.into_bytes();
        let mut r = BitReader::new(&bytes);
        assert_eq!(r.read_bit(), Some(true));
        assert_eq!(r.read_bits(4), Some(0b1011));
        assert_eq!(r.read_bits(64), Some(u64::MAX));
        assert_eq!(r.read_bit(), Some(false));
        assert_eq!(r.read_bits(7), Some(42));
    }

    #[test]
    fn varints_roundtrip() {
        let mut w = BitWriter::new();
        // Offset the stream so varints are not byte-aligned.
        w.write_bit(true);
        for v in [0i64, 1, -1, 63, -64, 1_000_000, -1_000_000, i64::MAX, i64::MIN] {
            w.write_varint(v);
        }
        for v in [0u64, 127, 128, 300, u64::MAX] {
            w.write_uvarint(v);
        }
        let bytes = w.into_bytes();
        let mut r = BitReader::new(&bytes);
        assert_eq!(r.read_bit(), Some(true));
        for v in [0i64, 1, -1, 63, -64, 1_000_000, -1_000_000, i64::MAX, i64::MIN] {
            assert_eq!(r.read_varint(), Some(v));
        }
        for v in [0u64, 127, 128, 300, u64::MAX] {
            assert_eq!(r.read_uvarint(), Some(v));
        }
    }
}
