//! Block compression codecs (SPEC §12).
//!
//! Used for SSTable data blocks (and available for internode frames). Both
//! backends are pure-Rust so the static (musl) build needs no C toolchain:
//! - **LZ4** (`lz4_flex`): very fast, modest ratio — for hot levels.
//! - **Brotli**: slower, higher ratio — for the bottom (cold) level.

use std::io::Read;

use crate::error::{Result, StorageError};

/// Quality for Brotli (0–11). 9 is a high-ratio setting without the extreme
/// cost of 11; well-suited to write-once bottom-level SSTables.
const BROTLI_QUALITY: u32 = 9;
const BROTLI_WINDOW: u32 = 22;

/// A compression codec for stored blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    None,
    Lz4,
    Brotli,
}

impl Codec {
    pub fn to_u8(self) -> u8 {
        match self {
            Codec::None => 0,
            Codec::Lz4 => 1,
            Codec::Brotli => 2,
        }
    }

    pub fn from_u8(b: u8) -> Option<Codec> {
        match b {
            0 => Some(Codec::None),
            1 => Some(Codec::Lz4),
            2 => Some(Codec::Brotli),
            _ => None,
        }
    }
}

/// Compress `data` with `codec`.
pub fn compress(codec: Codec, data: &[u8]) -> Vec<u8> {
    match codec {
        Codec::None => data.to_vec(),
        Codec::Lz4 => lz4_flex::block::compress(data),
        Codec::Brotli => {
            let mut out = Vec::new();
            let params = brotli::enc::BrotliEncoderParams {
                quality: BROTLI_QUALITY as i32,
                lgwin: BROTLI_WINDOW as i32,
                ..Default::default()
            };
            let mut input = data;
            brotli::BrotliCompress(&mut input, &mut out, &params).expect("brotli compress in-memory");
            out
        }
    }
}

/// Decompress `data` (produced by [`compress`]) given the original length.
pub fn decompress(codec: Codec, data: &[u8], uncompressed_len: usize) -> Result<Vec<u8>> {
    let corrupt = |detail| StorageError::Corruption { offset: 0, detail };
    match codec {
        Codec::None => Ok(data.to_vec()),
        Codec::Lz4 => {
            lz4_flex::block::decompress(data, uncompressed_len).map_err(|_| corrupt("lz4 decompress"))
        }
        Codec::Brotli => {
            let mut out = Vec::with_capacity(uncompressed_len);
            brotli::Decompressor::new(data, 4096)
                .read_to_end(&mut out)
                .map_err(|_| corrupt("brotli decompress"))?;
            Ok(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(codec: Codec) {
        // Repetitive data so compression actually shrinks it.
        let data = b"the quick brown fox jumps over the lazy dog ".repeat(64);
        let comp = compress(codec, &data);
        let back = decompress(codec, &comp, data.len()).unwrap();
        assert_eq!(back, data);
        if codec != Codec::None {
            assert!(comp.len() < data.len(), "{codec:?} should shrink repetitive data");
        }
    }

    #[test]
    fn roundtrips() {
        roundtrip(Codec::None);
        roundtrip(Codec::Lz4);
        roundtrip(Codec::Brotli);
    }

    #[test]
    fn brotli_beats_lz4_ratio_on_text() {
        let data = b"aaaaaaaa bbbbbbbb cccccccc dddddddd ".repeat(128);
        let lz4 = compress(Codec::Lz4, &data).len();
        let br = compress(Codec::Brotli, &data).len();
        assert!(br <= lz4, "brotli {br} should not exceed lz4 {lz4}");
    }

    #[test]
    fn codec_u8_roundtrip() {
        for c in [Codec::None, Codec::Lz4, Codec::Brotli] {
            assert_eq!(Codec::from_u8(c.to_u8()), Some(c));
        }
        assert_eq!(Codec::from_u8(9), None);
    }
}
