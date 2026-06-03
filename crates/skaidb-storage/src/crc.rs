//! CRC32 (IEEE 802.3) checksum used to detect torn or corrupted WAL records.

/// Compute the IEEE CRC32 of `data`.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            // Branchless: mask is all-ones when the low bit is set.
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::crc32;

    #[test]
    fn known_vector() {
        // CRC32("123456789") == 0xCBF43926.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn empty_is_zero() {
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn detects_single_bit_change() {
        assert_ne!(crc32(b"skaidb"), crc32(b"skaidc"));
    }
}
