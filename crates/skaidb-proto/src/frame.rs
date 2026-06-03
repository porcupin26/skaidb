//! Length-prefixed framing for the binary protocol (SPEC §11, scp.txt fast path).
//!
//! Each message is a single frame: a big-endian `u32` length followed by that
//! many payload bytes. This is the raw-TCP fast path; QUIC is the eventual WAN
//! default.

use std::io::{self, Read, Write};

/// Maximum accepted frame payload (64 MiB) — guards against bogus lengths.
pub const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

/// Write `payload` as one length-prefixed frame and flush.
pub fn write_frame(w: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    if payload.len() as u64 > MAX_FRAME_LEN as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame too large",
        ));
    }
    w.write_all(&(payload.len() as u32).to_be_bytes())?;
    w.write_all(payload)?;
    w.flush()
}

/// Read one length-prefixed frame. Returns the payload bytes.
pub fn read_frame(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut payload = vec![0u8; len as usize];
    r.read_exact(&mut payload)?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut buf = Vec::new();
        write_frame(&mut buf, b"hello").unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        assert_eq!(read_frame(&mut cursor).unwrap(), b"hello");
    }

    #[test]
    fn empty_frame() {
        let mut buf = Vec::new();
        write_frame(&mut buf, b"").unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        assert_eq!(read_frame(&mut cursor).unwrap(), Vec::<u8>::new());
    }
}
