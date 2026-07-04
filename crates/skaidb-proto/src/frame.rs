//! Length-prefixed framing for the binary protocol (SPEC §11, scp.txt fast path).
//!
//! Each message is a single frame: a big-endian `u32` length followed by that
//! many payload bytes. This is the raw-TCP fast path; QUIC is the eventual WAN
//! default.

use std::io::{self, IoSlice, Read, Write};

/// Maximum accepted frame payload (64 MiB) — guards against bogus lengths.
pub const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

/// Payloads up to this size are coalesced with the length prefix into one
/// buffer (one write syscall, one TCP segment); larger ones use a vectored
/// write to avoid copying the payload.
const COALESCE_LIMIT: usize = 8 * 1024;

/// Write `payload` as one length-prefixed frame and flush. The prefix and
/// payload go out in a single write, so with `TCP_NODELAY` the prefix is never
/// flushed as its own tiny segment.
pub fn write_frame(w: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    if payload.len() as u64 > MAX_FRAME_LEN as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame too large",
        ));
    }
    let len = (payload.len() as u32).to_be_bytes();
    if payload.len() <= COALESCE_LIMIT {
        let mut buf = Vec::with_capacity(4 + payload.len());
        buf.extend_from_slice(&len);
        buf.extend_from_slice(payload);
        w.write_all(&buf)?;
    } else {
        write_all_vectored(w, &len, payload)?;
    }
    w.flush()
}

/// `write_all` over two buffers via vectored I/O (one syscall per iteration on
/// sockets; writers without real vectored support just take an extra loop).
fn write_all_vectored(w: &mut impl Write, mut head: &[u8], mut body: &[u8]) -> io::Result<()> {
    while !head.is_empty() || !body.is_empty() {
        let n = w.write_vectored(&[IoSlice::new(head), IoSlice::new(body)])?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write whole frame",
            ));
        }
        if n >= head.len() {
            body = &body[n - head.len()..];
            head = &[];
        } else {
            head = &head[n..];
        }
    }
    Ok(())
}

/// Read one length-prefixed frame. Returns the payload bytes.
pub fn read_frame(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut payload = Vec::new();
    read_frame_into(r, &mut payload)?;
    Ok(payload)
}

/// Like [`read_frame`], but into a caller-owned buffer whose capacity is
/// reused across frames: on a long-lived connection the per-message
/// allocation (and the zero-fill of a fresh buffer) happens only when a frame
/// exceeds the high-water mark. The buffer is left holding exactly the
/// payload.
pub fn read_frame_into(r: &mut impl Read, buf: &mut Vec<u8>) -> io::Result<()> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let len = len as usize;
    if buf.len() < len {
        buf.resize(len, 0);
    }
    buf.truncate(len);
    r.read_exact(buf)?;
    Ok(())
}

/// Start building a frame in place: clear `buf` and reserve the 4-byte length
/// prefix. Encode the payload directly after it, then send with
/// [`finish_frame`] — one buffer, no payload copy.
pub fn begin_frame(buf: &mut Vec<u8>) {
    buf.clear();
    buf.extend_from_slice(&[0u8; 4]);
}

/// Finish a frame begun with [`begin_frame`]: patch the length prefix and
/// write the whole frame (prefix + payload) in one call, then flush.
pub fn finish_frame(w: &mut impl Write, buf: &mut [u8]) -> io::Result<()> {
    let payload_len = buf.len() - 4;
    if payload_len as u64 > MAX_FRAME_LEN as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame too large",
        ));
    }
    let len = (payload_len as u32).to_be_bytes();
    buf[..4].copy_from_slice(&len);
    w.write_all(buf)?;
    w.flush()
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
