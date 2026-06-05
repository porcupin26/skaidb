//! Cross-platform positioned file I/O (`pread`/`pwrite`-style).
//!
//! The storage engine reads SSTable blocks and appends WAL frames at explicit
//! byte offsets without disturbing the file cursor. Unix exposes this directly
//! via [`std::os::unix::fs::FileExt`] (`read_exact_at`/`write_all_at`); Windows
//! offers `seek_read`/`seek_write` (which may transfer fewer bytes per call), so
//! there we loop to fill/flush the whole buffer. Both are positioned and safe to
//! call concurrently with other positioned ops on the same file.

use std::fs::File;
use std::io;

/// Read exactly `buf.len()` bytes starting at `offset`, erroring with
/// `UnexpectedEof` if the file ends first.
pub fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    imp::read_exact_at(file, buf, offset)
}

/// Write all of `buf` starting at `offset`.
pub fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    imp::write_all_at(file, buf, offset)
}

#[cfg(unix)]
mod imp {
    use std::fs::File;
    use std::io;
    use std::os::unix::fs::FileExt;

    pub fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
        file.read_exact_at(buf, offset)
    }

    pub fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
        file.write_all_at(buf, offset)
    }
}

#[cfg(windows)]
mod imp {
    use std::fs::File;
    use std::io;
    use std::os::windows::fs::FileExt;

    pub fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
        // `seek_read` may return a short read, so loop until the buffer is full.
        while !buf.is_empty() {
            match file.seek_read(buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "failed to fill whole buffer",
                    ))
                }
                Ok(n) => {
                    buf = &mut buf[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
        while !buf.is_empty() {
            match file.seek_write(buf, offset) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ))
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}
