//! Byte-level varint helpers shared by the WAL and block formats.

use crate::{Result, TsdbError};

pub fn put_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            return;
        }
        buf.push(byte | 0x80);
    }
}

pub fn put_varint(buf: &mut Vec<u8>, v: i64) {
    put_uvarint(buf, ((v << 1) ^ (v >> 63)) as u64);
}

pub fn put_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    put_uvarint(buf, b.len() as u64);
    buf.extend_from_slice(b);
}

/// Sequential decoder over a byte slice.
#[derive(Debug)]
pub struct Dec<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Dec<'a> {
    pub fn new(buf: &'a [u8]) -> Dec<'a> {
        Dec { buf, pos: 0 }
    }

    fn corrupt(what: &str) -> TsdbError {
        TsdbError::Corrupt(format!("truncated {what}"))
    }

    pub fn u8(&mut self) -> Result<u8> {
        let b = *self.buf.get(self.pos).ok_or_else(|| Self::corrupt("byte"))?;
        self.pos += 1;
        Ok(b)
    }

    pub fn uvarint(&mut self) -> Result<u64> {
        let mut v = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = self.u8()?;
            v |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(v);
            }
            shift += 7;
            if shift >= 64 {
                return Err(Self::corrupt("uvarint"));
            }
        }
    }

    pub fn varint(&mut self) -> Result<i64> {
        let u = self.uvarint()?;
        Ok(((u >> 1) as i64) ^ -((u & 1) as i64))
    }

    pub fn u64_le(&mut self) -> Result<u64> {
        let end = self.pos + 8;
        let bytes = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| Self::corrupt("u64"))?;
        self.pos = end;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.uvarint()? as usize;
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| Self::corrupt("length"))?;
        let b = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| Self::corrupt("bytes"))?;
        self.pos = end;
        Ok(b)
    }

    pub fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?.to_vec())
            .map_err(|_| TsdbError::Corrupt("invalid utf-8".into()))
    }
}
