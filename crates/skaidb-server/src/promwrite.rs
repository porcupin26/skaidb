//! Prometheus `remote_write` ingestion (docs/TODO.md phase 4).
//!
//! `POST /api/v1/write` bodies are snappy-block-compressed protobuf
//! `WriteRequest`s. The decoder below hand-parses just the fields skaidb
//! needs (labels + samples), so no protobuf dependency is pulled in. Samples
//! land in the `metrics` time-series table (auto-created on first write as
//! `SERIES KEY (name), OOO 1h` — HA Prometheus pairs interleave); the `__name__` label maps to `name` (double
//! underscores are reserved in skaidb), other labels pass through.

use skaidb_tsdb::Labels;

use crate::shared::{execute_session_as, Shared};

/// The time-series table remote_write ingests into.
const TABLE: &str = "metrics";

/// Decode, map, and append a remote_write body. Returns accepted samples.
pub fn ingest(ctx: &Shared, role: &str, body: &[u8]) -> Result<usize, String> {
    if !ctx.allowed(
        role,
        skaidb_auth::Privilege::Insert,
        &skaidb_auth::Object::Table(TABLE.into()),
    ) {
        return Err(format!("permission denied: Insert on {TABLE}"));
    }
    let raw = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|e| format!("snappy: {e}"))?;
    let rows = decode_write_request(&raw)?;
    if rows.is_empty() {
        return Ok(0);
    }
    match ctx.backend.ts_append(TABLE, &rows) {
        Ok(n) => Ok(n),
        Err(e) if e.to_string().contains("does not exist") => {
            // First write: create the ingest table (broadcast in a cluster),
            // under the caller's role so RBAC still applies.
            let mut db = skaidb_engine::DEFAULT_DATABASE.to_string();
            let create = format!(
                "CREATE TIMESERIES TABLE IF NOT EXISTS {TABLE} (SERIES KEY (name), OOO 1h)"
            );
            let resp = execute_session_as(ctx, role, &mut db, &create, None);
            if let skaidb_proto::Response::Error(e) = resp {
                return Err(format!("auto-creating {TABLE}: {e}"));
            }
            ctx.backend.ts_append(TABLE, &rows).map_err(|e| e.to_string())
        }
        Err(e) => Err(e.to_string()),
    }
}

/// Parse a protobuf `WriteRequest`: field 1 = repeated TimeSeries
/// (1 = repeated Label{1 name, 2 value}, 2 = repeated Sample{1 value double,
/// 2 timestamp int64}). Unknown fields are skipped per wire type.
fn decode_write_request(buf: &[u8]) -> Result<Vec<(Labels, i64, f64)>, String> {
    let mut rows = Vec::new();
    let mut d = Proto::new(buf);
    while let Some((field, wire)) = d.tag()? {
        if field == 1 && wire == 2 {
            let ts_buf = d.bytes()?;
            decode_timeseries(ts_buf, &mut rows)?;
        } else {
            d.skip(wire)?;
        }
    }
    Ok(rows)
}

fn decode_timeseries(buf: &[u8], rows: &mut Vec<(Labels, i64, f64)>) -> Result<(), String> {
    let mut labels: Labels = Vec::new();
    let mut samples: Vec<(i64, f64)> = Vec::new();
    let mut d = Proto::new(buf);
    while let Some((field, wire)) = d.tag()? {
        match (field, wire) {
            (1, 2) => {
                let (mut name, value) = decode_label(d.bytes()?)?;
                // `__name__` → `name`; other double-underscore labels keep a
                // single-underscore prefix (reserved namespace in skaidb).
                if name == "__name__" {
                    name = "name".into();
                } else if let Some(stripped) = name.strip_prefix("__") {
                    name = format!("_{stripped}");
                }
                labels.push((name, value));
            }
            (2, 2) => {
                let mut ts = 0i64;
                let mut value = 0f64;
                let mut s = Proto::new(d.bytes()?);
                while let Some((f, w)) = s.tag()? {
                    match (f, w) {
                        (1, 1) => value = f64::from_bits(s.fixed64()?),
                        (2, 0) => ts = s.varint()? as i64,
                        _ => s.skip(w)?,
                    }
                }
                samples.push((ts, value));
            }
            (_, w) => d.skip(w)?,
        }
    }
    labels.push(("__field__".into(), "value".into()));
    labels.sort();
    labels.dedup_by(|a, b| a.0 == b.0);
    for (ts, value) in samples {
        rows.push((labels.clone(), ts, value));
    }
    Ok(())
}

fn decode_label(buf: &[u8]) -> Result<(String, String), String> {
    let (mut name, mut value) = (String::new(), String::new());
    let mut d = Proto::new(buf);
    while let Some((field, wire)) = d.tag()? {
        match (field, wire) {
            (1, 2) => name = d.string()?,
            (2, 2) => value = d.string()?,
            (_, w) => d.skip(w)?,
        }
    }
    Ok((name, value))
}

/// Minimal protobuf wire-format reader.
struct Proto<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Proto<'a> {
    fn new(buf: &'a [u8]) -> Proto<'a> {
        Proto { buf, pos: 0 }
    }

    fn tag(&mut self) -> Result<Option<(u64, u8)>, String> {
        if self.pos >= self.buf.len() {
            return Ok(None);
        }
        let key = self.varint()?;
        Ok(Some((key >> 3, (key & 7) as u8)))
    }

    fn varint(&mut self) -> Result<u64, String> {
        let mut v = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = *self
                .buf
                .get(self.pos)
                .ok_or_else(|| "truncated varint".to_string())?;
            self.pos += 1;
            v |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(v);
            }
            shift += 7;
            if shift >= 64 {
                return Err("varint overflow".into());
            }
        }
    }

    fn fixed64(&mut self) -> Result<u64, String> {
        let end = self.pos + 8;
        let bytes = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| "truncated fixed64".to_string())?;
        self.pos = end;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn bytes(&mut self) -> Result<&'a [u8], String> {
        let len = self.varint()? as usize;
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| "length overflow".to_string())?;
        let b = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| "truncated bytes".to_string())?;
        self.pos = end;
        Ok(b)
    }

    fn string(&mut self) -> Result<String, String> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| "invalid utf-8".into())
    }

    fn skip(&mut self, wire: u8) -> Result<(), String> {
        match wire {
            0 => {
                self.varint()?;
            }
            1 => {
                self.fixed64()?;
            }
            2 => {
                self.bytes()?;
            }
            5 => {
                let end = self.pos + 4;
                if end > self.buf.len() {
                    return Err("truncated fixed32".into());
                }
                self.pos = end;
            }
            other => return Err(format!("unsupported wire type {other}")),
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    // Hand-encode a WriteRequest for tests (the inverse of the decoder).
    fn pv(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                return;
            }
            out.push(b | 0x80);
        }
    }
    fn field_bytes(out: &mut Vec<u8>, field: u64, b: &[u8]) {
        pv(out, field << 3 | 2);
        pv(out, b.len() as u64);
        out.extend_from_slice(b);
    }

    type TestSeries<'a> = (&'a [(&'a str, &'a str)], &'a [(i64, f64)]);

    pub(crate) fn encode_write_request(series: &[TestSeries]) -> Vec<u8> {
        let mut req = Vec::new();
        for (labels, samples) in series {
            let mut ts_msg = Vec::new();
            for (k, v) in labels.iter() {
                let mut l = Vec::new();
                field_bytes(&mut l, 1, k.as_bytes());
                field_bytes(&mut l, 2, v.as_bytes());
                field_bytes(&mut ts_msg, 1, &l);
            }
            for (ts, value) in samples.iter() {
                let mut s = Vec::new();
                pv(&mut s, 1 << 3 | 1);
                s.extend_from_slice(&value.to_bits().to_le_bytes());
                pv(&mut s, 2 << 3);
                pv(&mut s, *ts as u64);
                field_bytes(&mut ts_msg, 2, &s);
            }
            field_bytes(&mut req, 1, &ts_msg);
        }
        snap::raw::Encoder::new().compress_vec(&req).unwrap()
    }

    #[test]
    fn decodes_write_request() {
        let body = encode_write_request(&[
            (
                &[("__name__", "http_requests_total"), ("job", "api")],
                &[(1000, 5.0), (2000, 7.0)],
            ),
            (&[("__name__", "up")], &[(1000, 1.0)]),
        ]);
        let raw = snap::raw::Decoder::new().decompress_vec(&body).unwrap();
        let rows = decode_write_request(&raw).unwrap();
        assert_eq!(rows.len(), 3);
        let (labels, ts, v) = &rows[0];
        assert!(labels.contains(&("name".into(), "http_requests_total".into())));
        assert!(labels.contains(&("job".into(), "api".into())));
        assert!(labels.contains(&("__field__".into(), "value".into())));
        assert!(labels.windows(2).all(|w| w[0].0 <= w[1].0), "sorted");
        assert_eq!((*ts, *v), (1000, 5.0));
        assert_eq!(rows[2].1, 1000);
    }
}
