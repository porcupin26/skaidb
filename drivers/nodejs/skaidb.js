'use strict';
// skaidb — Node.js driver. API modeled on node-postgres (`pg`).
// Pure standard library: `net` + `crypto`. No dependencies.
//
//   const { Client } = require('skaidb');
//   const client = new Client({ host: 'localhost', port: 7000,
//                               user: 'skaidb', password: 'secret' });
//   await client.connect();
//   const res = await client.query('SELECT id, name FROM users WHERE id = $1', [1]);
//   console.log(res.rows);          // [ { id: 1, name: 'Ada' } ]
//   await client.end();

const net = require('net');
const crypto = require('crypto');

const CONSISTENCY = { ONE: 0, QUORUM: 1, ALL: 2 };

class SkaidbError extends Error {}

// ---- Value codec (§4 of PROTOCOL.md) --------------------------------------

class Reader {
  constructor(buf) { this.buf = buf; this.pos = 0; }
  take(n) {
    const end = this.pos + n;
    if (end > this.buf.length) throw new SkaidbError('truncated server message');
    const s = this.buf.subarray(this.pos, end);
    this.pos = end;
    return s;
  }
  u8() { return this.take(1)[0]; }
  u32() { const b = this.take(4); return b.readUInt32LE(0); }
  i64() { return this.take(8).readBigInt64LE(0); }
  u64() { return this.take(8).readBigUInt64LE(0); }
  blob() { return Buffer.from(this.take(this.u32())); }
  text() { return this.blob().toString('utf8'); }
}

function safeInt(big) {
  return (big >= BigInt(Number.MIN_SAFE_INTEGER) && big <= BigInt(Number.MAX_SAFE_INTEGER))
    ? Number(big) : big; // BigInt only when it would lose precision
}

function decodeValue(r) {
  const tag = r.u8();
  switch (tag) {
    case 0: return null;
    case 1: return r.u8() !== 0;
    case 2: return safeInt(r.i64());
    case 3: return r.take(8).readDoubleLE(0);
    case 4: {                                   // Decimal -> string
      const mantissa = bytesToBigIntLE(r.take(16), true);
      const scale = r.u32();
      return decimalToString(mantissa, scale);
    }
    case 5: return r.text();
    case 6: return r.blob();                    // Bytes -> Buffer
    case 7: return formatUuid(r.take(16));      // Uuid -> canonical string
    case 8: return new Date(Number(r.i64()));   // Timestamp(ms) -> Date
    case 9: {                                   // Array
      const n = r.u32(); const out = [];
      for (let i = 0; i < n; i++) out.push(decodeValue(r));
      return out;
    }
    case 10: {                                  // Document -> object
      const n = r.u32(); const out = {};
      for (let i = 0; i < n; i++) { const k = r.text(); out[k] = decodeValue(r); }
      return out;
    }
    default: throw new SkaidbError(`unknown value tag ${tag}`);
  }
}

function bytesToBigIntLE(buf, signed) {
  let v = 0n;
  for (let i = buf.length - 1; i >= 0; i--) v = (v << 8n) | BigInt(buf[i]);
  if (signed && (buf[buf.length - 1] & 0x80)) v -= (1n << BigInt(8 * buf.length));
  return v;
}

function decimalToString(mantissa, scale) {
  if (scale === 0) return mantissa.toString();
  const neg = mantissa < 0n;
  let digits = (neg ? -mantissa : mantissa).toString().padStart(scale + 1, '0');
  const point = digits.length - scale;
  const s = digits.slice(0, point) + '.' + digits.slice(point);
  return neg ? '-' + s : s;
}

function formatUuid(b) {
  const h = Buffer.from(b).toString('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

// ---- Client-side parameter binding (§5, pg-style $1, $2 ...) ---------------

function quote(arg) {
  if (arg === null || arg === undefined) return 'NULL';
  switch (typeof arg) {
    case 'boolean': return arg ? 'TRUE' : 'FALSE';
    case 'number':
      if (!Number.isFinite(arg)) throw new SkaidbError('cannot bind NaN/Infinity');
      return String(arg);
    case 'bigint': return arg.toString();
    case 'string': return "'" + arg.replace(/'/g, "''") + "'";
  }
  if (arg instanceof Date) return String(arg.getTime());
  if (Buffer.isBuffer(arg)) return "'" + arg.toString('hex') + "'";
  throw new SkaidbError(`cannot bind value of type ${typeof arg}`);
}

function bindParams(sql, params) {
  if (!params || params.length === 0) return sql;
  // Replace $N outside string literals.
  let out = '';
  let inStr = false;
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    if (inStr) {
      out += ch;
      if (ch === "'") {
        if (sql[i + 1] === "'") { out += "'"; i++; } else inStr = false;
      }
      continue;
    }
    if (ch === "'") { inStr = true; out += ch; continue; }
    if (ch === '$' && /[0-9]/.test(sql[i + 1] || '')) {
      let j = i + 1, num = '';
      while (/[0-9]/.test(sql[j] || '')) { num += sql[j]; j++; }
      const idx = parseInt(num, 10) - 1;
      if (idx < 0 || idx >= params.length) throw new SkaidbError(`no parameter for $${num}`);
      out += quote(params[idx]);
      i = j - 1;
      continue;
    }
    out += ch;
  }
  return out;
}

// ---- Connection / Client (pg-style) ---------------------------------------

let nonceCounter = 0;

class Client {
  constructor(opts = {}) {
    this.host = opts.host || 'localhost';
    this.port = opts.port || 7000;
    this.user = opts.user || 'anonymous';
    this.password = opts.password || '';
    this.consistency = resolveConsistency(opts.consistency);
    this.connectTimeout = opts.connectTimeout || 10000;
    this._sock = null;
    this._buf = Buffer.alloc(0);
    this._waiters = [];       // queue of {resolve, reject} awaiting a frame
    this._queryChain = Promise.resolve();
    this._closed = false;
  }

  connect() {
    return new Promise((resolve, reject) => {
      const sock = net.createConnection({ host: this.host, port: this.port });
      sock.setNoDelay(true);
      const onErr = (e) => reject(new SkaidbError(`connect failed: ${e.message}`));
      sock.once('error', onErr);
      const to = setTimeout(() => { sock.destroy(); reject(new SkaidbError('connect timeout')); },
        this.connectTimeout);
      sock.once('connect', () => {
        clearTimeout(to);
        sock.removeListener('error', onErr);
        this._sock = sock;
        sock.on('data', (d) => this._onData(d));
        sock.on('error', (e) => this._fail(e));
        sock.on('close', () => this._fail(new SkaidbError('connection closed')));
        this._handshake().then(resolve, reject);
      });
    });
  }

  _onData(d) {
    this._buf = Buffer.concat([this._buf, d]);
    // Parse as many complete frames as available.
    for (;;) {
      if (this._buf.length < 4) return;
      const len = this._buf.readUInt32BE(0);
      if (this._buf.length < 4 + len) return;
      const frame = this._buf.subarray(4, 4 + len);
      this._buf = this._buf.subarray(4 + len);
      const w = this._waiters.shift();
      if (w) w.resolve(Buffer.from(frame));
    }
  }

  _fail(err) {
    if (this._closed) return;
    this._closed = true;
    const e = err instanceof Error ? err : new SkaidbError(String(err));
    while (this._waiters.length) this._waiters.shift().reject(e);
  }

  _writeFrame(payload) {
    const head = Buffer.allocUnsafe(4);
    head.writeUInt32BE(payload.length, 0);
    this._sock.write(Buffer.concat([head, payload]));
  }

  _readFrame() {
    return new Promise((resolve, reject) => this._waiters.push({ resolve, reject }));
  }

  async _handshake() {
    const clientNonce = `js${process.pid}.${nonceCounter++}`;
    this._writeFrame(Buffer.concat([Buffer.from([10]), encStr(this.user), encStr(clientNonce)]));

    const r1 = new Reader(await this._readFrame());
    if (r1.u8() !== 11) throw new SkaidbError('bad handshake challenge');
    const salt = r1.blob();
    const iterations = r1.u32();
    const serverNonce = r1.text();

    const authMessage = Buffer.from(
      [this.user, clientNonce, serverNonce, salt.toString('hex'), String(iterations)].join('\0'),
      'utf8');
    const salted = crypto.pbkdf2Sync(this.password, salt, iterations, 32, 'sha256');
    const clientKey = hmac(salted, Buffer.from('Client Key'));
    const storedKey = sha256(clientKey);
    const clientSig = hmac(storedKey, authMessage);
    const proof = xor(clientKey, clientSig);

    this._writeFrame(Buffer.concat([Buffer.from([12]), proof]));

    const r2 = new Reader(await this._readFrame());
    if (r2.u8() !== 13) throw new SkaidbError('bad handshake outcome');
    if (r2.u8() === 1) {
      const serverSig = r2.take(32);
      if (this.password) {
        const serverKey = hmac(salted, Buffer.from('Server Key'));
        const expected = hmac(serverKey, authMessage);
        if (!crypto.timingSafeEqual(serverSig, expected))
          throw new SkaidbError('server signature mismatch (mutual auth failed)');
      }
    } else {
      throw new SkaidbError(`authentication denied: ${r2.text()}`);
    }
  }

  // pg-style: query(text, [params]) or query({ text, values, consistency, rowMode })
  query(config, values) {
    let text, params, consistency = this.consistency, rowMode = 'object';
    if (typeof config === 'string') { text = config; params = values; }
    else {
      text = config.text; params = config.values || values;
      if (config.consistency !== undefined) consistency = resolveConsistency(config.consistency);
      if (config.rowMode) rowMode = config.rowMode;
    }
    // Serialize queries on this connection (one request/response in flight).
    const run = this._queryChain.then(() => this._doQuery(bindParams(text, params), consistency, rowMode));
    this._queryChain = run.catch(() => {}); // keep the chain alive after errors
    return run;
  }

  async _doQuery(sql, consistency, rowMode) {
    if (this._closed) throw new SkaidbError('connection is closed');
    const body = Buffer.from(sql, 'utf8');
    const head = Buffer.allocUnsafe(6);
    head[0] = 1; head[1] = consistency; head.writeUInt32LE(body.length, 2);
    this._writeFrame(Buffer.concat([head, body]));

    const r = new Reader(await this._readFrame());
    const tag = r.u8();
    if (tag === 0) {                            // Rows
      const ncols = r.u32();
      const fields = [];
      for (let i = 0; i < ncols; i++) fields.push({ name: r.text() });
      const nrows = r.u32();
      const rows = [];
      for (let i = 0; i < nrows; i++) {
        const ncells = r.u32();
        const cells = [];
        for (let c = 0; c < ncells; c++) cells.push(decodeValue(new Reader(r.blob())));
        if (rowMode === 'array') rows.push(cells);
        else {
          const obj = {};
          for (let c = 0; c < ncells; c++) obj[fields[c].name] = cells[c];
          rows.push(obj);
        }
      }
      return { command: 'SELECT', rowCount: rows.length, rows, fields,
               columns: fields.map((f) => f.name) };
    }
    if (tag === 1) {                            // Mutation
      const affected = r.u64();
      return { command: 'MUTATION', rowCount: safeInt(affected), rows: [], fields: [] };
    }
    if (tag === 2) return { command: 'DDL', rowCount: null, rows: [], fields: [] };
    if (tag === 3) throw new SkaidbError(r.text());
    throw new SkaidbError(`unknown response tag ${tag}`);
  }

  end() {
    this._closed = true;
    return new Promise((resolve) => {
      if (!this._sock) return resolve();
      this._sock.end(resolve);
    });
  }
}

// ---- helpers --------------------------------------------------------------

function resolveConsistency(c) {
  if (c === undefined) return CONSISTENCY.QUORUM;
  if (typeof c === 'number') { if ([0, 1, 2].includes(c)) return c; throw new SkaidbError(`bad consistency ${c}`); }
  const v = CONSISTENCY[String(c).toUpperCase()];
  if (v === undefined) throw new SkaidbError(`bad consistency ${c}`);
  return v;
}
function encStr(s) {
  const b = Buffer.from(s, 'utf8');
  const head = Buffer.allocUnsafe(4); head.writeUInt32LE(b.length, 0);
  return Buffer.concat([head, b]);
}
function hmac(key, msg) { return crypto.createHmac('sha256', key).update(msg).digest(); }
function sha256(b) { return crypto.createHash('sha256').update(b).digest(); }
function xor(a, b) { const o = Buffer.allocUnsafe(a.length); for (let i = 0; i < a.length; i++) o[i] = a[i] ^ b[i]; return o; }

module.exports = { Client, SkaidbError, CONSISTENCY };
