# skaidb wire protocol

This is the canonical specification every official driver implements. It
describes the **binary fast-path protocol** spoken on the server's `quic_port`
(default **7000**, raw TCP today). A simpler HTTP/JSON gateway also exists on
`rest_port` (default 7080) and is documented at the end.

The binary protocol is deliberately small: a length-prefixed frame, a four-step
SCRAM-SHA-256 handshake, then request/response query frames. The reference
implementation is the in-tree Rust driver (`crates/skaidb-driver`) and the
Python driver in `drivers/python`, which is verified against a live cluster.

All multi-byte integers are **little-endian (LE)** *except the frame length
prefix, which is big-endian (BE)*. Watch that distinction — it is the single
most common porting mistake.

---

## 1. Framing

Every message — handshake step or query — is one frame:

```
+-----------------+--------------------------+
| u32 length (BE) | payload (length bytes)   |
+-----------------+--------------------------+
```

- `length` is the byte count of `payload`, big-endian.
- Maximum accepted payload is 64 MiB (`64 * 1024 * 1024`).
- Read: read 4 bytes → length → read exactly `length` bytes.
- Write: write the BE length, then the payload, then flush.

Within payloads, all length fields are **u32 little-endian** unless stated.

---

## 2. Handshake (SCRAM-SHA-256)

Exactly **four frames**, always — even when the server has auth disabled (it
then accepts any proof). Run it immediately after connecting, before any query.

```
client → AuthStart     { username, client_nonce }
server → AuthChallenge { salt, iterations, server_nonce }
client → AuthFinish    { client_proof }
server → AuthOutcome   Ok{ server_signature } | Denied{ reason }
```

Each handshake payload starts with a **1-byte tag**:

| Message       | Tag |
|---------------|-----|
| AuthStart     | 10  |
| AuthChallenge | 11  |
| AuthFinish    | 12  |
| AuthOutcome   | 13  |

Encodings (a `str`/`bytes` field = `u32 LE length` followed by the bytes;
strings are UTF-8):

**AuthStart** (client → server)
```
u8  = 10
str username
str client_nonce
```
`client_nonce` is any fresh, unique ASCII string (e.g. `"c"+pid+"."+counter`).

**AuthChallenge** (server → client)
```
u8  = 11
bytes salt              (u32 LE len + salt bytes)
u32 iterations (LE)
str server_nonce
```

**AuthFinish** (client → server)
```
u8  = 12
32 bytes client_proof    (raw, NOT length-prefixed — always exactly 32 bytes)
```

**AuthOutcome** (server → client)
```
u8  = 13
u8  ok_flag
  if ok_flag == 1:  32 bytes server_signature   (raw, exactly 32 bytes)
  if ok_flag == 0:  str reason
```

### 2.1 Computing the proof

First build the **auth message** (identical on both sides). `salt_hex` is the
salt as **lowercase** hex; `iterations` is its decimal ASCII form; `\0` is a
single NUL byte:

```
auth_message = username + "\0" + client_nonce + "\0" + server_nonce
                        + "\0" + salt_hex      + "\0" + str(iterations)
```

Then, with HMAC = HMAC-SHA-256, `H` = SHA-256, PBKDF2 = PBKDF2-HMAC-SHA-256:

```
salted    = PBKDF2(password, salt, iterations, dkLen = 32)
clientKey = HMAC(salted, "Client Key")
storedKey = H(clientKey)
clientSig = HMAC(storedKey, auth_message)
clientProof = clientKey XOR clientSig           # 32 bytes, sent in AuthFinish
```

Note the HMAC key argument is **first**: `HMAC(key, message)`.

### 2.2 Verifying the server (mutual auth — optional but recommended)

When the password is non-empty, verify the `server_signature` from a successful
`AuthOutcome`:

```
serverKey      = HMAC(salted, "Server Key")
expectedServerSig = HMAC(serverKey, auth_message)
```

If `expectedServerSig != server_signature`, treat the connection as
untrustworthy and close it. Skip this check when connecting anonymously
(empty password).

### 2.3 Anonymous connections

If the server has auth disabled, connect with username `"anonymous"` and an
empty password. The handshake still runs all four frames; the server returns
`Ok`. Do not verify the server signature when the password is empty.

---

## 3. Query request / response

After a successful handshake the connection is a simple
request → response loop. One request frame, one response frame, repeat. The
connection is persistent and may be reused for many queries (and should be —
pool/reuse it).

### 3.1 Request

```
u8  = 1                       # OP_QUERY
u8  consistency               # 0 = ONE, 1 = QUORUM, 2 = ALL
u32 sql_len (LE)
sql bytes (UTF-8)
```

Consistency selects how many replicas must acknowledge (writes) or be consulted
(reads) before the server answers. Drivers default to **QUORUM (1)**.

Servers ≥ 0.16.8 also accept the prepared-statement opcodes 2–4 (§3.3); a
driver that only ever sends `OP_QUERY` is fully compatible in both directions.

### 3.2 Response

First byte is a tag:

| Tag | Meaning   |
|-----|-----------|
| 0   | Rows      |
| 1   | Mutation  |
| 2   | Ddl       |
| 3   | Error     |
| 4   | Prepared  |

**Rows (0)** — a `SELECT` result set:
```
u8  = 0
u32 ncols (LE)
ncols × [ u32 len (LE) + column name (UTF-8) ]
u32 nrows (LE)
nrows × row, where each row is:
    u32 ncells (LE)
    ncells × [ u32 vlen (LE) + value bytes ]      # value bytes per §4
```
`ncells` always equals `ncols`. Each cell's `value bytes` is a self-describing
[Value](#4-value-encoding).

**Mutation (1)** — `INSERT`/`UPDATE`/`DELETE`:
```
u8  = 1
u64 affected (LE)
```

**Ddl (2)** — `CREATE`/`DROP`/etc. succeeded:
```
u8  = 2
```

**Error (3)** — the statement failed:
```
u8  = 3
str message      (u32 LE len + UTF-8)
```
Drivers should raise/return this `message` as a query error.

**Prepared (4)** — reply to `OP_PREPARE` (§3.3):
```
u8  = 4
u32 statement id (LE)
u16 parameter count (LE)
```

### 3.3 Prepared statements (server ≥ 0.16.8)

A statement containing `?` placeholders can be parsed **once** and executed
many times with different bindings — skipping the per-request SQL parse and
giving a typed, injection-safe parameter path. Prepared ids are scoped to the
connection that created them: they are invalid on any other connection, and
gone when the connection closes. Only `SELECT`/`INSERT`/`UPDATE`/`DELETE` can
be prepared; DDL and session statements are refused with `Error`.

`OP_PREPARE` — parse and cache; answered with `Prepared`:
```
u8  = 2                       # OP_PREPARE
u32 sql_len (LE)
sql bytes (UTF-8), may contain `?` placeholders
```

`OP_EXECUTE` — run a prepared statement; answered like a normal query:
```
u8  = 3                       # OP_EXECUTE
u8  consistency               # as in OP_QUERY
u32 statement id (LE)
u16 nparams (LE)
nparams × [ u32 len (LE) + value bytes ]   # §4 Value encoding, in `?` order
```
The binding count must equal the statement's parameter count exactly, else
`Error`.

`OP_CLOSE` — free a prepared statement's slot (a server caps open statements
per connection at 256); answered with `Ddl` on success:
```
u8  = 4                       # OP_CLOSE
u32 statement id (LE)
```

An old server (< 0.16.8) answers opcodes 2–4 with `Error("unknown opcode")` —
drivers can feature-detect by preparing once and falling back to client-side
interpolation (§5).

---

## 4. Value encoding

Each result cell is one value, encoded losslessly. First byte is a type tag,
followed by the type's payload:

| Tag | Type      | Payload                                                        |
|-----|-----------|----------------------------------------------------------------|
| 0   | Null      | (none)                                                         |
| 1   | Bool      | `u8` (0 or 1)                                                  |
| 2   | Int       | `i64` LE                                                       |
| 3   | Float     | `f64` LE (IEEE-754 bits, little-endian)                        |
| 4   | Decimal   | `i128` mantissa LE (16 bytes) + `u32` scale LE                 |
| 5   | String    | `u32` len LE + UTF-8 bytes                                     |
| 6   | Bytes     | `u32` len LE + raw bytes                                       |
| 7   | Uuid      | 16 raw bytes (big-endian / RFC 4122 byte order)               |
| 8   | Timestamp | `i64` LE — **Unix time in milliseconds**                       |
| 9   | Array     | `u32` count LE + `count` values (recursive)                    |
| 10  | Document  | `u32` count LE + `count` × [ `u32` keylen LE + key + value ]   |

Notes:
- **Decimal** `value = mantissa / 10^scale`. Drivers may surface it as a
  string/decimal type to avoid precision loss.
- **Timestamp** is milliseconds since the Unix epoch (can be negative).
- **Document** preserves insertion order of keys.
- Map these to the most natural language type (Array→list, Document→map/dict,
  Uuid→UUID type or canonical string, Bytes→byte array).

---

## 5. Parameter binding (client-side fallback)

Against servers ≥ 0.16.8 prefer the server-side prepared statements of §3.3.
For older servers (or drivers that have not implemented §3.3), offer the
parameterized API (`?`/`%s`/`$1` placeholders) by interpolating arguments into
the SQL **client-side**, with correct SQL quoting:

- **string**: wrap in single quotes; escape each `'` by doubling it → `''`.
  (skaidb uses standard SQL `''` escaping; backslashes are literal.)
- **integer / float**: numeric literal as-is (use a round-trip-safe float
  format; reject NaN/Inf).
- **bool**: `TRUE` / `FALSE`.
- **null / None / nil**: `NULL`.
- **bytes**: hex/blob literal if supported, else document as unsupported.

Always quote-escape strings — this is the SQL-injection boundary. Provide the
placeholder style idiomatic to the language (see each driver's README).

---

## 6. Connection lifecycle & errors

1. TCP connect (set `TCP_NODELAY` for low latency).
2. Run the 4-frame handshake. On `Denied`, raise an auth error and close.
3. Loop: send request frame, read response frame.
4. On any framing/IO error, the connection is dead — discard it (don't reuse).
5. An `Error` response is a *statement* error, not a connection error; the
   connection stays usable for the next query.

A cluster has multiple nodes (default internode/clients on each). Every node
accepts reads and writes (leaderless), so a driver may connect to any node, or
round-robin a list of hosts for availability. Token-aware routing is optional.

---

## 7. REST/JSON gateway (alternative, port 7080)

For environments where a binary client is inconvenient (browsers, quick
scripts), the server also speaks HTTP/1.1:

- `POST /query` with the SQL as the body (raw text) or JSON `{"sql": "..."}`.
- Auth: HTTP **Basic** (`Authorization: Basic base64(user:pass)`) when the
  server requires auth.
- One request per connection (`Connection: close`).
- Response JSON:
  - rows: `{"columns": [...], "rows": [[...], ...]}` (values as JSON)
  - mutation: `{"affected": N}`
  - ddl: `{"ok": true}`
  - error: `{"error": "..."}` (HTTP 400)
- `GET /metrics` returns Prometheus text.

The REST path loses binary type fidelity (UUID/Bytes/Timestamp arrive as their
JSON forms) and opens a fresh connection per request, so the binary protocol is
preferred for application drivers. Drivers may offer REST as a fallback mode.
