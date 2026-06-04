"""skaidb — Python driver (DB-API 2.0 / PEP 249).

Pure standard library: ``socket``, ``hashlib``, ``hmac``. No third-party deps.

Quick start::

    import skaidb

    conn = skaidb.connect(host="localhost", port=7000,
                          user="skaidb", password="secret")
    cur = conn.cursor()
    cur.execute("CREATE TABLE users (PRIMARY KEY (id))")
    cur.execute("INSERT INTO users (id, name) VALUES (?, ?)", (1, "Ada"))
    cur.execute("SELECT id, name FROM users WHERE id = ?", (1,))
    print(cur.fetchall())          # [(1, 'Ada')]
    conn.close()

The API mirrors ``sqlite3``/``psycopg2`` so there is almost nothing new to
learn. Placeholders use the ``qmark`` style (``?``), exactly like ``sqlite3``.
"""

from __future__ import annotations

import datetime as _dt
import decimal
import hashlib
import hmac
import socket
import struct
import threading
import uuid
from typing import Any, Iterable, Optional, Sequence

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "Error",
    "DatabaseError",
    "OperationalError",
    "ProgrammingError",
    "Consistency",
    "apilevel",
    "threadsafety",
    "paramstyle",
]

# ---- DB-API 2.0 module globals -------------------------------------------

apilevel = "2.0"
threadsafety = 1  # threads may share the module, not connections
paramstyle = "qmark"  # uses '?' placeholders, like sqlite3

_UTC = _dt.timezone.utc
_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_UTC)


# ---- Exceptions (PEP 249 hierarchy) --------------------------------------


class Error(Exception):
    """Base class for all skaidb errors."""


class InterfaceError(Error):
    """Error related to the driver rather than the database."""


class DatabaseError(Error):
    """Error reported by the database."""


class OperationalError(DatabaseError):
    """Connection / transport problem."""


class ProgrammingError(DatabaseError):
    """A statement failed (bad SQL, constraint, etc.)."""


# ---- Consistency levels ---------------------------------------------------


class Consistency:
    ONE = 0
    QUORUM = 1
    ALL = 2

    _BY_NAME = {"ONE": 0, "QUORUM": 1, "ALL": 2}

    @classmethod
    def resolve(cls, value: "int | str") -> int:
        if isinstance(value, int):
            if value not in (0, 1, 2):
                raise ValueError(f"invalid consistency {value!r}")
            return value
        try:
            return cls._BY_NAME[value.upper()]
        except (AttributeError, KeyError):
            raise ValueError(f"invalid consistency {value!r}") from None


# ---- Value codec (§4 of PROTOCOL.md) --------------------------------------

_TAG_NULL = 0
_TAG_BOOL = 1
_TAG_INT = 2
_TAG_FLOAT = 3
_TAG_DECIMAL = 4
_TAG_STRING = 5
_TAG_BYTES = 6
_TAG_UUID = 7
_TAG_TIMESTAMP = 8
_TAG_ARRAY = 9
_TAG_DOCUMENT = 10


class _Reader:
    __slots__ = ("buf", "pos")

    def __init__(self, buf: bytes):
        self.buf = buf
        self.pos = 0

    def take(self, n: int) -> bytes:
        end = self.pos + n
        if end > len(self.buf):
            raise InterfaceError("truncated server message")
        s = self.buf[self.pos:end]
        self.pos = end
        return s

    def u8(self) -> int:
        return self.take(1)[0]

    def u32(self) -> int:
        return struct.unpack_from("<I", self.take(4))[0]

    def i64(self) -> int:
        return struct.unpack_from("<q", self.take(8))[0]

    def u64(self) -> int:
        return struct.unpack_from("<Q", self.take(8))[0]

    def blob(self) -> bytes:
        return self.take(self.u32())

    def text(self) -> str:
        return self.blob().decode("utf-8")


def _decode_value(r: _Reader) -> Any:
    tag = r.u8()
    if tag == _TAG_NULL:
        return None
    if tag == _TAG_BOOL:
        return r.u8() != 0
    if tag == _TAG_INT:
        return r.i64()
    if tag == _TAG_FLOAT:
        return struct.unpack_from("<d", r.take(8))[0]
    if tag == _TAG_DECIMAL:
        mantissa = int.from_bytes(r.take(16), "little", signed=True)
        scale = r.u32()
        return decimal.Decimal(mantissa).scaleb(-scale)
    if tag == _TAG_STRING:
        return r.text()
    if tag == _TAG_BYTES:
        return r.blob()
    if tag == _TAG_UUID:
        return uuid.UUID(bytes=r.take(16))
    if tag == _TAG_TIMESTAMP:
        return _EPOCH + _dt.timedelta(milliseconds=r.i64())
    if tag == _TAG_ARRAY:
        return [_decode_value(r) for _ in range(r.u32())]
    if tag == _TAG_DOCUMENT:
        out = {}
        for _ in range(r.u32()):
            key = r.text()
            out[key] = _decode_value(r)
        return out
    raise InterfaceError(f"unknown value tag {tag}")


# ---- Client-side parameter binding (§5) -----------------------------------


def _quote(arg: Any) -> str:
    if arg is None:
        return "NULL"
    if isinstance(arg, bool):
        return "TRUE" if arg else "FALSE"
    if isinstance(arg, int):
        return str(arg)
    if isinstance(arg, float):
        if arg != arg or arg in (float("inf"), float("-inf")):
            raise ProgrammingError("cannot bind NaN/Infinity")
        return repr(arg)
    if isinstance(arg, decimal.Decimal):
        return str(arg)
    if isinstance(arg, str):
        return "'" + arg.replace("'", "''") + "'"
    if isinstance(arg, (bytes, bytearray)):
        return "'" + bytes(arg).hex() + "'"
    if isinstance(arg, uuid.UUID):
        return "'" + str(arg) + "'"
    if isinstance(arg, _dt.datetime):
        ms = int((arg.astimezone(_UTC) - _EPOCH).total_seconds() * 1000)
        return str(ms)
    raise ProgrammingError(f"cannot bind value of type {type(arg).__name__}")


def _bind(sql: str, params: Optional[Sequence[Any]]) -> str:
    if not params:
        if "?" in _strip_strings(sql):
            raise ProgrammingError("query has placeholders but no parameters given")
        return sql
    out = []
    it = iter(params)
    in_str = False
    i = 0
    n = len(sql)
    used = 0
    while i < n:
        ch = sql[i]
        if in_str:
            out.append(ch)
            if ch == "'":
                # doubled '' stays inside the string
                if i + 1 < n and sql[i + 1] == "'":
                    out.append("'")
                    i += 2
                    continue
                in_str = False
            i += 1
            continue
        if ch == "'":
            in_str = True
            out.append(ch)
            i += 1
            continue
        if ch == "?":
            try:
                out.append(_quote(next(it)))
            except StopIteration:
                raise ProgrammingError("more placeholders than parameters") from None
            used += 1
            i += 1
            continue
        out.append(ch)
        i += 1
    remaining = list(it)
    if remaining:
        raise ProgrammingError("more parameters than placeholders")
    return "".join(out)


def _strip_strings(sql: str) -> str:
    """Return sql with single-quoted literals blanked, for placeholder counting."""
    out = []
    in_str = False
    i = 0
    n = len(sql)
    while i < n:
        ch = sql[i]
        if in_str:
            if ch == "'":
                if i + 1 < n and sql[i + 1] == "'":
                    i += 2
                    continue
                in_str = False
            i += 1
            continue
        if ch == "'":
            in_str = True
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


# ---- SCRAM-SHA-256 handshake (§2) -----------------------------------------


def _scram_proof(password: str, salt: bytes, iterations: int, auth_message: bytes):
    salted = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations, 32)
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    client_sig = hmac.new(stored_key, auth_message, hashlib.sha256).digest()
    proof = bytes(a ^ b for a, b in zip(client_key, client_sig))
    server_key = hmac.new(salted, b"Server Key", hashlib.sha256).digest()
    server_sig = hmac.new(server_key, auth_message, hashlib.sha256).digest()
    return proof, server_sig


# ---- Connection -----------------------------------------------------------


class Connection:
    """A DB-API 2.0 connection to one skaidb node.

    skaidb is non-transactional (each statement commits on its own), so
    ``commit()`` and ``rollback()`` are accepted no-ops for DB-API conformance.
    """

    _nonce_counter = 0
    _nonce_lock = threading.Lock()

    def __init__(self, host, port, user, password, consistency, timeout):
        self._consistency = Consistency.resolve(consistency)
        self._lock = threading.Lock()
        self.closed = False
        try:
            self._sock = socket.create_connection((host, port), timeout=timeout)
            self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._sock.settimeout(timeout)
            self._file = self._sock.makefile("rb")
            self._handshake(user, password)
        except OSError as e:
            raise OperationalError(f"connect failed: {e}") from e

    # -- framing --
    def _write_frame(self, payload: bytes) -> None:
        self._sock.sendall(struct.pack(">I", len(payload)) + payload)

    def _read_frame(self) -> bytes:
        head = self._read_exact(4)
        (length,) = struct.unpack(">I", head)
        return self._read_exact(length)

    def _read_exact(self, n: int) -> bytes:
        buf = self._file.read(n)
        if buf is None or len(buf) != n:
            raise OperationalError("connection closed by server")
        return buf

    # -- handshake --
    def _handshake(self, user: str, password: str) -> None:
        with Connection._nonce_lock:
            Connection._nonce_counter += 1
            counter = Connection._nonce_counter
        client_nonce = f"py{id(self)}.{counter}"

        start = bytes([10]) + _enc_str(user) + _enc_str(client_nonce)
        self._write_frame(start)

        r = _Reader(self._read_frame())
        if r.u8() != 11:
            raise OperationalError("bad handshake challenge")
        salt = r.blob()
        iterations = r.u32()
        server_nonce = r.text()

        salt_hex = salt.hex()
        auth_message = "\0".join(
            [user, client_nonce, server_nonce, salt_hex, str(iterations)]
        ).encode("utf-8")
        proof, expected_server_sig = _scram_proof(password, salt, iterations, auth_message)

        self._write_frame(bytes([12]) + proof)

        r = _Reader(self._read_frame())
        if r.u8() != 13:
            raise OperationalError("bad handshake outcome")
        if r.u8() == 1:
            server_sig = r.take(32)
            if password and not hmac.compare_digest(server_sig, expected_server_sig):
                raise OperationalError("server signature mismatch (mutual auth failed)")
        else:
            raise OperationalError(f"authentication denied: {r.text()}")

    # -- query --
    def _query(self, sql: str, consistency: int):
        if self.closed:
            raise ProgrammingError("connection is closed")
        req = bytes([1, consistency]) + struct.pack("<I", len(sql.encode("utf-8")))
        req += sql.encode("utf-8")
        with self._lock:
            self._write_frame(req)
            r = _Reader(self._read_frame())
        tag = r.u8()
        if tag == 0:  # Rows
            ncols = r.u32()
            columns = [r.text() for _ in range(ncols)]
            rows = []
            for _ in range(r.u32()):
                ncells = r.u32()
                row = tuple(_decode_value(_Reader(r.blob())) for _ in range(ncells))
                rows.append(row)
            return ("rows", columns, rows)
        if tag == 1:  # Mutation
            return ("mutation", r.u64(), None)
        if tag == 2:  # Ddl
            return ("ddl", 0, None)
        if tag == 3:  # Error
            raise ProgrammingError(r.text())
        raise InterfaceError(f"unknown response tag {tag}")

    # -- DB-API surface --
    def cursor(self) -> "Cursor":
        if self.closed:
            raise ProgrammingError("connection is closed")
        return Cursor(self)

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> "Cursor":
        """Convenience: make a cursor, execute, return it."""
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def commit(self) -> None:  # no-op: skaidb auto-commits each statement
        pass

    def rollback(self) -> None:  # no-op: skaidb is non-transactional
        raise OperationalError("skaidb does not support rollback (auto-commit only)")

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            try:
                self._file.close()
            finally:
                self._sock.close()

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _enc_str(s: str) -> bytes:
    b = s.encode("utf-8")
    return struct.pack("<I", len(b)) + b


# ---- Cursor ---------------------------------------------------------------


class Cursor:
    def __init__(self, connection: Connection):
        self.connection = connection
        self.arraysize = 1
        self.rowcount = -1
        self.description = None
        self._rows: list = []
        self._pos = 0
        self._consistency = connection._consistency

    def set_consistency(self, consistency) -> None:
        """Override the consistency level for subsequent executes on this cursor."""
        self._consistency = Consistency.resolve(consistency)

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> "Cursor":
        bound = _bind(sql, params)
        kind, a, b = self.connection._query(bound, self._consistency)
        if kind == "rows":
            columns, rows = a, b
            # description: 7-tuples per DB-API; only name is meaningful here
            self.description = [(c, None, None, None, None, None, None) for c in columns]
            self._rows = rows
            self.rowcount = len(rows)
        else:  # mutation / ddl
            self.description = None
            self._rows = []
            self.rowcount = a if kind == "mutation" else -1
        self._pos = 0
        return self

    def executemany(self, sql: str, seq_of_params: Iterable[Sequence[Any]]) -> None:
        total = 0
        for params in seq_of_params:
            self.execute(sql, params)
            if self.rowcount and self.rowcount > 0:
                total += self.rowcount
        self.rowcount = total

    def fetchone(self):
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: Optional[int] = None):
        size = self.arraysize if size is None else size
        chunk = self._rows[self._pos:self._pos + size]
        self._pos += len(chunk)
        return chunk

    def fetchall(self):
        chunk = self._rows[self._pos:]
        self._pos = len(self._rows)
        return chunk

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def close(self) -> None:
        self._rows = []

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# ---- module entry point ---------------------------------------------------


def connect(
    host: str = "localhost",
    port: int = 7000,
    user: str = "anonymous",
    password: str = "",
    consistency: "int | str" = Consistency.QUORUM,
    timeout: Optional[float] = 10.0,
) -> Connection:
    """Open a connection to a skaidb node and run the SCRAM handshake."""
    return Connection(host, port, user, password, consistency, timeout)
