"""Value-codec and prepared-statement framing tests for the skaidb driver.

Pure standard library (`unittest`) — run with `python -m unittest` from
`drivers/python`. No server required: the value codec is checked by round-trip
through the (production-proven) decoder plus golden byte layouts matching the
Rust `skaidb-types` codec, and the prepared-statement wire frames are checked
against a stubbed transport.
"""

import datetime as _dt
import decimal
import struct
import unittest
import uuid

import skaidb
from skaidb import (
    _OP_EXECUTE,
    _OP_PREPARE,
    _Reader,
    _decode_value,
    _decimal_parts,
    _encode_value,
)


def _roundtrip(v):
    return _decode_value(_Reader(_encode_value(v)))


class ValueCodec(unittest.TestCase):
    def test_scalars_roundtrip(self):
        cases = [
            None,
            True,
            False,
            0,
            1,
            -42,
            2**62,
            3.14,
            -1.5,
            "",
            "hello",
            "üñîçødé",
            b"\x00\x01\xff",
            uuid.UUID("12345678-1234-5678-1234-567812345678"),
        ]
        for v in cases:
            self.assertEqual(_roundtrip(v), v, repr(v))

    def test_timestamp_roundtrip(self):
        dt = _dt.datetime(2026, 7, 15, 12, 30, 0, tzinfo=_dt.timezone.utc)
        self.assertEqual(_roundtrip(dt), dt)

    def test_decimal_roundtrip(self):
        for s in ["0", "1", "-1", "3.14159", "-0.001", "12345678901234567890"]:
            d = decimal.Decimal(s)
            self.assertEqual(_roundtrip(d), d, s)

    def test_decimal_parts_scale_and_positive_exponent(self):
        # 3.14 -> mantissa 314, scale 2.
        self.assertEqual(_decimal_parts(decimal.Decimal("3.14")), (314, 2))
        # 1E+2 has a positive exponent; it must fold into the mantissa so the
        # scale stays unsigned.
        self.assertEqual(_decimal_parts(decimal.Decimal("1E+2")), (100, 0))

    def test_decimal_non_finite_rejected(self):
        with self.assertRaises(skaidb.ProgrammingError):
            _decimal_parts(decimal.Decimal("NaN"))

    def test_array_and_nested_document_roundtrip(self):
        # The whole reason prepared binding exists: arrays and nested documents
        # that have no SQL literal form.
        self.assertEqual(_roundtrip(["a", "b", "c"]), ["a", "b", "c"])
        self.assertEqual(_roundtrip((1, 2, 3)), [1, 2, 3])  # tuple -> Array
        doc = {"name": "ada", "tags": ["x", "y"], "meta": {"age": 30}}
        self.assertEqual(_roundtrip(doc), doc)

    def test_golden_byte_layouts(self):
        # Byte-exact against the Rust skaidb-types codec (tag + little-endian
        # payload). If these change, the wire format diverged.
        self.assertEqual(_encode_value(None), bytes([0]))
        self.assertEqual(_encode_value(True), bytes([1, 1]))
        self.assertEqual(_encode_value(1), bytes([2]) + (1).to_bytes(8, "little"))
        self.assertEqual(
            _encode_value("ab"), bytes([5]) + struct.pack("<I", 2) + b"ab"
        )
        # Array([1]) -> tag 9, count 1, then Int(1) frame.
        self.assertEqual(
            _encode_value([1]),
            bytes([9]) + struct.pack("<I", 1) + bytes([2]) + (1).to_bytes(8, "little"),
        )

    def test_unbindable_type_raises(self):
        with self.assertRaises(skaidb.ProgrammingError):
            _encode_value(object())


class _StubConn(skaidb.Connection):
    """A Connection that never touches a socket: records request frames and
    replays canned response frames."""

    def __init__(self):  # bypass socket connect + handshake
        import threading

        self._lock = threading.Lock()
        self.closed = False
        self._broken = False
        self._prepared = {}
        self.sent = []
        self._replies = []

    def queue_reply(self, payload: bytes):
        self._replies.append(payload)

    def _roundtrip(self, req: bytes) -> "_Reader":
        self.sent.append(req)
        return _Reader(self._replies.pop(0))


def _resp_prepared(stmt_id: int, nparams: int) -> bytes:
    return bytes([skaidb._RESP_PREPARED]) + struct.pack("<I", stmt_id) + struct.pack(
        "<H", nparams
    )


def _resp_mutation(affected: int) -> bytes:
    return bytes([skaidb._RESP_MUTATION]) + struct.pack("<Q", affected)


class PreparedFraming(unittest.TestCase):
    def test_execute_params_prepares_then_executes(self):
        conn = _StubConn()
        conn.queue_reply(_resp_prepared(7, 1))  # OP_PREPARE reply
        conn.queue_reply(_resp_mutation(1))  # OP_EXECUTE reply

        kind, affected, _ = conn._execute_params(
            "INSERT INTO t (id, tags) VALUES (1, ?)", [["work", "home"]], 1
        )
        self.assertEqual((kind, affected), ("mutation", 1))
        self.assertEqual(len(conn.sent), 2)

        # First frame: OP_PREPARE | u32 len | sql.
        prep = conn.sent[0]
        self.assertEqual(prep[0], _OP_PREPARE)
        (slen,) = struct.unpack_from("<I", prep, 1)
        self.assertEqual(prep[5 : 5 + slen].decode(), "INSERT INTO t (id, tags) VALUES (1, ?)")

        # Second frame: OP_EXECUTE | consistency | u32 id | u16 nparams |
        # (u32 len | value bytes). The one param is the array ['work','home'].
        exe = conn.sent[1]
        self.assertEqual(exe[0], _OP_EXECUTE)
        self.assertEqual(exe[1], 1)  # consistency
        (stmt_id,) = struct.unpack_from("<I", exe, 2)
        (nparams,) = struct.unpack_from("<H", exe, 6)
        self.assertEqual((stmt_id, nparams), (7, 1))
        (vlen,) = struct.unpack_from("<I", exe, 8)
        value = exe[12 : 12 + vlen]
        self.assertEqual(_decode_value(_Reader(value)), ["work", "home"])

        # The prepared id is cached for reuse.
        self.assertIn("INSERT INTO t (id, tags) VALUES (1, ?)", conn._prepared)

    def test_reused_statement_skips_reprepare(self):
        conn = _StubConn()
        conn._prepared["SELECT * FROM t WHERE id = ?"] = (3, 1)
        conn.queue_reply(_resp_mutation(0))  # only an execute reply is needed
        conn._execute_params("SELECT * FROM t WHERE id = ?", [1], 1)
        self.assertEqual(len(conn.sent), 1)  # no OP_PREPARE round-trip
        self.assertEqual(conn.sent[0][0], _OP_EXECUTE)

    def test_arity_mismatch_raises(self):
        conn = _StubConn()
        conn.queue_reply(_resp_prepared(1, 2))
        with self.assertRaises(skaidb.ProgrammingError):
            conn._execute_params("SELECT * FROM t WHERE a = ? AND b = ?", [1], 1)

    def test_unpreparable_falls_back_to_text(self):
        conn = _StubConn()
        conn.queue_reply(
            bytes([skaidb._RESP_ERROR])
            + struct.pack("<I", len(b"statement kind cannot be prepared"))
            + b"statement kind cannot be prepared"
        )
        conn.queue_reply(_resp_mutation(1))  # the OP_QUERY fallback reply
        kind, affected, _ = conn._execute_params("SET CONFIG a.b = ?", [5], 1)
        self.assertEqual((kind, affected), ("mutation", 1))
        # Two frames: the failed prepare, then a one-shot OP_QUERY with the
        # value interpolated into the text.
        self.assertEqual(len(conn.sent), 2)
        self.assertEqual(conn.sent[1][0], skaidb._OP_QUERY)


class Endpoints(unittest.TestCase):
    def test_parse_endpoint(self):
        self.assertEqual(skaidb._parse_endpoint("h", 7000), ("h", 7000))
        self.assertEqual(skaidb._parse_endpoint("h:7005", 7000), ("h", 7005))
        self.assertEqual(skaidb._parse_endpoint(" h.x:1 ", 7000), ("h.x", 1))

    def test_resolve_endpoints(self):
        self.assertEqual(skaidb._resolve_endpoints("h", 7000, None), [("h", 7000)])
        got = skaidb._resolve_endpoints("h", 7000, ["a", "b:2"])
        self.assertEqual(sorted(got), [("a", 7000), ("b", 2)])


class _FakeConn:
    """Stand-in connection for pool tests: no socket, controllable health."""

    def __init__(self):
        self.closed = False
        self.broken = False

    def is_usable(self):
        return not self.closed and not self.broken

    def close(self):
        self.closed = True


class Pool(unittest.TestCase):
    def _pool(self):
        # Patch connect() so the pool mints _FakeConns instead of dialing.
        self._made = []

        def fake_connect(**kwargs):
            c = _FakeConn()
            self._made.append(c)
            return c

        p = skaidb.ConnectionPool(maxsize=2)
        p._orig_connect = skaidb.connect
        skaidb.connect = fake_connect
        self.addCleanup(setattr, skaidb, "connect", p._orig_connect)
        return p

    def test_checkout_checkin_reuse(self):
        p = self._pool()
        c1 = p.getconn()
        p.putconn(c1)
        c2 = p.getconn()
        self.assertIs(c1, c2)  # reused, not a fresh dial
        self.assertEqual(len(self._made), 1)

    def test_broken_connection_discarded_on_checkin(self):
        p = self._pool()
        c1 = p.getconn()
        c1.broken = True
        p.putconn(c1)
        self.assertTrue(c1.closed)  # not retained
        c2 = p.getconn()
        self.assertIsNot(c1, c2)  # a fresh one was dialed

    def test_maxsize_caps_idle(self):
        p = self._pool()  # maxsize=2
        conns = [p.getconn() for _ in range(3)]
        for c in conns:
            p.putconn(c)
        self.assertEqual(len(p._idle), 2)  # third was closed, not pooled
        self.assertTrue(conns[2].closed)

    def test_context_manager_returns_and_discards(self):
        p = self._pool()
        with p.connection() as c:
            self.assertTrue(c.is_usable())
        self.assertEqual(len(p._idle), 1)  # returned on clean exit

        with self.assertRaises(RuntimeError):
            with p.connection() as c:  # reuses the idle conn from above
                c.broken = True  # broke mid-use
                raise RuntimeError("boom")
        # The checked-out conn was broken, so it was closed rather than
        # returned — the pool is left with no idle connections.
        self.assertEqual(len(p._idle), 0)
        self.assertTrue(c.closed)

    def test_close_drains_idle(self):
        p = self._pool()
        c = p.getconn()
        p.putconn(c)
        p.close()
        self.assertTrue(c.closed)
        with self.assertRaises(skaidb.ProgrammingError):
            p.getconn()


if __name__ == "__main__":
    unittest.main()
