#!/usr/bin/env python3
"""Load generator for skaidb over its binary protocol (SCP).

Usage:
    skaidb_bench.py <addr[,addr2,...]> <user> <pass> <write|read|mixed> <ops> <threads> [preload]

A comma-separated address list spreads threads round-robin across nodes
(leaderless: any node coordinates reads and writes). Mirrors the in-tree Rust
example (`cargo run --example bench -p skaidb-driver`); this reimplements the
wire protocol in Python so skaidb sits in the same harness as the other DBs.

Protocol (see crates/skaidb-proto): each message is a big-endian u32 length
prefix + payload. A connection first runs the SCRAM-SHA-256 handshake, then
sends query requests and reads responses.
"""
import hashlib
import hmac
import os
import random
import socket
import struct
import sys
import threading
import time

# Handshake frame tags / query opcodes (crates/skaidb-proto).
T_START, T_CHALLENGE, T_FINISH, T_OUTCOME = 10, 11, 12, 13
OP_QUERY = 1
RESP_ERROR = 3


def _recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("peer closed")
        buf.extend(chunk)
    return bytes(buf)


def read_frame(sock):
    (length,) = struct.unpack(">I", _recv_exact(sock, 4))
    return _recv_exact(sock, length)


def write_frame(sock, payload):
    sock.sendall(struct.pack(">I", len(payload)) + payload)


def _put_bytes(b):
    return struct.pack("<I", len(b)) + b


class Cursor:
    def __init__(self, buf):
        self.buf, self.pos = buf, 0

    def take(self, n):
        s = self.buf[self.pos : self.pos + n]
        self.pos += n
        return s

    def u8(self):
        return self.take(1)[0]

    def u32(self):
        return struct.unpack("<I", self.take(4))[0]

    def bytes_(self):
        return self.take(self.u32())


def handshake(sock, user, password):
    nonce = "c" + os.urandom(8).hex()
    write_frame(sock, bytes([T_START]) + _put_bytes(user.encode()) + _put_bytes(nonce.encode()))

    c = Cursor(read_frame(sock))
    assert c.u8() == T_CHALLENGE, "expected challenge"
    salt = c.bytes_()
    iterations = c.u32()
    server_nonce = c.bytes_().decode()

    auth_message = f"{user}\0{nonce}\0{server_nonce}\0{salt.hex()}\0{iterations}".encode()
    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations)
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    client_sig = hmac.new(stored_key, auth_message, hashlib.sha256).digest()
    proof = bytes(a ^ b for a, b in zip(client_key, client_sig))
    write_frame(sock, bytes([T_FINISH]) + proof)

    c = Cursor(read_frame(sock))
    assert c.u8() == T_OUTCOME, "expected outcome"
    if c.u8() != 1:
        raise PermissionError("auth denied: " + c.bytes_().decode(errors="replace"))


def connect(addr, user, password):
    host, port = addr.rsplit(":", 1)
    sock = socket.create_connection((host, int(port)), timeout=10)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    handshake(sock, user, password)
    return sock


def execute(sock, sql):
    """Send a query; return True on success, False on a server error."""
    payload = bytes([OP_QUERY, 1]) + _put_bytes(sql.encode())  # consistency byte ignored by server
    write_frame(sock, payload)
    resp = read_frame(sock)
    return resp[0] != RESP_ERROR


addrs = [a.strip() for a in sys.argv[1].split(",")]
user, password, mode = sys.argv[2], sys.argv[3], sys.argv[4]
ops, threads = int(sys.argv[5]), int(sys.argv[6])
preload = int(sys.argv[7]) if len(sys.argv) > 7 else 1000

# Fresh table; preload rows for read/mixed.
s = connect(addrs[0], user, password)
execute(s, "DROP TABLE IF EXISTS bench")
assert execute(s, "CREATE TABLE bench (PRIMARY KEY (id))"), "create failed"
if mode != "write":
    for i in range(preload):
        execute(s, f"INSERT INTO bench (id, v) VALUES ({i}, 'payload-{i}')")
    print(f"preloaded {preload} rows")
s.close()

per = ops // threads
lat, err, lock = [], [0], threading.Lock()


barrier = threading.Barrier(threads + 1)


def worker(t):
    # Connect + SCRAM handshake OUTSIDE the timed window; the main
    # thread starts the clock at the barrier.
    sock = connect(addrs[t % len(addrs)], user, password)
    barrier.wait()
    lats, e, rng = [], 0, random.Random(t)
    for i in range(per):
        if mode == "read" or (mode == "mixed" and rng.random() < 0.5):
            k = rng.randrange(preload)
            sql = f"SELECT v FROM bench WHERE id = {k}"
        else:
            _id = preload + t * 10_000_000 + i
            sql = f"INSERT INTO bench (id, v) VALUES ({_id}, 'payload-{_id}')"
        st = time.perf_counter()
        try:
            if not execute(sock, sql):
                e += 1
        except Exception:
            e += 1
        lats.append((time.perf_counter() - st) * 1000)
    with lock:
        lat.extend(lats)
        err[0] += e
    sock.close()


ts = [threading.Thread(target=worker, args=(t,)) for t in range(threads)]
[t.start() for t in ts]
barrier.wait()  # all workers connected; ops start now
start = time.perf_counter()
[t.join() for t in ts]
el = time.perf_counter() - start
lat.sort()
n = len(lat)
pct = lambda p: lat[min(int(n * p), n - 1)]
print(f"throughput : {n / el:.0f} ops/s")
print(
    f"latency ms : avg {sum(lat) / n:.2f}  p50 {pct(.5):.2f}  p95 {pct(.95):.2f}  "
    f"p99 {pct(.99):.2f}  max {pct(1.0):.2f}  errors {err[0]}"
)
