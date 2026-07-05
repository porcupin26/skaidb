#!/usr/bin/env python3
"""Load generator for a PostgreSQL primary (comparison benchmark for skaidb).

Usage:
    pg_bench.py <host> <write|read|mixed|writep|readp|mixedp> <ops> <threads> [preload]

The `*p` modes use server-side prepared statements (PREPARE once per
connection, EXECUTE per op) — the fair counterpart to skaidb's prepared
workloads; the plain modes send full SQL text per op (psycopg2 default).

Env:
    PG_PORT (5432), PG_DB (bench), PG_USER (skaidb), PG_PASS (changeme)

Write durability is set on the server, not the client: `synchronous_commit`
plus `synchronous_standby_names` decide how many nodes a commit waits for
(e.g. FIRST 2 ≈ all-3, ANY 1 ≈ quorum, '' ≈ primary only). Each op is its own
autocommit transaction.
"""
import os
import random
import sys
import threading
import time

import io

import psycopg2

def parse_preload(arg):
    """`N` or `NxS` -> (rows, value_size); S=0 keeps the short default value."""
    if arg is None:
        return 1000, 0
    if "x" in arg:
        n, s = arg.split("x", 1)
        return int(n), int(s)
    return int(arg), 0


def payload(i, valsize):
    v = f"payload-{i}"
    return v + "." * max(0, valsize - len(v))


host, mode, ops, threads = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
preload, valsize = parse_preload(sys.argv[5] if len(sys.argv) > 5 else None)
dsn = (
    f"host={host} port={os.environ.get('PG_PORT', '5432')} "
    f"dbname={os.environ.get('PG_DB', 'bench')} "
    f"user={os.environ.get('PG_USER', 'skaidb')} "
    f"password={os.environ.get('PG_PASS', 'changeme')}"
)


def conn():
    c = psycopg2.connect(dsn)
    c.autocommit = True
    return c


c = conn()
cur = c.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS bench (id bigint PRIMARY KEY, v text)")
cur.execute("TRUNCATE bench")
if mode not in ("write", "writep"):
    CHUNK = 100_000
    for base in range(0, preload, CHUNK):
        buf = io.StringIO(
            "".join(f"{i},{payload(i, valsize)}\n" for i in range(base, min(base + CHUNK, preload)))
        )
        cur.copy_expert("COPY bench (id,v) FROM STDIN WITH (FORMAT csv)", buf)
    print(f"preloaded {preload} rows (value ~{valsize}B)")
    if preload >= 100_000:
        print("settling 10s after large preload…")
        time.sleep(10)
c.close()

read_span = min(int(os.environ.get("READ_SPAN", preload) or preload), max(preload, 1))
per = ops // threads
lat, err, lock = [], [0], threading.Lock()


prepared = mode.endswith("p")
base = mode[:-1] if prepared else mode


def worker(t):
    c = conn()
    cur = c.cursor()
    if prepared:
        cur.execute("PREPARE ins (bigint, text) AS INSERT INTO bench (id,v) VALUES ($1,$2)")
        cur.execute("PREPARE sel (bigint) AS SELECT v FROM bench WHERE id=$1")
        read_sql, write_sql = "EXECUTE sel (%s)", "EXECUTE ins (%s,%s)"
    else:
        read_sql, write_sql = (
            "SELECT v FROM bench WHERE id=%s",
            "INSERT INTO bench (id,v) VALUES (%s,%s)",
        )
    lats, e, rng = [], 0, random.Random(t)
    for i in range(per):
        if base == "read" or (base == "mixed" and rng.random() < 0.5):
            k = rng.randrange(read_span)
            s = time.perf_counter()
            try:
                cur.execute(read_sql, (k,))
                cur.fetchone()
            except Exception:
                e += 1
        else:
            _id = preload + t * 10_000_000 + i
            s = time.perf_counter()
            try:
                cur.execute(write_sql, (_id, f"payload-{_id}"))
            except Exception:
                e += 1
        lats.append((time.perf_counter() - s) * 1000)
    with lock:
        lat.extend(lats)
        err[0] += e
    c.close()


start = time.perf_counter()
ts = [threading.Thread(target=worker, args=(t,)) for t in range(threads)]
[t.start() for t in ts]
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
