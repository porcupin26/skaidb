#!/usr/bin/env python3
"""Load generator for a MariaDB primary (comparison benchmark for skaidb).

Usage:
    maria_bench.py <host> <write|read|mixed> <ops> <threads> [preload]

Env:
    MARIA_PORT (3306), MARIA_DB (bench), MARIA_USER (skaidb), MARIA_PASS (changeme)

Write durability is set on the server: semi-synchronous replication
(rpl_semi_sync_master_enabled) makes the primary wait for one replica ack
(≈ 2-of-N); turning it off makes writes primary-only. Each op is autocommit.
"""
import os
import random
import sys
import threading
import time

import pymysql

host, mode, ops, threads = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
preload = int(sys.argv[5]) if len(sys.argv) > 5 else 1000


def conn():
    return pymysql.connect(
        host=host,
        port=int(os.environ.get("MARIA_PORT", "3306")),
        user=os.environ.get("MARIA_USER", "skaidb"),
        password=os.environ.get("MARIA_PASS", "changeme"),
        database=os.environ.get("MARIA_DB", "bench"),
        autocommit=True,
    )


c = conn()
cur = c.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS bench (id bigint PRIMARY KEY, v text) ENGINE=InnoDB")
cur.execute("TRUNCATE bench")
if mode != "write":
    cur.executemany(
        "INSERT INTO bench (id,v) VALUES (%s,%s)",
        [(i, f"payload-{i}") for i in range(preload)],
    )
    print(f"preloaded {preload} rows")
c.close()

per = ops // threads
lat, err, lock = [], [0], threading.Lock()


def worker(t):
    c = conn()
    cur = c.cursor()
    lats, e, rng = [], 0, random.Random(t)
    for i in range(per):
        if mode == "read" or (mode == "mixed" and rng.random() < 0.5):
            k = rng.randrange(preload)
            s = time.perf_counter()
            try:
                cur.execute("SELECT v FROM bench WHERE id=%s", (k,))
                cur.fetchone()
            except Exception:
                e += 1
        else:
            _id = preload + t * 10_000_000 + i
            s = time.perf_counter()
            try:
                cur.execute("INSERT INTO bench (id,v) VALUES (%s,%s)", (_id, f"payload-{_id}"))
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
