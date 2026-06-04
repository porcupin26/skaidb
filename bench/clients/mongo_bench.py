#!/usr/bin/env python3
"""Load generator for a MongoDB replica set (comparison benchmark for skaidb).

Usage:
    mongo_bench.py <seed1:port,seed2:port,...> <write|read|mixed> <ops> <threads> [preload]

Env:
    MONGO_RS    replica set name (default: rs0)
    MONGO_W     write concern: an int (e.g. 1, 2, 3) or "majority" (default: majority)
    MONGO_DB    database name (default: bench)

Each thread shares the client's connection pool. Writes use the configured
write concern so durability matches the other systems in a given config
(e.g. w=majority ≈ both/quorum, w=1 ≈ primary only, w=3 ≈ all 3).
"""
import os
import random
import sys
import threading
import time

from pymongo import MongoClient, WriteConcern

addr, mode, ops, threads = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
preload = int(sys.argv[5]) if len(sys.argv) > 5 else 1000
rs = os.environ.get("MONGO_RS", "rs0")
w = os.environ.get("MONGO_W", "majority")
w = int(w) if w.isdigit() else w
dbname = os.environ.get("MONGO_DB", "bench")

client = MongoClient(
    f"mongodb://{addr}/?replicaSet={rs}",
    maxPoolSize=threads + 4,
    w=w,
    serverSelectionTimeoutMS=8000,
)
coll = client[dbname].get_collection("bench", write_concern=WriteConcern(w=w))
coll.drop()
if mode != "write":
    coll.insert_many([{"_id": i, "v": f"payload-{i}"} for i in range(preload)])
    print(f"preloaded {preload} rows")

per = ops // threads
lat, err, lock = [], [0], threading.Lock()


def worker(t):
    lats, e, rng = [], 0, random.Random(t)
    for i in range(per):
        if mode == "read" or (mode == "mixed" and rng.random() < 0.5):
            k = rng.randrange(preload)
            s = time.perf_counter()
            try:
                coll.find_one({"_id": k})
            except Exception:
                e += 1
        else:
            _id = preload + t * 10_000_000 + i
            s = time.perf_counter()
            try:
                coll.insert_one({"_id": _id, "v": f"payload-{_id}"})
            except Exception:
                e += 1
        lats.append((time.perf_counter() - s) * 1000)
    with lock:
        lat.extend(lats)
        err[0] += e


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
client.close()
