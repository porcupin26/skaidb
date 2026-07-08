#!/usr/bin/env python3
"""Full-text search A/B harness: skaidb SEARCH INDEX vs Elasticsearch, same
corpus, same query sets, matched knobs (1 shard / RF=1, standard analyzer,
1 s refresh, per-batch durability). Pure stdlib; runs one persistent
connection per system's canonical protocol (skaidb binary, ES HTTP
keep-alive).

Usage:
  fts_bench.py skaidb <host:7000> setup|ingest|query|nrt|count <data_dir> [batch]
  fts_bench.py es     <host:9200> setup|ingest|query|nrt|count <data_dir> [batch]

`<data_dir>` holds corpus.jsonl + queries.json (from fts_corpus.py).
Ingest reads the whole corpus; query runs every query in queries.json
top-10-ranked and prints p50/p95/p99 per class.

skaidb credentials via SKAIDB_USER / SKAIDB_PASSWORD env (SCRAM).
"""

import hashlib
import hmac
import http.client
import json
import os
import socket
import struct
import sys
import time

# ---- skaidb binary protocol (same wire code as skaidb_bench.py) ----

T_START, T_CHALLENGE, T_FINISH, T_OUTCOME = 10, 11, 12, 13
OP_QUERY, RESP_ROWS, RESP_ERROR = 1, 0, 3


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


def skaidb_connect(addr):
    user = os.environ.get("SKAIDB_USER", "skaidb")
    password = os.environ.get("SKAIDB_PASSWORD", "skaidbClu5ter")
    host, port = addr.rsplit(":", 1)
    sock = socket.create_connection((host, int(port)), timeout=30)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
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
    assert c.u8() == T_OUTCOME and c.u8() == 1, "auth denied"
    return sock


def skaidb_execute(sock, sql):
    write_frame(sock, bytes([OP_QUERY, 1]) + _put_bytes(sql.encode()))
    resp = read_frame(sock)
    if resp[0] == RESP_ERROR:
        c = Cursor(resp)
        c.u8()
        raise RuntimeError(f"skaidb error for {sql[:120]}...: {c.bytes_().decode(errors='replace')}")
    return resp


def skaidb_row_count(resp):
    """Row count of a RESP_ROWS frame: tag | columns (u32 n + n×bytes) | u32."""
    c = Cursor(resp)
    if c.u8() != RESP_ROWS:
        return 0
    for _ in range(c.u32()):
        c.bytes_()
    return c.u32()


def q(text):
    """SQL single-quote a corpus string."""
    return "'" + text.replace("'", "''") + "'"


# ---- Elasticsearch over persistent HTTP ----


class Es:
    """Persistent HTTP client (used for Elasticsearch, and for skaidb's
    REST gateway in the parity check — pass `basic` credentials there)."""

    def __init__(self, addr, basic=None):
        host, port = addr.rsplit(":", 1)
        self.conn = http.client.HTTPConnection(host, int(port), timeout=120)
        self.headers = {}
        if basic:
            import base64

            token = base64.b64encode(basic.encode()).decode()
            self.headers["Authorization"] = f"Basic {token}"

    def call(self, method, path, body=None, ctype="application/json"):
        payload = None
        if body is not None:
            payload = body if isinstance(body, (bytes, str)) else json.dumps(body)
        self.conn.request(
            method, path, body=payload, headers={"Content-Type": ctype, **self.headers}
        )
        resp = self.conn.getresponse()
        data = resp.read()
        if resp.status >= 300 and not (method == "DELETE" and resp.status == 404):
            raise RuntimeError(f"ES {method} {path}: {resp.status} {data[:300]}")
        return json.loads(data) if data else {}


# ---- workload ----


def load_corpus(data_dir):
    with open(f"{data_dir}/corpus.jsonl") as fh:
        return [json.loads(line) for line in fh]


def percentiles(samples):
    samples = sorted(samples)
    pct = lambda p: samples[min(len(samples) - 1, int(len(samples) * p))]
    return pct(0.50) * 1e3, pct(0.95) * 1e3, pct(0.99) * 1e3


def run_queries(label, queries, run_one):
    # Warm-up (JIT, page cache, connection) outside the timed set.
    for query in queries[:5]:
        run_one(query)
    lats, hits = [], 0
    for query in queries:
        t = time.perf_counter()
        hits += run_one(query)
        lats.append(time.perf_counter() - t)
    p50, p95, p99 = percentiles(lats)
    print(
        f"  {label:<8} n={len(queries):<4} p50 {p50:7.2f} ms  p95 {p95:7.2f} ms  "
        f"p99 {p99:7.2f} ms  ({hits} hits total)"
    )


def parity(skaidb_addr, es_addr, data_dir):
    """Result-set parity (docs/FTS_TODO.md phase-3 exit): run the same
    query sets on both engines, compare top-10 id sets per query, report
    mean overlap per class. skaidb over REST (easy JSON ids), ES as usual.
    Overlap = |A∩B| / min(|A|,|B|) so a class where both engines find
    fewer than 10 hits isn't penalized for the shorter list."""
    user = os.environ.get("SKAIDB_USER", "skaidb")
    password = os.environ.get("SKAIDB_PASSWORD", "skaidbClu5ter")
    rest = Es(skaidb_addr, basic=f"{user}:{password}")  # skaidb REST gateway
    es = Es(es_addr)
    queries = json.load(open(f"{data_dir}/queries.json"))

    def skaidb_ids(where, k):
        out = rest.call(
            "POST", "/query",
            f"SELECT id FROM articles WHERE {where} ORDER BY score() DESC LIMIT {k}",
        )
        return [row[0] for row in out["rows"]]

    def es_ids(query, k):
        out = es.call("POST", "/articles/_search",
                      {"size": k, "_source": False, "query": query})
        return [int(h["_id"]) for h in out["hits"]["hits"]]

    classes = {
        "term": [(f"MATCH(body, {q(w)})", {"match": {"body": w}}) for w in queries["term"]],
        "and": [(
            f"SEARCH('+body:{a} +body:{b}')",
            {"bool": {"must": [{"match": {"body": a}}, {"match": {"body": b}}]}},
        ) for a, b in queries["and"]],
        "or": [(f"MATCH(body, {q(a + ' ' + b)})", {"match": {"body": f"{a} {b}"}})
               for a, b in queries["or"]],
        "phrase": [(f"MATCH_PHRASE(body, {q(p)})", {"match_phrase": {"body": p}})
                   for p in queries["phrase"]],
    }
    grand, grand_tie = [], []
    for label, pairs in classes.items():
        overlaps, tie_overlaps, low = [], [], 0
        for where, query in pairs:
            a15, b15 = skaidb_ids(where, 15), es_ids(query, 15)
            a10, b10 = set(a15[:10]), set(b15[:10])
            if not a10 and not b10:
                overlaps.append(1.0)
                tie_overlaps.append(1.0)
                continue
            denom = min(len(a10), len(b10)) or max(len(a10), len(b10))
            ov = len(a10 & b10) / denom
            overlaps.append(ov)
            low += ov < 0.8
            # Tie-tolerant containment: each engine's top-10 found within
            # the other's top-15 — separates genuinely missing results
            # from near-tied docs flipping order at the k=10 cutoff.
            contain = (
                len(a10 & set(b15)) + len(b10 & set(a15))
            ) / max(1, len(a10) + len(b10))
            tie_overlaps.append(contain)
        mean = sum(overlaps) / len(overlaps)
        tie = sum(tie_overlaps) / len(tie_overlaps)
        grand.extend(overlaps)
        grand_tie.extend(tie_overlaps)
        print(
            f"  {label:<8} strict@10 {mean*100:5.1f}%   @10-in-15 {tie*100:5.1f}%"
            f"   (<80% strict on {low}/{len(overlaps)})"
        )
    print(
        f"  overall  strict@10 {sum(grand)/len(grand)*100:5.1f}%"
        f"   @10-in-15 {sum(grand_tie)/len(grand_tie)*100:5.1f}%"
    )


def main():
    if sys.argv[1] == "parity":
        parity(sys.argv[2], sys.argv[3], sys.argv[4])
        return
    system, addr, phase, data_dir = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    batch = int(sys.argv[5]) if len(sys.argv) > 5 else 1000

    if system == "skaidb":
        sock = skaidb_connect(addr)

        if phase == "setup":
            try:
                skaidb_execute(sock, "DROP SEARCH INDEX IF EXISTS articles_fts")
                skaidb_execute(sock, "DROP TABLE IF EXISTS articles")
            except RuntimeError:
                pass
            skaidb_execute(sock, "CREATE TABLE articles (PRIMARY KEY (id))")
            skaidb_execute(
                sock,
                "CREATE SEARCH INDEX articles_fts ON articles (title, body)",
            )
            print("skaidb: table + search index ready")

        elif phase == "ingest":
            corpus = load_corpus(data_dir)
            t0 = time.perf_counter()
            for i in range(0, len(corpus), batch):
                chunk = corpus[i : i + batch]
                values = ",".join(
                    f"({d['id']}, {q(d['title'])}, {q(d['body'])})" for d in chunk
                )
                skaidb_execute(
                    sock, f"INSERT INTO articles (id, title, body) VALUES {values}"
                )
            secs = time.perf_counter() - t0
            print(f"skaidb ingest: {len(corpus)} docs in {secs:.1f}s = {len(corpus)/secs:,.0f} docs/s")

        elif phase == "query":
            queries = json.load(open(f"{data_dir}/queries.json"))
            tail = " ORDER BY score() DESC LIMIT 10"

            def one(sql):
                return skaidb_row_count(skaidb_execute(sock, sql))

            run_queries("term", queries["term"], lambda w: one(
                f"SELECT id FROM articles WHERE MATCH(body, {q(w)}){tail}"))
            run_queries("and", queries["and"], lambda ab: one(
                f"SELECT id FROM articles WHERE SEARCH('+body:{ab[0]} +body:{ab[1]}'){tail}"))
            run_queries("or", queries["or"], lambda ab: one(
                f"SELECT id FROM articles WHERE MATCH(body, {q(ab[0] + ' ' + ab[1])}){tail}"))
            run_queries("phrase", queries["phrase"], lambda p: one(
                f"SELECT id FROM articles WHERE MATCH_PHRASE(body, {q(p)}){tail}"))

        elif phase == "nrt":
            # Visibility from a second connection (the NRT read path, not
            # the coordinator's read-your-writes).
            reader = skaidb_connect(addr)
            marker = f"nrtprobe{int(time.time())}"
            t0 = time.perf_counter()
            skaidb_execute(
                sock,
                f"INSERT INTO articles (id, title, body) VALUES (9999999, 'probe', {q(marker)})",
            )
            sel = f"SELECT id FROM articles WHERE MATCH(body, {q(marker)}) ORDER BY score() DESC LIMIT 1"
            while skaidb_row_count(skaidb_execute(reader, sel)) == 0:
                time.sleep(0.02)
            print(f"skaidb NRT visibility: {(time.perf_counter()-t0)*1e3:.0f} ms")

        elif phase == "count":
            resp = skaidb_execute(sock, "SELECT COUNT(*) FROM articles")
            print(f"skaidb count frame: {len(resp)} bytes (nonzero = rows present)")

    elif system == "es":
        es = Es(addr)

        if phase == "setup":
            es.call("DELETE", "/articles")
            es.call(
                "PUT",
                "/articles",
                {
                    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                    "mappings": {
                        "properties": {
                            "title": {"type": "text"},
                            "body": {"type": "text"},
                        }
                    },
                },
            )
            print("es: index ready")

        elif phase == "ingest":
            corpus = load_corpus(data_dir)
            t0 = time.perf_counter()
            for i in range(0, len(corpus), batch):
                chunk = corpus[i : i + batch]
                lines = []
                for d in chunk:
                    lines.append(json.dumps({"index": {"_id": d["id"]}}))
                    lines.append(json.dumps({"title": d["title"], "body": d["body"]}))
                out = es.call("POST", "/articles/_bulk", "\n".join(lines) + "\n",
                              ctype="application/x-ndjson")
                if out.get("errors"):
                    raise RuntimeError("bulk errors")
            secs = time.perf_counter() - t0
            print(f"es ingest: {len(corpus)} docs in {secs:.1f}s = {len(corpus)/secs:,.0f} docs/s")

        elif phase == "query":
            queries = json.load(open(f"{data_dir}/queries.json"))

            def search(query):
                out = es.call("POST", "/articles/_search",
                              {"size": 10, "_source": False, "query": query})
                return len(out["hits"]["hits"])

            run_queries("term", queries["term"], lambda w: search({"match": {"body": w}}))
            run_queries("and", queries["and"], lambda ab: search(
                {"bool": {"must": [{"match": {"body": ab[0]}}, {"match": {"body": ab[1]}}]}}))
            run_queries("or", queries["or"], lambda ab: search({"match": {"body": f"{ab[0]} {ab[1]}"}}))
            run_queries("phrase", queries["phrase"], lambda p: search({"match_phrase": {"body": p}}))

        elif phase == "nrt":
            marker = f"nrtprobe{int(time.time())}"
            t0 = time.perf_counter()
            es.call("PUT", "/articles/_doc/9999999", {"title": "probe", "body": marker})
            body = {"size": 1, "_source": False, "query": {"match": {"body": marker}}}
            while True:
                out = es.call("POST", "/articles/_search", body)
                if out["hits"]["hits"]:
                    break
                time.sleep(0.02)
            print(f"es NRT visibility: {(time.perf_counter()-t0)*1e3:.0f} ms")

        elif phase == "count":
            out = es.call("GET", "/articles/_count")
            print(f"es count: {out['count']}")

    else:
        sys.exit(f"unknown system {system}")


if __name__ == "__main__":
    main()
