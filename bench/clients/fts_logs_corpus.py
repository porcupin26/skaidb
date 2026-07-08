#!/usr/bin/env python3
"""Synthetic http_logs-style corpus for the FTS aggregation benchmark
(docs/FTS_TODO.md phase-6 exit) — the ES Rally http_logs *shape* (request
line, method, status, byte size, timestamp) with deterministic generation
so both engines index identical bytes.

Emits:
  logs.jsonl        {"id", "msg", "method", "status", "bytes", "ts"} per line
  agg_queries.json  {"terms": [...]} — MATCH filter terms both engines use

Usage: fts_logs_corpus.py <out_dir> [n_docs]
"""

import json
import random
import sys

SEGMENTS = [
    "images", "static", "api", "checkout", "cart", "search", "login",
    "products", "reviews", "assets", "admin", "reports", "export", "media",
    "docs", "help", "account", "orders", "payment", "wishlist", "catalog",
    "category", "inventory", "profile", "settings", "history", "session",
    "tracking", "banner", "promo", "coupon", "gateway", "webhook", "metrics",
]
FILES = ["index.html", "logo.gif", "app.js", "style.css", "data.json", "img.png"]
METHODS = ["GET"] * 70 + ["POST"] * 15 + ["PUT"] * 5 + ["DELETE"] * 5 + ["HEAD"] * 5
STATUSES = ["200"] * 80 + ["404"] * 10 + ["500"] * 5 + ["301"] * 5


def main() -> None:
    out_dir = sys.argv[1]
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 500_000
    rng = random.Random(0x10C5)
    base_ts = 1_700_000_000_000
    with open(f"{out_dir}/logs.jsonl", "w") as out:
        for i in range(1, n + 1):
            seg1, seg2 = rng.choice(SEGMENTS), rng.choice(SEGMENTS)
            msg = f"{rng.choice(METHODS)} /{seg1}/{seg2}/{rng.choice(FILES)} HTTP/1.1"
            out.write(
                json.dumps(
                    {
                        "id": i,
                        "msg": msg,
                        "method": rng.choice(METHODS),
                        "status": rng.choice(STATUSES),
                        "bytes": int(rng.lognormvariate(8.5, 1.2)),
                        # ~600 ms apart on average → the corpus spans days.
                        "ts": base_ts + i * 600 + rng.randrange(600),
                    }
                )
                + "\n"
            )
    # Filter terms for the MATCH side of aggregation queries: path segments
    # (each matches a few % of the corpus).
    terms = rng.sample(SEGMENTS, 20)
    with open(f"{out_dir}/agg_queries.json", "w") as out:
        json.dump({"terms": terms}, out, indent=1)
    print(f"{n} log docs written")


if __name__ == "__main__":
    main()
