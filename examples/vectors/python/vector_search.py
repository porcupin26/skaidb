"""End-to-end vector search walkthrough: vectorize documents, store them,
index them, and run nearest-neighbor search — all in skaidb.

    python3 vector_search.py [host] [port] [user] [password]

This uses a small deterministic "hashing trick" bag-of-bigrams vectorizer
(see `vectorize()` below) so the example has no ML dependency and produces
the same vectors every run. In a real application, replace `vectorize()`
with a call to a real embedding model/API (OpenAI, Sentence-Transformers,
Cohere, ...) — everything downstream (storing, indexing, searching) is
identical regardless of where the vector comes from.
"""
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "drivers" / "python"))
import skaidb  # noqa: E402

DIM = 32  # embedding dimension — must match CREATE VECTOR INDEX ... DIM


def _fnv1a(s: str) -> int:
    """FNV-1a over UTF-8 bytes — deterministic across processes and Python
    versions, unlike the built-in `hash()`, which is randomly salted per
    process for strings (see PYTHONHASHSEED) and would make this example's
    vectors change on every run."""
    h = 0x811C9DC5
    for b in s.encode("utf-8"):
        h ^= b
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def vectorize(text: str) -> list[float]:
    """Toy embedding: hash each character bigram into one of DIM buckets and
    count occurrences, then L2-normalize (so cosine distance is meaningful).
    Deterministic and dependency-free — NOT a real semantic embedding, but
    similar text does land closer together because it shares bigrams."""
    v = [0.0] * DIM
    text = text.lower()
    for i in range(len(text) - 1):
        bucket = _fnv1a(text[i : i + 2]) % DIM
        v[bucket] += 1.0
    norm = math.sqrt(sum(x * x for x in v))
    return [x / norm for x in v] if norm > 0 else v


def vector_literal(v: list[float]) -> str:
    """Format a vector as a skaidb array literal for inline SQL."""
    return "[" + ", ".join(repr(x) for x in v) + "]"


host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
port = int(sys.argv[2]) if len(sys.argv) > 2 else 7000
user = sys.argv[3] if len(sys.argv) > 3 else "anonymous"
password = sys.argv[4] if len(sys.argv) > 4 else ""

docs = [
    (1, "tech", "The new GPU doubles inference throughput for transformer models."),
    (2, "tech", "Kubernetes autoscaling reduced our cluster's idle compute cost."),
    (3, "cooking", "Simmer the tomato sauce for twenty minutes before adding basil."),
    (4, "cooking", "A cast iron skillet gives the steak a perfect crust."),
    (5, "tech", "The database's read cache cut point-query latency significantly."),
]

with skaidb.connect(host=host, port=port, user=user, password=password) as conn:
    cur = conn.cursor()

    # --- Schema: a normal table plus a vector index on its embedding field ---
    cur.execute("DROP TABLE IF EXISTS docs")
    cur.execute("CREATE TABLE docs (PRIMARY KEY (id))")
    cur.execute(f"CREATE VECTOR INDEX docs_emb ON docs (embedding) DIM {DIM} USING cosine")

    # --- Vectorize each document's text and insert it alongside the text ---
    # The vector is embedded as a literal in the SQL text (arrays aren't a
    # bindable `?` parameter type in this driver — see drivers/PROTOCOL.md §4);
    # id/category/text still go through normal bound parameters.
    for doc_id, category, text in docs:
        vec = vectorize(text)
        cur.execute(
            f"INSERT INTO docs (id, category, text, embedding) "
            f"VALUES (?, ?, ?, {vector_literal(vec)})",
            (doc_id, category, text),
        )
    print(f"indexed {len(docs)} documents")

    # --- Nearest-neighbor search: vectorize the query the same way, then ask
    #     for the k closest documents ---
    query = "GPU memory bandwidth limits model throughput"
    query_vec = vectorize(query)
    cur.execute(
        f"SELECT id, category, text, _distance FROM docs "
        f"NEAREST (embedding, {vector_literal(query_vec)}, 3)"
    )
    print(f"\nnearest to: {query!r}")
    for doc_id, category, text, distance in cur:
        print(f"  [{distance:.3f}] ({category}) {text}")

    # --- Filtered nearest-neighbor search: WHERE narrows the candidate set,
    #     results are still nearest-first ---
    cur.execute(
        f"SELECT id, text, _distance FROM docs "
        f"NEAREST (embedding, {vector_literal(query_vec)}, 3) WHERE category = ?",
        ("cooking",),
    )
    print(f"\nnearest to: {query!r} (category = 'cooking' only)")
    for doc_id, text, distance in cur:
        print(f"  [{distance:.3f}] {text}")

    cur.execute("DROP VECTOR INDEX docs_emb")
    cur.execute("DROP TABLE docs")
