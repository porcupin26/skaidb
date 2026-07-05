"""How to use skaidb from Python — DB-API 2.0 driver (like sqlite3 / psycopg2).

    python3 basic_usage.py [host] [port] [user] [password]

Uses the driver at ../../drivers/python. In a real project, install it with
`pip install -e drivers/python` (or vendor it) instead of the sys.path hack
below, which exists only so this example runs straight out of the repo.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "drivers" / "python"))
import skaidb  # noqa: E402

host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
port = int(sys.argv[2]) if len(sys.argv) > 2 else 7000
user = sys.argv[3] if len(sys.argv) > 3 else "anonymous"
password = sys.argv[4] if len(sys.argv) > 4 else ""

with skaidb.connect(host=host, port=port, user=user, password=password) as conn:
    cur = conn.cursor()

    # --- DDL ---
    cur.execute("DROP TABLE IF EXISTS people")
    cur.execute("CREATE TABLE people (PRIMARY KEY (id))")

    # --- Batch insert with bound parameters (`?`) ---
    # Parameters are quoted client-side (see drivers/PROTOCOL.md §5); never
    # build SQL by string-formatting user input directly.
    cur.executemany(
        "INSERT INTO people (id, name, age) VALUES (?, ?, ?)",
        [(1, "Ada", 36), (2, "Linus", 54), (3, "Margaret", 80)],
    )

    # --- Query ---
    cur.execute("SELECT id, name, age FROM people WHERE age > ? ORDER BY id", (40,))
    print("age > 40:")
    for row in cur:
        print(" ", row)

    # --- Update ---
    cur.execute("UPDATE people SET age = ? WHERE id = ?", (37, 1))
    print(f"updated {cur.rowcount} row(s)")

    # --- Point read by primary key (routed directly to the owning replica) ---
    cur.execute("SELECT name, age FROM people WHERE id = ?", (1,))
    print("id=1:", cur.fetchone())

    # --- Error handling ---
    try:
        cur.execute("SELECT * FROM does_not_exist")
    except skaidb.DatabaseError as e:
        print(f"expected error: {e}")

    # --- Delete + cleanup ---
    cur.execute("DELETE FROM people WHERE id = ?", (2,))
    print(f"deleted {cur.rowcount} row(s)")
    cur.execute("DROP TABLE people")
