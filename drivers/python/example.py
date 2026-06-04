"""Runnable example for the skaidb Python driver.

    python3 example.py [host] [port] [user] [password]
"""
import sys
import skaidb

host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
port = int(sys.argv[2]) if len(sys.argv) > 2 else 7000
user = sys.argv[3] if len(sys.argv) > 3 else "anonymous"
pw = sys.argv[4] if len(sys.argv) > 4 else ""

with skaidb.connect(host=host, port=port, user=user, password=pw) as conn:
    cur = conn.cursor()
    cur.execute("CREATE TABLE people (PRIMARY KEY (id))")
    cur.executemany(
        "INSERT INTO people (id, name, age) VALUES (?, ?, ?)",
        [(1, "Ada", 36), (2, "Linus", 54), (3, "Margaret", 80)],
    )
    cur.execute("SELECT id, name, age FROM people WHERE age > ?", (40,))
    for row in cur:
        print(row)
    cur.execute("DROP TABLE people")
