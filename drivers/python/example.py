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
    # `?` binds typed values over prepared statements: a list becomes an Array
    # and a dict a nested Document — no SQL literal form needed.
    cur.executemany(
        "INSERT INTO people (id, name, age, labels, meta) VALUES (?, ?, ?, ?, ?)",
        [
            (1, "Ada", 36, ["math", "eng"], {"city": "London"}),
            (2, "Linus", 54, ["kernel"], {"city": "Portland"}),
            (3, "Margaret", 80, ["nasa", "sw"], {"city": "Boulder"}),
        ],
    )
    cur.execute("SELECT id, name, age, labels, meta FROM people WHERE age > ?", (40,))
    for row in cur:
        print(row)  # labels come back as list, meta as dict

    # Set membership: bind an array to `IN (?)` to fetch several ids at once.
    cur.execute("SELECT id, name FROM people WHERE id IN (?) ORDER BY id", ([1, 3],))
    print("by ids:", cur.fetchall())

    cur.execute("DROP TABLE people")
