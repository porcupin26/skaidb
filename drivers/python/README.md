# skaidb — Python driver

A [DB-API 2.0 (PEP 249)](https://peps.python.org/pep-0249/) driver. If you've
used `sqlite3` or `psycopg2`, you already know this API. **Pure standard
library** — no dependencies.

## Install

```sh
pip install ./drivers/python      # from the repo
# or just copy the `skaidb/` package next to your code
```

## Use

```python
import skaidb

conn = skaidb.connect(host="localhost", port=7000,
                      user="skaidb", password="secret")
cur = conn.cursor()

cur.execute("CREATE TABLE users (PRIMARY KEY (id))")
cur.execute("INSERT INTO users (id, name) VALUES (?, ?)", (1, "Ada"))

cur.execute("SELECT id, name FROM users WHERE id = ?", (1,))
print(cur.fetchone())     # (1, 'Ada')
print(cur.description)    # column metadata

conn.close()
```

- **Placeholders** use `?` (the `qmark` style, like `sqlite3`). A parameterized
  statement is **prepared** on the server and its values are bound as typed
  values over the binary protocol — so `?` can carry a `list` (→ Array) or a
  nested `dict` (→ Document), which have no SQL literal form, and `"O'Brien"`
  needs no escaping. Prepared statements are cached per connection and reused.
  A common use is set membership: `WHERE id IN (?)` bound to a `list` fetches
  those ids in one shot. (Statement kinds the server won't prepare — DDL and
  session control — fall back to client-side text binding for scalar params.)
- `connect()` runs the SCRAM-SHA-256 handshake automatically. Omit
  `user`/`password` for a server with auth disabled.
- Cursors are iterable; `fetchone()`, `fetchmany(n)`, `fetchall()`, `rowcount`,
  and `description` all behave per PEP 249.
- Connections and cursors are context managers (`with skaidb.connect(...) as c:`).

### Connecting: database, seeds, timeouts

```python
# Land the session in a database (runs USE as part of connecting):
conn = skaidb.connect(host="db1", database="app")

# Multi-seed failover: endpoints are tried in randomized order until one
# connects (skaidb is leaderless — any seed serves any request):
conn = skaidb.connect(seeds=["db1", "db2:7000", "db3"], database="app")

# Separate dial vs read timeouts (a read timeout can sit above the server's
# statement timeout without also slowing dial failures):
conn = skaidb.connect(host="db1", connect_timeout=2, read_timeout=120)
```

`conn.is_usable()` is a cheap (no round-trip) check that a connection has not
been left out of sync by a transport error; `conn.ping()` does a real
round-trip liveness check; `conn.reconnect()` re-dials (failing over across
seeds) and re-authenticates.

### Connection pool

Thread-safe, with the same keyword arguments as `connect()` (so pooled
connections inherit multi-seed failover and `database=`). Broken connections
are discarded on checkin and replaced transparently.

```python
pool = skaidb.pool(seeds=["db1", "db2", "db3"], database="app", maxsize=8)

with pool.connection() as conn:          # checked out, returned on exit
    conn.execute("SELECT ... WHERE id IN (?)", ([1, 2, 3],))

pool.close()                             # closes idle connections
```

### Consistency

skaidb is leaderless with tunable consistency. Default is `QUORUM`:

```python
conn = skaidb.connect(..., consistency="ONE")      # or "QUORUM" / "ALL"
cur.set_consistency("ALL")                          # per-cursor override
```

### Types

| skaidb     | Python                         |
|------------|--------------------------------|
| Null       | `None`                         |
| Bool       | `bool`                         |
| Int        | `int`                          |
| Float      | `float`                        |
| Decimal    | `decimal.Decimal`              |
| String     | `str`                          |
| Bytes      | `bytes`                        |
| Uuid       | `uuid.UUID`                    |
| Timestamp  | `datetime.datetime` (UTC)      |
| Array      | `list`                         |
| Document   | `dict`                         |

> skaidb auto-commits each statement (non-transactional), so `commit()` is a
> no-op and `rollback()` raises. There is no multi-statement transaction.

## Run the example

```sh
python3 example.py 192.168.7.117 7000 skaidb secret
```
