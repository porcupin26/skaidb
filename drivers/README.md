# skaidb drivers

Official client libraries for skaidb. Each one is written to feel **exactly like
the database driver you already use in that language** — so there's essentially
no new API to learn — and each speaks skaidb's binary fast-path protocol
directly (SCRAM-SHA-256 auth, parameterized queries, full type fidelity).

All drivers are **dependency-free** (standard library only) and small enough to
read in one sitting.

| Language | Directory | Modeled on | Placeholders | Status |
|----------|-----------|------------|--------------|--------|
| Python | [`python/`](python/) | DB-API 2.0 (`sqlite3` / `psycopg2`) | `?` | ✅ live-tested |
| Node.js / TS | [`nodejs/`](nodejs/) | `pg` (node-postgres) | `$1, $2` | ✅ live-tested |
| Go | [`go/`](go/) | `database/sql` | `?` | ✅ live-tested |
| Java | [`java/`](java/) | JDBC | `?` (1-based) | ✅ live-tested |
| Ruby | [`ruby/`](ruby/) | `pg` gem | `$1, $2` | ⚙️ spec-verified |
| PHP | [`php/`](php/) | PDO | `?` | ⚙️ spec-verified |
| C# / .NET | [`dotnet/`](dotnet/) | ADO.NET | `?` | ⚙️ spec-verified |
| Rust | [`../crates/skaidb-driver`](../crates/skaidb-driver) | native | n/a | ✅ in-tree |

> **live-tested** = verified end-to-end against a running 3-node cluster.
> **spec-verified** = implemented against the shared [`PROTOCOL.md`](PROTOCOL.md)
> and cross-checked byte-for-byte with the live-tested Python reference, but the
> runtime wasn't available in the authoring environment to execute it.

The wire protocol they all implement is documented in **[PROTOCOL.md](PROTOCOL.md)** —
read that if you want to write a driver for another language.

## 30-second tour

Same task in each language: connect, insert with a bound parameter, select.

**Python**
```python
import skaidb
conn = skaidb.connect(host="localhost", port=7000, user="skaidb", password="secret")
cur = conn.cursor()
cur.execute("INSERT INTO users (id, name) VALUES (?, ?)", (1, "Ada"))
cur.execute("SELECT name FROM users WHERE id = ?", (1,))
print(cur.fetchone())            # (1, 'Ada')
```

**Node.js**
```js
const { Client } = require('skaidb');
const client = new Client({ host: 'localhost', port: 7000, user: 'skaidb', password: 'secret' });
await client.connect();
await client.query('INSERT INTO users (id, name) VALUES ($1, $2)', [1, 'Ada']);
const { rows } = await client.query('SELECT name FROM users WHERE id = $1', [1]);
```

**Go**
```go
db, _ := sql.Open("skaidb", "skaidb://skaidb:secret@localhost:7000/")
db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", 1, "Ada")
rows, _ := db.Query("SELECT name FROM users WHERE id = ?", 1)
```

**Java**
```java
try (Skaidb.Connection conn = Skaidb.connect("skaidb://skaidb:secret@localhost:7000")) {
    conn.prepare("INSERT INTO users (id, name) VALUES (?, ?)").setInt(1, 1).setString(2, "Ada").executeUpdate();
}
```

**Ruby**
```ruby
conn = Skaidb.connect(host: "localhost", port: 7000, user: "skaidb", password: "secret")
conn.exec_params("INSERT INTO users (id, name) VALUES ($1, $2)", [1, "Ada"])
```

**PHP**
```php
$db = new Skaidb\Connection("localhost", 7000, "skaidb", "secret");
$db->prepare("INSERT INTO users (id, name) VALUES (?, ?)")->execute([1, "Ada"]);
```

**C#**
```csharp
using var conn = new SkaidbConnection("Host=localhost;Port=7000;User=skaidb;Password=secret");
conn.Open();
var cmd = conn.CreateCommand();
cmd.CommandText = "INSERT INTO users (id, name) VALUES (?, ?)";
cmd.Parameters.Add(1); cmd.Parameters.Add("Ada");
cmd.ExecuteNonQuery();
```

## Common notes

- **Parameters are bound client-side** (the protocol has no server bind params).
  Each driver quotes and escapes arguments safely, so `"O'Brien"` just works and
  SQL injection through bound values is prevented. Use placeholders — don't
  string-concatenate untrusted input.
- **Consistency** is tunable per connection (and usually per query): `ONE`,
  `QUORUM` (default), or `ALL`. skaidb is leaderless — connect to any node.
- **No transactions.** skaidb auto-commits each statement; there is no
  `BEGIN`/`COMMIT`/`ROLLBACK`. Driver transaction hooks are no-ops or raise.
- **Auth:** omit the username/password for a server with auth disabled (the
  drivers connect as `anonymous`). Otherwise SCRAM-SHA-256 runs automatically,
  including mutual server-signature verification.

## Connecting to the homelab cluster

```
host: 192.168.7.117 (or .107 / .198)   port: 7000
user: skaidb                            password: skaidbClu5ter
```

Each driver ships a runnable `example` you can point at it.
