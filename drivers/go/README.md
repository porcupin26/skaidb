# skaidb — Go driver

A standard [`database/sql`](https://pkg.go.dev/database/sql) driver. Import it
for its side effect and use the stdlib API you already know — `sql.Open`,
`db.Query`, `db.Exec`, `rows.Scan`. **No third-party dependencies.**

## Install

```sh
go get github.com/porcupin26/skaidb/drivers/go
```

## Use

```go
import (
    "database/sql"
    _ "github.com/porcupin26/skaidb/drivers/go"
)

db, err := sql.Open("skaidb", "skaidb://skaidb:secret@localhost:7000/?consistency=quorum")
if err != nil { log.Fatal(err) }
defer db.Close()

db.Exec("CREATE TABLE users (PRIMARY KEY (id))")
db.Exec("INSERT INTO users (id, name) VALUES (?, ?)", 1, "Ada")

rows, _ := db.Query("SELECT id, name FROM users WHERE id = ?", 1)
defer rows.Close()
for rows.Next() {
    var id int
    var name string
    rows.Scan(&id, &name)
    fmt.Println(id, name)
}
```

### DSN

```
skaidb://[user[:password]@]host[:port]/?consistency=quorum
```

- `port` defaults to `7000`.
- `consistency` is `one`, `quorum` (default), or `all`.
- Omit `user`/`password` for a server with auth disabled.

### Notes

- **Placeholders** use `?` (the `database/sql` convention). Args are quoted
  safely client-side.
- skaidb is **non-transactional**: `db.Begin()` returns an error. Each statement
  auto-commits. Use `db.Query`/`db.Exec` directly.
- A `*sql.DB` is a connection pool and is safe for concurrent use — the usual
  Go pattern. Each pooled connection serializes its own request/response.

### Types (`rows.Scan` targets)

| skaidb     | Go (driver value)        | Scan into            |
|------------|--------------------------|----------------------|
| Null       | `nil`                    | `*sql.NullX`, pointer|
| Bool       | `bool`                   | `*bool`              |
| Int        | `int64`                  | `*int`, `*int64`     |
| Float      | `float64`                | `*float64`           |
| Decimal    | `string` (exact)         | `*string`            |
| String     | `string`                 | `*string`            |
| Bytes      | `[]byte`                 | `*[]byte`            |
| Uuid       | `string` (canonical)     | `*string`            |
| Timestamp  | `time.Time` (UTC)        | `*time.Time`         |
| Array      | JSON `string`            | `*string`            |
| Document   | JSON `string`            | `*string`            |

## Run the example

```sh
cd example
go run . "skaidb://skaidb:secret@192.168.7.117:7000/?consistency=quorum"
```
