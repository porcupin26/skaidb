# skaidb — Ruby driver

A small client for skaidb's binary wire protocol, with an API modelled on the
[ruby-pg](https://github.com/ged/ruby-pg) gem (`PG`). If you've used Postgres
from Ruby, this should feel familiar. **Pure standard library** — no gems
(only `socket`, `openssl`, `securerandom`, `bigdecimal`).

## Install

```sh
gem build skaidb.gemspec
gem install ./skaidb-0.1.0.gem
# or just copy lib/skaidb.rb next to your code and `require_relative` it
```

## Use

```ruby
require "skaidb"

conn = Skaidb.connect(host: "localhost", port: 7000,
                      user: "skaidb", password: "secret")

conn.exec("CREATE TABLE users (PRIMARY KEY (id))")
conn.exec_params("INSERT INTO users (id, name) VALUES ($1, $2)", [1, "Ada"])

res = conn.exec_params("SELECT id, name FROM users WHERE id = $1", [1])
res.each { |row| puts row["name"] }   # => "Ada"  (rows are Hashes)
p res.fields                          # => ["id", "name"]
p res.rows                            # => [[1, "Ada"]]

conn.close
```

- **Placeholders** use the pg style: `$1`, `$2`, ... . Parameters are safely
  quoted client-side, so `"O'Brien"` just works. (The protocol has no
  server-side bind parameters; the driver interpolates with SQL quoting.)
- `Skaidb.connect` runs the SCRAM-SHA-256 handshake automatically. Omit
  `user`/`password` for a server with auth disabled (defaults: user
  `"anonymous"`, empty password).
- A **block form** closes the connection automatically:

  ```ruby
  Skaidb.connect(host: "localhost") do |conn|
    conn.exec("SELECT 1")
  end
  ```

### Result object

`exec` / `exec_params` return a `Skaidb::Result`, shaped like `PG::Result`:

| Method            | Returns                                              |
|-------------------|------------------------------------------------------|
| `each { \|row\| }`  | yields each row as a `Hash` keyed by column name     |
| `rows`            | `Array` of rows as positional `Array`s               |
| `fields`/`columns`| `Array<String>` of column names                      |
| `ntuples`         | number of rows                                       |
| `nfields`         | number of columns                                    |
| `cmd_tuples`      | rows affected by an `INSERT`/`UPDATE`/`DELETE`       |
| `[i]`             | row `i` as a `Hash`                                  |
| `getvalue(r, c)`  | one value by row index and column (name or index)    |

`Result` is `Enumerable`, so `map`, `select`, `to_a`, etc. all work.

### Consistency

skaidb is leaderless with tunable consistency. Default is `:quorum`:

```ruby
conn = Skaidb.connect(..., consistency: :one)   # or :quorum / :all (or 0/1/2)
conn.consistency = :all                          # change the default
res = conn.exec_params(sql, params, consistency: :all)  # per-query override
```

### Types

| skaidb     | Ruby                                  |
|------------|---------------------------------------|
| Null       | `nil`                                 |
| Bool       | `true` / `false`                      |
| Int        | `Integer`                             |
| Float      | `Float`                               |
| Decimal    | `BigDecimal`                          |
| String     | `String` (UTF-8)                      |
| Bytes      | `String` (`ASCII-8BIT`/binary)        |
| Uuid       | `String` (canonical `8-4-4-4-12` hex) |
| Timestamp  | `Time` (UTC, millisecond precision)   |
| Array      | `Array`                               |
| Document   | `Hash`                                |

When binding parameters, Ruby values map the other way: `nil`→`NULL`,
`true`/`false`→`TRUE`/`FALSE`, `Integer`/`Float`/`BigDecimal`→numeric literals,
`String`→quoted string (binary strings→hex literal), `Time`→Unix milliseconds.

> skaidb auto-commits each statement (non-transactional). There is no
> multi-statement transaction.

### Errors

- `Skaidb::Error` — base class.
- `Skaidb::ConnectionError` — transport/handshake failure; the connection is
  dead and should be discarded.
- `Skaidb::QueryError` — a statement failed (the server returned an error, or a
  parameter could not be bound); the connection stays usable.

## Run the example

```sh
ruby example.rb 192.168.7.117 7000 skaidb secret
```
