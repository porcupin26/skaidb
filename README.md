# skaidb

A schema-less, SQL-speaking, leaderless distributed database written in Rust.

skaidb stores dynamically-typed **documents** (schema-less: a missing field reads
as `NULL`) and queries them with a subset of **SQL:2016 core**. Storage is an
**LSM tree**; replication is **leaderless** with tunable quorums and HLC
last-writer-wins; the client protocol is a length-prefixed binary fast path with
a REST gateway alongside.

## Workspace layout

| Crate | Responsibility |
|-------|----------------|
| `skaidb-types` | Value/document model, 3-valued logic, order-preserving + lossless codecs, JSON interop |
| `skaidb-storage` | LSM engine: HLC clock, CRC WAL, MVCC memtable, SSTables + Bloom filters, lazy-leveled compaction |
| `skaidb-sql` | Lexer, AST, and parser for the SQL subset |
| `skaidb-engine` | Catalog + query execution (embeddable `Database`) |
| `skaidb-cluster` | Consistent-hash ring (vnodes), tunable quorum, LWW conflict resolution |
| `skaidb-proto` | Binary wire protocol (framing + messages) |
| `skaidb-driver` | Synchronous client over the binary endpoint |
| `skaidb-auth` | SHA-256/HMAC/PBKDF2 → SCRAM-SHA-256, and RBAC |
| `skaidb-config` | TOML config with full CLI + env overrides |
| `skaidb-server` | `skaidb` binary: binary + REST endpoints, metrics, audit logging |
| `skaidb-cli` | `skaidb-cli`: embedded SQL shell |

## Build & test

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## Run the server

```sh
skaidb --data-dir ./data --bind-addr 127.0.0.1 --quic-port 7000 --rest-port 7080
# every config option is also a CLI flag / env var; see:
skaidb --print-config
```

Query over REST:

```sh
curl -X POST 127.0.0.1:7080/query -d "CREATE TABLE users (PRIMARY KEY (id))"
curl -X POST 127.0.0.1:7080/query -d "INSERT INTO users (id, name) VALUES (1, 'ada')"
curl -X POST 127.0.0.1:7080/query -d '{"sql":"SELECT * FROM users"}'
curl 127.0.0.1:7080/metrics
```

Or use the embedded shell:

```sh
skaidb-cli --dir ./data -e "SELECT COUNT(*) FROM users"
```

## SQL surface (phase 1)

`CREATE/DROP TABLE` (declares only the primary key — no column list),
`CREATE/DROP INDEX`, `INSERT`, `SELECT` (projection incl. nested paths, `WHERE`,
`ORDER BY`, `LIMIT/OFFSET`, aggregates, `GROUP BY`), `UPDATE`, `DELETE`.

Types: `null, bool, int64, float64, decimal, string, bytes, uuid, timestamp`
(unixtime ms), `array`, `document`, plus JSON-like values.

## Status & deferred work

Implemented end-to-end and tested. The transport is the raw-TCP fast path; the
following are designed for but not yet built: **QUIC** transport and the
push-based control plane, **secondary-index-accelerated** reads (indexes are
recorded but reads full-scan), cross-node **replication wiring** (the ring and
quorum logic exist as a library), **distributed/multi-key transactions**, and
per-connection **auth handshake + RBAC enforcement** (the crypto, credentials,
and role model exist; the protocol does not yet carry an identity).

See `.priv/SPEC.md` for the full design.
