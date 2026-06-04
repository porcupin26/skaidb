# skaidb

A schema-less, SQL-speaking, leaderless distributed database written in Rust.

skaidb stores dynamically-typed **documents** (schema-less: a missing field reads
as `NULL`) and queries them with a subset of **SQL:2016 core**. Storage is an
**LSM tree**; replication is **leaderless** with tunable quorums and HLC
last-writer-wins; the client protocol is a length-prefixed binary fast path with
a REST gateway alongside.

On identical small nodes (1 vCPU / 512 MB) with matched durability, skaidb runs
within ~10–25% of MongoDB and PostgreSQL across reads, writes, and mixed
workloads — using **~19 MB of RAM**, 3–8× less than the others. Full
multi-database, multi-consistency results are in **[docs/BENCHMARKS.md](docs/BENCHMARKS.md)**.

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

Implemented end-to-end and tested (141 tests):

- soft-schema document model, SQL subset, 3-valued logic
- LSM storage: WAL recovery, SSTables + Bloom filters, lazy-leveled compaction,
  **group-commit WAL** (batched fsync across concurrent writers)
- **secondary indexes** that accelerate local `WHERE path = value`
- **leaderless replication**: consistent-hash placement; every node serves reads
  and writes; **tunable write consistency** (`ONE`/`QUORUM`/`ALL`) where weaker
  levels ack early and replicate the rest in the background; **PK point reads**
  routed to the key's replica set; scatter-gather reads merged by HLC
  last-writer-wins; quorum-broadcast DDL; one-node-down tolerance
- **auth**: SCRAM-SHA-256 handshake (mutual auth) on the binary endpoint and
  HTTP Basic on REST, + per-statement **RBAC**
- binary + REST endpoints, Prometheus metrics, masked audit logs
- **benchmarked** against MongoDB 7/8, PostgreSQL, MariaDB — see
  [docs/BENCHMARKS.md](docs/BENCHMARKS.md)

Designed for but deliberately not yet built: **QUIC** transport + push-based
control plane (the raw-TCP fast path is in; QUIC needs an async runtime),
**distributed/multi-key transactions** (needs a coordinator/2PC), active
**anti-entropy** (read-repair & hinted handoff — convergence currently relies on
writes reaching their replicas), and **secondary-index / range acceleration on
the distributed read path** (PK point reads are routed, but non-PK and range
reads still gather from replicas and filter).

See `.priv/SPEC.md` for the full design.

## License

skaidb is licensed under the **Server Side Public License, version 1 (SSPL-1.0)** —
see [LICENSE](LICENSE).

In plain terms: you may use, copy, modify, and self-host skaidb freely, including
inside your own company. The one restriction is the SSPL's Section 13 — if you
**offer skaidb (or a modified version) to third parties as a service**, you must
release the complete source of the service-management software you use to do so
under the SSPL. Internal use and ordinary distribution are unaffected. This is the
same license MongoDB uses, for the same reason.

> Not a substitute for legal advice — read the [LICENSE](LICENSE) for the binding
> terms.
