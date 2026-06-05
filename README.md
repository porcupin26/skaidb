# skaidb

A schema-less, SQL-speaking, leaderless distributed database written in Rust.

skaidb stores dynamically-typed **documents** (schema-less: a missing field reads
as `NULL`) and queries them with a subset of **SQL:2016 core**. Storage is an
**LSM tree**; replication is **leaderless** with tunable quorums and HLC
last-writer-wins; the client protocol is a length-prefixed binary fast path with
a REST gateway alongside.

On identical small nodes (1 vCPU / 512 MB) with matched durability, skaidb runs
close to MongoDB and PostgreSQL across reads, writes, and mixed workloads — and
is strong on concurrent writes — using **16 MB of RAM per node**, 3–9× less than
the others. Full multi-database, multi-consistency results are in
**[docs/BENCHMARKS.md](docs/BENCHMARKS.md)**.

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

Full grammar reference: **[docs/QUERY_SYNTAX.md](docs/QUERY_SYNTAX.md)**.

## Status & deferred work

Implemented end-to-end and tested (141 tests):

- soft-schema document model, SQL subset, 3-valued logic
- LSM storage: WAL recovery, SSTables + Bloom filters, lazy-leveled compaction,
  **group-commit WAL** (batched fsync across concurrent writers), and a bounded
  **RAM read cache** for point reads that fall through to SSTables
- **secondary indexes** — single or **composite** (`CREATE INDEX … ON t(a, b)`) —
  that accelerate local **equality and range** predicates (`= < <= > >=`,
  `BETWEEN`-style; composite uses a leftmost-prefix of equalities plus a trailing
  range) and `ORDER BY` along the index (sorted scan with early-stop `LIMIT` for
  top-N), since index entries are stored order-preserved
- **vector search** (prototype): an in-memory **HNSW** index for approximate
  nearest-neighbor search over embeddings, with **filtered** kNN
  ("nearest neighbors `WHERE …`"); cosine/L2/dot — see
  [docs/VECTOR.md](docs/VECTOR.md)
- **leaderless replication**: consistent-hash placement; every node serves reads
  and writes; **tunable write consistency** (`ONE`/`QUORUM`/`ALL`) where weaker
  levels ack early and replicate the rest in the background, and a coordinated
  write **overlaps its local fsync with peer replication** rather than running
  them serially; **PK point reads** routed to the key's replica set;
  **non-PK indexed reads pushed down** — the coordinator scatters the index scan
  to each member's local index, unions the candidate keys, then re-reads each at
  quorum (last-writer-wins authoritative version) — instead of shipping whole
  shards; non-indexed reads scatter-gather and merge by HLC LWW (tombstones
  included, so deletes win cluster-wide); quorum-broadcast DDL; one-node-down
  tolerance
- **auth**: SCRAM-SHA-256 handshake (mutual auth) on the binary endpoint and
  HTTP Basic on REST, + per-statement **RBAC**
- binary + REST endpoints, Prometheus metrics, masked audit logs
- **benchmarked** against MongoDB 7/8, PostgreSQL, MariaDB — see
  [docs/BENCHMARKS.md](docs/BENCHMARKS.md)

Designed for but deliberately not yet built: **QUIC** transport + push-based
control plane (the raw-TCP fast path is in; QUIC needs an async runtime),
**distributed/multi-key transactions** (needs a coordinator/2PC), active
**anti-entropy** (read-repair & hinted handoff — convergence currently relies on
writes reaching their replicas), **global (value-sharded) secondary indexes** —
today's indexes are local per node, so a distributed indexed read still scatters
to every member (their local indexes return only candidate keys) rather than
routing to a single owner — and specialized index types (text, geospatial,
array/multi-value).

See `.priv/SPEC.md` for the full design.

## Client drivers

Official, dependency-free drivers for **Python, Node.js/TypeScript, Go, Java,
Ruby, PHP, and C#/.NET** live in [`drivers/`](drivers/). Each mirrors the
idiomatic database API of its language (DB-API 2.0, `pg`, `database/sql`, JDBC,
the `pg` gem, PDO, ADO.NET) so there's almost nothing new to learn, and each
speaks the binary protocol directly with SCRAM auth and safe parameter binding.
The wire protocol is specified in [`drivers/PROTOCOL.md`](drivers/PROTOCOL.md).

## License

skaidb is licensed under the **Server Side Public License, version 1 (SSPL-1.0)** —
see [LICENSE](LICENSE).

In plain terms: you may use, copy, modify, and self-host skaidb freely, including
inside your own company. The one restriction is the SSPL's Section 13 — if you
**offer skaidb (or a modified version) to third parties as a service**, you must
release the complete source of the service-management software you use to do so
under the SSPL. Internal use and ordinary distribution are unaffected. 

> Not a substitute for legal advice — read the [LICENSE](LICENSE) for the binding
> terms.
