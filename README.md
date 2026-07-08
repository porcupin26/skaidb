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
| `skaidb-driver` | Synchronous client over the binary endpoint, with nearest-node selection and failover |
| `skaidb-auth` | SHA-256/HMAC/PBKDF2 → SCRAM-SHA-256, and RBAC |
| `skaidb-config` | TOML config with full CLI + env overrides |
| `skaidb-server` | `skaidb` binary: binary + REST endpoints, metrics, audit logging |
| `skaidb-cli` | `skaidbsh`: unified network shell + cluster/config admin client (also embedded via `--local`) |

## Build & test

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## Install

Prebuilt binaries and packages for Linux, macOS, and Windows are attached to
every [GitHub Release](https://github.com/porcupin26/skaidb/releases) — `.deb`
and `.rpm` (x86_64 + aarch64), `.dmg` (Intel + Apple Silicon), a Windows `.zip`/
`.exe`, and `.tar.gz` tarballs (incl. a static musl build), with `SHA256SUMS`.
Each bundle ships the `skaidb` server and `skaidbsh` — the unified interactive
shell and cluster/config admin client.

**Full, OS-by-OS instructions (packages, tarballs, source, binary-only,
verification, upgrade/uninstall) are in [docs/INSTALL.md](docs/INSTALL.md).**
The short version:

```sh
# Debian/Ubuntu              # Fedora/RHEL/openSUSE          # any Linux / macOS
sudo apt install ./skaidb_*_amd64.deb
sudo dnf install ./skaidb-*-1.x86_64.rpm
tar xzf skaidb-*-x86_64-unknown-linux-gnu.tar.gz && sudo install -m755 skaidb skaidbsh /usr/local/bin/
```

Releases are cut automatically on every push to `main` with SemVer version
bumps — see [docs/RELEASING.md](docs/RELEASING.md). Or build from source:

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

The node also serves a **built-in web UI** at `http://127.0.0.1:7080/ui` —
embedded in the binary (no external assets), same Basic auth + RBAC as
`/query`, live-toggleable with `\ui on|off` (or `config set ui.enabled`).

Or use the shell — `skaidbsh` connects over the network, picking the nearest
reachable node and failing over to another if the connected one dies. Pointed at
a single node it **discovers the rest of the cluster** (via `/status`) and adds
them to its failover pool, so one `--host` still survives a node loss:

```sh
skaidbsh --host 127.0.0.1 -e "SELECT COUNT(*) FROM users"   # one-shot
skaidbsh --host node1                                        # interactive; auto-discovers peers
skaidbsh --host node1,node2,node3                            # explicit seed list
skaidbsh --local ./data -e "SELECT COUNT(*) FROM users"     # offline embedded engine
```

Discovery assumes a uniform client (SQL) port across the cluster — the standard
deployment. If nodes use different client ports, list them explicitly with
`--host`.

## Run a cluster

Give every node the same `--seeds` list (each entry a member's
`host:internode_port`, including itself) and set `--bind-addr`/`--internode-port`
to match its own entry:

```sh
# one of three nodes — only --bind-addr differs across the cluster
skaidb --data-dir /var/lib/skaidb --bind-addr 10.0.0.1 \
  --internode-port 7100 --replication-factor 3 \
  --seeds 10.0.0.1:7100,10.0.0.2:7100,10.0.0.3:7100
```

Every node serves reads and writes; data is replicated and quorum-tuned. Inspect
and reshape a live cluster — and read or change configuration — with
**`skaidbsh`** (the same binary as the shell):

```sh
skaidbsh --host 10.0.0.1 cluster status                  # ring, epoch, members, RF
skaidbsh --host 10.0.0.1 cluster add-node 10.0.0.4:7100  # join + migrate its share online
skaidbsh --host 10.0.0.1 cluster remove-node 10.0.0.3:7100  # drain, then decommission
skaidbsh --host 10.0.0.1 config show                     # all settings (secrets masked)
skaidbsh --host 10.0.0.1 config set observability.slow_query_ms 100
```

These drive an authenticated `POST /admin/*` control plane on the node's REST
port (RBAC-gated). Inside the interactive shell the same operations are available
as `\cluster`, `\node`, `\config`, `\status`, and `\metrics`.
Replication factor, consistency, ports, internode auth, and the full admin
surface are in **[docs/CLUSTERING.md](docs/CLUSTERING.md)** (mechanics in
[docs/RESHARDING.md](docs/RESHARDING.md)).

## SQL surface (phase 1)

`CREATE/DROP TABLE` (declares only the primary key — no column list),
`CREATE/DROP INDEX`, `ALTER TABLE … RENAME`, `INSERT`, `SELECT` (projection incl.
nested paths, `WHERE`, `ORDER BY`, `LIMIT/OFFSET`, aggregates, `GROUP BY`,
`DISTINCT`, `HAVING`, `INNER/LEFT/RIGHT/CROSS JOIN`, `UNION [ALL]`), `UPDATE`,
`DELETE`, and embedded `BEGIN/COMMIT/ROLLBACK` transactions.

Types: `null, bool, int64, float64, decimal, string, bytes, uuid, timestamp`
(unixtime ms), `array`, `document`, plus JSON-like values.

Full grammar reference: **[docs/QUERY_SYNTAX.md](docs/QUERY_SYNTAX.md)**.

## Status & deferred work

Implemented end-to-end and tested (202 tests):

- soft-schema document model, SQL subset, 3-valued logic
- **SQL surface**: projection over nested paths, `WHERE`, `GROUP BY`/`HAVING`,
  aggregates, `DISTINCT`, `ORDER BY`/`LIMIT`/`OFFSET`, `INNER`/`LEFT`/`RIGHT`/
  `CROSS JOIN` (nested-loop, alias-qualified), `UNION`/`UNION ALL`,
  `ALTER TABLE … RENAME`, and embedded `BEGIN`/`COMMIT`/`ROLLBACK` transactions
  with read-your-writes — see [docs/QUERY_SYNTAX.md](docs/QUERY_SYNTAX.md)
- LSM storage: WAL recovery, SSTables + Bloom filters, lazy-leveled compaction,
  **group-commit WAL** (batched fsync across concurrent writers), and a bounded
  **RAM read cache** for point reads that fall through to SSTables
- **secondary indexes** — single or **composite** (`CREATE INDEX … ON t(a, b)`) —
  that accelerate local **equality and range** predicates (`= < <= > >=`,
  `BETWEEN`-style; composite uses a leftmost-prefix of equalities plus a trailing
  range) and `ORDER BY` along the index (sorted scan with early-stop `LIMIT` for
  top-N), since index entries are stored order-preserved
- **vector search**: `CREATE VECTOR INDEX … DIM n` builds an **HNSW** index for
  approximate nearest-neighbor search over embeddings, with **filtered** kNN
  ("nearest neighbors `WHERE …`"); cosine/L2/dot. **Distributed** — the index is
  broadcast so each node indexes its shard and queries scatter-gather + merge.
  In-memory/rebuilt-on-open for now — see [docs/VECTOR.md](docs/VECTOR.md)
- **full-text search**: `CREATE SEARCH INDEX … ON t (title, body)` builds a
  **BM25** index (embedded Tantivy) queried straight from SQL —
  `WHERE MATCH(body, '…') ORDER BY score() DESC LIMIT k` pushes ranked top-k
  into the index; `MATCH_PHRASE`/`MATCH_PREFIX`/`FUZZY`/`WILDCARD`/`REGEXP`/
  `SEARCH('query-string')` predicates composing with `AND`/`OR`/`NOT`, with
  ranges over typed (`long`/`double`/`bool`/`date`) fast fields;
  `HIGHLIGHT()` snippets; per-column analyzers (18 stemmed languages,
  ngram/edge-ngram, folding), boosts, `.keyword` exact-match twins, and
  `copy_to` composites; near-real-time refresh with WAL-replay crash
  recovery (the table is the source of truth). Fully distributed: every
  node indexes its shard, queries scatter-gather with per-shard top-k
  merged at the coordinator. An **ES-compatible REST subset**
  (`_bulk`/`_search`/`_count`/`_mapping`) serves existing Elasticsearch
  clients and log shippers — see [docs/SEARCH.md](docs/SEARCH.md)
- **time-series tables**: `CREATE TIMESERIES TABLE … (SERIES KEY (…),
  RETENTION 30d)` stores samples in a Prometheus-style engine
  (Gorilla-compressed chunks, ~1–1.5 B/sample, ≥2M samples/s ingest) with
  `time_bucket()` bucketing and counter-aware `rate()`/`increase()`
  aggregates. Distributed: series place on the ring and replicate; raw
  queries union-merge across members while eligible aggregations push
  **per-series per-bucket partials** down to the nodes instead of shipping
  raw samples — see [docs/TIMESERIES.md](docs/TIMESERIES.md)
- **leaderless replication**: consistent-hash placement; every node serves reads
  and writes; **tunable write consistency** (`ONE`/`QUORUM`/`ALL`) where weaker
  levels ack early and replicate the rest in the background, and a coordinated
  write **overlaps its local fsync with peer replication** rather than running
  them serially; **PK point reads** routed to the key's replica set;
  **non-PK reads pushed down** — for an indexed predicate the coordinator
  scatters the index scan, and for a non-indexed `WHERE` it scatters the
  **filter** itself, so each member returns only matching candidate keys; the
  coordinator unions them and re-reads each at quorum (last-writer-wins
  authoritative version) — instead of shipping whole shards. A `SELECT` with no
  predicate scatter-gathers and merges by HLC LWW (tombstones included, so
  deletes win cluster-wide); quorum-broadcast DDL; one-node-down
  tolerance; **anti-entropy** keeps replicas converged — **read-repair** on
  quorum reads, **hinted handoff** for writes that miss a down replica, and an
  active **repair** pass (bidirectional version reconciliation)
- **online resharding**: a node can **join or leave at runtime** — a join is a
  two-phase **pending-ranges** transition (the migrating keys' old and new owners
  are unioned, so writes dual-write and reads dual-read during the move — correct
  under live writes), bootstrapping the joiner's schema and pushing it the keys
  it now owns; on graceful **decommission** the
  leaving node first drains its keys to their new owners, then the ring shrinks
  (HLC-preserving, tombstones included, both ways). Consistent hashing means a
  single membership change only moves ~`1/N` of the keyspace; placements are
  otherwise undisturbed. A `reclaim` pass then physically frees the space the
  former owner held (ack-gated, no tombstone). All driven by **`skaidbsh`** over
  an authenticated `POST /admin/*` **control plane** (RBAC-gated, membership
  changes serialized). See [docs/CLUSTERING.md](docs/CLUSTERING.md) /
  [docs/RESHARDING.md](docs/RESHARDING.md)
- **auth**: SCRAM-SHA-256 handshake (mutual auth) on the binary endpoint and
  HTTP Basic on REST, + per-statement **RBAC**
- binary + REST endpoints, Prometheus metrics, masked audit logs
- **benchmarked** against MongoDB 7/8, PostgreSQL, MariaDB — see
  [docs/BENCHMARKS.md](docs/BENCHMARKS.md)

Designed for but deliberately not yet built: **QUIC** transport + push-based
control plane (the raw-TCP fast path is in; QUIC needs an async runtime),
**distributed/multi-key transactions** (single-node `BEGIN/COMMIT/ROLLBACK` is
in; spanning nodes needs a coordinator/2PC), **join pushdown** (a single-table
`WHERE` is now pushed to the shards, but a clustered *join* still gathers each
table to the coordinator rather than executing shard-local), active
**membership gossip/consensus** (data converges via anti-entropy, but a node
that missed a membership broadcast still needs it re-sent — there's no topology
gossip yet, and concurrent ring changes aren't linearizable. Online node
**join**, graceful **decommission**, post-move **space reclamation**, **versioned
+ persisted membership**, and **anti-entropy** (read-repair, hinted handoff,
active repair) are all built — see [docs/RESHARDING.md](docs/RESHARDING.md)),
**global (value-sharded) secondary indexes** —
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

## Examples

Runnable, self-contained usage examples for every language above (plus Rust)
live in [`examples/`](examples/) — connect, DDL, batch insert, query, update,
a primary-key point read, error handling, delete. A separate walkthrough in
[`examples/vectors/`](examples/vectors/) covers vector search end to end:
turning text into an embedding, storing and indexing it, and running
filtered nearest-neighbor search.

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
