# skaidb metrics & observability (SPEC §10)

This is the contract dashboards and monitoring agents build against. Every metric
skaidb exposes is listed here with its **type**, **labels**, and **meaning**. The
registry renders the Prometheus text exposition format with a correct `# TYPE`
and `# HELP` per metric — counters use `rate()`/`increase()`, gauges are
instantaneous, histograms expose `_bucket{le=…}` + `_sum` + `_count`.

## Endpoints

skaidb serves these over HTTP/1.1. They are **unauthenticated** and read-only, so
orchestrators, load balancers, and scrapers need no credentials and no admin
rights:

| Path        | Method | Purpose |
|-------------|--------|---------|
| `/metrics`  | GET    | Prometheus scrape. Pull-model gauges are refreshed on each scrape. |
| `/health`, `/healthz` | GET | **Liveness** — `200 ok` whenever the process is up. |
| `/ready`, `/readyz`   | GET | **Readiness** — `200 ready` when storage is open (and, clustered, the node has a topology); `503` otherwise. Drives LB/monitor decisions. |
| `/status`   | GET    | Low-privilege topology read: ring/epoch/members and default consistency, **no secrets**. |
| `/admin/slow` | POST | Sample of recent (masked) slow queries. Requires `Admin`. |
| `/admin/status` | POST | Full topology incl. member ids. Requires `Admin`. |

The `/metrics`, `/health`, `/ready`, `/status` routes are served both on the REST
port (`server.rest_port`, default 7080) **and** on a dedicated metrics listener
(`observability.prometheus_port`, default 9090) when that port differs from the
REST port. Point your scraper at `prometheus_port` to keep it off the data plane.

`SHOW TABLES` / `SHOW INDEXES` (see [QUERY_SYNTAX.md](QUERY_SYNTAX.md)) let a tool
enumerate the catalog without `/query` data access.

## Conventions

- **Counters** end in `_total` (with a few standard exceptions like
  `skaidb_*_seconds_count`). Only ever increase; reset to 0 on restart.
- **Gauges** are absolute, can go up or down (e.g. `skaidb_queries_in_flight`).
- **Histograms** (`skaidb_query_duration_seconds`) bucket observations; query the
  `_bucket`/`_sum`/`_count` series with `histogram_quantile()`.
- **Cardinality is bounded.** Per-table metrics are **opt-in**
  (`observability.per_table_metrics = true`) because the table count is unbounded.
  Error and consistency labels come from small fixed sets.
- Every series can be attributed to a node via `skaidb_node_info` (or by tagging
  the scrape target).

## Build / runtime

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `skaidb_up` | gauge | — | 1 while the server is up. |
| `skaidb_build_info` | gauge | `version`, `git_sha`, `rustc` | Always 1; build metadata for tracking deploys. |
| `skaidb_node_info` | gauge | `node_id`, `role` | Always 1; node identity so federated scrapes are distinguishable at the source. |
| `skaidb_start_time_seconds` | gauge | — | Unix time the process started. |
| `skaidb_uptime_seconds` | gauge | — | Seconds since start. |

`git_sha`/`rustc` come from the `SKAIDB_GIT_SHA` / `SKAIDB_RUSTC` build-time env
vars; unset → `"unknown"`.

## Query path

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `skaidb_queries_total` | counter | `type` | Statements executed (`select`/`insert`/`update`/`delete`/`ddl`/`tx`/`other`). |
| `skaidb_query_duration_seconds` | histogram | `type` | Execution latency. Use `histogram_quantile(0.99, …)`. |
| `skaidb_queries_in_flight` | gauge | — | Statements currently executing. |
| `skaidb_query_errors_total` | counter | `class` | Failed statements by class: `parse`, `constraint`, `storage`, `timeout`, `permission`, `other`. |
| `skaidb_rows_returned_total` | counter | — | Rows returned to clients. |
| `skaidb_rows_scanned_total` | counter | — | Result cells examined (rows × width) — a proxy for result volume. |
| `skaidb_slow_queries_total` | counter | — | Statements slower than `slow_query_ms`. |
| `skaidb_transactions_total` | counter | `kind` | `begin`/`commit`/`rollback` (embedded engine). |
| `skaidb_authz_denied_total` | counter | — | Statements denied by RBAC. |
| `skaidb_logins_total` / `skaidb_login_failures_total` | counter | — | Auth outcomes. |
| `skaidb_admin_total` | counter | `op` | Admin control-plane ops (`status`/`add_node`/`remove_node`/`repair`/`reclaim`/`slow`). |

## Connections

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `skaidb_connections_active` | gauge | `endpoint` | Open connections by endpoint (`binary`/`rest`). |
| `skaidb_connections_total` | counter | `endpoint` | Connections accepted by endpoint. |

## Storage / LSM

Pulled from the engine snapshot at scrape time (aggregated across all table and
index storage engines).

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `skaidb_storage_tables` | gauge | — | Tables in the catalog. |
| `skaidb_storage_indexes` | gauge | — | Secondary + vector indexes. |
| `skaidb_storage_memtable_bytes` | gauge | — | Approx live memtable footprint. |
| `skaidb_storage_sstables` | gauge | — | On-disk SSTable count across levels. |
| `skaidb_storage_disk_bytes` | gauge | — | On-disk bytes across all SSTables. |
| `skaidb_storage_compactions_total` | counter | — | Compaction passes completed. |
| `skaidb_storage_compaction_bytes_total` | counter | — | Bytes written by compaction. |
| `skaidb_wal_bytes` | gauge | — | Live write-ahead log size. |
| `skaidb_wal_fsyncs_total` | counter | — | WAL fsyncs issued (compare to writes to see group-commit coalescing). |
| `skaidb_cache_hits_total` / `skaidb_cache_misses_total` / `skaidb_cache_evictions_total` | counter | — | Read-cache effectiveness. |
| `skaidb_cache_entries` | gauge | — | Live read-cache entries. |
| `skaidb_bloom_negative_lookups_total` | counter | — | Point reads resolved absent by the Bloom/SSTable layer. |

### Host system (per node)

Sampled from `/proc` (plus `df` for the data-directory filesystem) at
each scrape; each node reports its own host. Memory is cgroup-aware — a
container reports its own limit and usage, not the host's. CPU% and disk
throughput are computed over the window since the previous sample.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `skaidb_host_cpu_percent` | gauge | — | Busy CPU as % of all cores over the last sampling window. |
| `skaidb_host_cpus` | gauge | — | Logical CPU count. |
| `skaidb_host_mem_total_bytes` | gauge | — | Total memory (cgroup limit when one applies). |
| `skaidb_host_mem_used_bytes` | gauge | — | Used memory (cgroup `memory.current` when limited, else `MemTotal - MemAvailable`). |
| `skaidb_host_rss_bytes` | gauge | — | The skaidb process's resident set size. |
| `skaidb_host_disk_read_bytes_total` / `skaidb_host_disk_written_bytes_total` | counter | — | Whole-host disk IO since boot (physical devices; partitions/loop/dm excluded). |
| `skaidb_host_disk_total_bytes` / `skaidb_host_disk_available_bytes` | gauge | — | The filesystem holding the data directory. |

### Per-table (opt-in)

Enabled with `observability.per_table_metrics = true`. Each carries a `table`
label — only turn this on when the table set is small and known.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `skaidb_table_live_keys` | gauge | `table` | Live keys (full merged scan; O(rows) at scrape). |
| `skaidb_table_tombstones` | gauge | `table` | Tombstones awaiting compaction. |
| `skaidb_table_disk_bytes` | gauge | `table` | On-disk bytes for the table. |

## Vector index

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `skaidb_vector_indexes` | gauge | — | HNSW indexes. |
| `skaidb_vector_indexed_total` | gauge | — | Total vectors held in memory. |
| `skaidb_vector_rebuild_seconds` | gauge | — | Time to rebuild vector indexes on the last open. |

## Cluster / replication

Present only when the node is clustered (`cluster.seeds` set). Pulled from the
coordinator at scrape time.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `skaidb_membership_epoch` | gauge | — | Membership epoch; **alert on changes**. |
| `skaidb_cluster_members` | gauge | — | Members visible from this node. |
| `skaidb_cluster_resharding` | gauge | — | 1 while a join/decommission dual-write window is open. |
| `skaidb_cluster_writes_total` | counter | `consistency` | Coordinated writes by level (`one`/`quorum`/`all`). |
| `skaidb_cluster_reads_total` | counter | `consistency` | Coordinated reads by level. |
| `skaidb_cluster_quorum_failures_total` | counter | `kind` | Operations that failed to reach quorum (`read`/`write`). |
| `skaidb_cluster_read_repairs_total` | counter | — | Read-repair writes pushed to lagging replicas. |
| `skaidb_cluster_hints_stored_total` | counter | — | Hinted-handoff writes buffered for unreachable replicas. |
| `skaidb_cluster_hints_replayed_total` | counter | — | Hinted-handoff writes successfully replayed. |
| `skaidb_cluster_hints_pending` | gauge | — | Hints currently buffered (all peers). |
| `skaidb_cluster_hints_pending_peer` | gauge | `peer` | Hints buffered **per peer** — exact replication backlog for that node. |
| `skaidb_cluster_replication_lag_ms` | gauge | `peer` | Approx. ms between this node's HLC frontier and the latest write it has confirmed `peer` applied. |
| `skaidb_cluster_peer_requests_total` | counter | — | Internode RPCs issued by the coordinator. |
| `skaidb_cluster_peer_errors_total` | counter | — | Internode RPCs that errored or timed out. |

These anti-entropy and quorum signals are correctness-critical — read-repair,
hinted handoff, and quorum failures were previously invisible.

**Reading replication health per peer.** `skaidb_cluster_hints_pending_peer` is
the *exact* backlog — writes this node has buffered for a peer it couldn't reach.
`skaidb_cluster_replication_lag_ms` is an *estimate*: it only advances a peer's
baseline when a write is confirmed to it, so it climbs while a peer is
unreachable and falls once hinted handoff/anti-entropy catch it up. A peer with
no confirmed write yet (freshly added, or down since startup) is **absent** from
`replication_lag_ms` — rely on `hints_pending_peer` and the `reachable` flag in
`\cluster` for those. Both are emitted per current peer (ring ∪ configured seeds).

## Logs

Audit/query/login logs are written to stderr in human-readable text by default.
Set `observability.log_format = "json"` to emit **one JSON object per line**
(`event`, `elapsed_ms`, `error`, `sql`, …) so a log agent can parse them
reliably. Query logs are masked (literals → `?`) unless `query_log_masked` is
disabled. A bounded, masked sample of recent slow queries is available at
`POST /admin/slow`.

### Log files

By default logs go to stderr. Set `observability.log_file` to a path to write
all audit logs to a file instead (created if absent, appended otherwise):

```toml
[observability]
log_file = "/var/log/skaidb/audit.log"
```

Each log category can be split into its own file with a per-category override;
an empty override falls back to `log_file`, and an empty `log_file` falls back
to stderr:

| Key | Stream |
| --- | --- |
| `observability.query_log_file` | executed-statement log |
| `observability.slow_query_log_file` | slow-query log |
| `observability.error_log_file` | error log |
| `observability.login_log_file` | login/auth log |

```toml
[observability]
log_file = "/var/log/skaidb/audit.log"   # everything not overridden below
error_log_file = "/var/log/skaidb/error.log"
slow_query_log_file = "/var/log/skaidb/slow.log"
```

Categories pointed at the same path share one file handle, so their lines
interleave safely. All of these keys are runtime-mutable (`config set`,
`--*` flags, and `SKAIDB_*_LOG_FILE` env vars), and a path that can't be opened
falls back to stderr with a one-line warning rather than failing startup.
