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
| `skaidb_rows_written_total` | counter | — | Rows written (inserted/updated/deleted) — the write-throughput signal a bulk import shows up in (`queries/s` counts statements, so a multi-row batch is one query). |
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

Enabled with `observability.per_table_metrics = true`. Each carries `db` and
`table` labels — only turn this on when the table set is small and known.

| Metric | Type | Labels | Meaning |
|--------|------|--------|---------|
| `skaidb_table_live_keys` | gauge | `db`, `table` | Live keys (full merged scan; O(rows) at scrape). |
| `skaidb_table_tombstones` | gauge | `db`, `table` | Tombstones awaiting compaction. |
| `skaidb_table_disk_bytes` | gauge | `db`, `table` | On-disk bytes for the table — every table kind, TIME-SERIES included. |
| `skaidb_ts_table_series` | gauge | `db`, `table` | Series count per time-series table. |
| `skaidb_ts_table_samples_appended_total` | counter | `db`, `table` | Samples appended per time-series table. |
| `skaidb_ts_table_samples_rejected_total` | counter | `db`, `table` | Samples rejected (OOO / series limit) per time-series table. |

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
| `skaidb_memory_shedding_writes` | gauge | — | 1 while the node is shedding writes under memory pressure (rejecting new writes so it can drain instead of being OOM-killed). **Alert on sustained 1.** |
| `skaidb_memory_used_bytes` / `skaidb_memory_limit_bytes` | gauge | — | Sampled memory usage vs. the node's limit (cgroup when set, else system RAM); shedding starts at 85% and clears at 70%. |
| `skaidb_memory_anon_bytes` / `skaidb_memory_file_bytes` | gauge | — | Cgroup anon vs file split. The production memory-wedge signature is anon ratcheting up while file collapses toward zero — graph these together. |
| `skaidb_alloc_allocated_bytes` / `skaidb_alloc_resident_bytes` / `skaidb_alloc_retained_bytes` | gauge | — | jemalloc live heap / resident pages / OS-unreturned address space. `resident − allocated` ≈ fragmentation + unpurged dirty pages: distinguishes "something holds memory" from "the allocator won't give it back". |
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

The same per-peer backlog and lag are surfaced without a scraper: `GET /status`
carries a `peers` array (`id`, `in_ring`, `in_config`, `hints_pending`, `lag_ms`),
and the UI **members** panel renders a **backlog** column (buffered writes owed to
that node — nonzero is flagged) and a **lag** column (that node's replication lag),
so you can see at a glance how far behind each node is.

## The `node_stats` table (replicated host statistics)

With `observability.node_stats` (default **on**), every node INSERTs its own
host statistics — CPU, load, memory, disk I/O and space, uptime, **restart
count**, and **cgroup OOM kills** — into the replicated `node_stats` table
every `node_stats_interval_secs` (default 1 s, live-mutable): one row per
node, keyed on the node id, stamped with the sample time (`ts`, epoch ms).
The row replicates like any write, so any member serves the whole cluster's
picture from a local read, and it is plain SQL:

```sql
SELECT node, ts, mem_used_bytes, restarts, oom_kills FROM node_stats;
-- memory-ramp composition (the anon-ratchet post-mortem columns):
SELECT node, mem_anon_bytes, mem_file_bytes,
       alloc_allocated_bytes, alloc_resident_bytes, alloc_retained_bytes
FROM node_stats;
```

The UI's stats **NODES** table reads this (falling back to live probes for
members without a fresh row, e.g. mid rolling-upgrade) and shows each row's
**age** — a silently struggling node dims and its age climbs, instead of the
old behavior where one missed probe flapped a live node to "unreachable".
Node restarts log their start number, and when the cgroup's OOM-kill count
advanced since the previous start, the log says the prior run likely died to
the OOM killer. Probe-loss/recovery transitions and circuit-breaker events
are logged on the coordinator as well.

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

**Every log line carries a timestamp**: text-format lines (and every
operational `skaidb:` line in the server log) are prefixed with an
ISO-8601 UTC instant (`2026-07-20T18:42:13.123Z …`); JSON-format lines
(`observability.log_format = "json"`) carry it as a `ts` field instead, so
each line stays independently parseable.

**Query/slow-query/error lines carry the execution context**: the
authenticated user, the session database, and the access surface —
`via=driver` (binary-protocol/driver connections), `rest`, `es`, `ui`,
`prom` (PromQL evaluations, which log like queries), or `internal`
(background/system statements):

```
2026-07-20T18:42:13.123Z [query] 3ms user=agencik db=agencik via=driver SELECT … WHERE id = ?
2026-07-20T18:42:14.001Z [query] 1ms user=pi_air_quality db=pi_air_quality via=prom promql query_range avg(pm25{}[?m])
```

JSON format carries the same as `user`/`db`/`via` fields.

## REST request activity

- `skaidb_rest_requests_total{path="query"|"insert"|"es"|"prom"|"ui"|"ops"|"admin"|"other"}`
  — REST requests served, per path class, timed end to end.
- `skaidb_rest_request_duration_us_total{path=…}` — total serving time in
  microseconds; divide by `skaidb_rest_requests_total` for the average
  response time (the status tab's REST-activity table shows exactly this).
