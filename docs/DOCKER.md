# Running skaidb in Docker

The `docker/` directory holds a multi-stage `Dockerfile` (static musl
build, Alpine runtime, non-root user, healthcheck against `/ready`) and
two compose files.

```sh
docker build -f docker/Dockerfile -t skaidb .
docker run -d --name skaidb \
  -p 7000:7000 -p 7080:7080 \
  -v skaidb-data:/var/lib/skaidb \
  skaidb
curl -X POST 127.0.0.1:7080/query -d "SHOW STATUS"
# web UI: http://127.0.0.1:7080/ui
```

Or with compose:

```sh
docker compose -f docker/docker-compose.yml up -d            # single node
docker compose -f docker/docker-compose.cluster.yml up -d    # 3-node RF=3
```

## Configuration — every parameter is an environment variable

skaidb's configuration precedence is **env var > config file > default**,
and *every* config option has a `SKAIDB_*` env var (and a matching CLI
flag) — `docker run --rm skaidb --help` prints the authoritative list.
The image sets three defaults: `SKAIDB_DATA_DIR=/var/lib/skaidb`,
`SKAIDB_BIND_ADDR=0.0.0.0`, `SKAIDB_MEMORY_TARGET=auto` (the budget reads
the **container's** cgroup memory limit, so `--memory 512m` is honored).

| Area | Variables |
|---|---|
| server | `SKAIDB_BIND_ADDR`, `SKAIDB_QUIC_PORT` (7000), `SKAIDB_REST_PORT` (7080), `SKAIDB_NODE_ROLE`, `SKAIDB_DATA_DIR`, `SKAIDB_CONFIG` |
| cluster | `SKAIDB_SEEDS` (`host:7100,...`), `SKAIDB_INTERNODE_PORT`, `SKAIDB_REPLICATION_FACTOR`, `SKAIDB_VNODES_PER_NODE`, `SKAIDB_DEFAULT_READ_CONSISTENCY`, `SKAIDB_DEFAULT_WRITE_CONSISTENCY`, `SKAIDB_ANTI_ENTROPY_INTERVAL_SECS` |
| auth | `SKAIDB_SCRAM_ENABLED`, `SKAIDB_SUPERUSER`, `SKAIDB_SUPERUSER_PASSWORD`, `SKAIDB_X509_ENABLED`, `SKAIDB_X509_CA_FILE`, `SKAIDB_INTERNODE_AUTH` (`none`/`token`/`mtls`), `SKAIDB_INTERNODE_TOKEN`, `SKAIDB_INTERNODE_KEYFILE`, `SKAIDB_INTERNODE_TLS_{CERT,KEY,CA}` |
| storage | `SKAIDB_MEMORY_TARGET` (`auto`/`1GB`/empty), `SKAIDB_MEMTABLE_SIZE_MB`, `SKAIDB_READ_CACHE_ENTRIES`, `SKAIDB_COMPACTION_STRATEGY`, `SKAIDB_USE_IO_URING` |
| encryption | `SKAIDB_TLS_CERT_FILE`, `SKAIDB_TLS_KEY_FILE`, `SKAIDB_AT_REST_ENABLED`, `SKAIDB_AT_REST_KEK_SOURCE`, `SKAIDB_AT_REST_KEYFILE` |
| observability | `SKAIDB_PROMETHEUS_PORT`, `SKAIDB_SLOW_QUERY_MS`, `SKAIDB_QUERY_LOG_ENABLED`, `SKAIDB_QUERY_LOG_MASKED`, `SKAIDB_LOGIN_LOG_ENABLED`, `SKAIDB_ERROR_LOG_LEVEL`, `SKAIDB_PER_TABLE_METRICS`, `SKAIDB_LOG_FORMAT` (`text`/`json`), `SKAIDB_LOG_FILE` + per-category `*_LOG_FILE`, `SKAIDB_SELF_SCRAPE`, `SKAIDB_SELF_SCRAPE_INTERVAL_SECS` |
| web UI | `SKAIDB_UI_ENABLED` |
| agent | `SKAIDB_SUBSET_TABLES`, `SKAIDB_MAX_STALENESS_MS` |

A mounted TOML file works too: mount it and point `SKAIDB_CONFIG` at it
(see the commented volume in `docker-compose.yml`); env vars still win
over the file.

## Clustering in compose

Each node must announce an address its peers can reach, so every service
sets `SKAIDB_BIND_ADDR` to its **service name** and all share one
`SKAIDB_SEEDS` list (`skaidb1:7100,skaidb2:7100,skaidb3:7100`). Scale-out
beyond the seed list works at runtime:

```sh
docker compose -f docker/docker-compose.cluster.yml exec skaidb1 \
  skaidbsh --host skaidb1 -e "ALTER CLUSTER ADD NODE 'skaidb4:7100'"
```

## Operational notes

- **Ports**: 7000 binary protocol, 7080 REST + UI + ES subset + PromQL,
  7100 internode (compose networks only — don't publish it), 9090
  optional dedicated metrics listener.
- **Persistence**: everything lives under `/var/lib/skaidb` — one named
  volume per node.
- **Health**: the built-in `HEALTHCHECK` polls `/ready`; `/health` is
  liveness, `/status` is topology.
- **Memory**: prefer `--memory <limit>` + `SKAIDB_MEMORY_TARGET=auto`
  over hand-tuning the individual knobs.
- **Logs**: stderr by default (docker-native); set
  `SKAIDB_LOG_FORMAT=json` for log collectors.
- The container runs as the non-root `skaidb` user.
