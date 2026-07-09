# Running a skaidb cluster

skaidb is **leaderless**: every node serves reads and writes, data is placed on a
[consistent-hash ring](../crates/skaidb-cluster/src/ring.rs) (virtual nodes), and
each key is replicated to the next `replication_factor` nodes clockwise. Writes
wait for a tunable quorum; reads gather from replicas and resolve by
last-writer-wins. There is no special "primary" to configure.

This guide covers running multiple `skaidb` server nodes. For the *mechanics* of
how membership changes and data rebalances, see
[RESHARDING.md](RESHARDING.md); for install, see [INSTALL.md](INSTALL.md).

## Contents

- [How a node decides it's clustered](#how-a-node-decides-its-clustered)
- [Ports](#ports)
- [Form a cluster (static seed list)](#form-a-cluster-static-seed-list)
  - [Three nodes on three machines](#three-nodes-on-three-machines)
  - [Three nodes on one machine (local test)](#three-nodes-on-one-machine-local-test)
  - [Config-file and env-var equivalents](#config-file-and-env-var-equivalents)
- [Replication factor & consistency](#replication-factor--consistency)
- [Verify the cluster](#verify-the-cluster)
- [Add or remove nodes at runtime (online resharding)](#add-or-remove-nodes-at-runtime-online-resharding)
- [Anti-entropy: repair & space reclamation](#anti-entropy-repair--space-reclamation)
- [Internode security](#internode-security)
- [Operational notes & limitations](#operational-notes--limitations)

## How a node decides it's clustered

- **`seeds` is empty** (the default) → the node runs **standalone**: a single
  local engine, no internode networking.
- **`seeds` is non-empty** → the node runs as a **cluster member**. The seed list
  is the full membership: every entry is a member's internode address
  `host:internode_port`, and the list **must include this node itself**.

A node's identity on the ring is `bind_addr:internode_port`. For the node to be
part of the ring, that exact string must appear in `seeds`. **`bind_addr` must
therefore be the address other nodes use to reach this one** — a routable IP/host
on a real cluster, not `0.0.0.0`. All nodes should be given the *same* seed list.

## Ports

Each node listens on three ports, all on `bind_addr`:

| Purpose | Config | Default | Who connects |
|---------|--------|---------|--------------|
| Client binary (fast path) | `server.quic_port` | `7000` | applications / drivers |
| Client REST | `server.rest_port` | `7080` | `curl`, HTTP clients |
| Internode RPC | `cluster.internode_port` | `7100` | other skaidb nodes |
| Prometheus metrics | `observability.prometheus_port` | `9090` | scrapers |

**The internode port must be reachable between every pair of nodes** — open it in
your firewall/security group across the cluster. Client ports only need to be
reachable by clients.

## Form a cluster (static seed list)

The supported way to stand up a cluster is to give every node the same `seeds`
list at startup. Each node must also be told `bind_addr` + `internode_port` so its
own identity (`bind_addr:internode_port`) matches its entry in `seeds`, and all
nodes must share the same `replication_factor`.

### Three nodes on three machines

Machines `10.0.0.1`, `10.0.0.2`, `10.0.0.3`, internode port `7100` on each, RF 3:

```sh
# On 10.0.0.1
skaidb --data-dir /var/lib/skaidb \
  --bind-addr 10.0.0.1 --quic-port 7000 --rest-port 7080 \
  --internode-port 7100 \
  --seeds 10.0.0.1:7100,10.0.0.2:7100,10.0.0.3:7100 \
  --replication-factor 3 \
  --default-read-consistency QUORUM --default-write-consistency QUORUM

# On 10.0.0.2 — identical, but --bind-addr 10.0.0.2
skaidb --data-dir /var/lib/skaidb --bind-addr 10.0.0.2 \
  --quic-port 7000 --rest-port 7080 --internode-port 7100 \
  --seeds 10.0.0.1:7100,10.0.0.2:7100,10.0.0.3:7100 \
  --replication-factor 3 \
  --default-read-consistency QUORUM --default-write-consistency QUORUM

# On 10.0.0.3 — identical, but --bind-addr 10.0.0.3
skaidb --data-dir /var/lib/skaidb --bind-addr 10.0.0.3 \
  --quic-port 7000 --rest-port 7080 --internode-port 7100 \
  --seeds 10.0.0.1:7100,10.0.0.2:7100,10.0.0.3:7100 \
  --replication-factor 3 \
  --default-read-consistency QUORUM --default-write-consistency QUORUM
```

Only `--bind-addr` differs between nodes; the `--seeds` list is identical
everywhere. Start order doesn't matter — a node tolerates peers that aren't up
yet (writes/reads just need their quorum).

### Three nodes on one machine (local test)

Same idea with `127.0.0.1` and distinct ports + data dirs per node:

```sh
SEEDS=127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102

skaidb --data-dir ./n1 --bind-addr 127.0.0.1 --quic-port 7000 --rest-port 7080 \
  --internode-port 7100 --seeds $SEEDS --replication-factor 3 &
skaidb --data-dir ./n2 --bind-addr 127.0.0.1 --quic-port 7001 --rest-port 7081 \
  --internode-port 7101 --seeds $SEEDS --replication-factor 3 &
skaidb --data-dir ./n3 --bind-addr 127.0.0.1 --quic-port 7002 --rest-port 7082 \
  --internode-port 7102 --seeds $SEEDS --replication-factor 3 &
```

Each node's `127.0.0.1:<internode_port>` matches one entry in `$SEEDS`.

### Config-file and env-var equivalents

Every flag is also a TOML key and an environment variable. A per-node config file
(`skaidb --config /etc/skaidb.toml`):

```toml
[server]
bind_addr = "10.0.0.1"      # this node's reachable address
quic_port = 7000
rest_port = 7080
data_dir  = "/var/lib/skaidb"

[cluster]
seeds = ["10.0.0.1:7100", "10.0.0.2:7100", "10.0.0.3:7100"]
internode_port = 7100
replication_factor = 3
vnodes_per_node = 256
default_read_consistency = "QUORUM"
default_write_consistency = "QUORUM"
```

Or env vars (handy for containers; CLI flags override these, which override the
file):

```sh
export SKAIDB_BIND_ADDR=10.0.0.1
export SKAIDB_INTERNODE_PORT=7100
export SKAIDB_SEEDS=10.0.0.1:7100,10.0.0.2:7100,10.0.0.3:7100
export SKAIDB_REPLICATION_FACTOR=3
export SKAIDB_DEFAULT_READ_CONSISTENCY=QUORUM
export SKAIDB_DEFAULT_WRITE_CONSISTENCY=QUORUM
skaidb --data-dir /var/lib/skaidb
```

Confirm the resolved settings on any node with `skaidb --print-config`.

## Replication factor & consistency

- **`replication_factor` (RF)** — how many nodes hold each key. RF 3 tolerates one
  node down at `QUORUM`. RF must be ≤ the number of nodes (it's capped at the node
  count otherwise). Use the **same RF on every node**.
- **Consistency** (`ONE` / `QUORUM` / `ALL`), set as the cluster defaults:
  - `ONE` — ack after one replica (fast, weak).
  - `QUORUM` — majority of replicas (`floor(RF/2)+1`).
  - `ALL` — every replica.
  - **Strong consistency** when read CL + write CL > RF (e.g. `QUORUM`+`QUORUM`
    with RF 3 → R2 + W2 > 3). With weaker levels, the remaining replicas are
    updated in the background and converge via anti-entropy.
  - Per session: `SET CONSISTENCY ONE|QUORUM|ALL` on a binary-protocol
    connection (or `\consistency` in `skaidbsh`) overrides the defaults for
    that session's statements. REST is stateless and rejects it.

## Verify the cluster

Write through one node and read it back through another — leaderless means any
node accepts both:

```sh
# Create + insert via node 1 (REST on :7080)
curl -X POST 10.0.0.1:7080/query -d "CREATE TABLE users (PRIMARY KEY (id))"
curl -X POST 10.0.0.1:7080/query -d "INSERT INTO users (id, name) VALUES (1, 'ada')"

# Read via node 2 — sees the replicated row
curl -X POST 10.0.0.2:7080/query -d '{"sql":"SELECT * FROM users WHERE id = 1"}'

# Per-node metrics
curl 10.0.0.3:7080/metrics
```

Each node's startup log prints its endpoints (`binary endpoint listening on …`,
`REST endpoint listening on …`).

## Add or remove nodes at runtime (online resharding)

The ring can change while serving traffic — a node can **join** (and receive its
share of the keyspace) or be **gracefully decommissioned** (drain its keys first).
The full mechanics — pending-ranges dual-write during a join, single-sender
migration, epoch'd membership, throttling/resume — are in
[RESHARDING.md](RESHARDING.md).

Drive them with **`skaidbsh`**, the unified shell/admin client (shipped
alongside `skaidb`). It talks to any node's REST endpoint over an authenticated
`POST /admin/*` control plane (RBAC: the role needs `Admin` on the whole cluster;
membership changes are serialized server-side, one at a time):

```sh
# Point it at any node (REST port defaults to 7080, override with --rest-port);
# add --user/--password if the server requires auth.
skaidbsh --host 10.0.0.1 cluster status            # show the ring, epoch, members, RF

skaidbsh --host 10.0.0.1 cluster add-node 10.0.0.4:7100    # join: migrates its share in
skaidbsh --host 10.0.0.1 cluster remove-node 10.0.0.3:7100 # decommission: drains, then leaves
skaidbsh --host 10.0.0.1 cluster repair            # anti-entropy: converge all replicas
skaidbsh --host 10.0.0.1 cluster reclaim           # free space former owners no longer own
```

`status` prints JSON like:

```json
{ "clustered": true, "node_id": "10.0.0.1:7100", "epoch": 3,
  "replication_factor": 3,
  "configured": ["10.0.0.1:7100", "10.0.0.2:7100", "10.0.0.3:7100"],
  "self_in_ring": true,
  "members": ["10.0.0.1:7100", "10.0.0.2:7100", "10.0.0.4:7100"],
  "peers": [
    { "id": "10.0.0.2:7100", "in_config": true, "in_ring": true,
      "reachable": true, "hints_pending": 0, "lag_ms": 4 },
    { "id": "10.0.0.4:7100", "in_config": false, "in_ring": true,
      "reachable": true, "hints_pending": 0, "lag_ms": 7 }
  ],
  "discrepancies": {
    "configured_not_in_ring": ["10.0.0.3:7100"],
    "ring_not_configured": ["10.0.0.4:7100"]
  } }
```

**Configured vs. actual.** `configured` is what `seeds` says membership *should*
be; `members` is the **live ring** the coordinator actually routes and replicates
to. They diverge in normal operation: `cluster add-node` admits a node that was
never in anyone's `seeds` (it shows up under `ring_not_configured`), and a seed
that has not (yet) been admitted to the ring shows up under
`configured_not_in_ring`. The latter is exactly the trap to watch for: a node
started with `seeds` pointing at the cluster will **pull data via background
catch-up** (it sees peers, so anti-entropy runs) **without ever joining the
ring** — it serves stale-but-converging reads while no one routes writes to it.
Such a node appears in `configured_not_in_ring` until you run `cluster add-node`
for it. If the joining node's own `seeds` omit itself (it lists only peers),
peers never learn of it at all — there is no gossip — so the tell is on the node
itself: its `self_in_ring` is `false`, meaning it is coordinating/catching-up but
owns no ring tokens. The unauthenticated `GET /status` carries the same
`configured` / `self_in_ring` / `configured_not_in_ring` / `ring_not_configured`
fields (without the per-peer liveness probe); `\cluster` adds `reachable`,
`hints_pending`, and `lag_ms` per peer (see [METRICS.md](METRICS.md) for the
matching Prometheus gauges).

Under the hood these call the coordinator: `add-node` broadcasts the new ring,
bootstraps the joiner's schema, and streams it the keys it now owns (dual-writing
during the move so concurrent writes stay correct); `remove-node` drains the
leaving node's keys to their new owners before dropping it from the ring. The
same operations are also available as raw HTTP (`POST /admin/status`,
`/admin/add-node` with `{"addr":"…"}`, `/admin/remove-node` with `{"id":"…"}`,
`/admin/repair`, `/admin/reclaim`), as plain SQL from any client —
`SHOW CLUSTER`, `ALTER CLUSTER ADD NODE 'host:7100'` / `REMOVE NODE 'id'`,
`REPAIR CLUSTER`, `RECLAIM`, plus `SHOW CONFIG [LIKE]` / `SET CONFIG` and
`SHOW SLOW QUERIES` (identical RBAC and audit as the HTTP endpoints) —
and as `skaidb_cluster::Node` library methods.

A long migration keeps the `skaidbsh` request open until it finishes; tune the
push rate per node with the migration throttle (see
[RESHARDING.md](RESHARDING.md)). Run one membership change at a time.

A join that fails partway (e.g. the joiner became unreachable during its
schema bootstrap) leaves the ring in its dual-placement phase — `/status`
shows `"resharding": true` indefinitely. Recovery is automatic on the
joiner's next announce (restart the joining node: the re-announce
finalizes the pending transition); or remove and re-add the node.

### Backups on a cluster

`BACKUP TO '/path'` backs up **the answering node's shard** (a
crash-consistent copy of its data directory — each node backs up its
own). `RESTORE FROM` is refused on a live cluster: swapping one node's
data underneath quorum reads would silently diverge replicas. To restore
a node: stop it, restore its data directory offline, start it, and let
repair converge it.

## Anti-entropy: repair & space reclamation

Replicas converge automatically through **read-repair** (a quorum read writes the
winning version back to stale replicas) and **hinted handoff** (a write to a
down replica is buffered and replayed when it returns). Hints are held in
memory up to a per-replica cap and **spill to a per-replica on-disk log**
beyond it — so a replica that stays down or keeps shedding for a long time
loses no writes (bounded memory, durable across restarts) rather than
dropping the overflow. For a full sweep — e.g.
after a node was down a long time — run an active **repair**
(`Node::repair`/`repair_cluster`), which reconciles every co-replica pair in both
directions, **including the catalog**: databases, tables, and indexes are synced
both ways, so a node that missed a DDL broadcast while it was down gets the
missing schema too. Schema reconciles by **last-writer-wins with tombstones** —
every DDL is HLC-stamped and a `DROP` leaves a versioned tombstone — so a *drop*
that happened while a node was down propagates to it on rejoin, and a lagging
node holding the now-dropped object does **not** resurrect it (the tombstone's
newer stamp wins). A genuinely newer re-`CREATE` still wins over an older drop.

**Continuous anti-entropy.** Beyond read-repair (on reads) and hinted handoff
(for writes to a briefly-down replica), each node runs a full repair pass on a
timer — `cluster.anti_entropy_interval_secs` (default **60s**, `0` disables) — so
a node that *missed a broadcast while it was up* (e.g. a DDL that committed at
quorum while this node was momentarily behind) converges **on its own**, with no
operator action. Passes are staggered per-node so the cluster doesn't repair in
lockstep.

**Automatic catch-up on (re)join.** When a node starts and finds peers, it runs a
catch-up pass in the background as soon as a peer is reachable — the same repair
(schema + data) — so a node that was down converges on its own without an
operator running anything. This covers schema that quorum-DDL couldn't reach
while the node was offline, plus any row writes beyond what hinted handoff
replayed. (A brand-new node added with `cluster add-node` is bootstrapped
explicitly with schema + its share of the data.)

**Automatic join (self-announce).** A node that starts with `seeds` pointing at
an existing cluster but that the cluster doesn't yet know about will **announce
itself** to a reachable seed, which runs `add_member` and broadcasts the new
membership to every node — so you no longer have to run `cluster add-node` by
hand, and you avoid the half-join trap (a node that pulls data via catch-up but
was never admitted to the ring). The announce is a no-op when the seed already
lists the node (symmetric seeds), and is **rejected if the joiner's replication
factor doesn't match the cluster's** — fix the RF and restart rather than form a
cluster whose coordinators disagree on each key's replica set. Joins are still
serialized; do one at a time. `\cluster` cross-checks each peer's membership view
and flags `membership_disagreement` when a peer you route to doesn't list you.

After resharding, **reclaim** (`Node::reclaim`/`reclaim_cluster`) physically frees
space for keys a former owner no longer holds. See
[RESHARDING.md](RESHARDING.md#anti-entropy-keeping-replicas-converged).

## Internode security

By default internode traffic is **unauthenticated** (`internode_auth = "none"`) —
fine on an isolated/trusted network, but anything that can reach a node's
internode port can read data and change membership. Two modes lock it down; every
node must use the **same mode and material**:

**Token** — a shared secret. Peers prove knowledge of it with a mutual
HMAC-SHA256 challenge-response, so the secret never crosses the wire and each
connection uses a fresh nonce (no replay). No encryption.

```toml
[auth]
internode_auth = "token"
internode_token = "a-long-random-shared-secret"      # or:
# internode_keyfile = "/etc/skaidb/cluster.token"     # file holding the secret
```

**Cert** — mutual TLS. Every node presents a certificate signed by a shared CA,
and the channel is encrypted. Node certificates must carry the SAN `DNS:skaidb`
(how peers verify each other without per-node hostnames) and
`extendedKeyUsage = serverAuth, clientAuth`.

```toml
[auth]
internode_auth = "cert"
internode_tls_cert = "/etc/skaidb/node.pem"   # this node's cert (SAN: skaidb)
internode_tls_key  = "/etc/skaidb/node.key"
internode_tls_ca   = "/etc/skaidb/ca.pem"     # CA that signs every node's cert
```

Both are also settable via flags/env (`--internode-auth`, `SKAIDB_INTERNODE_*`).
A node that can't satisfy the configured mode is dropped at the handshake, before
any RPC. **Rollout:** there's no mixed-mode window — turn the mode on with the
same material on every node and restart them together. Client auth is separate —
SCRAM on the binary endpoint and HTTP Basic on REST, plus RBAC; see the
[README](../README.md).

## Resilience

- **A slow or unresponsive replica cannot hang a write.** Every internode
  connection has a bounded read/write timeout, so a peer that is *up but
  not answering* (thrashing under memory pressure, a kernel that accepted
  the socket while the process is stalled) is failed fast — the coordinator
  meets the write quorum from the responsive replicas and hints the slow
  one for handoff, rather than blocking on it. (A *refused* connection
  already failed fast on connect; this covers the connected-but-silent
  case.)
- **Memory-pressure load shedding.** A node watches its memory against its
  limit (cgroup when set, else system RAM). Past 85% it **sheds writes** —
  rejecting new writes (client and inbound-replica) with a retryable
  "memory pressure" error — so it can flush the memtable and commit/merge
  search segments, shrink its footprint, and leave the OS headroom, instead
  of allocating until the OOM killer takes the whole container down. It
  clears the flag at 70% (hysteresis). Reads and DDL are never shed; a
  coordinator that gets a shed rejection from a replica hints it and
  proceeds at quorum. Watch `skaidb_memory_shedding_writes` /
  `skaidb_memory_used_bytes` (METRICS.md); a node stuck shedding is
  undersized for its workload.
- **Bulk index builds stream the table.** Building or rebuilding a search
  index (`CREATE SEARCH INDEX`, startup catch-up, or an automatic rebuild)
  reads the source table one row at a time rather than gathering the whole
  shard into memory first, so indexing a large table stays within a bounded
  footprint (the writer heap, sized from `memory_target`, plus one row) and
  does not OOM a small node. DB workload must never OOM a node.

## Operational notes & limitations

- **Same RF and seed list on every node.** A node's own `bind_addr:internode_port`
  must be in `seeds`, and `bind_addr` must be reachable by peers (not `0.0.0.0`).
- **Distinct `data_dir` per node** (and distinct ports when co-located).
- **No membership gossip/consensus yet.** Static membership is via `seeds`;
  runtime changes are best-effort broadcasts ordered by an epoch and persisted, so
  a restart reloads the live ring. A fresh node auto-announces to a seed to get
  admitted (above), but there's still no continuous gossip — a node that missed a
  membership broadcast while up needs it re-sent, and two *concurrent* topology
  changes aren't linearizable. Do one membership change at a time.
- **Transactions are single-node.** `BEGIN/COMMIT/ROLLBACK` work against the
  embedded engine; the cluster coordinator autocommits per statement.
- **Joins gather to the coordinator.** A single-table `WHERE` is pushed to the
  shards, but a SQL `JOIN` pulls the tables to the coordinating node.
- **Unfiltered scans page, merge, and honour `LIMIT`.** `SELECT … FROM t` with no
  `WHERE` gathers every shard last-writer-wins, paged so the coordinator holds a
  few pages at a time rather than whole shards. A plain `LIMIT n` (no `ORDER BY`)
  is pushed into the gather: sources are paged in lockstep and a row is emitted
  once every still-active replica has scanned past it, so the scan stops after
  the first `n` rows are sealed instead of materialising the whole table.

See [RESHARDING.md](RESHARDING.md) for the deeper design and the current edges.
