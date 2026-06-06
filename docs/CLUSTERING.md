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

These operations are exposed today as **`Node` library APIs**, not yet as a
`skaidb` server subcommand or admin endpoint:

```rust
// Driven from an embedded/admin context holding the cluster `Node`:
node.add_member("10.0.0.4:7100", "10.0.0.4:7100")?; // join: migrates its share in
node.remove_member("10.0.0.3:7100")?;               // decommission: drains, then leaves
node.reclaim_cluster()?;                             // free space former owners no longer own
node.repair_cluster()?;                              // anti-entropy: converge replicas
```

`add_member` broadcasts the new ring, bootstraps the joiner's schema, and streams
it the keys it now owns (dual-writing during the move so concurrent writes stay
correct); `remove_member` drains the leaving node's keys to their new owners
before dropping it from the ring.

**Server-managed alternative (maintenance window), no runtime API:**

1. Add the new node's `host:internode_port` to the `seeds` list on **every** node
   (and on the new node).
2. Start the new node; restart the existing nodes so they adopt the larger ring.
3. Run an anti-entropy pass so the new node's replicas fill in.

This works best with `replication_factor > 1` (anti-entropy can populate the new
replica from a peer). With `RF = 1` a moved key's *only* owner becomes the new
(empty) node, so prefer the online `add_member` path, which actually migrates the
data. Note: a node that has already gone through an online membership change
persists its ring and uses that over the static `seeds` list on restart.

## Anti-entropy: repair & space reclamation

Replicas converge automatically through **read-repair** (a quorum read writes the
winning version back to stale replicas) and **hinted handoff** (a write to a
down replica is buffered and replayed when it returns). For a full sweep — e.g.
after a node was down a long time — run an active **repair**
(`Node::repair`/`repair_cluster`), which reconciles every co-replica pair in both
directions. After resharding, **reclaim** (`Node::reclaim`/`reclaim_cluster`)
physically frees space for keys a former owner no longer holds. Both are library
APIs today; see [RESHARDING.md](RESHARDING.md#anti-entropy-keeping-replicas-converged).

## Internode security

Node-to-node traffic can be authenticated with a shared keyfile:

```toml
[auth]
internode_auth = "keyfile"
internode_keyfile = "/etc/skaidb/internode.key"   # same file on every node
```

(or `--internode-auth keyfile --internode-keyfile …`). Client auth is separate —
SCRAM on the binary endpoint and HTTP Basic on REST, plus RBAC; see the
[README](../README.md).

## Operational notes & limitations

- **Same RF and seed list on every node.** A node's own `bind_addr:internode_port`
  must be in `seeds`, and `bind_addr` must be reachable by peers (not `0.0.0.0`).
- **Distinct `data_dir` per node** (and distinct ports when co-located).
- **No membership gossip/consensus yet.** Static membership is via `seeds`;
  runtime changes are best-effort broadcasts ordered by an epoch and persisted, so
  a restart reloads the live ring — but a node that missed a membership broadcast
  needs it re-sent, and two *concurrent* topology changes aren't linearizable. Do
  one membership change at a time.
- **Transactions are single-node.** `BEGIN/COMMIT/ROLLBACK` work against the
  embedded engine; the cluster coordinator autocommits per statement.
- **Joins gather to the coordinator.** A single-table `WHERE` is pushed to the
  shards, but a SQL `JOIN` pulls the tables to the coordinating node.

See [RESHARDING.md](RESHARDING.md) for the deeper design and the current edges.
