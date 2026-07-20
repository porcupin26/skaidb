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
- [Per-table placement (RF overrides & pins)](#per-table-placement-rf-overrides--pins)
- [Verify the cluster](#verify-the-cluster)
- [Add or remove nodes at runtime (online resharding)](#add-or-remove-nodes-at-runtime-online-resharding)
- [Anti-entropy: repair & space reclamation](#anti-entropy-repair--space-reclamation)
- [Internode security](#internode-security)
- [Witness nodes](#witness-nodes)
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

## Per-table placement (RF overrides & pins)

The cluster RF is the default, not the law. Any table can override it at
CREATE or later:

```sql
CREATE TABLE metrics_cold (PRIMARY KEY (id)) WITH (replication = 1)
CREATE TABLE hot_config  (PRIMARY KEY (k))  WITH (nodes = ['skai2'])
ALTER  TABLE metrics_cold SET (replication = 2)   -- online transition
ALTER  TABLE hot_config   SET (nodes = ['skai3']) -- online pin move
```

- **`replication = n`** — ring placement at a per-table copy count
  (`n >=` member count behaves as a full copy, like cluster-wide RF
  does). Quorums for that table's reads and writes derive from *its*
  replica count, not the cluster default.
- **`nodes = ['<alias-or-id>', ...]`** — the whole table lives on
  exactly those members (aliases resolve to stable internode ids at DDL
  time; renames never move data). Every pin holds every row; a non-pin
  coordinator routes reads and writes to the pins. Pins are a
  durability trade the operator owns: a pinned node down means quorum
  errors for that table until it returns, and
  `ALTER CLUSTER REMOVE NODE` refuses to remove a pinned member until
  it is re-pinned away. Mutually exclusive with `replication`.
- **Changing placement is online.** The ALTER opens a *dual-placement
  window*: the table's old and new placement are both live, and every
  read/write addresses the union — the per-table twin of the
  membership-change dual ring — so quorum reads stay correct while new
  owners are still empty. A background driver (the sorted union's first
  member) repairs until every member has completed a full anti-entropy
  pass that began after the change, then finalizes automatically.
  `SHOW TABLES` shows `transition = true` (the UI inventory tab shows
  `→ moving`) while the window is open. One transition per table at a
  time; if the driver node is down the window just stays open — safe,
  merely wider than needed — and the operator escape is
  `REPAIR CLUSTER` followed by
  `ALTER TABLE t SET (placement_finalized = true)`. After finalize,
  `RECLAIM` trims the copies the new placement no longer owns.
- GLOBAL-index entry tables follow their base table's placement; system
  tables refuse placement options; sharded tables (RF below member
  count) are scatter-pulled and merged by witness nodes, so mirrors
  stay complete for any placement.

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
dropping the overflow. A drain that aborts (peer busy, restart mid-pass)
is retried by a 60-second ticker while any backlog exists, with delivered
work logged — a large backlog deferred at an unreachable peer logs its
size instead of waiting silently. Tables created `WITH (memory = true)`
are excluded from hinted handoff's durability story and from repair and
reshard data motion entirely: they are ephemeral by contract (empty on
restart, repopulated by their writers). For a full sweep — e.g.
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

**Digest-gated passes.** A repair pass first exchanges a compact XOR digest
(4096 buckets over `key ‖ hlc ‖ op`, ~32 KB) per (table, peer) pair; equal
digests prove the pair converged and skip the full paged compare, so a
steady-state pass ships digests instead of tables. Digest computation itself
is cheap: it scans **value-free stamps** — each SSTable carries a
`<file>.stamps` sidecar holding just `(key, hlc, op)` per entry, so no row
value is even decompressed (tables written before the sidecar existed fall
back to data blocks until compaction rewrites them). On full-copy clusters
(replication factor ≥ member count) each node also **caches its digest per
table**, keyed on a `(schema stamp, write-sequence)` version, so an idle
table's digest is served from memory — a fully converged pass touches no
table data at all. The cache can never mask divergence: any write bumps the
version on the node that has it, forcing a fresh digest — and therefore a
mismatch — on at least one side of the pair.

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

Generate the CA and per-node leaf certs with the built-in helper (no OpenSSL
incantations, no locale footguns):

```bash
skaidbsh certs gen --out ./skaidb-certs --nodes 3
# writes ca.crt, ca.key, and node1..node3.{crt,key}, all 0600
```

Give each node its own leaf (`node<i>.crt`/`node<i>.key`) plus the shared
`ca.crt`; keep `ca.key` **off** the nodes (it's the issuing root — store it
offline for future cert minting). Then:

```toml
[auth]
internode_auth = "cert"
internode_tls_cert = "/etc/skaidb/node.crt"   # this node's leaf (SAN: skaidb)
internode_tls_key  = "/etc/skaidb/node.key"
internode_tls_ca   = "/etc/skaidb/ca.crt"     # CA that signs every node's cert
```

Both are also settable via flags/env (`--internode-auth`, `SKAIDB_INTERNODE_*`).
A node that can't satisfy the configured mode is dropped at the handshake, before
any RPC. **Rollout:** there's no mixed-mode window — a `cert` node and a
`none`/`token` node cannot talk, so turn the mode on with the same material on
every node and restart them together (a brief flag-day; clients fail over and
retry). The effective mode is surfaced at **`GET /status`** as
`"internode_auth": "none" | "token" | "cert"`, so a monitoring check can catch a
node that silently came up unauthenticated. Client auth is separate — SCRAM on
the binary endpoint and HTTP Basic on REST, plus RBAC; see the
[README](../README.md).

> **Encryption note:** only `cert` mode encrypts the internode channel.
> `none` and `token` are plaintext on the wire (`token` authenticates but does
> not encrypt).

### Client TLS (driver ↔ cluster)

The binary (7000) and REST (7080) ports can be TLS-wrapped independently of
internode mode. Set `[encryption]`:

```toml
[encryption]
client_tls = "opportunistic"   # off | opportunistic | required
tls_cert_file = "/etc/skaidb/server.crt"
tls_key_file  = "/etc/skaidb/server.key"
```

- **`off`** (default) — plaintext only.
- **`opportunistic`** — one port serves both: a TLS ClientHello is wrapped, a
  plaintext connection is served as before. Use this to migrate — point every
  client at TLS, confirm, then switch to `required`.
- **`required`** — plaintext connections (including probes on the REST port)
  are refused. TLS only.

Clients authenticate with SCRAM/HTTP-Basic **inside** the TLS channel (client
certs are not required — this is one-way server TLS). A misconfiguration
(`client_tls` on but cert/key unset) fails the listener startup **loud**,
never silently plaintext; the effective mode shows at `GET /status` as
`client_tls`. The server cert can be any TLS cert; the cluster CA from
`skaidbsh certs gen` works (its node certs carry SAN `skaidb`). Connect with:

```bash
skaidbsh -H node --tls --tls-ca ca.crt          # verify against the cluster CA
skaidbsh -H node --tls --tls-insecure           # self-signed / dev (INSECURE)
# --tls-server-name <name>  overrides the verified SAN (default: skaidb)
```

The driver takes `Client::connect_many_tls(endpoints, user, pw, Some(tls))`.

### At-rest encryption

Encrypt every table's and index's WAL and SSTables on disk with AES-256-GCM.
The scheme is envelope: a **KEK** from a keyfile wraps a per-file **DEK** that
seals the data, so key rotation is cheap and the KEK never touches data.

```bash
skaidbsh keyfile gen --out /etc/skaidb/at-rest.key   # 32 bytes, 0600
```

```toml
[encryption]
at_rest_enabled = true
at_rest_kek_source = "keyfile"          # kms: not yet implemented
at_rest_keyfile = "/etc/skaidb/at-rest.key"
```

- **New files encrypt; existing plaintext files stay readable** (mixed
  migration). To fully encrypt an existing node, do a **rolling per-node
  resync**: wipe the node's data dir and let it rebuild from peers onto the
  encrypted engine — one node at a time, RF keeps the cluster serving (the
  same shape as re-encrypting any replicated store).
- A **missing or bad keyfile fails startup loud** — the node never comes up
  silently unencrypted. `at_rest_enabled` is restart-scoped. The effective
  state shows at `GET /status` as `at_rest`.
- **Back up the keyfile off-box before enabling.** Losing it makes all
  encrypted data unrecoverable — it is operator-critical.
- The WAL and SSTables are ciphertext on disk; the stamps sidecar is omitted
  for encrypted tables (stamp scans fall back to decoding data blocks —
  correct, slightly slower for repair).

## Resilience

- **A slow or unresponsive replica cannot hang a write.** Every internode
  connection has a bounded read/write timeout, so a peer that is *up but
  not answering* (thrashing under memory pressure, a kernel that accepted
  the socket while the process is stalled) is failed fast — the coordinator
  meets the write quorum from the responsive replicas and hints the slow
  one for handoff, rather than blocking on it. (A *refused* connection
  already failed fast on connect; this covers the connected-but-silent
  case.)
- **Memory-pressure release and load shedding.** A node watches its memory
  against its limit (cgroup when set, else system RAM), measuring
  **non-reclaimable** usage — the cgroup charge minus reclaimable file-backed
  page cache (mmap'd SSTable/WAL/search segments the kernel evicts before
  OOM), so a cache-filled node isn't falsely shed while it still has real
  headroom. Two tiers:
  - Past **75%** it **actively releases**: flushes table/index memtables
    (≥4 MB) and commits every dirty search-index writer (Tantivy holds
    indexed-but-uncommitted documents in heap buffers; a node that stops
    taking writes otherwise never commits them and rides its limit until the
    fault storm or the OOM killer gets it — observed in production). Release
    actions are paced (at most every 10 s).
  - Past **85%** it also **sheds writes** — rejecting new writes (client and
    inbound-replica) with a retryable "memory pressure" error — and the
    release pass turns aggressive (flushes memtables down to 64 KB). The
    release is driven by the memory sampler, not a client write — otherwise a
    shedding node would deadlock (it rejects the very writes that would
    trigger a flush) — and covers every memtable, since pressure spread thin
    across many tables leaves each below the per-engine flush threshold while
    the sum pins the node. The flag clears at 70% (hysteresis).

  Shedding is loud: entering it logs the anon/file split plus jemalloc's
  allocated/resident/retained numbers, a distress line repeats every 60 s
  while it persists ("releases are not freeing enough; OOM risk"), and
  recovery logs the episode's duration. Anti-entropy passes log their
  duration (and allocator stats) whenever they reconcile rows or take ≥60 s.
  The packaged unit also sets `MemoryHigh=85%` so the kernel throttles and
  reclaims the service before a hard OOM kill (which costs a restart + full
  search-index rebuild). Reads and DDL are never shed; a coordinator that
  gets a shed rejection from a replica hints it and proceeds at quorum. Watch
  `skaidb_memory_shedding_writes` / `skaidb_memory_used_bytes` (METRICS.md);
  a node stuck shedding is undersized for its workload.
- **Graceful shutdown.** SIGTERM/SIGINT (what systemd's `stop`/`restart`
  send) flush memtables and commit search-index writers before exit, so the
  next start replays almost nothing — an unclean kill costs a full
  search-index rebuild from the last committed watermark. The flush waits at
  most ~10 s for the engine lock, so a wedged node still exits inside
  systemd's kill window.
- **Full-copy counts are local.** When `replication_factor >= members`
  (every node holds every row), unfiltered `COUNT(*)` is answered from the
  local engine's key statistics — no cluster gather. The gather materialized
  the whole merged table on the coordinator (a plain count OOM-killed a
  production node); the local answer is O(keys) with no value decode, at the
  same freshness trade the search paths already make.
- **Compaction commits before it deletes.** Retired SSTables are removed
  only after the manifest durably points at their replacement — the old
  order left a kill window where the manifest named deleted files and the
  engine refused to open (two production incidents). A manifest entry that
  fails to open is logged with the exact file and manifest path.
- **Bulk index builds stream the table.** Building or rebuilding a search
  index (`CREATE SEARCH INDEX`, startup catch-up, or an automatic rebuild)
  reads the source table one row at a time rather than gathering the whole
  shard into memory first, so indexing a large table stays within a bounded
  footprint (the writer heap, sized from `memory_target`, plus one row) and
  does not OOM a small node. DB workload must never OOM a node.

## Witness nodes

A **witness** is a standalone node (never a ring member, never counted
in quorums) that mirrors chosen databases from a primary cluster on its
own schedule — a cross-region, pull-based backup. Configure `[witness]`
in its toml (primary SQL + internode addresses, witness-scoped
credentials, databases) and pair it with `server.read_only = true`
(drivers may connect for read-only queries; every mutation is refused
for non-superusers). Pulls ride the internode protocol near-live:
per-table `write_seq` change hints gate incremental stamps-walked delta
pages (`interval_secs`, default 60 s), with a full sweep every
`full_sweep_interval_secs` (default 24 h) as the anti-entropy backstop;
pulls self-pace to at most `witness.duty_pct` (default 50 %) of the
serving member's capacity. The primary's `witnesses` registry drives
the status-tab sync detail and holds tombstone GC back (up to
`witness_gc_config.grace_period_secs`) until every live witness has
pulled the deletes. Witness mirroring is per-table opt-out:
`WITH (witness = false)` / `ALTER TABLE t SET (witness = false)`.

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
- **Bulk-apply QoS.** Inbound bulk batch appliers (drain, rebalance, hint
  replay, repair) run under admission control — at most half the cores
  (clamped 1–4) apply batches concurrently, so a migration flood can't
  monopolize the CPU and starve foreground queries (measured during a
  decommission: cpu PSI ~80% with io ~0 — pure CPU saturation from
  FTS-indexing inbound rows). The admission wait is **bounded (2 s)**: a
  saturated node answers "busy" fast instead of parking excess connection
  threads on the gate — an unbounded queue let a rejoining node facing every
  peer's catch-up flood accumulate 2 800+ abandoned threads (senders time
  out at 10 s and retry on fresh connections) while making no progress.
  Rejected/timed-out senders degrade to hints, which repair backstops.
  "Busy"/shedding replies count toward the sender's **circuit breaker** like
  transport errors (probes still bypass and close it), so a saturated peer
  gets a cooldown instead of a full-rate retry hammer — without this the
  rejecting node burned cores decoding a flood it kept refusing. Inbound
  repair scans (`ScanPage`/`LocalScan`) take the engine read lock with a
  bounded wait (1.5 s) and answer "busy" rather than parking behind a
  catch-up write flood; repair treats the peer as unreachable for that pass
  and retries next interval. The drain also pauses `migration_pause_ms`
  (floor 10 ms) between chunks. `node_stats` carries `cpu_pressure_pct`
  (PSI) so saturation is visible per node.
- **Broad filters resolve in one merged scan.** A pushed-down `WHERE` (or an
  index scan) gathers candidate keys per member and re-reads them for the
  authoritative LWW version. Few candidates re-read as quorum point reads;
  past ~256 the coordinator switches to a single paged, LWW-merged pass over
  the table intersected with the candidate set — same read-quorum guarantee,
  one scan instead of one RPC fan-out per key (a `count(*)` over a 100k-row
  match previously issued 100k sequential quorum reads).
- **A flapping peer is circuit-broken.** A zombie node (TCP up, application
  unresponsive) used to make every replicated write burn the full internode
  I/O timeout — worse than a cleanly-down peer. After 3 consecutive failures
  a peer's circuit opens: calls to it fail fast (the coordinator hints
  immediately) for a 10 s cooldown, then one call re-tests. Liveness probes
  bypass the breaker and close it on success.
- **Disk-spilled hints drain in bounded pages, only to live peers.** The
  hinted-handoff overflow log replays a page (1024 records) at a time and is
  left untouched while its peer is down. (Previously each flush cycle decoded
  the whole log into memory and rewrote it per-record when the peer was still
  down — a large log could balloon a restarting node to its cgroup limit.)
  The drain also runs after a restart for logs inherited from the previous
  process.
- **Unfiltered scans page, merge, and honour `LIMIT`.** `SELECT … FROM t` with no
  `WHERE` gathers every shard last-writer-wins, paged so the coordinator holds a
  few pages at a time rather than whole shards. A plain `LIMIT n` (no `ORDER BY`)
  is pushed into the gather: sources are paged in lockstep and a row is emitted
  once every still-active replica has scanned past it, so the scan stops after
  the first `n` rows are sealed instead of materialising the whole table.
- **A PK pinned by `=`/`IN` is a point-read set.** When every primary-key
  column is pinned by an equality or a literal `IN` list (bound array
  parameters included), the coordinator resolves the exact candidate keys
  (≤ 1000; composite keys cross-multiply) and routes each to its replica
  set — the "fetch these N ids" shape never scatters a filter.
- **`ORDER BY <indexed> LIMIT k` at QUORUM is a distributed sorted top-k**
  (single exact sort key, `k ≤ 1000`): every member returns its local
  index-ordered top candidates (4× overfetched to absorb per-shard
  staleness in the sort column and boundary ties), the bounded union is
  re-read at quorum, and the executor re-sorts — ~`members × 4k` row reads
  instead of gathering the whole match set. Conservative by construction:
  if any member is unreachable or cannot walk in order, or fewer matches
  than `k` resolve, the exact full gather answers instead; every returned
  row is quorum-fresh. (At consistency `ONE` on a full-copy cluster the
  ordered read serves entirely from the local replica's index walk.)
- **Schema repair converges index definitions, not just names.** Every
  repair pass exchanges the full catalog as stamped idempotent DDL; a
  replayed `CREATE … IF NOT EXISTS` whose schema stamp advances **replaces**
  a differing search/vector index definition and rebuilds it — a node that
  missed the DDL that widened an index while down no longer keeps the stale,
  narrower definition forever. `SHOW INDEXES`' `local` column
  (`ok`/`building`/`missing`) exposes each node's live index state for
  cross-ring comparison.

See [RESHARDING.md](RESHARDING.md) for the deeper design and the current edges.
