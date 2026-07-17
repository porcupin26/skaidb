# Built-in web UI

Every skaidb node serves a small admin UI at `http://<node>:<rest_port>/ui`
(default REST port 7080). It is embedded in the binary at compile time —
no external assets, no JS toolchain, works air-gapped — and is a **pure API
client**: everything it does goes through the same REST endpoints, HTTP
Basic auth, and per-statement RBAC as any other client.

```
http://127.0.0.1:7080/ui
```

## Tabs

- **status** — node (version, ready, uptime) and cluster (members, epoch,
  ring membership, replication factor, consistency levels, hints,
  resharding) cards, plus a members table with configured-vs-ring
  discrepancies; a **drivers** table of live binary-protocol connections
  (node, endpoint, remote address, authenticated user, connected duration —
  `GET /ui/drivers`, reading the replicated `drivers` table; REST
  connections aren't tracked here, since REST is one request per
  connection and would just be table churn for no signal); and a
  **witnesses** table of registered cross-region backup nodes (id, region,
  registered/last-seen duration, dimmed/flagged when a witness has gone
  quiet past the GC grace period — `GET /ui/witnesses`, reading the
  replicated `witnesses` table, shown alongside the grace period currently
  in effect from `witness_gc_config`). A witness registers itself the same
  way any client would, over an ordinary SQL connection with witness-scoped
  credentials — see `.priv/witness-node-plan.md` for the design (not
  committed; ask if you need it). Auto-refreshes every 5 s; the
  drivers/witnesses fetches are independent of the core status fetch, so a
  failure there dims those two tables without breaking the rest of the tab.
- **query** — SQL console with a **schema browser**: databases and tables
  the logged-in role may read (`GET /ui/schema`, filtered server-side by
  the same RBAC check `/query` enforces — table, database, and global
  grants, role inheritance included); clicking a table targets its
  database and pre-fills a `SELECT`. Ctrl/⌘+Enter runs, Alt+↑/↓ cycles history
  — and when a result looks like a time series (a `ts`/`time`/`bucket`
  column plus numeric columns), a line chart renders above the table
  (kept in `localStorage`, statements only — never results or
  credentials), canned-statement and history dropdowns, CSV/JSON export,
  client-measured latency. Results render escaped; FTS `HIGHLIGHT()`
  snippets are the one exception, rendered by splitting on the literal
  `<b>`/`</b>` tokens. Display is capped at 1000 rows with a visible
  banner suggesting a `LIMIT`. `USE <db>` is tracked client-side and sent
  per request (`POST /query` accepts an optional `"db"` JSON key).
- **search** — the FTS playground: pick a table/column/predicate
  (`MATCH`/`MATCH_PHRASE`/`FUZZY`/`WILDCARD`/`REGEXP`/`MORE_LIKE_THIS`/
  `SEARCH`), and it builds the SQL (with `score()` + `HIGHLIGHT()`) and
  runs it in the query console; a `SUGGEST` tester; and an
  Elasticsearch-subset request tester (method + path + JSON body →
  pretty-printed response, same auth as everything else).
- **stats** — a per-node system table first (CPU%, load, RAM used/total
  — cgroup-aware in containers —, process RSS, disk read/write
  throughput, and data-directory disk space, one row per cluster member
  via `GET /ui/hosts`, with a cluster totals row on multi-node
  deployments and unreachable members flagged); then queries/s, mean
  latency, rows scanned/s, bytes returned/s with canvas sparklines (5 s
  samples, 5 min window), and storage/cache/WAL counters. Polls only
  while the tab is visible. (Per-table and per-index breakdowns live on
  the **inventory** tab.)
- **inventory** — the consolidated schema-and-storage view
  (`GET /ui/inventory`, RBAC-filtered like the schema browser): every
  database's tables (type — table / memory / timeseries —, key, TTL,
  approximate row count, tombstones, disk, file count) and indexes
  (secondary / vector / search, with paths, vector dim·metric·ef,
  entry/doc counts, disk). Usage numbers are the serving node's; counts
  are approximate until compaction. The per-table and per-search-index
  breakdowns that used to live on the stats tab moved here.
- **config** — the masked config (`/admin/config`), one card per section,
  with per-key set. The result states whether the key applied live or
  needs a restart; every set persists to the node's config file.
- **admin** — repair / reclaim / add node / remove node behind
  confirmation dialogs, and the slow-query log (masked SQL).

The config and admin tabs only appear for roles with the `Admin`
privilege (the UI probes `/admin/config` at login). Hiding them is UX,
not security — the server enforces RBAC on every request regardless.

## Enable / disable

The UI is on by default and exactly as exposed as `POST /query` on the
same port. To remove the surface entirely:

```toml
[ui]
enabled = false
```

`ui.enabled` is live-mutable — no restart, effective on the next request,
and every `/ui` path returns the same 404 a UI-less build would:

```
skaidbsh> \ui            -- print each node's UI URL + enabled state
skaidbsh> \ui off        -- disable live (persists to the config file)
skaidbsh> \ui on
skaidbsh> \config set ui.enabled false   -- equivalent generic form
```

## Security model

- **Auth**: the shell page is static and secret-free, so it serves
  unauthenticated; every data call is a `fetch()` with an explicit
  `Authorization: Basic` header from the login form. Credentials live in
  JS memory, or `sessionStorage` with the "remember for this tab"
  opt-in — never `localStorage`, cookies, or URLs. Logging out drops them
  and clears fetched results from the screen.
- **No CSRF surface**: no cookies and no server-side sessions means no
  ambient credential for a cross-site request to ride.
- **CSP**: every `/ui` response carries
  `default-src 'none'; script-src 'self'; style-src 'self';
  img-src 'self' data:; connect-src 'self'` and
  `X-Content-Type-Options: nosniff` — the no-external-assets rule,
  enforced by the browser too.
- **XSS**: all server-derived text renders via `textContent`; the sole
  deliberate exception is the highlight-token renderer described above,
  which never interprets HTML.
- **`GET /ui/meta`** (the one new JSON endpoint) carries version,
  node id, clustered flag, whether auth is required, and uptime — the
  same trust level as `/health` and `/status`.
- **Audit**: console queries are ordinary `/query` calls and ride the
  existing query log / audit settings unchanged.
- **TLS**: same as the REST endpoint — put a TLS-terminating proxy in
  front on untrusted networks; Basic auth wants it.

## Cluster notes

The UI talks only to the node that served it; the status tab lists the
peers' client endpoints, and each peer serves its own `/ui`. There is no
cross-node proxying.
