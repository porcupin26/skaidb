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
  discrepancies. Auto-refreshes every 5 s.
- **query** — SQL console. Ctrl/⌘+Enter runs, Alt+↑/↓ cycles history
  (kept in `localStorage`, statements only — never results or
  credentials), canned-statement and history dropdowns, CSV/JSON export,
  client-measured latency. Results render escaped; FTS `HIGHLIGHT()`
  snippets are the one exception, rendered by splitting on the literal
  `<b>`/`</b>` tokens. Display is capped at 1000 rows with a visible
  banner suggesting a `LIMIT`. `USE <db>` is tracked client-side and sent
  per request (`POST /query` accepts an optional `"db"` JSON key).
- **stats** — queries/s, mean latency, rows scanned/s, bytes returned/s
  with canvas sparklines (5 s samples, 5 min window), storage/cache/WAL
  counters, and per-table / per-search-index breakdowns from
  `SHOW STATUS`. Polls only while the tab is visible.
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
