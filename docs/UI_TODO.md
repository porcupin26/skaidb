# Web UI — implementation plan

Goal: a **built-in, zero-dependency admin UI** served by the node itself —
status, statistics, configuration, and a SQL query console — reusing the
existing authentication (HTTP Basic over the SCRAM user store) and
per-statement RBAC unchanged, and toggleable via `[ui] enabled` in the
config file and live via `\config set ui.enabled` from the CLI.

This is the working plan (the FTS_TODO.md pattern): decisions and the
phased roadmap live here; shipped state moves to a `docs/UI.md` feature
doc as phases land.

---

## 0. Principles (constraints, not preferences)

- **The single static binary stays sacred.** All UI assets are embedded at
  compile time (`include_str!`); no JS build toolchain, no npm, no
  framework, no CDN or external request of any kind. The UI works
  air-gapped and is versioned with the binary it ships in.
- **The UI is a pure API client, not a privileged surface.** Every action
  it performs goes through the same REST endpoints with the same Basic
  auth and RBAC as any other client. Authorization is enforced
  server-side per request; the UI merely *adapts* its chrome to the role
  (hiding an admin tab is UX, never security).
- **Near-zero cost when disabled.** A single live-config check per
  request on the `/ui` prefix; `404` when off (indistinguishable from a
  build without it — no information leak).

---

## 1. Architecture

- **Serving**: the existing REST listener (`server.rest_port`). Routes:
  - `GET /ui` — the embedded single-page shell (one self-contained HTML
    file with inlined CSS + vanilla JS; hand-written, target ≤ ~100 KB).
  - `GET /ui/meta` — tiny unauthenticated JSON: `version`, `node_id`,
    `clustered`, `auth_required`. The login screen needs to know whether
    to ask for credentials before any authenticated call can succeed;
    carries nothing secret (same trust level as `/health`).
  - Everything else the UI does uses **existing** endpoints: `POST
    /query` (SQL in, JSON rows out), `GET /status`, `GET /metrics`
    (Prometheus text — trivially parsed client-side), and the `POST
    /admin/*` verbs (`status`, `slow`, `config[/get|/set]`, `repair`,
    `reclaim`, `add-node`, `remove-node`). No new data endpoints are
    required for v1.
- **Auth flow**: the shell page itself is static and secret-free, so it
  serves unauthenticated; every *data* call is `fetch()` with an
  `Authorization: Basic` header built from a login form. Credentials live
  in JS memory (opt-in "remember for this tab" = `sessionStorage`; never
  `localStorage`). No cookies and no server-side sessions → no ambient
  credential, so **no CSRF surface**; the browser's native Basic-auth
  prompt never appears because auth failures happen on `fetch`, not
  navigations.
- **RBAC adaptation**: after login the UI probes what the role can do
  (e.g. `SHOW GRANTS` for itself, a no-op `/admin/config` call) and hides
  what's denied. The server remains the boundary — a hand-crafted request
  from a non-admin gets the same `permission denied` it gets today.
- **Cluster awareness**: the UI talks to the one node it loaded from; the
  cluster page lists peers (from `/status`) with links to *their* `/ui`.
  No cross-node proxying.

---

## 2. Feature matrix

**Status** (phase 1)
- Node: version, uptime, ready/health, data dir, ports, role.
- Cluster: members + reachability, epoch, `self_in_ring`, replication
  factor, read/write consistency, hints pending, resharding flag (all
  already in `/status` + `/admin/status`).

**Query console** (phase 2)
- SQL editor (plain `<textarea>` + shortcuts: ⌘⏎ run, ↑ history), results
  as an escaped table, affected-count and error display, execution time.
- History in `localStorage` (statements only, never results/credentials).
- Canned statements menu (`SHOW TABLES` / `SHOW INDEXES` / `SHOW STATUS` /
  `SUGGEST … ON …`), `USE <db>` awareness, client-side CSV/JSON export.
- Long results: rely on `LIMIT` + a UI default (append `LIMIT 500` hint
  banner rather than silently truncating).

**Stats** (phase 3)
- Auto-refresh (5 s) dashboards from `SHOW STATUS` rows + `/metrics`
  gauges: storage per table, memtable/cache, connections, query counters,
  FTS per-index (`search.*` rows: docs/disk/uncommitted), TS head/blocks.
- Client-side sparklines from the polling history (no server change).

**Configuration & admin ops** (phase 4)
- Masked full-config view (`/admin/config` already masks secrets),
  per-key get/set with live-mutable vs restart-required marking (the
  existing `config set` semantics).
- Cluster operations with confirm dialogs: repair, reclaim, add/remove
  node; slow-query log viewer (`/admin/slow`).

**Later** (phase 5+, demand-driven)
- FTS playground (query + highlight + SUGGEST tester), PromQL/TS mini
  graphs, ES-subset request tester, dark mode.

---

## 3. Enable / disable

- **Config**: new `[ui] enabled = true` section (`skaidb-config`).
  Default **on** — the UI is exactly as exposed as `POST /query` on the
  same port with the same auth; operators wanting zero surface set
  `false` (and get a `404`).
- **Live toggle**: `ui.enabled` joins the live-mutable config keys — the
  `/ui` route guard reads the live config per request, so
  `\config set ui.enabled false` (or the `/admin/config/set` HTTP verb)
  takes effect immediately, no restart, and persists to the config file
  through the existing `config set` persistence.
- **CLI**: nothing new needed beyond the existing generic
  `\config set ui.enabled true|false`; add a convenience `\ui` command in
  skaidbsh that prints the URL (`http://<rest_addr>/ui`) and the current
  enabled state.

---

## 4. Security

- **CSP** on every `/ui` response: `default-src 'none'; script-src
  'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'` —
  the no-external-assets rule enforced by the browser too, and inline
  script kept out (the shell references `/ui/app.js` served embedded, or
  uses hashed inline — decide at implementation, hashes preferred).
- **XSS**: query results and any server-derived text render via
  `textContent`, never `innerHTML`. One deliberate exception: FTS
  `HIGHLIGHT()` snippets contain `<b>` marks — render them by splitting
  on the known `<b>`/`</b>` tokens, never by trusting arbitrary HTML.
- **CSRF**: none by construction (no cookies; every mutating call
  requires the explicit `Authorization` header).
- **Credentials**: JS memory by default, `sessionStorage` opt-in, a
  visible logout that drops them; never written to `localStorage` or
  URLs.
- **Secrets**: the config view reuses the existing masking; `/ui/meta`
  carries none.
- **Audit**: UI queries are ordinary `/query` calls and ride the existing
  query log / audit settings unchanged.
- **TLS**: same story as the REST endpoint today (terminate at a proxy);
  document plainly that Basic auth wants TLS in front on untrusted
  networks.

---

## 5. Phases (each ends tested, clippy-clean, docs updated — the FTS cadence)

- [x] **Phase 1 — skeleton + status**: `[ui]` config section wired
  through `skaidb-config` → server route guard (live-checked); embedded
  shell + `GET /ui` + `GET /ui/meta`; login flow against Basic auth; the
  status page (node + cluster). Exit: UI loads and shows live status on
  single-node and on the 3-node test cluster; `\config set ui.enabled
  false` 404s it immediately and back; server tests cover route gating,
  meta shape, and disabled-mode 404.
  *Shipped notes*: assets live in `crates/skaidb-server/assets/`
  (`ui.html`/`ui.css`/`ui.js`, `include_str!` in `src/ui.rs`), served as
  three files (`/ui`, `/ui/app.css`, `/ui/app.js`) rather than one
  inlined page — keeps CSP hash-free (`script-src 'self'`). `/ui/meta`
  also carries `uptime_seconds` (from `ctx.start`; `/status` has no
  uptime). Login verifies credentials against `POST /query` (`SHOW
  TABLES`) because `/status` is unauthenticated. Members table renders
  from `endpoints` + the `configured_not_in_ring` /
  `ring_not_configured` discrepancy lists. `\ui [on|off]` landed in
  skaidbsh. Cluster-side visual check rides the next release rollout.
- [x] **Phase 2 — query console**: editor, results table, errors,
  history, exports, canned statements. Exit: FTS (`MATCH`/`HIGHLIGHT`),
  TS, and plain relational queries all render correctly incl. the
  highlight-token renderer; RBAC denials surface as clean inline errors
  (verified with a read-only role).
  *Shipped notes*: `USE` awareness is client-side — the console tracks
  the current db and sends it per request as `{"sql", "db"}`, a new
  optional JSON key on `POST /query` (the stateless gateway runs the
  statement with that session db; bad names fail like `USE`). Ctrl/⌘+
  Enter runs, Alt+↑/↓ cycles history (localStorage, statements only,
  cap 100), canned-statement + history dropdowns, CSV/JSON export via
  Blob download, display capped at 1000 rows with a visible add-a-LIMIT
  banner, results cleared on logout. Server test covers the `db` key;
  browser-side RBAC/readonly check rides the cluster verification.
- [ ] **Phase 3 — stats dashboards**: storage/FTS/TS/cluster panels with
  auto-refresh + sparklines. Exit: numbers cross-checked against
  `SHOW STATUS` and `/metrics` on the test cluster under load.
- [ ] **Phase 4 — config + admin ops**: config viewer/editor, repair/
  reclaim/add/remove-node with confirmations, slow-log view. Exit: a
  node join driven entirely from the UI on the test cluster; non-admin
  role sees no admin tab and gets server-side denials if it tries.
- [ ] **Phase 5 — polish & hardening**: CSP/XSS audit pass, keyboard UX,
  dark mode, `docs/UI.md` feature doc + README screenshot; fleet
  verification rides a release rollout.

---

## 6. Risks / open questions

- **`Connection: close` HTTP**: the REST server serves one request per
  connection. Fine for an admin UI's request rates; if the auto-refresh
  dashboards ever feel it, adding keep-alive to the REST server is a
  bounded, separately-testable change — measure before doing it.
- **Hand-rolled frontend drift**: no framework means conventions must be
  established early (one `app.js`, small view functions per tab, no
  innerHTML rule) — the phase-5 audit enforces them.
- **Binary size**: ~100 KB of embedded assets against a ~14 MB binary —
  negligible, but track it in the release size check.
- **Browser support**: evergreen browsers only (fetch, ES2020); no
  polyfills.
