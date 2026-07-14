// skaidb built-in UI (phase 1: login + status). Pure API client: every
// data call carries HTTP Basic and is authorized server-side; this file
// never uses innerHTML with server-derived data.
"use strict";

const $ = (id) => document.getElementById(id);

// ---- credentials (memory by default; sessionStorage opt-in) ----
let auth = sessionStorage.getItem("skaidb-auth") || null;

function setAuth(user, pass, remember) {
  auth = "Basic " + btoa(`${user}:${pass}`);
  if (remember) sessionStorage.setItem("skaidb-auth", auth);
}

function clearAuth() {
  auth = null;
  sessionStorage.removeItem("skaidb-auth");
}

async function api(method, path, body) {
  const headers = {};
  if (auth) headers["Authorization"] = auth;
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const resp = await fetch(path, {
    method,
    headers,
    body: body === undefined ? undefined : body,
  });
  const text = await resp.text();
  let json = null;
  try { json = JSON.parse(text); } catch { /* non-JSON (e.g. health) */ }
  if (resp.status === 401) throw new AuthError();
  if (!resp.ok) throw new Error(json?.error?.reason || json?.error || text || `HTTP ${resp.status}`);
  return json;
}

class AuthError extends Error {}

// ---- views ----
let meta = { auth_required: true, version: "", node_id: "" };

function show(view) {
  $("login-view").hidden = view !== "login";
  $("app-view").hidden = view !== "app";
  $("logout").hidden = view !== "app" || !meta.auth_required;
}

function kv(tableId, pairs) {
  const table = $(tableId);
  table.textContent = "";
  for (const [k, v] of pairs) {
    const tr = document.createElement("tr");
    const kd = document.createElement("td");
    kd.textContent = k;
    const vd = document.createElement("td");
    if (v instanceof Node) vd.append(v);
    else vd.textContent = v === undefined || v === null ? "—" : String(v);
    tr.append(kd, vd);
    table.append(tr);
  }
}

function mark(ok, label) {
  const span = document.createElement("span");
  span.className = ok ? "ok" : "bad";
  span.textContent = label ?? (ok ? "yes" : "no");
  return span;
}

// ---- status tab ----
async function refreshStatus() {
  const [status, m] = await Promise.all([api("GET", "/status"), api("GET", "/ui/meta")]);
  meta = m;
  kv("node-info", [
    ["version", meta.version],
    ["node", status.node_id || "standalone"],
    ["ready", mark(status.ready)],
    ["uptime", meta.uptime_seconds !== undefined ? fmtDuration(meta.uptime_seconds) : undefined],
  ]);
  if (status.clustered) {
    kv("cluster-info", [
      ["members", status.members],
      ["epoch", status.epoch],
      ["in ring", mark(status.self_in_ring)],
      ["replication factor", status.replication_factor],
      ["read / write consistency", `${status.read_consistency} / ${status.write_consistency}`],
      ["hints pending", status.hints_pending],
      ["resharding", mark(!status.resharding, status.resharding ? "active" : "idle")],
    ]);
    renderMembers(status);
  } else {
    kv("cluster-info", [["mode", "single node"]]);
    renderMembers(null);
  }
  $("status-refreshed").textContent = `refreshed ${new Date().toLocaleTimeString()}`;
}

function renderMembers(status) {
  const tbody = $("members").querySelector("tbody");
  tbody.textContent = "";
  if (!status) return;
  // Live client endpoints, plus configured-vs-ring discrepancies the
  // server surfaces explicitly.
  const halfJoined = new Set(status.configured_not_in_ring || []);
  const unconfigured = new Set(status.ring_not_configured || []);
  // Per-peer replication backlog/lag, keyed by host (peer ids and client
  // endpoints share a host but differ in port). One node per host here.
  const byHost = new Map();
  for (const p of status.peers || []) byHost.set(hostOf(p.id), p);
  for (const endpoint of (status.endpoints || []).slice().sort()) {
    const tr = document.createElement("tr");
    const idc = document.createElement("td");
    idc.textContent = endpoint;
    const cc = document.createElement("td");
    cc.append(mark(!unconfigured.has(endpoint)));
    const rc = document.createElement("td");
    rc.append(mark(!halfJoined.has(endpoint)));
    tr.append(idc, cc, rc, backlogCell(byHost.get(hostOf(endpoint))), lagCell(byHost.get(hostOf(endpoint))));
    tbody.append(tr);
  }
  for (const id of [...halfJoined].sort()) {
    const tr = document.createElement("tr");
    const idc = document.createElement("td");
    idc.textContent = `${id} (configured, not in ring)`;
    idc.className = "bad";
    tr.append(idc, td(), td(), backlogCell(byHost.get(hostOf(id))), lagCell(byHost.get(hostOf(id))));
    tbody.append(tr);
  }
}

function hostOf(idOrEndpoint) {
  return String(idOrEndpoint || "").split(":")[0];
}

function td() {
  return document.createElement("td");
}

// Hinted-handoff backlog for a peer: how many writes are buffered awaiting
// handoff. 0 (or self, no peer entry) is caught up; any backlog is flagged.
function backlogCell(peer) {
  const c = td();
  if (!peer) {
    c.textContent = "—";
    c.className = "muted";
  } else if (peer.hints_pending > 0) {
    c.textContent = peer.hints_pending;
    c.className = "bad";
  } else {
    c.textContent = "0";
  }
  return c;
}

// Replication lag for a peer in ms: the gap between the last write this node
// coordinated and the latest one the peer confirmed. null until first confirm.
function lagCell(peer) {
  const c = td();
  if (!peer || peer.lag_ms == null) {
    c.textContent = "—";
    c.className = "muted";
  } else {
    c.textContent = fmtLag(peer.lag_ms);
    if (peer.lag_ms >= 5000) c.className = "bad";
    else if (peer.lag_ms >= 1000) c.className = "warn";
  }
  return c;
}

function fmtLag(ms) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.round(ms / 60000)}m`;
}

// ---- tabs ----
// ---- inventory tab: databases → tables and indexes, with usage ----
async function loadInventory() {
  const inv = await api("GET", "/ui/inventory");
  const fmtTtl = (ms) => (ms == null ? "—" : ms >= 3600000 ? `${ms / 3600000}h` : `${ms / 1000}s`);
  const cell = (text) => {
    const td = document.createElement("td");
    td.textContent = text;
    return td;
  };
  const tb = $("inv-tables").querySelector("tbody");
  const ib = $("inv-indexes").querySelector("tbody");
  tb.textContent = "";
  ib.textContent = "";
  for (const db of inv.databases || []) {
    for (const t of db.tables || []) {
      const tr = document.createElement("tr");
      tr.append(
        cell(db.name), cell(t.name), cell(t.kind),
        cell((t.key || []).join(", ")), cell(fmtTtl(t.ttl_ms)),
        cell(t.live_keys ?? "—"), cell(t.tombstones ?? "—"),
        cell(fmtBytes(t.disk_bytes ?? 0)), cell(t.files ?? "—"),
      );
      tb.append(tr);
    }
    for (const x of db.indexes || []) {
      const detail =
        x.kind === "vector"
          ? `dim ${x.dim} · ${x.metric}${x.ef_search ? ` · ef ${x.ef_search}` : ""}`
          : x.kind === "search"
            ? `${x.uncommitted ?? 0} uncommitted`
            : "";
      const tr = document.createElement("tr");
      tr.append(
        cell(db.name), cell(x.name), cell(x.kind),
        cell(`${x.table} (${(x.paths || []).join(", ")})`), cell(detail),
        cell(x.entries ?? "—"), cell(fmtBytes(x.disk_bytes ?? 0)),
      );
      ib.append(tr);
    }
  }
  $("inv-refreshed").textContent = `refreshed ${new Date().toLocaleTimeString()} · usage numbers are this node's (counts approximate until compaction)`;
}

function showTab(name) {
  for (const btn of document.querySelectorAll("#tabs button")) {
    btn.classList.toggle("active", btn.dataset.tab === name);
  }
  for (const section of document.querySelectorAll("main#app-view > section")) {
    section.hidden = section.id !== `tab-${name}`;
  }
  // The stats tab polls only while visible (two requests every 5s).
  clearInterval(statsTimer);
  statsTimer = null;
  if (name === "stats") {
    statsTick();
    statsTimer = setInterval(statsTick, STATS_INTERVAL_MS);
  }
  const authFail = (e) => { if (e instanceof AuthError) logout(); };
  if (name === "config") loadConfig().catch(authFail);
  if (name === "admin") loadSlow().catch(authFail);
  if (name === "query") {
    loadSchema().catch(authFail);
    $("q-sql").focus();
  }
  if (name === "search") populateSearchTables();
  if (name === "inventory") loadInventory().catch(authFail);
}

document.querySelector("#tabs").addEventListener("click", (ev) => {
  const tab = ev.target.dataset?.tab;
  if (tab) showTab(tab);
});

// ---- query console ----
let currentDb = "default";
let lastResult = null; // {columns, rows} of the last SELECT, for exports
let history = [];
let histIdx = -1; // cursor for Alt+arrow cycling (-1 = live input)
const MAX_HISTORY = 100;
const MAX_RENDER = 1000;

try { history = JSON.parse(localStorage.getItem("skaidb-query-history")) || []; } catch { history = []; }

function pushHistory(sql) {
  if (history[0] === sql) return;
  history.unshift(sql);
  history.length = Math.min(history.length, MAX_HISTORY);
  localStorage.setItem("skaidb-query-history", JSON.stringify(history));
  renderHistorySelect();
}

function renderHistorySelect() {
  const sel = $("q-history-sel");
  sel.textContent = "";
  const head = document.createElement("option");
  head.value = "";
  head.textContent = "history…";
  sel.append(head);
  for (const sql of history) {
    const opt = document.createElement("option");
    opt.value = sql;
    opt.textContent = sql.length > 80 ? sql.slice(0, 77) + "…" : sql;
    sel.append(opt);
  }
}

// FTS HIGHLIGHT() snippets carry literal <b>…</b> marks. Render them by
// splitting on those two known tokens — everything else stays text, so no
// other markup in the value can ever become HTML.
function renderCell(td, text) {
  if (text.includes("<b>") && text.includes("</b>")) {
    let bold = false;
    for (const part of text.split(/(<\/?b>)/)) {
      if (part === "<b>") bold = true;
      else if (part === "</b>") bold = false;
      else if (part !== "") {
        if (bold) {
          const b = document.createElement("b");
          b.textContent = part;
          td.append(b);
        } else {
          td.append(part);
        }
      }
    }
  } else {
    td.textContent = text;
  }
}

function renderRows(columns, rows) {
  const box = $("q-results");
  box.textContent = "";
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const hr = document.createElement("tr");
  for (const c of columns) {
    const th = document.createElement("th");
    th.textContent = c;
    hr.append(th);
  }
  thead.append(hr);
  const tbody = document.createElement("tbody");
  for (const row of rows.slice(0, MAX_RENDER)) {
    const tr = document.createElement("tr");
    for (const v of row) {
      const td = document.createElement("td");
      if (v === null) {
        td.textContent = "NULL";
        td.className = "null";
      } else if (typeof v === "object") {
        td.textContent = JSON.stringify(v);
      } else {
        renderCell(td, String(v));
      }
      tr.append(td);
    }
    tbody.append(tr);
  }
  table.append(thead, tbody);
  box.append(table);
  const banner = $("q-banner");
  if (rows.length > MAX_RENDER) {
    banner.textContent = `showing the first ${MAX_RENDER} of ${rows.length} rows — add a LIMIT to keep responses small`;
    banner.hidden = false;
  } else {
    banner.hidden = true;
  }
}

async function runQuery() {
  const sql = $("q-sql").value.trim().replace(/;+\s*$/, "");
  if (!sql) return;
  const err = $("q-error");
  err.hidden = true;
  $("q-meta").textContent = "running…";
  const t0 = performance.now();
  let result;
  try {
    result = await api("POST", "/query", JSON.stringify({ sql, db: currentDb }));
  } catch (e) {
    if (e instanceof AuthError) return logout();
    $("q-meta").textContent = "";
    err.textContent = e.message;
    err.hidden = false;
    return;
  }
  const ms = Math.round(performance.now() - t0);
  pushHistory(sql);
  histIdx = -1;
  const use = sql.match(/^use\s+("?)([A-Za-z_][\w$]*)\1$/i);
  if (use) {
    currentDb = use[2];
    $("q-db").textContent = `db: ${currentDb}`;
  }
  lastResult = null;
  $("q-results").textContent = "";
  $("q-banner").hidden = true;
  $("q-chart").hidden = true;
  $("q-chart").textContent = "";
  if (result && Array.isArray(result.columns)) {
    lastResult = result;
    renderRows(result.columns, result.rows);
    renderChart(result.columns, result.rows);
    $("q-meta").textContent = `${result.rows.length} row${result.rows.length === 1 ? "" : "s"} · ${ms} ms`;
  } else if (result && result.affected !== undefined) {
    $("q-meta").textContent = `${result.affected} affected · ${ms} ms`;
  } else {
    $("q-meta").textContent = `ok · ${ms} ms`;
  }
  $("q-csv").hidden = $("q-json").hidden = !lastResult;
}

function download(name, type, content) {
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([content], { type }));
  a.download = name;
  a.click();
  URL.revokeObjectURL(a.href);
}

function csvEscape(v) {
  const s = v === null ? "" : typeof v === "object" ? JSON.stringify(v) : String(v);
  return /[",\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
}

$("q-csv").addEventListener("click", () => {
  if (!lastResult) return;
  const lines = [lastResult.columns.map(csvEscape).join(",")];
  for (const row of lastResult.rows) lines.push(row.map(csvEscape).join(","));
  download("skaidb-result.csv", "text/csv", lines.join("\n") + "\n");
});

$("q-json").addEventListener("click", () => {
  if (!lastResult) return;
  const objects = lastResult.rows.map((row) =>
    Object.fromEntries(lastResult.columns.map((c, i) => [c, row[i]])));
  download("skaidb-result.json", "application/json", JSON.stringify(objects, null, 2));
});

$("q-run").addEventListener("click", runQuery);

$("q-sql").addEventListener("keydown", (ev) => {
  if (ev.key === "Enter" && (ev.ctrlKey || ev.metaKey)) {
    ev.preventDefault();
    runQuery();
  } else if (ev.altKey && (ev.key === "ArrowUp" || ev.key === "ArrowDown")) {
    ev.preventDefault();
    if (!history.length) return;
    histIdx = ev.key === "ArrowUp"
      ? Math.min(histIdx + 1, history.length - 1)
      : Math.max(histIdx - 1, -1);
    $("q-sql").value = histIdx === -1 ? "" : history[histIdx];
  }
});

$("q-canned").addEventListener("change", (ev) => {
  if (!ev.target.value) return;
  $("q-sql").value = ev.target.value;
  ev.target.value = "";
  runQuery();
});

// ---- schema browser (RBAC-filtered server-side via /ui/schema) ----
async function loadSchema() {
  const note = $("q-schema-note");
  note.hidden = true;
  let schema;
  try {
    schema = await api("GET", "/ui/schema");
  } catch (e) {
    if (e instanceof AuthError) return logout();
    note.textContent = e.message;
    note.hidden = false;
    return;
  }
  const box = $("q-schema");
  box.textContent = "";
  const databases = schema.databases || [];
  for (const db of databases) {
    const head = document.createElement("div");
    head.className = "qdb";
    head.textContent = db.name + " ";
    const count = document.createElement("span");
    count.className = "muted";
    count.textContent = `(${db.tables.length})`;
    head.append(count);
    box.append(head);
    for (const table of db.tables) {
      const btn = document.createElement("button");
      btn.className = "qtable";
      btn.type = "button";
      btn.textContent = table.name;
      btn.title = `${db.name}.${table.name} — primary key: ${table.primary_key ?? "?"}`;
      btn.addEventListener("click", () => {
        currentDb = db.name;
        $("q-db").textContent = `db: ${currentDb}`;
        $("q-sql").value = `SELECT * FROM ${table.name} LIMIT 100`;
        $("q-sql").focus();
      });
      box.append(btn);
    }
  }
  if (!databases.length) {
    note.textContent = "no databases visible to this role";
    note.hidden = false;
  }
}

$("q-schema-refresh").addEventListener("click", loadSchema);

$("q-history-sel").addEventListener("change", (ev) => {
  if (!ev.target.value) return;
  $("q-sql").value = ev.target.value;
  ev.target.value = "";
  $("q-sql").focus();
});

// ---- search tab (FTS playground, SUGGEST, ES tester) ----
function sqlQuote(s) {
  return "'" + s.replaceAll("'", "''") + "'";
}

// Run a built statement through the query console (one rendering path:
// highlight tokens, exports, history all behave like a typed query).
function runInConsole(sql) {
  $("q-sql").value = sql;
  showTab("query");
  runQuery();
}

$("s-run").addEventListener("click", () => {
  const table = $("s-table").value;
  const col = $("s-col").value.trim();
  const mode = $("s-mode").value;
  const text = $("s-text").value;
  const limit = Math.max(1, parseInt($("s-limit").value, 10) || 10);
  if (!table || !text || (mode !== "SEARCH" && !col)) return;
  const pred = mode === "SEARCH"
    ? `SEARCH(${sqlQuote(text)})`
    : `${mode}(${col}, ${sqlQuote(text)})`;
  const hl = col ? `, HIGHLIGHT(${col}) AS snippet` : "";
  runInConsole(
    `SELECT *${hl} FROM ${table} WHERE ${pred} ORDER BY score() DESC LIMIT ${limit}`,
  );
});

$("sg-run").addEventListener("click", () => {
  const index = $("sg-index").value.trim();
  const text = $("sg-text").value;
  if (index && text) runInConsole(`SUGGEST ${sqlQuote(text)} ON ${index}`);
});

async function populateSearchTables() {
  let schema;
  try {
    schema = await api("GET", "/ui/schema");
  } catch {
    return;
  }
  const sel = $("s-table");
  const current = sel.value;
  sel.textContent = "";
  const head = document.createElement("option");
  head.value = "";
  head.textContent = "table…";
  sel.append(head);
  for (const db of schema.databases || []) {
    for (const table of db.tables) {
      // The playground runs in the console session, which targets
      // `currentDb` — list that database's tables.
      if (db.name !== currentDb) continue;
      const opt = document.createElement("option");
      opt.value = table.name;
      opt.textContent = table.name;
      sel.append(opt);
    }
  }
  sel.value = current;
}

$("es-send").addEventListener("click", async () => {
  const method = $("es-method").value;
  const path = $("es-path").value.trim();
  const body = $("es-body").value.trim();
  const err = $("es-error");
  const out = $("es-out");
  err.hidden = true;
  out.hidden = true;
  if (!path.startsWith("/")) {
    err.textContent = "path must start with /";
    err.hidden = false;
    return;
  }
  try {
    const resp = await api(method, path, body === "" ? "{}" : body);
    out.textContent = JSON.stringify(resp, null, 2);
    out.hidden = false;
  } catch (e) {
    if (e instanceof AuthError) return logout();
    err.textContent = e.message;
    err.hidden = false;
  }
});

// ---- result mini chart (time series in the console) ----
const CHART_COLORS = ["#2563eb", "#16a34a", "#d97706", "#dc2626", "#7c3aed", "#0891b2"];

// A line chart when the result looks like a time series: a numeric
/// column named like time plus at least one other numeric column.
function renderChart(columns, rows) {
  const box = $("q-chart");
  box.hidden = true;
  box.textContent = "";
  if (rows.length < 2) return;
  const timeIdx = columns.findIndex((c) =>
    ["ts", "t", "time", "bucket", "timestamp"].includes(c.toLowerCase()));
  if (timeIdx < 0 || typeof rows[0][timeIdx] !== "number") return;
  const numericIdx = columns
    .map((_, i) => i)
    .filter((i) => i !== timeIdx && rows.some((r) => typeof r[i] === "number"));
  if (!numericIdx.length) return;

  const points = rows
    .filter((r) => typeof r[timeIdx] === "number")
    .slice()
    .sort((a, b) => a[timeIdx] - b[timeIdx]);
  const t0 = points[0][timeIdx];
  const t1 = points[points.length - 1][timeIdx];
  if (t1 === t0) return;

  const canvas = document.createElement("canvas");
  const width = Math.max(320, Math.min(box.parentElement.clientWidth || 800, 1100));
  canvas.width = width;
  canvas.height = 160;
  const g = canvas.getContext("2d");
  const pad = { l: 8, r: 8, t: 8, b: 8 };
  let vmin = Infinity;
  let vmax = -Infinity;
  for (const i of numericIdx) {
    for (const r of points) {
      if (typeof r[i] === "number") {
        vmin = Math.min(vmin, r[i]);
        vmax = Math.max(vmax, r[i]);
      }
    }
  }
  if (vmin === vmax) {
    vmin -= 1;
    vmax += 1;
  }
  const x = (t) => pad.l + ((t - t0) / (t1 - t0)) * (width - pad.l - pad.r);
  const y = (v) => canvas.height - pad.b - ((v - vmin) / (vmax - vmin)) * (canvas.height - pad.t - pad.b);
  numericIdx.forEach((idx, series) => {
    g.strokeStyle = CHART_COLORS[series % CHART_COLORS.length];
    g.lineWidth = 1.5;
    g.beginPath();
    let started = false;
    for (const r of points) {
      if (typeof r[idx] !== "number") continue;
      if (started) g.lineTo(x(r[timeIdx]), y(r[idx]));
      else {
        g.moveTo(x(r[timeIdx]), y(r[idx]));
        started = true;
      }
    }
    g.stroke();
  });
  const legend = document.createElement("p");
  legend.className = "muted";
  numericIdx.forEach((idx, series) => {
    const chip = document.createElement("span");
    chip.style.color = CHART_COLORS[series % CHART_COLORS.length];
    chip.textContent = `— ${columns[idx]}  `;
    legend.append(chip);
  });
  const range = document.createElement("span");
  range.textContent = `(${+vmin.toPrecision(6)} … ${+vmax.toPrecision(6)})`;
  legend.append(range);
  box.append(canvas, legend);
  box.hidden = false;
}

// ---- stats tab ----
const STATS_INTERVAL_MS = 5000;
const SPARK_WINDOW = 60; // 60 samples x 5s = 5 minutes of history
let statsHistory = []; // [{t, m: Map(metric name → value)}]
let statsTimer = null;

async function apiText(path) {
  const resp = await fetch(path, { headers: auth ? { Authorization: auth } : {} });
  if (resp.status === 401) throw new AuthError();
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.text();
}

// Prometheus text → Map. Each exact series keeps its own key; label sets
// are also summed under the bare metric name (all we need client-side).
function parseProm(text) {
  const m = new Map();
  for (const line of text.split("\n")) {
    if (!line || line.startsWith("#")) continue;
    const sp = line.lastIndexOf(" ");
    if (sp < 0) continue;
    const key = line.slice(0, sp);
    const val = Number(line.slice(sp + 1));
    if (!Number.isFinite(val)) continue;
    m.set(key, val);
    const brace = key.indexOf("{");
    if (brace > 0) {
      const name = key.slice(0, brace);
      m.set(name, (m.get(name) || 0) + val);
    }
  }
  return m;
}

// Per-interval rate of a counter across the sampled history.
function rateSeries(name) {
  const out = [];
  for (let i = 1; i < statsHistory.length; i++) {
    const dt = (statsHistory[i].t - statsHistory[i - 1].t) / 1000;
    const d = (statsHistory[i].m.get(name) ?? 0) - (statsHistory[i - 1].m.get(name) ?? 0);
    out.push(dt > 0 ? Math.max(0, d) / dt : 0);
  }
  return out;
}

function spark(values) {
  const c = document.createElement("canvas");
  c.width = 120;
  c.height = 22;
  c.className = "spark";
  if (values.length > 1) {
    const g = c.getContext("2d");
    const max = Math.max(...values, 1e-9);
    g.strokeStyle = getComputedStyle(document.documentElement).getPropertyValue("--accent").trim() || "#2563eb";
    g.lineWidth = 1.5;
    g.beginPath();
    values.forEach((v, i) => {
      const x = (i / (values.length - 1)) * 118 + 1;
      const y = 20 - (v / max) * 18 + 1;
      if (i === 0) g.moveTo(x, y);
      else g.lineTo(x, y);
    });
    g.stroke();
  }
  return c;
}

// A value with a trailing sparkline, for kv() cells.
function withSpark(text, values) {
  const wrap = document.createElement("span");
  wrap.className = "valspark";
  const label = document.createElement("span");
  label.textContent = text;
  wrap.append(label, spark(values));
  return wrap;
}

function fmtBytes(n) {
  if (n < 1024) return `${Math.round(n)} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let i = -1;
  do { n /= 1024; i++; } while (n >= 1024 && i < units.length - 1);
  return `${n.toFixed(n < 10 ? 1 : 0)} ${units[i]}`;
}

function fmtRate(v) {
  return v >= 100 ? Math.round(v).toString() : v.toFixed(1);
}

async function refreshStats() {
  const [promText, statusResult, hosts, status] = await Promise.all([
    apiText("/metrics"),
    api("POST", "/query", JSON.stringify({ sql: "SHOW STATUS" })),
    api("GET", "/ui/hosts"),
    api("GET", "/status"),
  ]);
  renderHosts(hosts, status);
  const m = parseProm(promText);
  statsHistory.push({ t: Date.now(), m });
  if (statsHistory.length > SPARK_WINDOW) statsHistory.shift();
  const s = new Map(statusResult.rows);

  const qRates = rateSeries("skaidb_queries_total");
  const scanRates = rateSeries("skaidb_rows_scanned_total");
  const writeRates = rateSeries("skaidb_rows_written_total");
  const byteRates = rateSeries("skaidb_bytes_returned_total");
  // Mean latency over the last interval: Δsum / Δcount.
  let latency = "—";
  if (statsHistory.length > 1) {
    const [a, b] = statsHistory.slice(-2);
    const dc = (b.m.get("skaidb_query_duration_seconds_count") ?? 0) -
               (a.m.get("skaidb_query_duration_seconds_count") ?? 0);
    const ds = (b.m.get("skaidb_query_duration_seconds_sum") ?? 0) -
               (a.m.get("skaidb_query_duration_seconds_sum") ?? 0);
    if (dc > 0) latency = `${(ds / dc * 1000).toFixed(2)} ms`;
  }
  kv("st-queries", [
    ["queries/s", withSpark(fmtRate(qRates.at(-1) ?? 0), qRates)],
    ["mean latency (5s)", latency],
    ["in flight", m.get("skaidb_queries_in_flight") ?? 0],
    ["rows scanned/s", withSpark(fmtRate(scanRates.at(-1) ?? 0), scanRates)],
    ["rows written/s", withSpark(fmtRate(writeRates.at(-1) ?? 0), writeRates)],
    ["bytes returned/s", withSpark(fmtBytes(byteRates.at(-1) ?? 0), byteRates)],
    ["connections", m.get("skaidb_connections_active") ?? 0],
  ]);

  kv("st-storage", [
    ["tables", s.get("tables")],
    ["disk", fmtBytes(s.get("disk_bytes") ?? 0)],
    ["memtable", fmtBytes(s.get("memtable_bytes") ?? 0)],
    ["sstables", s.get("sstable_count")],
    ["secondary indexes", s.get("secondary_indexes")],
    ["compactions", s.get("compactions")],
    ["compacted", fmtBytes(s.get("compaction_bytes") ?? 0)],
  ]);

  kv("st-cache", [
    ["cache hit rate", s.get("cache_hit_rate")],
    ["cache entries", s.get("cache_entries")],
    ["cache evictions", s.get("cache_evictions")],
    ["bloom negatives", s.get("bloom_negatives")],
    ["wal", fmtBytes(s.get("wal_bytes") ?? 0)],
    ["wal fsyncs", s.get("wal_fsyncs")],
  ]);

  kv("st-search", [
    ["search indexes", s.get("search_indexes")],
    ["search docs", s.get("search_docs")],
    ["search disk", fmtBytes(m.get("skaidb_search_disk_bytes") ?? 0)],
    ["last rebuild", `${s.get("search_rebuild_ms") ?? 0} ms`],
    ["timeseries tables", s.get("timeseries_tables")],
    ["vector indexes", s.get("vector_indexes")],
  ]);

  $("stats-refreshed").textContent = `refreshed ${new Date().toLocaleTimeString()} · sparklines cover ${Math.round((statsHistory.length - 1) * STATS_INTERVAL_MS / 1000)}s`;
}

// The per-node system stats table, plus a cluster totals row when the
// deployment has more than one node.
function renderHosts(hosts, status) {
  const tbody = $("st-nodes").querySelector("tbody");
  tbody.textContent = "";
  // Per-node replication lag/backlog from /status. Peer ids (host:internode
  // port) match the host-stats node ids exactly. The node serving this page
  // has no lag-to-self, so it shows a dash.
  const peers = new Map(((status && status.peers) || []).map((p) => [p.id, p]));
  const selfId = status && status.node_id;
  const cell = (text) => {
    const td = document.createElement("td");
    td.textContent = text;
    return td;
  };
  const usedOfTotal = (used, total) =>
    total ? `${fmtBytes(used)} / ${fmtBytes(total)} (${Math.round(used / total * 100)}%)` : "—";
  for (const n of hosts.nodes) {
    const tr = document.createElement("tr");
    if (!n.reachable) {
      tr.append(cell(n.id));
      const td = cell("unreachable");
      td.colSpan = 11;
      td.className = "muted";
      tr.append(td);
      tbody.append(tr);
      continue;
    }
    // Stale rows (node hasn't reported recently) dim instead of vanishing —
    // last-known data plus its age beats a flapping "unreachable".
    const age = n.stale_secs || 0;
    if (age > 10) tr.style.opacity = "0.55";
    const restarts = cell(
      n.oom_kills > 0 ? `${n.restarts || 0} (${n.oom_kills} oom)` : `${n.restarts || 0}`,
    );
    if (n.oom_kills > 0) restarts.className = "warn";
    const ageCell = cell(age > 0 ? fmtDuration(age) : "now");
    if (age > 60) ageCell.className = "bad";
    else if (age > 10) ageCell.className = "warn";
    else ageCell.className = "muted";
    tr.append(
      cell(n.id),
      cell(`${n.cpu_percent.toFixed(1)}% of ${n.cpus}`),
      cell(n.load1.toFixed(2)),
      cell(usedOfTotal(n.mem_used_bytes, n.mem_total_bytes)),
      cell(fmtBytes(n.rss_bytes)),
      cell(`${fmtBytes(n.disk_read_bps)}/s`),
      cell(`${fmtBytes(n.disk_write_bps)}/s`),
      cell(usedOfTotal(n.disk_total_bytes - n.disk_available_bytes, n.disk_total_bytes)),
      replLagCell(peers.get(n.id), n.id === selfId),
      cell(n.uptime_secs ? fmtDuration(n.uptime_secs) : "—"),
      restarts,
      ageCell,
    );
    tbody.append(tr);
  }
  if (hosts.nodes.length > 1) {
    const c = hosts.cluster;
    const tr = document.createElement("tr");
    tr.className = "cluster-total";
    tr.append(
      cell(`cluster (${c.reachable}/${c.nodes} up)`),
      cell(`${c.cpu_percent_avg.toFixed(1)}% avg`),
      cell(""),
      cell(usedOfTotal(c.mem_used_bytes, c.mem_total_bytes)),
      cell(""),
      cell(`${fmtBytes(c.disk_read_bps)}/s`),
      cell(`${fmtBytes(c.disk_write_bps)}/s`),
      cell(usedOfTotal(c.disk_total_bytes - c.disk_available_bytes, c.disk_total_bytes)),
      cell(""),
      cell(""),
      cell(""),
      cell(""),
    );
    tbody.append(tr);
  }
}

// Replication lag for the nodes table: the exact hinted-handoff backlog owed to
// this node plus its estimated lag. Self (the node serving the page) has none.
function replLagCell(peer, isSelf) {
  const td = document.createElement("td");
  if (isSelf || !peer) {
    td.textContent = "—";
    td.className = "muted";
    return td;
  }
  const parts = [];
  if (peer.lag_ms != null) parts.push(fmtLag(peer.lag_ms));
  if (peer.hints_pending > 0) parts.push(`${peer.hints_pending} queued`);
  td.textContent = parts.length ? parts.join(", ") : "0";
  if (peer.hints_pending > 0 || peer.lag_ms >= 5000) td.className = "bad";
  else if (peer.lag_ms >= 1000) td.className = "warn";
  return td;
}

// Rows like `<prefix>.<label>[.<label>...].<field>` → one row per label
// tuple. The pattern's leading captures are label columns (e.g. database +
function statsTick() {
  refreshStats().catch((e) => {
    if (e instanceof AuthError) logout();
    else $("stats-refreshed").textContent = `refresh failed: ${e.message}`;
  });
}

// ---- config tab ----
async function loadConfig() {
  const cfg = await api("POST", "/admin/config", "{}");
  const box = $("cfg-sections");
  box.textContent = "";
  const keySel = $("cfg-key");
  keySel.textContent = "";
  const head = document.createElement("option");
  head.value = "";
  head.textContent = "key…";
  keySel.append(head);
  for (const section of Object.keys(cfg).sort()) {
    if (typeof cfg[section] !== "object" || cfg[section] === null) continue;
    const card = document.createElement("div");
    card.className = "card";
    const h = document.createElement("h2");
    h.textContent = section;
    const table = document.createElement("table");
    table.className = "kv";
    for (const [field, value] of Object.entries(cfg[section]).sort()) {
      const tr = document.createElement("tr");
      const kd = document.createElement("td");
      kd.textContent = field;
      const vd = document.createElement("td");
      vd.textContent = typeof value === "object" ? JSON.stringify(value) : String(value);
      tr.append(kd, vd);
      table.append(tr);
      if (typeof value !== "object") {
        const opt = document.createElement("option");
        opt.value = `${section}.${field}`;
        opt.textContent = `${section}.${field}`;
        keySel.append(opt);
      }
    }
    card.append(h, table);
    box.append(card);
  }
}

$("cfg-set").addEventListener("click", async () => {
  const key = $("cfg-key").value;
  const value = $("cfg-value").value;
  const out = $("cfg-result");
  if (!key) return;
  try {
    const r = await api("POST", "/admin/config/set", JSON.stringify({ key, value }));
    out.textContent = r.applied
      ? "applied live" + (r.persisted ? " and persisted" : "")
      : "persisted — restart required to take effect";
    await loadConfig();
  } catch (e) {
    if (e instanceof AuthError) return logout();
    out.textContent = e.message;
  }
});

// ---- admin tab ----
async function adminOp(path, body, confirmText) {
  if (confirmText && !window.confirm(confirmText)) return;
  const out = $("ad-result");
  out.textContent = "running…";
  try {
    const r = await api("POST", path, JSON.stringify(body));
    out.textContent = JSON.stringify(r);
  } catch (e) {
    if (e instanceof AuthError) return logout();
    out.textContent = e.message;
  }
}

$("ad-repair").addEventListener("click", () =>
  adminOp("/admin/repair", {}, "Run a cluster-wide repair? This re-replicates data and can take a while."));
$("ad-reclaim").addEventListener("click", () =>
  adminOp("/admin/reclaim", {}, "Reclaim keys this node no longer owns?"));
$("ad-add").addEventListener("click", () => {
  const addr = $("ad-add-addr").value.trim();
  if (addr) adminOp("/admin/add-node", { addr }, `Add node ${addr} to the cluster?`);
});
$("ad-rm").addEventListener("click", () => {
  const id = $("ad-rm-id").value.trim();
  if (id) adminOp("/admin/remove-node", { id }, `Remove node ${id} from the cluster? Its ranges move to the remaining nodes.`);
});

async function loadSlow() {
  const snap = await api("POST", "/admin/slow", "{}");
  const rows = snap.slow_queries || [];
  const tbody = $("ad-slow").querySelector("tbody");
  tbody.textContent = "";
  for (const q of rows) {
    const tr = document.createElement("tr");
    for (const v of [q.seq, q.elapsed_ms, q.sql]) {
      const td = document.createElement("td");
      td.textContent = String(v);
      tr.append(td);
    }
    tbody.append(tr);
  }
  $("ad-slow-empty").hidden = rows.length > 0;
}

$("ad-slow-refresh").addEventListener("click", () =>
  loadSlow().catch((e) => { if (e instanceof AuthError) logout(); }));

// Enter submits the single-input forms.
$("cfg-value").addEventListener("keydown", (ev) => {
  if (ev.key === "Enter") $("cfg-set").click();
});
$("ad-add-addr").addEventListener("keydown", (ev) => {
  if (ev.key === "Enter") $("ad-add").click();
});
$("ad-rm-id").addEventListener("keydown", (ev) => {
  if (ev.key === "Enter") $("ad-rm").click();
});

// Hide the config/admin tabs when the role lacks Admin. The server stays
// the boundary — this only trims chrome the role cannot use.
async function probeAdmin() {
  let allowed = true;
  try {
    await api("POST", "/admin/config", "{}");
  } catch (e) {
    if (e instanceof AuthError) throw e;
    allowed = false;
  }
  for (const tab of document.querySelectorAll('#tabs button[data-tab="config"], #tabs button[data-tab="admin"]')) {
    tab.hidden = !allowed;
  }
}

function fmtDuration(secs) {
  const d = Math.floor(secs / 86400), h = Math.floor((secs % 86400) / 3600),
        m = Math.floor((secs % 3600) / 60);
  return d > 0 ? `${d}d ${h}h` : h > 0 ? `${h}h ${m}m` : `${m}m`;
}

// ---- boot ----
let refreshTimer = null;

async function enterApp() {
  show("app");
  $("node-badge").textContent = `${meta.node_id || "standalone"} · v${meta.version}`;
  probeAdmin().catch(() => {});
  const tick = () =>
    refreshStatus().catch((e) => {
      if (e instanceof AuthError) logout();
      else $("status-refreshed").textContent = `refresh failed: ${e.message}`;
    });
  await tick();
  clearInterval(refreshTimer);
  refreshTimer = setInterval(tick, 5000);
}

function logout() {
  clearAuth();
  clearInterval(refreshTimer);
  clearInterval(statsTimer);
  statsTimer = null;
  statsHistory = [];
  $("login-pass").value = "";
  $("login-error").hidden = true;
  // Drop anything fetched with the old credentials from the screen.
  lastResult = null;
  $("q-results").textContent = "";
  $("q-meta").textContent = "";
  $("q-csv").hidden = $("q-json").hidden = true;
  $("cfg-sections").textContent = "";
  $("ad-slow").querySelector("tbody").textContent = "";
  $("ad-result").textContent = "";
  $("q-schema").textContent = "";
  $("q-chart").textContent = "";
  $("q-chart").hidden = true;
  $("es-out").textContent = "";
  $("es-out").hidden = true;
  showTab("status");
  show("login");
}

renderHistorySelect();

$("login-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  setAuth($("login-user").value, $("login-pass").value, $("login-remember").checked);
  try {
    await api("POST", "/query", "SHOW TABLES");
    await enterApp();
  } catch (e) {
    clearAuth();
    const err = $("login-error");
    err.textContent = e instanceof AuthError ? "authentication failed" : e.message;
    err.hidden = false;
  }
});

$("logout").addEventListener("click", logout);

(async function boot() {
  try {
    meta = await api("GET", "/ui/meta");
  } catch {
    /* keep defaults; login will surface real errors */
  }
  if (!meta.auth_required) {
    await enterApp();
    return;
  }
  if (auth) {
    try {
      await api("POST", "/query", "SHOW TABLES");
      await enterApp();
      return;
    } catch {
      clearAuth();
    }
  }
  $("login-hint").textContent = meta.node_id
    ? `node ${meta.node_id} · v${meta.version}`
    : `v${meta.version}`;
  show("login");
})();
