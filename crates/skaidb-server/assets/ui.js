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
  for (const endpoint of (status.endpoints || []).slice().sort()) {
    const tr = document.createElement("tr");
    const idc = document.createElement("td");
    idc.textContent = endpoint;
    const cc = document.createElement("td");
    cc.append(mark(!unconfigured.has(endpoint)));
    const rc = document.createElement("td");
    rc.append(mark(!halfJoined.has(endpoint)));
    tr.append(idc, cc, rc);
    tbody.append(tr);
  }
  for (const id of [...halfJoined].sort()) {
    const tr = document.createElement("tr");
    const idc = document.createElement("td");
    idc.textContent = `${id} (configured, not in ring)`;
    idc.className = "bad";
    tr.append(idc, document.createElement("td"), document.createElement("td"));
    tbody.append(tr);
  }
}

// ---- tabs ----
function showTab(name) {
  for (const btn of document.querySelectorAll("#tabs button")) {
    btn.classList.toggle("active", btn.dataset.tab === name);
  }
  for (const section of document.querySelectorAll("main#app-view > section")) {
    section.hidden = section.id !== `tab-${name}`;
  }
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
  if (result && Array.isArray(result.columns)) {
    lastResult = result;
    renderRows(result.columns, result.rows);
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

$("q-history-sel").addEventListener("change", (ev) => {
  if (!ev.target.value) return;
  $("q-sql").value = ev.target.value;
  ev.target.value = "";
  $("q-sql").focus();
});

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
  $("login-pass").value = "";
  $("login-error").hidden = true;
  // Drop anything fetched with the old credentials from the screen.
  lastResult = null;
  $("q-results").textContent = "";
  $("q-meta").textContent = "";
  $("q-csv").hidden = $("q-json").hidden = true;
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
