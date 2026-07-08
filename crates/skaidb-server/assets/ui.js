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
  show("login");
}

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
