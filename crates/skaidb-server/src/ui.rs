//! The built-in web UI (docs/UI_TODO.md) — a pure API client embedded in the
//! binary at compile time. It adds no privileged surface: the shell and its
//! assets are static and secret-free, and every data call the page makes goes
//! through the ordinary authenticated endpoints (`POST /query`, `GET /status`,
//! `POST /admin/*`). `GET /ui/meta` is the one new JSON route; it carries the
//! same trust level as `/health`.
//!
//! `[ui] enabled` is live-mutable: the guard reads the live config on every
//! request, so `config set ui.enabled false` 404s the whole prefix
//! immediately — indistinguishable from a build without a UI.

use serde_json::json;

use crate::shared::Shared;

const HTML: &str = include_str!("../assets/ui.html");
const CSS: &str = include_str!("../assets/ui.css");
const JS: &str = include_str!("../assets/ui.js");

/// The no-external-assets rule, enforced by the browser too.
pub const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; \
     img-src 'self' data:; connect-src 'self'";

/// A response ready for the wire: status, content type, body.
pub struct Asset {
    pub status: u16,
    pub content_type: &'static str,
    pub body: String,
}

/// Route a `GET /ui[...]` request, or `None` if the path isn't ours.
/// Returns 404 for every `/ui` path when the UI is disabled (live config).
pub fn try_route(ctx: &Shared, path: &str) -> Option<Asset> {
    if path != "/ui" && !path.starts_with("/ui/") {
        return None;
    }
    let enabled = ctx.config.read().map(|cfg| cfg.ui.enabled).unwrap_or(false);
    if !enabled {
        return Some(Asset {
            status: 404,
            content_type: "application/json",
            body: "{\"error\": \"not found\"}".to_string(),
        });
    }
    let asset = match path {
        "/ui" | "/ui/" => Asset {
            status: 200,
            content_type: "text/html; charset=utf-8",
            body: HTML.to_string(),
        },
        "/ui/app.css" => Asset {
            status: 200,
            content_type: "text/css; charset=utf-8",
            body: CSS.to_string(),
        },
        "/ui/app.js" => Asset {
            status: 200,
            content_type: "text/javascript; charset=utf-8",
            body: JS.to_string(),
        },
        "/ui/meta" => Asset {
            status: 200,
            content_type: "application/json",
            body: meta_json(ctx),
        },
        _ => Asset {
            status: 404,
            content_type: "application/json",
            body: "{\"error\": \"not found\"}".to_string(),
        },
    };
    Some(asset)
}

/// What the login screen needs before any authenticated call can succeed.
/// Nothing here is secret (same trust level as `/health` and `/status`).
fn meta_json(ctx: &Shared) -> String {
    let cluster = ctx.backend.cluster_stats();
    json!({
        "version": env!("CARGO_PKG_VERSION"),
        "node_id": cluster.as_ref().map(|c| c.node_id.clone()).unwrap_or_default(),
        "clustered": cluster.is_some(),
        "auth_required": ctx.authn.required,
        "uptime_seconds": ctx.start.elapsed().as_secs(),
    })
    .to_string()
}
