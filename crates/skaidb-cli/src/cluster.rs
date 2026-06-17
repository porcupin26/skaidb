//! Human-readable rendering of the `/admin/status` cluster report (`\cluster`).
//!
//! The endpoint returns a JSON snapshot of membership from one node's point of
//! view. Operators want a verdict — is the cluster healthy? — and, when it
//! isn't, which node is at fault and what to do about it. This turns the raw
//! blob into that: a one-line status (OK / DEGRADED / CRITICAL), a topology
//! summary, a per-peer table, and an actionable "attention" list. `\cluster raw`
//! still prints the underlying JSON.

use serde_json::Value;

/// One peer's fields, pulled out of the JSON for convenience.
struct Peer {
    id: String,
    in_ring: bool,
    in_config: bool,
    /// `Some` from a liveness probe; `None` if not probed.
    reachable: Option<bool>,
    /// Staleness vs. our HLC frontier; `None` if nothing confirmed yet.
    lag_ms: Option<u64>,
    hints: u64,
}

/// Render the `/admin/status` JSON as a status summary. Falls back to the raw
/// body if it doesn't look like the expected shape, so a server-side change
/// never leaves the operator staring at nothing.
pub fn render(body: &str) {
    let v: Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => return crate::http::print_body(body),
    };
    match v.get("clustered").and_then(Value::as_bool) {
        Some(true) => {}
        Some(false) => {
            println!(
                "Cluster: STANDALONE — single node, no replication.\n  \
                 Set `[cluster].seeds` in the config to form a cluster."
            );
            return;
        }
        // Not a status payload (e.g. an error object) — show it verbatim.
        None => return crate::http::print_body(body),
    }

    let node_id = v.get("node_id").and_then(Value::as_str).unwrap_or("?");
    let epoch = v.get("epoch").and_then(Value::as_u64).unwrap_or(0);
    let rf = v.get("replication_factor").and_then(Value::as_u64).unwrap_or(0);
    let self_in_ring = v.get("self_in_ring").and_then(Value::as_bool).unwrap_or(false);
    let resharding = v.get("resharding").and_then(Value::as_bool).unwrap_or(false);
    let configured_n = arr_len(&v, "configured");
    let ring_total = arr_len(&v, "members");

    let peers: Vec<Peer> = v
        .get("peers")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .map(|p| Peer {
                    id: p.get("id").and_then(Value::as_str).unwrap_or("?").to_string(),
                    in_ring: p.get("in_ring").and_then(Value::as_bool).unwrap_or(false),
                    in_config: p.get("in_config").and_then(Value::as_bool).unwrap_or(false),
                    reachable: p.get("reachable").and_then(Value::as_bool),
                    lag_ms: p.get("lag_ms").and_then(Value::as_u64),
                    hints: p.get("hints_pending").and_then(Value::as_u64).unwrap_or(0),
                })
                .collect()
        })
        .unwrap_or_default();

    // We are obviously reachable to ourselves; count self when it holds tokens.
    let reachable_ring = (self_in_ring as usize)
        + peers.iter().filter(|p| p.in_ring && p.reachable == Some(true)).count();
    let quorum = ring_total / 2 + 1;
    let unreachable = peers.iter().filter(|p| p.in_ring && p.reachable == Some(false)).count();

    let disc = |k: &str| -> Vec<String> {
        v.get("discrepancies")
            .and_then(|d| d.get(k))
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default()
    };
    let disagreement = disc("membership_disagreement");
    let configured_not_in_ring = disc("configured_not_in_ring");
    let ring_not_configured = disc("ring_not_configured");

    // Build the actionable list; `critical` escalates the headline verdict.
    let mut issues: Vec<String> = Vec::new();
    let mut critical = false;

    if !self_in_ring {
        critical = true;
        issues.push(format!(
            "this node ({node_id}) is NOT in the ring (half-join): it catches up via\n\
             \x20     anti-entropy but owns no tokens, so no peer routes writes to it.\n\
             \x20     -> confirm seeds + epoch agree across nodes, or re-add it: \\node add <addr>"
        ));
    }
    for p in peers.iter().filter(|p| p.in_ring && p.reachable == Some(false)) {
        let hints = if p.hints > 0 {
            format!("; {} hinted write(s) buffered for it", p.hints)
        } else {
            String::new()
        };
        issues.push(format!(
            "{} is in the ring but not responding (process down, or internode port\n\
             \x20     blocked / auth token mismatch on the upgraded node){hints}.\n\
             \x20     -> on that host:  systemctl status skaidb ; journalctl -u skaidb -e",
            p.id
        ));
    }
    for id in &disagreement {
        critical = true;
        issues.push(format!(
            "{id} disagrees about membership (it doesn't list this node, or sees a\n\
             \x20     different member count) — possible split-brain.\n\
             \x20     -> run \\cluster on {id} and confirm its seeds + epoch match"
        ));
    }
    for id in &configured_not_in_ring {
        issues.push(format!(
            "{id} is a configured seed but not in the ring — still joining, or never admitted.\n\
             \x20     -> if it should be live:  \\node add {id}"
        ));
    }
    for id in &ring_not_configured {
        issues.push(format!(
            "{id} is in the ring but not in this node's seeds — stale/orphaned entry.\n\
             \x20     -> add it to seeds, or decommission it:  \\node remove {id}"
        ));
    }
    if resharding {
        issues.push(
            "membership change in progress (resharding; dual-write window open) — transient.".into(),
        );
    }
    if reachable_ring < quorum {
        critical = true;
        issues.push(format!(
            "QUORUM LOST: only {reachable_ring}/{ring_total} ring members reachable (need {quorum}).\n\
             \x20     QUORUM reads and writes will fail until a member returns."
        ));
    }

    let status = if critical {
        "CRITICAL"
    } else if !issues.is_empty() {
        "DEGRADED"
    } else {
        "OK"
    };
    let headline = if status == "OK" {
        "all ring members reachable and in agreement".to_string()
    } else if !self_in_ring {
        "this node is not admitted to the ring".to_string()
    } else if reachable_ring < quorum {
        format!("quorum lost — {reachable_ring}/{ring_total} members reachable")
    } else if unreachable > 0 {
        format!("{unreachable} of {ring_total} ring members unreachable")
    } else {
        "membership discrepancies — see below".to_string()
    };

    println!("Cluster: {status}   ({headline})");
    println!("  this node   {node_id}   epoch {epoch}   RF {rf}");
    println!(
        "  members     {configured_n} configured, {ring_total} in ring, {reachable_ring} reachable   (quorum {quorum})"
    );

    if !peers.is_empty() {
        println!();
        println!("  peers");
        let id_w = peers.iter().map(|p| p.id.len()).max().unwrap_or(0);
        for p in &peers {
            let mark = match p.reachable {
                Some(true) => "up  ",
                Some(false) => "DOWN",
                None => "?   ",
            };
            let role = match (p.in_ring, p.in_config) {
                (true, true) => "ring",
                (false, true) => "config-only",
                (true, false) => "ring-only",
                (false, false) => "unknown",
            };
            let detail = match p.reachable {
                Some(true) => format!("lag {}", fmt_ms(p.lag_ms)),
                Some(false) => match p.lag_ms {
                    Some(ms) => format!("last ack {} ago", fmt_ms(Some(ms))),
                    None => "no ack yet".to_string(),
                },
                None => "not probed".to_string(),
            };
            let hints = if p.hints > 0 {
                format!("   hints {}", p.hints)
            } else {
                String::new()
            };
            println!("    {mark}  {:id_w$}   {:<11}   {detail}{hints}", p.id, role);
        }
    }

    if !issues.is_empty() {
        println!();
        println!("  attention");
        for i in &issues {
            println!("    - {i}");
        }
    }
}

/// Length of a top-level JSON array field, or 0 if missing/not an array.
fn arr_len(v: &Value, key: &str) -> usize {
    v.get(key).and_then(Value::as_array).map(|a| a.len()).unwrap_or(0)
}

/// Format a millisecond duration compactly: `0ms`, `850ms`, `43s`, `2m3s`.
/// `None` (nothing confirmed yet) renders as `n/a`.
fn fmt_ms(ms: Option<u64>) -> String {
    match ms {
        None => "n/a".to_string(),
        Some(ms) if ms < 1000 => format!("{ms}ms"),
        Some(ms) if ms < 60_000 => format!("{}s", ms / 1000),
        Some(ms) => format!("{}m{}s", ms / 60_000, (ms % 60_000) / 1000),
    }
}

#[cfg(test)]
mod tests {
    use super::fmt_ms;

    #[test]
    fn formats_durations() {
        assert_eq!(fmt_ms(None), "n/a");
        assert_eq!(fmt_ms(Some(0)), "0ms");
        assert_eq!(fmt_ms(Some(850)), "850ms");
        assert_eq!(fmt_ms(Some(43_249)), "43s");
        assert_eq!(fmt_ms(Some(123_000)), "2m3s");
    }
}

