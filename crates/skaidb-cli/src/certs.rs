//! `skaidbsh certs gen` — mint a cluster CA and per-node leaf certificates
//! for internode mutual TLS (`internode_auth = cert`).
//!
//! The trust model is a **shared cluster CA**: every node presents a leaf
//! certificate signed by the one CA and valid for the fixed internode server
//! name `skaidb`, and every node trusts that CA. A peer without a CA-signed
//! certificate cannot complete the handshake, so it can neither join the ring
//! nor read replicated traffic. Per-node private keys keep one leaked key from
//! forging the others; the CA key is the issuing root — keep it offline.

use std::path::{Path, PathBuf};

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, KeyPair, KeyUsagePurpose,
};

/// The internode TLS server name every leaf cert must be valid for. Must match
/// `skaidb-cluster::transport::TLS_SERVER_NAME`.
const TLS_SERVER_NAME: &str = "skaidb";

/// Generate `ca.crt`/`ca.key` plus `node1..nodeN.{crt,key}` into `out_dir`.
pub fn generate(out_dir: &str, nodes: usize) -> Result<Vec<PathBuf>, String> {
    if nodes == 0 {
        return Err("need at least one node certificate (--nodes >= 1)".into());
    }
    let dir = Path::new(out_dir);
    std::fs::create_dir_all(dir).map_err(|e| format!("create {out_dir}: {e}"))?;

    // --- Cluster CA (self-signed, is_ca) ---
    let mut ca_params =
        CertificateParams::new(Vec::new()).map_err(|e| format!("CA params: {e}"))?;
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    ca_params.distinguished_name = dn("skaidb cluster CA");
    let ca_key = KeyPair::generate().map_err(|e| format!("CA key: {e}"))?;
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .map_err(|e| format!("self-sign CA: {e}"))?;

    let mut written = Vec::new();
    let ca_crt_path = dir.join("ca.crt");
    let ca_key_path = dir.join("ca.key");
    write_secret(&ca_crt_path, ca_cert.pem().as_bytes())?;
    // The CA key issues future node certs; 0600 and keep it OFF the nodes.
    write_secret(&ca_key_path, ca_key.serialize_pem().as_bytes())?;
    written.push(ca_crt_path);
    written.push(ca_key_path.clone());

    // --- Per-node leaf certs, all with SAN DNS:skaidb, signed by the CA ---
    for i in 1..=nodes {
        let mut params = CertificateParams::new(vec![TLS_SERVER_NAME.to_string()])
            .map_err(|e| format!("node{i} params: {e}"))?;
        params.is_ca = IsCa::NoCa;
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.distinguished_name = dn(&format!("skaidb node {i}"));
        let key = KeyPair::generate().map_err(|e| format!("node{i} key: {e}"))?;
        let cert = params
            .signed_by(&key, &ca_cert, &ca_key)
            .map_err(|e| format!("sign node{i}: {e}"))?;
        let crt_path = dir.join(format!("node{i}.crt"));
        let key_path = dir.join(format!("node{i}.key"));
        write_secret(&crt_path, cert.pem().as_bytes())?;
        write_secret(&key_path, key.serialize_pem().as_bytes())?;
        written.push(crt_path);
        written.push(key_path);
    }
    Ok(written)
}

fn dn(cn: &str) -> DistinguishedName {
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, cn);
    dn
}

/// Write `bytes` to `path` with 0600 permissions (best effort on non-unix).
fn write_secret(path: &Path, bytes: &[u8]) -> Result<(), String> {
    std::fs::write(path, bytes).map_err(|e| format!("write {}: {e}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}
