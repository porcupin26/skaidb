//! Connection authentication state (SPEC §8.1).
//!
//! Holds the user directory (username → SCRAM verifier + role) and runs the
//! server side of the handshake. When authentication is not required, any
//! connection is accepted and mapped to the anonymous role.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use skaidb_auth::crypto::{ct_eq, sha256};
use skaidb_auth::{ScramCredential, DEFAULT_ITERATIONS};

/// A user account: how to verify the password, and which role it acts as.
#[derive(Debug, Clone)]
pub struct UserAccount {
    pub credential: ScramCredential,
    pub role: String,
}

/// The result of verifying a client's proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthResult {
    Authenticated {
        role: String,
        server_signature: [u8; 32],
    },
    Denied(String),
}

/// Server authentication state.
#[derive(Debug)]
pub struct AuthState {
    pub required: bool,
    users: HashMap<String, UserAccount>,
    default_salt: Vec<u8>,
    nonce_counter: AtomicU64,
}

impl AuthState {
    /// Authentication disabled: connections are accepted anonymously.
    pub fn disabled() -> Self {
        AuthState {
            required: false,
            users: HashMap::new(),
            default_salt: derive_salt(b"skaidb-default"),
            nonce_counter: AtomicU64::new(0),
        }
    }

    /// Authentication required against the configured user directory.
    pub fn required() -> Self {
        AuthState {
            required: true,
            users: HashMap::new(),
            default_salt: derive_salt(b"skaidb-default"),
            nonce_counter: AtomicU64::new(0),
        }
    }

    /// Add (or replace) a user with the given password and role.
    pub fn add_user(&mut self, username: &str, password: &str, role: &str) {
        let salt = derive_salt(username.as_bytes());
        let credential = ScramCredential::new(password, &salt, DEFAULT_ITERATIONS);
        self.users.insert(
            username.to_string(),
            UserAccount {
                credential,
                role: role.to_string(),
            },
        );
    }

    /// The config-directory account for `username` (catalog-managed users
    /// are resolved by the caller via `Context::lookup_account`).
    pub fn account(&self, username: &str) -> Option<UserAccount> {
        self.users.get(username).cloned()
    }

    /// Salt and iteration count to advertise in the challenge for a resolved
    /// account. Unknown users get a stable decoy salt (avoids trivial user
    /// enumeration).
    pub fn salt_for(&self, account: Option<&UserAccount>) -> (Vec<u8>, u32) {
        match account {
            Some(acct) => (acct.credential.salt.clone(), acct.credential.iterations),
            None => (self.default_salt.clone(), DEFAULT_ITERATIONS),
        }
    }

    /// Verify a plaintext `password` against a resolved account (HTTP Basic
    /// auth). Returns the acting role on success. Recomputes the SCRAM
    /// verifier from the stored salt/iterations and compares stored keys.
    pub fn verify_password(account: Option<&UserAccount>, password: &str) -> Option<String> {
        let acct = account?;
        let candidate =
            ScramCredential::new(password, &acct.credential.salt, acct.credential.iterations);
        if ct_eq(&candidate.stored_key, &acct.credential.stored_key) {
            Some(acct.role.clone())
        } else {
            None
        }
    }

    /// Generate a server nonce binding the client's nonce.
    pub fn server_nonce(&self, client_nonce: &str) -> String {
        let n = self.nonce_counter.fetch_add(1, Ordering::Relaxed);
        format!("{client_nonce}.s{n}")
    }

    /// Verify a client's proof. When auth is disabled, accept and map to
    /// `anonymous_role`.
    pub fn verify(
        &self,
        account: Option<&UserAccount>,
        auth_message: &[u8],
        client_proof: &[u8; 32],
        anonymous_role: &str,
    ) -> AuthResult {
        if !self.required {
            return AuthResult::Authenticated {
                role: anonymous_role.to_string(),
                server_signature: [0u8; 32],
            };
        }
        match account {
            Some(acct) => match acct.credential.verify(auth_message, client_proof) {
                Some(server_signature) => AuthResult::Authenticated {
                    role: acct.role.clone(),
                    server_signature,
                },
                None => AuthResult::Denied("authentication failed".into()),
            },
            None => AuthResult::Denied("unknown user".into()),
        }
    }
}

/// Derive a stable 16-byte salt for a name. (A production deployment would use
/// a random per-user salt; this keeps the directory reproducible without a
/// CSPRNG dependency.)
fn derive_salt(name: &[u8]) -> Vec<u8> {
    let mut input = b"skaidb-salt:".to_vec();
    input.extend_from_slice(name);
    sha256(&input)[..16].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use skaidb_auth::scram;
    use skaidb_proto::auth_message;

    #[test]
    fn disabled_accepts_anyone() {
        let auth = AuthState::disabled();
        let r = auth.verify(auth.account("whoever").as_ref(), b"msg", &[0u8; 32], "anon");
        assert_eq!(
            r,
            AuthResult::Authenticated {
                role: "anon".into(),
                server_signature: [0u8; 32]
            }
        );
    }

    #[test]
    fn required_verifies_correct_password() {
        let mut auth = AuthState::required();
        auth.add_user("ada", "pencil", "admin");
        let acct = auth.account("ada");
        let (salt, iters) = auth.salt_for(acct.as_ref());
        let am = auth_message("ada", "cn", "sn", &salt, iters);
        let proof = scram::client_proof("pencil", &salt, iters, &am);

        match auth.verify(auth.account("ada").as_ref(), &am, &proof, "anon") {
            AuthResult::Authenticated { role, .. } => assert_eq!(role, "admin"),
            other => panic!("expected auth, got {other:?}"),
        }
    }

    #[test]
    fn required_rejects_wrong_password_and_unknown_user() {
        let mut auth = AuthState::required();
        auth.add_user("ada", "pencil", "admin");
        let acct = auth.account("ada");
        let (salt, iters) = auth.salt_for(acct.as_ref());
        let am = auth_message("ada", "cn", "sn", &salt, iters);

        let bad = scram::client_proof("WRONG", &salt, iters, &am);
        assert!(matches!(
            auth.verify(auth.account("ada").as_ref(), &am, &bad, "anon"),
            AuthResult::Denied(_)
        ));
        let any = scram::client_proof("x", &salt, iters, &am);
        assert!(matches!(
            auth.verify(auth.account("ghost").as_ref(), &am, &any, "anon"),
            AuthResult::Denied(_)
        ));
    }
}
