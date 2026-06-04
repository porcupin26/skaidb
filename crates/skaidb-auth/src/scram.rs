//! SCRAM-SHA-256 credentials and proof verification (SPEC §8.1, RFC 5802).
//!
//! The server stores only a *verifier* — salt, iteration count, `StoredKey`,
//! and `ServerKey` — never the password. Authentication verifies the client's
//! proof against the stored keys and returns the server signature for mutual
//! authentication. The cryptographic core is exact RFC 5802; the textual
//! message framing (base64, GS2 header) sits above this and is left to the
//! transport layer.

use crate::crypto::{ct_eq, hex, hmac_sha256, pbkdf2_hmac_sha256, sha256};

/// Default PBKDF2 iteration count for new credentials.
pub const DEFAULT_ITERATIONS: u32 = 15_000;

/// A stored SCRAM-SHA-256 verifier for one user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredential {
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

impl ScramCredential {
    /// Derive a verifier from a password, salt, and iteration count.
    pub fn new(password: &str, salt: &[u8], iterations: u32) -> ScramCredential {
        let salted = pbkdf2_hmac_sha256(password.as_bytes(), salt, iterations);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac_sha256(&salted, b"Server Key");
        ScramCredential {
            salt: salt.to_vec(),
            iterations,
            stored_key,
            server_key,
        }
    }

    /// Verify a client's proof over `auth_message`. On success returns the
    /// `ServerSignature` the client can check for mutual authentication.
    pub fn verify(&self, auth_message: &[u8], client_proof: &[u8; 32]) -> Option<[u8; 32]> {
        let client_signature = hmac_sha256(&self.stored_key, auth_message);
        // ClientKey = ClientProof XOR ClientSignature
        let mut client_key = [0u8; 32];
        for i in 0..32 {
            client_key[i] = client_proof[i] ^ client_signature[i];
        }
        if ct_eq(&sha256(&client_key), &self.stored_key) {
            Some(hmac_sha256(&self.server_key, auth_message))
        } else {
            None
        }
    }

    /// Serialize for storage: `SCRAM-SHA-256$<iter>$<salt_hex>$<stored_hex>$<server_hex>`.
    pub fn to_storage_string(&self) -> String {
        format!(
            "SCRAM-SHA-256${}${}${}${}",
            self.iterations,
            hex(&self.salt),
            hex(&self.stored_key),
            hex(&self.server_key)
        )
    }

    /// Parse a verifier produced by [`ScramCredential::to_storage_string`].
    pub fn from_storage_string(s: &str) -> Option<ScramCredential> {
        let mut parts = s.split('$');
        if parts.next()? != "SCRAM-SHA-256" {
            return None;
        }
        let iterations = parts.next()?.parse().ok()?;
        let salt = from_hex(parts.next()?)?;
        let stored_key = from_hex(parts.next()?)?.try_into().ok()?;
        let server_key = from_hex(parts.next()?)?.try_into().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some(ScramCredential {
            salt,
            iterations,
            stored_key,
            server_key,
        })
    }
}

/// Client-side: compute the `ClientProof` over `auth_message` for a password.
pub fn client_proof(password: &str, salt: &[u8], iterations: u32, auth_message: &[u8]) -> [u8; 32] {
    let salted = pbkdf2_hmac_sha256(password.as_bytes(), salt, iterations);
    let client_key = hmac_sha256(&salted, b"Client Key");
    let stored_key = sha256(&client_key);
    let client_signature = hmac_sha256(&stored_key, auth_message);
    let mut proof = [0u8; 32];
    for i in 0..32 {
        proof[i] = client_key[i] ^ client_signature[i];
    }
    proof
}

/// Client-side: the expected `ServerSignature` for mutual authentication.
pub fn server_signature(
    password: &str,
    salt: &[u8],
    iterations: u32,
    auth_message: &[u8],
) -> [u8; 32] {
    let salted = pbkdf2_hmac_sha256(password.as_bytes(), salt, iterations);
    let server_key = hmac_sha256(&salted, b"Server Key");
    hmac_sha256(&server_key, auth_message)
}

fn from_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SALT: &[u8] = b"0123456789abcdef";

    #[test]
    fn correct_proof_authenticates_and_yields_server_sig() {
        let cred = ScramCredential::new("pencil", SALT, 4096);
        let auth_message = b"n=user,r=clientnonce,s=...,i=4096,c=biws,r=fullnonce";

        let proof = client_proof("pencil", SALT, 4096, auth_message);
        let server_sig = cred.verify(auth_message, &proof).expect("auth ok");

        // Client verifies the server signature (mutual auth).
        let expected = server_signature("pencil", SALT, 4096, auth_message);
        assert_eq!(server_sig, expected);
    }

    #[test]
    fn wrong_password_fails() {
        let cred = ScramCredential::new("pencil", SALT, 4096);
        let auth_message = b"auth-message";
        let proof = client_proof("WRONG", SALT, 4096, auth_message);
        assert!(cred.verify(auth_message, &proof).is_none());
    }

    #[test]
    fn tampered_auth_message_fails() {
        let cred = ScramCredential::new("pencil", SALT, 4096);
        let proof = client_proof("pencil", SALT, 4096, b"original");
        assert!(cred.verify(b"tampered", &proof).is_none());
    }

    #[test]
    fn credential_storage_roundtrip() {
        let cred = ScramCredential::new("hunter2", SALT, DEFAULT_ITERATIONS);
        let s = cred.to_storage_string();
        assert_eq!(ScramCredential::from_storage_string(&s), Some(cred));
    }

    #[test]
    fn storage_string_has_no_plaintext() {
        let cred = ScramCredential::new("supersecret", SALT, 4096);
        assert!(!cred.to_storage_string().contains("supersecret"));
    }
}
