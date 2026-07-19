//! At-rest encryption primitives (envelope, AES-256-GCM via ring).
//!
//! **Envelope.** A single **KEK** (key-encryption key, from the operator's
//! keyfile) never touches data. Each encrypted file (SSTable, WAL segment)
//! gets its own random **DEK** (data-encryption key); the DEK, wrapped by the
//! KEK, rides in the file's header. Data — compressed SSTable blocks, WAL
//! record payloads — is sealed with the DEK. This makes **key rotation cheap**
//! (rewrap the small DEKs; never re-encrypt data) and bounds nonce reuse to a
//! single file's key.
//!
//! **Nonces.** AES-GCM requires a unique nonce per (key, nonce). Within one
//! file, every sealed region is keyed by its **byte offset**, which never
//! repeats — so `nonce = offset` is unique for that file's DEK, and a fresh
//! DEK per file means offsets can safely repeat across files.
//!
//! Losing the KEK makes every DEK — and thus all data — unrecoverable. The
//! keyfile is operator-critical; back it up off-box before enabling at-rest.

use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use ring::rand::{SecureRandom, SystemRandom};

use crate::error::{Result, StorageError};

/// The AEAD tag length appended to every sealed region (AES-256-GCM = 16).
pub const TAG_LEN: usize = 16;
const KEY_LEN: usize = 32; // AES-256

fn crypto_err(msg: &str) -> StorageError {
    StorageError::Crypto(msg.to_string())
}

/// Build a 12-byte GCM nonce from a 64-bit offset (4 zero bytes + offset LE).
fn nonce_from_offset(offset: u64) -> Nonce {
    let mut n = [0u8; NONCE_LEN];
    n[NONCE_LEN - 8..].copy_from_slice(&offset.to_le_bytes());
    Nonce::assume_unique_for_key(n)
}

fn aead_key(bytes: &[u8; KEY_LEN]) -> LessSafeKey {
    let unbound = UnboundKey::new(&AES_256_GCM, bytes).expect("32-byte AES-256 key");
    LessSafeKey::new(unbound)
}

/// The key-encryption key. Wraps/unwraps per-file DEKs; never seals data.
#[derive(Clone)]
pub struct Kek([u8; KEY_LEN]);

impl std::fmt::Debug for Kek {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Kek(<redacted>)")
    }
}

impl Kek {
    /// Load a KEK from a keyfile: exactly 32 raw bytes, or 64 hex chars.
    pub fn from_keyfile(path: &str) -> Result<Kek> {
        let raw = std::fs::read(path)
            .map_err(|e| crypto_err(&format!("read keyfile {path}: {e}")))?;
        Self::from_bytes(&raw)
            .or_else(|_| {
                // Accept a hex-encoded keyfile (64 hex chars, trailing ws ok).
                let hex: String = String::from_utf8_lossy(&raw)
                    .chars()
                    .filter(|c| !c.is_whitespace())
                    .collect();
                let decoded = decode_hex(&hex)
                    .ok_or_else(|| crypto_err("keyfile is neither 32 raw bytes nor 64 hex chars"))?;
                Self::from_bytes(&decoded)
            })
    }

    /// A KEK from exactly 32 bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Kek> {
        let arr: [u8; KEY_LEN] = bytes
            .try_into()
            .map_err(|_| crypto_err("key must be exactly 32 bytes"))?;
        Ok(Kek(arr))
    }

    /// Generate a fresh random KEK (for `skaidbsh` key generation).
    pub fn generate() -> Result<Kek> {
        Ok(Kek(random_key()?))
    }

    /// The raw 32 bytes (for writing a generated keyfile).
    pub fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }

    /// Wrap a fresh random DEK, returning `(dek, wrapped)`. `wrapped` is
    /// `nonce[12] | ciphertext(32) | tag(16)` = 60 bytes, stored in the file
    /// header. The KEK-wrap nonce is random (only a handful of DEKs are ever
    /// wrapped by one KEK per file, so a random 96-bit nonce is safe).
    pub fn wrap_new_dek(&self) -> Result<(Dek, Vec<u8>)> {
        let dek = random_key()?;
        let mut nonce_bytes = [0u8; NONCE_LEN];
        SystemRandom::new()
            .fill(&mut nonce_bytes)
            .map_err(|_| crypto_err("rng"))?;
        let mut in_out = dek.to_vec();
        aead_key(&self.0)
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::empty(),
                &mut in_out,
            )
            .map_err(|_| crypto_err("wrap dek"))?;
        let mut wrapped = Vec::with_capacity(NONCE_LEN + in_out.len());
        wrapped.extend_from_slice(&nonce_bytes);
        wrapped.extend_from_slice(&in_out);
        Ok((Dek(dek), wrapped))
    }

    /// Unwrap a DEK from a file header's wrapped bytes.
    pub fn unwrap_dek(&self, wrapped: &[u8]) -> Result<Dek> {
        if wrapped.len() < NONCE_LEN + KEY_LEN + TAG_LEN {
            return Err(crypto_err("wrapped DEK too short"));
        }
        let (nonce_bytes, ct) = wrapped.split_at(NONCE_LEN);
        let mut in_out = ct.to_vec();
        let nonce = Nonce::try_assume_unique_for_key(nonce_bytes)
            .map_err(|_| crypto_err("bad wrap nonce"))?;
        let plain = aead_key(&self.0)
            .open_in_place(nonce, Aad::empty(), &mut in_out)
            .map_err(|_| crypto_err("unwrap dek (wrong KEK or corrupt header)"))?;
        Dek::from_bytes(plain)
    }
}

/// A per-file data-encryption key. Seals/opens data regions.
pub struct Dek(LessSafeKeyBytes);

impl std::fmt::Debug for Dek {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Dek(<redacted>)")
    }
}

/// Keep the raw key bytes so a `Dek` is cheap to hold; build the ring key per
/// call (ring's `LessSafeKey` isn't `Clone`).
type LessSafeKeyBytes = [u8; KEY_LEN];

impl Dek {
    fn from_bytes(bytes: &[u8]) -> Result<Dek> {
        let arr: [u8; KEY_LEN] = bytes
            .try_into()
            .map_err(|_| crypto_err("dek must be 32 bytes"))?;
        Ok(Dek(arr))
    }

    /// Seal `plaintext` for the region at `offset`. Returns
    /// `ciphertext || tag` (input length + [`TAG_LEN`]). `offset` is the
    /// nonce and part of the AAD, binding the ciphertext to its position.
    pub fn seal(&self, offset: u64, plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut in_out = plaintext.to_vec();
        aead_key(&self.0)
            .seal_in_place_append_tag(
                nonce_from_offset(offset),
                Aad::from(offset.to_le_bytes()),
                &mut in_out,
            )
            .map_err(|_| crypto_err("seal"))?;
        Ok(in_out)
    }

    /// Open a `ciphertext || tag` region sealed at `offset`. A wrong key,
    /// wrong offset, or any tampering fails the AEAD tag — never returns
    /// garbage as data.
    pub fn open(&self, offset: u64, ciphertext: &[u8]) -> Result<Vec<u8>> {
        if ciphertext.len() < TAG_LEN {
            return Err(crypto_err("ciphertext shorter than tag"));
        }
        let mut in_out = ciphertext.to_vec();
        let plain = aead_key(&self.0)
            .open_in_place(
                nonce_from_offset(offset),
                Aad::from(offset.to_le_bytes()),
                &mut in_out,
            )
            .map_err(|_| crypto_err("open (auth failed: wrong key or tampered data)"))?;
        Ok(plain.to_vec())
    }
}

fn random_key() -> Result<[u8; KEY_LEN]> {
    let mut k = [0u8; KEY_LEN];
    SystemRandom::new()
        .fill(&mut k)
        .map_err(|_| crypto_err("rng"))?;
    Ok(k)
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
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

    #[test]
    fn seal_open_round_trip() {
        let kek = Kek::generate().unwrap();
        let (dek, wrapped) = kek.wrap_new_dek().unwrap();
        let pt = b"the quick brown fox jumps over the lazy dog";
        let ct = dek.seal(4096, pt).unwrap();
        assert_eq!(ct.len(), pt.len() + TAG_LEN);
        assert_ne!(&ct[..pt.len()], &pt[..], "ciphertext must differ from plaintext");
        assert_eq!(dek.open(4096, &ct).unwrap(), pt);
        // Unwrap the DEK via the KEK and re-open — envelope round-trips.
        let dek2 = kek.unwrap_dek(&wrapped).unwrap();
        assert_eq!(dek2.open(4096, &ct).unwrap(), pt);
    }

    #[test]
    fn wrong_offset_or_tamper_fails() {
        let kek = Kek::generate().unwrap();
        let (dek, _) = kek.wrap_new_dek().unwrap();
        let ct = dek.seal(100, b"secret").unwrap();
        assert!(dek.open(101, &ct).is_err(), "wrong offset (nonce/aad) must fail");
        let mut bad = ct.clone();
        bad[0] ^= 0x01;
        assert!(dek.open(100, &bad).is_err(), "a flipped bit must fail the tag");
    }

    #[test]
    fn wrong_kek_cannot_unwrap() {
        let kek = Kek::generate().unwrap();
        let (_, wrapped) = kek.wrap_new_dek().unwrap();
        let other = Kek::generate().unwrap();
        assert!(other.unwrap_dek(&wrapped).is_err(), "a different KEK must not unwrap");
    }

    #[test]
    fn keyfile_accepts_raw_and_hex() {
        let raw = [7u8; KEY_LEN];
        assert!(Kek::from_bytes(&raw).is_ok());
        let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(decode_hex(&hex).unwrap(), raw.to_vec());
    }
}
