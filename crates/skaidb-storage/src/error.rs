//! Storage engine error types.

/// Errors raised by the storage engine.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The WAL contained a record whose checksum did not match, indicating a
    /// torn or corrupted write. The offset is where the bad record began.
    #[error("WAL corruption at offset {offset}: {detail}")]
    Corruption { offset: u64, detail: &'static str },

    /// At-rest encryption failure: a bad/missing key, an unloadable keyfile,
    /// or an AEAD authentication failure (tampered or wrong-key data). Kept
    /// distinct from `Corruption` so "wrong key" never looks like disk rot.
    #[error("at-rest crypto: {0}")]
    Crypto(String),
}

/// Convenience result alias for storage operations.
pub type Result<T> = std::result::Result<T, StorageError>;
