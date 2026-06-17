//! Hybrid Logical Clocks (SPEC §5).
//!
//! An [`Hlc`] timestamp combines a physical millisecond component with a logical
//! counter, giving a monotonic, causally-consistent ordering that tolerates
//! bounded clock skew. It is the version stamp for every write and the basis for
//! field-level last-writer-wins conflict resolution.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// A hybrid logical clock timestamp.
///
/// Ordering is by physical time first, then the logical counter, so derived
/// `Ord` gives the intended causal order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hlc {
    /// Physical time in unixtime milliseconds.
    pub physical: u64,
    /// Logical counter, incremented to break ties within the same millisecond.
    pub logical: u32,
}

impl Hlc {
    /// The smallest possible timestamp.
    pub const MIN: Hlc = Hlc {
        physical: 0,
        logical: 0,
    };
    /// The largest possible timestamp (useful as a scan sentinel).
    pub const MAX: Hlc = Hlc {
        physical: u64::MAX,
        logical: u32::MAX,
    };

    pub fn new(physical: u64, logical: u32) -> Self {
        Hlc { physical, logical }
    }

    /// Encode as 12 big-endian bytes (physical then logical), preserving order.
    pub fn to_bytes(self) -> [u8; 12] {
        let mut out = [0u8; 12];
        out[..8].copy_from_slice(&self.physical.to_be_bytes());
        out[8..].copy_from_slice(&self.logical.to_be_bytes());
        out
    }

    /// Decode from the 12-byte big-endian form produced by [`Hlc::to_bytes`].
    pub fn from_bytes(bytes: [u8; 12]) -> Self {
        let mut p = [0u8; 8];
        p.copy_from_slice(&bytes[..8]);
        let mut l = [0u8; 4];
        l.copy_from_slice(&bytes[8..]);
        Hlc {
            physical: u64::from_be_bytes(p),
            logical: u32::from_be_bytes(l),
        }
    }
}

/// A thread-safe hybrid logical clock generator.
#[derive(Debug)]
pub struct HlcClock {
    last: Mutex<Hlc>,
}

impl HlcClock {
    /// Create a clock seeded at [`Hlc::MIN`].
    pub fn new() -> Self {
        HlcClock {
            last: Mutex::new(Hlc::MIN),
        }
    }

    /// Generate the next timestamp, guaranteed strictly greater than any prior
    /// value returned by this clock or observed via [`HlcClock::observe`].
    pub fn now(&self) -> Hlc {
        let mut last = self.last.lock().expect("hlc mutex poisoned");
        let next = advance(*last, physical_now_ms());
        *last = next;
        next
    }

    /// The clock's current frontier: the greatest timestamp it has generated or
    /// observed, without advancing it. Used as this node's "latest known write"
    /// reference point when measuring how far a peer is behind.
    pub fn peek(&self) -> Hlc {
        *self.last.lock().expect("hlc mutex poisoned")
    }

    /// Merge a remote timestamp (e.g. received from a peer) so this clock stays
    /// ahead of causally-prior events, then return a fresh local timestamp.
    pub fn observe(&self, remote: Hlc) -> Hlc {
        let mut last = self.last.lock().expect("hlc mutex poisoned");
        let wall = physical_now_ms();
        let base = (*last).max(remote);
        let next = advance(base, wall);
        *last = next;
        next
    }
}

impl Default for HlcClock {
    fn default() -> Self {
        HlcClock::new()
    }
}

/// Advance `last` toward wall-clock `wall`, bumping the logical counter when the
/// physical component does not move forward.
fn advance(last: Hlc, wall: u64) -> Hlc {
    if wall > last.physical {
        Hlc {
            physical: wall,
            logical: 0,
        }
    } else {
        Hlc {
            physical: last.physical,
            logical: last.logical + 1,
        }
    }
}

fn physical_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_is_strictly_monotonic() {
        let clock = HlcClock::new();
        let mut prev = Hlc::MIN;
        for _ in 0..1000 {
            let t = clock.now();
            assert!(t > prev, "expected {t:?} > {prev:?}");
            prev = t;
        }
    }

    #[test]
    fn observe_moves_past_remote() {
        let clock = HlcClock::new();
        let remote = Hlc::new(physical_now_ms() + 60_000, 5);
        let t = clock.observe(remote);
        assert!(t > remote);
    }

    #[test]
    fn bytes_roundtrip_and_order() {
        let a = Hlc::new(100, 2);
        let b = Hlc::new(100, 3);
        assert_eq!(Hlc::from_bytes(a.to_bytes()), a);
        assert!(a.to_bytes() < b.to_bytes());
        assert!(a < b);
    }
}
