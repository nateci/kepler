//! Hybrid Logical Clock.
//!
//! Packs `(physical_ms : u48, logical : u16)` into a single `u64`. Monotonic
//! across a single process; nearly-monotonic across processes given bounded
//! clock skew. Updates the local clock on every observed remote timestamp to
//! preserve causal ordering.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use kepler_types::Timestamp;

const LOGICAL_BITS: u32 = 16;
const LOGICAL_MASK: u64 = (1u64 << LOGICAL_BITS) - 1;

pub struct HybridLogicalClock {
    state: AtomicU64,
}

impl HybridLogicalClock {
    pub fn new() -> Self {
        Self { state: AtomicU64::new(physical_now() << LOGICAL_BITS) }
    }

    /// Produce a new local timestamp.
    pub fn now(&self) -> Timestamp {
        let phys_now = physical_now();
        loop {
            let prev = self.state.load(Ordering::Acquire);
            let prev_phys = prev >> LOGICAL_BITS;
            let prev_log = prev & LOGICAL_MASK;

            let next = if phys_now > prev_phys {
                phys_now << LOGICAL_BITS
            } else {
                (prev_phys << LOGICAL_BITS) | (prev_log + 1)
            };

            if self
                .state
                .compare_exchange(prev, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return next;
            }
        }
    }

    /// Advance the clock to be strictly greater than `remote`. Call when
    /// receiving a timestamp from another node.
    pub fn observe(&self, remote: Timestamp) {
        let phys_now = physical_now();
        loop {
            let prev = self.state.load(Ordering::Acquire);
            let prev_phys = prev >> LOGICAL_BITS;
            let prev_log = prev & LOGICAL_MASK;
            let rem_phys = remote >> LOGICAL_BITS;
            let rem_log = remote & LOGICAL_MASK;

            let max_phys = phys_now.max(prev_phys).max(rem_phys);
            let next_log = if max_phys == prev_phys && max_phys == rem_phys {
                prev_log.max(rem_log) + 1
            } else if max_phys == prev_phys {
                prev_log + 1
            } else if max_phys == rem_phys {
                rem_log + 1
            } else {
                0
            };
            let next = (max_phys << LOGICAL_BITS) | (next_log & LOGICAL_MASK);

            if self
                .state
                .compare_exchange(prev, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

fn physical_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hlc_is_strictly_monotonic() {
        let clock = HybridLogicalClock::new();
        let mut last = clock.now();
        for _ in 0..1000 {
            let t = clock.now();
            assert!(t > last);
            last = t;
        }
    }

    #[test]
    fn observe_advances_past_remote() {
        let clock = HybridLogicalClock::new();
        let remote = clock.now() + 1_000_000;
        clock.observe(remote);
        assert!(clock.now() > remote);
    }
}
