//! OS randomness: fallible for secrets and identifiers, best-effort for jitter and name suffixes.

use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub use getrandom::Error;

/// `N` bytes from the OS CSPRNG; fails only when the kernel source is unavailable.
pub fn bytes<const N: usize>() -> Result<[u8; N], Error> {
    let mut buf = [0u8; N];
    getrandom::fill(&mut buf)?;
    Ok(buf)
}

/// A value for collision avoidance only; never use it for secrets or identifiers.
pub fn weak_u64() -> u64 {
    getrandom::u64().unwrap_or_else(|_| clock_fallback())
}

/// Uniform-enough jitter in `range`; modulo bias is irrelevant at millisecond spans.
pub fn jitter(range: RangeInclusive<u64>) -> u64 {
    let (low, high) = range.into_inner();
    let span = high.saturating_sub(low).saturating_add(1);
    low + weak_u64() % span
}

fn clock_fallback() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    nanos ^ COUNTER.fetch_add(1, Ordering::Relaxed).rotate_left(32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_within_inclusive_bounds() {
        for _ in 0..1000 {
            let v = jitter(100..=103);
            assert!((100..=103).contains(&v), "{v}");
        }
        assert_eq!(jitter(7..=7), 7);
    }

    #[test]
    fn clock_fallback_differs_between_calls() {
        assert_ne!(clock_fallback(), clock_fallback());
    }
}
