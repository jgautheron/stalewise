//! Bounded in-memory history of admitted invalidations and their per-target delivery state.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use serde::Serialize;
use tokio::time::Instant;

/// Random 128-bit invalidation identifier, written as 32 lowercase hex digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InvalidationId(u128);

impl InvalidationId {
    /// Fails only when the OS CSPRNG is unavailable.
    pub fn random() -> Result<Self, crate::entropy::Error> {
        crate::entropy::bytes().map(|b| Self(u128::from_le_bytes(b)))
    }
}

impl fmt::Display for InvalidationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

impl FromStr for InvalidationId {
    type Err = ();

    /// Accepts exactly 32 lowercase hexadecimal characters.
    fn from_str(raw: &str) -> Result<Self, ()> {
        let valid = raw.len() == 32 && raw.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
        if !valid {
            return Err(());
        }
        u128::from_str_radix(raw, 16).map(Self).map_err(|_| ())
    }
}

impl Serialize for InvalidationId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

/// One delivery destination frozen at admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub pod: Option<String>,
    pub addr: SocketAddr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryState {
    Pending,
    Delivered,
    Exhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ExhaustReason {
    Rejected,
    Deadline,
    Shutdown,
}

/// The terminal result of one target's delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Delivered,
    Exhausted(ExhaustReason),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PeerStatus {
    pub pod: Option<String>,
    pub address: String,
    pub status: DeliveryState,
    pub reason: Option<ExhaustReason>,
}

/// The JSON document returned for writes and status lookups.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Status {
    pub id: InvalidationId,
    pub pod: String,
    pub peers: Vec<PeerStatus>,
}

impl Status {
    pub fn finished(&self) -> bool {
        self.peers
            .iter()
            .all(|p| p.status != DeliveryState::Pending)
    }

    pub fn all_delivered(&self) -> bool {
        self.peers
            .iter()
            .all(|p| p.status == DeliveryState::Delivered)
    }
}

/// Reported once the last target of an invalidation reaches a terminal state.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Completion {
    pub latency: Duration,
    pub delivered: bool,
}

struct Record {
    status: Status,
    admitted: Instant,
    pending: usize,
}

/// Retains at most `max_records` invalidations and `max_rows` target rows; only terminal records are evicted.
pub struct History {
    max_records: usize,
    max_rows: usize,
    rows: usize,
    next_seq: u64,
    records: BTreeMap<u64, Record>,
    index: HashMap<InvalidationId, u64>,
}

impl History {
    pub fn new(max_records: usize, max_rows: usize) -> Self {
        Self {
            max_records,
            max_rows,
            rows: 0,
            next_seq: 0,
            records: BTreeMap::new(),
            index: HashMap::new(),
        }
    }

    /// The oldest terminal records to evict so `rows` more fit, or `None` when pending work blocks it.
    pub fn plan(&self, rows: usize) -> Option<Vec<u64>> {
        let mut records = self.records.len();
        let mut used = self.rows;
        let fits = |records: usize, used: usize| {
            records < self.max_records && used.saturating_add(rows) <= self.max_rows
        };
        let mut victims = Vec::new();
        for (seq, record) in &self.records {
            if fits(records, used) {
                break;
            }
            if record.pending == 0 {
                victims.push(*seq);
                records -= 1;
                used -= record.status.peers.len();
            }
        }
        fits(records, used).then_some(victims)
    }

    /// Applies a plan from [`History::plan`] and records a new all-pending invalidation.
    pub fn insert(&mut self, victims: &[u64], status: Status, now: Instant) {
        for seq in victims {
            if let Some(old) = self.records.remove(seq) {
                self.rows -= old.status.peers.len();
                self.index.remove(&old.status.id);
            }
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.rows += status.peers.len();
        self.index.insert(status.id, seq);
        let pending = status.peers.len();
        self.records.insert(
            seq,
            Record {
                status,
                admitted: now,
                pending,
            },
        );
    }

    pub fn lookup(&self, id: InvalidationId) -> Option<Status> {
        let seq = self.index.get(&id)?;
        self.records.get(seq).map(|r| r.status.clone())
    }

    /// Moves one pending target to its terminal state; later transitions for it are ignored.
    pub fn complete(
        &mut self,
        id: InvalidationId,
        slot: usize,
        outcome: Outcome,
        now: Instant,
    ) -> Option<Completion> {
        let seq = self.index.get(&id)?;
        let record = self.records.get_mut(seq)?;
        let row = record.status.peers.get_mut(slot)?;
        if row.status != DeliveryState::Pending {
            return None;
        }
        (row.status, row.reason) = match outcome {
            Outcome::Delivered => (DeliveryState::Delivered, None),
            Outcome::Exhausted(reason) => (DeliveryState::Exhausted, Some(reason)),
        };
        record.pending -= 1;
        (record.pending == 0).then(|| Completion {
            latency: now.saturating_duration_since(record.admitted),
            delivered: record.status.all_delivered(),
        })
    }

    /// Distinct peer identities with any retained pending or exhausted row.
    pub fn lagging(&self) -> usize {
        let lagging: BTreeSet<String> = self
            .records
            .values()
            .flat_map(|r| &r.status.peers)
            .filter(|p| p.status != DeliveryState::Delivered)
            .map(|p| p.pod.clone().unwrap_or_else(|| p.address.clone()))
            .collect();
        lagging.len()
    }
}

/// An all-pending status for freshly admitted targets.
pub fn pending_status(id: InvalidationId, pod: &str, targets: &[Target]) -> Status {
    Status {
        id,
        pod: pod.to_owned(),
        peers: targets
            .iter()
            .map(|t| PeerStatus {
                pod: t.pod.clone(),
                address: t.addr.to_string(),
                status: DeliveryState::Pending,
                reason: None,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn targets(n: usize) -> Vec<Target> {
        (0..n)
            .map(|i| Target {
                pod: Some(format!("cache-{i}")),
                addr: format!("10.0.0.{i}:8080").parse().unwrap(),
            })
            .collect()
    }

    fn admit(h: &mut History, n: usize) -> Option<InvalidationId> {
        let victims = h.plan(n)?;
        let id = InvalidationId::random().unwrap();
        h.insert(
            &victims,
            pending_status(id, "cache-0", &targets(n)),
            Instant::now(),
        );
        Some(id)
    }

    fn finish(h: &mut History, id: InvalidationId, n: usize) {
        for slot in 0..n {
            h.complete(id, slot, Outcome::Delivered, Instant::now());
        }
    }

    #[test]
    fn ids_are_32_lowercase_hex_and_round_trip() {
        let id = InvalidationId::random().unwrap();
        let text = id.to_string();
        assert_eq!(text.len(), 32);
        assert_eq!(text.parse::<InvalidationId>(), Ok(id));
        for bad in [
            "",
            "abc",
            &"A".repeat(32),
            &"g".repeat(32),
            &"0".repeat(33),
            &format!("+{}", "0".repeat(31)),
        ] {
            assert!(bad.parse::<InvalidationId>().is_err(), "{bad}");
        }
    }

    #[test]
    fn oldest_terminal_records_are_evicted_first_and_pending_ones_never() {
        let mut h = History::new(2, 100);
        let first = admit(&mut h, 1).unwrap();
        let second = admit(&mut h, 1).unwrap();
        assert!(h.plan(1).is_none(), "both pending: no room");
        finish(&mut h, second, 1);
        let third = admit(&mut h, 1).unwrap();
        assert!(h.lookup(first).is_some(), "pending survives eviction");
        assert!(h.lookup(second).is_none(), "terminal record evicted");
        assert!(h.lookup(third).is_some());
        finish(&mut h, first, 1);
        finish(&mut h, third, 1);
        admit(&mut h, 1).unwrap();
        assert!(h.lookup(first).is_none(), "oldest terminal goes first");
        assert!(h.lookup(third).is_some());
    }

    #[test]
    fn row_budget_bounds_retained_targets() {
        let mut h = History::new(100, 5);
        let a = admit(&mut h, 3).unwrap();
        assert!(
            h.plan(3).is_none(),
            "6 rows exceed the budget while a is pending"
        );
        assert!(h.plan(6).is_none(), "an oversized batch never fits");
        finish(&mut h, a, 3);
        assert_eq!(h.plan(3), Some(vec![0]));
    }

    #[test]
    fn transitions_are_monotonic_and_complete_once() {
        let mut h = History::new(4, 16);
        let id = admit(&mut h, 2).unwrap();
        let now = Instant::now();
        assert_eq!(
            h.complete(id, 0, Outcome::Exhausted(ExhaustReason::Deadline), now),
            None
        );
        assert_eq!(
            h.complete(id, 0, Outcome::Delivered, now),
            None,
            "terminal rows never change"
        );
        assert_eq!(h.lagging(), 2, "one exhausted and one pending peer");
        let done = h.complete(id, 1, Outcome::Delivered, now).unwrap();
        assert!(!done.delivered);
        let status = h.lookup(id).unwrap();
        assert!(status.finished() && !status.all_delivered());
        assert_eq!(status.peers[0].reason, Some(ExhaustReason::Deadline));
        assert_eq!(
            h.complete(id, 9, Outcome::Delivered, now),
            None,
            "unknown slot"
        );
    }

    #[test]
    fn lagging_counts_distinct_identities_and_falls_on_eviction() {
        let mut h = History::new(1, 16);
        let a = admit(&mut h, 2).unwrap();
        assert_eq!(h.lagging(), 2);
        h.complete(
            a,
            0,
            Outcome::Exhausted(ExhaustReason::Rejected),
            Instant::now(),
        );
        h.complete(a, 1, Outcome::Delivered, Instant::now());
        assert_eq!(h.lagging(), 1);
        let b = admit(&mut h, 1).unwrap();
        finish(&mut h, b, 1);
        assert_eq!(h.lagging(), 0, "eviction drops the exhausted row");
    }

    #[test]
    fn status_serialises_the_documented_shape() {
        let id: InvalidationId = "0123456789abcdef0123456789abcdef".parse().unwrap();
        let mut status = pending_status(id, "cache-0", &targets(1));
        status.peers[0].status = DeliveryState::Exhausted;
        status.peers[0].reason = Some(ExhaustReason::Shutdown);
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"id": "0123456789abcdef0123456789abcdef", "pod": "cache-0",
                "peers": [{"pod": "cache-0", "address": "10.0.0.0:8080", "status": "exhausted", "reason": "shutdown"}]})
        );
    }
}
