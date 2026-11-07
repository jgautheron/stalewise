//! Stable cache-peer membership: pod identity, lexical order and per-peer hysteresis.

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::time::Duration;

use k8s_openapi::api::discovery::v1::{Endpoint as SliceEndpoint, EndpointSlice};
use serde::Serialize;
use tokio::time::Instant;

use crate::discovery::{DiscoveryError, slice_port};
use crate::endpoint::{Endpoint, Host};

const MAX_NAME: usize = 253;

/// One eligible cache pod; `namespace/pod_name` is its ring identity, independent of address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Peer {
    pub pod_name: String,
    pub namespace: String,
    pub address: IpAddr,
    pub port: u16,
}

impl Peer {
    /// The stable shard identity, `namespace/pod_name`.
    pub fn ident(&self) -> String {
        format!("{}/{}", self.namespace, self.pod_name)
    }

    pub fn endpoint(&self) -> Endpoint {
        Endpoint::new(Host::Ip(self.address), self.port)
    }
}

/// One peer per ready, non-terminating Pod endpoint, preferring its lowest (IPv4 first) address.
/// Sorted by `namespace/pod`; endpoints without a Pod reference are excluded.
pub fn merge_peers(slices: &[EndpointSlice], port: &str) -> Result<Vec<Peer>, DiscoveryError> {
    let mut by_ident: BTreeMap<String, Peer> = BTreeMap::new();
    for slice in slices {
        let Some(number) = slice_port(slice, port)? else {
            continue;
        };
        let family_v6 = match slice.address_type.as_str() {
            "IPv4" => false,
            "IPv6" => true,
            _ => continue,
        };
        let slice_ns = slice.metadata.namespace.as_deref();
        for endpoint in &slice.endpoints {
            let Some((namespace, pod_name)) = eligible(endpoint, slice_ns) else {
                continue;
            };
            for address in endpoint
                .addresses
                .iter()
                .filter_map(|a| a.parse::<IpAddr>().ok())
            {
                if address.is_ipv6() != family_v6 {
                    continue;
                }
                let peer = Peer {
                    pod_name: pod_name.clone(),
                    namespace: namespace.clone(),
                    address,
                    port: number,
                };
                by_ident
                    .entry(peer.ident())
                    .and_modify(|kept| {
                        if peer.address < kept.address {
                            *kept = peer.clone();
                        }
                    })
                    .or_insert(peer);
            }
        }
    }
    Ok(by_ident.into_values().collect())
}

/// `(namespace, pod)` of a ready, non-terminating Pod endpoint; logs why others are excluded.
fn eligible(endpoint: &SliceEndpoint, slice_ns: Option<&str>) -> Option<(String, String)> {
    let conditions = endpoint.conditions.as_ref();
    if conditions.and_then(|c| c.ready) != Some(true)
        || conditions.and_then(|c| c.terminating) == Some(true)
    {
        return None;
    }
    let target = endpoint.target_ref.as_ref();
    let pod = target.filter(|t| t.kind.as_deref() == Some("Pod"));
    let name = pod.and_then(|t| t.name.clone()).filter(|n| valid_name(n));
    let namespace = pod
        .and_then(|t| t.namespace.as_deref().or(slice_ns))
        .filter(|n| valid_name(n))
        .map(str::to_owned);
    match (namespace, name) {
        (Some(namespace), Some(name)) => Some((namespace, name)),
        _ => {
            tracing::warn!(addresses = ?endpoint.addresses, "excluding peer without a valid Pod identity");
            None
        }
    }
}

/// DNS-1123 subdomain: the identity is embedded in a VCL string, so no quote can reach it.
pub fn valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_NAME
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-' || *b == b'.')
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
}

/// Per-peer hysteresis: a change commits once it has persisted for `interval`, at most once per interval.
#[derive(Debug)]
pub struct Debouncer {
    interval: Duration,
    observed: Option<BTreeMap<String, Peer>>,
    committed: Option<BTreeMap<String, Peer>>,
    /// When each differing peer's current observed state was first seen.
    pending: BTreeMap<String, Instant>,
    last_commit: Option<Instant>,
}

impl Debouncer {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            observed: None,
            committed: None,
            pending: BTreeMap::new(),
            last_commit: None,
        }
    }

    /// Records a complete observation; a peer whose state changes again restarts its timer.
    pub fn observe(&mut self, peers: Vec<Peer>, now: Instant) {
        let next: BTreeMap<String, Peer> = peers.into_iter().map(|p| (p.ident(), p)).collect();
        let previous = self.observed.take().unwrap_or_default();
        let committed = self.committed.clone().unwrap_or_default();
        let mut keys: Vec<&String> = next.keys().chain(committed.keys()).collect();
        keys.sort();
        keys.dedup();
        for key in keys {
            let wanted = next.get(key);
            if wanted == committed.get(key) {
                self.pending.remove(key);
            } else if wanted != previous.get(key) || !self.pending.contains_key(key) {
                self.pending.insert(key.clone(), now);
            }
        }
        self.observed = Some(next);
    }

    /// Commits due changes; returns the new membership when it changed. The first observation commits at once.
    pub fn poll(&mut self, now: Instant) -> Option<Vec<Peer>> {
        let observed = self.observed.as_ref()?;
        let Some(committed) = self.committed.as_mut() else {
            self.committed = Some(observed.clone());
            self.pending.clear();
            self.last_commit = Some(now);
            return self.snapshot();
        };
        if self.last_commit.is_some_and(|t| now < t + self.interval) {
            return None;
        }
        let due: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, since)| **since + self.interval <= now)
            .map(|(key, _)| key.clone())
            .collect();
        if due.is_empty() {
            return None;
        }
        for key in due {
            self.pending.remove(&key);
            match observed.get(&key) {
                Some(peer) => committed.insert(key, peer.clone()),
                None => committed.remove(&key),
            };
        }
        self.last_commit = Some(now);
        self.snapshot()
    }

    /// The earliest instant `poll` could commit a pending change.
    pub fn deadline(&self) -> Option<Instant> {
        let earliest = *self.pending.values().min()? + self.interval;
        let window = self.last_commit.map(|t| t + self.interval);
        Some(window.map_or(earliest, |w| w.max(earliest)))
    }

    fn snapshot(&self) -> Option<Vec<Peer>> {
        self.committed
            .as_ref()
            .map(|c| c.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn slice(family: &str, endpoints: serde_json::Value) -> EndpointSlice {
        serde_json::from_value(json!({
            "apiVersion": "discovery.k8s.io/v1",
            "kind": "EndpointSlice",
            "metadata": {"name": format!("s-{family}"), "namespace": "apps"},
            "addressType": family,
            "ports": [{"name": "http", "port": 8080, "protocol": "TCP"}],
            "endpoints": endpoints,
        }))
        .unwrap()
    }

    fn pod(name: &str, ip: &str) -> serde_json::Value {
        json!({"addresses": [ip], "conditions": {"ready": true},
               "targetRef": {"kind": "Pod", "namespace": "apps", "name": name}})
    }

    fn peer(name: &str, ip: &str) -> Peer {
        Peer {
            pod_name: name.into(),
            namespace: "apps".into(),
            address: ip.parse().unwrap(),
            port: 8080,
        }
    }

    fn idents(peers: &[Peer]) -> Vec<String> {
        peers
            .iter()
            .map(|p| format!("{}@{}", p.ident(), p.address))
            .collect()
    }

    #[test]
    fn membership_order_is_lexical_by_pod_and_independent_of_input_order() {
        let a = slice(
            "IPv4",
            json!([pod("cache-2", "10.0.0.1"), pod("cache-10", "10.0.0.3")]),
        );
        let b = slice("IPv4", json!([pod("cache-1", "10.0.0.9")]));
        let want = [
            "apps/cache-1@10.0.0.9",
            "apps/cache-10@10.0.0.3",
            "apps/cache-2@10.0.0.1",
        ];
        assert_eq!(
            idents(&merge_peers(&[a.clone(), b.clone()], "http").unwrap()),
            want
        );
        assert_eq!(idents(&merge_peers(&[b, a], "http").unwrap()), want);
    }

    #[test]
    fn dual_stack_pod_is_one_peer_preferring_ipv4() {
        let v6 = slice("IPv6", json!([pod("cache-0", "fd00::1")]));
        let v4 = slice("IPv4", json!([pod("cache-0", "10.0.0.1")]));
        let only_v6 = merge_peers(std::slice::from_ref(&v6), "http").unwrap();
        assert_eq!(idents(&only_v6), ["apps/cache-0@fd00::1"]);
        assert_eq!(
            idents(&merge_peers(&[v6, v4], "http").unwrap()),
            ["apps/cache-0@10.0.0.1"]
        );
    }

    #[test]
    fn eligibility_needs_explicit_readiness_a_pod_reference_and_no_termination() {
        let s = slice(
            "IPv4",
            json!([
                pod("ok", "10.0.0.1"),
                {"addresses": ["10.0.0.2"], "targetRef": {"kind": "Pod", "name": "unknown-ready"}},
                {"addresses": ["10.0.0.3"], "conditions": {"ready": true, "terminating": true},
                 "targetRef": {"kind": "Pod", "name": "leaving"}},
                {"addresses": ["10.0.0.4"], "conditions": {"ready": true}},
                {"addresses": ["10.0.0.5"], "conditions": {"ready": true},
                 "targetRef": {"kind": "Node", "name": "n"}},
                {"addresses": ["10.0.0.6"], "conditions": {"ready": true},
                 "targetRef": {"kind": "Pod", "name": "Bad\"Name"}},
                {"addresses": ["10.0.0.7"], "conditions": {"ready": true},
                 "targetRef": {"kind": "Pod", "name": "slice-ns"}},
            ]),
        );
        assert_eq!(
            idents(&merge_peers(&[s], "http").unwrap()),
            ["apps/ok@10.0.0.1", "apps/slice-ns@10.0.0.7"]
        );
    }

    #[test]
    fn ip_change_keeps_the_identity_and_replacement_changes_it() {
        let before = peer("cache-0", "10.0.0.1");
        let moved = peer("cache-0", "10.0.0.9");
        let replaced = peer("cache-0-x", "10.0.0.1");
        assert_eq!(before.ident(), moved.ident());
        assert_ne!(before.ident(), replaced.ident());
    }

    const D: Duration = Duration::from_secs(5);

    #[test]
    fn first_observation_commits_at_once_then_changes_wait_for_the_interval() {
        let t0 = Instant::now();
        let mut d = Debouncer::new(D);
        assert_eq!(d.poll(t0), None, "nothing observed yet");
        d.observe(vec![peer("a", "10.0.0.1")], t0);
        assert_eq!(d.poll(t0).unwrap().len(), 1);
        d.observe(vec![peer("a", "10.0.0.1"), peer("b", "10.0.0.2")], t0 + D);
        assert_eq!(d.deadline(), Some(t0 + D * 2));
        assert_eq!(d.poll(t0 + D * 2 - Duration::from_millis(1)), None);
        assert_eq!(d.poll(t0 + D * 2).unwrap().len(), 2);
        assert_eq!(d.deadline(), None);
    }

    #[test]
    fn readiness_flapping_never_commits_and_settles_back() {
        let t0 = Instant::now();
        let mut d = Debouncer::new(D);
        d.observe(vec![peer("a", "10.0.0.1")], t0);
        d.poll(t0);
        for i in 1..10u32 {
            let now = t0 + Duration::from_secs(u64::from(i));
            let peers = if i % 2 == 1 {
                vec![]
            } else {
                vec![peer("a", "10.0.0.1")]
            };
            d.observe(peers, now);
            assert_eq!(d.poll(now), None, "flap {i} must not commit");
        }
        assert_eq!(d.deadline(), Some(t0 + Duration::from_secs(14)));
        d.observe(vec![peer("a", "10.0.0.1")], t0 + Duration::from_secs(10));
        assert_eq!(
            d.deadline(),
            None,
            "returning to the committed state clears the timer"
        );
    }

    #[test]
    fn unrelated_flapping_cannot_postpone_a_stable_change() {
        let t0 = Instant::now();
        let mut d = Debouncer::new(D);
        d.observe(vec![peer("a", "10.0.0.1"), peer("flappy", "10.0.0.5")], t0);
        d.poll(t0);
        let with_b = |flappy: bool| {
            let mut v = vec![peer("a", "10.0.0.1"), peer("b", "10.0.0.2")];
            if flappy {
                v.push(peer("flappy", "10.0.0.5"));
            }
            v
        };
        for ms in (0..=5000).step_by(500) {
            let now = t0 + Duration::from_secs(1) + Duration::from_millis(ms);
            d.observe(with_b(ms % 1000 == 0), now);
            if let Some(committed) = d.poll(now) {
                assert_eq!(ms, 5000, "b commits exactly when its own interval ends");
                let names: Vec<&str> = committed.iter().map(|p| p.pod_name.as_str()).collect();
                assert!(names.contains(&"b"), "{names:?}");
                assert!(
                    names.contains(&"flappy"),
                    "the flapping peer stays committed"
                );
                return;
            }
        }
        panic!("stable change never committed");
    }

    #[test]
    fn commits_happen_at_most_once_per_interval() {
        let t0 = Instant::now();
        let mut d = Debouncer::new(D);
        d.observe(vec![], t0);
        d.poll(t0);
        d.observe(vec![peer("a", "10.0.0.1")], t0);
        d.observe(
            vec![peer("a", "10.0.0.1"), peer("b", "10.0.0.2")],
            t0 + Duration::from_secs(1),
        );
        assert_eq!(d.poll(t0 + D).unwrap().len(), 1, "only a is due");
        assert_eq!(
            d.deadline(),
            Some(t0 + D * 2),
            "b waits for the next window"
        );
        assert_eq!(d.poll(t0 + D + Duration::from_secs(1)), None);
        assert_eq!(d.poll(t0 + D * 2).unwrap().len(), 2);
    }

    #[test]
    fn address_change_is_debounced_and_restarts_when_it_moves_again() {
        let t0 = Instant::now();
        let mut d = Debouncer::new(D);
        d.observe(vec![peer("a", "10.0.0.1")], t0);
        d.poll(t0);
        d.observe(vec![peer("a", "10.0.0.2")], t0 + Duration::from_secs(1));
        d.observe(vec![peer("a", "10.0.0.3")], t0 + Duration::from_secs(4));
        assert_eq!(d.poll(t0 + Duration::from_secs(6)), None);
        let committed = d.poll(t0 + Duration::from_secs(9)).unwrap();
        assert_eq!(idents(&committed), ["apps/a@10.0.0.3"]);
    }
}
