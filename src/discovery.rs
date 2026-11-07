//! EndpointSlice watchers and the pure merge that turns slices into sorted endpoints.

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures_util::StreamExt;
use k8s_openapi::api::discovery::v1::{Endpoint as SliceEndpoint, EndpointSlice};
use kube::Api;
use kube::runtime::watcher::{self, Event};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::{Role, RoleConfig};
use crate::endpoint::{Endpoint, Host};
use crate::membership::{self, Debouncer, Peer};
use crate::metrics::Metrics;
use crate::status::{Health, Reason};

const SERVICE_LABEL: &str = "kubernetes.io/service-name";
/// Server-side watch timeout; kube's client read timeout must exceed it.
const WATCH_TIMEOUT_SECS: u32 = 290;
const BACKOFF_BASE_MS: u64 = 500;
const BACKOFF_CAP_MS: u64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DiscoveryError {
    #[error("EndpointSlice watch failed: {0}")]
    Watch(String),
    #[error("EndpointSlice port {0} is out of range")]
    InvalidPort(i32),
}

/// One complete, merged snapshot of a role's endpoints.
#[derive(Debug, Clone)]
pub struct Discovery {
    /// Frontend endpoints follow `peers` order; origin endpoints are sorted numerically.
    pub endpoints: Arc<[Endpoint]>,
    /// Stable cache-peer membership; empty for the origin role.
    pub peers: Arc<[Peer]>,
    pub last_success: SystemTime,
    pub error: Option<DiscoveryError>,
}

/// Merges every slice into one endpoint per pod, sorted by numeric address then port.
///
/// A dual-stack pod appears once per family; its lowest address (IPv4 first) represents it.
pub fn merge(slices: &[EndpointSlice], port: &str) -> Result<Vec<Endpoint>, DiscoveryError> {
    let mut by_pod: BTreeMap<String, (IpAddr, u16)> = BTreeMap::new();
    for slice in slices {
        let Some(number) = slice_port(slice, port)? else {
            continue;
        };
        let family_v6 = match slice.address_type.as_str() {
            "IPv4" => false,
            "IPv6" => true,
            _ => continue,
        };
        for endpoint in slice.endpoints.iter().filter(|e| usable(e)) {
            for ip in addresses(endpoint, family_v6) {
                let key = pod_key(endpoint).unwrap_or_else(|| ip.to_string());
                let candidate = (ip, number);
                by_pod
                    .entry(key)
                    .and_modify(|kept| *kept = (*kept).min(candidate))
                    .or_insert(candidate);
            }
        }
    }
    let set: BTreeSet<(IpAddr, u16)> = by_pod.into_values().collect();
    Ok(set
        .into_iter()
        .map(|(ip, port)| Endpoint::new(Host::Ip(ip), port))
        .collect())
}

fn usable(endpoint: &SliceEndpoint) -> bool {
    let conditions = endpoint.conditions.as_ref();
    conditions.and_then(|c| c.ready) != Some(false)
        && conditions.and_then(|c| c.terminating) != Some(true)
}

fn addresses(endpoint: &SliceEndpoint, family_v6: bool) -> impl Iterator<Item = IpAddr> + '_ {
    endpoint
        .addresses
        .iter()
        .filter_map(move |address| match address.parse::<IpAddr>() {
            Ok(ip) if ip.is_ipv6() == family_v6 => Some(ip),
            _ => {
                tracing::warn!(address = %address, "ignoring invalid EndpointSlice address");
                None
            }
        })
}

/// The owning pod, so one pod's per-family endpoints collapse to a single peer.
fn pod_key(endpoint: &SliceEndpoint) -> Option<String> {
    let target = endpoint.target_ref.as_ref()?;
    if let Some(uid) = &target.uid {
        return Some(uid.clone());
    }
    let name = target.name.as_ref()?;
    Some(format!(
        "{}/{name}",
        target.namespace.as_deref().unwrap_or_default()
    ))
}

/// The named TCP port of a slice, if it exposes one.
pub(crate) fn slice_port(slice: &EndpointSlice, name: &str) -> Result<Option<u16>, DiscoveryError> {
    let found = slice.ports.iter().flatten().find(|p| {
        p.name.as_deref() == Some(name) && p.protocol.as_deref().unwrap_or("TCP") == "TCP"
    });
    let Some(number) = found.and_then(|p| p.port) else {
        return Ok(None);
    };
    u16::try_from(number)
        .ok()
        .filter(|p| *p != 0)
        .map(Some)
        .ok_or(DiscoveryError::InvalidPort(number))
}

/// Slice store; a relist only replaces the live set once it completes.
#[derive(Debug, Default)]
pub struct SliceStore {
    live: BTreeMap<String, EndpointSlice>,
    pending: Option<BTreeMap<String, EndpointSlice>>,
    synced: bool,
}

impl SliceStore {
    /// Applies one watch event; returns true when a complete snapshot should be published.
    pub fn apply(&mut self, event: Event<EndpointSlice>) -> bool {
        match event {
            Event::Init => {
                self.pending = Some(BTreeMap::new());
                false
            }
            Event::InitApply(slice) => {
                if let Some(pending) = self.pending.as_mut() {
                    pending.insert(key(&slice), slice);
                }
                false
            }
            Event::InitDone => {
                self.live = self.pending.take().unwrap_or_default();
                self.synced = true;
                true
            }
            Event::Apply(slice) => {
                self.live.insert(key(&slice), slice);
                self.synced
            }
            Event::Delete(slice) => {
                self.live.remove(&key(&slice));
                self.synced
            }
        }
    }

    pub fn slices(&self) -> Vec<EndpointSlice> {
        self.live.values().cloned().collect()
    }
}

fn key(slice: &EndpointSlice) -> String {
    slice.metadata.name.clone().unwrap_or_default()
}

/// Exponential backoff with full jitter between 0.5s and 30s.
#[derive(Debug, Default)]
pub struct Backoff {
    attempt: u32,
}

impl Backoff {
    pub fn next_delay(&mut self) -> Duration {
        let ceiling = BACKOFF_BASE_MS
            .saturating_mul(2u64.saturating_pow(self.attempt))
            .min(BACKOFF_CAP_MS);
        self.attempt = self.attempt.saturating_add(1);
        Duration::from_millis(rand::random_range(BACKOFF_BASE_MS..=ceiling))
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

/// Watches one Service's EndpointSlices until `stop`; errors retain the last snapshot and a
/// finished stream is reopened, so the task returns only on `stop`. Frontend membership is debounced.
pub async fn watch(
    client: kube::Client,
    role: RoleConfig,
    debounce: Duration,
    out: watch::Sender<Option<Discovery>>,
    metrics: Arc<Metrics>,
    health: Health,
    stop: CancellationToken,
) {
    let api: Api<EndpointSlice> = Api::namespaced(client, &role.namespace);
    let selector = format!("{SERVICE_LABEL}={}", role.service);
    let config = watcher::Config::default()
        .labels(&selector)
        .timeout(WATCH_TIMEOUT_SECS);
    let mut stream = watcher::watcher(api.clone(), config.clone()).boxed();
    let mut store = SliceStore::default();
    let mut backoff = Backoff::default();
    let mut members = (role.role == Role::Frontend).then(|| Debouncer::new(debounce));
    let sink = Sink {
        role: &role,
        out: &out,
        metrics: &metrics,
        health: &health,
    };
    let labels = Metrics::role(role.role);
    loop {
        let deadline = members.as_ref().and_then(Debouncer::deadline);
        let item = tokio::select! {
            () = stop.cancelled() => return,
            () = sleep_until(deadline) => {
                if let Some(peers) = members.as_mut().and_then(|d| d.poll(Instant::now())) {
                    sink.send_peers(peers);
                }
                continue;
            }
            item = stream.next() => item,
        };
        match item {
            Some(Ok(event)) => {
                metrics.discovery_events.get_or_create(&labels).inc();
                backoff.reset();
                if store.apply(event) {
                    sink.publish(&store, members.as_mut());
                }
            }
            Some(Err(err)) => {
                metrics.discovery_errors.get_or_create(&labels).inc();
                sink.report_error(DiscoveryError::Watch(err.to_string()));
                tokio::select! {
                    () = stop.cancelled() => return,
                    () = tokio::time::sleep(backoff.next_delay()) => {}
                }
            }
            None => {
                tracing::warn!(
                    role = role.role.as_str(),
                    "EndpointSlice watch ended; restarting"
                );
                tokio::select! {
                    () = stop.cancelled() => return,
                    () = tokio::time::sleep(backoff.next_delay()) => {}
                }
                stream = watcher::watcher(api.clone(), config.clone()).boxed();
            }
        }
    }
}

async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

fn discovery_reason(role: Role) -> Reason {
    match role {
        Role::Frontend => Reason::FrontendDiscovery,
        Role::Backend => Reason::BackendDiscovery,
    }
}

/// Where one watcher publishes snapshots, errors and metrics.
struct Sink<'a> {
    role: &'a RoleConfig,
    out: &'a watch::Sender<Option<Discovery>>,
    metrics: &'a Metrics,
    health: &'a Health,
}

impl Sink<'_> {
    fn report_error(&self, err: DiscoveryError) {
        let role = self.role.role;
        tracing::warn!(role = role.as_str(), error = %err, "discovery error; keeping last snapshot");
        self.health.set(discovery_reason(role), true);
        self.out.send_if_modified(|current| match current {
            Some(snapshot) => {
                snapshot.error = Some(err);
                true
            }
            None => false,
        });
    }

    /// Merges a synchronised store; frontend membership passes through the debouncer first.
    fn publish(&self, store: &SliceStore, members: Option<&mut Debouncer>) {
        let slices = store.slices();
        let Some(members) = members else {
            return match merge(&slices, &self.role.port_name) {
                Ok(endpoints) => self.send(endpoints, Vec::new()),
                Err(err) => self.report_error(err),
            };
        };
        let peers = match membership::merge_peers(&slices, &self.role.port_name) {
            Ok(peers) => peers,
            Err(err) => return self.report_error(err),
        };
        let now = Instant::now();
        members.observe(peers, now);
        match members.poll(now) {
            Some(peers) => self.send_peers(peers),
            None => self.mark_synced(),
        }
    }

    fn send_peers(&self, peers: Vec<Peer>) {
        let endpoints = peers.iter().map(Peer::endpoint).collect();
        self.send(endpoints, peers);
    }

    /// A successful sync whose membership change is still settling keeps the committed snapshot.
    fn mark_synced(&self) {
        let now = self.record_success();
        self.out.send_if_modified(|current| match current {
            Some(snapshot) => {
                snapshot.error = None;
                snapshot.last_success = now;
                true
            }
            None => false,
        });
    }

    fn record_success(&self) -> SystemTime {
        let now = SystemTime::now();
        let unix = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        self.metrics
            .discovery_last_success
            .get_or_create(&Metrics::role(self.role.role))
            .set(unix.as_secs_f64());
        self.health.set(discovery_reason(self.role.role), false);
        now
    }

    fn send(&self, endpoints: Vec<Endpoint>, peers: Vec<Peer>) {
        let role = self.role;
        let labels = Metrics::role(role.role);
        let now = self.record_success();
        self.metrics
            .discovery_endpoints
            .get_or_create(&labels)
            .set(i64::try_from(endpoints.len()).unwrap_or(i64::MAX));
        if endpoints.is_empty() {
            self.metrics.empty_endpoints.get_or_create(&labels).inc();
            tracing::warn!(role = role.role.as_str(), service = %role.service, "service has no ready endpoints");
        }
        self.out.send_replace(Some(Discovery {
            endpoints: endpoints.into(),
            peers: peers.into(),
            last_success: now,
            error: None,
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn slice(
        name: &str,
        family: &str,
        ports: serde_json::Value,
        endpoints: serde_json::Value,
    ) -> EndpointSlice {
        serde_json::from_value(json!({
            "apiVersion": "discovery.k8s.io/v1",
            "kind": "EndpointSlice",
            "metadata": {"name": name, "labels": {SERVICE_LABEL: "catalog"}},
            "addressType": family,
            "ports": ports,
            "endpoints": endpoints,
        }))
        .unwrap()
    }

    fn http(port: i32) -> serde_json::Value {
        json!([{"name": "http", "port": port, "protocol": "TCP"}, {"name": "metrics", "port": 9000}])
    }

    fn names(endpoints: &[Endpoint]) -> Vec<String> {
        endpoints.iter().map(Endpoint::authority).collect()
    }

    #[test]
    fn honours_ready_and_terminating_conditions() {
        let s = slice(
            "a",
            "IPv4",
            http(8080),
            json!([
                {"addresses": ["10.0.0.1"], "conditions": {"ready": true}},
                {"addresses": ["10.0.0.2"], "conditions": {"ready": false}},
                {"addresses": ["10.0.0.3"], "conditions": {"ready": true, "terminating": true}},
                {"addresses": ["10.0.0.4"]},
                {"addresses": ["10.0.0.5"], "conditions": {"serving": true, "terminating": false}},
            ]),
        );
        assert_eq!(
            names(&merge(&[s], "http").unwrap()),
            ["10.0.0.1:8080", "10.0.0.4:8080", "10.0.0.5:8080"]
        );
    }

    #[test]
    fn matches_named_tcp_ports_only() {
        let udp = slice(
            "u",
            "IPv4",
            json!([{"name": "http", "port": 53, "protocol": "UDP"}]),
            json!([{"addresses": ["10.0.0.9"]}]),
        );
        let other = slice(
            "o",
            "IPv4",
            json!([{"name": "grpc", "port": 9090}]),
            json!([{"addresses": ["10.0.0.8"]}]),
        );
        let ok = slice("k", "IPv4", http(80), json!([{"addresses": ["10.0.0.7"]}]));
        assert_eq!(
            names(&merge(&[udp, other, ok], "http").unwrap()),
            ["10.0.0.7:80"]
        );
        let bad = slice(
            "b",
            "IPv4",
            http(70000),
            json!([{"addresses": ["10.0.0.7"]}]),
        );
        assert_eq!(
            merge(&[bad], "http"),
            Err(DiscoveryError::InvalidPort(70000))
        );
    }

    #[test]
    fn dedupes_across_slices_and_sorts_numerically_with_ipv6() {
        let a = slice(
            "a",
            "IPv4",
            http(80),
            json!([{"addresses": ["10.0.0.10"]}, {"addresses": ["10.0.0.9"]}]),
        );
        let b = slice("b", "IPv4", http(80), json!([{"addresses": ["10.0.0.9"]}]));
        let c = slice(
            "c",
            "IPv6",
            http(80),
            json!([{"addresses": ["fd00:0::2"]}, {"addresses": ["10.0.0.1"]}]),
        );
        let d = slice(
            "d",
            "FQDN",
            http(80),
            json!([{"addresses": ["example.com"]}]),
        );
        let merged = merge(&[c, b, d, a], "http").unwrap();
        assert_eq!(
            names(&merged),
            ["10.0.0.9:80", "10.0.0.10:80", "[fd00::2]:80"]
        );
        assert_eq!(merged[2].name, "ip_fd00__2_80_v6");
    }

    #[test]
    fn dual_stack_pod_is_one_peer_and_unowned_addresses_stay_distinct() {
        let pod = |name: &str| json!({"kind": "Pod", "namespace": "apps", "name": name});
        let v4 = slice(
            "v4",
            "IPv4",
            http(80),
            json!([
                {"addresses": ["10.0.0.2"], "targetRef": pod("a")},
                {"addresses": ["10.0.0.3"], "targetRef": pod("b")},
                {"addresses": ["10.0.0.9"]},
            ]),
        );
        let v6 = slice(
            "v6",
            "IPv6",
            http(80),
            json!([
                {"addresses": ["fd00::2"], "targetRef": pod("a")},
                {"addresses": ["fd00::3"], "targetRef": pod("b")},
                {"addresses": ["fd00::9"]},
            ]),
        );
        assert_eq!(
            names(&merge(&[v6, v4], "http").unwrap()),
            ["10.0.0.2:80", "10.0.0.3:80", "10.0.0.9:80", "[fd00::9]:80"]
        );
    }

    #[test]
    fn relist_replaces_store_only_when_complete() {
        let mut store = SliceStore::default();
        let a = slice("a", "IPv4", http(80), json!([{"addresses": ["10.0.0.1"]}]));
        let b = slice("b", "IPv4", http(80), json!([{"addresses": ["10.0.0.2"]}]));
        assert!(
            !store.apply(Event::Apply(a.clone())),
            "no publish before first sync"
        );
        assert!(!store.apply(Event::Init));
        assert!(!store.apply(Event::InitApply(a.clone())));
        assert!(store.apply(Event::InitDone));
        assert!(store.apply(Event::Apply(b.clone())));
        assert_eq!(store.slices().len(), 2);
        assert!(!store.apply(Event::Init));
        assert!(!store.apply(Event::InitApply(b)));
        assert_eq!(
            store.slices().len(),
            2,
            "partial relist keeps healthy state"
        );
        assert!(store.apply(Event::InitDone));
        assert_eq!(
            names(&merge(&store.slices(), "http").unwrap()),
            ["10.0.0.2:80"]
        );
        assert!(store.apply(Event::Delete(a)));
    }

    #[test]
    fn backoff_is_bounded_and_resets() {
        let mut backoff = Backoff::default();
        for _ in 0..40 {
            let delay = backoff.next_delay();
            assert!(
                (BACKOFF_BASE_MS..=BACKOFF_CAP_MS).contains(&(delay.as_millis() as u64)),
                "{delay:?}"
            );
        }
        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_millis(BACKOFF_BASE_MS));
    }

    #[test]
    fn publish_retains_snapshot_on_error_and_never_fabricates_success() {
        let metrics = Arc::new(Metrics::new());
        let health = Health::new(Arc::clone(&metrics));
        let role = RoleConfig {
            role: Role::Backend,
            namespace: "apps".into(),
            service: "catalog".into(),
            port_name: "http".into(),
        };
        let (tx, rx) = watch::channel(None);
        let sink = Sink {
            role: &role,
            out: &tx,
            metrics: &metrics,
            health: &health,
        };
        sink.report_error(DiscoveryError::Watch("down".into()));
        assert!(rx.borrow().is_none());
        let mut store = SliceStore::default();
        store.apply(Event::Init);
        store.apply(Event::InitApply(slice(
            "a",
            "IPv4",
            http(80),
            json!([{"addresses": ["10.0.0.1"]}]),
        )));
        store.apply(Event::InitDone);
        sink.publish(&store, None);
        assert!(health.current().is_empty());
        sink.report_error(DiscoveryError::Watch("down".into()));
        let snap = rx.borrow().clone().unwrap();
        assert_eq!(snap.endpoints.len(), 1);
        assert!(snap.error.is_some());
        assert!(health.current().contains(&Reason::BackendDiscovery));
    }

    #[test]
    fn frontend_publishes_debounced_peers_and_a_settling_sync_clears_errors() {
        let metrics = Arc::new(Metrics::new());
        let health = Health::new(Arc::clone(&metrics));
        let role = RoleConfig {
            role: Role::Frontend,
            namespace: "apps".into(),
            service: "cache-peers".into(),
            port_name: "http".into(),
        };
        let (tx, rx) = watch::channel(None);
        let sink = Sink {
            role: &role,
            out: &tx,
            metrics: &metrics,
            health: &health,
        };
        let pod = |name: &str, ip: &str| {
            json!({"addresses": [ip], "conditions": {"ready": true},
                   "targetRef": {"kind": "Pod", "namespace": "apps", "name": name}})
        };
        let mut members = Debouncer::new(Duration::from_secs(60));
        let mut store = SliceStore::default();
        store.apply(Event::Init);
        store.apply(Event::InitApply(slice(
            "a",
            "IPv4",
            http(8080),
            json!([pod("b", "10.0.0.1"), pod("a", "10.0.0.2")]),
        )));
        store.apply(Event::InitDone);
        sink.publish(&store, Some(&mut members));
        let snap = rx.borrow().clone().unwrap();
        let names: Vec<&str> = snap.peers.iter().map(|p| p.pod_name.as_str()).collect();
        assert_eq!(names, ["a", "b"]);
        assert_eq!(
            names_of(&snap.endpoints),
            ["10.0.0.2:8080", "10.0.0.1:8080"]
        );
        sink.report_error(DiscoveryError::Watch("down".into()));
        store.apply(Event::Apply(slice(
            "a",
            "IPv4",
            http(8080),
            json!([pod("a", "10.0.0.2")]),
        )));
        sink.publish(&store, Some(&mut members));
        let snap = rx.borrow().clone().unwrap();
        assert_eq!(snap.peers.len(), 2, "the removal is still settling");
        assert!(snap.error.is_none());
    }

    fn names_of(endpoints: &[Endpoint]) -> Vec<String> {
        endpoints.iter().map(Endpoint::authority).collect()
    }
}
