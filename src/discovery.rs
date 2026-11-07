//! EndpointSlice watchers and the pure merge that turns slices into sorted endpoints.

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures_util::StreamExt;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::Api;
use kube::runtime::watcher::{self, Event};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::config::{Role, RoleConfig};
use crate::endpoint::{Endpoint, Host};
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
    pub endpoints: Arc<[Endpoint]>,
    pub last_success: SystemTime,
    pub error: Option<DiscoveryError>,
}

/// Merges every slice into deduplicated endpoints sorted by numeric address then port.
pub fn merge(slices: &[EndpointSlice], port: &str) -> Result<Vec<Endpoint>, DiscoveryError> {
    let mut set: BTreeSet<(IpAddr, u16)> = BTreeSet::new();
    for slice in slices {
        let Some(number) = slice_port(slice, port)? else {
            continue;
        };
        let family_v6 = match slice.address_type.as_str() {
            "IPv4" => false,
            "IPv6" => true,
            _ => continue,
        };
        let usable = slice.endpoints.iter().filter(|e| {
            let conditions = e.conditions.as_ref();
            conditions.and_then(|c| c.ready) != Some(false)
                && conditions.and_then(|c| c.terminating) != Some(true)
        });
        for address in usable.flat_map(|e| e.addresses.iter()) {
            match address.parse::<IpAddr>() {
                Ok(ip) if ip.is_ipv6() == family_v6 => {
                    set.insert((ip, number));
                }
                _ => tracing::warn!(address = %address, "ignoring invalid EndpointSlice address"),
            }
        }
    }
    Ok(set
        .into_iter()
        .map(|(ip, port)| Endpoint::new(Host::Ip(ip), port))
        .collect())
}

/// The named TCP port of a slice, if it exposes one.
fn slice_port(slice: &EndpointSlice, name: &str) -> Result<Option<u16>, DiscoveryError> {
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
/// finished stream is reopened, so the task returns only on `stop`.
pub async fn watch(
    client: kube::Client,
    role: RoleConfig,
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
    let labels = Metrics::role(role.role);
    loop {
        let item = tokio::select! {
            () = stop.cancelled() => return,
            item = stream.next() => item,
        };
        match item {
            Some(Ok(event)) => {
                metrics.discovery_events.get_or_create(&labels).inc();
                backoff.reset();
                if store.apply(event) {
                    publish(&role, &store, &out, &metrics, &health);
                }
            }
            Some(Err(err)) => {
                metrics.discovery_errors.get_or_create(&labels).inc();
                report_error(&role, &out, &health, DiscoveryError::Watch(err.to_string()));
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

fn discovery_reason(role: Role) -> Reason {
    match role {
        Role::Frontend => Reason::FrontendDiscovery,
        Role::Backend => Reason::BackendDiscovery,
    }
}

fn report_error(
    role: &RoleConfig,
    out: &watch::Sender<Option<Discovery>>,
    health: &Health,
    err: DiscoveryError,
) {
    tracing::warn!(role = role.role.as_str(), error = %err, "discovery error; keeping last snapshot");
    health.set(discovery_reason(role.role), true);
    out.send_if_modified(|current| match current {
        Some(snapshot) => {
            snapshot.error = Some(err);
            true
        }
        None => false,
    });
}

fn publish(
    role: &RoleConfig,
    store: &SliceStore,
    out: &watch::Sender<Option<Discovery>>,
    metrics: &Metrics,
    health: &Health,
) {
    let labels = Metrics::role(role.role);
    let endpoints = match merge(&store.slices(), &role.port_name) {
        Ok(endpoints) => endpoints,
        Err(err) => return report_error(role, out, health, err),
    };
    let now = SystemTime::now();
    let unix = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    metrics
        .discovery_last_success
        .get_or_create(&labels)
        .set(unix.as_secs_f64());
    metrics
        .discovery_endpoints
        .get_or_create(&labels)
        .set(i64::try_from(endpoints.len()).unwrap_or(i64::MAX));
    if endpoints.is_empty() {
        metrics.empty_endpoints.get_or_create(&labels).inc();
        tracing::warn!(role = role.role.as_str(), service = %role.service, "service has no ready endpoints");
    }
    health.set(discovery_reason(role.role), false);
    out.send_replace(Some(Discovery {
        endpoints: endpoints.into(),
        last_success: now,
        error: None,
    }));
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
        report_error(&role, &tx, &health, DiscoveryError::Watch("down".into()));
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
        publish(&role, &store, &tx, &metrics, &health);
        assert!(health.current().is_empty());
        report_error(&role, &tx, &health, DiscoveryError::Watch("down".into()));
        let snap = rx.borrow().clone().unwrap();
        assert_eq!(snap.endpoints.len(), 1);
        assert!(snap.error.is_some());
        assert!(health.current().contains(&Reason::BackendDiscovery));
    }
}
