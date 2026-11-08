//! Prometheus metrics with bounded label sets only.

use std::sync::atomic::AtomicU64;

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue, LabelValueEncoder};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

use crate::config::Role;
use crate::status::Reason;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EncodeLabelValue)]
pub enum RoleLabel {
    Frontend,
    Backend,
}

impl From<Role> for RoleLabel {
    fn from(role: Role) -> Self {
        match role {
            Role::Frontend => Self::Frontend,
            Role::Backend => Self::Backend,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StatusClass {
    Success,
    ClientError,
    ServerError,
}

impl StatusClass {
    pub fn of(status: u16) -> Self {
        match status {
            ..400 => Self::Success,
            400..500 => Self::ClientError,
            _ => Self::ServerError,
        }
    }
}

impl EncodeLabelValue for StatusClass {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        let text = match self {
            Self::Success => "2xx",
            Self::ClientError => "4xx",
            Self::ServerError => "5xx",
        };
        std::fmt::Write::write_str(encoder, text)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EncodeLabelValue)]
pub enum DeliveryResult {
    Delivered,
    Rejected,
    Retried,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InvalidationResult {
    Delivered,
    Exhausted,
}

impl EncodeLabelValue for InvalidationResult {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        let text = match self {
            Self::Delivered => "delivered",
            Self::Exhausted => "exhausted",
        };
        std::fmt::Write::write_str(encoder, text)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub struct InvalidationLabels {
    pub result: InvalidationResult,
}

type LatencyFamily = Family<InvalidationLabels, Histogram, fn() -> Histogram>;

/// 10ms doubling to about 40s, covering the delivery deadline range.
fn latency_histogram() -> Histogram {
    Histogram::new(exponential_buckets(0.01, 2.0, 13))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub struct RoleLabels {
    pub role: RoleLabel,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub struct ReasonLabels {
    pub reason: Reason,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub struct StatusLabels {
    pub status_class: StatusClass,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
pub struct ResultLabels {
    pub result: DeliveryResult,
}

type FloatGauge = Gauge<f64, AtomicU64>;

/// Every exported metric; the registry is read-only after construction.
#[derive(Debug)]
pub struct Metrics {
    pub registry: Registry,
    pub reload_attempts: Counter,
    pub reload_successes: Counter,
    pub reload_failures: Family<ReasonLabels, Counter>,
    pub reload_skips: Counter,
    pub active_vcl_age: FloatGauge,
    pub discovery_events: Family<RoleLabels, Counter>,
    pub discovery_errors: Family<RoleLabels, Counter>,
    pub discovery_endpoints: Family<RoleLabels, Gauge>,
    pub discovery_last_success: Family<RoleLabels, FloatGauge>,
    pub empty_endpoints: Family<RoleLabels, Counter>,
    pub signaller_requests: Family<StatusLabels, Counter>,
    pub signaller_queue_depth: Gauge,
    pub signaller_deliveries: Family<ResultLabels, Counter>,
    pub signaller_exhausted: Counter,
    pub invalidation_latency: LatencyFamily,
    pub invalidation_peers_lagging: Gauge,
    pub ready: Gauge,
    pub degraded: Gauge,
    pub degradation_events: Family<ReasonLabels, Counter>,
    pub memguard_bans: Counter,
    pub memguard_failures: Counter,
    pub memguard_active: Gauge,
    pub memory_used: Gauge,
    pub memory_usage: Gauge,
    pub memory_limit: Gauge,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let mut m = Self {
            registry: Registry::with_prefix("stalewise"),
            reload_attempts: Counter::default(),
            reload_successes: Counter::default(),
            reload_failures: Family::default(),
            reload_skips: Counter::default(),
            active_vcl_age: FloatGauge::default(),
            discovery_events: Family::default(),
            discovery_errors: Family::default(),
            discovery_endpoints: Family::default(),
            discovery_last_success: Family::default(),
            empty_endpoints: Family::default(),
            signaller_requests: Family::default(),
            signaller_queue_depth: Gauge::default(),
            signaller_deliveries: Family::default(),
            signaller_exhausted: Counter::default(),
            invalidation_latency: Family::new_with_constructor(latency_histogram),
            invalidation_peers_lagging: Gauge::default(),
            ready: Gauge::default(),
            degraded: Gauge::default(),
            degradation_events: Family::default(),
            memguard_bans: Counter::default(),
            memguard_failures: Counter::default(),
            memguard_active: Gauge::default(),
            memory_used: Gauge::default(),
            memory_usage: Gauge::default(),
            memory_limit: Gauge::default(),
        };
        let r = &mut m.registry;
        r.register(
            "reload_attempts",
            "VCL load attempts",
            m.reload_attempts.clone(),
        );
        r.register(
            "reload_successes",
            "Confirmed VCL activations",
            m.reload_successes.clone(),
        );
        r.register(
            "reload_failures",
            "Failed reloads by reason",
            m.reload_failures.clone(),
        );
        r.register(
            "reload_skips",
            "Renders identical to the active VCL",
            m.reload_skips.clone(),
        );
        r.register(
            "active_vcl_age_seconds",
            "Seconds since the active VCL was confirmed",
            m.active_vcl_age.clone(),
        );
        r.register(
            "discovery_events",
            "EndpointSlice watch events",
            m.discovery_events.clone(),
        );
        r.register(
            "discovery_errors",
            "EndpointSlice watch errors",
            m.discovery_errors.clone(),
        );
        r.register(
            "discovery_endpoints",
            "Current merged endpoints",
            m.discovery_endpoints.clone(),
        );
        r.register(
            "discovery_last_success_timestamp_seconds",
            "Unix time of the last complete synchronisation",
            m.discovery_last_success.clone(),
        );
        r.register(
            "empty_endpoints",
            "Snapshots with zero endpoints",
            m.empty_endpoints.clone(),
        );
        r.register(
            "signaller_requests",
            "Signaller responses by status class",
            m.signaller_requests.clone(),
        );
        r.register(
            "signaller_queue_depth",
            "Queued invalidation deliveries",
            m.signaller_queue_depth.clone(),
        );
        r.register(
            "signaller_deliveries",
            "Delivery attempts by result",
            m.signaller_deliveries.clone(),
        );
        r.register(
            "signaller_exhausted_deliveries",
            "Deliveries abandoned at their deadline",
            m.signaller_exhausted.clone(),
        );
        r.register(
            "invalidation_latency_seconds",
            "Admission to final target completion, by result",
            m.invalidation_latency.clone(),
        );
        r.register(
            "invalidation_peers_lagging",
            "Distinct peers with a retained pending or exhausted delivery",
            m.invalidation_peers_lagging.clone(),
        );
        r.register("ready", "1 when /readyz returns 200", m.ready.clone());
        r.register(
            "degraded",
            "1 when any degradation reason is set",
            m.degraded.clone(),
        );
        r.register(
            "degradation_events",
            "Transitions into a degraded reason",
            m.degradation_events.clone(),
        );
        r.register(
            "memguard_bans",
            "Bans the memory guard issued on the oldest objects",
            m.memguard_bans.clone(),
        );
        r.register(
            "memguard_failures",
            "Memory guard steps that failed to read memory or ban",
            m.memguard_failures.clone(),
        );
        r.register(
            "memguard_active",
            "1 while memory is above high water and not yet below low water",
            m.memguard_active.clone(),
        );
        r.register(
            "memory_used_bytes",
            "Container working set: cgroup usage minus inactive page cache",
            m.memory_used.clone(),
        );
        r.register(
            "memory_cgroup_usage_bytes",
            "Raw cgroup memory usage, reclaimable page cache included",
            m.memory_usage.clone(),
        );
        r.register(
            "memory_limit_bytes",
            "Container memory limit; 0 when unlimited",
            m.memory_limit.clone(),
        );
        m
    }

    pub fn degradation_event(&self, reason: Reason) {
        self.degradation_events
            .get_or_create(&ReasonLabels { reason })
            .inc();
    }

    pub fn reload_failure(&self, reason: Reason) {
        self.reload_failures
            .get_or_create(&ReasonLabels { reason })
            .inc();
    }

    pub fn role(role: Role) -> RoleLabels {
        RoleLabels { role: role.into() }
    }

    /// Prometheus text exposition.
    pub fn encode(&self) -> Result<String, std::fmt::Error> {
        let mut out = String::new();
        prometheus_client::encoding::text::encode(&mut out, &self.registry)?;
        Ok(out)
    }
}
