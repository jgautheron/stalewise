//! Prometheus metrics with bounded label sets only.

use std::sync::atomic::AtomicU64;

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue, LabelValueEncoder};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
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
    pub ready: Gauge,
    pub degraded: Gauge,
    pub degradation_events: Family<ReasonLabels, Counter>,
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
            ready: Gauge::default(),
            degraded: Gauge::default(),
            degradation_events: Family::default(),
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
