//! Bounded degradation reasons shared by producers and the management listener.

use std::collections::BTreeSet;
use std::sync::Arc;

use prometheus_client::encoding::EncodeLabelValue;
use tokio::sync::watch;

use crate::metrics::Metrics;

/// Why a serving cache is degraded; also used as a bounded metric label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, EncodeLabelValue)]
pub enum Reason {
    BackendEmpty,
    BackendDiscovery,
    FrontendDiscovery,
    TemplateRejected,
    RenderFailed,
    CompileFailed,
    ActivationRejected,
    AdminUnavailable,
    PersistenceFailed,
    CleanupFailed,
    RetentionExceeded,
    MemguardFailed,
}

impl Reason {
    /// Operator-facing text for `/readyz`.
    pub fn describe(self) -> &'static str {
        match self {
            Self::BackendEmpty => "backend has no endpoints",
            Self::BackendDiscovery => "backend discovery failing",
            Self::FrontendDiscovery => "frontend discovery failing",
            Self::TemplateRejected => "template revision rejected",
            Self::RenderFailed => "render failed",
            Self::CompileFailed => "VCL compilation failed",
            Self::ActivationRejected => "VCL activation rejected",
            Self::AdminUnavailable => "admin interface unavailable",
            Self::PersistenceFailed => "active VCL not persisted",
            Self::CleanupFailed => "VCL cleanup failed",
            Self::RetentionExceeded => "VCL retention bound exceeded; loads paused",
            Self::MemguardFailed => "memory guard failing",
        }
    }
}

/// Shared set of current degradation reasons; transitions to set are counted.
#[derive(Debug, Clone)]
pub struct Health {
    reasons: Arc<watch::Sender<BTreeSet<Reason>>>,
    metrics: Arc<Metrics>,
}

impl Health {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            reasons: Arc::new(watch::Sender::new(BTreeSet::new())),
            metrics,
        }
    }

    pub fn set(&self, reason: Reason, on: bool) {
        let changed = self.reasons.send_if_modified(|set| {
            if on {
                set.insert(reason)
            } else {
                set.remove(&reason)
            }
        });
        if changed && on {
            tracing::warn!(reason = reason.describe(), "degraded");
            self.metrics.degradation_event(reason);
        }
    }

    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    pub fn current(&self) -> BTreeSet<Reason> {
        self.reasons.borrow().clone()
    }
}
