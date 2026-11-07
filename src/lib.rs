//! stalewise supervises Varnish as a Kubernetes-aware HTTP cache.

pub mod app;
pub mod config;
pub mod discovery;
pub mod endpoint;
pub mod http;
pub mod metrics;
pub mod mgmt;
pub mod reload;
pub mod render;
pub mod signaller;
pub mod status;
pub mod varnish;

#[cfg(test)]
pub(crate) mod testutil;
