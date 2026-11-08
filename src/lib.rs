//! stalewise supervises Varnish as a Kubernetes-aware HTTP cache.

pub mod app;
pub mod attach;
pub mod capacity;
pub mod config;
pub mod controller;
pub mod discovery;
pub mod endpoint;
pub mod entropy;
pub mod http;
pub mod ledger;
pub mod membership;
pub mod metrics;
pub mod mgmt;
pub mod policy;
pub mod reload;
pub mod render;
pub mod signaller;
pub mod status;
pub mod varnish;

#[cfg(test)]
pub(crate) mod testutil;
