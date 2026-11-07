//! stalewise supervises Varnish as a Kubernetes-aware HTTP cache.

pub mod config;
pub mod endpoint;
pub mod render;
pub mod varnish;

#[cfg(test)]
pub(crate) mod testutil;
