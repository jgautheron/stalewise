//! Liveness, readiness and metrics; reads state but never changes configuration.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::http::{self, Body, Limits};
use crate::metrics::Metrics;
use crate::reload::Active;
use crate::status::Health;
use crate::varnish::process::ProcessState;

pub const PROBE_PATH: &str = "/__stalewise_probe";
const PROBE_INTERVAL: Duration = Duration::from_secs(2);
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const PROBE_FRESHNESS: Duration = Duration::from_secs(5);
const OPENMETRICS: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

#[derive(Debug, Clone)]
pub struct ManagementConfig {
    pub listen: SocketAddr,
    /// Kubelet probes only; its own connection slots keep scrapers from starving liveness.
    pub probe_listen: SocketAddr,
}

/// Everything readiness and metrics observe.
#[derive(Debug, Clone)]
pub struct StatusInputs {
    pub active: watch::Receiver<Option<Active>>,
    pub process: watch::Receiver<ProcessState>,
    pub probe: watch::Receiver<Option<Instant>>,
    pub health: Health,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readiness {
    Ready { degraded: Vec<&'static str> },
    NotReady { reason: &'static str },
}

/// 503 only without an active VCL or a fresh local probe; degradation never removes traffic.
pub fn readiness(inputs: &StatusInputs) -> Readiness {
    if *inputs.process.borrow() != ProcessState::Running {
        return Readiness::NotReady {
            reason: "varnishd is not running",
        };
    }
    if inputs.active.borrow().is_none() {
        return Readiness::NotReady {
            reason: "no active VCL",
        };
    }
    let fresh = inputs
        .probe
        .borrow()
        .is_some_and(|at| at.elapsed() <= PROBE_FRESHNESS);
    if !fresh {
        return Readiness::NotReady {
            reason: "local frontend probe failing",
        };
    }
    let degraded = inputs
        .health
        .current()
        .into_iter()
        .map(|r| r.describe())
        .collect();
    Readiness::Ready { degraded }
}

/// The loopback address the probe dials for a given frontend bind.
pub fn probe_target(listen: SocketAddr) -> SocketAddr {
    let ip = match listen.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, listen.port())
}

/// Probes the local frontend every 2s; success is cleared at once when the child stops.
pub async fn probe(
    target: SocketAddr,
    mut process: watch::Receiver<ProcessState>,
    out: watch::Sender<Option<Instant>>,
    stop: CancellationToken,
) {
    loop {
        let running = *process.borrow_and_update() == ProcessState::Running;
        let ok = running && probe_once(target).await;
        out.send_replace(ok.then(Instant::now));
        tokio::select! {
            () = stop.cancelled() => return,
            _ = process.changed() => {}
            () = tokio::time::sleep(PROBE_INTERVAL) => {}
        }
    }
}

async fn probe_once(target: SocketAddr) -> bool {
    let mut request = Request::new(Full::new(Bytes::new()));
    *request.uri_mut() = hyper::Uri::from_static(PROBE_PATH);
    request
        .headers_mut()
        .insert(header::HOST, HeaderValue::from_static("localhost"));
    matches!(
        http::send(target, request, PROBE_TIMEOUT).await,
        Ok(StatusCode::OK)
    )
}

/// Which endpoints a listener exposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Surface {
    Management,
    Probe,
}

/// Serves /healthz, /readyz and /metrics on the management port and /healthz, /readyz on the
/// probe port until `stop`; bind failure is fatal.
pub async fn serve(
    config: ManagementConfig,
    inputs: StatusInputs,
    metrics: Arc<Metrics>,
    stop: CancellationToken,
) -> std::io::Result<()> {
    let management = tokio::net::TcpListener::bind(config.listen).await?;
    let probes = tokio::net::TcpListener::bind(config.probe_listen).await?;
    tracing::info!(addr = %config.listen, probe_addr = %config.probe_listen, "management listening");
    let limits = Limits {
        header_timeout: Duration::from_secs(5),
        connection_timeout: Duration::from_secs(15),
    };
    let handler = |surface: Surface| {
        let inputs = inputs.clone();
        let metrics = Arc::clone(&metrics);
        move |req: Request<Incoming>| {
            let response = respond(surface, &req, &inputs, &metrics);
            async move { response }
        }
    };
    tokio::join!(
        http::serve(
            management,
            limits,
            handler(Surface::Management),
            stop.clone()
        ),
        http::serve(probes, limits, handler(Surface::Probe), stop),
    );
    Ok(())
}

fn respond(
    surface: Surface,
    req: &Request<Incoming>,
    inputs: &StatusInputs,
    metrics: &Metrics,
) -> Response<Body> {
    if req.method() != Method::GET {
        return http::text(StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n");
    }
    match (surface, req.uri().path()) {
        (_, "/healthz") => healthz(inputs),
        (_, "/readyz") => readyz(inputs, metrics),
        (Surface::Management, "/metrics") => scrape(inputs, metrics),
        _ => http::text(StatusCode::NOT_FOUND, "not found\n"),
    }
}

fn healthz(inputs: &StatusInputs) -> Response<Body> {
    match *inputs.process.borrow() {
        ProcessState::Exited => http::text(StatusCode::SERVICE_UNAVAILABLE, "varnishd exited\n"),
        ProcessState::Starting | ProcessState::Running => http::text(StatusCode::OK, "ok\n"),
    }
}

fn readyz(inputs: &StatusInputs, metrics: &Metrics) -> Response<Body> {
    let readiness = readiness(inputs);
    metrics
        .ready
        .set(i64::from(matches!(readiness, Readiness::Ready { .. })));
    match readiness {
        Readiness::NotReady { reason } => http::text(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not ready: {reason}\n"),
        ),
        Readiness::Ready { degraded } => {
            let mut body = String::from("ok\n");
            for reason in degraded {
                body.push_str(&format!("degraded: {reason}\n"));
            }
            http::text(StatusCode::OK, body)
        }
    }
}

fn scrape(inputs: &StatusInputs, metrics: &Metrics) -> Response<Body> {
    let readiness = readiness(inputs);
    metrics
        .ready
        .set(i64::from(matches!(readiness, Readiness::Ready { .. })));
    metrics
        .degraded
        .set(i64::from(!inputs.health.current().is_empty()));
    let age = inputs
        .active
        .borrow()
        .as_ref()
        .map_or(0.0, |a| a.since.elapsed().as_secs_f64());
    metrics.active_vcl_age.set(age);
    match metrics.encode() {
        Ok(text) => {
            let mut response = http::text(StatusCode::OK, text);
            response
                .headers_mut()
                .insert(header::CONTENT_TYPE, HeaderValue::from_static(OPENMETRICS));
            response
        }
        Err(_) => http::text(StatusCode::INTERNAL_SERVER_ERROR, "encoding failed\n"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::Reason;
    use crate::varnish::admin::VclName;

    struct Fixture {
        inputs: StatusInputs,
        active: watch::Sender<Option<Active>>,
        process: watch::Sender<ProcessState>,
        probe: watch::Sender<Option<Instant>>,
    }

    fn fixture() -> Fixture {
        let (active, active_rx) = watch::channel(None);
        let (process, process_rx) = watch::channel(ProcessState::Running);
        let (probe, probe_rx) = watch::channel(Some(Instant::now()));
        let health = Health::new(Arc::new(Metrics::new()));
        Fixture {
            inputs: StatusInputs {
                active: active_rx,
                process: process_rx,
                probe: probe_rx,
                health,
            },
            active,
            process,
            probe,
        }
    }

    fn activate(f: &Fixture) {
        f.active.send_replace(Some(Active {
            name: VclName::generate(1),
            bytes: Arc::from(&b"vcl"[..]),
            since: Instant::now(),
            policies: Arc::from([]),
        }));
    }

    #[test]
    fn not_ready_without_active_vcl() {
        let f = fixture();
        assert_eq!(
            readiness(&f.inputs),
            Readiness::NotReady {
                reason: "no active VCL"
            }
        );
    }

    #[test]
    fn degradation_keeps_ready_and_lists_reasons() {
        let f = fixture();
        activate(&f);
        f.inputs.health.set(Reason::BackendEmpty, true);
        f.inputs.health.set(Reason::FrontendDiscovery, true);
        assert_eq!(
            readiness(&f.inputs),
            Readiness::Ready {
                degraded: vec!["backend has no endpoints", "frontend discovery failing"]
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stale_or_failed_probe_is_not_ready() {
        let f = fixture();
        activate(&f);
        assert!(matches!(readiness(&f.inputs), Readiness::Ready { .. }));
        tokio::time::advance(PROBE_FRESHNESS + Duration::from_millis(1)).await;
        assert!(matches!(readiness(&f.inputs), Readiness::NotReady { .. }));
        f.probe.send_replace(None);
        assert!(matches!(readiness(&f.inputs), Readiness::NotReady { .. }));
    }

    #[test]
    fn child_exit_is_not_ready_even_with_fresh_probe() {
        let f = fixture();
        activate(&f);
        f.process.send_replace(ProcessState::Exited);
        assert_eq!(
            readiness(&f.inputs),
            Readiness::NotReady {
                reason: "varnishd is not running"
            }
        );
    }

    #[test]
    fn probe_targets_loopback_of_matching_family() {
        assert_eq!(
            probe_target("0.0.0.0:8080".parse().unwrap()),
            "127.0.0.1:8080".parse().unwrap()
        );
        assert_eq!(
            probe_target("[::]:8080".parse().unwrap()),
            "[::1]:8080".parse().unwrap()
        );
        assert_eq!(
            probe_target("10.0.0.1:80".parse().unwrap()),
            "10.0.0.1:80".parse().unwrap()
        );
    }

    #[tokio::test]
    async fn endpoints_report_status_and_metrics() {
        let f = fixture();
        activate(&f);
        f.inputs.health.set(Reason::BackendEmpty, true);
        let metrics = Arc::clone(f.inputs.health.metrics());
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let stop = CancellationToken::new();
        let task = tokio::spawn(serve(
            ManagementConfig {
                listen: addr,
                probe_listen: free_port(),
            },
            f.inputs.clone(),
            metrics,
            stop.clone(),
        ));
        let get = |path: &'static str| async move {
            for _ in 0..100 {
                let mut req = Request::new(Full::new(Bytes::new()));
                *req.uri_mut() = hyper::Uri::from_static(path);
                req.headers_mut()
                    .insert(header::HOST, HeaderValue::from_static("x"));
                if let Ok(status) = http::send(addr, req, Duration::from_secs(1)).await {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("management never answered");
        };
        assert_eq!(get("/healthz").await, StatusCode::OK);
        assert_eq!(get("/readyz").await, StatusCode::OK);
        assert_eq!(get("/metrics").await, StatusCode::OK);
        let text = f.inputs.health.metrics().encode().unwrap();
        assert!(text.contains("stalewise_ready 1"));
        assert!(text.contains("stalewise_degraded 1"));
        assert!(text.contains("stalewise_degradation_events_total{reason=\"BackendEmpty\"} 1"));
        f.process.send_replace(ProcessState::Exited);
        assert_eq!(get("/healthz").await, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(get("/readyz").await, StatusCode::SERVICE_UNAVAILABLE);
        stop.cancel();
        task.await.unwrap().unwrap();
    }

    fn free_port() -> SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    }

    async fn healthz_status(addr: SocketAddr) -> Option<StatusCode> {
        let mut req = Request::new(Full::new(Bytes::new()));
        *req.uri_mut() = hyper::Uri::from_static("/healthz");
        req.headers_mut()
            .insert(header::HOST, HeaderValue::from_static("x"));
        http::send(addr, req, Duration::from_millis(500)).await.ok()
    }

    #[tokio::test]
    async fn probe_answers_while_management_is_saturated() {
        let f = fixture();
        activate(&f);
        let metrics = Arc::clone(f.inputs.health.metrics());
        let management = free_port();
        let probe = free_port();
        let stop = CancellationToken::new();
        let task = tokio::spawn(serve(
            ManagementConfig {
                listen: management,
                probe_listen: probe,
            },
            f.inputs.clone(),
            metrics,
            stop.clone(),
        ));
        for _ in 0..100 {
            if healthz_status(probe).await == Some(StatusCode::OK) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut idle = Vec::new();
        for _ in 0..128 {
            idle.push(tokio::net::TcpStream::connect(management).await.unwrap());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(healthz_status(probe).await, Some(StatusCode::OK));
        drop(idle);
        stop.cancel();
        task.await.unwrap().unwrap();
    }
}
