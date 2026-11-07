//! Authenticated invalidation intake that fans out to every peer through a bounded queue.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{self, HeaderMap, HeaderName, HeaderValue};
use hyper::{Method as HttpMethod, Request, Response, StatusCode};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::{Method, SignalConfig};
use crate::discovery::Discovery;
use crate::endpoint::Authority;
use crate::http::{self, Body, Limits};
use crate::metrics::{DeliveryResult, Metrics, ResultLabels, StatusClass, StatusLabels};

const TOKEN_MIN: usize = 32;
const TOKEN_MAX: usize = 256;
const TAG_HEADER: &str = "x-stalewise-tag";
const HOST_HEADER: &str = "x-stalewise-host";
const MAX_TAG: usize = 128;
const RETRY_BASE_MS: u64 = 100;
const RETRY_CAP_MS: u64 = 1000;
const HOP_BY_HOP: [&str; 9] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

#[derive(Debug, thiserror::Error)]
pub enum SignallerError {
    #[error("signaller token file is unreadable: {0}")]
    TokenIo(std::io::Error),
    #[error("signaller token must be {TOKEN_MIN}-{TOKEN_MAX} visible ASCII bytes")]
    TokenInvalid,
    #[error("signaller listener: {0}")]
    Bind(std::io::Error),
}

/// SHA-256 of the bearer token, so comparisons are fixed-size and constant-time.
#[derive(Clone)]
pub struct TokenDigest([u8; 32]);

impl std::fmt::Debug for TokenDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TokenDigest(..)")
    }
}

impl TokenDigest {
    /// Validates raw file bytes: one optional trailing LF, then 32-256 visible bytes.
    pub fn from_file_bytes(raw: &[u8]) -> Result<Self, SignallerError> {
        let token = raw.strip_suffix(b"\n").unwrap_or(raw);
        let visible = token.iter().all(|b| (0x21..=0x7e).contains(b));
        if !(TOKEN_MIN..=TOKEN_MAX).contains(&token.len()) || !visible {
            return Err(SignallerError::TokenInvalid);
        }
        Ok(Self(Sha256::digest(token).into()))
    }

    fn matches(&self, candidate: &[u8]) -> bool {
        let digest: [u8; 32] = Sha256::digest(candidate).into();
        self.0.ct_eq(&digest).into()
    }
}

/// Reads and validates the token file; an invalid file prevents enabling the listener.
pub fn load_token(path: &Path) -> Result<TokenDigest, SignallerError> {
    use std::io::Read;
    let file = std::fs::File::open(path).map_err(SignallerError::TokenIo)?;
    let mut raw = Vec::new();
    file.take(TOKEN_MAX as u64 + 2)
        .read_to_end(&mut raw)
        .map_err(SignallerError::TokenIo)?;
    TokenDigest::from_file_bytes(&raw)
}

/// One validated invalidation shared by every delivery of its batch.
#[derive(Debug)]
pub struct Invalidation {
    method: HttpMethod,
    path: String,
    host: HeaderValue,
    headers: HeaderMap,
    body: Bytes,
}

#[derive(Debug)]
pub struct Delivery {
    pub request: Arc<Invalidation>,
    pub peer: SocketAddr,
    /// Host header sent on the wire; the invalidation host travels in `X-Stalewise-Host`.
    pub authority: HeaderValue,
    pub deadline: Instant,
}

struct Intake {
    config: SignalConfig,
    token: TokenDigest,
    peers: watch::Receiver<Option<Discovery>>,
    queue: mpsc::Sender<Delivery>,
    metrics: Arc<Metrics>,
}

/// Serves until `stop`, then drains accepted deliveries within their deadlines.
pub async fn serve(
    config: SignalConfig,
    token: TokenDigest,
    peers: watch::Receiver<Option<Discovery>>,
    metrics: Arc<Metrics>,
    stop: CancellationToken,
) -> Result<(), SignallerError> {
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(SignallerError::Bind)?;
    tracing::info!(addr = %config.listen, "signaller listening");
    let (queue, rx) = mpsc::channel(config.queue);
    let dispatcher = tokio::spawn(dispatch(rx, config.clone(), Arc::clone(&metrics)));
    let limits = Limits {
        header_timeout: config.header_timeout,
        connection_timeout: (config.header_timeout + config.read_timeout + config.write_timeout)
            .min(config.idle_timeout),
    };
    let intake = Arc::new(Intake {
        config,
        token,
        peers,
        queue,
        metrics,
    });
    http::serve(
        listener,
        limits,
        move |req| handle(Arc::clone(&intake), req),
        stop,
    )
    .await;
    let _ = dispatcher.await;
    Ok(())
}

async fn handle(intake: Arc<Intake>, req: Request<Incoming>) -> Response<Body> {
    let response = match admit(&intake, req).await {
        Ok(()) => http::text(StatusCode::ACCEPTED, "accepted for delivery\n"),
        Err(rejection) => rejection.response(&intake.config.methods),
    };
    let class = StatusClass::of(response.status().as_u16());
    intake
        .metrics
        .signaller_requests
        .get_or_create(&StatusLabels {
            status_class: class,
        })
        .inc();
    response
}

/// Why a request was refused before admission; nothing is queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rejection {
    Unauthorized,
    MethodNotAllowed,
    BadRequest(&'static str),
    TooLarge,
    Timeout,
    QueueFull,
}

impl Rejection {
    fn response(self, methods: &[Method]) -> Response<Body> {
        let (status, body) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized\n"),
            Self::MethodNotAllowed => (StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n"),
            Self::BadRequest(why) => (StatusCode::BAD_REQUEST, why),
            Self::TooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "body too large\n"),
            Self::Timeout => (StatusCode::REQUEST_TIMEOUT, "body read timed out\n"),
            Self::QueueFull => (StatusCode::SERVICE_UNAVAILABLE, "queue full\n"),
        };
        let mut response = http::text(status, body);
        let headers = response.headers_mut();
        if self == Self::Unauthorized {
            headers.insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        let allow: Vec<&str> = methods.iter().map(|m| m.as_str()).collect();
        if let (Self::MethodNotAllowed, Ok(value)) =
            (self, HeaderValue::from_str(&allow.join(", ")))
        {
            headers.insert(header::ALLOW, value);
        }
        response
    }
}

async fn admit(intake: &Intake, req: Request<Incoming>) -> Result<(), Rejection> {
    let config = &intake.config;
    authorize(&intake.token, req.headers())?;
    let method = allowed(&config.methods, req.method())?;
    let (parts, body) = req.into_parts();
    let path = origin_form(&parts.uri)?;
    let host = target_host(config.canonical_host.as_ref(), &parts.headers)?;
    if method == Method::Ban {
        single_tag(&parts.headers)?;
    }
    let body = read_body(body, &parts.headers, config).await?;
    let request = Arc::new(Invalidation {
        method: parts.method,
        path,
        host,
        headers: forwardable(&parts.headers),
        body,
    });
    let peers = targets(config, &intake.peers.borrow());
    enqueue(intake, request, &peers)
}

fn authorize(token: &TokenDigest, headers: &HeaderMap) -> Result<(), Rejection> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return Err(Rejection::Unauthorized);
    };
    let presented = value
        .as_bytes()
        .strip_prefix(b"Bearer ")
        .ok_or(Rejection::Unauthorized)?;
    if token.matches(presented) {
        Ok(())
    } else {
        Err(Rejection::Unauthorized)
    }
}

fn allowed(methods: &[Method], method: &HttpMethod) -> Result<Method, Rejection> {
    methods
        .iter()
        .find(|m| m.as_str() == method.as_str())
        .copied()
        .ok_or(Rejection::MethodNotAllowed)
}

fn origin_form(uri: &hyper::Uri) -> Result<String, Rejection> {
    let bad = Rejection::BadRequest("origin-form request target required\n");
    if uri.scheme().is_some() || uri.authority().is_some() {
        return Err(bad);
    }
    let path = uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
    if path.starts_with('/') {
        Ok(path.to_owned())
    } else {
        Err(bad)
    }
}

fn target_host(
    canonical: Option<&Authority>,
    headers: &HeaderMap,
) -> Result<HeaderValue, Rejection> {
    let bad = Rejection::BadRequest("valid Host required\n");
    if let Some(canonical) = canonical {
        return authority_value(canonical).ok_or(bad);
    }
    let mut values = headers.get_all(header::HOST).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return Err(bad);
    };
    let text = value.to_str().map_err(|_| bad)?;
    let authority = text.parse::<Authority>().map_err(|_| bad)?;
    authority_value(&authority).ok_or(bad)
}

fn authority_value(authority: &Authority) -> Option<HeaderValue> {
    HeaderValue::from_str(&authority.to_string()).ok()
}

/// Host header for one target: peers get `--peer-authority` when set, so a mesh sidecar
/// routes by the peers Service instead of the application's routes.
pub fn wire_authority(
    config: &SignalConfig,
    peer: SocketAddr,
    invalidation_host: &HeaderValue,
) -> HeaderValue {
    match &config.peer_authority {
        Some(authority) if peer != config.local_frontend => authority.clone(),
        _ => invalidation_host.clone(),
    }
}

fn single_tag(headers: &HeaderMap) -> Result<(), Rejection> {
    let mut values = headers.get_all(TAG_HEADER).iter();
    let valid = |v: &HeaderValue| {
        let b = v.as_bytes();
        (1..=MAX_TAG).contains(&b.len())
            && b.iter()
                .all(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c == b'-')
    };
    match (values.next(), values.next()) {
        (Some(value), None) if valid(value) => Ok(()),
        _ => Err(Rejection::BadRequest(
            "exactly one valid X-Stalewise-Tag required\n",
        )),
    }
}

async fn read_body(
    body: Incoming,
    headers: &HeaderMap,
    config: &SignalConfig,
) -> Result<Bytes, Rejection> {
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if declared.is_some_and(|len| len > config.body_limit as u64) {
        return Err(Rejection::TooLarge);
    }
    let collected = tokio::time::timeout(
        config.read_timeout,
        Limited::new(body, config.body_limit).collect(),
    )
    .await;
    match collected {
        Err(_) => Err(Rejection::Timeout),
        Ok(Err(err))
            if err
                .downcast_ref::<http_body_util::LengthLimitError>()
                .is_some() =>
        {
            Err(Rejection::TooLarge)
        }
        Ok(Err(_)) => Err(Rejection::BadRequest("malformed body\n")),
        Ok(Ok(collected)) => Ok(collected.to_bytes()),
    }
}

/// Drops credentials, hop-by-hop headers, Connection-nominated headers, Host and framing.
pub fn forwardable(headers: &HeaderMap) -> HeaderMap {
    let nominated: BTreeSet<String> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .collect();
    let mut out = HeaderMap::new();
    for (name, value) in headers {
        let lower = name.as_str();
        let dropped = HOP_BY_HOP.contains(&lower)
            || nominated.contains(lower)
            || name == header::AUTHORIZATION
            || name == header::HOST
            || name == header::CONTENT_LENGTH;
        if !dropped {
            out.append(name.clone(), value.clone());
        }
    }
    out
}

/// Every discovered peer except this pod, plus the local frontend; deduplicated.
pub fn targets(config: &SignalConfig, snapshot: &Option<Discovery>) -> Vec<SocketAddr> {
    let mut set: BTreeSet<SocketAddr> = snapshot
        .iter()
        .flat_map(|d| d.endpoints.iter())
        .filter_map(|e| match e.host {
            crate::endpoint::Host::Ip(ip) if !config.pod_ips.contains(&ip) => {
                Some(SocketAddr::new(ip, e.port))
            }
            _ => None,
        })
        .collect();
    set.insert(config.local_frontend);
    set.into_iter().collect()
}

fn enqueue(
    intake: &Intake,
    request: Arc<Invalidation>,
    peers: &[SocketAddr],
) -> Result<(), Rejection> {
    let permits = intake
        .queue
        .try_reserve_many(peers.len())
        .map_err(|_| Rejection::QueueFull)?;
    let deadline = Instant::now() + intake.config.delivery_deadline;
    for (permit, peer) in permits.zip(peers) {
        permit.send(Delivery {
            authority: wire_authority(&intake.config, *peer, &request.host),
            request: Arc::clone(&request),
            peer: *peer,
            deadline,
        });
    }
    intake
        .metrics
        .signaller_queue_depth
        .set(queue_depth(&intake.queue));
    Ok(())
}

fn queue_depth(queue: &mpsc::Sender<Delivery>) -> i64 {
    i64::try_from(queue.max_capacity() - queue.capacity()).unwrap_or(i64::MAX)
}

/// Owns the receiver and runs at most `workers` deliveries at once until the queue closes.
async fn dispatch(mut rx: mpsc::Receiver<Delivery>, config: SignalConfig, metrics: Arc<Metrics>) {
    let slots = Arc::new(Semaphore::new(config.workers));
    let tracker = tokio_util::task::TaskTracker::new();
    while let Some(delivery) = rx.recv().await {
        let depth = i64::try_from(rx.len()).unwrap_or(i64::MAX);
        metrics.signaller_queue_depth.set(depth);
        let Ok(permit) = Arc::clone(&slots).acquire_owned().await else {
            break;
        };
        let metrics = Arc::clone(&metrics);
        let timeout = config.peer_timeout;
        tracker.spawn(async move {
            deliver(delivery, timeout, &metrics).await;
            drop(permit);
        });
    }
    tracker.close();
    tracker.wait().await;
}

enum Outcome {
    Delivered,
    Rejected,
    Retry,
}

fn classify(result: &Result<StatusCode, http::ClientError>) -> Outcome {
    match result {
        Ok(s) if s.is_success() => Outcome::Delivered,
        Ok(s)
            if *s == StatusCode::REQUEST_TIMEOUT
                || *s == StatusCode::TOO_MANY_REQUESTS
                || s.is_server_error() =>
        {
            Outcome::Retry
        }
        Ok(_) => Outcome::Rejected,
        Err(_) => Outcome::Retry,
    }
}

async fn deliver(delivery: Delivery, peer_timeout: Duration, metrics: &Metrics) {
    let count = |result| {
        metrics
            .signaller_deliveries
            .get_or_create(&ResultLabels { result })
            .inc();
    };
    let mut attempt: u32 = 0;
    loop {
        let remaining = delivery.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            metrics.signaller_exhausted.inc();
            tracing::warn!(peer = %delivery.peer, "invalidation delivery exhausted at its deadline");
            return;
        }
        let result = http::send(
            delivery.peer,
            build(&delivery.request, &delivery.authority),
            peer_timeout.min(remaining),
        )
        .await;
        match classify(&result) {
            Outcome::Delivered => return count(DeliveryResult::Delivered),
            Outcome::Rejected => {
                tracing::warn!(peer = %delivery.peer, status = ?result.ok(), "peer rejected invalidation");
                return count(DeliveryResult::Rejected);
            }
            Outcome::Retry => count(DeliveryResult::Retried),
        }
        let ceiling = RETRY_BASE_MS
            .saturating_mul(2u64.saturating_pow(attempt))
            .min(RETRY_CAP_MS);
        attempt = attempt.saturating_add(1);
        let pause = Duration::from_millis(rand::random_range(RETRY_BASE_MS..=ceiling));
        tokio::time::sleep_until((Instant::now() + pause).min(delivery.deadline)).await;
    }
}

fn build(inv: &Invalidation, authority: &HeaderValue) -> Request<Body> {
    let mut request = Request::new(Full::new(inv.body.clone()));
    *request.method_mut() = inv.method.clone();
    *request.uri_mut() = inv.path.parse().unwrap_or_default();
    let headers = request.headers_mut();
    headers.clone_from(&inv.headers);
    headers.insert(header::HOST, authority.clone());
    headers.insert(HeaderName::from_static(HOST_HEADER), inv.host.clone());
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(inv.body.len()));
    headers.insert(
        HeaderName::from_static("connection"),
        HeaderValue::from_static("close"),
    );
    request
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::{Endpoint, Host};
    use std::net::IpAddr;
    use std::sync::Mutex;
    use std::time::SystemTime;

    const TOKEN: &str = "0123456789abcdef0123456789abcdef";

    #[derive(Debug, Clone)]
    struct Seen {
        method: String,
        path: String,
        headers: HeaderMap,
        body: Bytes,
    }

    /// A peer answering with scripted statuses (last one repeats) and recording requests.
    async fn peer(bind: &str, statuses: Vec<u16>) -> (SocketAddr, Arc<Mutex<Vec<Seen>>>) {
        let listener = tokio::net::TcpListener::bind(bind).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        let limits = Limits {
            header_timeout: Duration::from_secs(1),
            connection_timeout: Duration::from_secs(1),
        };
        tokio::spawn(http::serve(
            listener,
            limits,
            move |req: Request<Incoming>| {
                let log = Arc::clone(&log);
                let statuses = statuses.clone();
                async move {
                    let (parts, body) = req.into_parts();
                    let body = body.collect().await.unwrap().to_bytes();
                    let mut log = log.lock().unwrap();
                    let status = statuses[log.len().min(statuses.len() - 1)];
                    log.push(Seen {
                        method: parts.method.to_string(),
                        path: parts.uri.to_string(),
                        headers: parts.headers,
                        body,
                    });
                    http::text(StatusCode::from_u16(status).unwrap(), "")
                }
            },
            CancellationToken::new(),
        ));
        (addr, seen)
    }

    fn config(local: SocketAddr, queue: usize, deadline: Duration) -> SignalConfig {
        SignalConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            token_file: "/unused".into(),
            pod_ips: vec!["10.9.9.9".parse().unwrap(), "fd00::9".parse().unwrap()],
            local_frontend: local,
            canonical_host: Some("catalog.apps.svc".parse().unwrap()),
            peer_authority: None,
            methods: vec![Method::Purge, Method::Ban],
            body_limit: 64,
            workers: 4,
            queue,
            delivery_deadline: deadline,
            peer_timeout: Duration::from_millis(200),
            header_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_millis(300),
            write_timeout: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(2),
        }
    }

    fn snapshot(addrs: &[SocketAddr]) -> Option<Discovery> {
        let endpoints: Vec<Endpoint> = addrs
            .iter()
            .map(|a| Endpoint::new(Host::Ip(a.ip()), a.port()))
            .collect();
        Some(Discovery {
            endpoints: endpoints.into(),
            last_success: SystemTime::now(),
            error: None,
        })
    }

    struct Running {
        addr: SocketAddr,
        metrics: Arc<Metrics>,
        stop: CancellationToken,
        task: tokio::task::JoinHandle<Result<(), SignallerError>>,
        _peers: watch::Sender<Option<Discovery>>,
    }

    async fn start(config: SignalConfig, peers: Option<Discovery>) -> Running {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let config = SignalConfig {
            listen: addr,
            ..config
        };
        let (tx, rx) = watch::channel(peers);
        let metrics = Arc::new(Metrics::new());
        let stop = CancellationToken::new();
        let token = TokenDigest::from_file_bytes(format!("{TOKEN}\n").as_bytes()).unwrap();
        let task = tokio::spawn(serve(config, token, rx, Arc::clone(&metrics), stop.clone()));
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Running {
            addr,
            metrics,
            stop,
            task,
            _peers: tx,
        }
    }

    async fn raw(addr: SocketAddr, request: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut out = String::new();
        let _ = stream.read_to_string(&mut out).await;
        out
    }

    fn purge(extra: &str) -> String {
        format!(
            "PURGE /a?b=1 HTTP/1.1\r\nHost: alias\r\nAuthorization: Bearer {TOKEN}\r\n{extra}Content-Length: 0\r\n\r\n"
        )
    }

    async fn eventually(mut check: impl FnMut() -> bool) {
        for _ in 0..300 {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not reached");
    }

    #[test]
    fn token_file_rules() {
        assert!(TokenDigest::from_file_bytes(TOKEN.as_bytes()).is_ok());
        assert!(TokenDigest::from_file_bytes(format!("{TOKEN}\n").as_bytes()).is_ok());
        assert!(TokenDigest::from_file_bytes(format!("{TOKEN}\n\n").as_bytes()).is_err());
        assert!(TokenDigest::from_file_bytes(b"short").is_err());
        assert!(TokenDigest::from_file_bytes(format!("{TOKEN} x").as_bytes()).is_err());
        assert!(TokenDigest::from_file_bytes(&[b'a'; 257]).is_err());
        let digest = TokenDigest::from_file_bytes(TOKEN.as_bytes()).unwrap();
        assert!(digest.matches(TOKEN.as_bytes()));
        assert!(!digest.matches(b"0123456789abcdef0123456789abcdeX"));
    }

    #[test]
    fn forwarding_strips_credentials_hop_by_hop_and_nominated_headers() {
        let mut h = HeaderMap::new();
        for (k, v) in [
            ("authorization", "Bearer x"),
            ("proxy-authorization", "x"),
            ("connection", "keep-alive, X-Secret"),
            ("x-secret", "1"),
            ("te", "trailers"),
            ("transfer-encoding", "chunked"),
            ("host", "a"),
            ("x-keep", "yes"),
        ] {
            h.append(HeaderName::from_static(k), HeaderValue::from_static(v));
        }
        let out = forwardable(&h);
        let names: Vec<&str> = out.keys().map(HeaderName::as_str).collect();
        assert_eq!(names, ["x-keep"]);
    }

    #[test]
    fn targets_drop_every_own_pod_ip_add_loopback_and_format_ipv6() {
        let local: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let cfg = config(local, 8, Duration::from_secs(1));
        let own_v4 = SocketAddr::new(cfg.pod_ips[0], 8080);
        let own_v6 = SocketAddr::new(cfg.pod_ips[1], 8080);
        let v6: SocketAddr = "[fd00::5]:8080".parse().unwrap();
        let got = targets(&cfg, &snapshot(&[own_v4, own_v6, v6, v6]));
        assert_eq!(got, [local, v6]);
        assert_eq!(v6.to_string(), "[fd00::5]:8080");
        assert!(matches!(v6.ip(), IpAddr::V6(_)));
    }

    #[test]
    fn peers_get_the_peer_authority_and_local_delivery_keeps_the_invalidation_host() {
        let local: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let peer: SocketAddr = "10.1.2.3:8080".parse().unwrap();
        let host = HeaderValue::from_static("catalog.apps.svc");
        let mut cfg = config(local, 8, Duration::from_secs(1));
        assert_eq!(
            wire_authority(&cfg, peer, &host),
            host,
            "unset keeps today's Host"
        );
        cfg.peer_authority = Some(HeaderValue::from_static("cache-peers.ns.svc:8080"));
        assert_eq!(wire_authority(&cfg, peer, &host), "cache-peers.ns.svc:8080");
        assert_eq!(wire_authority(&cfg, local, &host), host);
    }

    #[test]
    fn built_request_carries_wire_host_and_invalidation_host_separately() {
        let mut spoofed = HeaderMap::new();
        spoofed.insert(HOST_HEADER, HeaderValue::from_static("evil.test"));
        let inv = Invalidation {
            method: HttpMethod::from_bytes(b"PURGE").unwrap(),
            path: "/a".into(),
            host: HeaderValue::from_static("catalog.apps.svc"),
            headers: spoofed,
            body: Bytes::new(),
        };
        let req = build(&inv, &HeaderValue::from_static("cache-peers.ns.svc:8080"));
        assert_eq!(req.headers()["host"], "cache-peers.ns.svc:8080");
        let carried: Vec<_> = req.headers().get_all(HOST_HEADER).iter().collect();
        assert_eq!(carried, ["catalog.apps.svc"], "client value is replaced");
    }

    #[tokio::test]
    async fn rejects_bad_auth_methods_bodies_and_tags_without_queueing() {
        let (local, seen) = peer("127.0.0.1:0", vec![200]).await;
        let s = start(config(local, 8, Duration::from_secs(1)), None).await;
        let no_auth = "PURGE / HTTP/1.1\r\nHost: a\r\nContent-Length: 0\r\n\r\n";
        assert!(raw(s.addr, no_auth).await.starts_with("HTTP/1.1 401"));
        let wrong = purge("").replace(TOKEN, "0123456789abcdef0123456789abcdeX");
        assert!(raw(s.addr, &wrong).await.starts_with("HTTP/1.1 401"));
        let dup = purge(&format!("Authorization: Bearer {TOKEN}\r\n"));
        assert!(raw(s.addr, &dup).await.starts_with("HTTP/1.1 401"));
        let get = purge("").replacen("PURGE", "GET", 1);
        let resp = raw(s.addr, &get).await;
        assert!(resp.starts_with("HTTP/1.1 405"));
        assert!(resp.to_ascii_lowercase().contains("allow: purge, ban"));
        let big = purge("").replace("Content-Length: 0", "Content-Length: 65");
        assert!(raw(s.addr, &big).await.starts_with("HTTP/1.1 413"));
        let chunked = purge("Transfer-Encoding: chunked\r\n").replace("Content-Length: 0\r\n", "")
            + &format!("41\r\n{}\r\n0\r\n\r\n", "x".repeat(65));
        assert!(raw(s.addr, &chunked).await.starts_with("HTTP/1.1 413"));
        let ban = purge("").replacen("PURGE", "BAN", 1);
        assert!(raw(s.addr, &ban).await.starts_with("HTTP/1.1 400"));
        let ban_two =
            purge("X-Stalewise-Tag: a\r\nX-Stalewise-Tag: b\r\n").replacen("PURGE", "BAN", 1);
        assert!(raw(s.addr, &ban_two).await.starts_with("HTTP/1.1 400"));
        let absolute = purge("").replacen("/a?b=1", "http://evil/a", 1);
        assert!(raw(s.addr, &absolute).await.starts_with("HTTP/1.1 400"));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(seen.lock().unwrap().is_empty());
        let client_errors = s.metrics.signaller_requests.get_or_create(&StatusLabels {
            status_class: StatusClass::ClientError,
        });
        assert_eq!(client_errors.get(), 9);
        s.stop.cancel();
        s.task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn accepted_requests_fan_out_with_canonical_host_and_stripped_headers() {
        let (local, local_seen) = peer("127.0.0.1:0", vec![200]).await;
        let (remote, remote_seen) = peer("127.0.0.1:0", vec![200]).await;
        let s = start(
            config(local, 8, Duration::from_secs(2)),
            snapshot(&[remote]),
        )
        .await;
        let req = purge("Connection: X-Hop\r\nX-Hop: 1\r\nX-Keep: 1\r\n")
            .replace("Content-Length: 0\r\n\r\n", "Content-Length: 2\r\n\r\nhi");
        let resp = raw(s.addr, &req).await;
        assert!(resp.starts_with("HTTP/1.1 202"), "{resp}");
        eventually(|| {
            local_seen.lock().unwrap().len() == 1 && remote_seen.lock().unwrap().len() == 1
        })
        .await;
        let got = remote_seen.lock().unwrap()[0].clone();
        assert_eq!(got.method, "PURGE");
        assert_eq!(got.path, "/a?b=1");
        assert_eq!(got.body, Bytes::from_static(b"hi"));
        assert_eq!(got.headers["host"], "catalog.apps.svc");
        assert_eq!(got.headers["x-stalewise-host"], "catalog.apps.svc");
        assert!(got.headers.get("authorization").is_none());
        assert!(got.headers.get("x-hop").is_none());
        assert_eq!(got.headers["x-keep"], "1");
        s.stop.cancel();
        s.task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn queue_admission_is_all_or_nothing() {
        let (local, local_seen) = peer("127.0.0.1:0", vec![200]).await;
        let (remote, _) = peer("127.0.0.1:0", vec![200]).await;
        let s = start(
            config(local, 1, Duration::from_secs(1)),
            snapshot(&[remote]),
        )
        .await;
        assert!(raw(s.addr, &purge("")).await.starts_with("HTTP/1.1 503"));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(local_seen.lock().unwrap().is_empty(), "no partial delivery");
        s.stop.cancel();
        s.task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn transient_failures_retry_and_client_errors_do_not() {
        let (local, local_seen) = peer("127.0.0.1:0", vec![503, 429, 200]).await;
        let (remote, remote_seen) = peer("127.0.0.1:0", vec![403, 200]).await;
        let s = start(
            config(local, 8, Duration::from_secs(3)),
            snapshot(&[remote]),
        )
        .await;
        assert!(raw(s.addr, &purge("")).await.starts_with("HTTP/1.1 202"));
        eventually(|| local_seen.lock().unwrap().len() == 3).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(remote_seen.lock().unwrap().len(), 1);
        let delivered = s.metrics.signaller_deliveries.get_or_create(&ResultLabels {
            result: DeliveryResult::Delivered,
        });
        assert_eq!(delivered.get(), 1);
        s.stop.cancel();
        s.task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn deadline_exhaustion_is_counted_and_shutdown_drains() {
        let (local, seen) = peer("127.0.0.1:0", vec![500]).await;
        let s = start(config(local, 8, Duration::from_millis(400)), None).await;
        assert!(raw(s.addr, &purge("")).await.starts_with("HTTP/1.1 202"));
        s.stop.cancel();
        s.task.await.unwrap().unwrap();
        assert_eq!(s.metrics.signaller_exhausted.get(), 1);
        assert!(seen.lock().unwrap().len() >= 2);
    }

    #[tokio::test]
    async fn delivers_to_ipv6_peers() {
        let Ok(probe) = std::net::TcpListener::bind("[::1]:0") else {
            return;
        };
        drop(probe);
        let (local, _) = peer("127.0.0.1:0", vec![200]).await;
        let (v6, seen) = peer("[::1]:0", vec![200]).await;
        let s = start(config(local, 8, Duration::from_secs(1)), snapshot(&[v6])).await;
        assert!(raw(s.addr, &purge("")).await.starts_with("HTTP/1.1 202"));
        eventually(|| seen.lock().unwrap().len() == 1).await;
        s.stop.cancel();
        s.task.await.unwrap().unwrap();
    }
}
