//! Authenticated invalidation intake that fans out to every peer through a bounded queue.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
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

use crate::config::{self, Method, SignalConfig};
use crate::discovery::Discovery;
use crate::endpoint::Authority;
use crate::http::{self, Body, Limits};
use crate::ledger::{
    ExhaustReason, History, InvalidationId, Outcome, Status, Target, pending_status,
};
use crate::metrics::{
    DeliveryResult, InvalidationLabels, InvalidationResult, Metrics, ResultLabels, StatusClass,
    StatusLabels,
};

const TOKEN_MIN: usize = 32;
const TOKEN_MAX: usize = 256;
const TAG_HEADER: &str = "x-stalewise-tag";
const HOST_HEADER: &str = "x-stalewise-host";
const MAX_TAG: usize = 128;
const RETRY_BASE_MS: u64 = 100;
const RETRY_CAP_MS: u64 = 1000;
const WAIT_HEADER: &str = "x-stalewise-wait";
const POD_HEADER: &str = "x-stalewise-pod";
const STATUS_PREFIX: &str = "/invalidations/";
/// Longest synchronous wait a write or status request may ask for.
pub const MAX_WAIT: Duration = Duration::from_secs(30);
/// Retained target rows per queue slot.
const ROWS_PER_SLOT: usize = 4;
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
    pub id: InvalidationId,
    /// Index of this target in the invalidation's status rows.
    pub slot: usize,
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
    ledger: Arc<Ledger>,
    metrics: Arc<Metrics>,
}

/// Invalidation history plus a version bump that wakes waiters on every transition.
struct Ledger {
    history: Mutex<History>,
    changed: watch::Sender<u64>,
    metrics: Arc<Metrics>,
}

impl Ledger {
    fn lock(&self) -> MutexGuard<'_, History> {
        self.history.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn lookup(&self, id: InvalidationId) -> Option<Status> {
        self.lock().lookup(id)
    }

    fn complete(&self, id: InvalidationId, slot: usize, outcome: Outcome) {
        let (done, lagging) = {
            let mut history = self.lock();
            let done = history.complete(id, slot, outcome, Instant::now());
            (done, history.lagging())
        };
        self.metrics.invalidation_peers_lagging.set(count(lagging));
        if let Some(done) = done {
            let result = if done.delivered {
                InvalidationResult::Delivered
            } else {
                InvalidationResult::Exhausted
            };
            self.metrics
                .invalidation_latency
                .get_or_create(&InvalidationLabels { result })
                .observe(done.latency.as_secs_f64());
            tracing::info!(id = %id, delivered = done.delivered, "invalidation finished");
        }
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }
}

fn count(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
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
    let ledger = Arc::new(Ledger {
        history: Mutex::new(History::new(
            config.history,
            config.queue.saturating_mul(ROWS_PER_SLOT),
        )),
        changed: watch::Sender::new(0),
        metrics: Arc::clone(&metrics),
    });
    let dispatcher = tokio::spawn(dispatch(
        rx,
        config.clone(),
        Arc::clone(&ledger),
        stop.clone(),
    ));
    let limits = Limits {
        header_timeout: config.header_timeout,
        connection_timeout: config.header_timeout
            + config.read_timeout
            + MAX_WAIT
            + config.write_timeout,
    };
    let intake = Arc::new(Intake {
        config,
        token,
        peers,
        queue,
        ledger,
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
    let response = route(&intake, req)
        .await
        .unwrap_or_else(|rejection| rejection.response(&intake.config.methods));
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

/// Authentication precedes everything; GET reads status, configured methods invalidate.
async fn route(intake: &Intake, req: Request<Incoming>) -> Result<Response<Body>, Rejection> {
    authorize(&intake.token, req.headers())?;
    if req.method() == HttpMethod::GET {
        return status(intake, &req).await;
    }
    let method = allowed(&intake.config.methods, req.method())?;
    let wait = header_wait(req.headers())?;
    let admitted = admit(intake, method, req).await?;
    let Some(limit) = wait else {
        return Ok(status_response(intake, &admitted, StatusCode::ACCEPTED));
    };
    let finished = wait_for(&intake.ledger, admitted, limit).await;
    Ok(status_response(intake, &finished, wait_code(&finished)))
}

async fn status(intake: &Intake, req: &Request<Incoming>) -> Result<Response<Body>, Rejection> {
    let id = status_id(req.uri().path())?;
    let wait = query_wait(req.uri().query())?;
    let current = intake.ledger.lookup(id).ok_or(Rejection::NotFound)?;
    let Some(limit) = wait else {
        return Ok(status_response(intake, &current, StatusCode::OK));
    };
    let finished = wait_for(&intake.ledger, current, limit).await;
    Ok(status_response(intake, &finished, wait_code(&finished)))
}

fn wait_code(status: &Status) -> StatusCode {
    if status.all_delivered() {
        StatusCode::OK
    } else {
        StatusCode::GATEWAY_TIMEOUT
    }
}

fn status_id(path: &str) -> Result<InvalidationId, Rejection> {
    let raw = path
        .strip_prefix(STATUS_PREFIX)
        .ok_or(Rejection::NotFound)?;
    raw.parse()
        .map_err(|()| Rejection::BadRequest("malformed invalidation id\n"))
}

const BAD_WAIT: Rejection = Rejection::BadRequest("wait must be one duration from 1ms to 30s\n");

/// A positive `ms`/`s`/`m` duration of at most [`MAX_WAIT`].
pub fn parse_wait(raw: &str) -> Option<Duration> {
    config::parse_duration(raw)
        .ok()
        .filter(|d| !d.is_zero() && *d <= MAX_WAIT)
}

fn header_wait(headers: &HeaderMap) -> Result<Option<Duration>, Rejection> {
    let mut values = headers.get_all(WAIT_HEADER).iter();
    match (values.next(), values.next()) {
        (None, _) => Ok(None),
        (Some(value), None) => value
            .to_str()
            .ok()
            .and_then(parse_wait)
            .map(Some)
            .ok_or(BAD_WAIT),
        _ => Err(BAD_WAIT),
    }
}

/// The status query accepts exactly one `wait` control and nothing else.
fn query_wait(query: Option<&str>) -> Result<Option<Duration>, Rejection> {
    let Some(query) = query else {
        return Ok(None);
    };
    query
        .strip_prefix("wait=")
        .filter(|raw| !raw.contains('&'))
        .and_then(parse_wait)
        .map(Some)
        .ok_or(BAD_WAIT)
}

/// Returns once every target is terminal or `limit` passes; never cancels delivery.
async fn wait_for(ledger: &Ledger, initial: Status, limit: Duration) -> Status {
    let deadline = Instant::now() + limit;
    let mut changes = ledger.changed.subscribe();
    let mut current = initial;
    loop {
        changes.borrow_and_update();
        if let Some(latest) = ledger.lookup(current.id) {
            current = latest;
        }
        if current.finished() {
            return current;
        }
        match tokio::time::timeout_at(deadline, changes.changed()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => return current,
        }
    }
}

fn status_response(intake: &Intake, status: &Status, code: StatusCode) -> Response<Body> {
    let body = serde_json::to_vec(status).unwrap_or_default();
    let mut response = http::text(code, body);
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Ok(location) = HeaderValue::from_str(&format!("{STATUS_PREFIX}{}", status.id)) {
        headers.insert(header::LOCATION, location);
    }
    if let Ok(pod) = HeaderValue::from_str(&intake.config.pod_name) {
        headers.insert(HeaderName::from_static(POD_HEADER), pod);
    }
    response
}

/// Why a request was refused before admission; nothing is queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rejection {
    Unauthorized,
    MethodNotAllowed,
    BadRequest(&'static str),
    NotFound,
    TooLarge,
    Timeout,
    QueueFull,
    HistoryFull,
}

impl Rejection {
    fn response(self, methods: &[Method]) -> Response<Body> {
        let (status, body) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized\n"),
            Self::MethodNotAllowed => (StatusCode::METHOD_NOT_ALLOWED, "method not allowed\n"),
            Self::BadRequest(why) => (StatusCode::BAD_REQUEST, why),
            Self::NotFound => (StatusCode::NOT_FOUND, "unknown invalidation\n"),
            Self::TooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "body too large\n"),
            Self::Timeout => (StatusCode::REQUEST_TIMEOUT, "body read timed out\n"),
            Self::QueueFull => (StatusCode::SERVICE_UNAVAILABLE, "queue full\n"),
            Self::HistoryFull => (StatusCode::SERVICE_UNAVAILABLE, "history full\n"),
        };
        let mut response = http::text(status, body);
        let headers = response.headers_mut();
        if self == Self::Unauthorized {
            headers.insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        let allow: Vec<&str> = std::iter::once("GET")
            .chain(methods.iter().map(|m| m.as_str()))
            .collect();
        if let (Self::MethodNotAllowed, Ok(value)) =
            (self, HeaderValue::from_str(&allow.join(", ")))
        {
            headers.insert(header::ALLOW, value);
        }
        response
    }
}

async fn admit(
    intake: &Intake,
    method: Method,
    req: Request<Incoming>,
) -> Result<Status, Rejection> {
    let config = &intake.config;
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
            || lower == WAIT_HEADER
            || name == header::HOST
            || name == header::CONTENT_LENGTH;
        if !dropped {
            out.append(name.clone(), value.clone());
        }
    }
    out
}

/// The local frontend, then every discovered peer except this pod; frozen at admission.
pub fn targets(config: &SignalConfig, snapshot: &Option<Discovery>) -> Vec<Target> {
    let mut out = vec![Target {
        pod: Some(config.pod_name.clone()),
        addr: config.local_frontend,
    }];
    let peers = snapshot.iter().flat_map(|d| d.peers.iter());
    for peer in peers {
        let addr = SocketAddr::new(peer.address, peer.port);
        let own = peer.pod_name == config.pod_name || config.pod_ips.contains(&peer.address);
        if own || out.iter().any(|t| t.addr == addr) {
            continue;
        }
        out.push(Target {
            pod: Some(peer.pod_name.clone()),
            addr,
        });
    }
    out
}

/// Reserves history and queue capacity together; on any shortfall nothing is recorded or queued.
fn enqueue(
    intake: &Intake,
    request: Arc<Invalidation>,
    targets: &[Target],
) -> Result<Status, Rejection> {
    let id = InvalidationId::random();
    let status = pending_status(id, &intake.config.pod_name, targets);
    let mut history = intake.ledger.lock();
    let victims = history.plan(targets.len()).ok_or(Rejection::HistoryFull)?;
    let permits = intake
        .queue
        .try_reserve_many(targets.len())
        .map_err(|_| Rejection::QueueFull)?;
    history.insert(&victims, status.clone(), Instant::now());
    let lagging = history.lagging();
    drop(history);
    let deadline = Instant::now() + intake.config.delivery_deadline;
    for ((slot, target), permit) in targets.iter().enumerate().zip(permits) {
        permit.send(Delivery {
            authority: wire_authority(&intake.config, target.addr, &request.host),
            request: Arc::clone(&request),
            id,
            slot,
            peer: target.addr,
            deadline,
        });
    }
    intake
        .metrics
        .invalidation_peers_lagging
        .set(count(lagging));
    intake
        .metrics
        .signaller_queue_depth
        .set(queue_depth(&intake.queue));
    tracing::info!(id = %id, targets = targets.len(), "invalidation admitted");
    Ok(status)
}

fn queue_depth(queue: &mpsc::Sender<Delivery>) -> i64 {
    i64::try_from(queue.max_capacity() - queue.capacity()).unwrap_or(i64::MAX)
}

/// Owns the receiver and runs at most `workers` deliveries at once until the queue closes.
async fn dispatch(
    mut rx: mpsc::Receiver<Delivery>,
    config: SignalConfig,
    ledger: Arc<Ledger>,
    stop: CancellationToken,
) {
    let metrics = Arc::clone(&ledger.metrics);
    let slots = Arc::new(Semaphore::new(config.workers));
    let tracker = tokio_util::task::TaskTracker::new();
    while let Some(delivery) = rx.recv().await {
        let depth = i64::try_from(rx.len()).unwrap_or(i64::MAX);
        metrics.signaller_queue_depth.set(depth);
        let Ok(permit) = Arc::clone(&slots).acquire_owned().await else {
            break;
        };
        let ledger = Arc::clone(&ledger);
        let stop = stop.clone();
        let timeout = config.peer_timeout;
        tracker.spawn(async move {
            let (id, slot) = (delivery.id, delivery.slot);
            let outcome = deliver(delivery, timeout, &ledger.metrics, &stop).await;
            ledger.complete(id, slot, outcome);
            drop(permit);
        });
    }
    tracker.close();
    tracker.wait().await;
}

enum Attempt {
    Delivered,
    Rejected,
    Retry,
}

fn classify(result: &Result<StatusCode, http::ClientError>) -> Attempt {
    match result {
        Ok(s) if s.is_success() => Attempt::Delivered,
        Ok(s)
            if *s == StatusCode::REQUEST_TIMEOUT
                || *s == StatusCode::TOO_MANY_REQUESTS
                || s.is_server_error() =>
        {
            Attempt::Retry
        }
        Ok(_) => Attempt::Rejected,
        Err(_) => Attempt::Retry,
    }
}

/// Retries transient failures until the admission deadline; returns the target's terminal state.
async fn deliver(
    delivery: Delivery,
    peer_timeout: Duration,
    metrics: &Metrics,
    stop: &CancellationToken,
) -> Outcome {
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
            let reason = if stop.is_cancelled() {
                ExhaustReason::Shutdown
            } else {
                ExhaustReason::Deadline
            };
            tracing::warn!(id = %delivery.id, peer = %delivery.peer, reason = ?reason, "invalidation delivery exhausted");
            return Outcome::Exhausted(reason);
        }
        let result = http::send(
            delivery.peer,
            build(&delivery.request, &delivery.authority),
            peer_timeout.min(remaining),
        )
        .await;
        match classify(&result) {
            Attempt::Delivered => {
                count(DeliveryResult::Delivered);
                return Outcome::Delivered;
            }
            Attempt::Rejected => {
                tracing::warn!(id = %delivery.id, peer = %delivery.peer, status = ?result.ok(), "peer rejected invalidation");
                count(DeliveryResult::Rejected);
                return Outcome::Exhausted(ExhaustReason::Rejected);
            }
            Attempt::Retry => count(DeliveryResult::Retried),
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
    use crate::endpoint::Endpoint;
    use crate::membership::Peer;
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
            pod_name: "cache-0".into(),
            history: 16,
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
        let peers: Vec<Peer> = addrs
            .iter()
            .enumerate()
            .map(|(i, a)| Peer {
                pod_name: format!("peer-{i}"),
                namespace: "apps".into(),
                address: a.ip(),
                port: a.port(),
            })
            .collect();
        let endpoints: Vec<Endpoint> = peers.iter().map(Peer::endpoint).collect();
        Some(Discovery {
            endpoints: endpoints.into(),
            peers: peers.into(),
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
        let got: Vec<SocketAddr> = targets(&cfg, &snapshot(&[own_v4, own_v6, v6, v6]))
            .iter()
            .map(|t| t.addr)
            .collect();
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
        let delete = purge("").replacen("PURGE", "DELETE", 1);
        let resp = raw(s.addr, &delete).await;
        assert!(resp.starts_with("HTTP/1.1 405"));
        assert!(resp.to_ascii_lowercase().contains("allow: get, purge, ban"));
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

    fn header<'a>(response: &'a str, name: &str) -> Option<&'a str> {
        let head = response.split("\r\n\r\n").next()?;
        head.lines().find_map(|line| {
            let (k, v) = line.split_once(':')?;
            k.eq_ignore_ascii_case(name).then(|| v.trim())
        })
    }

    fn json(response: &str) -> serde_json::Value {
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, b)| b)
            .unwrap_or("");
        serde_json::from_str(body).unwrap_or(serde_json::Value::Null)
    }

    fn states(response: &str) -> Vec<(String, String)> {
        json(response)["peers"]
            .as_array()
            .map(|peers| {
                peers
                    .iter()
                    .map(|p| {
                        let state = p["status"].as_str().unwrap_or_default().to_owned();
                        let reason = p["reason"].as_str().unwrap_or("-").to_owned();
                        (state, reason)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn get(path: &str, auth: bool) -> String {
        let auth = if auth {
            format!("Authorization: Bearer {TOKEN}\r\n")
        } else {
            String::new()
        };
        format!("GET {path} HTTP/1.1\r\nHost: s\r\n{auth}\r\n")
    }

    #[tokio::test]
    async fn writes_return_a_receipt_whose_status_is_authenticated_and_pollable() {
        let (local, _) = peer("127.0.0.1:0", vec![200]).await;
        let s = start(config(local, 8, Duration::from_secs(2)), None).await;
        let resp = raw(s.addr, &purge("")).await;
        assert!(resp.starts_with("HTTP/1.1 202"), "{resp}");
        let location = header(&resp, "location").unwrap().to_owned();
        let id = location.strip_prefix("/invalidations/").unwrap();
        assert_eq!(id.len(), 32);
        assert_eq!(json(&resp)["id"], id);
        assert_eq!(header(&resp, "x-stalewise-pod"), Some("cache-0"));
        assert_eq!(header(&resp, "cache-control"), Some("no-store"));
        assert_eq!(json(&resp)["peers"][0]["pod"], "cache-0");

        let polled = raw(s.addr, &get(&format!("{location}?wait=2s"), true)).await;
        assert!(polled.starts_with("HTTP/1.1 200"), "{polled}");
        assert_eq!(states(&polled), [("delivered".to_owned(), "-".to_owned())]);
        assert_eq!(header(&polled, "location"), Some(location.as_str()));

        let unknown = format!("/invalidations/{}", "0".repeat(32));
        assert!(
            raw(s.addr, &get(&unknown, false))
                .await
                .starts_with("HTTP/1.1 401"),
            "auth precedes lookup"
        );
        assert!(
            raw(s.addr, &get(&unknown, true))
                .await
                .starts_with("HTTP/1.1 404")
        );
        assert!(
            raw(s.addr, &get("/elsewhere", true))
                .await
                .starts_with("HTTP/1.1 404")
        );
        for bad in [
            "/invalidations/xyz".to_owned(),
            format!("{location}?wait=0s"),
            format!("{location}?wait=31s"),
            format!("{location}?wait=1s&wait=2s"),
            format!("{location}?other=1"),
            format!("{location}?wait=2"),
        ] {
            let resp = raw(s.addr, &get(&bad, true)).await;
            assert!(resp.starts_with("HTTP/1.1 400"), "{bad}: {resp}");
        }
        s.stop.cancel();
        s.task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn synchronous_writes_wait_for_every_target_and_never_forward_the_control() {
        let (local, local_seen) = peer("127.0.0.1:0", vec![200]).await;
        let (remote, remote_seen) = peer("127.0.0.1:0", vec![200]).await;
        let s = start(
            config(local, 8, Duration::from_secs(2)),
            snapshot(&[remote]),
        )
        .await;
        let resp = raw(s.addr, &purge("X-Stalewise-Wait: 2s\r\n")).await;
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        assert_eq!(states(&resp).len(), 2);
        assert!(states(&resp).iter().all(|(state, _)| state == "delivered"));
        assert_eq!(json(&resp)["peers"][1]["pod"], "peer-0");
        let got = remote_seen.lock().unwrap()[0].clone();
        assert_eq!(got.path, "/a?b=1", "the object query is untouched");
        assert!(got.headers.get(WAIT_HEADER).is_none());
        for bad in [
            "X-Stalewise-Wait: 2\r\n",
            "X-Stalewise-Wait: 31s\r\n",
            "X-Stalewise-Wait: 1s\r\nX-Stalewise-Wait: 1s\r\n",
        ] {
            assert!(
                raw(s.addr, &purge(bad)).await.starts_with("HTTP/1.1 400"),
                "{bad}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            local_seen.lock().unwrap().len(),
            1,
            "rejected waits queue nothing"
        );
        s.stop.cancel();
        s.task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn wait_reports_current_status_at_timeout_and_terminal_failures_with_reasons() {
        let (local, _) = peer("127.0.0.1:0", vec![200]).await;
        let (slow, _) = peer("127.0.0.1:0", vec![503]).await;
        let s = start(
            config(local, 8, Duration::from_millis(800)),
            snapshot(&[slow]),
        )
        .await;
        let resp = raw(s.addr, &purge("X-Stalewise-Wait: 200ms\r\n")).await;
        assert!(resp.starts_with("HTTP/1.1 504"), "{resp}");
        assert_eq!(states(&resp)[1], ("pending".to_owned(), "-".to_owned()));
        let location = header(&resp, "location").unwrap().to_owned();
        let done = raw(s.addr, &get(&format!("{location}?wait=5s"), true)).await;
        assert!(
            done.starts_with("HTTP/1.1 504"),
            "an exhausted target never reports success"
        );
        assert_eq!(
            states(&done)[1],
            ("exhausted".to_owned(), "deadline".to_owned())
        );
        assert_eq!(s.metrics.invalidation_peers_lagging.get(), 1);
        let metrics = s.metrics.encode().unwrap();
        assert!(
            metrics
                .contains("stalewise_invalidation_latency_seconds_count{result=\"exhausted\"} 1"),
            "{metrics}"
        );
        s.stop.cancel();
        s.task.await.unwrap().unwrap();

        let (local, _) = peer("127.0.0.1:0", vec![403]).await;
        let s = start(config(local, 8, Duration::from_secs(5)), None).await;
        let started = Instant::now();
        let resp = raw(s.addr, &purge("X-Stalewise-Wait: 5s\r\n")).await;
        assert!(resp.starts_with("HTTP/1.1 504"));
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "terminal failures return early"
        );
        assert_eq!(
            states(&resp)[0],
            ("exhausted".to_owned(), "rejected".to_owned())
        );
        s.stop.cancel();
        s.task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn full_history_of_pending_work_rejects_new_writes_without_queueing() {
        let (local, seen) = peer("127.0.0.1:0", vec![503]).await;
        let mut cfg = config(local, 8, Duration::from_millis(600));
        cfg.history = 1;
        let s = start(cfg, None).await;
        assert!(raw(s.addr, &purge("")).await.starts_with("HTTP/1.1 202"));
        let full = raw(s.addr, &purge("")).await;
        assert!(full.starts_with("HTTP/1.1 503"), "{full}");
        assert!(full.contains("history full"));
        eventually(|| s.metrics.signaller_exhausted.get() == 1).await;
        let attempts = seen.lock().unwrap().len();
        assert!(
            raw(s.addr, &purge("")).await.starts_with("HTTP/1.1 202"),
            "terminal records make room"
        );
        assert!(attempts >= 1);
        s.stop.cancel();
        s.task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn deadline_reached_during_shutdown_is_reported_as_shutdown() {
        let request = Arc::new(Invalidation {
            method: HttpMethod::from_bytes(b"PURGE").unwrap(),
            path: "/".into(),
            host: HeaderValue::from_static("a"),
            headers: HeaderMap::new(),
            body: Bytes::new(),
        });
        let delivery = Delivery {
            request,
            id: InvalidationId::random(),
            slot: 0,
            peer: "127.0.0.1:9".parse().unwrap(),
            authority: HeaderValue::from_static("a"),
            deadline: Instant::now(),
        };
        let stop = CancellationToken::new();
        stop.cancel();
        let outcome = deliver(delivery, Duration::from_millis(50), &Metrics::new(), &stop).await;
        assert_eq!(outcome, Outcome::Exhausted(ExhaustReason::Shutdown));
    }

    #[test]
    fn wait_durations_are_positive_and_capped() {
        assert_eq!(parse_wait("30s"), Some(MAX_WAIT));
        assert_eq!(parse_wait("1ms"), Some(Duration::from_millis(1)));
        for bad in ["0ms", "31s", "1m", "", "5", "-1s"] {
            assert_eq!(parse_wait(bad), None, "{bad}");
        }
    }
}
