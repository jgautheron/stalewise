//! End-to-end checks against the real `varnish:9.1.0` image; run with `cargo test -- --ignored`.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Request, Response, StatusCode};
use stalewise::config::Profile;
use stalewise::http::{self, Body, Limits};
use stalewise::render::Template;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

const TOKEN: &str = "integration-token-0123456789abcdef";

fn image() -> String {
    std::env::var("STALEWISE_IMAGE").unwrap_or_else(|_| "stalewise:it".into())
}

fn docker(args: &[&str]) -> String {
    let out = Command::new("docker")
        .args(args)
        .output()
        .expect("docker runs");
    assert!(
        out.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

fn build_image_once() {
    static BUILT: std::sync::Once = std::sync::Once::new();
    BUILT.call_once(|| {
        if std::env::var("STALEWISE_IMAGE").is_err() {
            docker(&[
                "build",
                "-q",
                "-t",
                "stalewise:it",
                env!("CARGO_MANIFEST_DIR"),
            ]);
        }
    });
}

/// Origin state: per-path hit counts, the last Host header seen, and paths forced to 500.
#[derive(Default)]
struct Origin {
    hits: HashMap<String, usize>,
    last_host: Option<String>,
    failing: HashSet<String>,
}

type SharedOrigin = Arc<Mutex<Origin>>;

fn route(path: &str, n: usize) -> (StatusCode, Vec<(&'static str, String)>, String) {
    let body = format!("{path} #{n}");
    let cc = |v: &str| vec![("cache-control", v.to_owned())];
    match path {
        "/cache" | "/" => (StatusCode::OK, cc("max-age=60"), body),
        "/stale" => (
            StatusCode::OK,
            cc("max-age=1, stale-while-revalidate=30"),
            body,
        ),
        "/private" => (
            StatusCode::OK,
            vec![
                ("cache-control", "max-age=60".into()),
                ("set-cookie", "a=b".into()),
            ],
            body,
        ),
        "/surrogate" => (
            StatusCode::OK,
            vec![
                ("cache-control", "max-age=0".into()),
                ("surrogate-control", "max-age=60".into()),
            ],
            body,
        ),
        "/nofresh" => (StatusCode::OK, vec![], body),
        "/flaky" => (StatusCode::OK, cc("max-age=1, stale-if-error=120"), body),
        "/aged" => (
            StatusCode::OK,
            vec![
                (
                    "cache-control",
                    "max-age=60, stale-while-revalidate=300".into(),
                ),
                ("age", "600".into()),
            ],
            body,
        ),
        "/events" => (
            StatusCode::OK,
            vec![
                ("cache-control", "max-age=60".into()),
                ("content-type", "text/event-stream".into()),
            ],
            body,
        ),
        "/tagged" => (
            StatusCode::OK,
            vec![
                ("cache-control", "max-age=60".into()),
                ("surrogate-key", "alpha beta".into()),
            ],
            body,
        ),
        _ => (StatusCode::NOT_FOUND, vec![], body),
    }
}

async fn origin_handler(origin: SharedOrigin, req: Request<Incoming>) -> Response<Body> {
    let path = req.uri().path().to_owned();
    let host = req
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .map(str::to_owned);
    let _ = req.into_body().collect().await;
    let (n, failing) = {
        let mut o = origin.lock().unwrap();
        o.last_host = host;
        let failing = o.failing.contains(&path);
        let n = o.hits.entry(path.clone()).or_default();
        *n += 1;
        (*n, failing)
    };
    if failing {
        return http::text(
            StatusCode::INTERNAL_SERVER_ERROR,
            Bytes::from("origin error"),
        );
    }
    let (status, headers, body) = route(&path, n);
    let mut resp = http::text(status, Bytes::from(body));
    for (k, v) in headers {
        resp.headers_mut().append(
            HeaderName::from_static(k),
            HeaderValue::from_str(&v).unwrap(),
        );
    }
    resp
}

async fn start_origin() -> (u16, SharedOrigin) {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let origin: SharedOrigin = Arc::default();
    let state = Arc::clone(&origin);
    let limits = Limits {
        header_timeout: Duration::from_secs(5),
        connection_timeout: Duration::from_secs(30),
    };
    tokio::spawn(http::serve(
        listener,
        limits,
        move |req| origin_handler(Arc::clone(&state), req),
        CancellationToken::new(),
    ));
    (port, origin)
}

struct Reply {
    status: u16,
    headers: HashMap<String, String>,
    body: String,
}

impl Reply {
    fn is_hit(&self) -> bool {
        self.headers
            .get("x-varnish")
            .is_some_and(|v| v.split_whitespace().count() == 2)
    }
}

async fn request(addr: SocketAddr, method: &str, path: &str, extra: &str) -> Reply {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: Catalog.Test\r\nConnection: close\r\n{extra}Content-Length: 0\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    Reply {
        status,
        headers,
        body: body.to_owned(),
    }
}

fn mapped(container: &str, port: &str) -> SocketAddr {
    let out = docker(&["port", container, port]);
    let first = out
        .lines()
        .find(|l| l.starts_with("127.0.0.1"))
        .unwrap_or_default();
    first.parse().unwrap()
}

async fn wait_for(what: &str, timeout: Duration, mut check: impl AsyncFnMut() -> bool) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("timed out waiting for {what}");
}

struct Container {
    id: String,
    traffic: SocketAddr,
    management: SocketAddr,
    signaller: SocketAddr,
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.id]).output();
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("stalewise-it-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::set_permissions(&dir, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    dir
}

async fn run_cache(config_dir: &Path, args: &[String]) -> Container {
    run_cache_with(config_dir, &[], args).await
}

async fn run_cache_with(config_dir: &Path, docker_extra: &[String], args: &[String]) -> Container {
    build_image_once();
    let mount = format!("{}:/config:ro", config_dir.display());
    let mut argv: Vec<String> = [
        "run",
        "-d",
        "--read-only",
        "--tmpfs",
        "/var/lib/stalewise:uid=1000,gid=1000,mode=0700",
        "--tmpfs",
        "/var/lib/varnish:uid=1000,gid=1000,mode=0755,exec",
        "--tmpfs",
        "/tmp:mode=1777,exec",
        "--add-host",
        "host.docker.internal:host-gateway",
        "-p",
        "127.0.0.1::8080",
        "-p",
        "127.0.0.1::8081",
        "-p",
        "127.0.0.1::9090",
        "-v",
        &mount,
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    argv.extend_from_slice(docker_extra);
    argv.push(image());
    argv.extend_from_slice(args);
    let refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    let id = docker(&refs);
    let c = Container {
        traffic: mapped(&id, "8080/tcp"),
        signaller: mapped(&id, "8081/tcp"),
        management: mapped(&id, "9090/tcp"),
        id,
    };
    let management = c.management;
    let id = c.id.clone();
    wait_for("readiness", Duration::from_secs(90), async || {
        let state = docker(&["inspect", "-f", "{{.State.Running}}", &id]);
        assert_eq!(state, "true", "container exited: {}", logs(&id));
        request(management, "GET", "/readyz", "").await.status == 200
    })
    .await;
    c
}

fn logs(id: &str) -> String {
    let out = Command::new("docker").args(["logs", id]).output().unwrap();
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn base_args(origin_port: u16) -> Vec<String> {
    [
        "--frontend-service=cache-peers",
        "--frontend-service-host=cache.test",
        "--backend-watch=false",
        "--backend-host=host.docker.internal",
        "--signaller",
        "--signaller-token-file=/config/token",
        "--pod-ip=10.255.255.1",
        "--log-level=info",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .chain([format!("--backend-port={origin_port}")])
    .collect()
}

async fn warm(addr: SocketAddr) {
    wait_for("origin health", Duration::from_secs(30), async || {
        request(addr, "GET", "/", "").await.status == 200
    })
    .await;
}

fn hits(origin: &SharedOrigin, path: &str) -> usize {
    origin.lock().unwrap().hits.get(path).copied().unwrap_or(0)
}

async fn metric(addr: SocketAddr, name: &str) -> f64 {
    let body = request(addr, "GET", "/metrics", "").await.body;
    body.lines()
        .find(|l| l.starts_with(name))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn generic_profile_caches_purges_reloads_and_serves_stale() {
    let (port, origin) = start_origin().await;
    let dir = temp_dir("generic");
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    let template = Template::embedded(Profile::Generic);
    std::fs::write(dir.join("template.vcl"), template.source.as_bytes()).unwrap();
    let mut args = base_args(port);
    args.extend([
        "--template=/config/template.vcl".into(),
        "--template-poll=250ms".into(),
    ]);
    let c = run_cache(&dir, &args).await;
    warm(c.traffic).await;

    let miss = request(c.traffic, "GET", "/cache", "").await;
    let hit = request(c.traffic, "GET", "/cache", "").await;
    assert_eq!((miss.status, miss.is_hit()), (200, false));
    assert!(hit.is_hit(), "second request is a hit");
    assert_eq!(hit.body, miss.body);
    assert_eq!(hits(&origin, "/cache"), 1);

    let auth = format!("Authorization: Bearer {TOKEN}\r\n");
    assert_eq!(
        request(c.signaller, "PURGE", "/cache", &auth).await.status,
        202
    );
    wait_for("purge delivery", Duration::from_secs(5), async || {
        !request(c.traffic, "GET", "/cache", "").await.is_hit()
    })
    .await;
    assert_eq!(hits(&origin, "/cache"), 2);
    assert_eq!(
        request(c.signaller, "PURGE", "/cache", "").await.status,
        401
    );

    for path in ["/private", "/nofresh", "/events"] {
        request(c.traffic, "GET", path, "").await;
        let again = request(c.traffic, "GET", path, "").await;
        assert!(!again.is_hit(), "{path} must not be stored");
        assert_eq!(hits(&origin, path), 2, "{path}");
    }
    request(c.traffic, "GET", "/cache", "Cookie: a=b\r\n").await;
    assert_eq!(hits(&origin, "/cache"), 3, "cookie requests pass");

    request(c.traffic, "GET", "/surrogate", "").await;
    let surrogate = request(c.traffic, "GET", "/surrogate", "").await;
    assert!(surrogate.is_hit());
    assert!(!surrogate.headers.contains_key("surrogate-control"));
    assert_eq!(
        surrogate.headers.get("cache-control").map(String::as_str),
        Some("max-age=0")
    );

    request(c.traffic, "GET", "/tagged", "").await;
    let tagged = request(c.traffic, "GET", "/tagged", "").await;
    assert!(tagged.is_hit());
    assert!(!tagged.headers.contains_key("surrogate-key"));
    let ban = format!("{auth}X-Stalewise-Tag: beta\r\n");
    assert_eq!(request(c.signaller, "BAN", "/", &ban).await.status, 202);
    wait_for("ban delivery", Duration::from_secs(5), async || {
        !request(c.traffic, "GET", "/tagged", "").await.is_hit()
    })
    .await;

    let first = request(c.traffic, "GET", "/stale", "").await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let stale = request(c.traffic, "GET", "/stale", "").await;
    assert_eq!(
        stale.body, first.body,
        "expired object served within stale-while-revalidate"
    );
    wait_for("background refresh", Duration::from_secs(5), async || {
        hits(&origin, "/stale") == 2
    })
    .await;
    let fresh = request(c.traffic, "GET", "/stale", "").await;
    assert_ne!(fresh.body, first.body);

    let warm_hits = hits(&origin, "/cache");
    request(c.traffic, "GET", "/cache", "").await;
    std::fs::write(
        dir.join("template.vcl"),
        format!("{}\n# revision two\n", template.source),
    )
    .unwrap();
    wait_for("reload", Duration::from_secs(10), async || {
        metric(c.management, "stalewise_reload_successes_total").await >= 1.0
    })
    .await;
    assert!(
        request(c.traffic, "GET", "/cache", "").await.is_hit(),
        "cache survives reload"
    );
    assert_eq!(
        hits(&origin, "/cache"),
        warm_hits,
        "no origin fetch across the reload"
    );

    std::fs::write(dir.join("template.vcl"), "vcl 4.1; this does not compile").unwrap();
    wait_for("compile failure", Duration::from_secs(10), async || {
        request(c.management, "GET", "/readyz", "")
            .await
            .body
            .contains("VCL compilation failed")
    })
    .await;
    assert_eq!(
        request(c.management, "GET", "/readyz", "").await.status,
        200
    );
    assert!(
        request(c.traffic, "GET", "/cache", "").await.is_hit(),
        "last good VCL keeps serving"
    );

    let code = docker(&["stop", "-t", "40", &c.id]);
    assert_eq!(code, c.id);
    let exit = docker(&["inspect", "-f", "{{.State.ExitCode}}", &c.id]);
    assert_eq!(exit, "0", "graceful shutdown exits zero: {}", logs(&c.id));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn mesh_profile_rewrites_authority_and_child_death_is_fatal() {
    let (port, origin) = start_origin().await;
    let dir = temp_dir("mesh");
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    let mut args = base_args(port);
    args.retain(|a| a != "--backend-watch=false");
    args.extend([
        "--profile=mesh".into(),
        "--canonical-host=catalog.test".into(),
    ]);
    let c = run_cache(&dir, &args).await;
    warm(c.traffic).await;
    request(c.traffic, "GET", "/cache", "").await;
    assert!(request(c.traffic, "GET", "/cache", "").await.is_hit());
    let host = origin.lock().unwrap().last_host.clone();
    assert_eq!(
        host.as_deref(),
        Some(format!("host.docker.internal:{port}").as_str())
    );
    assert_eq!(
        request(c.traffic, "GET", "/__stalewise_probe", "")
            .await
            .status,
        404
    );

    let kill = "for p in /proc/[0-9]*; do [ \"$(cat $p/comm 2>/dev/null)\" = varnishd ] && kill -KILL ${p#/proc/}; done; true";
    // The exec can lose its container mid-command, so only the container's exit code is checked.
    let _ = Command::new("docker")
        .args(["exec", &c.id, "sh", "-c", kill])
        .output();
    let exit = docker(&["wait", &c.id]);
    assert_eq!(exit, "1", "child death exits nonzero: {}", logs(&c.id));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn origin_5xx_then_sick_keeps_serving_stale_if_error() {
    let (port, origin) = start_origin().await;
    let dir = temp_dir("stale-if-error");
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    let c = run_cache(&dir, &base_args(port)).await;
    warm(c.traffic).await;
    let first = request(c.traffic, "GET", "/flaky", "").await;
    assert_eq!(first.body, "/flaky #1");
    origin.lock().unwrap().failing.insert("/flaky".into());
    tokio::time::sleep(Duration::from_millis(2500)).await;

    for _ in 0..3 {
        let stale = request(c.traffic, "GET", "/flaky", "").await;
        assert_eq!(
            (stale.status, stale.body.as_str()),
            (200, "/flaky #1"),
            "a refetch 5xx serves stale and stores no hit-for-miss"
        );
    }
    assert!(hits(&origin, "/flaky") >= 2, "the origin was asked again");

    origin.lock().unwrap().failing.insert("/".into());
    let mut previous = hits(&origin, "/flaky");
    wait_for("origin marked sick", Duration::from_secs(30), async || {
        let stale = request(c.traffic, "GET", "/flaky", "").await;
        assert_eq!((stale.status, stale.body.as_str()), (200, "/flaky #1"));
        let now = hits(&origin, "/flaky");
        let quiet = now == previous;
        previous = now;
        quiet
    })
    .await;
    let stale = request(c.traffic, "GET", "/flaky", "").await;
    assert_eq!((stale.status, stale.body.as_str()), (200, "/flaky #1"));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn age_beyond_max_age_never_restarts_stale_while_revalidate() {
    let (port, origin) = start_origin().await;
    let dir = temp_dir("aged");
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    let c = run_cache(&dir, &base_args(port)).await;
    warm(c.traffic).await;
    request(c.traffic, "GET", "/aged", "").await;
    let again = request(c.traffic, "GET", "/aged", "").await;
    assert!(!again.is_hit(), "allowance already spent upstream");
    assert_eq!(hits(&origin, "/aged"), 2);
}

const PEER_SUBNET: &str = "10.213.77.0/24";
const PEER_A: &str = "10.213.77.10";
const PEER_B: &str = "10.213.77.11";
const PEER_AUTHORITY: &str = "cache-peers.default.svc.cluster.local:8080";

struct Network(String);

impl Drop for Network {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["network", "rm", &self.0])
            .output();
    }
}

/// Minimal Kubernetes API: one EndpointSlice listing both peers; watches idle, then end.
async fn start_fake_kube(peers: &[&str]) -> u16 {
    let addresses: Vec<String> = peers
        .iter()
        .map(|ip| format!(r#"{{"addresses":["{ip}"],"conditions":{{"ready":true}}}}"#))
        .collect();
    let list = format!(
        r#"{{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","metadata":{{"resourceVersion":"1"}},"items":[{{"metadata":{{"name":"cache-peers-x","namespace":"default","resourceVersion":"1","labels":{{"kubernetes.io/service-name":"cache-peers"}}}},"addressType":"IPv4","endpoints":[{}],"ports":[{{"name":"http","port":8080,"protocol":"TCP"}}]}}]}}"#,
        addresses.join(",")
    );
    let list = Bytes::from(list);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let limits = Limits {
        header_timeout: Duration::from_secs(5),
        connection_timeout: Duration::from_secs(60),
    };
    tokio::spawn(http::serve(
        listener,
        limits,
        move |req: Request<Incoming>| {
            let list = list.clone();
            async move {
                let watching = req.uri().query().is_some_and(|q| q.contains("watch=true"));
                let body = if watching {
                    tokio::time::sleep(Duration::from_secs(20)).await;
                    Bytes::new()
                } else {
                    list
                };
                let mut resp = http::text(StatusCode::OK, body);
                resp.headers_mut().insert(
                    HeaderName::from_static("content-type"),
                    HeaderValue::from_static("application/json"),
                );
                resp
            }
        },
        CancellationToken::new(),
    ));
    port
}

fn kubeconfig(port: u16) -> String {
    format!(
        "apiVersion: v1\nkind: Config\ncurrent-context: it\nclusters:\n- name: it\n  cluster:\n    server: http://host.docker.internal:{port}\ncontexts:\n- name: it\n  context:\n    cluster: it\n    user: it\n    namespace: default\nusers:\n- name: it\n  user:\n    token: unused\n"
    )
}

fn peer_args(origin_port: u16, ip: &str) -> Vec<String> {
    base_args(origin_port)
        .into_iter()
        .filter(|a| !a.starts_with("--pod-ip"))
        .chain([
            format!("--pod-ip={ip}"),
            format!("--peer-authority={PEER_AUTHORITY}"),
        ])
        .collect()
}

async fn run_peer(dir: &Path, network: &str, ip: &str, origin_port: u16) -> Container {
    let docker_extra: Vec<String> = [
        "--network",
        network,
        "--ip",
        ip,
        "-e",
        "KUBECONFIG=/config/kubeconfig",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .collect();
    run_cache_with(dir, &docker_extra, &peer_args(origin_port, ip)).await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn peer_purge_uses_the_peer_authority_and_honours_only_trusted_invalidation_hosts() {
    let (port, origin) = start_origin().await;
    let kube_port = start_fake_kube(&[PEER_A, PEER_B]).await;
    let dir = temp_dir("peers");
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    std::fs::write(dir.join("kubeconfig"), kubeconfig(kube_port)).unwrap();
    let name = format!("stalewise-it-peers-{}", std::process::id());
    docker(&["network", "create", "--subnet", PEER_SUBNET, &name]);
    let network = Network(name);
    let a = run_peer(&dir, &network.0, PEER_A, port).await;
    let b = run_peer(&dir, &network.0, PEER_B, port).await;
    // Readiness precedes the first discovery-rendered VCL, which is what admits peers.
    for c in [&a, &b] {
        wait_for("peer-aware VCL", Duration::from_secs(30), async || {
            logs(&c.id).contains("activated VCL")
        })
        .await;
    }
    warm(b.traffic).await;

    request(b.traffic, "GET", "/cache", "").await;
    let spoof = "X-Stalewise-Host: other.test\r\n";
    assert!(
        request(b.traffic, "GET", "/cache", spoof).await.is_hit(),
        "a client X-Stalewise-Host never changes the cache key"
    );
    assert_eq!(
        request(b.traffic, "PURGE", "/cache", spoof).await.status,
        403,
        "a non-ACL client cannot purge with or without the header"
    );
    assert!(request(b.traffic, "GET", "/cache", "").await.is_hit());
    let before = hits(&origin, "/cache");

    let auth = format!("Authorization: Bearer {TOKEN}\r\n");
    assert_eq!(
        request(a.signaller, "PURGE", "/cache", &auth).await.status,
        202
    );
    wait_for("peer purge delivery", Duration::from_secs(10), async || {
        !request(b.traffic, "GET", "/cache", "").await.is_hit()
    })
    .await;
    assert_eq!(hits(&origin, "/cache"), before + 1);
    assert_eq!(
        metric(
            a.management,
            "stalewise_signaller_exhausted_deliveries_total"
        )
        .await,
        0.0,
        "{}",
        logs(&a.id)
    );
}
