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

#[allow(dead_code)]
mod common;
use common::*;

/// Origin state: per-path hit counts, Host and raw targets seen, and paths forced to 500.
#[derive(Default)]
struct Origin {
    hits: HashMap<String, usize>,
    last_host: Option<String>,
    failing: HashSet<String>,
    targets: Vec<(String, Option<String>)>,
}

type SharedOrigin = Arc<Mutex<Origin>>;

fn route(path: &str, n: usize) -> (StatusCode, Vec<(&'static str, String)>, String) {
    let body = format!("{path} #{n}");
    let cc = |v: &str| vec![("cache-control", v.to_owned())];
    if ["/slow/", "/shard/", "/echo/", "/hop/", "/fb/"]
        .iter()
        .any(|p| path.starts_with(p))
    {
        return (StatusCode::OK, cc("max-age=60"), body);
    }
    if path.starts_with("/ltag/") {
        let mut headers = cc("max-age=60");
        headers.push(("surrogate-key", "gamma".into()));
        return (StatusCode::OK, headers, body);
    }
    if path.starts_with("/deny/") {
        return (StatusCode::FORBIDDEN, vec![], body);
    }
    match path {
        "/spoofed" => (
            StatusCode::OK,
            vec![
                ("cache-control", "max-age=60".into()),
                ("x-stalewise-peer-ttl", "999".into()),
                ("x-stalewise-peer-tags", ",forged,".into()),
            ],
            body,
        ),
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
    let target = req.uri().to_string();
    let host = req
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .map(str::to_owned);
    let _ = req.into_body().collect().await;
    if path.starts_with("/slow/") {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let (n, failing) = {
        let mut o = origin.lock().unwrap();
        o.targets.push((target, host.clone()));
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
    // Keep-alive, unlike `http::serve`: a fresh backend connection per fetch churns Docker
    // Desktop's host gateway, which then fails connects with ENETUNREACH.
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let state = Arc::clone(&state);
            let service = hyper::service::service_fn(move |req| {
                let response = origin_handler(Arc::clone(&state), req);
                async move { Ok::<_, std::convert::Infallible>(response.await) }
            });
            tokio::spawn(
                hyper::server::conn::http1::Builder::new()
                    .timer(hyper_util::rt::TokioTimer::new())
                    .header_read_timeout(Duration::from_secs(30))
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service),
            );
        }
    });
    (port, origin)
}

fn hits(origin: &SharedOrigin, path: &str) -> usize {
    origin.lock().unwrap().hits.get(path).copied().unwrap_or(0)
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
async fn sigterm_reports_draining_keeps_serving_for_the_delay_then_exits_zero() {
    let (port, _origin) = start_origin().await;
    let dir = temp_dir("drain");
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    let delay = Duration::from_secs(4);
    let mut args = base_args(port);
    args.push(format!("--drain-delay={}s", delay.as_secs()));
    let c = run_cache(&dir, &args).await;
    warm(c.traffic).await;

    docker(&["kill", "--signal", "TERM", &c.id]);
    let signalled = Instant::now();
    let readyz = request(c.management, "GET", "/readyz", "").await;
    assert!(signalled.elapsed() < Duration::from_secs(1));
    assert_eq!(readyz.status, 503, "{}", readyz.body);
    assert!(readyz.body.contains("draining"), "{}", readyz.body);
    while signalled.elapsed() < delay - Duration::from_millis(500) {
        let reply = request(c.traffic, "GET", "/cache", "").await;
        assert_eq!(reply.status, 200, "traffic served while draining");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let exit = docker(&["wait", &c.id]);
    assert!(
        signalled.elapsed() >= delay,
        "varnishd outlived the drain delay"
    );
    assert_eq!(exit, "0", "drained shutdown exits zero: {}", logs(&c.id));
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

/// Minimal Kubernetes API: one EndpointSlice listing every peer Pod; watches idle, then end.
async fn start_fake_kube(peers: &[&str]) -> u16 {
    let addresses: Vec<String> = peers
        .iter()
        .enumerate()
        .map(|(i, ip)| {
            format!(
                r#"{{"addresses":["{ip}"],"conditions":{{"ready":true}},"targetRef":{{"kind":"Pod","namespace":"default","name":"cache-{i}"}}}}"#
            )
        })
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

fn peer_args(origin_port: u16, ip: &str, pod: &str) -> Vec<String> {
    base_args(origin_port)
        .into_iter()
        .filter(|a| !a.starts_with("--pod-ip") && !a.starts_with("--pod-name"))
        .chain([
            format!("--pod-ip={ip}"),
            format!("--pod-name={pod}"),
            format!("--peer-authority={PEER_AUTHORITY}"),
        ])
        .collect()
}

async fn run_peer(dir: &Path, network: &str, ip: &str, origin_port: u16) -> Container {
    let pod = if ip == PEER_A { "cache-0" } else { "cache-1" };
    run_peer_with(dir, network, ip, peer_args(origin_port, ip, pod)).await
}

async fn run_peer_with(dir: &Path, network: &str, ip: &str, args: Vec<String>) -> Container {
    run_peer_docker(dir, network, ip, args, &[]).await
}

async fn run_peer_docker(
    dir: &Path,
    network: &str,
    ip: &str,
    args: Vec<String>,
    extra: &[&str],
) -> Container {
    let docker_extra: Vec<String> = [
        "--network",
        network,
        "--ip",
        ip,
        "-e",
        "KUBECONFIG=/config/kubeconfig",
    ]
    .iter()
    .chain(extra)
    .map(|s| (*s).to_owned())
    .collect();
    run_cache_with(dir, &docker_extra, &args).await
}

fn ttl_policies() -> Vec<stalewise::policy::RenderPolicy> {
    use stalewise::policy::{self, Candidate, OwnService};
    let own = OwnService {
        name: "catalog".into(),
        port_name: None,
        port_number: None,
    };
    let candidates: Vec<Candidate> = [
        (
            "nofresh",
            "/nofresh",
            "{ttlSeconds: 60, cookies: {mode: stripListed, names: [analytics]}}\nbypass: [{header: {name: X-Preview, exact: \"true\"}}]",
        ),
        ("query", "/q", "{ttlSeconds: 60, key: {queryAllowList: [page]}}"),
        ("asset", "/asset", "{ttlSeconds: 60, key: {queryAllowList: []}}"),
        (
            "shop",
            "/shop",
            "{ttlSeconds: 60}\nbypass: [{pathPrefix: /shop/account}]",
        ),
        (
            "tenant",
            "/tenant",
            "{ttlSeconds: 60, cacheableMethods: [HEAD], key: {headers: [X-Tenant]}}\nbypass: [{header: {name: X-Preview, exact: \"true\"}}]",
        ),
        ("private", "/private", "{ttlSeconds: 60}"),
        ("cache", "/cache", "{ttlSeconds: 0}"),
    ]
    .iter()
    .map(|(name, prefix, overrides)| {
        let yaml = format!("serviceRef: {{name: catalog}}\npathPrefix: {prefix}\noverrides: {overrides}\n");
        let spec = serde_norway::from_str(&yaml).unwrap();
        Candidate {
            name: (*name).into(),
            generation: 1,
            outcome: policy::validate(name, 1, &spec, &own),
        }
    })
    .collect();
    policy::resolve(&candidates, &std::collections::BTreeMap::new()).policies
}

/// Fully rendered generic VCL; it contains no template syntax, so stalewise passes it through.
fn rendered_policy_vcl(origin_port: u16) -> String {
    use stalewise::endpoint::{Endpoint, Host};
    use stalewise::render::{self, Context, RenderSettings};
    let settings = RenderSettings {
        env: std::collections::BTreeMap::new(),
        canonical_host: None,
        backend_authority: format!("host.docker.internal:{origin_port}")
            .parse()
            .unwrap(),
        purge_acl: vec!["127.0.0.1".parse().unwrap()],
        peer_purge: false,
        probe_path: "/".into(),
        uncacheable_ttl: Duration::from_secs(30),
        max_object_size: 10_485_760,
        strip_rewrite_headers: true,
        shard: None,
    };
    let origin = Endpoint::new(
        Host::Dns("host.docker.internal".parse().unwrap()),
        origin_port,
    );
    let ctx = Context::new(&settings, &[], &[origin], &ttl_policies()).unwrap();
    let source = Template::embedded(Profile::Generic).source;
    String::from_utf8(render::render(&source, &ctx).unwrap()).unwrap()
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

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn cache_policy_ttl_overrides_a_headerless_response_but_not_privacy() {
    let (port, origin) = start_origin().await;
    let dir = temp_dir("policy-ttl");
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    std::fs::write(dir.join("template.vcl"), rendered_policy_vcl(port)).unwrap();
    let mut args = base_args(port);
    args.push("--template=/config/template.vcl".into());
    let c = run_cache(&dir, &args).await;
    warm(c.traffic).await;

    request(c.traffic, "GET", "/nofresh", "").await;
    let again = request(c.traffic, "GET", "/nofresh", "").await;
    assert!(
        again.is_hit(),
        "policy TTL stores a response without freshness headers"
    );
    assert_eq!(hits(&origin, "/nofresh"), 1);
    assert!(!again.headers.contains_key("x-stalewise-policy"));
    let stripped = request(c.traffic, "GET", "/nofresh", "Cookie: analytics=1\r\n").await;
    assert!(
        stripped.is_hit(),
        "listed cookies are stripped before the pass check"
    );
    let kept = request(
        c.traffic,
        "GET",
        "/nofresh",
        "Cookie: analytics=1; session=2\r\n",
    )
    .await;
    assert!(!kept.is_hit(), "undeclared cookies still pass");
    let preview = request(c.traffic, "GET", "/nofresh", "X-Preview: true\r\n").await;
    assert!(!preview.is_hit(), "bypass rules win before lookup");
    assert_eq!(hits(&origin, "/nofresh"), 3);

    request(c.traffic, "GET", "/q?utm=a&page=2", "").await;
    let same_key = request(c.traffic, "GET", "/q?page=2&utm=b", "").await;
    assert!(
        same_key.is_hit(),
        "only allow-listed query names enter the key"
    );
    let other_page = request(c.traffic, "GET", "/q?page=3", "").await;
    assert!(!other_page.is_hit());

    for path in ["/private", "/cache"] {
        request(c.traffic, "GET", path, "").await;
        let again = request(c.traffic, "GET", path, "").await;
        assert!(
            !again.is_hit(),
            "{path}: Set-Cookie and explicit zero never store"
        );
        assert_eq!(hits(&origin, path), 2, "{path}");
    }
}

struct Cluster {
    nodes: Vec<Container>,
    ips: Vec<String>,
    origin: SharedOrigin,
    _network: Network,
}

/// Lines in the confirmed active VCL that add a peer to the ring.
fn ring_size(c: &Container) -> usize {
    let out = Command::new("docker")
        .args([
            "exec",
            &c.id,
            "grep",
            "-c",
            "add_backend(peer_",
            "/var/lib/stalewise/run/active.vcl",
        ])
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0)
}

/// Three sharded caches on one network, discovered through the fake API as cache-0..2.
async fn start_cluster(name: &str, prefix: &str, extra: &[&str]) -> Cluster {
    let (port, origin) = start_origin().await;
    let ips: Vec<String> = (10..13).map(|n| format!("{prefix}.{n}")).collect();
    let refs: Vec<&str> = ips.iter().map(String::as_str).collect();
    let kube_port = start_fake_kube(&refs).await;
    let dir = temp_dir(name);
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    std::fs::write(dir.join("kubeconfig"), kubeconfig(kube_port)).unwrap();
    let net = format!("stalewise-it-{name}-{}", std::process::id());
    docker(&[
        "network",
        "create",
        "--subnet",
        &format!("{prefix}.0/24"),
        &net,
    ]);
    let network = Network(net);
    let mut nodes = Vec::new();
    for (i, ip) in ips.iter().enumerate() {
        let mut args = peer_args(port, ip, &format!("cache-{i}"));
        args.extend(
            [
                "--shard",
                "--peer-timeout=1s",
                "--membership-debounce=100ms",
                "--delivery-deadline=3s",
            ]
            .iter()
            .chain(extra)
            .map(|s| (*s).to_owned()),
        );
        nodes.push(run_peer_with(&dir, &network.0, ip, args).await);
    }
    for c in &nodes {
        wait_for("three-peer ring", Duration::from_secs(60), async || {
            ring_size(c) == 3
        })
        .await;
    }
    for c in &nodes {
        warm(c.traffic).await;
    }
    Cluster {
        nodes,
        ips,
        origin,
        _network: network,
    }
}

/// Runs curl inside `from`, so the request leaves a discovered peer address; returns status and headers.
fn curl_from(from: &Container, url: &str, headers: &[&str]) -> (u16, String) {
    let mut args = vec![
        "exec",
        &from.id,
        "curl",
        "-s",
        "-o",
        "/dev/null",
        "-D",
        "-",
        "-m",
        "10",
    ];
    for h in headers {
        args.extend(["-H", h]);
    }
    args.push(url);
    let out = Command::new("docker").args(&args).output().unwrap();
    let head = String::from_utf8_lossy(&out.stdout).to_ascii_lowercase();
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    (status, head)
}

async fn concurrent(addrs: &[SocketAddr], path: &str, per_node: usize) -> Vec<Reply> {
    let mut tasks = Vec::new();
    for _ in 0..per_node {
        for addr in addrs {
            let (addr, path) = (*addr, path.to_owned());
            tasks.push(tokio::spawn(async move {
                request(addr, "GET", &path, "").await
            }));
        }
    }
    let mut replies = Vec::new();
    for task in tasks {
        replies.push(task.await.unwrap());
    }
    replies
}

fn status_of(reply: &Reply) -> Vec<String> {
    let doc: serde_json::Value = serde_json::from_str(&reply.body).unwrap_or_default();
    doc["peers"]
        .as_array()
        .map(|peers| {
            peers
                .iter()
                .map(|p| p["status"].as_str().unwrap_or_default().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn three_sharded_caches_fill_once_forward_one_hop_and_fall_back_to_origin() {
    let cluster = start_cluster("shard", "10.213.78", &[]).await;
    let nodes = &cluster.nodes;
    let traffic: Vec<SocketAddr> = nodes.iter().map(|c| c.traffic).collect();
    let origin = &cluster.origin;

    for key in [
        "/slow/a", "/slow/b", "/slow/c", "/slow/d", "/slow/e", "/slow/f",
    ] {
        let replies = concurrent(&traffic, key, 10).await;
        assert!(replies.iter().all(|r| r.status == 200), "{key}");
        assert!(
            replies.iter().all(|r| r.body == replies[0].body),
            "{key}: one representation"
        );
        assert_eq!(
            hits(origin, key),
            1,
            "{key}: 30 concurrent cold requests through 3 caches fill once"
        );
        concurrent(&traffic, key, 2).await;
        assert_eq!(hits(origin, key), 1, "{key}: warm requests never refill");
        let leaked = replies
            .iter()
            .flat_map(|r| r.headers.keys())
            .find(|k| k.starts_with("x-stalewise"));
        assert_eq!(leaked, None, "internal headers never reach clients");
    }

    let raw = "?b=2&a=%2F&a=1&x=&%7E=~";
    for i in 0..3 {
        let path = format!("/echo/{i}{raw}");
        for addr in &traffic {
            assert_eq!(request(*addr, "GET", &path, "").await.status, 200);
        }
        let seen: Vec<_> = origin
            .lock()
            .unwrap()
            .targets
            .iter()
            .filter(|(t, _)| t.starts_with(&format!("/echo/{i}?")))
            .cloned()
            .collect();
        assert_eq!(seen.len(), 1, "one fill per object");
        assert_eq!(
            seen[0].0, path,
            "the request target reaches the origin byte for byte"
        );
        assert_eq!(
            seen[0].1.as_deref(),
            Some("catalog.test"),
            "the original authority is restored"
        );
    }

    for i in 0..6 {
        let key = format!("/deny/{i}");
        let reply = request(traffic[0], "GET", &key, "").await;
        assert_eq!(reply.status, 403, "{key}: an origin 403 is delivered as-is");
        assert!(!reply.headers.keys().any(|k| k.starts_with("x-stalewise")));
        assert_eq!(
            hits(origin, &key),
            1,
            "{key}: an origin 403 is never retried"
        );
    }

    for addr in &traffic {
        let spoofed = request(*addr, "GET", "/spoofed", "").await;
        assert!(!spoofed.headers.keys().any(|k| k.starts_with("x-stalewise")));
        let forged = request(
            *addr,
            "GET",
            "/shard/forged",
            "X-Stalewise-Shard-Hop: 1\r\nX-Stalewise-Host: other.test\r\n",
        )
        .await;
        assert_eq!(
            forged.status, 403,
            "untrusted clients cannot send hop headers"
        );
    }
    assert_eq!(hits(origin, "/shard/forged"), 0);

    let host = "X-Stalewise-Host: catalog.test";
    let hop = "X-Stalewise-Shard-Hop: 1";
    let authority = format!("Host: {PEER_AUTHORITY}");
    for ip in &cluster.ips {
        let url = format!("http://{ip}:8080/hop/one");
        let (status, head) = curl_from(&nodes[0], &url, &[&authority, hop, host]);
        assert_eq!(status, 200, "{head}");
        let ttl: u64 = head
            .lines()
            .find_map(|l| l.strip_prefix("x-stalewise-peer-ttl:"))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        assert!(
            (1..=60).contains(&ttl),
            "trusted forwards learn the owner's freshness: {head}"
        );
        assert!(head.contains("x-stalewise-peer-tags: ,"), "{head}");
    }
    assert_eq!(
        hits(origin, "/hop/one"),
        3,
        "a forwarded request is served locally, never forwarded again"
    );
    let target = format!("http://{}:8080/hop/two", cluster.ips[1]);
    for (bad, why) in [
        (
            vec![authority.as_str(), "X-Stalewise-Shard-Hop: 2", host],
            "hop count",
        ),
        (vec![authority.as_str(), hop, hop, host], "duplicate hop"),
        (vec![authority.as_str(), hop], "missing authority"),
        (
            vec![authority.as_str(), hop, "X-Stalewise-Host: a b"],
            "malformed authority",
        ),
        (
            vec![authority.as_str(), hop, host, host],
            "duplicate authority",
        ),
    ] {
        assert_eq!(curl_from(&nodes[0], &target, &bad).0, 400, "{why}");
    }
    assert_eq!(hits(origin, "/hop/two"), 0);

    let auth = format!("Authorization: Bearer {TOKEN}\r\n");
    let sync = request(
        nodes[0].signaller,
        "PURGE",
        "/slow/a",
        &format!("{auth}X-Stalewise-Wait: 10s\r\n"),
    )
    .await;
    assert_eq!(sync.status, 200, "{}", sync.body);
    assert_eq!(status_of(&sync), ["delivered"; 3]);
    concurrent(&traffic, "/slow/a", 3).await;
    assert_eq!(
        hits(origin, "/slow/a"),
        2,
        "a broadcast purge refills exactly once"
    );

    let accepted = request(nodes[0].signaller, "PURGE", "/slow/b", &auth).await;
    assert_eq!(accepted.status, 202);
    assert_eq!(
        accepted.headers.get("x-stalewise-pod").map(String::as_str),
        Some("cache-0")
    );
    let location = accepted.headers["location"].clone();
    let polled = request(
        nodes[0].signaller,
        "GET",
        &format!("{location}?wait=10s"),
        &auth,
    )
    .await;
    assert_eq!(
        (polled.status, status_of(&polled)),
        (200, vec!["delivered".to_owned(); 3])
    );
    assert_eq!(
        request(nodes[1].signaller, "GET", &location, &auth)
            .await
            .status,
        404,
        "status lives on the admitting pod"
    );
    assert_eq!(
        request(nodes[0].signaller, "GET", &location, "")
            .await
            .status,
        401
    );

    docker(&["stop", "-t", "1", &nodes[2].id]);
    let keys: Vec<String> = (0..18).map(|i| format!("/fb/{i}")).collect();
    for key in &keys {
        for _ in 0..2 {
            let reply = request(traffic[0], "GET", key, "").await;
            assert_eq!(
                reply.status, 200,
                "{key} falls back to the origin when its owner is down"
            );
        }
    }
    let counts: Vec<usize> = keys.iter().map(|k| hits(origin, k)).collect();
    assert!(counts.iter().all(|n| (1..=2).contains(n)), "{counts:?}");
    let fallbacks = counts.iter().filter(|n| **n == 2).count();
    assert!(
        fallbacks > 0,
        "keys owned by the stopped cache were fetched directly: {counts:?}"
    );
    let degraded = request(
        nodes[0].signaller,
        "PURGE",
        "/fb/0",
        &format!("{auth}X-Stalewise-Wait: 10s\r\n"),
    )
    .await;
    assert_eq!(degraded.status, 504, "{}", degraded.body);
    let states = status_of(&degraded);
    assert_eq!(
        states.iter().filter(|s| *s == "delivered").count(),
        2,
        "{states:?}"
    );
    assert!(
        degraded.body.contains("\"reason\":\"deadline\""),
        "{}",
        degraded.body
    );
    eprintln!("fills: fallback keys {fallbacks}/18 counts {counts:?}");
}

/// Two shards whose memberships disagree: A rings both pods, B rings and trusts only itself.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn owner_refusing_an_untrusted_hop_falls_back_to_the_origin() {
    let (port, origin) = start_origin().await;
    let prefix = "10.213.80";
    let (ip_a, ip_b) = (format!("{prefix}.10"), format!("{prefix}.11"));
    let full = start_fake_kube(&[&ip_a, &ip_b]).await;
    let lagging = start_fake_kube(&[&ip_b]).await;
    let net = format!("stalewise-it-skew-{}", std::process::id());
    docker(&[
        "network",
        "create",
        "--subnet",
        &format!("{prefix}.0/24"),
        &net,
    ]);
    let network = Network(net);
    let mut nodes = Vec::new();
    for (ip, pod, kube) in [(&ip_a, "cache-0", full), (&ip_b, "cache-0", lagging)] {
        let dir = temp_dir(&format!("skew-{pod}-{kube}"));
        std::fs::write(dir.join("token"), TOKEN).unwrap();
        std::fs::write(dir.join("kubeconfig"), kubeconfig(kube)).unwrap();
        let mut args = peer_args(port, ip, pod);
        args.extend(
            [
                "--shard",
                "--peer-timeout=1s",
                "--membership-debounce=100ms",
            ]
            .iter()
            .map(|s| (*s).to_owned()),
        );
        nodes.push(run_peer_with(&dir, &network.0, ip, args).await);
    }
    for (c, size) in nodes.iter().zip([2, 1]) {
        wait_for("skewed rings", Duration::from_secs(60), async || {
            ring_size(c) == size
        })
        .await;
        warm(c.traffic).await;
    }
    let keys: Vec<String> = (0..12).map(|i| format!("/shard/skew{i}")).collect();
    for key in &keys {
        let reply = request(nodes[0].traffic, "GET", key, "").await;
        assert_eq!(
            reply.status, 200,
            "{key}: a refused hop falls back to the origin"
        );
        assert!(!reply.headers.keys().any(|k| k.starts_with("x-stalewise")));
    }
    let counts: Vec<usize> = keys.iter().map(|k| hits(&origin, k)).collect();
    assert!(counts.iter().all(|n| *n == 1), "{counts:?}");
    let forged = request(
        nodes[1].traffic,
        "GET",
        "/shard/forged",
        "X-Stalewise-Shard-Hop: 1\r\nX-Stalewise-Host: catalog.test\r\n",
    )
    .await;
    assert_eq!(
        forged.status, 403,
        "a non-peer still cannot send hop headers"
    );
    assert_eq!(hits(&origin, "/shard/forged"), 0);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn sharded_local_copies_are_capped_and_purged_by_tag() {
    let cluster = start_cluster("local", "10.213.79", &["--shard-local-ttl=2s"]).await;
    let traffic: Vec<SocketAddr> = cluster.nodes.iter().map(|c| c.traffic).collect();
    let origin = &cluster.origin;
    for addr in &traffic {
        assert_eq!(request(*addr, "GET", "/ltag/x", "").await.status, 200);
    }
    assert_eq!(hits(origin, "/ltag/x"), 1);
    for addr in &traffic {
        let reply = request(*addr, "GET", "/ltag/x", "").await;
        assert!(reply.is_hit(), "every cache now answers from its own copy");
        assert!(!reply.headers.contains_key("surrogate-key"));
    }
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let mut local_hits = 0;
    for addr in &traffic {
        local_hits += usize::from(request(*addr, "GET", "/ltag/x", "").await.is_hit());
    }
    assert_eq!(
        local_hits, 1,
        "copies expire at the local cap while the owner stays fresh"
    );
    assert_eq!(hits(origin, "/ltag/x"), 1);
    let auth = format!("Authorization: Bearer {TOKEN}\r\n");
    let ban = request(
        cluster.nodes[1].signaller,
        "BAN",
        "/",
        &format!("{auth}X-Stalewise-Tag: gamma\r\nX-Stalewise-Wait: 10s\r\n"),
    )
    .await;
    assert_eq!(ban.status, 200, "{}", ban.body);
    for addr in &traffic {
        let reply = request(*addr, "GET", "/ltag/x", "").await;
        assert_eq!(reply.body, "/ltag/x #2", "the tag purge reached every copy");
    }
    assert_eq!(hits(origin, "/ltag/x"), 2, "one refill after the purge");
}

/// Fully rendered sharding VCL with a fixture entry that reports the owner a key maps to.
fn ring_fixture(peers: &[(&str, &str)]) -> String {
    use stalewise::endpoint::{Endpoint, Host};
    use stalewise::membership::Peer;
    use stalewise::render::{self, Context, RenderSettings, ShardSettings};
    let settings = RenderSettings {
        env: std::collections::BTreeMap::new(),
        canonical_host: None,
        backend_authority: "origin.test:8080".parse().unwrap(),
        purge_acl: vec!["127.0.0.1".parse().unwrap()],
        peer_purge: false,
        probe_path: "/".into(),
        uncacheable_ttl: Duration::from_secs(30),
        max_object_size: 10_485_760,
        strip_rewrite_headers: true,
        shard: Some(ShardSettings {
            pod_name: "fixture".into(),
            peer_authority: PEER_AUTHORITY.parse().unwrap(),
            local_ttl: Duration::ZERO,
            peer_timeout: Duration::from_secs(1),
        }),
    };
    let ring: Vec<Peer> = peers
        .iter()
        .map(|(pod, ip)| Peer {
            pod_name: (*pod).into(),
            namespace: "apps".into(),
            address: ip.parse().unwrap(),
            port: 8080,
        })
        .collect();
    let origin = Endpoint::new(Host::Ip("127.0.0.1".parse().unwrap()), 9);
    let ctx = Context::new(&settings, &ring, &[origin], &[]).unwrap();
    let source = Template::embedded(Profile::Generic).source;
    let vcl = String::from_utf8(render::render(&source, &ctx).unwrap()).unwrap();
    vcl.replace("sub vcl_recv {", "sub stalewise_main_recv {")
        + r#"
sub vcl_recv {
    if (req.http.X-Fixture-Int) {
        set req.backend_hint = stalewise_owners.backend(by=KEY,
            key=std.integer(req.http.X-Fixture-Int, 0),
            healthy=IGNORE, warmup=0, rampup=false, resolve=NOW);
        return (synth(200));
    }
    if (req.http.X-Fixture-Host) {
        set req.http.Host = req.http.X-Fixture-Host;
        set req.url = "/k/" + req.http.X-Fixture-Path;
        call stalewise_shard_key;
        call stalewise_shard_owner;
        return (synth(200));
    }
    call stalewise_main_recv;
}

sub vcl_synth {
    set resp.http.X-Owner = req.backend_hint;
}
"#
}

struct RingHost {
    id: String,
    addr: SocketAddr,
    dir: PathBuf,
    loaded: usize,
}

impl Drop for RingHost {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.id]).output();
    }
}

impl RingHost {
    async fn start(first: &str) -> Self {
        build_image_once();
        let dir = temp_dir("ring");
        std::fs::write(dir.join("ring0.vcl"), first).unwrap();
        let mount = format!("{}:/fx:ro", dir.display());
        let id = docker(&[
            "run",
            "-d",
            "--entrypoint",
            "varnishd",
            "-p",
            "127.0.0.1::8080",
            "-v",
            &mount,
            &image(),
            "-F",
            "-a",
            ":8080",
            "-f",
            "/fx/ring0.vcl",
            "-n",
            "/tmp/varnish",
            "-s",
            "malloc,16m",
        ]);
        let addr = mapped(&id, "8080/tcp");
        wait_for("fixture varnishd", Duration::from_secs(60), async || {
            try_request(addr, "GET", "/", "X-Fixture-Int: 1\r\n")
                .await
                .is_ok_and(|r| r.status == 200)
        })
        .await;
        Self {
            id,
            addr,
            dir,
            loaded: 0,
        }
    }

    fn load(&mut self, vcl: &str) {
        self.loaded += 1;
        let name = format!("ring{}", self.loaded);
        std::fs::write(self.dir.join(format!("{name}.vcl")), vcl).unwrap();
        let path = format!("/fx/{name}.vcl");
        docker(&[
            "exec",
            &self.id,
            "varnishadm",
            "-n",
            "/tmp/varnish",
            "vcl.load",
            &name,
            &path,
        ]);
        docker(&[
            "exec",
            &self.id,
            "varnishadm",
            "-n",
            "/tmp/varnish",
            "vcl.use",
            &name,
        ]);
    }

    async fn owner(&self, header: &str) -> String {
        let reply = request(self.addr, "GET", "/", &format!("{header}\r\n")).await;
        reply.headers.get("x-owner").cloned().unwrap_or_default()
    }

    /// The owning pod for a fixture header, mapped back from the backend name.
    async fn owner_pod(&self, header: &str, peers: &[(&str, &str)]) -> String {
        let backend = self.owner(header).await;
        peers
            .iter()
            .find(|(_, ip)| {
                let host = stalewise::endpoint::Host::Ip(ip.parse().unwrap());
                backend == format!("peer_{}", stalewise::endpoint::vcl_identifier(&host, 8080))
            })
            .map_or(backend.clone(), |(pod, _)| (*pod).to_owned())
    }

    /// Owner pod for each of 240 paths under one authority.
    async fn owners(&self, host: &str, peers: &[(&str, &str)]) -> Vec<String> {
        let mut out = Vec::new();
        for i in 0..240 {
            let header = format!("X-Fixture-Host: {host}\r\nX-Fixture-Path: {i}");
            out.push(self.owner_pod(&header, peers).await);
        }
        out
    }

    /// Owner pods for the lowest and highest 32-bit ring keys.
    async fn edges(&self, peers: &[(&str, &str)]) -> Vec<String> {
        let mut out = Vec::new();
        for key in [0u32, 1, u32::MAX - 1, u32::MAX] {
            out.push(
                self.owner_pod(&format!("X-Fixture-Int: {key}"), peers)
                    .await,
            );
        }
        out
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn director_ring_is_deterministic_stable_and_moves_only_changed_ranges() {
    let three = [
        ("cache-0", "10.1.0.1"),
        ("cache-1", "10.1.0.2"),
        ("cache-2", "10.1.0.3"),
    ];
    let mut ring = RingHost::start(&ring_fixture(&three)).await;
    let base = ring.owners("catalog.test", &three).await;
    let pods: HashSet<&String> = base.iter().collect();
    assert_eq!(
        pods.len(),
        3,
        "every member owns part of the ring: {pods:?}"
    );

    ring.load(&ring_fixture(&three));
    assert_eq!(
        ring.owners("catalog.test", &three).await,
        base,
        "identical inputs give identical owners"
    );
    let other = ring.owners("other.test", &three).await;
    assert_ne!(other, base, "the authority is part of the key");

    // The director clamps instead of wrapping: keys past the last point stay on the last point.
    let edges = ring.edges(&three).await;
    assert_eq!(
        edges[0], edges[1],
        "the bottom edge belongs to the first point"
    );
    assert_eq!(edges[2], edges[3], "the top edge belongs to the last point");

    let moved = [
        ("cache-0", "10.9.0.1"),
        ("cache-1", "10.9.0.2"),
        ("cache-2", "10.9.0.3"),
    ];
    ring.load(&ring_fixture(&moved));
    assert_eq!(
        ring.owners("catalog.test", &moved).await,
        base,
        "address changes keep ownership"
    );
    assert_eq!(ring.edges(&moved).await, edges);

    let four = [three[0], three[1], three[2], ("cache-3", "10.1.0.4")];
    ring.load(&ring_fixture(&four));
    let grown = ring.owners("catalog.test", &four).await;
    let mut gained = 0;
    for (before, after) in base.iter().zip(&grown) {
        if before != after {
            assert_eq!(after, "cache-3", "keys only move to the new member");
            gained += 1;
        }
    }
    assert!(gained > 0);
    for (before, after) in edges.iter().zip(ring.edges(&four).await) {
        assert!(
            *before == after || after == "cache-3",
            "edge keys move only to the new member"
        );
    }

    let two = [three[0], three[2]];
    ring.load(&ring_fixture(&two));
    let shrunk = ring.owners("catalog.test", &two).await;
    for (before, after) in base.iter().zip(&shrunk) {
        if before != after {
            assert_eq!(before, "cache-1", "only the removed member's keys move");
        }
    }
    eprintln!("ring: {gained}/240 keys moved to the added member");
}

/// HTTP/1.1 origin that keeps every connection alive and records each request head it reads.
async fn start_keepalive_origin() -> (u16, Arc<Mutex<Vec<String>>>) {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen: Arc<Mutex<Vec<String>>> = Arc::default();
    let log = Arc::clone(&seen);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                continue;
            };
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                while let Some(head) = read_head(&mut stream).await {
                    let close = head.to_ascii_lowercase().contains("connection: close");
                    log.lock().unwrap().push(head);
                    let body = "keepalive origin";
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\nCache-Control: max-age=60\r\nContent-Length: {}\r\n\r\n{body}",
                        body.len()
                    );
                    if stream.write_all(reply.as_bytes()).await.is_err() || close {
                        return;
                    }
                }
            });
        }
    });
    (port, seen)
}

/// Reads one request or response head; `None` at EOF.
async fn read_head(stream: &mut tokio::net::TcpStream) -> Option<String> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if stream.read(&mut byte).await.ok()? == 0 {
            return None;
        }
        head.push(byte[0]);
    }
    Some(String::from_utf8_lossy(&head).into_owned())
}

/// Reads one response with a Content-Length body from a connection that stays open.
async fn read_response(stream: &mut tokio::net::TcpStream) -> String {
    let head = tokio::time::timeout(Duration::from_secs(10), read_head(stream))
        .await
        .expect("response in time")
        .expect("response head");
    let length: usize = head
        .lines()
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .map_or(0, |(_, v)| v.trim().parse().unwrap());
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).await.unwrap();
    head
}

fn origin_saw(seen: &Arc<Mutex<Vec<String>>>, line: &str) -> usize {
    seen.lock()
        .unwrap()
        .iter()
        .filter(|h| h.starts_with(line))
        .count()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn declined_upgrade_never_tunnels_the_next_request_past_vcl() {
    let (port, seen) = start_keepalive_origin().await;
    let dir = temp_dir("upgrade");
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    let c = run_cache(&dir, &base_args(port)).await;
    warm(c.traffic).await;
    request(c.traffic, "GET", "/cache", "").await;
    assert!(request(c.traffic, "GET", "/cache", "").await.is_hit());
    let fetched = origin_saw(&seen, "GET /cache ");

    let mut conn = tokio::net::TcpStream::connect(c.traffic).await.unwrap();
    conn.write_all(
        b"GET /h2c HTTP/1.1\r\nHost: catalog.test\r\nUpgrade: h2c\r\nConnection: Upgrade, HTTP2-Settings\r\nHTTP2-Settings: AAMAAABkAARAAAAAAAIAAAAA\r\n\r\n",
    )
    .await
    .unwrap();
    read_response(&mut conn).await;
    conn.write_all(b"GET /cache HTTP/1.1\r\nHost: catalog.test\r\n\r\n")
        .await
        .unwrap();
    let second = read_response(&mut conn).await;
    assert!(
        second.to_ascii_lowercase().contains("x-varnish:"),
        "the second request went through VCL: {second}"
    );
    assert_eq!(
        origin_saw(&seen, "GET /cache "),
        fetched,
        "the second request was a hit, not a tunnelled origin fetch"
    );
    let h2c = seen
        .lock()
        .unwrap()
        .iter()
        .find(|h| h.starts_with("GET /h2c "))
        .cloned()
        .unwrap();
    assert!(!h2c.to_ascii_lowercase().contains("upgrade: h2c"), "{h2c}");

    let mut ws = tokio::net::TcpStream::connect(c.traffic).await.unwrap();
    ws.write_all(b"GET /ws HTTP/1.1\r\nHost: catalog.test\r\nUpgrade: WebSocket\r\nConnection: keep-alive, Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
        .await
        .unwrap();
    read_response(&mut ws).await;
    let tunnel = seen
        .lock()
        .unwrap()
        .iter()
        .find(|h| h.starts_with("GET /ws "))
        .cloned()
        .unwrap()
        .to_ascii_lowercase();
    assert!(tunnel.contains("connection: upgrade"), "{tunnel}");
    assert!(tunnel.contains("upgrade: websocket"), "{tunnel}");

    request(c.traffic, "POST", "/expect", "Expect: 100-continue\r\n").await;
    let expect = seen
        .lock()
        .unwrap()
        .iter()
        .find(|h| h.starts_with("POST /expect "))
        .cloned()
        .unwrap()
        .to_ascii_lowercase();
    assert!(
        expect.contains("x-varnish:"),
        "Expect requests pass through VCL instead of a pipe: {expect}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn transient_storage_is_bounded_by_default() {
    let (port, _origin) = start_origin().await;
    let dir = temp_dir("transient");
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    let c = run_cache(&dir, &base_args(port)).await;
    let out = docker(&[
        "exec",
        &c.id,
        "varnishstat",
        "-n",
        "/var/lib/varnish",
        "-1",
        "-f",
        "SMA.Transient.g_space",
    ]);
    let space: u64 = out
        .split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("no Transient gauge: {out}"));
    // An unbounded malloc stevedore reports zero available bytes.
    assert!(
        (1..=64 << 20).contains(&space),
        "Transient must have a finite 64MiB budget: {space}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn purge_uses_the_policy_cache_key_and_skips_bypass_and_method_rules() {
    let (port, origin) = start_origin().await;
    let dir = temp_dir("policy-purge");
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    std::fs::write(dir.join("template.vcl"), rendered_policy_vcl(port)).unwrap();
    let mut args = base_args(port);
    args.push("--template=/config/template.vcl".into());
    let c = run_cache(&dir, &args).await;
    warm(c.traffic).await;
    let auth = format!("Authorization: Bearer {TOKEN}\r\n");
    let purge = async |path: &str, extra: &str| {
        let headers = format!("{auth}X-Stalewise-Wait: 5s\r\n{extra}");
        let reply = request(c.signaller, "PURGE", path, &headers).await;
        assert!(reply.status < 300, "purge {path}: {}", reply.status);
    };

    request(c.traffic, "GET", "/asset?tracking=x", "").await;
    assert!(
        request(c.traffic, "GET", "/asset?tracking=x", "")
            .await
            .is_hit()
    );
    purge("/asset?tracking=x", "").await;
    assert!(
        !request(c.traffic, "GET", "/asset?tracking=x", "")
            .await
            .is_hit(),
        "the purge used the normalised key"
    );
    assert_eq!(hits(&origin, "/asset"), 2);

    let tenant = "X-Tenant: a\r\n";
    request(c.traffic, "HEAD", "/tenant", tenant).await;
    assert!(request(c.traffic, "HEAD", "/tenant", tenant).await.is_hit());
    purge("/tenant", &format!("{tenant}X-Preview: true\r\n")).await;
    assert!(
        !request(c.traffic, "HEAD", "/tenant", tenant).await.is_hit(),
        "header-keyed objects purge despite bypass and method rules"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn rewrite_headers_are_stripped_unless_opted_out() {
    let (port, seen) = start_keepalive_origin().await;
    let dir = temp_dir("rewrite-headers");
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    let spoof =
        "X-Forwarded-Host: evil.test\r\nX-Original-URL: /admin\r\nX-Rewrite-URL: /admin\r\n";
    let head = |path: &str| {
        seen.lock()
            .unwrap()
            .iter()
            .find(|h| h.starts_with(&format!("GET {path} ")))
            .cloned()
            .unwrap()
            .to_ascii_lowercase()
    };

    let c = run_cache(&dir, &base_args(port)).await;
    warm(c.traffic).await;
    request(c.traffic, "GET", "/strip", spoof).await;
    let stripped = head("/strip");
    for name in ["x-forwarded-host", "x-original-url", "x-rewrite-url"] {
        assert!(
            !stripped.contains(name),
            "{name} reached the origin: {stripped}"
        );
    }
    assert!(
        request(c.traffic, "GET", "/strip", "").await.is_hit(),
        "the spoofed headers never split the key"
    );
    drop(c);

    let mut args = base_args(port);
    args.push("--strip-rewrite-headers=false".into());
    let c = run_cache(&dir, &args).await;
    warm(c.traffic).await;
    request(c.traffic, "GET", "/kept", spoof).await;
    assert!(head("/kept").contains("x-forwarded-host: evil.test"));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn ambiguous_paths_never_dodge_policy_bypass_rules() {
    let (port, _origin) = start_origin().await;
    let dir = temp_dir("ambiguous-path");
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    std::fs::write(dir.join("template.vcl"), rendered_policy_vcl(port)).unwrap();
    let mut args = base_args(port);
    args.push("--template=/config/template.vcl".into());
    let c = run_cache(&dir, &args).await;
    warm(c.traffic).await;

    request(c.traffic, "GET", "/shop/item", "").await;
    assert!(request(c.traffic, "GET", "/shop/item", "").await.is_hit());
    for path in [
        "/shop/account",
        "/shop/./account",
        "/shop/x/../account",
        "/shop//account",
        "/shop/%61ccount",
        "/shop%2Faccount",
        "/shop/.%2e/shop/account",
    ] {
        request(c.traffic, "GET", path, "").await;
        assert!(
            !request(c.traffic, "GET", path, "").await.is_hit(),
            "{path} was stored despite the bypass rule"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn stores_larger_than_the_memory_limit_refuse_to_start() {
    build_image_once();
    let out = std::process::Command::new("docker")
        .args(["run", "--rm", "--memory=192m", "--memory-swap=192m"])
        .arg(image())
        .args(base_args(1))
        .args(["--storage=malloc,64m", "--transient-storage=malloc,16m"])
        .output()
        .unwrap();
    let logs = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(2), "{logs}");
    assert!(logs.contains("memory limit 192 MiB cannot hold"), "{logs}");
}

/// A memory limit the idle cache already uses a tenth of, so a 0.1 high-water mark trips the
/// guard without filling memory.
const GUARD_DOCKER: [&str; 2] = ["--memory=768m", "--memory-swap=768m"];
const GUARD_TRIP: [&str; 2] = ["--memguard-high=0.1", "--memguard-low=0.05"];

async fn guard_bans(c: &Container) -> f64 {
    metric(c.management, "stalewise_memguard_bans_total").await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn memory_guard_bans_only_its_own_shard_and_requests_keep_succeeding() {
    let (port, _origin) = start_origin().await;
    let kube_port = start_fake_kube(&[PEER_A, PEER_B]).await;
    let dir = temp_dir("guard-shard");
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    std::fs::write(dir.join("kubeconfig"), kubeconfig(kube_port)).unwrap();
    let name = format!("stalewise-it-guard-shard-{}", std::process::id());
    docker(&["network", "create", "--subnet", PEER_SUBNET, &name]);
    let network = Network(name);
    let node = async |ip: &str, pod: &str, trip: bool| {
        let mut args = peer_args(port, ip, pod);
        args.extend(["--shard", "--membership-debounce=100ms"].map(str::to_owned));
        if trip {
            args.extend(GUARD_TRIP.map(str::to_owned));
        }
        run_peer_docker(&dir, &network.0, ip, args, &GUARD_DOCKER).await
    };
    let a = node(PEER_A, "cache-0", true).await;
    let b = node(PEER_B, "cache-1", false).await;
    for c in [&a, &b] {
        wait_for("two-peer ring", Duration::from_secs(60), async || {
            ring_size(c) == 2
        })
        .await;
        warm(c.traffic).await;
    }
    let paths: Vec<String> = (0..40).map(|n| format!("/shard/g{n}")).collect();
    let all_ok = async || {
        for path in &paths {
            for c in [&a, &b] {
                assert_eq!(
                    request(c.traffic, "GET", path, "").await.status,
                    200,
                    "{path}"
                );
            }
        }
    };
    all_ok().await;
    wait_for(
        "a guard ban on the tripped node",
        Duration::from_secs(30),
        async || guard_bans(&a).await >= 1.0,
    )
    .await;
    all_ok().await;
    assert_eq!(metric(a.management, "stalewise_memguard_active").await, 1.0);
    assert_eq!(guard_bans(&b).await, 0.0, "the guard never reaches a peer");
    assert_eq!(metric(b.management, "stalewise_memguard_active").await, 0.0);
    let ready = request(a.management, "GET", "/readyz", "").await;
    assert_eq!(ready.status, 200, "{}", ready.body);
    assert!(!ready.body.contains("memory guard"), "{}", ready.body);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; CI runs it"]
async fn memory_guard_bans_and_signaller_purges_run_together() {
    let (port, _origin) = start_origin().await;
    let dir = temp_dir("guard-purge");
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    let mut args = base_args(port);
    args.extend(GUARD_TRIP.map(str::to_owned));
    let extra: Vec<String> = GUARD_DOCKER.map(str::to_owned).to_vec();
    let c = run_cache_with(&dir, &extra, &args).await;
    warm(c.traffic).await;
    let auth = format!("Authorization: Bearer {TOKEN}\r\nX-Stalewise-Wait: 5s\r\n");
    // The guard cannot ban before varnishd is 10s old, so this loop spans its first ban.
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut purges = 0;
    while Instant::now() < deadline {
        let path = format!("/cache?p={purges}");
        let miss = request(c.traffic, "GET", &path, "").await;
        assert_eq!(miss.status, 200, "{path}: {}", miss.body);
        assert!(
            request(c.traffic, "GET", &path, "").await.is_hit(),
            "{path}"
        );
        let reply = request(c.signaller, "PURGE", &path, &auth).await;
        assert!(reply.status < 300, "purge {path}: {}", reply.status);
        assert!(
            !request(c.traffic, "GET", &path, "").await.is_hit(),
            "{path} purged"
        );
        purges += 1;
        // Paced: a tight loop of fresh connections exhausts the Docker port proxy, not Varnish.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(guard_bans(&c).await >= 1.0, "{}", logs(&c.id));
    assert_eq!(
        metric(c.management, "stalewise_memguard_failures_total").await,
        0.0
    );
    assert_eq!(
        metric(
            c.management,
            "stalewise_signaller_exhausted_deliveries_total"
        )
        .await,
        0.0
    );
}
