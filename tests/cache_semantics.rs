//! Real HTTP semantics against Varnish; all origin bodies stream in bounded chunks.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::Request;
use hyper_util::rt::TokioIo;
use sha2::{Digest, Sha256};
use stalewise::config::Profile;
use stalewise::endpoint::{Endpoint, Host};
use stalewise::render::{self, Context, RenderSettings, Template};
use tokio::task::JoinHandle;

#[allow(dead_code)]
mod common;
#[path = "common/origin.rs"]
mod origin;
use common::*;
use origin::{Origin, Route};

struct Fixture {
    cache: Container,
    origin: Origin,
    dir: PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn fixture(name: &str, profile: Profile, storage: &str, keep: bool) -> Fixture {
    fixture_with(name, profile, storage, keep, &[], &[]).await
}

async fn fixture_with(
    name: &str,
    profile: Profile,
    storage: &str,
    keep: bool,
    docker_extra: &[String],
    extra: &[String],
) -> Fixture {
    let origin = Origin::start().await;
    let dir = temp_dir(name);
    std::fs::write(dir.join("token"), TOKEN).unwrap();
    let settings = RenderSettings {
        env: BTreeMap::new(),
        canonical_host: Some("catalog.test".parse().unwrap()),
        backend_authority: format!("host.docker.internal:{}", origin.port)
            .parse()
            .unwrap(),
        purge_acl: vec!["127.0.0.1".parse().unwrap()],
        peer_purge: false,
        probe_path: "/".into(),
        uncacheable_ttl: Duration::from_secs(30),
        max_object_size: 10 * 1024 * 1024,
        strip_rewrite_headers: true,
        shard: None,
    };
    let policies = if keep {
        let spec = serde_norway::from_str(
            "serviceRef: {name: catalog}\npathPrefix: /keep\noverrides: {keepSeconds: 30}",
        )
        .unwrap();
        vec![
            stalewise::policy::validate(
                "keep",
                1,
                &spec,
                &stalewise::policy::OwnService {
                    name: "catalog".into(),
                    port_name: None,
                    port_number: None,
                },
            )
            .unwrap(),
        ]
    } else {
        vec![]
    };
    let backend = Endpoint::new(
        Host::Dns("host.docker.internal".parse().unwrap()),
        origin.port,
    );
    let context = Context::new(&settings, &[], &[backend], &policies).unwrap();
    let vcl = render::render(&Template::embedded(profile).source, &context).unwrap();
    std::fs::write(dir.join("template.vcl"), vcl).unwrap();
    let mut args = base_args(origin.port);
    args.extend([
        "--template=/config/template.vcl".into(),
        format!("--storage={storage}"),
    ]);
    args.extend_from_slice(extra);
    let cache = run_cache_with(&dir, docker_extra, &args).await;
    warm(cache.traffic).await;
    Fixture { cache, origin, dir }
}

async fn pair(f: &Fixture, path: &str, headers: &str, stored: bool, status: u16) {
    let first = request(f.cache.traffic, "GET", path, headers).await;
    let second = request(f.cache.traffic, "GET", path, headers).await;
    assert_eq!((first.status, second.status), (status, status), "{path}");
    assert!(!first.is_hit(), "{path}: first response must miss/pass");
    assert_eq!(second.is_hit(), stored, "{path}: X-Varnish hit marker");
    assert_eq!(
        f.origin.count(path),
        if stored { 1 } else { 2 },
        "{path}: origin fetches"
    );
    assert!(
        second.headers.contains_key("age"),
        "{path}: Age is delivered"
    );
    if stored {
        assert_eq!(second.body, first.body, "{path}: same representation");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn freshness_privacy_and_parser_matrix() {
    type Case<'a> = (&'a str, &'a [(&'a str, &'a str)], bool);
    let cases: &[Case<'_>] = &[
        ("max-age", &[("cache-control", "max-age=60")], true),
        (
            "shared-wins",
            &[("cache-control", "max-age=0, s-maxage=60")],
            true,
        ),
        (
            "shared-zero",
            &[("cache-control", "max-age=60, s-maxage=0")],
            false,
        ),
        ("zero", &[("cache-control", "max-age=0")], false),
        (
            "no-store",
            &[("cache-control", "max-age=60, no-store")],
            false,
        ),
        (
            "no-cache",
            &[("cache-control", "max-age=60, no-cache")],
            false,
        ),
        (
            "private",
            &[("cache-control", "max-age=60, private")],
            false,
        ),
        ("public", &[("cache-control", "public, max-age=60")], true),
        (
            "must-revalidate",
            &[("cache-control", "max-age=60, must-revalidate")],
            true,
        ),
        (
            "proxy-revalidate",
            &[("cache-control", "max-age=60, proxy-revalidate")],
            true,
        ),
        (
            "must-revalidate-expired",
            &[("cache-control", "max-age=0, must-revalidate")],
            false,
        ),
        (
            "proxy-revalidate-expired",
            &[("cache-control", "max-age=0, proxy-revalidate")],
            false,
        ),
        ("unknown", &[("cache-control", "extension=60")], false),
        (
            "unknown-with-freshness",
            &[("cache-control", "extension=value, max-age=60")],
            true,
        ),
        ("garbage", &[("cache-control", "max-age=wat")], false),
        ("negative", &[("cache-control", "max-age=-1")], false),
        (
            "overflow",
            &[("cache-control", "max-age=99999999999999999999")],
            false,
        ),
        (
            "substring",
            &[("cache-control", "extension=\"max-age=60, s-maxage=60\"")],
            false,
        ),
        (
            "quoted-comma",
            &[("cache-control", "extension=\"a,b\", max-age=60")],
            false,
        ),
        ("case", &[("Cache-Control", "MaX-AgE=60")], true),
        (
            "duplicate-conflict",
            &[
                ("cache-control", "max-age=60"),
                ("cache-control", "max-age=0"),
            ],
            false,
        ),
        (
            "duplicate-identical",
            &[
                ("cache-control", "max-age=60"),
                ("cache-control", "max-age=60"),
            ],
            false,
        ),
        (
            "duplicate-disjoint",
            &[("cache-control", "public"), ("cache-control", "max-age=60")],
            true,
        ),
        (
            "duplicate-private",
            &[
                ("cache-control", "max-age=60"),
                ("cache-control", "private"),
            ],
            false,
        ),
        (
            "expires",
            &[("expires", "Thu, 01 Jan 2099 00:00:00 GMT")],
            true,
        ),
        (
            "expires-past",
            &[("expires", "Sat, 01 Jan 2000 00:00:00 GMT")],
            false,
        ),
        (
            "max-age-wins",
            &[
                ("expires", "Sat, 01 Jan 2000 00:00:00 GMT"),
                ("cache-control", "max-age=60"),
            ],
            true,
        ),
        (
            "max-age-zero-wins",
            &[
                ("expires", "Thu, 01 Jan 2099 00:00:00 GMT"),
                ("cache-control", "max-age=0"),
            ],
            false,
        ),
        (
            "aged-max",
            &[("cache-control", "max-age=60"), ("age", "61")],
            false,
        ),
        (
            "aged-shared",
            &[("cache-control", "max-age=600, s-maxage=60"), ("age", "61")],
            false,
        ),
        (
            "age-remaining",
            &[("cache-control", "max-age=60"), ("age", "10")],
            true,
        ),
        (
            "surrogate",
            &[
                ("cache-control", "max-age=0"),
                ("surrogate-control", "max-age=60"),
            ],
            true,
        ),
        (
            "surrogate-zero",
            &[
                ("cache-control", "max-age=60"),
                ("surrogate-control", "max-age=0"),
            ],
            false,
        ),
        (
            "surrogate-unknown",
            &[
                ("cache-control", "max-age=60"),
                ("surrogate-control", "extension=60"),
            ],
            false,
        ),
        (
            "surrogate-private",
            &[
                ("cache-control", "private"),
                ("surrogate-control", "max-age=60"),
            ],
            false,
        ),
        (
            "set-cookie",
            &[
                ("cache-control", "max-age=60"),
                ("set-cookie", "session=secret"),
            ],
            false,
        ),
        (
            "vary-star",
            &[("cache-control", "max-age=60"), ("vary", "*")],
            false,
        ),
        ("default-zero", &[], false),
    ];
    for (name, profile) in [
        ("matrix-generic", Profile::Generic),
        ("matrix-mesh", Profile::Mesh),
    ] {
        let f = fixture(name, profile, "malloc,256m", false).await;
        for (name, headers, stored) in cases {
            let path = format!("/{name}");
            f.origin.set(&path, Route::headers(headers));
            pair(&f, &path, "", *stored, 200).await;
            let delivered = request(f.cache.traffic, "GET", &path, "").await;
            assert!(
                !delivered.headers.contains_key("surrogate-control"),
                "{name}: surrogate metadata hidden"
            );
            if *name == "surrogate" {
                assert_eq!(
                    delivered.headers.get("cache-control").map(String::as_str),
                    Some("max-age=0")
                );
            }
            if *name == "age-remaining" {
                assert!(delivered.headers["age"].parse::<u64>().unwrap() >= 10);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn request_credentials_and_body_methods_always_pass() {
    let f = fixture("request-pass", Profile::Generic, "malloc,256m", false).await;
    for (name, headers) in [
        ("cookie", "Cookie: session=secret\r\n"),
        ("authorization", "Authorization: Bearer secret\r\n"),
    ] {
        let path = format!("/{name}");
        f.origin.set(
            &path,
            Route::headers(&[("cache-control", "public, max-age=60")]),
        );
        pair(&f, &path, headers, false, 200).await;
        let miss = request(f.cache.traffic, "GET", &path, "").await;
        let hit = request(f.cache.traffic, "GET", &path, "").await;
        assert!(!miss.is_hit());
        assert!(hit.is_hit(), "credential-free requests remain cacheable");
        assert_eq!(f.origin.count(&path), 3);
    }
    for method in ["POST", "PUT", "DELETE"] {
        let path = format!("/{method}");
        for _ in 0..2 {
            let reply = request(f.cache.traffic, method, &path, "").await;
            assert_eq!(reply.status, 200);
            assert!(!reply.is_hit(), "{method}");
        }
        assert_eq!(f.origin.count(&path), 2, "{method}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn vary_partitions_representations_and_reuses_each_variant() {
    let f = fixture("vary", Profile::Generic, "malloc,256m", false).await;
    let path = "/vary-custom";
    let mut route = Route::headers(&[("cache-control", "max-age=60"), ("vary", "x-flavor")]);
    route.echo = Some("x-flavor".into());
    f.origin.set(path, route);
    let mut bodies = BTreeMap::new();
    for value in ["vanilla", "chocolate"] {
        let headers = format!("X-Flavor: {value}\r\n");
        let miss = request(f.cache.traffic, "GET", path, &headers).await;
        let hit = request(f.cache.traffic, "GET", path, &headers).await;
        assert!(!miss.is_hit(), "{value} starts a new variant");
        assert!(hit.is_hit(), "{value} reuses its variant");
        assert_eq!(hit.body, miss.body);
        assert!(hit.body.ends_with(value));
        bodies.insert(value, hit.body);
    }
    assert_ne!(bodies["vanilla"], bodies["chocolate"]);
    for value in ["vanilla", "chocolate"] {
        let hit = request(
            f.cache.traffic,
            "GET",
            path,
            &format!("X-Flavor: {value}\r\n"),
        )
        .await;
        assert!(hit.is_hit());
        assert_eq!(hit.body, bodies[value]);
    }
    assert_eq!(f.origin.count(path), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn conditional_head_and_range_are_served_from_the_full_cached_object() {
    let f = fixture("conditional", Profile::Generic, "malloc,256m", false).await;
    let mut route = Route::headers(&[
        ("cache-control", "max-age=60"),
        ("etag", "\"version-one\""),
        ("last-modified", "Sat, 01 Jan 2000 00:00:00 GMT"),
    ]);
    route.size = Some(1024);
    f.origin.set("/object", route);
    let first = request(f.cache.traffic, "GET", "/object", "").await;
    assert_eq!(first.status, 200);
    for headers in [
        "If-None-Match: \"version-one\"\r\n",
        "If-Modified-Since: Sat, 01 Jan 2000 00:00:00 GMT\r\n",
    ] {
        let reply = request(f.cache.traffic, "GET", "/object", headers).await;
        assert_eq!(reply.status, 304);
        assert!(reply.is_hit());
        assert!(reply.body.is_empty());
    }
    let changed = request(
        f.cache.traffic,
        "GET",
        "/object",
        "If-None-Match: \"other\"\r\n",
    )
    .await;
    assert_eq!(changed.status, 200);
    assert!(changed.is_hit());
    let head = request(f.cache.traffic, "HEAD", "/object", "").await;
    assert_eq!(head.status, 200);
    assert!(head.is_hit());
    assert!(head.body.is_empty());
    let range = request(f.cache.traffic, "GET", "/object", "Range: bytes=10-19\r\n").await;
    assert_eq!(range.status, 206);
    assert!(range.is_hit());
    assert_eq!(range.headers["content-range"], "bytes 10-19/1024");
    assert_eq!(range.body, "xxxxxxxxxx");
    assert_eq!(f.origin.count("/object"), 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn keep_revalidates_with_backend_validators_and_retains_the_body() {
    let f = fixture("keep", Profile::Generic, "malloc,256m", true).await;
    let mut route = Route::headers(&[
        ("cache-control", "max-age=1"),
        ("etag", "\"version-one\""),
        ("last-modified", "Sat, 01 Jan 2000 00:00:00 GMT"),
    ]);
    route.conditional = true;
    f.origin.set("/keep/object", route);
    let first = request(f.cache.traffic, "GET", "/keep/object", "").await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let validated = request(f.cache.traffic, "GET", "/keep/object", "").await;
    assert_eq!(validated.status, 200);
    assert_eq!(
        validated.body, first.body,
        "backend 304 keeps original bytes"
    );
    assert_eq!(
        f.origin.validations("/keep/object"),
        1,
        "backend receives a validator"
    );
    assert_eq!(f.origin.count("/keep/object"), 2);
    assert!(
        request(f.cache.traffic, "GET", "/keep/object", "")
            .await
            .is_hit()
    );
    assert_eq!(f.origin.count("/keep/object"), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn native_cacheable_statuses_store_but_server_errors_do_not() {
    let f = fixture("statuses", Profile::Generic, "malloc,256m", false).await;
    for status in [200, 203, 204, 301, 404, 410, 500, 502, 503] {
        let path = format!("/status-{status}");
        let mut route = Route::headers(&[("cache-control", "max-age=60")]);
        route.status = status;
        f.origin.set(&path, route);
        pair(&f, &path, "", status < 500, status).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn ttl_expires_between_one_and_three_seconds() {
    let f = fixture("expiry", Profile::Generic, "malloc,256m", false).await;
    f.origin
        .set("/expiry", Route::headers(&[("cache-control", "max-age=2")]));
    let first = request(f.cache.traffic, "GET", "/expiry", "").await;
    let start = Instant::now();
    tokio::time::sleep_until((start + Duration::from_millis(1100)).into()).await;
    let hit = request(f.cache.traffic, "GET", "/expiry", "").await;
    assert!(hit.is_hit());
    assert_eq!(hit.body, first.body);
    assert_eq!(f.origin.count("/expiry"), 1);
    assert!(hit.headers["age"].parse::<u64>().unwrap() >= 1);
    tokio::time::sleep_until((start + Duration::from_secs(3)).into()).await;
    let expired = request(f.cache.traffic, "GET", "/expiry", "").await;
    assert!(!expired.is_hit());
    assert_ne!(expired.body, first.body);
    assert_eq!(f.origin.count("/expiry"), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn hit_for_miss_allows_twenty_parallel_origin_fetches() {
    let f = fixture("hit-for-miss", Profile::Generic, "malloc,256m", false).await;
    let mut route = Route::headers(&[("cache-control", "no-store")]);
    route.delay = Duration::from_millis(300);
    f.origin.set("/parallel", route);
    request(f.cache.traffic, "GET", "/parallel", "").await;
    let start = Instant::now();
    let replies = futures_util::future::join_all(
        (0..20).map(|_| request(f.cache.traffic, "GET", "/parallel", "")),
    )
    .await;
    assert!(replies.iter().all(|r| r.status == 200 && !r.is_hit()));
    assert_eq!(f.origin.count("/parallel"), 21);
    assert!(
        f.origin.peak.load(Ordering::SeqCst) >= 15,
        "origin requests overlap"
    );
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "20 x 300ms must not serialize: {:?}",
        start.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn tag_ban_and_url_purge_invalidate_only_their_targets() {
    let f = fixture("invalidation", Profile::Generic, "malloc,256m", false).await;
    f.origin.set(
        "/tagged",
        Route::headers(&[
            ("cache-control", "max-age=60"),
            ("surrogate-key", "alpha beta"),
        ]),
    );
    pair(&f, "/tagged", "", true, 200).await;
    pair(&f, "/untagged", "", true, 200).await;
    let headers = format!(
        "Authorization: Bearer {TOKEN}\r\nX-Stalewise-Wait: 5s\r\nX-Stalewise-Tag: beta\r\n"
    );
    assert_eq!(
        request(f.cache.signaller, "BAN", "/", &headers)
            .await
            .status,
        200
    );
    assert!(
        !request(f.cache.traffic, "GET", "/tagged", "")
            .await
            .is_hit()
    );
    assert_eq!(f.origin.count("/tagged"), 2);
    assert!(
        request(f.cache.traffic, "GET", "/untagged", "")
            .await
            .is_hit()
    );
    assert_eq!(f.origin.count("/untagged"), 1);
    let headers = format!("Authorization: Bearer {TOKEN}\r\nX-Stalewise-Wait: 5s\r\n");
    assert_eq!(
        request(f.cache.signaller, "PURGE", "/untagged", &headers)
            .await
            .status,
        200
    );
    assert!(
        !request(f.cache.traffic, "GET", "/untagged", "")
            .await
            .is_hit()
    );
    assert_eq!(f.origin.count("/untagged"), 2);
}

struct Driver(JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn streamed(addr: SocketAddr, path: &str, early: Option<&Route>) -> (bool, usize, Vec<u8>) {
    streamed_headers(addr, path, early, "").await
}

async fn streamed_headers(
    addr: SocketAddr,
    path: &str,
    early: Option<&Route>,
    accept: &str,
) -> (bool, usize, Vec<u8>) {
    tokio::time::timeout(Duration::from_secs(60), async {
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        let _driver = Driver(tokio::spawn(async move {
            let _ = conn.await;
        }));
        let req = Request::builder()
            .uri(path)
            .header("host", "catalog.test")
            .header("accept-encoding", accept)
            .body(Empty::<Bytes>::new())
            .unwrap();
        let response = sender.send_request(req).await.unwrap();
        assert_eq!(response.status(), 200);
        let hit = response.headers()["x-varnish"]
            .to_str()
            .unwrap()
            .split_whitespace()
            .count()
            == 2;
        let mut body = response.into_body();
        let mut bytes = 0;
        let mut hash = Sha256::new();
        while let Some(frame) = body.frame().await {
            if let Ok(data) = frame.unwrap().into_data() {
                if bytes == 0
                    && let Some(route) = early
                {
                    assert!(
                        !route.finished.load(Ordering::SeqCst),
                        "first bytes precede origin completion"
                    );
                }
                bytes += data.len();
                hash.update(&data);
            }
        }
        (hit, bytes, hash.finalize().to_vec())
    })
    .await
    .expect("bounded streaming request")
}

fn checksum(size: usize) -> Vec<u8> {
    let block = [b'x'; 64 * 1024];
    let mut hash = Sha256::new();
    for offset in (0..size).step_by(block.len()) {
        hash.update(&block[..(size - offset).min(block.len())]);
    }
    hash.finalize().to_vec()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn payload_sizes_cache_below_limit_and_stream_larger_bodies_intact() {
    let f = fixture("payloads", Profile::Generic, "malloc,256m", false).await;
    for size in [
        0,
        1,
        1024,
        1024 * 1024,
        99 * 1024 * 1024 / 10,
        10 * 1024 * 1024 + 1,
        50 * 1024 * 1024,
        200 * 1024 * 1024,
    ] {
        let path = format!("/size-{size}");
        let mut route = Route::headers(&[("cache-control", "max-age=60")]);
        route
            .headers
            .push(("content-length".into(), size.to_string()));
        route.size = Some(size);
        f.origin.set(&path, route);
        let expected = checksum(size);
        let first = streamed(f.cache.traffic, &path, None).await;
        let second = streamed(f.cache.traffic, &path, None).await;
        let stored = size <= 10 * 1024 * 1024;
        assert_eq!(
            first,
            (false, size, expected.clone()),
            "{size}: first payload"
        );
        assert_eq!(second, (stored, size, expected), "{size}: repeated payload");
        assert_eq!(f.origin.count(&path), if stored { 1 } else { 2 }, "{size}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn chunked_objects_cache_and_small_storage_evicts_without_crashing() {
    let f = fixture("eviction", Profile::Generic, "malloc,4m", false).await;
    let size = 1024 * 1024;
    let mut route = Route::headers(&[("cache-control", "max-age=60")]);
    route.size = Some(size);
    f.origin.set("/old", route.clone());
    assert_eq!(
        streamed(f.cache.traffic, "/old", None).await,
        (false, size, checksum(size))
    );
    assert!(
        streamed(f.cache.traffic, "/old", None).await.0,
        "unknown length opts into caching"
    );
    for n in 0..8 {
        let path = format!("/pressure-{n}");
        f.origin.set(&path, route.clone());
        assert_eq!(
            streamed(f.cache.traffic, &path, None).await,
            (false, size, checksum(size))
        );
    }
    assert!(
        streamed(f.cache.traffic, "/pressure-7", None).await.0,
        "newest survives eviction"
    );
    assert!(
        !streamed(f.cache.traffic, "/old", None).await.0,
        "oldest object was evicted"
    );
    assert_eq!(f.origin.count("/old"), 2);
    assert_eq!(f.origin.count("/pressure-7"), 1);
    assert_eq!(
        request(f.cache.management, "GET", "/readyz", "")
            .await
            .status,
        200
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn event_streams_deliver_progressively_and_never_store() {
    let f = fixture("streaming", Profile::Generic, "malloc,256m", false).await;
    let mut route = Route::headers(&[
        ("cache-control", "max-age=60"),
        ("content-type", "text/event-stream"),
    ]);
    route.size = Some(256 * 1024);
    route.chunk_delay = Duration::from_millis(300);
    f.origin.set("/events", route.clone());
    for _ in 0..2 {
        route.finished.store(false, Ordering::SeqCst);
        assert_eq!(
            streamed(f.cache.traffic, "/events", Some(&route)).await,
            (false, 256 * 1024, checksum(256 * 1024))
        );
    }
    assert_eq!(f.origin.count("/events"), 2);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn vary_accept_encoding_serves_gzip_and_identity_from_one_cached_representation() {
    let f = fixture("vary-encoding", Profile::Generic, "malloc,256m", false).await;
    let encoded: &[u8] = &[
        31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 75, 206, 207, 45, 40, 74, 45, 46, 78, 77, 81, 40, 74, 5,
        177, 82, 243, 74, 18, 75, 50, 243, 243, 0, 57, 197, 192, 177, 25, 0, 0, 0,
    ];
    let mut route = Route::headers(&[
        ("cache-control", "max-age=60"),
        ("vary", "Accept-Encoding"),
        ("content-encoding", "gzip"),
    ]);
    route.body = Some(Bytes::copy_from_slice(encoded));
    f.origin.set("/compressed", route);
    let compressed_hash = Sha256::digest(encoded).to_vec();
    let plain_hash = Sha256::digest(b"compressed representation").to_vec();
    assert_eq!(
        streamed_headers(f.cache.traffic, "/compressed", None, "gzip").await,
        (false, encoded.len(), compressed_hash.clone())
    );
    assert_eq!(
        streamed_headers(f.cache.traffic, "/compressed", None, "gzip").await,
        (true, encoded.len(), compressed_hash)
    );
    assert_eq!(
        streamed_headers(f.cache.traffic, "/compressed", None, "identity").await,
        (true, 25, plain_hash)
    );
    assert_eq!(f.origin.count("/compressed"), 1);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn stale_while_revalidate_keeps_objects_through_background_server_errors() {
    let f = fixture("swr", Profile::Generic, "malloc,256m", false).await;
    let route = Route::headers(&[("cache-control", "max-age=1, stale-while-revalidate=30")]);
    f.origin.set("/swr", route.clone());
    let first = request(f.cache.traffic, "GET", "/swr", "").await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    for status in [500, 502, 503] {
        let mut failure = route.clone();
        failure.status = status;
        // Keeps each background fetch busy long enough that stale hits coalesce onto it.
        failure.delay = Duration::from_millis(500);
        f.origin.set("/swr", failure);
        let before = f.origin.count("/swr");
        // Varnish runs one background fetch per object at a time, so a second failed fetch
        // proves the first was abandoned while every request in between stayed a stale hit.
        wait_for(
            "two background error fetches",
            Duration::from_secs(10),
            async || {
                let stale = request(f.cache.traffic, "GET", "/swr", "").await;
                assert_eq!(stale.status, 200, "{status}: no hit-for-miss poisoning");
                assert!(stale.is_hit(), "{status}: served from cache");
                assert_eq!(stale.body, first.body, "{status}: stale survives");
                f.origin.count("/swr") >= before + 2
            },
        )
        .await;
    }
    f.origin.set("/swr", route);
    let before = f.origin.count("/swr");
    request(f.cache.traffic, "GET", "/swr", "").await;
    wait_for(
        "successful background refresh",
        Duration::from_secs(5),
        async || {
            let response = request(f.cache.traffic, "GET", "/swr", "").await;
            response.is_hit() && response.body != first.body
        },
    )
    .await;
    assert!(f.origin.count("/swr") > before);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn stale_if_error_is_health_gated_and_ends_at_its_deadline() {
    let f = fixture("sie", Profile::Generic, "malloc,256m", false).await;
    let route = Route::headers(&[("cache-control", "max-age=1, stale-if-error=20")]);
    f.origin.set("/sie", route.clone());
    let first = request(f.cache.traffic, "GET", "/sie", "").await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let refreshed = request(f.cache.traffic, "GET", "/sie", "").await;
    assert!(
        !refreshed.is_hit(),
        "healthy origin without SWR must refetch"
    );
    assert_ne!(refreshed.body, first.body);
    assert_eq!(f.origin.count("/sie"), 2);
    let mut failure = Route::headers(&[]);
    failure.status = 503;
    f.origin.set("/", failure.clone());
    f.origin.set("/sie", failure);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let stale = request(f.cache.traffic, "GET", "/sie", "").await;
    assert_eq!(
        stale.status, 200,
        "failed foreground revalidation retains SIE"
    );
    assert_eq!(stale.body, refreshed.body);
    f.origin.set(
        "/health-check-miss",
        Route::headers(&[("cache-control", "no-store")]),
    );
    wait_for(
        "origin probes report sick",
        Duration::from_secs(15),
        async || {
            request(f.cache.traffic, "GET", "/health-check-miss", "")
                .await
                .status
                == 503
        },
    )
    .await;
    let before = f.origin.count("/sie");
    let sick = request(f.cache.traffic, "GET", "/sie", "").await;
    assert_eq!(sick.status, 200);
    assert!(sick.is_hit());
    assert_eq!(sick.body, refreshed.body);
    assert_eq!(
        f.origin.count("/sie"),
        before,
        "sick backend is not fetched"
    );
    let age = sick.headers["age"].parse::<u64>().unwrap();
    tokio::time::sleep(Duration::from_secs(23u64.saturating_sub(age))).await;
    let expired = request(f.cache.traffic, "GET", "/sie", "").await;
    assert_eq!(expired.status, 503, "expired SIE never serves stale");
    assert_ne!(expired.body, refreshed.body);
}

const MIB: usize = 1024 * 1024;

/// Reads at most `rate` bytes per second; returns the byte count and SHA-256, or why it stopped.
async fn throttled(addr: SocketAddr, path: &str, rate: usize) -> Result<(usize, Vec<u8>), String> {
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    let _driver = Driver(tokio::spawn(async move {
        let _ = conn.await;
    }));
    let req = Request::builder()
        .uri(path)
        .header("host", "catalog.test")
        .body(Empty::<Bytes>::new())
        .unwrap();
    let response = sender.send_request(req).await.unwrap();
    if response.status() != 200 {
        return Err(format!("status {}", response.status()));
    }
    let mut body = response.into_body();
    let start = Instant::now();
    let mut bytes = 0;
    let mut hash = Sha256::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| format!("after {bytes} bytes: {e:?}"))?;
        if let Ok(data) = frame.into_data() {
            bytes += data.len();
            hash.update(&data);
        }
        let due = Duration::from_secs_f64(bytes as f64 / rate as f64);
        if let Some(wait) = due.checked_sub(start.elapsed()) {
            tokio::time::sleep(wait).await;
        }
    }
    Ok((bytes, hash.finalize().to_vec()))
}

fn stat(id: &str, field: &str) -> u64 {
    let out = docker(&[
        "exec",
        id,
        "varnishstat",
        "-n",
        "/var/lib/varnish",
        "-1",
        "-f",
        field,
    ]);
    out.split_whitespace()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("no {field}: {out}"))
}

async fn slow_pass_fixture(name: &str, docker_extra: &[String]) -> Fixture {
    let f = fixture_with(
        name,
        Profile::Generic,
        "malloc,16m",
        false,
        docker_extra,
        &["--transient-storage=malloc,16m".into()],
    )
    .await;
    let mut route = Route::headers(&[("cache-control", "no-store")]);
    route
        .headers
        .push(("content-length".into(), SLOW_BODY.to_string()));
    route.size = Some(SLOW_BODY);
    f.origin.set("/slow", route);
    f
}

/// Far larger than the 16 MiB Transient store, so the fetch cannot buffer it whole.
const SLOW_BODY: usize = 200 * MIB;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn slow_client_receives_a_pass_body_larger_than_transient() {
    let f = slow_pass_fixture("slow-client", &[]).await;
    let received = tokio::time::timeout(
        Duration::from_secs(120),
        throttled(f.cache.traffic, "/slow", 40 * MIB),
    )
    .await
    .expect("slow client finishes");
    assert_eq!(received, Ok((SLOW_BODY, checksum(SLOW_BODY))));
    assert_eq!(
        stat(&f.cache.id, "SMA.Transient.c_fail"),
        0,
        "the fetch waits for the client instead of filling Transient"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn concurrent_slow_clients_of_large_pass_bodies_stay_within_memory() {
    const CLIENTS: usize = 20;
    // The chart's default memory limit.
    const LIMIT: u64 = 512 * MIB as u64;
    let docker_extra = [
        format!("--memory={LIMIT}"),
        format!("--memory-swap={LIMIT}"),
    ];
    let f = slow_pass_fixture("slow-clients", &docker_extra).await;
    let clients = (0..CLIENTS).map(|_| {
        tokio::time::timeout(
            Duration::from_secs(120),
            throttled(f.cache.traffic, "/slow", 20 * MIB),
        )
    });
    let results = futures_util::future::join_all(clients).await;
    assert!(
        results.iter().all(Result::is_ok),
        "every client ends, complete or cut short"
    );
    assert_eq!(f.origin.count("/slow"), CLIENTS);
    assert_eq!(
        docker(&[
            "inspect",
            "-f",
            "{{.State.Running}} {{.State.OOMKilled}}",
            &f.cache.id
        ]),
        "true false",
        "the cache survives {CLIENTS} slow clients"
    );
    let peak: u64 = docker(&["exec", &f.cache.id, "cat", "/sys/fs/cgroup/memory.peak"])
        .parse()
        .unwrap();
    assert!(peak < LIMIT, "memory peak {peak}");
    assert_eq!(
        throttled(f.cache.traffic, "/slow", usize::MAX).await,
        Ok((SLOW_BODY, checksum(SLOW_BODY))),
        "Transient is released once the load ends"
    );
}

const SMALL_BODY: usize = 2 * 1024;

/// Fills the cache with distinct small objects over 32 keep-alive connections; panics on any non-200.
async fn fill_small(f: &Fixture, from: usize, count: usize) {
    const WORKERS: usize = 32;
    let mut route = Route::headers(&[("cache-control", "max-age=600")]);
    route.body = Some(Bytes::from(vec![b's'; SMALL_BODY]));
    route
        .headers
        .push(("content-length".into(), SMALL_BODY.to_string()));
    f.origin.set_fallback(route);
    let traffic = f.cache.traffic;
    let workers = (0..WORKERS).map(|worker| async move {
        let stream = tokio::net::TcpStream::connect(traffic).await.unwrap();
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        let _driver = Driver(tokio::spawn(async move {
            let _ = conn.await;
        }));
        for n in (from + worker..from + count).step_by(WORKERS) {
            let req = Request::builder()
                .uri(format!("/small-{n}"))
                .header("host", "catalog.test")
                .body(Empty::<Bytes>::new())
                .unwrap();
            sender.ready().await.unwrap();
            let response = sender.send_request(req).await.unwrap();
            assert_eq!(response.status(), 200, "/small-{n}");
            response.into_body().collect().await.unwrap();
        }
    });
    futures_util::future::join_all(workers).await;
}

fn memory_peak(id: &str) -> u64 {
    docker(&["exec", id, "cat", "/sys/fs/cgroup/memory.peak"])
        .parse()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn large_object_after_small_objects_fill_storage_is_delivered_whole() {
    let f = fixture("nuke-limit", Profile::Generic, "malloc,64m", false).await;
    fill_small(&f, 0, 40_000).await;
    assert!(stat(&f.cache.id, "MAIN.n_lru_nuked") > 0, "storage is full");
    const BIG: usize = 9 * MIB;
    let mut route = Route::headers(&[("cache-control", "max-age=600")]);
    route
        .headers
        .push(("content-length".into(), BIG.to_string()));
    route.size = Some(BIG);
    f.origin.set("/big", route);
    let received = throttled(f.cache.traffic, "/big", usize::MAX).await;
    // With Varnish's default nuke_limit of 50 the fetch gives up mid-stream and the body is cut.
    assert_eq!(received, Ok((BIG, checksum(BIG))));
    assert_eq!(stat(&f.cache.id, "MAIN.n_lru_limited"), 0);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn small_objects_past_storage_evict_within_the_chart_minimum_memory() {
    // The chart minimum for 64m storage, 16m transient and 32768 objects (64 MiB of 2 KiB ones):
    // storage + transient + 128 MiB base + 1 KiB per object.
    const LIMIT: u64 = (64 + 16 + 128 + 32) * MIB as u64;
    let docker_extra = [
        format!("--memory={LIMIT}"),
        format!("--memory-swap={LIMIT}"),
    ];
    let f = fixture_with(
        "small-objects",
        Profile::Generic,
        "malloc,64m",
        false,
        &docker_extra,
        &["--transient-storage=malloc,16m".into()],
    )
    .await;
    fill_small(&f, 0, 100_000).await;
    let peak = memory_peak(&f.cache.id);
    assert!(stat(&f.cache.id, "MAIN.n_lru_nuked") > 50_000, "LRU evicts");
    assert_eq!(
        docker(&[
            "inspect",
            "-f",
            "{{.State.Running}} {{.State.OOMKilled}} {{.RestartCount}}",
            &f.cache.id
        ]),
        "true false 0"
    );
    assert!(peak < LIMIT, "memory peak {peak}");
    assert_eq!(stat(&f.cache.id, "MAIN.n_lru_limited"), 0);
}
