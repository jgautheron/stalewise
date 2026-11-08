use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

pub(crate) const TOKEN: &str = "integration-token-0123456789abcdef";

pub(crate) fn image() -> String {
    std::env::var("STALEWISE_IMAGE").unwrap_or_else(|_| "stalewise:it".into())
}

pub(crate) fn docker(args: &[&str]) -> String {
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

pub(crate) fn build_image_once() {
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

pub(crate) struct Reply {
    pub(crate) status: u16,
    pub(crate) headers: HashMap<String, String>,
    pub(crate) body: String,
}

impl Reply {
    pub(crate) fn is_hit(&self) -> bool {
        self.headers
            .get("x-varnish")
            .is_some_and(|v| v.split_whitespace().count() == 2)
    }
}

pub(crate) async fn request(addr: SocketAddr, method: &str, path: &str, extra: &str) -> Reply {
    try_request(addr, method, path, extra).await.unwrap()
}

/// Readiness probes use this: Docker's port proxy accepts and then resets until the listener is up.
pub(crate) async fn try_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    extra: &str,
) -> std::io::Result<Reply> {
    tokio::time::timeout(Duration::from_secs(35), async {
        let stream = tokio::net::TcpStream::connect(addr).await?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .map_err(std::io::Error::other)?;
        struct Driver(tokio::task::JoinHandle<()>);
        impl Drop for Driver {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _driver = Driver(tokio::spawn(async move {
            let _ = connection.await;
        }));
        let mut request = hyper::Request::builder()
            .method(method)
            .uri(path)
            .header("host", "Catalog.Test")
            .header("connection", "close");
        for line in extra.lines() {
            if let Some((key, value)) = line.split_once(':') {
                request = request.header(key.trim(), value.trim());
            }
        }
        let response = sender
            .send_request(
                request
                    .body(http_body_util::Empty::<bytes::Bytes>::new())
                    .map_err(std::io::Error::other)?,
            )
            .await
            .map_err(std::io::Error::other)?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(key, value)| {
                (
                    key.to_string(),
                    value.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        let body = http_body_util::BodyExt::collect(response.into_body())
            .await
            .map_err(std::io::Error::other)?
            .to_bytes();
        Ok(Reply {
            status,
            headers,
            body: String::from_utf8_lossy(&body).into_owned(),
        })
    })
    .await
    .map_err(std::io::Error::other)?
}

pub(crate) fn mapped(container: &str, port: &str) -> SocketAddr {
    let out = docker(&["port", container, port]);
    let first = out
        .lines()
        .find(|l| l.starts_with("127.0.0.1"))
        .unwrap_or_default();
    first.parse().unwrap()
}

pub(crate) async fn wait_for(what: &str, timeout: Duration, mut check: impl AsyncFnMut() -> bool) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("timed out waiting for {what}");
}

pub(crate) struct Container {
    pub(crate) id: String,
    pub(crate) traffic: SocketAddr,
    pub(crate) management: SocketAddr,
    pub(crate) signaller: SocketAddr,
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.id]).output();
    }
}

pub(crate) fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("stalewise-it-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::set_permissions(&dir, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    dir
}

pub(crate) async fn run_cache(config_dir: &Path, args: &[String]) -> Container {
    run_cache_with(config_dir, &[], args).await
}

pub(crate) async fn run_cache_with(
    config_dir: &Path,
    docker_extra: &[String],
    args: &[String],
) -> Container {
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
    // Mimics a small CI runner locally, e.g. STALEWISE_IT_CPUS=1.
    if let Ok(cpus) = std::env::var("STALEWISE_IT_CPUS") {
        argv.push(format!("--cpus={cpus}"));
    }
    argv.push(image());
    argv.extend_from_slice(args);
    let refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    let id = docker(&refs);
    let unbound = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut c = Container {
        id,
        traffic: unbound,
        signaller: unbound,
        management: unbound,
    };
    c.traffic = mapped(&c.id, "8080/tcp");
    c.signaller = mapped(&c.id, "8081/tcp");
    c.management = mapped(&c.id, "9090/tcp");
    let management = c.management;
    let id = c.id.clone();
    wait_for("readiness", Duration::from_secs(90), async || {
        let state = docker(&["inspect", "-f", "{{.State.Running}}", &id]);
        assert_eq!(state, "true", "container exited: {}", logs(&id));
        try_request(management, "GET", "/readyz", "")
            .await
            .is_ok_and(|r| r.status == 200)
    })
    .await;
    c
}

pub(crate) fn logs(id: &str) -> String {
    let out = Command::new("docker").args(["logs", id]).output().unwrap();
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

pub(crate) fn base_args(origin_port: u16) -> Vec<String> {
    [
        "--frontend-service=cache-peers",
        "--frontend-service-host=cache.test",
        "--backend-watch=false",
        "--backend-host=host.docker.internal",
        "--signaller",
        "--signaller-token-file=/config/token",
        "--pod-ip=10.255.255.1",
        "--pod-name=cache-it",
        "--log-level=info",
    ]
    .iter()
    .map(|s| (*s).to_owned())
    .chain([format!("--backend-port={origin_port}")])
    .collect()
}

pub(crate) async fn warm(addr: SocketAddr) {
    wait_for("origin health", Duration::from_secs(30), async || {
        request(addr, "GET", "/", "").await.status == 200
    })
    .await;
}

pub(crate) async fn metric(addr: SocketAddr, name: &str) -> f64 {
    let body = request(addr, "GET", "/metrics", "").await.body;
    body.lines()
        .find(|l| l.starts_with(name))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0)
}
