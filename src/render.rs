//! Template rendering, safe on-disk publication and template-source polling.

use std::collections::{BTreeMap, HashSet};
use std::ffi::CString;
use std::fs::File;
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use minijinja::syntax::SyntaxConfig;
use minijinja::value::Serde;
use minijinja::{Environment, UndefinedBehavior};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::config::Profile;
use crate::endpoint::{Authority, Cidr, Endpoint, Host};
use crate::membership::Peer;
use crate::policy::RenderPolicy;

/// Template input and rendered output limit.
pub const MAX_BYTES: usize = 1 << 20;
const FUEL: u64 = 5_000_000;
const OUTPUT_DIR: &str = "run";

/// Rendering or publication failure; the active configuration is left untouched.
#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    #[error("template exceeds {MAX_BYTES} bytes")]
    TooLarge,
    #[error("template or output contains NUL or invalid UTF-8")]
    Encoding,
    #[error("template error: {0}")]
    Template(String),
    #[error("ambiguous VCL identifier {0}")]
    Collision(String),
    #[error("template does not implement --shard (no X-Stalewise-Shard-Hop handling)")]
    ShardUnsupported,
    #[error("unsafe output path {path}: {reason}")]
    UnsafePath { path: PathBuf, reason: &'static str },
    #[error("publish I/O: {0}")]
    Io(#[from] std::io::Error),
}

impl From<minijinja::Error> for RenderError {
    fn from(err: minijinja::Error) -> Self {
        Self::Template(format!(
            "{:?}: {}",
            err.kind(),
            err.detail().unwrap_or_default()
        ))
    }
}

/// An accepted template revision identified by its content hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Template {
    pub source: Arc<str>,
    pub digest: [u8; 32],
}

impl Template {
    /// Validates encoding, size and syntax; never accepts a template that cannot compile.
    pub fn parse(bytes: &[u8]) -> Result<Self, RenderError> {
        if bytes.len() > MAX_BYTES {
            return Err(RenderError::TooLarge);
        }
        let text = std::str::from_utf8(bytes).map_err(|_| RenderError::Encoding)?;
        if text.contains('\0') {
            return Err(RenderError::Encoding);
        }
        environment()?.template_from_str(text)?;
        Ok(Self {
            source: Arc::from(text),
            digest: Sha256::digest(bytes).into(),
        })
    }

    pub fn embedded(profile: Profile) -> Self {
        let source = match profile {
            Profile::Generic => concat!(
                include_str!("templates/generic.vcl.j2"),
                include_str!("templates/policy.vcl.j2")
            ),
            Profile::Mesh => concat!(
                include_str!("templates/mesh.vcl.j2"),
                include_str!("templates/policy.vcl.j2")
            ),
        };
        Self {
            source: Arc::from(source),
            digest: Sha256::digest(source.as_bytes()).into(),
        }
    }
}

/// Process-wide inputs to every render.
#[derive(Debug, Clone)]
pub struct RenderSettings {
    pub env: BTreeMap<String, String>,
    pub canonical_host: Option<Authority>,
    pub backend_authority: Authority,
    pub purge_acl: Vec<Cidr>,
    /// Admit discovered peer pod addresses to the purge ACL, one host entry each.
    pub peer_purge: bool,
    pub probe_path: String,
    pub uncacheable_ttl: Duration,
    pub max_object_size: u64,
    pub shard: Option<ShardSettings>,
}

/// Sharding inputs that do not change with membership.
#[derive(Debug, Clone)]
pub struct ShardSettings {
    pub pod_name: String,
    pub peer_authority: Authority,
    pub local_ttl: Duration,
    pub peer_timeout: Duration,
}

/// Header every sharding template must handle; its absence means the template ignores `--shard`.
pub const SHARD_HOP_HEADER: &str = "X-Stalewise-Shard-Hop";
/// Virtual ring points per peer; every replica renders the same value.
pub const SHARD_REPLICAS: u32 = 67;

/// One ring member; `ident` is stable across address changes.
#[derive(Debug, Clone, Serialize)]
pub struct ShardPeer {
    pub name: String,
    pub host: Host,
    pub port: u16,
    pub ident: String,
}

/// Present whenever sharding is enabled; an empty ring routes every request to the origin.
#[derive(Debug, Clone, Serialize)]
pub struct ShardContext {
    pub peers: Vec<ShardPeer>,
    /// The ring member that is this pod, compared by backend equality in VCL.
    pub self_backend: Option<String>,
    pub replicas: u32,
    /// Discovered peers plus the configured purge ACL; only they may send hop headers.
    pub trusted: Vec<Cidr>,
    pub peer_authority: Authority,
    pub peer_timeout_ms: u128,
    pub local_ttl_seconds: u64,
}

/// The documented template context; field names are the template contract.
#[derive(Debug, Clone, Serialize)]
pub struct Context {
    pub frontends: Vec<Endpoint>,
    pub backends: Vec<Endpoint>,
    pub primary_frontend: Option<Endpoint>,
    pub primary_backend: Option<Endpoint>,
    pub env: BTreeMap<String, String>,
    pub canonical_host: Option<Authority>,
    pub backend_authority: Authority,
    pub purge_acl: Vec<Cidr>,
    pub probe_path: String,
    pub uncacheable_ttl_seconds: u64,
    pub max_object_size: u64,
    /// Accepted CachePolicies, most specific first; `id` is each one's VCL tag.
    pub policies: Vec<RenderPolicy>,
    pub shard: Option<ShardContext>,
}

impl Context {
    /// Builds a context from ordered peers and sorted origins; rejects identifier collisions.
    pub fn new(
        settings: &RenderSettings,
        peers: &[Peer],
        backends: &[Endpoint],
        policies: &[RenderPolicy],
    ) -> Result<Self, RenderError> {
        let frontends: Vec<Endpoint> = peers.iter().map(Peer::endpoint).collect();
        check_unique(&frontends)?;
        check_unique(backends)?;
        let shard = settings
            .shard
            .as_ref()
            .map(|shard| shard_context(settings, shard, peers));
        Ok(Self {
            purge_acl: purge_acl(settings, &frontends),
            primary_frontend: frontends.first().cloned(),
            frontends,
            backends: backends.to_vec(),
            primary_backend: backends.first().cloned(),
            env: settings.env.clone(),
            canonical_host: settings.canonical_host.clone(),
            backend_authority: settings.backend_authority.clone(),
            probe_path: settings.probe_path.clone(),
            uncacheable_ttl_seconds: settings.uncacheable_ttl.as_secs().max(1),
            max_object_size: settings.max_object_size,
            policies: policies.to_vec(),
            shard,
        })
    }
}

fn shard_context(settings: &RenderSettings, shard: &ShardSettings, peers: &[Peer]) -> ShardContext {
    let ring: Vec<ShardPeer> = peers
        .iter()
        .map(|p| ShardPeer {
            name: format!("peer_{}", p.endpoint().name),
            host: Host::Ip(p.address),
            port: p.port,
            ident: p.ident(),
        })
        .collect();
    let self_backend = peers
        .iter()
        .zip(&ring)
        .find(|(p, _)| p.pod_name == shard.pod_name)
        .map(|(_, r)| r.name.clone());
    let mut trusted: Vec<Cidr> = peers.iter().map(|p| Cidr::host(p.address)).collect();
    for net in &settings.purge_acl {
        if !trusted.contains(net) {
            trusted.push(*net);
        }
    }
    ShardContext {
        peers: ring,
        self_backend,
        replicas: SHARD_REPLICAS,
        trusted,
        peer_authority: shard.peer_authority.clone(),
        peer_timeout_ms: shard.peer_timeout.as_millis(),
        local_ttl_seconds: shard.local_ttl.as_secs(),
    }
}

fn purge_acl(settings: &RenderSettings, frontends: &[Endpoint]) -> Vec<Cidr> {
    let mut acl = settings.purge_acl.clone();
    let peers = frontends.iter().filter_map(|e| match e.host {
        Host::Ip(ip) if settings.peer_purge => Some(Cidr::host(ip)),
        _ => None,
    });
    for peer in peers {
        if !acl.contains(&peer) {
            acl.push(peer);
        }
    }
    acl
}

fn check_unique(endpoints: &[Endpoint]) -> Result<(), RenderError> {
    let mut seen = HashSet::new();
    for endpoint in endpoints {
        if !seen.insert(endpoint.name.as_str()) {
            return Err(RenderError::Collision(endpoint.name.clone()));
        }
    }
    Ok(())
}

fn environment() -> Result<Environment<'static>, RenderError> {
    let mut env = Environment::new();
    env.set_undefined_behavior(UndefinedBehavior::Strict);
    env.set_fuel(Some(FUEL));
    env.set_syntax(SyntaxConfig::builder().trim_blocks(true).build()?);
    env.add_filter("vcl_str", vcl_str);
    Ok(env)
}

/// Emits a VCL string literal; VCL has no escapes, so quotes force the long-string form.
pub fn vcl_str(value: String) -> Result<String, minijinja::Error> {
    let bad =
        |why: &str| minijinja::Error::new(minijinja::ErrorKind::InvalidOperation, why.to_owned());
    if value.chars().any(char::is_control) {
        return Err(bad("vcl_str rejects control characters"));
    }
    if !value.contains('"') {
        return Ok(format!("\"{value}\""));
    }
    if value.contains("\"}") {
        return Err(bad("vcl_str cannot embed '\"}'"));
    }
    Ok(format!("{{\"{value}\"}}"))
}

/// Renders a template into bounded bytes with strict undefined variables.
pub fn render(template: &str, context: &Context) -> Result<Vec<u8>, RenderError> {
    if template.len() > MAX_BYTES {
        return Err(RenderError::TooLarge);
    }
    let env = environment()?;
    let output = env.template_from_str(template)?.render(Serde(context))?;
    if output.len() > MAX_BYTES {
        return Err(RenderError::TooLarge);
    }
    if output.contains('\0') {
        return Err(RenderError::Encoding);
    }
    if context.shard.is_some() && !output.contains(SHARD_HOP_HEADER) {
        return Err(RenderError::ShardUnsupported);
    }
    Ok(output.into_bytes())
}

/// A private output directory reached without following any symlink.
#[derive(Debug)]
pub struct OutputDir {
    fd: OwnedFd,
    path: PathBuf,
}

impl OutputDir {
    /// Opens `<work_dir>/run`, creating it 0700; rejects symlinks and group/other-writable modes.
    pub fn open(work_dir: &Path) -> Result<Self, RenderError> {
        let parent = open_nofollow_path(work_dir)?;
        let name = c_name(OUTPUT_DIR)?;
        // SAFETY: `parent` is a live directory descriptor and `name` is NUL-terminated.
        let rc = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(err.into());
            }
        }
        let fd = open_dir_at(&parent, &name)?;
        let path = work_dir.join(OUTPUT_DIR);
        let meta = File::from(fd.try_clone()?).metadata()?;
        if std::os::unix::fs::PermissionsExt::mode(&meta.permissions()) & 0o022 != 0 {
            return Err(RenderError::UnsafePath {
                path,
                reason: "writable by group or others",
            });
        }
        Ok(Self { fd, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Atomically replaces `file_name` with `bytes`: O_EXCL 0600 temp, fsync, rename, dir fsync.
    pub fn publish(&self, file_name: &str, bytes: &[u8]) -> Result<PathBuf, RenderError> {
        if file_name.contains('/') || file_name.starts_with('.') || file_name.is_empty() {
            return Err(RenderError::UnsafePath {
                path: self.path.join(file_name),
                reason: "file name must be a plain relative name",
            });
        }
        let suffix: u64 = rand::random();
        let tmp = c_name(&format!(".{file_name}.{suffix:016x}.tmp"))?;
        let target = c_name(file_name)?;
        let dir = self.fd.as_raw_fd();
        let flags =
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC;
        // SAFETY: `dir` stays open for `self`'s lifetime and `tmp` is NUL-terminated.
        let raw = unsafe { libc::openat(dir, tmp.as_ptr(), flags, 0o600 as libc::c_uint) };
        if raw < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: `raw` is a freshly opened descriptor owned by nobody else.
        let mut file = File::from(unsafe { OwnedFd::from_raw_fd(raw) });
        let written = file
            .write_all(bytes)
            .and_then(|()| file.sync_all())
            .and_then(|()| renameat(dir, &tmp, &target));
        if let Err(err) = written {
            // SAFETY: same descriptor/name invariants as above; failure only leaves a temp file.
            unsafe { libc::unlinkat(dir, tmp.as_ptr(), 0) };
            return Err(err.into());
        }
        File::from(self.fd.try_clone()?).sync_all()?;
        Ok(self.path.join(file_name))
    }
}

fn renameat(dir: libc::c_int, from: &CString, to: &CString) -> std::io::Result<()> {
    // SAFETY: both names are NUL-terminated and resolved relative to the open `dir`.
    let rc = unsafe { libc::renameat(dir, from.as_ptr(), dir, to.as_ptr()) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn c_name(name: &str) -> Result<CString, RenderError> {
    CString::new(name).map_err(|_| RenderError::Encoding)
}

fn open_dir_at(parent: &OwnedFd, name: &CString) -> Result<OwnedFd, RenderError> {
    let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;
    // SAFETY: `parent` is a live directory descriptor and `name` is NUL-terminated.
    let raw = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
    if raw < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: `raw` was just returned by openat and is not owned elsewhere.
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

/// Walks an absolute path one component at a time with O_NOFOLLOW.
fn open_nofollow_path(path: &Path) -> Result<OwnedFd, RenderError> {
    let unsafe_path = |reason| RenderError::UnsafePath {
        path: path.to_owned(),
        reason,
    };
    let mut components = path.components();
    if components.next() != Some(Component::RootDir) {
        return Err(unsafe_path("must be absolute"));
    }
    let root = File::open("/")?;
    let mut fd = OwnedFd::from(root);
    for component in components {
        let Component::Normal(part) = component else {
            return Err(unsafe_path("must not contain . or .."));
        };
        let name = CString::new(part.as_bytes()).map_err(|_| RenderError::Encoding)?;
        fd = open_dir_at(&fd, &name).map_err(|err| match err {
            RenderError::Io(io)
                if io.raw_os_error() == Some(libc::ELOOP)
                    || io.raw_os_error() == Some(libc::ENOTDIR) =>
            {
                unsafe_path("a component is a symlink or not a directory")
            }
            other => other,
        })?;
    }
    Ok(fd)
}

/// Polls a template file by content hash, following ConfigMap `..data` symlink swaps.
pub async fn poll(
    path: PathBuf,
    interval: Duration,
    out: watch::Sender<Option<Template>>,
    stop: CancellationToken,
) {
    let mut seen: Option<[u8; 32]> = None;
    loop {
        match read_bounded(&path).await {
            Ok(bytes) => accept_revision(&bytes, &mut seen, &out),
            Err(err) => {
                tracing::warn!(error = %err, "template read failed; keeping last accepted revision")
            }
        }
        tokio::select! {
            () = stop.cancelled() => return,
            () = tokio::time::sleep(interval) => {}
        }
    }
}

fn accept_revision(
    bytes: &[u8],
    seen: &mut Option<[u8; 32]>,
    out: &watch::Sender<Option<Template>>,
) {
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    if *seen == Some(digest) {
        return;
    }
    *seen = Some(digest);
    match Template::parse(bytes) {
        Ok(template) => {
            tracing::info!(bytes = bytes.len(), "accepted template revision");
            out.send_replace(Some(template));
        }
        Err(err) => {
            tracing::warn!(error = %err, "rejected template revision; keeping last accepted")
        }
    }
}

async fn read_bounded(path: &Path) -> std::io::Result<Vec<u8>> {
    let file = tokio::fs::File::open(path).await?;
    let mut bytes = Vec::new();
    file.take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .await?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::Host;

    fn settings() -> RenderSettings {
        RenderSettings {
            env: BTreeMap::from([("SHARD".into(), "a\"b".into())]),
            canonical_host: Some("catalog.apps.svc.cluster.local".parse().unwrap()),
            backend_authority: "catalog.apps.svc.cluster.local:8080".parse().unwrap(),
            purge_acl: vec!["127.0.0.1".parse().unwrap(), "10.0.0.0/8".parse().unwrap()],
            peer_purge: false,
            probe_path: "/healthz".into(),
            uncacheable_ttl: Duration::from_secs(30),
            max_object_size: 10_485_760,
            shard: None,
        }
    }

    fn ip(addr: &str, port: u16) -> Endpoint {
        Endpoint::new(Host::Ip(addr.parse().unwrap()), port)
    }

    fn peer(name: &str, addr: &str) -> Peer {
        Peer {
            pod_name: name.into(),
            namespace: "apps".into(),
            address: addr.parse().unwrap(),
            port: 8080,
        }
    }

    fn sharded(local_ttl: u64) -> RenderSettings {
        RenderSettings {
            shard: Some(ShardSettings {
                pod_name: "cache-1".into(),
                peer_authority: "cache-peers.apps.svc.cluster.local:8080".parse().unwrap(),
                local_ttl: Duration::from_secs(local_ttl),
                peer_timeout: Duration::from_millis(1500),
            }),
            ..settings()
        }
    }

    fn render_generic(s: &RenderSettings, peers: &[Peer]) -> String {
        let ctx = Context::new(s, peers, &[ip("10.0.0.7", 8080)], &[]).unwrap();
        String::from_utf8(render(&Template::embedded(Profile::Generic).source, &ctx).unwrap())
            .unwrap()
    }

    #[test]
    fn shard_ring_uses_stable_identities_and_checks_every_initialisation() {
        let peers = [peer("cache-0", "10.1.0.1"), peer("cache-1", "10.1.0.2")];
        let out = render_generic(&sharded(0), &peers);
        for fragment in [
            "new stalewise_owners = directors.shard();",
            "if (!stalewise_owners.add_backend(peer_ip_10_1_0_1_8080_v4, ident=\"apps/cache-0\")) {",
            "if (!stalewise_owners.reconfigure(67)) {",
            ".first_byte_timeout = 1500ms;",
            "if (req.backend_hint == peer_ip_10_1_0_2_8080_v4) {",
            "set bereq.http.Host = \"cache-peers.apps.svc.cluster.local:8080\";",
            "healthy=IGNORE, warmup=0, rampup=false, resolve=NOW);",
            "\"10.1.0.1\"/32;",
        ] {
            assert!(out.contains(fragment), "missing {fragment}");
        }
        assert_eq!(out.matches("return (fail);").count(), 3);
        assert!(
            out.contains("return (pass);\n        }\n    }\n}"),
            "non-owners store no bodies by default"
        );
        assert!(!out.contains("sub stalewise_local_copy"));
        let moved = [peer("cache-0", "10.9.9.9"), peer("cache-1", "10.1.0.2")];
        assert!(render_generic(&sharded(0), &moved).contains("ident=\"apps/cache-0\""));
    }

    #[test]
    fn shard_local_copies_render_only_when_enabled() {
        let out = render_generic(&sharded(10), &[peer("cache-0", "10.1.0.1")]);
        assert!(out.contains("sub stalewise_local_copy"));
        assert!(out.contains("set beresp.ttl = 10s;"));
        assert!(out.contains("call stalewise_fallback_cap;"));
        assert!(
            !out.contains("req.backend_hint == peer_"),
            "this pod is not a member yet"
        );
    }

    #[test]
    fn empty_membership_fetches_directly_but_still_honours_hop_headers() {
        let out = render_generic(&sharded(0), &[]);
        assert!(!out.contains("directors.shard()"));
        assert!(!out.contains("call stalewise_shard_route;"));
        assert!(out.contains("call stalewise_peer_request;"));
        let off = render_generic(&settings(), &[peer("cache-0", "10.1.0.1")]);
        assert!(!off.contains("stalewise_peers"));
        assert!(off.contains("unset req.http.X-Stalewise-Shard-Hop;"));
    }

    #[test]
    fn mesh_template_supports_sharding() {
        let dns = Endpoint::new(
            Host::Dns("catalog.apps.svc.cluster.local".parse().unwrap()),
            8080,
        );
        let ctx = Context::new(&sharded(0), &[peer("cache-0", "10.1.0.1")], &[dns], &[]).unwrap();
        let out =
            String::from_utf8(render(&Template::embedded(Profile::Mesh).source, &ctx).unwrap())
                .unwrap();
        assert!(out.contains("import directors;"));
        assert!(out.contains("set bereq.backend = stalewise_origin;"));
    }

    #[test]
    fn custom_templates_without_the_hop_contract_reject_sharding() {
        let ctx = Context::new(&sharded(0), &[], &[ip("10.0.0.7", 8080)], &[]).unwrap();
        assert!(matches!(
            render("vcl 4.1;", &ctx),
            Err(RenderError::ShardUnsupported)
        ));
        assert!(render("vcl 4.1; # X-Stalewise-Shard-Hop", &ctx).is_ok());
    }

    #[test]
    fn generic_template_renders_every_backend_into_director() {
        let ctx = Context::new(
            &settings(),
            &[],
            &[ip("10.0.0.7", 8080), ip("fd00::1", 8080)],
            &[],
        )
        .unwrap();
        let out =
            String::from_utf8(render(&Template::embedded(Profile::Generic).source, &ctx).unwrap())
                .unwrap();
        assert!(out.contains("backend ip_10_0_0_7_8080_v4 {"));
        assert!(out.contains(".host = \"fd00::1\";"));
        assert!(out.contains("stalewise_origin.add_backend(ip_fd00__1_8080_v6);"));
        assert!(out.contains("\"10.0.0.0\"/8;"));
        assert!(out.contains("set beresp.ttl = 30s;"));
        assert!(out.contains("std.integer(beresp.http.X-Stalewise-Max, 10485760)"));
        assert!(out.contains("set req.http.Host = \"catalog.apps.svc.cluster.local\";"));
    }

    #[test]
    fn peer_purge_admits_each_discovered_peer_as_a_single_host() {
        let mut with = settings();
        with.purge_acl = vec!["127.0.0.1".parse().unwrap()];
        with.peer_purge = true;
        let peers = [peer("a", "10.1.2.3"), peer("b", "fd00::9")];
        let render_acl = |s: &RenderSettings| {
            let ctx = Context::new(s, &peers, &[ip("10.0.0.7", 8080)], &[]).unwrap();
            String::from_utf8(render(&Template::embedded(Profile::Generic).source, &ctx).unwrap())
                .unwrap()
        };
        let out = render_acl(&with);
        assert!(out.contains("\"10.1.2.3\"/32;"));
        assert!(out.contains("\"fd00::9\"/128;"));
        with.peer_purge = false;
        let out = render_acl(&with);
        assert!(
            !out.contains("10.1.2.3"),
            "peers are untrusted without the signaller"
        );
    }

    #[test]
    fn generic_template_without_backends_uses_none_backend() {
        let ctx = Context::new(&settings(), &[], &[], &[]).unwrap();
        let out =
            String::from_utf8(render(&Template::embedded(Profile::Generic).source, &ctx).unwrap())
                .unwrap();
        assert!(out.contains("backend stalewise_none none;"));
        assert!(!out.contains("probe stalewise_origin_probe"));
    }

    #[test]
    fn mesh_template_rewrites_backend_authority_and_needs_a_backend() {
        let dns = Endpoint::new(
            Host::Dns("catalog.apps.svc.cluster.local".parse().unwrap()),
            8080,
        );
        let ctx = Context::new(&settings(), &[], &[dns], &[]).unwrap();
        let out =
            String::from_utf8(render(&Template::embedded(Profile::Mesh).source, &ctx).unwrap())
                .unwrap();
        assert!(out.contains("set bereq.http.Host = \"catalog.apps.svc.cluster.local:8080\";"));
        assert!(out.contains(".host = \"catalog.apps.svc.cluster.local\";"));
        let empty = Context::new(&settings(), &[], &[], &[]).unwrap();
        assert!(render(&Template::embedded(Profile::Mesh).source, &empty).is_err());
    }

    fn policies() -> Vec<RenderPolicy> {
        let own = crate::policy::OwnService {
            name: "catalog".into(),
            port_name: None,
            port_number: None,
        };
        let candidates: Vec<crate::policy::Candidate> = [
            ("root", "serviceRef: {name: catalog}\npathPrefix: /\noverrides: {ttlSeconds: 0}"),
            ("api", r#"
serviceRef: {name: catalog}
pathPrefix: /api/
overrides:
  ttlSeconds: 120
  graceSeconds: 15
  keepSeconds: 60
  cacheableMethods: [GET]
  cacheableStatusCodes: [200, 404]
  maxObjectBytes: 2048
  key: {headers: [X-Version], queryAllowList: [page, a.b]}
  cookies: {mode: stripListed, names: [analytics]}
bypass:
  - pathPrefix: /api/account
  - header: {name: X-Preview, exact: "true"}
"#),
            ("plain", "serviceRef: {name: catalog}\npathPrefix: /static\noverrides: {key: {queryAllowList: []}}"),
        ]
        .iter()
        .map(|(name, yaml)| crate::policy::Candidate {
            name: (*name).into(),
            generation: 1,
            outcome: crate::policy::validate(name, 1, &serde_norway::from_str(yaml).unwrap(), &own),
        })
        .collect();
        crate::policy::resolve(&candidates, &BTreeMap::new()).policies
    }

    fn render_with(policies: &[RenderPolicy]) -> String {
        let ctx = Context::new(&settings(), &[], &[ip("10.0.0.7", 8080)], policies).unwrap();
        String::from_utf8(render(&Template::embedded(Profile::Generic).source, &ctx).unwrap())
            .unwrap()
    }

    #[test]
    fn policies_render_most_specific_match_first_on_both_sides() {
        let out = render_with(&policies());
        let api = out.find(r#"if (req.url ~ "^/api(/|\?|$)")"#).unwrap();
        let root = out.find(r#"} elsif (req.url ~ "^/")"#).unwrap();
        assert!(api < root, "longest prefix is tested first");
        assert!(out.contains(r#"if (bereq.url ~ "^/api(/|\?|$)") {"#));
        assert!(out.contains(r#"set beresp.http.X-Stalewise-Max = "2048";"#));
    }

    #[test]
    fn policy_overrides_render_beside_header_defaults() {
        let out = render_with(&policies());
        for fragment in [
            r#"set beresp.http.X-Stalewise-Fresh = "120";"#,
            r#"set beresp.http.X-Stalewise-Fresh = "0";"#,
            r#"set beresp.http.X-Stalewise-Swr = "15";"#,
            "set beresp.keep = 60s;",
            r#"if (req.url ~ "^/api/account(/|\?|$)") {"#,
            r#"if (req.http.X-Preview == "true") {"#,
            r#"if (req.method != "GET") {"#,
            r#"(?:page|a\.b)(?:=|&|$)"#,
            r#"(^|;)\s*(analytics)=[^;]*"#,
            "hash_data(req.http.X-Version);",
            r#"beresp.http.X-Stalewise-Policy != "2" &&"#,
            "if (beresp.status != 200 &&\n            beresp.status != 404) {",
            r#"set req.http.X-Stalewise-Key-Url = regsub(req.url, "\?.*$", "");"#,
        ] {
            assert!(out.contains(fragment), "missing {fragment}");
        }
    }

    #[test]
    fn no_policies_renders_header_only_behaviour() {
        let out = render_with(&[]);
        assert!(!out.contains("X-Stalewise-Policy = "));
        assert!(!out.contains("X-Stalewise-Policy =="));
        assert!(out.contains("std.integer(beresp.http.X-Stalewise-Max, 10485760)"));
    }

    #[test]
    fn strict_undefined_fails_rendering() {
        let ctx = Context::new(&settings(), &[], &[], &[]).unwrap();
        assert!(render("{{ nope }}", &ctx).is_err());
        assert!(render("{{ env.MISSING }}", &ctx).is_err());
    }

    #[test]
    fn vcl_str_escapes_quotes_and_rejects_controls() {
        let ctx = Context::new(&settings(), &[], &[], &[]).unwrap();
        let out = render("{{ env.SHARD | vcl_str }}", &ctx).unwrap();
        assert_eq!(out, b"{\"a\"b\"}");
        assert_eq!(vcl_str("plain".into()).unwrap(), "\"plain\"");
        assert!(vcl_str("a\nb".into()).is_err());
        assert!(vcl_str("x\"}y".into()).is_err());
    }

    #[test]
    fn identifier_collisions_are_rejected() {
        let a = Endpoint::new(Host::Dns("a-b.svc".parse().unwrap()), 80);
        let b = Endpoint::new(Host::Dns("a.b.svc".parse().unwrap()), 80);
        assert!(matches!(
            Context::new(&settings(), &[], &[a, b], &[]),
            Err(RenderError::Collision(_))
        ));
    }

    #[test]
    fn oversized_or_invalid_templates_are_rejected() {
        assert!(matches!(
            Template::parse(&vec![b'a'; MAX_BYTES + 1]),
            Err(RenderError::TooLarge)
        ));
        assert!(matches!(
            Template::parse(b"a\0b"),
            Err(RenderError::Encoding)
        ));
        assert!(matches!(
            Template::parse(&[0xff]),
            Err(RenderError::Encoding)
        ));
        assert!(Template::parse(b"{% if %}").is_err());
        let ctx = Context::new(&settings(), &[], &[], &[]).unwrap();
        assert!(matches!(
            render("{% for i in range(2000000) %}x{% endfor %}", &ctx),
            Err(RenderError::TooLarge | RenderError::Template(_))
        ));
    }

    #[test]
    fn publish_is_private_atomic_and_replaces_symlinks_without_following() {
        let work = crate::testutil::temp_dir("publish");
        let dir = OutputDir::open(&work).unwrap();
        let path = dir.publish("active.vcl", b"one").unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions();
        assert_eq!(
            std::os::unix::fs::PermissionsExt::mode(&mode) & 0o777,
            0o600
        );
        let victim = work.join("victim");
        std::fs::write(&victim, b"untouched").unwrap();
        std::fs::remove_file(&path).unwrap();
        std::os::unix::fs::symlink(&victim, &path).unwrap();
        dir.publish("active.vcl", b"two").unwrap();
        assert_eq!(std::fs::read(&victim).unwrap(), b"untouched");
        assert_eq!(std::fs::read(&path).unwrap(), b"two");
        assert!(
            !std::fs::symlink_metadata(&path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        let leftovers = std::fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(leftovers, 1, "no temp files remain");
    }

    #[test]
    fn output_dir_rejects_symlinked_components_and_open_modes() {
        let work = crate::testutil::temp_dir("unsafe-dir");
        let real = work.join("real");
        std::fs::create_dir(&real).unwrap();
        let link = work.join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(matches!(
            OutputDir::open(&link),
            Err(RenderError::UnsafePath { .. })
        ));
        assert!(OutputDir::open(Path::new("relative")).is_err());
        let open = work.join("open");
        std::fs::create_dir_all(open.join("run")).unwrap();
        std::fs::set_permissions(
            open.join("run"),
            std::os::unix::fs::PermissionsExt::from_mode(0o777),
        )
        .unwrap();
        assert!(matches!(
            OutputDir::open(&open),
            Err(RenderError::UnsafePath { .. })
        ));
    }

    #[tokio::test]
    async fn poll_follows_configmap_symlink_swaps_and_keeps_last_good() {
        let dir = crate::testutil::temp_dir("poll");
        for (rev, body) in [
            ("rev1", "one {{ probe_path }}"),
            ("rev2", "{% if %}"),
            ("rev3", "three"),
        ] {
            std::fs::create_dir(dir.join(rev)).unwrap();
            std::fs::write(dir.join(rev).join("t.vcl"), body).unwrap();
        }
        std::os::unix::fs::symlink("rev1", dir.join("..data")).unwrap();
        std::os::unix::fs::symlink("..data/t.vcl", dir.join("t.vcl")).unwrap();
        let (tx, mut rx) = watch::channel(None);
        let stop = CancellationToken::new();
        let task = tokio::spawn(poll(
            dir.join("t.vcl"),
            Duration::from_millis(10),
            tx,
            stop.clone(),
        ));
        rx.changed().await.unwrap();
        assert_eq!(
            &*rx.borrow().as_ref().unwrap().source,
            "one {{ probe_path }}"
        );
        swap_data(&dir, "rev2");
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            &*rx.borrow().as_ref().unwrap().source,
            "one {{ probe_path }}"
        );
        swap_data(&dir, "rev3");
        rx.changed().await.unwrap();
        assert_eq!(&*rx.borrow().as_ref().unwrap().source, "three");
        stop.cancel();
        task.await.unwrap();
    }

    fn swap_data(dir: &Path, rev: &str) {
        let tmp = dir.join("..data_tmp");
        std::os::unix::fs::symlink(rev, &tmp).unwrap();
        std::fs::rename(&tmp, dir.join("..data")).unwrap();
    }
}
