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
use crate::endpoint::{Authority, Cidr, Endpoint};

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
    pub probe_path: String,
    pub uncacheable_ttl: Duration,
    pub max_object_size: u64,
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
}

impl Context {
    /// Builds a context from sorted endpoint lists; rejects identifier collisions.
    pub fn new(
        settings: &RenderSettings,
        frontends: &[Endpoint],
        backends: &[Endpoint],
    ) -> Result<Self, RenderError> {
        check_unique(frontends)?;
        check_unique(backends)?;
        Ok(Self {
            frontends: frontends.to_vec(),
            backends: backends.to_vec(),
            primary_frontend: frontends.first().cloned(),
            primary_backend: backends.first().cloned(),
            env: settings.env.clone(),
            canonical_host: settings.canonical_host.clone(),
            backend_authority: settings.backend_authority.clone(),
            purge_acl: settings.purge_acl.clone(),
            probe_path: settings.probe_path.clone(),
            uncacheable_ttl_seconds: settings.uncacheable_ttl.as_secs().max(1),
            max_object_size: settings.max_object_size,
        })
    }
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
            probe_path: "/healthz".into(),
            uncacheable_ttl: Duration::from_secs(30),
            max_object_size: 10_485_760,
        }
    }

    fn ip(addr: &str, port: u16) -> Endpoint {
        Endpoint::new(Host::Ip(addr.parse().unwrap()), port)
    }

    #[test]
    fn generic_template_renders_every_backend_into_director() {
        let ctx = Context::new(
            &settings(),
            &[],
            &[ip("10.0.0.7", 8080), ip("fd00::1", 8080)],
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
        assert!(out.contains("> 10485760)"));
        assert!(out.contains("set req.http.Host = \"catalog.apps.svc.cluster.local\";"));
    }

    #[test]
    fn generic_template_without_backends_uses_none_backend() {
        let ctx = Context::new(&settings(), &[], &[]).unwrap();
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
        let ctx = Context::new(&settings(), &[], &[dns]).unwrap();
        let out =
            String::from_utf8(render(&Template::embedded(Profile::Mesh).source, &ctx).unwrap())
                .unwrap();
        assert!(out.contains("set bereq.http.Host = \"catalog.apps.svc.cluster.local:8080\";"));
        assert!(out.contains(".host = \"catalog.apps.svc.cluster.local\";"));
        let empty = Context::new(&settings(), &[], &[]).unwrap();
        assert!(render(&Template::embedded(Profile::Mesh).source, &empty).is_err());
    }

    #[test]
    fn strict_undefined_fails_rendering() {
        let ctx = Context::new(&settings(), &[], &[]).unwrap();
        assert!(render("{{ nope }}", &ctx).is_err());
        assert!(render("{{ env.MISSING }}", &ctx).is_err());
    }

    #[test]
    fn vcl_str_escapes_quotes_and_rejects_controls() {
        let ctx = Context::new(&settings(), &[], &[]).unwrap();
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
            Context::new(&settings(), &[], &[a, b]),
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
        let ctx = Context::new(&settings(), &[], &[]).unwrap();
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
