//! Owns the `varnishd -F` child: spawn, admin readiness, signal forwarding and reaping.

use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::config::VarnishParam;
use crate::varnish::admin::{self, AdminConfig};

const ADMIN_POLL: Duration = Duration::from_millis(200);
const MAX_LOG_LINE: usize = 256;
const ZERO_DEFAULTS: [&str; 3] = ["default_ttl=0", "default_grace=0", "default_keep=0"];
/// Varnish holds the lurker off new bans for 60s by default; the guard needs eviction within
/// seconds, and a short hold keeps `obj.age` drift past the ban's T to about a second. Evicted
/// hash entries linger two cooloffs (2 x 180s by default); 60s is Varnish's own minimum.
const MEMGUARD_PARAMS: [&str; 2] = ["ban_lurker_age=1", "critbit_cooloff=60"];

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("cannot spawn varnishd: {0}")]
    Spawn(std::io::Error),
    #[error("varnishd exited during startup: {0}")]
    ExitedDuringStartup(ExitStatus),
    #[error("varnishd admin port did not answer within the startup timeout")]
    StartupTimeout,
    #[error("varnishd exited unexpectedly: {0}")]
    UnexpectedExit(ExitStatus),
    #[error("waiting for varnishd failed: {0}")]
    Wait(std::io::Error),
}

#[derive(Debug, Clone)]
pub struct ProcessConfig {
    pub bin: PathBuf,
    pub initial_vcl: PathBuf,
    pub admin: AdminConfig,
    pub listen: SocketAddr,
    pub storage: String,
    pub transient_storage: String,
    pub transit_buffer: u64,
    pub nuke_limit: u32,
    pub work_dir: PathBuf,
    pub params: Vec<VarnishParam>,
    /// The memory guard's bans need the lurker to act within seconds.
    pub memguard: bool,
    /// Caps glibc malloc arenas in varnishd; `None` keeps glibc's default of 8 per core, which
    /// leaves freed object memory scattered and inflated RSS by a third under churn.
    pub malloc_arena_max: Option<u32>,
    pub startup_timeout: Duration,
    pub shutdown_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessState {
    Starting,
    Running,
    Exited,
}

/// The exact argv, without a shell; zero TTL/grace/keep defaults, transit buffer and nuke limit precede
/// operator params, so `--varnish-param` can override them.
pub fn arguments(config: &ProcessConfig) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec![
        "-F".into(),
        "-f".into(),
        config.initial_vcl.clone().into(),
        "-S".into(),
        config.admin.secret_file.clone().into(),
        "-T".into(),
        config.admin.addr.to_string().into(),
        "-a".into(),
        config.listen.to_string().into(),
        "-s".into(),
        config.storage.clone().into(),
        "-s".into(),
        format!("Transient={}", config.transient_storage).into(),
        "-n".into(),
        config.work_dir.clone().into(),
    ];
    let params = ZERO_DEFAULTS
        .iter()
        .map(|p| (*p).to_owned())
        .chain([
            format!("transit_buffer={}", config.transit_buffer),
            format!("nuke_limit={}", config.nuke_limit),
        ])
        .chain(
            MEMGUARD_PARAMS
                .iter()
                .filter(|_| config.memguard)
                .map(|p| (*p).to_owned()),
        )
        .chain(
            config
                .params
                .iter()
                .map(|p| format!("{}={}", p.key, p.value)),
        );
    for param in params {
        args.push("-p".into());
        args.push(param.into());
    }
    args
}

enum Phase {
    Ready,
    Failed(ProcessError),
    Stop,
}

/// Runs the child until `stop` (clean exit after reaping) or until it dies (error).
pub async fn supervise(
    config: ProcessConfig,
    state: watch::Sender<ProcessState>,
    stop: CancellationToken,
) -> Result<ExitStatus, ProcessError> {
    let mut command = Command::new(&config.bin);
    if let Some(arenas) = config.malloc_arena_max {
        command.env("MALLOC_ARENA_MAX", arenas.to_string());
    }
    let mut child = command
        .args(arguments(&config))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(ProcessError::Spawn)?;
    drain_output(&mut child);
    state.send_replace(ProcessState::Starting);
    let phase = tokio::select! {
        status = child.wait() => match status {
            Ok(status) => Phase::Failed(ProcessError::ExitedDuringStartup(status)),
            Err(err) => Phase::Failed(ProcessError::Wait(err)),
        },
        () = tokio::time::sleep(config.startup_timeout) => Phase::Failed(ProcessError::StartupTimeout),
        () = wait_admin(&config.admin) => Phase::Ready,
        () = stop.cancelled() => Phase::Stop,
    };
    match phase {
        Phase::Ready => {}
        Phase::Stop => return finish(&mut child, &config, &state).await,
        Phase::Failed(err) => {
            let _ = finish(&mut child, &config, &state).await;
            return Err(err);
        }
    }
    tracing::info!(pid = child.id(), "varnishd admin interface ready");
    state.send_replace(ProcessState::Running);
    tokio::select! {
        status = child.wait() => {
            state.send_replace(ProcessState::Exited);
            Err(status.map_or_else(ProcessError::Wait, ProcessError::UnexpectedExit))
        }
        () = stop.cancelled() => finish(&mut child, &config, &state).await,
    }
}

async fn wait_admin(config: &AdminConfig) {
    loop {
        if let Ok(mut client) = admin::connect(config).await
            && client.ping().await.is_ok()
        {
            return;
        }
        tokio::time::sleep(ADMIN_POLL).await;
    }
}

/// SIGTERM, wait out the shutdown budget, then SIGKILL; always reaps.
async fn finish(
    child: &mut Child,
    config: &ProcessConfig,
    state: &watch::Sender<ProcessState>,
) -> Result<ExitStatus, ProcessError> {
    if let Some(pid) = child.id().and_then(|pid| libc::pid_t::try_from(pid).ok()) {
        // SAFETY: `pid` is our own child and is not yet reaped, so it cannot name another process.
        unsafe { libc::kill(pid, libc::SIGTERM) };
    }
    let status = match tokio::time::timeout(config.shutdown_timeout, child.wait()).await {
        Ok(status) => status,
        Err(_) => {
            tracing::warn!("varnishd ignored SIGTERM within the shutdown budget; killing");
            child.kill().await.map_err(ProcessError::Wait)?;
            child.wait().await
        }
    };
    state.send_replace(ProcessState::Exited);
    status.map_err(ProcessError::Wait)
}

fn drain_output(child: &mut Child) {
    if let Some(out) = child.stdout.take() {
        tokio::spawn(drain(out, "stdout"));
    }
    if let Some(err) = child.stderr.take() {
        tokio::spawn(drain(err, "stderr"));
    }
}

/// Reads continuously so the child never blocks; logs bounded lines with excerpts suppressed.
async fn drain(mut reader: impl AsyncRead + Unpin, stream: &'static str) {
    let mut chunk = [0u8; 4096];
    let mut filter = LineFilter::default();
    loop {
        let n = match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        for line in filter.feed(chunk.get(..n).unwrap_or_default()) {
            tracing::info!(stream, line = %line, "varnishd");
        }
    }
}

/// Splits output into bounded lines and drops source excerpts following a VCC location.
#[derive(Debug, Default)]
pub struct LineFilter {
    partial: Vec<u8>,
    skip: u8,
}

impl LineFilter {
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<String> {
        let mut lines = Vec::new();
        for &b in bytes {
            if b != b'\n' {
                if self.partial.len() < MAX_LOG_LINE {
                    self.partial.push(b);
                }
                continue;
            }
            let line: String = String::from_utf8_lossy(&self.partial)
                .chars()
                .map(|c| if c.is_control() { ' ' } else { c })
                .collect();
            self.partial.clear();
            if self.skip > 0 {
                self.skip -= 1;
                continue;
            }
            if line.trim_start().starts_with("At: (") {
                self.skip = 2;
            }
            if !line.trim().is_empty() {
                lines.push(line);
            }
        }
        lines
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varnish::admin::fake::{Behavior, FakeVarnish};

    fn script(body: &str) -> PathBuf {
        let dir = crate::testutil::temp_dir("process");
        let path = dir.join("varnishd");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o755))
            .unwrap();
        path
    }

    fn config(bin: PathBuf, admin: AdminConfig) -> ProcessConfig {
        ProcessConfig {
            bin,
            initial_vcl: "/w/initial.vcl".into(),
            admin,
            listen: "0.0.0.0:8080".parse().unwrap(),
            storage: "malloc,256m".into(),
            transient_storage: "malloc,64m".into(),
            transit_buffer: 1_048_576,
            nuke_limit: 10_000,
            work_dir: "/var/lib/varnish".into(),
            params: vec![VarnishParam {
                key: "thread_pools".into(),
                value: "2".into(),
            }],
            memguard: true,
            malloc_arena_max: Some(4),
            startup_timeout: Duration::from_secs(2),
            shutdown_timeout: Duration::from_millis(500),
        }
    }

    fn dead_admin() -> AdminConfig {
        AdminConfig {
            addr: "127.0.0.1:9".parse().unwrap(),
            secret_file: "/nonexistent".into(),
            timeout: Duration::from_millis(50),
        }
    }

    #[test]
    fn argv_is_exact_and_zeroes_defaults_first() {
        let args = arguments(&config("varnishd".into(), dead_admin()));
        let text: Vec<&str> = args.iter().map(|a| a.to_str().unwrap()).collect();
        assert_eq!(
            text,
            [
                "-F",
                "-f",
                "/w/initial.vcl",
                "-S",
                "/nonexistent",
                "-T",
                "127.0.0.1:9",
                "-a",
                "0.0.0.0:8080",
                "-s",
                "malloc,256m",
                "-s",
                "Transient=malloc,64m",
                "-n",
                "/var/lib/varnish",
                "-p",
                "default_ttl=0",
                "-p",
                "default_grace=0",
                "-p",
                "default_keep=0",
                "-p",
                "transit_buffer=1048576",
                "-p",
                "nuke_limit=10000",
                "-p",
                "ban_lurker_age=1",
                "-p",
                "critbit_cooloff=60",
                "-p",
                "thread_pools=2",
            ]
        );
    }

    #[tokio::test]
    async fn child_exit_during_startup_is_an_error() {
        let (tx, rx) = watch::channel(ProcessState::Starting);
        let mut cfg = config(
            script("echo 'At: (x Line 1 Pos 1)'; echo secret_source; exit 0"),
            dead_admin(),
        );
        // The admin port is dead, so with no reachable timeout only the child's exit can end startup.
        cfg.startup_timeout = Duration::from_secs(3600);
        let err = supervise(cfg, tx, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(err, ProcessError::ExitedDuringStartup(_)));
        assert_eq!(*rx.borrow(), ProcessState::Exited);
    }

    #[tokio::test]
    async fn startup_timeout_kills_and_reaps() {
        let (tx, _rx) = watch::channel(ProcessState::Starting);
        let mut cfg = config(script("exec sleep 30"), dead_admin());
        cfg.startup_timeout = Duration::from_millis(300);
        let err = supervise(cfg, tx, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(err, ProcessError::StartupTimeout));
    }

    #[tokio::test]
    async fn stop_forwards_sigterm_and_reaps() {
        let fake = FakeVarnish::start(Behavior::default()).await;
        let (tx, mut rx) = watch::channel(ProcessState::Starting);
        let stop = CancellationToken::new();
        let task = tokio::spawn(supervise(
            config(script("exec sleep 30"), fake.config()),
            tx,
            stop.clone(),
        ));
        rx.wait_for(|s| *s == ProcessState::Running).await.unwrap();
        stop.cancel();
        let status = task.await.unwrap().unwrap();
        assert!(!status.success(), "terminated by SIGTERM");
        assert_eq!(*rx.borrow(), ProcessState::Exited);
    }

    #[tokio::test]
    async fn ignored_sigterm_escalates_to_sigkill() {
        let fake = FakeVarnish::start(Behavior::default()).await;
        let (tx, mut rx) = watch::channel(ProcessState::Starting);
        let stop = CancellationToken::new();
        let bin = script("trap '' TERM; while true; do sleep 0.05; done");
        let task = tokio::spawn(supervise(config(bin, fake.config()), tx, stop.clone()));
        rx.wait_for(|s| *s == ProcessState::Running).await.unwrap();
        stop.cancel();
        assert!(task.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn exit_after_startup_is_unexpected_even_with_code_zero() {
        let fake = FakeVarnish::start(Behavior::default()).await;
        let (tx, _rx) = watch::channel(ProcessState::Starting);
        let cfg = config(script("sleep 0.5; exit 0"), fake.config());
        let err = supervise(cfg, tx, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(err, ProcessError::UnexpectedExit(status) if status.success()));
    }

    #[test]
    fn line_filter_bounds_lines_and_drops_excerpts() {
        let mut filter = LineFilter::default();
        let mut lines = filter
            .feed(b"Message\nAt: ('<x>' Line 1 Pos 2)\nsecret source\n--##--\nVCL compilation fa");
        lines.extend(filter.feed(b"iled\n"));
        lines.extend(filter.feed(&[b'a'; 1000]));
        lines.extend(filter.feed(b"\x07\n"));
        assert_eq!(
            lines[..3],
            [
                "Message",
                "At: ('<x>' Line 1 Pos 2)",
                "VCL compilation failed"
            ]
        );
        assert_eq!(lines[3].len(), MAX_LOG_LINE);
    }
}
