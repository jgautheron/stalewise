//! Task wiring, shutdown ordering and exit codes.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::{BackendSource, Config, TemplateSource};
use crate::controller;
use crate::discovery::{self, Discovery};
use crate::endpoint::{Endpoint, Host};
use crate::metrics::Metrics;
use crate::mgmt::{self, ManagementConfig, StatusInputs};
use crate::reload::{self, Active, BackendInput, Inputs, Policies, ReloadConfig};
use crate::render::{self, OutputDir, RenderSettings, ShardSettings, Template};
use crate::signaller::{self, TokenDigest};
use crate::status::{Health, Reason};
use crate::varnish::admin::AdminConfig;
use crate::varnish::process::{self, ProcessConfig, ProcessState};

/// Exit code for invalid startup configuration, secrets or tokens.
pub const EXIT_CONFIG: u8 = 2;
/// Exit code for runtime infrastructure failures, including child death.
pub const EXIT_RUNTIME: u8 = 1;
const SECRET_FILE: &str = "admin.secret";
const MAX_SECRET: u64 = 64 * 1024;

#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error("{0}")]
    Config(String),
    #[error("{0}")]
    Runtime(String),
}

impl StartupError {
    fn code(&self) -> ExitCode {
        match self {
            Self::Config(_) => ExitCode::from(EXIT_CONFIG),
            Self::Runtime(_) => ExitCode::from(EXIT_RUNTIME),
        }
    }
}

/// Runs until a signal (exit 0 after reaping) or a fatal failure (nonzero).
pub async fn run(config: Config) -> ExitCode {
    let root = CancellationToken::new();
    tokio::spawn(forward_signals(root.clone()));
    match start(config, root).await {
        Ok(code) => code,
        Err(err) => {
            tracing::error!(error = %err, "startup failed");
            err.code()
        }
    }
}

async fn forward_signals(root: CancellationToken) {
    use tokio::signal::unix::{SignalKind, signal};
    let (Ok(mut term), Ok(mut int)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) else {
        tracing::error!("cannot install signal handlers");
        return root.cancel();
    };
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
    tracing::info!("shutdown requested");
    root.cancel();
}

fn admin_secret(config: &Config, output: &OutputDir) -> Result<PathBuf, StartupError> {
    if let Some(path) = &config.admin_secret_file {
        use std::io::Read;
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .and_then(|f| f.take(MAX_SECRET + 1).read_to_end(&mut bytes))
            .map_err(|e| StartupError::Config(format!("admin secret file unreadable: {e}")))?;
        if bytes.is_empty() || bytes.len() as u64 > MAX_SECRET {
            return Err(StartupError::Config(
                "admin secret file must be nonempty and at most 64KiB".into(),
            ));
        }
        return Ok(path.clone());
    }
    let raw: [u8; 32] = crate::entropy::bytes()
        .map_err(|e| StartupError::Runtime(format!("cannot generate admin secret: {e}")))?;
    let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
    output
        .publish(SECRET_FILE, hex.as_bytes())
        .map_err(|e| StartupError::Runtime(format!("cannot write admin secret: {e}")))
}

fn render_settings(config: &Config) -> RenderSettings {
    RenderSettings {
        env: config.template_env.clone(),
        canonical_host: config.canonical_host.clone(),
        backend_authority: config.backend_authority.clone(),
        purge_acl: config.purge_acl.clone(),
        peer_purge: config.signaller.is_some(),
        probe_path: config.backend_probe_path.clone(),
        uncacheable_ttl: config.uncacheable_ttl,
        max_object_size: config.max_object_size,
        shard: config.shard.as_ref().map(|shard| ShardSettings {
            pod_name: shard.pod_name.clone(),
            peer_authority: shard.peer_authority.clone(),
            local_ttl: shard.local_ttl,
            peer_timeout: shard.peer_timeout,
        }),
    }
}

struct Shared {
    metrics: Arc<Metrics>,
    health: Health,
    root: CancellationToken,
}

/// Tasks that return only on `root` cancellation; any earlier exit, including a panic, is fatal.
#[derive(Default)]
struct Background(Vec<(&'static str, JoinHandle<()>)>);

impl Background {
    fn spawn<F>(&mut self, name: &'static str, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.0.push((name, tokio::spawn(task)));
    }

    /// Resolves when the first task ends, naming it; pending forever when there are none.
    async fn first_exit(&mut self) -> String {
        if self.0.is_empty() {
            return std::future::pending().await;
        }
        let handles = self.0.iter_mut().map(|(_, handle)| handle);
        let (result, index, _) = futures_util::future::select_all(handles).await;
        let name = self.0.get(index).map_or("task", |(name, _)| *name);
        match result {
            Ok(()) => format!("{name} stopped"),
            Err(err) => format!("{name} failed: {err}"),
        }
    }
}

fn template_input(
    config: &Config,
    root: &CancellationToken,
    tasks: &mut Background,
) -> watch::Receiver<Option<Template>> {
    let (tx, rx) = watch::channel(None);
    match &config.template {
        TemplateSource::Embedded(profile) => {
            tx.send_replace(Some(Template::embedded(*profile)));
        }
        TemplateSource::File(path) => {
            let poll = render::poll(path.clone(), config.template_poll, tx, root.clone());
            tasks.spawn("template poller", poll);
        }
    }
    rx
}

/// Kubernetes-fed inputs; policies stay `None` (headers only) until the controller publishes.
struct ApiInputs {
    frontends: watch::Receiver<Option<Discovery>>,
    backends: BackendInput,
    policies: watch::Receiver<Option<Policies>>,
}

async fn api_inputs(
    config: &Config,
    shared: &Shared,
    tasks: &mut Background,
    active: watch::Receiver<Option<Active>>,
) -> Result<ApiInputs, StartupError> {
    let client = kube::Client::try_default().await;
    let (policy_tx, policies) = watch::channel(None);
    if let Some(policy) = config.policies.clone() {
        let client = client
            .as_ref()
            .map_err(|e| {
                StartupError::Runtime(format!("--cache-policies needs the Kubernetes API: {e}"))
            })?
            .clone();
        let run = controller::run(client, policy, policy_tx, active, shared.root.clone());
        tasks.spawn("CachePolicy controller", run);
    }
    let (front_tx, front_rx) = watch::channel(None);
    let mut spawn_watch = |client: kube::Client, role, tx| {
        let watch = discovery::watch(
            client,
            role,
            config.membership_debounce,
            tx,
            Arc::clone(&shared.metrics),
            shared.health.clone(),
            shared.root.clone(),
        );
        tasks.spawn("EndpointSlice watcher", watch);
    };
    match &client {
        Ok(client) => spawn_watch(client.clone(), config.frontend.clone(), front_tx),
        Err(err) => {
            tracing::warn!(error = %err, "no Kubernetes API client; peer discovery disabled");
            shared.health.set(Reason::FrontendDiscovery, true);
        }
    }
    let backends = match (&config.backend, client) {
        (BackendSource::Static { host, port }, _) => {
            BackendInput::Static(Endpoint::new(Host::Dns(host.clone()), *port))
        }
        (BackendSource::Watch(role), Ok(client)) => {
            let (tx, rx) = watch::channel(None);
            spawn_watch(client, role.clone(), tx);
            BackendInput::Watch(rx)
        }
        (BackendSource::Watch(_), Err(err)) => {
            return Err(StartupError::Runtime(format!(
                "backend watching needs the Kubernetes API: {err}"
            )));
        }
    };
    Ok(ApiInputs {
        frontends: front_rx,
        backends,
        policies,
    })
}

async fn start(config: Config, root: CancellationToken) -> Result<ExitCode, StartupError> {
    let token = config
        .signaller
        .as_ref()
        .map(|s| signaller::load_token(&s.token_file))
        .transpose()
        .map_err(|e| StartupError::Config(e.to_string()))?;
    let output = Arc::new(
        OutputDir::open(&config.work_dir).map_err(|e| StartupError::Runtime(e.to_string()))?,
    );
    let secret_file = admin_secret(&config, &output)?;
    let metrics = Arc::new(Metrics::new());
    let shared = Shared {
        health: Health::new(Arc::clone(&metrics)),
        metrics,
        root: root.clone(),
    };
    let (process_tx, process_rx) = watch::channel(ProcessState::Starting);
    let (active_tx, active_rx) = watch::channel(None);
    let (probe_tx, probe_rx) = watch::channel(None);
    let status = StatusInputs {
        active: active_rx,
        process: process_rx.clone(),
        probe: probe_rx,
        health: shared.health.clone(),
    };
    let management = ManagementConfig {
        listen: config.management_listen,
    };
    let mut mgmt_task = tokio::spawn(mgmt::serve(
        management,
        status,
        Arc::clone(&shared.metrics),
        root.clone(),
    ));

    let mut tasks = Background::default();
    let template = template_input(&config, &root, &mut tasks);
    let api = api_inputs(&config, &shared, &mut tasks, active_tx.subscribe()).await?;
    let frontends = api.frontends;
    let admin = AdminConfig {
        addr: config.admin_listen,
        secret_file,
        timeout: config.admin_timeout,
    };
    let reload_config = ReloadConfig {
        admin: admin.clone(),
        settings: render_settings(&config),
        max_vcls: config.max_vcls,
        allow_empty_backends: config.allow_empty_backends,
        output,
    };
    let mut inputs = Inputs {
        template,
        frontends: frontends.clone(),
        backends: api.backends,
        policies: api.policies,
    };
    let initial = tokio::time::timeout(
        config.startup_timeout,
        reload::initial(&reload_config, &mut inputs, &shared.health, &root),
    )
    .await;
    let initial = match initial {
        Ok(Ok(initial)) => initial,
        Ok(Err(reload::ReloadError::Cancelled)) => return Ok(ExitCode::SUCCESS),
        Ok(Err(err)) => {
            return Err(StartupError::Runtime(format!(
                "initial configuration: {err}"
            )));
        }
        Err(_) => {
            return Err(StartupError::Runtime(
                "no initial configuration within the startup timeout".into(),
            ));
        }
    };

    let drain_budget = match &config.signaller {
        Some(s) => s.delivery_deadline.min(config.shutdown_timeout / 2),
        None => Duration::ZERO,
    };
    let process_config = ProcessConfig {
        bin: config.varnish_bin.clone(),
        initial_vcl: initial.path.clone(),
        admin,
        listen: config.listen,
        storage: config.storage.clone(),
        work_dir: config.varnish_work_dir.clone(),
        params: config.varnish_params.clone(),
        startup_timeout: config.startup_timeout,
        shutdown_timeout: config.shutdown_timeout.saturating_sub(drain_budget),
    };
    let child_stop = CancellationToken::new();
    let mut child = tokio::spawn(process::supervise(
        process_config,
        process_tx,
        child_stop.clone(),
    ));
    let reload = reload::run(
        reload_config,
        inputs,
        initial,
        process_rx.clone(),
        active_tx,
        shared.health.clone(),
        root.clone(),
    );
    tasks.spawn("reloader", reload);
    let probe = mgmt::probe(
        mgmt::probe_target(config.listen),
        process_rx,
        probe_tx,
        root.clone(),
    );
    tasks.spawn("frontend probe", probe);
    let mut signal_task: JoinHandle<Result<(), signaller::SignallerError>> =
        match (config.signaller.clone(), token) {
            (Some(sig), Some(token)) => spawn_signaller(sig, token, frontends, &shared),
            _ => tokio::spawn(std::future::pending()),
        };

    let failure = tokio::select! {
        biased;
        () = root.cancelled() => None,
        result = &mut child => {
            root.cancel();
            tracing::error!(reason = %describe_child(result), "varnishd is gone; exiting");
            return Ok(ExitCode::from(EXIT_RUNTIME));
        }
        result = &mut mgmt_task => Some(format!("management listener stopped: {}", flatten(result))),
        result = &mut signal_task => Some(format!("signaller stopped: {}", flatten(result))),
        reason = tasks.first_exit() => Some(reason),
    };
    root.cancel();
    if let Some(reason) = failure {
        tracing::error!(reason = %reason, "fatal; shutting down");
        child_stop.cancel();
        let _ = tokio::time::timeout(config.shutdown_timeout, child).await;
        return Ok(ExitCode::from(EXIT_RUNTIME));
    }
    let _ = tokio::time::timeout(drain_budget, signal_task).await;
    child_stop.cancel();
    match child.await {
        Ok(Ok(status)) => {
            tracing::info!(status = %status, "varnishd reaped; exiting");
            Ok(ExitCode::SUCCESS)
        }
        other => {
            tracing::error!(reason = %describe_child(other), "varnishd shutdown failed");
            Ok(ExitCode::from(EXIT_RUNTIME))
        }
    }
}

fn spawn_signaller(
    config: crate::config::SignalConfig,
    token: TokenDigest,
    peers: watch::Receiver<Option<Discovery>>,
    shared: &Shared,
) -> JoinHandle<Result<(), signaller::SignallerError>> {
    tokio::spawn(signaller::serve(
        config,
        token,
        peers,
        Arc::clone(&shared.metrics),
        shared.root.clone(),
    ))
}

fn flatten<E: std::fmt::Display>(result: Result<Result<(), E>, tokio::task::JoinError>) -> String {
    match result {
        Ok(Ok(())) => "exited".into(),
        Ok(Err(err)) => err.to_string(),
        Err(err) => format!("task failed: {err}"),
    }
}

fn describe_child(
    result: Result<Result<std::process::ExitStatus, process::ProcessError>, tokio::task::JoinError>,
) -> String {
    match result {
        Ok(Ok(status)) => format!("varnishd exited: {status}"),
        Ok(Err(err)) => err.to_string(),
        Err(err) => format!("supervisor task failed: {err}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn background_reports_a_panicked_task_by_name() {
        let mut tasks = Background::default();
        tasks.spawn("idle", std::future::pending());
        tasks.spawn("reloader", async { panic!("boom") });
        let reason = tasks.first_exit().await;
        assert!(reason.starts_with("reloader failed"), "{reason}");
    }
}
