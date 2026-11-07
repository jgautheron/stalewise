//! The sole configuration writer: render, compile, activate, persist and clean up.

use std::sync::Arc;

use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::discovery::{Backoff, Discovery};
use crate::endpoint::Endpoint;
use crate::metrics::Metrics;
use crate::policy::RenderPolicy;
use crate::render::{self, Context, OutputDir, RenderError, RenderSettings, Template};
use crate::status::{Health, Reason};
use crate::varnish::admin::{self, AdminConfig, AdminError, Client, VclName, VclState, VclStatus};
use crate::varnish::process::ProcessState;

const ACTIVE_FILE: &str = "active.vcl";
const INITIAL_FILE: &str = "initial.vcl";
const BOOT: &str = "boot";

#[derive(Debug, thiserror::Error)]
pub enum ReloadError {
    #[error(transparent)]
    Render(#[from] RenderError),
    #[error("stopped before an initial configuration was available")]
    Cancelled,
    #[error("publishing task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug, Clone)]
pub struct ReloadConfig {
    pub admin: AdminConfig,
    pub settings: RenderSettings,
    pub max_vcls: usize,
    pub allow_empty_backends: bool,
    pub output: Arc<OutputDir>,
}

#[derive(Debug, Clone)]
pub enum BackendInput {
    Static(Endpoint),
    Watch(watch::Receiver<Option<Discovery>>),
}

/// Latest-value snapshots consumed by the reloader.
#[derive(Debug, Clone)]
pub struct Inputs {
    pub template: watch::Receiver<Option<Template>>,
    pub frontends: watch::Receiver<Option<Discovery>>,
    pub backends: BackendInput,
    /// Accepted CachePolicies; `None` renders headers-only behaviour.
    pub policies: watch::Receiver<Option<Policies>>,
}

/// An immutable, ordered set of policies rendered together.
pub type Policies = Arc<[RenderPolicy]>;

/// The confirmed active configuration.
#[derive(Debug, Clone)]
pub struct Active {
    pub name: VclName,
    pub bytes: Arc<[u8]>,
    pub since: Instant,
    /// Policies the active VCL enforces.
    pub policies: Policies,
}

#[derive(Debug, Clone)]
pub struct InitialVcl {
    pub path: std::path::PathBuf,
    pub bytes: Arc<[u8]>,
    pub policies: Policies,
}

enum Candidate {
    Pending,
    Empty,
    Ready(Arc<[u8]>, Policies),
}

/// Renders the current inputs; `Pending` until a template and an origin snapshot exist.
fn candidate(config: &ReloadConfig, inputs: &Inputs) -> Result<Candidate, RenderError> {
    let Some(template) = inputs.template.borrow().clone() else {
        return Ok(Candidate::Pending);
    };
    let backends: Arc<[Endpoint]> = match &inputs.backends {
        BackendInput::Static(endpoint) => Arc::from([endpoint.clone()]),
        BackendInput::Watch(rx) => match rx.borrow().as_ref() {
            Some(snapshot) => Arc::clone(&snapshot.endpoints),
            None => return Ok(Candidate::Pending),
        },
    };
    if backends.is_empty() && !config.allow_empty_backends {
        return Ok(Candidate::Empty);
    }
    let peers = inputs
        .frontends
        .borrow()
        .as_ref()
        .map(|d| Arc::clone(&d.peers))
        .unwrap_or_else(|| Arc::from([]));
    let policies = inputs
        .policies
        .borrow()
        .clone()
        .unwrap_or_else(|| Arc::from([]));
    let context = Context::new(&config.settings, &peers, &backends, &policies)?;
    Ok(Candidate::Ready(
        render::render(&template.source, &context)?.into(),
        policies,
    ))
}

/// Waits for a valid origin configuration and publishes it as the child's `-f` file.
pub async fn initial(
    config: &ReloadConfig,
    inputs: &mut Inputs,
    health: &Health,
    stop: &CancellationToken,
) -> Result<InitialVcl, ReloadError> {
    loop {
        match candidate(config, inputs) {
            Ok(Candidate::Ready(bytes, policies)) => {
                health.set(Reason::BackendEmpty, false);
                health.set(Reason::RenderFailed, false);
                let path = persist(config, INITIAL_FILE, Arc::clone(&bytes)).await??;
                return Ok(InitialVcl {
                    path,
                    bytes,
                    policies,
                });
            }
            Ok(Candidate::Pending) => {}
            Ok(Candidate::Empty) => health.set(Reason::BackendEmpty, true),
            Err(err) => {
                tracing::warn!(error = %err, "initial render failed; waiting for new inputs");
                health.set(Reason::RenderFailed, true);
            }
        }
        tokio::select! {
            () = stop.cancelled() => return Err(ReloadError::Cancelled),
            () = changed(inputs) => {}
        }
    }
}

async fn persist(
    config: &ReloadConfig,
    file: &'static str,
    bytes: Arc<[u8]>,
) -> Result<Result<std::path::PathBuf, RenderError>, tokio::task::JoinError> {
    let output = Arc::clone(&config.output);
    tokio::task::spawn_blocking(move || output.publish(file, &bytes)).await
}

async fn changed_or_pending<T>(rx: &mut watch::Receiver<T>) {
    if rx.changed().await.is_err() {
        std::future::pending::<()>().await;
    }
}

async fn changed(inputs: &mut Inputs) {
    let backends = async {
        match &mut inputs.backends {
            BackendInput::Watch(rx) => changed_or_pending(rx).await,
            BackendInput::Static(_) => std::future::pending().await,
        }
    };
    tokio::select! {
        () = changed_or_pending(&mut inputs.template) => {}
        () = changed_or_pending(&mut inputs.frontends) => {}
        () = changed_or_pending(&mut inputs.policies) => {}
        () = backends => {}
    }
}

/// Serialises every reload after the child is running; returns only on `stop`.
pub async fn run(
    config: ReloadConfig,
    mut inputs: Inputs,
    initial: InitialVcl,
    mut process: watch::Receiver<ProcessState>,
    state: watch::Sender<Option<Active>>,
    health: Health,
    stop: CancellationToken,
) {
    tokio::select! {
        () = stop.cancelled() => return,
        r = process.wait_for(|s| *s == ProcessState::Running) => if r.is_err() { return },
    }
    let mut reloader = Reloader {
        config,
        state,
        metrics: Arc::clone(health.metrics()),
        health,
        sequence: 0,
        last_failed: None,
        retry_at: None,
        backoff: Backoff::default(),
        persist_pending: false,
        boot: Some((initial.bytes, initial.policies)),
    };
    loop {
        reloader.attempt(&inputs).await;
        let retry = reloader.retry_at;
        tokio::select! {
            () = stop.cancelled() => return,
            () = changed(&mut inputs) => {}
            () = sleep_until(retry) => {}
        }
    }
}

async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

struct Reloader {
    config: ReloadConfig,
    state: watch::Sender<Option<Active>>,
    metrics: Arc<Metrics>,
    health: Health,
    sequence: u64,
    last_failed: Option<[u8; 32]>,
    retry_at: Option<Instant>,
    backoff: Backoff,
    persist_pending: bool,
    /// The `-f` configuration until its activation is confirmed.
    boot: Option<(Arc<[u8]>, Policies)>,
}

/// Load order for retention: `boot` first, then generated names by sequence.
fn age_key(name: &VclName) -> Option<u64> {
    if name.as_str() == BOOT {
        return Some(0);
    }
    let rest = name.as_str().strip_prefix(admin::OWNED_PREFIX)?;
    let (seq, _) = rest.split_once('_')?;
    seq.parse::<u64>().ok().map(|n| n.saturating_add(1))
}

fn owned(list: &[VclState]) -> Vec<&VclState> {
    let mut rows: Vec<&VclState> = list
        .iter()
        .filter(|v| !v.is_label && v.status != VclStatus::Discarded && age_key(&v.name).is_some())
        .collect();
    rows.sort_by_key(|v| age_key(&v.name));
    rows
}

fn active_name(list: &[VclState]) -> Option<&VclName> {
    list.iter()
        .find(|v| v.status == VclStatus::Active)
        .map(|v| &v.name)
}

impl Reloader {
    fn active(&self) -> Option<Active> {
        self.state.borrow().clone()
    }

    fn fail(&mut self, reason: Reason, transient: bool) {
        self.metrics.reload_failure(reason);
        self.health.set(reason, true);
        if transient {
            self.retry_at = Some(Instant::now() + self.backoff.next_delay());
        }
    }

    async fn attempt(&mut self, inputs: &Inputs) {
        self.retry_at = None;
        if self.active().is_none() && !self.adopt_boot().await {
            return;
        }
        if self.persist_pending {
            self.persist_active().await;
        }
        let (bytes, policies) = match candidate(&self.config, inputs) {
            Ok(Candidate::Ready(bytes, policies)) => (bytes, policies),
            Ok(Candidate::Pending) => return,
            Ok(Candidate::Empty) => return self.health.set(Reason::BackendEmpty, true),
            Err(err) => {
                tracing::warn!(error = %err, "render failed; keeping active VCL");
                return self.fail(Reason::RenderFailed, false);
            }
        };
        self.health.set(Reason::BackendEmpty, false);
        self.health.set(Reason::RenderFailed, false);
        if self.active().is_some_and(|a| *a.bytes == *bytes) {
            self.metrics.reload_skips.inc();
            self.health.set(Reason::CompileFailed, false);
            self.health.set(Reason::ActivationRejected, false);
            return self.retry_cleanup().await;
        }
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        if self.last_failed == Some(digest) {
            return;
        }
        self.load(bytes, policies, digest).await;
    }

    /// Confirms the child's `boot` configuration before any reload.
    async fn adopt_boot(&mut self) -> bool {
        let listed = match admin::connect(&self.config.admin).await {
            Ok(mut client) => client.list().await,
            Err(err) => Err(err),
        };
        let list = match listed {
            Ok(list) => list,
            Err(err) => {
                tracing::warn!(error = %err, "cannot confirm boot configuration");
                self.fail(Reason::AdminUnavailable, true);
                return false;
            }
        };
        let Some(name) = active_name(&list) else {
            self.fail(Reason::AdminUnavailable, true);
            return false;
        };
        let Some((bytes, policies)) = self.boot.take() else {
            return false;
        };
        self.health.set(Reason::AdminUnavailable, false);
        self.state.send_replace(Some(Active {
            name: name.clone(),
            bytes,
            since: Instant::now(),
            policies,
        }));
        self.persist_pending = true;
        true
    }

    async fn load(&mut self, bytes: Arc<[u8]>, policies: Policies, digest: [u8; 32]) {
        self.metrics.reload_attempts.inc();
        let mut client = match self.session_with_room().await {
            Some(client) => client,
            None => return,
        };
        self.sequence += 1;
        let name = VclName::generate(self.sequence);
        match client.inline(&name, &bytes).await {
            Ok(()) => {}
            Err(AdminError::Command { status, diagnostic }) => {
                tracing::warn!(status, diagnostic = %diagnostic, "candidate VCL rejected; keeping active VCL");
                self.last_failed = Some(digest);
                return self.fail(Reason::CompileFailed, false);
            }
            Err(err) => {
                tracing::warn!(error = %err, "loading candidate failed");
                return self.fail(Reason::AdminUnavailable, true);
            }
        }
        let used = client.use_vcl(&name).await;
        if let Err(err) = &used {
            tracing::warn!(error = %err, "vcl.use did not confirm; reconciling");
        }
        let confirmed = match used {
            Ok(()) => client.list().await,
            Err(_) => self.relist().await,
        };
        let rejected = matches!(used, Err(AdminError::Command { .. }));
        match confirmed.as_ref().ok().and_then(|l| active_name(l)) {
            Some(active) if *active == name => self.commit(name, bytes, policies).await,
            Some(_) if rejected => {
                self.last_failed = Some(digest);
                self.fail(Reason::ActivationRejected, false);
            }
            Some(_) => self.fail(Reason::AdminUnavailable, true),
            None => {
                tracing::warn!("active VCL unknown after activation; deferring cleanup");
                self.fail(Reason::AdminUnavailable, true);
            }
        }
    }

    async fn relist(&self) -> Result<Vec<VclState>, AdminError> {
        admin::connect(&self.config.admin).await?.list().await
    }

    /// A fresh session with retention room for one more candidate, or `None` after recording why.
    async fn session_with_room(&mut self) -> Option<Client> {
        let mut client = match admin::connect(&self.config.admin).await {
            Ok(client) => client,
            Err(err) => {
                tracing::warn!(error = %err, "admin connection failed");
                self.fail(Reason::AdminUnavailable, true);
                return None;
            }
        };
        self.health.set(Reason::AdminUnavailable, false);
        let over = match client.list().await {
            Ok(list) => owned(&list).len() > self.config.max_vcls,
            Err(_) => {
                self.fail(Reason::AdminUnavailable, true);
                return None;
            }
        };
        if over && !self.cleanup(&mut client).await {
            tracing::warn!("retention bound exceeded; pausing loads");
            self.fail(Reason::RetentionExceeded, true);
            return None;
        }
        self.health.set(Reason::RetentionExceeded, false);
        Some(client)
    }

    async fn commit(&mut self, name: VclName, bytes: Arc<[u8]>, policies: Policies) {
        tracing::info!(vcl = %name, "activated VCL");
        self.metrics.reload_successes.inc();
        self.backoff.reset();
        self.last_failed = None;
        for reason in [
            Reason::CompileFailed,
            Reason::ActivationRejected,
            Reason::AdminUnavailable,
        ] {
            self.health.set(reason, false);
        }
        self.state.send_replace(Some(Active {
            name,
            bytes,
            since: Instant::now(),
            policies,
        }));
        self.persist_active().await;
        self.retry_cleanup().await;
    }

    async fn persist_active(&mut self) {
        let Some(active) = self.active() else { return };
        let ok = matches!(
            persist(&self.config, ACTIVE_FILE, active.bytes).await,
            Ok(Ok(_))
        );
        self.persist_pending = !ok;
        self.health.set(Reason::PersistenceFailed, !ok);
        if !ok {
            self.retry_at = Some(Instant::now() + self.backoff.next_delay());
        }
    }

    async fn retry_cleanup(&mut self) {
        let Ok(mut client) = admin::connect(&self.config.admin).await else {
            return self.fail(Reason::CleanupFailed, true);
        };
        if !self.cleanup(&mut client).await {
            self.fail(Reason::CleanupFailed, true);
        }
    }

    /// Discards oldest owned inactive VCLs until at most `max_vcls` remain; true when within bound.
    async fn cleanup(&mut self, client: &mut Client) -> bool {
        let Ok(list) = client.list().await else {
            return false;
        };
        let keep = self.active().map(|a| a.name);
        let rows = owned(&list);
        let mut excess = rows.len().saturating_sub(self.config.max_vcls);
        let victims: Vec<VclName> = rows
            .iter()
            .filter(|v| {
                v.status == VclStatus::Available && !v.labelled && Some(&v.name) != keep.as_ref()
            })
            .map(|v| v.name.clone())
            .collect();
        for victim in victims {
            if excess == 0 {
                break;
            }
            match client.discard(&victim).await {
                Ok(()) => excess -= 1,
                Err(err) => {
                    tracing::warn!(vcl = %victim, error = %err, "discard failed");
                    return false;
                }
            }
        }
        let ok = excess == 0;
        self.health.set(Reason::CleanupFailed, !ok);
        ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint::Host;
    use crate::varnish::admin::fake::{Behavior, FakeVarnish};
    use std::collections::BTreeMap;
    use std::time::Duration;
    use std::time::SystemTime;

    const TEMPLATE: &str = "vcl 4.1;{% for b in backends %} {{ b.name }}{% endfor %}{% for p in policies %} {{ p.name }}{% endfor %}";

    struct Harness {
        fake: FakeVarnish,
        template: watch::Sender<Option<Template>>,
        backends: watch::Sender<Option<Discovery>>,
        policies: watch::Sender<Option<Policies>>,
        active: watch::Receiver<Option<Active>>,
        health: Health,
        metrics: Arc<Metrics>,
        work: std::path::PathBuf,
        stop: CancellationToken,
    }

    fn snapshot(ips: &[&str]) -> Option<Discovery> {
        let endpoints: Vec<Endpoint> = ips
            .iter()
            .map(|ip| Endpoint::new(Host::Ip(ip.parse().unwrap()), 80))
            .collect();
        Some(Discovery {
            endpoints: endpoints.into(),
            peers: Arc::from([]),
            last_success: SystemTime::now(),
            error: None,
        })
    }

    async fn start(behavior: Behavior, max_vcls: usize) -> Harness {
        let fake = FakeVarnish::start(behavior).await;
        let work = crate::testutil::temp_dir("reload");
        let config = ReloadConfig {
            admin: fake.config(),
            settings: RenderSettings {
                env: BTreeMap::new(),
                canonical_host: None,
                backend_authority: "origin:80".parse().unwrap(),
                purge_acl: vec![],
                peer_purge: false,
                probe_path: "/".into(),
                uncacheable_ttl: Duration::from_secs(30),
                max_object_size: 1,
                shard: None,
            },
            max_vcls,
            allow_empty_backends: false,
            output: Arc::new(OutputDir::open(&work).unwrap()),
        };
        let (template, template_rx) =
            watch::channel(Some(Template::parse(TEMPLATE.as_bytes()).unwrap()));
        let (backends, backends_rx) = watch::channel(snapshot(&["10.0.0.1"]));
        let (frontends_tx, frontends_rx) = watch::channel(None);
        let (policies, policies_rx) = watch::channel(None);
        let mut inputs = Inputs {
            template: template_rx,
            frontends: frontends_rx,
            backends: BackendInput::Watch(backends_rx),
            policies: policies_rx,
        };
        let metrics = Arc::new(Metrics::new());
        let health = Health::new(Arc::clone(&metrics));
        let stop = CancellationToken::new();
        let initial = initial(&config, &mut inputs, &health, &stop).await.unwrap();
        assert_eq!(
            std::fs::read(&initial.path).unwrap(),
            b"vcl 4.1; ip_10_0_0_1_80_v4"
        );
        fake.state.lock().unwrap().vcls[0].source = initial.bytes.to_vec();
        let (process_tx, process_rx) = watch::channel(ProcessState::Running);
        std::mem::forget(process_tx);
        let (state, active) = watch::channel(None);
        tokio::spawn(run(
            config,
            inputs,
            initial,
            process_rx,
            state,
            health.clone(),
            stop.clone(),
        ));
        std::mem::forget(frontends_tx);
        let mut h = Harness {
            fake,
            template,
            backends,
            policies,
            active,
            health,
            metrics,
            work,
            stop,
        };
        h.active.wait_for(Option::is_some).await.unwrap();
        h
    }

    async fn eventually(mut check: impl FnMut() -> bool) {
        for _ in 0..200 {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not reached");
    }

    #[tokio::test]
    async fn adopts_boot_then_activates_and_persists_new_render() {
        let mut h = start(Behavior::default(), 5).await;
        assert_eq!(h.active.borrow().as_ref().unwrap().name.as_str(), "boot");
        h.backends.send_replace(snapshot(&["10.0.0.1", "10.0.0.2"]));
        h.active
            .wait_for(|a| a.as_ref().is_some_and(|a| a.name.is_owned()))
            .await
            .unwrap();
        let active = h.active.borrow().clone().unwrap();
        assert_eq!(h.fake.active(), active.name.as_str());
        assert_eq!(
            &*active.bytes,
            b"vcl 4.1; ip_10_0_0_1_80_v4 ip_10_0_0_2_80_v4"
        );
        let path = h.work.join("run").join(ACTIVE_FILE);
        eventually(|| std::fs::read(&path).ok().as_deref() == Some(&active.bytes[..])).await;
        assert_eq!(h.metrics.reload_successes.get(), 1);
        h.stop.cancel();
    }

    #[tokio::test]
    async fn policy_change_reloads_and_active_records_the_policies() {
        let mut h = start(Behavior::default(), 5).await;
        let spec = serde_norway::from_str("serviceRef: {name: catalog}\npathPrefix: /api").unwrap();
        let own = crate::policy::OwnService {
            name: "catalog".into(),
            port_name: None,
            port_number: None,
        };
        let policy = crate::policy::validate("catalog-api", 4, &spec, &own).unwrap();
        h.policies.send_replace(Some(Arc::from([policy])));
        h.active
            .wait_for(|a| a.as_ref().is_some_and(|a| a.policies.len() == 1))
            .await
            .unwrap();
        let active = h.active.borrow().clone().unwrap();
        assert!(active.bytes.ends_with(b" catalog-api"));
        assert_eq!(active.policies[0].generation, 4);
        h.stop.cancel();
    }

    #[tokio::test]
    async fn identical_bytes_skip_without_loading() {
        let h = start(Behavior::default(), 5).await;
        let before = h.fake.state.lock().unwrap().inline_count;
        eventually(|| h.metrics.reload_skips.get() == 1).await;
        let skips = h.metrics.reload_skips.get();
        h.backends.send_replace(snapshot(&["10.0.0.1"]));
        eventually(|| h.metrics.reload_skips.get() > skips).await;
        assert_eq!(h.fake.state.lock().unwrap().inline_count, before);
        h.stop.cancel();
    }

    #[tokio::test]
    async fn compile_failure_keeps_last_good_and_degrades() {
        let h = start(Behavior::default(), 5).await;
        h.template.send_replace(Some(
            Template::parse(b"COMPILE_FAIL {{ backends | length }}").unwrap(),
        ));
        eventually(|| h.health.current().contains(&Reason::CompileFailed)).await;
        assert_eq!(h.fake.active(), "boot");
        assert_eq!(h.active.borrow().as_ref().unwrap().name.as_str(), "boot");
        h.template
            .send_replace(Some(Template::parse(TEMPLATE.as_bytes()).unwrap()));
        eventually(|| !h.health.current().contains(&Reason::CompileFailed)).await;
        h.stop.cancel();
    }

    #[tokio::test]
    async fn empty_backends_keep_active_vcl() {
        let h = start(Behavior::default(), 5).await;
        h.backends.send_replace(snapshot(&[]));
        eventually(|| h.health.current().contains(&Reason::BackendEmpty)).await;
        assert_eq!(h.fake.active(), "boot");
        h.backends.send_replace(snapshot(&["10.0.0.3"]));
        eventually(|| h.fake.active().starts_with("stalewise_")).await;
        assert!(!h.health.current().contains(&Reason::BackendEmpty));
        h.stop.cancel();
    }

    #[tokio::test]
    async fn retention_discards_oldest_and_never_the_active() {
        let mut h = start(Behavior::default(), 2).await;
        for i in 2..7 {
            h.backends.send_replace(snapshot(&[&format!("10.0.0.{i}")]));
            let want = format!("10.0.0.{i}").replace('.', "_");
            h.active
                .wait_for(|a| {
                    a.as_ref()
                        .is_some_and(|a| String::from_utf8_lossy(&a.bytes).contains(&want))
                })
                .await
                .unwrap();
        }
        eventually(|| h.fake.names().len() <= 2).await;
        let names = h.fake.names();
        assert!(names.contains(&h.fake.active()));
        assert!(!names.contains(&"boot".to_owned()));
        h.stop.cancel();
    }

    #[tokio::test]
    async fn lost_activation_reply_reconciles_by_listing() {
        let mut h = start(
            Behavior {
                lose_reply_on: Some("vcl.use"),
                ..Behavior::default()
            },
            5,
        )
        .await;
        h.backends.send_replace(snapshot(&["10.0.0.9"]));
        h.active
            .wait_for(|a| a.as_ref().is_some_and(|a| a.name.is_owned()))
            .await
            .unwrap();
        assert_eq!(
            h.fake.active(),
            h.active.borrow().as_ref().unwrap().name.as_str()
        );
        assert_eq!(
            h.fake.state.lock().unwrap().inline_count,
            1,
            "no blind replay"
        );
        h.stop.cancel();
    }

    #[tokio::test]
    async fn rejected_activation_is_not_retried_for_the_same_bytes() {
        let h = start(
            Behavior {
                reject_use: true,
                ..Behavior::default()
            },
            5,
        )
        .await;
        h.backends.send_replace(snapshot(&["10.0.0.4"]));
        eventually(|| h.health.current().contains(&Reason::ActivationRejected)).await;
        assert!(!h.health.current().contains(&Reason::AdminUnavailable));
        let loads = h.fake.state.lock().unwrap().inline_count;
        h.backends.send_replace(snapshot(&["10.0.0.4"]));
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(h.fake.state.lock().unwrap().inline_count, loads);
        assert_eq!(h.fake.active(), "boot");
        h.stop.cancel();
    }

    #[tokio::test]
    async fn failed_cleanup_pauses_loads_beyond_one_extra_candidate() {
        let mut h = start(
            Behavior {
                fail_discard: true,
                ..Behavior::default()
            },
            1,
        )
        .await;
        h.backends.send_replace(snapshot(&["10.0.0.5"]));
        h.active
            .wait_for(|a| a.as_ref().is_some_and(|a| a.name.is_owned()))
            .await
            .unwrap();
        eventually(|| h.health.current().contains(&Reason::CleanupFailed)).await;
        h.backends.send_replace(snapshot(&["10.0.0.6"]));
        eventually(|| h.health.current().contains(&Reason::RetentionExceeded)).await;
        assert_eq!(
            h.fake.names().len(),
            2,
            "one candidate beyond the bound at most"
        );
        h.stop.cancel();
    }
}
