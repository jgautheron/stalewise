//! Memory-pressure watchdog: bans the longest-cached objects before the cgroup limit is reached.
//!
//! Varnish's LRU bounds stevedore bytes only; the per-object overhead outside storage grows
//! unchecked. This task is the only issuer of `ban obj.age > T`, over its own admin session.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::capacity::{self, Usage};
use crate::config::MemguardConfig;
use crate::metrics::Metrics;
use crate::status::{Health, Reason};
use crate::varnish::admin::{self, AdminConfig, AdminError, Client};
use crate::varnish::process::ProcessState;

/// Lets the ban lurker (`ban_lurker_age=1`) walk the cache and the allocator release memory
/// before the next, harsher step.
pub const MIN_STEP_GAP: Duration = Duration::from_secs(3);
/// Growth since the last ban, as a fraction of the limit, that justifies another step. glibc keeps
/// freed object memory for reuse instead of returning it, so a flat working set after a ban means
/// the ban worked and new objects reuse what it freed.
pub const GROWTH: f64 = 0.005;

/// The watchdog's thresholds; `high` and `low` are fractions of the memory limit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Thresholds {
    pub high: f64,
    pub low: f64,
    pub min_age: Duration,
    pub max_age: Duration,
    pub gap: Duration,
}

impl Thresholds {
    pub fn from_config(config: &MemguardConfig) -> Self {
        Self {
            high: config.high,
            low: config.low,
            min_age: config.min_age,
            max_age: config.max_age,
            gap: config.interval.max(MIN_STEP_GAP),
        }
    }
}

/// What the watchdog does on one tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Below high water, or back below low water.
    Idle,
    /// Under pressure but rate-limited, not growing since the last ban, or nothing is older
    /// than the floor.
    Hold,
    /// Ban objects cached longer than this.
    Ban(Duration),
}

#[derive(Debug, Clone, Copy)]
struct Episode {
    /// The largest T the next step may use; halves after every confirmed ban.
    ceiling: Duration,
    last_ban: Option<Instant>,
    /// The working set ratio when the last ban was issued.
    ratio_at_ban: Option<f64>,
}

/// Hysteresis and T stepping; pure, so tests drive it with any clock.
#[derive(Debug)]
pub struct Guard {
    thresholds: Thresholds,
    episode: Option<Episode>,
    /// No cached object was stored before this: varnishd's start, raised by every confirmed ban.
    oldest_since: Instant,
}

impl Guard {
    pub fn new(thresholds: Thresholds, started: Instant) -> Self {
        Self {
            thresholds,
            episode: None,
            oldest_since: started,
        }
    }

    /// A fresh varnishd has an empty cache.
    pub fn restarted(&mut self, now: Instant) {
        self.episode = None;
        self.oldest_since = now;
    }

    pub fn active(&self) -> bool {
        self.episode.is_some()
    }

    /// Advances the state machine with the working set as a fraction of the limit.
    pub fn step(&mut self, ratio: f64, now: Instant) -> Action {
        self.track_pressure(ratio);
        let Some(episode) = &mut self.episode else {
            return Action::Idle;
        };
        let t = self.thresholds;
        if episode
            .last_ban
            .is_some_and(|at| now.saturating_duration_since(at) < t.gap)
        {
            return Action::Hold;
        }
        if episode.ratio_at_ban.is_some_and(|at| ratio < at + GROWTH) {
            return Action::Hold;
        }
        let oldest = now.saturating_duration_since(self.oldest_since);
        if oldest <= t.min_age {
            return Action::Hold;
        }
        episode.last_ban = Some(now);
        episode.ratio_at_ban = Some(ratio);
        let age = episode.ceiling.min(oldest / 2);
        Action::Ban(Duration::from_secs(age.as_secs()).max(t.min_age))
    }

    /// Records that Varnish accepted `ban obj.age > age` at `now`.
    pub fn confirm(&mut self, age: Duration, now: Instant) {
        if let Some(episode) = &mut self.episode {
            episode.ceiling = (age / 2).max(self.thresholds.min_age);
        }
        if let Some(cut) = now.checked_sub(age) {
            self.oldest_since = self.oldest_since.max(cut);
        }
    }

    fn track_pressure(&mut self, ratio: f64) {
        let t = self.thresholds;
        match self.episode {
            None if ratio >= t.high => {
                self.episode = Some(Episode {
                    ceiling: t.max_age,
                    last_ban: None,
                    ratio_at_ban: None,
                });
            }
            Some(_) if ratio < t.low => self.episode = None,
            _ => {}
        }
    }
}

/// Everything the task needs besides its configuration.
pub struct Inputs {
    pub admin: AdminConfig,
    pub cgroup_root: PathBuf,
    pub process: watch::Receiver<ProcessState>,
    pub metrics: Arc<Metrics>,
    pub health: Health,
}

/// Samples memory every interval and bans the oldest objects under pressure, until `stop`.
pub async fn run(config: MemguardConfig, inputs: Inputs, stop: CancellationToken) {
    let Some(limit) = capacity::cgroup_limit(&inputs.cgroup_root) else {
        tracing::info!("memory guard disabled: no container memory limit");
        return stop.cancelled().await;
    };
    inputs.metrics.memory_limit.set(saturating_i64(limit));
    let thresholds = Thresholds::from_config(&config);
    tracing::info!(
        memory_limit = limit,
        high = thresholds.high,
        low = thresholds.low,
        min_age_s = thresholds.min_age.as_secs(),
        "memory guard armed"
    );
    let mut watchdog = Watchdog {
        guard: Guard::new(thresholds, Instant::now()),
        limit,
        running: false,
        client: None,
        inputs,
    };
    let mut ticker = tokio::time::interval(config.interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = ticker.tick() => watchdog.tick(Instant::now()).await,
        }
    }
}

struct Watchdog {
    guard: Guard,
    limit: u64,
    running: bool,
    client: Option<Client>,
    inputs: Inputs,
}

#[derive(Debug, thiserror::Error)]
enum TickError {
    #[error("cgroup memory usage unreadable")]
    Unreadable,
    #[error("ban failed: {0}")]
    Ban(#[from] AdminError),
}

impl Watchdog {
    async fn tick(&mut self, now: Instant) {
        if !self.varnish_running(now) {
            return;
        }
        let outcome = self.sample_and_act(now).await;
        self.inputs
            .metrics
            .memguard_active
            .set(i64::from(self.guard.active()));
        let failed = outcome.is_err();
        if let Err(err) = outcome {
            self.inputs.metrics.memguard_failures.inc();
            tracing::warn!(error = %err, "memory guard step failed");
        }
        self.inputs.health.set(Reason::MemguardFailed, failed);
    }

    /// Tracks varnishd restarts, which empty the cache; no step runs while it is down.
    fn varnish_running(&mut self, now: Instant) -> bool {
        let running = *self.inputs.process.borrow_and_update() == ProcessState::Running;
        if running && !self.running {
            self.guard.restarted(now);
            self.client = None;
        }
        self.running = running;
        running
    }

    async fn sample_and_act(&mut self, now: Instant) -> Result<(), TickError> {
        let usage =
            capacity::cgroup_usage(&self.inputs.cgroup_root).ok_or(TickError::Unreadable)?;
        self.record(usage);
        let was_active = self.guard.active();
        let ratio = usage.working_set as f64 / self.limit as f64;
        let action = self.guard.step(ratio, now);
        log_transition(
            was_active,
            self.guard.active(),
            usage.working_set,
            self.limit,
        );
        let Action::Ban(age) = action else {
            return Ok(());
        };
        self.ban(age).await?;
        self.guard.confirm(age, now);
        self.inputs.metrics.memguard_bans.inc();
        tracing::info!(
            age_s = age.as_secs(),
            working_set = usage.working_set,
            memory_limit = self.limit,
            "memory guard banned objects cached longer than age_s"
        );
        Ok(())
    }

    fn record(&self, usage: Usage) {
        let m = &self.inputs.metrics;
        m.memory_used.set(saturating_i64(usage.working_set));
        m.memory_usage.set(saturating_i64(usage.current));
    }

    /// Reuses one session; any error drops it so the next step reconnects.
    async fn ban(&mut self, age: Duration) -> Result<(), AdminError> {
        let mut client = match self.client.take() {
            Some(client) => client,
            None => admin::connect(&self.inputs.admin).await?,
        };
        client.ban_older_than(age).await?;
        self.client = Some(client);
        Ok(())
    }
}

fn log_transition(was_active: bool, active: bool, working_set: u64, limit: u64) {
    match (was_active, active) {
        (false, true) => tracing::warn!(
            working_set,
            memory_limit = limit,
            "memory pressure: evicting the oldest objects"
        ),
        (true, false) => tracing::info!(
            working_set,
            memory_limit = limit,
            "memory pressure relieved"
        ),
        _ => {}
    }
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varnish::admin::fake::{Behavior, FakeVarnish};

    const S: Duration = Duration::from_secs(1);

    fn thresholds() -> Thresholds {
        Thresholds {
            high: 0.85,
            low: 0.75,
            min_age: 10 * S,
            max_age: 3600 * S,
            gap: 3 * S,
        }
    }

    /// Runs one step `after` past `start` and confirms any ban, as a successful run would.
    fn step(guard: &mut Guard, start: Instant, after: Duration, ratio: f64) -> Action {
        let now = start + after;
        let action = guard.step(ratio, now);
        if let Action::Ban(age) = action {
            guard.confirm(age, now);
        }
        action
    }

    #[test]
    fn pressure_starts_at_high_water_and_ends_only_below_low_water() {
        let start = Instant::now();
        let mut guard = Guard::new(thresholds(), start);
        let day = 86_400 * S;
        assert_eq!(step(&mut guard, start, day, 0.84), Action::Idle);
        assert!(matches!(step(&mut guard, start, day, 0.85), Action::Ban(_)));
        assert!(guard.active());
        assert_eq!(step(&mut guard, start, day + 10 * S, 0.80), Action::Hold);
        assert!(guard.active(), "between the marks the episode continues");
        assert_eq!(
            step(&mut guard, start, day + 15 * S, 0.86),
            Action::Ban(1800 * S)
        );
        assert_eq!(step(&mut guard, start, day + 20 * S, 0.7499), Action::Idle);
        assert!(!guard.active());
        assert_eq!(
            step(&mut guard, start, day + 30 * S, 0.80),
            Action::Idle,
            "no restart below high water"
        );
    }

    #[test]
    fn age_starts_at_half_the_oldest_possible_object_and_halves_to_the_floor() {
        let start = Instant::now();
        let mut guard = Guard::new(thresholds(), start);
        let ages: Vec<_> = (0..8)
            .filter_map(|n| {
                match step(&mut guard, start, (400 + 5 * n) * S, 0.9 + 0.01 * n as f64) {
                    Action::Ban(age) => Some(age.as_secs()),
                    _ => None,
                }
            })
            .collect();
        // Varnish started 400s ago, so nothing is older: 200, then halving, floored at 10.
        assert_eq!(ages, [200, 100, 50, 25, 12, 10, 10, 10]);
    }

    #[test]
    fn the_configured_cap_bounds_the_first_age() {
        let start = Instant::now();
        let mut guard = Guard::new(thresholds(), start);
        assert_eq!(
            step(&mut guard, start, 86_400 * S, 0.9),
            Action::Ban(3600 * S)
        );
    }

    #[test]
    fn bans_are_rate_limited_to_one_per_gap() {
        let start = Instant::now();
        let mut guard = Guard::new(thresholds(), start);
        assert!(matches!(
            step(&mut guard, start, 1000 * S, 0.9),
            Action::Ban(_)
        ));
        assert_eq!(step(&mut guard, start, 1001 * S, 0.9), Action::Hold);
        assert_eq!(step(&mut guard, start, 1002 * S, 0.9), Action::Hold);
        assert!(matches!(
            step(&mut guard, start, 1003 * S, 0.91),
            Action::Ban(_)
        ));
    }

    #[test]
    fn a_young_cache_is_never_banned_below_the_floor() {
        let start = Instant::now();
        let mut guard = Guard::new(thresholds(), start);
        assert_eq!(step(&mut guard, start, 10 * S, 0.99), Action::Hold);
        assert_eq!(step(&mut guard, start, 14 * S, 0.99), Action::Ban(10 * S));
        guard.restarted(start + 15 * S);
        assert!(!guard.active());
        assert_eq!(
            step(&mut guard, start, 20 * S, 0.99),
            Action::Hold,
            "restart empties the cache"
        );
    }

    #[test]
    fn an_unconfirmed_ban_keeps_the_age_but_still_waits() {
        let start = Instant::now();
        let mut guard = Guard::new(thresholds(), start);
        assert_eq!(guard.step(0.9, start + 400 * S), Action::Ban(200 * S));
        assert_eq!(guard.step(0.9, start + 401 * S), Action::Hold);
        assert_eq!(guard.step(0.91, start + 403 * S), Action::Ban(201 * S));
    }

    fn inputs(fake: &FakeVarnish, root: PathBuf) -> (Inputs, watch::Sender<ProcessState>) {
        let (tx, process) = watch::channel(ProcessState::Running);
        let metrics = Arc::new(Metrics::new());
        let inputs = Inputs {
            admin: fake.config(),
            cgroup_root: root,
            process,
            health: Health::new(Arc::clone(&metrics)),
            metrics,
        };
        (inputs, tx)
    }

    fn cgroup(root: &std::path::Path, current: u64, max: u64) {
        let dir = root.join("sys/fs/cgroup");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("memory.current"), format!("{current}\n")).unwrap();
        std::fs::write(dir.join("memory.max"), format!("{max}\n")).unwrap();
    }

    #[tokio::test]
    async fn pressure_issues_an_obj_age_ban_over_the_admin_session() {
        let fake = FakeVarnish::start(Behavior::default()).await;
        let root = crate::testutil::temp_dir("memguard");
        cgroup(&root, 90, 100);
        let (inputs, _tx) = inputs(&fake, root);
        let metrics = Arc::clone(&inputs.metrics);
        let start = Instant::now();
        let mut dog = Watchdog {
            guard: Guard::new(thresholds(), start),
            limit: 100,
            running: true,
            client: None,
            inputs,
        };
        dog.tick(start + 400 * S).await;
        assert_eq!(fake.state.lock().unwrap().bans, ["obj.age > 200s"]);
        assert_eq!(metrics.memguard_bans.get(), 1);
        assert_eq!(metrics.memguard_active.get(), 1);
        assert_eq!(metrics.memory_used.get(), 90);
        assert!(dog.inputs.health.current().is_empty());
    }

    #[tokio::test]
    async fn failures_degrade_readiness_reasons_and_recover() {
        let fake = FakeVarnish::start(Behavior {
            stall_on: Some("ban"),
            ..Behavior::default()
        })
        .await;
        let root = crate::testutil::temp_dir("memguard-fail");
        cgroup(&root, 90, 100);
        let (mut inputs, _tx) = inputs(&fake, root.clone());
        inputs.admin.timeout = Duration::from_millis(50);
        let metrics = Arc::clone(&inputs.metrics);
        let start = Instant::now();
        let mut dog = Watchdog {
            guard: Guard::new(thresholds(), start),
            limit: 100,
            running: true,
            client: None,
            inputs,
        };
        dog.tick(start + 400 * S).await;
        assert_eq!(metrics.memguard_failures.get(), 1);
        assert!(
            dog.inputs
                .health
                .current()
                .contains(&Reason::MemguardFailed)
        );
        cgroup(&root, 10, 100);
        dog.tick(start + 410 * S).await;
        assert!(
            dog.inputs.health.current().is_empty(),
            "a healthy step clears the reason"
        );
        std::fs::remove_file(root.join("sys/fs/cgroup/memory.current")).unwrap();
        dog.tick(start + 420 * S).await;
        assert_eq!(
            metrics.memguard_failures.get(),
            2,
            "unreadable usage counts"
        );
    }

    #[tokio::test]
    async fn no_memory_limit_disables_the_guard_until_stop() {
        let fake = FakeVarnish::start(Behavior::default()).await;
        let (inputs, _tx) = inputs(&fake, crate::testutil::temp_dir("memguard-none"));
        let stop = CancellationToken::new();
        stop.cancel();
        let config = MemguardConfig {
            interval: S,
            high: 0.85,
            low: 0.75,
            min_age: 10 * S,
            max_age: 3600 * S,
        };
        run(config, inputs, stop).await;
        assert!(fake.state.lock().unwrap().bans.is_empty());
    }
}
