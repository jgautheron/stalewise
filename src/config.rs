//! Command-line and environment configuration, validated once at startup.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use clap::{ArgAction, Parser, ValueEnum};
use hyper::header::HeaderValue;

use crate::endpoint::{AddressError, Authority, Cidr, DnsName, Host};

/// Invalid startup configuration; the process exits with code 2.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{flag}: {reason}")]
    Invalid { flag: &'static str, reason: String },
    #[error(transparent)]
    Address(#[from] AddressError),
}

fn invalid(flag: &'static str, reason: impl Into<String>) -> ConfigError {
    ConfigError::Invalid {
        flag,
        reason: reason.into(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Profile {
    Generic,
    Mesh,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Method {
    #[value(name = "PURGE")]
    Purge,
    #[value(name = "BAN")]
    Ban,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Purge => "PURGE",
            Self::Ban => "BAN",
        }
    }
}

/// Parses `250ms`, `2s` or `1m`.
pub fn parse_duration(raw: &str) -> Result<Duration, String> {
    let split = raw
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("{raw:?} needs a unit: ms, s or m"))?;
    let (digits, unit) = raw.split_at(split);
    let value: u64 = digits
        .parse()
        .map_err(|_| format!("{raw:?} is not a duration"))?;
    match unit {
        "ms" => Ok(Duration::from_millis(value)),
        "s" => Ok(Duration::from_secs(value)),
        "m" => value
            .checked_mul(60)
            .map(Duration::from_secs)
            .ok_or_else(|| format!("{raw:?} overflows")),
        _ => Err(format!("{raw:?} has unknown unit; use ms, s or m")),
    }
}

/// Raw flags; every flag has an environment equivalent and CLI wins.
#[derive(Debug, Parser)]
#[command(
    name = "stalewise",
    version,
    about = "Supervises Varnish as a Kubernetes-aware HTTP cache"
)]
pub struct Cli {
    #[arg(long, env = "STALEWISE_PROFILE", value_enum, default_value = "generic")]
    pub profile: Profile,
    #[arg(long, env = "STALEWISE_NAMESPACE", default_value = "default")]
    pub namespace: String,
    #[arg(long, env = "STALEWISE_FRONTEND_SERVICE")]
    pub frontend_service: Option<String>,
    #[arg(long, env = "STALEWISE_FRONTEND_SERVICE_HOST")]
    pub frontend_service_host: Option<String>,
    #[arg(long, env = "STALEWISE_FRONTEND_PORT_NAME", default_value = "http")]
    pub frontend_port_name: String,
    #[arg(long, env = "STALEWISE_BACKEND_WATCH", num_args = 0..=1, default_missing_value = "true", action = ArgAction::Set)]
    pub backend_watch: Option<bool>,
    #[arg(long, env = "STALEWISE_BACKEND_SERVICE")]
    pub backend_service: Option<String>,
    #[arg(long, env = "STALEWISE_BACKEND_PORT_NAME", default_value = "http")]
    pub backend_port_name: String,
    #[arg(long, env = "STALEWISE_BACKEND_HOST")]
    pub backend_host: Option<String>,
    #[arg(long, env = "STALEWISE_BACKEND_PORT", default_value_t = 8080)]
    pub backend_port: u16,
    #[arg(long, env = "STALEWISE_CANONICAL_HOST")]
    pub canonical_host: Option<String>,
    #[arg(long, env = "STALEWISE_ALLOW_EMPTY_BACKENDS", num_args = 0..=1, default_missing_value = "true", default_value = "false", action = ArgAction::Set)]
    pub allow_empty_backends: bool,
    #[arg(long, env = "STALEWISE_TEMPLATE")]
    pub template: Option<PathBuf>,
    #[arg(long, env = "STALEWISE_TEMPLATE_POLL", value_parser = parse_duration, default_value = "2s")]
    pub template_poll: Duration,
    #[arg(long, env = "STALEWISE_TEMPLATE_ENV", value_delimiter = ',', action = ArgAction::Append)]
    pub template_env: Vec<String>,
    #[arg(long, env = "STALEWISE_WORK_DIR", default_value = "/var/lib/stalewise")]
    pub work_dir: PathBuf,
    #[arg(
        long,
        env = "STALEWISE_VARNISH_WORK_DIR",
        default_value = "/var/lib/varnish"
    )]
    pub varnish_work_dir: PathBuf,
    #[arg(long, env = "STALEWISE_VARNISH_BIN", default_value = "varnishd")]
    pub varnish_bin: PathBuf,
    #[arg(long, env = "STALEWISE_LISTEN", default_value = "0.0.0.0:8080")]
    pub listen: SocketAddr,
    #[arg(long, env = "STALEWISE_ADMIN_LISTEN", default_value = "127.0.0.1:6082")]
    pub admin_listen: SocketAddr,
    #[arg(long, env = "STALEWISE_ADMIN_SECRET_FILE")]
    pub admin_secret_file: Option<PathBuf>,
    #[arg(long, env = "STALEWISE_ADMIN_TIMEOUT", value_parser = parse_duration, default_value = "5s")]
    pub admin_timeout: Duration,
    #[arg(long, env = "STALEWISE_STORAGE", default_value = "malloc,256m")]
    pub storage: String,
    #[arg(long, env = "STALEWISE_VARNISH_PARAM", value_delimiter = ',', action = ArgAction::Append)]
    pub varnish_param: Vec<String>,
    #[arg(long, env = "STALEWISE_MAX_VCLS", default_value_t = 5)]
    pub max_vcls: usize,
    #[arg(long, env = "STALEWISE_STARTUP_TIMEOUT", value_parser = parse_duration, default_value = "60s")]
    pub startup_timeout: Duration,
    #[arg(long, env = "STALEWISE_SHUTDOWN_TIMEOUT", value_parser = parse_duration, default_value = "30s")]
    pub shutdown_timeout: Duration,
    #[arg(
        long,
        env = "STALEWISE_MANAGEMENT_LISTEN",
        default_value = "0.0.0.0:9090"
    )]
    pub management_listen: SocketAddr,
    #[arg(long, env = "STALEWISE_SIGNALLER", num_args = 0..=1, default_missing_value = "true", default_value = "false", action = ArgAction::Set)]
    pub signaller: bool,
    #[arg(
        long,
        env = "STALEWISE_SIGNALLER_LISTEN",
        default_value = "0.0.0.0:8081"
    )]
    pub signaller_listen: SocketAddr,
    #[arg(long, env = "STALEWISE_SIGNALLER_TOKEN_FILE")]
    pub signaller_token_file: Option<PathBuf>,
    /// Host header for peer deliveries, e.g. the headless peers Service FQDN and port.
    #[arg(long, env = "STALEWISE_PEER_AUTHORITY")]
    pub peer_authority: Option<String>,
    /// Every address of this pod; dual-stack pods list both families.
    #[arg(long, env = "STALEWISE_POD_IP", value_delimiter = ',', action = ArgAction::Append)]
    pub pod_ip: Vec<IpAddr>,
    #[arg(long, env = "STALEWISE_SIGNALLER_METHODS", value_enum, value_delimiter = ',', action = ArgAction::Append)]
    pub signaller_methods: Vec<Method>,
    #[arg(long, env = "STALEWISE_SIGNALLER_BODY_LIMIT", default_value_t = 65536)]
    pub signaller_body_limit: usize,
    #[arg(long, env = "STALEWISE_SIGNALLER_WORKERS", default_value_t = 4)]
    pub signaller_workers: usize,
    #[arg(long, env = "STALEWISE_SIGNALLER_QUEUE", default_value_t = 1024)]
    pub signaller_queue: usize,
    #[arg(long, env = "STALEWISE_DELIVERY_DEADLINE", value_parser = parse_duration, default_value = "10s")]
    pub delivery_deadline: Duration,
    #[arg(long, env = "STALEWISE_PEER_TIMEOUT", value_parser = parse_duration, default_value = "2s")]
    pub peer_timeout: Duration,
    #[arg(long, env = "STALEWISE_HEADER_TIMEOUT", value_parser = parse_duration, default_value = "5s")]
    pub header_timeout: Duration,
    #[arg(long, env = "STALEWISE_READ_TIMEOUT", value_parser = parse_duration, default_value = "5s")]
    pub read_timeout: Duration,
    #[arg(long, env = "STALEWISE_WRITE_TIMEOUT", value_parser = parse_duration, default_value = "5s")]
    pub write_timeout: Duration,
    #[arg(long, env = "STALEWISE_IDLE_TIMEOUT", value_parser = parse_duration, default_value = "15s")]
    pub idle_timeout: Duration,
    #[arg(long, env = "STALEWISE_PURGE_ACL", value_delimiter = ',', action = ArgAction::Append)]
    pub purge_acl: Vec<String>,
    #[arg(long, env = "STALEWISE_BACKEND_PROBE_PATH", default_value = "/")]
    pub backend_probe_path: String,
    #[arg(long, env = "STALEWISE_UNCACHEABLE_TTL", value_parser = parse_duration, default_value = "30s")]
    pub uncacheable_ttl: Duration,
    #[arg(long, env = "STALEWISE_MAX_OBJECT_SIZE", default_value_t = 10_485_760)]
    pub max_object_size: u64,
    #[arg(long, env = "STALEWISE_LOG_LEVEL", default_value = "info")]
    pub log_level: String,
}

const DEFAULT_PURGE_ACL: [&str; 2] = ["127.0.0.1/32", "::1/128"];

/// Which EndpointSlice set a watcher follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Frontend,
    Backend,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Frontend => "frontend",
            Self::Backend => "backend",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleConfig {
    pub role: Role,
    pub namespace: String,
    pub service: String,
    pub port_name: String,
}

/// Origin discovery: a watched Service, or one static DNS name the mesh balances.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendSource {
    Watch(RoleConfig),
    Static { host: DnsName, port: u16 },
}

/// Template input: the embedded profile default or a trusted file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateSource {
    Embedded(Profile),
    File(PathBuf),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VarnishParam {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalConfig {
    pub listen: SocketAddr,
    pub token_file: PathBuf,
    pub pod_ips: Vec<IpAddr>,
    pub local_frontend: SocketAddr,
    pub canonical_host: Option<Authority>,
    /// Host header for peer deliveries; `None` sends the invalidation host.
    pub peer_authority: Option<HeaderValue>,
    pub methods: Vec<Method>,
    pub body_limit: usize,
    pub workers: usize,
    pub queue: usize,
    pub delivery_deadline: Duration,
    pub peer_timeout: Duration,
    pub header_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub idle_timeout: Duration,
}

/// Fully validated runtime configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub profile: Profile,
    pub frontend: RoleConfig,
    pub frontend_service_host: DnsName,
    pub backend: BackendSource,
    /// Host header origins expect; also sent by origin health probes.
    pub backend_authority: Authority,
    pub canonical_host: Option<Authority>,
    pub allow_empty_backends: bool,
    pub template: TemplateSource,
    pub template_poll: Duration,
    pub template_env: BTreeMap<String, String>,
    pub work_dir: PathBuf,
    pub varnish_work_dir: PathBuf,
    pub varnish_bin: PathBuf,
    pub listen: SocketAddr,
    pub admin_listen: SocketAddr,
    pub admin_secret_file: Option<PathBuf>,
    pub admin_timeout: Duration,
    pub storage: String,
    pub varnish_params: Vec<VarnishParam>,
    pub max_vcls: usize,
    pub startup_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub management_listen: SocketAddr,
    pub signaller: Option<SignalConfig>,
    pub purge_acl: Vec<Cidr>,
    pub backend_probe_path: String,
    pub uncacheable_ttl: Duration,
    pub max_object_size: u64,
    pub log_level: String,
}

/// Parses process arguments and environment; clap usage errors exit 2 itself.
pub fn load() -> Result<Config, ConfigError> {
    Config::from_cli(Cli::parse(), |name| std::env::var(name).ok())
}

impl Config {
    /// Applies profile defaults beneath explicit values, then validates everything.
    pub fn from_cli(cli: Cli, env: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let mesh = cli.profile == Profile::Mesh;
        let namespace = kube_name("--namespace", &cli.namespace)?;
        let frontend_service = cli
            .frontend_service
            .as_deref()
            .ok_or_else(|| invalid("--frontend-service", "is required"))?;
        let frontend_service = kube_name("--frontend-service", frontend_service)?;
        let frontend_service_host: DnsName = cli
            .frontend_service_host
            .as_deref()
            .ok_or_else(|| invalid("--frontend-service-host", "is required"))?
            .parse()?;
        let canonical_host = cli
            .canonical_host
            .as_deref()
            .map(str::parse::<Authority>)
            .transpose()?;
        if mesh && canonical_host.is_none() {
            return Err(invalid("--canonical-host", "is required by --profile=mesh"));
        }
        let frontend = RoleConfig {
            role: Role::Frontend,
            namespace: namespace.clone(),
            service: frontend_service,
            port_name: port_name("--frontend-port-name", &cli.frontend_port_name)?,
        };
        let watch = cli.backend_watch.unwrap_or(!mesh);
        let backend = backend_source(&cli, watch, &namespace)?;
        check_no_loop(&backend, &frontend, &frontend_service_host)?;
        let template = match &cli.template {
            Some(path) => TemplateSource::File(path.clone()),
            None => TemplateSource::Embedded(cli.profile),
        };
        let backend_authority = backend_authority(&backend)?;
        let signaller = signal_config(&cli, canonical_host.clone())?;
        Ok(Self {
            profile: cli.profile,
            frontend,
            frontend_service_host,
            backend,
            backend_authority,
            canonical_host,
            allow_empty_backends: cli.allow_empty_backends,
            template,
            template_poll: positive("--template-poll", cli.template_poll)?,
            template_env: template_env(&cli.template_env, env)?,
            work_dir: cli.work_dir,
            varnish_work_dir: cli.varnish_work_dir,
            varnish_bin: cli.varnish_bin,
            listen: frontend_listen(cli.listen)?,
            admin_listen: loopback("--admin-listen", cli.admin_listen)?,
            admin_secret_file: cli.admin_secret_file,
            admin_timeout: positive("--admin-timeout", cli.admin_timeout)?,
            storage: opaque("--storage", cli.storage)?,
            varnish_params: varnish_params(&cli.varnish_param)?,
            max_vcls: at_least_one("--max-vcls", cli.max_vcls)?,
            startup_timeout: positive("--startup-timeout", cli.startup_timeout)?,
            shutdown_timeout: positive("--shutdown-timeout", cli.shutdown_timeout)?,
            management_listen: cli.management_listen,
            signaller,
            purge_acl: purge_acl(&cli.purge_acl)?,
            backend_probe_path: probe_path(&cli.backend_probe_path)?,
            uncacheable_ttl: positive("--uncacheable-ttl", cli.uncacheable_ttl)?,
            max_object_size: if cli.max_object_size == 0 {
                return Err(invalid("--max-object-size", "must be positive"));
            } else {
                cli.max_object_size
            },
            log_level: cli.log_level,
        })
    }
}

fn backend_authority(backend: &BackendSource) -> Result<Authority, ConfigError> {
    let authority = match backend {
        BackendSource::Static { host, port } => {
            Authority::from_parts(&Host::Dns(host.clone()), Some(*port))
        }
        BackendSource::Watch(role) => format!("{}.{}.svc", role.service, role.namespace).parse()?,
    };
    Ok(authority)
}

fn backend_source(cli: &Cli, watch: bool, namespace: &str) -> Result<BackendSource, ConfigError> {
    if watch {
        if cli.backend_host.is_some() {
            return Err(invalid(
                "--backend-host",
                "conflicts with --backend-watch=true",
            ));
        }
        let service = cli
            .backend_service
            .as_deref()
            .ok_or_else(|| invalid("--backend-service", "is required when watching"))?;
        return Ok(BackendSource::Watch(RoleConfig {
            role: Role::Backend,
            namespace: namespace.to_owned(),
            service: kube_name("--backend-service", service)?,
            port_name: port_name("--backend-port-name", &cli.backend_port_name)?,
        }));
    }
    if cli.backend_service.is_some() {
        return Err(invalid(
            "--backend-service",
            "conflicts with a static backend",
        ));
    }
    let host = cli
        .backend_host
        .as_deref()
        .ok_or_else(|| invalid("--backend-host", "is required for a static backend"))?
        .parse()?;
    if cli.backend_port == 0 {
        return Err(invalid("--backend-port", "must be positive"));
    }
    Ok(BackendSource::Static {
        host,
        port: cli.backend_port,
    })
}

fn check_no_loop(
    backend: &BackendSource,
    frontend: &RoleConfig,
    frontend_host: &DnsName,
) -> Result<(), ConfigError> {
    let looped = match backend {
        BackendSource::Static { host, .. } => host == frontend_host,
        BackendSource::Watch(role) => role.service == frontend.service,
    };
    if looped {
        return Err(invalid(
            "--backend-host",
            "equals the cache's own Service and would route traffic back into the cache",
        ));
    }
    Ok(())
}

fn signal_config(
    cli: &Cli,
    canonical_host: Option<Authority>,
) -> Result<Option<SignalConfig>, ConfigError> {
    if !cli.signaller {
        return Ok(None);
    }
    let token_file = cli
        .signaller_token_file
        .clone()
        .ok_or_else(|| invalid("--signaller-token-file", "is required with --signaller"))?;
    if cli.pod_ip.is_empty() {
        return Err(invalid("--pod-ip", "is required with --signaller"));
    }
    let mut methods = cli.signaller_methods.clone();
    if methods.is_empty() {
        methods = vec![Method::Purge, Method::Ban];
    }
    methods.dedup();
    let local_ip: IpAddr = match cli.listen.ip() {
        ip if !ip.is_unspecified() => ip,
        IpAddr::V4(_) => IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        IpAddr::V6(_) => IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
    };
    Ok(Some(SignalConfig {
        listen: cli.signaller_listen,
        token_file,
        pod_ips: cli.pod_ip.clone(),
        local_frontend: SocketAddr::new(local_ip, cli.listen.port()),
        canonical_host,
        peer_authority: peer_authority(cli.peer_authority.as_deref())?,
        methods,
        body_limit: at_least_one("--signaller-body-limit", cli.signaller_body_limit)?,
        workers: at_least_one("--signaller-workers", cli.signaller_workers)?,
        queue: at_least_one("--signaller-queue", cli.signaller_queue)?,
        delivery_deadline: positive("--delivery-deadline", cli.delivery_deadline)?,
        peer_timeout: positive("--peer-timeout", cli.peer_timeout)?,
        header_timeout: positive("--header-timeout", cli.header_timeout)?,
        read_timeout: positive("--read-timeout", cli.read_timeout)?,
        write_timeout: positive("--write-timeout", cli.write_timeout)?,
        idle_timeout: positive("--idle-timeout", cli.idle_timeout)?,
    }))
}

fn peer_authority(raw: Option<&str>) -> Result<Option<HeaderValue>, ConfigError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let authority: Authority = raw.parse()?;
    HeaderValue::from_str(&authority.to_string())
        .map(Some)
        .map_err(|_| invalid("--peer-authority", "is not a valid Host header"))
}

fn kube_name(flag: &'static str, raw: &str) -> Result<String, ConfigError> {
    let ok = !raw.is_empty()
        && raw.len() <= 63
        && raw
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !raw.starts_with('-')
        && !raw.ends_with('-');
    if ok {
        Ok(raw.to_owned())
    } else {
        Err(invalid(flag, format!("{raw:?} is not a DNS-1123 label")))
    }
}

fn port_name(flag: &'static str, raw: &str) -> Result<String, ConfigError> {
    let ok = !raw.is_empty()
        && raw.len() <= 15
        && raw
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if ok {
        Ok(raw.to_owned())
    } else {
        Err(invalid(flag, format!("{raw:?} is not a Service port name")))
    }
}

fn positive(flag: &'static str, value: Duration) -> Result<Duration, ConfigError> {
    if value.is_zero() {
        Err(invalid(flag, "must be positive"))
    } else {
        Ok(value)
    }
}

fn at_least_one(flag: &'static str, value: usize) -> Result<usize, ConfigError> {
    if value == 0 {
        Err(invalid(flag, "must be positive"))
    } else {
        Ok(value)
    }
}

fn loopback(flag: &'static str, addr: SocketAddr) -> Result<SocketAddr, ConfigError> {
    if addr.ip().is_loopback() && addr.port() != 0 {
        Ok(addr)
    } else {
        Err(invalid(flag, "must be a loopback address with a port"))
    }
}

/// The readiness probe dials loopback, which only a wildcard or loopback bind answers.
fn frontend_listen(addr: SocketAddr) -> Result<SocketAddr, ConfigError> {
    let ip = addr.ip();
    if (ip.is_unspecified() || ip.is_loopback()) && addr.port() != 0 {
        Ok(addr)
    } else {
        Err(invalid(
            "--listen",
            "must be a wildcard or loopback address with a port",
        ))
    }
}

fn opaque(flag: &'static str, value: String) -> Result<String, ConfigError> {
    if value.is_empty()
        || value
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        Err(invalid(flag, "must be nonempty without whitespace"))
    } else {
        Ok(value)
    }
}

fn varnish_params(raw: &[String]) -> Result<Vec<VarnishParam>, ConfigError> {
    raw.iter()
        .map(|item| {
            let (key, value) = item
                .split_once('=')
                .ok_or_else(|| invalid("--varnish-param", format!("{item:?} is not key=value")))?;
            let key_ok =
                !key.is_empty() && key.bytes().all(|b| b.is_ascii_lowercase() || b == b'_');
            if !key_ok {
                return Err(invalid("--varnish-param", format!("bad key {key:?}")));
            }
            Ok(VarnishParam {
                key: key.to_owned(),
                value: opaque("--varnish-param", value.to_owned())?,
            })
        })
        .collect()
}

fn purge_acl(raw: &[String]) -> Result<Vec<Cidr>, ConfigError> {
    let items: Vec<&str> = if raw.is_empty() {
        DEFAULT_PURGE_ACL.to_vec()
    } else {
        raw.iter().map(String::as_str).collect()
    };
    let mut acl = items
        .into_iter()
        .map(|item| item.trim().parse::<Cidr>())
        .collect::<Result<Vec<_>, _>>()?;
    acl.dedup();
    Ok(acl)
}

fn probe_path(raw: &str) -> Result<String, ConfigError> {
    let ok = raw.starts_with('/')
        && raw.len() <= 256
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/-._~%?=&".contains(&b));
    if ok {
        Ok(raw.to_owned())
    } else {
        Err(invalid(
            "--backend-probe-path",
            "must be an origin-form path without quotes or spaces",
        ))
    }
}

fn template_env(
    names: &[String],
    env: impl Fn(&str) -> Option<String>,
) -> Result<BTreeMap<String, String>, ConfigError> {
    let mut out = BTreeMap::new();
    for name in names {
        let ok = !name.is_empty()
            && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            && !name.as_bytes().first().is_some_and(u8::is_ascii_digit);
        if !ok {
            return Err(invalid(
                "--template-env",
                format!("{name:?} is not a variable name"),
            ));
        }
        if let Some(value) = env(name) {
            out.insert(name.clone(), value);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Config, ConfigError> {
        let mut argv = vec!["stalewise"];
        argv.extend_from_slice(args);
        let cli = Cli::try_parse_from(argv).map_err(|e| invalid("clap", e.to_string()))?;
        Config::from_cli(cli, |name| (name == "SHARD").then(|| "a".to_owned()))
    }

    const BASE: [&str; 4] = [
        "--frontend-service=cache-peers",
        "--frontend-service-host=cache.apps.svc.cluster.local",
        "--namespace=apps",
        "--backend-service=catalog",
    ];

    #[test]
    fn generic_profile_watches_backend_with_loopback_acl() {
        let cfg = parse(&BASE).unwrap();
        assert!(matches!(cfg.backend, BackendSource::Watch(ref r) if r.service == "catalog"));
        assert_eq!(cfg.template, TemplateSource::Embedded(Profile::Generic));
        let acl: Vec<String> = cfg.purge_acl.iter().map(ToString::to_string).collect();
        assert_eq!(acl, ["127.0.0.1/32", "::1/128"]);
        assert!(cfg.signaller.is_none());
    }

    #[test]
    fn mesh_profile_expands_to_static_backend_and_requires_canonical_host() {
        let args = [
            "--profile=mesh",
            "--frontend-service=cache-peers",
            "--frontend-service-host=catalog-cache.apps.svc.cluster.local",
            "--backend-host=catalog.apps.svc.cluster.local",
        ];
        let err = parse(&args).unwrap_err();
        assert!(err.to_string().contains("--canonical-host"));
        let mut args = args.to_vec();
        args.push("--canonical-host=catalog.apps.svc.cluster.local");
        let cfg = parse(&args).unwrap();
        assert!(matches!(
            cfg.backend,
            BackendSource::Static { port: 8080, .. }
        ));
        assert_eq!(cfg.template, TemplateSource::Embedded(Profile::Mesh));
        assert_eq!(
            cfg.backend_authority.as_str(),
            "catalog.apps.svc.cluster.local:8080"
        );
    }

    #[test]
    fn explicit_watch_overrides_mesh_default() {
        let args = [
            "--profile=mesh",
            "--canonical-host=catalog",
            "--frontend-service=peers",
            "--frontend-service-host=c.apps",
            "--backend-watch",
            "--backend-service=catalog",
        ];
        assert!(matches!(
            parse(&args).unwrap().backend,
            BackendSource::Watch(_)
        ));
    }

    #[test]
    fn rejects_backend_equal_to_cache_service_host() {
        let args = [
            "--frontend-service=peers",
            "--frontend-service-host=cache.apps.svc",
            "--backend-watch=false",
            "--backend-host=CACHE.apps.svc",
        ];
        assert!(
            parse(&args)
                .unwrap_err()
                .to_string()
                .contains("route traffic back")
        );
    }

    #[test]
    fn rejects_conflicting_static_and_watch_inputs() {
        let mut args = BASE.to_vec();
        args.push("--backend-host=x.y");
        assert!(parse(&args).is_err());
        let args = [
            "--frontend-service=p",
            "--frontend-service-host=c.d",
            "--backend-watch=false",
            "--backend-service=x",
            "--backend-host=x.y",
        ];
        assert!(parse(&args).is_err());
    }

    #[test]
    fn rejects_invalid_values() {
        for extra in [
            "--purge-acl=10.0.0.0/40",
            "--admin-listen=0.0.0.0:6082",
            "--listen=10.0.0.5:8080",
            "--listen=[2001:db8::5]:8080",
            "--max-vcls=0",
            "--template-poll=0s",
            "--backend-probe-path=/a\"b",
            "--varnish-param=nokey",
            "--template-env=1BAD",
            "--canonical-host=a b",
        ] {
            let mut args = BASE.to_vec();
            args.push(extra);
            assert!(parse(&args).is_err(), "{extra}");
        }
    }

    #[test]
    fn signaller_requires_token_and_pod_ip_and_defaults_methods() {
        let mut args = BASE.to_vec();
        args.push("--signaller");
        assert!(parse(&args).is_err());
        args.extend(["--signaller-token-file=/t", "--pod-ip=10.0.0.7,fd00::7"]);
        let sig = parse(&args).unwrap().signaller.unwrap();
        assert_eq!(sig.pod_ips.len(), 2, "dual-stack downward API value splits");
        assert_eq!(sig.methods, [Method::Purge, Method::Ban]);
        assert_eq!(sig.local_frontend, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(sig.workers, 4);
        assert_eq!(sig.peer_authority, None);
        args.push("--peer-authority=cache-peers.ns.svc.cluster.local:8080");
        let sig = parse(&args).unwrap().signaller.unwrap();
        assert_eq!(
            sig.peer_authority.unwrap(),
            "cache-peers.ns.svc.cluster.local:8080"
        );
        args.push("--peer-authority=a b");
        assert!(parse(&args).is_err(), "peer authority is validated");
    }

    #[test]
    fn lists_split_on_commas_and_append_and_env_is_allow_listed() {
        let mut args = BASE.to_vec();
        args.extend([
            "--purge-acl=10.0.0.0/8,127.0.0.1",
            "--purge-acl=::1",
            "--template-env=SHARD,ABSENT",
            "--varnish-param=thread_pools=2,http_max_hdr=128",
        ]);
        let cfg = parse(&args).unwrap();
        assert_eq!(cfg.purge_acl.len(), 3);
        assert_eq!(cfg.template_env.len(), 1);
        assert_eq!(cfg.varnish_params.len(), 2);
    }

    #[test]
    fn listen_accepts_wildcards_and_loopback_the_probe_can_reach() {
        for listen in ["0.0.0.0:8080", "[::]:8080", "127.0.0.1:8080", "[::1]:8080"] {
            let mut args = BASE.to_vec();
            let flag = format!("--listen={listen}");
            args.push(&flag);
            assert!(parse(&args).is_ok(), "{listen}");
        }
    }

    #[test]
    fn durations_need_units() {
        assert_eq!(parse_duration("250ms").unwrap(), Duration::from_millis(250));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert!(parse_duration("2").is_err());
        assert!(parse_duration("2h").is_err());
        assert!(parse_duration("-1s").is_err());
    }
}
