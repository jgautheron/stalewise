# stalewise v0.3

## Shape and interfaces

One Rust 2024 binary crate supervises Varnish. It never handles cache traffic. It serves one application per deployment. v0.2 added the `CachePolicy` resource, read-only attachment validation, and a structured config file. v0.3 adds stable peer membership, opt-in VCL sharding, and observable purge delivery.

These are module contracts, not implementation. `Result<T>` carries the owning module's typed error. Invalid external inputs never reach rendering.

| Module | Responsibility and public interface |
|---|---|
| `config` | Validate flags/environment once: `fn load() -> Result<Config>` |
| `discovery` | Own each slice store: `async fn watch(client: kube::Client, role: RoleConfig, out: watch::Sender<Discovery>, stop: CancellationToken) -> Result<()>`; `fn merge(slices: &[EndpointSlice], port: &str) -> Result<Vec<Endpoint>>` |
| `render` | Validate template inputs and produce bytes: `fn render(template: &str, context: &Context) -> Result<Vec<u8>>`; `fn publish(dir: &File, bytes: &[u8]) -> Result<()>`; `async fn poll(path: PathBuf, out: watch::Sender<Template>, stop: CancellationToken) -> Result<()>` |
| `varnish::process` | Own/reap child: `async fn supervise(config: ProcessConfig, state: watch::Sender<ProcessState>, stop: CancellationToken) -> Result<ExitStatus>` |
| `varnish::admin` | Frame/authenticate commands: `async fn connect(config: &AdminConfig) -> Result<Client>`; `Client::{ping,inline,use_vcl,list,discard}` are asynchronous methods returning `Result<Pong>`, `Result<()>`, `Result<()>`, `Result<Vec<VclState>>`, `Result<()>` respectively |
| `reload` | Sole configuration writer: `async fn run(config: ReloadConfig, inputs: Inputs, state: watch::Sender<Active>, stop: CancellationToken) -> Result<()>` |
| `signaller` | Validate/admit bounded invalidations: `async fn serve(config: SignalConfig, peers: watch::Receiver<Discovery>, metrics: Arc<Metrics>, stop: CancellationToken) -> Result<()>` |
| `mgmt` | Probe/publish status and metrics: `async fn serve(config: ManagementConfig, inputs: StatusInputs, metrics: Arc<Metrics>, stop: CancellationToken) -> Result<()>`; `fn readiness(inputs: &StatusInputs) -> Readiness` |
| `policy` | `CachePolicy` CRD types, runtime validation and resolution: `fn validate(name, generation, spec, own) -> Result<RenderPolicy, Rejection>`; `fn resolve(candidates, accepted) -> Resolution`; `fn crd_yaml()` |
| `attach` | Pure route check: `fn validate(target: &Target, virtual_services: &[serde_json::Value]) -> Outcome` |
| `controller` | Watch CachePolicies and VirtualServices, publish accepted policies, write status: `async fn run(client, config: PolicyConfig, out: watch::Sender<Option<Policies>>, active: watch::Receiver<Option<Active>>, stop)` |
| `main` | Wire tasks and exit codes: `async fn run(config: Config) -> Result<ExitCode>` |

Admin arguments are `inline(&mut self,name:&VclName,source:&[u8])`, `use_vcl/discard(&mut self,name:&VclName)` and `ping/list(&mut self)`.

Shared values: `Host { Ip(IpAddr), Dns(String) }`; `Endpoint { name:String, host:Host, port:u16 }`; `Discovery { endpoints:Arc<[Endpoint]>, last_success:SystemTime, error:Option<DiscoveryError> }`; `Active { name:VclName, bytes:Arc<[u8]>, since:Instant, policies:Arc<[RenderPolicy]> }`. Initial discovery/active states are `Option::None`, never synthetic success.

## Data flow and tasks

Tokio runs independent frontend/backend watchers, template polling, the optional CachePolicy controller, reload, child supervision, management/probing, and optional signaller tasks. Each producer publishes immutable snapshots through `watch<Option<T>>`, capacity one/latest-value. Reload consumes discovery/template/policy snapshots; signaller consumes frontend snapshots. The controller consumes `Active` to learn which policy generations are live. Policies never gate startup: until the controller publishes, the VCL is headers-only. Management reads all states. No unbounded event channels exist.

Each watcher selects `kubernetes.io/service-name`, matches TCP named ports, and merges by normalized `(IP,port)`. Accept `ready != false` and `terminating != true`; deduplicate and sort numerically. Reject invalid addresses/ports. Relists replace the complete store atomically; partial lists never erase healthy state. Watch expiration relists; errors retain snapshots and retry with 0.5–30s exponential full jitter. API requests time out after 30s; watches reconnect after 300s.

`reload::initial(config:&ReloadConfig,inputs:&mut Inputs) -> Result<InitialVcl>` is asynchronous and owns initial publication. Main passes its result into child supervision. Reload then serializes render/compile/activate/cleanup. Identical active bytes skip. Startup starts management immediately, obtains a valid initial origin configuration, publishes VCL, then starts the child. Peer discovery never gates startup. Static mesh origins need no API response.

Signaller queues `Delivery { request:Arc<Invalidation>, peer:SocketAddr, deadline:Instant }` in bounded `mpsc`, default 1024. Admission reserves the entire peer batch atomically or returns 503. Four workers consume through a dispatcher with at most four in-flight jobs. Snapshot peers plus local Varnish; remove the local pod IP before adding loopback. No forwarding to signaller ports.

A root `CancellationToken` stops admission/producers. Drain accepted deliveries within their deadlines, then SIGTERM and reap Varnish within the remaining 30s shutdown budget; SIGKILL on expiry. Unexpected child exit cancels everything and exits nonzero. Task failures reach main; task panics cannot silently remove supervision.

## Failure direction

Serving fails open; configuration changes fail closed. Readiness is 200 exactly when an active VCL exists and a fresh local frontend probe succeeds. Degradation never removes a working cache from traffic. Probe a loopback-only synthetic `GET /__stalewise_probe` every 2s, timeout 1s; expire success after 5s and invalidate it immediately on child exit. Use IPv6 loopback for IPv6-only frontend binds.

| Failure | Effect and operator signal |
|---|---|
| API unavailable | Keep snapshots/VCL; warning, discovery error/degraded gauges; readiness 200 if serving |
| Empty frontend | Warning/event/count only; never a readiness gate |
| Empty backend | Keep VCL; 200 with `degraded: backend has no endpoints`; degraded gauge/event counter |
| Child crash | Exit nonzero; health/readiness 503 while listeners remain |
| Admin authentication | Refuse changes; error counter/log; degraded 200, or 503 without initial active VCL |
| Compile/invalid template | Preserve active configuration; sanitized diagnostics/failure counter; degraded 200, initial 503 |
| Queue full | Reject entire invalidation with 503; request counter/queue gauge; readiness unaffected |
| Peer unreachable | Retry transport errors, 408/429/5xx until deadline; exhausted counter/warning; accepted HTTP response remains 202 |
| Invalid token/method/body | Fail closed: 401/405/413; bounded status counters; no queue insertion |

`/healthz` is 200 only while the child is alive. `/readyz` includes bounded reason codes and degradation details. Initial configuration/child startup exceeding 60s exits nonzero. Secret/configuration errors exit immediately; listener failures are fatal.

## CLI contract

CLI overrides environment, then the `--config` file, then profile defaults. List-valued flags are comma-separated; repeated flags append. Storage is one opaque value, not a list. Boolean flags accept explicit true/false. Durations use `ms`, `s`, `m`. `—` means absent.

| Flag | Environment | Default |
|---|---|---|
| `--profile` | `STALEWISE_PROFILE` | generic |
| `--namespace` | `STALEWISE_NAMESPACE` | default |
| `--frontend-service` | `STALEWISE_FRONTEND_SERVICE` | required peer Service |
| `--frontend-service-host` | `STALEWISE_FRONTEND_SERVICE_HOST` | required traffic Service DNS |
| `--frontend-port-name` | `STALEWISE_FRONTEND_PORT_NAME` | http |
| `--backend-watch` | `STALEWISE_BACKEND_WATCH` | true |
| `--backend-service` | `STALEWISE_BACKEND_SERVICE` | required when watching |
| `--backend-port-name` | `STALEWISE_BACKEND_PORT_NAME` | http |
| `--backend-host` | `STALEWISE_BACKEND_HOST` | required when static |
| `--backend-port` | `STALEWISE_BACKEND_PORT` | 8080 |
| `--canonical-host` | `STALEWISE_CANONICAL_HOST` | —; required for mesh |
| `--allow-empty-backends` | `STALEWISE_ALLOW_EMPTY_BACKENDS` | false |
| `--template` | `STALEWISE_TEMPLATE` | embedded profile template |
| `--template-poll` | `STALEWISE_TEMPLATE_POLL` | 2s |
| `--template-env` | `STALEWISE_TEMPLATE_ENV` | empty allow-list |
| `--work-dir` | `STALEWISE_WORK_DIR` | /var/lib/stalewise |
| `--varnish-work-dir` | `STALEWISE_VARNISH_WORK_DIR` | /var/lib/varnish |
| `--varnish-bin` | `STALEWISE_VARNISH_BIN` | varnishd |
| `--listen` | `STALEWISE_LISTEN` | 0.0.0.0:8080; wildcard or loopback only |
| `--admin-listen` | `STALEWISE_ADMIN_LISTEN` | 127.0.0.1:6082 |
| `--admin-secret-file` | `STALEWISE_ADMIN_SECRET_FILE` | generated work-dir/admin.secret |
| `--admin-timeout` | `STALEWISE_ADMIN_TIMEOUT` | 5s |
| `--storage` | `STALEWISE_STORAGE` | malloc,256m |
| `--varnish-param` | `STALEWISE_VARNISH_PARAM` | empty key=value list |
| `--max-vcls` | `STALEWISE_MAX_VCLS` | 5 including active |
| `--startup-timeout` | `STALEWISE_STARTUP_TIMEOUT` | 60s |
| `--shutdown-timeout` | `STALEWISE_SHUTDOWN_TIMEOUT` | 30s |
| `--management-listen` | `STALEWISE_MANAGEMENT_LISTEN` | 0.0.0.0:9090 |
| `--signaller` | `STALEWISE_SIGNALLER` | false |
| `--signaller-listen` | `STALEWISE_SIGNALLER_LISTEN` | 0.0.0.0:8081 |
| `--signaller-token-file` | `STALEWISE_SIGNALLER_TOKEN_FILE` | required when enabled |
| `--pod-ip` | `STALEWISE_POD_IP` | required when enabled; comma-separated pod IPs |
| `--signaller-methods` | `STALEWISE_SIGNALLER_METHODS` | PURGE,BAN |
| `--signaller-body-limit` | `STALEWISE_SIGNALLER_BODY_LIMIT` | 65536 bytes |
| `--signaller-workers` | `STALEWISE_SIGNALLER_WORKERS` | 4 |
| `--signaller-queue` | `STALEWISE_SIGNALLER_QUEUE` | 1024 deliveries |
| `--delivery-deadline` | `STALEWISE_DELIVERY_DEADLINE` | 10s from admission |
| `--peer-timeout` | `STALEWISE_PEER_TIMEOUT` | 2s per attempt |
| `--header-timeout` | `STALEWISE_HEADER_TIMEOUT` | 5s |
| `--read-timeout` | `STALEWISE_READ_TIMEOUT` | 5s |
| `--write-timeout` | `STALEWISE_WRITE_TIMEOUT` | 5s |
| `--idle-timeout` | `STALEWISE_IDLE_TIMEOUT` | 15s |
| `--purge-acl` | `STALEWISE_PURGE_ACL` | 127.0.0.1/32,::1/128 |
| `--backend-probe-path` | `STALEWISE_BACKEND_PROBE_PATH` | / |
| `--uncacheable-ttl` | `STALEWISE_UNCACHEABLE_TTL` | 30s |
| `--max-object-size` | `STALEWISE_MAX_OBJECT_SIZE` | 10485760 bytes |
| `--log-level` | `STALEWISE_LOG_LEVEL` | info |
| `--config` | `STALEWISE_CONFIG` | —; YAML file, see below |
| `--cache-policies` | `STALEWISE_CACHE_POLICIES` | false; conflicts with `--template` |
| `--policy-service` | `STALEWISE_POLICY_SERVICE` | origin Service, or first label of a static origin host |
| `--validate-attachment` | `STALEWISE_VALIDATE_ATTACHMENT` | value of `--cache-policies` |
| `--cache-source-labels` | `STALEWISE_CACHE_SOURCE_LABELS` | empty key=value list; required with attachment validation |

`--help`/`--version` are informational, without environment equivalents. `stalewise crd` prints the CachePolicy CRD.

The config file is YAML (serde_norway) with `apiVersion: stalewise.dev/v1alpha1` and `kind: Config`. Other keys are flag names in camelCase; values are scalars or lists of scalars. A file value applies only where neither the CLI nor the environment set the flag, and then passes through the same validation. Unknown keys, nested maps and header mismatches fail startup with the file path. Raw templates remain the expert mode.

`--profile=mesh` expands to static backend discovery, mesh template, wildcard frontend binding, and asynchronous peers. Explicit settings override these defaults. Reject conflicting static/watch inputs, invalid authorities/CIDRs, nonpositive limits, and backend host equal to the cache traffic Service host. Chart `cache.enabled=true` derives required identity settings; users need no additional cache policy.

## Rendering and admin protocol

Context: `{frontends:[{name,host,port}],backends:[{name,host,port}],primary_frontend:Endpoint|null,primary_backend:Endpoint|null,env:{NAME:value},canonical_host:string|null,backend_authority:string,purge_acl:[CIDR]}`. Static hosts are validated DNS names. Primary means first sorted entry. Empty opt-in generates a `backend none` miss-to-503 configuration.

Names use `ip_`/`dns_`, lowercase, replace non-`[a-z0-9_]` bytes with `_`, and append port plus address-family; collision detection rejects ambiguous identifiers. Host/port/CIDR types are validated. Environment strings use a VCL-string filter, rejecting controls and escaping quotes/backslashes. Raw templates are trusted executable configuration, not a tenant sandbox. Strict undefined variables fail rendering.

Poll file contents through ConfigMap symlinks, hashing bytes; invalid reads/templates retain the last accepted revision. Output paths never follow symlinks: trusted directory descriptors, exclusive 0600 temporary files, fsync, rename, directory fsync. Compile candidates before publishing active bytes. See [architecture ADR](adr/0001-architecture.md) for recovery details.

Generic VCL uses a round-robin origin director; mesh VCL uses one Service DNS origin and rewrites `bereq.http.host`. Both normalize canonical Host, provide the local synthetic probe, select `req.backend_hint` before purge, enforce ACLs, and exclude request IDs from hashing. Neither routes through frontend peers.

Signaller forwarding removes hop-by-hop headers, Connection-nominated headers, and Authorization. Token comparison is constant-time; configured token files are mandatory. Header/body/write/idle deadlines bound every connection. Accepted deliveries are volatile and may duplicate on retry. Remote invalidations are admitted from discovered peer pod addresses rendered as host ACL entries, plus network/mesh authorization.

The [official CLI protocol](https://www.varnish.org/docs/reference/cli_protocol/) specifies a 13-byte status line: three-digit status, space, eight-character padded decimal body length, LF. Read exactly that body plus its framing LF; cap responses at 1MiB. Status 107 provides a 32-byte challenge. Send `auth ` plus lowercase SHA256 of `challenge + LF + secret_bytes + challenge + LF`; preserve secret-file newlines. Require 200. Encode `vcl.inline NAME "escaped source"` using [CLI escaping](https://www.varnish.org/docs/reference/varnish-cli/). Close each reconciliation session; never blindly replay uncertain `vcl.use` results.

## Dependencies

v0.2 adds kube's `derive` feature, schemars 1.2.3 (CRD schema), serde_json 1.0.154 (unstructured VirtualServices, status patches), serde_norway 0.9.42 (config file, CRD output), and k8s-openapi's `schemars` feature.

Verified crates.io `max_stable_version` and feature/dependency responses on 2026-11-07 using Python urllib GET with `User-Agent: stalewise-design/0.1 (dependency research)`.

Reproduce: `curl -H 'User-Agent: stalewise-design/0.1 (dependency research)' https://crates.io/api/v1/crates/<name>`.

| Exact version | Purpose/features |
|---|---|
| tokio 1.54.1 | runtime, net, process, signal, sync, time, fs, io-util, macros |
| kube 5.0.0 | client/runtime/derive/rustls-tls/ring |
| k8s-openapi 0.29.0 | `v1_33`; supported baseline Kubernetes 1.33 |
| hyper 1.12.0 | HTTP/1 server/client |
| hyper-util 0.1.21 | Tokio I/O adapter |
| http-body-util 0.1.5; bytes 1.12.1 | bounded HTTP bodies |
| minijinja 3.0.5 | serde/fuel, strict templates |
| serde 1.0.229 | derive context serialization |
| clap 4.6.7 | derive/env flags |
| tracing 0.1.44; tracing-subscriber 0.3.23 | structured diagnostics |
| prometheus-client 0.25.2 | bounded metrics exposition |
| sha2 0.11.1; rand 0.10.3 | authentication, hashes, secret randomness/jitter |
| thiserror 2.0.21; tokio-util 0.7.20 | typed errors; rt/CancellationToken |
| futures-util 0.3.34 | watcher StreamExt |
| libc 0.2.191; subtle 2.6.1 | descriptor-safe writes/SIGTERM; constant-time token comparison |

`GET /api/v1/crates/kube/5.0.0/dependencies` returned `k8s-openapi ^0.29.0`; its feature response includes `v1_33`. kube requires Rust 1.89.0. Pin the resolved lockfile and a compatible stable toolchain.

Reject axum 0.8.9, anyhow 1.0.104, and third-party admin clients: unnecessary layers. Defer YAML entirely: serde_yaml 0.9.34+deprecated identifies deprecation in registry metadata; prefer serde_norway 0.9.42 if v0.2 needs YAML.

## Per-service cache policy

v0.1 requires no service policy: backend headers decide caching. Default TTL is zero. Request Authorization/Cookie passes; Upgrade/Expect pipes. Preserve Vary, Range, and conditional semantics. Never store Set-Cookie, private, no-store, or no-cache responses. Surrogate-Control overrides freshness directives, not privacy safeguards, and disappears before delivery.

Use s-maxage before max-age, accounting for Age. Store separate stale-while-revalidate and stale-if-error windows; `beresp.grace` retains their maximum, while health selects the usable window. Varnish already initializes grace from Cache-Control stale-while-revalidate; custom parsing handles surrogate directives and stale-if-error. See [policy ADR](adr/0002-cache-policy-source.md) for exact semantics, VCL parsing, and schema.

Uncacheable responses become configurable 30s hit-for-miss objects; already-waiting first requests cannot be retrospectively unqueued. Always stream. Event streams never store. Oversized Content-Length bypasses storage; explicitly cacheable unknown-length bodies remain cacheable and can consume bounded storage.

Tag headers enable host-scoped BAN with validated tokens, never arbitrary ban expressions. v0.2 introduces namespaced `stalewise.dev/v1alpha1 CachePolicy`: Service/path selection, typed overrides, bypass rules, and Accepted/Invalid status. Unset fields inherit headers; most-specific path wins; equal-specificity conflicts are rejected. Invalid changes preserve active VCL. Any application chart can include a CachePolicy template. Reject annotations for weak validation/status; reject central values for wrong ownership. Headers-only remains the default, not the only future policy source.

### v0.2 policy rendering

The controller validates every policy for its own Service, keeps the last accepted spec of a policy whose new generation is invalid, drops every policy that shares a normalized prefix with another, and orders the rest longest prefix first. Each rendered policy gets a numeric tag. `vcl_recv` matches `req.url` on segment boundaries, applies bypass rules, method limits, cookie stripping and the query allow-list key; `vcl_backend_response` matches `bereq.url` again and applies TTL, grace, keep, status and size overrides beside the header logic. Internal headers carrying the tag never reach the origin or the client. Policy output joins the same single-writer reload path, so a candidate that fails compilation leaves the active VCL in place and the policy stays `Accepted=False/Pending`.

## Traffic attachment: how a service opts into caching

An application chart owns one cache Deployment and its existing VirtualService. Enabling caching forces that VirtualService to render. Its ordered rules are: cache source labels → normal destinations; `x-stalewise-bypass: true` → normal; GET/HEAD → cache; everything else → normal. Omitted methods/pathPrefixes mean every GET/HEAD. [Attachment ADR](adr/0003-traffic-attachment.md) contains exact YAML.

This preserves origin canary weights/retries on misses. Hits ignore those weights; bypass canary tests or add a version key later. Every routed request pays a mesh hop, including uncacheable responses. Path restrictions only avoid that cost. A counter recording rule approximates pass/hit-for-miss share per service.

v0.2 validates attachment status; stalewise never creates/edits VirtualServices. It reads `networking.istio.io/v1` VirtualServices as unstructured objects, evaluates synthetic cache-sourced, bypass-header and plain GET/HEAD requests against the HTTP rules in order, and reports the first failure. Match conditions it cannot evaluate (regex, query parameters, other headers) never count as matching. Source-label bypass requires sidecars; use a separate direct Service otherwise. Ingress HTTPRoute attachment is separate: explicitly target the cache Service. Without mesh routing, callers explicitly use cache DNS. Backend sidecars remain a latency-focused alternative; reject client sidecars and unsupported cache-filter plumbing. Shared Host-routed tiers come later. Removing attachment restores direct traffic after route propagation, without client changes.

## Metrics and exporter

`/metrics` exports reload attempts/successes/failures/skips, active age, discovery events/counts/last-success timestamps, signaller status classes/depth/deliveries/exhaustions, readiness, and degradation. Labels are bounded enums, never hosts or URLs. The [architecture ADR](adr/0001-architecture.md) defines names and the exporter recording rule. A separate exporter accesses shared VSM or local varnishstat, exposes port 9131, and supplies MAIN.s_pass/cache_hitmiss/cache_hitpass/client_req. Its approximate per-service pass/hit-for-miss share guides path restrictions; stalewise never reimplements Varnish counters. No exporter image is assumed verified.

## Validation, phases, rollback

Test every module: configuration precedence, EndpointSlice merge/relist fixtures, escaping/symlinks, unchanged bytes, lifecycle, and status. An in-process fake CLI fragments frames, rejects authentication, stalls, and loses activation replies. Signaller tests cover authentication, limits, atomic queue admission, headers, IPv6, retries, deadlines, and local deduplication.

CI runs Docker `varnish:9.1.0`: both templates, header/stale/tag behavior, streaming, reload preservation, process death, and shutdown. Pin image digests; verify uid 1000 rather than trusting the tag. Enforce fmt/clippy/tests/deny; use non-root, read-only root, writable work volumes, separate Services, namespaced RBAC, probes, and pinned signed multi-arch packaging.

Build admin/render/process first, then discovery/reload, signaller/management, packaging. Restore previous template/config to roll back; remove traffic attachment before rolling back binaries. v0.2 owns structured policy/CRDs and attachment validation. v0.3 owns stable membership, opt-in sharding, and observable purge delivery. Shared-tier routing remains deferred. Delivery deadlines already belong to v0.1.

## v0.3

Implemented in 0.3.0. [ADR 0004](adr/0004-sharding-and-purge-consistency.md) defines routing, failure handling, tests, migration, and rejected alternatives. Existing modules remain responsible; there is no new proxy or controller.

### Configuration and ownership

| Flag | Default | Contract |
|---|---|---|
| `--shard` | false | Enable VCL consistent hashing across cache peers. |
| `--pod-name` | required for sharding/signaller | Identify this pod; the chart supplies the downward API value. |
| `--membership-debounce` | 5s | Validate 100ms–30s; stabilize frontend membership before reload. |
| `--shard-local-ttl` | 0s | Validate 0–30s; zero disables non-owner body storage. |
| `--invalidation-history` | 1024 | Positive maximum retained invalidation records. |

Use the existing flag/environment precedence with matching `STALEWISE_` names. Sharding additionally requires validated `--peer-authority` and `--pod-ip`; reject incomplete configuration before startup. Custom templates must implement this contract or reject sharding, never silently ignore it.

Discovery retains `Peer { pod_name, namespace, address, port }`, including local identity, separately from origin endpoints. It validates Pod references and readiness, sorts by pod name, deduplicates dual-stack endpoints, and publishes hysteresis-filtered snapshots. Reload renders equal-weight peer backends with stable `namespace/pod_name` identifiers and 67 virtual points through `directors.shard().reconfigure(67)`. No Rust request hashing occurs. Empty membership means direct origin; failed discovery/reload preserves working state.

VCL routes normalized authority plus effective URL to one owner, independently of local health observations. Owner requests fetch origin; non-owner misses dial the owner's IP with the peers Host and trusted original-host/hop headers. Received peer requests never forward. One pre-delivery failure permits direct-origin fallback. Bypass/privacy behavior remains unchanged. Local-copy TTL never exceeds owner freshness, and local grace/keep are zero.

### Signaller state and HTTP

The signaller owns `admit(Invalidation) -> Result<Receipt, Rejection>` and `lookup(Id) -> Option<Status>` semantics. Its dispatcher alone transitions peer results monotonically from pending to delivered/exhausted. Delivery means acknowledgment, not a distributed read barrier.

```text
Status { id, pod, peers: [{ pod, address, status, reason }] }
status = pending | delivered | exhausted
reason = null | rejected | deadline | shutdown
```

Return status JSON with `Cache-Control: no-store`. IDs are 32 hexadecimal characters; reject malformed IDs/durations with 400. Authenticate all status and wait requests using the existing bearer token, returning 401 before lookup. Unknown IDs return 404. Wrong methods return 405. Malformed or duplicated wait controls return 400.

Reserve history and delivery slots atomically. Bound retained peer rows to four times queue capacity. Evict oldest terminal records to fit both limits; never evict pending records. If either budget cannot fit, return 503 without queuing anything. Restart loses history and accepted work; receipts are not durable.

Writes return 202, relative Location, and `X-Stalewise-Pod`. `X-Stalewise-Wait` on writes or `?wait` on status GETs accepts positive `ms/s/m` durations through 30s. Wait returns 200 after all acknowledgments, or 504 with status at timeout. Preserve object queries; strip the wait header before forwarding. Pin polling to the admitting pod. Increase the HTTP connection budget to cover header/body timeouts, the validated wait, and response writing; retain bounded concurrency.

### Observability and implementation handoff

Add histogram `stalewise_invalidation_latency_seconds{result="delivered|exhausted"}` from admission to final target completion. Add gauge `stalewise_invalidation_peers_lagging`: distinct peer identities with any retained pending/exhausted result. Eviction may lower it; it is not proof of coherence. Log IDs and bounded failure reasons, never URLs or credentials.

Implement status first, then membership and VCL forwarding. Require three-instance fills=1, owner-failure fallback, broadcast purge/status, and authenticated wait tests before opt-in rollout. Disable sharding before binary rollback. No TUI or additional product surface is planned.

### Implementation notes

- The Varnish 9.1 shard director clamps rather than wraps: keys above the last ring point belong to the last point, keys below the first to the first. Ring fixtures assert this edge behaviour.
- Peers allowed to send hop headers are the discovered peer addresses plus `--purge-acl`; the chart's sidecar entry stays gated on the AuthorizationPolicy.
- `X-Stalewise-Peer-Tags` uses the stored `,tag,` form; `,` encodes an empty tag set so the field is always present.
- A wait returns as soon as every target is terminal: 200 when all delivered, otherwise 504 without waiting for the timeout.
- `--pod-name` is required by the signaller as well as sharding, because status reports the admitting pod.
