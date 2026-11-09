# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

0.3.0 is the first tagged release.
The 0.1.0 and 0.2.0 sections describe untagged development milestones whose changes ship in 0.3.0.

## [Unreleased]

## [0.6.0] - 2026-11-09

### Added

- A memory guard keeps a pod from being OOM-killed when the object count outgrows the sizing formula. When the container working set reaches `--memguard-high` (default 0.85) of the memory limit, stalewise bans the longest-cached objects with `ban obj.age > T` on the local admin port. It halves T down to `--memguard-min-age` (default 10s) while memory keeps growing, and stops below `--memguard-low` (default 0.75). The chart exposes `memguard.enabled` (default true), `high`, `low`, `minAge` and `interval`. A 224 MiB container filled with 200,000 objects of 100 bytes peaked at a 208 MB working set with the guard and was OOM-killed without it.
- Metrics `stalewise_memguard_bans_total`, `stalewise_memguard_failures_total`, `stalewise_memguard_active`, `stalewise_memory_used_bytes`, `stalewise_memory_cgroup_usage_bytes` and `stalewise_memory_limit_bytes`.

### Changed

- varnishd runs with `MALLOC_ARENA_MAX=4` (`--malloc-arena-max`, chart `mallocArenaMax`). Under object churn this measured 240 MB instead of 316 MB peak memory at the same throughput.
- With the memory guard enabled, varnishd runs with `ban_lurker_age=1` (Varnish default 60s) and `critbit_cooloff=60` (default 180s). `--varnish-param` still overrides both.
- Durations accept an `h` unit.

### Verified

- In an Istio STRICT-mTLS cluster, a 209Mi pod filled with 100,000 small objects answered every request with the guard (2 bans, 187 of 219 MB) and was OOM-killed with 2,548 errors without it.

## [0.5.0] - 2026-11-08

### Fixed

- Many small objects no longer exceed the memory limit unnoticed. Varnish keeps about 1 KiB per object outside the stevedore size, so a 64m store with a 192Mi limit was OOM-killed after 20,000 objects of 2 KiB. The chart adds `capacity.maxObjects` (default 65536), derives an unset memory limit from `storage + transientStorage + 128Mi + 1 KiB x maxObjects`, and fails rendering when an explicit limit is smaller. The README has a sizing section.
- stalewise reads the cgroup memory limit at startup. It refuses to start when the stores plus the 128 MiB base exceed the limit, and warns when the headroom is tight.
- A large response no longer arrives truncated when storage is full of small objects. `--nuke-limit` (`STALEWISE_NUKE_LIMIT`, default 10000, chart `nukeLimit`) replaces the Varnish default of 50, which made a 9 MiB fetch give up mid-stream.

- A Docker test fills 64m of storage with 300 objects of 1 MiB and requires every body whole, the newest object a hit and the oldest evicted. Truncated bodies seen behind an Istio sidecar were backend connections closing before `Content-Length` (`FetchError straight insufficient bytes`), not eviction. Varnish 9.1 in Docker shows no truncation at any `nuke_limit` from 50 to 100000. The README documents the failure mode.

### Changed

- **Breaking (chart):** an explicit memory limit below the computed minimum, or `mesh.proxy.concurrency` without a matching `cpuLimit`, now fails rendering.
- **Chart:** the default `resources` no longer sets a memory limit or request; both are derived (512Mi with the defaults).
- **Chart:** `mesh.proxy.concurrency`, `mesh.proxy.cpuRequest` and `mesh.proxy.cpuLimit` render the Istio sidecar annotations and merge with `podAnnotations`. Rendering fails when the concurrency exceeds the CPU limit in cores. `values-mesh.yaml` sets 4 workers, a 500m request and a 4-core limit, which measured +28% hit throughput and a lower p99 at the cost of more reserved CPU.

## [0.4.0] - 2026-11-08

### Security

- Only WebSocket handshakes are piped. Other `Upgrade` requests pass with the header removed, and `Expect` requests pass, so a declined upgrade can no longer tunnel later requests past VCL.
- `--transient-storage` (`STALEWISE_TRANSIENT_STORAGE`, default `malloc,64m`) bounds the Transient stevedore, which Varnish otherwise leaves unbounded. The chart adds `transientStorage` and a default 512Mi memory limit.
- `--transit-buffer` (`STALEWISE_TRANSIT_BUFFER`, default 1048576 bytes, chart `transitBuffer`) sets Varnish `transit_buffer`, so an uncacheable fetch waits for its client instead of filling Transient. Before, a client reading slower than the origin got a truncated body once a large pass response outgrew `--transient-storage`.
- The image workflow signs the index digest reported by its own `imagetools create`, instead of re-reading the mutable version tag, so a repointed tag can no longer obtain a trusted signature. CI runs actionlint and `scripts/workflow-check.sh`.
- CachePolicy conflict messages name at most three rivals plus a count, so many same-prefix policies no longer cause quadratic memory use. `--max-cache-policies` (default 256, chart `cachePolicies.maxPolicies`) admits the oldest policies and marks the rest Invalid with reason `TooManyPolicies`.
- `PURGE` applies the CachePolicy query allow-list and key headers, so it removes objects stored under a normalized key. Before, `PURGE /asset?tracking=x` missed an object stored as `/asset`.
- Kubelet probes use a dedicated `--probe-listen` listener (default `0.0.0.0:9091`; chart port `probe`, bound on the pod IP) with its own connection slots, so 128 idle management connections can no longer fail liveness.
- `X-Forwarded-Host`, `X-Original-URL` and `X-Rewrite-URL` are removed from requests before hashing and the origin fetch. `--strip-rewrite-headers=false` (chart `stripRewriteHeaders`) opts out.
- While a CachePolicy is active, paths with dot segments, empty segments, backslashes, or percent-encoded slash, backslash or unreserved characters pass uncached, so `/shop/./account` can no longer dodge a `/shop/account` bypass rule.

### Fixed

- With `--shard`, an owner that does not yet trust a forwarding peer no longer makes the client receive 403. Memberships briefly disagree at startup, on scale-up and during rollouts. The owner now marks hop refusals, and the forwarder retries once directly against the origin, as it already did for a peer 5xx. A 403 from the origin is still delivered unchanged.
- `kubectl rollout restart` no longer resets client connections. On SIGTERM, `/readyz` answers 503 `draining`, the signaller stops admitting, and Varnish keeps serving for `--drain-delay` (`STALEWISE_DRAIN_DELAY`, default `10s`, `0` disables) before it stops. The delay counts against `--shutdown-timeout` and must be shorter than it.
- The chart adds `drainDelay` (10) and `shutdownTimeout` (30) and renders `terminationGracePeriodSeconds` as at least `drainDelay + shutdownTimeout + 5`. `values-mesh.yaml` sets the Istio `terminationDrainDuration` so the sidecar outlives the drain.

### Changed

- **Breaking (chart):** with `networkPolicy.enabled`, an empty `trafficFrom`, `managementFrom` or `signallerFrom` now admits no source on that port instead of every source. Cache pods still reach each other's `http` port.
- `--log-level` accepts a level or comma-separated `target=level` directives; span and field filters are no longer accepted and fail startup.
- Log lines and CLI help no longer contain ANSI color codes, and mistyped flags no longer get spelling suggestions.
- The signaller answers 503 `entropy unavailable` when the OS random source fails instead of panicking.

### Tests

- HTTP cache semantics matrix against real Varnish (`tests/cache_semantics.rs`, `docs/testing.md`), payloads from 0 bytes to 200 MiB, and Kyverno Chainsaw suites in kind for install, CachePolicy status, attachment, many releases, RBAC and rollout. Mutation evidence is in `docs/mutation-evidence.md`.

### Dependencies

- Dropped `rand` (now `getrandom`) and the regex-based log filter; trimmed default features. 256 to 242 locked packages. `cargo deny` allows crates.io only and fails on new duplicate versions; Dependabot covers cargo, actions and docker.

## [0.3.0] - 2026-11-07

### Added

- `--shard` renders a `directors.shard()` ring keyed by `namespace/pod`, so a cold object is filled once per Deployment.
- Non-owners forward misses to the owner once with trusted `X-Stalewise-Shard-Hop` headers and fall back to the origin on failure.
- `--shard-local-ttl` lets non-owners keep short copies capped by the owner's remaining freshness.
- Each invalidation gets an ID, a `Location: /invalidations/<id>` status and per-target `pending`, `delivered` or `exhausted` results.
- `GET /invalidations/<id>?wait=` and the `X-Stalewise-Wait` write header wait for delivery and return 504 on timeout.
- `--invalidation-history` bounds retained invalidation records.
- `--pod-name` and `--membership-debounce` give peers a stable, debounced `namespace/pod` identity.
- Metrics `stalewise_invalidation_latency_seconds` and `stalewise_invalidation_peers_lagging`.
- Chart values for sharding and pod identity, and an AuthorizationPolicy rule that restricts hop headers to the cache's identity.
- Chart value `mesh.peerAuthentication` renders an optional STRICT mTLS PeerAuthentication for the cache pods.
- The release workflow publishes the chart to `oci://ghcr.io/jgautheron/charts/stalewise` and creates a GitHub release.

### Milestone 0.2.0

These changes were never tagged separately and first ship in 0.3.0.

- The namespaced `stalewise.dev/v1alpha1` `CachePolicy` resource with typed overrides, bypass rules and `Accepted`/`Invalid` status.
- Read-only attachment validation against Istio VirtualServices, reported in an `Attached` condition.
- The `--config` YAML file and the `stalewise crd` subcommand.
- Chart support for the CachePolicy CRD, policy RBAC, policies from dependent charts and `nameOverride`/`fullnameOverride`.

### Milestone 0.1.0

These changes were never tagged separately and first ship in 0.3.0.

- `varnishd` supervision with transactional VCL reloads that keep the last good VCL.
- EndpointSlice discovery for peers and, in the generic profile, the origin.
- Generic and mesh VCL templates that cache only what origin headers allow.
- A token-authenticated signaller that broadcasts `PURGE` and tag `BAN` to every peer.
- Management endpoints `/healthz`, `/readyz` and `/metrics`.
- A signed multi-arch container image, the Helm chart and real Varnish integration tests.

### Fixed

- Peer deliveries send `--peer-authority` as `Host`, so sidecars no longer route them to the origin.
- The purge ACL trusts discovered peer pod addresses instead of pod CIDRs.
- A dual-stack pod counts as one peer.
- A candidate that `vcl.use` rejects is no longer retried forever.
- A `--listen` address that the readiness probe cannot reach is rejected at startup.
- An early exit of any background task is fatal instead of silently leaving the pod ready.
- The chart refuses a sidecar signaller without the AuthorizationPolicy.
- `Cache-Control` stale windows shrink by `Age`.
- `stale-if-error` serves the stale object when a foreground refetch fails.
- Tag bans match stored objects.
- Task exits after a shutdown request count as orderly.
- The chart renders numeric environment values as integers.

[0.3.0]: https://github.com/jgautheron/stalewise/releases/tag/v0.3.0
