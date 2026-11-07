# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

0.3.0 is the first tagged release.
The 0.1.0 and 0.2.0 sections describe untagged development milestones whose changes ship in 0.3.0.

## [Unreleased]

### Security

- Only WebSocket handshakes are piped. Other `Upgrade` requests pass with the header removed, and `Expect` requests pass, so a declined upgrade can no longer tunnel later requests past VCL.
- `--transient-storage` (`STALEWISE_TRANSIENT_STORAGE`, default `malloc,64m`) bounds the Transient stevedore, which Varnish otherwise leaves unbounded. The chart adds `transientStorage` and a default 512Mi memory limit.
- The image workflow signs the index digest reported by its own `imagetools create`, instead of re-reading the mutable version tag, so a repointed tag can no longer obtain a trusted signature. CI runs actionlint and `scripts/workflow-check.sh`.
- CachePolicy conflict messages name at most three rivals plus a count, so many same-prefix policies no longer cause quadratic memory use. `--max-cache-policies` (default 256, chart `cachePolicies.maxPolicies`) admits the oldest policies and marks the rest Invalid with reason `TooManyPolicies`.
- `PURGE` applies the CachePolicy query allow-list and key headers, so it removes objects stored under a normalized key. Before, `PURGE /asset?tracking=x` missed an object stored as `/asset`.
- Kubelet probes use a dedicated `--probe-listen` listener (default `0.0.0.0:9091`; chart port `probe`, bound on the pod IP) with its own connection slots, so 128 idle management connections can no longer fail liveness.
- `X-Forwarded-Host`, `X-Original-URL` and `X-Rewrite-URL` are removed from requests before hashing and the origin fetch. `--strip-rewrite-headers=false` (chart `stripRewriteHeaders`) opts out.

### Changed

- **Breaking (chart):** with `networkPolicy.enabled`, an empty `trafficFrom`, `managementFrom` or `signallerFrom` now admits no source on that port instead of every source. Cache pods still reach each other's `http` port.
- `--log-level` accepts a level or comma-separated `target=level` directives; span and field filters are no longer accepted and fail startup.
- Log lines and CLI help no longer contain ANSI color codes, and mistyped flags no longer get spelling suggestions.
- The signaller answers 503 `entropy unavailable` when the OS random source fails instead of panicking.

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
