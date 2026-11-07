# 0001: A supervisor with one configuration writer

## Context

Discovery and invalidation must not interrupt cached delivery. Varnish already owns storage, request handling, and backend fetches.

## Decision

Use one Rust binary crate with concrete modules, not a workspace or plugin framework. Tokio owns asynchronous I/O. Varnish remains a supervised child, outside the controller's data path.

The reload task alone owns the active VCL identity and bytes. Every producer publishes replaceable snapshots. The signaller owns a bounded delivery queue. Management reads state but never changes configuration.

Readiness requires an active VCL and a successful local synthetic request. Discovery, templates, and reload errors degrade a working cache without removing it from traffic. Frontend membership never gates readiness. Empty origins retain the last good VCL unless explicit empty rendering is enabled.

### Process and filesystem boundary

Launch without a shell:

```text
varnishd -F -f <initial.vcl> -S <secret> -T <admin>
         -a <frontend> -s <storage> -n <varnish-work-dir> [-p <key=value>...]
```

Use separate writable directories for generated files and Varnish state. Reject writable-by-others output directories and symlinked output path components. Open directory descriptors before writing. Use `openat` with `O_NOFOLLOW|O_CREAT|O_EXCL`, mode 0600, unpredictable temporary names, then descriptor-relative rename. Sync the file and directory. Never resolve a destination symlink. ConfigMap input symlinks are intentionally allowed; output symlinks are not.

Generate a 32-byte random admin secret if none is supplied. Write it with the same exclusive-file rules. Supplied secrets must be readable, nonempty, and never logged. Re-read the secret for every new CLI connection. Admin addresses must be loopback in v0.1; authentication offers no transport confidentiality.

Retain stdout/stderr pipes and continuously drain them. Emit bounded, sanitized diagnostics, not VCL source or arbitrary child output. Suppress source-line excerpts and redact configured secret/environment values. Do not log tokens, bodies, complete requests, or template contents.

Manage child lifetime independently of admin availability. The supervisor owns `Child::wait`; cancellation cannot drop an unreaped child. Unexpected exit is nonzero even if the child's code is zero. Requested shutdown may exit zero after reaping. Runtime infrastructure failures exit one; invalid startup configuration exits two.

### Transaction boundary

1. Render and validate a candidate into bounded memory, then stage its file.
2. Compare exact bytes with the last confirmed active bytes.
3. Authenticate, list current VCLs, then load a unique generated name with `vcl.inline`.
4. Activate with `vcl.use` and confirm the active identity with `vcl.list`.
5. Publish active bytes atomically and update the in-memory snapshot.
6. Discard oldest owned inactive VCLs beyond the configured total, including startup VCL.

An activation timeout is ambiguous, not proof of failure. Reconnect and list before retrying. If the candidate is active, commit its bytes. If the previous VCL remains active, retry safely. Never discard either possible active identity while reconciliation is uncertain.

A failed disk publication after activation does not revert a successful activation silently. Keep the confirmed runtime state, mark persistence degraded, and retry publication. Startup always reconstructs from validated inputs; the disk file is not a durable truth source. An emptyDir cannot recover the last good VCL after pod loss.

Cleanup failures do not undo activation. Retry cleanup; never discard active or unowned configurations. Permit one temporary candidate beyond the retention target. If cleanup cannot restore the bound, stop further loads and degrade until an operator fixes the problem.

Limit template input/rendered output to 1MiB each; apply template fuel. Reject NUL and invalid UTF-8. Templates are trusted executable VCL, including any explicit include/import. This is not a tenant isolation mechanism.

### Admin error surface

The client returns typed `Io`, `Timeout`, `Authentication`, `Protocol`, `TooLarge`, or `Command { status, diagnostic }` errors. It never panics on received bytes.

Parse exactly 13 status-header bytes, a bounded decimal length, the declared body, and the final framing newline. Reject partial frames, malformed padding/status, oversize lengths, and unexpected authentication states. Treat 201/truncated output as failure, not successful compilation. Close the socket on framing errors or timeout.

Send one command at a time. Escape backslash, quote, LF, CR, and tab in quoted CLI arguments. Generated VCL names contain only identifier characters. A source string cannot introduce a second command. Parse `vcl.list` against recorded 9.1.0 fixtures; unknown layouts fail closed.

Use the [official protocol](https://www.varnish.org/docs/reference/cli_protocol/) and [command reference](https://www.varnish.org/docs/reference/varnish-cli/), never another client's implementation.

### Signaller trust boundary

Read the bearer-token file at startup; an invalid file prevents enabling the listener. Token rotation requires a restart in v0.1. Permit one trailing LF; otherwise require 32–256 visible token bytes. Compare fixed-size token digests in constant time. Reject missing, duplicated, or malformed Authorization with 401 before admission.

Enforce 16KiB request headers, 128 concurrent accepted connections, the configured body limit, and stage-specific deadlines. Header/body timeout returns 408 where a response is possible; write/idle timeout closes the connection. Do not follow redirects. Bound peer response headers/bodies and close response streams after their status is known.

Supported configured methods are a nonempty subset of PURGE/BAN. Unsupported methods return 405 with Allow. Bad URI/authority/tag syntax returns 400. Oversize Content-Length or streamed bodies return 413. Queue rejection returns 503 without partial delivery. Successful admission returns 202, meaning queued in memory, never completed everywhere.

Remove Authorization, proxy credentials, standard hop-by-hop headers, and every header named by Connection. Recompute framing. Preserve origin-form path/query and validated Host unless canonical Host overrides it. Never resolve a caller-controlled destination: dial only validated discovered IPs and the local frontend socket. IPv6 addresses use proper socket/authority formatting.

A dispatcher owns the queue receiver and at most N delivery tasks. Admission reserves all destination slots with `try_reserve_many`; a peer set exceeding capacity returns 503. Requests share body storage. Retry transient errors with 100ms–1s exponential full jitter, respecting the admission-time deadline. Other 4xx responses terminate delivery. Redirects never cause delivery elsewhere. Queue expiry counts as exhaustion even without a network attempt. No retry survives the deadline or shutdown budget.

202 is not durable: crashes can lose queued work. Retries can duplicate invalidations. Mesh sidecars can hide source IPs, so ACLs alone do not authenticate remote peers. Restrict invalidation reachability with mesh authorization/network policy; the token protects only signaller admission.

With the signaller enabled, the reloader renders each discovered peer pod address into the purge ACL as a `/32` or `/128` entry; operators never list cluster pod CIDRs. We chose this over forwarding to peer signaller ports because it reuses the existing reload path and needs no second authenticated hop or loop guard. The sidecar address `127.0.0.6` is trusted only when the chart also enforces the AuthorizationPolicy.

### Observability

Use `stalewise_` metrics with bounded role/reason/status-class/result labels:

- `reload_attempts_total`, `reload_successes_total`, `reload_failures_total`, `reload_skips_total`, and `active_vcl_age_seconds`.
- `discovery_events_total`, `discovery_errors_total`, `discovery_endpoints`, `discovery_last_success_timestamp_seconds`, and `empty_endpoints_total`.
- `signaller_requests_total`, `signaller_queue_depth`, `signaller_deliveries_total`, and `signaller_exhausted_deliveries_total`.
- `ready`, `degraded`, and `degradation_events_total`.

Never label by URL, host, token, or generated VCL name. Set discovery timestamps only after successful synchronization, not merely connection attempts. Active age starts at confirmed activation and does not reset on identical renders. Expose Prometheus-compatible text with the matching content type.

Run a separately selected Varnish exporter sidecar. Mount the Varnish VSM directory read-only at the same path, or provide equivalent local `varnishstat` access. Expose its scrape endpoint on 9131, separately from stalewise's 9090 endpoint. Match exporter permissions to the non-root Varnish process. Verify exporter support for 9.1.0 before selecting an image; no particular distribution is designated here.

The exporter must expose `MAIN.s_pass`, `MAIN.cache_hitmiss`, `MAIN.cache_hitpass`, and `MAIN.client_req`. Relabel them to stable recording-rule inputs. One deployment per service gives per-service target labels without parsing traffic in stalewise.

```promql
sum by (namespace, service) (
  rate(varnish_main_s_pass[5m]) + rate(varnish_main_cache_hitmiss[5m])
)
/
clamp_min(sum by (namespace, service) (rate(varnish_main_client_req[5m])), 0.001)
```

Record this as `stalewise:uncacheable_request_share:ratio`. `cache_hitpass` is a separate diagnostic, not an added numerator term: those requests also pass. This is an approximation. Synthetic probes, invalidations, first uncacheable fetches, and exporter counter semantics affect it. Confirm names and overlap against the selected exporter's documentation and integration fixtures. Do not label this as an exact routed-request percentage.

### Packaging boundary

Use digest-pinned stable Rust/bookworm builder and `varnish:9.1.0` runtime images. Verify matching libc, amd64/arm64 support, UID, writable VSM/compiler paths, and executable permissions in CI. Mount writable `/tmp` if the compiler requires it. No privileged ports or additional capabilities are needed.

The chart provides distinct traffic, headless discovery, and management Services, namespaced EndpointSlice get/list/watch permissions, probes, disruption budget, optional NetworkPolicy, and a values schema. The optional signaller can use a separate port on the traffic Service. Restrict its exposure independently. Pin build actions by SHA and attach SBOM, provenance, and keyless signatures.

## Consequences

Serving remains available when control-plane dependencies fail. Operators must monitor degradation separately from readiness. Stale IPs and stale DNS resolution can persist while the API or DNS is unavailable. Static DNS resolution happens when VCL loads; changed Service addresses require a successful reload. Avoid changing the origin Service address in place.

Configuration recovery is explicit, but not a distributed transaction. Purges are best-effort in-memory deliveries. Conservative limits sometimes reject work rather than risking unbounded memory.

## Rejected alternatives

- Go: a second language conflicts with the selected Rust runtime and toolchain.
- An operator/CRD in v0.1: unnecessary control-plane ownership for template-based supervision; typed policy belongs in v0.2.
- An embedded forwarding proxy: adds latency and another data-path failure mode.
- Third-party Varnish CLI crates: a narrow protocol needs explicit framing, authentication, and ambiguous-result handling.
- A workspace or generic discovery framework: there is one binary and two instances of one watcher.
- Readiness tied to origins or peers: removes useful stale caches and can deadlock bootstrap.

## Migration and rollback

Implement protocol/filesystem tests before wiring process supervision. Add discovery and transactional reload next, then bounded invalidation and management. Add packaging only after the real-Varnish tests pass.

Roll back templates by restoring prior input content. Roll back application traffic by removing cache attachment before changing the deployment. Never treat a newer on-disk candidate as confirmed active VCL.
