# stalewise

stalewise runs Varnish as a Kubernetes-aware HTTP cache for one application Service.
It is the container entrypoint: it renders VCL from cluster state, supervises `varnishd`, reloads configuration without restarts, and broadcasts invalidations to every replica.
stalewise never handles cache traffic itself.

The design is in [docs/design.md](docs/design.md), with decisions in [docs/adr](docs/adr).

## How it works

- Discovery watches `discovery.k8s.io/v1` EndpointSlices for the peer Service and, in the generic profile, the origin Service.
- Rendering fills a VCL template with sorted, deduplicated endpoints and validated settings.
- The reload loop compiles each candidate with `vcl.inline`, activates it with `vcl.use`, and confirms it with `vcl.list`.
  It skips byte-identical renders and keeps the last good VCL when anything fails.
- The optional signaller accepts authenticated `PURGE` and `BAN` requests and delivers them to every peer through a bounded queue.
- The management listener serves `/healthz`, `/readyz` and `/metrics`.

Serving fails open and configuration changes fail closed.
`/readyz` returns 503 only when there is no active VCL or the local synthetic probe fails.
Discovery, template and reload problems keep the pod ready and appear as `degraded:` lines in the `/readyz` body.

## Usage

Every flag has an environment variable equivalent (`--backend-host` is `STALEWISE_BACKEND_HOST`).
Command-line flags override environment variables, which override profile defaults.
Run `stalewise --help` for the complete list.

Generic profile, watching the origin's EndpointSlices:

```sh
stalewise \
  --namespace=apps \
  --frontend-service=catalog-cache-peers \
  --frontend-service-host=catalog-cache.apps.svc.cluster.local \
  --backend-service=catalog
```

Mesh profile, using one Service DNS origin that the sidecar balances:

```sh
stalewise --profile=mesh \
  --frontend-service=catalog-cache-peers \
  --frontend-service-host=catalog-cache.apps.svc.cluster.local \
  --backend-host=catalog.apps.svc.cluster.local --backend-port=8080 \
  --canonical-host=catalog.apps.svc.cluster.local
```

`--profile=mesh` turns off origin watching, selects the mesh template, binds the frontend on `0.0.0.0`, and rewrites `bereq.http.host` to the origin authority.
Startup never waits for peers.
stalewise rejects a backend host equal to the cache's own traffic Service host, because that would loop.

Exit codes are `0` after a requested shutdown, `1` for runtime failures including an unexpected `varnishd` exit, and `2` for invalid configuration, secrets or tokens.

## Caching behaviour

The embedded templates cache only what the origin's headers allow.

- `Surrogate-Control` takes precedence over `Cache-Control` for freshness, and `s-maxage` takes precedence over `max-age`.
- Responses without freshness information get a TTL of zero and are not stored.
- `stale-while-revalidate` and `stale-if-error` become grace.
  Stale objects are served within the revalidation window while the origin is healthy, and within the larger window when the origin is sick.
- Requests with `Authorization` or `Cookie`, and methods other than `GET` and `HEAD`, pass to the origin.
- `Upgrade` and `Expect` requests are piped.
- `Set-Cookie`, `private`, `no-store`, `no-cache`, `Vary: *` and `text/event-stream` responses are never stored.
- Uncacheable responses become hit-for-miss objects for `--uncacheable-ttl` (default 30s).
- Responses whose `Content-Length` exceeds `--max-object-size` (default 10 MiB) stream without being stored.
- `Surrogate-Key` and `Cache-Tag` values are indexed for tag invalidation and removed before delivery.

## Invalidation

Enable the signaller with `--signaller`, `--signaller-token-file` and `--pod-ip`.
On a dual-stack pod, pass every pod address to `--pod-ip` as a comma-separated list.
Callers send `PURGE` for one URL, or `BAN` with exactly one `X-Stalewise-Tag` header, to port 8081 with `Authorization: Bearer <token>`.
A `202 Accepted` response means the request is queued in memory, not that every peer purged it.

```sh
curl -X PURGE -H "Authorization: Bearer $TOKEN" -H 'Host: catalog.apps.svc.cluster.local' \
  http://catalog-cache.apps.svc.cluster.local:8081/products/42
curl -X BAN -H "Authorization: Bearer $TOKEN" -H 'X-Stalewise-Tag: product-42' \
  http://catalog-cache.apps.svc.cluster.local:8081/
```

Varnish accepts `PURGE` and `BAN` only from addresses in `--purge-acl`, which defaults to loopback.
The local delivery always arrives from loopback, but peer deliveries arrive from peer pod IPs.
With `--signaller`, each discovered peer pod address is added to the ACL as a single-host entry, so you never list pod CIDRs.
With an Istio-style sidecar, inbound traffic arrives from `127.0.0.6`.
That address admits every caller the sidecar forwards, so trust it only together with a mesh `AuthorizationPolicy` that restricts invalidation methods.
The Helm chart adds `127.0.0.6/32` only when it renders that `AuthorizationPolicy`.

## Helm chart

The chart in [charts/stalewise](charts/stalewise) deploys one cache per application Service.
It creates traffic, headless peer, and management Services, a namespaced Role for EndpointSlices, probes, a PodDisruptionBudget, and an optional NetworkPolicy.
Pods run as uid 1000 with a read-only root filesystem and emptyDir work volumes.

```sh
helm install catalog-cache charts/stalewise --set backend.service=catalog
helm install catalog-cache charts/stalewise -f charts/stalewise/values-mesh.yaml
```

When `mesh.sidecar` and `signaller.enabled` are both true, the chart fails unless `mesh.authorizationPolicy` is also true.

## Attaching traffic with `cache.enabled`

An application chart opts into caching through an optional `cache.enabled` block.
That block renders this chart as a dependency and forces the application's existing VirtualService to render.
The application keeps ownership of its single VirtualService; never add a second VirtualService for the same host.

Order the HTTP rules as follows:

1. Requests from the cache's own pods use the normal destinations.
2. Requests with `x-stalewise-bypass: true` use the normal destinations.
3. `GET` and `HEAD` requests use the cache Service.
4. All other requests follow the existing routing unchanged.

Use the complete reference VirtualService in [ADR 0003](docs/adr/0003-traffic-attachment.md#reference-virtualservice).
Copy the application's existing routes, retries and canary weights into rules 1, 2 and 4.
To remove caching, delete the cache match and wait for route propagation before draining the cache.

## Metrics

`/metrics` on port 9090 exposes OpenMetrics text with bounded labels only.

| Metric | Labels |
|---|---|
| `stalewise_reload_attempts_total`, `stalewise_reload_successes_total`, `stalewise_reload_skips_total` | none |
| `stalewise_reload_failures_total` | `reason` |
| `stalewise_active_vcl_age_seconds` | none |
| `stalewise_discovery_events_total`, `stalewise_discovery_errors_total`, `stalewise_empty_endpoints_total` | `role` |
| `stalewise_discovery_endpoints`, `stalewise_discovery_last_success_timestamp_seconds` | `role` |
| `stalewise_signaller_requests_total` | `status_class` |
| `stalewise_signaller_queue_depth`, `stalewise_signaller_exhausted_deliveries_total` | none |
| `stalewise_signaller_deliveries_total` | `result` |
| `stalewise_ready`, `stalewise_degraded` | none |
| `stalewise_degradation_events_total` | `reason` |

stalewise does not reimplement Varnish counters.
Run a Prometheus Varnish exporter as a sidecar through `extraContainers`.
Mount the chart's `varnish` volume read-only at `/var/lib/varnish`, run the exporter as uid 1000, and expose it on port 9131.
Verify that the exporter you choose supports Varnish 9.1 before relying on it.
This recording rule approximates the share of uncacheable traffic per service:

```promql
sum by (namespace, service) (
  rate(varnish_main_s_pass[5m]) + rate(varnish_main_cache_hitmiss[5m])
)
/
clamp_min(sum by (namespace, service) (rate(varnish_main_client_req[5m])), 0.001)
```

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo deny check
```

The integration tests run the real `varnish:9.1.0` image in Docker and are ignored by default:

```sh
docker build -t stalewise:it .
STALEWISE_IMAGE=stalewise:it cargo test --test varnish -- --ignored
```

## License

MIT
