# stalewise

stalewise runs Varnish as a Kubernetes-aware HTTP cache for one application Service.
It is the container entrypoint: it renders VCL from cluster state, supervises `varnishd`, reloads configuration without restarts, and broadcasts invalidations to every replica.
stalewise never handles cache traffic itself.

The design is in [docs/design.md](docs/design.md), with decisions in [docs/adr](docs/adr).

## How it works

- Discovery watches `discovery.k8s.io/v1` EndpointSlices for the peer Service and, in the generic profile, the origin Service.
  Peers are identified by `namespace/pod`, sorted by name, and debounced by `--membership-debounce`.
- Rendering fills a VCL template with sorted, deduplicated endpoints and validated settings.
- The reload loop compiles each candidate with `vcl.inline`, activates it with `vcl.use`, and confirms it with `vcl.list`.
  It skips byte-identical renders and keeps the last good VCL when anything fails.
- The optional signaller accepts authenticated `PURGE` and `BAN` requests and delivers them to every peer through a bounded queue.
- The management listener serves `/healthz`, `/readyz` and `/metrics`.
  A separate probe listener (`--probe-listen`, default `0.0.0.0:9091`) serves only `/healthz` and `/readyz` for the kubelet.
  It has its own connection slots, so clients holding management connections open cannot fail liveness.

Serving fails open and configuration changes fail closed.
`/readyz` returns 503 only when there is no active VCL or the local synthetic probe fails.
Discovery, template and reload problems keep the pod ready and appear as `degraded:` lines in the `/readyz` body.

## Usage

Every flag has an environment variable equivalent (`--backend-host` is `STALEWISE_BACKEND_HOST`).
Run `stalewise --help` for the complete list.

`--config` reads the same settings from a YAML file, with each flag name in camelCase.
Precedence is command line, then environment, then the config file, then profile defaults.
Unknown fields, nested values and a wrong `apiVersion` or `kind` are startup errors.

```yaml
apiVersion: stalewise.dev/v1alpha1
kind: Config
namespace: apps
frontendService: catalog-cache-peers
frontendServiceHost: catalog-cache.apps.svc.cluster.local
backendService: catalog
purgeAcl: [10.0.0.0/8]
cachePolicies: true
cacheSourceLabels: [app.kubernetes.io/name=stalewise, app.kubernetes.io/instance=catalog-cache]
```

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

On SIGTERM, `/readyz` answers 503 `draining` at once, the signaller stops admitting invalidations, and Varnish keeps serving for `--drain-delay` (default `10s`, `0` disables).
Only then is `varnishd` stopped, so clients are not reset while EndpointSlices and proxies still route to the pod.
The drain counts against `--shutdown-timeout` (default `30s`), and `--drain-delay` must be shorter than that timeout.
The chart sets `terminationGracePeriodSeconds` to at least `drainDelay + shutdownTimeout + 5`.
The chart uses no preStop hook, because the in-process delay already covers the drain window and is the only timing source.

Exit codes are `0` after a requested shutdown, `1` for runtime failures including an unexpected `varnishd` exit, and `2` for invalid configuration, secrets or tokens.

## Caching behaviour

The embedded templates cache only what the origin's headers allow.

- `Surrogate-Control` takes precedence over `Cache-Control` for freshness, and `s-maxage` takes precedence over `max-age`.
- Responses without freshness information get a TTL of zero and are not stored.
- `stale-while-revalidate` and `stale-if-error` become grace.
  Stale objects are served within the revalidation window while the origin is healthy, and within the larger window when the origin is sick.
- `X-Forwarded-Host`, `X-Original-URL` and `X-Rewrite-URL` are removed from every request before hashing and the origin fetch, because an origin that honours them could serve one URL's content under another URL's key.
  Set `--strip-rewrite-headers=false` (chart `stripRewriteHeaders: false`) only when the origin needs them and every caller is trusted.
- Requests with `Authorization` or `Cookie`, and methods other than `GET` and `HEAD`, pass to the origin.
- Only a WebSocket handshake (`GET` with `Upgrade: websocket`, `Connection: upgrade`, `Sec-WebSocket-Key` and `Sec-WebSocket-Version`) is piped.
  A WebSocket tunnel is unchecked by design: after the handshake, Varnish copies bytes in both directions and no VCL rule applies to them.
- Other `Upgrade` requests pass to the origin with the `Upgrade` header removed, so the connection stays plain HTTP/1.1.
- `Expect: 100-continue` requests pass to the origin.
- `Set-Cookie`, `private`, `no-store`, `no-cache`, `Vary: *` and `text/event-stream` responses are never stored.
- Uncacheable and streamed pass responses use the Transient store, bounded by `--transient-storage` (default `malloc,64m`).
  `--transit-buffer` (default 1048576 bytes) limits how far such a fetch reads ahead of its client, so a slow client still receives the whole body.
  Each slow stream holds a few times this value in Transient; size `transientStorage` for your concurrent large uncacheable streams, because a full store cuts responses short.
- Uncacheable responses become hit-for-miss objects for `--uncacheable-ttl` (default 30s).
- Responses whose `Content-Length` exceeds `--max-object-size` (default 10 MiB) stream without being stored.
- `Surrogate-Key` and `Cache-Tag` values are indexed for tag invalidation and removed before delivery.

## Cache policies

With `--cache-policies`, stalewise watches `CachePolicy` resources (`stalewise.dev/v1alpha1`) in its namespace.
It applies only policies whose `serviceRef` names the cached Service, which defaults to the origin Service and can be set with `--policy-service`.
Fields you set override the matching header-derived setting; fields you omit keep header behaviour.
An explicit `ttlSeconds: 0` means "do not store", which differs from leaving the field out.
Privacy rules still win: `Set-Cookie`, `private`, `no-store`, `no-cache` and `Authorization` are never cacheable.

```yaml
apiVersion: stalewise.dev/v1alpha1
kind: CachePolicy
metadata:
  name: catalog-api
  namespace: apps
spec:
  serviceRef: {name: catalog, port: http}
  pathPrefix: /api
  overrides:
    ttlSeconds: 60
    graceSeconds: 30
    cacheableStatusCodes: [200, 404]
    key:
      headers: [X-Application-Version]
      queryAllowList: [page, locale]
    cookies: {mode: stripListed, names: [analytics]}
  bypass:
    - pathPrefix: /api/account
    - header: {name: X-Preview, exact: "true"}
  attach:
    methods: [GET, HEAD]
    pathPrefixes: [/]
```

- Prefixes match on segment boundaries, so `/api` matches `/api/x` but not `/apix`.
- The longest matching prefix wins, and only that policy applies.
- Two policies with the same prefix are both rejected with reason `Conflict`; the message names at most three rivals plus a count.
- Prefixes and bypass paths match the raw request path.
  When any CachePolicy is active, a path containing a dot segment (`/./`, `/../`), an empty segment (`//`), a backslash, or a percent-encoded slash, backslash or unreserved character (such as `%2F` or `%61`) passes to the origin uncached.
  stalewise does not rewrite such paths, because the origin must receive the URL the client sent, and different origins resolve them differently.
- At most `--max-cache-policies` (default 256) policies per cache are considered, oldest first by creation time, then by name.
  The rest are rejected with reason `TooManyPolicies` without being validated.
- Removing a query name or cookie from the key can mix personalised responses, so list only names that never change the body per user.
- A rejected update keeps the last accepted generation of that policy in force.
  The status shows the rejected `observedGeneration` and the enforced `activeGeneration`.

The controller writes `Accepted` and `Invalid` conditions through the status subresource.
`Accepted=True` appears only after a VCL that contains this generation becomes active.
An invalid policy never replaces the active VCL.
`--cache-policies` cannot be combined with `--template`: a raw template owns the whole policy.

With `attach` set and `--validate-attachment` on (the default with `--cache-policies`), stalewise reads the application's VirtualServices and reports an `Attached` condition.
Reasons include `Attached`, `NoVirtualService`, `MultipleVirtualServices`, `MissingSourceBypass`, `MissingHeaderBypass`, `MissingCacheRoute`, `UnrestrictedCacheRoute` and `Loop`, plus `VirtualServicesUnavailable` with status `Unknown` while VirtualServices cannot be listed.
stalewise never creates or changes a VirtualService, and a static check cannot prove that the data plane applied a route.
Pass the cache pods' labels with `--cache-source-labels` so the check can match the source bypass rule.

## Invalidation

Enable the signaller with `--signaller`, `--signaller-token-file`, `--pod-ip` and `--pod-name`.
On a dual-stack pod, pass every pod address to `--pod-ip` as a comma-separated list.
Callers send `PURGE` for one URL, or `BAN` with exactly one `X-Stalewise-Tag` header, to port 8081 with `Authorization: Bearer <token>`.
`PURGE` computes the same cache key as a read: a CachePolicy's query allow-list and key headers apply, so send the URL and key headers a client would send.
Bypass and method rules never apply to `PURGE`.
A `202 Accepted` response means the request is queued in memory, not that every peer purged it.

Each accepted write returns a JSON status, `Location: /invalidations/<id>` and `X-Stalewise-Pod`.
The status lists every target frozen at admission as `pending`, `delivered` or `exhausted`, with a reason of `rejected`, `deadline` or `shutdown`.
Poll `GET /invalidations/<id>` with the same bearer token, optionally with `?wait=10s`, on the pod that admitted the write.
Another pod, an evicted record or a restarted pod answers 404.
Through a balanced Service, send `X-Stalewise-Wait: 10s` on the write instead.
A waiting request returns 200 when every target acknowledged and 504 with the current status otherwise.
Waits are capped at 30s, and a timeout never cancels delivery.
`--invalidation-history` bounds the retained records; a write that cannot fit beside pending records gets 503.
Delivery is an acknowledgment, not a global freshness barrier.

Call the signaller Service by its own name and do not override `Host`.
Behind a mesh, a `Host` set to the application host makes the caller's sidecar route the request to the application, not to the signaller.
Set `--canonical-host` so that stalewise applies the application host on the server side.

```sh
curl -X PURGE -H "Authorization: Bearer $TOKEN" \
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

Peer deliveries dial each peer pod address and send `--peer-authority` as `Host` when it is set.
The chart sets it to the headless peers Service, for example `catalog-cache-stalewise-peers.apps.svc.cluster.local:8080`.
Every delivery carries the invalidation host in `X-Stalewise-Host`.
Varnish uses that header as the purge or ban key host only when the client is in the purge ACL, and it removes the header from every other request.

## Sharding

`--shard` routes each object to one owner pod, so a cold object is filled once for the whole Deployment.
It needs `--pod-name`, `--pod-ip` and `--peer-authority`.
The generated VCL feeds every peer, identified by `namespace/pod`, to a `directors.shard()` ring with 67 points per peer.
The ring key is the normalised authority plus the URL, or the policy-normalised URL when a CachePolicy sets one.
Ownership ignores health, so replicas with the same membership always agree.

A non-owner forwards a miss to the owner's pod address with `Host: <peer-authority>`, `X-Stalewise-Host: <authority>` and `X-Stalewise-Shard-Hop: 1`.
The owner restores the authority, serves the request itself, and never forwards it again.
Only discovered peer addresses and `--purge-acl` entries may send these headers; others get 403, and malformed or duplicated fields get 400.
If the owner fails before the response starts, the request is retried once against the origin, and `--peer-timeout` bounds each forward.
Membership changes wait for `--membership-debounce`, and an empty membership fetches from the origin directly.

Non-owners store nothing by default.
`--shard-local-ttl` (at most 30s) lets them keep short copies, capped by the owner's remaining freshness and without grace.
Purges and bans still broadcast to every peer, so they also reach these copies.
Custom templates must handle `X-Stalewise-Shard-Hop`; rendering fails when sharding is on and a template ignores it.
Upgrade every replica before enabling sharding, and disable it before rolling back the binary.

## Helm chart

The chart in [charts/stalewise](charts/stalewise) deploys one cache per application Service.
It creates traffic, headless peer, and management Services, a namespaced Role for EndpointSlices, probes, a PodDisruptionBudget, and an optional NetworkPolicy.
Probes target the `probe` port (9091), which the chart binds on the pod IP and exposes through no Service.

With `networkPolicy.enabled`, each source list admits only the peers it names, and an empty list admits nobody on that port.
Cache pods can always reach each other's `http` port.
The probe port has no rule, because kubelet traffic comes from the node.
Earlier charts treated an empty list as "allow every source"; set `trafficFrom`, `managementFrom` and `signallerFrom` explicitly when you upgrade.
Pods run as uid 1000 with a read-only root filesystem and emptyDir work volumes.
The default memory limit is 512Mi: `storage` (256m) plus `transientStorage` (64m) plus process overhead.
Raise `resources.limits.memory` together with either store.

Each release publishes the chart as an OCI artifact and the image at the same version:

```sh
helm install catalog-cache oci://ghcr.io/jgautheron/charts/stalewise --version 0.3.0 \
  --set backend.service=catalog --set fullnameOverride=catalog-cache
```

The chart defaults `image.tag` to its `appVersion`, so it pulls `ghcr.io/jgautheron/stalewise:0.3.0`.
Images are signed keylessly with cosign from the release workflow.
The signature covers the exact index digest the workflow created, never whatever a tag points to later.
Verify a signature before you deploy:

```sh
cosign verify ghcr.io/jgautheron/stalewise:0.3.0 \
  --certificate-identity-regexp '^https://github.com/jgautheron/stalewise/.github/workflows/image.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

From a source checkout:

```sh
helm install catalog-cache charts/stalewise --set backend.service=catalog --set fullnameOverride=catalog-cache
helm install catalog-cache charts/stalewise -f charts/stalewise/values-mesh.yaml
```

Resource names default to `<release>-stalewise`, so release `catalog-cache` creates the Service `catalog-cache-stalewise`.
Set `fullnameOverride: catalog-cache` (as `values-mesh.yaml` does) to get the Service host `catalog-cache.apps.svc.cluster.local` that the reference VirtualService routes to.

The chart installs the `CachePolicy` CRD from `crds/`.
Set `cachePolicies.enabled=true` to enable the controller and its RBAC: read access to CachePolicies, `update` and `patch` on their status, and read access to VirtualServices while `cachePolicies.validateAttachment` is true.
An application chart that depends on this chart ships its policies through `cachePolicies.policies`:

```yaml
stalewise:
  fullnameOverride: catalog-cache
  cachePolicies:
    enabled: true
    policies:
      - name: catalog-api
        spec:
          serviceRef: {name: catalog}
          pathPrefix: /api
          overrides: {ttlSeconds: 60}
          attach: {mode: route}
```

`scripts/chart-check.sh` renders the documented release and validates it with kubeconform.
Regenerate the CRD after changing the Rust types with `cargo run -- crd > charts/stalewise/crds/cachepolicies.stalewise.dev.yaml`; a unit test fails while the committed file is stale.

When `mesh.sidecar` is true with `signaller.enabled` or `shard.enabled`, the chart fails unless `mesh.authorizationPolicy` is also true.
With `shard.enabled`, that policy also denies `x-stalewise-shard-hop` requests from any other identity.
Set `mesh.peerAuthentication=true` to render a `security.istio.io/v1` PeerAuthentication that requires STRICT mTLS for the cache pods.
Without it, make sure the mesh already enforces STRICT mTLS, because the AuthorizationPolicy relies on peer identities.

The sidecar must keep forwarding until stalewise has drained.
Set `terminationDrainDuration` above `drainDelay`, as `values-mesh.yaml` does:

```yaml
podAnnotations:
  proxy.istio.io/config: '{"holdApplicationUntilProxyStarts": true, "terminationDrainDuration": "15s"}'
```

Alternatively, set the proxy environment variable `EXIT_ON_ZERO_ACTIVE_CONNECTIONS=true` so the sidecar exits only after its connections close.

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
It assumes release `catalog-cache` installed with `fullnameOverride: catalog-cache`.
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
| `stalewise_invalidation_latency_seconds` (histogram, admission to last target) | `result` (`delivered`, `exhausted`) |
| `stalewise_invalidation_peers_lagging` | none |
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
scripts/chart-check.sh
```

The integration tests run the real `varnish:9.1.0` image in Docker and are ignored by default:

```sh
docker build -t stalewise:it .
STALEWISE_IMAGE=stalewise:it cargo test --test varnish -- --ignored
```

## License

MIT
