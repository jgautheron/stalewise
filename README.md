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
- `--nuke-limit` (default 10000, chart `nukeLimit`) sets Varnish `nuke_limit`, the number of objects one fetch may evict to make room.
  Varnish defaults to 50. With storage full of 2 KiB objects, a 9 MiB response then failed mid-stream and the client received a truncated body.
  A fetch that still runs out of evictions is cut short the same way, and Varnish counts it in `MAIN.n_lru_limited`.
- Cached responses stream to the client while Varnish fetches them, so a backend connection that closes before `Content-Length` truncates the client body.
  Varnish logs `FetchError straight insufficient bytes` and counts the fetch in `MAIN.fetch_failed`; storage limits do not cause this error.
  Varnish cannot retry such a fetch because the client has already received bytes.
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
The chart derives the memory limit from the stores and the expected object count; see [Sizing memory](#sizing-memory).

Each release publishes the chart as an OCI artifact and the image at the same version:

```sh
helm install catalog-cache oci://ghcr.io/jgautheron/charts/stalewise --version 0.6.0 \
  --set backend.service=catalog --set fullnameOverride=catalog-cache
```

The chart defaults `image.tag` to its `appVersion`, so it pulls `ghcr.io/jgautheron/stalewise:0.6.0`.
Images are signed keylessly with cosign from the release workflow.
The signature covers the exact index digest the workflow created, never whatever a tag points to later.
Verify a signature before you deploy:

```sh
cosign verify ghcr.io/jgautheron/stalewise:0.6.0 \
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

### Sidecar CPU

Every cache request crosses the Envoy sidecar, which becomes the throughput ceiling before Varnish does.
`mesh.proxy.concurrency`, `mesh.proxy.cpuRequest` and `mesh.proxy.cpuLimit` render the `concurrency` field of `proxy.istio.io/config`, `sidecar.istio.io/proxyCPU` and `sidecar.istio.io/proxyCPULimit`.
The concurrency merges into any `proxy.istio.io/config` from `podAnnotations`, and explicit `podAnnotations` win.
`values-mesh.yaml` sets 4 workers, a 500m request and a 4-core limit.

Measured on arm64 EKS with Istio STRICT mTLS, against the mesh defaults:

| Sidecar | Hit max qps | Hit p99 | p99 at 2k qps | 100 KiB qps / p99 |
|---|---|---|---|---|
| Defaults | 9.8k to 10.1k | 5.2 to 5.4 ms | 3.0 to 3.4 ms | 6.2k to 6.4k / 7.6 ms |
| `concurrency: 4`, CPU limit 2 | 9.7k | 27.4 ms | 3.0 ms | 5.9k / 38.8 ms |
| `concurrency: 4`, CPU limit 4 | 12.7k (+28%) | 4.3 ms | 2.5 ms | 8.4k (+33%) / 6.3 ms |

Four workers under the default CPU limit are throttled, and the tail latency grows instead.
The chart therefore fails when `concurrency` exceeds the `cpuLimit` cores, or when `concurrency` is set without `cpuLimit`.
The cost is CPU: each cache pod's sidecar may use up to 4 cores, so leave `mesh.proxy` empty when the node budget matters more than throughput.
Varnish thread pool tuning showed no measurable effect.

### Sizing memory

Varnish keeps per-object state (object core, object head, hash entry, allocator slack) outside the malloc stevedore size.
Many small objects can therefore exceed a memory limit that only covers `storage` and `transientStorage`.
A 64m store with 16m of Transient and a 192Mi limit was OOM-killed after 20,000 objects of 2 KiB.

The chart computes the smallest memory limit from this formula:

```text
limit = storage + transientStorage + 128Mi + 1 KiB x capacity.maxObjects
```

- `128Mi` is the base: varnishd, its 80 MiB shared log, and stalewise. An idle container measured 102 MiB on Varnish 9.1.
- `1 KiB` per object covers the 0.6 to 1.2 KiB measured outside storage for 1 B and 2 KiB bodies, including LRU churn.
- `capacity.maxObjects` (default 65536) is the number of objects held at once, which is roughly `storage / average object size`.

An unset `resources.limits.memory` is derived from the formula, and a smaller explicit limit fails rendering.
The defaults give 256m + 64m + 128Mi + 64Mi = 512Mi.

Worked example: 500m of storage filled with 2 KiB objects holds about 256,000 objects.
Set `storage: malloc,500m` and `capacity.maxObjects: 256000`.
The limit is then 500 + 64 + 128 + 250 = 942Mi.

Each evicted object leaves an object head of about 0.5 KiB, which Varnish frees two `critbit_cooloff` periods later.
The memory guard lowers `critbit_cooloff` from 180s to Varnish's minimum of 60s, so this is about 2 minutes, or 6 minutes with the guard disabled.
A cache that evicts thousands of objects per second needs more headroom: add evictions per second x 120 s (360 s without the guard) x 0.5 KiB.
Watch `MAIN.n_lru_nuked` to estimate the rate.

At startup, stalewise reads the cgroup memory limit and compares it with the stores plus the 128 MiB base.
It refuses to start when they do not fit, and it logs a warning when the remaining headroom holds fewer objects than storage filled with 4 KiB objects.

varnishd runs with `MALLOC_ARENA_MAX=4` (`--malloc-arena-max`, chart `mallocArenaMax`; 0 keeps the glibc default).
The `varnish:9.1.0` image uses glibc malloc, not jemalloc.
Filling 64m of storage with 150,000 objects of 2 KiB peaked at 316 MB with glibc's default arenas and at 240 MB with 4 arenas, at the same request rate.
Disabling transparent huge pages made no measurable difference, so stalewise leaves them alone.

### Memory guard

The sizing formula is the primary protection; the memory guard is a safety net for when the object count outgrows it.
Open-source Varnish has no memory governor: its LRU bounds only the stevedore bytes, not the per-object overhead outside them.

Every `--memguard-interval` (default 1s), stalewise reads the container working set: cgroup `memory.current` minus `inactive_file`, the value Kubernetes uses for OOM decisions and eviction.
Reclaimable page cache, such as the shared log on a disk-backed volume, therefore never triggers it.
When the working set reaches `--memguard-high` (default 0.85) of the memory limit, stalewise issues `ban obj.age > T` on the local admin port.

- The first T is half the age of the oldest possible object, capped at `--memguard-max-age` (default 24h). stalewise derives that age from the varnishd start time and its own earlier bans, so no object listing is needed.
- Each further ban halves T, down to `--memguard-min-age` (default 10s). Objects younger than the floor are never banned by the guard.
- The guard issues at most one ban every 3 seconds (or every interval, if longer), so the ban lurker has time to evict before the next step.
- A further ban needs the working set to be above high water and to have grown by 0.5% of the limit since the previous ban. glibc keeps freed memory for reuse instead of returning it, so a flat working set means the last ban is working.
- The episode ends when the working set falls below `--memguard-low` (default 0.75).

Know these semantics before relying on the guard:

- Eviction is by time in cache, not true LRU: a popular object that was stored long ago goes before an unpopular recent one. `obj.age` also counts any `Age` the origin sent.
- Memory frees gradually. The lurker evicts objects within seconds, but glibc reuses the freed memory rather than returning it, and evicted hash entries linger for about 2 minutes (`critbit_cooloff`). The working set can stay above low water after the guard has stopped growth.
- Varnish tests a ban lazily, so an object that ages past T before the lurker reaches it is banned too. With `ban_lurker_age=1`, set when the guard is enabled, that drift is about one to two seconds beyond T.
- A ban removes stale objects kept for grace or `stale-if-error`, and copies that a non-owner keeps in shard mode. Requests for them refetch through the normal path.
- The guard acts on its own pod only. It never uses the signaller and never reaches peers.
- Bans touch only stored objects, never fetches in progress, so they cannot truncate a response.
- Each guard ban uses only `obj.*` fields, so the lurker completes and removes it, and Varnish's `ban_dups` drops older identical bans. The ban list stays short.

Under sustained growth the guard steps down to the floor and keeps about the last `--memguard-min-age` of objects; the hit ratio drops instead of the pod being OOM-killed.
A Docker test fills a 224 MiB container (64m storage, 16m Transient) with 200,000 objects of 100 bytes at 2,000 per second.
With the guard, the working set peaked at about 208 MB of the 235 MB limit, with no OOM kill and every request answered 200, and the newest objects still hit.
Without it, the kernel OOM-killed varnishd after about 129,000 objects.

Failures to read memory or to ban are logged, counted in `stalewise_memguard_failures_total`, and listed as a readiness degradation reason (`memory guard failing`) without failing readiness.
Without a memory limit, the guard logs once that it is disabled.
Disable it with `--memguard=false` (chart `memguard.enabled: false`).

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
| `stalewise_memguard_bans_total`, `stalewise_memguard_failures_total`, `stalewise_memguard_active` | none |
| `stalewise_memory_used_bytes` (working set), `stalewise_memory_cgroup_usage_bytes`, `stalewise_memory_limit_bytes` | none |

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
