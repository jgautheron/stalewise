# 0003: Application-owned mesh attachment

## Context

Most application charts own at most one VirtualService per host. Some render none until mesh features are enabled. Competing VirtualServices for the same sidecar host do not reliably merge and conflict with declarative deployment ownership.

Cache attachment must preserve existing destinations, retries, and rollout behavior without changing callers.

## Decision

v0.1 uses one stalewise Deployment per cached service. An application chart renders it through an optional `cache.enabled` block. The chart derives namespaces, Service identities, origin authority, and canonical Host. No additional cache policy is required.

The application chart owns its VirtualService. Enabling caching forces that resource to render even when other mesh features are disabled. Integrate cache rules into that resource; never introduce a second VirtualService for the same host.

Order the HTTP rules as follows:

1. Requests from this cache's source labels use the application's normal destinations.
2. Requests with `x-stalewise-bypass: true` use those same normal destinations.
3. GET/HEAD requests, optionally restricted by path prefixes, use the cache Service.
4. Remaining requests follow the application's existing routing unchanged.

No configured methods/pathPrefixes means all GET/HEAD. Headers determine caching; callers never opt responses into storage. The bypass header only opts out. Applications may restrict who can send it if origin-load protection requires that.

### Reference VirtualService

This complete example assumes namespace `apps`, application Service `catalog`, and cache Service `catalog-cache`, all using port 8080. Cache pods carry the two matching labels. Replace the normal route and retry blocks with the application's existing settings in all three normal-destination rules.

```yaml
apiVersion: networking.istio.io/v1
kind: VirtualService
metadata:
  name: catalog
  namespace: apps
spec:
  hosts:
    - catalog.apps.svc.cluster.local
  gateways:
    - mesh
  http:
    - name: origin-from-cache
      match:
        - sourceNamespace: apps
          sourceLabels:
            app.kubernetes.io/name: stalewise
            app.kubernetes.io/instance: catalog-cache
      retries:
        attempts: 2
        perTryTimeout: 2s
        retryOn: connect-failure,reset,5xx
      route:
        - destination:
            host: catalog.apps.svc.cluster.local
            port:
              number: 8080
          weight: 100
    - name: explicit-cache-bypass
      match:
        - headers:
            x-stalewise-bypass:
              exact: "true"
      retries:
        attempts: 2
        perTryTimeout: 2s
        retryOn: connect-failure,reset,5xx
      route:
        - destination:
            host: catalog.apps.svc.cluster.local
            port:
              number: 8080
          weight: 100
    - name: cache-read-methods
      match:
        - method:
            exact: GET
        - method:
            exact: HEAD
      route:
        - destination:
            host: catalog-cache.apps.svc.cluster.local
            port:
              number: 8080
          weight: 100
    - name: existing-application-route
      retries:
        attempts: 2
        perTryTimeout: 2s
        retryOn: connect-failure,reset,5xx
      route:
        - destination:
            host: catalog.apps.svc.cluster.local
            port:
              number: 8080
          weight: 100
```

The cache's backend and canonical authority are `catalog.apps.svc.cluster.local`; its frontend Service host is `catalog-cache.apps.svc.cluster.local`. These identities must remain distinct. Set the origin authority header during backend fetches.

For restricted paths, add `uri: {prefix: /public/}` to each GET/HEAD match. Render the method/path cross-product for multiple prefixes; separate match objects are OR alternatives. Ensure `/public` itself has an explicit exact match if required. Do not accidentally add an unrestricted method-only match alongside restricted matches.

The example has one normal destination. Existing canary applications retain their complete destination list and weights in rules one, two, and four. If normal routing includes path/header branches, expand the source and bypass branches over those same rules. Preserve rewrite, timeout, retry, and other existing semantics; do not collapse them into one destination. Existing authentication and authorization policy remains in force.

### Loop prevention and ownership

`sourceLabels` selects sidecar routing configuration; it is not an HTTP header or a security identity. Requests from cache pods skip cache attachment before any method match. This preserves normal routing, including canary weights, for misses.

The source-label approach requires sidecar evaluation. Do not assume equivalent behavior for ambient mode, waypoints, gateways, or other data planes. Use an application-owned separate direct Service as the fallback. Point cache origin fetches and explicit bypasses at that Service, which must not itself route back through the cache. Reject a direct Service equal to either the routed Service or cache Service.

For source-label mode, the origin Service intentionally equals the application's routed host. That is valid only when the earlier source bypass is present. Independently, the origin must never equal the cache's traffic Service. Validation must distinguish these identities rather than rejecting every routed origin.

v0.2 `CachePolicy.attach` requests validation only. stalewise reads existing VirtualServices, checks source/direct bypass, header bypass, method/path coverage, rule order, and destination loops, and writes `Attached` status. It never creates or modifies VirtualServices or direct Services. Application charts retain ownership of both. Limit namespaced RBAC to reads plus CachePolicy status updates.

Static validation cannot prove that a data plane has applied a route. Report missing or ambiguous rules as not attached; require a live smoke test before relying on loop protection. Do not silently create a competing route.

### Ingress, canaries, and topology

An ingress Gateway API HTTPRoute routes to Services directly. The mesh VirtualService does not automatically cache that traffic. External caching is a separate opt-in: point the HTTPRoute backendRef at the cache Service and configure the accepted canonical authority. Keep write routes on the origin.

Hits are shared across backend versions and ignore canary weights because no backend is contacted. For canary testing, bypass the cache. A version header can become a key addition in v0.2, provided routing uses the same trusted discriminator. Random canary weights alone cannot provide a version-consistent cache key. Raw templates can express this earlier, but the default does not.

A later shared tier may select several origins by Host/route with tenant-isolated keys and purge scope. v0.1 remains one deployment per service for ownership, blast-radius isolation, and interpretable hit ratios.

Without mesh routing, callers explicitly target the cache Service DNS name. Preserve the application's canonical Host and configure a distinct direct origin. There is no claim of transparent attachment in that mode.

### Cost and alternatives

Observed workload results were approximately 3–5ms p50 extra keepalive latency, 99.96% hits, and one origin fetch per concurrent cold key. Those measurements demonstrate feasibility, not performance guarantees. A reported miss cost matching direct calls does not remove the architectural extra hop.

Every routed GET/HEAD pays the hop, including pass and hit-for-miss traffic. Use the approximate per-service uncacheable share before narrowing paths. Hits remove origin work; cache replicas still have independent storage and do not coalesce across pods.

A Varnish sidecar inside each backend pod is an alternative for single-replica or latency-critical services. It avoids a separate cache-tier hop, but fragments storage and coalescing across replicas. The application chart must route traffic to the local Varnish port and make its origin loopback distinct. This is a documented deployment alternative, not automatic injection.

## Rejected alternatives

- A second VirtualService: host ownership conflicts with existing routing and deployment reconciliation.
- Controller-written attachment routes: wrong owner and unsafe interaction with existing canaries/retries.
- Client-side caches: fragmented storage and invalidation across unrelated client deployments.
- An HTTP cache filter in the mesh proxy: required internal-listener plumbing is not modeled by the routing API; naive attachment returned 503.
- A shared multi-service tier in v0.1: introduces tenant routing and key-isolation decisions before they are needed.

## Migration and rollback

1. Deploy the cache and confirm local readiness before changing application routes.
2. Render source bypass, explicit bypass, cache matching, and unchanged normal routes atomically in the existing VirtualService.
3. Smoke-test normal requests, explicit bypass, misses, writes, canary behavior, and loop prevention.
4. Opt ingress into caching separately if needed.

Remove cache matching or restore the previous VirtualService to return traffic directly to the origin. This requires no client changes. Restoration follows mesh configuration propagation; existing connections and in-flight requests may finish on their prior path. Drain the cache only after that propagation. Keep the origin Service and normal routes available throughout rollback.
