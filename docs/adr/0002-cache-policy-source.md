# 0002: Headers first, application-owned policy later

## Context

Application teams own response semantics. Enabling a cache must not require a second description of every cacheable route.

## Decision

v0.1 is headers-driven. Any application chart can enable caching without specifying methods or paths. The default route includes every GET/HEAD; responses determine storage. Path restrictions avoid unnecessary hops, not establish cacheability.

v0.2 adds namespaced `CachePolicy` objects in `stalewise.dev/v1alpha1`. Any application chart can include a CachePolicy template. Fields override corresponding header-derived settings; absent fields preserve header semantics. Explicit zero and absence are different values.

Raw templates remain a trusted expert mode. A deployment selects either generated policy or raw template ownership, never an implicit merge.

### v0.1 request semantics

1. Validate and normalize Host before hashing or invalidation. The configured canonical authority replaces aliases consistently.
2. Handle PURGE/BAN only after ACL validation and backend selection.
3. Pipe Upgrade/WebSocket and Expect requests. Set `Connection: close` in pipe mode to prevent later uninspected requests sharing that connection.
4. Pass requests with Authorization or Cookie, and methods other than GET/HEAD.
5. Preserve Varnish's native Range, If-Range, conditional request, and HEAD handling.

Do not strip conditional headers globally or cache partial responses as complete objects. Default hash inputs are normalized authority and URL. Remove mesh request IDs from hash inputs; do not add tracing headers. Vary remains enforced and `Vary: *` is uncacheable.

### v0.1 response semantics

Privacy constraints dominate all freshness configuration: Set-Cookie, private, no-store, and no-cache responses never store. Authorization/Cookie passes cannot become cacheable during response handling. This conservative interpretation treats no-cache as bypass rather than implementing mandatory revalidation storage.

Freshness comes from Surrogate-Control when present, otherwise Cache-Control. Prefer s-maxage over max-age. For Cache-Control, retain Varnish's native Age/Date/Expires calculation when valid. No freshness information means TTL zero, not the Varnish default. A present Surrogate-Control without valid freshness also means zero; do not silently reuse Cache-Control freshness.

Preserve the original client-facing Cache-Control. Remove Surrogate-Control and internal metadata before delivery. Validate directives case-insensitively, using complete comma-delimited names. Reject duplicate/conflicting or malformed numeric directives conservatively. Do not match text inside unrelated extension values. Unknown extensions do not enable caching.

Explicitly set default TTL, grace, and keep to zero in generated VCL and process parameters. Header freshness may raise TTL. Keep defaults to zero until policy overrides it. Use native cacheable status handling; unexpected statuses cannot gain storage merely by carrying max-age.

Set `beresp.do_stream = true`. Mark uncacheable responses with `beresp.uncacheable = true` and the configurable 30s hit-for-miss TTL, then deliver. The TTL records a cacheability decision, not permission to cache the body. Later concurrent requests avoid the waiting list; requests waiting before the first response may already have coalesced.

Treat `text/event-stream` as uncacheable regardless of freshness. Other identified long-lived stream types need an explicit bypass or trusted template rule. Chunked framing alone does not prove streaming semantics. Validly opted-in compressed/chunked JSON remains cacheable.

Compare valid Content-Length against the default 10MiB limit before storing. Oversized responses stream without storage. Unknown-length responses cannot be bounded before storage without buffering. They may fill the configured storage if explicitly cacheable. Storage capacity and later policy bypass/size overrides are the safeguards; do not claim a hard per-object cap for chunked bodies.

### Stale windows and VCL parsing

The [official variables reference](https://www.varnish.org/docs/reference/vcl-var/) already maps Cache-Control stale-while-revalidate into `beresp.grace`. It does not provide the complete surrogate precedence and health-gated stale-if-error policy required here.

Use `std.collect`, anchored VCL regular expressions, `regsub`, and `std.duration(value + "s", 0s)`. Accept nonnegative integer seconds, capped at 604800. Reject malformed/duplicate known directives instead of extracting a convenient substring. `std.duration` alone is not a directive parser.

The following is the extraction contract after token validation. It is a fragment, not a complete default template:

```vcl
import std;

sub stalewise_stale_windows {
    set beresp.http.X-Stalewise-Swr = "0";
    set beresp.http.X-Stalewise-Sie = "0";
    if (beresp.http.X-Stalewise-Control ~ "(?i)(^|,) *stale-while-revalidate *= *[0-9]+ *(,|$)") {
        set beresp.http.X-Stalewise-Swr = regsub(
            beresp.http.X-Stalewise-Control,
            "(?i)^.*(?:^|,) *stale-while-revalidate *= *([0-9]+) *(?:,.*)?$", "\1");
    }
    if (beresp.http.X-Stalewise-Control ~ "(?i)(^|,) *stale-if-error *= *[0-9]+ *(,|$)") {
        set beresp.http.X-Stalewise-Sie = regsub(
            beresp.http.X-Stalewise-Control,
            "(?i)^.*(?:^|,) *stale-if-error *= *([0-9]+) *(?:,.*)?$", "\1");
    }
    set beresp.grace = std.duration(beresp.http.X-Stalewise-Swr + "s", 0s);
    if (std.duration(beresp.http.X-Stalewise-Sie + "s", 0s) > beresp.grace) {
        set beresp.grace = std.duration(beresp.http.X-Stalewise-Sie + "s", 0s);
    }
    unset beresp.http.X-Stalewise-Control;
}
```

Select the effective control header before calling this subroutine. Preserve the two duration metadata fields on stored objects and strip them from responses. Clear caller-supplied internal headers at ingress and origin-supplied internal headers before assigning them.

For surrogate freshness, calculate nonnegative TTL from the selected directive minus valid Age. Clamp stale windows conservatively when Age already exceeds freshness; never restart an already-expired allowance at insertion. Negative, overflowing, or invalid Age values do not enable caching.

In `vcl_hit`, fresh objects deliver. Stale objects deliver within the stored SWR window while the selected backend is healthy. When sick, permit the maximum of SWR and SIE. Compare against `obj.ttl`, not object creation time alone. Outside that window, restart once with `req.hash_always_miss = true`; never invent a `return(miss)` action for `vcl_hit`. A background 5xx abandons its replacement so the existing stale object survives.

Health means `std.healthy(req.backend_hint)`, backed by probes. Generated probes use the configured path, origin authority, a 1s timeout, 5s interval, window three, and threshold two. The path must return 200. A static mesh origin needs the same authority-aware probe. Without useful probes, connection failures do not automatically make a backend sick. This is not an instant fallback for the first failed fetch or every transient 5xx.

See the official [grace guide](https://www.varnish.org/docs/users-guide/vcl-grace/), [std conversion reference](https://www.varnish.org/docs/reference/vmod_std/), and [VCL steps](https://www.varnish.org/docs/reference/vcl-step/).

### Tag invalidation

Store the union of Surrogate-Key and Cache-Tag response tokens in internal object metadata. Surrogate-Key uses whitespace; Cache-Tag accepts commas or whitespace. Limit metadata to 4096 bytes and tokens to `[A-Za-z0-9_-]{1,128}`. Invalid metadata disables tag indexing, not privacy safeguards. Preserve normalized authority alongside tags.

A BAN request supplies exactly one `X-Stalewise-Tag` token. The signaller validates it before queue admission; each default VCL validates it again at its separate public boundary. BAN without a valid tag returns 400. Bodies are never interpreted as executable ban expressions.

Construct a fixed host-equality plus token-boundary tag expression with `std.ban`. PURGE continues to invalidate authority+URL. Host validation and token restrictions prevent expression injection. Strip internal metadata, Surrogate-Key, and Cache-Tag before delivery. Never let one authority invalidate another authority's objects.

### v0.2 schema contract

Use a structural OpenAPI schema with unknown fields rejected, bounded arrays, and CEL for cross-field checks. The following is a schema shape, not an object with every override enabled:

```yaml
apiVersion: stalewise.dev/v1alpha1
kind: CachePolicy
metadata:
  name: catalog
  namespace: apps
spec:
  serviceRef:
    name: catalog
    port: http
  pathPrefix: /
  overrides:
    ttlSeconds: 60
    graceSeconds: 30
    keepSeconds: 0
    cacheableMethods: [GET, HEAD]
    cacheableStatusCodes: [200, 203, 204]
    maxObjectBytes: 10485760
    key:
      headers: [X-Application-Version]
      queryAllowList: [page, locale]
    cookies:
      mode: stripListed
      names: [analytics]
  bypass:
    - pathPrefix: /account
    - header:
        name: X-Preview
        exact: "true"
  attach:
    mode: route
    methods: [GET, HEAD]
    pathPrefixes: [/]
    loopProtection: sourceLabels
```

Required fields are `serviceRef` and `pathPrefix`; override, bypass, and attach fields are optional. `serviceRef` references only the same namespace. Names/ports follow Service naming rules. Path prefixes start with `/`, contain no query/fragment/control bytes, and match on segment boundaries. `/api` does not match `/apix`.

Durations are integers from zero through 604800. Size is a positive integer up to configured storage capacity, subject to the unknown-length limitation. Arrays are unique, nonempty when present, and capped at 64 items. Header/query/cookie names have protocol-valid syntax and length limits. `queryAllowList: []` explicitly removes all query parameters from the key; absence retains the original query. Query normalization must preserve duplicates/order for retained names.

v0.2 cacheable and attach methods are subsets of GET/HEAD. Arbitrary body-bearing method caching is not introduced. Status codes are explicit supported final responses, excluding 1xx, 206, 304, and 5xx. Set-Cookie/privacy/Authorization safeguards remain non-overridable. Cookie mode is `pass` or `stripListed`; undeclared cookies still pass. Removing a meaningful cookie or query key can mix personalized objects, so admission and documentation must make that risk explicit.

`graceSeconds` overrides both stale windows. Bypass rules run before lookup and always win. Only one winning policy applies; do not merge fields from less-specific policies. Longest matching path wins within the selected Service. Equal specificity for the same Service/path invalidates all competing candidates, regardless of creation order.

`attach` is a validation request, not authority to mutate routes. Missing methods/pathPrefixes mean GET/HEAD and all paths. `loopProtection` is `sourceLabels` or `directService`; the latter additionally requires a same-namespace `directServiceRef`. Validate destination existence, missing bypass, and route order. A direct Service cannot equal the routed Service or cache Service.

### Status, authorization, and failures

Conditions use standard `type`, `status`, `reason`, `message`, `observedGeneration`, and `lastTransitionTime` fields. Expose:

- `Accepted=True/Invalid=False` only after validated policy generation becomes active.
- `Accepted=False/Invalid=True` for semantic errors, with reasons such as Conflict, UnknownService, InvalidOverride, or RenderFailed.
- `Accepted=False/Invalid=False` while waiting on activation or a dependency; reason Pending or DependencyUnavailable.
- `Attached=True/False/Unknown` independently reports route validation, including MissingRoute, MissingBypass, LoopDetected, or UnsupportedDataPlane.

Schema-invalid writes fail API admission and cannot acquire status. Runtime-invalid updates leave the last accepted active policy unchanged; status identifies the rejected generation and last applied generation. Removing or correcting a conflict triggers reconciliation. A valid deletion returns that route to headers-only behavior through the normal transactional reload.

Use per-namespace Roles for Service, EndpointSlice, CachePolicy, and VirtualService reads and CachePolicy status updates. Do not grant route writes. Application writers control only policies in authorized namespaces. Namespace separation alone does not isolate teams sharing one namespace; admission/ownership controls remain necessary.

Later shared tiers select the backend by validated Host/route before policy matching. Use trusted route ownership, never arbitrary policy host claims. Cache keys and tag bans include service identity plus authority to prevent cross-service collisions. v0.1 canonical-host rewriting must not be reused globally across a shared tier.

## Rejected alternatives

- Headers only forever: cannot express operational bypasses, key variations, or typed overrides.
- Service annotations: stringly typed, difficult to validate, and unable to report activation status.
- Central controller values: puts application semantics under the wrong owner.
- A mandatory CRD in v0.1: unnecessary for services already emitting cache headers.

## Consequences, migration, rollback

The default is safe but can route many uncacheable requests through an extra hop. Observe the per-service approximate uncacheable share before adding path restrictions.

Deliver header behavior and integration fixtures first. Add typed schema/status and policy rendering in v0.2 without changing unset-field behavior. Validate candidate policies before activation. Deleting policy overrides restores headers-only behavior; remove traffic attachment first when complete cache bypass is required.

Invalid updates preserve availability, but also preserve previously accepted semantics until corrected. Report that distinction rather than pretending rejected configuration took effect.
