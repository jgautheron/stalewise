# 0004: Sharding and observable purge delivery

## Context

Random Service balancing currently fills each replica independently. Peer traffic must dial a pod IP with the peers authority as Host. The sidecar then uses ORIGINAL_DST over mTLS. Existing discovery, transactional reload, VCL, and signaller remain the owners of their respective boundaries.

## Decision

### Ownership and membership

`--shard` is opt-in. VCL owns consistent hashing; Rust only publishes membership and renders configuration. The [directors reference](https://www.varnish.org/docs/reference/vmod_directors/) documents `shard`, stable `ident`, `.reconfigure()`, and health-independent selection.

Verification used `docker pull varnish:9.1.0`, `docker image inspect varnish:9.1.0`, and an isolated container. Output:

```text
Digest: sha256:f97e894d90bccc585e5847e766696b7785c98046c29f76007f06d2e2c3228a4e
varnishd (varnish-9.1.0 revision ffd36e9db9a1ec2bf6637deda180a87d40a13d0f)
/usr/lib/varnish/vmods/libvmod_directors.so
shard compile: passed
```

`varnishd -V`, `find /usr -name libvmod_directors.so`, and `varnishd -C -f /tmp/shard-check.vcl` produced those results. The compilation exercised this contract, including backend equality for self-detection:

```vcl
new owners = directors.shard();
owners.add_backend(peer, ident="apps/cache-0");
owners.reconfigure(67);
```

```vcl
set req.backend_hint = owners.backend(by=KEY,
    key=owners.key(req.http.Host + "|" + req.url),
    healthy=IGNORE, warmup=0, rampup=false, resolve=NOW);
```

Generation must check both boolean initialization results and fail candidate activation on failure. All peers receive identical ring parameters. Production uses the policy-normalized URL instead of `req.url` when configured. Authority cannot contain the delimiter. Policy headers and Vary still distinguish objects within their owner.

Discovery retains namespace/pod-name identity, sorts lexically, and selects one deterministic address per pod, preferring IPv4. Frontend eligibility requires `ready=true`, nontermination, and a Pod target reference. Missing identity/readiness excludes the peer with a diagnostic; origin discovery remains unchanged. This replaces numeric-IP frontend ordering, not the origin contract. IP changes preserve ring identity; pod replacement changes it.

`--membership-debounce=5s` provides per-peer hysteresis: a change must persist for that interval. Commit eligible changes at most once per interval; unrelated flapping cannot postpone stable changes indefinitely. The reload task builds a new ring during candidate initialization, never mutating active VCL. Failed reloads retain the prior ring. Discovery failures retain the last snapshot; absent or empty membership fetches directly without gating readiness.

### Request path and failure direction

After existing bypass/privacy checks, VCL selects an owner before lookup. A local owner uses the existing origin backend. A non-owner miss uses a generated peer backend dialing that owner's pod IP and frontend port.

The peer request sets `Host: <peer-authority>`, `X-Stalewise-Host: <normalized original authority>`, and `X-Stalewise-Shard-Hop: 1`. The receiving VCL restores authority before policy/hash evaluation. Trusted forwarded requests never forward again, even during ring disagreement. PURGE/BAN never traverse this forwarding path.

Peer headers require discovered-source ACLs plus network/mesh authorization identifying cache peers. A sidecar loopback address alone is not authentication. Reject unauthorized peer headers with 403 and malformed/duplicate hop or authority fields with 400. Strip internal headers before origin requests and client responses.

Each pod builds its ring and peer ACL from its own debounced membership snapshot, so a forwarder can route to an owner that does not trust it yet. This happens at startup, on scale-up, and during rollouts. The owner therefore marks these two refusals with `X-Stalewise-Hop-Refused: 1`, and the forwarder treats a marked refusal like a peer 5xx: it makes one retry directly against the origin and strips the marker. An unmarked 403 or 400 is an origin response and is delivered without a retry. Readiness does not wait for the first membership snapshot, because the skew sits in the owner's snapshot and gating the forwarder's own readiness cannot close it, while the marked fallback covers every skew.

Peer connect, first-byte, and between-byte timeouts use `--peer-timeout`. Before response delivery, transport failure or peer 5xx permits exactly one backend retry directly against origin. Reset peer routing headers and restore origin authority. Never select another peer. Failure after streaming begins terminates the response; it cannot safely retry. Origin failure keeps existing stale/error behavior. Log bounded failure reasons through VSL; operators inspect peer failures and reload/discovery degradation.

Non-owners store no bodies by default. `--shard-local-ttl` enables short copies, capped by the owner's remaining freshness, with zero local grace/keep. Strip origin-supplied peer metadata. Only trusted peer responses may expose `X-Stalewise-Peer-Ttl` (nonnegative remaining freshness in seconds, zero when uncacheable) and `X-Stalewise-Peer-Tags` (existing validated tag format). Compute remaining freshness at owner delivery and subtract transit time once. Preserve Vary; never reapply freshness overrides to peer responses. Both metadata fields are required, allowing an empty tag set; missing or invalid fields forbid local storage. Privacy checks still win, and clients never receive internal metadata. Fallback responses obey the same local-copy cap.

With identical stable membership/policies and a healthy owner, concurrent requests produce one origin fill per cacheable representation per freshness lifetime. Owner Varnish coalesces misses. Failover, eviction, membership skew, uncacheable responses, and in-flight invalidations are not exactly-once cases.

### Purge delivery contract

Keep broadcast to discovered peers plus local Varnish, including with sharding. Ownership-only delivery would miss optional copies, fallback caches, and previous owners. Freeze targets at admission; returning/new peers are not retroactively included.

The signaller assigns each admitted invalidation a random 128-bit ID and atomically reserves queue and history capacity. Its bounded in-memory ring records each target as `pending`, `delivered`, or `exhausted`. A 2xx acknowledgment delivers; permanent rejection, deadline, or shutdown exhaustion terminates failure. Existing transient retries remain bounded. Admission failure returns 503 without partial delivery.

Normal writes return 202 with JSON status, relative `Location: /invalidations/<id>`, and `X-Stalewise-Pod`. Authenticated `GET /invalidations/<id>` returns 200 with per-peer status; unknown, evicted, restarted, or wrong-replica IDs return 404. Authentication precedes lookup. Clients must poll the admitting pod; a balanced Service cannot locate another pod's memory.

For synchronous writes, use `X-Stalewise-Wait: <duration>`, capped at 30s, and strip it before delivery. Object paths and queries remain byte-for-byte unchanged. Reserving `?wait` on writes would corrupt cache keys. Status GETs may use `?wait=<duration>` safely. Waiting returns 200 only when every target delivered, otherwise 504 at timeout with current per-peer status. Timeout/disconnection does not cancel admitted work. Through a balanced Service, prefer synchronous writes.

Delivery acknowledgment is not a global freshness barrier: concurrent fills, unavailable peers, membership changes, and process loss remain possible. Operators inspect exhausted targets and resubmit after recovery; IDs are not idempotency keys.

## Validation

- Unit tests cover membership permutations, dual-stack deduplication, IP changes, readiness flapping, hysteresis deadlines, history eviction, authentication, retry transitions, and wait timeout/disconnect.
- Varnish unit fixtures exercise actual director ring math: determinism, authority separation, wraparound, unchanged-IP identity, and movement only into/out of changed ownership ranges.
- A Docker integration uses three instances and a counted origin. Concurrent cold requests through every instance must yield one fill. Test owner-down fallback, one-hop limits, status/wait results, local-copy tag purges, URI preservation, and forged headers.
- A sidecar sandbox uses STRICT mTLS and random Service routing. Verify pod-IP/peers-Host delivery, fills, partitions, pod churn, purge exhaustion/recovery, wrong-pod status, and reload failure. Run without local copies, then with them enabled. Exclude probes from fill counts.

## Alternatives and consequences

- Reject Rust request hashing: it adds a data-path boundary and duplicates an available director.
- Reject health-dependent ownership and probabilistic warming: observers would disagree and duplicate fills.
- Defer durable logs, consensus, status replication, ownership-only purges, cache transfer, shared tiers, new routing controllers, and other features.

## Migration and rollback

First deploy status tracking and stable identity with sharding disabled. Then verify peer authorization and three-instance tests before enabling sharding everywhere. Keep local copies disabled initially. Upgrade every receiver before enabling forwarding; older receivers do not understand the hop contract. Coordinate the frontend-order change with custom-template users. Roll back by disabling sharding, retaining broadcast, and allowing copies to expire or purging them before binary rollback. Removing cache attachment remains the emergency bypass.
