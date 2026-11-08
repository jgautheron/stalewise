# End-to-end tests

The Docker tests exercise real Varnish over HTTP. They share the existing container runner and HTTP client in `tests/common/mod.rs`.
The origin counts requests and generates bodies in 64 KiB chunks. The payload client hashes frames without collecting large bodies.

Run the Docker checks:

```sh
docker build -t stalewise:tests .
STALEWISE_IMAGE=stalewise:tests cargo test --test varnish --test cache_semantics -- --ignored
```

Run the Kubernetes checks:

```sh
make test-chainsaw
```

The runner downloads checksum-pinned Chainsaw 0.2.15 and kind 0.31.0 into `.tools`.
It pins Kubernetes 1.35.0 by image digest and installs only the three vendored Istio CRDs.
It creates a unique `stalewise-tests-*` cluster and a temporary kubeconfig. Its exit trap deletes only that cluster.
Existing clusters and the default kubeconfig are never modified. Supported hosts are macOS arm64 and Linux x86_64.
JUnit step reports remain under `.tools/reports`, or `CHAINSAW_REPORT_DIR` when specified.
Both controller and rollout suites run locally and in CI. No throughput benchmarks run in CI.

## Coverage matrix

A hit requires two X-Varnish transaction IDs, an Age header, and no additional origin fetch.
A miss or pass requires one transaction ID and an additional origin fetch. Body comparisons verify representation identity.
Real Varnish clocks require bounded wall-clock waits for TTL and probe tests.

The following test names refer to `tests/cache_semantics.rs`.

| Header or operation | Expected outcome | Test |
|---|---|---|
| max-age=60; public | Miss, then hit | `freshness_privacy_and_parser_matrix` |
| max-age=0, s-maxage=60 | Hit; shared freshness wins | `freshness_privacy_and_parser_matrix` |
| max-age=60, s-maxage=0; max-age=0 | Pass; zero shared freshness wins | `freshness_privacy_and_parser_matrix` |
| no-store; no-cache; private; Set-Cookie | Pass on every request | `freshness_privacy_and_parser_matrix` |
| must-revalidate; proxy-revalidate | Hit while fresh; pass with zero freshness | `freshness_privacy_and_parser_matrix` |
| Unknown extension alone | Pass; no implicit freshness | `freshness_privacy_and_parser_matrix` |
| Unknown extension plus max-age | Hit | `freshness_privacy_and_parser_matrix` |
| Garbage, negative, overflowing numeric directives | Pass | `freshness_privacy_and_parser_matrix` |
| Mixed-case directive and header names | Hit | `freshness_privacy_and_parser_matrix` |
| Duplicate conflicting or identical max-age | Pass | `freshness_privacy_and_parser_matrix` |
| Separate public and max-age headers | Hit after collection | `freshness_privacy_and_parser_matrix` |
| Duplicate header containing private | Pass | `freshness_privacy_and_parser_matrix` |
| Quoted comma; freshness text inside an extension | Conservative pass; never substring-match | `freshness_privacy_and_parser_matrix` |
| Expires future; Expires past | Hit; pass | `freshness_privacy_and_parser_matrix` |
| Expires plus max-age | max-age wins in both directions | `freshness_privacy_and_parser_matrix` |
| Age greater than max-age or s-maxage | Pass | `freshness_privacy_and_parser_matrix` |
| Age below max-age | Hit; delivered Age preserves elapsed lifetime | `freshness_privacy_and_parser_matrix` |
| Surrogate-Control positive, zero, or unknown | Overrides Cache-Control freshness; removed before delivery | `freshness_privacy_and_parser_matrix` |
| Surrogate-Control plus private | Pass; privacy wins | `freshness_privacy_and_parser_matrix` |
| No freshness headers | Pass; default TTL zero | `freshness_privacy_and_parser_matrix` |
| Cookie; Authorization with public response | Pass; later credential-free requests cache normally | `request_credentials_and_body_methods_always_pass` |
| POST, PUT, DELETE | Pass on every request | `request_credentials_and_body_methods_always_pass` |
| Vary: custom header | One fetch per variant; each variant reused | `vary_partitions_representations_and_reuses_each_variant` |
| Vary: Accept-Encoding | Cached gzip and decoded identity bytes match their checksums | `vary_accept_encoding_serves_gzip_and_identity_from_one_cached_representation` |
| Vary: * | Pass | `freshness_privacy_and_parser_matrix` |
| ETag / If-None-Match; Last-Modified / If-Modified-Since | Cached 304 without origin access; mismatched ETag returns 200 | `conditional_head_and_range_are_served_from_the_full_cached_object` |
| HEAD; Range | Cached HEAD has no body; cached full object supplies exact 206 bytes | `conditional_head_and_range_are_served_from_the_full_cached_object` |
| Conditional backend request inside keep | Backend 304 retains original body and refreshes freshness | `keep_revalidates_with_backend_validators_and_retains_the_body` |
| 200, 203, 204, 301, 404, 410 | Miss, then hit | `native_cacheable_statuses_store_but_server_errors_do_not` |
| 500, 502, 503 | Pass | `native_cacheable_statuses_store_but_server_errors_do_not` |
| stale-while-revalidate | Stale hit, background refresh; background 500/502/503 never replace stale | `stale_while_revalidate_keeps_objects_through_background_server_errors` |
| stale-if-error | Healthy refetch; failed refetch retains stale; sick origin serves stale until deadline | `stale_if_error_is_health_gated_and_ends_at_its_deadline` |
| max-age=2 | Hit after 1.1 seconds; miss after 3 seconds | `ttl_expires_between_one_and_three_seconds` |
| no-store with twenty concurrent requests | Twenty parallel origin fetches after priming hit-for-miss; under two seconds | `hit_for_miss_allows_twenty_parallel_origin_fetches` |
| Surrogate-Key BAN; URL PURGE | Selected object invalidated; unrelated object retained | `tag_ban_and_url_purge_invalidate_only_their_targets` |
| 0, 1, 1024, 1048576, 10380902 bytes | Cached; full byte count and SHA256 match | `payload_sizes_cache_below_limit_and_stream_larger_bodies_intact` |
| 10485761, 52428800, 209715200 bytes with Content-Length | Not stored; full byte count and SHA256 match | `payload_sizes_cache_below_limit_and_stream_larger_bodies_intact` |
| Cacheable chunked response without Content-Length | Stored; 4 MiB malloc storage evicts oldest objects without crashing | `chunked_objects_cache_and_small_storage_evicts_without_crashing` |
| 100,000 objects of 2 KiB into 64 MiB storage | LRU evicts; memory peak stays below the chart minimum limit (240Mi); no OOM kill | `small_objects_past_storage_evict_within_the_chart_minimum_memory` |
| 9 MiB response after small objects fill storage | Delivered whole; no `n_lru_limited` | `large_object_after_small_objects_fill_storage_is_delivered_whole` |
| text/event-stream | Never stored; first bytes precede origin completion | `event_streams_deliver_progressively_and_never_store` |

The complete header matrix runs against both generic and mesh templates. Focused tests use the generic template.

## Kubernetes matrix

| Boundary | Assertion | Suite / step |
|---|---|---|
| Chart profiles | Default, mesh, shard releases become Ready and serve the expected origin body | controller / install-default-mesh-and-shard |
| Management | Every release returns HTTP 200 from /readyz | controller / install-default-mesh-and-shard |
| Mesh resources | AuthorizationPolicy and STRICT PeerAuthentication exist without a mesh | controller / install-default-mesh-and-shard |
| Policy activation | Accepted=True, Invalid=False, Attached=True | controller / policy-active-and-attached |
| Attachment updates | MissingSourceBypass after removal; Attached=True after restoration | controller / attachment-fails-and-recovers |
| Equal-prefix conflict | Both named policies report Accepted=False and Invalid=True with Conflict; deletion restores acceptance | controller / conflicts-are-invalid |
| API admission | Negative TTL is rejected; no invalid resource exists | controller / admission-many-policies-and-read-only-routes |
| Multiple policies | Each of ten named policies reaches Accepted=True | controller / admission-many-policies-and-read-only-routes |
| RBAC | VirtualService list allowed; create/update/patch/delete denied | controller / admission-many-policies-and-read-only-routes |
| Rollout | Origin body remains available during restart; rollout completes; PDB exists | rollout / rollout-keeps-serving |

## Mutation evidence

Run `STALEWISE_IMAGE=stalewise:tests python3 scripts/cache-mutations.py` to repeat the VCL mutation checks.
The script edits actual template source, requires an assertion failure, and restores exact original contents in `finally`.
It does not restore files with checkout. Logs remain in `.tools/mutations`.

All fifteen new Docker test groups have assertion-RED proof. The final 36 VCL mutants were killed:

- Freshness, shared precedence, zero freshness, privacy, Set-Cookie, Vary star, Age, surrogate precedence and stripping.
- Parser validation, case handling, Expires, default zero TTL, Cookie, Authorization, and body methods.
- Custom Vary, conditional requests, HEAD, Range, keep, status handling, expiry, and hit-for-miss parallelism.
- BAN, PURGE, lowered and raised object-size limits, chunked storage, event-stream storage and progressive delivery.
- Gzip/identity delivery, SWR window, background error preservation, SIE window, and SIE deadline.

Kubernetes mutation checks removed the PDB, granted VirtualService writes, admitted negative TTL, and broke the management port.
Further checks changed Active to Pending, disabled conflict detection, and skipped source-loop validation.
Each produced a failure at its corresponding assertion. A deterministic nine-policy capacity limit failed on `many-8`.
Forcing 256 MiB storage instead of the requested 4 MiB failed the oldest-object eviction assertion. Source and chart files were restored after each run.

Initial insufficient mutations were replaced, not counted as successes. Removing one surrogate strip left the other intact.
Removing zero-TTL handling still inherited native Varnish rejection. An infinite hit window could not revive already-evicted objects.

## Bug found

The rollout assertion failed twice before the drain fix: `curl: (56) Recv failure: Connection reset by peer`.
At the original base, `charts/stalewise/templates/deployment.yaml:185-205` had probes but no drain lifecycle.
`src/app.rs:361,379` cancelled the child; `src/varnish/process.rs:131,152-154` immediately sent SIGTERM.
The engineer fixed draining separately on main in `658cc0c`. The tests were rebased onto that fix.
The rollout suite remains independent and is enabled in the normal Kubernetes gate.

## Final validation

After rebasing onto `658cc0c`, both Kubernetes suites passed twice in separate kind clusters.
Each run reported two passed tests, zero failures, and zero skips. Both rollout checks served continuously.
The runner allows 120 seconds for namespace cleanup because pod termination now includes draining.
The pre-fix resets were product failures; early fixture and cleanup failures were corrected before the final runs.

Final local gates passed: formatting, clippy with all targets and denied warnings, 152 Rust unit tests, and 30 Docker tests.
`cargo deny check` reported advisories, bans, licenses, and sources OK. `scripts/chart-check.sh` reported `chart-check: ok`.
`actionlint` and `git diff --check` exited successfully without output.
The final mutation run reported 36 assertion-RED VCL mutants; storage and eight Kubernetes mutants add nine independent RED checks.
The two final kind runs used clusters `stalewise-tests-86712` and `stalewise-tests-4815`; both were deleted afterward.
Each JUnit report contains the named controller and rollout steps with zero failures.
