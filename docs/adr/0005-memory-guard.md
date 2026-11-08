# 0005: Memory guard

## Context

Varnish keeps about 1 KiB per object outside the malloc stevedore: the object core, the object head, the hash entry and allocator slack.
The LRU bounds only stevedore bytes, so a cache of many small objects can pass its container memory limit and be OOM-killed with storage not yet full.
Release 0.5.0 sizes the limit from `storage + transientStorage + 128Mi + 1 KiB x maxObjects` and checks it at startup.
That formula holds only while the real object count stays below `maxObjects`, and nothing in open-source Varnish enforces that count.

Varnish Software's guidance is about 1 KB of overhead per object and about 75% of available memory for malloc storage.
The formula matches the first figure.
The chart defaults give storage and Transient 320 MiB of a 512 MiB limit, about 62%, which is within the second figure.

## Decision

stalewise runs a `memory guard` task, the only issuer of these bans, on its own admin session.
The reloader and the signaller keep their own sessions and are unaffected.

1. Every `--memguard-interval` (default 1s) it reads the working set: cgroup v2 `memory.current` minus `inactive_file` from `memory.stat`, with the v1 equivalents as fallback.
   Kubernetes uses this value for OOM decisions and eviction. Raw usage would count reclaimable page cache, such as the shared log on a disk-backed emptyDir, and ban for nothing.
2. Without a memory limit, the guard logs once and stays idle.
3. At `--memguard-high` (default 0.85) of the limit, it starts an episode and issues `ban obj.age > T`.
4. The first T is half the age of the oldest possible object, capped at `--memguard-max-age` (default 24h).
   No admin command lists object ages cheaply, so stalewise bounds the oldest age from the varnishd start time, raised by every confirmed ban.
   This replaces a binary search from the cap.
5. Each further ban halves T, floored at `--memguard-min-age` (default 10s).
6. At most one ban per `max(interval, 3s)`, and only when the working set has grown by 0.5% of the limit since the previous ban.
   glibc keeps freed memory for reuse, so a flat working set after a ban means the ban is working; banning further would drain the cache without lowering memory.
7. The episode ends below `--memguard-low` (default 0.75).
8. Failures are logged, counted, and set the `memory guard failing` readiness reason without failing readiness.

With the guard enabled, varnishd also gets two parameters, which `--varnish-param` can override:

- `ban_lurker_age=1`: Varnish holds the lurker off bans younger than 60s by default, which is too slow to relieve pressure.
- `critbit_cooloff=60`: evicted object heads are freed after two cooloffs; 60s is Varnish's minimum and cuts the linger from about 6 to about 2 minutes.

`ban_lurker_sleep` (0.010s) and `ban_lurker_batch` (1000) keep their defaults; the lurker walked 170,000 objects within seconds in the Docker test.
`ban_dups` stays on (the default), so a repeated `obj.age > 10s` at the floor replaces its predecessor.
`ban_cutoff` stays 0: guard bans use only `obj.*`, so the lurker completes and removes them, and the test found no guard ban left in `ban.list` after the fill.
A non-zero cutoff would also discard objects for operator bans.

### Allocator

varnishd in `varnish:9.1.0` uses glibc malloc, not jemalloc.
Filling 64m of storage with 150,000 objects of 2 KiB in Docker (transparent huge pages set to `always`) measured:

| Setting | Peak memory | Fill time |
|---|---|---|
| glibc default | 316 MB | 22.7 s |
| `MALLOC_ARENA_MAX=2` | 239 MB | 25.0 s |
| `MALLOC_ARENA_MAX=4` | 240 MB | 22.4 s |
| `PR_SET_THP_DISABLE` | 310 MB | 25.1 s |

stalewise sets `MALLOC_ARENA_MAX=4` by default (`--malloc-arena-max`).
Disabling transparent huge pages did not help measurably, so it is not implemented.

## Semantics

- Eviction is by time in cache, not true LRU. `obj.age` also counts any `Age` header the origin sent.
- Varnish tests a ban lazily, at lookup or when the lurker reaches the object, against the age at that moment.
  An object younger than T when the ban was issued is also banned if it ages past T first.
  With `ban_lurker_age=1` the drift is bounded by the lurker delay plus its walk, about one to two seconds beyond T.
- Memory frees gradually: glibc reuses freed memory instead of returning it, and object heads linger for two cooloffs.
- Bans affect only stored objects, never fetches in progress, so peer-forwarded and streaming fetches are not truncated.
- Bans remove objects held for grace, keep or `stale-if-error`, which is acceptable under memory pressure.
  CachePolicy TTL overrides change how long objects live but not how the guard selects them.
- In shard mode the guard bans only on its own pod; owners refetch through the existing path and non-owner local copies go the same way. It never uses the signaller.
- The guard stops with every other task on SIGTERM, before the drain delay ends.

## Alternatives considered

- **Varnish Enterprise Memory Governor.** The commercial equivalent, not available to open-source Varnish. Its documentation reportedly computes a percentage target from host RAM rather than the container limit; the guard reads the cgroup limit.
- **SLASH buddy stevedore (UPLEX).** It bounds storage, not the per-object overhead that causes the OOM, and it tracks Vinyl Cache rather than Varnish 9.1. Rejected.
- **Startup sizing only.** Release 0.5.0 behaviour; it cannot react when the object count exceeds `maxObjects`. Kept as the primary protection.
- **A cgroup soft limit (`memory.high`).** The kernel would throttle and reclaim, but Varnish memory is anonymous and not reclaimable without swap, so the pod would stall rather than shrink. Rejected.
- **Generation tags with exact-match bans.** Tagging objects with a coarse insert generation and banning `obj.http.X-Stalewise-Gen ~ "^(g1|g2)$"` removes the drift but adds a header to every object and larger ban expressions. The measured drift with a 1s lurker is small, so it is not worth the cost.

## Verification

- Unit tests drive the state machine with a synthetic clock: thresholds, hysteresis, T stepping, the floor, rate limiting, growth gating and cgroup parsing.
- A Docker test fills a 224 MiB container (64m storage, 16m Transient, passing the startup check) with 200,000 objects of 100 bytes at 2,000 per second.
  With the guard: no OOM kill, every request answered 200, a working-set peak of about 208 MB against a 235 MB limit, a few bans, the newest objects hit, the oldest evicted, and no guard ban left in `ban.list`.
  Without the guard: the kernel OOM-killed varnishd after about 129,000 objects.
- A two-pod shard test trips the guard on one pod only; the other issues no bans and every request returns 200.
- A test runs signaller purges while the guard bans; every purge and hit check holds.
